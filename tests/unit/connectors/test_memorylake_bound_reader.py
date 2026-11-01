"""Real current-owner SQLite composition with fake CLI reads only."""

import asyncio
import json
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from tests.support.memorylake import offline_test_guard

from octop.config import OctopConfig
from octop.infra.connectors.memorylake.application import MemoryLakeApplication
from octop.infra.connectors.memorylake.connections import ConnectionManager
from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.deployment import CliArtifact
from octop.infra.connectors.memorylake.runner import MemoryLakeCancelledError, ProcessResult
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.utils.paths import PathLayout


class BoundReaderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(offline_test_guard())
        root = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        config = OctopConfig()
        config = replace(config, memorylake_cli=replace(config.memorylake_cli, enabled=True))
        self.services = SharedServices(PathLayout(root), config, RepoBundle.from_pool(self.pool))
        self.owner = self.services.user_repo.create(username="owner", role="user")
        self.services.agent_repo.create(agent_id="agent", user_id=self.owner, name="agent")
        self.binding = ConnectionManager(self.services).create(
            self.owner,
            "agent",
            api_key="synthetic-key",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_id="project",
            library_parent_id="folder",
            display_name="ML",
        )
        self.server = SimpleNamespace(services=self.services)
        self.app = MemoryLakeApplication(self.server)
        self.server.memorylake_application = self.app
        self.server.memorylake_file_runtime = self.app.runtime
        self.app._artifact = CliArtifact(root / "never-executed", "0.0.0", "0" * 64, 1)
        self.app._reason = "ready"
        self.addAsyncCleanup(self.app.runtime.aclose)
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.calls = []
        self.after = None
        self.block = False
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.reply = ProcessResult(0, b'{"documents":[],"facts":[],"databases":[]}', b"")

        async def run(runner, argv, *, env, cwd):
            self.calls.append((argv, dict(env), cwd))
            self.entered.set()
            if self.block:
                await self.release.wait()
            if self.after:
                self.after()
            return self.reply

        self.enterContext(
            patch("octop.infra.connectors.memorylake.read_service.MemoryLakeRunner.run", run)
        )

    async def read(self, **overrides):
        args = {
            "caller": self.caller,
            "binding_id": self.binding.binding_id,
            "operation": "search_documents",
            "arguments": {"query": "synthetic"},
            "check_invocation": lambda: None,
        }
        return await self.app.read_documents(**(args | overrides))

    async def test_disabled_writes_allow_scoped_read_and_private_session_cleanup(self):
        result = await self.read()
        self.assertTrue(result["untrusted"])
        argv, env, cwd = self.calls[0]
        self.assertEqual(argv[argv.index("--projects") + 1], "project")
        self.assertEqual(argv[argv.index("--types") + 1], "document")
        self.assertEqual(env["MEMORYLAKE_API_KEY"], "synthetic-key")
        self.assertFalse(cwd.exists())

    async def test_foreign_shared_and_argument_injection_denied_before_execution(self):
        for overrides in (
            {"caller": replace(self.caller, user_id=self.owner + 100)},
            {"arguments": {"query": "q", "project_ids": ["other"]}},
            {"arguments": {"query": "q", "profile": "evil"}},
            {"operation": "search_facts"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                await self.read(**overrides)
        with self.pool.transaction() as conn:
            conn.execute("UPDATE agents SET is_shared=1")
        with self.assertRaises(ValueError):
            await self.read()
        self.assertEqual(self.calls, [])

    async def test_revoke_and_rotate_during_read_discard_result(self):
        for change in ("revoke", "rotate"):

            def mutate(change=change):
                if change == "revoke":
                    with self.pool.transaction() as conn:
                        conn.execute("UPDATE users SET disabled=1")
                else:
                    master = self.services.secret_repo.get_bounded(PURPOSE_CONNECTOR)
                    self.services.connector_repo.upsert_credentials(
                        instance_id=self.binding.connector_id,
                        blob=Fernet(master).encrypt(
                            json.dumps(
                                {"instance_id": self.binding.connector_id, "api_key": "rotated"}
                            ).encode()
                        ),
                    )

            self.after = mutate
            with self.subTest(change=change), self.assertRaises(ValueError):
                await self.read()
            with self.pool.transaction() as conn:
                conn.execute("UPDATE users SET disabled=0")

    async def test_out_of_scope_result_denied(self):
        self.reply = ProcessResult(
            0,
            b'{"documents":[{"document_id":"doc","project_id":"other"}],"facts":[],"databases":[]}',
            b"",
        )
        with self.assertRaises(ValueError):
            await self.read()

    async def test_shared_admission_serializes_independent_calls_and_close_denies_queued(self):
        self.block = True
        first = asyncio.create_task(self.read())
        await self.entered.wait()
        second = asyncio.create_task(self.read())
        try:
            for _ in range(100):
                if self.app._read_admission._queued:
                    break
                await asyncio.sleep(0.001)
            self.assertEqual(self.app._read_admission._queued, 1)
            self.assertEqual(len(self.calls), 1)
            self.app.runtime.close()
        finally:
            self.release.set()
            results = await asyncio.gather(first, second, return_exceptions=True)
        self.assertTrue(all(isinstance(result, ValueError) for result in results))
        self.assertEqual(len(self.calls), 1)

    async def test_get_uses_fixed_project_and_rejects_invalid_id(self):
        self.reply = ProcessResult(0, b'{"id":"doc","status":"okay"}', b"")
        result = await self.read(operation="get_document", arguments={"document_id": "doc"})
        self.assertEqual(result["operation"], "get_document")
        argv = self.calls[0][0]
        self.assertEqual(argv[argv.index("--project") + 1], "project")
        with self.assertRaises(ValueError):
            await self.read(operation="get_document", arguments={"document_id": "bad\nid"})
        self.assertEqual(len(self.calls), 1)

    async def test_typed_user_cancel_propagates_and_owned_deadline_is_safe_failure(self):
        real_timeout = asyncio.timeout
        for expired in (False, True):
            self.entered.clear()

            async def cancelled(runner, argv, *, env, cwd):
                self.entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    raise MemoryLakeCancelledError() from None

            with (
                self.subTest(expired=expired),
                patch(
                    "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner.run", cancelled
                ),
                patch(
                    "octop.infra.connectors.memorylake.bound_reader.asyncio.timeout",
                    side_effect=lambda _, expired=expired: real_timeout(0.1 if expired else 30),
                ),
            ):
                task = asyncio.create_task(self.read())
                await self.entered.wait()
                if not expired:
                    task.cancel()
                with self.assertRaises(ValueError if expired else asyncio.CancelledError):
                    await task
                self.assertFalse(self.app._read_admission._active)
                self.assertFalse(self.app.runtime._holders)
