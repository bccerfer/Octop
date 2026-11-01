"""Two binding-scoped new-upload flows sharing one dormant runtime and real SQLite.

Synthetic bytes/fake transport only; not an HTTP upload, approval UI, registered
production path, workspace re-read, or a cross-process resource-budget guarantee.
"""

import asyncio
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.connectors.memorylake.submission_storage import DatabaseSubmissionLedger
from octop.infra.connectors.memorylake.submissions import Approval, SubmissionService
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
from octop.infra.db.repos.users import UserRepo


class ScopedUploadFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        directory = self.enterContext(TemporaryDirectory())
        self.pool = SqlitePool(Path(directory) / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.owner = UserRepo(self.pool).create(username="owner", role="user")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="agent")
        self.bindings = MemoryLakeBindingRepo(self.pool)
        self.repo = MemoryLakeSubmissionRepo(self.pool)
        connectors = ConnectorRepo(self.pool)
        for name in ("a", "b"):
            connectors.create(
                instance_id=f"connector-{name}",
                user_id=self.owner,
                kind="memorylake",
                display_name=name,
                mcp_server_name=f"ml-{name}",
            )
            connectors.upsert_credentials(instance_id=f"connector-{name}", blob=b"synthetic")
            self.bindings.create(
                binding_id=f"binding-{name}",
                connector_id=f"connector-{name}",
                owner_user_id=self.owner,
                agent_id="agent",
                base_url="https://app.memorylake.ai/openapi/memorylake",
                workspace_id="workspace",
                project_ids_json='["project"]',
                enabled=True,
            )
            self.repo.replace_policy(
                binding_id=f"binding-{name}",
                expected_revision=0,
                project_id="project",
                library_parent_id="folder",
                enabled=True,
            )
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.calls = []

    def loader(self, name, admission):
        return StoredWriteBindingLoader(
            caller_user_id=self.owner,
            agent_id="agent",
            is_group=False,
            binding_id=f"binding-{name}",
            connector_id=f"connector-{name}",
            repo=self.bindings,
            admission=admission,
        )

    async def asyncSetUp(self):
        self.runtime = MemoryLakeFileRuntime(
            authorize_factory=lambda admission: self.loader("a", admission).authorize,
            limits=FileRuntimeLimits(
                max_files=2, max_file_bytes=16, max_total_bytes=32, max_db_jobs=1
            ),
        )
        self.addAsyncCleanup(self.runtime.aclose)
        self.sources, self.ledgers, self.services = {}, {}, {}
        for name in ("a", "b"):
            loader = self.loader(name, self.runtime.database)
            source = self.runtime.source.bind(
                binding_id=f"binding-{name}",
                connector_id=f"connector-{name}",
                caller=self.caller,
                authorize=loader.authorize,
            )
            ledger = DatabaseSubmissionLedger(
                self.repo, binding_id=f"binding-{name}", admission=self.runtime.database
            )

            class FakeTransport:
                async def upload(inner, binding, snapshot, *, lease):
                    self.calls.append(("upload", binding.scope.connector_id))
                    return ProcessResult(0, b'{"item_id":"item:1"}', b"")

                async def import_file(inner, binding, item_id, *, lease):
                    self.calls.append(("import", binding.scope.connector_id))
                    return ProcessResult(
                        0,
                        b'{"success_count":1,"failure_count":0,"duplicate_count":0,"details_truncated":false,"details":[{"result":"success","drive_item_id":"item:1","document_id":"doc:1"}]}',
                        b"",
                    )

                async def get_document(inner, binding, document_id):
                    self.calls.append(("get", binding.scope.connector_id))
                    return ProcessResult(0, b'{"id":"doc:1","status":"okay"}', b"")

            self.sources[name], self.ledgers[name] = source, ledger
            self.services[name] = SubmissionService(
                f"connector-{name}",
                load_binding=loader.load,
                source=source,
                ledger=ledger,
                transport=FakeTransport(),
            )

    async def test_cross_binding_and_raw_store_cannot_prepare_or_submit_opaque_id(self):
        with self.runtime.operation():
            key = await self.sources["a"].add(self.caller, "synthetic.txt", b"abc")
            intent = await self.services["a"].prepare(self.caller, key)
            await self.ledgers["a"].create_approval(
                Approval("approval-a", intent, time.time() + 60)
            )
            with self.assertRaises(ValueError):
                await self.runtime.source.read(self.caller, key)
            with self.assertRaises(ValueError):
                await self.sources["b"].read(self.caller, key)
            with self.assertRaises(ValueError):
                await self.services["b"].prepare(self.caller, key)
            with self.assertRaises(ValueError):
                await self.services["b"].submit(self.caller, key, "approval-a")
            self.assertEqual(self.calls, [])
            self.assertFalse(self.repo.get_approval("approval-a").claimed)
            record = await self.services["a"].submit(self.caller, key, "approval-a")
            self.assertEqual(record.state, "processing")
            self.assertEqual(
                (await self.services["a"].check_status(self.caller, "approval-a")).state, "ready"
            )
        self.assertEqual(
            self.calls,
            [("upload", "connector-a"), ("import", "connector-a"), ("get", "connector-a")],
        )

    async def test_distinct_source_cannot_borrow_other_binding_approval(self):
        with self.runtime.operation():
            keys = {}
            for name in ("a", "b"):
                keys[name] = await self.sources[name].add(self.caller, "synthetic.txt", b"abc")
            intent = await self.services["a"].prepare(self.caller, keys["a"])
            await self.ledgers["a"].create_approval(
                Approval("approval-a", intent, time.time() + 60)
            )
            with self.assertRaises(ValueError):
                await self.services["b"].submit(self.caller, keys["b"], "approval-a")
            self.assertEqual(self.calls, [])
            self.assertFalse(self.repo.get_approval("approval-a").claimed)
            own_intent = await self.services["b"].prepare(self.caller, keys["b"])
            await self.ledgers["b"].create_approval(
                Approval("approval-b", own_intent, time.time() + 60)
            )
            self.assertEqual(
                (await self.services["b"].submit(self.caller, keys["b"], "approval-b")).state,
                "processing",
            )
        self.assertEqual(self.calls, [("upload", "connector-b"), ("import", "connector-b")])

    async def test_live_binding_and_policy_revocation_are_not_authorized_by_facade(self):
        with self.runtime.operation():
            key_a = await self.sources["a"].add(self.caller, "synthetic.txt", b"abc")
            key_b = await self.sources["b"].add(self.caller, "synthetic.txt", b"def")
            with self.pool.transaction() as conn:
                conn.execute(
                    "UPDATE memorylake_bindings SET enabled=0 WHERE binding_id='binding-a'"
                )
            with self.assertRaises(ValueError):
                await self.services["a"].prepare(self.caller, key_a)
            self.assertEqual((await self.sources["b"].read(self.caller, key_b)).content, b"def")
            self.repo.replace_policy(
                binding_id="binding-b",
                expected_revision=1,
                project_id="project",
                library_parent_id="folder",
                enabled=False,
            )
            with self.assertRaises(ValueError):
                await self.services["b"].prepare(self.caller, key_b)
        self.assertEqual(self.calls, [])

    async def test_facades_share_capacity_but_facade_close_does_not_close_store(self):
        with self.runtime.operation():
            await self.sources["a"].add(self.caller, "synthetic.txt", b"abc")
            key_b = await self.sources["b"].add(self.caller, "synthetic.txt", b"def")
            with self.assertRaises(ValueError):
                await self.sources["b"].add(self.caller, "extra.txt", b"x")
            self.sources["a"].close()
            with self.assertRaises(ValueError):
                await self.sources["a"].add(self.caller, "closed.txt", b"x")
            self.assertEqual((await self.sources["b"].read(self.caller, key_b)).content, b"def")
            await self.sources["b"].revoke(self.caller, key_b)
            self.assertTrue(await self.sources["b"].add(self.caller, "replacement.txt", b"ghi"))
        self.assertEqual(self.calls, [])

    async def test_cancelled_scope_a_authorization_keeps_shared_database_slot(self):
        started, release = threading.Event(), threading.Event()
        original = self.bindings.load_owned_write_binding

        def blocked(*args, **kwargs):
            started.set()
            release.wait(3)
            return original(*args, **kwargs)

        with (
            self.runtime.operation(),
            patch.object(self.bindings, "load_owned_write_binding", side_effect=blocked) as load,
        ):
            task = asyncio.create_task(self.sources["a"].add(self.caller, "synthetic.txt", b"abc"))
            actual = ()
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                actual = tuple(self.runtime.database._jobs)
                self.assertEqual(len(actual), 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertFalse(actual[0].done())
                with self.assertRaises(ValueError):
                    await self.sources["b"].add(self.caller, "synthetic.txt", b"def")
                # B cannot start a second repository call through a separate quota.
                self.assertEqual(load.call_count, 1)
                self.assertEqual(tuple(self.runtime.database._jobs), actual)
                self.assertEqual(self.calls, [])
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
                await asyncio.gather(*actual, return_exceptions=True)
            self.assertFalse(self.runtime.database._jobs)
            key = await self.sources["b"].add(self.caller, "synthetic.txt", b"def")
            self.assertEqual((await self.sources["b"].read(self.caller, key)).content, b"def")
            self.assertEqual(load.call_count, 3)
        self.assertEqual(self.calls, [])
