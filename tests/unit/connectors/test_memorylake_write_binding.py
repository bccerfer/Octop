"""Real temporary SQLite write-policy loader; no credentials or remote calls."""

import asyncio
import hashlib
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.submission_storage import (
    DatabaseJobAdmission,
    DatabaseSubmissionLedger,
)
from octop.infra.connectors.memorylake.submissions import (
    Approval,
    SubmissionError,
    SubmissionIntent,
)
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
from octop.infra.db.repos.users import UserRepo


class StoredWriteBindingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        directory = self.enterContext(TemporaryDirectory())
        self.pool = SqlitePool(Path(directory) / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        users = UserRepo(self.pool)
        self.owner = users.create(username="owner", role="user")
        self.other = users.create(username="other", role="admin")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="agent")
        self.connectors = ConnectorRepo(self.pool)
        self.connectors.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="ml",
            mcp_server_name="ml",
        )
        self.connectors.upsert_credentials(instance_id="connector", blob=b"synthetic")
        self.repo = MemoryLakeBindingRepo(self.pool)
        self.repo.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project"]',
            enabled=True,
        )
        self.writes = MemoryLakeSubmissionRepo(self.pool)
        self.writes.replace_policy(
            binding_id="binding",
            expected_revision=0,
            project_id="project",
            library_parent_id="folder",
            enabled=True,
        )
        self.admission = DatabaseJobAdmission(max_jobs=1)
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.loader = self.make_loader()

    def make_loader(self, **overrides):
        options = {
            "caller_user_id": self.owner,
            "agent_id": "agent",
            "is_group": False,
            "binding_id": "binding",
            "connector_id": "connector",
            "repo": self.repo,
            "admission": self.admission,
        }
        options.update(overrides)
        return StoredWriteBindingLoader(**options)

    async def test_owner_snapshot_and_rotation_never_loads_master_secret(self):
        first = await self.loader.load()
        self.assertEqual(first.scope.owner_user_id, self.owner)
        self.assertEqual(first.project_id, "project")
        self.assertEqual(first.library_parent_id, "folder")
        self.assertTrue(first.writes_enabled)
        self.assertEqual(first.credential_revision, 1)
        self.assertFalse(hasattr(first, "credential_blob"))
        self.connectors.upsert_credentials(instance_id="connector", blob=b"rotated")
        self.assertEqual((await self.loader.load()).credential_revision, 2)
        await self.loader.authorize(self.caller)

    async def test_disabled_write_policy_remains_available_for_read_reconciliation(self):
        self.writes.replace_policy(
            binding_id="binding",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=False,
        )
        self.assertFalse((await self.loader.load()).writes_enabled)
        with self.assertRaises(SubmissionError):
            await self.loader.authorize(self.caller)

    async def test_other_owner_group_wrong_binding_and_connector_fail_closed(self):
        for options in (
            {"caller_user_id": self.other},
            {"caller_user_id": True},
            {"is_group": True},
            {"agent_id": "other"},
            {"binding_id": "other"},
            {"connector_id": "other"},
        ):
            with self.subTest(options=options), self.assertRaises(SubmissionError):
                await self.make_loader(**options).load()
        for caller in (
            replace(self.caller, user_id=self.other),
            replace(self.caller, is_group=True),
            replace(self.caller, authorization_revision=2),
            replace(self.caller, agent_id="other"),
        ):
            with self.subTest(caller=caller), self.assertRaises(ValueError):
                await self.loader.authorize(caller)

    async def test_live_resource_and_policy_revocation(self):
        for table, column, value in (
            ("users", "disabled", 1),
            ("agents", "is_shared", 1),
            ("agents", "kind", "team"),
            ("agents", "enabled", 0),
            ("connectors", "shared", 1),
            ("connectors", "status", "inactive"),
            ("memorylake_bindings", "enabled", 0),
        ):
            with self.subTest(table=table, column=column):
                with self.pool.connect() as conn:
                    old = conn.execute(f"SELECT {column} FROM {table} LIMIT 1").fetchone()[0]
                with self.pool.transaction() as conn:
                    conn.execute(f"UPDATE {table} SET {column}=?", (value,))
                try:
                    with self.assertRaises(SubmissionError):
                        await self.loader.load()
                finally:
                    with self.pool.transaction() as conn:
                        conn.execute(f"UPDATE {table} SET {column}=?", (old,))
        with self.pool.transaction() as conn:
            conn.execute("DELETE FROM memorylake_write_policies")
        self.assertFalse((await self.loader.load()).writes_enabled)
        with self.assertRaises(SubmissionError):
            await self.loader.authorize(self.caller)

    async def test_invalid_scope_endpoint_expiry_and_policy_fail_closed(self):
        for table, column, value in (
            ("memorylake_bindings", "base_url", "https://invalid.example"),
            ("memorylake_bindings", "project_ids_json", "[]"),
            ("memorylake_bindings", "project_ids_json", '["project","project"]'),
            ("memorylake_bindings", "project_ids_json", "null"),
            ("connectors", "credential_expires_at", 1),
            ("memorylake_write_policies", "project_id", "other"),
            ("memorylake_write_policies", "library_parent_id", "bad\nsecret"),
        ):
            with self.subTest(table=table, column=column):
                with self.pool.connect() as conn:
                    old = conn.execute(f"SELECT {column} FROM {table} LIMIT 1").fetchone()[0]
                with self.pool.transaction() as conn:
                    conn.execute(f"UPDATE {table} SET {column}=?", (value,))
                try:
                    with self.assertRaises(SubmissionError):
                        await self.loader.load()
                finally:
                    with self.pool.transaction() as conn:
                        conn.execute(f"UPDATE {table} SET {column}=?", (old,))

    async def test_current_invocation_checked_after_await(self):
        entered, release = threading.Event(), threading.Event()
        active = True
        original = self.repo.load_owned_write_binding

        def check():
            if not active:
                raise ValueError("sensitive body")

        def blocked(*args, **kwargs):
            result = original(*args, **kwargs)
            entered.set()
            release.wait(3)
            return result

        loader = self.make_loader(check_invocation=check)
        with patch.object(self.repo, "load_owned_write_binding", side_effect=blocked):
            task = asyncio.create_task(loader.load())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                active = False
            finally:
                release.set()
            with self.assertRaises(SubmissionError) as caught:
                await task
        self.assertNotIn("sensitive body", str(caught.exception))

    async def test_cancelled_load_keeps_shared_database_quota_until_actual_completion(self):
        entered, release = threading.Event(), threading.Event()
        original = self.repo.load_owned_write_binding

        def blocked(*args, **kwargs):
            entered.set()
            release.wait(3)
            return original(*args, **kwargs)

        with patch.object(self.repo, "load_owned_write_binding", side_effect=blocked):
            task = asyncio.create_task(self.loader.load())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                with self.assertRaises(SubmissionError):
                    await self.make_loader().load()
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
                for _ in range(100):
                    if not self.admission._jobs:
                        break
                    await asyncio.sleep(0.01)
        self.assertTrue((await self.loader.load()).writes_enabled)

    async def test_policy_revision_change_cannot_silently_rebind_reviewed_intent(self):
        binding = await self.loader.load()
        intent = SubmissionIntent(
            self.caller,
            binding,
            "upload",
            "s.txt",
            hashlib.sha256(b"abc").hexdigest(),
            3,
        )
        self.writes.replace_policy(
            binding_id="binding",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=True,
        )
        ledger = DatabaseSubmissionLedger(
            self.writes, binding_id="binding", admission=self.admission
        )
        with self.assertRaises(SubmissionError):
            await ledger.create_approval(Approval("stale", intent, time.time() + 60))
        self.assertIsNone(self.writes.get_approval("stale"))
        current = await self.loader.load()
        self.assertEqual(current.policy_revision, 2)
        await ledger.create_approval(
            Approval("fresh", replace(intent, binding=current), time.time() + 60)
        )
        self.assertEqual((await ledger.approval("fresh")).intent.binding.policy_revision, 2)
