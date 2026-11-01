"""Admission/share linearization on disposable SQLite; no content or real CLI."""

import asyncio
import sqlite3
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.db.repos.users import UserRepo


class AgentAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.owner = UserRepo(self.pool).create(username="owner", role="user")
        self.agents = AgentRepo(self.pool)
        self.agents.create(agent_id="agent", user_id=self.owner, name="synthetic")
        self.connectors = ConnectorRepo(self.pool)
        self.connectors.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="synthetic",
            mcp_server_name="synthetic",
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
        self.session = f"agent:dashboard:{self.owner}:dm"
        ThreadRepo(self.pool).insert(
            thread_id="thread",
            agent_id="agent",
            user_id=self.owner,
            channel_type="dashboard",
            session_key=self.session,
        )

    def admit(self, **overrides):
        args = {
            "caller_user_id": self.owner,
            "agent_id": "agent",
            "binding_id": "binding",
            "connector_id": "connector",
            "authorization_revision": 1,
            "credential_revision": 1,
            "thread_id": "thread",
            "session_key": self.session,
        }
        args.update(overrides)
        self.repo.admit_agent_material(**args)

    async def test_marker_survives_reopen_and_binding_deletion_and_blocks_share(self):
        self.admit()
        self.admit()
        with self.pool.connect() as conn:
            rows = conn.execute("SELECT * FROM memorylake_agent_admissions").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                set(rows[0].keys()), {"agent_id", "owner_user_id", "first_admitted_at"}
            )
        with self.pool.transaction() as conn:
            conn.execute("DELETE FROM memorylake_bindings WHERE binding_id='binding'")
        reopened = SqlitePool(self.pool.path)
        self.addCleanup(reopened.close)
        run_migrations(reopened)
        with self.assertRaisesRegex(ValueError, "^memorylake_material_not_shareable$"):
            AgentRepo(reopened).set_shared("agent", True)
        self.agents.set_shared("agent", False)
        self.assertFalse(self.agents.get("agent").is_shared)

    async def test_native_unmarked_share_allowed_admission_then_denied(self):
        self.agents.set_shared("agent", True)
        with self.assertRaisesRegex(ValueError, "^memorylake_admission_denied$"):
            self.admit()
        self.assertTrue(self.agents.get("agent").is_shared)

    async def test_snapshot_and_thread_mismatch_never_marks(self):
        for overrides in (
            {"authorization_revision": 2},
            {"credential_revision": 2},
            {"thread_id": "missing"},
            {"session_key": "wrong"},
            {"caller_user_id": self.owner + 1},
            {"connector_id": "wrong"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.admit(**overrides)
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_agent_admissions").fetchone()[0], 0
            )

    async def test_migration_repairs_high_watermark_without_resetting_marker(self):
        with self.pool.transaction() as conn:
            conn.execute("DROP TABLE IF EXISTS memorylake_agent_admissions")
            conn.execute("UPDATE _schema_version SET version=99")
        run_migrations(self.pool)
        self.admit()
        run_migrations(self.pool)
        with self.pool.connect() as conn:
            self.assertEqual(conn.execute("SELECT version FROM _schema_version").fetchone()[0], 99)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_agent_admissions").fetchone()[0], 1
            )
            self.assertEqual(
                conn.execute("PRAGMA foreign_key_list(memorylake_agent_admissions)").fetchall(), []
            )

    async def test_two_connection_admission_wins_share_waits_then_denied(self):
        second = SqlitePool(self.pool.path)
        self.addCleanup(second.close)
        entered, release = threading.Event(), threading.Event()
        original = self.pool.transaction

        @contextmanager
        def held_transaction():
            with original() as conn:
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("test release missing")
                yield conn

        loop = asyncio.get_running_loop()
        with patch.object(self.pool, "transaction", held_transaction):
            admission = loop.run_in_executor(None, self.admit)
            share = None
            try:
                async with asyncio.timeout(2):
                    while not entered.is_set():
                        await asyncio.sleep(0.001)
                share = loop.run_in_executor(None, AgentRepo(second).set_shared, "agent", True)
                await asyncio.sleep(0.02)
                self.assertFalse(share.done())
            finally:
                release.set()
                await admission
            with self.assertRaisesRegex(ValueError, "^memorylake_material_not_shareable$"):
                await share

    async def test_live_revocation_expiry_and_malformed_expiry_are_denied(self):
        for table, column, bad, original in (
            ("users", "disabled", 1, 0),
            ("agents", "enabled", 0, 1),
            ("agents", "kind", "team", "expert"),
            ("connectors", "status", "disabled", "active"),
            ("connectors", "shared", 1, 0),
            ("connectors", "credential_expires_at", 1, None),
            ("connectors", "credential_expires_at", "malformed", None),
            ("memorylake_bindings", "enabled", 0, 1),
            ("threads", "channel_type", "im", "dashboard"),
        ):
            with self.subTest(table=table, column=column, bad=bad):
                with self.pool.transaction() as conn:
                    conn.execute(f"UPDATE {table} SET {column}=?", (bad,))
                try:
                    with self.assertRaisesRegex(ValueError, "^memorylake_admission_denied$"):
                        self.admit()
                finally:
                    with self.pool.transaction() as conn:
                        conn.execute(f"UPDATE {table} SET {column}=?", (original,))

    async def test_marker_survives_agent_delete_recreate_and_owner_mismatch(self):
        self.admit()
        with self.pool.transaction() as conn:
            conn.execute("DELETE FROM memorylake_bindings")
            conn.execute("DELETE FROM threads")
            conn.execute("DELETE FROM agents WHERE agent_id='agent'")
        other = UserRepo(self.pool).create(username="other", role="user")
        self.agents.create(agent_id="agent", user_id=other, name="recreated")
        with self.assertRaisesRegex(ValueError, "^memorylake_material_not_shareable$"):
            self.agents.set_shared("agent", True)
        with self.pool.transaction() as conn:
            conn.execute("UPDATE agents SET user_id=?", (self.owner,))
            conn.execute("UPDATE memorylake_agent_admissions SET owner_user_id=?", (other,))
        self.repo.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="synthetic",
            workspace_id="workspace",
            project_ids_json='["project"]',
            enabled=True,
        )
        ThreadRepo(self.pool).insert(
            thread_id="thread",
            agent_id="agent",
            user_id=self.owner,
            channel_type="dashboard",
            session_key=self.session,
        )
        with self.assertRaisesRegex(ValueError, "^memorylake_admission_denied$"):
            self.admit()

    async def test_two_connection_share_wins_admission_waits_then_denied(self):
        second = SqlitePool(self.pool.path)
        self.addCleanup(second.close)
        entered, release = threading.Event(), threading.Event()
        original = self.pool.transaction

        @contextmanager
        def held_transaction():
            with original() as conn:
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("test release missing")
                yield conn

        loop = asyncio.get_running_loop()
        with patch.object(self.pool, "transaction", held_transaction):
            share = loop.run_in_executor(None, self.agents.set_shared, "agent", True)
            admission = None
            try:
                async with asyncio.timeout(2):
                    while not entered.is_set():
                        await asyncio.sleep(0.001)
                with patch.object(self.repo, "_db", second):
                    admission = loop.run_in_executor(None, self.admit)
                    await asyncio.sleep(0.02)
                    self.assertFalse(admission.done())
                    release.set()
                    with self.assertRaisesRegex(ValueError, "^memorylake_admission_denied$"):
                        await admission
            finally:
                release.set()
                await share
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_agent_admissions").fetchone()[0], 0
            )

    async def test_content_free_marker_read_only_true_absence_is_none(self):
        self.assertIsNone(self.repo.agent_material_owner("agent"))
        self.admit()
        self.assertEqual(self.repo.agent_material_owner("agent"), self.owner)
        with self.pool.transaction() as conn:
            conn.execute("UPDATE memorylake_agent_admissions SET owner_user_id=0")
        with self.assertRaises(ValueError):
            self.repo.agent_material_owner("agent")
        with self.pool.transaction() as conn:
            conn.execute("DROP TABLE memorylake_agent_admissions")
        with self.assertRaises(sqlite3.OperationalError):
            self.repo.agent_material_owner("agent")


class RecordedPostgresAdmissionTests(unittest.TestCase):
    """SQL branch contracts, NOT a live PostgreSQL concurrency claim."""

    def setUp(self):
        self.enterContext(offline_test_guard())

    def test_admission_and_share_lock_same_agent_and_v27_repair_uses_pg_source(self):
        from octop.infra.db import migrate

        class Recorded:
            dialect = "postgresql"

            def __init__(self):
                self.statements = []
                self.active = False

            @contextmanager
            def transaction(self):
                assert not self.active
                self.active = True
                try:
                    yield self
                finally:
                    self.active = False

            def execute(self, sql, params=()):
                assert self.active
                self.statements.append((sql, params))
                self.last = sql
                return self

            def fetchone(self):
                if "SELECT c.credential_expires_at" in self.last:
                    return {"credential_expires_at": None}
                if "SELECT owner_user_id" in self.last:
                    return {"owner_user_id": 1}
                return {"agent_id": "agent"}

        db = Recorded()
        MemoryLakeBindingRepo(db).admit_agent_material(
            caller_user_id=1,
            agent_id="agent",
            binding_id="binding",
            connector_id="connector",
            authorization_revision=1,
            credential_revision=1,
            thread_id="thread",
            session_key="agent:dashboard:1:dm",
        )
        locks = [
            sql.split(" FROM ")[1].split()[0] for sql, _ in db.statements if "FOR UPDATE" in sql
        ]
        self.assertEqual(locks, ["users", "agents", "connectors", "memorylake_bindings", "threads"])
        self.assertFalse(db.active)
        db.statements.clear()
        with self.assertRaisesRegex(ValueError, "^memorylake_material_not_shareable$"):
            AgentRepo(db).set_shared("agent", True)
        self.assertIn("FROM agents WHERE agent_id=? FOR UPDATE", db.statements[0][0])
        self.assertFalse(any(sql.startswith("UPDATE agents") for sql, _ in db.statements))
        db.statements.clear()
        path = next(path for version, path in migrate._discover("postgresql") if version == 27)
        migrate._ensure_memorylake_agent_admissions_schema(db, path)
        statements = [sql for sql, _ in db.statements]
        self.assertEqual(len(statements), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS memorylake_agent_admissions", statements[0])
        self.assertNotIn("UPDATE _schema_version", statements[0])
        self.assertNotIn("REFERENCES", statements[0])
        self.assertNotIn("PRAGMA", statements[0])
