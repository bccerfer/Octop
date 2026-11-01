"""Atomic management rollback and live owner check on real disposable SQLite."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.support.memorylake import offline_test_guard

from octop.config import OctopConfig
from octop.infra.connectors.memorylake.connections import ConnectionManager
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.utils.paths import PathLayout


class ManagementTransactionTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.services = SharedServices(
            PathLayout(root), OctopConfig(), RepoBundle.from_pool(self.pool)
        )
        self.user = self.services.user_repo.create(username="owner", role="user")
        self.services.agent_repo.create(agent_id="agent", user_id=self.user, name="agent")

    def test_owns_agent_uses_current_owner_without_admin_or_shared_access(self):
        repo = self.services.memorylake_binding_repo
        self.assertTrue(repo.owns_agent(caller_user_id=self.user, agent_id="agent"))
        self.assertFalse(repo.owns_agent(caller_user_id=True, agent_id="agent"))
        self.assertFalse(repo.owns_agent(caller_user_id=self.user + 1, agent_id="agent"))
        with self.pool.transaction() as conn:
            conn.execute("UPDATE users SET disabled=1")
        self.assertFalse(repo.owns_agent(caller_user_id=self.user, agent_id="agent"))

    def test_failure_at_final_policy_insert_rolls_back_master_and_all_rows(self):
        with self.pool.transaction() as conn:
            conn.execute(
                "CREATE TRIGGER deny_policy BEFORE INSERT ON memorylake_write_policies BEGIN SELECT RAISE(ABORT, 'synthetic secret error'); END"
            )
        with self.assertRaisesRegex(ValueError, "^memorylake_connection_unavailable$"):
            ConnectionManager(self.services).create(
                self.user,
                "agent",
                api_key="synthetic-key",
                base_url="https://app.memorylake.ai/openapi/memorylake",
                workspace_id="workspace",
                project_id="project",
                library_parent_id="folder",
                display_name="Synthetic",
                writes_enabled=True,
            )
        with self.pool.connect() as conn:
            for table in ("connectors", "memorylake_bindings", "memorylake_write_policies"):
                self.assertEqual(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        self.assertIsNone(self.services.secret_repo.get_bounded(PURPOSE_CONNECTOR))
