"""Atomic owner-scoped ciphertext/master snapshot; only disposable synthetic data."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.support.memorylake import offline_test_guard

from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.secrets import SecretRepo
from octop.infra.db.repos.users import UserRepo


class MemoryLakeCredentialSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        directory = self.enterContext(TemporaryDirectory())
        self.pool = SqlitePool(Path(directory) / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.user = UserRepo(self.pool).create(username="synthetic", role="user")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.user, name="synthetic")
        connector = ConnectorRepo(self.pool)
        connector.create(
            instance_id="connector",
            user_id=self.user,
            kind="memorylake",
            display_name="synthetic",
            mcp_server_name="synthetic",
        )
        connector.upsert_credentials(instance_id="connector", blob=b"synthetic-ciphertext")
        SecretRepo(self.pool).get_or_create("connector_fernet", lambda: b"synthetic-master")
        self.repo = MemoryLakeBindingRepo(self.pool)
        self.repo.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.user,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project"]',
            enabled=True,
        )

    def load(self, **kwargs):
        args = {
            "caller_user_id": self.user,
            "agent_id": "agent",
            "master_key_name": "connector_fernet",
        }
        args.update(kwargs)
        return self.repo.load_owned_credentials("binding", **args)

    def test_master_ciphertext_revision_and_permissions_use_one_select(self):
        statements = []
        with self.pool.connect() as conn:
            conn.set_trace_callback(statements.append)
        try:
            snapshot = self.load()
        finally:
            with self.pool.connect() as conn:
                conn.set_trace_callback(None)
        self.assertEqual(snapshot.master_key, b"synthetic-master")
        self.assertEqual(snapshot.credential_blob, b"synthetic-ciphertext")
        self.assertEqual(snapshot.credential_revision, 1)
        selects = [s for s in statements if s.lstrip().upper().startswith("SELECT")]
        self.assertEqual(len(selects), 1)
        self.assertIn("secrets", selects[0])
        self.assertIn("users", selects[0])
        self.assertNotIn("synthetic-master", repr(snapshot))
        self.assertNotIn("synthetic-ciphertext", repr(snapshot))

    def test_denied_identity_returns_no_secret_snapshot(self):
        self.assertIsNone(self.load(caller_user_id=self.user + 1))
        self.assertIsNone(self.load(agent_id="another"))
        with self.pool.transaction() as conn:
            conn.execute("UPDATE users SET disabled=1")
        self.assertIsNone(self.load())

    def test_missing_master_is_not_bootstrapped(self):
        with self.pool.transaction() as conn:
            conn.execute("DELETE FROM secrets WHERE k='connector_fernet'")
        self.assertIsNone(self.load().master_key)
        self.assertIsNone(SecretRepo(self.pool).get("connector_fernet"))

    def test_corrupt_sqlite_integer_fields_are_rejected_not_converted_to_zero_bytes(self):
        # Small sentinels reproduce bytes(int) without making a large allocation.
        for target in ("master", "ciphertext"):
            with self.subTest(target=target):
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE secrets SET v=? WHERE k='connector_fernet'",
                        (42 if target == "master" else b"synthetic-master",),
                    )
                    conn.execute(
                        "UPDATE connectors SET credential_blob=?",
                        (42 if target == "ciphertext" else b"synthetic-ciphertext",),
                    )
                with self.assertRaises(ValueError):
                    self.load()
                if target == "ciphertext":
                    with self.assertRaises(ValueError):
                        self.repo.load_owned_active(
                            "binding", caller_user_id=self.user, agent_id="agent"
                        )

    def test_oversized_binary_snapshot_fields_are_rejected_before_copy(self):
        for target in ("master", "ciphertext"):
            with self.subTest(target=target):
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE secrets SET v=? WHERE k='connector_fernet'",
                        (b"m" * 4097 if target == "master" else b"synthetic-master",),
                    )
                    conn.execute(
                        "UPDATE connectors SET credential_blob=?",
                        (b"c" * 65_537 if target == "ciphertext" else b"synthetic-ciphertext",),
                    )
                with self.assertRaises(ValueError):
                    self.load()
