"""Real disposable owner-scoped connection storage; synthetic encrypted keys only."""

import json
import unittest
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cryptography.fernet import Fernet
from tests.support.memorylake import offline_test_guard

from octop.config import OctopConfig
from octop.infra.connectors.crypto import decrypt_credentials_snapshot
from octop.infra.connectors.memorylake.connections import ConnectionManager
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.utils.paths import PathLayout


class ConnectionManagerTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.services = SharedServices(
            PathLayout(root), OctopConfig(), RepoBundle.from_pool(self.pool)
        )
        self.owner = self.services.user_repo.create(username="owner", role="user")
        self.other = self.services.user_repo.create(username="other", role="admin")
        self.services.agent_repo.create(agent_id="agent", user_id=self.owner, name="agent")
        self.manager = ConnectionManager(self.services)
        self.inputs = {
            "api_key": " synthetic-api-key ",
            "base_url": "https://app.memorylake.ai/openapi/memorylake",
            "workspace_id": "workspace",
            "project_id": "project",
            "library_parent_id": "folder",
            "display_name": "Synthetic",
        }

    def create(self, **overrides):
        return self.manager.create(self.owner, "agent", **(self.inputs | overrides))

    def count(self, table):
        with self.pool.connect() as conn:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_create_encrypted_scope_and_explicit_write_optin(self):
        binding = self.create()
        self.assertFalse(binding.writes_enabled)
        self.assertEqual(binding.policy_revision, 1)
        self.assertEqual(self.manager.list_owned(self.owner, "agent"), [binding])
        self.assertNotIn("synthetic-api-key", json.dumps(asdict(binding)))
        row = self.services.connector_repo.get(binding.connector_id)
        master = self.services.secret_repo.get_bounded(PURPOSE_CONNECTOR)
        self.assertNotIn(b"synthetic-api-key", row.credential_blob)
        decoded = decrypt_credentials_snapshot(master, row.credential_blob)
        self.assertEqual(
            decoded, {"instance_id": binding.connector_id, "api_key": "synthetic-api-key"}
        )
        self.assertEqual(row.credential_revision, 1)
        self.assertFalse(row.shared)
        self.assertEqual(self.count("memorylake_submission_approvals"), 0)
        updated = self.manager.set_write_policy(
            self.owner, "agent", binding.binding_id, enabled=True, expected_revision=1
        )
        self.assertTrue(updated.writes_enabled)
        self.assertEqual(updated.policy_revision, 2)
        with self.assertRaises(ValueError):
            self.manager.set_write_policy(
                self.owner, "agent", binding.binding_id, enabled=False, expected_revision=1
            )
        self.assertTrue(self.manager.list_owned(self.owner, "agent")[0].writes_enabled)

    def test_foreign_shared_disabled_and_team_denied_before_bootstrap(self):
        for kind in ("foreign", "shared", "disabled", "team"):
            with self.subTest(kind=kind):
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE agents SET is_shared=?,kind=?",
                        (int(kind == "shared"), "team" if kind == "team" else "expert"),
                    )
                    conn.execute(
                        "UPDATE users SET disabled=? WHERE id=?",
                        (int(kind == "disabled"), self.owner),
                    )
                caller = self.other if kind == "foreign" else self.owner
                with self.assertRaisesRegex(ValueError, "^memorylake_connection_unavailable$"):
                    self.manager.create(caller, "agent", **self.inputs)
                with self.assertRaises(ValueError):
                    self.manager.list_owned(caller, "agent")
                self.assertEqual(self.count("connectors"), 0)
                self.assertIsNone(self.services.secret_repo.get_bounded(PURPOSE_CONNECTOR))

    def test_invalid_inputs_never_persist_or_reflect_values(self):
        for override in (
            {"api_key": ""},
            {"api_key": "secret\nline"},
            {"base_url": "https://evil.invalid"},
            {"project_id": ""},
            {"workspace_id": "x\ny"},
            {"library_parent_id": "../folder"},
            {"writes_enabled": "true"},
            {"display_name": "secret\nname"},
        ):
            with self.subTest(fields=tuple(override)):
                with self.assertRaisesRegex(ValueError, "^memorylake_connection_unavailable$"):
                    self.create(**override)
                self.assertEqual(self.count("connectors"), 0)
                self.assertIsNone(self.services.secret_repo.get_bounded(PURPOSE_CONNECTOR))

    def test_current_master_rotation_never_uses_cached_fernet(self):
        first = self.create()
        rotated = Fernet.generate_key()
        self.services.secret_repo.rotate(PURPOSE_CONNECTOR, rotated)
        second = self.create(display_name="Second")
        row = self.services.connector_repo.get(second.connector_id)
        self.assertEqual(
            decrypt_credentials_snapshot(rotated, row.credential_blob)["api_key"],
            "synthetic-api-key",
        )
        self.assertNotEqual(first.connector_id, second.connector_id)

    def test_rotation_between_encrypt_and_atomic_insert_rejects_without_partial_connection(self):
        self.create()
        original = self.services.memorylake_binding_repo.create_managed_owned

        def rotate(*args, **kwargs):
            self.services.secret_repo.rotate(PURPOSE_CONNECTOR, Fernet.generate_key())
            return original(*args, **kwargs)

        with (
            patch.object(
                self.services.memorylake_binding_repo, "create_managed_owned", side_effect=rotate
            ),
            self.assertRaises(ValueError),
        ):
            self.create(display_name="Rejected")
        self.assertEqual(self.count("connectors"), 1)
        self.assertEqual(self.count("memorylake_bindings"), 1)
        self.assertEqual(self.count("memorylake_write_policies"), 1)

    def test_policy_update_denies_foreign_and_changed_live_owner(self):
        binding = self.create(writes_enabled=True)
        with self.assertRaises(ValueError):
            self.manager.set_write_policy(
                self.other, "agent", binding.binding_id, enabled=False, expected_revision=1
            )
        with self.pool.transaction() as conn:
            conn.execute("UPDATE agents SET is_shared=1")
        with self.assertRaises(ValueError):
            self.manager.set_write_policy(
                self.owner, "agent", binding.binding_id, enabled=False, expected_revision=1
            )
        self.assertEqual(
            self.services.memorylake_submission_repo.get_policy(binding.binding_id).revision, 1
        )

    def test_owner_revoked_between_preflight_and_commit_cannot_bootstrap_or_write(self):
        original = self.services.memorylake_binding_repo.create_managed_owned

        def revoke(*args, **kwargs):
            with self.pool.transaction() as conn:
                conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
            return original(*args, **kwargs)

        with (
            patch.object(
                self.services.memorylake_binding_repo, "create_managed_owned", side_effect=revoke
            ),
            self.assertRaises(ValueError),
        ):
            self.create()
        self.assertEqual(self.count("connectors"), 0)
        self.assertIsNone(self.services.secret_repo.get_bounded(PURPOSE_CONNECTOR))

    def test_stale_policy_target_rejected_before_mutation(self):
        binding = self.create()
        self.services.memorylake_binding_repo.replace_scope(
            binding_id=binding.binding_id,
            owner_user_id=self.owner,
            expected_revision=1,
            base_url=self.inputs["base_url"],
            workspace_id="workspace",
            project_ids_json='["other"]',
            actor_id=None,
            enabled=True,
        )
        with self.assertRaises(ValueError):
            self.manager.set_write_policy(
                self.owner, "agent", binding.binding_id, enabled=True, expected_revision=1
            )
        policy = self.services.memorylake_submission_repo.get_policy(binding.binding_id)
        self.assertFalse(policy.enabled)
        self.assertEqual(policy.revision, 1)

    def test_scope_changes_between_validation_and_policy_transaction_rejects(self):
        binding = self.create()
        original = self.services.memorylake_binding_repo.set_managed_write_policy

        def change_scope(*args, **kwargs):
            self.services.memorylake_binding_repo.replace_scope(
                binding_id=binding.binding_id,
                owner_user_id=self.owner,
                expected_revision=1,
                base_url=self.inputs["base_url"],
                workspace_id="other-workspace",
                project_ids_json='["project"]',
                actor_id=None,
                enabled=True,
            )
            return original(*args, **kwargs)

        with (
            patch.object(
                self.services.memorylake_binding_repo,
                "set_managed_write_policy",
                side_effect=change_scope,
            ),
            self.assertRaises(ValueError),
        ):
            self.manager.set_write_policy(
                self.owner, "agent", binding.binding_id, enabled=True, expected_revision=1
            )
        policy = self.services.memorylake_submission_repo.get_policy(binding.binding_id)
        self.assertFalse(policy.enabled)
        self.assertEqual(policy.revision, 1)
