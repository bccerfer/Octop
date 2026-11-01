"""Real temporary SQLite + synthetic encrypted credentials; no real CLI/network."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.crypto import encrypt_credentials
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.connectors.memorylake.storage import StoredMemoryLakeReader
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.secrets import SecretRepo
from octop.infra.db.repos.users import UserRepo


@unittest.skipUnless(os.name == "posix", "private CLI sessions require POSIX")
class StoredMemoryLakeReaderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.temp = tempfile.TemporaryDirectory()
        self.pool = SqlitePool(Path(self.temp.name) / "test.db")
        run_migrations(self.pool)
        users = UserRepo(self.pool)
        self.owner = users.create(username="owner", role="user")
        self.other = users.create(username="other", role="user")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="agent")
        self.connectors = ConnectorRepo(self.pool)
        self.connectors.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="ml",
            mcp_server_name="ml",
        )
        self.secrets = SecretRepo(self.pool)
        self.store_key("dummy-first")
        self.repo = MemoryLakeBindingRepo(self.pool)
        self.row = self.repo.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project"]',
            actor_id="actor",
            enabled=True,
        )
        self.calls = []
        self.after_run = None
        outer = self

        class FakeRunner:
            async def run(self, arguments, *, env, cwd):
                outer.calls.append((tuple(arguments), dict(env), cwd))
                if outer.after_run:
                    outer.after_run()
                return ProcessResult(
                    0, b'{"documents":[{"document_id":"doc","project_id":"project"}]}', b""
                )

        self.runner_patch = self.enterContext(
            patch(
                "octop.infra.connectors.memorylake.read_service.MemoryLakeRunner",
                return_value=FakeRunner(),
            )
        )
        self.database = DatabaseJobAdmission(max_jobs=1)
        self.reader = self.make_reader()

    async def asyncTearDown(self):
        await self.database.aclose()

    def tearDown(self):
        self.pool.close()
        self.temp.cleanup()

    def make_reader(self, **overrides):
        options = {
            "database": self.database,
            "caller_user_id": self.owner,
            "agent_id": "agent",
            "is_group": False,
            "binding_id": "binding",
            "connector_id": "connector",
            "repo": self.repo,
            "secret_repo": self.secrets,
            "executable": Path(sys.executable),
        }
        options.update(overrides)
        return StoredMemoryLakeReader(**options)

    def store_key(self, key, *, instance_id="connector", expires_at=None):
        blob = encrypt_credentials(self.secrets, {"instance_id": instance_id, "api_key": key})
        self.connectors.upsert_credentials(
            instance_id="connector", blob=blob, expires_at=expires_at
        )

    def sql(self, statement, parameters=()):
        with self.pool.transaction() as connection:
            connection.execute(statement, parameters)

    async def call(self, **arguments):
        return await self.reader.call("search_documents", {"query": "synthetic", **arguments})

    async def test_real_sql_encrypted_key_read_service_and_cleanup(self):
        response = await self.call()
        self.assertEqual(response["data"]["documents"][0]["document_id"], "doc")
        args, env, cwd = self.calls[0]
        self.assertEqual(env["MEMORYLAKE_API_KEY"], "dummy-first")
        self.assertEqual(args[args.index("--projects") + 1], "project")
        self.assertFalse(cwd.exists())
        self.assertNotIn("dummy-first", repr(self.reader))

    async def test_wrong_owner_group_or_agent_denied_before_decryption(self):
        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot"
        ) as decrypt:
            for overrides in (
                {"caller_user_id": self.other},
                {"is_group": True},
                {"agent_id": "other"},
            ):
                with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                    await self.make_reader(**overrides).call("search_documents", {"query": "q"})
            decrypt.assert_not_called()
        self.assertEqual(self.calls, [])

    async def test_shared_disabled_and_reassigned_resources_deny_before_decryption(self):
        cases = [
            ("agents", "is_shared", 1, 0),
            ("agents", "enabled", 0, 1),
            ("agents", "user_id", self.other, self.owner),
            ("users", "disabled", 1, 0),
            ("connectors", "shared", 1, 0),
            ("connectors", "status", "disabled", "active"),
            ("connectors", "user_id", self.other, self.owner),
            ("memorylake_bindings", "enabled", 0, 1),
        ]
        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot"
        ) as decrypt:
            for table, field, bad, original in cases:
                self.sql(f"UPDATE {table} SET {field}=?", (bad,))
                with self.subTest(table=table, field=field), self.assertRaises(ValueError):
                    await self.call()
                self.sql(f"UPDATE {table} SET {field}=?", (original,))
            decrypt.assert_not_called()
        self.assertEqual(self.calls, [])

    async def test_same_second_rotation_uses_latest_key_revision(self):
        await self.call()
        with patch("octop.infra.db.repos.connectors.now_ts", return_value=123):
            self.store_key("dummy-second")
            self.store_key("dummy-third")
        await self.call()
        self.assertEqual(
            [call[1]["MEMORYLAKE_API_KEY"] for call in self.calls], ["dummy-first", "dummy-third"]
        )

    async def test_scope_cas_revocation_during_remote_read_discards_response(self):
        def revoke():
            self.repo.replace_scope(
                binding_id="binding",
                owner_user_id=self.owner,
                expected_revision=1,
                base_url=self.row.base_url,
                workspace_id="workspace",
                project_ids_json='["project"]',
                actor_id="actor",
                enabled=False,
            )

        self.after_run = revoke
        with self.assertRaises(ValueError):
            await self.call()
        self.assertEqual(len(self.calls), 1)

    async def test_expired_key_rejected_before_decrypt(self):
        self.sql("UPDATE connectors SET credential_expires_at=1")
        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot"
        ) as decrypt:
            with self.assertRaises(ValueError):
                await self.call()
            decrypt.assert_not_called()
        self.assertEqual(self.calls, [])

    async def test_bad_scope_and_endpoint_rejected_before_decrypt(self):
        cases = [
            ("project_ids_json", value)
            for value in (
                "{}",
                '["project","project"]',
                '["../x"]',
                "[true]",
                json.dumps([f"p{i}" for i in range(51)]),
            )
        ]
        cases += [("base_url", "http://127.0.0.1"), ("actor_id", "../actor")]
        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot"
        ) as decrypt:
            for field, bad in cases:
                original = getattr(self.row, field)
                self.sql(f"UPDATE memorylake_bindings SET {field}=?", (bad,))
                with self.subTest(field=field, bad=bad), self.assertRaises(ValueError):
                    await self.call()
                self.sql(f"UPDATE memorylake_bindings SET {field}=?", (original,))
            decrypt.assert_not_called()
        self.assertEqual(self.calls, [])

    async def test_missing_master_does_not_create_secret(self):
        self.sql("DELETE FROM secrets WHERE k=?", (PURPOSE_CONNECTOR,))
        with (
            patch.object(
                self.secrets, "get_or_create", side_effect=AssertionError("must not create")
            ),
            self.assertRaises(ValueError),
        ):
            await self.call()
        self.assertIsNone(self.secrets.get(PURPOSE_CONNECTOR))
        self.assertEqual(self.calls, [])

    async def test_decryption_never_uses_mutating_secret_repo_method(self):
        with patch.object(
            self.secrets, "get_or_create", side_effect=AssertionError("must not create")
        ):
            await self.call()
        self.assertEqual(len(self.calls), 1)

    async def test_wrong_ciphertext_instance_and_invalid_key_denied(self):
        for key, instance in (
            ("dummy", "other"),
            ("", "connector"),
            ("dummy\0key", "connector"),
            (None, "connector"),
        ):
            self.store_key(key, instance_id=instance)
            with self.subTest(instance=instance), self.assertRaises(ValueError):
                await self.call()
        self.assertEqual(self.calls, [])

    async def test_model_identity_endpoint_and_argv_overrides_are_rejected(self):
        for name in ("as_user", "endpoint", "argv", "api_key", "actor_id", "workspace"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                await self.call(**{name: "untrusted"})
        self.assertEqual(self.calls, [])

    async def test_sql_and_crypto_execute_off_event_loop(self):
        from octop.infra.connectors.crypto import decrypt_credentials_snapshot

        threads = []
        original_load = self.repo.load_owned_active

        def load(*args, **kwargs):
            threads.append(threading.get_ident())
            return original_load(*args, **kwargs)

        def decrypt(*args, **kwargs):
            threads.append(threading.get_ident())
            return decrypt_credentials_snapshot(*args, **kwargs)

        with (
            patch.object(self.repo, "load_owned_active", side_effect=load),
            patch(
                "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
                side_effect=decrypt,
            ),
        ):
            await self.call()
        self.assertTrue(threads)
        self.assertNotIn(threading.get_ident(), threads)

    async def test_database_rejects_nonpositive_authorization_revision(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.sql("UPDATE memorylake_bindings SET authorization_revision=0")

    async def test_master_deleted_after_read_never_gets_recreated(self):
        original_load = self.repo.load_owned_credentials

        def remove_after_read(*args, **kwargs):
            value = original_load(*args, **kwargs)
            self.sql("DELETE FROM secrets WHERE k=?", (PURPOSE_CONNECTOR,))
            return value

        with (
            patch.object(self.repo, "load_owned_credentials", side_effect=remove_after_read),
            patch.object(
                self.secrets, "get_or_create", side_effect=AssertionError("must not recreate")
            ),
            self.assertRaises(ValueError),
        ):
            await self.call()
        self.assertIsNone(self.secrets.get(PURPOSE_CONNECTOR))
        self.assertEqual(self.calls, [])

    async def test_rotation_during_decrypt_cannot_mix_blob_and_revision(self):
        from octop.infra.connectors.crypto import decrypt_credentials_snapshot

        def rotate(master, blob):
            payload = decrypt_credentials_snapshot(master, blob)
            self.store_key("dummy-rotated")
            return payload

        with (
            patch(
                "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
                side_effect=rotate,
            ),
            self.assertRaises(ValueError),
        ):
            await self.call()
        self.assertEqual(self.calls, [])
        await self.call()
        self.assertEqual(self.calls[0][1]["MEMORYLAKE_API_KEY"], "dummy-rotated")

    async def test_master_rotation_does_not_reuse_cached_fernet(self):
        from cryptography.fernet import Fernet

        # setUp intentionally populated the existing encryption helper's cache.
        # Snapshot decoding must instead use the newly stored master bytes.
        master = Fernet.generate_key()
        self.secrets.rotate(PURPOSE_CONNECTOR, master)
        blob = Fernet(master).encrypt(
            json.dumps({"instance_id": "connector", "api_key": "dummy-new-master"}).encode()
        )
        self.connectors.upsert_credentials(instance_id="connector", blob=blob, expires_at=None)
        await self.call()
        self.assertEqual(self.calls[0][1]["MEMORYLAKE_API_KEY"], "dummy-new-master")

    async def test_wrong_fixed_connector_and_tampered_ciphertext_fail_closed(self):
        with self.assertRaises(ValueError):
            await self.make_reader(connector_id="another").call("search_documents", {"query": "q"})
        self.connectors.upsert_credentials(
            instance_id="connector", blob=b"dummy-invalid-ciphertext", expires_at=None
        )
        with self.assertRaises(ValueError) as caught:
            await self.call()
        self.assertNotIn("dummy-invalid-ciphertext", str(caught.exception))
        self.assertEqual(self.calls, [])

    def test_reader_requires_explicit_shared_database(self):
        with self.assertRaisesRegex(ValueError, "shared_database_admission_required"):
            self.make_reader(database=None)

    def test_omitted_database_cannot_construct_reader(self):
        with self.assertRaises(TypeError):
            StoredMemoryLakeReader(
                caller_user_id=self.owner,
                agent_id="agent",
                is_group=False,
                binding_id="binding",
                connector_id="connector",
                repo=self.repo,
                secret_repo=self.secrets,
                executable=Path(sys.executable),
            )
        self.assertEqual(self.calls, [])
