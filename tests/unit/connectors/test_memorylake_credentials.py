"""Real SQLite and synthetic Fernet snapshots; no external credentials or CLI."""

import asyncio
import base64
import json
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cryptography.fernet import Fernet
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.crypto import decrypt_credentials_snapshot
from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.credentials import StoredMemoryLakeCredentials
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.secrets import SecretRepo
from octop.infra.db.repos.users import UserRepo


class CredentialsTests(unittest.IsolatedAsyncioTestCase):
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
        self.master = base64.urlsafe_b64encode(b"m" * 32)
        self.key = "synthetic-api-key-secret"
        self.restore_master()
        self.store_key("  " + self.key + "  ")
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.admission = DatabaseJobAdmission(max_jobs=1)
        self.addAsyncCleanup(self.admission.aclose)
        self.active = True

    def restore_master(self):
        with self.pool.transaction() as conn:
            conn.execute(
                "INSERT INTO secrets(k,v,created_at) VALUES (?,?,0) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (PURPOSE_CONNECTOR, self.master),
            )

    def store_key(self, key, **extras):
        payload = {"instance_id": "connector", "api_key": key}
        payload.update(extras)
        blob = Fernet(self.master).encrypt(json.dumps(payload).encode())
        self.connectors.upsert_credentials(instance_id="connector", blob=blob)

    def check(self):
        if not self.active:
            raise ValueError("synthetic invocation private reason")

    def loader(self, **overrides):
        options = {
            "caller": self.caller,
            "binding_id": "binding",
            "connector_id": "connector",
            "repo": self.repo,
            "admission": self.admission,
            "check_invocation": self.check,
        }
        options.update(overrides)
        return StoredMemoryLakeCredentials(**options)

    async def test_current_synthetic_key_revision_trim_and_no_bootstrap(self):
        with (
            patch.object(
                SecretRepo, "get_or_create", side_effect=AssertionError("must not bootstrap")
            ),
            patch(
                "octop.infra.crypto.fernet.get_fernet", side_effect=AssertionError("must not cache")
            ),
        ):
            credentials = await self.loader().load()
        self.assertEqual((credentials.api_key, credentials.revision), (self.key, 1))
        self.assertNotIn(self.key, repr(credentials))
        self.store_key("synthetic-rotated")
        credentials = await self.loader().load()
        self.assertEqual((credentials.api_key, credentials.revision), ("synthetic-rotated", 2))

    async def test_invalid_identity_scope_and_disabled_user_reject_before_decrypt(self):
        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
            wraps=decrypt_credentials_snapshot,
        ) as decrypt:
            for options in (
                {
                    "caller": replace(
                        self.caller, user_id=self.other, agent_owner_user_id=self.other
                    )
                },
                {"caller": replace(self.caller, is_group=True)},
                {"connector_id": "other"},
                {"caller": replace(self.caller, authorization_revision=2)},
            ):
                with self.subTest(options=options), self.assertRaises(ValueError):
                    await self.loader(**options).load()
            with self.pool.transaction() as conn:
                conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
            with self.assertRaises(ValueError):
                await self.loader().load()
            decrypt.assert_not_called()

    async def test_rotation_revocation_and_master_changes_during_decrypt_deny_return(self):
        for change in ("rotation", "owner", "binding", "master", "delete_master"):
            with self.subTest(change=change):
                self.restore_master()
                self.store_key(self.key)
                with self.pool.transaction() as conn:
                    conn.execute("UPDATE users SET disabled=0 WHERE id=?", (self.owner,))
                    conn.execute("UPDATE memorylake_bindings SET enabled=1")

                def decrypt(master, blob, change=change):
                    result = decrypt_credentials_snapshot(master, blob)
                    if change == "rotation":
                        self.store_key("synthetic-new-key")
                    else:
                        with self.pool.transaction() as conn:
                            if change == "owner":
                                conn.execute(
                                    "UPDATE users SET disabled=1 WHERE id=?", (self.owner,)
                                )
                            elif change == "binding":
                                conn.execute("UPDATE memorylake_bindings SET enabled=0")
                            elif change == "master":
                                conn.execute(
                                    "UPDATE secrets SET v=? WHERE k=?",
                                    (base64.urlsafe_b64encode(b"n" * 32), PURPOSE_CONNECTOR),
                                )
                            else:
                                conn.execute("DELETE FROM secrets WHERE k=?", (PURPOSE_CONNECTOR,))
                    return result

                with (
                    patch(
                        "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
                        side_effect=decrypt,
                    ),
                    self.assertRaises(ValueError) as caught,
                ):
                    await self.loader().load()
                self.assertNotIn(self.key, str(caught.exception))
                self.assertNotIn(self.master.decode(), str(caught.exception))

    async def test_missing_master_malformed_cipher_and_key_are_body_free_without_bootstrap(self):
        for kind in (
            "missing_master",
            "malformed_master",
            "malformed_cipher",
            "blank_key",
            "dict_key",
            "nul_key",
            "wrong_instance",
        ):
            with self.subTest(kind=kind):
                self.restore_master()
                self.store_key(self.key)
                if kind == "missing_master":
                    with self.pool.transaction() as conn:
                        conn.execute("DELETE FROM secrets WHERE k=?", (PURPOSE_CONNECTOR,))
                elif kind == "malformed_master":
                    with self.pool.transaction() as conn:
                        conn.execute(
                            "UPDATE secrets SET v=? WHERE k=?",
                            (b"synthetic-malformed-sensitive-master", PURPOSE_CONNECTOR),
                        )
                elif kind == "malformed_cipher":
                    self.connectors.upsert_credentials(
                        instance_id="connector", blob=b"synthetic-sensitive-cipher"
                    )
                elif kind == "wrong_instance":
                    self.store_key(self.key, instance_id="other")
                else:
                    self.store_key(
                        {
                            "blank_key": "  ",
                            "dict_key": {"secret": self.key},
                            "nul_key": "synthetic\0secret",
                        }[kind]
                    )
                with (
                    patch.object(
                        SecretRepo,
                        "get_or_create",
                        side_effect=AssertionError("must not bootstrap"),
                    ),
                    self.assertRaises(ValueError) as caught,
                ):
                    await self.loader().load()
                self.assertNotIn("synthetic", str(caught.exception))
                if kind == "missing_master":
                    self.assertIsNone(SecretRepo(self.pool).get(PURPOSE_CONNECTOR))

    async def test_cancelled_decrypt_holds_actual_executor_quota_until_finish(self):
        started, release = threading.Event(), threading.Event()

        def blocked(master, blob):
            started.set()
            release.wait(3)
            return decrypt_credentials_snapshot(master, blob)

        with patch(
            "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
            side_effect=blocked,
        ):
            task = asyncio.create_task(self.loader().load())
            jobs = ()
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                jobs = tuple(self.admission._jobs)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(len(jobs), 1)
                self.assertFalse(jobs[0].done())
                with self.assertRaises(ValueError):
                    await self.loader().load()
                self.assertEqual(tuple(self.admission._jobs), jobs)
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
                await asyncio.gather(*jobs, return_exceptions=True)
        self.assertEqual((await self.loader().load()).api_key, self.key)

    async def test_invocation_closed_during_decrypt_never_returns_key(self):
        def decrypt(master, blob):
            result = decrypt_credentials_snapshot(master, blob)
            self.active = False
            return result

        with (
            patch(
                "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
                side_effect=decrypt,
            ),
            self.assertRaises(ValueError) as caught,
        ):
            await self.loader().load()
        self.assertNotIn("synthetic", str(caught.exception))

    async def test_expiry_crossed_after_final_worker_snapshot_rejects_key_delivery(self):
        with self.pool.transaction() as conn:
            conn.execute(
                "UPDATE connectors SET credential_expires_at=101 WHERE instance_id='connector'"
            )
        now = [100.0]
        loader = self.loader()
        original = loader._current

        def crosses_expiry():
            result = original()
            now[0] = 101.0
            return result

        with (
            patch(
                "octop.infra.connectors.memorylake.storage.time.time", side_effect=lambda: now[0]
            ),
            patch.object(loader, "_current", side_effect=crosses_expiry),
            self.assertRaises(ValueError) as caught,
        ):
            await loader.load()
        self.assertNotIn(self.key, str(caught.exception))
