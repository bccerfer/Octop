"""Shared real-worker admission and atomic thread credentials for dashboard reads."""

import asyncio
import threading
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from tests.unit.connectors import test_memorylake_storage as fixtures

from octop.infra.connectors.crypto import decrypt_credentials_snapshot
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.repos.threads import ThreadRepo


class ReaderAdmissionTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.StoredMemoryLakeReaderTests.setUp
    tearDown = fixtures.StoredMemoryLakeReaderTests.tearDown
    asyncTearDown = fixtures.StoredMemoryLakeReaderTests.asyncTearDown
    make_reader = fixtures.StoredMemoryLakeReaderTests.make_reader
    store_key = fixtures.StoredMemoryLakeReaderTests.store_key

    async def asyncSetUp(self):
        self.addAsyncCleanup(self.database.aclose)
        ThreadRepo(self.pool).insert(
            thread_id="thread",
            agent_id="agent",
            user_id=self.owner,
            channel_type="dashboard",
            session_key=f"agent:dashboard:{self.owner}:dm",
        )
        self.first = self.make_reader(database=self.database, dashboard_thread_id="thread")
        self.second = self.make_reader(database=self.database, dashboard_thread_id="thread")

    async def test_shared_worker_quota_survives_cancellation_until_thread_completes(self):
        started, release = threading.Event(), threading.Event()
        original = self.repo.load_owned_active

        def blocked(*args, **kwargs):
            started.set()
            release.wait(3)
            return original(*args, **kwargs)

        with patch.object(self.repo, "load_owned_active", side_effect=blocked):
            task = asyncio.create_task(self.first.call("search_documents", {"query": "q"}))
            try:
                for _ in range(100):
                    if started.is_set():
                        break
                    await asyncio.sleep(0.001)
                self.assertTrue(started.is_set())
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(len(self.database._jobs), 1)
                with self.assertRaises(ValueError):
                    await self.second.call("search_documents", {"query": "q"})
                self.assertEqual(self.calls, [])
            finally:
                release.set()
                await asyncio.gather(*tuple(self.database._jobs), return_exceptions=True)
                await asyncio.gather(task, return_exceptions=True)
        result = await self.second.call("search_documents", {"query": "q"})
        self.assertTrue(result["untrusted"])

    async def test_thread_revoked_during_decrypt_never_discloses_credentials(self):
        def revoke(master, blob):
            payload = decrypt_credentials_snapshot(master, blob)
            with self.pool.transaction() as conn:
                conn.execute("UPDATE threads SET user_id=? WHERE thread_id='thread'", (self.other,))
            return payload

        with (
            patch(
                "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
                side_effect=revoke,
            ),
            self.assertRaises(ValueError),
        ):
            await self.first._load_credentials()
        self.assertEqual(self.calls, [])

    async def test_master_rotation_during_decode_disallows_old_plaintext(self):
        def rotate(master, blob):
            payload = decrypt_credentials_snapshot(master, blob)
            self.secrets.rotate(PURPOSE_CONNECTOR, Fernet.generate_key())
            return payload

        with (
            patch(
                "octop.infra.connectors.memorylake.credentials.decrypt_credentials_snapshot",
                side_effect=rotate,
            ),
            self.assertRaises(ValueError),
        ):
            await self.first._load_credentials()
        self.assertEqual(self.calls, [])

    async def test_live_thread_read_uses_atomic_credentials_not_legacy_secret_get(self):
        with patch.object(
            self.secrets, "get", side_effect=AssertionError("separate master lookup")
        ):
            result = await self.first.call("search_documents", {"query": "q"})
        self.assertTrue(result["untrusted"])
        self.assertEqual(len(self.calls), 1)

    async def test_share_committed_before_admission_denies_material_and_marker(self):
        from octop.infra.db.repos.agents import AgentRepo

        original = self.repo.admit_agent_material

        def share_first(**kwargs):
            AgentRepo(self.pool).set_shared("agent", True)
            return original(**kwargs)

        with (
            patch.object(self.repo, "admit_agent_material", side_effect=share_first),
            self.assertRaises(ValueError),
        ):
            await self.first.call("search_documents", {"query": "q"})
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_agent_admissions").fetchone()[0], 0
            )

    async def test_cancelled_marker_worker_keeps_quota_and_may_commit_conservative_marker(self):
        started, release = threading.Event(), threading.Event()
        original = self.repo.admit_agent_material

        def blocked(**kwargs):
            started.set()
            release.wait(3)
            return original(**kwargs)

        with patch.object(self.repo, "admit_agent_material", side_effect=blocked):
            task = asyncio.create_task(self.first.call("search_documents", {"query": "q"}))
            try:
                for _ in range(200):
                    if started.is_set():
                        break
                    await asyncio.sleep(0.001)
                self.assertTrue(started.is_set())
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(len(self.database._jobs), 1)
            finally:
                release.set()
                await asyncio.gather(*tuple(self.database._jobs), return_exceptions=True)
                await asyncio.gather(task, return_exceptions=True)
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_agent_admissions").fetchone()[0], 1
            )
