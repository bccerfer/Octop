"""Synthetic new uploads only: no filesystem, service, or native attachment access."""

import asyncio
import unittest
import weakref
from dataclasses import replace

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.upload_source import EphemeralUploadSource


class UploadSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def asyncSetUp(self):
        self.now = 100.0
        self.caller = CallerContext(1, "agent", 1, 1)
        self.calls = []

        async def authorize(caller):
            self.calls.append(caller)

        self.authorize = authorize
        self.source = self.make_source()
        self.addAsyncCleanup(self.cleanup)

    def make_source(self, **kwargs):
        return EphemeralUploadSource(
            authorize=kwargs.pop("authorize", self.authorize), clock=lambda: self.now, **kwargs
        )

    async def cleanup(self):
        self.source.close()

    async def test_new_bytes_repeat_read_prepare_submit_contract(self):
        key = await self.source.add(self.caller, "synthetic.txt", b"abc")
        self.assertRegex(key, r"^[A-Za-z0-9][A-Za-z0-9:_-]{0,127}$")
        first = await self.source.read(self.caller, key)
        second = await self.source.read(self.caller, key)
        self.assertEqual(first, second)
        self.assertEqual((first.filename, first.content), ("synthetic.txt", b"abc"))
        self.assertEqual(len(self.calls), 3)
        self.assertNotIn("abc", repr(first))

    async def test_identity_group_shared_and_path_inputs_rejected(self):
        key = await self.source.add(self.caller, "synthetic.txt", b"abc")
        for caller in (
            replace(self.caller, user_id=2),
            replace(self.caller, agent_id="peer"),
            replace(self.caller, agent_owner_user_id=2),
            replace(self.caller, is_group=True),
            replace(self.caller, authorization_revision=2),
        ):
            with self.subTest(caller=caller), self.assertRaises(ValueError):
                await self.source.read(caller, key)
        self.assertEqual((await self.source.read(self.caller, key)).content, b"abc")
        for name, content in (
            ("../private", b"x"),
            ("bad\nname", b"x"),
            ("ok", bytearray(b"x")),
            ("ok", b""),
        ):
            with self.subTest(name=name), self.assertRaises(ValueError):
                await self.source.add(self.caller, name, content)

    async def test_capacity_expiry_revoke_and_close_clear_owned_references(self):
        source = self.make_source(max_files=1, max_total_bytes=3, max_file_bytes=3, ttl_s=1)
        try:
            key = await source.add(self.caller, "s.txt", b"abc")
            with self.assertRaises(ValueError):
                await source.add(self.caller, "s.txt", b"x")
            self.now += 2
            with self.assertRaises(ValueError):
                await source.read(self.caller, key)
            self.assertFalse(source._entries)
            key = await source.add(self.caller, "s.txt", b"abc")
            await source.revoke(self.caller, key)
            with self.assertRaises(ValueError):
                await source.read(self.caller, key)
            await source.add(self.caller, "s.txt", b"abc")
            source.close()
            self.assertFalse(source._entries)
            with self.assertRaises(ValueError):
                await source.add(self.caller, "s.txt", b"x")
        finally:
            source.close()

    async def test_auth_failure_cancel_and_close_during_authorization_leave_no_snapshot(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def blocked(caller):
            started.set()
            await release.wait()

        for action in ("cancel", "close", "expire"):
            with self.subTest(action=action):
                started.clear()
                release.clear()
                source = self.make_source(authorize=blocked, ttl_s=1)
                task = asyncio.create_task(source.add(self.caller, "s.txt", b"abc"))
                try:
                    await started.wait()
                    if action == "cancel":
                        task.cancel()
                    elif action == "close":
                        source.close()
                    else:
                        self.now += 2
                    release.set()
                    with self.assertRaises(
                        asyncio.CancelledError if action == "cancel" else ValueError
                    ):
                        await task
                    self.assertFalse(source._entries)
                finally:
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)
                    source.close()

        async def denied(caller):
            raise ValueError("sensitive auth detail")

        source = self.make_source(authorize=denied)
        try:
            with self.assertRaises(ValueError) as caught:
                await source.add(self.caller, "s.txt", b"abc")
            self.assertNotIn("sensitive", str(caught.exception))
            self.assertFalse(source._entries)
        finally:
            source.close()

    async def test_pending_authorizations_reserve_bounded_capacity(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def blocked(caller):
            started.set()
            await release.wait()

        source = self.make_source(
            authorize=blocked, max_files=1, max_total_bytes=3, max_file_bytes=3
        )
        task = asyncio.create_task(source.add(self.caller, "s.txt", b"abc"))
        try:
            await started.wait()
            with self.assertRaises(ValueError):
                await source.add(self.caller, "s.txt", b"abc")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            release.set()
            self.assertTrue(await source.add(self.caller, "s.txt", b"abc"))
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            source.close()

    async def test_invalid_limits_and_clock_fail_closed(self):
        for kwargs in (
            {"ttl_s": 0},
            {"ttl_s": 301},
            {"ttl_s": float("nan")},
            {"max_files": True},
            {"max_pending_reads": 0},
            {"max_pending_reads": True},
            {"max_total_bytes": 0},
            {"max_file_bytes": 21 * 1024 * 1024},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.make_source(**kwargs)
        key = await self.source.add(self.caller, "s.txt", b"abc")
        for now in (float("nan"), float("inf"), True, "100", 99.0):
            with self.subTest(now=now):
                self.now = now
                with self.assertRaises(ValueError):
                    await self.source.read(self.caller, key)

    async def test_read_authorization_interleavings_do_not_disclose_snapshot(self):
        for action in ("deny", "close", "expire", "cancel"):
            with self.subTest(action=action):
                source = self.make_source(ttl_s=1)
                key = await source.add(self.caller, "s.txt", b"abc")
                started, release = asyncio.Event(), asyncio.Event()

                async def blocked(caller, action=action, started=started, release=release):
                    started.set()
                    await release.wait()
                    if action == "deny":
                        raise ValueError("synthetic private denial")

                source._authorize = blocked
                task = asyncio.create_task(source.read(self.caller, key))
                try:
                    await started.wait()
                    if action == "close":
                        source.close()
                    elif action == "expire":
                        self.now += 2
                    elif action == "cancel":
                        task.cancel()
                    release.set()
                    with self.assertRaises(
                        asyncio.CancelledError if action == "cancel" else ValueError
                    ):
                        await task
                    self.assertFalse(source._entries)
                finally:
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)
                    source.close()

    async def test_timer_removes_snapshot_without_further_access(self):
        source = self.make_source(ttl_s=0.01)
        try:
            await source.add(self.caller, "s.txt", b"abc")
            await asyncio.sleep(0.03)
            self.assertFalse(source._entries)
            self.assertEqual(source._bytes, 0)
        finally:
            source.close()

    async def test_suspended_read_does_not_retain_expired_or_closed_bytes(self):
        for action in ("expire", "close"):
            with self.subTest(action=action):
                source = self.make_source(ttl_s=1, max_total_bytes=3)
                key = await source.add(self.caller, "s.txt", b"abc")
                entry_ref = weakref.ref(source._entries[key])
                snapshot_ref = weakref.ref(source._entries[key].snapshot)
                started, release = asyncio.Event(), asyncio.Event()

                async def blocked(caller, started=started, release=release):
                    started.set()
                    await release.wait()

                source._authorize = blocked
                task = asyncio.create_task(source.read(self.caller, key))
                try:
                    await started.wait()
                    if action == "close":
                        source.close()
                    else:
                        self.now += 2
                        source._check(self.caller)
                    self.assertFalse(source._entries)
                    self.assertEqual(source._bytes, 0)
                    # Task remains suspended: neither Entry nor FileSnapshot may
                    # retain the old content outside the store's accounting.
                    self.assertIsNone(entry_ref())
                    self.assertIsNone(snapshot_ref())
                finally:
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)
                    source.close()

    async def test_pending_read_authorizations_are_bounded_and_release_on_cancel(self):
        source = self.make_source(max_pending_reads=1)
        key = await source.add(self.caller, "s.txt", b"abc")
        started, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def blocked(caller):
            calls.append(caller)
            started.set()
            await release.wait()

        source._authorize = blocked
        task = asyncio.create_task(source.read(self.caller, key))
        try:
            await started.wait()
            with self.assertRaisesRegex(ValueError, "upload_read_capacity_exceeded"):
                await source.read(self.caller, key)
            self.assertEqual(len(calls), 1)
            self.assertIn(key, source._entries)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(source._pending_reads, 0)
            release.set()
            new_key = await source.add(self.caller, "s.txt", b"abc")
            self.assertEqual((await source.read(self.caller, new_key)).content, b"abc")
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            source.close()

    async def test_authorization_wait_expires_without_manual_release(self):
        async def blocked(caller):
            await asyncio.Event().wait()

        source = self.make_source(authorize=blocked, ttl_s=0.02)
        try:
            with self.assertRaisesRegex(ValueError, "upload_authorization_timeout"):
                await asyncio.wait_for(source.add(self.caller, "s.txt", b"abc"), 0.3)
            self.assertEqual((source._pending_files, source._pending_bytes), (0, 0))
            self.assertFalse(source._entries)
        finally:
            source.close()

    async def test_uncooperative_auth_keeps_reservation_until_it_exits(self):
        cancelled, release = asyncio.Event(), asyncio.Event()

        async def blocked(caller):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()

        source = self.make_source(authorize=blocked, ttl_s=0.02, max_total_bytes=3)
        task = asyncio.create_task(source.add(self.caller, "s.txt", b"abc"))
        try:
            await asyncio.wait_for(cancelled.wait(), 0.3)
            self.assertFalse(task.done())
            self.assertEqual(source._pending_bytes, 3)
            with self.assertRaisesRegex(ValueError, "upload_capacity_exceeded"):
                await source.add(self.caller, "s.txt", b"abc")
            release.set()
            with self.assertRaisesRegex(ValueError, "upload_authorization_timeout"):
                await task
            self.assertFalse(source._entries)
            self.assertEqual(source._pending_bytes, 0)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            source.close()

    async def test_capture_reserves_before_auth_or_reader_and_rechecks_auth(self):
        source = self.make_source(max_file_bytes=3, max_total_bytes=6)
        reads = []

        async def reader(limit):
            reads.append(limit)
            return b"abc"

        try:
            key = await source.capture(self.caller, "s.txt", reader)
            self.assertEqual(reads, [3])
            self.assertEqual((await source.read(self.caller, key)).content, b"abc")
            with self.assertRaisesRegex(ValueError, "upload_capacity_exceeded"):
                await source.capture(self.caller, "s.txt", reader)
            self.assertEqual(reads, [3])
            await source.revoke(self.caller, key)

            async def deny(caller):
                raise ValueError("private denial")

            source._authorize = deny
            with self.assertRaisesRegex(ValueError, "upload_access_denied"):
                await source.capture(self.caller, "s.txt", reader)
            self.assertEqual(reads, [3])
            source._authorize = self.authorize

            async def revoke_during_read(limit):
                source._authorize = deny
                return b"abc"

            with self.assertRaisesRegex(ValueError, "upload_access_denied"):
                await source.capture(self.caller, "s.txt", revoke_during_read)
            self.assertFalse(source._entries)
            self.assertEqual((source._pending_files, source._pending_bytes), (0, 0))
        finally:
            source.close()

    async def test_capture_blocked_reader_lifecycle_and_shared_reservation(self):
        for action in ("cancel", "timeout", "close"):
            with self.subTest(action=action):
                source = self.make_source(max_file_bytes=3, max_total_bytes=6, ttl_s=0.04)
                started, release = asyncio.Event(), asyncio.Event()

                async def reader(limit, started=started, release=release):
                    started.set()
                    await release.wait()
                    return b"abc"

                task = asyncio.create_task(source.capture(self.caller, "s.txt", reader))
                try:
                    await started.wait()
                    self.assertEqual(source._pending_bytes, 6)
                    with self.assertRaisesRegex(ValueError, "upload_capacity_exceeded"):
                        await source.add(self.caller, "s.txt", b"x")
                    if action == "cancel":
                        task.cancel()
                    elif action == "close":
                        source.close()
                        self.assertEqual(source._pending_bytes, 6)
                        release.set()
                    with self.assertRaises(
                        asyncio.CancelledError if action == "cancel" else ValueError
                    ):
                        await asyncio.wait_for(task, 0.3)
                    self.assertFalse(source._entries)
                    self.assertEqual((source._pending_files, source._pending_bytes), (0, 0))
                finally:
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)
                    source.close()

    async def test_capture_rejects_invalid_reader_results_without_publication(self):
        for value in (b"", b"abcd", bytearray(b"abc")):
            with self.subTest(value=value):
                source = self.make_source(max_file_bytes=3, max_total_bytes=6)

                async def reader(limit, value=value):
                    return value

                try:
                    with self.assertRaisesRegex(ValueError, "invalid_upload_snapshot"):
                        await source.capture(self.caller, "s.txt", reader)
                    self.assertFalse(source._entries)
                    self.assertEqual(source._pending_bytes, 0)
                finally:
                    source.close()

    async def test_capture_uncooperative_reader_cannot_release_quota_or_publish_late(self):
        source = self.make_source(max_file_bytes=3, max_total_bytes=6, ttl_s=0.02)
        cancelled, release = asyncio.Event(), asyncio.Event()

        async def reader(limit):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return b"abc"

        task = asyncio.create_task(source.capture(self.caller, "s.txt", reader))
        try:
            await asyncio.wait_for(cancelled.wait(), 0.3)
            self.assertFalse(task.done())
            self.assertEqual(source._pending_bytes, 6)
            with self.assertRaisesRegex(ValueError, "upload_capacity_exceeded"):
                await source.add(self.caller, "s.txt", b"x")
            release.set()
            with self.assertRaisesRegex(ValueError, "upload_capture_timeout"):
                await task
            self.assertFalse(source._entries)
            self.assertEqual(source._pending_bytes, 0)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            source.close()
