"""Budgeted parser callback composition: synthetic bytes, no HTTP/network/files."""

import asyncio
import unittest

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.upload_source import EphemeralUploadSource


class NamedCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(offline_test_guard())
        self.caller = CallerContext(1, "agent", 1, 1)
        self.authorized = True
        self.events = []

        async def authorize(caller):
            self.events.append("authorize")
            if not self.authorized:
                raise ValueError("private authorization detail")

        self.store = EphemeralUploadSource(
            authorize=authorize, max_file_bytes=3, max_total_bytes=65_545
        )

        async def cleanup():
            self.store.close()

        self.addAsyncCleanup(cleanup)
        self.source = self.store.bind(
            binding_id="binding",
            connector_id="connector",
            caller=self.caller,
            authorize=authorize,
        )

    async def test_filename_discovered_after_budget_and_authorization(self):
        async def receive(maximum):
            self.events.append("receive")
            self.assertEqual(maximum, 3)
            self.assertEqual(self.store._pending_bytes, 65_545)
            self.assertEqual(self.store._pending_files, 1)
            return "中文.txt", b"abc"

        key = await self.source.capture_named(self.caller, receive)
        self.assertEqual(self.events, ["authorize", "receive", "authorize"])
        self.assertEqual(self.store._pending_bytes, 0)
        self.assertEqual(self.store._bytes, 3)
        snapshot = await self.source.read(self.caller, key)
        self.assertEqual((snapshot.filename, snapshot.content), ("中文.txt", b"abc"))

    async def test_parallel_capacity_denies_before_second_receive_and_cancel_releases(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def receive(maximum):
            calls.append(maximum)
            entered.set()
            await release.wait()
            return "x.txt", b"abc"

        task = asyncio.create_task(self.source.capture_named(self.caller, receive))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with self.assertRaisesRegex(ValueError, "upload_capacity_exceeded"):
                await self.source.capture_named(self.caller, receive)
            self.assertEqual(calls, [3])
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(self.store._pending_bytes, 0)
            self.assertFalse(self.store._entries)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_invalid_name_content_or_late_revocation_never_publishes(self):
        for result in (
            ("../x", b"abc"),
            ("x", b""),
            ("x", b"abcd"),
            ("x", bytearray(b"a")),
            b"private",
        ):

            async def receive(maximum, result=result):
                return result

            with self.subTest(result_type=type(result).__name__), self.assertRaises(ValueError):
                await self.source.capture_named(self.caller, receive)
            self.assertEqual(self.store._pending_bytes, 0)
            self.assertFalse(self.store._entries)

        async def revoke(maximum):
            self.authorized = False
            return "x", b"abc"

        with self.assertRaises(ValueError):
            await self.source.capture_named(self.caller, revoke)
        self.assertFalse(self.store._entries)
        self.assertEqual(self.store._pending_bytes, 0)
