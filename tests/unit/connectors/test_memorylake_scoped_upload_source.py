"""One shared store, server-bound namespaces, synthetic bytes only."""

import asyncio
import unittest
from dataclasses import replace

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.upload_source import EphemeralUploadSource


class ScopedUploadSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.caller = CallerContext(1, "agent", 1, 1)
        self.calls = []
        self.now = 100.0

    async def asyncSetUp(self):
        async def deny_unscoped(caller):
            raise AssertionError("raw authorization must not be used")

        self.store = EphemeralUploadSource(
            authorize=deny_unscoped,
            clock=lambda: self.now,
            ttl_s=2,
            max_total_bytes=6,
            max_file_bytes=3,
            max_files=2,
            max_pending_reads=1,
        )
        self.addAsyncCleanup(self.cleanup)

    async def cleanup(self):
        self.store.close()

    def scope(self, binding="binding-a", connector="connector-a", authorize=None, caller=None):
        async def fresh(who):
            self.calls.append((binding, connector, who))

        return self.store.bind(
            binding_id=binding,
            connector_id=connector,
            caller=caller or self.caller,
            authorize=authorize or fresh,
        )

    async def test_same_revision_cross_binding_connector_and_raw_reads_rejected(self):
        first = self.scope()
        key = await first.add(self.caller, "s.txt", b"abc")
        for other in (
            self.scope("binding-b"),
            self.scope(connector="connector-b"),
            self.store,
        ):
            with self.subTest(source=type(other).__name__), self.assertRaises(ValueError):
                await other.read(self.caller, key)
        self.assertEqual(len(self.calls), 1)  # Reject routing before even checking auth.
        self.assertEqual((await first.read(self.caller, key)).content, b"abc")
        self.assertEqual((await self.scope().read(self.caller, key)).content, b"abc")

    async def test_fixed_caller_and_closed_scope_deny_before_authorization(self):
        scope = self.scope()
        key = await scope.add(self.caller, "s.txt", b"abc")
        for caller in (
            replace(self.caller, user_id=2, agent_owner_user_id=2),
            replace(self.caller, authorization_revision=2),
            replace(self.caller, is_group=True),
        ):
            with self.subTest(caller=caller), self.assertRaises(ValueError):
                await scope.add(caller, "s.txt", b"x")
        scope.close()
        with self.assertRaises(ValueError):
            await scope.read(self.caller, key)
        self.assertEqual(len(self.calls), 1)
        # Closing a request scope neither clears the shared store nor grants a
        # new scope: this replacement still needs a current trusted authorizer.
        self.assertEqual((await self.scope().read(self.caller, key)).content, b"abc")

    async def test_two_scopes_share_total_bytes_files_and_pending_capture_reservation(self):
        first, second = self.scope(), self.scope("binding-b", "connector-b")
        one = await first.add(self.caller, "s.txt", b"abc")
        two = await second.add(self.caller, "s.txt", b"abc")
        with self.assertRaises(ValueError):
            await second.add(self.caller, "s.txt", b"x")
        await first.revoke(self.caller, one)
        await second.revoke(self.caller, two)
        entered, release = asyncio.Event(), asyncio.Event()

        async def read(max_bytes):
            self.assertEqual(max_bytes, 3)
            entered.set()
            await release.wait()
            return b"abc"

        capture = asyncio.create_task(first.capture(self.caller, "s.txt", read))
        try:
            await entered.wait()
            self.assertEqual(self.store._pending_bytes, 6)
            with self.assertRaises(ValueError):
                await second.add(self.caller, "s.txt", b"x")
        finally:
            release.set()
            await capture

    async def test_pending_read_limit_is_shared_and_close_during_auth_does_not_disclose(self):
        key_a = await self.scope().add(self.caller, "a.txt", b"abc")
        second = self.scope("binding-b", "connector-b")
        key_b = await second.add(self.caller, "b.txt", b"abc")
        entered, release = asyncio.Event(), asyncio.Event()

        async def blocked(caller):
            entered.set()
            await release.wait()

        first = self.scope(authorize=blocked)
        reading = asyncio.create_task(first.read(self.caller, key_a))
        try:
            await entered.wait()
            with self.assertRaises(ValueError):
                await second.read(self.caller, key_b)
            first.close()
            release.set()
            with self.assertRaises(ValueError):
                await reading
        finally:
            release.set()
            await asyncio.gather(reading, return_exceptions=True)
        self.assertEqual((await second.read(self.caller, key_b)).content, b"abc")

    async def test_stale_scope_auth_failure_expiry_and_store_close_remain_fail_closed(self):
        scope = self.scope()
        key = await scope.add(self.caller, "s.txt", b"abc")

        async def denied(caller):
            raise ValueError("sensitive stale binding")

        with self.assertRaises(ValueError) as caught:
            await self.scope(authorize=denied).read(self.caller, key)
        self.assertNotIn("sensitive", str(caught.exception))
        self.assertFalse(self.store._entries)
        key = await scope.add(self.caller, "s.txt", b"abc")
        self.now += 3
        with self.assertRaises(ValueError):
            await scope.read(self.caller, key)
        key = await scope.add(self.caller, "s.txt", b"abc")
        self.store.close()
        with self.assertRaises(ValueError):
            await scope.read(self.caller, key)

    async def test_invalid_scope_identifiers_and_callback_rejected(self):
        for binding, connector, authorize in (
            ("bad\nvalue", "connector", lambda caller: None),
            ("binding", "bad/path", lambda caller: None),
            ("binding", "connector", False),
        ):
            with self.subTest(binding=binding), self.assertRaises(ValueError):
                self.store.bind(
                    binding_id=binding,
                    connector_id=connector,
                    caller=self.caller,
                    authorize=authorize,
                )

    async def test_scoped_view_cannot_read_a_legacy_unscoped_entry(self):
        async def authorize(caller):
            return None

        self.store._authorize = authorize
        key = await self.store.add(self.caller, "s.txt", b"abc")
        with self.assertRaises(ValueError):
            await self.scope().read(self.caller, key)
        self.assertEqual((await self.store.read(self.caller, key)).content, b"abc")

    async def test_cancel_or_close_one_read_view_does_not_delete_another_views_upload(self):
        for action in ("cancel", "close"):
            with self.subTest(action=action):
                survivor = self.scope()
                key = await survivor.add(self.caller, "s.txt", b"abc")
                entered, release = asyncio.Event(), asyncio.Event()

                async def blocked(caller, entered=entered, release=release):
                    entered.set()
                    await release.wait()

                temporary = self.scope(authorize=blocked)
                task = asyncio.create_task(temporary.read(self.caller, key))
                try:
                    await entered.wait()
                    if action == "cancel":
                        task.cancel()
                    else:
                        temporary.close()
                    release.set()
                    with self.assertRaises(
                        asyncio.CancelledError if action == "cancel" else ValueError
                    ):
                        await task
                    self.assertIn(key, self.store._entries)
                    self.assertEqual(self.store._bytes, 3)
                    self.assertEqual((await survivor.read(self.caller, key)).content, b"abc")
                    await survivor.revoke(self.caller, key)
                    with self.assertRaises(ValueError):
                        await self.scope().read(self.caller, key)
                finally:
                    release.set()
                    await asyncio.gather(task, return_exceptions=True)
