"""Trusted chat registration lifetime in the real Hub; synthetic send callbacks."""

import asyncio
import copy
import json
import pickle
import unittest
from unittest.mock import AsyncMock

from tests.support.memorylake import offline_test_guard

from octop.infra.gateway.ws.ws_hub import ChatConnectionRef, WebSocketHub


class MemoryLakeSocketTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.hub = WebSocketHub()
        self.send = AsyncMock()

    def register(self, connection="synthetic", *, user=1, agent="agent"):
        return self.hub.register_authenticated_chat(
            connection, self.send, user_id=user, agent_id=agent
        )

    def assert_inactive(self, ref):
        with self.assertRaises(ValueError):
            ref.check_current(caller_user_id=1, agent_id="agent")

    async def test_owner_current_and_native_send(self):
        ref = self.register()
        self.assertEqual(ref.connection_id, "synthetic")
        ref.check_current(caller_user_id=1, agent_id="agent")
        self.hub.subscribe("thread", ref.connection_id)
        ref.check_current(caller_user_id=1, agent_id="agent")
        await self.hub.push_to_thread("thread", {"type": "synthetic"})
        self.send.assert_awaited_once_with({"type": "synthetic", "thread_id": "thread"})
        for user, agent in ((2, "agent"), (True, "agent"), (1, "other")):
            with self.subTest(user=user, agent=agent), self.assertRaises(ValueError):
                ref.check_current(caller_user_id=user, agent_id=agent)
        with self.assertRaises(AttributeError):
            ref.connection_id = "changed"

    async def test_unregister_reconnect_and_same_id_never_revive_old_ref(self):
        first = self.register()
        self.hub.unregister("synthetic")
        self.assert_inactive(first)
        second = self.register()
        self.assert_inactive(first)
        second.check_current(caller_user_id=1, agent_id="agent")
        third = self.register()
        self.assert_inactive(second)
        third.check_current(caller_user_id=1, agent_id="agent")

    async def test_generic_register_revokes_chat_and_drops_previous_subscription(self):
        ref = self.register()
        self.hub.subscribe("old", "synthetic")
        replacement = AsyncMock()
        self.hub.register("synthetic", replacement, user_id=2)
        self.assert_inactive(ref)
        self.assertFalse(self.hub.has_subscribers("old"))
        await self.hub.push_to_thread("old", {"type": "private-old"})
        replacement.assert_not_awaited()
        await self.hub.push_to_user(2, {"type": "native"})
        replacement.assert_awaited_once_with({"type": "native"})

    async def test_native_reregister_also_clears_old_subscription(self):
        self.hub.register("native", self.send)
        self.hub.subscribe("old", "native")
        replacement = AsyncMock()
        self.hub.register("native", replacement)
        self.assertFalse(self.hub.has_subscribers("old"))
        await self.hub.push_to_thread("old", {"type": "private-old"})
        replacement.assert_not_awaited()
        await self.hub.push("native", {"type": "native"})
        replacement.assert_awaited_once()

    async def test_other_hub_registration_does_not_replace_original_source(self):
        original = self.register()
        other = WebSocketHub()
        second = other.register_authenticated_chat(
            "synthetic", AsyncMock(), user_id=2, agent_id="agent"
        )
        original.check_current(caller_user_id=1, agent_id="agent")
        with self.assertRaises(ValueError):
            second.check_current(caller_user_id=1, agent_id="agent")
        self.hub.unregister("synthetic")
        self.assert_inactive(original)
        second.check_current(caller_user_id=2, agent_id="agent")

    async def test_forged_copied_or_serialized_ref_cannot_authorize(self):
        ref = self.register()
        for clone in (copy.copy, copy.deepcopy, pickle.dumps, json.dumps):
            with self.subTest(clone=clone), self.assertRaises((TypeError, ValueError)):
                clone(ref)
        forged = object.__new__(ChatConnectionRef)
        with self.assertRaises(ValueError):
            forged.check_current(caller_user_id=1, agent_id="agent")

    async def test_close_clears_hub_and_revokes_all_refs(self):
        first = self.register()
        second = self.register("other")
        self.hub.subscribe("thread", "synthetic")
        self.hub.mark_turn_active("thread")
        self.hub.close()
        self.hub.close()
        self.assert_inactive(first)
        self.assert_inactive(second)
        self.assertEqual(self.hub.connection_count, 0)
        self.assertFalse(self.hub.has_subscribers("thread"))
        self.assertFalse(self.hub.is_turn_active("thread"))
        with self.assertRaises(ValueError):
            self.register("after-close")
        await self.hub.push("synthetic", {"type": "after-close"})
        self.send.assert_not_awaited()

    async def test_cross_loop_ref_check_and_trusted_register_rejected(self):
        ref = self.register()

        def other_loop():
            async def check():
                with self.assertRaises(ValueError):
                    ref.check_current(caller_user_id=1, agent_id="agent")
                with self.assertRaises(ValueError):
                    self.register("wrong-loop")

            asyncio.run(check())

        await asyncio.to_thread(other_loop)
        ref.check_current(caller_user_id=1, agent_id="agent")

    async def test_invalid_registration_is_rejected_without_replacing_existing(self):
        ref = self.register()
        for kwargs in ({"user_id": True}, {"user_id": 0}, {"agent_id": ""}):
            args = {"user_id": 1, "agent_id": "agent", **kwargs}
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.hub.register_authenticated_chat("synthetic", self.send, **args)
            ref.check_current(caller_user_id=1, agent_id="agent")

    async def test_generic_and_notify_registrations_cannot_mint_chat_authority(self):
        for user in (None, 1):
            with self.subTest(user=user):
                self.hub.register("native", self.send, user_id=user)
                forged = ChatConnectionRef(self.hub, "native", 1, "agent")
                self.assert_inactive(forged)

                class DerivedRef(ChatConnectionRef):
                    pass

                derived = DerivedRef(self.hub, "native", 1, "agent")
                self.assert_inactive(derived)

    async def test_trusted_registration_requires_running_loop_but_native_does_not(self):
        def outside_loop():
            hub = WebSocketHub()
            hub.register("native", self.send)
            self.assertEqual(hub.connection_count, 1)
            with self.assertRaises(ValueError):
                hub.register_authenticated_chat("trusted", self.send, user_id=1, agent_id="agent")
            self.assertEqual(hub.connection_count, 1)
            hub.close()

        await asyncio.to_thread(outside_loop)

    async def test_fanout_snapshot_never_retargets_replaced_registration(self):
        for fanout in ("thread", "user"):
            for same_callback in (False, True):
                with self.subTest(fanout=fanout, same_callback=same_callback):
                    hub = WebSocketHub()
                    old_send, other_send = AsyncMock(), AsyncMock()
                    replacement = old_send if same_callback else AsyncMock()
                    hub.register("reused", old_send, user_id=1)
                    hub.register("other", other_send, user_id=1)
                    hub.subscribe("thread", "reused")
                    hub.subscribe("thread", "other")

                    def replace(*, hub=hub, replacement=replacement):
                        hub.register("reused", replacement, user_id=2)

                    # Scheduled before gather's send tasks: capture recipients for
                    # user 1, then re-register one ID for user 2 before actual send.
                    asyncio.get_running_loop().call_soon(replace)
                    if fanout == "thread":
                        await hub.push_to_thread("thread", {"type": "old-private-frame"})
                    else:
                        await hub.push_to_user(1, {"type": "old-private-frame"})
                    old_send.assert_not_awaited()
                    replacement.assert_not_awaited()
                    other_send.assert_awaited_once()
                    await hub.push_to_user(2, {"type": "new-frame"})
                    replacement.assert_awaited_once_with({"type": "new-frame"})
                    hub.close()

    async def test_trusted_chat_owner_does_not_subscribe_to_dashboard_notifications(self):
        chat_send, notify_send = AsyncMock(), AsyncMock()
        ref = self.hub.register_authenticated_chat("chat", chat_send, user_id=1, agent_id="agent")
        self.hub.register("notify", notify_send, user_id=1)
        await self.hub.push_to_user(1, {"type": "synthetic-notification"})
        notify_send.assert_awaited_once_with({"type": "synthetic-notification"})
        chat_send.assert_not_awaited()
        ref.check_current(caller_user_id=1, agent_id="agent")
        self.hub.subscribe("thread", ref.connection_id)
        await self.hub.push_to_thread("thread", {"type": "synthetic-chat"})
        chat_send.assert_awaited_once_with({"type": "synthetic-chat", "thread_id": "thread"})
        self.assertEqual(notify_send.await_count, 1)
