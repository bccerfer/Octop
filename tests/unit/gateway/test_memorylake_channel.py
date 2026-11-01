"""Dormant front-of-channel adapter, not registration or GlobalProcessor enablement."""

import asyncio
import unittest
from contextlib import contextmanager
from contextvars import ContextVar
from unittest.mock import AsyncMock, MagicMock, patch

from octop_gateway.models import ChannelSubject, ImageContent, InboundMessage, TextContent
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.delivery import DashboardDelivery
from octop.infra.connectors.memorylake.origin import (
    DashboardInbound,
    DashboardOriginError,
    DashboardOriginRegistry,
)
from octop.infra.gateway.process.message_keys import INBOUND_ATTACHMENTS_KEY
from octop.infra.gateway.ws.memorylake import MemoryLakeWebSocketChannel

_CURRENT = ContextVar("memorylake_channel_test_identity", default=None)


class DerivedInbound(DashboardInbound):
    pass


class Identities:
    def owns_dashboard_thread(self, **kwargs):
        return True


class MemoryLakeChannelTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.registry = DashboardOriginRegistry(identities=Identities())
        self.delivery = DashboardDelivery(self.registry)
        self.hub = MagicMock()
        self.hub.push = AsyncMock()
        self.hub.push_to_thread = AsyncMock()
        self.identities = []
        self.scope_exits = []
        self.processed = []
        self.tasks = []
        self.receipts = []
        self.entered = asyncio.Event()
        self.mode = "normal"
        outer = self

        class Processor:
            async def iter_turn_chunks(self, message):
                identity = _CURRENT.get()
                if isinstance(message, DashboardInbound):
                    identity.check_current(agent_id="agent", thread_id="thread")
                outer.processed.append(message)
                outer.entered.set()
                if outer.mode == "cancel":
                    await asyncio.Event().wait()
                if outer.mode == "error":
                    raise RuntimeError("synthetic processor failure")
                yield {"type": "token", "content": "synthetic"}
                yield {"type": "done"}

        self.channel = MemoryLakeWebSocketChannel(
            Processor(), hub=self.hub, delivery=self.delivery, turn_scope=self.scope
        )
        self.channel.set_media_backend(object())
        self.media = self.enterContext(
            patch.object(self.channel, "_persist_media", new=AsyncMock())
        )

    async def asyncTearDown(self):
        self.delivery.close()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    @contextmanager
    def scope(self, identity):
        self.identities.append(identity)
        identity.check_current(agent_id="agent", thread_id="thread")
        if self.mode == "scope_error":
            raise RuntimeError("synthetic scope entry failure")
        token = _CURRENT.set(identity)
        try:
            yield
        finally:
            # Closing after context reset would leave inherited child contexts live.
            with self.assertRaises(DashboardOriginError):
                identity.check_current(agent_id="agent", thread_id="thread")
            self.scope_exits.append(identity)
            _CURRENT.reset(token)

    def message(self, cls=DashboardInbound):
        return cls(
            channel_id="octop-dashboard",
            channel_type="dashboard",
            tenant_id="agent",
            channel_subject=ChannelSubject(subject_id="1"),
            channel_session_id="agent:dashboard:1:dm",
            content=[TextContent(text="synthetic")],
            metadata={
                "thread_id": "thread",
                "session_key": "agent:dashboard:1:dm",
                "ws_connection_id": "synthetic-connection",
            },
        )

    async def dispatch(self, message, enqueue=None, timeout=2):
        def start(msg):
            task = asyncio.create_task(self.channel.handle_inbound(msg))
            self.tasks.append(task)

        receipt = await self.delivery.dispatch(
            message,
            caller_user_id=1,
            agent_id="agent",
            thread_id="thread",
            enqueue=start if enqueue is None else enqueue,
            timeout_s=timeout,
        )
        self.receipts.append(receipt.status)
        return receipt

    def assert_no_channel_effects(self, message, original):
        self.assertIs(message.channel_subject, original)
        self.media.assert_not_awaited()
        self.assertEqual(self.hub.mock_calls, [])
        self.assertEqual(self.processed, [])
        self.assertEqual(self.identities, [])

    async def test_unissued_and_subclass_denied_before_base_side_effects(self):
        for cls in (DashboardInbound, DerivedInbound):
            with self.subTest(carrier=cls):
                msg = self.message(cls)
                subject = msg.channel_subject
                with self.assertRaises(ValueError):
                    await self.channel.handle_inbound(msg)
                self.assert_no_channel_effects(msg, subject)

    async def test_timeout_revoked_late_delivery_has_no_base_side_effects(self):
        msg = self.message()
        subject = msg.channel_subject
        with self.assertRaises(ValueError):
            await self.dispatch(msg, enqueue=lambda _: None, timeout=0.01)
        with self.assertRaises(ValueError):
            await self.channel.handle_inbound(msg)
        self.assert_no_channel_effects(msg, subject)

    async def test_commit_precedes_base_effects_and_normal_exit_closes_identity(self):
        msg = self.message()
        subject = msg.channel_subject

        async def media(message):
            self.assertEqual(self.receipts, ["accepted"])
            _CURRENT.get().check_current(agent_id="agent", thread_id="thread")

        self.media.side_effect = media
        self.assertEqual((await self.dispatch(msg)).status, "accepted")
        await asyncio.gather(*self.tasks)
        self.assertIs(msg.channel_subject, subject)
        self.assertIsNot(self.processed[0], msg)
        self.assertIsNot(self.processed[0].channel_subject, subject)
        self.hub.subscribe.assert_called_once_with("thread", "synthetic-connection")
        self.assertEqual(len(self.processed), 1)
        self.assertEqual(self.scope_exits, self.identities)
        self.assertIsNone(_CURRENT.get())
        with self.assertRaises(DashboardOriginError):
            self.identities[0].check_current(agent_id="agent", thread_id="thread")

    async def test_processor_failure_closes_before_scope_exit(self):
        self.mode = "error"
        await self.dispatch(self.message())
        await asyncio.gather(*self.tasks)
        self.assertEqual(len(self.scope_exits), 1)
        frames = [call.args[1] for call in self.hub.push_to_thread.await_args_list]
        self.assertTrue(any(frame["type"] == "error" for frame in frames))

    async def test_task_cancellation_closes_before_scope_exit(self):
        self.mode = "cancel"
        await self.dispatch(self.message())
        await asyncio.wait_for(self.entered.wait(), 2)
        self.tasks[0].cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.tasks[0]
        self.assertEqual(len(self.scope_exits), 1)

    async def test_scope_enter_failure_closes_without_base_effects(self):
        self.mode = "scope_error"
        msg = self.message()
        subject = msg.channel_subject
        await self.dispatch(msg)
        with self.assertRaisesRegex(ValueError, "^memorylake_channel_unavailable$") as caught:
            await self.tasks[0]
        self.assertNotIn("synthetic scope entry failure", str(caught.exception))
        self.assertEqual(len(self.identities), 1)
        with self.assertRaises(DashboardOriginError):
            self.identities[0].check_current(agent_id="agent", thread_id="thread")
        self.assertIs(msg.channel_subject, subject)
        self.media.assert_not_awaited()
        self.assertEqual(self.hub.mock_calls, [])

    async def test_native_message_bypasses_delivery_and_scope(self):
        self.delivery.close()
        await self.channel.handle_inbound(self.message(InboundMessage))
        self.assertEqual(self.identities, [])
        self.assertEqual(len(self.processed), 1)
        self.media.assert_awaited_once()

    async def test_queued_payload_mutation_rejected_before_base_effects(self):
        for change in ("image", "hitl", "attachment"):
            with self.subTest(change=change):
                msg = self.message()
                subject = msg.channel_subject
                start = len(self.identities)

                def enqueue(queued, *, change=change):
                    # Mutation occurs AFTER dispatch's origin issue has finished.
                    if change == "image":
                        queued.content = [ImageContent(data="c3ludGhldGlj")]
                    elif change == "hitl":
                        queued.metadata["hitl_policy"] = {"mode": "synthetic"}
                    else:
                        queued.metadata[INBOUND_ATTACHMENTS_KEY] = [{"name": "synthetic"}]
                    self.tasks.append(asyncio.create_task(self.channel.handle_inbound(queued)))

                self.assertEqual((await self.dispatch(msg, enqueue=enqueue)).status, "accepted")
                with self.assertRaisesRegex(ValueError, "^memorylake_channel_payload_unsupported$"):
                    await self.tasks[-1]
                self.assertEqual(len(self.identities), start + 1)
                with self.assertRaises(DashboardOriginError):
                    self.identities[-1].check_current(agent_id="agent", thread_id="thread")
                self.assertIs(msg.channel_subject, subject)
                self.media.assert_not_awaited()
                self.assertEqual(self.hub.mock_calls, [])
                self.assertEqual(self.processed, [])

    async def test_post_commit_routing_mutation_is_rejected_before_base(self):
        for field in (
            "thread",
            "agent",
            "subject",
            "session",
            "channel_session",
            "group",
            "as_user",
            "connection",
        ):
            with self.subTest(field=field):
                msg = self.message()
                await self.dispatch(msg)
                # commit has returned to the producer; the consumer awaiting
                # that Future has not resumed yet. Its origin SQL already passed.
                if field == "thread":
                    msg.metadata["thread_id"] = "other-thread"
                elif field == "agent":
                    msg.tenant_id = "other-agent"
                elif field == "subject":
                    msg.channel_subject.subject_id = "2"
                elif field == "session":
                    msg.metadata["session_key"] = "other-session"
                elif field == "channel_session":
                    msg.channel_session_id = "other-session"
                elif field == "group":
                    msg.metadata["is_group"] = True
                elif field == "connection":
                    msg.metadata["ws_connection_id"] = "other-connection"
                else:
                    msg.metadata["as_user"] = "2"
                with self.assertRaises(ValueError):
                    await self.tasks[-1]
                self.media.assert_not_awaited()
                self.assertEqual(self.hub.mock_calls, [])
                self.assertEqual(self.processed, [])

    async def test_base_media_await_uses_isolated_execution_snapshot(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def media(message):
            entered.set()
            await release.wait()

        self.media.side_effect = media
        msg = self.message()
        await self.dispatch(msg)
        await asyncio.wait_for(entered.wait(), 2)
        msg.metadata["thread_id"] = "other-thread"
        msg.metadata["ws_connection_id"] = "other-connection"
        msg.tenant_id = "other-agent"
        msg.content[0].text = "mutated text"
        release.set()
        await self.tasks[-1]
        self.hub.subscribe.assert_called_once_with("thread", "synthetic-connection")
        executed = self.processed[0]
        self.assertIsNot(executed, msg)
        self.assertIs(type(executed), DashboardInbound)
        self.assertIsNone(executed._origin)
        self.assertEqual(executed.tenant_id, "agent")
        self.assertEqual(executed.text, "synthetic")
        self.assertEqual(executed.metadata["thread_id"], "thread")
