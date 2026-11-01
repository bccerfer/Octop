"""Dormant delivery handshake tests, NOT registered WS authentication wiring.

Real OriginRegistry uses an explicitly permissive SQL-identity stub here; these
cases test queue/claim lifetime, not database authorization. Real ChannelManager
workers + WebSocketChannel run in process with synthetic text and no network.
The subclass/ContextVar below are TEST shims, not production middleware.
"""

from __future__ import annotations

import asyncio
import threading
import unittest
from contextvars import ContextVar
from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock, patch

from octop_gateway.manager import ChannelManager
from octop_gateway.models import ChannelSubject, InboundMessage, TextContent
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.delivery import DashboardDelivery
from octop.infra.connectors.memorylake.origin import (
    DashboardInbound,
    DashboardOriginError,
    DashboardOriginRegistry,
)
from octop.infra.gateway.gateway import Gateway
from octop.infra.gateway.ws.ws_channel import WS_CHANNEL_ID, WebSocketChannel
from octop.infra.gateway.ws.ws_hub import WebSocketHub

_IDENTITY = ContextVar("test_delivery_identity", default=None)


class _Identities:
    """Test stub only; real owner/thread SQL is covered by repository tests."""

    allowed = True

    def owns_dashboard_thread(self, **kwargs):
        return self.allowed


class DashboardDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def asyncSetUp(self):
        self.identities = _Identities()
        self.registry = DashboardOriginRegistry(identities=self.identities)
        self.delivery = DashboardDelivery(self.registry, max_inflight=2)
        self.addAsyncCleanup(self.close_delivery, self.delivery)
        self.tasks = []
        self.addAsyncCleanup(self.cleanup_tasks)

    async def close_delivery(self, delivery):
        delivery.close()

    async def cleanup_tasks(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def message(self, thread="thread"):
        return DashboardInbound(
            channel_id=WS_CHANNEL_ID,
            channel_type="dashboard",
            tenant_id="agent",
            channel_subject=ChannelSubject(subject_id="1"),
            channel_session_id="agent:dashboard:1:dm",
            content=[TextContent(text="synthetic")],
            metadata={
                "thread_id": thread,
                "session_key": "agent:dashboard:1:dm",
                "ws_connection_id": "fake-connection",
            },
        )

    async def dispatch(self, message, enqueue, *, timeout=0.1, delivery=None):
        return await (delivery or self.delivery).dispatch(
            message,
            caller_user_id=1,
            agent_id="agent",
            thread_id=message.metadata["thread_id"],
            enqueue=enqueue,
            timeout_s=timeout,
        )

    def task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def test_real_workers_ack_before_processing_finishes_and_close_identity(self):
        entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        identities = []
        phases = []
        outer = self

        class Processor:
            async def iter_turn_chunks(self, message):
                identity = _IDENTITY.get()
                identity.check_current(agent_id="agent", thread_id="thread")
                phases.append("processor")
                entered.set()
                await release.wait()
                yield {"type": "done"}

        class Channel(WebSocketChannel):
            async def handle_inbound(self, raw_payload):
                # The claim encloses media persistence and subject rewriting too.
                try:
                    async with outer.delivery.receive(raw_payload) as identity:
                        identities.append(identity)
                        token = _IDENTITY.set(identity)
                        try:
                            phases.append("claimed")
                            await super().handle_inbound(raw_payload)
                        finally:
                            identity.close()
                            _IDENTITY.reset(token)
                finally:
                    finished.set()

            async def _persist_media(self, message):
                _IDENTITY.get().check_current(agent_id="agent", thread_id="thread")
                phases.append("media")
                await super()._persist_media(message)

        channel = Channel(Processor(), hub=WebSocketHub())
        manager = ChannelManager(
            {WS_CHANNEL_ID: channel}, workers_per_channel=1, media_backend=object()
        )
        await manager.start()  # Real consumer workers predate issuance.
        try:
            self.assertIsNone(_IDENTITY.get())
            message = self.message()
            receipt = await self.dispatch(
                message, lambda msg: manager.enqueue(WS_CHANNEL_ID, msg), timeout=2
            )
            self.assertEqual(receipt.status, "accepted")
            with self.assertRaises((FrozenInstanceError, AttributeError)):
                receipt.status = "complete"
            await asyncio.wait_for(entered.wait(), 2)
            self.assertFalse(finished.is_set())
            self.assertEqual(phases[:3], ["claimed", "media", "processor"])
            release.set()
            await asyncio.wait_for(finished.wait(), 2)
            with self.assertRaises(DashboardOriginError):
                identities[0].check_current(agent_id="agent", thread_id="thread")
            self.assertIsNone(_IDENTITY.get())
        finally:
            release.set()
            await manager.stop()

    async def test_real_queue_full_no_ack_times_out_and_late_receive_denied(self):
        channel = WebSocketChannel(object(), hub=WebSocketHub())
        manager = ChannelManager(
            {WS_CHANNEL_ID: channel}, workers_per_channel=0, queue_maxsize=1, media_backend=object()
        )
        await manager.start()
        try:
            manager.enqueue(WS_CHANNEL_ID, self.message("filler"))
            await asyncio.sleep(0)  # Let public enqueue callback fill the real queue.
            message = self.message()
            with self.assertRaises((ValueError, TimeoutError)):
                await self.dispatch(message, lambda msg: manager.enqueue(WS_CHANNEL_ID, msg))
            with self.assertRaises(ValueError):
                async with self.delivery.receive(message):
                    self.fail("late dropped delivery was accepted")
            with self.assertRaises(DashboardOriginError):
                await self.registry.consume(message)
        finally:
            await manager.stop()

    async def test_unavailable_channel_and_noack_have_no_receipt(self):
        manager = ChannelManager({}, workers_per_channel=0, media_backend=object())
        await manager.start()
        try:
            for enqueue in (lambda msg: manager.enqueue("missing", msg), lambda msg: None):
                with self.subTest(enqueue=enqueue):
                    message = self.message()
                    with self.assertRaises((ValueError, TimeoutError)):
                        await self.dispatch(message, enqueue)
                    with self.assertRaises(ValueError):
                        async with self.delivery.receive(message):
                            self.fail("unacknowledged message accepted late")
        finally:
            await manager.stop()

    async def test_producer_cancel_before_ack_revokes_late_message(self):
        enqueued = asyncio.Event()
        message = self.message()
        sender = self.task(self.dispatch(message, lambda msg: enqueued.set(), timeout=2))
        await enqueued.wait()
        sender.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await sender
        with self.assertRaises(ValueError):
            async with self.delivery.receive(message):
                self.fail("cancelled producer delivered")
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)

    async def test_consumer_exception_and_cancellation_close_accepted_identity(self):
        for action in ("raise", "cancel"):
            with self.subTest(action=action):
                message = self.message()
                entered, release = asyncio.Event(), asyncio.Event()
                claims = []

                async def consume(msg, *, claims=claims, entered=entered, release=release):
                    async with self.delivery.receive(msg) as identity:
                        claims.append(identity)
                        entered.set()
                        await release.wait()
                        raise RuntimeError("synthetic consumer failure")

                consumers = []

                def enqueue(msg, *, consumers=consumers, consume=consume):
                    consumers.append(self.task(consume(msg)))

                self.assertEqual((await self.dispatch(message, enqueue)).status, "accepted")
                await entered.wait()
                if action == "cancel":
                    consumers[0].cancel()
                    expected = asyncio.CancelledError
                else:
                    release.set()
                    expected = RuntimeError
                with self.assertRaises(expected):
                    await consumers[0]
                with self.assertRaises(DashboardOriginError):
                    claims[0].check_current(agent_id="agent", thread_id="thread")

    async def test_copy_duplicate_and_capacity_do_not_create_extra_claims(self):
        delivery = DashboardDelivery(self.registry, max_inflight=1)
        self.addAsyncCleanup(self.close_delivery, delivery)
        release, entered = asyncio.Event(), asyncio.Event()
        message = self.message()

        async def consume(msg):
            async with delivery.receive(msg):
                entered.set()
                await release.wait()

        def enqueue(msg):
            self.task(consume(msg))

        try:
            self.assertEqual(
                (await self.dispatch(message, enqueue, delivery=delivery)).status, "accepted"
            )
            await entered.wait()
            for invalid in (message, message.model_copy(deep=True)):
                with self.subTest(copy=invalid is not message), self.assertRaises(ValueError):
                    async with delivery.receive(invalid):
                        self.fail("duplicate/copy accepted")
            with self.assertRaises(ValueError):
                await self.dispatch(self.message("other"), enqueue, delivery=delivery)
        finally:
            release.set()
        await asyncio.gather(*self.tasks)
        # Consumer exit, not producer acceptance, releases capacity.
        receipt = await self.dispatch(self.message("next"), enqueue, delivery=delivery)
        self.assertEqual(receipt.status, "accepted")

    async def test_close_revokes_pending_and_active_and_stops_dispatch(self):
        active_claims = []
        release, entered = asyncio.Event(), asyncio.Event()

        async def consume(msg):
            async with self.delivery.receive(msg) as identity:
                active_claims.append(identity)
                entered.set()
                await release.wait()

        active = self.message("active")
        self.assertEqual(
            (await self.dispatch(active, lambda msg: self.task(consume(msg)))).status, "accepted"
        )
        await entered.wait()
        enqueued = asyncio.Event()
        pending = self.message("pending")
        sender = self.task(self.dispatch(pending, lambda msg: enqueued.set(), timeout=2))
        await enqueued.wait()
        self.delivery.close()
        with self.assertRaises((ValueError, TimeoutError)):
            await sender
        with self.assertRaises(DashboardOriginError):
            active_claims[0].check_current(agent_id="agent", thread_id="active")
        with self.assertRaises(ValueError):
            async with self.delivery.receive(pending):
                self.fail("closed pending delivery")
        with self.assertRaises(ValueError):
            await self.dispatch(self.message(), lambda msg: None)
        release.set()

    async def test_consumer_auth_failure_before_commit_never_enters_handler(self):
        body = []
        failures = []

        async def consume(msg):
            try:
                async with self.delivery.receive(msg):
                    body.append(True)
            except ValueError as exc:
                failures.append(exc)

        def enqueue(msg):
            self.identities.allowed = False
            self.task(consume(msg))

        with self.assertRaises((ValueError, TimeoutError)):
            await self.dispatch(self.message(), enqueue)
        await asyncio.gather(*self.tasks)
        self.assertEqual(body, [])
        self.assertEqual(len(failures), 1)

    async def test_sender_cancel_after_claim_before_commit_closes_identity_without_handler(self):
        original = self.registry.consume
        claimed, body, consumers = [], [], []
        sender = None

        async def claim_then_cancel(message):
            identity = await original(message)
            claimed.append(identity)
            # Runs before dispatch can resume from the ready notification.
            asyncio.get_running_loop().call_soon(sender.cancel)
            return identity

        async def consume(message):
            async with self.delivery.receive(message):
                body.append(True)

        def enqueue(message):
            consumers.append(self.task(consume(message)))

        with patch.object(self.registry, "consume", side_effect=claim_then_cancel):
            sender = self.task(self.dispatch(self.message(), enqueue, timeout=2))
            with self.assertRaises(asyncio.CancelledError):
                await sender
            await asyncio.gather(*consumers, return_exceptions=True)
        self.assertEqual(body, [])
        self.assertEqual(len(claimed), 1)
        with self.assertRaises(DashboardOriginError):
            claimed[0].check_current(agent_id="agent", thread_id="thread")

    async def test_manager_stop_cancels_consumer_and_closes_identity(self):
        entered, exited = asyncio.Event(), asyncio.Event()
        claims = []
        outer = self

        class Processor:
            async def iter_turn_chunks(self, message):
                _IDENTITY.get().check_current(agent_id="agent", thread_id="thread")
                entered.set()
                await asyncio.Event().wait()
                yield {"type": "done"}

        class Channel(WebSocketChannel):
            async def handle_inbound(self, message):
                try:
                    async with outer.delivery.receive(message) as identity:
                        claims.append(identity)
                        token = _IDENTITY.set(identity)
                        try:
                            await super().handle_inbound(message)
                        finally:
                            identity.close()
                            _IDENTITY.reset(token)
                finally:
                    exited.set()

        manager = ChannelManager(
            {WS_CHANNEL_ID: Channel(Processor(), hub=WebSocketHub())},
            workers_per_channel=1,
            media_backend=object(),
        )
        await manager.start()
        try:
            receipt = await self.dispatch(
                self.message(), lambda msg: manager.enqueue(WS_CHANNEL_ID, msg), timeout=2
            )
            self.assertEqual(receipt.status, "accepted")
            await asyncio.wait_for(entered.wait(), 2)
            await manager.stop()
            self.assertTrue(exited.is_set())
            with self.assertRaises(DashboardOriginError):
                claims[0].check_current(agent_id="agent", thread_id="thread")
            self.assertIsNone(_IDENTITY.get())
        finally:
            await manager.stop()

    async def test_blocked_consume_sql_abort_retains_executor_quota_until_real_completion(self):
        for action in ("timeout", "cancel"):
            with self.subTest(action=action):
                registry = DashboardOriginRegistry(identities=self.identities, max_pending=1)
                delivery = DashboardDelivery(registry, max_inflight=2)
                self.addAsyncCleanup(self.close_delivery, delivery)
                entered, release = threading.Event(), threading.Event()
                count = 0
                body, consumers = [], []
                futures = ()

                def authorize(*, entered=entered, release=release, **kwargs):
                    nonlocal count
                    count += 1
                    if count == 2:  # issue succeeds; consume's real executor job blocks.
                        entered.set()
                        release.wait(3)
                    return True

                async def consume(msg, *, delivery=delivery, body=body):
                    async with delivery.receive(msg):
                        body.append(True)

                def enqueue(msg, *, consume=consume, consumers=consumers):
                    consumers.append(self.task(consume(msg)))

                with patch.object(self.identities, "owns_dashboard_thread", side_effect=authorize):
                    sender = self.task(
                        self.dispatch(
                            self.message(),
                            enqueue,
                            timeout=0.3 if action == "timeout" else 2,
                            delivery=delivery,
                        )
                    )
                    try:
                        self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                        # Track the actual shielded executor Future, not the thread
                        # Event: releasing a thread is not proof of its completion.
                        futures = tuple(registry._auth_jobs)
                        self.assertEqual(len(futures), 1)
                        if action == "cancel":
                            sender.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await sender
                        else:
                            with self.assertRaises((ValueError, TimeoutError)):
                                await sender
                        self.assertEqual(body, [])
                        self.assertFalse(futures[0].done())
                        with self.assertRaisesRegex(DashboardOriginError, "capacity"):
                            await registry.issue(
                                self.message("next"),
                                caller_user_id=1,
                                agent_id="agent",
                                thread_id="next",
                            )
                    finally:
                        release.set()
                        sender.cancel()
                        await asyncio.gather(sender, return_exceptions=True)
                        await asyncio.gather(
                            *futures, *tuple(registry._auth_jobs), return_exceptions=True
                        )
                        await asyncio.gather(*consumers, return_exceptions=True)
                    self.assertTrue(all(future.done() for future in futures))
                    self.assertEqual(body, [])
                    fresh = self.message("fresh")
                    await registry.issue(
                        fresh, caller_user_id=1, agent_id="agent", thread_id="fresh"
                    )
                    (await registry.consume(fresh)).close()

    async def test_identity_expiry_after_claim_before_commit_prevents_handler(self):
        now = [100.0]
        registry = DashboardOriginRegistry(
            identities=self.identities, ttl_s=1, clock=lambda: now[0]
        )
        delivery = DashboardDelivery(registry)
        self.addAsyncCleanup(self.close_delivery, delivery)
        original = registry.consume
        claims, body, consumers = [], [], []

        async def claim_then_expire(message):
            identity = await original(message)
            claims.append(identity)
            asyncio.get_running_loop().call_soon(now.__setitem__, 0, 101.0)
            return identity

        async def consume(message):
            async with delivery.receive(message):
                body.append(True)

        def enqueue(message):
            consumers.append(self.task(consume(message)))

        with patch.object(registry, "consume", side_effect=claim_then_expire):
            with self.assertRaises(ValueError):
                await self.dispatch(self.message(), enqueue, delivery=delivery)
            await asyncio.gather(*consumers, return_exceptions=True)
        self.assertEqual(body, [])
        self.assertEqual(len(claims), 1)
        with self.assertRaises(DashboardOriginError):
            claims[0].check_current(agent_id="agent", thread_id="thread")

    async def test_stop_control_text_is_denied_before_enqueue(self):
        for text in ("/stop", "/cancel", "/STOP", " /CaNcEl reason", "\t/stop\nargument"):
            with self.subTest(text=text):
                message = self.message()
                message.content = [TextContent(text=text)]
                enqueued = []
                with self.assertRaises((ValueError, TimeoutError)):
                    await self.dispatch(message, enqueued.append, timeout=0.02)
                self.assertEqual(enqueued, [])

    async def test_real_prelock_does_not_preempt_mutated_or_revoked_origins(self):
        for mode in ("mutated", "late", "subclass"):
            with self.subTest(mode=mode):
                await self._prelock_case(mode)

    async def test_real_prelock_preserves_native_stop(self):
        await self._prelock_case("native")

    async def _prelock_case(self, mode):
        processed = asyncio.Event()
        gateway = object.__new__(Gateway)
        gateway._agent_manager = MagicMock()
        gateway._thread_registry = MagicMock()
        gateway._thread_registry.get_bound_thread_id.return_value = "thread"
        body = []
        outer = self

        class Channel(WebSocketChannel):
            async def handle_inbound(self, message):
                try:
                    if isinstance(message, DashboardInbound):
                        try:
                            async with outer.delivery.receive(message):
                                body.append(True)
                        except ValueError:
                            pass
                    # Plain native stop needs no MemoryLake claim. This probe
                    # measures the REAL pre-lock hook, not native slash execution.
                finally:
                    processed.set()

        manager = ChannelManager(
            {WS_CHANNEL_ID: Channel(object(), hub=WebSocketHub())},
            workers_per_channel=1,
            media_backend=object(),
            on_pre_lock=gateway._preempt_cancel_on_stop,
        )
        await manager.start()
        try:
            message = self.message()
            if mode == "mutated":

                def enqueue(msg):
                    msg.content = [TextContent(text="/stop")]
                    manager.enqueue(WS_CHANNEL_ID, msg)

                with self.assertRaises((ValueError, TimeoutError)):
                    await self.dispatch(message, enqueue)
            else:
                if mode == "late":
                    with self.assertRaises((ValueError, TimeoutError)):
                        await self.dispatch(message, lambda msg: None, timeout=0.02)
                elif mode == "native":
                    message = InboundMessage.model_validate(message.model_dump())
                elif mode == "subclass":

                    class ChildInbound(DashboardInbound):
                        pass

                    message = ChildInbound.model_validate(message.model_dump())
                message.content = [TextContent(text="/stop")]
                manager.enqueue(WS_CHANNEL_ID, message)
            await asyncio.wait_for(processed.wait(), 2)
            if mode == "native":
                gateway._agent_manager.cancel_stream.assert_called_once_with("agent", "thread")
            else:
                gateway._agent_manager.cancel_stream.assert_not_called()
                self.assertEqual(body, [])
        finally:
            await manager.stop()

    async def test_enqueue_timeout_error_body_is_not_exposed(self):
        sentinel = "SYNTHETIC-ENQUEUE-SECRET-MUST-NOT-ECHO"

        def broken_enqueue(message):
            raise TimeoutError(sentinel)

        with self.assertRaises((ValueError, TimeoutError)) as caught:
            await self.dispatch(self.message(), broken_enqueue)
        self.assertNotIn(sentinel, str(caught.exception))
        self.assertNotIn(sentinel, repr(caught.exception))
        # A genuine acknowledgement deadline must remain body-free too.
        with self.assertRaises((ValueError, TimeoutError)) as deadline:
            await self.dispatch(self.message(), lambda msg: None, timeout=0.02)
        self.assertNotIn(sentinel, str(deadline.exception))

    async def test_stop_like_ordinary_text_is_not_a_control_command(self):
        for text in ("/stopwatch", "/cancellation", "please /stop"):
            with self.subTest(text=text):
                message = self.message()
                message.content = [TextContent(text=text)]
                await self.registry.issue(
                    message, caller_user_id=1, agent_id="agent", thread_id="thread"
                )
                identity = await self.registry.consume(message)
                identity.close()

    async def test_producer_cancellation_after_commit_does_not_revoke_consumer(self):
        accepted, entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        claims, consumers, receipts = [], [], []

        async def consume(message):
            async with self.delivery.receive(message) as identity:
                claims.append(identity)
                entered.set()
                await release.wait()

        def enqueue(message):
            consumers.append(self.task(consume(message)))

        async def producer():
            receipts.append(await self.dispatch(self.message(), enqueue, timeout=2))
            accepted.set()
            await asyncio.Event().wait()  # Sender's own post-handoff work.

        sender = self.task(producer())
        try:
            await asyncio.wait_for(accepted.wait(), 2)
            await asyncio.wait_for(entered.wait(), 2)
            self.assertEqual(receipts[0].status, "accepted")
            sender.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await sender
            # Accepted is a handoff, not a promise that sender cancellation can
            # retract a consumer's already-authorized work or previously sent data.
            claims[0].check_current(agent_id="agent", thread_id="thread")
            self.assertFalse(consumers[0].done())
        finally:
            release.set()
            await asyncio.gather(*consumers, return_exceptions=True)
        with self.assertRaises(DashboardOriginError):
            claims[0].check_current(agent_id="agent", thread_id="thread")

    async def test_manager_stop_before_claim_leaves_no_ack_until_deadline_revokes(self):
        filler_entered, enqueued = asyncio.Event(), asyncio.Event()
        body = []
        outer = self

        class Channel(WebSocketChannel):
            async def handle_inbound(self, message):
                if not isinstance(message, DashboardInbound):
                    filler_entered.set()
                    await asyncio.Event().wait()
                    return
                async with outer.delivery.receive(message):
                    body.append(True)

        manager = ChannelManager(
            {WS_CHANNEL_ID: Channel(object(), hub=WebSocketHub())},
            workers_per_channel=1,
            media_backend=object(),
        )
        await manager.start()
        try:
            filler = InboundMessage.model_validate(self.message("filler").model_dump())
            manager.enqueue(WS_CHANNEL_ID, filler)
            await asyncio.wait_for(filler_entered.wait(), 2)
            message = self.message()

            def enqueue(msg):
                manager.enqueue(WS_CHANNEL_ID, msg)
                enqueued.set()  # Only attempted enqueue, explicitly NOT a receipt.

            sender = self.task(self.dispatch(message, enqueue, timeout=0.3))
            await enqueued.wait()
            await asyncio.sleep(0)  # Run public enqueue's scheduled callback.
            await manager.stop()
            # No worker claimed this message; stop itself does not notify
            # delivery. The finite dispatch deadline supplies failure/revocation.
            with self.assertRaises((ValueError, TimeoutError)):
                await sender
            self.assertEqual(body, [])
            with self.assertRaises(ValueError):
                async with self.delivery.receive(message):
                    self.fail("stopped, timed-out delivery accepted late")
            with self.assertRaises(DashboardOriginError):
                await self.registry.consume(message)
        finally:
            await manager.stop()
