"""Real steer coordinator: dormant typed grants must never cover a continuation."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from unittest.mock import AsyncMock, MagicMock

import pytest
from octop_gateway.models import ChannelSubject, InboundMessage, TextContent
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.delivery import DashboardDelivery
from octop.infra.connectors.memorylake.origin import DashboardInbound, DashboardOriginRegistry
from octop.infra.gateway.steer.coordinator import SteerCoordinator
from octop.infra.gateway.ws.memorylake import MemoryLakeWebSocketChannel

_IDENTITY = ContextVar("memorylake_steer_test_identity", default=None)


class Identities:
    def owns_dashboard_thread(self, **kwargs):
        return True


@pytest.fixture(autouse=True)
def offline_safety():
    with offline_test_guard():
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize("typed,hitl", [(True, False), (False, False), (True, True)])
async def test_real_steer_never_extends_typed_grant_but_native_still_continues(typed, hitl):
    steer = SteerCoordinator()
    delivery = DashboardDelivery(DashboardOriginRegistry(identities=Identities()))
    calls, consumers = [], []
    hub = MagicMock()
    hub.push = AsyncMock()
    hub.push_to_thread = AsyncMock()

    @contextmanager
    def scope(identity):
        token = _IDENTITY.set(identity)
        try:
            yield
        finally:
            _IDENTITY.reset(token)

    class Processor:
        async def iter_turn_chunks(self, message):
            identity = _IDENTITY.get()
            if identity is not None:
                identity.check_current(agent_id="agent", thread_id="thread")
            calls.append((type(message), identity is not None))
            if len(calls) == 1:
                state = steer.get_state("thread")
                assert state is not None
                steer.admit(
                    "thread",
                    text="synthetic continuation",
                    client_pending_id="pending",
                    expected_turn_id=state.turn_id,
                    delivery="steer",
                )
                assert steer.has_pending("thread")
            if hitl:
                yield {"type": "hitl_required", "request": {"synthetic": True}}
            else:
                yield {"type": "token", "content": "synthetic"}
            yield {"type": "done"}

    channel = MemoryLakeWebSocketChannel(
        Processor(), hub=hub, delivery=delivery, turn_scope=scope, steer=steer
    )
    cls = DashboardInbound if typed else InboundMessage
    message = cls(
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

    def enqueue(message):
        consumers.append(asyncio.create_task(channel.handle_inbound(message)))

    try:
        if typed:
            receipt = await delivery.dispatch(
                message,
                caller_user_id=1,
                agent_id="agent",
                thread_id="thread",
                enqueue=enqueue,
                timeout_s=2,
            )
            assert receipt.status == "accepted"
            await asyncio.gather(*consumers)
            assert calls == [(DashboardInbound, True)], (
                "ordinary continuation inherited typed grant"
            )
            if hitl:
                assert not any(
                    call.args[1]["type"] == "hitl_required"
                    for call in hub.push_to_thread.await_args_list
                ), "unsupported typed HITL was exposed to the hub"
                assert not steer.is_hitl_paused("thread")
                assert steer.stage_hitl_pending("thread") is None
            assert not steer.has_pending("thread")
            assert not any(
                call.args[1]["type"] in ("steer.drained", "turn.resumed")
                for call in hub.push_to_thread.await_args_list
            )
        else:
            await channel.handle_inbound(message)
            assert calls == [(InboundMessage, False), (InboundMessage, False)]
            assert any(
                call.args[1]["type"] == "turn.resumed"
                for call in hub.push_to_thread.await_args_list
            )
    finally:
        delivery.close()
        for consumer in consumers:
            consumer.cancel()
        await asyncio.gather(*consumers, return_exceptions=True)
