"""Actual native WS route and Gateway shutdown; no socket transport or services."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import WebSocketDisconnect
from octop_gateway.models import InboundMessage, TextContent
from starlette.websockets import WebSocketState
from tests.support.memorylake import offline_test_guard

from octop.api.routers.chat import ws
from octop.api.routers.chat.turn import PreparedDashboardTurn
from octop.infra.gateway.gateway import Gateway
from octop.infra.gateway.steer.coordinator import SteerCoordinator
from octop.infra.gateway.ws.ws_hub import WebSocketHub


@pytest.fixture(autouse=True)
def offline_safety():
    with offline_test_guard():
        yield


class SyntheticSocket:
    def __init__(self, server, frames):
        self.app = SimpleNamespace(state=SimpleNamespace(octop_server=server))
        self.application_state = WebSocketState.CONNECTING
        self.frames = list(frames)
        self.sent = []

    async def accept(self):
        self.application_state = WebSocketState.CONNECTED

    async def receive_text(self):
        if self.frames:
            return json.dumps(self.frames.pop(0))
        self.application_state = WebSocketState.DISCONNECTED
        raise WebSocketDisconnect()

    async def send_text(self, frame):
        self.sent.append(json.loads(frame))

    async def close(self, **kwargs):
        self.application_state = WebSocketState.DISCONNECTED


@pytest.mark.asyncio
@pytest.mark.parametrize("user_turn", [False, True])
async def test_native_chat_route_mints_verified_source_and_disconnect_revokes(user_turn):
    hub = WebSocketHub()
    queue = MagicMock()
    gateway = SimpleNamespace(
        ws_hub=hub,
        channel_manager=queue,
        steer=SteerCoordinator(),
        processor=SimpleNamespace(hitl_coordinator=MagicMock()),
    )
    gateway.processor.hitl_coordinator.store.resolve_pending_for_thread.return_value = None
    server = SimpleNamespace(app_runtime=SimpleNamespace(gateway=gateway))
    user = SimpleNamespace(id=7, is_admin=False)
    frame = {"type": "user_turn", "text": "synthetic"} if user_turn else {"type": "ping"}
    socket = SyntheticSocket(server, [frame])
    prepared = PreparedDashboardTurn(
        thread_id="thread",
        session_key="agent:dashboard:7:dm",
        mcp_servers=None,
        skills=None,
        model_ref=None,
        inbound_content=[TextContent(text="synthetic")],
        composer_context=None,
        inbound_attachments=[],
    )
    refs = []
    original = hub.register_authenticated_chat

    def record_registration(connection_id, send_fn, *, user_id, agent_id):
        ref = original(connection_id, send_fn, user_id=user_id, agent_id=agent_id)
        ref.check_current(caller_user_id=7, agent_id="agent")
        refs.append(ref)
        return ref

    with (
        patch.object(hub, "register_authenticated_chat", side_effect=record_registration),
        patch.object(ws, "resolve_user_from_token", return_value=user) as verify,
        patch.object(ws, "assert_agent_access") as access,
        patch.object(ws, "prepare_dashboard_turn", new=AsyncMock(return_value=prepared)),
    ):
        await ws.dashboard_chat_ws(socket, "agent", token="synthetic-token")

    verify.assert_called_once_with(server, "synthetic-token")
    access.assert_called_once_with(server, "agent", user)
    assert len(refs) == 1, "native authenticated chat registration did not bind JWT owner"
    assert hub.connection_count == 0
    with pytest.raises(ValueError):
        refs[0].check_current(caller_user_id=7, agent_id="agent")
    if user_turn:
        queue.enqueue.assert_called_once()
        envelope = queue.enqueue.call_args.args[1]
        assert type(envelope) is InboundMessage, "native route silently enabled MemoryLake envelope"
        assert envelope.metadata["ws_connection_id"] == refs[0].connection_id
    else:
        queue.enqueue.assert_not_called()
        assert socket.sent == [{"type": "pong"}]


@pytest.mark.asyncio
async def test_real_gateway_shutdown_revokes_socket_before_awaiting_workers():
    gateway = Gateway(agent_manager=MagicMock(), repos=MagicMock())
    hub = gateway.ws_hub
    ref = hub.register_authenticated_chat("synthetic", AsyncMock(), user_id=7, agent_id="agent")
    hub.subscribe("thread", ref.connection_id)
    events = []

    async def history_close():
        events.append("history")
        with pytest.raises(ValueError):
            ref.check_current(caller_user_id=7, agent_id="agent")
        assert hub.connection_count == 0

    async def stop_workers():
        events.append("workers")
        with pytest.raises(ValueError):
            ref.check_current(caller_user_id=7, agent_id="agent")
        assert not hub.has_subscribers("thread")

    gateway._history_backfill = SimpleNamespace(close=AsyncMock(side_effect=history_close))
    gateway._channel_manager = SimpleNamespace(stop=AsyncMock(side_effect=stop_workers))
    await gateway.shutdown()
    assert events == ["history", "workers"]
    assert gateway.channel_manager is None
    assert hub.connection_count == 0


@pytest.mark.asyncio
async def test_reboot_prefix_replaces_closed_hub_without_reviving_old_ref():
    """Exercise actual boot prefix only; intentionally stop before worker startup."""
    gateway = Gateway(agent_manager=MagicMock(), repos=MagicMock())
    old_hub = gateway.ws_hub
    old_ref = old_hub.register_authenticated_chat(
        "synthetic", AsyncMock(), user_id=7, agent_id="agent"
    )
    gateway._history_backfill = SimpleNamespace(close=AsyncMock())
    await gateway.shutdown()
    visited = []

    class StopBeforeWorkers(Exception):
        pass

    def first_boot_hook():
        visited.append(True)
        assert gateway.ws_hub is not old_hub, "boot reused the irreversibly closed Hub"
        new_ref = gateway.ws_hub.register_authenticated_chat(
            "synthetic", AsyncMock(), user_id=7, agent_id="agent"
        )
        new_ref.check_current(caller_user_id=7, agent_id="agent")
        with pytest.raises(ValueError):
            old_ref.check_current(caller_user_id=7, agent_id="agent")
        raise StopBeforeWorkers()

    with (
        patch(
            "octop.infra.gateway.process.channel_thinking.install_channel_thinking_clean",
            side_effect=first_boot_hook,
        ),
        pytest.raises(StopBeforeWorkers),
    ):
        await gateway.boot()
    assert visited == [True]
    gateway.ws_hub.close()


@pytest.mark.asyncio
async def test_hub_close_during_socket_accept_does_not_escape_route_cleanup():
    hub = WebSocketHub()
    gateway = SimpleNamespace(ws_hub=hub, channel_manager=MagicMock())
    server = SimpleNamespace(app_runtime=SimpleNamespace(gateway=gateway))
    user = SimpleNamespace(id=7, is_admin=False)

    class ClosingSocket(SyntheticSocket):
        async def accept(self):
            await super().accept()
            asyncio.get_running_loop().call_soon(hub.close)
            await asyncio.sleep(0)

    socket = ClosingSocket(server, [])
    with (
        patch.object(ws, "resolve_user_from_token", return_value=user),
        patch.object(ws, "assert_agent_access"),
    ):
        # Failure to register must enter route's existing error/finally boundary.
        await ws.dashboard_chat_ws(socket, "agent", token="synthetic-token")
    assert socket.application_state is WebSocketState.DISCONNECTED
    assert hub.connection_count == 0
    gateway.channel_manager.enqueue.assert_not_called()
    assert socket.sent == [{"type": "error", "message": "internal error"}]


@pytest.mark.asyncio
async def test_full_gateway_boot_shutdown_boot_uses_fresh_hub_and_native_channels():
    """Real processor, channel manager and native channel lifecycle; no work enqueued."""
    from octop_gateway.manager import ChannelManager

    from octop.infra.gateway.cli import CLI_CHANNEL_ID, CliChannel
    from octop.infra.gateway.process.processor import GlobalProcessor
    from octop.infra.gateway.ws import WS_CHANNEL_ID, WebSocketChannel

    repos = MagicMock()
    repos.channel_repo.list_all.return_value = []
    gateway = Gateway(agent_manager=MagicMock(), repos=repos)
    try:
        await gateway.boot()
        first_manager = gateway.channel_manager
        assert isinstance(first_manager, ChannelManager)
        assert isinstance(gateway.processor, GlobalProcessor)
        old_hub = gateway.ws_hub
        old_channel = first_manager.get_channel(WS_CHANNEL_ID)
        assert type(old_channel) is WebSocketChannel
        assert type(first_manager.get_channel(CLI_CHANNEL_ID)) is CliChannel
        assert old_channel._hub is old_hub
        old_ref = old_hub.register_authenticated_chat(
            "synthetic-reused-id", AsyncMock(), user_id=7, agent_id="agent"
        )
        old_ref.check_current(caller_user_id=7, agent_id="agent")
        await asyncio.sleep(0)  # Allow actual idle consumer workers to start.

        await gateway.shutdown()
        assert gateway.channel_manager is None
        assert old_hub.connection_count == 0
        with pytest.raises(ValueError):
            old_ref.check_current(caller_user_id=7, agent_id="agent")

        await gateway.boot()
        second_manager = gateway.channel_manager
        assert isinstance(second_manager, ChannelManager)
        assert second_manager is not first_manager
        new_hub = gateway.ws_hub
        assert new_hub is not old_hub
        new_channel = second_manager.get_channel(WS_CHANNEL_ID)
        assert type(new_channel) is WebSocketChannel
        assert new_channel is not old_channel
        assert new_channel._hub is new_hub
        assert type(second_manager.get_channel(CLI_CHANNEL_ID)) is CliChannel
        new_ref = new_hub.register_authenticated_chat(
            "synthetic-reused-id", AsyncMock(), user_id=7, agent_id="agent"
        )
        new_ref.check_current(caller_user_id=7, agent_id="agent")
        with pytest.raises(ValueError):
            old_ref.check_current(caller_user_id=7, agent_id="agent")
        await asyncio.sleep(0)
    finally:
        await gateway.shutdown()
