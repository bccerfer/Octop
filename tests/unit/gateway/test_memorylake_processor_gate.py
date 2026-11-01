"""Production processor remains closed to dormant MemoryLake origin carriers."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from octop_gateway.models import ChannelSubject, InboundMessage, TextContent
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.origin import DashboardInbound
from octop.infra.gateway.process.processor import GlobalProcessor
from octop.infra.gateway.slash.dispatcher import SlashDispatcher


class DerivedDashboardInbound(DashboardInbound):
    pass


@pytest.fixture
def setup():
    with offline_test_guard():
        streams = []

        async def stream(*args, **kwargs):
            streams.append((args, kwargs))
            yield {"type": "token", "content": "synthetic native response"}

        manager = MagicMock()
        manager.stream = stream
        manager.merge_turn_mcp_servers.return_value = None
        manager.prepare_chat_mcp = AsyncMock(return_value=[])
        agent = MagicMock()
        agent.user_id = 1
        agent.config_json = json.dumps({})
        agent.system_prompt = None
        agent.kind = "expert"
        agents = MagicMock()
        agents.get.return_value = agent
        users, connectors, threads, audit, trajectory = (MagicMock() for _ in range(5))
        threads.get_or_create_by_key = AsyncMock(return_value="thread")
        processor = GlobalProcessor(
            agent_manager=manager,
            thread_registry=threads,
            audit_repo=audit,
            agent_repo=agents,
            user_repo=users,
            connector_repo=connectors,
            dispatcher=SlashDispatcher(),
            trajectory_service=trajectory,
        )
        yield processor, streams, (manager, agents, users, connectors, threads, audit, trajectory)


def message(cls):
    return cls(
        channel_id="octop-dashboard",
        channel_type="dashboard",
        tenant_id="agent",
        channel_subject=ChannelSubject(subject_id="1"),
        channel_session_id="agent:dashboard:1:dm",
        content=[TextContent(text="synthetic request")],
        metadata={"thread_id": "thread", "session_key": "agent:dashboard:1:dm"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["__call__", "iter_turn_chunks"])
@pytest.mark.parametrize("carrier", [DashboardInbound, DerivedDashboardInbound])
async def test_dormant_carrier_denied_before_any_observer(setup, entry, carrier):
    processor, streams, dependencies = setup
    for dependency in dependencies:
        dependency.reset_mock()
    # Earliest observable operation is itself forbidden. This also prevents a
    # red test from accidentally running any later history/media/tool pipeline.
    with (
        patch(
            "octop.infra.metrics.METRICS.inc", side_effect=AssertionError("metrics touched")
        ) as metric,
        pytest.raises(ValueError, match="^memorylake_production_lifecycle_unverified$"),
    ):
        _ = [chunk async for chunk in getattr(processor, entry)(message(carrier))]
    metric.assert_not_called()
    assert streams == []
    for dependency in dependencies:
        assert dependency.mock_calls == []


@pytest.mark.asyncio
async def test_plain_inbound_retains_native_dashboard_stream(setup):
    processor, streams, _ = setup
    chunks = [chunk async for chunk in processor.iter_turn_chunks(message(InboundMessage))]
    assert len(streams) == 1
    assert {"type": "token", "content": "synthetic native response"} in chunks
    assert chunks[-1]["type"] == "done"


@pytest.mark.asyncio
async def test_plain_inbound_retains_native_im_entry(setup):
    processor, streams, _ = setup
    events = [event async for event in processor(message(InboundMessage))]
    assert len(streams) == 1
    assert events[-1].type == "completed"


@pytest.mark.asyncio
async def test_actual_channel_side_effects_precede_processor_gate(setup, caplog):
    """Processor fail-close is NOT a no-side-effect gateway/ingress boundary.

    Use actual channel + processor with synthetic text and a recording-only hub.
    No media backend is installed: this test does not persist any file.
    """
    from octop.infra.gateway.ws.ws_channel import WebSocketChannel

    processor, streams, dependencies = setup
    hub = MagicMock()
    hub.push_to_thread = AsyncMock()
    hub.push = AsyncMock()
    channel = WebSocketChannel(processor, hub=hub)
    inbound = message(DashboardInbound)
    inbound.metadata["ws_connection_id"] = "synthetic-connection"
    original_subject = inbound.channel_subject
    for dependency in dependencies:
        dependency.reset_mock()

    with patch("octop.infra.metrics.METRICS.inc") as metric:
        # Channel catches the processor's denial and emits an error lifecycle.
        await channel.handle_inbound(inbound)

    assert "memorylake_production_lifecycle_unverified" in caplog.text
    metric.assert_not_called()
    assert streams == []
    for dependency in dependencies:
        assert dependency.mock_calls == []
    # These mutations/notifications ALREADY happened before processor iteration.
    assert inbound.channel_subject is not original_subject
    assert inbound.channel_subject.metadata["ws_connection_id"] == "synthetic-connection"
    assert inbound.channel_subject.metadata["thread_id"] == "thread"
    hub.subscribe.assert_called_once_with("thread", "synthetic-connection")
    hub.mark_turn_active.assert_called_once_with("thread")
    hub.mark_turn_idle.assert_called_once_with("thread")
    frames = [call.args[1] for call in hub.push_to_thread.await_args_list]
    assert frames[0]["type"] == "turn.started"
    assert any(frame["type"] == "error" for frame in frames)
    assert frames[-1]["type"] == "turn.ended"
    assert frames[-1]["failed"] is True
