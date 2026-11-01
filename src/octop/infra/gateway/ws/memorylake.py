"""Dormant authenticated front-of-channel adapter; never registered by default.

This wraps initial delivery and scope cleanup before WebSocketChannel's media,
subject and notification work. It does NOT verify each downstream observer or
delivery sink, authorize steer/HITL continuations, or enable GlobalProcessor.
That processor retains its hard gate even for a correctly consumed envelope.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from copy import deepcopy
from typing import Any

from langgraph.errors import GraphBubbleUp
from octop_gateway.channel import MessageProcessor
from octop_gateway.models import InboundMessage, TextContent

from octop.infra.connectors.memorylake.delivery import DashboardDelivery
from octop.infra.connectors.memorylake.origin import DashboardInbound, DashboardTurnIdentity
from octop.infra.gateway.process.message_keys import INBOUND_ATTACHMENTS_KEY
from octop.infra.gateway.steer import SteerCoordinator
from octop.infra.gateway.ws.ws_channel import WebSocketChannel
from octop.infra.gateway.ws.ws_hub import WebSocketHub


class _UnsupportedPayload(ValueError):
    pass


class MemoryLakeWebSocketChannel(WebSocketChannel):
    """Server-injected prototype, not a replacement for the native channel.

    The trusted scope factory can bind consumed identity to server-selected read
    resources. No identity or tools are reconstructed from request metadata.
    The scope must enclose full channel/stream consumption, not iterator creation.
    The server owner must close/drain delivery separately during shutdown.
    """

    def __init__(
        self,
        processor: MessageProcessor,
        *,
        hub: WebSocketHub,
        delivery: DashboardDelivery,
        turn_scope: Callable[[DashboardTurnIdentity], AbstractContextManager[None]],
        steer: SteerCoordinator | None = None,
    ) -> None:
        super().__init__(processor, hub=hub, steer=steer)
        self._delivery = delivery
        self._turn_scope = turn_scope

    def _allows_continuation(self, message: InboundMessage) -> bool:
        # A reconstructed plain InboundMessage must never inherit this grant.
        # This is per message/task, not mutation of the shared steer coordinator.
        return not isinstance(message, DashboardInbound)

    async def handle_inbound(self, raw_payload: Any) -> None:
        if not isinstance(raw_payload, DashboardInbound):
            # Native/IM behavior is unchanged, including its exception policy.
            await super().handle_inbound(raw_payload)
            return
        try:
            # Reject missing/revoked/copied/subclass origins before even calling
            # parse_inbound or any of the base channel's side-effecting methods.
            async with self._delivery.receive(raw_payload) as identity:
                with self._turn_scope(identity):
                    try:
                        # Capture after the final handoff await. Never expose
                        # this private working copy to the producer/queue, and
                        # never consume it again. Keep its carrier type so the
                        # production processor gate cannot be bypassed, but do
                        # not copy the private one-use origin token.
                        execution = DashboardInbound.model_validate(
                            deepcopy(raw_payload.model_dump())
                        )
                        identity.check_inbound(execution)
                        # The queued object is mutable. Recheck the initial,
                        # text-only policy AFTER awaited consumption and scope
                        # entry, rather than trusting a producer's earlier check.
                        metadata = execution.metadata
                        if (
                            any(type(part) is not TextContent for part in execution.content)
                            or not isinstance(metadata, dict)
                            or metadata.get(INBOUND_ATTACHMENTS_KEY)
                            or metadata.get("hitl_policy") is not None
                        ):
                            raise _UnsupportedPayload()
                        await super().handle_inbound(execution)
                    finally:
                        # Inherited child contexts must already be invalid when
                        # the enclosing scope begins its reset/exit procedure.
                        identity.close()
                # If factory/__enter__ failed, receive's finally closes instead.
        except GraphBubbleUp:
            raise
        except _UnsupportedPayload:
            raise ValueError("memorylake_channel_payload_unsupported") from None
        except Exception:
            raise ValueError("memorylake_channel_unavailable") from None
