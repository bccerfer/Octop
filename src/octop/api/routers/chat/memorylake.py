"""Dormant JWT producer prototype, NOT a registered WS/HTTP entry point.

The real processor deliberately rejects this envelope until HITL, history and
observer delivery policies are verified. Only synthetic local consumers are
currently exercised. Never accept caller identity from turn/model metadata.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable
from copy import deepcopy
from typing import TYPE_CHECKING

from octop_gateway.models import TextContent

from octop.api.deps import _decode, resolve_user_from_token
from octop.api.routers.chat.models import ChatTurnBody
from octop.api.routers.chat.turn import PreparedDashboardTurn, build_dashboard_inbound
from octop.infra.connectors.memorylake.delivery import DashboardDelivery, DashboardDeliveryReceipt
from octop.infra.connectors.memorylake.origin import DashboardInbound
from octop.infra.gateway.ws.ws_hub import ChatConnectionRef

if TYPE_CHECKING:
    from octop.infra.server import OctopServer


async def dispatch_memorylake_dashboard_turn(
    *,
    server: OctopServer,
    token: str,
    agent_id: str,
    prepared: PreparedDashboardTurn,
    turn: ChatTurnBody,
    connection: ChatConnectionRef,
    delivery: DashboardDelivery,
    enqueue: Callable[[DashboardInbound], object],
    timeout_s: float = 5,
) -> DashboardDeliveryReceipt:
    """Reverify JWT per turn and hand off a bounded, text-only owned DM.

    UserManager is a cache, not the authority for current disabled/owner status;
    the origin registry performs those SQL checks on issue and consumption.
    Token expiry caps both monotonic and wall-clock identity lifetime. Rotation
    is checked on the next dispatch; in-flight key/session revocation is still
    a production gate, not a guarantee supplied by this prototype.
    """
    try:
        # Do not validate caller-owned mutable objects, await authentication,
        # then emit their mutated content/policies under an earlier decision.
        prepared = deepcopy(prepared)
        turn = turn.model_copy(deep=True)
        if (
            type(token) is not str
            or type(connection) is not ChatConnectionRef
            or not 1 <= len(token) <= 8192
            or type(timeout_s) not in (int, float)
            or not math.isfinite(timeout_s)
            or not 0 < timeout_s <= 30
            or prepared.inbound_attachments
            or any(type(part) is not TextContent for part in prepared.inbound_content)
            or turn.hitl_policy is not None
            or turn.target_agent_ids
            or turn.delivery not in (None, "follow_up")
            or turn.client_pending_id is not None
            or turn.expected_turn_id is not None
            or turn.conversation_mode not in (None, "craft")
        ):
            raise ValueError("memorylake_ingress_unsupported")

        def verify() -> tuple[int, float]:
            # Both calls use the existing auth implementation and current key;
            # no handshake-cached user or unverified JWT decode is accepted.
            user = resolve_user_from_token(server, token)
            payload = _decode(server, token)
            expiry = payload.get("exp")
            if (
                type(user.id) is not int
                or user.id <= 0
                or payload.get("sub") != user.id
                or not isinstance(expiry, (int, float))
                or isinstance(expiry, bool)
                or not math.isfinite(expiry)
            ):
                raise ValueError("memorylake_ingress_auth_denied")
            return user.id, float(expiry)

        # Capture both clocks BEFORE authentication can suspend. Mapping expiry
        # only after SQL would let a wall-clock rollback replenish JWT lifetime.
        anchor_wall, anchor_mono = time.time(), time.monotonic()
        if any(
            type(value) not in (int, float) or not math.isfinite(value)
            for value in (anchor_wall, anchor_mono)
        ):
            raise ValueError("memorylake_ingress_auth_denied")
        # One absolute deadline covers verification AND handoff, rather than
        # starting a new budget only after a potentially blocked secret lookup.
        async with asyncio.timeout(timeout_s):
            caller_id, expiry = await delivery.verify(verify)
            jwt_deadline = anchor_mono + max(0.0, expiry - anchor_wall)

            def check_source() -> None:
                wall, mono = time.time(), time.monotonic()
                if (
                    any(
                        type(value) not in (int, float) or not math.isfinite(value)
                        for value in (wall, mono)
                    )
                    or wall >= expiry
                    or mono >= jwt_deadline
                ):
                    raise ValueError("memorylake_ingress_auth_expired")
                connection.check_current(caller_user_id=caller_id, agent_id=agent_id)

            check_source()
            inbound = build_dashboard_inbound(
                agent_id=agent_id,
                user_id=caller_id,
                prepared=prepared,
                turn=turn,
                ws_connection_id=connection.connection_id,
            )
            message = DashboardInbound.model_validate(inbound.model_dump())
            return await delivery.dispatch(
                message,
                caller_user_id=caller_id,
                agent_id=agent_id,
                thread_id=prepared.thread_id,
                not_after_epoch=expiry,
                source_check=check_source,
                enqueue=enqueue,
                timeout_s=timeout_s,
            )
    except Exception:
        # Do not echo a token, content, DB exception or callback's error body.
        # CancelledError remains cancellation, with auth jobs retaining quota.
        raise ValueError("memorylake_ingress_unavailable") from None
