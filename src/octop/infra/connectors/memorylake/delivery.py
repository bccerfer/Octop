"""Dormant consumer-acknowledged delivery; NOT an enqueue-success API.

The gateway's public enqueue may silently drop work. Only a consumer which has
redeemed the exact origin can acknowledge this handshake. A second, synchronous
producer commit transfers lifetime ownership before the consumer enters its body.
No JWT endpoint, gateway hook or production consumer is registered here.
"""

from __future__ import annotations

import asyncio
import inspect
import math
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal, TypeVar

from octop_gateway.models import InboundMessage

from octop.infra.connectors.memorylake.origin import (
    DashboardInbound,
    DashboardOriginRegistry,
    DashboardTurnIdentity,
)

_T = TypeVar("_T")


class DashboardDeliveryError(ValueError):
    """Constant internal code; neither payloads nor exception bodies are echoed."""


@dataclass(frozen=True)
class DashboardDeliveryReceipt:
    """Consumer handoff committed, not turn completion or proof of CLI execution."""

    status: Literal["accepted"] = "accepted"


@dataclass(repr=False)
class _Delivery:
    message: DashboardInbound
    ready: asyncio.Future[bool]
    commit: asyncio.Future[bool]
    phase: Literal["issuing", "queued", "claiming", "claimed", "accepted", "closed"] = "issuing"
    identity: DashboardTurnIdentity | None = None


class DashboardDelivery:
    """Bounded same-loop handoff with a consumer-owned, finally-closed identity.

    `dispatch` is for a future authenticated ingress. `receive` MUST enclose the
    channel handler before media persistence or message mutation. Metadata and
    a receipt alone confer no permission. Accepted turns retain a capacity slot
    until consumer exit. No authority is inherited by steer/HITL reconstruction.

    Close invalidates identities and wakes handshakes, but does not cancel
    arbitrary consumer bodies, stop executor SQL, drain the database pool, or
    recall already returned results. Production stream/observer policy and
    bounded ingress scheduling (so waiting does not block stop/steer) are gates.
    """

    def __init__(self, registry: DashboardOriginRegistry, *, max_inflight: int = 64) -> None:
        if type(max_inflight) is not int or not 1 <= max_inflight <= 1024:
            raise ValueError("invalid_dashboard_delivery_limit")
        self._registry = registry
        self._max_inflight = max_inflight
        self._entries: dict[int, _Delivery] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False

    def _check_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif loop is not self._loop:
            raise DashboardDeliveryError("dashboard_delivery_loop_mismatch")
        return loop

    def _current(self, entry: _Delivery, phase: str) -> None:
        if (
            self._closed
            or self._entries.get(id(entry.message)) is not entry
            or entry.phase != phase
        ):
            raise DashboardDeliveryError("dashboard_delivery_inactive")

    def _abort(self, entry: _Delivery) -> None:
        if entry.identity is not None:
            entry.identity.close()
        self._registry.revoke(entry.message)
        entry.phase = "closed"
        if self._entries.get(id(entry.message)) is entry:
            del self._entries[id(entry.message)]
        # Boolean outcomes avoid unobserved Future exceptions after cancellation.
        for future in (entry.ready, entry.commit):
            if not future.done():
                future.set_result(False)

    async def verify(self, check: Callable[[], _T]) -> _T:
        """Bound ingress auth work without accepting caller-provided identity."""
        self._check_loop()
        if self._closed:
            raise DashboardDeliveryError("dashboard_delivery_inactive")
        result = await self._registry.verify(check)
        if self._closed:
            raise DashboardDeliveryError("dashboard_delivery_inactive")
        return result

    async def dispatch(
        self,
        message: DashboardInbound,
        *,
        caller_user_id: int,
        agent_id: str,
        thread_id: str,
        enqueue: Callable[[DashboardInbound], object],
        timeout_s: float = 5,
        not_after_epoch: float | None = None,
        source_check: Callable[[], None] | None = None,
    ) -> DashboardDeliveryReceipt:
        loop = self._check_loop()
        if (
            self._closed
            or type(message) is not DashboardInbound
            or id(message) in self._entries
            or len(self._entries) >= self._max_inflight
            or type(timeout_s) not in (int, float)
            or not math.isfinite(timeout_s)
            or not 0 < timeout_s <= 30
            or not callable(enqueue)
        ):
            raise DashboardDeliveryError("dashboard_delivery_denied")
        entry = _Delivery(message, loop.create_future(), loop.create_future())
        self._entries[id(message)] = entry
        committed = False
        try:
            async with asyncio.timeout(timeout_s):
                await self._registry.issue(
                    message,
                    caller_user_id=caller_user_id,
                    agent_id=agent_id,
                    thread_id=thread_id,
                    not_after_epoch=not_after_epoch,
                    source_check=source_check,
                )
                self._current(entry, "issuing")
                entry.phase = "queued"
                returned = enqueue(message)
                # The supported enqueue call is synchronous. Its return value
                # is NEVER acceptance evidence (including an already started Task).
                if inspect.iscoroutine(returned):
                    returned.close()
                    raise DashboardDeliveryError("dashboard_delivery_enqueue_not_sync")
                if not await asyncio.shield(entry.ready):
                    raise DashboardDeliveryError("dashboard_delivery_not_claimed")
                self._current(entry, "claimed")
                if entry.identity is None:
                    raise DashboardDeliveryError("dashboard_delivery_not_claimed")
                entry.identity.check_current(agent_id=agent_id, thread_id=thread_id)
                # Linearization point: no await between this commit and receipt.
                # A later ingress disconnect cannot assert the turn never started.
                entry.phase = "accepted"
                entry.commit.set_result(True)
                committed = True
                return DashboardDeliveryReceipt()
        except Exception:
            # Includes enqueue-raised TimeoutError: only our constant code may
            # escape, never a trusted callback's potentially sensitive body.
            raise DashboardDeliveryError("dashboard_delivery_unavailable") from None
        finally:
            if not committed:
                self._abort(entry)

    @asynccontextmanager
    async def receive(self, message: InboundMessage) -> AsyncIterator[DashboardTurnIdentity]:
        self._check_loop()
        entry = self._entries.get(id(message))
        if type(message) is not DashboardInbound or entry is None or entry.message is not message:
            raise DashboardDeliveryError("dashboard_delivery_missing")
        # Duplicate/copy attempts must not abort the legitimate claimant.
        self._current(entry, "queued")
        entry.phase = "claiming"
        identity: DashboardTurnIdentity | None = None
        try:
            identity = await self._registry.consume(message)
            self._current(entry, "claiming")
            entry.identity = identity
            identity.check_current(agent_id=identity.agent_id, thread_id=identity.thread_id)
            entry.phase = "claimed"
            entry.ready.set_result(True)
            if not await asyncio.shield(entry.commit):
                raise DashboardDeliveryError("dashboard_delivery_not_committed")
            self._current(entry, "accepted")
            identity.check_current(agent_id=identity.agent_id, thread_id=identity.thread_id)
            yield identity
        finally:
            # Also covers a claim returned just as dispatch/close invalidated
            # the entry, before the identity was attached to that entry.
            if identity is not None:
                identity.close()
            self._abort(entry)

    def close(self) -> None:
        """Invalidate pending/active deliveries on the owning running event loop."""
        self._check_loop()
        self._closed = True
        for entry in tuple(self._entries.values()):
            self._abort(entry)
