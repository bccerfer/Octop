"""Dormant dashboard-origin boundary, NOT a registered authentication endpoint.

Only an authenticated server ingress may call ``issue`` with its verified caller
and resolved agent/thread. These arguments must never be taken from model args,
metadata or an IM-to-owner fallback. The private object must cross the same-process
queue unchanged. No claim is accepted from JSON, configurable or checkpoints.

Origin evidence is not MemoryLake permission: tools still need current binding,
credential and owner checks. No WS/processor/middleware wiring is installed here.
Independent HITL, peer/steer reconstruction and cross-worker delivery have no grant.
Identity ABA history and full graph lifetime integration remain production gates.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, NoReturn, SupportsIndex, TypeVar

from octop_gateway.models import ChannelSubject, InboundMessage
from pydantic import PrivateAttr

from octop.infra.connectors.memorylake.contract import _identifier

if TYPE_CHECKING:
    from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo

# Internal misuse guard, not a cryptographic capability or a security boundary
# against trusted Python code with access to this process. Never serialized.
_IDENTITY_MINT = object()
_T = TypeVar("_T")


class DashboardOriginError(ValueError):
    """Body-free internal code; never echo messages or identity fields."""


def _check_source(check: Callable[[], None] | None) -> None:
    if check is not None:
        try:
            check()
        except Exception:
            raise DashboardOriginError("dashboard_origin_source_inactive") from None


class DashboardInbound(InboundMessage):
    """Internal same-process carrier. Being this type alone proves nothing."""

    _origin: object | None = PrivateAttr(default=None)


# The gateway declares ChannelSubject after InboundMessage. Its unresolved
# forward reference needs the defining namespace when subclassed in our module.
DashboardInbound.model_rebuild(_types_namespace={"ChannelSubject": ChannelSubject})


class DashboardTurnIdentity:
    """Short-lived internal evidence, not a serializable authorization token.

    A future consumer must close this in ``finally`` on turn completion, error
    or cancellation, BEFORE resetting ambient context. Child tasks retain the
    same invalidated object. Middleware must check before AND after awaited work;
    resetting a ContextVar alone cannot revoke copies inherited by child tasks.
    """

    __slots__ = (
        "_active",
        "_agent_id",
        "_clock",
        "_connection_id",
        "_expires_at",
        "_minted",
        "_not_after_epoch",
        "_source_check",
        "_thread_id",
        "_user_id",
        "_wall_clock",
    )

    def __init__(
        self,
        user_id: int,
        agent_id: str,
        thread_id: str,
        expires_at: float,
        clock: Callable[[], float],
        *,
        _mint: object | None = None,
        not_after_epoch: float | None = None,
        wall_clock: Callable[[], float] = time.time,
        connection_id: str | None = None,
        source_check: Callable[[], None] | None = None,
    ) -> None:
        self._user_id, self._agent_id, self._thread_id = user_id, agent_id, thread_id
        self._expires_at, self._clock, self._active = expires_at, clock, True
        self._minted = _mint is _IDENTITY_MINT
        self._not_after_epoch, self._wall_clock = not_after_epoch, wall_clock
        self._connection_id = connection_id
        self._source_check = source_check

    @property
    def user_id(self) -> int:
        return self._user_id

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def thread_id(self) -> str:
        return self._thread_id

    def check_current(self, *, agent_id: str, thread_id: str) -> None:
        if not self._minted or not self._active:
            raise DashboardOriginError("dashboard_identity_inactive")
        try:
            _check_source(self._source_check)
        except DashboardOriginError:
            self.close()
            raise
        if self._active and (
            self._clock() >= self._expires_at
            or (self._not_after_epoch is not None and self._wall_clock() >= self._not_after_epoch)
        ):
            # Observed expiry is irreversible even if the wall clock rolls back.
            self.close()
        if (
            not self._minted
            or not self._active
            or agent_id != self._agent_id
            or thread_id != self._thread_id
        ):
            raise DashboardOriginError("dashboard_identity_inactive")

    def close(self) -> None:
        self._active = False

    def check_inbound(self, message: DashboardInbound) -> None:
        """Recheck routing against this consumed identity after handoff awaits.

        This does not consume/reissue origin and grants no authority to a copy.
        A channel may use it on a private execution snapshot of its claimed turn.
        """
        self.check_current(agent_id=self._agent_id, thread_id=self._thread_id)
        DashboardOriginRegistry._shape(
            _Pending(
                message,
                self._user_id,
                self._agent_id,
                self._thread_id,
                f"{self._agent_id}:dashboard:{self._user_id}:dm",
                self._expires_at,
                self._not_after_epoch,
                connection_id=self._connection_id,
            )
        )

    def __copy__(self) -> NoReturn:
        raise TypeError("dashboard_identity_not_serializable")

    def __deepcopy__(self, memo: dict[int, object]) -> NoReturn:
        raise TypeError("dashboard_identity_not_serializable")

    def __reduce_ex__(self, protocol: SupportsIndex) -> NoReturn:
        raise TypeError("dashboard_identity_not_serializable")


@dataclass(repr=False)
class _Pending:
    message: DashboardInbound
    user_id: int
    agent_id: str
    thread_id: str
    session_key: str
    expires_at: float
    not_after_epoch: float | None = None
    connection_id: str | None = None
    source_check: Callable[[], None] | None = None
    phase: Literal["issuing", "ready", "consuming"] = "issuing"


class DashboardOriginRegistry:
    """Bounded one-use origin registry belonging to one server event loop.

    Pending entries are expired lazily at the next registry operation; both
    issuance and redemption check the deadline. A consumed/rejected origin cannot
    be retried. Queue overflow/cancellation should explicitly ``revoke`` pending
    messages. After consumption the holder must instead close its identity.
    This registry bounds outstanding executor jobs but cannot interrupt SQLite
    or impose a SQL execution deadline. Shutdown/drain integration is not wired;
    a future owner must let jobs finish before closing the repository's pool.
    """

    def __init__(
        self,
        *,
        identities: MemoryLakeBindingRepo,
        ttl_s: float = 60,
        max_pending: int = 64,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if (
            type(ttl_s) not in (int, float)
            or not math.isfinite(ttl_s)
            or not 0 < ttl_s <= 300
            or type(max_pending) is not int
            or not 1 <= max_pending <= 1024
        ):
            raise ValueError("invalid_dashboard_origin_limits")
        self._identities = identities
        self._ttl_s, self._max_pending, self._clock = ttl_s, max_pending, clock
        self._wall_clock = wall_clock
        self._pending: dict[object, _Pending] = {}
        # A cancelled await does not stop its executor thread. Keep a separate
        # bounded set until each shielded Future REALLY finishes, not merely
        # until the origin entry is revoked/expired/consumed.
        self._auth_jobs: set[asyncio.Future[Any]] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    def _now(self) -> float:
        now = self._clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            raise DashboardOriginError("dashboard_origin_clock_invalid")
        return float(now)

    def _check_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise DashboardOriginError("dashboard_origin_loop_mismatch")

    def _wall_now(self) -> float:
        now = self._wall_clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            raise DashboardOriginError("dashboard_origin_clock_invalid")
        return float(now)

    def _expired(self, pending: _Pending, now: float) -> bool:
        return now >= pending.expires_at or (
            pending.not_after_epoch is not None and self._wall_now() >= pending.not_after_epoch
        )

    def _expire(self, now: float) -> None:
        self._pending = {
            token: pending
            for token, pending in self._pending.items()
            if not self._expired(pending, now)
        }

    @staticmethod
    def _shape(pending: _Pending) -> None:
        message = pending.message
        subject = message.channel_subject
        first_word = message.text.split(maxsplit=1)
        if (
            type(message) is not DashboardInbound
            or message.channel_type != "dashboard"
            or message.channel_id != "octop-dashboard"
            or message.tenant_id != pending.agent_id
            or not isinstance(subject, ChannelSubject)
            or subject.subject_id != str(pending.user_id)
            or subject.chat_type not in ("", "dm", "direct")
            or message.channel_session_id != pending.session_key
            or message.group_context is not None
            # Native stop/cancel run a pre-lock side effect, before a delivery
            # consumer could authorize this envelope. They use the independent
            # native control path, never a MemoryLake turn grant.
            or (first_word and first_word[0].lower() in ("/stop", "/cancel"))
            or not isinstance(message.metadata, dict)
            or message.metadata.get("chat_type") not in (None, "dm")
            or bool(message.metadata.get("is_group"))
            or message.metadata.get("thread_id") != pending.thread_id
            or message.metadata.get("session_key") != pending.session_key
            or message.metadata.get("ws_connection_id") != pending.connection_id
            or message.metadata.get("as_user") is not None
            or message.metadata.get("target_agent_ids")
            or message.metadata.get("conversation_mode") not in (None, "craft")
        ):
            raise DashboardOriginError("dashboard_origin_shape_denied")

    def _auth_finished(self, future: asyncio.Future[Any]) -> None:
        self._auth_jobs.discard(future)
        if not future.cancelled():
            # The outer turn may already be cancelled. Retrieve (never log)
            # any late error so it cannot become an unhandled Future exception.
            future.exception()

    async def verify(self, check: Callable[[], _T]) -> _T:
        """Run trusted blocking ingress verification within the shared job quota.

        Cancellation burns the caller's attempt but cannot release the slot or
        stop its thread. This is not an API for model-provided callbacks.
        """
        self._check_loop()
        # A fast executor Future can finish before add_done_callback; awaiting
        # shield(done_future) need not yield. Reap genuinely completed jobs now
        # rather than treating a delayed callback as still-running SQL. Running
        # jobs retain their slot, including after outer cancellation/revocation.
        for completed in tuple(self._auth_jobs):
            if completed.done():
                self._auth_finished(completed)
        if len(self._auth_jobs) >= self._max_pending:
            raise DashboardOriginError("dashboard_origin_auth_capacity")
        try:
            future = asyncio.get_running_loop().run_in_executor(None, check)
            self._auth_jobs.add(future)
            future.add_done_callback(self._auth_finished)
            return await asyncio.shield(future)
        except Exception:
            raise DashboardOriginError("dashboard_origin_identity_unavailable") from None

    async def _authorized(self, pending: _Pending) -> None:
        check = partial(
            self._identities.owns_dashboard_thread,
            caller_user_id=pending.user_id,
            agent_id=pending.agent_id,
            thread_id=pending.thread_id,
            session_key=pending.session_key,
        )
        allowed = await self.verify(check)
        if allowed is not True:
            raise DashboardOriginError("dashboard_origin_identity_denied")

    async def issue(
        self,
        message: DashboardInbound,
        *,
        caller_user_id: int,
        agent_id: str,
        thread_id: str,
        not_after_epoch: float | None = None,
        source_check: Callable[[], None] | None = None,
    ) -> None:
        self._check_loop()
        if (
            type(message) is not DashboardInbound
            or message._origin is not None
            or type(caller_user_id) is not int
            or caller_user_id <= 0
        ):
            raise DashboardOriginError("dashboard_origin_issue_denied")
        agent_id, thread_id = _identifier(agent_id), _identifier(thread_id)
        connection_id = (
            message.metadata.get("ws_connection_id") if isinstance(message.metadata, dict) else None
        )
        if connection_id is not None:
            connection_id = _identifier(connection_id)
        now = self._now()
        expires_at = now + self._ttl_s
        if not_after_epoch is not None:
            wall_now = self._wall_now()
            if (
                type(not_after_epoch) not in (int, float)
                or not math.isfinite(not_after_epoch)
                or not_after_epoch <= wall_now
            ):
                raise DashboardOriginError("dashboard_origin_auth_expired")
            # Wall-clock rollback must never extend an already minted grant;
            # forward jumps are also enforced independently at every check.
            expires_at = min(expires_at, now + not_after_epoch - wall_now)
        pending = _Pending(
            message,
            caller_user_id,
            agent_id,
            thread_id,
            f"{agent_id}:dashboard:{caller_user_id}:dm",
            expires_at,
            not_after_epoch,
            connection_id=connection_id,
        )
        self._shape(pending)
        _check_source(source_check)
        pending.source_check = source_check
        self._expire(now)
        if len(self._pending) >= self._max_pending:
            raise DashboardOriginError("dashboard_origin_capacity")
        # Reserve BEFORE SQL: in-flight signing consumes capacity and can be
        # revoked. The exact object is burned on any failure/cancellation.
        token = object()
        message._origin = token
        self._pending[token] = pending
        try:
            await self._authorized(pending)
            _check_source(pending.source_check)
            self._shape(pending)
            if (
                self._expired(pending, self._now())
                or message._origin is not token
                or self._pending.get(token) is not pending
            ):
                raise DashboardOriginError("dashboard_origin_issue_denied")
            pending.phase = "ready"
        except BaseException:
            self._pending.pop(token, None)
            raise

    async def consume(self, message: InboundMessage) -> DashboardTurnIdentity:
        self._check_loop()
        self._expire(self._now())
        if type(message) is not DashboardInbound or type(message._origin) is not object:
            raise DashboardOriginError("dashboard_origin_missing")
        token = message._origin
        pending = self._pending.get(token)
        if pending is None or pending.message is not message:
            raise DashboardOriginError("dashboard_origin_missing")
        if pending.phase == "consuming":
            # A replay must not cancel the legitimate winner's in-flight check.
            raise DashboardOriginError("dashboard_origin_already_consumed")
        if pending.phase != "ready":
            del self._pending[token]
            raise DashboardOriginError("dashboard_origin_not_issued")
        pending.phase = "consuming"
        try:
            self._shape(pending)
            _check_source(pending.source_check)
            await self._authorized(pending)
            _check_source(pending.source_check)
            self._shape(pending)
            if (
                self._expired(pending, self._now())
                or message._origin is not token
                or self._pending.get(token) is not pending
            ):
                raise DashboardOriginError("dashboard_origin_expired_or_revoked")
            return DashboardTurnIdentity(
                pending.user_id,
                pending.agent_id,
                pending.thread_id,
                pending.expires_at,
                self._now,
                _mint=_IDENTITY_MINT,
                not_after_epoch=pending.not_after_epoch,
                wall_clock=self._wall_now,
                connection_id=pending.connection_id,
                source_check=pending.source_check,
            )
        finally:
            self._pending.pop(token, None)

    def revoke(self, message: InboundMessage) -> None:
        """Invalidate a still-pending exact object, not an already claimed turn."""
        self._check_loop()
        self._expire(self._now())
        if type(message) is not DashboardInbound or type(message._origin) is not object:
            return
        pending = self._pending.get(message._origin)
        if pending is not None and pending.message is message:
            del self._pending[message._origin]
