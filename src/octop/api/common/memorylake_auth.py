"""Strict owner identity for MemoryLake connection management, before body reads.

This scope does not replace each business transaction's fresh SQL authorization.
The monotonic lifetime bounds identity checks, not arbitrary caller work at yield.
Fresh disclosure checks reauthenticate the current JWT key and owner in bounded DB jobs.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from fastapi import Request

from octop.infra.auth.tokens import decode_token
from octop.infra.connectors.memorylake.contract import _identifier
from octop.infra.errors import ErrorCode, OctopError

if TYPE_CHECKING:
    from octop.infra.connectors.memorylake.application import MemoryLakeApplication
    from octop.infra.server import OctopServer


@dataclass(frozen=True)
class OwnerRequest:
    user_id: int
    agent_id: str
    _check: Callable[[], None] = field(repr=False, compare=False)
    _disclose: Callable[[], Awaitable[None]] = field(repr=False, compare=False)

    def check_current(self) -> None:
        self._check()

    async def check_disclosure(self) -> None:
        await self._disclose()


@asynccontextmanager
async def authorize_memorylake_owner_request(
    request: Request,
    server: OctopServer,
    *,
    agent_id: str,
    application: MemoryLakeApplication,
    timeout_s: float = 5,
    max_request_s: float = 30,
) -> AsyncIterator[OwnerRequest]:
    """Mint a request-local owner identity using one bounded shared DB worker.

    Cancellation releases the HTTP operation, not the actual executor job's
    admission slot. Runtime drain still waits for that worker. No request body,
    connector key, cached request user, query token or client actor is consumed.
    """
    closed = False
    loop = asyncio.get_running_loop()
    services = server.services
    runtime = application.runtime
    expires_at = 0.0
    deadline = loop.time()

    def check_current() -> None:
        nonlocal closed
        try:
            application.check_current()
            now, mono = time.time(), loop.time()
            if (
                closed
                or runtime.closed
                or application.runtime is not runtime
                or server.memorylake_application is not application
                or server.memorylake_file_runtime is not runtime
                or server.services is not services
                or type(now) not in (int, float)
                or type(mono) not in (int, float)
                or not math.isfinite(now)
                or not math.isfinite(mono)
                or now >= expires_at
                or mono >= deadline
            ):
                raise ValueError
        except Exception:
            closed = True
            raise OctopError.localized(ErrorCode.AUTH_FAILED) from None

    try:
        try:
            agent_id = _identifier(agent_id)
            for value, maximum in ((timeout_s, 30), (max_request_s, 300)):
                if (
                    type(value) not in (int, float)
                    or not math.isfinite(value)
                    or not 0 < value <= maximum
                ):
                    raise ValueError
            deadline = loop.time() + max_request_s
            application.check_current()
            headers = [v for k, v in request.scope["headers"] if k.lower() == b"authorization"]
            if (
                services is None
                or server.services is not services
                or application.runtime is not runtime
                or runtime.closed
                or server.memorylake_application is not application
                or server.memorylake_file_runtime is not runtime
                or request.scope.get("query_string")
                or len(headers) != 1
                or not 8 <= len(headers[0]) <= 8192
            ):
                raise ValueError
            scheme, separator, token = headers[0].partition(b" ")
            if (
                scheme.lower() != b"bearer"
                or separator != b" "
                or not token
                or any(c <= 32 or c >= 127 for c in token)
            ):
                raise ValueError
            encoded = token.decode("ascii")

            def authenticate() -> tuple[int, float]:
                secret = services.secret_repo.get_bounded("jwt", max_bytes=4096)
                if not secret:
                    raise ValueError
                claims = decode_token(secret, encoded)
                user_id, expiry = claims.get("sub"), claims.get("exp")
                if (
                    type(user_id) is not int
                    or user_id <= 0
                    or not isinstance(expiry, (int, float))
                    or isinstance(expiry, bool)
                    or not math.isfinite(expiry)
                    or not services.memorylake_binding_repo.owns_agent(
                        caller_user_id=user_id, agent_id=agent_id
                    )
                ):
                    raise ValueError
                return user_id, float(expiry)

            anchor_wall, anchor_mono = time.time(), loop.time()
            if any(
                type(value) not in (int, float) or not math.isfinite(value)
                for value in (anchor_wall, anchor_mono)
            ):
                raise ValueError
            async with asyncio.timeout_at(min(deadline, anchor_mono + timeout_s)):
                with runtime.operation():
                    user_id, expires_at = await runtime.database.submit(authenticate)
                    # Anchor before SQL: a wall-clock rollback while waiting must
                    # not replenish the JWT's originally remaining lifetime.
                    deadline = min(deadline, anchor_mono + max(0.0, expires_at - anchor_wall))
                    check_current()

            async def check_disclosure() -> None:
                nonlocal closed
                try:
                    check_current()
                    async with asyncio.timeout_at(min(deadline, loop.time() + timeout_s)):
                        with runtime.operation():
                            current_user, current_expiry = await runtime.database.submit(
                                authenticate
                            )
                            check_current()
                            if current_user != user_id or current_expiry != expires_at:
                                raise ValueError
                except Exception:
                    closed = True
                    raise OctopError.localized(ErrorCode.AUTH_FAILED) from None

            owner = OwnerRequest(user_id, agent_id, check_current, check_disclosure)
        except Exception:
            raise OctopError.localized(ErrorCode.AUTH_FAILED) from None
        owner.check_current()
        yield owner
        owner.check_current()
    finally:
        closed = True
