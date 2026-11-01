"""Authenticated NEW-upload helpers used by the opt-in MemoryLake router.

The request receiver composes bearer/DB authorization with bounded streaming and
shared snapshots. No approval, remote command or router registration is implied.
Legacy UploadFile helpers below still require a trusted server caller and source;
FastAPI owns their input lifecycle. Their byte limits do not bound earlier spool.
Native build_app middleware stays in place; upstream buffering and final-send
authorization are not made atomic by these helpers.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, BinaryIO

from fastapi import Request, UploadFile

from octop.api.common.upload_limit import read_upload_capped
from octop.infra.auth.tokens import decode_token
from octop.infra.connectors.memorylake.contract import CallerContext, _authorize, _identifier
from octop.infra.connectors.memorylake.file_binding import MemoryLakeFileBinding, TransportFactory
from octop.infra.connectors.memorylake.file_runtime import MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.storage import validate_connection_snapshot
from octop.infra.connectors.memorylake.upload_io import UploadReadAdmission
from octop.infra.connectors.memorylake.upload_source import (
    EphemeralUploadSource,
    ScopedUploadSource,
    UploadSourceError,
)


class _SettledUploadReader:
    """Own the underlying upload-read worker through request cancellation.

    Own the executor Future for public UploadFile.file.read, rather than an
    intermediate Task around Starlette's hidden worker. Task cancel-all cannot
    make this Future appear completed while its thread is still running.
    Cancellation waits for that chunk, then propagates without reading another;
    the source keeps its reservation meanwhile. This is not a hard I/O timeout,
    and server shutdown must drain these operations before closing upload files.
    """

    def __init__(self, file: BinaryIO, size: int | None, admission: UploadReadAdmission) -> None:
        self._file = file
        self._admission = admission
        self.size = size

    async def read(self, size: int = -1) -> bytes:
        self._admission.check_current(self._file)
        job = self._admission.submit_read(self._file, size)
        cancelled = False
        while not job.done():
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        if cancelled:
            if not job.cancelled():
                job.exception()  # Observe late worker failures, never log their body.
            raise asyncio.CancelledError("memorylake_upload_read_cancelled")
        content = job.result()
        self._admission.check_current(self._file)
        return content


async def capture_new_memorylake_upload(
    source: EphemeralUploadSource | ScopedUploadSource,
    caller: CallerContext,
    upload: UploadFile,
    *,
    io_admission: UploadReadAdmission,
) -> str:
    filename = upload.filename or "upload.bin"
    file = upload.file
    reader = _SettledUploadReader(file, upload.size, io_admission)

    async def read_bytes(max_bytes: int) -> bytes:
        return await read_upload_capped(
            reader,
            max_bytes=max_bytes,
            # Keep an overflowing first chunk bounded even for a tiny budget.
            chunk_size=min(1024 * 1024, max_bytes + 1),
        )

    try:
        with io_admission.exclusive(file):
            return await source.capture(caller, filename, read_bytes)
    except Exception:
        # No filename, content, token, reader exception or path is reflected.
        raise UploadSourceError("memorylake_upload_unavailable") from None


async def capture_runtime_memorylake_upload(
    runtime: MemoryLakeFileRuntime,
    caller: CallerContext,
    upload: UploadFile,
    *,
    scoped_source: ScopedUploadSource | None = None,
) -> str:
    """Unregistered server-owner entry: retain operation scope through I/O settle.

    Caller identity still comes from a future authenticated handler, not a model.
    The runtime's authorization factory must supply the current owner-only check.
    Scoped input must belong to this resource owner; stale/invalid/foreign views
    never fall back to the unscoped prototype path. This adds no JWT/HTTP route.
    """
    with runtime.operation():
        if scoped_source is not None:
            if not isinstance(scoped_source, ScopedUploadSource):
                raise UploadSourceError("upload_scope_unavailable")
            scoped_source.check_owner(runtime.source, caller)
        return await capture_new_memorylake_upload(
            scoped_source if scoped_source is not None else runtime.source,
            caller,
            upload,
            io_admission=runtime.io,
        )


if TYPE_CHECKING:
    from octop.infra.server import OctopServer


@asynccontextmanager
async def authorize_memorylake_file_request(
    request: Request,
    server: OctopServer,
    *,
    agent_id: str,
    binding_id: str,
    transport_factory: TransportFactory | None = None,
    transport_provider: Callable[[CallerContext, str, str, Callable[[], None]], TransportFactory]
    | None = None,
    check_deployment: Callable[[], None] | None = None,
    timeout_s: float = 5,
    max_request_s: float = 30,
    require_write: bool = True,
) -> AsyncIterator[MemoryLakeFileBinding]:
    """Testable authenticated ingress scope; deliberately NOT a registered route.

    Run before reading any request body (never use FastAPI File/Form dependencies).
    Only the bearer header is accepted; query aliases and cached request users are
    not authority. Current SQL owner scope determines connector and revision. The
    trusted server factory must be side-effect-free and cannot come from a client.
    max_request_s bounds identity validity at checks, not arbitrary code within
    yield; callers must provide their own execution deadline/operation scope.
    Every request re-verifies the current JWT key. In-flight signing-key rotation
    is NOT a distributed revocation mechanism. Closing this scope does not retract
    a previously granted write or erase its durable recovery evidence.
    """
    from octop.infra.errors import ErrorCode, OctopError

    closed = False
    binding: MemoryLakeFileBinding | None = None
    runtime = server.memorylake_file_runtime
    services = server.services
    loop = asyncio.get_running_loop()
    expires_at = 0.0
    deadline = loop.time()

    def check_current() -> None:
        nonlocal closed
        if check_deployment is not None:
            check_deployment()
        now = time.time()
        mono = loop.time()
        if (
            closed
            or runtime is None
            or runtime.closed
            or server.memorylake_file_runtime is not runtime
            or server.services is not services
            or not math.isfinite(now)
            or not math.isfinite(mono)
            or now >= expires_at
            or mono >= deadline
        ):
            # Observing expiry permanently burns this invocation, even if the
            # wall clock subsequently moves backwards.
            closed = True
            raise OctopError.localized(ErrorCode.AUTH_FAILED)

    try:
        try:
            agent_id, binding_id = _identifier(agent_id), _identifier(binding_id)
            if (transport_factory is None) == (transport_provider is None):
                raise ValueError
            if check_deployment is not None:
                check_deployment()
            for value, maximum in ((timeout_s, 30), (max_request_s, 300)):
                if (
                    type(value) not in (int, float)
                    or not math.isfinite(value)
                    or not 0 < value <= maximum
                ):
                    raise ValueError
            deadline = loop.time() + max_request_s
            headers = [v for k, v in request.scope["headers"] if k.lower() == b"authorization"]
            if (
                runtime is None
                or services is None
                or runtime.closed
                or request.scope.get("query_string")
                or len(headers) != 1
                or not 8 <= len(headers[0]) <= 8192
                or type(require_write) is not bool
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
            # Anchor JWT lifetime before the executor wait. Recomputing against
            # wall time after an await would extend a token during clock rollback.
            auth_mono, auth_wall = loop.time(), time.time()
            if any(
                type(value) not in (int, float) or not math.isfinite(value)
                for value in (auth_mono, auth_wall)
            ):
                raise ValueError

            def authenticate() -> tuple[CallerContext, str, float]:
                secret = services.secret_repo.get_bounded("jwt", max_bytes=4096)
                if not secret:
                    raise ValueError
                claims = decode_token(secret, encoded)
                user_id, expiry = claims.get("sub"), claims.get("exp")
                if (
                    type(user_id) is not int
                    or user_id <= 0
                    or isinstance(expiry, bool)
                    or not isinstance(expiry, (int, float))
                    or not math.isfinite(expiry)
                ):
                    raise ValueError
                authorized = services.memorylake_binding_repo.load_owned_write_binding(
                    binding_id, caller_user_id=user_id, agent_id=agent_id
                )
                if authorized is None or (require_write and authorized.writes_enabled is not True):
                    raise ValueError
                connector_id = _identifier(authorized.binding.connector_id)
                connection = validate_connection_snapshot(
                    authorized,
                    caller_user_id=user_id,
                    agent_id=agent_id,
                    binding_id=binding_id,
                    connector_id=connector_id,
                )
                caller = CallerContext(
                    user_id,
                    agent_id,
                    connection.scope.owner_user_id,
                    connection.scope.authorization_revision,
                )
                _authorize(connection.scope, caller)
                return caller, connector_id, float(expiry)

            auth_deadline = min(deadline, loop.time() + timeout_s)
            async with asyncio.timeout_at(auth_deadline) as auth_timeout:
                with runtime.operation():
                    caller, connector_id, expires_at = await runtime.database.submit(authenticate)
                    deadline = min(deadline, auth_mono + max(0.0, expires_at - auth_wall))
                    auth_timeout.reschedule(min(auth_deadline, deadline))
                    check_current()
                factory = (
                    transport_provider(caller, binding_id, connector_id, check_current)
                    if transport_provider is not None
                    else transport_factory
                )
                assert factory is not None
                binding = await MemoryLakeFileBinding.create(
                    runtime=runtime,
                    caller=caller,
                    binding_id=binding_id,
                    connector_id=connector_id,
                    binding_repo=services.memorylake_binding_repo,
                    submission_repo=services.memorylake_submission_repo,
                    transport_factory=factory,
                    check_invocation=check_current,
                    require_write=require_write,
                )
                check_current()
        except Exception:
            # Never reflect a token, repository exception, filename or evidence.
            raise OctopError.localized(ErrorCode.AUTH_FAILED) from None
        yield binding
        check_current()
    finally:
        closed = True
        if binding is not None:
            binding.close()


async def receive_memorylake_file_request(
    request: Request,
    server: OctopServer,
    *,
    agent_id: str,
    binding_id: str,
    transport_factory: TransportFactory | None = None,
    transport_provider: Callable[[CallerContext, str, str, Callable[[], None]], TransportFactory]
    | None = None,
    check_deployment: Callable[[], None] | None = None,
    timeout_s: float = 5,
    max_request_s: float = 30,
) -> str:
    """Authenticated bounded NEW-upload receive, still unregistered.

    One absolute deadline covers authentication and cooperative body receiving.
    The shared source reservation precedes the first receive; no UploadFile,
    spool, approval or remote command is created. Runtime owns the operation
    until parsing/publication exits. This cannot limit prior proxy/middleware
    buffering, forcibly interrupt synchronous parser code or stop DB workers.
    Final HTTP send and multi-worker routing remain separate integration gates.
    """
    from octop.api.common.memorylake_multipart import read_memorylake_multipart
    from octop.infra.errors import ErrorCode, OctopError

    try:
        if (
            type(max_request_s) not in (int, float)
            or not math.isfinite(max_request_s)
            or not 0 < max_request_s <= 300
        ):
            raise ValueError
        runtime = server.memorylake_file_runtime
        if runtime is None:
            raise ValueError
        deadline = asyncio.get_running_loop().time() + max_request_s
        async with asyncio.timeout_at(deadline):
            async with authorize_memorylake_file_request(
                request,
                server,
                agent_id=agent_id,
                binding_id=binding_id,
                transport_factory=transport_factory,
                transport_provider=transport_provider,
                check_deployment=check_deployment,
                timeout_s=timeout_s,
                max_request_s=max_request_s,
                require_write=True,
            ) as binding:
                with runtime.operation():
                    binding.check_invocation()
                    binding.source.check_owner(runtime.source, binding.caller)

                    async def receive(maximum: int) -> tuple[str, bytes]:
                        return await read_memorylake_multipart(
                            request,
                            max_file_bytes=maximum,
                            check_current=binding.check_invocation,
                            timeout_s=max_request_s,
                        )

                    identifier = await binding.source.capture_named(binding.caller, receive)
                    binding.check_invocation()
                    return identifier
    except OctopError:
        raise
    except Exception:
        raise OctopError.localized(ErrorCode.RESOURCE_NOT_ALLOWED) from None
