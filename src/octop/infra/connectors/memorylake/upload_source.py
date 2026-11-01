"""Unregistered, process-local snapshots of NEW explicitly supplied upload bytes.

Not a workspace/attachment reader: no host path, BackendWorkspace, native save or
remote upload occurs. Trusted server code must authenticate the caller and provide
a fresh ownership authorization callback. Full caller revision is bound; prepare
and submit can reread until expiry/revocation. Restart loses bytes and requires a
new upload/approval. Retention removes references, NOT memory zeroization. This
prototype does not settle durable/encrypted retention or multi-worker routing.
Limits cover store-owned references, not snapshots already returned to services or
whole-process heap. Revocation/expiry does not revoke durable DB approval. Trusted
authorization callbacks must cooperate with cancellation; no task is force-killed.
"""

from __future__ import annotations

import asyncio
import math
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import cast

from octop.infra.connectors.memorylake.contract import CallerContext, _identifier
from octop.infra.connectors.memorylake.submissions import FileSnapshot


class UploadSourceError(ValueError):
    """Body-free internal reason; no file contents, filename or auth details."""


@dataclass(frozen=True)
class _Entry:
    caller: CallerContext
    snapshot: FileSnapshot
    expires_at: float
    timer: asyncio.TimerHandle
    generation: object
    namespace: tuple[str, str] | None


class ScopedUploadSource:
    """Trusted binding/caller routing over one store, NOT an auth capability.

    Only the composition root may bind these views, with a fresh owner-only DB
    authorizer for the same binding/connector. No endpoint/profile/credentials
    are accepted. Recreating a view is not permission: every access still checks
    current authorization. Closing it invalidates just this view, not approvals,
    another view or the shared resource owner. Identity-history ABA is not solved.
    """

    def __init__(
        self,
        store: EphemeralUploadSource,
        namespace: tuple[str, str],
        caller: CallerContext,
        authorize: Callable[[CallerContext], Awaitable[None]],
    ) -> None:
        self._store = store
        self._namespace = namespace
        self._caller = caller
        self._authorize = authorize
        self._closed = False

    def _check(self, store: EphemeralUploadSource, caller: CallerContext) -> None:
        if self._closed or self._store is not store or self._caller != caller:
            raise UploadSourceError("upload_scope_unavailable")

    def check_owner(self, store: EphemeralUploadSource, caller: CallerContext) -> None:
        """Local routing/liveness only; subsequent I/O still requires fresh DB auth."""
        store._check(caller, self)

    def close(self) -> None:
        self._store._owner_loop()
        self._closed = True

    async def add(self, caller: CallerContext, filename: str, content: bytes) -> str:
        return await self._store._add(caller, filename, content, self)

    async def capture(
        self,
        caller: CallerContext,
        filename: str,
        read_bytes: Callable[[int], Awaitable[bytes]],
    ) -> str:
        return await self._store._capture(caller, filename, read_bytes, self)

    async def capture_named(
        self,
        caller: CallerContext,
        read_named: Callable[[int], Awaitable[tuple[str, bytes]]],
    ) -> str:
        """Trusted bounded receive/parser; reserve BEFORE reading name or bytes.

        The conservative payload budget includes accumulation, immutable return
        copy and one bounded parser feed. It excludes transport-owned ASGI frames
        and Python object metadata. A parser callback is server code, not a URL,
        arbitrary client function or general-purpose unbounded allocator.
        """
        return await self._store._capture_named(
            caller, read_named, self, reservation=3 * self._store._max_file + 65_536
        )

    async def read(self, caller: CallerContext, attachment_id: str) -> FileSnapshot:
        return await self._store._read(caller, attachment_id, self)

    async def revoke(self, caller: CallerContext, attachment_id: str) -> None:
        await self._store._revoke(caller, attachment_id, self)

    def release(self, caller: CallerContext, attachment_id: str) -> None:
        """Trusted local cleanup AFTER the binding's current read authorization.

        No await or byte read: removes only this caller/namespace's store reference.
        Missing IDs are idempotent. Already returned bytes, approvals, leases and
        remote data are unaffected; this is not secure erasure or write revocation.
        """
        self._store._owner_loop()
        self._check(self._store, caller)
        _identifier(attachment_id)
        # Store close already dropped every entry; a settling submit's cleanup
        # remains a harmless no-op, not a fabricated failure after durable writes.
        if self._store._closed:
            return
        self._store._check(caller, self)
        if attachment_id not in self._store._entries:
            return
        self._store._entry(caller, attachment_id, self)
        self._store._drop(attachment_id)


class EphemeralUploadSource:
    def __init__(
        self,
        *,
        authorize: Callable[[CallerContext], Awaitable[None]],
        ttl_s: float = 60,
        max_files: int = 64,
        max_pending_reads: int = 64,
        max_total_bytes: int = 128 * 1024 * 1024,
        max_file_bytes: int = 20 * 1024 * 1024,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if (
            not callable(authorize)
            or not callable(clock)
            or type(ttl_s) not in (int, float)
            or not math.isfinite(ttl_s)
            or not 0 < ttl_s <= 300
            or type(max_pending_reads) is not int
            or not 1 <= max_pending_reads <= 1024
            or type(max_files) is not int
            or not 1 <= max_files <= 1024
            or type(max_total_bytes) is not int
            or not 1 <= max_total_bytes <= 256 * 1024 * 1024
            or type(max_file_bytes) is not int
            or not 1 <= max_file_bytes <= 20 * 1024 * 1024
        ):
            raise UploadSourceError("invalid_upload_limits")
        self._authorize = authorize
        self._clock = clock
        self._ttl = ttl_s
        self._max_files = max_files
        self._max_pending_reads = max_pending_reads
        self._pending_reads = 0
        self._max_bytes = max_total_bytes
        self._max_file = max_file_bytes
        self._entries: dict[str, _Entry] = {}
        self._bytes = 0
        self._pending_files = 0
        self._pending_bytes = 0
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_now: float | None = None

    def bind(
        self,
        *,
        binding_id: str,
        connector_id: str,
        caller: CallerContext,
        authorize: Callable[[CallerContext], Awaitable[None]],
    ) -> ScopedUploadSource:
        """Create a server-only view without allocating another store or quota.

        The factory, not a model/client, supplies identifiers and authorizer.
        Unscoped access cannot read namespaced entries, even for the same caller.
        """
        self._check(caller)
        try:
            namespace = (_identifier(binding_id), _identifier(connector_id))
            if not callable(authorize):
                raise ValueError
        except ValueError:
            raise UploadSourceError("invalid_upload_scope") from None
        return ScopedUploadSource(self, namespace, caller, authorize)

    def _owner_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise UploadSourceError("upload_event_loop_changed")
        self._loop = loop
        return loop

    def _drop(self, key: str) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            entry.timer.cancel()
            self._bytes -= len(entry.snapshot.content)

    def close(self) -> None:
        """Owner-loop only. Pending callers cannot publish after close.

        Suspended authorization callers may still hold their own input references;
        this is not cancellation of external tasks or secure erasure of bytes.
        """
        self._owner_loop()
        self._closed = True
        for key in tuple(self._entries):
            self._drop(key)

    def _check(self, caller: CallerContext, scope: ScopedUploadSource | None = None) -> float:
        self._owner_loop()
        if scope is not None:
            scope._check(self, caller)
        if self._closed:
            raise UploadSourceError("upload_source_closed")
        try:
            now = self._clock()
            if (
                type(now) not in (int, float)
                or not math.isfinite(now)
                or (self._last_now is not None and now < self._last_now)
            ):
                raise ValueError
        except Exception:
            self.close()
            raise UploadSourceError("upload_clock_invalid") from None
        self._last_now = now
        for key, entry in tuple(self._entries.items()):
            if now >= entry.expires_at:
                self._drop(key)
        if (
            not isinstance(caller, CallerContext)
            or type(caller.user_id) is not int
            or caller.user_id <= 0
            or type(caller.agent_owner_user_id) is not int
            or caller.agent_owner_user_id != caller.user_id
            or caller.is_group is not False
            or type(caller.authorization_revision) is not int
            or caller.authorization_revision < 1
        ):
            raise UploadSourceError("upload_access_denied")
        try:
            _identifier(caller.agent_id)
        except ValueError:
            raise UploadSourceError("upload_access_denied") from None
        return now

    async def _fresh(
        self, caller: CallerContext, timeout_s: float, scope: ScopedUploadSource | None
    ) -> None:
        try:
            self._check(caller, scope)
            authorize = self._authorize if scope is None else scope._authorize
            async with asyncio.timeout(timeout_s) as deadline:
                result = await cast("Awaitable[object]", authorize(caller))
            if deadline.expired():
                raise TimeoutError
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise asyncio.CancelledError
            if result is not None:
                raise ValueError
            self._check(caller, scope)
        except TimeoutError:
            raise UploadSourceError("upload_authorization_timeout") from None
        except Exception:
            raise UploadSourceError("upload_access_denied") from None

    async def add(self, caller: CallerContext, filename: str, content: bytes) -> str:
        """Unscoped prototype path; never reads or creates binding-scoped entries."""
        return await self._add(caller, filename, content, None)

    async def _add(
        self, caller: CallerContext, filename: str, content: bytes, scope: ScopedUploadSource | None
    ) -> str:
        """Trusted server-only input; never accepts a model path or external URL."""
        start = self._check(caller, scope)
        if (
            type(content) is not bytes
            or not 0 < len(content) <= self._max_file
            or not isinstance(filename, str)
            or not 0 < len(filename) <= 255
            or filename in (".", "..")
            or any(c in filename for c in "/\\\0\r\n:")
        ):
            raise UploadSourceError("invalid_upload_snapshot")
        if (
            len(self._entries) + self._pending_files >= self._max_files
            or self._bytes + self._pending_bytes + len(content) > self._max_bytes
        ):
            raise UploadSourceError("upload_capacity_exceeded")
        # Reservation covers suspended authorization, not just completed entries.
        self._pending_files += 1
        self._pending_bytes += len(content)
        try:
            await self._fresh(caller, start + self._ttl - self._check(caller, scope), scope)
            return self._publish(caller, filename, content, start, scope)
        finally:
            self._pending_files -= 1
            self._pending_bytes -= len(content)

    def _publish(
        self,
        caller: CallerContext,
        filename: str,
        content: bytes,
        start: float,
        scope: ScopedUploadSource | None,
    ) -> str:
        now = self._check(caller, scope)
        expires = start + self._ttl
        if not math.isfinite(expires) or now >= expires:
            raise UploadSourceError("upload_expired")
        key = "upload_" + secrets.token_urlsafe(24)
        while key in self._entries:
            key = "upload_" + secrets.token_urlsafe(24)
        loop = self._owner_loop()
        timer = loop.call_later(expires - now, self._drop, key)
        self._entries[key] = _Entry(
            caller,
            FileSnapshot(key, filename, content),
            expires,
            timer,
            object(),
            scope._namespace if scope is not None else None,
        )
        self._bytes += len(content)
        return key

    async def capture(
        self,
        caller: CallerContext,
        filename: str,
        read_bytes: Callable[[int], Awaitable[bytes]],
    ) -> str:
        return await self._capture(caller, filename, read_bytes, None)

    async def _capture(
        self,
        caller: CallerContext,
        filename: str,
        read_bytes: Callable[[int], Awaitable[bytes]],
        scope: ScopedUploadSource | None,
    ) -> str:
        """Reserve before reading NEW bytes through a trusted bounded callback.

        Callback must honor max_bytes and cooperate with cancellation. Reservation
        covers chunk storage plus join, not multipart ingress or arbitrary callback
        allocations. It remains held until the callback actually exits.
        """
        self._check(caller, scope)
        if (
            not callable(read_bytes)
            or not isinstance(filename, str)
            or not 0 < len(filename) <= 255
            or filename in (".", "..")
            or any(c in filename for c in "/\\\0\r\n:")
        ):
            raise UploadSourceError("invalid_upload_snapshot")

        async def read_named(maximum: int) -> tuple[str, bytes]:
            return filename, await read_bytes(maximum)

        return await self._capture_named(caller, read_named, scope, reservation=2 * self._max_file)

    async def _capture_named(
        self,
        caller: CallerContext,
        read_named: Callable[[int], Awaitable[tuple[str, bytes]]],
        scope: ScopedUploadSource | None,
        *,
        reservation: int,
    ) -> str:
        start = self._check(caller, scope)
        if not callable(read_named):
            raise UploadSourceError("invalid_upload_snapshot")
        if (
            len(self._entries) + self._pending_files >= self._max_files
            or self._bytes + self._pending_bytes + reservation > self._max_bytes
        ):
            raise UploadSourceError("upload_capacity_exceeded")
        self._pending_files += 1
        self._pending_bytes += reservation
        try:
            async with asyncio.timeout(self._ttl) as deadline:
                await self._fresh(caller, start + self._ttl - self._check(caller, scope), scope)
                self._check(caller, scope)
                result = await read_named(self._max_file)
                if not isinstance(result, tuple) or len(result) != 2:
                    raise UploadSourceError("invalid_upload_snapshot")
                filename, content = result
                if deadline.expired():
                    raise TimeoutError
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    raise asyncio.CancelledError
                self._check(caller, scope)
                if (
                    type(content) is not bytes
                    or not 0 < len(content) <= self._max_file
                    or not isinstance(filename, str)
                    or not 0 < len(filename) <= 255
                    or filename in (".", "..")
                    or any(c in filename for c in "/\\\0\r\n:")
                ):
                    raise UploadSourceError("invalid_upload_snapshot")
                await self._fresh(caller, start + self._ttl - self._check(caller, scope), scope)
                if deadline.expired():
                    raise TimeoutError
                return self._publish(caller, filename, content, start, scope)
        except TimeoutError:
            raise UploadSourceError("upload_capture_timeout") from None
        except UploadSourceError:
            raise
        except Exception:
            raise UploadSourceError("upload_capture_failed") from None
        finally:
            self._pending_files -= 1
            self._pending_bytes -= reservation

    def _entry(self, caller: CallerContext, key: str, scope: ScopedUploadSource | None) -> _Entry:
        self._check(caller, scope)
        try:
            _identifier(key)
        except ValueError:
            raise UploadSourceError("upload_unavailable") from None
        entry = self._entries.get(key)
        if (
            entry is None
            or entry.caller != caller
            or entry.namespace != (scope._namespace if scope is not None else None)
        ):
            raise UploadSourceError("upload_unavailable")
        return entry

    async def read(self, caller: CallerContext, attachment_id: str) -> FileSnapshot:
        return await self._read(caller, attachment_id, None)

    async def _read(
        self, caller: CallerContext, attachment_id: str, scope: ScopedUploadSource | None
    ) -> FileSnapshot:
        # Never retain Entry/FileSnapshot/content across authorization suspension.
        generation = self._entry(caller, attachment_id, scope).generation
        expires_at = self._entry(caller, attachment_id, scope).expires_at
        if self._pending_reads >= self._max_pending_reads:
            raise UploadSourceError("upload_read_capacity_exceeded")
        self._pending_reads += 1
        try:
            await self._fresh(caller, expires_at - self._check(caller, scope), scope)
            entry = self._entry(caller, attachment_id, scope)
            if entry.generation is not generation:
                raise UploadSourceError("upload_unavailable")
            return entry.snapshot
        except BaseException as exc:
            # A stale read must not evict a replacement, even with a reused ID.
            # Cancelling/closing ONE bound view is not revocation of the shared
            # upload. Keep it charged until TTL or an explicit revoke/store close;
            # another view still needs fresh authorization to obtain its bytes.
            view_stopped = scope is not None and (
                isinstance(exc, asyncio.CancelledError) or scope._closed
            )
            current = self._entries.get(attachment_id)
            if not view_stopped and current is not None and current.generation is generation:
                self._drop(attachment_id)
            raise
        finally:
            self._pending_reads -= 1

    async def revoke(self, caller: CallerContext, attachment_id: str) -> None:
        await self._revoke(caller, attachment_id, None)

    async def _revoke(
        self, caller: CallerContext, attachment_id: str, scope: ScopedUploadSource | None
    ) -> None:
        await self._read(caller, attachment_id, scope)
        self._drop(attachment_id)
