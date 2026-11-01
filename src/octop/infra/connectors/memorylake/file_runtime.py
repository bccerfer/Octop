"""Server-owned resources for the opt-in NEW-upload flow, not an HTTP adapter.

One owner must be reused for all of its file operations. The authorization factory
receives that owner's shared database admission; it must bind trusted identity and
current repositories, never caller-supplied endpoints/credentials. The optional
application composes this owner; routing and consent remain outside this module.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission
from octop.infra.connectors.memorylake.upload_io import UploadReadAdmission
from octop.infra.connectors.memorylake.upload_source import EphemeralUploadSource


class FileRuntimeError(ValueError):
    """Safe reason code only, no file, identity or repository details."""


@dataclass(frozen=True)
class FileRuntimeLimits:
    ttl_s: float = 60
    max_file_bytes: int = 20 * 1024 * 1024
    # Shared process-local payload/reservation budget, NOT per-user fairness.
    max_total_bytes: int = 128 * 1024 * 1024
    max_files: int = 64
    max_pending_reads: int = 16
    max_operations: int = 4
    max_db_jobs: int = 16
    max_io_active: int = 4
    io_workers: int = 2

    def __post_init__(self) -> None:
        for value, upper in (
            (self.max_file_bytes, 20 * 1024 * 1024),
            (self.max_total_bytes, 256 * 1024 * 1024),
            (self.max_files, 1024),
            (self.max_pending_reads, 1024),
            (self.max_operations, 64),
            (self.max_db_jobs, 1024),
            (self.max_io_active, 1024),
            (self.io_workers, 16),
        ):
            if type(value) is not int or not 1 <= value <= upper:
                raise FileRuntimeError("invalid_file_runtime_limits")
        if (
            type(self.ttl_s) not in (int, float)
            or not math.isfinite(self.ttl_s)
            or not 0 < self.ttl_s <= 300
            or self.max_total_bytes < 2 * self.max_file_bytes
        ):
            raise FileRuntimeError("invalid_file_runtime_limits")


AuthorizeFactory = Callable[[DatabaseJobAdmission], Callable[[CallerContext], Awaitable[None]]]


class MemoryLakeFileRuntime:
    def __init__(
        self, *, authorize_factory: AuthorizeFactory, limits: FileRuntimeLimits | None = None
    ) -> None:
        self._limits = limits if limits is not None else FileRuntimeLimits()
        self._database = DatabaseJobAdmission(max_jobs=self._limits.max_db_jobs)
        authorize = authorize_factory(self._database)
        if not callable(authorize):
            raise FileRuntimeError("invalid_file_runtime_authorizer")
        self._source = EphemeralUploadSource(
            authorize=authorize,
            ttl_s=self._limits.ttl_s,
            max_files=self._limits.max_files,
            max_total_bytes=self._limits.max_total_bytes,
            max_file_bytes=self._limits.max_file_bytes,
            max_pending_reads=self._limits.max_pending_reads,
        )
        self._io = UploadReadAdmission(
            max_active=self._limits.max_io_active, workers=self._limits.io_workers
        )
        self._holders: set[asyncio.Task[Any]] = set()
        self._idle = asyncio.Event()
        self._idle.set()
        self._drain_lock = asyncio.Lock()
        self._closed = False
        self._drained = False
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def source(self) -> EphemeralUploadSource:
        return self._source

    @property
    def database(self) -> DatabaseJobAdmission:
        return self._database

    @property
    def io(self) -> UploadReadAdmission:
        return self._io

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def drained(self) -> bool:
        return self._drained

    def _owner_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise FileRuntimeError("file_runtime_event_loop_changed")
        self._loop = loop

    @contextmanager
    def operation(self) -> Iterator[None]:
        """Bound one ingress/approval/submission operation through its final save.

        All production callers must use this boundary; direct access to resource
        properties is trusted wiring, not a public bypass or bearer capability.
        """
        self._owner_loop()
        task = asyncio.current_task()
        if (
            self._closed
            or task is None
            or task in self._holders
            or len(self._holders) >= self._limits.max_operations
        ):
            raise FileRuntimeError("file_runtime_unavailable")
        self._holders.add(task)
        self._idle.clear()
        try:
            yield
        finally:
            self._holders.remove(task)
            if not self._holders:
                self._idle.set()

    def close(self) -> None:
        """Stop ingress/file capture, not approval revocation or DB shutdown."""
        self._owner_loop()
        self._closed = True
        self._source.close()
        self._io.close()
        # Do not close DB admission yet: already-authorized operations may need
        # to save a remote receipt/unknown state before their operation exits.

    async def drain(self) -> None:
        self._owner_loop()
        if not self._closed or asyncio.current_task() in self._holders:
            raise FileRuntimeError("file_runtime_not_drainable")
        async with self._drain_lock:
            if self._drained:
                return
            await self._idle.wait()
            await self._io.drain()
            self._database.close()
            await self._database.drain()
            await self._io.aclose()
            self._drained = True

    async def aclose(self) -> None:
        """Settle owned work before propagating cancellation; no hard deadline.

        The server must call this BEFORE closing files/DB pools or its event loop.
        Blocking I/O/DB may delay it indefinitely; no worker is force-stopped.
        """
        self.close()
        if asyncio.current_task() in self._holders:
            raise FileRuntimeError("file_runtime_not_drainable")
        cancelled = False
        while not self._drained:
            try:
                await self.drain()
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError("file_runtime_closed_after_cancellation")
