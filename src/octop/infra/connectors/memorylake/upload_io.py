"""Unregistered server-owned admission for trusted bounded upload readers.

Every API helper must reuse the same server-owned instance and pass the underlying
UploadFile.file, not its wrapper. Hold exclusive until actual read work finishes,
not merely until its caller cancels. Actual jobs use an owned bounded pool and remain accounted for after early holder
exit. This object does not interrupt I/O, close files or provide cross-worker limits.
Production integration and shutdown ownership remain explicit gates.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Protocol, cast


class _ByteReader(Protocol):
    def read(self, size: int) -> bytes: ...


class UploadReadAdmissionError(ValueError):
    """Body-free reason; never include file identity, name, content or repr."""


class UploadReadAdmission:
    def __init__(self, *, max_active: int = 4, workers: int = 2) -> None:
        if type(max_active) is not int or not 1 <= max_active <= 1024:
            raise UploadReadAdmissionError("invalid_upload_read_limit")
        if type(workers) is not int or not 1 <= workers <= 16:
            raise UploadReadAdmissionError("invalid_upload_read_workers")
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="memorylake-upload")
        self._jobs: dict[int, tuple[object, asyncio.Future[bytes]]] = {}
        self._closing: asyncio.Future[None] | None = None
        self._max_active = max_active
        self._active: dict[int, object] = {}
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._idle = asyncio.Event()
        self._idle.set()

    def _owner_loop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise UploadReadAdmissionError("upload_read_event_loop_required") from None
        if self._loop is not None and self._loop is not loop:
            raise UploadReadAdmissionError("upload_read_event_loop_changed")
        self._loop = loop

    @contextmanager
    def exclusive(self, file: object) -> Iterator[None]:
        """Reserve synchronously before any await; no queued admission waiters."""
        self._owner_loop()
        if self._closed:
            raise UploadReadAdmissionError("upload_read_closed")
        self._prune_jobs()
        key = id(file)
        if (
            key in self._active
            or key in self._jobs
            or len(self._active.keys() | self._jobs.keys()) >= self._max_active
        ):
            raise UploadReadAdmissionError("upload_read_busy")
        # Strong reference prevents id reuse while this reservation is active.
        self._active[key] = file
        self._idle.clear()
        try:
            yield
        finally:
            self._active.pop(key)
            self._update_idle()

    def check_current(self, file: object) -> None:
        """Liveness check only, not caller authentication or a transferable lease."""
        self._owner_loop()
        if self._closed:
            raise UploadReadAdmissionError("upload_read_closed")
        if id(file) not in self._active or self._active[id(file)] is not file:
            raise UploadReadAdmissionError("upload_read_unavailable")

    def _update_idle(self) -> None:
        if not self._active and not self._jobs:
            self._idle.set()
        else:
            self._idle.clear()

    def _prune_jobs(self) -> None:
        for key, (_, future) in tuple(self._jobs.items()):
            if future.done():
                self._jobs.pop(key)
                if not future.cancelled():
                    future.exception()
        self._update_idle()

    def submit_read(self, file: object, size: int) -> asyncio.Future[bytes]:
        """Return a cancellation-isolated view; actual work retains its slot."""
        self.check_current(file)
        if type(size) is not int or size <= 0:
            raise UploadReadAdmissionError("invalid_upload_read_size")
        self._prune_jobs()
        key = id(file)
        if key in self._jobs:
            raise UploadReadAdmissionError("upload_read_busy")

        def read() -> bytes:
            try:
                data = cast("_ByteReader", file).read(size)
                if type(data) is not bytes:
                    raise ValueError
                return data
            except Exception:
                raise UploadReadAdmissionError("upload_read_failed") from None

        try:
            future = asyncio.get_running_loop().run_in_executor(self._pool, read)
        except Exception:
            raise UploadReadAdmissionError("upload_read_failed") from None
        self._jobs[key] = (file, future)
        self._update_idle()
        future.add_done_callback(lambda _: self._prune_jobs())
        view = asyncio.shield(future)

        def observe_view(done: asyncio.Future[bytes]) -> None:
            # A caller may shield this view again, cancel, then abandon it.
            # Observing the actual Future alone does not observe this view.
            if not done.cancelled():
                done.exception()

        view.add_done_callback(observe_view)
        return view

    def close(self) -> None:
        """Stop new entries; holders remain accounted for until context exit."""
        self._owner_loop()
        self._closed = True

    async def drain(self) -> None:
        """Wait for holders after close. Does not cancel holders or close files."""
        self._owner_loop()
        if not self._closed:
            raise UploadReadAdmissionError("upload_read_not_closed")
        self._prune_jobs()
        await self._idle.wait()

    async def aclose(self) -> None:
        """Defer caller cancellation until workers AND actual shutdown finish.

        No hidden Task: server cancel-all must not cancel a background lifetime
        owner and mistake its completion for worker completion. No hard deadline:
        uncooperative file.read can indefinitely delay close. Never call from
        inside a holder whose release depends on this same call returning.
        """
        self.close()
        cancelled = False
        while True:
            try:
                await self.drain()
                break
            except asyncio.CancelledError:
                cancelled = True
        if self._closing is None:
            # Assignment has no await: concurrent closers share exactly one real
            # shutdown Future. Joining worker threads must run off-loop and not
            # in the pool being shut down. Read I/O never uses the default pool.
            self._closing = asyncio.get_running_loop().run_in_executor(None, self._pool.shutdown)
        while not self._closing.done():
            try:
                await asyncio.shield(self._closing)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        try:
            self._closing.result()
        except Exception:
            if cancelled:
                raise asyncio.CancelledError from None
            raise UploadReadAdmissionError("upload_read_shutdown_failed") from None
        if cancelled:
            raise asyncio.CancelledError
