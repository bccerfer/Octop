"""Bounded read admission shared by server-created per-invocation services.

One object belongs to one server event loop, not to a caller or tool instance.
It holds no credentials or caller tools. Limits are process-local, not a
cross-worker deployment quota. Waiting is bounded in count, not wall time;
callers may cancel or apply an invocation deadline. No FIFO guarantee is made.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from octop.infra.connectors.memorylake.contract import _identifier


class ReadAdmissionError(ValueError):
    """Safe reason code, never a connection identifier or upstream text."""


class ReadAdmission:
    def __init__(self, *, max_active: int = 8, max_queued: int = 32) -> None:
        if type(max_active) is not int or not 1 <= max_active <= 128:
            raise ValueError("invalid_read_admission_limits")
        if type(max_queued) is not int or not 0 <= max_queued <= 1024:
            raise ValueError("invalid_read_admission_limits")
        self._max_active = max_active
        self._max_queued = max_queued
        self._active: set[str] = set()
        self._queued = 0
        self._changed = asyncio.Event()

    def _available(self, connector_id: str) -> bool:
        return connector_id not in self._active and len(self._active) < self._max_active

    @asynccontextmanager
    async def acquire(self, connector_id: str) -> AsyncIterator[None]:
        _identifier(connector_id)
        # All accounting is synchronous on the owning event loop. In particular,
        # cancellation cleanup never awaits another lock and cannot leak a slot.
        if not self._available(connector_id):
            if self._queued >= self._max_queued:
                raise ReadAdmissionError("read_admission_full")
            self._queued += 1
            try:
                while not self._available(connector_id):
                    await self._changed.wait()
            finally:
                self._queued -= 1
        self._active.add(connector_id)
        try:
            yield
        finally:
            self._active.remove(connector_id)
            # Existing waiters retain the old event; new waiters see the next
            # generation. No await occurs between checking capacity and waiting.
            self._changed.set()
            self._changed = asyncio.Event()
