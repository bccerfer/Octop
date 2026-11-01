"""Internal POSIX runner for trusted MemoryLake command plans, not an agent tool.

The caller must supply a pinned executable, validated arguments, a private cwd,
and an isolated environment (see session.py). This is not an OS sandbox: a
hostile executable or descendant that escapes its process group is unsupported.
Timeout covers execution, not time spent waiting for the instance's semaphore.
"""

from __future__ import annotations

import asyncio
import math
import os
import signal
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path


class MemoryLakeRunnerError(RuntimeError):
    """Safe reason text plus optional sensitive, bounded reconciliation evidence.

    ``str``, ``repr`` and ``args`` contain only the reason. The partial result is
    internal-only: a timeout/overflow never proves that a remote write failed.
    """

    def __init__(self, reason: str, *, partial_result: ProcessResult | None = None) -> None:
        super().__init__(reason)
        self.partial_result = partial_result


class MemoryLakeCancelledError(asyncio.CancelledError):
    """Cancellation with bounded reconciliation evidence, never write success.

    Consumers that need durable unknown-write handling must catch this inside
    their task boundary, persist the evidence, then re-raise cancellation.
    """

    def __init__(self, *, partial_result: ProcessResult | None = None) -> None:
        super().__init__("cancelled")
        self.partial_result = partial_result


@dataclass(frozen=True)
class ProcessResult:
    returncode: int
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)


async def _drain(
    stream: asyncio.StreamReader, index: int, capture: Callable[[bytes, int], bool]
) -> None:
    while chunk := await stream.read(8192):
        # Keep available evidence within the shared cap, then discard excess.
        # Cleanup must not replace the original cancellation/timeout/overflow.
        capture(chunk, index)


def _kill_group(process: asyncio.subprocess.Process) -> None:
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)


async def _cleanup(
    process: asyncio.subprocess.Process,
    readers: Sequence[asyncio.Task[None]],
    capture: Callable[[bytes, int], bool],
) -> None:
    # Also kill when the leader has exited: descendants may still hold pipes.
    _kill_group(process)
    for reader in readers:
        reader.cancel()
    await asyncio.gather(*readers, return_exceptions=True)
    assert process.stdout is not None and process.stderr is not None
    # Draining avoids subprocess.wait deadlock on paused full pipe transports.
    await asyncio.gather(
        _drain(process.stdout, 0, capture),
        _drain(process.stderr, 1, capture),
        process.wait(),
    )


async def _uncancelled_cleanup(
    process: asyncio.subprocess.Process,
    readers: Sequence[asyncio.Task[None]],
    capture: Callable[[bytes, int], bool],
) -> None:
    cleanup = asyncio.create_task(_cleanup(process, readers, capture))
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            # A second cancellation must not interrupt process reaping.
            continue
    cleanup.result()


class MemoryLakeRunner:
    """One bounded execution pool; never accepts a caller-selected executable."""

    def __init__(
        self,
        executable: Path,
        *,
        max_output_bytes: int = 1024 * 1024,
        timeout_s: float = 30.0,
        concurrency: int = 1,
    ) -> None:
        if os.name != "posix":
            raise MemoryLakeRunnerError("unsupported_platform")
        if not executable.is_absolute():
            raise MemoryLakeRunnerError("invalid_executable")
        if (
            type(max_output_bytes) is not int
            or max_output_bytes < 1
            or type(concurrency) is not int
            or concurrency < 1
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise MemoryLakeRunnerError("invalid_limits")
        self._executable = str(executable)
        self._max_output_bytes = max_output_bytes
        self._timeout_s = timeout_s
        self._slots = asyncio.Semaphore(concurrency)

    async def run(
        self, arguments: Sequence[str], *, env: Mapping[str, str], cwd: Path
    ) -> ProcessResult:
        """Return bounded bytes even for nonzero exit; failures are not retried.

        Raw result bytes are sensitive, internal-only data: consumers must parse
        and sanitize them before logging or exposing them to users or models.
        """
        if not cwd.is_absolute():
            raise MemoryLakeRunnerError("invalid_cwd")
        async with self._slots:
            return await self._execute(arguments, env, cwd)

    async def _execute(
        self, arguments: Sequence[str], env: Mapping[str, str], cwd: Path
    ) -> ProcessResult:
        process: asyncio.subprocess.Process | None = None
        readers: list[asyncio.Task[None]] = []
        output = (bytearray(), bytearray())
        total = 0

        def capture(chunk: bytes, index: int) -> bool:
            nonlocal total
            remaining = self._max_output_bytes - total
            kept = chunk[:remaining]
            output[index].extend(kept)
            total += len(kept)
            return len(chunk) > remaining

        async def read(stream: asyncio.StreamReader, index: int) -> None:
            while chunk := await stream.read(8192):
                if capture(chunk, index):
                    raise MemoryLakeRunnerError("output_limit")

        # Shield creation so cancellation cannot lose the newly launched process.
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(
                self._executable,
                *arguments,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=dict(env),
                cwd=str(cwd),
                start_new_session=True,
                limit=8192,
            )
        )
        try:
            async with asyncio.timeout(self._timeout_s):
                process = await asyncio.shield(spawn)
                assert process.stdout is not None and process.stderr is not None
                readers = [
                    asyncio.create_task(read(process.stdout, 0)),
                    asyncio.create_task(read(process.stderr, 1)),
                ]
                await asyncio.gather(*readers, process.wait())
        except BaseException as error:
            # Spawn can finish while cancellation is delivered. Recover its handle
            # before propagating any error, without leaking argv in OS errors.
            while not spawn.done():
                try:
                    await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    continue
                except (OSError, ValueError):
                    break
            if not spawn.cancelled() and spawn.exception() is None:
                process = spawn.result()
                await _uncancelled_cleanup(process, readers, capture)
            partial = None
            if process is not None and process.returncode is not None:
                partial = ProcessResult(process.returncode, bytes(output[0]), bytes(output[1]))
            if isinstance(error, asyncio.CancelledError):
                raise MemoryLakeCancelledError(partial_result=partial) from None
            if isinstance(error, TimeoutError):
                raise MemoryLakeRunnerError("timeout", partial_result=partial) from None
            if isinstance(error, MemoryLakeRunnerError):
                error.partial_result = partial
            if isinstance(error, (OSError, ValueError)):
                raise MemoryLakeRunnerError("spawn_failed") from None
            raise
        assert process is not None and process.returncode is not None
        _kill_group(process)
        return ProcessResult(process.returncode, bytes(output[0]), bytes(output[1]))
