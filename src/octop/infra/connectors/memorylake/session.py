"""Private, ephemeral CLI profile bootstrap (offline PoC; POSIX only).

The pinned CLI requires a credential profile even with MEMORYLAKE_API_KEY.
An empty profile key selects its environment fallback without persisting the key.
This is a source-derived contract, not yet verified with a real CLI executable.
Environment secrets are still visible to privileged/same-UID processes: this is
not a sandbox or a substitute for OS-user/container isolation in multi-tenancy.
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory


@dataclass(frozen=True)
class CliSession:
    profile: str
    environment: dict[str, str] = field(repr=False)


@contextmanager
def isolated_cli_session(api_key: str) -> Iterator[CliSession]:
    """Build a fresh child-only environment; never read user CLI configuration.

    Use off the async event loop. The future runner must close the child before
    exiting this context; neither subprocess execution nor retries happen here.
    """
    if os.name != "posix":
        # chmod is not an equivalent Windows ACL boundary. Do not claim it is.
        raise ValueError("secure_session_unsupported")
    if (
        not isinstance(api_key, str)
        or not api_key.strip()
        or "\0" in api_key
        or len(api_key) > 8192
    ):
        raise ValueError("invalid_api_key")
    api_key = api_key.strip()
    with TemporaryDirectory(prefix="octop-memorylake-") as directory:
        root = Path(directory)
        root.chmod(0o700)
        path = root / "credentials.toml"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write('[profiles.octop]\napi_key = ""\nlogin_method = "api_key"\n')
        environment = {
            "MEMORYLAKE_CONFIG_DIR": str(root),
            "MEMORYLAKE_API_KEY": api_key,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
        }
        try:
            yield CliSession(profile="octop", environment=environment)
        finally:
            # Do not retain the key in a session object after child shutdown.
            # Python strings cannot be guaranteed to be erased from memory.
            environment.clear()


async def _complete[T](future: asyncio.Future[T]) -> tuple[T, asyncio.CancelledError | None]:
    """Do not cancel executor work: recover its result even after cancellation."""
    cancelled = None
    while True:
        try:
            return await asyncio.shield(future), cancelled
        except asyncio.CancelledError as error:
            if future.cancelled():
                raise
            if cancelled is None:
                cancelled = error


@asynccontextmanager
async def async_isolated_cli_session(api_key: str) -> AsyncIterator[CliSession]:
    """Create and remove the private profile off-loop, including on cancellation.

    The caller must finish/reap child processes before leaving this scope. On
    cancellation during entry we wait for initialization, then close the acquired
    context without yielding. Repeated cancellation cannot abandon cleanup.
    An existing body exception (including typed cancellation evidence) is kept
    rather than replaced by a later cancellation during cleanup.
    """
    loop = asyncio.get_running_loop()
    context = isolated_cli_session(api_key)
    session, cancelled = await _complete(loop.run_in_executor(None, context.__enter__))
    try:
        if cancelled is not None:
            raise cancelled
        yield session
    finally:
        exc_info = sys.exc_info()
        _, cleanup_cancelled = await _complete(
            loop.run_in_executor(None, context.__exit__, *exc_info)
        )
        if cleanup_cancelled is not None and exc_info[1] is None:
            raise cleanup_cancelled
