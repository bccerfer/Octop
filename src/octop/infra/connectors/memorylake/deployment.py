"""Optional standard-install CLI artifact inspection, never installation/execution.

A trusted operator supplies one absolute file path and a version-labelled SHA256
pin. This synchronous disk operation must run only in a server-owned bounded job
by the optional application, never on an HTTP event loop. Disabled startup
never invokes it. A matching hash proves only inspected content, not
release provenance, the binary's reported version, interface compatibility, or
immutability between inspection and a later exec. Require a trusted read-only
artifact location; this is not isolation from a same-UID/privileged local actor.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import stat
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from octop.config import MemoryLakeCliConfig

_MAX_ARTIFACT = 256 * 1024 * 1024
_CHUNK = 256 * 1024
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,63}\Z")
_SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")


@dataclass(frozen=True)
class CliArtifact:
    executable: Path = field(repr=False)
    expected_version: str
    sha256: str
    size: int


@dataclass(frozen=True)
class CliArtifactInspection:
    status: Literal["disabled", "unavailable", "content_verified"]
    artifact: CliArtifact | None = field(default=None, repr=False)
    # Deliberately never set by hash inspection; no binary has been run.
    version_probed: bool = False


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def inspect_cli_artifact(config: MemoryLakeCliConfig) -> CliArtifactInspection:
    """Bounded hash-only preflight, fail closed locally without breaking Octop.

    No download, shell, PATH search, login, credential read or version command.
    Disabled/missing/unusable CLI does not raise a file error into native startup.
    File I/O may block on the filesystem: no hard wall-clock deadline is promised.
    """
    unavailable = CliArtifactInspection("unavailable")
    if not isinstance(config, MemoryLakeCliConfig):
        return unavailable
    if config.enabled is False:
        return CliArtifactInspection("disabled")
    if (
        config.enabled is not True
        or os.name != "posix"
        or not isinstance(config.executable, str)
        or not 1 <= len(config.executable) <= 4096
        or any(ord(c) < 32 or ord(c) == 127 for c in config.executable)
        or not isinstance(config.expected_version, str)
        or _VERSION.fullmatch(config.expected_version) is None
        or not isinstance(config.sha256, str)
        or _SHA256.fullmatch(config.sha256) is None
    ):
        return unavailable
    path = Path(config.executable)
    if not path.is_absolute():
        return unavailable
    descriptor = None
    try:
        # O_NONBLOCK avoids hanging on a FIFO before fstat can reject it. Refuse
        # symlink components as a conservative deployment rule, not a complete
        # hostile-filesystem proof. Recheck the final path/inode after hashing.
        if path.resolve(strict=True) != path:
            return unavailable
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or not 0 < before.st_size <= _MAX_ARTIFACT
            or not before.st_mode & 0o111
            or before.st_mode & (0o022 | stat.S_ISUID | stat.S_ISGID)
        ):
            return unavailable
        digest = hashlib.sha256()
        size = 0
        while True:
            chunk = os.read(descriptor, min(_CHUNK, before.st_size - size + 1))
            if not chunk:
                break
            size += len(chunk)
            if size > before.st_size:
                return unavailable
            digest.update(chunk)
        if (
            size != before.st_size
            or _identity(before) != _identity(os.fstat(descriptor))
            or _identity(before) != _identity(os.stat(path, follow_symlinks=False))
            or not hmac.compare_digest(digest.hexdigest(), config.sha256.lower())
        ):
            return unavailable
        return CliArtifactInspection(
            "content_verified",
            CliArtifact(path, config.expected_version, digest.hexdigest(), size),
        )
    except (OSError, ValueError, RuntimeError):
        return unavailable
    finally:
        if descriptor is not None:
            # No raw file error is promoted into native startup.
            with suppress(OSError):
                os.close(descriptor)
