"""Scoped stdlib-only guard for offline MemoryLake tests and Python fake CLIs.

This blocks Python socket egress, not arbitrary native code/OS syscalls. It is a
regression tripwire, not a production sandbox. Never install it in production.
Test processes using it must not concurrently run tests that need real networking.
"""
from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from functools import wraps
from pathlib import Path
from textwrap import indent
from unittest.mock import patch


class NetworkDeniedError(PermissionError):
    pass


def _deny(*args, **kwargs):
    raise NetworkDeniedError("memorylake_test_network_denied")


def _guard_method(original):
    @wraps(original)
    def guarded(sock, *args, **kwargs):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            _deny()
        return original(sock, *args, **kwargs)
    return guarded


@contextmanager
def deny_network() -> Iterator[None]:
    """Deny INET sends/connects and DNS; restore all patched functions on exit."""
    with ExitStack() as stack:
        for method in ("connect", "connect_ex", "send", "sendall", "sendto", "sendmsg", "sendfile"):
            if hasattr(socket.socket, method):
                original = getattr(socket.socket, method)
                stack.enter_context(patch.object(socket.socket, method, _guard_method(original)))
        for name in ("getaddrinfo", "gethostbyname", "gethostbyname_ex", "gethostbyaddr", "getnameinfo", "getfqdn"):
            stack.enter_context(patch.object(socket, name, _deny))
        yield


@contextmanager
def offline_test_guard() -> Iterator[None]:
    """Temporarily remove ambient MemoryLake configuration and deny networking.

    Explicit dummy variables added inside a test remain usable until exit. Only
    MEMORYLAKE_* keys are restored: unrelated environment changes are untouched.
    No inherited values are printed or passed to subprocesses by this helper.
    """
    saved = {key: value for key, value in os.environ.items() if key.startswith("MEMORYLAKE_")}
    for key in saved:
        os.environ.pop(key, None)
    try:
        with deny_network():
            yield
    finally:
        for key in tuple(os.environ):
            if key.startswith("MEMORYLAKE_"):
                os.environ.pop(key, None)
        os.environ.update(saved)


def protected_fake_python(source: str) -> str:
    """Wrap a normal fake script (optional shebang) in the same network guard.

    run_path loads this stdlib-only helper by its local absolute path, independent
    of child HOME/PYTHONPATH. The child's explicit dummy session env is preserved.
    This is for test scripts, not arbitrary Python modules with future imports.
    """
    shebang = ""
    if source.startswith("#!"):
        shebang, source = source.split("\n", 1)
        shebang += "\n"
    helper = str(Path(__file__).resolve())
    return (
        shebang
        + "import runpy as _memorylake_runpy\n"
        + f"with _memorylake_runpy.run_path({helper!r})['deny_network']():\n"
        + indent(source, "    ")
        + "\n"
    )
