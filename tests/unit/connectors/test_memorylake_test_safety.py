"""Verify test-only network denial without ever attempting a network connection."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests.support.memorylake import NetworkDeniedError, offline_test_guard, protected_fake_python


class MemoryLakeTestSafetyTests(unittest.TestCase):
    def test_inet_methods_block_before_the_underlying_operation(self):
        for family in (socket.AF_INET, socket.AF_INET6):
            for method in ("connect", "connect_ex", "send", "sendall", "sendto"):
                with self.subTest(family=family, method=method):
                    with patch.object(socket.socket, method) as underlying:
                        with offline_test_guard():
                            with self.assertRaises(NetworkDeniedError):
                                getattr(socket.socket, method)(SimpleNamespace(family=family), b"dummy")
                        underlying.assert_not_called()

    def test_dns_is_denied_before_resolution(self):
        with patch("socket.getaddrinfo") as underlying:
            with offline_test_guard(), self.assertRaises(NetworkDeniedError):
                socket.getaddrinfo("never-resolve.invalid", 443)
            underlying.assert_not_called()

    @unittest.skipUnless(hasattr(socket, "AF_UNIX"), "Unix socketpair needed")
    def test_unix_socketpair_still_works(self):
        with offline_test_guard():
            left, right = socket.socketpair(socket.AF_UNIX)
            with left, right:
                left.sendall(b"local")
                self.assertEqual(right.recv(5), b"local")

    def test_environment_and_socket_functions_restore_after_exception(self):
        original_connect = socket.socket.connect
        original_lookup = socket.getaddrinfo
        with patch.dict(os.environ, {"MEMORYLAKE_TEST_SENTINEL": "dummy", "UNRELATED_SENTINEL": "keep"}):
            before = {key: value for key, value in os.environ.items() if key.startswith("MEMORYLAKE_")}
            with self.assertRaisesRegex(RuntimeError, "intentional"):
                with offline_test_guard():
                    self.assertFalse(any(key.startswith("MEMORYLAKE_") for key in os.environ))
                    self.assertEqual(os.environ["UNRELATED_SENTINEL"], "keep")
                    os.environ["MEMORYLAKE_CREATED_DURING_TEST"] = "dummy"
                    raise RuntimeError("intentional")
            self.assertEqual({key: value for key, value in os.environ.items() if key.startswith("MEMORYLAKE_")}, before)
        self.assertIs(socket.socket.connect, original_connect)
        self.assertIs(socket.getaddrinfo, original_lookup)

    def test_generated_fake_python_installs_guard_and_keeps_dummy_session_env(self):
        # An unbound method with a non-socket receiver cannot contact any network,
        # even if the guard is broken. A missing guard raises TypeError, not the
        # expected NetworkDeniedError, so this cannot produce a false pass.
        body = '''import socket, os
from types import SimpleNamespace
try:
    socket.socket.connect(SimpleNamespace(family=socket.AF_INET), ("never-used.invalid", 443))
except PermissionError as error:
    assert str(error) == "memorylake_test_network_denied"
else:
    raise AssertionError("network guard absent")
assert os.environ["MEMORYLAKE_API_KEY"] == "dummy-only"
print("guarded")
'''
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fake.py"
            path.write_text(protected_fake_python(body))
            with offline_test_guard():
                result = subprocess.run([sys.executable, str(path)], env={"MEMORYLAKE_API_KEY": "dummy-only"}, capture_output=True, timeout=3)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, b"guarded\n")
