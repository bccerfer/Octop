"""Private profile bootstrap PoC with dummy keys only; never starts the CLI."""

from __future__ import annotations

import asyncio
import os
import stat
import threading
import tomllib
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from octop.infra.connectors.memorylake.session import isolated_cli_session
from tests.support.memorylake import offline_test_guard


@unittest.skipUnless(os.name == "posix", "secure Windows ACL provisioning is not implemented")
class MemoryLakeSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())

    def test_profile_has_empty_key_and_explicit_login_method(self) -> None:
        with isolated_cli_session("dummy-key-a") as session:
            root = Path(session.environment["MEMORYLAKE_CONFIG_DIR"])
            data = tomllib.loads((root / "credentials.toml").read_text())
            self.assertEqual(data, {"profiles": {"octop": {"api_key": "", "login_method": "api_key"}}})
            self.assertEqual(session.profile, "octop")
            self.assertEqual(session.environment["MEMORYLAKE_API_KEY"], "dummy-key-a")
            self.assertEqual(list(root.iterdir()), [root / "credentials.toml"])

    def test_inherited_profile_key_endpoint_workspace_proxy_and_debug_are_dropped(self) -> None:
        inherited = {
            "MEMORYLAKE_API_KEY": "unrelated-user-key",
            "MEMORYLAKE_CONFIG_DIR": "/unrelated-user",
            "MEMORYLAKE_BASE_URL": "https://untrusted.invalid",
            "MEMORYLAKE_WORKSPACE": "other-user-workspace",
            "HTTP_PROXY": "https://untrusted.invalid",
            "RUST_LOG": "trace",
            "HOME": "/unrelated-user",
        }
        with patch.dict(os.environ, inherited, clear=True):
            before = dict(os.environ)
            with isolated_cli_session("dummy-key-a") as session:
                self.assertEqual(set(session.environment), {"MEMORYLAKE_CONFIG_DIR", "MEMORYLAKE_API_KEY", "LANG", "LC_ALL"})
                self.assertNotEqual(session.environment["MEMORYLAKE_CONFIG_DIR"], inherited["MEMORYLAKE_CONFIG_DIR"])
                self.assertNotIn("unrelated-user-key", session.environment.values())
                self.assertEqual(dict(os.environ), before)
            self.assertEqual(dict(os.environ), before)

    def test_concurrent_connections_have_distinct_profile_directories(self) -> None:
        with isolated_cli_session("dummy-key-a") as first, isolated_cli_session("dummy-key-b") as second:
            self.assertNotEqual(first.environment["MEMORYLAKE_CONFIG_DIR"], second.environment["MEMORYLAKE_CONFIG_DIR"])
            self.assertEqual(first.environment["MEMORYLAKE_API_KEY"], "dummy-key-a")
            self.assertEqual(second.environment["MEMORYLAKE_API_KEY"], "dummy-key-b")

    def test_key_is_trimmed_like_the_cli_before_environment_injection(self) -> None:
        with isolated_cli_session("  dummy-key-a \n") as session:
            self.assertEqual(session.environment["MEMORYLAKE_API_KEY"], "dummy-key-a")

    def test_private_permissions_and_cleanup(self) -> None:
        with isolated_cli_session("dummy-key-a") as session:
            root = Path(session.environment["MEMORYLAKE_CONFIG_DIR"])
            credential_file = root / "credentials.toml"
            self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(credential_file.stat().st_mode), 0o600)
            self.assertEqual(root.stat().st_uid, os.getuid())
            self.assertFalse(credential_file.is_symlink())
        self.assertFalse(root.exists())
        self.assertEqual(session.environment, {})

    def test_exception_also_cleans_up(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "fake failure"), isolated_cli_session("dummy-key-a") as session:
            root = Path(session.environment["MEMORYLAKE_CONFIG_DIR"])
            raise RuntimeError("fake failure")
        self.assertFalse(root.exists())
        self.assertEqual(session.environment, {})

    def test_repr_does_not_reveal_key_or_private_directory(self) -> None:
        with isolated_cli_session("dummy-key-a") as session:
            self.assertNotIn("dummy-key-a", repr(session))
            self.assertNotIn(session.environment["MEMORYLAKE_CONFIG_DIR"], repr(session))

    def test_blank_invalid_and_oversized_key_fail_closed(self) -> None:
        for value in ("", "  ", "dummy\0key", "x" * 8193, None):
            with self.subTest(value=type(value).__name__), self.assertRaises(ValueError), isolated_cli_session(value):
                self.fail("must reject invalid key")


class MemoryLakeSessionPlatformTests(unittest.TestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())

    def test_unimplemented_non_posix_acl_is_not_silently_insecure(self) -> None:
        with patch("octop.infra.connectors.memorylake.session.os.name", "nt"), self.assertRaisesRegex(ValueError, "secure_session_unsupported"), isolated_cli_session("dummy-key"):
            self.fail("must reject unsupported platform")


@unittest.skipUnless(os.name == "posix", "secure Windows ACL provisioning is not implemented")
class MemoryLakeAsyncSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.enterContext(offline_test_guard())

    async def test_normal_entry_exit_are_off_loop_and_clear_environment(self) -> None:
        from octop.infra.connectors.memorylake.session import async_isolated_cli_session

        threads = []
        original = isolated_cli_session

        @contextmanager
        def tracked(key):
            threads.append(threading.get_ident())
            with original(key) as session:
                yield session
            threads.append(threading.get_ident())

        with patch("octop.infra.connectors.memorylake.session.isolated_cli_session", tracked):
            async with async_isolated_cli_session("dummy-key") as session:
                root = Path(session.environment["MEMORYLAKE_CONFIG_DIR"])
                self.assertTrue(root.exists())
        self.assertFalse(root.exists())
        self.assertEqual(session.environment, {})
        self.assertEqual(len(threads), 2)
        self.assertNotIn(threading.get_ident(), threads)

    async def test_body_exception_is_preserved_and_cleans_session(self) -> None:
        from octop.infra.connectors.memorylake.session import async_isolated_cli_session

        failure = RuntimeError("body failed")
        with self.assertRaises(RuntimeError) as raised:
            async with async_isolated_cli_session("dummy-key") as session:
                root = Path(session.environment["MEMORYLAKE_CONFIG_DIR"])
                raise failure
        self.assertIs(raised.exception, failure)
        self.assertFalse(root.exists())
        self.assertEqual(session.environment, {})

    async def test_cancel_during_enter_waits_and_cleans_allocated_context(self) -> None:
        from octop.infra.connectors.memorylake.session import async_isolated_cli_session

        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        release = threading.Event()
        retained = []

        @contextmanager
        def delayed(key):
            with isolated_cli_session(key) as session:
                retained.append((session, Path(session.environment["MEMORYLAKE_CONFIG_DIR"])))
                loop.call_soon_threadsafe(started.set)
                release.wait(2)
                yield session

        async def body():
            async with async_isolated_cli_session("dummy-key"):
                self.fail("cancelled entry must not yield")

        with patch("octop.infra.connectors.memorylake.session.isolated_cli_session", delayed):
            task = asyncio.create_task(body())
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(retained[0][1].exists())
        self.assertEqual(retained[0][0].environment, {})

    async def test_repeated_cancel_during_exit_still_finishes_cleanup(self) -> None:
        from octop.infra.connectors.memorylake.session import async_isolated_cli_session

        loop = asyncio.get_running_loop()
        exiting = asyncio.Event()
        release = threading.Event()
        retained = []

        @contextmanager
        def delayed(key):
            with isolated_cli_session(key) as session:
                retained.append((session, Path(session.environment["MEMORYLAKE_CONFIG_DIR"])))
                try:
                    yield session
                finally:
                    loop.call_soon_threadsafe(exiting.set)
                    release.wait(2)

        async def body():
            async with async_isolated_cli_session("dummy-key"):
                pass

        with patch("octop.infra.connectors.memorylake.session.isolated_cli_session", delayed):
            task = asyncio.create_task(body())
            await asyncio.wait_for(exiting.wait(), 2)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(retained[0][1].exists())
        self.assertEqual(retained[0][0].environment, {})

    async def test_body_cancellation_evidence_survives_cancel_during_cleanup(self) -> None:
        from octop.infra.connectors.memorylake.runner import MemoryLakeCancelledError, ProcessResult
        from octop.infra.connectors.memorylake.session import async_isolated_cli_session

        loop = asyncio.get_running_loop()
        exiting = asyncio.Event()
        release = threading.Event()
        failure = MemoryLakeCancelledError(partial_result=ProcessResult(-9, b"accepted", b""))
        retained = []

        @contextmanager
        def delayed(key):
            with isolated_cli_session(key) as session:
                retained.append((session, Path(session.environment["MEMORYLAKE_CONFIG_DIR"])))
                try:
                    yield session
                finally:
                    loop.call_soon_threadsafe(exiting.set)
                    release.wait(2)

        async def body():
            async with async_isolated_cli_session("dummy-key"):
                raise failure

        with patch("octop.infra.connectors.memorylake.session.isolated_cli_session", delayed):
            task = asyncio.create_task(body())
            await asyncio.wait_for(exiting.wait(), 2)
            task.cancel()
            await asyncio.sleep(0)
            release.set()
            with self.assertRaises(MemoryLakeCancelledError) as raised:
                await task
        self.assertIs(raised.exception, failure)
        self.assertFalse(retained[0][1].exists())
        self.assertEqual(retained[0][0].environment, {})

    async def test_unsupported_platform_remains_closed(self) -> None:
        from octop.infra.connectors.memorylake.session import async_isolated_cli_session

        with patch("octop.infra.connectors.memorylake.session.os.name", "nt"):
            with self.assertRaisesRegex(ValueError, "secure_session_unsupported"):
                async with async_isolated_cli_session("dummy-key"):
                    self.fail("must reject unsupported platform")


if __name__ == "__main__":
    unittest.main()
