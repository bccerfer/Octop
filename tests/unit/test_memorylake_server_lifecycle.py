"""Real OctopServer.stop with disposable SQLite; no boot/plugins/remote services."""

import asyncio
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from octop.api.routers import setup
from octop.config import OctopConfig
from octop.infra.connectors.memorylake.file_runtime import MemoryLakeFileRuntime
from octop.infra.db.pool import SqlitePool
from octop.infra.db.rebind import assert_memorylake_file_runtime_detached, rebind_control_plane
from octop.infra.errors import OctopError
from octop.infra.server import OctopServer
from tests.support.memorylake import offline_test_guard


class MemoryLakeServerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        directory = Path(self.enterContext(TemporaryDirectory()))
        self.events = []
        events = self.events

        class TrackedPool(SqlitePool):
            def close(inner):
                events.append("db_close")
                super().close()

        self.pool = TrackedPool(directory / "db")
        self.addCleanup(self.pool.close)
        self.server = OctopServer(home=directory)
        self.server._started = True
        self.server.services = SimpleNamespace(db=self.pool, config=OctopConfig())

        def closer(name):
            async def close():
                self.events.append(name)

            return AsyncMock(side_effect=close)

        self.server.app_runtime = SimpleNamespace(
            proactive_scheduler=SimpleNamespace(shutdown=closer("proactive")),
            cron_manager=SimpleNamespace(shutdown=closer("cron")),
            gateway=SimpleNamespace(shutdown=closer("gateway")),
            agent_registry=SimpleNamespace(shutdown=closer("agents")),
            user_manager=SimpleNamespace(shutdown_all=closer("users")),
            history_archive=None,
        )
        self.enterContext(
            patch("octop.infra.connectors.gateway.agently_auth.close", closer("auth"))
        )
        self.enterContext(
            patch(
                "octop.infra.backend.browse.dispose_all_browse_sessions",
                side_effect=lambda: self.events.append("browse"),
            )
        )

    def runtime(self):
        def factory(admission):
            async def authorize(caller):
                await admission.submit(lambda: None)

            return authorize

        runtime = MemoryLakeFileRuntime(authorize_factory=factory)
        self.addAsyncCleanup(runtime.aclose)
        return runtime

    async def test_default_none_preserves_existing_stop_order(self):
        self.assertIsNone(self.server.memorylake_file_runtime)
        await self.server.stop()
        self.assertEqual(
            self.events,
            ["auth", "proactive", "cron", "gateway", "agents", "users", "browse", "db_close"],
        )
        self.assertFalse(self.server._started)
        self.assertIsNone(self.server.services)

    async def test_real_db_worker_must_finish_before_stop_finally_closes_pool(self):
        runtime = self.runtime()
        self.server.attach_memorylake_file_runtime(runtime)
        entered, release = threading.Event(), threading.Event()

        def blocked():
            entered.set()
            release.wait(3)
            with self.pool.connect() as conn:
                conn.execute("SELECT 1").fetchone()
            self.events.append("worker_finished")

        async def read():
            with runtime.operation():
                await runtime.database.submit(blocked)

        task = asyncio.create_task(read())
        stopping = None
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            stopping = asyncio.create_task(self.server.stop())
            await asyncio.sleep(0.02)
            self.assertFalse(stopping.done())
            self.assertNotIn("db_close", self.events)
            self.assertTrue(runtime.closed)
            stopping.cancel()
            await asyncio.sleep(0.02)
            self.assertFalse(stopping.done())
        finally:
            release.set()
            await asyncio.gather(task, *([stopping] if stopping else []), return_exceptions=True)
        self.assertTrue(stopping.cancelled())
        self.assertTrue(runtime.drained)
        self.assertLess(self.events.index("worker_finished"), self.events.index("db_close"))
        self.assertFalse(self.server._started)
        self.assertIsNone(self.server.memorylake_file_runtime)

    async def test_shutdown_failure_or_false_drain_keeps_database_and_runtime_for_retry(self):
        runtime = self.runtime()
        self.server.attach_memorylake_file_runtime(runtime)
        for close in (AsyncMock(side_effect=ValueError("sensitive shutdown body")), AsyncMock()):
            with patch.object(runtime, "aclose", close), self.assertRaises(RuntimeError) as caught:
                await self.server.stop()
            self.assertNotIn("sensitive", str(caught.exception))
            self.assertEqual(self.events, [])
            self.assertTrue(self.server._started)
            self.assertIs(self.server.memorylake_file_runtime, runtime)
            with self.pool.connect() as conn:
                self.assertEqual(conn.execute("SELECT 1").fetchone()[0], 1)
        await self.server.stop()
        self.assertTrue(runtime.drained)
        self.assertEqual(self.events[-1], "db_close")

    async def test_attach_is_explicit_singleton_and_rejected_during_stop(self):
        first, second = self.runtime(), self.runtime()
        self.server.attach_memorylake_file_runtime(first)
        with self.assertRaises(ValueError):
            self.server.attach_memorylake_file_runtime(second)
        active, release = asyncio.Event(), asyncio.Event()

        async def operation():
            with first.operation():
                active.set()
                await release.wait()
                await first.database.submit(lambda: self.events.append("outcome_saved"))

        worker = asyncio.create_task(operation())
        await active.wait()
        stopping = asyncio.create_task(self.server.stop())
        await asyncio.sleep(0)
        try:
            self.assertFalse(stopping.done())
            with self.assertRaises(ValueError):
                self.server.attach_memorylake_file_runtime(second)
        finally:
            release.set()
            await worker
            await stopping
        self.assertLess(self.events.index("outcome_saved"), self.events.index("db_close"))
        with self.assertRaises(ValueError):
            self.server.attach_memorylake_file_runtime(second)

    async def test_native_failure_after_drain_retains_existing_teardown_boundary(self):
        runtime = self.runtime()
        self.server.attach_memorylake_file_runtime(runtime)
        self.server.app_runtime.gateway.shutdown.side_effect = RuntimeError(
            "synthetic native failure"
        )
        with self.assertRaisesRegex(RuntimeError, "synthetic native failure"):
            await self.server.stop()
        self.assertTrue(runtime.drained)
        self.assertIn("db_close", self.events)
        self.assertNotIn("agents", self.events)
        self.assertNotIn("users", self.events)
        # Characterize the unchanged native finally, NOT whole-server safety.
        self.assertFalse(self.server._started)

    async def test_late_native_cancellation_after_drain_is_not_claimed_safe(self):
        runtime = self.runtime()
        self.server.attach_memorylake_file_runtime(runtime)
        entered = asyncio.Event()

        async def native_gateway():
            entered.set()
            await asyncio.Event().wait()

        self.server.app_runtime.gateway.shutdown.side_effect = native_gateway
        stopping = asyncio.create_task(self.server.stop())
        await entered.wait()
        self.assertTrue(runtime.drained)
        stopping.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await stopping
        self.assertIn("db_close", self.events)
        self.assertNotIn("agents", self.events)
        self.assertNotIn("users", self.events)

    async def test_drained_owner_blocks_setup_and_rebind_during_native_teardown(self):
        runtime = self.runtime()
        self.server.attach_memorylake_file_runtime(runtime)
        entered, release = asyncio.Event(), asyncio.Event()
        self.server.app_runtime.user_manager.count = lambda: 0
        self.server.paths.config.write_text('{"synthetic":"preserve"}')
        original_config = self.server.paths.config.read_bytes()

        async def native_gateway():
            entered.set()
            await release.wait()

        self.server.app_runtime.gateway.shutdown.side_effect = native_gateway
        stopping = asyncio.create_task(self.server.stop())
        await entered.wait()
        try:
            self.assertTrue(runtime.drained)
            self.assertIsNone(self.server.memorylake_file_runtime)
            body = setup.DatabaseSetupBody(driver="sqlite", sqlite_path="other.db")
            for endpoint in (setup.test_database, setup.apply_database):
                with self.subTest(endpoint=endpoint.__name__):
                    with (
                        patch("octop.infra.db.probe.probe_database") as probe,
                        patch("octop.infra.db.rebind.persist_database_config") as persist,
                        patch("octop.infra.db.rebind.assert_control_plane_database_empty") as empty,
                        patch("octop.infra.db.rebind.rebind_control_plane") as rebind,
                        self.assertRaises(OctopError) as caught,
                    ):
                        await endpoint(body, server=self.server)
                    self.assertEqual(caught.exception.status, 409)
                    probe.assert_not_called()
                    persist.assert_not_called()
                    empty.assert_not_called()
                    rebind.assert_not_called()
            with (
                patch(
                    "octop.infra.db.rebind.load_config",
                    side_effect=AssertionError("replacement load must not run"),
                ) as load,
                patch("octop.infra.db.rebind.open_database") as open_db,
                self.assertRaises(OctopError) as caught,
            ):
                rebind_control_plane(self.server)
            self.assertEqual(caught.exception.status, 409)
            load.assert_not_called()
            open_db.assert_not_called()
            self.assertEqual(self.server.paths.config.read_bytes(), original_config)
        finally:
            release.set()
            await stopping
        # Latch remains after native teardown; only a subsequent start resets it.
        with self.assertRaises(OctopError):
            assert_memorylake_file_runtime_detached(self.server)

    async def test_default_none_native_stop_does_not_latch_memorylake_rebind_guard(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def native_gateway():
            entered.set()
            await release.wait()

        self.server.app_runtime.gateway.shutdown.side_effect = native_gateway
        stopping = asyncio.create_task(self.server.stop())
        await entered.wait()
        try:
            assert_memorylake_file_runtime_detached(self.server)
        finally:
            release.set()
            await stopping
        assert_memorylake_file_runtime_detached(self.server)
