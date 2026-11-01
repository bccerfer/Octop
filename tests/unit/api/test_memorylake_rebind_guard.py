"""Guard real setup/rebind entry points before side effects; disposable DB only."""

import asyncio
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.api.routers import setup
from octop.config import OctopConfig
from octop.infra.connectors.memorylake.file_runtime import MemoryLakeFileRuntime
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.rebind import rebind_control_plane
from octop.infra.db.repos.users import UserRepo
from octop.infra.errors import OctopError
from octop.infra.server import OctopServer


class MemoryLakeRebindGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.directory = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(self.directory / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.users = UserRepo(self.pool)
        self.owner = self.users.create(username="synthetic", role="user")
        self.server = self.make_server()
        self.server.paths.config.write_text(
            '{"fixture":"preserve","database":{"driver":"sqlite","sqlite_path":"db"}}'
        )
        self.original_config = self.server.paths.config.read_bytes()
        self.body = setup.DatabaseSetupBody(driver="sqlite", sqlite_path="other.db")

    def make_server(self):
        server = OctopServer(home=self.directory)
        server._started = True
        server.services = SimpleNamespace(db=self.pool, config=OctopConfig())
        server.app_runtime = SimpleNamespace(
            user_manager=SimpleNamespace(count=self.users.count),
            history_archive=None,
        )
        return server

    async def asyncSetUp(self):
        def factory(admission):
            async def authorize(caller):
                await admission.submit(lambda: None)

            return authorize

        self.runtime = MemoryLakeFileRuntime(authorize_factory=factory)
        self.server.attach_memorylake_file_runtime(self.runtime)
        self.addAsyncCleanup(self.runtime.aclose)
        self.users.delete(self.owner)  # Synthetic user count reaches zero after attachment.
        self.assertEqual(self.users.count(), 0)

    async def test_setup_apply_and_probe_reject_before_external_or_config_effects(self):
        for endpoint in (setup.apply_database, setup.test_database):
            with self.subTest(endpoint=endpoint.__name__):
                with (
                    patch("octop.infra.db.probe.probe_database") as probe,
                    patch("octop.infra.db.rebind.assert_control_plane_database_empty") as empty,
                    patch("octop.infra.db.rebind.persist_database_config") as persist,
                    patch("octop.infra.db.rebind.rebind_control_plane") as rebind,
                    self.assertRaises(OctopError),
                ):
                    await endpoint(self.body, server=self.server)
                probe.assert_not_called()
                empty.assert_not_called()
                persist.assert_not_called()
                rebind.assert_not_called()
                self.assertEqual(self.server.paths.config.read_bytes(), self.original_config)

    async def test_lower_rebind_rejects_before_loading_or_opening_replacement(self):
        with (
            patch(
                "octop.infra.db.rebind.load_config",
                side_effect=AssertionError("must not load replacement"),
            ) as load,
            patch("octop.infra.db.rebind.open_database") as open_db,
            self.assertRaises(OctopError),
        ):
            rebind_control_plane(self.server)
        load.assert_not_called()
        open_db.assert_not_called()
        self.assertIs(self.server.services.db, self.pool)
        self.assertEqual(self.server.paths.config.read_bytes(), self.original_config)

    async def test_user_zero_with_actual_worker_keeps_old_pool_and_configuration(self):
        entered, release = threading.Event(), threading.Event()

        def blocked():
            entered.set()
            release.wait(3)
            with self.pool.connect() as conn:
                return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

        view = self.runtime.database.submit(blocked)
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            with (
                patch(
                    "octop.infra.db.rebind.load_config",
                    side_effect=AssertionError("must not rebind"),
                ),
                self.assertRaises(OctopError),
            ):
                rebind_control_plane(self.server)
            self.assertTrue(self.runtime.database._jobs)
            with self.pool.connect() as conn:
                self.assertEqual(conn.execute("SELECT 1").fetchone()[0], 1)
            self.assertEqual(self.server.paths.config.read_bytes(), self.original_config)
        finally:
            release.set()
            await asyncio.gather(view, return_exceptions=True)

    async def test_closed_but_attached_runtime_still_requires_restart(self):
        await self.runtime.aclose()
        with patch("octop.infra.db.probe.probe_database") as probe, self.assertRaises(OctopError):
            await setup.test_database(self.body, server=self.server)
        probe.assert_not_called()

    async def test_default_none_preserves_native_setup_calls(self):
        plain = self.make_server()
        self.assertIsNone(plain.memorylake_file_runtime)
        with (
            patch("octop.infra.db.probe.probe_database") as probe,
            patch("octop.infra.db.rebind.assert_control_plane_database_empty") as empty,
            patch("octop.infra.db.rebind.persist_database_config") as persist,
            patch("octop.infra.db.rebind.rebind_control_plane") as rebind,
        ):
            result = await setup.apply_database(self.body, server=plain)
        self.assertTrue(result["ok"])
        probe.assert_called_once()
        empty.assert_called_once()
        persist.assert_called_once()
        rebind.assert_called_once_with(plain)
        self.assertEqual(plain.paths.config.read_bytes(), self.original_config)
