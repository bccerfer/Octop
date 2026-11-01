"""Optional server application with inert hash fixtures and FAKE version runner."""

import asyncio
import hashlib
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.support.memorylake import offline_test_guard

from octop.config import MemoryLakeCliConfig, OctopConfig
from octop.infra.connectors.memorylake.application import MemoryLakeApplication
from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.deployment import inspect_cli_artifact
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.server import OctopServer
from octop.infra.utils.paths import PathLayout


class ApplicationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.binary = self.root / "synthetic-inert-never-execute"
        self.binary.write_bytes(b"INERT SYNTHETIC FILE")
        self.binary.chmod(0o755)
        self.config = MemoryLakeCliConfig(
            True,
            str(self.binary),
            "v-synthetic",
            hashlib.sha256(self.binary.read_bytes()).hexdigest(),
        )
        self.pool = SqlitePool(self.root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.server = OctopServer(home=self.root)
        self.server._started = True
        self.server.config = OctopConfig(memorylake_cli=self.config)
        self.server.services = SharedServices(
            PathLayout(self.root), self.server.config, RepoBundle.from_pool(self.pool)
        )
        self.server.app_runtime = SimpleNamespace(
            proactive_scheduler=SimpleNamespace(shutdown=AsyncMock()),
            cron_manager=SimpleNamespace(shutdown=AsyncMock()),
            gateway=SimpleNamespace(shutdown=AsyncMock()),
            agent_registry=SimpleNamespace(shutdown=AsyncMock()),
            user_manager=SimpleNamespace(shutdown_all=AsyncMock()),
            history_archive=None,
        )
        self.enterContext(patch("octop.infra.connectors.gateway.agently_auth.close", AsyncMock()))
        self.enterContext(patch("octop.infra.backend.browse.dispose_all_browse_sessions"))
        self.probes, self.probe_roots = [], []
        self.output = b"memorylake v-synthetic\n"

        class FakeRunner:
            def __init__(inner, executable, **kwargs):
                self.assertEqual(executable, self.binary)
                self.assertLessEqual(kwargs["max_output_bytes"], 4096)
                self.assertLessEqual(kwargs["timeout_s"], 5)

            async def run(inner, args, *, env, cwd):
                self.assertEqual(tuple(args), ("--version",))
                self.assertEqual(set(env), {"HOME", "MEMORYLAKE_CONFIG_DIR", "LANG", "LC_ALL"})
                self.assertEqual(env["HOME"], str(cwd))
                self.assertEqual(env["MEMORYLAKE_CONFIG_DIR"], str(cwd))
                self.assertEqual(list(cwd.iterdir()), [])
                self.probes.append(tuple(args))
                self.probe_roots.append(cwd)
                return ProcessResult(0, self.output, b"")

        self.enterContext(
            patch("octop.infra.connectors.memorylake.application.MemoryLakeRunner", FakeRunner)
        )
        self.enterContext(
            patch(
                "asyncio.create_subprocess_exec",
                side_effect=AssertionError("NEVER execute artifact"),
            )
        )
        self.addAsyncCleanup(self.server.stop)

    async def test_disabled_server_hook_creates_no_resources_or_probe(self):
        self.server.services = replace(self.server.services, config=OctopConfig())
        with patch(
            "octop.infra.connectors.memorylake.application.inspect_cli_artifact",
            side_effect=AssertionError("no disabled inspection"),
        ):
            await self.server._start_memorylake_application()
        self.assertIsNone(self.server.memorylake_application)
        self.assertIsNone(self.server.memorylake_file_runtime)
        self.assertEqual(self.probes, [])

    async def test_enabled_hook_returns_pending_then_exact_fake_version_becomes_ready(self):
        await self.server._start_memorylake_application()
        app = self.server.memorylake_application
        self.assertIsInstance(app, MemoryLakeApplication)
        self.assertIs(app.runtime, self.server.memorylake_file_runtime)
        self.assertFalse(app.status()["available"])
        await app.settle_readiness()
        self.assertEqual(
            app.status(),
            {
                "enabled": True,
                "available": True,
                "reason": "ready",
                "max_file_bytes": 20 * 1024 * 1024,
                "upload_ttl_seconds": 60,
            },
        )
        app.check_current()
        self.assertEqual(self.probes, [("--version",)])
        self.assertTrue(all(not path.exists() for path in self.probe_roots))
        with app.runtime.operation(), self.assertRaises(ValueError):
            await app.runtime.source.add(CallerContext(1, "agent", 1, 1), "x", b"x")

    async def test_unverified_hash_never_probes_and_wrong_version_never_activates(self):
        self.server.services = replace(
            self.server.services,
            config=OctopConfig(memorylake_cli=replace(self.config, sha256="0" * 64)),
        )
        await self.server._start_memorylake_application()
        app = self.server.memorylake_application
        await app.settle_readiness()
        self.assertFalse(app.status()["available"])
        self.assertEqual(self.probes, [])
        with self.assertRaises(ValueError):
            app.check_current()

    async def test_wrong_version_is_unavailable_and_scope_replacement_revokes_ready(self):
        self.output = b"memorylake v-other\n"
        await self.server._start_memorylake_application()
        app = self.server.memorylake_application
        await app.settle_readiness()
        self.assertEqual(app.status()["reason"], "version_mismatch")
        self.assertFalse(app.status()["available"])
        self.assertNotIn("v-other", repr(app.status()))

    async def test_readiness_hash_worker_settles_before_server_closes_database(self):
        entered, release = threading.Event(), threading.Event()
        events = []

        def blocked(config):
            entered.set()
            release.wait(3)
            with self.pool.connect() as conn:
                conn.execute("SELECT 1").fetchone()
            events.append("hash_done")
            return inspect_cli_artifact(config)

        with patch(
            "octop.infra.connectors.memorylake.application.inspect_cli_artifact",
            side_effect=blocked,
        ):
            await self.server._start_memorylake_application()
            app = self.server.memorylake_application
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            app._readiness.cancel()  # A cancel-all cannot abandon the real hash worker.
            await asyncio.sleep(0)
            self.assertFalse(app._readiness.done())
            stop = asyncio.create_task(self.server.stop())
            try:
                await asyncio.sleep(0)
                stop.cancel()
                await asyncio.sleep(0)
                self.assertFalse(stop.done())
                with self.pool.connect() as conn:
                    self.assertEqual(conn.execute("SELECT 1").fetchone()[0], 1)
            finally:
                release.set()
                await asyncio.gather(stop, return_exceptions=True)
        self.assertTrue(app.runtime.drained)
        self.assertEqual(events, ["hash_done"])
        self.assertEqual(self.probes, [])
        self.assertIsNone(self.server.memorylake_application)
        self.assertTrue(self.server.memorylake_file_runtime_stopping)

    async def test_provider_shares_admission_and_rejects_request_or_services_change(self):
        await self.server._start_memorylake_application()
        app = self.server.memorylake_application
        await app.settle_readiness()
        current = True

        def check():
            if not current:
                raise ValueError("request_closed")

        caller = CallerContext(1, "agent", 1, 1)
        factory = app.transport_provider(caller, "binding", "connector", check)
        loader = AsyncMock(return_value=object())
        transport = factory("connector", loader)
        credentials = transport._load_credentials.__self__
        self.assertIs(credentials._admission, app.runtime.database)
        self.assertIs(credentials._repo, self.server.services.memorylake_binding_repo)
        self.assertEqual(credentials._caller, caller)
        with self.assertRaises(ValueError):
            factory("other", loader)
        current = False
        with self.assertRaises(ValueError):
            await transport._load_binding()
        loader.assert_not_called()
        current = True
        previous = self.server.services
        self.server.services = replace(previous)
        try:
            self.assertFalse(app.status()["available"])
            with self.assertRaisesRegex(ValueError, "memorylake_application_unavailable"):
                app.check_current()
            with self.assertRaises(ValueError):
                factory("connector", loader)
        finally:
            self.server.services = previous

    async def test_actual_server_start_hook_with_all_external_boot_collaborators_stubbed(self):
        self.server._started = False
        self.server.app_runtime.user_manager.count = lambda: 1
        services = self.server.services
        from unittest.mock import MagicMock

        replacements = {
            "octop.infra.connectors.gateway.cli_install.ensure_cli_path": MagicMock(),
            "octop.infra.utils.env_file.apply_env_file": MagicMock(),
            "octop.infra.backend.grep_safety.install_safe_grep_fallback": MagicMock(),
            "octop.infra.backend.shell_output.install_shell_output_decoding": MagicMock(),
            "octop.infra.server.ensure_nofile_limit": MagicMock(),
            "octop.infra.server.ensure_mobile_capabilities_probed": MagicMock(
                return_value=self.server.config
            ),
            "octop.infra.server.ExpertCatalog": MagicMock(),
            "octop.infra.server.SubagentCatalog": MagicMock(),
            "octop.infra.server.PluginManager": MagicMock(),
            "octop.infra.server.should_defer_control_plane_db": MagicMock(return_value=False),
            "octop.infra.server.open_database": MagicMock(return_value=self.pool),
            "octop.infra.server.build_shared_services": MagicMock(return_value=services),
            "octop.infra.auth.captcha.boot_from_services": MagicMock(),
        }
        for target, replacement in replacements.items():
            self.enterContext(patch(target, replacement))
        for method in (
            "_setup_logging",
            "_apply_plugin_channels",
            "_ensure_jwt_secret",
            "_emit_wizard_password",
        ):
            self.enterContext(patch.object(self.server, method))
        self.enterContext(patch.object(self.server, "_boot_runtime", AsyncMock()))
        await self.server.start()  # Real method and optional hook, not a fake start.
        self.assertTrue(self.server._started)
        self.assertIsNotNone(self.server.memorylake_application)
        app = self.server.memorylake_application
        await app.settle_readiness()
        self.assertTrue(app.status()["available"])
        await self.server.start()
        self.assertIs(self.server.memorylake_application, app)
        self.assertEqual(self.probes, [("--version",)])

    async def test_configuration_change_invalidates_ready_application(self):
        await self.server._start_memorylake_application()
        app = self.server.memorylake_application
        await app.settle_readiness()
        self.assertTrue(app.status()["available"])
        # Frozen SharedServices normally changes as a whole; this isolates the
        # optional-config comparison as well as the separately tested identity.
        previous = self.server.services.config
        object.__setattr__(
            self.server.services,
            "config",
            replace(previous, memorylake_cli=replace(self.config, enabled=False)),
        )
        try:
            with self.assertRaisesRegex(ValueError, "memorylake_application_unavailable"):
                app.check_current()
            self.assertFalse(app.status()["available"])
        finally:
            object.__setattr__(self.server.services, "config", previous)

    async def test_stop_before_readiness_task_enters_never_probes(self):
        await self.server._start_memorylake_application()
        app = self.server.memorylake_application
        await self.server.stop()
        self.assertTrue(app._readiness.done())
        self.assertTrue(app.runtime.drained)
        self.assertEqual(self.probes, [])
        self.assertIsNone(self.server.memorylake_application)
