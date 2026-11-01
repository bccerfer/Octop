"""Characterize CURRENT legacy middleware gates, NOT a production-auth fix.

Actual build_app, real disposable SQLite and cached UserManager; no server.start,
MemoryLake registration, external service or network. The test-only endpoint is
not an upload handler. Synchronous DB reads/cache acceptance below are known gates
that an isolated future authenticated entry must address without changing native
routes accidentally.
"""

import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi import Request
from tests.support.memorylake import offline_test_guard

from octop.api.app import build_app
from octop.config import OctopConfig
from octop.infra.auth.tokens import sign_token
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.users.manager import UserManager
from octop.infra.utils.paths import PathLayout


class MemoryLakeAppBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.services = SharedServices(
            PathLayout(root), OctopConfig(enable_dashboard=False), RepoBundle.from_pool(self.pool)
        )
        self.owner = self.services.user_repo.create(username="synthetic-owner", role="user")
        secret = b"S" * 32
        with self.pool.transaction() as conn:
            conn.execute("INSERT INTO secrets(k,v,created_at) VALUES (?,?,0)", ("jwt", secret))
        self.token = sign_token(
            secret, sub=self.owner, uname="synthetic-owner", role="user", ttl_seconds=30
        )
        self.events = []
        self.endpoint_calls = 0

    async def asyncSetUp(self):
        self.loop_thread = threading.get_ident()
        self.users = UserManager(self.services)
        await self.users.boot()
        self.addAsyncCleanup(self.users.shutdown_all)
        self.server = SimpleNamespace(
            services=self.services,
            user_manager=self.users,
            app_runtime=None,
            _started=True,
            memorylake_application=None,
        )
        self.app = build_app(self.server)
        self.native_routes = tuple(self.app.routes)

        @self.app.get("/api/_test_local_auth_boundary")
        async def probe(request: Request):
            self.endpoint_calls += 1
            self.events.append(("endpoint", threading.get_ident() == self.loop_thread))
            return {"cached_user_id": request.state.octop_user.id}

        get = self.services.secret_repo.get
        count = self.services.user_repo.count

        def secret_get(*args, **kwargs):
            self.events.append(("secret.get", threading.get_ident() == self.loop_thread))
            return get(*args, **kwargs)

        def user_count(*args, **kwargs):
            self.events.append(("user.count", threading.get_ident() == self.loop_thread))
            return count(*args, **kwargs)

        self.enterContext(patch.object(self.services.secret_repo, "get", side_effect=secret_get))
        self.enterContext(patch.object(self.services.user_repo, "count", side_effect=user_count))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://synthetic"
        )
        self.addAsyncCleanup(self.client.aclose)

    async def test_production_memorylake_routes_remain_default_disabled(self):
        # API declarations are now present, while native defaults create no
        # runtime/CLI capability. Registration is not deployment activation.
        status = await self.client.get(
            "/api/memorylake/status", headers={"Authorization": "Bearer " + self.token}
        )
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["available"], False)
        self.assertEqual(status.json()["enabled"], False)
        response = await self.client.post(
            "/api/memorylake/agents/agent/bindings/binding/uploads",
            headers={"Authorization": "Bearer " + self.token},
            content=b"synthetic unconsumed file",
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(self.endpoint_calls, 0)
        self.assertIsNone(self.server.memorylake_application)

    async def test_current_legacy_sync_reads_surround_endpoint_and_slide_renew(self):
        response = await self.client.get(
            "/api/_test_local_auth_boundary", headers={"Authorization": "Bearer " + self.token}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"cached_user_id": self.owner})
        self.assertTrue(response.headers.get("X-Octop-Access-Token"))
        self.assertEqual(
            self.events,
            [
                ("user.count", True),
                ("secret.get", True),
                ("endpoint", True),
                ("secret.get", True),
                ("secret.get", True),
            ],
        )

    async def test_current_cached_user_can_outlive_direct_db_disable_known_gate(self):
        with self.pool.transaction() as conn:
            conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
        self.assertTrue(self.services.user_repo.get(self.owner).disabled)
        response = await self.client.get(
            "/api/_test_local_auth_boundary", headers={"Authorization": "Bearer " + self.token}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["cached_user_id"], self.owner)
        self.assertTrue(response.headers.get("X-Octop-Access-Token"))
        self.assertEqual(self.endpoint_calls, 1)
        # This endpoint intentionally uses legacy state, NOT the new helper's
        # fresh SQL owner check. No claim that MemoryLake accepts this user.

    async def test_native_malformed_token_never_calls_endpoint(self):
        response = await self.client.get(
            "/api/_test_local_auth_boundary",
            headers={"Authorization": "Bearer synthetic-invalid-token"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(self.endpoint_calls, 0)
        self.assertNotIn("X-Octop-Access-Token", response.headers)
        self.assertEqual(self.events, [("user.count", True), ("secret.get", True)])
