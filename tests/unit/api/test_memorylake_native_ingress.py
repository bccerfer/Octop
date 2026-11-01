"""Actual build_app with a TEST-ONLY upload route, retaining every middleware.

Synthetic JWT/SQLite/body and fake transport; no production registration or server
start. Legacy synchronous auth DB reads, cached users, sliding renewal and proxy
buffering are retained technical debt, not a bounded whole-app security claim.
"""

import hashlib
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import Request
from tests.support.memorylake import offline_test_guard

from octop.api.app import build_app
from octop.api.routers.memorylake_upload import receive_memorylake_file_request
from octop.config import OctopConfig
from octop.infra.auth.tokens import sign_token
from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.file_binding import MemoryLakeFileBinding
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.users.manager import UserManager
from octop.infra.utils.paths import PathLayout

PATH = "/api/_test_memorylake/agents/agent/bindings/binding/uploads"


class NativeIngressTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.services = SharedServices(
            PathLayout(root),
            OctopConfig(enable_dashboard=False, cors_origins=["https://synthetic.example"]),
            RepoBundle.from_pool(self.pool),
        )
        self.owner = self.services.user_repo.create(username="owner", role="user")
        self.services.agent_repo.create(agent_id="agent", user_id=self.owner, name="agent")
        self.services.connector_repo.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="ml",
            mcp_server_name="ml",
        )
        self.services.connector_repo.upsert_credentials(
            instance_id="connector", blob=b"synthetic-no-decryption"
        )
        self.services.memorylake_binding_repo.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project"]',
            enabled=True,
        )
        self.services.memorylake_submission_repo.replace_policy(
            binding_id="binding",
            expected_revision=0,
            project_id="project",
            library_parent_id="folder",
            enabled=True,
        )
        secret = self.secret = b"U" * 32
        self.services.secret_repo.get_or_create("jwt", lambda: secret)
        self.token = sign_token(secret, sub=self.owner, uname="owner", role="user", ttl_seconds=30)
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.remote_calls = []
        self.boundary = b"memorylake-synthetic-boundary"
        self.payload = b"synthetic payload"

    async def asyncSetUp(self):
        self.runtime = MemoryLakeFileRuntime(
            authorize_factory=lambda admission: (
                StoredWriteBindingLoader(
                    caller_user_id=self.owner,
                    agent_id="agent",
                    is_group=False,
                    binding_id="binding",
                    connector_id="connector",
                    repo=self.services.memorylake_binding_repo,
                    admission=admission,
                ).authorize
            ),
            limits=FileRuntimeLimits(max_file_bytes=64, max_total_bytes=3 * 64 + 65536),
        )
        self.addAsyncCleanup(self.runtime.aclose)
        self.users = UserManager(self.services)
        await self.users.boot()
        self.addAsyncCleanup(self.users.shutdown_all)
        self.bridge = SimpleNamespace(bind_asgi_app=lambda *a, **k: None, tunnel_http=AsyncMock())
        self.server = SimpleNamespace(
            services=self.services,
            user_manager=self.users,
            app_runtime=SimpleNamespace(bridge_manager=self.bridge),
            memorylake_file_runtime=self.runtime,
        )
        self.app = build_app(self.server)
        self.events = []
        self.loop_thread = threading.get_ident()
        get, count = self.services.secret_repo.get, self.services.user_repo.count

        def observed_get(*a, **k):
            self.events.append(("get", threading.get_ident() == self.loop_thread))
            return get(*a, **k)

        def observed_count(*a, **k):
            self.events.append(("count", threading.get_ident() == self.loop_thread))
            return count(*a, **k)

        self.enterContext(patch.object(self.services.secret_repo, "get", side_effect=observed_get))
        self.enterContext(
            patch.object(self.services.user_repo, "count", side_effect=observed_count)
        )

        @self.app.post(PATH)
        async def upload(request: Request):
            self.events.append(("handler", True))
            identifier = await receive_memorylake_file_request(
                request,
                self.server,
                agent_id="agent",
                binding_id="binding",
                transport_factory=self.transport_factory,
            )
            return {"upload_id": identifier}

        @self.app.get("/api/_test_native_probe")
        async def probe(request: Request):
            return {"owner": request.state.octop_user.id}

        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://synthetic"
        )
        self.addAsyncCleanup(self.client.aclose)

    def transport_factory(self, connector_id, load_binding):
        outer = self

        class FakeTransport:
            async def upload(self, *args, **kwargs):
                outer.remote_calls.append("upload")
                raise AssertionError("ingress must not upload")

            async def import_file(self, *args, **kwargs):
                outer.remote_calls.append("import")
                raise AssertionError("ingress must not import")

            async def get_document(self, *args, **kwargs):
                outer.remote_calls.append("get")
                raise AssertionError("ingress must not query")

        return FakeTransport()

    async def trusted_binding(self):
        binding = await MemoryLakeFileBinding.create(
            runtime=self.runtime,
            caller=self.caller,
            binding_id="binding",
            connector_id="connector",
            binding_repo=self.services.memorylake_binding_repo,
            submission_repo=self.services.memorylake_submission_repo,
            transport_factory=self.transport_factory,
            check_invocation=lambda: None,
        )

        async def close():
            binding.close()

        self.addAsyncCleanup(close)
        return binding

    def multipart(self, *, second=False):
        part = (
            b"--"
            + self.boundary
            + b'\r\nContent-Disposition: form-data; name="file"; filename="synthetic.txt"\r\nContent-Type: application/octet-stream\r\n\r\n'
            + self.payload
            + b"\r\n"
        )
        return part + (part if second else b"") + b"--" + self.boundary + b"--\r\n"

    async def request(self, *, headers=None, query=""):
        async def chunks():
            body = self.multipart()
            for pos in range(0, len(body), 7):
                self.events.append(("body_frame", True))
                yield body[pos : pos + 7]

        return await self.client.post(
            PATH + query,
            content=chunks(),
            headers=headers
            or [
                ("Authorization", "Bearer " + self.token),
                ("Content-Type", "multipart/form-data; boundary=" + self.boundary.decode()),
                ("Origin", "https://synthetic.example"),
            ],
        )

    async def test_valid_native_stack_then_strong_auth_and_scoped_snapshot(self):
        response = await self.request()
        self.assertEqual(response.status_code, 200, response.text)
        binding = await self.trusted_binding()
        intent = await binding.prepare(response.json()["upload_id"])
        self.assertEqual(
            (intent.filename, intent.size, intent.sha256),
            ("synthetic.txt", len(self.payload), hashlib.sha256(self.payload).hexdigest()),
        )
        self.assertEqual(
            response.headers["access-control-allow-origin"], "https://synthetic.example"
        )
        self.assertIn(("count", True), self.events)
        self.assertIn(("get", True), self.events)
        self.assertLess(
            self.events.index(("handler", True)), self.events.index(("body_frame", True))
        )
        self.assertTrue(response.headers.get("X-Octop-Access-Token"))
        with self.pool.connect() as conn:
            for table in ("memorylake_submissions", "memorylake_submission_approvals"):
                self.assertEqual(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        self.assertEqual(self.remote_calls, [])

    async def test_live_denial_overrides_cached_user_before_body_and_keeps_native_behavior(self):
        for mutation in ("disabled", "shared", "writeoff"):
            with self.subTest(mutation=mutation):
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE users SET disabled=? WHERE id=?",
                        (int(mutation == "disabled"), self.owner),
                    )
                    conn.execute(
                        "UPDATE agents SET is_shared=? WHERE agent_id='agent'",
                        (int(mutation == "shared"),),
                    )
                    conn.execute(
                        "UPDATE memorylake_write_policies SET enabled=?",
                        (int(mutation != "writeoff"),),
                    )
                self.events.clear()
                response = await self.request()
                self.assertEqual(response.status_code, 401, response.text)
                self.assertIn(("handler", True), self.events)
                self.assertEqual(response.json()["error"]["code"], "AUTH_FAILED")
                self.assertNotIn(("body_frame", True), self.events)
                self.assertFalse(self.runtime.source._entries)
                self.assertEqual(
                    response.headers["access-control-allow-origin"], "https://synthetic.example"
                )
                self.assertIn("error", response.json())
                # Retained legacy renewal on denied response is NOT strong-auth success.
                self.assertTrue(response.headers.get("X-Octop-Access-Token"))
                native = await self.client.get(
                    "/api/_test_native_probe", headers={"Authorization": "Bearer " + self.token}
                )
                self.assertEqual(native.json(), {"owner": self.owner})
        self.assertEqual(self.remote_calls, [])

    async def test_duplicate_authorization_and_legacy_query_token_cannot_authorize_helper(self):
        auth = ("Authorization", "Bearer " + self.token)
        for headers, query in (
            ([auth, auth], ""),
            ([("Content-Type", "application/octet-stream")], "?access_token=" + self.token),
        ):
            with self.subTest(query=bool(query)):
                self.events.clear()
                response = await self.request(headers=headers, query=query)
                self.assertEqual(response.status_code, 401, response.text)
                self.assertIn(("handler", True), self.events)
                self.assertEqual(response.json()["error"]["code"], "AUTH_FAILED")
                self.assertNotIn(("body_frame", True), self.events)
                self.assertNotIn(self.token, response.text)
                self.assertFalse(self.runtime.source._entries)

    async def test_bridge_header_does_not_tunnel_local_namespace(self):
        response = await self.request(
            headers=[
                ("Authorization", "Bearer " + self.token),
                ("Content-Type", "multipart/form-data; boundary=" + self.boundary.decode()),
                ("X-Octop-Agent-Id", "bridge:remote:agent"),
            ]
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.bridge.tunnel_http.assert_not_called()
        self.assertEqual(self.remote_calls, [])

    async def test_unauthenticated_cors_preflight_retains_native_jwt_gate(self):
        # Successful authorized responses get CORS headers, but JWT is outside
        # CORSMiddleware: this is NOT complete browser cross-origin support.
        for path in (PATH, "/api/_test_native_probe"):
            with self.subTest(path=path):
                self.events.clear()
                response = await self.client.options(
                    path,
                    headers={
                        "Origin": "https://synthetic.example",
                        "Access-Control-Request-Method": "POST" if path == PATH else "GET",
                        "Access-Control-Request-Headers": "authorization,content-type",
                    },
                )
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.json()["error"]["code"], "AUTH_FAILED")
                self.assertNotIn("access-control-allow-origin", response.headers)
                self.assertNotIn(("handler", True), self.events)
                self.assertFalse(self.runtime.source._entries)
        self.assertEqual(self.remote_calls, [])
