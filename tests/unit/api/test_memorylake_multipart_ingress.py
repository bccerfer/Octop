"""Disposable ASGI multipart ingress: real JWT/SQLite, synthetic payload only.

No production route, FastAPI File dependency, external upload, CLI, or socket.
"""

import asyncio
import hashlib
import json
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
from tests.support.memorylake import offline_test_guard

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
from octop.infra.errors import OctopError
from octop.infra.utils.paths import PathLayout


class MultipartIngressTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = Path(self.enterContext(TemporaryDirectory()))
        self.pool = SqlitePool(root / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.services = SharedServices(
            PathLayout(root), OctopConfig(), RepoBundle.from_pool(self.pool)
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
        self.token = sign_token(secret, sub=self.owner, uname="owner", role="user")
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.remote_calls = []
        self.options = {}
        self.during_receive = None
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
            limits=FileRuntimeLimits(
                max_file_bytes=64, max_total_bytes=3 * 64 + 65536, max_files=8
            ),
        )
        self.addAsyncCleanup(self.runtime.aclose)
        self.server = SimpleNamespace(services=self.services, memorylake_file_runtime=self.runtime)
        self.app = FastAPI()

        @self.app.exception_handler(OctopError)
        async def safe_error(request, error):
            return JSONResponse({"code": error.code.value, "message": str(error)}, status_code=403)

        @self.app.post("/test-upload")
        async def upload(request: Request):
            identifier = await receive_memorylake_file_request(
                request,
                self.server,
                agent_id="agent",
                binding_id="binding",
                transport_factory=self.transport_factory,
                **self.options,
            )
            return {"upload_id": identifier}

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

    async def invoke(self, body, *, authorized=True, blocked=None, started=None, observation=None):
        observed = observation if observation is not None else {"receives": 0, "messages": []}
        headers = [(b"content-type", b"multipart/form-data; boundary=" + self.boundary)]
        if authorized:
            headers.append((b"authorization", ("Bearer " + self.token).encode()))
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/test-upload",
            "raw_path": b"/test-upload",
            "query_string": b"",
            "root_path": "",
            "headers": headers,
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
            "state": {},
        }
        chunks = [body[i : i + 7] for i in range(0, len(body), 7)] or [b""]
        position = 0

        async def receive():
            nonlocal position
            observed["receives"] += 1
            if started is not None:
                started.set()
            if blocked is not None:
                await blocked.wait()
            if position == 0 and self.during_receive is not None:
                self.during_receive()
            content = chunks[position] if position < len(chunks) else b""
            position += 1
            return {"type": "http.request", "body": content, "more_body": position < len(chunks)}

        async def send(message):
            observed["messages"].append(message)

        await self.app(scope, receive, send)
        return observed

    def status(self, observed):
        return next(m["status"] for m in observed["messages"] if m["type"] == "http.response.start")

    def assert_clean(self):
        source = self.runtime.source
        self.assertFalse(source._entries)
        self.assertEqual((source._bytes, source._pending_files, source._pending_bytes), (0, 0, 0))
        self.assertEqual(self.remote_calls, [])

    async def test_chunked_multipart_publishes_scoped_snapshot_without_approval_or_remote_call(
        self,
    ):
        observed = await self.invoke(self.multipart())
        self.assertEqual(self.status(observed), 200)
        self.assertGreater(observed["receives"], 1)
        output = json.loads(b"".join(m.get("body", b"") for m in observed["messages"]))
        identifier = output["upload_id"]
        self.assertRegex(identifier, r"^[A-Za-z0-9][A-Za-z0-9:_-]{0,127}$")
        binding = await self.trusted_binding()
        intent = await binding.prepare(identifier)
        self.assertEqual(
            (intent.filename, intent.size, intent.sha256),
            ("synthetic.txt", len(self.payload), hashlib.sha256(self.payload).hexdigest()),
        )
        with self.pool.connect() as conn:
            for table in ("memorylake_submission_approvals", "memorylake_submissions"):
                self.assertEqual(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
        self.assertEqual(self.remote_calls, [])
        self.assertEqual(
            (self.runtime.source._pending_files, self.runtime.source._pending_bytes), (0, 0)
        )

    async def test_missing_auth_rejected_before_body_receive(self):
        observed = await self.invoke(self.multipart(), authorized=False)
        self.assertGreaterEqual(self.status(observed), 400)
        self.assertEqual(observed["receives"], 0)
        self.assert_clean()

    async def test_shared_snapshot_budget_reserved_before_body_receive(self):
        binding = await self.trusted_binding()
        with self.runtime.operation():
            existing = await binding.source.add(self.caller, "existing.txt", b"x")
        observed = await self.invoke(self.multipart())
        self.assertGreaterEqual(self.status(observed), 400)
        self.assertEqual(observed["receives"], 0)
        self.assertEqual(len(self.runtime.source._entries), 1)
        self.assertEqual((await binding.prepare(existing)).size, 1)
        self.assertEqual(self.remote_calls, [])

    async def test_malformed_truncated_or_second_file_never_publishes_snapshot(self):
        for body in (b"not multipart", self.multipart()[:-20], self.multipart(second=True)):
            with self.subTest(body_kind="invalid"):
                observed = await self.invoke(body)
                self.assertGreaterEqual(self.status(observed), 400)
                self.assert_clean()

    async def test_blocked_body_cancellation_or_deadline_releases_reservation(self):
        for action in ("cancel", "timeout"):
            with self.subTest(action=action):
                self.options = {"max_request_s": 0.1 if action == "timeout" else 30}
                started, release = asyncio.Event(), asyncio.Event()
                observed = {"receives": 0, "messages": []}
                task = asyncio.create_task(
                    self.invoke(
                        self.multipart(), started=started, blocked=release, observation=observed
                    )
                )
                try:
                    await asyncio.wait_for(started.wait(), timeout=2)
                    self.assertGreater(self.runtime.source._pending_bytes, 0)
                    if action == "cancel":
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    else:
                        done, _ = await asyncio.wait({task}, timeout=0.4)
                        self.assertIn(task, done)
                        self.assertGreaterEqual(self.status(task.result()), 400)
                    self.assert_clean()
                finally:
                    release.set()
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_live_db_revocation_during_receive_rejects_publication(self):
        for table in ("users", "memorylake_bindings", "memorylake_write_policies"):
            column = "disabled" if table == "users" else "enabled"
            revoked = 1 if table == "users" else 0

            def revoke(table=table, column=column, revoked=revoked):
                with self.pool.transaction() as conn:
                    conn.execute(f"UPDATE {table} SET {column}=?", (revoked,))

            self.during_receive = revoke
            try:
                with self.subTest(table=table):
                    observed = await self.invoke(self.multipart())
                    self.assertGreater(observed["receives"], 0)
                    self.assertGreaterEqual(self.status(observed), 400)
                    self.assert_clean()
            finally:
                with self.pool.transaction() as conn:
                    conn.execute(f"UPDATE {table} SET {column}=?", (1 - revoked,))

    async def test_jwt_expiry_during_receive_rejects_without_publication(self):
        now = [time.time()]
        self.token = sign_token(
            self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=10
        )

        def expire():
            now[0] += 11

        self.during_receive = expire
        with patch("octop.api.routers.memorylake_upload.time.time", side_effect=lambda: now[0]):
            observed = await self.invoke(self.multipart())
        self.assertEqual(observed["receives"], 1)
        self.assertGreaterEqual(self.status(observed), 400)
        self.assert_clean()

    async def test_runtime_close_during_receive_never_publishes(self):
        self.during_receive = self.runtime.close
        observed = await self.invoke(self.multipart())
        self.assertEqual(observed["receives"], 1)
        self.assertGreaterEqual(self.status(observed), 400)
        self.assert_clean()
        self.assertTrue(self.runtime.closed)
