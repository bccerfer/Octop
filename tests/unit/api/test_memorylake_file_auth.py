"""Test-only FastAPI registration: actual ASGI body ordering, synthetic JWT/SQLite.

No production route/server start, multipart parser, credential transport or network.
"""

import asyncio
import json
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import jwt
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
from tests.support.memorylake import offline_test_guard

from octop.api.routers.memorylake_upload import authorize_memorylake_file_request
from octop.config import OctopConfig
from octop.infra.auth.tokens import sign_token
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.utils.paths import PathLayout


class FileAuthTests(unittest.IsolatedAsyncioTestCase):
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
        self.other = self.services.user_repo.create(username="other", role="admin")
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
        self.secret = b"S" * 32
        self.services.secret_repo.get_or_create("jwt", lambda: self.secret)
        self.token = sign_token(self.secret, sub=self.owner, uname="owner", role="user")
        self.factory_calls, self.minted = [], []
        self.auth_options = {}
        self.inside = None
        self.mode = "read"
        self.entered, self.release = asyncio.Event(), asyncio.Event()

    async def asyncSetUp(self):
        self.runtime = MemoryLakeFileRuntime(
            limits=FileRuntimeLimits(max_db_jobs=1),
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
        )
        self.addAsyncCleanup(self.runtime.aclose)
        self.server = SimpleNamespace(services=self.services, memorylake_file_runtime=self.runtime)
        self.app = FastAPI()

        @self.app.exception_handler(OctopError)
        async def safe_error(request, error):
            return JSONResponse({"code": error.code.value, "message": str(error)}, status_code=403)

        @self.app.post("/test-file")
        async def endpoint(request: Request):
            async with authorize_memorylake_file_request(
                request,
                self.server,
                agent_id="agent",
                binding_id="binding",
                transport_factory=self.transport_factory,
                **self.auth_options,
            ) as binding:
                self.minted.append(binding)
                self.entered.set()
                if self.inside is not None:
                    await self.inside(binding)
                if self.mode == "block":
                    await self.release.wait()
                if self.mode == "exception":
                    raise RuntimeError("test endpoint failure")
                body = await request.body()
                return {"owner": binding.caller.user_id, "size": len(body)}

    def transport_factory(self, connector_id, load_binding):
        self.factory_calls.append((connector_id, load_binding))

        class FakeTransport:
            async def upload(self, *args, **kwargs):
                raise AssertionError("auth test must not upload")

            async def import_file(self, *args, **kwargs):
                raise AssertionError("auth test must not import")

            async def get_document(self, *args, **kwargs):
                raise AssertionError("auth test must not query")

        return FakeTransport()

    async def invoke(self, headers=None, query=b"", state=None, observation=None):
        observed = observation if observation is not None else {"receives": 0, "messages": []}
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/test-file",
            "raw_path": b"/test-file",
            "query_string": query,
            "root_path": "",
            "headers": headers
            if headers is not None
            else [(b"authorization", ("Bearer " + self.token).encode())],
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
            "state": state or {},
        }

        async def receive():
            observed["receives"] += 1
            return {"type": "http.request", "body": b"synthetic body", "more_body": False}

        async def send(message):
            observed["messages"].append(message)

        await self.app(scope, receive, send)
        return observed

    def assert_denied_before_body(self, observed):
        self.assertEqual(observed["receives"], 0)
        start = next(m for m in observed["messages"] if m["type"] == "http.response.start")
        self.assertGreaterEqual(start["status"], 400)
        raw = b"".join(m.get("body", b"") for m in observed["messages"])
        self.assertNotIn(self.token.encode(), raw)
        self.assertNotIn(self.secret, raw)
        self.assertEqual(self.factory_calls, [])

    async def test_invalid_missing_expired_or_unsigned_identity_never_receives_body(self):
        expired = sign_token(
            self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=-1
        )
        wrong = sign_token(b"W" * 32, sub=self.owner, uname="owner", role="user")
        no_exp = jwt.encode({"sub": str(self.owner)}, self.secret, algorithm="HS256")
        for token in (None, "synthetic-malformed-jwt", expired, wrong, no_exp):
            with self.subTest(token_kind="missing" if token is None else "invalid"):
                headers = (
                    [] if token is None else [(b"authorization", ("Bearer " + token).encode())]
                )
                self.assert_denied_before_body(await self.invoke(headers=headers))

    async def test_queries_duplicate_headers_and_cached_user_forgery_do_not_authorize(self):
        normal = (b"authorization", ("Bearer " + self.token).encode())
        for query, headers in (
            (b"access_token=" + self.token.encode(), []),
            (b"as_user=1", [normal]),
            (b"arbitrary=value", [normal]),
            (b"", [normal, normal]),
        ):
            with self.subTest(query=query[:20]):
                self.assert_denied_before_body(await self.invoke(query=query, headers=headers))
        other_token = sign_token(self.secret, sub=self.other, uname="other", role="admin")
        forged = {
            "octop_user": SimpleNamespace(id=self.owner, role="admin"),
            "user": SimpleNamespace(id=self.owner),
        }
        self.assert_denied_before_body(
            await self.invoke(
                headers=[(b"authorization", ("Bearer " + other_token).encode())], state=forged
            )
        )

    async def test_shared_disabled_owner_or_write_policy_fail_before_body(self):
        changes = (
            ("agents", "is_shared", 1),
            ("users", "disabled", 1),
            ("memorylake_write_policies", "enabled", 0),
        )
        for table, column, value in changes:
            with self.subTest(table=table):
                with self.pool.transaction() as conn:
                    old = conn.execute(f"SELECT {column} FROM {table} LIMIT 1").fetchone()[0]
                    conn.execute(f"UPDATE {table} SET {column}=?", (value,))
                try:
                    self.assert_denied_before_body(await self.invoke())
                finally:
                    with self.pool.transaction() as conn:
                        conn.execute(f"UPDATE {table} SET {column}=?", (old,))

    async def test_owner_body_read_follows_authorization_and_context_closes_binding(self):
        observed = await self.invoke(state={"octop_user": SimpleNamespace(id=self.other)})
        self.assertEqual(observed["receives"], 1)
        start = next(m for m in observed["messages"] if m["type"] == "http.response.start")
        self.assertEqual(start["status"], 200)
        body = b"".join(m.get("body", b"") for m in observed["messages"])
        self.assertEqual(json.loads(body), {"owner": self.owner, "size": 14})
        self.assertEqual(len(self.minted), 1)
        with self.assertRaises(ValueError):
            await self.factory_calls[-1][1]()
        self.assertNotIn(self.token.encode(), body)
        self.assertNotIn(self.secret, body)

    async def test_handler_exception_and_cancellation_close_minted_binding(self):
        self.mode = "exception"
        observed = {"receives": 0, "messages": []}
        with self.assertRaises(RuntimeError):
            await self.invoke(observation=observed)
        self.assertEqual(observed["receives"], 0)
        with self.assertRaises(ValueError):
            await self.factory_calls[-1][1]()
        self.mode = "block"
        self.entered.clear()
        task = asyncio.create_task(self.invoke())
        try:
            await asyncio.wait_for(self.entered.wait(), timeout=2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            with self.assertRaises(ValueError):
                await self.factory_calls[-1][1]()
        finally:
            self.release.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_missing_runtime_denies_before_body(self):
        self.server.memorylake_file_runtime = None
        self.assert_denied_before_body(await self.invoke())

    async def test_blocked_auth_timeout_and_cancel_hold_actual_database_quota(self):
        for action in ("timeout", "cancel"):
            with self.subTest(action=action):
                self.auth_options = {"timeout_s": 0.03 if action == "timeout" else 5}
                started, release = threading.Event(), threading.Event()
                original = self.services.secret_repo.get_bounded

                def blocked(*args, original=original, started=started, release=release, **kwargs):
                    result = original(*args, **kwargs)
                    started.set()
                    release.wait(3)
                    return result

                observed = {"receives": 0, "messages": []}
                with patch.object(
                    self.services.secret_repo, "get_bounded", side_effect=blocked
                ) as read:
                    task = asyncio.create_task(self.invoke(observation=observed))
                    jobs = ()
                    try:
                        self.assertTrue(await asyncio.to_thread(started.wait, 1))
                        jobs = tuple(self.runtime.database._jobs)
                        if action == "cancel":
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        else:
                            done, _ = await asyncio.wait({task}, timeout=0.3)
                            self.assertIn(task, done)
                            self.assert_denied_before_body(task.result())
                        self.assertEqual(observed["receives"], 0)
                        self.assertEqual(len(jobs), 1)
                        self.assertFalse(jobs[0].done())
                        self.assert_denied_before_body(await self.invoke())
                        self.assertEqual(read.call_count, 1)
                    finally:
                        release.set()
                        await asyncio.gather(task, return_exceptions=True)
                        await asyncio.gather(*jobs, return_exceptions=True)
                self.assertFalse(self.runtime.database._jobs)

    async def test_jwt_key_rotation_is_checked_on_the_next_request(self):
        self.assertEqual((await self.invoke())["receives"], 1)
        self.factory_calls.clear()
        replacement = b"R" * 32
        self.services.secret_repo.rotate("jwt", replacement)
        self.assert_denied_before_body(await self.invoke())
        self.token = sign_token(replacement, sub=self.owner, uname="owner", role="user")
        self.assertEqual((await self.invoke())["receives"], 1)

    async def test_expiry_after_auth_worker_snapshot_denies_before_body(self):
        base = time.time()
        self.token = sign_token(
            self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=10
        )
        now = [base]
        original = self.services.memorylake_binding_repo.load_owned_write_binding

        def cross_expiry(*args, **kwargs):
            result = original(*args, **kwargs)
            now[0] = base + 11
            return result

        with (
            patch("octop.api.routers.memorylake_upload.time.time", side_effect=lambda: now[0]),
            patch.object(
                self.services.memorylake_binding_repo,
                "load_owned_write_binding",
                side_effect=cross_expiry,
            ),
        ):
            self.assert_denied_before_body(await self.invoke())

    async def test_observed_expiry_cannot_be_revived_by_wall_clock_rollback(self):
        base = time.time()
        self.token = sign_token(
            self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=10
        )
        now = [base]

        async def probe(binding):
            now[0] = base + 11
            with self.assertRaises(ValueError):
                await self.factory_calls[-1][1]()
            now[0] = base
            with self.assertRaises(ValueError):
                await self.factory_calls[-1][1]()
            # No body read is needed for this trusted test-only lifecycle probe.
            raise OctopError(ErrorCode.AUTH_FAILED, "auth_failed")

        self.inside = probe
        with patch("octop.api.routers.memorylake_upload.time.time", side_effect=lambda: now[0]):
            observed = await self.invoke()
        self.assertEqual(observed["receives"], 0)
        self.assertEqual(len(self.minted), 1)

    async def test_runtime_or_services_replacement_during_final_binding_lookup_denies(self):
        for kind in ("runtime", "services"):
            with self.subTest(kind=kind):
                calls = [0]
                original = self.services.memorylake_binding_repo.load_owned_write_binding

                def replace_owner(*args, kind=kind, calls=calls, original=original, **kwargs):
                    result = original(*args, **kwargs)
                    calls[0] += 1
                    if calls[0] == 2:
                        if kind == "runtime":
                            self.server.memorylake_file_runtime = None
                        else:
                            self.server.services = SharedServices(
                                self.services.paths, self.services.config, self.services.repos
                            )
                    return result

                try:
                    with patch.object(
                        self.services.memorylake_binding_repo,
                        "load_owned_write_binding",
                        side_effect=replace_owner,
                    ):
                        self.assert_denied_before_body(await self.invoke())
                    self.assertEqual(calls[0], 2)
                finally:
                    self.server.memorylake_file_runtime = self.runtime
                    self.server.services = self.services

    async def test_write_optout_between_initial_auth_and_binding_creation_denies_before_body(self):
        calls = [0]
        original = self.services.memorylake_binding_repo.load_owned_write_binding

        def revoke_on_second_snapshot(*args, **kwargs):
            calls[0] += 1
            if calls[0] == 2:
                self.services.memorylake_submission_repo.replace_policy(
                    binding_id="binding",
                    expected_revision=1,
                    project_id="project",
                    library_parent_id="folder",
                    enabled=False,
                )
            return original(*args, **kwargs)

        with patch.object(
            self.services.memorylake_binding_repo,
            "load_owned_write_binding",
            side_effect=revoke_on_second_snapshot,
        ):
            observed = await self.invoke()
        self.assertEqual(calls[0], 2)
        self.assert_denied_before_body(observed)

    async def test_jwt_remaining_lifetime_caps_monotonic_scope_despite_wall_rollback(self):
        self.token = sign_token(
            self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=60
        )
        expiry = jwt.decode(self.token, options={"verify_signature": False})["exp"]
        loop = asyncio.get_running_loop()
        mono, wall = [loop.time()], [expiry - 0.03]

        async def probe(binding):
            # No check ever sees wall-clock expiry: wall rolls back first.
            wall[0] -= 100
            await asyncio.sleep(0)
            mono[0] += 0.06
            with self.assertRaises(ValueError):
                binding.check_invocation()
            raise OctopError.localized(ErrorCode.AUTH_FAILED)

        self.inside = probe
        with (
            patch("octop.api.routers.memorylake_upload.time.time", side_effect=lambda: wall[0]),
            patch.object(loop, "time", side_effect=lambda: mono[0]),
        ):
            observed = await self.invoke()
        self.assertEqual(observed["receives"], 0)
        self.assertEqual(len(self.minted), 1)

    async def test_auth_sql_wait_cannot_rebase_jwt_deadline_after_wall_rollback(self):
        self.token = sign_token(
            self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=60
        )
        expiry = jwt.decode(self.token, options={"verify_signature": False})["exp"]
        loop = asyncio.get_running_loop()
        mono, wall = [loop.time()], [expiry - 0.03]
        original = self.services.memorylake_binding_repo.load_owned_write_binding

        def delayed_snapshot(*args, **kwargs):
            result = original(*args, **kwargs)
            wall[0] -= 100
            mono[0] += 0.06
            return result

        with (
            patch("octop.api.routers.memorylake_upload.time.time", side_effect=lambda: wall[0]),
            patch.object(loop, "time", side_effect=lambda: mono[0]),
            patch.object(
                self.services.memorylake_binding_repo,
                "load_owned_write_binding",
                side_effect=delayed_snapshot,
            ),
        ):
            self.assert_denied_before_body(await self.invoke())
        self.assertEqual(self.factory_calls, [])
