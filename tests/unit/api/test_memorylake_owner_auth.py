"""Owner-only request scope: real SQLite/JWT; no router, CLI, body or network."""

import asyncio
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import jwt
from fastapi import Request
from tests.support.memorylake import offline_test_guard

from octop.api.common.memorylake_auth import authorize_memorylake_owner_request
from octop.config import OctopConfig
from octop.infra.auth.tokens import sign_token
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.services import RepoBundle, SharedServices
from octop.infra.errors import OctopError
from octop.infra.utils.paths import PathLayout


class OwnerAuthTests(unittest.IsolatedAsyncioTestCase):
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
        self.secret = b"Q" * 32
        self.services.secret_repo.get_or_create("jwt", lambda: self.secret)
        self.token = sign_token(self.secret, sub=self.owner, uname="owner", role="user")
        self.receive = AsyncMock(side_effect=AssertionError("no body"))

    async def asyncSetUp(self):
        async def deny(caller):
            raise ValueError("unscoped")

        self.runtime = MemoryLakeFileRuntime(
            authorize_factory=lambda _: deny, limits=FileRuntimeLimits(max_db_jobs=1)
        )
        self.addAsyncCleanup(self.runtime.aclose)
        self.available = True

        def check():
            if not self.available:
                raise ValueError("synthetic-private-error")

        self.application = SimpleNamespace(runtime=self.runtime, check_current=check)
        self.server = SimpleNamespace(
            services=self.services,
            memorylake_application=self.application,
            memorylake_file_runtime=self.runtime,
        )

    def request(self, token=None, *, query=b"", duplicate=False):
        headers = [
            (b"authorization", ("Bearer " + (self.token if token is None else token)).encode())
        ]
        if duplicate:
            headers += headers
        return Request(
            {
                "type": "http",
                "headers": headers,
                "query_string": query,
                "state": {"octop_user": SimpleNamespace(id=self.owner)},
            },
            self.receive,
        )

    def scope(self, request=None, **kwargs):
        return authorize_memorylake_owner_request(
            request or self.request(),
            self.server,
            agent_id="agent",
            application=self.application,
            **kwargs,
        )

    async def test_current_owner_and_released_operation_then_closed_scope(self):
        async with self.scope() as owner:
            self.assertEqual((owner.user_id, owner.agent_id), (self.owner, "agent"))
            owner.check_current()
            with self.runtime.operation():
                pass  # No nested operation across yield.
        with self.assertRaises(OctopError):
            owner.check_current()
        self.receive.assert_not_called()

    async def test_invalid_credentials_and_request_aliases_never_read_body(self):
        other = sign_token(self.secret, sub=self.other, uname="other", role="admin")
        for req in [
            self.request("sentinel"),
            self.request(sign_token(b"W" * 32, sub=self.owner, uname="owner", role="user")),
            self.request(
                sign_token(self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=-1)
            ),
            self.request(jwt.encode({"sub": str(self.owner)}, self.secret, algorithm="HS256")),
            Request({"type": "http", "headers": [], "query_string": b""}, self.receive),
            self.request(other),
            self.request(query=b"as_user=owner"),
            self.request(query=b"access_token=sentinel"),
            self.request(duplicate=True),
        ]:
            with self.subTest(headers=len(req.scope["headers"])):
                with self.assertRaises(OctopError) as caught:
                    async with self.scope(req):
                        self.fail("must deny")
                self.assertNotIn("sentinel", str(caught.exception))
        self.receive.assert_not_called()

    async def test_disabled_shared_nonexpert_and_unavailable_deny_cached_user(self):
        for case in ["disabled", "shared", "team", "unavailable"]:
            with self.subTest(case=case):
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE users SET disabled=? WHERE id=?", (case == "disabled", self.owner)
                    )
                    conn.execute(
                        "UPDATE agents SET is_shared=?,kind=? WHERE agent_id='agent'",
                        (case == "shared", "team" if case == "team" else "expert"),
                    )
                self.available = case != "unavailable"
                with self.assertRaises(OctopError):
                    async with self.scope():
                        self.fail("must deny")
        self.receive.assert_not_called()

    async def test_expiry_latches_and_service_replacement_denies(self):
        future = time.time() + 1000
        token = sign_token(self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=500)
        with self.assertRaises(OctopError):
            async with self.scope(self.request(token)) as owner:
                with (
                    patch("octop.api.common.memorylake_auth.time.time", return_value=future),
                    self.assertRaises(OctopError),
                ):
                    owner.check_current()
                with self.assertRaises(OctopError):
                    owner.check_current()
        with self.assertRaises(OctopError):
            async with self.scope() as owner:
                self.server.services = object()
                owner.check_current()
        self.server.services = self.services

    async def test_cancelled_or_timed_out_worker_holds_quota_until_actual_completion(self):
        original = self.services.secret_repo.get_bounded
        for cancel in [False, True]:
            with self.subTest(cancel=cancel):
                started, release = threading.Event(), threading.Event()

                def blocked(*args, started=started, release=release, **kwargs):
                    started.set()
                    release.wait(3)
                    return original(*args, **kwargs)

                async def enter(cancel=cancel):
                    async with self.scope(timeout_s=0.03 if not cancel else 5):
                        self.fail("must not yield")

                with patch.object(self.services.secret_repo, "get_bounded", side_effect=blocked):
                    task = asyncio.create_task(enter())
                    try:
                        for _ in range(100):
                            if started.is_set():
                                break
                            await asyncio.sleep(0.001)
                        self.assertTrue(started.is_set())
                        jobs = tuple(self.runtime.database._jobs)
                        if cancel:
                            task.cancel()
                        done, _ = await asyncio.wait({task}, timeout=0.3)
                        self.assertTrue(done)
                        with self.assertRaises(asyncio.CancelledError if cancel else OctopError):
                            await task
                        self.assertEqual(len(self.runtime.database._jobs), 1)
                        with self.assertRaises(OctopError):
                            async with self.scope():
                                self.fail("quota")
                    finally:
                        release.set()
                        await asyncio.gather(*jobs, return_exceptions=True)
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                self.assertFalse(self.runtime.database._jobs)
        self.receive.assert_not_called()

    async def test_worker_completion_rechecks_expiry_and_server_identity(self):
        original = self.services.memorylake_binding_repo.owns_agent
        real_now = time.time()
        token = sign_token(self.secret, sub=self.owner, uname="owner", role="user", ttl_seconds=100)
        for change in ("expiry", "services", "runtime", "application"):
            with self.subTest(change=change):
                now = [real_now]

                def changed(*args, change=change, now=now, **kwargs):
                    allowed = original(*args, **kwargs)
                    if change == "expiry":
                        now[0] = real_now + 200
                    elif change == "services":
                        self.server.services = object()
                    elif change == "runtime":
                        self.server.memorylake_file_runtime = object()
                    else:
                        self.server.memorylake_application = object()
                    return allowed

                try:
                    with (
                        patch.object(
                            self.services.memorylake_binding_repo, "owns_agent", side_effect=changed
                        ),
                        patch(
                            "octop.api.common.memorylake_auth.time.time",
                            side_effect=lambda now=now: now[0],
                        ),
                        self.assertRaises(OctopError),
                    ):
                        async with self.scope(self.request(token)):
                            self.fail("stale result")
                finally:
                    self.server.services = self.services
                    self.server.memorylake_file_runtime = self.runtime
                    self.server.memorylake_application = self.application
        self.receive.assert_not_called()

    async def test_cancellation_after_yield_closes_captured_owner(self):
        entered = asyncio.Event()
        captured = []

        async def consumer():
            async with self.scope() as owner:
                captured.append(owner)
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(consumer())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(OctopError):
            captured[0].check_current()
        self.receive.assert_not_called()

    async def test_fresh_disclosure_denies_revoked_owner_and_rotated_jwt_key(self):
        for change in ("owner", "jwt"):
            with self.subTest(change=change), self.assertRaises(OctopError):
                async with self.scope() as owner:
                    if change == "owner":
                        with self.pool.transaction() as conn:
                            conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
                    else:
                        self.services.secret_repo.rotate("jwt", b"R" * 32)
                    await owner.check_disclosure()
            with self.pool.transaction() as conn:
                conn.execute("UPDATE users SET disabled=0 WHERE id=?", (self.owner,))
            self.services.secret_repo.rotate("jwt", self.secret)

    async def test_monotonic_expiry_survives_wall_clock_rollback(self):
        now = time.time()
        wall = [now]
        loop = asyncio.get_running_loop()
        original_mono = loop.time
        shift = [0.0]
        with (
            patch(
                "octop.api.common.memorylake_auth.decode_token",
                return_value={"sub": self.owner, "exp": now + 1},
            ),
            patch("octop.api.common.memorylake_auth.time.time", side_effect=lambda: wall[0]),
            patch.object(loop, "time", side_effect=lambda: original_mono() + shift[0]),
            self.assertRaises(OctopError),
        ):
            async with self.scope() as owner:
                wall[0] = now - 100
                shift[0] = 2
                owner.check_current()

    async def test_disclosure_timeout_retains_worker_admission_and_latches_closed(self):
        started, release = threading.Event(), threading.Event()
        original = self.services.secret_repo.get_bounded

        def blocked(*args, **kwargs):
            started.set()
            release.wait(3)
            return original(*args, **kwargs)

        with self.assertRaises(OctopError):
            async with self.scope(timeout_s=0.03) as owner:
                with patch.object(self.services.secret_repo, "get_bounded", side_effect=blocked):
                    try:
                        with self.assertRaises(OctopError):
                            await owner.check_disclosure()
                        self.assertTrue(started.is_set())
                        jobs = tuple(self.runtime.database._jobs)
                        self.assertEqual(len(jobs), 1)
                        with self.assertRaises(OctopError):
                            owner.check_current()
                    finally:
                        release.set()
                        await asyncio.gather(
                            *tuple(self.runtime.database._jobs), return_exceptions=True
                        )

    async def test_disclosure_worker_return_rechecks_expiry(self):
        now = time.time()
        original = self.services.memorylake_binding_repo.owns_agent

        with self.assertRaises(OctopError):
            async with self.scope() as owner:
                wall = [now]

                def elapsed(*args, **kwargs):
                    result = original(*args, **kwargs)
                    wall[0] = now + 1e9
                    return result

                with (
                    patch.object(
                        self.services.memorylake_binding_repo, "owns_agent", side_effect=elapsed
                    ),
                    patch(
                        "octop.api.common.memorylake_auth.time.time", side_effect=lambda: wall[0]
                    ),
                ):
                    await owner.check_disclosure()

    async def test_auth_worker_wall_rollback_cannot_extend_original_jwt_remaining_time(self):
        now = time.time()
        wall, shift = [now], [0.0]
        loop = asyncio.get_running_loop()
        mono = loop.time
        original = self.services.memorylake_binding_repo.owns_agent

        def delayed(*args, **kwargs):
            result = original(*args, **kwargs)
            wall[0] = now - 100
            shift[0] = 0.06
            return result

        with (
            patch(
                "octop.api.common.memorylake_auth.decode_token",
                return_value={"sub": self.owner, "exp": now + 0.03},
            ),
            patch("octop.api.common.memorylake_auth.time.time", side_effect=lambda: wall[0]),
            patch.object(loop, "time", side_effect=lambda: mono() + shift[0]),
            patch.object(self.services.memorylake_binding_repo, "owns_agent", side_effect=delayed),
            self.assertRaises(OctopError),
        ):
            async with self.scope():
                pass
