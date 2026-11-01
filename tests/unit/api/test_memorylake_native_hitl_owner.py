"""Actual HITL route must authorize persisted thread before any resume effects."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import Request
from tests.unit.connectors import test_memorylake_middleware as fixture

from octop.api.routers.chat.models import HitlResumeBody
from octop.api.routers.chat.routes import resume_hitl
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.errors import OctopError
from octop.infra.gateway.hitl.coordinator import HitlChannelCoordinator


class NativeHitlOwnerTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixture.MemoryLakeMiddlewareTests.setUp
    seed = fixture.MemoryLakeMiddlewareTests.seed

    async def asyncSetUp(self):
        await fixture.MemoryLakeMiddlewareTests.asyncSetUp(self)
        self.hitl = HitlChannelCoordinator()
        self.resumes = []

        async def stream(**kwargs):
            self.resumes.append(kwargs)
            yield {"type": "token", "content": "synthetic private checkpoint"}

        self.processor = SimpleNamespace(hitl_coordinator=self.hitl, iter_hitl_resume_chunks=stream)
        threads = ThreadRepo(self.pool)
        self.server = SimpleNamespace(
            app_runtime=SimpleNamespace(
                agent_registry=SimpleNamespace(get_row=AgentRepo(self.pool).get),
                gateway=SimpleNamespace(
                    processor=self.processor,
                    thread_registry=SimpleNamespace(get_thread=threads.get),
                ),
            )
        )
        self.user = SimpleNamespace(id=self.first["user"], is_admin=False)
        self.request = Request({"type": "http", "headers": [], "path": "/", "method": "POST"})
        self.request.is_disconnected = AsyncMock(return_value=False)
        with self.pool.transaction() as conn:
            conn.execute("UPDATE agents SET is_shared=1 WHERE agent_id='first'")
        threads.insert(
            thread_id="other-user",
            agent_id="first",
            user_id=self.second["user"],
            channel_type="dashboard",
            session_key="other-user-session",
        )

    def pending(self, thread_id):
        return self.hitl.store.register(
            thread_id=thread_id,
            agent_id="first",
            user_id=self.user.id,
            session_key="synthetic-session",
            channel_type="dashboard",
            action_requests=[{"name": "native_echo", "args": {}}],
            review_configs=None,
        )

    async def test_foreign_or_missing_thread_denied_even_with_matching_pending(self):
        for thread_id in ("other-user", "second", "missing"):
            for has_pending in (False, True):
                with self.subTest(thread=thread_id, pending=has_pending):
                    pending = self.pending(thread_id) if has_pending else None
                    body = HitlResumeBody(
                        thread_id=thread_id,
                        decisions=[{"type": "approve"}],
                        pending_id=pending.pending_id if pending else None,
                    )
                    with self.assertRaises(OctopError):
                        await resume_hitl("first", body, self.request, self.user, self.server)
                    if pending:
                        self.assertEqual(pending.status, "pending")
        self.assertEqual(self.resumes, [])

    async def test_owner_native_resume_without_and_with_pending_still_runs(self):
        for has_pending in (False, True):
            with self.subTest(pending=has_pending):
                pending = self.pending("first") if has_pending else None
                response = await resume_hitl(
                    "first",
                    HitlResumeBody(
                        thread_id="first",
                        decisions=[{"type": "approve"}],
                        pending_id=pending.pending_id if pending else None,
                    ),
                    self.request,
                    self.user,
                    self.server,
                )
                frames = [frame async for frame in response.body_iterator]
                self.assertIn("synthetic private checkpoint", "".join(frames))
        self.assertEqual(len(self.resumes), 2)
