"""Dormant producer helper: real synthetic JWT, UserManager and SQLite identity.

No HTTP/WS route is registered or exercised. Tokens/secrets are synthetic; the
in-process consumer is a test harness, not production GlobalProcessor wiring.
"""

from __future__ import annotations

import asyncio
import inspect
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager, suppress
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
from octop_gateway.models import ImageContent, TextContent
from tests.support.memorylake import offline_test_guard

from octop.api.deps import sign_token
from octop.api.routers.chat.memorylake import dispatch_memorylake_dashboard_turn
from octop.api.routers.chat.models import ChatTurnBody
from octop.api.routers.chat.turn import PreparedDashboardTurn
from octop.infra.connectors.memorylake.delivery import DashboardDelivery
from octop.infra.connectors.memorylake.origin import DashboardOriginError, DashboardOriginRegistry
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.secrets import SecretRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.db.repos.users import UserRepo
from octop.infra.gateway.process.processor import GlobalProcessor
from octop.infra.gateway.slash.dispatcher import SlashDispatcher
from octop.infra.gateway.ws.memorylake import MemoryLakeWebSocketChannel
from octop.infra.gateway.ws.ws_hub import WebSocketHub
from octop.infra.users.manager import UserManager


class MemoryLakeProducerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.pool = SqlitePool(Path(root) / "producer.db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        self.users = UserRepo(self.pool)
        self.owner = self.users.create(username="owner", role="user")
        self.admin = self.users.create(username="admin", role="admin")
        self.agents = AgentRepo(self.pool)
        self.agents.create(agent_id="agent", user_id=self.owner, name="agent")
        self.secrets = SecretRepo(self.pool)
        self.secret = b"T" * 32
        self.assertEqual(len(self.secret), 32)
        self.secrets.get_or_create("jwt", lambda: self.secret)
        self.services = SimpleNamespace(
            user_repo=self.users,
            secret_repo=self.secrets,
            config=SimpleNamespace(login_max_attempts=5, login_lockout_seconds=60),
        )
        self.manager = UserManager(self.services)
        self.server = SimpleNamespace(services=self.services, user_manager=self.manager)
        self.session = f"agent:dashboard:{self.owner}:dm"
        ThreadRepo(self.pool).insert(
            thread_id="thread",
            agent_id="agent",
            user_id=self.owner,
            channel_type="dashboard",
            session_key=self.session,
        )
        self.prepared = PreparedDashboardTurn(
            thread_id="thread",
            session_key=self.session,
            mcp_servers=None,
            skills=None,
            model_ref=None,
            inbound_content=[TextContent(text="synthetic")],
            composer_context=None,
            inbound_attachments=[],
        )
        self.turn = ChatTurnBody(text="synthetic")
        self.now, self.wall = 100.0, time.time()
        self.registry = DashboardOriginRegistry(
            identities=MemoryLakeBindingRepo(self.pool),
            max_pending=1,
            clock=lambda: self.now,
            wall_clock=lambda: self.wall,
        )
        self.delivery = DashboardDelivery(self.registry)
        self.source_hub = WebSocketHub()
        self.messages, self.claims, self.errors, self.tasks = [], [], [], []
        self.claimed = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.addAsyncCleanup(self.cleanup)

    async def asyncSetUp(self):
        await self.manager.boot()
        self.token = self.sign()
        self.connection = self.source_hub.register_authenticated_chat(
            "synthetic-connection", AsyncMock(), user_id=self.owner, agent_id="agent"
        )

    async def cleanup(self):
        self.release.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.delivery.close()
        self.source_hub.close()
        await asyncio.gather(*tuple(self.registry._auth_jobs), return_exceptions=True)
        await self.manager.shutdown_all()

    def sign(self, user=None, *, key=None, ttl=60):
        user = self.owner if user is None else user
        return sign_token(
            self.secret if key is None else key,
            sub=user,
            uname="synthetic",
            role="user",
            ttl_seconds=ttl,
        )

    def enqueue(self, message):
        self.messages.append(message)

        async def consume():
            try:
                async with self.delivery.receive(message) as identity:
                    self.claims.append(identity)
                    self.claimed.set()
                    await self.release.wait()
            except ValueError as exc:
                self.errors.append(exc)

        self.tasks.append(asyncio.create_task(consume()))

    async def dispatch(self, **overrides):
        kwargs = {
            "server": self.server,
            "token": self.token,
            "agent_id": "agent",
            "prepared": self.prepared,
            "turn": self.turn,
            "connection": self.connection,
            "delivery": self.delivery,
            "enqueue": self.enqueue,
            "timeout_s": 1,
        }
        kwargs.update(overrides)
        return await dispatch_memorylake_dashboard_turn(**kwargs)

    async def drain(self):
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def test_valid_jwt_uses_authenticated_user_without_token_payload(self):
        receipt = await self.dispatch()
        await self.drain()
        self.assertEqual(receipt.status, "accepted")
        self.assertEqual(self.errors, [])
        self.assertEqual(self.claims[0].user_id, self.owner)
        self.assertEqual(self.messages[0].channel_subject.subject_id, str(self.owner))
        dumped = repr(self.messages[0].model_dump()) + repr(receipt)
        self.assertNotIn(self.token, dumped)
        self.assertNotIn(self.secret.decode(), dumped)
        for forbidden in ("user", "caller_user_id", "as_user"):
            self.assertNotIn(
                forbidden, inspect.signature(dispatch_memorylake_dashboard_turn).parameters
            )

    async def test_connection_must_be_current_chat_source_of_jwt_caller_and_agent(self):
        other = self.source_hub.register_authenticated_chat(
            "other", AsyncMock(), user_id=self.admin, agent_id="agent"
        )
        wrong_agent = self.source_hub.register_authenticated_chat(
            "wrong-agent", AsyncMock(), user_id=self.owner, agent_id="other-agent"
        )
        for connection in (other, wrong_agent, "synthetic-connection", None):
            with self.subTest(kind=type(connection).__name__), self.assertRaises(ValueError):
                await self.dispatch(connection=connection)
        self.source_hub.unregister("synthetic-connection")
        with self.assertRaises(ValueError):
            await self.dispatch()
        self.assertEqual(self.messages, [])

    async def test_connection_disconnected_during_auth_cannot_issue_origin(self):
        entered, release = threading.Event(), threading.Event()
        original = self.secrets.get

        def blocked(key):
            entered.set()
            release.wait(3)
            return original(key)

        with patch.object(self.secrets, "get", side_effect=blocked):
            sender = asyncio.create_task(self.dispatch())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                self.source_hub.unregister("synthetic-connection")
                release.set()
                with self.assertRaises(ValueError):
                    await sender
                self.assertEqual(self.messages, [])
            finally:
                release.set()
                await asyncio.gather(
                    sender, *tuple(self.registry._auth_jobs), return_exceptions=True
                )

    async def test_disconnect_and_same_id_reconnect_never_revives_active_identity(self):
        self.release.clear()
        await self.dispatch()
        await asyncio.wait_for(self.claimed.wait(), 2)
        identity = self.claims[0]
        self.source_hub.unregister("synthetic-connection")
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")
        self.source_hub.register_authenticated_chat(
            "synthetic-connection", AsyncMock(), user_id=self.owner, agent_id="agent"
        )
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")
        with self.assertRaises(ValueError):
            await self.dispatch()
        self.release.set()
        await self.drain()

    async def test_bad_signature_expired_missing_exp_and_invalid_sub_are_bodyfree(self):
        payload = {"sub": str(self.owner), "exp": int(time.time()) + 60}
        tokens = [
            self.sign(key=b"X" * 32),
            self.sign(ttl=-1),
            jwt.encode({"sub": str(self.owner)}, self.secret, algorithm="HS256"),
            jwt.encode(
                {**payload, "sub": "SYNTHETIC-PRIVATE-ACTOR"}, self.secret, algorithm="HS256"
            ),
            "not-a-signed-token",
        ]
        for token in tokens:
            with (
                self.subTest(token_kind=tokens.index(token)),
                self.assertRaises(ValueError) as caught,
            ):
                await self.dispatch(token=token)
            self.assertNotIn(token, str(caught.exception))
            self.assertNotIn("SYNTHETIC-PRIVATE-ACTOR", str(caught.exception))
        self.assertEqual(self.messages, [])

    async def test_signed_actor_hints_cannot_override_sub_or_owner(self):
        token = jwt.encode(
            {
                "sub": str(self.owner),
                "exp": int(time.time()) + 60,
                "actor_id": str(self.admin),
                "as_user": self.admin,
                "user": self.admin,
                "role": "admin",
            },
            self.secret,
            algorithm="HS256",
        )
        self.assertEqual((await self.dispatch(token=token)).status, "accepted")
        await self.drain()
        self.assertEqual(self.claims[0].user_id, self.owner)
        # Correctly signed other-user/admin tokens still cannot export owner data.
        with self.assertRaises(ValueError):
            await self.dispatch(token=self.sign(self.admin))
        self.assertEqual(len(self.messages), 1)

    async def test_disabled_database_user_rejected_even_if_cached_active(self):
        cached = self.manager.get_by_id(self.owner)
        self.assertIsNotNone(cached)
        self.users.set_disabled(self.owner, True)
        self.assertIs(self.manager.get_by_id(self.owner), cached)
        with self.assertRaises(ValueError):
            await self.dispatch()
        self.assertEqual(self.messages, [])

    async def test_shared_disabled_or_team_agent_rejected(self):
        for column, value in (("is_shared", 1), ("enabled", 0), ("kind", "team")):
            with self.subTest(column=column):
                with self.pool.transaction() as conn:
                    conn.execute(f"UPDATE agents SET {column}=? WHERE agent_id=?", (value, "agent"))
                try:
                    with self.assertRaises(ValueError):
                        await self.dispatch()
                finally:
                    with self.pool.transaction() as conn:
                        conn.execute(
                            "UPDATE agents SET is_shared=0,enabled=1,kind='expert' WHERE agent_id='agent'"
                        )
        self.assertEqual(self.messages, [])

    async def test_jwt_key_rotation_invalidates_next_dispatch(self):
        self.assertEqual((await self.dispatch()).status, "accepted")
        await self.drain()
        self.secrets.rotate("jwt", b"R" * 32)
        with self.assertRaises(ValueError):
            await self.dispatch()
        self.assertEqual(len(self.messages), 1)
        self.assertEqual((await self.dispatch(token=self.sign(key=b"R" * 32))).status, "accepted")
        await self.drain()

    async def test_text_only_turn_rejects_media_attachments_policy_peers_and_steer(self):
        changes = [
            {
                "prepared": replace(
                    self.prepared,
                    inbound_content=[ImageContent(url="https://invalid.example/synthetic.png")],
                )
            },
            {"prepared": replace(self.prepared, inbound_attachments=[{"id": "synthetic"}])},
            {"turn": ChatTurnBody(text="synthetic", hitl_policy={"mode": "allow_all"})},
            {"turn": ChatTurnBody(text="synthetic", target_agent_ids=["peer"])},
            {"turn": ChatTurnBody(text="synthetic", delivery="steer")},
            {"turn": ChatTurnBody(text="synthetic", conversation_mode="ask")},
            {"turn": ChatTurnBody(text="synthetic", conversation_mode="plan")},
            {
                "prepared": replace(
                    self.prepared, inbound_content=[TextContent(text="/STOP reason")]
                )
            },
        ]
        for change in changes:
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                await self.dispatch(**change)
        self.assertEqual(self.messages, [])

    async def test_jwt_budget_uses_clock_anchor_before_verification_wait(self):
        expiry = self.wall + 0.03

        def rolled_back_claims(*_):
            self.wall -= 100
            self.now += 0.06
            return {"sub": self.owner, "exp": expiry}

        with (
            patch(
                "octop.api.routers.chat.memorylake.time",
                SimpleNamespace(
                    time=lambda: self.wall,
                    monotonic=lambda: self.now,
                ),
                create=True,
            ),
            patch(
                "octop.api.routers.chat.memorylake.resolve_user_from_token",
                return_value=SimpleNamespace(id=self.owner),
            ),
            patch("octop.api.routers.chat.memorylake._decode", side_effect=rolled_back_claims),
            self.assertRaises(ValueError),
        ):
            await self.dispatch()
        await self.drain()
        self.assertEqual(self.messages, [])
        self.assertEqual(self.claims, [])

    async def test_consumed_identity_keeps_pre_auth_jwt_budget_after_clock_rollback(self):
        expiry = self.wall + 0.03
        self.release.clear()

        def rolled_back_claims(*_):
            self.wall -= 100
            self.now += 0.01
            return {"sub": self.owner, "exp": expiry}

        with (
            patch(
                "octop.api.routers.chat.memorylake.time",
                SimpleNamespace(
                    time=lambda: self.wall,
                    monotonic=lambda: self.now,
                ),
                create=True,
            ),
            patch(
                "octop.api.routers.chat.memorylake.resolve_user_from_token",
                return_value=SimpleNamespace(id=self.owner),
            ),
            patch("octop.api.routers.chat.memorylake._decode", side_effect=rolled_back_claims),
        ):
            self.assertEqual((await self.dispatch()).status, "accepted")
            await asyncio.wait_for(self.claimed.wait(), 2)
            self.now += 0.03
            with self.assertRaises(DashboardOriginError):
                self.claims[0].check_current(agent_id="agent", thread_id="thread")
        self.release.set()
        await self.drain()

    async def test_token_wall_expiry_caps_claim_even_with_unexpired_monotonic_ttl(self):
        self.release.clear()
        token = self.sign(ttl=5)
        expiry = jwt.decode(token, self.secret, algorithms=["HS256"])["exp"]
        self.assertEqual((await self.dispatch(token=token)).status, "accepted")
        await asyncio.wait_for(self.claimed.wait(), 2)
        self.assertEqual(len(self.claims), 1)
        self.claims[0].check_current(agent_id="agent", thread_id="thread")
        self.wall = expiry + 1
        self.assertEqual(self.now, 100.0)
        with self.assertRaises(DashboardOriginError):
            self.claims[0].check_current(agent_id="agent", thread_id="thread")
        self.release.set()
        await self.drain()

    async def test_wall_expiry_before_consume_never_enters_consumer(self):
        token = self.sign(ttl=5)
        expiry = jwt.decode(token, self.secret, algorithms=["HS256"])["exp"]

        def enqueue(message):
            self.wall = expiry + 1
            self.enqueue(message)

        with self.assertRaises(ValueError):
            await self.dispatch(token=token, enqueue=enqueue)
        await self.drain()
        self.assertEqual(self.claims, [])

    async def test_cancelled_auth_retains_quota_until_actual_executor_future_finishes(self):
        entered, release = threading.Event(), threading.Event()
        original = self.secrets.get
        futures = ()

        def blocked(key):
            if key == "jwt":
                entered.set()
                release.wait(3)
            return original(key)

        with patch.object(self.secrets, "get", side_effect=blocked):
            sender = asyncio.create_task(self.dispatch())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                futures = tuple(self.registry._auth_jobs)
                self.assertEqual(len(futures), 1)
                sender.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await sender
                self.assertFalse(futures[0].done())
                with self.assertRaises(ValueError):
                    await self.dispatch()
                self.assertEqual(self.messages, [])
            finally:
                release.set()
                sender.cancel()
                await asyncio.gather(sender, return_exceptions=True)
                await asyncio.gather(
                    *futures, *tuple(self.registry._auth_jobs), return_exceptions=True
                )
        self.assertTrue(all(future.done() for future in futures))
        self.assertEqual((await self.dispatch()).status, "accepted")
        await self.drain()

    async def test_auth_deadline_returns_before_blocked_executor_and_retains_quota(self):
        entered, release = threading.Event(), threading.Event()
        original = self.secrets.get
        futures = ()

        def blocked(key):
            if key == "jwt":
                entered.set()
                release.wait(3)
            return original(key)

        with patch.object(self.secrets, "get", side_effect=blocked):
            sender = asyncio.create_task(self.dispatch(timeout_s=0.03))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                futures = tuple(self.registry._auth_jobs)
                done, _ = await asyncio.wait({sender}, timeout=0.3)
                self.assertIn(sender, done, "auth verification must share the helper deadline")
                with self.assertRaises(ValueError) as caught:
                    await sender
                self.assertNotIn(self.token, str(caught.exception))
                self.assertEqual(self.messages, [])
                self.assertEqual(len(futures), 1)
                self.assertFalse(futures[0].done())
                with self.assertRaises(ValueError):
                    await self.dispatch(timeout_s=0.03)
            finally:
                release.set()
                sender.cancel()
                await asyncio.gather(sender, return_exceptions=True)
                await asyncio.gather(
                    *futures, *tuple(self.registry._auth_jobs), return_exceptions=True
                )
        self.assertTrue(all(future.done() for future in futures))

    async def test_auth_suspension_cannot_smuggle_mutated_media_or_hitl_policy(self):
        for mutation in ("media", "policy"):
            with self.subTest(mutation=mutation):
                prepared = replace(self.prepared, inbound_content=[TextContent(text="original")])
                turn = ChatTurnBody(text="original")
                entered, release = threading.Event(), threading.Event()
                original = self.secrets.get
                futures = ()
                self.messages.clear()

                def blocked(key, *, entered=entered, release=release, original=original):
                    if key == "jwt":
                        entered.set()
                        release.wait(3)
                    return original(key)

                with patch.object(self.secrets, "get", side_effect=blocked):
                    sender = asyncio.create_task(self.dispatch(prepared=prepared, turn=turn))
                    try:
                        self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                        futures = tuple(self.registry._auth_jobs)
                        if mutation == "media":
                            prepared.inbound_content[:] = [
                                ImageContent(url="https://invalid.example/never-fetch.png")
                            ]
                        else:
                            turn.hitl_policy = ChatTurnBody(
                                hitl_policy={"mode": "allow_all"}
                            ).hitl_policy
                        release.set()
                        # Rejection or the original validated snapshot are safe.
                        with suppress(ValueError):
                            await sender
                        await self.drain()
                        for message in self.messages:
                            self.assertTrue(
                                all(type(part) is TextContent for part in message.content)
                            )
                            self.assertNotIn("hitl_policy", message.metadata)
                    finally:
                        release.set()
                        sender.cancel()
                        await asyncio.gather(sender, return_exceptions=True)
                        await asyncio.gather(
                            *futures, *tuple(self.registry._auth_jobs), return_exceptions=True
                        )

    async def test_real_channel_handoff_does_not_bypass_global_processor_hard_gate(self):
        # JWT + SQLite + delivery + channel + processor are real. Only processor
        # dependencies and delivery notifications are spies; its methods are NOT
        # mocked. Accepted handoff must not be presented as graph execution.
        dependencies = [MagicMock() for _ in range(7)]
        manager, threads, audit, agents, users, connectors, trajectory = dependencies
        processor = GlobalProcessor(
            agent_manager=manager,
            thread_registry=threads,
            audit_repo=audit,
            agent_repo=agents,
            user_repo=users,
            connector_repo=connectors,
            dispatcher=SlashDispatcher(),
            trajectory_service=trajectory,
        )
        for dependency in dependencies:
            dependency.reset_mock()
        hub = MagicMock()
        hub.push = AsyncMock()
        hub.push_to_thread = AsyncMock()
        identities, scope_exits = [], []

        @contextmanager
        def scope(identity):
            identity.check_current(agent_id="agent", thread_id="thread")
            identities.append(identity)
            try:
                yield  # No tool injection or custom permission ContextVar.
            finally:
                with self.assertRaises(DashboardOriginError):
                    identity.check_current(agent_id="agent", thread_id="thread")
                scope_exits.append(identity)

        channel = MemoryLakeWebSocketChannel(
            processor,
            hub=hub,
            delivery=self.delivery,
            turn_scope=scope,
        )

        # No media backend or actual socket is installed.
        def enqueue(message):
            self.messages.append(message)
            self.tasks.append(asyncio.create_task(channel.handle_inbound(message)))

        with (
            patch(
                "octop.infra.metrics.METRICS.inc",
                side_effect=AssertionError("processor metrics forbidden"),
            ) as metrics,
            patch(
                "octop.infra.connectors.memorylake.runner.MemoryLakeRunner.run",
                new_callable=AsyncMock,
            ) as runner,
        ):
            receipt = await self.dispatch(enqueue=enqueue)
            await asyncio.gather(*self.tasks)
        self.assertEqual(receipt.status, "accepted")
        self.assertEqual(len(identities), 1)
        self.assertEqual(scope_exits, identities)
        for dependency in dependencies:
            self.assertEqual(dependency.mock_calls, [])
        metrics.assert_not_called()
        runner.assert_not_called()
        frames = [call.args[1] for call in hub.push_to_thread.await_args_list]
        self.assertEqual(frames[0]["type"], "turn.started")
        self.assertTrue(any(frame["type"] == "error" for frame in frames))
        self.assertEqual(frames[-1]["type"], "turn.ended")
        self.assertIs(frames[-1]["failed"], True)
        self.assertFalse(any(frame["type"] in ("token", "tool_result") for frame in frames))
        self.assertNotIn(self.token, repr(frames))
