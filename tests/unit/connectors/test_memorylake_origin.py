"""Local origin boundary and actual queue, not a wired dashboard auth endpoint."""

from __future__ import annotations

import asyncio
import copy
import gc
import json
import pickle
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from octop_gateway.manager import ChannelManager
from octop_gateway.models import ChannelSubject, InboundMessage, TextContent
from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.origin import (
    DashboardInbound,
    DashboardOriginError,
    DashboardOriginRegistry,
    DashboardTurnIdentity,
)
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.threads import ThreadRepo
from octop.infra.db.repos.users import UserRepo
from octop.infra.gateway.ws.ws_channel import WS_CHANNEL_ID, WebSocketChannel
from octop.infra.gateway.ws.ws_hub import WebSocketHub


class DashboardOriginTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.pool = SqlitePool(Path(root) / "origin.db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        users = UserRepo(self.pool)
        self.owner = users.create(username="owner", role="user")
        self.other = users.create(username="admin", role="admin")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="agent")
        self.session = f"agent:dashboard:{self.owner}:dm"
        ThreadRepo(self.pool).insert(
            thread_id="thread",
            agent_id="agent",
            user_id=self.owner,
            channel_type="dashboard",
            session_key=self.session,
        )
        self.repo = MemoryLakeBindingRepo(self.pool)
        self.now = 100.0
        self.registry = DashboardOriginRegistry(
            identities=self.repo,
            ttl_s=60,
            max_pending=2,
            clock=lambda: self.now,
        )

    def message(self):
        return DashboardInbound(
            channel_id=WS_CHANNEL_ID,
            channel_type="dashboard",
            tenant_id="agent",
            channel_subject=ChannelSubject(subject_id=str(self.owner)),
            channel_session_id=self.session,
            content=[TextContent(text="synthetic")],
            metadata={"thread_id": "thread", "session_key": self.session},
        )

    async def issue(self, message=None, **overrides):
        message = message or self.message()
        kwargs = {"caller_user_id": self.owner, "agent_id": "agent", "thread_id": "thread"}
        kwargs.update(overrides)
        await self.registry.issue(message, **kwargs)
        return message

    def sql(self, sql, parameters=()):
        with self.pool.transaction() as conn:
            conn.execute(sql, parameters)

    async def test_unissued_and_metadata_forgery_fail(self):
        message = self.message()
        message.metadata["origin"] = {"user_id": self.owner, "authenticated": True}
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(InboundMessage.model_validate(message.model_dump()))

    def test_direct_construction_is_not_registry_minted_identity(self):
        identity = DashboardTurnIdentity(self.owner, "agent", "thread", 160, lambda: self.now)
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")

    async def test_verified_expiry_caps_monotonic_lifetime_even_if_wall_clock_moves_back(self):
        wall = [1000.0]
        registry = DashboardOriginRegistry(
            identities=self.repo, clock=lambda: self.now, wall_clock=lambda: wall[0]
        )
        message = self.message()
        await registry.issue(
            message,
            caller_user_id=self.owner,
            agent_id="agent",
            thread_id="thread",
            not_after_epoch=1002,
        )
        identity = await registry.consume(message)
        wall[0] = 900
        self.now = 103
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")

    async def test_wall_expiry_rejects_claimed_and_pending_origins_before_monotonic_deadline(self):
        wall = [1000.0]
        registry = DashboardOriginRegistry(
            identities=self.repo, clock=lambda: self.now, wall_clock=lambda: wall[0]
        )
        first, second = self.message(), self.message()
        for message in (first, second):
            await registry.issue(
                message,
                caller_user_id=self.owner,
                agent_id="agent",
                thread_id="thread",
                not_after_epoch=1010,
            )
        identity = await registry.consume(first)
        wall[0] = 1011
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")
        with self.assertRaises(DashboardOriginError):
            await registry.consume(second)

    async def test_invalid_or_expired_auth_deadline_is_rejected_before_sql(self):
        registry = DashboardOriginRegistry(
            identities=self.repo, clock=lambda: self.now, wall_clock=lambda: 1000.0
        )
        with patch.object(self.repo, "owns_dashboard_thread") as sql:
            for deadline in (True, "1001", float("nan"), float("inf"), 999, 1000):
                with self.subTest(deadline=deadline), self.assertRaises(DashboardOriginError):
                    await registry.issue(
                        self.message(),
                        caller_user_id=self.owner,
                        agent_id="agent",
                        thread_id="thread",
                        not_after_epoch=deadline,
                    )
            sql.assert_not_called()

    async def test_long_auth_expiry_cannot_extend_registry_ttl(self):
        registry = DashboardOriginRegistry(
            identities=self.repo, clock=lambda: self.now, wall_clock=lambda: 1000.0
        )
        message = self.message()
        await registry.issue(
            message,
            caller_user_id=self.owner,
            agent_id="agent",
            thread_id="thread",
            not_after_epoch=2000,
        )
        identity = await registry.consume(message)
        self.now = 161
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")

    async def test_observed_wall_expiry_cannot_revive_after_clock_rollback(self):
        wall = [1000.0]
        registry = DashboardOriginRegistry(
            identities=self.repo, clock=lambda: self.now, wall_clock=lambda: wall[0]
        )
        message = self.message()
        await registry.issue(
            message, caller_user_id=1, agent_id="agent", thread_id="thread", not_after_epoch=1010.0
        )
        identity = await registry.consume(message)
        wall[0] = 1011.0
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")
        wall[0] = 1001.0
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")

    async def test_revoked_source_cannot_mint_after_issue_or_consume_sql(self):
        for phase in ("issue", "consume"):
            with self.subTest(phase=phase):
                active = [True]
                entered, release = threading.Event(), threading.Event()
                original = self.repo.owns_dashboard_thread

                def check(active=active):
                    if not active[0]:
                        raise ValueError("synthetic source revoked")

                def blocked(*, entered=entered, release=release, original=original, **kwargs):
                    entered.set()
                    release.wait(3)
                    return original(**kwargs)

                message = self.message()
                kwargs = {
                    "caller_user_id": self.owner,
                    "agent_id": "agent",
                    "thread_id": "thread",
                    "source_check": check,
                }
                if phase == "consume":
                    await self.registry.issue(message, **kwargs)
                with patch.object(self.repo, "owns_dashboard_thread", side_effect=blocked):
                    task = asyncio.create_task(
                        self.registry.issue(message, **kwargs)
                        if phase == "issue"
                        else self.registry.consume(message)
                    )
                    try:
                        self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                        active[0] = False
                        release.set()
                        with self.assertRaises(DashboardOriginError):
                            await task
                    finally:
                        release.set()
                        await asyncio.gather(
                            task, *tuple(self.registry._auth_jobs), return_exceptions=True
                        )

    async def test_source_revocation_closes_claimed_identity_permanently(self):
        active = [True]

        def check():
            if not active[0]:
                raise ValueError("synthetic source revoked")

        message = self.message()
        await self.registry.issue(
            message,
            caller_user_id=self.owner,
            agent_id="agent",
            thread_id="thread",
            source_check=check,
        )
        identity = await self.registry.consume(message)
        active[0] = False
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")
        active[0] = True
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")

    async def test_issue_consume_exactly_once_and_close_identity(self):
        message = await self.issue()
        identity = await self.registry.consume(message)
        self.assertEqual(
            (identity.user_id, identity.agent_id, identity.thread_id),
            (self.owner, "agent", "thread"),
        )
        identity.check_current(agent_id="agent", thread_id="thread")
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)
        identity.close()
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")

    async def test_copy_or_json_roundtrip_cannot_steal_original_origin(self):
        message = await self.issue()
        for copied in (
            copy.copy(message),
            copy.deepcopy(message),
            DashboardInbound.model_validate_json(message.model_dump_json()),
        ):
            with (
                self.subTest(copy_type=type(copied).__name__),
                self.assertRaises(DashboardOriginError),
            ):
                await self.registry.consume(copied)
        self.assertNotIn("_origin", message.model_dump_json())
        # Failed forged/copy attempts must not consume the genuine message.
        identity = await self.registry.consume(message)
        identity.close()

    async def test_other_registry_cannot_consume_origin(self):
        message = await self.issue()
        other = DashboardOriginRegistry(identities=self.repo, clock=lambda: self.now)
        with self.assertRaises(DashboardOriginError):
            await other.consume(message)
        (await self.registry.consume(message)).close()

    async def test_authenticated_caller_must_match_current_owner_and_thread(self):
        with self.assertRaises(DashboardOriginError):
            await self.issue(caller_user_id=self.other)
        self.sql("UPDATE agents SET is_shared=1")
        with self.assertRaises(DashboardOriginError):
            await self.issue()

    async def test_queued_revocation_consumes_without_grant(self):
        message = await self.issue()
        self.sql("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)
        self.sql("UPDATE users SET disabled=0 WHERE id=?", (self.owner,))
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)

    async def test_tampered_identity_fields_fail(self):
        for field, value in (
            ("tenant_id", "other"),
            ("channel_type", "telegram"),
            ("channel_id", "bot"),
            ("channel_session_id", "group"),
        ):
            message = await self.issue()
            setattr(message, field, value)
            with self.subTest(field=field), self.assertRaises(DashboardOriginError):
                await self.registry.consume(message)
        message = await self.issue()
        message.metadata["thread_id"] = "other"
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)

    async def test_group_or_non_dashboard_message_is_never_issued(self):
        for field, value in (
            ("channel_type", "telegram"),
            ("channel_id", "bot"),
            ("channel_session_id", "agent:dashboard:1:group"),
        ):
            message = self.message()
            setattr(message, field, value)
            with self.subTest(field=field), self.assertRaises(DashboardOriginError):
                await self.issue(message)
        message = self.message()
        message.channel_subject.chat_type = "group"
        with self.assertRaises(DashboardOriginError):
            await self.issue(message)

    async def test_peer_impersonation_and_unapproved_modes_are_not_issued(self):
        for key, value in (
            ("as_user", str(self.owner)),
            ("target_agent_ids", ["agent"]),
            ("conversation_mode", "ask"),
            ("conversation_mode", "plan"),
            ("conversation_mode", "resume"),
        ):
            message = self.message()
            message.metadata[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(DashboardOriginError):
                await self.issue(message)

    async def test_metadata_group_signals_reject_issuance_and_later_tampering(self):
        for key, value in (("chat_type", "group"), ("is_group", True), ("is_group", "yes")):
            with self.subTest(key=key, value=value):
                message = self.message()
                message.metadata[key] = value
                try:
                    with self.assertRaises(DashboardOriginError):
                        await self.issue(message)
                finally:
                    self.registry.revoke(message)
                message = await self.issue()
                message.metadata[key] = value
                with self.assertRaises(DashboardOriginError):
                    await self.registry.consume(message)

    async def test_cancel_revoke_and_expiry_do_not_release_running_auth_job_capacity(self):
        original = self.repo.owns_dashboard_thread
        for action in ("cancel", "revoke", "expiry", "cancel_consume"):
            with self.subTest(action=action):
                registry = DashboardOriginRegistry(
                    identities=self.repo, max_pending=1, clock=lambda: self.now
                )
                entered, release, finished = threading.Event(), threading.Event(), threading.Event()
                calls, completed = [], []
                message = self.message()
                if action == "cancel_consume":
                    await registry.issue(
                        message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
                    )

                def blocked(
                    calls=calls,
                    entered=entered,
                    release=release,
                    completed=completed,
                    finished=finished,
                    **kwargs,
                ):
                    calls.append(1)
                    entered.set()
                    try:
                        release.wait(2)
                        return original(**kwargs)
                    finally:
                        completed.append(1)
                        if len(completed) == len(calls):
                            finished.set()

                with patch.object(self.repo, "owns_dashboard_thread", side_effect=blocked):
                    task = asyncio.create_task(
                        registry.consume(message)
                        if action == "cancel_consume"
                        else registry.issue(
                            message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
                        )
                    )
                    self.assertTrue(
                        await asyncio.get_running_loop().run_in_executor(None, entered.wait, 2)
                    )
                    try:
                        if action in ("cancel", "cancel_consume"):
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        elif action == "revoke":
                            registry.revoke(message)
                        else:
                            self.now += 61
                        with self.assertRaises(DashboardOriginError):
                            await asyncio.wait_for(
                                registry.issue(
                                    self.message(),
                                    caller_user_id=self.owner,
                                    agent_id="agent",
                                    thread_id="thread",
                                ),
                                0.2,
                            )
                        self.assertEqual(len(calls), 1)
                    finally:
                        release.set()
                        await asyncio.gather(task, return_exceptions=True)
                        self.assertTrue(
                            await asyncio.get_running_loop().run_in_executor(None, finished.wait, 2)
                        )
                        await asyncio.gather(*registry._auth_jobs, return_exceptions=True)
                    fresh = self.message()
                    await registry.issue(
                        fresh, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
                    )
                    (await registry.consume(fresh)).close()

    async def test_completed_auth_future_releases_quota_before_delayed_done_callback(self):
        registry = DashboardOriginRegistry(
            identities=self.repo, max_pending=1, clock=lambda: self.now
        )
        loop = asyncio.get_running_loop()

        def completed(executor, function, *args):
            # A very fast job can already be done when its callback is added.
            # Awaiting shield(done_future) does not yield to that callback.
            future = loop.create_future()
            future.set_result(True)
            return future

        message = self.message()
        with patch.object(loop, "run_in_executor", side_effect=completed):
            await registry.issue(
                message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
            )
            self.assertTrue(all(future.done() for future in registry._auth_jobs))
            (await registry.consume(message)).close()
        await asyncio.sleep(0)
        self.assertEqual(registry._auth_jobs, set())

    async def test_late_sql_exception_after_cancellation_is_retrieved_without_logging(self):
        loop = asyncio.get_running_loop()
        reports = []
        original_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: reports.append(context))
        entered, release = threading.Event(), threading.Event()
        completed = asyncio.Event()

        def fail_late(**kwargs):
            entered.set()
            release.wait(2)
            raise RuntimeError("SYNTHETIC-LATE-DB-ERROR")

        try:
            with patch.object(self.repo, "owns_dashboard_thread", side_effect=fail_late):
                task = asyncio.create_task(self.issue())
                self.assertTrue(await loop.run_in_executor(None, entered.wait, 2))
                future = next(iter(self.registry._auth_jobs))
                future.add_done_callback(lambda _future: completed.set())
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                release.set()
                await asyncio.wait_for(completed.wait(), 2)
                self.assertEqual(self.registry._auth_jobs, set())
                # Do not await/inspect the failed Future here: that would mask
                # whether the production callback retrieved its late exception.
                del task, future
                gc.collect()
                await asyncio.sleep(0)
                self.assertEqual(reports, [])
        finally:
            release.set()
            await asyncio.gather(*self.registry._auth_jobs, return_exceptions=True)
            loop.set_exception_handler(original_handler)

    async def test_invalid_metadata_or_subject_type_is_body_free(self):
        for field, value in (("metadata", None), ("channel_subject", "SENSITIVE-FAKE")):
            message = self.message()
            setattr(message, field, value)
            with self.subTest(field=field), self.assertRaises(DashboardOriginError) as caught:
                await self.issue(message)
            self.assertNotIn("SENSITIVE-FAKE", str(caught.exception))

    async def test_identity_mutation_during_issuer_sql_fails(self):
        message = self.message()
        original = self.repo.owns_dashboard_thread

        def mutate(**kwargs):
            valid = original(**kwargs)
            message.metadata["thread_id"] = "other"
            return valid

        with (
            patch.object(self.repo, "owns_dashboard_thread", side_effect=mutate),
            self.assertRaises(DashboardOriginError),
        ):
            await self.issue(message)
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)

    async def test_origin_queries_do_not_block_event_loop(self):
        seen = []
        original = self.repo.owns_dashboard_thread

        def observe(**kwargs):
            seen.append(threading.get_ident())
            return original(**kwargs)

        with patch.object(self.repo, "owns_dashboard_thread", side_effect=observe):
            identity = await self.registry.consume(await self.issue())
            identity.close()
        self.assertEqual(len(seen), 2)
        self.assertNotIn(threading.get_ident(), seen)

    async def test_expired_pending_origin_and_active_identity_fail(self):
        message = await self.issue()
        self.now = 160
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)
        message = await self.issue()
        identity = await self.registry.consume(message)
        self.now = 220
        with self.assertRaises(DashboardOriginError):
            identity.check_current(agent_id="agent", thread_id="thread")

    async def test_expiry_during_auth_query_does_not_extend_deadline(self):
        message = await self.issue()
        original = self.repo.owns_dashboard_thread

        def expired(**kwargs):
            valid = original(**kwargs)
            self.now = 161
            return valid

        with (
            patch.object(self.repo, "owns_dashboard_thread", side_effect=expired),
            self.assertRaises(DashboardOriginError),
        ):
            await self.registry.consume(message)

    async def test_cancelled_redemption_cannot_be_replayed(self):
        message = await self.issue()
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        original = self.repo.owns_dashboard_thread

        def blocked(**kwargs):
            try:
                entered.set()
                release.wait(2)
                return original(**kwargs)
            finally:
                finished.set()

        with patch.object(self.repo, "owns_dashboard_thread", side_effect=blocked):
            task = asyncio.create_task(self.registry.consume(message))
            await asyncio.get_running_loop().run_in_executor(None, entered.wait, 2)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(
                await asyncio.get_running_loop().run_in_executor(None, finished.wait, 2)
            )
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(message)

    async def test_concurrent_consume_has_one_winner(self):
        message = await self.issue()
        outcomes = await asyncio.gather(
            self.registry.consume(message), self.registry.consume(message), return_exceptions=True
        )
        identities = [value for value in outcomes if not isinstance(value, BaseException)]
        self.assertEqual(len(identities), 1)
        self.assertEqual(sum(isinstance(value, DashboardOriginError) for value in outcomes), 1)
        identities[0].close()

    async def test_revoke_during_issuance_cannot_be_overwritten_by_late_auth(self):
        await self._issuing_interleaving("revoke")

    async def test_premature_consume_invalidates_inflight_issuance(self):
        await self._issuing_interleaving("consume")

    async def test_inflight_issuance_counts_against_capacity(self):
        await self._issuing_interleaving("capacity")

    async def test_revoke_during_redemption_prevents_late_identity(self):
        await self._issuing_interleaving("revoke_consume")

    async def test_cancelled_issuance_burns_origin_and_releases_capacity(self):
        await self._issuing_interleaving("cancel_issue")

    async def _issuing_interleaving(self, action):
        registry = DashboardOriginRegistry(
            identities=self.repo, max_pending=1, clock=lambda: self.now
        )
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        original = self.repo.owns_dashboard_thread
        message = self.message()
        if action == "revoke_consume":
            await registry.issue(
                message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
            )

        def blocked(**kwargs):
            try:
                entered.set()
                release.wait(2)
                return original(**kwargs)
            finally:
                finished.set()

        with patch.object(self.repo, "owns_dashboard_thread", side_effect=blocked):
            task = asyncio.create_task(
                registry.consume(message)
                if action == "revoke_consume"
                else registry.issue(
                    message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
                )
            )
            self.assertTrue(await asyncio.get_running_loop().run_in_executor(None, entered.wait, 2))
            try:
                if action in ("revoke", "revoke_consume"):
                    registry.revoke(message)
                elif action == "cancel_issue":
                    task.cancel()
                elif action == "consume":
                    with self.assertRaises(DashboardOriginError):
                        await registry.consume(message)
                else:
                    with self.assertRaises(DashboardOriginError):
                        await asyncio.wait_for(
                            registry.issue(
                                self.message(),
                                caller_user_id=self.owner,
                                agent_id="agent",
                                thread_id="thread",
                            ),
                            0.2,
                        )
            finally:
                release.set()
                if action == "capacity":
                    await task
                    registry.revoke(message)
                elif action == "cancel_issue":
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                else:
                    with self.assertRaises(DashboardOriginError):
                        await task
                self.assertTrue(
                    await asyncio.get_running_loop().run_in_executor(None, finished.wait, 2)
                )
                # A worker's Event can precede its Future's completion callback.
                await asyncio.gather(*registry._auth_jobs, return_exceptions=True)
        if action == "cancel_issue":
            with self.assertRaises(DashboardOriginError):
                await registry.issue(
                    message, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
                )
            fresh = self.message()
            await registry.issue(
                fresh, caller_user_id=self.owner, agent_id="agent", thread_id="thread"
            )
            (await registry.consume(fresh)).close()

    async def test_bounded_pending_registry_expiration_and_explicit_revoke(self):
        first, second = await self.issue(), await self.issue()
        with self.assertRaises(DashboardOriginError):
            await self.issue()
        self.registry.revoke(first)
        with self.assertRaises(DashboardOriginError):
            await self.registry.consume(first)
        third = await self.issue()
        self.now = 161
        await self.issue()
        for old in (second, third):
            with self.assertRaises(DashboardOriginError):
                await self.registry.consume(old)

    async def test_identity_not_copyable_serializable_or_valid_for_other_graph(self):
        identity = await self.registry.consume(await self.issue())
        for operation in (copy.copy, copy.deepcopy, pickle.dumps, json.dumps):
            with self.subTest(operation=operation.__name__), self.assertRaises(TypeError):
                operation(identity)
        for agent, thread in (("other", "thread"), ("agent", "other")):
            with self.assertRaises(DashboardOriginError):
                identity.check_current(agent_id=agent, thread_id=thread)
        identity.close()

    async def test_closed_identity_in_inherited_child_task_is_invalid(self):
        identity = await self.registry.consume(await self.issue())
        release = asyncio.Event()

        async def child():
            await release.wait()
            identity.check_current(agent_id="agent", thread_id="thread")

        task = asyncio.create_task(child())
        identity.close()
        release.set()
        with self.assertRaises(DashboardOriginError):
            await task

    async def test_actual_manager_queue_and_websocket_parser_preserve_instance(self):
        # Real enqueue callback/queue/parser, no real WS connection or processor.
        channel = WebSocketChannel(object(), hub=WebSocketHub())
        manager = ChannelManager(
            {WS_CHANNEL_ID: channel},
            workers_per_channel=0,
            media_backend=object(),
            interrupt_message_predicate=lambda message: False,
        )
        await manager.start()
        consumer = None
        try:

            async def existing_consumer():
                queued = await manager._queues[WS_CHANNEL_ID].get()
                parsed = channel.parse_inbound(queued)
                return parsed, await self.registry.consume(parsed)

            consumer = asyncio.create_task(existing_consumer())
            message = await self.issue()
            manager.enqueue(WS_CHANNEL_ID, message)
            parsed, identity = await asyncio.wait_for(consumer, 2)
            self.assertIs(parsed, message)
            identity.check_current(agent_id="agent", thread_id="thread")
            identity.close()
        finally:
            if consumer is not None:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
            await manager.stop()

    def test_bad_configuration_rejected(self):
        for kwargs in (
            {"ttl_s": 0},
            {"ttl_s": float("inf")},
            {"ttl_s": True},
            {"max_pending": 0},
            {"max_pending": True},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                DashboardOriginRegistry(identities=self.repo, **kwargs)
