"""Real SQLite idempotent consent/receipt reads; synthetic transport only."""

import asyncio
import threading
import time
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from tests.unit.connectors import test_memorylake_file_binding as fixtures

from octop.config import MemoryLakeCliConfig
from octop.infra.connectors.memorylake.application import MemoryLakeApplication
from octop.infra.connectors.memorylake.submissions import Approval, SubmissionError
from octop.infra.db.repos.connectors import ConnectorRepo


class FileIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.FileBindingTests.setUp
    asyncSetUp = fixtures.FileBindingTests.asyncSetUp
    create = fixtures.FileBindingTests.create
    transport_factory = fixtures.FileBindingTests.transport_factory
    approve = fixtures.FileBindingTests.approve

    async def candidate(self, binding):
        with self.runtime.operation():
            attachment = await binding.source.add(self.caller, "synthetic.txt", b"abc")
        return Approval("approval", await binding.prepare(attachment), time.time() + 60)

    async def test_exact_retry_concurrent_creation_and_mismatch(self):
        binding = await self.create()
        approval = await self.candidate(binding)
        barrier = threading.Barrier(2)
        original = self.repo.create_approval

        def concurrent_create(**kwargs):
            barrier.wait(timeout=3)
            return original(**kwargs)

        with patch.object(self.repo, "create_approval", side_effect=concurrent_create) as create:
            await asyncio.gather(*(binding.create_approval_once(approval) for _ in range(2)))
            self.assertEqual(create.call_count, 2)
        with self.pool.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM memorylake_submission_approvals").fetchone()[0],
                1,
            )
        await binding.create_approval_once(approval)
        self.assertEqual(await binding._ledger.approval("approval"), approval)
        for changed in (
            replace(approval, expires_at=approval.expires_at + 1),
            replace(approval, intent=replace(approval.intent, filename="other.txt")),
            replace(approval, revoked=True),
        ):
            with self.assertRaises(ValueError):
                await binding.create_approval_once(changed)
        self.repo.revoke_approval("approval", approver_user_id=self.owner)
        with self.assertRaises(ValueError):
            await binding.create_approval_once(approval)
        self.assertEqual(self.calls, [])

    async def test_receipt_read_after_rotation_optout_but_not_owner_revocation(self):
        binding = await self.create()
        attachment = await self.approve(binding)
        record = await binding.submit(attachment, "approval")
        ConnectorRepo(self.pool).upsert_credentials(instance_id="connector-a", blob=b"rotated")
        self.repo.replace_policy(
            binding_id="binding-a",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=False,
        )
        self.assertEqual(await binding.existing_submission("approval"), record)
        with self.pool.connect() as conn:
            conn.execute("UPDATE users SET disabled=1 WHERE username='owner'")
            conn.commit()
        with self.assertRaises(ValueError):
            await binding.existing_submission("approval")
        self.assertEqual(self.calls, [("upload", "connector-a"), ("import", "connector-a")])

    async def test_absence_is_not_database_failure_or_corruption(self):
        binding = await self.create()
        self.assertIsNone(await binding.existing_submission("missing"))
        with (
            patch.object(self.repo, "get", side_effect=RuntimeError("synthetic DB body")),
            self.assertRaisesRegex(SubmissionError, "^submission_database_unavailable$"),
        ):
            await binding.existing_submission("missing")
        approval = await self.candidate(binding)
        with (
            patch.object(self.repo, "get_approval", side_effect=RuntimeError("synthetic DB body")),
            patch.object(self.repo, "create_approval") as create,
        ):
            with self.assertRaises(ValueError):
                await binding.create_approval_once(approval)
            create.assert_not_called()
        await binding.create_approval_once(approval)
        await binding.submit(approval.intent.attachment_id, approval.approval_id)
        with self.pool.connect() as conn:
            conn.execute("UPDATE memorylake_submission_approvals SET intent_json='{}'")
            conn.commit()
        with self.assertRaises(ValueError):
            await binding.existing_submission("approval")
        with self.assertRaises(ValueError):
            await binding.create_approval_once(approval)

    async def test_cancelled_creation_settles_without_retrying_consent(self):
        binding = await self.create()
        approval = await self.candidate(binding)
        entered, release = threading.Event(), threading.Event()
        original = self.repo.create_approval

        def blocked(**kwargs):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("test release missing")
            return original(**kwargs)

        with patch.object(self.repo, "create_approval", side_effect=blocked) as create:
            task = asyncio.create_task(binding.create_approval_once(approval))
            try:
                async with asyncio.timeout(2):
                    while not entered.is_set():
                        await asyncio.sleep(0.001)
                task.cancel()
                await asyncio.sleep(0)
                self.assertFalse(task.done())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(create.call_count, 1)
        self.assertEqual(await binding._ledger.approval("approval"), approval)

    async def test_application_closes_reviews_without_revoking_persisted_consent(self):
        binding = await self.create()
        approval = await self.candidate(binding)
        await binding.create_approval_once(approval)
        server = SimpleNamespace(
            services=SimpleNamespace(
                config=SimpleNamespace(memorylake_cli=MemoryLakeCliConfig(enabled=True))
            )
        )
        app = MemoryLakeApplication(server)
        self.addAsyncCleanup(app.runtime.aclose)
        review = app.reviews.create("binding-a", approval.intent)
        app.close()
        with self.assertRaises(ValueError):
            app.reviews.create("binding-a", review.intent)
        self.assertEqual(await binding._ledger.approval("approval"), approval)
