"""Global capture budget and local-only release, synthetic memory/SQLite only."""

import asyncio
import unittest
from dataclasses import replace

from tests.support.memorylake import offline_test_guard
from tests.unit.connectors import test_memorylake_file_binding as fixture

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime


class CaptureBudgetTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def test_default_cross_owner_named_capture_and_shared_reservation_bound(self):
        async def authorize(caller):
            return None

        runtime = MemoryLakeFileRuntime(authorize_factory=lambda admission: authorize)
        self.addAsyncCleanup(runtime.aclose)
        a = CallerContext(1, "agent-a", 1, 1)
        b = CallerContext(2, "agent-b", 2, 1)
        av = runtime.source.bind(binding_id="a", connector_id="ca", caller=a, authorize=authorize)
        bv = runtime.source.bind(binding_id="b", connector_id="cb", caller=b, authorize=authorize)

        async def five_mb(maximum):
            self.assertEqual(maximum, 20 * 1024 * 1024)
            return "synthetic.txt", b"x" * (5 * 1024 * 1024)

        async def one_byte(maximum):
            return "tiny.txt", b"x"

        await av.capture_named(a, five_mb)
        await bv.capture_named(b, one_byte)
        self.assertEqual(runtime.source._bytes, 5 * 1024 * 1024 + 1)
        self.assertEqual(FileRuntimeLimits().max_total_bytes, 128 * 1024 * 1024)
        entered, release = asyncio.Event(), asyncio.Event()
        count = 0

        async def blocked(maximum):
            nonlocal count
            count += 1
            if count == 2:
                entered.set()
            await release.wait()
            return "pending.txt", b"x"

        jobs = [
            asyncio.create_task(av.capture_named(a, blocked)),
            asyncio.create_task(bv.capture_named(b, blocked)),
        ]
        try:
            await asyncio.wait_for(entered.wait(), 2)
            self.assertEqual(runtime.source._pending_bytes, 2 * (3 * 20 * 1024 * 1024 + 65536))
            self.assertLessEqual(
                runtime.source._bytes + runtime.source._pending_bytes,
                FileRuntimeLimits().max_total_bytes,
            )
            with self.assertRaisesRegex(ValueError, "upload_capacity_exceeded"):
                await bv.capture_named(b, one_byte)
        finally:
            release.set()
            await asyncio.gather(*jobs)
        self.assertEqual(runtime.source._pending_bytes, 0)

    async def test_exact_total_boundary_keeps_named_reservation(self):
        async def authorize(caller):
            return None

        limits = FileRuntimeLimits(max_file_bytes=1, max_total_bytes=65540)
        runtime = MemoryLakeFileRuntime(
            authorize_factory=lambda admission: authorize, limits=limits
        )
        self.addAsyncCleanup(runtime.aclose)
        caller = CallerContext(1, "agent", 1, 1)
        view = runtime.source.bind(
            binding_id="binding", connector_id="connector", caller=caller, authorize=authorize
        )
        await view.add(caller, "stored.txt", b"x")
        entered, release = asyncio.Event(), asyncio.Event()

        async def blocked(maximum):
            entered.set()
            await release.wait()
            return "captured.txt", b"x"

        task = asyncio.create_task(view.capture_named(caller, blocked))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            self.assertEqual(runtime.source._bytes + runtime.source._pending_bytes, 65540)
            with self.assertRaisesRegex(ValueError, "upload_capacity_exceeded"):
                await view.add(caller, "extra.txt", b"x")
        finally:
            release.set()
            await task
        self.assertEqual(runtime.source._bytes, 2)
        self.assertEqual(runtime.source._pending_bytes, 0)


class LocalReleaseTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixture.FileBindingTests.setUp
    asyncSetUp = fixture.FileBindingTests.asyncSetUp
    create = fixture.FileBindingTests.create
    transport_factory = fixture.FileBindingTests.transport_factory
    approve = fixture.FileBindingTests.approve

    async def test_submit_and_existing_receipt_release_without_revoking_consent(self):
        binding = await self.create()
        attachment = await self.approve(binding)
        record = await binding.submit(attachment, "approval")
        self.assertNotIn(attachment, self.runtime.source._entries)
        self.assertEqual(self.runtime.source._bytes, 0)
        self.assertFalse(self.repo.get_approval("approval").revoked)
        self.assertEqual(await binding.existing_submission("approval"), record)
        self.assertEqual(self.calls, [("upload", "connector-a"), ("import", "connector-a")])

    async def test_discard_read_authorization_after_write_optout_exact_namespace(self):
        a, b = await self.create(), await self.create("b")
        attachment = await self.approve(a)
        with self.assertRaises(ValueError):
            await b.discard_upload(attachment)
        self.assertIn(attachment, self.runtime.source._entries)
        self.repo.replace_policy(
            binding_id="binding-a",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=False,
        )
        await a.discard_upload(attachment)
        await a.discard_upload(attachment)
        self.assertNotIn(attachment, self.runtime.source._entries)
        self.assertFalse(self.repo.get_approval("approval").revoked)
        self.assertIsNone(self.repo.get("approval"))
        self.assertEqual(self.calls, [])

    async def test_revoked_owner_cannot_discard_or_release_other_caller(self):
        binding = await self.create()
        attachment = await self.approve(binding)
        with self.assertRaises(ValueError):
            binding.source.release(replace(self.caller, user_id=self.other), attachment)
        with self.pool.transaction() as conn:
            conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
        with self.assertRaises(ValueError):
            await binding.discard_upload(attachment)
        self.assertIn(attachment, self.runtime.source._entries)

    async def test_failed_submit_does_not_drop_local_snapshot(self):
        binding = await self.create()
        attachment = await self.approve(binding)
        self.repo.revoke_approval("approval", approver_user_id=self.owner)
        with self.assertRaises(ValueError):
            await binding.submit(attachment, "approval")
        self.assertIn(attachment, self.runtime.source._entries)

    async def test_observing_inflight_receipt_does_not_release_live_snapshot(self):
        binding = await self.create()
        attachment = await self.approve(binding)
        self.release_import.clear()
        task = asyncio.create_task(binding.submit(attachment, "approval"))
        try:
            await asyncio.wait_for(self.entered_import.wait(), 2)
            record = await binding.existing_submission("approval")
            self.assertEqual(record.state, "importing")
            self.assertIn(attachment, self.runtime.source._entries)
            self.assertEqual(self.runtime.source._bytes, 3)
        finally:
            self.release_import.set()
            await task
        self.assertNotIn(attachment, self.runtime.source._entries)
