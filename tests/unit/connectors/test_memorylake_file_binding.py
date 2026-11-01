"""Dormant trusted composition with real SQLite; synthetic bytes/fake transport.

No HTTP/JWT ingress, real MemoryLake command, or production capability registration.
"""

import asyncio
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.file_binding import MemoryLakeFileBinding
from octop.infra.connectors.memorylake.file_runtime import MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.connectors.memorylake.submissions import Approval, SubmissionError
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
from octop.infra.db.repos.users import UserRepo


class FileBindingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        directory = self.enterContext(TemporaryDirectory())
        self.pool = SqlitePool(Path(directory) / "db")
        self.addCleanup(self.pool.close)
        run_migrations(self.pool)
        users = UserRepo(self.pool)
        self.owner = users.create(username="owner", role="user")
        self.other = users.create(username="other", role="admin")
        AgentRepo(self.pool).create(agent_id="agent", user_id=self.owner, name="agent")
        self.bindings, self.repo = (
            MemoryLakeBindingRepo(self.pool),
            MemoryLakeSubmissionRepo(self.pool),
        )
        connectors = ConnectorRepo(self.pool)
        for name in ("a", "b"):
            connectors.create(
                instance_id=f"connector-{name}",
                user_id=self.owner,
                kind="memorylake",
                display_name=name,
                mcp_server_name=f"ml-{name}",
            )
            connectors.upsert_credentials(instance_id=f"connector-{name}", blob=b"synthetic")
            self.bindings.create(
                binding_id=f"binding-{name}",
                connector_id=f"connector-{name}",
                owner_user_id=self.owner,
                agent_id="agent",
                base_url="https://app.memorylake.ai/openapi/memorylake",
                workspace_id="workspace",
                project_ids_json='["project"]',
                enabled=True,
            )
            self.repo.replace_policy(
                binding_id=f"binding-{name}",
                expected_revision=0,
                project_id="project",
                library_parent_id="folder",
                enabled=True,
            )
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.factory_calls, self.calls = [], []
        self.entered_import, self.release_import = asyncio.Event(), asyncio.Event()
        self.release_import.set()
        self.after_upload = lambda: None
        self.after_import = lambda: None

    async def asyncSetUp(self):
        self.runtime = MemoryLakeFileRuntime(
            authorize_factory=lambda admission: (
                StoredWriteBindingLoader(
                    caller_user_id=self.owner,
                    agent_id="agent",
                    is_group=False,
                    binding_id="binding-a",
                    connector_id="connector-a",
                    repo=self.bindings,
                    admission=admission,
                ).authorize
            )
        )
        self.addAsyncCleanup(self.runtime.aclose)

    def transport_factory(self, connector_id, load_binding):
        self.factory_calls.append((connector_id, load_binding))

        class FakeTransport:
            async def upload(inner, binding, snapshot, *, lease):
                self.calls.append(("upload", binding.scope.connector_id))
                self.after_upload()
                return ProcessResult(0, b'{"item_id":"item:1"}', b"")

            async def import_file(inner, binding, item_id, *, lease):
                self.calls.append(("import", binding.scope.connector_id))
                self.entered_import.set()
                await self.release_import.wait()
                self.after_import()
                return ProcessResult(
                    0,
                    b'{"success_count":1,"failure_count":0,"duplicate_count":0,"details_truncated":false,"details":[{"result":"success","drive_item_id":"item:1","document_id":"doc:1"}]}',
                    b"",
                )

            async def get_document(inner, binding, document_id):
                self.calls.append(("get", binding.scope.connector_id))
                return ProcessResult(0, b'{"id":"doc:1","status":"okay"}', b"")

        return FakeTransport()

    async def create(self, name="a", **kwargs):
        options = {
            "runtime": self.runtime,
            "caller": self.caller,
            "binding_id": f"binding-{name}",
            "connector_id": f"connector-{name}",
            "binding_repo": self.bindings,
            "submission_repo": self.repo,
            "transport_factory": self.transport_factory,
            "check_invocation": lambda: None,
        }
        options.update(kwargs)
        return await MemoryLakeFileBinding.create(**options)

    async def close_binding(self, binding):
        binding.close()

    async def approve(self, binding, approval_id="approval"):
        with self.runtime.operation():
            attachment = await binding.source.add(binding.caller, "synthetic.txt", b"abc")
        intent = await binding.prepare(attachment)
        self.assertIsNone(self.repo.get_approval(approval_id))
        await binding.create_approval(Approval(approval_id, intent, time.time() + 60))
        return attachment

    async def test_bad_server_binding_owner_revision_rejected_before_transport_factory(self):
        for overrides in (
            {"connector_id": "connector-b"},
            {"caller": replace(self.caller, user_id=self.other, agent_owner_user_id=self.other)},
            {"caller": replace(self.caller, authorization_revision=2)},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                await self.create(**overrides)
        self.assertEqual(self.factory_calls, [])
        self.assertEqual(self.calls, [])

    async def test_source_prepare_explicit_approval_submit_and_status(self):
        binding = await self.create()
        self.addAsyncCleanup(self.close_binding, binding)
        self.assertEqual(binding.caller, self.caller)
        attachment = await self.approve(binding)
        self.assertEqual(self.calls, [])
        self.assertEqual((await binding.submit(attachment, "approval")).state, "processing")
        result = await binding.poll_status("approval", max_checks=1, timeout_s=1, interval_s=0.1)
        self.assertEqual(result.stop_reason, "ready")
        self.assertEqual((await binding.check_status("approval")).state, "ready")
        self.assertEqual(
            self.calls,
            [("upload", "connector-a"), ("import", "connector-a"), ("get", "connector-a")],
        )

    async def test_binding_b_cannot_use_binding_a_source_or_approval(self):
        a, b = await self.create(), await self.create("b")
        self.addAsyncCleanup(self.close_binding, a)
        self.addAsyncCleanup(self.close_binding, b)
        attachment = await self.approve(a)
        with self.assertRaises(ValueError):
            await b.prepare(attachment)
        with self.assertRaises(ValueError):
            await b.submit(attachment, "approval")
        with self.runtime.operation():
            own = await b.source.add(b.caller, "synthetic.txt", b"abc")
        with self.assertRaises(ValueError):
            await b.submit(own, "approval")
        self.assertFalse(self.repo.get_approval("approval").claimed)
        self.assertEqual(self.calls, [])

    async def test_runtime_close_waits_for_active_import_and_final_persistence(self):
        binding = await self.create()
        self.addAsyncCleanup(self.close_binding, binding)
        attachment = await self.approve(binding)
        self.release_import.clear()
        submit = asyncio.create_task(binding.submit(attachment, "approval"))
        shutdown = None
        try:
            await asyncio.wait_for(self.entered_import.wait(), timeout=2)
            self.runtime.close()
            with self.assertRaises(ValueError):
                await binding.prepare(attachment)
            shutdown = asyncio.create_task(self.runtime.aclose())
            await asyncio.sleep(0)
            self.assertFalse(shutdown.done())
            self.release_import.set()
            self.assertEqual((await submit).state, "processing")
            await shutdown
            self.assertTrue(self.runtime.drained)
            self.assertEqual(self.repo.get("approval").state, "processing")
            self.assertEqual(self.repo.get("approval").document_id, "doc:1")
        finally:
            self.release_import.set()
            await asyncio.gather(submit, return_exceptions=True)
            if shutdown is not None:
                await shutdown

    async def test_invocation_close_after_upload_keeps_receipt_and_denies_later_loads(self):
        binding = await self.create()
        self.after_upload = binding.close
        attachment = await self.approve(binding)
        with self.assertRaises(ValueError):
            await binding.submit(attachment, "approval")
        saved = self.repo.get("approval")
        self.assertEqual(saved.item_id, "item:1")
        self.assertEqual(saved.state, "uploaded")
        self.assertEqual(self.calls, [("upload", "connector-a")])
        with self.assertRaises(ValueError):
            await self.factory_calls[0][1]()
        with self.assertRaises(ValueError):
            await binding.check_status("approval")

    async def test_create_invalidation_during_real_sql_await_never_builds_transport(self):
        started, release = threading.Event(), threading.Event()
        active = True
        original = self.bindings.load_owned_write_binding

        def check():
            if not active:
                raise ValueError("synthetic invocation detail")

        def blocked(*args, **kwargs):
            result = original(*args, **kwargs)
            started.set()
            release.wait(3)
            return result

        with patch.object(self.bindings, "load_owned_write_binding", side_effect=blocked):
            task = asyncio.create_task(self.create(check_invocation=check))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                active = False
            finally:
                release.set()
                with self.assertRaises(SubmissionError) as caught:
                    await task
                jobs = tuple(self.runtime.database._jobs)
                if jobs:
                    await asyncio.gather(*jobs, return_exceptions=True)
        self.assertNotIn("synthetic invocation detail", str(caught.exception))
        self.assertEqual(self.factory_calls, [])
        self.assertEqual(self.calls, [])

    async def test_invocation_close_at_import_result_saves_internal_evidence_not_success(self):
        binding = await self.create()
        self.addAsyncCleanup(self.close_binding, binding)
        attachment = await self.approve(binding)
        self.after_import = binding.close
        with self.assertRaises(SubmissionError) as caught:
            await binding.submit(attachment, "approval")
        saved = self.repo.get("approval")
        evidence = caught.exception.evidence
        self.assertEqual(str(caught.exception), "file_invocation_unavailable")
        self.assertIsNotNone(evidence)
        self.assertEqual(
            (saved.state, saved.item_id, saved.document_id), ("processing", "item:1", "doc:1")
        )
        self.assertEqual(
            (evidence.state, evidence.item_id, evidence.document_id, evidence.revision),
            (saved.state, saved.item_id, saved.document_id, saved.revision),
        )
        self.assertEqual(self.calls, [("upload", "connector-a"), ("import", "connector-a")])
        with self.assertRaises(SubmissionError):
            await binding.submit(attachment, "approval")
        self.assertEqual(len(self.calls), 2)

    async def test_cancelled_transport_factory_cannot_leave_captured_loader_active(self):
        captured = []

        def cancelling_factory(connector_id, load_binding):
            captured.append(load_binding)
            raise asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.create(transport_factory=cancelling_factory)
        self.assertEqual(len(captured), 1)
        with self.assertRaises(SubmissionError):
            await captured[0]()
        self.assertEqual(self.calls, [])

    async def assert_late_database_read_revocation_keeps_internal_receipt(self, statement):
        binding = await self.create()
        self.addAsyncCleanup(self.close_binding, binding)
        attachment = await self.approve(binding)

        def revoke():
            with self.pool.transaction() as conn:
                conn.execute(statement)

        self.after_import = revoke
        with self.assertRaises(SubmissionError) as caught:
            await binding.submit(attachment, "approval")
        saved = self.repo.get("approval")
        self.assertEqual(
            (saved.state, saved.item_id, saved.document_id), ("processing", "item:1", "doc:1")
        )
        evidence = caught.exception.evidence
        self.assertIsNotNone(evidence)
        self.assertEqual(
            (evidence.state, evidence.item_id, evidence.document_id, evidence.revision),
            (saved.state, saved.item_id, saved.document_id, saved.revision),
        )
        with self.assertRaises(ValueError):
            await binding.submit(attachment, "approval")
        self.assertEqual(self.calls, [("upload", "connector-a"), ("import", "connector-a")])

    async def test_submit_binding_disabled_after_import_does_not_deliver_saved_receipt(self):
        await self.assert_late_database_read_revocation_keeps_internal_receipt(
            "UPDATE memorylake_bindings SET enabled=0 WHERE binding_id='binding-a'"
        )

    async def test_submit_owner_disabled_after_import_does_not_deliver_saved_receipt(self):
        await self.assert_late_database_read_revocation_keeps_internal_receipt(
            "UPDATE users SET disabled=1 WHERE username='owner'"
        )

    async def test_submit_write_optout_after_import_does_not_revoke_read_receipt(self):
        binding = await self.create()
        self.addAsyncCleanup(self.close_binding, binding)
        attachment = await self.approve(binding)
        self.after_import = lambda: self.repo.replace_policy(
            binding_id="binding-a",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=False,
        )
        record = await binding.submit(attachment, "approval")
        self.assertEqual(
            (record.state, record.item_id, record.document_id), ("processing", "item:1", "doc:1")
        )
        self.assertEqual(self.calls, [("upload", "connector-a"), ("import", "connector-a")])

    async def test_submit_credential_rotation_after_import_allows_current_read_receipt(self):
        binding = await self.create()
        self.addAsyncCleanup(self.close_binding, binding)
        attachment = await self.approve(binding)
        self.after_import = lambda: ConnectorRepo(self.pool).upsert_credentials(
            instance_id="connector-a", blob=b"rotated synthetic"
        )
        record = await binding.submit(attachment, "approval")
        self.assertEqual((record.state, record.document_id), ("processing", "doc:1"))
        self.assertEqual((await self.factory_calls[0][1]()).credential_revision, 2)
        self.assertEqual(self.calls, [("upload", "connector-a"), ("import", "connector-a")])

    async def test_cancelled_final_read_authorization_keeps_persisted_processing_evidence(self):
        binding = await self.create()
        self.addAsyncCleanup(self.close_binding, binding)
        attachment = await self.approve(binding)
        imported, started, release = threading.Event(), threading.Event(), threading.Event()
        self.after_import = imported.set
        original = self.bindings.load_owned_write_binding

        def blocked(*args, **kwargs):
            result = original(*args, **kwargs)
            if imported.is_set():
                started.set()
                release.wait(3)
            return result

        with patch.object(self.bindings, "load_owned_write_binding", side_effect=blocked):
            task = asyncio.create_task(binding.submit(attachment, "approval"))
            jobs = ()
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                jobs = tuple(self.runtime.database._jobs)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError) as caught:
                    await task
                self.assertEqual(str(caught.exception), "submission_cancelled_with_receipt")
                evidence = getattr(caught.exception, "evidence", None)
                self.assertIsNotNone(evidence)
                saved = self.repo.get("approval")
                self.assertEqual(
                    (saved.state, saved.item_id, saved.document_id),
                    ("processing", "item:1", "doc:1"),
                )
                self.assertEqual(
                    (evidence.state, evidence.item_id, evidence.document_id, evidence.revision),
                    (saved.state, saved.item_id, saved.document_id, saved.revision),
                )
                self.assertEqual(self.calls, [("upload", "connector-a"), ("import", "connector-a")])
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
                await asyncio.gather(*jobs, return_exceptions=True)
