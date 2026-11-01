"""Synthetic new-byte flow through real SQLite adapters, never HTTP/UploadFile.

No route registration, native attachments, real credentials, or executable CLI.
The transport is fake; passing tests do not authorize production external writes.
"""

import asyncio
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.connectors.memorylake.submission_storage import (
    DatabaseJobAdmission,
    DatabaseSubmissionLedger,
)
from octop.infra.connectors.memorylake.submissions import Approval, SubmissionService
from octop.infra.connectors.memorylake.upload_source import EphemeralUploadSource
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.migrate import run_migrations
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
from octop.infra.db.repos.users import UserRepo


class UploadFlowTests(unittest.IsolatedAsyncioTestCase):
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
        self.connectors = ConnectorRepo(self.pool)
        self.connectors.create(
            instance_id="connector",
            user_id=self.owner,
            kind="memorylake",
            display_name="ml",
            mcp_server_name="ml",
        )
        self.connectors.upsert_credentials(
            instance_id="connector", blob=b"synthetic ciphertext placeholder"
        )
        self.bindings = MemoryLakeBindingRepo(self.pool)
        self.bindings.create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=self.owner,
            agent_id="agent",
            base_url="https://app.memorylake.ai/openapi/memorylake",
            workspace_id="workspace",
            project_ids_json='["project","other-project"]',
            enabled=True,
        )
        self.now = 100.0
        self.repo = MemoryLakeSubmissionRepo(self.pool, clock=lambda: self.now)
        self.repo.replace_policy(
            binding_id="binding",
            expected_revision=0,
            project_id="project",
            library_parent_id="folder",
            enabled=True,
        )
        self.admission = DatabaseJobAdmission(max_jobs=4)
        self.loader = StoredWriteBindingLoader(
            caller_user_id=self.owner,
            agent_id="agent",
            is_group=False,
            binding_id="binding",
            connector_id="connector",
            repo=self.bindings,
            admission=self.admission,
        )
        self.ledger = DatabaseSubmissionLedger(
            self.repo, binding_id="binding", admission=self.admission
        )
        self.caller = CallerContext(self.owner, "agent", self.owner, 1)
        self.source = EphemeralUploadSource(
            authorize=self.loader.authorize, clock=lambda: self.now, ttl_s=10
        )
        self.calls = []
        self.after_upload = lambda: None

        class FakeTransport:
            async def upload(inner, binding, snapshot, *, lease):
                self.calls.append(("upload", binding, snapshot.content))
                self.after_upload()
                return ProcessResult(0, b'{"item_id":"item:1"}', b"")

            async def import_file(inner, binding, item_id, *, lease):
                self.calls.append(("import", binding, item_id))
                return ProcessResult(
                    0,
                    b'{"success_count":1,"failure_count":0,"duplicate_count":0,"details_truncated":false,"details":[{"result":"success","drive_item_id":"item:1","document_id":"document:1"}]}',
                    b"",
                )

            async def get_document(inner, binding, document_id):
                self.calls.append(("get", binding, document_id))
                return ProcessResult(0, b'{"id":"document:1","status":"okay"}', b"")

        self.service = SubmissionService(
            "connector",
            load_binding=self.loader.load,
            source=self.source,
            ledger=self.ledger,
            transport=FakeTransport(),
            clock=lambda: self.now,
        )
        self.addAsyncCleanup(self.cleanup)

    async def cleanup(self):
        self.source.close()
        self.assertFalse(self.source._entries)
        jobs = tuple(self.admission._jobs)
        if jobs:
            await asyncio.gather(*jobs, return_exceptions=True)

    async def approved(self, approval_id="approval"):
        attachment = await self.source.add(self.caller, "synthetic.txt", b"synthetic bytes")
        intent = await self.service.prepare(self.caller, attachment)
        self.assertIsNone(self.repo.get_approval(approval_id))
        self.assertEqual(self.calls, [])
        await self.ledger.create_approval(Approval(approval_id, intent, self.now + 60))
        return attachment

    async def test_explicit_approval_complete_flow_and_no_duplicate_submit(self):
        attachment = await self.source.add(self.caller, "synthetic.txt", b"synthetic bytes")
        intent = await self.service.prepare(self.caller, attachment)
        self.assertIsNone(self.repo.get_approval("approval"))
        with self.assertRaises(ValueError):
            await self.service.submit(self.caller, attachment, "approval")
        self.assertEqual(self.calls, [])
        await self.ledger.create_approval(Approval("approval", intent, self.now + 60))
        record = await self.service.submit(self.caller, attachment, "approval")
        self.assertEqual(record.state, "processing")
        result = await self.service.poll_status(self.caller, "approval", max_checks=2, timeout_s=1)
        self.assertEqual(result.stop_reason, "ready")
        self.assertEqual(result.record.document_id, "document:1")
        with self.assertRaises(ValueError):
            await self.service.submit(self.caller, attachment, "approval")
        self.assertEqual([call[0] for call in self.calls], ["upload", "import", "get"])
        self.assertEqual(self.calls[0][2], b"synthetic bytes")

    async def test_source_expiry_and_revoke_block_unclaimed_approval(self):
        for action in ("expire", "revoke"):
            with self.subTest(action=action):
                attachment = await self.approved(action)
                if action == "expire":
                    self.now += 11
                else:
                    await self.source.revoke(self.caller, attachment)
                with self.assertRaises(ValueError):
                    await self.service.submit(self.caller, attachment, action)
                self.assertFalse(self.repo.get_approval(action).claimed)
                self.assertEqual(self.calls, [])

    async def test_content_id_and_caller_binding_reject_before_transport(self):
        attachment = await self.approved()
        changed = await self.source.add(self.caller, "synthetic.txt", b"different synthetic bytes")
        other_caller = replace(self.caller, user_id=self.other, agent_owner_user_id=self.other)
        for caller, key, approval in (
            (self.caller, changed, "approval"),
            (self.caller, attachment, "unknown"),
            (other_caller, attachment, "approval"),
        ):
            with self.subTest(key=key, caller=caller), self.assertRaises(ValueError):
                await self.service.submit(caller, key, approval)
        self.assertFalse(self.repo.get_approval("approval").claimed)
        self.assertEqual(self.calls, [])

    async def test_live_revocation_before_remote_steps(self):
        attachment = await self.approved()
        with self.pool.transaction() as conn:
            conn.execute("UPDATE users SET disabled=1 WHERE id=?", (self.owner,))
        with self.assertRaises(ValueError):
            await self.service.submit(self.caller, attachment, "approval")
        self.assertEqual(self.calls, [])

    async def test_revocation_between_remote_steps_preserves_item_without_import(self):
        attachment = await self.approved()
        self.after_upload = lambda: self.repo.replace_policy(
            binding_id="binding",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=False,
        )
        with self.assertRaises(ValueError):
            await self.service.submit(self.caller, attachment, "approval")
        self.assertEqual([call[0] for call in self.calls], ["upload"])
        record = await self.ledger.get("approval")
        self.assertEqual((record.state, record.item_id), ("uploaded", "item:1"))
        with self.assertRaises(ValueError):
            await self.service.submit(self.caller, attachment, "approval")
        self.assertEqual(len(self.calls), 1)

    async def test_write_optout_rotation_and_default_target_change_allow_readonly_reconcile(self):
        attachment = await self.approved()
        await self.service.submit(self.caller, attachment, "approval")
        self.repo.replace_policy(
            binding_id="binding",
            expected_revision=1,
            project_id="other-project",
            library_parent_id="other-folder",
            enabled=False,
        )
        self.connectors.upsert_credentials(instance_id="connector", blob=b"rotated synthetic")
        self.source.close()
        result = await self.service.poll_status(self.caller, "approval", max_checks=1, timeout_s=1)
        self.assertEqual(result.stop_reason, "ready")
        operation, binding, _ = self.calls[-1]
        self.assertEqual(operation, "get")
        self.assertFalse(binding.writes_enabled)
        self.assertEqual((binding.credential_revision, binding.project_id), (2, "project"))
        self.assertEqual([call[0] for call in self.calls], ["upload", "import", "get"])

    async def test_deleted_write_policy_does_not_block_authorized_read_reconciliation(self):
        attachment = await self.approved()
        await self.service.submit(self.caller, attachment, "approval")
        with self.pool.transaction() as conn:
            conn.execute("DELETE FROM memorylake_write_policies WHERE binding_id='binding'")
        result = await self.service.check_status(self.caller, "approval")
        self.assertEqual(result.state, "ready")
        self.assertEqual([call[0] for call in self.calls], ["upload", "import", "get"])
        operation, binding, document = self.calls[-1]
        self.assertEqual(
            (operation, binding.project_id, document), ("get", "project", "document:1")
        )
        self.assertFalse(binding.writes_enabled)

    async def test_disabled_obsolete_write_target_does_not_replace_original_read_project(self):
        attachment = await self.approved()
        await self.service.submit(self.caller, attachment, "approval")
        self.repo.replace_policy(
            binding_id="binding",
            expected_revision=1,
            project_id="revoked-write-project",
            library_parent_id="folder",
            enabled=False,
        )
        result = await self.service.check_status(self.caller, "approval")
        self.assertEqual(result.state, "ready")
        self.assertEqual([call[0] for call in self.calls], ["upload", "import", "get"])
        _, binding, document = self.calls[-1]
        self.assertEqual((binding.project_id, document), ("project", "document:1"))
        self.assertFalse(binding.writes_enabled)

    async def test_real_runtime_closes_ingress_before_active_submission_final_save(self):
        from octop.infra.connectors.memorylake.file_runtime import (
            FileRuntimeError,
            MemoryLakeFileRuntime,
        )

        loaders = []

        def authorize_factory(admission):
            loader = StoredWriteBindingLoader(
                caller_user_id=self.owner,
                agent_id="agent",
                is_group=False,
                binding_id="binding",
                connector_id="connector",
                repo=self.bindings,
                admission=admission,
            )
            loaders.append(loader)
            return loader.authorize

        runtime = MemoryLakeFileRuntime(authorize_factory=authorize_factory)
        ledger = DatabaseSubmissionLedger(
            self.repo, binding_id="binding", admission=runtime.database
        )
        entered_import, release_import = asyncio.Event(), asyncio.Event()
        original_transport = self.service._transport

        class FakeTransport:
            async def upload(inner, binding, snapshot, *, lease):
                return await original_transport.upload(binding, snapshot, lease=lease)

            async def import_file(inner, binding, item_id, *, lease):
                result = await original_transport.import_file(binding, item_id, lease=lease)
                entered_import.set()
                await release_import.wait()
                return result

            async def get_document(inner, binding, document_id):
                return await original_transport.get_document(binding, document_id)

        service = SubmissionService(
            "connector",
            load_binding=loaders[0].load,
            source=runtime.source,
            ledger=ledger,
            transport=FakeTransport(),
            clock=lambda: self.now,
        )

        async def operation():
            with runtime.operation():
                attachment = await runtime.source.add(
                    self.caller, "synthetic.txt", b"synthetic runtime bytes"
                )
                intent = await service.prepare(self.caller, attachment)
                self.assertIsNone(self.repo.get_approval("runtime-approval"))
                await ledger.create_approval(Approval("runtime-approval", intent, self.now + 60))
                submitted = await service.submit(self.caller, attachment, "runtime-approval")
                self.assertEqual(submitted.state, "processing")
                return await service.check_status(self.caller, "runtime-approval")

        worker = asyncio.create_task(operation())
        shutdown = None
        try:
            await asyncio.wait_for(entered_import.wait(), timeout=2)
            runtime.close()
            self.assertTrue(runtime.closed)
            with self.assertRaises(FileRuntimeError), runtime.operation():
                self.fail("new operation entered closed runtime")
            shutdown = asyncio.create_task(runtime.aclose())
            await asyncio.sleep(0)
            self.assertFalse(shutdown.done())
            release_import.set()
            self.assertEqual((await worker).state, "ready")
            await shutdown
            self.assertTrue(runtime.drained)
            self.assertFalse(runtime.source._entries)
            self.assertFalse(runtime.database._jobs)
            self.assertEqual(self.repo.get("runtime-approval").state, "ready")
            self.assertEqual([call[0] for call in self.calls], ["upload", "import", "get"])
            # The fixture closes the actual pool only after this completed drain.
            with self.pool.connect() as conn:
                self.assertEqual(conn.execute("SELECT 1").fetchone()[0], 1)
        finally:
            release_import.set()
            await asyncio.gather(worker, return_exceptions=True)
            await runtime.aclose()
            if shutdown is not None:
                await shutdown
