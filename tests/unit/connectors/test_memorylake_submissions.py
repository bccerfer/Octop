"""Submission state tests: immutable synthetic bytes and fake transports only."""

from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope
from octop.infra.connectors.memorylake.runner import (
    MemoryLakeCancelledError,
    MemoryLakeRunnerError,
    ProcessResult,
)
from octop.infra.connectors.memorylake.submissions import (
    Approval,
    FileSnapshot,
    SubmissionCancelledError,
    SubmissionError,
    SubmissionRecord,
    SubmissionService,
    WriteBinding,
)


class FakeLedger:
    def __init__(self):
        self.approvals = {}
        self.records = {}
        self.consumed = set()
        self.fail_state = None
        self.binding = None
        self.step_grants = []
        self.before_step = None

    async def approval(self, approval_id):
        return self.approvals[approval_id]

    async def claim(self, approval_id, intent, now):
        approval = self.approvals[approval_id]
        if (
            approval_id in self.consumed
            or approval.revoked
            or approval.intent != intent
            or now >= approval.expires_at
        ):
            raise ValueError("claim denied")
        self.consumed.add(approval_id)
        record = SubmissionRecord(approval_id, intent)
        self.records[record.submission_id] = record
        return record

    async def begin_step(self, previous, phase, now):
        from octop.infra.connectors.memorylake.submissions import StepLease

        if self.before_step:
            self.before_step()
        approval = self.approvals[previous.submission_id]
        current = self.binding()
        if (
            previous.submission_id not in self.consumed
            or self.records[previous.submission_id] != previous
            or current != previous.intent.binding
            or not current.writes_enabled
            or approval.revoked
            or approval.intent != previous.intent
            or now >= approval.expires_at
            or previous.state != {"uploading": "approved", "importing": "uploaded"}[phase]
        ):
            raise ValueError("atomic preflight denied")
        if phase == self.fail_state:
            raise OSError("private database detail")
        updated = replace(previous, state=phase, revision=previous.revision + 1)
        lease = StepLease(updated, phase, updated.revision, min(approval.expires_at, now + 30))
        self.records[updated.submission_id] = updated
        self.step_grants.append(lease)
        return lease

    async def save(self, previous, updated):
        if updated.state == self.fail_state:
            raise OSError("private database detail")
        if self.records[previous.submission_id] != previous:
            raise ValueError("CAS conflict")
        self.records[updated.submission_id] = updated

    async def get(self, submission_id):
        return self.records[submission_id]


class SubmissionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())

    async def asyncSetUp(self):
        self.caller = CallerContext(11, "agent-1", 11, 1)
        self.binding = WriteBinding(
            ReadScope("connection-1", 11, "agent-1", "workspace-1", ("project-1",)),
            "project-1",
            "folder:1",
            "https://app.memorylake.ai/openapi/memorylake",
            1,
            True,
        )
        self.file = FileSnapshot("attachment-1", "demo.txt", b"synthetic example")
        self.reads = 0
        self.calls = []
        self.ledger = FakeLedger()
        self.ledger.binding = lambda: self.binding
        self.upload_reply = ProcessResult(0, b'{"item_id":"file:1"}', b"")
        self.import_reply = ProcessResult(
            0,
            b'{"success_count":1,"failure_count":0,"duplicate_count":0,"details_truncated":false,"details":[{"result":"success","drive_item_id":"file:1","document_id":"doc-1"}]}',
            b"",
        )
        self.status = "pending"
        self.observed_binding = None
        self.upload_error = None
        self.after_upload = None
        outer = self

        class Source:
            async def read(self, caller, attachment_id):
                outer.reads += 1
                return outer.file

        class Transport:
            async def upload(self, binding, snapshot, *, lease):
                outer.calls.append("upload")
                assert outer.ledger.records["approval-1"].state == "uploading"
                if outer.upload_error:
                    raise outer.upload_error
                if outer.after_upload:
                    outer.after_upload()
                return outer.upload_reply

            async def import_file(self, binding, item_id, *, lease):
                outer.calls.append("import")
                assert outer.ledger.records["approval-1"].state == "importing"
                return outer.import_reply

            async def get_document(self, binding, document_id):
                outer.observed_binding = binding
                outer.calls.append("get")
                return ProcessResult(
                    0, ('{"id":"doc-1","status":"' + outer.status + '"}').encode(), b""
                )

        async def load():
            return self.binding

        self.service = SubmissionService(
            "connection-1",
            load_binding=load,
            source=Source(),
            ledger=self.ledger,
            transport=Transport(),
            clock=lambda: 100.0,
        )
        intent = await self.service.prepare(self.caller, "attachment-1")
        self.ledger.approvals["approval-1"] = Approval("approval-1", intent, 200.0)
        self.reads = 0

    async def submit(self):
        return await self.service.submit(self.caller, "attachment-1", "approval-1")

    async def test_complete_fake_flow_distinguishes_processing_and_ready(self):
        record = await self.submit()
        self.assertEqual(record.state, "processing")
        self.assertEqual((record.item_id, record.document_id), ("file:1", "doc-1"))
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "processing")
        self.status = "okay"
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "ready")
        self.assertEqual(self.calls, ["upload", "import", "get", "get"])

    async def test_identity_and_write_optin_are_checked_before_source_read(self):
        for caller in (
            replace(self.caller, user_id=12),
            replace(self.caller, is_group=True),
            replace(self.caller, agent_owner_user_id=None),
        ):
            with self.subTest(caller=caller), self.assertRaises(ValueError):
                await self.service.submit(caller, "attachment-1", "approval-1")
        self.binding = replace(self.binding, writes_enabled=False)
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.reads, 0)
        self.assertEqual(self.calls, [])

    async def test_changed_content_filename_and_attachment_are_not_approved(self):
        for snapshot in (
            replace(self.file, content=b"changed"),
            replace(self.file, filename="other.txt"),
            replace(self.file, attachment_id="other"),
        ):
            self.file = snapshot
            with self.subTest(snapshot=snapshot), self.assertRaises(ValueError):
                await self.submit()
        self.assertEqual(self.calls, [])

    async def test_changed_target_and_credentials_are_not_approved(self):
        for binding in (
            replace(self.binding, library_parent_id="another"),
            replace(self.binding, credential_revision=2),
            replace(self.binding, scope=replace(self.binding.scope, workspace_id="another")),
        ):
            self.binding = binding
            with self.subTest(binding=binding), self.assertRaises(ValueError):
                await self.submit()
        self.assertEqual(self.calls, [])

    async def test_expired_revoked_or_replayed_approval_cannot_upload(self):
        original = self.ledger.approvals["approval-1"]
        for approval in (replace(original, expires_at=100.0), replace(original, revoked=True)):
            self.ledger.approvals["approval-1"] = approval
            with self.assertRaises(ValueError):
                await self.submit()
        self.ledger.approvals["approval-1"] = original
        await self.submit()
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.calls, ["upload", "import"])

    async def test_revocation_between_writes_stops_import(self):
        self.after_upload = lambda: setattr(
            self, "binding", replace(self.binding, writes_enabled=False)
        )
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.calls, ["upload"])
        self.assertEqual(self.ledger.records["approval-1"].item_id, "file:1")

    async def test_persist_failure_prevents_remote_steps(self):
        self.ledger.fail_state = "uploading"
        with self.assertRaises(SubmissionError):
            await self.submit()
        self.assertEqual(self.calls, [])

    async def test_persist_failure_after_upload_preserves_evidence_and_stops_import(self):
        self.ledger.fail_state = "uploaded"
        with self.assertRaises(SubmissionError) as caught:
            await self.submit()
        self.assertEqual(caught.exception.evidence.item_id, "file:1")
        self.assertNotIn("private", str(caught.exception))
        self.assertEqual(self.calls, ["upload"])

    async def test_nonzero_upload_keeps_known_id_without_import(self):
        self.upload_reply = ProcessResult(1, b'{"item_id":"file:1"}', b"secret")
        record = await self.submit()
        self.assertEqual(record.state, "unknown")
        self.assertEqual(record.item_id, "file:1")
        self.assertEqual(self.calls, ["upload"])

    async def test_nonzero_import_keeps_document_id_and_never_reuploads(self):
        self.import_reply = replace(self.import_reply, returncode=1)
        record = await self.submit()
        self.assertEqual(record.state, "unknown")
        self.assertEqual(record.document_id, "doc-1")
        self.assertEqual(self.calls, ["upload", "import"])

    async def test_cancelled_upload_keeps_partial_safe_id_and_propagates_cancel(self):
        self.upload_error = MemoryLakeCancelledError(
            partial_result=ProcessResult(-9, b'{"item_id":"file:1"}', b"secret")
        )
        with self.assertRaises(SubmissionCancelledError) as caught:
            await self.submit()
        self.assertEqual(caught.exception.evidence.item_id, "file:1")
        self.assertEqual(self.ledger.records["approval-1"].state, "unknown")
        self.assertEqual(self.calls, ["upload"])

    async def test_timeout_without_result_is_unknown_not_retry(self):
        self.upload_error = TimeoutError("secret")
        record = await self.submit()
        self.assertEqual(record.state, "unknown")
        self.assertEqual(self.calls, ["upload"])

    async def test_unknown_status_is_not_ready(self):
        record = await self.submit()
        self.status = "future-status"
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "unknown")

    async def test_source_accepts_only_opaque_attachment_and_immutable_bytes(self):
        for attachment in ("/etc/passwd", "../private"):
            with self.assertRaises(ValueError):
                await self.service.prepare(self.caller, attachment)
        self.file = replace(self.file, content=bytearray(b"mutable"))
        with self.assertRaises(ValueError):
            await self.service.prepare(self.caller, "attachment-1")

    async def test_cancellation_during_post_upload_persistence_keeps_evidence(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        original_save = self.ledger.save

        async def save(previous, updated):
            if updated.state == "uploaded":
                entered.set()
                await release.wait()
            await original_save(previous, updated)

        self.ledger.save = save
        task = asyncio.create_task(self.submit())
        await entered.wait()
        task.cancel()
        release.set()
        with self.assertRaises(SubmissionCancelledError) as caught:
            await task
        self.assertEqual(caught.exception.evidence.item_id, "file:1")
        self.assertEqual(self.ledger.records["approval-1"].state, "unknown")
        self.assertEqual(self.calls, ["upload"])

    async def test_wrong_claim_record_is_rejected_before_any_write(self):
        original_claim = self.ledger.claim

        async def claim(approval_id, intent, now):
            record = await original_claim(approval_id, intent, now)
            return replace(record, intent=replace(intent, filename="other.txt"))

        self.ledger.claim = claim
        with self.assertRaisesRegex(SubmissionError, "^invalid_claim_record$"):
            await self.submit()
        self.assertEqual(self.calls, [])

    async def test_concurrent_approval_claim_never_duplicates_remote_writes(self):
        results = await asyncio.gather(self.submit(), self.submit(), return_exceptions=True)
        self.assertEqual(sum(isinstance(result, SubmissionRecord) for result in results), 1)
        self.assertEqual(self.calls.count("upload"), 1)
        self.assertEqual(self.calls.count("import"), 1)

    async def test_import_for_another_file_is_unknown_not_processing(self):
        self.import_reply = replace(
            self.import_reply, stdout=self.import_reply.stdout.replace(b"file:1", b"file:other")
        )
        record = await self.submit()
        self.assertEqual(record.state, "unknown")
        self.assertIsNone(record.document_id)
        self.assertEqual(record.item_id, "file:1")

    async def test_repeated_cancellation_cannot_abandon_unknown_save(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        original_save = self.ledger.save
        self.upload_error = MemoryLakeCancelledError(
            partial_result=ProcessResult(-9, b'{"item_id":"file:1"}', b"")
        )

        async def save(previous, updated):
            if updated.state == "unknown":
                entered.set()
                await release.wait()
            await original_save(previous, updated)

        self.ledger.save = save
        task = asyncio.create_task(self.submit())
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        with self.assertRaises(SubmissionCancelledError) as caught:
            await task
        self.assertEqual(self.ledger.records["approval-1"].state, "unknown")
        self.assertEqual(caught.exception.evidence.item_id, "file:1")

    async def test_cancel_save_failure_carries_safe_evidence(self):
        self.upload_error = MemoryLakeCancelledError(
            partial_result=ProcessResult(-9, b'{"item_id":"file:1"}', b"secret")
        )
        self.ledger.fail_state = "unknown"
        with self.assertRaises(SubmissionCancelledError) as caught:
            await self.submit()
        self.assertEqual(caught.exception.evidence.item_id, "file:1")
        self.assertEqual(caught.exception.evidence.state, "unknown")
        self.assertEqual(self.ledger.records["approval-1"].state, "uploading")
        self.assertNotIn("secret", str(caught.exception))

    async def test_status_cannot_read_another_users_record(self):
        record = await self.submit()
        self.calls.clear()
        other = replace(
            record, intent=replace(record.intent, caller=replace(self.caller, user_id=12))
        )
        self.ledger.records[record.submission_id] = other
        with self.assertRaisesRegex(SubmissionError, "^submission_access_denied$"):
            await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(self.calls, [])

    async def test_status_error_is_failed_not_ready(self):
        record = await self.submit()
        self.status = "error"
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "failed")

    async def test_cancel_before_import_barrier_keeps_uploaded_id(self):
        entered = asyncio.Event()
        original_begin = self.ledger.begin_step

        async def begin(previous, phase, now):
            if phase == "importing":
                entered.set()
                await asyncio.Event().wait()
            return await original_begin(previous, phase, now)

        self.ledger.begin_step = begin
        task = asyncio.create_task(self.submit())
        await entered.wait()
        task.cancel()
        with self.assertRaises(SubmissionCancelledError) as caught:
            await task
        self.assertEqual(caught.exception.evidence.item_id, "file:1")
        self.assertEqual(caught.exception.evidence.state, "unknown")
        self.assertEqual(self.calls, ["upload"])

    async def test_failed_import_document_reference_does_not_prove_submission_ready(self):
        self.import_reply = ProcessResult(
            1,
            b'{"success_count":0,"failure_count":1,"duplicate_count":0,"details_truncated":false,"details":[{"result":"failed","drive_item_id":"file:1","document_id":"doc-1"}]}',
            b"",
        )
        record = await self.submit()
        self.assertEqual(record.document_id, "doc-1")
        self.status = "okay"
        with self.assertRaisesRegex(SubmissionError, "^document_not_observable$"):
            await self.service.check_status(self.caller, record.submission_id)

    async def test_known_nonzero_accepted_import_can_be_reconciled_without_reupload(self):
        self.import_reply = replace(self.import_reply, returncode=1)
        record = await self.submit()
        self.status = "okay"
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "ready")
        self.assertEqual(self.calls, ["upload", "import", "get"])

    async def test_endpoint_change_requires_new_approval(self):
        self.binding = replace(
            self.binding, base_url="https://app.memorylake.cn/openapi/memorylake"
        )
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.calls, [])

    async def test_last_approval_load_cannot_hide_binding_revocation(self):
        original = self.ledger.approval
        calls = 0

        async def approval(approval_id):
            nonlocal calls
            calls += 1
            if calls == 2:
                self.binding = replace(self.binding, writes_enabled=False)
            return await original(approval_id)

        self.ledger.approval = approval
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.calls, [])

    async def test_wrong_submission_id_from_ledger_is_not_read(self):
        record = await self.submit()
        self.calls.clear()
        self.ledger.records[record.submission_id] = replace(record, submission_id="another")
        with self.assertRaisesRegex(SubmissionError, "^submission_access_denied$"):
            await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(self.calls, [])

    async def test_atomic_step_checks_revoked_approval_before_grant(self):
        def revoke():
            approval = self.ledger.approvals["approval-1"]
            self.ledger.approvals["approval-1"] = replace(approval, revoked=True)

        self.ledger.before_step = revoke
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.calls, [])

    async def test_each_write_requires_a_durable_step_lease(self):
        await self.submit()
        self.assertEqual(
            [lease.phase for lease in self.ledger.step_grants], ["uploading", "importing"]
        )

    async def test_readonly_status_survives_write_disable_and_credential_rotation(self):
        record = await self.submit()
        self.binding = replace(self.binding, writes_enabled=False, credential_revision=2)
        self.status = "okay"
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "ready")
        self.assertEqual(self.calls, ["upload", "import", "get"])

    async def test_incomplete_multi_accept_report_cannot_later_claim_ready(self):
        self.import_reply = replace(
            self.import_reply,
            stdout=self.import_reply.stdout.replace(
                b'"success_count":1', b'"success_count":2'
            ).replace(b'"details_truncated":false', b'"details_truncated":true'),
        )
        record = await self.submit()
        self.assertEqual(record.state, "unknown")
        self.assertEqual(record.document_id, "doc-1")
        self.assertFalse(record.import_accepted)
        self.status = "okay"
        with self.assertRaises(SubmissionError):
            await self.service.check_status(self.caller, record.submission_id)

    async def test_reversed_preflight_interleaving_cannot_grant_revoked_approval(self):
        calls = 0

        async def load():
            nonlocal calls
            calls += 1
            if calls == 5:  # Last diagnostic binding read, after approval read.
                approval = self.ledger.approvals["approval-1"]
                self.ledger.approvals["approval-1"] = replace(approval, revoked=True)
            return self.binding

        self.service._load_binding = load
        with self.assertRaisesRegex(SubmissionError, "^step_not_authorized$"):
            await self.submit()
        self.assertEqual(self.calls, [])
        self.assertEqual(self.ledger.step_grants, [])

    async def test_status_uses_original_project_with_new_permission_revision(self):
        record = await self.submit()
        self.caller = replace(self.caller, authorization_revision=2)
        self.binding = replace(
            self.binding,
            scope=replace(
                self.binding.scope,
                authorization_revision=2,
                project_ids=("project-1", "new-default"),
            ),
            project_id="new-default",
            library_parent_id="new-folder",
            writes_enabled=False,
            credential_revision=2,
        )
        self.status = "okay"
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "ready")
        self.assertEqual(self.observed_binding.project_id, "project-1")
        self.assertEqual(self.observed_binding.credential_revision, 2)
        self.assertFalse(self.observed_binding.writes_enabled)

    async def test_status_rejects_original_project_revoke_or_deployment_change(self):
        record = await self.submit()
        original = self.binding
        self.calls.clear()
        for current in (
            replace(
                original, scope=replace(original.scope, project_ids=("other",)), project_id="other"
            ),
            replace(original, scope=replace(original.scope, workspace_id="other")),
            replace(original, base_url="https://app.memorylake.cn/openapi/memorylake"),
        ):
            self.binding = current
            with self.subTest(current=current), self.assertRaises(ValueError):
                await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(self.calls, [])

    async def test_expired_step_lease_cannot_execute(self):
        original = self.ledger.begin_step

        async def begin(previous, phase, now):
            return replace(await original(previous, phase, now), expires_at=now)

        self.ledger.begin_step = begin
        with self.assertRaisesRegex(SubmissionError, "^invalid_step_lease$"):
            await self.submit()
        self.assertEqual(self.calls, [])

    async def test_granted_step_may_complete_but_revocation_blocks_next_grant(self):
        original = self.ledger.begin_step

        async def begin(previous, phase, now):
            lease = await original(previous, phase, now)
            self.binding = replace(self.binding, writes_enabled=False)
            return lease

        self.ledger.begin_step = begin
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.calls, ["upload"])
        self.assertEqual(self.ledger.records["approval-1"].state, "uploaded")
        self.assertEqual(len(self.ledger.step_grants), 1)

    async def no_sleep(self, _delay):
        pass

    async def test_poll_status_stops_on_ready_with_no_writes(self):
        record = await self.submit()

        async def sleep(_delay):
            self.status = "okay"

        result = await self.service.poll_status(self.caller, record.submission_id, sleep=sleep)
        self.assertEqual(result.stop_reason, "ready")
        self.assertEqual(result.record.state, "ready")
        self.assertEqual(result.checks, 2)
        self.assertEqual(self.calls, ["upload", "import", "get", "get"])

    async def test_poll_unknown_status_stops_at_check_limit(self):
        record = await self.submit()
        self.status = "future-status"
        result = await self.service.poll_status(
            self.caller, record.submission_id, max_checks=3, interval_s=0.1, sleep=self.no_sleep
        )
        self.assertEqual(result.stop_reason, "check_limit")
        self.assertEqual(result.record.state, "unknown")
        self.assertEqual(result.checks, 3)
        self.assertEqual(self.calls.count("upload"), 1)
        self.assertEqual(self.calls.count("import"), 1)

    async def test_poll_transient_query_failure_can_reconcile_later(self):
        record = await self.submit()
        original = self.service._transport.get_document
        attempts = 0

        async def get(binding, document_id):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TimeoutError("private upstream body")
            self.status = "okay"
            return await original(binding, document_id)

        self.service._transport.get_document = get
        result = await self.service.poll_status(
            self.caller, record.submission_id, interval_s=0.1, sleep=self.no_sleep
        )
        self.assertEqual(result.stop_reason, "ready")
        self.assertEqual(result.checks, 2)

    async def test_poll_authorization_denial_stops_immediately(self):
        record = await self.submit()

        async def sleep(_delay):
            self.binding = replace(self.binding, scope=replace(self.binding.scope, enabled=False))

        with self.assertRaises(ValueError):
            await self.service.poll_status(self.caller, record.submission_id, sleep=sleep)
        self.assertEqual(self.calls.count("get"), 1)

    async def test_poll_cancellation_does_not_claim_ready(self):
        record = await self.submit()

        async def sleep(_delay):
            raise asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.service.poll_status(self.caller, record.submission_id, sleep=sleep)
        self.assertEqual(self.ledger.records[record.submission_id].state, "processing")
        self.assertEqual(self.calls.count("get"), 1)

    async def test_poll_total_time_budget_is_enforced_with_fake_clock(self):
        record = await self.submit()
        now = [10.0]

        async def sleep(delay):
            now[0] += delay

        result = await self.service.poll_status(
            self.caller,
            record.submission_id,
            timeout_s=1,
            interval_s=2,
            sleep=sleep,
            monotonic=lambda: now[0],
        )
        self.assertEqual(result.stop_reason, "timeout")
        self.assertIsNone(result.record)
        self.assertEqual(result.checks, 1)

    async def test_poll_real_timeout_bounds_a_hanging_query(self):
        record = await self.submit()

        async def get(_binding, _document_id):
            await asyncio.Event().wait()

        self.service._transport.get_document = get
        result = await self.service.poll_status(self.caller, record.submission_id, timeout_s=0.01)
        self.assertEqual(result.stop_reason, "timeout")
        self.assertIsNone(result.record)
        self.assertEqual(result.checks, 1)
        self.assertEqual(self.ledger.records[record.submission_id].state, "processing")

    async def test_poll_terminal_failed_and_ready_are_idempotent(self):
        record = await self.submit()
        self.status = "error"
        result = await self.service.poll_status(
            self.caller, record.submission_id, interval_s=0.1, sleep=self.no_sleep
        )
        self.assertEqual(result.stop_reason, "failed")
        count = self.calls.count("get")
        again = await self.service.poll_status(
            self.caller, record.submission_id, interval_s=0.1, sleep=self.no_sleep
        )
        self.assertEqual(again.stop_reason, "failed")
        self.assertEqual(self.calls.count("get"), count)

    async def test_poll_invalid_limits_are_rejected_before_query(self):
        record = await self.submit()
        for limits in (
            {"max_checks": 0},
            {"max_checks": True},
            {"max_checks": 101},
            {"timeout_s": float("nan")},
            {"timeout_s": 0},
            {"interval_s": -1},
            {"interval_s": 0},
            {"interval_s": float("inf")},
            {"timeout_s": True},
            {"timeout_s": 301},
            {"interval_s": 31},
        ):
            with (
                self.subTest(limits=limits),
                self.assertRaisesRegex(ValueError, "^invalid_poll_limits$"),
            ):
                await self.service.poll_status(self.caller, record.submission_id, **limits)
        self.assertEqual(self.calls.count("get"), 0)

    async def test_readonly_reconciliation_ignores_cleared_write_targets(self):
        record = await self.submit()
        self.binding = replace(
            self.binding, writes_enabled=False, project_id="", library_parent_id=""
        )
        self.status = "okay"
        record = await self.service.check_status(self.caller, record.submission_id)
        self.assertEqual(record.state, "ready")
        self.assertEqual(self.observed_binding.project_id, "project-1")
        self.assertEqual(self.observed_binding.library_parent_id, "folder:1")

    async def test_poll_deadline_after_revocation_does_not_return_cached_intent(self):
        record = await self.submit()
        now = [10.0]

        async def sleep(delay):
            self.binding = replace(self.binding, scope=replace(self.binding.scope, enabled=False))
            now[0] += delay

        result = await self.service.poll_status(
            self.caller,
            record.submission_id,
            timeout_s=1,
            interval_s=2,
            sleep=sleep,
            monotonic=lambda: now[0],
        )
        self.assertEqual(result.stop_reason, "timeout")
        self.assertIsNone(result.record)
        self.assertEqual(self.calls.count("get"), 1)

    async def test_poll_ledger_failure_is_not_a_retryable_query_failure(self):
        record = await self.submit()
        self.ledger.fail_state = "processing"
        with self.assertRaisesRegex(SubmissionError, "^ledger_unavailable$"):
            await self.service.poll_status(self.caller, record.submission_id, sleep=self.no_sleep)
        self.assertEqual(self.calls.count("get"), 1)

    async def test_poll_requires_accepted_known_document(self):
        self.upload_error = TimeoutError()
        record = await self.submit()
        with self.assertRaisesRegex(SubmissionError, "^document_not_observable$"):
            await self.service.poll_status(self.caller, record.submission_id, sleep=self.no_sleep)
        self.assertEqual(self.calls, ["upload"])

    async def test_poll_deadline_is_checked_after_observation_before_ready_return(self):
        record = await self.submit()
        now = [10.0]
        original = self.service._transport.get_document

        async def get(binding, document_id):
            self.status = "okay"
            result = await original(binding, document_id)
            now[0] += 2
            return result

        self.service._transport.get_document = get
        result = await self.service.poll_status(
            self.caller, record.submission_id, timeout_s=1, monotonic=lambda: now[0]
        )
        self.assertEqual(result.stop_reason, "timeout")
        self.assertIsNone(result.record)
        self.assertEqual(self.ledger.records[record.submission_id].state, "ready")

    async def test_status_revocation_during_save_prevents_record_disclosure(self):
        record = await self.submit()
        entered = asyncio.Event()
        release = asyncio.Event()
        original = self.ledger.save
        self.status = "okay"

        async def save(previous, updated):
            entered.set()
            await release.wait()
            await original(previous, updated)

        self.ledger.save = save
        task = asyncio.create_task(self.service.check_status(self.caller, record.submission_id))
        await entered.wait()
        self.binding = replace(self.binding, scope=replace(self.binding.scope, enabled=False))
        release.set()
        with self.assertRaises(ValueError):
            await task
        self.assertEqual(self.ledger.records[record.submission_id].state, "ready")
        self.assertEqual(self.calls, ["upload", "import", "get"])

    async def test_poll_typed_transport_authorization_errors_stop_without_save(self):
        record = await self.submit()
        original_save = self.ledger.save
        saves = []

        async def save(previous, updated):
            saves.append(updated)
            await original_save(previous, updated)

        self.ledger.save = save
        for reason in ("authorization_changed", "credentials_changed"):
            self.calls.clear()

            async def get(_binding, _document_id, *, reason=reason):
                self.calls.append("get")
                raise MemoryLakeRunnerError(reason)

            self.service._transport.get_document = get
            with (
                self.subTest(reason=reason),
                self.assertRaisesRegex(SubmissionError, "^" + reason + "$"),
            ):
                await self.service.poll_status(
                    self.caller, record.submission_id, sleep=self.no_sleep
                )
            self.assertEqual(self.calls, ["get"])
            self.assertEqual(saves, [])
            self.assertEqual(self.ledger.records[record.submission_id], record)

    async def test_poll_typed_operational_query_failure_remains_reconcilable(self):
        record = await self.submit()
        original = self.service._transport.get_document
        attempts = 0

        async def get(binding, document_id):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise MemoryLakeRunnerError("timeout")
            self.status = "okay"
            return await original(binding, document_id)

        self.service._transport.get_document = get
        result = await self.service.poll_status(
            self.caller, record.submission_id, sleep=self.no_sleep
        )
        self.assertEqual(result.stop_reason, "ready")
        self.assertEqual(result.checks, 2)

    async def test_poll_deadline_handles_runner_typed_cancellation(self):
        record = await self.submit()

        async def get(_binding, _document_id):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise MemoryLakeCancelledError() from None

        self.service._transport.get_document = get
        result = await self.service.poll_status(self.caller, record.submission_id, timeout_s=0.01)
        self.assertEqual(result.stop_reason, "timeout")
        self.assertIsNone(result.record)

    async def test_poll_typed_external_cancellation_still_propagates(self):
        record = await self.submit()
        entered = asyncio.Event()

        async def get(_binding, _document_id):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise MemoryLakeCancelledError() from None

        self.service._transport.get_document = get
        task = asyncio.create_task(self.service.poll_status(self.caller, record.submission_id))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_poll_concurrent_external_cancel_is_not_swallowed_by_deadline(self):
        record = await self.submit()
        deadline_received = asyncio.Event()

        async def get(_binding, _document_id):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                deadline_received.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    raise MemoryLakeCancelledError() from None

        self.service._transport.get_document = get
        task = asyncio.create_task(
            self.service.poll_status(self.caller, record.submission_id, timeout_s=0.01)
        )
        await deadline_received.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_poll_nonzero_status_and_nonretryable_typed_errors_stop(self):
        record = await self.submit()
        original_record = self.ledger.records[record.submission_id]
        for reason in (None, "output_limit", "spawn_failed", "unknown private diagnostic"):
            self.calls.clear()

            async def get(_binding, _document_id, *, reason=reason):
                self.calls.append("get")
                if reason is not None:
                    raise MemoryLakeRunnerError(reason)
                return ProcessResult(1, b'{"id":"doc-1","status":"okay"}', b"private")

            self.service._transport.get_document = get
            with (
                self.subTest(reason=reason),
                self.assertRaisesRegex(SubmissionError, "^status_query_failed$"),
            ):
                await self.service.poll_status(
                    self.caller, record.submission_id, sleep=self.no_sleep
                )
            self.assertEqual(self.calls, ["get"])
            self.assertEqual(self.ledger.records[record.submission_id], original_record)
