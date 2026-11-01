"""Real SQLite adapter with synthetic bytes and fake transport; no production wiring."""

import asyncio
import hashlib
import json
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard

from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope
from octop.infra.connectors.memorylake.runner import ProcessResult
from octop.infra.connectors.memorylake.submission_storage import (
    DatabaseJobAdmission,
    DatabaseSubmissionLedger,
)
from octop.infra.connectors.memorylake.submissions import (
    Approval,
    FileSnapshot,
    SubmissionCancelledError,
    SubmissionError,
    SubmissionIntent,
    SubmissionService,
    WriteBinding,
)
from octop.infra.db import migrate
from octop.infra.db.pool import SqlitePool
from octop.infra.db.repos.agents import AgentRepo
from octop.infra.db.repos.connectors import ConnectorRepo
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo
from octop.infra.db.repos.users import UserRepo


class SubmissionStorageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.tmp = self.enterContext(TemporaryDirectory())
        self.pool = SqlitePool(Path(self.tmp) / "db")
        self.addCleanup(self.pool.close)
        migrate.run_migrations(self.pool)
        owner = UserRepo(self.pool).create(username="owner", role="user")
        AgentRepo(self.pool).create(agent_id="agent", user_id=owner, name="agent")
        self.connectors = ConnectorRepo(self.pool)
        self.connectors.create(
            instance_id="connector",
            user_id=owner,
            kind="memorylake",
            display_name="ml",
            mcp_server_name="ml",
        )
        self.connectors.upsert_credentials(instance_id="connector", blob=b"synthetic")
        self.binding = WriteBinding(
            ReadScope("connector", owner, "agent", "workspace", ("project",)),
            "project",
            "folder",
            "https://app.memorylake.ai/openapi/memorylake",
            1,
            True,
        )
        MemoryLakeBindingRepo(self.pool).create(
            binding_id="binding",
            connector_id="connector",
            owner_user_id=owner,
            agent_id="agent",
            base_url=self.binding.base_url,
            workspace_id="workspace",
            project_ids_json='["project"]',
            enabled=True,
        )
        self.repo = MemoryLakeSubmissionRepo(self.pool, clock=lambda: 100.0)
        self.repo.replace_policy(
            binding_id="binding",
            expected_revision=0,
            project_id="project",
            library_parent_id="folder",
            enabled=True,
        )
        self.admission = DatabaseJobAdmission()
        self.ledger = DatabaseSubmissionLedger(
            self.repo, binding_id="binding", admission=self.admission
        )
        self.caller = CallerContext(owner, "agent", owner, 1)
        self.snapshot = FileSnapshot("attachment", "synthetic.txt", b"abc")
        self.intent = SubmissionIntent(
            self.caller,
            self.binding,
            "attachment",
            "synthetic.txt",
            hashlib.sha256(b"abc").hexdigest(),
            3,
        )
        self.approval = Approval("approval", self.intent, 200.0)

    async def test_roundtrip_claim_step_and_no_get_lease(self):
        await self.ledger.create_approval(self.approval)
        self.assertEqual(await self.ledger.approval("approval"), self.approval)
        record = await self.ledger.claim("approval", self.intent, 100.0)
        lease = await self.ledger.begin_step(record, "uploading", 100.0)
        self.assertEqual(lease.record.state, "uploading")
        self.assertEqual(await self.ledger.get("approval"), lease.record)
        self.assertFalse(hasattr(await self.ledger.get("approval"), "expires_at"))

    async def test_bad_intent_and_column_json_disagreement_fail_closed(self):
        for intent in (
            replace(self.intent, sha256="bad"),
            replace(self.intent, size=True),
            replace(self.intent, filename="../secret"),
            replace(self.intent, caller=replace(self.caller, is_group=True)),
        ):
            with self.subTest(intent=intent), self.assertRaises(ValueError):
                await self.ledger.create_approval(replace(self.approval, intent=intent))
        await self.ledger.create_approval(self.approval)
        with self.pool.transaction() as conn:
            conn.execute("UPDATE memorylake_submission_approvals SET workspace_id='other'")
        with self.assertRaises(ValueError):
            await self.ledger.approval("approval")

    async def test_claim_and_step_cancel_after_real_commit_save_unknown(self):
        for operation in ("claim", "begin_step"):
            with self.subTest(operation=operation):
                approval = replace(self.approval, approval_id=operation)
                await self.ledger.create_approval(approval)
                record = None
                if operation == "begin_step":
                    record = await self.ledger.claim(operation, self.intent, 100.0)
                committed, release = threading.Event(), threading.Event()
                original = getattr(self.repo, operation)

                def blocked(
                    *args, original=original, committed=committed, release=release, **kwargs
                ):
                    result = original(*args, **kwargs)
                    committed.set()
                    release.wait(3)
                    return result

                with patch.object(self.repo, operation, side_effect=blocked):
                    call = (
                        self.ledger.claim(operation, self.intent, 100.0)
                        if record is None
                        else self.ledger.begin_step(record, "uploading", 100.0)
                    )
                    task = asyncio.create_task(call)
                    try:
                        self.assertTrue(await asyncio.to_thread(committed.wait, 1))
                        task.cancel()
                        await asyncio.sleep(0)
                        self.assertFalse(task.done())
                    finally:
                        release.set()
                        with self.assertRaises(SubmissionCancelledError) as caught:
                            await task
                saved = await self.ledger.get(operation)
                self.assertEqual(saved.state, "unknown")
                self.assertEqual(saved, caught.exception.evidence)
                self.assertEqual(saved.revision, 2 if record is None else 3)

    async def test_real_service_fake_transport_and_read_reconcile(self):
        calls = []

        class Source:
            async def read(inner, caller, attachment):
                return self.snapshot

        class Transport:
            async def upload(inner, binding, snapshot, *, lease):
                calls.append("upload")
                return ProcessResult(0, b'{"item_id":"item:1"}', b"")

            async def import_file(inner, binding, item_id, *, lease):
                calls.append("import")
                return ProcessResult(
                    0,
                    b'{"success_count":1,"failure_count":0,"duplicate_count":0,"details_truncated":false,"details":[{"drive_item_id":"item:1","document_id":"doc:1","result":"success"}]}',
                    b"",
                )

            async def get_document(inner, binding, document_id):
                calls.append("get")
                return ProcessResult(0, b'{"id":"doc:1","status":"okay"}', b"")

        async def binding():
            return self.binding

        service = SubmissionService(
            "connector",
            load_binding=binding,
            source=Source(),
            ledger=self.ledger,
            transport=Transport(),
            clock=lambda: 100.0,
        )
        await self.ledger.create_approval(self.approval)
        record = await service.submit(self.caller, "attachment", "approval")
        self.assertEqual(record.state, "processing")
        self.connectors.upsert_credentials(instance_id="connector", blob=b"rotated")
        self.repo.replace_policy(
            binding_id="binding",
            expected_revision=1,
            project_id="project",
            library_parent_id="folder",
            enabled=False,
        )
        self.binding = replace(self.binding, writes_enabled=False, credential_revision=2)
        self.assertEqual((await service.check_status(self.caller, "approval")).state, "ready")
        self.assertEqual(calls, ["upload", "import", "get"])

    async def test_canonical_decode_rejects_unknown_fields_versions_and_bool_revision(self):
        await self.ledger.create_approval(self.approval)
        original = self.repo.get_approval("approval").intent_json
        mutations = []
        for kind in ("version", "extra", "bool_revision"):
            data = json.loads(original)
            if kind == "version":
                data["version"] = 99
            elif kind == "extra":
                data["intent"]["host_path"] = "/synthetic/private"
            else:
                data["intent"]["caller"]["authorization_revision"] = True
            mutations.append(json.dumps(data, sort_keys=True, separators=(",", ":")))
        for encoded in mutations:
            with self.subTest(encoded=encoded):
                with self.pool.transaction() as conn:
                    conn.execute(
                        "UPDATE memorylake_submission_approvals SET intent_json=?", (encoded,)
                    )
                with self.assertRaises(SubmissionError):
                    await self.ledger.approval("approval")

    async def test_cancelled_committed_step_failed_save_is_not_reported_persisted(self):
        await self.ledger.create_approval(self.approval)
        record = await self.ledger.claim("approval", self.intent, 100.0)
        committed, release = threading.Event(), threading.Event()
        original = self.repo.begin_step

        def blocked(*args, **kwargs):
            result = original(*args, **kwargs)
            committed.set()
            release.wait(3)
            return result

        with (
            patch.object(self.repo, "begin_step", side_effect=blocked),
            patch.object(self.repo, "save", return_value=False),
        ):
            task = asyncio.create_task(self.ledger.begin_step(record, "uploading", 100.0))
            try:
                self.assertTrue(await asyncio.to_thread(committed.wait, 1))
                task.cancel()
                await asyncio.sleep(0)
            finally:
                release.set()
                with self.assertRaises(SubmissionError) as caught:
                    await task
        self.assertEqual(str(caught.exception), "cancelled_commit_recovery_required")
        self.assertEqual(caught.exception.evidence.state, "unknown")
        actual = await self.ledger.get("approval")
        self.assertEqual((actual.state, actual.revision), ("uploading", 2))
        self.assertNotEqual(actual, caught.exception.evidence)

    async def test_second_cancel_during_failed_unknown_save_keeps_safe_evidence(self):
        await self.ledger.create_approval(self.approval)
        record = await self.ledger.claim("approval", self.intent, 100.0)
        committed, release = threading.Event(), threading.Event()
        saving, release_save = threading.Event(), threading.Event()
        original = self.repo.begin_step

        def blocked(*args, **kwargs):
            result = original(*args, **kwargs)
            committed.set()
            release.wait(3)
            return result

        def failed_save(*args, **kwargs):
            saving.set()
            release_save.wait(3)
            raise ValueError("synthetic sensitive backend error")

        with (
            patch.object(self.repo, "begin_step", side_effect=blocked),
            patch.object(self.repo, "save", side_effect=failed_save),
        ):
            task = asyncio.create_task(self.ledger.begin_step(record, "uploading", 100.0))
            try:
                self.assertTrue(await asyncio.to_thread(committed.wait, 1))
                task.cancel()
                release.set()
                self.assertTrue(await asyncio.to_thread(saving.wait, 1))
                task.cancel()
            finally:
                release.set()
                release_save.set()
                with self.assertRaises(SubmissionCancelledError) as caught:
                    await task
        self.assertEqual(caught.exception.evidence.state, "unknown")
        self.assertEqual(caught.exception.evidence.revision, 3)
        self.assertEqual((await self.ledger.get("approval")).state, "uploading")

    async def test_post_commit_approval_read_failure_cannot_drop_cancel_evidence(self):
        for operation in ("claim", "begin_step"):
            with self.subTest(operation=operation):
                await self.ledger.create_approval(replace(self.approval, approval_id=operation))
                record = None
                if operation == "begin_step":
                    record = await self.ledger.claim(operation, self.intent, 100.0)
                committed, release = threading.Event(), threading.Event()
                mutation = getattr(self.repo, operation)
                read = self.repo.get_approval

                def blocked(
                    *args, mutation=mutation, committed=committed, release=release, **kwargs
                ):
                    result = mutation(*args, **kwargs)
                    committed.set()
                    release.wait(3)
                    return result

                def unavailable(*args, committed=committed, read=read, **kwargs):
                    if committed.is_set():
                        raise ValueError("synthetic postcommit database failure")
                    return read(*args, **kwargs)

                with (
                    patch.object(self.repo, operation, side_effect=blocked),
                    patch.object(self.repo, "get_approval", side_effect=unavailable),
                ):
                    call = (
                        self.ledger.claim(operation, self.intent, 100.0)
                        if record is None
                        else self.ledger.begin_step(record, "uploading", 100.0)
                    )
                    task = asyncio.create_task(call)
                    try:
                        self.assertTrue(await asyncio.to_thread(committed.wait, 1))
                        task.cancel()
                        await asyncio.sleep(0)
                    finally:
                        release.set()
                        with self.assertRaises(SubmissionCancelledError) as caught:
                            await task
                saved = await self.ledger.get(operation)
                self.assertEqual(saved, caught.exception.evidence)
                self.assertEqual(saved.state, "unknown")

    def make_service(self):
        class Source:
            async def read(inner, caller, attachment):
                return self.snapshot

        async def binding():
            return self.binding

        return SubmissionService(
            "connector",
            load_binding=binding,
            source=Source(),
            ledger=self.ledger,
            transport=object(),
            clock=lambda: 100.0,
        )

    async def test_service_keeps_committed_cancel_recovery_evidence(self):
        for operation in ("claim", "begin_step"):
            with self.subTest(operation=operation):
                await self.ledger.create_approval(replace(self.approval, approval_id=operation))
                committed, release = threading.Event(), threading.Event()
                mutation = getattr(self.repo, operation)

                def blocked(
                    *args, mutation=mutation, committed=committed, release=release, **kwargs
                ):
                    result = mutation(*args, **kwargs)
                    committed.set()
                    release.wait(3)
                    return result

                with (
                    patch.object(self.repo, operation, side_effect=blocked),
                    patch.object(self.repo, "save", return_value=False),
                ):
                    task = asyncio.create_task(
                        self.make_service().submit(self.caller, "attachment", operation)
                    )
                    try:
                        self.assertTrue(await asyncio.to_thread(committed.wait, 1))
                        task.cancel()
                        await asyncio.sleep(0)
                    finally:
                        release.set()
                        with self.assertRaises(SubmissionError) as caught:
                            await task
                self.assertEqual(str(caught.exception), "cancelled_commit_recovery_required")
                self.assertEqual(caught.exception.evidence.state, "unknown")
                self.assertEqual(
                    caught.exception.evidence.revision, 2 if operation == "claim" else 3
                )

    async def test_poll_timeout_does_not_wait_for_blocked_read_transaction(self):
        await self.ledger.create_approval(self.approval)
        await self.ledger.claim("approval", self.intent, 100.0)
        started, release = threading.Event(), threading.Event()
        original = self.repo.get

        def blocked(*args, **kwargs):
            started.set()
            release.wait(3)
            return original(*args, **kwargs)

        with patch.object(self.repo, "get", side_effect=blocked):
            task = asyncio.create_task(
                self.make_service().poll_status(self.caller, "approval", timeout_s=0.03)
            )
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                done, _ = await asyncio.wait({task}, timeout=0.2)
                self.assertIn(task, done)
                result = task.result()
                self.assertEqual(result.stop_reason, "timeout")
                self.assertIsNone(result.record)
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
                jobs = tuple(self.admission._jobs)
                if jobs:
                    await asyncio.gather(*jobs, return_exceptions=True)

    async def test_cancelled_reads_hold_shared_job_quota_until_real_completion(self):
        await self.ledger.create_approval(self.approval)
        await self.ledger.claim("approval", self.intent, 100.0)
        for method in ("get", "get_approval"):
            with self.subTest(method=method):
                admission = DatabaseJobAdmission(max_jobs=1)
                ledger = DatabaseSubmissionLedger(
                    self.repo, binding_id="binding", admission=admission
                )
                other = DatabaseSubmissionLedger(
                    self.repo, binding_id="binding", admission=admission
                )
                started, release = threading.Event(), threading.Event()
                original = getattr(self.repo, method)

                def blocked(*args, original=original, started=started, release=release, **kwargs):
                    started.set()
                    release.wait(3)
                    return original(*args, **kwargs)

                with patch.object(self.repo, method, side_effect=blocked):
                    call = (
                        ledger.get("approval") if method == "get" else ledger.approval("approval")
                    )
                    task = asyncio.create_task(call)
                    jobs = ()
                    try:
                        self.assertTrue(await asyncio.to_thread(started.wait, 1))
                        jobs = tuple(admission._jobs)
                        self.assertEqual(len(jobs), 1)
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                        self.assertFalse(jobs[0].done())
                        for call in (
                            lambda other=other: other.create_approval(
                                replace(self.approval, approval_id="another")
                            ),
                            lambda other=other: other.approval("approval"),
                        ):
                            with self.assertRaisesRegex(
                                SubmissionError, "submission_database_busy"
                            ):
                                await call()
                    finally:
                        release.set()
                        await asyncio.gather(task, return_exceptions=True)
                        await asyncio.gather(*jobs, return_exceptions=True)
                self.assertEqual(len(admission._jobs), 0)
                self.assertEqual((await other.get("approval")).state, "approved")

    async def test_done_job_is_observed_and_pruned_before_callback_runs(self):
        admission = DatabaseJobAdmission(max_jobs=1)
        observed = []

        class ObservedFuture(asyncio.Future):
            def exception(inner):
                observed.append(True)
                return super().exception()

        done = ObservedFuture()
        admission._jobs.add(done)
        done.add_done_callback(lambda future: future.exception())
        done.set_exception(ValueError("synthetic late database failure"))
        # No await: the completion callback cannot have run yet.
        job = admission.submit(lambda: 1)
        try:
            self.assertTrue(observed)
            self.assertNotIn(done, admission._jobs)
        finally:
            await job

    async def test_poll_deadline_waits_for_actual_blocked_save_completion(self):
        """Characterization: deadline is not hard while a durable write settles."""
        await self.ledger.create_approval(self.approval)
        record = await self.ledger.claim("approval", self.intent, 100.0)
        await self.ledger.save(
            record,
            replace(
                record,
                state="unknown",
                revision=record.revision + 1,
                item_id="item:1",
                document_id="doc:1",
                import_accepted=True,
            ),
        )

        class Transport:
            async def get_document(inner, binding, document_id):
                return ProcessResult(0, b'{"id":"doc:1","status":"okay"}', b"")

        service = self.make_service()
        service._transport = Transport()
        started, release = threading.Event(), threading.Event()
        original = self.repo.save

        def blocked(*args, **kwargs):
            started.set()
            release.wait(3)
            return original(*args, **kwargs)

        with patch.object(self.repo, "save", side_effect=blocked):
            task = asyncio.create_task(service.poll_status(self.caller, "approval", timeout_s=0.03))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                done, _ = await asyncio.wait({task}, timeout=0.1)
                self.assertNotIn(task, done)
                self.assertEqual(len(self.admission._jobs), 1)
                self.assertFalse(next(iter(self.admission._jobs)).done())
            finally:
                release.set()
                result = await task
        self.assertEqual(result.stop_reason, "timeout")
        self.assertIsNone(result.record)
        self.assertEqual((await self.ledger.get("approval")).state, "ready")

    async def test_v2_intent_does_not_silently_upgrade_legacy_approval(self):
        await self.ledger.create_approval(self.approval)
        row = self.repo.get_approval("approval")
        payload = json.loads(row.intent_json)
        self.assertEqual(payload["version"], 2)
        record = await self.ledger.claim("approval", self.intent, 100.0)
        saved = replace(
            record,
            state="unknown",
            revision=record.revision + 1,
            item_id="item:legacy",
            document_id="document:legacy",
            import_accepted=True,
        )
        await self.ledger.save(record, saved)
        payload["version"] = 1
        del payload["intent"]["binding"]["policy_revision"]
        legacy_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self.pool.transaction() as conn:
            conn.execute(
                "UPDATE memorylake_submission_approvals SET intent_json=? WHERE approval_id=?",
                (legacy_json, "approval"),
            )
        with self.assertRaises(SubmissionError):
            await self.ledger.get("approval")
        with self.assertRaises(SubmissionError):
            await self.ledger.claim("approval", self.intent, 100.0)
        # Unsupported experimental history is retained, not migrated, deleted or replayed.
        self.assertEqual(self.repo.get_approval("approval").intent_json, legacy_json)
        raw = self.repo.get("approval")
        self.assertEqual(
            (raw.state, raw.item_id, raw.document_id), ("unknown", "item:legacy", "document:legacy")
        )

    async def test_admission_close_rejects_new_reads_writes_and_requires_closed_drain(self):
        await self.ledger.create_approval(self.approval)
        with self.assertRaises(SubmissionError):
            await self.admission.drain()
        self.admission.close()
        self.admission.close()
        with self.assertRaisesRegex(SubmissionError, "submission_database_closed"):
            await self.ledger.approval("approval")
        with self.assertRaisesRegex(SubmissionError, "submission_database_closed"):
            await self.ledger.create_approval(replace(self.approval, approval_id="other"))
        await self.admission.drain()
        await self.admission.aclose()

    async def test_admission_drain_waits_for_cancelled_read_actual_future_and_late_error(self):
        await self.ledger.create_approval(self.approval)
        for fail in (False, True):
            with self.subTest(fail=fail):
                admission = DatabaseJobAdmission(max_jobs=1)
                ledger = DatabaseSubmissionLedger(
                    self.repo, binding_id="binding", admission=admission
                )
                started, release = threading.Event(), threading.Event()
                original = self.repo.get_approval

                def blocked(
                    *args, original=original, started=started, release=release, fail=fail, **kwargs
                ):
                    started.set()
                    release.wait(3)
                    if fail:
                        raise ValueError("synthetic sensitive late read failure")
                    return original(*args, **kwargs)

                with patch.object(self.repo, "get_approval", side_effect=blocked):
                    reader = asyncio.create_task(ledger.approval("approval"))
                    drain = None
                    jobs = ()
                    try:
                        self.assertTrue(await asyncio.to_thread(started.wait, 1))
                        jobs = tuple(admission._jobs)
                        reader.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await reader
                        admission.close()
                        drain = asyncio.create_task(admission.drain())
                        await asyncio.sleep(0)
                        self.assertFalse(drain.done())
                        self.assertFalse(jobs[0].done())
                    finally:
                        release.set()
                        await asyncio.gather(reader, return_exceptions=True)
                        if drain is not None:
                            await drain
                        await asyncio.gather(*jobs, return_exceptions=True)
                self.assertFalse(admission._jobs)
                await admission.aclose()

    async def test_cancelled_aclose_does_not_cancel_executor_work(self):
        admission = DatabaseJobAdmission(max_jobs=1)
        started, release = threading.Event(), threading.Event()

        def blocked():
            started.set()
            release.wait(3)
            return "completed"

        future = admission.submit(blocked)
        actual = next(iter(admission._jobs))
        closer = None
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            closer = asyncio.create_task(admission.aclose())
            await asyncio.sleep(0)
            closer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await closer
            self.assertFalse(future.done())
            self.assertIn(actual, admission._jobs)
            with self.assertRaisesRegex(SubmissionError, "submission_database_closed"):
                admission.submit(lambda: None)
        finally:
            release.set()
            if closer is not None:
                await asyncio.gather(closer, return_exceptions=True)
            self.assertEqual(await future, "completed")
        await admission.drain()
        self.assertFalse(admission._jobs)

    async def test_cancel_returned_future_keeps_actual_worker_occupied_and_draining(self):
        admission = DatabaseJobAdmission(max_jobs=1)
        started, release = threading.Event(), threading.Event()

        def blocked():
            started.set()
            release.wait(3)

        view = admission.submit(blocked)
        actual = tuple(admission._jobs)
        drain = None
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            view.cancel()
            await asyncio.sleep(0)
            self.assertFalse(actual[0].done())
            with self.assertRaisesRegex(SubmissionError, "submission_database_busy"):
                admission.submit(lambda: None)
            admission.close()
            drain = asyncio.create_task(admission.drain())
            await asyncio.sleep(0)
            self.assertFalse(drain.done())
        finally:
            release.set()
            await asyncio.gather(*actual, return_exceptions=True)
            if drain is not None:
                await drain

    async def test_abandoned_shield_view_observes_sensitive_late_error_without_logging(self):
        import gc

        admission = DatabaseJobAdmission(max_jobs=1)
        started, release = threading.Event(), threading.Event()

        def blocked():
            started.set()
            release.wait(3)
            raise ValueError("synthetic-sensitive-database-body")

        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()
        errors = []
        loop.set_exception_handler(lambda loop, context: errors.append(context))

        async def caller():
            await asyncio.shield(admission.submit(blocked))

        task = asyncio.create_task(caller())
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            admission.close()
            release.set()
            await admission.drain()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            gc.collect()
            self.assertEqual(len(errors), 0)
        finally:
            release.set()
            admission.close()
            await admission.drain()
            await asyncio.gather(task, return_exceptions=True)
            loop.set_exception_handler(previous)
