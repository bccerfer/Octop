"""Offline CLI adapter tests: temporary Python executable, synthetic bytes only."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import tempfile
import threading
import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from tests.support.memorylake import offline_test_guard, protected_fake_python

from octop.infra.connectors.memorylake.cli_transport import MemoryLakeCliTransport
from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope
from octop.infra.connectors.memorylake.read_service import ReadCredentials
from octop.infra.connectors.memorylake.runner import (
    MemoryLakeCancelledError,
    MemoryLakeRunnerError,
    ProcessResult,
)
from octop.infra.connectors.memorylake.submissions import (
    FileSnapshot,
    SubmissionIntent,
    SubmissionRecord,
    WriteBinding,
)


@unittest.skipUnless(os.name == "posix", "private sessions require POSIX")
class CliTransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(offline_test_guard())
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.audit = self.root / "audit.json"
        self.binary = self.root / "fake-cli"
        self.binary.write_text(
            protected_fake_python(
                f"#!{sys.executable}\n"
                "import sys,os,json,pathlib,stat\n"
                f"audit=pathlib.Path({str(self.audit)!r})\n"
                "args=sys.argv[1:]; root=pathlib.Path(os.environ['MEMORYLAKE_CONFIG_DIR'])\n"
                "data={'args':args,'cwd':str(root),'profile':(root/'credentials.toml').read_text()}\n"
                "if 'upload' in args:\n"
                " p=pathlib.Path(args[-1]); data.update(path=str(p),body=p.read_text(),mode=stat.S_IMODE(p.stat().st_mode))\n"
                "audit.write_text(json.dumps(data))\n"
                "if 'upload' in args: print('{\"item_id\":\"file:1\"}')\n"
                "elif 'import' in args:\n"
                ' print(\'{"success_count":1,"failure_count":0,"duplicate_count":0,"details_truncated":false,"details":[{"result":"success","drive_item_id":"file:1","document_id":"doc-1"}]}\'); sys.exit(7)\n'
                'elif \'search\' in args: print(\'{"documents":[{"document_id":"doc-1","project_id":"p","items":[{"text":"synthetic"}]}],"facts":[],"databases":[]}\')\n'
                'else: print(\'{"id":"doc-1","status":"okay"}\')\n'
            )
        )
        self.binary.chmod(0o700)
        self.binding = WriteBinding(
            ReadScope("c", 11, "a", "w", ("p",)),
            "p",
            "folder:1",
            "https://app.memorylake.ai/openapi/memorylake",
            1,
            True,
        )
        self.now = 100.0
        self.credentials = ReadCredentials("dummy-secret", 1)
        self.file = FileSnapshot("attachment-1", "demo.txt", b"synthetic")

        async def load_binding():
            return self.binding

        async def load_credentials():
            return self.credentials

        self.transport = MemoryLakeCliTransport(
            "c",
            executable=self.binary,
            load_binding=load_binding,
            load_credentials=load_credentials,
            clock=lambda: self.now,
        )

    def lease(self, binding, snapshot=None, phase="uploading"):
        from octop.infra.connectors.memorylake.submissions import StepLease

        snapshot = snapshot or self.file
        intent = SubmissionIntent(
            CallerContext(11, "a", 11, 1),
            binding,
            snapshot.attachment_id,
            snapshot.filename,
            hashlib.sha256(snapshot.content).hexdigest(),
            len(snapshot.content),
        )
        record = SubmissionRecord(
            "approval-1",
            intent,
            state=phase,
            revision=2,
            item_id="file:1" if phase == "importing" else None,
        )
        return StepLease(record, phase, record.revision, 200.0)

    async def upload(self, binding, snapshot):
        return await self.transport.upload(binding, snapshot, lease=self.lease(binding, snapshot))

    async def import_file(self, binding, item_id):
        return await self.transport.import_file(
            binding, item_id, lease=self.lease(binding, phase="importing")
        )

    def tearDown(self):
        self.temp.cleanup()

    async def test_upload_exact_argv_private_staging_and_leading_dash_name(self):
        result = await self.upload(self.binding, replace(self.file, filename="--base-url=evil.txt"))
        self.assertEqual(json.loads(result.stdout), {"item_id": "file:1"})
        audit = json.loads(self.audit.read_text())
        staged = Path(audit["path"])
        self.assertEqual(
            audit["args"],
            [
                "--profile",
                "octop",
                "--base-url",
                self.binding.base_url,
                "library",
                "upload",
                "--parent",
                "folder:1",
                "--name=--base-url=evil.txt",
                "--on-conflict",
                "deny",
                "--",
                str(staged),
            ],
        )
        self.assertEqual(staged.parent, Path(audit["cwd"]) / "source")
        self.assertEqual(audit["mode"], 0o600)
        self.assertEqual(audit["body"], "synthetic")
        self.assertFalse(Path(audit["cwd"]).exists())

    async def test_credentials_filename_never_overwrites_profile(self):
        await self.upload(self.binding, replace(self.file, filename="credentials.toml"))
        audit = json.loads(self.audit.read_text())
        self.assertIn("[profiles.octop]", audit["profile"])
        self.assertNotIn("synthetic", audit["profile"])

    async def test_nonzero_import_retains_json_without_wait_or_recursive(self):
        result = await self.import_file(self.binding, "file:1")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(json.loads(result.stdout)["details"][0]["document_id"], "doc-1")
        args = json.loads(self.audit.read_text())["args"]
        self.assertEqual(
            args[4:],
            [
                "project",
                "document",
                "import",
                "--workspace",
                "w",
                "--project",
                "p",
                "--max-files",
                "1",
                "--",
                "file:1",
            ],
        )

    async def test_document_get_exact_scope(self):
        result = await self.transport.get_document(self.binding, "doc-1")
        self.assertEqual(json.loads(result.stdout), {"id": "doc-1", "status": "okay"})
        self.assertEqual(
            json.loads(self.audit.read_text())["args"][4:],
            ["project", "document", "get", "--workspace", "w", "--project", "p", "--", "doc-1"],
        )

    async def test_revocation_while_loading_key_prevents_execution(self):
        original = self.binding

        async def changed():
            self.binding = replace(self.binding, writes_enabled=False)
            return self.credentials

        self.transport._load_credentials = changed
        with self.assertRaises(MemoryLakeRunnerError):
            await self.upload(original, self.file)
        self.assertFalse(self.audit.exists())

    async def test_credential_revision_mismatch_prevents_execution(self):
        self.credentials = replace(self.credentials, revision=2)
        with self.assertRaises(MemoryLakeRunnerError):
            await self.upload(self.binding, self.file)
        self.assertFalse(self.audit.exists())

    async def test_invalid_file_or_endpoint_cannot_execute(self):
        for name in ("../secret", "/host/path", "a\\b", "a\nfile", ".", ".."):
            with self.subTest(name=name), self.assertRaises(MemoryLakeRunnerError):
                await self.upload(self.binding, replace(self.file, filename=name))
        with self.assertRaises(MemoryLakeRunnerError):
            await self.upload(replace(self.binding, base_url="http://localhost"), self.file)
        self.assertFalse(self.audit.exists())

    async def test_cancelled_partial_json_is_redacted_and_tmp_cleaned(self):
        seen = []

        async def cancelled(arguments, *, env, cwd):
            seen.append(cwd)
            raise MemoryLakeCancelledError(
                partial_result=ProcessResult(
                    -9, b'{"item_id":"file:1","debug":"dummy-\\u0073ecret"}', b"dummy-secret"
                )
            )

        with (
            patch.object(self.transport._runner, "run", side_effect=cancelled),
            self.assertRaises(MemoryLakeCancelledError) as raised,
        ):
            await self.upload(self.binding, self.file)
        self.assertEqual(json.loads(raised.exception.partial_result.stdout), {"item_id": "file:1"})
        self.assertEqual(raised.exception.partial_result.stderr, b"")
        self.assertNotIn("dummy-secret", repr(raised.exception))
        self.assertFalse(seen[0].exists())

    async def test_known_key_in_id_is_decoded_and_redacted(self):
        async def reply(*args, **kwargs):
            return ProcessResult(0, b'{"item_id":"dummy-\\u0073ecret"}', b"secret stderr")

        with patch.object(self.transport._runner, "run", side_effect=reply):
            result = await self.upload(self.binding, self.file)
        self.assertNotIn(b"dummy-secret", result.stdout)
        self.assertEqual(result.stderr, b"")

    async def test_invalid_json_never_echoes_raw_body(self):
        async def reply(*args, **kwargs):
            return ProcessResult(1, b"private raw body", b"private stderr")

        with patch.object(self.transport._runner, "run", side_effect=reply):
            result = await self.import_file(self.binding, "file:1")
        self.assertEqual((result.returncode, result.stdout, result.stderr), (1, b"", b""))

    async def test_timeout_partial_ids_survive_without_raw_stderr(self):
        async def timeout(*args, **kwargs):
            raise MemoryLakeRunnerError(
                "timeout",
                partial_result=ProcessResult(-9, b'{"item_id":"file:1"}', b"dummy-secret"),
            )

        with (
            patch.object(self.transport._runner, "run", side_effect=timeout),
            self.assertRaises(MemoryLakeRunnerError) as raised,
        ):
            await self.upload(self.binding, self.file)
        self.assertEqual(str(raised.exception), "timeout")
        self.assertEqual(json.loads(raised.exception.partial_result.stdout), {"item_id": "file:1"})
        self.assertEqual(raised.exception.partial_result.stderr, b"")

    async def test_queued_call_rechecks_revocation(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def blocked(*args, **kwargs):
            calls.append(args)
            entered.set()
            await release.wait()
            return ProcessResult(0, b'{"item_id":"file:1"}', b"")

        original = self.binding
        with patch.object(self.transport._runner, "run", side_effect=blocked):
            first = asyncio.create_task(self.upload(original, self.file))
            await entered.wait()
            second = asyncio.create_task(self.upload(original, self.file))
            await asyncio.sleep(0)
            self.binding = replace(self.binding, writes_enabled=False)
            release.set()
            await first
            with self.assertRaises(MemoryLakeRunnerError):
                await second
        self.assertEqual(len(calls), 1)

    async def test_stage_cancellation_waits_for_worker_before_cleanup(self):
        from octop.infra.connectors.memorylake.cli_transport import _stage

        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        release = threading.Event()
        roots = []

        def blocked(root, snapshot):
            roots.append(root)
            loop.call_soon_threadsafe(entered.set)
            release.wait(2)
            return _stage(root, snapshot)

        with patch("octop.infra.connectors.memorylake.cli_transport._stage", side_effect=blocked):
            task = asyncio.create_task(self.upload(self.binding, self.file))
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(MemoryLakeCancelledError):
                await task
        self.assertFalse(roots[0].exists())
        self.assertFalse(self.audit.exists())

    async def test_rotation_after_staging_stops_execution(self):
        from octop.infra.connectors.memorylake.cli_transport import _stage

        def rotate(root, snapshot):
            path = _stage(root, snapshot)
            self.credentials = ReadCredentials("new-dummy-secret", 1)
            return path

        with (
            patch("octop.infra.connectors.memorylake.cli_transport._stage", side_effect=rotate),
            self.assertRaises(MemoryLakeRunnerError),
        ):
            await self.upload(self.binding, self.file)
        self.assertFalse(self.audit.exists())

    async def test_malformed_nested_receipt_does_not_preserve_arbitrary_body(self):
        async def reply(*args, **kwargs):
            return ProcessResult(0, b'{"item_id":{"private":"upstream-private-body"}}', b"")

        with patch.object(self.transport._runner, "run", side_effect=reply):
            result = await self.upload(self.binding, self.file)
        self.assertNotIn(b"upstream-private-body", result.stdout)

    async def test_loader_exception_message_is_not_exposed(self):
        async def broken():
            raise MemoryLakeRunnerError("private-loader-body")

        self.transport._load_credentials = broken
        with self.assertRaises(MemoryLakeRunnerError) as raised:
            await self.upload(self.binding, self.file)
        self.assertNotIn("private-loader-body", str(raised.exception))

    async def test_missing_lease_cannot_upload(self):
        with self.assertRaises(TypeError):
            await self.transport.upload(self.binding, self.file)
        self.assertFalse(self.audit.exists())

    async def test_lease_fields_and_exact_file_are_enforced(self):
        lease = self.lease(self.binding)
        for invalid in (
            replace(lease, phase="importing"),
            replace(lease, revision=9),
            replace(lease, expires_at=100.0),
            replace(lease, expires_at=float("nan")),
            replace(lease, record=replace(lease.record, state="approved")),
        ):
            with self.subTest(lease=invalid), self.assertRaises(MemoryLakeRunnerError):
                await self.transport.upload(self.binding, self.file, lease=invalid)
        for snapshot in (
            replace(self.file, content=b"changed"),
            replace(self.file, filename="other.txt"),
            replace(self.file, attachment_id="other"),
        ):
            with self.assertRaises(MemoryLakeRunnerError):
                await self.transport.upload(self.binding, snapshot, lease=lease)
        self.assertFalse(self.audit.exists())

    async def test_import_cannot_substitute_unapproved_item(self):
        with self.assertRaises(MemoryLakeRunnerError):
            await self.transport.import_file(
                self.binding, "another-item", lease=self.lease(self.binding, phase="importing")
            )
        self.assertFalse(self.audit.exists())

    async def test_lease_expiry_during_staging_stops_launch(self):
        from octop.infra.connectors.memorylake.cli_transport import _stage

        def expire(root, snapshot):
            path = _stage(root, snapshot)
            self.now = 200.0
            return path

        with (
            patch("octop.infra.connectors.memorylake.cli_transport._stage", side_effect=expire),
            self.assertRaises(MemoryLakeRunnerError),
        ):
            await self.upload(self.binding, self.file)
        self.assertFalse(self.audit.exists())

    async def test_document_observation_allows_write_optout(self):
        self.binding = replace(self.binding, writes_enabled=False)
        result = await self.transport.get_document(self.binding, "doc-1")
        self.assertEqual(json.loads(result.stdout)["status"], "okay")

    async def test_get_observes_original_authorized_project_not_new_default(self):
        self.binding = replace(
            self.binding,
            scope=replace(self.binding.scope, project_ids=("p", "new-default")),
            project_id="new-default",
            library_parent_id="new-folder",
            writes_enabled=False,
        )
        observation = replace(
            self.binding, project_id="p", library_parent_id="old-folder", writes_enabled=True
        )
        result = await self.transport.get_document(observation, "doc-1")
        self.assertEqual(json.loads(result.stdout)["id"], "doc-1")
        args = json.loads(self.audit.read_text())["args"]
        self.assertEqual(args[args.index("--project") + 1], "p")
        with self.assertRaises(MemoryLakeRunnerError):
            await self.transport.get_document(replace(observation, project_id="outside"), "doc-1")

    async def test_cancellation_during_session_cleanup_keeps_completed_receipt(self):
        from octop.infra.connectors.memorylake.session import async_isolated_cli_session

        exiting = asyncio.Event()
        release = asyncio.Event()
        roots = []

        @asynccontextmanager
        async def delayed(key):
            async with async_isolated_cli_session(key) as session:
                roots.append(Path(session.environment["MEMORYLAKE_CONFIG_DIR"]))
                yield session
                exiting.set()
                await release.wait()

        with patch(
            "octop.infra.connectors.memorylake.cli_transport.async_isolated_cli_session", delayed
        ):
            task = asyncio.create_task(self.upload(self.binding, self.file))
            await asyncio.wait_for(exiting.wait(), 2)
            task.cancel()
            with self.assertRaises(MemoryLakeCancelledError) as raised:
                await task
        self.assertEqual(json.loads(raised.exception.partial_result.stdout), {"item_id": "file:1"})
        self.assertFalse(roots[0].exists())

    async def test_queued_lease_expiry_prevents_second_execution(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def blocked(*args, **kwargs):
            calls.append(args)
            entered.set()
            await release.wait()
            return ProcessResult(0, b'{"item_id":"file:1"}', b"")

        with patch.object(self.transport._runner, "run", side_effect=blocked):
            first = asyncio.create_task(self.upload(self.binding, self.file))
            await entered.wait()
            second = asyncio.create_task(self.upload(self.binding, self.file))
            await asyncio.sleep(0)
            self.now = 200.0
            release.set()
            await first
            with self.assertRaisesRegex(MemoryLakeRunnerError, "invalid_step_lease"):
                await second
        self.assertEqual(len(calls), 1)

    async def test_domain_generated_lease_upload_import_reconcile_and_search(self):
        from tests.unit.connectors.test_memorylake_submissions import FakeLedger

        from octop.infra.connectors.memorylake.read_service import (
            MemoryLakeReadService,
            ReadConnection,
        )
        from octop.infra.connectors.memorylake.submissions import Approval, SubmissionService

        caller = CallerContext(11, "a", 11, 1)
        ledger = FakeLedger()
        ledger.binding = lambda: self.binding
        outer = self

        class Source:
            async def read(self, actual_caller, attachment_id):
                if actual_caller != caller or attachment_id != outer.file.attachment_id:
                    raise ValueError("source denied")
                return outer.file

        async def load_binding():
            return self.binding

        service = SubmissionService(
            "c",
            load_binding=load_binding,
            source=Source(),
            ledger=ledger,
            transport=self.transport,
            clock=lambda: self.now,
        )
        intent = await service.prepare(caller, self.file.attachment_id)
        ledger.approvals["approved-domain"] = Approval("approved-domain", intent, 200.0)
        record = await service.submit(caller, self.file.attachment_id, "approved-domain")
        self.assertEqual([lease.phase for lease in ledger.step_grants], ["uploading", "importing"])
        self.assertEqual(record.state, "unknown")
        self.assertTrue(record.import_accepted)
        self.assertEqual((record.item_id, record.document_id), ("file:1", "doc-1"))
        self.assertFalse(Path(json.loads(self.audit.read_text())["cwd"]).exists())

        self.binding = replace(self.binding, writes_enabled=False, credential_revision=2)
        self.credentials = ReadCredentials("rotated-dummy-secret", 2)
        record = await service.check_status(caller, record.submission_id)
        self.assertEqual(record.state, "ready")
        self.assertEqual(ledger.records[record.submission_id], record)

        async def connection():
            return ReadConnection(
                self.binding.scope, self.binding.base_url, self.binding.credential_revision
            )

        async def credentials():
            return self.credentials

        reader = MemoryLakeReadService(
            "c", executable=self.binary, load_connection=connection, load_credentials=credentials
        )
        found = await reader.call(caller, "search_documents", {"query": "synthetic"})
        self.assertEqual(found["data"]["documents"][0]["document_id"], record.document_id)
        self.assertFalse(Path(json.loads(self.audit.read_text())["cwd"]).exists())
