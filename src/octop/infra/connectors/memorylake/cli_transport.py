"""Unregistered CLI transport for the approved-submission domain.

This adapter does not issue approval. Only SubmissionService with a transactional
ledger and authenticated caller may supply StepLease. A lease is a trusted-domain
snapshot, not an unforgeable capability. Revocation after its atomic grant cannot
retract the already-granted step; expiration is checked again before launch. Executable version
and hash verification, durable storage and workspace adapters remain deployment
gates. No operation retries, deletes remote data, or invokes authentication CLI.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from octop.infra.connectors.memorylake.contract import _identifier
from octop.infra.connectors.memorylake.read_service import ReadCredentials, _redact
from octop.infra.connectors.memorylake.runner import (
    MemoryLakeCancelledError,
    MemoryLakeRunner,
    MemoryLakeRunnerError,
    ProcessResult,
)
from octop.infra.connectors.memorylake.session import _complete, async_isolated_cli_session
from octop.infra.connectors.memorylake.submissions import (
    _APPROVED_BASE_URLS,
    FileSnapshot,
    StepLease,
    WriteBinding,
)


def _validate_snapshot(snapshot: FileSnapshot) -> None:
    if (
        not isinstance(snapshot, FileSnapshot)
        or type(snapshot.content) is not bytes
        or not 0 < len(snapshot.content) <= 20 * 1024 * 1024
        or not isinstance(snapshot.filename, str)
        or not 0 < len(snapshot.filename) <= 255
        or snapshot.filename in (".", "..")
        or any(char in snapshot.filename for char in "/\\\0\r\n:")
    ):
        raise MemoryLakeRunnerError("invalid_source_snapshot")
    _identifier(snapshot.attachment_id)


def _stage(root: Path, snapshot: FileSnapshot) -> Path:
    directory = root / "source"
    directory.mkdir(mode=0o700)
    # The host filename is fixed, not derived from user input; --name supplies
    # the validated destination filename. It can never overwrite the profile.
    path = directory / "payload"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(snapshot.content)
    return path


def _safe_id(value: object) -> str | None:
    try:
        return _identifier(value)
    except ValueError:
        return None


def _safe_result(result: ProcessResult | None, operation: str, secret: str) -> ProcessResult | None:
    if result is None:
        return None
    stdout = b""
    try:
        payload = _redact(json.loads(result.stdout.decode("utf-8")), secret)
        if isinstance(payload, dict):
            selected: dict[str, object] = {}
            if operation == "upload":
                if "item_id" in payload:
                    selected["item_id"] = _safe_id(payload["item_id"])
            elif operation == "get":
                if "id" in payload:
                    selected["id"] = _safe_id(payload["id"])
                if "status" in payload:
                    status = payload["status"]
                    selected["status"] = status if status in ("pending", "running", "okay", "error") else None
            else:
                for key in ("success_count", "failure_count", "duplicate_count"):
                    if key in payload:
                        value = payload[key]
                        selected[key] = value if type(value) is int and value >= 0 else None
                if "details_truncated" in payload:
                    value = payload["details_truncated"]
                    selected["details_truncated"] = value if type(value) is bool else None
                if "details" in payload:
                    details = payload["details"]
                    selected["details"] = [
                        {
                            "result": item.get("result") if item.get("result") in ("success", "duplicate", "failed") else None,
                            "drive_item_id": _safe_id(item.get("drive_item_id")),
                            "document_id": _safe_id(item.get("document_id")),
                        } if isinstance(item, dict) else None
                        for item in details
                    ] if isinstance(details, list) else None
            encoded = json.dumps(selected, allow_nan=False).encode("utf-8")
            if len(encoded) <= 1024 * 1024:
                stdout = encoded
    except (UnicodeError, ValueError, RecursionError):
        pass
    return ProcessResult(result.returncode, stdout, b"")


class MemoryLakeCliTransport:
    """Per-connection admission; loaders must return fresh atomic snapshots.

    Revocation cannot retract requests already sent. Credential rotation must
    increase binding.credential_revision; the adapter also compares the freshly
    loaded key before launch. Binding is checked last after all staging awaits.
    """

    def __init__(
        self,
        connector_id: str,
        *,
        executable: Path,
        load_binding: Callable[[], Awaitable[WriteBinding]],
        load_credentials: Callable[[], Awaitable[ReadCredentials]],
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._connector_id = _identifier(connector_id)
        self._load_binding = load_binding
        self._load_credentials = load_credentials
        self._runner = MemoryLakeRunner(executable)
        self._gate = asyncio.Lock()
        self._clock = clock

    async def _fresh(self, expected: WriteBinding, *, write: bool) -> None:
        current = await self._load_binding()
        if not isinstance(current, WriteBinding) or not isinstance(expected, WriteBinding):
            raise MemoryLakeRunnerError("authorization_changed")
        same_scope = current == expected if write else (
            current.scope == expected.scope
            and current.base_url == expected.base_url
            and current.credential_revision == expected.credential_revision
        )
        if (
            not same_scope
            or current.scope.connector_id != self._connector_id
            or current.scope.enabled is not True
            or (write and current.writes_enabled is not True)
            or current.base_url not in _APPROVED_BASE_URLS
            or type(current.credential_revision) is not int
            or current.credential_revision < 1
            or expected.project_id not in current.scope.project_ids
        ):
            raise MemoryLakeRunnerError("authorization_changed")
        for value in (current.scope.workspace_id, expected.project_id):
            _identifier(value)
        if write:
            _identifier(current.library_parent_id)

    async def _credentials(self, binding: WriteBinding) -> ReadCredentials:
        credentials = await self._load_credentials()
        if (
            not isinstance(credentials, ReadCredentials)
            or type(credentials.revision) is not int
            or credentials.revision != binding.credential_revision
            or not isinstance(credentials.api_key, str)
            or not credentials.api_key.strip()
            or "\0" in credentials.api_key
            or len(credentials.api_key) > 8192
        ):
            raise MemoryLakeRunnerError("credentials_changed")
        return ReadCredentials(credentials.api_key.strip(), credentials.revision)

    def _lease(
        self, binding: WriteBinding, operation: str, lease: StepLease | None,
        snapshot: FileSnapshot | None, identifier: str | None,
    ) -> None:
        if operation == "get":
            return
        expected_phase = "uploading" if operation == "upload" else "importing"
        if (
            not isinstance(lease, StepLease)
            or lease.phase != expected_phase
            or type(lease.revision) is not int
            or lease.revision < 1
            or type(lease.record.revision) is not int
            or lease.revision != lease.record.revision
            or lease.record.state != expected_phase
            or lease.record.intent.binding != binding
            or type(lease.expires_at) not in (int, float)
            or not math.isfinite(lease.expires_at)
            or self._clock() >= lease.expires_at
        ):
            raise MemoryLakeRunnerError("invalid_step_lease")
        if operation == "upload":
            assert snapshot is not None
            intent = lease.record.intent
            if (
                snapshot.attachment_id != intent.attachment_id
                or snapshot.filename != intent.filename
                or len(snapshot.content) != intent.size
                or hashlib.sha256(snapshot.content).hexdigest() != intent.sha256
            ):
                raise MemoryLakeRunnerError("lease_snapshot_mismatch")
        elif identifier != lease.record.item_id:
            raise MemoryLakeRunnerError("lease_item_mismatch")

    async def upload(
        self, binding: WriteBinding, snapshot: FileSnapshot, *, lease: StepLease
    ) -> ProcessResult:
        _validate_snapshot(snapshot)
        return await self._run(binding, "upload", snapshot=snapshot, lease=lease)

    async def import_file(
        self, binding: WriteBinding, item_id: str, *, lease: StepLease
    ) -> ProcessResult:
        return await self._run(binding, "import", identifier=_identifier(item_id), lease=lease)

    async def get_document(self, binding: WriteBinding, document_id: str) -> ProcessResult:
        return await self._run(binding, "get", identifier=_identifier(document_id))

    async def _run(
        self,
        binding: WriteBinding,
        operation: str,
        *,
        snapshot: FileSnapshot | None = None,
        identifier: str | None = None,
        lease: StepLease | None = None,
    ) -> ProcessResult:
        async with self._gate:
            result = None
            key = ""
            try:
                await self._fresh(binding, write=operation != "get")
                self._lease(binding, operation, lease, snapshot, identifier)
                credentials = await self._credentials(binding)
                key = credentials.api_key
                async with async_isolated_cli_session(key) as session:
                    root = Path(session.environment["MEMORYLAKE_CONFIG_DIR"])
                    if operation == "upload":
                        assert snapshot is not None
                        staged, cancelled = await _complete(
                            asyncio.get_running_loop().run_in_executor(None, _stage, root, snapshot)
                        )
                        if cancelled is not None:
                            raise cancelled
                        args = ("library", "upload", "--parent", binding.library_parent_id,
                                "--name=" + snapshot.filename, "--on-conflict", "deny", "--", str(staged))
                    else:
                        assert identifier is not None
                        args = ("project", "document", operation, "--workspace", binding.scope.workspace_id,
                                "--project", binding.project_id)
                        if operation == "import":
                            args += ("--max-files", "1")
                        args += ("--", identifier)
                    if await self._credentials(binding) != credentials:
                        raise MemoryLakeRunnerError("credentials_changed")
                    await self._fresh(binding, write=operation != "get")
                    self._lease(binding, operation, lease, snapshot, identifier)
                    result = await self._runner.run(
                        ("--profile", session.profile, "--base-url", binding.base_url, *args),
                        env=session.environment,
                        cwd=root,
                    )
                safe = _safe_result(result, operation, key)
                assert safe is not None
                return safe
            except asyncio.CancelledError as error:
                partial = error.partial_result if isinstance(error, MemoryLakeCancelledError) else result
                raise MemoryLakeCancelledError(partial_result=_safe_result(partial, operation, key)) from None
            except MemoryLakeRunnerError as error:
                reason = str(error)
                if reason not in {"timeout", "output_limit", "spawn_failed", "authorization_changed", "credentials_changed", "invalid_step_lease", "lease_snapshot_mismatch", "lease_item_mismatch"}:
                    reason = "cli_transport_failed"
                raise MemoryLakeRunnerError(
                    reason, partial_result=_safe_result(error.partial_result or result, operation, key)
                ) from None
            except Exception:
                raise MemoryLakeRunnerError(
                    "cli_transport_failed", partial_result=_safe_result(result, operation, key)
                ) from None
