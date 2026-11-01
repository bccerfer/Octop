"""Real-DB ledger adapter, not itself an approval UI or remote writer.

Only trusted server code may create consent. Canonical intent binds the immutable
file digest/size, not a host path; bytes/credentials are never stored here. Identity
ABA, attachment authorization and retention policy remain production gates.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Callable
from dataclasses import asdict, fields, replace
from typing import Any, Literal, TypeVar, cast

from octop.infra.connectors.memorylake.contract import (
    CallerContext,
    ReadScope,
    _authorize,
    _identifier,
)
from octop.infra.connectors.memorylake.submissions import (
    _APPROVED_BASE_URLS,
    Approval,
    State,
    StepLease,
    SubmissionCancelledError,
    SubmissionError,
    SubmissionIntent,
    SubmissionRecord,
    WriteBinding,
)
from octop.infra.db.repos.memorylake_submissions import (
    ApprovalRow,
    MemoryLakeSubmissionRepo,
    StepGrantRow,
    SubmissionRow,
)

_T = TypeVar("_T")


def _validate(intent: SubmissionIntent) -> None:
    _authorize(intent.binding.scope, intent.caller)
    binding = intent.binding
    if (
        binding.base_url not in _APPROVED_BASE_URLS
        or binding.writes_enabled is not True
        or type(binding.credential_revision) is not int
        or binding.credential_revision < 1
        or type(binding.policy_revision) is not int
        or binding.policy_revision < 1
        or _identifier(binding.project_id) not in binding.scope.project_ids
        or type(intent.size) is not int
        or not 0 < intent.size <= 20 * 1024 * 1024
        or not isinstance(intent.sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", intent.sha256) is None
        or not isinstance(intent.filename, str)
        or not 0 < len(intent.filename) <= 255
        or intent.filename in (".", "..")
        or any(c in intent.filename for c in "/\\\0\r\n:")
        or len(set(binding.scope.project_ids)) != len(binding.scope.project_ids)
    ):
        raise SubmissionError("invalid_stored_intent")
    _identifier(binding.library_parent_id)
    _identifier(intent.attachment_id)


def _encode(intent: SubmissionIntent) -> str:
    try:
        _validate(intent)
        return json.dumps(
            {"version": 2, "intent": asdict(intent)},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except Exception:
        raise SubmissionError("invalid_stored_intent") from None


def _shape(value: Any, cls: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {f.name for f in fields(cls)}:
        raise SubmissionError("invalid_stored_intent")
    return dict(value)


def _decode(row: ApprovalRow, binding_id: str) -> Approval:
    try:
        data = json.loads(row.intent_json)
        if (
            not isinstance(data, dict)
            or set(data) != {"version", "intent"}
            or type(data["version"]) is not int
            or data["version"] != 2
        ):
            raise ValueError
        values = _shape(data["intent"], SubmissionIntent)
        caller = CallerContext(**_shape(values["caller"], CallerContext))
        binding_values = _shape(values["binding"], WriteBinding)
        scope_values = _shape(binding_values["scope"], ReadScope)
        if not isinstance(scope_values["project_ids"], list):
            raise ValueError
        scope_values["project_ids"] = tuple(scope_values["project_ids"])
        binding_values["scope"] = ReadScope(**scope_values)
        binding = WriteBinding(**binding_values)
        values.update(caller=caller, binding=binding)
        intent = SubmissionIntent(**values)
        if _encode(intent) != row.intent_json:
            raise ValueError
        if (
            row.binding_id != binding_id
            or row.approver_user_id != caller.user_id
            or row.caller_user_id != caller.user_id
            or row.agent_id != caller.agent_id
            or row.connector_id != binding.scope.connector_id
            or row.authorization_revision != binding.scope.authorization_revision
            or row.credential_revision != binding.credential_revision
            or row.base_url != binding.base_url
            or row.workspace_id != binding.scope.workspace_id
            or row.project_id != binding.project_id
            or row.library_parent_id != binding.library_parent_id
            or type(row.policy_revision) is not int
            or row.policy_revision < 1
            or row.policy_revision != binding.policy_revision
            or type(row.expires_at) not in (int, float)
            or not math.isfinite(row.expires_at)
        ):
            raise ValueError
        _identifier(row.approval_id)
        return Approval(row.approval_id, intent, row.expires_at, row.revoked)
    except Exception:
        raise SubmissionError("invalid_stored_approval") from None


class DatabaseJobAdmission:
    """Server-owned executor quota shared by every submission ledger instance.

    Occupancy lasts until the actual executor Future finishes, even when a read
    caller cancels. The optional application factory reuses one instance;
    this is not a cross-worker quota or a database statement timeout. Shutdown
    must first finish active submission operations (including their outcome saves),
    THEN close admission and drain actual executor jobs before closing the DB pool.
    """

    def __init__(self, *, max_jobs: int = 64) -> None:
        if type(max_jobs) is not int or not 1 <= max_jobs <= 1024:
            raise SubmissionError("invalid_database_job_limit")
        self._max_jobs = max_jobs
        self._jobs: set[asyncio.Future[Any]] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False

    def _owner_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise SubmissionError("database_event_loop_changed")
        self._loop = loop
        return loop

    def close(self) -> None:
        """Reject future jobs; do not cancel workers or close the database pool.

        Calling this during an active submission can deny its required follow-up
        save. The owner runtime must drain its operations BEFORE closing admission.
        """
        self._owner_loop()
        self._closed = True

    async def drain(self) -> None:
        """Wait for actual jobs after close; cancellation never cancels workers.

        A caller cancelling drain must await it again before closing the DB pool.
        This intentionally does not invent a hard timeout for running SQL.
        """
        self._owner_loop()
        if not self._closed:
            raise SubmissionError("database_admission_not_closed")
        jobs = tuple(self._jobs)
        if jobs:
            await asyncio.shield(asyncio.gather(*jobs, return_exceptions=True))
            self._jobs.difference_update(jobs)

    async def aclose(self) -> None:
        self.close()
        await self.drain()

    def submit(self, operation: Callable[[], _T]) -> asyncio.Future[_T]:
        loop = self._owner_loop()
        if self._closed:
            raise SubmissionError("submission_database_closed")
        # A completed Future may still have its done callback queued.
        for job in tuple(self._jobs):
            if job.done():
                self._jobs.discard(job)
                if not job.cancelled():
                    job.exception()
        if len(self._jobs) >= self._max_jobs:
            raise SubmissionError("submission_database_busy")
        future = loop.run_in_executor(None, operation)
        self._jobs.add(future)

        def finished(done: asyncio.Future[_T]) -> None:
            self._jobs.discard(done)
            if not done.cancelled():
                done.exception()  # Observe late failure without logging its body.

        future.add_done_callback(finished)
        # The public view may be cancelled without cancelling the actual worker.
        # Observe both layers: an abandoned shield(view) must not log late DB bodies.
        view = asyncio.shield(future)

        def observe_view(done: asyncio.Future[_T]) -> None:
            if not done.cancelled():
                done.exception()

        view.add_done_callback(observe_view)
        return view


class DatabaseSubmissionLedger:
    def __init__(
        self,
        repo: MemoryLakeSubmissionRepo,
        *,
        binding_id: str,
        admission: DatabaseJobAdmission,
    ) -> None:
        self._repo = repo
        self._binding_id = _identifier(binding_id)
        self._admission = admission

    def _submit(self, operation: Callable[[], _T]) -> asyncio.Future[_T]:
        return self._admission.submit(operation)

    async def _read_call(self, operation: Callable[[], _T]) -> _T:
        future = self._submit(operation)
        try:
            return await asyncio.shield(future)
        except Exception:
            raise SubmissionError("submission_database_unavailable") from None

    async def _complete(self, operation: Callable[[], _T]) -> tuple[_T, bool]:
        """Cancellation never abandons a database transaction's actual Future.

        Thus a caller deadline is NOT a hard bound while a write settles. A
        production background-settle alternative requires explicit ownership,
        durable recovery and shutdown draining; none is registered here.
        """
        future = self._submit(operation)
        cancelled = False
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        try:
            result = future.result()
        except Exception:
            if cancelled:
                raise asyncio.CancelledError("submission_database_cancelled") from None
            raise SubmissionError("submission_database_unavailable") from None
        return result, cancelled

    async def _call(self, operation: Callable[[], _T]) -> _T:
        result, cancelled = await self._complete(operation)
        if cancelled:
            raise asyncio.CancelledError("submission_database_cancelled")
        return result

    def _approval(self, approval_id: str) -> Approval:
        row = self._repo.get_approval(approval_id)
        if row is None or row.approval_id != approval_id:
            raise SubmissionError("approval_unavailable")
        return _decode(row, self._binding_id)

    def _record(self, row: SubmissionRow | None) -> SubmissionRecord:
        if row is None or row.submission_id != row.approval_id:
            raise SubmissionError("submission_unavailable")
        return self._map_record(row, self._approval(row.approval_id).intent)

    @staticmethod
    def _map_record(row: SubmissionRow, intent: SubmissionIntent) -> SubmissionRecord:
        """Pure mapping: never perform fallible DB I/O after a mutation commits."""
        if row.submission_id != row.approval_id:
            raise SubmissionError("invalid_stored_submission")
        states = {
            "approved",
            "uploading",
            "uploaded",
            "importing",
            "processing",
            "ready",
            "failed",
            "unknown",
        }
        if row.state not in states or type(row.revision) is not int or row.revision < 1:
            raise SubmissionError("invalid_stored_submission")
        for identifier in (row.item_id, row.document_id):
            if identifier is not None:
                _identifier(identifier)
        return SubmissionRecord(
            row.submission_id,
            intent,
            cast(State, row.state),
            row.revision,
            row.item_id,
            row.document_id,
            row.import_accepted,
        )

    async def create_approval(self, approval: Approval) -> None:
        """Trusted-only: caller must already have authenticated the real approver."""
        _identifier(approval.approval_id)
        encoded = _encode(approval.intent)
        if (
            approval.revoked is not False
            or type(approval.expires_at) not in (int, float)
            or not math.isfinite(approval.expires_at)
        ):
            raise SubmissionError("invalid_approval")

        def create() -> None:
            policy = self._repo.get_policy(self._binding_id)
            if policy is None or policy.revision != approval.intent.binding.policy_revision:
                raise SubmissionError("write_policy_unavailable")
            intent, binding = approval.intent, approval.intent.binding
            self._repo.create_approval(
                approval_id=approval.approval_id,
                approver_user_id=intent.caller.user_id,
                binding_id=self._binding_id,
                connector_id=binding.scope.connector_id,
                caller_user_id=intent.caller.user_id,
                agent_id=intent.caller.agent_id,
                authorization_revision=binding.scope.authorization_revision,
                policy_revision=binding.policy_revision,
                credential_revision=binding.credential_revision,
                base_url=binding.base_url,
                workspace_id=binding.scope.workspace_id,
                project_id=binding.project_id,
                library_parent_id=binding.library_parent_id,
                intent_json=encoded,
                expires_at=approval.expires_at,
            )

        await self._call(create)

    async def approval(self, approval_id: str) -> Approval:
        _identifier(approval_id)
        row = await self._read_call(lambda: self._repo.get_approval(approval_id))
        if row is None:
            raise SubmissionError("approval_unavailable")
        if row.approval_id != approval_id:
            raise SubmissionError("invalid_stored_approval")
        return _decode(row, self._binding_id)

    def _save(self, previous: SubmissionRecord, updated: SubmissionRecord) -> None:
        if (
            updated.submission_id != previous.submission_id
            or updated.intent != previous.intent
            or updated.revision != previous.revision + 1
        ):
            raise SubmissionError("invalid_submission_update")
        if not self._repo.save(
            previous.submission_id,
            expected_revision=previous.revision,
            expected_state=previous.state,
            expected_intent_json=_encode(previous.intent),
            state=updated.state,
            item_id=updated.item_id,
            document_id=updated.document_id,
            import_accepted=updated.import_accepted,
        ):
            raise SubmissionError("submission_cas_conflict")

    async def _cancelled_commit(self, record: SubmissionRecord) -> None:
        updated = replace(record, state="unknown", revision=record.revision + 1)
        try:
            await self._complete(lambda: self._save(record, updated))
        except asyncio.CancelledError:
            # Repeated cancellation plus failed persistence still carries the
            # proposed safe evidence, never a claim that unknown was committed.
            raise SubmissionCancelledError(updated) from None
        except Exception:
            # Safe evidence is not a claim that a failed CAS persisted unknown.
            raise SubmissionError("cancelled_commit_recovery_required", updated) from None
        raise SubmissionCancelledError(updated)

    async def claim(
        self, approval_id: str, intent: SubmissionIntent, now: float
    ) -> SubmissionRecord:
        _identifier(approval_id)
        encoded = _encode(intent)

        def claim() -> SubmissionRow:
            if self._approval(approval_id).intent != intent:
                raise SubmissionError("approval_intent_mismatch")
            return self._repo.claim(approval_id, expected_intent_json=encoded)

        row, cancelled = await self._complete(claim)
        record = self._map_record(row, intent)
        if cancelled:
            await self._cancelled_commit(record)
        return record

    async def begin_step(
        self, previous: SubmissionRecord, phase: Literal["uploading", "importing"], now: float
    ) -> StepLease:
        def begin() -> StepGrantRow:
            if self._record(self._repo.get(previous.submission_id)) != previous:
                raise SubmissionError("submission_changed")
            return self._repo.begin_step(
                previous.submission_id,
                expected_revision=previous.revision,
                expected_state=previous.state,
                phase=phase,
            )

        grant, cancelled = await self._complete(begin)
        lease = StepLease(
            self._map_record(grant.record, previous.intent),
            phase,
            grant.record.revision,
            grant.expires_at,
        )
        if cancelled:
            await self._cancelled_commit(lease.record)
        return lease

    async def save(self, previous: SubmissionRecord, updated: SubmissionRecord) -> None:
        await self._call(lambda: self._save(previous, updated))

    async def get(self, submission_id: str) -> SubmissionRecord:
        _identifier(submission_id)
        return await self._read_call(lambda: self._record(self._repo.get(submission_id)))

    async def find(self, submission_id: str) -> SubmissionRecord | None:
        """Only a truly absent row is None; database/canonicalization errors fail closed."""
        _identifier(submission_id)

        def find() -> SubmissionRecord | None:
            row = self._repo.get(submission_id)
            if row is None:
                return None
            if row.submission_id != submission_id:
                raise SubmissionError("invalid_stored_submission")
            return self._record(row)

        return await self._read_call(find)
