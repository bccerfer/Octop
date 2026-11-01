"""Unregistered file-submission domain, without production persistence/auth adapters.

Production gates: transactional ledger, authenticated approval UI, fresh identity
loader, BackendWorkspace attachment adapter, and durable unknown-state recovery.
The protocols are deliberately narrower than a CLI runner. No retries or deletes
are performed; approval consumption must be atomic across processes and restarts.
Ledger.begin_step is the authorization linearization point: one transaction must
check live consent/identity/credentials and durably grant one bounded step lease.
Revocation blocks later grants, not already granted leases or remote side effects.
The fake ledger is not proof that a production transaction provides this guarantee.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Literal, Protocol

from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope, _authorize, _identifier
from octop.infra.connectors.memorylake.documents import parse_document_result, parse_import_result, parse_upload_result
from octop.infra.connectors.memorylake.runner import MemoryLakeCancelledError, MemoryLakeRunnerError, ProcessResult

_APPROVED_BASE_URLS = frozenset({
    "https://app.memorylake.ai/openapi/memorylake",
    "https://app.memorylake.cn/openapi/memorylake",
})

State = Literal["approved", "uploading", "uploaded", "importing", "processing", "ready", "failed", "unknown"]


@dataclass(frozen=True)
class WriteBinding:
    scope: ReadScope
    project_id: str
    library_parent_id: str
    base_url: str
    credential_revision: int
    writes_enabled: bool = False


@dataclass(frozen=True)
class FileSnapshot:
    attachment_id: str
    filename: str
    content: bytes = field(repr=False)


@dataclass(frozen=True)
class SubmissionIntent:
    caller: CallerContext
    binding: WriteBinding
    attachment_id: str
    filename: str
    sha256: str
    size: int


@dataclass(frozen=True)
class Approval:
    approval_id: str
    intent: SubmissionIntent
    expires_at: float
    revoked: bool = False


@dataclass(frozen=True)
class SubmissionRecord:
    submission_id: str
    intent: SubmissionIntent
    state: State = "approved"
    revision: int = 1
    item_id: str | None = None
    document_id: str | None = None
    import_accepted: bool = False


@dataclass(frozen=True)
class StepLease:
    """Internal one-step snapshot, never a user-facing or replayable capability.

    Pass only directly from begin_step to one execution. Do not expose it to a
    model/user, persist it as a reusable token, or replay it from a recovery worker.
    Recovery must reconcile state through newly authorized reads, not rerun a lease.
    """

    record: SubmissionRecord
    phase: Literal["uploading", "importing"]
    revision: int
    expires_at: float


@dataclass(frozen=True)
class PollResult:
    record: SubmissionRecord | None
    checks: int
    stop_reason: Literal["ready", "failed", "check_limit", "timeout"]


class SubmissionError(ValueError):
    """Body-free reason, with safe recovery evidence if persistence failed."""

    def __init__(self, reason: str, evidence: SubmissionRecord | None = None) -> None:
        super().__init__(reason)
        self.evidence = evidence


class SubmissionCancelledError(asyncio.CancelledError):
    def __init__(self, evidence: SubmissionRecord) -> None:
        super().__init__("submission_cancelled_unknown")
        self.evidence = evidence


class WorkspaceSource(Protocol):
    async def read(self, caller: CallerContext, attachment_id: str) -> FileSnapshot:
        """Check attachment ownership and read through BackendWorkspace, not host paths."""
        ...


class SubmissionLedger(Protocol):
    async def approval(self, approval_id: str) -> Approval: ...

    async def claim(self, approval_id: str, intent: SubmissionIntent, now: float) -> SubmissionRecord:
        """Atomically validate exact intent, active/unexpired consent and consume once.

        Reject an already-consumed approval, including concurrent claims. Create
        one durable approved record whose ID is approval_id in the same transaction.
        """
        ...

    async def begin_step(self, previous: SubmissionRecord, phase: Literal["uploading", "importing"], now: float) -> StepLease:
        """Atomically validate live approval, binding, agent rights and key revision.

        In the SAME transaction check exact intent, expiry, unrevoked approval,
        current write opt-in and owner/agent/project access, verify the claimed
        approval, CAS the expected
        approved/uploaded record, persist uploading/importing, and grant one
        short-lived lease. Never grant a second lease for the same step/revision.
        Lease expiry cannot exceed consent expiry. Grants after revocation fail;
        an already granted lease may complete. Requires a real transactional
        authorization/ledger adapter, not separate mutable snapshot reads.
        """
        ...

    async def save(self, previous: SubmissionRecord, updated: SubmissionRecord) -> None:
        """Durable compare-and-swap on record revision, state and immutable intent."""
        ...

    async def get(self, submission_id: str) -> SubmissionRecord: ...


class SubmissionTransport(Protocol):
    """Bind credentials/revision and exact binding.base_url; safely stage snapshot bytes.

    Never look up a different mutable endpoint or infer a target from CLI defaults.
    """

    async def upload(self, binding: WriteBinding, snapshot: FileSnapshot, *, lease: StepLease) -> ProcessResult: ...
    async def import_file(self, binding: WriteBinding, item_id: str, *, lease: StepLease) -> ProcessResult: ...
    async def get_document(self, binding: WriteBinding, document_id: str) -> ProcessResult: ...


class SubmissionService:
    def __init__(
        self,
        connector_id: str,
        *,
        load_binding: Callable[[], Awaitable[WriteBinding]],
        source: WorkspaceSource,
        ledger: SubmissionLedger,
        transport: SubmissionTransport,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._connector_id = _identifier(connector_id)
        self._load_binding = load_binding
        self._source = source
        self._ledger = ledger
        self._transport = transport
        self._clock = clock

    async def _binding(self, caller: CallerContext, *, write: bool = True) -> WriteBinding:
        try:
            binding = await self._load_binding()
        except Exception:
            raise SubmissionError("binding_unavailable") from None
        if not isinstance(binding, WriteBinding):
            raise SubmissionError("binding_unavailable")
        _authorize(binding.scope, caller)
        if (
            binding.scope.connector_id != self._connector_id
            or (write and binding.writes_enabled is not True)
            or binding.base_url not in _APPROVED_BASE_URLS
            or type(binding.credential_revision) is not int
            or binding.credential_revision < 1
            or (write and _identifier(binding.project_id) not in binding.scope.project_ids)
        ):
            raise SubmissionError("write_not_authorized")
        if write:
            _identifier(binding.library_parent_id)
        return binding

    async def _prepare(self, caller: CallerContext, attachment_id: str) -> tuple[SubmissionIntent, FileSnapshot]:
        _identifier(attachment_id)
        binding = await self._binding(caller)
        try:
            snapshot = await self._source.read(caller, attachment_id)
        except Exception:
            raise SubmissionError("source_unavailable") from None
        if (
            not isinstance(snapshot, FileSnapshot)
            or snapshot.attachment_id != attachment_id
            or type(snapshot.content) is not bytes
            or not 0 < len(snapshot.content) <= 20 * 1024 * 1024
            or not isinstance(snapshot.filename, str)
            or not 0 < len(snapshot.filename) <= 255
            or snapshot.filename in (".", "..")
            or any(char in snapshot.filename for char in "/\\\0\r\n:")
        ):
            raise SubmissionError("invalid_source_snapshot")
        intent = SubmissionIntent(caller, binding, attachment_id, snapshot.filename, hashlib.sha256(snapshot.content).hexdigest(), len(snapshot.content))
        return intent, snapshot

    async def prepare(self, caller: CallerContext, attachment_id: str) -> SubmissionIntent:
        """Produce a reviewable intent; only the trusted approval UI may issue consent."""
        intent, _ = await self._prepare(caller, attachment_id)
        return intent

    async def _fresh(self, approval_id: str, intent: SubmissionIntent) -> None:
        # Diagnostic checks only. Only atomic begin_step authorizes a remote write.
        if await self._binding(intent.caller) != intent.binding:
            raise SubmissionError("authorization_changed")
        try:
            approval = await self._ledger.approval(approval_id)
        except Exception:
            raise SubmissionError("approval_unavailable") from None
        if (
            not isinstance(approval, Approval)
            or approval.approval_id != approval_id
            or approval.intent != intent
            or approval.revoked is not False
            or type(approval.expires_at) not in (int, float)
            or not math.isfinite(approval.expires_at)
            or self._clock() >= approval.expires_at
        ):
            raise SubmissionError("approval_invalid")
        # Approval I/O can suspend; do not use the binding checked before it.
        if await self._binding(intent.caller) != intent.binding:
            raise SubmissionError("authorization_changed")
        if self._clock() >= approval.expires_at:
            raise SubmissionError("approval_invalid")

    async def _persist(self, previous: SubmissionRecord, updated: SubmissionRecord) -> SubmissionRecord:
        updated = replace(updated, revision=previous.revision + 1)
        try:
            await self._ledger.save(previous, updated)
        except Exception:
            # A previous uploading/importing state is already durable. Evidence is
            # not proof of persistence and must be reconciled, never re-submitted.
            raise SubmissionError("ledger_unavailable", updated) from None
        return updated

    @staticmethod
    def _receipt(record: SubmissionRecord, result: ProcessResult | None) -> SubmissionRecord:
        if result is None:
            return replace(record, state="unknown")
        try:
            stdout = result.stdout.decode("utf-8")
        except UnicodeError:
            return replace(record, state="unknown")
        if record.state == "uploading":
            upload = parse_upload_result(result.returncode, stdout)
            return replace(record, state="uploaded" if upload.state == "uploaded" else "unknown", item_id=upload.item_id)
        imported = parse_import_result(result.returncode, stdout)
        matching = [item for item in imported.items if item.file_id == record.item_id and item.document_id]
        document_id = matching[0].document_id if len(matching) == 1 else None
        import_accepted = (
            imported.state == "accepted" and imported.complete_report and len(imported.items) == 1
            and len(matching) == 1 and matching[0].result in ("success", "duplicate")
        )
        complete = result.returncode == 0 and import_accepted and not imported.requires_reconciliation
        return replace(record, state="processing" if complete else "unknown", document_id=document_id, import_accepted=import_accepted)

    async def _send(self, record: SubmissionRecord, snapshot: FileSnapshot, lease: StepLease) -> SubmissionRecord:
        result = None
        cancelled = False
        failed = False
        try:
            if record.state == "uploading":
                result = await self._transport.upload(record.intent.binding, snapshot, lease=lease)
            else:
                assert record.item_id is not None
                result = await self._transport.import_file(record.intent.binding, record.item_id, lease=lease)
        except asyncio.CancelledError as error:
            cancelled = True
            if isinstance(error, MemoryLakeCancelledError):
                result = error.partial_result
        except Exception as error:
            failed = True
            if isinstance(error, MemoryLakeRunnerError):
                result = error.partial_result
        updated = self._receipt(record, result)
        if cancelled or failed:
            updated = replace(updated, state="unknown")
        return await self._settle(record, updated, cancelled=cancelled)

    async def _settle(self, record: SubmissionRecord, updated: SubmissionRecord, *, cancelled: bool) -> SubmissionRecord:
        """Finish post-write persistence even if cancellation arrives during save."""
        async def durable(previous: SubmissionRecord, proposed: SubmissionRecord) -> SubmissionRecord:
            nonlocal cancelled
            task = asyncio.create_task(self._persist(previous, proposed))
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
                except SubmissionError:
                    break
            if task.cancelled():
                raise SubmissionError("ledger_unavailable", proposed)
            return task.result()

        try:
            saved = await durable(record, updated)
            if cancelled:
                if saved.state != "unknown":
                    saved = await durable(saved, replace(saved, state="unknown"))
                raise SubmissionCancelledError(saved) from None
            return saved
        except SubmissionError as error:
            evidence = replace(error.evidence or updated, state="unknown")
            if cancelled:
                raise SubmissionCancelledError(evidence) from None
            raise SubmissionError("ledger_unavailable", evidence) from None

    async def submit(self, caller: CallerContext, attachment_id: str, approval_id: str) -> SubmissionRecord:
        _identifier(approval_id)
        intent, snapshot = await self._prepare(caller, attachment_id)
        await self._fresh(approval_id, intent)
        try:
            record = await self._ledger.claim(approval_id, intent, self._clock())
        except Exception:
            raise SubmissionError("approval_claim_denied") from None
        if (
            not isinstance(record, SubmissionRecord)
            or record.submission_id != approval_id
            or record.intent != intent
            or record.state != "approved"
            or record.revision != 1
            or record.item_id is not None
            or record.document_id is not None
            or record.import_accepted is not False
        ):
            raise SubmissionError("invalid_claim_record")
        try:
            phases: tuple[Literal["uploading", "importing"], ...] = ("uploading", "importing")
            for phase in phases:
                await self._fresh(approval_id, intent)
                lease = await self._begin_step(record, phase)
                record = lease.record
                record = await self._send(record, snapshot, lease)
                if record.state == "unknown":
                    break
        except SubmissionCancelledError:
            raise
        except asyncio.CancelledError:
            # Cancellation during a pre-write barrier may leave its transaction's
            # outcome uncertain. Never proceed or retry; retain known prior IDs.
            return await self._settle(record, replace(record, state="unknown"), cancelled=True)
        return record

    async def _begin_step(self, record: SubmissionRecord, phase: Literal["uploading", "importing"]) -> StepLease:
        try:
            lease = await self._ledger.begin_step(record, phase, self._clock())
        except Exception:
            raise SubmissionError("step_not_authorized", record) from None
        expected = replace(record, state=phase, revision=record.revision + 1)
        if (
            not isinstance(lease, StepLease) or lease.record != expected
            or lease.phase != phase or lease.revision != expected.revision
            or type(lease.expires_at) not in (int, float)
            or not math.isfinite(lease.expires_at) or self._clock() >= lease.expires_at
        ):
            raise SubmissionError("invalid_step_lease", expected)
        return lease

    async def _reconciliation_binding(self, caller: CallerContext, record: SubmissionRecord) -> WriteBinding:
        current = await self._binding(caller, write=False)
        original = record.intent.binding
        if (
            record.intent.caller.user_id != caller.user_id
            or record.intent.caller.agent_id != caller.agent_id
            or record.intent.caller.agent_owner_user_id != caller.agent_owner_user_id
            or record.intent.caller.is_group != caller.is_group
            or original.scope.connector_id != current.scope.connector_id
            or original.scope.owner_user_id != current.scope.owner_user_id
            or original.scope.agent_id != current.scope.agent_id
            or original.scope.workspace_id != current.scope.workspace_id
            or original.base_url != current.base_url
            or original.project_id not in current.scope.project_ids
        ):
            raise SubmissionError("submission_access_denied")
        # Read the original project using CURRENT credentials, even when writes
        # were disabled or a different currently authorized target was selected.
        return replace(current, project_id=original.project_id, library_parent_id=original.library_parent_id)

    async def check_status(self, caller: CallerContext, submission_id: str) -> SubmissionRecord:
        """One authorized read-only reconciliation, independent of write opt-in."""
        _identifier(submission_id)
        await self._binding(caller, write=False)
        try:
            record = await self._ledger.get(submission_id)
        except Exception:
            raise SubmissionError("submission_unavailable") from None
        if not isinstance(record, SubmissionRecord) or record.submission_id != submission_id:
            raise SubmissionError("submission_access_denied")
        binding = await self._reconciliation_binding(caller, record)
        if record.state not in ("processing", "unknown", "ready", "failed") or record.document_id is None or not record.import_accepted:
            raise SubmissionError("document_not_observable")
        if record.state in ("ready", "failed"):
            return record
        try:
            result = await self._transport.get_document(binding, record.document_id)
            if result.returncode != 0:
                # Nonzero exit cannot distinguish permission denial from transient
                # failure without machine-readable trusted codes. Stop, don't retry.
                raise SubmissionError("status_query_failed")
            receipt = parse_document_result(result.returncode, result.stdout.decode("utf-8"), record.document_id)
        except MemoryLakeRunnerError as error:
            # Only the trusted transport's typed codes carry authorization meaning;
            # never infer it from arbitrary upstream bodies or generic exceptions.
            reason = str(error)
            if reason in ("authorization_changed", "credentials_changed"):
                raise SubmissionError(reason) from None
            if reason != "timeout":
                raise SubmissionError("status_query_failed") from None
            # Only known timeout failures are eligible for bounded observation.
            updated = replace(record, state="unknown")
        except TimeoutError:
            updated = replace(record, state="unknown")
        except Exception:
            raise SubmissionError("status_query_failed") from None
        else:
            updated = replace(record, state=receipt.state)
        if await self._reconciliation_binding(caller, record) != binding:
            raise SubmissionError("authorization_changed")
        saved = await self._persist(record, updated)
        # Persistence suspends too. Keep the observation for reconciliation, but
        # never disclose it after permission changed while saving the ledger.
        if await self._reconciliation_binding(caller, saved) != binding:
            raise SubmissionError("authorization_changed")
        return saved

    async def poll_status(
        self,
        caller: CallerContext,
        submission_id: str,
        *,
        max_checks: int = 5,
        timeout_s: float = 30.0,
        interval_s: float = 1.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> PollResult:
        """Bounded read-only observations, not a background/recovery worker.

        Every observation reauthorizes through check_status. Transient query errors
        remain unknown and can be observed again; authorization/ledger failures
        propagate immediately. Deadline results intentionally omit cached records,
        whose access might have been revoked while sleeping/querying. Cancellation
        is never swallowed. No upload, import, delete or lease replay occurs.
        The injected clock/sleep are trusted test hooks, not model parameters.
        """
        if (
            type(max_checks) is not int or not 1 <= max_checks <= 100
            or type(timeout_s) not in (int, float) or not math.isfinite(timeout_s)
            or not 0 < timeout_s <= 300
            or type(interval_s) not in (int, float) or not math.isfinite(interval_s)
            or not 0.1 <= interval_s <= 30
        ):
            raise SubmissionError("invalid_poll_limits")
        checks = 0
        deadline = monotonic() + timeout_s
        try:
            # Bounds hanging auth, ledger, transport and sleep, not just intervals.
            async with asyncio.timeout(timeout_s):
                try:
                    while checks < max_checks:
                        if monotonic() >= deadline:
                            return PollResult(None, checks, "timeout")
                        checks += 1
                        record = await self.check_status(caller, submission_id)
                        # Synchronous work can delay the event-loop timeout callback.
                        # A late observation may remain in the ledger, not this reply.
                        if monotonic() >= deadline:
                            return PollResult(None, checks, "timeout")
                        if record.state == "ready":
                            return PollResult(record, checks, "ready")
                        if record.state == "failed":
                            return PollResult(record, checks, "failed")
                        if checks == max_checks:
                            return PollResult(record, checks, "check_limit")
                        remaining = deadline - monotonic()
                        if remaining <= 0:
                            return PollResult(None, checks, "timeout")
                        await sleep(min(interval_s, remaining))
                except MemoryLakeCancelledError:
                    # Python 3.12 Timeout recognizes exact CancelledError only.
                    # Normalize read-runner cancellation INSIDE the context so
                    # stdlib retains ownership/counting of deadline vs user cancel.
                    # Read-only partial stdout is not reconciliation evidence.
                    raise asyncio.CancelledError from None
        except TimeoutError:
            return PollResult(None, checks, "timeout")
        raise AssertionError("unreachable poll loop")
