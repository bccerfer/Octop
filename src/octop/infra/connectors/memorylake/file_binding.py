"""Trusted composition for one caller and file-submission binding.

The server must supply authenticated identity, current same-database repositories
and its own transport factory. This module does NOT validate JWTs, select a CLI,
load secrets or register ingress. Never expose its constructor/factory as a model
tool or accept an authorization callback/endpoint/argv from a client. Scope IDs,
fresh loader, source namespace and ledger binding are composed together here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from octop.infra.connectors.memorylake.contract import CallerContext, _authorize, _identifier
from octop.infra.connectors.memorylake.file_runtime import MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.submission_storage import DatabaseSubmissionLedger
from octop.infra.connectors.memorylake.submissions import (
    Approval,
    PollResult,
    SubmissionCancelledError,
    SubmissionError,
    SubmissionIntent,
    SubmissionRecord,
    SubmissionService,
    SubmissionTransport,
    WriteBinding,
)
from octop.infra.connectors.memorylake.upload_source import ScopedUploadSource
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
from octop.infra.db.repos.memorylake_submissions import MemoryLakeSubmissionRepo

TransportFactory = Callable[[str, Callable[[], Awaitable[WriteBinding]]], SubmissionTransport]


class _Invocation:
    def __init__(self, check: Callable[[], None]) -> None:
        if not callable(check):
            raise SubmissionError("file_invocation_unavailable")
        self._check = check
        self.closed = False

    def check(self) -> None:
        try:
            if self.closed or self._check() is not None or self.closed:
                raise ValueError
        except Exception:
            raise SubmissionError("file_invocation_unavailable") from None


class MemoryLakeFileBinding:
    """Per-invocation objects over a shared owner, not another resource budget.

    Public async operations hold runtime.operation through final durable saves.
    Source access is trusted wiring for the existing capture adapter, which holds
    its own operation. Direct source use must similarly hold an operation. No
    approval is manufactured: create_approval is only for a trusted confirmation
    path that has already obtained explicit user consent to the exact intent.
    """

    def __init__(
        self,
        *,
        runtime: MemoryLakeFileRuntime,
        caller: CallerContext,
        invocation: _Invocation,
        loader: StoredWriteBindingLoader,
        source: ScopedUploadSource,
        ledger: DatabaseSubmissionLedger,
        service: SubmissionService,
    ) -> None:
        self._runtime = runtime
        self._caller = caller
        self._invocation = invocation
        self._loader = loader
        self._source = source
        self._ledger = ledger
        self._service = service

    @classmethod
    async def create(
        cls,
        *,
        runtime: MemoryLakeFileRuntime,
        caller: CallerContext,
        binding_id: str,
        connector_id: str,
        binding_repo: MemoryLakeBindingRepo,
        submission_repo: MemoryLakeSubmissionRepo,
        transport_factory: TransportFactory,
        check_invocation: Callable[[], None],
        require_write: bool = False,
    ) -> MemoryLakeFileBinding:
        """Authorize one fixed tuple before constructing any transport object.

        Factory construction must be side-effect free: actual credentials/CLI
        deployment remain separate gates. It receives the SAME fixed connector
        and live loader as the service, not an independently supplied authorizer.
        """
        if (
            not isinstance(caller, CallerContext)
            or not callable(transport_factory)
            or type(require_write) is not bool
        ):
            raise SubmissionError("file_binding_unavailable")
        binding_id, connector_id = _identifier(binding_id), _identifier(connector_id)
        invocation = _Invocation(check_invocation)
        with runtime.operation():
            invocation.check()
            loader = StoredWriteBindingLoader(
                caller_user_id=caller.user_id,
                agent_id=caller.agent_id,
                is_group=caller.is_group,
                binding_id=binding_id,
                connector_id=connector_id,
                repo=binding_repo,
                admission=runtime.database,
                check_invocation=invocation.check,
            )
            binding = await loader.load()
            _authorize(binding.scope, caller)
            if require_write and binding.writes_enabled is not True:
                raise SubmissionError("write_not_authorized")
            invocation.check()
            source = runtime.source.bind(
                binding_id=binding_id,
                connector_id=connector_id,
                caller=caller,
                authorize=loader.authorize,
            )
            try:
                transport = transport_factory(connector_id, loader.load)
                if not all(
                    callable(getattr(transport, name, None))
                    for name in ("upload", "import_file", "get_document")
                ):
                    raise ValueError
                invocation.check()
                source.check_owner(runtime.source, caller)
            except BaseException as error:
                invocation.closed = True
                source.close()
                if not isinstance(error, Exception):
                    raise
                raise SubmissionError("file_binding_unavailable") from None
            ledger = DatabaseSubmissionLedger(
                submission_repo, binding_id=binding_id, admission=runtime.database
            )
            service = SubmissionService(
                connector_id,
                load_binding=loader.load,
                source=source,
                ledger=ledger,
                transport=transport,
            )
            return cls(
                runtime=runtime,
                caller=caller,
                invocation=invocation,
                loader=loader,
                source=source,
                ledger=ledger,
                service=service,
            )

    def check_invocation(self) -> None:
        """Synchronous caller-lifetime check, NOT a fresh database permission read.

        Bounded HTTP receivers use this around awaits; source publication and
        actual write execution still perform their separate current SQL checks.
        """
        self._invocation.check()

    @property
    def caller(self) -> CallerContext:
        return self._caller

    @property
    def source(self) -> ScopedUploadSource:
        return self._source

    def close(self) -> None:
        """Invalidate this invocation; never cancel/replay an already granted write."""
        self._source.close()
        self._invocation.closed = True

    async def prepare(self, attachment_id: str) -> SubmissionIntent:
        with self._runtime.operation():
            self._invocation.check()
            intent = await self._service.prepare(self._caller, attachment_id)
            self._invocation.check()
            return intent

    async def create_approval(self, approval: Approval) -> None:
        """Trusted explicit-consent path only, not an automatic write grant.

        A failure after persistence is not proof no approval was saved. Recovery
        must inspect its existing ID, not blindly create a replacement consent.
        """
        with self._runtime.operation():
            self._invocation.check()
            if not isinstance(approval, Approval) or approval.intent.caller != self._caller:
                raise SubmissionError("approval_intent_mismatch")
            binding = await self._loader.load()
            if binding != approval.intent.binding:
                raise SubmissionError("approval_intent_mismatch")
            await self._ledger.create_approval(approval)
            self._invocation.check()

    async def existing_submission(self, submission_id: str) -> SubmissionRecord | None:
        """Read persisted evidence under current read permission, never replay a write."""
        with self._runtime.operation():
            self._invocation.check()
            record = await self._ledger.find(submission_id)
            self._invocation.check()
            if record is not None:
                await self._service.authorize_result(self._caller, record)
                self._invocation.check()
            return record

    async def create_approval_once(self, approval: Approval) -> None:
        """Idempotent exact consent only; never extend expiry or revive revocation."""
        with self._runtime.operation():

            async def current() -> None:
                self._invocation.check()
                if (
                    not isinstance(approval, Approval)
                    or approval.revoked is not False
                    or approval.intent.caller != self._caller
                    or await self._loader.load() != approval.intent.binding
                ):
                    raise SubmissionError("approval_intent_mismatch")
                self._invocation.check()

            async def existing() -> bool:
                try:
                    stored = await self._ledger.approval(approval.approval_id)
                except SubmissionError as error:
                    if error.args == ("approval_unavailable",):
                        return False
                    raise
                if stored != approval or stored.revoked is not False:
                    raise SubmissionError("approval_intent_mismatch")
                await current()
                return True

            await current()
            if await existing():
                return
            await current()
            try:
                await self._ledger.create_approval(approval)
            except Exception:
                # A unique-ID race or uncertain commit is recoverable ONLY by
                # observing this exact consent, never by creating another ID.
                if await existing():
                    return
                raise SubmissionError("approval_unavailable") from None
            await current()

    async def submit(self, attachment_id: str, approval_id: str) -> SubmissionRecord:
        with self._runtime.operation():
            self._invocation.check()
            record = await self._service.submit(self._caller, attachment_id, approval_id)
            try:
                self._invocation.check()
                await self._service.authorize_result(self._caller, record)
                self._invocation.check()
                # Only this completed submit releases its store reference. A
                # concurrent read of an in-flight receipt must not uncharge it.
                self._source.release(self._caller, record.intent.attachment_id)
            except asyncio.CancelledError:
                # The write outcome was already persisted before this read-only
                # disclosure barrier. Do not relabel it unknown or replay it.
                raise SubmissionCancelledError(record) from None
            except Exception:
                # Persisted evidence is internal recovery state, never an
                # authorization failure response body or proof of no write.
                raise SubmissionError("file_invocation_unavailable", record) from None
            return record

    async def discard_upload(self, attachment_id: str) -> None:
        """Discard local bytes under current read permission, including write opt-out.

        This cannot recall snapshots already returned to an operation, consent,
        granted leases or remote files. It is not a cancellation of submission.
        """
        with self._runtime.operation():
            self._invocation.check()
            binding = await self._loader.load()
            _authorize(binding.scope, self._caller)
            self._invocation.check()
            self._source.release(self._caller, attachment_id)

    async def observe_submission(self, submission_id: str) -> SubmissionRecord:
        """Read any persisted phase; only query remote state with accepted doc evidence.

        In-flight/uncertain writes remain observable recovery evidence, never an
        instruction to replay upload/import. Both paths perform current read
        authorization before handing a receipt back to the HTTP adapter.
        """
        record = await self.existing_submission(submission_id)
        if record is None:
            raise SubmissionError("submission_unavailable")
        if (
            record.state in ("processing", "unknown")
            and record.import_accepted
            and record.document_id is not None
        ):
            return await self.check_status(submission_id)
        return record

    async def check_status(self, submission_id: str) -> SubmissionRecord:
        with self._runtime.operation():
            self._invocation.check()
            record = await self._service.check_status(self._caller, submission_id)
            self._invocation.check()
            return record

    async def poll_status(
        self,
        submission_id: str,
        *,
        max_checks: int = 5,
        timeout_s: float = 30,
        interval_s: float = 1,
    ) -> PollResult:
        with self._runtime.operation():
            self._invocation.check()
            result = await self._service.poll_status(
                self._caller,
                submission_id,
                max_checks=max_checks,
                timeout_s=timeout_s,
                interval_s=interval_s,
            )
            self._invocation.check()
            return result
