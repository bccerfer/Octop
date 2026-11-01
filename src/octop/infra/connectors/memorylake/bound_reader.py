"""Trusted HTTP document-read composition; no Agent tool registration or writes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast

from octop.infra.connectors.memorylake.admission import ReadAdmission
from octop.infra.connectors.memorylake.contract import CallerContext, _authorize, _identifier
from octop.infra.connectors.memorylake.credentials import StoredMemoryLakeCredentials
from octop.infra.connectors.memorylake.file_runtime import MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.read_service import (
    MemoryLakeReadError,
    MemoryLakeReadService,
    ReadConnection,
)
from octop.infra.connectors.memorylake.runner import MemoryLakeCancelledError
from octop.infra.connectors.memorylake.write_binding import StoredWriteBindingLoader
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo


async def read_bound_documents(
    *,
    caller: CallerContext,
    binding_id: str,
    operation: str,
    arguments: Mapping[str, object],
    check_invocation: Callable[[], None],
    runtime: MemoryLakeFileRuntime,
    repo: MemoryLakeBindingRepo,
    executable: Path,
    admission: ReadAdmission,
) -> dict[str, object]:
    """Server-owned 30s deadline includes SQL, admission and private CLI cleanup.

    Awaited SQL/process cleanup may settle after a deadline; cancellation does
    not undo a previously sent read. Project selectors never come from the user.
    """
    supplied = dict(arguments)
    allowed = {"search_documents": {"query", "top_k"}, "get_document": {"document_id"}}
    if operation not in allowed or set(supplied) - allowed[operation]:
        raise MemoryLakeReadError("unsupported_operation_or_arguments")
    binding_id = _identifier(binding_id)

    def check() -> None:
        if cast("Callable[[], object]", check_invocation)() is not None or runtime.closed:
            raise MemoryLakeReadError("connection_unavailable")

    def connector() -> str:
        current = repo.load_owned_active(
            binding_id, caller_user_id=caller.user_id, agent_id=caller.agent_id
        )
        if current is None:
            raise MemoryLakeReadError("connection_unavailable")
        return _identifier(current.binding.connector_id)

    try:
        check()
        async with asyncio.timeout(30):
            try:
                with runtime.operation():
                    connector_id = await runtime.database.submit(connector)
                    check()
                    loader = StoredWriteBindingLoader(
                        caller_user_id=caller.user_id,
                        agent_id=caller.agent_id,
                        is_group=caller.is_group,
                        binding_id=binding_id,
                        connector_id=connector_id,
                        repo=repo,
                        admission=runtime.database,
                        check_invocation=check,
                    )

                    async def connection() -> ReadConnection:
                        binding = await loader.load()
                        _authorize(binding.scope, caller)
                        return ReadConnection(
                            binding.scope, binding.base_url, binding.credential_revision
                        )

                    current = await connection()
                    if operation == "get_document":
                        if len(current.scope.project_ids) != 1:
                            raise MemoryLakeReadError("single_project_required")
                        supplied["project_id"] = current.scope.project_ids[0]
                    else:
                        supplied["project_ids"] = current.scope.project_ids
                    credentials = StoredMemoryLakeCredentials(
                        caller=caller,
                        binding_id=binding_id,
                        connector_id=connector_id,
                        repo=repo,
                        admission=runtime.database,
                        check_invocation=check,
                    )
                    service = MemoryLakeReadService(
                        connector_id,
                        executable=executable,
                        load_connection=connection,
                        load_credentials=credentials.load,
                        admission=admission,
                        check_invocation=check,
                    )
                    result = await service.call(caller, operation, supplied)
                    check()
                    return result
            except MemoryLakeCancelledError:
                # Preserve timeout ownership on Python 3.12; external user
                # cancellation remains CancelledError, never a successful read.
                raise asyncio.CancelledError from None
    except Exception:
        raise MemoryLakeReadError("document_read_unavailable") from None
