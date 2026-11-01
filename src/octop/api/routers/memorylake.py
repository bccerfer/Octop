"""Opt-in MemoryLake file API; native middleware remains fully in place.

Every write is owner/private-expert scoped and separately reviewed/confirmed.
No FastAPI body dependency may precede strong authorization. Deployment is off
by default; unavailable CLI never consumes upload/credential request bodies.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
from functools import partial

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictBool, StrictInt

from octop.api.common.memorylake_auth import authorize_memorylake_owner_request
from octop.api.deps import get_server
from octop.api.routers.memorylake_upload import (
    authorize_memorylake_file_request,
    receive_memorylake_file_request,
)
from octop.infra.connectors.memorylake.application import MemoryLakeApplication
from octop.infra.connectors.memorylake.connections import ConnectionManager
from octop.infra.connectors.memorylake.contract import _identifier
from octop.infra.connectors.memorylake.file_binding import MemoryLakeFileBinding
from octop.infra.connectors.memorylake.reviews import FileReviewRegistry
from octop.infra.connectors.memorylake.submissions import Approval, SubmissionRecord
from octop.infra.errors import ErrorCode, OctopError
from octop.infra.server import OctopServer

router = APIRouter()


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _Connection(_Body):
    api_key: SecretStr = Field(min_length=1, max_length=8192)
    base_url: str = Field(max_length=256)
    workspace_id: str = Field(max_length=128)
    project_id: str = Field(max_length=128)
    library_parent_id: str = Field(max_length=128)
    display_name: str = Field(min_length=1, max_length=128)
    writes_enabled: StrictBool = False


class _Policy(_Body):
    enabled: StrictBool
    expected_revision: StrictInt = Field(ge=1)


class _Search(_Body):
    query: str = Field(min_length=1, max_length=4096)
    top_k: StrictInt = Field(default=5, ge=1, le=50)


class _Confirm(_Body):
    review_id: str = Field(min_length=1, max_length=128)
    confirm: StrictBool


def _application(server: OctopServer) -> MemoryLakeApplication:
    try:
        application = server.memorylake_application
        if application is None:
            raise ValueError
        application.check_current()
        return application
    except Exception:
        raise OctopError.localized(ErrorCode.RESOURCE_NOT_ALLOWED, status=503) from None


@asynccontextmanager
async def _safe() -> AsyncIterator[None]:
    try:
        yield
    except OctopError:
        raise
    except Exception:
        # SubmissionError evidence, crypto/DB errors and secret model inputs are
        # INTERNAL only. Existing app's ordinary error handler must never log them.
        raise OctopError.localized(ErrorCode.RESOURCE_NOT_ALLOWED, status=409) from None


async def _body[Model: BaseModel](
    request: Request, model: type[Model], check: Callable[[], None]
) -> Model:
    """Strict bounded JSON, called only AFTER auth; no parser error reflection."""
    chunks = bytearray()
    try:
        types = [v for k, v in request.scope["headers"] if k.lower() == b"content-type"]
        if len(types) != 1 or types[0].split(b";", 1)[0].strip().lower() != b"application/json":
            raise ValueError
        async with asyncio.timeout(5):
            while True:
                check()
                frame = await request.receive()
                check()
                if frame["type"] != "http.request":
                    raise ValueError
                body, more = frame.get("body", b""), frame.get("more_body", False)
                if (
                    type(body) is not bytes
                    or type(more) is not bool
                    or len(chunks) + len(body) > 16_384
                ):
                    raise ValueError
                chunks.extend(body)
                if not more:
                    break
                await asyncio.sleep(0)
            check()

            # Reject duplicate JSON keys, rather than silently accepting last wins.
            def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
                value: dict[str, object] = {}
                for key, item in pairs:
                    if key in value:
                        raise ValueError
                    value[key] = item
                return value

            value = json.loads(chunks, object_pairs_hook=unique)
            result = model.model_validate(value)
            check()
            return result
    finally:
        chunks.clear()


def _receipt(record: SubmissionRecord) -> dict[str, object]:
    # Exact allowlist; never serialize intent, key, file bytes or hidden evidence.
    return {
        "submission_id": record.submission_id,
        "state": record.state,
        "requires_reconciliation": record.state
        in {"approved", "uploading", "uploaded", "importing", "unknown"},
    }


@asynccontextmanager
async def _binding(
    request: Request,
    server: OctopServer,
    application: MemoryLakeApplication,
    agent_id: str,
    binding_id: str,
    *,
    write: bool,
) -> AsyncIterator[MemoryLakeFileBinding]:
    async with authorize_memorylake_file_request(
        request,
        server,
        agent_id=agent_id,
        binding_id=binding_id,
        transport_provider=application.transport_provider,
        check_deployment=application.check_current,
        require_write=write,
    ) as binding:
        yield binding


@router.get("/status")
async def status(server: OctopServer = Depends(get_server)) -> dict[str, object]:
    application = server.memorylake_application
    if application is not None:
        return application.status()
    enabled = bool(server.services and server.services.config.memorylake_cli.enabled)
    return {
        "enabled": enabled,
        "available": False,
        "reason": "unavailable" if enabled else "disabled",
        "max_file_bytes": 20 * 1024 * 1024,
        "upload_ttl_seconds": 60,
    }


@router.get("/agents/{agent_id}/bindings")
async def bindings(
    request: Request, agent_id: str, server: OctopServer = Depends(get_server)
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        authorize_memorylake_owner_request(
            request, server, agent_id=agent_id, application=application
        ) as owner,
    ):
        assert server.services is not None
        manager = ConnectionManager(server.services)
        with application.runtime.operation():
            rows = await application.runtime.database.submit(
                partial(manager.list_owned, owner.user_id, agent_id)
            )
        await owner.check_disclosure()
        return {"bindings": [asdict(row) for row in rows]}


@router.post("/agents/{agent_id}/bindings")
async def create_binding(
    request: Request, agent_id: str, server: OctopServer = Depends(get_server)
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        authorize_memorylake_owner_request(
            request, server, agent_id=agent_id, application=application
        ) as owner,
    ):
        with application.runtime.operation():
            body = await _body(request, _Connection, owner.check_current)
        assert server.services is not None
        manager = ConnectionManager(server.services)
        with application.runtime.operation():
            row = await application.runtime.database.submit(
                partial(
                    manager.create,
                    owner.user_id,
                    agent_id,
                    api_key=body.api_key.get_secret_value(),
                    base_url=body.base_url,
                    workspace_id=body.workspace_id,
                    project_id=body.project_id,
                    library_parent_id=body.library_parent_id,
                    display_name=body.display_name,
                    writes_enabled=body.writes_enabled,
                )
            )
        await owner.check_disclosure()
        return asdict(row)


@router.patch("/agents/{agent_id}/bindings/{binding_id}/write-policy")
async def policy(
    request: Request, agent_id: str, binding_id: str, server: OctopServer = Depends(get_server)
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        authorize_memorylake_owner_request(
            request, server, agent_id=agent_id, application=application
        ) as owner,
    ):
        with application.runtime.operation():
            body = await _body(request, _Policy, owner.check_current)
        assert server.services is not None
        manager = ConnectionManager(server.services)
        with application.runtime.operation():
            row = await application.runtime.database.submit(
                partial(
                    manager.set_write_policy,
                    owner.user_id,
                    agent_id,
                    _identifier(binding_id),
                    enabled=body.enabled,
                    expected_revision=body.expected_revision,
                )
            )
        await owner.check_disclosure()
        return asdict(row)


@router.post("/agents/{agent_id}/bindings/{binding_id}/uploads")
async def upload(
    request: Request, agent_id: str, binding_id: str, server: OctopServer = Depends(get_server)
) -> dict[str, str]:
    application = _application(server)
    async with _safe():
        identifier = await receive_memorylake_file_request(
            request,
            server,
            agent_id=agent_id,
            binding_id=binding_id,
            transport_provider=application.transport_provider,
            check_deployment=application.check_current,
        )
        return {"upload_id": identifier}


@router.delete("/agents/{agent_id}/bindings/{binding_id}/uploads/{upload_id}")
async def discard_upload(
    request: Request,
    agent_id: str,
    binding_id: str,
    upload_id: str,
    server: OctopServer = Depends(get_server),
) -> dict[str, bool]:
    application = _application(server)
    async with (
        _safe(),
        _binding(request, server, application, agent_id, binding_id, write=False) as binding,
    ):
        await binding.discard_upload(_identifier(upload_id))
        return {"discarded": True}


@router.post("/agents/{agent_id}/bindings/{binding_id}/uploads/{upload_id}/review")
async def review(
    request: Request,
    agent_id: str,
    binding_id: str,
    upload_id: str,
    server: OctopServer = Depends(get_server),
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        _binding(request, server, application, agent_id, binding_id, write=True) as binding,
    ):
        intent = await binding.prepare(_identifier(upload_id))
        binding.check_invocation()
        item = application.reviews.create(binding_id, intent)
        return {
            "review_id": item.review_id,
            "submission_id": item.submission_id,
            "filename": intent.filename,
            "size": intent.size,
            "sha256": intent.sha256,
            "workspace_id": intent.binding.scope.workspace_id,
            "project_id": intent.binding.project_id,
            "library_parent_id": intent.binding.library_parent_id,
            "expires_at": item.expires_at,
        }


@router.post("/agents/{agent_id}/bindings/{binding_id}/uploads/{upload_id}/submit")
async def submit(
    request: Request,
    agent_id: str,
    binding_id: str,
    upload_id: str,
    server: OctopServer = Depends(get_server),
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        _binding(request, server, application, agent_id, binding_id, write=False) as binding,
    ):
        with application.runtime.operation():
            body = await _body(request, _Confirm, binding.check_invocation)
        if body.confirm is not True:
            raise ValueError
        submission_id = FileReviewRegistry.submission_id(binding.caller, binding_id, upload_id)
        existing = await binding.existing_submission(submission_id)
        if existing is not None:
            return _receipt(existing)  # Never resume/replay a persisted remote step.
        with application.reviews.confirm(
            binding.caller, binding_id, upload_id, body.review_id
        ) as item:
            current = await binding.prepare(upload_id)
            if current != item.intent:
                raise ValueError
            await binding.create_approval_once(
                Approval(item.submission_id, item.intent, item.expires_at)
            )
            record = await binding.submit(upload_id, item.submission_id)
            return _receipt(record)


@router.get("/agents/{agent_id}/bindings/{binding_id}/submissions/{submission_id}")
async def submission(
    request: Request,
    agent_id: str,
    binding_id: str,
    submission_id: str,
    server: OctopServer = Depends(get_server),
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        _binding(request, server, application, agent_id, binding_id, write=False) as binding,
    ):
        record = await binding.observe_submission(_identifier(submission_id))
        return _receipt(record)


@router.post("/agents/{agent_id}/bindings/{binding_id}/search")
async def search_documents(
    request: Request,
    agent_id: str,
    binding_id: str,
    server: OctopServer = Depends(get_server),
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        _binding(request, server, application, agent_id, binding_id, write=False) as binding,
    ):
        with application.runtime.operation():
            body = await _body(request, _Search, binding.check_invocation)
        return await application.read_documents(
            caller=binding.caller,
            binding_id=binding_id,
            operation="search_documents",
            arguments={"query": body.query, "top_k": body.top_k},
            check_invocation=binding.check_invocation,
        )


@router.get("/agents/{agent_id}/bindings/{binding_id}/documents/{document_id}")
async def get_document(
    request: Request,
    agent_id: str,
    binding_id: str,
    document_id: str,
    server: OctopServer = Depends(get_server),
) -> dict[str, object]:
    application = _application(server)
    async with (
        _safe(),
        _binding(request, server, application, agent_id, binding_id, write=False) as binding,
    ):
        return await application.read_documents(
            caller=binding.caller,
            binding_id=binding_id,
            operation="get_document",
            arguments={"document_id": _identifier(document_id)},
            check_invocation=binding.check_invocation,
        )
