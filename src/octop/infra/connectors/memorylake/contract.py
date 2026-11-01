"""Fail-closed read command planning for the pinned MemoryLake CLI contract.

No subprocess, credential, filesystem or network access happens here. Callers
must load current scope/consent and construct CallerContext from authenticated
server state for every invocation, never from model arguments or a tool closure.
Shared agents and group conversations are deliberately outside this first slice.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,127}\Z")


class MemoryLakeContractError(ValueError):
    """Stable internal reason code; adapters must localize before presentation."""


@dataclass(frozen=True)
class CallerContext:
    user_id: int
    agent_id: str
    agent_owner_user_id: int | None
    authorization_revision: int
    is_group: bool = False


@dataclass(frozen=True)
class ReadScope:
    connector_id: str
    owner_user_id: int
    agent_id: str
    workspace_id: str
    project_ids: tuple[str, ...] = ()
    actor_id: str | None = None
    authorization_revision: int = 1
    enabled: bool = True


@dataclass(frozen=True)
class ReadCommand:
    connector_id: str
    authorization_revision: int
    operation: str
    # Queries are sensitive even without a key. Never log argv or this field.
    arguments: tuple[str, ...] = field(repr=False)


def _identifier(value: object) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise MemoryLakeContractError("invalid_identifier")
    return value


def _authorize(scope: ReadScope, caller: CallerContext) -> None:
    if (
        scope.enabled is not True
        or caller.is_group is not False
        or type(caller.user_id) is not int
        or type(scope.owner_user_id) is not int
        or caller.user_id <= 0
        or caller.user_id != scope.owner_user_id
        or type(caller.agent_owner_user_id) is not int
        or caller.agent_owner_user_id != caller.user_id
        or caller.agent_id != scope.agent_id
        or type(scope.authorization_revision) is not int
        or scope.authorization_revision < 1
        or type(caller.authorization_revision) is not int
        or caller.authorization_revision != scope.authorization_revision
    ):
        raise MemoryLakeContractError("access_denied")
    for value in (scope.connector_id, scope.agent_id, scope.workspace_id):
        _identifier(value)
    if not isinstance(scope.project_ids, tuple):
        raise MemoryLakeContractError("invalid_scope")
    for value in scope.project_ids:
        _identifier(value)
    if scope.actor_id is not None:
        _identifier(scope.actor_id)


def _query(arguments: Mapping[str, object]) -> str:
    query = arguments.get("query")
    if (
        not isinstance(query, str)
        or not query.strip()
        or "\0" in query
        or len(query) > 4096
    ):
        raise MemoryLakeContractError("invalid_query")
    return query.strip()


def _top_k(arguments: Mapping[str, object]) -> str:
    value = arguments.get("top_k", 10)
    if type(value) is not int or not 1 <= value <= 50:
        raise MemoryLakeContractError("invalid_top_k")
    return str(value)


def _projects(scope: ReadScope, arguments: Mapping[str, object]) -> tuple[str, ...]:
    selected = arguments.get("project_ids", scope.project_ids)
    if not isinstance(selected, (list, tuple)) or not selected:
        raise MemoryLakeContractError("project_scope_required")
    projects = tuple(_identifier(value) for value in selected)
    if not set(projects).issubset(scope.project_ids):
        raise MemoryLakeContractError("project_scope_denied")
    return tuple(dict.fromkeys(projects))


def plan_read(
    scope: ReadScope,
    caller: CallerContext,
    operation: str,
    arguments: Mapping[str, object],
) -> ReadCommand:
    """Plan an allowlisted read after identity and explicit scope validation.

    The returned arguments exclude binary/profile/endpoint, which a future
    trusted runtime must supply. This is not an executable authorization token.
    """
    _authorize(scope, caller)
    allowed: dict[str, set[str]] = {
        "search_documents": {"query", "project_ids", "top_k"},
        "search_facts": {"query", "top_k"},
        "get_document": {"project_id", "document_id"},
        "get_fact": {"fact_id"},
        "trace_fact": {"fact_id"},
    }
    if operation not in allowed or set(arguments) - allowed[operation]:
        raise MemoryLakeContractError("unsupported_operation_or_arguments")
    if operation in {"get_fact", "trace_fact"}:
        if scope.actor_id is None:
            raise MemoryLakeContractError("actor_scope_required")
        argv = (
            "fact",
            "get" if operation == "get_fact" else "trace",
            "--workspace",
            scope.workspace_id,
            "--actor",
            scope.actor_id,
            "--",
            _identifier(arguments.get("fact_id")),
        )
    elif operation == "get_document":
        project = _identifier(arguments.get("project_id"))
        if project not in scope.project_ids:
            raise MemoryLakeContractError("project_scope_denied")
        argv = (
            "project",
            "document",
            "get",
            "--workspace",
            scope.workspace_id,
            "--project",
            project,
            "--",
            _identifier(arguments.get("document_id")),
        )
    else:
        query, top_k = _query(arguments), _top_k(arguments)
        if operation == "search_documents":
            filter_args = (
                "--projects",
                ",".join(_projects(scope, arguments)),
                "--types",
                "document",
            )
        else:
            if scope.actor_id is None:
                raise MemoryLakeContractError("actor_scope_required")
            filter_args = ("--actors", scope.actor_id, "--types", "fact")
        argv = (
            "search",
            "--workspace",
            scope.workspace_id,
            *filter_args,
            "--top-k",
            top_k,
            "--",
            query,
        )
    return ReadCommand(scope.connector_id, scope.authorization_revision, operation, argv)
