"""Allowlisted, bounded CLI read data; never executable instructions.

Request scope describes the trusted invocation, not proof of server filtering.
Unknown fields, URLs and fact metadata are intentionally excluded. Supported
content is never truncated: any limit breach rejects the entire response.
Call only after a successful bounded invocation; this module authorizes no reads.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import NoReturn

_MAX_TEXT_CHARS = 16_384
_MAX_TOTAL_TEXT_CHARS = 131_072
_MAX_RESULTS = 50
_MAX_ITEMS = 200
_MAX_TOTAL_ITEMS = 1_000


@dataclass
class _Budget:
    text_chars: int = 0
    items: int = 0

    def text(self, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("invalid_result_shape")
        self.text_chars += len(value)
        if len(value) > _MAX_TEXT_CHARS or self.text_chars > _MAX_TOTAL_TEXT_CHARS:
            raise ValueError("result_limit_exceeded")
        return value

    def collection(self, value: object, limit: int = _MAX_ITEMS) -> list[object]:
        if not isinstance(value, list):
            raise ValueError("invalid_result_shape")
        self.items += len(value)
        if len(value) > limit or self.items > _MAX_TOTAL_ITEMS:
            raise ValueError("result_limit_exceeded")
        return value


def _identifier(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,127}", value):
        raise ValueError("invalid_result_identifier")
    return value


def _invalid_constant(_value: str) -> NoReturn:
    raise ValueError("invalid_result_json")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("invalid_result_json")
    return number


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("invalid_result_shape")
    return value


def _texts(payload: dict[str, object], keys: tuple[str, ...], budget: _Budget) -> dict[str, object]:
    return {key: budget.text(payload[key]) for key in keys if key in payload}


def _optional_ids(payload: dict[str, object], keys: tuple[str, ...]) -> dict[str, object]:
    return {
        key: _identifier(payload[key]) if payload[key] is not None else None
        for key in keys if key in payload
    }


def _numbers(payload: dict[str, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key in ("rank", "score"):
        if key not in payload:
            continue
        value = payload[key]
        if value is not None:
            if key == "rank":
                valid = type(value) is int and value >= 1
            else:
                valid = type(value) in (int, float)
            if not valid:
                raise ValueError("invalid_result_shape")
        result[key] = value
    return result


def _fact(payload: dict[str, object], actor_id: str, budget: _Budget, expected_id: str | None = None) -> dict[str, object]:
    identifier = _identifier(payload.get("id"))
    if expected_id is not None and identifier != expected_id:
        raise ValueError("result_id_mismatch")
    result: dict[str, object] = {"id": identifier}
    result.update(_texts(payload, ("fact", "expiration_date", "created_at", "updated_at"), budget))
    result.update(_numbers(payload))
    if "expired" in payload:
        if payload["expired"] is not None and not isinstance(payload["expired"], bool):
            raise ValueError("invalid_result_shape")
        result["expired"] = payload["expired"]
    if "owner" in payload:
        owner = payload["owner"]
        if owner is None:
            result["owner"] = None
        else:
            owner = _object(owner)
            if owner.get("type") != "actor" or _identifier(owner.get("id")) != actor_id:
                raise ValueError("result_scope_mismatch")
            result["owner"] = {"type": "actor", "id": actor_id}
    return result


def _block(payload: dict[str, object], budget: _Budget) -> dict[str, object]:
    result = _texts(payload, ("type", "text", "range"), budget)
    if "highlight" in payload:
        if payload["highlight"] is None:
            result["highlight"] = None
        else:
            highlight = _object(payload["highlight"])
            chunks = []
            for raw in budget.collection(highlight.get("chunks")):
                chunk = _object(raw)
                chunks.append({**_optional_ids(chunk, ("id",)), **_texts(chunk, ("text", "range"), budget)})
            result["highlight"] = {"chunks": chunks}
    return result


def _document(payload: dict[str, object], projects: tuple[str, ...], budget: _Budget) -> dict[str, object]:
    result: dict[str, object] = {"document_id": _identifier(payload.get("document_id"))}
    if "project_id" in payload:
        project = payload["project_id"]
        if project is not None and _identifier(project) not in projects:
            raise ValueError("result_scope_mismatch")
        result["project_id"] = project
    result.update(_texts(payload, ("document_name", "file_name", "document_summary", "source_type", "sheet_name"), budget))
    result.update(_numbers({key: payload[key] for key in ("rank",) if key in payload}))
    if "items" in payload:
        result["items"] = [_block(_object(item), budget) for item in budget.collection(payload["items"])]
    return result


def _trace_entry(payload: dict[str, object], budget: _Budget) -> dict[str, object]:
    result = _texts(payload, ("event", "timestamp", "source_kind", "old_fact", "new_fact"), budget)
    result.update(_optional_ids(payload, ("history_id", "conversation_id", "source_event_id")))
    if "source_entry_ids" in payload:
        result["source_entry_ids"] = [_identifier(value) for value in budget.collection(payload["source_entry_ids"])]
    return result


def normalize_read_result(
    operation: str,
    stdout: str,
    *,
    project_ids: tuple[str, ...] = (),
    actor_id: str | None = None,
    expected_id: str | None = None,
) -> dict[str, object]:
    """Return scoped, untrusted data or a body-free reason code.

    The typed CLI always serializes the target collection. Missing collections
    are malformed, not zero matches. Optional scope/rank/citations stay absent;
    nested fields are allowlisted and bounded, with no lossy text truncation.
    """
    if operation not in {"search_documents", "search_facts", "get_document", "get_fact", "trace_fact"}:
        raise ValueError("unsupported_read_operation")
    scope: dict[str, object] = {}
    projects: tuple[str, ...] = ()
    if operation in {"search_documents", "get_document"}:
        if not isinstance(project_ids, tuple) or not project_ids:
            raise ValueError("result_project_scope_required")
        projects = tuple(_identifier(project) for project in project_ids)
        if operation == "get_document" and len(projects) != 1:
            raise ValueError("result_project_scope_required")
        scope["project_ids"] = list(projects)
    else:
        actor_id = _identifier(actor_id)
        scope["actor_id"] = actor_id
    if operation in {"get_document", "get_fact", "trace_fact"}:
        expected_id = _identifier(expected_id)
        scope["expected_id"] = expected_id
    try:
        decoded = json.loads(stdout, parse_constant=_invalid_constant, parse_float=_finite_float)
    except (ValueError, RecursionError):
        raise ValueError("invalid_result_json") from None
    payload = _object(decoded)
    if "error" in payload and operation != "get_document":
        raise ValueError("upstream_read_error")
    budget = _Budget()
    data: dict[str, object]
    if operation == "search_documents":
        data = {"documents": [_document(_object(item), projects, budget) for item in budget.collection(payload.get("documents"), _MAX_RESULTS)]}
    elif operation == "search_facts":
        assert actor_id is not None
        data = {"facts": [_fact(_object(item), actor_id, budget) for item in budget.collection(payload.get("facts"), _MAX_RESULTS)]}
    elif operation == "get_document":
        identifier = _identifier(payload.get("id"))
        if identifier != expected_id:
            raise ValueError("result_id_mismatch")
        if payload.get("project_id") is not None and _identifier(payload["project_id"]) not in projects:
            raise ValueError("result_scope_mismatch")
        data = {"id": identifier, **_texts(payload, ("name", "status", "document_type", "created_at"), budget)}
    elif operation == "get_fact":
        assert actor_id is not None
        data = _fact(payload, actor_id, budget, expected_id)
    else:
        assert actor_id is not None
        data = {"trace": [_trace_entry(_object(item), budget) for item in budget.collection(payload.get("trace"))]}
        if payload.get("fact") is not None:
            data["fact"] = _fact(_object(payload["fact"]), actor_id, budget, expected_id)
    return {"operation": operation, "request_scope": scope, "untrusted": True, "data": data}
