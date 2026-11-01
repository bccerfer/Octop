"""Offline CLI result contracts, not a file-submission service.

Writes can have taken effect before a nonzero exit or malformed response. Keep
known IDs for reconciliation and never infer that an upload should be repeated.
No raw upstream error, file name, URL or credential is returned by these parsers.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class UploadReceipt:
    state: Literal["uploaded", "unknown"]
    item_id: str | None


@dataclass(frozen=True)
class ImportItem:
    file_id: str | None
    document_id: str | None
    result: Literal["success", "duplicate", "failed"] | None


@dataclass(frozen=True)
class ImportReceipt:
    state: Literal["accepted", "partial", "failed", "unknown"]
    items: tuple[ImportItem, ...]
    requires_reconciliation: bool
    # Structural completeness only; independent of the actual CLI exit code.
    complete_report: bool = False

    @property
    def document_ids(self) -> tuple[str, ...]:
        """Known IDs, including duplicates or unfamiliar per-item results."""
        return tuple(dict.fromkeys(item.document_id for item in self.items if item.document_id))


@dataclass(frozen=True)
class DocumentReceipt:
    state: Literal["ready", "processing", "failed", "unknown"]
    document_id: str | None


def _object(stdout: str) -> dict[str, object]:
    try:
        payload = json.loads(stdout)
    except (ValueError, RecursionError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _identifier(value: object) -> str | None:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,127}", value):
        return value
    return None


def _result(value: object) -> Literal["success", "duplicate", "failed"] | None:
    if value == "success":
        return "success"
    if value == "duplicate":
        return "duplicate"
    if value == "failed":
        return "failed"
    return None


def _count(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def parse_upload_result(returncode: int, stdout: str) -> UploadReceipt:
    """Library creation is distinct from project import and indexing."""
    item_id = _identifier(_object(stdout).get("item_id"))
    return UploadReceipt("uploaded" if returncode == 0 and item_id else "unknown", item_id)


def parse_import_result(returncode: int, stdout: str) -> ImportReceipt:
    """Preserve the acceptance report even when ``import --wait`` exits nonzero.

    Counts describe acceptance only. Poll known document IDs separately; even
    an all-success batch is not evidence that the documents are indexed.
    """
    payload = _object(stdout)
    details = payload.get("details")
    items = tuple(
        ImportItem(
            _identifier(detail.get("drive_item_id")),
            _identifier(detail.get("document_id")),
            _result(detail.get("result")),
        )
        for detail in (details if isinstance(details, list) else [])
        if isinstance(detail, dict)
    )
    success = _count(payload.get("success_count"))
    failure = _count(payload.get("failure_count"))
    duplicate = _count(payload.get("duplicate_count"))
    if success is None or failure is None or duplicate is None:
        return ImportReceipt("unknown", items, True)
    truncated = payload.get("details_truncated")
    if (
        not isinstance(details, list)
        or len(items) != len(details)
        or not isinstance(truncated, bool)
        or any(item.result is None for item in items)
    ):
        return ImportReceipt("unknown", items, True)
    for result, count in (("success", success), ("failed", failure), ("duplicate", duplicate)):
        observed = sum(item.result == result for item in items)
        if observed > count or (not truncated and observed != count):
            return ImportReceipt("unknown", items, True)
    accepted = success + duplicate
    if accepted == 0 and failure == 0:
        return ImportReceipt("unknown", items, True)
    state: Literal["accepted", "partial", "failed", "unknown"]
    state = "partial" if accepted and failure else "accepted" if accepted else "failed"
    invalid_ids = any(
        detail.get(key) is not None and _identifier(detail.get(key)) is None
        for detail in details
        for key in ("drive_item_id", "document_id")
    )
    incomplete = truncated or invalid_ids or any(
        item.result in ("success", "duplicate") and item.document_id is None for item in items
    )
    return ImportReceipt(state, items, returncode != 0 or incomplete, not incomplete)


def parse_document_result(returncode: int, stdout: str, expected_id: str) -> DocumentReceipt:
    """Only a successful read of the expected document can establish readiness."""
    payload = _object(stdout)
    document_id = _identifier(expected_id)
    state: Literal["ready", "processing", "failed", "unknown"] = "unknown"
    if returncode == 0 and document_id and payload.get("id") == document_id:
        status = payload.get("status")
        if status == "okay":
            state = "ready"
        elif status == "error":
            state = "failed"
        elif status in ("pending", "running"):
            state = "processing"
    return DocumentReceipt(state, document_id)
