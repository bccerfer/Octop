"""Unregistered transactional submission storage; no domain/transport dependencies.

History keeps scalar resource IDs: missing live resources deny grants, rather than
silently deleting evidence. This does not solve identity disable/re-enable ABA.
Only begin_step returns a grant; get never reconstructs an executable lease.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, fields
from typing import Any

from octop.infra.db.pool import DatabasePool


@dataclass(frozen=True)
class WritePolicyRow:
    binding_id: str
    enabled: bool
    project_id: str
    library_parent_id: str
    revision: int


@dataclass(frozen=True)
class ApprovalRow:
    approval_id: str
    approver_user_id: int
    binding_id: str
    connector_id: str
    caller_user_id: int
    agent_id: str
    authorization_revision: int
    policy_revision: int
    credential_revision: int
    base_url: str
    workspace_id: str
    project_id: str
    library_parent_id: str
    intent_json: str
    expires_at: float
    revoked: bool = False
    claimed: bool = False


@dataclass(frozen=True)
class SubmissionRow:
    submission_id: str
    approval_id: str
    state: str
    revision: int
    item_id: str | None
    document_id: str | None
    import_accepted: bool


@dataclass(frozen=True)
class StepGrantRow:
    record: SubmissionRow
    phase: str
    expires_at: float


def _approval(row: Any) -> ApprovalRow:
    values = {f.name: row[f.name] for f in fields(ApprovalRow)}
    values["revoked"] = bool(values["revoked"])
    values["claimed"] = bool(values["claimed"])
    return ApprovalRow(**values)


def _record(row: Any) -> SubmissionRow:
    return SubmissionRow(
        row["submission_id"],
        row["approval_id"],
        row["state"],
        row["revision"],
        row["item_id"],
        row["document_id"],
        bool(row["import_accepted"]),
    )


class MemoryLakeSubmissionRepo:
    def __init__(self, db: DatabasePool, *, clock: Callable[[], float] = time.time) -> None:
        self._db = db
        self._clock = clock

    def replace_policy(
        self,
        *,
        binding_id: str,
        expected_revision: int,
        project_id: str,
        library_parent_id: str,
        enabled: bool = False,
    ) -> WritePolicyRow | None:
        with self._db.transaction() as conn:
            if expected_revision == 0:
                cur = conn.execute(
                    "INSERT INTO memorylake_write_policies(binding_id,enabled,project_id,library_parent_id,revision) VALUES (?,?,?,?,1) ON CONFLICT(binding_id) DO NOTHING",
                    (binding_id, int(enabled), project_id, library_parent_id),
                )
            else:
                cur = conn.execute(
                    "UPDATE memorylake_write_policies SET enabled=?,project_id=?,library_parent_id=?,revision=revision+1 WHERE binding_id=? AND revision=?",
                    (int(enabled), project_id, library_parent_id, binding_id, expected_revision),
                )
            if cur.rowcount != 1:
                return None
            return WritePolicyRow(
                binding_id, enabled, project_id, library_parent_id, expected_revision + 1
            )

    def get_policy(self, binding_id: str) -> WritePolicyRow | None:
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM memorylake_write_policies WHERE binding_id=?", (binding_id,)
            ).fetchone()
        if row is None:
            return None
        return WritePolicyRow(
            row["binding_id"],
            bool(row["enabled"]),
            row["project_id"],
            row["library_parent_id"],
            row["revision"],
        )

    def _lock(self, conn: Any, table: str, column: str, value: object) -> None:
        # All identifiers are internal fixed call-site constants, never model input.
        if self._db.dialect == "postgresql":
            conn.execute(
                f"SELECT {column} FROM {table} WHERE {column}=? FOR UPDATE", (value,)
            ).fetchone()

    def _live(self, conn: Any, approval: ApprovalRow) -> float:
        # Fixed lock order shared by create/claim/begin_step. SQLite's outer
        # BEGIN IMMEDIATE already excludes concurrent writers across connections.
        for table, column, value in (
            ("users", "id", approval.caller_user_id),
            ("agents", "agent_id", approval.agent_id),
            ("connectors", "instance_id", approval.connector_id),
            ("memorylake_bindings", "binding_id", approval.binding_id),
            ("memorylake_write_policies", "binding_id", approval.binding_id),
        ):
            self._lock(conn, table, column, value)
        row = conn.execute(
            "SELECT b.*,c.credential_revision,c.credential_expires_at,p.revision AS policy_revision,"
            "p.project_id AS write_project,p.library_parent_id "
            "FROM memorylake_bindings b JOIN users u ON u.id=b.owner_user_id "
            "JOIN agents a ON a.agent_id=b.agent_id "
            "JOIN connectors c ON c.instance_id=b.connector_id "
            "JOIN memorylake_write_policies p ON p.binding_id=b.binding_id "
            "WHERE b.binding_id=? AND b.owner_user_id=? AND b.agent_id=? AND b.connector_id=? "
            "AND b.enabled=1 AND u.disabled=0 AND a.user_id=b.owner_user_id "
            "AND a.enabled=1 AND a.is_shared=0 AND a.kind='expert' "
            "AND c.user_id=b.owner_user_id AND c.kind='memorylake' AND c.status='active' "
            "AND c.shared=0 AND c.credential_blob IS NOT NULL AND length(c.credential_blob)>0 "
            "AND p.enabled=1",
            (
                approval.binding_id,
                approval.caller_user_id,
                approval.agent_id,
                approval.connector_id,
            ),
        ).fetchone()
        now = self._clock()
        if type(now) not in (int, float) or not math.isfinite(now):
            raise ValueError("submission_clock_invalid")
        if row is None or approval.approver_user_id != approval.caller_user_id:
            raise ValueError("submission_authorization_denied")
        try:
            projects = json.loads(row["project_ids_json"])
        except (ValueError, TypeError):
            raise ValueError("submission_authorization_denied") from None
        if (
            not isinstance(projects, list)
            or approval.project_id not in projects
            or row["authorization_revision"] != approval.authorization_revision
            or row["policy_revision"] != approval.policy_revision
            or row["credential_revision"] != approval.credential_revision
            or approval.credential_revision < 1
            or row["base_url"] != approval.base_url
            or row["workspace_id"] != approval.workspace_id
            or row["write_project"] != approval.project_id
            or row["library_parent_id"] != approval.library_parent_id
            or (row["credential_expires_at"] is not None and now >= row["credential_expires_at"])
            or not math.isfinite(approval.expires_at)
            or now >= approval.expires_at
        ):
            raise ValueError("submission_authorization_denied")
        return now

    def create_approval(
        self,
        *,
        approval_id: str,
        approver_user_id: int,
        binding_id: str,
        connector_id: str,
        caller_user_id: int,
        agent_id: str,
        authorization_revision: int,
        policy_revision: int,
        credential_revision: int,
        base_url: str,
        workspace_id: str,
        project_id: str,
        library_parent_id: str,
        intent_json: str,
        expires_at: float,
    ) -> ApprovalRow:
        approval = ApprovalRow(
            approval_id,
            approver_user_id,
            binding_id,
            connector_id,
            caller_user_id,
            agent_id,
            authorization_revision,
            policy_revision,
            credential_revision,
            base_url,
            workspace_id,
            project_id,
            library_parent_id,
            intent_json,
            expires_at,
        )
        with self._db.transaction() as conn:
            self._live(conn, approval)
            names = [f.name for f in fields(ApprovalRow)]
            values = [
                int(getattr(approval, n)) if n in ("revoked", "claimed") else getattr(approval, n)
                for n in names
            ]
            conn.execute(
                f"INSERT INTO memorylake_submission_approvals({','.join(names)}) VALUES ({','.join('?' for _ in names)})",
                tuple(values),
            )
        return approval

    def get_approval(self, approval_id: str) -> ApprovalRow | None:
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM memorylake_submission_approvals WHERE approval_id=?", (approval_id,)
            ).fetchone()
        return _approval(row) if row is not None else None

    def revoke_approval(self, approval_id: str, *, approver_user_id: int) -> bool:
        with self._db.transaction() as conn:
            return bool(
                conn.execute(
                    "UPDATE memorylake_submission_approvals SET revoked=1 WHERE approval_id=? AND approver_user_id=? AND revoked=0",
                    (approval_id, approver_user_id),
                ).rowcount
                == 1
            )

    def _active(self, conn: Any, approval_id: str) -> tuple[ApprovalRow, float]:
        row = conn.execute(
            "SELECT * FROM memorylake_submission_approvals WHERE approval_id=?", (approval_id,)
        ).fetchone()
        if row is None:
            raise ValueError("approval_unavailable")
        approval = _approval(row)
        self._live(conn, approval)
        self._lock(conn, "memorylake_submission_approvals", "approval_id", approval_id)
        row = conn.execute(
            "SELECT * FROM memorylake_submission_approvals WHERE approval_id=?", (approval_id,)
        ).fetchone()
        if row is None:
            raise ValueError("approval_unavailable")
        current = _approval(row)
        now = self._live(conn, current)
        if current.revoked or now >= current.expires_at:
            raise ValueError("approval_inactive")
        # Only revoked/claimed may change; reject any unexpected immutable edit.
        if any(
            getattr(current, f.name) != getattr(approval, f.name)
            for f in fields(ApprovalRow)
            if f.name not in ("revoked", "claimed")
        ):
            raise ValueError("approval_changed")
        return current, now

    def claim(self, approval_id: str, *, expected_intent_json: str) -> SubmissionRow:
        with self._db.transaction() as conn:
            approval, _ = self._active(conn, approval_id)
            if approval.claimed or approval.intent_json != expected_intent_json:
                raise ValueError("approval_claim_denied")
            conn.execute(
                "UPDATE memorylake_submission_approvals SET claimed=1 WHERE approval_id=?",
                (approval_id,),
            )
            conn.execute(
                "INSERT INTO memorylake_submissions(submission_id,approval_id,state,revision) VALUES (?,?,'approved',1)",
                (approval_id, approval_id),
            )
            row = conn.execute(
                "SELECT * FROM memorylake_submissions WHERE submission_id=?", (approval_id,)
            ).fetchone()
        return _record(row)

    def begin_step(
        self, submission_id: str, *, expected_revision: int, expected_state: str, phase: str
    ) -> StepGrantRow:
        if (expected_state, phase) not in (("approved", "uploading"), ("uploaded", "importing")):
            raise ValueError("step_transition_denied")
        with self._db.transaction() as conn:
            approval, now = self._active(conn, submission_id)
            self._lock(conn, "memorylake_submissions", "submission_id", submission_id)
            now = self._live(conn, approval)
            if not approval.claimed:
                raise ValueError("approval_unclaimed")
            expires = min(approval.expires_at, now + 30.0)
            cur = conn.execute(
                "UPDATE memorylake_submissions SET state=?,revision=revision+1,lease_phase=?,lease_revision=revision+1,lease_expires_at=? WHERE submission_id=? AND approval_id=? AND revision=? AND state=? AND (?='uploading' OR item_id IS NOT NULL)",
                (
                    phase,
                    phase,
                    expires,
                    submission_id,
                    submission_id,
                    expected_revision,
                    expected_state,
                    phase,
                ),
            )
            if cur.rowcount != 1:
                raise ValueError("step_cas_denied")
            row = conn.execute(
                "SELECT * FROM memorylake_submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
        return StepGrantRow(_record(row), phase, expires)

    def save(
        self,
        submission_id: str,
        *,
        expected_revision: int,
        expected_state: str,
        expected_intent_json: str,
        state: str,
        item_id: str | None,
        document_id: str | None,
        import_accepted: bool,
    ) -> bool:
        allowed = {
            "uploading": {"uploaded", "unknown"},
            "importing": {"processing", "unknown"},
            "processing": {"ready", "failed", "unknown", "processing"},
            "unknown": {"ready", "failed", "unknown", "processing"},
            "uploaded": {"unknown"},
            "approved": {"unknown"},
            "ready": {"unknown"},
            "failed": {"unknown"},
        }
        if state not in allowed.get(expected_state, set()):
            raise ValueError("save_transition_denied")
        with self._db.transaction() as conn:
            return bool(
                conn.execute(
                    "UPDATE memorylake_submissions SET state=?,revision=revision+1,item_id=?,document_id=?,import_accepted=? WHERE submission_id=? AND revision=? AND state=? AND EXISTS (SELECT 1 FROM memorylake_submission_approvals a WHERE a.approval_id=memorylake_submissions.approval_id AND a.intent_json=?)",
                    (
                        state,
                        item_id,
                        document_id,
                        int(import_accepted),
                        submission_id,
                        expected_revision,
                        expected_state,
                        expected_intent_json,
                    ),
                ).rowcount
                == 1
            )

    def get(self, submission_id: str) -> SubmissionRow | None:
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM memorylake_submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
        return _record(row) if row is not None else None
