"""Current write-binding loader; no approval, secret access or remote I/O.

A single SQL statement includes live owner/expert/read scope and write policy.
It is a diagnostic/current configuration snapshot, NOT an execution lease:
DatabaseSubmissionLedger.begin_step remains the atomic write authorization point.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from octop.infra.connectors.memorylake.contract import CallerContext, _authorize, _identifier
from octop.infra.connectors.memorylake.storage import validate_connection_snapshot
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission
from octop.infra.connectors.memorylake.submissions import SubmissionError, WriteBinding
from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo


class StoredWriteBindingLoader:
    def __init__(
        self,
        *,
        caller_user_id: int,
        agent_id: str,
        is_group: bool,
        binding_id: str,
        connector_id: str,
        repo: MemoryLakeBindingRepo,
        admission: DatabaseJobAdmission,
        dashboard_thread_id: str | None = None,
        check_invocation: Callable[[], None] | None = None,
    ) -> None:
        self._caller_user_id = caller_user_id
        self._agent_id = _identifier(agent_id)
        self._is_group = is_group
        self._binding_id = _identifier(binding_id)
        self._connector_id = _identifier(connector_id)
        self._repo = repo
        self._admission = admission
        self._thread_id = (
            _identifier(dashboard_thread_id) if dashboard_thread_id is not None else None
        )
        self._check_invocation = check_invocation

    def _check(self) -> None:
        if self._check_invocation is not None:
            self._check_invocation()
        if (
            type(self._caller_user_id) is not int
            or self._caller_user_id <= 0
            or self._is_group is not False
        ):
            raise SubmissionError("binding_access_denied")

    def _snapshot(self) -> WriteBinding:
        authorized = self._repo.load_owned_write_binding(
            self._binding_id,
            caller_user_id=self._caller_user_id,
            agent_id=self._agent_id,
            dashboard_thread_id=self._thread_id,
            dashboard_session_key=(
                f"{self._agent_id}:dashboard:{self._caller_user_id}:dm"
                if self._thread_id is not None
                else None
            ),
        )
        if authorized is None:
            raise SubmissionError("binding_access_denied")
        connection = validate_connection_snapshot(
            authorized,
            caller_user_id=self._caller_user_id,
            agent_id=self._agent_id,
            binding_id=self._binding_id,
            connector_id=self._connector_id,
        )
        # A paused/deleted write policy cannot revoke independently valid read
        # reconciliation. Empty targets are NEVER executed: the service replaces
        # them only with the original approved project after checking read scope.
        project_id = folder_id = ""
        if authorized.writes_enabled:
            project_id = _identifier(authorized.project_id)
            folder_id = _identifier(authorized.library_parent_id)
            if (
                project_id not in connection.scope.project_ids
                or type(authorized.policy_revision) is not int
                or authorized.policy_revision < 1
            ):
                raise SubmissionError("write_scope_invalid")
        return WriteBinding(
            connection.scope,
            project_id,
            folder_id,
            connection.base_url,
            connection.credential_revision,
            authorized.writes_enabled,
            authorized.policy_revision,
        )

    async def load(self) -> WriteBinding:
        try:
            self._check()
            future = self._admission.submit(self._snapshot)
            binding = await asyncio.shield(future)
            self._check()
            return binding
        except SubmissionError:
            raise
        except Exception:
            raise SubmissionError("binding_unavailable") from None

    async def authorize(self, caller: CallerContext) -> None:
        """Fresh owner-only preflight for new-upload capture/read, never approval."""
        binding = await self.load()
        _authorize(binding.scope, caller)
        if binding.writes_enabled is not True:
            raise SubmissionError("write_not_authorized")
