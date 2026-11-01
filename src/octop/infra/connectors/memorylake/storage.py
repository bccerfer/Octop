"""Unregistered owner-only reader backed by real Octop repositories.

Caller identity and fixed binding/connector IDs are trusted request inputs, never
model arguments. Every loader re-reads the repository's single SQL authorization
snapshot. No API keys are retained between calls; database and crypto operations
run off the event loop. This does not register an API, connector or agent tool.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, cast

from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope, _identifier
from octop.infra.connectors.memorylake.read_service import (
    _APPROVED_BASE_URLS,
    MemoryLakeReadService,
    ReadConnection,
    ReadCredentials,
)
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission

if TYPE_CHECKING:
    from octop.infra.connectors.memorylake.admission import ReadAdmission
    from octop.infra.db.repos.memorylake import (
        AuthorizedMemoryLakeBinding,
        AuthorizedMemoryLakeWriteBinding,
        MemoryLakeBindingRepo,
    )
    from octop.infra.db.repos.secrets import SecretRepo


class StoredMemoryLakeError(ValueError):
    """Internal reason code only, with no database or credential details."""


def validate_connection_snapshot(
    authorized: AuthorizedMemoryLakeBinding | AuthorizedMemoryLakeWriteBinding,
    *,
    caller_user_id: int,
    agent_id: str,
    binding_id: str,
    connector_id: str,
) -> ReadConnection:
    """Pure shared validation of a repository's single authorization snapshot."""
    row = authorized.binding
    if (
        row.binding_id != binding_id
        or row.connector_id != connector_id
        or row.owner_user_id != caller_user_id
        or row.agent_id != agent_id
        or row.enabled is not True
        or row.base_url not in _APPROVED_BASE_URLS
        or type(row.authorization_revision) is not int
        or row.authorization_revision < 1
        or type(authorized.credential_revision) is not int
        or authorized.credential_revision < 1
    ):
        raise StoredMemoryLakeError("invalid_binding")
    expiry = authorized.credential_expires_at
    if expiry is not None and (type(expiry) is not int or time.time() >= expiry):
        raise StoredMemoryLakeError("credential_expired")
    if not isinstance(row.project_ids_json, str) or len(row.project_ids_json) > 16_384:
        raise StoredMemoryLakeError("invalid_scope")
    try:
        projects = json.loads(row.project_ids_json)
        if not isinstance(projects, list) or len(projects) > 50:
            raise StoredMemoryLakeError("invalid_scope")
        project_ids = tuple(_identifier(value) for value in projects)
        if len(set(project_ids)) != len(project_ids):
            raise StoredMemoryLakeError("invalid_scope")
        actor_id = _identifier(row.actor_id) if row.actor_id is not None else None
        workspace_id = _identifier(row.workspace_id)
        if not project_ids and actor_id is None:
            raise StoredMemoryLakeError("invalid_scope")
    except (ValueError, RecursionError):
        raise StoredMemoryLakeError("invalid_scope") from None
    scope = ReadScope(
        connector_id=row.connector_id,
        owner_user_id=row.owner_user_id,
        agent_id=row.agent_id,
        workspace_id=workspace_id,
        project_ids=project_ids,
        actor_id=actor_id,
        authorization_revision=row.authorization_revision,
        enabled=row.enabled,
    )
    return ReadConnection(scope, row.base_url, authorized.credential_revision)


class StoredMemoryLakeReader:
    """Production must pass the server runtime's shared ``database`` admission.

    Missing/None admission fails closed before any SQL or CLI access. No reader
    creates a private quota or falls back to an unbounded executor.
    """

    def __init__(
        self,
        *,
        caller_user_id: int,
        agent_id: str,
        is_group: bool,
        binding_id: str,
        connector_id: str,
        repo: MemoryLakeBindingRepo,
        secret_repo: SecretRepo,
        executable: Path,
        dashboard_thread_id: str | None = None,
        admission: ReadAdmission | None = None,
        database: DatabaseJobAdmission,
        check_invocation: Callable[[], None] | None = None,
    ) -> None:
        self._caller_user_id = caller_user_id
        self._agent_id = _identifier(agent_id)
        self._is_group = is_group
        self._binding_id = _identifier(binding_id)
        self._connector_id = _identifier(connector_id)
        self._dashboard_thread_id = (
            _identifier(dashboard_thread_id) if dashboard_thread_id is not None else None
        )
        self._dashboard_session_key = (
            f"{self._agent_id}:dashboard:{caller_user_id}:dm"
            if self._dashboard_thread_id is not None
            else None
        )
        self._repo = repo
        if not isinstance(database, DatabaseJobAdmission):
            raise StoredMemoryLakeError("shared_database_admission_required")
        self._database = database
        self._check_invocation = check_invocation
        self._service = MemoryLakeReadService(
            self._connector_id,
            executable=executable,
            load_connection=self._load_connection,
            load_credentials=self._load_credentials,
            admission=admission,
            check_invocation=check_invocation,
            admit_result=(
                self._admit_result
                if database is not None and self._dashboard_thread_id is not None
                else None
            ),
        )

    async def _admit_result(self, connection: ReadConnection, caller: CallerContext) -> None:
        """Durable sharing guard before any native observer receives ML material.

        Cancellation never publishes the result or rolls back a committed marker.
        The actual DB worker stays charged to the shared admission until settled.
        """
        self._check()
        database = self._database
        thread_id, session_key = self._dashboard_thread_id, self._dashboard_session_key
        if database is None or thread_id is None or session_key is None:
            raise StoredMemoryLakeError("material_admission_denied")
        if (
            caller.user_id != self._caller_user_id
            or caller.agent_id != self._agent_id
            or connection.scope.connector_id != self._connector_id
        ):
            raise StoredMemoryLakeError("material_admission_denied")
        await database.submit(
            lambda: self._repo.admit_agent_material(
                caller_user_id=self._caller_user_id,
                agent_id=self._agent_id,
                binding_id=self._binding_id,
                connector_id=self._connector_id,
                authorization_revision=connection.scope.authorization_revision,
                credential_revision=connection.credential_revision,
                thread_id=thread_id,
                session_key=session_key,
            )
        )
        self._check()

    def _snapshot(self) -> tuple[AuthorizedMemoryLakeBinding, ReadConnection]:
        if (
            type(self._caller_user_id) is not int
            or self._caller_user_id <= 0
            or self._is_group is not False
        ):
            raise StoredMemoryLakeError("binding_access_denied")
        authorized = self._repo.load_owned_active(
            binding_id=self._binding_id,
            caller_user_id=self._caller_user_id,
            agent_id=self._agent_id,
            dashboard_thread_id=self._dashboard_thread_id,
            dashboard_session_key=self._dashboard_session_key,
        )
        if authorized is None:
            raise StoredMemoryLakeError("binding_access_denied")
        return authorized, validate_connection_snapshot(
            authorized,
            caller_user_id=self._caller_user_id,
            agent_id=self._agent_id,
            binding_id=self._binding_id,
            connector_id=self._connector_id,
        )

    async def _authorized(self) -> tuple[AuthorizedMemoryLakeBinding, ReadConnection]:
        try:
            self._check()
            result = await self._database.submit(self._snapshot)
            self._check()
            return result
        except Exception:
            raise StoredMemoryLakeError("binding_unavailable") from None

    async def _load_connection(self) -> ReadConnection:
        _, connection = await self._authorized()
        return connection

    def _check(self) -> None:
        if (
            self._check_invocation is not None
            and cast("Callable[[], object]", self._check_invocation)() is not None
        ):
            raise StoredMemoryLakeError("binding_unavailable")

    async def _load_credentials(self) -> ReadCredentials:
        # Local import avoids the credential validator's storage import cycle.
        from octop.infra.connectors.memorylake.credentials import StoredMemoryLakeCredentials

        try:
            _, connection = await self._authorized()
            caller = CallerContext(
                self._caller_user_id,
                self._agent_id,
                connection.scope.owner_user_id,
                connection.scope.authorization_revision,
                self._is_group,
            )
            return await StoredMemoryLakeCredentials(
                caller=caller,
                binding_id=self._binding_id,
                connector_id=self._connector_id,
                repo=self._repo,
                admission=self._database,
                check_invocation=self._check,
                dashboard_thread_id=self._dashboard_thread_id,
            ).load()
        except Exception:
            raise StoredMemoryLakeError("credentials_unavailable") from None

    async def call(self, operation: str, arguments: Mapping[str, object]) -> dict[str, object]:
        _, connection = await self._authorized()
        caller = CallerContext(
            user_id=self._caller_user_id,
            agent_id=self._agent_id,
            agent_owner_user_id=connection.scope.owner_user_id,
            authorization_revision=connection.scope.authorization_revision,
            is_group=self._is_group,
        )
        return await self._service.call(caller, operation, arguments)
