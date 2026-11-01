"""Current-owner connection management, executed by a bounded server DB job.

No CLI/login/HTTP or native-memory changes. A connection write opt-in is NOT a
file approval. Secrets are encrypted from a fresh master snapshot, never cached.
"""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass

from cryptography.fernet import Fernet

from octop.infra.connectors.memorylake.contract import _identifier
from octop.infra.connectors.memorylake.read_service import _APPROVED_BASE_URLS
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.repos.memorylake import ManagedMemoryLakeBindingRow
from octop.infra.db.services import SharedServices
from octop.infra.utils.ulid import new_ulid


@dataclass(frozen=True)
class SafeBinding:
    binding_id: str
    connector_id: str
    workspace_id: str
    project_id: str
    library_parent_id: str
    project_ids: tuple[str, ...]
    writes_enabled: bool
    authorization_revision: int
    policy_revision: int
    display_name: str


class ConnectionManager:
    def __init__(self, services: SharedServices) -> None:
        self._services = services
        self._repo = services.memorylake_binding_repo

    def _owner(self, caller_user_id: int, agent_id: str) -> None:
        _identifier(agent_id)
        if not self._repo.owns_agent(caller_user_id=caller_user_id, agent_id=agent_id):
            raise ValueError

    @staticmethod
    def _safe(row: ManagedMemoryLakeBindingRow) -> SafeBinding:
        binding = row.binding
        if not isinstance(binding.project_ids_json, str) or len(binding.project_ids_json) > 16_384:
            raise ValueError
        projects = json.loads(binding.project_ids_json)
        if not isinstance(projects, list) or not 1 <= len(projects) <= 50:
            raise ValueError
        ids = tuple(_identifier(project) for project in projects)
        if row.project_id not in ids:
            raise ValueError
        return SafeBinding(
            _identifier(binding.binding_id),
            _identifier(binding.connector_id),
            _identifier(binding.workspace_id),
            _identifier(row.project_id),
            _identifier(row.library_parent_id),
            ids,
            row.writes_enabled,
            binding.authorization_revision,
            row.policy_revision,
            row.display_name,
        )

    def create(
        self,
        caller_user_id: int,
        agent_id: str,
        *,
        api_key: str,
        base_url: str,
        workspace_id: str,
        project_id: str,
        library_parent_id: str,
        display_name: str,
        writes_enabled: bool = False,
    ) -> SafeBinding:
        try:
            self._owner(caller_user_id, agent_id)
            for value in (workspace_id, project_id, library_parent_id):
                _identifier(value)
            if (
                base_url not in _APPROVED_BASE_URLS
                or type(writes_enabled) is not bool
                or not isinstance(api_key, str)
                or not 1 <= len(api_key) <= 8192
                or not api_key.strip()
                or any(ord(c) < 32 or ord(c) == 127 for c in api_key)
                or not isinstance(display_name, str)
                or not 1 <= len(display_name) <= 128
                or not display_name.strip()
                or any(unicodedata.category(c).startswith("C") for c in display_name)
            ):
                raise ValueError
            connector_id, binding_id = "mlc_" + new_ulid(), "mlb_" + new_ulid()
            master = self._services.secret_repo.get_bounded(PURPOSE_CONNECTOR)
            if master is None:
                master = Fernet.generate_key()
            cipher = Fernet(master).encrypt(
                json.dumps(
                    {"instance_id": connector_id, "api_key": api_key.strip()}, separators=(",", ":")
                ).encode()
            )
            row = self._repo.create_managed_owned(
                caller_user_id=caller_user_id,
                agent_id=agent_id,
                binding_id=binding_id,
                connector_id=connector_id,
                display_name=display_name.strip(),
                base_url=base_url,
                workspace_id=workspace_id,
                project_id=project_id,
                project_ids_json=json.dumps([project_id], separators=(",", ":")),
                library_parent_id=library_parent_id,
                writes_enabled=writes_enabled,
                credential_blob=cipher,
                master_key_name=PURPOSE_CONNECTOR,
                master_key=master,
            )
            return self._safe(row)
        except Exception:
            raise ValueError("memorylake_connection_unavailable") from None

    def list_owned(self, caller_user_id: int, agent_id: str) -> list[SafeBinding]:
        try:
            self._owner(caller_user_id, agent_id)
            return [
                self._safe(row)
                for row in self._repo.list_managed_owned(
                    caller_user_id=caller_user_id, agent_id=agent_id
                )
            ]
        except Exception:
            raise ValueError("memorylake_connection_unavailable") from None

    def set_write_policy(
        self,
        caller_user_id: int,
        agent_id: str,
        binding_id: str,
        *,
        enabled: bool,
        expected_revision: int,
    ) -> SafeBinding:
        try:
            self._owner(caller_user_id, agent_id)
            _identifier(binding_id)
            if (
                type(enabled) is not bool
                or type(expected_revision) is not int
                or expected_revision < 1
            ):
                raise ValueError
            current = next(
                (
                    row
                    for row in self._repo.list_managed_owned(
                        caller_user_id=caller_user_id, agent_id=agent_id
                    )
                    if row.binding.binding_id == binding_id
                ),
                None,
            )
            if current is None:
                raise ValueError
            snapshot = self._safe(current)
            return self._safe(
                self._repo.set_managed_write_policy(
                    caller_user_id=caller_user_id,
                    agent_id=agent_id,
                    binding_id=binding_id,
                    enabled=enabled,
                    expected_revision=expected_revision,
                    expected_authorization_revision=snapshot.authorization_revision,
                )
            )
        except Exception:
            raise ValueError("memorylake_connection_unavailable") from None
