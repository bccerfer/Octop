"""Owner-only MemoryLake read-binding persistence, not a write-approval ledger.

Scope validation/canonicalization belongs to the domain adapter. Authorization
loads use one SQL snapshot; this does not provide identity-history/ABA epochs.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, cast

from octop.infra.db.pool import DatabasePool
from octop.infra.db.repos._base import DbRow, bool_int, now_ts


def _bounded_binary(value: object, maximum: int) -> bytes:
    """Validate driver values BEFORE copying; bytes(integer) allocates zeros."""
    if isinstance(value, memoryview):
        size = value.nbytes
    elif isinstance(value, (bytes, bytearray)):
        size = len(value)
    else:
        raise ValueError("invalid_credential_snapshot")
    if not 1 <= size <= maximum:
        raise ValueError("invalid_credential_snapshot")
    return bytes(value)


@dataclass(frozen=True)
class MemoryLakeBindingRow:
    binding_id: str
    connector_id: str
    owner_user_id: int
    agent_id: str
    base_url: str
    workspace_id: str
    project_ids_json: str
    actor_id: str | None
    enabled: bool
    authorization_revision: int
    created_at: int
    updated_at: int

    @classmethod
    def from_row(cls, row: DbRow) -> MemoryLakeBindingRow:
        return cls(
            binding_id=row["binding_id"],
            connector_id=row["connector_id"],
            owner_user_id=row["owner_user_id"],
            agent_id=row["agent_id"],
            base_url=row["base_url"],
            workspace_id=row["workspace_id"],
            project_ids_json=row["project_ids_json"],
            actor_id=row["actor_id"],
            enabled=bool(row["enabled"]),
            authorization_revision=row["authorization_revision"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@dataclass(frozen=True)
class AuthorizedMemoryLakeBinding:
    binding: MemoryLakeBindingRow
    credential_revision: int
    credential_blob: bytes = field(repr=False)
    credential_expires_at: int | None


@dataclass(frozen=True)
class AuthorizedMemoryLakeCredentials(AuthorizedMemoryLakeBinding):
    master_key: bytes | None = field(repr=False)


@dataclass(frozen=True)
class AuthorizedMemoryLakeWriteBinding:
    binding: MemoryLakeBindingRow
    credential_revision: int
    credential_expires_at: int | None
    writes_enabled: bool
    project_id: str
    library_parent_id: str
    policy_revision: int


@dataclass(frozen=True)
class ManagedMemoryLakeBindingRow:
    binding: MemoryLakeBindingRow
    display_name: str
    project_id: str
    library_parent_id: str
    writes_enabled: bool
    policy_revision: int

    @classmethod
    def from_row(cls, row: DbRow) -> ManagedMemoryLakeBindingRow:
        return cls(
            MemoryLakeBindingRow.from_row(row),
            row["display_name"],
            row["project_id"],
            row["library_parent_id"],
            bool(row["writes_enabled"]),
            row["policy_revision"],
        )


class MemoryLakeBindingRepo:
    def __init__(self, db: DatabasePool) -> None:
        self._db = db

    def owns_dashboard_thread(
        self,
        *,
        caller_user_id: int,
        agent_id: str,
        thread_id: str,
        session_key: str,
    ) -> bool:
        """Current owner-DM identity in one SQL snapshot, not binding authorization.

        Only expert agents are supported; team hosts do not inherit member access.
        No administrator override and no credential access. Callers doing async
        work must invoke this synchronous repository method off the event loop.
        """
        if (
            type(caller_user_id) is not int
            or caller_user_id <= 0
            or type(agent_id) is not str
            or not agent_id
            or type(thread_id) is not str
            or not thread_id
            or type(session_key) is not str
            or session_key != f"{agent_id}:dashboard:{caller_user_id}:dm"
        ):
            return False
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM threads t "
                "JOIN agents a ON a.agent_id = t.agent_id "
                "JOIN users u ON u.id = t.user_id "
                "WHERE t.thread_id = ? AND t.session_key = ? "
                "AND t.channel_type = 'dashboard' AND t.agent_id = ? "
                "AND t.user_id = ? AND a.user_id = t.user_id "
                "AND u.disabled = 0 AND a.enabled = 1 AND a.is_shared = 0 "
                "AND a.kind = 'expert'",
                (thread_id, session_key, agent_id, caller_user_id),
            ).fetchone()
        return row is not None

    def create(
        self,
        *,
        binding_id: str,
        connector_id: str,
        owner_user_id: int,
        agent_id: str,
        base_url: str,
        workspace_id: str,
        project_ids_json: str,
        actor_id: str | None = None,
        enabled: bool = False,
    ) -> MemoryLakeBindingRow:
        ts = now_ts()
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT INTO memorylake_bindings("
                "binding_id,connector_id,owner_user_id,agent_id,base_url,workspace_id,"
                "project_ids_json,actor_id,enabled,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    binding_id,
                    connector_id,
                    owner_user_id,
                    agent_id,
                    base_url,
                    workspace_id,
                    project_ids_json,
                    actor_id,
                    bool_int(enabled),
                    ts,
                    ts,
                ),
            )
            row = conn.execute(
                "SELECT * FROM memorylake_bindings WHERE binding_id = ?", (binding_id,)
            ).fetchone()
        assert row is not None
        return MemoryLakeBindingRow.from_row(row)

    def replace_scope(
        self,
        *,
        binding_id: str,
        owner_user_id: int,
        expected_revision: int,
        base_url: str,
        workspace_id: str,
        project_ids_json: str,
        actor_id: str | None,
        enabled: bool,
    ) -> MemoryLakeBindingRow | None:
        with self._db.transaction() as conn:
            cursor = conn.execute(
                "UPDATE memorylake_bindings SET base_url=?,workspace_id=?,project_ids_json=?,"
                "actor_id=?,enabled=?,authorization_revision=authorization_revision+1,updated_at=? "
                "WHERE binding_id=? AND owner_user_id=? AND authorization_revision=?",
                (
                    base_url,
                    workspace_id,
                    project_ids_json,
                    actor_id,
                    bool_int(enabled),
                    now_ts(),
                    binding_id,
                    owner_user_id,
                    expected_revision,
                ),
            )
            if cursor.rowcount != 1:
                return None
            row = conn.execute(
                "SELECT * FROM memorylake_bindings WHERE binding_id=?", (binding_id,)
            ).fetchone()
        assert row is not None
        return MemoryLakeBindingRow.from_row(row)

    def _load_owned_row(
        self,
        binding_id: str,
        *,
        caller_user_id: int,
        agent_id: str,
        dashboard_thread_id: str | None = None,
        dashboard_session_key: str | None = None,
        with_write_policy: bool = False,
        master_key_name: str | None = None,
    ) -> DbRow | None:
        # A dashboard tool adds its exact thread to THIS query, not a separate
        # await before/after authorization. Other trusted non-tool adapters may
        # omit both fields. Supplying only half a scope must never broaden it.
        if (dashboard_thread_id is None) != (dashboard_session_key is None):
            return None
        parameters: tuple[object, ...] = (binding_id, caller_user_id, agent_id)
        thread_predicate = ""
        if dashboard_thread_id is not None:
            # Only fixed SQL is assembled. Avoid an independent "? IS NULL"
            # parameter whose type PostgreSQL cannot infer when None is bound.
            thread_predicate = (
                " AND EXISTS (SELECT 1 FROM threads t "
                "WHERE t.thread_id=? AND t.agent_id=b.agent_id AND t.user_id=b.owner_user_id "
                "AND t.channel_type='dashboard' AND t.session_key=?)"
            )
            parameters += (dashboard_thread_id, dashboard_session_key)
        projection = "c.credential_blob"
        policy_join = ""
        if master_key_name is not None:
            if with_write_policy:
                raise ValueError("incompatible_snapshot_projection")
            # Secret purpose is a fixed server value supplied by the adapter,
            # never SQL syntax or a client-selected credentials lookup. Both
            # secret and permission/ciphertext fields share this statement's
            # read snapshot; no key is returned for an unauthorized row.
            projection += ",(SELECT v FROM secrets WHERE k=?) AS master_key"
            parameters = (master_key_name, *parameters)
        if with_write_policy:
            projection = (
                "p.enabled AS writes_enabled,p.project_id,p.library_parent_id,"
                "p.revision AS policy_revision"
            )
            policy_join = "LEFT JOIN memorylake_write_policies p ON p.binding_id=b.binding_id "
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT b.*,c.credential_revision,c.credential_expires_at," + projection + " "
                "FROM memorylake_bindings b "
                "JOIN users u ON u.id=b.owner_user_id "
                "JOIN agents a ON a.agent_id=b.agent_id "
                "JOIN connectors c ON c.instance_id=b.connector_id "
                + policy_join
                + "WHERE b.binding_id=? AND b.owner_user_id=? AND b.agent_id=? AND b.enabled=1 "
                "AND u.disabled=0 AND a.user_id=b.owner_user_id AND a.enabled=1 AND a.is_shared=0 "
                "AND a.kind='expert' "
                "AND c.user_id=b.owner_user_id AND c.kind='memorylake' AND c.status='active' "
                "AND c.shared=0 AND c.credential_blob IS NOT NULL AND length(c.credential_blob)>0 "
                "AND c.credential_revision>0" + thread_predicate,
                parameters,
            ).fetchone()
        return cast(DbRow | None, row)

    def load_owned_active(
        self,
        binding_id: str,
        *,
        caller_user_id: int,
        agent_id: str,
        dashboard_thread_id: str | None = None,
        dashboard_session_key: str | None = None,
    ) -> AuthorizedMemoryLakeBinding | None:
        row = self._load_owned_row(
            binding_id,
            caller_user_id=caller_user_id,
            agent_id=agent_id,
            dashboard_thread_id=dashboard_thread_id,
            dashboard_session_key=dashboard_session_key,
        )
        if row is None:
            return None
        return AuthorizedMemoryLakeBinding(
            binding=MemoryLakeBindingRow.from_row(row),
            credential_revision=row["credential_revision"],
            credential_blob=_bounded_binary(row["credential_blob"], 65_536),
            credential_expires_at=row["credential_expires_at"],
        )

    def load_owned_credentials(
        self,
        binding_id: str,
        *,
        caller_user_id: int,
        agent_id: str,
        master_key_name: str,
        dashboard_thread_id: str | None = None,
        dashboard_session_key: str | None = None,
    ) -> AuthorizedMemoryLakeCredentials | None:
        """One authorized cipher/revision/master snapshot, without bootstrap.

        Optional dashboard thread constraints belong to this SAME SQL snapshot.
        The domain layer validates and decrypts values.
        """
        row = self._load_owned_row(
            binding_id,
            caller_user_id=caller_user_id,
            agent_id=agent_id,
            master_key_name=master_key_name,
            dashboard_thread_id=dashboard_thread_id,
            dashboard_session_key=dashboard_session_key,
        )
        if row is None:
            return None
        return AuthorizedMemoryLakeCredentials(
            binding=MemoryLakeBindingRow.from_row(row),
            credential_revision=row["credential_revision"],
            credential_blob=_bounded_binary(row["credential_blob"], 65_536),
            credential_expires_at=row["credential_expires_at"],
            master_key=(
                _bounded_binary(row["master_key"], 4096) if row["master_key"] is not None else None
            ),
        )

    def load_owned_write_binding(
        self,
        binding_id: str,
        *,
        caller_user_id: int,
        agent_id: str,
        dashboard_thread_id: str | None = None,
        dashboard_session_key: str | None = None,
    ) -> AuthorizedMemoryLakeWriteBinding | None:
        """One current read-permission/write-policy snapshot, not a write grant.

        Disabled or missing write policies do not remove existing read authorization.
        No encrypted blob or master secret is returned by this query.
        """
        row = self._load_owned_row(
            binding_id,
            caller_user_id=caller_user_id,
            agent_id=agent_id,
            dashboard_thread_id=dashboard_thread_id,
            dashboard_session_key=dashboard_session_key,
            with_write_policy=True,
        )
        if row is None:
            return None
        return AuthorizedMemoryLakeWriteBinding(
            binding=MemoryLakeBindingRow.from_row(row),
            credential_revision=row["credential_revision"],
            credential_expires_at=row["credential_expires_at"],
            writes_enabled=bool(row["writes_enabled"]),
            project_id=row["project_id"] if row["project_id"] is not None else "",
            library_parent_id=row["library_parent_id"]
            if row["library_parent_id"] is not None
            else "",
            policy_revision=row["policy_revision"] if row["policy_revision"] is not None else 0,
        )

    def _management_lock(self, conn: Any, table: str, column: str, value: object) -> None:
        # Identifiers are fixed call-site constants, never caller-controlled SQL.
        if self._db.dialect == "postgresql":
            conn.execute(
                f"SELECT {column} FROM {table} WHERE {column}=? FOR UPDATE", (value,)
            ).fetchone()

    def _management_owner(
        self, conn: Any, caller_user_id: int, agent_id: str, *, lock: bool = False
    ) -> bool:
        if (
            type(caller_user_id) is not int
            or caller_user_id <= 0
            or type(agent_id) is not str
            or not agent_id
        ):
            return False
        if lock:
            self._management_lock(conn, "users", "id", caller_user_id)
            self._management_lock(conn, "agents", "agent_id", agent_id)
        return (
            conn.execute(
                "SELECT 1 FROM agents a JOIN users u ON u.id=a.user_id "
                "WHERE a.agent_id=? AND a.user_id=? AND a.enabled=1 AND a.is_shared=0 "
                "AND a.kind='expert' AND u.disabled=0",
                (agent_id, caller_user_id),
            ).fetchone()
            is not None
        )

    def agent_material_owner(self, agent_id: str) -> int | None:
        """Content-free retention guard; only an absent row returns None.

        Missing schema, failed SQL and corrupt marker values fail closed. Caller
        maps database errors at its domain boundary, not to an unmarked agent.
        """
        if type(agent_id) is not str or not agent_id:
            raise ValueError("memorylake_admission_denied")
        with self._db.connect() as conn:
            row = conn.execute(
                "SELECT owner_user_id FROM memorylake_agent_admissions WHERE agent_id=?",
                (agent_id,),
            ).fetchone()
        if row is None:
            return None
        owner = row["owner_user_id"]
        if type(owner) is not int or owner <= 0:
            raise ValueError("memorylake_admission_denied")
        return owner

    def admit_agent_material(
        self,
        *,
        caller_user_id: int,
        agent_id: str,
        binding_id: str,
        connector_id: str,
        authorization_revision: int,
        credential_revision: int,
        thread_id: str,
        session_key: str,
    ) -> None:
        """Commit content-free acceptance before releasing ML data to native sinks.

        Shares the agent-row serialization point with AgentRepo.set_shared.
        Caller must run this transaction off-loop and await its actual outcome.
        This guards sharing, not publication/export or arbitrary data propagation.
        """
        if (
            type(caller_user_id) is not int
            or caller_user_id <= 0
            or type(authorization_revision) is not int
            or authorization_revision <= 0
            or type(credential_revision) is not int
            or credential_revision <= 0
            or any(
                type(value) is not str or not value
                for value in (agent_id, binding_id, connector_id, thread_id, session_key)
            )
            or session_key != f"{agent_id}:dashboard:{caller_user_id}:dm"
        ):
            raise ValueError("memorylake_admission_denied")
        with self._db.transaction() as conn:
            # Match submission/management lock order; credentials live on c.
            for table, column, value in (
                ("users", "id", caller_user_id),
                ("agents", "agent_id", agent_id),
                ("connectors", "instance_id", connector_id),
                ("memorylake_bindings", "binding_id", binding_id),
                ("threads", "thread_id", thread_id),
            ):
                self._management_lock(conn, table, column, value)
            allowed = conn.execute(
                "SELECT c.credential_expires_at FROM memorylake_bindings b "
                "JOIN users u ON u.id=b.owner_user_id "
                "JOIN agents a ON a.agent_id=b.agent_id "
                "JOIN connectors c ON c.instance_id=b.connector_id "
                "JOIN threads t ON t.agent_id=a.agent_id AND t.user_id=u.id "
                "WHERE b.binding_id=? AND b.connector_id=? AND b.owner_user_id=? "
                "AND b.agent_id=? AND b.enabled=1 AND b.authorization_revision=? "
                "AND u.disabled=0 AND a.user_id=u.id AND a.enabled=1 "
                "AND a.kind='expert' AND a.is_shared=0 "
                "AND c.user_id=u.id AND c.kind='memorylake' AND c.status='active' "
                "AND c.shared=0 AND c.credential_revision=? "
                "AND c.credential_blob IS NOT NULL AND length(c.credential_blob)>0 "
                "AND (c.credential_expires_at IS NULL OR c.credential_expires_at>?) "
                "AND t.thread_id=? AND t.session_key=? AND t.channel_type='dashboard'",
                (
                    binding_id,
                    connector_id,
                    caller_user_id,
                    agent_id,
                    authorization_revision,
                    credential_revision,
                    now_ts(),
                    thread_id,
                    session_key,
                ),
            ).fetchone()
            expiry = allowed["credential_expires_at"] if allowed is not None else None
            if allowed is None or (
                expiry is not None
                and (
                    type(expiry) not in (int, float)
                    or not math.isfinite(expiry)
                    or expiry <= now_ts()
                )
            ):
                raise ValueError("memorylake_admission_denied")
            conn.execute(
                "INSERT INTO memorylake_agent_admissions(agent_id,owner_user_id,first_admitted_at) "
                "VALUES (?,?,?) ON CONFLICT (agent_id) DO NOTHING",
                (agent_id, caller_user_id, now_ts()),
            )
            marker = conn.execute(
                "SELECT owner_user_id FROM memorylake_agent_admissions WHERE agent_id=?",
                (agent_id,),
            ).fetchone()
            if marker is None or marker["owner_user_id"] != caller_user_id:
                raise ValueError("memorylake_admission_denied")

    def owns_agent(self, *, caller_user_id: int, agent_id: str) -> bool:
        """Fresh owner/expert/private authorization, with no admin override."""
        with self._db.connect() as conn:
            return self._management_owner(conn, caller_user_id, agent_id)

    def _managed_rows(
        self, conn: Any, caller_user_id: int, agent_id: str, binding_id: str | None = None
    ) -> list[ManagedMemoryLakeBindingRow]:
        predicate = " AND b.binding_id=?" if binding_id is not None else ""
        values: tuple[object, ...] = (caller_user_id, agent_id)
        if binding_id is not None:
            values += (binding_id,)
        rows = conn.execute(
            "SELECT b.*,c.display_name,p.project_id,p.library_parent_id,"
            "p.enabled AS writes_enabled,p.revision AS policy_revision "
            "FROM memorylake_bindings b JOIN users u ON u.id=b.owner_user_id "
            "JOIN agents a ON a.agent_id=b.agent_id "
            "JOIN connectors c ON c.instance_id=b.connector_id "
            "JOIN memorylake_write_policies p ON p.binding_id=b.binding_id "
            "WHERE b.owner_user_id=? AND b.agent_id=? AND b.enabled=1 "
            "AND u.disabled=0 AND a.user_id=b.owner_user_id AND a.enabled=1 "
            "AND a.is_shared=0 AND a.kind='expert' AND c.user_id=b.owner_user_id "
            "AND c.kind='memorylake' AND c.status='active' AND c.shared=0 "
            "AND c.credential_blob IS NOT NULL AND length(c.credential_blob)>0 "
            "AND c.credential_revision>0" + predicate + " ORDER BY b.binding_id LIMIT 101",
            values,
        ).fetchall()
        if len(rows) > 100:
            raise ValueError("connection_capacity_exceeded")
        return [ManagedMemoryLakeBindingRow.from_row(row) for row in rows]

    def list_managed_owned(
        self, *, caller_user_id: int, agent_id: str
    ) -> list[ManagedMemoryLakeBindingRow]:
        with self._db.connect() as conn:
            if not self._management_owner(conn, caller_user_id, agent_id):
                raise ValueError("connection_access_denied")
            return self._managed_rows(conn, caller_user_id, agent_id)

    def create_managed_owned(
        self,
        *,
        caller_user_id: int,
        agent_id: str,
        binding_id: str,
        connector_id: str,
        display_name: str,
        base_url: str,
        workspace_id: str,
        project_id: str,
        project_ids_json: str,
        library_parent_id: str,
        writes_enabled: bool,
        credential_blob: bytes,
        master_key_name: str,
        master_key: bytes,
    ) -> ManagedMemoryLakeBindingRow:
        """One current-owner transaction: key bootstrap + connector + scope + policy.

        Domain provides validated values and an uncached encryption snapshot.
        A concurrent master rotation rejects the entire write, never stores an
        undecryptable partial connection. PostgreSQL uses the ledger lock order.
        """
        ts = now_ts()
        with self._db.transaction() as conn:
            if not self._management_owner(conn, caller_user_id, agent_id, lock=True):
                raise ValueError("connection_access_denied")
            # Bound persistent resources as well as the list response.
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM memorylake_bindings WHERE owner_user_id=? AND agent_id=?",
                (caller_user_id, agent_id),
            ).fetchone()
            if count["n"] >= 100:
                raise ValueError("connection_capacity_exceeded")
            conn.execute(
                "INSERT INTO secrets(k,v,created_at) VALUES (?,?,?) ON CONFLICT(k) DO NOTHING",
                (master_key_name, master_key, ts),
            )
            self._management_lock(conn, "secrets", "k", master_key_name)
            key = conn.execute(
                "SELECT CASE WHEN length(v) BETWEEN 1 AND 4096 THEN v ELSE NULL END AS v FROM secrets WHERE k=?",
                (master_key_name,),
            ).fetchone()
            if key is None or _bounded_binary(key["v"], 4096) != master_key:
                raise ValueError("connection_master_changed")
            conn.execute(
                "INSERT INTO connectors(instance_id,user_id,kind,display_name,status,shared,mcp_server_name,credential_blob,credential_revision,credential_rotated_at,created_at,updated_at) "
                "VALUES (?,?,'memorylake',?,'active',0,?,?,1,?,?,?)",
                (
                    connector_id,
                    caller_user_id,
                    display_name,
                    connector_id,
                    credential_blob,
                    ts,
                    ts,
                    ts,
                ),
            )
            conn.execute(
                "INSERT INTO memorylake_bindings(binding_id,connector_id,owner_user_id,agent_id,base_url,workspace_id,project_ids_json,enabled,created_at,updated_at) VALUES (?,?,?,?,?,?,?,1,?,?)",
                (
                    binding_id,
                    connector_id,
                    caller_user_id,
                    agent_id,
                    base_url,
                    workspace_id,
                    project_ids_json,
                    ts,
                    ts,
                ),
            )
            conn.execute(
                "INSERT INTO memorylake_write_policies(binding_id,enabled,project_id,library_parent_id,revision) VALUES (?,?,?,?,1)",
                (binding_id, int(writes_enabled), project_id, library_parent_id),
            )
            return self._managed_rows(conn, caller_user_id, agent_id, binding_id)[0]

    def set_managed_write_policy(
        self,
        *,
        caller_user_id: int,
        agent_id: str,
        binding_id: str,
        enabled: bool,
        expected_revision: int,
        expected_authorization_revision: int,
    ) -> ManagedMemoryLakeBindingRow:
        with self._db.transaction() as conn:
            if not self._management_owner(conn, caller_user_id, agent_id, lock=True):
                raise ValueError("connection_access_denied")
            binding = conn.execute(
                "SELECT connector_id FROM memorylake_bindings WHERE binding_id=? AND owner_user_id=? AND agent_id=?",
                (binding_id, caller_user_id, agent_id),
            ).fetchone()
            if binding is None:
                raise ValueError("connection_access_denied")
            self._management_lock(conn, "connectors", "instance_id", binding["connector_id"])
            self._management_lock(conn, "memorylake_bindings", "binding_id", binding_id)
            self._management_lock(conn, "memorylake_write_policies", "binding_id", binding_id)
            current = self._managed_rows(conn, caller_user_id, agent_id, binding_id)
            if not current:
                raise ValueError("connection_access_denied")
            if current[0].binding.authorization_revision != expected_authorization_revision:
                raise ValueError("connection_authorization_changed")
            cursor = conn.execute(
                "UPDATE memorylake_write_policies SET enabled=?,revision=revision+1 WHERE binding_id=? AND revision=?",
                (int(enabled), binding_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise ValueError("write_policy_changed")
            return self._managed_rows(conn, caller_user_id, agent_id, binding_id)[0]
