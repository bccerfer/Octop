"""Fixed-binding credentials adapter with shared actual-worker admission.

Only synthetic encrypted values are used in its tests. No secret bootstrap,
cached Fernet, environment lookup or CLI execution happens in this adapter.
The return value is for a trusted transport only, never a tool/model/API result.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from octop.infra.connectors.crypto import decrypt_credentials_snapshot
from octop.infra.connectors.memorylake.contract import CallerContext, _authorize, _identifier
from octop.infra.connectors.memorylake.read_service import ReadConnection, ReadCredentials
from octop.infra.connectors.memorylake.storage import validate_connection_snapshot
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission
from octop.infra.crypto.fernet import PURPOSE_CONNECTOR
from octop.infra.db.repos.memorylake import (
    AuthorizedMemoryLakeCredentials,
    MemoryLakeBindingRepo,
)


class StoredCredentialsError(ValueError):
    """Constant reason only; never preserve upstream error text or secret repr."""


@dataclass(frozen=True)
class _Decoded:
    credentials: ReadCredentials = field(repr=False)
    connection: ReadConnection = field(repr=False)
    fingerprint: bytes = field(repr=False)


class StoredMemoryLakeCredentials:
    def __init__(
        self,
        *,
        caller: CallerContext,
        binding_id: str,
        connector_id: str,
        repo: MemoryLakeBindingRepo,
        admission: DatabaseJobAdmission,
        check_invocation: Callable[[], None],
        dashboard_thread_id: str | None = None,
    ) -> None:
        if not isinstance(caller, CallerContext) or not callable(check_invocation):
            raise StoredCredentialsError("credentials_unavailable")
        self._caller = caller
        self._binding_id = _identifier(binding_id)
        self._connector_id = _identifier(connector_id)
        self._repo = repo
        self._admission = admission
        self._check_invocation = check_invocation
        self._thread_id = (
            _identifier(dashboard_thread_id) if dashboard_thread_id is not None else None
        )

    def _check(self) -> None:
        caller = self._caller
        if (
            self._check_invocation() is not None
            or type(caller.user_id) is not int
            or caller.user_id <= 0
            or type(caller.agent_owner_user_id) is not int
            or caller.agent_owner_user_id != caller.user_id
            or caller.is_group is not False
            or type(caller.authorization_revision) is not int
            or caller.authorization_revision < 1
        ):
            raise StoredCredentialsError("credentials_unavailable")
        _identifier(caller.agent_id)

    def _material(self) -> tuple[AuthorizedMemoryLakeCredentials, ReadConnection]:
        snapshot = self._repo.load_owned_credentials(
            self._binding_id,
            caller_user_id=self._caller.user_id,
            agent_id=self._caller.agent_id,
            master_key_name=PURPOSE_CONNECTOR,
            dashboard_thread_id=self._thread_id,
            dashboard_session_key=(
                f"{self._caller.agent_id}:dashboard:{self._caller.user_id}:dm"
                if self._thread_id is not None
                else None
            ),
        )
        if snapshot is None:
            raise StoredCredentialsError("credentials_unavailable")
        connection = validate_connection_snapshot(
            snapshot,
            caller_user_id=self._caller.user_id,
            agent_id=self._caller.agent_id,
            binding_id=self._binding_id,
            connector_id=self._connector_id,
        )
        _authorize(connection.scope, self._caller)
        if (
            type(snapshot.master_key) is not bytes
            or not 1 <= len(snapshot.master_key) <= 4096
            or type(snapshot.credential_blob) is not bytes
            or not 1 <= len(snapshot.credential_blob) <= 65_536
        ):
            raise StoredCredentialsError("credentials_unavailable")
        return snapshot, connection

    @staticmethod
    def _fingerprint(snapshot: AuthorizedMemoryLakeCredentials) -> bytes:
        # Private equality evidence, not a public token; neither master nor blob
        # is retained in the coroutine while awaiting its final authorization.
        assert snapshot.master_key is not None
        digest = hashlib.sha256()
        digest.update(len(snapshot.master_key).to_bytes(4, "big"))
        digest.update(snapshot.master_key)
        digest.update(snapshot.credential_blob)
        return digest.digest()

    def _decode(self) -> _Decoded:
        snapshot, connection = self._material()
        assert snapshot.master_key is not None
        payload = decrypt_credentials_snapshot(snapshot.master_key, snapshot.credential_blob)
        key = payload.get("api_key")
        if (
            payload.get("instance_id") != self._connector_id
            or not isinstance(key, str)
            or not key.strip()
            or "\0" in key
            or len(key) > 8192
        ):
            raise StoredCredentialsError("credentials_unavailable")
        return _Decoded(
            ReadCredentials(key.strip(), connection.credential_revision),
            connection,
            self._fingerprint(snapshot),
        )

    def _current(self) -> tuple[ReadConnection, bytes, int | None]:
        snapshot, connection = self._material()
        return connection, self._fingerprint(snapshot), snapshot.credential_expires_at

    async def load(self) -> ReadCredentials:
        """Load once, then reauthorize/compare before disclosing to the transport.

        Cancellation returns promptly but does not release a running DB/decrypt
        job's shared admission. The owner must drain actual work before DB close.
        This is not a hard SQL timeout or secure erasure of Python string copies.
        """
        try:
            self._check()
            decoded = await asyncio.shield(self._admission.submit(self._decode))
            self._check()
            current, fingerprint, expires_at = await asyncio.shield(
                self._admission.submit(self._current)
            )
            self._check()
            # Worker validation is not the delivery time: executor/loop delay
            # can cross expiry after the snapshot was authorized. No await may
            # follow this final local expiry check before returning the key.
            if expires_at is not None:
                now = time.time()
                if type(now) not in (int, float) or not math.isfinite(now) or now >= expires_at:
                    raise StoredCredentialsError("credentials_expired")
            if current != decoded.connection or not hmac.compare_digest(
                fingerprint, decoded.fingerprint
            ):
                raise StoredCredentialsError("credentials_changed")
            return decoded.credentials
        except Exception:
            raise StoredCredentialsError("credentials_unavailable") from None
