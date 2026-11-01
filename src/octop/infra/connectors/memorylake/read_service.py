"""Read execution boundary with invocation-time authorization.

Storage and authenticated caller adapters are supplied by trusted composition.
The service owns a private runner. Production factories must inject the same
server-owned ReadAdmission into independent per-turn service instances. No writes, retries, tool registration or native-memory
changes happen in this module.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from octop.infra.connectors.memorylake.admission import ReadAdmission
from octop.infra.connectors.memorylake.contract import CallerContext, ReadScope, plan_read
from octop.infra.connectors.memorylake.results import normalize_read_result
from octop.infra.connectors.memorylake.runner import MemoryLakeRunner, MemoryLakeRunnerError
from octop.infra.connectors.memorylake.session import async_isolated_cli_session

# Only the documented deployments are supported in this first slice. A custom
# endpoint needs an explicit deployment-level approval/SSRF policy, not tool args.
_APPROVED_BASE_URLS = frozenset(
    {
        "https://app.memorylake.ai/openapi/memorylake",
        "https://app.memorylake.cn/openapi/memorylake",
    }
)


def _redact(value: object, secret: str) -> object:
    # Sanitize decoded strings: raw JSON replacement misses escaped key echoes.
    if isinstance(value, str):
        return value.replace(secret, "[redacted]")
    if isinstance(value, list):
        return [_redact(item, secret) for item in value]
    if isinstance(value, dict):
        return {
            str(key).replace(secret, "[redacted]"): _redact(item, secret)
            for key, item in value.items()
        }
    return value


class MemoryLakeReadError(ValueError):
    """Internal reason code; localize only at a future transport boundary."""


@dataclass(frozen=True)
class ReadConnection:
    scope: ReadScope
    base_url: str
    credential_revision: int


@dataclass(frozen=True)
class ReadCredentials:
    # The loader must read key + revision atomically from the same stored record.
    api_key: str = field(repr=False)
    revision: int


class MemoryLakeReadService:
    """Fresh binding + credentials, private session, bounded read-only process.

    Loaders must read the current connection/permissions, not a startup snapshot.
    Credentials must be loaded atomically with their monotonically increasing
    revision; a key change must increment the connection credential_revision.
    The authenticated CallerContext must be constructed for the actual turn.
    Revocation cannot retract a request that has already reached the remote API.
    """

    def __init__(
        self,
        connector_id: str,
        *,
        executable: Path,
        load_connection: Callable[[], Awaitable[ReadConnection]],
        load_credentials: Callable[[], Awaitable[ReadCredentials]],
        admission: ReadAdmission | None = None,
        check_invocation: Callable[[], None] | None = None,
        admit_result: Callable[[ReadConnection, CallerContext], Awaitable[None]] | None = None,
    ) -> None:
        self._connector_id = connector_id
        self._load_connection = load_connection
        self._load_credentials = load_credentials
        self._check_invocation = check_invocation
        self._admit_result = admit_result
        self._runner = MemoryLakeRunner(executable)
        # Compatibility-only fallback: isolated instances cannot coordinate turns.
        # Production factories must pass a server-owned shared admission object.
        self._admission = admission if admission is not None else ReadAdmission(max_active=1)

    async def _current_connection(self) -> ReadConnection:
        try:
            if self._check_invocation is not None:
                self._check_invocation()
            connection = await self._load_connection()
            if self._check_invocation is not None:
                self._check_invocation()
        except Exception:
            raise MemoryLakeReadError("connection_unavailable") from None
        if not isinstance(connection, ReadConnection):
            raise MemoryLakeReadError("connection_unavailable")
        if type(connection.credential_revision) is not int or connection.credential_revision < 1:
            raise MemoryLakeReadError("connection_unavailable")
        if connection.scope.connector_id != self._connector_id:
            raise MemoryLakeReadError("connection_mismatch")
        if connection.base_url not in _APPROVED_BASE_URLS:
            raise MemoryLakeReadError("endpoint_not_approved")
        return connection

    async def call(
        self,
        caller: CallerContext,
        operation: str,
        arguments: Mapping[str, object],
    ) -> dict[str, object]:
        # Snapshot the one mutable allowed argument before waiting for admission.
        supplied = {
            key: tuple(value) if key == "project_ids" and isinstance(value, list) else value
            for key, value in arguments.items()
        }
        async with self._admission.acquire(self._connector_id):
            connection = await self._current_connection()
            # Reject before touching credentials or materializing a private folder.
            plan_read(connection.scope, caller, operation, supplied)
            try:
                credentials = await self._load_credentials()
            except Exception:
                raise MemoryLakeReadError("credentials_unavailable") from None
            if not isinstance(credentials, ReadCredentials):
                raise MemoryLakeReadError("credentials_unavailable")
            if (
                type(credentials.revision) is not int
                or credentials.revision != connection.credential_revision
            ):
                raise MemoryLakeReadError("credentials_changed")
            api_key = credentials.api_key
            if not isinstance(api_key, str) or not api_key.strip():
                raise MemoryLakeReadError("credentials_unavailable")
            api_key = api_key.strip()
            async with async_isolated_cli_session(api_key) as session:
                # Loading secrets and provisioning the directory can suspend. Check
                # live consent again immediately before the private runner is used.
                current = await self._current_connection()
                if current != connection:
                    raise MemoryLakeReadError("authorization_changed")
                command = plan_read(current.scope, caller, operation, supplied)
                argv = (
                    "--profile",
                    session.profile,
                    "--base-url",
                    current.base_url,
                    *command.arguments,
                )
                try:
                    result = await self._runner.run(
                        argv,
                        env=session.environment,
                        cwd=Path(session.environment["MEMORYLAKE_CONFIG_DIR"]),
                    )
                except MemoryLakeRunnerError:
                    # Partial output is not a successful read and must not be shown.
                    raise MemoryLakeReadError("external_read_failed") from None
                if result.returncode != 0:
                    raise MemoryLakeReadError("external_read_failed")
                projects: tuple[str, ...] = ()
                if operation == "search_documents":
                    index = command.arguments.index("--projects")
                    projects = tuple(command.arguments[index + 1].split(","))
                elif operation == "get_document":
                    index = command.arguments.index("--project")
                    projects = (command.arguments[index + 1],)
                expected_id = (
                    command.arguments[-1]
                    if operation in {"get_document", "get_fact", "trace_fact"}
                    else None
                )
                try:
                    # Upstream output is untrusted, including accidental key echoes.
                    stdout = result.stdout.decode("utf-8")
                    response = normalize_read_result(
                        operation,
                        stdout,
                        project_ids=projects,
                        actor_id=current.scope.actor_id,
                        expected_id=expected_id,
                    )
                    response = {key: _redact(value, api_key) for key, value in response.items()}
                except (UnicodeError, ValueError, RecursionError):
                    raise MemoryLakeReadError("invalid_read_response") from None
            # Cleanup also suspends. Discard in-flight reads after revocation or
            # credential rotation, even though the remote request cannot be undone.
            if await self._current_connection() != connection:
                raise MemoryLakeReadError("authorization_changed")
            if self._admit_result is not None:
                try:
                    await self._admit_result(connection, caller)
                except Exception:
                    raise MemoryLakeReadError("material_admission_denied") from None
            if self._check_invocation is not None:
                self._check_invocation()
            return {
                **response,
                "connector_id": current.scope.connector_id,
                "workspace_id": current.scope.workspace_id,
            }
