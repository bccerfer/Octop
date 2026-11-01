"""Optional server-owned CLI readiness and trusted file-transport composition.

Disabled deployments create nothing. Enabled deployments inspect only an operator
pinned local artifact, then run its fixed --version under an empty private HOME.
A matching hash/version is not release provenance or a sandbox: the artifact must
be trusted and read-only (same-UID replacement/inspection-to-exec races remain a
deployment assumption). No install, PATH fallback, login or automatic approval.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, cast

from octop.infra.connectors.memorylake.admission import ReadAdmission
from octop.infra.connectors.memorylake.bound_reader import read_bound_documents
from octop.infra.connectors.memorylake.cli_transport import MemoryLakeCliTransport
from octop.infra.connectors.memorylake.contract import CallerContext, _identifier
from octop.infra.connectors.memorylake.credentials import StoredMemoryLakeCredentials
from octop.infra.connectors.memorylake.deployment import CliArtifact, inspect_cli_artifact
from octop.infra.connectors.memorylake.file_binding import TransportFactory
from octop.infra.connectors.memorylake.file_runtime import FileRuntimeLimits, MemoryLakeFileRuntime
from octop.infra.connectors.memorylake.reviews import FileReviewRegistry
from octop.infra.connectors.memorylake.runner import MemoryLakeRunner
from octop.infra.connectors.memorylake.session import _complete
from octop.infra.connectors.memorylake.submissions import SubmissionTransport, WriteBinding

if TYPE_CHECKING:
    from octop.infra.server import OctopServer


class MemoryLakeApplication:
    def __init__(self, server: OctopServer) -> None:
        services = server.services
        if services is None or services.config.memorylake_cli.enabled is not True:
            raise ValueError("memorylake_application_unavailable")
        self._server = server
        self._services = services
        self._config = services.config.memorylake_cli
        self._limits = FileRuntimeLimits()

        async def deny_unscoped(caller: CallerContext) -> None:
            raise ValueError("memorylake_unscoped_source_denied")

        self.runtime = MemoryLakeFileRuntime(
            authorize_factory=lambda admission: deny_unscoped, limits=self._limits
        )
        self.reviews = FileReviewRegistry()
        self._read_admission = ReadAdmission()
        self._closed = False
        self._reason = "checking"
        self._artifact: CliArtifact | None = None
        self._readiness: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._check_lifetime()
        if self._readiness is not None:
            raise ValueError("memorylake_application_already_started")
        self._readiness = asyncio.create_task(self._prepare(), name="memorylake-cli-readiness")

    def _check_lifetime(self) -> None:
        if (
            self._closed
            or self.runtime.closed
            or self._server.services is not self._services
            or self._services.config.memorylake_cli != self._config
            or self._server.memorylake_file_runtime is not self.runtime
            or self._server.memorylake_application is not self
        ):
            raise ValueError("memorylake_application_unavailable")

    def check_current(self) -> None:
        self._check_lifetime()
        if self._artifact is None or self._reason != "ready":
            raise ValueError("memorylake_application_unavailable")

    def status(self) -> dict[str, object]:
        reason = self._reason
        available = False
        try:
            self._check_lifetime()
            available = self._artifact is not None and reason == "ready"
        except ValueError:
            reason = "unavailable"
        return {
            "enabled": True,
            "available": available,
            "reason": reason,
            "max_file_bytes": self._limits.max_file_bytes,
            "upload_ttl_seconds": self._limits.ttl_s,
        }

    async def _prepare(self) -> None:
        directory: TemporaryDirectory[str] | None = None
        try:
            with self.runtime.operation():
                inspection, cancelled = await _complete(
                    self.runtime.database.submit(lambda: inspect_cli_artifact(self._config))
                )
                if cancelled is not None:
                    raise cancelled
                self._check_lifetime()
                artifact = inspection.artifact
                if inspection.status != "content_verified" or artifact is None:
                    self._reason = "artifact_unavailable"
                    return
                try:
                    directory, cancelled = await _complete(
                        self.runtime.database.submit(
                            lambda: TemporaryDirectory(prefix="octop-memorylake-version-")
                        )
                    )
                    if cancelled is not None:
                        raise cancelled
                    self._check_lifetime()
                    assert directory is not None
                    root = Path(directory.name)
                    runner = MemoryLakeRunner(
                        artifact.executable, max_output_bytes=4096, timeout_s=5, concurrency=1
                    )
                    result = await runner.run(
                        ("--version",),
                        env={
                            "HOME": str(root),
                            "MEMORYLAKE_CONFIG_DIR": str(root),
                            "LANG": "C.UTF-8",
                            "LC_ALL": "C.UTF-8",
                        },
                        cwd=root,
                    )
                    self._check_lifetime()
                    if (
                        result.returncode != 0
                        or result.stderr
                        or result.stdout
                        != f"memorylake {artifact.expected_version}\n".encode("ascii")
                    ):
                        self._reason = "version_mismatch"
                        return
                finally:
                    if directory is not None:
                        _, cancelled = await _complete(
                            self.runtime.database.submit(directory.cleanup)
                        )
                        if cancelled is not None:
                            raise cancelled
                self._check_lifetime()
                self._artifact = artifact
                self._reason = "ready"
        except asyncio.CancelledError:
            self._reason = "unavailable"
            raise
        except Exception:
            # Never log or expose filesystem paths, probe output or exception body.
            self._reason = "unavailable"

    def close(self) -> None:
        self.reviews.close()
        self._closed = True

    async def settle_readiness(self) -> bool:
        """Wait for the owned readiness task, deferring caller cancellation.

        Return whether the caller was cancelled so server.stop can first finish
        runtime/actual-worker draining and normal DB teardown, then propagate it.
        This is not a hard filesystem deadline. No hidden shutdown Task is created.
        """
        cancelled = False
        task = self._readiness
        if task is None:
            return False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                cancelled = cancelled or (current is not None and current.cancelling() > 0)
            except Exception:
                break
        if not task.cancelled():
            task.exception()
        return cancelled

    def transport_provider(
        self,
        caller: CallerContext,
        binding_id: str,
        connector_id: str,
        check_invocation: Callable[[], None],
    ) -> TransportFactory:
        self.check_current()
        binding_id, connector_id = _identifier(binding_id), _identifier(connector_id)

        def check() -> None:
            self.check_current()
            if cast("Callable[[], object]", check_invocation)() is not None:
                raise ValueError("memorylake_application_unavailable")

        def factory(
            fixed_connector: str, load_binding: Callable[[], Awaitable[WriteBinding]]
        ) -> SubmissionTransport:
            check()
            if fixed_connector != connector_id:
                raise ValueError("memorylake_application_unavailable")
            credentials = StoredMemoryLakeCredentials(
                caller=caller,
                binding_id=binding_id,
                connector_id=connector_id,
                repo=self._services.memorylake_binding_repo,
                admission=self.runtime.database,
                check_invocation=check,
            )

            async def guarded_binding() -> WriteBinding:
                check()
                binding = await load_binding()
                check()
                return binding

            assert self._artifact is not None
            return MemoryLakeCliTransport(
                connector_id,
                executable=self._artifact.executable,
                load_binding=guarded_binding,
                load_credentials=credentials.load,
            )

        return factory

    async def read_documents(
        self,
        *,
        caller: CallerContext,
        binding_id: str,
        operation: str,
        arguments: Mapping[str, object],
        check_invocation: Callable[[], None],
    ) -> dict[str, object]:
        """Current-binding HTTP reads; never installs an Agent tool."""

        def check() -> None:
            self.check_current()
            if cast("Callable[[], object]", check_invocation)() is not None:
                raise ValueError("memorylake_application_unavailable")

        check()
        assert self._artifact is not None
        return await read_bound_documents(
            caller=caller,
            binding_id=binding_id,
            operation=operation,
            arguments=arguments,
            check_invocation=check,
            runtime=self.runtime,
            repo=self._services.memorylake_binding_repo,
            executable=self._artifact.executable,
            admission=self._read_admission,
        )
