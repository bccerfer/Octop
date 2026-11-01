"""Dormant, real agent-bound middleware; no production registration or ingress.

The scope must enclose complete stream consumption, not merely iterator creation.
Only trusted server code may select binding/connector IDs for a redeemed origin.
No caller, tools or authority are recovered from RunnableConfig/checkpoint state.
Tool/model return checks cannot undo downstream graph observers or prior writes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, StructuredTool
from langgraph.config import get_config
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from octop.infra.connectors.memorylake.admission import ReadAdmission
from octop.infra.connectors.memorylake.contract import _identifier
from octop.infra.connectors.memorylake.langchain import (
    _check_dashboard_read_context,
    build_dashboard_read_tools,
)
from octop.infra.connectors.memorylake.origin import DashboardTurnIdentity
from octop.infra.connectors.memorylake.submission_storage import DatabaseJobAdmission

if TYPE_CHECKING:
    from octop.infra.db.repos.memorylake import MemoryLakeBindingRepo
    from octop.infra.db.repos.secrets import SecretRepo


@dataclass(frozen=True, repr=False)
class _Turn:
    identity: DashboardTurnIdentity
    tools: tuple[StructuredTool, ...]


_TURN: ContextVar[_Turn | None] = ContextVar("octop_memorylake_read_turn", default=None)


@contextmanager
def dashboard_read_turn(
    *,
    identity: DashboardTurnIdentity,
    binding_id: str,
    connector_id: str,
    bindings: MemoryLakeBindingRepo,
    secrets: SecretRepo,
    executable: Path,
    admission: ReadAdmission,
    database: DatabaseJobAdmission,
) -> Iterator[None]:
    """Internal invocation lifetime; close shared identity BEFORE resetting scope.

    Shared runtime.database is mandatory: missing/None admission fails closed
    before constructing readers or invocation tools.

    Use inside DashboardDelivery.receive, never around only stream construction.
    Nesting/peer/resume reconstruction cannot replace an existing turn. Future
    authenticated consumers still own cleanup if entering this context fails.
    """
    if not isinstance(database, DatabaseJobAdmission):
        raise ValueError("shared_database_admission_required")
    if _TURN.get() is not None:
        raise ValueError("memorylake_turn_nested")
    if type(identity) is not DashboardTurnIdentity:
        raise ValueError("dashboard_identity_required")
    tools = build_dashboard_read_tools(
        identity=identity,
        caller_user_id=identity.user_id,
        agent_id=identity.agent_id,
        thread_id=identity.thread_id,
        binding_id=binding_id,
        connector_id=connector_id,
        bindings=bindings,
        secrets=secrets,
        executable=executable,
        admission=admission,
        database=database,
    )
    token = _TURN.set(_Turn(identity, tuple(tools)))
    try:
        yield
    finally:
        identity.close()
        _TURN.reset(token)


def _reserved(name: object) -> bool:
    return isinstance(name, str) and name.startswith("memorylake_")


def _reserved_schema(tool: BaseTool | dict[str, Any]) -> bool:
    if isinstance(tool, BaseTool):
        return _reserved(tool.name)
    nested = tool.get("function")
    return _reserved(tool.get("name")) or (
        isinstance(nested, dict) and _reserved(nested.get("name"))
    )


def _native_request(request: ModelRequest[Any]) -> ModelRequest[Any]:
    # Even an accidental old/global closure may not bypass per-turn selection.
    return request.override(tools=[tool for tool in request.tools if not _reserved_schema(tool)])


def _denied(request: ToolCallRequest) -> ToolMessage:
    return ToolMessage(
        content="memorylake_read_unavailable",
        tool_call_id=request.tool_call["id"],
        status="error",
    )


class MemoryLakeReadMiddleware(AgentMiddleware[Any, Any]):
    """One stateless instance per real agent, not a caller-bearing tool cache.

    `agent_id` comes from the server's actual compiled agent, never config/model
    input. Peer graphs inherit context but cannot inherit another agent's grant.
    This middleware is NOT added to AgentManager or any default tool inventory.
    """

    def __init__(self, *, agent_id: str) -> None:
        super().__init__()
        self._agent_id = _identifier(agent_id)

    def _check(self, turn: _Turn) -> None:
        if _TURN.get() is not turn:
            raise ValueError("memorylake_turn_changed")
        turn.identity.check_current(agent_id=self._agent_id, thread_id=turn.identity.thread_id)
        _check_dashboard_read_context(turn.identity, get_config())

    def _current(self) -> _Turn | None:
        turn = _TURN.get()
        if turn is not None:
            try:
                self._check(turn)
            except (ValueError, RuntimeError):
                return None
        return turn

    def wrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], ModelResponse[Any]],
    ) -> ModelResponse[Any]:
        # The CLI boundary is async-only. Keep native sync calls, never ML tools.
        turn = _TURN.get()
        if turn is not None:
            self._check(turn)
        result = handler(_native_request(request))
        if turn is not None:
            self._check(turn)
        return result

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[ModelResponse[Any]]],
    ) -> ModelResponse[Any]:
        request = _native_request(request)
        turn = _TURN.get()
        if turn is None:
            return await handler(request)
        # An existing invalid scope is NOT an ordinary native turn: previous
        # MemoryLake ToolMessages may already be present in its messages. Stop
        # before another model sees them, rather than silently dropping tools.
        self._check(turn)
        result = await handler(request.override(tools=[*request.tools, *turn.tools]))
        # No await after the checkpoint and before returning this result.
        # Earlier model callbacks / later graph checkpoints remain separate gates.
        self._check(turn)
        return result

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        if _reserved(request.tool_call.get("name")):
            return _denied(request)
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        name = request.tool_call.get("name")
        if not _reserved(name):
            return await handler(request)
        turn = self._current()
        if turn is None:
            return _denied(request)
        tool = next((tool for tool in turn.tools if tool.name == name), None)
        if tool is None:
            return _denied(request)
        # Do not catch cancellation or GraphBubbleUp control flow from handlers.
        result = await handler(request.override(tool=tool))
        try:
            self._check(turn)
        except (ValueError, RuntimeError):
            return _denied(request)
        if not isinstance(result, ToolMessage) or result.tool_call_id != request.tool_call["id"]:
            return _denied(request)
        return result
