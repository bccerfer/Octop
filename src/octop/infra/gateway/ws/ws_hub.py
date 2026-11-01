"""In-process registry of Dashboard WebSocket connections."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, NoReturn, SupportsIndex

logger = logging.getLogger(__name__)

SendFn = Callable[[dict[str, Any]], Awaitable[None]]


def stamp_thread_id(frame: dict[str, Any], thread_id: str) -> dict[str, Any]:
    """Return *frame* with ``thread_id`` set so clients can drop cross-talk."""
    tid = thread_id.strip()
    if not tid or frame.get("thread_id") == tid:
        return frame
    stamped = dict(frame)
    stamped["thread_id"] = tid
    return stamped


_CHAT_MINT = object()


class ChatConnectionRef:
    """Internal same-process chat source, not a serializable bearer credential.

    Only authenticated server ingress may register it. This proves current
    socket registration, NOT thread ownership, JWT validity or delivery success.
    Python-private minting is an accidental-misuse guard, not a hostile-code sandbox.
    """

    __slots__ = ("_agent_id", "_connection_id", "_hub", "_mint", "_user_id")

    def __init__(
        self,
        hub: WebSocketHub,
        connection_id: str,
        user_id: int,
        agent_id: str,
        *,
        _mint: object | None = None,
    ) -> None:
        self._hub, self._connection_id = hub, connection_id
        self._user_id, self._agent_id, self._mint = user_id, agent_id, _mint

    @property
    def connection_id(self) -> str:
        return self._connection_id

    def check_current(self, *, caller_user_id: int, agent_id: str) -> None:
        if (
            type(self) is not ChatConnectionRef
            or getattr(self, "_mint", None) is not _CHAT_MINT
            or type(caller_user_id) is not int
            or caller_user_id <= 0
            or type(agent_id) is not str
            or caller_user_id != self._user_id
            or agent_id != self._agent_id
        ):
            raise ValueError("chat_connection_inactive")
        hub = self._hub
        hub._check_chat_loop()
        if (
            hub._closed
            or hub._chat_refs.get(self._connection_id) is not self
            or self._connection_id not in hub._connections
        ):
            raise ValueError("chat_connection_inactive")

    def __copy__(self) -> NoReturn:
        raise TypeError("chat_connection_not_serializable")

    def __deepcopy__(self, memo: dict[int, object]) -> NoReturn:
        raise TypeError("chat_connection_not_serializable")

    def __reduce_ex__(self, protocol: SupportsIndex) -> NoReturn:
        raise TypeError("chat_connection_not_serializable")


class WebSocketHub:
    """Maps connection ids to async send callbacks for Dashboard sockets.

    Tracks per-thread subscriptions (chat) and optional per-user bindings
    (dashboard-wide toasts). Reconnecting clients resume live chunks via
    in-memory "turn active" flags. Each connection is bound to at most one
    thread at a time.
    """

    def __init__(self) -> None:
        self._connections: dict[str, SendFn] = {}
        self._registrations: dict[str, object] = {}
        self._thread_subscribers: dict[str, set[str]] = {}
        self._conn_thread: dict[str, str] = {}
        self._user_conns: dict[int, set[str]] = {}
        self._conn_user: dict[str, int] = {}
        self._active_turns: set[str] = set()
        self._chat_refs: dict[str, ChatConnectionRef] = {}
        self._chat_loop: asyncio.AbstractEventLoop | None = None
        self._closed = False

    def _check_chat_loop(self) -> asyncio.AbstractEventLoop:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise ValueError("chat_connection_loop_required") from None
        if self._chat_loop is not None and self._chat_loop is not loop:
            raise ValueError("chat_connection_loop_mismatch")
        return loop

    def register_authenticated_chat(
        self,
        connection_id: str,
        send_fn: SendFn,
        *,
        user_id: int,
        agent_id: str,
    ) -> ChatConnectionRef:
        """Register trusted JWT ingress; never call with model-supplied identity.

        Every registration has a fresh object identity. Only active refs are
        retained, so reconnects cannot revive old grants or accumulate tombstones.
        """
        loop = self._check_chat_loop()
        if (
            self._closed
            or type(user_id) is not int
            or user_id <= 0
            or type(agent_id) is not str
            or not 1 <= len(agent_id) <= 128
            or "\0" in agent_id
            or type(connection_id) is not str
            or not 1 <= len(connection_id) <= 128
            or "\0" in connection_id
            or not callable(send_fn)
        ):
            raise ValueError("chat_connection_registration_denied")
        self._chat_loop = loop
        # Chat ownership is authorization evidence, not a notification opt-in.
        # Only the separate notify socket uses generic register(user_id=...).
        self.register(connection_id, send_fn)
        ref = ChatConnectionRef(self, connection_id, user_id, agent_id, _mint=_CHAT_MINT)
        self._chat_refs[connection_id] = ref
        return ref

    def close(self) -> None:
        """Invalidate all sources and clear connections, without recalling sends.

        Does not cancel a callback which was already awaited by a push operation.
        The owner must separately stop/drain consumers before closing resources.
        """
        if self._chat_loop is not None:
            self._check_chat_loop()
        self._closed = True
        self._chat_refs.clear()
        self._connections.clear()
        self._registrations.clear()
        self._thread_subscribers.clear()
        self._conn_thread.clear()
        self._user_conns.clear()
        self._conn_user.clear()
        self._active_turns.clear()

    def register(
        self,
        connection_id: str,
        send_fn: SendFn,
        *,
        user_id: int | None = None,
    ) -> None:
        if self._closed:
            raise ValueError("websocket_hub_closed")
        # A replacement callback must never inherit the previous socket scope.
        self.unregister(connection_id)
        self._connections[connection_id] = send_fn
        self._registrations[connection_id] = object()
        if user_id is None:
            return
        self._conn_user[connection_id] = user_id
        self._user_conns.setdefault(user_id, set()).add(connection_id)

    def unregister(self, connection_id: str) -> None:
        self._chat_refs.pop(connection_id, None)
        self._registrations.pop(connection_id, None)
        self.unsubscribe_connection(connection_id)
        self._unbind_user(connection_id)
        self._connections.pop(connection_id, None)

    def _unbind_user(self, connection_id: str) -> None:
        user_id = self._conn_user.pop(connection_id, None)
        if user_id is None:
            return
        conns = self._user_conns.get(user_id)
        if conns is None:
            return
        conns.discard(connection_id)
        if not conns:
            self._user_conns.pop(user_id, None)

    def subscribe(self, thread_id: str, connection_id: str) -> None:
        """Add *connection_id* as a subscriber for *thread_id*.

        Switching threads unsubscribes this connection from the previous
        thread. Other connections on the same thread keep receiving.
        """
        tid = thread_id.strip()
        if not tid or connection_id not in self._connections:
            return
        prev = self._conn_thread.get(connection_id)
        if prev == tid:
            return
        if prev is not None:
            self._drop_subscriber(prev, connection_id)
        self._thread_subscribers.setdefault(tid, set()).add(connection_id)
        self._conn_thread[connection_id] = tid

    def unsubscribe_connection(self, connection_id: str) -> None:
        tid = self._conn_thread.pop(connection_id, None)
        if tid is not None:
            self._drop_subscriber(tid, connection_id)

    def _drop_subscriber(self, thread_id: str, connection_id: str) -> None:
        conns = self._thread_subscribers.get(thread_id)
        if conns is None:
            return
        conns.discard(connection_id)
        if not conns:
            self._thread_subscribers.pop(thread_id, None)

    def mark_turn_active(self, thread_id: str) -> None:
        tid = thread_id.strip()
        if tid:
            self._active_turns.add(tid)

    def mark_turn_idle(self, thread_id: str) -> None:
        self._active_turns.discard(thread_id.strip())

    def is_turn_active(self, thread_id: str) -> bool:
        return thread_id.strip() in self._active_turns

    def has_subscribers(self, thread_id: str) -> bool:
        """True when at least one Dashboard socket is bound to *thread_id*."""
        tid = thread_id.strip()
        return bool(tid and self._thread_subscribers.get(tid))

    def _send_snapshot(self, connection_id: str) -> tuple[str, object, SendFn] | None:
        send_fn = self._connections.get(connection_id)
        registration = self._registrations.get(connection_id)
        if send_fn is None or registration is None:
            return None
        return connection_id, registration, send_fn

    async def _push_snapshot(
        self, snapshot: tuple[str, object, SendFn], frame: dict[str, Any]
    ) -> None:
        connection_id, registration, send_fn = snapshot
        # Fanout tasks may start after a reused ID was registered to a different
        # socket/user. Never resolve the new callback using only that old ID.
        # A fresh nonce also invalidates re-registration with the SAME callback.
        if (
            self._registrations.get(connection_id) is not registration
            or self._connections.get(connection_id) is not send_fn
        ):
            return
        try:
            await send_fn(frame)
        except Exception:
            logger.exception("ws hub: push failed for %s", connection_id)

    async def push(self, connection_id: str, frame: dict[str, Any]) -> None:
        snapshot = self._send_snapshot(connection_id)
        if snapshot is None:
            logger.debug("ws hub: connection %s not found", connection_id)
            return
        await self._push_snapshot(snapshot, frame)

    async def push_to_thread(self, thread_id: str, frame: dict[str, Any]) -> None:
        tid = thread_id.strip()
        conns = self._thread_subscribers.get(tid)
        if not conns:
            logger.debug("ws hub: no subscriber for thread %s", thread_id)
            return
        outbound = stamp_thread_id(frame, tid)
        targets = [snapshot for cid in conns if (snapshot := self._send_snapshot(cid)) is not None]
        if len(targets) == 1:
            await self._push_snapshot(targets[0], outbound)
            return
        await asyncio.gather(*(self._push_snapshot(target, outbound) for target in targets))

    async def push_to_user(self, user_id: int, frame: dict[str, Any]) -> None:
        conns = self._user_conns.get(user_id)
        if not conns:
            logger.debug("ws hub: no subscriber for user %s", user_id)
            return
        targets = [snapshot for cid in conns if (snapshot := self._send_snapshot(cid)) is not None]
        if len(targets) == 1:
            await self._push_snapshot(targets[0], frame)
            return
        await asyncio.gather(*(self._push_snapshot(target, frame) for target in targets))

    async def push_json(self, connection_id: str, payload: str) -> None:
        try:
            frame = json.loads(payload)
        except (TypeError, ValueError):
            logger.warning("ws hub: invalid json for %s", connection_id)
            return
        if isinstance(frame, dict):
            await self.push(connection_id, frame)

    @property
    def connection_count(self) -> int:
        return len(self._connections)


__all__ = ["ChatConnectionRef", "SendFn", "WebSocketHub", "stamp_thread_id"]
