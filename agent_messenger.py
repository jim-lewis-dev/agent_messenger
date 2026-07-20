#!/usr/bin/env python3
"""Transport-independent messaging for named agents."""

from __future__ import annotations

import json
import queue
import string
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Protocol

VALID_NAME_CHARACTERS = set(string.ascii_letters + string.digits + "_-")
_WAKE = object()


class AgentMessengerError(Exception):
    pass


class RequestTimeout(AgentMessengerError):
    pass


class NoPendingRequest(AgentMessengerError):
    pass


class RequestExpired(AgentMessengerError):
    pass


class Transport(Protocol):
    def subscribe(self, topic: str, callback: Callable[[bytes], None]) -> None: ...
    def publish(self, topic: str, data: bytes) -> None: ...
    def close(self) -> None: ...


@dataclass
class _WaitingRequest:
    recipient: str
    done: threading.Event
    result: list[str]


@dataclass
class _PendingRequest:
    sender: str
    deadline: float
    text: str


def one_line(value: object) -> str:
    return " ".join(str(value).split()) or type(value).__name__


def format_message_line(
    timestamp: str,
    kind: str,
    sender: str,
    recipient: str,
    message: object,
) -> str:
    """Format message, request, and reply traffic uniformly."""
    return (
        f"{timestamp}  {kind.upper():<7}  {sender[:8]:<8}  ->  "
        f"{recipient[:8]:<8}  :  {one_line(message)}"
    )


def _name(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise AgentMessengerError("agent name must be a nonempty string")
    if any(character not in VALID_NAME_CHARACTERS for character in value):
        raise AgentMessengerError("agent names may contain only letters, digits, '_' and '-'")
    return value


def _text(value: str) -> str:
    if not isinstance(value, str):
        raise AgentMessengerError("message must be a string")
    return value


def _timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise AgentMessengerError("timeout must be a positive number")
    return float(value)


class AgentMessenger:
    """Portable agent send/request/reply behavior over any Transport."""

    def __init__(
        self,
        name: str,
        transport: Transport,
        prefix: str = "agents",
        on_event: Callable[[dict], None] | None = None,
    ):
        self.name = _name(name)
        self.transport = transport
        self.prefix = prefix.strip(".")
        if not self.prefix:
            raise AgentMessengerError("prefix must not be empty")

        self._listeners: list[Callable[[dict], None]] = []
        if on_event is not None:
            if not callable(on_event):
                raise AgentMessengerError("listener must be callable")
            self._listeners.append(on_event)

        self._listeners_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._waiting: dict[str, _WaitingRequest] = {}
        self._pending: dict[str, _PendingRequest] = {}
        self._inbox: queue.Queue[bytes | object | None] = queue.Queue()
        self._closed = False

        try:
            self.transport.subscribe(self._topic(self.name), self._inbox.put)
        except BaseException:
            self.transport.close()
            raise

        self._worker = threading.Thread(
            target=self._run,
            name=f"agent-messenger-{self.name}",
            daemon=True,
        )
        self._worker.start()
        self._emit("ready")

    def listen(self, callback: Callable[[dict], None]) -> Callable[[dict], None]:
        if not callable(callback):
            raise AgentMessengerError("listener must be callable")
        with self._listeners_lock:
            self._listeners.append(callback)
        return callback

    def unlisten(self, callback: Callable[[dict], None]) -> None:
        with self._listeners_lock:
            if callback in self._listeners:
                self._listeners.remove(callback)

    def send(self, recipient: str, message: str) -> None:
        recipient, message = _name(recipient), _text(message)
        self._emit("out_message", peer=recipient, text=message)
        self._publish(self._envelope("message", recipient, message))

    def request(self, recipient: str, message: str, timeout: float = 10) -> str:
        recipient, message, timeout = _name(recipient), _text(message), _timeout(timeout)
        deadline = time.monotonic() + timeout
        request_id = uuid.uuid4().hex
        waiting = _WaitingRequest(recipient, threading.Event(), [])

        with self._state_lock:
            self._waiting[request_id] = waiting

        self._emit(
            "out_request",
            peer=recipient,
            text=message,
            timeout=timeout,
            request_id=request_id,
        )
        try:
            self._publish(
                self._envelope(
                    "request",
                    recipient,
                    message,
                    request_id=request_id,
                    timeout=timeout,
                )
            )
        except Exception:
            with self._state_lock:
                self._waiting.pop(request_id, None)
            raise

        remaining = max(0.0, deadline - time.monotonic())
        if not waiting.done.wait(remaining):
            with self._state_lock:
                removed = self._waiting.pop(request_id, None)
            if removed is None and waiting.done.is_set():
                return waiting.result[0]
            self._emit(
                "request_timeout",
                peer=recipient,
                text=message,
                timeout=timeout,
                request_id=request_id,
            )
            raise RequestTimeout(
                f"NO REPLY — {recipient!r} did not reply within {timeout:g}s"
            )

        return waiting.result[0]

    def reply(self, request_id: str, message: str) -> None:
        if not isinstance(request_id, str) or not request_id:
            raise AgentMessengerError("request_id must be a nonempty string")
        message = _text(message)

        with self._state_lock:
            pending = self._pending.pop(request_id, None)

        if pending is None:
            raise NoPendingRequest(
                f"NOTHING TO REPLY TO — no live request {request_id!r}"
            )

        if time.monotonic() >= pending.deadline:
            raise RequestExpired(
                f"TOO LATE — request {request_id[:8]!r} from {pending.sender!r} expired"
            )

        self._emit(
            "out_reply",
            peer=pending.sender,
            text=message,
            request_id=request_id,
        )
        try:
            self._publish(
                self._envelope(
                    "reply",
                    pending.sender,
                    message,
                    request_id=request_id,
                )
            )
        except Exception:
            with self._state_lock:
                if request_id not in self._pending and time.monotonic() < pending.deadline:
                    self._pending[request_id] = pending
                    self._inbox.put(_WAKE)
            raise

    def pending(self) -> tuple[dict, ...]:
        """Return snapshots of all currently live incoming requests."""
        now = time.monotonic()
        with self._state_lock:
            requests = [
                {
                    "request_id": request_id,
                    "peer": pending.sender,
                    "text": pending.text,
                    "remaining": max(0.0, pending.deadline - now),
                }
                for request_id, pending in self._pending.items()
                if pending.deadline > now
            ]
        return tuple(sorted(requests, key=lambda item: (item["peer"], item["request_id"])))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._inbox.put(None)
        self._worker.join(timeout=2)
        self.transport.close()

    stop = close

    def _topic(self, agent: str) -> str:
        return f"{self.prefix}.inbox.{agent}"

    def _envelope(self, kind: str, recipient: str, text: str, **extra: object) -> dict:
        return {
            "kind": kind,
            "sender": self.name,
            "recipient": recipient,
            "text": text,
            **extra,
        }

    def _publish(self, envelope: dict) -> None:
        if self._closed:
            raise AgentMessengerError("agent messenger is closed")
        data = json.dumps(envelope, separators=(",", ":")).encode()
        self.transport.publish(self._topic(envelope["recipient"]), data)

    def _run(self) -> None:
        while True:
            self._expire_pending()
            wait = self._next_expiration_wait()
            try:
                data = self._inbox.get(timeout=wait)
            except queue.Empty:
                continue
            if data is None:
                return
            if data is _WAKE:
                continue
            try:
                self._receive(json.loads(data.decode()))
            except Exception as error:
                self._emit("protocol_error", text=one_line(error))

    def _next_expiration_wait(self) -> float | None:
        now = time.monotonic()
        with self._state_lock:
            if not self._pending:
                return None
            nearest = min(request.deadline for request in self._pending.values())
        return max(0.0, nearest - now)

    def _expire_pending(self) -> None:
        now = time.monotonic()
        expired: list[tuple[str, _PendingRequest]] = []
        with self._state_lock:
            for request_id, pending in tuple(self._pending.items()):
                if now >= pending.deadline:
                    expired.append((request_id, self._pending.pop(request_id)))
        for request_id, pending in expired:
            self._emit(
                "expired_request",
                peer=pending.sender,
                text=pending.text,
                request_id=request_id,
            )

    def _receive(self, envelope: dict) -> None:

        sender = _name(envelope.get("sender"))
        text = _text(envelope.get("text"))
        kind = envelope.get("kind")

        if kind == "message":
            self._emit("in_message", peer=sender, text=text)
            return

        if kind == "request":
            request_id = envelope.get("request_id")
            timeout = envelope.get("timeout")
            if not isinstance(request_id, str) or not request_id:
                raise AgentMessengerError("invalid request_id")
            timeout = _timeout(timeout)
            pending = _PendingRequest(sender, time.monotonic() + timeout, text)
            with self._state_lock:
                if request_id in self._pending:
                    raise AgentMessengerError("duplicate request_id")
                self._pending[request_id] = pending
            self._emit(
                "in_request",
                peer=sender,
                text=text,
                timeout=timeout,
                request_id=request_id,
            )
            return

        if kind == "reply":
            request_id = envelope.get("request_id")
            if not isinstance(request_id, str) or not request_id:
                raise AgentMessengerError("invalid request_id")

            with self._state_lock:
                waiting = self._waiting.get(request_id)
                if waiting is None:
                    late = True
                    wrong_sender = False
                elif sender != waiting.recipient:
                    late = False
                    wrong_sender = True
                else:
                    late = False
                    wrong_sender = False
                    waiting.result.append(text)
                    waiting.done.set()
                    del self._waiting[request_id]

            if late:
                self._emit(
                    "late_reply",
                    peer=sender,
                    text=text,
                    request_id=request_id,
                )
                return
            if wrong_sender:
                raise AgentMessengerError(
                    f"reply for {request_id[:8]!r} came from unexpected agent {sender!r}"
                )

            self._emit(
                "in_reply",
                peer=sender,
                text=text,
                request_id=request_id,
            )
            return

        raise AgentMessengerError(f"unknown envelope kind {kind!r}")

    def _emit(self, event_type: str, **fields: object) -> None:
        event = {
            "type": event_type,
            "time": datetime.now().strftime("%H:%M:%S"),
            "agent": self.name,
            **fields,
        }
        with self._listeners_lock:
            listeners = tuple(self._listeners)
        for listener in listeners:
            try:
                listener(event)
            except Exception:
                pass
