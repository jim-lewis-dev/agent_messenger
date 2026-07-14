#!/usr/bin/env python3
"""Transport-independent messenger for named agents."""

from __future__ import annotations

import json
import queue
import string
import threading
import time
import uuid
from datetime import datetime
from typing import Callable, Protocol

VALID_NAME_CHARACTERS = set(string.ascii_letters + string.digits + "_-")


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
    """Portable agent send/request/reply logic over any Transport implementation."""

    def __init__(
        self,
        name: str,
        transport: Transport,
        prefix: str = "agents",
        publish_flow: bool = True,
        on_event: Callable[[dict], None] | None = None,
    ):
        self.name = _name(name)
        self.transport = transport
        self.prefix = prefix.strip(".")
        if not self.prefix:
            raise AgentMessengerError("prefix must not be empty")
        self.publish_flow = publish_flow
        self._listeners: list[Callable[[dict], None]] = []
        if on_event is not None:
            if not callable(on_event):
                raise AgentMessengerError("listener must be callable")
            self._listeners.append(on_event)
        self._listeners_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._waiting: dict[str, tuple[threading.Event, list[str]]] = {}
        self._pending: dict[str, tuple[str, float, str]] = {}
        self._inbox: queue.Queue[bytes | None] = queue.Queue()
        self._closed = False

        self.transport.subscribe(self._topic(self.name), self._inbox.put)
        self._worker = threading.Thread(
            target=self._run,
            name=f"agent_messenger-{self.name}",
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
        request_id = uuid.uuid4().hex
        done = threading.Event()
        result: list[str] = []
        with self._state_lock:
            self._waiting[request_id] = (done, result)

        envelope = self._envelope(
            "request",
            recipient,
            message,
            request_id=request_id,
            expires_at=time.time() + timeout,
        )
        self._emit("out_request", peer=recipient, text=message, timeout=timeout)
        try:
            self._publish(envelope)
        except Exception:
            with self._state_lock:
                self._waiting.pop(request_id, None)
            raise

        if not done.wait(timeout):
            with self._state_lock:
                self._waiting.pop(request_id, None)
            self._emit("request_timeout", peer=recipient, text=message, timeout=timeout)
            raise RequestTimeout(
                f"NO REPLY — {recipient!r} did not reply within {timeout:g}s"
            )
        return result[0]

    def reply(self, recipient: str, message: str) -> None:
        recipient, message = _name(recipient), _text(message)
        with self._state_lock:
            pending = self._pending.pop(recipient, None)
        if pending is None:
            raise NoPendingRequest(
                f"NOTHING TO REPLY TO — no live request from {recipient!r}"
            )

        request_id, expires_at, request_text = pending
        if time.time() >= expires_at:
            raise RequestExpired(f"TOO LATE — {recipient!r}'s request expired")

        self._emit("out_reply", peer=recipient, text=message)
        try:
            self._publish(
                self._envelope("reply", recipient, message, request_id=request_id)
            )
        except Exception:
            with self._state_lock:
                if recipient not in self._pending and time.time() < expires_at:
                    self._pending[recipient] = pending
            raise

    def pending_senders(self) -> tuple[str, ...]:
        now = time.time()
        with self._state_lock:
            expired = [peer for peer, (_, end, _) in self._pending.items() if now >= end]
            for peer in expired:
                del self._pending[peer]
            return tuple(sorted(self._pending))

    def pending(self) -> dict[str, str]:
        """Return newest live request text keyed by sender name."""
        now = time.time()
        with self._state_lock:
            expired = [peer for peer, (_, end, _) in self._pending.items() if now >= end]
            for peer in expired:
                del self._pending[peer]
            return {peer: self._pending[peer][2] for peer in sorted(self._pending)}

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

    @property
    def flow_topic(self) -> str:
        return f"{self.prefix}.flow"

    def _envelope(self, kind: str, recipient: str, text: str, **extra: object) -> dict:
        return {
            "version": 1,
            "id": uuid.uuid4().hex,
            "kind": kind,
            "sender": self.name,
            "recipient": recipient,
            "text": text,
            "sent_at": time.time(),
            **extra,
        }

    def _publish(self, envelope: dict) -> None:
        if self._closed:
            raise AgentMessengerError("agent messenger is closed")
        data = json.dumps(envelope, separators=(",", ":")).encode()
        self.transport.publish(self._topic(envelope["recipient"]), data)
        if self.publish_flow:
            try:
                self.transport.publish(self.flow_topic, data)
            except Exception as error:
                self._emit("flow_error", text=one_line(error))

    def _run(self) -> None:
        while True:
            data = self._inbox.get()
            if data is None:
                return
            try:
                self._receive(json.loads(data.decode()))
            except Exception as error:
                self._emit("protocol_error", text=one_line(error))

    def _receive(self, envelope: dict) -> None:
        if envelope.get("version") != 1 or envelope.get("recipient") != self.name:
            raise AgentMessengerError("invalid envelope")
        sender = _name(envelope.get("sender"))
        text = _text(envelope.get("text"))
        kind = envelope.get("kind")

        if kind == "message":
            self._emit("in_message", peer=sender, text=text)
            return

        if kind == "request":
            request_id = envelope.get("request_id")
            expires_at = envelope.get("expires_at")
            if not isinstance(request_id, str) or not isinstance(expires_at, (int, float)):
                raise AgentMessengerError("invalid request envelope")
            if time.time() >= expires_at:
                self._emit("expired_request", peer=sender, text=text)
                return
            with self._state_lock:
                replaced = sender in self._pending
                self._pending[sender] = (request_id, float(expires_at), text)
            self._emit("in_request", peer=sender, text=text, replaced=replaced)
            return

        if kind == "reply":
            request_id = envelope.get("request_id")
            with self._state_lock:
                waiting = self._waiting.pop(request_id, None)
            if waiting is None:
                self._emit("late_reply", peer=sender, text=text)
                return
            done, result = waiting
            result.append(text)
            self._emit("in_reply", peer=sender, text=text)
            done.set()
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
