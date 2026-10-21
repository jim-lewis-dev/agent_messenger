#!/usr/bin/env python3
"""Transport-independent messaging for named agents."""

from __future__ import annotations

import json
import math
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


class AgentMessengerClosed(AgentMessengerError):
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
    response: str | None = None
    error: AgentMessengerError | None = None


@dataclass
class _PendingRequest:
    sender: str
    deadline: float
    payload: str


@dataclass(frozen=True)
class _ReceivedMessage:
    data: bytes
    received_at: float


@dataclass
class _WaitingDiscovery:
    done: threading.Event
    names: set[str]
    error: AgentMessengerError | None = None


def one_line(value: object) -> str:
    return " ".join(str(value).split()) or type(value).__name__


def format_message_line(
    timestamp: str,
    message_type: str,
    sender: str,
    recipient: str,
    payload: object,
) -> str:
    """Format message, request, and response traffic uniformly."""
    return (
        f"{timestamp}  {message_type.upper():<8}  {sender[:16]:<16}  ->  "
        f"{recipient[:16]:<16}  :  {one_line(payload)}"
    )


def _name(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise AgentMessengerError("agent name must be a nonempty string")
    if any(character not in VALID_NAME_CHARACTERS for character in value):
        raise AgentMessengerError(
            "agent names may contain only letters, digits, '_' and '-'"
        )
    return value


def _prefix(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise AgentMessengerError("prefix must be a nonempty string")

    parts = value.split(".")
    if any(
        not part
        or any(character not in VALID_NAME_CHARACTERS for character in part)
        for part in parts
    ):
        raise AgentMessengerError(
            "prefix must contain dot-separated names using only "
            "letters, digits, '_' and '-'"
        )

    return value


def _payload(value: str) -> str:
    if not isinstance(value, str):
        raise AgentMessengerError("payload must be a string")
    return value


def _timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AgentMessengerError("timeout must be a positive finite number")

    timeout = float(value)
    if not math.isfinite(timeout) or timeout <= 0:
        raise AgentMessengerError("timeout must be a positive finite number")

    return timeout


def _request_id(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise AgentMessengerError("request_id must be a nonempty string")
    return value


class AgentMessenger:
    """Portable agent messaging and discovery over any Transport."""

    def __init__(
        self,
        name: str,
        transport: Transport,
        prefix: str = "agents",
        on_event: Callable[[dict], None] | None = None,
        discovery_groups: tuple[str, ...] = (),
    ):
        self.name = _name(name)
        self.transport = transport
        self.prefix = _prefix(prefix)
        self.discovery_groups = tuple(
            dict.fromkeys(_name(group) for group in discovery_groups)
        )

        self._listeners: list[Callable[[dict], None]] = []
        if on_event is not None:
            if not callable(on_event):
                raise AgentMessengerError("listener must be callable")
            self._listeners.append(on_event)

        self._listeners_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._waiting: dict[str, _WaitingRequest] = {}
        self._pending: dict[str, _PendingRequest] = {}
        self._discoveries: dict[str, _WaitingDiscovery] = {}
        self._inbox: queue.Queue[_ReceivedMessage | object | None] = queue.Queue()
        self._closed = False

        try:
            self.transport.subscribe(self._topic(self.name), self._enqueue_received)
            self.transport.subscribe(self._discovery_topic(), self._enqueue_received)
            for group in self.discovery_groups:
                self.transport.subscribe(
                    self._discovery_topic(group),
                    self._enqueue_received
                )
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

    def send(self, recipient: str, payload: str) -> None:
        recipient, payload = _name(recipient), _payload(payload)
        self._ensure_open()
        self._emit("out_message", peer=recipient, payload=payload)
        self._publish(self._envelope("message", recipient, payload))

    def request(self, recipient: str, payload: str, timeout: float = 10) -> str:
        recipient = _name(recipient)
        payload = _payload(payload)
        timeout = _timeout(timeout)

        deadline = time.monotonic() + timeout
        request_id = uuid.uuid4().hex
        waiting = _WaitingRequest(recipient, threading.Event())

        with self._state_lock:
            if self._closed:
                raise AgentMessengerClosed("agent messenger is closed")
            self._waiting[request_id] = waiting

        self._emit(
            "out_request",
            peer=recipient,
            payload=payload,
            timeout=timeout,
            request_id=request_id,
        )

        try:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                self._publish(
                    self._envelope(
                        "request",
                        recipient,
                        payload,
                        request_id=request_id,
                        timeout=remaining,
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
                return self._completed_response(waiting)

            self._emit(
                "request_timeout",
                peer=recipient,
                payload=payload,
                timeout=timeout,
                request_id=request_id,
            )
            raise RequestTimeout(
                f"{recipient!r} did not respond within {timeout:g}s"
            )

        return self._completed_response(waiting)

    def discover(
        self,
        timeout: float = 0.2,
        group: str | None = None
    ) -> tuple[str, ...]:
        """Return names that answer the general or selected discovery broadcast."""
        timeout = _timeout(timeout)
        if group is not None:
            group = _name(group)
        deadline = time.monotonic() + timeout
        request_id = uuid.uuid4().hex
        own_name = (
            {self.name}
            if group is None or group in self.discovery_groups
            else set()
        )
        waiting = _WaitingDiscovery(threading.Event(), own_name)

        with self._state_lock:
            if self._closed:
                raise AgentMessengerClosed("agent messenger is closed")
            self._discoveries[request_id] = waiting

        try:
            self._publish_topic(
                self._discovery_topic(group),
                {
                    "type": "discover",
                    "sender": self.name,
                    "request_id": request_id,
                },
            )
        except Exception:
            with self._state_lock:
                self._discoveries.pop(request_id, None)
            raise

        waiting.done.wait(max(0.0, deadline - time.monotonic()))

        with self._state_lock:
            self._discoveries.pop(request_id, None)
            names = tuple(sorted(waiting.names))

        if waiting.error is not None:
            raise waiting.error
        return names

    def respond(self, request_id: str, payload: str) -> None:
        request_id = _request_id(request_id)
        payload = _payload(payload)

        with self._state_lock:
            if self._closed:
                raise AgentMessengerClosed("agent messenger is closed")
            pending = self._pending.pop(request_id, None)

        if pending is None:
            raise NoPendingRequest(
                f"request {request_id!r} is not pending or has expired"
            )

        if time.monotonic() >= pending.deadline:
            raise RequestExpired(
                f"request {request_id[:8]!r} from {pending.sender!r} has expired"
            )

        self._emit(
            "out_response",
            peer=pending.sender,
            payload=payload,
            request_id=request_id,
        )

        try:
            self._publish(
                self._envelope(
                    "response",
                    pending.sender,
                    payload,
                    request_id=request_id,
                )
            )
        except Exception:
            with self._state_lock:
                if (
                    not self._closed
                    and request_id not in self._pending
                    and time.monotonic() < pending.deadline
                ):
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
                    "payload": pending.payload,
                    "remaining": max(0.0, pending.deadline - now),
                }
                for request_id, pending in self._pending.items()
                if pending.deadline > now
            ]

        return tuple(
            sorted(
                requests,
                key=lambda item: (
                    item["remaining"],
                    item["peer"],
                    item["request_id"],
                ),
            )
        )

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return

            self._closed = True

            for waiting in self._waiting.values():
                waiting.error = AgentMessengerClosed("agent messenger is closed")
                waiting.done.set()

            self._waiting.clear()
            self._pending.clear()

            for discovery in self._discoveries.values():
                discovery.error = AgentMessengerClosed("agent messenger is closed")
                discovery.done.set()

            self._discoveries.clear()

        self._inbox.put(None)

        worker_error: AgentMessengerError | None = None
        if threading.current_thread() is not self._worker:
            self._worker.join(timeout=2)
            if self._worker.is_alive():
                worker_error = AgentMessengerError(
                    "agent messenger worker thread did not stop"
                )

        try:
            self.transport.close()
        except Exception:
            if worker_error is not None:
                raise worker_error
            raise

        if worker_error is not None:
            raise worker_error

    stop = close

    def _ensure_open(self) -> None:
        with self._state_lock:
            if self._closed:
                raise AgentMessengerClosed("agent messenger is closed")

    @staticmethod
    def _completed_response(waiting: _WaitingRequest) -> str:
        if waiting.error is not None:
            raise waiting.error
        if waiting.response is None:
            raise AgentMessengerError("request completed without a response")
        return waiting.response

    def _topic(self, agent: str) -> str:
        return f"{self.prefix}.inbox.{agent}"

    def _discovery_topic(self, group: str | None = None) -> str:
        topic = f"{self.prefix}.discovery"
        if group is not None:
            topic += f".{group}"
        return topic

    def _envelope(
        self,
        message_type: str,
        recipient: str,
        payload: str,
        **extra: object,
    ) -> dict:
        return {
            "type": message_type,
            "sender": self.name,
            "recipient": recipient,
            "payload": payload,
            **extra,
        }

    def _publish(self, envelope: dict) -> None:
        self._publish_topic(self._topic(envelope["recipient"]), envelope)

    def _publish_topic(self, topic: str, envelope: dict) -> None:
        self._ensure_open()
        data = json.dumps(envelope, separators=(",", ":")).encode()
        self.transport.publish(topic, data)

    def _enqueue_received(self, data: bytes) -> None:
        # Timestamp before the messenger's own worker queue can delay delivery.
        self._inbox.put(_ReceivedMessage(data, time.monotonic()))

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

            with self._state_lock:
                if self._closed:
                    continue

            try:
                envelope = json.loads(data.data.decode())
                self._receive(envelope, data.received_at)
            except Exception as error:
                self._emit("protocol_error", payload=one_line(error))

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
                payload=pending.payload,
                request_id=request_id,
            )

    def _receive(self, envelope: object, received_at: float) -> None:
        if not isinstance(envelope, dict):
            raise AgentMessengerError("envelope must be a JSON object")

        message_type = envelope.get("type")

        if message_type == "discover":
            sender = _name(envelope.get("sender"))
            request_id = _request_id(envelope.get("request_id"))
            self._publish_topic(
                self._topic(sender),
                {
                    "type": "discover_response",
                    "sender": self.name,
                    "recipient": sender,
                    "request_id": request_id,
                },
            )
            return

        if message_type == "discover_response":
            recipient = _name(envelope.get("recipient"))
            if recipient != self.name:
                raise AgentMessengerError(
                    f"envelope recipient {recipient!r} does not match {self.name!r}"
                )
            sender = _name(envelope.get("sender"))
            request_id = _request_id(envelope.get("request_id"))
            with self._state_lock:
                waiting = self._discoveries.get(request_id)
                if waiting is not None:
                    waiting.names.add(sender)
            return

        recipient = _name(envelope.get("recipient"))
        if recipient != self.name:
            raise AgentMessengerError(
                f"envelope recipient {recipient!r} does not match {self.name!r}"
            )

        sender = _name(envelope.get("sender"))
        payload = _payload(envelope.get("payload"))
        if message_type == "message":
            self._emit("in_message", peer=sender, payload=payload)
            return

        if message_type == "request":
            request_id = _request_id(envelope.get("request_id"))
            timeout = _timeout(envelope.get("timeout"))
            pending = _PendingRequest(
                sender=sender,
                deadline=received_at + timeout,
                payload=payload,
            )
            if time.monotonic() >= pending.deadline:
                self._emit("expired_request", peer=sender, payload=payload, request_id=request_id)
                return

            with self._state_lock:
                if self._closed:
                    return
                if request_id in self._pending:
                    raise AgentMessengerError("duplicate request_id")
                self._pending[request_id] = pending

            self._emit(
                "in_request",
                peer=sender,
                payload=payload,
                timeout=timeout,
                deadline=pending.deadline,
                request_id=request_id,
            )
            return

        if message_type == "response":
            request_id = _request_id(envelope.get("request_id"))

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
                    waiting.response = payload
                    waiting.done.set()
                    del self._waiting[request_id]

            if late:
                self._emit(
                    "late_response",
                    peer=sender,
                    payload=payload,
                    request_id=request_id,
                )
                return

            if wrong_sender:
                raise AgentMessengerError(
                    f"response for {request_id[:8]!r} came from "
                    f"unexpected agent {sender!r}"
                )

            self._emit(
                "in_response",
                peer=sender,
                payload=payload,
                request_id=request_id,
            )
            return

        raise AgentMessengerError(f"unknown envelope type {message_type!r}")

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
