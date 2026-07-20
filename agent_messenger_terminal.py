#!/usr/bin/env python3
"""Full-screen interactive terminal for Agent Messenger."""

import argparse
import curses
import queue
import threading
from datetime import datetime

from agent_messenger import (
    AgentMessenger,
    AgentMessengerError,
    NoPendingRequest,
    format_message_line,
    one_line,
)
from nats_transport import NatsTransport


class AgentTerminal:
    """Live activity pane, permanent prompt, and one-line status."""

    def __init__(self, name: str, server: str, prefix: str):
        self.name = name
        self.server = server
        self.prefix = prefix
        self.events: queue.Queue[dict] = queue.Queue()
        self.lines: list[str] = []
        self.scroll_offset = 0
        self.page_size = 1
        self.input: list[str] = []
        self.cursor = 0
        self.history: list[str] = []
        self.history_pos = 0
        self.status = (
            "READY — PgUp/PgDn scroll | End latest | Up/Down history | "
            "Ctrl-L clear | Ctrl-C quit"
        )
        self.running = True
        self.transport = NatsTransport(server)
        self.agent = AgentMessenger(
            name,
            self.transport,
            prefix=prefix,
            on_event=self.post,
        )
        self.namespace = {
            "send": self.agent.send,
            "request": self.agent.request,
            "respond": self.respond,
            "pending": self.pending,
        }

    def post(self, event: dict) -> None:
        self.events.put(event)

    def close(self) -> None:
        self.agent.close()

    def respond(self, request_reference: str, payload: str) -> None:
        """Respond using a full request ID or a unique displayed prefix."""
        request_id = self._resolve_request_id(request_reference)
        self.agent.respond(request_id, payload)

    def pending(self) -> dict[str, dict]:
        """Return pending requests keyed by their displayed short IDs."""
        return {
            item["request_id"][:8]: {
                "peer": item["peer"],
                "payload": item["payload"],
                "remaining": round(item["remaining"], 3),
            }
            for item in self.agent.pending()
        }

    def _resolve_request_id(self, reference: str) -> str:
        if not isinstance(reference, str) or not reference:
            raise AgentMessengerError("request reference must be a nonempty string")

        request_ids = [item["request_id"] for item in self.agent.pending()]
        if reference in request_ids:
            return reference

        matches = [request_id for request_id in request_ids if request_id.startswith(reference)]
        if not matches:
            raise NoPendingRequest(f"no live request matches {reference!r}")
        if len(matches) > 1:
            raise AgentMessengerError(f"request prefix {reference!r} is ambiguous")
        return matches[0]

    def run(self) -> None:
        curses.wrapper(self._run)

    def _run(self, screen) -> None:
        try:
            curses.curs_set(1)
        except curses.error:
            pass
        screen.keypad(True)
        screen.timeout(50)

        while self.running:
            self._drain_events()
            self._draw(screen)
            try:
                self._handle_key(screen.get_wch())
            except curses.error:
                pass

    def _drain_events(self) -> None:
        while True:
            try:
                event = self.events.get_nowait()
            except queue.Empty:
                return

            event_type = event.get("type")
            if event_type in {"error", "protocol_error"}:
                self.status = "ERROR — " + one_line(
                    event.get("payload", "unknown error")
                )
                continue
            if event_type == "status":
                self.status = one_line(event.get("payload", "READY"))
                continue

            line = self._format_event(event)
            if line:
                if self.scroll_offset:
                    self.scroll_offset += 1
                self.lines.append(line)

    def _format_event(self, event: dict) -> str | None:
        timestamp = event.get("time", datetime.now().strftime("%H:%M:%S"))
        event_type = event.get("type")
        peer = event.get("peer", "")
        payload = one_line(event.get("payload", ""))
        request_id = event.get("request_id")
        if isinstance(request_id, str):
            payload = f"[{request_id[:8]}] {payload}"

        if event_type == "ready":
            return f"{timestamp}  READY     {self.name} @ {self.server}"

        traffic = {
            "out_message": ("MESSAGE", self.name, peer),
            "in_message": ("MESSAGE", peer, self.name),
            "out_request": ("REQUEST", self.name, peer),
            "in_request": ("REQUEST", peer, self.name),
            "out_response": ("RESPONSE", self.name, peer),
            "in_response": ("RESPONSE", peer, self.name),
        }
        if event_type in traffic:
            label, sender, recipient = traffic[event_type]
            return format_message_line(timestamp, label, sender, recipient, payload)
        if event_type == "request_timeout":
            return f"{timestamp}  TIMEOUT   {peer} ({event['timeout']:g}s): {payload}"
        if event_type == "expired_request":
            return f"{timestamp}  EXPIRED   request from {peer}: {payload}"
        if event_type == "late_response":
            return f"{timestamp}  LATE RESP {peer}: {payload}"
        if event_type == "result":
            return f"{timestamp}  RESULT    {payload}"
        return None

    def _draw(self, screen) -> None:
        height, width = screen.getmaxyx()
        screen.erase()

        if height < 6 or width < 30:
            self._add(screen, 0, 0, "terminal too small")
            screen.refresh()
            return

        pending = self.agent.pending()
        pending_count = len(pending)
        header = f" {self.name} | pending: {pending_count or 'none'} "
        self._add(screen, 0, 0, header.ljust(width - 1), curses.A_REVERSE)

        pending_rows = min(pending_count, max(0, height - 6))
        for row, item in enumerate(pending[:pending_rows], 1):
            request_id = item["request_id"][:8]
            line = f"   {request_id}  {item['peer']}: {one_line(item['payload'])}"
            self._add(screen, row, 0, line[: width - 1], curses.A_BOLD)

        activity_start = 1 + pending_rows
        activity_rows = height - activity_start - 3
        self.page_size = max(1, activity_rows)
        max_offset = max(0, len(self.lines) - activity_rows)
        self.scroll_offset = min(self.scroll_offset, max_offset)
        end = len(self.lines) - self.scroll_offset
        start = max(0, end - activity_rows)
        for row, line in enumerate(self.lines[start:end], activity_start):
            self._add(screen, row, 0, line[: width - 1])

        self._add(screen, height - 3, 0, "-" * (width - 1), curses.A_DIM)

        prompt = f"{self.name}> "
        input_room = max(1, width - len(prompt) - 1)
        left = max(0, self.cursor - input_room + 1)
        shown = "".join(self.input[left : left + input_room])
        self._add(screen, height - 2, 0, prompt, curses.A_BOLD)
        self._add(screen, height - 2, len(prompt), shown)

        status_attr = curses.A_BOLD if self.status.startswith("ERROR") else curses.A_DIM
        self._add(
            screen,
            height - 1,
            0,
            self.status[: width - 1].ljust(width - 1),
            status_attr,
        )

        try:
            screen.move(
                height - 2,
                min(len(prompt) + self.cursor - left, width - 2),
            )
        except curses.error:
            pass
        screen.refresh()

    @staticmethod
    def _add(screen, y: int, x: int, text: str, attr: int = 0) -> None:
        try:
            screen.addstr(y, x, text, attr)
        except curses.error:
            pass

    def _handle_key(self, key) -> None:
        if key in ("\n", "\r", curses.KEY_ENTER):
            self._submit()
            return
        if key == "\x03":
            self.running = False
            return
        if key == "\x0c":
            self.lines.clear()
            self.scroll_offset = 0
            self.status = "ACTIVITY CLEARED"
            return
        if key == curses.KEY_LEFT:
            self.cursor = max(0, self.cursor - 1)
            return
        if key == curses.KEY_RIGHT:
            self.cursor = min(len(self.input), self.cursor + 1)
            return
        if key == curses.KEY_PPAGE:
            max_offset = max(0, len(self.lines) - self.page_size)
            self.scroll_offset = min(max_offset, self.scroll_offset + self.page_size)
            return
        if key == curses.KEY_NPAGE:
            self.scroll_offset = max(0, self.scroll_offset - self.page_size)
            return
        if key == curses.KEY_END:
            self.scroll_offset = 0
            return
        if key == curses.KEY_HOME:
            self.cursor = 0
            return
        if key == curses.KEY_DC:
            if self.cursor < len(self.input):
                del self.input[self.cursor]
            return
        if key in (curses.KEY_BACKSPACE, "\b", "\x7f"):
            if self.cursor:
                del self.input[self.cursor - 1]
                self.cursor -= 1
            return
        if key == curses.KEY_UP:
            self._move_history(-1)
            return
        if key == curses.KEY_DOWN:
            self._move_history(1)
            return
        if isinstance(key, str) and key.isprintable():
            self.input.insert(self.cursor, key)
            self.cursor += 1

    def _move_history(self, step: int) -> None:
        if not self.history:
            return
        self.history_pos = max(0, min(len(self.history), self.history_pos + step))
        text = "" if self.history_pos == len(self.history) else self.history[self.history_pos]
        self.input = list(text)
        self.cursor = len(text)

    def _submit(self) -> None:
        command = "".join(self.input).strip()
        self.input.clear()
        self.cursor = 0
        if not command:
            return
        if not self.history or self.history[-1] != command:
            self.history.append(command)
        self.history_pos = len(self.history)
        self.status = "RUNNING — " + command
        threading.Thread(
            target=self._execute,
            args=(command,),
            name=f"command-{self.name}",
            daemon=True,
        ).start()

    def _execute(self, command: str) -> None:
        try:
            result = eval(compile(command, "<agent-terminal>", "eval"), self.namespace)
            if result is not None:
                self.post({"type": "result", "payload": repr(result)})
            self.post({"type": "status", "payload": "READY"})
        except SyntaxError as error:
            self.post({"type": "error", "payload": f"SYNTAX — {one_line(error.msg)}"})
        except NameError as error:
            self.post({"type": "error", "payload": f"UNKNOWN COMMAND — {one_line(error)}"})
        except TypeError as error:
            self.post({"type": "error", "payload": f"BAD CALL — {one_line(error)}"})
        except BaseException as error:
            self.post({"type": "error", "payload": one_line(error)})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("agent_name")
    parser.add_argument("--server", default="nats://127.0.0.1:4222")
    parser.add_argument("--prefix", default="agents")
    args = parser.parse_args()

    terminal = None
    try:
        terminal = AgentTerminal(args.agent_name, args.server, args.prefix)
        terminal.run()
    except KeyboardInterrupt:
        return 0
    except Exception as error:
        print(f"ERROR — {one_line(error)}")
        return 1
    finally:
        if terminal is not None:
            terminal.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
