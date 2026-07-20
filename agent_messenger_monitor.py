#!/usr/bin/env python3
"""Full-screen system-wide Agent Messenger traffic monitor."""

import argparse
import curses
import json
import queue
import time
from datetime import datetime

from agent_messenger import format_message_line, one_line
from nats_transport import NatsTransport


class FlowTerminal:
    def __init__(self, server: str, prefix: str):
        self.server = server
        self.prefix = prefix
        self.events: queue.Queue[bytes] = queue.Queue()
        self.lines: list[str] = []
        self.scroll_offset = 0
        self.page_size = 1
        self.pending: dict[str, tuple[str, str, float, str]] = {}
        self.status = (
            "LIVE — PgUp/PgDn scroll | End latest | Ctrl-L clear | "
            "Ctrl-D or Q quit"
        )
        self.running = True
        self.transport = NatsTransport(server)
        self.transport.subscribe(f"{prefix}.inbox.*", self.events.put)

    def close(self) -> None:
        self.transport.close()

    def run(self) -> None:
        curses.wrapper(self._run)

    def _run(self, screen) -> None:
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        screen.keypad(True)
        screen.timeout(50)

        while self.running:
            self._drain()
            self._expire_pending()
            self._draw(screen)
            try:
                key = screen.get_wch()
            except curses.error:
                continue
            if key in ("q", "Q", "\x04", "\x11"):
                self.running = False
            elif key == "\x0c":
                self.lines.clear()
                self.scroll_offset = 0
                self.status = "FLOW CLEARED"
            elif key == curses.KEY_PPAGE:
                max_offset = max(0, len(self.lines) - self.page_size)
                self.scroll_offset = min(max_offset, self.scroll_offset + self.page_size)
            elif key == curses.KEY_NPAGE:
                self.scroll_offset = max(0, self.scroll_offset - self.page_size)
            elif key == curses.KEY_END:
                self.scroll_offset = 0

    def _drain(self) -> None:
        while True:
            try:
                data = self.events.get_nowait()
            except queue.Empty:
                return
            try:
                envelope = json.loads(data.decode())
                self._update_pending(envelope)
                timestamp = datetime.now().strftime("%H:%M:%S")
                text = one_line(envelope.get("text", ""))
                request_id = envelope.get("request_id")
                if isinstance(request_id, str):
                    text = f"[{request_id[:8]}] {text}"
                if self.scroll_offset:
                    self.scroll_offset += 1
                self.lines.append(
                    format_message_line(
                        timestamp,
                        envelope["kind"],
                        envelope["sender"],
                        envelope["recipient"],
                        text,
                    )
                )
                self.status = (
                    f"LIVE — {len(self.lines)} events | PgUp/PgDn scroll | "
                    "End latest | Ctrl-L clear | Ctrl-D or Q quit"
                )
            except Exception as error:
                self.status = "ERROR — " + one_line(error)

    def _update_pending(self, envelope: dict) -> None:
        sender = envelope.get("sender")
        recipient = envelope.get("recipient")
        kind = envelope.get("kind")
        request_id = envelope.get("request_id")
        if not all(isinstance(value, str) for value in (sender, recipient, request_id)):
            return

        if kind == "request":
            timeout = envelope.get("timeout")
            if isinstance(timeout, (int, float)) and not isinstance(timeout, bool) and timeout > 0:
                self.pending[request_id] = (
                    sender,
                    recipient,
                    time.monotonic() + float(timeout),
                    one_line(envelope.get("text", "")),
                )
        elif kind == "reply":
            current = self.pending.get(request_id)
            if current and current[0] == recipient and current[1] == sender:
                del self.pending[request_id]

    def _expire_pending(self) -> None:
        now = time.monotonic()
        for request_id, (_, _, deadline, _) in tuple(self.pending.items()):
            if now >= deadline:
                del self.pending[request_id]

    def _draw(self, screen) -> None:
        height, width = screen.getmaxyx()
        screen.erase()
        if height < 4 or width < 40:
            self._add(screen, 0, 0, "terminal too small")
            screen.refresh()
            return

        pending_items = sorted(self.pending.items(), key=lambda item: item[1][:2])
        header = (
            f" AGENT MESSENGER MONITOR | {self.prefix} | pending: "
            f"{len(pending_items) or 'none'} | {self.server} "
        )
        self._add(screen, 0, 0, header.ljust(width - 1), curses.A_REVERSE)

        pending_rows = min(len(pending_items), max(0, height - 4))
        for row, (request_id, (sender, recipient, _, question)) in enumerate(
            pending_items[:pending_rows], 1
        ):
            line = f"   {request_id[:8]}  {recipient} <- {sender}: {question}"
            self._add(screen, row, 0, line[: width - 1], curses.A_BOLD)

        activity_start = 1 + pending_rows
        activity_rows = height - activity_start - 1
        self.page_size = max(1, activity_rows)
        max_offset = max(0, len(self.lines) - activity_rows)
        self.scroll_offset = min(self.scroll_offset, max_offset)
        end = len(self.lines) - self.scroll_offset
        start = max(0, end - activity_rows)
        for row, line in enumerate(self.lines[start:end], activity_start):
            self._add(screen, row, 0, line[: width - 1])

        attr = curses.A_BOLD if self.status.startswith("ERROR") else curses.A_DIM
        self._add(screen, height - 1, 0, self.status[: width - 1].ljust(width - 1), attr)
        screen.refresh()

    @staticmethod
    def _add(screen, y: int, x: int, text: str, attr: int = 0) -> None:
        try:
            screen.addstr(y, x, text, attr)
        except curses.error:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="nats://127.0.0.1:4222")
    parser.add_argument("--prefix", default="agents")
    args = parser.parse_args()

    terminal = None
    try:
        terminal = FlowTerminal(args.server, args.prefix)
        terminal.run()
    except Exception as error:
        print(f"ERROR — {one_line(error)}")
        return 1
    finally:
        if terminal is not None:
            terminal.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
