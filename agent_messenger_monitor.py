#!/usr/bin/env python3
"""Full-screen system-wide agent messenger flow watcher."""

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
        self.pending: dict[tuple[str, str], tuple[str, float, str]] = {}
        self.status = (
            "LIVE — PgUp/PgDn scroll | End latest | Ctrl-L clear | "
            "Ctrl-D or Q quit"
        )
        self.running = True
        self.transport = NatsTransport(server)
        self.transport.subscribe(f"{prefix}.flow", self.events.put)

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
                self.scroll_offset = min(
                    max_offset,
                    self.scroll_offset + self.page_size,
                )
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
                timestamp = datetime.fromtimestamp(envelope["sent_at"]).strftime("%H:%M:%S")
                text = one_line(envelope.get("text", ""))
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
        if not isinstance(sender, str) or not isinstance(recipient, str):
            return

        key = (recipient, sender)
        if kind == "request":
            request_id = envelope.get("request_id")
            expires_at = envelope.get("expires_at")
            if isinstance(request_id, str) and isinstance(expires_at, (int, float)):
                self.pending[key] = (
                    request_id,
                    float(expires_at),
                    one_line(envelope.get("text", "")),
                )
        elif kind == "reply":
            key = (sender, recipient)
            current = self.pending.get(key)
            if current and current[0] == envelope.get("request_id"):
                del self.pending[key]

    def _expire_pending(self) -> None:
        now = time.time()
        for key, (_, expires_at, _) in tuple(self.pending.items()):
            if now >= expires_at:
                del self.pending[key]

    def _draw(self, screen) -> None:
        height, width = screen.getmaxyx()
        screen.erase()
        if height < 4 or width < 40:
            self._add(screen, 0, 0, "terminal too small")
            screen.refresh()
            return

        pending_items = sorted(self.pending.items())
        header = (
            f" AGENT MESSENGER MONITOR | {self.prefix} | pending: "
            f"{len(pending_items) or 'none'} | {self.server} "
        )
        self._add(screen, 0, 0, header.ljust(width - 1), curses.A_REVERSE)

        pending_rows = min(len(pending_items), max(0, height - 4))
        for row, ((recipient, sender), (_, _, question)) in enumerate(
            pending_items[:pending_rows], 1
        ):
            self._add(
                screen,
                row,
                0,
                f"   {recipient} <- {sender}: {question}"[: width - 1],
                curses.A_BOLD,
            )

        activity_start = 1 + pending_rows
        activity_rows = height - activity_start - 1
        self.page_size = max(1, activity_rows)
        lines = self.lines
        max_offset = max(0, len(lines) - activity_rows)
        self.scroll_offset = min(self.scroll_offset, max_offset)
        end = len(lines) - self.scroll_offset
        start = max(0, end - activity_rows)
        for row, line in enumerate(lines[start:end], activity_start):
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
