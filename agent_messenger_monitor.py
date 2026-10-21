#!/usr/bin/env python3
"""Full-screen system-wide Agent Messenger traffic monitor."""

import argparse
from contextlib import suppress
import curses
import json
import math
import queue
import sys
import time
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

from agent_messenger import format_message_line, one_line
from nats_transport import NatsTransport


class FlowTerminal:
    def __init__(
        self,
        server: str,
        prefix: str,
        agents: list[str],
    ):
        self.server = server
        self.prefix = prefix
        self.agents = frozenset(agents)
        self.events: queue.Queue[tuple[bytes, float]] = queue.Queue()
        self.lines: list[str] = []
        self.scroll_offset = 0
        self.page_size = 1
        self.pending: dict[str, tuple[str, str, float, str]] = {}
        self.status = (
            "LIVE — PgUp/PgDn scroll | Home/End oldest/newest | Ctrl-L clear | "
            "Ctrl-C or Q quit"
        )
        self.running = True
        address = urlsplit(server)
        self.server_label = urlunsplit(address._replace(netloc=address.netloc.rsplit("@", 1)[-1]))
        self.transport = NatsTransport(server)
        try:
            self.transport.subscribe(f"{prefix}.inbox.*", self._enqueue)
        except BaseException:
            with suppress(BaseException):
                self.transport.close()
            raise

    def _enqueue(self, data: bytes) -> None:
        self.events.put((data, time.monotonic()))

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
            if key in ("q", "Q", "\x03"):
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
            elif key == curses.KEY_HOME:
                self.scroll_offset = max(0, len(self.lines) - self.page_size)

    def _drain(self) -> None:
        while True:
            try:
                data, received_at = self.events.get_nowait()
            except queue.Empty:
                return
            try:
                envelope = json.loads(data.decode())
                if not self._matches(envelope):
                    continue
                self._update_pending(envelope, received_at)
                timestamp = datetime.now().strftime("%H:%M:%S")
                payload = one_line(envelope.get("payload", ""))
                request_id = envelope.get("request_id")
                if isinstance(request_id, str):
                    payload = f"[{request_id[:8]}] {payload}"
                if self.scroll_offset:
                    self.scroll_offset += 1
                self.lines.append(
                    format_message_line(
                        timestamp,
                        envelope["type"],
                        envelope["sender"],
                        envelope["recipient"],
                        payload,
                    )
                )
                self.status = (
                    f"LIVE — {len(self.lines)} events | PgUp/PgDn scroll | "
                    "Home/End oldest/newest | Ctrl-L clear | Ctrl-C or Q quit"
                )
            except Exception as error:
                self.status = "ERROR — " + one_line(error)

    def _matches(self, envelope: dict) -> bool:
        if not self.agents:
            return True
        return (
            envelope.get("sender") in self.agents
            or envelope.get("recipient") in self.agents
        )

    def _update_pending(self, envelope: dict, received_at: float) -> None:
        sender = envelope.get("sender")
        recipient = envelope.get("recipient")
        message_type = envelope.get("type")
        request_id = envelope.get("request_id")
        if not all(isinstance(value, str) for value in (sender, recipient, request_id)):
            return

        if message_type == "request":
            timeout = envelope.get("timeout")
            if (
                isinstance(timeout, (int, float))
                and not isinstance(timeout, bool)
                and math.isfinite(float(timeout))
                and timeout > 0
            ):
                self.pending[request_id] = (
                    sender,
                    recipient,
                    received_at + float(timeout),
                    one_line(envelope.get("payload", "")),
                )
        elif message_type == "response":
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

        pending_items = sorted(
            self.pending.items(),
            key=lambda item: (item[1][2], item[1][0], item[1][1], item[0]),
        )
        agent_scope = ",".join(sorted(self.agents)) if self.agents else "all"
        header = (
            f" AGENT MESSENGER MONITOR | agents: {agent_scope} | {self.prefix} | "
            f"pending: {len(pending_items) or 'none'} | {self.server_label} "
        )
        self._add(screen, 0, 0, header.ljust(width - 1), curses.A_REVERSE)

        pending_rows = min(len(pending_items), max(0, height - 4))
        now = time.monotonic()
        for row, (request_id, (sender, recipient, deadline, payload)) in enumerate(
            pending_items[:pending_rows], 1
        ):
            seconds = max(0, math.ceil(deadline - now))
            line = (
                f"   {request_id[:8]} [{seconds:5d}s left]  "
                f"{sender[:16]:<16}  ->  {recipient[:16]:<16}  :  {payload}"
            )
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="nats://127.0.0.1:4222")
    parser.add_argument("--prefix", default="agents")
    parser.add_argument(
        "--agents",
        nargs="+",
        default=[],
        metavar="NAME",
        help="show only traffic involving these agents",
    )
    args = parser.parse_args(argv)

    terminal = None
    result = 0
    try:
        terminal = FlowTerminal(args.server, args.prefix, args.agents)
        terminal.run()
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"ERROR — {one_line(error)}", file=sys.stderr)
        result = 1
    finally:
        if terminal is not None:
            try:
                terminal.close()
            except Exception as error:
                print(f"ERROR — {one_line(error)}", file=sys.stderr)
                result = 1
    return result


if __name__ == "__main__":
    raise SystemExit(main())
