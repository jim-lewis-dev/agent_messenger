"""Terminal/monitor checks without a broker or interactive terminal."""
import contextlib
import io
import json
import unittest
from unittest.mock import MagicMock, patch

import agent_messenger_monitor as monitor
import agent_messenger_terminal as terminal


class FakeTransport:
    def __init__(self):
        self.callback = None
        self.closed = 0

    def subscribe(self, topic, callback):
        self.topic = topic
        self.callback = callback

    def close(self):
        self.closed += 1


class ToolsTests(unittest.TestCase):
    def make_monitor(self, agents=()):
        transport = FakeTransport()
        with patch.object(monitor, "NatsTransport", return_value=transport):
            tool = monitor.FlowTerminal("nats://alice:secret@localhost:4222", "agents", list(agents))
        self.addCleanup(tool.close)
        return tool, transport

    def test_monitor_closes_transport_when_subscribe_fails(self):
        transport = FakeTransport()
        with patch.object(transport, "subscribe", side_effect=RuntimeError("subscription failed")):
            with patch.object(monitor, "NatsTransport", return_value=transport):
                with self.assertRaisesRegex(RuntimeError, "subscription failed"):
                    monitor.FlowTerminal("nats://localhost:4222", "agents", [])
        self.assertEqual(transport.closed, 1)

    def test_terminal_closes_transport_on_invalid_name(self):
        transport = FakeTransport()
        with patch.object(terminal, "NatsTransport", return_value=transport):
            with self.assertRaises(terminal.AgentMessengerError):
                terminal.AgentTerminal("invalid name", "nats://localhost:4222", "agents")
        self.assertEqual(transport.closed, 1)

    def test_constructor_failure_is_preserved_when_cleanup_also_fails(self):
        transport = FakeTransport()
        with patch.object(transport, "close", side_effect=RuntimeError("cleanup failed")):
            with patch.object(transport, "subscribe", side_effect=RuntimeError("subscription failed")):
                with patch.object(monitor, "NatsTransport", return_value=transport):
                    with self.assertRaisesRegex(RuntimeError, "subscription failed"):
                        monitor.FlowTerminal("nats://localhost:4222", "agents", [])
            with patch.object(terminal, "NatsTransport", return_value=transport):
                with self.assertRaisesRegex(terminal.AgentMessengerError, "agent names"):
                    terminal.AgentTerminal("invalid name", "nats://localhost:4222", "agents")

    def test_invalid_broker_address_is_rejected_before_opening_transport(self):
        for module, constructor, args in (
            (monitor, monitor.FlowTerminal, ("nats://[bad", "agents", [])),
            (terminal, terminal.AgentTerminal, ("human", "nats://[bad", "agents")),
        ):
            with self.subTest(module=module.__name__):
                with patch.object(module, "NatsTransport") as factory:
                    with self.assertRaises(ValueError):
                        constructor(*args)
                    factory.assert_not_called()

    def test_terminal_exposes_discovery_and_reports_result(self):
        fake_agent = MagicMock()
        fake_agent.discover.return_value = ("alice", "bob")
        with patch.object(terminal, "NatsTransport", return_value=FakeTransport()):
            with patch.object(terminal, "AgentMessenger", return_value=fake_agent):
                tool = terminal.AgentTerminal("human", "nats://localhost:4222", "agents")
        self.addCleanup(tool.close)
        tool._execute("discover(timeout=0.2)")
        fake_agent.discover.assert_called_once_with(timeout=0.2)
        result = tool.events.get_nowait()
        self.assertEqual(result["type"], "result")
        self.assertEqual(result["payload"], "('alice', 'bob')")

    def test_monitor_deadline_includes_time_spent_queued(self):
        tool, transport = self.make_monitor()
        message = {"type": "request", "sender": "alice", "recipient": "bob",
                   "request_id": "request123", "timeout": 2, "payload": "hello"}
        with patch.object(monitor.time, "monotonic", return_value=100):
            transport.callback(json.dumps(message).encode())
        with patch.object(monitor.time, "monotonic", return_value=105):
            tool._drain()
            tool._expire_pending()
        self.assertEqual(tool.pending, {})
        self.assertEqual(len(tool.lines), 1)

    def test_monitor_response_must_match_request_participants(self):
        tool, _ = self.make_monitor()
        tool._update_pending({"type": "request", "sender": "alice", "recipient": "bob",
                              "request_id": "request123", "timeout": 10}, 100)
        tool._update_pending({"type": "response", "sender": "mallory", "recipient": "alice",
                              "request_id": "request123"}, 101)
        self.assertIn("request123", tool.pending)
        tool._update_pending({"type": "response", "sender": "bob", "recipient": "alice",
                              "request_id": "request123"}, 102)
        self.assertEqual(tool.pending, {})

    def test_monitor_filter_and_bad_input_keep_processing(self):
        tool, transport = self.make_monitor(["alice"])
        transport.callback(b"not JSON")
        for sender in ("carol", "alice"):
            transport.callback(json.dumps({"type": "message", "sender": sender,
                                            "recipient": "bob", "payload": "hi"}).encode())
        tool._drain()
        self.assertEqual(len(tool.lines), 1)
        self.assertIn("alice", tool.lines[0])

    def test_tools_display_broker_without_credentials(self):
        tool, _ = self.make_monitor()
        self.assertEqual(tool.server_label, "nats://localhost:4222")
        fake_agent = MagicMock()
        with patch.object(terminal, "NatsTransport", return_value=FakeTransport()):
            with patch.object(terminal, "AgentMessenger", return_value=fake_agent):
                tool = terminal.AgentTerminal("human", "nats://alice:secret@localhost:4222", "agents")
        output = tool._format_event({"type": "ready", "time": "12:00:00"})
        self.assertIn("nats://localhost:4222", output)
        self.assertNotIn("secret", output)
        self.assertNotIn("alice", output)
        tool.close()

    def test_terminal_short_request_reference_rejects_ambiguity(self):
        tool = terminal.AgentTerminal.__new__(terminal.AgentTerminal)
        tool.agent = MagicMock()
        tool.agent.pending.return_value = [{"request_id": "12345678abcdef"}, {"request_id": "12345678fedcba"}]
        with self.assertRaisesRegex(terminal.AgentMessengerError, "ambiguous"):
            tool.respond("12345678", "hello")
        tool.respond("12345678a", "hello")
        tool.agent.respond.assert_called_once_with("12345678abcdef", "hello")

    def test_main_defaults_to_localhost_and_closes_after_keyboard_interrupt(self):
        for module, name, args, expected in (
            (monitor, "FlowTerminal", [], ("nats://127.0.0.1:4222", "agents", [])),
            (terminal, "AgentTerminal", ["human"], ("human", "nats://127.0.0.1:4222", "agents")),
        ):
            with self.subTest(module=module.__name__):
                with patch.object(module, name) as factory:
                    factory.return_value.run.side_effect = KeyboardInterrupt
                    self.assertEqual(module.main(args), 0)
                    factory.assert_called_once_with(*expected)
                    factory.return_value.close.assert_called_once_with()

    def test_shutdown_failure_is_reported_without_traceback(self):
        for module, name, args in ((monitor, "FlowTerminal", []), (terminal, "AgentTerminal", ["human"])):
            with self.subTest(module=module.__name__):
                with patch.object(module, name) as factory:
                    factory.return_value.close.side_effect = RuntimeError("close failed")
                    output = io.StringIO()
                    with contextlib.redirect_stderr(output):
                        self.assertEqual(module.main(args), 1)
                    self.assertIn("close failed", output.getvalue())
                    self.assertNotIn("Traceback", output.getvalue())


if __name__ == "__main__":
    unittest.main()
