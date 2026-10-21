"""Agent Messenger behavior independent of applications or a broker server."""
import json
import queue
import threading
import time
import unittest
from unittest.mock import patch

from agent_messenger import (AgentMessenger, AgentMessengerClosed, AgentMessengerError,
                             RequestExpired, RequestTimeout)

class MemoryBroker:
    def __init__(self):
        self.subscriptions = []
        self.published = []
        self.lock = threading.Lock()

    def subscribe(self, pattern, callback):
        with self.lock:
            self.subscriptions.append((pattern, callback))

    def publish(self, topic, data):
        with self.lock:
            self.published.append((topic, data))
            subscriptions = tuple(self.subscriptions)
        for pattern, callback in subscriptions:
            if self._matches(pattern, topic):
                callback(data)

    @staticmethod
    def _matches(pattern, topic):
        pattern_parts = pattern.split(".")
        topic_parts = topic.split(".")
        return len(pattern_parts) == len(topic_parts) and all(
            expected == "*" or expected == actual
            for expected, actual in zip(pattern_parts, topic_parts)
        )



class MemoryTransport:
    def __init__(self, broker):
        self.broker = broker
        self.closed = False

    def subscribe(self, topic, callback):
        self.broker.subscribe(topic, callback)

    def publish(self, topic, data, *, timeout=None):
        if self.closed:
            raise RuntimeError("closed")
        self.broker.publish(topic, data)

    def close(self):
        self.closed = True



class MessengerTests(unittest.TestCase):
    def make_agents(self, *names):
        broker = MemoryBroker()
        agents = {
            name: AgentMessenger(name, MemoryTransport(broker))
            for name in names
        }
        self.addCleanup(lambda: [agent.close() for agent in agents.values()])
        return broker, agents


    @staticmethod
    def wait_for(predicate, timeout=1.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.005)
        raise AssertionError("condition did not become true")


    def test_send_is_delivered_once(self):
        broker, agents = self.make_agents("alice", "bob")
        events = queue.Queue()
        agents["bob"].listen(events.put)

        agents["alice"].send("bob", "hello")
        event = self.wait_for(
            lambda: next(
                (event for event in list(events.queue) if event["type"] == "in_message"),
                None,
            )
        )
        self.assertEqual(event["peer"], "alice")
        self.assertEqual(event["payload"], "hello")
        self.assertEqual([topic for topic, _ in broker.published], ["agents.inbox.bob"])

        envelope = json.loads(broker.published[0][1].decode())
        self.assertEqual(
            envelope,
            {
                "type": "message",
                "sender": "alice",
                "recipient": "bob",
                "payload": "hello",
            },
        )


    def test_multiple_requests_from_same_sender_can_respond_out_of_order(self):
        _, agents = self.make_agents("alice", "bob")
        results = {}

        def ask(label):
            results[label] = agents["alice"].request("bob", label, 2)

        first = threading.Thread(target=ask, args=("first",))
        second = threading.Thread(target=ask, args=("second",))
        first.start()
        second.start()

        pending = self.wait_for(
            lambda: agents["bob"].pending() if len(agents["bob"].pending()) == 2 else None
        )
        by_payload = {item["payload"]: item["request_id"] for item in pending}

        agents["bob"].respond(by_payload["second"], "answer two")
        agents["bob"].respond(by_payload["first"], "answer one")
        first.join(1)
        second.join(1)

        self.assertEqual(results, {"first": "answer one", "second": "answer two"})
        self.assertEqual(agents["bob"].pending(), ())


    def test_timeout_uses_duration_and_receiver_expires_locally(self):
        broker, agents = self.make_agents("alice", "bob")
        bob_events = queue.Queue()
        agents["bob"].listen(bob_events.put)

        with self.assertRaises(RequestTimeout):
            agents["alice"].request("bob", "slow", 0.08)

        request_envelope = next(
            json.loads(data.decode())
            for topic, data in broker.published
            if topic == "agents.inbox.bob"
        )
        self.assertEqual(request_envelope["type"], "request")
        self.assertEqual(request_envelope["payload"], "slow")
        self.assertGreater(request_envelope["timeout"], 0)
        self.assertLessEqual(request_envelope["timeout"], 0.08)
        self.assertNotIn("version", request_envelope)
        self.assertNotIn("kind", request_envelope)
        self.assertNotIn("text", request_envelope)
        self.assertNotIn("expires_at", request_envelope)
        self.assertNotIn("sent_at", request_envelope)
        self.assertNotIn("id", request_envelope)

        self.wait_for(lambda: agents["bob"].pending() == (), timeout=0.5)
        expired = self.wait_for(
            lambda: next(
                (
                    event
                    for event in list(bob_events.queue)
                    if event["type"] == "expired_request"
                ),
                None,
            ),
            timeout=0.5,
        )
        self.assertEqual(expired["payload"], "slow")


    def test_response_from_wrong_agent_is_rejected_and_request_times_out(self):
        broker, agents = self.make_agents("alice", "bob", "mallory")
        alice_events = queue.Queue()
        agents["alice"].listen(alice_events.put)

        outcome = []

        def ask():
            try:
                agents["alice"].request("bob", "secret", 0.12)
            except Exception as error:
                outcome.append(error)

        thread = threading.Thread(target=ask)
        thread.start()
        pending = self.wait_for(lambda: agents["bob"].pending())
        request_id = pending[0]["request_id"]

        fake_response = {
            "type": "response",
            "sender": "mallory",
            "recipient": "alice",
            "payload": "forged",
            "request_id": request_id,
        }
        broker.publish("agents.inbox.alice", json.dumps(fake_response).encode())
        thread.join(1)

        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], RequestTimeout)
        protocol_error = self.wait_for(
            lambda: next(
                (
                    event
                    for event in list(alice_events.queue)
                    if event["type"] == "protocol_error"
                ),
                None,
            )
        )
        self.assertIn("unexpected agent", protocol_error["payload"])


    def test_envelope_for_wrong_recipient_is_rejected(self):
        broker, agents = self.make_agents("alice")
        events = queue.Queue()
        agents["alice"].listen(events.put)

        envelope = {
            "type": "message",
            "sender": "bob",
            "recipient": "carol",
            "payload": "not for alice",
        }
        broker.publish("agents.inbox.alice", json.dumps(envelope).encode())

        protocol_error = self.wait_for(
            lambda: next(
                (
                    event
                    for event in list(events.queue)
                    if event["type"] == "protocol_error"
                ),
                None,
            )
        )
        self.assertIn("does not match", protocol_error["payload"])


    def test_same_name_request_uses_the_same_request_response_path(self):
        _, agents = self.make_agents("alice")
        result = []

        thread = threading.Thread(
            target=lambda: result.append(agents["alice"].request("alice", "think", 1))
        )
        thread.start()
        pending = self.wait_for(lambda: agents["alice"].pending())
        agents["alice"].respond(pending[0]["request_id"], "thought")
        thread.join(1)
        self.assertEqual(result, ["thought"])


    def test_ten_named_agents(self):
        broker = MemoryBroker()
        agents = [
            AgentMessenger(f"agent_{index}", MemoryTransport(broker))
            for index in range(10)
        ]
        self.addCleanup(lambda: [agent.close() for agent in agents])
        received = queue.Queue()
        agents[-1].listen(received.put)

        for agent in agents[:-1]:
            agent.send("agent_9", agent.name)

        messages = []
        deadline = time.monotonic() + 1
        while len(messages) < 9 and time.monotonic() < deadline:
            try:
                event = received.get(timeout=0.05)
            except queue.Empty:
                continue
            if event["type"] == "in_message":
                messages.append(event["payload"])
        self.assertEqual(set(messages), {f"agent_{index}" for index in range(9)})


    def test_request_timeout_includes_time_spent_publishing(self):
        class SlowTransport(MemoryTransport):
            def publish(self, topic, data, *, timeout=None):
                time.sleep(0.08)
                super().publish(topic, data)

        broker = MemoryBroker()
        alice = AgentMessenger("alice", SlowTransport(broker))
        bob = AgentMessenger("bob", MemoryTransport(broker))
        self.addCleanup(alice.close)
        self.addCleanup(bob.close)

        started = time.monotonic()
        with self.assertRaises(RequestTimeout):
            alice.request("bob", "slow publish", 0.05)
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.13)


    def test_discover_returns_sorted_agents_with_same_prefix(self):
        broker = MemoryBroker()
        alice = AgentMessenger("alice", MemoryTransport(broker))
        bob = AgentMessenger("bob", MemoryTransport(broker))
        carol = AgentMessenger("carol", MemoryTransport(broker))
        outsider = AgentMessenger(
            "outsider",
            MemoryTransport(broker),
            prefix="other"
        )
        for messenger in (alice, bob, carol, outsider):
            self.addCleanup(messenger.close)

        self.assertEqual(
            alice.discover(0.05),
            ("alice", "bob", "carol")
        )
        self.assertIn(
            "agents.discovery",
            [topic for topic, _data in broker.published]
        )


    def test_group_discovery_returns_only_opted_in_agents(self):
        broker = MemoryBroker()
        alice = AgentMessenger(
            "alice",
            MemoryTransport(broker),
            discovery_groups=("notes",)
        )
        bob = AgentMessenger("bob", MemoryTransport(broker))
        carol = AgentMessenger(
            "carol",
            MemoryTransport(broker),
            discovery_groups=("notes",)
        )
        for messenger in (alice, bob, carol):
            self.addCleanup(messenger.close)

        self.assertEqual(
            alice.discover(0.05, group="notes"),
            ("alice", "carol")
        )
        self.assertEqual(
            bob.discover(0.05, group="notes"),
            ("alice", "carol")
        )
        self.assertIn(
            "agents.discovery.notes",
            [topic for topic, _data in broker.published]
        )


    def test_discovery_does_not_create_pending_requests(self):
        _broker, agents = self.make_agents("alice", "bob")
        self.assertEqual(agents["alice"].discover(0.05), ("alice", "bob"))
        self.assertEqual(agents["alice"].pending(), ())
        self.assertEqual(agents["bob"].pending(), ())


    def test_close_wakes_blocked_discovery(self):
        broker = MemoryBroker()
        alice = AgentMessenger("alice", MemoryTransport(broker))
        outcome = []

        def discover():
            try:
                alice.discover(60)
            except Exception as error:
                outcome.append(error)

        thread = threading.Thread(target=discover)
        thread.start()
        self.wait_for(
            lambda: any(topic == "agents.discovery" for topic, _ in broker.published)
        )
        alice.close()
        thread.join(1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], AgentMessengerClosed)


    def test_close_wakes_blocked_request(self):
        broker = MemoryBroker()
        alice = AgentMessenger("alice", MemoryTransport(broker))
        outcome = []

        def ask():
            try:
                alice.request("missing", "hello", 60)
            except Exception as error:
                outcome.append(error)

        thread = threading.Thread(target=ask)
        thread.start()
        self.wait_for(lambda: bool(broker.published))

        started = time.monotonic()
        alice.close()
        thread.join(1)
        elapsed = time.monotonic() - started

        self.assertFalse(thread.is_alive())
        self.assertLess(elapsed, 1)
        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], AgentMessengerClosed)


    def test_timeout_must_be_positive_and_finite(self):
        broker = MemoryBroker()
        agent = AgentMessenger("alice", MemoryTransport(broker))
        self.addCleanup(agent.close)

        for value in (0, -1, float("nan"), float("inf"), float("-inf"), True):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    AgentMessengerError,
                    "positive finite number",
                ):
                    agent.request("bob", "test", value)
                with self.assertRaisesRegex(
                    AgentMessengerError,
                    "positive finite number",
                ):
                    agent.discover(value)


    def test_prefix_must_use_valid_dot_separated_names(self):
        broker = MemoryBroker()

        for prefix in ("", ".agents", "agents.", "agents..test", "agents.*", "bad prefix"):
            with self.subTest(prefix=prefix):
                with self.assertRaises(AgentMessengerError):
                    AgentMessenger("alice", MemoryTransport(broker), prefix=prefix)

        agent = AgentMessenger(
            "alice",
            MemoryTransport(broker),
            prefix="jai.project_agents",
        )
        agent.close()


    def test_subscribe_failure_closes_owned_transport(self):
        class FailingTransport:
            def __init__(self):
                self.closed = False

            def subscribe(self, topic, callback):
                raise RuntimeError("subscribe failed")

            def publish(self, topic, data):
                raise AssertionError("publish should not be called")

            def close(self):
                self.closed = True

        transport = FailingTransport()
        with self.assertRaisesRegex(AgentMessengerError, "subscribe failed"):
            AgentMessenger("alice", transport)
        self.assertTrue(transport.closed)


    def test_messenger_worker_queue_does_not_restart_timeout(self):
        broker = MemoryBroker()
        blocked = threading.Event()
        release = threading.Event()
        expired = threading.Event()
        requests = []

        def listener(event):
            if event["type"] == "in_message":
                blocked.set()
                release.wait(2)
            elif event["type"] == "in_request":
                requests.append(event)
            elif event["type"] == "expired_request":
                expired.set()

        receiver = AgentMessenger("receiver", MemoryTransport(broker), on_event=listener)
        caller = AgentMessenger("caller", MemoryTransport(broker))
        self.addCleanup(receiver.close)
        self.addCleanup(caller.close)
        try:
            caller.send("receiver", "hold worker")
            self.assertTrue(blocked.wait(1))
            with self.assertRaises(RequestTimeout):
                caller.request("receiver", "expired before dispatch", timeout=0.15)
        finally:
            release.set()
        self.assertTrue(expired.wait(1))
        self.assertEqual(requests, [])
        self.assertEqual(receiver.pending(), ())


    def test_outgoing_listener_delay_does_not_publish_expired_request(self):
        broker = MemoryBroker()
        def listener(event):
            if event["type"] == "out_request":
                threading.Event().wait(0.15)

        caller = AgentMessenger("caller", MemoryTransport(broker), on_event=listener)
        receiver = AgentMessenger("receiver", MemoryTransport(broker))
        self.addCleanup(caller.close)
        self.addCleanup(receiver.close)
        with self.assertRaises(RequestTimeout):
            caller.request("receiver", "never send", timeout=0.1)
        self.assertFalse(any(json.loads(data)["type"] == "request" for _topic, data in broker.published))

    def test_request_and_discovery_pass_remaining_deadline_to_transport(self):
        class StalledTransport(MemoryTransport):
            def publish(self, topic, data, *, timeout=None):
                if timeout is None:
                    raise AssertionError("publication was not given a deadline")
                self.allowance = timeout
                time.sleep(timeout)
                raise TimeoutError("publication stalled")

        transport = StalledTransport(MemoryBroker())
        caller = AgentMessenger("caller", transport)
        self.addCleanup(caller.close)
        for operation, error_type in ((lambda: caller.request("peer", "hello", timeout=0.03), RequestTimeout),
                                      (lambda: caller.discover(timeout=0.03), AgentMessengerError)):
            started = time.monotonic()
            with self.assertRaises(error_type):
                operation()
            self.assertLess(time.monotonic() - started, 0.3)
            self.assertGreater(transport.allowance, 0)
            self.assertLessEqual(transport.allowance, 0.03)
            self.assertEqual(caller._waiting, {})
            self.assertEqual(caller._discoveries, {})

    def test_response_listener_cannot_publish_after_expiration(self):
        broker, agents = self.make_agents("receiver")
        receiver = agents["receiver"]
        broker.publish("agents.inbox.receiver", json.dumps({
            "type": "request", "sender": "caller", "recipient": "receiver",
            "payload": "hello", "request_id": "deadline-test", "timeout": 0.05,
        }).encode())
        self.wait_for(receiver.pending)
        receiver.listen(lambda event: time.sleep(0.07) if event["type"] == "out_response" else None)
        with self.assertRaises(RequestExpired):
            receiver.respond("deadline-test", "late")
        self.assertFalse(any(json.loads(data)["type"] == "response" for _, data in broker.published))

    def test_response_publication_uses_remaining_deadline(self):
        broker, agents = self.make_agents("receiver")
        receiver = agents["receiver"]
        broker.publish("agents.inbox.receiver", json.dumps({
            "type": "request", "sender": "caller", "recipient": "receiver",
            "payload": "hello", "request_id": "deadline-test", "timeout": 0.5,
        }).encode())
        self.wait_for(receiver.pending)
        with patch.object(receiver.transport, "publish", wraps=receiver.transport.publish) as publish:
            receiver.respond("deadline-test", "answer")
        allowance = publish.call_args.kwargs["timeout"]
        self.assertGreater(allowance, 0)
        self.assertLessEqual(allowance, 0.5)

    def test_late_response_is_rejected_while_caller_is_still_publishing(self):
        broker, agents = self.make_agents("caller")
        caller = agents["caller"]
        handled = threading.Event()
        events = []

        def observe(event):
            events.append(event)
            if event["type"] in {"late_response", "in_response"}:
                handled.set()

        caller.listen(observe)

        def delayed_publication(topic, data, *, timeout=None):
            request = json.loads(data)
            time.sleep(0.05)
            broker.publish("agents.inbox.caller", json.dumps({
                "type": "response", "sender": "peer", "recipient": "caller",
                "request_id": request["request_id"], "payload": "too late",
            }).encode())
            self.assertTrue(handled.wait(1))

        with patch.object(caller.transport, "publish", side_effect=delayed_publication):
            with self.assertRaises(RequestTimeout):
                caller.request("peer", "hello", timeout=0.02)
        self.assertTrue(any(event["type"] == "late_response" for event in events))
        self.assertFalse(any(event["type"] == "in_response" for event in events))

    def test_transport_failures_have_messenger_error_with_original_cause(self):
        _, agents = self.make_agents("caller")
        caller = agents["caller"]
        original = RuntimeError("connection lost")
        with patch.object(caller.transport, "publish", side_effect=original):
            with self.assertRaises(AgentMessengerError) as error:
                caller.send("peer", "hello")
        self.assertIs(error.exception.__cause__, original)

    def test_subscription_failure_is_preserved_if_transport_cleanup_also_fails(self):
        transport = MemoryTransport(MemoryBroker())
        original = RuntimeError("cannot subscribe")
        with patch.object(transport, "subscribe", side_effect=original), \
             patch.object(transport, "close", side_effect=RuntimeError("cannot close")):
            with self.assertRaises(AgentMessengerError) as error:
                AgentMessenger("caller", transport)
        self.assertIs(error.exception.__cause__, original)


if __name__ == "__main__":
    unittest.main()


