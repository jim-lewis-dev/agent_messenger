"""Verify Agent Messenger's existing protocol over the standalone transport."""

import asyncio
import json
import queue
import threading
import time
import unittest
from unittest.mock import patch

from agent_messenger import AgentMessenger, AgentMessengerError, RequestTimeout
from nats_transport import NatsTransport
from nats_server import NatsServer


class MessengerNatsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = NatsServer().start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def messenger(self, name, **options):
        messenger = AgentMessenger(name, NatsTransport(self.server.url), **options)
        self.addCleanup(messenger.close)
        return messenger

    def test_original_envelope_and_json_string_payload(self):
        wire = queue.Queue()
        monitor = NatsTransport(self.server.url)
        self.addCleanup(monitor.close)
        monitor.subscribe("agents.inbox.bob", wire.put)
        alice = self.messenger("alice")
        payload = '{"command":"status","data":[1,2]}'
        alice.send("bob", payload)
        envelope = json.loads(wire.get(timeout=2))
        self.assertEqual(envelope, {
            "type": "message", "sender": "alice", "recipient": "bob", "payload": payload,
        })

    def test_requests_replies_and_discovery(self):
        alice = self.messenger("alice")
        bob = self.messenger("bob", discovery_groups=("workers",))

        def answer(event):
            if event["type"] == "in_request":
                bob.respond(event["request_id"], event["payload"].upper())

        bob.listen(answer)
        self.assertEqual(alice.request("bob", "hello", timeout=1), "HELLO")
        self.assertEqual(alice.discover(timeout=0.15), ("alice", "bob"))
        self.assertEqual(alice.discover(timeout=0.15, group="workers"), ("bob",))

    def test_simultaneous_requests_are_correlated(self):
        alice, bob = self.messenger("alice"), self.messenger("bob")
        for messenger in (alice, bob):
            messenger.listen(lambda event, owner=messenger: (
                owner.respond(event["request_id"], "reply:" + event["payload"])
                if event["type"] == "in_request" else None
            ))
        results = queue.Queue()

        def request(source, recipient, number):
            try:
                payload = f"{source.name}:{number}"
                results.put((payload, source.request(recipient, payload, timeout=2)))
            except Exception as error:
                results.put(error)

        threads = [threading.Thread(target=request, args=(source, recipient, index))
                   for source, recipient in ((alice, "bob"), (bob, "alice")) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        for _ in threads:
            result = results.get(timeout=1)
            self.assertIsInstance(result, tuple)
            payload, reply = result
            self.assertEqual(reply, "reply:" + payload)

    def test_missing_peer_uses_request_deadline(self):
        alice = self.messenger("alice")
        started = time.monotonic()
        with self.assertRaises(RequestTimeout):
            alice.request("missing", "question", timeout=0.08)
        self.assertLess(time.monotonic() - started, 0.3)

    def test_request_deadline_bounds_a_stalled_local_publication(self):
        alice = self.messenger("alice")
        cancelled = threading.Event()

        async def stalled_publish(*_args):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        with patch.object(alice.transport._nc, "publish", stalled_publish):
            started = time.monotonic()
            with self.assertRaises(RequestTimeout):
                alice.request("bob", "question", timeout=0.05)
            self.assertLess(time.monotonic() - started, 0.3)
            self.assertTrue(cancelled.wait(1))
        self.assertEqual(alice._waiting, {})

    def test_transport_errors_keep_messenger_exception_contract(self):
        alice = self.messenger("alice")
        alice.transport.close()
        with self.assertRaises(AgentMessengerError) as caught:
            alice.send("bob", "question")
        self.assertIsNotNone(caught.exception.__cause__)

    def test_discovery_deadline_bounds_local_publication_and_cleans_up(self):
        alice = self.messenger("alice")

        async def stalled_publish(*_args):
            await asyncio.Event().wait()

        with patch.object(alice.transport._nc, "publish", stalled_publish):
            started = time.monotonic()
            with self.assertRaises(AgentMessengerError):
                alice.discover(timeout=0.05)
            self.assertLess(time.monotonic() - started, 0.3)
        self.assertEqual(alice._discoveries, {})


if __name__ == "__main__":
    unittest.main()
