#!/usr/bin/env python3
"""Echo requests between named computers using an application-owned queue.

Receiver: python examples/echo.py beacon
Sender:   python examples/echo.py atlas --ask beacon --message '{"command":"ping"}'
Both accept --server nats://BROKER:4222. The receiver echoes text unchanged.
"""
import argparse
import queue
import sys

from agent_messenger import AgentMessenger, AgentMessengerError
from nats_transport import NatsTransport, NatsTransportError


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name")
    parser.add_argument("--ask", metavar="PEER")
    parser.add_argument("--message", default='{"command":"ping"}')
    parser.add_argument("--server", default="nats://127.0.0.1:4222")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args(argv)
    events = queue.Queue()
    transport = None
    messenger = None
    result = 0
    try:
        transport = NatsTransport(args.server)
        messenger = AgentMessenger(args.name, transport, on_event=events.put)
        if args.ask:
            print(messenger.request(args.ask, args.message, timeout=args.timeout))
        else:
            print(f"{args.name} ready; Ctrl-C stops it.", flush=True)
            while True:
                event = events.get()
                if event["type"] == "in_request":
                    try:
                        messenger.respond(event["request_id"], event["payload"])
                    except AgentMessengerError as error:
                        print(f"Could not reply: {error}", file=sys.stderr)
                elif event["type"] == "in_message":
                    print(f"{event['peer']}: {event['payload']}", flush=True)
    except KeyboardInterrupt:
        pass
    except (AgentMessengerError, NatsTransportError) as error:
        print(f"Messenger: {error}", file=sys.stderr)
        result = 1
    finally:
        owner = messenger if messenger is not None else transport
        if owner is not None:
            try:
                owner.close()
            except (AgentMessengerError, NatsTransportError) as error:
                print(f"Messenger shutdown: {error}", file=sys.stderr)
                result = 1
    return result


if __name__ == "__main__":
    raise SystemExit(main())
