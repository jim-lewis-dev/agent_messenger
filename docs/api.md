# API and behavior

[Back to the README](../README.md)


```python
import json
import queue
from agent_messenger import AgentMessenger
from nats_transport import NatsTransport

events = queue.Queue()
messenger = AgentMessenger(
    "atlas", NatsTransport("nats://127.0.0.1:4223"), on_event=events.put
)
try:
    print(messenger.discover(timeout=0.3))
    messenger.send("beacon", json.dumps({"event": "ready"}))
    reply = messenger.request("beacon", json.dumps({"command": "ping"}), timeout=5)
    print(json.loads(reply))
finally:
    messenger.close()
```

| Call | Behavior |
| --- | --- |
| `AgentMessenger(name, transport, prefix="agents", on_event=None, discovery_groups=())` | Subscribe and start one receive worker; takes ownership of the transport. |
| `send(recipient, payload)` | Publish a string without waiting for an application reply. |
| `request(recipient, payload, timeout=10)` | Wait for one matching string reply or raise `RequestTimeout`. |
| `respond(request_id, payload)` | Reply to a live incoming request; one successful local publication consumes it. |
| `discover(timeout=0.2, group=None)` | Collect and return a sorted tuple of names; includes this peer when it belongs to the selected scope. |
| `pending()` | Return incoming request snapshots with `request_id`, `peer`, `payload`, and remaining seconds. |
| `listen(callback)` / `unlisten(callback)` | Add/remove an event listener. |
| `close()` | Wake waiting callers, stop the worker, and close the transport; repeated calls are safe. |

Names and discovery groups use letters, digits, underscores, and hyphens.
Prefixes are dot-separated components with those characters. Payloads are
strings. Use `json.dumps()` / `json.loads()` for JSON; Messenger's surrounding
envelope is handled automatically. Existing imports stay
`from agent_messenger import AgentMessenger`.

An incoming message event has `type="in_message"`, `peer`, and `payload`.
An incoming request has `type="in_request"`, plus `request_id`, `timeout`, and
`deadline`. That deadline uses the receiving process's `time.monotonic()` clock;
never compare it to another computer's clock or save it as a wall-clock time.
The [echo example](../examples/echo.py) shows processing these events outside the worker.

Listeners run synchronously and must return promptly. Put events into a queue
and process slow work elsewhere. Calling blocking `request()` or `discover()`
from the receive worker prevents that worker from processing replies. Callback
exceptions are isolated from other listeners; applications should report their
own processing errors.

All Messenger operation errors derive from `AgentMessengerError`. Specific
cases include `AgentMessengerClosed`, `RequestTimeout`, `NoPendingRequest`, and
`RequestExpired`. Failures constructing `NatsTransport` are transport errors
and occur before Messenger exists. Keep them in your application's error
handling too.

## Delivery and deadlines

This is live Core NATS messaging. A peer must be subscribed and connected to
receive traffic; messages missed while offline are not recovered. `send()`
returning means local publication completed, not that a peer received or
processed the message. Duplicate peer names can cause multiple programs to
perform the same request; name uniqueness is the application's responsibility.

Request and discovery timeouts cover their local publication and waiting.
Messenger passes the remaining allowance to the transport. Incoming request
expiry includes time waiting in Messenger's receive queue. Responses arriving
after the caller's deadline are rejected. Receiver timeouts are relative to
local receipt, so network transit and transport queue delay are not a shared
cross-computer clock or an exact global deadline.

A timeout does not cancel work already started remotely and does not prove a
mutation failed. Messenger does not automatically retry requests. The caller
must decide whether retrying an application action is safe. User callbacks and
custom transport implementations must honor their own timing contracts; Python
cannot interrupt arbitrary synchronous application code. `close()` allows up
to two seconds for its worker to exit, then closes the transport using the
transport's own deadline.

The core uses a small transport interface, so another implementation can be
supplied: `subscribe(topic, callback)`, `publish(topic, data, *, timeout=None)`,
and `close()`. Subscription callbacks receive bytes. The pinned NATS transport
implements this interface; custom transports must honor the publication
timeout. Messenger owns and closes its transport; do not share one instance
between multiple Messenger objects.

## Protocol and access


Inbox subjects are `<prefix>.inbox.<name>`. Discovery uses
`<prefix>.discovery` and `<prefix>.discovery.<group>`. The payload of a normal
message is a UTF-8 JSON object:

```json
{"type":"message","sender":"atlas","recipient":"beacon","payload":"hello"}
```

Requests add `request_id` and a positive `timeout` in seconds. Responses contain
the same request ID and a string payload. Discovery messages contain `type`,
`sender`, and `request_id`; discovery replies also contain `recipient`. The
discovery type names are `discover` and `discover_response`. There is one
format; application data lives in the string payload.

Configure NATS authentication, TLS, and subject permissions for your network.
Messenger peer names are routing labels, not authenticated identities. The
monitor requires permission to subscribe to `<prefix>.inbox.*`, which exposes
message contents. Never commit broker credentials or personal configuration.

