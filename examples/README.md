# Examples

[Back to the project](../README.md)

The examples use a running Core NATS broker. Install the package first and run
these commands from the repository root. The commands below assume the local
demo broker from the [quick start](../README.md#quick-start).

## A JSON round trip

Keep the receiver running in one terminal:

```bash
.venv/bin/python examples/echo.py beacon --server nats://127.0.0.1:4223
```

Send a request from another:

```bash
.venv/bin/python examples/echo.py atlas --ask beacon \
  --message '{"text":"hello from atlas","value":42}' \
  --server nats://127.0.0.1:4223
```

The sender prints the same JSON and exits. `--timeout 10` changes how long it
will wait. The receiver stays online until you press Ctrl-C.

Read [echo.py](echo.py) to see the receiving pattern: the callback puts events
on an application queue; the application takes an event, does its work, and
calls `respond()`. Replace that work with your own application behavior.

## Find peers and send notifications

With `beacon` still running, open the interactive terminal:

```bash
.venv/bin/agent-messenger-terminal operator --server nats://127.0.0.1:4223
```

Enter these Python expressions inside it:

```python
discover(timeout=0.3)
send("beacon", '{"event":"backup.finished","files":42}')
request("beacon", '{"question":"ready?"}', timeout=5)
```

Discovery returns names of responding peers, including `operator` itself.
The notification appears in the receiver's terminal. The request's JSON comes
back as a reply. Neither example executes the contents of a message.

## Watch a conversation

Start the monitor before sending your next message:

```bash
.venv/bin/agent-messenger-monitor --server nats://127.0.0.1:4223
```

Repeat the terminal expressions to see messages, request IDs, and matching
responses. The monitor observes live inbox traffic; discovery is not displayed.

## When something does not connect

| Symptom | Check |
| --- | --- |
| Connection fails | Is the broker running at the address passed to `--server`? |
| Request times out | Is the named receiver online, using the same broker and prefix, and replying before the timeout? |
| A second computer cannot connect | `127.0.0.1` is local to each computer; use a shared broker address and configure its listener and network access. |
| Import or command not found | Use the `.venv/bin/` commands after installing the package into that environment. |

For your own handlers, see the [API and behavior reference](../docs/api.md).
