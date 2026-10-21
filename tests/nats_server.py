"""A temporary loopback broker for integration tests; never uses a home server."""
import os
import shutil
import socket
import subprocess
import time
import unittest


class NatsServer:
    def __init__(self):
        self.binary = os.environ.get("NATS_SERVER_BIN") or shutil.which("nats-server")
        if not self.binary:
            if os.environ.get("REQUIRE_NATS_SERVER"):
                raise RuntimeError("install nats-server or set NATS_SERVER_BIN")
            raise unittest.SkipTest("install nats-server or set NATS_SERVER_BIN")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.url = f"nats://127.0.0.1:{self.port}"
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            [self.binary, "-a", "127.0.0.1", "-p", str(self.port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError("test broker exited during startup")
                try:
                    with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                        return self
                except OSError:
                    time.sleep(0.01)
            raise RuntimeError("test broker did not start")
        except BaseException:
            self.stop()
            raise

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
