"""Focused v2 daemon state tests through synthetic IPC clients and fake PTYs.

Subprocess-level multi-device coverage lives in ``test_daemon_multidevice.py``;
this module injects a deterministic monotonic clock for lease expiry.
"""

import os
import pty
import socket
import time

import pytest

import serial_mcp_ipc as ipc
from serial_mcp_daemon import DaemonServer
from serial_mcp_engine import SerialState


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def live_daemon(tmp_path):
    runtime = tmp_path / "runtime"
    clock = Clock()
    state = SerialState(log_path=str(tmp_path / "device.log"))
    daemon = DaemonServer(runtime=runtime, clock=clock, state=state,
                          start_housekeeping=False)
    daemon.start()
    clients = []

    def client(role="mcp"):
        deadline = time.monotonic() + 3
        while True:
            try:
                value = ipc.DaemonClient.connect(path=runtime / "daemon.sock", timeout=0.1)
                break
            except ipc.DaemonUnavailable:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        clients.append(value)
        value.hello(role=role, name="synthetic")
        return value

    yield daemon, clock, client
    for value in clients:
        value.close()
    daemon.stop("test")
    state.disconnect()


def event_of(client, name):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        event = client.next_event(timeout=max(0.01, deadline - time.monotonic()))
        if event and event.get("event") == name:
            return event
    raise AssertionError(f"did not receive {name} event")


def wait_until(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached")


def fake_tty():
    master, slave = pty.openpty()
    port = os.ttyname(slave)
    os.close(slave)  # the daemon must be the only process opening the slave
    return master, port


def test_expired_control_requires_explicit_connect_and_never_auto_takes_over(live_daemon):
    daemon, clock, new_client = live_daemon
    master, port = fake_tty()
    first = new_client()
    observer = new_client()
    try:
        assert first.call("connect", {"port": port, "baudrate": 115200})["control"] == "granted"
        assert observer.call("connect", {"port": port, "baudrate": 57600})["control"] == "not_granted"
        clock.advance(61)
        daemon.tick()
        assert event_of(first, "control_lost")["reason"] == "expired"
        with pytest.raises(ipc.DaemonError) as error:
            observer.call("send_command", {"port": port, "text": "no takeover"})
        assert error.value.code == "control_required"
        assert observer.call("connect", {"port": port, "baudrate": 57600})["control"] == "granted"
        with pytest.raises(ipc.DaemonError) as error:
            first.call("send_command", {"port": port, "text": "not owner"})
        assert error.value.code == "busy"
    finally:
        first.close(); observer.close(); os.close(master)


def test_zero_clients_grace_is_cancelled_then_expires_on_controlled_clock(live_daemon):
    daemon, clock, new_client = live_daemon
    first = new_client()
    first.close()
    wait_until(lambda: daemon.status()["clients"] == 0)
    clock.advance(29)
    daemon.tick()
    assert not daemon.stopped
    second = new_client()
    assert second.status()["grace"]["active"] is False
    second.close()
    wait_until(lambda: daemon.status()["clients"] == 0)
    clock.advance(31)
    daemon.tick()
    assert daemon.stopped


def test_driver_rejection_is_an_ipc_error_and_keeps_actual_rate(live_daemon):
    daemon, _clock, new_client = live_daemon
    master, port = fake_tty()
    client = new_client()
    try:
        client.call("connect", {"port": port, "baudrate": 9600})
        device = next(iter(daemon._devices.values()))
        original = device.state.set_baudrate
        device.state.set_baudrate = lambda _rate: "Error: driver rejected rate; actual baudrate is 9600."
        with pytest.raises(ipc.DaemonError) as error:
            client.call("set_baudrate", {"port": port, "baudrate": 19200})
        assert error.value.code == "baudrate_rejected"
        assert client.status()["devices"][0]["baudrate"] == 9600
        device.state.set_baudrate = original
    finally:
        client.close(); os.close(master)


def test_each_member_reads_its_own_device_output_cursor(live_daemon):
    _daemon, _clock, new_client = live_daemon
    master, port = fake_tty()
    first = new_client()
    observer = new_client()
    try:
        first.call("connect", {"port": port, "baudrate": 115200})
        observer.call("connect", {"port": port, "baudrate": 115200})
        os.write(master, b"each observer receives this")
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if "each observer receives this" in first.call(
                    "read_output", {"port": port, "timeout": 0}):
                break
            time.sleep(0.01)
        # The first read did not consume the observer's cursor.
        assert "each observer receives this" in observer.call(
            "read_output", {"port": port, "timeout": 0})
    finally:
        first.close(); observer.close(); os.close(master)
