"""Protocol-v2 integration tests through a real daemon subprocess and fake PTYs."""

import os
import pty
import select
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import serial_mcp_ipc as ipc

ROOT = Path(__file__).resolve().parent


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not reached")


@pytest.fixture
def devices_runtime(tmp_path):
    runtime = tmp_path / "runtime"
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               SERIAL_MCP_DAEMON_LOG=str(tmp_path / "daemon.log"),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    process = subprocess.Popen([sys.executable, "-m", "serial_mcp_daemon"], cwd=ROOT, env=env,
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not (runtime / "daemon.sock").exists():
        if process.poll() is not None:
            raise AssertionError(process.stderr.read().decode())
        time.sleep(0.01)
    if not (runtime / "daemon.sock").exists():
        process.kill()
        raise AssertionError("daemon did not create its socket")
    yield runtime
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=10)


def connect_client(runtime, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return ipc.DaemonClient.connect(path=runtime / "daemon.sock", timeout=0.2)
        except ipc.DaemonUnavailable:
            time.sleep(0.01)
    raise AssertionError("daemon socket did not become ready")


def next_event_of(client, name, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        event = client.next_event(timeout=max(0.01, deadline - time.monotonic()))
        if event and event.get("event") == name:
            return event
    raise AssertionError(f"did not receive {name} event")


def open_pty():
    master, slave = pty.openpty()
    return master, os.ttyname(slave), slave


def next_device_state(client, state, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        event = client.next_event(timeout=max(0.01, deadline - time.monotonic()))
        if event and event.get("event") == "device" and event.get("state") == state:
            return event
    raise AssertionError(f"did not receive device state {state}")


def test_two_devices_and_busy_join_are_independent(devices_runtime, tmp_path):
    m1, p1, s1 = open_pty()
    m2, p2, s2 = open_pty()
    os.close(s1); os.close(s2)
    alias = str(tmp_path / "alias")
    os.symlink(p1, alias)
    a = connect_client(devices_runtime)
    b = connect_client(devices_runtime)
    try:
        a.hello("mcp", "a")
        b.hello("mcp", "b")
        first = a.call("connect", {"port": p1, "baudrate": 115200})
        busy = b.call("connect", {"port": alias, "baudrate": 57600})
        second = b.call("connect", {"port": p2, "baudrate": 38400})
        assert first["control"] == "granted" and first["baudrate"] == 115200
        assert busy["control"] == "not_granted" and busy["baudrate"] == 115200
        assert second["control"] == "granted" and second["baudrate"] == 38400
        assert len(a.status()["devices"]) == 2
        changed = a.call("set_baudrate", {"port": p1, "baudrate": 19200})
        assert changed["baudrate"] == 19200
        assert next_event_of(b, "settings")["baudrate"] == 19200
        os.write(m1, b"independent output")
        assert "independent output" in a.call("read_output", {"port": p1, "timeout": 2})
        assert "independent output" in b.call("read_output", {"port": alias, "timeout": 0})
        with pytest.raises(ipc.DaemonError) as err:
            b.call("send_command", {"port": alias, "text": "should not run"})
        assert err.value.code == "busy"
        a.call("disconnect", {"port": p1})
        assert len(b.status()["devices"]) == 2  # observer still keeps the first tty open
        with pytest.raises(ipc.DaemonError) as err:
            b.call("send_command", {"port": alias, "text": "no takeover"})
        assert err.value.code == "control_required"
    finally:
        a.close(); b.close()
        for fd in (m1, m2):
            os.close(fd)


def test_replug_reopens_the_existing_device_instead_of_reporting_a_stale_connection(
        devices_runtime, tmp_path):
    old_master, old_port, old_slave = open_pty()
    os.close(old_slave)
    alias = str(tmp_path / "device")
    os.symlink(old_port, alias)
    client = connect_client(devices_runtime)
    replacement_master = replacement_slave = None
    try:
        client.hello("mcp", "replug-test")
        first = client.call("connect", {"port": alias, "baudrate": 115200})
        device_id = first["device_id"]
        next_device_state(client, "open")
        next_device_state(client, "joined")
        os.close(old_master)
        old_master = -1
        replacement_master, replacement_port, replacement_slave = open_pty()
        os.close(replacement_slave)
        replacement_slave = None
        os.unlink(alias)
        os.symlink(replacement_port, alias)

        reopened = client.call("connect", {"port": alias, "baudrate": 57600})
        next_device_state(client, "disconnected")
        next_device_state(client, "open")
        next_device_state(client, "joined")
        assert reopened["device_id"] == device_id
        assert reopened["baudrate"] == 115200  # first-open baud remains authoritative
        assert reopened["control"] == "granted"
        os.write(replacement_master, b"after-replug")
        output = client.call("read_output", {"port": alias, "timeout": 2})
        assert "after-replug" in output
        devices = client.status()["devices"]
        assert len(devices) == 1
        assert devices[0]["state"] == "connected"
    finally:
        client.close()
        for fd in (old_master, replacement_master, replacement_slave):
            if fd is not None and fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass


def test_disconnect_succeeds_after_the_tty_path_disappears(devices_runtime, tmp_path):
    master, port, slave = open_pty()
    os.close(slave)
    alias = str(tmp_path / "device")
    os.symlink(port, alias)
    client = connect_client(devices_runtime)
    try:
        client.hello("mcp", "vanish-test")
        client.call("connect", {"port": alias, "baudrate": 115200})
        os.close(master)
        master = -1
        wait_until(lambda: client.status()["devices"][0]["state"] == "disconnected")
        result = client.call("disconnect", {"port": alias})
        assert result["result"] == "Disconnected."
        next_device_state(client, "left")
        next_device_state(client, "closed")
        assert client.status()["devices"] == []
    finally:
        client.close()
        if master >= 0:
            os.close(master)


def test_baud_change_during_active_command_is_allowed_and_warns(devices_runtime):
    master, port, slave = open_pty()
    os.close(slave)
    client = connect_client(devices_runtime)
    human = connect_client(devices_runtime)
    result = {}
    thread = None
    try:
        client.hello("mcp", "owner")
        client.call("connect", {"port": port, "baudrate": 115200})
        human.hello("human", "terminal")
        human.join(port)
        def command():
            try:
                result["value"] = client.call(
                    "send_command", {"port": port, "text": "PING", "timeout": 5,
                                     "idle_timeout": 4, "terminator_regex": "DONE"})
            except Exception as exc:
                result["error"] = exc
        thread = threading.Thread(target=command)
        thread.start()
        readable, _, _ = select.select([master], [], [], 3)
        assert readable, "command did not reach the fake tty"
        os.read(master, 1024)
        human.human_input(b"KEY", port)
        readable, _, _ = select.select([master], [], [], 1)
        assert readable and b"KEY" in os.read(master, 1024)
        changed = client.call("set_baudrate", {"port": port, "baudrate": 19200})
        assert changed["baudrate"] == 19200
        os.write(master, b"DONE\\n")
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert "error" not in result
        assert result["value"]["warning"].startswith("settings changed during")
        assert "DONE" in result["value"]["result"]
    finally:
        if thread is not None:
            thread.join(timeout=1)
        human.close()
        client.close()
        os.close(master)


def test_client_exit_releases_last_membership_and_closes_physical_device(devices_runtime):
    master, port, slave = open_pty()
    os.close(slave)
    owner = connect_client(devices_runtime)
    watcher = connect_client(devices_runtime)
    try:
        owner.hello("mcp", "owner")
        watcher.hello("mcp", "idle")
        owner.call("connect", {"port": port, "baudrate": 115200})
        assert len(watcher.status()["devices"]) == 1
        owner.close()  # abrupt socket loss uses the same cleanup path as process crash
        wait_until(lambda: watcher.status()["devices"] == [])
        reopened = watcher.call("connect", {"port": port, "baudrate": 19200})
        assert reopened["baudrate"] == 19200 and reopened["control"] == "granted"
        watcher.call("disconnect", {"port": port})
    finally:
        watcher.close()
        os.close(master)


def test_v1_python_mcp_frontend_fails_clearly_at_negotiation(devices_runtime):
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(devices_runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    process = subprocess.run([sys.executable, "-c",
        "from serial_mcp_client import FRONTEND; FRONTEND._open_session()"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
    assert process.returncode != 0
    assert b"speaks protocol 2" in process.stderr and b"frontend speaks 1" in process.stderr


def test_human_join_and_input_need_no_ai_control(devices_runtime):
    master, port, slave = open_pty()
    os.close(slave)
    human = connect_client(devices_runtime)
    try:
        human.hello("human", "terminal")
        joined = human.join(port, 9600)
        assert joined["baudrate"] == 9600 and joined["control"] == "available"
        result = human.human_input(b"x", port)
        assert result["written"] == 1
        assert os.read(master, 1) == b"x"
        human.leave(port)
        assert human.status()["devices"] == []
    finally:
        human.close()
        os.close(master)
