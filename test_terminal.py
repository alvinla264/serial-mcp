"""Public IPC/pty smoke coverage for the multi-device terminal list and exit."""
import base64
import os
import pty
import select
import signal
import socket
import subprocess
import sys
import termios
import threading
import tty
import time
from pathlib import Path

import pytest

import serial_mcp_daemon
import serial_mcp_engine
import serial_mcp_ipc as ipc
import serial_mcp_terminal as terminal

ROOT = Path(__file__).resolve().parent


@pytest.mark.parametrize("has_mcp_count", [True, False])
def test_terminal_requires_mcp_count_and_hides_client_identity(tmp_path, has_mcp_count):
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(runtime / "daemon.sock"))
    listener.listen(1)
    master, slave = pty.openpty()
    failures = []

    def serve():
        try:
            peer, _ = listener.accept()
            with peer:
                reader = ipc.FrameReader(peer)
                while request := reader.read_frame():
                    op = request["op"]
                    if op == "hello":
                        result = {"protocol": 2, "daemon_version": "test", "client_id": "private",
                                  "role": "human", "clients": 2, "devices": []}
                    elif op == "status":
                        result = {"protocol": 2, "clients": 2,
                                  "devices": [], "grace": None}
                        if has_mcp_count:
                            result["mcp_clients"] = 1
                    elif op == "ports":
                        result = {"result": "/dev/test-serial  synthetic port"}
                    else:
                        result = {}
                    peer.sendall(ipc.encode_frame({"id": request["id"], "ok": True, "result": result}))
        except Exception as exc:
            failures.append(exc)

    server = threading.Thread(target=serve, daemon=True)
    server.start()
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    try:
        process = subprocess.Popen([sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT,
                                   env=env, stdin=slave, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        terminal_output = bytearray()
        deadline = time.monotonic() + 5
        expected = (b"Ctrl-Q quits." if has_mcp_count
                    else b"does not provide MCP-only client counts")
        while expected not in terminal_output and time.monotonic() < deadline:
            readable, _, _ = select.select([process.stderr], [], [], 0.1)
            if readable:
                chunk = os.read(process.stderr.fileno(), 4096)
                if not chunk:
                    break
                terminal_output.extend(chunk)
        assert expected in terminal_output
        if has_mcp_count:
            os.write(master, b"q")
            stdout, stderr = process.communicate(timeout=10)
            terminal_output.extend(stderr)
            assert process.returncode == 0
            assert b"Registered MCP clients: 1" in terminal_output
            assert b"/dev/test-serial" in terminal_output
        else:
            stdout, stderr = process.communicate(timeout=10)
            terminal_output.extend(stderr)
            assert process.returncode == 1
        server.join(timeout=5)
        assert not server.is_alive()
        assert b"private" not in terminal_output
    finally:
        os.close(master)
        os.close(slave)
        listener.close()
        server.join(timeout=1)


def test_terminal_joins_real_isolated_daemon_and_leaves_without_transmitting_control(
    tmp_path, monkeypatch
):
    """Exercise the shipped TUI against a real daemon and a synthetic PTY device."""
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    device_master, device_slave = pty.openpty()
    port = os.ttyname(device_slave)
    os.close(device_slave)
    monkeypatch.setattr(
        serial_mcp_engine.serial.tools.list_ports,
        "comports",
        lambda: [type("Port", (), {"device": port, "description": "synthetic", "hwid": ""})()],
    )
    state = serial_mcp_engine.SerialState(log_path=str(tmp_path / "daemon.log"))
    daemon = serial_mcp_daemon.DaemonServer(runtime, state=state)
    daemon.start()
    human_master, human_slave = pty.openpty()
    initial_tty = termios.tcgetattr(human_slave)
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    process = subprocess.Popen(
        [sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT, env=env,
        stdin=human_slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    os.close(human_slave)
    output = bytearray()

    def wait_for(needle, timeout=5, start=0):
        deadline = time.monotonic() + timeout
        while needle not in output[start:] and time.monotonic() < deadline:
            ready, _, _ = select.select([process.stderr], [], [], 0.1)
            if ready:
                chunk = os.read(process.stderr.fileno(), 4096)
                if not chunk:
                    break
                output.extend(chunk)
        assert needle in output[start:], output.decode("utf-8", "replace")[-2000:]

    ai = ipc.DaemonClient.connect(runtime / "daemon.sock")
    try:
        ai.hello(role="mcp", name="synthetic-idle-client")
        wait_for(b"Registered MCP clients: 1")
        wait_for(port.encode())
        mark = len(output)
        os.write(human_master, b"\r")
        wait_for(b"Baud rate (e.g. 115200): ", start=mark)
        mark = len(output)
        os.write(human_master, b"0\r")
        wait_for(b"Invalid baud rate", start=mark)
        mark = len(output)
        wait_for(b"device list", start=mark)
        os.write(human_master, b"\r")
        mark = len(output)
        wait_for(b"Baud rate (e.g. 115200): ", start=mark)
        mark = len(output)
        os.write(human_master, b"not-a-number\r")
        wait_for(b"Invalid baud rate.", start=mark)
        mark = len(output)
        wait_for(b"device list", start=mark)
        os.write(human_master, b"\r")
        mark = len(output)
        wait_for(b"Baud rate (e.g. 115200): ", start=mark)
        os.write(human_master, b"115200\r")
        wait_for(b"Ctrl-] returns to device list", start=mark)
        os.write(device_master, b"synthetic-rx")
        received = bytearray()
        deadline = time.monotonic() + 5
        while b"synthetic-rx" not in received and time.monotonic() < deadline:
            ready, _, _ = select.select([process.stdout], [], [], 0.1)
            if ready:
                received.extend(os.read(process.stdout.fileno(), 4096))
        assert b"synthetic-rx" in received
        assert ai.status()["clients"] == 2
        mark = len(output)
        os.write(human_master, b"\x1d")
        wait_for(b"device list", timeout=5, start=mark)
        assert ai.status()["devices"] == []  # listing does not retain human membership
        idle_mcp = ipc.DaemonClient.connect(runtime / "daemon.sock")
        try:
            idle_mcp.hello(role="mcp", name="synthetic-second-idle")
            mark = len(output)
            wait_for(b"Registered MCP clients: 2", start=mark)
        finally:
            idle_mcp.close()
        mark = len(output)
        wait_for(b"Registered MCP clients: 1", start=mark)
        # Ctrl-] is a local navigation key and must not be written to the serial PTY.
        ready, _, _ = select.select([device_master], [], [], 0.2)
        if ready:
            try:
                received = os.read(device_master, 4096)
            except OSError:  # last participant left; the PTY slave is now closed
                received = b""
            assert b"\x1d" not in received
        assert process.poll() is None  # returning to the list retains app registration
        mark = len(output)
        granted = ai.call("connect", {"port": port, "baudrate": 115200})
        assert granted["control"] == "granted"
        wait_for(b"115200 baud", start=mark)
        mark = len(output)
        os.write(human_master, b"\r")
        wait_for(b"Ctrl-] returns to device list", start=mark)
        assert b"Baud rate (e.g. 115200): " not in output[mark:]
        operation_result = []
        operation = threading.Thread(
            target=lambda: operation_result.append(ai.call("send_command", {
                "port": port, "text": "active-operation", "timeout": 4, "idle_timeout": 0.5,
            }))
        )
        operation.start()
        wire = bytearray()
        deadline = time.monotonic() + 5
        while b"active-operation\n" not in wire and time.monotonic() < deadline:
            readable, _, _ = select.select([device_master], [], [], 0.1)
            if readable:
                wire.extend(os.read(device_master, 4096))
        assert b"active-operation\n" in wire
        os.write(human_master, b"human-during-operation")
        deadline = time.monotonic() + 5
        while b"human-during-operation" not in wire and time.monotonic() < deadline:
            readable, _, _ = select.select([device_master], [], [], 0.1)
            if readable:
                wire.extend(os.read(device_master, 4096))
        assert b"human-during-operation" in wire
        mark = len(output)
        changed = ai.call("set_baudrate", {"port": port, "baudrate": 57600})
        assert changed["baudrate"] == 57600
        wait_for(b"baud rate changed to 57600", start=mark)
        os.write(device_master, b"operation-complete")
        operation.join(timeout=5)
        assert not operation.is_alive()
        assert operation_result
        os.write(human_master, b"normal-tx")
        transmitted = bytearray()
        deadline = time.monotonic() + 5
        while b"normal-tx" not in transmitted and time.monotonic() < deadline:
            ready, _, _ = select.select([device_master], [], [], 0.1)
            if ready:
                try:
                    transmitted.extend(os.read(device_master, 4096))
                except OSError:
                    break
        assert b"normal-tx" in transmitted
        os.write(human_master, b"\x11")
        assert process.wait(timeout=5) == 0
        deadline = time.monotonic() + 5
        while ai.status()["clients"] != 1 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ai.status()["clients"] == 1
        assert [item["port"] for item in ai.status()["devices"]] == [port]
        ready, _, _ = select.select([device_master], [], [], 0.2)
        if ready:
            assert b"\x11" not in os.read(device_master, 4096)
        assert termios.tcgetattr(human_master) == initial_tty
        assert b"synthetic-idle-client" not in output
        assert b"synthetic-second-idle" not in output
    finally:
        ai.close()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        daemon.stop("test complete")
        os.close(human_master)
        os.close(device_master)


@pytest.mark.parametrize("shutdown", ["sigterm", "sigint", "daemon-loss", "terminal-output-error"])
def test_terminal_restores_tty_and_deregisters_on_signal_or_daemon_loss(tmp_path, monkeypatch, shutdown):
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    monkeypatch.setattr(serial_mcp_engine.serial.tools.list_ports, "comports", lambda: [])
    state = serial_mcp_engine.SerialState(log_path=str(tmp_path / "daemon.log"))
    daemon = serial_mcp_daemon.DaemonServer(runtime, state=state)
    daemon.start()
    human_master, human_slave = pty.openpty()
    initial_tty = termios.tcgetattr(human_slave)
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    process = subprocess.Popen(
        [sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT, env=env,
        stdin=human_slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    os.close(human_slave)
    output = bytearray()

    def wait_for(needle, timeout=5):
        deadline = time.monotonic() + timeout
        while needle not in output and time.monotonic() < deadline:
            readable, _, _ = select.select([process.stderr], [], [], 0.1)
            if readable:
                chunk = os.read(process.stderr.fileno(), 4096)
                if not chunk:
                    break
                output.extend(chunk)
        assert needle in output, output.decode("utf-8", "replace")[-2000:]

    ai = ipc.DaemonClient.connect(runtime / "daemon.sock")
    try:
        ai.hello(role="mcp", name="synthetic-idle-client")
        wait_for(b"Registered MCP clients: 1")
        if shutdown in ("sigterm", "sigint"):
            process.send_signal(signal.SIGTERM if shutdown == "sigterm" else signal.SIGINT)
            assert process.wait(timeout=5) == 130
            assert ai.status()["clients"] == 1
        elif shutdown == "daemon-loss":
            daemon.stop("test daemon loss")
            assert process.wait(timeout=5) == 1
        else:
            process.stderr.close()
            assert process.wait(timeout=5) != 0
            assert ai.status()["clients"] == 1
        assert termios.tcgetattr(human_master) == initial_tty
    finally:
        ai.close()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        if not daemon.stopped:
            daemon.stop("test complete")
        os.close(human_master)


def test_terminal_keeps_device_output_streams_independent(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    first_master, first_slave = pty.openpty()
    first_port = os.ttyname(first_slave)
    os.close(first_slave)
    second_master, second_slave = pty.openpty()
    second_port = os.ttyname(second_slave)
    os.close(second_slave)
    monkeypatch.setattr(
        serial_mcp_engine.serial.tools.list_ports,
        "comports",
        lambda: [
            type("Port", (), {"device": port, "description": "synthetic", "hwid": ""})()
            for port in (first_port, second_port)
        ],
    )
    state = serial_mcp_engine.SerialState(log_path=str(tmp_path / "daemon.log"))
    daemon = serial_mcp_daemon.DaemonServer(runtime, state=state)
    daemon.start()
    human_master, human_slave = pty.openpty()
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    process = subprocess.Popen(
        [sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT, env=env,
        stdin=human_slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    os.close(human_slave)
    output = bytearray()

    def wait_for(needle, timeout=5, start=0):
        deadline = time.monotonic() + timeout
        while needle not in output[start:] and time.monotonic() < deadline:
            readable, _, _ = select.select([process.stderr], [], [], 0.1)
            if readable:
                chunk = os.read(process.stderr.fileno(), 4096)
                if not chunk:
                    break
                output.extend(chunk)
        assert needle in output[start:], output.decode("utf-8", "replace")[-2000:]

    ai = ipc.DaemonClient.connect(runtime / "daemon.sock")
    try:
        ai.hello(role="mcp", name="synthetic-observer")
        assert ai.call("connect", {"port": first_port, "baudrate": 115200})["control"] == "granted"
        ai.subscribe(first_port)
        wait_for(b"Registered MCP clients: 1")
        wait_for(second_port.encode())
        mark = len(output)
        os.write(human_master, b"j")
        wait_for(f"> {second_port}:".encode(), start=mark)
        mark = len(output)
        os.write(human_master, b"\r")
        wait_for(b"Baud rate (e.g. 115200): ", start=mark)
        os.write(human_master, b"57600\r")
        wait_for(b"Ctrl-] returns to device list", start=mark)

        os.write(first_master, b"first-only-output")
        observed = bytearray()
        deadline = time.monotonic() + 5
        while b"first-only-output" not in observed and time.monotonic() < deadline:
            event = ai.next_event(timeout=0.1)
            if event and event.get("event") == "output" and event.get("port") == first_port:
                observed.extend(base64.b64decode(event["data"], validate=True))
        assert b"first-only-output" in observed
        readable, _, _ = select.select([process.stdout], [], [], 0.2)
        assert not readable  # the pane does not consume another observer's port stream

        os.write(second_master, b"selected-device-output")
        received = bytearray()
        deadline = time.monotonic() + 5
        while b"selected-device-output" not in received and time.monotonic() < deadline:
            readable, _, _ = select.select([process.stdout], [], [], 0.1)
            if readable:
                received.extend(os.read(process.stdout.fileno(), 4096))
        assert b"selected-device-output" in received
        assert b"first-only-output" not in received

        mark = len(output)
        os.write(human_master, b"\x1d")
        wait_for(b"device list", start=mark)
        status = ai.status()
        assert [item["port"] for item in status["devices"]] == [first_port]
        os.write(human_master, b"\x11")
        assert process.wait(timeout=5) == 0
        assert ai.status()["clients"] == 1
    finally:
        ai.close()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        if not daemon.stopped:
            daemon.stop("test complete")
        os.close(human_master)
        os.close(first_master)
        os.close(second_master)


def test_terminal_reports_unavailable_port_and_returns_to_the_list(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    missing_port = str(tmp_path / "disappeared-serial-port")
    monkeypatch.setattr(
        serial_mcp_engine.serial.tools.list_ports,
        "comports",
        lambda: [type("Port", (), {"device": missing_port, "description": "synthetic", "hwid": ""})()],
    )
    state = serial_mcp_engine.SerialState(log_path=str(tmp_path / "daemon.log"))
    daemon = serial_mcp_daemon.DaemonServer(runtime, state=state)
    daemon.start()
    human_master, human_slave = pty.openpty()
    initial_tty = termios.tcgetattr(human_slave)
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    process = subprocess.Popen(
        [sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT, env=env,
        stdin=human_slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    os.close(human_slave)
    output = bytearray()

    def wait_for(needle, timeout=5, start=0):
        deadline = time.monotonic() + timeout
        while needle not in output[start:] and time.monotonic() < deadline:
            readable, _, _ = select.select([process.stderr], [], [], 0.1)
            if readable:
                chunk = os.read(process.stderr.fileno(), 4096)
                if not chunk:
                    break
                output.extend(chunk)
        assert needle in output[start:], output.decode("utf-8", "replace")[-2000:]

    try:
        wait_for(b"Registered MCP clients: 0")
        wait_for(missing_port.encode())
        mark = len(output)
        os.write(human_master, b"\r")
        wait_for(b"Baud rate (e.g. 115200): ", start=mark)
        os.write(human_master, b"115200\r")
        wait_for(f"cannot join {missing_port}".encode(), start=mark)
        assert daemon.status()["devices"] == []
        assert process.poll() is None
        os.write(human_master, b"q")
        assert process.wait(timeout=5) == 0
        assert termios.tcgetattr(human_master) == initial_tty
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        daemon.stop("test complete")
        os.close(human_master)


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        ({"event": "event_gap", "dropped_events": 3,
          "reason": "client event queue overflow"}, b"3 events lost"),
        ({"event": "settings_gap", "port": "/dev/synthetic"},
         b"settings notices lost; current baud is 57600"),
    ],
)
def test_terminal_reports_dropped_event_and_settings_notices(event, expected):
    terminal_master, terminal_slave = pty.openpty()
    tty.setraw(terminal_slave)
    output_read, output_write = os.pipe()

    class Events:
        calls = 0

        def next_event(self, timeout=0):
            self.calls += 1
            if self.calls == 1:
                return event
            if self.calls == 2:
                os.write(terminal_master, b"\x1d")
            return None

        def status(self):
            return {"devices": [{"port": "/dev/synthetic", "baudrate": 57600}]}

    try:
        assert terminal._terminal_pane(Events(), "/dev/synthetic", 115200,
                                       terminal_slave, output_write) == 0
        diagnostics = os.read(output_read, 4096)
        assert expected in diagnostics
        if event["event"] == "event_gap":
            assert b"output or settings may be incomplete" in diagnostics
    finally:
        os.close(terminal_master)
        os.close(terminal_slave)
        os.close(output_read)
        os.close(output_write)


def test_terminal_slow_stdout_keeps_rss_bounded_and_reports_event_loss(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    device_master, device_slave = pty.openpty()
    port = os.ttyname(device_slave)
    os.close(device_slave)
    monkeypatch.setattr(
        serial_mcp_engine.serial.tools.list_ports,
        "comports",
        lambda: [type("Port", (), {"device": port, "description": "synthetic", "hwid": ""})()],
    )
    state = serial_mcp_engine.SerialState(log_path=str(tmp_path / "daemon.log"))
    daemon = serial_mcp_daemon.DaemonServer(runtime, state=state)
    daemon.start()
    human_master, human_slave = pty.openpty()
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    process = subprocess.Popen(
        [sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT, env=env,
        stdin=human_slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    os.close(human_slave)
    diagnostics = bytearray()

    def read_until(needle, timeout=10, start=0):
        deadline = time.monotonic() + timeout
        while needle not in diagnostics[start:] and time.monotonic() < deadline:
            readable, _, _ = select.select([process.stdout, process.stderr], [], [], 0.1)
            for stream in readable:
                chunk = os.read(stream.fileno(), 65536)
                if stream is process.stderr:
                    diagnostics.extend(chunk)
        assert needle in diagnostics[start:], diagnostics.decode("utf-8", "replace")[-2000:]

    producer = None
    try:
        read_until(b"Registered MCP clients: 0")
        read_until(port.encode())
        mark = len(diagnostics)
        os.write(human_master, b"\r")
        read_until(b"Baud rate (e.g. 115200): ", start=mark)
        os.write(human_master, b"115200\r")
        read_until(b"Ctrl-] returns to device list", start=mark)
        with open(f"/proc/{process.pid}/status", encoding="ascii") as status_file:
            baseline = next(int(line.split()[1]) for line in status_file if line.startswith("VmRSS:"))

        payload = b"x" * (4 * 1024 * 1024)
        write_errors = []
        def flood_device():
            offset = 0
            try:
                while offset < len(payload):
                    offset += os.write(device_master, payload[offset:offset + 8192])
            except OSError as exc:
                write_errors.append(exc)

        producer = threading.Thread(target=flood_device, daemon=True)
        producer.start()
        producer.join(timeout=15)
        assert not producer.is_alive(), "synthetic device flood did not drain"
        assert not write_errors, write_errors
        with open(f"/proc/{process.pid}/status", encoding="ascii") as status_file:
            current = next(int(line.split()[1]) for line in status_file if line.startswith("VmRSS:"))
        assert current - baseline < 64 * 1024  # bounded IPC event queue; under 64 MiB growth
        read_until(b"terminal event queue overflow", timeout=10)
        mark = len(diagnostics)
        os.write(human_master, b"\x1d")
        read_until(b"device list", timeout=5, start=mark)
        os.write(human_master, b"\x11")
        assert process.wait(timeout=5) == 0
        assert daemon.status()["clients"] == 0
        assert daemon.status()["devices"] == []
    finally:
        if producer is not None and producer.is_alive():
            os.close(device_master)
            producer.join(timeout=1)
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        daemon.stop("test complete")
        os.close(human_master)
        try:
            os.close(device_master)
        except OSError:
            pass
