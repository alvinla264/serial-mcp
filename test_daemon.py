"""Daemon tests through the public IPC contract (docs/daemon-ipc.md).

Every behaviour is exercised by synthetic clients speaking the frozen protocol
over a real UNIX socket to an in-process DaemonServer. Devices are ptys. The
daemon's lease and grace clocks are injected (FakeClock) and advanced
explicitly; no test depends on wall-clock sleeps.
"""

import base64
import os
import pty
import select
import socket
import threading
import time
from pathlib import Path

import pytest

import serial_mcp_ipc as ipc
import serial_mcp_daemon as daemon_mod
from serial_mcp_daemon import DaemonServer
from server import SerialState


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Harness:
    """One in-process daemon on a private runtime directory with a fake clock."""

    def __init__(self, tmp_path: Path) -> None:
        self.clock = FakeClock()
        self.runtime = tmp_path / "rt"
        self.state = SerialState(log_path=str(tmp_path / "daemon.log"))
        self.daemon = DaemonServer(
            runtime=self.runtime,
            clock=self.clock,
            state=self.state,
            start_housekeeping=False,
        )
        self.daemon.start()
        self.clients: list[ipc.DaemonClient] = []

    def client(self, role: str = "mcp", name: str = "test", pid: int | None = None) -> ipc.DaemonClient:
        c = ipc.DaemonClient.connect(path=self.runtime / "daemon.sock", timeout=5)
        self.clients.append(c)
        c.hello(role=role, name=name, pid=pid)
        return c

    def close(self) -> None:
        for c in self.clients:
            c.close()
        self.daemon.stop(reason="test")
        self.state.disconnect()


def _status_direct(harness):
    """Daemon-side status for lifecycle assertions (no lease effects)."""
    return harness.daemon.status()


def _wait_until(predicate, timeout=5.0):
    """Bounded poll for asynchronous reader-thread bookkeeping. Never a fixed sleep."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition not reached within the bounded wait")


def _next_event_of(client, name, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        event = client.next_event(timeout=0.2)
        if event is not None and event.get("event") == name:
            return event
    raise AssertionError(f"no {name!r} event within {timeout}s")


def _raw_connect(harness):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect(str(harness.runtime / "daemon.sock"))
    return sock


@pytest.fixture()
def harness(tmp_path):
    h = Harness(tmp_path)
    yield h
    h.close()


def _pty_device(handler):
    master, slave = pty.openpty()
    stop = threading.Event()

    def loop():
        try:
            handler(master, stop)
        finally:
            stop.set()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return master, slave, os.ttyname(slave), stop, thread


def _echo_upper(master, stop):
    while not stop.is_set():
        readable, _, _ = select.select([master], [], [], 0.05)
        if master in readable:
            data = os.read(master, 1024)
            os.write(master, data.upper())


def _release_device(master, slave, stop, thread):
    stop.set()
    thread.join(timeout=2)
    os.close(master)
    os.close(slave)


# ---- handshake and surface -------------------------------------------------


def test_hello_returns_identity_and_lease_state(harness):
    c = ipc.DaemonClient.connect(path=harness.runtime / "daemon.sock", timeout=5)
    harness.clients.append(c)
    result = c.hello(role="mcp", name="agent-a", pid=4242)
    assert result["protocol"] == 1
    assert result["role"] == "mcp"
    assert result["client_id"].startswith("c")
    assert result["lease"]["state"] == "free"
    assert result["lease"]["holder"] is None


def test_request_before_hello_is_rejected(harness):
    sock = _raw_connect(harness)
    try:
        sock.sendall(ipc.encode_frame({"id": 1, "op": "status"}))
        reply = ipc.FrameReader(sock).read_frame()
        assert reply["ok"] is False and reply["error"]["code"] == "hello_required"
    finally:
        sock.close()


def test_second_hello_and_unknown_op_are_rejected(harness):
    c = harness.client()
    with pytest.raises(ipc.DaemonError) as err:
        c.request("hello", role="mcp", name="again")
    assert err.value.code == "already_hello"
    with pytest.raises(ipc.DaemonError) as err:
        c.request("frobnicate")
    assert err.value.code == "unknown_op"


def test_unsupported_protocol_version_closes_the_connection(harness):
    sock = _raw_connect(harness)
    try:
        sock.sendall(b'{"v":9,"id":1,"op":"hello","role":"mcp","name":"x"}\n')
        reply = ipc.FrameReader(sock).read_frame()
        assert reply["error"]["code"] == "unsupported_version"
        assert ipc.FrameReader(sock).read_frame() is None
    finally:
        sock.close()


def test_overlong_frame_is_refused_and_closed(harness):
    sock = _raw_connect(harness)
    try:
        sock.sendall(b"x" * (ipc.MAX_FRAME_BYTES + 16))
        reply = ipc.FrameReader(sock).read_frame()
        assert reply["error"]["code"] == "frame_too_large"
    finally:
        sock.close()


def test_status_reports_clients_grace_and_no_device(harness):
    c = harness.client(name="observer")
    status = c.request("status")
    assert status["protocol"] == 1
    assert status["clients"] == 1
    assert status["grace"] == {"active": False, "remaining": None}
    assert status["device"] is None
    assert status["lease"]["state"] == "free"


# ---- tool dispatch -----------------------------------------------------------


def test_ungated_tool_returns_the_same_text_as_the_mcp_tool(harness):
    c = harness.client()
    text = c.call("file_transfer_capabilities")
    assert text.startswith("File transfer requires a serial connection")
    assert c.call("tio_info").startswith("Not connected")


def test_unknown_tool_and_bad_arguments_are_structured_errors(harness):
    c = harness.client()
    with pytest.raises(ipc.DaemonError) as err:
        c.call("no_such_tool")
    assert err.value.code == "unknown_tool"
    with pytest.raises(ipc.DaemonError) as err:
        c.call("view_io", {"lines": 3, "bogus": True})
    assert err.value.code == "bad_args"


def test_tool_error_strings_are_successful_results_like_the_mcp_tool(harness):
    c = harness.client()
    # With no device, send_command returns its ordinary error text, not a protocol error.
    text = c.call("send_command", {"text": "hi"})
    assert text == "Error: Not connected to a serial port."


def test_gated_tool_round_trip_through_a_fake_device(harness):
    master, slave, port, stop, thread = _pty_device(_echo_upper)
    try:
        c = harness.client()
        result = c.call("connect", {"port": port, "session_socket": ""})
        assert result.startswith("Connected")
        assert c.call("send_command", {"text": "ping", "timeout": 2, "idle_timeout": 0.2}).strip() == "PING"
        assert c.call("disconnect").startswith("Disconnected")
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


# ---- lifecycle: client connections keep the daemon alive ---------------------


def test_zero_clients_starts_grace_and_a_new_client_cancels_it(harness):
    first = harness.client(name="first")
    first.close()
    harness.clients.remove(first)
    # Wait for the daemon to notice the disconnect (bounded poll of its own status).
    _wait_until(lambda: _status_direct(harness)["clients"] == 0)
    status = _status_direct(harness)
    assert status["grace"]["active"] is True
    assert status["grace"]["remaining"] == pytest.approx(30.0)

    harness.clock.advance(20)
    harness.daemon.tick()
    assert not harness.daemon.stopped

    second = harness.client(name="second")
    assert _status_direct(harness)["grace"]["active"] is False
    harness.clock.advance(60)
    harness.daemon.tick()
    assert not harness.daemon.stopped, "a connected client must keep the daemon alive"
    second.close()
    harness.clients.remove(second)


def test_grace_expiry_stops_the_daemon_and_removes_the_socket(harness):
    c = harness.client()
    c.close()
    harness.clients.remove(c)
    _wait_until(lambda: _status_direct(harness)["clients"] == 0)
    harness.clock.advance(29.9)
    harness.daemon.tick()
    assert not harness.daemon.stopped
    harness.clock.advance(0.2)
    harness.daemon.tick()
    assert harness.daemon.stopped
    assert not os.path.lexists(harness.runtime / "daemon.sock")


def test_human_connection_keeps_the_daemon_alive_with_no_ai_client(harness):
    human = harness.client(role="human", name="terminal")
    harness.clock.advance(3600)
    harness.daemon.tick()
    assert not harness.daemon.stopped
    human.close()
    harness.clients.remove(human)
    _wait_until(lambda: _status_direct(harness)["clients"] == 0)
    harness.clock.advance(30)
    harness.daemon.tick()
    assert harness.daemon.stopped


def test_alive_but_unused_client_still_counts_as_connected(harness):
    idle = harness.client(name="idle")
    harness.clock.advance(10_000)
    harness.daemon.tick()
    assert not harness.daemon.stopped
    assert _status_direct(harness)["clients"] == 1
    idle.close()
    harness.clients.remove(idle)


def test_client_that_never_says_hello_does_not_count(harness):
    sock = _raw_connect(harness)
    try:
        _wait_until(lambda: len(harness.daemon._connections) == 1)
        # Handshake deadline is wall-clock; we only assert it is not counted as a client.
        assert _status_direct(harness)["clients"] == 0
    finally:
        sock.close()


# ---- control lease ---------------------------------------------------------


def test_first_gated_use_auto_acquires_and_status_is_inspectable(harness):
    a = harness.client(name="agent-a")
    assert a.lease("status")["state"] == "free"
    assert a.call("disconnect") == "No active connection."
    lease = a.lease("status")
    assert lease["holder"]["name"] == "agent-a"
    assert lease["state"] == "idle"
    assert lease["expires_in"] == pytest.approx(60.0)
    assert lease["you"] == "holder"


def test_conflicting_client_gets_busy_with_expiry_information(harness):
    a = harness.client(name="agent-a")
    b = harness.client(name="agent-b")
    a.call("disconnect")
    with pytest.raises(ipc.DaemonError) as err:
        b.call("disconnect")
    assert err.value.code == "busy"
    assert err.value.error["lease"]["holder"]["name"] == "agent-a"
    assert err.value.error["lease"]["expires_in"] == pytest.approx(60.0)
    assert b.lease("status")["you"] == "none"


def test_idle_expiry_after_sixty_seconds_frees_lease_and_warns_former_holder(harness):
    a = harness.client(name="agent-a")
    b = harness.client(name="agent-b")
    a.call("disconnect")
    harness.clock.advance(59.9)
    harness.daemon.tick()
    assert a.lease("status")["state"] == "idle"
    harness.clock.advance(0.2)
    harness.daemon.tick()

    event = _next_event_of(a, "lease_lost")
    assert event["reason"] == "expired"
    assert "device state may have changed" in event["warning"]
    assert b.lease("status")["state"] == "free"
    assert b.lease("status")["you"] == "none"


def test_former_holder_must_explicitly_reacquire_even_when_free(harness):
    a = harness.client(name="agent-a")
    a.call("disconnect")
    harness.clock.advance(61)
    harness.daemon.tick()
    with pytest.raises(ipc.DaemonError) as err:
        a.call("disconnect")
    assert err.value.code == "lease_lost"
    status = a.lease("status")
    assert status["you"] == "reacquire_required"

    granted = a.lease("acquire")
    assert granted["you"] == "holder"
    assert "device state may have changed" in granted["warning"]
    assert a.call("disconnect") == "No active connection."


def test_operations_renew_the_lease_only_when_they_finish(harness):
    a = harness.client(name="agent-a")
    a.call("disconnect")
    harness.clock.advance(50)
    a.call("disconnect")          # gated: renews at completion
    harness.clock.advance(50)
    harness.daemon.tick()
    assert a.lease("status")["holder"] is not None, "renewal at completion must restart the 60 s window"
    harness.clock.advance(11)
    harness.daemon.tick()
    assert a.lease("status")["state"] == "free"


def test_observation_and_read_only_tools_do_not_renew_control(harness):
    a = harness.client(name="agent-a")
    observer = harness.client(name="observer")
    a.call("disconnect")
    harness.clock.advance(45)
    observer.subscribe()
    observer.call("tio_info")
    observer.call("view_io", {"lines": 5})
    observer.lease("status")
    harness.clock.advance(20)  # 65 s since the last gated operation
    harness.daemon.tick()
    status = observer.lease("status")
    assert status["state"] == "free", "observation must not renew control"
    assert status["holder"] is None


def test_explicit_acquire_conflicts_and_renews_own_lease(harness):
    a = harness.client(name="agent-a")
    b = harness.client(name="agent-b")
    a.call("disconnect")
    with pytest.raises(ipc.DaemonError) as err:
        b.lease("acquire")
    assert err.value.code == "busy"
    harness.clock.advance(50)
    renewed = a.lease("acquire")
    assert renewed["holder"]["name"] == "agent-a"
    harness.clock.advance(50)
    harness.daemon.tick()
    assert a.lease("status")["holder"] is not None


def test_explicit_release_frees_the_lease_and_requires_reacquire(harness):
    a = harness.client(name="agent-a")
    b = harness.client(name="agent-b")
    a.call("disconnect")
    released = a.lease("release")
    assert released["state"] == "free"
    assert b.call("disconnect") == "No active connection."
    # b now holds the lease, so a is told the device is busy; it must reacquire later.
    with pytest.raises(ipc.DaemonError) as err:
        a.call("disconnect")
    assert err.value.code == "busy"
    assert err.value.error["lease"]["holder"]["name"] == "agent-b"
    assert a.lease("status")["you"] == "reacquire_required"


# ---- independent output and ungated human input ------------------------------


def _recording_upper_device():
    """Echo upper-case input and record every byte the device received."""
    received = bytearray()
    lock = threading.Lock()

    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data = os.read(master, 1024)
                with lock:
                    received.extend(data)
                os.write(master, data.upper())

    return handler, received, lock


def _collect_output(client, want: bytes, timeout=5.0) -> bytes:
    got = bytearray()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and want not in got:
        event = client.next_event(timeout=0.2)
        if event and event.get("event") == "output":
            got.extend(base64.b64decode(event["data"]))
    return bytes(got)


def test_observer_sees_output_without_consuming_the_controllers_unread_output(harness):
    handler, _received, _lock = _recording_upper_device()
    master, slave, port, stop, thread = _pty_device(handler)
    try:
        controller = harness.client(name="controller")
        observer = harness.client(name="observer")
        assert controller.call("connect", {"port": port, "session_socket": ""}).startswith("Connected")
        observer.subscribe()

        human = harness.client(role="human", name="terminal")
        human.human_input(b"beta")

        assert b"BETA" in _collect_output(observer, b"BETA")
        # The controller's unread output is still there for read_output.
        assert controller.call("read_output", {"timeout": 2, "idle_timeout": 0.2}).strip() == "BETA"
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


def test_human_input_is_not_gated_and_does_not_wait_for_an_ai_operation(harness):
    handler, received, lock = _recording_upper_device()
    master, slave, port, stop, thread = _pty_device(handler)
    try:
        controller = harness.client(name="controller")
        human = harness.client(role="human", name="terminal")
        controller.call("connect", {"port": port, "session_socket": ""})

        # A gated operation that stays in flight (device output is not expected).
        result = {}
        worker = threading.Thread(
            target=lambda: result.setdefault("text", controller.call(
                "send_command", {"text": "slow", "timeout": 3, "idle_timeout": 0.2, "line_ending": "none"})),
            daemon=True,
        )
        worker.start()
        _wait_until(lambda: _status_direct(harness)["lease"]["state"] == "active")

        written = human.human_input(b"HUMAN")
        assert written == {"written": 5}
        assert worker.is_alive(), "the human write must not have waited for the AI operation to finish"
        _wait_until(lambda: b"HUMAN" in bytes(received))
        worker.join(timeout=10)
        assert not worker.is_alive()
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


def test_human_input_rules_forbidden_role_not_connected_and_size(harness):
    mcp = harness.client(role="mcp", name="agent")
    with pytest.raises(ipc.DaemonError) as err:
        mcp.request("input", data=base64.b64encode(b"x").decode())
    assert err.value.code == "forbidden_role"

    human = harness.client(role="human", name="terminal")
    with pytest.raises(ipc.DaemonError) as err:
        human.human_input(b"x")
    assert err.value.code == "not_connected"

    with pytest.raises(ipc.DaemonError) as err:
        human.human_input(b"x" * (ipc.MAX_HUMAN_INPUT_BYTES + 1))
    assert err.value.code == "too_large"


def test_device_disconnect_releases_the_port_without_stopping_the_daemon(harness):
    handler, _received, _lock = _recording_upper_device()
    master, slave, port, stop, thread = _pty_device(handler)
    try:
        controller = harness.client(name="controller")
        assert controller.call("connect", {"port": port, "session_socket": ""}).startswith("Connected")
        assert controller.call("disconnect").startswith("Disconnected")
        assert harness.state.transport is None
        assert not harness.daemon.stopped
        assert controller.call("connect", {"port": port, "session_socket": ""}).startswith("Connected")
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


def test_client_exit_during_an_operation_keeps_control_closing_until_it_settles(harness):
    handler, _received, _lock = _recording_upper_device()
    master, slave, port, stop, thread = _pty_device(handler)
    try:
        agent = harness.client(name="agent")
        other = harness.client(name="other")
        agent.call("connect", {"port": port, "session_socket": ""})
        agent.call("disconnect")  # ensure the lease is idle first
        agent.call("connect", {"port": port, "session_socket": ""})

        def long_op():
            try:
                agent.call("send_command", {"text": "wait", "timeout": 2, "idle_timeout": 0.2, "line_ending": "none"})
            except Exception:
                pass

        worker = threading.Thread(target=long_op, daemon=True)
        worker.start()
        _wait_until(lambda: _status_direct(harness)["lease"]["state"] == "active")
        agent.close()
        harness.clients.remove(agent)
        _wait_until(lambda: _status_direct(harness)["lease"]["state"] == "closing")
        with pytest.raises(ipc.DaemonError) as err:
            other.call("disconnect")
        assert err.value.code == "busy"
        assert err.value.error["lease"]["state"] == "closing"

        worker.join(timeout=10)
        _wait_until(lambda: _status_direct(harness)["lease"]["state"] == "free")
        assert other.call("disconnect").startswith("Disconnected")
        assert other.lease("status")["holder"]["name"] == "other"
        assert not harness.daemon.stopped
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


def test_bounded_subscriber_queue_never_blocks_the_device_reader(harness):
    healthy = harness.client(name="healthy")
    healthy.subscribe()
    stalled = _raw_connect(harness)
    try:
        stalled_hello = ipc.encode_frame({"id": 1, "op": "hello", "role": "mcp", "name": "stalled"})
        stalled.sendall(stalled_hello)
        assert ipc.FrameReader(stalled).read_frame()["ok"] is True
        stalled.sendall(ipc.encode_frame({"id": 2, "op": "subscribe"}))
        assert ipc.FrameReader(stalled).read_frame()["ok"] is True

        fed = 0
        chunk = b"z" * 65536
        started = time.monotonic()
        for _ in range(64):  # 4 MiB of device output, far past the 256 KiB queue
            for listener in list(harness.state.rx_listeners):
                listener(chunk)
            fed += len(chunk)
        assert time.monotonic() - started < 5, "the device reader must not wait on a subscriber"

        received = 0
        gap = 0
        deadline = time.monotonic() + 10
        while received + gap < fed and time.monotonic() < deadline:
            event = healthy.next_event(timeout=0.2)
            if event is None:
                continue
            if event.get("event") == "output":
                received += len(base64.b64decode(event["data"]))
            elif event.get("event") == "output_gap":
                gap += event["dropped_bytes"]
        assert received + gap == fed, "every byte is either delivered or reported as a gap"
    finally:
        stalled.close()
        healthy.close()
        harness.clients.remove(healthy)


def test_shutdown_drains_in_flight_work_then_releases_the_device(harness):
    handler, _received, _lock = _recording_upper_device()
    master, slave, port, stop, thread = _pty_device(handler)
    try:
        agent = harness.client(name="agent")
        agent.call("connect", {"port": port, "session_socket": ""})
        result = {}
        worker = threading.Thread(
            target=lambda: result.setdefault("text", agent.call(
                "send_command", {"text": "wait", "timeout": 1, "idle_timeout": 0.2, "line_ending": "none"})),
            daemon=True,
        )
        worker.start()
        _wait_until(lambda: _status_direct(harness)["lease"]["state"] == "active")
        harness.daemon.stop(reason="test shutdown")
        worker.join(timeout=10)
        assert "text" in result, "the in-flight operation finished before the device was released"
        assert harness.state.transport is None
        assert harness.daemon.stopped
        assert not os.path.lexists(harness.runtime / "daemon.sock")
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


# ---- call cap, slow consumers, external owner -------------------------------


def test_per_connection_call_cap_returns_overloaded_for_the_extra_call(harness):
    raw = _raw_connect(harness)
    try:
        raw.sendall(ipc.encode_frame({"id": 1, "op": "hello", "role": "mcp", "name": "burst"}))
        reader = ipc.FrameReader(raw)
        assert reader.read_frame()["ok"] is True
        # read_output with a positive timeout keeps each call in flight without a device.
        count = ipc.MAX_INFLIGHT_CALLS_PER_CONNECTION + 1
        for rid in range(100, 100 + count):
            raw.sendall(ipc.encode_frame({"id": rid, "op": "call", "tool": "read_output",
                                           "args": {"timeout": 1.5, "idle_timeout": 0.2}}))
        replies = [reader.read_frame() for _ in range(count)]
        errors = [r["error"]["code"] for r in replies if not r.get("ok")]
        assert errors == ["overloaded"]
    finally:
        raw.close()


def test_stalled_subscriber_is_dropped_and_healthy_clients_are_unaffected(harness, monkeypatch):
    monkeypatch.setattr(daemon_mod, "SLOW_CONSUMER_TIMEOUT_SECONDS", 0.3)
    healthy = harness.client(name="healthy")
    healthy.subscribe()
    stalled = _raw_connect(harness)
    try:
        stalled.sendall(ipc.encode_frame({"id": 1, "op": "hello", "role": "mcp", "name": "stalled"}))
        assert ipc.FrameReader(stalled).read_frame()["ok"] is True
        stalled.sendall(ipc.encode_frame({"id": 2, "op": "subscribe"}))
        assert ipc.FrameReader(stalled).read_frame()["ok"] is True
        assert _status_direct(harness)["clients"] == 2

        chunk = b"q" * 65536
        for _ in range(64):  # overflow the kernel socket buffer of the stalled peer
            for listener in list(harness.state.rx_listeners):
                listener(chunk)
        _wait_until(lambda: _status_direct(harness)["clients"] == 1, timeout=10)
        assert healthy.status()["clients"] == 1
    finally:
        stalled.close()
        healthy.close()
        harness.clients.remove(healthy)


def test_external_tio_style_owner_can_be_attached_and_detached_via_call(harness, tmp_path):
    from test_server import FakeSessionOwner

    owner_path = str(tmp_path / "owner.sock")
    owner = FakeSessionOwner(owner_path)
    try:
        agent = harness.client(name="agent")
        assert agent.call("attach", {"attach_socket": owner_path}).startswith("Attached")
        assert "Attached to the session" in agent.call("tio_info")
        assert agent.call("disconnect").startswith("Detached")
        assert owner.alive
        assert not harness.daemon.stopped
    finally:
        owner.close()


def test_malformed_frame_gets_bad_request_and_the_connection_stays_open(harness):
    sock = _raw_connect(harness)
    try:
        sock.sendall(b"this is not json\n")
        reply = ipc.FrameReader(sock).read_frame()
        assert reply["ok"] is False and reply["error"]["code"] == "bad_request"
        sock.sendall(ipc.encode_frame({"id": 1, "op": "hello", "role": "mcp", "name": "after-garbage"}))
        assert ipc.FrameReader(sock).read_frame()["ok"] is True
    finally:
        sock.close()


def test_bye_replies_then_closes_and_releases_the_client(harness):
    raw = _raw_connect(harness)
    try:
        raw.sendall(ipc.encode_frame({"id": 1, "op": "hello", "role": "mcp", "name": "polite"}))
        reader = ipc.FrameReader(raw)
        assert reader.read_frame()["ok"] is True
        assert _status_direct(harness)["clients"] == 1
        raw.sendall(ipc.encode_frame({"id": 2, "op": "bye"}))
        assert reader.read_frame() == {"v": 1, "id": 2, "ok": True, "result": {}}
        assert reader.read_frame() is None
        _wait_until(lambda: _status_direct(harness)["clients"] == 0)
    finally:
        raw.close()


def test_unsubscribe_stops_output_events(harness):
    handler, _received, _lock = _recording_upper_device()
    master, slave, port, stop, thread = _pty_device(handler)
    try:
        controller = harness.client(name="controller")
        observer = harness.client(name="observer")
        controller.call("connect", {"port": port, "session_socket": ""})
        observer.subscribe()
        assert observer.unsubscribe() == {"subscribed": False}

        harness.client(role="human", name="terminal").human_input(b"gamma")
        # The controller still receives the device output: it is an unread buffer read.
        assert controller.call("read_output", {"timeout": 2, "idle_timeout": 0.2}).strip() == "GAMMA"

        outputs = []
        while True:
            event = observer.next_event(timeout=0.3)
            if event is None:
                break
            if event.get("event") == "output":
                outputs.append(event)
        assert outputs == [], "an unsubscribed client receives no output events"
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


def test_device_event_reaches_every_client_on_connect_and_disconnect(harness):
    handler, _received, _lock = _recording_upper_device()
    master, slave, port, stop, thread = _pty_device(handler)
    try:
        controller = harness.client(name="controller")
        watcher = harness.client(name="watcher")
        controller.call("connect", {"port": port, "session_socket": ""})
        event = _next_event_of(watcher, "device")
        assert event["state"] == "connected" and event["label"].startswith(port)
        controller.call("disconnect")
        event = _next_event_of(watcher, "device")
        assert event["state"] == "disconnected" and event["label"] is None
    finally:
        harness.state.disconnect()
        _release_device(master, slave, stop, thread)


def test_results_up_to_the_frame_limit_are_delivered_and_larger_ones_refused(harness):
    c = harness.client()
    harness.state.buffer = b"a" * (3 * 1024 * 1024 + 512 * 1024)  # 3.5 MiB: fits one 4 MiB frame
    assert len(c.call("read_output", {"timeout": 0})) == 3 * 1024 * 1024 + 512 * 1024

    harness.state.buffer = b"a" * (5 * 1024 * 1024)  # 5 MiB: cannot be framed
    with pytest.raises(ipc.DaemonError) as err:
        c.call("read_output", {"timeout": 0})
    assert err.value.code == "result_too_large"
