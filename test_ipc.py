"""Tests for the stdlib-only daemon IPC helpers (serial_mcp_ipc).

These exercise the frozen contract in docs/daemon-ipc.md at its public seams:
framing limits, version checks, and runtime-directory safety.
"""

import json
import os
import socket
import stat

import pytest

import serial_mcp_ipc as ipc


def _pair():
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    return a, b


def test_contract_constants_match_the_frozen_document():
    assert ipc.PROTOCOL_VERSION == 2
    assert ipc.MAX_FRAME_BYTES == 4 * 1024 * 1024
    assert ipc.HELLO_TIMEOUT_SECONDS == 5
    assert ipc.LEASE_IDLE_SECONDS == 60
    assert ipc.GRACE_SECONDS == 30
    assert ipc.DRAIN_TIMEOUT_SECONDS == 5
    assert ipc.START_TIMEOUT_SECONDS == 10
    assert ipc.SLOW_CONSUMER_TIMEOUT_SECONDS == 5
    assert ipc.MAX_SUBSCRIBER_QUEUE_BYTES == 256 * 1024
    assert ipc.MAX_OUTPUT_EVENT_BYTES == 16 * 1024
    assert ipc.MAX_INFLIGHT_CALLS_PER_CONNECTION == 8
    assert ipc.MAX_CLIENT_EVENT_QUEUE == 256
    assert ipc.MAX_HUMAN_INPUT_BYTES == 64 * 1024


def test_encode_frame_is_one_json_line_with_version():
    line = ipc.encode_frame({"id": 1, "op": "status"})
    assert line.endswith(b"\n") and line.count(b"\n") == 1
    assert json.loads(line) == {"v": 2, "id": 1, "op": "status"}


def test_reader_splits_lines_and_rejects_non_objects():
    a, b = _pair()
    try:
        a.sendall(ipc.encode_frame({"id": 1, "op": "status"}) + b'[1,2]\n')
        reader = ipc.FrameReader(b)
        assert reader.read_frame()["id"] == 1
        with pytest.raises(ipc.ProtocolError) as err:
            reader.read_frame()
        assert err.value.code == "bad_request"
    finally:
        a.close()
        b.close()


def test_reader_rejects_wrong_protocol_version():
    a, b = _pair()
    try:
        a.sendall(b'{"v":1,"id":1,"op":"status"}\n')
        with pytest.raises(ipc.ProtocolError) as err:
            ipc.FrameReader(b).read_frame()
        assert err.value.code == "unsupported_version"
    finally:
        a.close()
        b.close()


def test_reader_bounds_an_overlong_line_without_buffering_it():
    a, b = _pair()
    b.settimeout(5)
    try:
        # Send one byte more than the limit with no newline; the reader must stop
        # at the limit instead of growing an unbounded buffer.
        payload = b"x" * (ipc.MAX_FRAME_BYTES + 1)
        t = _start_sender(a, payload)
        with pytest.raises(ipc.ProtocolError) as err:
            ipc.FrameReader(b).read_frame()
        assert err.value.code == "frame_too_large"
        t.join(5)
    finally:
        a.close()
        b.close()


def _start_sender(sock, payload):
    import threading

    def run():
        try:
            sock.sendall(payload)
        except OSError:
            pass

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


def test_reader_reports_eof_as_none():
    a, b = _pair()
    try:
        a.close()
        assert ipc.FrameReader(b).read_frame() is None
    finally:
        b.close()


def test_runtime_dir_prefers_explicit_override(monkeypatch, tmp_path):
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(tmp_path / "rt"))
    assert ipc.runtime_dir() == tmp_path / "rt"
    assert ipc.socket_path() == tmp_path / "rt" / "daemon.sock"
    assert ipc.lock_path() == tmp_path / "rt" / "daemon.lock"


def test_runtime_dir_creation_is_private(monkeypatch, tmp_path):
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(tmp_path / "rt"))
    path = ipc.ensure_runtime_dir()
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o700
    assert os.stat(path).st_uid == os.geteuid()


def test_runtime_dir_refuses_group_or_world_access(monkeypatch, tmp_path):
    target = tmp_path / "rt"
    target.mkdir(mode=0o755)
    os.chmod(target, 0o755)
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(target))
    with pytest.raises(ipc.UnsafeRuntimeDir):
        ipc.ensure_runtime_dir()


def test_runtime_dir_refuses_a_symlink(monkeypatch, tmp_path):
    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(link))
    with pytest.raises(ipc.UnsafeRuntimeDir):
        ipc.ensure_runtime_dir()


def test_socket_path_length_is_checked(monkeypatch, tmp_path):
    long_dir = tmp_path / ("d" * 120)
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(long_dir))
    with pytest.raises(ipc.UnsafeRuntimeDir):
        ipc.socket_path()


def test_client_event_queue_is_bounded_and_reports_overflow():
    a, b = _pair()
    client = ipc.DaemonClient(a)
    try:
        for index in range(ipc.MAX_CLIENT_EVENT_QUEUE + 10):
            client._queue_event({"event": "settings", "device_id": "d1", "generation": index})
        assert client._events.qsize() <= ipc.MAX_CLIENT_EVENT_QUEUE
        events = []
        while (event := client.next_event(timeout=0)) is not None:
            events.append(event)
        assert any(event["event"] == "event_gap" for event in events)
        assert events[-1]["generation"] == ipc.MAX_CLIENT_EVENT_QUEUE + 9
    finally:
        client.close()
        b.close()


def test_client_names_an_unsupported_daemon_protocol_instead_of_a_silent_close():
    """A peer that answers in another protocol version must produce a clear error.

    The reader cannot parse the reply, so it closes the connection. The caller
    must see why (the version), not a generic "closed before a reply".
    """
    import threading

    a, b = _pair()
    b.settimeout(5)
    client = ipc.DaemonClient(a)

    def fake_daemon():
        b.recv(65536)  # the request line
        b.sendall(b'{"v":1,"id":1,"ok":true,"result":{}}\n')

    thread = threading.Thread(target=fake_daemon, daemon=True)
    thread.start()
    try:
        with pytest.raises(ipc.DaemonUnavailable, match="protocol version 1"):
            client.request("status", timeout=5)
    finally:
        thread.join(5)
        client.close()
        b.close()
