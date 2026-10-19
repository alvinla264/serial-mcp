import json
import os
import errno
import base64
import re
import pty
import shlex
import select
import tty
import socket
import subprocess
import sys
import tempfile

import pytest
import threading
import time
import asyncio
from pathlib import Path

import server
from fastmcp import Client
from server import state
from transfer_test_support import (
    InteractiveShellTioOwner, RealShellDevice, UBootPeer, build_target_bin,
)


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


def _cleanup(master, slave, stop, thread):
    state.disconnect()
    stop.set()
    thread.join(timeout=1)
    os.close(master)
    os.close(slave)


def _echo_upper_handler(master, stop):
    while not stop.is_set():
        readable, _, _ = select.select([master], [], [], 0.05)
        if master in readable:
            data = os.read(master, 1024)
            os.write(master, data.upper())


def _rx_log_lines():
    return [line for line in Path(state.log_path).read_text(encoding="utf-8").splitlines() if "[RX]" in line]


def _log_lines():
    return Path(state.log_path).read_text(encoding="utf-8").splitlines()


def _session_path(port):
    return "/tmp/serial-mcp-" + os.path.basename(port) + ".sock"


def _connect(port, **kwargs):
    result = state.connect(port, **kwargs)
    assert result.startswith("Connected"), result
    return result


def test_connect_disconnect_and_session_socket_lifecycle():
    def handler(master, stop):
        while not stop.is_set():
            select.select([master], [], [], 0.05)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        result = _connect(port)
        expected_sock = _session_path(port)
        expected_tty = expected_sock[: -len(".sock")]
        assert expected_sock in result
        assert f"tio {expected_tty}" in result
        assert state.ser is not None and state.ser.is_open
        assert os.path.islink(expected_tty) and os.path.exists(expected_tty)

        # Raw tio protocol: connectable, no handshake
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(expected_sock)
        time.sleep(0.1)
        info = state.tio_info()
        assert expected_sock in info and "1 client(s)" in info
        client.close()
        time.sleep(0.1)

        assert state.disconnect() == "Disconnected."
        assert not os.path.exists(expected_sock)
        assert not os.path.exists(expected_tty)
    finally:
        _cleanup(master, slave, stop, thread)
        if os.path.exists(_session_path(port)):
            os.unlink(_session_path(port))


def test_tio_pty_attach_user_typing_reaches_device_and_rx_flows_back():
    received = []

    def handler(master, stop):
        # Single reader: log everything the device sees, echo a marker on match
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data = os.read(master, 1024)
                received.append(data)
                if b"from-tio" in data:
                    os.write(master, b"DEVICE-SAW-IT")
                if b"ping-device" in data:
                    os.write(master, b"device-says-hi")

    fake_master, fake_slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        tty_path = _session_path(port)[: -len(".sock")]

        # Attach like real tio would: open the exposed pty slave
        tio_fd = os.open(tty_path, os.O_RDWR)
        try:
            # User typing in tio must reach the device...
            os.write(tio_fd, b"from-tio\n")
            deadline = time.monotonic() + 2.0
            while not any(b"from-tio" in d for d in received) and time.monotonic() < deadline:
                time.sleep(0.05)
            assert any(b"from-tio" in d for d in received)

            # ...triggering the device reply, which must flow back to tio
            deadline = time.monotonic() + 2.0
            got = b""
            while b"DEVICE-SAW-IT" not in got and time.monotonic() < deadline:
                readable, _, _ = select.select([tio_fd], [], [], 0.1)
                if tio_fd in readable:
                    got += os.read(tio_fd, 1024)
            assert b"DEVICE-SAW-IT" in got

            # Second round trip with a different payload
            os.write(tio_fd, b"ping-device\n")
            deadline = time.monotonic() + 2.0
            back = b""
            while b"device-says-hi" not in back and time.monotonic() < deadline:
                readable, _, _ = select.select([tio_fd], [], [], 0.1)
                if tio_fd in readable:
                    back += os.read(tio_fd, 1024)
            assert b"device-says-hi" in back

            text = state.view_io(lines=10)
            assert "[TX:user] from-tio" in text
            assert "[RX] device-says-hi" in text
        finally:
            os.close(tio_fd)
        time.sleep(0.3)  # pty reader recovers from EIO after detach
    finally:
        _cleanup(fake_master, fake_slave, stop, thread)
        if os.path.exists(_session_path(port)):
            os.unlink(_session_path(port))


def test_send_with_line_ending_and_idle_completion():
    seen = []

    def handler(master, stop):
        data = b""
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data += os.read(master, 1024)
                if data.endswith(b"\r\n"):
                    seen.append(data)
                    os.write(master, b"ack-1")
                    time.sleep(0.03)
                    os.write(master, b"-done")
                    return

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        out = state.send_command("ping", line_ending="crlf", timeout=1.0, idle_timeout=0.05)
        assert out == "ack-1-done"
        assert seen == [b"ping\r\n"]
    finally:
        _cleanup(master, slave, stop, thread)


def test_terminator_regex_completion_does_not_wait_full_timeout():
    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                os.read(master, 1024)
                os.write(master, b"part DONE more-later")
                time.sleep(0.3)
                os.write(master, b"ignored")
                return

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        start = time.monotonic()
        out = state.send_command("go", timeout=2.0, idle_timeout=1.0, terminator_regex="DONE")
        elapsed = time.monotonic() - start
        assert "DONE" in out
        assert "ignored" not in out
        assert elapsed < 1.0
    finally:
        _cleanup(master, slave, stop, thread)


def test_cap_lines_keeps_the_tail_and_marks_the_elision():
    cap = state._cap_lines
    # 0 means unlimited: callers that do not ask for a cap keep old behaviour.
    assert cap("a\nb\nc\n", 0) == "a\nb\nc\n"
    assert cap("a\nb\n", -1) == "a\nb\n"
    # Under the limit nothing is touched.
    assert cap("a\nb\n", 5) == "a\nb\n"
    # The *last* lines survive: that is where the prompt/result lives.
    assert cap("a\nb\nc\n", 2) == "[... 1 earlier line omitted ...]\nb\nc\n"
    assert cap("a\nb\nc\nd\n", 2) == "[... 2 earlier lines omitted ...]\nc\nd\n"
    # A final line without a newline must not be lost or double-counted.
    assert cap("a\nb\nc", 2) == "[... 1 earlier line omitted ...]\nb\nc"
    assert cap("a\nb", 2) == "a\nb"
    assert cap("", 3) == ""


def test_send_command_max_lines_caps_console_noise():
    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                os.read(master, 1024)
                # A realistic burst: noise first, the useful line last.
                os.write(master, b"noise-1\nnoise-2\nnoise-3\nRESULT=42\n")
                return

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        out = state.send_command("go", timeout=1.0, idle_timeout=0.05, max_lines=2)
        assert "RESULT=42" in out
        assert "earlier lines omitted" in out
        assert "noise-1" not in out
        assert "noise-2" not in out
    finally:
        _cleanup(master, slave, stop, thread)


def test_send_command_without_max_lines_still_returns_everything():
    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                os.read(master, 1024)
                os.write(master, b"noise-1\nnoise-2\nnoise-3\nRESULT=42\n")
                return

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        out = state.send_command("go", timeout=1.0, idle_timeout=0.05)
        assert out == "noise-1\nnoise-2\nnoise-3\nRESULT=42\n"
    finally:
        _cleanup(master, slave, stop, thread)


def test_read_output_max_lines_caps_the_drained_buffer():
    def handler(master, stop):
        while not stop.is_set():
            time.sleep(0.05)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        # Unique marker: the RX log is a shared module-level file that other tests
        # also scan, so reusing a common prompt string would collide with them.
        os.write(master, b"boot line\nmore noise\nLINECAP-MARKER> ")
        time.sleep(0.05)
        out = state.read_output(timeout=1.0, idle_timeout=0.05, max_lines=1)
        # The trailing prompt survives; the preceding noise is summarised away.
        assert "LINECAP-MARKER> " in out
        assert "boot line" not in out
        assert "earlier lines omitted" in out
    finally:
        _cleanup(master, slave, stop, thread)


def test_max_lines_is_exposed_on_both_mcp_tools():
    """The cap must be visible to the AI on send_command and read_output."""
    if hasattr(server.mcp, "get_tools"):
        tools = asyncio.run(server.mcp.get_tools())
    else:
        tools = asyncio.run(server.mcp.list_tools())
    by_name = {t.name: t for t in tools} if not isinstance(tools, dict) else tools

    for name in ("send_command", "read_output"):
        tool = by_name[name]
        schema = getattr(tool, "parameters", None) or getattr(tool, "inputSchema", {})
        props = schema.get("properties", {})
        assert "max_lines" in props, f"{name} does not expose max_lines"
        assert props["max_lines"].get("default") == 0, (
            f"{name}.max_lines must default to 0 (unlimited)"
        )


def test_async_read_output_and_clear_output():
    def handler(master, stop):
        while not stop.is_set():
            time.sleep(0.05)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        # pyserial flushes pending pty input on open, so write after connect
        os.write(master, b"boot")
        time.sleep(0.03)
        os.write(master, b" message")
        out = state.read_output(timeout=1.0, idle_timeout=0.05)
        assert out in {"boot message", " message"}  # chunk split may idle-split the read
        os.write(master, b"discard me")
        time.sleep(0.05)
        assert "Cleared" in state.clear_output()
        assert state.read_output() == ""
    finally:
        _cleanup(master, slave, stop, thread)


def test_no_direct_attach_tools_exposed():
    expected = {
        "connect",
        "disconnect",
        "send_command",
        "read_output",
        "clear_output",
        "set_prompt_pattern",
        "view_io",
        "list_serial_ports",
        "tio_info",
        "list_boot_profiles",
        "set_boot_credentials",
        "enter_bootloader",
        "login",
        "attach",
        "upload_file",
        "download_file",
        "file_transfer_capabilities",
    }
    forbidden = {"connect_via_tio", "logout", "enter_cli_mode", "check_mode"}
    if hasattr(server.mcp, "get_tools"):
        actual = set(asyncio.run(server.mcp.get_tools()).keys())  # fastmcp 2.x
    else:
        actual = {t.name for t in asyncio.run(server.mcp.list_tools())}  # fastmcp 3.x+

    assert actual == expected
    assert forbidden.isdisjoint(actual)


def test_invalid_prompt_regex_does_not_open_connection():
    def handler(master, stop):
        while not stop.is_set():
            select.select([master], [], [], 0.05)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        result = state.connect(port, prompt_regex="[")
        assert result.startswith("Error connecting")
        assert state.ser is None or not state.ser.is_open
        assert state.reader_thread is None
    finally:
        _cleanup(master, slave, stop, thread)


def test_view_io_records_ai_and_user_tx_and_rx():
    master, slave, port, stop, thread = _pty_device(_echo_upper_handler)
    try:
        _connect(port)
        state.send_command("ping", timeout=1.0, idle_timeout=0.05)

        # Attach a session user and have them type; device echo makes it visible
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(_session_path(port))
        time.sleep(0.1)
        client.sendall(b"hello\n")
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if any(d == "TX" and source == "user" and b"hello" in data for _, d, source, data in state.io_history):
                break
            time.sleep(0.01)

        text = state.view_io(lines=10)
        assert "[TX:ai] ping" in text
        assert "[RX] PING" in text
        assert "[TX:user] hello" in text

        hexed = state.view_io(lines=10, direction="tx", hex_mode=True)
        assert "70 69 6e 67" in hexed  # "ping"
        assert "[RX]" not in hexed
        assert state.view_io(direction="bogus").startswith("Error:")
        client.close()
    finally:
        _cleanup(master, slave, stop, thread)


def test_session_user_sees_device_rx_and_their_typing_reaches_device():
    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data = os.read(master, 1024)
                if b"hello" in data:
                    os.write(master, b"HI-FROM-DEVICE")

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(_session_path(port))
        client.settimeout(2.0)
        client.sendall(b"hello\n")

        # Device reply must be broadcast to the session client (raw bytes)
        buf = b""
        deadline = time.monotonic() + 2.0
        while b"HI-FROM-DEVICE" not in buf and time.monotonic() < deadline:
            try:
                chunk = client.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                break
            buf += chunk
        assert b"HI-FROM-DEVICE" in buf
        client.close()
    finally:
        _cleanup(master, slave, stop, thread)


def test_session_broadcasts_to_multiple_clients():
    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                os.read(master, 1024)
                os.write(master, b"news-flash")

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        c1 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        c2 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        c1.connect(_session_path(port))
        c2.connect(_session_path(port))
        time.sleep(0.2)
        os.write(master, b"news-flash")

        received = {c1: b"", c2: b""}
        for c in (c1, c2):
            c.settimeout(2.0)
            deadline = time.monotonic() + 2.0
            while b"news-flash" not in received[c] and time.monotonic() < deadline:
                try:
                    chunk = c.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                received[c] += chunk
            assert b"news-flash" in received[c]
        c1.close()
        c2.close()
    finally:
        _cleanup(master, slave, stop, thread)


def test_tio_info_when_not_connected():
    state.disconnect()
    assert state.tio_info().startswith("Not connected")


def test_ai_reconnect_keeps_session_and_attached_client():
    """The user attaches first; a later AI connect must not drop them."""

    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                os.read(master, 1024)
                os.write(master, b"tick")

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(_session_path(port))
        client.settimeout(2.0)
        time.sleep(0.1)

        # AI calls connect again (same device, same session path).
        result = state.connect(port)
        assert result.startswith("Connected"), result
        assert os.path.exists(_session_path(port))

        # The attach survived: typing still reaches the device and RX still
        # flows back to this client.
        client.sendall(b"x")
        buf = b""
        deadline = time.monotonic() + 2.0
        while b"tick" not in buf and time.monotonic() < deadline:
            try:
                chunk = client.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                break
            buf += chunk
        assert b"tick" in buf
        client.close()
    finally:
        _cleanup(master, slave, stop, thread)
        if os.path.exists(_session_path(port)):
            os.unlink(_session_path(port))


def test_reader_eio_on_active_connection_warns_once_and_marks_unavailable():
    class _EIOSerial:
        is_open = True

        @property
        def in_waiting(self):
            raise OSError(5, "Input/output error")

        def close(self):
            self.is_open = False

    state.disconnect()
    state.ser = _EIOSerial()
    state.stop_event.clear()
    state.reader_thread = threading.Thread(target=state._reader_loop, daemon=True)
    before = len(_log_lines())
    state.reader_thread.start()
    state.reader_thread.join(timeout=1)

    lines = _log_lines()[before:]
    assert sum("[WARN] Serial reader stopping after I/O error" in line for line in lines) == 1
    assert not any("[ERR] Reader loop error" in line and "Input/output error" in line for line in lines)
    assert state.ser is None
    assert state.stop_event.is_set()
    state.reader_thread = None


def test_reader_eio_during_intentional_shutdown_is_not_logged_as_error_or_warning():
    class _EIOSerial:
        is_open = True

        @property
        def in_waiting(self):
            raise OSError(5, "Input/output error")

        def close(self):
            self.is_open = False

    state.disconnect()
    state.ser = _EIOSerial()
    state.stop_event.set()

    before = len(_log_lines())
    state._reader_loop()
    after = _log_lines()[before:]

    assert not any("Input/output error" in line for line in after)
    state.ser = None


def test_rx_logging_keeps_fragmented_complete_line_as_single_record():
    def handler(master, stop):
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                os.read(master, 1024)
                os.write(master, b"frag")
                time.sleep(0.03)
                os.write(master, b"mented\n")
                return

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        assert state.send_command("go", timeout=1.0, idle_timeout=0.05) == "fragmented\n"
        state.disconnect()
        rx_lines = _rx_log_lines()
        assert sum("fragmented" in line for line in rx_lines) == 1
        assert not any("[RX] frag" in line and "fragmented" not in line for line in rx_lines)
    finally:
        _cleanup(master, slave, stop, thread)


def test_rx_logging_flushes_newline_free_partial_once_on_idle_and_disconnect():
    def handler(master, stop):
        while not stop.is_set():
            time.sleep(0.05)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        # pyserial flushes pending pty input on open, so write after connect
        os.write(master, b"PROMPT> ")
        assert state.read_output(timeout=1.0, idle_timeout=0.05) == "PROMPT> "
        time.sleep(state.log_rx_idle_flush_interval + 0.05)
        state.disconnect()
        rx_lines = _rx_log_lines()
        assert sum("PROMPT> " in line for line in rx_lines) == 1
    finally:
        _cleanup(master, slave, stop, thread)


# ---- bootloader entry -------------------------------------------------------

import server as _server_mod


@pytest.fixture()
def boot_creds(monkeypatch, tmp_path):
    """Point config dir at a temp location."""
    monkeypatch.setattr(_server_mod, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(_server_mod, "USER_PROFILES_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(_server_mod, "CREDENTIALS_PATH", tmp_path / "credentials.json")
    return tmp_path


def _uboot_device_handler(state_box):
    """Fake device: autoboot window responds to Space; else boots to Login.

    Full flow: login root/sekrit -> reboot -> autoboot -> Space -> U-Boot#.
    """
    def handler(master, stop):
        buf = b""
        stage = "running"
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data = os.read(master, 1024)
                buf += data
                if stage == "running":
                    if b"root" in buf:
                        os.write(master, b"Password: ")
                        stage = "password"
                        buf = b""
                elif stage == "password":
                    if b"sekrit" in buf:
                        os.write(master, b"root@dev:~# ")
                        stage = "shell"
                        buf = b""
                elif stage == "shell":
                    if b"reboot" in buf:
                        os.write(master, b"Hit any key to stop autoboot: 3 ")
                        stage = "autoboot"
                        buf = b""
                elif stage == "autoboot":
                    if b" " in buf:
                        os.write(master, b"\r\nU-Boot> ")
                        stage = "uboot"
                        buf = b""
                    elif b"\n" in buf:  # newline = missed window
                        os.write(master, b"test-device login: ")
                        stage = "running"
                        buf = b""
            time.sleep(0.01)
    return handler


def test_list_boot_profiles_builtin_and_user_file(boot_creds):
    text = state.list_boot_profiles()
    assert "generic [builtin]" in text
    assert "u-boot-any-key [builtin]" in text
    # user profile merge
    boot_creds.joinpath("profiles.json").write_text(json.dumps({
        "mydevice": {"description": "custom", "interrupt": {"keys": ["1"]}}
    }))
    text = state.list_boot_profiles()
    assert "mydevice [user]" in text and "custom" in text


def test_set_boot_credentials_writes_0600_file(boot_creds):
    result = state.set_boot_credentials("mydevice", "root", "sekrit")
    assert "0600" in result
    p = boot_creds / "credentials.json"
    assert oct(p.stat().st_mode & 0o777) == "0o600"
    data = json.loads(p.read_text())
    assert data["mydevice"] == {"username": "root", "password": "sekrit"}


def test_enter_bootloader_full_flow_login_reboot_interrupt(boot_creds):
    state.set_boot_credentials("generic", "root", "sekrit")
    # generic profile has no login section; creds keyed by profile name resolve.
    state.enter_bootloader  # existence check

    def handler(master, stop):
        buf = b""
        stage = "login"
        last_prompt = 0.0
        while not stop.is_set():
            # Real consoles (getty) re-prompt continuously; also survives the
            # pyserial open-time flush of pending input.
            if stage == "login" and time.monotonic() - last_prompt > 0.3:
                os.write(master, b"test-device login: ")
                last_prompt = time.monotonic()
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data = os.read(master, 1024)
                buf += data
                if stage == "login" and b"root" in buf:
                    os.write(master, b"Password: ")
                    stage, buf = "password", b""
                elif stage == "password" and b"sekrit" in buf:
                    os.write(master, b"root@dev:~# ")
                    stage, buf = "shell", b""
                elif stage == "shell" and b"reboot" in buf:
                    os.write(master, b"Hit any key to stop autoboot: 3 ")
                    stage, buf = "autoboot", b""
                elif stage == "autoboot" and b" " in buf:
                    os.write(master, b"\r\nU-Boot> ")
                    stage, buf = "uboot", b""
                elif stage == "uboot":
                    pass
            time.sleep(0.01)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        result = state.enter_bootloader(profile="generic")
        assert "Entered bootloader" in result, result
        assert "U-Boot" in result
        # password never appears in the I/O transcript
        assert "sekrit" not in state.view_io(lines=100)
        assert "[redacted]" in state.view_io(lines=100)
    finally:
        _cleanup(master, slave, stop, thread)


def test_enter_bootloader_unknown_profile_and_not_connected():
    assert state.enter_bootloader(profile="nope").startswith("Error: unknown boot profile")
    state.disconnect()
    assert state.enter_bootloader().startswith("Error: Not connected")


def _login_device_handler(password=b"sekrit", wrong=False):
    """Fake getty: prompts for login, then Password, then a shell/nak."""
    def handler(master, stop):
        buf = b""
        stage = "login"
        last_prompt = 0.0
        while not stop.is_set():
            # getty re-prompts until someone answers (and survives pyserial's
            # open-time flush of pending input).
            if stage == "login" and time.monotonic() - last_prompt > 0.3:
                os.write(master, b"test-device login: ")
                last_prompt = time.monotonic()
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                buf += os.read(master, 1024)
                if stage == "login" and b"root" in buf:
                    os.write(master, b"Password: ")
                    stage, buf = "password", b""
                elif stage == "password" and password in buf:
                    if wrong:
                        os.write(master, b"Login incorrect\r\ntest-device login: ")
                        stage, buf = "login", b""
                    else:
                        os.write(master, b"root@dev:~# ")
                        stage, buf = "shell", b""
            time.sleep(0.01)
    return handler


def test_login_logs_in_and_redacts_the_password(boot_creds):
    state.set_boot_credentials("mydevice", "root", "sekrit")
    (boot_creds / "profiles.json").write_text(json.dumps({
        "mydevice": {"login": {"username": "root", "password_ref": "mydevice"}}
    }))

    master, slave, port, stop, thread = _pty_device(_login_device_handler())
    try:
        _connect(port)
        result = state.login(profile="mydevice")
        assert "Logged in" in result, result
        # The secret must never appear in the transcript or the log file.
        assert "sekrit" not in state.view_io(lines=100)
        assert "[redacted]" in state.view_io(lines=100)
        assert "sekrit" not in Path(state.log_path).read_text(encoding="utf-8")
    finally:
        _cleanup(master, slave, stop, thread)


def test_login_reports_rejection(boot_creds):
    state.set_boot_credentials("mydevice", "root", "sekrit")
    (boot_creds / "profiles.json").write_text(json.dumps({
        "mydevice": {"login": {"username": "root", "password_ref": "mydevice"}}
    }))

    master, slave, port, stop, thread = _pty_device(_login_device_handler(wrong=True))
    try:
        _connect(port)
        result = state.login(profile="mydevice")
        assert result.startswith("Login rejected"), result
        assert "sekrit" not in Path(state.log_path).read_text(encoding="utf-8")
    finally:
        _cleanup(master, slave, stop, thread)


def test_login_blind_mode_does_not_leak_password(boot_creds):
    state.set_boot_credentials("mydevice", "root", "sekrit")
    (boot_creds / "profiles.json").write_text(json.dumps({
        "mydevice": {"login": {"username": "root", "password_ref": "mydevice"}}
    }))

    master, slave, port, stop, thread = _pty_device(_login_device_handler())
    try:
        _connect(port)
        result = state.login(profile="mydevice", login_mode="blind")
        assert not result.startswith("Error"), result
        assert "sekrit" not in Path(state.log_path).read_text(encoding="utf-8")
        assert "[redacted]" in Path(state.log_path).read_text(encoding="utf-8")
    finally:
        _cleanup(master, slave, stop, thread)


def test_login_blind_reports_success_when_prompt_is_delayed(boot_creds):
    """A login that works must not be reported as a failure.

    Real consoles emit a banner in bursts: output goes quiet mid-stream, so a
    single idle-bounded read returns before the shell prompt appears. That made
    a successful login look like "no shell prompt was observed".
    """
    state.set_boot_credentials("mydevice", "root", "sekrit")
    (boot_creds / "profiles.json").write_text(json.dumps({
        "mydevice": {"login": {"username": "root", "password_ref": "mydevice"}}
    }))

    def handler(master, stop):
        seen_pw = False
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data = os.read(master, 1024)
                if b"sekrit" in data:
                    seen_pw = True
                if seen_pw and b"echo" not in data:
                    continue
            if seen_pw:
                # Banner in two bursts, separated by a long quiet patch, then
                # the prompt -- the quiet patch must not end the wait.
                os.write(master, b"BANNER-PART-1\r\n")
                time.sleep(1.2)
                os.write(master, b"BANNER-PART-2\r\n")
                time.sleep(0.9)
                os.write(master, b"root@device:~# ")
                return
            time.sleep(0.01)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        result = state.login(profile="mydevice", login_mode="blind", overall_timeout=10.0)
        assert result.startswith("Logged in"), result
        assert "sekrit" not in Path(state.log_path).read_text(encoding="utf-8")
    finally:
        _cleanup(master, slave, stop, thread)


def test_login_blind_reports_rejection_not_success(boot_creds):
    """Console chatter ending in '#' must not be mistaken for a shell prompt."""
    state.set_boot_credentials("mydevice", "root", "sekrit")
    (boot_creds / "profiles.json").write_text(json.dumps({
        "mydevice": {"login": {"username": "root", "password_ref": "mydevice"}}
    }))

    def handler(master, stop):
        seen_pw = False
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                data = os.read(master, 1024)
                if b"sekrit" in data:
                    seen_pw = True
            if seen_pw:
                os.write(master, b"Login incorrect\r\n")
                os.write(master, b"some chatter ending in #\r\n")
                os.write(master, b"test-device login: ")
                return
            time.sleep(0.01)

    master, slave, port, stop, thread = _pty_device(handler)
    try:
        _connect(port)
        result = state.login(profile="mydevice", login_mode="blind", overall_timeout=6.0)
        assert result.startswith("Login rejected"), result
    finally:
        _cleanup(master, slave, stop, thread)


def test_shell_prompt_regex_ignores_console_chatter():
    assert state._SHELL_PROMPT_RE.search("root@dev:~# ")
    assert state._SHELL_PROMPT_RE.search("admin@host:/tmp$ ")
    assert state._SHELL_PROMPT_RE.search("\n# ")
    for chatter in ("Login incorrect", "Password: ", "dev login: ", "--> # ", "switch# "):
        assert not state._SHELL_PROMPT_RE.search(chatter), chatter


def test_login_error_paths(boot_creds):
    state.disconnect()
    assert state.login(profile="generic").startswith("Error: Not connected")

    master, slave, port, stop, thread = _pty_device(lambda m, s: select.select([m], [], [], 0.05))
    try:
        _connect(port)
        # Connected, but no usable credentials: both error paths are reachable.
        assert state.login(profile="nope").startswith("Error: unknown profile")
        assert state.login().startswith("Error: no credentials")
    finally:
        _cleanup(master, slave, stop, thread)


# ---- attach mode: joining a session someone else serves ---------------------
#
# Models the user-first flow: the user runs something that owns the device and
# serves a tio-protocol socket; the MCP server joins as a client. The stand-in
# owner here is a plain fake server, not tio itself, so the tests never touch a
# real device or a real tio install.


class FakeSessionOwner:
    """Minimal stand-in for `tio -S unix:<path> <device>`.

    Owns a fake device (a pty) and serves the socket with the same raw-byte
    protocol: everything the device writes is broadcast to attached clients, and
    every client's bytes go to the device.
    """

    def __init__(self, path):
        self.path = path
        self.device_master, self.device_slave = pty.openpty()
        self.device_port = os.ttyname(self.device_slave)
        self.clients = []
        self.device_input = bytearray()
        self.alive = True
        self._lock = threading.Lock()
        self._stop = threading.Event()

        if os.path.exists(path):
            os.unlink(path)
        self.srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.srv.bind(path)
        self.srv.listen(8)
        self.srv.settimeout(0.2)

        self._threads = [
            threading.Thread(target=self._accept_loop, daemon=True),
            threading.Thread(target=self._device_loop, daemon=True),
        ]
        for t in self._threads:
            t.start()

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.srv.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            conn.settimeout(0.2)
            with self._lock:
                self.clients.append(conn)
            threading.Thread(target=self._client_loop, args=(conn,), daemon=True).start()
    def _client_loop(self, conn):
        while not self._stop.is_set():
            try:
                data = conn.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                break
            with self._lock:
                self.device_input += data
            try:
                os.write(self.device_master, data)  # client -> device
            except OSError:
                break

    def _device_loop(self):
        while not self._stop.is_set():
            r, _, _ = select.select([self.device_master], [], [], 0.1)
            if self.device_master in r:
                data = os.read(self.device_master, 4096)
                if data:
                    with self._lock:
                        for c in list(self.clients):
                            try:
                                c.sendall(data)  # device -> all clients
                            except OSError:
                                pass

    def device_reply(self, text):
        os.write(self.device_master, text)

    def device_seen(self, timeout=2.0):
        """Bytes the device received from clients (recorded, not consumed)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if self.device_input:
                    return bytes(self.device_input)
            time.sleep(0.02)
        with self._lock:
            return bytes(self.device_input)

    def close(self):
        self._stop.set()
        self.alive = False
        try:
            self.srv.close()
        except OSError:
            pass
        with self._lock:
            for c in self.clients:
                try:
                    c.close()
                except OSError:
                    pass
            self.clients = []
        for t in self._threads:
            t.join(timeout=1)
        if os.path.exists(self.path):
            os.unlink(self.path)
        for fd in (self.device_master, self.device_slave):
            try:
                os.close(fd)
            except OSError:
                pass

    def wait_for_clients(self, count=1, timeout=2.0):
        """Block until at least `count` clients have been accepted.

        Attaching is asynchronous: the owner's accept loop may not have taken the
        connection yet, so closing immediately can miss the very connection the
        test means to drop. Waiting makes that deterministic instead of racy.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.clients) >= count:
                    return True
            time.sleep(0.02)
        with self._lock:
            return len(self.clients) >= count

    def drop_all_clients(self):
        """Close every accepted client connection, keeping the owner alive."""
        with self._lock:
            conns, self.clients = list(self.clients), []
        for c in conns:
            try:
                c.close()
            except OSError:
                pass


def test_attach_joins_existing_session_and_exchanges_bytes():
    owner = FakeSessionOwner("/tmp/serial-mcp-TESTATTACH.sock")
    try:
        result = state.attach(attach_socket=owner.path)
        assert result.startswith("Attached"), result
        assert owner.wait_for_clients(1)

        # AI -> device, through the owner.
        out = state.send_command("ping", timeout=1.0, idle_timeout=0.05)
        assert owner.device_seen(1.0) == b"ping\n"

        # device -> AI, through the owner.
        owner.device_reply(b"pong")
        seen = state.read_output(timeout=1.0, idle_timeout=0.05)
        assert "pong" in seen
    finally:
        state.disconnect()
        owner.close()


def test_attach_creates_no_session_socket_or_pty_of_its_own():
    """In attach mode this server must not claim the device or its paths."""
    owner = FakeSessionOwner("/tmp/serial-mcp-TESTATTACH.sock")
    try:
        state.attach(attach_socket=owner.path)
        assert state.transport.kind == "attach"
        assert state.ser is None                      # holds no port
        assert not state.session_running.is_set()     # serves no session
        assert state.session_path is None
        assert state.session_pty_master is None       # exposes no pty
    finally:
        state.disconnect()
        owner.close()


def test_attach_disconnect_leaves_the_owner_running():
    owner = FakeSessionOwner("/tmp/serial-mcp-TESTATTACH.sock")
    try:
        state.attach(attach_socket=owner.path)
        result = state.disconnect()
        assert result.startswith("Detached"), result
        # The owner is untouched and still reachable by someone else.
        assert owner.alive
        assert os.path.exists(owner.path)
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.connect(owner.path)
        probe.close()
    finally:
        state.disconnect()
        owner.close()


def test_attach_reports_errors_clearly():
    state.disconnect()
    missing = "/tmp/serial-mcp-NOSUCH.sock"
    if os.path.exists(missing):
        os.unlink(missing)
    result = state.attach(attach_socket=missing)
    assert result.startswith("Error: no session socket"), result
    assert "tio -S unix:" in result  # tells the user how to start one


def test_attach_rejects_stale_socket_file():
    """"A leftover file with no listener must not look like a session."""
    stale = "/tmp/serial-mcp-STALE.sock"
    if os.path.exists(stale):
        os.unlink(stale)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(stale)
    s.close()  # bound then closed: file remains, nothing listens
    try:
        result = state.attach(attach_socket=stale)
        assert result.startswith("Error:"), result
        assert "nothing is listening" in result
    finally:
        if os.path.exists(stale):
            os.unlink(stale)


def test_connect_rejects_both_port_and_attach_socket():
    result = state.connect(port="/dev/ttyUSB0", attach_socket="/tmp/x.sock")
    assert result.startswith("Error:"), result
    assert "not both" in result


def test_connect_requires_a_target():
    state.disconnect()
    result = state.connect()
    assert result.startswith("Error:"), result
    assert "attach_socket" in result


def test_attach_auto_detection_finds_the_live_session(monkeypatch):
    with tempfile.TemporaryDirectory(prefix="smc") as d:
        monkeypatch.setattr(server, "SESSION_SOCKET_DIR", d)
        owner = FakeSessionOwner(os.path.join(d, "serial-mcp-A.sock"))
        try:
            result = state.attach()  # no path: auto-detect
            assert result.startswith("Attached"), result
            assert owner.path in result
        finally:
            state.disconnect()
            owner.close()


def test_attach_auto_detection_refuses_when_ambiguous(monkeypatch):
    with tempfile.TemporaryDirectory(prefix="smc") as d:
        monkeypatch.setattr(server, "SESSION_SOCKET_DIR", d)
        a = FakeSessionOwner(os.path.join(d, "serial-mcp-A.sock"))
        b = FakeSessionOwner(os.path.join(d, "serial-mcp-B.sock"))
        try:
            result = state.attach()
            assert result.startswith("Error:"), result
            assert "several live session sockets" in result
            assert "serial-mcp-A.sock" in result and "serial-mcp-B.sock" in result
        finally:
            state.disconnect()
            a.close()
            b.close()


def test_attach_auto_detection_reports_no_session(monkeypatch):
    with tempfile.TemporaryDirectory(prefix="smc") as d:
        monkeypatch.setattr(server, "SESSION_SOCKET_DIR", d)
        state.disconnect()
        result = state.attach()
        assert result.startswith("Error: no live session socket"), result
        assert "tio -S unix:" in result


def test_tio_info_describes_attach_mode():
    owner = FakeSessionOwner("/tmp/serial-mcp-TESTATTACH.sock")
    try:
        state.attach(attach_socket=owner.path)
        info = state.tio_info()
        assert owner.path in info
        assert "owned by" in info
        assert "Disconnect detaches" in info
    finally:
        state.disconnect()
        owner.close()


def test_reader_stops_when_the_owner_closes_the_socket():
    """Losing the owner must surface as an unavailable connection, not a hang."""
    owner = FakeSessionOwner("/tmp/serial-mcp-TESTATTACH.sock")
    try:
        state.attach(attach_socket=owner.path)
        # The owner's accept loop is asynchronous, and an earlier liveness probe
        # also counts as a client, so wait for the *real* connection: probe +
        # ours = 2 accepted clients for one attach.
        assert owner.wait_for_clients(2), f"clients={len(owner.clients)}"
        owner.drop_all_clients()
        deadline = time.monotonic() + 5.0
        while state.transport is not None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert state.transport is None, "reader did not notice the owner closing"
        assert state.stop_event.is_set()
    finally:
        state.disconnect()
        owner.close()


# ---- refusing to steal a session or silently share a device -----------------
#
# ttys are not exclusive-open: pyserial opens a port another process already
# holds, and the two then interleave bytes. And _start_session used to unlink a
# live session socket before binding its own, which silently orphaned the
# previous owner. Both must now fail loudly instead.


def test_connect_refuses_to_steal_a_live_session_socket():
    """A live session path must never be unlinked and taken over."""
    owner = FakeSessionOwner("/tmp/serial-mcp-TESTSTEAL.sock")
    try:
        before_inode = os.stat(owner.path).st_ino

        # An agent tries to own a *different* device but collides on the path.
        result = state.connect(port="/dev/ttyDOESNOTEXIST0", session_socket=owner.path)

        assert result.startswith("Error"), result
        assert "already served" in result
        # The owner's socket must be untouched: same inode, still live, still
        # relaying to its device.
        assert os.path.exists(owner.path)
        assert os.stat(owner.path).st_ino == before_inode
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.connect(owner.path)
        probe.close()
        os.write(owner.device_master, b"still-mine")
        # the device write must not raise: the owner still owns its pty
    finally:
        state.disconnect()
        owner.close()


def test_connect_does_not_destroy_session_on_failed_open(monkeypatch):
    """If the port cannot be opened, session paths must not be created."""
    path = "/tmp/serial-mcp-TESTNOOPEN.sock"
    if os.path.exists(path):
        os.unlink(path)
    try:
        result = state.connect(port="/dev/ttyDOESNOTEXIST1", session_socket=path)
        assert result.startswith("Error"), result
        assert not os.path.exists(path), "session socket created despite failed open"
        assert state.transport is None
        assert not state.session_running.is_set()
    finally:
        state.disconnect()


def test_port_lock_blocks_a_second_owner(monkeypatch):
    """Two servers must not both open the same device."""
    import fcntl

    master, slave = pty.openpty()
    port = os.ttyname(slave)
    lock_path = server._serial_port_lock_path(server._serial_device_id(port))
    try:
        # A different process holding the advisory lock stands in for a second
        # server. flock is per-open-file-description, so an independent fd here
        # is seen as a competing holder.
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        result = state.connect(port=port)
        assert result.startswith("Error"), result
        assert "already held" in result or "supplementary serial-mcp lock" in result
        assert state.transport is None

        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        result2 = state.connect(port=port)
        assert result2.startswith("Connected"), result2
    finally:
        state.disconnect()
        if os.path.exists(lock_path):
            os.unlink(lock_path)
        os.close(master)
        os.close(slave)


def test_port_lock_is_released_on_disconnect():
    master, slave = pty.openpty()
    port = os.ttyname(slave)
    try:
        assert state.connect(port=port).startswith("Connected")
        assert state._port_lock_fd is not None
        state.disconnect()
        assert state._port_lock_fd is None
        # Re-taking it must succeed immediately (no stale lock).
        assert state.connect(port=port).startswith("Connected")
    finally:
        state.disconnect()
        os.close(master)
        os.close(slave)


def _subprocess_holds_tty(port):
    script = (
        "import os,sys,time; fd=os.open(sys.argv[1], os.O_RDWR|os.O_NOCTTY|os.O_NONBLOCK); "
        "print('READY', flush=True); time.sleep(30)"
    )
    proc = subprocess.Popen([sys.executable, "-c", script, str(port)], stdout=subprocess.PIPE, text=True)
    assert proc.stdout.readline().strip() == "READY"
    return proc


def test_tio_first_direct_connect_refuses_alias_without_touching_owner(tmp_path):
    master, slave = pty.openpty()
    port = os.ttyname(slave)
    alias = tmp_path / "serial-alias"
    alias.symlink_to(port)
    owner = _subprocess_holds_tty(port)
    session_path = tmp_path / "must-not-be-created.sock"
    try:
        result = state.connect(port=str(alias), session_socket=str(session_path))
        assert result.startswith("Error"), result
        assert "already open" in result and "attach" in result.lower(), result
        assert owner.poll() is None, "existing owner was killed"
        assert state.transport is None
        assert not state.session_running.is_set()
        assert not session_path.exists()
        assert state._port_lock_fd is None, "supplementary lock leaked after refusal"

        owner.terminate()
        owner.wait(timeout=3)
        result = state.connect(port=str(alias), session_socket="")
        assert result.startswith("Connected"), result
        assert state.transport.device_id == server._serial_device_id(port)
    finally:
        state.disconnect()
        if owner.poll() is None:
            owner.terminate()
            owner.wait(timeout=3)
        os.close(master)
        os.close(slave)


def test_direct_owner_blocks_second_open_through_alias_and_releases_exclusivity(tmp_path):
    master, slave = pty.openpty()
    port = os.ttyname(slave)
    alias = tmp_path / "serial-alias"
    alias.symlink_to(port)
    try:
        result = state.connect(port=port, session_socket="")
        assert result.startswith("Connected"), result
        script = (
            "import os,sys\n"
            "try:\n"
            " fd=os.open(sys.argv[1], os.O_RDWR|os.O_NOCTTY|os.O_NONBLOCK)\n"
            " print('OPENED', flush=True); os.close(fd)\n"
            "except OSError as e:\n print('ERR', e.errno, flush=True)\n"
        )
        attempt = subprocess.run([sys.executable, "-c", script, str(alias)], capture_output=True, text=True, timeout=3)
        assert attempt.stdout.strip() == f"ERR {errno.EBUSY}", attempt.stdout + attempt.stderr
        assert state.transport is not None and state.transport.is_open
        state.disconnect()
        # TIOCNXCL cleanup must let a later owner open the same device again.
        fd = os.open(alias, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
        os.close(fd)
    finally:
        state.disconnect()
        os.close(master)
        os.close(slave)


class _TransferMemorySerial:
    """Byte-preserving fake direct serial transport for transfer unit tests."""
    is_open = True
    port = "<synthetic-transfer-peer>"
    baudrate = 115200

    def __init__(self):
        self.rx = bytearray()
        self.writes = []

    @property
    def in_waiting(self):
        return len(self.rx)

    def read(self, size):
        data = bytes(self.rx[:size])
        del self.rx[:size]
        return data

    def write(self, data):
        self.writes.append(bytes(data))
        return len(data)

    def flush(self):
        pass

    def close(self):
        self.is_open = False


@pytest.fixture()
def transfer_connection():
    state.disconnect()
    fake = _TransferMemorySerial()
    state.ser = fake
    state.transfer_active.clear()
    state._transfer_cleanup_paths = []
    with state.session_lock:
        state.session_clients.clear()
    try:
        yield fake
    finally:
        state.disconnect()
        state.ser = None
        state.transfer_active.clear()
        with state.session_lock:
            state.session_clients.clear()


def test_transfer_capabilities_and_shared_io_gating(transfer_connection):
    capabilities = getattr(server.file_transfer_capabilities, "fn", server.file_transfer_capabilities)
    assert "confirm_shared_quiescence=true" in capabilities()
    client, peer = socket.socketpair()
    client.settimeout(1)
    with state.session_lock:
        state.session_clients.add(client)
    state.session_running.set()
    state.session_path = "/synthetic/session.sock"
    before_writes = list(transfer_connection.writes)
    try:
        deadline, error = state._transfer_begin(2)
        assert error is None and deadline is not None, error
        assert state.transfer_active.is_set()
        # A server-owned pty/socket client's bytes are discarded during the
        # transfer, never queued to corrupt the shell protocol.
        state._session_user_write(b"must-not-reach-device")
        assert transfer_connection.writes == before_writes
        start_notice = peer.recv(4096)
        assert b"input is temporarily paused" in start_notice
        assert state._session_transfer_dropped_bytes == len(b"must-not-reach-device")
        state._transfer_end()
        end_notice = peer.recv(4096)
        assert b"terminal input resumed" in end_notice and b"discarded" in end_notice
        assert not state.transfer_active.is_set()

        # External tio raw sockets cannot arbitrate with human clients. The
        # transfer is refused by default and requires explicit idle confirmation.
        state.session_running.clear()
        state.session_path = None
        state.transport.kind = "attach"
        _, error = state._transfer_begin(2)
        assert "confirm_shared_quiescence=true" in error
        deadline, error = state._transfer_begin(2, confirm_shared_quiescence=True)
        assert error is None and deadline is not None, error
        state._transfer_end()
    finally:
        if state.transfer_active.is_set():
            state._transfer_end()
        state.session_running.clear()
        state.session_path = None
        with state.session_lock:
            state.session_clients.discard(client)
        client.close()
        peer.close()


def test_uboot_md_dump_parses_exact_bytes_and_rejects_missing_rows(transfer_connection, monkeypatch):
    output = (
        b"md.b 0x1000 10\r\n"
        b"00001000: 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f    ................\r\n"
        b"=> "
    )
    monkeypatch.setattr(state, "_transfer_read_regex", lambda *args, **kwargs: output)
    data, strength = state._download_uboot("1000", 16, re.compile(rb"=>\s*$"), time.monotonic() + 2)
    assert data == bytes(range(16))
    assert "no end-to-end checksum" in strength

    malformed = b"00001000: 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f\r\n00001020: 10\r\n=> "
    monkeypatch.setattr(state, "_transfer_read_regex", lambda *args, **kwargs: malformed)
    with pytest.raises(ValueError, match="address row"):
        state._download_uboot("1000", 17, re.compile(rb"=>\s*$"), time.monotonic() + 2)


def test_uboot_ymodem_crc_retry_and_ram_readback(tmp_path, monkeypatch, transfer_connection):
    import re

    assert state._ymodem_crc(b"123456789") == 0x31C3
    local = tmp_path / "ram.bin"
    payload = b"\x00\xffA"
    local.write_bytes(payload)
    writes = []
    transfer_started = False

    def fake_write(data):
        nonlocal transfer_started
        writes.append(bytes(data))
        if data.startswith(b"loady "):
            transfer_started = True

    monkeypatch.setattr(state, "_transfer_write", fake_write)
    handshake = iter([b"## Ready for download\r\nC", b""])
    monkeypatch.setattr(state, "_transfer_read", lambda size=4096: next(handshake, b"") if transfer_started else b"")
    replies = iter([0x06, 0x43, 0x15, 0x06, 0x06, 0x06])
    monkeypatch.setattr(state, "_ymodem_wait", lambda *args, **kwargs: next(replies))
    monkeypatch.setattr(state, "_transfer_read_regex", lambda *args, **kwargs: b"Bytes transferred: 3\r\nU-Boot> ")
    monkeypatch.setattr(state, "_download_uboot", lambda addr, length, prompt, deadline: (payload, "exact readback"))
    result = state.upload_file(str(local), "test.bin", target="uboot", address="0x1000", prompt_regex=r"U-Boot>\s*$", timeout=3)
    assert "exact RAM readback verified" in result
    packets = [w for w in writes if w and w[0] in (0x01, 0x02)]
    assert packets[0][0:3] == bytes((0x01, 0x00, 0xff))
    data_packets = [packet for packet in packets if packet[0] == 0x02]
    assert len(data_packets) == 2 and data_packets[0] == data_packets[1]
    assert data_packets[0][3:6] == payload
    assert b"loady 0x1000\r" in writes[0]


def test_interrupted_transfer_releases_exclusive_state(tmp_path, monkeypatch, transfer_connection):
    local = tmp_path / "input.bin"
    local.write_bytes(b"synthetic")
    monkeypatch.setattr(state, "_upload_linux", lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("peer stopped")))
    result = state.upload_file(str(local), "/target", timeout=2)
    assert result.startswith("Error:") and "peer stopped" in result
    assert not state.transfer_active.is_set()
    assert state.operation_lock.acquire(blocking=False)
    state.operation_lock.release()
    assert state.io_lock.acquire(blocking=False)
    state.io_lock.release()


def test_transfer_timeout_and_tool_schema(transfer_connection):
    state.transfer_active.set()
    assert "file transfer" in state.send_command("must-not-write")
    assert "file transfer" in state.read_output()
    state.transfer_active.clear()
    with pytest.raises(TimeoutError, match="timed out"):
        state._transfer_read_marker(b"never", time.monotonic() - 1, 10)
    if hasattr(server.mcp, "get_tools"):
        tools = asyncio.run(server.mcp.get_tools())
        by_name = tools if isinstance(tools, dict) else {item.name: item for item in tools}
    else:
        by_name = {item.name: item for item in asyncio.run(server.mcp.list_tools())}
    for name, expected in (("upload_file", {"local_path", "target_path", "target", "address", "overwrite", "confirm_shared_quiescence"}),
                           ("download_file", {"local_path", "target", "target_path", "address", "length", "overwrite", "confirm_shared_quiescence"})):
        schema = getattr(by_name[name], "parameters", None) or by_name[name].inputSchema
        assert expected <= set(schema["properties"])


def _mcp_text(result):
    return "\\n".join(getattr(item, "text", "") for item in result.content)


def _pty_target_peer(target, master_or_slave, **options):
    return UBootPeer(master_or_slave, **options)


@pytest.mark.parametrize("mode", ["direct", "attach"])
@pytest.mark.parametrize("target", ["uboot"])
def test_mcp_upload_download_direct_and_attach_pty_peers(tmp_path, mode, target):
    payload = bytes(range(256)) + b"\x00\xff\r\n\x04" + bytes(reversed(range(256)))
    source = tmp_path / "mcp-fixture.bin"
    destination = tmp_path / f"{mode}-{target}-download.bin"
    source.write_bytes(payload)
    peer = None
    owner = None
    direct_fds = ()
    if mode == "direct":
        master, slave = pty.openpty()
        tty.setraw(slave)
        peer = _pty_target_peer(target, master)
        direct_fds = (master, slave)
        connect_args = {"port": os.ttyname(slave), "session_socket": ""}
    else:
        owner = FakeSessionOwner(str(tmp_path / "tio-like.sock"))
        tty.setraw(owner.device_slave)
        peer = _pty_target_peer(target, owner.device_slave)
        connect_args = {"attach_socket": owner.path}
    remote_path = f"/tmp/serial-mcp-{mode}-{target}-fixture.bin"
    prompt = r"=>\s*$"

    async def exercise():
        async with Client(server.mcp) as client:
            exposed = {tool.name for tool in await client.list_tools()}
            assert {"upload_file", "download_file", "file_transfer_capabilities"} <= exposed
            connected = await client.call_tool("connect", connect_args)
            assert "Error" not in _mcp_text(connected), _mcp_text(connected)
            try:
                if mode == "attach":
                    refused = await client.call_tool("upload_file", {
                        "local_path": str(source), "target_path": remote_path,
                        "target": target, "address": "0x1000", "timeout": 10,
                    })
                    assert "confirm_shared_quiescence=true" in _mcp_text(refused)
                upload_args = {
                    "local_path": str(source), "target": target, "timeout": 15,
                    "max_bytes": 8192, "overwrite": False,
                }
                if target == "linux":
                    upload_args["target_path"] = remote_path
                else:
                    upload_args.update({"target_path": "fixture.bin", "address": "0x1000", "prompt_regex": prompt})
                if mode == "attach":
                    upload_args["confirm_shared_quiescence"] = True
                uploaded = await client.call_tool("upload_file", upload_args)
                upload_text = _mcp_text(uploaded)
                assert "Uploaded 517 bytes" in upload_text, upload_text
                if target == "linux":
                    rejected = await client.call_tool("upload_file", upload_args)
                    assert "not overwritten" in _mcp_text(rejected)
                    download_args = {
                        "local_path": str(destination), "target": target,
                        "target_path": remote_path, "overwrite": True,
                        "timeout": 15, "max_bytes": 8192,
                    }
                else:
                    missing_address = dict(upload_args)
                    missing_address["address"] = None
                    missing_address.pop("confirm_shared_quiescence", None)
                    if mode == "attach":
                        missing_address["confirm_shared_quiescence"] = True
                    rejected = await client.call_tool("upload_file", missing_address)
                    assert "explicit hexadecimal address" in _mcp_text(rejected)
                    download_args = {
                        "local_path": str(destination), "target": target,
                        "address": "0x1000", "length": len(payload),
                        "prompt_regex": prompt, "overwrite": True,
                        "timeout": 15, "max_bytes": 8192,
                    }
                if mode == "attach":
                    download_args["confirm_shared_quiescence"] = True
                downloaded = await client.call_tool("download_file", download_args)
                download_text = _mcp_text(downloaded)
                assert downloaded.is_error is False and "Downloaded 517 bytes" in download_text, download_text
                if target == "linux":
                    recovered = await client.call_tool("send_command", {
                        "text": "echo after-transfer", "line_ending": "cr", "timeout": 3,
                        "idle_timeout": 0.1,
                    })
                    assert "after-transfer" in _mcp_text(recovered)
                else:
                    recovered = await client.call_tool("send_command", {
                        "text": "md.b 0x1000 10", "line_ending": "cr", "timeout": 3,
                        "idle_timeout": 0.1,
                    })
                    assert "00001000:" in _mcp_text(recovered)
            finally:
                await client.call_tool("disconnect", {})
        return True

    try:
        asyncio.run(exercise())
        assert destination.read_bytes() == payload
        if mode == "attach":
            assert owner.alive, "disconnecting an attach must not stop the tio-like owner"
    finally:
        if peer:
            peer.close()
        if owner:
            owner.close()
        for fd in direct_fds:
            try:
                os.close(fd)
            except OSError:
                pass


def test_mcp_openssl_only_real_interactive_shell_with_echo_wrapping(tmp_path):
    import shutil
    payload = bytes(range(256)) + b"\x00\xff\r\n\x04" + bytes(reversed(range(256)))
    source = tmp_path / "interactive-fixture.bin"
    destination = tmp_path / "interactive-down.bin"
    target_path = str(tmp_path / "remote ' path with spaces.bin")
    source.write_bytes(payload)
    command_dir = tmp_path / "target-bin"
    command_dir.mkdir()
    # Simulate a constrained BusyBox-like target PATH: no stty or sha256sum.
    for name in ("cat", "base64", "openssl", "ln", "mv", "rm", "wc"):
        binary = shutil.which(name)
        assert binary, f"host test prerequisite missing: {name}"
        (command_dir / name).symlink_to(binary)
    owner = InteractiveShellTioOwner(path=str(command_dir))
    try:
        async def exercise():
            async with Client(server.mcp) as client:
                connected = await client.call_tool("connect", {"attach_socket": owner.path})
                assert "Attached" in _mcp_text(connected)
                up = await client.call_tool("upload_file", {
                    "local_path": str(source), "target": "linux", "target_path": target_path,
                    "confirm_shared_quiescence": True, "overwrite": False,
                    "timeout": 45, "max_bytes": 8192,
                })
                assert "Uploaded 517 bytes" in _mcp_text(up), _mcp_text(up)
                down = await client.call_tool("download_file", {
                    "local_path": str(destination), "target": "linux", "target_path": target_path,
                    "confirm_shared_quiescence": True, "overwrite": True,
                    "timeout": 45, "max_bytes": 8192,
                })
                assert "Downloaded 517 bytes" in _mcp_text(down), _mcp_text(down)
                await client.call_tool("disconnect", {})
        asyncio.run(exercise())
        assert destination.read_bytes() == payload
    finally:
        state.disconnect()
        owner.close()


def test_attach_interrupted_uboot_transfer_cancels_loady_and_restores_io(tmp_path):
    owner = FakeSessionOwner(str(tmp_path / "owner.sock"))
    tty.setraw(owner.device_slave)
    peer = UBootPeer(owner.device_slave, reject_data=True)
    source = tmp_path / "uboot-interrupt.bin"
    source.write_bytes(bytes(range(32)))
    try:
        async def exercise():
            async with Client(server.mcp) as client:
                connected = await client.call_tool("connect", {"attach_socket": owner.path})
                assert "Attached" in _mcp_text(connected)
                failed = await client.call_tool("upload_file", {
                    "local_path": str(source), "target": "uboot", "target_path": "reject.bin",
                    "address": "0x1000", "prompt_regex": r"=>\s*$",
                    "confirm_shared_quiescence": True, "timeout": 10, "max_bytes": 1024,
                })
                failed_text = _mcp_text(failed)
                assert "during uboot-ymodem-data" in failed_text and "YMODEM block 1" in failed_text and "Partial volatile RAM" in failed_text, failed_text
                assert not state.transfer_active.is_set()
                recovered = await client.call_tool("send_command", {
                    "text": "md.b 0x1000 10", "line_ending": "cr", "timeout": 3,
                    "idle_timeout": 0.1,
                })
                assert "=>" in _mcp_text(recovered)
                await client.call_tool("disconnect", {})
        asyncio.run(exercise())
        assert owner.alive
    finally:
        state.disconnect()
        peer.close()
        owner.close()


# ---- real interactive shell over PTY and raw socket -----------------------
#
# These tests run an actual /bin/sh (dash here; BusyBox ash was not available
# on this host) with canonical input, echo, and ISIG, behind either a separate
# device PTY that the server opens directly or a tio-like raw UNIX socket. The
# fake command parser used earlier could not reproduce the target failures, so
# every Linux transfer path below drives real shell line handling.

REAL_SHELL_TOOLS = ("cat", "base64", "openssl", "ln", "mv", "rm", "wc", "sha256sum")
PAYLOAD_517 = bytes(range(256)) + b"\x00\xff\r\n\x04" + bytes(reversed(range(256)))


def _open_real_target(mode, tmp_path, names=REAL_SHELL_TOOLS, shims=None, pace=0.0):
    """Return (connect arguments, close callable) for a real-shell target."""
    bindir = build_target_bin(tmp_path / "target-bin", names, shims)
    if mode == "direct":
        device = RealShellDevice(bindir, pace=pace)
        return {"port": device.device_path, "session_socket": ""}, device.close
    owner = InteractiveShellTioOwner(
        socket_path=str(tmp_path / "tio-like.sock"), path=bindir, pace=pace,
    )
    return {"attach_socket": owner.path}, owner.close


def _staging_leftovers(directory):
    return sorted(p.name for p in Path(directory).iterdir() if p.name.startswith(".serial-mcp-"))


def _sha256_shim(wrong_digest_for_files):
    import shutil
    real = shutil.which("sha256sum")
    assert real, "host prerequisite missing: sha256sum"
    if not wrong_digest_for_files:
        return None
    return (
        "#!/bin/sh\n"
        "if [ \"$#\" -gt 0 ]; then\n"
        "  echo \"0000000000000000000000000000000000000000000000000000000000000000  $1\"\n"
        "else\n"
        f"  exec {real}\n"
        "fi\n"
    )


@pytest.mark.parametrize("mode", ["direct", "attach"])
def test_real_shell_linux_round_trip_binary_and_recovery(tmp_path, mode):
    source = tmp_path / "real-source.bin"
    source.write_bytes(PAYLOAD_517)
    remote_dir = tmp_path / "target dir"
    remote_dir.mkdir()
    remote = remote_dir / "remote fixture.bin"
    destination = tmp_path / f"{mode}-download.bin"
    connect_args, close = _open_real_target(mode, tmp_path)
    confirm = {"confirm_shared_quiescence": True} if mode == "attach" else {}

    async def exercise():
        async with Client(server.mcp) as client:
            connected = _mcp_text(await client.call_tool("connect", connect_args))
            assert "Error" not in connected, connected
            try:
                up = _mcp_text(await client.call_tool("upload_file", {
                    "local_path": str(source), "target": "linux", "target_path": str(remote),
                    "overwrite": False, "timeout": 60, "max_bytes": 8192, **confirm,
                }))
                assert up.startswith("Uploaded 517 bytes"), up
                assert "matched the local file via sha256sum" in up, up
                down = _mcp_text(await client.call_tool("download_file", {
                    "local_path": str(destination), "target": "linux", "target_path": str(remote),
                    "overwrite": True, "timeout": 60, "max_bytes": 8192, **confirm,
                }))
                assert down.startswith("Downloaded 517 bytes"), down
                recovered = _mcp_text(await client.call_tool("send_command", {
                    "text": "echo after-transfer", "line_ending": "cr", "timeout": 5, "idle_timeout": 0.2,
                }))
                assert "after-transfer" in recovered, recovered
            finally:
                await client.call_tool("disconnect", {})

    try:
        asyncio.run(exercise())
        assert destination.read_bytes() == PAYLOAD_517
        assert remote.read_bytes() == PAYLOAD_517
        assert _staging_leftovers(remote_dir) == []
    finally:
        state.disconnect()
        close()


@pytest.mark.parametrize("mode", ["direct", "attach"])
def test_real_shell_openssl_only_target_without_sha256sum(tmp_path, mode):
    source = tmp_path / "openssl-only.bin"
    source.write_bytes(PAYLOAD_517)
    remote = tmp_path / "openssl-only-remote.bin"
    destination = tmp_path / "openssl-only-download.bin"
    names = tuple(n for n in REAL_SHELL_TOOLS if n != "sha256sum")
    connect_args, close = _open_real_target(mode, tmp_path, names=names)
    confirm = {"confirm_shared_quiescence": True} if mode == "attach" else {}

    async def exercise():
        async with Client(server.mcp) as client:
            assert "Error" not in _mcp_text(await client.call_tool("connect", connect_args))
            try:
                up = _mcp_text(await client.call_tool("upload_file", {
                    "local_path": str(source), "target": "linux", "target_path": str(remote),
                    "overwrite": False, "timeout": 60, "max_bytes": 8192, **confirm,
                }))
                assert up.startswith("Uploaded 517 bytes") and "via openssl" in up, up
                down = _mcp_text(await client.call_tool("download_file", {
                    "local_path": str(destination), "target": "linux", "target_path": str(remote),
                    "overwrite": True, "timeout": 60, "max_bytes": 8192, **confirm,
                }))
                assert down.startswith("Downloaded 517 bytes") and "hash tool: openssl" in down, down
            finally:
                await client.call_tool("disconnect", {})

    try:
        asyncio.run(exercise())
        assert destination.read_bytes() == PAYLOAD_517
    finally:
        state.disconnect()
        close()


def test_real_shell_wrong_hash_installs_nothing_and_cleans_staging(tmp_path):
    source = tmp_path / "bad-hash-source.bin"
    source.write_bytes(PAYLOAD_517)
    remote_dir = tmp_path / "bad-hash-dir"
    remote_dir.mkdir()
    remote = remote_dir / "never-installed.bin"
    shim = _sha256_shim(wrong_digest_for_files=True)
    connect_args, close = _open_real_target("direct", tmp_path, shims={"sha256sum": shim})

    async def exercise():
        async with Client(server.mcp) as client:
            assert "Error" not in _mcp_text(await client.call_tool("connect", connect_args))
            try:
                result = _mcp_text(await client.call_tool("upload_file", {
                    "local_path": str(source), "target": "linux", "target_path": str(remote),
                    "overwrite": False, "timeout": 60, "max_bytes": 8192,
                }))
                assert "did not match" in result and "nothing was installed" in result, result
                assert "The install was not attempted" in result, result
                assert "Staging files were removed" in result, result
            finally:
                await client.call_tool("disconnect", {})

    try:
        asyncio.run(exercise())
        assert not remote.exists()
        assert _staging_leftovers(remote_dir) == []
    finally:
        state.disconnect()
        close()


def test_real_shell_no_clobber_keeps_existing_destination(tmp_path):
    source = tmp_path / "noclobber-source.bin"
    source.write_bytes(PAYLOAD_517)
    remote = tmp_path / "existing-destination.bin"
    remote.write_bytes(b"keep-me")
    connect_args, close = _open_real_target("direct", tmp_path)

    async def exercise():
        async with Client(server.mcp) as client:
            assert "Error" not in _mcp_text(await client.call_tool("connect", connect_args))
            try:
                result = _mcp_text(await client.call_tool("upload_file", {
                    "local_path": str(source), "target": "linux", "target_path": str(remote),
                    "overwrite": False, "timeout": 60, "max_bytes": 8192,
                }))
                assert "was not overwritten" in result, result
                assert "Staging files were removed" in result, result
            finally:
                await client.call_tool("disconnect", {})

    try:
        asyncio.run(exercise())
        assert remote.read_bytes() == b"keep-me"
        assert _staging_leftovers(tmp_path) == []
    finally:
        state.disconnect()
        close()


@pytest.mark.parametrize("mode", ["direct", "attach"])
def test_real_shell_interrupted_stream_recovers_prompt_and_cleans_staging(tmp_path, mode):
    source = tmp_path / "interrupted.bin"
    source.write_bytes(os.urandom(6000))
    remote = tmp_path / "interrupted-remote.bin"
    connect_args, close = _open_real_target(mode, tmp_path, pace=0.02)
    confirm = {"confirm_shared_quiescence": True} if mode == "attach" else {}

    async def exercise():
        async with Client(server.mcp) as client:
            assert "Error" not in _mcp_text(await client.call_tool("connect", connect_args))
            try:
                failed = _mcp_text(await client.call_tool("upload_file", {
                    "local_path": str(source), "target": "linux", "target_path": str(remote),
                    "overwrite": False, "timeout": 4, "max_bytes": 8192, **confirm,
                }))
                assert "failed during linux-stream" in failed, failed
                assert "Staging files were removed" in failed, failed
                assert "The install was not attempted" in failed, failed
                alive = _mcp_text(await client.call_tool("send_command", {
                    "text": "echo ALIVE", "line_ending": "cr", "timeout": 5, "idle_timeout": 0.3,
                }))
                assert "ALIVE" in alive, alive
            finally:
                await client.call_tool("disconnect", {})

    try:
        asyncio.run(exercise())
        assert not remote.exists()
        assert _staging_leftovers(tmp_path) == []
    finally:
        state.disconnect()
        close()


def test_real_shell_continuation_prompt_needs_only_ctrl_c(tmp_path):
    connect_args, close = _open_real_target("direct", tmp_path)
    try:
        assert state.connect(**connect_args).startswith("Connected")
        deadline, error = state._transfer_begin(10)
        assert error is None, error
        try:
            state._transfer_write(b'printf "unterminated\r')
            assert state._settle_prompt(b"", 1.0) == "continuation"
            assert state._settle_prompt(b"\x03", 1.0) == "primary"
        finally:
            state._transfer_end()
    finally:
        state.disconnect()
        close()


def test_real_shell_long_control_line_is_refused_before_any_byte_is_sent(tmp_path, monkeypatch):
    source = tmp_path / "long-path-source.bin"
    source.write_bytes(PAYLOAD_517)
    connect_args, close = _open_real_target("direct", tmp_path)
    writes = []
    original = state._transfer_write
    try:
        assert state.connect(**connect_args).startswith("Connected")
        monkeypatch.setattr(state, "_transfer_write", lambda data: (writes.append(bytes(data)), original(data)))
        result = state.upload_file(str(source), str(tmp_path / ("x" * 260)), timeout=10, max_bytes=8192)
        assert "240-byte limit" in result and "shorter target path" in result, result
        assert writes == [], "no target bytes may be sent for an over-long control line"
    finally:
        monkeypatch.setattr(state, "_transfer_write", original)
        state.disconnect()
        close()


def test_control_line_keeps_marker_out_of_its_own_echo():
    wire, marker = state._control_wire("printf x | sha256sum")
    text = wire.decode("utf-8")
    assert marker.decode("ascii") not in text
    assert text.endswith("\r") and len(wire) - 1 <= state.MAX_LINUX_COMMAND_BYTES
    assert "RC:%s" in text


def test_diagnostics_redact_base64_payload_but_keep_stage_names():
    state._reset_transfer_trace()
    state._trace("linux-stream", "684-chars")
    state._transfer_recent.extend(b"QUJD" * 40 + b" tail")
    summary = state._trace_summary()
    assert "QUJDQUJD" not in summary
    assert "<b64-redacted>" in summary and "linux-stream:684-chars" in summary


@pytest.mark.parametrize("mode", ["direct"])
def test_real_shell_own_session_gates_human_input_during_upload(tmp_path, mode):
    payload = os.urandom(2000)
    source = tmp_path / "gated-source.bin"
    source.write_bytes(payload)
    remote = tmp_path / "gated-remote.bin"
    session_path = str(tmp_path / "owned-session.sock")
    bindir = build_target_bin(tmp_path / "target-bin", REAL_SHELL_TOOLS)
    if mode == "direct":
        device = RealShellDevice(bindir, pace=0.02)
        connect_args, close = {"port": device.device_path, "session_socket": session_path}, device.close
    else:
        owner = InteractiveShellTioOwner(socket_path=str(tmp_path / "tio-like.sock"), path=bindir, pace=0.02)
        connect_args, close = {"attach_socket": owner.path}, owner.close
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(1)
    result_box = {}

    def upload_in_thread():
        async def call():
            async with Client(server.mcp) as mcp_client:
                result_box["text"] = _mcp_text(await mcp_client.call_tool("upload_file", {
                    "local_path": str(source), "target": "linux", "target_path": str(remote),
                    "overwrite": False, "timeout": 60, "max_bytes": 8192,
                    **({"confirm_shared_quiescence": True} if mode == "attach" else {}),
                }))
        asyncio.run(call())

    try:
        connected = state.connect(**connect_args) if mode == "direct" else state.attach(attach_socket=connect_args["attach_socket"])
        assert connected.startswith(("Connected", "Attached")), connected
        if mode == "direct":
            client.connect(session_path)
            deadline = time.monotonic() + 3
            while not state.session_clients and time.monotonic() < deadline:
                time.sleep(0.01)
        thread = threading.Thread(target=upload_in_thread, daemon=True)
        thread.start()
        deadline = time.monotonic() + 20
        while state._transfer_stage != "linux-stream" and time.monotonic() < deadline and thread.is_alive():
            time.sleep(0.005)
        if mode == "direct":
            client.sendall(b"HUMAN-INPUT-MUST-NOT-REACH-SHELL\r")
        thread.join(timeout=90)
        assert not thread.is_alive(), "upload did not finish"
        assert "Uploaded 2000 bytes" in result_box.get("text", ""), result_box
        assert remote.read_bytes() == payload, "human input reached the shell during the transfer"
        assert _staging_leftovers(tmp_path / "") == []
        if mode == "direct":
            notices = bytearray()
            end = time.monotonic() + 1.5
            while time.monotonic() < end:
                try:
                    notices.extend(client.recv(4096))
                except socket.timeout:
                    break
            assert b"input is temporarily paused" in notices
            assert b"resumed" in notices
    finally:
        state.disconnect()
        client.close()
        close()
