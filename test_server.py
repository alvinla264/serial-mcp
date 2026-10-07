import json
import os
import pty
import select
import socket

import pytest
import threading
import time
import asyncio
from pathlib import Path

import server
from server import state


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
        time.sleep(0.1)

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


def test_startup_config_env_overrides_file(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DEFAULT_CONFIG_PATH", tmp_path / "config.json")
    (tmp_path / "config.json").write_text(
        json.dumps({"port": "/dev/ttyUSB9", "baudrate": 9600, "junk": 1})
    )
    monkeypatch.setenv("SERIAL_MCP_PORT", "/dev/ttyUSB7")
    for var in ("SERIAL_MCP_BAUDRATE", "SERIAL_MCP_TIMEOUT", "SERIAL_MCP_PROMPT_REGEX", "SERIAL_MCP_SESSION_SOCKET"):
        monkeypatch.delenv(var, raising=False)

    cfg = server._load_startup_config()
    assert cfg["port"] == "/dev/ttyUSB7"  # env wins
    assert cfg["baudrate"] == 9600  # untouched key from file
    assert "junk" not in cfg


def test_auto_connect_from_config_connects_to_configured_port(monkeypatch):
    calls = {}

    def fake_connect(port, **kwargs):
        calls["port"] = port
        calls.update(kwargs)
        return f"Connected to {port} at {kwargs.get('baudrate')} baud."

    monkeypatch.setattr(server, "_load_startup_config", lambda: {"port": "/dev/ttyUSB3", "baudrate": 57600})
    monkeypatch.setattr(server.state, "connect", fake_connect)

    assert server._auto_connect_from_config().startswith("Connected")
    assert calls["port"] == "/dev/ttyUSB3"
    assert calls["baudrate"] == 57600


def test_auto_connect_from_config_is_noop_without_port(monkeypatch):
    monkeypatch.setattr(server, "_load_startup_config", lambda: {})
    assert server._auto_connect_from_config() is None


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

    master, slave, port, stop, thread = _pty_device(_login_device_handler())
    try:
        _connect(port)
        state.login(profile="mydevice", login_mode="blind")
        assert "sekrit" not in Path(state.log_path).read_text(encoding="utf-8")
        assert "[redacted]" in Path(state.log_path).read_text(encoding="utf-8")
    finally:
        _cleanup(master, slave, stop, thread)


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
