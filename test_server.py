import json
import os
import pty
import select
import socket
import tempfile

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
        "attach",
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


def test_attach_joins_existing_session_and_exchanges_bytes():
    owner = FakeSessionOwner("/tmp/serial-mcp-TESTATTACH.sock")
    try:
        result = state.attach(attach_socket=owner.path)
        assert result.startswith("Attached"), result

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
        owner.close()  # owner goes away (closes every connection it accepted)
        deadline = time.monotonic() + 3.0
        while state.transport is not None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert state.transport is None
        assert state.stop_event.is_set()
    finally:
        state.disconnect()
