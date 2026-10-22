"""Human terminal (serial-mcp-terminal) tests.

The terminal is exercised as a real subprocess on a pseudo-terminal, exactly as a
person runs it. It talks to a real daemon (docs/daemon-ipc.md) over its UNIX
socket. The daemon runs in-process so lifetime tests can move its injected clock;
devices are ptys; AI peers are synthetic ``DaemonClient`` connections. No test
touches real hardware, and no test relies on a racing sleep: every wait is a
bounded poll on observable state.
"""

import os
import pty
import select
import signal
import subprocess
import sys
import termios
import threading
import time
from pathlib import Path

import pytest

import serial_mcp_ipc as ipc
from test_daemon import Harness, _collect_output, _pty_device, _release_device, _wait_until

ROOT = Path(__file__).resolve().parent
EXIT_KEY = b"\x1d"  # Ctrl-]
BANNER_END = b"other keys go to the device."


class TerminalProcess:
    """One ``serial-mcp-terminal`` process on its own pseudo-terminal."""

    def __init__(self, runtime: Path, extra_env: dict | None = None) -> None:
        self.master, self.slave = pty.openpty()
        # Terminal settings are per tty, so the test's copy of the slave fd sees
        # exactly what the terminal process sees.
        self.initial_settings = termios.tcgetattr(self.slave)
        env = dict(os.environ)
        env["SERIAL_MCP_RUNTIME_DIR"] = str(runtime)
        env.update(extra_env or {})
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "serial_mcp_terminal"],
            stdin=self.slave, stdout=self.slave, stderr=self.slave,
            env=env, cwd=str(ROOT), start_new_session=True, close_fds=True,
        )
        self._seen = bytearray()

    def read_until(self, needle: bytes, timeout: float = 10.0) -> bytes:
        """Return all terminal output once ``needle`` has appeared (bounded)."""
        deadline = time.monotonic() + timeout
        while needle not in self._seen:
            if time.monotonic() >= deadline:
                raise AssertionError(f"terminal never printed {needle!r}; got {bytes(self._seen)[-400:]!r}")
            self._read_some(deadline)
        return bytes(self._seen)

    def _read_some(self, deadline: float) -> None:
        readable, _, _ = select.select([self.master], [], [], max(0.0, min(0.2, deadline - time.monotonic())))
        if self.master in readable:
            try:
                self._seen.extend(os.read(self.master, 65536))
            except OSError:
                pass  # the terminal closed its side

    def type(self, data: bytes) -> None:
        os.write(self.master, data)

    def wait(self, timeout: float = 10.0) -> int:
        deadline = time.monotonic() + timeout
        while self.proc.poll() is None:
            if time.monotonic() >= deadline:
                self.proc.kill()
                raise AssertionError("terminal did not exit within the bounded wait")
            self._read_some(deadline)
        # Drain whatever it printed on its way out.
        while True:
            readable, _, _ = select.select([self.master], [], [], 0)
            if self.master not in readable:
                break
            try:
                chunk = os.read(self.master, 65536)
            except OSError:
                break
            if not chunk:
                break
            self._seen.extend(chunk)
        return self.proc.returncode

    def output(self) -> bytes:
        return bytes(self._seen)

    def settings(self):
        return termios.tcgetattr(self.slave)

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        for fd in (self.master, self.slave):
            try:
                os.close(fd)
            except OSError:
                pass


class Lab:
    """A daemon (in-process, injected clock), one fake device, and terminals."""

    def __init__(self, tmp_path: Path) -> None:
        self.harness = Harness(tmp_path)
        self.runtime = self.harness.runtime
        self.terminals: list[TerminalProcess] = []
        self.device = None  # (master, slave, port, stop, thread)
        self._device_buf = bytearray()  # everything the daemon has written to the device

    # -- device ------------------------------------------------------------

    def connect_device(self, controller) -> None:
        handler = _silent_device
        self.device = _pty_device(handler)
        master, _slave, port, _stop, _thread = self.device
        assert controller.call("connect", {"port": port, "session_socket": ""}).startswith("Connected")

    @property
    def device_master(self) -> int:
        return self.device[0]

    def device_write(self, data: bytes) -> None:
        os.write(self.device_master, data)

    def device_read_until(self, needle: bytes, timeout: float = 10.0) -> bytes:
        """Every byte the daemon has written to the device so far, once ``needle`` arrived."""
        deadline = time.monotonic() + timeout
        while needle not in self._device_buf:
            if time.monotonic() >= deadline:
                raise AssertionError(f"device never received {needle!r}; got {bytes(self._device_buf)!r}")
            self._pull_device(deadline)
        return bytes(self._device_buf)

    def device_drain(self) -> bytes:
        """Every byte delivered to the device so far, without waiting for more."""
        while True:
            readable, _, _ = select.select([self.device_master], [], [], 0)
            if self.device_master not in readable:
                break
            self._device_buf.extend(os.read(self.device_master, 65536))
        return bytes(self._device_buf)

    def _pull_device(self, deadline: float) -> None:
        readable, _, _ = select.select([self.device_master], [], [], max(0.0, min(0.2, deadline - time.monotonic())))
        if self.device_master in readable:
            self._device_buf.extend(os.read(self.device_master, 65536))

    # -- people --------------------------------------------------------------

    def terminal(self, extra_env: dict | None = None) -> TerminalProcess:
        term = TerminalProcess(self.runtime, extra_env)
        self.terminals.append(term)
        term.read_until(BANNER_END)
        return term

    def close(self) -> None:
        for term in self.terminals:
            term.close()
        self.harness.close()
        if self.device is not None:
            master, slave, _port, stop, thread = self.device
            _release_device(master, slave, stop, thread)


def _silent_device(master, stop):
    """Keeps the device open. The test itself reads the wire, so nothing else may."""
    stop.wait()


@pytest.fixture()
def lab(tmp_path):
    instance = Lab(tmp_path)
    yield instance
    instance.close()


def _status(lab) -> dict:
    return lab.harness.daemon.status()


# ---- slice 1: a human terminal shows output and forwards keystrokes ---------


def test_human_terminal_shows_device_output_and_forwards_keystrokes(lab):
    ai = lab.harness.client(name="ai-a")
    lab.connect_device(ai)

    term = lab.terminal()
    lab.device_write(b"boot ok\r\n")
    term.read_until(b"boot ok")

    term.type(b"ls\r")
    assert b"ls\r" in lab.device_read_until(b"ls\r")
    assert _status(lab)["clients"] == 2


# ---- slice 2: the local exit key, tty restore and connection-only close ----


def test_exit_key_is_local_and_closes_only_the_human_connection(lab):
    ai_a = lab.harness.client(name="ai-a")
    lab.connect_device(ai_a)
    lab.harness.client(name="ai-b")
    term = lab.terminal()

    term.type(b"abc\x1d")
    assert b"abc" in lab.device_read_until(b"abc")
    assert term.wait() == 0
    # Ctrl-] was consumed locally: it never reached the device.
    assert EXIT_KEY not in lab.device_drain()
    # Only the human connection went away; the device and both AI clients remain.
    _wait_until(lambda: _status(lab)["clients"] == 2)
    status = _status(lab)
    assert status["device"] is not None
    assert status["shutting_down"] is False
    assert status["grace"]["active"] is False


def test_exit_restores_the_terminal_settings_it_changed(lab):
    lab.connect_device(lab.harness.client(name="ai-a"))
    term = lab.terminal()
    assert term.settings() != term.initial_settings, "the terminal should be in raw mode while running"
    term.type(EXIT_KEY)
    assert term.wait() == 0
    assert term.settings() == term.initial_settings


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGHUP])
def test_catchable_termination_signals_restore_the_terminal(lab, signum):
    lab.connect_device(lab.harness.client(name="ai-a"))
    term = lab.terminal()
    term.proc.send_signal(signum)
    assert term.wait() == 128 + signum
    assert term.settings() == term.initial_settings
    _wait_until(lambda: _status(lab)["clients"] == 1)
    assert _status(lab)["device"] is not None


# ---- slice 3: daemon-side failures are reported and the tty is restored ----


def test_daemon_shutdown_is_reported_and_restores_the_terminal(lab):
    lab.connect_device(lab.harness.client(name="ai-a"))
    term = lab.terminal()
    lab.harness.daemon.stop(reason="maintenance")
    assert term.wait() == 1
    assert b"the daemon is shutting down" in term.output()
    assert term.settings() == term.initial_settings


def test_unusable_runtime_directory_is_reported_before_the_tty_is_touched(tmp_path):
    runtime = tmp_path / "shared-rt"
    runtime.mkdir(mode=0o755)  # group/other readable: the daemon helpers must refuse it
    master, slave = pty.openpty()
    initial = termios.tcgetattr(slave)
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime))
    proc = subprocess.Popen(
        [sys.executable, "-m", "serial_mcp_terminal"],
        stdin=slave, stdout=slave, stderr=slave, env=env, cwd=str(ROOT),
        start_new_session=True, close_fds=True,
    )
    try:
        assert proc.wait(timeout=20) == 1
        output = b""
        while True:
            readable, _, _ = select.select([master], [], [], 0)
            if master not in readable:
                break
            output += os.read(master, 65536)
        assert b"cannot reach the serial-mcp daemon" in output
        assert b"accessible to other users" in output
        assert termios.tcgetattr(slave) == initial
    finally:
        if proc.poll() is None:
            proc.kill()
        os.close(master)
        os.close(slave)


def test_stdin_that_is_not_a_terminal_is_refused(tmp_path):
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(tmp_path / "rt"))
    proc = subprocess.run(
        [sys.executable, "-m", "serial_mcp_terminal"], input=b"", capture_output=True,
        env=env, cwd=str(ROOT), timeout=20,
    )
    assert proc.returncode == 2
    assert b"standard input is not a terminal" in proc.stderr


# ---- slice 4: lifetime. A human connection counts as a live client -----------


def test_human_terminal_keeps_the_daemon_alive_and_last_exit_starts_grace(lab):
    clock = lab.harness.clock
    ai = lab.harness.client(name="ai-a")
    ai.close()
    lab.harness.clients.remove(ai)
    _wait_until(lambda: _status(lab)["clients"] == 0)

    term = lab.terminal()
    assert _status(lab)["clients"] == 1
    assert _status(lab)["grace"]["active"] is False
    clock.advance(3600)
    lab.harness.daemon.tick()
    assert not lab.harness.daemon.stopped, "a connected human terminal keeps the daemon alive"

    term.type(EXIT_KEY)
    assert term.wait() == 0
    _wait_until(lambda: _status(lab)["clients"] == 0)
    assert _status(lab)["grace"]["remaining"] == pytest.approx(30.0)

    clock.advance(29.9)
    lab.harness.daemon.tick()
    assert not lab.harness.daemon.stopped
    newcomer = lab.harness.client(name="ai-late")  # cancels the grace period
    clock.advance(120)
    lab.harness.daemon.tick()
    assert not lab.harness.daemon.stopped
    assert _status(lab)["grace"]["active"] is False
    newcomer.close()
    lab.harness.clients.remove(newcomer)
    _wait_until(lambda: _status(lab)["clients"] == 0)
    clock.advance(30)
    lab.harness.daemon.tick()
    assert lab.harness.daemon.stopped


# ---- slice 5: one daemon, one device owner, many clients -------------------


def test_human_input_reaches_the_device_during_an_ai_held_operation(lab):
    ai_a = lab.harness.client(name="ai-a")
    observer = lab.harness.client(name="ai-b")
    lab.connect_device(ai_a)
    observer.subscribe()
    term = lab.terminal()

    worker = threading.Thread(target=lambda: ai_a.call("send_command", {
        "text": "slow", "timeout": 20, "idle_timeout": 0.2, "line_ending": "none",
        "terminator_regex": "DONE"}), name="ai-operation", daemon=True)
    worker.start()
    _wait_until(lambda: _status(lab)["lease"]["state"] == "active")

    term.type(b"HUMAN")
    assert b"HUMAN" in lab.device_read_until(b"HUMAN")
    # The human write neither waited for the operation nor disturbed the lease.
    assert _status(lab)["lease"]["state"] == "active"
    assert worker.is_alive()

    lab.device_write(b"reply DONE\r\n")  # the device answers and ends the AI operation
    worker.join(timeout=10)
    assert not worker.is_alive()
    assert b"reply" in _collect_output(observer, b"reply")
    term.read_until(b"reply")
    assert term.proc.poll() is None


def test_ai_client_exit_leaves_the_human_terminal_and_the_device_running(lab):
    ai_a = lab.harness.client(name="ai-a")
    lab.connect_device(ai_a)
    lab.harness.client(name="ai-b")
    term = lab.terminal()

    ai_a.close()
    lab.harness.clients.remove(ai_a)
    _wait_until(lambda: _status(lab)["clients"] == 2)
    _wait_until(lambda: _status(lab)["lease"]["state"] == "free")

    lab.device_write(b"still here\r\n")
    term.read_until(b"still here")
    term.type(b"x")
    assert b"x" in lab.device_read_until(b"x")
    assert _status(lab)["device"] is not None
    assert term.proc.poll() is None


def test_a_stalled_human_terminal_does_not_stall_the_daemon_or_ai_clients(lab):
    ai = lab.harness.client(name="ai-a")
    lab.connect_device(ai)
    term = lab.terminal()

    payload = (b"0123456789abcdef" * 65536) + b"END-OF-STREAM"  # 1 MiB, then a marker
    writer = threading.Thread(target=lambda: _write_all(lab.device_master, payload), daemon=True)
    writer.start()
    # The terminal's screen is not drained, so its output pump blocks. The daemon and the AI
    # client must keep answering regardless.
    ai.request("status", timeout=5)
    assert ai.call("tio_info", timeout=5).startswith("Connected")
    writer.join(timeout=30)
    assert not writer.is_alive()
    term.read_until(b"END-OF-STREAM", timeout=30)
    assert term.proc.poll() is None


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


# ---- slice 6: device-side compatibility (direct session socket, external tio)


def test_direct_session_socket_and_human_terminal_see_the_same_device_output(lab, tmp_path):
    ai = lab.harness.client(name="ai-a")
    lab.device = _pty_device(_silent_device)
    port = lab.device[2]
    session_path = str(tmp_path / "session.sock")
    assert ai.call("connect", {"port": port, "session_socket": session_path}).startswith("Connected")

    import socket as socket_mod
    tio = socket_mod.socket(socket_mod.AF_UNIX, socket_mod.SOCK_STREAM)
    tio.settimeout(10)
    tio.connect(session_path)
    try:
        term = lab.terminal()
        lab.device_write(b"both see this\r\n")
        term.read_until(b"both see this")
        seen = bytearray()
        while b"both see this" not in seen:
            seen.extend(tio.recv(4096))
        term.type(b"k")
        assert b"k" in lab.device_read_until(b"k")
    finally:
        tio.close()


def test_external_tio_owned_session_is_reachable_from_the_human_terminal(lab, tmp_path):
    from test_server import FakeSessionOwner

    owner_path = str(tmp_path / "owner.sock")
    owner = FakeSessionOwner(owner_path)
    try:
        agent = lab.harness.client(name="ai-a")
        assert agent.call("attach", {"attach_socket": owner_path}).startswith("Attached")
        term = lab.terminal()

        os.write(owner.device_slave, b"from tio owner\r\n")
        term.read_until(b"from tio owner")
        term.type(b"q")
        _wait_until(lambda: b"q" in bytes(owner.device_input))
        assert agent.call("disconnect").startswith("Detached")
        assert owner.alive
        assert term.proc.poll() is None
    finally:
        owner.close()


# ---- slice 7: faithful pass-through and a human crash -------------------------


def test_control_characters_and_escape_sequences_reach_the_device_unchanged(lab):
    lab.connect_device(lab.harness.client(name="ai-a"))
    term = lab.terminal()
    keys = b"\x03\x1a\x1c\x1b[A\x7f\r\t\x13\x11"  # Ctrl-C, Ctrl-Z, Ctrl-\\, Up, Backspace, CR, Tab, Ctrl-S, Ctrl-Q
    term.type(keys)
    assert keys in lab.device_read_until(keys)
    assert term.proc.poll() is None


def test_device_output_is_relayed_byte_for_byte(lab):
    lab.connect_device(lab.harness.client(name="ai-a"))
    term = lab.terminal()
    wire = bytes(range(256)) + b"\r\n\x1b[31mred\x1b[0m\r\n"
    lab.device_write(wire)
    term.read_until(b"red\x1b[0m\r\n")
    assert wire in term.output(), "device bytes must not be translated or interpreted"


def test_a_crashed_human_terminal_is_just_a_client_that_left(lab):
    ai_a = lab.harness.client(name="ai-a")
    lab.connect_device(ai_a)
    term = lab.terminal()
    term.proc.kill()  # no cleanup can run: this is the crash case
    term.proc.wait(timeout=10)
    _wait_until(lambda: _status(lab)["clients"] == 1)
    assert _status(lab)["device"] is not None
    assert _status(lab)["grace"]["active"] is False
    assert ai_a.call("tio_info", timeout=5).startswith("Connected")
    assert not lab.harness.daemon.stopped


# ---- slice 8: a real auto-started daemon outlives the terminal's exit ------


def test_terminal_auto_starts_the_daemon_and_the_daemon_outlives_its_exit(tmp_path, monkeypatch):
    from test_daemon_start import _daemon_pids_for, _wait_gone
    import signal as signal_mod

    runtime = tmp_path / "rt"
    env = {"SERIAL_MCP_RUNTIME_DIR": str(runtime), "SERIAL_MCP_DAEMON_LOG": str(tmp_path / "daemon.log")}
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(runtime))
    monkeypatch.setenv("SERIAL_MCP_DAEMON_LOG", str(tmp_path / "daemon.log"))
    term = TerminalProcess(runtime, env)
    try:
        term.read_until(BANNER_END, timeout=20)
        assert len(_daemon_pids_for(runtime)) == 1
        term.type(EXIT_KEY)
        assert term.wait() == 0
        # The daemon is still up after the human left. The only client left is this probe.
        probe = ipc.DaemonClient.connect(path=runtime / "daemon.sock", timeout=5)
        try:
            probe.hello(role="mcp", name="probe")
            status = probe.status()
            assert status["clients"] == 1
            assert status["grace"]["active"] is False
            assert status["device"] is None
        finally:
            probe.close()
        assert len(_daemon_pids_for(runtime)) == 1, "exiting the terminal must not stop the daemon"
    finally:
        term.close()
        for pid in _daemon_pids_for(runtime):
            os.kill(pid, signal_mod.SIGTERM)
        _wait_gone(_daemon_pids_for(runtime))


# ---- slice 9: packaging and import weight ------------------------------------


def test_terminal_module_imports_no_mcp_or_serial_stack():
    code = (
        "import sys, serial_mcp_terminal;"
        "heavy = [m for m in ('server', 'fastmcp', 'serial', 'serial_mcp_daemon') if m in sys.modules];"
        "print(heavy)"
    )
    out = subprocess.run([sys.executable, "-c", code], cwd=str(ROOT), capture_output=True, timeout=30, text=True)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "[]"


def test_packaging_ships_the_terminal_module_and_entry_point():
    import tomllib

    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert project["project"]["scripts"]["serial-mcp-terminal"] == "serial_mcp_terminal:main"
    assert project["project"]["scripts"]["serial-mcp"] == "serial_mcp_client:main"  # MCP frontend (serial-mcp/4)
    assert project["project"]["scripts"]["serial-mcp-legacy"] == "server:main"
    assert project["project"]["scripts"]["serial-mcp-daemon"] == "serial_mcp_daemon:main"
    assert "serial_mcp_terminal.py" in project["tool"]["hatch"]["build"]["targets"]["wheel"]["include"]
    flake = (ROOT / "flake.nix").read_text()
    assert "serial_mcp_terminal.py" in flake and "serial-mcp-terminal" in flake


# ---- slice 10: no device yet, then one appears ------------------------------


def test_terminal_without_a_device_says_so_and_picks_one_up_when_connected(lab):
    ai = lab.harness.client(name="ai-a")
    term = lab.terminal()
    assert b"no device is connected" in term.output()

    term.type(b"early\r")
    term.read_until(b"not sent: no device is connected")
    term.type(b"again\r")  # the same condition is reported once, not per keystroke
    lab.connect_device(ai)
    term.read_until(b"device connected")
    lab.device_write(b"hello late\r\n")
    term.read_until(b"hello late")
    assert term.output().count(b"not sent: no device is connected") == 1
    assert term.proc.poll() is None


# ---- slice 11: exit, lease release and device disconnect stay distinct -------


def test_human_exit_does_not_release_an_ai_lease_or_cancel_its_operation(lab):
    ai_a = lab.harness.client(name="ai-a")
    lab.connect_device(ai_a)
    term = lab.terminal()
    worker = threading.Thread(target=lambda: ai_a.call("send_command", {
        "text": "slow", "timeout": 20, "idle_timeout": 0.2, "line_ending": "none",
        "terminator_regex": "DONE"}), name="ai-operation", daemon=True)
    worker.start()
    _wait_until(lambda: _status(lab)["lease"]["state"] == "active")

    term.type(EXIT_KEY)
    assert term.wait() == 0
    _wait_until(lambda: _status(lab)["clients"] == 1)
    assert _status(lab)["lease"]["state"] == "active", "the AI still holds control"
    assert worker.is_alive()

    lab.device_write(b"DONE\r\n")
    worker.join(timeout=10)
    assert not worker.is_alive()
    _wait_until(lambda: _status(lab)["lease"]["state"] == "idle")


def test_an_explicit_device_disconnect_is_reported_and_keeps_the_terminal_running(lab):
    ai = lab.harness.client(name="ai-a")
    lab.connect_device(ai)
    term = lab.terminal()

    assert ai.call("disconnect").startswith("Disconnected")
    term.read_until(b"device disconnected")
    assert term.proc.poll() is None
    assert _status(lab)["device"] is None
    assert _status(lab)["clients"] == 2
    assert not lab.harness.daemon.stopped

    old_master, old_slave, _port, old_stop, old_thread = lab.device
    lab.device = None
    _release_device(old_master, old_slave, old_stop, old_thread)
    lab.connect_device(ai)  # a new device connection: the same terminal shows its output again
    lab.device_write(b"back online\r\n")
    term.read_until(b"back online")
    term.type(EXIT_KEY)
    assert term.wait() == 0
