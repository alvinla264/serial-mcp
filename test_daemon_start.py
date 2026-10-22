"""Race-safe discovery, auto-start, stale recovery and single-daemon guarantees.

These run real daemon subprocesses in private runtime directories (never the
user's). Waits are bounded polls on observable state, not fixed sleeps.
"""

import fcntl
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import serial_mcp_ipc as ipc

ROOT = Path(__file__).resolve().parent


def _daemon_pids_for(runtime: Path) -> list[int]:
    """Live processes started with this runtime directory (read from their environment)."""
    pids = []
    needle = f"SERIAL_MCP_RUNTIME_DIR={runtime}".encode()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/environ", "rb") as f:
                env = f.read().split(b"\0")
            with open(f"/proc/{entry}/cmdline", "rb") as f:
                cmd = f.read()
        except OSError:
            continue
        if needle in env and b"serial_mcp_daemon" in cmd:
            pids.append(int(entry))
    return pids


def _is_running(pid: int) -> bool:
    """True while the process exists and is not a zombie awaiting reap."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            state = f.read().rsplit(b")", 1)[1].split()[0]
    except (OSError, IndexError):
        return False
    return state != b"Z"


def _wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not reached within the bounded wait")


def _wait_gone(pids, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        alive = [p for p in pids if _is_running(p)]
        if not alive:
            return
        time.sleep(0.02)
    raise AssertionError(f"processes still running: {alive}")


@pytest.fixture()
def runtime(tmp_path, monkeypatch):
    rt = tmp_path / "rt"
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(rt))
    monkeypatch.setenv("SERIAL_MCP_DAEMON_LOG", str(tmp_path / "daemon.log"))
    yield rt
    for pid in _daemon_pids_for(rt):
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


def _ensure(**kwargs):
    return ipc.ensure_daemon(daemon_argv=[sys.executable, "-m", "serial_mcp_daemon"], **kwargs)


def test_first_client_auto_starts_one_daemon_and_is_connected(runtime):
    client = _ensure()
    try:
        client.hello(role="mcp", name="first")
        assert client.status()["protocol"] == 1
        assert len(_daemon_pids_for(runtime)) == 1
    finally:
        client.close()
        for pid in _daemon_pids_for(runtime):
            os.kill(pid, signal.SIGTERM)
        _wait_gone(_daemon_pids_for(runtime))


def test_simultaneous_first_clients_start_exactly_one_daemon(runtime):
    with ThreadPoolExecutor(max_workers=8) as pool:
        clients = list(pool.map(lambda _: _ensure(timeout=20), range(8)))
    try:
        for index, client in enumerate(clients):
            client.hello(role="mcp", name=f"racer-{index}")
        # Every client reached the same daemon, and losing starters that briefly
        # spawned exit on their own once the lock probe times out.
        assert all(c.status()["clients"] == 8 for c in clients)
        _wait_until(lambda: len(_daemon_pids_for(runtime)) == 1, timeout=10)
    finally:
        for client in clients:
            client.close()
        for pid in _daemon_pids_for(runtime):
            os.kill(pid, signal.SIGTERM)
        _wait_gone(_daemon_pids_for(runtime))


def test_stale_socket_file_is_recovered_under_the_lock(runtime):
    ipc.ensure_runtime_dir(runtime)
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(runtime / "daemon.sock"))
    stale.close()  # file remains; nothing listens
    assert (runtime / "daemon.sock").exists()

    client = _ensure()
    try:
        client.hello(role="mcp", name="after-stale")
        assert client.status()["protocol"] == 1
    finally:
        client.close()
        for pid in _daemon_pids_for(runtime):
            os.kill(pid, signal.SIGTERM)
        _wait_gone(_daemon_pids_for(runtime))


def test_a_held_lock_with_no_listener_is_bounded_and_spawns_nothing(runtime):
    ipc.ensure_runtime_dir(runtime)
    holder = os.open(runtime / "daemon.lock", os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        started = time.monotonic()
        with pytest.raises(ipc.DaemonUnavailable):
            _ensure(timeout=0.6)
        assert time.monotonic() - started < 5
        assert _daemon_pids_for(runtime) == []
    finally:
        os.close(holder)


def test_daemon_refuses_to_run_without_the_lock(runtime):
    ipc.ensure_runtime_dir(runtime)
    holder = os.open(runtime / "daemon.lock", os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "serial_mcp_daemon"], cwd=ROOT, capture_output=True, timeout=20,
        )
        assert proc.returncode == 0
        assert not (runtime / "daemon.sock").exists()
    finally:
        os.close(holder)


def test_unsafe_runtime_directory_is_refused_by_the_daemon(runtime):
    runtime.mkdir(mode=0o755)
    os.chmod(runtime, 0o755)
    proc = subprocess.run(
        [sys.executable, "-m", "serial_mcp_daemon"], cwd=ROOT, capture_output=True, timeout=20,
    )
    assert proc.returncode == 2
    assert b"accessible to other users" in proc.stderr


def test_sigterm_stops_the_daemon_and_removes_the_endpoint(runtime):
    client = _ensure()
    client.hello(role="mcp", name="term")
    client.close()
    pids = _daemon_pids_for(runtime)
    assert len(pids) == 1
    os.kill(pids[0], signal.SIGTERM)
    _wait_gone(pids)
    assert not (runtime / "daemon.sock").exists()
    # The lock is released, so a new daemon can start cleanly.
    again = _ensure()
    again.hello(role="mcp", name="again")
    again.close()
    for pid in _daemon_pids_for(runtime):
        os.kill(pid, signal.SIGTERM)
    _wait_gone(_daemon_pids_for(runtime))


def test_two_independent_clients_share_one_daemon_and_one_device_owner(runtime, tmp_path):
    """Two stdio-style clients use one daemon; exiting one leaves the other usable."""
    import pty

    master, slave = pty.openpty()
    port = os.ttyname(slave)
    # The daemon is another process and refuses a port this process still holds open.
    os.close(slave)
    stop = threading.Event()

    def echo():
        import select as sel
        while not stop.is_set():
            readable, _, _ = sel.select([master], [], [], 0.05)
            if master in readable:
                try:
                    data = os.read(master, 1024)
                except OSError:
                    # EIO while no slave is open (before the daemon connects); not an error.
                    time.sleep(0.01)
                    continue
                os.write(master, data.upper())

    worker = threading.Thread(target=echo, daemon=True)
    worker.start()
    first = _ensure()
    second = _ensure()
    try:
        first.hello(role="mcp", name="agent-one")
        second.hello(role="mcp", name="agent-two")
        assert len(_daemon_pids_for(runtime)) == 1, "both clients must reach the same daemon"

        assert first.call("connect", {"port": port, "session_socket": ""}).startswith("Connected")
        assert second.status()["device"].startswith(port)
        with pytest.raises(ipc.DaemonError) as err:
            second.call("send_command", {"text": "hi"})
        assert err.value.code == "busy"

        first.close()
        # Lease released by the exit (no operation was in flight): the other client takes over.
        deadline = time.monotonic() + 10
        while True:
            try:
                reply = second.call("send_command", {"text": "ping", "timeout": 2, "idle_timeout": 0.2})
                break
            except ipc.DaemonError as exc:
                if exc.code != "busy" or time.monotonic() > deadline:
                    raise
        assert reply.strip() == "PING"
        assert second.call("disconnect").startswith("Disconnected")
    finally:
        stop.set()
        worker.join(timeout=2)
        second.close()
        os.close(master)
        for pid in _daemon_pids_for(runtime):
            os.kill(pid, signal.SIGTERM)
        _wait_gone(_daemon_pids_for(runtime))


def test_a_second_daemon_cannot_take_over_a_live_endpoint(runtime):
    """A second daemon started while the first holds daemon.lock must exit without touching the socket."""
    client = _ensure()
    try:
        client.hello(role="mcp", name="incumbent")
        before = os.stat(runtime / "daemon.sock").st_ino
        proc = subprocess.run(
            [sys.executable, "-m", "serial_mcp_daemon"], cwd=ROOT, env=dict(os.environ),
            stdin=subprocess.DEVNULL, capture_output=True, timeout=30,
        )
        assert proc.returncode == 0, proc.stderr
        assert os.stat(runtime / "daemon.sock").st_ino == before, "the live socket was replaced"
        assert client.status()["protocol"] == 1, "the incumbent daemon stopped answering"
        assert len(_daemon_pids_for(runtime)) == 1
    finally:
        client.close()
        for pid in _daemon_pids_for(runtime):
            os.kill(pid, signal.SIGTERM)
        _wait_gone(_daemon_pids_for(runtime))


def test_daemon_start_does_not_truncate_the_legacy_log(runtime, tmp_path, monkeypatch):
    """Importing server must not wipe the legacy stdio launcher's log (/tmp/serial-mcp.log by default)."""
    legacy_log = tmp_path / "legacy-serial-mcp.log"
    legacy_log.write_text("legacy history line\n", encoding="utf-8")
    monkeypatch.setenv("SERIAL_MCP_LOG", str(legacy_log))
    monkeypatch.delenv("SERIAL_MCP_DAEMON_LOG", raising=False)  # exercise the default runtime log
    client = _ensure()
    try:
        client.hello(role="mcp", name="log-check")
        assert legacy_log.read_text(encoding="utf-8") == "legacy history line\n"
        assert (runtime / "daemon.log").exists(), "the daemon writes its own log in its runtime directory"
    finally:
        client.close()
        for pid in _daemon_pids_for(runtime):
            os.kill(pid, signal.SIGTERM)
        _wait_gone(_daemon_pids_for(runtime))
