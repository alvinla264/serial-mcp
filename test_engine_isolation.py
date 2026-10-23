"""Import and startup invariants of the serial engine and the shared daemon.

Every check runs in a fresh subprocess, so nothing a previous test imported
(``server``, FastMCP, a log file, a daemon) can hide a side effect. The
framework is forbidden by setting ``sys.modules[name] = None`` for ``fastmcp``,
``mcp`` and ``server``: any import of them raises ImportError, so a daemon that
still reaches for the legacy module fails here instead of passing by accident.

Devices are ptys and the daemon runs on a private runtime directory. Nothing
touches real hardware or the owner's live socket.
"""

import json
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

REPO = Path(__file__).resolve().parent
BLOCKED = ("fastmcp", "mcp", "server")

BLOCK_PREAMBLE = (
    "import sys\n"
    f"for _name in {BLOCKED!r}:\n"
    "    sys.modules[_name] = None\n"
)


def _child_env(extra: dict) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "SERIAL_MCP_LOG")}
    env["PYTHONPATH"] = str(REPO)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra)
    return env


def _run_child(code: str, env_extra: dict | None = None, timeout: float = 60) -> dict:
    proc = subprocess.run(
        [sys.executable, "-s", "-B", "-c", code],
        cwd=REPO, env=_child_env(env_extra or {}), capture_output=True, text=True, timeout=timeout,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


# ---- engine import ----------------------------------------------------------


def test_engine_import_is_side_effect_free(tmp_path):
    log = tmp_path / "engine.log"
    code = (
        "import json, sys, threading\n"
        "import serial_mcp_engine\n"
        "print(json.dumps({\n"
        "    'threads': threading.active_count(),\n"
        "    'fastmcp': 'fastmcp' in sys.modules,\n"
        "    'mcp': any(m == 'mcp' or m.startswith('mcp.') for m in sys.modules),\n"
        "    'server': 'server' in sys.modules,\n"
        "    'has_instance': any(\n"
        "        isinstance(v, serial_mcp_engine.SerialState) for v in vars(serial_mcp_engine).values()),\n"
        "}))\n"
    )
    result = _run_child(code, {"SERIAL_MCP_LOG": str(log)})
    assert result == {"threads": 1, "fastmcp": False, "mcp": False, "server": False, "has_instance": False}
    assert not log.exists(), "importing the engine must not truncate or create the log"


def test_daemon_import_loads_no_mcp_framework_and_creates_no_engine(tmp_path):
    log = tmp_path / "daemon.log"
    code = (
        "import json, sys, threading\n"
        "import serial_mcp_daemon\n"
        "import serial_mcp_engine\n"
        "print(json.dumps({\n"
        "    'threads': threading.active_count(),\n"
        "    'fastmcp': 'fastmcp' in sys.modules,\n"
        "    'mcp': any(m == 'mcp' or m.startswith('mcp.') for m in sys.modules),\n"
        "    'server': 'server' in sys.modules,\n"
        "    'daemon_state': serial_mcp_daemon.DaemonServer(runtime=None).state is None,\n"
        "}))\n"
    )
    result = _run_child(code, {"SERIAL_MCP_LOG": str(log), "SERIAL_MCP_RUNTIME_DIR": str(tmp_path / "rt")})
    assert result == {"threads": 1, "fastmcp": False, "mcp": False, "server": False, "daemon_state": True}
    assert not log.exists()
    assert not (tmp_path / "rt").exists(), "constructing the daemon must not create its runtime dir"


# ---- daemon startup and IPC without the framework ---------------------------

# Bootstrap for the real entry point: block the framework, count engine
# construction, run ``serial_mcp_daemon.main`` as ``python -m`` would, and record
# what happened when the process exits.
BOOTSTRAP = BLOCK_PREAMBLE + (
    "import atexit, json, os, runpy\n"
    "import serial_mcp_engine as _engine\n"
    "_constructed = []\n"
    "_orig_init = _engine.SerialState.__init__\n"
    "def _counting_init(self, *a, **k):\n"
    "    _constructed.append(type(self).__module__)\n"
    "    _orig_init(self, *a, **k)\n"
    "_engine.SerialState.__init__ = _counting_init\n"
    "import serial_mcp_daemon\n"
    "_after_import = len(_constructed)\n"
    "def _record():\n"
    "    with open(os.environ['RECORD_PATH'], 'w') as f:\n"
    "        json.dump({'after_import': _after_import, 'constructed': _constructed,\n"
    "                   'fastmcp': sys.modules.get('fastmcp') is not None}, f)\n"
    "atexit.register(_record)\n"
    "runpy.run_module('serial_mcp_daemon', run_name='__main__')\n"
)


class FakeDevice:
    """A pty that answers PING with a short reply followed by END."""

    def __init__(self) -> None:
        self.master, slave = pty.openpty()
        self.port = os.ttyname(slave)
        os.close(slave)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                ready, _, _ = select.select([self.master], [], [], 0.05)
                if ready and b"PING" in os.read(self.master, 4096):
                    os.write(self.master, b"pong\r\nEND\r\n")
            except OSError:
                time.sleep(0.01)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        os.close(self.master)


@pytest.fixture()
def daemon_run(tmp_path):
    """Start the real daemon entry point with the framework blocked; yield (runtime, record path, proc)."""
    rt = tmp_path / "rt"
    record = tmp_path / "record.json"
    env = _child_env({
        "SERIAL_MCP_RUNTIME_DIR": str(rt),
        "SERIAL_MCP_DAEMON_LOG": str(tmp_path / "daemon.log"),
        "RECORD_PATH": str(record),
    })
    proc = subprocess.Popen(
        [sys.executable, "-s", "-B", "-c", BOOTSTRAP], cwd=REPO, env=env,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    deadline = time.monotonic() + 30
    ready = False
    while time.monotonic() < deadline and proc.poll() is None:
        try:
            probe = ipc.DaemonClient.connect(path=rt / "daemon.sock", timeout=0.2)
            probe.close()
            ready = True
            break
        except Exception:
            time.sleep(0.02)
    if not ready:
        proc.kill()
        _, err = proc.communicate(timeout=10)
        pytest.fail(f"daemon did not start with the framework blocked: {err[-2000:]}")
    yield rt, record, proc
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        pytest.fail("daemon did not stop on SIGTERM")


def test_daemon_entry_point_starts_and_serves_without_mcp_framework(daemon_run):
    rt, record, proc = daemon_run
    device = FakeDevice()
    mcp = ipc.DaemonClient.connect(path=rt / "daemon.sock", timeout=5)
    human = None
    try:
        hello = mcp.hello(role="mcp", name="ai-test")
        assert hello["role"] == "mcp"
        assert mcp.status()["device"] is None

        assert "not connected" in mcp.call("tio_info", {}).lower()
        assert mcp.call("file_transfer_capabilities", {}) == _engine_caps()

        connected = mcp.call("connect", {"port": device.port, "session_socket": ""}, timeout=30)
        assert "Connected" in connected
        reply = mcp.call("send_command", {"text": "PING", "terminator_regex": "END"}, timeout=30)
        assert "pong" in reply and "END" in reply
        assert "PING" in mcp.call("view_io", {"lines": 20})

        # The connect call acquired the lease for this client; release, then re-acquire.
        released = mcp.lease("release")
        assert released["state"] == "free" and released["holder"] is None
        acquired = mcp.lease("acquire")
        assert acquired["holder"]["client_id"] == hello["client_id"]
        assert acquired["you"] == "holder"

        human = ipc.DaemonClient.connect(path=rt / "daemon.sock", timeout=5)
        human.hello(role="human", name="term-test")
        assert human.human_input(b"PING\n") == {"written": 5}

        mcp.call("disconnect", {}, timeout=30)
        assert mcp.status()["device"] is None
    finally:
        if human is not None:
            human.close()
        mcp.close()
        device.close()

    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 0
    data = json.loads(record.read_text())
    assert data["fastmcp"] is False
    assert data["constructed"] == ["serial_mcp_engine"], "exactly one engine must be created, by the daemon"
    assert data["after_import"] == 0, "importing the daemon must not construct an engine"


def _engine_caps() -> str:
    import serial_mcp_engine
    return serial_mcp_engine.FILE_TRANSFER_CAPABILITIES


# ---- legacy adapter compatibility and packaging ----------------------------


def test_legacy_server_reexports_engine_objects():
    import server
    import serial_mcp_engine as engine

    assert server.SerialState is engine.SerialState
    assert server.FILE_TRANSFER_CAPABILITIES is engine.FILE_TRANSFER_CAPABILITIES
    assert server.LINE_ENDINGS is engine.LINE_ENDINGS
    assert server.DirectTransport is engine.DirectTransport
    assert server.SocketTransport is engine.SocketTransport


def test_packaging_ships_python_entry_points_without_the_native_frontend_shim():
    tomllib = pytest.importorskip("tomllib")
    data = tomllib.loads((REPO / "pyproject.toml").read_text())
    included = set(data["tool"]["hatch"]["build"]["targets"]["wheel"]["include"])
    for module in ("server.py", "serial_mcp_ipc.py", "serial_mcp_daemon.py",
                   "serial_mcp_terminal.py", "serial_mcp_engine.py"):
        assert module in included, f"{module} missing from the wheel include list"
    assert "serial_mcp_client.py" not in included
    assert "serial-mcp" not in data["project"]["scripts"]
    assert data["build-system"]["build-backend"] == "serial_mcp_build"
    flake = (REPO / "flake.nix").read_text()
    assert "serial_mcp_engine.py" in flake, "flake installPhase must copy the engine module"
