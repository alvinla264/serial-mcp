"""Tests for the Python parity oracle and shared daemon.

The public seam is MCP over stdio: each oracle process is driven by a FastMCP
client. The daemon is either in-process with an injected clock (lease and grace
are advanced explicitly, never slept) or a real daemon subprocess (auto-start,
crash and race behaviour). Devices are ptys and fake-daemon peers are scripted
UNIX sockets; no hardware is touched. Native frontend coverage is in
``test_rust_mcp_client.py``.
"""

import asyncio
import json
import os
import pty
import signal
import socket
import subprocess
import sys
import threading
import time
import tty
from pathlib import Path

import pytest
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

import serial_mcp_ipc as ipc
import serial_mcp_daemon as daemon_mod
from serial_mcp_daemon import DaemonServer
from serial_mcp_engine import SerialState
import server as legacy

ROOT = Path(__file__).resolve().parent
FRONTEND = ROOT / "serial_mcp_client.py"
LEASE_TOOLS = {"lease_acquire", "lease_release", "lease_status"}
SCENARIO_TIMEOUT = 90


# ---- helpers ----------------------------------------------------------------


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class InProcessDaemon:
    """The daemon in this process, on a private runtime directory with a fake clock."""

    def __init__(self, tmp_path: Path) -> None:
        self.clock = FakeClock()
        self.runtime = tmp_path / "rt"
        self.state = SerialState(log_path=str(tmp_path / "daemon.log"))
        self.daemon = DaemonServer(runtime=self.runtime, clock=self.clock, state=self.state,
                                   start_housekeeping=False)
        self.daemon.start()

    def close(self) -> None:
        self.daemon.stop(reason="test")
        self.state.disconnect()


@pytest.fixture()
def daemon(tmp_path, monkeypatch):
    monkeypatch.setenv("SERIAL_MCP_LOG", str(tmp_path / "unused-legacy.log"))
    d = InProcessDaemon(tmp_path)
    yield d
    d.close()


def frontend(runtime: Path, name: str, **extra_env) -> Client:
    """A real frontend process, spoken to over MCP stdio, with an explicit environment."""
    env = {"SERIAL_MCP_RUNTIME_DIR": str(runtime), "SERIAL_MCP_CLIENT_NAME": name}
    env.update(extra_env)
    transport = StdioTransport(command=sys.executable, args=[str(FRONTEND)], env=env, cwd=str(ROOT),
                               keep_alive=False)  # the process must end when the client context exits
    return Client(transport)


def text_of(result) -> str:
    return "".join(getattr(item, "text", "") for item in result.content)


async def call(client: Client, tool: str, args: dict | None = None):
    return await client.call_tool(tool, args or {}, raise_on_error=False)


async def wait_for(predicate, timeout: float = 10.0):
    """Bounded poll of observable daemon state (not a fixed sleep)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached within the bounded wait")


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, SCENARIO_TIMEOUT))


def _listed(mcp_obj):
    if hasattr(mcp_obj, "get_tools"):  # fastmcp 2.x
        tools = asyncio.run(mcp_obj.get_tools())
        return tools if isinstance(tools, dict) else {t.name: t for t in tools}
    return {t.name: t for t in asyncio.run(mcp_obj.list_tools())}  # fastmcp 3.x+


def _schema(tool):
    return getattr(tool, "parameters", None) or getattr(tool, "inputSchema", {})


def _pty_device(handler):
    """A fake device: the master end is driven by ``handler(master, stop)`` in a thread."""
    master, slave = pty.openpty()
    tty.setraw(slave)
    stop = threading.Event()

    def loop():
        try:
            handler(master, stop)
        finally:
            stop.set()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return master, slave, os.ttyname(slave), stop, thread


def _release_device(master, slave, stop, thread):
    stop.set()
    thread.join(timeout=2)
    os.close(master)
    os.close(slave)


def _echo_upper(master, stop):
    import select

    while not stop.is_set():
        readable, _, _ = select.select([master], [], [], 0.05)
        if master in readable:
            os.write(master, os.read(master, 1024).upper())


@pytest.fixture()
def echo_device():
    device = _pty_device(_echo_upper)
    yield device
    _release_device(*device[:2], device[3], device[4])


def _daemon_pids_for(runtime: Path) -> list[int]:
    """Live daemon processes started with this runtime directory (from their environment)."""
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


def _frontend_pids_named(name: str) -> list[int]:
    pids = []
    needle = f"SERIAL_MCP_CLIENT_NAME={name}".encode()
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
        if needle in env and b"serial_mcp_client.py" in cmd:
            pids.append(int(entry))
    return pids


@pytest.fixture()
def real_runtime(tmp_path, monkeypatch):
    """A runtime directory served by real daemon subprocesses; any leftovers are stopped."""
    rt = tmp_path / "rt"
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(rt))
    yield rt
    for pid in _daemon_pids_for(rt):
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


# ---- the tool surface -------------------------------------------------------


def test_tool_surface_mirrors_the_legacy_server():
    """Same names, parameters, defaults and descriptions as server.py, plus the lease tools.

    The frontend cannot import server.py (that constructs device state and truncates the
    legacy log), so its tool definitions are a copy. This test is the drift guard.
    """
    import serial_mcp_client as frontend_module

    legacy_tools = _listed(legacy.mcp)
    ours = _listed(frontend_module.mcp)
    assert LEASE_TOOLS <= set(ours)
    assert set(ours) - LEASE_TOOLS == set(legacy_tools)
    assert not (LEASE_TOOLS & set(legacy_tools))
    for name, legacy_tool in legacy_tools.items():
        assert _schema(ours[name]) == _schema(legacy_tool), f"{name}: parameters differ"
        assert ours[name].description == legacy_tool.description, f"{name}: description differs"


def test_gating_matches_the_daemon_contract_table():
    import serial_mcp_client as frontend_module

    gated = {name for name, (_m, is_gated) in daemon_mod.TOOL_TABLE.items() if is_gated}
    ungated = {name for name, (_m, is_gated) in daemon_mod.TOOL_TABLE.items() if not is_gated}
    assert frontend_module.GATED_TOOLS == gated
    assert set(frontend_module.GATED_TOOLS).isdisjoint(ungated)
    assert gated | ungated == set(_listed(legacy.mcp))


def test_default_max_bytes_matches_the_legacy_limit():
    import serial_mcp_client as frontend_module

    for name in ("upload_file", "download_file"):
        default = _schema(_listed(frontend_module.mcp)[name])["properties"]["max_bytes"]["default"]
        assert default == SerialState.MAX_FILE_TRANSFER_BYTES


def test_console_scripts_and_wheel_include_every_module_they_name():
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    included = set(project["tool"]["hatch"]["build"]["targets"]["wheel"]["include"])
    scripts = project["project"]["scripts"]
    assert scripts["serial-mcp"] == "serial_mcp_launcher:main"
    assert "serial_mcp_launcher.py" in included
    assert scripts["serial-mcp-legacy"] == "server:main"
    assert scripts["serial-mcp-daemon"] == "serial_mcp_daemon:main"
    for target in scripts.values():
        module = target.split(":")[0]
        assert f"{module}.py" in included, f"wheel does not include {module}.py"
    assert project["build-system"]["build-backend"] == "serial_mcp_build"
    assert "serial_mcp_client.py" not in included
    sdist_include = set(project["tool"]["hatch"]["build"]["targets"]["sdist"]["include"])
    assert "serial_mcp_client.py" not in sdist_include
    assert "serial_mcp_launcher.py" in sdist_include


def test_rust_tool_catalog_is_generated_from_the_python_mcp_oracle():
    result = subprocess.run([sys.executable, str(ROOT / "tools" / "export_tool_catalog.py"), "--check"],
                            cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr or result.stdout


def test_importing_the_frontend_starts_no_device_state_daemon_or_log(tmp_path):
    """The frontend is light: no SerialState, no pyserial, no daemon module, no log, no runtime dir."""
    log = tmp_path / "legacy.log"
    runtime = tmp_path / "rt"
    env = dict(os.environ, SERIAL_MCP_LOG=str(log), SERIAL_MCP_RUNTIME_DIR=str(runtime))
    code = (
        "import sys, serial_mcp_client\n"
        "loaded = sorted(n for n in ('server', 'serial', 'serial_mcp_daemon') if n in sys.modules)\n"
        "print(loaded)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], cwd=str(ROOT), env=env,
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "[]"
    assert not log.exists()
    assert not runtime.exists()


# ---- forwarding fidelity ----------------------------------------------------


def test_results_and_errors_are_forwarded_unchanged(daemon, echo_device, tmp_path):
    master, slave, port, _stop, _thread = echo_device

    async def scenario():
        async with frontend(daemon.runtime, "fidelity") as client:
            caps = await call(client, "file_transfer_capabilities")
            assert not caps.is_error
            assert text_of(caps) == legacy.FILE_TRANSFER_CAPABILITIES

            # A tool that reports a condition as text is a successful result, as in the legacy server.
            not_connected = await call(client, "send_command", {"text": "x"})
            assert not not_connected.is_error
            assert text_of(not_connected).startswith("Error: Not connected")

            connected = await call(client, "connect", {"port": port, "session_socket": ""})
            assert not connected.is_error, text_of(connected)

            echoed = await call(client, "send_command", {"text": "abc", "timeout": 3, "idle_timeout": 0.2})
            assert not echoed.is_error
            assert "ABC" in text_of(echoed)

            bad = await call(client, "read_output", {"max_lines": "not-a-number"})
            assert bad.is_error  # schema validation is the MCP layer's, unchanged

    run(scenario())


def test_lease_tools_report_json_and_warnings(daemon, echo_device):
    _master, _slave, port, _stop, _thread = echo_device

    async def scenario():
        async with frontend(daemon.runtime, "json") as client:
            status = await call(client, "lease_status")
            view = json.loads(text_of(status))
            assert view["state"] == "free"
            assert view["you"] == "none"
            connected = await call(client, "connect", {"port": port, "session_socket": ""})
            assert not connected.is_error
            held = json.loads(text_of(await call(client, "lease_status")))
            assert held["you"] == "holder"
            assert held["holder"]["name"] == "json"
            released = json.loads(text_of(await call(client, "lease_release")))
            assert released["you"] == "reacquire_required"
            reacquired = await call(client, "lease_acquire")
            assert not reacquired.is_error
            payload = json.loads(text_of(reacquired))
            assert "warning" in payload
            assert "device state may have changed" in payload["warning"]

    run(scenario())


# ---- two clients: identity, conflicts, expiry, reacquisition ----------------


def test_two_frontends_share_one_daemon_and_control_expires_on_the_daemon_clock(daemon, echo_device):
    _master, _slave, port, _stop, _thread = echo_device

    async def scenario():
        async with frontend(daemon.runtime, "alpha") as alpha, frontend(daemon.runtime, "bravo") as bravo:
            assert not (await call(alpha, "connect", {"port": port, "session_socket": ""})).is_error

            refused = await call(bravo, "send_command", {"text": "x"})
            assert refused.is_error
            assert "busy" in text_of(refused) and "alpha" in text_of(refused)

            holder = json.loads(text_of(await call(bravo, "lease_status")))
            assert holder["holder"]["name"] == "alpha"
            assert (await call(bravo, "lease_acquire")).is_error  # busy, not granted

            # Observation never renews control: bravo's read-only view at +30 s does not extend it.
            daemon.clock.advance(30)
            assert not (await call(bravo, "view_io")).is_error
            daemon.clock.advance(31)
            daemon.daemon.tick()

            # The former holder is told, on its next reply, that control expired.
            alpha_view = json.loads(text_of(await call(alpha, "lease_status")))
            assert any("device state may have changed" in note for note in alpha_view["notices"])
            assert alpha_view["you"] == "reacquire_required"

            # No silent resumption, even though the lease is free now.
            silent = await call(alpha, "send_command", {"text": "x"})
            assert silent.is_error and "lease_lost" in text_of(silent)

            # bravo takes control; alpha cannot take it back while bravo holds it.
            took = await call(bravo, "send_command", {"text": "b", "timeout": 3, "idle_timeout": 0.2})
            assert not took.is_error and "B" in text_of(took)
            assert (await call(alpha, "lease_acquire")).is_error

            # bravo releases; alpha reacquires explicitly and is warned.
            await call(bravo, "lease_release")
            reacquired = await call(alpha, "lease_acquire")
            assert not reacquired.is_error
            assert "warning" in json.loads(text_of(reacquired))
            again = await call(alpha, "send_command", {"text": "a", "timeout": 3, "idle_timeout": 0.2})
            assert not again.is_error and "A" in text_of(again)

    run(scenario())


# ---- client exit, crash and explicit disconnect -----------------------------


def test_client_exit_releases_control_without_tearing_down_the_device(daemon, echo_device):
    _master, _slave, port, _stop, _thread = echo_device

    async def scenario():
        async with frontend(daemon.runtime, "leaver") as leaver:
            assert not (await call(leaver, "connect", {"port": port, "session_socket": ""})).is_error
        # leaver has exited cleanly; its socket closed.
        await wait_for(lambda: daemon.daemon.status()["clients"] == 0)
        async with frontend(daemon.runtime, "stayer") as stayer:
            assert not (await call(stayer, "send_command", {"text": "s", "timeout": 3, "idle_timeout": 0.2})).is_error
            assert daemon.state._is_connected()  # device still owned by the daemon

    run(scenario())


def test_killed_client_is_cleaned_up_like_an_exit(daemon, echo_device):
    _master, _slave, port, _stop, _thread = echo_device

    async def scenario():
        victim = frontend(daemon.runtime, "crashy")
        await victim.__aenter__()
        assert not (await call(victim, "connect", {"port": port, "session_socket": ""})).is_error
        pids = _frontend_pids_named("crashy")
        assert pids, "frontend process not found"
        for pid in pids:
            os.kill(pid, signal.SIGKILL)
        await wait_for(lambda: daemon.daemon.status()["clients"] == 0)
        async with frontend(daemon.runtime, "successor") as successor:
            ok = await call(successor, "send_command", {"text": "n", "timeout": 3, "idle_timeout": 0.2})
            assert not ok.is_error and "N" in text_of(ok)

    run(scenario())


def test_explicit_disconnect_releases_the_transport_and_the_daemon_keeps_running(daemon, echo_device):
    _master, _slave, port, _stop, _thread = echo_device

    async def scenario():
        async with frontend(daemon.runtime, "operator") as client:
            assert not (await call(client, "connect", {"port": port, "session_socket": ""})).is_error
            gone = await call(client, "disconnect")
            assert not gone.is_error
            assert not daemon.state._is_connected()
            assert not daemon.daemon.stopped
            back = await call(client, "connect", {"port": port, "session_socket": ""})
            assert not back.is_error
            assert daemon.state._is_connected()

    run(scenario())


def test_external_tio_attach_survives_the_frontend_exiting(daemon, tmp_path):
    session = tmp_path / "tio-like.sock"

    def pong(master, stop):
        import select

        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable and b"ping" in os.read(master, 1024):
                os.write(master, b"PONG\n")

    master, slave, port, stop, thread = _pty_device(pong)
    try:
        async def scenario():
            async with frontend(daemon.runtime, "owner") as owner:
                connected = await call(owner, "connect", {"port": port, "session_socket": str(session)})
                assert not connected.is_error, text_of(connected)
            # The owner has exited. The daemon still owns the device and serves the session socket.
            assert session.exists()
            return True

        assert run(scenario())
        tio = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        tio.settimeout(5)
        tio.connect(str(session))
        try:
            tio.sendall(b"ping\n")
            seen = b""
            deadline = time.monotonic() + 5
            while b"PONG" not in seen and time.monotonic() < deadline:
                seen += tio.recv(1024)
            assert b"PONG" in seen
        finally:
            tio.close()
    finally:
        _release_device(master, slave, stop, thread)


# ---- lifetime -----------------------------------------------------------------


def test_idle_client_stays_registered_and_its_exit_starts_the_grace_period(daemon):
    async def scenario():
        async with frontend(daemon.runtime, "idle") as idle:
            assert not (await call(idle, "tio_info")).is_error
            daemon.clock.advance(300)  # far beyond both lease idle time and the grace period
            daemon.daemon.tick()
            assert not daemon.daemon.stopped
            assert daemon.daemon.status()["clients"] == 1
            assert not (await call(idle, "tio_info")).is_error
        await wait_for(lambda: daemon.daemon.status()["clients"] == 0)
        assert daemon.daemon.status()["grace"]["active"]

    run(scenario())
    daemon.clock.advance(ipc.GRACE_SECONDS)
    daemon.daemon.tick()
    assert daemon.daemon.stopped


# ---- human input is not gated by AI control --------------------------------


def test_human_input_is_forwarded_while_an_ai_operation_is_in_flight(daemon):
    got_trigger = threading.Event()
    seen = bytearray()

    def gate(master, stop):
        import select

        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master in readable:
                seen.extend(os.read(master, 1024))
                if b"need-human" in seen:
                    got_trigger.set()
                if got_trigger.is_set() and b"H" in seen[seen.index(b"need-human") + len(b"need-human"):]:
                    os.write(master, b"DONE\n")
                    return

    master, slave, port, stop, thread = _pty_device(gate)
    human = None
    try:
        async def scenario():
            nonlocal human
            async with frontend(daemon.runtime, "ai") as ai:
                assert not (await call(ai, "connect", {"port": port, "session_socket": ""})).is_error
                operation = asyncio.ensure_future(
                    call(ai, "send_command", {"text": "need-human", "terminator_regex": "DONE",
                                              "timeout": 15, "idle_timeout": 5}))
                assert await asyncio.to_thread(got_trigger.wait, 10), "device never saw the AI command"
                human = ipc.DaemonClient.connect(path=daemon.runtime / "daemon.sock", timeout=5)
                human.hello(role="human", name="operator", pid=os.getpid())
                written = human.human_input(b"H")
                assert written == {"written": 1}
                result = await operation
                assert not result.is_error
                assert "DONE" in text_of(result)

        run(scenario())
    finally:
        if human is not None:
            human.close()
        _release_device(master, slave, stop, thread)


# ---- real daemon subprocess: auto-start, races, crash, failures ---------------


def test_simultaneous_frontends_auto_start_exactly_one_daemon(real_runtime):
    async def scenario():
        clients = [frontend(real_runtime, f"racer{i}") for i in range(3)]
        for client in clients:
            await client.__aenter__()
        try:
            results = await asyncio.gather(*(call(c, "lease_status") for c in clients))
            assert all(not r.is_error for r in results), [text_of(r) for r in results]
            assert len(_daemon_pids_for(real_runtime)) == 1
        finally:
            for client in clients:
                await client.__aexit__(None, None, None)

    run(scenario())


def test_killed_daemon_is_reported_and_control_needs_explicit_reacquisition(real_runtime, echo_device):
    _master, _slave, port, _stop, _thread = echo_device

    async def scenario():
        async with frontend(real_runtime, "survivor") as client:
            assert not (await call(client, "connect", {"port": port, "session_socket": ""})).is_error
            pids = _daemon_pids_for(real_runtime)
            assert len(pids) == 1
            for pid in pids:
                os.kill(pid, signal.SIGKILL)
            await wait_for(lambda: not _daemon_pids_for(real_runtime))

            lost = await call(client, "send_command", {"text": "x"})
            assert lost.is_error
            assert "not retried" in text_of(lost) or "lease_lost" in text_of(lost)
            # The new daemon would grant control to a fresh client. The frontend refuses instead.
            refused_again = await call(client, "send_command", {"text": "x"})
            assert refused_again.is_error and "lease_lost" in text_of(refused_again)

            reacquired = await call(client, "lease_acquire")
            assert not reacquired.is_error, text_of(reacquired)
            assert "device state may have changed" in json.loads(text_of(reacquired))["warning"]
            fresh = await call(client, "send_command", {"text": "x"})
            assert not fresh.is_error
            assert "Not connected" in text_of(fresh)  # the new daemon has no device yet

    run(scenario())


def test_runtime_directory_refusal_is_reported_by_the_tool(tmp_path):
    runtime = tmp_path / "rt"
    runtime.mkdir(mode=0o755)  # other users could reach a daemon here; the frontend refuses

    async def scenario():
        async with frontend(runtime, "unsafe") as client:
            refused = await call(client, "lease_status")
            assert refused.is_error
            assert "runtime directory" in text_of(refused)
            assert "accessible to other users" in text_of(refused)

    run(scenario())


# ---- scripted fake daemons: version mismatch and non-idempotent calls -------


class ScriptedDaemon:
    """Serves the frozen socket path and answers each connection through ``handle(conn, frame)``.

    ``handle`` returns False to close the connection. Frames are recorded in ``frames``.
    """

    def __init__(self, runtime: Path, handle) -> None:
        runtime.mkdir(mode=0o700, exist_ok=True)
        self.handle = handle
        self.frames: list[dict] = []
        self.connections = 0
        self._stop = threading.Event()
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(runtime / "daemon.sock"))
        self._listener.listen(8)
        self._listener.settimeout(0.1)
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.connections += 1
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        reader = ipc.FrameReader(conn)
        try:
            while True:
                frame = reader.read_frame()
                if frame is None:
                    return
                self.frames.append(frame)
                if not self.handle(conn, frame):
                    return
        except (ipc.ProtocolError, OSError):
            return
        finally:
            conn.close()

    def close(self) -> None:
        self._stop.set()
        self._listener.close()


def _reply(conn, frame, result=None, *, version=1):
    body = {"v": version, "id": frame["id"], "ok": True, "result": result or {}}
    conn.sendall((json.dumps(body) + "\n").encode())


def _hello_result(protocol=1):
    return {"protocol": protocol, "daemon_version": "scripted", "client_id": "c1", "role": "mcp",
            "lease": {"state": "free", "holder": None, "expires_in": None, "active_operations": 0,
                      "generation": 0, "you": "none"}}


def test_newer_daemon_protocol_is_refused_with_the_reason(tmp_path):
    runtime = tmp_path / "rt"

    def speak_v2(conn, frame):
        conn.sendall(b'{"v":2,"id":' + str(frame["id"]).encode() + b',"ok":true,"result":{}}\n')
        return False

    fake = ScriptedDaemon(runtime, speak_v2)

    async def scenario():
        async with frontend(runtime, "mismatch") as client:
            refused = await call(client, "lease_status")
            assert refused.is_error
            assert "protocol version 2" in text_of(refused)

    try:
        run(scenario())
    finally:
        fake.close()


def test_daemon_announcing_another_protocol_is_refused(tmp_path):
    runtime = tmp_path / "rt"
    def announce_protocol_2(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result(protocol=2))
        return True

    fake = ScriptedDaemon(runtime, announce_protocol_2)

    async def scenario():
        async with frontend(runtime, "announces-2") as client:
            refused = await call(client, "lease_status")
            assert refused.is_error
            assert "speaks protocol 2" in text_of(refused)

    try:
        run(scenario())
    finally:
        fake.close()


def test_a_gated_call_whose_reply_is_lost_is_never_replayed(tmp_path):
    runtime = tmp_path / "rt"

    def hello_then_drop_calls(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result())
            return True
        return False  # the daemon takes the call and vanishes without answering

    fake = ScriptedDaemon(runtime, hello_then_drop_calls)

    async def scenario():
        async with frontend(runtime, "one-shot") as client:
            lost = await call(client, "send_command", {"text": "reset-the-board"})
            assert lost.is_error
            assert "not retried" in text_of(lost)

    try:
        run(scenario())
        calls = [f for f in fake.frames if f["op"] == "call"]
        assert len(calls) == 1, "a tool call must never be replayed automatically"
        assert fake.connections == 1
    finally:
        fake.close()


def test_a_call_with_no_reply_within_the_bound_is_reported_as_unknown(tmp_path, monkeypatch):
    """Bounded waiting: a silent daemon yields a clear outcome-unknown error, never a hang."""
    import serial_mcp_client as frontend_module

    runtime = tmp_path / "rt"
    monkeypatch.setenv("SERIAL_MCP_RUNTIME_DIR", str(runtime))
    monkeypatch.setattr(frontend_module, "CALL_WAIT_SECONDS", 0.3)

    def hello_then_silence(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result())
        return True  # never answer anything else

    fake = ScriptedDaemon(runtime, hello_then_silence)
    fe = frontend_module.Frontend()
    try:
        with pytest.raises(frontend_module.ToolError) as err:
            fe.call_tool("send_command", {"text": "slow"})
        assert "outcome is unknown" in str(err.value)
        assert len([f for f in fake.frames if f["op"] == "call"]) == 1
    finally:
        fe.close()
        fake.close()


def test_hello_during_a_daemon_shutdown_is_retried_on_a_new_connection(tmp_path):
    """A daemon that is draining refuses hello; hello changes nothing on the device, so it is retried."""
    runtime = tmp_path / "rt"
    connections = {"n": 0}

    def draining_then_ready(conn, frame):
        if frame["op"] == "hello":
            connections["n"] += 1
            if connections["n"] == 1:
                body = {"v": 1, "id": frame["id"], "ok": False,
                        "error": {"code": "shutting_down", "message": "the daemon is shutting down"}}
                conn.sendall((json.dumps(body) + "\n").encode())
                return False
            _reply(conn, frame, _hello_result())
            return True
        _reply(conn, frame, {"lease": _hello_result()["lease"]})
        return True

    fake = ScriptedDaemon(runtime, draining_then_ready)

    async def scenario():
        async with frontend(runtime, "after-drain") as client:
            # Registration at startup retries on its own, before any tool call is made.
            await wait_for(lambda: connections["n"] >= 2)
            ok = await call(client, "lease_status")
            assert not ok.is_error, text_of(ok)

    try:
        run(scenario())
        assert connections["n"] == 2  # the refused hello was not retried more than needed
    finally:
        fake.close()


def test_released_control_is_not_resumed_silently_on_a_later_connection(tmp_path):
    """After an explicit release, a new connection must not let the daemon grant control again."""
    runtime = tmp_path / "rt"
    seen = {"calls_after_release": 0, "released": False}

    def handler(conn, frame):
        op = frame["op"]
        if op == "hello":
            _reply(conn, frame, _hello_result())
            return True
        if op == "call":
            if seen["released"]:
                seen["calls_after_release"] += 1
            _reply(conn, frame, {"result": "ok"})
            return True
        if op == "lease" and frame.get("action") == "release":
            seen["released"] = True
            _reply(conn, frame, {"state": "free", "holder": None, "expires_in": None,
                                 "active_operations": 0, "generation": 1, "you": "reacquire_required"})
            return False  # then the connection drops
        _reply(conn, frame, {})
        return True

    fake = ScriptedDaemon(runtime, handler)

    async def scenario():
        async with frontend(runtime, "releaser") as client:
            assert not (await call(client, "send_command", {"text": "first"})).is_error
            assert not (await call(client, "lease_release")).is_error
            first = await call(client, "send_command", {"text": "second"})
            assert first.is_error  # the connection may already be gone, or the request is refused
            second = await call(client, "send_command", {"text": "third"})
            assert second.is_error and "lease_lost" in text_of(second)

    try:
        run(scenario())
        assert seen["calls_after_release"] == 0
        assert fake.connections >= 2  # a fresh connection was made, and it was refused locally
    finally:
        fake.close()
