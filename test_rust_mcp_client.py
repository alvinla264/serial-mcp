"""Public-stdio integration checks for the built native frontend."""
import asyncio
import json
import os
import select
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import serial as pyserial
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

import serial_mcp_client as python_oracle
import server as legacy
from tools.export_tool_catalog import _fastmcp2_metadata
from test_mcp_client import (
    ScriptedDaemon, _daemon_pids_for, _echo_upper, _hello_result, _pty_device,
    _release_device, _reply, daemon, echo_device, real_runtime,
)

ROOT = Path(__file__).resolve().parent
BINARY = Path(os.environ.get("SERIAL_MCP_RUST_CLIENT", ROOT / "build/rust-target/release/serial-mcp-rust-client"))


def text_of(result):
    return "".join(getattr(item, "text", "") for item in result.content)


async def _mcp_metadata(mcp):
    if hasattr(mcp, "get_tools"):
        tools = await mcp.get_tools()
        tools = tools.values() if isinstance(tools, dict) else tools
        return {tool.name: _fastmcp2_metadata(tool) for tool in tools}
    tools = await mcp.list_tools()
    return {tool.name: (tool.description, tool.parameters) for tool in tools}


def test_rust_catalog_matches_the_shared_daemon_v2_surface():
    subprocess.run([sys.executable, str(ROOT / "tools/export_tool_catalog.py"), "--check"],
                   cwd=ROOT, check=True)
    catalog = json.loads((ROOT / "rust-client/tools.json").read_text())
    tools = {item["name"]: item for item in catalog}
    assert len(catalog) == 16
    assert {"attach", "tio_info", "lease_acquire", "lease_release", "lease_status"}.isdisjoint(tools)
    assert "set_baudrate" in tools
    device_tools = {
        "connect", "disconnect", "send_command", "read_output", "clear_output",
        "set_prompt_pattern", "login", "enter_bootloader", "upload_file",
        "download_file", "view_io", "set_baudrate",
    }
    for name in device_tools:
        schema = tools[name]["inputSchema"]
        assert "port" in schema["required"], name
        assert schema["properties"]["port"]["type"] == "string", name
    assert set(tools["connect"]["inputSchema"]["properties"]) == {"port", "baudrate"}
    assert set(tools["set_baudrate"]["inputSchema"]["required"]) == {"port", "baudrate"}
    for name in {"file_transfer_capabilities", "list_boot_profiles", "set_boot_credentials", "list_serial_ports"}:
        assert "port" not in tools[name]["inputSchema"].get("required", []), name


def transport(runtime: Path, name: str) -> StdioTransport:
    return StdioTransport(command=str(BINARY), args=[],
        env={"SERIAL_MCP_RUNTIME_DIR": str(runtime), "SERIAL_MCP_CLIENT_NAME": name,
             "PYTHONPATH": str(ROOT)}, cwd=str(ROOT), keep_alive=False)


def test_rust_v2_routes_tools_to_multiple_devices_with_independent_control(daemon, real_runtime, tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first; tests always exercise the built executable, never cargo at launch")
    first = _pty_device(_echo_upper)
    second = _pty_device(_echo_upper)
    alias = tmp_path / "serial-alias"
    alias.symlink_to(first[2])

    async def scenario():
        async with Client(transport(real_runtime, "rust-multi-one")) as owner:
            async with Client(transport(real_runtime, "rust-multi-two")) as observer:
                names = {tool.name for tool in await owner.list_tools()}
                assert len(names) == 16
                assert {"attach", "tio_info", "lease_acquire", "lease_release", "lease_status"}.isdisjoint(names)
                assert "set_baudrate" in names
                invalid = await owner.call_tool("connect", {"port": "/dev/serial-mcp-missing", "baudrate": 115200}, raise_on_error=False)
                assert invalid.is_error and "invalid_port" in text_of(invalid)
                connected = await owner.call_tool("connect", {"port": first[2], "baudrate": 115200})
                assert not connected.is_error, text_of(connected)
                bad_baud = await owner.call_tool("set_baudrate", {"port": first[2], "baudrate": 0}, raise_on_error=False)
                assert bad_baud.is_error and "baudrate_rejected" in text_of(bad_baud)
                assert "Actual baudrate remains 115200" in text_of(bad_baud)
                busy = await observer.call_tool("connect", {"port": str(alias), "baudrate": 57600})
                assert not busy.is_error, text_of(busy)
                assert "Control not granted" in text_of(busy)
                assert "actual baudrate 115200" in text_of(busy)
                assert "Requested baudrate 57600 differs" in text_of(busy)
                other = await owner.call_tool("connect", {"port": second[2], "baudrate": 38400})
                assert not other.is_error, text_of(other)
                r1 = await owner.call_tool("send_command", {"port": first[2], "text": "alpha"})
                r2 = await owner.call_tool("send_command", {"port": second[2], "text": "beta"})
                assert not r1.is_error and "ALPHA" in text_of(r1)
                assert not r2.is_error and "BETA" in text_of(r2)
                observed_output = await observer.call_tool("read_output", {"port": str(alias), "timeout": 0})
                owner_output = await owner.call_tool("read_output", {"port": first[2], "timeout": 0})
                assert "ALPHA" in text_of(observed_output) and "ALPHA" in text_of(owner_output)
                changed = await owner.call_tool("set_baudrate", {"port": first[2], "baudrate": 19200})
                assert not changed.is_error, text_of(changed)
                assert "actual baudrate is 19200" in text_of(changed)
                notice = await observer.call_tool("read_output", {"port": str(alias), "timeout": 0})
                assert not notice.is_error
                assert "settings changed: baudrate is now 19200" in text_of(notice)
                left = await owner.call_tool("disconnect", {"port": first[2]})
                assert not left.is_error
                refused = await observer.call_tool("send_command", {"port": str(alias), "text": "must-not-acquire"}, raise_on_error=False)
                assert refused.is_error and "control_required" in text_of(refused)
                reacquired = await observer.call_tool("connect", {"port": str(alias), "baudrate": 57600})
                assert not reacquired.is_error and "Control granted" in text_of(reacquired)
                assert "remains at 19200" in text_of(reacquired)
                remains = await owner.call_tool("send_command", {"port": second[2], "text": "still-here"})
                assert not remains.is_error and "STILL-HERE" in text_of(remains)

    try:
        asyncio.run(scenario())
    finally:
        _release_device(*first[:2], first[3], first[4])
        _release_device(*second[:2], second[3], second[4])


def test_rust_set_baudrate_warns_when_an_inflight_call_spans_the_change(daemon, real_runtime):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    started = threading.Event()
    release = threading.Event()

    def delayed_echo(master, stop):
        import select
        while not stop.is_set():
            readable, _, _ = select.select([master], [], [], 0.05)
            if master not in readable:
                continue
            payload = os.read(master, 1024)
            started.set()
            if not release.wait(4):
                return
            os.write(master, payload.upper())
            return

    device = _pty_device(delayed_echo)

    async def scenario():
        async with Client(transport(real_runtime, "rust-inflight-baud")) as client:
            joined = await client.call_tool("connect", {"port": device[2], "baudrate": 115200})
            assert not joined.is_error, text_of(joined)
            inflight = asyncio.create_task(client.call_tool(
                "send_command", {"port": device[2], "text": "held", "timeout": 4}
            ))
            assert await asyncio.to_thread(started.wait, 3), "device did not receive command"
            changed = await client.call_tool("set_baudrate", {"port": device[2], "baudrate": 19200})
            assert not changed.is_error, text_of(changed)
            release.set()
            result = await inflight
            assert not result.is_error and "HELD" in text_of(result)
            assert "settings changed during this operation; output/outcome may be affected" in text_of(result)

    try:
        asyncio.run(scenario())
    finally:
        release.set()
        _release_device(*device[:2], device[3], device[4])


def test_rust_local_source_build_starts_daemon_outside_repository_cwd(tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    runtime = tmp_path / "runtime"
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime), SERIAL_MCP_CLIENT_NAME="cwd-test")
    env.pop("PYTHONPATH", None)
    isolated_python = tmp_path / "isolated-python"
    purelib = json.dumps(str(Path(pyserial.__file__).parent.parent))
    isolated_python.write_text(
        "#!/bin/sh\n"
        "[ \"$1\" = \"-m\" ] || exit 64\n"
        f"exec {shlex.quote(sys.executable)} -S -c "
        f"'import runpy,sys; sys.path.insert(0, {purelib}); "
        f"module=sys.argv[1]; sys.argv=[module]; "
        f"runpy.run_module(module, run_name=\"__main__\")' \"$2\"\n"
    )
    isolated_python.chmod(0o700)
    env["PYTHON"] = str(isolated_python)
    frontend = subprocess.Popen(
        [str(BINARY)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, cwd=tmp_path,
    )

    def send(message):
        frontend.stdin.write((json.dumps(message) + "\n").encode())
        frontend.stdin.flush()

    def receive(request_id, timeout=25):
        ready, _, _ = select.select([frontend.stdout], [], [], timeout)
        assert ready, f"frontend gave no response to MCP request {request_id}"
        response = json.loads(frontend.stdout.readline())
        assert response.get("id") == request_id, response
        return response

    try:
        send({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "source-cwd-test", "version": "1"},
            },
        })
        assert "result" in receive(1)
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "file_transfer_capabilities", "arguments": {}},
        })
        reply = receive(2)
        assert reply.get("result", {}).get("isError") is False, reply
        assert len(_daemon_pids_for(runtime)) == 1
    finally:
        if frontend.poll() is None:
            frontend.terminate()
            frontend.wait(timeout=5)
        for pid in _daemon_pids_for(runtime):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        frontend.stdin.close()
        frontend.stdout.close()
        frontend.stderr.close()


def test_rust_process_crash_releases_all_device_memberships(daemon, real_runtime, echo_device):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(real_runtime),
               SERIAL_MCP_CLIENT_NAME="rust-crash-owner", PYTHONPATH=str(ROOT))
    frontend = subprocess.Popen(
        [str(BINARY)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, cwd=ROOT,
    )

    def send(frame):
        frontend.stdin.write((json.dumps(frame) + "\n").encode())
        frontend.stdin.flush()

    def receive(request_id, timeout=15):
        ready, _, _ = select.select([frontend.stdout], [], [], timeout)
        assert ready, f"frontend gave no MCP response to {request_id}"
        response = json.loads(frontend.stdout.readline())
        assert response.get("id") == request_id, response
        return response

    try:
        send({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{
            "protocolVersion":"2025-03-26", "capabilities":{},
            "clientInfo":{"name":"crash-owner", "version":"1"}}})
        assert "result" in receive(1)
        send({"jsonrpc":"2.0", "method":"notifications/initialized"})
        send({"jsonrpc":"2.0", "id":2, "method":"tools/call", "params":{
            "name":"connect", "arguments":{"port":echo_device[2], "baudrate":115200}}})
        connected = receive(2)
        assert not connected["result"]["isError"], connected
        assert "Control granted" in "".join(x.get("text", "") for x in connected["result"]["content"])
        frontend.kill()
        frontend.wait(timeout=5)
        frontend.stdin.close()
        frontend.stdout.close()
        frontend.stderr.close()
        frontend = None

        async def rejoin():
            async with Client(transport(real_runtime, "rust-after-crash")) as client:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    result = await client.call_tool("connect", {"port":echo_device[2], "baudrate":57600}, raise_on_error=False)
                    text = text_of(result)
                    if not result.is_error and "Control granted" in text and "57600 baud" in text:
                        return
                    await asyncio.sleep(0.05)
                raise AssertionError(f"device membership/control was not released after client crash: {text}")
        asyncio.run(rejoin())
    finally:
        if frontend is not None and frontend.poll() is None:
            frontend.kill()
            frontend.wait(timeout=5)


def test_rust_registers_daemon_during_idle_startup(tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    runtime = tmp_path / "runtime"

    def handle(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result(protocol=2))
        return True

    fake = ScriptedDaemon(runtime, handle)

    async def scenario():
        async with Client(transport(runtime, "idle-registration")) as client:
            tools = await client.list_tools()
            assert tools
            deadline = time.monotonic() + 5
            while not any(frame.get("op") == "hello" for frame in fake.frames):
                if time.monotonic() >= deadline:
                    raise AssertionError("idle frontend did not register with daemon")
                await asyncio.sleep(0.01)

    try:
        asyncio.run(scenario())
    finally:
        fake.close()


def test_autostarted_daemon_survives_frontend_process_group_termination(tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    runtime = tmp_path / "runtime"
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime), SERIAL_MCP_CLIENT_NAME="group-test")
    frontend = subprocess.Popen(
        [str(BINARY)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, cwd=ROOT, start_new_session=True,
    )
    daemon_pids = []
    try:
        initialize = {
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "process-group-test", "version": "1"},
            },
        }
        frontend.stdin.write((json.dumps(initialize) + "\n").encode())
        frontend.stdin.flush()
        ready, _, _ = select.select([frontend.stdout], [], [], 15)
        assert ready, "native frontend did not answer initialize"
        response = json.loads(frontend.stdout.readline())
        assert response.get("id") == 1 and "result" in response, response
        frontend.stdin.write((json.dumps({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "file_transfer_capabilities", "arguments": {}},
        }) + "\n").encode())
        frontend.stdin.flush()
        ready, _, _ = select.select([frontend.stdout], [], [], 15)
        assert ready, "first tool call did not respond"
        assert json.loads(frontend.stdout.readline()).get("id") == 2
        deadline = time.monotonic() + 5
        while not (daemon_pids := _daemon_pids_for(runtime)) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert daemon_pids, "frontend did not auto-start the daemon"
        daemon_pid = daemon_pids[0]
        assert os.getpgid(daemon_pid) != os.getpgid(frontend.pid), (
            "auto-started daemon must have its own process group"
        )

        os.killpg(os.getpgid(frontend.pid), signal.SIGTERM)
        frontend.wait(timeout=5)
        os.kill(daemon_pid, 0)
    finally:
        if frontend.poll() is None:
            frontend.terminate()
            frontend.wait(timeout=5)
        for pid in _daemon_pids_for(runtime):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        frontend.stdin.close()
        frontend.stdout.close()
        frontend.stderr.close()


def test_repeated_v2_calls_do_not_deadlock_the_rust_frontend(tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    runtime = tmp_path / "runtime"
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime), SERIAL_MCP_CLIENT_NAME="loss-test")
    frontend = subprocess.Popen(
        [str(BINARY)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, cwd=ROOT, start_new_session=True,
    )

    def send(message):
        frontend.stdin.write((json.dumps(message) + "\n").encode())
        frontend.stdin.flush()

    def response_for(request_id, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ready, _, _ = select.select([frontend.stdout], [], [], max(0, deadline - time.monotonic()))
            if not ready:
                break
            line = frontend.stdout.readline()
            if not line:
                break
            message = json.loads(line)
            if message.get("id") == request_id:
                return message
        raise AssertionError(f"no MCP response for request {request_id} within {timeout}s")

    try:
        send({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "daemon-loss-test", "version": "1"},
            },
        })
        assert "result" in response_for(1)
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "file_transfer_capabilities", "arguments": {}},
        })
        first = response_for(2)
        assert first["result"].get("isError") is False
        deadline = time.monotonic() + 5
        while not (pids := _daemon_pids_for(runtime)) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(pids) == 1, "frontend did not start exactly one daemon"

        send({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "file_transfer_capabilities", "arguments": {}},
        })
        second = response_for(3, timeout=5)
        assert second["result"].get("isError") is False
    finally:
        if frontend.poll() is None:
            frontend.terminate()
            frontend.wait(timeout=5)
        for pid in _daemon_pids_for(runtime):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        frontend.stdin.close()
        frontend.stdout.close()
        frontend.stderr.close()


def test_rust_never_replays_a_non_idempotent_call_after_its_reply_is_lost(tmp_path):
    runtime = tmp_path / "runtime"

    def hello_then_drop_calls(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result(protocol=2))
            return True
        return False

    fake = ScriptedDaemon(runtime, hello_then_drop_calls)

    async def scenario():
        async with Client(transport(runtime, "rust-one-shot")) as client:
            lost = await client.call_tool("send_command", {"port": "/dev/synthetic", "text": "synthetic-reset"}, raise_on_error=False)
            assert lost.is_error
            assert "not retried" in "".join(item.text for item in lost.content)

    try:
        asyncio.run(scenario())
        calls = [frame for frame in fake.frames if frame["op"] == "call"]
        assert len(calls) == 1, "a non-idempotent tool call must not be replayed"
        assert fake.connections == 1
    finally:
        fake.close()


@pytest.mark.parametrize("wire_version", [1, 3])
def test_rust_rejects_daemon_using_another_wire_version(tmp_path, wire_version):
    runtime = tmp_path / f"runtime-{wire_version}"

    def announce_version(conn, frame):
        if frame["op"] == "hello":
            conn.sendall(json.dumps({
                "v": wire_version, "id": frame["id"], "ok": True,
                "result": _hello_result(protocol=wire_version),
            }).encode() + b"\n")
            return False
        return True

    fake = ScriptedDaemon(runtime, announce_version)

    async def scenario():
        async with Client(transport(runtime, f"protocol-{wire_version}")) as client:
            result = await client.call_tool("file_transfer_capabilities", {}, raise_on_error=False)
            message = "".join(item.text for item in result.content)
            assert result.is_error
            assert "daemon protocol mismatch" in message
            assert f"protocol version {wire_version}" in message
            assert "outcome unknown" not in message

    try:
        asyncio.run(scenario())
    finally:
        fake.close()


def test_rust_identifies_an_explicit_v2_protocol_rejection(tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    runtime = tmp_path / "runtime"

    def reject_v1_hello(conn, frame):
        if frame["op"] == "hello":
            conn.sendall((json.dumps({
                "v": 2, "id": None, "ok": False,
                "error": {"code": "unsupported_version",
                          "message": "protocol version 1 is not supported; this peer speaks 2"},
            }) + "\n").encode())
            return False
        return True

    fake = ScriptedDaemon(runtime, reject_v1_hello)

    async def scenario():
        async with Client(transport(runtime, "rust-v2-rejection")) as client:
            result = await client.call_tool("file_transfer_capabilities", {}, raise_on_error=False)
            message = "".join(item.text for item in result.content)
            assert result.is_error
            assert "daemon protocol mismatch" in message
            assert "frontend speaks protocol 2" in message
            assert "protocol version 1 is not supported" in message
            assert "outcome unknown" not in message

    try:
        asyncio.run(scenario())
    finally:
        fake.close()


def test_rust_waits_for_a_slow_daemon_tool_reply(tmp_path):
    runtime = tmp_path / "runtime"

    def hello_then_slow_call(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result(protocol=2))
        elif frame["op"] == "call":
            time.sleep(0.35)
            _reply(conn, frame, {"result": "completed after delay"})
        else:
            _reply(conn, frame, {"result": "slow operation finished"})
        return True

    fake = ScriptedDaemon(runtime, hello_then_slow_call)

    async def scenario():
        async with Client(transport(runtime, "rust-slow-call")) as client:
            start = time.monotonic()
            result = await client.call_tool("file_transfer_capabilities", {})
            elapsed = time.monotonic() - start
            assert not result.is_error
            assert "completed after delay" in "".join(item.text for item in result.content)
            assert elapsed >= 0.3

    try:
        asyncio.run(scenario())
    finally:
        fake.close()


def test_uvx_installs_the_native_shared_app_frontend(tmp_path):
    uvx = shutil.which("uvx")
    if not uvx or not shutil.which("cargo"):
        pytest.skip("uvx and Cargo are required to verify the documented source-install path")
    runtime = tmp_path / "runtime"
    env = {
        "SERIAL_MCP_RUNTIME_DIR": str(runtime),
        "SERIAL_MCP_CLIENT_NAME": "uvx-smoke",
        "UV_CACHE_DIR": str(tmp_path / "uv-cache"),
    }

    async def scenario():
        transport = StdioTransport(
            command=uvx,
            args=["--from", str(ROOT), "serial-mcp"],
            env=env,
            cwd=str(tmp_path),
            keep_alive=False,
        )
        async with Client(transport) as client:
            tools = {tool.name for tool in await client.list_tools()}
            assert {"connect", "set_baudrate"} <= tools
            assert {"lease_acquire", "lease_status"}.isdisjoint(tools)
            status = await client.call_tool("file_transfer_capabilities", {}, raise_on_error=False)
            assert not status.is_error

    try:
        asyncio.run(scenario())
    finally:
        for pid in _daemon_pids_for(runtime):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


def test_rust_frontend_autostarts_daemon_without_fastmcp_or_legacy_server(tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    runtime = tmp_path / "runtime"
    blocker = tmp_path / "blocker"
    blocker.mkdir()
    marker = tmp_path / "sitecustomize-ran"
    (blocker / "sitecustomize.py").write_text(
        "import builtins, os\n"
        "original_import = builtins.__import__\n"
        "blocked = ('fastmcp', 'mcp', 'server')\n"
        "def guarded(name, *args, **kwargs):\n"
        "    if any(name == prefix or name.startswith(prefix + '.') for prefix in blocked):\n"
        "        raise ImportError('blocked dependency: ' + name)\n"
        "    return original_import(name, *args, **kwargs)\n"
        "builtins.__import__ = guarded\n"
        "open(os.environ['SERIAL_MCP_BLOCK_MARKER'], 'w').close()\n"
    )

    async def scenario():
        server = StdioTransport(command=str(BINARY), args=[],
            env={"SERIAL_MCP_RUNTIME_DIR": str(runtime), "SERIAL_MCP_CLIENT_NAME": "rust-autostart",
                 "SERIAL_MCP_BLOCK_MARKER": str(marker), "PYTHONPATH": f"{blocker}:{ROOT}"},
            cwd=str(ROOT), keep_alive=False)
        async with Client(server) as client:
            capabilities = await client.call_tool("file_transfer_capabilities", {}, raise_on_error=False)
            assert not capabilities.is_error

    try:
        asyncio.run(scenario())
        assert marker.exists(), "the Rust frontend did not start the Python daemon"
    finally:
        owned = _daemon_pids_for(runtime)
        for pid in owned:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 5
        while _daemon_pids_for(runtime) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not _daemon_pids_for(runtime), "isolated test daemon did not stop"


def test_multiple_rust_clients_register_with_the_v2_daemon(daemon, real_runtime):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")

    async def scenario():
        async with Client(transport(real_runtime, "rust-one")) as first:
            async with Client(transport(real_runtime, "rust-two")) as second:
                for client in (first, second):
                    result = await client.call_tool("file_transfer_capabilities", {}, raise_on_error=False)
                    assert not result.is_error, text_of(result)

    asyncio.run(scenario())
