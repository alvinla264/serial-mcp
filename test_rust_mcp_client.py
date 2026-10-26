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
    ScriptedDaemon, _daemon_pids_for, _hello_result, _reply,
    daemon, echo_device, real_runtime,
)

ROOT = Path(__file__).resolve().parent
BINARY = Path(os.environ.get("SERIAL_MCP_RUST_CLIENT", ROOT / "build/rust-target/release/serial-mcp-rust-client"))


async def _mcp_metadata(mcp):
    if hasattr(mcp, "get_tools"):
        tools = await mcp.get_tools()
        tools = tools.values() if isinstance(tools, dict) else tools
        return {tool.name: _fastmcp2_metadata(tool) for tool in tools}
    tools = await mcp.list_tools()
    return {tool.name: (tool.description, tool.parameters) for tool in tools}


def test_rust_tool_catalog_matches_the_python_oracle():
    subprocess.run([sys.executable, str(ROOT / "tools/export_tool_catalog.py"), "--check"],
                   cwd=ROOT, check=True)


def transport(runtime: Path, name: str) -> StdioTransport:
    return StdioTransport(command=str(BINARY), args=[],
        env={"SERIAL_MCP_RUNTIME_DIR": str(runtime), "SERIAL_MCP_CLIENT_NAME": name,
             "PYTHONPATH": str(ROOT)}, cwd=str(ROOT), keep_alive=False)


def test_rust_v1_tool_catalog_and_v2_rejection(daemon, real_runtime):
    if not BINARY.is_file():
        pytest.skip("build rust-client first; tests always exercise the built executable, never cargo at launch")

    async def scenario():
        async with Client(transport(real_runtime, "rust-integration")) as client:
            native = {tool.name: tool for tool in await client.list_tools()}
            oracle = await _mcp_metadata(python_oracle.mcp)
            legacy_tools = await _mcp_metadata(legacy.mcp)
            assert set(native) == set(oracle)
            assert set(native) - {"lease_acquire", "lease_release", "lease_status"} == set(legacy_tools)
            for name, tool in native.items():
                assert tool.description == (oracle[name][0] or "")
                native_schema = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", {})
                assert native_schema == oracle[name][1]
                if name in legacy_tools:
                    assert tool.description == (legacy_tools[name][0] or "")
                    assert native_schema == legacy_tools[name][1]

            refused = await client.call_tool("file_transfer_capabilities", {}, raise_on_error=False)
            message = "".join(item.text for item in refused.content)
            assert refused.is_error
            assert "daemon protocol mismatch" in message
            assert "frontend speaks protocol 1" in message
            assert "peer speaks 2" in message

    asyncio.run(scenario())


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
            "params": {"name": "lease_status", "arguments": {}},
        })
        reply = receive(2)
        assert reply.get("result", {}).get("isError") is True, reply
        message = "".join(block.get("text", "") for block in reply["result"]["content"])
        assert "daemon protocol mismatch" in message
        assert "frontend speaks protocol 1" in message
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


def test_rust_registers_daemon_during_idle_startup(tmp_path):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")
    runtime = tmp_path / "runtime"

    def handle(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result())
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
            "params": {"name": "lease_status", "arguments": {}},
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


def test_repeated_v2_rejection_does_not_deadlock_the_rust_frontend(tmp_path):
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
            "params": {"name": "lease_status", "arguments": {}},
        })
        first = response_for(2)
        first_message = "".join(block.get("text", "") for block in first["result"]["content"])
        assert first["result"].get("isError") is True
        assert "daemon protocol mismatch" in first_message
        deadline = time.monotonic() + 5
        while not (pids := _daemon_pids_for(runtime)) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(pids) == 1, "frontend did not start exactly one daemon"

        send({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "lease_status", "arguments": {}},
        })
        second = response_for(3, timeout=5)
        second_message = "".join(block.get("text", "") for block in second["result"]["content"])
        assert second["result"].get("isError") is True
        assert "daemon protocol mismatch" in second_message
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
            _reply(conn, frame, _hello_result())
            return True
        return False

    fake = ScriptedDaemon(runtime, hello_then_drop_calls)

    async def scenario():
        async with Client(transport(runtime, "rust-one-shot")) as client:
            lost = await client.call_tool("send_command", {"text": "synthetic-reset"}, raise_on_error=False)
            assert lost.is_error
            assert "not retried" in "".join(item.text for item in lost.content)

    try:
        asyncio.run(scenario())
        calls = [frame for frame in fake.frames if frame["op"] == "call"]
        assert len(calls) == 1, "a non-idempotent tool call must not be replayed"
        assert fake.connections == 1
    finally:
        fake.close()


@pytest.mark.parametrize("wire_version", [1, 2])
def test_rust_delivers_daemon_protocol_mismatch_as_tool_error(tmp_path, wire_version):
    runtime = tmp_path / f"runtime-{wire_version}"

    def announce_version(conn, frame):
        if frame["op"] == "hello":
            if wire_version == 1:
                _reply(conn, frame, _hello_result(protocol=2))
            else:
                conn.sendall(json.dumps({
                    "v": wire_version, "id": frame["id"], "ok": True,
                    "result": _hello_result(),
                }).encode() + b"\n")
            return False
        return True

    fake = ScriptedDaemon(runtime, announce_version)

    async def scenario():
        async with Client(transport(runtime, f"protocol-{wire_version}")) as client:
            result = await client.call_tool("lease_status", {}, raise_on_error=False)
            message = "".join(item.text for item in result.content)
            assert result.is_error
            assert "protocol version 2" in message
            assert "outcome unknown" not in message

    try:
        asyncio.run(scenario())
    finally:
        fake.close()


def test_rust_identifies_the_daemons_v2_rejection_of_its_v1_hello(tmp_path):
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
            result = await client.call_tool("lease_status", {}, raise_on_error=False)
            message = "".join(item.text for item in result.content)
            assert result.is_error
            assert "daemon protocol mismatch" in message
            assert "frontend speaks protocol 1" in message
            assert "peer speaks 2" in message
            assert "outcome unknown" not in message

    try:
        asyncio.run(scenario())
    finally:
        fake.close()


def test_rust_waits_for_a_slow_daemon_tool_reply(tmp_path):
    runtime = tmp_path / "runtime"

    def hello_then_slow_call(conn, frame):
        if frame["op"] == "hello":
            _reply(conn, frame, _hello_result())
        elif frame["op"] == "call":
            time.sleep(0.35)
            _reply(conn, frame, {"result": "completed after delay"})
        else:
            _reply(conn, frame, {"lease": _hello_result()["lease"]})
        return True

    fake = ScriptedDaemon(runtime, hello_then_slow_call)

    async def scenario():
        async with Client(transport(runtime, "rust-slow-call")) as client:
            start = time.monotonic()
            result = await client.call_tool("tio_info", {})
            elapsed = time.monotonic() - start
            assert not result.is_error
            assert "completed after delay" in "".join(item.text for item in result.content)
            assert elapsed >= 0.3

    try:
        asyncio.run(scenario())
    finally:
        fake.close()


def test_uvx_installs_and_reports_the_native_frontend_protocol_mismatch(tmp_path):
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
            assert {"connect", "lease_acquire", "lease_status"} <= tools
            status = await client.call_tool("lease_status", {}, raise_on_error=False)
            message = "".join(item.text for item in status.content)
            assert status.is_error
            assert "daemon protocol mismatch" in message
            assert "frontend speaks protocol 1" in message

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
            profiles = await client.call_tool("list_boot_profiles", {}, raise_on_error=False)
            message = "".join(item.text for item in profiles.content)
            assert profiles.is_error
            assert "daemon protocol mismatch" in message

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


def test_rust_clients_receive_clear_v2_protocol_mismatch(daemon, real_runtime):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")

    async def scenario():
        async with Client(transport(real_runtime, "rust-one")) as first:
            async with Client(transport(real_runtime, "rust-two")) as second:
                for client in (first, second):
                    result = await client.call_tool("lease_status", {}, raise_on_error=False)
                    message = "".join(item.text for item in result.content)
                    assert result.is_error
                    assert "daemon protocol mismatch" in message
                    assert "frontend speaks protocol 1" in message

    asyncio.run(scenario())
