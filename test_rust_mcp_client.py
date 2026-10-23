"""Public-stdio integration checks for the built native frontend."""
import asyncio
import json
import os
import select
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
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


def test_rust_tools_list_and_calls_match_python_oracle(daemon, real_runtime, echo_device):
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

            caps = await client.call_tool("file_transfer_capabilities", {})
            assert not caps.is_error
            assert "YMODEM" in "".join(item.text for item in caps.content)

            # Responses are correlated while two daemon operations overlap.
            results = await asyncio.gather(client.call_tool("tio_info", {}),
                                           client.call_tool("list_boot_profiles", {}))
            assert all(not result.is_error for result in results)
            assert "Not connected" in "".join(item.text for item in results[0].content)
            assert "[builtin]" in "".join(item.text for item in results[1].content)

            _, _, device_path, _, _ = echo_device
            connected = await client.call_tool("connect", {"port": device_path, "session_socket": ""})
            assert not connected.is_error
            echoed = await client.call_tool("send_command", {"text": "abc", "timeout": 2})
            assert not echoed.is_error
            assert "ABC" in "".join(item.text for item in echoed.content)
            await client.call_tool("disconnect", {})

            invalid = await client.call_tool("read_output", {"max_lines": "wrong"}, raise_on_error=False)
            assert invalid.is_error
            nested_invalid = await client.call_tool(
                "enter_bootloader", {"interrupt_keys": [7]}, raise_on_error=False
            )
            assert nested_invalid.is_error
            assert "interrupt_keys" in "".join(item.text for item in nested_invalid.content)
            unknown = await client.call_tool("__unknown_tool__", {}, raise_on_error=False)
            assert unknown.is_error

    asyncio.run(scenario())


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


def test_daemon_loss_does_not_deadlock_next_mcp_call(tmp_path):
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
        deadline = time.monotonic() + 5
        while not (pids := _daemon_pids_for(runtime)) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(pids) == 1, "frontend did not start exactly one daemon"
        os.kill(pids[0], signal.SIGKILL)
        deadline = time.monotonic() + 5
        while _daemon_pids_for(runtime) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not _daemon_pids_for(runtime), "killed daemon is still present"

        send({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "lease_status", "arguments": {}},
        })
        first_after_loss = response_for(2, timeout=5)
        assert "result" in first_after_loss, first_after_loss

        send({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "lease_status", "arguments": {}},
        })
        recovered = response_for(3, timeout=10)
        assert "result" in recovered and recovered["result"].get("isError") is not True, recovered
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


def test_uvx_installs_and_launches_the_native_frontend_from_source(tmp_path):
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
            status = await client.call_tool("lease_status", {})
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
            profiles = await client.call_tool("list_boot_profiles", {})
            assert not profiles.is_error
            assert "[builtin]" in "".join(item.text for item in profiles.content)

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


def test_rust_clients_share_daemon_lease_without_blocking_second_client(daemon, real_runtime):
    if not BINARY.is_file():
        pytest.skip("build rust-client first")

    async def scenario():
        async with Client(transport(real_runtime, "rust-one")) as first:
            async with Client(transport(real_runtime, "rust-two")) as second:
                acquired = await first.call_tool("lease_acquire", {})
                assert not acquired.is_error
                busy = await second.call_tool("lease_acquire", {}, raise_on_error=False)
                assert busy.is_error
                assert "busy" in "".join(item.text for item in busy.content).lower()

                released = await first.call_tool("lease_release", {})
                assert not released.is_error
                refused = await first.call_tool(
                    "connect", {"port": "/dev/null", "session_socket": ""}, raise_on_error=False
                )
                assert refused.is_error
                assert "lease_lost" in "".join(item.text for item in refused.content)
                reacquired = await first.call_tool("lease_acquire", {})
                assert not reacquired.is_error
                still_busy = await second.call_tool("lease_acquire", {}, raise_on_error=False)
                assert still_busy.is_error

                await first.call_tool("lease_release", {})
                released_to_second = await second.call_tool("lease_acquire", {})
                assert not released_to_second.is_error

    asyncio.run(scenario())
