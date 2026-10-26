"""Compatibility smoke test for the v1 terminal against a v2 daemon peer."""

import os
import pty
import socket
import subprocess
import sys
import threading
from pathlib import Path

import serial_mcp_ipc as ipc

ROOT = Path(__file__).resolve().parent


def test_v1_terminal_fails_clearly_at_v2_negotiation(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(runtime / "daemon.sock"))
    listener.listen(1)
    listener.settimeout(5)
    failures = []

    def serve_hello():
        try:
            peer, _ = listener.accept()
            with peer:
                request = ipc.FrameReader(peer).read_frame()
                peer.sendall(ipc.encode_frame({
                    "id": request["id"], "ok": True,
                    "result": {"protocol": 2, "daemon_version": "test-v2",
                               "client_id": "c1", "role": "human", "clients": 1,
                               "devices": []},
                }))
        except Exception as exc:
            failures.append(exc)

    server = threading.Thread(target=serve_hello, daemon=True)
    server.start()
    master, slave = pty.openpty()
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    try:
        process = subprocess.run([sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT,
                                 env=env, stdin=slave, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, timeout=10)
        server.join(timeout=5)
        assert not server.is_alive(), "the terminal did not complete hello negotiation"
        assert not failures, failures
        assert process.returncode == 1
        assert b"incompatible" in process.stderr
        assert b"speaks 1" in process.stderr
        assert b"protocol v2" in process.stderr
    finally:
        os.close(master)
        os.close(slave)
        listener.close()
        server.join(timeout=1)
