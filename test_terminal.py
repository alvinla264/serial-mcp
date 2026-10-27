"""Public IPC/pty smoke coverage for the multi-device terminal list and exit."""
import os
import pty
import socket
import subprocess
import sys
import threading
from pathlib import Path

import serial_mcp_ipc as ipc

ROOT = Path(__file__).resolve().parent


def test_terminal_lists_count_without_identity_and_quits_cleanly(tmp_path):
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(runtime / "daemon.sock"))
    listener.listen(1)
    master, slave = pty.openpty()
    failures = []

    def serve():
        try:
            peer, _ = listener.accept()
            with peer:
                reader = ipc.FrameReader(peer)
                while request := reader.read_frame():
                    op = request["op"]
                    if op == "hello":
                        result = {"protocol": 2, "daemon_version": "test", "client_id": "private",
                                  "role": "human", "clients": 2, "devices": []}
                    elif op == "status":
                        result = {"protocol": 2, "clients": 2, "devices": [], "grace": None}
                    elif op == "ports":
                        result = {"result": "/dev/test-serial  synthetic port"}
                        os.write(master, b"q")
                    else:
                        result = {}
                    peer.sendall(ipc.encode_frame({"id": request["id"], "ok": True, "result": result}))
        except Exception as exc:
            failures.append(exc)

    server = threading.Thread(target=serve, daemon=True)
    server.start()
    env = dict(os.environ, SERIAL_MCP_RUNTIME_DIR=str(runtime),
               PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    try:
        process = subprocess.run([sys.executable, "-m", "serial_mcp_terminal"], cwd=ROOT,
                                 env=env, stdin=slave, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, timeout=10)
        server.join(timeout=5)
        assert not server.is_alive()
        assert process.returncode == 0
        assert b"Registered MCP clients: 2" in process.stderr
        assert b"/dev/test-serial" in process.stderr
        assert b"private" not in process.stderr
    finally:
        os.close(master)
        os.close(slave)
        listener.close()
        server.join(timeout=1)
