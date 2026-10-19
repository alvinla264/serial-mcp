"""Deterministic Linux-shell and U-Boot target peers for PTY/socket tests."""
from __future__ import annotations

import base64
import fcntl
import hashlib
import os
import re
import select
import shlex
import signal
import socket
import struct
import subprocess
import threading
import time
import tty
import pty


def _write(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _arg(text: str) -> str:
    return shlex.split(text)[0]


class UBootPeer:
    ACK, NAK, C, SOH, STX, EOT, CAN = 0x06, 0x15, 0x43, 0x01, 0x02, 0x04, 0x18

    def __init__(self, fd: int, reject_data: bool = False):
        self.fd = fd
        self.reject_data = reject_data
        self.mode = "command"
        self.pending = bytearray()
        self.base = 0
        self.size = 0
        self.received = bytearray()
        self.eot = False
        self.memory: dict[int, bytes] = {}
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    @staticmethod
    def crc(data: bytes) -> int:
        crc = 0
        for b in data:
            crc ^= b << 8
            for _ in range(8):
                crc = ((crc << 1) ^ 0x1021) & 0xffff if crc & 0x8000 else (crc << 1) & 0xffff
        return crc

    def _command(self, raw: bytes) -> None:
        parts = raw.decode("ascii", "replace").strip().split()
        if len(parts) == 2 and parts[0] == "loady":
            self.base = int(parts[1], 16)
            self.received.clear(); self.eot = False; self.mode = "ymodem"
            _write(self.fd, b"## Ready for binary download\r\nC")
        elif len(parts) == 3 and parts[0] == "md.b":
            address, length = int(parts[1], 16), int(parts[2], 16)
            payload = self.memory.get(address, b"")[:length]
            out = bytearray()
            for offset in range(0, len(payload), 16):
                row = payload[offset:offset + 16]
                hx = b" ".join(f"{b:02x}".encode() for b in row)
                asc = bytes(b if 32 <= b < 127 else ord(".") for b in row)
                out.extend(f"{address + offset:08x}: ".encode() + hx + b"    " + asc + b"\r\n")
            _write(self.fd, bytes(out) + b"=> ")
        else:
            _write(self.fd, b"=> ")

    def _packet(self, packet: bytes) -> None:
        start, seq, inverse = packet[:3]
        content = packet[3:-2]
        crc = packet[-2] << 8 | packet[-1]
        if inverse != 0xff - seq or crc != self.crc(content):
            _write(self.fd, bytes((self.NAK,))); return
        if start == self.SOH and seq == 0:
            fields = content.split(b"\x00")
            if fields[0]:
                self.size = int(fields[1] or b"0")
                _write(self.fd, bytes((self.ACK, self.C)))
            else:
                self.memory[self.base] = bytes(self.received[:self.size])
                _write(self.fd, bytes((self.ACK,)) + f"\r\nBytes transferred: {self.size}\r\n=> ".encode())
                self.mode = "command"
        elif self.reject_data:
            _write(self.fd, bytes((self.NAK,)))
        else:
            self.received.extend(content); _write(self.fd, bytes((self.ACK,)))

    def _feed(self, data: bytes) -> None:
        self.pending.extend(data)
        while self.pending:
            if self.mode == "command":
                ends = [p for p in (self.pending.find(b"\r"), self.pending.find(b"\n")) if p >= 0]
                if not ends: return
                pos = min(ends); line = bytes(self.pending[:pos]); del self.pending[:pos + 1]
                while self.pending[:1] in (b"\r", b"\n"): del self.pending[:1]
                self._command(line)
            elif self.pending[0] == self.CAN:
                while self.pending[:1] == bytes((self.CAN,)): del self.pending[:1]
                self.mode = "command"; _write(self.fd, b"\r\n=> ")
            elif self.pending[0] == self.EOT:
                del self.pending[:1]
                if not self.eot:
                    self.eot = True; _write(self.fd, bytes((self.NAK,)))
                else:
                    self.mode = "final"; _write(self.fd, bytes((self.ACK, self.C)))
            else:
                start = self.pending[0]
                size = 133 if start == self.SOH else 1029 if start == self.STX else 0
                if not size: del self.pending[:1]; continue
                if len(self.pending) < size: return
                packet = bytes(self.pending[:size]); del self.pending[:size]
                self._packet(packet)

    def _run(self) -> None:
        while not self.stop.is_set():
            try:
                readable, _, _ = select.select([self.fd], [], [], 0.05)
                if readable: self._feed(os.read(self.fd, 65536))
            except OSError:
                if self.stop.is_set(): return

    def close(self) -> None:
        self.stop.set(); self.thread.join(timeout=1)


class InteractiveShellTioOwner:
    """Real /bin/sh in canonical/echo PTY mode behind a tio-like raw socket."""
    def __init__(self, socket_path: str | None = None, columns: int = 80, path: str | None = None, pace: float = 0.0):
        self.pace = pace
        self.pid, self.master = pty.fork()
        if self.pid == 0:
            try:
                import termios
                attrs = termios.tcgetattr(0)
                attrs[0] |= termios.ICRNL
                attrs[1] |= termios.OPOST | termios.ONLCR
                attrs[3] |= termios.ICANON | termios.ECHO | termios.ECHOE | termios.ECHOK | termios.ISIG
                termios.tcsetattr(0, termios.TCSANOW, attrs)
                env = os.environ.copy()
                env.update({"PS1": "ash-test# ", "PS2": "> ", "HISTFILE": "/dev/null", "TERM": "dumb"})
                if path is not None:
                    env["PATH"] = path
                os.execvpe("/bin/sh", ["/bin/sh", "-i"], env)
            except BaseException:
                os._exit(127)
        import termios
        fcntl.ioctl(self.master, termios.TIOCSWINSZ, struct.pack("HHHH", 24, columns, 0, 0))
        self.path = socket_path or f"/tmp/serial-mcp-shell-{os.getpid()}-{id(self):x}.sock"
        try: os.unlink(self.path)
        except FileNotFoundError: pass
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(self.path); self.server.listen(8); self.server.settimeout(0.1)
        self.clients: set[socket.socket] = set()
        self.lock = threading.Lock(); self.stop = threading.Event()
        self.accept_thread = threading.Thread(target=self._accept, daemon=True)
        self.output_thread = threading.Thread(target=self._output, daemon=True)
        self.accept_thread.start(); self.output_thread.start()

    def _accept(self) -> None:
        while not self.stop.is_set():
            try: client, _ = self.server.accept()
            except socket.timeout: continue
            except OSError: return
            client.settimeout(0.1)
            with self.lock: self.clients.add(client)
            threading.Thread(target=self._client_input, args=(client,), daemon=True).start()

    def _client_input(self, client: socket.socket) -> None:
        try:
            while not self.stop.is_set():
                try: data = client.recv(8192)
                except socket.timeout: continue
                if not data: break
                _paced_write(self.master, data, self.pace)
        except OSError:
            pass
        finally:
            with self.lock: self.clients.discard(client)
            try: client.close()
            except OSError: pass

    def _output(self) -> None:
        while not self.stop.is_set():
            try:
                readable, _, _ = select.select([self.master], [], [], 0.1)
                if not readable: continue
                data = os.read(self.master, 65536)
            except OSError:
                return
            if self.pace:
                time.sleep(self.pace)
            with self.lock: clients = list(self.clients)
            for client in clients:
                try: client.sendall(data)
                except OSError:
                    with self.lock: self.clients.discard(client)

    def close(self) -> None:
        self.stop.set()
        try: self.server.close()
        except OSError: pass
        with self.lock: clients = list(self.clients); self.clients.clear()
        for client in clients:
            try: client.close()
            except OSError: pass
        try: os.kill(self.pid, signal.SIGTERM)
        except ProcessLookupError: pass
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                pid, _ = os.waitpid(self.pid, os.WNOHANG)
                if pid: break
            except ChildProcessError: break
            time.sleep(0.01)
        else:
            try: os.kill(self.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            try: os.waitpid(self.pid, 0)
            except ChildProcessError: pass
        self.accept_thread.join(timeout=1); self.output_thread.join(timeout=1)
        try: os.close(self.master)
        except OSError: pass
        try: os.unlink(self.path)
        except OSError: pass


def _paced_write(fd: int, data: bytes, pace: float) -> None:
    """Write in 64-byte pieces with `pace` seconds between them (UART-like link)."""
    for i in range(0, len(data), 64):
        if pace:
            time.sleep(pace)
        _write(fd, data[i:i + 64])


def spawn_real_shell(path: str, columns: int = 80) -> tuple[int, int]:
    """Start a real /bin/sh on a new PTY with canonical input, echo, and ISIG.

    Returns (pid, master_fd). `path` is the only PATH the shell sees, so tests
    can omit tools such as sha256sum to model a constrained target.
    """
    import termios
    pid, master = pty.fork()
    if pid == 0:
        try:
            attrs = termios.tcgetattr(0)
            attrs[0] |= termios.ICRNL
            attrs[1] |= termios.OPOST | termios.ONLCR
            attrs[3] |= termios.ICANON | termios.ECHO | termios.ECHOE | termios.ECHOK | termios.ISIG
            termios.tcsetattr(0, termios.TCSANOW, attrs)
            env = os.environ.copy()
            env.update({"PS1": "root@test:~# ", "PS2": "> ", "HISTFILE": "/dev/null", "TERM": "dumb", "PATH": path})
            os.execve("/bin/sh", ["/bin/sh", "-i"], env)
        except BaseException:
            os._exit(127)
    fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 24, columns, 0, 0))
    return pid, master


def reap_shell(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if done:
            return
        time.sleep(0.01)
    try:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
    except (ProcessLookupError, ChildProcessError):
        pass


def build_target_bin(directory, names, shims=None) -> str:
    """Symlink the named host tools into `directory`; `shims` maps name -> shell script."""
    import shutil
    from pathlib import Path
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    for name in names:
        binary = shutil.which(name)
        if binary is None:
            raise RuntimeError(f"host prerequisite missing for the test target: {name}")
        (root / name).symlink_to(binary)
    for name, script in (shims or {}).items():
        shim = root / name
        if shim.is_symlink() or shim.exists():
            shim.unlink()
        shim.write_text(script, encoding="utf-8")
        shim.chmod(0o755)
    return str(root)


class PtyBridge:
    """Joins two PTY masters so the server's device port and the shell's terminal
    behave like a serial link: bytes cross unmodified, each end keeps its own
    line discipline (the shell's canonical/echo handling stays on the shell side).
    """

    def __init__(self, a_master: int, b_master: int, pace: float = 0.0):
        self.a, self.b, self.pace = a_master, b_master, pace
        self.stop_event = threading.Event()
        self.threads = [
            threading.Thread(target=self._pump, args=(a_master, b_master), daemon=True),
            threading.Thread(target=self._pump, args=(b_master, a_master), daemon=True),
        ]
        for thread in self.threads:
            thread.start()

    def _pump(self, src: int, dst: int) -> None:
        while not self.stop_event.is_set():
            try:
                readable, _, _ = select.select([src], [], [], 0.05)
                if not readable:
                    continue
                data = os.read(src, 4096)
            except OSError:
                return
            if not data:
                return
            _paced_write(dst, data, self.pace)

    def close(self) -> None:
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=1)


class RealShellDevice:
    """Real /bin/sh behind a separate 'device' PTY that the server opens directly.

    The device path has no shell holding it, so direct ownership succeeds, while
    the shell still sees canonical/echo terminal behaviour on its own PTY.
    """

    def __init__(self, path: str, pace: float = 0.0):
        self.pid, self.shell_master = spawn_real_shell(path)
        self.device_master, self.device_slave = pty.openpty()
        import tty
        tty.setraw(self.device_slave)
        self.device_path = os.ttyname(self.device_slave)
        self.bridge = PtyBridge(self.shell_master, self.device_master, pace)

    def close(self) -> None:
        self.bridge.close()
        reap_shell(self.pid)
        for fd in (self.device_master, self.device_slave, self.shell_master):
            try:
                os.close(fd)
            except OSError:
                pass
