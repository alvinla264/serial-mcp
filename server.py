#!/usr/bin/env python3
"""Generic serial transport MCP server.

The AI opens the serial device and serves a session socket that speaks tio's
raw-byte --socket protocol, so the user can attach to the AI's session from
their own terminal (real tio via a socat pty bridge, or nc -UN).
"""

from __future__ import annotations

import re
import json
import socket
import threading
import time
import errno
import os
from collections import deque
from pathlib import Path

import serial
import serial.tools.list_ports
from fastmcp import FastMCP


mcp = FastMCP("GenericSerialTransport")

LINE_ENDINGS = {
    "none": b"",
    "lf": b"\n",
    "cr": b"\r",
    "crlf": b"\r\n",
}

SESSION_SOCKET_AUTO = "auto"  # derives /tmp/serial-mcp-<devname>.sock from the port

# Where session sockets live. A module constant rather than a literal so the
# lookup is testable without inheriting whatever happens to be live in /tmp.
SESSION_SOCKET_DIR = "/tmp"
SESSION_SOCKET_GLOB = "serial-mcp-*.sock"


def _session_socket_for_port(port: str) -> str:
    """Derive a unique session socket path from the serial device name."""
    name = os.path.basename(port.rstrip("/")) or "port"
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", name)
    return f"/tmp/serial-mcp-{safe}.sock"


def _attach_hint(path: str) -> str:
    tty_path = path[: -len(".sock")] if path.endswith(".sock") else path + ".tty"
    return (
        f"session socket: {path}\n"
        f"companion pty: {tty_path} (any tty tool: tio, screen, minicom, cat)\n"
        f"e.g.:  tio {tty_path}\n"
        f"or poke it directly:  echo cmd | nc -UN {path}\n"
    )


# Built-in bootloader profiles: the shareable "recipe" for entering U-Boot on a
# device family. Credentials are NEVER stored here — profiles reference a
# credential key resolved from the local, gitignored credentials file.
BUILTIN_BOOT_PROFILES = {
    "generic": {
        "description": "Generic U-Boot: tap Space during the autoboot window.",
        "bootloader_prompt_regex": r"(U-Boot|uboot)[^\n]*[>#]\s|Enter magic string to stop autoboot|^\s*=>\s",
        "abort_regex": r"(login:|Login:)",
        "interrupt": {"keys": [" "], "delay": 0.2, "interval": 0.05, "max_attempts": 40},
        "reboot": {"command": "reboot", "cycles": 3},
    },
    "u-boot-any-key": {
        "description": "U-Boot variants that accept any character to abort autoboot.",
        "bootloader_prompt_regex": r"(U-Boot|uboot)[^\n]*[>#]\s|Enter magic string to stop autoboot|^\s*=>\s",
        "abort_regex": r"(login:|Login:)",
        "interrupt": {"keys": ["\r", " ", "x"], "delay": 0.2, "interval": 0.05, "max_attempts": 40},
        "reboot": {"command": "reboot", "cycles": 3},
    },
}

CONFIG_DIR = Path(os.environ.get("SERIAL_MCP_CONFIG", "~/.config/serial-mcp")).expanduser()
USER_PROFILES_PATH = CONFIG_DIR / "profiles.json"
CREDENTIALS_PATH = CONFIG_DIR / "credentials.json"


def _load_json_file(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def _attach_socket_for_port(port: str) -> str:
    """Default socket path to attach to, matching the documented tio pairing."""
    return _session_socket_for_port(port)


def _glob_session_sockets() -> list[str]:
    """Session sockets this server (or a matching tio) may be serving."""
    import glob as _glob

    return _glob.glob(os.path.join(SESSION_SOCKET_DIR, SESSION_SOCKET_GLOB))


def _socket_is_live(path: str) -> bool:
    """True if something is listening on `path` right now.

    A stale socket file with no listener accepts neither connect nor is it
    useful, so this is what distinguishes "tio is serving this device" from
    "leftover file from a dead process".

    The probe connects and immediately closes. On a stream socket the owner sees
    that as a client that attached and left straight away, which its accept loop
    must tolerate; it is the only way to tell a live listener from a dead file.
    """
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(0.5)
    try:
        probe.connect(path)
        return True
    except OSError:
        return False
    finally:
        try:
            probe.close()
        except OSError:
            pass


class DirectTransport:
    """Owns a serial device: the server is the only party holding the port.

    Exposes the same minimal interface as SocketTransport so the reader loop and
    every tool work identically over either transport.
    """

    kind = "direct"

    def __init__(self, port: str, baudrate: int, timeout: float) -> None:
        self.ser = serial.Serial(port, baudrate, timeout=timeout)
        self.port = port
        self.baudrate = baudrate

    @property
    def is_open(self) -> bool:
        return bool(self.ser and self.ser.is_open)

    @property
    def in_waiting(self) -> int:
        return self.ser.in_waiting

    def read(self, size: int) -> bytes:
        return self.ser.read(size)

    def write(self, data: bytes) -> None:
        self.ser.write(data)

    def flush(self) -> None:
        self.ser.flush()

    def close(self) -> None:
        try:
            self.ser.close()
        except Exception:
            pass


class SocketTransport:
    """Attaches to a socket that someone else is serving (typically tio).

    The owner — the user's `tio -S unix:/tmp/...` — holds the device. This side
    only relays raw bytes, using the same tio --socket protocol the session
    server speaks: no handshake, no framing. Nothing here kills the owner: the
    socket is simply closed on disconnect, which drops this attachment and
    leaves the user's tio session running.
    """

    kind = "attach"

    def __init__(self, path: str, timeout: float = 0.1) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(max(timeout, 0.05))
        self.sock.connect(path)
        self.port = path
        self.baudrate = None
        self._pending = b""

    @property
    def is_open(self) -> bool:
        return self.sock is not None

    @property
    def in_waiting(self) -> int:
        # A stream socket cannot report a count, so pull whatever is already
        # buffered locally; the reader loops on this same call cheaply.
        if self._pending:
            return len(self._pending)
        try:
            data = self.sock.recv(65536)
        except (socket.timeout, BlockingIOError):
            return 0
        except OSError:
            raise
        if not data:
            # Owner closed the socket: surface as EIO so the reader stops with
            # the same "connection unavailable" handling as an unplugged port.
            raise OSError(errno.EIO, "socket closed by peer")
        self._pending += data
        return len(self._pending)

    def read(self, size: int) -> bytes:
        data, self._pending = self._pending[:size], self._pending[size:]
        return data

    def write(self, data: bytes) -> None:
        self.sock.sendall(data)

    def flush(self) -> None:
        pass  # sendall already pushed the bytes

    def close(self) -> None:
        sock, self.sock = self.sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass


class SerialState:
    """Thread-safe serial connection, background RX buffer/log, and a tio-protocol
    session socket the user can attach to.

    Two transports are supported, selected by how connect() is called:

    * direct  — this server opens the serial port and owns it, exposing a
                session socket + companion pty for the user to attach to.
    * attach  — someone else owns the device (typically the user's
                `tio -S unix:/tmp/... `), and this server joins that session as
                a client. No session socket or pty is created; the owner keeps
                running when we disconnect.

    Everything above the transport (buffering, send/read, login, bootloader) is
    transport-agnostic.
    """

    def __init__(self) -> None:
        self.transport: DirectTransport | SocketTransport | None = None
        self.default_timeout = 5.0
        self.default_idle_timeout = 0.2
        self.prompt_pattern: re.Pattern[bytes] | None = None
        self.log_path = "/tmp/serial-mcp.log"

        self.reader_thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.buffer = b""
        self.buffer_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.log_rx_buffer = ""
        self.log_rx_last_update: float | None = None
        self.log_rx_idle_flush_interval = 0.1

        # Advisory exclusive lock for the owned serial port (see _claim_port_lock)
        self._port_lock_fd: int | None = None
        self._port_lock_path: str | None = None

        # Session socket (tio raw-byte protocol): RX broadcast to all clients,
        # client bytes forwarded to the device.
        self.session_path: str | None = None
        self.session_server: socket.socket | None = None
        self.session_running = threading.Event()
        self.session_thread: threading.Thread | None = None
        self.session_clients: set[socket.socket] = set()
        self.session_lock = threading.Lock()

        # Companion pty so terminal tools (tio, screen, minicom, cat...) can attach
        # directly; they cannot open a unix socket as a device. Symlinked at
        # <session path sans .sock>.
        self.session_pty_master: int | None = None
        self.session_pty_link: str | None = None

        # In-memory I/O history for view_io: (timestamp, "RX"|"TX", source, bytes)
        self.io_history: deque[tuple[float, str, str, bytes]] = deque(maxlen=2000)

        with open(self.log_path, "w", encoding="utf-8") as f:
            f.write(f"--- Serial MCP Log Started at {time.strftime('%c')} ---\n")

    def _log(self, prefix: str, data: bytes | str) -> None:
        with self.log_lock:
            try:
                timestamp = time.strftime("%H:%M:%S")
                text = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else data
                formatted = text.replace("\r", "").replace("\n", "\n    ")
                with open(self.log_path, "a", encoding="utf-8") as f:
                    f.write(f"[{timestamp}] [{prefix}] {formatted}\n")
            except Exception:
                pass

    def _record_io(self, direction: str, source: str, data: bytes) -> None:
        self.io_history.append((time.time(), direction, source, data))

    def _log_rx_chunk(self, chunk: bytes) -> None:
        text = chunk.decode("utf-8", errors="replace")
        with self.log_lock:
            self.log_rx_buffer += text
            self.log_rx_last_update = time.monotonic()
            while "\n" in self.log_rx_buffer:
                line, self.log_rx_buffer = self.log_rx_buffer.split("\n", 1)
                self._write_rx_log_line_locked(line)
            if not self.log_rx_buffer:
                self.log_rx_last_update = None

    def _write_rx_log_line_locked(self, text: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] [RX] {text.replace(chr(13), '')}\n")
        except Exception:
            pass

    def _flush_rx_log_buffer(self, force: bool = False) -> None:
        with self.log_lock:
            if not self.log_rx_buffer:
                self.log_rx_last_update = None
                return
            if not force and self.log_rx_last_update is not None:
                if time.monotonic() - self.log_rx_last_update < self.log_rx_idle_flush_interval:
                    return
            self._write_rx_log_line_locked(self.log_rx_buffer)
            self.log_rx_buffer = ""
            self.log_rx_last_update = None

    def _reader_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                transport = self.transport
                if transport is not None and transport.is_open:
                    waiting = transport.in_waiting
                    if waiting > 0:
                        chunk = transport.read(waiting)
                        if chunk:
                            self._log_rx_chunk(chunk)
                            self._record_io("RX", "device", chunk)
                            self._broadcast(chunk)
                            with self.buffer_lock:
                                self.buffer += chunk
                    else:
                        self._flush_rx_log_buffer(force=False)
                        time.sleep(0.01)
                else:
                    self._flush_rx_log_buffer(force=False)
                    time.sleep(0.1)
            except Exception as e:
                if self._is_expected_shutdown_error(e):
                    break
                if self._is_eio_error(e):
                    self._log("WARN", f"Serial reader stopping after I/O error; connection unavailable: {e}")
                    self.stop_event.set()
                    transport = self.transport
                    if transport is not None:
                        transport.close()
                        self.transport = None
                    break
                self._log("ERR", f"Reader loop error: {e}")
                time.sleep(0.2)

    def _is_expected_shutdown_error(self, exc: Exception) -> bool:
        return self.stop_event.is_set() and self._is_eio_error(exc)

    def _is_eio_error(self, exc: Exception) -> bool:
        if isinstance(exc, OSError) and exc.errno == errno.EIO:
            return True
        inner = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
        if isinstance(inner, OSError) and inner.errno == errno.EIO:
            return True
        return "[Errno 5]" in str(exc) or "Input/output error" in str(exc)

    def connect(
        self,
        port: str | None = None,
        baudrate: int = 115200,
        timeout: float = 0.1,
        prompt_regex: str | None = None,
        session_socket: str | None = SESSION_SOCKET_AUTO,
        attach_socket: str | None = None,
    ) -> str:
        """Open the serial device, or attach to a session someone else is serving.

        Two modes, chosen by the arguments:

        * **Direct** (`port=...`, `attach_socket=None`): open the device and own
          it, serving a session socket and companion pty for the user to attach
          to. A live session at the requested path is kept intact across
          reconnects so an attached user is not dropped.

        * **Attach** (`attach_socket=...`): join a session that is already
          being served by someone else — typically the user's
          `tio -S unix:/tmp/serial-mcp-<dev>.sock <device>`. This server becomes
          a client: it never opens the port, never creates a session socket or
          pty, and disconnecting leaves the owner's tio session running.

        Passing both is rejected: exactly one side must own the device.
        """
        try:
            if port and attach_socket:
                return (
                    "Error: pass either port= (own the device) or attach_socket= "
                    "(join an existing session), not both."
                )
            if not port and not attach_socket:
                return (
                    "Error: pass port= to open a device, or attach_socket= to join an "
                    "existing session (e.g. your tio -S unix:/tmp/serial-mcp-<dev>.sock)."
                )

            prompt_pattern = self._compile_optional_regex(prompt_regex)

            if attach_socket is not None:
                return self._connect_attach(attach_socket, prompt_pattern, timeout)

            if session_socket == SESSION_SOCKET_AUTO:
                session_socket = _session_socket_for_port(port)

            keep_session = (
                bool(session_socket)
                and self.session_running.is_set()
                and self.session_path == session_socket
            )
            same_port = (
                self.transport is not None
                and self.transport.kind == "direct"
                and self.transport.is_open
                and self.transport.port == port
                and int(self.transport.baudrate or 0) == int(baudrate)
            )

            # Refuse before opening anything if the session path is already
            # served by someone else: unlinking it would break that session and
            # leave its clients attached to a socket nobody serves.
            if session_socket and not keep_session:
                if os.path.exists(session_socket) and _socket_is_live(session_socket):
                    return (
                        f"Error: session socket {session_socket} is already served by "
                        "another process (probably the user's tio, or another agent). "
                        "Join it with attach(attach_socket=...), or pass a different "
                        "session_socket path."
                    )

            # "Device busy" is not enforced by the OS for ttys: pyserial opens a
            # port another process already holds, and the two then steal each
            # other's bytes. Take an advisory lock so a second server fails
            # loudly instead of silently sharing the device.
            if not same_port and not self._claim_port_lock(port):
                holder = self._port_lock_holder(port) or "another process"
                return (
                    f"Error: {port} is already held by {holder}. Attach to that "
                    "session instead of opening the port again (see attach)."
                )

            if same_port:
                # The device link is unchanged: keep the port (and the live
                # session) instead of bouncing it on every AI connect.
                if not keep_session:
                    self._stop_session()
                self.prompt_pattern = prompt_pattern
                with self.buffer_lock:
                    self.buffer = b""
                self.log_rx_buffer = ""
                self.log_rx_last_update = None
                self.io_history.clear()
                if self.reader_thread is None:
                    self.stop_event.clear()
                    self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
                    self.reader_thread.start()
            else:
                if not keep_session:
                    self._stop_session()
                self._stop_reader()
                self._flush_rx_log_buffer(force=True)
                self._close_transport()

                self.transport = DirectTransport(port, baudrate, timeout)
                if not self.transport.is_open:
                    # Do not claim success, and do not create session paths, for
                    # a device we did not actually open.
                    self._close_transport()
                    self._release_port_lock()
                    raise OSError(f"could not open {port}")
                self.prompt_pattern = prompt_pattern
                with self.buffer_lock:
                    self.buffer = b""
                self.log_rx_buffer = ""
                self.log_rx_last_update = None
                self.io_history.clear()

                self.stop_event.clear()
                self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
                self.reader_thread.start()

            self._log("SYS", f"Connected to {port} @ {baudrate}")
            result = f"Connected to {port} at {baudrate} baud."
            if session_socket:
                if not self.session_running.is_set():
                    session_result = self._start_session(session_socket)
                    if session_result.startswith("Error"):
                        self._log("WARN", session_result)
                        result += f" (session socket unavailable: {session_result})"
                    else:
                        result += " " + _attach_hint(session_result)
                else:
                    result += " " + _attach_hint(self.session_path)
            return result
        except Exception as e:
            self._log("ERR", f"Connect failed: {e}")
            target = port or attach_socket
            return f"Error connecting to {target}: {e}"

    def attach(self, attach_socket: str | None = None, prompt_regex: str | None = None) -> str:
        """Join a session served elsewhere, auto-detecting the socket if not given.

        Detection looks for live session sockets matching the naming this server
        uses for its own sessions (serial-mcp-*.sock in the session directory).
        Exactly one must be live: zero means the user has not started a session,
        several means the choice is ambiguous and guessing could join the wrong
        device.
        """
        if not attach_socket:
            candidates = [
                p
                for p in sorted(_glob_session_sockets())
                if _socket_is_live(p)
            ]
            if not candidates:
                return (
                    "Error: no live session socket found under "
                    f"{os.path.join(SESSION_SOCKET_DIR, SESSION_SOCKET_GLOB)}. "
                    "Start one first, e.g.\n"
                    "  tio -S unix:/tmp/serial-mcp-<dev>.sock /dev/ttyUSB0\n"
                    "or pass attach_socket= explicitly."
                )
            if len(candidates) > 1:
                listed = "\n".join(f"  {p}" for p in candidates)
                return (
                    f"Error: several live session sockets found; pass attach_socket= to "
                    f"choose one:\n{listed}"
                )
            attach_socket = candidates[0]

        return self.connect(attach_socket=attach_socket, prompt_regex=prompt_regex)

    def _connect_attach(
        self, path: str, prompt_pattern: re.Pattern[bytes] | None, timeout: float
    ) -> str:
        """Join a session served elsewhere (tio --socket protocol)."""
        already = (
            self.transport is not None
            and self.transport.kind == "attach"
            and self.transport.is_open
            and self.transport.port == path
        )
        if already:
            self.prompt_pattern = prompt_pattern
            return f"Already attached to {path}."

        # Owning the device and attaching to it at the same time would fight
        # over the same bytes; drop our own session first.
        self._stop_session()
        self._stop_reader()
        self._flush_rx_log_buffer(force=True)
        self._close_transport()

        if not os.path.exists(path):
            return (
                f"Error: no session socket at {path}. Start one first, e.g.\n"
                f"  tio -S unix:{path} /dev/ttyUSB0"
            )
        if not _socket_is_live(path):
            return (
                f"Error: {path} exists but nothing is listening on it (stale file from "
                f"a dead process?). Start a session, e.g.\n  tio -S unix:{path} /dev/ttyUSB0"
            )

        self.transport = SocketTransport(path, timeout=timeout)
        self.prompt_pattern = prompt_pattern
        with self.buffer_lock:
            self.buffer = b""
        self.log_rx_buffer = ""
        self.log_rx_last_update = None
        self.io_history.clear()

        self.stop_event.clear()
        self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.reader_thread.start()

        self._log("SYS", f"Attached to session socket {path}")
        return (
            f"Attached to the session at {path}.\n"
            "The device is owned by whoever serves that socket (your tio); "
            "disconnect detaches this server without disturbing their session."
        )

    def _claim_port_lock(self, port: str) -> bool:
        """Take an advisory exclusive lock for a serial port.

        ttys are not exclusive-open, so pyserial will happily open a port another
        process already holds and the two will interleave bytes. flock on a
        per-device lock file gives a hard, self-clearing refusal: the kernel drops
        the lock when the holder exits, so a crashed server never blocks the port.

        Returns True when the lock is held (by us or already by us).
        """
        import fcntl

        if self._port_lock_path == port and self._port_lock_fd is not None:
            return True

        self._release_port_lock()
        lock_path = f"/tmp/serial-mcp-{re.sub(r'[^A-Za-z0-9_.-]', '_', os.path.basename(port))}.lock"
        try:
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError:
            # If the lock file cannot be created, do not block normal use; the
            # live-session guard still prevents the worst case.
            return True
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            try:
                os.close(fd)
            except OSError:
                pass
            return False
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode())
        except OSError:
            pass
        self._port_lock_fd = fd
        self._port_lock_path = port
        return True

    def _port_lock_holder(self, port: str) -> str | None:
        """Read the pid recorded in the port's lock file, for error messages."""
        lock_path = f"/tmp/serial-mcp-{re.sub(r'[^A-Za-z0-9_.-]', '_', os.path.basename(port))}.lock"
        try:
            with open(lock_path, encoding="utf-8") as f:
                pid = f.read().strip()
        except OSError:
            return None
        if not pid.isdigit():
            return None
        cmd = None
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
        except OSError:
            pass
        name = (cmd or "another process")[:80]
        return f"pid {pid} ({name})" if cmd else f"pid {pid}"

    def _release_port_lock(self) -> None:
        import fcntl

        fd, self._port_lock_fd = self._port_lock_fd, None
        self._port_lock_path = None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass
        # The lock file is deliberately left in place. The kernel lock lives on
        # the fd, not the path, so an empty leftover file is harmless, whereas
        # unlinking it can race: another holder's fd would survive a fresh
        # create-and-lock by a third process at the same path, letting two
        # processes believe they own the device.

    def _close_transport(self) -> None:
        transport, self.transport = self.transport, None
        if transport is not None:
            transport.close()

    def _is_connected(self) -> bool:
        """True when a transport is present and usable, either mode."""
        return self.transport is not None and self.transport.is_open

    def _connection_label(self) -> str:
        """Human-readable description of what this server is connected to."""
        if self.transport is None:
            return "nothing"
        if self.transport.kind == "attach":
            return f"session at {self.transport.port} (owned by its server, e.g. your tio)"
        return f"{self.transport.port} at {self.transport.baudrate} baud"

    @property
    def ser(self):
        """The underlying pyserial object in direct mode, else None.

        Kept for callers (and tests) that inspect the raw serial handle; attach
        mode has no local port, so this is None there.
        """
        transport = self.transport
        if transport is not None and transport.kind == "direct":
            return transport.ser
        return None

    @ser.setter
    def ser(self, value) -> None:
        """Allow tests (and any direct caller) to install a serial-like handle.

        Wrapping it in a DirectTransport keeps one read path for the reader loop,
        so a fake object is exercised exactly like a real port. Passing None
        drops the transport.
        """
        if value is None:
            transport = self.transport
            if transport is not None and transport.kind == "direct":
                transport.ser = None
            return

        transport = self.transport
        if transport is not None and transport.kind == "direct":
            transport.ser = value
            return

        wrapped = DirectTransport.__new__(DirectTransport)
        wrapped.ser = value
        wrapped.port = getattr(value, "port", "<serial>")
        wrapped.baudrate = getattr(value, "baudrate", None)
        self.transport = wrapped
    def _stop_reader(self) -> None:
        self.stop_event.set()
        if self.reader_thread:
            self.reader_thread.join(timeout=2.0)
            self.reader_thread = None

    def disconnect(self) -> str:
        was_attach = self.transport is not None and self.transport.kind == "attach"
        owner = self.transport.port if self.transport is not None else None
        self._stop_session()
        self._stop_reader()

        self._flush_rx_log_buffer(force=True)

        if self.transport is not None and self.transport.is_open:
            self._close_transport()
            self._release_port_lock()
            if was_attach:
                self._log("SYS", f"Detached from session socket {owner}")
                return (
                    f"Detached from {owner}. The session is still running (the owner's "
                    "tio was not disturbed)."
                )
            self._log("SYS", "Disconnected")
            return "Disconnected."
        self._close_transport()
        self._release_port_lock()
        return "No active connection."

    def _compile_optional_regex(self, pattern: str | None) -> re.Pattern[bytes] | None:
        if not pattern:
            return None
        return re.compile(pattern.encode("utf-8"))

    def _raw_logged_write(self, data: bytes) -> None:
        with self.write_lock:
            self._log("TX", data)
            self._record_io("TX", "ai", data)
            self.transport.write(data)
            self.transport.flush()

    @staticmethod
    def _cap_lines(text: str, max_lines: int) -> str:
        """Keep only the last `max_lines` lines, marking what was dropped.

        Console output is mostly noise (boot banners, repeated log spew), and the
        part worth reading is at the *end* — the prompt, the command result, the
        error. So the tail is what survives a cap, not the head.

        `max_lines <= 0` means unlimited, which is the default so callers that do
        not ask for a cap see exactly what they saw before.

        The elision marker is prepended rather than appended so a truncated
        result is never mistaken for a complete one; silently dropping text would
        read as "that was all the device said".
        """
        limit = int(max_lines)
        if limit <= 0:
            return text
        lines = text.splitlines(keepends=True)
        if len(lines) <= limit:
            return text
        dropped = len(lines) - limit
        kept = "".join(lines[-limit:])
        noun = "line" if dropped == 1 else "lines"
        return f"[... {dropped} earlier {noun} omitted ...]\n{kept}"

    def _snapshot_len(self) -> int:
        with self.buffer_lock:
            return len(self.buffer)

    def _consume_from(self, start: int) -> bytes:
        with self.buffer_lock:
            data = self.buffer[start:]
            self.buffer = self.buffer[:start]
            return data

    def _wait_for_output(self, start: int, timeout: float, idle_timeout: float, terminator: re.Pattern[bytes] | None) -> bytes:
        deadline = time.monotonic() + max(timeout, 0)
        last_len = start
        last_change = time.monotonic()
        saw_data = False

        while True:
            now = time.monotonic()
            with self.buffer_lock:
                current = self.buffer[start:]
                total_len = len(self.buffer)

            if total_len != last_len:
                last_len = total_len
                last_change = now
                saw_data = bool(current)

            if terminator and terminator.search(current):
                break
            if saw_data and idle_timeout >= 0 and now - last_change >= idle_timeout:
                break
            if now >= deadline:
                break
            time.sleep(min(0.01, max(deadline - now, 0)))

        return self._consume_from(start)

    def send_command(
        self,
        text: str,
        line_ending: str = "lf",
        timeout: float = 5.0,
        idle_timeout: float = 0.2,
        terminator_regex: str | None = None,
        encoding: str = "utf-8",
        max_lines: int = 0,
    ) -> str:
        if not self._is_connected():
            return "Error: Not connected to a serial port."
        if line_ending not in LINE_ENDINGS:
            return f"Error: line_ending must be one of {', '.join(LINE_ENDINGS)}."

        start = self._snapshot_len()
        payload = text.encode(encoding) + LINE_ENDINGS[line_ending]
        terminator = self._compile_optional_regex(terminator_regex) or self.prompt_pattern

        with self.write_lock:
            self._log("TX", payload)
            self._record_io("TX", "ai", payload)
            self.transport.write(payload)
            self.transport.flush()

        data = self._wait_for_output(start, timeout, idle_timeout, terminator)
        return self._cap_lines(data.decode(encoding, errors="replace"), max_lines)

    def read_output(
        self,
        timeout: float = 0.0,
        idle_timeout: float = 0.2,
        encoding: str = "utf-8",
        max_lines: int = 0,
    ) -> str:
        start = 0
        data = self._wait_for_output(start, timeout, idle_timeout, None)
        return self._cap_lines(data.decode(encoding, errors="replace"), max_lines)

    def clear_output(self) -> str:
        with self.buffer_lock:
            count = len(self.buffer)
            self.buffer = b""
        self._log("SYS", f"Cleared {count} buffered bytes")
        return f"Cleared {count} buffered bytes."

    def set_prompt_pattern(self, regex_pattern: str | None = None) -> str:
        self.prompt_pattern = self._compile_optional_regex(regex_pattern)
        if self.prompt_pattern is None:
            return "Default terminator regex cleared; idle timeout completion will be used."
        return f"Default terminator regex set to: {regex_pattern}"

    # ---- session socket (tio raw-byte protocol) ------------------------

    def _start_session(self, path: str) -> str:
        """Serve the tio-protocol session socket at `path`.

        Refuses to take the path from a live session. A serial path is shared
        state: if something is already listening there (another MCP server, or
        the user's own tio), unlinking it and binding our own would silently
        break that session — its clients would keep holding a socket nobody
        serves, and the device would appear to work while relaying nothing.
        """
        if not (self.session_running.is_set() and self.session_path == path):
            if os.path.exists(path) and _socket_is_live(path):
                return (
                    f"Error: session socket {path} is already being served by another "
                    "process. Join it with attach instead of taking it over "
                    "(connect(port=...) would otherwise steal it from its owner)."
                )

        self._stop_session()
        try:
            if os.path.exists(path):
                os.unlink(path)
            srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            srv.bind(path)
            srv.listen(16)
            srv.settimeout(0.5)
        except Exception as e:
            return f"Error starting session socket on {path}: {e}"
        self.session_server = srv
        self.session_path = path
        self.session_running.set()
        self.session_thread = threading.Thread(target=self._session_accept_loop, daemon=True)
        self.session_thread.start()

        pty_result = self._start_session_pty(path)
        if pty_result.startswith("Error"):
            self._log("WARN", pty_result)

        self._log("SYS", f"Session socket (tio protocol) listening on {path}")
        return path

    def _start_session_pty(self, session_path: str) -> str:
        """Expose a pty (symlinked at a stable path) so terminal tools can attach directly."""
        import pty as _pty

        link = session_path[: -len(".sock")] if session_path.endswith(".sock") else session_path + ".tty"
        try:
            master, slave = _pty.openpty()
            # Raw, no echo: bytes pass through unmodified in both directions
            # and slave reads are not line-buffered (like socat raw,echo=0).
            import tty as _tty
            _tty.setraw(slave)
            os.set_blocking(master, False)
            if os.path.islink(link) or os.path.exists(link):
                os.unlink(link)
            slave_name = os.ttyname(slave)
            os.symlink(slave_name, link)
            os.close(slave)
        except Exception as e:
            return f"Error creating tio pty at {link}: {e}"
        self.session_pty_master = master
        self.session_pty_link = link
        self.session_pty_thread = threading.Thread(target=self._session_pty_loop, daemon=True)
        self.session_pty_thread.start()
        self._log("SYS", f"session pty listening on {link} -> {slave_name}")
        return link

    def _session_pty_loop(self) -> None:
        """Forward bytes from an attached terminal tool to the device.

        When nothing is attached, the master fd reads EIO; that is retried
        (reopening the slave clears it on Linux), so tools can attach/detach
        repeatedly without restarting the session.
        """
        while self.session_running.is_set():
            master = self.session_pty_master
            if master is None:
                break
            try:
                data = os.read(master, 4096)
                if data:
                    self._session_user_write(data)
                else:
                    time.sleep(0.05)
            except BlockingIOError:
                time.sleep(0.05)
            except OSError:
                # EIO while no tio holds the slave; wait for the next attach
                time.sleep(0.2)

    def _stop_session(self) -> None:
        self.session_running.clear()
        with self.session_lock:
            clients = list(self.session_clients)
            self.session_clients.clear()
        for sock in clients:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        srv, path = self.session_server, self.session_path
        self.session_server = None
        self.session_path = None
        if srv is not None:
            try:
                srv.close()
            except OSError:
                pass
        if path and os.path.exists(path):
            try:
                os.unlink(path)
            except OSError:
                pass

        master, link = self.session_pty_master, self.session_pty_link
        self.session_pty_master = None
        self.session_pty_link = None
        if master is not None:
            try:
                os.close(master)
            except OSError:
                pass
        if link and (os.path.islink(link) or os.path.exists(link)):
            try:
                os.unlink(link)
            except OSError:
                pass

    def _session_accept_loop(self) -> None:
        while self.session_running.is_set():
            try:
                sock, _ = self.session_server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            sock.settimeout(0.5)
            with self.session_lock:
                self.session_clients.add(sock)
            threading.Thread(target=self._session_client_loop, args=(sock,), daemon=True).start()
            self._log("SYS", "session client attached")

    def _session_client_loop(self, sock: socket.socket) -> None:
        """Forward raw client bytes to the device, tio --socket style.

        No echo is sent back to the sender; the device echo (broadcast RX)
        shows everyone's typing, exactly like tio's sharing mode.
        """
        try:
            while self.session_running.is_set():
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                if data:
                    self._session_user_write(data)
        except OSError:
            pass
        finally:
            self._remove_session_client(sock)

    def _session_user_write(self, payload: bytes) -> None:
        with self.write_lock:
            if not self._is_connected():
                return
            try:
                self._log("TX:USER", payload)
                self._record_io("TX", "user", payload)
                self.transport.write(payload)
                self.transport.flush()
            except Exception:
                return

    def _broadcast(self, payload: bytes) -> None:
        if not self.session_running.is_set() or not payload:
            return
        with self.session_lock:
            clients = list(self.session_clients)
        for sock in clients:
            try:
                sock.sendall(payload)
            except OSError:
                self._remove_session_client(sock)
        # Also feed the companion pty, if one is exposed
        master = self.session_pty_master
        if master is not None:
            try:
                os.write(master, payload)
            except (BlockingIOError, OSError):
                pass

    def _remove_session_client(self, sock: socket.socket) -> None:
        with self.session_lock:
            self.session_clients.discard(sock)
        try:
            sock.close()
        except OSError:
            pass

    # ---- bootloader entry -----------------------------------------------

    def _merged_boot_profiles(self) -> dict:
        profiles = {k: dict(v) for k, v in BUILTIN_BOOT_PROFILES.items()}
        for key, prof in _load_json_file(USER_PROFILES_PATH).items():
            base = dict(profiles.get(key, {}))
            base.update(prof if isinstance(prof, dict) else {})
            profiles[key] = base
        return profiles

    def _redacted_write(self, data: bytes) -> None:
        """Write to the port but keep secrets out of the log and transcript."""
        with self.write_lock:
            self._log("TX", "[redacted]")
            self._record_io("TX", "ai", b"[redacted]")
            self.transport.write(data)
            self.transport.flush()

    def _blind_login(self, username: str, password: str, encoding: str, settle: float = 0.6) -> None:
        """Log in without verifying prompts.

        On consoles that are flooded with unsolicited logging (mesh daemons
        retrying, kernel spew), prompt detection is unreliable: the login banner
        scrolls past, timeouts reset the prompt, and output-based verification
        reads noise. This does what a human does instead — wakes the console,
        waits a beat, then sends username and password on a schedule and assumes
        it worked. Any wrong-prompt misdelivery simply fails and is retried by
        the caller's next cycle.
        """
        self._raw_logged_write(b"\r")
        time.sleep(settle)
        self._raw_logged_write((username + "\n").encode(encoding))
        time.sleep(settle)
        self._redacted_write((password + "\n").encode(encoding))
        time.sleep(settle)

    def _try_login(self, username: str, password: str, seen: str, encoding: str, transcript: list | None = None) -> bool:
        """Complete a login if a login prompt is visible.

        Polls for the "Password:" prompt (gives up if the device never asks),
        then submits the password without logging it. Wakes the console with a
        CR first because the prompt often only appears after a keypress.

        When `transcript` is supplied, every chunk this method consumes is
        appended to it, so callers can report *why* a login failed (the chunks
        are consumed from the shared buffer and are otherwise unavailable).
        """
        def note(text: str) -> str:
            if transcript is not None:
                transcript.append(text)
            return text

        self._raw_logged_write(b"\r")
        out = self._wait_for_output(self._snapshot_len(), timeout=2.0, idle_timeout=0.5, terminator=None)
        seen += note(out.decode(encoding, errors="replace"))
        if not re.search(r"login:", seen, re.IGNORECASE):
            return False

        self._raw_logged_write((username + "\n").encode(encoding))
        got_password = False
        deadline = time.monotonic() + 6.0
        while time.monotonic() < deadline:
            out = self._wait_for_output(
                self._snapshot_len(), timeout=1.0, idle_timeout=0.3, terminator=None
            )
            chunk = note(out.decode(encoding, errors="replace"))
            seen += chunk
            if re.search(r"password", chunk, re.IGNORECASE):
                got_password = True
                break
            if re.search(r"[#$]\s*$", chunk):
                return True  # already at a shell, no password needed
        if not got_password:
            return False

        self._redacted_write((password + "\n").encode(encoding))
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            out = self._wait_for_output(
                self._snapshot_len(), timeout=1.0, idle_timeout=0.5, terminator=None
            )
            chunk = note(out.decode(encoding, errors="replace"))
            seen += chunk
            if re.search(r"(login|password) incorrect", chunk, re.IGNORECASE):
                return False
            if re.search(r"(root@|#\s*$|\$\s*$)", chunk):
                return True
        return True

    def _poll_for_login_outcome(
        self,
        start: int,
        deadline: float,
        encoding: str,
        interval: float = 0.5,
        idle_timeout: float = 0.5,
    ) -> tuple[str, str]:
        """Read until a shell prompt or a rejection appears, or the deadline passes.

        `start` is a buffer offset (see _snapshot_len) captured *before* the login
        was typed, so output that arrived while typing is included rather than
        skipped — a device that answers during a typing pause would otherwise be
        judged as silent.

        A single idle-bounded read is not enough either: the console may pause
        mid-stream (boot banners scroll in bursts, a shell can take a moment to
        spawn), so returning on the first quiet patch reports a false negative
        for a login that actually succeeded. Poll instead, keeping every chunk,
        and stop early on a definitive result.

        Returns (accumulated_text, outcome) where outcome is one of
        "prompt", "rejected" or "unknown".
        """
        chunks: list[str] = []
        first = True
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            offset = start if first else self._snapshot_len()
            first = False
            chunk = self._wait_for_output(
                offset,
                timeout=min(interval, remaining),
                idle_timeout=min(idle_timeout, remaining),
                terminator=None,
            ).decode(encoding, errors="replace")
            if chunk:
                chunks.append(chunk)
            text = "".join(chunks)
            if self._LOGIN_FAILED_RE.search(text):
                return text, "rejected"
            if self._SHELL_PROMPT_RE.search(text):
                return text, "prompt"
        return "".join(chunks), "unknown"

    def login(
        self,
        profile: str | None = None,
        username: str | None = None,
        password: str | None = None,
        login_mode: str = "auto",
        overall_timeout: float = 20.0,
        encoding: str = "utf-8",
    ) -> str:
        """Log in to a shell console, without rebooting or entering U-Boot.

        Credentials resolve exactly like enter_bootloader's: explicit arguments
        win, else the profile's login section, else the local credentials file.
        The password is only ever submitted through _redacted_write, so it never
        appears in the log file or the view_io transcript.
        """
        if not self._is_connected():
            return "Error: Not connected to a serial port."

        prof = self._merged_boot_profiles().get(profile or "", {})
        if profile and not prof:
            return (
                f"Error: unknown profile '{profile}'. See list_boot_profiles; "
                f"user profiles live in {USER_PROFILES_PATH}."
            )
        login_cfg = prof.get("login") or {}
        cred_key = login_cfg.get("password_ref") or profile
        creds = _load_json_file(CREDENTIALS_PATH).get(cred_key, {}) if cred_key else {}
        username = username or login_cfg.get("username") or creds.get("username")
        password = password or creds.get("password")
        if not username or not password:
            return (
                "Error: no credentials available. Pass a profile with a login section "
                "whose password_ref has been stored via set_boot_credentials, or supply "
                "username/password explicitly."
            )

        mode = login_mode
        if mode == "auto":
            mode = prof.get("login_mode", "auto")

        if mode == "blind":
            # Blind typing is open-loop: nothing is verified at send time, so the
            # outcome is judged only from what arrives afterwards — and that
            # judging must include output produced while typing, not just after.
            attempt_start = self._snapshot_len()
            self._blind_login(username, password, encoding)
            tail, outcome = self._poll_for_login_outcome(
                attempt_start, time.monotonic() + max(float(overall_timeout), 1.0), encoding
            )
            if outcome == "rejected":
                return "Login rejected by the device (wrong credentials).\n\nTail of output:\n" + tail[-300:]
            if outcome == "prompt":
                return "Logged in.\n\nTail of output:\n" + tail[-300:]
            return (
                "Login typed blind and submitted, but no shell prompt was observed. "
                "The console may be flooded with logging, or the prompt differs from "
                "the expected pattern; check read_output/view_io.\n\nTail of output:\n"
                + tail[-300:]
            )

        # Prompt-driven login. If the console is already sitting at "Password:",
        # the username is in; sending it again would be read as the password.
        seen = self._wait_for_output(
            self._snapshot_len(), timeout=1.5, idle_timeout=0.3, terminator=None
        ).decode(encoding, errors="replace")
        if self._LOGIN_FAILED_RE.search(seen):
            return "Login rejected by the device (wrong credentials).\n\nTail of output:\n" + seen[-300:]
        if self._SHELL_PROMPT_RE.search(seen):
            return "Already logged in.\n\nTail of output:\n" + seen[-300:]

        if self._LOGIN_PASSWORD_RE.search(seen):
            self._redacted_write((password + "\n").encode(encoding))
            tail = self._wait_for_output(
                self._snapshot_len(), timeout=max(float(overall_timeout), 1.0),
                idle_timeout=0.8, terminator=None,
            ).decode(encoding, errors="replace")
            landed = bool(self._SHELL_PROMPT_RE.search(tail)) and not self._LOGIN_FAILED_RE.search(tail)
        else:
            # _try_login drives the prompt handshake itself and reports whether
            # it reached a shell; it consumes the output it inspects, so ask it
            # to hand that text back for accurate failure reporting.
            consumed: list[str] = []
            landed = self._try_login(username, password, seen, encoding, transcript=consumed)
            tail = "".join(consumed)

        if landed:
            return "Logged in.\n\nTail of output:\n" + (tail or seen)[-300:]
        if self._LOGIN_FAILED_RE.search(tail) or self._LOGIN_FAILED_RE.search(seen):
            return "Login rejected by the device (wrong credentials).\n\nTail of output:\n" + (tail or seen)[-300:]
        return (
            "Login did not reach a shell. The credentials may be wrong, the console "
            "may be busy, or the prompt differs from the expected pattern; check "
            "read_output/view_io.\n\nTail of output:\n" + (tail or seen)[-300:]
        )

    _LOGIN_PASSWORD_RE = re.compile(r"password\s*:\s*$", re.IGNORECASE | re.M)
    _LOGIN_FAILED_RE = re.compile(r"(login|password) incorrect", re.IGNORECASE)
    # A shell prompt is either the structured "user@host[:path]$" form, or a
    # bare marker that *starts* a line (so console chatter ending in "#" or "$"
    # mid-line is not mistaken for a prompt and reported as a false success).
    _SHELL_PROMPT_RE = re.compile(
        r"[\w.-]+@[\w.-]+[:~][^\n]*[#$]\s*$|^\s*[#$]\s*$", re.M
    )

    def enter_bootloader(
        self,
        profile: str | None = None,
        interrupt_keys: list[str] | None = None,
        success_regex: str | None = None,
        abort_regex: str | None = None,
        reboot: bool = True,
        reboot_command: str | None = None,
        delay: float | None = None,
        interval: float | None = None,
        max_attempts: int | None = None,
        login_username: str | None = None,
        login_password: str | None = None,
        login_mode: str = "auto",
        overall_timeout: float = 120.0,
        encoding: str = "utf-8",
    ) -> str:
        """Automated U-Boot entry: login (if needed) -> reboot -> paced interrupt
        tapping -> success detection. All parameters fall back to the profile,
        then to generic defaults."""
        prof = self._merged_boot_profiles().get(profile or "", {})
        if profile and not prof:
            return (
                f"Error: unknown boot profile '{profile}'. See list_boot_profiles; "
                f"user profiles live in {USER_PROFILES_PATH}."
            )
        if not self._is_connected():
            return "Error: Not connected to a serial port."

        iprof = prof.get("interrupt", {})
        rprof = prof.get("reboot", {})
        keys = interrupt_keys or iprof.get("keys", [" "])
        delay = float(delay if delay is not None else iprof.get("delay", 0.2))
        interval = float(interval if interval is not None else iprof.get("interval", 0.05))
        max_attempts = int(max_attempts if max_attempts is not None else iprof.get("max_attempts", 40))
        success_src = success_regex or prof.get("bootloader_prompt_regex", r"(U-Boot|uboot)[^\n]*[>#]\s|^\s*=>\s")
        success_re = re.compile(success_src)
        abort_src = abort_regex or prof.get("abort_regex")
        abort_re = re.compile(abort_src) if abort_src else None
        reboot_command = reboot_command or rprof.get("command", "reboot")
        max_cycles = int(rprof.get("cycles", 3)) if reboot else 1

        # Credentials: explicit args win; else resolve profile login section.
        username, password = login_username, login_password
        login_cfg = prof.get("login") or {}
        cred_key = login_cfg.get("password_ref") or profile
        creds = _load_json_file(CREDENTIALS_PATH).get(cred_key, {})
        username = username or login_cfg.get("username") or creds.get("username")
        password = password or creds.get("password")

        deadline = time.monotonic() + overall_timeout
        seen = ""
        logged_in = False
        start = self._snapshot_len()

        # Maybe already sitting in U-Boot.
        buffered = self._wait_for_output(start, timeout=0.2, idle_timeout=0.05, terminator=None)
        seen += buffered.decode(encoding, errors="replace")
        if self._is_bootloader_prompt(seen, success_re):
            return f"Already in bootloader.\n\nMatched: {seen[-300:]}"

        for cycle in range(1, max_cycles + 1):
            if time.monotonic() > deadline:
                break

            # Establish a shell first: probe the console, and if a login prompt
            # is (or becomes) visible, complete the login before rebooting.
            # Relying on previously-seen output is not enough — the prompt may
            # appear only now, and typing reboot into a login prompt just
            # produces "Login incorrect".
            if not logged_in and username and password:
                mode = login_mode
                if mode == "auto":
                    mode = prof.get("login_mode", "auto")
                if mode == "blind":
                    self._blind_login(username, password, encoding)
                    logged_in = True
                else:
                    probe = self.send_command("", line_ending="cr", timeout=3.0, idle_timeout=0.5)
                    seen += probe
                    login_re = abort_re or re.compile(r"(login:|Login:)")
                    if login_re.search(probe) or login_re.search(seen):
                        logged_in = self._try_login(username, password, seen, encoding)
                        if not logged_in:
                            # Prompt detection read noise (busy console) —
                            # fall back to blind typing on the next cycle.
                            self._blind_login(username, password, encoding)
                            logged_in = True
            elif not logged_in and (not username or not password):
                # No credentials: still allow interrupt-only flows (the user may
                # be sitting at the bootloader already or power-cycling).
                pass

            if reboot or cycle > 1:
                self._raw_logged_write((reboot_command + "\n").encode(encoding))

            # Tap continuously *through* the boot. Short autoboot windows
            # (e.g. "stop autoboot in 1 seconds") cannot be won by waiting for
            # the banner first, so keys are sent from the moment reboot is
            # issued and keep going until success or the attempt cap.
            time.sleep(max(delay, 0.0))
            taps = 0
            while taps < max_attempts:
                if time.monotonic() > deadline:
                    return self._bootloader_failure(seen, deadline_hit=True)
                self._raw_logged_write(keys[taps % len(keys)].encode(encoding))
                taps += 1
                out = self._wait_for_output(
                    self._snapshot_len(), timeout=interval, idle_timeout=0.02, terminator=None
                )
                chunk = out.decode(encoding, errors="replace")
                seen += chunk
                if self._is_bootloader_prompt(seen, success_re):
                    # Confirm the boot actually paused: if the device keeps
                    # streaming Linux boot output, the prompt was a banner.
                    settle = self._wait_for_output(
                        self._snapshot_len(), timeout=2.0, idle_timeout=0.6, terminator=None
                    )
                    settle_text = settle.decode(encoding, errors="replace")
                    seen += settle_text
                    if not self._LINUX_RESUMED_RE.search(settle_text):
                        return self._bootloader_success(seen)
                # If Linux clearly resumed, this attempt lost the window.
                if self._LINUX_RESUMED_RE.search(chunk):
                    break
                if not reboot:
                    # No reboot requested: nothing new will appear, so a short
                    # attempt budget is enough.
                    continue

        return self._bootloader_failure(seen, deadline_hit=time.monotonic() > deadline)

    # Signals that the OS actually resumed: matching success text alone is not
    # proof of a bootloader prompt (banner text, shutdown chatter and
    # "press Enter" console messages all contain similar words).
    # Signals that the OS actually came back up. Deliberately excludes shutdown
    # chatter (uloop_done, app-classifier stop, procd stopping services): those
    # appear *before* the reboot and must not abort the interrupt attempt, or
    # tapping stops seconds before the autoboot window opens.
    _LINUX_RESUMED_RE = re.compile(
        r"(Linux version \d|Kernel command line:|Freeing unused kernel|"
        r"Starting kernel \.\.\.|Run /sbin/init|procd: (Running|init))",
        re.IGNORECASE,
    )

    # A bootloader *banner* is printed on every boot, so seeing it proves
    # nothing. Only an interactive prompt (or a paused boot) counts.
    _BANNER_ONLY_RE = re.compile(r"^(U-Boot|uboot)[^\n]*\(.*\)\s*$|^U-Boot [0-9]", re.M)

    def _is_bootloader_prompt(self, seen: str, success_re: re.Pattern[str]) -> bool:
        """True only if the boot actually stopped at the bootloader.

        The banner alone is not evidence (it prints on every boot), so success is
        only claimed for an interactive prompt marker, and only while Linux has
        not started after it.
        """
        match = success_re.search(seen)
        if not match:
            return False
        matched = match.group(0)
        if self._BANNER_ONLY_RE.match(matched.strip()):
            return False
        after = seen[match.end():]
        return not self._LINUX_RESUMED_RE.search(after)

    def _bootloader_success(self, seen: str) -> str:
        return "Entered bootloader.\n\nTail of output:\n" + seen[-400:]

    def _bootloader_failure(self, seen: str, deadline_hit: bool = False) -> str:
        why = "overall timeout reached" if deadline_hit else "interrupt keys exhausted without matching the success pattern"
        return (
            f"Failed to enter bootloader ({why}).\n\n"
            "Check: correct interrupt key? autoboot window longer than tap window? "
            "device actually rebooting? Tail of output:\n" + seen[-500:]
        )

    def list_boot_profiles(self) -> str:
        profiles = self._merged_boot_profiles()
        lines = []
        for name, prof in sorted(profiles.items()):
            desc = prof.get("description", "")
            keys = prof.get("interrupt", {}).get("keys", [" "])
            login = "login: yes" if prof.get("login") or name in _load_json_file(CREDENTIALS_PATH) else "login: not configured"
            source = "builtin" if name in BUILTIN_BOOT_PROFILES else "user"
            lines.append(f"{name} [{source}]  interrupt={keys}  {login}\n  {desc}")
        return "\n".join(lines) if lines else "No profiles."

    def set_boot_credentials(self, profile_key: str, username: str, password: str) -> str:
        """Store login credentials locally (gitignored, mode 0600). Never logged."""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        try:
            creds = _load_json_file(CREDENTIALS_PATH)
            creds[profile_key] = {"username": username, "password": password}
            fd = os.open(CREDENTIALS_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(creds, f, indent=2)
            os.chmod(CREDENTIALS_PATH, 0o600)
            self._log("SYS", f"Stored credentials for profile '{profile_key}' (redacted)")
            return f"Credentials stored locally for '{profile_key}' at {CREDENTIALS_PATH} (mode 0600, gitignored location)."
        except Exception as e:
            return f"Error storing credentials: {e}"

    # ---- inspection tools ----------------------------------------------

    def view_io(self, lines: int = 50, direction: str = "all", hex_mode: bool = False, encoding: str = "utf-8") -> str:
        if direction not in ("all", "tx", "rx"):
            return "Error: direction must be one of all, tx, rx."
        records = [
            r for r in self.io_history if direction == "all" or r[1].lower() == direction
        ]
        records = list(records[-max(int(lines), 1) :])
        if not records:
            return "No I/O recorded yet (connect and send something first)."
        out = []
        for ts, dir_, source, data in records:
            stamp = time.strftime("%H:%M:%S", time.localtime(ts))
            label = "[RX]" if dir_ == "RX" else f"[TX:{source}]"
            if hex_mode:
                body = " ".join(f"{b:02x}" for b in data) if data else "(empty)"
            else:
                body = data.decode(encoding, errors="replace").replace("\r", "")
            out.append(f"[{stamp}] {label} {body}")
        return "\n".join(out)

    def list_serial_ports(self) -> str:
        ports = list(serial.tools.list_ports.comports())
        if not ports:
            return "No serial ports found."
        return "\n".join(
            f"{p.device}  {p.description}" + (f"  (hwid: {p.hwid})" if p.hwid else "")
            for p in ports
        )

    def tio_info(self) -> str:
        if not self._is_connected():
            return (
                "Not connected. Call connect(port=...) to open the device, or "
                "connect(attach_socket=...) to join a session you are already "
                "serving with tio."
            )
        if self.transport.kind == "attach":
            return (
                f"Attached to the session at {self.transport.port}.\n"
                "The device is owned by whoever serves that socket (your tio), so "
                "this server holds no port and exposes no session of its own.\n"
                f"Disconnect detaches without disturbing {self.transport.port}.\n"
            )
        if not self.session_running.is_set() or not self.session_path:
            return (
                f"Connected to {self.transport.port}, but the session socket is not "
                "running (reconnect to restart it)."
            )
        with self.session_lock:
            count = len(self.session_clients)
        return (
            f"Device {self.transport.port} is held by this MCP session; "
            f"{count} client(s) attached.\n"
            + _attach_hint(self.session_path)
        )


state = SerialState()


@mcp.tool()
def connect(
    port: str | None = None,
    baudrate: int = 115200,
    timeout: float = 0.1,
    prompt_regex: str | None = None,
    session_socket: str | None = SESSION_SOCKET_AUTO,
    attach_socket: str | None = None,
) -> str:
    """Open a serial port (owning it), or join a session someone else serves.

    Two modes:

    * `port=...` — open the device and own it. The user then attaches to this
      server's session socket or companion pty. Calling it again for the same
      device keeps the running session (and anyone attached) alive.
    * `attach_socket=...` — join a session already being served, typically the
      user's own `tio -S unix:/tmp/serial-mcp-<dev>.sock <device>`. This server
      becomes a client and never opens the port, so the user's tio keeps owning
      the device and keeps running when this server detaches.

    Prefer attach mode when the user is already in tio: it cannot lock them out,
    and several agent sessions can join one session without fighting over the
    device. Pass exactly one of `port` or `attach_socket`.

    Args:
        port: Serial device path to open and own, e.g. /dev/ttyUSB0.
        baudrate: Serial baud rate (direct mode).
        timeout: Low-level read timeout used by the reader thread.
        prompt_regex: Optional default regex terminator for send_command completion.
        session_socket: UNIX socket this server serves in direct mode. "auto"
            derives /tmp/serial-mcp-<dev>.sock; "" disables it.
        attach_socket: Session socket to join instead of opening a device.
    """
    return state.connect(port, baudrate, timeout, prompt_regex, session_socket, attach_socket)


@mcp.tool()
def attach(attach_socket: str | None = None, prompt_regex: str | None = None) -> str:
    """Join a session the user is already serving (e.g. their tio session).

    Convenience wrapper for connect(attach_socket=...) that defaults the socket
    path to /tmp/serial-mcp-<dev>.sock for a known device, or auto-detects the
    live session socket when exactly one exists.

    Args:
        attach_socket: Session socket to join. Defaults to auto-detection.
        prompt_regex: Optional default regex terminator for send_command.
    """
    return state.attach(attach_socket, prompt_regex)


@mcp.tool()
def disconnect() -> str:
    """Close the active serial connection, stop the reader, and drop session clients."""
    return state.disconnect()


@mcp.tool()
def send_command(
    text: str,
    line_ending: str = "lf",
    timeout: float = 5.0,
    idle_timeout: float = 0.2,
    terminator_regex: str | None = None,
    encoding: str = "utf-8",
    max_lines: int = 0,
) -> str:
    """Send text bytes to the serial port and return output observed after the send.

    Completion uses terminator_regex (or the connection default prompt_regex) when supplied;
    otherwise it returns after output has been quiet for idle_timeout, bounded by timeout.
    line_ending is explicit: none, lf, cr, or crlf. Data already buffered before this call is
    preserved for read_output; this call consumes only bytes received after its send point.

    Args:
        text: Bytes to send.
        line_ending: One of none, lf, cr, crlf.
        timeout: Overall cap in seconds for waiting on output.
        idle_timeout: Seconds of silence that ends the wait.
        terminator_regex: Regex that marks completion (overrides prompt_regex).
        encoding: Text encoding for the serial stream.
        max_lines: Return at most this many lines, keeping the *last* ones where
            the prompt/result/error lives and noting how many earlier lines were
            dropped. Use it to avoid pulling large boot banners or repetitive log
            spew into context. 0 (the default) returns everything. Ignored for
            hex/raw needs — raise it or use view_io(direction="rx") instead.
    """
    return state.send_command(
        text, line_ending, timeout, idle_timeout, terminator_regex, encoding, max_lines
    )


@mcp.tool()
def read_output(
    timeout: float = 0.0,
    idle_timeout: float = 0.2,
    encoding: str = "utf-8",
    max_lines: int = 0,
) -> str:
    """Consume and return buffered/asynchronously arriving serial output.

    If timeout is 0, returns currently buffered data immediately. If timeout is positive,
    waits for data and then for idle_timeout of silence, never exceeding timeout.

    Args:
        timeout: Overall cap in seconds.
        idle_timeout: Seconds of silence that ends the wait.
        encoding: Text encoding for the serial stream.
        max_lines: Return at most this many lines, keeping the *last* ones and
            noting how many earlier lines were dropped. Useful when the buffer has
            accumulated boot or log noise you do not need in context. 0 (the
            default) returns everything.
    """
    return state.read_output(timeout, idle_timeout, encoding, max_lines)


@mcp.tool()
def clear_output() -> str:
    """Discard all currently buffered serial output."""
    return state.clear_output()


@mcp.tool()
def set_prompt_pattern(regex_pattern: str | None = None) -> str:
    """Set or clear the default regex terminator used by send_command."""
    return state.set_prompt_pattern(regex_pattern)


@mcp.tool()
def list_boot_profiles() -> str:
    """List known bootloader-entry profiles (builtin + user-defined).

    User profiles live in ~/.config/serial-mcp/profiles.json; credentials are
    stored separately in ~/.config/serial-mcp/credentials.json (mode 0600) and
    are never displayed.
    """
    return state.list_boot_profiles()


@mcp.tool()
def set_boot_credentials(profile_key: str, username: str, password: str) -> str:
    """Store device login credentials locally for use by enter_bootloader.

    Credentials are written to ~/.config/serial-mcp/credentials.json with mode
    0600 (outside any repo). They are redacted from logs and view_io transcripts.

    Args:
        profile_key: Key that profiles reference via login.password_ref (or the
            profile name itself).
        username: Login username.
        password: Login password (stored locally only; redacted in logs).
    """
    return state.set_boot_credentials(profile_key, username, password)


@mcp.tool()
def login(
    profile: str | None = None,
    username: str | None = None,
    password: str | None = None,
    login_mode: str = "auto",
    overall_timeout: float = 20.0,
    encoding: str = "utf-8",
) -> str:
    """Log in to the device's shell console (no reboot, no bootloader entry).

    Credentials resolve like enter_bootloader's: explicit arguments win, else the
    profile's login section, else the local credentials file. The password is
    always written through the redacting path, so it never reaches the log file
    or the view_io transcript — prefer a profile + set_boot_credentials over
    passing a secret as an argument.

    Args:
        profile: Profile whose login section provides username/password_ref
            (see list_boot_profiles).
        username: Explicit username (overrides profile/credentials).
        password: Explicit password (overrides profile/credentials; prefer stored
            credentials so the secret stays out of call arguments).
        login_mode: "auto" detects the login/password prompts; "blind" types
            Enter, username, password on a schedule, for consoles flooded with
            unsolicited logging where prompt detection reads noise.
        overall_timeout: Wall-clock cap in seconds for the whole login.
        encoding: Text encoding for the serial stream.
    """
    return state.login(profile, username, password, login_mode, overall_timeout, encoding)


@mcp.tool()
def enter_bootloader(
    profile: str | None = None,
    interrupt_keys: list[str] | None = None,
    success_regex: str | None = None,
    abort_regex: str | None = None,
    reboot: bool = True,
    reboot_command: str | None = None,
    delay: float | None = None,
    interval: float | None = None,
    max_attempts: int | None = None,
    login_username: str | None = None,
    login_password: str | None = None,
    login_mode: str = "auto",
    overall_timeout: float = 120.0,
    encoding: str = "utf-8",
) -> str:
    """Enter the device's U-Boot bootloader automatically.

    Flow: optionally log in (if the device sits at a login prompt), reboot,
    wait for the autoboot window, then send interrupt keys at a paced interval
    until the bootloader prompt appears. Never loops forever: attempts are
    capped and the whole operation has a wall-clock timeout.

    Args:
        profile: Named boot profile (see list_boot_profiles). Provides defaults
            for everything below.
        interrupt_keys: Characters to tap during the autoboot window, e.g. [" "].
            Sent repeatedly; each key is tried max_attempts times.
        success_regex: Regex indicating the bootloader prompt was reached.
        abort_regex: Regex indicating the device booted fully (e.g. "Login:") —
            triggers another login+reboot cycle instead of endless tapping.
        reboot: Whether to issue the reboot command first (False = device is
            already cycling / power-cycled externally).
        reboot_command: Shell command to reboot (default from profile or "reboot").
        delay: Seconds to wait after reboot before tapping begins.
        interval: Seconds between interrupt key taps (pacing protects the RX
            buffer — flooding can cause dropped characters in U-Boot).
        max_attempts: Hard cap on interrupt keys per pass.
        login_username: Explicit username (overrides profile/credentials).
        login_password: Explicit password (overrides profile/credentials;
            prefer set_boot_credentials so secrets stay out of logs).
        login_mode: "auto" detects prompts and falls back to blind typing;
            "blind" skips all prompt verification and fires Enter, username,
            password on a fixed schedule. Use "blind" on consoles flooded with
            unsolicited logging, where prompt detection reads noise.
        overall_timeout: Wall-clock cap in seconds for the entire operation.
        encoding: Text encoding for the serial stream.
    """
    return state.enter_bootloader(
        profile, interrupt_keys, success_regex, abort_regex, reboot,
        reboot_command, delay, interval, max_attempts,
        login_username, login_password, login_mode, overall_timeout, encoding,
    )


@mcp.tool()
def view_io(lines: int = 50, direction: str = "all", hex_mode: bool = False, encoding: str = "utf-8") -> str:
    """View recent serial I/O (tio-style timestamped TX/RX transcript).

    Shows what was sent to the device ([TX:ai] by the AI, [TX:user] by attached
    session users) and what came back ([RX]). direction filters to tx, rx, or
    all. hex_mode shows raw bytes in hex like tio --hex.

    Args:
        lines: Maximum number of records to show (most recent first-tail).
        direction: One of all, tx, rx.
        hex_mode: Render payloads as hex instead of text.
        encoding: Text decoding for non-hex output.
    """
    return state.view_io(lines, direction, hex_mode, encoding)


@mcp.tool()
def list_serial_ports() -> str:
    """List available serial ports with descriptions (like tio --list)."""
    return state.list_serial_ports()


@mcp.tool()
def tio_info() -> str:
    """Show connection status, the session socket path, and attach instructions."""
    return state.tio_info()


def main() -> None:
    """Entry point for the serial-mcp console script (uvx/pip installs).

    The device is opened only when the AI calls connect. Opening at startup
    would race when several MCP servers run at once (one per agent session):
    each would open the same port and create its own session socket and pty at
    the same paths, so the last one to start would silently steal them from the
    others.
    """
    mcp.run()


if __name__ == "__main__":
    main()
