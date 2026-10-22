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
import stat
import posixpath
import sys
import shutil
import subprocess
import struct
import base64
import binascii
import hashlib
import shlex
import tempfile
import secrets
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


def _serial_device_id(port: str) -> int:
    """Return st_rdev for a character device, normalizing symlink aliases."""
    device = os.stat(port)
    if not stat.S_ISCHR(device.st_mode):
        raise OSError(errno.ENOTTY, f"{port} is not a character device")
    return device.st_rdev


def _serial_port_lock_path(device_id: int) -> str:
    """Advisory cross-agent lock path keyed by device number, not pathname."""
    return f"/tmp/serial-mcp-{os.major(device_id)}-{os.minor(device_id)}.lock"


class DirectTransport:
    """Owns a serial device: the server is the only party holding the port.

    Exposes the same minimal interface as SocketTransport so the reader loop and
    every tool work identically over either transport.
    """

    kind = "direct"

    def __init__(self, port: str, baudrate: int, timeout: float, serial_instance=None, device_id: int | None = None) -> None:
        self.ser = serial_instance if serial_instance is not None else serial.Serial(port, baudrate, timeout=timeout)
        self.port = port
        self.baudrate = baudrate
        if device_id is not None:
            self.device_id = device_id
        else:
            try:
                self.device_id = _serial_device_id(port)
            except OSError:
                self.device_id = None

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
            if self.ser and self.ser.is_open and getattr(self.ser, "_serial_mcp_tioc_excl_owned", False):
                import fcntl
                import termios
                fcntl.ioctl(self.ser.fileno(), termios.TIOCNXCL)
                self.ser._serial_mcp_tioc_excl_owned = False
        except OSError:
            pass
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

    def __init__(self, log_path: str | None = None) -> None:
        self.transport: DirectTransport | SocketTransport | None = None
        self.default_timeout = 5.0
        self.default_idle_timeout = 0.2
        self.prompt_pattern: re.Pattern[bytes] | None = None
        # SERIAL_MCP_LOG lets the shared daemon keep its own log: the module-level
        # `state` below is created at import and opens this file for writing.
        self.log_path = log_path or os.environ.get("SERIAL_MCP_LOG", "/tmp/serial-mcp.log")
        # Callables receiving each device RX chunk (the shared daemon fans
        # output out to its subscribers). They must not block.
        self.rx_listeners: list = []

        self.reader_thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.buffer = b""
        self.buffer_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.io_lock = threading.RLock()
        self.operation_lock = threading.RLock()
        self.transfer_active = threading.Event()
        self._transfer_cleanup_paths: list[str] = []
        self._transfer_cleanup_command: str | None = None
        self._transfer_target_install_attempted = False
        self._transfer_trace: list[str] = []
        self._transfer_recent = bytearray()
        self._linux_cat_foreground = False
        self._session_transfer_dropped_bytes = 0
        self._transfer_generation = 0
        self._transfer_stage = "idle"
        self.log_lock = threading.Lock()
        self.log_rx_buffer = ""
        self.log_rx_last_update: float | None = None
        self.log_rx_idle_flush_interval = 0.1

        # Advisory exclusive lock for the owned serial port (see _claim_port_lock)
        self._port_lock_fd: int | None = None
        self._port_lock_device_id: int | None = None

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
                if self.transfer_active.is_set():
                    time.sleep(0.01)
                    continue
                # Transfers take this lock for their entire wire protocol. This
                # prevents the background reader from consuming protocol bytes.
                if not self.io_lock.acquire(timeout=0.05):
                    continue
                try:
                    if self.transfer_active.is_set():
                        continue
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
                                for listener in list(self.rx_listeners):
                                    try:
                                        listener(chunk)
                                    except Exception as listener_exc:
                                        self._log("ERR", f"RX listener failed: {listener_exc}")
                        else:
                            self._flush_rx_log_buffer(force=False)
                            time.sleep(0.01)
                    else:
                        self._flush_rx_log_buffer(force=False)
                        time.sleep(0.1)
                finally:
                    self.io_lock.release()
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
        if self.transfer_active.is_set():
            return "Error: cannot reconnect while a file transfer owns serial I/O."
        with self.operation_lock:
            if self.transfer_active.is_set():
                return "Error: cannot reconnect while a file transfer owns serial I/O."
            return self._connect_locked(port, baudrate, timeout, prompt_regex, session_socket, attach_socket)

    def _connect_locked(
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
            if self.transfer_active.is_set():
                return "Error: cannot reconnect while a file transfer owns serial I/O."
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

            auto_session = session_socket == SESSION_SOCKET_AUTO
            device_id = None
            same_direct_device = False
            if auto_session:
                try:
                    device_id = _serial_device_id(port)
                except OSError:
                    # Preserve the established ordering: a live session-path
                    # collision is reported before an invalid device path.
                    pass
                same_direct_device = (
                    device_id is not None
                    and self.transport is not None
                    and self.transport.kind == "direct"
                    and self.transport.is_open
                    and getattr(self.transport, "device_id", None) == device_id
                )
                session_socket = (
                    self.session_path
                    if same_direct_device and self.session_running.is_set()
                    else _session_socket_for_port(port)
                )

            keep_session = (
                bool(session_socket)
                and self.session_running.is_set()
                and self.session_path == session_socket
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

            if device_id is None:
                device_id = _serial_device_id(port)
            same_direct_device = (
                self.transport is not None
                and self.transport.kind == "direct"
                and self.transport.is_open
                and getattr(self.transport, "device_id", None) == device_id
            )
            same_port = same_direct_device and int(self.transport.baudrate or 0) == int(baudrate)

            # This advisory lock protects against another serial-mcp owner;
            # TIOCEXCL and open-handle detection below protect against tio and
            # other non-cooperating programs, including device-path aliases.
            if not same_port and not self._claim_port_lock(port):
                holder = self._port_lock_holder(port)
                if holder:
                    return (
                        f"Error: {port} is already held by {holder}. Attach to that "
                        "owner's tio socket, or ask the owner to close the device."
                    )
                return f"Error: could not establish the supplementary serial-mcp lock for {port}; refusing direct open."

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

                direct_serial = None
                try:
                    direct_serial = self._open_owned_serial(port, baudrate, timeout)
                    self.transport = DirectTransport(port, baudrate, timeout, serial_instance=direct_serial, device_id=device_id)
                except Exception:
                    if direct_serial is not None:
                        try:
                            if getattr(direct_serial, "_serial_mcp_tioc_excl_owned", False):
                                import fcntl
                                import termios
                                fcntl.ioctl(direct_serial.fileno(), termios.TIOCNXCL)
                                direct_serial._serial_mcp_tioc_excl_owned = False
                            direct_serial.close()
                        except Exception:
                            pass
                    self._release_port_lock()
                    raise
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

    @staticmethod
    def _process_name(pid: int) -> str:
        try:
            with open(f"/proc/{pid}/comm", encoding="utf-8") as f:
                return f.read().strip()[:64] or "process"
        except OSError:
            return "process"

    def _find_open_serial_holders(self, device_id: int, port: str) -> tuple[set[int], set[int], str | None]:
        """Find other visible openers using fuser plus /proc st_rdev alias checks.

        The proc scan deliberately checks same-UID processes even when the path
        used to open the tty was an alias. Unknown same-UID proc entries make the
        result inconclusive; direct-open fails closed in that case.
        """
        holders: set[int] = set()
        incomplete: set[int] = set()
        fuser = shutil.which("fuser")
        fuser_checked = False
        if fuser:
            try:
                result = subprocess.run(
                    [fuser, "-n", "file", port], capture_output=True, text=True,
                    timeout=2.0, check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                return holders, incomplete, f"could not check existing tty owners with fuser: {exc}"
            if result.returncode not in (0, 1) or re.search(r"permission denied|cannot stat|can't stat", result.stderr, re.I):
                return holders, incomplete, "fuser could not reliably inspect open tty owners"
            fuser_checked = True
            for token in result.stdout.split():
                if token.isdigit() and int(token) != os.getpid():
                    holders.add(int(token))

        proc_root = "/proc"
        try:
            proc_entries = list(os.scandir(proc_root))
        except OSError as exc:
            return holders, incomplete, f"cannot inspect {proc_root}: {exc}"
        euid = os.geteuid()
        for process in proc_entries:
            if not process.name.isdigit() or int(process.name) == os.getpid():
                continue
            pid = int(process.name)
            try:
                with open(f"/proc/{pid}/status", encoding="utf-8") as f:
                    uid_line = next((line for line in f if line.startswith("Uid:")), "")
                owner_uid = int(uid_line.split()[1])
            except (FileNotFoundError, ProcessLookupError):
                continue
            except (OSError, ValueError, IndexError):
                continue
            if owner_uid != euid:
                if not fuser_checked:
                    incomplete.add(pid)
                continue
            try:
                descriptors = list(os.scandir(f"/proc/{pid}/fd"))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError:
                incomplete.add(pid)
                continue
            except OSError:
                continue
            for descriptor in descriptors:
                try:
                    opened = descriptor.stat(follow_symlinks=True)
                except (FileNotFoundError, ProcessLookupError):
                    continue
                except PermissionError:
                    incomplete.add(pid)
                    continue
                except OSError:
                    continue
                if stat.S_ISCHR(opened.st_mode) and opened.st_rdev == device_id:
                    holders.add(pid)
                    break
        holders.discard(os.getpid())
        if incomplete and not fuser_checked:
            return holders, incomplete, None
        # When fuser succeeds it is the primary process-wide check; /proc fills
        # in visible same-UID alias descriptors. fuser's own permission errors
        # above are fatal rather than being mistaken for an empty result.
        if fuser_checked:
            incomplete.clear()
        return holders, incomplete, None

    def _open_owned_serial(self, port: str, baudrate: int, timeout: float):
        """Open a tty without flushing RX, exclude future opens, then detect prior owners.

        On Linux, TIOCEXCL is kernel-enforced for later opens. It is not enough
        by itself: an already-open tio remains open, so visible holders are
        checked before serial configuration or any read/write. Pyserial's
        `exclusive=True` flock is supplementary, never treated as proof of
        exclusive ownership. Other platforms without these checks fail closed.
        """
        if not sys.platform.startswith("linux"):
            raise OSError(
                errno.ENOTSUP,
                "safe direct ownership cannot be established on this platform; "
                "join an already-served serial session with attach instead",
            )
        try:
            import fcntl
            import termios
        except ImportError as exc:
            raise OSError(errno.ENOTSUP, "Linux TIOCEXCL support is unavailable; use attach") from exc

        device_id = _serial_device_id(port)
        holders, incomplete, scan_error = self._find_open_serial_holders(device_id, port)
        if scan_error:
            raise OSError(errno.EACCES, f"{scan_error}; refusing direct open rather than risk stealing serial I/O")
        if incomplete:
            raise OSError(errno.EACCES, "cannot verify existing serial owners; refusing direct open")
        if holders:
            labels = ", ".join(f"pid {pid} ({self._process_name(pid)})" for pid in sorted(holders))
            raise OSError(errno.EBUSY, f"{port} is already open by {labels}; use its tio socket with attach(), or ask its owner to close it")
        try:
            fd = os.open(port, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
        except OSError as exc:
            if exc.errno in (errno.EBUSY, errno.EACCES):
                raise OSError(
                    exc.errno,
                    f"{port} is busy or exclusively held; attach to its owner's tio session instead of opening it. "
                    "If fuser shows no owner, verify the tty's TIOCEXCL state before recovery.",
                ) from exc
            raise

        ser = None
        pipe_fds: list[int] = []
        exclusive_set = False
        try:
            opened = os.fstat(fd)
            if not stat.S_ISCHR(opened.st_mode) or opened.st_rdev != device_id:
                raise OSError(errno.ENODEV, f"{port} changed device identity during open; refusing direct connection")
            if not hasattr(termios, "TIOCEXCL"):
                raise OSError(errno.ENOTSUP, "TIOCEXCL is unavailable; cannot prevent competing direct opens")
            try:
                prior_exclusive = bytearray(struct.pack("i", 0))
                fcntl.ioctl(fd, 0x80045440, prior_exclusive, True)  # Linux TIOCGEXCL
            except OSError as exc:
                raise OSError(errno.ENOTSUP, "cannot inspect prior TTY exclusive state; refusing direct open") from exc
            if struct.unpack("i", prior_exclusive)[0]:
                raise OSError(
                    errno.EBUSY,
                    f"{port} is already marked TIOCEXCL; refusing to alter its owner's state. "
                    "Use its tio socket if available; otherwise verify with fuser that no owner remains before recovery.",
                )

            # Recheck after our non-flushing open but before setting TIOCEXCL to
            # close the probe/open race without changing an existing owner's tty.
            holders, incomplete, scan_error = self._find_open_serial_holders(device_id, port)
            if scan_error or incomplete:
                detail = scan_error or "open-owner scan was incomplete"
                raise OSError(errno.EACCES, f"{detail}; refusing direct open")
            if holders:
                labels = ", ".join(f"pid {pid} ({self._process_name(pid)})" for pid in sorted(holders))
                raise OSError(errno.EBUSY, f"{port} is already open by {labels}; use its tio socket with attach(), or ask its owner to close it")

            fcntl.ioctl(fd, termios.TIOCEXCL)
            exclusive_set = True
            holders, incomplete, scan_error = self._find_open_serial_holders(device_id, port)
            if scan_error or incomplete or holders:
                detail = scan_error or (f"open by pid(s) {', '.join(map(str, sorted(holders)))}" if holders else "open-owner scan was incomplete")
                fcntl.ioctl(fd, termios.TIOCNXCL)
                exclusive_set = False
                raise OSError(errno.EBUSY, f"{port} changed ownership during TIOCEXCL check ({detail}); refusing direct open")

            # Attach pyserial to the already-open, exclusively claimed fd. This
            # avoids pyserial.open()'s input-buffer reset before the ownership
            # scan and keeps its normal timeout/read/write implementation.
            ser = serial.Serial(None, baudrate, timeout=timeout, exclusive=True)
            ser.port = port
            ser.fd = fd
            ser.pipe_abort_read_r = ser.pipe_abort_read_w = None
            ser.pipe_abort_write_r = ser.pipe_abort_write_w = None
            ser._reconfigure_port(force_update=True)
            ser.pipe_abort_read_r, ser.pipe_abort_read_w = os.pipe()
            pipe_fds.extend((ser.pipe_abort_read_r, ser.pipe_abort_read_w))
            ser.pipe_abort_write_r, ser.pipe_abort_write_w = os.pipe()
            pipe_fds.extend((ser.pipe_abort_write_r, ser.pipe_abort_write_w))
            fcntl.fcntl(ser.pipe_abort_read_r, fcntl.F_SETFL, os.O_NONBLOCK)
            fcntl.fcntl(ser.pipe_abort_write_r, fcntl.F_SETFL, os.O_NONBLOCK)
            ser.is_open = True
            ser._serial_mcp_tioc_excl_owned = True
            exclusive_set = False
            try:
                if not ser._dsrdtr:
                    ser._update_dtr_state()
                if not ser._rtscts:
                    ser._update_rts_state()
            except OSError as exc:
                if exc.errno not in (errno.EINVAL, errno.ENOTTY):
                    raise
            return ser
        except Exception:
            if exclusive_set:
                try:
                    import fcntl
                    import termios
                    fcntl.ioctl(fd, termios.TIOCNXCL)
                except OSError:
                    pass
            if ser is not None and ser.is_open:
                try:
                    if getattr(ser, "_serial_mcp_tioc_excl_owned", False):
                        fcntl.ioctl(ser.fileno(), termios.TIOCNXCL)
                        ser._serial_mcp_tioc_excl_owned = False
                    ser.close()
                    fd = -1
                    pipe_fds.clear()
                except Exception:
                    pass
            for pipe_fd in pipe_fds:
                try:
                    os.close(pipe_fd)
                except OSError:
                    pass
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
            raise

    def _claim_port_lock(self, port: str) -> bool:
        """Take a supplementary advisory lock; it does not exclude tio.

        TIOCEXCL and existing-open detection, not this lock, protect direct opens.
        The supplementary lock prevents concurrent serial-mcp owners.
        It is keyed by device number so path aliases share the lock.
        The kernel releases it when the holder exits.

        Returns True when held by us or already by us.
        """
        import fcntl

        device_id = _serial_device_id(port)
        if self._port_lock_device_id == device_id and self._port_lock_fd is not None:
            return True

        self._release_port_lock()
        lock_path = _serial_port_lock_path(device_id)
        try:
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError:
            return False
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
        self._port_lock_device_id = device_id
        return True

    def _port_lock_holder(self, port: str) -> str | None:
        """Read the pid recorded in the supplementary lock file, if available."""
        try:
            lock_path = _serial_port_lock_path(_serial_device_id(port))
        except OSError:
            return None
        try:
            with open(lock_path, encoding="utf-8") as f:
                pid = f.read().strip()
        except OSError:
            return None
        if not pid.isdigit():
            return None
        name = self._process_name(int(pid))
        return f"pid {pid} ({name})"

    def _release_port_lock(self) -> None:
        import fcntl

        fd, self._port_lock_fd = self._port_lock_fd, None
        self._port_lock_device_id = None
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
        if self.transfer_active.is_set():
            return "Error: cannot disconnect while a file transfer owns serial I/O."
        with self.operation_lock:
            if self.transfer_active.is_set():
                return "Error: cannot disconnect while a file transfer owns serial I/O."
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

        if self.transfer_active.is_set():
            return "Error: a file transfer currently owns serial I/O."
        with self.operation_lock:
            if self.transfer_active.is_set():
                return "Error: a file transfer currently owns serial I/O."
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
        if self.transfer_active.is_set():
            return "Error: file transfer currently owns serial I/O."
        with self.operation_lock:
            if self.transfer_active.is_set():
                return "Error: file transfer currently owns serial I/O."
            data = self._wait_for_output(0, timeout, idle_timeout, None)
            return self._cap_lines(data.decode(encoding, errors="replace"), max_lines)

    def clear_output(self) -> str:
        if self.transfer_active.is_set():
            return "Error: file transfer currently owns serial I/O."
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

    # ---- bounded, exclusive file transfers ------------------------------

    MAX_FILE_TRANSFER_BYTES = 8 * 1024 * 1024
    MAX_LINUX_COMMAND_BYTES = 240

    def _transfer_begin(
        self, timeout: float, confirm_shared_quiescence: bool = False
    ) -> tuple[float | None, str | None]:
        if not self._is_connected():
            return None, "Error: not connected to a serial device/session."
        if not 1 <= timeout <= 1800:
            return None, "Error: timeout must be between 1 and 1800 seconds."
        if self.transfer_active.is_set():
            return None, "Error: another file transfer is already active."
        if self.transport.kind == "attach" and not confirm_shared_quiescence:
            return None, (
                "Error: an attached tio-style socket has no transfer arbitration. "
                "Before retrying, ensure every human/other client will remain idle "
                "for the whole transfer, then set confirm_shared_quiescence=true. "
                "Linux SHA-256/length checks and U-Boot YMODEM/readback detect many "
                "collisions but cannot prevent competing input."
            )
        started = time.monotonic()
        deadline = started + timeout
        # Serialize with command/login/connect operations while leaving the RX
        # reader active until the current operation has finished.
        if not self.operation_lock.acquire(timeout=timeout):
            return None, "Error: serial I/O did not become available before the transfer timeout."
        if time.monotonic() >= deadline:
            self.operation_lock.release()
            return None, "Error: transfer timeout expired while waiting for exclusive serial I/O."

        with self.session_lock:
            self._session_transfer_dropped_bytes = 0
            self._transfer_generation += 1
            self.transfer_active.set()
        self._transfer_stage = "acquiring-exclusive-rx"
        if self.session_running.is_set():
            self._session_transfer_notice(
                "File transfer active: terminal input is temporarily paused; keystrokes are discarded."
            )
        if not self.io_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            self.transfer_active.clear()
            self.operation_lock.release()
            if self.session_running.is_set():
                self._session_transfer_notice("Transfer did not start; terminal input has resumed.")
            return None, "Error: serial reader did not yield before the transfer timeout."
        if not self._is_connected():
            self.io_lock.release()
            self.operation_lock.release()
            self.transfer_active.clear()
            if self.session_running.is_set():
                self._session_transfer_notice("Transfer did not start; terminal input has resumed.")
            return None, "Error: serial connection was lost before transfer started."
        with self.buffer_lock:
            self.buffer = b""
        # Drop stale prompts/chatter so they cannot satisfy the first U-Boot
        # prompt match. Transfer bytes themselves remain exclusively on this path.
        drain_deadline = min(deadline, time.monotonic() + 0.5)
        quiet_since = time.monotonic()
        try:
            while time.monotonic() < drain_deadline:
                stale = self._transfer_read(4096)
                if stale:
                    quiet_since = time.monotonic()
                elif time.monotonic() - quiet_since >= 0.05:
                    break
                else:
                    time.sleep(0.005)
        except OSError as exc:
            self.io_lock.release()
            self.operation_lock.release()
            self.transfer_active.clear()
            if self.session_running.is_set():
                self._session_transfer_notice("Transfer did not start; terminal input has resumed.")
            return None, f"Error: serial connection failed before transfer: {exc}"
        self._transfer_stage = "transfer-ready"
        return deadline, None

    def _session_transfer_notice(self, message: str, clients: list[socket.socket] | None = None) -> None:
        """Notify our own clients out-of-band; never write notice bytes to device."""
        payload = f"\r\n[serial-mcp] {message}\r\n".encode("utf-8")
        if clients is None:
            with self.session_lock:
                clients = list(self.session_clients)
        for sock in clients:
            try:
                sock.sendall(payload)
            except OSError:
                self._remove_session_client(sock)
        master = self.session_pty_master
        if master is not None:
            try:
                os.write(master, payload)
            except (BlockingIOError, OSError):
                pass

    def _transfer_end(self) -> None:
        self.io_lock.release()
        self.operation_lock.release()
        self.transfer_active.clear()
        if self.session_running.is_set():
            with self.session_lock:
                dropped = self._session_transfer_dropped_bytes
            self._session_transfer_notice(
                f"Transfer ended; terminal input resumed. {dropped} byte(s) typed during transfer were discarded."
            )
        self._transfer_stage = "idle"

    def _transfer_read(self, size: int = 4096) -> bytes:
        transport = self.transport
        if transport is None or not transport.is_open:
            raise OSError(errno.EIO, "serial connection unavailable during file transfer")
        waiting = transport.in_waiting
        chunk = transport.read(min(waiting, size)) if waiting else b""
        if chunk:
            self._transfer_recent.extend(chunk)
            del self._transfer_recent[:-1024]
        return chunk

    def _transfer_read_marker(
        self, marker: bytes, deadline: float, limit: int = 65536
    ) -> tuple[bytes, bytes]:
        """Read through a marker on its own output line; return body and tail."""
        data = bytearray()
        line_markers = (b"\r\n" + marker + b"\r\n", b"\n" + marker + b"\n")
        while time.monotonic() < deadline:
            chunk = self._transfer_read()
            if chunk:
                data.extend(chunk)
                if len(data) > limit:
                    raise ValueError("target response exceeded the configured transfer bound")
                for delim in line_markers:
                    pos = data.find(delim)
                    if pos >= 0:
                        tail = bytes(data[pos + len(delim):])
                        self._transfer_restore_tail(tail)
                        return bytes(data[:pos]), tail
            else:
                time.sleep(0.005)
        raise TimeoutError("timed out waiting for the target shell marker")

    def _transfer_restore_tail(self, tail: bytes) -> None:
        # The only bytes restored are after a protocol terminator (normally a
        # shell/U-Boot prompt), never transfer payload or echoed file data.
        if not tail:
            return
        self._log_rx_chunk(tail)
        self._record_io("RX", "device", tail)
        self._broadcast(tail)
        with self.buffer_lock:
            self.buffer += tail

    def _transfer_read_regex(
        self, pattern: re.Pattern[bytes], deadline: float, limit: int = 65536
    ) -> bytes:
        data = bytearray()
        while time.monotonic() < deadline:
            chunk = self._transfer_read(1024)
            if chunk:
                data.extend(chunk)
                if len(data) > limit:
                    raise ValueError("target response exceeded the configured transfer bound")
                match = pattern.search(bytes(data))
                if match:
                    self._transfer_restore_tail(bytes(data[match.start():]))
                    return bytes(data)
            else:
                time.sleep(0.005)
        raise TimeoutError("timed out waiting for the configured U-Boot prompt")

    def _transfer_write(self, payload: bytes) -> None:
        transport = self.transport
        if transport is None or not transport.is_open:
            raise OSError(errno.EIO, "serial connection unavailable during file transfer")
        transport.write(payload)
        transport.flush()

    def _transfer_drain_echo(self) -> None:
        # Keep up with line-discipline echo while a Linux `cat` is receiving
        # base64. These bytes are deliberately neither logged nor broadcast.
        while True:
            chunk = self._transfer_read(8192)
            if not chunk:
                return

    @staticmethod
    def _valid_uboot_address(address: str | None) -> bool:
        if address is None or not re.fullmatch(r"(?:0[xX])?[0-9a-fA-F]+", address):
            return False
        digits = address[2:] if address.lower().startswith("0x") else address
        return len(digits) <= 16

    @staticmethod
    def _shell_path(path: str) -> str:
        if not path or "\x00" in path or "\r" in path or "\n" in path:
            raise ValueError("target_path must be a non-empty path without control newlines")
        if path.startswith("-"):
            path = "./" + path
        return shlex.quote(path)

    # ---- Linux shell control protocol -------------------------------------
    #
    # Interactive target shells (BusyBox ash, dash, bash) are driven with short
    # control lines. Each line reports its own exit status as RC:<n> and ends
    # with a marker printed through printf "%s", so the echoed command text
    # never contains the marker itself, which keeps echo/wrap artifacts from
    # matching. Lines longer than MAX_LINUX_COMMAND_BYTES are refused before
    # anything is sent. Before any transfer step the shell must show its primary
    # prompt; a continuation prompt ("> ") means input is still pending, so only
    # Ctrl-C is sent, and EOF is sent only into a cat this server started.

    _PRIMARY_PROMPT_RE = re.compile(rb"[#$%] \Z")
    _CONTINUATION_PROMPT_RE = re.compile(rb"> \Z")
    _RC_LINE_RE = re.compile(rb"(?m)^RC:(\d+)[ \t]*$")
    STREAM_CHUNK_CHARS = 256

    def _reset_transfer_trace(self) -> None:
        self._transfer_trace = []
        self._transfer_recent = bytearray()
        self._linux_cat_foreground = False

    def _trace(self, stage: str, detail: str) -> None:
        self._transfer_trace.append(f"{stage}:{detail}")
        del self._transfer_trace[:-12]

    def _trace_summary(self) -> str:
        """Diagnostics without file content: stages plus redacted recent RX."""
        recent = bytes(self._transfer_recent[-160:])
        redacted = re.sub(rb"[A-Za-z0-9+/=]{24,}", b"<b64-redacted>", recent)
        shown = redacted.decode("utf-8", "replace").replace("\r", "\\r").replace("\n", "\\n")
        steps = " ".join(self._transfer_trace[-8:]) or "none"
        return f"[steps: {steps}; last_rx: \"{shown}\"]"

    def _check_linux_line(self, nbytes: int) -> None:
        if nbytes > self.MAX_LINUX_COMMAND_BYTES:
            raise ValueError(
                f"Linux shell control line is {nbytes} bytes, above the "
                f"{self.MAX_LINUX_COMMAND_BYTES}-byte limit that keeps interactive "
                "shells reliable; use a shorter target path"
            )

    def _control_wire(self, command: str) -> tuple[bytes, bytes]:
        """Return (wire bytes, end marker) for one short exit-status control line."""
        suffix = secrets.token_hex(6)
        line = f"{command}; printf '\\nRC:%s\\n' \"$?\"; printf '\\n__SMC_%s__\\n' {suffix}"
        wire = (line + "\r").encode("utf-8")
        self._check_linux_line(len(wire) - 1)
        return wire, f"__SMC_{suffix}__".encode("ascii")

    def _run(self, command: str, deadline: float, stage: str, limit: int = 8192) -> tuple[int, bytes]:
        """Run one control line; return (exit status, body before the end marker)."""
        self._transfer_stage = stage
        wire, marker = self._control_wire(command)
        self._transfer_write(wire)
        try:
            body, _ = self._transfer_read_marker(marker, deadline, limit)
        except TimeoutError:
            self._trace(stage, "no-end-marker")
            raise TimeoutError(f"no end marker after {stage}; the shell did not finish the line") from None
        codes = self._RC_LINE_RE.findall(body.replace(b"\r", b""))
        if not codes:
            self._trace(stage, "no-rc")
            raise RuntimeError(f"{stage} finished without an exit status line")
        rc = int(codes[-1])
        self._trace(stage, f"rc={rc}")
        return rc, body

    def _drain_echo(self, deadline: float, quiet: float) -> None:
        """Consume echoed input until the link has been quiet for `quiet` seconds."""
        last = time.monotonic()
        while time.monotonic() < deadline:
            if self._transfer_read(8192):
                last = time.monotonic()
            elif time.monotonic() - last >= quiet:
                return
            else:
                time.sleep(0.002)

    def _settle_prompt(self, payload: bytes, wait: float) -> str:
        """Send payload (if any), then classify the prompt shown after the link quiets.

        Returns "primary", "continuation", "unknown", or "silent". Output consumed
        here is restored to the RX buffer so the user still sees it.
        """
        if payload:
            self._transfer_write(payload)
        data = bytearray()
        end = time.monotonic() + wait
        quiet_since = time.monotonic()
        while time.monotonic() < end:
            chunk = self._transfer_read(4096)
            if chunk:
                data.extend(chunk)
                quiet_since = time.monotonic()
            elif data and time.monotonic() - quiet_since >= 0.15:
                break
            else:
                time.sleep(0.01)
        if data:
            self._transfer_restore_tail(bytes(data))
        text = bytes(data).replace(b"\r", b"")
        if self._PRIMARY_PROMPT_RE.search(text):
            return "primary"
        if self._CONTINUATION_PROMPT_RE.search(text):
            return "continuation"
        return "unknown" if data else "silent"

    def _require_primary_prompt(self, stage: str) -> None:
        self._transfer_stage = stage
        state = self._settle_prompt(b"\x03", 1.0)
        if state != "primary":
            raise RuntimeError(
                f"the shell is not at its primary prompt (observed {state}); only Ctrl-C was "
                "sent and no transfer data was written"
            )

    def _recover_linux_shell(self) -> str:
        """Ctrl-C first; EOF only into a cat this server started (never into a shell)."""
        state = self._settle_prompt(b"\x03", 1.0)
        if state != "primary" and self._linux_cat_foreground:
            state = self._settle_prompt(b"\r\x04", 1.5)
        if state == "primary":
            self._linux_cat_foreground = False
        return state

    def _recover_note_linux(self) -> str:
        state = self._recover_linux_shell()
        if state != "primary":
            return (
                f"The shell is in {state} state; press Ctrl-C in tio until the root prompt "
                "returns before retrying."
            )
        command = self._transfer_cleanup_command
        if command:
            try:
                self._run(command, time.monotonic() + 3.0, "linux-cleanup", 4096)
            except (OSError, TimeoutError, RuntimeError, ValueError):
                return "The shell returned to its primary prompt; clearing its transfer variables was not confirmed."
        return "The shell returned to its primary prompt."

    def _cleanup_linux_staging(self) -> str:
        command = self._transfer_cleanup_command
        paths = ", ".join(self._transfer_cleanup_paths)
        if not command:
            return "No target staging files were created."
        state = self._recover_linux_shell()
        if state != "primary":
            return (
                f"The shell is in {state} state, so cleanup was not sent; staging files may "
                f"remain at {paths}. Press Ctrl-C in tio until the root prompt returns, then "
                "remove those files."
            )
        try:
            rc, _ = self._run(command, time.monotonic() + 3.0, "linux-cleanup", 4096)
        except (OSError, TimeoutError, RuntimeError, ValueError) as exc:
            return f"Staging cleanup was not confirmed ({exc}); staging files may remain at {paths}."
        if rc == 0:
            return "Staging files were removed."
        return f"Staging cleanup returned rc={rc}; staging files may remain at {paths}."

    def _linux_capability_check(self, deadline: float) -> str:
        """Pick the hash tool only after a known-output probe (digest and exit status)."""
        rc, _ = self._run(
            "command -v cat >/dev/null 2>&1 && command -v base64 >/dev/null 2>&1 "
            "&& printf eA== | base64 -d >/dev/null 2>&1",
            deadline, "linux-probe-base",
        )
        if rc != 0:
            return "Error: Linux target requires a POSIX-like shell with cat and base64 -d; the base probe failed."
        expected = hashlib.sha256(b"x").hexdigest()
        for tool, command in (
            ("sha256sum", "printf x | sha256sum"),
            ("openssl", "printf x | openssl dgst -sha256"),
        ):
            rc, body = self._run(command, deadline, f"linux-probe-{tool}")
            digests = [d.decode("ascii").lower() for d in re.findall(rb"\b([0-9a-fA-F]{64})\b", body)]
            if rc == 0 and digests and digests[-1] == expected:
                return tool
        return "Error: Linux target requires a working sha256sum or openssl dgst -sha256; known-output probes failed."

    def _upload_linux(self, payload: bytes, target_path: str, overwrite: bool, deadline: float) -> str:
        self._transfer_stage = "linux-preflight"
        directory, basename = posixpath.split(target_path)
        if not basename or basename in (".", ".."):
            raise ValueError("target_path must name a file, not a directory")
        directory = directory or "."
        staged_temp = f".serial-mcp-{secrets.token_hex(4)}"
        staged_b64 = staged_temp + ".b"
        size = len(payload)
        digest = hashlib.sha256(payload).hexdigest()
        ready = secrets.token_hex(6)
        # The directory and names are set once in SMC_* shell variables, so every
        # later control line stays short no matter how long the target path is.
        assign = (
            f"SMC_D={self._shell_path(directory)}; SMC_T={self._shell_path(staged_temp)}; "
            f"SMC_B=\"$SMC_T\".b; SMC_F={self._shell_path(basename)}"
        )
        setup = (
            "(cd \"$SMC_D\" && umask 077 && : > \"$SMC_B\") && "
            f"printf '\\n__SMC_%s__\\n' {ready} && cat >> \"$SMC_D/$SMC_B\""
        )
        decode = "(cd \"$SMC_D\" && umask 077 && base64 -d \"$SMC_B\" > \"$SMC_T\")"
        size_check = f"(cd \"$SMC_D\" && set -- $(wc -c < \"$SMC_T\") && [ \"$1\" -eq {size} ])"
        hash_checks = {
            "sha256sum": f"(cd \"$SMC_D\" && h=$(sha256sum \"$SMC_T\") && [ \"${{h%% *}}\" = {digest} ])",
            "openssl": f"(cd \"$SMC_D\" && h=$(openssl dgst -sha256 < \"$SMC_T\") && [ \"${{h##* }}\" = {digest} ])",
        }
        install = (
            "(cd \"$SMC_D\" && mv -f \"$SMC_T\" \"$SMC_F\")" if overwrite
            else "(cd \"$SMC_D\" && ln \"$SMC_T\" \"$SMC_F\")"
        )
        # Cleanup removes only the two staging names, and clears the variables even
        # when the directory no longer exists; it never touches the destination.
        cleanup = (
            "(cd \"${SMC_D:-.}\" && rm -f \"$SMC_B\" \"$SMC_T\"); R=$?; "
            "unset SMC_D SMC_T SMC_B SMC_F; (exit $R)"
        )
        # Everything is validated before the first byte is sent to the target.
        self._check_linux_line(len(assign.encode("utf-8")))
        self._check_linux_line(len(setup.encode("utf-8")))
        for command in (decode, size_check, install, cleanup, *hash_checks.values()):
            self._control_wire(command)
        self._transfer_cleanup_paths = [
            posixpath.join(directory, staged_temp),
            posixpath.join(directory, staged_b64),
        ]
        self._transfer_cleanup_command = cleanup

        self._require_primary_prompt("linux-prompt-sync")
        hash_tool = self._linux_capability_check(deadline)
        if hash_tool.startswith("Error:"):
            return hash_tool

        rc, _ = self._run(assign, deadline, "linux-prepare")
        if rc != 0:
            raise RuntimeError(f"could not set staging variables (rc={rc}); nothing was written")
        self._transfer_stage = "linux-setup"
        self._transfer_write((setup + "\r").encode("utf-8"))
        self._transfer_read_marker(f"__SMC_{ready}__".encode("ascii"), deadline, 4096)
        # The ready marker is printed before cat starts; from here on cat is the
        # foreground job and owns the line, so Ctrl-C and EOF target it alone.
        self._linux_cat_foreground = True
        self._trace("linux-setup", "ready")
        self._transfer_stage = "linux-stream"
        encoded = base64.b64encode(payload)
        for offset in range(0, len(encoded), self.STREAM_CHUNK_CHARS):
            if time.monotonic() >= deadline:
                raise TimeoutError("linux-stream exceeded the transfer deadline")
            self._transfer_write(encoded[offset:offset + self.STREAM_CHUNK_CHARS] + b"\r")
            self._drain_echo(deadline, quiet=0.03)
        self._trace("linux-stream", f"{len(encoded)}-chars")
        self._transfer_stage = "linux-eof"
        state = self._settle_prompt(b"\x04", 2.0)
        self._linux_cat_foreground = False
        if state != "primary":
            raise RuntimeError(f"cat did not return to the primary prompt after EOF (observed {state})")

        rc, _ = self._run(decode, deadline, "linux-decode")
        if rc != 0:
            raise RuntimeError(f"base64 decode of the staged file failed (rc={rc})")
        rc, _ = self._run(size_check, deadline, "linux-size-check")
        if rc != 0:
            raise RuntimeError("the decoded staged file length did not match; nothing was installed")
        rc, _ = self._run(hash_checks[hash_tool], deadline, "linux-sha256-check")
        if rc != 0:
            raise RuntimeError(f"the staged file SHA-256 did not match via {hash_tool}; nothing was installed")

        self._transfer_target_install_attempted = True
        rc, _ = self._run(install, deadline, "linux-install")
        if rc != 0:
            note = self._cleanup_linux_staging()
            if overwrite:
                return f"Error: installing the verified file failed (rc={rc}); the destination state is UNKNOWN. {note}"
            return (
                f"Error: destination exists or hard links are unsupported (rc={rc}); the "
                f"destination was not overwritten. {note}"
            )

        self._transfer_stage = "linux-cleanup"
        try:
            rc, _ = self._run(cleanup, deadline, "linux-cleanup", 4096)
            note = "Staging files were removed." if rc == 0 else f"Staging cleanup returned rc={rc}."
        except (OSError, TimeoutError, RuntimeError, ValueError) as exc:
            note = f"Staging cleanup was not confirmed ({exc})."
        return (
            f"Uploaded {size} bytes to Linux path {target_path}; target SHA-256 matched the "
            f"local file via {hash_tool}. {note}"
        )

    def _download_linux(self, target_path: str, max_bytes: int, deadline: float) -> tuple[bytes, str]:
        self._transfer_stage = "linux-preflight"
        qpath = self._shell_path(target_path)
        assign = f"SMC_F={qpath}"
        self._check_linux_line(len(assign.encode("utf-8")))
        self._transfer_cleanup_command = "unset SMC_F; (exit 0)"
        self._require_primary_prompt("linux-prompt-sync")
        hash_tool = self._linux_capability_check(deadline)
        if hash_tool.startswith("Error:"):
            raise RuntimeError(hash_tool)
        rc, _ = self._run(assign, deadline, "linux-prepare")
        if rc != 0:
            raise RuntimeError(f"could not set the source variable (rc={rc})")

        rc, body = self._run(
            "test -f \"$SMC_F\" && set -- $(wc -c < \"$SMC_F\") && printf '\\nSIZE:%s\\n' \"$1\"",
            deadline, "linux-download-size",
        )
        match = re.search(rb"(?m)^SIZE:(\d+)\s*$", body.replace(b"\r", b""))
        if rc != 0 or not match:
            raise ValueError("the target file is missing or is not a regular file")
        size = int(match.group(1))
        if size > max_bytes:
            raise ValueError(f"the target file is {size} bytes; max_bytes is {max_bytes}")

        if hash_tool == "sha256sum":
            hash_cmd = "h=$(sha256sum \"$SMC_F\") && printf '\\nDIGEST:%s\\n' \"${h%% *}\""
        else:
            hash_cmd = "h=$(openssl dgst -sha256 < \"$SMC_F\") && printf '\\nDIGEST:%s\\n' \"${h##* }\""
        rc, body = self._run(hash_cmd, deadline, "linux-download-hash")
        match = re.search(rb"(?m)^DIGEST:([0-9a-fA-F]{64})\s*$", body.replace(b"\r", b""))
        if rc != 0 or not match:
            raise ValueError(f"the target {hash_tool} output was missing or malformed")
        remote_digest = match.group(1).decode("ascii").lower()

        begin = secrets.token_hex(6)
        rc, body = self._run(
            f"printf '\\n__SMC_%s__\\n' {begin}; base64 < \"$SMC_F\"",
            deadline, "linux-download-data", max_bytes * 2 + 65536,
        )
        start_marker = f"__SMC_{begin}__".encode("ascii")
        if rc != 0 or start_marker not in body:
            raise ValueError(f"base64 read of the target file failed (rc={rc})")
        text = body[body.index(start_marker) + len(start_marker):].replace(b"\r", b"")
        rc_match = self._RC_LINE_RE.search(text)
        if not rc_match:
            raise ValueError("base64 output had no exit status line; the output is truncated")
        encoded = b"".join(text[:rc_match.start()].split())
        try:
            data = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("the target base64 output was malformed or truncated") from exc
        if len(data) != size:
            raise ValueError(f"decoded byte count mismatch: expected {size}, got {len(data)}")
        if hashlib.sha256(data).hexdigest() != remote_digest:
            raise ValueError(f"local SHA-256 did not match the target {hash_tool} output")
        self._run("unset SMC_F; (exit 0)", deadline, "linux-download-clear", 4096)
        return data, f"Target SHA-256 matched the downloaded bytes (hash tool: {hash_tool})."

    def upload_file(
        self, local_path: str, target_path: str, target: str = "linux",
        address: str | None = None, overwrite: bool = False,
        timeout: float = 300.0, max_bytes: int = MAX_FILE_TRANSFER_BYTES,
        prompt_regex: str | None = None, confirm_shared_quiescence: bool = False,
    ) -> str:
        """Upload to Linux storage or U-Boot RAM; attach transfers need explicit co-user quiescence confirmation."""
        if target not in ("linux", "uboot"):
            return "Error: target must be 'linux' or 'uboot'."
        if not 1 <= max_bytes <= self.MAX_FILE_TRANSFER_BYTES:
            return f"Error: max_bytes must be between 1 and {self.MAX_FILE_TRANSFER_BYTES}."
        try:
            source = Path(local_path)
            size = source.stat().st_size
            if size > max_bytes:
                return f"Error: local file is {size} bytes; max_bytes is {max_bytes}."
            payload = source.read_bytes()
            if len(payload) != size:
                return "Error: local file changed while being read; retry with a stable file."
        except (OSError, ValueError) as exc:
            return f"Error reading local file: {exc}"
        self._reset_transfer_trace()
        self._transfer_cleanup_paths = []
        self._transfer_cleanup_command = None
        self._transfer_target_install_attempted = False
        deadline, error = self._transfer_begin(timeout, confirm_shared_quiescence)
        if error:
            return error
        try:
            if target == "linux":
                return self._upload_linux(payload, target_path, overwrite, deadline)
            if not self._valid_uboot_address(address):
                return "Error: U-Boot upload requires an explicit hexadecimal address of at most 64 bits; no default is used."
            address_value = int(address, 16)
            if address_value + max(0, len(payload) - 1) > 0xffffffffffffffff:
                return "Error: U-Boot upload address range overflows 64-bit address space."
            if not target_path or not target_path.isascii() or "/" in target_path or "\\" in target_path or any(not 0x20 <= ord(c) <= 0x7e for c in target_path):
                return "Error: target_path must be a simple printable-ASCII YMODEM filename (no path separators/control chars)."
            if len(target_path.encode("ascii")) + len(str(len(payload))) + 2 > 128:
                return "Error: YMODEM filename and size do not fit the 128-byte header block."
            prompt = self._compile_optional_regex(prompt_regex)
            if prompt is None:
                prompt = self.prompt_pattern
            if prompt is None:
                return "Error: U-Boot upload requires prompt_regex or a configured prompt pattern."
            return self._upload_uboot(payload, target_path, address, prompt, deadline)
        except (OSError, TimeoutError, ValueError, RuntimeError, re.error) as exc:
            failed_stage = self._transfer_stage
            if target == "linux":
                cleanup_note = self._cleanup_linux_staging()
                if self._transfer_target_install_attempted:
                    impact = "The install command was started, so the destination state is UNKNOWN; inspect it before retrying."
                else:
                    impact = "The install was not attempted, so this transfer did not change the destination."
            else:
                cleanup_note = ""
                impact = (
                    "Partial volatile RAM at the selected U-Boot address may have changed; "
                    "do not execute or use it unless a later readback verifies it."
                )
            return (
                f"Error: file upload failed during {failed_stage}: {exc}. {impact} "
                f"{cleanup_note} {self._trace_summary()}"
            ).strip()
        finally:
            self._transfer_cleanup_paths = []
            self._transfer_cleanup_command = None
            self._transfer_target_install_attempted = False
            self._linux_cat_foreground = False
            self._transfer_end()

    def download_file(
        self, local_path: str, target: str = "linux", target_path: str | None = None,
        address: str | None = None, length: int | None = None,
        overwrite: bool = False, timeout: float = 300.0,
        max_bytes: int = MAX_FILE_TRANSFER_BYTES, prompt_regex: str | None = None,
        confirm_shared_quiescence: bool = False,
    ) -> str:
        """Download to a local path; attach transfers need explicit co-user quiescence confirmation."""
        if target not in ("linux", "uboot"):
            return "Error: target must be 'linux' or 'uboot'."
        if not 1 <= max_bytes <= self.MAX_FILE_TRANSFER_BYTES:
            return f"Error: max_bytes must be between 1 and {self.MAX_FILE_TRANSFER_BYTES}."
        try:
            local = Path(local_path)
            if local.exists() and not overwrite:
                return f"Error: local destination exists: {local_path}; set overwrite=true to replace it."
            if not local.parent.exists() or not local.parent.is_dir():
                return "Error: local destination directory does not exist."
        except (OSError, ValueError) as exc:
            return f"Error: invalid local destination: {exc}"
        self._reset_transfer_trace()
        deadline, error = self._transfer_begin(timeout, confirm_shared_quiescence)
        if error:
            return error
        try:
            if target == "linux":
                if target_path is None:
                    return "Error: Linux download requires target_path."
                data, verification = self._download_linux(target_path, max_bytes, deadline)
            else:
                if not self._valid_uboot_address(address):
                    return "Error: U-Boot download requires an explicit hexadecimal address of at most 64 bits; no default is used."
                if length is None or not 0 <= length <= max_bytes:
                    return f"Error: U-Boot download requires length from 0 through max_bytes ({max_bytes})."
                address_value = int(address, 16)
                if length and address_value + length - 1 > 0xffffffffffffffff:
                    return "Error: U-Boot download address range overflows 64-bit address space."
                prompt = self._compile_optional_regex(prompt_regex) or self.prompt_pattern
                if prompt is None:
                    return "Error: U-Boot download requires prompt_regex or a configured prompt pattern."
                data, verification = self._download_uboot(address, length, prompt, deadline)
            if local.exists() and not overwrite:
                return f"Error: local destination appeared during transfer: {local_path}. Nothing was replaced."
            fd, temp_name = tempfile.mkstemp(prefix=".serial-mcp-", dir=str(local.parent))
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                digest = hashlib.sha256(data).hexdigest()
                if overwrite:
                    os.replace(temp_name, local)
                else:
                    os.link(temp_name, local)
                    os.unlink(temp_name)
            except Exception:
                try:
                    os.unlink(temp_name)
                except OSError:
                    pass
                raise
            return f"Downloaded {len(data)} bytes to {local_path}; SHA-256 {digest}. {verification}"
        except (OSError, TimeoutError, ValueError, RuntimeError, re.error) as exc:
            failed_stage = self._transfer_stage
            note = self._recover_note_linux() if target == "linux" else ""
            return (
                f"Error: file download failed during {failed_stage}: {exc}; local destination was "
                f"not replaced. {note} {self._trace_summary()}"
            ).strip()
        finally:
            self._linux_cat_foreground = False
            self._transfer_end()

    @staticmethod
    def _ymodem_crc(data: bytes) -> int:
        crc = 0
        for byte in data:
            crc ^= byte << 8
            for _ in range(8):
                crc = ((crc << 1) ^ 0x1021) & 0xffff if crc & 0x8000 else (crc << 1) & 0xffff
        return crc

    def _ymodem_byte(self, deadline: float) -> int:
        while time.monotonic() < deadline:
            data = self._transfer_read(1)
            if data:
                return data[0]
            time.sleep(0.005)
        raise TimeoutError("timed out during U-Boot YMODEM negotiation")

    def _ymodem_wait(self, expected: set[int], deadline: float, retries: int = 10) -> int:
        for _ in range(retries):
            try:
                value = self._ymodem_byte(min(deadline, time.monotonic() + 5))
            except TimeoutError:
                continue
            if value in expected:
                return value
            if value == 0x15:
                return value
        raise TimeoutError("U-Boot YMODEM peer did not acknowledge within retry bounds")

    def _ymodem_packet(self, start: int, sequence: int, content: bytes) -> bytes:
        header = bytes((start, sequence & 0xff, 0xff - (sequence & 0xff)))
        crc = self._ymodem_crc(content)
        return header + content + bytes((crc >> 8, crc & 0xff))

    def _ymodem_send_packet(self, packet: bytes, sequence: int, deadline: float) -> None:
        for _ in range(10):
            self._transfer_write(packet)
            try:
                response = self._ymodem_wait({0x06}, deadline)
            except TimeoutError:
                if time.monotonic() >= deadline:
                    raise
                continue
            if response == 0x06:
                return
        raise RuntimeError(f"YMODEM block {sequence} was not acknowledged after 10 attempts")

    def _upload_uboot(self, payload: bytes, filename: str, address: str, prompt: re.Pattern[bytes], deadline: float) -> str:
        addr = address if address.lower().startswith("0x") else "0x" + address
        self._transfer_stage = "uboot-loady-negotiation"
        prompt_returned = False
        loady_sent = False
        try:
            self._transfer_write((f"loady {addr}\r").encode("ascii"))
            loady_sent = True
            # The initial C is a CRC-mode request. Wait for it to be the last
            # byte after quiet, so status text cannot trigger binary transmission.
            handshake = bytearray()
            while time.monotonic() < deadline:
                chunk = self._transfer_read(4096)
                if chunk:
                    handshake.extend(chunk)
                    if prompt.search(bytes(handshake)):
                        prompt_returned = True
                        raise RuntimeError(
                            "U-Boot returned to its prompt without a YMODEM request; "
                            "confirm loady exists and was built with YMODEM support."
                        )
                    if len(handshake) > 4096:
                        del handshake[:-256]
                elif handshake.endswith(b"C"):
                    break
                else:
                    time.sleep(0.005)
            else:
                raise TimeoutError("U-Boot did not request YMODEM CRC mode after loady")

            self._transfer_stage = "uboot-ymodem-header"
            name = filename.encode("ascii", "strict")
            header_data = (name + b"\x00" + str(len(payload)).encode("ascii") + b"\x00").ljust(128, b"\x00")
            self._ymodem_send_packet(self._ymodem_packet(0x01, 0, header_data), 0, deadline)
            if self._ymodem_wait({0x43}, deadline) != 0x43:
                raise RuntimeError("U-Boot rejected the YMODEM header; confirm CRC-mode loady support.")
            self._transfer_stage = "uboot-ymodem-data"
            seq = 1
            for offset in range(0, len(payload), 1024):
                block = payload[offset:offset + 1024].ljust(1024, b"\x1a")
                self._ymodem_send_packet(self._ymodem_packet(0x02, seq, block), seq, deadline)
                seq = (seq + 1) & 0xff
            self._transfer_stage = "uboot-ymodem-finalize"
            self._transfer_write(b"\x04")
            first = self._ymodem_wait({0x06, 0x15}, deadline)
            if first == 0x15:
                self._transfer_write(b"\x04")
                self._ymodem_wait({0x06}, deadline)
            self._ymodem_send_packet(self._ymodem_packet(0x01, 0, bytes(128)), 0, deadline)
            self._transfer_read_regex(prompt, deadline, 65536)
            prompt_returned = True
            self._transfer_stage = "uboot-ram-readback"
            # Verify bytes in RAM, not only YMODEM's packet CRC/ACKs.
            readback, _ = self._download_uboot(addr, len(payload), prompt, deadline)
            if readback != payload:
                raise ValueError("U-Boot RAM readback differs from the uploaded file")
            return f"Uploaded {len(payload)} bytes to U-Boot RAM at {addr}; YMODEM CRC and exact RAM readback verified. No flash write or execution was performed."
        except Exception as exc:
            if loady_sent and not prompt_returned:
                try:
                    # YMODEM CAN CAN aborts a receiver; wait briefly for the
                    # configured prompt so the serial console is left usable.
                    self._transfer_write(b"\x18\x18")
                    self._transfer_read_regex(prompt, time.monotonic() + 2.0, 65536)
                except Exception as abort_exc:
                    raise RuntimeError(
                        f"{exc}; YMODEM cancellation/prompt recovery failed: {abort_exc}. "
                        "The console may still be in loady mode."
                    ) from exc
            raise

    def _download_uboot(self, address: str, length: int, prompt: re.Pattern[bytes], deadline: float) -> tuple[bytes, str]:
        if length == 0:
            return b"", "Verification: requested zero-length range; no target bytes were read."
        addr = address if address.lower().startswith("0x") else "0x" + address
        self._transfer_stage = "uboot-md.b-read"
        self._transfer_write((f"md.b {addr} {length:x}\r").encode("ascii"))
        output = self._transfer_read_regex(prompt, deadline, max(65536, length * 6 + 8192))
        rows: list[tuple[int, bytes]] = []
        for line in output.replace(b"\r", b"").splitlines():
            match = re.match(rb"^\s*([0-9a-fA-F]{8,16}):\s*(.*)$", line)
            if not match:
                continue
            row_addr = int(match.group(1), 16)
            hex_column = re.split(rb"\s{2,}", match.group(2), maxsplit=1)[0]
            candidates = re.findall(rb"(?<![0-9a-fA-F])([0-9a-fA-F]{2})(?![0-9a-fA-F])", hex_column)
            if candidates:
                rows.append((row_addr, bytes(int(x, 16) for x in candidates[:16])))
        if length == 0:
            return b"", "U-Boot returned the requested zero-length range."
        expected_addr = int(addr, 16)
        data = bytearray()
        for row_addr, row in rows:
            if row_addr != expected_addr + len(data):
                raise ValueError("U-Boot md.b output had a missing or unexpected address row")
            take = min(len(row), length - len(data))
            data.extend(row[:take])
            if len(data) == length:
                break
        if len(data) != length:
            raise ValueError(f"could not parse exact U-Boot md.b length: expected {length}, got {len(data)}")
        return bytes(data), "Verification: exact byte count and sequential U-Boot md.b addresses; no end-to-end checksum is available for this read."


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
                with self.session_lock:
                    generation = self._transfer_generation
                    transfer_active = self.transfer_active.is_set()
                data = os.read(master, 4096)
                if data:
                    self._session_user_write(data, generation, transfer_active)
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
                transfer_active = self.transfer_active.is_set()
            threading.Thread(target=self._session_client_loop, args=(sock,), daemon=True).start()
            if transfer_active:
                self._session_transfer_notice(
                    "File transfer active: terminal input is temporarily paused; keystrokes are discarded.",
                    clients=[sock],
                )
            self._log("SYS", "session client attached")

    def _session_client_loop(self, sock: socket.socket) -> None:
        """Forward raw client bytes to the device, tio --socket style.

        No echo is sent back to the sender; the device echo (broadcast RX)
        shows everyone's typing, exactly like tio's sharing mode.
        """
        try:
            while self.session_running.is_set():
                with self.session_lock:
                    generation = self._transfer_generation
                    transfer_active = self.transfer_active.is_set()
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                if data:
                    self._session_user_write(data, generation, transfer_active)
        except OSError:
            pass
        finally:
            self._remove_session_client(sock)

    def _session_user_write(
        self, payload: bytes, generation: int | None = None,
        transfer_active_at_read: bool | None = None,
    ) -> None:
        if generation is None or transfer_active_at_read is None:
            with self.session_lock:
                generation = self._transfer_generation
                transfer_active_at_read = self.transfer_active.is_set()
        with self.session_lock:
            if transfer_active_at_read or self.transfer_active.is_set() or generation != self._transfer_generation:
                self._session_transfer_dropped_bytes += len(payload)
                return
        deadline = time.monotonic() + 0.25
        acquired = False
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if self.io_lock.acquire(timeout=min(0.05, remaining)):
                acquired = True
                break
            with self.session_lock:
                if self.transfer_active.is_set() or generation != self._transfer_generation:
                    self._session_transfer_dropped_bytes += len(payload)
                    return
        if not acquired:
            self._session_transfer_notice("Input was not forwarded because serial I/O remained busy.")
            return
        try:
            with self.session_lock:
                if transfer_active_at_read or self.transfer_active.is_set() or generation != self._transfer_generation:
                    self._session_transfer_dropped_bytes += len(payload)
                    return
            if not self._is_connected():
                return
            with self.write_lock:
                try:
                    self._log("TX:USER", payload)
                    self._record_io("TX", "user", payload)
                    self.transport.write(payload)
                    self.transport.flush()
                except Exception:
                    return
        finally:
            self.io_lock.release()

    def human_write(self, data: bytes) -> bool:
        """Forward raw bytes typed by a human terminal to the device.

        Deliberately ungated: it takes only the device write lock, which AI
        operations hold for a single write, so it never waits behind a command
        or transfer. Returns False when no transport is open.
        """
        transport = self.transport
        if transport is None or not transport.is_open:
            return False
        with self.write_lock:
            self._log("TX:HUMAN", data)
            self._record_io("TX", "human", data)
            transport.write(data)
            transport.flush()
        return True

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
        if self.transfer_active.is_set():
            return "Error: file transfer currently owns serial I/O."
        with self.operation_lock:
            if self.transfer_active.is_set():
                return "Error: file transfer currently owns serial I/O."
            return self._login_locked(profile, username, password, login_mode, overall_timeout, encoding)

    def _login_locked(
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
        if self.transfer_active.is_set():
            return "Error: file transfer currently owns serial I/O."
        with self.operation_lock:
            if self.transfer_active.is_set():
                return "Error: file transfer currently owns serial I/O."
            return self._enter_bootloader_locked(
                profile, interrupt_keys, success_regex, abort_regex, reboot,
                reboot_command, delay, interval, max_attempts, login_username,
                login_password, login_mode, overall_timeout, encoding,
            )

    def _enter_bootloader_locked(
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


# Text returned by file_transfer_capabilities. Module-level so the shared daemon
# can serve it without going through the FastMCP tool wrapper.
FILE_TRANSFER_CAPABILITIES = (
        "File transfer requires a serial connection. Direct-owned sessions announce "
        "and gate their own socket/pty writers during transfer; queued user bytes are "
        "discarded and counted. Attach mode is supported "
        "only with confirm_shared_quiescence=true after all other tio clients agree "
        "to stay idle; raw tio sockets have no arbitration mechanism. "
        "Linux: requires a POSIX-like interactive shell, base64 -d, cat, and a "
        "working sha256sum or openssl dgst -sha256; hash output and exit status "
        "are probed. Transfers use shell-quoted paths and "
        "temporary files. Upload verifies target byte count and SHA-256; download "
        "verifies decoded byte count and target SHA-256. No-clobber Linux upload "
        "uses same-directory ln, so hard links must be supported. U-Boot upload: "
        "requires loady with YMODEM/CRC support, an explicit user-selected RAM "
        "address, and a simple ASCII filename; uploaded RAM is read back with md.b "
        "for exact comparison. U-Boot download: requires md.b, explicit address, "
        "length, and an anchored prompt_regex; verifies sequential addresses and "
        "byte count but has no end-to-end checksum. U-Boot md.b formatting must be "
        "standard address-colon hex rows. Maximum file/range size is 8 MiB; timeout "
        "is 1–1800 seconds. No default address, flash write, execution, reboot, or "
        "persistent environment change is performed."
)


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

    On Linux, direct port opens claim TIOCEXCL and check visible existing device
    holders by character-device identity (so symlink aliases are covered). A
    pre-existing owner is refused without reading, flushing, or reconfiguring the
    tty; use attach_socket when it serves a tio session. Advisory lock files are
    only a supplementary serial-mcp-instance guard. On platforms without these
    checks, direct open fails closed.

    Attach mode allows the human tio owner to keep the port. File transfers in
    this mode need explicit co-user idle confirmation because tio's raw socket
    protocol has no transfer lock. Pass exactly one of `port` or `attach_socket`.

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
def file_transfer_capabilities() -> str:
    """Describe implemented transfer methods, limits, requirements, and safety boundaries.

    Linux requires a POSIX-like shell with `cat`, `base64 -d`, and a working
    `sha256sum` or OpenSSL `dgst -sha256` command.
    U-Boot upload requires YMODEM-enabled `loady`; download uses `md.b` and a
    user-supplied prompt regex. Direct-owned sessions announce/gate their own
    socket/pty writers and count dropped input. Attach-mode transfers require explicit
    confirmation that all other tio clients will remain idle; the raw tio socket
    has no lock/arbitration protocol, so this cannot be enforced remotely.
    """
    return FILE_TRANSFER_CAPABILITIES


@mcp.tool()
def upload_file(
    local_path: str, target_path: str, target: str = "linux",
    address: str | None = None, overwrite: bool = False,
    timeout: float = 300.0, max_bytes: int = SerialState.MAX_FILE_TRANSFER_BYTES,
    prompt_regex: str | None = None, confirm_shared_quiescence: bool = False,
) -> str:
    """Upload a local binary file to Linux storage or an explicit U-Boot RAM address.

    Direct-owned sessions pause their own socket/pty writers during transfer.
    Attach mode requires confirm_shared_quiescence=true after all other tio users
    agree to stay idle; raw tio sockets cannot enforce arbitration. Linux uses
    base64 over a shell and verifies target size plus SHA-256 before installation,
    using target sha256sum when it passes a known-output probe or OpenSSL dgst as
    fallback. U-Boot uses `loady <address>` (YMODEM CRC); target_path is the
    transfer filename and address is required. U-Boot RAM is read back and
    compared. The operation never flashes or executes the image.

    Args:
        local_path: Path to a local file readable by this MCP server process.
        target_path: Linux destination path, or a simple ASCII YMODEM filename on U-Boot.
        target: `linux` or `uboot`.
        address: Required explicit hexadecimal RAM address for U-Boot; no default.
        overwrite: Whether an existing destination may be replaced (Linux only).
        timeout: Overall transfer deadline in seconds (1–1800).
        max_bytes: Maximum accepted file size, at most 8 MiB.
        prompt_regex: U-Boot prompt regex; otherwise use the configured prompt pattern.
        confirm_shared_quiescence: For attach mode only, explicitly acknowledge
            that every other human/tio client stays idle for the entire transfer.
    """
    return state.upload_file(local_path, target_path, target, address, overwrite, timeout, max_bytes, prompt_regex, confirm_shared_quiescence)


@mcp.tool()
def download_file(
    local_path: str, target: str = "linux", target_path: str | None = None,
    address: str | None = None, length: int | None = None,
    overwrite: bool = False, timeout: float = 300.0,
    max_bytes: int = SerialState.MAX_FILE_TRANSFER_BYTES,
    prompt_regex: str | None = None,
    confirm_shared_quiescence: bool = False,
) -> str:
    """Download a Linux filesystem path or explicit U-Boot RAM range to a local file.

    Direct-owned sessions pause their own socket/pty writers during transfer.
    Attach mode requires confirm_shared_quiescence=true after all other tio users
    agree to stay idle; raw tio sockets cannot enforce arbitration. Linux requires
    `base64` and a working `sha256sum` or OpenSSL `dgst -sha256`. U-Boot uses
    textual `md.b`, not an assumed serial-send command, and requires explicit address, length, and an
    anchored prompt regex. U-Boot dump verification is limited to exact byte
    count and sequential address rows (no checksum is available).

    Args:
        local_path: Local destination path; an existing file requires overwrite=true.
        target: `linux` or `uboot`.
        target_path: Required Linux filesystem path.
        address: Required explicit hexadecimal U-Boot RAM address; no default.
        length: Required U-Boot byte length; bounded by max_bytes.
        overwrite: Whether the local destination may be replaced.
        timeout: Overall transfer deadline in seconds (1–1800).
        max_bytes: Maximum accepted transfer size, at most 8 MiB.
        prompt_regex: U-Boot prompt regex; otherwise use the configured prompt pattern.
        confirm_shared_quiescence: For attach mode only, explicitly acknowledge
            that every other human/tio client stays idle for the entire transfer.
    """
    return state.download_file(local_path, target, target_path, address, length, overwrite, timeout, max_bytes, prompt_regex, confirm_shared_quiescence)


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
