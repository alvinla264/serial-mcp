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
import sys
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

# Startup configuration: if a default port is configured, the server connects
# and serves the session the moment the MCP process starts — no AI action
# needed. File keys: port, baudrate, timeout, prompt_regex, session_socket.
# SERIAL_MCP_* environment variables override the file.
DEFAULT_CONFIG_PATH = CONFIG_DIR / "config.json"
_CONFIG_KEYS = ("port", "baudrate", "timeout", "prompt_regex", "session_socket")
_ENV_OVERRIDES = {
    "port": "SERIAL_MCP_PORT",
    "baudrate": "SERIAL_MCP_BAUDRATE",
    "timeout": "SERIAL_MCP_TIMEOUT",
    "prompt_regex": "SERIAL_MCP_PROMPT_REGEX",
    "session_socket": "SERIAL_MCP_SESSION_SOCKET",
}


def _load_startup_config() -> dict:
    """Merge config file with SERIAL_MCP_* environment overrides (env wins)."""
    file_cfg = _load_json_file(DEFAULT_CONFIG_PATH)
    cfg = {k: v for k, v in file_cfg.items() if k in _CONFIG_KEYS and v not in (None, "")}
    for key, var in _ENV_OVERRIDES.items():
        value = os.environ.get(var)
        if value:
            cfg[key] = value
    return cfg


def _auto_connect_from_config() -> str | None:
    """Connect at server startup when a default port is configured.

    This is what makes the terminal watchable before the AI does anything:
    the session socket and companion pty exist as soon as the MCP server is
    up, so the user can attach (tio, screen, nc) immediately and keep that
    attach across every later AI connect/reconnect.
    """
    cfg = _load_startup_config()
    if not cfg.get("port"):
        return None
    result = state.connect(
        cfg["port"],
        baudrate=int(cfg.get("baudrate", 115200)),
        timeout=float(cfg.get("timeout", 0.1)),
        prompt_regex=cfg.get("prompt_regex"),
        session_socket=cfg.get("session_socket", SESSION_SOCKET_AUTO),
    )
    # stderr only: stdout is the MCP transport.
    print(f"[serial-mcp] startup connect: {result}", file=sys.stderr)
    return result


def _load_json_file(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


class SerialState:
    """Thread-safe serial connection, background RX buffer/log, and a tio-protocol
    session socket the user can attach to."""

    def __init__(self) -> None:
        self.ser: serial.Serial | None = None
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
                if self.ser and self.ser.is_open:
                    waiting = self.ser.in_waiting
                    if waiting > 0:
                        chunk = self.ser.read(waiting)
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
                    if self.ser:
                        try:
                            self.ser.close()
                        except Exception:
                            pass
                        self.ser = None
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
        port: str,
        baudrate: int = 115200,
        timeout: float = 0.1,
        prompt_regex: str | None = None,
        session_socket: str | None = SESSION_SOCKET_AUTO,
    ) -> str:
        """Open the serial device and serve a tio-protocol session socket.

        If the session socket is already running at the requested path, it is
        kept alive across the (re)connect so an attached user's tio/screen
        session survives AI connects, reconnects, and baudrate changes.
        """
        try:
            prompt_pattern = self._compile_optional_regex(prompt_regex)

            if session_socket == SESSION_SOCKET_AUTO:
                session_socket = _session_socket_for_port(port)

            keep_session = (
                bool(session_socket)
                and self.session_running.is_set()
                and self.session_path == session_socket
            )
            same_port = (
                self.ser is not None
                and self.ser.is_open
                and getattr(self.ser, "port", None) == port
                and int(getattr(self.ser, "baudrate", 0) or 0) == int(baudrate)
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
                if self.ser is not None:
                    try:
                        self.ser.close()
                    except Exception:
                        pass
                    self.ser = None

                self.ser = serial.Serial(port, baudrate, timeout=timeout)
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
            return f"Error connecting to {port}: {e}"

    def _stop_reader(self) -> None:
        self.stop_event.set()
        if self.reader_thread:
            self.reader_thread.join(timeout=2.0)
            self.reader_thread = None

    def disconnect(self) -> str:
        self._stop_session()
        self._stop_reader()

        self._flush_rx_log_buffer(force=True)

        if self.ser and self.ser.is_open:
            self.ser.close()
            self.ser = None
            self._log("SYS", "Disconnected")
            return "Disconnected."
        self.ser = None
        return "No active connection."

    def _compile_optional_regex(self, pattern: str | None) -> re.Pattern[bytes] | None:
        if not pattern:
            return None
        return re.compile(pattern.encode("utf-8"))

    def _raw_logged_write(self, data: bytes) -> None:
        with self.write_lock:
            self._log("TX", data)
            self._record_io("TX", "ai", data)
            self.ser.write(data)
            self.ser.flush()

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
    ) -> str:
        if not self.ser or not self.ser.is_open:
            return "Error: Not connected to a serial port."
        if line_ending not in LINE_ENDINGS:
            return f"Error: line_ending must be one of {', '.join(LINE_ENDINGS)}."

        start = self._snapshot_len()
        payload = text.encode(encoding) + LINE_ENDINGS[line_ending]
        terminator = self._compile_optional_regex(terminator_regex) or self.prompt_pattern

        with self.write_lock:
            self._log("TX", payload)
            self._record_io("TX", "ai", payload)
            self.ser.write(payload)
            self.ser.flush()

        data = self._wait_for_output(start, timeout, idle_timeout, terminator)
        return data.decode(encoding, errors="replace")

    def read_output(self, timeout: float = 0.0, idle_timeout: float = 0.2, encoding: str = "utf-8") -> str:
        start = 0
        data = self._wait_for_output(start, timeout, idle_timeout, None)
        return data.decode(encoding, errors="replace")

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
            if not self.ser or not self.ser.is_open:
                return
            try:
                self._log("TX:USER", payload)
                self._record_io("TX", "user", payload)
                self.ser.write(payload)
                self.ser.flush()
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
            self.ser.write(data)
            self.ser.flush()

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
        if not self.ser or not self.ser.is_open:
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
        if not self.ser or not self.ser.is_open:
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
        if self.ser is None or not self.ser.is_open:
            return (
                "Not connected. The session socket/pty appears here as soon as a "
                "device is open: set a default port in "
                f"{DEFAULT_CONFIG_PATH} (or SERIAL_MCP_PORT) so the server connects "
                "at startup, or call connect(port=...)."
            )
        if not self.session_running.is_set() or not self.session_path:
            return (
                f"Connected to {self.ser.port}, but the session socket is not running "
                "(reconnect to restart it)."
            )
        with self.session_lock:
            count = len(self.session_clients)
        return (
            f"Device {self.ser.port} is held by this MCP session; {count} client(s) attached.\n"
            + _attach_hint(self.session_path)
        )


state = SerialState()


@mcp.tool()
def connect(
    port: str,
    baudrate: int = 115200,
    timeout: float = 0.1,
    prompt_regex: str | None = None,
    session_socket: str | None = SESSION_SOCKET_AUTO,
) -> str:
    """Open a serial port, start background reading, and serve a session socket.

    The AI holds the device; the user attaches to the session from their own
    terminal (any tty tool on the companion pty, or nc -UN) using tio's raw-byte socket
    protocol. Calling this while a session is already running keeps that session
    (and anyone attached to it) alive; only the device link is (re)opened.

    Args:
        port: Serial device path, for example /dev/ttyUSB0.
        baudrate: Serial baud rate.
        timeout: Low-level pyserial read timeout used by the reader thread.
        prompt_regex: Optional default regex terminator for send_command completion.
        session_socket: UNIX socket path for the shared session. Defaults to
            "auto", which derives a unique name from the device, e.g.
            /tmp/serial-mcp-ttyUSB0.sock (pass "" to disable).
    """
    return state.connect(port, baudrate, timeout, prompt_regex, session_socket)


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
) -> str:
    """Send text bytes to the serial port and return output observed after the send.

    Completion uses terminator_regex (or the connection default prompt_regex) when supplied;
    otherwise it returns after output has been quiet for idle_timeout, bounded by timeout.
    line_ending is explicit: none, lf, cr, or crlf. Data already buffered before this call is
    preserved for read_output; this call consumes only bytes received after its send point.
    """
    return state.send_command(text, line_ending, timeout, idle_timeout, terminator_regex, encoding)


@mcp.tool()
def read_output(timeout: float = 0.0, idle_timeout: float = 0.2, encoding: str = "utf-8") -> str:
    """Consume and return buffered/asynchronously arriving serial output.

    If timeout is 0, returns currently buffered data immediately. If timeout is positive,
    waits for data and then for idle_timeout of silence, never exceeding timeout.
    """
    return state.read_output(timeout, idle_timeout, encoding)


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
    """Entry point for the serial-mcp console script (uvx/pip installs)."""
    # Bring the session up before serving MCP, if a default port is configured:
    # the terminal is then watchable (tio, screen, nc) whether or not the AI
    # ever calls connect, and the user's attach survives later AI connects.
    _auto_connect_from_config()
    mcp.run()


if __name__ == "__main__":
    main()
