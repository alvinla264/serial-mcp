#!/usr/bin/env python3
"""Generic serial transport MCP server.

The AI opens the serial device and serves a session socket that speaks tio's
raw-byte --socket protocol, so the user can attach to the AI's session from
their own terminal (real tio via a socat pty bridge, or nc -UN).
"""

from __future__ import annotations

import re
import socket
import threading
import time
import errno
import os
from collections import deque

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
        """Open the serial device and serve a tio-protocol session socket."""
        try:
            prompt_pattern = self._compile_optional_regex(prompt_regex)

            if self.ser and self.ser.is_open:
                self.disconnect()

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
            if session_socket == SESSION_SOCKET_AUTO:
                session_socket = _session_socket_for_port(port)
            if session_socket:
                session_result = self._start_session(session_socket)
                if session_result.startswith("Error"):
                    self._log("WARN", session_result)
                    result += f" (session socket unavailable: {session_result})"
                else:
                    result += " " + _attach_hint(session_result)
            return result
        except Exception as e:
            self._log("ERR", f"Connect failed: {e}")
            return f"Error connecting to {port}: {e}"

    def disconnect(self) -> str:
        self._stop_session()
        self.stop_event.set()
        if self.reader_thread:
            self.reader_thread.join(timeout=2.0)
            self.reader_thread = None

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
                "Not connected. Call connect(port=...) to open the device; a session "
                "socket starts automatically, then you can attach from your terminal."
            )
        if not self.session_running.is_set() or not self.session_path:
            return (
                f"Connected to {self.ser.port}, but the session socket is not running "
                "(reconnect to restart it)."
            )
        with self.session_lock:
            count = len(self.session_clients)
        tty_path = self.session_pty_link
        attach = f"Attach with any tty tool, e.g.:  tio {tty_path}\n" if tty_path else ""
        return (
            f"AI holds the device ({self.ser.port}); session socket: {self.session_path} "
            f"({count} client(s) attached, tio raw-byte protocol).\n"
            + attach
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
    protocol.

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
    mcp.run()


if __name__ == "__main__":
    main()
