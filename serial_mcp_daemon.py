"""Per-user shared serial daemon (protocol v1, docs/daemon-ipc.md).

The daemon owns the single device connection (a ``SerialState``), its background
reader, buffers, and the device operations. Clients connect over a permission-
restricted UNIX socket, identify themselves with ``hello``, and then:

* run tools through the same implementation the MCP tools use (``call``), gated
  by one control lease so two clients cannot drive the device at once;
* observe device output without consuming another client's unread output
  (``subscribe``);
* forward raw human keystrokes without lease gating (``input``, role ``human``).

Lifetime: the daemon stays up while any client connection is open. When none
remain, a 30-second grace period starts and any new client cancels it. At
expiry it drains in-flight work and then releases the device.

The clock is injected so lease expiry and grace are tested deterministically;
production uses ``time.monotonic``.
"""

from __future__ import annotations

import base64
import fcntl
import inspect
import itertools
import os
import select
import signal
import socket
import stat
import struct
import sys
import threading
import time
from collections import deque
from pathlib import Path

import serial_mcp_ipc as ipc
from serial_mcp_engine import FILE_TRANSFER_CAPABILITIES, SerialState
from serial_mcp_ipc import (
    DRAIN_TIMEOUT_SECONDS,
    GRACE_SECONDS,
    HELLO_TIMEOUT_SECONDS,
    LEASE_IDLE_SECONDS,
    MAX_HUMAN_INPUT_BYTES,
    LOCK_PROBE_WAIT_SECONDS,
    MAX_INFLIGHT_CALLS_PER_CONNECTION,
    MAX_OUTPUT_EVENT_BYTES,
    MAX_SUBSCRIBER_QUEUE_BYTES,
    PROTOCOL_VERSION,
    SLOW_CONSUMER_TIMEOUT_SECONDS,
    ProtocolError,
    UnsafeRuntimeDir,
    encode_frame,
)

DAEMON_VERSION = "0.2.0"
MAX_CLIENT_NAME = 64
ROLES = ("mcp", "human")

# Tool name -> (SerialState method, gated). "file_transfer_capabilities" is served
# from its module constant and is handled separately.
TOOL_TABLE: dict[str, tuple[str | None, bool]] = {
    "connect": ("connect", True),
    "attach": ("attach", True),
    "disconnect": ("disconnect", True),
    "send_command": ("send_command", True),
    "read_output": ("read_output", True),
    "clear_output": ("clear_output", True),
    "set_prompt_pattern": ("set_prompt_pattern", True),
    "login": ("login", True),
    "enter_bootloader": ("enter_bootloader", True),
    "upload_file": ("upload_file", True),
    "download_file": ("download_file", True),
    "view_io": ("view_io", False),
    "tio_info": ("tio_info", False),
    "list_serial_ports": ("list_serial_ports", False),
    "list_boot_profiles": ("list_boot_profiles", False),
    "set_boot_credentials": ("set_boot_credentials", False),
    "file_transfer_capabilities": (None, False),
}
DEVICE_CHANGING_TOOLS = {"connect", "attach", "disconnect"}


class DaemonAlreadyRunning(Exception):
    """Another daemon holds the lock for this runtime directory."""


def _device_label(state) -> str | None:
    if not state._is_connected():
        return None
    return state._connection_label()


# ---- per-connection state --------------------------------------------------


class Connection:
    """One client socket: a reader thread (requests), a writer thread (frames)."""

    def __init__(self, daemon: "DaemonServer", sock: socket.socket, cid: str) -> None:
        self.daemon = daemon
        self.sock = sock
        self.cid = cid
        self.role: str | None = None
        self.name: str = ""
        self.pid: int | None = None
        self.hello_done = False
        self.subscribed = False
        self.closed = False
        self.needs_reacquire = False
        self.lost_reason: str | None = None
        self.calls_in_flight = 0
        self._cv = threading.Condition()
        self._control: deque[bytes] = deque()
        self._chunks: deque[tuple[int, bytes]] = deque()
        self._queued_bytes = 0
        self._dropped = 0
        self._stop_writer = False
        self._flushing = False
        self._writer = threading.Thread(target=self._write_loop, name=f"{cid}-writer", daemon=True)
        self._reader = threading.Thread(target=self._read_loop, name=f"{cid}-reader", daemon=True)

    def start(self) -> None:
        self.sock.settimeout(None)
        self._writer.start()
        self._reader.start()

    # -- outbound ---------------------------------------------------------

    def send_control(self, frame: dict) -> None:
        data = encode_frame(frame)
        with self._cv:
            if self._stop_writer:
                return
            self._control.append(data)
            self._cv.notify()

    def send_result(self, rid, result: dict) -> None:
        self.send_control({"id": rid, "ok": True, "result": result})

    def send_error(self, rid, code: str, message: str, **extra) -> None:
        error = {"code": code, "message": message}
        error.update(extra)
        self.send_control({"id": rid, "ok": False, "error": error})

    def offer_output(self, seq: int, data: bytes) -> None:
        """Queue device output for this subscriber without ever blocking.

        Overflow drops the oldest queued bytes and counts them; the writer then
        reports an ``output_gap`` before the next chunk.
        """
        with self._cv:
            if self._stop_writer or not self.subscribed:
                return
            for start in range(0, len(data), MAX_OUTPUT_EVENT_BYTES):
                piece = data[start:start + MAX_OUTPUT_EVENT_BYTES]
                self._chunks.append((seq + start, piece))
                self._queued_bytes += len(piece)
            while self._queued_bytes > MAX_SUBSCRIBER_QUEUE_BYTES and self._chunks:
                _seq, dropped = self._chunks.popleft()
                self._queued_bytes -= len(dropped)
                self._dropped += len(dropped)
            self._cv.notify()

    def _write_loop(self) -> None:
        try:
            while True:
                payloads = self._next_payloads()
                if payloads is None:
                    return
                for data in payloads:
                    self._send_all(data)
        except (OSError, ValueError):
            # Slow or vanished peer (ValueError: socket already closed): drop only this connection.
            pass
        finally:
            self._finish_writer()
            self.close()

    def _next_payloads(self) -> list[bytes] | None:
        """Block until something is queued; return the bytes to send next."""
        with self._cv:
            while not self._control and not self._chunks and not self._stop_writer:
                self._cv.wait()
            if self._control:
                frames = list(self._control)
                self._control.clear()
                return frames
            if self._dropped and self._chunks:
                gap = {"v": PROTOCOL_VERSION, "event": "output_gap",
                       "dropped_bytes": self._dropped, "seq": self._chunks[0][0]}
                self._dropped = 0
                return [encode_frame(gap)]
            if self._chunks:
                seq, data = self._chunks.popleft()
                self._queued_bytes -= len(data)
                return [encode_frame({"event": "output", "seq": seq,
                                      "data": base64.b64encode(data).decode("ascii")})]
            return None  # stop requested with nothing left to send

    def _send_all(self, data: bytes) -> None:
        """Send without ever blocking indefinitely on a stalled peer."""
        view = memoryview(data)
        while view:
            _, writable, _ = select.select([], [self.sock], [], SLOW_CONSUMER_TIMEOUT_SECONDS)
            if not writable:
                raise OSError("slow consumer: peer stopped reading")
            sent = self.sock.send(view)
            view = view[sent:]

    def _finish_writer(self) -> None:
        with self._cv:
            self._stop_writer = True
            self._control.clear()
            self._chunks.clear()
            self._cv.notify_all()

    # -- inbound ----------------------------------------------------------

    def _read_loop(self) -> None:
        reader = ipc.FrameReader(self.sock)
        deadline = threading.Timer(HELLO_TIMEOUT_SECONDS, self._hello_deadline)
        deadline.daemon = True
        deadline.start()
        try:
            while not self.closed and not self._flushing:
                try:
                    frame = reader.read_frame()
                except ProtocolError as exc:
                    self.send_error(None, exc.code, exc.message)
                    if exc.code in _FATAL_FRAME_CODES:
                        # Oversized or wrong-version framing cannot be resynchronised.
                        self._close_after_flush()
                        return
                    continue  # a malformed but delimited frame: keep the connection
                except OSError:
                    return
                if frame is None:
                    return
                try:
                    self.handle(frame)
                except Exception as exc:  # never let one request kill the connection
                    self.daemon.log("ERR", f"request handling failed: {exc!r}")
                    self.send_error(frame.get("id"), "internal", "unexpected daemon error")
        finally:
            if not self._flushing:
                self.close()

    def _hello_deadline(self) -> None:
        if not self.hello_done and not self.closed:
            self.send_error(None, "hello_required", "no hello within the handshake timeout")
            self._close_after_flush()

    def _close_after_flush(self) -> None:
        """Send the control frames already queued, then close (writer closes the socket).

        Output chunks are discarded: a connection that is ending gets no more output.
        """
        with self._cv:
            self._flushing = True
            self._stop_writer = True
            self._chunks.clear()
            self._queued_bytes = 0
            self._cv.notify_all()

    def handle(self, frame: dict) -> None:
        rid = frame.get("id")
        if rid is not None and (isinstance(rid, bool) or not isinstance(rid, (int, str))):
            self.send_error(None, "bad_request", "id must be an integer or string")
            return
        op = frame.get("op")
        if not isinstance(op, str):
            self.send_error(rid, "bad_request", "op must be a string")
            return
        if op == "hello":
            return self._op_hello(rid, frame)
        if not self.hello_done:
            self.send_error(rid, "hello_required", "the first request must be hello")
            return
        if op == "status":
            return self.send_result(rid, self.daemon.status(self))
        if op == "call":
            return self._op_call(rid, frame)
        if op == "lease":
            return self._op_lease(rid, frame)
        if op in ("subscribe", "unsubscribe"):
            self.subscribed = op == "subscribe"
            result = {"subscribed": self.subscribed}
            if self.subscribed:
                result["seq"] = self.daemon.rx_offset()
            return self.send_result(rid, result)
        if op == "input":
            return self._op_input(rid, frame)
        if op == "bye":
            self.send_result(rid, {})
            self._close_after_flush()
            return None
        self.send_error(rid, "unknown_op", f"unknown op {op!r}")

    def _op_hello(self, rid, frame: dict) -> None:
        if self.hello_done:
            return self.send_error(rid, "already_hello", "hello was already accepted on this connection")
        role = frame.get("role")
        name = frame.get("name")
        pid = frame.get("pid")
        if role not in ROLES:
            return self.send_error(rid, "bad_request", "role must be 'mcp' or 'human'")
        if not isinstance(name, str) or len(name) > MAX_CLIENT_NAME:
            return self.send_error(rid, "bad_request", f"name must be a string of at most {MAX_CLIENT_NAME} chars")
        if pid is not None and (not isinstance(pid, int) or isinstance(pid, bool)):
            return self.send_error(rid, "bad_request", "pid must be an integer")
        self.role, self.name, self.pid = role, name, pid
        result = self.daemon.register(self)
        if result is None:
            return self.send_error(rid, "shutting_down", "the daemon is shutting down")
        self.hello_done = True
        return self.send_result(rid, result)

    def _op_call(self, rid, frame: dict) -> None:
        tool = frame.get("tool")
        args = frame.get("args", {})
        if not isinstance(tool, str):
            return self.send_error(rid, "bad_request", "tool must be a string")
        if not isinstance(args, dict) or not all(isinstance(k, str) for k in args):
            return self.send_error(rid, "bad_request", "args must be an object with string keys")
        if tool not in TOOL_TABLE:
            return self.send_error(rid, "unknown_tool", f"unknown tool {tool!r}")
        if self.calls_in_flight >= MAX_INFLIGHT_CALLS_PER_CONNECTION:
            return self.send_error(rid, "overloaded", "too many concurrent calls on this connection")
        with self._cv:
            self.calls_in_flight += 1
        threading.Thread(
            target=self._run_call, args=(rid, tool, args), name=f"{self.cid}-call", daemon=True
        ).start()

    def _run_call(self, rid, tool: str, args: dict) -> None:
        try:
            self.daemon.run_call(self, rid, tool, args)
        except Exception as exc:
            self.daemon.log("ERR", f"tool {tool} failed: {exc!r}")
            self.send_error(rid, "internal", "unexpected daemon error")
        finally:
            with self._cv:
                self.calls_in_flight -= 1

    def _op_lease(self, rid, frame: dict) -> None:
        action = frame.get("action")
        if action not in ("acquire", "release", "status"):
            return self.send_error(rid, "bad_request", "action must be acquire, release or status")
        result, error = self.daemon.lease_action(self, action)
        if error is not None:
            return self.send_error(rid, error["code"], error["message"], lease=error.get("lease"))
        return self.send_result(rid, result)

    def _op_input(self, rid, frame: dict) -> None:
        if self.role != "human":
            return self.send_error(rid, "forbidden_role", "input is only accepted from human clients")
        encoded = frame.get("data")
        if not isinstance(encoded, str):
            return self.send_error(rid, "bad_request", "data must be base64 text")
        try:
            data = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError):
            return self.send_error(rid, "bad_request", "data is not valid base64")
        if len(data) > MAX_HUMAN_INPUT_BYTES:
            return self.send_error(rid, "too_large", "input exceeds 64 KiB per frame")
        result, error = self.daemon.human_input(self, data)
        if error is not None:
            return self.send_error(rid, error["code"], error["message"])
        return self.send_result(rid, result)

    # -- teardown ---------------------------------------------------------

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.daemon.unregister(self)
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
        self._finish_writer()


# ---- the daemon --------------------------------------------------------------


class DaemonServer:
    """Owns the device, the client connections, the lease, and the lifetime.

    ``runtime`` is the private runtime directory. The daemon always takes the
    exclusive ``daemon.lock`` itself and never serves or touches the socket without
    it; if another daemon keeps the lock past ``LOCK_PROBE_WAIT_SECONDS`` it refuses to run.
    """

    def __init__(
        self,
        runtime: Path | None = None,
        *,
        clock=time.monotonic,
        state=None,
        start_housekeeping: bool = True,
    ) -> None:
        self.runtime = Path(runtime) if runtime is not None else ipc.runtime_dir()
        self.clock = clock
        # Created in start(), after the lock is held: constructing SerialState truncates
        # the log, and a daemon that loses the lock must not touch it.
        self.state = state
        self._start_housekeeping = start_housekeeping
        self._lock_fd: int | None = None
        self._lock = threading.RLock()
        self._drained = threading.Condition(self._lock)
        self._listener: socket.socket | None = None
        self._connections: set[Connection] = set()
        self._clients: set[Connection] = set()
        self._ids = itertools.count(1)
        self._shutting_down = False
        self._stopped = threading.Event()
        self._grace_started: float | None = None
        self._rx_offset = 0
        # Lease: one slot for the single device.
        self._holder: Connection | None = None
        self._active = 0
        self._inflight = 0
        self._pending_release = False
        self._idle_since: float | None = None
        self._generation = 0

    # -- lifecycle --------------------------------------------------------

    @property
    def socket_path(self) -> Path:
        return self.runtime / "daemon.sock"

    def log(self, prefix: str, message: str) -> None:
        try:
            self.state._log(prefix, message)
        except Exception:
            pass

    def start(self) -> None:
        ipc.ensure_runtime_dir(self.runtime)
        self._acquire_lock()
        try:
            if self.state is None:
                # The one engine this daemon owns. Built here, not at import time.
                self.state = SerialState()
            self.state.rx_listeners.append(self._on_device_rx)
            self._bind()
        except Exception:
            self._release_lock()
            raise
        with self._lock:
            self._grace_started = self.clock()
        threading.Thread(target=self._accept_loop, name="daemon-accept", daemon=True).start()
        if self._start_housekeeping:
            threading.Thread(target=self._housekeeping_loop, name="daemon-housekeeping", daemon=True).start()
        self.log("SYS", f"daemon listening on {self.socket_path}")

    def _acquire_lock(self) -> None:
        path = self.runtime / "daemon.lock"
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        deadline = time.monotonic() + LOCK_PROBE_WAIT_SECONDS
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise DaemonAlreadyRunning(f"another daemon holds {path}") from None
                time.sleep(0.02)
        self._lock_fd = fd

    def _release_lock(self) -> None:
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)  # closing releases the flock
            except OSError:
                pass
        self._lock_fd = None

    def _bind(self) -> None:
        path = self.socket_path
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            st = None
        if st is not None:
            if not stat.S_ISSOCK(st.st_mode):
                raise UnsafeRuntimeDir(f"{path} exists and is not a socket; refusing to replace it")
            # We hold the lock, so no live daemon can own this socket: it is stale.
            os.unlink(path)
        if len(os.fsencode(str(path))) > ipc.SUN_PATH_MAX:
            raise UnsafeRuntimeDir(f"socket path is too long for AF_UNIX: {path}")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        old_umask = os.umask(0o177)
        try:
            listener.bind(str(path))
        finally:
            os.umask(old_umask)
        os.chmod(path, 0o600)
        listener.listen(16)
        listener.settimeout(0.2)
        self._listener = listener

    def stop(self, reason: str = "requested") -> None:
        """Drain in-flight work, release the device, and stop listening."""
        with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True
            for conn in list(self._connections):
                conn.send_control({"v": PROTOCOL_VERSION, "event": "shutdown", "reason": reason})
        self._close_listener()
        deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
        with self._drained:
            while self._inflight > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.log("WARN", "shutdown: in-flight operations did not settle; closing the device")
                    break
                self._drained.wait(remaining)
        closer = threading.Thread(target=self._close_device, name="daemon-device-close", daemon=True)
        closer.start()
        closer.join(DRAIN_TIMEOUT_SECONDS)
        for conn in list(self._connections):
            conn.close()
        try:
            if os.path.lexists(self.socket_path):
                os.unlink(self.socket_path)
        except OSError:
            pass
        self._release_lock()
        self.log("SYS", f"daemon stopped ({reason})")
        self._stopped.set()

    def _close_device(self) -> None:
        if self.state is None:
            return
        try:
            self.state.disconnect()
        except Exception as exc:
            self.log("ERR", f"device close during shutdown failed: {exc!r}")

    def _close_listener(self) -> None:
        listener, self._listener = self._listener, None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass

    def wait_stopped(self, timeout: float | None = None) -> bool:
        return self._stopped.wait(timeout)

    @property
    def stopped(self) -> bool:
        return self._stopped.is_set()

    def _accept_loop(self) -> None:
        while True:
            listener = self._listener
            if listener is None or self._shutting_down:
                return
            try:
                sock, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if not _peer_is_same_user(sock):
                sock.close()
                continue
            sock.settimeout(None)
            conn = Connection(self, sock, f"c{next(self._ids)}")
            with self._lock:
                self._connections.add(conn)
            conn.start()

    def _housekeeping_loop(self) -> None:
        while not self._stopped.is_set():
            self.tick()
            self._stopped.wait(0.1)

    def tick(self) -> None:
        """Advance lease expiry and grace. Tests call this after moving the clock."""
        now = self.clock()
        with self._lock:
            if self._shutting_down:
                return
            self._expire_locked(now)
            expired_grace = (
                not self._clients
                and self._grace_started is not None
                and now - self._grace_started >= GRACE_SECONDS
            )
        if expired_grace:
            self.stop(reason="grace period expired")

    # -- clients and lifetime ---------------------------------------------

    def register(self, conn: Connection) -> dict | None:
        with self._lock:
            if self._shutting_down:
                return None
            self._clients.add(conn)
            self._grace_started = None
            now = self.clock()
            view = self._lease_view_locked(conn, now)
            return {
                "protocol": PROTOCOL_VERSION,
                "daemon_version": DAEMON_VERSION,
                "client_id": conn.cid,
                "role": conn.role,
                "lease": view,
            }

    def unregister(self, conn: Connection) -> None:
        with self._lock:
            self._connections.discard(conn)
            if conn not in self._clients:
                # A connection that never completed hello is not a client: closing it
                # must not touch the lease or restart the grace period.
                return
            self._clients.discard(conn)
            if self._holder is conn:
                if self._active == 0:
                    self._free_locked()
                else:
                    self._pending_release = True  # closing until the operation settles
            if not self._clients and not self._shutting_down:
                self._grace_started = self.clock()
            self._drained.notify_all()

    def status(self, viewer: Connection | None = None) -> dict:
        with self._lock:
            now = self.clock()
            grace_remaining = None
            if self._grace_started is not None and not self._clients:
                grace_remaining = max(0.0, GRACE_SECONDS - (now - self._grace_started))
            return {
                "daemon_version": DAEMON_VERSION,
                "protocol": PROTOCOL_VERSION,
                "clients": len(self._clients),
                "grace": {"active": grace_remaining is not None, "remaining": grace_remaining},
                "shutting_down": self._shutting_down,
                "device": _device_label(self.state),
                "lease": self._lease_view_locked(viewer, now),
            }

    def rx_offset(self) -> int:
        with self._lock:
            return self._rx_offset

    # -- device output fan-out --------------------------------------------

    def _on_device_rx(self, chunk: bytes) -> None:
        with self._lock:
            seq = self._rx_offset
            self._rx_offset += len(chunk)
            subscribers = [c for c in self._clients if c.subscribed]
        for conn in subscribers:
            conn.offer_output(seq, chunk)

    def _broadcast_device_event(self, state_name: str) -> None:
        frame = {"v": PROTOCOL_VERSION, "event": "device", "state": state_name,
                 "label": _device_label(self.state)}
        with self._lock:
            clients = list(self._clients)
        for conn in clients:
            conn.send_control(frame)

    # -- lease --------------------------------------------------------------

    def _lease_view_locked(self, viewer: Connection | None, now: float) -> dict:
        holder = self._holder
        expires_in = None
        if holder is None:
            state_name = "free"
        elif holder.closed or self._pending_release:
            state_name = "closing"  # held only until the in-flight operation settles
        elif self._active > 0:
            state_name = "active"
        else:
            state_name = "idle"
            if self._idle_since is not None:
                expires_in = max(0.0, LEASE_IDLE_SECONDS - (now - self._idle_since))
        if viewer is not None and holder is viewer:
            you = "holder"
        elif viewer is not None and viewer.needs_reacquire:
            you = "reacquire_required"
        else:
            you = "none"
        return {
            "state": state_name,
            "holder": None if holder is None else {
                "client_id": holder.cid, "name": holder.name, "role": holder.role, "pid": holder.pid,
            },
            "expires_in": expires_in,
            "active_operations": self._active,
            "generation": self._generation,
            "you": you,
        }

    def _grant_locked(self, conn: Connection, now: float) -> None:
        self._holder = conn
        self._generation += 1
        self._idle_since = now
        self._pending_release = False
        conn.needs_reacquire = False
        conn.lost_reason = None

    def _free_locked(self) -> None:
        self._holder = None
        self._idle_since = None
        self._pending_release = False

    def _expire_locked(self, now: float) -> None:
        holder = self._holder
        if holder is None or self._active > 0 or self._idle_since is None:
            return
        if now - self._idle_since < LEASE_IDLE_SECONDS:
            return
        self._free_locked()
        holder.needs_reacquire = True
        holder.lost_reason = "expired"
        holder.send_control({
            "v": PROTOCOL_VERSION, "event": "lease_lost", "reason": "expired",
            "warning": _LOST_WARNING,
        })

    def _busy_error_locked(self, viewer: Connection, now: float) -> dict:
        view = self._lease_view_locked(viewer, now)
        holder = view["holder"]
        who = f"{holder['name'] or holder['client_id']}" if holder else "another client"
        return {"code": "busy",
                "message": f"the device is under control of {who} ({view['state']}); "
                           "retry after it is released or expires",
                "lease": view}

    def _claim_locked(self, conn: Connection, now: float) -> dict | None:
        """Return an error object if ``conn`` may not start a gated operation."""
        holder = self._holder
        if holder is conn:
            if self._pending_release or conn.closed:
                return {"code": "lease_lost", "message": "your lease was released; acquire it again",
                        "lease": self._lease_view_locked(conn, now)}
            return None
        if holder is not None:
            return self._busy_error_locked(conn, now)
        if conn.needs_reacquire:
            return {"code": "lease_lost",
                    "message": f"your control was lost ({conn.lost_reason or 'released'}); "
                               "acquire the lease explicitly before using the device",
                    "lease": self._lease_view_locked(conn, now)}
        self._grant_locked(conn, now)
        return None

    def lease_action(self, conn: Connection, action: str) -> tuple[dict | None, dict | None]:
        now = self.clock()
        with self._lock:
            self._expire_locked(now)
            if action == "status":
                return self._lease_view_locked(conn, now), None
            if action == "release":
                if self._holder is conn:
                    if self._active > 0:
                        self._pending_release = True
                    else:
                        self._free_locked()
                    conn.needs_reacquire = True
                    conn.lost_reason = "released"
                return self._lease_view_locked(conn, now), None
            # acquire
            if self._shutting_down:
                return None, {"code": "shutting_down", "message": "the daemon is shutting down"}
            if self._holder is conn:
                if self._pending_release or conn.closed:
                    return None, self._busy_error_locked(conn, now)
                if self._active == 0:
                    self._idle_since = now
                return self._lease_view_locked(conn, now), None
            if self._holder is not None:
                return None, self._busy_error_locked(conn, now)
            warning = None
            if conn.needs_reacquire:
                warning = _REACQUIRE_WARNING
            self._grant_locked(conn, now)
            view = self._lease_view_locked(conn, now)
            if warning:
                view["warning"] = warning
            return view, None

    def _begin_gated(self, conn: Connection) -> dict | None:
        now = self.clock()
        with self._lock:
            if self._shutting_down:
                return {"code": "shutting_down", "message": "the daemon is shutting down"}
            self._expire_locked(now)
            error = self._claim_locked(conn, now)
            if error is None:
                self._active += 1
                self._inflight += 1
            return error

    def _end_gated(self, conn: Connection) -> None:
        now = self.clock()
        with self._drained:
            self._active -= 1
            self._inflight -= 1
            if self._active == 0 and self._holder is conn and (self._pending_release or conn.closed):
                self._free_locked()
            elif self._active == 0 and self._holder is not None:
                self._idle_since = now
            self._drained.notify_all()

    # -- tools and human input -----------------------------------------------

    def run_call(self, conn: Connection, rid, tool: str, args: dict) -> None:
        method_name, gated = TOOL_TABLE[tool]
        if method_name is None:
            conn.send_result(rid, {"result": FILE_TRANSFER_CAPABILITIES})
            return
        method = getattr(self.state, method_name)
        try:
            bound = inspect.signature(method).bind(**args)
        except TypeError as exc:
            conn.send_error(rid, "bad_args", f"{tool}: {exc}")
            return
        if gated:
            error = self._begin_gated(conn)
            if error is not None:
                conn.send_error(rid, error["code"], error["message"], lease=error.get("lease"))
                return
        try:
            text = method(*bound.args, **bound.kwargs)
        except Exception as exc:
            self.log("ERR", f"tool {tool} raised: {exc!r}")
            conn.send_error(rid, "internal", f"{tool} failed unexpectedly")
            return
        finally:
            if gated:
                self._end_gated(conn)
        text = text if isinstance(text, str) else str(text)
        try:
            encode_frame({"id": rid, "ok": True, "result": {"result": text}})
        except ProtocolError:
            conn.send_error(rid, "result_too_large",
                            "the result does not fit a 4 MiB frame; use max_lines or view_io")
            return
        conn.send_result(rid, {"result": text})
        if tool in DEVICE_CHANGING_TOOLS:
            self._broadcast_device_event("connected" if _device_label(self.state) else "disconnected")

    def human_input(self, conn: Connection, data: bytes) -> tuple[dict | None, dict | None]:
        with self._lock:
            if self._shutting_down:
                return None, {"code": "shutting_down", "message": "the daemon is shutting down"}
        if not self.state.human_write(data):
            return None, {"code": "not_connected", "message": "no device is connected"}
        return {"written": len(data)}, None


_FATAL_FRAME_CODES = frozenset({"frame_too_large", "unsupported_version"})
_LOST_WARNING = "control was lost (lease expired); device state may have changed since you last used it"
_REACQUIRE_WARNING = ("control was lost or released earlier; device state may have changed "
                      "since you last used it")


def _peer_is_same_user(sock: socket.socket) -> bool:
    creds = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    _pid, uid, _gid = struct.unpack("3i", creds)
    return uid == os.geteuid()


# ---- entry point -------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="serial-mcp-daemon", description=__doc__.splitlines()[0])
    parser.parse_args(argv)

    try:
        # The daemon's log lives in its private runtime directory. Set before the engine
        # is constructed in start(), so the log never lands in /tmp/serial-mcp.log.
        os.environ["SERIAL_MCP_LOG"] = os.environ.get("SERIAL_MCP_DAEMON_LOG") or str(
            ipc.runtime_dir() / "daemon.log")
        server = DaemonServer(start_housekeeping=True)
        server.start()
    except DaemonAlreadyRunning:
        return 0
    except UnsafeRuntimeDir as exc:
        print(f"serial-mcp-daemon: {exc}", file=sys.stderr)
        return 2

    def on_signal(_signum, _frame):
        server.stop(reason="signal")

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    server.wait_stopped()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
