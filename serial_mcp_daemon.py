"""Multi-device serial daemon implementing protocol v2 in docs/daemon-ipc.md.

The daemon owns independently opened physical ttys, their reader/buffer state,
per-client memberships and per-device AI control leases. Its local UNIX IPC
accepts AI calls, human join/leave/input, independent output subscriptions and
status snapshots. Zero registered clients starts a cancellable 30-second shutdown
grace; last-member departure settles device operations before closing that tty.

The clock is injected so lease expiry and grace are deterministic in tests;
production uses ``time.monotonic``.
"""

from __future__ import annotations

import base64
import codecs
import fcntl
import inspect
import itertools
import json
import math
import os
import select
import signal
import socket
import stat
import struct
import sys
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from pathlib import Path

import serial_mcp_ipc as ipc
from serial_mcp_engine import FILE_TRANSFER_CAPABILITIES, SerialState
from serial_mcp_ipc import (
    DRAIN_TIMEOUT_SECONDS,
    GRACE_SECONDS,
    HELLO_TIMEOUT_SECONDS,
    LEASE_IDLE_SECONDS,
    MAX_HUMAN_INPUT_BYTES,
    MAX_CLIENT_EVENT_QUEUE,
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

DAEMON_VERSION = "0.3.0"
MAX_CLIENT_NAME = 64
MAX_PENDING_SETTINGS = 32
MAX_CONTROL_QUEUE_FRAMES = 64
ROLES = ("mcp", "human")

# Tool name -> (SerialState method, gated). Global tools are dispatched explicitly
# below so device membership and control checks cannot leak across tool scope.
TOOL_TABLE: dict[str, tuple[str | None, bool]] = {
    "connect": ("connect", True), "disconnect": ("disconnect", True),
    "set_baudrate": ("set_baudrate", True), "send_command": ("send_command", True),
    "read_output": ("read_output", False), "clear_output": ("clear_output", True),
    "set_prompt_pattern": ("set_prompt_pattern", True), "login": ("login", True),
    "enter_bootloader": ("enter_bootloader", True), "upload_file": ("upload_file", True),
    "download_file": ("download_file", True), "view_io": ("view_io", False),
    "file_transfer_capabilities": (None, False),
    "list_serial_ports": ("list_serial_ports", False),
    "list_boot_profiles": ("list_boot_profiles", False),
    "set_boot_credentials": ("set_boot_credentials", False),
}
@dataclass
class Device:
    """One normalized physical tty and its membership/control state."""

    device_id: str
    identity: tuple[int, int]
    state: SerialState
    port: str
    baudrate: int
    members: set["Connection"] = field(default_factory=set)
    controller: "Connection | None" = None
    active: int = 0
    in_flight: int = 0
    idle_since: float | None = None
    generation: int = 0
    rx_offset: int = 0
    cursors: dict["Connection", int] = field(default_factory=dict)
    output: deque[tuple[int, bytes]] = field(default_factory=deque)
    output_bytes: int = 0
    output_start_offset: int = 0
    closing: bool = False
    pending_release: bool = False
    settings_lock: threading.Lock = field(default_factory=threading.Lock)
    rx_listener: object | None = None
    paths: set[str] = field(default_factory=set)
    node_fingerprint: tuple[int, int, int, int] | None = None
    online: bool = False
    recovering: bool = False
    close_when_idle: bool = False


class DaemonAlreadyRunning(Exception):
    """Another daemon holds the lock for this runtime directory."""

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
        self.subscribed_ports: set[str] = set()
        self.memberships: set[str] = set()
        self.closed = False
        self.calls_in_flight = 0
        self._cv = threading.Condition()
        self._control: deque[bytes] = deque()
        self._events: deque[bytes] = deque()
        self._settings_pending: OrderedDict[str, tuple[dict, bool]] = OrderedDict()
        self._chunks: deque[tuple[int, bytes, dict]] = deque()
        self._queued_bytes = 0
        self._dropped: dict[str, int] = {}
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
            overflow = len(self._control) >= MAX_CONTROL_QUEUE_FRAMES
            if not overflow:
                self._control.append(data)
                self._cv.notify()
        if overflow:
            self.daemon.log("WARN", f"closing {self.cid}: control response queue overflow")
            self.close()

    def _queue_event_locked(self, frame: dict) -> None:
        if len(self._events) >= MAX_CLIENT_EVENT_QUEUE:
            dropped = 0
            while len(self._events) > MAX_CLIENT_EVENT_QUEUE - 2:
                try:
                    previous = json.loads(self._events.popleft())
                except (ValueError, TypeError):
                    previous = {}
                dropped += (previous.get("dropped_events", 1)
                            if previous.get("event") == "event_gap" else 1)
            self._events.append(encode_frame({"event": "event_gap", "dropped_events": dropped,
                "reason": "server event queue overflow"}))
        self._events.append(encode_frame(frame))

    def send_event(self, frame: dict) -> None:
        with self._cv:
            if self._stop_writer:
                return
            self._queue_event_locked(frame)
            self._cv.notify()

    def stop_subscribing(self, device_id: str) -> None:
        """Drop this device's queued stream chunks at an unsubscribe/leave seam."""
        with self._cv:
            self.subscribed_ports.discard(device_id)
            retained = deque()
            for seq, data, metadata in self._chunks:
                if metadata.get("device_id") == device_id:
                    self._queued_bytes -= len(data)
                else:
                    retained.append((seq, data, metadata))
            self._chunks = retained
            self._dropped.pop(device_id, None)
            self._settings_pending.pop(str(device_id), None)
            self._cv.notify_all()

    def send_settings_notice(self, notice: dict) -> None:
        """Coalesce latest settings per device in a bounded pending map."""
        device_id = notice.get("device_id")
        key = str(device_id)
        with self._cv:
            if self._stop_writer:
                return
            previous = self._settings_pending.pop(key, None)
            if previous is None:
                if len(self._settings_pending) >= MAX_PENDING_SETTINGS:
                    evicted_id, (evicted, _had_gap) = self._settings_pending.popitem(last=False)
                    self._queue_event_locked({"event": "settings_gap", "device_id": evicted_id,
                        "port": evicted.get("port"), "reason": "settings notice queue overflow"})
                had_gap = False
            else:
                had_gap = True
            self._settings_pending[key] = (notice, had_gap)
            self._cv.notify()

    def send_result(self, rid, result: dict) -> None:
        self.send_control({"id": rid, "ok": True, "result": result})

    def send_error(self, rid, code: str, message: str, **extra) -> None:
        error = {"code": code, "message": message}
        error.update(extra)
        self.send_control({"id": rid, "ok": False, "error": error})

    def offer_output(self, seq: int, data: bytes, metadata: dict | None = None) -> None:
        """Queue device output for this subscriber without ever blocking.

        Overflow drops the oldest queued bytes and counts them; the writer then
        reports an ``output_gap`` before the next chunk.
        """
        with self._cv:
            if self._stop_writer:
                return
            for start in range(0, len(data), MAX_OUTPUT_EVENT_BYTES):
                piece = data[start:start + MAX_OUTPUT_EVENT_BYTES]
                self._chunks.append((seq + start, piece, metadata or {}))
                self._queued_bytes += len(piece)
            while self._queued_bytes > MAX_SUBSCRIBER_QUEUE_BYTES and self._chunks:
                _seq, dropped, metadata = self._chunks.popleft()
                self._queued_bytes -= len(dropped)
                device_id = metadata.get("device_id", "")
                self._dropped[device_id] = self._dropped.get(device_id, 0) + len(dropped)
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
            while (not self._control and not self._settings_pending and not self._events
                   and not self._chunks and not self._stop_writer):
                self._cv.wait()
            if self._control:
                return [self._control.popleft()]
            if self._settings_pending:
                device_id, (notice, had_gap) = next(iter(self._settings_pending.items()))
                if had_gap:
                    self._settings_pending[device_id] = (notice, False)
                    return [encode_frame({"event": "settings_gap", "device_id": device_id,
                        "port": notice.get("port"), "reason": "settings notices coalesced"})]
                del self._settings_pending[device_id]
                return [encode_frame(notice)]
            if self._events:
                return [self._events.popleft()]
            if self._chunks:
                seq, data, metadata = self._chunks[0]
                device_id = metadata.get("device_id", "")
                dropped = self._dropped.pop(device_id, 0)
                if dropped:
                    return [encode_frame({"event": "output_gap", "dropped_bytes": dropped,
                                          "seq": seq, **metadata})]
                self._chunks.popleft()
                self._queued_bytes -= len(data)
                return [encode_frame({"event": "output", "seq": seq,
                                      "data": base64.b64encode(data).decode("ascii"),
                                      **metadata})]
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
            self._events.clear()
            self._settings_pending.clear()
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
            self._events.clear()
            self._settings_pending.clear()
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
        if op in ("subscribe", "unsubscribe"):
            port = frame.get("port")
            if not isinstance(port, str):
                return self.send_error(rid, "bad_request", "port is required")
            device = self.daemon._device_for_call(self, port)
            if device is None:
                return self.send_error(rid, "not_member", "join the device before subscribing")
            with self.daemon._lock:
                if op == "subscribe":
                    self.subscribed_ports.add(device.device_id)
                    cursor = device.rx_offset
                    return self.send_result(rid, {"subscribed": True, "device_id": device.device_id,
                                                  "cursor": cursor, "generation": device.generation})
                self.stop_subscribing(device.device_id)
                return self.send_result(rid, {"subscribed": False, "device_id": device.device_id})
        if op in ("join", "leave"):
            return self._op_membership(rid, frame, op)
        if op == "ports":
            if self.role != "human":
                return self.send_error(rid, "forbidden_role", "port enumeration is a human operation")
            return self.send_result(rid, {"result": self.daemon.state.list_serial_ports()})
        if op == "lease":
            port = frame.get("port")
            device = self.daemon._device_for_call(self, port) if isinstance(port, str) else None
            if frame.get("action") != "status" or device is None:
                return self.send_error(rid, "unknown_op", "only per-device lease status is supported")
            return self.send_result(rid, {"state": "free" if device.controller is None else
                                          ("active" if device.active else "idle"),
                                          "expires_in": None if device.idle_since is None else
                                          max(0, LEASE_IDLE_SECONDS - (self.daemon.clock() - device.idle_since)),
                                          "active_operations": device.active,
                                          "generation": device.generation,
                                          "you": "holder" if device.controller is self else "none"})
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
        with self._cv:
            overloaded = self.calls_in_flight >= MAX_INFLIGHT_CALLS_PER_CONNECTION
            if not overloaded:
                self.calls_in_flight += 1
        if overloaded:
            return self.send_error(rid, "overloaded", "too many concurrent calls on this connection")
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

    def _op_membership(self, rid, frame: dict, action: str) -> None:
        if self.role != "human":
            return self.send_error(rid, "forbidden_role", "internal membership is human-only")
        port = frame.get("port")
        if not isinstance(port, str):
            return self.send_error(rid, "bad_request", "port is required")
        if action == "leave":
            device = self.daemon._device_for_call(self, port)
            if device is not None:
                self.daemon._leave_device(self, device)
            return self.send_result(rid, {"left": True})
        device, error = self.daemon._join_device(self, port, frame.get("baudrate"), ai=False)
        if error:
            return self.send_error(rid, error["code"], error["message"])
        self.daemon._broadcast_device_event("joined", device)
        return self.send_result(rid, {"joined": True, "device_id": device.device_id,
                                      "port": port, "baudrate": device.baudrate,
                                      "control": "available" if device.controller is None else "held"})

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
        port = frame.get("port")
        result, error = self.daemon.human_input(self, data, port)
        if error is not None:
            return self.send_error(rid, error["code"], error["message"])
        return self.send_result(rid, result)

    # -- teardown ---------------------------------------------------------

    def close(self) -> None:
        with self._cv:
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
        self._shutdown_deadline: float | None = None
        self._stopped = threading.Event()
        self._grace_started: float | None = None
        self._devices: dict[tuple[int, int], Device] = {}
        self._device_ids = itertools.count(1)

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
        deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
        with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True
            self._shutdown_deadline = deadline
            for conn in list(self._connections):
                conn.send_control({"v": PROTOCOL_VERSION, "event": "shutdown", "reason": reason})
        self._close_listener()
        # Unregister first, then wait only until the shared drain deadline.
        # A transfer can outlive it; client cleanup is deferred to operation end,
        # while process shutdown proceeds with the bounded close attempt below.
        for conn in list(self._connections):
            conn.close()
        with self._drained:
            while any(device.in_flight for device in self._devices.values()):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.log("WARN", "shutdown drain timed out; closing transports asynchronously")
                    break
                self._drained.wait(remaining)
        closer = threading.Thread(target=self._close_device, name="daemon-device-close", daemon=True)
        closer.start()
        closer.join(DRAIN_TIMEOUT_SECONDS)
        if closer.is_alive():
            self.log("WARN", "device close is still in progress after shutdown timeout")
        try:
            if os.path.lexists(self.socket_path):
                os.unlink(self.socket_path)
        except OSError:
            pass
        self._release_lock()
        self.log("SYS", f"daemon stopped ({reason})")
        self._stopped.set()

    def _close_device(self) -> None:
        with self._drained:
            devices = list(self._devices.values())
            ready = []
            for device in devices:
                if device.in_flight:
                    # Do not race driver I/O after the bounded shutdown deadline.
                    # Its final operation completion owns the deferred close.
                    device.close_when_idle = True
                else:
                    ready.append(device)
            states = {id(device.state) for device in devices}
            fallback = self.state if self.state is not None and id(self.state) not in states else None
        for device in ready:
            self._disconnect_device_after_drain(device)
        if fallback is not None:
            try:
                fallback.disconnect()
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
            for device in self._devices.values():
                if (device.controller is not None and not device.active
                        and device.idle_since is not None
                        and now - device.idle_since >= LEASE_IDLE_SECONDS):
                    holder = device.controller
                    device.controller = None
                    device.idle_since = None
                    holder.send_event({"event": "control_lost", "device_id": device.device_id,
                                         "port": device.port, "reason": "expired"})
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
            count = len(self._clients)
            mcp_count = sum(client.role == "mcp" for client in self._clients)
            result = {"protocol": PROTOCOL_VERSION, "daemon_version": DAEMON_VERSION,
                      "client_id": conn.cid, "role": conn.role,
                      "devices": self._device_snapshots_locked(), "clients": count,
                      "mcp_clients": mcp_count}
        self._broadcast_count(count, mcp_count)
        return result

    def unregister(self, conn: Connection) -> None:
        with self._lock:
            self._connections.discard(conn)
            if conn not in self._clients:
                return
            self._clients.discard(conn)
            memberships = [d for d in self._devices.values() if conn in d.members]
            if not self._clients and not self._shutting_down:
                self._grace_started = self.clock()
            count = len(self._clients)
            mcp_count = sum(client.role == "mcp" for client in self._clients)
            self._drained.notify_all()
        deadline = (self._shutdown_deadline if self._shutting_down
                    and self._shutdown_deadline is not None
                    else time.monotonic() + DRAIN_TIMEOUT_SECONDS)
        for device in memberships:
            self._leave_device(conn, device, deadline=deadline)
        self._broadcast_count(count, mcp_count)

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
                "mcp_clients": sum(client.role == "mcp" for client in self._clients),
                "grace": {"active": grace_remaining is not None, "remaining": grace_remaining},
                "shutting_down": self._shutting_down,
                "devices": self._device_snapshots_locked(),
            }

    def _broadcast_count(self, count: int, mcp_count: int) -> None:
        frame = {"event": "clients", "clients": count, "mcp_clients": mcp_count}
        with self._lock:
            clients = list(self._clients)
        for conn in clients:
            conn.send_event(frame)

    def _device_snapshots_locked(self) -> list[dict]:
        snapshots = []
        for device in self._devices.values():
            live = self._device_is_live_locked(device)
            snapshots.append({
                "device_id": device.device_id, "port": device.port,
                "state": "closing" if device.closing else "connected" if live else "disconnected",
                "baudrate": device.baudrate,
                "control": "held" if live and device.controller is not None else "available",
                "members": len(device.members), "generation": device.generation,
            })
        return snapshots

    def _resolve_physical(self, port: str) -> tuple[tuple[int, int], tuple[int, int, int, int]]:
        if not isinstance(port, str) or not port:
            raise ValueError("port must be a non-empty path")
        st = os.stat(port)
        if not stat.S_ISCHR(st.st_mode):
            raise ValueError("port is not a character device")
        identity = (os.major(st.st_rdev), os.minor(st.st_rdev))
        fingerprint = (st.st_dev, st.st_ino, st.st_rdev, st.st_ctime_ns)
        return identity, fingerprint

    @staticmethod
    def _opened_node_fingerprint(device: Device) -> tuple[int, int, int, int] | None:
        transport = device.state.transport
        if transport is None or not transport.is_open:
            return None
        serial_handle = getattr(transport, "ser", None)
        if serial_handle is None:
            return None
        try:
            st = os.fstat(serial_handle.fileno())
        except (OSError, ValueError):
            return None
        return st.st_dev, st.st_ino, st.st_rdev, st.st_ctime_ns

    def _device_is_live_locked(self, device: Device) -> bool:
        opened = self._opened_node_fingerprint(device)
        if not device.online or opened is None or opened != device.node_fingerprint:
            if device.online:
                self._mark_device_disconnected_locked(device)
            return False
        for path in device.paths:
            try:
                identity, fingerprint = self._resolve_physical(path)
            except (OSError, ValueError):
                continue
            if identity == device.identity and fingerprint == opened:
                return True
        self._mark_device_disconnected_locked(device)
        return False

    def _mark_device_disconnected_locked(self, device: Device) -> None:
        if not device.online:
            return
        device.online = False
        device.generation += 1
        holder, device.controller = device.controller, None
        device.idle_since = None
        device.pending_release = False
        if holder is not None:
            holder.send_event({"event": "control_lost", "device_id": device.device_id,
                               "port": device.port, "reason": "device_disconnected"})
        self._broadcast_device_event("disconnected", device)

    # -- device output fan-out --------------------------------------------

    def _on_device_rx_for(self, device: Device, chunk: bytes) -> None:
        with self._drained:
            seq = device.rx_offset
            generation = device.generation
            device.rx_offset += len(chunk)
            subscribers = [c for c in device.members
                           if device.device_id in c.subscribed_ports]
        for conn in subscribers:
            conn.offer_output(seq, chunk, {"device_id": device.device_id,
                                           "port": device.port,
                                           "generation": generation})
        with self._lock:
            device.output.append((seq, bytes(chunk)))
            device.output_bytes += len(chunk)
            while device.output_bytes > MAX_SUBSCRIBER_QUEUE_BYTES and device.output:
                _old_seq, old = device.output.popleft()
                device.output_bytes -= len(old)
                device.output_start_offset = _old_seq + len(old)
            self._drained.notify_all()

    def _broadcast_device_event(self, state_name: str, device: Device | None = None,
                                *, all_clients: bool = False) -> None:
        frame = {"event": "device", "state": state_name}
        if device is not None:
            frame.update({"device_id": device.device_id, "port": device.port,
                          "baudrate": device.baudrate, "generation": device.generation})
        with self._lock:
            clients = (list(self._clients) if device is None or all_clients
                       else list(device.members))
        for conn in clients:
            conn.send_event(frame)
    # -- device membership, control leases and tools -------------------------

    def _join_device(self, conn: Connection, port: str, baudrate: int | None, *, ai: bool) -> tuple[Device | None, dict | None]:
        try:
            identity, fingerprint = self._resolve_physical(port)
        except (OSError, ValueError) as exc:
            return None, {"code": "invalid_port", "message": str(exc)}
        if ai and (baudrate is None or isinstance(baudrate, bool)
                   or not isinstance(baudrate, int) or baudrate <= 0):
            return None, {"code": "bad_args", "message": "connect requires a positive integer baudrate"}
        if baudrate is not None and (isinstance(baudrate, bool)
                                     or not isinstance(baudrate, int) or baudrate <= 0):
            return None, {"code": "bad_args", "message": "baudrate must be a positive integer"}

        with self._drained:
            if self._shutting_down:
                return None, {"code": "shutting_down", "message": "the daemon is shutting down"}
            if conn.closed:
                return None, {"code": "client_closed", "message": "client disconnected during join"}
            device = self._devices.get(identity)
            if device is not None and device.closing:
                deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
                while device.closing and self._devices.get(identity) is device:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None, {"code": "busy", "message": "device did not finish opening or closing in time"}
                    self._drained.wait(remaining)
                device = self._devices.get(identity)
            if device is not None and device.node_fingerprint == fingerprint \
                    and self._device_is_live_locked(device):
                device.paths.add(port)
                device.members.add(conn)
                conn.memberships.add(device.device_id)
                device.cursors.setdefault(conn, device.rx_offset)
                if device.controller is conn:
                    device.idle_since = self.clock()
                return device, None

            candidate = device
            if candidate is not None:
                self._mark_device_disconnected_locked(candidate)
            if candidate is None:
                for item in self._devices.values():
                    if port in item.paths:
                        self._device_is_live_locked(item)
                candidate = next((item for item in self._devices.values()
                                  if port in item.paths and not item.online and not item.closing), None)
            if candidate is not None:
                collision = self._devices.get(identity)
                if collision is not None and collision is not candidate:
                    return None, {"code": "busy", "message": "the replacement tty is already owned by another device session"}
                if candidate.closing or candidate.recovering:
                    return None, {"code": "busy", "message": "device recovery is already in progress"}
                candidate.closing = True
                candidate.recovering = True
                old_identity = candidate.identity
                if old_identity != identity:
                    if self._devices.get(old_identity) is candidate:
                        del self._devices[old_identity]
                    candidate.identity = identity
                    self._devices[identity] = candidate
                candidate.paths.add(port)
                deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
                while candidate.in_flight:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        candidate.recovering = False
                        candidate.closing = False
                        candidate.identity = old_identity
                        if self._devices.get(identity) is candidate:
                            del self._devices[identity]
                        self._devices[old_identity] = candidate
                        self._drained.notify_all()
                        return None, {"code": "busy", "message": "device operation did not settle before recovery"}
                    self._drained.wait(remaining)
                # Count the reconnect transaction so shutdown and membership
                # cleanup see it as in-flight work while the engine is reopened.
                candidate.active += 1
                candidate.in_flight += 1
                recovering = candidate
            else:
                if baudrate is None:
                    return None, {"code": "bad_args", "message": "baudrate is required when opening a device"}
                device_id = f"d{next(self._device_ids)}"
                state = self.state if not self._devices else SerialState(
                    log_path=str(self.runtime / f"serial-{device_id}.log"))
                device = Device(device_id, identity, state, port, baudrate, active=1, in_flight=1,
                                paths={port}, node_fingerprint=fingerprint)
                device.closing = True  # reserve identity while the driver open runs
                device.rx_listener = lambda chunk, d=device: self._on_device_rx_for(d, chunk)
                state.rx_listeners.append(device.rx_listener)
                self._devices[identity] = device
                recovering = None

        if recovering is not None:
            device = recovering
            state = device.state
            try:
                state.disconnect()
                opened = state.connect(port=port, baudrate=device.baudrate, session_socket="")
                if not isinstance(opened, str) or opened.startswith("Error"):
                    raise OSError(opened if isinstance(opened, str) else "device reopen failed")
                actual_identity = getattr(state.transport, "device_id", None)
                actual_fingerprint = self._opened_node_fingerprint(device)
                if (actual_identity is None
                        or (os.major(actual_identity), os.minor(actual_identity)) != identity
                        or actual_fingerprint != fingerprint):
                    raise OSError("reopened tty identity changed while connecting")
            except Exception as exc:
                try:
                    state.disconnect()
                except Exception:
                    pass
                with self._drained:
                    device.active = 0
                    device.in_flight = 0
                    device.online = False
                    device.recovering = False
                    device.closing = False
                    if self._devices.get(identity) is device:
                        del self._devices[identity]
                    self._devices[old_identity] = device
                    device.identity = old_identity
                    self._drained.notify_all()
                message = str(exc).replace("use its tio socket with attach(), or ask its owner to close it", "ask its owner to close it before connecting")
                return None, {"code": "internal", "message": f"could not reopen device: {message}"}
            with self._drained:
                device.port = port
                device.node_fingerprint = actual_fingerprint
                device.active = 0
                device.in_flight = 0
                device.online = True
                device.recovering = False
                device.closing = False
                device.generation += 1
                device.output.clear()
                device.output_bytes = 0
                device.output_start_offset = device.rx_offset
                device.cursors = {member: device.rx_offset for member in device.members}
                actual_baudrate = getattr(state.transport, "baudrate", device.baudrate)
                if isinstance(actual_baudrate, int) and actual_baudrate > 0:
                    device.baudrate = actual_baudrate
                if conn.closed or self._shutting_down:
                    cleanup = not device.members
                else:
                    device.members.add(conn)
                    conn.memberships.add(device.device_id)
                    device.cursors[conn] = device.rx_offset
                    cleanup = False
                self._drained.notify_all()
            self._broadcast_device_event("open", device)
            if cleanup:
                self._leave_device(conn, device)
                return None, {"code": "client_closed", "message": "client disconnected during recovery"}
            return device, None

        try:
            opened = state.connect(port=port, baudrate=baudrate, session_socket="")
            if not isinstance(opened, str) or opened.startswith("Error"):
                raise OSError(opened if isinstance(opened, str) else "device open failed")
            actual_identity = getattr(state.transport, "device_id", None)
            actual_fingerprint = self._opened_node_fingerprint(device)
            if (actual_identity is None
                    or (os.major(actual_identity), os.minor(actual_identity)) != identity
                    or actual_fingerprint != fingerprint):
                raise OSError("opened tty identity changed while connecting")
            actual_baudrate = getattr(state.transport, "baudrate", baudrate)
            if isinstance(actual_baudrate, int) and actual_baudrate > 0:
                device.baudrate = actual_baudrate
        except Exception as exc:
            try:
                if device.rx_listener in state.rx_listeners:
                    state.rx_listeners.remove(device.rx_listener)
                state.disconnect()
            except Exception:
                pass
            finally:
                with self._drained:
                    device.active = 0
                    device.in_flight = 0
                    if self._devices.get(identity) is device:
                        del self._devices[identity]
                    self._drained.notify_all()
            message = str(exc).replace("use its tio socket with attach(), or ask its owner to close it", "ask its owner to close it before connecting")
            return None, {"code": "internal", "message": f"could not open device: {message}"}
        with self._drained:
            if conn.closed or self._shutting_down:
                device.closing = True
                self._drained.notify_all()
                cleanup = True
            else:
                device.active = 0
                device.in_flight = 0
                device.closing = False
                device.online = True
                device.node_fingerprint = actual_fingerprint
                device.members.add(conn)
                conn.memberships.add(device.device_id)
                device.cursors[conn] = 0
                device.generation += 1
                self._drained.notify_all()
                cleanup = False
        if cleanup:
            try:
                if device.rx_listener in state.rx_listeners:
                    state.rx_listeners.remove(device.rx_listener)
                state.disconnect()
            except Exception:
                pass
            finally:
                with self._drained:
                    device.active = 0
                    device.in_flight = 0
                    if self._devices.get(identity) is device:
                        del self._devices[identity]
                    self._drained.notify_all()
            return None, {"code": "client_closed", "message": "client disconnected during join"}
        self._broadcast_device_event("open", device)
        return device, None

    def _leave_device(self, conn: Connection, device: Device,
                      *, deadline: float | None = None) -> None:
        self._broadcast_device_event("left", device)
        with self._drained:
            device.members.discard(conn)
            device.cursors.pop(conn, None)
            conn.memberships.discard(device.device_id)
            conn.stop_subscribing(device.device_id)
            if device.controller is conn:
                device.pending_release = True
            last_member = not device.members
            if last_member:
                device.closing = True
                self._drained.notify_all()
            if device.recovering:
                # The reconnect owner will either add its new membership or close
                # the device when recovery completes; do not race its driver calls.
                self._drained.notify_all()
                return
            if deadline is None:
                deadline = (self._shutdown_deadline if self._shutting_down
                            and self._shutdown_deadline is not None
                            else time.monotonic() + DRAIN_TIMEOUT_SECONDS)
            while device.in_flight:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.log("WARN", f"device drain timed out while {conn.cid} was leaving")
                    break
                self._drained.wait(remaining)
            timed_out = bool(device.in_flight)
            if device.controller is conn:
                device.controller = None
                device.idle_since = None
                device.pending_release = timed_out
            if not last_member:
                return
        if device.rx_listener in device.state.rx_listeners:
            device.state.rx_listeners.remove(device.rx_listener)
        if timed_out:
            # A transfer can explicitly refuse disconnect while it owns I/O.
            # Defer physical teardown until the accepted operation releases it.
            with self._drained:
                device.close_when_idle = True
                self._drained.notify_all()
            return
        self._disconnect_device_after_drain(device)
        with self._drained:
            if self._devices.get(device.identity) is device:
                del self._devices[device.identity]
            self._drained.notify_all()

    def _disconnect_device_after_drain(self, device: Device) -> None:
        try:
            device.state.disconnect()
        except Exception as exc:
            self.log("ERR", f"device close failed: {exc!r}")
        transport = getattr(device.state, "transport", None)
        if transport is None or not transport.is_open:
            device.online = False
            self._broadcast_device_event("closed", device, all_clients=True)
        else:
            self.log("WARN", f"device {device.device_id} refused close while I/O remained active")

    def _device_for_call(self, conn: Connection, port: str) -> Device | None:
        try:
            identity, fingerprint = self._resolve_physical(port)
        except (OSError, ValueError):
            identity = None
            fingerprint = None
        with self._lock:
            if identity is not None:
                device = self._devices.get(identity)
                if device is not None and conn in device.members:
                    if device.node_fingerprint != fingerprint:
                        self._mark_device_disconnected_locked(device)
                    else:
                        self._device_is_live_locked(device)
                    return device
            # A vanished tty cannot be stat'ed, but disconnect must remain possible
            # using the exact selector that originally joined this stale membership.
            for device in self._devices.values():
                if conn in device.members and port in device.paths:
                    self._device_is_live_locked(device)
                    return device
        return None

    def _check_device_control(self, conn: Connection, device: Device, modifying: bool) -> dict | None:
        now = self.clock()
        with self._lock:
            holder = device.controller
            if holder is not None and device.active == 0 and device.idle_since is not None \
                    and now - device.idle_since >= LEASE_IDLE_SECONDS:
                device.controller = None
                device.idle_since = None
                holder.send_event({"event": "control_lost", "device_id": device.device_id,
                                   "port": device.port, "reason": "expired"})
                holder = None
            if modifying:
                if holder is not conn or device.pending_release:
                    if holder is None:
                        return {"code": "control_required", "message": "call connect(port, baudrate) explicitly to acquire control"}
                    return {"code": "busy", "message": "device is controlled by another client"}
                device.active += 1
            device.in_flight += 1
            return None

    def _end_device_operation(self, device: Device, conn: Connection,
                              *, modifying: bool = True) -> None:
        with self._lock:
            device.in_flight = max(0, device.in_flight - 1)
            if modifying:
                device.active = max(0, device.active - 1)
            if modifying and device.active == 0 and device.controller is conn:
                device.idle_since = self.clock()
            if device.in_flight == 0 and device.pending_release and device.controller is None:
                device.pending_release = False
            close_now = device.in_flight == 0 and device.close_when_idle
            if close_now:
                device.close_when_idle = False
            self._drained.notify_all()
        if close_now:
            closer = threading.Thread(
                target=self._finish_deferred_close, args=(device,),
                name=f"daemon-close-{device.device_id}", daemon=True)
            closer.start()

    def _finish_deferred_close(self, device: Device) -> None:
        self._disconnect_device_after_drain(device)
        with self._drained:
            if self._devices.get(device.identity) is device:
                del self._devices[device.identity]
            self._drained.notify_all()

    def run_call(self, conn: Connection, rid, tool: str, args: dict) -> None:
        if conn.role != "mcp":
            conn.send_error(rid, "forbidden_role", "AI device tools require the mcp role")
            return
        if tool not in TOOL_TABLE:
            conn.send_error(rid, "unknown_tool", f"unknown tool {tool!r}")
            return
        if tool == "file_transfer_capabilities":
            if args:
                conn.send_error(rid, "bad_args", "file_transfer_capabilities accepts no arguments")
                return
            conn.send_result(rid, {"result": FILE_TRANSFER_CAPABILITIES})
            return
        if tool in {"list_serial_ports", "list_boot_profiles"}:
            if args:
                conn.send_error(rid, "bad_args", f"{tool} accepts no arguments")
                return
            try:
                result = getattr(self.state, TOOL_TABLE[tool][0])()
            except Exception:
                self.log("ERR", f"global tool {tool} failed")
                conn.send_error(rid, "internal", f"{tool} failed unexpectedly")
                return
            conn.send_result(rid, {"result": result})
            return
        if tool == "set_boot_credentials":
            if set(args) != {"profile_key", "username", "password"} or not all(
                isinstance(args[k], str) and args[k] and len(args[k]) <= 4096
                for k in ("profile_key", "username", "password")
            ):
                conn.send_error(rid, "bad_args", "set_boot_credentials requires non-empty profile_key, username, and password (each at most 4096 characters)")
                return
            try:
                result = self.state.set_boot_credentials(args["profile_key"], args["username"], args["password"])
            except Exception:
                self.log("ERR", "global tool set_boot_credentials failed")
                conn.send_error(rid, "internal", "set_boot_credentials failed unexpectedly")
                return
            if isinstance(result, str) and result.startswith("Error"):
                conn.send_error(rid, "credential_store_error", "credential storage failed")
            else:
                conn.send_result(rid, {"result": f"Credentials stored locally for {args['profile_key']}."})
            return
        port = args.get("port")
        if not isinstance(port, str) or not port:
            conn.send_error(rid, "bad_args", "port is required")
            return
        if tool == "connect":
            if set(args) != {"port", "baudrate"}:
                conn.send_error(rid, "bad_args", "connect accepts only port and baudrate")
                return
            device, error = self._join_device(conn, port, args.get("baudrate"), ai=True)
            if error:
                conn.send_error(rid, error["code"], error["message"])
                return
            with self._lock:
                if device.controller is None and not device.pending_release and device.active == 0:
                    device.controller = conn
                    device.idle_since = self.clock()
                control = "granted" if device.controller is conn else "not_granted"
                remaining = (None if device.idle_since is None or device.active else
                             max(0.0, LEASE_IDLE_SECONDS - (self.clock() - device.idle_since)))
                result = {"result": f"Connected to {port} at {device.baudrate} baud.",
                          "device_id": device.device_id, "port": port, "baudrate": device.baudrate,
                          "control": control, "lease_remaining": remaining}
            self._broadcast_device_event("joined", device)
            conn.send_result(rid, result)
            return
        device = self._device_for_call(conn, port)
        if device is None:
            conn.send_error(rid, "not_member", "join the device with connect(port, baudrate) first")
            return
        if tool == "disconnect":
            if set(args) != {"port"}:
                conn.send_error(rid, "bad_args", "disconnect accepts only port")
                return
            self._leave_device(conn, device)
            conn.send_result(rid, {"result": "Disconnected.", "device_id": device.device_id, "port": port})
            return
        if not device.online or device.closing:
            conn.send_error(rid, "not_connected", "the joined tty is no longer connected; reconnect or disconnect it")
            return
        modifying = tool not in ("read_output", "view_io")
        error = self._check_device_control(conn, device, modifying)
        if error:
            conn.send_error(rid, error["code"], error["message"])
            return
        if tool == "read_output":
            try:
                result = self._read_device_output(conn, device, args)
            except Exception as exc:
                self._end_device_operation(device, conn, modifying=False)
                conn.send_error(rid, "bad_args", str(exc))
                return
            self._end_device_operation(device, conn, modifying=False)
            conn.send_result(rid, {"result": result, "device_id": device.device_id,
                                   "port": port, "generation": device.generation})
            return
        if tool == "set_baudrate":
            if set(args) != {"port", "baudrate"}:
                conn.send_error(rid, "bad_args", "set_baudrate accepts only port and baudrate")
                self._end_device_operation(device, conn)
                return
            with device.settings_lock:
                try:
                    text = device.state.set_baudrate(args.get("baudrate"))
                except Exception as exc:
                    text = f"Error: {exc}"
                actual = getattr(device.state.transport, "baudrate", device.baudrate)
                if isinstance(actual, int) and actual > 0:
                    self._publish_settings(device, actual)
                committed_baudrate = device.baudrate
                committed_generation = device.generation
            self._end_device_operation(device, conn)
            if text.startswith("Error"):
                conn.send_error(rid, "baudrate_rejected", text, baudrate=committed_baudrate)
                return
            conn.send_result(rid, {"result": text, "device_id": device.device_id,
                                   "baudrate": committed_baudrate,
                                   "generation": committed_generation})
            return
        method_name = {"send_command": "send_command", "read_output": "read_output",
                       "clear_output": "clear_output", "set_prompt_pattern": "set_prompt_pattern",
                       "login": "login", "enter_bootloader": "enter_bootloader",
                       "upload_file": "upload_file", "download_file": "download_file",
                       "view_io": "view_io"}[tool]
        call_args = {k: v for k, v in args.items() if k != "port"}
        method = getattr(device.state, method_name)
        try:
            bound = inspect.signature(method).bind(**call_args)
        except TypeError as exc:
            self._end_device_operation(device, conn, modifying=modifying)
            conn.send_error(rid, "bad_args", f"{tool}: {exc}")
            return
        start_generation = device.generation
        try:
            text = method(*bound.args, **bound.kwargs)
        except Exception as exc:
            self.log("ERR", f"tool {tool} raised: {exc!r}")
            self._end_device_operation(device, conn, modifying=modifying)
            conn.send_error(rid, "internal", f"{tool} failed unexpectedly")
            return
        self._end_device_operation(device, conn, modifying=modifying)
        result = {"result": text if isinstance(text, str) else str(text),
                  "device_id": device.device_id, "port": port,
                  "generation": device.generation}
        if start_generation != device.generation:
            result["warning"] = "settings changed during this operation; output/outcome may be affected"
        try:
            encode_frame({"id": rid, "ok": True, "result": result})
        except ProtocolError:
            conn.send_error(rid, "result_too_large", "the result exceeds the frame limit")
            return
        conn.send_result(rid, result)

    def _publish_settings(self, device: Device, baudrate: int) -> None:
        with self._lock:
            if baudrate == device.baudrate:
                return
            device.baudrate = baudrate
            device.generation += 1
            members = list(device.members)
        for member in members:
            notice = {"event": "settings", "device_id": device.device_id,
                      "port": device.port, "baudrate": device.baudrate,
                      "generation": device.generation}
            member.send_settings_notice(notice)

    def _read_device_output(self, conn: Connection, device: Device, args: dict) -> str:
        timeout = args.get("timeout", 0)
        if (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)
                or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("timeout must be a finite non-negative number")
        deadline = time.monotonic() + min(float(timeout), 1800)
        encoding = args.get("encoding", "utf-8")
        if not isinstance(encoding, str):
            raise ValueError("encoding must be a string")
        try:
            codecs.lookup(encoding)
        except LookupError as exc:
            raise ValueError(f"unknown encoding: {exc}") from None
        max_lines = args.get("max_lines", 0)
        if isinstance(max_lines, bool) or not isinstance(max_lines, int) or max_lines < 0:
            raise ValueError("max_lines must be a non-negative integer")
        with self._drained:
            cursor = device.cursors.get(conn, device.rx_offset)
            while device.rx_offset <= cursor and timeout > 0 and not device.closing:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._drained.wait(remaining)
            chunks = []
            if cursor < device.output_start_offset:
                chunks.append(b"[output gap: earlier bytes were dropped]\\n")
            for seq, data in device.output:
                end = seq + len(data)
                if end <= cursor:
                    continue
                chunks.append(data[max(0, cursor - seq):])
            device.cursors[conn] = device.rx_offset
        text = b"".join(chunks).decode(encoding, errors="replace")
        if max_lines:
            text = "".join(text.splitlines(keepends=True)[-max_lines:])
        return text

    def human_input(self, conn: Connection, data: bytes, port: str | None = None) -> tuple[dict | None, dict | None]:
        if port is None:
            return None, {"code": "bad_args", "message": "port is required"}
        device = self._device_for_call(conn, port)
        # A human may write only after its own internal join, but requires no AI lease.
        if conn.role != "human" or device is None:
            return None, {"code": "not_member", "message": "join the device before writing"}
        with self._drained:
            if self._shutting_down:
                return None, {"code": "shutting_down", "message": "the daemon is shutting down"}
            if device.closing or conn not in device.members:
                return None, {"code": "not_member", "message": "join the device before writing"}
            device.active += 1
            device.in_flight += 1
        try:
            if not device.state.human_write(data):
                return None, {"code": "not_connected", "message": "no device is connected"}
            return {"written": len(data), "device_id": device.device_id}, None
        finally:
            self._end_device_operation(device, conn)


_FATAL_FRAME_CODES = frozenset({"frame_too_large", "unsupported_version"})
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
