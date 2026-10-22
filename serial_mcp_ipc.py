"""Shared daemon IPC primitives for serial-mcp (protocol v1).

This module is deliberately stdlib-only and must not import ``server`` (which
constructs device state and imports FastMCP). Frontends import it to discover,
start, and talk to the per-user daemon. See docs/daemon-ipc.md for the frozen
contract that this module implements.
"""

from __future__ import annotations

import base64
import fcntl
import json
import os
import queue
import socket
import stat
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

PROTOCOL_VERSION = 1

# Contract constants (docs/daemon-ipc.md section 11). Changing any of these is a
# protocol change.
MAX_FRAME_BYTES = 4 * 1024 * 1024
HELLO_TIMEOUT_SECONDS = 5
LEASE_IDLE_SECONDS = 60
GRACE_SECONDS = 30
DRAIN_TIMEOUT_SECONDS = 5
START_TIMEOUT_SECONDS = 10
SLOW_CONSUMER_TIMEOUT_SECONDS = 5
MAX_SUBSCRIBER_QUEUE_BYTES = 256 * 1024
MAX_OUTPUT_EVENT_BYTES = 16 * 1024
MAX_INFLIGHT_CALLS_PER_CONNECTION = 8
MAX_HUMAN_INPUT_BYTES = 64 * 1024
MAX_SPAWN_ATTEMPTS = 3
LOCK_PROBE_WAIT_SECONDS = 0.5  # how long a daemon waits for daemon.lock (starter probes hold it for microseconds)
START_GRACE_SECONDS = 2.0      # after a spawn, how long a starter waits for the child to take the lock

# AF_UNIX sun_path is 108 bytes including the terminating NUL on Linux.
SUN_PATH_MAX = 107


class ProtocolError(Exception):
    """A frame violated the contract. ``code`` is a contract error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class UnsafeRuntimeDir(Exception):
    """The runtime directory is not private to this user, or cannot hold a socket."""


class DaemonUnavailable(Exception):
    """The daemon could not be reached or started within the bounded budget."""


class DaemonError(Exception):
    """The daemon answered a request with ``ok: false``."""

    def __init__(self, code: str, message: str, error: dict | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.error = error or {}


# ---- framing ---------------------------------------------------------------


def encode_frame(obj: dict) -> bytes:
    """Serialize one frame: a JSON object with ``v`` set, terminated by LF."""
    body = dict(obj)
    body.setdefault("v", PROTOCOL_VERSION)
    line = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
    if len(line) > MAX_FRAME_BYTES:
        raise ProtocolError("frame_too_large", "frame exceeds MAX_FRAME_BYTES")
    return line


class FrameReader:
    """Bounded line reader over a connected stream socket.

    The buffer never grows past ``MAX_FRAME_BYTES`` without a newline; an
    overlong line raises ``frame_too_large`` so an abusive peer cannot exhaust
    memory. ``read_frame`` returns ``None`` on a clean EOF.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = bytearray()
        self._eof = False

    def read_frame(self) -> dict | None:
        while True:
            newline = self._buf.find(b"\n")
            if newline >= 0:
                if newline + 1 > MAX_FRAME_BYTES:
                    raise ProtocolError("frame_too_large", "frame exceeds MAX_FRAME_BYTES")
                line = bytes(self._buf[:newline])
                del self._buf[: newline + 1]
                return _decode_frame(line)
            if len(self._buf) >= MAX_FRAME_BYTES:
                raise ProtocolError("frame_too_large", "frame exceeds MAX_FRAME_BYTES")
            if self._eof:
                return None
            chunk = self._sock.recv(65536)
            if not chunk:
                self._eof = True
                if self._buf:
                    # A partial frame at EOF is a truncated request; drop it.
                    self._buf.clear()
                return None
            self._buf.extend(chunk)


def _decode_frame(line: bytes) -> dict:
    try:
        obj = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("bad_request", f"frame is not valid JSON: {exc.__class__.__name__}") from None
    if not isinstance(obj, dict):
        raise ProtocolError("bad_request", "frame must be a JSON object")
    if obj.get("v") != PROTOCOL_VERSION:
        raise ProtocolError(
            "unsupported_version",
            f"protocol version {obj.get('v')!r} is not supported; this peer speaks {PROTOCOL_VERSION}",
        )
    return obj


# ---- runtime directory -----------------------------------------------------


def runtime_dir() -> Path:
    """Directory holding the daemon socket and lock. Not created here."""
    override = os.environ.get("SERIAL_MCP_RUNTIME_DIR")
    if override:
        return Path(override)
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        return Path(xdg) / "serial-mcp"
    return Path(f"/tmp/serial-mcp-{os.getuid()}")


def socket_path(directory: Path | None = None) -> Path:
    path = (directory if directory is not None else runtime_dir()) / "daemon.sock"
    if len(os.fsencode(str(path))) > SUN_PATH_MAX:
        raise UnsafeRuntimeDir(f"socket path is too long for AF_UNIX: {path}")
    return path


def lock_path(directory: Path | None = None) -> Path:
    return (directory if directory is not None else runtime_dir()) / "daemon.lock"


def ensure_runtime_dir(directory: Path | None = None) -> Path:
    """Create the runtime directory privately, or verify an existing one.

    An existing directory is never repaired: a directory that others can read or
    write, that is a symlink, or that another user owns is refused, because
    accepting it would let someone else stand in for the daemon.
    """
    path = directory if directory is not None else runtime_dir()
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    except OSError as exc:
        raise UnsafeRuntimeDir(f"cannot create runtime directory {path}: {exc.strerror}") from None
    _verify_private_dir(path)
    return path


def _verify_private_dir(path: Path) -> None:
    try:
        st = os.lstat(path)
    except OSError as exc:
        raise UnsafeRuntimeDir(f"cannot inspect runtime directory {path}: {exc.strerror}") from None
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise UnsafeRuntimeDir(f"runtime path {path} is not a real directory")
    if st.st_uid != os.geteuid():
        raise UnsafeRuntimeDir(f"runtime directory {path} is owned by another user")
    if st.st_mode & 0o077:
        raise UnsafeRuntimeDir(f"runtime directory {path} is accessible to other users (mode {stat.S_IMODE(st.st_mode):o})")


# ---- client connection -----------------------------------------------------


class DaemonClient:
    """A connected, hello'd daemon session with request/response correlation.

    A background thread reads every incoming frame: responses wake the waiting
    request by ``id``, events are queued for ``next_event``. Requests may be
    issued from several threads concurrently, so a slow gated call never blocks
    a human input frame or a status query on the same connection.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._reader = FrameReader(sock)
        self._send_lock = threading.Lock()
        self._waiters: dict = {}
        self._waiters_lock = threading.Lock()
        self._events: "queue.Queue[dict]" = queue.Queue()
        self._next_id = 0
        self._closed = False
        self.client_id: str | None = None
        self._thread = threading.Thread(target=self._read_loop, name="daemon-client-reader", daemon=True)
        self._thread.start()

    @classmethod
    def connect(cls, path=None, timeout: float = START_TIMEOUT_SECONDS) -> "DaemonClient":
        """Connect to an already-running daemon. Does not start one."""
        target = str(path) if path is not None else str(socket_path())
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect(target)
        except OSError as exc:
            sock.close()
            raise DaemonUnavailable(f"no daemon at {target}: {exc.strerror or exc}") from None
        _verify_peer_uid(sock)
        sock.settimeout(None)
        return cls(sock)

    def hello(self, role: str, name: str, pid: int | None = None, client_version: str | None = None,
              timeout: float = 10.0) -> dict:
        fields = {"role": role, "name": name}
        if pid is not None:
            fields["pid"] = pid
        if client_version is not None:
            fields["client_version"] = client_version
        result = self.request("hello", timeout=timeout, **fields)
        self.client_id = result.get("client_id")
        return result

    def request(self, op: str, timeout: float = 60.0, **fields) -> dict:
        """Send one request and return its ``result``; raise DaemonError on failure."""
        with self._waiters_lock:
            if self._closed:
                raise DaemonUnavailable("connection to the daemon is closed")
            self._next_id += 1
            rid = self._next_id
            slot = _Waiter()
            self._waiters[rid] = slot
        frame = {"id": rid, "op": op}
        frame.update(fields)
        try:
            with self._send_lock:
                self._sock.sendall(encode_frame(frame))
        except OSError as exc:
            self._drop_waiter(rid)
            raise DaemonUnavailable(f"send failed: {exc.strerror or exc}") from None
        if not slot.event.wait(timeout):
            self._drop_waiter(rid)
            raise TimeoutError(f"no reply to {op!r} within {timeout}s")
        reply = slot.reply
        if reply is None:
            raise DaemonUnavailable("connection to the daemon closed before a reply")
        if not reply.get("ok"):
            error = reply.get("error") or {}
            raise DaemonError(error.get("code", "internal"), error.get("message", ""), error)
        return reply.get("result") or {}

    def call(self, tool: str, args: dict | None = None, timeout: float = 120.0) -> str:
        return self.request("call", timeout=timeout, tool=tool, args=args or {})["result"]

    def lease(self, action: str) -> dict:
        return self.request("lease", action=action)

    def status(self) -> dict:
        return self.request("status")

    def subscribe(self) -> dict:
        return self.request("subscribe")

    def unsubscribe(self) -> dict:
        return self.request("unsubscribe")

    def human_input(self, data: bytes) -> dict:
        return self.request("input", data=base64.b64encode(data).decode("ascii"))

    def next_event(self, timeout: float = 5.0) -> dict | None:
        try:
            event = self._events.get(timeout=timeout)
        except queue.Empty:
            return None
        return event

    def close(self) -> None:
        with self._waiters_lock:
            self._closed = True
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass

    def _drop_waiter(self, rid) -> None:
        with self._waiters_lock:
            self._waiters.pop(rid, None)

    def _read_loop(self) -> None:
        try:
            while True:
                frame = self._reader.read_frame()
                if frame is None:
                    break
                if "event" in frame:
                    self._events.put(frame)
                    continue
                with self._waiters_lock:
                    slot = self._waiters.pop(frame.get("id"), None)
                if slot is not None:
                    slot.reply = frame
                    slot.event.set()
        except (ProtocolError, OSError):
            pass
        finally:
            with self._waiters_lock:
                self._closed = True
                waiters = list(self._waiters.values())
                self._waiters.clear()
            for slot in waiters:
                slot.event.set()
            self._events.put({"event": "closed"})


class _Waiter:
    __slots__ = ("event", "reply")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.reply: dict | None = None


def _verify_peer_uid(sock: socket.socket) -> None:
    """Refuse a socket whose peer is not this user's process."""
    creds = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    _pid, uid, _gid = struct.unpack("3i", creds)
    if uid != os.geteuid():
        sock.close()
        raise DaemonUnavailable("daemon socket belongs to another user")


# ---- race-safe start -------------------------------------------------------

_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
_SPAWNED: list[subprocess.Popen] = []


def ensure_daemon(
    timeout: float = START_TIMEOUT_SECONDS,
    *,
    daemon_argv: list[str] | None = None,
) -> DaemonClient:
    """Connect to the per-user daemon, starting it if absent. Bounded and race-safe.

    Safety does not rest on this helper: the daemon itself takes the exclusive
    ``daemon.lock`` and refuses to serve or to touch the socket without it, so
    any number of starters, in any language, converge on one daemon. This helper
    only decides *when to spawn*: it spawns when the lock looks free, and polls
    for connectability otherwise, within ``timeout``. At most
    ``MAX_SPAWN_ATTEMPTS`` spawns are made.

    ``daemon_argv`` overrides the spawn command (tests).
    """
    directory = ensure_runtime_dir()
    path = directory / "daemon.sock"
    deadline = time.monotonic() + timeout
    spawns = 0
    last_spawn: float | None = None
    while True:
        remaining = deadline - time.monotonic()
        try:
            return DaemonClient.connect(path=path, timeout=max(0.1, min(remaining, 2.0)))
        except DaemonUnavailable:
            pass
        if time.monotonic() >= deadline:
            raise DaemonUnavailable(f"the daemon did not become ready within {timeout}s")
        in_cooldown = last_spawn is not None and time.monotonic() - last_spawn < START_GRACE_SECONDS
        if in_cooldown or not _lock_is_free(directory):
            # Our child (or another daemon) is still taking the lock and binding: poll.
            time.sleep(0.05)
            continue
        if spawns >= MAX_SPAWN_ATTEMPTS:
            raise DaemonUnavailable("the daemon failed to start after several attempts")
        _spawn_daemon(directory, daemon_argv)
        spawns += 1
        last_spawn = time.monotonic()


def _lock_is_free(directory: Path) -> bool:
    """Probe the daemon lock without keeping it. A racing daemon wins regardless."""
    fd = os.open(directory / "daemon.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    finally:
        os.close(fd)  # closing releases the probe lock
    return True


def _spawn_daemon(directory: Path, daemon_argv: list[str] | None) -> None:
    argv = list(daemon_argv) if daemon_argv else [sys.executable, "-m", "serial_mcp_daemon"]
    env = dict(os.environ)
    env["SERIAL_MCP_RUNTIME_DIR"] = str(directory)
    # Reap daemons this process spawned earlier that have since exited, so a
    # long-running client does not accumulate zombie children.
    _SPAWNED[:] = [child for child in _SPAWNED if child.poll() is None]
    _SPAWNED.append(subprocess.Popen(
        argv,
        cwd=_PACKAGE_DIR,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    ))
