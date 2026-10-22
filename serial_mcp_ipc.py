"""Shared daemon IPC primitives for serial-mcp (protocol v1).

This module is deliberately stdlib-only and must not import ``server`` (which
constructs device state and imports FastMCP). Frontends import it to discover,
start, and talk to the per-user daemon. See docs/daemon-ipc.md for the frozen
contract that this module implements.
"""

from __future__ import annotations

import errno
import json
import os
import socket
import stat
import struct
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

# AF_UNIX sun_path is 108 bytes including the terminating NUL on Linux.
_SUN_PATH_MAX = 107


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


def socket_path() -> Path:
    path = runtime_dir() / "daemon.sock"
    if len(os.fsencode(str(path))) > _SUN_PATH_MAX:
        raise UnsafeRuntimeDir(f"socket path is too long for AF_UNIX: {path}")
    return path


def lock_path() -> Path:
    return runtime_dir() / "daemon.lock"


def ensure_runtime_dir() -> Path:
    """Create the runtime directory privately, or verify an existing one.

    An existing directory is never repaired: a directory that others can read or
    write, that is a symlink, or that another user owns is refused, because
    accepting it would let someone else stand in for the daemon.
    """
    path = runtime_dir()
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
