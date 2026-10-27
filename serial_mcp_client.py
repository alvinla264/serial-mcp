#!/usr/bin/env python3
"""Thin stdio MCP frontend for the shared serial daemon (protocol v1).

Each coding-agent instance starts one of these. It discovers or race-safely
starts the per-user daemon (``serial_mcp_ipc.ensure_daemon``), registers as an
``mcp`` client, and forwards every tool call to the daemon unchanged. The daemon
owns the serial device, its reader, its buffers, the session socket and the
control lease. This module holds no serial state and imports no device code, so
starting it never opens a port, a log, or a session.

Tool names, parameters, defaults and docstrings mirror the legacy in-process
server (``server.py``). ``test_mcp_client.py`` compares them with the legacy tool
list so that the two cannot drift apart unnoticed. See docs/mcp-frontend.md.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

import serial_mcp_ipc as ipc

CLIENT_VERSION = "0.2.0"
FRONTEND_PROTOCOL_VERSION = 1  # this frontend is retained only as a v1 compatibility oracle
CALL_WAIT_SECONDS = 1860.0  # longest tool-level wait (1800 s transfer deadline) plus margin
HELLO_ATTEMPTS = 3          # bounded retries when the daemon is shutting down during hello
MAX_NOTICES = 20
MAX_CLIENT_NAME = 64        # daemon limit for the hello name
SESSION_SOCKET_AUTO = "auto"  # same default as server.py

# Mirrors TOOL_TABLE in serial_mcp_daemon.py. The daemon enforces the gate; the
# frontend repeats it only to refuse stale requests after a reconnect locally.
GATED_TOOLS = frozenset({
    "connect", "attach", "disconnect", "send_command", "read_output", "clear_output",
    "set_prompt_pattern", "login", "enter_bootloader", "upload_file", "download_file",
})

LOST_CONTROL_WARNING = (
    "control was lost; device state may have changed since you last used it. "
    "Call lease_acquire explicitly before using the device again. Requests are never retried automatically."
)

mcp = FastMCP("GenericSerialTransport")


def _client_name() -> str:
    raw = os.environ.get("SERIAL_MCP_CLIENT_NAME") or f"mcp-{os.getpid()}"
    return raw[:MAX_CLIENT_NAME]


class Frontend:
    """One daemon connection for this MCP process, plus the local notices it owes the agent."""

    def __init__(self) -> None:
        self._connect_lock = threading.Lock()  # serialises (re)connection
        self._state_lock = threading.Lock()    # guards the fields below
        self._client: ipc.DaemonClient | None = None
        self._notices: list[str] = []
        self._held_control = False      # this frontend has held control on some connection
        self._reacquire_local = False   # the connection ended while we held control

    # -- connection -------------------------------------------------------

    def start_in_background(self) -> None:
        """Register at startup so an idle agent session keeps the daemon alive."""
        def run() -> None:
            try:
                self._ensure_connected()
            except ToolError:
                pass  # reported again, with the same text, on the first tool call
        threading.Thread(target=run, name="serial-mcp-register", daemon=True).start()

    def _ensure_connected(self) -> ipc.DaemonClient:
        with self._connect_lock:
            with self._state_lock:
                client = self._client
            if client is not None:
                if self._drain_events(client):
                    return client
                self._connection_ended(client)  # the daemon closed it since the last request
            client = self._open_session()
            with self._state_lock:
                self._client = client
            return client

    def _open_session(self) -> ipc.DaemonClient:
        for _attempt in range(HELLO_ATTEMPTS):
            try:
                client = ipc.ensure_daemon()
            except ipc.UnsafeRuntimeDir as exc:
                raise ToolError(f"serial-mcp cannot use its runtime directory: {exc}") from None
            except ipc.DaemonUnavailable as exc:
                raise ToolError(f"serial-mcp daemon is unavailable: {exc}. {_runtime_hint()}") from None
            except OSError as exc:
                raise ToolError(f"serial-mcp cannot start the daemon: {exc}. {_runtime_hint()}") from None
            try:
                result = client.hello(role="mcp", name=_client_name(), pid=os.getpid(),
                                      client_version=CLIENT_VERSION)
            except ipc.DaemonError as exc:
                client.close()
                if exc.code == "shutting_down":
                    time.sleep(0.2)  # the old daemon is draining; ensure_daemon starts a new one
                    continue
                raise ToolError(f"serial-mcp daemon refused this client: {exc.code}: {exc.message}") from None
            except (ipc.DaemonUnavailable, TimeoutError) as exc:
                client.close()
                raise ToolError(f"serial-mcp daemon handshake failed: {exc}. {_runtime_hint()}") from None
            if result.get("protocol") != FRONTEND_PROTOCOL_VERSION:
                client.close()
                raise ToolError(
                    f"serial-mcp daemon speaks protocol {result.get('protocol')!r}; this frontend "
                    f"speaks {FRONTEND_PROTOCOL_VERSION}; this daemon contract is breaking. "
                    f"Migrate the frontend before using it with this daemon. {_runtime_hint()}"
                )
            return client
        raise ToolError("the serial-mcp daemon kept shutting down during registration; try again")

    def _drain_events(self, client: ipc.DaemonClient) -> bool:
        """Apply queued daemon events. Events precede the response that follows them on the
        socket, so draining after a response is deterministic. Returns False once closed."""
        alive = True
        while True:
            event = client.next_event(timeout=0)
            if event is None:
                return alive
            name = event.get("event")
            if name == "closed":
                alive = False
            elif name == "lease_lost":
                self._add_notice(event.get("warning") or LOST_CONTROL_WARNING)
            elif name == "shutdown":
                self._add_notice(f"the serial-mcp daemon is shutting down ({event.get('reason')}); "
                                 "the next tool call starts a new one")

    def _connection_ended(self, client: ipc.DaemonClient) -> None:
        with self._state_lock:
            if self._client is client:
                self._client = None
                if self._held_control:
                    # A new connection is a new client to the daemon, which would grant
                    # control again automatically. Refuse until the agent acquires explicitly.
                    self._reacquire_local = True
                    self._held_control = False
                    self._add_notice_locked(LOST_CONTROL_WARNING)
        client.close()

    # -- notices ----------------------------------------------------------

    def _add_notice(self, text: str) -> None:
        with self._state_lock:
            self._add_notice_locked(text)

    def _add_notice_locked(self, text: str) -> None:
        if text not in self._notices and len(self._notices) < MAX_NOTICES:
            self._notices.append(text)

    def _take_notices(self) -> list[str]:
        with self._state_lock:
            notices, self._notices = self._notices, []
        return notices

    def _prefix_notices(self, text: str) -> str:
        notices = self._take_notices()
        if not notices:
            return text
        return "".join(f"NOTICE: {n}\n" for n in notices) + "\n" + text

    # -- requests ---------------------------------------------------------

    def call_tool(self, tool: str, args: dict) -> str:
        """Forward one tool call. A call is never retried: a lost reply means the outcome is unknown."""
        gated = tool in GATED_TOOLS
        client = self._ensure_connected()
        with self._state_lock:
            refuse = gated and self._reacquire_local
        if refuse:
            raise ToolError(f"{tool} refused: lease_lost. {LOST_CONTROL_WARNING}")
        try:
            text = client.call(tool, args, timeout=CALL_WAIT_SECONDS)
        except ipc.DaemonError as exc:
            self._drain_events(client)
            raise ToolError(self._refusal_text(tool, exc)) from None
        except ipc.DaemonUnavailable as exc:
            self._connection_ended(client)
            raise ToolError(
                f"the connection to the serial-mcp daemon ended while {tool} was running ({exc}). "
                "The device may or may not have acted on it. The call was not retried; check the device "
                "state before calling again."
            ) from None
        except TimeoutError:
            self._connection_ended(client)
            raise ToolError(
                f"no reply to {tool} within {CALL_WAIT_SECONDS:.0f}s. The outcome is unknown and the "
                "call was not retried; check the device state before calling again."
            ) from None
        alive = self._drain_events(client)
        if gated:
            with self._state_lock:
                self._held_control = True
        if not alive:
            self._connection_ended(client)
        return self._prefix_notices(text)

    def lease(self, action: str) -> str:
        client = self._ensure_connected()
        try:
            view = client.lease(action)
        except ipc.DaemonError as exc:
            self._drain_events(client)
            raise ToolError(self._refusal_text(f"lease {action}", exc)) from None
        except ipc.DaemonUnavailable as exc:
            self._connection_ended(client)
            raise ToolError(f"the connection to the serial-mcp daemon ended during lease {action} ({exc})") from None
        alive = self._drain_events(client)
        with self._state_lock:
            lost_with_connection = self._reacquire_local
            if action == "acquire":
                self._reacquire_local = False
                self._held_control = True
            elif action == "release":
                # Released control is never resumed silently, even on a later connection.
                self._held_control = False
                self._reacquire_local = True
        if action == "acquire" and lost_with_connection and "warning" not in view:
            # The daemon cannot know about a lease held by a previous daemon or connection.
            view = dict(view, warning=LOST_CONTROL_WARNING)
        if not alive:
            self._connection_ended(client)
        notices = self._take_notices()
        if notices:
            view = dict(view, notices=notices)  # keep the lease result parseable JSON
        return json.dumps(view, sort_keys=True)

    def _refusal_text(self, what: str, exc: ipc.DaemonError) -> str:
        text = f"{what} was refused by the serial-mcp daemon ({exc.code}): {exc.message}"
        lease = exc.error.get("lease")
        if lease is not None:
            text += f" Lease: {json.dumps(lease, sort_keys=True)}"
        if exc.code == "lease_lost":
            text += f" {LOST_CONTROL_WARNING}"
        return self._prefix_notices(text)

    def close(self) -> None:
        """Say goodbye and close. A killed or crashed frontend is cleaned up by the daemon."""
        with self._connect_lock:
            with self._state_lock:
                client, self._client = self._client, None
            if client is not None:
                try:
                    client.request("bye", timeout=2)
                except Exception:
                    pass
                client.close()


def _runtime_hint() -> str:
    directory = ipc.runtime_dir()
    return f"Runtime directory: {directory} (daemon log: {directory / 'daemon.log'})."


FRONTEND = Frontend()


async def _forward(tool: str, args: dict) -> str:
    return await asyncio.to_thread(FRONTEND.call_tool, tool, args)


# ---- explicit AI control (additive; the daemon's lease op) ------------------


@mcp.tool()
async def lease_acquire() -> str:
    """Acquire AI control of the serial device explicitly, as JSON.

    Gated tools acquire control automatically when it is free. After control is
    lost (idle expiry, release, or a dropped connection) you must call this before
    the next gated tool. A warning in the result means device state may have changed
    since you last used it. Acquiring while another client holds control returns busy.
    """
    return await asyncio.to_thread(FRONTEND.lease, "acquire")


@mcp.tool()
async def lease_release() -> str:
    """Release AI control so another client can use the device, as JSON.

    Release takes effect once any in-flight operation finishes. After releasing you
    must call lease_acquire before gated tools work again.
    """
    return await asyncio.to_thread(FRONTEND.lease, "release")


@mcp.tool()
async def lease_status() -> str:
    """Show who holds AI control and when idle control expires, as JSON. Never renews the lease."""
    return await asyncio.to_thread(FRONTEND.lease, "status")


# ---- mirrored tools ----------------------------------------------------------

@mcp.tool()
async def connect(
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
    return await _forward("connect", {"port": port, "baudrate": baudrate, "timeout": timeout, "prompt_regex": prompt_regex, "session_socket": session_socket, "attach_socket": attach_socket})


@mcp.tool()
async def file_transfer_capabilities() -> str:
    """Describe implemented transfer methods, limits, requirements, and safety boundaries.

    Linux requires a POSIX-like shell with `cat`, `base64 -d`, and a working
    `sha256sum` or OpenSSL `dgst -sha256` command.
    U-Boot upload requires YMODEM-enabled `loady`; download uses `md.b` and a
    user-supplied prompt regex. Direct-owned sessions announce/gate their own
    socket/pty writers and count dropped input. Attach-mode transfers require explicit
    confirmation that all other tio clients will remain idle; the raw tio socket
    has no lock/arbitration protocol, so this cannot be enforced remotely.
    """
    return await _forward("file_transfer_capabilities", {})


@mcp.tool()
async def upload_file(
    local_path: str,
    target_path: str,
    target: str = 'linux',
    address: str | None = None,
    overwrite: bool = False,
    timeout: float = 300.0,
    max_bytes: int = 8388608,
    prompt_regex: str | None = None,
    confirm_shared_quiescence: bool = False,
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
    return await _forward("upload_file", {"local_path": local_path, "target_path": target_path, "target": target, "address": address, "overwrite": overwrite, "timeout": timeout, "max_bytes": max_bytes, "prompt_regex": prompt_regex, "confirm_shared_quiescence": confirm_shared_quiescence})


@mcp.tool()
async def download_file(
    local_path: str,
    target: str = 'linux',
    target_path: str | None = None,
    address: str | None = None,
    length: int | None = None,
    overwrite: bool = False,
    timeout: float = 300.0,
    max_bytes: int = 8388608,
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
    return await _forward("download_file", {"local_path": local_path, "target": target, "target_path": target_path, "address": address, "length": length, "overwrite": overwrite, "timeout": timeout, "max_bytes": max_bytes, "prompt_regex": prompt_regex, "confirm_shared_quiescence": confirm_shared_quiescence})


@mcp.tool()
async def attach(
    attach_socket: str | None = None,
    prompt_regex: str | None = None,
) -> str:
    """Join a session the user is already serving (e.g. their tio session).

    Convenience wrapper for connect(attach_socket=...) that defaults the socket
    path to /tmp/serial-mcp-<dev>.sock for a known device, or auto-detects the
    live session socket when exactly one exists.

    Args:
        attach_socket: Session socket to join. Defaults to auto-detection.
        prompt_regex: Optional default regex terminator for send_command.
    """
    return await _forward("attach", {"attach_socket": attach_socket, "prompt_regex": prompt_regex})


@mcp.tool()
async def disconnect() -> str:
    """Close the active serial connection, stop the reader, and drop session clients."""
    return await _forward("disconnect", {})


@mcp.tool()
async def send_command(
    text: str,
    line_ending: str = 'lf',
    timeout: float = 5.0,
    idle_timeout: float = 0.2,
    terminator_regex: str | None = None,
    encoding: str = 'utf-8',
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
    return await _forward("send_command", {"text": text, "line_ending": line_ending, "timeout": timeout, "idle_timeout": idle_timeout, "terminator_regex": terminator_regex, "encoding": encoding, "max_lines": max_lines})


@mcp.tool()
async def read_output(
    timeout: float = 0.0,
    idle_timeout: float = 0.2,
    encoding: str = 'utf-8',
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
    return await _forward("read_output", {"timeout": timeout, "idle_timeout": idle_timeout, "encoding": encoding, "max_lines": max_lines})


@mcp.tool()
async def clear_output() -> str:
    """Discard all currently buffered serial output."""
    return await _forward("clear_output", {})


@mcp.tool()
async def set_prompt_pattern(
    regex_pattern: str | None = None,
) -> str:
    """Set or clear the default regex terminator used by send_command."""
    return await _forward("set_prompt_pattern", {"regex_pattern": regex_pattern})


@mcp.tool()
async def list_boot_profiles() -> str:
    """List known bootloader-entry profiles (builtin + user-defined).

    User profiles live in ~/.config/serial-mcp/profiles.json; credentials are
    stored separately in ~/.config/serial-mcp/credentials.json (mode 0600) and
    are never displayed.
    """
    return await _forward("list_boot_profiles", {})


@mcp.tool()
async def set_boot_credentials(
    profile_key: str,
    username: str,
    password: str,
) -> str:
    """Store device login credentials locally for use by enter_bootloader.

    Credentials are written to ~/.config/serial-mcp/credentials.json with mode
    0600 (outside any repo). They are redacted from logs and view_io transcripts.

    Args:
        profile_key: Key that profiles reference via login.password_ref (or the
            profile name itself).
        username: Login username.
        password: Login password (stored locally only; redacted in logs).
    """
    return await _forward("set_boot_credentials", {"profile_key": profile_key, "username": username, "password": password})


@mcp.tool()
async def login(
    profile: str | None = None,
    username: str | None = None,
    password: str | None = None,
    login_mode: str = 'auto',
    overall_timeout: float = 20.0,
    encoding: str = 'utf-8',
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
    return await _forward("login", {"profile": profile, "username": username, "password": password, "login_mode": login_mode, "overall_timeout": overall_timeout, "encoding": encoding})


@mcp.tool()
async def enter_bootloader(
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
    login_mode: str = 'auto',
    overall_timeout: float = 120.0,
    encoding: str = 'utf-8',
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
    return await _forward("enter_bootloader", {"profile": profile, "interrupt_keys": interrupt_keys, "success_regex": success_regex, "abort_regex": abort_regex, "reboot": reboot, "reboot_command": reboot_command, "delay": delay, "interval": interval, "max_attempts": max_attempts, "login_username": login_username, "login_password": login_password, "login_mode": login_mode, "overall_timeout": overall_timeout, "encoding": encoding})


@mcp.tool()
async def view_io(
    lines: int = 50,
    direction: str = 'all',
    hex_mode: bool = False,
    encoding: str = 'utf-8',
) -> str:
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
    return await _forward("view_io", {"lines": lines, "direction": direction, "hex_mode": hex_mode, "encoding": encoding})


@mcp.tool()
async def list_serial_ports() -> str:
    """List available serial ports with descriptions (like tio --list)."""
    return await _forward("list_serial_ports", {})


@mcp.tool()
async def tio_info() -> str:
    """Show connection status, the session socket path, and attach instructions."""
    return await _forward("tio_info", {})


# ---- entry point -------------------------------------------------------------


def main() -> None:
    """Entry point for the serial-mcp console script: a stdio MCP client of the shared daemon."""
    FRONTEND.start_in_background()
    try:
        mcp.run()
    finally:
        FRONTEND.close()


if __name__ == "__main__":
    main()
