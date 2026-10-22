#!/usr/bin/env python3
"""Generic serial transport MCP server (legacy: one device per MCP process).

This is the FastMCP adapter over ``serial_mcp_engine``. It registers the MCP
tools and keeps this process's own module-level ``state``. The serial engine
(transports, ``SerialState``, helpers) has no MCP dependency. The shared daemon
imports the engine, never this module, so it does not load FastMCP.

The AI opens the serial device and serves a session socket that speaks tio's
raw-byte --socket protocol, so the user can attach to the AI's session from
their own terminal (real tio via a socat pty bridge, or nc -UN).
"""

from __future__ import annotations

from fastmcp import FastMCP

# Explicit re-exports of the engine's public names, kept importable from here for
# existing callers and tests. Mutable configuration (SESSION_SOCKET_DIR, CONFIG_DIR,
# USER_PROFILES_PATH, CREDENTIALS_PATH) is owned by the engine and is not
# re-exported, so patching it on this module fails loudly instead of silently.
from serial_mcp_engine import (  # noqa: F401
    BUILTIN_BOOT_PROFILES,
    FILE_TRANSFER_CAPABILITIES,
    LINE_ENDINGS,
    SESSION_SOCKET_AUTO,
    DirectTransport,
    SerialState,
    SocketTransport,
)

mcp = FastMCP("GenericSerialTransport")

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
