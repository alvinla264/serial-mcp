"""Native multi-device human terminal for the shared serial-mcp daemon.

The UI is stdlib-only and talks to the daemon over protocol v2. Ctrl-C exits
locally; Ctrl-] leaves a device pane; Ctrl-Q exits the application. In a pane,
all other bytes are sent immediately to the device.
"""
from __future__ import annotations

import argparse
import base64
import binascii
import os
import select
import signal
import sys
import termios
import time
import tty

import serial_mcp_ipc as ipc

TERMINAL_VERSION = "0.3.0"
CLIENT_NAME = "human-terminal"
POLL_SECONDS = 0.1
MAX_SCREEN_ROWS = 80


def _write(fd: int, text: str) -> None:
    data = text.encode("utf-8", "replace")
    view = memoryview(data)
    attempts = 0
    while view:
        attempts += 1
        if attempts > 64:
            raise OSError("terminal write made insufficient progress")
        written = os.write(fd, view)
        if written <= 0:
            raise BrokenPipeError("terminal output is closed")
        view = view[written:]


def _write_bytes(fd: int, data: bytes) -> None:
    view = memoryview(data)
    attempts = 0
    while view:
        attempts += 1
        if attempts > 64:
            raise OSError("terminal write made insufficient progress")
        written = os.write(fd, view)
        if written <= 0:
            raise BrokenPipeError("terminal output is closed")
        view = view[written:]


def _baud_input(fd: int, err_fd: int) -> int | None:
    """Read a bounded line in raw mode; reject malformed/out-of-range baudrates."""
    _write(err_fd, "Baud rate (e.g. 115200): ")
    chars = bytearray()
    while len(chars) < 32:
        ready, _, _ = select.select([fd], [], [], 0.2)
        if not ready:
            continue
        chunk = os.read(fd, 32)
        if not chunk:
            return None
        for byte in chunk:
            if byte in (10, 13):
                _write(err_fd, "\r\n")
                try:
                    baud = int(chars.decode("ascii"))
                except (ValueError, UnicodeDecodeError):
                    _write(err_fd, "Invalid baud rate.\r\n")
                    return None
                if not 1 <= baud <= 4_000_000:
                    _write(err_fd, "Invalid baud rate; enter a value from 1 to 4000000.\r\n")
                    return None
                return baud
            if byte in (8, 127):
                if chars:
                    chars.pop()
            elif 48 <= byte <= 57:
                chars.append(byte)
    _write(err_fd, "Baud rate input too long.\r\n")
    return None


def _device_list(client: ipc.DaemonClient, fd: int, err_fd: int) -> int:
    """Render/select devices. Membership exists only while a terminal pane is open."""
    selected = 0
    known: list[dict] = []
    while True:
        try:
            status = client.status()
            mcp_clients = status.get("mcp_clients")
            if (not isinstance(mcp_clients, int) or isinstance(mcp_clients, bool)
                    or mcp_clients < 0):
                _write(err_fd, "\r\nserial-mcp-terminal: daemon does not provide MCP-only client counts; update the daemon\r\n")
                return 1
            ports = client.list_ports().get("result", [])
        except (ipc.DaemonError, ipc.DaemonUnavailable, TimeoutError) as exc:
            _write(err_fd, f"\r\nserial-mcp-terminal: daemon unavailable: {exc}\r\n")
            return 1
        devices = status.get("devices", [])
        by_port = {d.get("port"): d for d in devices}
        known = ([{"port": line.split(None, 1)[0]} for line in ports.splitlines()
                  if line.strip() and not line.startswith("No serial ports found")]
                 if isinstance(ports, str) else [])
        selected = min(selected, max(0, min(len(known), MAX_SCREEN_ROWS) - 1))
        _write(err_fd, "\x1b[H\x1b[2Jserial-mcp-terminal — device list (↑/↓ select, Enter join, q quit)\r\n")
        _write(err_fd, f"Registered MCP clients: {mcp_clients}\r\n\r\n")
        if not known:
            _write(err_fd, "No serial devices found. Waiting for devices…\r\n")
        for index, item in enumerate(known[:MAX_SCREEN_ROWS]):
            port = item.get("port") if isinstance(item, dict) else str(item)
            dev = by_port.get(port)
            state = "connected" if dev and dev.get("state") == "connected" else "available"
            baud = f"{dev.get('baudrate')} baud" if dev and dev.get("baudrate") else "baud unset"
            control = f"control {dev.get('control')}" if dev else "control available"
            marker = ">" if index == selected else " "
            _write(err_fd, f"{marker} {port}: {state}, {baud}, {control}\r\n")
        _write(err_fd, "\r\nCtrl-Q quits. Selecting a device opens its terminal pane.\r\n")
        ready, _, _ = select.select([fd], [], [], 1.0)
        if not ready:
            continue
        key = os.read(fd, 32)
        if not key:
            return 0
        if b"\x11" in key or b"q" in key:
            return 0
        if b"\x1b[A" in key or key == b"k":
            selected = max(0, selected - 1)
        elif b"\x1b[B" in key or key == b"j":
            selected = min(max(0, min(len(known), MAX_SCREEN_ROWS) - 1), selected + 1)
        elif b"\r" in key or b"\n" in key:
            if not known:
                continue
            if selected >= len(known) or selected >= MAX_SCREEN_ROWS:
                selected = max(0, min(len(known), MAX_SCREEN_ROWS) - 1)
                continue
            item = known[selected]
            port = item.get("port") if isinstance(item, dict) else str(item)
            current = by_port.get(port)
            baud = None
            if not current or current.get("state") != "connected":
                baud = _baud_input(fd, err_fd)
                if baud is None:
                    continue
            try:
                joined = client.join(port, baud)
            except (ipc.DaemonError, ipc.DaemonUnavailable, TimeoutError) as exc:
                _write(err_fd, f"serial-mcp-terminal: cannot join {port}: {exc}\r\n")
                continue
            try:
                client.subscribe(port)
            except (ipc.DaemonError, ipc.DaemonUnavailable, TimeoutError) as exc:
                try:
                    client.leave(port)
                except (ipc.DaemonError, ipc.DaemonUnavailable, TimeoutError):
                    pass
                _write(err_fd, f"serial-mcp-terminal: joined {port} but could not subscribe to output: {exc}\r\n")
                continue
            result = _terminal_pane(client, port, joined.get("baudrate"), fd, err_fd)
            try:
                client.leave(port)
                client.unsubscribe(port)
            except (ipc.DaemonError, ipc.DaemonUnavailable, TimeoutError):
                pass
            if result == 2:  # Ctrl-Q from the pane quits the whole application.
                return 0
            if result:
                return result


def _terminal_pane(client: ipc.DaemonClient, port: str, baud: int | None,
                   fd: int, err_fd: int) -> int:
    _write(err_fd, f"\x1b[H\x1b[2J{port} — {baud} baud | Ctrl-] returns to device list; Ctrl-Q quits\r\n")
    while True:
        ready, _, _ = select.select([fd], [], [], POLL_SECONDS)
        if ready:
            data = os.read(fd, 1024)
            if not data:
                return 0
            if 0x11 in data:
                return 2
            if 0x1d in data:
                return 0
            try:
                client.human_input(data, port)
            except ipc.DaemonError as exc:
                if exc.code == "not_connected":
                    _write(err_fd, f"\r\n[device disconnected: {exc}]\r\n")
                    return 0
                _write(err_fd, f"\r\nserial-mcp-terminal: input failed: {exc}\r\n")
                return 1
            except (ipc.DaemonUnavailable, TimeoutError) as exc:
                _write(err_fd, f"\r\nserial-mcp-terminal: input failed: {exc}\r\n")
                return 1
        while True:
            event = client.next_event(timeout=0)
            if event is None:
                break
            if not isinstance(event, dict) or not isinstance(event.get("event"), str):
                _write(err_fd, "\r\n[ignored malformed daemon event]\r\n")
                continue
            if event.get("event") == "output" and event.get("port") == port:
                try:
                    _write_bytes(sys.stdout.fileno(), base64.b64decode(event["data"], validate=True))
                except (OSError, binascii.Error):
                    _write(err_fd, "\r\nserial-mcp-terminal: malformed device output or closed terminal\r\n")
                    return 1
            elif event.get("event") == "device" and event.get("port") == port and event.get("state") == "disconnected":
                _write(err_fd, "\r\n[device disconnected; returned to device list]\r\n")
                return 0
            elif event.get("event") == "settings" and event.get("port") == port:
                baud = event.get("baudrate", baud)
                _write(err_fd, f"\r\n[baud rate changed to {baud}; concurrent output may be affected]\r\n")
            elif event.get("event") == "settings_gap" and event.get("port") == port:
                try:
                    devices = client.status().get("devices", [])
                    current = next((item.get("baudrate") for item in devices
                                    if item.get("port") == port), None)
                    if isinstance(current, int) and not isinstance(current, bool):
                        baud = current
                        notice = f"settings notices lost; current baud is {baud}"
                    else:
                        notice = "settings notices lost; current baud is unavailable"
                except (ipc.DaemonError, ipc.DaemonUnavailable, TimeoutError):
                    notice = "settings notices lost; current baud could not be refreshed"
                _write(err_fd, f"\r\n[{notice}]\r\n")
            elif event.get("event") == "output_gap" and event.get("port") == port:
                _write(err_fd, f"\r\n[output gap: {event.get('dropped_bytes', 0)} bytes dropped]\r\n")
            elif event.get("event") == "event_gap":
                _write(err_fd, f"\r\n[terminal event queue overflow: {event.get('dropped_events', 0)} events lost; output or settings may be incomplete]\r\n")
            elif event.get("event") in ("closed", "shutdown"):
                _write(err_fd, "\r\nserial-mcp-terminal: daemon connection closed\r\n")
                return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="serial-mcp-terminal", description="Multi-device serial TUI")
    parser.add_argument("--version", action="version", version=f"serial-mcp-terminal {TERMINAL_VERSION}")
    parser.parse_args(argv)
    if not os.isatty(0):
        print("serial-mcp-terminal: standard input is not a terminal", file=sys.stderr)
        return 2
    try:
        client = ipc.ensure_daemon()
        hello = client.hello(role="human", name=CLIENT_NAME, pid=os.getpid(), client_version=TERMINAL_VERSION)
        if hello.get("protocol") != ipc.PROTOCOL_VERSION:
            raise RuntimeError(f"incompatible daemon protocol {hello.get('protocol')!r}; expected v{ipc.PROTOCOL_VERSION}")
    except (ipc.DaemonError, ipc.DaemonUnavailable, ipc.UnsafeRuntimeDir, OSError, RuntimeError) as exc:
        print(f"serial-mcp-terminal: cannot connect to daemon: {exc}", file=sys.stderr)
        return 1
    fd = sys.stdin.fileno()
    settings = termios.tcgetattr(fd)
    old_handlers = {}
    def terminate(signum, _frame):
        raise KeyboardInterrupt
    for name in ("SIGTERM", "SIGHUP", "SIGINT"):
        sig = getattr(signal, name)
        old_handlers[sig] = signal.signal(sig, terminate)
    try:
        tty.setraw(fd, termios.TCSADRAIN)
        return _device_list(client, fd, sys.stderr.fileno())
    except KeyboardInterrupt:
        return 128 + signal.SIGINT
    except (BrokenPipeError, OSError):
        return 1
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, settings)
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        try:
            client.request("bye", timeout=2)
        except Exception:
            pass
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
