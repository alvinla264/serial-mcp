"""Native human serial terminal for the shared serial-mcp daemon.

``serial-mcp-terminal`` is a thin interactive console: it attaches to the per-user
daemon as a ``human`` client, shows the device's live output, and forwards
keystrokes to the device immediately. It never opens the serial port itself, it
holds no AI control lease, and it does not wait behind AI operations. Ctrl-] exits
it locally; every other key, including Ctrl-C and Ctrl-Z, goes to the device.

Diagnostics go to standard error only and are bracketed by the program name, so
the device stream on standard output is never altered. See the README section
"Human terminal" for usage, the exit key and the concurrent-input risk.

This module is stdlib-only and must not import ``server`` (which loads FastMCP and
pyserial). It depends only on the frozen IPC helpers in ``serial_mcp_ipc``.
"""

from __future__ import annotations

import argparse
import base64
import os
import select
import signal
import sys
import termios
import threading
import tty

import serial_mcp_ipc as ipc

TERMINAL_VERSION = "0.2.0"
CLIENT_NAME = "human-terminal"
EXIT_KEY = 0x1D  # Ctrl-]: handled locally, never forwarded to the device
INPUT_TIMEOUT_SECONDS = 10.0
CLOSE_TIMEOUT_SECONDS = 2.0
PUMP_JOIN_SECONDS = 1.0
POLL_SECONDS = 0.2


class _Terminated(BaseException):
    """A catchable termination signal. It unwinds through ``finally`` so the tty is restored."""

    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


class Console:
    """One interactive session: an output pump, a keystroke forwarder, and the tty restore."""

    def __init__(self, client: ipc.DaemonClient, in_fd: int, out_fd: int, err_fd: int) -> None:
        self._client = client
        self._in = in_fd
        self._out = out_fd
        self._err = err_fd
        self._io_lock = threading.Lock()
        self._stop = threading.Event()
        self._cleaning = False
        self._exit_code = 0
        self._farewell = "disconnected from the daemon"
        self._raw = False
        self._device_missing_reported = False
        self._pump: threading.Thread | None = None

    # -- diagnostics (standard error only) ---------------------------------

    def notice(self, text: str) -> None:
        """Print a bracketed status line. Failures here must never end the session."""
        eol = "\r\n" if self._raw else "\n"
        try:
            self._write(self._err, f"serial-mcp-terminal: {text}{eol}".encode("utf-8", "replace"))
        except OSError:
            pass

    def _write(self, fd: int, data: bytes) -> None:
        with self._io_lock:
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view):]

    def _end(self, code: int, farewell: str) -> None:
        """Record why the session ends. The first reason wins."""
        if not self._stop.is_set():
            self._exit_code = code
            self._farewell = farewell
        self._stop.set()

    # -- session -------------------------------------------------------------

    def run(self, banner: str) -> int:
        settings = termios.tcgetattr(self._in)
        restore_handlers = self._install_signal_handlers()
        try:
            tty.setraw(self._in, termios.TCSADRAIN)
            self._raw = True
            self._pump = threading.Thread(target=self._pump_output, name="terminal-output", daemon=True)
            self._pump.start()
            self.notice(banner)
            return self._forward_input()
        except _Terminated as signal_exc:
            self._end(128 + signal_exc.signum, f"terminated by signal {signal_exc.signum}")
            return self._exit_code
        finally:
            # Cleanup comes first and must not be interrupted: late signals are ignored.
            self._cleaning = True
            self._stop.set()
            self._restore_tty(settings)
            if self._pump is not None:
                self._pump.join(PUMP_JOIN_SECONDS)
            restore_handlers()
            self._close_client()
            self.notice(self._farewell)

    def _install_signal_handlers(self):
        def raise_terminated(signum, _frame):
            if not self._cleaning:
                raise _Terminated(signum)

        previous = {}
        for name in ("SIGTERM", "SIGHUP", "SIGINT"):
            signum = getattr(signal, name)
            previous[signum] = signal.signal(signum, raise_terminated)

        def restore() -> None:
            for signum, handler in previous.items():
                signal.signal(signum, handler)

        return restore

    def _restore_tty(self, settings) -> None:
        self._raw = False
        try:
            termios.tcsetattr(self._in, termios.TCSADRAIN, settings)
        except termios.error:
            pass

    def _close_client(self) -> None:
        try:
            self._client.request("bye", timeout=CLOSE_TIMEOUT_SECONDS)
        except Exception:
            pass  # the daemon may already be gone; closing our end is what matters
        self._client.close()

    def _pump_output(self) -> None:
        while not self._stop.is_set():
            event = self._client.next_event(timeout=POLL_SECONDS)
            if event is None:
                continue
            try:
                self._handle_event(event)
            except OSError as exc:
                self._end(1, f"the terminal stopped accepting output ({exc.strerror or exc}); "
                             "disconnected from the daemon")
            except Exception as exc:  # a malformed event must not die silently
                self._end(1, f"unexpected event from the daemon ({exc!r}); disconnected")

    def _handle_event(self, event: dict) -> None:
        name = event.get("event")
        if name == "output":
            self._write(self._out, base64.b64decode(event["data"], validate=True))
        elif name == "output_gap":
            self.notice(f"{event.get('dropped_bytes', 0)} bytes of output were dropped "
                        "(this terminal fell behind)")
        elif name == "device":
            if event.get("state") == "connected":
                self._device_missing_reported = False
                self.notice(f"device connected: {event.get('label')}")
            else:
                self.notice("device disconnected")
        elif name == "shutdown":
            self._end(1, f"the daemon is shutting down ({event.get('reason')}); this terminal closed")
        elif name == "closed":
            self._end(1, "lost the connection to the daemon; this terminal closed")

    def _forward_input(self) -> int:
        while not self._stop.is_set():
            try:
                ready, _, _ = select.select([self._in], [], [], POLL_SECONDS)
                if not ready:
                    continue
                data = os.read(self._in, 1024)
            except OSError as exc:
                self._end(1, f"the terminal went away ({exc.strerror or exc}); disconnected from the daemon")
                break
            if not data:
                self._end(0, "the terminal input closed; disconnected from the daemon")
                break
            exit_at = data.find(bytes([EXIT_KEY]))
            payload = data if exit_at < 0 else data[:exit_at]
            if payload and not self._send(payload):
                break
            if exit_at >= 0:
                self._end(0, "disconnected from the daemon (Ctrl-] pressed)")
                break
        return self._exit_code

    def _send(self, payload: bytes) -> bool:
        """Forward keystrokes now. Returns False when the session must end."""
        try:
            self._client.request("input", timeout=INPUT_TIMEOUT_SECONDS,
                                 data=base64.b64encode(payload).decode("ascii"))
        except ipc.DaemonError as exc:
            if exc.code == "not_connected":
                if not self._device_missing_reported:  # say it once per episode, not per key
                    self._device_missing_reported = True
                    self.notice("not sent: no device is connected; keys are dropped until one is")
            elif exc.code == "shutting_down":
                self._end(1, "the daemon is shutting down; this terminal closed")
                return False
            else:
                self.notice(f"not sent: {exc.message}")
        except (ipc.DaemonUnavailable, TimeoutError) as exc:
            self._end(1, f"lost the connection to the daemon ({exc}); this terminal closed")
            return False
        return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="serial-mcp-terminal",
        description="Interactive serial console on the shared serial-mcp daemon. "
                    "Press Ctrl-] to exit; other keys go to the device.",
    )
    parser.add_argument("--version", action="version", version=f"serial-mcp-terminal {TERMINAL_VERSION}")
    parser.parse_args(argv)

    if not os.isatty(0):
        print("serial-mcp-terminal: standard input is not a terminal; run it from a terminal.", file=sys.stderr)
        return 2

    try:
        client = ipc.ensure_daemon()
    except (ipc.DaemonUnavailable, ipc.UnsafeRuntimeDir, OSError) as exc:
        print(f"serial-mcp-terminal: cannot reach the serial-mcp daemon: {exc}", file=sys.stderr)
        return 1
    try:
        hello = client.hello(role="human", name=CLIENT_NAME, pid=os.getpid(), client_version=TERMINAL_VERSION)
        client.subscribe()
        device = client.status().get("device")
    except (ipc.DaemonError, ipc.DaemonUnavailable, TimeoutError) as exc:
        client.close()
        print(f"serial-mcp-terminal: the daemon refused this connection: {exc}", file=sys.stderr)
        return 1

    where = (f"device: {device}" if device
             else "no device is connected yet; output appears once an MCP client connects one")
    banner = (f"connected to the daemon (client {hello.get('client_id')}); {where}. "
              "Ctrl-] exits; other keys go to the device.")
    console = Console(client, sys.stdin.fileno(), sys.stdout.fileno(), sys.stderr.fileno())
    try:
        return console.run(banner)
    except Exception as exc:  # run() has already restored the terminal
        print(f"serial-mcp-terminal: internal error: {exc!r}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
