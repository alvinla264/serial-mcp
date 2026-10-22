# Human terminal (`serial-mcp-terminal`)

`serial-mcp-terminal` is a plain interactive serial console for a human, built on
the shared `serial-mcp` daemon. It works like `tio` or `minicom` with the device
already open: you see the live output and every key goes straight to the device.
While you are connected, AI agents on their MCP clients stay connected too.

It is deliberately thin. There is no configuration dashboard, no logging to a
file, no scrollback, and no terminal emulation.

## Install and launch

It needs Python 3.10 or newer. The terminal itself uses only the standard library,
and it never imports `fastmcp` or `pyserial`. The package install still pulls in
the MCP dependencies, because it ships the same package.

```sh
uvx --from git+https://github.com/alvinla264/serial-mcp serial-mcp-terminal
# or, installed:
uv tool install git+https://github.com/alvinla264/serial-mcp
serial-mcp-terminal
# from a checkout, without installing:
python -m serial_mcp_terminal
# with Nix:
nix run .#terminal
```

Run it from a real terminal. Standard input must be a tty. Otherwise it exits with
status 2 and changes nothing.

On start it connects to the per-user daemon. If no daemon is running, it starts one
(the same way the MCP clients do). Then it prints one status line, such as
`connected to the daemon (client c2); device: … Ctrl-] exits; other keys go to the device.`
If no device is connected yet, the line says so, and the terminal shows output as
soon as a device is connected.

## Usage and exit

| Key | What happens |
|---|---|
| **Ctrl-]** | Exits the terminal. It is handled locally and is **never** sent to the device. |
| Everything else | Sent to the device immediately, byte for byte: letters, Enter (CR), Backspace, Tab, arrow keys, Ctrl-C, Ctrl-Z, Ctrl-\ and Ctrl-S/Ctrl-Q. |

Because Ctrl-] is reserved, you cannot type that one byte into the device. Nothing
else is reserved.

The terminal runs the tty in raw mode: no local echo, no line editing, no local
signals (Ctrl-C does not interrupt the terminal; the device gets the byte). Device
output is written to your terminal unchanged, so colour and cursor escape sequences
behave as in `tio`. A device can send escape sequences that change your terminal
too, which is also true of `tio` and `minicom`.

Status and diagnostics go to standard error, between `serial-mcp-terminal:` brackets.
They never touch the device stream on standard output.

Exit status: `0` after Ctrl-] or when the terminal input closes; `1` when the
daemon refuses or drops the connection, or on an internal error; `2` when standard
input is not a terminal; `128 + n` after a termination signal `n` (SIGTERM, SIGHUP,
SIGINT). The terminal restores its previous tty settings on every exit path that
Python can run, including those signals.

## What the terminal does and does not do to the daemon

- **Exiting** (Ctrl-] or a signal) closes only the terminal's own connection. The
  device stays connected, every MCP client keeps its connection, and no AI control
  lease is released or changed. A running AI operation keeps running.
- **Crashing** (for example `kill -9`) is seen by the daemon as the client leaving,
  exactly like an exit. A process that is killed cannot restore its tty, so the
  terminal may be left in raw mode. Run `reset` or `stty sane` in that shell.
- **Lifetime.** A terminal connection counts as a live client, like an MCP client.
  The daemon stays up while the terminal is connected, even with no AI client. When
  the last client leaves, whether MCP or terminal, the daemon's 30-second grace period
  starts, and any new connection cancels it. A terminal that auto-started the daemon
  therefore leaves it running for 30 seconds after its own exit, unless another
  client connects.
- **Daemon shutdown** (SIGTERM to the daemon, or grace expiry) is reported and the
  terminal exits with status 1. The terminal does not reconnect; run it again.

## Connect and disconnect semantics

The terminal never opens the serial port. It is a client of the daemon, which alone
owns the device. It has no connect or disconnect command.

- To **connect a device**, an MCP client calls `connect` (the daemon opens and owns
  the port), or calls `attach` to join a session that your own `tio` is serving.
  The terminal then shows that device's output and forwards your keys to it.
- To **disconnect a device**, an MCP client calls `disconnect`. The daemon closes its
  port or its attachment, but keeps running. The terminal prints `device disconnected`
  and stays open. Connect again later and it resumes.
- The explicit workflow `disconnect` → run `tio` yourself on the port → `attach` from
  the MCP client still works. The daemon itself only attaches or opens the port when
  an MCP client asks, as before.

Output starts when the terminal connects. The daemon does not replay earlier output.
If the device is not connected, keys are dropped and the terminal prints one
`not sent: no device is connected` line until a device appears.

## Concurrent input: the accepted risk

**Human keystrokes are not controlled by AI leases, and nothing locks a human
out.** This is intentional. The 60-second AI control lease coordinates AI agents
only. Your typing does not wait for an AI command, does not require the lease, and
has no acquire, release, watch mode or takeover delay.

As a consequence:

- Your keystrokes and an AI command or file transfer share one wire. Bytes
  interleave. Typing while an AI command runs can corrupt the command, and an AI
  command can corrupt what you are typing. A file transfer can fail if you type
  during it.
- The `confirm_shared_quiescence` confirmation of file transfers stays a request to
  the operator. It does **not** lock out your terminal. Only you can keep the
  terminal idle during a transfer.
- Device output is shown to every client independently. Nobody's output is consumed
  by another client's read.

**Secrecy.** The daemon records everything the terminal sends, in plain text. Your
keystrokes appear in the daemon's log file (`daemon.log` in the runtime directory)
and in the transcript that the `view_io` MCP tool returns, labelled `[TX:human]`.
Any MCP client can read that transcript. If you type a password at a login prompt
through this terminal, it is in those places. Do not type credentials into the
terminal unless that is acceptable for every MCP client connected to the daemon.
(A future change could redact or stop recording human input. That is a daemon
decision, not something this terminal does.)

## Limits

- The terminal shows output from the moment it connects. It keeps no history.
- If your screen stops drawing for a long time, the terminal holds the device output
  it has received in memory until the screen catches up. The daemon itself never
  waits for a slow terminal. If the terminal falls too far behind, the daemon drops
  the oldest output for that terminal and it prints a `bytes of output were dropped`
  line.
- The terminal does not reconnect after the daemon goes away. Run it again.
- The `human` role is not a security boundary. Any program running as your user can
  join the daemon as a human client, as with the MCP clients.
- Serial settings (baud rate and so on) are set by the MCP `connect` tool, not here.

## Relation to the other clients

The daemon's frozen wire contract is in [daemon-ipc.md](daemon-ipc.md). The human
terminal is one of its clients. The rules for MCP agents, the control lease and the
MCP client configuration are documented in the README.
