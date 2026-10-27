# Human terminal (`serial-mcp-terminal`)

`serial-mcp-terminal` is a stdlib-only multi-device TUI over daemon IPC v2. It lists serial ports, shows registered MCP client count and per-device connection/baud/control status, and joins a selected port in a terminal pane. Listing does not pin devices; membership lasts only while the pane is open. Human input requires no AI lease and is sent immediately.

## Install and launch

```sh
uvx --from git+https://github.com/alvinla264/serial-mcp serial-mcp-terminal
# installed:
serial-mcp-terminal
# checkout:
python -m serial_mcp_terminal
# Nix:
nix run .#terminal
```

Run from a real terminal (stdin must be a tty). The app starts or joins the per-user daemon. Use Up/Down or j/k to choose a port, Enter to join, and `q` or Ctrl-Q to quit from the list. An unopened port prompts for baud (an integer from 1 through 4,000,000); malformed or out-of-range input is rejected. An already-open device is joined at its actual baud without prompting or reconfiguration. The registered MCP count includes idle clients. No client names/IDs or controller identity are displayed.

| Input | Action |
|---|---|
| Up/Down, j/k | Select a port in the device list |
| Enter | Join/open selected port and enter its pane |
| Ctrl-] | Leave the pane and return to the list; not sent to hardware |
| Ctrl-Q in list | Quit; not sent |
| Any other pane input | Sent immediately and without local echo, including Ctrl-C, Ctrl-Z, and arrow keys |

The terminal uses raw mode in a pane and restores the previous tty settings on normal exit and handled SIGTERM/SIGHUP/SIGINT. Diagnostics/status appear on stderr; received device bytes are written to stdout. The interface has no terminal emulation or scrollback. Output and daemon event queues are bounded; visible gap events report dropped output/events. On daemon loss the app exits rather than reconnecting. Exit code is 2 if stdin is not a tty, 1 for daemon failure, and 0 on normal quit.

## Ownership and concurrent input

The daemon alone owns serial ports. The human TUI never acquires AI control. A human may type while AI operations are active; bytes can interleave, corrupting a command or interrupting a file transfer. Baud changes take effect for all participants immediately and may likewise affect an in-flight operation. This interference is accepted behavior, not prevented by the interface.

Leaving a pane removes only this app's membership. Other AI/human participants keep the physical port open; after the last participant leaves, the daemon safely settles active work and closes it. Exiting the application removes all its memberships and deregisters its client; zero clients starts the daemon's 30-second shutdown grace.

## Credential and transcript behavior

Human keystrokes are not masked by this UI. The daemon's `view_io` transcript may expose human TX bytes to MCP clients; the daemon logger may also record operation diagnostics. Do not type credentials unless their visibility to other authorized daemon clients is acceptable. Device output is delivered independently; one client's reads do not consume another client's output. The daemon IPC contract is documented in [daemon-ipc.md](daemon-ipc.md).
