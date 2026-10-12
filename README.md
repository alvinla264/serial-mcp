# Generic Serial MCP

Small FastMCP server that exposes a generic serial transport. It does not assume a
specific device, shell, login flow, prompt, or command language.

**Either side can own the device.** Serial ports cannot be shared, so one party
holds the port and the other joins its session over a raw-byte UNIX socket:

- **You own it** — run `tio -S unix:/tmp/serial-mcp-<dev>.sock <device>` and the
  agent attaches to your session (`attach`). It cannot lock you out, and several
  agent sessions can share the one device.
- **The agent owns it** — it calls `connect(port=...)` and serves a session socket
  plus a companion pty for your terminal tools to attach to.

Repo: https://github.com/alvinla264/serial-mcp

## Quick start

There are three ways to run it, no local checkout required for any of them:

| Source | Command |
|---|---|
| **GitHub (recommended)** | `uvx --from git+https://github.com/alvinla264/serial-mcp serial-mcp` |
| **Local checkout + uv** | `uv run --with fastmcp,pyserial /path/to/serial-mcp/server.py` |
| **Local checkout + nix** | `nix run github:alvinla264/serial-mcp` — or `nix run /path/to/serial-mcp` |

Notes:

- The GitHub/uvx and nix forms fetch, build, and run straight from the repo —
  nothing to clone or install first.
- Plain pip works too: `pip install fastmcp pyserial`, then
  `python3 server.py` from a checkout.
- Tests: `nix develop -c python -m pytest test_server.py`, or
  `uv run --with fastmcp,pyserial,pytest python -m pytest test_server.py`.
- Serial permissions still apply outside nix: your user needs `dialout` group
  membership (or udev rules) to open `/dev/ttyUSB0`.

### Register with your AI agent

All agents take a command + args; pick the launcher form from the table above.
The examples below use the GitHub source so there's no local path dependency —
swap in a local-path form if you prefer a checkout. `alvinla264/serial-mcp`
can be forked/replaced with your own.

Pin a specific version by appending `@<tag-or-commit>` to the git URL, e.g.
`git+https://github.com/alvinla264/serial-mcp@v0.2.0`, otherwise the branch
tip is re-resolved on each start.

**pi** (`~/.pi/agent/mcp.json`) — GitHub via uvx:

```json
{
  "mcpServers": {
    "serial-mcp": {
      "transport": "stdio",
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/alvinla264/serial-mcp",
        "serial-mcp"
      ]
    }
  }
}
```

pi — local checkout via nix: `"command": "nix"`,
`"args": ["run", "/path/to/serial-mcp"]`.

**Claude Code** (`~/.claude.json` or project `.mcp.json`):

```bash
claude mcp add serial-mcp -- uvx --from git+https://github.com/alvinla264/serial-mcp serial-mcp
```

**Codex CLI** (`~/.codex/config.toml`):

```toml
[mcp_servers.serial-mcp]
command = "uvx"
args = ["--from", "git+https://github.com/alvinla264/serial-mcp", "serial-mcp"]
```

**OpenCode** (`~/.config/opencode/opencode.json`):

```json
{
  "mcp": {
    "serial-mcp": {
      "type": "local",
      "command": [
        "uvx",
        "--from",
        "git+https://github.com/alvinla264/serial-mcp",
        "serial-mcp"
      ]
    }
  }
}
```

**Gemini CLI** (`~/.gemini/settings.json`):

```json
{
  "mcpServers": {
    "serial-mcp": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/alvinla264/serial-mcp",
        "serial-mcp"
      ]
    }
  }
}
```

**Cursor** (`~/.cursor/mcp.json`) — same JSON shape as Gemini's: `"command":
"uvx"`, `"args": ["--from", "git+https://github.com/alvinla264/serial-mcp",
"serial-mcp"]`.

**VS Code** (Copilot Chat MCP support, `.vscode/mcp.json` in the workspace or
`~/.vscode/mcp.json` for all projects) — note the `"servers"` wrapper and
`"type": "stdio"`:

```json
{
  "servers": {
    "serial-mcp": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/alvinla264/serial-mcp",
        "serial-mcp"
      ]
    }
  }
}
```

Reload with the `MCP: List Servers` command (or restart VS Code), then start
Copilot Chat in agent mode and ask it to connect to the serial device.

Then reload/restart the agent so it picks up the server, and tell it what you
want — "attach to my serial session" (see below) or "connect to the serial
device".

### Two ways to share one device

A serial port can only be held by one process, so either the **agent** or **you**
owns it — and the other attaches to that session. Both directions use the same
raw-byte tio socket protocol.

#### You own it, the agent attaches (recommended)

Start tio yourself. It holds the device and serves a socket; the agent joins as
a client whenever you ask it to.

```bash
tio -S unix:/tmp/serial-mcp-ttyUSB0.sock /dev/ttyUSB0
```

Then tell the agent to attach — it calls `attach()` (auto-detects the socket) or
`connect(attach_socket="/tmp/serial-mcp-ttyUSB0.sock")`.

Why this is usually better:

- **The agent cannot lock you out.** It holds no port; `disconnect` only detaches
  it, and your tio session keeps running.
- **Several agent sessions can share one device.** They all attach to your tio, so
  nothing races for the port or for the session paths. Everyone sees all device
  output, whoever caused it.
- **You see everything first.** Including the boot messages from before the agent
  was involved.

> Sharing is for **coexistence, not for simultaneous typing.** tio forwards each
> client's bytes to the port as they arrive and does not buffer per client, so if
> two clients write at the same instant their commands interleave — `echo a` from
> one and `echo b` from the other can reach the device as `echoecho a` and fail.
> Have one agent (or you) drive the device at a time; the others can watch. There
> is no locking in tio's socket protocol, so this cannot be enforced from here.

> tio 3.9+ **requires the `unix:` prefix**: `-S /tmp/x.sock` fails with "Invalid
> socket scheme". Use `-S unix:/tmp/x.sock`.
>
> Run tio in a real terminal. With stdin redirected (a pipe or `/dev/null`) it
> prints `Disconnected` and exits immediately, and nothing is relayed.

#### The agent owns it, you attach

The agent calls `connect(port="/dev/ttyUSB0")`, which opens the device and serves
a session socket plus a **companion pty**. You then attach with any tty tool:

    tio /tmp/serial-mcp-ttyUSB0        # or screen, minicom, cat/echo, ...

The pty (symlinked next to the socket, same name without `.sock`) exists so real
terminal tools can attach as if it were the device: device output streams in, your
typing goes to the device. With tio you also get timestamps and the ctrl-t command
menu (ctrl-t q quits). You can re-attach anytime without reconnecting the agent.

Or poke the session directly (same raw protocol):

    echo "ls" | nc -UN /tmp/serial-mcp-ttyUSB0.sock

`disconnect` closes the device and drops all attached clients.

> **Only one process can hold the serial device, and the server enforces it.**
> ttys are not exclusive-open — `pyserial` will happily open a port another
> process already holds, after which the two interleave bytes on the wire — so
> the server takes an advisory lock per device instead. If something else holds
> it, `connect(port=...)` fails and names the holder's pid and command rather
> than sharing the port. The kernel releases the lock when the holder exits, so a
> crashed server never leaves the port blocked.
>
> **A live session is never taken over.** If a session socket is already being
> served — by your own tio, or by another agent — `connect(port=...)` refuses
> instead of unlinking it and binding its own, and tells you to join it with
> `attach` (or to pass a different `session_socket` path). This matters because
> taking the path over would leave the previous owner running, still holding the
> device, but unreachable — silently breaking the session you were using.
>
> If an agent reports *"session socket ... is already served"* or *"... is
> already held"*, it does not need a fix: it should `attach` to the existing
> session, or you should quit the holder first.
>
> In this direction, run **one** agent session per serial device. Attach mode is
> exempt: there your tio owns the port and the session paths, and any number of
> agents can join it.

## Tools

- `connect(port=None, baudrate=115200, timeout=0.1, prompt_regex=None,
  session_socket="auto", attach_socket=None)`: opens the serial port (owning it),
  or joins a session someone else serves. Pass exactly one of `port` or
  `attach_socket`:

  - `port=...` — open and own the device, then serve the session socket.
    `session_socket` defaults to "auto", deriving
    `/tmp/serial-mcp-<devname>.sock`; pass an explicit path or `""` to disable.
    Calling it again for the same device reuses the existing session (and keeps
    anyone attached to it).
  - `attach_socket=...` — join a session already being served (typically your
    own `tio -S unix:/tmp/serial-mcp-<dev>.sock <device>`). No port is opened and
    no session socket or pty is created; `disconnect` detaches without
    disturbing the owner.

  `prompt_regex` is an optional default terminator regex for command completion.
- `attach(attach_socket=None, prompt_regex=None)`: convenience wrapper for
  joining a session. Without a path it auto-detects live session sockets and
  requires exactly one, so an ambiguous setup is reported rather than guessed at.
  Pass the path explicitly when more than one session is live.
- `disconnect()`: closes the port, stops the reader, and drops session clients.
  In attach mode it only detaches: the owner's session keeps running.
- `send_command(text, line_ending="lf", timeout=5.0, idle_timeout=0.2,
  terminator_regex=None, encoding="utf-8")`: sends `text` plus an explicit line
  ending (`none`, `lf`, `cr`, or `crlf`) and returns bytes observed after the
  send point. Completion uses `terminator_regex` (or the connection default) if
  provided; otherwise it returns after `idle_timeout` seconds of silence, bounded
  by the overall `timeout`.
- `read_output(timeout=0.0, idle_timeout=0.2, encoding="utf-8")`: consumes and
  returns buffered unsolicited/asynchronous output. With a positive timeout it
  waits for arriving data and then for idle silence, never beyond timeout.
- `clear_output()`: discards currently buffered output.
- `set_prompt_pattern(regex_pattern=None)`: sets or clears the default regex
  terminator used by `send_command`.
- `view_io(lines=50, direction="all", hex_mode=False, encoding="utf-8")`: tio-style
  timestamped transcript of recent I/O. Shows `[TX:ai]` (what the AI sent),
  `[TX:user]` (what an attached user typed), and `[RX]` (device output).
  `direction` filters to `tx`, `rx`, or `all`; `hex_mode=True` renders raw bytes
  in hex (like `tio --hex`).
- `list_serial_ports()`: lists available serial ports with descriptions
  (like `tio --list`).
- `tio_info()`: connection status. In direct mode: the session socket path,
  attached client count, and attach instructions. In attach mode: which session is
  joined, that this server holds no port, and that detaching leaves it running.

### Bootloader entry (U-Boot)

Automated login -> reboot -> autoboot-interrupt flow:

- `enter_bootloader(...)`: logs in if the device sits at a login prompt,
  reboots, then sends interrupt keys at a paced interval during the autoboot
  window until the U-Boot prompt appears. Pacing (`interval`, default 50ms)
  matters: flooding a U-Boot console can overflow the RX buffer and *drop*
  interrupt characters. Hard caps everywhere: `max_attempts` per pass and
  `overall_timeout` for the whole operation; if an `abort_regex` (e.g.
  `Login:`) shows the device booted fully, it logs in and reboots again
  instead of tapping into a dead console.
- `list_boot_profiles()`: shows known device profiles (builtin: `generic`,
  `u-boot-any-key`).
- `set_boot_credentials(profile_key, username, password)`: stores login
  credentials locally for a profile.

**Profiles** (shareable) vs **credentials** (local-only): profiles describe the
*recipe* — interrupt keys, pacing, reboot command, prompt regexes, and a
`login: {username, password_ref}` reference. Secrets never live in profiles.
Credentials are stored at `~/.config/serial-mcp/credentials.json` (mode 0600,
outside any repo) and are redacted from logs and `view_io` transcripts.

Custom profiles go in `~/.config/serial-mcp/profiles.json`:

```json
{
  "mydevice": {
    "description": "Vendor X board",
    "bootloader_prompt_regex": "(U-Boot|uboot)[^\n]*[>#]\s|^\s*=>\s",
    "abort_regex": "(login:|Login:)",
    "interrupt": { "keys": [" "], "delay": 0.2, "interval": 0.05, "max_attempts": 40 },
    "reboot": { "command": "reboot", "cycles": 3 },
    "login": { "username": "root", "password_ref": "mydevice" }
  }
}
```

Then: `set_boot_credentials("mydevice", "root", "…")` once, and the AI can
just call `enter_bootloader(profile="mydevice")`.

## Session protocol notes

One owner, any number of clients. The owner holds the serial port; clients join
over a UNIX socket that speaks tio's `--socket` protocol (see tio
`src/socket.c`) — raw bytes, no handshake or framing, so `nc -UN`, expect
scripts, and tio itself can all join.

**When this server owns the device** it serves that socket, and additionally
exposes a companion pty so terminal tools can attach as if it were a real device:

- **Companion pty** (`/tmp/serial-mcp-<dev>`, a symlink to a virtual tty): what
  `tio`, `screen`, `minicom`, `cat` attach to.
- **UNIX socket** (`/tmp/serial-mcp-<dev>.sock`): the same raw protocol, for
  `nc -UN`, scripts, and other agents.

Device output is broadcast to every attached client, and every client's bytes are
forwarded to the device, so both directions are bridged across both surfaces.

**When this server attaches** to someone else's session (your tio), it is simply
another client on that socket: it holds no port, serves nothing, and creates no
pty. Detaching closes only its own connection.

Nobody's typing is echoed by the server; each party sees input via the device's
echo, exactly like tio's shared-session mode. Connection loss is surfaced as an
EIO error — the device was unplugged (direct mode), or the owner's socket closed
(attach mode). The reader stops and `connect` must be called again.

A liveness probe explains one wrinkle of attach mode: to tell a live session from
a leftover socket file, the server connects briefly and closes. The owner sees
that as a client that joined and left immediately, which its accept loop must
tolerate — tio does.

## Buffer semantics

The background reader continuously appends received bytes to an internal buffer
and logs RX/TX activity to `/tmp/serial-mcp.log`. `send_command` does not clear
pre-existing buffered data; it consumes and returns only data received after the
send point. Pre-existing unsolicited data remains available to `read_output`.
