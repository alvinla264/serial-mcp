# Generic Serial MCP

Small FastMCP server that exposes a generic serial transport. It does not assume a
specific device, shell, login flow, prompt, or command language.

**The AI starts the session.** It opens the serial device, starts background
reading, and serves a session socket that speaks tio's raw-byte `--socket`
protocol. You attach to that session from your own terminal whenever you want
to watch or type — with any tty tool (tio, screen, minicom), or plain netcat.

## Quick start

### 1. Run the server

**Without nix** — any Python 3.10+ with two pip packages:

    pip install fastmcp pyserial
    python3 /path/to/serial-mcp/server.py

Or with [uv](https://docs.astral.sh/uv/) (no venv juggling):

    uv run --with fastmcp,pyserial /path/to/serial-mcp/server.py

**With nix** (reproducible, pinned deps):

    nix run /path/to/serial-mcp

Tests: `nix develop -c python -m pytest test_server.py`, or
`python3 -m pytest test_server.py` with fastmcp/pyserial/pytest installed.

### 2. Register with your AI agent

Add the server to your agent's MCP config. Replace `/path/to/serial-mcp` with
this repo's checkout path.

All agents take a command + args; swap in whichever launcher you have.
`/path/to/serial-mcp` below is this repo's checkout.

**pi** (`~/.pi/agent/mcp.json`) — nix:

```json
{
  "mcpServers": {
    "serial-mcp": {
      "transport": "stdio",
      "command": "nix",
      "args": ["run", "/path/to/serial-mcp"],
      "lifecycle": "eager"
    }
  }
}
```

pi — no nix (uv or system python):

```json
{
  "mcpServers": {
    "serial-mcp": {
      "transport": "stdio",
      "command": "uv",
      "args": ["run", "--with", "fastmcp,pyserial", "/path/to/serial-mcp/server.py"],
      "lifecycle": "eager"
    }
  }
}
```

**Claude Code** (`~/.claude.json` or project `.mcp.json`):

```bash
claude mcp add serial-mcp -- nix run /path/to/serial-mcp
# or without nix:
claude mcp add serial-mcp -- uv run --with fastmcp,pyserial /path/to/serial-mcp/server.py
```

**Codex CLI** (`~/.codex/config.toml`):

```toml
[mcp_servers.serial-mcp]
command = "uv"
args = ["run", "--with", "fastmcp,pyserial", "/path/to/serial-mcp/server.py"]
```

**OpenCode** (`~/.config/opencode/opencode.json`):

```json
{
  "mcp": {
    "serial-mcp": {
      "type": "local",
      "command": ["uv", "run", "--with", "fastmcp,pyserial", "/path/to/serial-mcp/server.py"]
    }
  }
}
```

**Gemini CLI** (`~/.gemini/settings.json`):

```json
{
  "mcpServers": {
    "serial-mcp": {
      "command": "uv",
      "args": ["run", "--with", "fastmcp,pyserial", "/path/to/serial-mcp/server.py"]
    }
  }
}
```

**Cursor** (`~/.cursor/mcp.json`) — same JSON shape as pi's no-nix variant:
`"command": "uv"`, `"args": ["run", "--with", "fastmcp,pyserial",
"/path/to/serial-mcp/server.py"]`.

Then reload/restart the agent so it picks up the server, and just say
"connect to the serial device".

### 3. Share the session from your terminal

1. Tell the AI to connect: it calls `connect(port="/dev/ttyUSB0")` and reports
   the session socket, e.g. `/tmp/serial-mcp-ttyUSB0.sock` (derived from the
   device name, so multiple devices get unique sockets).
2. Attach from your terminal with any tty tool on the companion pty:

       tio /tmp/serial-mcp-ttyUSB0        # or screen, minicom, cat/echo, ...

   The server exposes a pty (symlinked next to the socket, same name without
   `.sock`) precisely so terminal tools can attach: device output streams in,
   your typing goes to the device. With tio you additionally get timestamps
   and the ctrl-t command menu (ctrl-t q quits). You can re-attach anytime
   without reconnecting.

   Or poke the session directly (same raw protocol tio serves):

       echo "ls" | nc -UN /tmp/serial-mcp-ttyUSB0.sock

3. `disconnect` closes the device and drops all attached clients.

> Only one process can hold the serial device. If you already have tio open on
> `/dev/ttyUSB0`, quit it first; if the AI holds it, attach to its session as
> above instead of opening the port yourself.

## Tools

- `connect(port, baudrate=115200, timeout=0.1, prompt_regex=None,
  session_socket="auto")`: opens the serial port, starts the background
  reader, and serves the session socket. `session_socket` defaults to
  "auto", which derives `/tmp/serial-mcp-<devname>.sock` from the port;
  pass an explicit path or `""` to disable. `prompt_regex` is an optional
  default terminator regex for command completion.
- `disconnect()`: closes the port, stops the reader, and drops session clients.
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
- `tio_info()`: connection status, the session socket path, attached client
  count, and attach instructions.

## Session protocol notes

Two attach surfaces share one broadcast path:

- **Companion pty** (`/tmp/serial-mcp-<dev>`, a symlink to a virtual tty):
  what real tio attaches to — it just sees a normal serial device.
- **UNIX socket** (`/tmp/serial-mcp-<dev>.sock`): speaks tio's `--socket`
  protocol (see tio `src/socket.c`), which is raw bytes: all device output is
  broadcast to every attached client, and every client's bytes are forwarded
  to the device. No handshake or framing, so `nc -UN`, expect scripts, etc.
  can join.

Both directions are bridged: input from either surface goes to the device,
device output goes to both.

Nobody's typing is echoed by the server; each party sees input via the
device's echo, exactly like tio's shared-session mode. Connection loss (the
device is unplugged) is surfaced as an EIO error; the reader stops and
`connect` must be called again.

## Buffer semantics

The background reader continuously appends received bytes to an internal buffer
and logs RX/TX activity to `/tmp/serial-mcp.log`. `send_command` does not clear
pre-existing buffered data; it consumes and returns only data received after the
send point. Pre-existing unsolicited data remains available to `read_output`.
