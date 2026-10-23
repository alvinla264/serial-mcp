# MCP frontend (`serial-mcp`)

`serial-mcp` is the stdio MCP server that coding agents start. It is a thin
client of the per-user shared daemon (`serial-mcp-daemon`, protocol v1, frozen in
[daemon-ipc.md](daemon-ipc.md)). Each agent instance starts its own copy. The copies
discover the daemon, or start it race-safely, and forward each tool call to it.

- The **daemon** owns the serial device, its reader thread, its buffers, the
  companion pty and session socket that `tio` attaches to, and the AI control lease.
- The **frontend** owns nothing on the device. It holds one daemon connection, sends
  requests, and returns the results and errors unchanged. It opens no port, starts no
  reader, writes no log, and imports no device code. It is a native Rust executable using the pinned rmcp SDK. Normal agent launches run this built executable directly; no Cargo or Python MCP framework is invoked. Linux and WSL are the supported frontend targets. Source builds, including wheel builds, require Cargo and a Rust toolchain; build cost is separate from launch cost.

Tool metadata is exported from the Python MCP oracle into `rust-client/tools.json`. The exporter normalizes FastMCP 2's sparse schema/docstring metadata into the canonical schema emitted by FastMCP 4, so `tools/export_tool_catalog.py --check` detects real drift across both development environments. `test_rust_mcp_client.py` compares the Rust public tool list with the normalized oracle and legacy server. The normal Rust process lists tools without importing Python/FastMCP, and forwards calls and lease operations over daemon protocol v1. Tool names, parameters, defaults and descriptions match the canonical catalog.

## Install and launch

The entry point is `serial-mcp`. It uses the same forms as before:

| Source | Command |
|---|---|
| GitHub (recommended) | `uvx --from git+https://github.com/alvinla264/serial-mcp serial-mcp` |
| Local checkout | Build with `cargo build --release --locked --manifest-path rust-client/Cargo.toml`, then run `build/rust-target/release/serial-mcp-rust-client` |
| Local checkout + nix | `nix run github:alvinla264/serial-mcp` or `nix run /path/to/serial-mcp` |
| pip / uvx | Build/install from source with Cargo available to the PEP 517 backend; the wheel's `serial-mcp` console entry point immediately `exec`s the bundled native Rust executable |

Serial access still needs the usual permission (`dialout` group or udev rules).
The daemon, not the agent, opens the port, so the group must apply to the user that
runs the daemon, which is the same user as the agent.

Set the runtime directory only when you need to. It defaults to
`$SERIAL_MCP_RUNTIME_DIR`, then `$XDG_RUNTIME_DIR/serial-mcp`, then
`/tmp/serial-mcp-<uid>`. The directory must be private to you (mode `0700`, owned by
you, not a symlink). The frontend refuses it otherwise and says why.

Optional: `SERIAL_MCP_CLIENT_NAME` sets the name other clients see in `busy` messages
(default `mcp-<pid>`, at most 64 characters).

## Agent configuration

All agents take a command and arguments over stdio. There is no network endpoint.

**Pi** (`~/.pi/agent/mcp.json`):

```json
{
  "mcpServers": {
    "serial-mcp": {
      "transport": "stdio",
      "command": "uvx",
      "args": ["--from", "git+https://github.com/alvinla264/serial-mcp", "serial-mcp"]
    }
  }
}
```

**Any stdio MCP client** (generic shape; use the same command in every agent):

```json
{
  "mcpServers": {
    "serial-mcp": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/alvinla264/serial-mcp", "serial-mcp"],
      "env": {"SERIAL_MCP_CLIENT_NAME": "my-agent"}
    }
  }
}
```

Use the same launcher form (and, for git, the same tag or commit) in every agent.
All of them talk to one daemon, which is started by whichever agent starts first. The
daemon checks only the protocol version, not the release, so mixed versions share the
daemon that is already running.

## Migrating from the old launcher

- `serial-mcp` immediately execs the Rust/rmcp executable (through a tiny Python console-script trampoline when installed from a wheel). Existing stdio configurations retain the command name and arguments. No Python/FastMCP stdio frontend is installed.
- `serial-mcp-legacy` runs the previous in-process server. Each MCP process then owns
  its own device. Do not run it against a device while the daemon has that device open.
  The port is exclusive, so the second opener is refused, but the intent is one owner.
- Connect semantics changed. `connect(port=...)` makes the **daemon** own the port,
  and `disconnect` closes the transport while the daemon keeps running.
- Lifetime changed. Closing an agent no longer closes the device at once. The daemon
  keeps it while any client is connected. When the last client leaves, the daemon
  waits 30 seconds (the grace period), then drains, releases the device, and exits.

## Lifecycle

| Event | What happens |
|---|---|
| Agent starts the frontend | The frontend registers with the daemon in the background. The daemon starts if it is not running. An idle but healthy frontend stays registered. |
| Agent exits (stdin closed) | The frontend says `bye` and closes. The daemon removes the client. If that client held control, control is released after its in-flight operation settles. The device stays connected for the other clients. |
| Agent crashes or is killed | The kernel closes the socket. The daemon removes the client the same way. Nothing has to be remembered by the agent. |
| Explicit `disconnect` | Only the device transport and its session surfaces close. The daemon and every client stay. This is a gated tool. |
| Last client leaves | 30-second grace. A new client cancels it. Otherwise the daemon exits and releases the device. |
| Daemon killed or crashed | The next tool call reports the lost connection. It is **not** retried. The frontend then starts a new daemon on the next call, but the device is disconnected there and control must be reacquired explicitly. |

Human terminals count as clients too, so a connected human keeps the daemon alive.

## Control (AI lease)

One lease per device controls the gated tools: `connect`, `attach`, `disconnect`,
`send_command`, `read_output`, `clear_output`, `set_prompt_pattern`, `login`,
`enter_bootloader`, `upload_file`, `download_file`. Read-only tools (`view_io`,
`tio_info`, `list_serial_ports`, `list_boot_profiles`, `set_boot_credentials`,
`file_transfer_capabilities`) never need it.

- **Automatic.** A gated call takes control when it is free.
- **Conflicts.** If another client holds control, the call fails with `busy`. The message
  names the holder and its state.
- **Expiry.** Control expires after 60 idle seconds. An operation in progress never
  expires. Reading views (`view_io` and the rest) does not renew control.
- **Reacquisition.** After control is lost (expiry, release, or a dropped connection),
  gated calls are refused with `lease_lost` until `lease_acquire` succeeds. The
  frontend never retries a stale request. A successful reacquisition carries a
  `warning` that device state may have changed.
- **Tools.** `lease_acquire`, `lease_release` and `lease_status` return JSON, the
  lease object from [daemon-ipc.md](daemon-ipc.md#6-control-lease) with `you`
  (`holder`, `reacquire_required` or `none`). `lease_status` never renews.

Events the frontend receives since the last reply appear as a `NOTICE:` line before a
text result, or as a `notices` array in lease JSON. For example, after expiry you see
`NOTICE: control was lost; device state may have changed since you last used it...`.

Human input is not gated by the lease, and no MCP tool sends human input. The human
terminal is described in its own documentation.

## Results and errors

- A tool result is the same text the legacy tool returns. A condition the tool reports as
  text (for example `Error: Not connected to a serial port.`) is still a successful
  result, as before.
- A refusal or failure is an MCP tool error (`isError`). Its text names the daemon error
  code and message, and for `busy` and `lease_lost` the lease object as JSON.
- A call whose reply is lost (the connection ends, or no reply arrives within the
  bound) is reported with *the outcome is unknown; the call was not retried*. The device
  may already have acted. Check the device state before calling again.
- Daemon not reachable, runtime directory unsafe, or protocol mismatch: the error names
  the cause and the runtime directory and daemon log locations. A daemon announcing a
  different protocol version is refused with that version in the message.
- A daemon that is draining refuses a new client's hello. The frontend retries the
  hello on a new connection, a bounded number of times. Hello changes nothing on the
  device, so this is safe.

## Human attach and file transfer

- `attach` and `tio -S unix:/tmp/serial-mcp-<dev>.sock <device>` keep working. External
  tio clients attach to the daemon-owned session socket and companion pty as before.
- File transfers keep their existing confirmations. Attach-mode transfers still need
  `confirm_shared_quiescence=true`, which is a request to the operator and does not stop
  human writers. The lease does not arbitrate human input.

## Limits

- MCP has no server push to the agent. Device output for observers comes through
  `view_io` and `tio_info`. Output that a subscribed human receives is not delivered to
  an MCP client.
- Unread output (`read_output`) belongs to the device, and it is gated. Only the
  controlling client can consume it.
- The frontend compares protocol versions, not release versions. Keep all agents on the
  same launcher source.

## Tests

`test_rust_mcp_client.py` drives the built native executable over MCP stdio; `test_mcp_client.py` retains daemon and Python-oracle coverage. The daemon is either in-process with an injected clock (lease and grace advance without sleeps) or a real daemon subprocess (auto-start, races, crashes). Devices are ptys, and fake daemons are scripted sockets. Run with:

```bash
cargo build --release --locked --manifest-path rust-client/Cargo.toml --target-dir build/rust-target
SERIAL_MCP_RUST_CLIENT="$PWD/build/rust-target/release/serial-mcp-rust-client" \
  uv run --with fastmcp,pyserial,pytest python -m pytest -q
SERIAL_MCP_RUST_CLIENT="$(nix build .#default --no-link --print-out-paths)/bin/serial-mcp" \
  nix develop -c python -m pytest -q
```
