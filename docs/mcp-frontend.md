# MCP frontend migration status

## Breaking transition to daemon protocol v2

The shared daemon on this branch speaks protocol v2. The Rust stdio MCP frontend
(`serial-mcp`) and the retained Python daemon frontend (`serial_mcp_client.py`)
still speak protocol v1 and **cannot be used with this daemon**. They fail clearly
when their hello response reports an incompatible version. The Rust frontend
migration is pending in task /14; do not treat the launcher commands below or
examples from older documentation as working against this v2 daemon.

The native human terminal also still speaks v1 and is incompatible; its migration
is pending in task /15. See [human-terminal.md](human-terminal.md) for the
explicit migration status and [daemon-ipc.md](daemon-ipc.md) for the current wire
contract.

The in-process legacy server (`serial-mcp-legacy`, `server.py`) is a separate
adapter and is not a v2 daemon frontend. It remains available for its existing
in-process use; do not use it concurrently with another process opening the same
exclusive serial device.

## Current daemon boundary

- The daemon owns independently opened physical tty devices, reader/output state,
  client membership and per-device AI control. It is device-agnostic.
- MCP callers use the daemon `call` operation, with an explicit port selector for
  device-affecting tools. `connect(port, baudrate)` opens or joins a device and
  requests control; a busy join becomes an observer. Modifying calls require the
  current controller. See the v2 contract for the complete tool and permission
  matrix.
- Human clients join/leave without acquiring AI control. Input is ungated by AI
  control; each client has independent output cursors. Human applications use
  daemon IPC, not the legacy engine's tio socket or companion pty.
- The daemon stays available while any client is registered. The last client's
  departure starts a 30-second grace period; new clients cancel it.
- Runtime discovery uses `$SERIAL_MCP_RUNTIME_DIR`, then
  `$XDG_RUNTIME_DIR/serial-mcp`, then `/tmp/serial-mcp-<uid>`. The directory must
  be private to the current user.

The frozen v2 framing, requests, events, errors, lease behavior, backpressure,
limits and unknown-outcome rules are in [daemon-ipc.md](daemon-ipc.md). Frontend
migration must update the client contract and its tests together; the Rust tool
catalog and integrated Cargo gates are deliberately deferred to task /14.

## Historical installation forms

These commands still build or launch the v1 Rust executable, but **must not be
used with the protocol-v2 daemon until task /14 completes its migration**:

| Source | Command |
|---|---|
| GitHub | `uvx --from git+https://github.com/alvinla264/serial-mcp serial-mcp` |
| Local checkout | Build with `cargo build --release --locked --manifest-path rust-client/Cargo.toml`, then run the built `serial-mcp-rust-client` executable |
| Nix | `nix run github:alvinla264/serial-mcp` or `nix run /path/to/serial-mcp` |

Source builds require Cargo and a Rust toolchain. Normal native launches do not
invoke Cargo. These build details do not imply v2 compatibility.

## Verification status

Daemon and IPC behavior is tested with synthetic IPC clients and fake PTYs. The
v1 frontend regression suites still describe the earlier contract and are not
acceptance tests for protocol v2. The Rust frontend migration and combined Cargo
checks remain for task /14; the native human terminal migration remains for task
/15.
