# MCP frontend and daemon protocol

## Supported shared application

The shared daemon speaks IPC protocol v2. The native Rust/rmcp stdio frontend
(`serial-mcp`) now negotiates v2 and is the supported shared-app MCP frontend.
It sends every device-specific operation through daemon IPC and does not own a
tty directly. Normal launches run the prebuilt executable; they do not invoke
Cargo or start a Python MCP frontend.

The Python `serial_mcp_client.py` is retained as a legacy parity oracle for
migration tests. It still speaks v1 and must reject the v2 daemon; it is not the
installed or supported shared-app frontend. `server.py` / `serial-mcp-legacy`
is a separate in-process adapter, not a client of the shared daemon. Do not run
it concurrently with another process that owns the same serial device.

The human terminal is a separate install-time-built Rust executable and remains a human IPC client of the shared v2 daemon. See [human-terminal.md](human-terminal.md) for its controls and ownership rules.

## MCP tool behavior

The Rust frontend exposes the shared-daemon catalog. Each device-scoped tool
requires an explicit `port`; one MCP process can use multiple ports concurrently.
`connect(port, baudrate)` joins or opens that device and requests control. A busy
connect joins read-only and reports that control was not granted, the actual
baudrate, and remaining lease time. Only an explicit `connect` can acquire or
reacquire control. Modifying calls never take control implicitly, queue, or
replay after an unknown outcome. `disconnect(port)` leaves only that device.

The frontend removes the legacy `attach`, `tio_info`, and public lease tools,
and exposes `set_baudrate(port, baudrate)`. There is no `attach_socket` or
`session_socket` option in the shared app, and normal connects do not create a
tio UNIX socket or companion pty. Baud changes and lease-loss/settings events
are presented in the next response for the affected device. The frozen contract
and complete tool authorization rules are documented in
[daemon-ipc.md](daemon-ipc.md).

The Rust tool catalog is generated from legacy metadata by
`tools/export_tool_catalog.py`, with a deliberate shared-app v2 transformation.
Run that script with `--check` to detect drift. The Python parity oracle's v1
tool surface is intentionally not used as the shared-app catalog.

## Build and packaging

Build the native frontend once during installation or from a checkout:

```bash
cargo build --release --locked --manifest-path rust-client/Cargo.toml
```

The local executables are `rust-client/target/release/serial-mcp-rust-client` and `rust-client/target/release/serial-mcp-terminal`.
The supported launch paths include `uvx --from <source> serial-mcp` and the Nix
entrypoint. Source builds require Cargo and a Rust toolchain; ordinary native
launches do not. The Python packaging trampoline only `exec`s the built native
binary. The frontend is supported on Linux/WSL.

## Daemon boundary and lifecycle

- The daemon owns physical tty devices, readers, output state, memberships and
  per-device AI control. It remains device-agnostic.
- Human clients join and leave independently of AI control; their input is
  ungated. Human applications use daemon IPC, not the legacy engine's tio socket
  or companion pty.
- Every registered MCP or human client keeps the daemon alive. The last client's
  departure starts a 30-second shutdown grace; a new registration cancels it.
- Runtime discovery uses `$SERIAL_MCP_RUNTIME_DIR`, then
  `$XDG_RUNTIME_DIR/serial-mcp`, then `/tmp/serial-mcp-<uid>`. The directory must
  be a private directory owned by the current user.

## Verification

The Rust frontend is tested over MCP stdio against isolated v2 daemons and fake
PTYs, including multi-device routing, per-device control, settings notices,
protocol mismatch and no-replay behavior. Cargo and Python test commands are
listed in the repository development instructions. Tests never use real serial
hardware.
