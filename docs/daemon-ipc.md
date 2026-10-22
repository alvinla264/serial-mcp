# Shared daemon IPC contract (protocol v1)

Status: **frozen** for protocol version 1. Changes that alter a frame shape,
error code, or lifetime rule require a new protocol version. Additive fields
may be ignored by clients; clients must tolerate unknown fields and unknown
event names.

This document is the contract between the per-user `serial-mcp` daemon
(`serial_mcp_daemon.py`) and any client: the stdio MCP frontend, the native
human terminal, or a synthetic test client. The shared client primitives
(`serial_mcp_ipc.py`) are a reference implementation; a client written in any
language may follow this document directly.

## 1. Scope and non-goals

- One daemon per user, one serial device per daemon (single-device tool
  semantics are unchanged).
- Local UNIX stream socket only. No TCP/HTTP listener, no remote sharing.
- The legacy `serial-mcp` stdio launcher (`server.py`) is **not** changed by
  this contract and still runs its own in-process device session.
- The raw tio/session-socket/companion-pty surfaces of a daemon-owned device are
  unchanged. This contract does not protect them; see section 9.

## 2. Discovery, endpoint and permissions

Runtime directory, first match wins:

1. `$SERIAL_MCP_RUNTIME_DIR` (absolute path; used by tests and advanced users);
2. `$XDG_RUNTIME_DIR/serial-mcp`;
3. `/tmp/serial-mcp-<uid>`.

Endpoint files inside it:

| File | Purpose |
|---|---|
| `daemon.sock` | UNIX stream socket, mode `0600` |
| `daemon.lock` | `flock` file. The daemon takes it exclusively itself and holds it for its whole lifetime; never deleted |

Safety rules (enforced by both daemon and client helpers):

- The directory is created with mode `0700`. If it already exists it must be a
  real directory (not a symlink), owned by the effective uid, and have no group
  or other permission bits. Otherwise the helper refuses to use it.
- The daemon accepts a connection only if `SO_PEERCRED` reports the same uid.
- The client helper checks the peer uid of the connected socket as well.
- The socket path must fit `sun_path` (108 bytes); longer paths are refused.
- The daemon writes its own log to `<runtime dir>/daemon.log`; `SERIAL_MCP_DAEMON_LOG`
  overrides the path (tests use this). The legacy launcher keeps `/tmp/serial-mcp.log`
  (or `SERIAL_MCP_LOG` when set). A daemon sets `SERIAL_MCP_LOG` to its own log before
  importing `server`, and it creates its state only after it holds the lock, so starting
  a daemon never truncates the legacy log, and a daemon that loses the lock touches no log.
- Any process running as the same user can connect and claim any role. The uid
  check and the `0600`/`0700` modes are the only boundary, and they are the same
  boundary that protects the legacy session files.

## 3. Start and stop

Safety rests on the daemon, not on the starter. The daemon takes `daemon.lock`
exclusively on its own descriptor before it touches the socket. It waits at most
`LOCK_PROBE_WAIT_SECONDS` (0.5 s) for the lock and otherwise exits 0 without
binding or unlinking anything. A daemon that does not hold the lock therefore
never serves, never replaces a socket, and cannot take over a live endpoint, no
matter how it was started. The daemon accepts no inherited lock descriptor.

`ensure_daemon()` (client helper) decides only when to spawn, and is bounded:

1. Try to connect to `daemon.sock`. On success, use it.
2. If a spawn happened less than `START_GRACE_SECONDS` (2 s) ago, poll: the child
   is still taking the lock and binding.
3. Probe `daemon.lock` with a non-blocking exclusive `flock` and release it at
   once. If the lock is held, a daemon is starting or running: poll. If it is
   free, spawn `python -m serial_mcp_daemon` (stdio on `DEVNULL`,
   `start_new_session=True`, runtime directory passed through). Racing starters
   may each spawn; the losers exit on their own after their lock probe times out.
4. Repeat until connected or `START_TIMEOUT_SECONDS` (10 s) elapse. At most 3
   spawns are made. Timeout raises `DaemonUnavailable`.

Starters never remove a socket file. Only a daemon that holds the lock unlinks a
stale socket. Any client, in any language, may follow the same steps, and the
daemon still enforces the single-instance rule.

Lifetime (daemon side):

- A **client** is an open connection that completed `hello` (roles `mcp` and
  `human`). Connections that never send `hello` within `HELLO_TIMEOUT_SECONDS`
  (5 s) are closed and do not count.
- A client is live while its socket is open. An alive but idle client counts.
  EOF or reset (including a crashed process, whose kernel closes the socket)
  removes it immediately. Clients are never polled for voluntary disconnect.
- When the live-client count drops to zero, a **grace period** of
  `GRACE_SECONDS` (30 s) starts. Any new `hello` cancels it. The daemon also
  starts in the grace state, so a daemon nobody connects to exits after 30 s.
- When the grace period expires, the daemon: stops accepting connections, rejects
  new requests with `shutting_down`, waits up to `DRAIN_TIMEOUT_SECONDS` (5 s)
  for in-flight operations, then disconnects the device (closing its transport
  and session surfaces, which cancels any operation still blocked on it), removes
  `daemon.sock`, releases `daemon.lock`, and exits. SIGTERM takes the same path.

## 4. Framing and versioning

- Transport: a stream of UTF-8 JSON objects, one per line, each terminated by
  `\n` (LF). No other framing.
- Maximum frame length including the newline: `MAX_FRAME_BYTES` = 4 MiB.
  A longer line is never buffered beyond the limit: the daemon replies
  `frame_too_large` (when a request id is known) and closes the connection.
- Every frame carries `"v": 1`. A different value is rejected with
  `unsupported_version` and the connection is closed.
- A line that is not a JSON object is rejected with `bad_request`. The
  connection stays open if the frame was otherwise well-formed.
- Client requests carry an `id` (integer or string, chosen by the client, unique
  among its outstanding requests). Responses echo it. Events have no `id`.

Frames from the daemon are one of:

```text
{"v":1,"id":<id>,"ok":true,"result":<object>}
{"v":1,"id":<id>,"ok":false,"error":{"code":"<code>","message":"<text>", ...}}
{"v":1,"event":"<name>", ...}
```

## 5. Requests

The first request on every connection must be `hello`. Any other op before
`hello` returns `hello_required`. A second `hello` returns `already_hello`.

| op | fields | result |
|---|---|---|
| `hello` | `role` (`"mcp"` or `"human"`), `name` (string, ≤ 64 chars), `pid` (optional int), `client_version` (optional string) | `{"protocol":1,"daemon_version":"…","client_id":"c<n>","role":"…","lease":<lease>}` |
| `status` | — | `{"daemon_version","protocol","clients":n,"grace":{"active":bool,"remaining":secs\|null},"shutting_down":bool,"device":"<label>"\|null,"lease":<lease>}` |
| `call` | `tool` (string), `args` (object, default `{}`) | `{"result":"<text>"}` |
| `lease` | `action`: `"acquire"`, `"release"` or `"status"` | a lease object (section 6) plus, for `acquire`, an optional `warning` |
| `subscribe` | — | `{"subscribed":true}`; output events start (section 7) |
| `unsubscribe` | — | `{"subscribed":false}` |
| `input` | `data` (base64 of raw bytes, ≤ 64 KiB decoded); **role `human` only** | `{"written":n}` |
| `bye` | — | `{}`; the daemon closes the connection after responding |

Unknown ops return `unknown_op`. The `status` op is available to any hello'd
client and never renews or changes a lease.

### Tools (`call`)

Tool names and argument names match the MCP tools exactly
(`connect`, `attach`, `disconnect`, `send_command`, `read_output`,
`clear_output`, `set_prompt_pattern`, `login`, `enter_bootloader`,
`upload_file`, `download_file`, `view_io`, `tio_info`, `list_serial_ports`,
`list_boot_profiles`, `set_boot_credentials`, `file_transfer_capabilities`).
Arguments are bound by name to the same implementation the MCP tool calls, so
an unknown or misspelled argument returns `bad_args`. The result is the same
text the MCP tool would return.

Tools are either **gated** (may change or consume device state, or are
device-affecting; they require the control lease) or **ungated** (read-only or
local-only; they never require or renew the lease):

- ungated: `view_io`, `tio_info`, `list_serial_ports`, `list_boot_profiles`,
  `set_boot_credentials`, `file_transfer_capabilities`;
- gated: every other tool above.

Errors: an unknown tool returns `unknown_tool`; gated calls can also return
`busy` or `lease_lost` (section 6). A tool that returns an error string (for
example `Error: Not connected to a serial port.`) is still a successful
`call` whose `result` is that text, exactly as the MCP tool behaves.

A result is delivered only if its response frame fits `MAX_FRAME_BYTES` (4 MiB)
after JSON encoding; a larger result is refused with `result_too_large`. Use
`max_lines` or `view_io` instead.

## 6. Control lease

One lease exists per daemon (per device). It is the right to run gated tools.
Connections are not leases; a client may be connected without the lease.

Lease object:

```text
{"state": "free" | "active" | "idle" | "closing",
 "holder": null | {"client_id":"c3","name":"…","role":"mcp","pid":1234},
 "expires_in": <secs> | null,     # seconds until idle expiry; null unless idle
 "active_operations": n,
 "generation": n,                 # increments on every grant
 "you": "holder" | "reacquire_required" | "none"}
```

State meanings:

- `free`: nobody holds the lease.
- `active`: the holder has gated operations in flight. It never expires.
- `idle`: the holder has none in flight; `expires_in` counts down from
  `LEASE_IDLE_SECONDS` (60 s).
- `closing`: the holder disconnected or released while an operation was still in
  flight. The lease is held until that operation settles, then released. Other
  clients see `busy` throughout.

Rules:

1. **Auto-acquire on first use.** A gated call from a client that has never lost
   or released the lease is granted the lease if it is free.
2. **Renewal.** Each gated operation counts as active from start to finish.
   The idle timer starts when the last in-flight operation finishes (and when a
   lease is granted or explicitly acquired). Only gated operations renew.
   `status`, `lease status`, observation (`subscribe`), `view_io`, and the other
   ungated tools never renew.
3. **Expiry.** After `LEASE_IDLE_SECONDS` of idle time the lease becomes free.
   The former holder is flagged `reacquire_required` and receives a
   `lease_lost` event with reason `expired`.
4. **Conflict.** A gated call or `lease acquire` from another client while the
   lease is held (any state) returns `busy` with the lease object in
   `error.lease`. There is no queue; the caller decides when to retry.
5. **Explicit acquire.** `lease acquire` grants a free lease, renews the
   caller's own lease, or returns `busy`. A `reacquire_required` client must
   use it. When the caller is re-granting after a loss or release, the result
   includes `warning: "control was lost or released earlier; device state may
   have changed since you last used it"`.
6. **No silent resumption.** A `reacquire_required` client that calls a gated
   tool without first acquiring gets `lease_lost`. This happens even if the
   lease is currently free. A client becomes `reacquire_required` after it
   loses the lease by expiry or explicit release.
7. **Release.** `lease release` by the holder frees the lease once its in-flight
   operations finish (state `closing` in between) and sets `reacquire_required`
   on the caller. Release by a non-holder is a no-op that still reports the
   current lease.
8. **Disconnect.** A holder that disconnects releases the lease immediately if it
   has no operations in flight, otherwise when its last operation settles.
   Neither event closes the device, the session surfaces, or any other client's
   connection.
9. The lease never closes the device or the connection. `disconnect` is a gated
   tool: the controller may ask the daemon to close its selected device or
   session attachment, and the daemon keeps running.

Error objects for lease conflicts:

- `busy`: `error.lease` contains the lease object.
- `lease_lost`: `error.lease` contains the lease object, and the message names
  the reason (`expired` or `released`).

## 7. Events

Events are pushed without an `id`.

| event | fields | sent when |
|---|---|---|
| `output` | `seq` (int, stream byte offset of the first byte), `data` (base64 of raw bytes, ≤ 16 KiB decoded) | a subscribed client's device output chunk |
| `output_gap` | `dropped_bytes` (int), `seq` (int, offset where output resumes) | the client's outbound output queue overflowed (section 8) |
| `lease_lost` | `reason` (`expired` or `released`), `warning` (string) | the client lost the lease |
| `device` | `state` (`connected` or `disconnected`), `label` (string or null) | a connect/attach/disconnect call completed |
| `shutdown` | `reason` (string) | the daemon is shutting down; the connection closes next |

Output is delivered only after `subscribe`. Output delivery is independent of
the lease, of `read_output`, and of which client is controlling the device:
observers never consume the controller's unread output, and the controller's
`read_output` is unaffected by subscribers.

`seq` values are the daemon's device-receive byte offsets since the daemon
started. A client that reconnects can see the gap through `output_gap` or by
comparing `seq` values.

## 8. Backpressure and slow clients

- Output for each subscriber sits in a bounded queue of `MAX_SUBSCRIBER_QUEUE_BYTES`
  (256 KiB). The device reader never blocks on a subscriber. When the queue
  overflows, the oldest queued output is dropped and counted; the next delivered
  event is `output_gap` with the number of dropped bytes.
- Responses and other control events are never dropped. At most
  `MAX_INFLIGHT_CALLS_PER_CONNECTION` (8) calls run at once per connection; the
  next call returns `overloaded`.
- A socket write that stalls for more than `SLOW_CONSUMER_TIMEOUT_SECONDS`
  (5 s) closes that connection. Its lease and client registration are released
  as in section 6 rule 8. Other clients are unaffected.
- A stalled reader on the client side therefore causes gaps for itself only.

## 9. Human input and the human path

A client with role `human` may send `input` frames. Each frame is written to the
device's transport immediately, in order per connection:

- it is **not** gated by the lease and does not wait for in-flight operations;
- it is written under the device write lock only, which is held briefly, so it
  never waits for an AI command to finish;
- it is forwarded even while a file transfer is in progress. **This is
  deliberate and unrestricted.** Pre-transfer quiescence confirmations
  (`confirm_shared_quiescence`) remain a request to the operator and do not
  enforce that a human stays idle. Leases do not arbitrate raw human input.

Human clients count as live clients for daemon lifetime. A connected human keeps
the daemon alive. Device output reaches a subscribed human client as `output`
events regardless of AI reads.

Security note: the `human` role is not a privilege separation between programs
running as the same user. Any such process can connect and send `input`. Ungated
raw input is the intended behaviour, not an access-control boundary.

The raw session socket and companion pty (tio protocol) of a daemon-owned
device remain available as before. Their bytes are not covered by lease
arbitration. Do not describe them as protected.

## 10. Error codes

| code | meaning |
|---|---|
| `unsupported_version` | `v` is not 1 |
| `hello_required` | an op came before `hello` |
| `already_hello` | a second `hello` |
| `bad_request` | frame is not a JSON object, or a field has the wrong type |
| `frame_too_large` | line exceeds `MAX_FRAME_BYTES`; the connection closes |
| `unknown_op` | unknown `op` |
| `unknown_tool` | unknown `call.tool` |
| `bad_args` | arguments do not bind to the tool |
| `forbidden_role` | op not allowed for this role (`input` from non-human) |
| `busy` | lease held by another client (section 6) |
| `lease_lost` | the caller must explicitly reacquire (section 6) |
| `not_connected` | `input` with no device connected |
| `too_large` | `input` data over 64 KiB |
| `result_too_large` | the result does not fit one 4 MiB response frame after JSON encoding |
| `overloaded` | too many concurrent calls on this connection |
| `shutting_down` | the daemon is draining and rejects new work |
| `internal` | unexpected daemon error; details are logged, not sent |

## 11. Constants (protocol v1)

| name | value |
|---|---|
| `MAX_FRAME_BYTES` | 4 MiB |
| `HELLO_TIMEOUT_SECONDS` | 5 |
| `LEASE_IDLE_SECONDS` | 60 |
| `GRACE_SECONDS` | 30 |
| `DRAIN_TIMEOUT_SECONDS` | 5 |
| `START_TIMEOUT_SECONDS` | 10 |
| `START_GRACE_SECONDS` | 2 |
| `LOCK_PROBE_WAIT_SECONDS` | 0.5 |
| `SLOW_CONSUMER_TIMEOUT_SECONDS` | 5 |
| `MAX_SUBSCRIBER_QUEUE_BYTES` | 256 KiB |
| `MAX_OUTPUT_EVENT_BYTES` | 16 KiB |
| `MAX_INFLIGHT_CALLS_PER_CONNECTION` | 8 |
| `MAX_HUMAN_INPUT_BYTES` | 64 KiB |

## 12. Synthetic-client conformance

The repository test suite (`test_daemon.py`, `test_daemon_start.py`, `test_ipc.py`) drives the daemon with synthetic
clients over the real socket, using fake devices (ptys). It covers: concurrent
auto-start, socket and directory permissions, stale socket recovery, hello
handling, unsupported versions, frame limits, tool dispatch and argument
binding, lease acquire/renew/expire/conflict/reacquire/release, client exit with
an in-flight operation, zero-client grace and cancellation, independent output
consumption, observer non-renewal, slow-subscriber gaps, ungated human input
during a gated operation, human keep-alive, and explicit device disconnect
without daemon shutdown. Frontend tasks should reproduce these behaviours
against their own clients.
