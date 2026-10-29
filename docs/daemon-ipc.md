# Shared daemon IPC contract (protocol v2)

Status: **frozen** for protocol version 2. This is a breaking replacement for
v1; peers must reject any other version with `unsupported_version` and close the
connection. V1 single-device semantics are never inferred or adapted. The Rust MCP frontend and native Rust human terminal negotiate v2.

The daemon is per-user and owns zero or more physical serial connections. Its
stdlib-only local UNIX IPC is shared by the MCP and human clients. The daemon
must not import an MCP framework. Device-specific tools explicitly take `port`.

## Framing, endpoint and lifecycle

Use the private runtime directory and UNIX socket/lock rules from the previous
contract: `$SERIAL_MCP_RUNTIME_DIR`, `$XDG_RUNTIME_DIR/serial-mcp`, or
`/tmp/serial-mcp-<uid>`; directory 0700, socket 0600, same-uid SO_PEERCRED,
exclusive daemon lock, bounded LF-delimited UTF-8 JSON frames (4 MiB maximum).
Every request/response/event has `v:2`; requests have unique outstanding `id`.
Frames are `{v,id,ok:true,result:{...}}`, `{v,id,ok:false,error:{code,message,...}}`,
or unsolicited `{v,event,...}`. Unknown additive result fields/event kinds are
ignored. A well-formed unsupported-version frame receives `unsupported_version`
and closes. Errors never contain private controller identity.

A client becomes registered only after `hello`; idle registered MCP and human
clients count in `status.clients`, while `status.mcp_clients` counts only MCP
clients. EOF/crash unregisters immediately. Zero
registered clients starts the 30-second shutdown grace; a new hello cancels it.
The daemon closes devices only after memberships leave and safe operations settle.
A disconnected client leaves every device. Registration alone holds no device.

## Identity and selectors

`port` is the caller's explicit port path string, required for every
port-specific operation. On Linux the daemon resolves a physical identity by
`stat(port)` and the character-device `st_rdev` major/minor tuple. Symlinks and
aliases resolving to that tuple refer to one `Device`; they cannot create a
second open, settings record, or lease. Unresolvable/non-character paths fail
`invalid_port`. Result `port` is the selected caller spelling; `device_id` is a
stable daemon-local opaque key, not a serial number. It may be used to associate
events/cursors within the daemon lifetime, not as a global identity.

## Requests

First request is `hello`:

- `hello {role:"mcp"|"human", name:string<=64, pid?:int, client_version?:string}`
  → `{protocol:2,daemon_version,client_id,role,clients,mcp_clients,devices:[snapshot...]}`.
  `clients` is the all-role registered-client total; `mcp_clients` counts only
  registered MCP clients, including idle MCP clients and excluding human
  terminals. These are totals only. `client_id` is private to that caller's
  handshake; never disclose other client names, IDs, or membership mappings.
  `mcp_clients` is an additive v2 field; a human terminal that requires it must
  fail clearly against an older v2 daemon rather than mislabel the total.
- `status {}` → `{protocol,daemon_version,clients,mcp_clients,grace,shutting_down,devices}`.
  `clients` counts all registered MCP and human clients; `mcp_clients` counts
  registered MCP clients only, including idle clients.
  Each device snapshot is `{device_id,port,state:"connected"|"disconnected"|"closing",
  baudrate,control:"available"|"held",members:n,generation}`. A vanished tty is
  reported as `disconnected`; a later `connect` on the same selector reopens it
  before returning success. No controller identity is exposed. `clients` counts
  all registered clients, including clients with no memberships; `mcp_clients`
  is the MCP-only count. `members` is the number of AI/human participants joined
  to that device.
- `call {tool,args}` invokes an approved engine tool. Device-affecting tools
  require `args.port`; `connect` also requires `baudrate`. `connect(port,baudrate)`
  resolves/opens or joins the physical device and requests control atomically.
  First opener's baud is authoritative; later requests never reconfigure. It
  returns `{result,device_id,port,baudrate,control:"granted"|"not_granted",
  lease_remaining}`. Existing controller renews; a different controller's
  connect joins as an observer and succeeds with `control:not_granted`, actual
  baud and remaining lease. Observers remain members until `disconnect(port)` or
  client exit. A disconnected port is opened only once, by first joiner.
- `disconnect {port}` leaves this caller's membership. If caller holds control,
  active operation settles before release. Other members/connections are kept.
  Last member departure marks closing, safely settles and closes the physical
  port; joins racing close wait up to the bounded drain interval, then either
  join the retained device or create a fresh one after close. A drain timeout
  returns `busy`, and the caller may retry. Reopen selects a new baud.
- `lease {action:"status",port}` is an internal status operation only and
  returns `{state,expires_in,active_operations,generation,you}` without identity.
  Public lease acquire/release is removed. Control can be acquired/reacquired
  only by `connect`.
- `subscribe {port}` / `unsubscribe {port}` controls that client's output stream.
  Each client has its own cursor per device. Subscribe returns
  `{subscribed:true,device_id,cursor}`; a newly subscribed client receives bytes
  after that cursor only. `read_output(port,...)` reads from an independent
  per-client cursor; it never consumes another client's bytes and never renews
  control. A stream event includes `device_id,port,seq,generation,data` (base64).
  `output_gap {device_id,dropped_bytes,seq}` explicitly signals bounded queue loss.
- `input {port,data}` is human-role only, writes raw bytes immediately to the
  selected connected device independent of AI membership/control/operations.
  It never takes or renews a lease and returns `{written,device_id}`. `join
  {port,baudrate?}` and `leave {port}` are human-role internal operations. Join
  opens if absent (baud required then),
  otherwise joins at actual baud; neither grants AI control. Leave removes only
  that human membership. `human_set_baudrate {port,baudrate}` is human-role-only,
  requires membership, validates 1–4,000,000, and changes the shared transport
  immediately without acquiring or renewing AI control. It broadcasts the normal
  `settings` event and may disrupt concurrent AI work; clients must confirm that
  shared impact before sending it.
- `ports {}` is human-role-only and returns `{result:<engine port listing>}` for
  selection; it does not open a port. `bye {}` responds then closes.

AI tools include `send_command`, `read_output`, `clear_output`,
`set_prompt_pattern`, `login`, `enter_bootloader`, `upload_file`,
`download_file`, `view_io`, `set_baudrate`, and `file_transfer_capabilities`.
`connect`, `disconnect`, and `set_baudrate` have daemon-owned semantics above.
Legacy attach/tio/session-socket controls, `tio_info`, and public lease
acquire/release/status are not exposed in the shared application. The MCP role
exposes the 16-tool catalog in `rust-client/tools.json`. Device-scoped tools
require explicit `port`; `file_transfer_capabilities`, `list_serial_ports`, and
`list_boot_profiles` are global and require no device membership or lease.
`set_boot_credentials` is also global, MCP-only, and writes only to the protected
user credential store (mode 0600); secrets are never returned or logged. Global
tools reject unexpected arguments. Tools not defined here return `unknown_tool`.

## Permissions, lease, operation concurrency

Every AI tool except a read-only watch/status explicitly names a joined `port`.
`send_command`, clear, prompt/login/bootloader, and file transfers are modifying
and require this caller to be current controller. `connect` is the only acquire
and reacquire path. A busy connect still joins as observer. Other modifying
calls from nonowners return `control_required` (or `busy` if held elsewhere),
with instruction to call `connect` explicitly. No implicit acquisition,
takeover, queued work, or replay after unknown outcome. Server checks every
modification. Reads/watch do not renew the lease.

Per-device AI control is independently leased for 60 seconds idle. Idle timing
starts after the last active operation settles. Active work prevents expiry;
expiry releases control but preserves membership and emits `control_lost` to
that client. The former controller becomes observer; no automatic takeover.
Disconnect/client crash releases control after active work safely settles.
Human input is unrestricted and can interleave with AI operations.

`set_baudrate(port,baudrate)` is controller-only, applies immediately even while
an operation/transfer is active, uses a narrow driver lock (not the whole
operation lock), and succeeds only if the driver accepts it. No probe bytes or
login/reboot. On failure preserve/report coherent actual settings; on accepted
change increment `generation`, broadcast `settings {device_id,port,baudrate,
 generation}`, and directly confirm to caller. Every in-flight call that spans a
generation change returns an explicit warning that output/outcome may have been
affected; do not infer success or failure from the warning.

## Events, cursors and backpressure

Events: `settings` (accepted baud change), `settings_gap` (coalesced/evicted
settings), and `device` with `state` equal to `open` (physical tty opened or
reopened), `joined` / `left` (membership changed), `disconnected` (physical tty
vanished), or `closed` (physical tty close completed). Device events carry
snapshot-safe `device_id`, `port`, `baudrate`, and `generation` fields. `output`,
`output_gap`, `control_lost`, `clients` (all-role `clients` and MCP-only
`mcp_clients` counts only), `event_gap` (local client event queue overflow), and
`shutdown` are also emitted. These names describe the
wire states; `connected` is a status snapshot value, not a device-event state.
Events are not authorization. MCP clients retain
settings notices in a bounded per-device queue and prepend notices to the next
response for that device; TUI updates its view promptly. No model-wakeup promise.
Queues are bounded; the server caps pending control responses at 64 per client
(closing a non-reading client on overflow), retains at most 32 pending per-device
settings notices (emitting `settings_gap` when coalescing or evicting), and the
stdlib client's local event queue is capped at 256 events with `event_gap` on
overflow. Other frontends must likewise emit a visible coalesced/dropped notice
or explicit gap, never silently claim complete output/settings history. Output
sequence offsets are per physical device and monotonic for its open lifetime;
each client cursor is independent. `generation` increases on accepted settings
changes and new physical opens. A cursor is scoped to `(device_id,generation)`;
reopen invalidates prior-generation cursor explicitly.

## Error codes and unknown outcomes

`hello_required`, `already_hello`, `bad_request`, `unsupported_version`,
`frame_too_large`, `unknown_op`, `unknown_tool`, `invalid_port`, `not_connected`,
`not_member`, `control_required`, `busy`, `overloaded`, `too_large`,
`result_too_large`, `forbidden_role`, `shutting_down`, `bad_args`, `internal`,
`client_closed`, and `baudrate_rejected` (includes actual `baudrate`).
Error result is `{code,message,...}`; no controller identity is returned.
A transport failure after a modifying request has been accepted but before its
response is received has **unknown outcome**. Clients must not replay. They may
reconnect, inspect status/output, and explicitly call `connect(port,baudrate)`
to regain control. On socket EOF/crash daemon membership cleanup follows the
same safe-settlement/last-member-close rules as `disconnect`.

This contract intentionally breaks v1. The Rust shared-app MCP frontend now
negotiates v2 and requires explicit `port` arguments for device-scoped tools. Its
catalog exposes `set_baudrate` and removes `attach`, `tio_info`, and the three
public lease tools. It never creates a raw tio socket or companion pty. The
native human terminal uses the human membership/input surface, not MCP control.
The Python parity oracle is likewise
a legacy-only v1 adapter outside the supported shared-app surface.
