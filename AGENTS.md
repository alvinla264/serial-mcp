# AGENTS.md

Guidance for AI agents (and humans) working in this repository.

## What this project is

`serial-mcp` is a generic serial-transport MCP server. It makes **no** assumptions
about the device on the other end — no shell, login flow, prompt style, or command
language is built in. Device-specific knowledge lives in *profiles*, never in code.

Core design point: **the AI opens the serial device and the human attaches to it.**
The server holds the port, starts a background reader, and serves two mirrored
surfaces named after the device:

- a UNIX socket speaking tio's raw-byte `--socket` protocol
  (`/tmp/serial-mcp-<dev>.sock`), and
- a companion pty symlink (`/tmp/serial-mcp-<dev>`) so real tty tools (tio,
  screen, minicom, cat) can attach as if it were a normal serial device.

Both directions are bridged: AI writes, human writes, and device output all flow
to every party.

## Repository layout

| Path | Purpose |
|---|---|
| `server.py` | The entire server: MCP tools, serial handling, session sockets, bootloader entry logic. Single module by design. |
| `test_server.py` | Pytest suite. Uses fake ptys / fake sockets — never a real device. |
| `README.md` | User-facing documentation: install, agent configs, tool reference, protocol notes. |
| `pyproject.toml` | Packaging + `serial-mcp` console entry point (enables `uvx --from git+...`). |
| `flake.nix` | Nix flake (`nix run github:...`). |
| `AGENTS.md` | This file. |

## Hard rules

1. **NEVER commit sensitive information.** No exceptions, no "it's just a test
   fixture", no "it's already in the chat so it doesn't matter". This covers:

   - **Credentials of any kind**: passwords, passphrases, PINs, API keys, tokens,
     PATs, private keys, certificates, session cookies, `~/.netrc` entries,
     cloud credentials.
   - **Device identity**: MAC addresses, serial numbers, IMEI/ESN, device IDs,
     UUIDs, hostnames, FQDNs, SSIDs, BSSIDs, board/serial-tag values.
   - **Network identity**: IP addresses, subnets, VPN endpoints, internal domain
     names, URLs containing credentials or tokens.
   - **Personal/organisational data**: personal names, personal or corporate email
     addresses (including in commit author/committer fields), customer or account
     identifiers, ticket numbers that leak internals.
   - **Raw transcript content**: captured console output, `view_io` dumps, serial
     logs, or boot logs containing any of the above.

   Where such values genuinely belong:

   - Credentials → `~/.config/serial-mcp/credentials.json` (mode `0600`, outside the
     repo), referenced from profiles via `login.password_ref`. Never inline them.
   - Device/network specifics → user profiles in `~/.config/serial-mcp/profiles.json`,
     also outside the repo and never tracked.
   - Tests → synthetic placeholders only (`test-device login:`, `sekrit`), never real
     values from a device you touched.
   - Commit identity → a personal address, not a corporate one, if the repo is public.

   **Before every commit**: grep the staged diff for secrets, MACs, IPs, hostnames,
   and device model names. If something sensitive was committed, treat it as
   compromised: rotate the credential, then rewrite history — deleting the file in a
   later commit is *not* sufficient, the value stays in history.

2. **Keep the server device-agnostic.** Do not hardcode a vendor, model, prompt, or
   command. If a new device needs behaviour, add a profile, not a code branch.
3. **Never fabricate a "success".** Detection logic must not report success on
   ambiguous evidence (see "Bootloader entry" below — a bootloader banner prints on
   *every* boot and proves nothing).
4. **Bound every loop.** Any retry/interrupt loop needs a hard attempt cap *and* a
   wall-clock timeout. Report and stop; never spin forever.
5. **Tests must not touch real hardware.** Use ptys and fake sockets like the
   existing suite. Keep tests deterministic — no sleeps that race.

## Development

```bash
# run the server from the working tree
uv run --with fastmcp,pyserial server.py

# tests (both environments must pass)
uv run --with fastmcp,pyserial,pytest python -m pytest test_server.py -q
nix develop -c python -m pytest test_server.py -q
```

Note: `fastmcp` 2.x exposes `mcp.get_tools()`, 3.x+ exposes `mcp.list_tools()`.
Keep tests compatible with both (see `test_no_direct_attach_tools_exposed`).

## Bootloader entry

`enter_bootloader` performs: login (if needed) → reboot → paced interrupt tapping →
success detection. Things learned the hard way, worth preserving:

- **Tap *through* the boot**, starting before the autoboot window opens. Waiting for
  the banner first loses short windows (this device's window is ~1s, and U-Boot
  prints the prompt then continues immediately).
- **Pace the taps** (tens of ms apart). Flooding a U-Boot console can overflow its
  RX buffer and *drop* interrupt characters.
- **The banner is not success.** `U-Boot 2016.01 ...` prints on every boot. Only an
  interactive prompt counts, confirmed by output actually *pausing* and no
  Linux-resume signal afterward.
- **Shutdown text is not "Linux resumed."** Messages like `uloop_done` or
  `app-classifier stop` appear *before* reboot; treating them as an abort stops
  tapping seconds before the window opens.
- **`login_mode="blind"`** exists because flooded consoles make prompt detection
  read noise. Blind mode fires Enter → username → password on a schedule and
  assumes delivery, like a human would.

## Commit messages — Conventional Commits

This repository follows [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).
All agent-authored commits **must** use it.

### Format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Structure rules:

- The message **must** start with a type, an optional scope in parentheses, an
  optional `!` for breaking changes, then a colon and a space.
- The description is a short summary, lowercase after the colon, no trailing period.
- The body (optional) begins **one blank line** after the description, and may be
  any number of newline-separated paragraphs.
- Footers (optional) come one blank line after the body, as `token: value` or
  `token #value` (git trailer style). Use `-` instead of spaces in tokens
  (`Refs:`, `Reviewed-by:`).

### Types

| Type | Use for |
|---|---|
| `feat` | A new feature (SemVer MINOR) |
| `fix` | A bug fix (SemVer PATCH) |
| `docs` | Documentation only |
| `test` | Adding or fixing tests |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvement |
| `build` | Build system, packaging, dependencies |
| `ci` | CI configuration |
| `chore` | Maintenance that doesn't fit the above |

### Breaking changes

Mark with `!` after the type/scope, and/or a `BREAKING CHANGE:` footer:

```
feat(api)!: drop support for the legacy session protocol

BREAKING CHANGE: tools now require the companion pty; raw socket framing is gone.
```

### Examples

```
feat(bootloader): add automated U-Boot entry with device profiles
```

```
fix(bootloader): stop reporting success on the bootloader banner

The banner prints on every boot, so matching it proved nothing and
reported false success while the device booted to Linux. Only an
interactive prompt now counts, confirmed by a pause in output.
```

```
docs: document the conventional commit format for agents

Refs: #12
```

```
test: cover login-mode fallback when prompt detection reads noise
```

### Scope

Use a short noun naming the area of the codebase: `bootloader`, `session`, `login`,
`profiles`, `tools`, `tests`, `docs`, `packaging`. Scopes are optional but
encouraged for anything touching the larger subsystems.

### Agent checklist before committing

1. Both test environments pass (`uv` and `nix`).
2. No secrets, device identifiers, or real-hardware values in the diff or fixtures.
3. Message follows the format above; body explains *why* when the change is
   non-obvious.
4. Breaking changes are flagged with `!` and/or `BREAKING CHANGE:`.
