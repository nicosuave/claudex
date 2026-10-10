# Codex desktop through a dedicated localhost SSH connection

The macOS installer copies the facade to a stable per-user location and creates a
login-session LaunchAgent, a dedicated localhost SSH identity, and the
`claude-codex-local` SSH alias. Claude runs in the GUI login session, where its
normal authentication is available. SSH transports the desktop connection.

## Install

You need macOS, Codex desktop, and an authenticated
Claude Code CLI supporting `--system-prompt-snapshot` (tested with 2.1.294).
Run `claude auth login` in a local terminal if necessary. Enable **Remote Login**
for your user in **System Settings > General > Sharing**. The installer does not
enable Remote Login or change system settings. Run these commands as your normal
login user, without `sudo`:

```sh
brew install nicosuave/tap/claudex
claudex install
```

Homebrew installs the signed, notarized release binary; Rust is not required.
`claudex install` sets up the desktop service and connection and checks readiness.
For optional diagnostics afterward, run `claudex doctor`; it does not install or
upgrade anything.
You can also unpack the matching archive from
[GitHub Releases](https://github.com/nicosuave/claudex/releases), verify its
`.sha256` checksum, and run `./claudex install`.

To build from source instead, install Rust with edition 2024 support and run:

```sh
cargo build --locked --release --bin claude-codex-server
./target/release/claude-codex-server install
```

Use `mbx build` in place of `cargo build` when Boxington is installed.
After installation, the checkout can move: the installed binary lives at
`~/Library/Application Support/claude-codex/bin/claude-codex-server`.

The installer resolves `claude` and genuine `codex` from PATH, with a fallback to
`/Applications/Codex.app/Contents/Resources/codex` for Codex. Use `--claude
/absolute/path/to/claude` when needed. `CLAUDE_CODEX_PLUGIN_EXECUTABLE` selects a
particular genuine Codex binary; `CLAUDE_CODEX_PLUGIN_HOME` selects its existing
home, normally `~/.codex`. These paths are recorded in the dedicated launchers.
The facade does not copy credentials or replace either CLI.

In **Codex Settings > Connections**, add or select **claude-codex-local**. Refresh
the picker if it was already open. Choose identity-file authentication and select
`~/Library/Application Support/claude-codex/id_ed25519` (the installer prints the
expanded path). Port is **22**; the display name can be **Claude**. Select this
connection for your project/chat and choose a Claude model, such as `opus`.
Do not substitute plain `localhost`, which can select your ordinary SSH identity.

Use **Workspace write** and **Approve for me** for native Claude auto review.
Bash uses Claude's OS sandbox; native file tools use scoped permissions. Explicit
sandbox escapes use the selected automatic or manual reviewer. MCP integrations
and hooks remain trusted services outside that sandbox. Full access and manual approval modes are also
available; read-only profiles are unsupported.

### Development sandbox settings

To allow routine network access and writes to build caches, create
`~/Library/Application Support/claude-codex/state/workspace-defaults.json`:

```json
{
  "network_access": true,
  "writable_roots": ["/absolute/path/to/cache"]
}
```

For a custom `--state-dir`, place the file there instead. Roots must be existing
absolute directories; `~` is not expanded. The service reads this file at startup
and reports its grants through `config/read`. Restart the idle service and refresh
the desktop connection after editing it. New chats and explicit selections of the
Workspace preset use these defaults. Existing chats retain their saved policy
until you reselect the preset; explicit `sandboxPolicy` requests always win,
including `networkAccess: false`. Without this file, the restrictive defaults stay
unchanged. Do not put this host configuration in a repository's Claude settings.

In your user Claude settings (`~/.claude/settings.json`), these native options can
remove common development friction:

```json
{
  "sandbox": {
    "allowUnsandboxedCommands": true,
    "enableWeakerNetworkIsolation": true,
    "network": { "allowLocalBinding": true }
  }
}
```

Unsandboxed retries use native automatic review under **Approve for me** and
desktop approval under **Ask for approval**; an existing `false` prohibits them.
Explicit native ask/deny rules remain authoritative. On macOS,
`enableWeakerNetworkIsolation` permits access to the system TLS
trust service for Go tools such as `gh`; it does not disable certificate checking.
This opens access to that service beyond the network proxy boundary. Local binding
is honored only when the selected workspace policy grants network access. Unix
sockets and command exclusions remain restricted. Existing native deny rules are
preserved. Each new turn reloads native settings; changing them does not require
a service restart.

Native skills and nested instructions load normally in workspace sessions. Direct
file tools can edit authorized workspace files automatically, but outside and
protected destinations are blocked. For those changes, Claude must request an
explicit Bash escape, which uses the selected reviewer (or is denied by never
ask). Callback handling failures stop the native process instead of allowing the
write. See [native loading and permissions](docs/native-skill-settings-boundary.md).

On macOS, the facade repairs Claude's Git-over-SSH proxy authentication through a
native session environment hook. Git SSH traffic stays inside the sandbox and
uses its authenticated proxy; network-disabled policies still block it. SSH keys
and host-key checks remain native. Plain `ssh` can still need a reviewed
unsandboxed retry. Codex can permit native TCP directly when
network access is enabled without a managed proxy. The two runtimes do not have
identical network enforcement or classifier decisions.

If native auto review denies an action, explicit subsequent authorization in chat
(such as "I allow that" in response to the denial) tells Claude to retry that same
action once through native review. It does not automatically allow execution or
change reviewer mode. If the reviewer still denies it, **Ask for approval** in
that chat provides the native manual review path. A terminal classifier denial
does not itself create an approval callback. Do not add a global allow rule to
recover one action; explicit deny rules and managed restrictions still apply.

## Manage the service

Use the installed executable after moving or removing the build checkout:

```sh
facade="$HOME/Library/Application Support/claude-codex/bin/claude-codex-server"
"$facade" doctor
"$facade" service status
"$facade" service stop
"$facade" service start
"$facade" service restart
```

`doctor` checks native executable compatibility, the private WebSocket handshake,
the dedicated SSH route, and Claude's authentication status. It does not make a
model request. `service status` prints installed, loaded, and ready separately.
The service label is `com.claude-codex.desktop`; logs are in
`~/Library/Application Support/claude-codex/logs/server.log`.

To upgrade, run `brew upgrade nicosuave/tap/claudex`, then `claudex install`.
Homebrew updates the command-line tool; the second command refreshes the separate
stable binary used by the desktop service. Both steps are currently required.
For a source installation, build the new source and run its `install` command again. The
installer copies the binary and refreshes the service, preserving conversation
state, the dedicated identity, and unrelated SSH entries. Wait for active chats
to finish first. Stop, restart, and uninstall refuse active saved turns unless
you explicitly pass `--force`; forcing interrupts those turns. SSH transport
reconnection preserves live turns, but restarting the facade process does not.

```sh
"$facade" service uninstall  # Remove only the LaunchAgent; keep the SSH connection.
"$facade" uninstall          # Remove the LaunchAgent and managed SSH entries.
```

Both retain conversations, binaries, logs, and identity files. Full uninstall
removes only the exact dedicated key authorization, not other authorized keys.
Existing SSH config symlinks and file permissions are preserved. Legacy shell
entry points delegate to the native installer/service manager; preparation-only
mode is no longer available.

## Installation boundary

All instance files are under `~/Library/Application Support/claude-codex`, except
its LaunchAgent in `~/Library/LaunchAgents` and the managed SSH entries in
`~/.ssh/config` and `~/.ssh/authorized_keys`. The alias is prepended as a literal
Host block so desktop discovery can find it. It restricts identity selection to
the dedicated key and pins the localhost SSH host key. The authorized key accepts
only loopback connections, disables forwarding and PTYs, and uses a forced
launcher. It can still execute commands as your user; it is not a separate OS
security principal.

| Environment variable | Dedicated value |
| --- | --- |
| `CODEX_INSTALL_DIR` | Instance `bin` directory |
| `CODEX_HOME` | Instance `codex-home` directory |
| `CLAUDE_CODEX_HOME` | Instance `state` directory |
| `CLAUDE_CODEX_SOCKET_DIR` | Instance `sockets` directory |
| `CLAUDE_CODE_EXECUTABLE` | Resolved Claude CLI path |
| `CLAUDE_CODEX_PLUGIN_EXECUTABLE` | Resolved genuine Codex CLI path |
| `CLAUDE_CODEX_PLUGIN_HOME` | Existing Codex home |

Genuine Codex supplies plugin services from its existing registry and auth;
facade conversations use separate storage. The ordinary Codex connection and
shell profiles remain unchanged. `CODEX_SSH_SKIP_APP_SERVER_BOOT=true` makes SSH
use the already-running login-session service via `app-server proxy`.

## Verification and limitations

The protocol is pinned to Codex CLI 0.160.0. It is a supported subset, not complete
feature parity. See [COMPATIBILITY.md](COMPATIBILITY.md) and the
[desktop integration audit](docs/desktop-integration-audit.md). Native desktop
pixels/menu interaction have not been automatically verified. Transport/RPC
checks and opt-in live model probes cover separate parts of the integration.

These optional probes make real Claude model requests and archive their test chats:

```sh
mbx build --locked --example desktop_smoke --example reconnect_smoke
./target/debug/examples/desktop_smoke claude-codex-local opus
./target/debug/examples/reconnect_smoke claude-codex-local opus
```

The first checks model/folder discovery, desktop-tool exchange, saved-path resume,
timeline, host commands and queued native recall. The second reconnects during a
pending desktop tool, checks stable request identity and returns a fresh token.
For startup problems, inspect `doctor` and the instance log. Refresh the desktop
connection after an upgrade to clear cached host capability information.
