# Claudex

A Rust facade that exposes the Codex app-server thread/turn protocol over Claude Code's bidirectional `claude -p` transport. Clients send Codex JSON-RPC messages; Claude does the inference and tool execution.

The implementation targets the generated **Codex CLI 0.160.0** schema. The Claude transport was developed against **Claude Code 2.1.288** and the official Claude Agent SDK's subprocess/control implementation. No Node or Python bridge is required.

This is a working core implementation, **not complete Codex feature parity**. Unsupported methods return `-32601`; unsupported non-null options return `-32602`. See [COMPATIBILITY.md](COMPATIBILITY.md) for the exact surface and behavioral differences.

## macOS desktop installation

```sh
cargo build --locked --release --bin claude-codex-server
./target/release/claude-codex-server install
./target/release/claude-codex-server doctor
```

Enable Remote Login for your user and authenticate Claude first. The installer
copies the binary to a stable per-user location, creates its own LaunchAgent and
SSH connection, and checks service readiness. Select `claude-codex-local` in Codex
Settings > Connections. See [DESKTOP.md](DESKTOP.md) for prerequisites, upgrades,
service commands, uninstall behavior, and the precise local trust boundary.

## Build and run

Requires Rust with edition 2024 support and an installed, authenticated `claude` executable. Claude uses its normal configured authentication. The facade neither reads credentials nor changes login settings.

```sh
mbx build --locked --bin claude-codex-server
./target/debug/claude-codex-server app-server --stdio
```

Use `cargo build` if Boxington (`mbx`) is unavailable. The default model is `sonnet`; override it with `--model opus` or a Claude model identifier.

For a client that lets you configure its app-server command, set the executable to the built `claude-codex-server` binary and arguments to `app-server --stdio`. For a separate Codex desktop SSH connection, see [DESKTOP.md](DESKTOP.md). Desktop transport and startup compatibility are implemented; the actual desktop UI still needs a manual connection test.

Other transports:

```sh
./target/debug/claude-codex-server --listen ws://127.0.0.1:4510
./target/debug/claude-codex-server --listen unix:///tmp/claude-codex.sock
```

WebSockets accept loopback IP bindings only. All browser Origin headers are rejected. `unix://` carries WebSockets as Codex desktop expects; the previous newline JSON framing is available as `unix-lines:///absolute/path`. Unix sockets use mode `0600`; only stale sockets owned by the current user may be removed; live sockets, files, and symlinks are retained. All transports are intended for trusted local clients, with no network authentication layer. Logs go to stderr; stdout carries only protocol frames in stdio mode.

Useful options:

| Option | Purpose |
| --- | --- |
| `--claude /absolute/path/to/claude` | Backend executable; also `CLAUDE_CODE_EXECUTABLE` |
| `--state-dir /absolute/path` | Facade persistence; also `CLAUDE_CODEX_HOME` |
| `--model sonnet` | Default model alias or identifier |
| `--claude-arg=--safe-mode` | Forward an administrator-selected argument; repeat as needed |
| `--initialize-timeout-seconds 60` | Claude control handshake deadline |
| `--approval-timeout-seconds 300` | Deny unanswered permission requests after this much connected time |

State defaults to `~/.local/state/claude-codex-server`, independently of `CODEX_HOME`. Only one facade process may use a state directory. The facade stores its own thread/item history there, while Claude retains its own native session transcripts. Both are needed for durable conversation recall. Deleting facade metadata does not delete Claude's transcripts.

## Minimal client exchange

Stdio and `unix-lines://` use one JSON object per line. TCP and Unix WebSockets use one JSON object per text frame. As with Codex, outgoing messages omit `jsonrpc`. String and integer request IDs are preserved.

Send `initialize`, await its response, then send `initialized`:

```json
{"id":1,"method":"initialize","params":{"clientInfo":{"name":"my-client","version":"1"},"capabilities":{"experimentalApi":true}}}
{"method":"initialized"}
```

Start a thread, read `result.thread.id`, then substitute that ID into the turn request:

```json
{"id":2,"method":"thread/start","params":{"model":"sonnet","sandbox":"danger-full-access","approvalPolicy":"on-request"}}
{"id":3,"method":"turn/start","params":{"threadId":"<thread-id>","input":[{"type":"text","text":"Hello","text_elements":[]}]}}
```

The client receives `turn/started`, item start/delta/completion events, and exactly one `turn/completed` for an accepted turn. Responses and notifications can interleave. Wait for `turn/completed` before starting another turn on that thread; separate threads may run concurrently.

A permission request is a **server-initiated request** with its own ID. For commands and file changes, respond using the exact ID:

```json
{"id":"<approval-request-id>","result":{"decision":"accept"}}
```

The supported decisions are `accept`, `acceptForSession`, `decline`, and `cancel`. Session grants apply only to identical tool input and cwd within the same thread; broader permission amendments are rejected. Multi-select native questions use the installed desktop option-picker extension. See [COMPATIBILITY.md](COMPATIBILITY.md) for request and recovery semantics.

Workspace-write uses Claude's OS-enforced Bash sandbox and scoped file permissions; startup verifies the resolved native restrictions before sending user input. Desktop **Approve for me** selects native Claude auto review. Manual review and full access remain supported; `never` selects `dontAsk`, not permission bypass. Explicit unsandboxed retries require desktop approval. MCP integrations and hooks remain trusted services outside the Bash sandbox. Read-only/external profiles are rejected. The generated desktop launcher also enables genuine Codex plugin services using the existing plugin registry and authentication; see [plugin integration](docs/plugins-design.md).

## Verification

```sh
mbx test --locked --all-targets --features test-backend
mbx clippy --locked --all-targets --features test-backend -- -D warnings
cargo fmt --all -- --check
```

The deterministic suite drives the real facade executable with a fake Claude subprocess. It checks emitted responses, notifications, and approval requests against the vendored upstream schema, plus stream reconciliation, approvals, interruption, background continuations, restart, fork metadata, history pagination, and socket transports. The fake backend is available only with the `test-backend` feature.

Current verification results and live transport evidence are recorded in the [desktop integration audit](docs/desktop-integration-audit.md).

On a heavily loaded host, use `RUST_TEST_THREADS=1 TOKIO_WORKER_THREADS=2`. If the local `mbx` test launcher stalls, compile with `mbx test --no-run --all-targets --features test-backend`, then run the reported test executables directly; this preserves compiler caching.

The live smoke example exercises inference, recall after restarting the facade process, and a fork that remains anchored while the source conversation advances:

```sh
mbx build --locked --bin claude-codex-server --example smoke
./target/debug/examples/smoke ./target/debug/claude-codex-server
```

To also verify a real command approval, add `--exercise-approval`. The example approves only the exact command `printf FACADE_TOOL_OK`, denies other tool requests, and uses a temporary workspace with Claude safe mode. It makes real model calls and leaves native Claude session records in Claude's normal storage.

**Live verification status (October 8, 2026):** inference, recall after a server restart, fork snapshot isolation, and a real command approval all passed. The smoke example adds an explicit Bash ask rule so its harmless `printf` command requires a callback. The previous expired-OAuth blocker is resolved. Desktop UI behavior remains unverified because the computer-use tool does not allow access to the Codex app.

## Design and references

`transport.rs` owns framing and connections; `server.rs` owns RPC ordering, subscriptions, turns, and approvals; `backend.rs` owns Claude subprocess/control I/O; `translate.rs` owns item lifecycles; `store.rs` owns atomic facade persistence. A completed turn is published only after backend cleanup and process reaping. A Claude background-agent continuation is kept within its initiating Codex turn until Claude reports idle.

Primary references:

- [Codex app-server protocol](https://developers.openai.com/codex/app-server)
- [Codex source](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol)
- [Claude Agent SDK control implementation](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/_internal/query.py)
- [Claude Agent SDK subprocess implementation](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/_internal/transport/subprocess_cli.py)

The checked-in schema was generated with `codex app-server generate-json-schema --experimental` from the installed version, rather than fetched from a moving branch. See [NOTICE](NOTICE) for attribution.
