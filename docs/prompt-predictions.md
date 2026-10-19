# Prompt-bar predictions

The native Claude backend accepts `thread/fork` with
`experimentalPredictionMode: true`, `ephemeral: true`, and optional
`excludeTurns` and `threadSource` fields. The parent must be loaded, persisted,
and idle. Configuration overrides and history-boundary overrides are rejected.
ACP agents do not support this mode.

A prediction uses an exact native transcript fork, the parent's model and
instructions, and the requested turn effort. It permits one turn. Built-in
tools, MCP servers, desktop tools, plugin runtime startup, native hooks, and
automatic memory are disabled for that fork. It does not save a new chat or
change the parent's conversation. Unsubscribing removes the temporary record.

## Desktop compatibility

The inspected desktop build `26.1013.40934` (`20770`) blocks prompt predictions
unless the parent provider is `openai`, before sending any request to Claudex.
Its `cumberbatch` controller gets configuration from
`/wham/predictions/config`, then requests a prediction fork and starts its turn.
This path uses the current chat's model. Separate ambient-suggestion and title
jobs can use Luna.

Native Claude `thread/resume` replies expose the facade-specific boolean
`claudexPredictionFork`. On these replies, the combined desktop router aliases
the top-level `modelProvider` to `openai`, satisfying the existing desktop's
prediction gate. This is a transport compatibility value: the nested thread
provider, stored metadata, model selection, and actual execution remain Claude.
If the desktop echoes the alias on a later request, the router restores the
actual provider after selecting the thread's backend. ACP replies without the
capability keep their provider unchanged.

The router uses genuine Codex's initialization/version response, so its desktop
version gate reflects the installed Codex executable. The direct facade keeps
its baseline protocol version and native provider; use the combined router for
predictions. Plan-mode exclusion, unsupported-model filtering, prediction
settings, and remote feature configuration retain their existing behavior.

Install or upgrade Claudex normally and reconnect the desktop's app-server
connection to load the updated router. The ChatGPT app bundle and signature
are unchanged.

## Verification

`tests/history_wire.rs` covers the desktop fork request, inherited model/history
anchor, tool-free launch, temporary cleanup, parent isolation, and rejection of
overrides and unsupported parents. `tests/router.rs` covers the compatibility
provider, its echoed value, preserved thread metadata, and the routed prediction
fork/turn/unsubscribe transaction.

The native test makes real Claude requests and requires a normal authenticated
Claude installation:

```sh
mbx test --locked --features test-backend --test history_wire \
  native_prediction_fork_uses_history_without_tools_or_hooks -- --ignored --nocapture
```

It verifies real transcript recall, absence of tool and hook side effects, and
continued parent recall. A successful protocol probe does not by itself prove
that the installed desktop renders its prediction.
