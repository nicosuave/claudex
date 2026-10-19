# Prompt-bar predictions

The native Claude backend accepts `thread/fork` with
`experimentalPredictionMode: true`, `ephemeral: true`, and optional
`excludeTurns` and `threadSource` fields. The parent must be loaded, persisted,
and idle. Configuration overrides and history-boundary overrides are rejected.
ACP agents do not support this mode.

A prediction uses Claude's native read-only `side_question` against the parent's
parked process. That preserves the parent's model, instructions, tools, effort,
and cached history prefix. Native Claude denies tool execution and permits one
response without adding the question to the parent's transcript or rebuilding
its cached prefix. No new Claude process or plugin runtime starts, and the
parent conversation is unchanged.
Identical prompts against the same parked parent share one pending or completed
request. Unsubscribing removes the temporary facade record.

Predictions are best effort and never fall back to a cold full-history request.
They require a live idle parent with matching settings and an unchanged history
anchor. The last parent request must have started within five minutes and have
at most 10,000 uncached or cache-write input tokens, matching native suggestions'
cold-prefix budget. The prediction prompt is limited to 8 KiB. Ineligible
requests return `{"suggestion":null}` without making a model call. Requests time
out after ten seconds; cancellation or a new parent turn cancels only the side
question. Steering, configuration overrides, and structured-output overrides
are unsupported. An optional prediction effort is accepted but the parent's
effort is retained to preserve its cache.

Anthropic's [prompt cache guidance](https://claude.com/resources/articles/reducing-cost-and-improving-performance-with-claude-platform)
states that caches are model-specific. Switching an Opus parent to Haiku
would require a separate cache; predictions therefore keep the current model.
The native control response has no per-request usage object, so the facade does
not report the parent's cumulative billing as prediction usage.

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

`tests/history_wire.rs` covers the desktop fork request, warm-process reuse,
coalescing, cold and stale suppression, prompt limits, cancellation, temporary
cleanup, parent isolation, and rejection of overrides and unsupported parents.
`tests/router.rs` covers the compatibility
provider, its echoed value, preserved thread metadata, and the routed prediction
fork/turn/unsubscribe transaction.

The native test makes real Claude requests and requires a normal authenticated
Claude installation:

```sh
mbx test --locked --features test-backend --test history_wire \
  native_prediction_reuses_cache_without_tools_or_hooks -- --ignored --nocapture
```

It verifies real prediction recall, absence of tool and hook side effects, and
continued parent recall. Native diagnostic usage must show over 10,000 cached
input tokens and fewer than 2,048 fresh/cache-write input tokens; an identical
request must not make another model call. A successful protocol probe does not
by itself prove that the installed desktop renders its prediction.
