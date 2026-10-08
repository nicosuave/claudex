# Compatibility with Codex app-server 0.160.0

This document describes implemented behavior, not full protocol equivalence or a guarantee about future Codex/Claude releases. The wire schema is `protocol/codex-0.160.0.json`.

## Client methods

Accepted requests are validated against the pinned schema before execution, with supplemental schemas for legacy `getAuthStatus` and `thread/rollback` RPCs omitted from the export. Fields below are the supported non-null fields; unsupported non-null fields fail. Values must also satisfy the upstream schema.

| Method | Supported fields / behavior |
| --- | --- |
| `initialize` | `clientInfo`, `capabilities.experimentalApi`, `capabilities.optOutNotificationMethods`; requests accepted immediately after reply; optional `initialized` |
| `thread/start` | `cwd`, `model`, `modelProvider`, `approvalPolicy`, `sandbox`, `baseInstructions`, `developerInstructions`, `ephemeral`, `historyMode`, `projectId`, `threadSource` |
| `thread/resume` | `threadId` or returned `path`, `cwd`, `model`, `modelProvider`, `approvalPolicy`, `sandbox`, `baseInstructions`, `developerInstructions`, `excludeTurns`, `initialTurnsPage`; snapshot cursors and unchanged active settings supported |
| `thread/fork` | `threadId`, `cwd`, `model`, `modelProvider`, `approvalPolicy`, `sandbox`, `ephemeral`, `excludeTurns`, `threadSource`, `beforeTurnId` (exclusive), `lastTurnId` (inclusive); exact native transcript snapshots |
| `thread/revert`, `thread/rollback` | Idle loaded thread; retain the prefix before `beforeTurnId` (paginated) or remove `numTurns` (legacy); next native turn forks at the retained UUID |
| `thread/attachment/{add,list,remove}` | Durable arbitrary attachment type/identity/payload metadata; notification only after save; desktop Git worker owns checkout creation/archive/restore |
| `thread/read` | `threadId`, `includeTurns` |
| `thread/list` | `cursor`, `limit`, `sortKey`, `sortDirection`, `modelProviders`, `sourceKinds`, `archived`, `projectId`, `cwd`, `searchTerm`, `useStateDbOnly`, `ancestorThreadId`; all records come from facade state; descendant filter returns no unrelated threads |
| `thread/loaded/list` | `cursor`, `limit` |
| `thread/turns/list` | `threadId`, `cursor`, `limit`, `sortDirection`, `itemsView` |
| `thread/items/list` | `threadId`, optional `turnId`, `cursor`, `limit`, `sortDirection` |
| `thread/timeline/list` | `threadId`, `cursor`, `limit`; newest bounded page in chronological order, earlier pages via cursor |
| `thread/name/set` | `threadId`, `name` |
| `thread/archive`, `thread/unarchive` | `threadId`; requires no active turn |
| `thread/unsubscribe` | `threadId`; active-turn owners must interrupt first |
| `turn/start` | `threadId`, `input`, `clientUserMessageId`, `cwd`, `model`, `approvalPolicy`, `sandboxPolicy`, `effort`, `summary`, `outputSchema` |
| `turn/steer` | Owner-only active-turn input; checks `expectedTurnId`, persists user input, waits for native replay consumption before completing the turn |
| `thread/queue/{add,list,update,delete,reorder,start}` | Persisted queue with client message identity, notifications, and atomic dequeue/start |
| `turn/interrupt` | `threadId`, `turnId`; ACK acknowledges the request, `turn/completed` is the terminal event |
| `model/list` | `cursor`, `limit`, `includeHidden`; native Claude initialize catalog, cached per service process; preserves versioned display names and per-model effort levels, including Fable |
| `account/read` | Returns `account: null`, `requiresOpenaiAuth: false`; `refreshToken: true` rejected |
| `getAuthStatus` | Legacy desktop discovery; null OpenAI auth method/token, `requiresOpenaiAuth: false`; token flags have no effect for this provider |
| `config/read` | `includeLayers`, `cwd`; facade defaults and genuine Codex plugin enable state; empty facade origins/layers |
| `config/batchWrite` | Atomic `upsert` of `model` and `model_reasoning_effort`; persists in facade state. Plugin-only edits are forwarded to genuine Codex; mixed facade/plugin transactions are rejected |
| `fs/createDirectory` | Absolute `path`, optional `recursive` (defaults true); creates the desktop task workspace |
| `fs/getMetadata`, `fs/readDirectory` | Absolute `path`; metadata and direct child entries for the remote project folder picker; follows directory symlinks |
| `fs/readFile`, `fs/writeFile` | Absolute path, base64 binary payload; 16 MiB regular-file limit |
| `fs/copy`, `fs/remove` | Absolute paths; explicit recursive copy, recursive removal defaults true; symlinks preserved on copy |
| `command/exec`, `/write`, `/resize`, `/terminate` | Connection-owned real pipe/PTY processes; cwd/env, streaming stdin/output, cancellation, timeout and output caps |
| `plugin/{list,installed,search,read,install,uninstall}` | Genuine Codex catalog and administration when configured; native consent/auth failures propagate |
| `configRequirements/read` | Full-access/workspace-write, on-request/never approvals, user/native auto review |
| `collaborationMode/list` | Empty list: no Codex collaboration presets |
| `experimentalFeature/list` | `cursor`, `limit`; empty list: no Codex experimental features |

`historyMode` and `projectId` on `thread/start`, and `projectId` on `thread/list`, require `initialize.capabilities.experimentalApi: true`, matching their absence from the pinned stable schema.

Only provider `anthropic` is accepted. Model aliases and identifiers are sent to Claude; the backend decides availability. Supported effort values are `low`, `medium`, `high`, `xhigh`, and `max`; a particular model may reject an otherwise valid effort. No effort is forced by default.

Desktop submission options are translated before dispatch: `approvalsReviewer: user` uses the native human approval callback; desktop guardian/auto-review selections use Claude native auto review, not a Codex reviewer. `experimentalRawEvents: false` is accepted. `disabledPluginIds` persists per thread and becomes a native `enabledPlugins` launch override. Additive `config.shell_environment_policy` overlays persist and become native `env` settings; restrictive inheritance/filtering/profile policies fail explicitly. Absolute `runtimeWorkspaceRoots` are workspace metadata, not filesystem confinement. Default collaboration mode maps its model and effort; plan mode remains unsupported. Turn trigger/client analytics and the deprecated `multiAgentMode` do not affect execution. Additional context is passed as reference text to Claude. Codex runtime feature overrides and app-tool allowlists produce warnings because Claude uses its own runtime/tools; they do not enable Codex-only features or connect app-provided tools. Dynamic function/namespace definitions are persisted with the thread and exposed through the Claude SDK MCP transport. Calls use item/tool/call on the turn owner, validate arguments, and map client success/error and text/image/audio results back to MCP; the desktop remains responsible for its own tool approval UI. The deprecated personality field is accepted as a no-op. Other unsupported options are reported together rather than one at a time.

`baseInstructions` maps to `--system-prompt`; `developerInstructions` maps to `--append-system-prompt`. An idle resumed thread can refresh both instruction fields, model, cwd, or approval policy. The facade disables native system-prompt snapshots so refreshed instructions take effect while native conversation history is preserved. Active turns reject reconfiguration.

## Notifications and tool translation

Implemented notifications include `thread/started`, name/archive/unarchive/status changes, `thread/tokenUsage/updated`, `turn/started`, `turn/completed`, `turn/plan/updated`, `item/started`, `item/completed`, assistant/reasoning/command deltas, `serverRequest/resolved`, `thread/reverted`, attachment updates, and `error`.

| Claude content | Codex representation |
| --- | --- |
| Text blocks and streaming text | `agentMessage`; partial/final messages reconciled without duplicate text |
| Exposed thinking blocks | `reasoning` and text deltas |
| `Bash` | `commandExecution`, result output, exit code only when supplied by Claude |
| `Write`, `Edit`, `MultiEdit` | `fileChange` with best-effort proposed diffs from small UTF-8 files |
| `mcp__server__tool` | `mcpToolCall` |
| Other native tools | `dynamicToolCall` in namespace `claude` |
| `TodoWrite` | Tool item plus `turn/plan/updated` |
| Compaction boundary | `contextCompaction` |
| Structured output | Final assistant message containing JSON |

File diffs are previews, not an OS audit; command-mediated file edits do not synthesize file-change items. Tool output arrives when Claude emits its tool result, so command output is not necessarily streamed while the process runs. Structured/multimodal tool results are currently rendered as text, including serialized non-text blocks; native MCP result metadata is not preserved.

`summary: none` suppresses reasoning items; `auto` exposes native text. `concise` and `detailed` expose native text with a warning because the facade cannot impose those summary lengths. The facade forwards only reasoning text Claude actually exposes. Child-agent content is represented by the parent tool result, not independent Codex child threads. Claude's local-agent/local-workflow tasks and session state are tracked to keep stdin open for continuations and permission requests after an intermediate result. Claude versions without session-state events fall back to the result plus observed task state.

## Permissions and lifecycle

- `on-request` uses Claude `manual`; `never` uses `dontAsk`. `never` denies callback permission requests rather than bypassing protections; native user questions remain answerable.
- Commands use `item/commandExecution/requestApproval`; file writes use `item/fileChange/requestApproval`. Supported decisions are `accept`, `acceptForSession`, `decline`, `cancel`. Session grants match the exact tool name, complete input and cwd within this thread, persist across facade restarts, and are not copied to forks. Native broad rule amendments remain rejected.
- Desktop tool execution has no approval deadline; the client response, cancellation, or native turn termination ends it. Transport loss does not cancel it.
- Single-select `AskUserQuestion` calls use `item/tool/requestUserInput`. Calls containing multi-select questions use sequential `item/tool/requestOptionPicker` requests with fresh IDs, the installed desktop extension documented in [approval design](docs/approval-design.md). Skip/dismiss denies the native call without partial answers. Generic tool permissions offer once/session/deny. Only the live owner, or its replacement after disconnect, can answer pending requests.
- Each facade thread has at most one active turn. Separate threads have independent Claude processes. Resuming an active thread allows another client to subscribe, but changing its settings while active is rejected.
- Disconnecting the turn owner detaches it without stopping Claude. Resume claims a detached turn, returns current streamed state, then replays pending requests with their original IDs/call IDs. A delayed result with the original request ID can claim after initialization, before resume. Approval deadlines count connected time only. A live owner cannot be displaced by an observer. Slow clients are disconnected when their bounded output queue fills. Host command/PTY processes remain connection-owned and terminate on disconnect.
- Interrupted runs drain transcript events through a result/exit for up to five seconds before forced termination. All forced cancellations await process cleanup before publishing the terminal frontend state. Native descendants in the backend process group are terminated on Unix.
- One native process runs per facade turn. Claude-native background shells are not guaranteed to survive the turn; a completed task is not a persistent terminal service.
- Facade records are written atomically with private file permissions and a process-exclusive directory lock. Recovering an active saved turn after a facade crash marks it interrupted. Persistence does not coordinate arbitrary outside clients using the same native Claude session.
- `threadId` identifies the facade record. The returned `sessionId` groups a source thread and its forks as in this pinned protocol. Native Claude session IDs are private metadata and are not interchangeable with either field.
- Fork/revert/rollback use `--fork-session --resume-session-at` at an exact per-turn native UUID. Empty history starts fresh. Missing anchors fail closed; old records support full snapshot forks only when their latest anchor is known. Recovery does not undo files, external effects or native persistent memory.
- Disconnect recovery lasts while the facade process runs. Facade shutdown/restart interrupts active native work. Stable call IDs allow the desktop to deduplicate reconnect replay; exactly-once effects across a full desktop process crash are not guaranteed.
- Pagination cursors bind to list scope, filters, direction, and an item anchor. If the anchor is removed from that list, the cursor errors. Limits are 1–1000. Summary requests can receive full items labeled `full`.

## Explicit limitations

- Workspace-write enforces Claude’s native Bash sandbox and scoped file permissions. Native auto review accepts installed desktop guardian payloads. MCP and hooks are trusted integrations outside the Bash sandbox; read-only and external profiles are unsupported. Resolved restrictions are checked before model input.
- No raw transcript import, realtime audio, remote control, login, quotas, remote environments, collaboration modes, or other unlisted RPCs. Codex skills/hooks management and MCP services are forwarded when configured; Codex hook execution around Claude turns and interactive MCP elicitation are unsupported.
- Inputs: text, supported image data/HTTP(S) URLs, and local PNG/JPEG/GIF/WebP paths. No OpenAI file IDs, image detail selection, audio, video, skill, or mention input mapping.
- Ephemeral threads use `--no-session-persistence`, remain out of disk/list history, and currently support one turn only. They cannot be a fork source.
- Model listing is the installed Claude CLI's catalog, not an inference entitlement check. Config reads declare the Claude custom provider; model/effort default writes and Codex plugin enable edits are supported. Plugin configuration and authentication remain owned by genuine Codex.
- Native authentication and API failures produce failed turns; there is no credential management or provider failover.
- Validated on macOS. Windows process-tree cleanup and Unix-only transports are not interchangeable; Windows support is not claimed.
- Live inference, restart recall, fork isolation, and a real tool approval passed earlier on October 8, 2026. The expanded installed-service probe also passed first turn/tool roundtrip, saved-path resume, timeline, host command, and queued context-preserving followup. Two-input steering passed separately against live Opus. The current build passed 91 deterministic tests and installed SSH reconnect with one recovered tool result; earlier fork/revert excluded removed native context, and the environment overlay reached a real Bash tool. Native desktop rendering remains unverified because computer-use access is denied. See [the workflow audit](docs/desktop-integration-audit.md) for the complete known gap map.

The installed desktop host API also supports `process/spawn`, `process/writeStdin`, `process/kill`, and `process/resizePty`, with immediate spawn acknowledgement, binary output notifications and exit notifications. Explicit null caps/timeouts follow that API’s unlimited semantics. New threads default to workspace-write. Native Read approvals use filesystem permission requests; generic tools retain explicit question-based approval.

Context usage now reports the latest top-level Claude model request, including cache tokens; cumulative turn billing remains separate. Updates stream during turns and replay on resume. Legacy inflated context readings remain unknown until fresh native usage arrives. The denominator uses the actual main model rather than a larger auxiliary model.

Native auto review can deny an action before Claude emits `can_use_tool`; no
human callback exists for the facade to approve in that case. The denial is
preserved and surfaced. To request manual review, select **Ask for approval** for
that chat and ask Claude to retry the specific action. The facade does not switch
modes automatically or install global permission allow rules. Explicit configured
deny rules still apply. This boundary was checked against Claude Code 2.1.294.
