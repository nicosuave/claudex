//! Backend processes (Claude or an ACP agent) kept between turns.
//!
//! Every Claude launch renders its request prefix again: MCP servers race to
//! connect, and the Codex plugin catalog is rebuilt. Any difference invalidates
//! the prompt cache for the whole conversation, so a thread keeps its process
//! while idle and the next turn reuses it when nothing it was launched with changed.
//!
//! An idle Claude process can also start work by itself: a background command
//! or agent that outlived its turn finishes, and Claude answers the
//! notification. The process is then handed back to the server as a new turn.
use std::time::Duration;

use serde_json::{Value, json};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, BackendEvent, SessionOptions};

pub struct WarmSession {
    pub backend: Backend,
    pub output: mpsc::Receiver<BackendEvent>,
    pub plugins: Option<crate::plugin_runtime::Runtime>,
}

impl WarmSession {
    pub async fn close(mut self) {
        // Shutdown sends EOF first so Claude persists its transcript.
        let _ = self.backend.shutdown().await;
        if let Some(runtime) = self.plugins {
            runtime.client.shutdown().await;
        }
    }
}

/// An idle session owned by a watcher task until a turn reclaims it.
pub struct Parked {
    pub key: Value,
    pub session_id: String,
    pub parked_at: Instant,
    reclaim: CancellationToken,
    watcher: JoinHandle<Option<WarmSession>>,
}

/// Where an idle session goes when Claude starts work by itself.
pub struct Wake {
    pub thread_id: String,
    pub events: mpsc::Sender<crate::server::Event>,
}

impl Parked {
    pub fn new(
        key: Value,
        session_id: String,
        session: WarmSession,
        timeout: Duration,
        wake: Wake,
    ) -> Self {
        let reclaim = CancellationToken::new();
        Self {
            watcher: tokio::spawn(watch(session, reclaim.clone(), timeout, key.clone(), wake)),
            key,
            session_id,
            parked_at: Instant::now(),
            reclaim,
        }
    }

    /// False once the watcher closed the session or handed it back as a wake.
    pub fn is_open(&self) -> bool {
        !self.watcher.is_finished()
    }

    pub async fn take(self) -> Option<WarmSession> {
        self.reclaim.cancel();
        self.watcher.await.ok().flatten()
    }

    pub async fn close(self) {
        if let Some(session) = self.take().await {
            session.close().await;
        }
    }
}

async fn watch(
    mut session: WarmSession,
    reclaim: CancellationToken,
    timeout: Duration,
    key: Value,
    wake: Wake,
) -> Option<WarmSession> {
    let expired = tokio::time::sleep(timeout);
    tokio::pin!(expired);
    // Status frames since parking, such as the task notification that woke Claude.
    let mut idle_frames = Vec::new();
    loop {
        tokio::select! {
            _ = reclaim.cancelled() => {
                // Frames queued before the reclaim still belong to the idle period.
                let mut busy = false;
                while let Ok(event) = session.output.try_recv() {
                    busy |= !quiet(&event);
                }
                if !busy && session.backend.is_running() {
                    return Some(session);
                }
                break;
            }
            _ = &mut expired => break,
            event = session.output.recv() => match event {
                Some(BackendEvent::Message(frame)) => {
                    let busy = !quiet(&BackendEvent::Message(frame.clone()));
                    idle_frames.push(frame);
                    if busy {
                        let woke = crate::server::Event::Woke {
                            thread_id: wake.thread_id,
                            key,
                            session,
                            frames: idle_frames,
                        };
                        // A closed event loop drops the session, which kills the process.
                        let _ = wake.events.send(woke).await;
                        return None;
                    }
                }
                _ => break,
            },
        }
    }
    session.close().await;
    None
}

/// Idle Claude only reports status. Anything else means it started work, which
/// needs a turn to observe and approve it.
fn quiet(event: &BackendEvent) -> bool {
    match event {
        BackendEvent::Message(frame) => match frame["type"].as_str() {
            Some("system") => {
                frame["subtype"] != "session_state_changed" || frame["state"] == "idle"
            }
            Some("rate_limit_event") => true,
            // ACP agents announce commands, modes, config, titles and usage while idle.
            Some("acp") => matches!(
                frame["update"]["sessionUpdate"].as_str(),
                Some(
                    "available_commands_update"
                        | "current_mode_update"
                        | "config_option_update"
                        | "session_info_update"
                        | "usage_update"
                )
            ),
            _ => false,
        },
        BackendEvent::Exited { .. } => false,
    }
}

/// Launch settings a later turn must match to reuse the process. None when the
/// launch is turn-specific.
pub fn launch_key(options: &SessionOptions) -> Option<Value> {
    if options.ephemeral || options.output_schema.is_some() {
        return None;
    }
    Some(json!({
        "cwd": options.cwd,
        "model": options.model,
        "permissionMode": options.permission_mode,
        "systemPrompt": options.system_prompt,
        "appendSystemPrompt": options.append_system_prompt,
        "effort": options.effort,
        "dynamicTools": options.dynamic_tools,
        "nativeSettings": options.native_settings,
        "sandbox": options.sandbox,
    }))
}

/// Why a parked session cannot serve this launch, or None when it can.
pub fn mismatch(parked: &Parked, options: &SessionOptions, key: Option<&Value>) -> Option<String> {
    if !options.resume || options.fork_from.is_some() || options.resume_at.is_some() {
        return Some("history was rewound or forked".into());
    }
    if options.session_id != parked.session_id {
        return Some("native session changed".into());
    }
    let Some(key) = key else {
        return Some("turn needs its own launch".into());
    };
    let changed: Vec<&str> = parked
        .key
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, value)| key.get(name.as_str()) != Some(*value))
        .map(|(name, _)| name.as_str())
        .collect();
    (!changed.is_empty()).then(|| format!("changed {}", changed.join(", ")))
}
