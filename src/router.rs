//! Local desktop router: one Codex app-server connection backed by both the
//! genuine Codex app-server and the Claude facade.
//!
//! The desktop spawns this process in place of `codex app-server` (through
//! `CODEX_CLI_PATH`). Every request goes to genuine Codex unless it concerns a
//! Claude model or a Claude-owned thread; thread lists and the model picker
//! merge both backends. The facade itself runs as the separate login-session
//! service and is reached through its private Unix WebSocket.

use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};

use crate::protocol::ADOPT_THREAD_METHOD;

const MAX_FRAME: usize = 32 * 1024 * 1024;
/// Prefix for request ids the router itself sends to a backend.
const INTERNAL_ID: &str = "claudex-router-";
/// Prefix for facade server-request ids as presented to the desktop.
const CLAUDE_REQUEST_ID: &str = "claudex:";
const CURSOR_PREFIX: &str = "claudex-router:";
/// Claude's catalog alias for "whatever Claude Code defaults to"; it would only
/// duplicate a concrete model in the shared picker.
const CLAUDE_DEFAULT_ALIAS: &str = "default";
/// Fields the facade's thread/list accepts; the desktop sends newer ones too.
const CLAUDE_LIST_FILTERS: &[&str] = &[
    "archived",
    "cwd",
    "projectId",
    "searchTerm",
    "sourceKinds",
    "sortKey",
    "sortDirection",
];

static LOG: std::sync::OnceLock<std::sync::Mutex<std::fs::File>> = std::sync::OnceLock::new();

/// Diagnostics go to stderr (the desktop's app-server log) and, when
/// configured, to the router's own log file.
macro_rules! note {
    ($($arg:tt)*) => {{
        let line = format!("claudex router: {}", format_args!($($arg)*));
        eprintln!("{line}");
        if let Some(file) = LOG.get()
            && let Ok(mut file) = file.lock()
        {
            use std::io::Write;
            let _ = writeln!(file, "{} {line}", crate::protocol::now_ms());
        }
    }};
}

/// Whether `args` (everything after the codex executable) is the desktop's
/// stdio app-server launch. Anything else runs genuine Codex unchanged.
pub fn is_app_server_launch(args: &[String]) -> bool {
    let Some(position) = args.iter().position(|arg| arg == "app-server") else {
        return false;
    };
    config_flags_only(&args[..position], false) && config_flags_only(&args[position + 1..], true)
}

fn config_flags_only(args: &[String], app_server: bool) -> bool {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--config" | "--enable" | "--disable" => {
                if args.next().is_none() {
                    return false;
                }
            }
            "--analytics-default-enabled" if app_server => {}
            flag if flag.starts_with("--config=")
                || flag.starts_with("--enable=")
                || flag.starts_with("--disable=") => {}
            _ => return false,
        }
    }
    true
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Side {
    Codex,
    Claude,
}

enum Input {
    Client(Value),
    ClientClosed,
    Backend(Side, Value),
    BackendClosed(Side),
}

enum Continuation {
    Ignore,
    Probe { thread_id: String },
    ClaudeModels,
    Adopt { thread_id: String, turn: Value },
    Merge { merge: u64, side: Side },
}

enum MergeKind {
    Models,
    Threads { search: bool },
    Loaded,
}

struct Merge {
    client_id: Value,
    kind: MergeKind,
    params: Value,
    /// Codex cursor and the sort boundary of the previous merged page.
    boundary: Option<i64>,
    codex: Option<Result<Value, Value>>,
    claude: Vec<Value>,
    claude_done: bool,
}

struct Forwarded {
    method: String,
    params: Value,
}

pub struct Router {
    client: mpsc::Sender<Value>,
    codex: mpsc::Sender<Value>,
    claude: Option<mpsc::Sender<Value>>,
    claude_initialized: bool,
    threads: HashMap<String, Side>,
    /// Codex threads started on this connection that have not run a turn;
    /// the desktop prewarms these before the user picks a model.
    fresh: HashMap<String, Value>,
    forwarded: HashMap<(Side, String), Forwarded>,
    internal: HashMap<String, Continuation>,
    next_internal: u64,
    claude_models: Option<Vec<Value>>,
    models_requested: bool,
    merges: HashMap<u64, Merge>,
    next_merge: u64,
    queue: VecDeque<Value>,
    /// Client requests wait (in order) while one routing fact is outstanding.
    blocked: Option<Block>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Block {
    Probe,
    Models,
    Adopt,
}

/// Run the router on stdio until the desktop or genuine Codex disconnects.
pub async fn run(
    codex: &Path,
    args: &[String],
    claude_socket: Option<PathBuf>,
    log: Option<PathBuf>,
) -> Result<()> {
    if let Some(path) = log {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Cannot open {}", path.display()))?;
        let _ = LOG.set(std::sync::Mutex::new(file));
    }
    note!("started for {}", codex.display());
    let (events, mut receiver) = mpsc::channel::<Input>(512);

    let (client, mut client_output) = mpsc::channel::<Value>(512);
    let client_writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(message) = client_output.recv().await {
            let mut line = message.to_string();
            line.push('\n');
            if stdout.write_all(line.as_bytes()).await.is_err() || stdout.flush().await.is_err() {
                break;
            }
        }
    });
    spawn_lines(
        tokio::io::stdin(),
        events.clone(),
        Input::Client,
        Input::ClientClosed,
    );

    let mut child = Command::new(codex)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("Cannot start {}", codex.display()))?;
    let codex_input = child.stdin.take().unwrap();
    let (codex_sender, codex_output) = mpsc::channel::<Value>(512);
    spawn_line_writer(codex_input, codex_output);
    spawn_lines(
        child.stdout.take().unwrap(),
        events.clone(),
        |message| Input::Backend(Side::Codex, message),
        Input::BackendClosed(Side::Codex),
    );

    let claude = match claude_socket {
        Some(path) => match connect_claude(&path, events.clone()).await {
            Ok(sender) => Some(sender),
            Err(error) => {
                note!("Claude models are unavailable ({error:#}); serving Codex only");
                None
            }
        },
        None => None,
    };

    let mut router = Router::new(client, codex_sender, claude);
    while let Some(input) = receiver.recv().await {
        match input {
            Input::Client(message) => router.client_message(message),
            Input::Backend(side, message) => router.backend_message(side, message),
            Input::BackendClosed(Side::Claude) => router.claude_failed(),
            Input::ClientClosed | Input::BackendClosed(Side::Codex) => break,
        }
    }
    // Closing Codex's stdin lets it finish its own shutdown.
    drop(router);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await;
    client_writer.abort();
    Ok(())
}

fn spawn_lines<R>(
    reader: R,
    events: mpsc::Sender<Input>,
    wrap: impl Fn(Value) -> Input + Send + 'static,
    closed: Input,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let message =
                serde_json::from_str(&line).unwrap_or_else(|_| json!({"__routerParseError": true}));
            if events.send(wrap(message)).await.is_err() {
                return;
            }
        }
        let _ = events.send(closed).await;
    });
}

fn spawn_line_writer<W>(mut writer: W, mut messages: mpsc::Receiver<Value>)
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            let mut line = message.to_string();
            line.push('\n');
            if writer.write_all(line.as_bytes()).await.is_err() || writer.flush().await.is_err() {
                break;
            }
        }
    });
}

async fn connect_claude(path: &Path, events: mpsc::Sender<Input>) -> Result<mpsc::Sender<Value>> {
    let stream = tokio::net::UnixStream::connect(path)
        .await
        .with_context(|| format!("Cannot connect to {}", path.display()))?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let (socket, _) =
        tokio_tungstenite::client_async_with_config("ws://localhost/rpc", stream, Some(config))
            .await
            .context("Facade WebSocket handshake failed")?;
    let (mut sink, mut source) = socket.split();
    let (sender, mut messages) = mpsc::channel::<Value>(512);
    tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            if sink
                .send(Message::Text(message.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
        let _ = sink.close().await;
    });
    tokio::spawn(async move {
        while let Some(frame) = source.next().await {
            match frame {
                Ok(Message::Text(text)) => {
                    let Ok(message) = serde_json::from_str(&text) else {
                        continue;
                    };
                    if events
                        .send(Input::Backend(Side::Claude, message))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        let _ = events.send(Input::BackendClosed(Side::Claude)).await;
    });
    Ok(sender)
}

fn id_key(id: &Value) -> String {
    id.to_string()
}

fn error(id: Value, message: impl Into<String>) -> Value {
    json!({"id": id, "error": {"code": -32600, "message": message.into()}})
}

/// Thread ids a message refers to, so ownership is learned from traffic.
fn thread_ids(message: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    for value in [
        &message["params"]["threadId"],
        &message["params"]["thread"]["id"],
        &message["result"]["thread"]["id"],
    ] {
        if let Some(id) = value.as_str() {
            ids.push(id.to_owned());
        }
    }
    ids
}

fn sort_field(params: &Value) -> &'static str {
    match params["sortKey"].as_str() {
        Some("updated_at") => "updatedAt",
        Some("recency_at") => "recencyAt",
        _ => "createdAt",
    }
}

fn descending(params: &Value) -> bool {
    params["sortDirection"].as_str() != Some("asc")
}

fn list_thread(item: &Value) -> &Value {
    if item["thread"].is_object() {
        &item["thread"]
    } else {
        item
    }
}

fn sort_value(item: &Value, field: &str) -> i64 {
    list_thread(item)[field].as_i64().unwrap_or(0)
}

fn encode_cursor(codex: &Value, boundary: Option<i64>) -> String {
    let state = json!({"codex": codex, "boundary": boundary});
    format!(
        "{CURSOR_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(state.to_string())
    )
}

/// Router cursors carry Codex's cursor and the sort value where the previous
/// page ended. Foreign cursors are passed to Codex unchanged.
fn decode_cursor(cursor: &Value) -> (Value, Option<i64>) {
    let Some(encoded) = cursor.as_str().and_then(|c| c.strip_prefix(CURSOR_PREFIX)) else {
        return (cursor.clone(), None);
    };
    let state: Value = URL_SAFE_NO_PAD
        .decode(encoded)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    (state["codex"].clone(), state["boundary"].as_i64())
}

fn claude_list_query(params: &Value, cursor: Option<Value>) -> Value {
    let mut query = json!({"limit": 1000});
    for field in CLAUDE_LIST_FILTERS {
        if let Some(value) = params.get(*field).filter(|v| !v.is_null()) {
            query[*field] = value.clone();
        }
    }
    if let Some(cursor) = cursor {
        query["cursor"] = cursor;
    }
    query
}

impl Router {
    fn new(
        client: mpsc::Sender<Value>,
        codex: mpsc::Sender<Value>,
        claude: Option<mpsc::Sender<Value>>,
    ) -> Self {
        Self {
            client,
            codex,
            claude,
            claude_initialized: false,
            threads: HashMap::new(),
            fresh: HashMap::new(),
            forwarded: HashMap::new(),
            internal: HashMap::new(),
            next_internal: 1,
            claude_models: None,
            models_requested: false,
            merges: HashMap::new(),
            next_merge: 1,
            queue: VecDeque::new(),
            blocked: None,
        }
    }

    fn send_client(&self, message: Value) {
        // A stalled desktop must not stall the backends indefinitely; the
        // desktop restarts the app-server when its connection breaks.
        if self.client.try_send(message).is_err() {
            note!("desktop output queue is full or closed");
        }
    }

    fn send_backend(&mut self, side: Side, message: Value) {
        let sender = match side {
            Side::Codex => Some(&self.codex),
            Side::Claude => self.claude.as_ref(),
        };
        if sender.is_none_or(|sender| sender.try_send(message).is_err()) {
            note!("{side:?} input queue is full or closed");
        }
    }

    fn internal_request(
        &mut self,
        side: Side,
        method: &str,
        params: Value,
        continuation: Continuation,
    ) {
        let id = format!("{INTERNAL_ID}{}", self.next_internal);
        self.next_internal += 1;
        self.internal.insert(id.clone(), continuation);
        self.send_backend(side, json!({"id": id, "method": method, "params": params}));
    }

    fn forward(&mut self, side: Side, message: Value) {
        if let Some(id) = message.get("id") {
            self.forwarded.insert(
                (side, id_key(id)),
                Forwarded {
                    method: message["method"].as_str().unwrap_or("").to_owned(),
                    params: message["params"].clone(),
                },
            );
        }
        self.send_backend(side, message);
    }

    fn claude_available(&self) -> bool {
        self.claude.is_some() && self.claude_initialized
    }

    /// `None` until the facade's catalog is known.
    fn is_claude_model(&self, model: &str) -> Option<bool> {
        if !self.claude_available() {
            return Some(false);
        }
        self.claude_models
            .as_ref()
            .map(|models| models.iter().any(|entry| entry["id"] == model))
    }

    fn request_claude_models(&mut self) {
        if !self.models_requested && self.claude_available() {
            self.models_requested = true;
            self.internal_request(
                Side::Claude,
                "model/list",
                json!({"limit": 1000}),
                Continuation::ClaudeModels,
            );
        }
    }

    /// Hold client traffic until a routing fact arrives, preserving order.
    fn block(&mut self, reason: Block, message: Value) {
        self.queue.push_front(message);
        self.blocked = Some(reason);
    }

    fn unblock(&mut self, reason: Block) {
        if self.blocked != Some(reason) {
            return;
        }
        self.blocked = None;
        while self.blocked.is_none() {
            let Some(message) = self.queue.pop_front() else {
                break;
            };
            self.dispatch(message);
        }
    }

    fn client_message(&mut self, message: Value) {
        if message.get("__routerParseError").is_some() {
            self.send_client(
                json!({"id": null, "error": {"code": -32700, "message": "Invalid JSON"}}),
            );
            return;
        }
        // Responses to server requests are routed by their id and never wait.
        if message.get("method").is_none() && message.get("id").is_some() {
            self.client_response(message);
            return;
        }
        if self.blocked.is_some() {
            self.queue.push_back(message);
        } else {
            self.dispatch(message);
        }
    }

    fn client_response(&mut self, mut message: Value) {
        let decoded = message["id"]
            .as_str()
            .and_then(|id| id.strip_prefix(CLAUDE_REQUEST_ID))
            .and_then(|id| serde_json::from_str::<Value>(id).ok());
        match decoded {
            Some(id) => {
                message["id"] = id;
                self.send_backend(Side::Claude, message);
            }
            None => self.send_backend(Side::Codex, message),
        }
    }

    fn dispatch(&mut self, message: Value) {
        let Some(method) = message["method"].as_str().map(str::to_owned) else {
            self.send_backend(Side::Codex, message);
            return;
        };
        let Some(id) = message.get("id").cloned() else {
            if method == "initialized" && self.claude_available() {
                self.send_backend(Side::Claude, message.clone());
            }
            self.send_backend(Side::Codex, message);
            return;
        };
        let params = message["params"].clone();
        match method.as_str() {
            "initialize" => {
                if self.claude.is_some() {
                    self.claude_initialized = true;
                    self.internal_request(Side::Claude, "initialize", params, Continuation::Ignore);
                }
                self.forward(Side::Codex, message);
            }
            "model/list" if self.claude_available() => {
                self.request_claude_models();
                self.start_merge(id, MergeKind::Models, params);
            }
            "thread/loaded/list" if self.claude_available() => {
                self.start_merge(id, MergeKind::Loaded, params);
            }
            "thread/list" | "thread/search" if self.claude_available() => {
                let related = params["parentThreadId"]
                    .as_str()
                    .or_else(|| params["ancestorThreadId"].as_str())
                    .map(str::to_owned);
                match related {
                    // Relations never cross backends.
                    Some(thread) => self.route_by_thread(&thread, message),
                    None => self.start_merge(
                        id,
                        MergeKind::Threads {
                            search: method == "thread/search",
                        },
                        params,
                    ),
                }
            }
            "thread/start" => match params["model"].as_str() {
                Some(model) => match self.is_claude_model(model) {
                    None => {
                        self.request_claude_models();
                        self.block(Block::Models, message);
                    }
                    Some(true) => self.forward(Side::Claude, message),
                    Some(false) => self.forward(Side::Codex, message),
                },
                None => self.forward(Side::Codex, message),
            },
            _ => match params["threadId"].as_str() {
                Some(thread) => {
                    let thread = thread.to_owned();
                    self.route_by_thread(&thread, message);
                }
                None => self.forward(Side::Codex, message),
            },
        }
    }

    fn route_by_thread(&mut self, thread: &str, message: Value) {
        let side = match self.threads.get(thread) {
            Some(side) => *side,
            None if self.claude_available() => {
                let thread_id = thread.to_owned();
                self.internal_request(
                    Side::Claude,
                    "thread/read",
                    json!({"threadId": thread_id}),
                    Continuation::Probe { thread_id },
                );
                self.block(Block::Probe, message);
                return;
            }
            None => Side::Codex,
        };
        let mut message = message;
        let method = message["method"].as_str().unwrap_or("").to_owned();
        if let Some(model) = message["params"]["model"].as_str().map(str::to_owned) {
            let Some(claude_model) = self.is_claude_model(&model) else {
                self.request_claude_models();
                self.block(Block::Models, message);
                return;
            };
            if claude_model != (side == Side::Claude) {
                if method == "turn/start" {
                    if side == Side::Codex && self.fresh.contains_key(thread) {
                        self.adopt(thread, message);
                    } else {
                        let id = message["id"].clone();
                        self.send_client(error(
                            id,
                            match side {
                                Side::Claude => format!(
                                    "This chat runs on Claude; start a new chat to use {model}"
                                ),
                                Side::Codex => format!(
                                    "This chat runs on Codex; start a new chat to use {model}"
                                ),
                            },
                        ));
                    }
                    return;
                }
                // Keep the owning backend's own model for other thread requests.
                message["params"].as_object_mut().unwrap().remove("model");
            }
        }
        if side == Side::Codex && method == "turn/start" {
            self.fresh.remove(thread);
        }
        self.forward(side, message);
    }

    /// Move the desktop's prewarmed, still-empty Codex thread to Claude under
    /// the same id, then run the requested turn there.
    fn adopt(&mut self, thread: &str, turn: Value) {
        let mut params = self.fresh.remove(thread).unwrap_or_else(|| json!({}));
        params["model"] = turn["params"]["model"].clone();
        if let Some(effort) = turn["params"].get("effort").filter(|v| !v.is_null()) {
            params["effort"] = effort.clone();
        }
        let thread_id = thread.to_owned();
        self.internal_request(
            Side::Claude,
            ADOPT_THREAD_METHOD,
            json!({"threadId": thread_id, "params": params}),
            Continuation::Adopt { thread_id, turn },
        );
        self.blocked = Some(Block::Adopt);
    }

    fn start_merge(&mut self, client_id: Value, kind: MergeKind, params: Value) {
        let merge = self.next_merge;
        self.next_merge += 1;
        let mut codex_params = params.clone();
        let mut boundary = None;
        if let Some(cursor) = params.get("cursor").filter(|c| !c.is_null()) {
            let (codex_cursor, previous) = decode_cursor(cursor);
            codex_params["cursor"] = codex_cursor;
            boundary = previous;
        }
        let codex_method = match kind {
            MergeKind::Models => "model/list",
            MergeKind::Loaded => "thread/loaded/list",
            MergeKind::Threads { search: true } => "thread/search",
            MergeKind::Threads { search: false } => "thread/list",
        };
        self.internal_request(
            Side::Codex,
            codex_method,
            codex_params,
            Continuation::Merge {
                merge,
                side: Side::Codex,
            },
        );
        let claude_done = match kind {
            MergeKind::Models => true,
            MergeKind::Loaded => {
                self.internal_request(
                    Side::Claude,
                    "thread/loaded/list",
                    json!({"limit": 1000}),
                    Continuation::Merge {
                        merge,
                        side: Side::Claude,
                    },
                );
                false
            }
            // Claude chats are never filed in desktop sections.
            MergeKind::Threads { .. } if !params["sectionId"].is_null() => true,
            MergeKind::Threads { .. } => {
                let query = claude_list_query(&params, None);
                self.internal_request(
                    Side::Claude,
                    "thread/list",
                    query,
                    Continuation::Merge {
                        merge,
                        side: Side::Claude,
                    },
                );
                false
            }
        };
        self.merges.insert(
            merge,
            Merge {
                client_id,
                kind,
                params,
                boundary,
                codex: None,
                claude: vec![],
                claude_done,
            },
        );
    }

    fn merge_part(&mut self, merge: u64, side: Side, result: Result<Value, Value>) {
        let Some(state) = self.merges.get_mut(&merge) else {
            return;
        };
        let mut next_page = None;
        match (side, result) {
            (Side::Codex, result) => state.codex = Some(result),
            (Side::Claude, Ok(page)) => {
                state
                    .claude
                    .extend(page["data"].as_array().cloned().unwrap_or_default());
                match page["nextCursor"].as_str() {
                    Some(cursor) => {
                        let cursor = json!(cursor);
                        next_page = Some(match state.kind {
                            MergeKind::Loaded => (
                                "thread/loaded/list",
                                json!({"limit": 1000, "cursor": cursor}),
                            ),
                            _ => (
                                "thread/list",
                                claude_list_query(&state.params, Some(cursor)),
                            ),
                        });
                    }
                    None => state.claude_done = true,
                }
            }
            // The Claude side is additive; its failures never hide Codex.
            (Side::Claude, Err(error)) => {
                note!("Claude list failed: {error}");
                state.claude_done = true;
            }
        }
        if let Some((method, query)) = next_page {
            self.internal_request(
                Side::Claude,
                method,
                query,
                Continuation::Merge { merge, side },
            );
        }
        self.finish_merges();
    }

    fn finish_merges(&mut self) {
        let models_ready = self.claude_models.is_some() || !self.claude_available();
        let ready: Vec<u64> = self
            .merges
            .iter()
            .filter(|(_, m)| {
                m.codex.is_some()
                    && m.claude_done
                    && (!matches!(m.kind, MergeKind::Models) || models_ready)
            })
            .map(|(id, _)| *id)
            .collect();
        for merge in ready {
            let state = self.merges.remove(&merge).unwrap();
            let reply = self.merged(state);
            self.send_client(reply);
        }
    }

    fn merged(&mut self, state: Merge) -> Value {
        let mut result = match state.codex.unwrap() {
            Ok(result) => result,
            Err(error) => return json!({"id": state.client_id, "error": error}),
        };
        let last_page = result["nextCursor"].is_null();
        match state.kind {
            MergeKind::Models => {
                if last_page && let Some(models) = &self.claude_models {
                    let data = result["data"].as_array_mut();
                    if let Some(data) = data {
                        data.extend(models.iter().cloned());
                    }
                }
            }
            MergeKind::Loaded => {
                for id in state.claude.iter().filter_map(Value::as_str) {
                    self.threads.insert(id.to_owned(), Side::Claude);
                }
                if last_page && let Some(data) = result["data"].as_array_mut() {
                    data.extend(state.claude);
                }
            }
            MergeKind::Threads { .. } => {
                let field = sort_field(&state.params);
                let descending = descending(&state.params);
                let data = result["data"].as_array().cloned().unwrap_or_default();
                for item in &data {
                    if let Some(id) = list_thread(item)["id"].as_str() {
                        self.threads.entry(id.to_owned()).or_insert(Side::Codex);
                    }
                }
                // Each Claude thread belongs to the page whose Codex sort
                // values bracket it, so cursors stay Codex-driven and stable.
                let end = if last_page {
                    None
                } else {
                    data.last().map(|item| sort_value(item, field))
                };
                let in_window = |value: i64| {
                    let after_previous = state.boundary.is_none_or(|boundary| {
                        if descending {
                            value < boundary
                        } else {
                            value > boundary
                        }
                    });
                    let before_end = end.is_none_or(|end| {
                        if descending {
                            value >= end
                        } else {
                            value <= end
                        }
                    });
                    after_previous && before_end
                };
                let mut merged = data;
                // The facade's thread/list predates the originator filter.
                let originators = state.params["originators"]
                    .as_array()
                    .filter(|list| !list.is_empty());
                for thread in state.claude {
                    if let Some(id) = thread["id"].as_str() {
                        self.threads.insert(id.to_owned(), Side::Claude);
                    }
                    if originators.is_none_or(|list| list.contains(&thread["originator"]))
                        && in_window(sort_value(&thread, field))
                    {
                        merged.push(thread);
                    }
                }
                merged.sort_by_key(|item| {
                    let value = sort_value(item, field);
                    if descending { -value } else { value }
                });
                result["data"] = json!(merged);
                if !last_page {
                    let boundary = end.or(state.boundary);
                    result["nextCursor"] = json!(encode_cursor(&result["nextCursor"], boundary));
                }
                if result.get("backwardsCursor").is_some() {
                    result["backwardsCursor"] = Value::Null;
                }
            }
        }
        json!({"id": state.client_id, "result": result})
    }

    fn backend_message(&mut self, side: Side, mut message: Value) {
        let method = message["method"].as_str().map(str::to_owned);
        let id = message.get("id").cloned();
        match (method, id) {
            (None, Some(id)) => self.backend_response(side, id, message),
            (Some(_), Some(id)) => {
                self.learn(side, &message);
                if side == Side::Claude {
                    message["id"] = json!(format!("{CLAUDE_REQUEST_ID}{id}"));
                }
                self.send_client(message);
            }
            (Some(method), None) => {
                let ids = thread_ids(&message);
                if side == Side::Claude && ids.is_empty() {
                    // Facade-wide plugin events duplicate genuine Codex's own.
                    return;
                }
                if ids
                    .iter()
                    .any(|id| self.threads.get(id).is_some_and(|owner| *owner != side))
                {
                    // A thread that moved to Claude no longer belongs to Codex.
                    return;
                }
                self.learn(side, &message);
                if side == Side::Codex && method == "turn/started" {
                    for id in &ids {
                        self.fresh.remove(id);
                    }
                }
                if side == Side::Claude
                    && method == "serverRequest/resolved"
                    && let Some(request) = message["params"].get("requestId").cloned()
                {
                    message["params"]["requestId"] = json!(format!("{CLAUDE_REQUEST_ID}{request}"));
                }
                self.send_client(message);
            }
            (None, None) => {}
        }
    }

    fn learn(&mut self, side: Side, message: &Value) {
        for id in thread_ids(message) {
            self.threads.entry(id).or_insert(side);
        }
    }

    fn backend_response(&mut self, side: Side, id: Value, message: Value) {
        let result = match message.get("error") {
            Some(error) => Err(error.clone()),
            None => Ok(message["result"].clone()),
        };
        if let Some(continuation) = id.as_str().and_then(|id| self.internal.remove(id)) {
            self.continue_with(continuation, result);
            return;
        }
        let forwarded = self.forwarded.remove(&(side, id_key(&id)));
        if side == Side::Claude
            && let (Some(forwarded), Err(error)) = (&forwarded, &result)
        {
            note!("Claude {} failed: {error}", forwarded.method);
        }
        if let Some(forwarded) = forwarded
            && let Ok(result) = &result
            && let Some(thread) = result["thread"]["id"].as_str()
        {
            self.threads.insert(thread.to_owned(), side);
            if side == Side::Codex && forwarded.method == "thread/start" {
                self.fresh.insert(thread.to_owned(), forwarded.params);
            }
        }
        self.send_client(message);
    }

    fn continue_with(&mut self, continuation: Continuation, result: Result<Value, Value>) {
        match continuation {
            Continuation::Ignore => {}
            Continuation::Probe { thread_id } => {
                let side = if result.is_ok() {
                    Side::Claude
                } else {
                    Side::Codex
                };
                self.threads.insert(thread_id, side);
                self.unblock(Block::Probe);
            }
            Continuation::ClaudeModels => {
                let models = match result {
                    Ok(result) => result["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|model| model["id"] != CLAUDE_DEFAULT_ALIAS)
                        .map(|model| {
                            let mut model = model.clone();
                            model["isDefault"] = json!(false);
                            model
                        })
                        .collect(),
                    Err(error) => {
                        note!("Claude model catalog failed: {error}");
                        vec![]
                    }
                };
                self.claude_models = Some(models);
                self.finish_merges();
                self.unblock(Block::Models);
            }
            Continuation::Adopt { thread_id, turn } => {
                match result {
                    Ok(_) => {
                        self.threads.insert(thread_id.clone(), Side::Claude);
                        self.forward(Side::Claude, turn);
                        self.internal_request(
                            Side::Codex,
                            "thread/unsubscribe",
                            json!({"threadId": thread_id}),
                            Continuation::Ignore,
                        );
                    }
                    Err(failure) => {
                        self.send_client(json!({"id": turn["id"], "error": failure}));
                    }
                }
                self.unblock(Block::Adopt);
            }
            Continuation::Merge { merge, side } => self.merge_part(merge, side, result),
        }
    }

    /// The facade went away: finish everything waiting on it as Codex-only.
    fn claude_failed(&mut self) {
        if self.claude.take().is_none() {
            return;
        }
        note!("lost the Claude facade connection");
        self.claude_initialized = false;
        let failed: Vec<(Side, String)> = self
            .forwarded
            .keys()
            .filter(|(side, _)| *side == Side::Claude)
            .cloned()
            .collect();
        for key in failed {
            self.forwarded.remove(&key);
            if let Ok(id) = serde_json::from_str::<Value>(&key.1) {
                self.send_client(error(id, "The Claude facade disconnected"));
            }
        }
        let pending: Vec<String> = self.internal.keys().cloned().collect();
        let failure = json!({"code": -32000, "message": "The Claude facade disconnected"});
        for id in pending {
            let continuation = match self.internal.get(&id) {
                Some(Continuation::Merge {
                    side: Side::Claude, ..
                })
                | Some(Continuation::Probe { .. })
                | Some(Continuation::ClaudeModels)
                | Some(Continuation::Adopt { .. }) => self.internal.remove(&id).unwrap(),
                _ => continue,
            };
            self.continue_with(continuation, Err(failure.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_owned()).collect()
    }

    #[test]
    fn recognizes_only_the_desktop_stdio_launch() {
        assert!(is_app_server_launch(&strings(&[
            "-c",
            "features.code_mode_host=true",
            "app-server",
            "--analytics-default-enabled",
            "-c",
            "chatgpt_base_url=\"x\"",
        ])));
        assert!(is_app_server_launch(&strings(&["app-server"])));
        for other in [
            vec!["--version"],
            vec!["exec", "hi"],
            vec!["app-server", "proxy"],
            vec!["app-server", "--listen", "ws://127.0.0.1:1"],
            vec!["app-server", "generate-json-schema", "--out", "x"],
            vec!["-c"],
        ] {
            assert!(!is_app_server_launch(&strings(&other)), "{other:?}");
        }
    }

    #[test]
    fn cursors_round_trip_and_pass_foreign_cursors_through() {
        let cursor = encode_cursor(&json!("codex-page-2"), Some(17));
        assert_eq!(
            decode_cursor(&json!(cursor)),
            (json!("codex-page-2"), Some(17))
        );
        assert_eq!(decode_cursor(&json!("raw")), (json!("raw"), None));
    }
}
