//! Local desktop router: one Codex app-server connection backed by both the
//! genuine Codex app-server and the Claude facade.
//!
//! The desktop spawns this process in place of `codex app-server` (through
//! `CODEX_CLI_PATH`). Every request goes to genuine Codex unless it concerns a
//! Claude model or a Claude-owned thread; thread lists and the model picker
//! merge both backends. The facade itself runs as the separate login-session
//! service and is reached through its private Unix WebSocket.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};

use crate::protocol::{ADOPT_THREAD_METHOD, SECTION_CHANGED_METHOD};

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
    "parentThreadId",
    "ancestorThreadId",
];

/// Codex app (connector) methods. A chat's `threadId` only selects Codex's
/// per-thread app configuration; apps do not depend on the chat's model.
const APP_METHODS: &[&str] = &["app/list", "app/installed", "app/read"];

/// Facade methods whose `input` carries the user's message content.
const INPUT_METHODS: &[&str] = &[
    "turn/start",
    "turn/steer",
    "thread/queue/add",
    "thread/queue/update",
];
/// Encoded image bytes one forwarded request may carry, leaving room for the
/// rest of the message within the facade's frame limit.
const MAX_INLINED_IMAGES: usize = MAX_FRAME - 1024 * 1024;

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

#[path = "router_delegation.rs"]
mod agents;

/// Whether `args` (everything after the codex executable) is the desktop's
/// stdio app-server launch. Anything else runs genuine Codex unchanged.
pub fn is_app_server_launch(args: &[String]) -> bool {
    if is_desktop_proxy(args) {
        return true;
    }
    let Some(position) = args.iter().position(|arg| arg == "app-server") else {
        return false;
    };
    config_flags_only(&args[..position], false) && config_flags_only(&args[position + 1..], true)
}

/// Desktop SSH carries a WebSocket over the proxy's stdin/stdout, rather than
/// the JSON lines used by a local desktop launch.
fn is_desktop_proxy(args: &[String]) -> bool {
    args == ["app-server", "proxy"]
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
    ClaudeConnected(mpsc::Sender<Value>),
}

enum Continuation {
    Ignore,
    Probe { thread_id: String, request: Value },
    ClaudeModels,
    Adopt { thread_id: String, turn: Value },
    Resume { thread_id: String, request: Value },
    Section { thread_id: String, request: Value },
    StartDefault { request: Value },
    Merge { merge: u64, side: Side },
    Delegation { side: Side, step: agents::Step },
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
    codex_pages: Vec<Value>,
    claude: Vec<Value>,
    claude_done: bool,
}

struct Forwarded {
    method: String,
    params: Value,
    delegation_scope: Option<String>,
}

#[derive(Clone)]
struct FreshThread {
    params: Value,
    metadata: Value,
}

pub struct Router {
    client: mpsc::Sender<Value>,
    codex: mpsc::Sender<Value>,
    claude: Option<mpsc::Sender<Value>>,
    claude_initialized: bool,
    claude_configured: bool,
    threads: HashMap<String, Side>,
    /// Codex threads started on this connection that have not run a turn;
    /// the desktop prewarms these before the user picks a model.
    fresh: HashMap<String, FreshThread>,
    initialize_params: Option<Value>,
    claude_loaded: HashSet<String>,
    /// Actual providers behind the desktop prediction compatibility alias.
    prediction_providers: HashMap<String, String>,
    /// Claude turns the desktop saw start and not yet finish, by thread. The
    /// desktop's connection outlives a facade restart, so if the facade goes
    /// away first, only the router can end them.
    claude_turns: HashMap<String, Value>,
    /// Facade server requests (as presented to the desktop) awaiting an answer,
    /// with their thread.
    claude_requests: HashMap<String, String>,
    forwarded: HashMap<(Side, String), Forwarded>,
    internal: HashMap<String, Continuation>,
    next_internal: u64,
    claude_models: Option<Vec<Value>>,
    known_claude_models: HashSet<String>,
    models_requested: bool,
    merges: HashMap<u64, Merge>,
    next_merge: u64,
    /// Client requests waiting for a routing fact, in arrival order.
    queue: VecDeque<Value>,
    /// Every request waits while the Claude catalog or the saved default model
    /// decides where a new thread goes.
    blocked: Option<Block>,
    /// Requests for these threads wait while their ownership, subscription or
    /// adoption is settled. Other chats, including genuine Codex ones, proceed.
    blocked_threads: HashMap<String, Block>,
    draining: bool,
    redrain: bool,
    agents: agents::State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Block {
    Probe,
    Models,
    Adopt,
    Resume,
    Section,
    DefaultModel,
}

/// Run the router on stdio until the desktop or genuine Codex disconnects.
pub async fn run(
    codex: &Path,
    args: &[String],
    claude_socket: Option<PathBuf>,
    log: Option<PathBuf>,
) -> Result<()> {
    let websocket = is_desktop_proxy(args);
    let proxy_args = [
        "-c".to_owned(),
        "features.code_mode_host=true".to_owned(),
        "app-server".to_owned(),
    ];
    let args = if is_desktop_proxy(args) {
        &proxy_args[..]
    } else {
        args
    };
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
    let client_writer = if websocket {
        let events = events.clone();
        tokio::spawn(async move {
            let config = WebSocketConfig::default()
                .max_message_size(Some(MAX_FRAME))
                .max_frame_size(Some(MAX_FRAME));
            let stream = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
            match tokio_tungstenite::accept_async_with_config(stream, Some(config)).await {
                Ok(mut socket) => loop {
                    tokio::select! {
                        message = client_output.recv() => {
                            let Some(message) = message else {
                                break;
                            };
                            if socket.send(Message::Text(message.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                        message = socket.next() => match message {
                            Some(Ok(Message::Text(text))) => match serde_json::from_str(&text) {
                                Ok(message) => {
                                    if events.send(Input::Client(message)).await.is_err() {
                                        break;
                                    }
                                },
                                Err(error) => {
                                    note!("invalid SSH WebSocket RPC: {error}");
                                    break;
                                }
                            },
                            Some(Ok(Message::Ping(bytes))) => {
                                if socket.send(Message::Pong(bytes)).await.is_err() {
                                    break;
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {},
                            _ => break,
                        }
                    }
                },
                Err(error) => note!("SSH WebSocket handshake failed: {error}"),
            }
            let _ = events.send(Input::ClientClosed).await;
        })
    } else {
        spawn_lines(
            tokio::io::stdin(),
            events.clone(),
            Input::Client,
            Input::ClientClosed,
        );
        tokio::spawn(async move {
            let mut stdout = tokio::io::stdout();
            while let Some(message) = client_output.recv().await {
                let mut line = message.to_string();
                line.push('\n');
                if stdout.write_all(line.as_bytes()).await.is_err() || stdout.flush().await.is_err()
                {
                    break;
                }
            }
        })
    };

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

    let claude = match &claude_socket {
        Some(path) => match connect_claude(path, events.clone(), false).await {
            Ok(sender) => Some(sender),
            Err(error) => {
                note!("Claude models are unavailable ({error:#}); serving Codex only");
                None
            }
        },
        None => None,
    };

    let mut reconnecting = false;
    if claude.is_none()
        && let Some(path) = &claude_socket
    {
        reconnect_claude(path.clone(), events.clone());
        reconnecting = true;
    }
    let mut router = Router::new(client, codex_sender, claude, claude_socket.is_some());
    let (calls, mut agent_calls) = mpsc::channel(64);
    router.agents.calls = Some(calls);
    if let Some(socket) = &claude_socket {
        router.agents.registry =
            crate::delegation::Registry::open(socket.with_extension("agents"))?;
    }
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        let input = tokio::select! {
            input = receiver.recv() => match input { Some(input) => input, None => break },
            Some(call) = agent_calls.recv() => { router.agent_mcp_call(call); continue; },
            _ = tick.tick() => { router.agent_tick(); continue; },
        };
        match input {
            Input::Client(message) => router.client_message(message),
            Input::Backend(side, message) => router.backend_message(side, message),
            Input::BackendClosed(Side::Claude) => {
                router.claude_failed();
                if !reconnecting && let Some(path) = &claude_socket {
                    reconnect_claude(path.clone(), events.clone());
                    reconnecting = true;
                }
            }
            Input::ClaudeConnected(sender) => {
                reconnecting = false;
                router.claude_connected(sender);
            }
            Input::ClientClosed | Input::BackendClosed(Side::Codex) => break,
        }
    }
    router.stop_agents();
    // Native Claude sessions live in the separate service. Wait for interruption
    // acknowledgements before closing its transport; never leave delegated work
    // running merely because the desktop disconnected.
    let shutdown = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while router.agents.has_shutdown_work() && tokio::time::Instant::now() < shutdown {
        match tokio::time::timeout_at(shutdown, receiver.recv()).await {
            Ok(Some(Input::Backend(side, message))) => router.backend_message(side, message),
            Ok(Some(Input::BackendClosed(side))) => router.agent_backend_failed(side),
            _ => break,
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

async fn connect_claude(
    path: &Path,
    events: mpsc::Sender<Input>,
    announce: bool,
) -> Result<mpsc::Sender<Value>> {
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
    // Announce before starting the reader so even an immediate close is
    // ordered after the connection event, never mistaken for the old socket.
    if announce {
        events
            .send(Input::ClaudeConnected(sender.clone()))
            .await
            .map_err(|_| anyhow::anyhow!("Desktop connection closed"))?;
    }
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

/// Keep the desktop's Codex connection alive while the login service restarts.
/// No user requests are replayed: only the handshake and catalog are refreshed.
fn reconnect_claude(path: PathBuf, events: mpsc::Sender<Input>) {
    tokio::spawn(async move {
        while !events.is_closed() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if let Ok(Ok(sender)) = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                connect_claude(&path, events.clone(), true),
            )
            .await
            {
                drop(sender);
                return;
            }
        }
    });
}

/// Collaboration mode settings override the top-level model in both runtimes.
fn selected_model(params: &Value) -> Option<&str> {
    params["collaborationMode"]["settings"]["model"]
        .as_str()
        .or_else(|| params["model"].as_str())
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

/// Embed `localImage` attachments as data URLs before input reaches the facade.
///
/// The desktop can attach files that only its own process tree may open, such
/// as screenshots dragged from the capture thumbnail. The router runs in that
/// tree; the facade is a separate login-session service that macOS refuses.
/// Malformed items are left for the facade to reject.
fn inline_local_images(params: &mut Value) -> Result<(), String> {
    let Some(input) = params.get_mut("input").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    let mut inlined = 0;
    for item in input {
        let Some(path) = item["path"]
            .as_str()
            .filter(|_| item["type"] == "localImage")
        else {
            continue;
        };
        let (mime, bytes) =
            crate::translate::read_local_image(Path::new(path)).map_err(|error| error.message)?;
        let url = format!("data:{mime};base64,{}", STANDARD.encode(bytes));
        inlined += url.len();
        if inlined > MAX_INLINED_IMAGES {
            return Err("Attached images are too large to send together".into());
        }
        let item = item.as_object_mut().expect("typed input items are objects");
        item.remove("path");
        item.insert("type".into(), json!("image"));
        item.insert("url".into(), json!(url));
    }
    Ok(())
}

fn claude_list_query(params: &Value, cursor: Option<Value>) -> Value {
    let mut query = json!({"limit": 1000});
    for field in CLAUDE_LIST_FILTERS {
        if let Some(value) = params.get(*field).filter(|v| !v.is_null()) {
            query[*field] = value.clone();
        }
    }
    // Present and null is a filter of its own: threads in no section.
    if let Some(section) = params.get("sectionId") {
        query["sectionId"] = section.clone();
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
        claude_configured: bool,
    ) -> Self {
        Self {
            client,
            codex,
            claude,
            claude_initialized: false,
            claude_configured,
            threads: HashMap::new(),
            fresh: HashMap::new(),
            initialize_params: None,
            claude_loaded: HashSet::new(),
            prediction_providers: HashMap::new(),
            claude_turns: HashMap::new(),
            claude_requests: HashMap::new(),
            forwarded: HashMap::new(),
            internal: HashMap::new(),
            next_internal: 1,
            claude_models: None,
            known_claude_models: HashSet::new(),
            models_requested: false,
            merges: HashMap::new(),
            next_merge: 1,
            queue: VecDeque::new(),
            blocked: None,
            blocked_threads: HashMap::new(),
            draining: false,
            redrain: false,
            agents: agents::State::default(),
        }
    }

    fn send_client(&self, message: Value) {
        // A stalled desktop must not stall the backends indefinitely; the
        // desktop restarts the app-server when its connection breaks.
        if self.client.try_send(message).is_err() {
            note!("desktop output queue is full or closed");
        }
    }

    fn send_backend_checked(&self, side: Side, message: Value) -> bool {
        let sender = match side {
            Side::Codex => Some(&self.codex),
            Side::Claude => self.claude.as_ref(),
        };
        if sender.is_none_or(|sender| sender.try_send(message).is_err()) {
            note!("{side:?} input queue is full or closed");
            return false;
        }
        true
    }

    fn send_backend(&mut self, side: Side, message: Value) {
        self.send_backend_checked(side, message);
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
        if !self.send_backend_checked(side, json!({"id": id, "method": method, "params": params})) {
            let continuation = self.internal.remove(&id).unwrap();
            self.continue_with(
                continuation,
                Err(json!({"code":-32000,"message":"Native backend input queue is unavailable"})),
            );
        }
    }

    fn forward(&mut self, side: Side, mut message: Value) {
        if side == Side::Claude && !self.claude_available() {
            if let Some(id) = message.get("id") {
                self.send_client(error(
                    id.clone(),
                    "The Claude service is reconnecting; retry this request shortly",
                ));
            }
            return;
        }
        if side == Side::Claude
            && message["params"]["modelProvider"] == "openai"
            && let Some(provider) = message["params"]["threadId"]
                .as_str()
                .and_then(|thread| self.prediction_providers.get(thread))
        {
            // The desktop can echo the resume response's compatibility alias.
            // Resolve it only after thread ownership has selected Claude.
            message["params"]["modelProvider"] = json!(provider);
        }
        if side == Side::Claude
            && matches!(
                message["method"].as_str(),
                Some("turn/start" | "turn/steer" | "turn/interrupt" | "thread/compact/start")
            )
            && let Some(thread) = message["params"]["threadId"].as_str()
            && !self.claude_loaded.contains(thread)
        {
            let thread_id = thread.to_owned();
            self.block_thread(&thread_id, Block::Resume);
            self.internal_request(
                Side::Claude,
                "thread/resume",
                json!({"threadId":thread_id,"excludeTurns":true}),
                Continuation::Resume {
                    thread_id: thread_id.clone(),
                    request: message,
                },
            );
            return;
        }
        if side == Side::Claude
            && message["method"] == "thread/resume"
            && message["params"]["threadId"]
                .as_str()
                .is_some_and(|thread| self.agents.registry.agents.contains_key(thread))
            && let Some(params) = message["params"].as_object_mut()
        {
            // Desktop supplies its own launch settings when opening a chat.
            // A delegated session owns its inherited permissions, instructions
            // and tools: opening it only hydrates and subscribes, including while
            // its turn is active. Explicit settings/turn requests still apply.
            params.retain(|key, _| {
                matches!(
                    key.as_str(),
                    "threadId" | "path" | "excludeTurns" | "initialTurnsPage"
                )
            });
        }
        if side == Side::Claude
            && message["method"]
                .as_str()
                .is_some_and(|method| INPUT_METHODS.contains(&method))
            && let Some(params) = message.get_mut("params")
            && let Err(failure) = inline_local_images(params)
        {
            self.send_client(error(message["id"].clone(), failure));
            return;
        }
        let delegation_scope = match self.prepare_agent_tools(side, &mut message) {
            Ok(scope) => scope,
            Err(failure) => {
                self.send_client(error(message["id"].clone(), failure.to_string()));
                return;
            }
        };
        self.invalidate_agent_snapshot(&message);
        if message["method"] == "turn/interrupt"
            && let Some(thread) = message["params"]["threadId"].as_str()
        {
            self.interrupt_descendants(thread);
        }
        if let Some(id) = message.get("id") {
            self.forwarded.insert(
                (side, id_key(id)),
                Forwarded {
                    method: message["method"].as_str().unwrap_or("").to_owned(),
                    params: message["params"].clone(),
                    delegation_scope,
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
        if self.known_claude_models.contains(model)
            || model.starts_with("claude-")
            || ["opus", "sonnet", "haiku", "fable"].contains(&model)
        {
            return Some(true);
        }
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
        self.drain();
    }

    /// Hold one thread's traffic until a routing fact about it arrives. The
    /// request that needed the fact is held by its continuation.
    fn block_thread(&mut self, thread: &str, reason: Block) {
        self.blocked_threads.insert(thread.to_owned(), reason);
    }

    fn unblock_thread(&mut self, thread: &str, reason: Block) {
        if self.blocked_threads.get(thread) != Some(&reason) {
            return;
        }
        self.blocked_threads.remove(thread);
        self.drain();
    }

    /// Whether a request must stay behind an outstanding routing fact or an
    /// earlier waiting request it may depend on: one for the same thread, or
    /// one without a thread.
    fn must_wait(&self, message: &Value, earlier: &VecDeque<Value>) -> bool {
        if self.blocked.is_some() {
            return true;
        }
        let thread = message["params"]["threadId"].as_str();
        if thread.is_some_and(|thread| self.blocked_threads.contains_key(thread)) {
            return true;
        }
        earlier.iter().any(|queued| {
            let queued = queued["params"]["threadId"].as_str();
            queued.is_none() || queued == thread
        })
    }

    /// Dispatch every queued request that no longer has to wait, in order.
    fn drain(&mut self) {
        // A dispatch can settle another routing fact synchronously.
        if self.draining {
            self.redrain = true;
            return;
        }
        self.draining = true;
        loop {
            self.redrain = false;
            let mut waiting = VecDeque::new();
            while let Some(message) = self.queue.pop_front() {
                if self.must_wait(&message, &waiting) {
                    waiting.push_back(message);
                } else {
                    self.dispatch(message);
                }
            }
            self.queue = waiting;
            if !self.redrain {
                break;
            }
        }
        self.draining = false;
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
        if self.must_wait(&message, &self.queue) {
            self.queue.push_back(message);
        } else {
            self.dispatch(message);
        }
    }

    fn client_response(&mut self, mut message: Value) {
        if let Some(id) = message["id"].as_str() {
            self.claude_requests.remove(id);
        }
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
            if method == "initialized" {
                self.recover_agents();
            }
            return;
        };
        let params = message["params"].clone();
        match method.as_str() {
            "initialize" => {
                self.initialize_params = Some(params.clone());
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
                    Some(_) => self.start_merge(
                        id,
                        MergeKind::Threads {
                            search: method == "thread/search",
                        },
                        params,
                    ),
                    None => self.start_merge(
                        id,
                        MergeKind::Threads {
                            search: method == "thread/search",
                        },
                        params,
                    ),
                }
            }
            "thread/start" => match selected_model(&params) {
                Some(model) => match self.is_claude_model(model) {
                    None => {
                        // Block first: a failed request completes synchronously.
                        self.block(Block::Models, message);
                        self.request_claude_models();
                    }
                    Some(true) => self.forward(Side::Claude, message),
                    Some(false) => self.forward(Side::Codex, message),
                },
                None => {
                    // The desktop saves its picked model as a Codex config
                    // default, then can prewarm without an explicit model.
                    // Resolve it in this cwd instead of assuming the default
                    // belongs to Codex or caching another project's config.
                    self.blocked = Some(Block::DefaultModel);
                    self.internal_request(
                        Side::Codex,
                        "config/read",
                        json!({"cwd":params["cwd"],"includeLayers":false}),
                        Continuation::StartDefault { request: message },
                    );
                }
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
                self.block_thread(&thread_id, Block::Probe);
                self.internal_request(
                    Side::Claude,
                    "thread/read",
                    json!({"threadId": thread_id}),
                    Continuation::Probe {
                        thread_id: thread_id.clone(),
                        request: message,
                    },
                );
                return;
            }
            None if self.claude_configured => {
                self.send_client(error(
                    message["id"].clone(),
                    "The Claude service is reconnecting; thread ownership cannot be checked yet",
                ));
                return;
            }
            None => Side::Codex,
        };
        let method = message["method"].as_str().unwrap_or("").to_owned();
        if let Some(model) = selected_model(&message["params"]).map(str::to_owned) {
            let Some(claude_model) = self.is_claude_model(&model) else {
                self.block(Block::Models, message);
                self.request_claude_models();
                return;
            };
            if claude_model != (side == Side::Claude) {
                if matches!(method.as_str(), "turn/start" | "thread/settings/update")
                    && side == Side::Codex
                    && self.fresh.contains_key(thread)
                {
                    self.adopt(thread, message);
                } else {
                    self.send_client(error(
                        message["id"].clone(),
                        match side {
                            Side::Claude => {
                                format!("This chat runs on Claude; start a new chat to use {model}")
                            }
                            Side::Codex => {
                                format!("This chat runs on Codex; start a new chat to use {model}")
                            }
                        },
                    ));
                }
                return;
            }
        }
        if side == Side::Claude && APP_METHODS.contains(&method.as_str()) {
            // Genuine Codex serves apps; it does not know the Claude thread.
            let mut message = message;
            if let Some(params) = message["params"].as_object_mut() {
                params.remove("threadId");
            }
            self.forward(Side::Codex, message);
            return;
        }
        if method == "thread/section/move" {
            self.move_to_section(side, message);
            return;
        }
        if side == Side::Codex && method == "turn/start" {
            self.fresh.remove(thread);
        }
        self.forward(side, message);
    }

    /// Each backend files its own threads, and neither can place a thread
    /// relative to the other's: a `beforeThreadId` on the other backend
    /// appends instead. Sections belong to genuine Codex, so a Claude thread's
    /// destination is first looked up there.
    fn move_to_section(&mut self, side: Side, mut message: Value) {
        let foreign_before = message["params"]["beforeThreadId"]
            .as_str()
            .is_some_and(|before| self.threads.get(before).is_some_and(|owner| *owner != side));
        if foreign_before {
            message["params"]["beforeThreadId"] = Value::Null;
        }
        if side == Side::Codex
            || message["params"]["sectionId"]
                .as_str()
                .is_none_or(|section| section.trim().is_empty())
        {
            self.forward(side, message);
            return;
        }
        let thread_id = message["params"]["threadId"]
            .as_str()
            .unwrap_or("")
            .to_owned();
        self.block_thread(&thread_id, Block::Section);
        self.internal_request(
            Side::Codex,
            "threadSection/list",
            json!({"cursor": null, "limit": 100}),
            Continuation::Section {
                thread_id,
                request: message,
            },
        );
    }

    /// A deleted thread's id never returns: forget its ownership and state.
    fn forget_thread(&mut self, thread: &str) {
        self.threads.remove(thread);
        self.fresh.remove(thread);
        self.claude_loaded.remove(thread);
        self.prediction_providers.remove(thread);
        self.claude_turns.remove(thread);
        self.agent_thread_deleted(thread);
    }

    /// Claude threads keep their own copy of the section they are filed in.
    fn section_changed(&mut self, section_id: Value, section: Value) {
        if self.claude_available() && section_id.is_string() {
            self.internal_request(
                Side::Claude,
                SECTION_CHANGED_METHOD,
                json!({"sectionId": section_id, "section": section}),
                Continuation::Ignore,
            );
        }
    }

    /// Move the desktop's prewarmed, still-empty Codex thread to Claude under
    /// the same id, then run the requested turn there.
    fn adopt(&mut self, thread: &str, turn: Value) {
        if !self.claude_available() {
            self.send_client(error(
                turn["id"].clone(),
                "The Claude service is reconnecting; retry this request shortly",
            ));
            return;
        }
        let fresh = self
            .fresh
            .get(thread)
            .expect("fresh thread checked before adoption")
            .clone();
        let mut params = fresh.params;
        if let Some(config) = params["config"].as_object_mut() {
            config.remove("mcp_servers.claudex_agents");
        }
        if let Some(tools) = params["dynamicTools"].as_array_mut() {
            tools.retain(|tool| *tool != crate::delegation::dynamic_spec());
        }
        let mut start = json!({"method":"thread/start","params":params});
        if let Err(failure) = self.prepare_agent_tools(Side::Claude, &mut start) {
            self.send_client(error(turn["id"].clone(), failure.to_string()));
            return;
        }
        let mut params = start["params"].clone();
        params["model"] = json!(selected_model(&turn["params"]));
        if let Some(effort) = turn["params"].get("effort").filter(|v| !v.is_null()) {
            params["effort"] = effort.clone();
        }
        let thread_id = thread.to_owned();
        self.block_thread(&thread_id, Block::Adopt);
        self.internal_request(
            Side::Claude,
            ADOPT_THREAD_METHOD,
            json!({"threadId": thread_id, "params": params, "metadata": {
                "projectId":fresh.metadata["projectId"],
                "gitInfo":fresh.metadata["gitInfo"],
                "daybreakEnabled":fresh.metadata["daybreakEnabled"]
            }}),
            Continuation::Adopt {
                thread_id: thread_id.clone(),
                turn,
            },
        );
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
        if matches!(kind, MergeKind::Threads { search: false }) && agents::shared_listing(&params) {
            codex_params.as_object_mut().unwrap().remove("cursor");
            codex_params["limit"] = json!(1000);
            boundary = None;
        }
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
                codex_pages: vec![],
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
            (Side::Codex, Ok(mut page))
                if matches!(state.kind, MergeKind::Threads { search: false })
                    && agents::shared_listing(&state.params) =>
            {
                state
                    .codex_pages
                    .extend(page["data"].as_array().cloned().unwrap_or_default());
                if let Some(cursor) = page["nextCursor"].as_str() {
                    let mut query = state.params.clone();
                    query["limit"] = json!(1000);
                    query["cursor"] = json!(cursor);
                    next_page = Some(("thread/list", query));
                } else {
                    page["data"] = json!(std::mem::take(&mut state.codex_pages));
                    state.codex = Some(Ok(page));
                }
            }
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
            self.internal_request(side, method, query, Continuation::Merge { merge, side });
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
                    let mut seen = HashSet::new();
                    data.retain(|id| seen.insert(id.to_string()));
                }
            }
            MergeKind::Threads { .. } => {
                let field = sort_field(&state.params);
                let descending = descending(&state.params);
                // Codex orders a section by its own positions, which cannot
                // interleave with the facade's: Claude's members follow Codex's.
                let section_order = state.params["sortKey"] == "section_position";
                let data = result["data"].as_array().cloned().unwrap_or_default();
                // An adopted prewarm can still exist in Codex's metadata.
                // Claude owns that id; never publish both copies or let the
                // Codex copy overwrite it when the desktop hydrates its list.
                for thread in &state.claude {
                    if let Some(id) = thread["id"].as_str() {
                        self.threads.insert(id.to_owned(), Side::Claude);
                    }
                }
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
                let mut merged: Vec<Value> = data
                    .into_iter()
                    .filter(|item| {
                        list_thread(item)["id"]
                            .as_str()
                            .is_none_or(|id| self.threads.get(id) != Some(&Side::Claude))
                    })
                    .collect();
                // The facade's thread/list predates the originator filter.
                let originators = state.params["originators"]
                    .as_array()
                    .filter(|list| !list.is_empty());
                for thread in state.claude {
                    if let Some(id) = thread["id"].as_str() {
                        self.threads.insert(id.to_owned(), Side::Claude);
                    }
                    let placed = if section_order {
                        last_page
                    } else {
                        in_window(sort_value(&thread, field))
                    };
                    if originators.is_none_or(|list| list.contains(&thread["originator"])) && placed
                    {
                        merged.push(thread);
                    }
                }
                if !section_order {
                    merged.sort_by_key(|item| {
                        let value = sort_value(item, field);
                        if descending { -value } else { value }
                    });
                }
                result["data"] = json!(merged);
                self.decorate_agent_listing(&state.params, &mut result);
                if matches!(state.kind, MergeKind::Threads { search: false })
                    && agents::shared_listing(&state.params)
                    && let Err(failure) = agents::paginate_listing(&state.params, &mut result)
                {
                    return error(state.client_id, failure.to_string());
                }
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
                if self.agent_dynamic_call(side, &message) {
                    return;
                }
                if side == Side::Claude {
                    let request_id = format!("{CLAUDE_REQUEST_ID}{id}");
                    if let Some(thread) = message["params"]["threadId"].as_str() {
                        self.claude_requests
                            .insert(request_id.clone(), thread.to_owned());
                    }
                    message["id"] = json!(request_id);
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
                if method == "thread/deleted" {
                    for id in &ids {
                        self.forget_thread(id);
                    }
                }
                self.agent_notification(side, &message);
                self.decorate_agent_message(&mut message);
                if side == Side::Codex && method == "turn/started" {
                    for id in &ids {
                        self.fresh.remove(id);
                    }
                }
                if side == Side::Claude
                    && method == "serverRequest/resolved"
                    && let Some(request) = message["params"].get("requestId").cloned()
                {
                    let request_id = format!("{CLAUDE_REQUEST_ID}{request}");
                    self.claude_requests.remove(&request_id);
                    message["params"]["requestId"] = json!(request_id);
                }
                if side == Side::Claude
                    && let Some(thread) = message["params"]["threadId"].as_str()
                {
                    match method.as_str() {
                        "turn/started" => {
                            self.claude_turns
                                .insert(thread.to_owned(), message["params"]["turn"].clone());
                        }
                        "turn/completed" => {
                            self.claude_turns.remove(thread);
                        }
                        _ => {}
                    }
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

    fn backend_response(&mut self, side: Side, id: Value, mut message: Value) {
        let result = match message.get("error") {
            Some(error) => Err(error.clone()),
            None => Ok(message["result"].clone()),
        };
        if let Ok(result) = &result {
            self.remember_agent_snapshot(result);
        }
        if let Some(continuation) = id.as_str().and_then(|id| self.internal.remove(id)) {
            self.continue_with(continuation, result);
            return;
        }
        let forwarded = self.forwarded.remove(&(side, id_key(&id)));
        if let Some(forwarded) = &forwarded {
            if result.is_ok()
                && matches!(
                    forwarded.method.as_str(),
                    "thread/unsubscribe" | "thread/archive"
                )
                && let Some(thread) = forwarded.params["threadId"].as_str()
            {
                self.agent_thread_unloaded(thread);
                // Its later turn events no longer reach this connection.
                self.claude_turns.remove(thread);
            }
            if result.is_ok()
                && forwarded.method == "thread/delete"
                && let Some(thread) = forwarded.params["threadId"].as_str()
            {
                self.forget_thread(thread);
            }
            if side == Side::Codex
                && let Ok(result) = &result
            {
                match forwarded.method.as_str() {
                    "threadSection/update" => self.section_changed(
                        result["section"]["id"].clone(),
                        result["section"].clone(),
                    ),
                    "threadSection/delete" => {
                        self.section_changed(forwarded.params["sectionId"].clone(), Value::Null)
                    }
                    _ => {}
                }
            }
            if result
                .as_ref()
                .is_ok_and(|result| result["turn"]["status"] == "inProgress")
                && forwarded.method == "turn/start"
            {
                self.agent_notification(side,&json!({"method":"turn/started","params":{"threadId":forwarded.params["threadId"],"turn":message["result"]["turn"]}}));
            }
            self.decorate_agent_pages(&forwarded.method, &forwarded.params, &mut message);
            if side == Side::Claude
                && forwarded.method == "thread/resume"
                && message["result"]["claudexPredictionFork"] == true
                && let Some(thread) = message["result"]["thread"]["id"].as_str()
                && let Some(provider) = message["result"]["modelProvider"].as_str()
            {
                self.prediction_providers
                    .insert(thread.into(), provider.into());
                // The unmodified desktop gates composer predictions on this
                // top-level field. Keep the actual model, nested thread provider,
                // persisted metadata and backend routing unchanged.
                message["result"]["modelProvider"] = json!("openai");
            }
        }
        if side == Side::Claude
            && let (Some(forwarded), Err(error)) = (&forwarded, &result)
        {
            note!("Claude {} failed: {error}", forwarded.method);
        }
        if let Some(forwarded) = forwarded
            && let Ok(result) = &result
            && let Some(thread) = result["thread"]["id"].as_str()
        {
            if let Some(scope) = &forwarded.delegation_scope {
                self.agents.scopes.insert(scope.clone(), thread.to_owned());
                self.agents.attached.insert(thread.to_owned());
            }
            if side == Side::Claude && forwarded.method == "thread/resume" {
                self.internal_request(
                    Side::Claude,
                    "claudex/delegation/attach",
                    json!({"threadId":thread}),
                    Continuation::Ignore,
                );
            }
            self.threads.insert(thread.to_owned(), side);
            if side == Side::Claude
                && matches!(forwarded.method.as_str(), "thread/start" | "thread/resume")
            {
                self.claude_loaded.insert(thread.to_owned());
            }
            if side == Side::Codex && forwarded.method == "thread/start" {
                self.fresh.insert(
                    thread.to_owned(),
                    FreshThread {
                        params: forwarded.params,
                        metadata: result["thread"].clone(),
                    },
                );
            } else if side == Side::Codex
                && forwarded.method == "thread/metadata/update"
                && let Some(fresh) = self.fresh.get_mut(thread)
            {
                fresh.metadata = result["thread"].clone();
            }
        }
        self.decorate_agent_message(&mut message);
        self.send_client(message);
    }

    fn continue_with(&mut self, continuation: Continuation, result: Result<Value, Value>) {
        match continuation {
            Continuation::Ignore => {}
            Continuation::Probe { thread_id, request } => {
                let blocked = thread_id.clone();
                match result {
                    Ok(_) => {
                        self.threads.insert(thread_id, Side::Claude);
                        self.dispatch(request);
                    }
                    Err(failure)
                        if failure["code"] == -32602
                            && failure["message"]
                                .as_str()
                                .is_some_and(|m| m.starts_with("Thread not found:")) =>
                    {
                        self.threads.insert(thread_id, Side::Codex);
                        self.dispatch(request);
                    }
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock_thread(&blocked, Block::Probe);
            }
            Continuation::ClaudeModels => {
                let models: Vec<Value> = match result {
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
                for model in &models {
                    if let Some(id) = model["id"].as_str() {
                        self.known_claude_models.insert(id.to_owned());
                    }
                }
                self.claude_models = Some(models);
                self.finish_merges();
                self.unblock(Block::Models);
            }
            Continuation::Adopt { thread_id, turn } => {
                let blocked = thread_id.clone();
                match result {
                    Ok(_) => {
                        self.fresh.remove(&thread_id);
                        self.threads.insert(thread_id.clone(), Side::Claude);
                        self.claude_loaded.insert(thread_id.clone());
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
                self.unblock_thread(&blocked, Block::Adopt);
            }
            Continuation::Resume { thread_id, request } => {
                let blocked = thread_id.clone();
                match result {
                    Ok(_) => {
                        self.claude_loaded.insert(thread_id);
                        self.forward(Side::Claude, request);
                    }
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock_thread(&blocked, Block::Resume);
            }
            Continuation::Section {
                thread_id,
                mut request,
            } => {
                let wanted = request["params"]["sectionId"]
                    .as_str()
                    .unwrap_or("")
                    .to_owned();
                match result {
                    Ok(page) => {
                        let section = page["data"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .find(|section| section["id"] == wanted.as_str())
                            .cloned();
                        match (section, page["nextCursor"].as_str()) {
                            (Some(section), _) => {
                                request["params"]["claudexSection"] = section;
                                self.forward(Side::Claude, request);
                            }
                            (None, Some(cursor)) => {
                                // The thread stays blocked while the next page is read.
                                self.internal_request(
                                    Side::Codex,
                                    "threadSection/list",
                                    json!({"cursor": cursor, "limit": 100}),
                                    Continuation::Section { thread_id, request },
                                );
                                return;
                            }
                            (None, None) => self.send_client(error(
                                request["id"].clone(),
                                format!("section {wanted} does not exist"),
                            )),
                        }
                    }
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock_thread(&thread_id, Block::Section);
            }
            Continuation::StartDefault { mut request } => {
                match result {
                    Ok(result) => match result["config"]["model"].as_str() {
                        Some(model) if request["params"].is_object() => {
                            request["params"]["model"] = json!(model);
                            self.dispatch(request);
                        }
                        _ => self.forward(Side::Codex, request),
                    },
                    Err(failure) => self.send_client(json!({"id":request["id"],"error":failure})),
                }
                self.unblock(Block::DefaultModel);
            }
            Continuation::Merge { merge, side } => self.merge_part(merge, side, result),
            Continuation::Delegation { side, step } => self.agent_continue(side, step, result),
        }
    }

    /// The facade went away: fail pending Claude work without changing ownership.
    fn claude_failed(&mut self) {
        if self.claude.take().is_none() {
            return;
        }
        note!("lost the Claude facade connection");
        self.agent_backend_failed(Side::Claude);
        self.claude_initialized = false;
        self.claude_loaded.clear();
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
                | Some(Continuation::Resume { .. })
                | Some(Continuation::Adopt { .. }) => self.internal.remove(&id).unwrap(),
                Some(Continuation::Delegation {
                    side: Side::Claude, ..
                }) => self.internal.remove(&id).unwrap(),
                _ => continue,
            };
            self.continue_with(continuation, Err(failure.clone()));
        }
        // The facade ends its turns on shutdown, but that notification cannot
        // reach the desktop once the connection is gone, and a crash sends none.
        // Its saved record marks the same turns interrupted when it restarts.
        for (request_id, thread_id) in std::mem::take(&mut self.claude_requests) {
            self.send_client(json!({"method":"serverRequest/resolved",
                "params":{"threadId":thread_id,"requestId":request_id}}));
        }
        let completed_at = crate::protocol::now();
        for (thread_id, mut turn) in std::mem::take(&mut self.claude_turns) {
            turn["status"] = json!("interrupted");
            turn["error"] = json!({"message":"The Claude service stopped while this turn was running",
                "codexErrorInfo":null,"additionalDetails":null,"misalignment":null});
            // Items already streamed to the desktop stay as they are.
            turn["items"] = json!([]);
            turn["itemsView"] = json!("notLoaded");
            turn["completedAt"] = json!(completed_at);
            if let Some(started_at) = turn["startedAt"].as_u64() {
                turn["durationMs"] = json!(completed_at.saturating_sub(started_at) * 1000);
            }
            self.send_client(
                json!({"method":"turn/completed","params":{"threadId":thread_id,"turn":turn}}),
            );
            self.send_client(json!({"method":"thread/status/changed",
                "params":{"threadId":thread_id,"status":{"type":"idle"}}}));
        }
    }

    fn claude_connected(&mut self, sender: mpsc::Sender<Value>) {
        note!("reconnected to the Claude facade");
        self.claude = Some(sender);
        self.models_requested = false;
        if let Some(params) = self.initialize_params.clone() {
            self.claude_initialized = true;
            self.internal_request(Side::Claude, "initialize", params, Continuation::Ignore);
            self.send_backend(Side::Claude, json!({"method":"initialized","params":{}}));
            self.request_claude_models();
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
    fn recognizes_desktop_stdio_and_default_ssh_proxy_launches() {
        assert!(is_app_server_launch(&strings(&[
            "-c",
            "features.code_mode_host=true",
            "app-server",
            "--analytics-default-enabled",
            "-c",
            "chatgpt_base_url=\"x\"",
        ])));
        assert!(is_app_server_launch(&strings(&["app-server"])));
        assert!(is_app_server_launch(&strings(&["app-server", "proxy"])));
        for other in [
            vec!["--version"],
            vec!["exec", "hi"],
            vec!["app-server", "proxy", "--sock", "/custom.sock"],
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

    #[tokio::test]
    async fn failed_ownership_probe_never_forwards_a_turn_to_codex() {
        let (client, mut replies) = mpsc::channel(16);
        let (codex, mut codex_requests) = mpsc::channel(16);
        let (claude, mut claude_requests) = mpsc::channel(16);
        let mut router = Router::new(client, codex, Some(claude), true);
        router.client_message(json!({"id":0,"method":"initialize","params":{}}));
        assert_eq!(codex_requests.recv().await.unwrap()["method"], "initialize");
        assert_eq!(
            claude_requests.recv().await.unwrap()["method"],
            "initialize"
        );
        router.client_message(json!({"id":1,"method":"turn/start","params":{
            "threadId":"unknown-owner","input":[{"type":"text","text":"first message","text_elements":[]}]}}));
        assert_eq!(
            claude_requests.recv().await.unwrap()["method"],
            "thread/read"
        );
        router.claude_failed();
        let failure = replies.recv().await.unwrap();
        assert_eq!(failure["id"], 1);
        assert_eq!(failure["error"]["code"], -32000);
        assert!(
            codex_requests.try_recv().is_err(),
            "Ownership failure forwarded a turn to Codex"
        );
        assert!(!router.threads.contains_key("unknown-owner"));
    }

    #[tokio::test]
    async fn a_pending_facade_round_trip_holds_only_its_own_thread() {
        let (client, _replies) = mpsc::channel(16);
        let (codex, mut codex_requests) = mpsc::channel(16);
        let (claude, mut claude_requests) = mpsc::channel(16);
        let mut router = Router::new(client, codex, Some(claude), true);
        router.client_message(json!({"id":0,"method":"initialize","params":{}}));
        assert_eq!(codex_requests.recv().await.unwrap()["method"], "initialize");
        assert_eq!(
            claude_requests.recv().await.unwrap()["method"],
            "initialize"
        );
        router.client_message(json!({"id":1,"method":"turn/start","params":{
            "threadId":"slow","input":[{"type":"text","text":"first","text_elements":[]}]}}));
        let probe = claude_requests.recv().await.unwrap();
        assert_eq!(probe["method"], "thread/read");
        router.client_message(json!({"id":2,"method":"thread/read","params":{"threadId":"slow"}}));
        // A slow facade must not hold up genuine Codex or other chats.
        router.client_message(json!({"id":3,"method":"config/read","params":{}}));
        assert_eq!(codex_requests.try_recv().unwrap()["id"], 3);
        router.client_message(json!({"id":4,"method":"thread/read","params":{"threadId":"other"}}));
        assert_eq!(
            claude_requests.try_recv().unwrap()["params"]["threadId"],
            "other"
        );
        assert!(claude_requests.try_recv().is_err());
        router.backend_message(
            Side::Claude,
            json!({"id":probe["id"],"error":{"code":-32602,"message":"Thread not found: slow"}}),
        );
        // The thread's own requests then follow in their original order.
        assert_eq!(codex_requests.try_recv().unwrap()["id"], 1);
        assert_eq!(codex_requests.try_recv().unwrap()["id"], 2);
    }
}
