//! Bounded, bidirectional Claude Code stream-json transport.
use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{ChildStdin, Command},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
};

const MAX_FRAME: usize = 32 * 1024 * 1024;
const STDERR_TAIL: usize = 16 * 1024;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;
type SharedStdin = Arc<tokio::sync::Mutex<Option<ChildStdin>>>;

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub executable: PathBuf,
    pub extra_args: Vec<String>,
    pub initialize_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct SessionOptions {
    pub cwd: PathBuf,
    pub session_id: String,
    pub resume: bool,
    pub fork_from: Option<String>,
    pub resume_at: Option<String>,
    pub model: String,
    pub permission_mode: String,
    pub system_prompt: Option<String>,
    pub append_system_prompt: Option<String>,
    pub effort: Option<String>,
    pub output_schema: Option<Value>,
    pub ephemeral: bool,
    pub dynamic_tools: Vec<Value>,
    pub native_settings: Value,
    pub sandbox: crate::sandbox::Policy,
}

#[derive(Debug)]
pub enum BackendEvent {
    Message(Value),
    Exited { success: bool, message: String },
}

pub struct Backend {
    stdin: SharedStdin,
    pending: Pending,
    controls: Semaphore,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
    process_id: u32,
    alive: Arc<AtomicBool>,
    initialization: Value,
}

fn command(config: &BackendConfig, options: &SessionOptions) -> Result<Command> {
    let mut cmd = Command::new(&config.executable);
    let (extra_args, settings) = launch_settings(config, options)?;
    cmd.args(extra_args).args([
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--replay-user-messages",
        "--permission-prompt-tool",
        "stdio",
        // Desktop instructions and plugin catalogs can change between turns.
        // Claude's default snapshot otherwise ignores new launch instructions
        // on resume, even though the transcript is resumed correctly.
        "--system-prompt-snapshot",
        "off",
    ]);
    if let Some(settings) = settings {
        cmd.arg(format!("--settings={settings}"));
    }
    if let Some(source) = &options.fork_from {
        cmd.arg(format!("--resume={source}"))
            .arg("--fork-session")
            .arg(format!("--session-id={}", options.session_id));
    } else {
        cmd.arg(format!(
            "--{}={}",
            if options.resume {
                "resume"
            } else {
                "session-id"
            },
            options.session_id
        ));
    }
    cmd.arg(format!("--model={}", options.model));
    if let Some(message_id) = &options.resume_at {
        cmd.arg(format!("--resume-session-at={message_id}"));
    }
    cmd.arg(format!("--permission-mode={}", options.permission_mode));
    for (flag, value) in [
        ("system-prompt", &options.system_prompt),
        ("append-system-prompt", &options.append_system_prompt),
        ("effort", &options.effort),
    ] {
        if let Some(value) = value {
            cmd.arg(format!("--{flag}={value}"));
        }
    }
    if let Some(schema) = &options.output_schema {
        cmd.arg(format!("--json-schema={schema}"));
    }
    if options.ephemeral {
        cmd.arg("--no-session-persistence");
    }
    if !options.dynamic_tools.is_empty() {
        cmd.arg("--mcp-config").arg(
            json!({"mcpServers":{"codex_desktop":{"type":"sdk","name":"codex_desktop"}}})
                .to_string(),
        );
    }
    cmd.current_dir(&options.cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // The official SDK waits for idle before closing a bidirectional session.
    cmd.env("CLAUDE_CODE_SDK_READS_SESSION_STATE", "1");
    #[cfg(unix)]
    cmd.process_group(0);
    Ok(cmd)
}

/// Preserve caller-provided settings (including permission rules) while adding
/// session-local environment/plugin overrides through one native settings flag.
fn launch_settings(
    config: &BackendConfig,
    options: &SessionOptions,
) -> Result<(Vec<String>, Option<Value>)> {
    let mut args = Vec::new();
    let mut settings = json!({});
    let mut supplied = false;
    let mut sources = "user,project,local".to_owned();
    let mut overlays = Vec::new();
    let mut input = config.extra_args.iter();
    while let Some(arg) = input.next() {
        if options.sandbox.is_workspace() {
            if arg == "--setting-sources" {
                sources = input
                    .next()
                    .context("--setting-sources requires a value")?
                    .clone();
                continue;
            }
            if let Some(value) = arg.strip_prefix("--setting-sources=") {
                sources = value.to_owned();
                continue;
            }
            if [
                "--add-dir",
                "--allowedTools",
                "--allowed-tools",
                "--dangerously-skip-permissions",
                "--allow-dangerously-skip-permissions",
                "--permission-mode",
            ]
            .iter()
            .any(|flag| arg == flag || arg.starts_with(&format!("{flag}=")))
            {
                bail!(
                    "{arg} conflicts with the selected workspace sandbox; configure permissions through the facade"
                );
            }
        }
        let value = if arg == "--settings" {
            Some(
                input
                    .next()
                    .context("--settings requires JSON or a file path")?
                    .as_str(),
            )
        } else {
            arg.strip_prefix("--settings=")
        };
        if let Some(value) = value {
            let mut parsed: Value = if value.trim_start().starts_with('{') {
                serde_json::from_str(value).context("invalid Claude --settings JSON")?
            } else {
                let path = options.cwd.join(value);
                serde_json::from_slice(
                    &std::fs::read(path).context("reading Claude --settings file")?,
                )
                .context("invalid Claude --settings file JSON")?
            };
            if !parsed.is_object() {
                bail!("Claude --settings must contain an object");
            }
            if options.sandbox.is_workspace() {
                let anchor = if value.trim_start().starts_with('{') {
                    options.cwd.clone()
                } else {
                    options
                        .cwd
                        .join(value)
                        .parent()
                        .context("settings path has no parent")?
                        .to_path_buf()
                };
                crate::sandbox::rebase_rules(&mut parsed, &anchor)?;
                crate::sandbox::rebase_sandbox_paths(&mut parsed, &anchor)?;
                overlays.push(parsed);
            } else {
                merge_settings(&mut settings, parsed);
            }
            supplied = true;
        } else {
            args.push(arg.clone());
        }
    }
    if options.sandbox.is_workspace() {
        let sources: Vec<&str> = sources.split(',').filter(|s| !s.is_empty()).collect();
        if sources
            .iter()
            .any(|source| !["user", "project", "local"].contains(source))
        {
            bail!("unsupported Claude setting source");
        }
        settings = crate::sandbox::load_settings(&options.cwd, &sources)?;
        for overlay in overlays {
            crate::sandbox::merge(&mut settings, overlay);
        }
        args.extend(["--setting-sources".into(), "".into()]);
    }
    let native = options
        .native_settings
        .as_object()
        .context("native Claude settings must be an object")?;
    supplied |= !native.is_empty();
    merge_settings(&mut settings, options.native_settings.clone());
    if options.sandbox.is_workspace() {
        options.sandbox.apply_native(&mut settings, &options.cwd)?;
        supplied = true;
    }
    Ok((args, supplied.then_some(settings)))
}

fn merge_settings(target: &mut Value, overlay: Value) {
    match (target, overlay) {
        (Value::Object(target), Value::Object(overlay)) => {
            for (key, value) in overlay {
                merge_settings(target.entry(key).or_insert(Value::Null), value);
            }
        }
        (target, overlay) => *target = overlay,
    }
}

// The process group belongs to this backend, including tool subprocesses.
fn kill_group(id: u32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(id as i32), libc::SIGKILL);
    }
    #[cfg(not(unix))]
    let _ = id;
}

fn parse_frame(line: &str) -> Result<Option<Value>> {
    let line = line.trim();
    if line.is_empty() || !(line.starts_with('{') || line.starts_with('[')) {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(line).context("malformed Claude JSON frame")?;
    if !value.is_object() {
        bail!("Claude frame must be a JSON object");
    }
    Ok(Some(value))
}

// Discovery must run in the reader itself: Claude can await it before answering
// initialize, while the caller cannot consume BackendEvents until spawn returns.
fn mcp_control_response(value: &Value, tools: &[Value]) -> Option<Value> {
    if value["type"] != "control_request" || value["request"]["subtype"] != "mcp_message" {
        return None;
    }
    let request = &value["request"];
    let message = &request["message"];
    let error = |code: i32, text: &str| json!({"jsonrpc":"2.0","id":message["id"],"error":{"code":code,"message":text}});
    let response = if request["server_name"] != "codex_desktop" || tools.is_empty() {
        error(-32601, "SDK MCP server not registered")
    } else if !message.is_object() || message["jsonrpc"] != "2.0" {
        error(-32600, "Invalid JSON-RPC message")
    } else if message.get("id").is_none() {
        // Notifications have no JSON-RPC response, but their outer control
        // request still needs the SDK's empty acknowledgement.
        json!({"jsonrpc":"2.0","result":{}})
    } else {
        let result = match message["method"].as_str() {
            Some("tools/call") => return None,
            Some("initialize") => {
                let requested = message["params"]["protocolVersion"].as_str().unwrap_or("");
                let version = match requested {
                    "2024-11-05" | "2025-03-26" | "2025-06-18" | "2025-11-25" => requested,
                    _ => "2025-11-25",
                };
                json!({"protocolVersion":version,"capabilities":{"tools":{}},
                    "serverInfo":{"name":"codex_desktop","version":env!("CARGO_PKG_VERSION")}})
            }
            Some("tools/list") => json!({"tools":tools}),
            Some("ping") => json!({}),
            Some(_) => {
                return Some(mcp_response_envelope(
                    value,
                    error(-32601, "Method not found"),
                ));
            }
            None => {
                return Some(mcp_response_envelope(
                    value,
                    error(-32600, "Invalid JSON-RPC request"),
                ));
            }
        };
        json!({"jsonrpc":"2.0","id":message["id"],"result":result})
    };
    Some(mcp_response_envelope(value, response))
}

fn mcp_response_envelope(request: &Value, response: Value) -> Value {
    json!({"type":"control_response","response":{"subtype":"success",
        "request_id":request["request_id"],"response":{"mcp_response":response}}})
}

async fn write_frame(stdin: &SharedStdin, message: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_FRAME {
        bail!("Claude input exceeds {MAX_FRAME} bytes");
    }
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut stdin = stdin.lock().await;
        let stdin = stdin.as_mut().context("Claude stdin is closed")?;
        stdin
            .write_all(&bytes)
            .await
            .context("writing Claude stdin")?;
        stdin.flush().await.context("flushing Claude stdin")
    })
    .await
    .context("Claude stdin write timed out")?
}

impl Backend {
    pub async fn spawn(
        config: &BackendConfig,
        options: &SessionOptions,
    ) -> Result<(Self, mpsc::Receiver<BackendEvent>)> {
        Self::spawn_cancellable(config, options, CancellationToken::new()).await
    }

    pub async fn spawn_cancellable(
        config: &BackendConfig,
        options: &SessionOptions,
        startup_cancel: CancellationToken,
    ) -> Result<(Self, mpsc::Receiver<BackendEvent>)> {
        if options.resume && options.fork_from.is_some() {
            bail!("Cannot resume and fork a Claude session simultaneously");
        }
        let mut child = command(config, options)?
            .spawn()
            .context("starting Claude subprocess")?;
        let process_id = child.id().context("Claude subprocess has no process ID")?;
        let stdin = Arc::new(tokio::sync::Mutex::new(Some(
            child.stdin.take().context("missing Claude stdin")?,
        )));
        let stdout = child.stdout.take().context("missing Claude stdout")?;
        let mut stderr = child.stderr.take().context("missing Claude stderr")?;
        let cancel = CancellationToken::new();
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(128);
        let task_cancel = cancel.clone();
        let task_pending = pending.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let task_alive = alive.clone();
        let task_stdin = stdin.clone();
        let dynamic_tools = options.dynamic_tools.clone();
        let task = tokio::spawn(async move {
            let tail = Arc::new(Mutex::new(Vec::<u8>::new()));
            let stderr_tail = tail.clone();
            let mut stderr_task = tokio::spawn(async move {
                let mut buffer = [0; 4096];
                while let Ok(count) = stderr.read(&mut buffer).await {
                    if count == 0 {
                        break;
                    }
                    let mut tail = stderr_tail.lock().unwrap();
                    tail.extend_from_slice(&buffer[..count]);
                    let excess = tail.len().saturating_sub(STDERR_TAIL);
                    tail.drain(..excess);
                }
            });
            let mut lines = FramedRead::new(stdout, LinesCodec::new_with_max_length(MAX_FRAME));
            let mut status = None;
            let outcome: Result<()> = async {
                loop {
                    tokio::select! {
                        _ = task_cancel.cancelled() => break,
                        result = child.wait(), if status.is_none() => {
                            status = Some(result.context("waiting for Claude")?);
                            // Reap descendants holding inherited pipes open as well.
                            kill_group(process_id);
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)), if status.is_some() => {
                            bail!("Claude stdout did not close after process exit");
                        }
                        line = lines.next() => {
                            let Some(line) = line else { break; };
                            let Some(value) = parse_frame(&line.context("reading Claude stdout")?)? else { continue; };
                            if value["type"] == "control_response" {
                                let response = &value["response"];
                                if let Some(id) = response["request_id"].as_str()
                                    && let Some(sender) = task_pending.lock().unwrap().remove(id) {
                                        let result = if response["subtype"] == "error" {
                                            Err(response["error"].as_str().unwrap_or("Claude control request failed").to_owned())
                                        } else { Ok(response["response"].clone()) };
                                        let _ = sender.send(result);
                                    }
                                continue;
                            }
                            if let Some(response) = mcp_control_response(&value, &dynamic_tools) {
                                tokio::select! {
                                    _ = task_cancel.cancelled() => break,
                                    result = write_frame(&task_stdin, &response) => result?,
                                }
                                continue;
                            }
                            tokio::select! {
                                _ = task_cancel.cancelled() => break,
                                result = tx.send(BackendEvent::Message(value)) => if result.is_err() { break; },
                            }
                        }
                    }
                }
                Ok(())
            }.await;
            if status.is_none() {
                // EOF normally accompanies exit. Allow it briefly before killing a stalled child.
                if outcome.is_ok()
                    && !task_cancel.is_cancelled()
                    && let Ok(result) =
                        tokio::time::timeout(Duration::from_millis(250), child.wait()).await
                {
                    status = result.ok();
                }
                kill_group(process_id);
                let _ = child.start_kill();
                if status.is_none() {
                    status = child.wait().await.ok();
                }
            }
            task_alive.store(false, Ordering::Release);
            if tokio::time::timeout(Duration::from_millis(250), &mut stderr_task)
                .await
                .is_err()
            {
                stderr_task.abort();
                let _ = stderr_task.await;
            }
            let success = outcome.is_ok() && status.as_ref().is_some_and(|s| s.success());
            let mut message = match outcome {
                Ok(()) => format!(
                    "Claude exited: {}",
                    status
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "unknown status".into())
                ),
                Err(error) => format!("{error:#}"),
            };
            let tail = String::from_utf8_lossy(&tail.lock().unwrap())
                .trim()
                .to_owned();
            if !tail.is_empty() {
                message.push_str("; stderr: ");
                message.push_str(&tail);
            }
            for (_, sender) in task_pending.lock().unwrap().drain() {
                let _ = sender.send(Err(message.clone()));
            }
            tokio::select! { _ = task_cancel.cancelled() => {}, _ = tx.send(BackendEvent::Exited { success, message }) => {} }
        });
        let mut backend = Self {
            stdin,
            pending,
            controls: Semaphore::new(8),
            cancel,
            task: Some(task),
            process_id,
            alive,
            initialization: Value::Null,
        };
        let initialized = tokio::select! {
            result = backend.control(json!({"subtype":"initialize", "hooks":null}), config.initialize_timeout) => result,
            _ = startup_cancel.cancelled() => Err(anyhow!("Claude startup cancelled")),
        };
        match initialized {
            Ok(value) => backend.initialization = value,
            Err(error) => {
                backend.terminate().await?;
                return Err(error);
            }
        }
        if options.sandbox.is_workspace() {
            let verified = tokio::select! {
                result = async {
                    let settings = backend.control(json!({"subtype":"get_settings"}), config.initialize_timeout).await?;
                    let status = backend.control(json!({"subtype":"get_sandbox_dialog"}), config.initialize_timeout).await?;
                    options.sandbox.verify_native(&settings, &status, &options.cwd)
                } => result,
                _ = startup_cancel.cancelled() => Err(anyhow!("Claude startup cancelled")),
            };
            if let Err(error) = verified {
                backend.terminate().await?;
                return Err(error.context("verifying Claude workspace sandbox"));
            }
        }
        Ok((backend, rx))
    }

    pub fn initialization(&self) -> &Value {
        &self.initialization
    }

    pub async fn send(&self, message: Value) -> Result<()> {
        if self.cancel.is_cancelled() || self.task.as_ref().is_none_or(|task| task.is_finished()) {
            bail!("Claude backend is closed");
        }
        let result = write_frame(&self.stdin, &message).await;
        if result.is_err() {
            self.cancel.cancel();
        }
        result
    }

    async fn control(&self, request: Value, timeout: Duration) -> Result<Value> {
        let _permit = self.controls.acquire().await?;
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);
        let _cleanup = PendingGuard {
            pending: self.pending.clone(),
            id: id.clone(),
        };
        tokio::time::timeout(timeout, async {
            if let Err(error) = self
                .send(json!({"type":"control_request", "request_id":id, "request":request}))
                .await
            {
                // A child can exit before the write completes. send cancels the
                // supervisor, which drains stderr before failing pending requests.
                // Keep that diagnostic instead of reporting only a broken pipe.
                // Drop any sender still in the map: a finished supervisor may
                // have drained the map before this request was registered.
                if self.task.as_ref().is_none_or(|task| task.is_finished()) {
                    self.pending.lock().unwrap().remove(&id);
                }
                return match rx.await {
                    Ok(Err(message)) => Err(error.context(message)),
                    _ => Err(error),
                };
            }
            rx.await
                .context("Claude control response channel closed")?
                .map_err(|error| anyhow!(error))
        })
        .await
        .context("Claude control request timed out")?
    }

    pub async fn interrupt(&self) -> Result<()> {
        self.control(json!({"subtype":"interrupt"}), Duration::from_secs(10))
            .await
            .map(|_| ())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        // EOF gives Claude a chance to persist its transcript before termination.
        if let Ok(mut stdin) =
            tokio::time::timeout(Duration::from_millis(250), self.stdin.lock()).await
        {
            stdin.take();
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(result) => {
                    result.context("joining Claude backend")?;
                }
                Err(_) => {
                    self.cancel.cancel();
                    if self.alive.load(Ordering::Acquire) {
                        kill_group(self.process_id);
                    }
                    task.await.context("joining Claude backend")?;
                }
            }
        }
        self.cancel.cancel();
        Ok(())
    }

    /// Cancel I/O, kill the entire process group, and await the supervisor's reap.
    /// Callers must await this before reusing a session after forced cancellation.
    pub async fn terminate(&mut self) -> Result<()> {
        self.cancel.cancel();
        if self.alive.load(Ordering::Acquire) {
            kill_group(self.process_id);
        }
        if let Some(task) = self.task.take() {
            task.await.context("joining terminated Claude backend")?;
        }
        if let Ok(mut stdin) = self.stdin.try_lock() {
            stdin.take();
        }
        Ok(())
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.cancel.cancel();
        if self.alive.load(Ordering::Acquire) {
            kill_group(self.process_id);
        }
    }
}

struct PendingGuard {
    pending: Pending,
    id: String,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sdk_mcp_discovery_and_routing() {
        let tools = vec![json!({"name":"lookup","description":"Find a record",
            "inputSchema":{"type":"object"}})];
        let request = |method: &str| {
            json!({"type":"control_request","request_id":"outer",
            "request":{"subtype":"mcp_message","server_name":"codex_desktop",
                "message":{"jsonrpc":"2.0","id":7,"method":method,
                    "params":{"protocolVersion":"2025-06-18"}}}})
        };
        let response = mcp_control_response(&request("initialize"), &tools).unwrap();
        assert_eq!(response["response"]["request_id"], "outer");
        assert_eq!(response["response"]["response"]["mcp_response"]["id"], 7);
        assert_eq!(
            response["response"]["response"]["mcp_response"]["result"]["protocolVersion"],
            "2025-06-18"
        );
        let response = mcp_control_response(&request("tools/list"), &tools).unwrap();
        assert_eq!(
            response["response"]["response"]["mcp_response"]["result"]["tools"],
            json!(tools)
        );
        assert!(mcp_control_response(&request("tools/call"), &tools).is_none());
        for method in ["unsupported", "tools/call"] {
            let response = mcp_control_response(&request(method), &[]).unwrap();
            assert_eq!(
                response["response"]["response"]["mcp_response"]["error"]["code"],
                -32601
            );
        }
        let response = mcp_control_response(&request("unsupported"), &tools).unwrap();
        assert_eq!(
            response["response"]["response"]["mcp_response"]["error"]["code"],
            -32601
        );
        let mut notification = request("notifications/initialized");
        notification["request"]["message"]
            .as_object_mut()
            .unwrap()
            .remove("id");
        let response = mcp_control_response(&notification, &tools).unwrap();
        assert_eq!(
            response["response"]["response"]["mcp_response"],
            json!({"jsonrpc":"2.0","result":{}})
        );
        let mut wrong_server = request("tools/call");
        wrong_server["request"]["server_name"] = json!("unknown");
        assert!(mcp_control_response(&wrong_server, &tools).is_some());
    }

    #[test]
    fn diagnostics_and_bad_frames() {
        assert!(parse_frame("warning: hello").unwrap().is_none());
        assert!(parse_frame("{broken}").is_err());
        assert!(parse_frame("[]").is_err());
        assert_eq!(
            parse_frame("{\"type\":\"result\"}").unwrap().unwrap()["type"],
            "result"
        );
    }

    #[cfg(unix)]
    fn test_options() -> SessionOptions {
        SessionOptions {
            cwd: std::env::temp_dir(),
            session_id: "test-session".into(),
            resume: false,
            fork_from: None,
            resume_at: None,
            model: "fake".into(),
            permission_mode: "default".into(),
            system_prompt: None,
            append_system_prompt: None,
            effort: None,
            output_schema: None,
            ephemeral: true,
            dynamic_tools: Vec::new(),
            native_settings: json!({}),
            sandbox: Default::default(),
        }
    }

    #[cfg(unix)]
    fn shell_config(script: &str) -> BackendConfig {
        BackendConfig {
            executable: "/bin/sh".into(),
            extra_args: vec!["-c".into(), script.into()],
            initialize_timeout: Duration::from_secs(2),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn launch_merges_settings_into_one_argument_without_losing_permissions() {
        let fixture = tempfile::tempdir().unwrap();
        std::fs::write(
            fixture.path().join("settings.json"),
            json!({
                "permissions":{"ask":["Bash"]},
                "env":{"KEEP":"existing","OVERLAY":"old"},
                "enabledPlugins":{"keep@market":true,"disabled@market":true}
            })
            .to_string(),
        )
        .unwrap();
        let mut options = test_options();
        options.cwd = fixture.path().into();
        options.native_settings = json!({
            "env":{"OVERLAY":"a value with spaces and $literal"},
            "enabledPlugins":{"disabled@market":false}
        });
        let mut config = shell_config("printf '%s\\n' \"$@\"");
        config.extra_args.extend([
            "launcher".into(),
            "--settings".into(),
            "settings.json".into(),
            "--settings={\"env\":{\"SECOND\":\"inline\"}}".into(),
        ]);
        let output = command(&config, &options).unwrap().output().await.unwrap();
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let settings: Vec<_> = stdout
            .lines()
            .filter_map(|line| line.strip_prefix("--settings="))
            .collect();
        assert_eq!(settings.len(), 1);
        let settings: Value = serde_json::from_str(settings[0]).unwrap();
        assert_eq!(settings["permissions"], json!({"ask":["Bash"]}));
        assert_eq!(
            settings["env"],
            json!({"KEEP":"existing","SECOND":"inline","OVERLAY":"a value with spaces and $literal"})
        );
        assert_eq!(
            settings["enabledPlugins"],
            json!({"keep@market":true,"disabled@market":false})
        );
        assert!(!stdout.lines().any(|line| line == "--settings"));
    }

    #[cfg(unix)]
    #[test]
    fn malformed_native_settings_fail_before_process_spawn() {
        let mut config = shell_config(":");
        config.extra_args.push("--settings".into());
        assert!(command(&config, &test_options()).is_err());
        config.extra_args.pop();
        let mut options = test_options();
        options.native_settings = Value::Null;
        assert!(command(&config, &options).is_err());
    }

    // UUID control IDs do not contain shell or JSON metacharacters. This tiny
    // peer keeps driver tests independent of building a separate test binary.
    #[cfg(unix)]
    const ACK: &str = r#"
        id=${line#*\"request_id\":\"}
        id=${id%%\"*}
        printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s","response":{"models":[]}}}\n' "$id"
    "#;

    #[cfg(unix)]
    #[tokio::test]
    async fn sdk_discovery_completes_before_initialization_and_calls_are_forwarded() {
        let script = format!(
            r#"
            read -r line
            initialize=$line
            for method in initialize tools/list ping notifications/initialized; do
                rpc_id='"id":1,'
                case "$method" in notifications/*) rpc_id='' ;; esac
                printf '{{"type":"control_request","request_id":"discovery","request":{{"subtype":"mcp_message","server_name":"codex_desktop","message":{{"jsonrpc":"2.0",%s"method":"%s","params":{{"protocolVersion":"2025-06-18"}}}}}}}}\n' "$rpc_id" "$method"
                read -r response
                case "$response" in *error*) exit 3 ;; esac
                case "$response" in *mcp_response*) ;; *) exit 2 ;; esac
            done
            line=$initialize
            {ACK}
            printf '{{"type":"control_request","request_id":"call","request":{{"subtype":"mcp_message","server_name":"codex_desktop","message":{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{{"name":"lookup","arguments":{{}}}}}}}}}}\n'
            while read -r line; do :; done
        "#
        );
        let mut options = test_options();
        options.dynamic_tools = vec![
            json!({"name":"lookup","description":"Find a record","inputSchema":{"type":"object"}}),
        ];
        let (mut backend, mut events) = Backend::spawn(&shell_config(&script), &options)
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event, BackendEvent::Message(value) if value["request_id"] == "call"
            && value["request"]["message"]["params"]["name"] == "lookup")
        );
        backend.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn handshake_and_interrupt_ack_are_not_turn_events() {
        let script = format!(
            r#"
            read -r line
            {ACK}
            printf '{{"type":"system","subtype":"init"}}\n'
            read -r line
            {ACK}
            printf '{{"type":"result","result":"interrupted"}}\n'
            while read -r line; do :; done
        "#
        );
        let (mut backend, mut events) = Backend::spawn(&shell_config(&script), &test_options())
            .await
            .unwrap();
        assert_eq!(backend.initialization(), &json!({"models":[]}));
        backend.interrupt().await.unwrap();
        assert!(
            matches!(events.recv().await, Some(BackendEvent::Message(value)) if value["type"] == "system")
        );
        assert!(
            matches!(events.recv().await, Some(BackendEvent::Message(value)) if value["type"] == "result")
        );
        backend.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn forced_termination_reaps_the_backend_before_returning() {
        let script = format!("read -r line\n{ACK}\nwhile read -r line; do :; done");
        let (mut backend, _events) = Backend::spawn(&shell_config(&script), &test_options())
            .await
            .unwrap();
        let process_id = backend.process_id as i32;
        assert_eq!(unsafe { libc::kill(process_id, 0) }, 0);
        backend.terminate().await.unwrap();
        assert_eq!(unsafe { libc::kill(process_id, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn early_exit_includes_stderr() {
        let result = Backend::spawn(
            &shell_config("echo initialization-failed >&2; exit 23"),
            &test_options(),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("expected initialization failure"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("initialization-failed"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_frame_terminates_backend() {
        let script = format!(
            r#"read -r line
            {ACK}
            read -r line
            printf '{{invalid JSON\n'
            sleep 60
        "#
        );
        let (mut backend, mut events) = Backend::spawn(&shell_config(&script), &test_options())
            .await
            .unwrap();
        backend
            .send(json!({"type":"user","message":{"role":"user","content":"malformed"}}))
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(event, BackendEvent::Exited { success: false, message } if message.contains("malformed Claude JSON frame"))
        );
        backend.shutdown().await.unwrap();
    }
}
