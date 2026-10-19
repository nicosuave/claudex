#![cfg(all(feature = "test-backend", unix))]
//! The local router between genuine Codex (a deterministic stand-in here) and
//! the Claude facade, exercised end to end through its stdio transport.

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

const DEADLINE: Duration = Duration::from_secs(15);
/// Deployment settings of an enclosing facade session must not leak into the
/// fixtures (for example, a real plugin sidecar).
const DEPLOYMENT_ENV: &[&str] = &[
    "CLAUDE_CODEX_HOME",
    "CLAUDE_CODEX_PLUGIN_EXECUTABLE",
    "CLAUDE_CODEX_PLUGIN_HOME",
    "CLAUDE_CODEX_SOCKET_DIR",
    "CODEX_HOME",
];
const DESKTOP_ARGS: &[&str] = &[
    "-c",
    "features.code_mode_host=true",
    "app-server",
    "--analytics-default-enabled",
];

/// The facade's login-session service, listening on its private socket.
struct Facade {
    _child: Child,
    socket: std::path::PathBuf,
    _state: tempfile::TempDir,
}

impl Facade {
    async fn start(claude: &str) -> Self {
        Self::start_with_model(claude, "fake-claude").await
    }

    async fn start_with_model(claude: &str, model: &str) -> Self {
        let state = tempfile::tempdir().unwrap();
        let socket = state.path().join("f.sock");
        let child = Self::launch_with_model(&state, &socket, claude, model).await;
        Self {
            _child: child,
            socket,
            _state: state,
        }
    }

    async fn launch(state: &tempfile::TempDir, socket: &Path, claude: &str) -> Child {
        Self::launch_with_model(state, socket, claude, "fake-claude").await
    }

    async fn launch_with_model(
        state: &tempfile::TempDir,
        socket: &Path,
        claude: &str,
        model: &str,
    ) -> Child {
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        for name in DEPLOYMENT_ENV {
            command.env_remove(name);
        }
        let child = command
            .args(["app-server", "--claude", claude, "--model", model])
            .args(["--initialize-timeout-seconds", "3"])
            .arg("--listen")
            .arg(format!("unix://{}", socket.display()))
            .arg("--state-dir")
            .arg(state.path().join("state"))
            .current_dir(state.path())
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        tokio::time::timeout(DEADLINE, async {
            // A restarted service can leave the old socket path behind before
            // rebinding it. Wait for a live listener, not just the path.
            while tokio::net::UnixStream::connect(socket).await.is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("facade socket");
        child
    }
}

struct Desktop {
    _child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    history: Vec<Value>,
    serial: u64,
    deadline: Duration,
}

impl Desktop {
    async fn connect(socket: &Path, seeded_codex_threads: usize) -> Self {
        Self::connect_with_codex(
            socket,
            seeded_codex_threads,
            env!("CARGO_BIN_EXE_fake-codex"),
            DEADLINE,
        )
        .await
    }

    async fn connect_with_codex(
        socket: &Path,
        seeded_codex_threads: usize,
        codex: &str,
        deadline: Duration,
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        for name in DEPLOYMENT_ENV {
            command.env_remove(name);
        }
        let mut child = command
            .arg("--sock")
            .arg(socket)
            .args(["route", "--codex", codex, "--"])
            .args(DESKTOP_ARGS)
            .env("FAKE_CODEX_THREADS", seeded_codex_threads.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut desktop = Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()).lines(),
            _child: child,
            history: vec![],
            serial: 0,
            deadline,
        };
        let init = desktop
            .ok(
                "initialize",
                json!({"clientInfo":{"name":"codex_desktop","version":"1"},
                    "capabilities":{"experimentalApi":true}}),
            )
            .await;
        assert!(
            init["userAgent"]
                .as_str()
                .is_some_and(|agent| agent.contains("codex")),
            "{init}"
        );
        desktop
            .send(json!({"method":"initialized","params":{}}))
            .await;
        desktop
    }

    async fn send(&mut self, value: Value) {
        let line = format!("{value}\n");
        tokio::time::timeout(DEADLINE, self.input.write_all(line.as_bytes()))
            .await
            .unwrap()
            .unwrap();
    }

    async fn next(&mut self) -> Value {
        let line = tokio::time::timeout(self.deadline, self.output.next_line())
            .await
            .expect("router output")
            .unwrap()
            .expect("router stdout open");
        serde_json::from_str(&line).unwrap()
    }

    /// Returns the next matching message, keeping the others for later.
    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(position) = self.history.iter().position(&predicate) {
            return self.history.remove(position);
        }
        loop {
            let message = self.next().await;
            if predicate(&message) {
                return message;
            }
            self.history.push(message);
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = self.serial;
        self.send(json!({"id": id, "method": method, "params": params}))
            .await;
        self.until(|v| v["id"] == id && v.get("method").is_none())
            .await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let reply = self.call(method, params).await;
        assert!(reply.get("error").is_none(), "{method} failed: {reply}");
        reply["result"].clone()
    }

    async fn thread(&mut self, params: Value) -> String {
        self.ok("thread/start", params).await["thread"]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn turn(&mut self, thread: &str, model: &str, text: &str) -> Value {
        self.call(
            "turn/start",
            json!({"threadId": thread, "model": model,
                "input": [{"type": "text", "text": text, "text_elements": []}]}),
        )
        .await
    }

    async fn completed(&mut self, thread: &str) -> Value {
        self.until(|v| v["method"] == "turn/completed" && v["params"]["threadId"] == thread)
            .await
    }
}

fn ids(page: &Value) -> Vec<String> {
    page["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            item["thread"]["id"]
                .as_str()
                .or_else(|| item["id"].as_str())
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn script(actions: Value) -> String {
    format!("delegate-script:{actions}")
}

fn reports(completed: &Value) -> Vec<Value> {
    let text = completed["params"]["turn"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "agentMessage")
        .unwrap()["text"]
        .as_str()
        .unwrap();
    serde_json::from_str(text).unwrap()
}

fn result(report: &Value) -> Value {
    assert_eq!(report["isError"], false, "{report}");
    serde_json::from_str(report["content"][0]["text"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn inherited_mcp_endpoint_cannot_use_another_native_callers_permissions() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
        .await;
    let actions = json!([
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"task"},"meta":{"threadId":"native-child"}},
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"task"},"meta":{}},
        {"tool":"list_agents","arguments":{}}
    ]);
    desktop.turn(&parent, "gpt-fake", &script(actions)).await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    for report in &reports[..2] {
        assert_eq!(report["isError"], true);
        assert!(
            report["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("does not match")
        );
    }
    assert!(result(&reports[2])["agents"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn refused_followup_preserves_queued_messages_for_the_next_native_turn() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"opus","approvalPolicy":"never"}))
        .await;
    let actions = json!([
        {"tool":"spawn_agent","arguments":{"harness":"codex","model":"gpt-fake","message":"task"}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
        {"tool":"send_message","arguments":{"agent_id":"$agent","message":"queued context"}},
        {"tool":"followup_task","arguments":{"agent_id":"$agent","message":"fail-turn-start"}},
        {"tool":"followup_task","arguments":{"agent_id":"$agent","message":"retry task"}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}}
    ]);
    desktop.turn(&parent, "opus", &script(actions)).await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    assert_eq!(reports[3]["isError"], true);
    assert_eq!(
        result(&reports[5])["report"],
        "Codex: queued context\n\nretry task"
    );
}

#[tokio::test]
async fn native_mcp_delegation_works_in_both_directions_with_linked_history() {
    for (parent_model, harness, child_model, expected) in [
        ("gpt-fake", "claude", "opus", "Echo: delegated task"),
        ("opus", "codex", "gpt-fake", "Codex: delegated task"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent=desktop.thread(json!({"model":parent_model,"cwd":facade._state.path(),
            "sandbox":"workspace-write","approvalPolicy":"never","approvalsReviewer":"auto_review"})).await;
        let actions = json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":"delegated task","name":"Cross-harness worker"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
            {"tool":"list_agents","arguments":{}}
        ]);
        assert!(
            desktop
                .turn(&parent, parent_model, &script(actions))
                .await
                .get("error")
                .is_none()
        );
        let completed = desktop.completed(&parent).await;
        let reports = reports(&completed);
        let child = result(&reports[0])["agent_id"].as_str().unwrap().to_owned();
        let waited = result(&reports[1]);
        assert_eq!(waited["status"], "completed");
        assert_eq!(waited["report"], expected);
        assert_eq!(result(&reports[2])["agents"].as_array().unwrap().len(), 1);
        let list = desktop
            .ok("thread/list", json!({"parentThreadId":parent}))
            .await;
        assert_eq!(ids(&list).as_slice(), std::slice::from_ref(&child));
        assert_eq!(
            list["data"][0]["source"]["subAgent"]["thread_spawn"]["parent_thread_id"],
            parent
        );
        assert_eq!(list["data"][0]["agentNickname"], "Cross-harness worker");
        let normal = desktop.ok("thread/list", json!({})).await;
        assert!(
            !ids(&normal).contains(&child),
            "Delegates appeared as unrelated top-level chats"
        );
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(read["thread"]["parentThreadId"], parent);
        assert_eq!(read["thread"]["canAcceptDirectInput"], true);
        let permissions = desktop.ok("thread/resume", json!({"threadId":child})).await;
        assert_eq!(permissions["approvalPolicy"], "never");
        assert_eq!(permissions["approvalsReviewer"], "auto_review");
        assert_eq!(permissions["sandbox"]["type"], "workspaceWrite");
        assert_eq!(permissions["sandbox"]["networkAccess"], false);
        let history = desktop
            .ok(
                "thread/read",
                json!({"threadId":parent,"includeTurns":true}),
            )
            .await;
        let spawn = history["thread"]["turns"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|turn| turn["items"].as_array().unwrap())
            .find(|item| item["type"] == "collabAgentToolCall")
            .unwrap();
        assert_eq!(spawn["receiverThreadIds"], json!([child]));
        assert_eq!(spawn["tool"], "spawnAgent");
    }
}

#[tokio::test]
async fn desktop_can_open_running_delegate_without_reconfiguring_it() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","cwd":facade._state.path(),
            "sandbox":"workspace-write","approvalPolicy":"never","approvalsReviewer":"auto_review"}))
        .await;
    desktop.turn(&parent, "gpt-fake", &script(json!([
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"hang"}}
    ]))).await;
    let completed = desktop.completed(&parent).await;
    let child = result(&reports(&completed)[0])["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let before = desktop.ok("thread/resume", json!({"threadId":child})).await;
    assert_eq!(before["thread"]["turns"][0]["status"], "inProgress");
    let open = json!({"threadId":child,"path":before["thread"]["path"],
        "excludeTurns":true,"initialTurnsPage":{"limit":10},
        "cwd":facade._state.path(),"model":"fable","effort":"high",
        "approvalPolicy":"on-request","approvalsReviewer":"user","sandbox":"danger-full-access",
        "developerInstructions":"Desktop opening context","baseInstructions":"Desktop base context",
        "dynamicTools":[]});
    let opened = desktop.ok("thread/resume", open.clone()).await;
    assert_eq!(opened["model"], "opus");
    assert_eq!(opened["approvalPolicy"], before["approvalPolicy"]);
    assert_eq!(opened["approvalsReviewer"], before["approvalsReviewer"]);
    assert_eq!(opened["sandbox"], before["sandbox"]);
    assert_eq!(opened["thread"]["turns"], json!([]));
    assert_eq!(
        opened["initialTurnsPage"]["data"][0]["status"],
        "inProgress"
    );
    assert_eq!(opened["thread"]["parentThreadId"], parent);
    // Direct control still reaches the running child after hydration.
    desktop
        .ok(
            "turn/interrupt",
            json!({"threadId":child,"turnId":before["thread"]["turns"][0]["id"]}),
        )
        .await;
    let completed = desktop.completed(&child).await;
    assert_eq!(completed["params"]["turn"]["status"], "interrupted");
    let idle = desktop.ok("thread/resume", open).await;
    assert_eq!(idle["model"], "opus");
    assert_eq!(idle["sandbox"], before["sandbox"]);
    let saved: Value =
        serde_json::from_slice(&std::fs::read(before["thread"]["path"].as_str().unwrap()).unwrap())
            .unwrap();
    assert!(
        saved["settings"]["developer_instructions"]
            .as_str()
            .unwrap()
            .starts_with("You are a delegated child agent")
    );
    assert_eq!(saved["settings"]["base_instructions"], Value::Null);
    assert!(
        !saved["settings"]["dynamic_tools"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":child,"model":"fable"}),
        )
        .await;
    let changed = desktop.ok("thread/resume", json!({"threadId":child})).await;
    assert_eq!(changed["model"], "fable");
}

#[tokio::test]
async fn delegation_wait_steer_queue_and_followup_preserve_one_native_child() {
    for (parent_model, harness, child_model) in [
        ("gpt-fake", "claude", "opus"),
        ("opus", "codex", "gpt-fake"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent = desktop
            .thread(json!({"model":parent_model,"approvalPolicy":"never"}))
            .await;
        let actions = json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":"hang"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":0}},
            {"tool":"send_message","arguments":{"agent_id":"$agent","message":"finish task"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
            {"tool":"send_message","arguments":{"agent_id":"$agent","message":"queued context"}},
            {"tool":"followup_task","arguments":{"agent_id":"$agent","message":"follow-up task"}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
            {"tool":"list_agents","arguments":{}}
        ]);
        desktop.turn(&parent, parent_model, &script(actions)).await;
        let completed = desktop.completed(&parent).await;
        let reports = reports(&completed);
        let child = result(&reports[0])["agent_id"].clone();
        assert_eq!(result(&reports[1])["status"], "running");
        assert!(
            result(&reports[3])["report"]
                .as_str()
                .unwrap()
                .contains("finish task")
        );
        assert_eq!(result(&reports[4])["queued"], true);
        assert_eq!(result(&reports[5])["agent_id"], child);
        assert!(
            result(&reports[6])["report"]
                .as_str()
                .unwrap()
                .contains("queued context\n\nfollow-up task")
        );
        assert_eq!(result(&reports[7])["agents"].as_array().unwrap().len(), 1);
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(read["thread"]["turns"].as_array().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn delegated_native_approvals_round_trip_on_the_child_thread() {
    for (parent_model, harness, child_model) in [
        ("gpt-fake", "claude", "opus"),
        ("opus", "codex", "gpt-fake"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent=desktop.thread(json!({"model":parent_model,"approvalPolicy":"on-request","approvalsReviewer":"user"})).await;
        let message = if harness == "claude" {
            "tool"
        } else {
            "permission"
        };
        desktop.turn(&parent,parent_model,&script(json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":message}},
            {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}}
        ]))).await;
        let approval = desktop
            .until(|message| message["method"] == "item/commandExecution/requestApproval")
            .await;
        let child = approval["params"]["threadId"].as_str().unwrap().to_owned();
        assert_ne!(child, parent);
        desktop
            .send(json!({"id":approval["id"],"result":{"decision":"decline"}}))
            .await;
        let completed = desktop.completed(&parent).await;
        let reports = reports(&completed);
        assert_eq!(result(&reports[0])["agent_id"], child);
        assert!(
            result(&reports[1])["report"]
                .as_str()
                .unwrap()
                .to_lowercase()
                .contains("denied")
        );
    }
}

/// Opt-in model-backed verification. Uses normal native authentication, creates
/// only test threads, archives them afterwards, and never installs or restarts
/// the user's service.
#[tokio::test]
#[ignore = "requires CLAUDEX_NATIVE_CODEX, CLAUDEX_NATIVE_CLAUDE and native authentication; makes model calls"]
async fn native_shared_delegation() {
    let codex = std::env::var("CLAUDEX_NATIVE_CODEX").expect("genuine Codex executable");
    let claude = std::env::var("CLAUDEX_NATIVE_CLAUDE").expect("native Claude executable");
    let gpt = std::env::var("CLAUDEX_NATIVE_GPT_MODEL").unwrap_or_else(|_| "gpt-6-luna".into());
    let facade = if std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET").is_none() {
        Some(Facade::start_with_model(&claude, "opus").await)
    } else {
        None
    };
    let scratch = tempfile::tempdir().unwrap();
    let socket = std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| facade.as_ref().unwrap().socket.clone());
    let mut desktop =
        Desktop::connect_with_codex(&socket, 0, &codex, Duration::from_secs(120)).await;
    for (parent_model, harness, child_model) in [
        (gpt.as_str(), "claude", "opus"),
        ("opus", "codex", gpt.as_str()),
    ] {
        let parent = desktop
            .thread(json!({"model":parent_model,"cwd":scratch.path(),
            "sandbox":"workspace-write","approvalPolicy":"on-request","approvalsReviewer":"user"}))
            .await;
        let prompt = format!(
            "Verify cross-harness delegation. Use the claudex_agents MCP tools, not native subagent tools or shell subprocesses. Call spawn_agent with harness={harness}, model={child_model}, name=Native verification, and message=Reply exactly CROSS_HARNESS_CHILD_OK without using other tools. Then call wait_agent on the returned agent_id until it finishes. Finally reply CROSS_HARNESS_PARENT_OK. Do not edit files or run other tools."
        );
        let reply = desktop.turn(&parent, parent_model, &prompt).await;
        assert!(reply.get("error").is_none(), "{reply}");
        let completed = loop {
            let message = desktop
                .until(|message| {
                    message["method"] == "turn/completed" && message["params"]["threadId"] == parent
                        || message.get("method").is_some() && message.get("id").is_some()
                })
                .await;
            if message["method"] == "turn/completed" {
                break message;
            }
            if message["method"] == "mcpServer/elicitation/request"
                && message["params"]["serverName"] == "claudex_agents"
                && message["params"]["_meta"]["codex_approval_kind"] == "mcp_tool_call"
            {
                let arguments = &message["params"]["_meta"]["tool_params"];
                if arguments.get("harness").is_some() {
                    assert_eq!(arguments["harness"], harness);
                    assert_eq!(arguments["model"], child_model);
                }
                desktop
                    .send(json!({"id":message["id"],"result":{"action":"accept","content":{}}}))
                    .await;
                continue;
            }
            panic!("Unexpected approval outside the native delegation probe: {message}");
        };
        assert_eq!(
            completed["params"]["turn"]["status"], "completed",
            "{completed}"
        );
        let spawn = completed["params"]["turn"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "collabAgentToolCall" && item["tool"] == "spawnAgent")
            .unwrap_or_else(||panic!("Native model did not spawn through the shared tool: {completed}; MCP startup: {:?}",desktop.history.iter().filter(|message|message["method"] == "mcpServer/startupStatus/updated").collect::<Vec<_>>()));
        let child = spawn["receiverThreadIds"][0].as_str().unwrap();
        let child = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(child["thread"]["parentThreadId"], parent);
        let report = child["thread"]["turns"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|turn| turn["items"].as_array().unwrap())
            .filter(|item| item["type"] == "agentMessage")
            .filter_map(|item| item["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(report.contains("CROSS_HARNESS_CHILD_OK"), "{report}");
        eprintln!(
            "Verified native {parent_model} -> {harness}/{child_model}: child {child_id}",
            child_id = child["thread"]["id"]
        );
        desktop
            .ok("thread/archive", json!({"threadId":child["thread"]["id"]}))
            .await;
        desktop
            .ok("thread/archive", json!({"threadId":parent}))
            .await;
    }
}

#[tokio::test]
async fn desktop_prediction_requests_keep_the_claude_provider_and_route_the_temporary_fork() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"fake-claude","sandbox":"danger-full-access"}))
        .await;
    assert!(
        desktop
            .turn(&parent, "fake-claude", "parent context")
            .await
            .get("error")
            .is_none()
    );
    let completed = desktop.completed(&parent).await;
    let resumed = desktop
        .ok(
            "thread/resume",
            json!({"threadId":parent,"excludeTurns":true}),
        )
        .await;
    assert_eq!(resumed["modelProvider"], "openai");
    assert_eq!(resumed["thread"]["modelProvider"], "anthropic");
    assert_eq!(resumed["claudexPredictionFork"], true);
    // A renderer may reuse the top-level compatibility provider on later resume.
    let echoed = desktop
        .ok(
            "thread/resume",
            json!({"threadId":parent,"modelProvider":resumed["modelProvider"],"excludeTurns":true}),
        )
        .await;
    assert_eq!(echoed["thread"]["modelProvider"], "anthropic");
    let fork = desktop
        .ok(
            "thread/fork",
            json!({
                "threadId":parent,"experimentalPredictionMode":true,"ephemeral":true,
                "excludeTurns":true,"threadSource":"composer_predictions"
            }),
        )
        .await;
    let prediction = fork["thread"]["id"].as_str().unwrap();
    // The desktop supplies no model override on this special fork's turn.
    desktop
        .ok(
            "turn/start",
            json!({
                "threadId":prediction,"turnTrigger":"composer_predictions",
                "responsesapiClientMetadata":{
                    "prediction_source_thread_id":parent,
                    "prediction_source_turn_id":completed["params"]["turn"]["id"],
                    "prediction_trigger":"live"
                },
                "clientUserMessageId":uuid::Uuid::new_v4().to_string(),
                "input":[{"type":"text","text":"fork-info","text_elements":[]}],
                "effort":"low","outputSchema":null
            }),
        )
        .await;
    let result = desktop.completed(prediction).await;
    assert_eq!(result["params"]["turn"]["status"], "completed");
    let text = result["params"]["turn"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "agentMessage")
        .unwrap()["text"]
        .as_str()
        .unwrap();
    let launch: Value = serde_json::from_str(text).unwrap();
    assert_eq!(launch["fork"], true);
    assert_eq!(launch["tools"], "");
    assert_eq!(launch["settings"]["disableAllHooks"], true);
    let retained = desktop
        .ok(
            "thread/read",
            json!({"threadId":parent,"includeTurns":true}),
        )
        .await;
    assert_eq!(retained["thread"]["modelProvider"], "anthropic");
    assert_eq!(retained["thread"]["turns"].as_array().unwrap().len(), 1);
    desktop
        .ok("thread/unsubscribe", json!({"threadId":prediction}))
        .await;
    assert!(
        desktop
            .call("thread/read", json!({"threadId":prediction}))
            .await
            .get("error")
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires native Codex/Claude authentication; makes model calls"]
async fn native_side_chat_preserves_parent_snapshot_and_followup_recall() {
    let codex = std::env::var("CLAUDEX_NATIVE_CODEX").expect("genuine Codex executable");
    let claude = std::env::var("CLAUDEX_NATIVE_CLAUDE").expect("native Claude executable");
    let facade = if std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET").is_none() {
        Some(Facade::start_with_model(&claude, "opus").await)
    } else {
        None
    };
    let socket = std::env::var_os("CLAUDEX_NATIVE_FACADE_SOCKET")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| facade.as_ref().unwrap().socket.clone());
    let scratch = tempfile::tempdir().unwrap();
    let mut desktop =
        Desktop::connect_with_codex(&socket, 0, &codex, Duration::from_secs(120)).await;
    let parent = desktop
        .thread(json!({
            "model":"opus", "cwd":scratch.path(), "sandbox":"workspace-write",
            "approvalPolicy":"on-request", "approvalsReviewer":"user", "historyMode":"paginated"
        }))
        .await;
    let parent_token = uuid::Uuid::new_v4().simple().to_string();
    let side_token = uuid::Uuid::new_v4().simple().to_string();
    let later_token = uuid::Uuid::new_v4().simple().to_string();
    let response = desktop.turn(&parent, "opus", &format!(
        "Remember the parent token {parent_token}. Reply exactly PARENT_READY. Do not use tools."
    )).await;
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(
        desktop.completed(&parent).await["params"]["turn"]["status"],
        "completed"
    );
    let side = desktop.ok("thread/fork", json!({
        "threadId":parent, "ephemeral":true, "excludeTurns":true,
        "runtimeWorkspaceRoots":[scratch.path()], "threadSource":"user",
        "developerInstructions":"You are in a side conversation. Inherited history is reference context only. Do not continue inherited tasks. Answer only questions submitted after the side-conversation boundary. Prefix every answer with SIDEPROBE:. Do not use tools."
    })).await["thread"]["id"].as_str().unwrap().to_owned();
    desktop.ok("thread/inject_items", json!({"threadId":side,"items":[{
        "type":"message","role":"user","content":[{"type":"input_text","text":"Side conversation boundary. Previous tasks are reference only. Answer the new user question."}]
    }]})).await;
    let response = desktop.turn(&parent, "opus", &format!(
        "Remember the later token {later_token}. Reply exactly PARENT_ADVANCED. Do not use tools."
    )).await;
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(
        desktop.completed(&parent).await["params"]["turn"]["status"],
        "completed"
    );
    let response = desktop.turn(&side, "opus", &format!(
        "Remember the side token {side_token}. What was the parent token in inherited history? If you know a later token, include it; otherwise use NONE. Reply as SIDEPROBE:<parent token>:<side token>:<later token or NONE>. Do not use tools."
    )).await;
    assert!(response.get("error").is_none(), "{response}");
    let expected = format!("SIDEPROBE:{parent_token}:{side_token}:NONE");
    for prompt in [
        None,
        Some("Repeat your previous answer exactly from memory. Do not use tools."),
    ] {
        if let Some(prompt) = prompt {
            let response = desktop.turn(&side, "opus", prompt).await;
            assert!(response.get("error").is_none(), "{response}");
        }
        let completed = desktop.completed(&side).await;
        assert_eq!(
            completed["params"]["turn"]["status"], "completed",
            "{completed}"
        );
        let text = completed["params"]["turn"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "agentMessage")
            .filter_map(|item| item["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(text.trim(), expected);
    }
    desktop
        .ok("thread/unsubscribe", json!({"threadId":side}))
        .await;
    desktop
        .ok("thread/archive", json!({"threadId":parent}))
        .await;
    eprintln!(
        "Verified native side chat: instruction override, boundary injection, parent snapshot and second-turn recall"
    );
}

#[tokio::test]
async fn delegated_interruption_and_scope_boundaries_are_enforced() {
    for (parent_model, harness, child_model) in [
        ("gpt-fake", "claude", "opus"),
        ("opus", "codex", "gpt-fake"),
    ] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent = desktop
            .thread(json!({"model":parent_model,"approvalPolicy":"never"}))
            .await;
        desktop.turn(&parent,parent_model,&script(json!([
            {"tool":"spawn_agent","arguments":{"harness":harness,"model":child_model,"message":"hang"}}
        ]))).await;
        let completed = desktop.completed(&parent).await;
        let child = result(&reports(&completed)[0])["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let unrelated = desktop.thread(json!({"model":parent_model})).await;
        desktop
            .turn(
                &unrelated,
                parent_model,
                &script(json!([
                    {"tool":"interrupt_agent","arguments":{"agent_id":child}}
                ])),
            )
            .await;
        let rejected = desktop.completed(&unrelated).await;
        let denied = reports(&rejected);
        assert_eq!(denied[0]["isError"], true);
        assert!(
            denied[0]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("outside the caller's task tree")
        );
        desktop
            .turn(
                &parent,
                parent_model,
                &script(json!([
                    {"tool":"interrupt_agent","arguments":{"agent_id":child}},
                    {"tool":"wait_agent","arguments":{"agent_id":child,"timeout_ms":10_000}}
                ])),
            )
            .await;
        let stopped = desktop.completed(&parent).await;
        assert_eq!(result(&reports(&stopped)[1])["status"], "interrupted");
    }
}

#[tokio::test]
async fn delegated_children_survive_router_reload_and_paginate_without_duplicates() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
        .await;
    let actions:Vec<Value>=(0..3).flat_map(|index|[
        json!({"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"task","name":format!("Worker {index}")}}),
        json!({"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}})
    ]).collect();
    desktop
        .turn(&parent, "gpt-fake", &script(json!(actions)))
        .await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    let expected: HashSet<String> = reports
        .iter()
        .step_by(2)
        .map(|report| result(report)["agent_id"].as_str().unwrap().to_owned())
        .collect();
    drop(desktop);
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let mut seen = HashSet::new();
    let mut cursor = Value::Null;
    for _ in 0..5 {
        let page = desktop
            .ok(
                "thread/list",
                json!({"parentThreadId":parent,"limit":1,"cursor":cursor}),
            )
            .await;
        assert_eq!(page["data"].as_array().unwrap().len(), 1);
        for id in ids(&page) {
            assert!(seen.insert(id), "duplicate child on a later page");
        }
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    assert_eq!(seen, expected);
    let filtered = desktop
        .ok(
            "thread/list",
            json!({"parentThreadId":parent,"sourceKinds":["appServer"]}),
        )
        .await;
    assert!(filtered["data"].as_array().unwrap().is_empty());
    for child in expected {
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(read["thread"]["parentThreadId"], parent);
    }
}

#[tokio::test]
async fn router_disconnect_and_crash_recovery_stop_native_claude_children() {
    for graceful in [true, false] {
        let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        let parent = desktop
            .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
            .await;
        desktop.turn(&parent,"gpt-fake",&script(json!([
            {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":"hang"}}
        ]))).await;
        let completed = desktop.completed(&parent).await;
        let child = result(&reports(&completed)[0])["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        if graceful {
            let Desktop {
                mut _child, input, ..
            } = desktop;
            drop(input);
            tokio::time::timeout(DEADLINE, _child.wait())
                .await
                .unwrap()
                .unwrap();
        } else {
            desktop._child.kill().await.unwrap();
            drop(desktop);
        }
        let mut desktop = Desktop::connect(&facade.socket, 0).await;
        if !graceful {
            desktop.completed(&child).await;
        }
        let read = desktop
            .ok("thread/read", json!({"threadId":child,"includeTurns":true}))
            .await;
        assert_eq!(
            read["thread"]["turns"].as_array().unwrap().last().unwrap()["status"],
            "interrupted"
        );
        assert_eq!(read["thread"]["parentThreadId"], parent);
    }
}

#[tokio::test]
async fn nested_delegation_keeps_one_tree_across_both_harnesses() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let parent = desktop
        .thread(json!({"model":"gpt-fake","approvalPolicy":"never"}))
        .await;
    let nested = script(json!([
        {"tool":"spawn_agent","arguments":{"harness":"codex","model":"gpt-fake","message":"nested task"}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}}
    ]));
    desktop.turn(&parent,"gpt-fake",&script(json!([
        {"tool":"spawn_agent","arguments":{"harness":"claude","model":"opus","message":nested}},
        {"tool":"wait_agent","arguments":{"agent_id":"$agent","timeout_ms":10_000}},
        {"tool":"list_agents","arguments":{}}
    ]))).await;
    let completed = desktop.completed(&parent).await;
    let reports = reports(&completed);
    let outer = result(&reports[0])["agent_id"].clone();
    assert_eq!(result(&reports[2])["agents"].as_array().unwrap().len(), 2);
    let all = desktop
        .ok("thread/list", json!({"ancestorThreadId":parent}))
        .await;
    assert_eq!(all["data"].as_array().unwrap().len(), 2);
    assert!(
        all["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|child| child["parentThreadId"] == outer)
    );
    let direct = desktop
        .ok("thread/list", json!({"parentThreadId":parent}))
        .await;
    assert_eq!(direct["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn ssh_proxy_lists_both_backends_and_runs_claude_over_websocket() {
    use tokio_tungstenite::tungstenite::Message;
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
    for name in DEPLOYMENT_ENV {
        command.env_remove(name);
    }
    let mut child = command
        .arg("--sock")
        .arg(&facade.socket)
        .args([
            "route",
            "--codex",
            env!("CARGO_BIN_EXE_fake-codex"),
            "--",
            "app-server",
            "proxy",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stream = tokio::io::join(child.stdout.take().unwrap(), child.stdin.take().unwrap());
    let (mut socket, _) = tokio::time::timeout(
        DEADLINE,
        tokio_tungstenite::client_async("ws://codex-app-server/rpc", stream),
    )
    .await
    .unwrap()
    .expect("desktop SSH WebSocket handshake");
    async fn rpc<S>(
        socket: &mut tokio_tungstenite::WebSocketStream<S>,
        id: u64,
        method: &str,
        params: Value,
    ) -> Value
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        socket
            .send(Message::Text(
                json!({"id":id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            let message = tokio::time::timeout(DEADLINE, socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["id"] == id {
                assert!(value.get("error").is_none(), "{value}");
                return value["result"].clone();
            }
        }
    }
    rpc(&mut socket, 1, "initialize", json!({"clientInfo":{"name":"codex_desktop","version":"1"},"capabilities":{"experimentalApi":true}})).await;
    socket
        .send(Message::Text(
            json!({"method":"initialized"}).to_string().into(),
        ))
        .await
        .unwrap();
    let models = rpc(
        &mut socket,
        2,
        "model/list",
        json!({"includeHidden":true,"limit":100}),
    )
    .await;
    let models = models["data"].as_array().unwrap();
    assert!(models.iter().any(|model| model["id"] == "gpt-fake"));
    assert!(models.iter().any(|model| model["id"] == "opus"));
    let thread = rpc(
        &mut socket,
        3,
        "thread/start",
        json!({"model":"opus","sandbox":"workspace-write","approvalPolicy":"on-request"}),
    )
    .await;
    let id = &thread["thread"]["id"];
    rpc(&mut socket, 4, "turn/start", json!({"threadId":id,"input":[{"type":"text","text":"hello through SSH","text_elements":[]}]})).await;
    loop {
        let message = tokio::time::timeout(DEADLINE, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Text(text) = message else {
            continue;
        };
        let value: Value = serde_json::from_str(&text).unwrap();
        if value["method"] == "turn/completed" {
            assert_eq!(value["params"]["turn"]["status"], "completed", "{value}");
            break;
        }
    }
    socket.close(None).await.unwrap();
}

#[tokio::test]
async fn one_picker_and_one_sidebar_span_codex_and_claude() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;

    let models = desktop.ok("model/list", json!({})).await;
    let names: Vec<&str> = models["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(&names[..2], ["gpt-fake", "gpt-fake-mini"]);
    for claude in ["fake-claude", "opus", "sonnet"] {
        assert!(names.contains(&claude), "{names:?}");
    }
    let defaults: Vec<_> = models["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["isDefault"] == true)
        .collect();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0]["id"], "gpt-fake");

    let desktop_mcp = json!({
        "mcp_servers.artifact_session":{"command":"/desktop/node_repl", "enabled":true},
        "mcp_servers.cua_repl":{"command":"/desktop/node", "enabled":true}
    });
    let codex = desktop
        .thread(json!({"model": "gpt-fake", "config":desktop_mcp}))
        .await;
    let reply = desktop.turn(&codex, "gpt-fake", "hello codex").await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&codex).await;
    let state = desktop.ok("fake/state", json!({})).await;
    for (key, value) in desktop_mcp.as_object().unwrap() {
        assert_eq!(state["threadConfigs"][&codex][key], *value);
    }

    let claude = desktop
        .thread(json!({"model": "opus", "config": {
            "mcp_servers.node_repl":{"command":"node", "args":["desktop-repl.js"]},
            "mcp_servers.artifact_session":{"command":"/desktop/node_repl", "env":{"NODE_REPL_TRUSTED_SERVICES":"{\"artifact_session\":\"@oai/artifact-tool/artifact-session/service\"}"}},
            "mcp_servers.cua_repl":{"command":"/desktop/node", "args":["/desktop/@oai/cua-repl/bin/cua-repl.mjs"], "enabled":true},
            "memories.dual_write":true, "memories.version":"v2"
        }}))
        .await;
    assert_ne!(claude, codex);
    let reply = desktop.turn(&claude, "opus", "hello claude").await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&claude).await;
    let read = desktop.ok("thread/read", json!({"threadId": claude})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");

    // Each backend keeps its own threads; switching families needs a new chat.
    let wrong = desktop.turn(&claude, "gpt-fake", "again").await;
    assert!(
        wrong["error"]["message"]
            .as_str()
            .unwrap()
            .contains("runs on Claude")
    );
    let wrong = desktop.turn(&codex, "opus", "again").await;
    assert!(
        wrong["error"]["message"]
            .as_str()
            .unwrap()
            .contains("runs on Codex")
    );

    let listed = desktop
        .ok("thread/list", json!({"limit": 50, "sortKey": "created_at"}))
        .await;
    let listed = ids(&listed);
    assert!(
        listed.contains(&codex) && listed.contains(&claude),
        "{listed:?}"
    );
    // Codex threads created later sort ahead of the Claude thread.
    assert!(listed.iter().position(|id| *id == codex) < listed.iter().position(|id| *id == claude));
    let loaded = desktop.ok("thread/loaded/list", json!({})).await;
    let loaded: Vec<&str> = loaded["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(loaded.contains(&codex.as_str()) && loaded.contains(&claude.as_str()));
}

#[tokio::test]
async fn a_prewarmed_codex_thread_moves_to_claude_on_its_first_claude_turn() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    // The desktop prewarms without a model, then submits with the picked one.
    let thread = desktop
        .thread(
            json!({"cwd": "/tmp", "sandbox": "danger-full-access", "config": {
                "mcp_servers.node_repl":{"command":"node", "args":["desktop-repl.js"]},
                "mcp_servers.artifact_session":{"command":"", "enabled":false},
                "mcp_servers.cua_repl":{"command":"/desktop/electron", "enabled":false},
                "memories.dual_write":true, "memories.version":"v2"
            }}),
        )
        .await;
    let reply = desktop.turn(&thread, "sonnet", "first message").await;
    assert!(reply.get("error").is_none(), "{reply}");
    let completed = desktop.completed(&thread).await;
    assert_eq!(
        completed["params"]["turn"]["status"], "completed",
        "{completed}"
    );
    let read = desktop.ok("thread/read", json!({"threadId": thread})).await;
    assert_eq!(read["thread"]["id"], thread.as_str());
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    assert_eq!(read["thread"]["model"], "sonnet");
    let state = desktop.ok("fake/state", json!({})).await;
    assert_eq!(state["unsubscribed"], json!([thread]));
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "the Claude turn must not reach Codex: {state}"
    );
    // A second router learns ownership from the facade itself.
    let mut fresh = Desktop::connect(&facade.socket, 0).await;
    let read = fresh.ok("thread/read", json!({"threadId": thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    fresh
        .ok(
            "thread/resume",
            json!({"threadId": thread, "model": "sonnet"}),
        )
        .await;
    let followup = fresh.turn(&thread, "sonnet", "second").await;
    assert!(followup.get("error").is_none(), "{followup}");
    fresh.completed(&thread).await;
}

#[tokio::test]
async fn merged_pages_keep_every_thread_exactly_once_in_order() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 7).await;
    let mut claude = HashSet::new();
    for n in 0..3 {
        let thread = desktop.thread(json!({"model": "haiku"})).await;
        desktop.turn(&thread, "haiku", &format!("c{n}")).await;
        desktop.completed(&thread).await;
        claude.insert(thread);
    }
    let codex_new = desktop.thread(json!({})).await;
    desktop.turn(&codex_new, "gpt-fake", "x").await;
    desktop.completed(&codex_new).await;

    for method in ["thread/list", "thread/search"] {
        let mut cursor = Value::Null;
        let mut seen = vec![];
        let mut created = vec![];
        loop {
            let mut params = json!({"limit": 3, "cursor": cursor, "sortKey": "created_at"});
            if method == "thread/search" {
                params["searchTerm"] = json!("");
            }
            let page = desktop.ok(method, params).await;
            for item in page["data"].as_array().unwrap() {
                let thread = if item["thread"].is_object() {
                    &item["thread"]
                } else {
                    item
                };
                created.push(thread["createdAt"].as_i64().unwrap());
            }
            seen.extend(ids(&page));
            assert_eq!(page["backwardsCursor"], Value::Null);
            cursor = page["nextCursor"].clone();
            if cursor.is_null() {
                break;
            }
        }
        let unique: HashSet<_> = seen.iter().cloned().collect();
        assert_eq!(unique.len(), seen.len(), "{method} duplicated: {seen:?}");
        assert_eq!(seen.len(), 7 + 1 + 3, "{method}: {seen:?}");
        assert!(claude.is_subset(&unique));
        assert!(created.windows(2).all(|w| w[0] >= w[1]), "{created:?}");
    }
}

/// Every source kind, as the desktop lists a section's members.
const ALL_SOURCE_KINDS: &[&str] = &[
    "cli",
    "vscode",
    "exec",
    "appServer",
    "subAgent",
    "subAgentReview",
    "subAgentCompact",
    "subAgentThreadSpawn",
    "subAgentOther",
    "unknown",
];

/// Thread ids in a section, paging the way the desktop does.
async fn section_members(desktop: &mut Desktop, section: &str, limit: u64) -> Vec<String> {
    let mut members = vec![];
    let mut cursor = Value::Null;
    loop {
        let page = desktop
            .ok(
                "thread/list",
                json!({"cursor":cursor,"limit":limit,"modelProviders":[],"sectionId":section,
                    "sortKey":"section_position","sourceKinds":ALL_SOURCE_KINDS,"useStateDbOnly":true}),
            )
            .await;
        members.extend(ids(&page));
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            return members;
        }
    }
}

#[tokio::test]
async fn claude_threads_share_codex_sections_through_the_router() {
    const PINNED: &str = "01984de2-8f74-7c91-a3b2-5c5e937cf318";
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let codex = desktop.thread(json!({"model": "gpt-fake"})).await;
    let claude = desktop.thread(json!({"model": "opus"})).await;
    let second = desktop.thread(json!({"model": "opus"})).await;
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": codex, "sectionId": PINNED, "beforeThreadId": null}),
        )
        .await;
    // Neither backend can order its threads around the other's: a Claude
    // thread placed before a Codex one follows Codex's members.
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": claude, "sectionId": PINNED, "beforeThreadId": codex}),
        )
        .await;
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": second, "sectionId": PINNED, "beforeThreadId": claude}),
        )
        .await;
    let read = desktop.ok("thread/read", json!({"threadId": claude})).await;
    assert_eq!(
        read["thread"]["section"],
        json!({"id": PINNED, "name": "Pinned", "appearance": null})
    );
    let pinned = section_members(&mut desktop, PINNED, 100).await;
    assert_eq!(pinned, [codex.clone(), second.clone(), claude.clone()]);
    assert_eq!(section_members(&mut desktop, PINNED, 1).await, pinned);
    // Codex's sections page two at a time; the router reads every page to
    // find a destination.
    desktop
        .ok("threadSection/create", json!({"name": "Someday"}))
        .await;
    let work = desktop
        .ok("threadSection/create", json!({"name": "Work"}))
        .await["section"]
        .clone();
    let work_id = work["id"].as_str().unwrap();
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": second, "sectionId": work_id}),
        )
        .await;
    assert_eq!(
        section_members(&mut desktop, PINNED, 100).await,
        [codex.clone(), claude.clone()]
    );
    assert_eq!(
        section_members(&mut desktop, work_id, 100).await.as_slice(),
        std::slice::from_ref(&second)
    );
    let missing = desktop
        .call(
            "thread/section/move",
            json!({"threadId": claude, "sectionId": "0199ffff-0000-7000-8000-000000000000"}),
        )
        .await;
    assert!(
        missing["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not exist"),
        "{missing}"
    );
    // Codex renames and deletions reach the Claude thread's copy.
    desktop
        .ok(
            "threadSection/update",
            json!({"sectionId": work_id, "name": "Renamed"}),
        )
        .await;
    let read = desktop.ok("thread/read", json!({"threadId": second})).await;
    assert_eq!(read["thread"]["section"]["name"], "Renamed");
    desktop
        .ok("threadSection/delete", json!({"sectionId": work_id}))
        .await;
    let read = desktop.ok("thread/read", json!({"threadId": second})).await;
    assert_eq!(read["thread"]["section"], Value::Null);
    let unsectioned = desktop
        .ok("thread/list", json!({"limit": 100, "sectionId": null}))
        .await;
    assert!(ids(&unsectioned).contains(&second), "{unsectioned}");
    assert!(!ids(&unsectioned).contains(&claude), "{unsectioned}");
    desktop
        .ok(
            "thread/section/move",
            json!({"threadId": claude, "sectionId": null}),
        )
        .await;
    assert_eq!(section_members(&mut desktop, PINNED, 100).await, [codex]);
}

#[tokio::test]
async fn deleting_a_claude_thread_forgets_it_and_apps_go_to_codex() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let codex = desktop.thread(json!({"model": "gpt-fake"})).await;
    let claude = desktop.thread(json!({"model": "opus"})).await;
    // Apps do not depend on the chat's model; Codex never sees the Claude id.
    desktop
        .ok("app/list", json!({"threadId": claude, "limit": 10}))
        .await;
    desktop
        .ok(
            "app/read",
            json!({"threadId": claude, "appIds": ["fixture"]}),
        )
        .await;
    desktop
        .ok("app/installed", json!({"threadId": codex}))
        .await;
    let state = desktop.ok("fake/state", json!({})).await;
    assert_eq!(
        state["apps"],
        json!([{"limit": 10}, {"appIds": ["fixture"]}, {"threadId": codex}])
    );

    let goal = desktop
        .ok("thread/goal/get", json!({"threadId": claude}))
        .await;
    assert_eq!(goal, json!({"goal": null}));
    desktop
        .ok("thread/delete", json!({"threadId": claude}))
        .await;
    desktop
        .until(|v| v["method"] == "thread/deleted" && v["params"]["threadId"] == claude)
        .await;
    let listed = desktop.ok("thread/list", json!({"limit": 100})).await;
    assert_eq!(ids(&listed).as_slice(), std::slice::from_ref(&codex));
    // The router no longer assumes Claude owns the id: it asks the facade,
    // which no longer knows it, and then genuine Codex.
    let read = desktop
        .call("thread/read", json!({"threadId": claude}))
        .await;
    assert!(read["error"].is_object(), "{read}");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("thread/read")),
        "{state}"
    );
}

#[tokio::test]
async fn claude_server_requests_round_trip_with_namespaced_ids() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_approval-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let thread = desktop
        .thread(json!({"model": "fake-claude", "sandbox": "danger-full-access"}))
        .await;
    let reply = desktop.turn(&thread, "fake-claude", "echo routed").await;
    assert!(reply.get("error").is_none(), "{reply}");
    let request = desktop
        .until(|v| v["method"] == "item/commandExecution/requestApproval")
        .await;
    assert!(request["id"].as_str().unwrap().starts_with("claudex:"));
    desktop
        .send(json!({"id": request["id"], "result": {"decision": "accept"}}))
        .await;
    let resolved = desktop
        .until(|v| v["method"] == "serverRequest/resolved")
        .await;
    assert_eq!(resolved["params"]["requestId"], request["id"]);
    desktop.completed(&thread).await;
}

#[tokio::test]
async fn losing_the_facade_ends_its_running_turns_for_the_desktop() {
    let mut facade = Facade::start(env!("CARGO_BIN_EXE_approval-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let thread = desktop
        .thread(json!({"model": "fake-claude", "sandbox": "danger-full-access"}))
        .await;
    let reply = desktop.turn(&thread, "fake-claude", "echo routed").await;
    assert!(reply.get("error").is_none(), "{reply}");
    let request = desktop
        .until(|v| v["method"] == "item/commandExecution/requestApproval")
        .await;
    // The desktop's own connection stays up across a facade crash or restart,
    // so the router must end what the desktop is showing as running.
    facade._child.kill().await.unwrap();
    let resolved = desktop
        .until(|v| v["method"] == "serverRequest/resolved")
        .await;
    assert_eq!(resolved["params"]["requestId"], request["id"]);
    assert_eq!(resolved["params"]["threadId"], thread);
    let completed = desktop.completed(&thread).await;
    let turn = &completed["params"]["turn"];
    assert_eq!(turn["id"], reply["result"]["turn"]["id"]);
    assert_eq!(turn["status"], "interrupted");
    assert!(turn["error"]["message"].is_string(), "{completed}");
    desktop
        .until(|v| {
            v["method"] == "thread/status/changed"
                && v["params"]["threadId"] == thread
                && v["params"]["status"]["type"] == "idle"
        })
        .await;
}

#[tokio::test]
async fn without_the_facade_the_router_is_plain_codex() {
    let missing = tempfile::tempdir().unwrap();
    let mut desktop = Desktop::connect(&missing.path().join("absent.sock"), 2).await;
    let models = desktop.ok("model/list", json!({})).await;
    assert_eq!(models["data"].as_array().unwrap().len(), 2);
    let rejected = desktop.call("thread/start", json!({"model":"opus"})).await;
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reconnecting")
    );
    let thread = desktop.thread(json!({"model": "gpt-fake"})).await;
    let read = desktop.ok("thread/read", json!({"threadId": thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "openai");
    let listed = desktop.ok("thread/list", json!({"limit": 1})).await;
    assert_eq!(listed["nextCursor"], "fake:1");
    assert_eq!(listed["backwardsCursor"], "fake-backwards");
}

#[tokio::test]
async fn desktop_creation_metadata_settings_and_collaboration_mode_reach_claude() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    // The desktop may prewarm on Codex, assign a project, then pick Claude
    // through settings before submitting a collaboration-mode turn.
    let thread = desktop
        .thread(json!({"cwd":"/tmp","sandbox":"danger-full-access"}))
        .await;
    desktop.ok("thread/metadata/update", json!({"threadId":thread,
        "projectId":"desktop-project", "gitInfo":{"branch":"main","sha":"abc","originUrl":"https://example.test/repo"},
        "daybreakEnabled":false})).await;
    desktop.ok("thread/settings/update", json!({"threadId":thread,
        "collaborationMode":{"mode":"default","settings":{"model":"opus","reasoning_effort":"medium","developer_instructions":null}}})).await;
    let read = desktop.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    assert_eq!(read["thread"]["projectId"], "desktop-project");
    assert_eq!(read["thread"]["gitInfo"]["branch"], "main");
    let turn = desktop.call("turn/start",json!({"threadId":thread,
        "model":null, "input":[{"type":"text","text":"desktop first message","text_elements":[]}],
        "collaborationMode":{"mode":"default","settings":{"model":"opus","reasoning_effort":"medium","developer_instructions":null}}})).await;
    assert!(turn.get("error").is_none(), "{turn}");
    desktop.completed(&thread).await;
    // Metadata patches and subsequent settings persist without erasing omitted fields.
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"gitInfo":{"branch":"feature","sha":null}}),
        )
        .await;
    desktop
        .ok(
            "thread/settings/update",
            json!({"threadId":thread,"model":"sonnet","effort":"high"}),
        )
        .await;
    let mut resumed = Desktop::connect(&facade.socket, 0).await;
    let read = resumed
        .ok("thread/resume", json!({"threadId":thread}))
        .await;
    assert_eq!(read["thread"]["projectId"], "desktop-project");
    assert_eq!(
        read["thread"]["gitInfo"],
        json!({"branch":"feature","sha":null,"originUrl":"https://example.test/repo"})
    );
    assert_eq!(read["model"], "sonnet");
    assert_eq!(read["reasoningEffort"], "high");
    let listed = desktop.ok("thread/list", json!({"limit":100})).await;
    let copies: Vec<_> = listed["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["id"] == thread)
        .collect();
    assert_eq!(
        copies.len(),
        1,
        "Both prewarm copies appeared in the desktop list: {listed}"
    );
    assert_eq!(copies[0]["modelProvider"], "anthropic");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "Claude turn reached Codex: {state}"
    );
}

#[tokio::test]
async fn current_desktop_projectless_task_submits_first_and_followup_messages() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    let cwd = tempfile::tempdir().unwrap();
    let thread=desktop.thread(json!({"cwd":cwd.path(),"model":"opus",
        "permissions":":workspace", "approvalPolicy":"on-request","approvalsReviewer":"auto_review",
        "historyMode":"paginated","threadSource":"user","dynamicTools":[],"runtimeWorkspaceRoots":[cwd.path()],
        "config":{"mcp_servers.node_repl":{"command":"node","args":["desktop-repl.js"]},
            "mcp_servers.artifact_session":{"command":"", "enabled":false},
            "mcp_servers.cua_repl":{"command":"/desktop/electron", "enabled":false},
            "memories.version":"v2","memories.dual_write":true,"features.request_permissions_tool":true}})).await;
    // main's project-assignment write must complete before the desktop sends
    // the first message, including projectless tasks (empty projectId).
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":""}),
        )
        .await;
    let fixture: Value = serde_json::from_str(include_str!("fixtures/desktop-turn.json")).unwrap();
    for _ in 0..2 {
        let mut request = fixture["request"].clone();
        request["threadId"] = json!(thread);
        request["cwd"] = json!(cwd.path());
        request["runtimeWorkspaceRoots"] = json!([cwd.path()]);
        request["sandboxPolicy"]["writableRoots"] = json!([cwd.path()]);
        let reply = desktop.call("turn/start", request).await;
        assert!(reply.get("error").is_none(), "{reply}");
        let completed = desktop.completed(&thread).await;
        assert_eq!(
            completed["params"]["turn"]["status"], "completed",
            "{completed}"
        );
    }
    let read = desktop
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await;
    assert_eq!(read["thread"]["turns"].as_array().unwrap().len(), 2);
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "{state}"
    );
}

#[tokio::test]
async fn model_less_prewarm_uses_the_effective_saved_claude_default() {
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop
        .ok(
            "config/batchWrite",
            json!({"edits":[{"keyPath":"model","value":"opus","mergeStrategy":"upsert"}]}),
        )
        .await;
    let thread = desktop
        .thread(json!({"cwd":"/tmp","sandbox":"danger-full-access"}))
        .await;
    desktop
        .ok(
            "thread/metadata/update",
            json!({"threadId":thread,"projectId":""}),
        )
        .await;
    let reply = desktop
        .call(
            "turn/start",
            json!({"threadId":thread,
        "input":[{"type":"text","text":"inherit saved model","text_elements":[]}]}),
        )
        .await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&thread).await;
    let read = desktop.ok("thread/read", json!({"threadId":thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "anthropic");
    assert_eq!(read["thread"]["model"], "opus");
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("thread/start")),
        "Claude default reached Codex: {state}"
    );
}

#[tokio::test]
async fn service_restart_reconnects_without_sending_claude_requests_to_codex() {
    let mut facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    desktop.ok("model/list", json!({})).await;
    let thread = desktop
        .thread(json!({"model":"opus","sandbox":"danger-full-access"}))
        .await;
    desktop.turn(&thread, "opus", "first").await;
    desktop.completed(&thread).await;
    facade._child.kill().await.unwrap();
    // Observe the disconnect through the merged catalog, rather than racing
    // the operating system's close notification with a fixed sleep.
    tokio::time::timeout(DEADLINE, async {
        loop {
            let models = desktop.ok("model/list", json!({})).await;
            if models["data"].as_array().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let failed = desktop.call("thread/start", json!({"model":"opus"})).await;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reconnecting"),
        "{failed}"
    );
    let failed = desktop.turn(&thread, "opus", "while disconnected").await;
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("reconnecting"),
        "{failed}"
    );
    facade._child = Facade::launch(
        &facade._state,
        &facade.socket,
        env!("CARGO_BIN_EXE_fake-claude"),
    )
    .await;
    tokio::time::timeout(DEADLINE, async {
        loop {
            let models = desktop.ok("model/list", json!({})).await;
            if models["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["id"] == "opus")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // The router restores the thread subscription on the new facade connection.
    let reply = desktop.turn(&thread, "opus", "after reconnect").await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&thread).await;
    let state = desktop.ok("fake/state", json!({})).await;
    assert!(
        !state["requests"]
            .as_array()
            .unwrap()
            .contains(&json!("turn/start")),
        "Claude turn reached Codex: {state}"
    );
}

#[tokio::test]
async fn dropped_images_reach_claude_inline_when_steering_and_queueing() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let facade = Facade::start(env!("CARGO_BIN_EXE_fake-claude")).await;
    let mut desktop = Desktop::connect(&facade.socket, 0).await;
    // Stands in for a screenshot dragged from the capture thumbnail, which
    // only the desktop's process tree (where the router runs) may open.
    let dropped = tempfile::tempdir().unwrap();
    let image = dropped.path().join("Screenshot.png");
    let png = b"\x89PNG\r\n\x1a\nscreenshot";
    std::fs::write(&image, png).unwrap();
    let attachment = json!({"type":"localImage","path":image});
    let inlined =
        json!({"type":"image","url":format!("data:image/png;base64,{}", STANDARD.encode(png))});

    let thread = desktop
        .thread(json!({"model": "fake-claude", "sandbox": "danger-full-access"}))
        .await;
    let reply = desktop.turn(&thread, "fake-claude", "steer-wait").await;
    let turn = reply["result"]["turn"]["id"].clone();
    desktop
        .until(|v| {
            v["method"] == "item/completed" && v["params"]["item"]["text"] == "Waiting for steering"
        })
        .await;

    let missing = desktop
        .call(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
        "input":[{"type":"localImage","path":dropped.path().join("gone.png")}]}),
        )
        .await;
    assert!(
        missing["error"]["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("Cannot read attached image")),
        "{missing}"
    );
    desktop
        .ok(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
        "input":[{"type":"text","text":"look","text_elements":[]},attachment]}),
        )
        .await;
    let steered = desktop
        .until(|v| {
            v["method"] == "item/started"
                && v["params"]["item"]["type"] == "userMessage"
                && v["params"]["item"]["content"][0]["text"] == "look"
        })
        .await;
    assert_eq!(steered["params"]["item"]["content"][1], inlined);
    desktop
        .ok(
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":turn,
        "input":[{"type":"text","text":"done","text_elements":[]}]}),
        )
        .await;
    let completed = desktop.completed(&thread).await;
    assert!(
        completed["params"]["turn"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["text"] == "Steered: look + done"),
        "{completed}"
    );

    // Queued input is replayed by the facade later, so it must be stored inline.
    let queued = desktop
        .ok(
            "thread/queue/add",
            json!({"threadId":thread,
        "clientUserMessageId":"queued-image","input":[attachment]}),
        )
        .await;
    assert_eq!(queued["queuedSubmission"]["input"], json!([inlined]));
}

#[tokio::test]
async fn other_codex_invocations_run_genuine_codex() {
    let output = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args([
            "route",
            "--codex",
            env!("CARGO_BIN_EXE_fake-codex"),
            "--",
            "--version",
        ])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "codex-cli 9.9.9-fake\n"
    );
}
