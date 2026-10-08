//! Opt-in proof of the installed desktop's guardian payload through the facade.
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Peer {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    serial: u64,
    approve_outside: Option<std::path::PathBuf>,
    approvals: usize,
}

impl Peer {
    async fn send(&mut self, value: Value) {
        self.input
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(90), self.output.next_line())
            .await
            .unwrap()
            .unwrap()
            .expect("facade closed");
        let value: Value = serde_json::from_str(&line).unwrap();
        if value.get("id").is_some() && value.get("method").is_some() {
            let path = self
                .approve_outside
                .as_ref()
                .expect("ordinary workspace work must not need manual approval");
            assert_eq!(
                value["method"], "item/commandExecution/requestApproval",
                "{value}"
            );
            assert!(
                value["params"]["command"]
                    .as_str()
                    .unwrap()
                    .contains(path.to_str().unwrap()),
                "{value}"
            );
            assert!(
                value["params"]["reason"]
                    .as_str()
                    .unwrap()
                    .to_lowercase()
                    .contains("sandbox"),
                "escalation scope must be visible: {value}"
            );
            self.approvals += 1;
            self.send(json!({"id":value["id"],"result":{"decision":"accept"}}))
                .await;
        }
        value
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = self.serial;
        self.send(json!({"id":id,"method":method,"params":params}))
            .await;
        loop {
            let value = self.read().await;
            if value["id"] == id {
                assert!(value.get("error").is_none(), "{method}: {value}");
                return value["result"].clone();
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires installed authenticated Claude and host sandbox access"]
async fn native_guardian_workspace_payload_runs_without_manual_approvals() {
    let fixture = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let workspace = fixture.path().join("workspace");
    let state = fixture.path().join("state");
    std::fs::create_dir(&workspace).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args(["app-server", "--stdio", "--model", "opus", "--state-dir"])
        .arg(&state)
        .args([
            "--claude-arg=--setting-sources=",
            "--claude-arg=--strict-mcp-config",
            "--claude-arg=--tools=Bash,Write,Read",
        ])
        .env_remove("CLAUDE_CODEX_PLUGIN_EXECUTABLE")
        .current_dir(&workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let input = child.stdin.take().unwrap();
    let output = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut peer = Peer {
        child,
        input,
        output,
        serial: 0,
        approve_outside: None,
        approvals: 0,
    };
    peer.rpc("initialize", json!({"clientInfo":{"name":"auto-workspace-proof","version":"1"},"capabilities":{"experimentalApi":true}})).await;
    let requirements = peer.rpc("configRequirements/read", Value::Null).await;
    assert!(
        requirements["requirements"]["allowedSandboxModes"]
            .as_array()
            .unwrap()
            .contains(&json!("workspace-write"))
    );
    assert!(
        requirements["requirements"]["allowedApprovalsReviewers"]
            .as_array()
            .unwrap()
            .contains(&json!("guardian_subagent"))
    );
    // Omit sandbox to exercise the same workspace default advertised by config/read.
    let thread = peer.rpc("thread/start", json!({"cwd":workspace,
        "approvalPolicy":"on-request","approvalsReviewer":"guardian_subagent","model":"opus","effort":"low","ephemeral":true})).await;
    assert_eq!(thread["sandbox"]["type"], "workspaceWrite");
    assert_eq!(thread["approvalsReviewer"], "auto_review");
    let id = thread["thread"]["id"].as_str().unwrap();
    peer.rpc("turn/start", json!({"threadId":id,"approvalsReviewer":"guardian_subagent","approvalPolicy":"on-request",
        "sandboxPolicy":{"type":"workspaceWrite","writableRoots":[],"networkAccess":false,"excludeSlashTmp":false,"excludeTmpdirEnvVar":false},
        "input":[{"type":"text","text":"Use Write to create auto-write.txt in the working directory with exactly AUTO_WRITE_OK. Then use Bash to run printf AUTO_SHELL_OK > auto-shell.txt. Do these ordinary project edits now and report completion.","text_elements":[]}]})).await;
    loop {
        let value = peer.read().await;
        if value["method"] == "turn/completed" {
            assert_eq!(value["params"]["turn"]["status"], "completed", "{value}");
            break;
        }
    }
    assert_eq!(
        std::fs::read_to_string(workspace.join("auto-write.txt"))
            .unwrap()
            .trim(),
        "AUTO_WRITE_OK"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("auto-shell.txt")).unwrap(),
        "AUTO_SHELL_OK"
    );
    assert_eq!(peer.approvals, 0);
    let outside = fixture.path().join("outside-approved.txt");
    peer.approve_outside = Some(outside.clone());
    let escape_thread = peer.rpc("thread/start", json!({"cwd":workspace,"permissions":":workspace",
        "approvalPolicy":"on-request","approvalsReviewer":"guardian_subagent","model":"opus","effort":"low","ephemeral":true})).await;
    peer.rpc("turn/start", json!({"threadId":escape_thread["thread"]["id"],
        "input":[{"type":"text","text":format!("Use Bash to save the integration status note AUTO_OUTSIDE_OK to {}. This exact destination is the output requested for this task.", outside.display()),"text_elements":[]}]})).await;
    loop {
        let value = peer.read().await;
        if value["method"] == "turn/completed" {
            assert_eq!(value["params"]["turn"]["status"], "completed", "{value}");
            break;
        }
    }
    assert_eq!(
        peer.approvals, 1,
        "one explicit sandbox escape approval expected"
    );
    assert_eq!(
        std::fs::read_to_string(outside).unwrap().trim(),
        "AUTO_OUTSIDE_OK"
    );
    peer.child.kill().await.unwrap();
    peer.child.wait().await.unwrap();
}

#[tokio::test]
#[ignore = "requires installed authenticated Claude"]
async fn native_resume_updates_instructions_and_preserves_history() {
    let fixture = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args(["app-server", "--stdio", "--model", "opus", "--state-dir"])
        .arg(fixture.path().join("state"))
        .args([
            "--claude-arg=--setting-sources=",
            "--claude-arg=--strict-mcp-config",
            "--claude-arg=--tools=",
        ])
        .env_remove("CLAUDE_CODEX_PLUGIN_EXECUTABLE")
        .current_dir(fixture.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let input = child.stdin.take().unwrap();
    let output = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut peer = Peer {
        child,
        input,
        output,
        serial: 0,
        approve_outside: None,
        approvals: 0,
    };
    peer.rpc("initialize", json!({"clientInfo":{"name":"instruction-resume-proof","version":"1"},"capabilities":{"experimentalApi":true}})).await;
    let thread = peer.rpc("thread/start", json!({"cwd":fixture.path(),"model":"opus","effort":"low","sandbox":"danger-full-access","approvalPolicy":"never","developerInstructions":"Respond briefly."})).await;
    let id = thread["thread"]["id"].as_str().unwrap();
    let secret = uuid::Uuid::new_v4().to_string();
    peer.rpc("turn/start", json!({"threadId":id,"input":[{"type":"text","text":format!("Remember this code for my next question: {secret}. Say OK.")}]})).await;
    loop {
        let value = peer.read().await;
        if value["method"] == "turn/completed" {
            assert_eq!(value["params"]["turn"]["status"], "completed", "{value}");
            break;
        }
    }
    let marker = uuid::Uuid::new_v4().to_string();
    peer.rpc("thread/resume", json!({"threadId":id,
        "baseInstructions":"Answer the user's question using the preserved conversation history. Follow the developer's response format.",
        "developerInstructions":format!("For your next response output the remembered code followed by this suffix: {marker}")})).await;
    peer.rpc("turn/start", json!({"threadId":id,"input":[{"type":"text","text":"What code did I ask you to remember?"}]})).await;
    loop {
        let value = peer.read().await;
        if value["method"] == "turn/completed" {
            let turn = &value["params"]["turn"];
            assert_eq!(turn["status"], "completed", "{value}");
            let response = turn["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| item["type"] == "agentMessage")
                .filter_map(|item| item["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(response.contains(&secret), "history lost: {response}");
            assert!(
                response.contains(&marker),
                "new instructions ignored: {response}"
            );
            break;
        }
    }
    peer.child.kill().await.unwrap();
}
