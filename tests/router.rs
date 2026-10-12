#![cfg(all(feature = "test-backend", unix))]
//! The local router between genuine Codex (a deterministic stand-in here) and
//! the Claude facade, exercised end to end through its stdio transport.

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
        let state = tempfile::tempdir().unwrap();
        let socket = state.path().join("f.sock");
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        for name in DEPLOYMENT_ENV {
            command.env_remove(name);
        }
        let child = command
            .args(["app-server", "--claude", claude, "--model", "fake-claude"])
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
            while !socket.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("facade socket");
        Self {
            _child: child,
            socket,
            _state: state,
        }
    }
}

struct Desktop {
    _child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    history: Vec<Value>,
    serial: u64,
}

impl Desktop {
    async fn connect(socket: &Path, seeded_codex_threads: usize) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"));
        for name in DEPLOYMENT_ENV {
            command.env_remove(name);
        }
        let mut child = command
            .arg("--sock")
            .arg(socket)
            .args(["route", "--codex", env!("CARGO_BIN_EXE_fake-codex"), "--"])
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
        };
        let init = desktop
            .ok(
                "initialize",
                json!({"clientInfo":{"name":"codex_desktop","version":"1"},
                    "capabilities":{"experimentalApi":true}}),
            )
            .await;
        assert_eq!(init["userAgent"], "codex_cli_rs/9.9.9-fake");
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
        let line = tokio::time::timeout(DEADLINE, self.output.next_line())
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

    let codex = desktop.thread(json!({"model": "gpt-fake"})).await;
    let reply = desktop.turn(&codex, "gpt-fake", "hello codex").await;
    assert!(reply.get("error").is_none(), "{reply}");
    desktop.completed(&codex).await;

    let claude = desktop
        .thread(json!({"model": "opus", "config": {
            "mcp_servers.node_repl":{"command":"node", "args":["desktop-repl.js"]},
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
async fn without_the_facade_the_router_is_plain_codex() {
    let missing = tempfile::tempdir().unwrap();
    let mut desktop = Desktop::connect(&missing.path().join("absent.sock"), 2).await;
    let models = desktop.ok("model/list", json!({})).await;
    assert_eq!(models["data"].as_array().unwrap().len(), 2);
    let thread = desktop.thread(json!({"model": "opus"})).await;
    let read = desktop.ok("thread/read", json!({"threadId": thread})).await;
    assert_eq!(read["thread"]["modelProvider"], "openai");
    let listed = desktop.ok("thread/list", json!({"limit": 1})).await;
    assert_eq!(listed["nextCursor"], "fake:1");
    assert_eq!(listed["backwardsCursor"], "fake-backwards");
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
