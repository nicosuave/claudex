#![cfg(feature = "test-backend")]

use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Peer {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    received: Vec<Value>,
    serial: u64,
}

impl Peer {
    async fn open(state: &Path) -> Self {
        Self::open_with_args(state, &[]).await
    }

    async fn open_with_args(state: &Path, extra: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args([
                "app-server",
                "--stdio",
                "--claude",
                env!("CARGO_BIN_EXE_fake-claude"),
            ])
            .args(extra)
            .arg("--state-dir")
            .arg(state)
            .current_dir(state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut peer = Self {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()).lines(),
            child,
            received: vec![],
            serial: 0,
        };
        peer.ok("initialize", json!({"clientInfo":{"name":"history-wire","version":"1"},"capabilities":{"experimentalApi":true}})).await;
        peer
    }

    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(found) = self.received.iter().find(|value| predicate(value)) {
            return found.clone();
        }
        loop {
            let line = tokio::time::timeout(Duration::from_secs(15), self.output.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_str(&line).unwrap();
            self.received.push(value.clone());
            if predicate(&value) {
                return value;
            }
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        let id = self.serial;
        let message = json!({"id":id,"method":method,"params":params});
        self.input
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
        self.until(|value| value["id"] == id && value.get("method").is_none())
            .await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let reply = self.request(method, params).await;
        assert!(reply.get("error").is_none(), "{method}: {reply}");
        reply["result"].clone()
    }

    async fn thread(&mut self, mode: &str) -> String {
        self.ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","historyMode":mode}),
        )
        .await["thread"]["id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn start(&mut self, thread: &str, text: &str) -> String {
        self.ok(
            "turn/start",
            json!({"threadId":thread,"input":[{"type":"text","text":text}]}),
        )
        .await["turn"]["id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn complete(&mut self, turn: &str) -> Value {
        let notification = self
            .until(|value| {
                value["method"] == "turn/completed" && value["params"]["turn"]["id"] == turn
            })
            .await;
        notification["params"]["turn"].clone()
    }

    async fn say(&mut self, thread: &str, text: &str) -> Value {
        let turn = self.start(thread, text).await;
        let result = self.complete(&turn).await;
        assert_eq!(result["status"], "completed", "{result}");
        result
    }

    async fn args(&mut self, thread: &str) -> Value {
        let turn = self.say(thread, "fork-info").await;
        let text = turn["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "agentMessage")
            .unwrap()["text"]
            .as_str()
            .unwrap();
        serde_json::from_str(text).unwrap()
    }

    async fn close(mut self) {
        drop(self.input);
        let status = tokio::time::timeout(Duration::from_secs(15), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
    }
}

fn record(state: &Path, thread: &str) -> Value {
    serde_json::from_slice(
        &std::fs::read(state.join("threads").join(format!("{thread}.json"))).unwrap(),
    )
    .unwrap()
}

fn assert_fork(args: &Value, checkpoint: &Value) {
    assert_eq!(args["fork"], true);
    assert_eq!(args["resume"], checkpoint["session_id"]);
    assert_eq!(args["resumeAt"], checkpoint["message_id"]);
    assert_ne!(args["session"], checkpoint["session_id"]);
}

fn validate_revert(value: &Value) {
    let root: Value = serde_json::from_str(include_str!("../protocol/codex-0.160.0.json")).unwrap();
    let mut schema = root["definitions"]["v2"]["ThreadRevertResponse"].clone();
    schema["definitions"] = root["definitions"].clone();
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert!(validator.is_valid(value), "{value}");
}

#[tokio::test]
async fn forks_use_exact_native_turn_anchors_and_leave_source_unchanged() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let source = peer.thread("legacy").await;
    let first = peer.say(&source, "retained").await;
    let second = peer.say(&source, "discarded").await;
    let original = record(state.path(), &source);
    assert_eq!(original["turn_anchors"].as_object().unwrap().len(), 2);
    let checkpoint = original["turn_anchors"][first["id"].as_str().unwrap()].clone();
    assert_ne!(checkpoint["message_id"], original["backend_message_id"]);
    for params in [
        json!({"threadId":source,"beforeTurnId":second["id"]}),
        json!({"threadId":source,"lastTurnId":first["id"]}),
    ] {
        let fork = peer.ok("thread/fork", params).await;
        assert_eq!(fork["thread"]["turns"], json!([first]));
        let child = fork["thread"]["id"].as_str().unwrap();
        assert_fork(&peer.args(child).await, &checkpoint);
    }
    assert_eq!(record(state.path(), &source), original);
    let empty = peer
        .ok(
            "thread/fork",
            json!({"threadId":source,"beforeTurnId":first["id"]}),
        )
        .await;
    assert_eq!(empty["thread"]["turns"], json!([]));
    let args = peer.args(empty["thread"]["id"].as_str().unwrap()).await;
    assert_eq!(args["fork"], false);
    assert!(args["resume"].is_null());
    assert!(args["resumeAt"].is_null());
    let rollback = peer
        .ok("thread/rollback", json!({"threadId":source,"numTurns":1}))
        .await;
    assert_eq!(rollback["thread"]["turns"], json!([first]));
    assert_fork(&peer.args(&source).await, &checkpoint);
    peer.close().await;
}

#[tokio::test]
async fn reverted_prefix_survives_restart_and_rollback_can_clear_everything() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let thread = peer.thread("paginated").await;
    let first = peer.say(&thread, "retained").await;
    let second = peer.say(&thread, "discarded").await;
    let before = record(state.path(), &thread);
    let checkpoint = before["turn_anchors"][first["id"].as_str().unwrap()].clone();
    let response = peer
        .ok(
            "thread/revert",
            json!({"threadId":thread,"beforeTurnId":second["id"]}),
        )
        .await;
    validate_revert(&response);
    assert_eq!(response["thread"]["turns"], json!([]));
    assert!(response["turnsBackwardsCursor"].is_string());
    assert!(response["itemsBackwardsCursor"].is_string());
    peer.until(|value| {
        value["method"] == "thread/reverted" && value["params"]["threadId"] == thread
    })
    .await;
    let page = peer.ok("thread/turns/list", json!({"threadId":thread,"cursor":response["turnsBackwardsCursor"],"sortDirection":"desc"})).await;
    assert_eq!(page["data"][0]["id"], first["id"]);
    let recovered = record(state.path(), &thread);
    assert_eq!(recovered["turns"], json!([first]));
    assert_eq!(recovered["turn_anchors"].as_object().unwrap().len(), 1);
    assert_eq!(recovered["has_session"], false);
    assert_ne!(recovered["session_id"], before["session_id"]);
    peer.close().await;

    let mut peer = Peer::open(state.path()).await;
    let unloaded = peer
        .request("thread/rollback", json!({"threadId":thread,"numTurns":1}))
        .await;
    assert!(
        unloaded["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Resume")
    );
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    assert_fork(&peer.args(&thread).await, &checkpoint);
    let clear = peer
        .ok("thread/rollback", json!({"threadId":thread,"numTurns":99}))
        .await;
    assert_eq!(clear["thread"]["turns"], json!([]));
    let cleared = record(state.path(), &thread);
    assert_eq!(cleared["turn_anchors"], json!({}));
    assert_eq!(cleared["item_times"], json!({}));
    let args = peer.args(&thread).await;
    assert_eq!(args["fork"], false);
    assert!(args["resume"].is_null());
    peer.close().await;
}

#[tokio::test]
async fn invalid_recovery_is_atomic_and_legacy_records_fail_closed() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let thread = peer.thread("legacy").await;
    let first = peer.say(&thread, "first").await;
    peer.say(&thread, "second").await;
    for (method, params) in [
        (
            "thread/revert",
            json!({"threadId":thread,"beforeTurnId":first["id"]}),
        ),
        ("thread/rollback", json!({"threadId":thread,"numTurns":0})),
        (
            "thread/fork",
            json!({"threadId":thread,"beforeTurnId":"unknown"}),
        ),
        (
            "thread/fork",
            json!({"threadId":thread,"beforeTurnId":first["id"],"lastTurnId":first["id"]}),
        ),
    ] {
        let before = record(state.path(), &thread);
        assert!(peer.request(method, params).await.get("error").is_some());
        assert_eq!(record(state.path(), &thread), before);
    }
    let active = peer.start(&thread, "hang").await;
    assert!(
        peer.request("thread/rollback", json!({"threadId":thread,"numTurns":1}))
            .await
            .get("error")
            .is_some()
    );
    peer.ok("turn/interrupt", json!({"threadId":thread,"turnId":active}))
        .await;
    peer.complete(&active).await;
    peer.close().await;

    let path = state.path().join("threads").join(format!("{thread}.json"));
    let mut legacy = record(state.path(), &thread);
    legacy.as_object_mut().unwrap().remove("turn_anchors");
    legacy
        .as_object_mut()
        .unwrap()
        .remove("tracks_turn_anchors");
    std::fs::write(&path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();
    let mut peer = Peer::open(state.path()).await;
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    let before = record(state.path(), &thread);
    let error = peer
        .request("thread/rollback", json!({"threadId":thread,"numTurns":1}))
        .await;
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("exact Claude transcript anchor")
    );
    assert_eq!(record(state.path(), &thread), before);
    let fork = peer.ok("thread/fork", json!({"threadId":thread})).await;
    let checkpoint =
        json!({"session_id":before["session_id"],"message_id":before["backend_message_id"]});
    assert_fork(
        &peer.args(fork["thread"]["id"].as_str().unwrap()).await,
        &checkpoint,
    );
    peer.close().await;
}

#[tokio::test]
async fn native_startup_failure_cannot_reuse_the_previous_turn_anchor() {
    let state = tempfile::tempdir().unwrap();
    let mut peer = Peer::open(state.path()).await;
    let thread = peer.thread("paginated").await;
    let first = peer.say(&thread, "retained native history").await;
    let successful = record(state.path(), &thread);
    peer.close().await;

    // Crash before native initialization or prompt consumption, so the failed
    // facade turn has no native UUID. The preceding UUID still exists globally.
    let mut peer = Peer::open_with_args(state.path(), &["--claude-arg=--fake-init-crash"]).await;
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    let failed_id = peer.start(&thread, "never consumed by Claude").await;
    let failed = peer.complete(&failed_id).await;
    assert_eq!(failed["status"], "failed");
    let crashed = record(state.path(), &thread);
    assert_eq!(crashed["tracks_turn_anchors"], true);
    assert_eq!(
        crashed["backend_message_id"],
        successful["backend_message_id"]
    );
    assert!(crashed["turn_anchors"].get(&failed_id).is_none());
    for params in [
        json!({"threadId":thread}),
        json!({"threadId":thread,"lastTurnId":failed_id}),
    ] {
        let rejected = peer.request("thread/fork", params).await;
        assert!(
            rejected["error"]["message"]
                .as_str()
                .unwrap()
                .contains("exact Claude transcript anchor")
        );
        assert_eq!(record(state.path(), &thread), crashed);
    }
    peer.close().await;

    let mut peer = Peer::open(state.path()).await;
    peer.ok("thread/resume", json!({"threadId":thread})).await;
    let next = peer.say(&thread, "later native turn").await;
    let before_revert = record(state.path(), &thread);
    let rejected = peer
        .request(
            "thread/revert",
            json!({"threadId":thread,"beforeTurnId":next["id"]}),
        )
        .await;
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("exact Claude transcript anchor")
    );
    assert_eq!(record(state.path(), &thread), before_revert);
    // Excluding the failed turn itself retains a known valid native prefix.
    peer.ok(
        "thread/revert",
        json!({"threadId":thread,"beforeTurnId":failed_id}),
    )
    .await;
    let checkpoint = &successful["turn_anchors"][first["id"].as_str().unwrap()];
    assert_fork(&peer.args(&thread).await, checkpoint);
    peer.close().await;
}
