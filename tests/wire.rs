#![cfg(feature = "test-backend")]

use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    process::Stdio,
    sync::OnceLock,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

const DEADLINE: Duration = Duration::from_secs(15);

/// Compile independent upstream contracts once; do not derive expected fields
/// from the facade's constructors or reuse its request-only validators.
fn schemas() -> &'static HashMap<String, jsonschema::Validator> {
    static SCHEMAS: OnceLock<HashMap<String, jsonschema::Validator>> = OnceLock::new();
    SCHEMAS.get_or_init(|| {
        let root: Value =
            serde_json::from_str(include_str!("../protocol/codex-0.160.0.json")).unwrap();
        let mut validators = HashMap::new();
        for name in [
            "InitializeResponse",
            "ServerNotification",
            "ServerRequest",
            "JSONRPCResponse",
            "JSONRPCErrorError",
        ] {
            let mut schema = root["definitions"][name].clone();
            schema["definitions"] = root["definitions"].clone();
            validators.insert(name.to_owned(), jsonschema::validator_for(&schema).unwrap());
        }
        for name in [
            "ThreadTimelineListResponse",
            "FsReadFileResponse",
            "FsWriteFileResponse",
            "FsCopyResponse",
            "FsRemoveResponse",
            "CommandExecResponse",
            "ThreadStartResponse",
            "ThreadAttachmentAddResponse",
            "ThreadAttachmentListResponse",
            "ThreadAttachmentRemoveResponse",
            "ThreadResumeResponse",
            "ThreadForkResponse",
            "ThreadReadResponse",
            "ThreadListResponse",
            "ThreadLoadedListResponse",
            "ThreadTurnsListResponse",
            "ThreadItemsListResponse",
            "ThreadSetNameResponse",
            "ThreadArchiveResponse",
            "ThreadUnarchiveResponse",
            "TurnStartResponse",
            "TurnInterruptResponse",
            "TurnSteerResponse",
            "ThreadQueueAddResponse",
            "ThreadQueueListResponse",
            "ThreadQueueUpdateResponse",
            "ThreadQueueDeleteResponse",
            "ThreadQueueReorderResponse",
            "ThreadQueueStartResponse",
            "ModelListResponse",
            "GetAccountResponse",
            "ConfigReadResponse",
            "ConfigWriteResponse",
            "FsCreateDirectoryResponse",
            "FsGetMetadataResponse",
            "FsReadDirectoryResponse",
            "ConfigRequirementsReadResponse",
            "CollaborationModeListResponse",
            "ExperimentalFeatureListResponse",
        ] {
            let mut schema = root["definitions"]["v2"][name].clone();
            assert!(schema.is_object(), "missing upstream definition {name}");
            schema["definitions"] = root["definitions"].clone();
            validators.insert(name.to_owned(), jsonschema::validator_for(&schema).unwrap());
        }
        // Legacy v1 response is not included in the schema export.
        validators.insert(
            "GetAuthStatusResponse".into(),
            jsonschema::validator_for(&json!({
                "type": "object", "required": ["authMethod", "authToken", "requiresOpenaiAuth"],
                "properties": {
                    "authMethod": {"type": "null"}, "authToken": {"type": "null"},
                    "requiresOpenaiAuth": {"const": false}
                }
            }))
            .unwrap(),
        );
        validators
    })
}

fn validate(name: &str, value: &Value) {
    let errors: Vec<_> = schemas()[name]
        .iter_errors(value)
        .map(|error| format!("{}: {error}", error.instance_path))
        .collect();
    assert!(
        errors.is_empty(),
        "{name} violated:\n{}\nWire value: {value}",
        errors.join("\n")
    );
}

fn response_schema(method: &str) -> &'static str {
    match method {
        "thread/timeline/list" => "ThreadTimelineListResponse",
        "fs/readFile" => "FsReadFileResponse",
        "fs/writeFile" => "FsWriteFileResponse",
        "fs/copy" => "FsCopyResponse",
        "fs/remove" => "FsRemoveResponse",
        "command/exec" => "CommandExecResponse",
        "initialize" => "InitializeResponse",
        "thread/start" => "ThreadStartResponse",
        "thread/attachment/add" => "ThreadAttachmentAddResponse",
        "thread/attachment/list" => "ThreadAttachmentListResponse",
        "thread/attachment/remove" => "ThreadAttachmentRemoveResponse",
        "thread/resume" => "ThreadResumeResponse",
        "thread/fork" => "ThreadForkResponse",
        "thread/read" => "ThreadReadResponse",
        "thread/list" => "ThreadListResponse",
        "thread/loaded/list" => "ThreadLoadedListResponse",
        "thread/turns/list" => "ThreadTurnsListResponse",
        "thread/items/list" => "ThreadItemsListResponse",
        "thread/name/set" => "ThreadSetNameResponse",
        "thread/archive" => "ThreadArchiveResponse",
        "thread/unarchive" => "ThreadUnarchiveResponse",
        "turn/start" => "TurnStartResponse",
        "turn/interrupt" => "TurnInterruptResponse",
        "turn/steer" => "TurnSteerResponse",
        "thread/queue/add" => "ThreadQueueAddResponse",
        "thread/queue/list" => "ThreadQueueListResponse",
        "thread/queue/update" => "ThreadQueueUpdateResponse",
        "thread/queue/delete" => "ThreadQueueDeleteResponse",
        "thread/queue/reorder" => "ThreadQueueReorderResponse",
        "thread/queue/start" => "ThreadQueueStartResponse",
        "model/list" => "ModelListResponse",
        "account/read" => "GetAccountResponse",
        "getAuthStatus" => "GetAuthStatusResponse",
        "config/read" => "ConfigReadResponse",
        "config/batchWrite" => "ConfigWriteResponse",
        "fs/createDirectory" => "FsCreateDirectoryResponse",
        "fs/getMetadata" => "FsGetMetadataResponse",
        "fs/readDirectory" => "FsReadDirectoryResponse",
        "configRequirements/read" => "ConfigRequirementsReadResponse",
        "collaborationMode/list" => "CollaborationModeListResponse",
        "experimentalFeature/list" => "ExperimentalFeatureListResponse",
        _ => panic!("unexpected success for unsupported method {method}"),
    }
}

struct Client {
    child: Child,
    input: Option<ChildStdin>,
    output: Lines<BufReader<ChildStdout>>,
    methods: HashMap<String, String>,
    history: Vec<Value>,
    serial: u64,
}

impl Client {
    async fn spawn(state: &Path) -> Self {
        Self::spawn_with_args(state, &[]).await
    }

    async fn spawn_with_args(state: &Path, extra: &[&str]) -> Self {
        schemas();
        let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
            .args([
                "app-server",
                "--stdio",
                "--claude",
                env!("CARGO_BIN_EXE_fake-claude"),
                "--model",
                "fake-claude",
                "--initialize-timeout-seconds",
                "3",
                "--approval-timeout-seconds",
                "5",
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
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            input,
            output,
            methods: HashMap::new(),
            history: vec![],
            serial: 0,
        }
    }

    async fn send(&mut self, value: Value) {
        self.raw(&format!("{value}\n")).await;
    }

    async fn raw(&mut self, text: &str) {
        tokio::time::timeout(
            DEADLINE,
            self.input.as_mut().unwrap().write_all(text.as_bytes()),
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(DEADLINE, self.output.next_line())
            .await
            .expect("facade response timed out")
            .unwrap()
            .expect("facade unexpectedly closed stdout");
        let value: Value =
            serde_json::from_str(&line).expect("stdout must contain only JSON frames");
        if value.get("method").is_some() {
            validate(
                if value.get("id").is_some() {
                    "ServerRequest"
                } else {
                    "ServerNotification"
                },
                &value,
            );
        } else if value.get("error").is_some() {
            // Parse errors have no request ID; Codex's RequestId schema excludes
            // null, so validate the owned error payload and test null explicitly.
            validate("JSONRPCErrorError", &value["error"]);
        } else {
            validate("JSONRPCResponse", &value);
            let method = self
                .methods
                .get(&value["id"].to_string())
                .expect("response ID must belong to a request");
            validate(response_schema(method), &value["result"]);
        }
        self.history.push(value.clone());
        value
    }

    async fn request_id(&mut self, id: Value, method: &str, params: Value) -> Value {
        assert!(
            self.methods
                .insert(id.to_string(), method.to_owned())
                .is_none(),
            "test reused a request ID"
        );
        self.send(json!({"id":id,"method":method,"params":params}))
            .await;
        loop {
            let value = self.read().await;
            if value.get("method").is_none() && value["id"] == id {
                return value;
            }
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.serial += 1;
        self.request_id(json!(format!("request-{}", self.serial)), method, params)
            .await
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let value = self.request(method, params).await;
        assert!(value.get("error").is_none(), "{method} failed: {value}");
        value["result"].clone()
    }

    async fn initialize(&mut self) {
        self.ok("initialize", json!({"clientInfo":{"name":"wire-tests","version":"1"},"capabilities":{"experimentalApi":true}})).await;
        self.send(json!({"method":"initialized"})).await;
    }

    async fn thread(&mut self) -> String {
        self.ok("thread/start", json!({"sandbox":"danger-full-access"}))
            .await["thread"]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn start(&mut self, thread: &str, text: &str) -> String {
        self.ok(
            "turn/start",
            json!({"threadId":thread,"input":[{"type":"text","text":text,"text_elements":[]}]}),
        )
        .await["turn"]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn until(&mut self, predicate: impl Fn(&Value) -> bool) -> Value {
        if let Some(value) = self.history.iter().find(|value| predicate(value)) {
            return value.clone();
        }
        loop {
            let value = self.read().await;
            if predicate(&value) {
                return value;
            }
        }
    }

    async fn completed(&mut self, turn: &str) -> Value {
        self.until(|value| {
            value["method"] == "turn/completed" && value["params"]["turn"]["id"] == turn
        })
        .await["params"]["turn"]
            .clone()
    }

    async fn close(mut self) {
        self.input.take();
        let status = tokio::time::timeout(DEADLINE, self.child.wait())
            .await
            .expect("facade did not stop after stdin EOF")
            .unwrap();
        assert!(status.success(), "facade exit: {status}");
    }
}

fn assert_echo_lifecycle(client: &Client, turn: &str, expected: &str) {
    let events: Vec<_> = client
        .history
        .iter()
        .filter(|value| value["params"]["turnId"] == turn || value["params"]["turn"]["id"] == turn)
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|value| value["method"] == "turn/started")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|value| value["method"] == "turn/completed")
            .count(),
        1
    );
    let started = events
        .iter()
        .position(|value| value["method"] == "turn/started")
        .unwrap();
    let completed = events
        .iter()
        .position(|value| value["method"] == "turn/completed")
        .unwrap();
    assert!(started < completed);
    let mut open = HashSet::new();
    let mut closed = HashSet::new();
    for value in &events {
        if value["method"] == "item/started" {
            assert!(
                open.insert(value["params"]["item"]["id"].to_string()),
                "duplicate item start"
            );
        }
        if value["method"] == "item/completed" {
            let id = value["params"]["item"]["id"].to_string();
            assert!(open.contains(&id), "item completed before start");
            assert!(closed.insert(id), "duplicate item completion");
        }
    }
    assert_eq!(open, closed, "terminal turn left incomplete items");
    let delta = events
        .iter()
        .filter(|value| value["method"] == "item/agentMessage/delta")
        .map(|value| value["params"]["delta"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(
        delta, expected,
        "partial and final Claude messages must not double text"
    );
    let messages: Vec<_> = events
        .iter()
        .filter(|value| {
            value["method"] == "item/completed" && value["params"]["item"]["type"] == "agentMessage"
        })
        .collect();
    assert_eq!(messages.len(), 1, "expected one final assistant item");
    assert_eq!(messages[0]["params"]["item"]["text"], expected);
    let terminal = &events[completed]["params"]["turn"];
    assert_eq!(terminal["status"], "completed");
    assert_eq!(
        terminal["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "agentMessage")
            .count(),
        1
    );
}

#[tokio::test]
async fn handshake_ids_and_malformed_input_recovery() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    let before = client.request_id(json!(17), "thread/list", json!({})).await;
    assert_eq!(before["id"], 17);
    assert_eq!(before["error"]["code"], -32000);
    let init = client
        .request_id(
            json!("initialize-string-id"),
            "initialize",
            json!({"clientInfo":{"name":"wire-tests","version":"1"}}),
        )
        .await;
    assert_eq!(init["id"], "initialize-string-id");
    assert!(
        init["result"]["userAgent"]
            .as_str()
            .unwrap()
            .contains("claude-codex-server")
    );
    assert_eq!(client.ok("thread/list", json!({})).await["data"], json!([]));
    client.ok("config/read", json!({})).await;
    assert_eq!(
        client
            .ok(
                "getAuthStatus",
                json!({"includeToken": true, "refreshToken": false})
            )
            .await,
        json!({"authMethod": null, "authToken": null, "requiresOpenaiAuth": false})
    );
    client.send(json!({"method":"initialized"})).await;
    let experimental = client
        .request("thread/start", json!({"historyMode":"legacy"}))
        .await;
    assert_eq!(experimental["error"]["code"], -32602);
    assert!(
        experimental["error"]["message"]
            .as_str()
            .unwrap()
            .contains("experimentalApi")
    );
    assert_eq!(
        client
            .request(
                "initialize",
                json!({"clientInfo":{"name":"again","version":"1"}})
            )
            .await["error"]["code"],
        -32600
    );
    client.raw("{bad json\n").await;
    let error = client.read().await;
    assert_eq!(error["error"]["code"], -32700);
    assert!(error["id"].is_null());
    let list = client
        .request_id(json!(9007199254740991_u64), "thread/list", json!({}))
        .await;
    assert_eq!(list["id"], json!(9007199254740991_u64));
    assert_eq!(list["result"]["data"], json!([]));
    for method in ["model/list", "account/read", "config/read"] {
        client.ok(method, json!({})).await;
    }
    client.ok("configRequirements/read", Value::Null).await;
    client.close().await;
}

#[tokio::test]
async fn desktop_workspace_and_model_selection_survive_restart() {
    let state = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let cwd = workspace.path().join("new-task/nested");
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let config = client
        .ok("config/read", json!({"includeLayers": false}))
        .await;
    let provider = config["config"]["model_provider"].as_str().unwrap();
    // Desktop requires this map entry to bypass its OpenAI model allowlist.
    assert!(config["config"]["model_providers"][provider].is_object());
    let catalog = client
        .ok("model/list", json!({"includeHidden": true, "limit": 100}))
        .await;
    for model in ["opus", "fable", "sonnet", "haiku"] {
        let entry = catalog["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["model"] == model)
            .unwrap();
        assert_eq!(entry["hidden"], false);
        assert!(entry["displayName"].as_str().unwrap().ends_with(" test"));
        assert_eq!(
            entry["supportedReasoningEfforts"].as_array().unwrap().len(),
            5
        );
    }
    client.ok("fs/createDirectory", json!({"path": cwd})).await;
    client
        .ok(
            "fs/createDirectory",
            json!({"path": cwd, "recursive": true}),
        )
        .await;
    assert!(cwd.is_dir());
    let metadata = client.ok("fs/getMetadata", json!({"path": cwd})).await;
    assert_eq!(metadata["isDirectory"], true);
    assert_eq!(metadata["isFile"], false);
    assert_eq!(metadata["isSymlink"], false);
    std::fs::write(cwd.join("file.txt"), "content").unwrap();
    std::fs::create_dir(cwd.join("folder")).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(cwd.join("folder"), cwd.join("link")).unwrap();
    let entries = client.ok("fs/readDirectory", json!({"path": cwd})).await;
    let entries = entries["entries"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e["fileName"] == "file.txt" && e["isFile"] == true)
    );
    assert!(
        entries
            .iter()
            .any(|e| e["fileName"] == "folder" && e["isDirectory"] == true)
    );
    #[cfg(unix)]
    {
        assert!(
            entries
                .iter()
                .any(|e| e["fileName"] == "link" && e["isDirectory"] == true)
        );
        let link = client
            .ok("fs/getMetadata", json!({"path": cwd.join("link")}))
            .await;
        assert_eq!(link["isSymlink"], true);
        assert_eq!(link["isDirectory"], true);
    }
    assert!(
        client
            .request("fs/getMetadata", json!({"path": cwd.join("absent")}))
            .await
            .get("error")
            .is_some()
    );
    assert!(
        client
            .request("fs/readDirectory", json!({"path": cwd.join("file.txt")}))
            .await
            .get("error")
            .is_some()
    );
    assert!(
        client
            .request("fs/createDirectory", json!({"path": "relative"}))
            .await
            .get("error")
            .is_some()
    );
    assert!(
        client
            .request(
                "fs/createDirectory",
                json!({"path": workspace.path().join("missing/child"), "recursive": false})
            )
            .await
            .get("error")
            .is_some()
    );
    let write = client
        .ok(
            "config/batchWrite",
            json!({"edits": [
        {"keyPath": "model", "value": "sonnet", "mergeStrategy": "upsert"},
        {"keyPath": "model_reasoning_effort", "value": "medium", "mergeStrategy": "upsert"}
    ], "filePath": null, "expectedVersion": null, "reloadUserConfig": true}),
        )
        .await;
    assert_eq!(write["status"], "ok");
    // Invalid batches must not partially apply earlier valid edits.
    assert!(client.request("config/batchWrite", json!({"edits": [
        {"keyPath": "model", "value": "opus", "mergeStrategy": "upsert"},
        {"keyPath": "sandbox_mode", "value": "workspace-write", "mergeStrategy": "upsert"}
    ]})).await.get("error").is_some());
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let config = client.ok("config/read", json!({})).await;
    assert_eq!(config["config"]["model"], "sonnet");
    assert_eq!(config["config"]["model_reasoning_effort"], "medium");
    let thread = client
        .ok(
            "thread/start",
            json!({"cwd": cwd, "threadSource": "user", "sandbox":"danger-full-access"}),
        )
        .await;
    assert_eq!(thread["model"], "sonnet");
    assert_eq!(thread["reasoningEffort"], "medium");
    let id = thread["thread"]["id"].as_str().unwrap();
    let turn = client.start(id, "desktop creation").await;
    assert_eq!(client.completed(&turn).await["status"], "completed");
    client.close().await;
}

#[tokio::test]
async fn desktop_thread_and_turn_options_reach_claude() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.ok("thread/start", json!({
        "cwd":state.path(), "model":"opus", "modelProvider":null,
        "approvalPolicy":"on-request", "approvalsReviewer":"user",
        "sandbox":"danger-full-access", "runtimeWorkspaceRoots":[state.path()],
        "historyMode":"paginated", "threadSource":"user", "ephemeral":null,
        "serviceTier":null, "personality":"pragmatic", "mockExperimentalField":null,
        "experimentalRawEvents":false, "dynamicTools":[],
        "baseInstructions":null, "developerInstructions":"Answer concisely.",
        "config":{"model_reasoning_effort":"xhigh", "features.request_permissions_tool":true,
            "apps.connector_openai_pages.tools":{"chatgpt_space.create_canvas":{"enabled":false}}}
    })).await;
    assert_eq!(thread["model"], "opus");
    assert_eq!(thread["reasoningEffort"], "xhigh");
    let id = &thread["thread"]["id"];
    let turn = client.ok("turn/start", json!({
        "threadId":id, "input":[{"type":"text","text":"desktop payload","text_elements":[]}],
        "approvalsReviewer":"user", "approvalPolicy":"on-request",
        "sandboxPolicy":{"type":"dangerFullAccess"}, "runtimeWorkspaceRoots":[state.path()],
        "turnTrigger":"submit", "responsesapiClientMetadata":{}, "multiAgentMode":"explicitRequestOnly",
        "disabledPluginIds":[], "collaborationMode":{"mode":"default",
            "settings":{"model":"fable","reasoning_effort":"max","developer_instructions":null}},
        "additionalContext":{"test":{"kind":"untrusted","value":"Desktop reference context"}}
    })).await;
    assert_eq!(
        client.completed(turn["turn"]["id"].as_str().unwrap()).await["status"],
        "completed"
    );
    let resumed = client
        .ok(
            "thread/resume",
            json!({"threadId":id,"approvalsReviewer":"user",
        "experimentalRawEvents":false,"config":{},"runtimeWorkspaceRoots":[state.path()]}),
        )
        .await;
    assert_eq!(resumed["model"], "fable");
    assert_eq!(resumed["reasoningEffort"], "max");
    let rejected = client
        .request(
            "thread/start",
            json!({"approvalsReviewer":"not-a-reviewer"}),
        )
        .await;
    assert_eq!(rejected["error"]["code"], -32602);
    assert_eq!(
        client
            .request("thread/start", json!({"experimentalRawEvents":true}))
            .await["error"]["code"],
        -32602
    );
    client.close().await;
}

#[tokio::test]
async fn installed_desktop_turn_builder_payload_submits_first_and_followup_turns() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let fixture: Value = serde_json::from_str(include_str!("fixtures/desktop-turn.json")).unwrap();
    for summary in ["detailed", "none", "auto", "concise"] {
        let mut request = fixture["request"].clone();
        request["threadId"] = json!(thread);
        request["cwd"] = json!(state.path());
        request["runtimeWorkspaceRoots"] = json!([state.path()]);
        request["summary"] = json!(summary);
        let turn = client.ok("turn/start", request).await;
        let id = turn["turn"]["id"].as_str().unwrap();
        assert_eq!(client.completed(id).await["status"], "completed");
        assert_echo_lifecycle(&client, id, "Echo: desktop payload");
    }
    client.close().await;
}

#[tokio::test]
async fn desktop_dynamic_tools_roundtrip_and_survive_restart() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.ok("thread/start", json!({"sandbox":"danger-full-access","personality":"friendly",
        "dynamicTools":[{"type":"namespace","name":"desktop","description":"Desktop tools","tools":[
            {"type":"function","name":"echo","description":"Echo a value","inputSchema":{
                "type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}}
        ]}]})).await["thread"]["id"].as_str().unwrap().to_owned();
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client
        .ok(
            "thread/resume",
            json!({"threadId":thread,"personality":"none"}),
        )
        .await;
    for reply in [
        json!({"result":{"success":true,"contentItems":[{"type":"inputText","text":"desktop result"}]}}),
        json!({"result":{"success":false,"contentItems":[{"type":"inputText","text":"declined"}]}}),
        json!({"error":{"code":-32603,"message":"client unavailable"}}),
        json!({"result":{"success":true,"contentItems":[{"type":"bogus"}]}}),
    ] {
        let turn = client.start(&thread, "desktop-tool").await;
        let call = client
            .until(|v| v["method"] == "item/tool/call" && v["params"]["turnId"] == turn)
            .await;
        assert_eq!(call["params"]["namespace"], "desktop");
        assert_eq!(call["params"]["tool"], "echo");
        assert_eq!(call["params"]["arguments"], json!({"value":"from Claude"}));
        let mut response = reply.clone();
        response["id"] = call["id"].clone();
        client.send(response).await;
        let completed = client.completed(&turn).await;
        let items = completed["items"].as_array().unwrap();
        let dynamic: Vec<_> = items
            .iter()
            .filter(|i| i["type"] == "dynamicToolCall")
            .collect();
        assert_eq!(dynamic.len(), 1);
        assert_eq!(dynamic[0]["id"], call["params"]["callId"]);
        let success = reply["result"]["success"] == true
            && reply["result"]["contentItems"][0]["type"] == "inputText";
        assert_eq!(dynamic[0]["success"], success);
        assert_eq!(
            dynamic[0]["status"],
            if success { "completed" } else { "failed" }
        );
        let text = items.iter().find(|i| i["type"] == "agentMessage").unwrap()["text"]
            .as_str()
            .unwrap();
        let returned: Value = serde_json::from_str(text).unwrap();
        assert_eq!(returned["result"]["isError"], !success);
        assert_eq!(returned["result"]["content"][0]["type"], "text");
    }
    for prompt in ["desktop-invalid", "desktop-unknown"] {
        let turn = client.start(&thread, prompt).await;
        client.completed(&turn).await;
        assert!(
            !client
                .history
                .iter()
                .any(|v| v["method"] == "item/tool/call" && v["params"]["turnId"] == turn)
        );
    }
    let turn = client.start(&thread, "desktop-tool").await;
    let call = client
        .until(|v| v["method"] == "item/tool/call" && v["params"]["turnId"] == turn)
        .await;
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "interrupted");
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["id"] == call["params"]["callId"] && i["status"] == "failed")
    );
    client.close().await;
}

#[tokio::test]
async fn text_turn_has_exactly_one_complete_lifecycle() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client
        .ok("thread/start", json!({"historyMode":"legacy"}))
        .await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "hello").await;
    let completed = client.completed(&turn).await;
    assert!(completed["error"].is_null());
    assert_echo_lifecycle(&client, &turn, "Echo: hello");
    let read = client
        .ok(
            "thread/read",
            json!({"threadId":thread,"includeTurns":true}),
        )
        .await;
    assert_eq!(read["thread"]["turns"], json!([completed]));
    let turns = client
        .ok(
            "thread/turns/list",
            json!({"threadId":thread,"itemsView":"full"}),
        )
        .await;
    assert_eq!(turns["data"].as_array().unwrap().len(), 1);
    let items = client
        .ok(
            "thread/items/list",
            json!({"threadId":thread,"turnId":turn}),
        )
        .await;
    assert_eq!(items["data"].as_array().unwrap().len(), 2);
    client.close().await;
}

#[tokio::test]
async fn steering_waits_for_consumed_inputs_and_handles_backend_startup() {
    for during_startup in [false, true] {
        let state = tempfile::tempdir().unwrap();
        let args = if during_startup {
            vec!["--claude-arg=--fake-init-delay"]
        } else {
            vec![]
        };
        let mut client = Client::spawn_with_args(state.path(), &args).await;
        client.initialize().await;
        let thread = client.thread().await;
        let turn = client.start(&thread, "steer-wait").await;
        if !during_startup {
            client
                .until(|v| {
                    v["method"] == "item/completed"
                        && v["params"]["item"]["text"] == "Waiting for steering"
                })
                .await;
        }
        let mismatch = client.request("turn/steer", json!({"threadId":thread,"expectedTurnId":"wrong", "input":[{"type":"text","text":"ignored"}]})).await;
        assert!(
            mismatch["error"]["message"]
                .as_str()
                .unwrap()
                .contains("expectedTurnId")
        );
        for (index, text) in ["one", "two"].into_iter().enumerate() {
            let result = client.ok("turn/steer", json!({"threadId":thread,"expectedTurnId":turn,
                "clientUserMessageId":format!("steer-{index}"),"input":[{"type":"text","text":text}]})).await;
            assert_eq!(result["turnId"], turn);
            assert!(
                !client
                    .history
                    .iter()
                    .any(|v| v["method"] == "turn/completed" && v["params"]["turn"]["id"] == turn)
            );
        }
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "completed");
        let items = completed["items"].as_array().unwrap();
        assert!(
            items
                .iter()
                .any(|item| item["text"] == "Steered: one + two")
        );
        for index in 0..2 {
            assert_eq!(
                items
                    .iter()
                    .filter(|item| item["type"] == "userMessage"
                        && item["clientId"] == format!("steer-{index}"))
                    .count(),
                1
            );
        }
        assert_eq!(
            client
                .history
                .iter()
                .filter(|v| v["method"] == "turn/completed" && v["params"]["turn"]["id"] == turn)
                .count(),
            1
        );
        let inactive = client.request("turn/steer", json!({"threadId":thread,"expectedTurnId":turn,"input":[{"type":"text","text":"late"}]})).await;
        assert!(inactive.get("error").is_some());
        client.close().await;
    }
}

#[tokio::test]
async fn queue_mutations_persist_and_start_consumes_only_on_success() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let mut entries = Vec::new();
    for index in 0..3 {
        let params = json!({"threadId":thread,"clientUserMessageId":format!("queued-{index}"),
            "input":[{"type":"text","text":format!("message-{index}")} ]});
        let entry = client.ok("thread/queue/add", params.clone()).await["queuedSubmission"].clone();
        assert_eq!(
            client.ok("thread/queue/add", params).await["queuedSubmission"],
            entry
        );
        entries.push(entry);
    }
    client
        .ok(
            "thread/queue/update",
            json!({"threadId":thread,"queuedSubmissionId":entries[1]["id"],
        "input":[{"type":"text","text":"updated"}]}),
        )
        .await;
    let invalid = client
        .request(
            "thread/queue/reorder",
            json!({"threadId":thread,
        "queuedSubmissionIds":[entries[1]["id"], entries[1]["id"], entries[0]["id"]]}),
        )
        .await;
    assert!(invalid.get("error").is_some());
    client
        .ok(
            "thread/queue/reorder",
            json!({"threadId":thread,
        "queuedSubmissionIds":[entries[1]["id"], entries[2]["id"], entries[0]["id"]]}),
        )
        .await;
    assert_eq!(
        client
            .ok(
                "thread/queue/delete",
                json!({"threadId":thread,"queuedSubmissionId":entries[2]["id"]})
            )
            .await["deleted"],
        true
    );
    assert_eq!(
        client
            .ok(
                "thread/queue/delete",
                json!({"threadId":thread,"queuedSubmissionId":entries[2]["id"]})
            )
            .await["deleted"],
        false
    );
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let page = client
        .ok("thread/queue/list", json!({"threadId":thread,"limit":1}))
        .await;
    assert_eq!(page["data"][0]["id"], entries[1]["id"]);
    assert_eq!(page["data"][0]["input"][0]["text"], "updated");
    assert_eq!(
        client
            .ok(
                "thread/queue/list",
                json!({"threadId":thread,"limit":1,"cursor":page["nextCursor"]})
            )
            .await["data"][0]["id"],
        entries[0]["id"]
    );
    let active = client.start(&thread, "hang").await;
    assert!(
        client
            .request("thread/queue/start", json!({"threadId":thread}))
            .await
            .get("error")
            .is_some()
    );
    assert_eq!(
        client
            .ok("thread/queue/list", json!({"threadId":thread}))
            .await["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":active}))
        .await;
    client.completed(&active).await;
    let turn = client
        .ok("thread/queue/start", json!({"threadId":thread}))
        .await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let completed = client.completed(&turn).await;
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["clientId"] == "queued-1")
    );
    assert_echo_lifecycle(&client, &turn, "Echo: updated");
    assert_eq!(
        client
            .ok("thread/queue/list", json!({"threadId":thread}))
            .await["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        client
            .history
            .iter()
            .any(|v| v["method"] == "thread/queue/changed")
    );
    let turn = client
        .ok(
            "thread/queue/start",
            json!({"threadId":thread,"queuedSubmissionId":entries[0]["id"]}),
        )
        .await["turn"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    client.completed(&turn).await;
    assert!(
        client
            .request("thread/queue/start", json!({"threadId":thread}))
            .await
            .get("error")
            .is_some()
    );
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    assert_eq!(
        client
            .ok("thread/queue/list", json!({"threadId":thread}))
            .await["data"],
        json!([])
    );
    client.close().await;
}

#[tokio::test]
async fn user_questions_remain_available_when_permission_prompts_are_disabled() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client
        .ok(
            "thread/start",
            json!({"sandbox":"danger-full-access","approvalPolicy":"never"}),
        )
        .await["thread"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let turn = client.start(&thread, "question").await;
    let question = client
        .until(|v| v["method"] == "item/tool/requestUserInput")
        .await;
    client
        .send(json!({"id":question["id"],"result":{"answers":{"q0":{"answers":["Two"]}}}}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "completed");
    let text = completed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "agentMessage")
        .unwrap()["text"]
        .as_str()
        .unwrap();
    let permission: Value = serde_json::from_str(text).unwrap();
    assert_eq!(permission["behavior"], "allow");
    assert_eq!(permission["updatedInput"]["answers"]["Which value?"], "Two");
    client.close().await;
}

#[tokio::test]
async fn desktop_tool_execution_can_outlive_the_permission_timeout() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.ok("thread/start", json!({"sandbox":"danger-full-access","dynamicTools":[
        {"type":"namespace","name":"desktop","description":"Desktop tools","tools":[{"type":"function","name":"echo","description":"Echo","inputSchema":{"type":"object"}}]}]})).await["thread"]["id"].as_str().unwrap().to_owned();
    let turn = client.start(&thread, "desktop-tool").await;
    let call = client.until(|v| v["method"] == "item/tool/call").await;
    tokio::time::sleep(Duration::from_millis(5200)).await;
    client.send(json!({"id":call["id"],"result":{"success":true,"contentItems":[{"type":"inputText","text":"late result"}]}})).await;
    let completed = client.completed(&turn).await;
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == call["params"]["callId"] && item["success"] == true)
    );
    client.close().await;
}

#[tokio::test]
async fn tool_permissions_round_trip_allow_and_deny() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    for (decision, expected, status) in [
        ("accept", "fake tool output", "completed"),
        ("decline", "Tool denied", "failed"),
    ] {
        let turn = client.start(&thread, "tool").await;
        let approval = client
            .until(|value| {
                value["method"] == "item/commandExecution/requestApproval"
                    && value["params"]["turnId"] == turn
            })
            .await;
        assert_eq!(approval["params"]["threadId"], thread);
        assert_eq!(approval["params"]["command"], "printf fake");
        assert!(
            !client
                .history
                .iter()
                .any(|value| value["method"] == "turn/completed"
                    && value["params"]["turn"]["id"] == turn)
        );
        client
            .send(json!({"id":approval["id"],"result":{"decision":decision}}))
            .await;
        let completed = client.completed(&turn).await;
        let command = completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "commandExecution")
            .unwrap();
        assert_eq!(command["status"], status);
        assert_eq!(command["aggregatedOutput"], expected);
        assert_echo_lifecycle(&client, &turn, expected);
        assert!(
            client
                .history
                .iter()
                .any(|value| value["method"] == "serverRequest/resolved"
                    && value["params"]["requestId"] == approval["id"])
        );
    }
    client.close().await;
}

#[tokio::test]
async fn interruption_crash_and_malformed_backend_are_terminal() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let hanging = client.start(&thread, "hang").await;
    client
        .ok(
            "turn/interrupt",
            json!({"threadId":thread,"turnId":hanging}),
        )
        .await;
    assert_eq!(client.completed(&hanging).await["status"], "interrupted");
    for (text, diagnostic) in [
        ("crash", "fake backend crashed deliberately"),
        ("malformed", "malformed Claude JSON frame"),
    ] {
        let turn = client.start(&thread, text).await;
        let completed = client.completed(&turn).await;
        assert_eq!(completed["status"], "failed");
        assert!(
            completed["error"]["message"]
                .as_str()
                .unwrap()
                .contains(diagnostic),
            "{completed}"
        );
        assert!(client.history.iter().any(|value| value["method"] == "error"
            && value["params"]["turnId"] == turn
            && value["params"]["willRetry"] == false));
    }
    let recovery = client.start(&thread, "recovered").await;
    client.completed(&recovery).await;
    assert_echo_lifecycle(&client, &recovery, "Echo: recovered");
    client.close().await;
}

#[tokio::test]
async fn persistence_pagination_archive_and_fork_metadata() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let source = client.thread().await;
    let turn = client.start(&source, "persist me").await;
    let terminal = client.completed(&turn).await;
    client
        .ok(
            "thread/name/set",
            json!({"threadId":source,"name":"Saved source"}),
        )
        .await;
    let second = client.thread().await;
    let first_page = client
        .ok("thread/list", json!({"limit":1,"sortDirection":"asc"}))
        .await;
    assert_eq!(first_page["data"].as_array().unwrap().len(), 1);
    assert!(first_page["nextCursor"].is_string());
    let second_page = client
        .ok(
            "thread/list",
            json!({"limit":1,"sortDirection":"asc","cursor":first_page["nextCursor"]}),
        )
        .await;
    let listed: HashSet<_> = [
        first_page["data"][0]["id"].as_str().unwrap(),
        second_page["data"][0]["id"].as_str().unwrap(),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        listed,
        [source.as_str(), second.as_str()].into_iter().collect()
    );
    assert!(second_page["nextCursor"].is_null());
    assert_eq!(
        client
            .request(
                "thread/loaded/list",
                json!({"cursor":first_page["nextCursor"]})
            )
            .await["error"]["code"],
        -32602
    );
    client.close().await;

    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let read = client
        .ok(
            "thread/read",
            json!({"threadId":source,"includeTurns":true}),
        )
        .await;
    assert_eq!(read["thread"]["name"], "Saved source");
    assert_eq!(read["thread"]["turns"], json!([terminal]));
    assert_eq!(read["thread"]["status"]["type"], "notLoaded");
    assert_eq!(
        client.ok("thread/loaded/list", json!({})).await["data"],
        json!([])
    );
    let resumed = client.ok("thread/resume", json!({"threadId":source})).await;
    assert_eq!(resumed["thread"]["turns"].as_array().unwrap().len(), 1);
    let next = client.start(&source, "after restart").await;
    client.completed(&next).await;
    let fork = client.ok("thread/fork", json!({"threadId":source})).await;
    let fork_id = fork["thread"]["id"].as_str().unwrap().to_owned();
    assert_ne!(fork_id, source);
    assert_eq!(fork["thread"]["forkedFromId"], source);
    assert_eq!(fork["thread"]["sessionId"], resumed["thread"]["sessionId"]);
    assert_eq!(fork["thread"]["turns"].as_array().unwrap().len(), 2);
    let source_record: Value = serde_json::from_slice(
        &std::fs::read(state.path().join("threads").join(format!("{source}.json"))).unwrap(),
    )
    .unwrap();
    let advanced = client.start(&source, "source advanced after fork").await;
    client.completed(&advanced).await;
    let fork_turn = client.start(&fork_id, "fork-info").await;
    let fork_result = client.completed(&fork_turn).await;
    let backend_args: Value = serde_json::from_str(
        fork_result["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "agentMessage")
            .unwrap()["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(backend_args["fork"], true);
    assert_eq!(backend_args["resume"], source_record["session_id"]);
    assert_eq!(
        backend_args["resumeAt"],
        source_record["backend_message_id"]
    );
    assert_eq!(
        client
            .ok(
                "thread/read",
                json!({"threadId":source,"includeTurns":true})
            )
            .await["thread"]["turns"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    client
        .ok("thread/archive", json!({"threadId":source}))
        .await;
    let archived = client.ok("thread/list", json!({"archived":true})).await;
    assert_eq!(archived["data"].as_array().unwrap().len(), 1);
    assert_eq!(archived["data"][0]["id"], source);
    assert_eq!(
        client
            .request("thread/resume", json!({"threadId":source}))
            .await["error"]["code"],
        -32602
    );
    client
        .ok("thread/unarchive", json!({"threadId":source}))
        .await;
    assert_eq!(
        client.ok("thread/list", json!({"archived":true})).await["data"],
        json!([])
    );
    client.close().await;
}

#[tokio::test]
async fn independent_threads_progress_while_another_turn_waits() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let blocked = client.thread().await;
    let fast = client.thread().await;
    let waiting = client.start(&blocked, "tool").await;
    let approval = client
        .until(|value| {
            value["method"] == "item/commandExecution/requestApproval"
                && value["params"]["turnId"] == waiting
        })
        .await;
    let quick = client.start(&fast, "concurrent").await;
    client.completed(&quick).await;
    assert_echo_lifecycle(&client, &quick, "Echo: concurrent");
    assert!(!client.history.iter().any(
        |value| value["method"] == "turn/completed" && value["params"]["turn"]["id"] == waiting
    ));
    client
        .send(json!({"id":approval["id"],"result":{"decision":"accept"}}))
        .await;
    client.completed(&waiting).await;
    client.close().await;
}

#[tokio::test]
async fn unsupported_sandbox_options_and_invalid_inputs_are_rejected() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    for params in [
        json!({"sandbox":"read-only"}),
        json!({"sandbox":"external-sandbox"}),
        json!({"approvalPolicy":"untrusted"}),
        json!({"modelProvider":"openai"}),
        json!({"serviceTier":"fast"}),
        json!({"config":{"model":"other"}}),
    ] {
        let error = client.request("thread/start", params).await;
        assert_eq!(error["error"]["code"], -32602, "{error}");
    }
    let thread = client.thread().await;
    let invalid = [
        json!({"threadId":thread,"input":[]}),
        json!({"threadId":thread,"input":[{"type":"text","text":"hello","text_elements":[]}],"sandboxPolicy":{"type":"readOnly"}}),
        json!({"threadId":thread,"input":[{"type":"text","text":"hello","text_elements":[]}],"serviceTier":"fast"}),
    ];
    for params in invalid {
        assert_eq!(
            client.request("turn/start", params).await["error"]["code"],
            -32602
        );
    }
    assert_eq!(
        client
            .request("turn/steer", json!({"threadId":thread,"input":[]}))
            .await["error"]["code"],
        -32602
    );
    assert_eq!(
        client
            .ok(
                "thread/read",
                json!({"threadId":thread,"includeTurns":true})
            )
            .await["thread"]["turns"],
        json!([]),
        "rejected requests must not create turns"
    );
    client.close().await;
}

#[tokio::test]
async fn background_agent_continuations_keep_stdin_open_until_idle() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "background").await;
    let approval = client
        .until(|v| v["method"] == "item/commandExecution/requestApproval")
        .await;
    assert!(
        !client
            .history
            .iter()
            .any(|v| v["method"] == "turn/completed")
    );
    client
        .send(json!({"id":approval["id"], "result":{"decision":"accept"}}))
        .await;
    let turn = client.completed(&turn).await;
    assert_eq!(turn["status"], "completed");
    let messages: Vec<&str> = turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["type"] == "agentMessage")
        .filter_map(|i| i["text"].as_str())
        .collect();
    assert_eq!(messages, ["Parent waiting", "Background complete"]);
    client.close().await;
}

#[tokio::test]
async fn interruption_drains_final_transcript_and_next_turn_resumes() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "hang").await;
    client
        .until(|v| v["method"] == "item/agentMessage/delta" && v["params"]["delta"] == "Working")
        .await;
    client
        .ok("turn/interrupt", json!({"threadId":thread,"turnId":turn}))
        .await;
    let completed = client.completed(&turn).await;
    assert_eq!(completed["status"], "interrupted");
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["text"] == "Interrupted")
    );
    let next = client.start(&thread, "resume-info").await;
    let completed = client.completed(&next).await;
    assert!(
        completed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["text"] == "resumed")
    );
    client.close().await;
}

#[tokio::test]
async fn desktop_discovery_reports_only_facade_capabilities() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let config = client
        .ok(
            "config/read",
            json!({"cwd":state.path(),"includeLayers":true}),
        )
        .await;
    assert_eq!(config["config"]["model_provider"], "anthropic");
    assert_eq!(config["config"]["sandbox_mode"], "workspace-write");
    assert_eq!(config["config"]["approval_policy"], "on-request");
    assert_eq!(config["config"]["approvals_reviewer"], "user");
    assert_eq!(
        config["config"]["sandbox_workspace_write"],
        json!({
            "writable_roots": [], "network_access": false,
            "exclude_slash_tmp": false, "exclude_tmpdir_env_var": false
        })
    );
    let started = client.ok("thread/start", json!({"cwd":state.path()})).await;
    assert_eq!(
        started["sandbox"],
        json!({
            "type": "workspaceWrite", "writableRoots": [], "networkAccess": false,
            "excludeSlashTmp": false, "excludeTmpdirEnvVar": false
        })
    );
    assert_eq!(config["layers"], json!([]));
    let requirements = client.ok("configRequirements/read", Value::Null).await;
    assert_eq!(
        requirements["requirements"]["allowedSandboxModes"],
        json!(["workspace-write", "danger-full-access"])
    );
    assert_eq!(
        requirements["requirements"]["allowedApprovalPolicies"],
        json!(["on-request", "never"])
    );
    assert_eq!(
        requirements["requirements"]["allowedApprovalsReviewers"],
        json!(["user", "auto_review", "guardian_subagent"])
    );
    assert_eq!(
        client.ok("collaborationMode/list", json!({})).await["data"],
        json!([])
    );
    let features = client
        .ok("experimentalFeature/list", json!({"limit":100}))
        .await;
    assert_eq!(features["data"], json!([]));
    assert!(features["nextCursor"].is_null());
    client.close().await;
}

#[tokio::test]
async fn desktop_reopen_hydrates_snapshot_without_early_history_fetch() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let created = client
        .ok(
            "thread/start",
            json!({"historyMode":"paginated","sandbox":"danger-full-access"}),
        )
        .await;
    let id = created["thread"]["id"].as_str().unwrap();
    let turn = client.start(id, "persisted history").await;
    client.completed(&turn).await;
    client.close().await;

    // Seed a large valid persisted transcript from real wire-produced items.
    // Reopening, paging, and resuming still run through the public transport.
    let path = created["thread"]["path"].as_str().unwrap();
    let mut record: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let template = record["turns"][0].clone();
    record["turns"] = json!(
        (0..7)
            .map(|index| {
                let mut turn = template.clone();
                turn["id"] = json!(format!("saved-turn-{index}"));
                turn["items"] = json!(
                    (0..105)
                        .map(|item_index| {
                            let mut item = template["items"][1].clone();
                            item["id"] = json!(format!("saved-item-{index}-{item_index}"));
                            item
                        })
                        .collect::<Vec<_>>()
                );
                turn
            })
            .collect::<Vec<_>>()
    );
    std::fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let metadata = client
        .ok("thread/read", json!({"threadId":id,"includeTurns":false}))
        .await;
    let request = json!({"threadId":id,"history":null,"path":metadata["thread"]["path"],
        "model":null,"modelProvider":"anthropic","cwd":metadata["thread"]["cwd"],
        "personality":"friendly","excludeTurns":true,"config":{},
        "initialTurnsPage":{"limit":5,"itemsView":"full","sortDirection":"desc"}});
    let resumed = client.ok("thread/resume", request.clone()).await;
    assert_eq!(resumed["thread"]["turns"], json!([]));
    assert_eq!(
        resumed["initialTurnsPage"]["data"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(resumed["initialTurnsPage"]["data"][0]["id"], "saved-turn-6");
    let older = client
        .ok(
            "thread/turns/list",
            json!({"threadId":id,
        "cursor":resumed["initialTurnsPage"]["nextCursor"], "limit":5,
        "itemsView":"notLoaded", "sortDirection":"desc"}),
        )
        .await;
    assert_eq!(older["data"].as_array().unwrap().len(), 2);
    assert_eq!(older["data"][0]["id"], "saved-turn-1");
    assert_eq!(older["data"][0]["items"], json!([]));
    // A newer turn must not move the inclusive snapshot boundary.
    let new_turn = client.start(id, "newer than resume").await;
    client.completed(&new_turn).await;
    let mut cursor = resumed["turnsBackwardsCursor"].clone();
    assert!(cursor.is_string());
    let mut turn_ids = vec![];
    loop {
        let page = client
            .ok(
                "thread/turns/list",
                json!({"threadId":id,"cursor":cursor,
            "limit":5,"itemsView":"notLoaded","sortDirection":"desc"}),
            )
            .await;
        for turn in page["data"].as_array().unwrap() {
            turn_ids.push(turn["id"].as_str().unwrap().to_owned());
            let mut item_cursor = resumed["itemsBackwardsCursor"].clone();
            let mut items = vec![];
            loop {
                let page = client
                    .ok(
                        "thread/items/list",
                        json!({"threadId":id,"turnId":turn["id"],
                    "cursor":item_cursor,"limit":100,"sortDirection":"desc"}),
                    )
                    .await;
                items.extend(
                    page["data"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|item| item["item"]["id"].clone()),
                );
                item_cursor = page["nextCursor"].clone();
                if item_cursor.is_null() {
                    break;
                }
            }
            assert_eq!(items.len(), 105);
            assert_eq!(
                items
                    .iter()
                    .map(Value::to_string)
                    .collect::<HashSet<_>>()
                    .len(),
                105
            );
        }
        cursor = page["nextCursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    assert_eq!(
        turn_ids,
        (0..7)
            .rev()
            .map(|index| format!("saved-turn-{index}"))
            .collect::<Vec<_>>()
    );
    let mut by_path = request.clone();
    by_path["threadId"] = json!("ignored-for-path-resume");
    assert_eq!(
        client.ok("thread/resume", by_path).await["thread"]["id"],
        id
    );
    let mut invalid = request;
    invalid["path"] = json!(state.path().join("not-a-thread.json"));
    assert_eq!(
        client.request("thread/resume", invalid).await["error"]["code"],
        -32602
    );
    client.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn desktop_active_resume_from_second_client_accepts_only_unchanged_settings() {
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    async fn request(
        read: &mut Lines<BufReader<OwnedReadHalf>>,
        write: &mut OwnedWriteHalf,
        id: u64,
        method: &str,
        params: Value,
    ) -> Value {
        write
            .write_all(format!("{}\n", json!({"id":id,"method":method,"params":params})).as_bytes())
            .await
            .unwrap();
        loop {
            let line = tokio::time::timeout(DEADLINE, read.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == id && value.get("method").is_none() {
                return value;
            }
        }
    }
    let state = tempfile::Builder::new()
        .prefix("facade-resume-")
        .tempdir_in("/tmp")
        .unwrap();
    let endpoint = format!("unix-lines://{}", state.path().join("rpc.sock").display());
    let mut child = Command::new(env!("CARGO_BIN_EXE_claude-codex-server"))
        .args([
            "--listen",
            &endpoint,
            "--claude",
            env!("CARGO_BIN_EXE_fake-claude"),
            "--state-dir",
        ])
        .arg(state.path())
        .current_dir(state.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut log = BufReader::new(child.stderr.take().unwrap()).lines();
    assert!(
        tokio::time::timeout(DEADLINE, log.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .starts_with("Listening on")
    );
    let (ar, mut aw) = tokio::net::UnixStream::connect(state.path().join("rpc.sock"))
        .await
        .unwrap()
        .into_split();
    let (br, mut bw) = tokio::net::UnixStream::connect(state.path().join("rpc.sock"))
        .await
        .unwrap()
        .into_split();
    let mut ar = BufReader::new(ar).lines();
    let mut br = BufReader::new(br).lines();
    let initialize = json!({"clientInfo":{"name":"wire-resume","version":"1"}});
    assert!(
        request(&mut ar, &mut aw, 1, "initialize", initialize.clone())
            .await
            .get("error")
            .is_none()
    );
    assert!(
        request(&mut br, &mut bw, 1, "initialize", initialize)
            .await
            .get("error")
            .is_none()
    );
    let created = request(
        &mut ar,
        &mut aw,
        2,
        "thread/start",
        json!({"baseInstructions":"Be precise","developerInstructions":"Be concise","sandbox":"danger-full-access"}),
    )
    .await;
    let id = &created["result"]["thread"]["id"];
    let started = request(
        &mut ar,
        &mut aw,
        3,
        "turn/start",
        json!({"threadId":id,"input":[{"type":"text","text":"hang","text_elements":[]}]}),
    )
    .await;
    assert!(started.get("error").is_none(), "{started}");
    let echo = json!({"threadId":id,"path":created["result"]["thread"]["path"],
        "cwd":created["result"]["cwd"],"modelProvider":"anthropic","excludeTurns":true,
        "baseInstructions":"Be precise","developerInstructions":"Be concise",
        "approvalPolicy":"on-request","sandbox":"danger-full-access"});
    let resumed = request(&mut br, &mut bw, 2, "thread/resume", echo.clone()).await;
    assert!(resumed.get("error").is_none(), "{resumed}");
    validate("ThreadResumeResponse", &resumed["result"]);
    for (index, (field, value)) in [
        ("model", json!("different-model")),
        ("developerInstructions", json!("Changed")),
        ("path", json!("/not/the/active/path")),
    ]
    .into_iter()
    .enumerate()
    {
        let mut changed = echo.clone();
        changed[field] = value;
        assert_eq!(
            request(&mut br, &mut bw, 3 + index as u64, "thread/resume", changed).await["error"]["code"],
            -32602
        );
    }
    let interrupted = request(
        &mut br,
        &mut bw,
        6,
        "turn/interrupt",
        json!({"threadId":id,"turnId":started["result"]["turn"]["id"]}),
    )
    .await;
    assert!(interrupted.get("error").is_none());
    // Receiving terminal notifications proves the second client subscribed.
    loop {
        let line = tokio::time::timeout(DEADLINE, br.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_str(&line).unwrap();
        if value["method"] == "turn/completed" {
            break;
        }
    }
    let idle_resume = request(&mut br, &mut bw, 7, "thread/resume", echo.clone()).await;
    assert!(idle_resume.get("error").is_none(), "{idle_resume}");
    let mut changed = echo;
    changed["developerInstructions"] = json!("Different after session creation");
    changed["baseInstructions"] = json!("Updated base instructions");
    let updated = request(&mut br, &mut bw, 8, "thread/resume", changed).await;
    assert!(updated.get("error").is_none(), "{updated}");
    validate("ThreadResumeResponse", &updated["result"]);
    child.kill().await.unwrap();
}

#[tokio::test]
async fn desktop_host_services_and_timeline_are_routed_with_schema_valid_results() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let path = state.path().join("binary.dat");
    let copy = state.path().join("copied.dat");
    client
        .ok("fs/writeFile", json!({"path":path,"dataBase64":"AP9BQgo="}))
        .await;
    assert_eq!(
        client.ok("fs/readFile", json!({"path":path})).await["dataBase64"],
        "AP9BQgo="
    );
    client
        .ok("fs/copy", json!({"sourcePath":path,"destinationPath":copy}))
        .await;
    assert_eq!(std::fs::read(&copy).unwrap(), b"\0\xffAB\n");
    client.ok("fs/remove", json!({"path":copy})).await;
    assert!(!copy.exists());
    let result = client
        .ok(
            "command/exec",
            json!({"command":["/bin/sh","-c","printf routed"],"cwd":state.path()}),
        )
        .await;
    assert_eq!(result["stdout"], "routed");
    assert_eq!(result["exitCode"], 0);
    let thread = client.thread().await;
    let turn = client.start(&thread, "hello").await;
    client.completed(&turn).await;
    let page = client
        .ok(
            "thread/timeline/list",
            json!({"threadId":thread,"limit":100}),
        )
        .await;
    let entries = page["data"].as_array().unwrap();
    assert_eq!(entries.first().unwrap()["type"], "turnStarted");
    assert_eq!(entries.last().unwrap()["type"], "turnCompleted");
    assert!(entries.iter().all(|entry| entry["turnId"] == turn));
    client.close().await;
}

#[tokio::test]
async fn workspace_attachment_metadata_and_environment_survive_restart() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let started = client.ok("thread/start",json!({"sandbox":"danger-full-access",
        "config":{"shell_environment_policy.inherit":"all","shell_environment_policy.set":{"FACADE_WORKTREE":"yes"}},
        "disabledPluginIds":["sample@local"]})).await;
    let id = started["thread"]["id"].as_str().unwrap().to_owned();
    assert_eq!(started["disabledPluginIds"], json!(["sample@local"]));
    let added = client.ok("thread/attachment/add",json!({"threadId":id,"attachmentType":"archived_worktree",
        "identityKey":"/tmp/worktree-metadata","payload":{"worktree":{"snapshot":"preserved"},"pullRequests":[]}})).await;
    assert_eq!(added["outcome"], "created");
    client
        .until(|v| v["method"] == "thread/attachment/updated")
        .await;
    let descendants = client
        .ok(
            "thread/list",
            json!({"ancestorThreadId":id,"sourceKinds":["subAgentThreadSpawn"]}),
        )
        .await;
    assert!(descendants["data"].as_array().unwrap().is_empty());
    client.close().await;
    let persisted: Value = serde_json::from_slice(
        &std::fs::read(state.path().join("threads").join(format!("{id}.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(
        persisted["settings"]["environment_overrides"]["FACADE_WORKTREE"],
        "yes"
    );
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let resumed = client.ok("thread/resume", json!({"threadId":id})).await;
    assert_eq!(resumed["disabledPluginIds"], json!(["sample@local"]));
    let listed = client
        .ok("thread/attachment/list", json!({"threadId":id}))
        .await;
    assert_eq!(
        listed["data"][0]["payload"]["worktree"]["snapshot"],
        "preserved"
    );
    client.ok("thread/attachment/remove",json!({"threadId":id,"attachmentType":"archived_worktree","identityKey":"/tmp/worktree-metadata"})).await;
    client.close().await;
}

#[tokio::test]
async fn context_usage_is_latest_request_and_replays_after_restart() {
    let state = tempfile::tempdir().unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    let thread = client.thread().await;
    let turn = client.start(&thread, "usage-probe").await;
    client.completed(&turn).await;
    let usage = client
        .history
        .iter()
        .rev()
        .find(|m| m["method"] == "thread/tokenUsage/updated")
        .unwrap()["params"]["tokenUsage"]
        .clone();
    assert_eq!(usage["last"]["totalTokens"], 7);
    assert_eq!(usage["total"]["totalTokens"], 70);
    assert_eq!(usage["modelContextWindow"], 200000);
    client.close().await;
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let replay = client
        .until(|m| m["method"] == "thread/tokenUsage/updated")
        .await;
    assert_eq!(replay["params"]["tokenUsage"], usage);
    client.close().await;
    let path = state.path().join("threads").join(format!("{thread}.json"));
    let mut legacy: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("tracks_request_usage");
    legacy["token_usage"]["last"] = legacy["token_usage"]["total"].clone();
    std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let mut client = Client::spawn(state.path()).await;
    client.initialize().await;
    client.ok("thread/resume", json!({"threadId":thread})).await;
    let replay = client
        .until(|m| m["method"] == "thread/tokenUsage/updated")
        .await;
    assert!(replay["params"]["tokenUsage"]["modelContextWindow"].is_null());
    assert_eq!(replay["params"]["tokenUsage"]["total"], usage["total"]);
    client.close().await;
}
