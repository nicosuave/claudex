use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::protocol::{self, RpcError, RpcResult, now, now_ms};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    pub cwd: PathBuf,
    pub model: String,
    pub approval_policy: String,
    #[serde(default = "default_approvals_reviewer")]
    pub approvals_reviewer: String,
    #[serde(default)]
    pub sandbox: crate::sandbox::Policy,
    pub base_instructions: Option<String>,
    pub developer_instructions: Option<String>,
    pub effort: Option<String>,
    #[serde(default)]
    pub dynamic_tools: Vec<crate::dynamic_tools::Tool>,
    #[serde(default)]
    pub reasoning_summary: Option<String>,
    #[serde(default)]
    pub environment_overrides: BTreeMap<String, String>,
    #[serde(default)]
    pub disabled_plugin_ids: Vec<String>,
}

impl Settings {
    pub fn new(cwd: PathBuf, model: String) -> Self {
        Self {
            cwd,
            model,
            approval_policy: "on-request".into(),
            approvals_reviewer: default_approvals_reviewer(),
            sandbox: Default::default(),
            base_instructions: None,
            developer_instructions: None,
            effort: None,
            dynamic_tools: vec![],
            reasoning_summary: None,
            environment_overrides: BTreeMap::new(),
            disabled_plugin_ids: vec![],
        }
    }

    pub fn apply(&mut self, p: &Value) -> RpcResult<()> {
        if let Some(reviewer) = p.get("approvalsReviewer").filter(|v| !v.is_null()) {
            self.approvals_reviewer = match reviewer.as_str() {
                Some("user") => "user",
                Some("auto_review" | "guardian_subagent") => "auto_review",
                _ => {
                    return Err(RpcError::invalid(
                        "approvalsReviewer must be user, auto_review, or guardian_subagent",
                    ));
                }
            }
            .into();
        }
        if let Some(value) = p.get("environmentOverrides") {
            self.environment_overrides =
                serde_json::from_value(value.clone()).map_err(RpcError::internal)?;
        }
        if let Some(value) = p.get("disabledPluginIds").filter(|v| !v.is_null()) {
            let ids: Vec<String> =
                serde_json::from_value(value.clone()).map_err(RpcError::internal)?;
            crate::plugins::disabled_settings(&ids)?;
            self.disabled_plugin_ids = ids;
        }
        if let Some(summary) = p["summary"].as_str() {
            self.reasoning_summary = Some(summary.into());
        }
        if let Some(tools) = p.get("dynamicTools").filter(|v| !v.is_null()) {
            self.dynamic_tools = crate::dynamic_tools::parse(tools)?;
        }
        if let Some(cwd) = p["cwd"].as_str() {
            let path = Path::new(cwd);
            if !path.is_absolute() || !path.is_dir() {
                return Err(RpcError::invalid(
                    "cwd must be an existing absolute directory",
                ));
            }
            self.cwd = path.canonicalize().map_err(RpcError::internal)?;
        }
        if let Some(model) = p["model"].as_str() {
            if model.trim().is_empty() || model.starts_with('-') {
                return Err(RpcError::invalid(
                    "model must be a nonempty Claude model name or alias",
                ));
            }
            self.model = model.to_owned();
        }
        if let Some(provider) = p["modelProvider"].as_str()
            && provider != "anthropic"
        {
            return Err(RpcError::invalid(
                "Only modelProvider=anthropic is supported",
            ));
        }
        if let Some(policy) = p.get("approvalPolicy").filter(|p| !p.is_null()) {
            let Some(policy @ ("on-request" | "never")) = policy.as_str() else {
                return Err(RpcError::invalid(
                    "Claude supports on-request and never approval policies; untrusted and granular policies cannot be reproduced",
                ));
            };
            self.approval_policy = policy.to_owned();
        }
        if let Some(sandbox) = p.get("sandbox").filter(|v| !v.is_null()) {
            self.sandbox = match sandbox.as_str() {
                Some("danger-full-access") => crate::sandbox::Policy::DangerFullAccess,
                Some("workspace-write") => crate::sandbox::Policy::workspace(),
                _ => return Err(sandbox_error()),
            };
        }
        if let Some(sandbox) = p.get("sandboxPolicy").filter(|v| !v.is_null()) {
            self.sandbox = serde_json::from_value(sandbox.clone()).map_err(|_| sandbox_error())?;
        }
        self.sandbox
            .validate()
            .map_err(|error| RpcError::invalid(error.to_string()))?;
        if let Some(effort) = p["effort"].as_str() {
            if !["none", "low", "medium", "high", "xhigh", "max"].contains(&effort) {
                return Err(RpcError::invalid(
                    "Claude effort must be low, medium, high, xhigh, or max",
                ));
            }
            self.effort = (effort != "none").then(|| effort.to_owned());
        }
        if let Some(instructions) = p["baseInstructions"].as_str() {
            self.base_instructions = Some(instructions.to_owned());
        }
        if let Some(instructions) = p["developerInstructions"].as_str() {
            self.developer_instructions = Some(instructions.to_owned());
        }
        Ok(())
    }

    /// Never suppress native classifier decisions with bypassPermissions. With
    /// approvalPolicy=never, native dontAsk retains the existing deny-on-prompt contract.
    pub fn native_permission_mode(&self) -> &'static str {
        if self.approval_policy == "never" {
            "dontAsk"
        } else if self.approvals_reviewer == "auto_review" {
            "auto"
        } else if self.sandbox.is_workspace() {
            "acceptEdits"
        } else {
            "manual"
        }
    }
}

fn default_approvals_reviewer() -> String {
    "user".into()
}

fn sandbox_error() -> RpcError {
    RpcError::invalid(
        "Supported sandbox profiles are danger-full-access and workspace-write. Workspace commands use Claude's native OS sandbox; read-only and external sandbox profiles are unsupported.",
    )
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    pub format_version: u32,
    pub thread: Value,
    pub settings: Settings,
    pub session_id: String,
    pub has_session: bool,
    pub fork_from: Option<String>,
    #[serde(default)]
    pub backend_message_id: Option<String>,
    #[serde(default)]
    pub turn_anchors: BTreeMap<String, crate::history_recovery::NativeAnchor>,
    #[serde(default)]
    pub tracks_turn_anchors: bool,
    #[serde(default)]
    pub attachments: Vec<crate::attachments::Attachment>,
    #[serde(default)]
    pub session_grants: Vec<crate::approvals::SessionGrant>,
    pub archived: bool,
    pub turns: Vec<Value>,
    #[serde(default)]
    pub queued_submissions: Vec<Value>,
    pub item_times: BTreeMap<String, (u64, Option<u64>)>,
    pub token_usage: Value,
    #[serde(default)]
    pub tracks_request_usage: bool,
}

impl Record {
    pub fn new(settings: Settings, path: PathBuf, p: &Value, originator: &str) -> Self {
        let thread_id = protocol::id();
        let ephemeral = p["ephemeral"].as_bool().unwrap_or(false);
        let timestamp = now();
        let thread = json!({
            "id": thread_id, "sessionId": thread_id, "forkedFromId": null, "parentThreadId": null,
            "environments": null, "extra": null, "preview": "", "ephemeral": ephemeral,
            "section": null, "sectionEnteredAt": null, "projectId": p["projectId"],
            "historyMode": p["historyMode"].as_str().unwrap_or("legacy"),
            "modelProvider": "anthropic", "model": settings.model, "reasoningEffort": settings.effort,
            "createdAt": timestamp, "updatedAt": timestamp, "recencyAt": timestamp, "status": {"type": "idle"},
            "path": if ephemeral { None } else { Some(path.join(format!("{thread_id}.json"))) },
            "cwd": settings.cwd, "cliVersion": protocol::VERSION, "originator": originator,
            "source": "appServer", "canAcceptDirectInput": true, "threadSource": p["threadSource"],
            "agentNickname": null, "agentRole": null, "gitInfo": null, "name": null,
            "daybreakEnabled": null, "turns": []
        });
        Self {
            format_version: 1,
            thread,
            settings,
            session_id: uuid::Uuid::new_v4().to_string(),
            has_session: false,
            fork_from: None,
            backend_message_id: None,
            turn_anchors: BTreeMap::new(),
            tracks_turn_anchors: true,
            attachments: vec![],
            session_grants: vec![],
            archived: false,
            turns: vec![],
            queued_submissions: vec![],
            item_times: BTreeMap::new(),
            token_usage: json!({"total": protocol::usage(0,0,0,0), "last": protocol::usage(0,0,0,0), "modelContextWindow": null}),
            tracks_request_usage: true,
        }
    }
    pub fn id(&self) -> &str {
        self.thread["id"].as_str().unwrap()
    }
    pub fn recover_history(
        &mut self,
        boundary: &crate::history_recovery::HistoryBoundary,
    ) -> RpcResult<()> {
        use crate::history_recovery::{NativeAnchor, plan_prefix};
        if self.ephemeral() {
            return Err(RpcError::invalid(
                "Ephemeral Claude history cannot be recovered",
            ));
        }
        let latest = self
            .backend_message_id
            .as_ref()
            .filter(|_| !self.tracks_turn_anchors)
            .map(|message_id| NativeAnchor {
                session_id: if self.has_session {
                    self.session_id.clone()
                } else {
                    self.fork_from
                        .clone()
                        .unwrap_or_else(|| self.session_id.clone())
                },
                message_id: message_id.clone(),
            });
        let plan = plan_prefix(&self.turns, &self.turn_anchors, latest.as_ref(), boundary)?;
        self.turns.truncate(plan.retained_len);
        self.turn_anchors
            .retain(|id, _| self.turns.iter().any(|turn| turn["id"] == *id));
        self.item_times.retain(|id, _| {
            self.turns.iter().any(|turn| {
                turn["items"]
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| item["id"] == *id))
            })
        });
        self.session_id = uuid::Uuid::new_v4().to_string();
        self.has_session = false;
        self.fork_from = plan.anchor.as_ref().map(|anchor| anchor.session_id.clone());
        self.backend_message_id = plan.anchor.map(|anchor| anchor.message_id);
        self.thread["preview"] = json!(
            self.turns
                .iter()
                .flat_map(|turn| turn["items"].as_array().into_iter().flatten())
                .find(|item| item["type"] == "userMessage")
                .and_then(|item| item["content"].as_array())
                .map(|content| content
                    .iter()
                    .filter_map(|part| part["text"].as_str())
                    .collect::<Vec<_>>()
                    .join(" "))
                .unwrap_or_default()
        );
        // Removed native usage cannot be reconstructed from historical display
        // items. Reset the counter rather than retaining removed-turn totals.
        self.token_usage = json!({"total":protocol::usage(0,0,0,0),"last":protocol::usage(0,0,0,0),"modelContextWindow":null});
        self.touch();
        Ok(())
    }
    pub fn ephemeral(&self) -> bool {
        self.thread["ephemeral"].as_bool().unwrap_or(false)
    }
    pub fn view(&self, include_turns: bool, loaded: bool) -> Value {
        let mut thread = self.thread.clone();
        thread["turns"] = if include_turns {
            json!(self.turns)
        } else {
            json!([])
        };
        if !loaded {
            thread["status"] = json!({"type": "notLoaded"});
            thread["canAcceptDirectInput"] = Value::Null;
        }
        thread
    }
    pub fn touch(&mut self) {
        self.thread["updatedAt"] = json!(now());
        self.thread["recencyAt"] = self.thread["updatedAt"].clone();
        self.thread["model"] = json!(self.settings.model);
        self.thread["reasoningEffort"] = json!(self.settings.effort);
        self.thread["cwd"] = json!(self.settings.cwd);
    }
    pub fn response(&self, include_turns: bool, resume: bool) -> Value {
        let mut response = json!({"thread": self.view(include_turns, true), "model": self.settings.model,
            "modelProvider": "anthropic", "serviceTier": null, "disabledPluginIds": self.settings.disabled_plugin_ids,
            "cwd": self.settings.cwd, "runtimeWorkspaceRoots": [self.settings.cwd], "instructionSources": [],
            "approvalPolicy": self.settings.approval_policy, "approvalsReviewer": self.settings.approvals_reviewer,
            "sandbox": self.settings.sandbox, "activePermissionProfile": null,
            "reasoningEffort": self.settings.effort, "multiAgentMode": "explicitRequestOnly"});
        if resume {
            response["collaborationMode"] = Value::Null;
            response["initialTurnsPage"] = Value::Null;
            response["turnsBackwardsCursor"] = Value::Null;
            response["itemsBackwardsCursor"] = Value::Null;
        }
        response
    }
    pub fn update_item(&mut self, item: Value, completed: bool) {
        let Some(turn) = self.turns.last_mut() else {
            return;
        };
        let id = item["id"].as_str().unwrap().to_owned();
        let times = self
            .item_times
            .entry(id.clone())
            .or_insert((now_ms(), None));
        if completed {
            times.1 = Some(now_ms());
        }
        let items = turn["items"].as_array_mut().unwrap();
        match items.iter_mut().find(|v| v["id"] == id) {
            Some(existing) => *existing = item,
            None => items.push(item),
        }
    }
}

/// A process-exclusive state directory. Each thread is replaced atomically, with private permissions.
pub struct Store {
    root: PathBuf,
    _lock: File,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ModelDefaults {
    pub model: String,
    pub effort: Option<String>,
    pub version: String,
}

impl Store {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let root = root.as_ref().canonicalize()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        }
        let lock = private_open(&root.join("server.lock"), false)?;
        lock.try_lock_exclusive()
            .context("Another facade process is using this state directory")?;
        fs::create_dir_all(root.join("threads"))?;
        Ok(Self { root, _lock: lock })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn threads_dir(&self) -> PathBuf {
        self.root.join("threads")
    }
    pub fn defaults_path(&self) -> PathBuf {
        self.root.join("model-defaults.json")
    }
    pub fn load_defaults(&self, model: String) -> Result<ModelDefaults> {
        match File::open(self.defaults_path()) {
            Ok(file) => Ok(serde_json::from_reader(file)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(ModelDefaults {
                model,
                effort: None,
                version: "0".into(),
            }),
            Err(error) => Err(error.into()),
        }
    }
    pub fn save_defaults(&self, defaults: &ModelDefaults) -> Result<()> {
        self.write_json(&self.defaults_path(), defaults)
    }
    pub fn save(&self, record: &Record) -> Result<()> {
        if record.ephemeral() {
            return Ok(());
        }
        let id = record.id();
        uuid::Uuid::parse_str(id).context("Invalid persisted thread id")?;
        self.write_json(&self.threads_dir().join(format!("{id}.json")), record)
    }
    fn write_json(&self, path: &Path, value: &impl Serialize) -> Result<()> {
        let dir = path.parent().context("State file needs a parent")?;
        let temp = dir.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = private_open(&temp, true)?;
            serde_json::to_writer(&mut file, value)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            fs::rename(&temp, path)?;
            #[cfg(unix)]
            File::open(dir)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
    pub fn load(&self) -> Result<BTreeMap<String, Record>> {
        let mut records = BTreeMap::new();
        for entry in fs::read_dir(self.threads_dir())? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let mut record: Record = serde_json::from_reader(File::open(&path)?)
                .with_context(|| format!("Cannot load {}", path.display()))?;
            if !record.tracks_request_usage {
                // Older records stored an entire turn's billing total as context.
                // Preserve billing; leave occupancy unknown until a native request.
                record.token_usage["last"] = protocol::usage(0, 0, 0, 0);
                record.token_usage["modelContextWindow"] = Value::Null;
            }
            if record.format_version != 1 {
                bail!("Unsupported state format in {}", path.display());
            }
            if path.file_stem().and_then(|v| v.to_str()) != Some(record.id()) {
                bail!("Thread id does not match state filename");
            }
            uuid::Uuid::parse_str(record.id())?;
            let mut recovered = false;
            for turn in &mut record.turns {
                if turn["status"] == "inProgress" {
                    turn["status"] = json!("interrupted");
                    turn["completedAt"] = json!(now());
                    turn["error"] = json!({"message": "Facade stopped while this turn was active", "codexErrorInfo": null, "additionalDetails": null, "misalignment": null});
                    for item in turn["items"].as_array_mut().unwrap() {
                        if item["status"] == "inProgress" {
                            item["status"] = json!("failed");
                        }
                    }
                    recovered = true;
                }
            }
            record.thread["status"] = json!({"type": "idle"});
            if recovered {
                self.save(&record)?;
            }
            records.insert(record.id().to_owned(), record);
        }
        Ok(records)
    }
}

fn private_open(path: &Path, exclusive: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if exclusive {
        options.create_new(true);
    } else {
        options.create(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    Ok(options.open(path)?)
}
