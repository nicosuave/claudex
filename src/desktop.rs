//! Translate desktop request options into the subset implemented by Claude.
use serde_json::{Value, json};

use crate::protocol::{RpcError, RpcResult};

pub fn normalize(method: &str, params: &Value) -> RpcResult<(Value, Vec<String>)> {
    if !matches!(
        method,
        "thread/start" | "thread/resume" | "thread/fork" | "turn/start"
    ) {
        return Ok((params.clone(), vec![]));
    }
    let mut p = params.clone();
    let object = p
        .as_object_mut()
        .ok_or_else(|| RpcError::invalid("params must be an object"))?;
    let mut warnings = Vec::new();
    if let Some(profile) = object
        .remove("permissions")
        .filter(|value| !value.is_null())
    {
        let sandbox = match profile.as_str() {
            Some(":workspace") => "workspace-write",
            Some(":danger-full-access") => "danger-full-access",
            _ => {
                return Err(RpcError::invalid(
                    "Only :workspace and :danger-full-access permission profiles are supported",
                ));
            }
        };
        if object.get("sandbox").is_some_and(|value| !value.is_null())
            || object
                .get("sandboxPolicy")
                .is_some_and(|value| !value.is_null())
        {
            return Err(RpcError::invalid(
                "Select either permissions or a sandbox policy, not both",
            ));
        }
        if method == "turn/start" {
            object.insert("sandboxPolicy".into(), json!({"type": if sandbox == "workspace-write" { "workspaceWrite" } else { "dangerFullAccess" }}));
        } else {
            object.insert("sandbox".into(), json!(sandbox));
        }
    }
    if object
        .get("approvalsReviewer")
        .is_some_and(|reviewer| reviewer == "auto_review" || reviewer == "guardian_subagent")
    {
        warnings.push("With on-request approvals, automated review uses Claude Code's native auto permission mode. The never policy still denies permission prompts. A danger-full-access profile remains unrestricted.".into());
    }
    if object.get("sandbox") == Some(&json!("workspace-write"))
        || object
            .get("sandboxPolicy")
            .is_some_and(|p| p["type"] == "workspaceWrite")
    {
        warnings.push("Workspace commands use Claude's native OS sandbox. File tools use native scoped permissions; MCP servers and hooks remain trusted integrations. Existing deny/ask rules and other native settings are preserved; preapproved command/write/network grants and sandbox exclusions are replaced by this workspace's roots and approval policy.".into());
    }
    if let Some(raw) = object
        .remove("experimentalRawEvents")
        .filter(|v| !v.is_null())
        && raw != false
    {
        return Err(RpcError::invalid(
            "Raw Codex events are not available from Claude",
        ));
    }
    // Workspace roots describe desktop context. Explicit sandboxPolicy writable
    // roots select any additional write access.
    if let Some(roots) = object
        .remove("runtimeWorkspaceRoots")
        .filter(|v| !v.is_null())
        && !roots.as_array().is_some_and(|roots| {
            roots.iter().all(|root| {
                root.as_str()
                    .is_some_and(|root| std::path::Path::new(root).is_absolute())
            })
        })
    {
        return Err(RpcError::invalid(
            "runtimeWorkspaceRoots must contain absolute paths",
        ));
    }
    // Client analytics do not affect Claude execution.
    object.remove("turnTrigger");
    object.remove("responsesapiClientMetadata");
    // Deprecated by the pinned protocol: friendly/pragmatic no longer select a style.
    object.remove("personality");
    if object
        .get("summary")
        .and_then(Value::as_str)
        .is_some_and(|s| s == "concise" || s == "detailed")
    {
        warnings.push("Claude returns its native reasoning text; Codex reasoning-summary length preferences are not applied.".into());
    }
    if object
        .get("environments")
        .is_some_and(|v| v.as_array().is_some_and(Vec::is_empty))
    {
        object.remove("environments");
    }
    for field in ["serviceTier", "serviceTierForTurn"] {
        if object.get(field).is_some_and(|v| v == "default") {
            object.remove(field);
        }
    }
    if let Some(config) = object.remove("config").filter(|v| !v.is_null()) {
        let config = config
            .as_object()
            .ok_or_else(|| RpcError::invalid("config must be an object"))?;
        if let Some(overrides) = crate::workspaces::shell_environment_overrides(config)? {
            object.insert("environmentOverrides".into(), json!(overrides));
        }
        let mut unsupported = Vec::new();
        let mut native_features = false;
        for (key, value) in config {
            if value.is_null() {
                continue;
            }
            match key.as_str() {
                key if crate::workspaces::is_shell_environment_key(key) => {}
                "model_reasoning_effort" => {
                    object.entry("effort").or_insert_with(|| value.clone());
                }
                // Desktop unconditionally supplies Codex runtime feature flags.
                // They are not Claude settings. Warn instead of pretending to
                // enable Codex-only execution features on the native backend.
                key if key.starts_with("features.") => native_features = true,
                "apps.connector_openai_pages.tools"
                    if value.as_object().is_some_and(|tools| {
                        tools.values().all(|tool| tool["enabled"] == false)
                    }) => {}
                "plugins.codex-app-tools@openai-bundled.mcp_servers.codex_app.enabled_tools"
                | "mcp_servers.codex_app.enabled_tools"
                    if value.is_array() =>
                {
                    if value.as_array().is_some_and(|tools| !tools.is_empty()) {
                        warnings.push("Codex tool allowlists do not configure Claude MCP servers. Desktop tools supplied in dynamicTools are bridged separately; Claude's native tools and configured MCP servers remain available.".into());
                    }
                }
                _ => unsupported.push(key.as_str()),
            }
        }
        if !unsupported.is_empty() {
            return Err(RpcError::invalid(format!(
                "Unsupported Claude configuration: {}",
                unsupported.join(", ")
            )));
        }
        if native_features {
            warnings.push("Claude uses its native runtime and tools; Codex runtime feature overrides are not applied.".into());
        }
    }
    if let Some(mode) = object.remove("collaborationMode").filter(|v| !v.is_null()) {
        if mode["mode"] != "default" {
            return Err(RpcError::invalid(
                "Only the default collaboration mode is supported by Claude",
            ));
        }
        for (source, target) in [("model", "model"), ("reasoning_effort", "effort")] {
            if !mode["settings"][source].is_null() {
                object.insert(target.into(), mode["settings"][source].clone());
            }
        }
        if let Some(instructions) = mode["settings"]["developer_instructions"].as_str()
            && !instructions.is_empty()
        {
            object.insert("developerInstructions".into(), json!(instructions));
        }
    }
    // Deprecated and ignored by the pinned Codex protocol as well.
    object.remove("multiAgentMode");
    Ok((p, warnings))
}
