use claude_codex_server::desktop::normalize;
use serde_json::json;

#[test]
fn workspace_overlay_and_disabled_plugin_selection_survive_normalization() {
    for method in ["thread/start", "thread/resume", "thread/fork", "turn/start"] {
        let (params, _) = normalize(
            method,
            &json!({
                "disabledPluginIds":["example@market"],
                "config":{
                    "shell_environment_policy.inherit":"all",
                    "shell_environment_policy.set":{"WORKTREE_LABEL":"task"},
                    "shell_environment_policy.exclude":[],
                    "features.native_feature":true
                }
            }),
        )
        .unwrap();
        assert_eq!(
            params["environmentOverrides"],
            json!({"WORKTREE_LABEL":"task"})
        );
        assert_eq!(params["disabledPluginIds"], json!(["example@market"]));
        assert!(params.get("config").is_none());
        let (params, _) = normalize(
            method,
            &json!({"disabledPluginIds":[], "config":{"shell_environment_policy.set":{}}}),
        )
        .unwrap();
        assert_eq!(params["environmentOverrides"], json!({}));
        assert_eq!(params["disabledPluginIds"], json!([]));
        let (params, _) = normalize(method, &json!({})).unwrap();
        assert!(params.get("environmentOverrides").is_none());
        assert!(params.get("disabledPluginIds").is_none());
    }
}

#[test]
fn incompatible_shell_restrictions_are_not_silently_discarded() {
    for config in [
        json!({"shell_environment_policy.inherit":"none"}),
        json!({"shell_environment_policy.exclude":["SECRET"]}),
        json!({"shell_environment_policy.experimental_use_profile":true}),
    ] {
        let error = normalize("thread/start", &json!({"config":config})).unwrap_err();
        assert!(error.message.contains("shell_environment_policy"));
    }
}

#[test]
fn codex_desktop_runtime_overrides_do_not_block_claude_requests() {
    for method in [
        "thread/start",
        "thread/resume",
        "thread/fork",
        "thread/settings/update",
        "turn/start",
    ] {
        let (params, warnings) = normalize(
            method,
            &json!({
                "approvalPolicy":"on-request", "sandbox":"workspace-write",
                "config":{
                    "mcp_servers.node_repl":{"command":"node", "args":["desktop-repl.js"]},
                    "mcp_servers.artifact_session":{"command":"/desktop/node_repl", "env":{"NODE_REPL_TRUSTED_SERVICES":"{\"artifact_session\":\"@oai/artifact-tool/artifact-session/service\"}"}},
                    "mcp_servers.cua_repl":{"command":"/desktop/node", "args":["/desktop/@oai/cua-repl/bin/cua-repl.mjs"], "enabled":true},
                    "memories.dual_write":true, "memories.version":"v2",
                    "model_reasoning_effort":"high"
                }
            }),
        )
        .unwrap();
        assert!(params.get("config").is_none());
        assert_eq!(params["effort"], "high");
        assert_eq!(params["approvalPolicy"], "on-request");
        assert_eq!(params["sandbox"], "workspace-write");
        assert!(warnings.iter().any(|w| w.contains(
            "desktop MCP overrides (node_repl, artifact_session, cua_repl) are not applied"
        )));
        let (disabled, _) = normalize(
            method,
            &json!({"config":{
                "mcp_servers.artifact_session":{"command":"", "enabled":false},
                "mcp_servers.cua_repl":{"command":"/desktop/electron", "enabled":false}
            }}),
        )
        .unwrap();
        assert!(disabled.get("config").is_none());
    }
    // Keep rejecting unrelated settings instead of dropping arbitrary MCP or
    // memory configuration merely because the desktop supplies some of it.
    for key in ["mcp_servers.other", "memories.unknown"] {
        let error =
            normalize("thread/start", &json!({"config":{key:{"enabled":true}}})).unwrap_err();
        assert!(error.message.contains(key));
    }
}
