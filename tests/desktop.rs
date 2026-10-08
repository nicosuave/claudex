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
