//! Deterministic CLI stand-in. No credentials, network, or Claude calls.
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

fn emit(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}

fn finish(session: &str, text: &str, error: bool) {
    let message_id = format!("msg_{}", uuid::Uuid::new_v4());
    let assistant_id = uuid::Uuid::new_v4().to_string();
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"message_start","message":{"id":message_id,"type":"message","role":"assistant","content":[],"model":"fake-claude","usage":{"input_tokens":4,"output_tokens":0}}}}),
    );
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}}),
    );
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}}),
    );
    emit(
        json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_stop","index":0}}),
    );
    emit(
        json!({"type":"assistant","session_id":session,"uuid":assistant_id,"parent_tool_use_id":null,"message":{"id":message_id,"type":"message","role":"assistant","model":"fake-claude","content":[{"type":"text","text":text}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":3}}}),
    );
    emit(
        json!({"type":"result","subtype":if error {"error_during_execution"} else {"success"},"is_error":error,"result":text,"errors":if error {vec![text]} else {vec![]},"session_id":session,"duration_ms":1,"duration_api_ms":1,"num_turns":1,"total_cost_usd":0.0,"usage":{"input_tokens":if text == "Echo: usage-probe" {40} else {4},"output_tokens":if text == "Echo: usage-probe" {30} else {3}},"modelUsage":{"fake-claude":{"contextWindow":200000},"auxiliary-model":{"contextWindow":1000000}},"permission_denials":[]}),
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).is_some_and(|arg| arg == "plugin") {
        emit(json!({"installed":[],"available":[]}));
        return;
    }
    let session = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--session-id="))
        .or_else(|| args.iter().find_map(|arg| arg.strip_prefix("--resume=")))
        .unwrap_or("fake-session");
    let mut hanging = false;
    let mut background = false;
    let mut prompts = 0;
    let mut steering_inputs: Option<Vec<String>> = None;
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else {
            break;
        };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        match value["type"].as_str() {
            Some("control_request") => {
                let request_id = &value["request_id"];
                match value["request"]["subtype"].as_str() {
                    Some("initialize") => {
                        if args.iter().any(|arg| arg == "--fake-init-delay") {
                            std::thread::sleep(std::time::Duration::from_millis(200));
                        }
                        if args.iter().any(|arg| arg == "--fake-init-crash") {
                            eprintln!("fake initialization failure");
                            std::process::exit(23);
                        }
                        if args.iter().any(|arg| arg == "--fake-init-hang") {
                            continue;
                        }
                        println!("fake diagnostic before initialization");
                        let mut models = vec![
                            json!({"value":"fake-claude","displayName":"Fake Claude","description":"Deterministic test backend"}),
                        ];
                        for (value, display) in [
                            ("opus", "Opus test"),
                            ("fable", "Fable test"),
                            ("sonnet", "Sonnet test"),
                            ("haiku", "Haiku test"),
                        ] {
                            models.push(json!({"value":value,"displayName":display,"description":"Fixture model",
                                "supportedEffortLevels":["low","medium","high","xhigh","max"]}));
                        }
                        emit(
                            json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":{"commands":[],"models":models}}}),
                        );
                        emit(
                            json!({"type":"system","subtype":"init","session_id":session,"cwd":std::env::current_dir().unwrap(),"tools":["Bash"],"model":"fake-claude","permissionMode":"default"}),
                        );
                    }
                    Some("interrupt") => {
                        emit(
                            json!({"type":"control_response","response":{"subtype":"success","request_id":request_id,"response":{}}}),
                        );
                        if hanging {
                            hanging = false;
                            finish(session, "Interrupted", true);
                        }
                    }
                    _ => emit(
                        json!({"type":"control_response","response":{"subtype":"error","request_id":request_id,"error":"unsupported fake control request"}}),
                    ),
                }
            }
            Some("user") => {
                prompts += 1;
                let content = &value["message"]["content"];
                let text = content.as_str().map(str::to_owned).unwrap_or_else(|| {
                    content
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|part| part["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                });
                if let Some(inputs) = &mut steering_inputs {
                    if inputs.is_empty() {
                        finish(
                            session,
                            "First result before queued input is consumed",
                            false,
                        );
                        emit(
                            json!({"type":"system","subtype":"session_state_changed","state":"idle"}),
                        );
                    }
                    inputs.push(text.clone());
                    emit(
                        json!({"type":"user","uuid":value["uuid"],"isReplay":true,"message":value["message"],"session_id":session,"parent_tool_use_id":null}),
                    );
                    if inputs.len() == 2 {
                        finish(session, &format!("Steered: {}", inputs.join(" + ")), false);
                        emit(
                            json!({"type":"system","subtype":"session_state_changed","state":"idle"}),
                        );
                        steering_inputs = None;
                    }
                    continue;
                }
                emit(
                    json!({"type":"user","uuid":value["uuid"],"isReplay":true,"message":value["message"],"session_id":session,"parent_tool_use_id":null}),
                );
                match text.as_str() {
                    "steer-busy" | "steer-closed-input" => {
                        if text == "steer-closed-input" {
                            unsafe { libc::close(0); }
                        }
                        emit(json!({"type":"assistant","session_id":session,"message":{"id":"busy","role":"assistant","content":[{"type":"text","text":"Backend input paused"}]}}));
                        std::thread::sleep(std::time::Duration::from_secs(2));
                    }
                    text if text.starts_with("large-steer:") => finish(session, "Large input consumed", false),
                    "steer-wait" => {
                        steering_inputs = Some(Vec::new());
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"running"}));
                        emit(json!({"type":"assistant","session_id":session,"message":{"id":"waiting","role":"assistant","content":[{"type":"text","text":"Waiting for steering"}]}}));
                    }
                    "question" => {
                        emit(json!({"type":"control_request","request_id":"question_fake","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","tool_use_id":"question_tool","input":{"questions":[{"header":"Choice","question":"Which value?","options":[{"label":"One","description":"First"},{"label":"Two","description":"Second"}],"multiSelect":false}]}}}));
                    }
                    "desktop-tool" | "desktop-invalid" | "desktop-unknown" => {
                        assert!(args.iter().any(|a| a.contains("\"codex_desktop\"")), "missing SDK MCP registration");
                        let name = if text == "desktop-unknown" {"missing"} else {"desktop__echo"};
                        let arguments = if text == "desktop-invalid" {json!({"value":42})} else {json!({"value":"from Claude"})};
                        emit(json!({"type":"control_request","request_id":"desktop_call",
                            "request":{"subtype":"mcp_message","server_name":"codex_desktop",
                                "message":{"jsonrpc":"2.0","id":73,"method":"tools/call","params":{"name":name,"arguments":arguments}}}}));
                    }
                    "hang" => {
                        hanging = true;
                        emit(json!({"type":"stream_event","session_id":session,"event":{"type":"message_start","message":{"id":"working"}}}));
                        emit(json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}}));
                        emit(json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Working"}}}));
                    }
                    "process-info" => finish(session, &format!("process {} prompt {prompts}", std::process::id()), false),
                    "resume-info" => finish(session, if args.iter().any(|a| a.starts_with("--resume=")) { "resumed" } else { "new" }, false),
                    "fork-info" => finish(session, &json!({
                        "resume":args.iter().find_map(|a| a.strip_prefix("--resume=")),
                        "resumeAt":args.iter().find_map(|a| a.strip_prefix("--resume-session-at=")),
                        "fork":args.iter().any(|a| a=="--fork-session"),
                        "session":session
                    }).to_string(), false),
                    "background" => {
                        background = true;
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"running"}));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"bg","task_type":"local_agent","description":"Background fixture"}));
                        finish(session, "Parent waiting", false);
                        emit(json!({"type":"control_request","request_id":"permission_fake","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"printf fake"},"tool_use_id":"tool_fake"}}));
                    }
                    "subagent" => {
                        // Foreground Agent call, frames shaped like Claude Code 2.1.294.
                        let child = |kind: &str, content: Value| json!({"type":kind,"session_id":session,
                            "uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":"agent_call","agent_id":"agent-1",
                            "message":{"id":format!("msg_{}", uuid::Uuid::new_v4()),"role":if kind == "user" {"user"} else {"assistant"},
                                "model":"fake-child","content":content,"usage":{"input_tokens":7,"output_tokens":2}}});
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":null,
                            "message":{"id":"msg_agent","role":"assistant","content":[{"type":"tool_use","id":"agent_call","name":"Agent",
                                "input":{"description":"Inspect","prompt":"Look around","subagent_type":"general-purpose","name":"scout"}}]}}));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"agent-1","tool_use_id":"agent_call",
                            "task_type":"local_agent","is_backgrounded":false,"description":"Inspect"}));
                        emit(child("user", json!([{"type":"text","text":"Look around"}])));
                        emit(child("assistant", json!([{"type":"text","text":"Child working"},
                            {"type":"tool_use","id":"child_bash","name":"Bash","input":{"command":"ls"}}])));
                        emit(child("user", json!([{"type":"tool_result","tool_use_id":"child_bash","content":"listing"}])));
                        // A nested agent emits no frames of its own, only task events.
                        emit(child("assistant", json!([{"type":"tool_use","id":"nested_call","name":"Agent",
                            "input":{"description":"Nested","prompt":"Go deeper","subagent_type":"Explore"}}])));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"agent-1b","tool_use_id":"nested_call",
                            "task_type":"local_agent","is_backgrounded":false,"spawn_depth":2,"parent_task_id":"agent-1"}));
                        let mut nested = child("user", json!([{"type":"tool_result","tool_use_id":"nested_call","content":"Nested report"}]));
                        nested["tool_use_result"] = json!({"status":"completed","agentId":"agent-1b","content":[{"type":"text","text":"Nested report"}]});
                        emit(nested);
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-1b","tool_use_id":"nested_call",
                            "status":"completed","summary":"Nested report"}));
                        emit(child("assistant", json!([{"type":"text","text":"Child done"}])));
                        emit(json!({"type":"user","session_id":session,"parent_tool_use_id":null,
                            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"agent_call","content":"Report: all good"}]},
                            "tool_use_result":{"status":"completed","agentId":"agent-1","content":[{"type":"text","text":"Report: all good"}],
                                "usage":{"input_tokens":10,"output_tokens":5}}}));
                        emit(json!({"type":"system","subtype":"task_updated","task_id":"agent-1","patch":{"status":"completed"}}));
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-1","tool_use_id":"agent_call",
                            "status":"completed","summary":"Report: all good"}));
                        finish(session, "Parent done", false);
                    }
                    "subagent-continue" => {
                        // SendMessage resumes agent-1 from an earlier turn.
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":null,
                            "message":{"id":"msg_send","role":"assistant","content":[{"type":"tool_use","id":"send_call","name":"SendMessage",
                                "input":{"to":"agent-1","summary":"More","message":"More please"}}]}}));
                        emit(json!({"type":"user","session_id":session,"parent_tool_use_id":null,
                            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"send_call","content":"{\"success\":true}"}]}}));
                        emit(json!({"type":"user","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),
                            "parent_tool_use_id":"send_call","agent_id":"agent-1","message":{"role":"user","content":[{"type":"text","text":"More please"}]}}));
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),
                            "parent_tool_use_id":"send_call","agent_id":"agent-1",
                            "message":{"id":"msg_continued","role":"assistant","content":[{"type":"text","text":"Continued"}]}}));
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-1","tool_use_id":"send_call",
                            "status":"completed","summary":"Continued"}));
                        finish(session, "Parent continued", false);
                    }
                    "subagent-background" => {
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"running"}));
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":null,
                            "message":{"id":"msg_bg","role":"assistant","content":[{"type":"tool_use","id":"agent_bg","name":"Agent",
                                "input":{"description":"Research","prompt":"Dig in","run_in_background":true}}]}}));
                        emit(json!({"type":"system","subtype":"task_started","task_id":"agent-2","tool_use_id":"agent_bg",
                            "task_type":"local_agent","is_backgrounded":true,"description":"Research"}));
                        emit(json!({"type":"user","session_id":session,"parent_tool_use_id":null,
                            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"agent_bg","content":"Async agent launched successfully"}]},
                            "tool_use_result":{"isAsync":true,"status":"async_launched","agentId":"agent-2"}}));
                        finish(session, "Parent waiting", false);
                        emit(json!({"type":"assistant","session_id":session,"uuid":uuid::Uuid::new_v4().to_string(),
                            "parent_tool_use_id":"agent_bg","agent_id":"agent-2",
                            "message":{"id":"msg_bg_child","role":"assistant","content":[{"type":"text","text":"Background report"}]}}));
                        emit(json!({"type":"system","subtype":"task_updated","task_id":"agent-2","patch":{"status":"completed"}}));
                        emit(json!({"type":"system","subtype":"task_notification","task_id":"agent-2","tool_use_id":"agent_bg",
                            "status":"completed","summary":"Background report"}));
                        finish(session, "Parent saw report", false);
                        emit(json!({"type":"system","subtype":"session_state_changed","state":"idle"}));
                    }
                    "crash" => {
                        eprintln!("fake backend crashed deliberately");
                        std::process::exit(17);
                    }
                    "malformed" => {
                        println!("{{not valid json");
                        io::stdout().flush().unwrap();
                    }
                    "tool" => {
                        emit(
                            json!({"type":"assistant","session_id":session,"message":{"id":"msg_tool","role":"assistant","content":[{"type":"tool_use","id":"tool_fake","name":"Bash","input":{"command":"printf fake"}}],"usage":{"input_tokens":2,"output_tokens":1}}}),
                        );
                        emit(
                            json!({"type":"control_request","request_id":"permission_fake","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"printf fake"},"tool_use_id":"tool_fake","permission_suggestions":[]}}),
                        );
                    }
                    _ => finish(session, &format!("Echo: {text}"), false),
                }
            }
            Some("control_response") if value["response"]["request_id"] == "question_fake" => {
                let data = &value["response"]["response"];
                finish(session, &data.to_string(), data["behavior"] != "allow");
            }
            Some("control_response") if value["response"]["request_id"] == "desktop_call" => {
                let response = &value["response"]["response"]["mcp_response"];
                assert_eq!(response["id"], 73);
                finish(session, &response.to_string(), false);
            }
            Some("control_response") if value["response"]["request_id"] == "permission_fake" => {
                let allowed = value["response"]["response"]["behavior"] == "allow";
                let text = if allowed {
                    "fake tool output"
                } else {
                    "Tool denied"
                };
                emit(
                    json!({"type":"user","session_id":session,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_fake","content":text,"is_error":!allowed}]}}),
                );
                if background {
                    background = false;
                    emit(
                        json!({"type":"system","subtype":"task_notification","task_id":"bg","status":"completed","summary":"Background fixture finished"}),
                    );
                    finish(session, "Background complete", false);
                    emit(json!({"type":"system","subtype":"session_state_changed","state":"idle"}));
                } else {
                    finish(session, text, false);
                }
            }
            _ => {}
        }
    }
}
