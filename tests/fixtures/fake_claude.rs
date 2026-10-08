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
                        emit(json!({"type":"system","subtype":"task_started","task_id":"bg","task_type":"local_agent"}));
                        finish(session, "Parent waiting", false);
                        emit(json!({"type":"control_request","request_id":"permission_fake","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"printf fake"},"tool_use_id":"tool_fake"}}));
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
                    emit(json!({"type":"system","subtype":"task_notification","task_id":"bg"}));
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
