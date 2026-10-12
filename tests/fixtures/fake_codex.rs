//! Deterministic stand-in for genuine `codex app-server`, used to test the
//! local router. Threads live in memory; `FAKE_CODEX_THREADS=n` seeds `n`
//! persisted threads with descending timestamps. `fake/state` reports what the
//! router asked of this backend.
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

fn emit(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}

fn thread(id: &str, created: i64, model: &str, turns: usize) -> Value {
    json!({"id": id, "preview": "", "modelProvider": "openai", "model": model,
        "createdAt": created, "updatedAt": created, "recencyAt": created, "turns": [],
        "status": {"type": "idle"}, "fakeTurns": turns})
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--version") {
        println!("codex-cli 9.9.9-fake");
        return;
    }
    assert!(
        args.iter().any(|arg| arg == "app-server"),
        "unexpected fake codex invocation: {args:?}"
    );
    let seeded: i64 = std::env::var("FAKE_CODEX_THREADS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    // Seeded threads predate everything the test creates afterwards.
    let mut threads: Vec<Value> = (0..seeded)
        .map(|n| thread(&format!("codex-seed-{n:03}"), 1_000 + n * 10, "gpt-fake", 1))
        .collect();
    let mut unsubscribed: Vec<String> = vec![];
    let mut requests: Vec<String> = vec![];
    let mut serial = 0u64;
    for line in io::stdin().lock().lines() {
        let message: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let Some(method) = message["method"].as_str() else {
            continue;
        };
        let Some(id) = message.get("id").cloned() else {
            continue;
        };
        requests.push(method.to_owned());
        let p = &message["params"];
        let result = match method {
            "initialize" => Ok(
                json!({"userAgent": "codex_cli_rs/9.9.9-fake", "codexHome": "/fake",
                "platformFamily": "unix", "platformOs": "macos", "args": args}),
            ),
            "model/list" => Ok(json!({"data": [
                {"id": "gpt-fake", "model": "gpt-fake", "displayName": "GPT Fake", "isDefault": true},
                {"id": "gpt-fake-mini", "model": "gpt-fake-mini", "displayName": "GPT Fake Mini", "isDefault": false}
            ], "nextCursor": null})),
            "thread/start" => {
                serial += 1;
                let id = format!("0199{serial:04}-0000-7000-8000-000000000000");
                let model = p["model"].as_str().unwrap_or("gpt-fake");
                let created = 2_000_000_000 + serial as i64;
                let started = thread(&id, created, model, 0);
                threads.push(started.clone());
                emit(json!({"method": "thread/started", "params": {"thread": started}}));
                Ok(json!({"thread": started, "model": model}))
            }
            "turn/start" => {
                let thread_id = p["threadId"].as_str().unwrap_or("");
                match threads.iter_mut().find(|t| t["id"] == thread_id) {
                    Some(thread) => {
                        thread["fakeTurns"] = json!(thread["fakeTurns"].as_u64().unwrap() + 1);
                        thread["model"] = p["model"].clone();
                        serial += 1;
                        let turn = json!({"id": format!("codex-turn-{serial}"), "items": [], "status": "inProgress"});
                        emit(json!({"id": id, "result": {"turn": turn}}));
                        emit(
                            json!({"method": "turn/started", "params": {"threadId": thread_id, "turn": turn}}),
                        );
                        emit(
                            json!({"method": "turn/completed", "params": {"threadId": thread_id,
                            "turn": {"id": turn["id"], "items": [], "status": "completed"}}}),
                        );
                        continue;
                    }
                    None => Err("thread not found"),
                }
            }
            "thread/read" => threads
                .iter()
                .find(|t| t["id"] == p["threadId"])
                .map(|t| json!({"thread": t}))
                .ok_or("thread not found"),
            "thread/unsubscribe" => {
                unsubscribed.push(p["threadId"].as_str().unwrap_or("").to_owned());
                Ok(json!({"status": "unsubscribed"}))
            }
            "thread/loaded/list" => Ok(
                json!({"data": threads.iter().map(|t| t["id"].clone()).collect::<Vec<_>>(), "nextCursor": null}),
            ),
            "thread/list" | "thread/search" => {
                let field = match p["sortKey"].as_str() {
                    Some("updated_at") => "updatedAt",
                    _ => "createdAt",
                };
                let mut listed: Vec<Value> = threads
                    .iter()
                    .filter(|t| t["fakeTurns"].as_u64() > Some(0))
                    .filter(|t| {
                        p["searchTerm"]
                            .as_str()
                            .is_none_or(|term| t["id"].as_str().unwrap().contains(term))
                    })
                    .cloned()
                    .collect();
                listed.sort_by_key(|t| -t[field].as_i64().unwrap());
                let start: usize = p["cursor"]
                    .as_str()
                    .map(|c| c.strip_prefix("fake:").unwrap().parse().unwrap())
                    .unwrap_or(0);
                let limit = p["limit"].as_u64().unwrap_or(50) as usize;
                let end = (start + limit).min(listed.len());
                let page: Vec<Value> = listed[start.min(end)..end]
                    .iter()
                    .map(|t| {
                        if method == "thread/search" {
                            json!({"thread": t})
                        } else {
                            t.clone()
                        }
                    })
                    .collect();
                let next = (end < listed.len()).then(|| format!("fake:{end}"));
                Ok(json!({"data": page, "nextCursor": next, "backwardsCursor": "fake-backwards"}))
            }
            "fake/state" => Ok(json!({"unsubscribed": unsubscribed, "requests": requests})),
            _ => Err("unsupported"),
        };
        emit(match result {
            Ok(result) => json!({"id": id, "result": result}),
            Err(message) => json!({"id": id, "error": {"code": -32600, "message": message}}),
        });
    }
}
