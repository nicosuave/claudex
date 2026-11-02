//! `thread/sample` for Claude threads.
//!
//! Desktop composer predictions call `thread/sample` once the app-server
//! advertises it, without falling back to prediction forks. The facade already
//! serves those forks with the parent's warm prompt cache, so a sample runs as
//! one hidden prediction fork turn whose final text answers the request.
use super::*;

pub(super) struct Sample {
    /// The desktop request id; `None` once the request has been answered.
    client_id: Option<Value>,
    params: Value,
    fork: Option<String>,
    turn: Option<String>,
    usage: Value,
    cancelled: bool,
}

pub(super) enum SampleStep {
    Checked,
    Forked,
    Started,
}

#[derive(Default)]
pub(super) struct Samples {
    /// Keyed by the desktop request id, which `thread/sample/cancel` names.
    pending: HashMap<String, Sample>,
    forks: HashMap<String, String>,
    /// Finished forks whose unsubscribe is unacknowledged; their trailing
    /// events precede that response.
    released: HashSet<String>,
}

impl Router {
    pub(super) fn start_sample(&mut self, message: Value) {
        let id = message["id"].clone();
        let p = &message["params"];
        let (Some(thread), Some(_), Some(prompt)) = (
            p["threadId"].as_str(),
            p["expectedTurnId"].as_str(),
            p["prompt"].as_str(),
        ) else {
            self.send_client(error(
                id,
                "thread/sample requires threadId, expectedTurnId and prompt",
            ));
            return;
        };
        if prompt.trim().is_empty() {
            self.send_client(error(id, "thread/sample requires a nonempty prompt"));
            return;
        }
        let key = id_key(&id);
        if self.samples.pending.contains_key(&key) {
            self.send_client(error(
                id,
                "A sample with this request id is already running",
            ));
            return;
        }
        let thread = thread.to_owned();
        self.samples.pending.insert(
            key.clone(),
            Sample {
                client_id: Some(id),
                params: p.clone(),
                fork: None,
                turn: None,
                usage: Value::Null,
                cancelled: false,
            },
        );
        self.internal_request(
            Side::Claude,
            "thread/turns/list",
            json!({"threadId":thread,"limit":1,"sortDirection":"desc"}),
            Continuation::Sample {
                key,
                step: SampleStep::Checked,
            },
        );
    }

    pub(super) fn sample_continue(
        &mut self,
        key: String,
        step: SampleStep,
        result: Result<Value, Value>,
    ) {
        let Some(sample) = self.samples.pending.get(&key) else {
            return;
        };
        let result = match result {
            Ok(result) => result,
            Err(failure) => {
                let message = failure["message"]
                    .as_str()
                    .unwrap_or("Claude could not sample this thread")
                    .to_owned();
                self.finish_sample(&key, Err(message));
                return;
            }
        };
        // Later steps own a fork, which they release or interrupt themselves.
        if sample.cancelled && matches!(step, SampleStep::Checked) {
            self.finish_sample(&key, Err("The sample was cancelled".into()));
            return;
        }
        match step {
            SampleStep::Checked => {
                // As in Codex, only the source's latest settled turn can be sampled.
                let latest = &result["data"][0];
                if latest["id"] != sample.params["expectedTurnId"]
                    || latest["status"] == "inProgress"
                {
                    self.finish_sample(
                        &key,
                        Err("The sample source turn is stale or still active".into()),
                    );
                    return;
                }
                let params = json!({"threadId":sample.params["threadId"],
                    "experimentalPredictionMode":true,"ephemeral":true,"excludeTurns":true,
                    "threadSource":sample.params["threadSource"]});
                self.internal_request(
                    Side::Claude,
                    "thread/fork",
                    params,
                    Continuation::Sample {
                        key,
                        step: SampleStep::Forked,
                    },
                );
            }
            SampleStep::Forked => {
                let Some(fork) = result["thread"]["id"].as_str().map(str::to_owned) else {
                    self.finish_sample(&key, Err("Claude returned no sample thread".into()));
                    return;
                };
                self.samples.forks.insert(fork.clone(), key.clone());
                let sample = self.samples.pending.get_mut(&key).unwrap();
                sample.fork = Some(fork.clone());
                if sample.cancelled {
                    self.finish_sample(&key, Err("The sample was cancelled".into()));
                    return;
                }
                // Claude keeps the parent's effort so the fork reuses its cache.
                let input =
                    json!([{"type":"text","text":sample.params["prompt"],"text_elements":[]}]);
                self.internal_request(
                    Side::Claude,
                    "turn/start",
                    json!({"threadId":fork,"input":input}),
                    Continuation::Sample {
                        key,
                        step: SampleStep::Started,
                    },
                );
            }
            SampleStep::Started => {
                let sample = self.samples.pending.get_mut(&key).unwrap();
                sample.turn = result["turn"]["id"].as_str().map(str::to_owned);
                if sample.cancelled {
                    self.interrupt_sample(&key);
                }
            }
        }
    }

    /// Sample forks are internal: their events never reach the desktop.
    pub(super) fn sample_notification(&mut self, message: &Value) -> bool {
        let Some(thread) = message["params"]["threadId"]
            .as_str()
            .or_else(|| message["params"]["thread"]["id"].as_str())
        else {
            return false;
        };
        if self.samples.released.contains(thread) {
            return true;
        }
        let Some(key) = self.samples.forks.get(thread).cloned() else {
            return false;
        };
        let p = &message["params"];
        match message["method"].as_str() {
            Some("thread/tokenUsage/updated") => {
                if let Some(sample) = self.samples.pending.get_mut(&key) {
                    sample.usage = p["tokenUsage"]["last"].clone();
                }
            }
            Some("turn/completed") => {
                let turn = &p["turn"];
                let result = match turn["status"].as_str() {
                    Some("completed") => {
                        let items = turn["items"].as_array().cloned().unwrap_or_default();
                        // Codex reports no text for a response that called a tool.
                        let tool_call = items.iter().any(|item| {
                            !matches!(
                                item["type"].as_str(),
                                Some("userMessage" | "agentMessage" | "reasoning")
                            )
                        });
                        let text = items
                            .iter()
                            .rev()
                            .find(|item| item["type"] == "agentMessage")
                            .and_then(|item| item["text"].as_str())
                            .filter(|_| !tool_call);
                        let sample = &self.samples.pending[&key];
                        Ok(json!({
                            "responseId": turn["id"],
                            "sourceTurnId": sample.params["expectedTurnId"],
                            "text": text,
                            "inputTokens": sample.usage["inputTokens"],
                            "cachedInputTokens": sample.usage["cachedInputTokens"],
                            "outputTokens": sample.usage["outputTokens"],
                        }))
                    }
                    Some("interrupted") => Err("The sample was cancelled".into()),
                    _ => Err(turn["error"]["message"]
                        .as_str()
                        .unwrap_or("Claude could not sample this thread")
                        .to_owned()),
                };
                self.finish_sample(&key, result);
            }
            _ => {}
        }
        true
    }

    /// A sample fork has no desktop to answer its server requests.
    pub(super) fn sample_server_request(&mut self, message: &Value) -> bool {
        let is_sample = message["params"]["threadId"]
            .as_str()
            .is_some_and(|thread| self.samples.forks.contains_key(thread));
        if is_sample {
            self.send_backend(
                Side::Claude,
                json!({"id":message["id"],"error":{"code":-32600,
                    "message":"Prediction samples cannot request user input"}}),
            );
        }
        is_sample
    }

    /// Returns false when the request names no Claude sample.
    pub(super) fn cancel_sample(&mut self, message: &Value) -> bool {
        let key = id_key(&message["params"]["requestId"]);
        let Some(sample) = self.samples.pending.get_mut(&key) else {
            return false;
        };
        sample.cancelled = true;
        if let Some(id) = sample.client_id.take() {
            self.send_client(error(id, "The sample was cancelled"));
        }
        self.interrupt_sample(&key);
        self.send_client(json!({"id":message["id"],"result":{}}));
        true
    }

    fn interrupt_sample(&mut self, key: &str) {
        let Some(sample) = self.samples.pending.get(key) else {
            return;
        };
        if let (Some(fork), Some(turn)) = (&sample.fork, &sample.turn) {
            let params = json!({"threadId":fork,"turnId":turn});
            self.internal_request(Side::Claude, "turn/interrupt", params, Continuation::Ignore);
        }
    }

    fn finish_sample(&mut self, key: &str, result: Result<Value, String>) {
        let Some(sample) = self.samples.pending.remove(key) else {
            return;
        };
        if let Some(id) = sample.client_id {
            self.send_client(match result {
                Ok(result) => json!({"id":id,"result":result}),
                Err(message) => error(id, message),
            });
        }
        if let Some(fork) = sample.fork {
            self.samples.forks.remove(&fork);
            self.forget_thread(&fork);
            // The facade discards an unsubscribed prediction fork.
            if self.claude_available() {
                self.samples.released.insert(fork.clone());
                self.internal_request(
                    Side::Claude,
                    "thread/unsubscribe",
                    json!({"threadId":fork}),
                    Continuation::SampleReleased { fork },
                );
            }
        }
    }

    pub(super) fn sample_released(&mut self, fork: &str) {
        self.samples.released.remove(fork);
    }

    pub(super) fn fail_samples(&mut self, message: &str) {
        let keys: Vec<String> = self.samples.pending.keys().cloned().collect();
        for key in keys {
            self.finish_sample(&key, Err(message.to_owned()));
        }
    }
}
