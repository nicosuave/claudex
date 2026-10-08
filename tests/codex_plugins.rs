use claude_codex_server::{
    codex_plugins::{Client, Config},
    protocol::RpcError,
};
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

struct Fixture {
    dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("home")).unwrap();
        let executable = dir.path().join("codex");
        fs::write(&executable,r#"#!/bin/sh
set -eu
while [ "$1" = -c ]; do printf '%s\n' "$2" >> "$CODEX_HOME/overrides"; shift 2; done
test "$1" = app-server
test "$2" = --stdio
printf '%s' "$$" > "$CODEX_HOME/pid"
if [ -e "$CODEX_HOME/fail-start" ]; then printf 'SENSITIVE_STDERR_SENTINEL\n' >&2; exit 19; fi
while IFS= read -r line; do
  id=$(expr "$line" : '.*"id":"\([^"]*\)".*' || true)
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' "$line" > "$CODEX_HOME/initialize.json"
      printf '{"id":"%s","result":{"userAgent":"genuine-fixture"}}\n' "$id"
      ;;
    *'"method":"initialized"'*) : > "$CODEX_HOME/initialized" ;;
    *'"method":"echo"'*) printf '{"id":"%s","result":%s}\n' "$id" "$line" ;;
    *'"method":"callback"'*)
      callback="$id"
      printf '{"method":"mcpServer/startupStatus/updated","params":{"name":"fixture","status":"ready"}}\n'
      printf '{"id":700,"method":"mcpServer/elicitation/request","params":{"threadId":"sidecar-thread","serverName":"fixture","message":"Choose"}}\n'
      ;;
    *'"id":700'*) printf '{"id":"%s","result":%s}\n' "$callback" "$line" ;;
    *'"method":"hold"'*) held="$id"; : > "$CODEX_HOME/held" ;;
    *'"method":"release"'*)
      printf '{"id":"%s","result":"released"}\n' "$held"
      printf '{"id":"%s","result":"release-ack"}\n' "$id"
      ;;
    *'"method":"rpc-error"'*) printf '{"id":"%s","error":{"code":-32077,"message":"fixture refused","data":{"reason":"policy"}}}\n' "$id" ;;
    *'"method":"malformed"'*) printf 'SENSITIVE_INVALID_FRAME\n' ;;
    *'"method":"exit"'*) printf 'SENSITIVE_STDERR_SENTINEL\n' >&2; exit 12 ;;
    *'"method":"flood"'*)
      count=0
      while [ "$count" -lt 256 ]; do printf '{"method":"fixture/event","params":{}}\n'; count=$((count+1)); done
      ;;
    *) exit 22 ;;
  esac
done
"#).unwrap();
        fs::set_permissions(executable, fs::Permissions::from_mode(0o755)).unwrap();
        Self { dir }
    }
    fn config(&self) -> Config {
        Config {
            executable: self.dir.path().join("codex"),
            home: self.dir.path().join("home"),
            cwd: self.dir.path().to_owned(),
            overrides: vec!["plugins.\"sample@fixture\".enabled=false".into()],
        }
    }
    fn pid(&self) -> i32 {
        fs::read_to_string(self.dir.path().join("home/pid"))
            .unwrap()
            .parse()
            .unwrap()
    }
}

async fn wait_dead(pid: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while unsafe { libc::kill(pid, 0) } == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sidecar was not killed and reaped");
}

#[tokio::test]
async fn multiplexes_callbacks_notifications_and_concurrent_requests() {
    let fixture = Fixture::new();
    let (client, mut events) = Client::spawn(fixture.config()).await.unwrap();
    assert!(client.is_alive());
    assert_eq!(
        fs::read_to_string(fixture.dir.path().join("home/overrides")).unwrap(),
        "plugins.\"sample@fixture\".enabled=false\n"
    );
    let initialize: Value =
        serde_json::from_slice(&fs::read(fixture.dir.path().join("home/initialize.json")).unwrap())
            .unwrap();
    assert_eq!(initialize["capabilities"], Value::Null);
    assert_eq!(
        initialize["params"]["capabilities"]["experimentalApi"],
        true
    );
    assert!(
        initialize["id"]
            .as_str()
            .unwrap()
            .starts_with("facade-plugin-")
    );
    let clone = client.clone();
    let callback = tokio::spawn(async move { clone.request("callback", json!({})).await });
    let notification = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        notification,
        json!({"method":"mcpServer/startupStatus/updated","params":{"name":"fixture","status":"ready"}})
    );
    let request = events.recv().await.unwrap();
    assert_eq!(
        request,
        json!({"id":700,"method":"mcpServer/elicitation/request","params":{"threadId":"sidecar-thread","serverName":"fixture","message":"Choose"}})
    );
    let (first, second) = tokio::join!(
        client.request("echo", json!({"value":1})),
        client.request("echo", json!({"value":2}))
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first["params"]["value"], 1);
    assert_eq!(second["params"]["value"], 2);
    assert_ne!(first["id"], second["id"]);
    client
        .respond(
            request["id"].clone(),
            Ok(json!({"action":"accept","content":{"answer":"yes"}})),
        )
        .await
        .unwrap();
    assert_eq!(
        callback.await.unwrap().unwrap(),
        json!({"id":700,"result":{"action":"accept","content":{"answer":"yes"}}})
    );
    client.shutdown().await;
    assert!(!client.is_alive());
    wait_dead(fixture.pid()).await;
}

#[tokio::test]
async fn preserves_rpc_errors_and_server_error_responses() {
    let fixture = Fixture::new();
    let (client, mut events) = Client::spawn(fixture.config()).await.unwrap();
    let error = client.request("rpc-error", json!({})).await.unwrap_err();
    assert_eq!(error.code, -32077);
    assert_eq!(error.message, "fixture refused");
    assert_eq!(error.data, Some(json!({"reason":"policy"})));
    let clone = client.clone();
    let callback = tokio::spawn(async move { clone.request("callback", json!({})).await });
    events.recv().await.unwrap();
    let request = events.recv().await.unwrap();
    client
        .respond(request["id"].clone(), Err(RpcError::invalid("Declined")))
        .await
        .unwrap();
    assert_eq!(
        callback.await.unwrap().unwrap()["error"],
        json!({"code":-32602,"message":"Declined"})
    );
    client.shutdown().await;
}

#[tokio::test]
async fn timeout_and_cancelled_caller_do_not_poison_transport() {
    let fixture = Fixture::new();
    let (client, _events) = Client::spawn(fixture.config()).await.unwrap();
    let error = client
        .request_with_timeout("hold", json!({}), Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(error.message.contains("may still be running"));
    assert_eq!(
        client.request("release", json!({})).await.unwrap(),
        "release-ack"
    );
    fs::remove_file(fixture.dir.path().join("home/held")).unwrap();
    let clone = client.clone();
    let waiting = tokio::spawn(async move { clone.request("hold", json!({})).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !fixture.dir.path().join("home/held").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    waiting.abort();
    let _ = waiting.await;
    assert_eq!(
        client.request("release", json!({})).await.unwrap(),
        "release-ack"
    );
    assert!(client.is_alive());
    client.shutdown().await;
}

#[tokio::test]
async fn exit_invalid_output_and_startup_failure_are_bounded_and_redacted() {
    for method in ["exit", "malformed"] {
        let fixture = Fixture::new();
        let (client, _events) = Client::spawn(fixture.config()).await.unwrap();
        let error = client
            .request_with_timeout(method, json!({}), Duration::from_secs(3))
            .await
            .unwrap_err();
        assert!(!error.message.contains("SENSITIVE"));
        client.shutdown().await;
        wait_dead(fixture.pid()).await;
        assert!(client.request("echo", json!({})).await.is_err());
    }
    let fixture = Fixture::new();
    fs::write(fixture.dir.path().join("home/fail-start"), "").unwrap();
    let error = match Client::spawn(fixture.config()).await {
        Ok(_) => panic!("startup succeeded"),
        Err(error) => error,
    };
    assert!(error.message.contains("initialization failed"));
    assert!(!error.message.contains("SENSITIVE"));
    wait_dead(fixture.pid()).await;
}

#[tokio::test]
async fn backpressure_and_last_clone_drop_clean_up_child() {
    let fixture = Fixture::new();
    let (client, _events) = Client::spawn(fixture.config()).await.unwrap();
    let error = client
        .request_with_timeout("flood", json!({}), Duration::from_secs(3))
        .await
        .unwrap_err();
    assert!(error.message.contains("queue is full"), "{error}");
    client.shutdown().await;
    wait_dead(fixture.pid()).await;
    let fixture = Fixture::new();
    let (client, _events) = Client::spawn(fixture.config()).await.unwrap();
    let clone = client.clone();
    drop(client);
    clone.request("echo", json!({})).await.unwrap();
    drop(clone);
    wait_dead(fixture.pid()).await;
}

#[tokio::test]
async fn refuses_recursive_or_ambiguous_executable_paths() {
    let fixture = Fixture::new();
    let mut config = fixture.config();
    config.executable = std::env::current_exe().unwrap();
    assert!(matches!(Client::spawn(config).await,Err(error) if error.code == -32602));
    let mut config = fixture.config();
    config.executable = "codex".into();
    assert!(matches!(Client::spawn(config).await,Err(error) if error.code == -32602));
    assert!(!fixture.dir.path().join("home/pid").exists());
}

#[tokio::test]
async fn shutdown_fails_in_flight_requests_and_reaps_process() {
    let fixture = Fixture::new();
    let (client, _events) = Client::spawn(fixture.config()).await.unwrap();
    let clone = client.clone();
    let pending = tokio::spawn(async move { clone.request("hold", json!({})).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !fixture.dir.path().join("home/held").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await;
    let error = pending.await.unwrap().unwrap_err();
    assert!(error.message.contains("shut down"));
    wait_dead(fixture.pid()).await;
}
