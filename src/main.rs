use std::{path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use claude_codex_server::{
    backend::BackendConfig,
    server::{Event, Server, ServerConfig},
    store::Store,
    transport,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    version = "0.160.0 (claude-codex-server 0.1.0; core protocol subset)",
    name = "claude-codex-server",
    about = "Codex app-server protocol over Claude Code's streaming subprocess"
)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, global = true)]
    sock: Option<PathBuf>,
    /// Desktop bootstrap currently passes features.code_mode_host=true.
    #[arg(short = 'c', long = "config", global = true)]
    config: Vec<String>,
    #[arg(long, default_value = "stdio://", global = true)]
    listen: String,
    #[arg(long, global = true)]
    stdio: bool,
    #[arg(
        long,
        env = "CLAUDE_CODE_EXECUTABLE",
        default_value = "claude",
        global = true
    )]
    claude: PathBuf,
    /// Passed directly to Claude; repeat for each argument. Intended for deployment configuration.
    #[arg(long, allow_hyphen_values = true, global = true)]
    claude_arg: Vec<String>,
    /// Separate state directory; never points at or modifies CODEX_HOME.
    #[arg(long, env = "CLAUDE_CODEX_HOME", global = true)]
    state_dir: Option<PathBuf>,
    #[arg(long, default_value = "sonnet", global = true)]
    model: String,
    #[arg(long, default_value_t = 60, global = true)]
    initialize_timeout_seconds: u64,
    #[arg(long, default_value_t = 300, global = true)]
    approval_timeout_seconds: u64,
}

#[derive(Subcommand)]
enum Command {
    /// Run the Codex-compatible app-server (also the default without a command).
    AppServer {
        #[command(subcommand)]
        action: Option<AppServerAction>,
    },
    /// Install this binary and its isolated macOS desktop connection.
    #[cfg(unix)]
    Install,
    /// Check executables, service, local SSH, and native Claude authentication.
    #[cfg(unix)]
    Doctor,
    /// Manage the per-user macOS LaunchAgent.
    #[cfg(unix)]
    Service {
        #[arg(value_enum)]
        action: claude_codex_server::install::ServiceAction,
        /// Explicitly allow stopping active conversations.
        #[arg(long)]
        force: bool,
    },
    /// Remove the service and managed SSH entries, keeping conversations and keys.
    #[cfg(unix)]
    Uninstall {
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum AppServerAction {
    /// Proxy bytes to the private desktop socket; does not open the state store.
    Proxy,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    #[cfg(unix)]
    match &args.command {
        Some(Command::Install) => {
            return claude_codex_server::install::Installation::current()?
                .install(&args.claude)
                .await;
        }
        Some(Command::Doctor) => {
            return claude_codex_server::install::Installation::current()?
                .doctor(&args.claude)
                .await;
        }
        Some(Command::Service { action, force }) => {
            return claude_codex_server::install::Installation::current()?
                .service(*action, *force)
                .await;
        }
        Some(Command::Uninstall { force }) => {
            return claude_codex_server::install::Installation::current()?
                .uninstall(*force)
                .await;
        }
        _ => {}
    }
    // This flag enables a Codex-specific host service; it does not change Claude
    // execution. Reject other overrides instead of silently discarding settings.
    for config in &args.config {
        if config != "features.code_mode_host=true" {
            bail!("Unsupported Codex config override: {config}");
        }
    }
    if matches!(
        args.command,
        Some(Command::AppServer {
            action: Some(AppServerAction::Proxy)
        })
    ) {
        let socket = match args.sock {
            Some(path) => path,
            None => default_socket()?,
        };
        return proxy(socket).await;
    }
    if args.sock.is_some() {
        bail!("--sock requires app-server proxy");
    }
    let endpoint = if args.stdio {
        "stdio://".to_owned()
    } else if args.listen == "unix://" {
        let path = default_socket()?;
        std::fs::create_dir_all(path.parent().unwrap())?;
        format!("unix://{}", path.display())
    } else {
        args.listen
    };
    let state = args.state_dir.unwrap_or_else(|| {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".local/state/claude-codex-server")
    });
    let config = ServerConfig {
        backend: BackendConfig {
            executable: args.claude,
            extra_args: args.claude_arg,
            initialize_timeout: Duration::from_secs(args.initialize_timeout_seconds),
        },
        default_cwd: std::env::current_dir()?,
        default_model: args.model,
        approval_timeout: Duration::from_secs(args.approval_timeout_seconds),
    };
    let store = Store::open(state)?;
    let (events, receiver) = mpsc::channel(512);
    let server = Server::new(config, store, events.clone())?;
    let engine = tokio::spawn(server.run(receiver));
    let shutdown = CancellationToken::new();
    let result = tokio::select! {
        result = transport::serve(&endpoint, events.clone(), shutdown.clone()) => result,
        _ = termination() => Ok(()),
    };
    shutdown.cancel();
    let _ = events.send(Event::Shutdown).await;
    let _ = engine.await;
    result
}

fn default_socket() -> Result<PathBuf> {
    // Deliberately do not default to CODEX_HOME: the facade must never bind the
    // real Codex control socket, even when launched without deployment settings.
    let home = std::env::var_os("CLAUDE_CODEX_SOCKET_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Set CLAUDE_CODEX_SOCKET_DIR to a dedicated absolute directory for unix:// or proxy"
            )
        })?;
    if !home.is_absolute() {
        bail!("CLAUDE_CODEX_SOCKET_DIR must be absolute");
    }
    Ok(home.join("app-server-control.sock"))
}

#[cfg(unix)]
async fn proxy(path: PathBuf) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let stream = tokio::net::UnixStream::connect(path).await?;
    let (mut read, mut write) = stream.into_split();
    let input = async {
        tokio::io::copy(&mut tokio::io::stdin(), &mut write).await?;
        match write.shutdown().await {
            // The WebSocket peer may have completed its close handshake before
            // stdin reaches EOF. That is a normal proxy shutdown on macOS.
            Err(error) if error.kind() == std::io::ErrorKind::NotConnected => Ok(()),
            result => result,
        }
    };
    let output = async {
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut read, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::try_join!(input, output)?;
    Ok(())
}

#[cfg(not(unix))]
async fn proxy(_path: PathBuf) -> Result<()> {
    bail!("Desktop SSH proxy requires Unix sockets")
}

async fn termination() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
