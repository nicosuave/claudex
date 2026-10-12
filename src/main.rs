use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use claude_codex_server::{
    backend::{BackendConfig, BackendProtocol},
    server::{Event, Server, ServerConfig},
    store::Store,
    transport,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    version = concat!("0.160.0 (claude-codex-server ", env!("CARGO_PKG_VERSION"), "; core protocol subset)"),
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
    /// Default model: sonnet for Claude; the agent's own default for ACP agents.
    #[arg(long, global = true)]
    model: Option<String>,
    /// Serve an Agent Client Protocol (v1) agent instead of Claude, e.g. `opencode`.
    #[arg(long, global = true)]
    acp_agent: Option<PathBuf>,
    /// Passed to the ACP agent; repeat for each argument, e.g. `--acp-arg=acp`.
    #[arg(
        long,
        allow_hyphen_values = true,
        global = true,
        requires = "acp_agent"
    )]
    acp_arg: Vec<String>,
    /// Model provider id reported to Codex (default: the agent executable name).
    #[arg(long, global = true, requires = "acp_agent")]
    provider_id: Option<String>,
    /// Model provider display name (default: the provider id).
    #[arg(long, global = true, requires = "acp_agent")]
    provider_name: Option<String>,
    #[arg(long, default_value_t = 60, global = true)]
    initialize_timeout_seconds: u64,
    #[arg(long, default_value_t = 300, global = true)]
    approval_timeout_seconds: u64,
    /// Claude model that automatically reviews ACP agent permission requests.
    #[arg(long, default_value = "sonnet", global = true)]
    auto_review_model: String,
    /// Deadline for one automatic review; it then fails closed.
    #[arg(long, default_value_t = 90, global = true)]
    auto_review_timeout_seconds: u64,
    /// Replace PATH with the one the user's interactive login shell configures,
    /// so a LaunchAgent-started server matches the user's terminal.
    #[arg(long, global = true)]
    login_shell_path: bool,
}

#[derive(Subcommand)]
enum Command {
    #[command(hide = true)]
    SandboxProxyConnect { host: String, port: u16 },
    /// Relay an ACP agent's stdio MCP connection to the desktop tool bridge.
    #[cfg(unix)]
    #[command(hide = true)]
    McpBridge {
        #[arg(long)]
        socket: PathBuf,
    },
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

fn main() -> Result<()> {
    let args = Args::parse();
    // Children inherit PATH, so set it before the runtime starts other threads.
    if args.login_shell_path {
        match claude_codex_server::shell_env::login_shell()
            .context("Cannot determine the login shell")
            .and_then(|shell| claude_codex_server::shell_env::login_path(&shell))
        {
            // SAFETY: single-threaded; no runtime or other thread exists yet.
            Ok(path) => unsafe { std::env::set_var("PATH", path) },
            Err(error) => eprintln!("Keeping the inherited PATH: {error:#}"),
        }
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let acp = args
        .acp_agent
        .clone()
        .map(|executable| {
            claude_codex_server::acp::AcpAgent::new(
                executable,
                args.acp_arg.clone(),
                args.provider_id.clone(),
                args.provider_name.clone(),
                args.model.clone(),
                args.initialize_timeout_seconds,
            )
        })
        .transpose()?;
    if let Some(Command::SandboxProxyConnect { host, port }) = &args.command {
        return claude_codex_server::ssh_proxy::connect(host, *port);
    }
    #[cfg(unix)]
    if let Some(Command::McpBridge { socket }) = &args.command {
        return claude_codex_server::acp::run_bridge(socket).await;
    }
    #[cfg(unix)]
    match &args.command {
        Some(Command::Install) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
                .install(&args.claude)
                .await;
        }
        Some(Command::Doctor) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
                .doctor(&args.claude)
                .await;
        }
        Some(Command::Service { action, force }) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
                .service(*action, *force)
                .await;
        }
        Some(Command::Uninstall { force }) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
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
    // Each agent keeps its own threads; one process owns a state directory.
    let state_name = match &acp {
        Some(agent) => format!("claude-codex-server-{}", agent.provider_id),
        None => "claude-codex-server".to_owned(),
    };
    let state = args.state_dir.unwrap_or_else(|| {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".local/state")
            .join(&state_name)
    });
    // Claude threads use Claude's native auto mode; ACP agents are reviewed by Claude.
    let reviewer = acp
        .is_some()
        .then(|| claude_codex_server::auto_review::ReviewerConfig {
            executable: args.claude.clone(),
            model: args.auto_review_model.clone(),
            timeout: Duration::from_secs(args.auto_review_timeout_seconds),
        });
    let (backend, default_model) = match acp {
        Some(agent) => {
            if !args.claude_arg.is_empty() {
                bail!("--claude-arg does not apply to an ACP agent; use --acp-arg");
            }
            claude_codex_server::protocol::set_provider(claude_codex_server::protocol::Provider {
                id: agent.provider_id,
                name: agent.provider_name,
            });
            (
                BackendConfig {
                    executable: agent.executable,
                    extra_args: agent.args,
                    initialize_timeout: Duration::from_secs(args.initialize_timeout_seconds),
                    protocol: BackendProtocol::Acp,
                },
                agent
                    .model
                    .unwrap_or_else(|| claude_codex_server::acp::DEFAULT_MODEL.into()),
            )
        }
        None => (
            BackendConfig {
                executable: args.claude,
                extra_args: args.claude_arg,
                initialize_timeout: Duration::from_secs(args.initialize_timeout_seconds),
                protocol: BackendProtocol::Claude,
            },
            args.model.unwrap_or_else(|| "sonnet".into()),
        ),
    };
    let config = ServerConfig {
        backend,
        default_cwd: std::env::current_dir()?,
        default_model,
        approval_timeout: Duration::from_secs(args.approval_timeout_seconds),
        reviewer,
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
