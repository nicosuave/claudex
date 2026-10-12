use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
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
    #[arg(long, default_value = "sonnet", global = true)]
    model: String,
    #[arg(long, default_value_t = 60, global = true)]
    initialize_timeout_seconds: u64,
    #[arg(long, default_value_t = 300, global = true)]
    approval_timeout_seconds: u64,
    /// Keep a thread's Claude process this long after a turn so the next turn
    /// reuses it and its prompt cache; 0 starts a new process for every turn.
    #[arg(long, default_value_t = 3600, global = true)]
    idle_session_timeout_seconds: u64,
    /// Idle Claude processes kept across all threads; the oldest closes first.
    #[arg(long, default_value_t = 8, global = true)]
    max_idle_sessions: usize,
    /// Replace PATH with the one the user's interactive login shell configures,
    /// so a LaunchAgent-started server matches the user's terminal.
    #[arg(long, global = true)]
    login_shell_path: bool,
}

#[derive(Subcommand)]
enum Command {
    #[command(hide = true)]
    SandboxProxyConnect { host: String, port: u16 },
    /// Run the Codex-compatible app-server (also the default without a command).
    AppServer {
        #[command(subcommand)]
        action: Option<AppServerAction>,
    },
    /// Install this binary and its isolated macOS desktop connection.
    #[cfg(unix)]
    Install {
        /// Skip the dedicated localhost SSH connection; the local desktop
        /// router then reaches Claude without Remote Login.
        #[arg(long)]
        no_ssh: bool,
    },
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
    /// Stand in for genuine Codex as the desktop's local CLI. The desktop's
    /// app-server launch is routed between Codex and the Claude facade
    /// (reached through --sock); every other invocation runs Codex unchanged.
    #[cfg(unix)]
    Route {
        /// The genuine Codex executable the desktop would otherwise run.
        #[arg(long, env = "CLAUDE_CODEX_ROUTER_CODEX")]
        codex: PathBuf,
        /// Also append router diagnostics to this file.
        #[arg(long)]
        log: Option<PathBuf>,
        /// Codex command-line arguments exactly as the desktop passed them.
        #[arg(last = true, allow_hyphen_values = true)]
        args: Vec<String>,
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
    #[cfg(unix)]
    if let Some(Command::Route {
        codex,
        args: codex_args,
        ..
    }) = &args.command
        && !claude_codex_server::router::is_app_server_launch(codex_args)
    {
        use std::os::unix::process::CommandExt;
        // Replace this process so exit status, signals, and stdio are Codex's own.
        let error = std::process::Command::new(codex).args(codex_args).exec();
        return Err(error).with_context(|| format!("Cannot run {}", codex.display()));
    }
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
    if let Some(Command::SandboxProxyConnect { host, port }) = &args.command {
        return claude_codex_server::ssh_proxy::connect(host, *port);
    }
    #[cfg(unix)]
    match &args.command {
        Some(Command::Install { no_ssh }) => {
            return claude_codex_server::install::Installation::current()?
                .install(&args.claude, !no_ssh)
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
        Some(Command::Route {
            codex,
            log,
            args: codex_args,
        }) => {
            let socket = match &args.sock {
                Some(path) => Some(path.clone()),
                None => default_socket().ok(),
            };
            return claude_codex_server::router::run(codex, codex_args, socket, log.clone()).await;
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
        idle_session_timeout: Duration::from_secs(args.idle_session_timeout_seconds),
        max_idle_sessions: args.max_idle_sessions,
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
