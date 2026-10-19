use clap::{parser::ValueSource, CommandFactory, FromArgMatches, Parser, ValueEnum};
use qdynn_proto::parse_secret;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Command {
    /// Validate configuration and exit without opening network connections
    CheckConfig,
    /// Diagnose local backend and public HTTPS/WebSocket (requires Python 3)
    Doctor,
}

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[command(
    name = "qdynn-bridge",
    author = "Stepan <stepan163s>",
    version = "0.1.0",
    about = "Telegram WEB Proxy relay with bounded queues and cancellable connections"
)]
pub struct Config {
    #[arg(value_enum, conflicts_with = "print_link")]
    #[serde(skip)]
    pub command: Option<Command>,
    /// Bind address and port for the HTTP/WebSocket server
    #[arg(
        short = 'l',
        long,
        default_value = "127.0.0.1:8080",
        env = "QDYNN_LISTEN"
    )]
    pub listen: SocketAddr,

    /// Public domain name (e.g. proxy.example.com) configured on Caddy / DNS
    #[arg(
        short = 'H',
        long,
        default_value = "proxy.example.com",
        env = "QDYNN_HOSTNAME"
    )]
    pub hostname: String,

    /// MTProxy secret in hex format (16 bytes or 17 bytes with 'dd' prefix)
    #[arg(short = 's', long, default_value = "", env = "QDYNN_SECRET")]
    pub secret: String,

    /// MTProxy backend TCP address
    #[arg(
        short = 'b',
        long,
        default_value = "127.0.0.1:2398",
        env = "QDYNN_BACKEND"
    )]
    pub backend: SocketAddr,

    /// Transport carrier mode: fixed mode or auto (server-approved mode negotiation)
    #[arg(
        short = 'm',
        long,
        default_value = "websocket",
        env = "QDYNN_CARRIER_MODE"
    )]
    pub carrier_mode: String,

    /// Optional path to custom static website directory for camouflage.
    /// By default, a sleek built-in camouflage website is automatically served.
    #[arg(short = 'p', long, env = "QDYNN_PUBLIC_DIR")]
    pub public_dir: Option<PathBuf>,

    /// Path to TOML configuration file
    #[arg(short = 'c', long, env = "QDYNN_CONFIG")]
    pub config_file: Option<PathBuf>,

    /// Print connection links and QR code, then exit (contains the secret)
    #[arg(long)]
    pub print_link: bool,

    /// Optional private health/metrics listener (numeric loopback only)
    #[arg(long, env = "QDYNN_ADMIN_LISTEN")]
    pub admin_listen: Option<SocketAddr>,

    /// Absolute WEB carrier startup deadline in milliseconds
    #[arg(long, default_value = "15000", env = "QDYNN_STARTUP_TIMEOUT_MS")]
    pub startup_timeout_ms: u64,

    /// Maximum WebSocket opening time in milliseconds
    #[arg(long, default_value = "4000", env = "QDYNN_WEBSOCKET_OPEN_TIMEOUT_MS")]
    pub websocket_open_timeout_ms: u64,

    /// Maximum duration of one bounded HTTP carrier request in milliseconds
    #[arg(long, default_value = "90000", env = "QDYNN_REQUEST_TIMEOUT_MS")]
    pub request_timeout_ms: u64,

    /// Maximum number of outstanding bootstrap capabilities
    #[arg(long, default_value = "1024", env = "QDYNN_MAX_BOOTSTRAPS")]
    pub max_bootstraps: usize,

    /// Maximum active sessions held by this process
    #[arg(long, default_value = "128", env = "QDYNN_MAX_SESSIONS")]
    pub max_sessions: usize,

    /// Maximum outstanding bootstraps for one profile
    #[arg(long, default_value = "1024", env = "QDYNN_MAX_BOOTSTRAPS_PER_PROFILE")]
    pub max_bootstraps_per_profile: usize,

    /// Maximum active sessions for one profile
    #[arg(long, default_value = "128", env = "QDYNN_MAX_SESSIONS_PER_PROFILE")]
    pub max_sessions_per_profile: usize,

    /// Maximum concurrent HTTP/API requests, including long polls
    #[arg(long, default_value = "256", env = "QDYNN_MAX_API_REQUESTS")]
    pub max_api_requests: usize,

    /// Maximum concurrent WebSocket connections
    #[arg(long, default_value = "256", env = "QDYNN_MAX_WEBSOCKETS")]
    pub max_websockets: usize,

    /// Per-session queued DATA budget in bytes
    #[arg(
        long,
        default_value = "16777216",
        env = "QDYNN_SESSION_QUEUE_DATA_BYTES"
    )]
    pub session_queue_data_bytes: usize,

    /// Per-session control-frame budget in bytes
    #[arg(
        long,
        default_value = "131072",
        env = "QDYNN_SESSION_QUEUE_CONTROL_BYTES"
    )]
    pub session_queue_control_bytes: usize,

    /// Process-wide queued DATA budget in bytes
    #[arg(
        long,
        default_value = "134217728",
        env = "QDYNN_GLOBAL_QUEUE_DATA_BYTES"
    )]
    pub global_queue_data_bytes: usize,

    /// Process-wide control-frame budget in bytes
    #[arg(
        long,
        default_value = "4194304",
        env = "QDYNN_GLOBAL_QUEUE_CONTROL_BYTES"
    )]
    pub global_queue_control_bytes: usize,

    /// Process-wide budget for incoming HTTP/WebSocket bodies
    #[arg(long, default_value = "67108864", env = "QDYNN_INPUT_BODY_BYTES")]
    pub input_body_bytes: usize,

    /// Maximum active backend TCP connections
    #[arg(long, default_value = "256", env = "QDYNN_MAX_BACKEND_CONNECTIONS")]
    pub max_backend_connections: usize,

    /// Maximum active streams within one session
    #[arg(long, default_value = "32", env = "QDYNN_MAX_STREAMS_PER_SESSION")]
    pub max_streams_per_session: usize,

    /// Maximum lane states per session, including control lane 0
    #[arg(long, default_value = "129", env = "QDYNN_MAX_LANES_PER_SESSION")]
    pub max_lanes_per_session: usize,

    /// Total bounded items in each carrier lane queue
    #[arg(long, default_value = "128", env = "QDYNN_LANE_QUEUE_ITEMS")]
    pub lane_queue_items: usize,

    /// DATA item slots in each lane; remaining capacity is available to control frames
    #[arg(long, default_value = "96", env = "QDYNN_LANE_DATA_ITEMS")]
    pub lane_data_items: usize,

    /// Uplink chunks queued for each backend stream
    #[arg(long, default_value = "4096", env = "QDYNN_STREAM_QUEUE_ITEMS")]
    pub stream_queue_items: usize,

    /// Comma-separated CIDRs of reverse proxies allowed to supply X-Forwarded-For
    #[arg(long, value_delimiter = ',', env = "QDYNN_TRUSTED_PROXY_CIDRS")]
    pub trusted_proxy_cidrs: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            command: None,
            listen: "127.0.0.1:8080".parse().unwrap(),
            hostname: "proxy.example.com".into(),
            secret: String::new(),
            backend: "127.0.0.1:2398".parse().unwrap(),
            carrier_mode: "websocket".into(),
            public_dir: None,
            config_file: None,
            print_link: false,
            admin_listen: None,
            startup_timeout_ms: 15_000,
            websocket_open_timeout_ms: 4_000,
            request_timeout_ms: 90_000,
            max_bootstraps: 1024,
            max_sessions: 128,
            max_bootstraps_per_profile: 1024,
            max_sessions_per_profile: 128,
            max_api_requests: 256,
            max_websockets: 256,
            session_queue_data_bytes: 16 * 1024 * 1024,
            session_queue_control_bytes: 128 * 1024,
            global_queue_data_bytes: 128 * 1024 * 1024,
            global_queue_control_bytes: 4 * 1024 * 1024,
            input_body_bytes: 64 * 1024 * 1024,
            max_backend_connections: 256,
            max_streams_per_session: 32,
            max_lanes_per_session: 129,
            lane_queue_items: 128,
            lane_data_items: 96,
            stream_queue_items: 4096,
            trusted_proxy_cidrs: Vec::new(),
        }
    }
}
impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let matches = Self::command()
            .try_get_matches()
            .unwrap_or_else(|error| error.exit());
        Self::load_matches(matches)
    }
    pub fn load_from(
        args: impl IntoIterator<Item = impl Into<std::ffi::OsString> + Clone>,
    ) -> anyhow::Result<Self> {
        let matches = Self::command().try_get_matches_from(args)?;
        Self::load_matches(matches)
    }
    fn load_matches(matches: clap::ArgMatches) -> anyhow::Result<Self> {
        let cli = Self::from_arg_matches(&matches)?;
        let mut config = if let Some(path) = &cli.config_file {
            let source = std::fs::read_to_string(path)?;
            toml::from_str::<Config>(&source).map_err(|error| {
                let line = error.span().map_or(1, |span| {
                    source.as_bytes()[..span.start].iter().filter(|&&b| b == b'\n').count() + 1
                });
                // TOML errors otherwise include source excerpts, possibly the secret.
                anyhow::anyhow!("invalid configuration near line {line}; check TOML syntax, field names and value types")
            })?
        } else {
            cli.clone()
        };
        if cli.config_file.is_some() {
            macro_rules! override_arg { ($($field:ident),*) => { $(if matches.value_source(stringify!($field)).is_some_and(|s| s != ValueSource::DefaultValue) { config.$field = cli.$field; })* }; }
            override_arg!(
                listen,
                hostname,
                secret,
                backend,
                carrier_mode,
                public_dir,
                config_file,
                print_link,
                admin_listen,
                startup_timeout_ms,
                websocket_open_timeout_ms,
                request_timeout_ms,
                max_bootstraps,
                max_sessions,
                max_bootstraps_per_profile,
                max_sessions_per_profile,
                max_api_requests,
                max_websockets,
                session_queue_data_bytes,
                session_queue_control_bytes,
                global_queue_data_bytes,
                global_queue_control_bytes,
                input_body_bytes,
                max_backend_connections,
                max_streams_per_session,
                max_lanes_per_session,
                lane_queue_items,
                lane_data_items,
                stream_queue_items,
                trusted_proxy_cidrs
            );
        }
        config.command = cli.command;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&mut self) -> anyhow::Result<()> {
        parse_secret(&self.secret)?;
        self.hostname = self.hostname.to_ascii_lowercase();
        anyhow::ensure!(
            self.hostname.len() <= 253
                && self.hostname.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                }),
            "hostname must be an ASCII DNS name without a port or path"
        );
        anyhow::ensure!(
            self.backend.ip().is_loopback() && self.backend.port() != 0,
            "backend must be a numeric loopback address with a nonzero port"
        );
        anyhow::ensure!(
            matches!(
                self.carrier_mode.as_str(),
                "websocket" | "websocket-lanes" | "https" | "https-lanes" | "auto"
            ),
            "unsupported carrier mode"
        );
        anyhow::ensure!(
            (1_000..=120_000).contains(&self.startup_timeout_ms),
            "startup_timeout_ms must be between 1000 and 120000"
        );
        anyhow::ensure!(
            (100..=30_000).contains(&self.websocket_open_timeout_ms)
                && self.websocket_open_timeout_ms <= self.startup_timeout_ms,
            "websocket_open_timeout_ms must be between 100 and 30000 and no greater than startup_timeout_ms"
        );
        anyhow::ensure!(
            (1_000..=300_000).contains(&self.request_timeout_ms),
            "request_timeout_ms must be between 1000 and 300000"
        );
        anyhow::ensure!(
            (1..=100_000).contains(&self.max_bootstraps),
            "max_bootstraps must be between 1 and 100000"
        );
        anyhow::ensure!(
            (1..=100_000).contains(&self.max_sessions),
            "max_sessions must be between 1 and 100000"
        );
        anyhow::ensure!(
            (1..=100_000).contains(&self.max_bootstraps_per_profile),
            "max_bootstraps_per_profile must be between 1 and 100000"
        );
        anyhow::ensure!(
            (1..=100_000).contains(&self.max_sessions_per_profile),
            "max_sessions_per_profile must be between 1 and 100000"
        );
        anyhow::ensure!(
            (2..=100_000).contains(&self.max_api_requests),
            "max_api_requests must be between 2 and 100000 to reserve a control slot"
        );
        anyhow::ensure!(
            (1..=100_000).contains(&self.max_websockets),
            "max_websockets must be between 1 and 100000"
        );
        const MIN_BUDGET: usize = 4 * 1024;
        const MAX_BUDGET: usize = u32::MAX as usize;
        for (name, value) in [
            ("session_queue_data_bytes", self.session_queue_data_bytes),
            (
                "session_queue_control_bytes",
                self.session_queue_control_bytes,
            ),
            ("global_queue_data_bytes", self.global_queue_data_bytes),
            (
                "global_queue_control_bytes",
                self.global_queue_control_bytes,
            ),
            ("input_body_bytes", self.input_body_bytes),
        ] {
            anyhow::ensure!(
                (MIN_BUDGET..=MAX_BUDGET).contains(&value),
                "{name} must be between 4096 and 4294967295 bytes"
            );
        }
        anyhow::ensure!(
            self.global_queue_data_bytes >= self.session_queue_data_bytes,
            "global_queue_data_bytes must be at least session_queue_data_bytes"
        );
        anyhow::ensure!(
            self.global_queue_control_bytes >= self.session_queue_control_bytes,
            "global_queue_control_bytes must be at least session_queue_control_bytes"
        );
        for cidr in &self.trusted_proxy_cidrs {
            cidr.parse::<crate::client_ip::IpCidr>().map_err(|error| {
                anyhow::anyhow!("invalid trusted_proxy_cidrs entry {cidr:?}: {error}")
            })?;
        }
        anyhow::ensure!(
            (1..=100_000).contains(&self.max_backend_connections),
            "max_backend_connections must be between 1 and 100000"
        );
        anyhow::ensure!(
            (1..=4096).contains(&self.max_streams_per_session),
            "max_streams_per_session must be between 1 and 4096"
        );
        anyhow::ensure!(
            (1..=4096).contains(&self.max_lanes_per_session),
            "max_lanes_per_session must be between 1 and 4096"
        );
        anyhow::ensure!(
            (2..=8192).contains(&self.lane_queue_items),
            "lane_queue_items must be between 2 and 8192 to reserve control capacity"
        );
        anyhow::ensure!(
            (1..=8191).contains(&self.lane_data_items),
            "lane_data_items must be between 1 and 8191"
        );
        anyhow::ensure!(
            self.lane_data_items < self.lane_queue_items,
            "lane_data_items must be less than lane_queue_items to reserve control capacity"
        );
        anyhow::ensure!(
            (1..=65536).contains(&self.stream_queue_items),
            "stream_queue_items must be between 1 and 65536"
        );
        if let Some(addr) = self.admin_listen {
            anyhow::ensure!(
                addr.ip().is_loopback(),
                "admin_listen must be a numeric loopback address"
            );
        }
        if let Some(dir) = &self.public_dir {
            anyhow::ensure!(dir.is_dir(), "public_dir must be an existing directory");
        }
        Ok(())
    }

    pub fn queue_config(&self) -> qdynn_core::queue::QueueConfig {
        qdynn_core::queue::QueueConfig {
            session_data_bytes: self.session_queue_data_bytes,
            session_control_bytes: self.session_queue_control_bytes,
            global_data_bytes: self.global_queue_data_bytes,
            global_control_bytes: self.global_queue_control_bytes,
            input_bytes: self.input_body_bytes,
            lane_queue_items: self.lane_queue_items,
            lane_data_items: self.lane_data_items,
            stream_queue_items: self.stream_queue_items,
            max_streams_per_session: self.max_streams_per_session,
            max_lanes_per_session: self.max_lanes_per_session,
            backend_connections: self.max_backend_connections,
        }
    }
}
