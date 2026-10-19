use qdynn_core::SessionRegistry;
use qdynn_proto::parse_secret;
use qdynn_server::{config::Config, handlers::AppState};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let config = Config::load()?;
    let secret_bytes = parse_secret(&config.secret)?;

    match config.command {
        Some(qdynn_server::config::Command::CheckConfig) => {
            println!("OK: configuration is valid");
            return Ok(());
        }
        Some(qdynn_server::config::Command::Doctor) => {
            return qdynn_server::doctor::run(&config, &secret_bytes).await;
        }
        None => {}
    }

    if config.print_link {
        let link = format!(
            "tg://webproxy?server={}&secret={}",
            config.hostname, config.secret
        );
        println!(
            "{link}\nhttps://t.me/webproxy?server={}&secret={}",
            config.hostname, config.secret
        );
        let qr = qrcode::QrCode::new(link.as_bytes())?;
        println!(
            "{}",
            qr.render::<qrcode::render::unicode::Dense1x2>().build()
        );
        return Ok(());
    }
    let registry = Arc::new(SessionRegistry::with_profile_limits(
        config.max_bootstraps,
        config.max_sessions,
        config.max_bootstraps_per_profile,
        config.max_sessions_per_profile,
        config.queue_config(),
    ));

    // Background cleanup task
    let registry_cleanup = Arc::clone(&registry);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            registry_cleanup.cleanup_stale();
        }
    });

    info!(listen = %config.listen, hostname = %config.hostname, backend = %config.backend,
        carrier = %config.carrier_mode, "Starting relay");

    let state = Arc::new(AppState::new(
        config.clone(),
        secret_bytes,
        registry.clone(),
    ));

    let app = qdynn_server::app(state.clone());
    let (stop_admin, admin_shutdown) = tokio::sync::oneshot::channel::<()>();
    let admin = if let Some(addr) = config.admin_listen {
        let listener = TcpListener::bind(addr).await?;
        let app = qdynn_server::admin_app(state);
        Some(tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = admin_shutdown.await;
                })
                .await
        }))
    } else {
        None
    };

    let listener = TcpListener::bind(config.listen).await?;
    info!("Server started successfully!");

    let shutdown_registry = registry.clone();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        shutdown_registry.close_all();
    })
    .await?;

    let _ = stop_admin.send(());
    if let Some(admin) = admin {
        admin.await??;
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
