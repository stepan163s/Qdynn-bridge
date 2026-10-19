//! Optional diagnostics use Python's standard-library TLS client, keeping TLS
//! dependencies out of the relay and credentials out of process arguments.
use crate::config::Config;
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command};

pub async fn run(config: &Config, secret: &[u8]) -> anyhow::Result<()> {
    let input = serde_json::to_vec(&serde_json::json!({
        "hostname": config.hostname,
        "capability": qdynn_proto::derive_bridge_capability(&config.hostname, secret),
        "backend_host": config.backend.ip().to_string(),
        "backend_port": config.backend.port(),
        "carrier": config.carrier_mode,
        "public_dir": config.public_dir,
    }))?;
    let mut child = Command::new("python3")
        .arg("-I")
        .arg("-c")
        .arg(include_str!("doctor.py"))
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| anyhow::anyhow!("doctor requires Python 3; relay operation does not"))?;
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&input).await?;
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(90), child.wait())
        .await
        .map_err(|_| anyhow::anyhow!("doctor exceeded its 90-second budget"))??;
    anyhow::ensure!(status.success(), "doctor found a problem; see checks above");
    Ok(())
}
