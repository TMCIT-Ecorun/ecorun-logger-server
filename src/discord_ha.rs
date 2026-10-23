//! Best-effort dual-bot audio failover. Both accounts remain connected to VC.
//! Not a distributed lease: network partitions can cause temporary double relaying.
use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use tracing::{info, warn};

pub fn start() -> Arc<AtomicBool> {
    let role = std::env::var("ECORUN_DISCORD_AUDIO_ROLE").unwrap_or_else(|_| "primary".into());
    let enabled = Arc::new(AtomicBool::new(role == "primary"));
    if role == "standby" {
        let url = std::env::var("ECORUN_DISCORD_PRIMARY_HEALTH_URL").unwrap_or_default();
        if url.is_empty() {
            warn!("standby audio muted: ECORUN_DISCORD_PRIMARY_HEALTH_URL missing");
        } else {
            let flag = enabled.clone();
            tokio::spawn(async move {
                let (mut failures, mut successes) = (0_u8, 0_u8);
                let mut tick = tokio::time::interval(Duration::from_secs(2));
                loop {
                    tick.tick().await;
                    let healthy = tokio::time::timeout(Duration::from_secs(2), probe(&url)).await.unwrap_or(false);
                    if healthy {
                        failures = 0;
                        successes = successes.saturating_add(1);
                        if successes >= 2 && flag.swap(false, Ordering::AcqRel) {
                            info!("primary recovered: standby audio muted");
                        }
                    } else {
                        successes = 0;
                        failures = failures.saturating_add(1);
                        if failures >= 3 && !flag.swap(true, Ordering::AcqRel) {
                            warn!("primary unreachable: standby audio enabled");
                        }
                    }
                }
            });
        }
    } else if role != "primary" {
        warn!(%role, "invalid Discord audio role; audio disabled");
    }
    enabled
}

// Private-node HTTP endpoint only. Do not point to a public load balancer.
async fn probe(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://") else { return false };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() || authority.contains('@') || authority.contains('[') { return false }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() { Ok(port) => (host, port), Err(_) => return false },
        None => (authority, 80),
    };
    if host.is_empty() { return false }
    let Ok(mut stream) = TcpStream::connect((host, port)).await else { return false };
    let request = format!("GET /{} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", path, authority);
    if stream.write_all(request.as_bytes()).await.is_err() { return false }
    let mut buf = [0_u8; 128];
    let Ok(n) = stream.read(&mut buf).await else { return false };
    let response = String::from_utf8_lossy(&buf[..n]);
    response.starts_with("HTTP/1.1 200 ") || response.starts_with("HTTP/1.0 200 ")
}
