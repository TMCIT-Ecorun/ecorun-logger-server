//! Send a continuous synthetic tone through the normal Opus/WebSocket path.
use std::{env, time::Duration};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use opus::{Application, Channels, Encoder};
use serde_json::Value;
use tokio::{
    net::TcpStream,
    time::{sleep, timeout, Instant},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

const SAMPLE_RATE: u32 = 8_000;
const FRAME_SAMPLES: usize = 480; // 60 ms
const MAX_PACKET_SIZE: usize = 1_275;
const DEFAULT_URL: &str = "ws://127.0.0.1:3000/ws/audio/main";
const DEFAULT_ROOM: &str = "main";
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn next_text(ws: &mut Ws) -> Result<Value> {
    loop {
        let message = timeout(Duration::from_secs(5), ws.next())
            .await
            .context("timed out waiting for server event")?
            .context("server closed WebSocket")??;
        match message {
            Message::Text(text) => return Ok(serde_json::from_str(&text)?),
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
            Message::Close(_) => anyhow::bail!("server closed WebSocket"),
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let url = args.next().unwrap_or_else(|| DEFAULT_URL.to_owned());
    let room = args.next().unwrap_or_else(|| DEFAULT_ROOM.to_owned());
    let seconds: u64 = args
        .next()
        .as_deref()
        .unwrap_or("30")
        .parse()
        .context("duration must be seconds")?;
    anyhow::ensure!(seconds > 0, "duration must be greater than zero");

    println!("Connecting synthetic 440 Hz Opus tone to {url}, room={room}, duration={seconds}s");
    let request = url
        .as_str()
        .into_client_request()
        .context("invalid WebSocket URL")?;
    let (mut ws, _) = connect_async(request)
        .await
        .context("WebSocket connection failed")?;
    let hello = next_text(&mut ws).await?;
    anyhow::ensure!(
        hello["type"] == "hello" && hello["protocol"] == 2,
        "unexpected hello event: {hello}"
    );
    let config = next_text(&mut ws).await?;
    anyhow::ensure!(
        config["type"] == "audio.config",
        "unexpected audio config event: {config}"
    );
    ws.send(Message::Text(
        serde_json::json!({"type":"audio.ptt", "active":true})
            .to_string()
            .into(),
    ))
    .await?;

    let mut encoder = Encoder::new(SAMPLE_RATE, Channels::Mono, Application::Voip)
        .context("Opus encoder init failed")?;
    let mut packet = vec![0_u8; MAX_PACKET_SIZE];
    let start = Instant::now();
    let interval = Duration::from_millis(60);
    let mut packet_count = 0_u64;
    while start.elapsed() < Duration::from_secs(seconds) {
        let pcm: Vec<i16> = (0..FRAME_SAMPLES)
            .map(|i| {
                let t =
                    (packet_count as f64 * FRAME_SAMPLES as f64 + i as f64) / SAMPLE_RATE as f64;
                (t * std::f64::consts::TAU * 440.0)
                    .sin()
                    .mul_add(8_000.0, 0.0) as i16
            })
            .collect();
        let len = encoder.encode(&pcm, &mut packet)?;
        ws.send(Message::Binary(packet[..len].to_vec().into()))
            .await?;
        packet_count += 1;
        sleep(interval).await;
    }
    ws.send(Message::Text(
        serde_json::json!({"type":"audio.ptt", "active":false})
            .to_string()
            .into(),
    ))
    .await?;
    println!(
        "Finished: sent {packet_count} Opus packets (~{} seconds).",
        packet_count * 60 / 1000
    );
    Ok(())
}
