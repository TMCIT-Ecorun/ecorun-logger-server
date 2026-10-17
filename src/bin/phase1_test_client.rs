use std::{env, time::Duration};

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use opus::{Application, Channels, Decoder, Encoder};
use serde_json::Value;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

const SAMPLE_RATE: u32 = 8_000;
const FRAME_SAMPLES: usize = 480;
const MAX_PACKET_SIZE: usize = 1_275;
const DEFAULT_URL: &str = "ws://127.0.0.1:3000/ws/audio/main";

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Client {
    name: &'static str,
    ws: Ws,
    decoder: Decoder,
}

impl Client {
    async fn connect(name: &'static str, url: &str) -> Result<Self> {
        let request = url
            .into_client_request()
            .with_context(|| format!("invalid WebSocket URL: {url}"))?;
        let (ws, _) = connect_async(request)
            .await
            .with_context(|| format!("{name}: failed to connect to {url}"))?;
        Ok(Self {
            name,
            ws,
            decoder: Decoder::new(SAMPLE_RATE, Channels::Mono)
                .context("failed to create Opus decoder")?,
        })
    }

    async fn next_text(&mut self) -> Result<Value> {
        loop {
            let message = timeout(Duration::from_secs(3), self.ws.next())
                .await
                .with_context(|| format!("{}: timed out waiting for server", self.name))?
                .ok_or_else(|| anyhow::anyhow!("{}: server closed connection", self.name))?
                .with_context(|| format!("{}: WebSocket receive failed", self.name))?;
            match message {
                Message::Text(text) => {
                    return serde_json::from_str(&text)
                        .with_context(|| format!("{}: invalid JSON: {text}", self.name));
                }
                Message::Ping(payload) => {
                    self.ws.send(Message::Pong(payload)).await?;
                }
                Message::Close(_) => bail!("{}: server closed connection", self.name),
                Message::Binary(_) => continue,
                Message::Pong(_) => {}
                _ => {}
            }
        }
    }

    async fn expect_event(&mut self, event_type: &str) -> Result<Value> {
        let event = self.next_text().await?;
        let actual = event["type"].as_str().unwrap_or("<missing>");
        if actual != event_type {
            bail!("{}: expected event {event_type:?}, got {event}", self.name);
        }
        Ok(event)
    }

    async fn set_ptt(&mut self, active: bool) -> Result<()> {
        self.ws
            .send(Message::Text(
                serde_json::json!({"type": "audio.ptt", "active": active})
                    .to_string()
                    .into(),
            ))
            .await?;
        let _ = self.expect_event("room.state").await?;
        Ok(())
    }

    async fn receive_audio(&mut self) -> Result<Vec<i16>> {
        loop {
            let message = timeout(Duration::from_secs(3), self.ws.next())
                .await
                .with_context(|| format!("{}: timed out waiting for audio", self.name))?
                .ok_or_else(|| anyhow::anyhow!("{}: server closed connection", self.name))?
                .with_context(|| format!("{}: WebSocket receive failed", self.name))?;
            match message {
                Message::Binary(packet) => {
                    if packet.len() > MAX_PACKET_SIZE {
                        bail!(
                            "{}: received oversized Opus packet: {} bytes",
                            self.name,
                            packet.len()
                        );
                    }
                    let mut pcm = vec![0_i16; FRAME_SAMPLES];
                    let samples = self.decoder.decode(&packet, &mut pcm, false)?;
                    if samples != FRAME_SAMPLES {
                        bail!(
                            "{}: received {samples} decoded samples, expected {FRAME_SAMPLES}",
                            self.name
                        );
                    }
                    return Ok(pcm);
                }
                Message::Text(text) => {
                    let event: Value = serde_json::from_str(&text)?;
                    if event["type"].as_str() == Some("error") {
                        bail!("{}: server error: {event}", self.name);
                    }
                }
                Message::Ping(payload) => {
                    self.ws.send(Message::Pong(payload)).await?;
                }
                Message::Close(_) => bail!("{}: server closed connection", self.name),
                Message::Pong(_) => {}
                _ => {}
            }
        }
    }
}

fn make_test_pcm() -> Vec<i16> {
    (0..FRAME_SAMPLES)
        .map(|i| {
            let phase = (i as f32 / SAMPLE_RATE as f32) * std::f32::consts::TAU * 440.0;
            (phase.sin() * 8_000.0) as i16
        })
        .collect()
}

fn rms(pcm: &[i16]) -> f64 {
    (pcm.iter()
        .map(|sample| (*sample as f64).powi(2))
        .sum::<f64>()
        / pcm.len() as f64)
        .sqrt()
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = env::args().nth(1).unwrap_or_else(|| DEFAULT_URL.to_owned());
    println!("phase1 test: connecting two clients to {url}");

    let mut sender = Client::connect("sender", &url).await?;
    let mut receiver = Client::connect("receiver", &url).await?;

    let sender_hello = sender.expect_event("hello").await?;
    let receiver_hello = receiver.expect_event("hello").await?;
    if sender_hello["protocol"] != 2 || receiver_hello["protocol"] != 2 {
        bail!("unexpected protocol version: sender={sender_hello}, receiver={receiver_hello}");
    }
    sender.expect_event("audio.config").await?;
    receiver.expect_event("audio.config").await?;
    sender.expect_event("room.state").await?;
    receiver.expect_event("room.state").await?;

    sender.set_ptt(true).await?;

    let expected = make_test_pcm();
    let mut encoder = Encoder::new(SAMPLE_RATE, Channels::Mono, Application::Voip)
        .context("failed to create Opus encoder")?;
    let mut packet = vec![0_u8; MAX_PACKET_SIZE];
    let packet_len = encoder.encode(&expected, &mut packet)?;
    packet.truncate(packet_len);
    sender.ws.send(Message::Binary(packet.into())).await?;

    let received = receiver.receive_audio().await?;
    let input_rms = rms(&expected);
    let output_rms = rms(&received);
    if output_rms < input_rms * 0.1 {
        bail!(
            "fan-out audio decoded, but received signal is unexpectedly weak: input RMS={input_rms:.1}, output RMS={output_rms:.1}"
        );
    }

    println!(
        "PASS: hello/config, initial room, Opus 8 kHz mono 60 ms, and room fan-out verified (packet={} bytes, decoded={} samples, RMS={output_rms:.1})",
        packet_len,
        received.len()
    );
    Ok(())
}
