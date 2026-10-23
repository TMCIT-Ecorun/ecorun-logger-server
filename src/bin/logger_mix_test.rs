//! End-to-end check of the logger audio mix: a fake browser talks over
//! Socket.IO (16 kHz Opus, 60 ms packets, like the web client) while a fake
//! logger listens on `/ws/audio/{room}` with the logger mix format. Prints
//! how much audio reached the logger and what it cost in bytes.
//!
//! Usage: logger_mix_test [http://127.0.0.1:3000] [room] [seconds] [room key]
//! The room must exist (`ecorun-logger-server admin`); its key authenticates
//! the fake browser.
use std::{env, time::Duration};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use opus::{Application, Bitrate, Channels, Decoder, Encoder};
use serde_json::json;
use tokio::time::{sleep, timeout, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const BROWSER_RATE: u32 = 16_000;
const BROWSER_FRAME: usize = 960; // 60 ms

const BROWSER_ID: &str = "mix-test-browser";

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let base = args
        .next()
        .unwrap_or_else(|| "http://127.0.0.1:3000".into());
    let room = args.next().unwrap_or_else(|| "mix-test".into());
    let seconds: u64 = args.next().as_deref().unwrap_or("5").parse()?;
    let key = args.next().unwrap_or_default();
    let ws_base = base.replacen("http", "ws", 1);

    // Logger: ask for the mix format only.
    let (mut logger, _) = connect_async(format!("{ws_base}/ws/audio/{room}"))
        .await
        .context("logger WebSocket connection failed")?;
    logger
        .send(Message::Text(
            json!({"type":"hello","client_id":"logger-mix-test","protocol":2,
                   "audio_formats":[{"sample_rate":8000,"channels":1,"frame_ms":120}]})
            .to_string()
            .into(),
        ))
        .await?;

    // Browser: raw Engine.IO v4 / Socket.IO v5 over WebSocket.
    let (mut browser, _) = connect_async(format!("{ws_base}/socket.io/?EIO=4&transport=websocket"))
        .await
        .context("Socket.IO connection failed")?;
    let ns = format!("/{room}");
    loop {
        let message = timeout(Duration::from_secs(5), browser.next())
            .await
            .context("no Engine.IO open packet")?
            .context("Socket.IO closed")??;
        if let Message::Text(text) = message {
            if text.starts_with('0') {
                break;
            }
        }
    }
    browser
        .send(Message::Text(
            format!("40{ns},{}", json!({"token": key})).into(),
        ))
        .await?;
    // Wait for the server's `hello` event, not just the namespace CONNECT ack:
    // the ack can arrive before the server has registered the participant, and
    // a PTT sent in between is lost (the browser then never "talks").
    loop {
        let message = timeout(Duration::from_secs(5), browser.next())
            .await
            .context("no hello from the room namespace")?
            .context("Socket.IO closed")??;
        if let Message::Text(text) = message {
            if text.starts_with(&format!("42{ns},[\"hello\"")) {
                break;
            }
            anyhow::ensure!(!text.starts_with("44"), "namespace connect refused: {text}");
        }
    }
    // Packets are accepted only from the client_id this socket identified as.
    browser
        .send(Message::Text(
            format!(
                "42{ns},{}",
                json!(["client.identify", {"client_id": BROWSER_ID}])
            )
            .into(),
        ))
        .await?;
    browser
        .send(Message::Text(
            format!("42{ns},{}", json!(["audio.ptt", {"active": true}])).into(),
        ))
        .await?;
    sleep(Duration::from_millis(300)).await;

    let receiver = tokio::spawn(async move {
        let mut decoder = Decoder::new(8_000, Channels::Mono).unwrap();
        let mut pcm = [0_i16; 960];
        let (mut packets, mut bytes, mut samples, mut energy) = (0_usize, 0_usize, 0_usize, 0_f64);
        let deadline = Instant::now() + Duration::from_secs(seconds + 2);
        while let Ok(Some(Ok(message))) = timeout(
            deadline.saturating_duration_since(Instant::now()),
            logger.next(),
        )
        .await
        {
            if let Message::Binary(packet) = message {
                packets += 1;
                bytes += packet.len();
                let n = decoder.decode(&packet, &mut pcm, false).unwrap_or(0);
                samples += n;
                energy += pcm[..n].iter().map(|&s| f64::from(s).powi(2)).sum::<f64>();
            }
        }
        (
            packets,
            bytes,
            samples,
            (energy / samples.max(1) as f64).sqrt(),
        )
    });

    let mut encoder = Encoder::new(BROWSER_RATE, Channels::Mono, Application::Voip)?;
    encoder.set_bitrate(Bitrate::Bits(12_000))?;
    let mut out = vec![0_u8; 1_275];
    let start = Instant::now();
    let mut sent = 0_usize;
    while start.elapsed() < Duration::from_secs(seconds) {
        let pcm: Vec<i16> = (0..BROWSER_FRAME)
            .map(|i| {
                let t = (sent * BROWSER_FRAME + i) as f64 / f64::from(BROWSER_RATE);
                ((t * std::f64::consts::TAU * 300.0).sin() * 8_000.0) as i16
            })
            .collect();
        let size = encoder.encode(&pcm, &mut out)?;
        browser
            .send(Message::Text(
                format!(
                    "451-{ns},{}",
                    json!(["audio.packet", {"client_id": BROWSER_ID, "sequence": sent,
                                             "packet": {"_placeholder": true, "num": 0}}])
                )
                .into(),
            ))
            .await?;
        browser
            .send(Message::Binary(out[..size].to_vec().into()))
            .await?;
        sent += 1;
        // Answer Engine.IO pings so the browser socket stays up.
        while let Ok(Some(Ok(Message::Text(text)))) =
            timeout(Duration::from_millis(1), browser.next()).await
        {
            if text == "2" {
                browser.send(Message::Text("3".into())).await?;
            }
        }
        sleep(Duration::from_millis(60)).await;
    }
    browser
        .send(Message::Text(
            format!("42{ns},{}", json!(["audio.ptt", {"active": false}])).into(),
        ))
        .await?;

    let (packets, bytes, samples, rms) = receiver.await?;
    let audio_s = samples as f64 / 8_000.0;
    println!("browser sent {sent} packets ({:.1} s)", sent as f64 * 0.06);
    println!(
        "logger got {packets} packets, {bytes} bytes payload, {audio_s:.1} s audio, rms {rms:.0}"
    );
    if packets > 0 {
        println!(
            "  {:.1} packets/s, {:.0} bytes/packet, payload {:.1} kbps while talking",
            packets as f64 / audio_s,
            bytes as f64 / packets as f64,
            bytes as f64 * 8.0 / audio_s / 1000.0
        );
    }
    let ok = packets > 0 && audio_s > seconds as f64 * 0.8 && rms > 1_000.0;
    anyhow::ensure!(ok, "FAIL: logger did not receive the browser's voice");
    println!("PASS");
    Ok(())
}
