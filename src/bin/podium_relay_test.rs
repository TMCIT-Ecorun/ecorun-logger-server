//! End-to-end check of the Podium relay against a fake Podium server.
//!
//! Create `podium-test` in `cargo run -- admin` and set its Podium key to `TESTKEY1`,
//! then start the logger server pointed at the fake Podium:
//!   ECORUN_PODIUM_ENDPOINT=127.0.0.1:18080 cargo run
//! then run:
//!   cargo run --bin podium_relay_test [ws://127.0.0.1:3000]
//!
//! The fake Podium binds 127.0.0.1:18080, checks the auth/meta/sample lines the
//! relay writes, and sends a heartbeat that must come back as `podium.status`.

use std::{env, time::Duration};

use anyhow::{bail, ensure, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    time::timeout,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const FAKE_PODIUM: &str = "127.0.0.1:18080";
const ROOM: &str = "podium-test";
const KEY: &str = "TESTKEY1";

#[tokio::main]
async fn main() -> Result<()> {
    let base = env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:3000".into());
    let listener = TcpListener::bind(FAKE_PODIUM)
        .await
        .context("failed to bind fake Podium")?;
    let podium = tokio::spawn(fake_podium(listener));

    let (mut ws, _) = connect_async(format!("{base}/ws/audio/{ROOM}"))
        .await
        .context("failed to connect to logger server")?;

    let meta = json!([
        {"nm":"Interval","ut":"ms","sr":10,"min":0,"max":0,"prec":0},
        {"nm":"Utc","ut":"ms","sr":10,"min":0,"max":0,"prec":0},
        {"nm":"Latitude","ut":"deg","sr":10,"min":-90,"max":90,"prec":6},
    ]);
    ws.send(Message::Text(
        json!({"type":"podium.meta","meta":meta}).to_string().into(),
    ))
    .await?;
    let rows: Vec<Value> = (0..5)
        .map(|i| json!([100 * i, 1_791_077_786_762u64 + 100 * i, 34.843112, 7]))
        .collect();
    ws.send(Message::Text(
        json!({"type":"podium.samples","samples":rows})
            .to_string()
            .into(),
    ))
    .await?;

    // Invalid input must be rejected without killing the session.
    ws.send(Message::Text(
        json!({"type":"podium.samples","samples":[["x"]]})
            .to_string()
            .into(),
    ))
    .await?;

    let mut saw_error = false;
    let mut saw_heartbeat = false;
    while !(saw_error && saw_heartbeat) {
        let message = timeout(Duration::from_secs(15), ws.next())
            .await
            .context("timed out waiting for podium.status / error")?
            .context("server closed connection")??;
        let Message::Text(text) = message else {
            continue;
        };
        let event: Value = serde_json::from_str(&text)?;
        println!("server -> {event}");
        match event["type"].as_str() {
            Some("error") if event["code"] == "podium_invalid" => saw_error = true,
            Some("podium.status") if event["lt"] == 4 => saw_heartbeat = true,
            Some("podium.status") if event["state"] == "error" => bail!("relay error: {event}"),
            _ => {}
        }
    }
    podium.await??;
    println!(
        "OK: auth, meta, 5 batched samples with t=0..4, heartbeat relayed, bad batch rejected"
    );
    Ok(())
}

async fn fake_podium(listener: TcpListener) -> Result<()> {
    let (stream, _) = timeout(Duration::from_secs(10), listener.accept())
        .await
        .context("relay never connected to fake Podium")??;
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();

    let raw_auth = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .context("relay went silent")??
        .context("relay closed")?;
    println!("podium <- {raw_auth}");
    // Byte-for-byte the field order the official app uses (confirmed to authenticate).
    ensure!(
        raw_auth.starts_with(r#"{"cmd":{"auth":{"deviceId":"#),
        "auth field order changed: {raw_auth}"
    );
    let auth: Value = serde_json::from_str(&raw_auth)?;
    ensure!(auth["cmd"]["auth"]["deviceId"] == KEY, "bad auth: {auth}");
    ensure!(
        auth["cmd"]["auth"]["apiVer"] == 1.1,
        "auth must use the new format: {auth}"
    );
    write
        .write_all(b"{\"status\":\"ok\",\"utc\":1791077774462,\"lt\":0}\n")
        .await?;

    let meta = next(&mut lines).await?;
    ensure!(meta["s"]["meta"][2]["nm"] == "Latitude", "bad meta: {meta}");
    for t in 0..5u64 {
        let sample = next(&mut lines).await?;
        ensure!(sample["s"]["t"] == t, "sample {t} out of order: {sample}");
        ensure!(
            sample["s"]["d"][2] == 34.843112,
            "latitude precision lost: {sample}"
        );
    }
    write.write_all(b"{\"hb\":{\"lt\":4}}\n").await?;
    // Keep the socket open until the client has seen the heartbeat.
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(())
}

async fn next(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
) -> Result<Value> {
    let line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .context("relay went silent")??
        .context("relay closed")?;
    println!("podium <- {line}");
    Ok(serde_json::from_str(&line)?)
}
