//! Podium Connect relay.
//!
//! The logger sends Podium-format channel definitions and sample rows over the
//! existing `/ws/audio/{room_id}` WebSocket. This module owns the TCP session
//! with Podium (authentication, sample numbering, heartbeat, reconnect), so the
//! logger no longer connects to Podium directly and the streaming key stays on
//! the server.
//!
//! Wire format confirmed against next-generation Podium on 2026-10-04:
//! newline-terminated ASCII JSON over plain TCP `telemetry.podium.live:8080`.
//!   client -> {"cmd":{"auth":{"deviceId":KEY,"apiVer":1.1,...}}}
//!   server -> {"status":"ok","utc":...,"lt":...}   (bad key: silent close ~1 s)
//!   client -> {"s":{"meta":[...]}}                 (once, and on channel change)
//!   client -> {"s":{"d":[values..., bitmask...],"t":n}}   (every sample)
//!   server -> {"hb":{"lt":n}}                      (~5 s, no reply needed)
//! Podium does not accept samples until they carry a GNSS-derived `Utc`; that
//! is the logger's responsibility, this relay forwards rows unchanged.

use std::{collections::HashMap, sync::Arc, time::Duration};

use serde::Serialize;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{mpsc, Mutex},
    time::{timeout, Instant},
};
use tracing::{info, warn};

use crate::config::RoomStore;
use crate::room::RoomManager;

const DEFAULT_ENDPOINT: &str = "telemetry.podium.live:8080";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const AUTH_TIMEOUT: Duration = Duration::from_secs(20);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Close the Podium session when the logger has been silent this long.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const MAX_LINE: usize = 8 * 1024;
const COMMAND_QUEUE_CAPACITY: usize = 32;

pub const MAX_CHANNELS: usize = 128;
pub const MAX_SAMPLES_PER_BATCH: usize = 50;

/// Values the official RaceCapture app 2.19.0 sends; this combination is the
/// one confirmed to authenticate (hardware_share_20261004_podium.md).
const AUTH_API_VER: f64 = 1.1;
const AUTH_DEVICE: &str = "RC_APP (RCPMK3)";
const AUTH_VER: &str = "2.19.0 (2.16.1)";
const DEFAULT_SERIAL: &str = "C3PROTO01";

#[derive(Debug)]
enum Command {
    Meta(Vec<Value>),
    Samples(Vec<Vec<Value>>),
}

#[derive(Clone, Debug, Serialize)]
struct Status {
    #[serde(rename = "type")]
    kind: &'static str,
    room_id: String,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    lt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug)]
pub enum RelayError {
    NotConfigured,
    Invalid(&'static str),
    Busy,
}

impl RelayError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotConfigured => "podium_not_configured",
            Self::Invalid(_) => "podium_invalid",
            Self::Busy => "podium_busy",
        }
    }
    pub fn message(&self) -> &'static str {
        match self {
            Self::NotConfigured => "no Podium streaming key is configured for this room",
            Self::Invalid(message) => message,
            Self::Busy => "Podium relay queue is full; batch dropped",
        }
    }
}

struct Config {
    endpoint: String,
    serial: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            endpoint: std::env::var("ECORUN_PODIUM_ENDPOINT")
                .unwrap_or_else(|_| DEFAULT_ENDPOINT.into()),
            serial: std::env::var("ECORUN_PODIUM_SERIAL").unwrap_or_else(|_| DEFAULT_SERIAL.into()),
        }
    }
}

pub struct PodiumHub {
    config: Arc<Config>,
    rooms: Arc<RoomManager>,
    room_config: Arc<RoomStore>,
    relays: Mutex<HashMap<String, mpsc::Sender<Command>>>,
}

impl PodiumHub {
    pub fn from_env(rooms: Arc<RoomManager>, room_config: Arc<RoomStore>) -> Self {
        let config = Config::from_env();
        info!(endpoint = %config.endpoint, "Podium relay configured");
        Self {
            config: Arc::new(config),
            rooms,
            room_config,
            relays: Mutex::new(HashMap::new()),
        }
    }

    pub async fn meta(&self, room_id: &str, meta: Vec<Value>) -> Result<(), RelayError> {
        validate_meta(&meta)?;
        self.send(room_id, Command::Meta(meta)).await
    }

    pub async fn samples(&self, room_id: &str, samples: Vec<Vec<Value>>) -> Result<(), RelayError> {
        validate_samples(&samples)?;
        self.send(room_id, Command::Samples(samples)).await
    }

    async fn send(&self, room_id: &str, command: Command) -> Result<(), RelayError> {
        let Some(key) = self
            .room_config
            .get(room_id)
            .map_err(|_| RelayError::NotConfigured)?
            .and_then(|room| room.podium_key)
            .filter(|key| !key.is_empty())
        else {
            return Err(RelayError::NotConfigured);
        };
        let mut relays = self.relays.lock().await;
        let sender = relays.entry(room_id.to_owned()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
            tokio::spawn(run_relay(
                room_id.to_owned(),
                key,
                self.config.clone(),
                self.rooms.clone(),
                rx,
            ));
            tx
        });
        // Live telemetry: if Podium is slower than the logger, drop the newest
        // batch rather than building up latency. Meta must not be lost.
        match command {
            Command::Meta(_) => sender.send(command).await.map_err(|_| RelayError::Busy),
            Command::Samples(_) => sender.try_send(command).map_err(|_| RelayError::Busy),
        }
    }
}

fn validate_meta(meta: &[Value]) -> Result<(), RelayError> {
    if meta.is_empty() || meta.len() > MAX_CHANNELS {
        return Err(RelayError::Invalid("meta must have 1..128 channels"));
    }
    for channel in meta {
        let Some(object) = channel.as_object() else {
            return Err(RelayError::Invalid("meta entries must be objects"));
        };
        match object.get("nm").and_then(Value::as_str) {
            Some(name)
                if !name.is_empty()
                    && name.len() <= 32
                    && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') => {}
            _ => {
                return Err(RelayError::Invalid(
                    "meta nm must be 1..32 ASCII letters/digits",
                ))
            }
        }
    }
    if !is_ascii_json(&Value::Array(meta.to_vec())) {
        return Err(RelayError::Invalid("meta must be ASCII"));
    }
    Ok(())
}

fn validate_samples(samples: &[Vec<Value>]) -> Result<(), RelayError> {
    if samples.is_empty() || samples.len() > MAX_SAMPLES_PER_BATCH {
        return Err(RelayError::Invalid("samples must have 1..50 rows"));
    }
    // values + one bitmask per 32 channels
    let max_len = MAX_CHANNELS + MAX_CHANNELS / 32;
    for row in samples {
        if row.is_empty() || row.len() > max_len || !row.iter().all(Value::is_number) {
            return Err(RelayError::Invalid(
                "sample rows must be non-empty numeric arrays",
            ));
        }
    }
    Ok(())
}

fn is_ascii_json(value: &Value) -> bool {
    match value {
        Value::String(text) => text.is_ascii(),
        Value::Array(items) => items.iter().all(is_ascii_json),
        Value::Object(map) => map.iter().all(|(k, v)| k.is_ascii() && is_ascii_json(v)),
        _ => true,
    }
}

fn line(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).expect("JSON value serializes");
    bytes.push(b'\n');
    bytes
}

struct Relay {
    room_id: String,
    rooms: Arc<RoomManager>,
    last_state: &'static str,
}

impl Relay {
    async fn report(&mut self, state: &'static str, lt: Option<u64>, message: Option<String>) {
        // Only state transitions and heartbeat progress are worth a message.
        if state == self.last_state && lt.is_none() {
            return;
        }
        self.last_state = state;
        let status = Status {
            kind: "podium.status",
            room_id: self.room_id.clone(),
            state,
            lt,
            message,
        };
        self.rooms
            .broadcast_control(&self.room_id, serde_json::to_string(&status).unwrap())
            .await;
    }
}

async fn run_relay(
    room_id: String,
    key: String,
    config: Arc<Config>,
    rooms: Arc<RoomManager>,
    mut rx: mpsc::Receiver<Command>,
) {
    let mut relay = Relay {
        room_id,
        rooms,
        last_state: "idle",
    };
    let mut meta: Option<Vec<Value>> = None;
    let mut backoff = Duration::from_secs(1);

    loop {
        // Wait for the logger to be active before (re)opening Podium. Samples
        // arriving before a channel definition cannot be interpreted; drop them.
        let Some(command) = rx.recv().await else {
            return;
        };
        match command {
            Command::Meta(value) => meta = Some(value),
            Command::Samples(_) => {}
        }
        let Some(current_meta) = meta.clone() else {
            relay.report("waiting_meta", None, None).await;
            continue;
        };

        relay.report("connecting", None, None).await;
        match session(&key, &config, &current_meta, &mut meta, &mut rx, &mut relay).await {
            Ok(SessionEnd::Idle) => {
                backoff = Duration::from_secs(1);
                relay.report("idle", None, None).await;
            }
            Ok(SessionEnd::LoggerGone) => {
                relay.report("idle", None, None).await;
                return;
            }
            Err(error) => {
                warn!(room = %relay.room_id, %error, "Podium session ended");
                relay.report("error", None, Some(error.to_string())).await;
                // Drop whatever the logger sent while we wait, keeping the
                // newest channel definition.
                let deadline = Instant::now() + backoff;
                while let Ok(Some(command)) = timeout(
                    deadline.saturating_duration_since(Instant::now()),
                    rx.recv(),
                )
                .await
                {
                    if let Command::Meta(value) = command {
                        meta = Some(value);
                    }
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

enum SessionEnd {
    Idle,
    LoggerGone,
}

async fn session(
    key: &str,
    config: &Config,
    initial_meta: &[Value],
    meta: &mut Option<Vec<Value>>,
    rx: &mut mpsc::Receiver<Command>,
    relay: &mut Relay,
) -> anyhow::Result<SessionEnd> {
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(&config.endpoint))
        .await
        .map_err(|_| anyhow::anyhow!("Podium connect timeout"))??;
    stream.set_nodelay(true).ok();
    let (read_half, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let auth = json!({"cmd":{"auth":{
        "deviceId": key,
        "apiVer": AUTH_API_VER,
        "device": AUTH_DEVICE,
        "ver": AUTH_VER,
        "sn": config.serial,
    }}});
    write(&mut writer, &line(&auth)).await?;

    // A bad key gets no reply: Podium just closes after about a second.
    let auth_deadline = Instant::now() + AUTH_TIMEOUT;
    let mut buffer = String::new();
    loop {
        buffer.clear();
        let remaining = auth_deadline.saturating_duration_since(Instant::now());
        let read = timeout(remaining, read_line(&mut reader, &mut buffer))
            .await
            .map_err(|_| anyhow::anyhow!("Podium auth response timeout"))??;
        if read == 0 {
            anyhow::bail!("Podium closed the connection during auth (streaming key rejected?)");
        }
        let Ok(response) = serde_json::from_str::<Value>(&buffer) else {
            continue;
        };
        match response.get("status").and_then(Value::as_str) {
            Some("ok") => break,
            Some(other) => anyhow::bail!("Podium auth status {other}"),
            None => continue,
        }
    }
    info!(room = %relay.room_id, "Podium authenticated");

    write(&mut writer, &line(&json!({"s":{"meta":initial_meta}}))).await?;
    relay.report("streaming", None, None).await;
    let mut tick: u64 = 0;

    buffer.clear();
    loop {
        // read_line is not cancel safe: a partial line may already be in
        // `buffer` when the other branch wins, so only clear after a full line.
        tokio::select! {
            command = timeout(IDLE_TIMEOUT, rx.recv()) => match command {
                Err(_) => {
                    writer.shutdown().await.ok();
                    return Ok(SessionEnd::Idle);
                }
                Ok(None) => {
                    writer.shutdown().await.ok();
                    return Ok(SessionEnd::LoggerGone);
                }
                Ok(Some(Command::Meta(value))) => {
                    let changed = meta.as_ref() != Some(&value);
                    *meta = Some(value);
                    if changed {
                        write(&mut writer, &line(&json!({"s":{"meta":meta.as_ref().unwrap()}}))).await?;
                    }
                }
                Ok(Some(Command::Samples(rows))) => {
                    // One write per batch: fewer syscalls, and Podium only
                    // needs each row on its own line.
                    let mut bytes = Vec::with_capacity(rows.len() * 128);
                    for row in rows {
                        bytes.extend(line(&json!({"s":{"d":row,"t":tick}})));
                        tick += 1;
                    }
                    write(&mut writer, &bytes).await?;
                }
            },
            read = read_line(&mut reader, &mut buffer) => {
                if read? == 0 {
                    anyhow::bail!("Podium closed the connection");
                }
                if let Ok(message) = serde_json::from_str::<Value>(&buffer) {
                    if let Some(lt) = message.pointer("/hb/lt").and_then(Value::as_u64) {
                        relay.report("streaming", Some(lt), None).await;
                    }
                }
                buffer.clear();
            }
        }
    }
}

async fn write(writer: &mut tokio::net::tcp::OwnedWriteHalf, bytes: &[u8]) -> anyhow::Result<()> {
    timeout(WRITE_TIMEOUT, writer.write_all(bytes))
        .await
        .map_err(|_| anyhow::anyhow!("Podium write timeout"))??;
    Ok(())
}

async fn read_line(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    buffer: &mut String,
) -> anyhow::Result<usize> {
    let read = reader.read_line(buffer).await?;
    if buffer.len() > MAX_LINE {
        anyhow::bail!("Podium line too long");
    }
    Ok(read)
}

#[cfg(test)]
mod tests;
