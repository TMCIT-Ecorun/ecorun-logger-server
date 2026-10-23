use std::{collections::VecDeque, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use socketioxide::{adapter::Adapter, layer::SocketIoLayer, SocketIo};
use socketioxide_core::adapter::DefinedAdapter;
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc},
    time::{interval, timeout, MissedTickBehavior},
};
use tracing::{info, warn};

mod admin;
mod audio;
mod audio_delivery;
mod cluster;
mod config;
mod discord_ha;
mod discord_voice;
mod map;
mod mixer;
mod podium;
mod room;
mod socketio_api;
#[cfg(test)]
mod telemetry;
mod telemetry_bridge;
mod telemetry_files;

use audio::{AudioCodec, OpusCodec};
use config::RoomStore;
use mixer::{MixHub, SourceId};
use podium::PodiumHub;
use room::{RoomManager, SessionId};

const DEFAULT_BIND: &str = "0.0.0.0:3000";
// At 120 ms per mixed packet this bounds queued audio to 360 ms per receiver.
const AUDIO_QUEUE_CAPACITY: usize = 3;
// A logger on LTE sends half a second of voice in one burst (one modem send).
// Its frames are passed on one per 60 ms instead, so the 3-packet receiver
// queues above do not overflow; this bounds the backlog to about 2 s.
const PACED_AUDIO_MAX: usize = 34;
const CONTROL_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
struct AppState {
    rooms_config: Arc<RoomStore>,
    rooms: Arc<RoomManager>,
    mix: Arc<MixHub>,
    podium: Arc<PodiumHub>,
    map: Arc<map::MapStore>,
    telemetry: Arc<socketio_api::TelemetryStore>,
    telemetry_updates: broadcast::Sender<socketio_api::TelemetryEvent>,
    raw_audio_updates: broadcast::Sender<(String, String, u64, Vec<u8>)>,
    cluster: cluster::Cluster,
    /// The loggers' `logger.config` answers, for the Discord bot's `/ecologger audio`.
    logger_config: broadcast::Sender<(String, serde_json::Value)>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlEvent {
    #[serde(rename = "hello", alias = "client_hello")]
    Hello {
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        protocol: Option<u8>,
        /// Audio formats the client can play. Omitted: assume the server's
        /// format (existing clients). Empty: the client takes no audio.
        /// Containing the logger mix format (8 kHz mono, 120 ms): the client
        /// gets one low-bitrate mix of everyone talking, browsers included,
        /// instead of each talker's packets.
        #[serde(default)]
        audio_formats: Option<Vec<AudioFormat>>,
        #[serde(default)]
        role: Option<String>,
        #[serde(default)]
        audio_config: Option<AudioFormat>,
        #[serde(default)]
        bitrate: Option<u32>,
        #[serde(default)]
        mixer: Option<serde_json::Value>,
        #[serde(default)]
        forward_to_logger: Option<bool>,
    },
    #[serde(rename = "audio.mute", alias = "mute")]
    AudioMute { muted: bool },
    #[serde(rename = "audio.ptt", alias = "ptt")]
    AudioPtt { active: bool },
    /// Podium channel definitions (`{"s":{"meta":[...]}}` contents) from a logger.
    #[serde(rename = "podium.meta")]
    PodiumMeta { meta: Vec<serde_json::Value> },
    /// Podium sample rows (`d` arrays: values then bitmasks), batched by the logger.
    #[serde(rename = "podium.samples")]
    PodiumSamples {
        samples: Vec<Vec<serde_json::Value>>,
    },
    /// Settings for the logger in this room, from the pit (Discord bot, call page), and
    /// the logger's answer with what it now uses. Passed through as is: the pit's goes to
    /// the loggers only, the logger's to everyone else. Nothing is stored.
    #[serde(rename = "logger.config")]
    LoggerConfig { data: serde_json::Value },
    #[serde(rename = "telemetry")]
    Telemetry {
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        timestamp: Option<String>,
        #[serde(default)]
        data: Option<serde_json::Value>,
    },
}

#[derive(Debug, Deserialize, PartialEq)]
struct AudioFormat {
    sample_rate: u32,
    channels: u8,
    frame_ms: u16,
}

impl AudioFormat {
    fn is_server_format(&self) -> bool {
        self.sample_rate == audio::SAMPLE_RATE
            && self.channels == 1
            && (self.frame_ms == audio::FRAME_MS || self.frame_ms == 120)
    }
    fn is_logger_mix_format(&self) -> bool {
        *self
            == AudioFormat {
                sample_rate: audio::SAMPLE_RATE,
                channels: 1,
                frame_ms: mixer::MIX_FRAME_MS,
            }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerEvent<'a> {
    #[serde(rename = "hello")]
    Hello { protocol: u8, session_id: SessionId },
    #[serde(rename = "audio.config")]
    AudioConfig {
        sample_rate: u32,
        channels: u8,
        frame_ms: u16,
    },
    #[serde(rename = "room.state")]
    RoomState {
        room_id: &'a str,
        participants: Vec<room::ParticipantState>,
    },
    #[serde(rename = "error")]
    Error { code: &'a str, message: &'a str },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("ECORUN_DB_PATH").unwrap_or_else(|_| "ecorun.sqlite3".into());
    if std::env::args().nth(1).as_deref() == Some("admin") {
        return admin::run(&db_path);
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "ecorun_logger_server=info".into()),
        )
        .init();

    let bind = std::env::var("ECORUN_BIND").unwrap_or_else(|_| DEFAULT_BIND.into());
    let rooms = Arc::new(RoomManager::new());
    let rooms_config = Arc::new(RoomStore::open(&db_path).context("failed to open room database")?);
    let map = Arc::new(map::MapStore::open(&db_path).context("failed to open SQLite database")?);
    let mix = Arc::new(MixHub::from_env());
    mix.clone().spawn();
    let telemetry = Arc::new(socketio_api::TelemetryStore::new());
    let (telemetry_updates, _) = broadcast::channel(512);
    let (raw_audio_updates, _) = broadcast::channel(128);
    let (logger_config, _) = broadcast::channel(16);
    let cluster = cluster::Cluster::from_env()?;
    let delivery = Arc::new(audio_delivery::AudioDelivery::new());
    let socket_state = socketio_api::SocketState::new(
        map.clone(),
        rooms_config.clone(),
        telemetry.clone(),
        delivery.clone(),
        cluster.clone(),
        telemetry_updates.clone(),
        mix.clone(),
        raw_audio_updates.clone(),
    );
    let podium = Arc::new(PodiumHub::from_env(rooms.clone(), rooms_config.clone()));
    discord_voice::spawn(
        rooms_config.clone(),
        rooms.clone(),
        mix.clone(),
        cluster.clone(),
        raw_audio_updates.clone(),
        logger_config.clone(),
    );
    let (layer, io) = SocketIo::builder()
        .with_state(socket_state.clone())
        .build_layer();
    let state = AppState {
        rooms_config,
        mix,
        podium,
        map: map.clone(),
        telemetry,
        telemetry_updates,
        raw_audio_updates,
        rooms,
        cluster,
        logger_config,
    };
    let routes = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/servers", get(server_list))
        .route("/api/rooms", get(room_list))
        .route("/api/rooms/{room_id}/map", get(map_get).put(map_put))
        .route("/api/rooms/{room_id}/telemetry", get(telemetry_get))
        .route("/ws/audio/{room_id}", get(audio_ws))
        .route("/", get(web_index))
        .route("/manifest.webmanifest", get(web_manifest))
        .route("/icon.svg", get(web_icon))
        .route("/icon-192.png", get(web_icon_192))
        .route("/icon-512.png", get(web_icon_512))
        .route("/service-worker.js", get(web_service_worker))
        .route("/_app/{*path}", get(web_asset));

    serve_app(routes, state, layer, io, bind, socket_state).await?;

    Ok(())
}

async fn serve_app<A: Adapter + DefinedAdapter>(
    routes: Router<AppState>,
    state: AppState,
    layer: SocketIoLayer<A>,
    io: SocketIo<A>,
    bind: String,
    socket_state: socketio_api::SocketState,
) -> anyhow::Result<()> {
    socketio_api::register(&io)
        .await
        .context("failed to register Socket.IO namespaces")?;
    let configured_rooms = state
        .rooms_config
        .list()?
        .into_iter()
        .map(|room| room.id)
        .collect();
    state.cluster.run(configured_rooms, state.map.clone());
    let mut cluster_rx = state.cluster.subscribe_inbound();
    let cluster_io = io.clone();
    let cluster_state = socket_state.clone();
    tokio::spawn(async move {
        loop {
            match cluster_rx.recv().await {
                Ok(event) => {
                    socketio_api::handle_cluster_event(&cluster_io, &cluster_state, event).await
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!(count, "cluster messages dropped")
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    // Dynamic room namespaces are created when a browser connects. Publish
    // Raw WebSocket telemetry to their Socket.IO room members.
    let mut telemetry_rx = state.telemetry_updates.subscribe();
    let telemetry_io = io.clone();
    tokio::spawn(async move {
        loop {
            let event = match telemetry_rx.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "Socket.IO telemetry broadcast fell behind");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            // SocketIo::of() cannot select a dynamic /{room_id} namespace.
            let path = format!("/{}", event.room_id);
            if let Some(namespace) = telemetry_io
                .nsps()
                .into_iter()
                .find(|ns| ns.ns_path() == path.as_str())
            {
                if let Err(error) = namespace
                    .to(event.room_id.clone())
                    .emit("telemetry", &event)
                    .await
                {
                    warn!(%error, room = %event.room_id, "failed to broadcast telemetry to Socket.IO");
                }
            }
        }
    });
    // Bridge logger microphone audio to browsers in this room.
    let mut raw_audio_rx = state.raw_audio_updates.subscribe();
    let audio_io = io.clone();
    let audio_state = socket_state.clone();
    tokio::spawn(async move {
        loop {
            let (room, source, sequence, packet) = match raw_audio_rx.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            };
            if let Some(namespace) = audio_io.of(format!("/{room}")) {
                socketio_api::deliver_audio_to_recipients(
                    namespace.to(room.clone()).sockets(),
                    &audio_state,
                    &room,
                    &source,
                    sequence,
                    &packet,
                )
                .await;
            }
        }
    });
    // Socket.IO presence is a per-node signal. A connected
    // browser can select the node that actually holds the logger session.
    let status_io = io.clone();
    let status_rooms = state.rooms.clone();
    let status_config = state.rooms_config.clone();
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;
            let Ok(configured) = status_config.list() else {
                continue;
            };
            for entry in configured {
                let room = entry.id;
                let count = status_rooms.logger_count(&room).await;
                if let Some(namespace) = status_io.of(format!("/{room}")) {
                    let _ = namespace.to(room.clone()).emit("logger.status", &serde_json::json!({"room_id":room,"connected": count > 0,"count": count})).await;
                }
            }
        }
    });
    let app = routes.layer(layer).with_state(state);
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("failed to bind {bind}"))?;
    info!(addr = %listener.local_addr()?, "EcoRun logger server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server failed")?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => info!("shutdown requested by Ctrl+C"),
        _ = terminate => info!("shutdown requested by SIGTERM"),
    }
}

async fn healthz() -> impl IntoResponse {
    "ok"
}

#[derive(Serialize, Deserialize)]
struct LoggerServer {
    id: String,
    name: String,
    url: String,
    available: bool,
}

async fn server_list() -> impl IntoResponse {
    let configured = std::env::var("ECORUN_SERVERS").unwrap_or_default();
    let parsed = serde_json::from_str::<Vec<LoggerServer>>(&configured).unwrap_or_default();
    let servers = if parsed.is_empty() {
        vec![LoggerServer {
            id: "local".into(),
            name: "This Logger".into(),
            url: String::new(),
            available: true,
        }]
    } else {
        parsed
    };
    ([(header::CACHE_CONTROL, "no-store")], axum::Json(servers))
}

async fn room_list(State(state): State<AppState>) -> impl IntoResponse {
    match state.rooms_config.list() {
        Ok(rooms) => ([(header::CACHE_CONTROL, "no-store")], axum::Json(rooms)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn map_get(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> impl IntoResponse {
    if !state.rooms_config.exists(&room_id).unwrap_or(false) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({"error":"room_not_allowed"})),
        )
            .into_response();
    }
    match state.map.get_map(&room_id) {
        Ok(map) => ([(header::CACHE_CONTROL, "no-store")], axum::Json(map)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn map_put(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
    axum::Json(map): axum::Json<map::MapConfig>,
) -> impl IntoResponse {
    if !state.rooms_config.exists(&room_id).unwrap_or(false) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({"error":"room_not_allowed"})),
        )
            .into_response();
    }
    if !map.center_lat.is_finite()
        || !map.center_lon.is_finite()
        || !map.zoom.is_finite()
        || map.zoom < 0.0
        || map.zoom > 24.0
    {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error":"invalid_map_view"})),
        )
            .into_response();
    }
    match state.map.save_map(&room_id, &map) {
        Ok(()) => {
            state
                .cluster
                .publish(state.cluster.event(cluster::ClusterKind::Map {
                    room: room_id,
                    map: map.clone(),
                }));
            axum::Json(map).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn telemetry_get(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
    Query(query): Query<TelemetryQuery>,
) -> impl IntoResponse {
    if !state.rooms_config.exists(&room_id).unwrap_or(false) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({"error":"room_not_allowed"})),
        )
            .into_response();
    }
    match state.map.telemetry_history(
        &room_id,
        query.since.as_deref(),
        query.until.as_deref(),
        5000,
    ) {
        Ok(events) => ([(header::CACHE_CONTROL, "no-store")], axum::Json(events)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Default, Deserialize)]
struct TelemetryQuery {
    since: Option<String>,
    until: Option<String>,
}

async fn web_index() -> Response {
    serve_web_file("web/build/index.html", "text/html; charset=utf-8").await
}
async fn web_manifest() -> Response {
    serve_web_file(
        "web/build/manifest.webmanifest",
        "application/manifest+json",
    )
    .await
}
async fn web_icon() -> Response {
    serve_web_file("web/build/icon.svg", "image/svg+xml").await
}
async fn web_icon_192() -> Response {
    serve_web_file("web/build/icon-192.png", "image/png").await
}
async fn web_icon_512() -> Response {
    serve_web_file("web/build/icon-512.png", "image/png").await
}
async fn web_service_worker() -> Response {
    serve_web_file(
        "web/build/service-worker.js",
        "text/javascript; charset=utf-8",
    )
    .await
}

async fn web_asset(Path(path): Path<String>) -> Response {
    let path = std::path::Path::new("web/build/_app").join(path);
    let content_type = match path.extension().and_then(|value| value.to_str()) {
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("map") => "application/json",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        _ => "application/octet-stream",
    };
    serve_web_file(path, content_type).await
}

async fn serve_web_file(path: impl AsRef<std::path::Path>, content_type: &'static str) -> Response {
    match tokio::fs::read(path).await {
        Ok(bytes) => (
            [(header::CONTENT_TYPE, HeaderValue::from_static(content_type))],
            bytes,
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn audio_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> impl IntoResponse {
    if room_id.is_empty()
        || room_id.len() > 64
        || !state.rooms_config.exists(&room_id).unwrap_or(false)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    ws.on_upgrade(move |socket| handle_audio_socket(socket, state, room_id))
        .into_response()
}

/// Anything sending Podium data is a logger, even when its hello carried no
/// `role` (EcoLogger V2 up to 2026-10 does not send one).
async fn mark_logger(state: &AppState, room: &str, session_id: SessionId, is_logger: &mut bool) {
    if !*is_logger {
        *is_logger = true;
        state.rooms.set_logger(room, session_id, true).await;
    }
}

async fn handle_audio_socket(mut socket: WebSocket, state: AppState, initial_room_id: String) {
    let session_id = state.rooms.new_session_id();
    let mut podium_channels = telemetry_bridge::PodiumChannels::default();
    let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<u8>>(AUDIO_QUEUE_CAPACITY);
    let (control_tx, mut control_rx) = mpsc::channel::<String>(CONTROL_QUEUE_CAPACITY);
    let room_id: Option<String> = Some(initial_room_id.clone());
    let mut receive_audio = true;
    let mut raw_audio_sequence = 0_u64;
    // Used once the client's hello asks for the logger mix; it then gets only mix_rx.
    let (mix_tx, mut mix_rx) = mpsc::channel::<Vec<u8>>(AUDIO_QUEUE_CAPACITY);

    let mut codec = match OpusCodec::new() {
        Ok(codec) => codec,
        Err(error) => {
            warn!(%error, "failed to initialize Opus codec");
            let _ = socket
                .send(Message::Text(
                    serde_json::to_string(&ServerEvent::Error {
                        code: "codec_init",
                        message: "audio codec initialization failed",
                    })
                    .unwrap()
                    .into(),
                ))
                .await;
            return;
        }
    };
    let mut pending_pcm: Vec<i16> = Vec::with_capacity(audio::FRAME_SAMPLES * 2);
    // Channel names from this logger's latest `podium.meta`, used to label
    // its sample rows for the web map.
    let mut is_logger = false;
    let mut paced: VecDeque<Vec<u8>> = VecDeque::new();
    let mut pace = interval(Duration::from_millis(u64::from(audio::FRAME_MS)));
    pace.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let hello = serde_json::to_string(&ServerEvent::Hello {
        protocol: 2,
        session_id,
    })
    .unwrap();
    let config = serde_json::to_string(&ServerEvent::AudioConfig {
        sample_rate: audio::SAMPLE_RATE,
        channels: 1,
        frame_ms: audio::FRAME_MS,
    })
    .unwrap();
    if socket.send(Message::Text(hello.into())).await.is_err()
        || socket.send(Message::Text(config.into())).await.is_err()
    {
        state.rooms.remove(session_id).await;
        return;
    }
    state
        .rooms
        .join(
            &initial_room_id,
            session_id,
            audio_tx.clone(),
            control_tx.clone(),
            false,
        )
        .await;
    let participants = state.rooms.participant_states(&initial_room_id).await;
    let state_event = serde_json::to_string(&ServerEvent::RoomState {
        room_id: &initial_room_id,
        participants,
    })
    .unwrap();
    state
        .rooms
        .broadcast_control(&initial_room_id, state_event)
        .await;

    loop {
        tokio::select! {
            outbound = audio_rx.recv() => match outbound {
                Some(packet) => if receive_audio {
                    // Slow LTE audio writes are expendable: never let one stall
                    // the logger's telemetry/control receive loop indefinitely.
                    match timeout(Duration::from_millis(50), socket.send(Message::Binary(packet.into()))).await {
                        Ok(Ok(())) | Err(_) => {},
                        Ok(Err(_)) => break,
                    }
                },
                None => break,
            },
            Some(packet) = mix_rx.recv() => {
                if socket.send(Message::Binary(packet.into())).await.is_err() { break }
            },
            _ = pace.tick(), if !paced.is_empty() => {
                if let (Some(room), Some(packet)) = (room_id.as_deref(), paced.pop_front()) {
                    state.rooms.broadcast_audio(room, session_id, packet).await;
                }
            },
            event = control_rx.recv() => match event {
                Some(json) => if socket.send(Message::Text(json.into())).await.is_err() { break },
                None => {}
            },
            incoming = timeout(Duration::from_secs(60), socket.recv()) => {
                let incoming = match incoming { Ok(value) => value, Err(_) => continue };
                match incoming {
                    Some(Ok(Message::Binary(packet))) => {
                        match codec.decode_and_validate(&packet) {
                            Ok(pcm) => {
                                if let Some(room) = room_id.as_deref() {
                                    // Talking is judged on arrival: paced frames may go out
                                    // after the speaker's PTT is already off.
                                    let talking = state.rooms.is_talking(room, session_id).await;
                                    if talking {
                                        state.mix.push(room, SourceId::Ws(session_id), &packet);
                                    }
                                    pending_pcm.extend_from_slice(&pcm);
                                    while pending_pcm.len() >= audio::FRAME_SAMPLES {
                                        let frame: Vec<i16> = pending_pcm.drain(..audio::FRAME_SAMPLES).collect();
                                        if !talking {
                                            continue;
                                        }
                                        match codec.encode(&frame) {
                                            Ok(encoded) => {
                                                if state.rooms.source_can_speak(room, session_id).await {
                                                    let source = format!("raw:{}:{session_id}", state.cluster.node_id());
                                                    let _ = state.raw_audio_updates.send((room.to_owned(), source.clone(), raw_audio_sequence, encoded.clone()));
                                                    state.cluster.publish(state.cluster.event(cluster::ClusterKind::Audio { room: room.to_owned(), source, sequence: raw_audio_sequence, packet: encoded.clone(), forward_to_logger: false }));
                                                    raw_audio_sequence = raw_audio_sequence.wrapping_add(1);
                                                }
                                                paced.push_back(encoded);
                                                if paced.len() > PACED_AUDIO_MAX { paced.pop_front(); }

                                            }
                                            Err(error) => warn!(%session_id, %error, "audio encode failed"),
                                        }
                                    }
                                }
                            }
                            Err(error) => warn!(%session_id, %error, "invalid Opus packet"),
                        }
                    }
                    Some(Ok(Message::Text(text))) => match serde_json::from_str::<ControlEvent>(&text) {
                        Ok(ControlEvent::Hello { client_id, protocol, audio_formats, role, audio_config, bitrate, mixer, forward_to_logger }) => {
                            let mix_listener = audio_formats
                                .as_ref()
                                .is_some_and(|formats| formats.iter().any(AudioFormat::is_logger_mix_format));
                            receive_audio = !mix_listener
                                && audio_formats.map_or(true, |formats| formats.iter().any(AudioFormat::is_server_format));
                            state.mix.remove_listener(session_id);
                            if let (true, Some(room)) = (mix_listener, room_id.as_deref()) {
                                state.mix.add_listener(room, session_id, mix_tx.clone());
                            }
                            if let Some(room) = room_id.as_deref() {
                                is_logger = role.as_deref() == Some("logger");

                                if let Some(config) = audio_config.as_ref() {
                                    if !config.is_server_format() {
                                        let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error { code: "unsupported_audio_config", message: "server audio format is 8 kHz mono 60 or 120 ms" }).unwrap());
                                    }
                                }
                                if let Some(bitrate) = bitrate {
                                    if !(4_000..=32_000).contains(&bitrate) {
                                        let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error { code: "invalid_bitrate", message: "bitrate must be 4000..32000 bps" }).unwrap());
                                    } else if let Err(error) = codec.set_bitrate(bitrate) {
                                        warn!(%session_id, %error, "failed to apply requested Opus bitrate");
                                    }
                                }
                                state.rooms.set_logger(room, session_id, is_logger).await;
                                // A mix listener gets its audio from the mixer only.
                                let per_talker = !mix_listener && forward_to_logger.unwrap_or(receive_audio);
                                state.rooms.set_receive_audio(room, session_id, per_talker).await;
                                if mix_listener {
                                    let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::AudioConfig { sample_rate: audio::SAMPLE_RATE, channels: 1, frame_ms: 120 }).unwrap());
                                }

                            }
                            info!(%session_id, ?client_id, ?protocol, ?role, ?mixer, ?bitrate, receive_audio, mix_listener, ?forward_to_logger, "client hello");
                        }
                        Ok(ControlEvent::AudioMute { muted }) => {
                            if let Some(room) = room_id.as_deref() {
                                if state.rooms.set_mute(room, session_id, muted).await {
                                    let participants = state.rooms.participant_states(room).await;
                                    let state_event = serde_json::to_string(&ServerEvent::RoomState {
                                        room_id: room, participants,
                                    }).unwrap();
                                    state.rooms.broadcast_control(room, state_event).await;
                                }
                            }
                        }
                        Ok(ControlEvent::LoggerConfig { data }) => {
                            if let Some(room) = room_id.as_deref() {
                                if is_logger {
                                    let _ = state.logger_config.send((room.to_owned(), data.clone()));
                                }
                                let event = serde_json::json!({"type":"logger.config","session_id":session_id,"data":data}).to_string();
                                if is_logger {
                                    state.rooms.broadcast_control_to_non_loggers(room, event).await;
                                } else {
                                    state.rooms.broadcast_control_to_loggers(room, event).await;
                                }
                            }
                        }
                        Ok(ControlEvent::AudioPtt { active }) => {
                            if let Some(room) = room_id.as_deref() {
                                if state.rooms.set_ptt(room, session_id, active).await {
                                    let participants = state.rooms.participant_states(room).await;
                                    let state_event = serde_json::to_string(&ServerEvent::RoomState {
                                        room_id: room, participants,
                                    }).unwrap();
                                    state.rooms.broadcast_control(room, state_event).await;
                                }
                            }
                        }
                        Ok(ControlEvent::PodiumMeta { meta }) => {
                            if let Some(room) = room_id.as_deref() {
                                mark_logger(&state, room, session_id, &mut is_logger).await;
                                podium_channels.update(&meta);
                                let persisted = serde_json::json!({"meta": &meta});
                                telemetry_bridge::publish(&state, room, "podium.meta".into(), None, persisted).await;

                                if let Err(error) = state.podium.meta(room, meta).await {
                                    let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                        code: error.code(), message: error.message(),
                                    }).unwrap());
                                }
                            }
                        }
                        Ok(ControlEvent::PodiumSamples { samples }) => {
                            if let Some(room) = room_id.as_deref() {
                                mark_logger(&state, room, session_id, &mut is_logger).await;
                                // Keep an original copy in SQLite for diagnostics.
                                let persisted = serde_json::json!({"samples": &samples});
                                telemetry_bridge::publish(&state, room, "podium.raw".into(), None, persisted).await;
                                // The Podium relay must receive the logger's exact rows.
                                if let Err(error) = state.podium.samples(room, samples.clone()).await {
                                    let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                        code: error.code(), message: error.message(),
                                    }).unwrap());
                                }
                                for row in &samples {
                                    if let Some(data) = podium_channels.decode(row) {
                                        telemetry_bridge::publish(&state, room, "podium".into(), None, data).await;
                                    } else {
                                        warn!(%session_id, room, "skipping Podium sample without usable metadata, numeric values or validity mask");
                                    }
                                }
                            }
                        }
                        Ok(ControlEvent::Telemetry { source, timestamp, data }) => {
                            if let Some(room) = room_id.as_deref() {
                                let payload = data.unwrap_or_else(|| serde_json::json!({}));
                                let source = source.unwrap_or_else(|| "logger".into());
                                telemetry_bridge::publish(&state, room, source, timestamp, payload).await;

                            }
                        }
                        Err(error) => {
                            let message = error.to_string();
                            let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                code: "invalid_control", message: &message,
                            }).unwrap());
                        }
                    },
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() { break }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }

    if let Some(room) = room_id {
        state.rooms.leave(&room, session_id).await;
        let participants = state.rooms.participant_states(&room).await;
        let state_event = serde_json::to_string(&ServerEvent::RoomState {
            room_id: &room,
            participants,
        })
        .unwrap();
        state.rooms.broadcast_control(&room, state_event).await;
    }
    state.rooms.remove(session_id).await;
    state.mix.remove_listener(session_id);
    info!(%session_id, "audio session disconnected");
}
