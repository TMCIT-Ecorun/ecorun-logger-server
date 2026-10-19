use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use socketioxide::{adapter::Adapter, layer::SocketIoLayer, SocketIo};
use socketioxide_core::adapter::DefinedAdapter;
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tracing::{info, warn};

mod admin;
mod audio;
mod config;
mod map;
mod mixer;
mod podium;
mod room;
mod socketio_api;

use audio::{AudioCodec, OpusCodec};
use config::RoomStore;
use mixer::{MixHub, SourceId};
use podium::PodiumHub;
use room::{RoomManager, SessionId};

const DEFAULT_BIND: &str = "0.0.0.0:3000";
// At 60 ms per packet this bounds queued audio to 180 ms per receiver.
const AUDIO_QUEUE_CAPACITY: usize = 3;
const CONTROL_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
struct AppState {
    rooms_config: Arc<RoomStore>,
    rooms: Arc<RoomManager>,
    mix: Arc<MixHub>,
    podium: Arc<PodiumHub>,
    map: Arc<map::MapStore>,
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
        mixer: Option<bool>,
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
        *self
            == AudioFormat {
                sample_rate: audio::SAMPLE_RATE,
                channels: 1,
                frame_ms: audio::FRAME_MS,
            }
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
    let socket_state = socketio_api::SocketState::new(map.clone(), rooms_config.clone(), mix.clone());
    let podium = Arc::new(PodiumHub::from_env(rooms.clone(), rooms_config.clone()));
    let state = AppState {
        rooms_config,
        mix,
        podium,
        map: map.clone(),
        rooms,
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

    let (layer, io) = SocketIo::builder().with_state(socket_state).build_layer();
    serve_app(routes, state, layer, io, bind).await?;
    Ok(())
}

async fn serve_app<A: Adapter + DefinedAdapter>(
    routes: Router<AppState>,
    state: AppState,
    layer: SocketIoLayer<A>,
    io: SocketIo<A>,
    bind: String,
) -> anyhow::Result<()> {
    socketio_api::register(&io)
        .await
        .context("failed to register Socket.IO namespaces")?;
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

fn request_authorized(headers: &HeaderMap, rooms: &RoomStore, room_id: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|key| rooms.authenticate(room_id, key).unwrap_or(false))
        .unwrap_or(false)
}

async fn map_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> impl IntoResponse {
    if !request_authorized(&headers, &state.rooms_config, &room_id) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
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
    headers: HeaderMap,
    Path(room_id): Path<String>,
    axum::Json(map): axum::Json<map::MapConfig>,
) -> impl IntoResponse {
    if !request_authorized(&headers, &state.rooms_config, &room_id) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
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
        Ok(()) => axum::Json(map).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn telemetry_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> impl IntoResponse {
    if !request_authorized(&headers, &state.rooms_config, &room_id) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !state.rooms_config.exists(&room_id).unwrap_or(false) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({"error":"room_not_allowed"})),
        )
            .into_response();
    }
    match state.map.recent_telemetry(&room_id, 100) {
        Ok(events) => ([(header::CACHE_CONTROL, "no-store")], axum::Json(events)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error":error.to_string()})),
        )
            .into_response(),
    }
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

async fn handle_audio_socket(mut socket: WebSocket, state: AppState, initial_room_id: String) {
    let session_id = state.rooms.new_session_id();
    let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<u8>>(AUDIO_QUEUE_CAPACITY);
    let (control_tx, mut control_rx) = mpsc::channel::<String>(CONTROL_QUEUE_CAPACITY);
    let room_id: Option<String> = Some(initial_room_id.clone());
    let mut receive_audio = true;
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
                Some(packet) => if receive_audio && socket.send(Message::Binary(packet.into())).await.is_err() { break },
                None => break,
            },
            Some(packet) = mix_rx.recv() => {
                if socket.send(Message::Binary(packet.into())).await.is_err() { break }
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
                                    if state.rooms.is_talking(room, session_id).await {
                                        state.mix.push(room, SourceId::Ws(session_id), &packet);
                                    }
                                    pending_pcm.extend_from_slice(&pcm);
                                    while pending_pcm.len() >= audio::FRAME_SAMPLES {
                                        let frame: Vec<i16> = pending_pcm.drain(..audio::FRAME_SAMPLES).collect();
                                        match codec.encode(&frame) {
                                            Ok(encoded) => state.rooms.broadcast_audio(room, session_id, encoded).await,
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
                                let is_logger = role.as_deref() == Some("logger");
                                if let Some(config) = audio_config.as_ref() {
                                    if !config.is_server_format() {
                                        let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error { code: "unsupported_audio_config", message: "server audio format is 8 kHz mono 60 ms" }).unwrap());
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
                                let persisted = serde_json::json!({"meta": meta});
                                if let Err(error) = state.map.append_telemetry(room, "podium.meta", None, &persisted) {
                                    warn!(%error, "failed to persist Podium metadata");
                                }
                                let meta = persisted.get("meta").and_then(|value| value.as_array()).cloned().unwrap_or_default();
                                if let Err(error) = state.podium.meta(room, meta).await {
                                    let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                        code: error.code(), message: error.message(),
                                    }).unwrap());
                                }
                            }
                        }
                        Ok(ControlEvent::PodiumSamples { samples }) => {
                            if let Some(room) = room_id.as_deref() {
                                let persisted = serde_json::json!({"samples": samples});
                                if let Err(error) = state.map.append_telemetry(room, "podium", None, &persisted) {
                                    warn!(%error, "failed to persist Podium telemetry");
                                } else {
                                    state.rooms.broadcast_control(room, serde_json::json!({"type":"telemetry","source":"podium","data":persisted}).to_string()).await;
                                }
                                let samples = persisted.get("samples").and_then(|value| value.as_array()).cloned().unwrap_or_default().into_iter().map(|value| value.as_array().cloned().unwrap_or_default()).collect();
                                if let Err(error) = state.podium.samples(room, samples).await {
                                    let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                        code: error.code(), message: error.message(),
                                    }).unwrap());
                                }
                            }
                        }
                        Ok(ControlEvent::Telemetry { source, timestamp, data }) => {
                            if let Some(room) = room_id.as_deref() {
                                let payload = data.unwrap_or_else(|| serde_json::json!({}));
                                let source = source.unwrap_or_else(|| "logger".into());
                                match state.map.append_telemetry(room, &source, timestamp.as_deref(), &payload) {
                                    Ok(id) => {
                                        let event = serde_json::json!({"type":"telemetry","id":id,"source":source,"timestamp":timestamp,"data":payload});
                                        state.rooms.broadcast_control(room, event.to_string()).await;
                                    }
                                    Err(error) => warn!(%error, "failed to persist telemetry"),
                                }
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
