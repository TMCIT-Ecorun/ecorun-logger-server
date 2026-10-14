use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use socketioxide::{adapter::Adapter, layer::SocketIoLayer, SocketIo};
use socketioxide_core::adapter::DefinedAdapter;
use socketioxide_redis::{
    drivers::redis::redis_client as redis, RedisAdapter, RedisAdapterConfig, RedisAdapterCtr,
};
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tracing::{info, warn};

mod audio;
mod podium;
mod room;
mod socketio_api;

use audio::{AudioCodec, OpusCodec};
use podium::PodiumHub;
use room::{RoomManager, SessionId};

const DEFAULT_BIND: &str = "0.0.0.0:3000";
// At 60 ms per packet this bounds queued audio to 180 ms per receiver.
const AUDIO_QUEUE_CAPACITY: usize = 3;
const CONTROL_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
struct AppState {
    rooms: Arc<RoomManager>,
    podium: Arc<PodiumHub>,
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
        /// format (existing clients). Empty: the client takes no audio, e.g. a
        /// telemetry-only logger on a metered LTE link.
        #[serde(default)]
        audio_formats: Option<Vec<AudioFormat>>,
    },
    #[serde(rename = "room.join", alias = "room_join")]
    RoomJoin { room_id: String },
    #[serde(rename = "room.leave", alias = "room_leave")]
    RoomLeave,
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
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "ecorun_logger_server=info".into()),
        )
        .init();

    let bind = std::env::var("ECORUN_BIND").unwrap_or_else(|_| DEFAULT_BIND.into());
    let socket_state = socketio_api::SocketState::new();
    let rooms = Arc::new(RoomManager::new());
    let state = AppState {
        podium: Arc::new(PodiumHub::from_env(rooms.clone())),
        rooms,
    };
    let routes = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/servers", get(server_list))
        .route("/ws/audio/{room_id}", get(audio_ws))
        .route("/", get(web_index))
        .route("/manifest.webmanifest", get(web_manifest))
        .route("/icon.svg", get(web_icon))
        .route("/icon-192.png", get(web_icon_192))
        .route("/icon-512.png", get(web_icon_512))
        .route("/service-worker.js", get(web_service_worker))
        .route("/_app/{*path}", get(web_asset));

    let redis_enabled = std::env::var("ECORUN_REDIS_ENABLED")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    if redis_enabled {
        let redis_url =
            std::env::var("ECORUN_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
        let prefix =
            std::env::var("ECORUN_REDIS_NAMESPACE").unwrap_or_else(|_| "ecorun-logger".into());
        let client = redis::Client::open(redis_url).context("invalid ECORUN_REDIS_URL")?;
        let (layer, io) = SocketIo::builder()
            .with_state(socket_state)
            .with_adapter::<RedisAdapter<_>>(
                RedisAdapterCtr::new_with_redis_config(
                    &client,
                    RedisAdapterConfig::new().with_prefix(prefix),
                )
                .await
                .context("failed to configure Socket.IO Redis adapter")?,
            )
            .build_layer();
        serve_app(routes, state, layer, io, bind).await?;
    } else {
        let (layer, io) = SocketIo::builder().with_state(socket_state).build_layer();
        serve_app(routes, state, layer, io, bind).await?;
    }
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
    if room_id.is_empty() || room_id.len() > 64 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    ws.on_upgrade(move |socket| handle_audio_socket(socket, state, room_id))
        .into_response()
}

async fn handle_audio_socket(mut socket: WebSocket, state: AppState, initial_room_id: String) {
    let session_id = state.rooms.new_session_id();
    let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<u8>>(AUDIO_QUEUE_CAPACITY);
    let (control_tx, mut control_rx) = mpsc::channel::<String>(CONTROL_QUEUE_CAPACITY);
    let mut room_id: Option<String> = Some(initial_room_id.clone());
    let mut receive_audio = true;
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
                        Ok(ControlEvent::Hello { client_id, protocol, audio_formats }) => {
                            receive_audio = audio_formats
                                .map_or(true, |formats| formats.iter().any(AudioFormat::is_server_format));
                            info!(%session_id, ?client_id, ?protocol, receive_audio, "client hello");
                        }
                        Ok(ControlEvent::RoomJoin { room_id: requested }) => {
                            if requested.is_empty() || requested.len() > 64 {
                                let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                    code: "invalid_room", message: "room_id must be 1..64 characters",
                                }).unwrap());
                                continue;
                            }
                            let previous = room_id.replace(requested.clone());
                            if let Some(previous) = previous.as_deref() {
                                state.rooms.leave(&previous, session_id).await;
                                let participants = state.rooms.participant_states(previous).await;
                                let state_event = serde_json::to_string(&ServerEvent::RoomState {
                                    room_id: previous,
                                    participants,
                                })
                                .unwrap();
                                state.rooms.broadcast_control(previous, state_event).await;
                            }
                            state.rooms.join(&requested, session_id, audio_tx.clone(), control_tx.clone()).await;
                            let participants = state.rooms.participant_states(&requested).await;
                            let state_event = serde_json::to_string(&ServerEvent::RoomState {
                                room_id: &requested, participants,
                            }).unwrap();
                            state.rooms.broadcast_control(&requested, state_event).await;
                        }
                        Ok(ControlEvent::RoomLeave) => {
                            if let Some(previous) = room_id.take() {
                                state.rooms.leave(&previous, session_id).await;
                                let participants = state.rooms.participant_states(&previous).await;
                                let state_event = serde_json::to_string(&ServerEvent::RoomState {
                                    room_id: &previous,
                                    participants,
                                }).unwrap();
                                state.rooms.broadcast_control(&previous, state_event).await;
                            }
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
                                if let Err(error) = state.podium.meta(room, meta).await {
                                    let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                        code: error.code(), message: error.message(),
                                    }).unwrap());
                                }
                            }
                        }
                        Ok(ControlEvent::PodiumSamples { samples }) => {
                            if let Some(room) = room_id.as_deref() {
                                if let Err(error) = state.podium.samples(room, samples).await {
                                    let _ = control_tx.try_send(serde_json::to_string(&ServerEvent::Error {
                                        code: error.code(), message: error.message(),
                                    }).unwrap());
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
    info!(%session_id, "audio session disconnected");
}
