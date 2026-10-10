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
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tracing::{info, warn};

mod audio;
mod room;

use audio::{AudioCodec, OpusCodec};
use room::{RoomManager, SessionId};

const DEFAULT_BIND: &str = "0.0.0.0:3000";
// At 60 ms per packet this bounds queued audio to 180 ms per receiver.
const AUDIO_QUEUE_CAPACITY: usize = 3;
const CONTROL_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
struct AppState {
    rooms: Arc<RoomManager>,
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
    },
    #[serde(rename = "room.join", alias = "room_join")]
    RoomJoin { room_id: String },
    #[serde(rename = "room.leave", alias = "room_leave")]
    RoomLeave,
    #[serde(rename = "audio.mute", alias = "mute")]
    AudioMute { muted: bool },
    #[serde(rename = "audio.ptt", alias = "ptt")]
    AudioPtt { active: bool },
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
    let state = AppState {
        rooms: Arc::new(RoomManager::new()),
    };
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/ws/audio/{room_id}", get(audio_ws))
        .route("/", get(web_index))
        .route("/manifest.webmanifest", get(web_manifest))
        .route("/icon.svg", get(web_icon))
        .route("/icon-192.png", get(web_icon_192))
        .route("/icon-512.png", get(web_icon_512))
        .route("/service-worker.js", get(web_service_worker))
        .route("/_app/{*path}", get(web_asset))
        .with_state(state);

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
    serve_web_file("web/build/service-worker.js", "text/javascript; charset=utf-8").await
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
    state.rooms.broadcast_control(&initial_room_id, state_event).await;

    loop {
        tokio::select! {
            outbound = audio_rx.recv() => match outbound {
                Some(packet) => if socket.send(Message::Binary(packet.into())).await.is_err() { break },
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
                        Ok(ControlEvent::Hello { client_id, protocol }) => {
                            info!(%session_id, ?client_id, ?protocol, "client hello");
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
