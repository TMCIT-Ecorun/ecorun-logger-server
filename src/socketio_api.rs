use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use socketioxide::{
    adapter::Adapter,
    extract::{Data, SocketRef, State},
    socket::DisconnectReason,
    SocketIo,
};
use socketioxide_core::adapter::DefinedAdapter;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Clone)]
pub struct SocketState {
    participants: Arc<RwLock<HashMap<String, HashMap<String, Participant>>>>,
}

#[derive(Clone, Serialize)]
struct Participant {
    session_id: String,
    muted: bool,
    ptt: bool,
}

#[derive(Deserialize)]
struct BoolPayload {
    muted: bool,
}
#[derive(Deserialize)]
struct PttPayload {
    active: bool,
}

impl SocketState {
    pub fn new() -> Self {
        Self {
            participants: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub async fn register<A: Adapter + DefinedAdapter>(io: &SocketIo<A>) -> anyhow::Result<()> {
    io.ns("/", on_root::<A>);
    io.dyn_ns("/{room_id}", on_room::<A>)
        .map_err(|error| anyhow::anyhow!("invalid room namespace pattern: {error}"))?;
    Ok(())
}

async fn on_root<A: Adapter>(socket: SocketRef<A>) {
    socket.emit("error", &serde_json::json!({"code":"room_required","message":"Connect to a /{room_id} namespace"})).ok();
}

async fn on_room<A: Adapter>(socket: SocketRef<A>, State(state): State<SocketState>) {
    let room = socket.ns().trim_start_matches('/').to_owned();
    if room.is_empty() || room.len() > 64 || room.contains('/') {
        socket.disconnect().ok();
        return;
    }
    let session_id = socket.id.to_string();
    socket.join(room.clone());
    {
        let mut all = state.participants.write().await;
        all.entry(room.clone()).or_default().insert(
            session_id.clone(),
            Participant {
                session_id: session_id.clone(),
                muted: false,
                ptt: false,
            },
        );
    }
    let participants = state
        .participants
        .read()
        .await
        .get(&room)
        .map(|p| p.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    socket
        .emit(
            "hello",
            &serde_json::json!({"protocol": 1, "session_id": session_id}),
        )
        .ok();
    socket
        .emit(
            "audio.config",
            &serde_json::json!({"sample_rate":8000,"channels":1,"frame_ms":60}),
        )
        .ok();
    socket
        .emit(
            "room.state",
            &serde_json::json!({"room_id":room,"participants":participants}),
        )
        .ok();
    if let Err(error) = socket
        .to(room.clone())
        .emit(
            "participant.joined",
            &Participant {
                session_id: session_id.clone(),
                muted: false,
                ptt: false,
            },
        )
        .await
    {
        warn!(%error, "failed to broadcast participant join");
    }

    socket.on(
        "audio.mute",
        move |socket: SocketRef<A>,
              State(state): State<SocketState>,
              Data(payload): Data<BoolPayload>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            let id = socket.id.to_string();
            let update = {
                let mut all = state.participants.write().await;
                all.get_mut(&room)
                    .and_then(|members| members.get_mut(&id))
                    .map(|participant| {
                        participant.muted = payload.muted;
                        if payload.muted {
                            participant.ptt = false;
                        }
                        participant.clone()
                    })
            };
            if let Some(participant) = update {
                let _ = socket
                    .to(room)
                    .emit("participant.updated", &participant)
                    .await;
            }
        },
    );

    socket.on(
        "audio.ptt",
        move |socket: SocketRef<A>,
              State(state): State<SocketState>,
              Data(payload): Data<PttPayload>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            let id = socket.id.to_string();
            let update = {
                let mut all = state.participants.write().await;
                all.get_mut(&room)
                    .and_then(|members| members.get_mut(&id))
                    .map(|participant| {
                        participant.ptt = payload.active && !participant.muted;
                        participant.clone()
                    })
            };
            if let Some(participant) = update {
                let _ = socket
                    .to(room)
                    .emit("participant.updated", &participant)
                    .await;
            }
        },
    );

    socket.on("audio.packet", move |socket: SocketRef<A>, State(state): State<SocketState>, Data(packet): Data<Bytes>| async move {
        let room = socket.ns().trim_start_matches('/').to_owned();
        let id = socket.id.to_string();
        let allowed = state.participants.read().await.get(&room).and_then(|members| members.get(&id)).map(|p| !p.muted && p.ptt).unwrap_or(false);
        if allowed {
            if let Err(error) = socket.to(room).emit("audio.packet", &packet).await {
                warn!(%error, "failed to fan out audio packet");
            }
        }
    });

    socket.on_disconnect(move |socket: SocketRef<A>, _reason: DisconnectReason, State(state): State<SocketState>| async move {
        let room = socket.ns().trim_start_matches('/').to_owned();
        let id = socket.id.to_string();
        let mut all = state.participants.write().await;
        if let Some(members) = all.get_mut(&room) {
            members.remove(&id);
            if members.is_empty() { all.remove(&room); }
        }
        drop(all);
        let _ = socket.broadcast().emit("participant.left", &serde_json::json!({"session_id":id})).await;
        info!(session_id=%id, %room, "Socket.IO participant disconnected");
    });
}
