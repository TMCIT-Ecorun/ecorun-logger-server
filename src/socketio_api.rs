use std::{collections::HashMap, sync::Arc, time::SystemTime};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use socketioxide::{
    adapter::Adapter,
    extract::{Data, SocketRef, State, TryData},
    socket::DisconnectReason,
    SocketIo,
};
use socketioxide_core::adapter::DefinedAdapter;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::audio_delivery::AudioDelivery;
use crate::cluster::{Cluster, ClusterEvent, ClusterKind};
use crate::config::RoomStore;
use crate::map::{MapConfig, MapStore};
use crate::mixer::{MixHub, SourceId};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct SocketState {
    participants: Arc<RwLock<HashMap<String, HashMap<String, Participant>>>>,
    pub map: Arc<MapStore>,
    pub rooms: Arc<RoomStore>,
    telemetry: Arc<TelemetryStore>,
    delivery: Arc<AudioDelivery>,
    cluster: Cluster,
    telemetry_updates: broadcast::Sender<TelemetryEvent>,
    mix: Arc<MixHub>,
    raw_audio_updates: broadcast::Sender<(String, String, u64, Vec<u8>)>,
}

#[derive(Deserialize)]
struct ClusterAuth {
    #[serde(default)]
    token: String,
    #[serde(default)]
    node_id: String,
}

#[derive(Clone)]
pub struct TelemetryStore {
    files: Arc<crate::telemetry_files::TelemetryFiles>,
    recent: Arc<RwLock<HashMap<String, Vec<TelemetryEvent>>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    #[serde(default)]
    pub event_id: Option<String>,
    pub room_id: String,
    pub source: String,
    pub timestamp: Option<String>,
    pub received_at: String,
    pub data: serde_json::Value,
}

impl TelemetryEvent {
    pub fn new(
        room_id: &str,
        source: String,
        timestamp: Option<String>,
        data: serde_json::Value,
    ) -> Self {
        Self {
            event_id: None,
            room_id: room_id.to_owned(),
            source,
            timestamp,
            received_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis().to_string())
                .unwrap_or_else(|_| "0".into()),
            data,
        }
    }
}

impl TelemetryStore {
    pub fn new() -> Self {
        Self {
            files: Arc::new(crate::telemetry_files::TelemetryFiles::from_env()),
            recent: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn append(&self, event: TelemetryEvent) {
        {
            let mut recent = self.recent.write().await;
            let entries = recent.entry(event.room_id.clone()).or_default();
            entries.push(event.clone());
            if entries.len() > 500 {
                let drain = entries.len() - 500;
                entries.drain(..drain);
            }
        }
        self.files.append(&event).await;
    }
}

#[derive(Clone, Serialize)]
struct Participant {
    session_id: String,
    client_id: Option<String>,
    muted: bool,
    ptt: bool,
    forward_to_logger: bool,
    receive_bitrate: u32,
    receive_frame_ms: u16,
}

#[derive(Deserialize)]
struct BoolPayload {
    muted: bool,
}
#[derive(Deserialize)]
struct PttPayload {
    active: bool,
}

#[derive(Deserialize)]
struct TelemetryPayload {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct IdentifyPayload {
    client_id: String,
}

#[derive(Deserialize)]
struct AudioPacketPayload {
    client_id: String,
    sequence: u64,
    packet: Bytes,
}

#[derive(Deserialize)]
struct ForwardPayload {
    enabled: bool,
}

#[derive(Deserialize)]
struct ReceiveConfigPayload {
    bitrate: u32,
    frame_ms: u16,
}

impl SocketState {
    pub fn new(
        map: Arc<MapStore>,
        rooms: Arc<RoomStore>,
        telemetry: Arc<TelemetryStore>,
        delivery: Arc<AudioDelivery>,
        cluster: Cluster,
        telemetry_updates: broadcast::Sender<TelemetryEvent>,
        mix: Arc<MixHub>,
        raw_audio_updates: broadcast::Sender<(String, String, u64, Vec<u8>)>,
    ) -> Self {
        Self {
            participants: Arc::new(RwLock::new(HashMap::new())),
            map,
            rooms,
            telemetry,
            delivery,
            cluster,
            telemetry_updates,
            mix,
            raw_audio_updates,
        }
    }
}

pub async fn register<A: Adapter + DefinedAdapter>(io: &SocketIo<A>) -> anyhow::Result<()> {
    io.ns("/", on_root::<A>);
    io.ns("/_cluster", on_cluster::<A>);
    io.dyn_ns("/{room_id}", on_room::<A>)
        .map_err(|error| anyhow::anyhow!("invalid room namespace pattern: {error}"))?;
    Ok(())
}

pub async fn deliver_audio_to_recipients<A: Adapter>(
    recipients: Vec<SocketRef<A>>,
    state: &SocketState,
    room: &str,
    source: &str,
    sequence: u64,
    packet: &[u8],
) {
    let samples = match state.delivery.decode_packet(source, packet).await {
        Ok(samples) => samples,
        Err(error) => {
            warn!(%error, source, "invalid incoming browser audio");
            return;
        }
    };
    let settings: Vec<_> = {
        let all = state.participants.read().await;
        recipients
            .into_iter()
            .filter_map(|recipient| {
                let id = recipient.id.to_string();
                let p = all.get(room)?.get(&id)?;
                (p.client_id.as_deref() != Some(source)).then_some((
                    recipient,
                    id,
                    p.receive_bitrate,
                    p.receive_frame_ms,
                ))
            })
            .collect()
    };
    for (recipient, id, bitrate, frame_ms) in settings {
        match state
            .delivery
            .encode_for(&id, source, &samples, bitrate, frame_ms)
            .await
        {
            Ok(packets) => {
                for (index, output) in packets.into_iter().enumerate() {
                    recipient
                        .emit(
                            "audio.packet",
                            &(
                                source,
                                sequence.wrapping_mul(4).wrapping_add(index as u64),
                                Bytes::from(output),
                            ),
                        )
                        .ok();
                }
            }
            Err(error) => warn!(%error, "failed to transcode browser audio"),
        }
    }
}

pub async fn accept_cluster_telemetry(state: &SocketState, event: TelemetryEvent) {
    let Some(event_id) = event.event_id.as_deref() else {
        return;
    };
    if !state.rooms.exists(&event.room_id).unwrap_or(false) {
        return;
    }
    match state.map.append_replicated_telemetry(
        event_id,
        &event.room_id,
        &event.source,
        event.timestamp.as_deref(),
        &event.data,
    ) {
        Ok(true) => {
            state.telemetry.append(event.clone()).await;
            let _ = state.telemetry_updates.send(event);
        }
        Ok(false) => {}
        Err(error) => warn!(%error, "failed to store replicated telemetry"),
    }
}

async fn record_local_telemetry(
    state: &SocketState,
    room: &str,
    source: String,
    timestamp: Option<String>,
    data: serde_json::Value,
) {
    let mut event = TelemetryEvent::new(room, source, timestamp, data);
    let envelope = state.cluster.event(ClusterKind::Telemetry {
        event: event.clone(),
    });
    let event_id = state.cluster.event_id(envelope.sequence);
    event.event_id = Some(event_id.clone());
    match state.map.append_replicated_telemetry(
        &event_id,
        room,
        &event.source,
        event.timestamp.as_deref(),
        &event.data,
    ) {
        Ok(true) => {
            state.cluster.publish(ClusterEvent {
                kind: ClusterKind::Telemetry {
                    event: event.clone(),
                },
                ..envelope
            });
            state.telemetry.append(event.clone()).await;
            let _ = state.telemetry_updates.send(event);
        }
        Ok(false) => {}
        Err(error) => warn!(%error, "failed to persist Socket.IO telemetry"),
    }
}

async fn stop_browser_audio(state: &SocketState, room: &str, id: &str, client_id: Option<&str>) {
    state
        .mix
        .remove_source(room, &SourceId::Socket(id.to_owned()));
    if let Some(source) = client_id {
        state.delivery.forget_source(source).await;
        state
            .cluster
            .publish(state.cluster.event(ClusterKind::AudioStop {
                room: room.to_owned(),
                source: source.to_owned(),
            }));
    }
}

pub async fn handle_cluster_event<A: Adapter>(
    io: &SocketIo<A>,
    state: &SocketState,
    envelope: ClusterEvent,
) {
    match envelope.kind {
        ClusterKind::Telemetry { event } => accept_cluster_telemetry(state, event).await,
        ClusterKind::Map { room, map } => {
            if state.rooms.exists(&room).unwrap_or(false) && state.map.save_map(&room, &map).is_ok()
            {
                if let Some(namespace) = io.of(format!("/{room}")) {
                    let _ = namespace.to(room).emit("map.state", &map).await;
                }
            }
        }
        ClusterKind::Audio {
            room,
            source,
            sequence,
            packet,
            forward_to_logger,
        } => {
            if packet.is_empty()
                || packet.len() > crate::audio::MAX_PACKET_SIZE
                || !state.rooms.exists(&room).unwrap_or(false)
            {
                return;
            }
            if forward_to_logger && !source.starts_with("raw:") {
                state.mix.push(
                    &room,
                    SourceId::Socket(format!("remote:{}:{source}", envelope.origin)),
                    &packet,
                );
            }
            if let Some(namespace) = io.of(format!("/{room}")) {
                deliver_audio_to_recipients(
                    namespace.to(room.clone()).sockets(),
                    state,
                    &room,
                    &source,
                    sequence,
                    &packet,
                )
                .await;
            }
        }
        ClusterKind::AudioStop { room, source } => {
            state.mix.remove_source(
                &room,
                &SourceId::Socket(format!("remote:{}:{source}", envelope.origin)),
            );

            state.delivery.forget_source(&source).await;
        }
    }
}

async fn on_root<A: Adapter>(socket: SocketRef<A>) {
    socket.emit("error", &serde_json::json!({"code":"room_required","message":"Connect to a /{room_id} namespace"})).ok();
}

async fn on_cluster<A: Adapter>(
    socket: SocketRef<A>,
    State(state): State<SocketState>,
    TryData(auth): TryData<ClusterAuth>,
) {
    let Ok(auth) = auth else {
        socket.disconnect().ok();
        return;
    };
    if !state.cluster.token_is_valid(&auth.token)
        || auth.node_id.is_empty()
        || auth.node_id == state.cluster.node_id()
    {
        socket.disconnect().ok();
        return;
    }
    let peer_id = auth.node_id.clone();
    let sends_live = state.cluster.node_id() > peer_id.as_str();
    if sends_live {
        let mut outgoing = state.cluster.subscribe_outbound();
        let peer_socket = socket.clone();
        tokio::spawn(async move {
            loop {
                match outgoing.recv().await {
                    Ok(event) => {
                        if peer_socket.emit("cluster.event", &event).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        warn!(count, "cluster outbound queue overflow")
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    for room in state.rooms.list().unwrap_or_default() {
        socket
            .emit(
                "cluster.sync",
                &serde_json::json!({"room":room.id,"after":0}),
            )
            .ok();
    }
    socket.on(
        "cluster.event",
        move |State(state): State<SocketState>, Data(event): Data<ClusterEvent>| {
            let peer_id = peer_id.clone();
            async move {
                if event.origin == peer_id && event.sequence > 0 {
                    state.cluster.accept(event);
                }
            }
        },
    );
    socket.on(
        "cluster.sync.progress",
        |socket: SocketRef<A>, Data(progress): Data<serde_json::Value>| async move {
            if progress.get("more").and_then(|v| v.as_bool()) != Some(true) {
                return;
            }
            if let (Some(room), Some(after)) = (
                progress.get("room").and_then(|v| v.as_str()),
                progress.get("after").and_then(|v| v.as_i64()),
            ) {
                socket
                    .emit(
                        "cluster.sync",
                        &serde_json::json!({"room":room,"after":after}),
                    )
                    .ok();
            }
        },
    );
    socket.on(
        "cluster.sync",
        |socket: SocketRef<A>,
         State(state): State<SocketState>,
         Data(request): Data<serde_json::Value>| async move {
            let Some(room) = request.get("room").and_then(|value| value.as_str()) else {
                return;
            };
            let after = request
                .get("after")
                .and_then(|value| value.as_i64())
                .unwrap_or(0)
                .max(0);
            if room.len() > 64 || !state.rooms.exists(room).unwrap_or(false) {
                return;
            }
            if after == 0 && state.map.has_map(room).unwrap_or(false) {
                if let Ok(map) = state.map.get_map(room) {
                    let envelope = state.cluster.event(ClusterKind::Map {
                        room: room.to_owned(),
                        map,
                    });
                    socket.emit("cluster.event", &envelope).ok();
                }
            }
            match state.map.replicated_telemetry_since(room, after, 100) {
                Ok(rows) => {
                    let count = rows.len();
                    let mut cursor = after;
                    for row in rows {
                        cursor = row.id;
                        let event = TelemetryEvent {
                            event_id: row.event_id,
                            room_id: row.room_id,
                            source: row.source,
                            timestamp: row.timestamp,
                            received_at: row.received_at,
                            data: row.data,
                        };
                        let envelope = ClusterEvent {
                            origin: state.cluster.node_id().to_owned(),
                            sequence: cursor as u64,
                            kind: ClusterKind::Telemetry { event },
                        };
                        socket.emit("cluster.event", &envelope).ok();
                    }
                    socket
                        .emit(
                            "cluster.sync.progress",
                            &serde_json::json!({"room":room,"after":cursor,"more":count == 100}),
                        )
                        .ok();
                }
                Err(error) => warn!(%error, room, "cluster telemetry sync failed"),
            }
        },
    );
}

async fn on_room<A: Adapter>(socket: SocketRef<A>, State(state): State<SocketState>) {
    let room = socket.ns().trim_start_matches('/').to_owned();
    if room.is_empty()
        || room.len() > 64
        || room.contains('/')
        || !state.rooms.exists(&room).unwrap_or(false)
    {
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
                client_id: None,
                muted: false,
                ptt: false,
                forward_to_logger: true,
                receive_bitrate: 8000,
                receive_frame_ms: 60,
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
            &serde_json::json!({"sample_rate":8000,"channels":1,"frame_ms":60,"min_bitrate":6000,"max_bitrate":32000,"default_bitrate":8000}),
        )
        .ok();
    socket
        .emit(
            "room.state",
            &serde_json::json!({"room_id":room,"participants":participants}),
        )
        .ok();
    if let Ok(map) = state.map.get_map(&room) {
        socket.emit("map.state", &map).ok();
    }
    if let Ok(rows) = state.map.recent_telemetry(&room, 500) {
        let events: Vec<_> = rows
            .into_iter()
            .rev()
            .map(|row| TelemetryEvent {
                event_id: row.event_id,
                room_id: row.room_id,
                source: row.source,
                timestamp: row.timestamp,
                received_at: row.received_at,
                data: row.data,
            })
            .collect();
        socket.emit("telemetry.state", &events).ok();
    }
    if let Err(error) = socket
        .to(room.clone())
        .emit(
            "participant.joined",
            &Participant {
                session_id: session_id.clone(),
                client_id: None,
                muted: false,
                ptt: false,
                forward_to_logger: true,
                receive_bitrate: 8000,
                receive_frame_ms: 60,
            },
        )
        .await
    {
        warn!(%error, "failed to broadcast participant join");
    }

    socket.on(
        "client.identify",
        move |socket: SocketRef<A>,
              State(state): State<SocketState>,
              Data(payload): Data<IdentifyPayload>| async move {
            if payload.client_id.is_empty() || payload.client_id.len() > 128 {
                return;
            }
            let room = socket.ns().trim_start_matches('/').to_owned();
            let id = socket.id.to_string();
            let update = {
                let mut all = state.participants.write().await;
                all.get_mut(&room)
                    .and_then(|members| members.get_mut(&id))
                    .map(|participant| {
                        participant.client_id = Some(payload.client_id.clone());
                        participant.clone()
                    })
            };
            if let Some(participant) = update {
                socket.emit("participant.updated", &participant).ok();
                let _ = socket
                    .to(room)
                    .emit("participant.updated", &participant)
                    .await;
            }
        },
    );

    socket.on(
        "telemetry.request",
        move |socket: SocketRef<A>, State(state): State<SocketState>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            if let Ok(rows) = state.map.recent_telemetry(&room, 500) {
                let events: Vec<_> = rows
                    .into_iter()
                    .rev()
                    .map(|row| TelemetryEvent {
                        event_id: row.event_id,
                        room_id: row.room_id,
                        source: row.source,
                        timestamp: row.timestamp,
                        received_at: row.received_at,
                        data: row.data,
                    })
                    .collect();
                socket.emit("telemetry.state", &events).ok();
            }
        },
    );

    socket.on(
        "map.request",
        move |socket: SocketRef<A>, State(state): State<SocketState>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            if let Ok(map) = state.map.get_map(&room) {
                socket.emit("map.state", &map).ok();
            }
        },
    );

    socket.on(
        "telemetry.ingest",
        move |socket: SocketRef<A>,
              State(state): State<SocketState>,
              Data(payload): Data<serde_json::Value>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            let source = payload
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or("logger")
                .to_owned();
            let timestamp = payload
                .get("timestamp")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned);
            let data = payload
                .get("data")
                .cloned()
                .unwrap_or_else(|| payload.clone());
            record_local_telemetry(&state, &room, source, timestamp, data).await;
        },
    );

    socket.on(
        "audio.forward_logger",
        |socket: SocketRef<A>,
         State(state): State<SocketState>,
         Data(payload): Data<ForwardPayload>| async move {
            let room = socket.ns().trim_start_matches('/');
            let id = socket.id.to_string();
            if let Some(participant) = state
                .participants
                .write()
                .await
                .get_mut(room)
                .and_then(|members| members.get_mut(&id))
            {
                participant.forward_to_logger = payload.enabled;
                if !payload.enabled {
                    state.mix.remove_source(room, &SourceId::Socket(id.clone()));
                    if let Some(source) = participant.client_id.clone() {
                        state.delivery.forget_source(&source).await;
                        state
                            .cluster
                            .publish(state.cluster.event(ClusterKind::AudioStop {
                                room: room.to_owned(),
                                source,
                            }));
                    }
                }
            }
        },
    );
    socket.on(
        "audio.receive_config",
        |socket: SocketRef<A>, State(state): State<SocketState>, Data(payload): Data<ReceiveConfigPayload>| async move {
            if !(4000..=32000).contains(&payload.bitrate) || ![20,40,60,120].contains(&payload.frame_ms) {
                socket.emit("error", &serde_json::json!({"code":"invalid_audio_config","message":"bitrate 4000..32000; frame_ms 20/40/60/120"})).ok();
                return;
            }
            let room = socket.ns().trim_start_matches('/');
            if let Some(participant) = state.participants.write().await.get_mut(room).and_then(|members| members.get_mut(&socket.id.to_string())) {
                participant.receive_bitrate = payload.bitrate;
                participant.receive_frame_ms = payload.frame_ms;
            }
        },
    );
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
                if participant.muted {
                    stop_browser_audio(&state, &room, &id, participant.client_id.as_deref()).await;
                }
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
                if !participant.ptt {
                    stop_browser_audio(&state, &room, &id, participant.client_id.as_deref()).await;
                }
                let _ = socket
                    .to(room)
                    .emit("participant.updated", &participant)
                    .await;
            }
        },
    );

    socket.on(
        "audio.packet",
        move |socket: SocketRef<A>,
              State(state): State<SocketState>,
              Data(payload): Data<AudioPacketPayload>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            let id = socket.id.to_string();
            let participant = state
                .participants
                .read()
                .await
                .get(&room)
                .and_then(|members| members.get(&id))
                .cloned();
            if participant.as_ref().is_some_and(|p| {
                !p.muted && p.ptt && p.client_id.as_deref() == Some(payload.client_id.as_str())
            }) {
                let packet = payload.packet.to_vec();
                if packet.is_empty() || packet.len() > crate::audio::MAX_PACKET_SIZE {
                    return;
                }
                if participant.as_ref().is_some_and(|p| p.forward_to_logger) {
                    state.mix.push(&room, SourceId::Socket(id.clone()), &packet);
                }
                deliver_audio_to_recipients(
                    socket.to(room.clone()).sockets(),
                    &state,
                    &room,
                    &payload.client_id,
                    payload.sequence,
                    &packet,
                )
                .await;
                let _ = state.raw_audio_updates.send((
                    room.clone(),
                    payload.client_id.clone(),
                    payload.sequence,
                    packet.clone(),
                ));
                state
                    .cluster
                    .publish(state.cluster.event(ClusterKind::Audio {
                        room,
                        source: payload.client_id,
                        sequence: payload.sequence,
                        packet,
                        forward_to_logger:
                            participant.as_ref().is_some_and(|p| p.forward_to_logger),
                    }));
            }
        },
    );

    socket.on(
        "map.update",
        move |socket: SocketRef<A>,
              State(state): State<SocketState>,
              Data(map): Data<MapConfig>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            if !state.rooms.exists(&room).unwrap_or(false)
                || !map.center_lat.is_finite()
                || !map.center_lon.is_finite()
                || !map.zoom.is_finite()
                || map.zoom < 0.0
                || map.zoom > 24.0
            {
                return;
            }
            if state.map.save_map(&room, &map).is_ok() {
                state.cluster.publish(state.cluster.event(ClusterKind::Map {
                    room: room.clone(),
                    map: map.clone(),
                }));
                let _ = socket.to(room).emit("map.state", &map).await;
            }
        },
    );

    socket.on(
        "telemetry",
        move |socket: SocketRef<A>,
              State(state): State<SocketState>,
              Data(payload): Data<TelemetryPayload>| async move {
            let room = socket.ns().trim_start_matches('/').to_owned();
            if !state.rooms.exists(&room).unwrap_or(false) {
                return;
            }
            let source = payload.source.unwrap_or_else(|| "web-client".into());
            let data = payload.data.unwrap_or_else(|| serde_json::json!({}));
            record_local_telemetry(&state, &room, source, payload.timestamp, data).await;
        },
    );

    // Dedicated replication event is persisted but never forwarded again.
    socket.on("telemetry.replica", move |socket: SocketRef<A>, State(state): State<SocketState>, Data(payload): Data<TelemetryPayload>| async move {
        let room = socket.ns().trim_start_matches('/').to_owned();
        let source = payload.source.unwrap_or_else(|| "replica".into());
        let data = payload.data.unwrap_or_else(|| serde_json::json!({}));
        match state.map.append_telemetry(&room, &source, payload.timestamp.as_deref(), &data) {
            Ok(id) => {
                let event = serde_json::json!({"type":"telemetry","id":id,"source":source,"timestamp":payload.timestamp,"data":data});
                let _ = socket.to(room).emit("telemetry", &event).await;
            }
            Err(error) => warn!(%error, "failed to persist replicated telemetry"),
        }
    });

    socket.on_disconnect(move |socket: SocketRef<A>, _reason: DisconnectReason, State(state): State<SocketState>| async move {
        let room = socket.ns().trim_start_matches('/').to_owned();
        let id = socket.id.to_string();
        let mut all = state.participants.write().await;
        let client_id = all
            .get(&room)
            .and_then(|members| members.get(&id))
            .and_then(|participant| participant.client_id.clone());
        if let Some(members) = all.get_mut(&room) {
            members.remove(&id);
            if members.is_empty() { all.remove(&room); }
        }
        drop(all);
        stop_browser_audio(&state, &room, &id, client_id.as_deref()).await;
        state.delivery.forget_recipient(&id).await;
        let _ = socket
            .broadcast()
            .emit("participant.left", &serde_json::json!({"session_id":id,"client_id":client_id}))
            .await;
        info!(session_id=%id, %room, "Socket.IO participant disconnected");
    });
}
