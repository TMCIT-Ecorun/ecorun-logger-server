//! Redis-free Socket.IO node federation. Peers connect to a private namespace
//! over Engine.IO v4 websocket transport, sharing telemetry, map and audio.
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    map::{MapConfig, MapStore},
    socketio_api::TelemetryEvent,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClusterKind {
    Telemetry {
        event: TelemetryEvent,
    },
    Map {
        room: String,
        map: MapConfig,
    },
    Audio {
        room: String,
        source: String,
        sequence: u64,
        packet: Vec<u8>,
        forward_to_logger: bool,
    },
    AudioStop {
        room: String,
        source: String,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterEvent {
    pub origin: String,
    pub sequence: u64,
    #[serde(flatten)]
    pub kind: ClusterKind,
}

#[derive(Clone)]
pub struct Cluster {
    node_id: Arc<String>,
    boot_id: Arc<String>,
    secret: Arc<String>,
    peers: Arc<Vec<(String, String)>>,
    sequence: Arc<AtomicU64>,
    outbound: broadcast::Sender<ClusterEvent>,
    inbound: broadcast::Sender<ClusterEvent>,
}

#[derive(Deserialize)]
struct ConfiguredPeer {
    id: String,
    url: String,
    #[serde(default = "default_true")]
    available: bool,
}
fn default_true() -> bool {
    true
}

impl Cluster {
    pub fn from_env() -> anyhow::Result<Self> {
        let configured = std::env::var("ECORUN_SERVERS").unwrap_or_default();
        let configured: Vec<ConfiguredPeer> = if configured.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&configured).context("parse ECORUN_SERVERS")?
        };
        let node_id = std::env::var("ECORUN_NODE_ID").unwrap_or_else(|_| "local".to_string());
        let multi_node = configured
            .iter()
            .any(|p| p.available && p.id != node_id && !p.url.is_empty());
        // Each node initiates a connection to every peer, allowing both sides
        // to request missed history after either side has been offline.
        let peer_urls: Vec<_> = configured
            .into_iter()
            .filter(|p| p.available && p.id != node_id && !p.url.is_empty())
            .map(|p| (p.id, p.url))
            .collect();
        let secret = std::env::var("ECORUN_CLUSTER_TOKEN").unwrap_or_default();
        if multi_node && (node_id == "local" || secret.len() < 24) {
            return Err(anyhow!("ECORUN_NODE_ID and ECORUN_CLUSTER_TOKEN (24+ characters) are required for multi-node Socket.IO sync"));
        }
        let boot_id = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        let (outbound, _) = broadcast::channel(1024);
        let (inbound, _) = broadcast::channel(1024);
        Ok(Self {
            node_id: Arc::new(node_id),
            boot_id: Arc::new(boot_id),
            secret: Arc::new(secret),
            peers: Arc::new(peer_urls),
            sequence: Arc::new(AtomicU64::new(1)),
            outbound,
            inbound,
        })
    }

    pub fn enabled(&self) -> bool {
        !self.secret.is_empty()
    }
    pub fn token_is_valid(&self, token: &str) -> bool {
        // A shared cluster token must never be accepted when federation is disabled.
        if !self.enabled() || token.len() != self.secret.len() {
            return false;
        }
        // Token comparison does not short-circuit based on matching prefix.
        self.secret
            .bytes()
            .zip(token.bytes())
            .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
            == 0
    }
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
    pub fn event_id(&self, sequence: u64) -> String {
        format!("{}:{}:{sequence}", self.node_id, self.boot_id)
    }
    pub fn subscribe_inbound(&self) -> broadcast::Receiver<ClusterEvent> {
        self.inbound.subscribe()
    }
    pub fn subscribe_outbound(&self) -> broadcast::Receiver<ClusterEvent> {
        self.outbound.subscribe()
    }
    pub fn event(&self, kind: ClusterKind) -> ClusterEvent {
        ClusterEvent {
            origin: (*self.node_id).clone(),
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            kind,
        }
    }
    pub fn publish(&self, event: ClusterEvent) {
        if !self.peers.is_empty() {
            let _ = self.outbound.send(event);
        }
    }
    pub fn accept(&self, event: ClusterEvent) {
        if event.origin != *self.node_id && !event.origin.is_empty() {
            let _ = self.inbound.send(event);
        }
    }
    pub fn run(&self, rooms: Vec<String>, map: Arc<MapStore>) {
        for (peer_id, peer) in self.peers.iter() {
            let peer = peer.clone();
            let peer_id = peer_id.clone();
            let self_clone = self.clone();
            let rooms = rooms.clone();
            let map = map.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(error) = self_clone.connect_peer(&peer, &peer_id, &rooms, &map).await
                    {
                        warn!(peer, %error, "cluster Socket.IO peer unavailable");
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            });
        }
    }

    async fn connect_peer(
        &self,
        peer: &str,
        peer_id: &str,
        rooms: &[String],
        map: &MapStore,
    ) -> anyhow::Result<()> {
        let ws = if let Some(rest) = peer.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = peer.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            return Err(anyhow!("peer must be an http(s) URL"));
        };
        let url = format!(
            "{}/socket.io/?EIO=4&transport=websocket",
            ws.trim_end_matches('/')
        );
        let (mut socket, _) = connect_async(&url)
            .await
            .context("open Socket.IO websocket")?;
        let mut outgoing = self.outbound.subscribe();
        let sends_live = self.node_id() < peer_id;
        let mut joined = false;
        loop {
            tokio::select! {
                message = socket.next() => {
                    let Some(message) = message else { break; };
                    let message = message.context("peer websocket closed")?;
                    let Message::Text(text) = message else { continue; };
                    let text = text.as_str();
                    if text.starts_with('0') {
                        let join = format!("40/_cluster,{}", serde_json::json!({"token":self.secret.as_str(),"node_id":self.node_id()}));
                        socket.send(Message::Text(join.into())).await?;
                    } else if text.starts_with("40/_cluster,") {
                        joined = true;
                        info!(peer, "cluster Socket.IO connected");
                        for room in rooms { send_event(&mut socket,"cluster.sync", &serde_json::json!({"room":room,"after":0})).await?; }
                    } else if text == "2" {
                        socket.send(Message::Text("3".into())).await?;
                    } else if text.starts_with("42/_cluster,") {
                        if let Ok(value) = serde_json::from_str::<Vec<serde_json::Value>>(&text[12..]) {
                            match value.first().and_then(|v| v.as_str()) {
                                Some("cluster.event") => {
                                    if let Some(event) = value.get(1).and_then(|v| serde_json::from_value::<ClusterEvent>(v.clone()).ok()) {
                                        self.accept(event);
                                    }
                                }
                                Some("cluster.sync.progress") => {
                                    if let Some(progress) = value.get(1) {
                                        if progress.get("more").and_then(|v| v.as_bool()) == Some(true) {
                                            if let (Some(room),Some(after)) = (progress.get("room").and_then(|v|v.as_str()),progress.get("after").and_then(|v|v.as_i64())) {
                                                send_event(&mut socket,"cluster.sync", &serde_json::json!({"room":room,"after":after})).await?;
                                            }
                                        }
                                    }
                                }
                                Some("cluster.sync") => {
                                    if let Some(request) = value.get(1) {
                                        if let (Some(room), Some(after)) = (request.get("room").and_then(|v| v.as_str()), request.get("after").and_then(|v| v.as_i64())) {
                                            if rooms.iter().any(|configured| configured == room) {
                                                self.send_replay(&mut socket, map, room, after.max(0)).await?;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    } else if text.starts_with("44/_cluster,") {
                        return Err(anyhow!("Socket.IO cluster namespace refused peer"));
                    }
                }
                event = outgoing.recv(), if joined && sends_live => {
                    match event {
                        Ok(event) => send_event(&mut socket,"cluster.event", &event).await?,
                        Err(broadcast::error::RecvError::Lagged(count)) => warn!(count, peer, "cluster outbound queue overflow"),
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
        Err(anyhow!("cluster peer websocket disconnected"))
    }

    async fn send_replay<S>(
        &self,
        socket: &mut tokio_tungstenite::WebSocketStream<S>,
        map: &MapStore,
        room: &str,
        after: i64,
    ) -> anyhow::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let rows = map.replicated_telemetry_since(room, after, 100)?;
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
            send_event(
                socket,
                "cluster.event",
                &ClusterEvent {
                    origin: self.node_id().to_owned(),
                    sequence: cursor as u64,
                    kind: ClusterKind::Telemetry { event },
                },
            )
            .await?;
        }
        send_event(
            socket,
            "cluster.sync.progress",
            &serde_json::json!({"room":room,"after":cursor,"more":count == 100}),
        )
        .await?;
        Ok(())
    }
}

async fn send_event<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    name: &str,
    data: &impl Serialize,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let json = serde_json::to_string(&(name, data))?;
    socket
        .send(Message::Text(format!("42/_cluster,{json}").into()))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn peer_event_prefix_is_correct() {
        let payload = "42/_cluster,[\"cluster.event\",{}]";
        assert_eq!(&payload[12..], "[\"cluster.event\",{}]");
    }
}
