//! Best-effort outbound Socket.IO replication. Each room/peer has one connection.
use crate::config::RoomStore;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;

#[derive(Deserialize)]
struct Peer {
    id: String,
    url: String,
    #[serde(default)]
    available: bool,
}

#[derive(Clone, Default)]
pub struct Replicator {
    rooms: Arc<HashMap<String, Vec<mpsc::Sender<Value>>>>,
}

impl Replicator {
    pub fn start(rooms: &RoomStore) -> Self {
        let raw = std::env::var("ECORUN_SERVERS").unwrap_or_default();
        let peers: Vec<Peer> = match serde_json::from_str(&raw) {
            Ok(peers) => peers,
            Err(error) => {
                warn!(%error, "invalid ECORUN_SERVERS for replication");
                return Self::default();
            }
        };
        let self_id = std::env::var("ECORUN_SERVER_ID").unwrap_or_default();
        let mut channels = HashMap::new();
        if let Ok(configs) = rooms.list() {
            for room in configs {
                let mut senders = Vec::new();
                for peer in &peers {
                    if peer.id == self_id || peer.url.trim().is_empty() || !peer.available {
                        continue;
                    }
                    let (tx, rx) = mpsc::channel(512);
                    tokio::spawn(run_peer(
                        peer.url.clone(),
                        room.id.clone(),
                        room.socket_key.clone(),
                        rx,
                    ));
                    senders.push(tx);
                }
                channels.insert(room.id, senders);
            }
        }
        Self {
            rooms: Arc::new(channels),
        }
    }

    pub fn forward(&self, room: &str, source: &str, timestamp: Option<&str>, data: &Value) {
        if let Some(peers) = self.rooms.get(room) {
            let event = json!({"source": source, "timestamp": timestamp, "data": data});
            for peer in peers {
                if let Err(error) = peer.try_send(event.clone()) {
                    warn!(%room, %error, "replication queue full or closed; dropping telemetry");
                }
            }
        }
    }
}

async fn run_peer(base: String, room: String, key: String, mut rx: mpsc::Receiver<Value>) {
    let base = base
        .trim_end_matches('/')
        .replace("https://", "wss://")
        .replace("http://", "ws://");
    if !(base.starts_with("ws://") || base.starts_with("wss://")) {
        warn!(%base, "invalid replication peer URL");
        return;
    }
    let url = format!("{base}/socket.io/?EIO=4&transport=websocket");
    loop {
        match connect_async(&url).await {
            Ok((mut socket, _)) => {
                // Engine.IO open -> Socket.IO namespace handshake.
                let mut joined = false;
                while let Some(message) = socket.next().await {
                    let Ok(message) = message else {
                        break;
                    };
                    let Message::Text(frame) = message else {
                        continue;
                    };
                    if frame.starts_with('0') {
                        let auth = json!({"token": key});
                        if socket
                            .send(Message::Text(format!("40/{room},{auth}").into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    } else if frame.starts_with(&format!("40/{room},")) {
                        joined = true;
                        break;
                    } else if frame.starts_with('2') {
                        if socket.send(Message::Text("3".into())).await.is_err() {
                            break;
                        }
                    } else if frame.starts_with("44") {
                        break;
                    }
                }
                if joined {
                    loop {
                        tokio::select! {
                            next = rx.recv() => {
                                let Some(event) = next else { return; };
                                let packet = format!("42/{room},{}", json!(["telemetry.replica", event]));
                                if socket.send(Message::Text(packet.into())).await.is_err() { break; }
                            }
                            next = socket.next() => match next {
                                Some(Ok(Message::Text(frame))) if frame.starts_with('2') => {
                                    if socket.send(Message::Text("3".into())).await.is_err() { break; }
                                }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(error) => warn!(%url, %error, "replication peer connection failed"),
        }
        // Dropped packets while disconnected; live replication resumes on reconnection.
        while rx.try_recv().is_ok() {}
        sleep(Duration::from_secs(3)).await;
    }
}
