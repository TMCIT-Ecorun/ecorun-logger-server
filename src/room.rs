use serde::Serialize;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::{mpsc, RwLock};

type AudioSender = mpsc::Sender<Vec<u8>>;
type ControlSender = mpsc::Sender<String>;
pub type SessionId = u64;

struct Participant {
    audio_tx: AudioSender,
    #[allow(dead_code)]
    control_tx: ControlSender,
    muted: bool,
    ptt: bool,
    is_logger: bool,
    receive_audio: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ParticipantState {
    pub session_id: SessionId,
    pub muted: bool,
    pub ptt: bool,
}

pub struct RoomManager {
    next_session: AtomicU64,
    rooms: RwLock<HashMap<String, HashMap<SessionId, Participant>>>,
}

impl RoomManager {
    pub fn new() -> Self {
        Self {
            next_session: AtomicU64::new(1),
            rooms: RwLock::new(HashMap::new()),
        }
    }
    pub fn new_session_id(&self) -> SessionId {
        self.next_session.fetch_add(1, Ordering::Relaxed)
    }
    pub async fn join(
        &self,
        room_id: &str,
        session_id: SessionId,
        audio_tx: AudioSender,
        control_tx: ControlSender,
        is_logger: bool,
    ) {
        let mut rooms = self.rooms.write().await;
        rooms.entry(room_id.to_owned()).or_default().insert(
            session_id,
            Participant {
                audio_tx,
                control_tx,
                muted: false,
                ptt: false,
                is_logger,
                receive_audio: true,
            },
        );
    }
    pub async fn leave(&self, room_id: &str, session_id: SessionId) {
        let mut rooms = self.rooms.write().await;
        if let Some(room) = rooms.get_mut(room_id) {
            room.remove(&session_id);
            if room.is_empty() {
                rooms.remove(room_id);
            }
        }
    }
    pub async fn remove(&self, session_id: SessionId) {
        let mut rooms = self.rooms.write().await;
        for room in rooms.values_mut() {
            room.remove(&session_id);
        }
        rooms.retain(|_, room| !room.is_empty());
    }
    pub async fn participant_states(&self, room_id: &str) -> Vec<ParticipantState> {
        self.rooms
            .read()
            .await
            .get(room_id)
            .map(|room| {
                room.iter()
                    .map(|(&session_id, participant)| ParticipantState {
                        session_id,
                        muted: participant.muted,
                        ptt: participant.ptt,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    pub async fn set_mute(&self, room_id: &str, session_id: SessionId, muted: bool) -> bool {
        let mut rooms = self.rooms.write().await;
        let Some(participant) = rooms
            .get_mut(room_id)
            .and_then(|room| room.get_mut(&session_id))
        else {
            return false;
        };
        participant.muted = muted;
        if muted {
            participant.ptt = false;
        }
        true
    }
    pub async fn set_ptt(&self, room_id: &str, session_id: SessionId, ptt: bool) -> bool {
        let mut rooms = self.rooms.write().await;
        let Some(participant) = rooms
            .get_mut(room_id)
            .and_then(|room| room.get_mut(&session_id))
        else {
            return false;
        };
        participant.ptt = ptt && !participant.muted;
        true
    }
    pub async fn set_logger(&self, room_id: &str, session_id: SessionId, is_logger: bool) -> bool {
        let mut rooms = self.rooms.write().await;
        let Some(participant) = rooms
            .get_mut(room_id)
            .and_then(|room| room.get_mut(&session_id))
        else {
            return false;
        };
        participant.is_logger = is_logger;
        true
    }
    pub async fn set_receive_audio(
        &self,
        room_id: &str,
        session_id: SessionId,
        receive_audio: bool,
    ) -> bool {
        let mut rooms = self.rooms.write().await;
        let Some(participant) = rooms
            .get_mut(room_id)
            .and_then(|room| room.get_mut(&session_id))
        else {
            return false;
        };
        participant.receive_audio = receive_audio;
        true
    }
    pub async fn is_talking(&self, room_id: &str, session_id: SessionId) -> bool {
        self.rooms
            .read()
            .await
            .get(room_id)
            .and_then(|room| room.get(&session_id))
            .is_some_and(|participant| participant.ptt && !participant.muted)
    }
    /// The caller checks `is_talking` when the audio arrives: frames are paced
    /// out later and the tail of a sentence may go out after PTT went off.
    pub async fn broadcast_audio(&self, room_id: &str, sender: SessionId, packet: Vec<u8>) {
        let rooms = self.rooms.read().await;
        let Some(room) = rooms.get(room_id) else {
            return;
        };
        let Some(source) = room.get(&sender) else {
            return;
        };
        for (&session_id, participant) in room {
            if session_id != sender
                && participant.receive_audio
                && (!participant.is_logger || !source.is_logger)
            {
                let _ = participant.audio_tx.try_send(packet.clone());
            }
        }
    }
    /// Forward audio originating outside a WebSocket session (e.g. Discord).
    pub async fn broadcast_external_audio(&self, room_id: &str, packet: Vec<u8>) {
        let rooms = self.rooms.read().await;
        if let Some(room) = rooms.get(room_id) {
            for participant in room.values() {
                if participant.receive_audio {
                    let _ = participant.audio_tx.try_send(packet.clone());
                }
            }
        }
    }
    pub async fn broadcast_control_to_non_loggers(&self, room_id: &str, event: String) {
        let rooms = self.rooms.read().await;
        let Some(room) = rooms.get(room_id) else {
            return;
        };
        for participant in room.values().filter(|participant| !participant.is_logger) {
            let _ = participant.control_tx.try_send(event.clone());
        }
    }
    pub async fn broadcast_control(&self, room_id: &str, event: String) {
        let rooms = self.rooms.read().await;
        let Some(room) = rooms.get(room_id) else {
            return;
        };
        for participant in room.values() {
            let _ = participant.control_tx.try_send(event.clone());
        }
    }
}
