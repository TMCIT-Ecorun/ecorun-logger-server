//! Per-room mix of everyone talking, re-encoded as one low-bitrate Opus stream
//! for loggers on a metered LTE link.
//!
//! Browsers (Socket.IO) and `/ws/audio` clients feed their Opus packets in as
//! sources. Every [`MIX_FRAME_MS`] the mixer sums whatever each source has
//! buffered and sends one packet to each logger listener, leaving out the
//! listener's own voice. Nothing is sent while nobody talks, and a listener
//! whose queue is full simply loses that packet, so audio never piles up in
//! front of the logger's telemetry.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use opus::{Application, Bitrate, Channels, Decoder, Encoder};
use tokio::sync::mpsc;
use tracing::warn;

use crate::audio::{MAX_PACKET_SIZE, SAMPLE_RATE};
use crate::room::SessionId;

/// One packet per 120 ms halves the per-packet WebSocket/TLS/TCP overhead
/// compared with the 60 ms packets used between browsers.
pub const MIX_FRAME_MS: u16 = 120;
pub const MIX_FRAME_SAMPLES: usize = SAMPLE_RATE as usize * MIX_FRAME_MS as usize / 1000;
pub const DEFAULT_BITRATE: i32 = 6_000;
/// Latency cap per source: older audio is dropped instead of delaying speech.
const MAX_BUFFER_SAMPLES: usize = MIX_FRAME_SAMPLES * 4;
/// A source that sent nothing for this long is forgotten (its decoder freed).
const SOURCE_IDLE: Duration = Duration::from_secs(3);
/// Longest Opus packet (120 ms) at our sample rate.
const MAX_DECODED_SAMPLES: usize = SAMPLE_RATE as usize * 120 / 1000;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum SourceId {
    /// A browser, keyed by its Socket.IO socket id.
    Socket(String),
    /// A `/ws/audio` client (logger, Pi).
    Ws(SessionId),
}

struct Source {
    decoder: Decoder,
    pcm: VecDeque<i16>,
    /// Starts once a full frame is buffered so network jitter does not chop
    /// the first words; stops when the buffer runs dry.
    playing: bool,
    last_packet: Instant,
}

struct Listener {
    encoder: Encoder,
    tx: mpsc::Sender<Vec<u8>>,
}

#[derive(Default)]
struct RoomMix {
    sources: HashMap<SourceId, Source>,
    listeners: HashMap<SessionId, Listener>,
}

pub struct MixHub {
    rooms: Mutex<HashMap<String, RoomMix>>,
    bitrate: i32,
}

impl MixHub {
    pub fn new(bitrate: i32) -> Self {
        Self {
            rooms: Mutex::new(HashMap::new()),
            bitrate,
        }
    }

    pub fn from_env() -> Self {
        let bitrate = std::env::var("ECORUN_LOGGER_AUDIO_BITRATE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_BITRATE);
        Self::new(bitrate)
    }

    /// Run the mix clock until the process exits.
    pub fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut clock =
                tokio::time::interval(Duration::from_millis(u64::from(MIX_FRAME_MS)));
            clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                clock.tick().await;
                self.tick();
            }
        });
    }

    pub fn add_listener(&self, room_id: &str, session_id: SessionId, tx: mpsc::Sender<Vec<u8>>) {
        let mut encoder = match Encoder::new(SAMPLE_RATE, Channels::Mono, Application::Voip) {
            Ok(encoder) => encoder,
            Err(error) => {
                warn!(%error, "failed to create logger mix encoder");
                return;
            }
        };
        if let Err(error) = encoder.set_bitrate(Bitrate::Bits(self.bitrate)) {
            warn!(%error, bitrate = self.bitrate, "failed to set logger mix bitrate");
        }
        let mut rooms = self.rooms.lock().unwrap();
        rooms
            .entry(room_id.to_owned())
            .or_default()
            .listeners
            .insert(session_id, Listener { encoder, tx });
    }

    pub fn remove_listener(&self, session_id: SessionId) {
        let mut rooms = self.rooms.lock().unwrap();
        for room in rooms.values_mut() {
            room.listeners.remove(&session_id);
        }
        rooms.retain(|_, room| !room.listeners.is_empty() || !room.sources.is_empty());
    }

    /// Feed one Opus packet from a talking participant. Ignored when no logger
    /// listens in that room, so browser-only rooms cost nothing.
    pub fn push(&self, room_id: &str, source_id: SourceId, packet: &[u8]) {
        let mut rooms = self.rooms.lock().unwrap();
        let Some(room) = rooms.get_mut(room_id) else {
            return;
        };
        if room.listeners.is_empty() {
            return;
        }
        let source = match room.sources.entry(source_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                match Decoder::new(SAMPLE_RATE, Channels::Mono) {
                    Ok(decoder) => entry.insert(Source {
                        decoder,
                        pcm: VecDeque::new(),
                        playing: false,
                        last_packet: Instant::now(),
                    }),
                    Err(error) => {
                        warn!(%error, "failed to create logger mix decoder");
                        return;
                    }
                }
            }
        };
        let mut pcm = [0_i16; MAX_DECODED_SAMPLES];
        match source.decoder.decode(packet, &mut pcm, false) {
            Ok(samples) => {
                source.pcm.extend(&pcm[..samples]);
                let excess = source.pcm.len().saturating_sub(MAX_BUFFER_SAMPLES);
                source.pcm.drain(..excess);
                source.last_packet = Instant::now();
            }
            Err(error) => warn!(%error, "invalid Opus packet for logger mix"),
        }
    }

    /// Mix one frame per room and send it to that room's listeners.
    pub fn tick(&self) {
        let mut rooms = self.rooms.lock().unwrap();
        let now = Instant::now();
        for room in rooms.values_mut() {
            room.sources
                .retain(|_, source| now.duration_since(source.last_packet) < SOURCE_IDLE);
            if room.listeners.is_empty() {
                room.sources.clear();
                continue;
            }
            let mut frames: Vec<(&SourceId, Vec<i16>)> = Vec::new();
            for (id, source) in room.sources.iter_mut() {
                if !source.playing && source.pcm.len() >= MIX_FRAME_SAMPLES {
                    source.playing = true;
                }
                if !source.playing {
                    continue;
                }
                let take = source.pcm.len().min(MIX_FRAME_SAMPLES);
                let mut frame: Vec<i16> = source.pcm.drain(..take).collect();
                frame.resize(MIX_FRAME_SAMPLES, 0);
                if source.pcm.is_empty() {
                    source.playing = false;
                }
                frames.push((id, frame));
            }
            if frames.is_empty() {
                continue;
            }
            for (&listener_id, listener) in room.listeners.iter_mut() {
                let own = SourceId::Ws(listener_id);
                let mut mixed = [0_i32; MIX_FRAME_SAMPLES];
                let mut any = false;
                for (id, frame) in &frames {
                    if **id == own {
                        continue;
                    }
                    any = true;
                    for (sum, &sample) in mixed.iter_mut().zip(frame) {
                        *sum += i32::from(sample);
                    }
                }
                if !any {
                    continue;
                }
                let pcm: Vec<i16> = mixed
                    .iter()
                    .map(|&sum| sum.clamp(i16::MIN.into(), i16::MAX.into()) as i16)
                    .collect();
                let mut packet = vec![0_u8; MAX_PACKET_SIZE];
                match listener.encoder.encode(&pcm, &mut packet) {
                    Ok(size) => {
                        packet.truncate(size);
                        // Full queue: drop this frame rather than delay telemetry.
                        let _ = listener.tx.try_send(packet);
                    }
                    Err(error) => warn!(%error, "logger mix encode failed"),
                }
            }
        }
        rooms.retain(|_, room| !room.listeners.is_empty() || !room.sources.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone_packets(rate: u32, frame_ms: usize, count: usize, hz: f64) -> Vec<Vec<u8>> {
        let mut encoder = Encoder::new(rate, Channels::Mono, Application::Voip).unwrap();
        let frame = rate as usize * frame_ms / 1000;
        (0..count)
            .map(|n| {
                let pcm: Vec<i16> = (0..frame)
                    .map(|i| {
                        let t = (n * frame + i) as f64 / f64::from(rate);
                        ((t * std::f64::consts::TAU * hz).sin() * 8_000.0) as i16
                    })
                    .collect();
                let mut out = vec![0_u8; MAX_PACKET_SIZE];
                let size = encoder.encode(&pcm, &mut out).unwrap();
                out.truncate(size);
                out
            })
            .collect()
    }

    fn drain(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<Vec<u8>> {
        let mut packets = Vec::new();
        while let Ok(packet) = rx.try_recv() {
            packets.push(packet);
        }
        packets
    }

    #[test]
    fn browser_voice_reaches_logger_as_low_bitrate_120ms_packets() {
        let hub = MixHub::new(DEFAULT_BITRATE);
        let (tx, mut rx) = mpsc::channel(64);
        hub.add_listener("main", 7, tx);
        // 16 kHz wideband, 60 ms packets: what the browser sends.
        for pair in tone_packets(16_000, 60, 20, 300.0).chunks(2) {
            for packet in pair {
                hub.push("main", SourceId::Socket("browser".into()), packet);
            }
            hub.tick();
        }
        hub.tick();
        let packets = drain(&mut rx);
        assert!(packets.len() >= 8, "got {} packets", packets.len());
        let mut decoder = Decoder::new(SAMPLE_RATE, Channels::Mono).unwrap();
        let mut pcm = [0_i16; MAX_DECODED_SAMPLES];
        let samples = decoder.decode(&packets[2], &mut pcm, false).unwrap();
        assert_eq!(samples, MIX_FRAME_SAMPLES);
        let rms = (pcm[..samples].iter().map(|&s| f64::from(s).powi(2)).sum::<f64>()
            / samples as f64)
            .sqrt();
        assert!(rms > 1_000.0, "mixed tone too quiet: rms={rms}");
        let average = packets.iter().map(Vec::len).sum::<usize>() / packets.len();
        // 6 kbps at 120 ms is 90 bytes; leave room for VBR.
        assert!(average <= 130, "average packet {average} bytes");
    }

    #[test]
    fn nothing_is_sent_while_nobody_talks() {
        let hub = MixHub::new(DEFAULT_BITRATE);
        let (tx, mut rx) = mpsc::channel(64);
        hub.add_listener("main", 7, tx);
        for _ in 0..10 {
            hub.tick();
        }
        assert!(drain(&mut rx).is_empty());
    }

    #[test]
    fn logger_does_not_hear_itself() {
        let hub = MixHub::new(DEFAULT_BITRATE);
        let (tx, mut rx) = mpsc::channel(64);
        hub.add_listener("main", 7, tx);
        for packet in tone_packets(8_000, 60, 10, 300.0) {
            hub.push("main", SourceId::Ws(7), &packet);
            hub.tick();
        }
        assert!(drain(&mut rx).is_empty());
    }

    #[test]
    fn two_talkers_are_one_stream() {
        let hub = MixHub::new(DEFAULT_BITRATE);
        let (tx, mut rx) = mpsc::channel(64);
        hub.add_listener("main", 7, tx);
        let a = tone_packets(16_000, 60, 8, 300.0);
        let b = tone_packets(16_000, 60, 8, 500.0);
        let mut ticks = 0;
        for (n, (pa, pb)) in a.iter().zip(&b).enumerate() {
            hub.push("main", SourceId::Socket("a".into()), pa);
            hub.push("main", SourceId::Socket("b".into()), pb);
            if n % 2 == 1 {
                hub.tick();
                ticks += 1;
            }
        }
        let packets = drain(&mut rx);
        assert!(!packets.is_empty());
        assert!(packets.len() <= ticks, "{} packets for {ticks} ticks", packets.len());
    }

    #[test]
    fn rooms_without_a_logger_cost_nothing() {
        let hub = MixHub::new(DEFAULT_BITRATE);
        for packet in tone_packets(16_000, 60, 5, 300.0) {
            hub.push("browsers-only", SourceId::Socket("a".into()), &packet);
        }
        assert!(hub.rooms.lock().unwrap().is_empty());
    }

    #[test]
    fn full_queue_drops_instead_of_blocking() {
        let hub = MixHub::new(DEFAULT_BITRATE);
        let (tx, mut rx) = mpsc::channel(1);
        hub.add_listener("main", 7, tx);
        for packet in tone_packets(16_000, 60, 20, 300.0) {
            hub.push("main", SourceId::Socket("a".into()), &packet);
            hub.tick();
        }
        assert_eq!(drain(&mut rx).len(), 1);
    }
}
