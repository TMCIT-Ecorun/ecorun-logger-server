//! Optional in-process Discord voice bridge. Enable with ECORUN_DISCORD_BOT_TOKEN.
use crate::{
    audio::{AudioCodec, OpusCodec, FRAME_SAMPLES},
    config::RoomStore,
    mixer::{MixHub, SourceId},
    room::RoomManager,
};
use bytes::Bytes;
use serenity::{
    all::{Channel, ChannelId, Context, GatewayIntents, GuildId, Ready},
    async_trait,
    client::{Client, EventHandler},
};
use socketioxide::SocketIo;
use songbird::{
    driver::{DecodeConfig, DecodeMode},
    events::{CoreEvent, Event, EventContext, EventHandler as VoiceHandler},
    input::RawAdapter,
    SerenityInit,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Seek, SeekFrom},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::broadcast;
use tracing::{info, warn};

pub type Packet = (String, String, u64, Vec<u8>);

pub fn spawn(
    rooms: Arc<RoomStore>,
    mix: Arc<MixHub>,
    io: SocketIo,
    peers: Arc<RoomManager>,
    audio: broadcast::Sender<Packet>,
) {
    let Ok(token) = std::env::var("ECORUN_DISCORD_BOT_TOKEN") else {
        return;
    };
    if token.trim().is_empty() {
        return;
    }
    let audio_active = crate::discord_ha::start();
    tokio::spawn(async move {
        let handler = Bot {
            audio_active,
            started: AtomicBool::new(false),
            rooms,
            mix,
            io,
            peers,
            audio,
        };
        let client = Client::builder(
            token,
            GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES,
        )
        .event_handler(handler)
        .register_songbird_from_config(
            songbird::Config::default().decode_mode(DecodeMode::Decode(DecodeConfig::default())),
        )
        .await;
        match client {
            Ok(mut client) => {
                if let Err(error) = client.start().await {
                    warn!(%error, "Discord gateway exited")
                }
            }
            Err(error) => warn!(%error, "Discord client initialization failed"),
        }
    });
}

struct Bot {
    started: AtomicBool,
    audio_active: Arc<AtomicBool>,
    rooms: Arc<RoomStore>,
    mix: Arc<MixHub>,
    io: SocketIo,
    peers: Arc<RoomManager>,
    audio: broadcast::Sender<Packet>,
}

#[async_trait]
impl EventHandler for Bot {
    async fn ready(&self, ctx: Context, _: Ready) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let audio_active = self.audio_active.clone();
        let rooms = self.rooms.clone();
        let mix = self.mix.clone();
        let io = self.io.clone();
        let peers = self.peers.clone();
        let audio = self.audio.clone();
        tokio::spawn(async move { watch(ctx, audio_active, rooms, mix, io, peers, audio).await });
    }
}

struct Active {
    channel: u64,
    guild: GuildId,
    playback: Arc<Mutex<VecDeque<u8>>>,
    decoder: OpusCodec,
}

async fn watch(
    ctx: Context,
    audio_active: Arc<AtomicBool>,
    rooms: Arc<RoomStore>,
    mix: Arc<MixHub>,
    io: SocketIo,
    peers: Arc<RoomManager>,
    audio: broadcast::Sender<Packet>,
) {
    let Some(manager) = songbird::get(&ctx).await else {
        return;
    };
    let mut active: HashMap<String, Active> = HashMap::new();
    let mut audio_rx = audio.subscribe();
    let mut was_active = audio_active.load(Ordering::Acquire);
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now_active = audio_active.load(Ordering::Acquire);
                if was_active && !now_active {
                    for a in active.values() { a.playback.lock().unwrap().clear(); }
                }
                was_active = now_active;
                let desired = match rooms.list() { Ok(list) => list, Err(err) => { warn!(%err, "Discord room scan failed"); continue } };
                let configured: HashMap<String,u64> = desired.into_iter().filter_map(|r| r.discord_channel_id.and_then(|v| v.parse().ok()).map(|id| (r.id,id))).collect();
                let stale: Vec<_> = active.iter().filter(|(room, a)| configured.get(*room) != Some(&a.channel)).map(|(room, _)| room.clone()).collect();
                for room in stale { if let Some(a) = active.remove(&room) { let _ = manager.remove(a.guild).await; } }
                let mut guilds: HashSet<GuildId> = active.values().map(|a| a.guild).collect();
                for (room, channel) in configured {
                    if active.contains_key(&room) { continue }
                    let channel_info = match ctx.http.get_channel(ChannelId::new(channel)).await {
                        Ok(Channel::Guild(c)) => c,
                        Ok(_) => { warn!(room, channel, "Discord channel is not a guild voice channel"); continue },
                        Err(error) => { warn!(%error, room, channel, "Discord channel lookup failed"); continue },
                    };
                    if !matches!(channel_info.kind, serenity::all::ChannelType::Voice | serenity::all::ChannelType::Stage) { continue }
                    let guild = channel_info.guild_id;
                    if guilds.contains(&guild) { warn!(room, "Discord permits only one VC per bot per guild"); continue }
                    match manager.join(guild, ChannelId::new(channel)).await {
                        Ok(call) => {
                            let playback = Arc::new(Mutex::new(VecDeque::new()));
                            let reader = Playback(playback.clone());
                            let mut guard = call.lock().await;
                            guard.add_global_event(Event::Core(CoreEvent::VoiceTick), VoiceReceive {
                                audio_active: audio_active.clone(),
                                room: room.clone(), mix: mix.clone(), io: io.clone(), peers: peers.clone(), audio: audio.clone(),
                                sources: Mutex::new(HashMap::new()),
                            });
                            guard.play_input(RawAdapter::new(reader, 48_000, 2).into());
                            guilds.insert(guild);
                            active.insert(room.clone(), Active { channel, guild, playback, decoder: OpusCodec::new().expect("Opus decoder") });
                            info!(room, channel, "Discord VC joined");
                        }
                        Err(error) => warn!(%error, room, channel, "Discord VC join failed"),
                    }
                }
            }
            result = audio_rx.recv() => {
                if let Ok((room, source, _, packet)) = result {
                    if audio_active.load(Ordering::Acquire) && !source.starts_with("discord:") { if let Some(a) = active.get_mut(&room) { feed(&a.playback, &mut a.decoder, &packet); } }
                }
            }
        }
    }
}

const MAX_PLAYBACK_BYTES: usize = 48_000 * 2 * 4 / 2; // 500ms stereo f32
struct Playback(Arc<Mutex<VecDeque<u8>>>);
impl Read for Playback {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let mut queue = self.0.lock().unwrap();
        for byte in out.iter_mut() {
            *byte = queue.pop_front().unwrap_or(0);
        }
        drop(queue);
        // Live PCM is clocked in real time; never let a mixer race ahead of packets.
        std::thread::sleep(Duration::from_secs_f64(
            out.len() as f64 / (48_000.0 * 2.0 * 4.0),
        ));
        Ok(out.len())
    }
}
impl Seek for Playback {
    fn seek(&mut self, _: SeekFrom) -> std::io::Result<u64> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
}
impl songbird::input::core::io::MediaSource for Playback {
    fn is_seekable(&self) -> bool {
        false
    }
    fn byte_len(&self) -> Option<u64> {
        None
    }
}
fn feed(queue: &Arc<Mutex<VecDeque<u8>>>, codec: &mut OpusCodec, packet: &[u8]) {
    let Ok(samples) = codec.decode_and_validate(packet) else {
        return;
    };
    let mut q = queue.lock().unwrap();
    // Drop aged audio; new sound always wins over a blocked voice connection.
    if q.len() > MAX_PLAYBACK_BYTES {
        q.clear();
    }
    for sample in samples {
        for _ in 0..6 {
            let float = (sample as f32 / 32768.0).to_le_bytes();
            q.extend(float);
            q.extend(float);
        }
    }
}

struct VoiceReceive {
    audio_active: Arc<AtomicBool>,
    room: String,
    mix: Arc<MixHub>,
    io: SocketIo,
    peers: Arc<RoomManager>,
    audio: broadcast::Sender<Packet>,
    sources: Mutex<HashMap<u32, (OpusCodec, VecDeque<i16>, u64)>>,
}
#[async_trait]
impl VoiceHandler for VoiceReceive {
    async fn act(&self, event: &EventContext<'_>) -> Option<Event> {
        if !self.audio_active.load(Ordering::Acquire) {
            self.sources.lock().unwrap().clear();
            return None;
        }
        if let EventContext::VoiceTick(tick) = event {
            let mut emitted = Vec::new();
            {
                let mut sources = self.sources.lock().unwrap();
                for (ssrc, voice) in &tick.speaking {
                    let Some(pcm) = &voice.decoded_voice else {
                        continue;
                    };
                    let source = sources.entry(*ssrc).or_insert_with(|| {
                        (OpusCodec::new().expect("Opus codec"), VecDeque::new(), 0)
                    });
                    for frame in pcm.chunks_exact(12) {
                        let sum: i32 = frame.iter().map(|&n| i32::from(n)).sum();
                        source.1.push_back((sum / 12) as i16);
                    }
                    while source.1.len() >= FRAME_SAMPLES {
                        let frame: Vec<_> = source.1.drain(..FRAME_SAMPLES).collect();
                        if let Ok(packet) = source.0.encode(&frame) {
                            let label = format!("discord:{ssrc}");
                            emitted.push((label, source.2, packet));
                            source.2 = source.2.wrapping_add(1);
                        }
                    }
                }
            }
            for (source, seq, packet) in emitted {
                self.mix
                    .push(&self.room, SourceId::Socket(source.clone()), &packet);
                let _ = self
                    .audio
                    .send((self.room.clone(), source.clone(), seq, packet.clone()));
                if let Some(namespace) = self.io.of(format!("/{}", self.room)) {
                    let _ = namespace
                        .emit("audio.packet", &Bytes::from(packet.clone()))
                        .await;
                }
                self.peers
                    .broadcast_external_audio(&self.room, packet)
                    .await;
            }
        }
        None
    }
}
