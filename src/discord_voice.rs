//! Optional in-process Discord voice bridge. Enable with ECORUN_DISCORD_BOT_TOKEN.
//!
//! Each room may be bound to one voice channel (`discord_channel_id`, set with
//! `cargo run -- admin` or the `/ecologger` slash command). The bot joins it and
//! bridges audio both ways:
//!
//! - room -> Discord: every talker in the room (browsers, loggers, other nodes)
//!   is decoded with its own decoder, upsampled 8 -> 48 kHz and mixed, so two
//!   people talking at once are heard together instead of one after the other.
//! - Discord -> room: each Discord speaker is low-passed and decimated
//!   48 -> 8 kHz, then sent as 60 ms packets like any other talker.
//!
//! `/ecologger join|leave|status|audio` lets the pit move the bot and read or
//! change the logger's voice settings (`logger.config`) from Discord.
use crate::{
    audio::{AudioCodec, OpusCodec, FRAME_SAMPLES},
    cluster::{Cluster, ClusterKind},
    config::RoomStore,
    mixer::{MixHub, SourceId},
    room::RoomManager,
};
use serenity::{
    all::{
        Channel, ChannelId, ChannelType, CommandInteraction, CommandOptionType, Context,
        CreateCommand, CreateCommandOption, CreateInteractionResponse,
        CreateInteractionResponseMessage, EditInteractionResponse, GatewayIntents, GuildId,
        Interaction, Ready, ResolvedOption, ResolvedValue,
    },
    async_trait,
    client::{Client, EventHandler},
};
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
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, Notify};
use tracing::{info, warn};

type Packet = (String, String, u64, Vec<u8>);

const COMMAND: &str = "ecologger";
/// `/ecologger audio` options: (option, logger.config key, description, min, max).
/// The ranges match the logger's voice agent.
const AUDIO_OPTIONS: [(&str, &str, &str, i64, i64); 5] = [
    (
        "level",
        "level_db",
        "ドライバーの声の大きさの目標 dBFS（-30〜-6。大きいほど大きい。標準 -16）",
        -30,
        -6,
    ),
    (
        "max_gain",
        "max_gain_db",
        "マイクの増幅の上限 dB（上げると小さいマイクでも大きくなるが雑音も増える。標準 30）",
        0,
        40,
    ),
    (
        "lowcut",
        "lowcut_hz",
        "低音カット Hz（上げると風切り音が減る。0でなし。標準 250）",
        0,
        400,
    ),
    (
        "bitrate",
        "bitrate",
        "ドライバーの声のビットレート kbps（上げると音質が上がるがLTEの通信量が増える。標準 6）",
        6,
        16,
    ),
    (
        "volume",
        "volume",
        "ドライバーのイヤホンの音量 %（標準 80）",
        0,
        100,
    ),
];
/// On LTE the logger reads the server every 2 s, so its answer can take ~6 s.
const LOGGER_ANSWER_TIMEOUT: Duration = Duration::from_secs(8);

pub fn spawn(
    rooms: Arc<RoomStore>,
    room_manager: Arc<RoomManager>,
    mix: Arc<MixHub>,
    cluster: Cluster,
    audio: broadcast::Sender<Packet>,
    logger_config: broadcast::Sender<(String, serde_json::Value)>,
) {
    let Ok(token) = std::env::var("ECORUN_DISCORD_BOT_TOKEN") else {
        return;
    };
    if token.trim().is_empty() {
        return;
    }
    // Only the designated node opens the bot gateway, avoiding duplicate voice sessions.
    let owner =
        std::env::var("ECORUN_DISCORD_NODE_ID").unwrap_or_else(|_| cluster.node_id().to_string());
    if std::env::var_os("ECORUN_DISCORD_AUDIO_ROLE").is_none() && owner != cluster.node_id() {
        return;
    }
    // The standby bot (dual-bot failover) only relays audio; commands go to the
    // primary so the pit sees one /ecologger.
    let commands = std::env::var("ECORUN_DISCORD_AUDIO_ROLE").as_deref() != Ok("standby");
    let audio_active = crate::discord_ha::start();
    let shared = Arc::new(Shared {
        audio_active,
        rooms,
        room_manager,
        mix,
        cluster,
        audio,
        logger_config,
        commands,
        joined: Mutex::new(HashMap::new()),
        speaking: Mutex::new(HashMap::new()),
        last_config: Mutex::new(HashMap::new()),
        rescan: Notify::new(),
    });
    tokio::spawn(async move {
        let handler = Bot {
            started: AtomicBool::new(false),
            shared,
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

/// State shared by the gateway handler (commands) and the voice watcher.
struct Shared {
    audio_active: Arc<AtomicBool>,
    rooms: Arc<RoomStore>,
    room_manager: Arc<RoomManager>,
    mix: Arc<MixHub>,
    cluster: Cluster,
    audio: broadcast::Sender<Packet>,
    logger_config: broadcast::Sender<(String, serde_json::Value)>,
    commands: bool,
    /// room -> (voice channel, guild) the bot is in now.
    joined: Mutex<HashMap<String, (u64, GuildId)>>,
    /// room -> when a Discord speaker was last relayed into it.
    speaking: Mutex<HashMap<String, Instant>>,
    /// room -> the logger's last `logger.config` answer.
    last_config: Mutex<HashMap<String, serde_json::Value>>,
    /// Wakes the watcher right after a command changed a binding.
    rescan: Notify,
}

struct Bot {
    started: AtomicBool,
    shared: Arc<Shared>,
}

#[async_trait]
impl EventHandler for Bot {
    async fn ready(&self, ctx: Context, ready: Ready) {
        if self.shared.commands {
            for guild in &ready.guilds {
                // Guild commands show up at once (global ones can take an hour).
                if let Err(error) = guild.id.set_commands(&ctx.http, vec![command()]).await {
                    warn!(%error, guild = %guild.id, "failed to register /ecologger");
                }
            }
            info!(guilds = ready.guilds.len(), "Discord /ecologger registered");
        }
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let shared = self.shared.clone();
        tokio::spawn(async move { watch(ctx, shared).await });
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let Interaction::Command(command) = interaction else {
            return;
        };
        if command.data.name != COMMAND || !self.shared.commands {
            return;
        }
        if let Err(error) = on_command(&ctx, &self.shared, &command).await {
            warn!(%error, "/ecologger failed");
            let _ = command
                .edit_response(
                    &ctx.http,
                    EditInteractionResponse::new().content(format!("失敗しました（{error}）")),
                )
                .await;
        }
    }
}

struct Active {
    channel: u64,
    guild: GuildId,
    playback: Arc<Mutex<PlaybackMix>>,
}

async fn watch(ctx: Context, shared: Arc<Shared>) {
    let Some(manager) = songbird::get(&ctx).await else {
        return;
    };
    let mut active: HashMap<String, Active> = HashMap::new();
    let mut audio_rx = shared.audio.subscribe();
    let mut was_active = shared.audio_active.load(Ordering::Acquire);
    let mut cluster_rx = shared.cluster.subscribe_inbound();
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        let rescan = tokio::select! {
            _ = tick.tick() => true,
            _ = shared.rescan.notified() => true,
            result = audio_rx.recv() => {
                if let Ok((room, source, _, packet)) = result {
                    relay_to_discord(&shared, &active, &room, &source, &packet);
                }
                false
            }
            result = cluster_rx.recv() => {
                if let Ok(event) = result {
                    if let ClusterKind::Audio { room, source, packet, .. } = event.kind {
                        relay_to_discord(&shared, &active, &room, &source, &packet);
                    }
                }
                false
            }
        };
        if !rescan {
            continue;
        }
        let now_active = shared.audio_active.load(Ordering::Acquire);
        if was_active && !now_active {
            for a in active.values() {
                a.playback.lock().unwrap().clear();
            }
        }
        was_active = now_active;
        let desired = match shared.rooms.list() {
            Ok(list) => list,
            Err(err) => {
                warn!(%err, "Discord room scan failed");
                continue;
            }
        };
        let configured: HashMap<String, u64> = desired
            .into_iter()
            .filter_map(|r| {
                r.discord_channel_id
                    .and_then(|v| v.parse().ok())
                    .map(|id| (r.id, id))
            })
            .collect();
        let stale: Vec<_> = active
            .iter()
            .filter(|(room, a)| configured.get(*room) != Some(&a.channel))
            .map(|(room, _)| room.clone())
            .collect();
        for room in stale {
            if let Some(a) = active.remove(&room) {
                let _ = manager.remove(a.guild).await;
                info!(room, channel = a.channel, "Discord VC left");
            }
        }
        let mut guilds: HashSet<GuildId> = active.values().map(|a| a.guild).collect();
        for (room, channel) in configured {
            if active.contains_key(&room) {
                continue;
            }
            let channel_info = match ctx.http.get_channel(ChannelId::new(channel)).await {
                Ok(Channel::Guild(c)) => c,
                Ok(_) => {
                    warn!(
                        room,
                        channel, "Discord channel is not a guild voice channel"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(%error, room, channel, "Discord channel lookup failed");
                    continue;
                }
            };
            if !matches!(channel_info.kind, ChannelType::Voice | ChannelType::Stage) {
                continue;
            }
            let guild = channel_info.guild_id;
            if guilds.contains(&guild) {
                warn!(room, "Discord permits only one VC per bot per guild");
                continue;
            }
            match manager.join(guild, ChannelId::new(channel)).await {
                Ok(call) => {
                    let playback = Arc::new(Mutex::new(PlaybackMix::default()));
                    let reader = Playback {
                        mix: playback.clone(),
                        pending: Vec::new(),
                    };
                    let mut guard = call.lock().await;
                    guard.add_global_event(
                        Event::Core(CoreEvent::VoiceTick),
                        VoiceReceive {
                            shared: shared.clone(),
                            room: room.clone(),
                            sources: Mutex::new(HashMap::new()),
                        },
                    );
                    guard.play_input(RawAdapter::new(reader, 48_000, 2).into());
                    guilds.insert(guild);
                    active.insert(
                        room.clone(),
                        Active {
                            channel,
                            guild,
                            playback,
                        },
                    );
                    info!(room, channel, "Discord VC joined");
                }
                Err(error) => warn!(%error, room, channel, "Discord VC join failed"),
            }
        }
        *shared.joined.lock().unwrap() = active
            .iter()
            .map(|(room, a)| (room.clone(), (a.channel, a.guild)))
            .collect();
    }
}

fn relay_to_discord(
    shared: &Shared,
    active: &HashMap<String, Active>,
    room: &str,
    source: &str,
    packet: &[u8],
) {
    if !shared.audio_active.load(Ordering::Acquire) || source.starts_with("discord:") {
        return;
    }
    if let Some(a) = active.get(room) {
        a.playback.lock().unwrap().feed(source, packet);
    }
}

// ---- room -> Discord ---------------------------------------------------------

/// 8 kHz -> 48 kHz.
const UPSAMPLE: usize = 6;
/// Mono 48 kHz samples queued at most per talker; more and its packet is dropped.
const MAX_PLAYBACK_SAMPLES: usize = 48_000 / 2; // 500 ms
/// Each talk spurt starts this far ahead of the read position, so network
/// jitter does not chop the first words.
const PREROLL_SAMPLES: usize = 48_000 * 60 / 1000;
const SOURCE_IDLE: Duration = Duration::from_secs(3);

struct PlaybackSource {
    decoder: OpusCodec,
    /// Where this talker's next sample goes, relative to the read position.
    cursor: usize,
    last: f32,
    seen: Instant,
}

/// Every room talker mixed into one 48 kHz mono timeline.
#[derive(Default)]
struct PlaybackMix {
    timeline: VecDeque<f32>,
    sources: HashMap<String, PlaybackSource>,
}

impl PlaybackMix {
    fn feed(&mut self, source: &str, packet: &[u8]) {
        let now = Instant::now();
        self.sources
            .retain(|_, s| now.duration_since(s.seen) < SOURCE_IDLE);
        if !self.sources.contains_key(source) {
            let Ok(decoder) = OpusCodec::new() else {
                return;
            };
            self.sources.insert(
                source.to_owned(),
                PlaybackSource {
                    decoder,
                    cursor: 0,
                    last: 0.0,
                    seen: now,
                },
            );
        }
        let s = self.sources.get_mut(source).expect("inserted above");
        let Ok(pcm) = s.decoder.decode_and_validate(packet) else {
            return;
        };
        s.seen = now;
        if s.cursor == 0 {
            s.cursor = PREROLL_SAMPLES;
        }
        let n = pcm.len() * UPSAMPLE;
        // Fell behind (blocked voice connection): new sound wins, drop this packet.
        if s.cursor + n > MAX_PLAYBACK_SAMPLES {
            return;
        }
        if self.timeline.len() < s.cursor + n {
            self.timeline.resize(s.cursor + n, 0.0);
        }
        for (i, &sample) in pcm.iter().enumerate() {
            let v = f32::from(sample) / 32768.0;
            // Linear interpolation: the voice band needs no better.
            for k in 0..UPSAMPLE {
                self.timeline[s.cursor + i * UPSAMPLE + k] +=
                    s.last + (v - s.last) * (k + 1) as f32 / UPSAMPLE as f32;
            }
            s.last = v;
        }
        s.cursor += n;
    }

    fn take(&mut self, n: usize) -> Vec<f32> {
        let available = n.min(self.timeline.len());
        let mut out: Vec<f32> = self.timeline.drain(..available).collect();
        out.resize(n, 0.0);
        for s in self.sources.values_mut() {
            s.cursor = if available < n {
                0
            } else {
                s.cursor.saturating_sub(available)
            };
        }
        out
    }

    fn clear(&mut self) {
        self.timeline.clear();
        self.sources.clear();
    }
}

/// songbird reads raw 48 kHz stereo f32 from here.
struct Playback {
    mix: Arc<Mutex<PlaybackMix>>,
    /// Bytes of a frame not yet handed out (reads need not align to frames).
    pending: Vec<u8>,
}
impl Read for Playback {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pending.len() < out.len() {
            let frames = (out.len() - self.pending.len()).div_ceil(8);
            let mono = self.mix.lock().unwrap().take(frames);
            for v in mono {
                let bytes = v.clamp(-1.0, 1.0).to_le_bytes();
                self.pending.extend_from_slice(&bytes);
                self.pending.extend_from_slice(&bytes);
            }
        }
        out.copy_from_slice(&self.pending[..out.len()]);
        self.pending.drain(..out.len());
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

// ---- Discord -> room ---------------------------------------------------------

/// 48 kHz -> 8 kHz.
const DECIMATE: usize = 6;

/// Windowed-sinc low-pass at 3.4 kHz (Hamming, 63 taps). Plain averaging of six
/// samples lets everything above 4 kHz fold back into the voice band as noise.
fn taps() -> &'static [f32] {
    static TAPS: OnceLock<Vec<f32>> = OnceLock::new();
    TAPS.get_or_init(|| {
        use std::f64::consts::PI;
        let n = 63;
        let fc = 3_400.0 / 48_000.0;
        let h: Vec<f64> = (0..n)
            .map(|i| {
                let m = i as f64 - (n - 1) as f64 / 2.0;
                let sinc = if m == 0.0 {
                    2.0 * fc
                } else {
                    (2.0 * PI * fc * m).sin() / (PI * m)
                };
                sinc * (0.54 - 0.46 * (2.0 * PI * i as f64 / (n - 1) as f64).cos())
            })
            .collect();
        let sum: f64 = h.iter().sum();
        h.into_iter().map(|v| (v / sum) as f32).collect()
    })
}

struct Downsampler {
    /// Filter history plus any samples left over from the last block.
    pending: Vec<f32>,
}

impl Default for Downsampler {
    fn default() -> Self {
        Self {
            pending: vec![0.0; taps().len() - 1],
        }
    }
}

impl Downsampler {
    /// Interleaved 48 kHz stereo -> 8 kHz mono.
    fn push(&mut self, stereo: &[i16]) -> Vec<i16> {
        let taps = taps();
        let history = taps.len() - 1;
        let mut x = std::mem::take(&mut self.pending);
        x.extend(
            stereo
                .chunks_exact(2)
                .map(|lr| (f32::from(lr[0]) + f32::from(lr[1])) / 2.0),
        );
        let outputs = (x.len() - history) / DECIMATE;
        let out = (0..outputs)
            .map(|o| {
                let end = history + (o + 1) * DECIMATE - 1;
                let acc: f32 = taps.iter().enumerate().map(|(k, t)| t * x[end - k]).sum();
                acc.round().clamp(-32768.0, 32767.0) as i16
            })
            .collect();
        self.pending = x[outputs * DECIMATE..].to_vec();
        out
    }
}

struct SpeakerSource {
    encoder: OpusCodec,
    down: Downsampler,
    pcm: VecDeque<i16>,
    sequence: u64,
}

struct VoiceReceive {
    shared: Arc<Shared>,
    room: String,
    sources: Mutex<HashMap<u32, SpeakerSource>>,
}
#[async_trait]
impl VoiceHandler for VoiceReceive {
    async fn act(&self, event: &EventContext<'_>) -> Option<Event> {
        let shared = &self.shared;
        if !shared.audio_active.load(Ordering::Acquire) {
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
                    let source = match sources.entry(*ssrc) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let Ok(encoder) = OpusCodec::new() else {
                                continue;
                            };
                            entry.insert(SpeakerSource {
                                encoder,
                                down: Downsampler::default(),
                                pcm: VecDeque::new(),
                                sequence: 0,
                            })
                        }
                    };
                    let mono = source.down.push(pcm);
                    source.pcm.extend(mono);
                    while source.pcm.len() >= FRAME_SAMPLES {
                        let frame: Vec<_> = source.pcm.drain(..FRAME_SAMPLES).collect();
                        if let Ok(packet) = source.encoder.encode(&frame) {
                            let label = format!("discord:{}:{ssrc}", shared.cluster.node_id());
                            emitted.push((label, source.sequence, packet));
                            source.sequence = source.sequence.wrapping_add(1);
                        }
                    }
                }
            }
            if !emitted.is_empty() {
                shared
                    .speaking
                    .lock()
                    .unwrap()
                    .insert(self.room.clone(), Instant::now());
            }
            for (source, seq, packet) in emitted {
                shared
                    .mix
                    .push(&self.room, SourceId::Socket(source.clone()), &packet);
                let _ = shared
                    .audio
                    .send((self.room.clone(), source.clone(), seq, packet.clone()));
                shared
                    .cluster
                    .publish(shared.cluster.event(ClusterKind::Audio {
                        room: self.room.clone(),
                        source,
                        sequence: seq,
                        packet,
                        forward_to_logger: true,
                    }));
            }
        }
        None
    }
}

// ---- /ecologger ----------------------------------------------------------------
// One parent command, so it never collides with other bots' /join and /leave
// (the team's ecorun-talk bot already uses those two). Replies are ephemeral.

fn command() -> CreateCommand {
    let mut audio = CreateCommandOption::new(
        CommandOptionType::SubCommand,
        "audio",
        "ロガーの音声の設定を見る・変える（何も指定しなければ見るだけ）",
    );
    for (name, _, description, min, max) in AUDIO_OPTIONS {
        let mut option = CreateCommandOption::new(CommandOptionType::Integer, name, description);
        // serenity only takes unsigned integer bounds; negative ranges are checked on use.
        if min >= 0 {
            option = option.min_int_value(min as u64).max_int_value(max as u64);
        }
        audio = audio.add_sub_option(option);
    }
    CreateCommand::new(COMMAND)
        .description("EcoLoggerの中継Bot（ロガーの部屋 ⇔ このボイスチャンネル）")
        .dm_permission(false)
        .add_option(
            CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "join",
                "ボイスチャンネルに入ってロガーの部屋とつなぐ",
            )
            .add_sub_option(
                CreateCommandOption::new(
                    CommandOptionType::Channel,
                    "channel",
                    "入るボイスチャンネル（省略時は呼んだ人がいる所）",
                )
                .channel_types(vec![ChannelType::Voice]),
            )
            .add_sub_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "room",
                    "つなぐロガーの部屋（省略時は今の部屋）",
                )
                .max_length(64),
            ),
        )
        .add_option(CreateCommandOption::new(
            CommandOptionType::SubCommand,
            "leave",
            "ボイスチャンネルから出る（再起動しても戻らない）",
        ))
        .add_option(CreateCommandOption::new(
            CommandOptionType::SubCommand,
            "status",
            "いまの接続先を表示",
        ))
        .add_option(audio)
}

async fn on_command(
    ctx: &Context,
    shared: &Shared,
    command: &CommandInteraction,
) -> anyhow::Result<()> {
    command
        .create_response(
            &ctx.http,
            CreateInteractionResponse::Defer(
                CreateInteractionResponseMessage::new().ephemeral(true),
            ),
        )
        .await?;
    let Some(guild) = command.guild_id else {
        return Ok(());
    };
    let options = command.data.options();
    let Some(ResolvedOption {
        name: sub,
        value: ResolvedValue::SubCommand(args),
        ..
    }) = options.first()
    else {
        return Ok(());
    };
    let reply = match *sub {
        "join" => join_command(ctx, shared, command, guild, args).await?,
        "leave" => {
            for room in bound_rooms(ctx, shared, guild)? {
                shared.rooms.set_discord_channel(&room, None)?;
            }
            shared.rescan.notify_one();
            info!(user = %command.user.tag(), "/ecologger leave");
            "ボイスチャンネルから出ました。再起動しても戻りません（もう一度 `/ecologger join` で入ります）。".to_owned()
        }
        "status" => status_text(ctx, shared, guild).await?,
        "audio" => audio_command(ctx, shared, command, guild, args).await?,
        _ => return Ok(()),
    };
    command
        .edit_response(&ctx.http, EditInteractionResponse::new().content(reply))
        .await?;
    Ok(())
}

/// Rooms whose voice channel is in this guild (at most one is joined at a time).
fn bound_rooms(ctx: &Context, shared: &Shared, guild: GuildId) -> anyhow::Result<Vec<String>> {
    let channels: HashSet<u64> = ctx
        .cache
        .guild(guild)
        .map(|g| g.channels.keys().map(|id| id.get()).collect())
        .unwrap_or_default();
    let joined = shared.joined.lock().unwrap().clone();
    Ok(shared
        .rooms
        .list()?
        .into_iter()
        .filter(|room| {
            room.discord_channel_id
                .as_deref()
                .and_then(|id| id.parse::<u64>().ok())
                .is_some_and(|id| {
                    channels.contains(&id)
                        || joined
                            .get(&room.id)
                            .is_some_and(|(c, g)| *c == id && *g == guild)
                })
        })
        .map(|room| room.id)
        .collect())
}

/// The room this guild talks to now: the joined or bound one, else
/// ECORUN_DISCORD_DEFAULT_ROOM.
fn current_room(ctx: &Context, shared: &Shared, guild: GuildId) -> anyhow::Result<Option<String>> {
    let joined = shared
        .joined
        .lock()
        .unwrap()
        .iter()
        .find(|(_, (_, g))| *g == guild)
        .map(|(room, _)| room.clone());
    if joined.is_some() {
        return Ok(joined);
    }
    if let Some(room) = bound_rooms(ctx, shared, guild)?.into_iter().next() {
        return Ok(Some(room));
    }
    Ok(std::env::var("ECORUN_DISCORD_DEFAULT_ROOM")
        .ok()
        .filter(|room| !room.is_empty()))
}

async fn join_command(
    ctx: &Context,
    shared: &Shared,
    command: &CommandInteraction,
    guild: GuildId,
    args: &[ResolvedOption<'_>],
) -> anyhow::Result<String> {
    let mut channel = None;
    let mut room = None;
    for arg in args {
        match (arg.name, &arg.value) {
            ("channel", ResolvedValue::Channel(c)) => channel = Some(c.id),
            ("room", ResolvedValue::String(r)) => room = Some((*r).to_owned()),
            _ => {}
        }
    }
    let room = match room {
        Some(room) => room,
        None => match current_room(ctx, shared, guild)? {
            Some(room) => room,
            None => {
                return Ok("つなぐ部屋を `room:` で指定してください（ロガーの部屋の名前）。".into())
            }
        },
    };
    if shared.rooms.get(&room)?.is_none() {
        return Ok(format!("部屋 `{room}` はサーバーに登録されていません。"));
    }
    let channel = match channel {
        Some(channel) => channel,
        None => {
            let here = ctx.cache.guild(guild).and_then(|g| {
                g.voice_states
                    .get(&command.user.id)
                    .and_then(|v| v.channel_id)
            });
            match here {
                Some(channel) => channel,
                None => {
                    return Ok(
                        "ボイスチャンネルに入ってから呼ぶか、`channel:` で指定してください。"
                            .into(),
                    )
                }
            }
        }
    };
    // The bot can be in one voice channel per guild: move it off the other rooms here.
    for other in bound_rooms(ctx, shared, guild)? {
        if other != room {
            shared.rooms.set_discord_channel(&other, None)?;
        }
    }
    shared
        .rooms
        .set_discord_channel(&room, Some(&channel.get().to_string()))?;
    shared.rescan.notify_one();
    info!(user = %command.user.tag(), room, channel = channel.get(), "/ecologger join");
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if shared
            .joined
            .lock()
            .unwrap()
            .get(&room)
            .is_some_and(|(c, _)| *c == channel.get())
        {
            return Ok(format!(
                "つなぎました。\n{}",
                status_text(ctx, shared, guild).await?
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(format!(
        "ボイスチャンネルに入れませんでした（Botの権限・チャンネルの種類を確認してください）。設定は残っているので、入れるようになれば自動で入ります。\n{}",
        status_text(ctx, shared, guild).await?
    ))
}

async fn status_text(ctx: &Context, shared: &Shared, guild: GuildId) -> anyhow::Result<String> {
    let Some(room) = current_room(ctx, shared, guild)? else {
        return Ok("どの部屋にもつないでいません（`/ecologger join room:<部屋>`）。".into());
    };
    let joined = shared.joined.lock().unwrap().get(&room).copied();
    let bound = shared.rooms.get(&room)?.and_then(|r| r.discord_channel_id);
    let loggers = shared.room_manager.logger_count(&room).await;
    let talking = shared
        .speaking
        .lock()
        .unwrap()
        .get(&room)
        .is_some_and(|at| at.elapsed() < Duration::from_millis(500));
    let voice = match (joined, bound) {
        (Some((channel, _)), _) => format!("<#{channel}>（接続中）"),
        (None, Some(channel)) => format!("<#{channel}>（入ろうとしている）"),
        (None, None) => "なし".into(),
    };
    Ok([
        format!("部屋: `{room}`（このサーバーにつながっているロガー {loggers}台）"),
        format!("ボイスチャンネル: {voice}"),
        format!(
            "Discord→部屋で送信中: {}",
            if talking { "はい" } else { "いいえ" }
        ),
        format!(
            "音声の中継: {}",
            if shared.audio_active.load(Ordering::Acquire) {
                "このBotが担当"
            } else {
                "待機中（もう一方のBotが担当）"
            }
        ),
    ]
    .join("\n"))
}

async fn audio_command(
    ctx: &Context,
    shared: &Shared,
    command: &CommandInteraction,
    guild: GuildId,
    args: &[ResolvedOption<'_>],
) -> anyhow::Result<String> {
    let Some(room) = current_room(ctx, shared, guild)? else {
        return Ok("どの部屋にもつないでいません（`/ecologger join room:<部屋>`）。".into());
    };
    let mut voice = serde_json::Map::new();
    for arg in args {
        let (Some((name, key, _, min, max)), ResolvedValue::Integer(value)) = (
            AUDIO_OPTIONS.iter().find(|(name, ..)| *name == arg.name),
            &arg.value,
        ) else {
            continue;
        };
        if !(*min..=*max).contains(value) {
            return Ok(format!("`{name}` は {min}〜{max} で指定してください。"));
        }
        let value = if *name == "bitrate" {
            value * 1000
        } else {
            *value
        };
        voice.insert((*key).to_owned(), value.into());
    }
    let changing = !voice.is_empty();
    if changing {
        info!(user = %command.user.tag(), room, ?voice, "/ecologger audio");
    }
    let answer = ask_logger(shared, &room, voice).await;
    let previous = shared.last_config.lock().unwrap().get(&room).cloned();
    Ok(match answer {
        Some(answer) => format!(
            "{}\n{}",
            if changing { "変えました。" } else { "いまの設定です。" },
            audio_text(&answer)
        ),
        None => format!(
            "ロガーから返事がありません（部屋 `{room}` にロガーがいない、または音声が止まっている）。{}",
            previous.map(|c| format!("\n前回の値:\n{}", audio_text(&c))).unwrap_or_default()
        ),
    })
}

/// Send `logger.config` to the room's loggers and wait for the first answer.
async fn ask_logger(
    shared: &Shared,
    room: &str,
    voice: serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    if shared.room_manager.logger_count(room).await == 0 {
        return None;
    }
    let mut answers = shared.logger_config.subscribe();
    let request = serde_json::json!({"type":"logger.config","session_id":0,"data":{"voice":voice}});
    shared
        .room_manager
        .broadcast_control_to_loggers(room, request.to_string())
        .await;
    let answer = tokio::time::timeout(LOGGER_ANSWER_TIMEOUT, async {
        loop {
            match answers.recv().await {
                Ok((from, data)) if from == room => return Some(data),
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
    .await
    .ok()
    .flatten()?;
    shared
        .last_config
        .lock()
        .unwrap()
        .insert(room.to_owned(), answer.clone());
    Some(answer)
}

/// Each line: what it is now, what changing it does, and the option to change it
/// with, so the pit can act on the reply alone.
fn audio_text(config: &serde_json::Value) -> String {
    let voice = &config["voice"];
    let show = |v: &serde_json::Value| match v {
        serde_json::Value::Null => "?".to_owned(),
        v => v.to_string(),
    };
    let at_max = matches!(
        (config["gain_db"].as_f64(), voice["max_gain_db"].as_f64()),
        (Some(gain), Some(max)) if gain >= max - 0.5
    );
    let lowcut = match voice["lowcut_hz"].as_f64() {
        Some(hz) if hz > 0.0 => format!("{} Hz", show(&voice["lowcut_hz"])),
        Some(_) => "なし".into(),
        None => "?".into(),
    };
    let bitrate = voice["bitrate"]
        .as_f64()
        .map(|b| (b / 1000.0).to_string())
        .unwrap_or_else(|| "?".into());
    let mic = if config["mic"].as_bool().unwrap_or(false) {
        config["device"].as_str().unwrap_or("あり").to_owned()
    } else {
        "なし（USBのマイク・イヤホンが挿さっているか）".into()
    };
    [
        format!("**ドライバーの声の大きさ** {} dBFS（-30〜-6、標準 -16）", show(&voice["level_db"])),
        "　声が小さい → 上げる（例 `level:-12`）／大きすぎ・割れる → 下げる".into(),
        format!(
            "**増幅の上限** {} dB（0〜40、標準 30）　いまの増幅 {} dB{}",
            show(&voice["max_gain_db"]),
            show(&config["gain_db"]),
            if at_max { "（上限に達している）" } else { "" }
        ),
        "　上限に達していて声が小さい → 上げる（例 `max_gain:36`）／雑音が大きい → 下げる".into(),
        format!("**低音カット** {lowcut}（0〜400、標準 250）"),
        "　風切り音・ゴーという音が大きい → 上げる（例 `lowcut:350`）／声がこもる・細い → 下げる".into(),
        format!("**ビットレート** {bitrate} kbps（6〜16、標準 6）"),
        "　声がガビガビ → 上げる（例 `bitrate:10`）。上げるほどLTEの通信量が増え、テレメトリを圧迫しうる".into(),
        format!("**ドライバーのイヤホンの音量** {}%（0〜100、標準 80）", show(&voice["volume"])),
        "　ドライバーがピットの声を聞き取れない → 上げる（例 `volume:95`）".into(),
        format!("マイク: {mic}"),
        "複数まとめて変えられる（例 `/ecologger audio level:-12 lowcut:300`）。値はロガーの再起動後も残る".into(),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn downsampler_keeps_voice_and_removes_tones_above_4khz() {
        let rms = |freq: f64| {
            let mut down = Downsampler::default();
            let mut out = Vec::new();
            for block in 0..50 {
                let stereo: Vec<i16> = (0..960)
                    .flat_map(|i| {
                        let t = (block * 960 + i) as f64 / 48_000.0;
                        let v = ((t * std::f64::consts::TAU * freq).sin() * 10_000.0) as i16;
                        [v, v]
                    })
                    .collect();
                out.extend(down.push(&stereo));
            }
            assert_eq!(out.len(), 50 * 160, "20 ms in, 20 ms out");
            let tail = &out[800..];
            (tail.iter().map(|&s| f64::from(s).powi(2)).sum::<f64>() / tail.len() as f64).sqrt()
        };
        assert!(rms(1_000.0) > 6_500.0, "1 kHz passes");
        assert!(
            rms(6_000.0) < 300.0,
            "6 kHz would alias to 2 kHz without the filter"
        );
    }

    fn packet(codec: &mut OpusCodec) -> Vec<u8> {
        let pcm: Vec<i16> = (0..FRAME_SAMPLES)
            .map(|i| ((i as f64 / 8_000.0 * std::f64::consts::TAU * 400.0).sin() * 8_000.0) as i16)
            .collect();
        codec.encode(&pcm).unwrap()
    }

    #[test]
    fn two_talkers_are_mixed_not_queued_one_after_the_other() {
        let mut a = OpusCodec::new().unwrap();
        let mut b = OpusCodec::new().unwrap();
        let mut mix = PlaybackMix::default();
        mix.feed("a", &packet(&mut a));
        mix.feed("b", &packet(&mut b));
        // Both 60 ms packets land on the same stretch of the timeline.
        assert_eq!(
            mix.timeline.len(),
            PREROLL_SAMPLES + FRAME_SAMPLES * UPSAMPLE
        );
        let out = mix.take(mix.timeline.len());
        assert!(out[..PREROLL_SAMPLES].iter().all(|&v| v == 0.0));
        assert!(mix.sources.values().all(|s| s.cursor == 0));
    }

    #[test]
    fn a_talker_too_far_ahead_is_dropped_and_reads_pad_with_silence() {
        let mut codec = OpusCodec::new().unwrap();
        let mut mix = PlaybackMix::default();
        for _ in 0..20 {
            mix.feed("a", &packet(&mut codec));
        }
        assert!(mix.timeline.len() <= MAX_PLAYBACK_SAMPLES);
        let len = mix.timeline.len();
        let out = mix.take(len + 100);
        assert_eq!(out.len(), len + 100);
        assert!(out[len..].iter().all(|&v| v == 0.0));
        assert!(mix.timeline.is_empty());
    }

    #[test]
    fn playback_hands_out_reads_that_do_not_align_to_frames() {
        let mut playback = Playback {
            mix: Arc::new(Mutex::new(PlaybackMix::default())),
            pending: Vec::new(),
        };
        let mut buf = [1_u8; 13];
        assert_eq!(playback.read(&mut buf).unwrap(), 13);
        assert!(buf.iter().all(|&b| b == 0), "silence when nobody talks");
        assert_eq!(playback.pending.len(), 16 - 13);
    }

    #[test]
    fn audio_text_shows_the_logger_values() {
        let text = audio_text(&json!({
            "voice": {"level_db": -16, "max_gain_db": 30, "lowcut_hz": 250, "bitrate": 6000, "volume": 80},
            "gain_db": 30, "mic": true, "device": "USB Audio"
        }));
        assert!(text.contains("-16 dBFS"));
        assert!(text.contains("いまの増幅 30 dB（上限に達している）"));
        assert!(text.contains("250 Hz"));
        assert!(text.contains("6 kbps"));
        assert!(text.contains("80%"));
        assert!(text.contains("マイク: USB Audio"));
        assert!(audio_text(&json!({"voice": {"lowcut_hz": 0}})).contains("**低音カット** なし"));
    }

    #[test]
    fn command_has_the_four_subcommands() {
        let json = serde_json::to_value(command()).unwrap();
        let names: Vec<_> = json["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(names, ["join", "leave", "status", "audio"]);
        assert_eq!(
            json["options"][3]["options"].as_array().unwrap().len(),
            AUDIO_OPTIONS.len()
        );
    }
}
