//! Per-recipient Opus transcoding for browser audio delivery.
//!
//! Incoming packets must contain 8 kHz, mono Opus audio. Each `source` has
//! its own long-lived decoder so Opus prediction survives packet boundaries.
//! Each `(source, recipient)` has an independent encoder and PCM remainder;
//! switching speakers or receiver settings cannot mix unrelated audio.
//!
//! For fan-out to several recipients, decode *once* with `decode_packet`, then
//! call `encode_for` for each recipient. Calling `transcode_for` repeatedly on
//! the same input packet would advance the source decoder more than once.
//! Packets from the same source must be supplied in their original order.
//!
//! The module performs no network I/O. `try_send_packets` is provided for
//! bounded, nonblocking delivery; audio is discarded when the queue is full.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
};

use opus::{Application, Bitrate, Channels, Decoder, Encoder, Error as OpusError, Repacketizer};
#[cfg(test)]
use tokio::sync::mpsc;
use tokio::sync::Mutex;

pub const SAMPLE_RATE: u32 = 8_000;
pub const MIN_BITRATE: u32 = 4_000;
pub const MAX_BITRATE: u32 = 32_000;
pub const SUPPORTED_FRAME_MS: [u16; 4] = [20, 40, 60, 120];

const MAX_INPUT_PACKET_BYTES: usize = 4_000;
const MAX_OUTPUT_PACKET_BYTES: usize = 4_000;
const MAX_DECODED_SAMPLES: usize = 960; // 120 ms at 8 kHz.
#[cfg(test)]
const DEFAULT_RECIPIENT: Option<String> = None;

#[derive(Debug)]
pub enum AudioDeliveryError {
    InvalidBitrate(u32),
    InvalidFrameMs(u16),
    InvalidPacketSize(usize),
    Codec(OpusError),
}

impl fmt::Display for AudioDeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBitrate(value) => write!(
                f,
                "unsupported Opus bitrate {value} (expected {MIN_BITRATE}..={MAX_BITRATE})"
            ),
            Self::InvalidFrameMs(value) => {
                write!(f, "unsupported Opus packet duration {value} ms")
            }
            Self::InvalidPacketSize(value) => {
                write!(f, "invalid incoming Opus packet size: {value} bytes")
            }
            Self::Codec(error) => write!(f, "Opus codec error: {error}"),
        }
    }
}

impl std::error::Error for AudioDeliveryError {}

impl From<OpusError> for AudioDeliveryError {
    fn from(error: OpusError) -> Self {
        Self::Codec(error)
    }
}

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SendReport {
    pub sent: usize,
    pub dropped: usize,
}

struct EncoderStream {
    encoder: Encoder,
    bitrate: u32,
    pending: VecDeque<i16>,
}

impl EncoderStream {
    fn new(bitrate: u32) -> Result<Self, AudioDeliveryError> {
        let mut encoder = Encoder::new(SAMPLE_RATE, Channels::Mono, Application::Voip)?;
        encoder.set_bitrate(Bitrate::Bits(bitrate as i32))?;
        Ok(Self {
            encoder,
            bitrate,
            pending: VecDeque::new(),
        })
    }

    fn set_bitrate(&mut self, bitrate: u32) -> Result<(), AudioDeliveryError> {
        if self.bitrate != bitrate {
            self.encoder.set_bitrate(Bitrate::Bits(bitrate as i32))?;
            self.bitrate = bitrate;
        }
        Ok(())
    }

    fn append_and_encode(
        &mut self,
        samples: &[i16],
        frame_ms: u16,
    ) -> Result<Vec<Vec<u8>>, AudioDeliveryError> {
        let frame_samples = SAMPLE_RATE as usize * frame_ms as usize / 1_000;
        self.pending.extend(samples.iter().copied());
        let mut packets = Vec::new();
        while self.pending.len() >= frame_samples {
            let pcm: Vec<_> = self.pending.drain(..frame_samples).collect();
            packets.push(self.encode_frame(&pcm)?);
        }
        Ok(packets)
    }

    fn encode_frame(&mut self, pcm: &[i16]) -> Result<Vec<u8>, AudioDeliveryError> {
        let mut output = vec![0_u8; MAX_OUTPUT_PACKET_BYTES];
        let size = if pcm.len() == MAX_DECODED_SAMPLES {
            // An Opus encoder can only process up to 60 ms at once.
            // Repacketize the two 60 ms frames into one 120 ms wire packet.
            let half = MAX_DECODED_SAMPLES / 2;
            let mut first = vec![0_u8; MAX_OUTPUT_PACKET_BYTES];
            let mut second = vec![0_u8; MAX_OUTPUT_PACKET_BYTES];
            let first_len = self.encoder.encode(&pcm[..half], &mut first)?;
            let second_len = self.encoder.encode(&pcm[half..], &mut second)?;
            Repacketizer::new()?
                .combine(&[&first[..first_len], &second[..second_len]], &mut output)?
        } else {
            self.encoder.encode(pcm, &mut output)?
        };
        output.truncate(size);
        Ok(output)
    }
}

#[derive(Default)]
struct DeliveryState {
    sources: HashMap<String, Decoder>,
    // None is the shared-profile convenience stream used by `transcode`.
    recipients: HashMap<(String, Option<String>), EncoderStream>,
}

/// Shared transcoder. Clones share decoder and encoder state.
///
/// The lock is held only for the in-memory codec work, never for I/O or a
/// blocking send. Call `forget_source`/`forget_recipient` on disconnect to
/// reclaim codec state and buffered samples.
#[derive(Default)]
pub struct AudioDelivery {
    state: Mutex<DeliveryState>,
}

impl AudioDelivery {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode one incoming Opus packet, preserving this source's decoder state.
    /// Use the returned PCM for all recipients of this input packet.
    pub async fn decode_packet(
        &self,
        source: &str,
        packet: &[u8],
    ) -> Result<Vec<i16>, AudioDeliveryError> {
        let mut state = self.state.lock().await;
        decode_packet(&mut state, source, packet)
    }

    /// Encode decoded 8 kHz mono PCM for one recipient, keeping its own stream
    /// state per source. A frame may be delayed until enough samples arrive.
    /// Bitrate and frame duration changes are applied on the next input.
    pub async fn encode_for(
        &self,
        recipient: &str,
        source: &str,
        samples: &[i16],
        bitrate: u32,
        frame_ms: u16,
    ) -> Result<Vec<Vec<u8>>, AudioDeliveryError> {
        validate_settings(bitrate, frame_ms)?;
        let mut state = self.state.lock().await;
        encode_for(
            &mut state,
            source,
            Some(recipient.to_owned()),
            samples,
            bitrate,
            frame_ms,
        )
    }

    /// Convenience for a single recipient: decode one packet and encode to the
    /// requested format atomically. Do not invoke once per recipient with the
    /// *same* source packet; instead use `decode_packet` + `encode_for`.
    #[cfg(test)]
    pub async fn transcode_for(
        &self,
        recipient: &str,
        source: &str,
        packet: &[u8],
        bitrate: u32,
        frame_ms: u16,
    ) -> Result<Vec<Vec<u8>>, AudioDeliveryError> {
        self.transcode_inner(
            source,
            Some(recipient.to_owned()),
            packet,
            bitrate,
            frame_ms,
        )
        .await
    }

    /// Simple one-output-profile interface. Call once per incoming packet;
    /// fan out the resulting packets to all clients sharing this format.
    /// On errors, drops the packet. Use `try_transcode` to inspect the error.
    #[cfg(test)]
    pub async fn transcode(
        &self,
        source: &str,
        packet: &[u8],
        bitrate: u32,
        frame_ms: u16,
    ) -> Vec<Vec<u8>> {
        self.try_transcode(source, packet, bitrate, frame_ms)
            .await
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub async fn try_transcode(
        &self,
        source: &str,
        packet: &[u8],
        bitrate: u32,
        frame_ms: u16,
    ) -> Result<Vec<Vec<u8>>, AudioDeliveryError> {
        self.transcode_inner(source, DEFAULT_RECIPIENT, packet, bitrate, frame_ms)
            .await
    }

    #[cfg(test)]
    async fn transcode_inner(
        &self,
        source: &str,
        recipient: Option<String>,
        packet: &[u8],
        bitrate: u32,
        frame_ms: u16,
    ) -> Result<Vec<Vec<u8>>, AudioDeliveryError> {
        validate_settings(bitrate, frame_ms)?;
        let mut state = self.state.lock().await;
        let pcm = decode_packet(&mut state, source, packet)?;
        encode_for(&mut state, source, recipient, &pcm, bitrate, frame_ms)
    }

    /// Drop a disconnected input source and its output codec streams.
    pub async fn forget_source(&self, source: &str) {
        let mut state = self.state.lock().await;
        state.sources.remove(source);
        state.recipients.retain(|(sender, _), _| sender != source);
    }

    /// Drop all output codec streams for a disconnected browser.
    pub async fn forget_recipient(&self, recipient: &str) {
        self.state
            .lock()
            .await
            .recipients
            .retain(|(_, sink), _| sink.as_deref() != Some(recipient));
    }

    /// Attempt bounded outbound sends without awaiting channel capacity.
    /// A full/closed queue drops audio; callers should prioritize telemetry.
    #[cfg(test)]
    pub fn try_send_packets(
        sender: &mpsc::Sender<Vec<u8>>,
        packets: impl IntoIterator<Item = Vec<u8>>,
    ) -> SendReport {
        let mut result = SendReport::default();
        for packet in packets {
            if sender.try_send(packet).is_ok() {
                result.sent += 1;
            } else {
                result.dropped += 1;
            }
        }
        result
    }
}

fn validate_settings(bitrate: u32, frame_ms: u16) -> Result<(), AudioDeliveryError> {
    if !(MIN_BITRATE..=MAX_BITRATE).contains(&bitrate) {
        return Err(AudioDeliveryError::InvalidBitrate(bitrate));
    }
    if !SUPPORTED_FRAME_MS.contains(&frame_ms) {
        return Err(AudioDeliveryError::InvalidFrameMs(frame_ms));
    }
    Ok(())
}

fn decode_packet(
    state: &mut DeliveryState,
    source: &str,
    packet: &[u8],
) -> Result<Vec<i16>, AudioDeliveryError> {
    if packet.is_empty() || packet.len() > MAX_INPUT_PACKET_BYTES {
        return Err(AudioDeliveryError::InvalidPacketSize(packet.len()));
    }
    if !state.sources.contains_key(source) {
        state.sources.insert(
            source.to_owned(),
            Decoder::new(SAMPLE_RATE, Channels::Mono)?,
        );
    }
    let decoder = state.sources.get_mut(source).expect("decoder inserted");
    let mut pcm = vec![0_i16; MAX_DECODED_SAMPLES];
    let count = decoder.decode(packet, &mut pcm, false)?;
    pcm.truncate(count);
    Ok(pcm)
}

fn encode_for(
    state: &mut DeliveryState,
    source: &str,
    recipient: Option<String>,
    samples: &[i16],
    bitrate: u32,
    frame_ms: u16,
) -> Result<Vec<Vec<u8>>, AudioDeliveryError> {
    let key = (source.to_owned(), recipient);
    if !state.recipients.contains_key(&key) {
        state
            .recipients
            .insert(key.clone(), EncoderStream::new(bitrate)?);
    }
    let encoder = state.recipients.get_mut(&key).expect("encoder inserted");
    encoder.set_bitrate(bitrate)?;
    encoder.append_and_encode(samples, frame_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_encoder() -> Encoder {
        let mut encoder = Encoder::new(SAMPLE_RATE, Channels::Mono, Application::Voip).unwrap();
        encoder.set_bitrate(Bitrate::Bits(16_000)).unwrap();
        encoder
    }

    fn input_packet(encoder: &mut Encoder, frame_ms: usize) -> Vec<u8> {
        let size = SAMPLE_RATE as usize * frame_ms / 1000;
        let input: Vec<i16> = (0..size)
            .map(|i| ((i as f64 / 8_000.0 * 440.0 * std::f64::consts::TAU).sin() * 6_000.0) as i16)
            .collect();
        let mut packet = vec![0_u8; MAX_OUTPUT_PACKET_BYTES];
        let count = encoder.encode(&input, &mut packet).unwrap();
        packet.truncate(count);
        packet
    }

    fn decoded_duration_ms(packet: &[u8], decoder: &mut Decoder) -> usize {
        let mut pcm = [0_i16; MAX_DECODED_SAMPLES];
        decoder.decode(packet, &mut pcm, false).unwrap() * 1000 / SAMPLE_RATE as usize
    }

    #[tokio::test]
    async fn splits_60ms_packet_into_three_20ms_packets() {
        let delivery = AudioDelivery::new();
        let packets = delivery
            .transcode_for(
                "web-a",
                "logger",
                &input_packet(&mut source_encoder(), 60),
                4_000,
                20,
            )
            .await
            .unwrap();
        assert_eq!(packets.len(), 3);
        let mut decoder = Decoder::new(SAMPLE_RATE, Channels::Mono).unwrap();
        for packet in packets {
            assert_eq!(decoded_duration_ms(&packet, &mut decoder), 20);
        }
    }

    #[tokio::test]
    async fn combines_two_60ms_packets_into_one_120ms_packet() {
        let delivery = AudioDelivery::new();
        let mut encoder = source_encoder();
        let first = delivery
            .transcode_for(
                "web-a",
                "logger",
                &input_packet(&mut encoder, 60),
                6_000,
                120,
            )
            .await
            .unwrap();
        assert!(first.is_empty());
        let second = delivery
            .transcode_for(
                "web-a",
                "logger",
                &input_packet(&mut encoder, 60),
                6_000,
                120,
            )
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        let mut decoder = Decoder::new(SAMPLE_RATE, Channels::Mono).unwrap();
        assert_eq!(decoded_duration_ms(&second[0], &mut decoder), 120);
    }

    #[tokio::test]
    async fn supports_40ms_and_60ms_profiles_without_losing_samples() {
        let delivery = AudioDelivery::new();
        let mut source = source_encoder();
        let mut decoder_40 = Decoder::new(SAMPLE_RATE, Channels::Mono).unwrap();
        let mut decoder_60 = Decoder::new(SAMPLE_RATE, Channels::Mono).unwrap();
        let mut received_40 = 0;
        for _ in 0..2 {
            let pcm = delivery
                .decode_packet("speaker", &input_packet(&mut source, 60))
                .await
                .unwrap();
            for packet in delivery
                .encode_for("receiver-40", "speaker", &pcm, 8_000, 40)
                .await
                .unwrap()
            {
                received_40 += decoded_duration_ms(&packet, &mut decoder_40);
            }
            let packets = delivery
                .encode_for("receiver-60", "speaker", &pcm, 16_000, 60)
                .await
                .unwrap();
            assert_eq!(packets.len(), 1);
            assert_eq!(decoded_duration_ms(&packets[0], &mut decoder_60), 60);
        }
        assert_eq!(received_40, 120);
    }

    #[tokio::test]
    async fn decodes_120ms_incoming_repacketized_opus() {
        let mut source = source_encoder();
        let first = input_packet(&mut source, 60);
        let second = input_packet(&mut source, 60);
        let mut combined = vec![0_u8; MAX_OUTPUT_PACKET_BYTES];
        let size = Repacketizer::new()
            .unwrap()
            .combine(&[&first, &second], &mut combined)
            .unwrap();
        combined.truncate(size);

        let delivery = AudioDelivery::new();
        let result = delivery.transcode("logger", &combined, 6_000, 20).await;
        assert_eq!(result.len(), 6);
        let mut decoder = Decoder::new(SAMPLE_RATE, Channels::Mono).unwrap();
        assert!(result
            .iter()
            .all(|packet| decoded_duration_ms(packet, &mut decoder) == 20));
    }

    #[tokio::test]
    async fn fanout_reuses_decoded_pcm_and_keeps_receiver_profiles_independent() {
        let delivery = AudioDelivery::new();
        let mut encoder = source_encoder();
        for _ in 0..2 {
            let pcm = delivery
                .decode_packet("speaker", &input_packet(&mut encoder, 60))
                .await
                .unwrap();
            let short = delivery
                .encode_for("web-a", "speaker", &pcm, 32_000, 20)
                .await
                .unwrap();
            let long = delivery
                .encode_for("web-b", "speaker", &pcm, 4_000, 120)
                .await
                .unwrap();
            assert_eq!(short.len(), 3);
            if !long.is_empty() {
                assert_eq!(long.len(), 1);
                let mut decoder = Decoder::new(SAMPLE_RATE, Channels::Mono).unwrap();
                assert_eq!(decoded_duration_ms(&long[0], &mut decoder), 120);
            }
        }
    }

    #[tokio::test]
    async fn maintains_remainder_across_frame_duration_change() {
        let delivery = AudioDelivery::new();
        let pcm = vec![100_i16; 160]; // 20 ms, below the original 60 ms setting.
        assert!(delivery
            .encode_for("a", "b", &pcm, 6_000, 60)
            .await
            .unwrap()
            .is_empty());
        let result = delivery.encode_for("a", "b", &[], 8_000, 20).await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn stopping_source_clears_decoder_and_all_recipient_buffers() {
        let delivery = AudioDelivery::new();
        let packet = input_packet(&mut source_encoder(), 60);
        let pcm = delivery.decode_packet("speaker", &packet).await.unwrap();
        delivery
            .encode_for("web-a", "speaker", &pcm, 8_000, 120)
            .await
            .unwrap();
        delivery
            .encode_for("web-b", "speaker", &pcm, 8_000, 20)
            .await
            .unwrap();
        {
            let state = delivery.state.lock().await;
            assert!(state.sources.contains_key("speaker"));
            assert_eq!(state.recipients.len(), 2);
        }
        delivery.forget_source("speaker").await;
        {
            let state = delivery.state.lock().await;
            assert!(!state.sources.contains_key("speaker"));
            assert!(state.recipients.is_empty());
        }
        // A restarted source cannot inherit the previous 60 ms remainder.
        assert!(delivery
            .encode_for("web-a", "speaker", &pcm, 8_000, 120)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn rejects_invalid_options_and_packets() {
        let delivery = AudioDelivery::new();
        let packet = input_packet(&mut source_encoder(), 20);
        assert!(matches!(
            delivery.try_transcode("src", &packet, 3_999, 20).await,
            Err(AudioDeliveryError::InvalidBitrate(3_999))
        ));
        assert!(matches!(
            delivery.try_transcode("src", &packet, 8_000, 10).await,
            Err(AudioDeliveryError::InvalidFrameMs(10))
        ));
        assert!(matches!(
            delivery.try_transcode("src", &[], 8_000, 20).await,
            Err(AudioDeliveryError::InvalidPacketSize(0))
        ));
    }

    #[test]
    fn try_send_drops_packets_instead_of_waiting_for_capacity() {
        let (tx, mut rx) = mpsc::channel(1);
        let report = AudioDelivery::try_send_packets(&tx, vec![vec![1], vec![2], vec![3]]);
        assert_eq!(
            report,
            SendReport {
                sent: 1,
                dropped: 2
            }
        );
        assert_eq!(rx.try_recv().unwrap(), vec![1]);
    }
}
