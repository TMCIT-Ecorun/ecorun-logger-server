use opus::{Application, Bitrate, Channels, Decoder, Encoder, Error as OpusError, Repacketizer};
use std::fmt;

pub const SAMPLE_RATE: u32 = 8_000;
pub const FRAME_MS: u16 = 60;
pub const FRAME_SAMPLES: usize = 480;
pub const MAX_PACKET_SIZE: usize = 1_275;

#[derive(Debug)]
pub enum AudioError {
    Codec(OpusError),
    InvalidFrameSize(usize),
    PacketTooLarge(usize),
}

impl fmt::Display for AudioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(f, "Opus error: {error}"),
            Self::InvalidFrameSize(size) => {
                write!(
                    f,
                    "frame has {size} samples, expected {FRAME_SAMPLES} or {}",
                    FRAME_SAMPLES * 2
                )
            }
            Self::PacketTooLarge(size) => {
                write!(f, "packet is {size} bytes, maximum is {MAX_PACKET_SIZE}")
            }
        }
    }
}
impl std::error::Error for AudioError {}
impl From<OpusError> for AudioError {
    fn from(value: OpusError) -> Self {
        Self::Codec(value)
    }
}

pub trait AudioCodec {
    fn decode_and_validate(&mut self, packet: &[u8]) -> Result<Vec<i16>, AudioError>;
    fn encode(&mut self, pcm: &[i16]) -> Result<Vec<u8>, AudioError>;
}

pub struct OpusCodec {
    encoder: Encoder,
    decoder: Decoder,
}

impl OpusCodec {
    pub fn new() -> Result<Self, AudioError> {
        Ok(Self {
            encoder: Encoder::new(8000, Channels::Mono, Application::Voip)?,
            decoder: Decoder::new(8000, Channels::Mono)?,
        })
    }

    pub fn set_bitrate(&mut self, bitrate: u32) -> Result<(), AudioError> {
        self.encoder
            .set_bitrate(Bitrate::Bits(bitrate as i32))
            .map_err(AudioError::from)
    }
}

impl AudioCodec for OpusCodec {
    fn decode_and_validate(&mut self, packet: &[u8]) -> Result<Vec<i16>, AudioError> {
        if packet.len() > MAX_PACKET_SIZE {
            return Err(AudioError::PacketTooLarge(packet.len()));
        }
        // WebCodecs may emit 20 ms Opus packets even when the input AudioData
        // is supplied in 60 ms blocks. Opus packets can also contain longer
        // frames, so decode into the codec's maximum 120 ms buffer.
        let mut pcm = vec![0_i16; SAMPLE_RATE as usize * 120 / 1000];
        let samples = self.decoder.decode(packet, &mut pcm, false)?;
        pcm.truncate(samples);
        Ok(pcm)
    }
    fn encode(&mut self, pcm: &[i16]) -> Result<Vec<u8>, AudioError> {
        if pcm.len() != FRAME_SAMPLES && pcm.len() != FRAME_SAMPLES * 2 {
            return Err(AudioError::InvalidFrameSize(pcm.len()));
        }
        if pcm.len() == FRAME_SAMPLES {
            let mut packet = vec![0_u8; MAX_PACKET_SIZE];
            let size = self.encoder.encode(pcm, &mut packet)?;
            packet.truncate(size);
            return Ok(packet);
        }

        // libopus encodes at most 60 ms in one call. A 120 ms wire packet
        // contains two consecutive 60 ms frames in a single Opus packet.
        let mut first = vec![0_u8; MAX_PACKET_SIZE];
        let mut second = vec![0_u8; MAX_PACKET_SIZE];
        let first_size = self.encoder.encode(&pcm[..FRAME_SAMPLES], &mut first)?;
        let second_size = self.encoder.encode(&pcm[FRAME_SAMPLES..], &mut second)?;
        let mut packet = vec![0_u8; MAX_PACKET_SIZE];
        let size = Repacketizer::new()?
            .combine(&[&first[..first_size], &second[..second_size]], &mut packet)?;
        packet.truncate(size);
        Ok(packet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_two_60ms_frames_as_one_120ms_packet() {
        let mut encoder = OpusCodec::new().unwrap();
        encoder.set_bitrate(6_000).unwrap();
        let pcm: Vec<i16> = (0..960)
            .map(|index| {
                ((index as f64 / 8_000.0 * 440.0 * std::f64::consts::TAU).sin() * 8_000.0) as i16
            })
            .collect();
        let packet = encoder.encode(&pcm).unwrap();
        assert!(packet.len() <= MAX_PACKET_SIZE);
        let mut decoder = Decoder::new(8_000, Channels::Mono).unwrap();
        let mut decoded = [0_i16; 960];
        assert_eq!(decoder.decode(&packet, &mut decoded, false).unwrap(), 960);
    }
}
