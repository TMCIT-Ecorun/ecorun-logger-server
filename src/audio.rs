use opus::{Application, Channels, Decoder, Encoder, Error as OpusError};
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
                write!(f, "frame has {size} samples, expected {FRAME_SAMPLES}")
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
}

impl AudioCodec for OpusCodec {
    fn decode_and_validate(&mut self, packet: &[u8]) -> Result<Vec<i16>, AudioError> {
        if packet.len() > MAX_PACKET_SIZE {
            return Err(AudioError::PacketTooLarge(packet.len()));
        }
        let mut pcm = vec![0_i16; FRAME_SAMPLES];
        let samples = self.decoder.decode(packet, &mut pcm, false)?;
        if samples != FRAME_SAMPLES {
            return Err(AudioError::InvalidFrameSize(samples));
        }
        Ok(pcm)
    }
    fn encode(&mut self, pcm: &[i16]) -> Result<Vec<u8>, AudioError> {
        if pcm.len() != FRAME_SAMPLES {
            return Err(AudioError::InvalidFrameSize(pcm.len()));
        }
        let mut packet = vec![0_u8; MAX_PACKET_SIZE];
        let size = self.encoder.encode(pcm, &mut packet)?;
        packet.truncate(size);
        Ok(packet)
    }
}
