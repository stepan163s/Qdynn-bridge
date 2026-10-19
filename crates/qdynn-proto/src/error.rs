use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("Invalid frame or batch: {0}")]
    InvalidFrame(&'static str),
    #[error("Frame buffer is too short: required at least {expected} bytes, got {actual}")]
    BufferTooShort { expected: usize, actual: usize },

    #[error("Unknown or unsupported frame type: 0x{0:02x}")]
    UnknownFrameType(u8),

    #[error(
        "Payload length mismatch: header specifies {header_len} bytes, available {available} bytes"
    )]
    PayloadLengthMismatch { header_len: u32, available: usize },

    #[error("Payload too large: length {actual} exceeds maximum permitted {max}")]
    PayloadTooLarge { actual: usize, max: usize },

    #[error("Invalid stream ID for frame {frame_type:?}: expected {expected}, got {actual}")]
    InvalidStreamId {
        frame_type: &'static str,
        expected: &'static str,
        actual: u32,
    },

    #[error("Invalid WINDOW delta: delta must be nonzero")]
    InvalidWindowDelta,

    #[error("Invalid secret format: {0}")]
    InvalidSecret(String),

    #[error("Invalid capability format: {0}")]
    InvalidCapability(String),
}
