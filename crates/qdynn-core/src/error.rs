use qdynn_proto::ProtoError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Protocol error: {0}")]
    Proto(#[from] ProtoError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Session not found or expired: {0}")]
    SessionNotFound(String),

    #[error("Invalid bootstrap token: {0}")]
    InvalidBootstrapToken(String),

    #[error("Bootstrap token expired")]
    BootstrapTokenExpired,

    #[error("Stream not found or closed: {0}")]
    StreamNotFound(u32),

    #[error("Stream limit exceeded")]
    StreamLimitExceeded,

    #[error("Failed to connect to MTProxy backend at {addr}: {source}")]
    BackendConnectionFailed {
        addr: String,
        source: std::io::Error,
    },
}
