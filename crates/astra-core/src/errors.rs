use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("required revision has been compacted (compact_revision={0})")]
    Compacted(i64),

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("key not found")]
    KeyNotFound,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Error)]
pub enum TieringError {
    #[error("aws sdk error: {0}")]
    AwsSdk(String),

    #[error("checksum mismatch")]
    ChecksumMismatch,

    #[error("store error: {0}")]
    Store(#[from] StoreError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
