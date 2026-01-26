use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: {0}")]
    PartitionNotFound(u32),

    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
