use thiserror::Error;

#[derive(Error, Debug)]
pub enum DruidSegmentError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid segment version: expected 9, got {0}")]
    InvalidVersion(i32),

    #[error("Invalid smoosh metadata: {0}")]
    InvalidSmooshMeta(String),

    #[error("Logical file not found in smoosh: {0}")]
    LogicalFileNotFound(String),

    #[error("Unsupported compression strategy: {0:#x}")]
    UnsupportedCompression(u8),

    #[error("Unsupported column type: {0}")]
    UnsupportedColumnType(String),

    #[error("Invalid GenericIndexed version: {0}")]
    InvalidGenericIndexedVersion(u8),

    #[error("Decompression error: {0}")]
    DecompressionError(String),

    #[error("JSON deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Column descriptor error: {0}")]
    ColumnDescriptorError(String),

    #[error("Invalid binary data: {0}")]
    InvalidData(String),

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("DataFusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
}

pub type Result<T> = std::result::Result<T, DruidSegmentError>;
