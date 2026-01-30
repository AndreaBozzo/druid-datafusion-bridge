use crate::error::{DruidSegmentError, Result};

/// Compression strategies used by Druid for columnar data blocks.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionStrategy {
    /// LZF compression (legacy).
    Lzf,
    /// LZ4 block compression (modern default).
    Lz4,
    /// Zstandard compression.
    Zstd,
    /// Data is stored uncompressed.
    Uncompressed,
    /// No compression marker.
    None,
}

impl CompressionStrategy {
    /// Parse a compression strategy from its single-byte identifier.
    pub fn from_id(id: u8) -> Result<Self> {
        match id {
            0x00 => Ok(Self::Lzf),
            0x01 => Ok(Self::Lz4),
            0x02 => Ok(Self::Zstd),
            0xFF => Ok(Self::Uncompressed),
            0xFE => Ok(Self::None),
            other => Err(DruidSegmentError::UnsupportedCompression(other)),
        }
    }
}

/// Decompress a block of data using the given strategy.
/// `decompressed_size` is the expected output size in bytes.
pub fn decompress_block(
    strategy: CompressionStrategy,
    compressed: &[u8],
    decompressed_size: usize,
) -> Result<Vec<u8>> {
    match strategy {
        CompressionStrategy::Lz4 => lz4_flex::block::decompress(compressed, decompressed_size)
            .map_err(|e| DruidSegmentError::DecompressionError(e.to_string())),
        CompressionStrategy::Uncompressed | CompressionStrategy::None => Ok(compressed.to_vec()),
        CompressionStrategy::Lzf => Err(DruidSegmentError::UnsupportedCompression(0x00)),
        CompressionStrategy::Zstd => Err(DruidSegmentError::UnsupportedCompression(0x02)),
    }
}
