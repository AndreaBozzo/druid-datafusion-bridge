use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

use super::generic_indexed::GenericIndexedV1;
use crate::compression::{CompressionStrategy, decompress_block};
use crate::error::{DruidSegmentError, Result};

/// Reader for Druid's CompressedColumnarLongs format.
///
/// Header layout (version 0x02):
/// ```text
/// [version: u8 = 0x02]
/// [total_size: i32]     -- total number of long values
/// [size_per: i32]       -- longs per compressed block
/// [compression: u8]     -- CompressionStrategy ID
/// [GenericIndexed<ByteBuffer>]  -- compressed blocks
/// ```
///
/// Each block in the GenericIndexed decompresses to an array of `size_per`
/// i64 values (big-endian), except possibly the last block which may be shorter.
pub struct CompressedColumnarLongs<'a> {
    total_size: usize,
    size_per: usize,
    compression: CompressionStrategy,
    blocks: GenericIndexedV1<'a>,
}

impl<'a> CompressedColumnarLongs<'a> {
    /// Parse from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> Result<Self> {
        if data.len() < 10 {
            return Err(DruidSegmentError::InvalidData(
                "CompressedColumnarLongs: data too short".into(),
            ));
        }

        let version = data[0];
        let mut cursor = Cursor::new(&data[1..]);
        let total_size = cursor.read_i32::<BigEndian>()? as usize;
        let size_per = cursor.read_i32::<BigEndian>()? as usize;

        let (compression, blocks_offset) = match version {
            0x01 => {
                // V1: LZF compression implied
                (CompressionStrategy::Lzf, 9)
            }
            0x02 => {
                // V2: explicit compression byte
                if data.len() < 11 {
                    return Err(DruidSegmentError::InvalidData(
                        "CompressedColumnarLongs v2: data too short for compression byte".into(),
                    ));
                }
                let compression = CompressionStrategy::from_id(data[9])?;
                (compression, 10)
            }
            other => {
                return Err(DruidSegmentError::InvalidData(format!(
                    "CompressedColumnarLongs: unsupported version {:#x}",
                    other
                )));
            }
        };

        let blocks = GenericIndexedV1::from_bytes(&data[blocks_offset..])?;

        Ok(Self {
            total_size,
            size_per,
            compression,
            blocks,
        })
    }

    /// Total number of long values.
    pub fn len(&self) -> usize {
        self.total_size
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.total_size == 0
    }

    /// Decompress all values into a Vec<i64>.
    pub fn decompress_all(&self) -> Result<Vec<i64>> {
        let mut result = Vec::with_capacity(self.total_size);
        let num_blocks = self.blocks.len();

        for block_idx in 0..num_blocks {
            let block_data = self.blocks.get(block_idx)?.ok_or_else(|| {
                DruidSegmentError::InvalidData(format!(
                    "CompressedColumnarLongs: null block at index {}",
                    block_idx
                ))
            })?;

            // Determine expected decompressed size for this block
            let remaining = self.total_size - result.len();
            let values_in_block = remaining.min(self.size_per);
            let decompressed_size = values_in_block * 8; // 8 bytes per i64

            let decompressed = decompress_block(self.compression, block_data, decompressed_size)?;

            // Read big-endian i64 values from decompressed bytes
            let mut cursor = Cursor::new(&decompressed);
            for _ in 0..values_in_block {
                let value = cursor.read_i64::<BigEndian>()?;
                result.push(value);
            }
        }

        Ok(result)
    }
}
