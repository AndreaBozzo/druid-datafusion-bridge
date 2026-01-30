use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

use super::generic_indexed::GenericIndexedV1;
use crate::compression::{CompressionStrategy, decompress_block};
use crate::error::{DruidSegmentError, Result};

/// Reader for Druid's CompressedColumnarInts (CompressedVSizeColumnarIntsSupplier).
///
/// Similar to CompressedColumnarLongs but stores integer values.
///
/// Header layout (version 0x02):
/// ```text
/// [version: u8 = 0x02]
/// [total_size: i32]     -- total number of int values
/// [size_per: i32]       -- ints per compressed block
/// [num_bytes: u8]       -- bytes per integer (1-4)
/// [compression: u8]     -- CompressionStrategy ID
/// [GenericIndexed<ByteBuffer>]  -- compressed blocks
/// ```
pub struct CompressedColumnarInts<'a> {
    total_size: usize,
    size_per: usize,
    num_bytes: usize,
    compression: CompressionStrategy,
    blocks: GenericIndexedV1<'a>,
}

impl<'a> CompressedColumnarInts<'a> {
    /// Parse from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> Result<Self> {
        if data.len() < 11 {
            return Err(DruidSegmentError::InvalidData(
                "CompressedColumnarInts: data too short".into(),
            ));
        }

        let version = data[0];
        if version != 0x02 {
            return Err(DruidSegmentError::InvalidData(format!(
                "CompressedColumnarInts: unsupported version {:#x}",
                version
            )));
        }

        let mut cursor = Cursor::new(&data[1..]);
        let total_size = cursor.read_i32::<BigEndian>()? as usize;
        let size_per = cursor.read_i32::<BigEndian>()? as usize;

        let num_bytes = data[9] as usize;
        if num_bytes == 0 || num_bytes > 4 {
            return Err(DruidSegmentError::InvalidData(format!(
                "CompressedColumnarInts: invalid num_bytes {}",
                num_bytes
            )));
        }

        let compression = CompressionStrategy::from_id(data[10])?;
        let blocks = GenericIndexedV1::from_bytes(&data[11..])?;

        Ok(Self {
            total_size,
            size_per,
            num_bytes,
            compression,
            blocks,
        })
    }

    /// Total number of int values.
    pub fn len(&self) -> usize {
        self.total_size
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.total_size == 0
    }

    /// Decompress all values into a Vec<u32>.
    pub fn decompress_all(&self) -> Result<Vec<u32>> {
        let mut result = Vec::with_capacity(self.total_size);
        let num_blocks = self.blocks.len();

        for block_idx in 0..num_blocks {
            let block_data = self.blocks.get(block_idx)?.ok_or_else(|| {
                DruidSegmentError::InvalidData(format!(
                    "CompressedColumnarInts: null block at index {}",
                    block_idx
                ))
            })?;

            let remaining = self.total_size - result.len();
            let values_in_block = remaining.min(self.size_per);
            let decompressed_size = values_in_block * self.num_bytes;

            let decompressed = decompress_block(self.compression, block_data, decompressed_size)?;

            // Read big-endian unsigned integers of variable width
            for i in 0..values_in_block {
                let offset = i * self.num_bytes;
                let bytes = &decompressed[offset..offset + self.num_bytes];
                let mut value: u32 = 0;
                for &b in bytes {
                    value = (value << 8) | (b as u32);
                }
                result.push(value);
            }
        }

        Ok(result)
    }
}
