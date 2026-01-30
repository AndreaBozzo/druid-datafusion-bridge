use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

use super::generic_indexed::GenericIndexedV1;
use crate::compression::{CompressionStrategy, decompress_block};
use crate::error::{DruidSegmentError, Result};

/// Reader for Druid's CompressedColumnarDoubles format.
///
/// Identical structure to CompressedColumnarLongs but stores f64 values.
///
/// Header layout:
/// ```text
/// [version: u8 = 0x02]
/// [total_size: i32]     -- total number of double values
/// [size_per: i32]       -- doubles per compressed block
/// [compression: u8]     -- CompressionStrategy ID
/// [GenericIndexed<ByteBuffer>]  -- compressed blocks
/// ```
pub struct CompressedColumnarDoubles<'a> {
    total_size: usize,
    size_per: usize,
    compression: CompressionStrategy,
    blocks: GenericIndexedV1<'a>,
}

impl<'a> CompressedColumnarDoubles<'a> {
    /// Parse from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> Result<Self> {
        if data.len() < 11 {
            return Err(DruidSegmentError::InvalidData(
                "CompressedColumnarDoubles: data too short".into(),
            ));
        }

        let version = data[0];
        if version != 0x02 {
            return Err(DruidSegmentError::InvalidData(format!(
                "CompressedColumnarDoubles: unsupported version {:#x}",
                version
            )));
        }

        let mut cursor = Cursor::new(&data[1..]);
        let total_size = cursor.read_i32::<BigEndian>()? as usize;
        let size_per = cursor.read_i32::<BigEndian>()? as usize;

        let compression = CompressionStrategy::from_id(data[9])?;
        let blocks = GenericIndexedV1::from_bytes(&data[10..])?;

        Ok(Self {
            total_size,
            size_per,
            compression,
            blocks,
        })
    }

    /// Total number of double values.
    pub fn len(&self) -> usize {
        self.total_size
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.total_size == 0
    }

    /// Decompress all values into a Vec<f64>.
    pub fn decompress_all(&self) -> Result<Vec<f64>> {
        let mut result = Vec::with_capacity(self.total_size);
        let num_blocks = self.blocks.len();

        for block_idx in 0..num_blocks {
            let block_data = self.blocks.get(block_idx)?.ok_or_else(|| {
                DruidSegmentError::InvalidData(format!(
                    "CompressedColumnarDoubles: null block at index {}",
                    block_idx
                ))
            })?;

            let remaining = self.total_size - result.len();
            let values_in_block = remaining.min(self.size_per);
            let decompressed_size = values_in_block * 8; // 8 bytes per f64

            let decompressed = decompress_block(self.compression, block_data, decompressed_size)?;

            let mut cursor = Cursor::new(&decompressed);
            for _ in 0..values_in_block {
                let value = cursor.read_f64::<BigEndian>()?;
                result.push(value);
            }
        }

        Ok(result)
    }
}

/// Reader for Druid's CompressedColumnarFloats format.
///
/// Same structure as doubles but with f32 values.
pub struct CompressedColumnarFloats<'a> {
    total_size: usize,
    size_per: usize,
    compression: CompressionStrategy,
    blocks: GenericIndexedV1<'a>,
}

impl<'a> CompressedColumnarFloats<'a> {
    /// Parse from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> Result<Self> {
        if data.len() < 11 {
            return Err(DruidSegmentError::InvalidData(
                "CompressedColumnarFloats: data too short".into(),
            ));
        }

        let version = data[0];
        if version != 0x02 {
            return Err(DruidSegmentError::InvalidData(format!(
                "CompressedColumnarFloats: unsupported version {:#x}",
                version
            )));
        }

        let mut cursor = Cursor::new(&data[1..]);
        let total_size = cursor.read_i32::<BigEndian>()? as usize;
        let size_per = cursor.read_i32::<BigEndian>()? as usize;

        let compression = CompressionStrategy::from_id(data[9])?;
        let blocks = GenericIndexedV1::from_bytes(&data[10..])?;

        Ok(Self {
            total_size,
            size_per,
            compression,
            blocks,
        })
    }

    /// Total number of float values.
    pub fn len(&self) -> usize {
        self.total_size
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.total_size == 0
    }

    /// Decompress all values into a Vec<f32>.
    pub fn decompress_all(&self) -> Result<Vec<f32>> {
        let mut result = Vec::with_capacity(self.total_size);
        let num_blocks = self.blocks.len();

        for block_idx in 0..num_blocks {
            let block_data = self.blocks.get(block_idx)?.ok_or_else(|| {
                DruidSegmentError::InvalidData(format!(
                    "CompressedColumnarFloats: null block at index {}",
                    block_idx
                ))
            })?;

            let remaining = self.total_size - result.len();
            let values_in_block = remaining.min(self.size_per);
            let decompressed_size = values_in_block * 4; // 4 bytes per f32

            let decompressed = decompress_block(self.compression, block_data, decompressed_size)?;

            let mut cursor = Cursor::new(&decompressed);
            for _ in 0..values_in_block {
                let value = cursor.read_f32::<BigEndian>()?;
                result.push(value);
            }
        }

        Ok(result)
    }
}
