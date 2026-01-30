use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

use crate::column::generic_indexed::GenericIndexedV1;
use crate::error::{DruidSegmentError, Result};

/// Segment metadata parsed from the `index.drd` logical file.
///
/// index.drd layout (v9):
/// ```text
/// [columns: GenericIndexed<String>]    -- list of column names
/// [dimensions: GenericIndexed<String>] -- list of dimension names
/// [interval_start: i64]                -- interval start in epoch millis
/// [interval_end: i64]                  -- interval end in epoch millis
/// [bitmap_serde_factory: optional]
/// ```
#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    pub columns: Vec<String>,
    pub dimensions: Vec<String>,
    pub interval_start_ms: i64,
    pub interval_end_ms: i64,
}

impl SegmentMetadata {
    /// Parse segment metadata from the raw bytes of `index.drd`.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut offset = 0;

        // Read column names (GenericIndexed<String>)
        let columns_gi = GenericIndexedV1::from_bytes(&data[offset..])?;
        let mut columns = Vec::with_capacity(columns_gi.len());
        for i in 0..columns_gi.len() {
            let name = columns_gi.get_str(i)?.ok_or_else(|| {
                DruidSegmentError::InvalidData(format!(
                    "index.drd: null column name at index {}",
                    i
                ))
            })?;
            columns.push(name.to_string());
        }
        offset += columns_gi.total_size()?;

        // Read dimension names (GenericIndexed<String>)
        let dimensions_gi = GenericIndexedV1::from_bytes(&data[offset..])?;
        let mut dimensions = Vec::with_capacity(dimensions_gi.len());
        for i in 0..dimensions_gi.len() {
            let name = dimensions_gi.get_str(i)?.ok_or_else(|| {
                DruidSegmentError::InvalidData(format!(
                    "index.drd: null dimension name at index {}",
                    i
                ))
            })?;
            dimensions.push(name.to_string());
        }
        offset += dimensions_gi.total_size()?;

        // Read interval (two i64 values, big-endian)
        if data.len() < offset + 16 {
            return Err(DruidSegmentError::InvalidData(
                "index.drd: data too short for interval timestamps".into(),
            ));
        }
        let mut cursor = Cursor::new(&data[offset..]);
        let interval_start_ms = cursor.read_i64::<BigEndian>()?;
        let interval_end_ms = cursor.read_i64::<BigEndian>()?;

        Ok(Self {
            columns,
            dimensions,
            interval_start_ms,
            interval_end_ms,
        })
    }
}
