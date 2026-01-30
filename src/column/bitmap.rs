use roaring::RoaringBitmap;

use crate::error::{DruidSegmentError, Result};

/// Bitmap type markers used by Druid.
const BITMAP_TYPE_ROARING: u8 = 0x01;
const BITMAP_TYPE_CONCISE: u8 = 0x00;

/// Read a bitmap from Druid's serialized format.
///
/// Druid serializes bitmaps with a type byte prefix:
/// - 0x00 = Concise bitmap (legacy, not yet supported)
/// - 0x01 = Roaring bitmap
///
/// The Roaring bitmap format follows the standard Roaring serialization.
pub fn read_bitmap(data: &[u8]) -> Result<RoaringBitmap> {
    if data.is_empty() {
        return Ok(RoaringBitmap::new());
    }

    let bitmap_type = data[0];
    match bitmap_type {
        BITMAP_TYPE_ROARING => {
            // Standard Roaring bitmap deserialization
            RoaringBitmap::deserialize_from(&data[1..]).map_err(|e| {
                DruidSegmentError::InvalidData(format!(
                    "Failed to deserialize Roaring bitmap: {}",
                    e
                ))
            })
        }
        BITMAP_TYPE_CONCISE => Err(DruidSegmentError::UnsupportedColumnType(
            "Concise bitmap format not yet supported".into(),
        )),
        other => Err(DruidSegmentError::InvalidData(format!(
            "Unknown bitmap type: {:#x}",
            other
        ))),
    }
}

/// Read a null bitmap and return the set of null row indices.
/// If the data is empty, returns an empty bitmap (no nulls).
pub fn read_null_bitmap(data: &[u8]) -> Result<RoaringBitmap> {
    if data.is_empty() {
        return Ok(RoaringBitmap::new());
    }
    read_bitmap(data)
}
