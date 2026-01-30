use arrow::array::Float32Array;

use super::compressed_doubles::CompressedColumnarFloats;
use crate::error::Result;

/// Read a float (Float32) column from its binary data (after the JSON header).
///
/// Float columns are stored as CompressedColumnarFloats, optionally with
/// a null bitmap.
pub fn read_float_column(data: &[u8]) -> Result<Float32Array> {
    let floats = CompressedColumnarFloats::from_bytes(data)?;
    let values = floats.decompress_all()?;
    Ok(Float32Array::from(values))
}
