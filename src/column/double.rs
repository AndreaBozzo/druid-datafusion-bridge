use arrow::array::Float64Array;

use super::compressed_doubles::CompressedColumnarDoubles;
use crate::error::Result;

/// Read a double (Float64) column from its binary data (after the JSON header).
///
/// Double columns are stored as CompressedColumnarDoubles, optionally with
/// a null bitmap.
pub fn read_double_column(data: &[u8]) -> Result<Float64Array> {
    let doubles = CompressedColumnarDoubles::from_bytes(data)?;
    let values = doubles.decompress_all()?;
    Ok(Float64Array::from(values))
}
