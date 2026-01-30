use arrow::array::Int64Array;

use super::compressed_longs::CompressedColumnarLongs;
use crate::error::Result;

/// Read a long (Int64) column from its binary data (after the JSON header).
///
/// Long columns are stored as CompressedColumnarLongs, optionally with
/// a null bitmap. For now we skip null bitmap handling and treat all
/// values as non-null.
pub fn read_long_column(data: &[u8]) -> Result<Int64Array> {
    let longs = CompressedColumnarLongs::from_bytes(data)?;
    let values = longs.decompress_all()?;
    Ok(Int64Array::from(values))
}
