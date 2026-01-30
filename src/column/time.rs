use arrow::array::TimestampMillisecondArray;

use super::compressed_longs::CompressedColumnarLongs;
use crate::error::Result;

/// Read the `__time` column from its binary data (after the JSON header).
///
/// The __time column stores epoch milliseconds as compressed longs.
/// We produce an Arrow TimestampMillisecondArray.
pub fn read_time_column(data: &[u8]) -> Result<TimestampMillisecondArray> {
    let longs = CompressedColumnarLongs::from_bytes(data)?;
    let values = longs.decompress_all()?;
    Ok(TimestampMillisecondArray::from(values))
}
