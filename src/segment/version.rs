use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

use crate::error::{DruidSegmentError, Result};

/// Expected segment format version.
pub const SEGMENT_VERSION_V9: i32 = 9;

/// Read and validate version.bin data.
/// Expects exactly 4 bytes encoding a big-endian i32 equal to 9.
pub fn read_version(data: &[u8]) -> Result<i32> {
    if data.len() < 4 {
        return Err(DruidSegmentError::InvalidData(format!(
            "version.bin too short: {} bytes, expected 4",
            data.len()
        )));
    }
    let mut cursor = Cursor::new(data);
    let version = cursor.read_i32::<BigEndian>()?;
    if version != SEGMENT_VERSION_V9 {
        return Err(DruidSegmentError::InvalidVersion(version));
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_v9() {
        let data = [0x00, 0x00, 0x00, 0x09];
        assert_eq!(read_version(&data).unwrap(), 9);
    }

    #[test]
    fn test_invalid_version() {
        let data = [0x00, 0x00, 0x00, 0x08];
        let err = read_version(&data).unwrap_err();
        assert!(matches!(err, DruidSegmentError::InvalidVersion(8)));
    }

    #[test]
    fn test_truncated_data() {
        let data = [0x00, 0x00];
        assert!(read_version(&data).is_err());
    }
}
