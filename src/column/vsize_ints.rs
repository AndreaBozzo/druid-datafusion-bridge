use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

use crate::error::{DruidSegmentError, Result};

/// Reader for Druid's VSizeColumnarInts.
///
/// Stores a column of unsigned integers using a fixed number of bytes per value
/// (1, 2, 3, or 4 bytes). Used for uncompressed dictionary-encoded column values.
///
/// Layout:
/// ```text
/// [version: u8 = 0x00]
/// [num_bytes: u8]     -- bytes per value (1-4)
/// [size: i32]         -- total size of the values buffer in bytes
/// [values: ...]       -- packed integers, each `num_bytes` wide, big-endian
/// ```
pub struct VSizeColumnarInts<'a> {
    data: &'a [u8],
    num_bytes: usize,
    num_values: usize,
    values_offset: usize,
}

const VERSION: u8 = 0x00;
const HEADER_SIZE: usize = 6; // version(1) + num_bytes(1) + size(4)

impl<'a> VSizeColumnarInts<'a> {
    /// Parse a VSizeColumnarInts from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(DruidSegmentError::InvalidData(
                "VSizeColumnarInts: data too short for header".into(),
            ));
        }

        let version = data[0];
        if version != VERSION {
            return Err(DruidSegmentError::InvalidData(format!(
                "VSizeColumnarInts: unexpected version {:#x}, expected {:#x}",
                version, VERSION
            )));
        }

        let num_bytes = data[1] as usize;
        if num_bytes == 0 || num_bytes > 4 {
            return Err(DruidSegmentError::InvalidData(format!(
                "VSizeColumnarInts: invalid num_bytes {}, expected 1-4",
                num_bytes
            )));
        }

        let mut cursor = Cursor::new(&data[2..]);
        let buffer_size = cursor.read_i32::<BigEndian>()? as usize;

        let num_values = buffer_size / num_bytes;
        let values_offset = HEADER_SIZE;

        Ok(Self {
            data,
            num_bytes,
            num_values,
            values_offset,
        })
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        self.num_values
    }

    /// Whether the column is empty.
    pub fn is_empty(&self) -> bool {
        self.num_values == 0
    }

    /// Get the value at the given index as a u32.
    pub fn get(&self, index: usize) -> Result<u32> {
        if index >= self.num_values {
            return Err(DruidSegmentError::InvalidData(format!(
                "VSizeColumnarInts: index {} out of range (len {})",
                index, self.num_values
            )));
        }

        let pos = self.values_offset + index * self.num_bytes;
        let bytes = &self.data[pos..pos + self.num_bytes];

        // Read big-endian unsigned integer of variable width
        let mut value: u32 = 0;
        for &b in bytes {
            value = (value << 8) | (b as u32);
        }
        Ok(value)
    }

    /// Read all values into a Vec.
    pub fn to_vec(&self) -> Result<Vec<u32>> {
        let mut values = Vec::with_capacity(self.num_values);
        for i in 0..self.num_values {
            values.push(self.get(i)?);
        }
        Ok(values)
    }

    /// Total bytes consumed by this structure.
    pub fn total_size(&self) -> usize {
        HEADER_SIZE + self.num_values * self.num_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::WriteBytesExt;

    fn build_vsize_ints(num_bytes: u8, values: &[u32]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(VERSION);
        buf.push(num_bytes);
        let buffer_size = values.len() * num_bytes as usize;
        buf.write_i32::<BigEndian>(buffer_size as i32).unwrap();
        for &v in values {
            // Write big-endian with the specified width
            let nb = num_bytes as usize;
            for i in (0..nb).rev() {
                buf.push(((v >> (i * 8)) & 0xFF) as u8);
            }
        }
        buf
    }

    #[test]
    fn test_single_byte() {
        let data = build_vsize_ints(1, &[0, 1, 2, 255]);
        let col = VSizeColumnarInts::from_bytes(&data).unwrap();
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0).unwrap(), 0);
        assert_eq!(col.get(1).unwrap(), 1);
        assert_eq!(col.get(2).unwrap(), 2);
        assert_eq!(col.get(3).unwrap(), 255);
    }

    #[test]
    fn test_two_bytes() {
        let data = build_vsize_ints(2, &[0, 256, 1000, 65535]);
        let col = VSizeColumnarInts::from_bytes(&data).unwrap();
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0).unwrap(), 0);
        assert_eq!(col.get(1).unwrap(), 256);
        assert_eq!(col.get(2).unwrap(), 1000);
        assert_eq!(col.get(3).unwrap(), 65535);
    }

    #[test]
    fn test_to_vec() {
        let values = &[10, 20, 30];
        let data = build_vsize_ints(1, values);
        let col = VSizeColumnarInts::from_bytes(&data).unwrap();
        assert_eq!(col.to_vec().unwrap(), vec![10, 20, 30]);
    }
}
