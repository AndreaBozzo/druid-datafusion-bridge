use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};

use crate::error::{DruidSegmentError, Result};

/// Reader for Druid's GenericIndexed<T> binary format (V1).
///
/// This is a fundamental data structure used throughout Druid segments:
/// dictionaries, compressed block containers, metadata column lists, etc.
///
/// V1 layout:
/// ```text
/// [version: u8 = 0x01]
/// [flags: u8]           -- 0x01 = sorted/reverse-lookup, 0x00 = unsorted
/// [total_bytes: i32]    -- total size of offsets + values (excluding version/flags/total_bytes/num_elements)
/// [num_elements: i32]
/// [offsets: i32 * N]    -- cumulative end-offset of each element's data (relative to values_start)
/// [values: ...]         -- concatenated elements
/// ```
///
/// Element format varies by strategy:
/// - For length-prefixed format: `[length: i32][bytes]` where length=-1 means null
/// - For null-prefixed strings (ObjectStrategy): `[4 zero bytes][string bytes]`
#[derive(Debug)]
pub struct GenericIndexedV1<'a> {
    data: &'a [u8],
    num_elements: usize,
    header_size: usize,
    values_start: usize,
}

const VERSION_V1: u8 = 0x01;

impl<'a> GenericIndexedV1<'a> {
    /// Parse a GenericIndexed V1 from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(DruidSegmentError::InvalidData(
                "GenericIndexed: empty data".into(),
            ));
        }

        let version = data[0];
        if version != VERSION_V1 {
            return Err(DruidSegmentError::InvalidGenericIndexedVersion(version));
        }

        if data.len() < 10 {
            return Err(DruidSegmentError::InvalidData(
                "GenericIndexed V1: data too short for header".into(),
            ));
        }

        // flags at offset 1 (we don't need it for reading)
        let _flags = data[1];

        let mut cursor = Cursor::new(&data[2..]);
        let _total_bytes = cursor.read_i32::<BigEndian>()? as usize;
        let num_elements = cursor.read_i32::<BigEndian>()? as usize;

        // Header: version(1) + flags(1) + total_bytes(4) + num_elements(4) = 10 bytes
        let header_size = 10;
        // Offsets: num_elements * 4 bytes
        let offsets_size = num_elements * 4;
        let values_start = header_size + offsets_size;

        Ok(Self {
            data,
            num_elements,
            header_size,
            values_start,
        })
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        self.num_elements
    }

    /// Whether the container is empty.
    pub fn is_empty(&self) -> bool {
        self.num_elements == 0
    }

    /// Get the cumulative end-offset for element `i` (relative to values_start).
    fn offset_at(&self, i: usize) -> Result<usize> {
        let pos = self.header_size + i * 4;
        if pos + 4 > self.data.len() {
            return Err(DruidSegmentError::InvalidData(format!(
                "GenericIndexed: offset {} out of bounds (data len {})",
                pos,
                self.data.len()
            )));
        }
        let mut cursor = Cursor::new(&self.data[pos..]);
        let offset = cursor.read_i32::<BigEndian>()? as usize;
        Ok(offset)
    }

    /// Get the byte range for element `i` within the values section.
    fn element_range(&self, i: usize) -> Result<(usize, usize)> {
        if i >= self.num_elements {
            return Err(DruidSegmentError::InvalidData(format!(
                "GenericIndexed: index {} out of range (len {})",
                i, self.num_elements
            )));
        }
        let start = if i == 0 { 0 } else { self.offset_at(i - 1)? };
        let end = self.offset_at(i)?;
        Ok((start, end))
    }

    /// Get the i-th element as `Option<&[u8]>`.
    ///
    /// This handles the length-prefixed format where:
    /// - length == -1 means null
    /// - length >= 0 means that many bytes of data follow
    pub fn get(&self, index: usize) -> Result<Option<&'a [u8]>> {
        let (start, end) = self.element_range(index)?;
        let abs_start = self.values_start + start;
        let abs_end = self.values_start + end;

        if abs_end > self.data.len() {
            return Err(DruidSegmentError::InvalidData(format!(
                "GenericIndexed: element {} data range [{}, {}) exceeds buffer size {}",
                index,
                abs_start,
                abs_end,
                self.data.len()
            )));
        }

        let element_data = &self.data[abs_start..abs_end];
        if element_data.len() < 4 {
            return Err(DruidSegmentError::InvalidData(format!(
                "GenericIndexed: element {} too short for length prefix ({} bytes)",
                index,
                element_data.len()
            )));
        }

        let mut cursor = Cursor::new(element_data);
        let length = cursor.read_i32::<BigEndian>()?;

        if length < 0 {
            // Null value
            Ok(None)
        } else {
            let length = length as usize;
            let value_start = abs_start + 4;
            let value_end = value_start + length;
            if value_end > self.data.len() {
                return Err(DruidSegmentError::InvalidData(format!(
                    "GenericIndexed: element {} value overflows buffer",
                    index
                )));
            }
            Ok(Some(&self.data[value_start..value_end]))
        }
    }
    
    /// Get the i-th element as raw bytes, using the offset table to determine boundaries.
    ///
    /// This is useful for formats where elements don't have a length prefix,
    /// such as ObjectStrategy-serialized strings that have a 4-byte null prefix
    /// followed by raw string bytes.
    pub fn get_raw(&self, index: usize) -> Result<&'a [u8]> {
        let (start, end) = self.element_range(index)?;
        let abs_start = self.values_start + start;
        let abs_end = self.values_start + end;

        if abs_end > self.data.len() {
            return Err(DruidSegmentError::InvalidData(format!(
                "GenericIndexed: element {} data range [{}, {}) exceeds buffer size {}",
                index,
                abs_start,
                abs_end,
                self.data.len()
            )));
        }

        Ok(&self.data[abs_start..abs_end])
    }
    
    /// Get the i-th element as a string, assuming ObjectStrategy format.
    ///
    /// ObjectStrategy format for strings: `[4 zero bytes][string bytes]`
    /// The string length is determined by the offset table (element_size - 4).
    pub fn get_object_string(&self, index: usize) -> Result<Option<&'a str>> {
        let raw = self.get_raw(index)?;
        
        if raw.len() < 4 {
            return Err(DruidSegmentError::InvalidData(format!(
                "GenericIndexed: element {} too short for ObjectStrategy prefix ({} bytes)",
                index,
                raw.len()
            )));
        }
        
        // First 4 bytes should be zeros (null marker)
        // If they're not all zero, it might be a different format
        let prefix = &raw[0..4];
        if prefix != [0, 0, 0, 0] {
            return Err(DruidSegmentError::InvalidData(format!(
                "GenericIndexed: element {} has unexpected ObjectStrategy prefix: {:?}",
                index, prefix
            )));
        }
        
        let str_bytes = &raw[4..];
        if str_bytes.is_empty() {
            return Ok(None); // Empty string treated as None
        }
        
        let s = std::str::from_utf8(str_bytes).map_err(|e| {
            DruidSegmentError::InvalidData(format!(
                "GenericIndexed: element {} is not valid UTF-8: {}",
                index, e
            ))
        })?;
        
        Ok(Some(s))
    }

    /// Get the i-th element as a UTF-8 string.
    pub fn get_str(&self, index: usize) -> Result<Option<&'a str>> {
        match self.get(index)? {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|e| {
                    DruidSegmentError::InvalidData(format!(
                        "GenericIndexed: element {} is not valid UTF-8: {}",
                        index, e
                    ))
                })?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// Total number of bytes consumed by this GenericIndexed structure.
    /// Useful for advancing past it when reading compound formats.
    pub fn total_size(&self) -> Result<usize> {
        if self.num_elements == 0 {
            return Ok(self.values_start);
        }
        let last_offset = self.offset_at(self.num_elements - 1)?;
        Ok(self.values_start + last_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::WriteBytesExt;

    /// Build a GenericIndexed V1 containing the given byte slices.
    fn build_generic_indexed(elements: &[Option<&[u8]>]) -> Vec<u8> {
        let mut buf = Vec::new();
        // version
        buf.push(VERSION_V1);
        // flags (sorted)
        buf.push(0x01);

        // Build values section first to compute offsets
        let mut values = Vec::new();
        let mut offsets = Vec::new();
        for elem in elements {
            match elem {
                Some(data) => {
                    values.write_i32::<BigEndian>(data.len() as i32).unwrap();
                    values.extend_from_slice(data);
                }
                None => {
                    values.write_i32::<BigEndian>(-1).unwrap();
                }
            }
            offsets.push(values.len() as i32);
        }

        // total_bytes = offsets_size + values_size
        let offsets_size = elements.len() * 4;
        let total_bytes = (offsets_size + values.len()) as i32;
        buf.write_i32::<BigEndian>(total_bytes).unwrap();

        // num_elements
        buf.write_i32::<BigEndian>(elements.len() as i32).unwrap();

        // offsets
        for &off in &offsets {
            buf.write_i32::<BigEndian>(off).unwrap();
        }

        // values
        buf.extend_from_slice(&values);

        buf
    }

    #[test]
    fn test_read_strings() {
        let data = build_generic_indexed(&[Some(b"alpha"), Some(b"beta"), Some(b"gamma")]);
        let gi = GenericIndexedV1::from_bytes(&data).unwrap();
        assert_eq!(gi.len(), 3);
        assert_eq!(gi.get_str(0).unwrap(), Some("alpha"));
        assert_eq!(gi.get_str(1).unwrap(), Some("beta"));
        assert_eq!(gi.get_str(2).unwrap(), Some("gamma"));
    }

    #[test]
    fn test_null_element() {
        let data = build_generic_indexed(&[Some(b"hello"), None, Some(b"world")]);
        let gi = GenericIndexedV1::from_bytes(&data).unwrap();
        assert_eq!(gi.len(), 3);
        assert_eq!(gi.get_str(0).unwrap(), Some("hello"));
        assert_eq!(gi.get(1).unwrap(), None);
        assert_eq!(gi.get_str(2).unwrap(), Some("world"));
    }

    #[test]
    fn test_empty_element() {
        let data = build_generic_indexed(&[Some(b""), Some(b"x")]);
        let gi = GenericIndexedV1::from_bytes(&data).unwrap();
        assert_eq!(gi.get(0).unwrap(), Some(&b""[..]));
        assert_eq!(gi.get(1).unwrap(), Some(&b"x"[..]));
    }

    #[test]
    fn test_empty_container() {
        let data = build_generic_indexed(&[]);
        let gi = GenericIndexedV1::from_bytes(&data).unwrap();
        assert_eq!(gi.len(), 0);
        assert!(gi.is_empty());
    }

    #[test]
    fn test_invalid_version() {
        let data = [0x02, 0x00, 0, 0, 0, 0, 0, 0, 0, 0];
        let err = GenericIndexedV1::from_bytes(&data).unwrap_err();
        assert!(matches!(
            err,
            DruidSegmentError::InvalidGenericIndexedVersion(0x02)
        ));
    }
}
