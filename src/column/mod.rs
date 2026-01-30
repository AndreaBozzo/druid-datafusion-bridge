pub mod bitmap;
pub mod compressed_doubles;
pub mod compressed_ints;
pub mod compressed_longs;
pub mod double;
pub mod float;
pub mod generic_indexed;
pub mod long;
pub mod string;
pub mod time;
pub mod vsize_ints;

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::ArrayRef;
use byteorder::{BigEndian, ReadBytesExt};

use crate::error::{DruidSegmentError, Result};
use crate::segment::column_descriptor::{ColumnDescriptor, ValueType};

/// Parse the column header: a length-prefixed JSON ColumnDescriptor string
/// followed by binary column data.
///
/// Layout: `[json_len: i32 (big-endian)][json_bytes: json_len][binary_data...]`
pub fn parse_column_header(data: &[u8]) -> Result<(ColumnDescriptor, &[u8])> {
    if data.len() < 4 {
        return Err(DruidSegmentError::InvalidData(
            "Column data too short for header length".into(),
        ));
    }
    let mut cursor = Cursor::new(data);
    let json_len = cursor.read_i32::<BigEndian>()? as usize;

    if data.len() < 4 + json_len {
        return Err(DruidSegmentError::InvalidData(format!(
            "Column data too short: need {} bytes for JSON header, have {}",
            json_len,
            data.len() - 4
        )));
    }

    let json_bytes = &data[4..4 + json_len];
    let descriptor: ColumnDescriptor = serde_json::from_slice(json_bytes)?;
    let remaining = &data[4 + json_len..];
    Ok((descriptor, remaining))
}

/// Read a column's data and return the descriptor and an Arrow array.
pub fn read_column(name: &str, data: &[u8]) -> Result<(ColumnDescriptor, ArrayRef)> {
    let (descriptor, binary_data) = parse_column_header(data)?;

    let array: ArrayRef = match (&descriptor.value_type, name) {
        (_, "__time") => Arc::new(self::time::read_time_column(binary_data)?),
        (ValueType::String, _) => Arc::new(self::string::read_string_column(binary_data)?),
        (ValueType::Long, _) => Arc::new(self::long::read_long_column(binary_data)?),
        (ValueType::Float, _) => Arc::new(self::float::read_float_column(binary_data)?),
        (ValueType::Double, _) => Arc::new(self::double::read_double_column(binary_data)?),
        (ValueType::Complex, _) => {
            return Err(DruidSegmentError::UnsupportedColumnType("Complex".into()));
        }
    };

    Ok((descriptor, array))
}
