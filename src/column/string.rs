use std::io::Cursor;

use arrow::array::StringArray;
use byteorder::{BigEndian, ReadBytesExt};

use super::compressed_ints::CompressedColumnarInts;
use super::generic_indexed::GenericIndexedV1;
use super::vsize_ints::VSizeColumnarInts;
use crate::error::{DruidSegmentError, Result};

/// Read a dictionary-encoded string column from its binary data
/// (after the JSON header).
///
/// Binary layout:
/// ```text
/// [version: u8]         -- column serialization version
/// [flags/feature_mask depending on version]
/// [dictionary: GenericIndexed<String>]
/// [encoded_values: CompressedColumnarInts or VSizeColumnarInts]
/// [bitmap serializer: optional, skipped]
/// ```
///
/// The version byte determines the exact layout:
/// - 0x00: Legacy uncompressed (VSizeColumnarInts for values)
/// - 0x02: Compressed with CompressedColumnarInts for values
/// - 0x03: Compressed with additional feature flags
pub fn read_string_column(data: &[u8]) -> Result<StringArray> {
    if data.is_empty() {
        return Err(DruidSegmentError::InvalidData(
            "String column: empty data".into(),
        ));
    }

    let version = data[0];

    match version {
        0x00 => read_string_v0(data),
        0x02 => read_string_v2(data),
        0x03 => read_string_v3(data),
        other => Err(DruidSegmentError::InvalidData(format!(
            "String column: unsupported version {:#x}",
            other
        ))),
    }
}

/// V0: Legacy uncompressed format.
/// Layout: [version=0x00][dictionary: GenericIndexed][values: VSizeColumnarInts]
fn read_string_v0(data: &[u8]) -> Result<StringArray> {
    let offset = 1; // skip version byte

    // Read dictionary
    let dictionary = GenericIndexedV1::from_bytes(&data[offset..])?;
    let dict_size = dictionary.total_size()?;
    let values_offset = offset + dict_size;

    // Read encoded values
    let encoded = VSizeColumnarInts::from_bytes(&data[values_offset..])?;

    // Resolve dictionary IDs to strings
    resolve_dictionary(&dictionary, &encoded.to_vec()?)
}

/// V2: Compressed format.
/// Layout: [version=0x02][flags: i32][dictionary: GenericIndexed][values: CompressedColumnarInts]
fn read_string_v2(data: &[u8]) -> Result<StringArray> {
    if data.len() < 5 {
        return Err(DruidSegmentError::InvalidData(
            "String column v2: data too short for flags".into(),
        ));
    }

    let mut cursor = Cursor::new(&data[1..]);
    let _flags = cursor.read_i32::<BigEndian>()?;
    let offset = 5; // version(1) + flags(4)

    // Read dictionary
    let dictionary = GenericIndexedV1::from_bytes(&data[offset..])?;
    let dict_size = dictionary.total_size()?;
    let values_offset = offset + dict_size;

    // Read compressed encoded values
    let encoded = CompressedColumnarInts::from_bytes(&data[values_offset..])?;
    let ids = encoded.decompress_all()?;

    resolve_dictionary(&dictionary, &ids)
}

/// V3: Compressed format with feature flags.
/// Layout: [version=0x03][feature_mask: i32][dictionary: GenericIndexed][values: CompressedColumnarInts]
fn read_string_v3(data: &[u8]) -> Result<StringArray> {
    if data.len() < 5 {
        return Err(DruidSegmentError::InvalidData(
            "String column v3: data too short for feature mask".into(),
        ));
    }

    let mut cursor = Cursor::new(&data[1..]);
    let _feature_mask = cursor.read_i32::<BigEndian>()?;
    let offset = 5; // version(1) + feature_mask(4)

    // Read dictionary
    let dictionary = GenericIndexedV1::from_bytes(&data[offset..])?;
    let dict_size = dictionary.total_size()?;
    let values_offset = offset + dict_size;

    // Read compressed encoded values
    let encoded = CompressedColumnarInts::from_bytes(&data[values_offset..])?;
    let ids = encoded.decompress_all()?;

    resolve_dictionary(&dictionary, &ids)
}

/// Given a dictionary and a list of integer IDs, resolve each ID to its
/// string value and build an Arrow StringArray.
fn resolve_dictionary(dictionary: &GenericIndexedV1<'_>, ids: &[u32]) -> Result<StringArray> {
    let mut builder = Vec::with_capacity(ids.len());

    for &id in ids {
        let value = dictionary.get_str(id as usize)?;
        builder.push(value);
    }

    Ok(StringArray::from(builder))
}
