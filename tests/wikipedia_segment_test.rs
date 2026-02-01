//! Integration tests using the real Wikipedia segment fixture.

use std::path::Path;

use druid_datafusion_bridge::column::generic_indexed::GenericIndexedV1;
use druid_datafusion_bridge::segment::column_descriptor::ColumnDescriptor;
use druid_datafusion_bridge::segment::smoosh::SmooshReader;

const FIXTURE_PATH: &str = "tests/fixtures/wikipedia-segment";

#[test]
fn test_smoosh_reader_opens() {
    let path = Path::new(FIXTURE_PATH);
    let reader = SmooshReader::open(path).expect("Failed to open smoosh");

    // Should have 22 logical files based on meta.smoosh
    assert!(reader.len() > 0, "Expected at least some files");

    // Check for expected files
    assert!(reader.has_file("__time"), "Missing __time column");
    assert!(reader.has_file("channel"), "Missing channel column");
    assert!(reader.has_file("index.drd"), "Missing index.drd");
    assert!(reader.has_file("metadata.drd"), "Missing metadata.drd");
}

#[test]
fn test_read_column_descriptor() {
    let path = Path::new(FIXTURE_PATH);
    let reader = SmooshReader::open(path).expect("Failed to open smoosh");

    // Read the __time column
    let time_data = reader.map_file("__time").expect("Failed to read __time");

    // First 4 bytes are the length of the JSON descriptor
    let json_len =
        u32::from_be_bytes([time_data[0], time_data[1], time_data[2], time_data[3]]) as usize;
    let json_bytes = &time_data[4..4 + json_len];
    let json_str = std::str::from_utf8(json_bytes).expect("Invalid UTF-8 in descriptor");

    println!("__time descriptor: {}", json_str);

    let descriptor: ColumnDescriptor =
        serde_json::from_str(json_str).expect("Failed to parse column descriptor");

    assert_eq!(
        descriptor.value_type,
        druid_datafusion_bridge::segment::column_descriptor::ValueType::Long
    );
    assert!(!descriptor.has_multiple_values);
    assert_eq!(descriptor.parts.len(), 1);
    assert_eq!(descriptor.parts[0].serde_type, "longV2");
}

#[test]
fn test_read_string_column_descriptor() {
    let path = Path::new(FIXTURE_PATH);
    let reader = SmooshReader::open(path).expect("Failed to open smoosh");

    // Read the channel column
    let channel_data = reader.map_file("channel").expect("Failed to read channel");

    // First 4 bytes are the length of the JSON descriptor
    let json_len = u32::from_be_bytes([
        channel_data[0],
        channel_data[1],
        channel_data[2],
        channel_data[3],
    ]) as usize;
    let json_bytes = &channel_data[4..4 + json_len];
    let json_str = std::str::from_utf8(json_bytes).expect("Invalid UTF-8 in descriptor");

    println!("channel descriptor: {}", json_str);

    let descriptor: ColumnDescriptor =
        serde_json::from_str(json_str).expect("Failed to parse column descriptor");

    assert_eq!(
        descriptor.value_type,
        druid_datafusion_bridge::segment::column_descriptor::ValueType::String
    );
    assert!(!descriptor.has_multiple_values);
    assert_eq!(descriptor.parts.len(), 1);
    assert_eq!(descriptor.parts[0].serde_type, "stringDictionary");
}

#[test]
fn test_read_metadata_drd() {
    let path = Path::new(FIXTURE_PATH);
    let reader = SmooshReader::open(path).expect("Failed to open smoosh");

    // metadata.drd is pure JSON
    let metadata_data = reader
        .map_file("metadata.drd")
        .expect("Failed to read metadata.drd");
    let metadata_str = std::str::from_utf8(metadata_data).expect("Invalid UTF-8 in metadata.drd");

    println!("metadata.drd: {}", metadata_str);

    let metadata: serde_json::Value =
        serde_json::from_str(metadata_str).expect("Failed to parse metadata.drd JSON");

    // Check structure
    assert!(metadata.get("container").is_some());
    assert!(metadata.get("aggregators").is_some());
    assert!(metadata.get("timestampSpec").is_some());
    assert!(metadata.get("queryGranularity").is_some());
    assert!(metadata.get("ordering").is_some());

    // Should have rollup = false
    assert_eq!(metadata.get("rollup"), Some(&serde_json::json!(false)));
}

#[test]
fn test_read_index_drd_structure() {
    let path = Path::new(FIXTURE_PATH);
    let reader = SmooshReader::open(path).expect("Failed to open smoosh");

    let index_data = reader
        .map_file("index.drd")
        .expect("Failed to read index.drd");

    println!("index.drd size: {} bytes", index_data.len());
    println!(
        "First 20 bytes: {:02x?}",
        &index_data[..20.min(index_data.len())]
    );

    // The index.drd contains GenericIndexed structures for column names
    // Let's try parsing the first one

    // Based on hex analysis:
    // - byte 0: version (0x01)
    // - byte 1: flags (0x00)
    // - bytes 2-5: total_bytes (big-endian)
    // - bytes 6-9: num_elements (big-endian)

    let version = index_data[0];
    let flags = index_data[1];
    let total_bytes =
        u32::from_be_bytes([index_data[2], index_data[3], index_data[4], index_data[5]]) as usize;
    let num_elements =
        u32::from_be_bytes([index_data[6], index_data[7], index_data[8], index_data[9]]) as usize;

    println!(
        "GenericIndexed: version={}, flags={:#x}, total_bytes={}, num_elements={}",
        version, flags, total_bytes, num_elements
    );

    assert_eq!(version, 0x01, "Expected version 1");
    assert_eq!(num_elements, 19, "Expected 19 column names");

    // The total_bytes should match the structure size
    // total_bytes = offsets_size + values_size
    // Header is not included in total_bytes
    assert_eq!(total_bytes, 312, "Expected total_bytes to be 312");

    // Parse the offset table
    let offsets_start = 10; // After header
    let mut offsets = Vec::with_capacity(num_elements);
    for i in 0..num_elements {
        let offset = u32::from_be_bytes([
            index_data[offsets_start + i * 4],
            index_data[offsets_start + i * 4 + 1],
            index_data[offsets_start + i * 4 + 2],
            index_data[offsets_start + i * 4 + 3],
        ]) as usize;
        offsets.push(offset);
    }

    println!("Offsets: {:?}", offsets);

    // Values start after header + offsets
    let values_start = 10 + num_elements * 4; // = 10 + 76 = 86
    println!("Values start at offset: {}", values_start);

    // Extract string values - offsets are cumulative end positions
    let mut strings = Vec::new();
    let mut prev_offset = 0;
    for &end_offset in &offsets {
        let start = values_start + prev_offset;
        let end = values_start + end_offset;
        let element_data = &index_data[start..end];

        println!("Element bytes [{}, {}): {:02x?}", start, end, element_data);

        // The format appears to be:
        // [4-byte marker/padding?][null-terminated string padded to 4 bytes]
        //
        // Actually, let me try: skip the first 4 bytes and read until null or end
        if element_data.len() > 4 {
            let str_part = &element_data[4..];
            // Find null terminator or use all bytes
            let str_end = str_part
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(str_part.len());
            if let Ok(s) = std::str::from_utf8(&str_part[..str_end]) {
                strings.push(s.to_string());
                println!("  Extracted string: {:?}", s);
            }
        }

        prev_offset = end_offset;
    }

    println!("\nExtracted column names: {:?}", strings);

    // Verify we got all expected column names
    let expected = vec![
        "channel",
        "cityName",
        "comment",
        "countryIsoCode",
        "countryName",
        "isAnonymous",
        "isMinor",
        "isNew",
        "isRobot",
        "isUnpatrolled",
        "metroCode",
        "namespace",
        "page",
        "regionIsoCode",
        "regionName",
        "user",
        "added",
        "deleted",
        "delta",
    ];
    assert_eq!(strings, expected, "Column names don't match expected");
}

#[test]
fn test_generic_indexed_v1_parse_column_names() {
    let path = Path::new(FIXTURE_PATH);
    let reader = SmooshReader::open(path).expect("Failed to open smoosh");

    let index_data = reader
        .map_file("index.drd")
        .expect("Failed to read index.drd");

    // Parse the first GenericIndexed structure (available dimensions)
    let gi = GenericIndexedV1::from_bytes(index_data).expect("Failed to parse GenericIndexed");

    assert_eq!(gi.len(), 19, "Expected 19 column names");

    // Extract column names using the ObjectStrategy format
    let mut column_names = Vec::new();
    for i in 0..gi.len() {
        let name = gi.get_object_string(i).expect("Failed to get column name");
        column_names.push(name.expect("Column name should not be null"));
    }

    println!("Column names via GenericIndexedV1: {:?}", column_names);

    let expected = vec![
        "channel",
        "cityName",
        "comment",
        "countryIsoCode",
        "countryName",
        "isAnonymous",
        "isMinor",
        "isNew",
        "isRobot",
        "isUnpatrolled",
        "metroCode",
        "namespace",
        "page",
        "regionIsoCode",
        "regionName",
        "user",
        "added",
        "deleted",
        "delta",
    ];
    assert_eq!(column_names, expected);

    // Test total_size to verify we can chain multiple GenericIndexed
    let first_size = gi.total_size().expect("Failed to get total size");
    println!("First GenericIndexed total size: {} bytes", first_size);

    // Should match: 10 (header) + 19*4 (offsets) + 232 (values) = 318 bytes
    // Actually based on offsets, the last one is 232, so total = 10 + 76 + 232 = 318
    assert_eq!(first_size, 318, "Unexpected total size");
}

#[test]
fn test_list_all_files() {
    let path = Path::new(FIXTURE_PATH);
    let reader = SmooshReader::open(path).expect("Failed to open smoosh");

    println!("Logical files in segment:");
    for name in reader.file_names() {
        let entry = reader.entry(name).unwrap();
        println!("  {} ({} bytes)", name, entry.size());
    }

    // Verify expected columns exist
    let expected_columns = vec![
        "__time",
        "added",
        "channel",
        "cityName",
        "comment",
        "countryIsoCode",
        "countryName",
        "deleted",
        "delta",
        "isAnonymous",
        "isMinor",
        "isNew",
        "isRobot",
        "isUnpatrolled",
        "metroCode",
        "namespace",
        "page",
        "regionIsoCode",
        "regionName",
        "user",
    ];

    for col in &expected_columns {
        assert!(reader.has_file(col), "Missing column: {}", col);
    }
}
