pub mod column_descriptor;
pub mod metadata;
pub mod smoosh;
pub mod version;

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use self::column_descriptor::{ColumnDescriptor, ValueType};
use self::metadata::SegmentMetadata;
use self::smoosh::SmooshReader;
use self::version::read_version;
use crate::column;
use crate::error::Result;

/// A fully opened Druid v9 segment, ready for reading.
pub struct DruidSegment {
    smoosh: SmooshReader,
    metadata: SegmentMetadata,
    schema: Arc<Schema>,
}

impl std::fmt::Debug for DruidSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DruidSegment")
            .field("metadata", &self.metadata)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DruidSegment {
    /// Open a segment directory, validating version and parsing metadata.
    pub fn open(path: &Path) -> Result<Self> {
        // 1. Validate version.bin
        let version_data = std::fs::read(path.join("version.bin"))?;
        read_version(&version_data)?;

        // 2. Open smoosh archive
        let smoosh = SmooshReader::open(path)?;

        // 3. Parse index.drd metadata
        let index_data = smoosh.map_file("index.drd")?;
        let metadata = SegmentMetadata::from_bytes(index_data)?;

        // 4. Build Arrow schema
        let schema = Self::build_schema(&smoosh, &metadata)?;

        Ok(Self {
            smoosh,
            metadata,
            schema,
        })
    }

    fn build_schema(smoosh: &SmooshReader, metadata: &SegmentMetadata) -> Result<Arc<Schema>> {
        let mut fields = Vec::new();
        for col_name in &metadata.columns {
            let col_data = smoosh.map_file(col_name)?;
            let (descriptor, _) = column::parse_column_header(col_data)?;
            let arrow_type = druid_type_to_arrow(&descriptor, col_name);
            fields.push(Field::new(col_name, arrow_type, true));
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Return the Arrow schema for this segment.
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the segment metadata.
    pub fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    /// Read all columns into a single RecordBatch.
    pub fn read_all(&self) -> Result<RecordBatch> {
        let col_names: Vec<&str> = self.metadata.columns.iter().map(|s| s.as_str()).collect();
        self.read_columns(&col_names)
    }

    /// Read specific columns by name into a RecordBatch.
    pub fn read_columns(&self, columns: &[&str]) -> Result<RecordBatch> {
        let mut arrays = Vec::new();
        let mut fields = Vec::new();

        for &col_name in columns {
            let col_data = self.smoosh.map_file(col_name)?;
            let (descriptor, array) = column::read_column(col_name, col_data)?;
            let arrow_type = druid_type_to_arrow(&descriptor, col_name);
            fields.push(Field::new(col_name, arrow_type, true));
            arrays.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    /// Return the number of rows in the segment.
    pub fn num_rows(&self) -> Result<usize> {
        // Determine row count from the __time column
        let time_data = self.smoosh.map_file("__time")?;
        let (_, array) = column::read_column("__time", time_data)?;
        Ok(array.len())
    }

    /// Get a reference to the smoosh reader for direct file access.
    pub fn smoosh(&self) -> &SmooshReader {
        &self.smoosh
    }
}

/// Map a Druid ValueType to an Arrow DataType.
fn druid_type_to_arrow(descriptor: &ColumnDescriptor, col_name: &str) -> DataType {
    if col_name == "__time" {
        return DataType::Timestamp(TimeUnit::Millisecond, None);
    }
    match descriptor.value_type {
        ValueType::String => DataType::Utf8,
        ValueType::Long => DataType::Int64,
        ValueType::Float => DataType::Float32,
        ValueType::Double => DataType::Float64,
        ValueType::Complex => DataType::Binary,
    }
}
