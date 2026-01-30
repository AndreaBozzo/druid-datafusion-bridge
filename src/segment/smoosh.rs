use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use crate::error::{DruidSegmentError, Result};

/// Metadata for a single logical file within the smoosh archive.
#[derive(Debug, Clone)]
pub struct SmooshEntry {
    pub name: String,
    pub chunk_number: usize,
    pub start_offset: usize,
    pub end_offset: usize,
}

impl SmooshEntry {
    /// Size in bytes of this logical file.
    pub fn size(&self) -> usize {
        self.end_offset - self.start_offset
    }
}

/// Memory-mapped smoosh archive reader.
///
/// Druid's smoosh format packs multiple logical files into a small number
/// of physical chunk files (max 2GB each). `meta.smoosh` is a text index
/// that maps logical file names to chunk number + byte range.
///
/// This mirrors Druid's Java `SmooshedFileMapper`.
pub struct SmooshReader {
    entries: BTreeMap<String, SmooshEntry>,
    mmaps: Vec<Mmap>,
}

impl SmooshReader {
    /// Open a segment directory, parse `meta.smoosh`, and mmap all chunk files.
    pub fn open(segment_dir: &Path) -> Result<Self> {
        let meta_path = segment_dir.join("meta.smoosh");
        let meta_content = std::fs::read_to_string(&meta_path).map_err(|e| {
            DruidSegmentError::InvalidSmooshMeta(format!(
                "Failed to read {}: {}",
                meta_path.display(),
                e
            ))
        })?;

        let mut lines = meta_content.lines();

        // First line: v1,<max_chunk_size>,<num_chunks>
        let header = lines
            .next()
            .ok_or_else(|| DruidSegmentError::InvalidSmooshMeta("meta.smoosh is empty".into()))?;
        let header_parts: Vec<&str> = header.split(',').collect();
        if header_parts.len() < 3 || header_parts[0] != "v1" {
            return Err(DruidSegmentError::InvalidSmooshMeta(format!(
                "Invalid header line: '{}'",
                header
            )));
        }
        let num_chunks: usize = header_parts[2].trim().parse().map_err(|e| {
            DruidSegmentError::InvalidSmooshMeta(format!(
                "Invalid num_chunks '{}': {}",
                header_parts[2], e
            ))
        })?;

        // Parse entry lines: <name>,<chunk>,<start>,<end>
        let mut entries = BTreeMap::new();
        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() < 4 {
                return Err(DruidSegmentError::InvalidSmooshMeta(format!(
                    "Invalid entry line: '{}'",
                    line
                )));
            }
            let name = parts[0].to_string();
            let chunk_number: usize = parts[1].parse().map_err(|e| {
                DruidSegmentError::InvalidSmooshMeta(format!(
                    "Invalid chunk number '{}': {}",
                    parts[1], e
                ))
            })?;
            let start_offset: usize = parts[2].parse().map_err(|e| {
                DruidSegmentError::InvalidSmooshMeta(format!(
                    "Invalid start offset '{}': {}",
                    parts[2], e
                ))
            })?;
            let end_offset: usize = parts[3].parse().map_err(|e| {
                DruidSegmentError::InvalidSmooshMeta(format!(
                    "Invalid end offset '{}': {}",
                    parts[3], e
                ))
            })?;

            entries.insert(
                name.clone(),
                SmooshEntry {
                    name,
                    chunk_number,
                    start_offset,
                    end_offset,
                },
            );
        }

        // Memory-map each physical chunk file
        let mut mmaps = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let chunk_path = segment_dir.join(format!("{:05}.smoosh", i));
            let file = File::open(&chunk_path).map_err(|e| {
                DruidSegmentError::InvalidSmooshMeta(format!(
                    "Failed to open {}: {}",
                    chunk_path.display(),
                    e
                ))
            })?;
            // SAFETY: The file is opened read-only and we hold the Mmap for the
            // lifetime of SmooshReader. External mutation of the file while
            // mapped is undefined behavior, but this matches Druid's own
            // usage pattern with MappedByteBuffer.
            let mmap = unsafe { Mmap::map(&file)? };
            mmaps.push(mmap);
        }

        Ok(Self { entries, mmaps })
    }

    /// Return a byte slice for the named logical file.
    pub fn map_file(&self, name: &str) -> Result<&[u8]> {
        let entry = self
            .entries
            .get(name)
            .ok_or_else(|| DruidSegmentError::LogicalFileNotFound(name.to_string()))?;

        if entry.chunk_number >= self.mmaps.len() {
            return Err(DruidSegmentError::InvalidSmooshMeta(format!(
                "Chunk {} for file '{}' is out of range (have {} chunks)",
                entry.chunk_number,
                name,
                self.mmaps.len()
            )));
        }

        let mmap = &self.mmaps[entry.chunk_number];
        if entry.end_offset > mmap.len() {
            return Err(DruidSegmentError::InvalidSmooshMeta(format!(
                "File '{}' end offset {} exceeds chunk size {}",
                name,
                entry.end_offset,
                mmap.len()
            )));
        }

        Ok(&mmap[entry.start_offset..entry.end_offset])
    }

    /// Iterate over all logical file names (sorted).
    pub fn file_names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|s| s.as_str())
    }

    /// Check if a logical file exists in the archive.
    pub fn has_file(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Get the entry metadata for a logical file.
    pub fn entry(&self, name: &str) -> Option<&SmooshEntry> {
        self.entries.get(name)
    }

    /// Return all entries (sorted by name).
    pub fn entries(&self) -> impl Iterator<Item = &SmooshEntry> {
        self.entries.values()
    }

    /// Number of logical files in the archive.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the archive contains no logical files.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
