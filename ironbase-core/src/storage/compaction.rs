// storage/compaction.rs
// Storage compaction functionality

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use serde_json::Value;
use crate::error::{Result};
use super::StorageEngine;

/// Compaction configuration
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Number of documents to process in memory at once (default: 1000)
    pub chunk_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            chunk_size: 1000,
        }
    }
}

/// Compaction statistics
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    pub size_before: u64,
    pub size_after: u64,
    pub documents_scanned: u64,
    pub documents_kept: u64,
    pub tombstones_removed: u64,
    pub peak_memory_mb: u64,  // Peak memory usage during compaction
}

impl CompactionStats {
    pub fn space_saved(&self) -> u64 {
        self.size_before.saturating_sub(self.size_after)
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.size_before == 0 {
            0.0
        } else {
            (self.size_after as f64 / self.size_before as f64) * 100.0
        }
    }
}

impl StorageEngine {
    /// Storage compaction - removes tombstones and old document versions
    /// Uses chunked processing to minimize memory usage
    pub fn compact(&mut self) -> Result<CompactionStats> {
        self.compact_with_config(&CompactionConfig::default())
    }

    /// Storage compaction with custom configuration
    pub fn compact_with_config(&mut self, config: &CompactionConfig) -> Result<CompactionStats> {
        let temp_path = format!("{}.compact", self.file_path);
        let mut stats = CompactionStats::default();

        // Get current file size
        stats.size_before = self.file.metadata()?.len();

        // Clone collections to avoid borrow conflicts
        let collections_snapshot = self.collections.clone();
        let file_len = self.file_len()?;

        // Create temporary new file
        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        // Prepare new collections metadata
        let mut new_collections = self.collections.clone();
        for coll_meta in new_collections.values_mut() {
            coll_meta.data_offset = super::DATA_START_OFFSET;
            coll_meta.document_catalog.clear();
            coll_meta.document_count = 0;
        }

        // Write placeholder metadata
        new_file.seek(SeekFrom::Start(0))?;
        Self::write_metadata(&mut new_file, &self.header, &new_collections)?;

        // Write documents starting at DATA_START_OFFSET
        new_file.seek(SeekFrom::Start(super::DATA_START_OFFSET))?;
        let mut write_offset = super::DATA_START_OFFSET;

        // Process each collection separately (collection-by-collection)
        for (coll_name, coll_meta) in &collections_snapshot {
            // Track latest version of each document in this collection using chunked processing
            let mut docs_by_id: HashMap<crate::document::DocumentId, Value> = HashMap::new();
            let mut current_offset = coll_meta.data_offset;
            let mut chunk_count = 0;
            // Scan all documents in this collection with chunked processing
            while current_offset < file_len {
                match self.read_data(current_offset) {
                    Ok(doc_bytes) => {
                        stats.documents_scanned += 1;

                        if let Ok(doc) = serde_json::from_slice::<Value>(&doc_bytes) {
                            // Check if this document belongs to this collection
                            let doc_collection = doc.get("_collection")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");

                            if doc_collection == coll_name {
                                if let Some(id_value) = doc.get("_id") {
                                    // Deserialize directly to DocumentId
                                    if let Ok(doc_id) = serde_json::from_value::<crate::document::DocumentId>(id_value.clone()) {
                                        // Track memory usage (estimate: document size + HashMap overhead)
                                        let doc_size_bytes = doc_bytes.len() as u64;
                                        let current_memory_bytes = docs_by_id.len() as u64 * doc_size_bytes;
                                        let current_memory_mb = current_memory_bytes / (1024 * 1024);
                                        if current_memory_mb > stats.peak_memory_mb {
                                            stats.peak_memory_mb = current_memory_mb;
                                        }

                                        docs_by_id.insert(doc_id, doc);
                                        chunk_count += 1;

                                        // If chunk is full, flush non-tombstones to new file
                                        if chunk_count >= config.chunk_size {
                                            write_offset = self.flush_compaction_chunk(
                                                &mut new_file,
                                                &mut new_collections,
                                                coll_name,
                                                &mut docs_by_id,
                                                write_offset,
                                                &mut stats,
                                            )?;
                                            chunk_count = 0;
                                            docs_by_id.clear();
                                        }
                                    }
                                }
                            }
                        }

                        current_offset += 4 + doc_bytes.len() as u64;
                    }
                    Err(_) => break,
                }
            }

            // Flush remaining documents in the final chunk
            if !docs_by_id.is_empty() {
                write_offset = self.flush_compaction_chunk(
                    &mut new_file,
                    &mut new_collections,
                    coll_name,
                    &mut docs_by_id,
                    write_offset,
                    &mut stats,
                )?;
            }
        }

        new_file.sync_all()?;

        // Now rewrite metadata with the populated document_catalog
        new_file.seek(SeekFrom::Start(0))?;
        Self::write_metadata(&mut new_file, &self.header, &new_collections)?;
        new_file.sync_all()?;

        // Get new file size
        stats.size_after = new_file.metadata()?.len();

        // Close new file before renaming
        drop(new_file);

        // Close old file and mmap
        drop(self.mmap.take());

        // Replace old file with new file
        fs::rename(&temp_path, &self.file_path)?;

        // Reopen the compacted file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)?;

        // Reload metadata
        let (header, collections) = Self::load_metadata(&mut file)?;

        // Update self
        self.file = file;
        self.header = header;
        self.collections = collections;
        self.mmap = None; // Reset mmap

        Ok(stats)
    }

    /// Helper function to flush a chunk of documents to the compacted file
    fn flush_compaction_chunk(
        &self,
        new_file: &mut std::fs::File,
        new_collections: &mut HashMap<String, super::CollectionMeta>,
        coll_name: &str,
        docs_by_id: &mut HashMap<crate::document::DocumentId, Value>,
        mut write_offset: u64,
        stats: &mut CompactionStats,
    ) -> Result<u64> {
        for (doc_id, doc) in docs_by_id.iter() {
            // Skip tombstones (deleted documents)
            if doc.get("_tombstone").and_then(|v| v.as_bool()).unwrap_or(false) {
                stats.tombstones_removed += 1;
                continue;
            }

            // Write document to new file
            let doc_offset = write_offset;
            let doc_bytes = serde_json::to_vec(&doc)?;
            let len = doc_bytes.len() as u32;

            new_file.write_all(&len.to_le_bytes())?;
            new_file.write_all(&doc_bytes)?;

            write_offset += 4 + doc_bytes.len() as u64;
            stats.documents_kept += 1;

            // Update document_catalog and document_count
            if let Some(coll_meta) = new_collections.get_mut(coll_name) {
                coll_meta.document_catalog.insert(doc_id.clone(), doc_offset);
                coll_meta.document_count += 1;
            }
        }

        Ok(write_offset)
    }
}
