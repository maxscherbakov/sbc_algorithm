use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use chunkfs::Data;
use fasthash::spooky;
use crate::chunkfs_sbc::ClusterPoint;
use crate::decoder::Decoder;
use crate::encoder::{get_parent_data, Encoder};
use crate::encoder::gdelta_encoder::GEAR;
use crate::hasher::SBCHash;
use crate::{SBCKey, SBCMap};

/// One kilobyte.
const KB: usize = 1024;
/// Expected arithmetic mean of all chunks present within a cluster (calculated empirically).
const AVERAGE_CHUNK_SIZE: usize = 8 * KB;
/// Threshold that determines when the Gear hash (fp) points to a chunk boundary.
const CHUNK_THRESHOLD: u64 = AVERAGE_CHUNK_SIZE as u64 / 2;

/// Ddelta compression encoder.
pub struct DdeltaEncoder;

impl Default for DdeltaEncoder {
    fn default() -> Self {
        Self
    }
}

impl Encoder for DdeltaEncoder {
    /// Encodes a cluster of data chunks using Ddelta compression against a parent chunk.
    ///
    /// # Arguments
    /// * `target_map` - Thread-safe reference to the chunk storage map (Arc<Mutex>).
    /// * `cluster` - Mutable slice of ClusterPoints to process.
    /// * `parent_hash` - Hash of the suggested parent chunk for delta reference.
    ///
    /// # Returns
    /// A tuple containing:
    /// 1. `usize` - Total bytes of data that couldn't be delta-encoded (left as-is).
    /// 2. `usize` - Total bytes of processed delta-encoded data.
    fn encode_cluster<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        cluster: &mut [ClusterPoint<Hash>],
        parent_hash: Hash,
    ) -> (usize, usize) {
        let mut processed_data = 0;
        let parent_chunk = get_parent_data(target_map.clone(), parent_hash.clone(), cluster);
        let mut data_left = parent_chunk.data_left;
        let parent_data = parent_chunk.parent_data;

        for (chunk_id, (hash, data_container)) in cluster.iter_mut().enumerate() {
            if parent_chunk.index > -1 && chunk_id == parent_chunk.index as usize {
                continue;
            }
            let mut target_hash = SBCKey::default();
            match data_container.extract() {
                Data::Chunk(data) => {
                    let (left, processed, sbc_hash) = self.encode_delta_chunk(
                        target_map.clone(),
                        data,
                        hash.clone(),
                        parent_data.as_slice(),
                        parent_hash.clone(),
                    );
                    data_left += left;
                    processed_data += processed;
                    target_hash = sbc_hash;
                }
                Data::TargetChunk(_) => {}
            }
            data_container.make_target(vec![target_hash]);
        }
        (data_left, processed_data)
    }
}

impl DdeltaEncoder {
    /// Creates a new DdeltaEncoder.
    fn new() -> DdeltaEncoder {
        DdeltaEncoder {}
    }

    /// Encodes a single data chunk using delta compression against a reference.
    ///
    /// # Arguments
    /// * `target_map` - Shared map for storing compressed chunks.
    /// * `target_data` - The data to be compressed.
    /// * `target_hash` - Hash identifier for the target data.
    /// * `source_data` - Reference data to compare against.
    /// * `source_hash` - Hash identifier for the parent/reference data.
    ///
    /// # Returns
    /// 1. Number of uncompressed bytes.
    /// 2. Total bytes processed.
    /// 3. Storage key for the compressed delta.
    fn encode_delta_chunk<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        target_data: &[u8],
        target_hash: Hash,
        source_data: &[u8],
        source_hash: Hash,
    ) -> (usize, usize, SBCKey<Hash>) {
        let mut delta_code: Vec<u8> = Vec::new();
        let source_chunks = gear_chunking(source_data);
        let target_chunks = gear_chunking(target_data);
        let source_chunks_indices = build_chunks_indices(source_chunks);

        let mut last_matched_end: usize = 0;
        for (i, target_chunk) in target_chunks.iter().enumerate() {
            let target_chunk_hash = spooky::hash64(target_chunk);
        }

        todo!()
    }
}

fn find_match(
    source_data: &[u8],
    source_chunks_indices: &HashMap<u64, usize>,
    target_data: &[u8],
    target_length: usize,
) -> Option<usize> {
    let target_hash = spooky::hash64(target_data);
    let &source_position = match source_chunks_indices.get(&target_hash) {
        Some(position) => position,
        None => return None,
    };

    if source_position + target_length <= source_data.len() {
        let source_slice = &source_data[source_position..source_position + target_length];
        // Дописать mamcmp
        if fast_memcmp(source_slice, target_data) {
            return Some(source_position);
        }
    }

    None
}

/// Creates an index of chunks for quick matching.
///
/// # Arguments
/// * `source_chunks` - vector of chunks from the base data block.
///
/// # Returns
/// Hash table, where key is the chunk hash, value is its position in the source data.
fn build_chunks_indices(source_chunks: Vec<&[u8]>) -> HashMap<u64, usize> {
    let mut chunks_indices: HashMap<u64, usize> = HashMap::new();
    let mut current_index: usize = 0;
    for chunk in source_chunks {
        let chunk_hash = spooky::hash64(chunk);
        chunks_indices.insert(chunk_hash, current_index);
        current_index += chunk.len();
    }

    chunks_indices
}

/// Splits input data into chunks using Gear-based Content-Defined Chunking (CDC) algorithm.
///
/// # Parameters
/// * `data` - Input byte slice to be chunked.
///
/// # Returns
/// Vector of byte slices (chunks) referencing the original data.
fn gear_chunking(data: &[u8]) -> Vec<&[u8]> {
    let mut source_chunks: Vec<&[u8]> = Vec::new();
    let mut current_window_hash: u64 = 0;
    let mut start_current_chunk: usize = 0;
    let mut data_index: usize = 0;
    while data_index < data.len() {
        current_window_hash = (current_window_hash << 1).wrapping_add(GEAR[data[data_index] as usize]);
        let mask = (1 << AVERAGE_CHUNK_SIZE.next_power_of_two().trailing_zeros()) - 1;
        if (current_window_hash & mask) == CHUNK_THRESHOLD {
            source_chunks.push(&data[start_current_chunk..data_index]);
            start_current_chunk = data_index;
        }

        data_index += 1;
    }

    if start_current_chunk < data.len() {
        source_chunks.push(&data[start_current_chunk..data.len()]);
    }

    source_chunks
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;

    #[test]
    fn gear_chunking_should_handle_empty_data() {
        let data = &[];
        assert_eq!(gear_chunking(data).len(), 0);
    }

    #[test]
    fn gear_chunking_should_handle_data_smaller_than_chunk() {
        let data = b"abc";
        let chunks = gear_chunking(data);
        assert_eq!(chunks, vec![b"abc".to_vec()]);
    }

    #[test]
    fn gear_chunking_should_return_chunk_for_exact_chunk_boundary() {
        let data = b"abcdefgh";
        let chunks = gear_chunking(data);
        assert_eq!(chunks, vec![b"abcdefgh".to_vec()]);
    }

    #[test]
    fn gear_chunking_should_split_data_into_multiple_chunks() {
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; AVERAGE_CHUNK_SIZE * 3];
        rng.fill(&mut data[..]);

        let chunks = gear_chunking(&data);
        assert!(!chunks.is_empty(), "Data should be split into multiple chunks");

        let total_len: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total_len, data.len());
    }

    #[test]
    fn gear_chunking_should_handle_last_chunk_smaller_than_average() {
        let data = vec![0u8; AVERAGE_CHUNK_SIZE + 100];
        let chunks = gear_chunking(&data);
        assert!(!chunks.is_empty());
        assert!(chunks.last().unwrap().len() <= AVERAGE_CHUNK_SIZE + 100);
    }
}