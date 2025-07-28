use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use chunkfs::{Data, Database};
use fasthash::spooky;
use crate::chunkfs_sbc::ClusterPoint;
use crate::decoder::Decoder;
use crate::encoder::{count_delta_chunks_with_hash, get_parent_data, Encoder};
use crate::encoder::gdelta_encoder::GEAR;
use crate::hasher::SBCHash;
use crate::{ChunkType, SBCKey, SBCMap};

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
        let source_chunks = gear_chunking(&parent_data);
        let source_chunks_indices = build_chunks_indices(&source_chunks);

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
                        &source_chunks_indices,
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
    /// * `source_chunks_indices` - Key is the chunk hash, value is its first position in the source data.
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
        source_chunks_indices: &HashMap<u64, usize>,
        source_hash: Hash,
    ) -> (usize, usize, SBCKey<Hash>) {
        let mut delta_code: Vec<u8> = Vec::new();
        let target_chunks = gear_chunking(target_data);

        for target_chunk in target_chunks.iter() {
            match find_match(source_data, source_chunks_indices, target_data) {
                Some(start_of_match_in_source_data) => {
                    // Copy
                    let copy_instruction_len = &target_chunk.len().to_ne_bytes()[..3];
                    let copy_instruction_offset = &start_of_match_in_source_data.to_ne_bytes()[..3];
                    delta_code.extend_from_slice(copy_instruction_len);
                    delta_code.extend_from_slice(copy_instruction_offset);
                }
                None => {
                    // Insert
                    let len_bytes = &mut (target_chunk.len() as u32).to_ne_bytes()[..3];
                    len_bytes[2] |= 1 << 7;
                    delta_code.extend_from_slice(len_bytes);
                    delta_code.extend_from_slice(target_chunk);
                }
            }
        }

        let (processed_data, sbc_hash) = store_delta_chunk(
            target_map,
            target_hash,
            source_hash,
            delta_code,
        );
        (0, processed_data, sbc_hash)
    }
}

/// Stores a delta-encoded chunk in the shared chunk map.
///
/// # Arguments
/// * `target_map` - Thread-safe reference to the chunk storage map (Arc<Mutex>).
/// * `target_hash` - Content hash of the original chunk data.
/// * `source_hash` - Hash of the parent chunk this delta is based on.
/// * `delta_code` - Raw delta-encoded data to store.
/// * `zstd_flag` - Whether to apply zstd compression to the delta data.
///
/// # Returns
/// A tuple containing:
/// 1. `usize` - Final size of the stored data (after optional compression).
/// 2. `SBCKey<Hash>` - Key under which the chunk was stored.
fn store_delta_chunk<D: Decoder, Hash: SBCHash>(
    target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
    hash: Hash,
    parent_hash: Hash,
    delta_code: Vec<u8>,
) -> (usize, SBCKey<Hash>) {
    let mut target_map_lock = target_map.lock().unwrap();
    let number_delta_chunk = count_delta_chunks_with_hash(&target_map_lock, &hash);
    let sbc_hash = SBCKey {
        hash,
        chunk_type: ChunkType::Delta {
            parent_hash,
            number: number_delta_chunk,
        },
    };

    let processed_data = delta_code.len();
    let _ = target_map_lock.insert(sbc_hash.clone(), delta_code);

    (processed_data, sbc_hash)
}

/// Finds a matching chunk in source data for the given target chunk.
///
/// # Arguments
/// * `source_data` - The original/reference data slice to search in
/// * `source_chunks_indices` - Precomputed hash map of chunk hashes to their positions in source_data
/// * `target_data` - The chunk of data to find in the source
///
/// # Returns
/// * `Some(usize)` - The starting position of the matching chunk in source_data if found
/// * `None` - If no matching chunk was found
fn find_match(
    source_data: &[u8],
    source_chunks_indices: &HashMap<u64, usize>,
    target_data: &[u8],
) -> Option<usize> {
    let target_hash = spooky::hash64(target_data);
    let &source_position = source_chunks_indices.get(&target_hash)?;

    if source_position + target_data.len() > source_data.len() {
        return None
    }

    let source_slice = &source_data[source_position..source_position + target_data.len()];
    if source_slice != target_data {
        return None
    }

    Some(source_position)
}

/// Creates an index of chunks for quick matching.
///
/// # Arguments
/// * `source_chunks` - vector of chunks from the base data block.
///
/// # Returns
/// Hash table, where key is the chunk hash, value is its first position in the source data.
fn build_chunks_indices(source_chunks: &Vec<&[u8]>) -> HashMap<u64, usize> {
    let mut chunks_indices: HashMap<u64, usize> = HashMap::new();
    let mut current_index: usize = 0;
    for chunk in source_chunks {
        let chunk_hash = spooky::hash64(chunk);
        chunks_indices.entry(chunk_hash).or_insert(current_index);
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

    let mask = (1 << AVERAGE_CHUNK_SIZE.next_power_of_two().trailing_zeros()) - 1;
    let mut data_index: usize = 0;
    while data_index < data.len() {
        current_window_hash = (current_window_hash << 1).wrapping_add(GEAR[data[data_index] as usize]);
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
    use crate::decoder;
    use crate::encoder::encode_simple_chunk;
    use crate::hasher::AronovichHash;

    #[test]
    fn find_match_should_return_none_for_empty_source_or_target() {
        let empty_data = &[];
        let empty_indices = HashMap::new();
        assert_eq!(
            find_match(empty_data, &empty_indices, b"test"),
            None,
            "Empty source should return None"
        );

        let non_empty_data = b"valid_data";
        let chunks = gear_chunking(non_empty_data);
        let indices = build_chunks_indices(&chunks);
        assert_eq!(
            find_match(non_empty_data, &indices, empty_data),
            None,
            "Empty target should return None"
        );
    }

    #[test]
    fn find_match_should_return_none_for_non_matching_data() {
        let source_data = vec![0u8; AVERAGE_CHUNK_SIZE * 2];
        let target_data = vec![1u8; AVERAGE_CHUNK_SIZE];

        let source_chunks = gear_chunking(&source_data);
        let source_indices = build_chunks_indices(&source_chunks);
        assert_eq!(
            find_match(&source_data, &source_indices, &target_data),
            None,
            "Non-matching data should return None"
        );
    }

    #[test]
    fn find_match_should_return_position_for_exact_match() {
        let data = b"__PATTERN1__PATTERN2__";
        let pattern = b"__PATTERN1_";
        let chunks = vec![
            &data[0..data.len() / 2],
            &data[data.len() / 2..],
        ];

        let chunk_indices = build_chunks_indices(&chunks);

        assert_eq!(
            find_match(data, &chunk_indices, pattern),
            Some(0),
            "Should find pattern at known position"
        );
    }

    #[test]
    fn build_chunks_indices_should_map_chunks_to_correct_positions() {
        let chunks: Vec<&[u8]> = vec![&[1u8; AVERAGE_CHUNK_SIZE], &[2u8; AVERAGE_CHUNK_SIZE]];

        let indices = build_chunks_indices(&chunks);
        assert_eq!(
            indices.get(&spooky::hash64(chunks[0])),
            Some(&0),
            "First chunk should be at position 0"
        );
        assert_eq!(
            indices.get(&spooky::hash64(chunks[1])),
            Some(&AVERAGE_CHUNK_SIZE),
            "Second chunk should be at position AVERAGE_CHUNK_SIZE"
        );
    }

    #[test]
    fn build_chunks_indices_should_handle_duplicate_hashes_correctly() {
        let chunks: Vec<&[u8]> = vec![&[1u8; AVERAGE_CHUNK_SIZE], &[1u8; AVERAGE_CHUNK_SIZE]];

        let indices = build_chunks_indices(&chunks);
        let hash = spooky::hash64(chunks[0]);
        assert_eq!(
            Some(&0),
            indices.get(&hash),
            "Only first position should be stored for duplicates"
        );
        assert_eq!(
            indices.len(),
            1,
            "HashMap should contain only one entry for duplicate chunks"
        );
    }

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
        let mut data = vec![0u8; AVERAGE_CHUNK_SIZE * 1000];
        rng.fill(&mut data[..]);

        let chunks = gear_chunking(&data);
        assert!(chunks.len() > 1, "Data should be split into multiple chunks");
    }

    #[test]
    fn test_restore_similarity_chunk_1_byte_diff() {
        let mut data: Vec<u8> = generate_test_data();
        let data2 = data.clone();
        if data[15] < 255 {
            data[15] = 255;
        } else {
            data[15] = 0;
        }

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_2_neighbor_byte_diff() {
        let mut data: Vec<u8> = generate_test_data();
        let data2 = data.clone();
        if data[15] < 255 {
            data[15] = 255;
        } else {
            data[15] = 0;
        }
        if data[16] < 255 {
            data[16] = 255;
        } else {
            data[16] = 0;
        }

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_2_byte_diff() {
        let mut data: Vec<u8> = generate_test_data();
        let data2 = data.clone();
        if data[15] < 255 {
            data[15] = 255;
        } else {
            data[15] = 0;
        }
        if data[106] < 255 {
            data[106] = 255;
        } else {
            data[106] = 0;
        }

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset_left() {
        let data: Vec<u8> = generate_test_data();
        let data2 = data[15..].to_vec();

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset_right() {
        let data: Vec<u8> = generate_test_data();
        let data2 = data[..8000].to_vec();

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset() {
        let data: Vec<u8> = generate_test_data();
        let mut data2 = data[15..8000].to_vec();
        data2[0] /= 3;
        data2[7000] /= 3;

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_cyclic_shift_right() {
        let data: Vec<u8> = generate_test_data();
        let mut data2 = data.clone();
        data2.extend(&data[8000..]);

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_ne!(data, []);
        assert_eq!(
            sbc_key.chunk_type,
            ChunkType::Delta {
                parent_hash: AronovichHash::new_with_u32(0),
                number: 0
            }
        );
        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_cyclic_shift_left() {
        let data: Vec<u8> = generate_test_data();
        let mut data2 = data[..192].to_vec();
        data2.extend(&data);

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_ne!(data, []);
        assert_eq!(
            sbc_key.chunk_type,
            ChunkType::Delta {
                parent_hash: AronovichHash::new_with_u32(0),
                number: 0
            }
        );
        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    fn generate_test_data() -> Vec<u8> {
        const TEST_DATA_SIZE: usize = 8192;
        (0..TEST_DATA_SIZE).map(|_| rand::random::<u8>()).collect()
    }

    fn create_map_and_key<'a>(
        data: &'a [u8],
        data2: &'a [u8],
    ) -> (
        SBCMap<decoder::GdeltaDecoder, AronovichHash>,
        SBCKey<AronovichHash>,
    ) {
        let source_chunks = gear_chunking(data);
        let word_hash_offsets = build_chunks_indices(&source_chunks);
        let mut binding = SBCMap::new(decoder::GdeltaDecoder::default());
        let sbc_map = Arc::new(Mutex::new(&mut binding));

        let (_, sbc_key) = encode_simple_chunk(
            &mut sbc_map.lock().unwrap(),
            data,
            AronovichHash::new_with_u32(0),
        );
        let (_, _, sbc_key_2) = DdeltaEncoder::default().encode_delta_chunk(
            sbc_map.clone(),
            data2,
            AronovichHash::new_with_u32(3),
            data,
            &word_hash_offsets,
            sbc_key.hash.clone(),
        );
        (binding, sbc_key_2)
    }
}