use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::cmp::min;
use chunkfs::Data;
use crate::decoder::Decoder;
use crate::hasher::SBCHash;
use crate::{SBCKey, SBCMap};
use crate::chunkfs_sbc::ClusterPoint;
use crate::encoder::{get_parent_data, Encoder};
use crate::encoder::zdelta_match_pointers::{MatchPointers, ReferencePointerType};

const LARGE_OFFSET_PENALTY_THRESHOLD: i32 = 4096;
const MIN_MATCH_LENGTH: usize = 3;
const MAX_MATCH_LENGTH: usize = 1026;

type Triplet = [u8; 3];
type TripletHash = u32;
type PositionInChunk = usize;
type TripletLocations = Vec<PositionInChunk>;

pub struct ZdeltaEncoder {
    use_huffman_encoding: bool,
}

impl Default for ZdeltaEncoder {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Encoder for ZdeltaEncoder {
    fn encode_cluster<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        cluster: &mut [ClusterPoint<Hash>],
        parent_hash: Hash
    ) -> (usize, usize) {
        let parent_info = get_parent_data(target_map.clone(), parent_hash.clone(), cluster);
        let mut data_left = parent_info.data_left;
        let mut total_processed_bytes = 0;
        let parent_data = parent_info.parent_data;
        let parent_triplet_lookup_table = build_triplet_lookup_table(&parent_data);

        for (chunk_id, (hash, data_container)) in cluster.iter_mut().enumerate() {
            if parent_info.index > -1 && chunk_id == parent_info.index as usize {
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
                        &parent_triplet_lookup_table,
                        parent_hash.clone(),
                    );
                    data_left += left;
                    total_processed_bytes += processed;
                    target_hash = sbc_hash;
                }
                Data::TargetChunk(_) => {}
            }
            data_container.make_target(vec![target_hash]);
        }
        (data_left, total_processed_bytes)
    }
}

impl ZdeltaEncoder {
    pub fn new(use_huffman_encoding: bool) -> Self {
        Self { use_huffman_encoding }
    }

    fn encode_delta_chunk<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        target_data: &[u8],
        target_hash: Hash,
        parent_data: &[u8],
        parent_triplet_lookup_table: &HashMap<TripletHash, TripletLocations>,
        parent_hash: Hash,
    ) -> (usize, usize, SBCKey<Hash>) {
        let mut delta_code : Vec<u8> = Vec::new();
        let mut uncompressed_data = 0;
        let mut pointers = MatchPointers::new(0, 0, 0);

        let mut i : PositionInChunk = 0;
        while i + MIN_MATCH_LENGTH <= target_data.len() {
            let mut triplet = [0u8; 3];
            triplet.copy_from_slice(&target_data[i..i+3]);
            let hash = compute_triplet_hash(&triplet);

            if let Some(parent_positions) = parent_triplet_lookup_table.get(&hash) {
                if let Some((length, offset, pointer_type)) =
                    select_best_match(target_data, parent_data, i, parent_positions, &pointers) {
                    // Кодируем совпадение
                    // Обновляем указатели
                    i += length;
                    continue;
                }
            }
        }

        todo!();
    }
}

fn select_best_match(
    target_data: &[u8],
    parent_data: &[u8],
    current_position: usize,
    parent_positions: &[usize],
    pointers: &MatchPointers,
) -> Option<(usize, i32, ReferencePointerType)> {
    const SCORE_LENGTH_SHIFT: usize = 16;
    const MAX_SCORE_OFFSET: usize = 0xFFFF;

    let mut best_match = None;
    let mut best_score = 0;

    for &parent_position in parent_positions {
        if let Some(length) = find_max_match_length(target_data, parent_data, current_position, parent_position) {
            let (offset, pointer_type) = pointers.calculate_offset(parent_position);

            let adjusted_length = if offset.abs() > LARGE_OFFSET_PENALTY_THRESHOLD {
                length.saturating_sub(1)
            } else {
                length
            };

            let score = (adjusted_length << SCORE_LENGTH_SHIFT) | (!offset.abs() as usize & MAX_SCORE_OFFSET);

            if score > best_score {
                best_score = score;
                best_match = Some((length, offset, pointer_type));
            }
        }
    }

    best_match
}

fn find_max_match_length(
    target_data: &[u8],
    parent_data: &[u8],
    start_position_in_target: PositionInChunk,
    start_position_in_parent: PositionInChunk,
) -> Option<usize> {
    if start_position_in_target + MIN_MATCH_LENGTH > target_data.len() ||
        start_position_in_parent + MIN_MATCH_LENGTH > parent_data.len() ||
        target_data[start_position_in_target..start_position_in_target + MIN_MATCH_LENGTH] !=
            parent_data[start_position_in_parent..start_position_in_parent + MIN_MATCH_LENGTH] {
        return None;
    }

    let max_possible_match_length = min(
        parent_data.len() - start_position_in_parent,
        target_data.len() - start_position_in_target,
    ).min(MAX_MATCH_LENGTH);

    let mut match_length = MIN_MATCH_LENGTH;
    while match_length < max_possible_match_length
    && target_data[start_position_in_target + match_length] == parent_data[start_position_in_parent + match_length] {
        match_length += 1;
    }
    Some(match_length)
}

fn compute_triplet_hash(triplet: &Triplet) -> TripletHash {
    ((triplet[0] as u32) << 16) | ((triplet[1] as u32) << 8) | triplet[2] as u32
}

fn build_triplet_lookup_table(chunk: &[u8]) -> HashMap<TripletHash, TripletLocations> {
    let mut lookup_table : HashMap<TripletHash, TripletLocations> = HashMap::new();

    for (current_position, triplet) in chunk.windows(MIN_MATCH_LENGTH).enumerate() {
        let triplet_array: Triplet = triplet.try_into().unwrap();
        let hash = compute_triplet_hash(&triplet_array);

        lookup_table
            .entry(hash)
            .or_default()
            .push(current_position);
    }

    lookup_table
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn select_best_match_should_find_best_match_with_small_offset() {
        let target = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let parent = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK".to_vec();
        let pointers = MatchPointers::new(0, 10, 20);
        let parent_positions = vec![10];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((26, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn select_best_match_should_apply_penalty_for_large_offset() {
        let target = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
        let parent = b"012345678#012345678#".repeat(500).to_vec();
        let pointers = MatchPointers::new(0, 0, 10_000);
        let parent_positions = vec![0, 10_000 - 10];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((9, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn select_best_match_should_prefer_closer_match_when_lengths_equal() {
        let target = b"abcdef".to_vec();
        let parent = b"xxabcdefyyabcdefzz".to_vec();
        let pointers = MatchPointers::new(0, 2, 10);
        let parent_positions = vec![2, 10];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((6, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn select_best_match_should_prefer_longer_match_over_closer() {
        let target = b"abcdefgh".to_vec();
        let parent = b"abcdwxyzabcdefghijkl".to_vec();
        let pointers = MatchPointers::new(0, 0, 8);
        let parent_positions = vec![0, 8];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((8, 0, ReferencePointerType::Auxiliary)));
    }

    #[test]
    fn select_best_match_should_use_target_local_for_matches_before_target_ptr() {
        let target = b"abcdef".to_vec();
        let parent = b"abcdef".to_vec();
        let pointers = MatchPointers::new(10, 0, 0);
        let parent_positions = vec![0];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((6, -10, ReferencePointerType::TargetLocal)));
    }

    #[test]
    fn select_best_match_should_return_none_when_no_matches_found() {
        let target = b"abcdef".to_vec();
        let parent = b"ghijkl".to_vec();
        let pointers = MatchPointers::default();
        let parent_positions = vec![0];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, None);
    }

    #[test]
    fn select_best_match_should_handle_min_length_match() {
        let target = b"abc".to_vec();
        let parent = b"xyzabc123".to_vec();
        let pointers = MatchPointers::new(0, 3, 0);
        let parent_positions = vec![3];

        let result = select_best_match(&target, &parent, 0, &parent_positions, &pointers);

        assert_eq!(result, Some((3, 0, ReferencePointerType::Main)));
    }

    #[test]
    fn find_max_match_length_should_return_full_match_length_when_sequences_are_identical() {
        let (parent_data, target_data) = create_test_data_for_find_max_match_length();
        let result = find_max_match_length(target_data, parent_data, 0, 0);
        assert_eq!(result, Some(6));
    }

    #[test]
    fn find_max_match_length_should_return_min_match_length_when_only_triplet_matches() {
        let (parent_data, target_data) = create_test_data_for_find_max_match_length();
        let result = find_max_match_length(target_data, parent_data, 3, 3);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn find_max_match_length_should_return_none_when_triplet_does_not_match() {
        let (parent_data, target_data) = create_test_data_for_find_max_match_length();
        let result = find_max_match_length(target_data, parent_data, 9, 9);
        assert_eq!(result, None);
    }

    #[test]
    fn find_max_match_length_should_respect_max_length_limit() {
        let long_data = vec![b'X'; 2000];
        let result = find_max_match_length(&long_data, &long_data, 0, 0);
        assert_eq!(result, Some(MAX_MATCH_LENGTH));
    }

    #[test]
    fn find_max_match_length_should_handle_edge_cases_safely() {
        assert_eq!(find_max_match_length(b"", b"", 0, 0), None);
        assert_eq!(find_max_match_length(b"a", b"a", 0, 0), None); // Меньше MIN_MATCH_LENGTH
    }

    #[test]
    fn find_max_match_length_should_detect_hash_collisions_correctly() {
        let parent = b"abc"; // Хэш может совпадать с "abd"
        let target = b"abd";
        assert_eq!(find_max_match_length(target, parent, 0, 0), None);
    }

    #[test]
    fn build_triplet_lookup_table_should_handles_duplicate_triplets_correctly() {
        let data = b"abcabcabc";
        let table = build_triplet_lookup_table(data);

        assert_eq!(table.len(), 3);

        assert_eq!(
            table.get(&compute_triplet_hash(b"abc")),
            Some(&vec![0, 3, 6])
        );
        assert_eq!(
            table.get(&compute_triplet_hash(b"bca")),
            Some(&vec![1, 4])
        );
        assert_eq!(
            table.get(&compute_triplet_hash(b"cab")),
            Some(&vec![2, 5])
        );
    }

    #[test]
    fn compute_triplet_hash_should_return_correct_hash_for_normal_triplet() {
        let data : Triplet = [1, 2, 3];
        assert_eq!(compute_triplet_hash(&data), 0x010203);
    }

    #[test]
    fn compute_triplet_hash_should_return_correct_hash_for_edge_case_values() {
        assert_eq!(
            compute_triplet_hash(&[0, 0, 0]),
            0x000000
        );
        assert_eq!(
            compute_triplet_hash(&[255, 255, 255]),
            0xFFFFFF
        );
    }

    fn create_test_data_for_find_max_match_length<'a>() -> (&'a [u8], &'a [u8]) {
        let target_data = b"abc123xyzabc";
        let parent_data = b"abc123def456";
        (target_data, parent_data)
    }
}