use std::collections::HashMap;
use std::ptr::hash;
use std::sync::{Arc, Mutex};
use std::cmp::min;
use chunkfs::Data;
use crate::decoder::Decoder;
use crate::hasher::SBCHash;
use crate::{SBCKey, SBCMap};
use crate::chunkfs_sbc::ClusterPoint;
use crate::encoder::{get_parent_data, Encoder};

const MIN_MATCH_LENGTH: usize = 3;
const MAX_MATCH_LENGTH: usize = 1026;
const MAX_OFFSET: i32 = 32766;

type Triplet = [u8; 3];
type TripletHash = u32;
type PositionInChunk = usize;
type TripletLocations = Vec<PositionInChunk>;

pub enum ReferencePointerType {
    Main,
    Auxiliary,
    TargetLocal,
}

struct MatchPointers {
    target_ptr: usize,
    main_ref_ptr: usize,
    auxiliary_ref_ptr: usize,
}

impl MatchPointers {
    pub fn new(target_ptr: usize, main_ref_ptr: usize, auxiliary_ref_ptr: usize) -> Self {
        MatchPointers { target_ptr, main_ref_ptr, auxiliary_ref_ptr }
    }

    pub fn calculate_offset(&self, parent_position: usize) -> (i32, ReferencePointerType) {
        if parent_position <= self.target_ptr {
            let offset = parent_position as i32 - self.target_ptr as i32;
            return (offset, ReferencePointerType::TargetLocal);
        }

        let offset_main = parent_position as i32 - self.main_ref_ptr as i32;
        let offset_auxiliary = parent_position as i32 - self.auxiliary_ref_ptr as i32;

        if offset_main.abs() <= offset_auxiliary.abs() {
            (offset_main, ReferencePointerType::Main)
        } else {
            (offset_auxiliary, ReferencePointerType::Auxiliary)
        }
    }
}

pub struct ZdeltaEncoder {
    use_huffman_encoding: bool,
}

impl Default for ZdeltaEncoder {
    fn default() -> Self {
        Self::new(true)
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

fn select_best_match(
    target_data: &[u8],
    parent_data: &[u8],
    current_position: usize,
    parent_positions: &[usize],
    pointers: &MatchPointers,
) -> Option<(usize, i32, ReferencePointerType)> {
    let mut best_match = None;
    let mut best_score = 0;

    for &parent_position in parent_positions {
        if let Some(length) = find_max_match_length(target_data, parent_data, current_position, parent_position) {
            let (offset, pointer_type) = pointers.calculate_offset(parent_position);

            let adjusted_length = if offset.abs() > 4096 {
                length.saturating_sub(1)
            } else {
                length
            };

            let score = (adjusted_length << 16) | (!offset.abs() as usize & 0xFFFF);

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
        target_data.len() - start_position_in_parent,
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

    const PARENT: &[u8] = b"abc123def456";
    const TARGET: &[u8] = b"abc123xyzabc";

    #[test]
    fn find_max_match_length_should_return_full_match_length_when_sequences_are_identical() {
        let result = find_max_match_length(TARGET, PARENT, 0, 0);
        assert_eq!(result, Some(6));
    }

    #[test]
    fn find_max_match_length_should_return_min_match_length_when_only_triplet_matches() {
        let result = find_max_match_length(TARGET, PARENT, 3, 3);
        assert_eq!(result, Some(3));
    }

    #[test]
    fn find_max_match_length_should_return_none_when_triplet_does_not_match() {
        let result = find_max_match_length(TARGET, PARENT, 9, 9);
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
}