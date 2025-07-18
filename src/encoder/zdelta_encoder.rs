use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::decoder::Decoder;
use crate::hasher::SBCHash;
use crate::{SBCKey, SBCMap};
use crate::chunkfs_sbc::ClusterPoint;
use crate::encoder::Encoder;

const MIN_MATCH_LENGTH: usize = 3;

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

impl ZdeltaEncoder {
    pub fn new(use_huffman_encoding: bool) -> Self {
        Self { use_huffman_encoding }
    }

    fn encode_delta_chunk<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        chunk_data: &[u8],
        hash: Hash,
        parent_data: &[u8],
        word_hash_offsets: &HashMap<u32, usize>,
        parent_hash: Hash,
    ) -> (usize, usize, SBCKey<Hash>) {
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
        todo!()
    }
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