use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::decoder::Decoder;
use crate::hasher::SBCHash;
use crate::{SBCKey, SBCMap};
use crate::chunkfs_sbc::ClusterPoint;
use crate::encoder::Encoder;

const MIN_MATCH_LENGTH: usize = 3;

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

fn calculate_triplet_hash(triplet: &[u8]) -> Result<u32, &'static str> {
    if triplet.len() != MIN_MATCH_LENGTH {
        return Err("Invalid triplet length");
    }

    Ok(((triplet[0] as u32) << 16) | ((triplet[1] as u32) << 8) | triplet[2] as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculate_triplet_hash_should_return_correct_hash_for_normal_triplet() {
        let data = &[1, 2, 3];
        assert_eq!(calculate_triplet_hash(data), Ok(0x010203));
    }

    #[test]
    fn calculate_triplet_hash_should_return_error_for_triplet_of_wrong_size() {
        let data = &[1, 2];
        assert_eq!(
            calculate_triplet_hash(data),
            Err("Invalid triplet length")
        );
    }

    #[test]
    fn calculate_triplet_hash_should_return_correct_hash_for_edge_case_values() {
        assert_eq!(
            calculate_triplet_hash(&[0, 0, 0]),
            Ok(0x000000)
        );
        assert_eq!(
            calculate_triplet_hash(&[255, 255, 255]),
            Ok(0xFFFFFF)
        );
    }
}