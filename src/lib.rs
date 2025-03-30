use crate::decoder::Decoder;
use crate::SBCHash::Aronovich;
pub use chunkfs_sbc::SBCScrubber;
use std::collections::HashMap;

mod chunkfs_sbc;
pub mod clusterer;
pub mod decoder;
pub mod encoder;
pub mod hasher;

#[derive(Hash, PartialEq, Eq, Clone, Default, Debug)]
enum ChunkType {
    Delta {
        parent_hash: SBCHash,
        number: u16,
    },
    #[default]
    Simple,
}

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub enum SBCHash {
    Aronovich(u32),
    Broder(u16),
}

impl Default for SBCHash {
    fn default() -> Self {
        Aronovich(u32::default())
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Default)]
pub struct SBCKey {
    hash: SBCHash,
    chunk_type: ChunkType,
}

pub struct SBCMap<D: Decoder> {
    sbc_hashmap: HashMap<SBCKey, Vec<u8>>,
    decoder: D,
}

impl<D: Decoder> SBCMap<D> {
    pub fn new(_decoder: D) -> SBCMap<D> {
        SBCMap {
            sbc_hashmap: HashMap::new(),
            decoder: _decoder,
        }
    }
}
