use crate::decoder::Decoder;
pub use chunkfs_sbc::SBCScrubber;
use std::collections::HashMap;
use hasher::SBCHash;

mod chunkfs_sbc;
pub mod clusterer;
pub mod decoder;
pub mod encoder;
pub mod hasher;

#[derive(Hash, PartialEq, Eq, Clone, Default, Debug)]
enum ChunkType<Hash: SBCHash> {
    Delta {
        parent_hash: Hash,
        number: u16,
    },
    #[default]
    Simple,
}



#[derive(Hash, PartialEq, Eq, Clone, Default)]
pub struct SBCKey<H: SBCHash> {
    hash: H,
    chunk_type: ChunkType<H>,
}

pub struct SBCMap<D: Decoder, H: SBCHash> {
    sbc_hashmap: HashMap<SBCKey<H>, Vec<u8>>,
    decoder: D,
}

impl<D: Decoder, H: SBCHash> SBCMap<D, H> {
    pub fn new(_decoder: D) -> SBCMap<D, H> {
        SBCMap {
            sbc_hashmap: HashMap::new(),
            decoder: _decoder,
        }
    }
}
