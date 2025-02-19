use crate::decoders::Decoder;
pub use chunkfs_sbc::SBCScrubber;
pub use hash_functions::sbc_hashing;
use std::collections::HashMap;

mod chunkfs_sbc;
pub mod decoders;
pub mod encoders;
mod graph;
mod hash_functions;
mod levenshtein_functions;

#[derive(Hash, PartialEq, Eq, Clone, Default, Debug)]
enum ChunkType {
    Delta(u16),
    #[default]
    Simple,
}

#[derive(Hash, PartialEq, Eq, Clone, Default)]
pub struct SBCHash {
    key: u32,
    chunk_type: ChunkType,
}

pub struct SBCMap<D: Decoder> {
    sbc_hashmap: HashMap<SBCHash, Vec<u8>>,
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

// impl Default for SBCMap {
//     fn default() -> Self {
//         Self::new()
//     }
// }
