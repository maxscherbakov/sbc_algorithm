pub use chunkfs_sbc::SBCScrubber;
pub use hash_functions::hash;
use std::collections::HashMap;

mod chunkfs_sbc;
mod clusterer;
mod graph;
mod hash_functions;
mod levenshtein_functions;

#[derive(Hash, PartialEq, Eq, Clone, Default)]
enum ChunkType {
    Delta,
    #[default]
    Simple,
}

#[derive(Hash, PartialEq, Eq, Clone, Default)]
pub struct SBCHash {
    key: u32,
    chunk_type: ChunkType,
}

pub struct SBCMap {
    sbc_hashmap: HashMap<SBCHash, Vec<u8>>,
}

impl SBCMap {
    pub fn new() -> SBCMap {
        SBCMap {
            sbc_hashmap: HashMap::new(),
        }
    }
}

impl Default for SBCMap {
    fn default() -> Self {
        Self::new()
    }
}
