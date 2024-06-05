pub use hash_function::hash;
use std::collections::HashMap;

mod chunkfs_sbc;
mod graph;
mod hash_function;
mod levenshtein_functions;

#[derive(Hash, PartialEq, Eq, Clone)]
enum ChunkType {
    Delta,
    Simple,
}
impl Default for ChunkType {
    fn default() -> Self { ChunkType::Simple }
}

#[derive(Hash, PartialEq, Eq, Clone, Default)]
pub struct SBCHash {
    key : u32,
    chunk_type: ChunkType,
}


#[allow(dead_code)]
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
