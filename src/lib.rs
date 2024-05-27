pub use hash_function::hash;
use std::collections::HashMap;

mod chunkfs_sbc;
mod graph;
mod hash_function;
mod levenshtein_functions;

#[allow(dead_code)]
pub struct SBCMap {
    sbc_hashmap: HashMap<u32, Vec<u8>>,
}

impl SBCMap {
    pub fn new() -> SBCMap {
        SBCMap {
            sbc_hashmap: HashMap::new(),
        }
    }
}
