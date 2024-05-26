use graph::Graph;
pub use hash_function::hash;
use levenshtein_functions::{Action, DeltaAction};
use std::collections::HashMap;
use std::mem::size_of_val;

mod chunkfs_sbc;
mod graph;
mod hash_function;
mod levenshtein_functions;

pub fn hashmap_size(sbc_map: &SBCMap) -> usize {
    let mut size = 0;
    for (_, chunk) in sbc_map.sbc_hashmap.iter() {
        match chunk {
            SBCChunk::Simple { data } => size += data.len(),
            SBCChunk::Delta {
                parent_hash: hash,
                delta_code,
            } => size += size_of_val(hash) + delta_code.len() * size_of_val(&delta_code[0]),
        }
    }
    size
}

pub enum SBCChunk {
    Simple {
        data: Vec<u8>,
    },
    Delta {
        parent_hash: u32,
        delta_code: Vec<DeltaAction>,
    },
}

#[allow(dead_code)]
pub struct SBCMap {
    sbc_hashmap: HashMap<u32, SBCChunk>,
}

impl SBCMap {
    pub fn new(sbc_vec: Vec<(u32, Vec<u8>)>) -> SBCMap {
        SBCMap {
            sbc_hashmap: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::hash_function::hash;
    use crate::{SBCChunk, SBCMap};
    use fastcdc::v2016::FastCDC;
    use std::fs;
    use std::fs::File;
    use std::io::{BufReader, Read};

    fn create_sbc_vec(input: File, chunks: FastCDC) -> Vec<(u32, Vec<u8>)> {
        let mut sbc_vec = Vec::new();
        let mut buffer = BufReader::new(input);

        for chunk in chunks {
            let length = chunk.length;
            let mut bytes = vec![0; length];
            buffer.read_exact(&mut bytes).expect("buffer crash");

            let sbc_hash = hash(bytes.as_slice());
            sbc_vec.push((sbc_hash, bytes));
        }
        sbc_vec
    }

    fn crate_sbc_map(path: &str) -> SBCMap {
        let contents = fs::read(path).unwrap();
        let chunks = FastCDC::new(&contents, 1000, 2000, 65536);
        let input = File::open(path).expect("File not open");

        let sbc_vec = create_sbc_vec(input, chunks);
        let mut sbc_map = SBCMap::new(sbc_vec);
        sbc_map.encode();
        sbc_map
    }

    #[test]
    fn checking_for_simple_chunks() {
        let path = "runner/files/test1.txt";
        let sbc_map = crate_sbc_map(path);
        let mut count_simple_chunk = 0;
        for (_sbc_hash, chunk) in sbc_map.sbc_hashmap {
            match chunk {
                SBCChunk::Simple { .. } => count_simple_chunk += 1,
                SBCChunk::Delta { .. } => {}
            }
        }
        assert!(count_simple_chunk > 0)
    }

    #[test]
    fn checking_for_delta_chunks() {
        let path = "runner/files/test1.txt";
        let sbc_map = crate_sbc_map(path);
        let mut count_delta_chunk = 0;
        for (_sbc_hash, chunk) in sbc_map.sbc_hashmap {
            match chunk {
                SBCChunk::Simple { .. } => {}
                SBCChunk::Delta { .. } => count_delta_chunk += 1,
            }
        }
        assert!(count_delta_chunk > 0)
    }
}