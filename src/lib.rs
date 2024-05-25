use graph::Graph;
use levenshtein_functions::{Action, DeltaAction};
use std::collections::HashMap;
use std::mem::size_of_val;
use chunkfs::{ChunkHash};


mod graph;
mod hash_function;
mod levenshtein_functions;
mod chunkfs_sbc;

pub fn hashmap_size<Hash : ChunkHash> (sbc_map: &SBCMap<Hash>) -> usize {
    let mut size = 0;
    for (_, chunk) in sbc_map.sbc_hashmap.iter() {
        match chunk {
            Chunk::Simple { data } => size += data.len(),
            Chunk::Delta {
                parent_hash: hash,
                delta_code,
            } => size += size_of_val(hash) + delta_code.len() * size_of_val(&delta_code[0]),
        }
    }
    size
}

pub enum Chunk {
    Simple {
        data: Vec<u8>,
    },
    Delta {
        parent_hash: u32,
        delta_code: Vec<DeltaAction>,
    },
}

pub fn match_chunk(sbc_hashmap: &HashMap<u32, Chunk>, hash: &u32) -> Vec<u8> {
    let chunk = sbc_hashmap.get(hash).unwrap();
    match chunk {
        Chunk::Simple { data } => data.clone(),
        Chunk::Delta {
            parent_hash: hash,
            delta_code,
        } => {
            let mut chunk_data = match_chunk(sbc_hashmap, hash);
            for delta_action in delta_code {
                let (action, index, byte_value) = delta_action.get();
                match action {
                    Action::Del => {
                        chunk_data.remove(index);
                    }
                    Action::Add => chunk_data.insert(index + 1, byte_value),
                    Action::Rep => chunk_data[index] = byte_value,
                }
            }
            chunk_data
        }
    }
}

#[allow(dead_code)]
pub struct SBCMap<Hash : ChunkHash> {
    hashmap_transitions: HashMap<Hash, u32>,
    sbc_hashmap: HashMap<u32, Chunk>,
    graph: Graph,
}


impl<Hash : ChunkHash> SBCMap<Hash> {
    pub fn new(cdc_map: Vec<(Hash, Vec<u8>)>) -> SBCMap<Hash> {
        let mut hashmap_transitions = HashMap::new();
        let mut chunks_hashmap = HashMap::new();

        for (cdc_hash, chunk) in cdc_map {
            let sbc_hash = hash_function::hash(chunk.as_slice());
            hashmap_transitions.insert(cdc_hash, sbc_hash);
            chunks_hashmap.insert(sbc_hash, Chunk::Simple { data: chunk });
        }

        let graph = Graph::new(&chunks_hashmap);

        SBCMap {
            hashmap_transitions,
            sbc_hashmap: chunks_hashmap,
            graph,
        }
    }

    pub fn encode(&mut self) {
        for (hash, vertex) in &self.graph.vertices {
            match self.sbc_hashmap.get(&vertex.parent).unwrap() {
                Chunk::Simple { .. } => {}
                Chunk::Delta { .. } => {
                    self.sbc_hashmap.insert(vertex.parent, Chunk::Simple{data : match_chunk(&self.sbc_hashmap, &vertex.parent)});
                }
            }
            if *hash != vertex.parent {
                let chunk_data_parent = match_chunk(&self.sbc_hashmap, &vertex.parent);
                let chunk_data = match_chunk(&self.sbc_hashmap, hash);

                self.sbc_hashmap.insert(
                    *hash,
                    Chunk::Delta {
                        parent_hash: vertex.parent,
                        delta_code: levenshtein_functions::encode(
                            chunk_data.as_slice(),
                            chunk_data_parent.as_slice(),
                        ),
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Chunk, SBCMap};
    use fastcdc::v2016::FastCDC;
    use std::fs;
    use std::fs::File;
    use std::io::{BufReader, Read};
    fn create_cdc_vec(input: File, chunks: FastCDC) -> Vec<(u64, Vec<u8>)> {
        let mut cdc_vec = Vec::new();
        let mut buffer = BufReader::new(input);

        for chunk in chunks {
            let length = chunk.length;
            let mut bytes = vec![0; length];
            buffer.read_exact(&mut bytes).expect("buffer crash");
            cdc_vec.push((chunk.hash.clone(), bytes));
        }
        cdc_vec
    }

    fn crate_sbc_map(path: &str) -> SBCMap<u64> {
        let contents = fs::read(path).unwrap();
        let chunks = FastCDC::new(&contents, 1000, 2000, 65536);
        let input = File::open(path).expect("File not open");

        let cdc_vec = create_cdc_vec(input, chunks);
        let mut sbc_map = SBCMap::new(cdc_vec);
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
                Chunk::Simple { .. } => count_simple_chunk += 1,
                Chunk::Delta { .. } => {}
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
                Chunk::Simple { .. } => {}
                Chunk::Delta { .. } => count_delta_chunk += 1,
            }
        }
        assert!(count_delta_chunk > 0)
    }
}
