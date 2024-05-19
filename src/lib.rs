use levenshtein_functions::{Action, DeltaAction};
use std::collections::HashMap;
use graph::Graph;

mod graph;
pub mod levenshtein_functions;
mod hash_function;

fn size_hashmap(hash_map: &HashMap<u32, Chunk>) -> u32 {
    let mut size = 0;
    for i in hash_map {
        match i.1 {
            Chunk::Simple { data } => size += data.len() as u32,
            Chunk::Delta { hash : _, delta_code } => size += 4 + delta_code.len() as u32 * 10,
        }
    }
    size
}


pub trait Map {
    fn get(&self, hash : u64) -> Vec<u8>;
    fn insert(&mut self, chunk : Vec<u8>, cdc_hash : u64);
}

enum Chunk {
    Simple {data : Vec<u8>},
    Delta {hash : u32, delta_code : Vec<DeltaAction>}
}

pub struct SBCMap {
    hashmap_transitions : HashMap<u64, u32>,
    sbc_hashmap: HashMap<u32, Chunk>,
    graph: Graph,
}

fn match_chunk(sbc_hashmap: &HashMap<u32, Chunk>, hash: &u32) -> Vec<u8>{
    let chunk = sbc_hashmap.get(hash).unwrap();
    match chunk {
        Chunk::Simple { data } => data.clone(),
        Chunk::Delta { hash, delta_code } => {
            let mut chunk_data = match_chunk(sbc_hashmap, hash);
            for delta_action in delta_code {
                match &delta_action.action {
                    Action::Del => {
                        chunk_data.remove(delta_action.index);
                    }
                    Action::Add => chunk_data.insert(delta_action.index + 1, delta_action.byte_value),
                    Action::Rep => chunk_data[delta_action.index] = delta_action.byte_value,
                }
            }
            chunk_data
        }
    }
}

impl SBCMap {
    pub fn new(cdc_map : Vec<(u64, Vec<u8>)>) -> SBCMap {
        let mut hashmap_transitions = HashMap::new();
        let mut chunks_hashmap = HashMap::new();

        for (cdc_hash, chunk) in cdc_map {
            let sbc_hash = hash_function::hash(chunk.as_slice());
            hashmap_transitions.insert(cdc_hash, sbc_hash);
            chunks_hashmap.insert(sbc_hash, Chunk::Simple{data : chunk});

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
            if *hash != vertex.parent {
                let chunk_data_parent = match_chunk(&self.sbc_hashmap, &vertex.parent);
                let chunk_data = match_chunk(&self.sbc_hashmap, hash);

                self.sbc_hashmap.insert(*hash, Chunk::Delta {
                    hash : vertex.parent,
                    delta_code : levenshtein_functions::encode(chunk_data.as_slice(),
                                                               chunk_data_parent.as_slice())
                });
            }
        }
        println!("size after chunking: {}", size_hashmap(&self.sbc_hashmap));
    }


}

impl Map for SBCMap {
    fn get(&self, cdc_hash: u64) -> Vec<u8> {
        let sbc_hash = self.hashmap_transitions.get(&cdc_hash).unwrap();
        match_chunk(&self.sbc_hashmap, sbc_hash)
    }

    fn insert(&mut self, data: Vec<u8>, cdc_hash : u64) {
        let sbc_hash = hash_function::hash(data.as_slice());

        self.hashmap_transitions.insert(cdc_hash, sbc_hash);
        self.graph.add_vertex(sbc_hash);

        let hash_leader = self.graph.vertices.get(&sbc_hash).unwrap().parent;

        if hash_leader == sbc_hash {
            self.sbc_hashmap.insert(sbc_hash, Chunk::Simple{data});
        } else {
            let chunk_data_1 = match_chunk(&self.sbc_hashmap, &hash_leader);

            self.sbc_hashmap.insert(sbc_hash, Chunk::Delta {
                hash: hash_leader,
                delta_code: levenshtein_functions::encode(chunk_data_1.as_slice(),
                                                          data.as_slice())
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::{BufReader, Read};
    use fastcdc::v2016::FastCDC;
    use crate::{Chunk, SBCMap};
    fn create_cdc_vec(input : File, chunks : FastCDC) -> Vec<(u64, Vec<u8>)>{
        let mut cdc_vec = Vec::new();
        let mut buffer = BufReader::new(input);

        for chunk in chunks {
            let length = chunk.length;
            let mut bytes = vec![0; length];
            buffer.read_exact(&mut bytes).expect("buffer crash");
            cdc_vec.push((chunk.hash, bytes));
        }
        cdc_vec
    }

    fn crate_sbc_map(path : &str) -> SBCMap {
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
                Chunk::Simple { .. } => {count_simple_chunk += 1}
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
                Chunk::Delta { .. } => {count_delta_chunk += 1}
            }
        }
        assert!(count_delta_chunk > 0)
    }
}