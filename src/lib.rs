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

pub(crate) enum Chunk {
    Simple {data : Vec<u8>},
    Delta {hash : u32, delta_code : Vec<DeltaAction>}
}

pub struct SBCMap {
    chunks_hashmap: HashMap<u32, Chunk>,
    graph: Graph,
}

fn match_chunk(chunks_hashmap : &HashMap<u32, Chunk>, hash: &u32) -> Vec<u8>{
    let chunk = chunks_hashmap.get(hash).unwrap();
    match chunk {
        Chunk::Simple { data } => data.clone(),
        Chunk::Delta { hash, delta_code } => {
            println!("{}", hash);
            let mut chunk_data = match_chunk(chunks_hashmap, hash);

            for delta_action in delta_code {
                match &delta_action.action {
                    Action::Del => {
                        chunk_data.remove(delta_action.index);
                    }
                    Action::Add => chunk_data.insert(delta_action.index, delta_action.byte_value),
                    Action::Rep => chunk_data[delta_action.index] = delta_action.byte_value,
                }
            }
            chunk_data
        }
    }
}

impl SBCMap {
    pub fn new(cdc_map : Vec<(u64, Vec<u8>)>) -> SBCMap {
        let mut chunks_hashmap : HashMap<u32, Chunk> = HashMap::new();
        for (_hash, chunk) in cdc_map {
            let chunk_hash = hash_function::hash(chunk.as_slice());
            chunks_hashmap.insert(chunk_hash, Chunk::Simple{data : chunk});
        }
        let graph = Graph::new(&chunks_hashmap);

        SBCMap {
            chunks_hashmap,
            graph,
        }
    }

    pub fn encode(&mut self) {
        for (hash, vertex) in &self.graph.vertices {
            if *hash != vertex.parent {
                let chunk_data_1 = match_chunk(&self.chunks_hashmap, &vertex.parent);
                let chunk_data_2 = match_chunk(&self.chunks_hashmap, hash);

                self.chunks_hashmap.insert(*hash, Chunk::Delta {
                    hash : vertex.parent,
                    delta_code : levenshtein_functions::encode(chunk_data_1.as_slice(),
                                                  chunk_data_2.as_slice())
                });
            }
        }
        println!("size after chunking: {}", size_hashmap(&self.chunks_hashmap));
    }

    // pub fn insert(&mut self, chunk : Vec<u8>) {
    //     let hash = hash_function::hash(chunk.as_slice());
    //     self.graph.add_vertex(hash);
    //
    //     let hash_leader = self.graph.vertices.get(&hash).unwrap().parent;
    //     if (hash_leader == hash) {
    //         self.chunks_hashmap.insert(hash, Box::new(ChunkWithFullCode::new(chunk)));
    //     } else {
    //         let chunk_data_1 = match self.chunks_hashmap.get(&hash_leader).unwrap().get_data() {
    //             DataEnum::Data(Data) => Data,
    //             DataEnum::DeltaCode {hash_leader_chunk, delta_code} => Vec::new(),
    //         };
    //         let chunk_data_2 = match self.chunks_hashmap.get(&hash).unwrap().get_data() {
    //             DataEnum::Data(Data) => Data,
    //             DataEnum::DeltaCode {hash_leader_chunk, delta_code} => Vec::new(),
    //         };
    //
    //         self.chunks_hashmap.insert(hash, Box::new(ChunkWithDeltaCode::new(
    //             hash_leader,
    //             levenshtein_functions::encode(chunk_data_1.as_slice(),
    //                                           chunk_data_2.as_slice())
    //         )));
    //     }
    // }

}
