use std::collections::HashMap;
use crate::graph::{Graph};
use crate::{ChunkType, hash_function, levenshtein_functions, SBCMap};
use chunkfs::{ChunkHash, Data, DataContainer, Database, Scrub, ScrubMeasurements};
use std::io;
use crate::levenshtein_functions::{Action};
use crate::levenshtein_functions::Action::{Add, Del, Rep};
use crate::{SBCHash};
use std::time::{Instant};

impl Database<SBCHash, Vec<u8>> for SBCMap {
    fn insert(&mut self, sbc_hash: SBCHash, chunk: Vec<u8>) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, chunk);
        Ok(())
    }

    fn get(&self, sbc_hash: &SBCHash) -> io::Result<Vec<u8>> {
        let sbc_value = self.sbc_hashmap.get(sbc_hash).unwrap();

        let chunk = match sbc_hash.chunk_type {
            ChunkType::Simple { } => { sbc_value.clone() }
            ChunkType::Delta { } => {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&sbc_value[..4]);

                let parent_hash = u32::from_be_bytes(buf);
                let mut data = self.get(&SBCHash { key : parent_hash, chunk_type : ChunkType::Simple}).unwrap().clone();

                let mut index = 4;
                while index < sbc_value.len() {
                    buf.copy_from_slice(&sbc_value[index..index+4]);
                    let delta_action = u32::from_be_bytes(buf);

                    let (action, index, byte_value) = get_delta_action(delta_action);
                    match action {
                        Del => {
                            data.remove(index);
                        }
                        Add => data.insert(index + 1, byte_value),
                        Rep => data[index] = byte_value,
                    }
                }
                index += 4;
                data
            }
        };
        Ok(chunk)
    }

    fn remove(&mut self, sbc_hash: &SBCHash) {
        self.sbc_hashmap.remove(sbc_hash);
    }

    fn contains(&self, key: &SBCHash) -> bool {
        self.sbc_hashmap.contains_key(key)
    }

}

pub struct SBCScrubber {
    graph: Graph,
}

impl<Hash: ChunkHash, B> Scrub<Hash, B, SBCHash> for SBCScrubber
    where
        B: Database<Hash, DataContainer<SBCHash>>,
        for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<SBCHash>)>,
{
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
    ) -> io::Result<ScrubMeasurements>
        where
            Hash: 'a,
    {
        let time_start = Instant::now();
        let mut keys = Vec::new();

        for (_, data_container) in database.into_iter() {
            let chunk = data_container.extract();
            match chunk {
                Data::Chunk(data) => {
                    let sbc_hash = hash_function::hash(data.as_slice());
                    let _ = target_map.insert(SBCHash {key : sbc_hash, chunk_type : ChunkType::Simple}, data.clone());
                    keys.push(sbc_hash);
                }
                Data::TargetChunk(_) => {}
            }
        }


        let mut processed_data = 0;
        let mut data_left = 0;

        let modified_clusters = self.graph.update_graph_based_on_the_kraskal_algorithm(keys.as_slice());
        self.graph.set_parents_in_clusters(target_map, &modified_clusters);
        let mut key_index = 0;
        for (_, data_container) in database.into_iter() {
            let chunk = data_container.extract();
            match chunk {
                Data::Chunk(data) => {
                    if self.graph.vertices.get(&keys[key_index]).unwrap().parent == keys[key_index] {
                        data_left += data.len();
                        data_container.make_target(vec![SBCHash { key : keys[key_index], chunk_type : ChunkType::Simple }]);
                    } else {
                        processed_data += data.len();
                        data_container.make_target(vec![ SBCHash { key : keys[key_index], chunk_type : ChunkType::Delta }])
                    }
                }
                Data::TargetChunk(_) => {}
            }
            key_index += 1;
        }

        encode_map(&mut self.graph, target_map, &modified_clusters);

        let running_time = time_start.elapsed();
        Ok(ScrubMeasurements{
            processed_data,
            running_time,
            data_left,
        })
    }

}



fn encode_map(graph : &mut Graph, target_map : &mut Box<dyn Database<SBCHash, Vec<u8>>>, clusters : &HashMap<u32, Vec<u32>>) {
    for (hash_parent_cluster, cluster) in clusters.iter() {
        let parent_hash = graph.find_set(*hash_parent_cluster);
        let mut parent_chunk_data = Vec::new();

        if target_map.contains(&SBCHash { key : parent_hash, chunk_type : ChunkType::Delta}) {
            parent_chunk_data = target_map.get(&SBCHash { key : parent_hash, chunk_type : ChunkType::Delta}).unwrap().clone();
            target_map.remove(&SBCHash { key : parent_hash, chunk_type : ChunkType::Delta});
            let _ = target_map.insert(SBCHash { key : parent_hash, chunk_type : ChunkType::Simple}, parent_chunk_data.clone());
        } else {
            parent_chunk_data = target_map.get(&SBCHash { key : parent_hash, chunk_type : ChunkType::Simple}).unwrap().clone();
        }

        for hash in cluster {
            if *hash == parent_hash { continue; }
            let chunk_data = get_chunk_data(target_map, *hash);
            let mut delta_chunk = Vec::new();
            for byte in parent_hash.to_be_bytes() {
                delta_chunk.push(byte);
            }

            for delta_action in levenshtein_functions::encode(chunk_data.as_slice(), parent_chunk_data.as_slice()) {
                for byte in delta_action.to_be_bytes() {
                    delta_chunk.push(byte);
                }
            }
            if target_map.contains(&SBCHash { key : *hash, chunk_type : ChunkType::Simple }) {
                target_map.remove(&SBCHash { key : *hash, chunk_type : ChunkType::Simple });
            }
            let _ = target_map.insert(SBCHash { key : *hash, chunk_type : ChunkType::Delta }, delta_chunk);
        }
    }
}

fn get_delta_action(code : u32) -> (Action, usize, u8) {
    let action = match code / (1 << 30) {
        0 => Rep,
        1 => Add,
        2 => Del,
        _ => panic!(),
    };
    let byte_value = code % (1 << 30) / (1 << 22);
    let index = code % (1 << 22);
    (action, index as usize, byte_value as u8)
}

pub fn get_chunk_data(target_map : &mut Box<dyn Database<SBCHash, Vec<u8>>>, hash : u32) -> Vec<u8> {
    if target_map.contains(&SBCHash{key : hash, chunk_type : ChunkType::Delta}) {
        target_map.get(&SBCHash{key : hash, chunk_type : ChunkType::Delta}).unwrap()
    } else {
        target_map.get(&SBCHash{key : hash, chunk_type : ChunkType::Simple}).unwrap()
    }
}