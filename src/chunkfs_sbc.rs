use std::collections::HashMap;
use crate::graph::{Graph};
use crate::{hash_function, levenshtein_functions, SBCChunk, SBCMap};
use chunkfs::{ChunkHash, Data, DataContainer, Database, Scrub, ScrubMeasurements};
use std::io;
use crate::levenshtein_functions::{Action};

impl Database<u32, SBCChunk> for SBCMap {
    fn insert(&mut self, sbc_hash: u32, chunk: SBCChunk) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, chunk);
        Ok(())
    }

    fn get(&self, sbc_hash: &u32) -> io::Result<SBCChunk> {
        let mut chunk : SBCChunk;
        match self.sbc_hashmap.get(sbc_hash).unwrap() {
            SBCChunk::Simple { data } => {chunk = SBCChunk::Simple{ data : data.clone() }}
            SBCChunk::Delta { parent_hash, delta_code } => {
                chunk = SBCChunk::Delta{
                    parent_hash : *parent_hash,
                    delta_code : (*delta_code).clone()
                }}
        }
        Ok(chunk)

    }

    fn remove(&mut self, sbc_hash: &u32) {
        self.sbc_hashmap.remove(sbc_hash);
    }

}

pub struct SBCScrubber {
    graph: Graph,
}

impl<Hash: ChunkHash, B> Scrub<Hash, B, u32> for SBCScrubber
    where
        B: Database<Hash, DataContainer<u32>>,
        for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<u32>)>,
{
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        target_map: &mut Box<dyn Database<u32, Vec<u8>>>,
    ) -> ScrubMeasurements
        where
            Hash: 'a,
    {
        let mut pairs = Vec::new();

        for (_, data_container) in database {
            let chunk = data_container.extract();
            match chunk {
                Data::Chunk(data) => {
                    let sbc_hash = hash_function::hash(data.as_slice());
                    pairs.push((sbc_hash, data.clone()));
                    data_container.make_target(vec![sbc_hash]);
                }
                Data::TargetChunk(_) => {}
            }
        }



        let modified_clusters = self.graph.update_graph_based_on_the_kraskal_algorithm(pairs);
        self.graph.set_leaders_in_clusters(target_map, modified_clusters.values());
        encode_map(&mut self.graph, target_map, &modified_clusters);


        ScrubMeasurements::default()
    }

}


pub fn get_data_chunk(chunk : SBCChunk, target_map : &Box<dyn Database<u32, SBCChunk>>) -> Vec<u8> {
    match chunk {
        SBCChunk::Simple { data } => { data }
        SBCChunk::Delta { parent_hash, delta_code } => {
            let mut data = get_data_chunk(target_map.get(&parent_hash).unwrap(), target_map);
            for delta_action in delta_code {
                let (action, index, byte_value) = delta_action.get();
                match action {
                    Action::Del => {
                        data.remove(index);
                    }
                    Action::Add => data.insert(index + 1, byte_value),
                    Action::Rep => data[index] = byte_value,
                }
            }
            data
        }
    }
}


fn encode_map(graph : &mut Graph, target_map : &mut Box<dyn Database<u32, SBCChunk>>, clusters : &HashMap<u32, Vec<u32>>) {
    for (hash_parent_cluster, cluster) in clusters.iter() {
        let parent_hash = graph.find_set(*hash_parent_cluster);
        let parent_chunk_data = get_data_chunk(target_map.get(&parent_hash).unwrap(), target_map);
        let parent_chunk = SBCChunk::Simple { data : parent_chunk_data.clone() };
        let _ = target_map.insert(parent_hash, parent_chunk);

        for hash in cluster {
            if *hash == parent_hash { continue; }
            let chunk_data = get_data_chunk(target_map.get(hash).unwrap(), target_map);
            let chunk = SBCChunk::Delta {
                parent_hash,
                delta_code:
                levenshtein_functions::encode(chunk_data.as_slice(), parent_chunk_data.as_slice())
            };
            let _ = target_map.insert(*hash, chunk);
        }
    }
}