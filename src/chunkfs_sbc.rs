use crate::graph::Graph;
use crate::levenshtein_functions::{levenshtein_distance, Action, DeltaAction};
use crate::{hash_function, levenshtein_functions, SBCMap};
use chunkfs::{ChunkHash, Data, DataContainer, Database, Scrub, ScrubMeasurements};
use std::collections::HashMap;
use std::io;

impl Database<u32, Vec<u8>> for SBCMap {
    fn insert(&mut self, sbc_hash: u32, data: Vec<u8>) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, data);
        Ok(())
    }

    fn get(&self, sbc_hash: &u32) -> io::Result<&Vec<u8>> {
        Ok(self.sbc_hashmap.get(sbc_hash).unwrap())
    }

    fn remove(&mut self, sbc_hash: &u32) {
        self.sbc_hashmap.remove(sbc_hash);
    }

    fn contains(&self, key: &u32) -> bool {
        self.sbc_hashmap.contains_key(key)
    }
}

pub struct SBCScrubber {
    graph: Graph,
    delta_codes_hashmap: HashMap<u32, (u32, Vec<DeltaAction>)>,
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
    ) -> io::Result<ScrubMeasurements>
    where
        Hash: 'a,
    {
        let mut keys = Vec::new();

        for (_, data_container) in database {
            let chunk = data_container.extract();
            match chunk {
                Data::Chunk(data) => {
                    let sbc_hash = hash_function::hash(data.as_slice());
                    let _ = target_map.insert(sbc_hash, data.clone());
                    keys.push(sbc_hash);
                    data_container.make_target(vec![sbc_hash]);
                }
                Data::TargetChunk(_) => {}
            }
        }

        let modified_clusters = self
            .graph
            .update_graph_based_on_the_kraskal_algorithm(keys.as_slice());
        self.set_parents_in_clusters(target_map, &modified_clusters);
        self.encode_map(target_map, &modified_clusters);

        Ok(ScrubMeasurements::default())
    }
}

impl SBCScrubber {
    pub fn set_parents_in_clusters(
        &mut self,
        target_map: &mut Box<dyn Database<u32, Vec<u8>>>,
        clusters: &HashMap<u32, Vec<u32>>,
    ) {
        for (parent_hash_past, cluster) in clusters {
            let parent_key = self.find_parent_key_in_cluster(target_map, cluster.as_slice());
            self.graph.vertices.get_mut(&parent_key).unwrap().rank =
                self.graph.vertices.get(parent_hash_past).unwrap().rank;
            for hash in cluster.iter() {
                self.graph.vertices.get_mut(hash).unwrap().parent = parent_key
            }
        }
    }

    fn find_parent_key_in_cluster(
        &mut self,
        target_map: &mut Box<dyn Database<u32, Vec<u8>>>,
        cluster: &[u32],
    ) -> u32 {
        let mut leader_hash = cluster[0];
        let mut min_sum_dist = u32::MAX;

        for chunk_hash_1 in cluster.iter() {
            let mut sum_dist_for_chunk = 0u32;
            let chunk_data_1 = self.get_data_chunk(chunk_hash_1, target_map);

            for chunk_hash_2 in cluster.iter() {
                if *chunk_hash_1 == *chunk_hash_2 {
                    continue;
                }

                let chunk_data_2 = self.get_data_chunk(chunk_hash_2, target_map);
                sum_dist_for_chunk +=
                    levenshtein_distance(chunk_data_1.as_slice(), chunk_data_2.as_slice());
            }

            if sum_dist_for_chunk < min_sum_dist {
                leader_hash = *chunk_hash_1;
                min_sum_dist = sum_dist_for_chunk
            }
        }
        leader_hash
    }

    pub fn get_delta_chunk(
        &mut self,
        key: &u32,
        target_map: &Box<dyn Database<u32, Vec<u8>>>,
    ) -> Vec<u8> {
        let parent_key = self.delta_codes_hashmap.get(key).unwrap().0;
        let mut data = target_map.get(&parent_key).unwrap().clone();

        for delta_action in &self.delta_codes_hashmap.get(key).unwrap().1 {
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

    fn get_data_chunk(
        &mut self,
        key: &u32,
        target_map: &Box<dyn Database<u32, Vec<u8>>>,
    ) -> Vec<u8> {
        if target_map.contains(key) {
            target_map.get(key).unwrap().clone()
        } else {
            self.get_delta_chunk(key, target_map)
        }
    }

    fn encode_map(
        &mut self,
        target_map: &mut Box<dyn Database<u32, Vec<u8>>>,
        clusters: &HashMap<u32, Vec<u32>>,
    ) {
        for (past_parent_key, cluster) in clusters.iter() {
            let parent_key = self.graph.find_set(*past_parent_key);
            let parent_chunk_data = self.get_data_chunk(&parent_key, target_map);

            let _ = target_map.insert(parent_key, parent_chunk_data.clone());

            for key in cluster {
                let delta_chunk = self.delta_codes_hashmap.get(key).unwrap();
                if delta_chunk.0 == parent_key {
                    continue;
                }

                let chunk_data = self.get_data_chunk(key, target_map);
                self.delta_codes_hashmap.insert(
                    *key,
                    (
                        parent_key,
                        levenshtein_functions::encode(
                            chunk_data.as_slice(),
                            parent_chunk_data.as_slice(),
                        ),
                    ),
                );

                if target_map.contains(key) {
                    target_map.remove(key);
                }
            }
        }
    }
}
