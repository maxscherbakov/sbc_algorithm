use crate::graph::Graph;
use crate::levenshtein_functions::{
    get_delta_action,
    Action::{Add, Del, Rep},
};
use crate::{clusterer, hash_functions, ChunkType, SBCHash, SBCMap};
use chunkfs::{
    ChunkHash, Data, DataContainer, Database, IterableDatabase, Scrub, ScrubMeasurements,
};
use std::collections::HashMap;
use std::io;
use std::time::Instant;

impl Database<SBCHash, Vec<u8>> for SBCMap {
    fn insert(&mut self, sbc_hash: SBCHash, chunk: Vec<u8>) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, chunk);
        Ok(())
    }

    fn get(&self, sbc_hash: &SBCHash) -> io::Result<Vec<u8>> {
        let sbc_value = match self.sbc_hashmap.get(sbc_hash) {
            None => {
                panic!("{}, {:?}", sbc_hash.key, sbc_hash.chunk_type)
            }
            Some(data) => data,
        };

        let chunk = match sbc_hash.chunk_type {
            ChunkType::Simple {} => sbc_value.clone(),
            ChunkType::Delta(_) => {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&sbc_value[..4]);

                let parent_hash = u32::from_be_bytes(buf);
                let mut data = self
                    .get(&SBCHash {
                        key: parent_hash,
                        chunk_type: ChunkType::Simple,
                    })
                    .unwrap();

                let mut byte_index = 4;
                while byte_index < sbc_value.len() {
                    buf.copy_from_slice(&sbc_value[byte_index..byte_index + 4]);
                    let delta_action = u32::from_be_bytes(buf);

                    let (action, index, byte_value) = get_delta_action(delta_action);
                    match action {
                        Del => {
                            data.remove(index);
                        }
                        Add => data.insert(index, byte_value),
                        Rep => data[index] = byte_value,
                    }
                    byte_index += 4;
                }
                data
            }
        };
        Ok(chunk)
    }

    // fn remove(&mut self, sbc_hash: &SBCHash) {
    //     self.sbc_hashmap.remove(sbc_hash);
    // }

    fn contains(&self, key: &SBCHash) -> bool {
        self.sbc_hashmap.contains_key(key)
    }
}

pub struct SBCScrubber {
    graph: Graph,
}

impl SBCScrubber {
    pub fn new() -> SBCScrubber {
        SBCScrubber {
            graph: Graph::new(),
        }
    }
}

impl Default for SBCScrubber {
    fn default() -> Self {
        Self::new()
    }
}

impl<Hash: ChunkHash, B> Scrub<Hash, B, SBCHash, SBCMap> for SBCScrubber
where
    B: IterableDatabase<Hash, DataContainer<SBCHash>>,
{
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        target_map: &mut SBCMap,
    ) -> io::Result<ScrubMeasurements>
    where
        Hash: 'a,
    {
        let time_start = Instant::now();
        let mut processed_data = 0;
        let mut data_left = 0;
        let mut clusters: HashMap<u32, Vec<(u32, &mut DataContainer<SBCHash>)>> = HashMap::new();
        for (_, data_container) in database.iterator_mut() {
            match data_container.extract() {
                Data::Chunk(data) => {
                    let sbc_hash = hash_functions::sbc_hashing(data.as_slice());
                    let parent_hash = self.graph.add_vertex(sbc_hash);
                    let cluster = clusters.entry(parent_hash).or_default();
                    cluster.push((sbc_hash, data_container));
                }
                Data::TargetChunk(_) => {}
            }
        }
        let time_hashing = time_start.elapsed();
        println!("time for hashing: {time_hashing:?}");
        let (clusters_data_left, clusters_processed_data) =
            clusterer::encode_clusters(&mut clusters, target_map);
        data_left += clusters_data_left;
        processed_data += clusters_processed_data;
        let running_time = time_start.elapsed();
        Ok(ScrubMeasurements {
            processed_data,
            running_time,
            data_left,
        })
    }
}
