use crate::graph::Graph;
use crate::levenshtein_functions::{
    get_delta_action,
    Action::{Add, Del, Rep},
};
use crate::{clusterer, hash_functions, ChunkType, SBCHash, SBCMap};
use chunkfs::{ChunkHash, Data, DataContainer, Database, Scrub, ScrubMeasurements};
use std::collections::HashMap;
use std::io;
use std::time::Instant;

const MAX_COUNT_CHUNKS_IN_PACK: usize = 1024;

impl Database<SBCHash, Vec<u8>> for SBCMap {
    fn insert(&mut self, sbc_hash: SBCHash, chunk: Vec<u8>) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, chunk);
        Ok(())
    }

    fn get(&self, sbc_hash: &SBCHash) -> io::Result<Vec<u8>> {
        let sbc_value = self.sbc_hashmap.get(sbc_hash).unwrap();

        let chunk = match sbc_hash.chunk_type {
            ChunkType::Simple {} => sbc_value.clone(),
            ChunkType::Delta {} => {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&sbc_value[..4]);

                let parent_hash = u32::from_be_bytes(buf);
                let mut data = if self.contains(&SBCHash {
                    key: parent_hash,
                    chunk_type: ChunkType::Delta,
                }) {
                    self.get(&SBCHash {
                        key: parent_hash,
                        chunk_type: ChunkType::Delta,
                    })
                    .unwrap()
                } else {
                    self.get(&SBCHash {
                        key: parent_hash,
                        chunk_type: ChunkType::Simple,
                    })
                    .unwrap()
                };

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
        let mut processed_data = 0;
        let mut data_left = 0;
        let count_chunks = database.into_iter().count();
        let mut clusters: HashMap<u32, Vec<(u32, &mut DataContainer<SBCHash>)>> = HashMap::new();
        for (chunk_index, (_, data_container)) in database.into_iter().enumerate() {
            match data_container.extract() {
                Data::Chunk(data) => {
                    let sbc_hash = hash_functions::hash(data.as_slice());
                    let parent_hash = self.graph.add_vertex(sbc_hash);
                    let cluster = clusters.entry(parent_hash).or_default();
                    cluster.push((sbc_hash, data_container));
                }
                Data::TargetChunk(_) => {}
            }
            if chunk_index % MAX_COUNT_CHUNKS_IN_PACK == 0 || chunk_index == count_chunks - 1 {
                let (clusters_data_left, clusters_processed_data) =
                    clusterer::encode_clusters(&mut clusters, target_map);
                data_left += clusters_data_left;
                processed_data += clusters_processed_data;
                clusters = HashMap::new();
            }
        }
        let running_time = time_start.elapsed();
        Ok(ScrubMeasurements {
            processed_data,
            running_time,
            data_left,
        })
    }
}
