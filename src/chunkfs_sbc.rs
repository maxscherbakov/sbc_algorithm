use crate::graph::Graph;
use crate::levenshtein_functions::Action::{Add, Del, Rep};
use crate::levenshtein_functions::{levenshtein_distance, Action};
use crate::{SBCHash};
use crate::{hash_functions, levenshtein_functions, ChunkType, SBCMap};
use chunkfs::{ChunkHash, Data, DataContainer, Database, Scrub, ScrubMeasurements};
use std::collections::{HashMap, HashSet};
use std::io;
use std::time::Instant;

const WORD_LEN : usize = 8;
const COUNT_WORDS : usize = 5;

const MAX_COUNT_CHUNKS_IN_PACK : usize = 1024;

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
        let mut clusters : HashMap<u32, Vec<(u32, &mut DataContainer<SBCHash>)>> = HashMap::new();
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
                let (clusters_data_left, clusters_processed_data) = encode_clusters(&mut clusters, target_map);
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

fn encode_clusters(clusters : &mut HashMap<u32, Vec<(u32, &mut DataContainer<SBCHash>)>>,
                   target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>) -> (usize, usize) {
    let mut data_left = 0;
    let mut processed_data = 0;
    for (_, cluster) in clusters.iter_mut() {
        let data_analyse = encode_cluster(target_map, cluster.as_mut_slice());
        data_left += data_analyse.0;
        processed_data += data_analyse.1;
    }
    (data_left, processed_data)
}

fn encode_cluster(target_map : &mut Box<dyn Database<SBCHash, Vec<u8>>>, cluster : &mut [(u32, &mut DataContainer<SBCHash>)]) -> (usize, usize) {
    let (parent_hash, parent_data) = find_parent_key_in_cluster(cluster);
    let mut data_left = 0;
    let mut processed_data = 0;
    let mut target_hash = SBCHash::default();
    for (hash, data_container) in cluster.iter_mut() {
        match data_container.extract() {
            Data::Chunk(data) => {
                if *hash == parent_hash {
                    target_hash = SBCHash { key: *hash, chunk_type: ChunkType::Simple };
                    let _ = target_map.insert(target_hash.clone(), data.clone());
                    data_left += data.len();
                } else {
                    target_hash = SBCHash { key: *hash, chunk_type: ChunkType::Delta };
                    let mut delta_chunk = Vec::new();
                    for byte in parent_hash.to_be_bytes() {
                        delta_chunk.push(byte);
                    }
                    for delta_action in
                        levenshtein_functions::encode(data.as_slice(), parent_data.as_slice())
                    {
                        for byte in delta_action.to_be_bytes() {
                            delta_chunk.push(byte);
                        }
                    }
                    let _ = target_map.insert(target_hash.clone(), delta_chunk);
                    processed_data += data.len();
                }
            }
            Data::TargetChunk(_) => {}
        }
        data_container.make_target(vec![target_hash.clone(), ]);
    }
    (data_left, processed_data)
}

fn get_delta_action(code: u32) -> (Action, usize, u8) {
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

fn find_parent_key_in_cluster(cluster: &[(u32, &mut DataContainer<SBCHash>)]) -> (u32, Vec<u8>) {
    let mut leader_hash = cluster[0].0;
    let mut leader_data = Vec::new();
    let mut min_sum_dist = u32::MAX;

    for (hash_1, data_container_1) in cluster.iter() {
        let mut sum_dist_for_chunk = 0u32;
        for (hash_2, data_container_2) in cluster.iter() {
            if *hash_1 == *hash_2 {
                continue;
            }
            match data_container_1.extract() {
                Data::Chunk(data_1) => {
                    match data_container_2.extract() {
                        Data::Chunk(data_2) => {
                            sum_dist_for_chunk +=

                                levenshtein_distance((*data_1).as_slice(), (*data_2).as_slice());
                        }
                        Data::TargetChunk(_) => {}
                    }
                }
                Data::TargetChunk(_) => {}
            }
        }

        if sum_dist_for_chunk < min_sum_dist {
            leader_hash = *hash_1;
            leader_data = match data_container_1.extract() {
                Data::Chunk(data) => data.clone(),
                Data::TargetChunk(_) => Vec::new(),
            };
            min_sum_dist = sum_dist_for_chunk
        }
    }
    (leader_hash,leader_data)
}

fn set_for_chunk(data: &[u8]) -> HashSet<u32> {
    let size_block = WORD_LEN * COUNT_WORDS;
    let mut set_blocks = HashSet::new();
    let mut rabin_hash = rabin_hash_simple(&data[0..std::cmp::min(size_block, data.len())]);

    for index_word in (0..data.len()).step_by(WORD_LEN) {
        set_blocks.insert(rabin_hash);
        if index_word + size_block > data.len() {
            break
        }
        rabin_hash = rabin_hash_next(
            rabin_hash,
            hash_word(&data[index_word..index_word + WORD_LEN]),
            hash_word(&data[index_word + size_block..std::cmp::min(index_word + size_block + WORD_LEN, data.len())]));
    }
    set_blocks
}

fn rabin_hash_simple(data: &[u8]) -> u32{
    let mut rabin_hash = 0;
    let x  = 43u32;
    let q = (1 << 31) - 1;
    for i in (0..data.len()).step_by(WORD_LEN) {
        rabin_hash += hash_word(&data[i..i+WORD_LEN]) * x.pow((COUNT_WORDS - i / WORD_LEN) as u32) % q;
    }
    rabin_hash
}

fn hash_word(word: &[u8]) -> u32 {
    let mut hash_word = 0;
    for byte in word {
        hash_word += *byte as u32;
    }
    hash_word
}

fn rabin_hash_next(past_hash: u32, hash_start_word: u32, hash_next_word: u32) -> u32 {
    let x = 43u32;
    let q = (1 << 31) - 1;
    let hash_next = ((past_hash - hash_start_word * x.pow(COUNT_WORDS as u32 - 1)) * x + hash_next_word) % q;
    hash_next
}











