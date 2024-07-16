use crate::graph::Graph;
use crate::levenshtein_functions::Action::{Add, Del, Rep};
use crate::levenshtein_functions::{levenshtein_distance, Action};
use crate::SBCHash;
use crate::{hash_function, levenshtein_functions, ChunkType, SBCMap};
use chunkfs::{ChunkHash, Data, DataContainer, Database, Scrub, ScrubMeasurements};
use std::collections::HashMap;
use std::io;
use std::time::Instant;

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
                        Add => data.insert(index + 1, byte_value),
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
    fn new() -> SBCScrubber {
        SBCScrubber {
            graph: Graph::new(),
        }
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

        for (_, data_container) in database.into_iter() {
            let chunk = data_container.extract();
            match chunk {
                Data::Chunk(data) => {
                    let sbc_hash = hash_function::hash(data.as_slice());
                    let (target_hash, parent_hash) = self.graph.add_vertex(sbc_hash);
                    match target_hash.chunk_type {
                        ChunkType::Delta => {
                            let parent_chunk_data = target_map.get(&SBCHash { key : parent_hash, chunk_type : ChunkType::Simple}).unwrap();
                            let mut delta_chunk = Vec::new();
                            for byte in parent_hash.to_be_bytes() {
                                delta_chunk.push(byte);
                            }
                            for delta_action in
                                levenshtein_functions::encode(data.as_slice(), parent_chunk_data.as_slice())
                            {
                                for byte in delta_action.to_be_bytes() {
                                    delta_chunk.push(byte);
                                }
                            }
                            let _ = target_map.insert(target_hash.clone(), delta_chunk);
                            processed_data += data.len();
                        }
                        ChunkType::Simple => {
                            let _ = target_map.insert(target_hash.clone(), data.clone());
                            data_left += data.len();
                        }
                    }
                    data_container.make_target(vec![target_hash, ]);
                }
                Data::TargetChunk(_) => {}
            }
        }
        //
        // let modified_clusters = self
        //     .graph
        //     .update_graph_based_on_the_kraskal_algorithm(keys.as_slice());
        // encode_map(&mut self.graph, target_map, &modified_clusters);
        //
        // let mut key_index = 0;
        // for (_, data_container) in database.into_iter() {
        //     let chunk = data_container.extract();
        //     match chunk {
        //         Data::Chunk(data) => {
        //             if self.graph.vertices.get(&keys[key_index]).unwrap().parent == keys[key_index]
        //             {
        //                 data_left += data.len();
        //                 data_container.make_target(vec![SBCHash {
        //                     key: keys[key_index],
        //                     chunk_type: ChunkType::Simple,
        //                 }]);
        //             } else {
        //                 processed_data += data.len();
        //                 data_container.make_target(vec![SBCHash {
        //                     key: keys[key_index],
        //                     chunk_type: ChunkType::Delta,
        //                 }])
        //             }
        //         }
        //         Data::TargetChunk(_) => {}
        //     }
        //     key_index += 1;
        // }
        //
        let running_time = time_start.elapsed();
        Ok(ScrubMeasurements {
            processed_data,
            running_time,
            data_left,
        })
    }
}

fn encode_map(
    graph: &mut Graph,
    target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
    clusters: &HashMap<u32, Vec<u32>>,
) {
    for (hash_parent_cluster, cluster) in clusters.iter() {
        let parent_hash = find_parent_key_in_cluster(target_map, cluster.as_slice());
        graph.vertices.get_mut(&parent_hash).unwrap().rank =
            graph.vertices.get(&hash_parent_cluster).unwrap().rank;

        for hash in cluster {
            graph.vertices.get_mut(hash).unwrap().parent = parent_hash
        }

        let mut parent_chunk_data = Vec::new();

        if target_map.contains(&SBCHash {
            key: parent_hash,
            chunk_type: ChunkType::Delta,
        }) {
            parent_chunk_data = target_map
                .get(&SBCHash {
                    key: parent_hash,
                    chunk_type: ChunkType::Delta,
                })
                .unwrap()
                .clone();
            target_map.remove(&SBCHash {
                key: parent_hash,
                chunk_type: ChunkType::Delta,
            });
            let _ = target_map.insert(
                SBCHash {
                    key: parent_hash,
                    chunk_type: ChunkType::Simple,
                },
                parent_chunk_data.clone(),
            );
        } else {
            parent_chunk_data = target_map
                .get(&SBCHash {
                    key: parent_hash,
                    chunk_type: ChunkType::Simple,
                })
                .unwrap()
                .clone();
        }

        for hash in cluster {
            if *hash == parent_hash {
                continue;
            }
            let chunk_data = get_chunk_data(target_map, *hash);
            let mut delta_chunk = Vec::new();
            for byte in parent_hash.to_be_bytes() {
                delta_chunk.push(byte);
            }

            for delta_action in
                levenshtein_functions::encode(chunk_data.as_slice(), parent_chunk_data.as_slice())
            {
                for byte in delta_action.to_be_bytes() {
                    delta_chunk.push(byte);
                }
            }
            if target_map.contains(&SBCHash {
                key: *hash,
                chunk_type: ChunkType::Simple,
            }) {
                target_map.remove(&SBCHash {
                    key: *hash,
                    chunk_type: ChunkType::Simple,
                });
            }
            let _ = target_map.insert(
                SBCHash {
                    key: *hash,
                    chunk_type: ChunkType::Delta,
                },
                delta_chunk,
            );
        }
    }
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

pub fn get_chunk_data(target_map: &Box<dyn Database<SBCHash, Vec<u8>>>, hash: u32) -> Vec<u8> {
    if target_map.contains(&SBCHash {
        key: hash,
        chunk_type: ChunkType::Delta,
    }) {
        target_map
            .get(&SBCHash {
                key: hash,
                chunk_type: ChunkType::Delta,
            })
            .unwrap()
    } else {
        target_map
            .get(&SBCHash {
                key: hash,
                chunk_type: ChunkType::Simple,
            })
            .unwrap()
    }
}

fn find_parent_key_in_cluster(
    target_map: &Box<dyn Database<SBCHash, Vec<u8>>>,
    cluster: &[u32],
) -> u32 {
    let mut leader_hash = cluster[0];
    let mut min_sum_dist = u32::MAX;

    for chunk_hash_1 in cluster.iter() {
        let mut sum_dist_for_chunk = 0u32;
        let chunk_data_1 = get_chunk_data(target_map, *chunk_hash_1);

        for chunk_hash_2 in cluster.iter() {
            if *chunk_hash_1 == *chunk_hash_2 {
                continue;
            }

            let chunk_data_2 = get_chunk_data(target_map, *chunk_hash_2);
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

#[cfg(test)]
mod test {
    use crate::chunkfs_sbc::{encode_map, get_chunk_data};
    use crate::graph::Graph;
    use crate::levenshtein_functions;
    use crate::{hash, ChunkType, SBCHash, SBCMap};
    use chunkfs::{Database, ScrubMeasurements};
    use fastcdc::v2016::FastCDC;
    use std::collections::HashMap;
    use std::fs;
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::time::Instant;

    const PATH: &str = "runner/files/test1.txt";

    #[test]
    fn test_data_recovery() -> Result<(), std::io::Error> {
        let contents = fs::read(PATH).unwrap();
        let chunks = FastCDC::new(&contents, 1000, 2000, 65536);
        let input = File::open(PATH)?;
        let mut buffer = BufReader::new(input);
        let mut hashmap_with_data = HashMap::new();
        let mut target_map: Box<dyn Database<SBCHash, Vec<u8>>> = Box::new(SBCMap::new());
        let mut graph = Graph::new();
        let time_start = Instant::now();
        let mut processed_data = 0;
        let mut data_left = 0;
        let mut count_delta_chunk = 0;

        for chunk in chunks {
            let length = chunk.length;
            let mut data = vec![0; length];
            buffer.read_exact(&mut data)?;

            let sbc_hash = hash(data.as_slice());
            hashmap_with_data.insert(sbc_hash, data.clone());
            let (target_hash, parent_hash) = graph.add_vertex(sbc_hash);
            match target_hash.chunk_type {
                ChunkType::Delta => {
                    let parent_chunk_data = target_map.get(&SBCHash { key : parent_hash, chunk_type : ChunkType::Simple}).unwrap();
                    let mut delta_chunk = Vec::new();
                    for byte in parent_hash.to_be_bytes() {
                        delta_chunk.push(byte);
                    }
                    for delta_action in
                        levenshtein_functions::encode(data.as_slice(), parent_chunk_data.as_slice())
                    {
                        for byte in delta_action.to_be_bytes() {
                            delta_chunk.push(byte);
                        }
                    }
                    let _ = target_map.insert(target_hash, delta_chunk);
                    processed_data += data.len();
                }
                ChunkType::Simple => {
                    let _ = target_map.insert(target_hash, data.clone());
                    data_left += data.len();
                }
            }
            // let sbc_hash = hash(bytes.as_slice());
            // datas.insert(sbc_hash, bytes.clone());
            // let _ = target_map.insert(
            //     SBCHash {
            //         key: sbc_hash,
            //         chunk_type: ChunkType::Simple,
            //     },
            //     bytes,
            // );
            // keys.push(sbc_hash);
        }
        //
        // let modified_clusters = graph.update_graph_based_on_the_kraskal_algorithm(keys.as_slice());
        // encode_map(&mut graph, &mut target_map, &modified_clusters);
        //
        // for (key, data) in hashmap_with_data.iter() {
        //     if graph.vertices.get(key).unwrap().parent == *key {
        //         data_left += data.len();
        //     } else {
        //         processed_data += data.len();
        //     }
        // }

        let running_time = time_start.elapsed();
        for (key, data_in_hashmap) in hashmap_with_data {
            if target_map.contains(&SBCHash {
                key,
                chunk_type: ChunkType::Delta,
            }) {
                count_delta_chunk += 1;
            }
            let recover_data = get_chunk_data(&target_map, key);
            assert_eq!(recover_data, data_in_hashmap);
        }
        assert!(count_delta_chunk > 0);

        ScrubMeasurements {
            processed_data,
            running_time,
            data_left,
        };
        Ok(())
    }
}
