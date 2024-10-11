use crate::levenshtein_functions::levenshtein_distance;
use crate::{levenshtein_functions, ChunkType, SBCHash};
use chunkfs::{Data, DataContainer, Database};
use std::collections::HashMap;

pub(crate) fn encode_clusters(
    clusters: &mut HashMap<u32, Vec<(u32, &mut DataContainer<SBCHash>)>>,
    target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
) -> (usize, usize) {
    let mut data_left = 0;
    let mut processed_data = 0;
    for (_, cluster) in clusters.iter_mut() {
        let data_analyse = encode_cluster(target_map, cluster.as_mut_slice());
        data_left += data_analyse.0;
        processed_data += data_analyse.1;
    }
    (data_left, processed_data)
}

fn encode_cluster(
    target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
    cluster: &mut [(u32, &mut DataContainer<SBCHash>)],
) -> (usize, usize) {
    let (parent_hash, parent_data) = find_parent_key_in_cluster(cluster);
    let mut data_left = 0;
    let mut processed_data = 0;
    let mut target_hash = SBCHash::default();
    for (hash, data_container) in cluster.iter_mut() {
        match data_container.extract() {
            Data::Chunk(data) => {
                if *hash == parent_hash {
                    target_hash = SBCHash {
                        key: *hash,
                        chunk_type: ChunkType::Simple,
                    };
                    let _ = target_map.insert(target_hash.clone(), data.clone());
                    data_left += data.len();
                } else {
                    target_hash = SBCHash {
                        key: *hash,
                        chunk_type: ChunkType::Delta,
                    };
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
        data_container.make_target(vec![target_hash.clone()]);
    }
    (data_left, processed_data)
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
                Data::Chunk(data_1) => match data_container_2.extract() {
                    Data::Chunk(data_2) => {
                        sum_dist_for_chunk +=
                            levenshtein_distance((*data_1).as_slice(), (*data_2).as_slice());
                    }
                    Data::TargetChunk(_) => {}
                },
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
    (leader_hash, leader_data)
}
