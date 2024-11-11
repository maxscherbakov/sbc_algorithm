use crate::levenshtein_functions::levenshtein_distance;
use crate::{levenshtein_functions, ChunkType, SBCHash};
use chunkfs::{Data, DataContainer, Database};
use std::collections::{HashMap, HashSet};

fn count_delta_chunks_with_hash(
    target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
    hash: u32,
) -> u8 {
    let mut count = 0;
    while target_map.contains(&SBCHash {
        key: hash,
        chunk_type: ChunkType::Delta(count),
    }) {
        count += 1
    }
    count
}

fn find_empty_cell(target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>, hash: u32) -> u32 {
    let mut left = hash;
    let mut right = hash + 1;
    loop {
        if target_map.contains(&SBCHash {
            key: left,
            chunk_type: ChunkType::Simple,
        }) {
            left = left.saturating_sub(1);
        } else {
            return left;
        }
        if target_map.contains(&SBCHash {
            key: right,
            chunk_type: ChunkType::Simple,
        }) {
            right = right.saturating_add(1);
        } else {
            return right;
        }
    }
}

fn encode_simple_chunk(
    target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
    data: &[u8],
    hash: u32,
) -> (usize, SBCHash) {
    let mut sbc_hash = SBCHash {
        key: hash,
        chunk_type: ChunkType::Simple,
    };
    if !target_map.contains(&sbc_hash){
        sbc_hash.key = find_empty_cell(target_map, hash);
        let _ = target_map.insert(sbc_hash.clone(), data.to_vec());
        (data.len(), sbc_hash)
    } else if target_map.get(&sbc_hash).unwrap().as_slice() == data {
        (0, sbc_hash)
    } else {
        sbc_hash.key = find_empty_cell(target_map, hash);
        let _ = target_map.insert(sbc_hash.clone(), data.to_vec());
        (data.len(), sbc_hash)
    }
}

fn encode_delta_chunk(
    target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
    data: &[u8],
    hash: u32,
    parent_data: &[u8],
    parent_hash: u32,
) -> (usize, SBCHash) {
    let number_delta_chunk = count_delta_chunks_with_hash(target_map, hash);
    let sbc_hash = SBCHash {
        key: hash,
        chunk_type: ChunkType::Delta(number_delta_chunk),
    };
    let mut delta_chunk = Vec::new();
    for byte in parent_hash.to_be_bytes() {
        delta_chunk.push(byte);
    }
    for delta_action in levenshtein_functions::encode(data, parent_data) {
        for byte in delta_action.to_be_bytes() {
            delta_chunk.push(byte);
        }
    }
    let processed_data = delta_chunk.len();
    let _ = target_map.insert(sbc_hash.clone(), delta_chunk);
    (processed_data, sbc_hash)
}

fn encode_cluster(
    target_map: &mut Box<dyn Database<SBCHash, Vec<u8>>>,
    cluster: &mut [(u32, &mut DataContainer<SBCHash>)],
) -> (usize, usize) {
    let (parent_hash, parent_data, not_delta_encoded) = find_parent_chunk_in_cluster(cluster);
    let mut data_left = 0;
    let mut processed_data = 0;
    for (hash, data_container) in cluster.iter_mut() {
        let mut target_hash = SBCHash::default();
        match data_container.extract() {
            Data::Chunk(data) => {
                if *hash == parent_hash
                    || match not_delta_encoded.clone() {
                        None => false,
                        Some(set) => set.contains(hash),
                    }
                {
                    let (left, sbc_hash) = encode_simple_chunk(target_map, data, *hash);
                    data_left += left;
                    target_hash = sbc_hash;
                } else {
                    println!(
                        "len1: {}; len2: {}, hash: {}; parent_hash: {}",
                        data.len(),
                        parent_data.len(),
                        hash,
                        parent_hash
                    );
                    let (processed, sbc_hash) = encode_delta_chunk(
                        target_map,
                        data,
                        *hash,
                        parent_data.as_slice(),
                        parent_hash,
                    );
                    processed_data += processed;
                    target_hash = sbc_hash;
                }
            }
            Data::TargetChunk(_) => {}
        }
        data_container.make_target(vec![target_hash]);
    }
    (data_left, processed_data)
}

fn find_parent_chunk_in_cluster(
    cluster: &[(u32, &mut DataContainer<SBCHash>)],
) -> (u32, Vec<u8>, Option<HashSet<u32>>) {
    let mut min_sum_dist = u32::MAX;
    let mut not_delta_encoded: HashMap<u32, HashSet<u32>> = HashMap::new();
    let mut parent_hash = cluster[0].0;
    let mut parent_data = match cluster[0].1.extract() {
        Data::Chunk(data) => data,
        Data::TargetChunk(_) => panic!(),
    };

    for (hash_1, data_container_1) in cluster.iter() {
        let mut sum_dist_for_chunk = 0u32;
        match data_container_1.extract() {
            Data::Chunk(data_1) => {
                for (hash_2, data_container_2) in cluster.iter() {
                    match data_container_2.extract() {
                        Data::Chunk(data_2) => {
                            if *hash_1 == *hash_2 && data_1 == data_2 {
                                continue;
                            }
                            if data_1.len().abs_diff(data_2.len()) > 4000
                                || data_1.len() * data_2.len() > 256 * (1 << 20)
                            {
                                let not_delta_encode_hashes =
                                    not_delta_encoded.entry(*hash_1).or_default();
                                not_delta_encode_hashes.insert(*hash_2);
                            } else {
                                let levenshtein_dist = levenshtein_distance(
                                    (*data_1).as_slice(),
                                    (*data_2).as_slice(),
                                );
                                if levenshtein_dist * 4 >= data_1.len() as u32 {
                                    let not_delta_encode_hashes =
                                        not_delta_encoded.entry(*hash_1).or_default();
                                    not_delta_encode_hashes.insert(*hash_2);
                                } else {
                                    sum_dist_for_chunk += levenshtein_dist;
                                }
                            }
                        }
                        Data::TargetChunk(_) => {}
                    }
                }
                if sum_dist_for_chunk < min_sum_dist {
                    min_sum_dist = sum_dist_for_chunk;
                    parent_hash = *hash_1;
                    parent_data = data_1;
                }
            }
            Data::TargetChunk(_) => {}
        }
    }
    (
        parent_hash,
        parent_data.clone(),
        not_delta_encoded.get(&parent_hash).cloned(),
    )
}

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
