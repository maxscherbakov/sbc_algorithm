use crate::levenshtein_functions::levenshtein_distance;
use crate::{levenshtein_functions, ChunkType, SBCHash, SBCMap};
use chunkfs::{Data, DataContainer, Database};
use std::collections::{HashMap, HashSet};

fn count_delta_chunks_with_hash(target_map: &mut SBCMap, hash: u32) -> u16 {
    let mut count = 0;
    while target_map.contains(&SBCHash {
        key: hash,
        chunk_type: ChunkType::Delta(count),
    }) {
        count += 1
    }
    count
}

fn find_empty_cell(target_map: &SBCMap, hash: u32) -> u32 {
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

fn encode_simple_chunk(target_map: &mut SBCMap, data: &[u8], hash: u32) -> (usize, SBCHash) {
    let sbc_hash = SBCHash {
        key: find_empty_cell(target_map, hash),
        chunk_type: ChunkType::Simple,
    };

    let _ = target_map.insert(sbc_hash.clone(), data.to_vec());
    (data.len(), sbc_hash)
}

fn encode_delta_chunk(
    target_map: &mut SBCMap,
    data: &[u8],
    hash: u32,
    parent_data: &[u8],
    parent_hash: u32,
) -> (usize, usize, SBCHash) {
    let number_delta_chunk = count_delta_chunks_with_hash(target_map, hash);
    let sbc_hash = SBCHash {
        key: hash,
        chunk_type: ChunkType::Delta(number_delta_chunk),
    };
    let mut delta_chunk = Vec::new();
    for byte in parent_hash.to_be_bytes() {
        delta_chunk.push(byte);
    }

    match levenshtein_functions::encode(data, parent_data) {
        None => {
            let (data_left, sbc_hash) = encode_simple_chunk(target_map, data, hash);
            (data_left, 0, sbc_hash)
        }
        Some(delta_code) => {
            for delta_action in delta_code {
                for byte in delta_action.to_be_bytes() {
                    delta_chunk.push(byte);
                }
            }
            let processed_data = delta_chunk.len();
            let _ = target_map.insert(sbc_hash.clone(), delta_chunk);
            (0, processed_data, sbc_hash)
        }
    }
}

fn encode_cluster(
    target_map: &mut SBCMap,
    cluster: &mut [(u32, &mut DataContainer<SBCHash>)],
) -> (usize, usize) {
    let mut data_left = 0;
    let mut processed_data = 0;
    let count_chunks_in_cluster = cluster.len();
    let (parent_id, not_delta_encoded) = (0, Option::<HashSet<usize>>::None); //find_parent_chunk_in_cluster(cluster);
    let (parent_hash, parent_data_container) = &mut cluster[parent_id];
    let parent_data = match parent_data_container.extract() {
        Data::Chunk(data) => data.clone(),
        Data::TargetChunk(_) => {
            panic!()
        }
    };

    if count_chunks_in_cluster > 5 {
        print!("parent hash for cluster: {} ", parent_hash.clone());
        println!("count chunks in cluster {}", count_chunks_in_cluster);
    }
    let (left, parent_sbc_hash) =
        encode_simple_chunk(target_map, parent_data.as_slice(), *parent_hash);
    let parent_hash = parent_sbc_hash.key;
    data_left += left;
    parent_data_container.make_target(vec![parent_sbc_hash]);

    for (chunk_id, (hash, data_container)) in cluster.iter_mut().enumerate() {
        if chunk_id == parent_id {
            continue;
        }
        let mut target_hash = SBCHash::default();
        match data_container.extract() {
            Data::Chunk(data) => {
                if match not_delta_encoded.clone() {
                    None => false,
                    Some(set) => set.contains(&chunk_id),
                } || data.len().abs_diff(parent_data.len()) > 4000
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
                    let (left, processed, sbc_hash) = encode_delta_chunk(
                        target_map,
                        data,
                        *hash,
                        parent_data.as_slice(),
                        parent_hash,
                    );
                    data_left += left;
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

#[allow(dead_code)]
fn find_parent_chunk_in_cluster(
    cluster: &[(u32, &mut DataContainer<SBCHash>)],
) -> (usize, Option<HashSet<usize>>) {
    if cluster.len() == 1 {
        return (0, None);
    }
    let mut min_sum_dist = u32::MAX;
    let mut not_delta_encoded: HashMap<usize, HashSet<usize>> = HashMap::new();
    let mut parent_id = 0;

    for (chunk_id_1, (_, data_container_1)) in cluster.iter().enumerate() {
        match data_container_1.extract() {
            Data::Chunk(data_1) => {
                let mut sum_dist_for_chunk = data_1.len() as u32;
                for (chunk_id_2, (_, data_container_2)) in cluster.iter().enumerate() {
                    match data_container_2.extract() {
                        Data::Chunk(data_2) => {
                            if chunk_id_1 == chunk_id_2 {
                                continue;
                            }
                            if data_1.len().abs_diff(data_2.len()) > 4000 {
                                let not_delta_encode_hashes =
                                    not_delta_encoded.entry(chunk_id_1).or_default();
                                not_delta_encode_hashes.insert(chunk_id_2);
                                sum_dist_for_chunk += data_2.len() as u32;
                            } else {
                                let levenshtein_dist = levenshtein_distance(
                                    (*data_1).as_slice(),
                                    (*data_2).as_slice(),
                                );
                                if levenshtein_dist * 4 >= data_1.len() as u32 {
                                    let not_delta_encode_hashes =
                                        not_delta_encoded.entry(chunk_id_1).or_default();
                                    not_delta_encode_hashes.insert(chunk_id_2);
                                    sum_dist_for_chunk += data_2.len() as u32;
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
                    parent_id = chunk_id_1;
                }
            }
            Data::TargetChunk(_) => {}
        }
    }
    (parent_id, not_delta_encoded.get(&parent_id).cloned())
}

pub(crate) fn encode_clusters(
    clusters: &mut HashMap<u32, Vec<(u32, &mut DataContainer<SBCHash>)>>,
    target_map: &mut SBCMap,
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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_restore_similarity_chunk_1_byte_diff() {
        let mut data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data.clone();
        if data[15] < 255 {
            data[15] = 255;
        } else {
            data[15] = 0;
        }
        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );

        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }
    #[test]
    fn test_restore_similarity_chunk_2_neighbor_byte_diff() {
        let mut data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data.clone();
        if data[15] < 255 {
            data[15] = 255;
        } else {
            data[15] = 0;
        }
        if data[16] < 255 {
            data[16] = 255;
        } else {
            data[16] = 0;
        }
        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );

        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }

    #[test]
    fn test_restore_similarity_chunk_2_byte_diff() {
        let mut data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data.clone();
        if data[15] < 255 {
            data[15] = 255;
        } else {
            data[15] = 0;
        }
        if data[106] < 255 {
            data[106] = 255;
        } else {
            data[106] = 0;
        }
        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );

        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset_left() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data[15..].to_vec();
        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );

        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset_right() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data[..8000].to_vec();
        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );

        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let mut data2 = data[15..8000].to_vec();
        data2[0] = data2[0] / 3;
        data2[7000] = data2[7000] / 3;

        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );

        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }

    #[test]
    fn test_restore_similarity_chunk_with_cyclic_shift_right() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let mut data2 = data.clone();
        data2.extend(&data[8000..]);

        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );
        assert_ne!(data, []);
        assert_eq!(sbc_hash_2.chunk_type, ChunkType::Delta(0));
        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }
    #[test]
    fn test_restore_similarity_chunk_with_cyclic_shift_left() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let mut data2 = data[..192].to_vec();
        data2.extend(&data);

        let mut sbc_map = SBCMap::new();

        let (_, sbc_hash) = encode_simple_chunk(&mut sbc_map, data.as_slice(), 0);
        let (_, _, sbc_hash_2) = encode_delta_chunk(
            &mut sbc_map,
            data2.as_slice(),
            3,
            data.as_slice(),
            sbc_hash.key,
        );
        assert_ne!(data, []);
        assert_eq!(sbc_hash_2.chunk_type, ChunkType::Delta(0));
        assert_eq!(sbc_map.get(&sbc_hash_2).unwrap(), data2)
    }

}
