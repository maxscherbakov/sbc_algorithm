use crate::chunkfs_sbc::ClusterPoint;
use crate::decoder::Decoder;
use crate::encoder::{count_delta_chunks_with_hash, encode_simple_chunk, get_parent_data, Encoder};
use crate::{ChunkType, SBCHash, SBCKey, SBCMap};
use chunkfs::{Data, Database};
use std::cmp::min;
use std::sync::{Arc, Mutex};

pub(crate) enum Action {
    Del,
    Add,
    Rep,
}

pub struct LevenshteinEncoder;

impl LevenshteinEncoder {
    fn encode_delta_chunk<D: Decoder, Hash: SBCHash>(
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        data: &[u8],
        hash: Hash,
        parent_data: &[u8],
        parent_hash: Hash,
    ) -> (usize, usize, SBCKey<Hash>) {
        let mut delta_chunk = Vec::new();

        match encode(data, parent_data) {
            None => {
                let (data_left, sbc_hash) =
                    encode_simple_chunk(&mut target_map.clone().lock().unwrap(), data, hash);
                (data_left, 0, sbc_hash)
            }
            Some(delta_code) => {
                for delta_action in delta_code {
                    for byte in delta_action.to_be_bytes() {
                        delta_chunk.push(byte);
                    }
                }
                let processed_data = delta_chunk.len();

                let mut target_map_lock = target_map.lock().unwrap();

                let number_delta_chunk = count_delta_chunks_with_hash(&target_map_lock, &hash);
                let sbc_hash = SBCKey {
                    hash,
                    chunk_type: ChunkType::Delta {
                        parent_hash,
                        number: number_delta_chunk,
                    },
                };
                let _ = target_map_lock.insert(sbc_hash.clone(), delta_chunk);
                (0, processed_data, sbc_hash)
            }
        }
    }
}

impl Encoder for LevenshteinEncoder {
    fn encode_cluster<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        cluster: &mut [ClusterPoint<Hash>],
        parent_hash: Hash,
    ) -> (usize, usize) {
        let mut processed_data = 0;
        let parent_chunk = get_parent_data(target_map.clone(), parent_hash.clone(), cluster);
        let mut data_left = parent_chunk.data_left;
        for (chunk_id, (hash, data_container)) in cluster.iter_mut().enumerate() {
            if parent_chunk.index > -1 && chunk_id == parent_chunk.index as usize {
                continue;
            }
            let mut target_hash = SBCKey::default();
            match data_container.extract() {
                Data::Chunk(data) => {
                    if data.len().abs_diff(parent_chunk.parent_data.len()) > 4000 {
                        let (left, sbc_hash) = encode_simple_chunk(
                            &mut target_map.clone().lock().unwrap(),
                            data,
                            hash.clone(),
                        );
                        data_left += left;
                        target_hash = sbc_hash;
                    } else {
                        let (left, processed, sbc_hash) = Self::encode_delta_chunk(
                            target_map.clone(),
                            data,
                            hash.clone(),
                            parent_chunk.parent_data.as_slice(),
                            parent_hash.clone(),
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
}

fn find_id_non_eq_byte(data_chunk: &[u8], data_chunk_parent: &[u8]) -> (usize, usize) {
    let mut id_non_eq_byte_start = 0;
    while data_chunk[id_non_eq_byte_start] == data_chunk_parent[id_non_eq_byte_start] {
        id_non_eq_byte_start += 1;
        if id_non_eq_byte_start == min(data_chunk_parent.len(), data_chunk.len()) {
            break;
        }
    }
    let mut id_non_eq_byte_end = 0;
    if !((data_chunk.len() <= id_non_eq_byte_start)
        | (data_chunk_parent.len() <= id_non_eq_byte_start))
    {
        while data_chunk[data_chunk.len() - id_non_eq_byte_end - 1]
            == data_chunk_parent[data_chunk_parent.len() - id_non_eq_byte_end - 1]
        {
            id_non_eq_byte_end += 1;
            if min(data_chunk.len(), data_chunk_parent.len()) - id_non_eq_byte_end
                == id_non_eq_byte_start
            {
                break;
            }
        }
    }
    (id_non_eq_byte_start, id_non_eq_byte_end)
}

fn encode(data_chunk: &[u8], data_chunk_parent: &[u8]) -> Option<Vec<u32>> {
    let max_len_delta_code = data_chunk.len() as u32;
    let mut delta_code = Vec::new();
    let (id_non_eq_byte_start, id_non_eq_byte_end) =
        find_id_non_eq_byte(data_chunk, data_chunk_parent);

    let data_chunk =
        data_chunk[id_non_eq_byte_start..data_chunk.len() - id_non_eq_byte_end].to_vec();
    let data_chunk_parent = data_chunk_parent
        [id_non_eq_byte_start..data_chunk_parent.len() - id_non_eq_byte_end]
        .to_vec();

    let matrix = levenshtein_matrix(data_chunk.as_slice(), data_chunk_parent.as_slice());

    if matrix[matrix.len() - 1][matrix[0].len() - 1] * 4 + 4 > max_len_delta_code {
        return None;
    }
    let mut x = matrix[0].len() - 1;
    let mut y = matrix.len() - 1;
    while x > 0 || y > 0 {
        if x > 0
            && y > 0
            && (data_chunk_parent[y - 1] != data_chunk[x - 1])
            && (matrix[y - 1][x - 1] < matrix[y][x])
        {
            delta_code.push(encode_delta_action(
                Action::Rep,
                id_non_eq_byte_start + y - 1,
                data_chunk[x - 1],
            ));
            x -= 1;
            y -= 1;
        } else if y > 0 && matrix[y - 1][x] < matrix[y][x] {
            delta_code.push(encode_delta_action(
                Action::Del,
                id_non_eq_byte_start + y - 1,
                0,
            ));
            y -= 1;
        } else if x > 0 && matrix[y][x - 1] < matrix[y][x] {
            delta_code.push(encode_delta_action(
                Action::Add,
                id_non_eq_byte_start + y,
                data_chunk[x - 1],
            ));
            x -= 1;
        } else {
            x -= 1;
            y -= 1;
        }
    }
    Some(delta_code)
}

#[allow(dead_code)]
pub(crate) fn levenshtein_distance(data_chunk: &[u8], data_chunk_parent: &[u8]) -> u32 {
    let mut id_eq_byte = 0;
    while data_chunk[id_eq_byte] == data_chunk_parent[id_eq_byte] {
        if id_eq_byte == min(data_chunk_parent.len(), data_chunk.len()) - 1 {
            break;
        }
        id_eq_byte += 1;
    }
    let levenshtein_matrix =
        levenshtein_matrix(&data_chunk[id_eq_byte..], &data_chunk_parent[id_eq_byte..]);
    levenshtein_matrix[data_chunk_parent.len()][data_chunk.len()]
}

fn levenshtein_matrix(data_chunk: &[u8], data_chunk_parent: &[u8]) -> Vec<Vec<u32>> {
    let mut levenshtein_matrix =
        vec![vec![0u32; data_chunk.len() + 1]; data_chunk_parent.len() + 1];
    levenshtein_matrix[0] = (0..data_chunk.len() as u32 + 1).collect();
    for y in 1..data_chunk_parent.len() + 1 {
        levenshtein_matrix[y][0] = y as u32;
        for x in 1..data_chunk.len() + 1 {
            let add = levenshtein_matrix[y - 1][x] + 1;
            let del = levenshtein_matrix[y][x - 1] + 1;
            let mut replace = levenshtein_matrix[y - 1][x - 1];
            if data_chunk_parent[y - 1] != data_chunk[x - 1] {
                replace += 1;
            }
            levenshtein_matrix[y][x] = min(min(del, add), replace);
        }
    }
    levenshtein_matrix
}

fn encode_delta_action(action: Action, index: usize, byte_value: u8) -> u32 {
    let mut code = 0u32;
    match action {
        Action::Del => {
            code += 1 << 31;
        }
        Action::Add => {
            code += 1 << 30;
        }
        Action::Rep => {}
    }
    code += byte_value as u32 * (1 << 22);
    if index >= (1 << 22) {
        panic!()
    }
    code += index as u32;
    code
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AronovichHash, decoder};
    use crate::SBCHash::Aronovich;

    #[test]
    fn test_restore_similarity_chunk_1_byte_diff() {
        let mut data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data.clone();
        if data[15] < 255 {
            data[15] = 255;
        } else {
            data[15] = 0;
        }

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
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

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
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

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset_left() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data[15..].to_vec();

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset_right() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let data2 = data[..8000].to_vec();

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_offset() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let mut data2 = data[15..8000].to_vec();
        data2[0] /= 3;
        data2[7000] /= 3;

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    #[test]
    fn test_restore_similarity_chunk_with_cyclic_shift_right() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let mut data2 = data.clone();
        data2.extend(&data[8000..]);

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_ne!(data, []);
        assert_eq!(
            sbc_key.chunk_type,
            ChunkType::Delta {
                parent_hash: AronovichHash::new(0),
                number: 0
            }
        );
        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }
    #[test]
    fn test_restore_similarity_chunk_with_cyclic_shift_left() {
        let data: Vec<u8> = (0..8192).map(|_| rand::random::<u8>()).collect();
        let mut data2 = data[..192].to_vec();
        data2.extend(&data);

        let (sbc_map, sbc_key) = create_map_and_key(data.as_slice(), data2.as_slice());

        assert_ne!(data, []);
        assert_eq!(
            sbc_key.chunk_type,
            ChunkType::Delta {
                parent_hash: AronovichHash::new(0),
                number: 0
            }
        );
        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }
    fn create_map_and_key<'a>(
        data: &'a [u8],
        data2: &'a [u8],
    ) -> (SBCMap<decoder::LevenshteinDecoder, AronovichHash>, SBCKey<AronovichHash>) {
        let mut binding = SBCMap::new(decoder::LevenshteinDecoder);
        let sbc_map = Arc::new(Mutex::new(&mut binding));

        let (_, sbc_key) = encode_simple_chunk(&mut sbc_map.lock().unwrap(), data, AronovichHash::new(0));
        let (_, _, sbc_key_2) = LevenshteinEncoder::encode_delta_chunk(
            sbc_map.clone(),
            data2,
            AronovichHash::new(3),
            data,
            sbc_key.hash.clone(),
        );
        (binding, sbc_key_2)
    }
}
