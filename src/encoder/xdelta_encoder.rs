use crate::chunkfs_sbc::ClusterPoint;
use crate::decoder::Decoder;
use crate::encoder::{count_delta_chunks_with_hash, get_parent_data, Encoder};
use crate::{ChunkType, SBCHash, SBCKey, SBCMap};
use chunkfs::Data;
use chunkfs::Database;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use zstd::stream;

const BLOCK_SIZE: usize = 16;
const ADLER_MOD: u32 = 65521;

pub struct XdeltaEncoder {
    zstd_flag: bool,
}

impl XdeltaEncoder {
    pub fn new(zstd_flag: bool) -> Self {
        XdeltaEncoder { zstd_flag }
    }
}

impl Default for XdeltaEncoder {
    fn default() -> Self {
        Self::new(false)
    }
}

impl XdeltaEncoder {
    fn encode_delta_chunk<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        chunk_data: &[u8],
        hash: Hash,
        parent_data: &[u8],
        word_hash_offsets: &HashMap<u32, usize>,
        parent_hash: Hash,
    ) -> (usize, usize, SBCKey<Hash>) {
        let mut delta_code = Vec::new();

        let mut i = 0;
        while i < chunk_data.len() - BLOCK_SIZE + 1 {
            let mut adler_hash_word = adler32(&chunk_data[i..i + BLOCK_SIZE]);

            if !word_hash_offsets.contains_key(&adler_hash_word) {
                let mut insert_data_len = 0usize;
                let mut insert_data = Vec::new();
                while !word_hash_offsets.contains_key(&adler_hash_word) {
                    insert_data_len += 1;
                    insert_data.push(chunk_data[i]);
                    i += 1;
                    if i <= chunk_data.len() - BLOCK_SIZE {
                        adler_hash_word = adler32(&chunk_data[i..i + BLOCK_SIZE]);
                    } else {
                        insert_data.extend_from_slice(&chunk_data[i..i - 1 + BLOCK_SIZE]);
                        insert_data_len += BLOCK_SIZE - 1;
                        i = chunk_data.len();
                        break;
                    }
                }

                // Insert instruction
                let insert_instruction = &mut insert_data_len.to_ne_bytes()[..3];
                insert_instruction[2] += 1 << 7;
                delta_code.extend_from_slice(insert_instruction);
                delta_code.extend_from_slice(&insert_data);
            } else {
                let offset = *word_hash_offsets.get(&adler_hash_word).unwrap();
                let mut equal_part_len = 0;
                let max_len = min(parent_data.len() - offset, chunk_data.len() - i);

                while equal_part_len < max_len
                    && parent_data[offset + equal_part_len] == chunk_data[i + equal_part_len]
                {
                    equal_part_len += 1;
                }

                // Copy instruction
                let copy_instruction_len = &equal_part_len.to_ne_bytes()[..3];
                let copy_instruction_offset = &offset.to_ne_bytes()[..3];
                delta_code.extend_from_slice(copy_instruction_len);
                delta_code.extend_from_slice(copy_instruction_offset);
                i += equal_part_len;
            }
        }
        if i < chunk_data.len() {
            let insert_instruction = &mut (chunk_data.len() - i).to_ne_bytes()[..3];
            insert_instruction[2] += 1 << 7;
            delta_code.extend_from_slice(insert_instruction);
            delta_code.extend_from_slice(&chunk_data[i..chunk_data.len()]);
        }

        let mut target_map_lock = target_map.lock().unwrap();
        let number_delta_chunk = count_delta_chunks_with_hash(&target_map_lock, &hash);
        let sbc_hash = SBCKey {
            hash,
            chunk_type: ChunkType::Delta {
                parent_hash,
                number: number_delta_chunk,
            },
        };

        if self.zstd_flag {
            delta_code = stream::encode_all(delta_code.as_slice(), 0).unwrap();
        }
        let processed_data = delta_code.len();
        let _ = target_map_lock.insert(sbc_hash.clone(), delta_code);
        (0, processed_data, sbc_hash)
    }
}
impl Encoder for XdeltaEncoder {
    fn encode_cluster<D: Decoder, Hash: SBCHash>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D, Hash>>>,
        cluster: &mut [ClusterPoint<Hash>],
        parent_hash: Hash,
    ) -> (usize, usize) {
        let mut processed_data = 0;
        let parent_chunk = get_parent_data(target_map.clone(), parent_hash.clone(), cluster);
        let mut data_left = parent_chunk.data_left;
        let parent_data = parent_chunk.parent_data;
        let word_hash_offsets = init_match(parent_data.as_slice());

        for (chunk_id, (hash, data_container)) in cluster.iter_mut().enumerate() {
            if parent_chunk.index > -1 && chunk_id == parent_chunk.index as usize {
                continue;
            }
            let mut target_hash = SBCKey::default();
            match data_container.extract() {
                Data::Chunk(data) => {
                    let (left, processed, sbc_hash) = self.encode_delta_chunk(
                        target_map.clone(),
                        data,
                        hash.clone(),
                        parent_data.as_slice(),
                        &word_hash_offsets,
                        parent_hash.clone(),
                    );
                    data_left += left;
                    processed_data += processed;
                    target_hash = sbc_hash;
                }
                Data::TargetChunk(_) => {}
            }
            data_container.make_target(vec![target_hash]);
        }
        (data_left, processed_data)
    }
}

/// Computes the Adler-32 checksum for a given byte slice.
///
/// # Parameters
///
/// * `data` - Byte slice to compute checksum for.
///
/// # Returns
///
/// 32-bit Adler-32 checksum value.
fn adler32(data: &[u8]) -> u32 {
    let mut a: u32 = 1;
    let mut b: u32 = 0;

    for &byte in data {
        a = (a + byte as u32) % ADLER_MOD;
        b = (b + a) % ADLER_MOD;
    }

    (b << 16) | a
}

// Инициализация сопоставления строк (создание хеш-таблицы)
fn init_match(src: &[u8]) -> HashMap<u32, usize> {
    let mut i = 0;
    let mut sindex = HashMap::new();

    while i + BLOCK_SIZE <= src.len() {
        let f = adler32(&src[i..i + BLOCK_SIZE]);
        sindex.insert(f, i);
        i += BLOCK_SIZE;
    }

    sindex
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::decoder;
    use crate::encoder::encode_simple_chunk;
    use crate::hasher::AronovichHash;

    #[test]
    fn test_insert_instruction() {
        let chunk_data = vec![10; 20];
        let mut delta_code = Vec::new();

        let mut i = 0;
        while i < chunk_data.len() - BLOCK_SIZE + 1 {
            let mut adler_hash_word = adler32(&chunk_data.as_slice()[i..i + BLOCK_SIZE]);
            let word_hash_offsets: HashMap<u32, usize> = HashMap::new();
            let mut insert_data_len = 0usize;
            let mut insert_data = Vec::new();
            while !word_hash_offsets.contains_key(&adler_hash_word) {
                insert_data_len += 1;
                insert_data.push(chunk_data[i]);
                i += 1;
                if i < chunk_data.len() - BLOCK_SIZE + 1 {
                    adler_hash_word = adler32(&chunk_data.as_slice()[i..i + BLOCK_SIZE]);
                } else {
                    insert_data.extend_from_slice(&chunk_data[i..i + BLOCK_SIZE - 1]);
                    insert_data_len += BLOCK_SIZE - 1;
                    break;
                }
            }

            // Insert instruction
            let insert_instruction = &mut insert_data_len.to_ne_bytes()[..3];
            insert_instruction[2] += 1 << 7;
            delta_code.extend_from_slice(insert_instruction);
            delta_code.extend_from_slice(&insert_data);
        }
        let mut assert_delta_code = vec![20, 0, 128];
        assert_delta_code.extend_from_slice(chunk_data.as_slice());
        assert_eq!(delta_code, assert_delta_code);
    }

    #[test]
    fn test_copy_instruction() {
        let parent_data = vec![10; 20];
        let chunk_data = vec![10; 20];
        let word_hash_offsets = init_match(parent_data.as_slice());

        let mut delta_code = Vec::new();
        let mut i = 0;
        while i < chunk_data.len() - BLOCK_SIZE + 1 {
            let adler_hash_word = adler32(&chunk_data.as_slice()[i..i + BLOCK_SIZE]);
            let offset = *word_hash_offsets.get(&adler_hash_word).unwrap();
            let mut equal_part_len = 0;
            let max_len = min(parent_data.len() - offset, chunk_data.len() - i);

            while equal_part_len < max_len
                && parent_data[offset + equal_part_len] == chunk_data[i + equal_part_len]
            {
                equal_part_len += 1;
            }

            // Copy instruction
            let copy_instruction_len = &equal_part_len.to_ne_bytes()[..3];
            let copy_instruction_offset = &offset.to_ne_bytes()[..3];
            delta_code.extend_from_slice(copy_instruction_len);
            delta_code.extend_from_slice(copy_instruction_offset);
            i += equal_part_len;
        }
        let assert_delta_code = vec![20, 0, 0, 0, 0, 0];
        assert_eq!(delta_code, assert_delta_code);
    }

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
                parent_hash: AronovichHash::new_with_u32(0),
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
                parent_hash: AronovichHash::new_with_u32(0),
                number: 0
            }
        );
        assert_eq!(sbc_map.get(&sbc_key).unwrap(), data2);
    }

    fn create_map_and_key<'a>(
        data: &'a [u8],
        data2: &'a [u8],
    ) -> (
        SBCMap<decoder::GdeltaDecoder, AronovichHash>,
        SBCKey<AronovichHash>,
    ) {
        let word_hash_offsets = init_match(data);
        let mut binding = SBCMap::new(decoder::GdeltaDecoder::default());
        let sbc_map = Arc::new(Mutex::new(&mut binding));

        let (_, sbc_key) = encode_simple_chunk(
            &mut sbc_map.lock().unwrap(),
            data,
            AronovichHash::new_with_u32(0),
        );
        let (_, _, sbc_key_2) = XdeltaEncoder::default().encode_delta_chunk(
            sbc_map.clone(),
            data2,
            AronovichHash::new_with_u32(3),
            data,
            &word_hash_offsets,
            sbc_key.hash.clone(),
        );
        (binding, sbc_key_2)
    }
}
