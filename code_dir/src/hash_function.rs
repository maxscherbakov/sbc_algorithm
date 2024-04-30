use std::collections::HashMap;

const BLOCKS_IN_C_SPECTRUM_COUNT : usize = 8;

pub fn hash(data: &[u8]) -> u32 {
    let mut byte_value_byte_frequency = HashMap::new();
    let mut pair_value_pair_frequency = HashMap::new();
    let mut last_byte = data[0];
    byte_value_byte_frequency.insert(last_byte, 1u32);
    for byte in &data[1..] {
        let byte_count = byte_value_byte_frequency.entry(*byte).or_insert(0);
        *byte_count += 1;

        let pair_count = pair_value_pair_frequency
            .entry((last_byte, *byte))
            .or_insert(0u32);
        *pair_count += 1;
        last_byte = *byte;
    }

    let mut bytes_vec: Vec<(&u8, &u32)> = byte_value_byte_frequency.iter().collect();
    bytes_vec.sort_by(|a, b| b.1.cmp(a.1));

    let mut pairs_vec: Vec<(&u8, &u32)> = byte_value_byte_frequency.iter().collect();
    pairs_vec.sort_by(|a, b| b.1.cmp(a.1));

    let mut spaces = Vec::new();
    for byte_index in 1..bytes_vec.len() {
        let frequency_delta = (bytes_vec[byte_index - 1].1 - bytes_vec[byte_index].1) * byte_index as u32;
        spaces.push((byte_index, frequency_delta));
    }
    spaces.sort_by(|a, b| b.1.cmp(&a.1));

    let mut space_indexes = Vec::new();
    for space_index in 0..std::cmp::min(spaces.len(), BLOCKS_IN_C_SPECTRUM_COUNT) {
        space_indexes.push(spaces[space_index].0);
    }
    space_indexes.sort();


    let mut hash : u32 = 0;

    let mut start_block = 0;
    for block_number in 0..space_indexes.len() {
        let end_block = space_indexes[block_number];
        let block = &bytes_vec[start_block..end_block];
        let mut byte = *block[0].0;
        for byte_index in 1..block.len() {
            byte ^= block[byte_index].0;
        }
        let mut block_value = byte as u32;
        block_value <<= (BLOCKS_IN_C_SPECTRUM_COUNT - block_number) * 3;
        hash ^= block_value;
        start_block = end_block;
    }

    return hash;
}
