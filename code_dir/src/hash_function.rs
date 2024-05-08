use std::collections::HashMap;
use std::ops::Range;

const BLOCKS_IN_C_SPECTRUM_COUNT: usize = 8;
const MIN_SPACE_VALUE: u32 = 1;
const BITS_IN_F_SPECTRUM_BLOCKS_COUNT: u32 = 3;
const BLOCKS_IN_F_SPECTRUM_COUNT: usize = 16;
const SHIFT_FOR_PAIR: u8 = 4;
const BLOCKS_FOR_P_SPECTRUM_INDEXES: Range<usize> = 5..9;

fn processing_of_c_spectrum(c_f_spectrum: &[(&u8, &u32)]) -> u32 {
    let mut spaces_in_c_spectrum = Vec::new();
    for byte_index in 1..c_f_spectrum.len() {
        let frequency_delta =
            (c_f_spectrum[byte_index - 1].1 - c_f_spectrum[byte_index].1) * byte_index as u32;
        if frequency_delta >= MIN_SPACE_VALUE {
            spaces_in_c_spectrum.push((byte_index, frequency_delta));
        }
    }
    spaces_in_c_spectrum.sort_by(|a, b| b.1.cmp(&a.1));

    let mut spaces_in_c_spectrum_indexes = Vec::new();
    for space in spaces_in_c_spectrum.iter().take(std::cmp::min(
        spaces_in_c_spectrum.len(),
        BLOCKS_IN_C_SPECTRUM_COUNT,
    )) {
        spaces_in_c_spectrum_indexes.push(space.0);
    }
    spaces_in_c_spectrum_indexes.sort();

    let mut hash: u32 = 0;

    let mut start_block = 0;
    for (block_number, block) in spaces_in_c_spectrum_indexes.iter().enumerate() {
        let end_block = *block;
        let block = &c_f_spectrum[start_block..end_block];
        let mut block_hash = 0;
        for byte_frequency in block {
            block_hash ^= *byte_frequency.0 as u32;
        }

        block_hash <<= (BLOCKS_IN_C_SPECTRUM_COUNT - block_number) * 3;
        hash ^= block_hash;
        start_block = end_block;
    }
    hash
}

fn find_first_significant_bit(block: u32) -> u32 {
    let mut number = block;
    let mut bit_index = 0;
    while number > 1 {
        number >>= 1;
        bit_index += 1;
    }
    bit_index
}

fn processing_of_f_spectrum(c_f_spectrum: &[(&u8, &u32)]) -> u32 {
    let mut hash: u32 = 0;
    let shifts = [0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 6];

    for block_index in 0..std::cmp::min(c_f_spectrum.len(), BLOCKS_IN_F_SPECTRUM_COUNT) {
        let mut block_hash = *c_f_spectrum[block_index].1;
        block_hash <<= BITS_IN_F_SPECTRUM_BLOCKS_COUNT;
        let significant_bit = find_first_significant_bit(block_hash);
        block_hash >>= significant_bit - BITS_IN_F_SPECTRUM_BLOCKS_COUNT;
        block_hash %= 1 << BITS_IN_F_SPECTRUM_BLOCKS_COUNT;

        block_hash <<= shifts[block_index];
        hash ^= block_hash;
    }

    hash
}

fn processing_of_pair(pair: &(u8, u8)) -> u32 {
    let byte1 = ((pair.0 % (1 << (8 - SHIFT_FOR_PAIR))) << SHIFT_FOR_PAIR)
        + pair.0 / (1 << (8 - SHIFT_FOR_PAIR));
    let byte2 =
        ((pair.1 % (1 << SHIFT_FOR_PAIR)) << (8 - SHIFT_FOR_PAIR)) + pair.1 / (1 << SHIFT_FOR_PAIR);
    (byte1 as u32) << (8 - SHIFT_FOR_PAIR as u32) ^ (byte2 as u32)
}

fn processing_of_p_spectrum(p_spectrum: &[(&(u8, u8), &u32)]) -> u32 {
    let mut hash: u32 = 0;
    for block_index in BLOCKS_FOR_P_SPECTRUM_INDEXES {
        if block_index >= p_spectrum.len() {
            break;
        }
        hash ^= processing_of_pair(p_spectrum[block_index].0) << (16 + SHIFT_FOR_PAIR);
    }

    hash
}

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

    let mut pairs_vec: Vec<(&(u8, u8), &u32)> = pair_value_pair_frequency.iter().collect();
    pairs_vec.sort_by(|a, b| b.1.cmp(a.1));

    let c_hash = processing_of_c_spectrum(bytes_vec.as_slice());
    let f_hash = processing_of_f_spectrum(bytes_vec.as_slice());
    let p_hash = processing_of_p_spectrum(pairs_vec.as_slice());
    let hash = c_hash ^ f_hash ^ p_hash;

    processing_of_pair(pairs_vec[0].0);
    hash
}
