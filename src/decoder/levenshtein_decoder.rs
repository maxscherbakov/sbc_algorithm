use crate::decoder::Decoder;
use crate::encoder::Action;

/// Decoder based on Levenshtein compression algorithm.
pub struct LevenshteinDecoder;

impl Decoder for LevenshteinDecoder {
    fn decode_chunk(&self, mut parent_data: Vec<u8>, delta_code: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; 4];
        let mut byte_index = 0;

        while byte_index < delta_code.len() {
            buf.copy_from_slice(&delta_code[byte_index..byte_index + 4]);
            let delta_action = u32::from_be_bytes(buf);

            let (action, index, byte_value) = get_delta_action(delta_action);
            match action {
                Action::Del => {
                    parent_data.remove(index);
                }
                Action::Add => parent_data.insert(index, byte_value),
                Action::Rep => parent_data[index] = byte_value,
            }
            byte_index += 4;
        }
        parent_data
    }
}

pub(crate) fn get_delta_action(code: u32) -> (Action, usize, u8) {
    let action = match code / (1 << 30) {
        0 => Action::Rep,
        1 => Action::Add,
        2 => Action::Del,
        _ => panic!(),
    };
    let byte_value = code % (1 << 30) / (1 << 22);
    let index = code % (1 << 22);
    (action, index as usize, byte_value as u8)
}
