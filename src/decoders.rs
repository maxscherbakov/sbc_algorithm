use crate::levenshtein_functions::{get_delta_action, Action};

pub trait Decoder {
    fn decode_chunk(&self, parent_data: Vec<u8>, delta_code: &[u8]) -> Vec<u8>;
}

pub struct LevenshteinDecoder;

impl Decoder for LevenshteinDecoder {
    fn decode_chunk(&self, mut parent_data: Vec<u8>, delta_code: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; 4];
        let mut byte_index = 4;

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

pub struct GdeltaDecoder;
impl Decoder for GdeltaDecoder {
    fn decode_chunk(&self, parent_data: Vec<u8>, delta_code: &[u8]) -> Vec<u8> {
        let mut chunk_data = Vec::new();
        let mut byte_id = 4;
        while byte_id < delta_code.len() {
            let mut buf = [0u8; 8];
            buf[..3].copy_from_slice(&delta_code[byte_id..byte_id + 3]);
            if buf[2] >= 128 {
                buf[2] -= 128;
                let insert_len = usize::from_ne_bytes(buf);
                chunk_data.extend_from_slice(&delta_code[byte_id + 3..byte_id + 3 + insert_len]);
                byte_id += 3 + insert_len
            } else {
                let copy_len = usize::from_ne_bytes(buf);
                buf[..3].copy_from_slice(&delta_code[byte_id + 3..byte_id + 6]);
                let copy_offset = usize::from_ne_bytes(buf);
                chunk_data.extend_from_slice(&parent_data[copy_offset..copy_offset + copy_len]);
                byte_id += 6
            }
        }
        chunk_data
    }
}
