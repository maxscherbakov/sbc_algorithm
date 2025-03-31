use crate::decoder::Decoder;

/// Decoder based on Gdelta compression algorithm.
pub struct GdeltaDecoder;

impl Decoder for GdeltaDecoder {
    fn decode_chunk(&self, parent_data: Vec<u8>, delta_code: &[u8]) -> Vec<u8> {
        let mut chunk_data = Vec::new();
        let mut byte_id = 0;

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
