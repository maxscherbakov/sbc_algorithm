use std::mem::size_of_val;
use crate::clusters::chunk::Chunk;
pub(crate) struct ChunkWithFullCode {
    data: Vec<u8>,
}

impl Chunk for ChunkWithFullCode {
    fn decode(&self) {
        for byte in self.get_data() {
            print!("{}", byte as char);
        }
    }
    fn get_data(&self) -> Vec<u8> {
        self.data.clone()
    }

    fn size_in_memory(&self) -> u32 {
        self.data.len() as u32 * size_of_val(&self.data[0]) as u32
    }
}

impl ChunkWithFullCode {
    pub(crate) fn new(chunk_data: Vec<u8>) -> ChunkWithFullCode {
        ChunkWithFullCode { data: chunk_data }
    }
}
