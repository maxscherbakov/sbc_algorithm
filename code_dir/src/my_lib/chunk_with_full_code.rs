use crate::my_lib::chunk::Chunk;
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
}

impl ChunkWithFullCode {
    pub(crate) fn new(chunk_data: Vec<u8>) -> ChunkWithFullCode {
        ChunkWithFullCode { data: chunk_data }
    }
}
