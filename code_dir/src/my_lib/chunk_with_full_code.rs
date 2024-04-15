use crate::my_lib::chunk::Chunk;
pub(crate) struct ChunkWithFullCode {
    index: usize,
    size: usize,
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
    fn get_index(&self) -> usize {
        self.index
    }

    fn get_type(&self) {
        println!("Chunk with full code")
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl ChunkWithFullCode {
    pub(crate) fn new(
        index: usize,
        size: usize,
        data: Vec<u8>,
    ) -> ChunkWithFullCode {
        ChunkWithFullCode {
            index,
            size,
            data,
        }
    }
}
