use crate::my_lib::chunk::Chunk;
use crate::my_lib::levenshtein_functions::{Action, DeltaAction};

pub(crate) struct ChunkWithDeltaCode<'a> {
    index: usize,
    size: usize,
    leader_chunk: &'a dyn Chunk,
    delta_code: Vec<DeltaAction>,
}

impl Chunk for ChunkWithDeltaCode<'_> {
    fn decode(&self) {
        for byte in self.get_data() {
            print!("{}", byte as char);
        }
    }
    fn get_data(&self) -> Vec<u8> {
        let mut chunk_data = self.get_data_leader_chunk();
        for delta_action in self.get_delta_code() {
            match &delta_action.action {
                Action::Del => {
                    chunk_data.remove(delta_action.index);
                }
                Action::Add => chunk_data.insert(delta_action.index, delta_action.byte_value),
                Action::Rep => chunk_data[delta_action.index] = delta_action.byte_value,
            }
        }
        chunk_data
    }
    fn get_index(&self) -> usize {
        self.index
    }

    fn get_type(&self) {
        println!("Chunk with delta code")
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl ChunkWithDeltaCode<'_> {
    pub(crate) fn new(
        index: usize,
        size: usize,
        leader_chunk: &dyn Chunk,
        delta_code: Vec<DeltaAction>,
    ) -> ChunkWithDeltaCode<'_> {
        ChunkWithDeltaCode {
            index,
            size,
            leader_chunk,
            delta_code,
        }
    }

    pub(crate) fn get_data_leader_chunk(&self) -> Vec<u8> {
        self.leader_chunk.get_data()
    }

    pub(crate) fn get_delta_code(&self) -> &[DeltaAction] {
        self.delta_code.as_slice()
    }
}
