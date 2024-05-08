use crate::my_lib::chunk::Chunk;
use crate::my_lib::levenshtein_functions::{Action, DeltaAction};

use std::rc::Rc;

pub(crate) struct ChunkWithDeltaCode {
    leader_chunk: Rc<dyn Chunk>,
    delta_code: Vec<DeltaAction>,
}

impl Chunk for ChunkWithDeltaCode {
    fn decode(&self) {
        for byte in self.get_data() {
            print!("{}", byte as char);
        }
    }
    fn get_data(&self) -> Vec<u8> {
        let mut chunk_data = self.leader_chunk.get_data();
        for delta_action in &self.delta_code {
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
}

impl ChunkWithDeltaCode {
    pub(crate) fn new(
        leader_chunk: Rc<dyn Chunk>,
        chunk_delta_code: Vec<DeltaAction>,
    ) -> ChunkWithDeltaCode {
        ChunkWithDeltaCode {
            leader_chunk,
            delta_code: chunk_delta_code,
        }
    }
}
