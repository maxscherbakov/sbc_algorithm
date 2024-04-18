use crate::my_lib::levenshtein_functions::{Action, DeltaAction};
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

pub(crate) fn decode_file_with_chunks(input_path: &str, output_path: &str) {
    let input = File::open(input_path).expect("file not open");
    let mut output = File::create(output_path).expect("file not create");

    let memory_map = unsafe { Mmap::map(&input).expect("Failed to create memory map") };
    let mut offset = 0usize;
    let header_size = 9usize;
    let max_offset = input.metadata().unwrap().len() as usize;

    while offset < max_offset {
        let header_chunk = &memory_map[offset..offset + header_size];
        match header_chunk[0] {
            0 => {
                let mut length_arr = [0u8; 8];
                for index_length in 0..8 {
                    length_arr[index_length] = header_chunk[index_length + 1];
                }
                let length =
                    unsafe { std::mem::transmute::<[u8; 8], u64>(length_arr) }.to_le() as usize;

                output
                    .write_all(&memory_map[offset + header_size..offset + header_size + length])
                    .expect("write failed");

                offset += length + header_size;
            }
            1 => {
                let mut length_arr = [0u8; 8];
                for index_length in 0..8 {
                    length_arr[index_length] = header_chunk[index_length + 1];
                }
                let length =
                    unsafe { std::mem::transmute::<[u8; 8], u64>(length_arr) }.to_le() as usize;

                let mut offset_leader_chunk_arr = [0u8; 8];
                let offset_leader_chunk_slice =
                    &memory_map[offset + header_size..offset + header_size + 8];
                for index_offset in 0..8 {
                    offset_leader_chunk_arr[index_offset] = offset_leader_chunk_slice[index_offset];
                }
                let offset_leader_chunk =
                    unsafe { std::mem::transmute::<[u8; 8], u64>(offset_leader_chunk_arr) }.to_le()
                        as usize;

                let mut length_leader_chunk_arr = [0u8; 8];

                let length_leader_chunk_slice =
                    &memory_map[offset_leader_chunk..offset_leader_chunk + header_size];
                for index_length in 0..8 {
                    length_leader_chunk_arr[index_length] =
                        length_leader_chunk_slice[index_length + 1];
                }
                let length_leader_chunk =
                    unsafe { std::mem::transmute::<[u8; 8], u64>(length_leader_chunk_arr) }.to_le()
                        as usize;
                let mut data_leader_chunk = (&memory_map[offset_leader_chunk + header_size
                    ..offset_leader_chunk + header_size + length_leader_chunk])
                    .to_vec();

                let delta_code_slice =
                    &memory_map[offset + header_size + 8..offset + header_size + 8 + length];
                let mut action_index = 0;
                while action_index < length {
                    let action: Action;
                    match delta_code_slice[action_index] {
                        0 => action = Action::Del,
                        1 => action = Action::Add,
                        2 => action = Action::Rep,
                        other_byte => panic!("There is no action with number {}!", other_byte),
                    }
                    let mut action_cursor_arr = [0u8; 8];
                    for index_byte_for_cursor in 1..9 {
                        action_cursor_arr[index_byte_for_cursor - 1] =
                            delta_code_slice[action_index + index_byte_for_cursor];
                    }
                    let action_cursor =
                        unsafe { std::mem::transmute::<[u8; 8], u64>(action_cursor_arr) }.to_le()
                            as usize;
                    let action_byte_value = delta_code_slice[action_index + 9];
                    let delta_action = DeltaAction {
                        action,
                        index: action_cursor,
                        byte_value: action_byte_value,
                    };

                    match delta_action.action {
                        Action::Del => {
                            data_leader_chunk.remove(delta_action.index);
                        }
                        Action::Add => {
                            data_leader_chunk.insert(delta_action.index, delta_action.byte_value)
                        }
                        Action::Rep => {
                            data_leader_chunk[delta_action.index] = delta_action.byte_value
                        }
                    }

                    action_index += 10;
                }
                output
                    .write_all(data_leader_chunk.as_slice())
                    .expect("write failed");

                offset += length + header_size + 8;
            }
            other_byte => panic!("There is no chunk with ID {}!", other_byte),
        }
    }
}
