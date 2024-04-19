use crate::my_lib::levenshtein_functions::{Action, DeltaAction};
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

pub(crate) fn decode_file_with_chunks(input_path: &str, output_path: &str) -> Result<(), std::io::Error>{
    let input = File::open(input_path)?;
    let mut output = File::create(output_path)?;

    let memory_map = unsafe { Mmap::map(&input)? };
    let mut offset = 0usize;
    let header_size = 9usize;
    let max_offset = input.metadata().unwrap().len() as usize;

    while offset < max_offset {
        let header_chunk = &memory_map[offset..offset + header_size];
        match header_chunk[0] {
            0 => {
                let mut length_arr = [0u8; 8];
                length_arr.copy_from_slice(&header_chunk[1..(8 + 1)]);
                let length = u64::from_le_bytes(length_arr) as usize;

                output
                    .write_all(&memory_map[offset + header_size..offset + header_size + length])?;
                offset += length + header_size;
            }
            1 => {
                let mut length_arr = [0u8; 8];
                length_arr.copy_from_slice(&header_chunk[1..(8 + 1)]);
                let length = u64::from_le_bytes(length_arr).to_le() as usize;

                let mut offset_leader_chunk_arr = [0u8; 8];
                offset_leader_chunk_arr.copy_from_slice(&memory_map[offset + header_size..offset + header_size + 8]);
                let offset_leader_chunk =
                    unsafe { std::mem::transmute::<[u8; 8], u64>(offset_leader_chunk_arr) }.to_le()
                        as usize;


                let mut length_leader_chunk_arr = [0u8; 8];
                length_leader_chunk_arr.copy_from_slice(&memory_map[offset_leader_chunk+1..offset_leader_chunk + header_size]);
                let length_leader_chunk =
                    unsafe { std::mem::transmute::<[u8; 8], u64>(length_leader_chunk_arr) }.to_le()
                        as usize;


                let mut data_leader_chunk = memory_map[offset_leader_chunk + header_size
                    ..offset_leader_chunk + header_size + length_leader_chunk]
                    .to_vec();

                let delta_code_slice =
                    &memory_map[offset + header_size + 8..offset + header_size + 8 + length];
                let mut action_index = 0;
                while action_index < length {
                    let action: Action = match delta_code_slice[action_index] {
                        0 => Action::Del,
                        1 => Action::Add,
                        2 => Action::Rep,
                        other_byte => panic!("There is no action with number {}!", other_byte),
                    };

                    let mut cursor_on_action_in_chunk_arr = [0u8; 8];
                    cursor_on_action_in_chunk_arr.copy_from_slice(&delta_code_slice[(1 + action_index)..(9 + action_index)]);
                    let cursor_on_action_in_chunk =
                        unsafe { std::mem::transmute::<[u8; 8], u64>(cursor_on_action_in_chunk_arr) }.to_le()
                            as usize;

                    let action_byte_value = delta_code_slice[action_index + 9];
                    let delta_action = DeltaAction {
                        action,
                        index: cursor_on_action_in_chunk,
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
                    .write_all(data_leader_chunk.as_slice())?;

                offset += length + header_size + 8;
            }
            other_byte => panic!("There is no chunk with ID {}!", other_byte),
        }
    }
    Ok(())
}
