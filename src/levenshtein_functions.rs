use std::cmp::min;
use Action::*;

pub(crate) enum Action {
    Del,
    Add,
    Rep,
}

fn find_id_non_eq_byte(data_chunk: &[u8], data_chunk_parent: &[u8]) -> (usize, usize) {
    let mut id_non_eq_byte_start = 0;
    while data_chunk[id_non_eq_byte_start] == data_chunk_parent[id_non_eq_byte_start] {
        id_non_eq_byte_start += 1;
        if id_non_eq_byte_start == min(data_chunk_parent.len(), data_chunk.len()) {
            break;
        }
    }
    let mut id_non_eq_byte_end = 0;
    if !((data_chunk.len() <= id_non_eq_byte_start)
        | (data_chunk_parent.len() <= id_non_eq_byte_start))
    {
        while data_chunk[data_chunk.len() - id_non_eq_byte_end - 1]
            == data_chunk_parent[data_chunk_parent.len() - id_non_eq_byte_end - 1]
        {
            id_non_eq_byte_end += 1;
            if min(data_chunk.len(), data_chunk_parent.len()) - id_non_eq_byte_end
                == id_non_eq_byte_start
            {
                break;
            }
        }
    }
    (id_non_eq_byte_start, id_non_eq_byte_end)
}

pub(crate) fn encode(data_chunk: &[u8], data_chunk_parent: &[u8]) -> Option<Vec<u32>> {
    let max_len_delta_code = data_chunk.len() as u32;
    let mut delta_code = Vec::new();
    let (id_non_eq_byte_start, id_non_eq_byte_end) =
        find_id_non_eq_byte(data_chunk, data_chunk_parent);

    let data_chunk =
        data_chunk[id_non_eq_byte_start..data_chunk.len() - id_non_eq_byte_end].to_vec();
    let data_chunk_parent = data_chunk_parent
        [id_non_eq_byte_start..data_chunk_parent.len() - id_non_eq_byte_end]
        .to_vec();

    let matrix = levenshtein_matrix(data_chunk.as_slice(), data_chunk_parent.as_slice());

    if matrix[matrix.len() - 1][matrix[0].len() - 1] * 4 + 4 > max_len_delta_code {
        return None;
    }
    let mut x = matrix[0].len() - 1;
    let mut y = matrix.len() - 1;
    while x > 0 || y > 0 {
        if x > 0
            && y > 0
            && (data_chunk_parent[y - 1] != data_chunk[x - 1])
            && (matrix[y - 1][x - 1] < matrix[y][x])
        {
            delta_code.push(encode_delta_action(
                Rep,
                id_non_eq_byte_start + y - 1,
                data_chunk[x - 1],
            ));
            x -= 1;
            y -= 1;
        } else if y > 0 && matrix[y - 1][x] < matrix[y][x] {
            delta_code.push(encode_delta_action(Del, id_non_eq_byte_start + y - 1, 0));
            y -= 1;
        } else if x > 0 && matrix[y][x - 1] < matrix[y][x] {
            delta_code.push(encode_delta_action(
                Add,
                id_non_eq_byte_start + y,
                data_chunk[x - 1],
            ));
            x -= 1;
        } else {
            x -= 1;
            y -= 1;
        }
    }
    Some(delta_code)
}

#[allow(dead_code)]
pub(crate) fn levenshtein_distance(data_chunk: &[u8], data_chunk_parent: &[u8]) -> u32 {
    let mut id_eq_byte = 0;
    while data_chunk[id_eq_byte] == data_chunk_parent[id_eq_byte] {
        if id_eq_byte == min(data_chunk_parent.len(), data_chunk.len()) - 1 {
            break;
        }
        id_eq_byte += 1;
    }
    let levenshtein_matrix =
        levenshtein_matrix(&data_chunk[id_eq_byte..], &data_chunk_parent[id_eq_byte..]);
    levenshtein_matrix[data_chunk_parent.len()][data_chunk.len()]
}

fn levenshtein_matrix(data_chunk: &[u8], data_chunk_parent: &[u8]) -> Vec<Vec<u32>> {
    let mut levenshtein_matrix =
        vec![vec![0u32; data_chunk.len() + 1]; data_chunk_parent.len() + 1];
    levenshtein_matrix[0] = (0..data_chunk.len() as u32 + 1).collect();
    for y in 1..data_chunk_parent.len() + 1 {
        levenshtein_matrix[y][0] = y as u32;
        for x in 1..data_chunk.len() + 1 {
            let add = levenshtein_matrix[y - 1][x] + 1;
            let del = levenshtein_matrix[y][x - 1] + 1;
            let mut replace = levenshtein_matrix[y - 1][x - 1];
            if data_chunk_parent[y - 1] != data_chunk[x - 1] {
                replace += 1;
            }
            levenshtein_matrix[y][x] = min(min(del, add), replace);
        }
    }
    levenshtein_matrix
}

fn encode_delta_action(action: Action, index: usize, byte_value: u8) -> u32 {
    let mut code = 0u32;
    match action {
        Del => {
            code += 1 << 31;
        }
        Add => {
            code += 1 << 30;
        }
        Rep => {}
    }
    code += byte_value as u32 * (1 << 22);
    if index >= (1 << 22) {
        panic!()
    }
    code += index as u32;
    code
}

pub(crate) fn get_delta_action(code: u32) -> (Action, usize, u8) {
    let action = match code / (1 << 30) {
        0 => Rep,
        1 => Add,
        2 => Del,
        _ => panic!(),
    };
    let byte_value = code % (1 << 30) / (1 << 22);
    let index = code % (1 << 22);
    (action, index as usize, byte_value as u8)
}

#[cfg(test)]
mod test {
    use crate::levenshtein_functions;
    use crate::levenshtein_functions::{get_delta_action, Action};

    #[test]
    fn test_chunk_recovery() {
        let parent_chunk_data = vec![134u8, 69, 17, 85, 92, 21, 249, 94];
        let data = vec![134u8, 69, 116, 85, 92, 21, 249, 94];
        let mut delta_chunk = Vec::new();
        for byte in 2u32.to_be_bytes() {
            delta_chunk.push(byte);
        }
        match levenshtein_functions::encode(data.as_slice(), parent_chunk_data.as_slice()) {
            None => {}
            Some(delta_code) => {
                for delta_action in delta_code {
                    for byte in delta_action.to_be_bytes() {
                        delta_chunk.push(byte);
                    }
                }
            }
        }

        let mut buf = [0u8; 4];
        buf.copy_from_slice(&delta_chunk[..4]);

        let _parent_hash = u32::from_be_bytes(buf);
        let mut data_recovery = parent_chunk_data.clone();

        let mut byte_index = 4;
        while byte_index < delta_chunk.len() {
            buf.copy_from_slice(&delta_chunk[byte_index..byte_index + 4]);
            let delta_action = u32::from_be_bytes(buf);

            let (action, index, byte_value) = get_delta_action(delta_action);
            match action {
                Action::Del => {
                    data_recovery.remove(index);
                }
                Action::Add => {
                    data_recovery.insert(index, byte_value);
                }
                Action::Rep => {
                    data_recovery[index] = byte_value;
                }
            }
            byte_index += 4;
        }

        assert_eq!(delta_chunk.len(), 8);
        assert_eq!(data_recovery, data);
    }
}
