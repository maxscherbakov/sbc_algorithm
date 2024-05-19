use std::cmp::min;
use Action::*;

pub(crate) enum Action {
    Del,
    Add,
    Rep,
}
pub(crate) struct DeltaAction {
    pub(crate) action: Action,
    pub(crate) index: usize,
    pub(crate) byte_value: u8,
}

pub(crate) fn encode(data_chunk: &[u8], data_chunk_parent: &[u8]) -> Vec<DeltaAction> {
    let matrix = levenshtein_matrix(data_chunk, data_chunk_parent);
    let mut delta_code: Vec<DeltaAction> = Vec::new();
    let mut x = data_chunk.len();
    let mut y = data_chunk_parent.len();
    while x > 0 && y > 0 {
        if (data_chunk_parent[y - 1] != data_chunk[x - 1]) && (matrix[y - 1][x - 1] < matrix[y][x])
        {
            delta_code.push(DeltaAction {
                action: Rep,
                index: y - 1,
                byte_value: data_chunk[x - 1],
            });
            x -= 1;
            y -= 1;
        } else if matrix[y - 1][x] < matrix[y][x] {
            delta_code.push(DeltaAction {
                action: Del,
                index: y - 1,
                byte_value: 0,
            });
            y -= 1;
        } else if matrix[y][x - 1] < matrix[y][x] {
            delta_code.push(DeltaAction {
                action: Add,
                index: y - 1,
                byte_value: data_chunk[x - 1],
            });
            x -= 1;
        } else {
            x -= 1;
            y -= 1;
        }
    }
    delta_code
}

pub(crate) fn levenshtein_distance(data_chunk: &[u8], data_chunk_parent: &[u8]) -> u32 {
    let levenshtein_matrix = levenshtein_matrix(data_chunk, data_chunk_parent);
    levenshtein_matrix[data_chunk_parent.len()][data_chunk.len()]
}

pub(crate) fn levenshtein_matrix(data_chunk: &[u8], data_chunk_parent: &[u8]) -> Vec<Vec<u32>> {
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
