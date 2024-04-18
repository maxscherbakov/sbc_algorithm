pub(super) mod chunk;
pub(super) mod decode;
mod graph;
mod levenshtein_functions;

use chunk::Chunk;
use graph::Graph;
use levenshtein_functions::*;
use std::fs::File;
use std::io::Write;

struct Edge {
    weight: u32,
    chunk_index_1: usize,
    chunk_index_2: usize,
}

fn create_edges(chunks: &[Chunk]) -> Vec<Edge> {
    let count_chunks = chunks.len();
    let mut graph_edges = Vec::new();

    for x in 0..count_chunks {
        for y in x + 1..count_chunks {
            let dist = levenshtein_distance(&chunks[x], &chunks[y]);
            if dist <= 50 {
                graph_edges.push(Edge {
                    weight: dist,
                    chunk_index_1: y,
                    chunk_index_2: x,
                })
            }
        }
    }
    graph_edges.sort_by(|a, b| a.weight.cmp(&b.weight));
    graph_edges
}

#[allow(dead_code)]
fn find_chunk_leader_index(group: &[Chunk]) -> usize {
    let mut sum_distance = vec![0u32; group.len()];
    for chunk_index_1 in 0..group.len() {
        for chunk_index_2 in chunk_index_1 + 1..group.len() {
            let distance = levenshtein_distance(&group[chunk_index_1], &group[chunk_index_2]);
            sum_distance[chunk_index_1] += distance;
            sum_distance[chunk_index_2] += distance;
        }
    }
    let min_sum_distance = sum_distance.iter().min().unwrap();
    sum_distance
        .iter()
        .position(|r| r == min_sum_distance)
        .unwrap()
}

pub(super) fn write_to_file_chunk_with_full_code(chunk: &Chunk, output: &mut File) {
    output.write_all(&0u8.to_ne_bytes()).expect("write failed");
    output
        .write_all(&chunk.get_length().to_ne_bytes())
        .expect("write failed");
    output.write_all(&chunk.data).expect("write failed");
}

fn write_to_file_chunk_with_delta_code(
    offset_leader_chunk: usize,
    delta_code: Vec<DeltaAction>,
    output: &mut File,
) {
    output.write_all(&1u8.to_ne_bytes()).expect("write failed");
    let size = delta_code.len() * 10;
    output.write_all(&size.to_ne_bytes()).expect("write failed");
    output
        .write_all(&offset_leader_chunk.to_ne_bytes())
        .expect("write failed");

    for action in delta_code {
        let action_id : u8;
        match action.action {
            Action::Del => action_id = 0,
            Action::Add => action_id = 1,
            Action::Rep => action_id = 2,
        }
        output
            .write_all(&action_id.to_ne_bytes())
            .expect("write failed");
        output
            .write_all(&action.index.to_ne_bytes())
            .expect("write failed");
        output
            .write_all(&action.byte_value.to_ne_bytes())
            .expect("write failed");
    }
}

pub(super) fn encode(chunks: &mut [Chunk], path: &str) {
    let mut file = File::create(path).expect("file not create");
    let graph_edges = create_edges(chunks);
    let mut graph = Graph::new(chunks.len(), &graph_edges);

    let leaders = graph.create_clusters_based_on_the_kraskal_algorithm();
    let mut offset = 0usize;

    for (chunk_index, leader_index) in leaders.iter().enumerate() {
        if *leader_index == chunk_index {
            chunks[*leader_index].offset = offset;
            offset += chunks[chunk_index].get_length() + 9;
        } else {
            offset += delta_encode(&chunks[chunk_index], &chunks[*leader_index]).len() * 10 + 17;
        }
    }
    for (chunk_index, leader_index) in leaders.iter().enumerate() {
        if *leader_index == chunk_index {
            write_to_file_chunk_with_full_code(&chunks[chunk_index], &mut file);
        } else {
            let delta_code = delta_encode(&chunks[chunk_index], &chunks[*leader_index]);
            write_to_file_chunk_with_delta_code(
                chunks[*leader_index].get_offset(),
                delta_code,
                &mut file,
            );
        }
    }
}
