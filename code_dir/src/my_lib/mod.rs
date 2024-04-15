pub(super) mod chunk;
pub(super) mod chunk_with_delta_code;
pub(super) mod chunk_with_full_code;
mod graph;
mod levenshtein_functions;

use chunk::Chunk;
use chunk_with_delta_code::ChunkWithDeltaCode;
use graph::Graph;
use levenshtein_functions::*;

struct Edge {
    weight: u32,
    chunk_index_1: usize,
    chunk_index_2: usize,
}
fn create_edges(chunks: &[&dyn Chunk]) -> Vec<Edge> {
    let count_chunks = chunks.len();
    let mut graph_edges: Vec<Edge> = Vec::new();

    for x in 0..count_chunks {
        for y in x + 1..count_chunks {
            let dist = levenshtein_distance(chunks[x], chunks[y]);
            if dist < 3 {
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

fn find_index_chunk_leader(group: &[&dyn Chunk]) -> usize {
    let mut sum_distance = vec![0u32; group.len()];
    for chunk_index_1 in 0..group.len() {
        for chunk_index_2 in chunk_index_1 + 1..group.len() {
            let distance = levenshtein_distance(group[chunk_index_1], group[chunk_index_2]);
            sum_distance[chunk_index_1] += distance;
            sum_distance[chunk_index_2] += distance;
        }
    }
    let mut min_sum_distance = u32::MAX;
    let mut leader_index: usize = 0;
    for (index_sum, sum) in sum_distance.iter().enumerate() {
        if *sum < min_sum_distance {
            leader_index = index_sum;
            min_sum_distance = *sum;
        }
    }

    leader_index
}

pub(super) fn encoding<'a>(
    chunks: &mut [&'a dyn Chunk],
    chunks_with_delta_code: &'a mut Vec<ChunkWithDeltaCode<'a>>,
) {
    let graph_edges = create_edges(chunks);
    let mut graph = Graph::new(chunks.len(), graph_edges.as_slice());
    let clusters = graph.create_clusters_based_on_the_kraskal_algorithm(chunks);

    for cluster in clusters {
        if cluster.is_empty() {
            continue;
        }
        let leader_index = find_index_chunk_leader(cluster.as_slice());
        let leader_link = cluster[leader_index];
        for chunk in cluster {
            if chunk.get_index() == leader_index {
                continue;
            }
            let delta_code = levenshtein_functions::coding(chunk, leader_link);
            chunks_with_delta_code.push(ChunkWithDeltaCode::new(
                chunk.get_index(),
                chunk.size(),
                leader_link,
                delta_code,
            ));
        }
    }
    for chunk_with_delta_code in chunks_with_delta_code {
        chunks[chunk_with_delta_code.get_index() - 1] = chunk_with_delta_code;
    }
}