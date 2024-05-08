pub(super) mod chunk;
pub(super) mod chunk_with_delta_code;
pub(super) mod chunk_with_full_code;
mod graph;
mod levenshtein_functions;

use chunk::Chunk;
use chunk_with_delta_code::ChunkWithDeltaCode;
use graph::Graph;
use std::collections::HashMap;
use std::rc::Rc;

struct Edge {
    weight: u32,
    chunk_index_1: usize,
    chunk_index_2: usize,
}

fn create_edges(chunks_vec: Vec<(&u32, &Rc<dyn Chunk>)>) -> Vec<Edge> {
    let mut graph_edges: Vec<Edge> = Vec::new();

    let count_chunks = chunks_vec.len();

    'continue_x: for x in 0..count_chunks {
        for y in x + 1..count_chunks {
            let dist = chunks_vec[y].0 - chunks_vec[x].0;
            if dist > 30 {
                continue 'continue_x;
            }
            graph_edges.push(Edge {
                weight: dist,
                chunk_index_1: x,
                chunk_index_2: y,
            })
        }
    }
    graph_edges.sort_by(|a, b| a.weight.cmp(&b.weight));
    graph_edges
}

pub(super) fn encoding(chunks_hashmap: &mut HashMap<u32, Rc<dyn Chunk>>) {
    let mut chunk_hash_leader_hash = Vec::new();
    {
        let mut chunks_vec: Vec<(&u32, &Rc<dyn Chunk>)> = chunks_hashmap.iter().collect();
        chunks_vec.sort_by(|a, b| a.0.cmp(b.0));

        let mut graph = Graph::new(chunks_hashmap.len());
        let graph_edges = create_edges(chunks_vec.clone());
        let clusters = graph.create_clusters_based_on_the_kraskal_algorithm(graph_edges);

        for (chunk_index, leader_index) in clusters.iter().enumerate() {
            chunk_hash_leader_hash.push((*chunks_vec[chunk_index].0, *chunks_vec[*leader_index].0))
        }
    }

    for (chunk_hash, leader_hash) in chunk_hash_leader_hash {
        if chunk_hash == leader_hash {
            continue;
        }

        let delta_code = levenshtein_functions::encode(
            chunks_hashmap.get(&chunk_hash).unwrap(),
            chunks_hashmap.get(&leader_hash).unwrap(),
        );
        let link = Rc::clone(chunks_hashmap.get(&leader_hash).unwrap());
        let chunk = chunks_hashmap.get_mut(&chunk_hash).unwrap();
        *chunk = Rc::new(ChunkWithDeltaCode::new(link, delta_code));
    }
}
