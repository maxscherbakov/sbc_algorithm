
pub(crate) mod chunk;
pub(crate) mod chunk_with_delta_code;
pub(crate) mod chunk_with_full_code;
pub mod graph;
pub mod levenshtein_functions;

use chunk::Chunk;
use chunk_with_delta_code::ChunkWithDeltaCode;
use graph::Graph;
use std::collections::HashMap;
use std::rc::Rc;
use std::fs::File;
use std::io::{Write, Error};
use crate::clusters::levenshtein_functions::levenshtein_distance;


struct Edge {
    weight: u32,
    chunk_index_1: usize,
    chunk_index_2: usize,
}

fn create_edges(chunks_vec: &Vec<(&u32, &Rc<dyn Chunk>)>) -> Vec<Edge> {
    let mut graph_edges: Vec<Edge> = Vec::new();

    let count_chunks = chunks_vec.len();

    'continue_x: for x in 0..count_chunks {
        for y in x + 1..count_chunks {
            let dist = chunks_vec[y].0 - chunks_vec[x].0;
            if dist > (1u32 << 31) {
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

fn find_leader_chunk_in_cluster(chunks_vec : &Vec<(&u32, &Rc<dyn Chunk>)>, cluster : &Vec<usize>) -> usize{
    let mut leader_index = 0;
    let mut min_sum_dist = std::u32::MAX;

    for chunk_index_1 in cluster.iter() {
        let mut sum_dist_for_chunk = 0u32;

        for chunk_index_2 in cluster.iter() {
            sum_dist_for_chunk += levenshtein_distance(Rc::clone(chunks_vec[*chunk_index_1].1), Rc::clone(chunks_vec[*chunk_index_2].1))
        }

        if sum_dist_for_chunk < min_sum_dist {
            leader_index = *chunk_index_1;
            min_sum_dist = sum_dist_for_chunk
        }
    }
    return leader_index;

}



pub(super) fn encoding(chunks_hashmap: &mut HashMap<u32, Rc<dyn Chunk>>) {
    let mut chunk_hash_leader_hash = Vec::new();
    {
        let mut chunks_vec: Vec<(&u32, &Rc<dyn Chunk>)> = chunks_hashmap.iter().collect();
        chunks_vec.sort_by(|a, b| a.0.cmp(b.0));

        let mut graph = Graph::new(chunks_hashmap.len());
        let graph_edges = create_edges(&chunks_vec);
        let clusters = graph.create_clusters_based_on_the_kraskal_algorithm(graph_edges);

        let mut clusters_vec = vec![Vec::new(); chunks_vec.len()];
        for (chunk_index, leader_index) in clusters.iter().enumerate() {
            clusters_vec[*leader_index].push(chunk_index);
        }

        for cluster in clusters_vec {
            if cluster.is_empty() { continue }
            let leader_index = find_leader_chunk_in_cluster(&chunks_vec, &cluster);

            for chunk_index in &cluster {
                chunk_hash_leader_hash.push((*chunks_vec[*chunk_index].0, *chunks_vec[leader_index].0))
            }
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

pub fn decode(chunks_hashmap: &HashMap<u32, Rc<dyn Chunk>>, vec_with_hash_for_file : Vec<u32>) -> Result<(), Error> {
    let mut file = File::create("files/output.txt")?;
    for hash in vec_with_hash_for_file {
        let chunk = chunks_hashmap.get(&hash).unwrap();
        file.write_all(chunk.get_data().as_slice())?;
    }
    Ok(())
}

pub fn size_hashmap(chunks_hashmap: &HashMap<u32, Rc<dyn Chunk>>) -> u32 {
    let mut size_hashmap = 0;
    for chunk in chunks_hashmap.iter() {
        size_hashmap += chunk.1.size_in_memory();
    }
    size_hashmap
}
