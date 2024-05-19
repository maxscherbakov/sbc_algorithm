use crate::levenshtein_functions::levenshtein_distance;
use crate::{match_chunk, Chunk};
use std::collections::HashMap;
const MAX_WEIGHT_EDGE: u32 = 1 << 8;

pub struct Vertex {
    pub(crate) parent: u32,
    rank: u32,
}

pub(self) struct Edge {
    weight: u32,
    hash_chunk_1: u32,
    hash_chunk_2: u32,
}

pub struct Graph {
    pub(crate) vertices: HashMap<u32, Vertex>,
}

fn find_leader_chunk_in_cluster(chunks_hashmap: &HashMap<u32, Chunk>, cluster: &Vec<u32>) -> u32 {
    let mut leader_hash = 0;
    let mut min_sum_dist = u32::MAX;

    for chunk_hash_1 in cluster.iter() {
        let mut sum_dist_for_chunk = 0u32;

        let chunk_data_1 = match_chunk(chunks_hashmap, chunk_hash_1);

        for chunk_hash_2 in cluster.iter() {
            let chunk_data_2 = match_chunk(chunks_hashmap, chunk_hash_2);

            sum_dist_for_chunk +=
                levenshtein_distance(chunk_data_1.as_slice(), chunk_data_2.as_slice());
        }

        if sum_dist_for_chunk < min_sum_dist {
            leader_hash = *chunk_hash_1;
            min_sum_dist = sum_dist_for_chunk
        }
    }
    return leader_hash;
}

fn create_edges(chunks_hashmap: &HashMap<u32, Chunk>) -> Vec<Edge> {
    let mut graph_edges: Vec<Edge> = Vec::new();

    for hash_1 in chunks_hashmap.keys() {
        for hash_2 in hash_1 - std::cmp::min(*hash_1, MAX_WEIGHT_EDGE)
            ..=hash_1 + std::cmp::min(u32::MAX - *hash_1, MAX_WEIGHT_EDGE)
        {
            if !chunks_hashmap.contains_key(&hash_2) {
                continue;
            }
            let dist = std::cmp::max(hash_2, *hash_1) - std::cmp::min(hash_2, *hash_1);

            graph_edges.push(Edge {
                weight: dist,
                hash_chunk_1: *hash_1,
                hash_chunk_2: hash_2,
            })
        }
    }

    graph_edges.sort_by(|a, b| a.weight.cmp(&b.weight));
    graph_edges
}

impl Graph {
    pub(crate) fn new(chunks_hashmap: &HashMap<u32, Chunk>) -> Graph {
        let mut vertices = HashMap::new();

        for chunk_hash in chunks_hashmap.keys() {
            vertices.insert(
                *chunk_hash,
                Vertex {
                    parent: *chunk_hash,
                    rank: 1,
                },
            );
        }

        let mut graph = Graph { vertices };
        let edges = create_edges(&chunks_hashmap);
        graph.create_graph_based_on_the_kraskal_algorithm(edges);

        graph.find_leaders_in_clusters(chunks_hashmap);
        graph
    }

    #[allow(dead_code)]
    pub(crate) fn add_vertex(&mut self, hash: u32) {
        let mut edge = Edge {
            weight: u32::MAX,
            hash_chunk_1: hash,
            hash_chunk_2: 0,
        };

        for other_hash in hash - std::cmp::min(MAX_WEIGHT_EDGE, hash)
            ..=hash + std::cmp::min(MAX_WEIGHT_EDGE, u32::MAX - hash)
        {
            if self.vertices.contains_key(&other_hash) {
                let leader_for_other_chunk = self.vertices.get(&other_hash).unwrap().parent;
                let dist = (leader_for_other_chunk as i64 - hash as i64).abs() as u32;
                if dist < edge.weight {
                    edge.weight = dist;
                    edge.hash_chunk_2 = leader_for_other_chunk;
                }
            }
        }

        if edge.weight <= MAX_WEIGHT_EDGE {
            self.vertices.insert(
                hash,
                Vertex {
                    parent: edge.hash_chunk_2,
                    rank: 1,
                },
            );
        } else {
            self.vertices.insert(
                hash,
                Vertex {
                    parent: hash,
                    rank: 1,
                },
            );
        }
    }

    fn union_set(&mut self, hash_set_1: u32, hash_set_2: u32) {
        let hash_vertex_1 = self.vertices.get_key_value(&hash_set_1).unwrap();
        let hash_vertex_2 = self.vertices.get_key_value(&hash_set_2).unwrap();
        let rank = hash_vertex_2.1.rank + hash_vertex_1.1.rank;
        let hash_1 = *hash_vertex_1.0;
        let hash_2 = *hash_vertex_2.0;

        if hash_vertex_1.1.rank < hash_vertex_2.1.rank {
            self.vertices.insert(
                hash_2,
                Vertex {
                    parent: hash_2,
                    rank,
                },
            );
            self.vertices.insert(
                hash_1,
                Vertex {
                    parent: hash_2,
                    rank,
                },
            );
        } else {
            self.vertices.insert(
                hash_1,
                Vertex {
                    parent: hash_1,
                    rank,
                },
            );
            self.vertices.insert(
                hash_2,
                Vertex {
                    parent: hash_1,
                    rank,
                },
            );
        }
    }

    fn find_set(&mut self, hash_set: u32) -> u32 {
        let parent = self.vertices.get(&hash_set).unwrap().parent;
        let rank = self.vertices.get(&hash_set).unwrap().rank;

        if hash_set != parent {
            let parent = self.find_set(parent);
            self.vertices.insert(hash_set, Vertex { parent, rank });
            return self.vertices.get(&hash_set).unwrap().parent;
        }
        hash_set
    }

    fn create_graph_based_on_the_kraskal_algorithm(&mut self, edges: Vec<Edge>) {
        for edge in edges {
            let hash_set_1 = self.find_set(edge.hash_chunk_1);
            let hash_set_2 = self.find_set(edge.hash_chunk_2);
            if hash_set_1 != hash_set_2 {
                self.union_set(hash_set_1, hash_set_2);
            }
        }
    }

    pub fn find_leaders_in_clusters(&mut self, chunks_hashmap: &HashMap<u32, Chunk>) {
        let mut clusters = HashMap::new();

        let mut vector_keys = Vec::new();
        for key in self.vertices.keys() {
            vector_keys.push(*key);
        }

        for hash in vector_keys {
            let leader = self.find_set(hash);
            let cluster = clusters.entry(leader).or_insert(Vec::new());
            cluster.push(hash);
        }

        for cluster in clusters.values() {
            if cluster.is_empty() {
                continue;
            }

            let leader_hash = find_leader_chunk_in_cluster(chunks_hashmap, cluster);

            for chunk_index in cluster {
                self.vertices.get_mut(chunk_index).unwrap().parent = leader_hash;
            }
        }
    }
}
