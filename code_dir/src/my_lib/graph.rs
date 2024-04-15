use crate::my_lib::chunk::Chunk;
use crate::my_lib::Edge;

pub(super) struct Graph<'a> {
    vertex_count: u32,
    parent: Vec<usize>,
    rank: Vec<u32>,
    edges: &'a Vec<Edge>,
}

impl Graph<'_> {
    pub(crate) fn new(vertex_count: usize, edges: &Vec<Edge>) -> Graph {
        Graph {
            vertex_count: vertex_count as u32,
            parent: (0..vertex_count).collect(),
            rank: vec![0u32; vertex_count],
            edges,
        }
    }

    fn union_set(&mut self, index_set_1: usize, index_set_2: usize) {
        if self.rank[index_set_1] < self.rank[index_set_2] {
            self.rank[index_set_2] += self.rank[index_set_1];
            self.parent[index_set_1] = self.parent[index_set_2];
        } else {
            self.rank[index_set_1] += self.rank[index_set_2];
            self.parent[index_set_2] = self.parent[index_set_1];
        }
    }

    fn find_set(&mut self, index_set: usize) -> usize {
        if index_set != self.parent[index_set] {
            self.parent[index_set] = self.find_set(self.parent[index_set]);
            return self.parent[index_set];
        }
        index_set
    }

    pub(super) fn create_clusters_based_on_the_kraskal_algorithm<'a>(
        &mut self,
        chunks: &[&'a dyn Chunk],
    ) -> Vec<Vec<&'a dyn Chunk>> {
        for edge in self.edges {
            let index_set_1 = self.find_set(edge.chunk_index_1);
            let index_set_2 = self.find_set(edge.chunk_index_2);
            if index_set_1 != index_set_2 {
                self.union_set(index_set_1, index_set_2);
            }
        }

        let mut cluster: Vec<Vec<&dyn Chunk>> = vec![Vec::new(); self.vertex_count as usize];
        for (index_chunk, leader_index) in self.parent.iter().enumerate() {
            cluster[*leader_index].push(chunks[index_chunk]);
        }

        cluster
    }
}
