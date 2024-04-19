use crate::my_lib::Edge;

pub(super) struct Graph<'a> {
    parents: Vec<usize>,
    rank: Vec<u32>,
    edges: &'a Vec<Edge>,
}

impl Graph<'_> {
    pub(crate) fn new(vertex_count: usize, edges: &Vec<Edge>) -> Graph {
        Graph {
            parents: (0..vertex_count).collect(),
            rank: vec![0u32; vertex_count],
            edges,
        }
    }

    fn union_set(&mut self, index_set_1: usize, index_set_2: usize) {
        if self.rank[index_set_1] < self.rank[index_set_2] {
            self.rank[index_set_2] += self.rank[index_set_1];
            self.parents[index_set_1] = self.parents[index_set_2];
        } else {
            self.rank[index_set_1] += self.rank[index_set_2];
            self.parents[index_set_2] = self.parents[index_set_1];
        }
    }

    fn find_set(&mut self, index_set: usize) -> usize {
        if index_set != self.parents[index_set] {
            self.parents[index_set] = self.find_set(self.parents[index_set]);
            return self.parents[index_set];
        }
        index_set
    }

    pub(super) fn create_clusters_based_on_the_kraskal_algorithm(&mut self) -> Vec<usize> {
        for edge in self.edges {
            let index_set_1 = self.find_set(edge.chunk_index_1);
            let index_set_2 = self.find_set(edge.chunk_index_2);
            if index_set_1 != index_set_2 {
                self.union_set(index_set_1, index_set_2);
            }
        }
        for vertex in 0..self.parents.len() {
            self.parents[vertex] = self.find_set(self.parents[vertex]);
        }
        self.parents.to_vec()
    }
}
