use crate::clusters::Edge;

pub(crate) struct Graph {
    parent: Vec<usize>,
    rank: Vec<u32>,
}

impl Graph {
    pub(crate) fn new(graph_count_vertices: usize) -> Graph {
        Graph {
            parent: (0..graph_count_vertices).collect(),
            rank: vec![0u32; graph_count_vertices],
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

    pub(super) fn create_clusters_based_on_the_kraskal_algorithm(
        &mut self,
        edges: Vec<Edge>,
    ) -> Vec<usize> {
        for edge in edges {
            let index_set_1 = self.find_set(edge.chunk_index_1);
            let index_set_2 = self.find_set(edge.chunk_index_2);
            if index_set_1 != index_set_2 {
                self.union_set(index_set_1, index_set_2);
            }
        }
        for i in self.parent.clone() {
            self.find_set(i);
        }

        self.parent.clone()
    }
}
