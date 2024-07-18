use std::collections::HashMap;

const MAX_WEIGHT_EDGE: u32 = 1 << 15;

pub struct Vertex {
    pub(crate) parent: u32,
    pub(crate) rank: u32,
}
impl Vertex {
    pub fn new(key: u32) -> Vertex {
        Vertex {
            parent: key,
            rank: 1,
        }
    }
}

pub struct Graph {
    pub(crate) vertices: HashMap<u32, Vertex>,
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            vertices: HashMap::new(),
        }
    }

    pub fn find_set(&mut self, hash_set: u32) -> u32 {
        let parent = self.vertices.get(&hash_set).unwrap().parent;
        let rank = self.vertices.get(&hash_set).unwrap().rank;

        if hash_set != parent {
            let parent = self.find_set(parent);
            self.vertices.insert(hash_set, Vertex { parent, rank });
            return self.vertices.get(&hash_set).unwrap().parent;
        }
        hash_set
    }

    pub fn add_vertex(&mut self, hash: u32) -> u32 {
        let mut min_dist = u32::MAX;
        let mut parent_hash = hash;
        for other_hash in hash - std::cmp::min(hash, MAX_WEIGHT_EDGE)
            ..=hash + std::cmp::min(u32::MAX - hash, MAX_WEIGHT_EDGE)
        {
            if self.vertices.contains_key(&other_hash) {
                let dist = u32::abs_diff(other_hash, hash);
                if dist < min_dist {
                    min_dist = dist;
                    parent_hash = self.find_set(other_hash);
                } else {
                    break;
                }
            }
        }

        self.vertices.insert(hash, Vertex::new(parent_hash));
        parent_hash
    }
}
