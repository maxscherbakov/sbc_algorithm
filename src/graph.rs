use std::collections::HashMap;

const MAX_WEIGHT_EDGE: u32 = 1 << 5;

struct Vertex {
    parent: u32,
}

impl Vertex {
    pub fn new(key: u32) -> Vertex {
        Vertex { parent: key }
    }
}

pub(crate) struct Graph {
    vertices: HashMap<u32, Vertex>,
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            vertices: HashMap::new(),
        }
    }

    pub fn find_set(&mut self, hash_set: u32) -> u32 {
        let parent = self.vertices.get(&hash_set).unwrap().parent;
        if hash_set != parent {
            let parent = self.find_set(parent);
            self.vertices.get_mut(&hash_set).unwrap().parent = parent;
        }
        parent
    }

    pub fn set_parent_vertex(&mut self, hash: u32) -> u32 {
        let mut min_dist = u32::MAX;
        let mut parent_hash = hash;
        for other_hash in hash - std::cmp::min(hash, MAX_WEIGHT_EDGE)
            ..=hash + std::cmp::min(u32::MAX - hash, MAX_WEIGHT_EDGE)
        {
            if self.vertices.contains_key(&other_hash) {
                let other_parent_hash = self.find_set(other_hash);
                let dist = u32::abs_diff(other_parent_hash, hash);
                if dist < min_dist && dist <= MAX_WEIGHT_EDGE {
                    min_dist = dist;
                    parent_hash = other_parent_hash;
                }
            }
        }
        self.vertices.insert(hash, Vertex::new(parent_hash));
        parent_hash
    }
}
