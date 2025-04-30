use crate::chunkfs_sbc::{ClusterPoint, Clusters};
use crate::clusterer::Clusterer;
use crate::SBCHash;
use std::collections::HashMap;

/// A vertex in the graph used for clustering.
///
/// Each vertex tracks its parent for union-find operations during MST construction.
struct Vertex {
    /// The parent vertex key in the union-find structure.
    parent: u32,
}

impl Vertex {
    /// Creates a new vertex with itself as its own parent.
    ///
    /// # Arguments
    ///
    /// * `key` - The unique key identifying this vertex.
    ///
    /// # Returns
    ///
    /// A new `Vertex` instance.
    pub fn new(key: u32) -> Vertex {
        Vertex { parent: key }
    }
}

/// A clusterer that groups chunks using Kruskal's algorithm to build a minimum spanning tree (MST).
///
/// `GraphClusterer` uses a union-find data structure to cluster chunks based on their hash keys,
/// grouping chunks whose keys are close within a certain threshold (`max_weight_edge`).
///
/// # Details
///
/// The clustering is performed by assigning each chunk to a cluster represented by the root parent
/// found via union-find. The `set_parent_vertex` method attempts to find a nearby parent vertex
/// within the allowed edge weight to merge clusters.
///
/// # Type Parameters
///
/// * `Hash` - The hash type implementing `SBCHash`.
///
/// # Example
///
/// ```
/// # use sbc_algorithm::clusterer::GraphClusterer;
///
/// let mut clusterer = GraphClusterer::default();
/// // Use clusterer.clusterize(...) to cluster chunks.
/// ```
pub struct GraphClusterer {
    /// Map of vertex keys to their union-find vertex data.
    vertices: HashMap<u32, Vertex>,
    max_weight_edge: u32,
}

impl Default for GraphClusterer {
    /// Creates a new, empty `GraphClusterer`.
    fn default() -> Self {
        Self::new(10)
    }
}

impl GraphClusterer {
    /// Constructs a new `GraphClusterer`.
    ///
    /// # Returns
    ///
    /// An empty `GraphClusterer`.
    pub fn new(_max_weight_edge: u32) -> GraphClusterer {
        GraphClusterer {
            max_weight_edge: _max_weight_edge,
            vertices: HashMap::new(),
        }
    }

    /// Finds the root parent of the given vertex key using path compression.
    ///
    /// # Arguments
    ///
    /// * `hash_set` - The vertex key to find the parent for.
    ///
    /// # Returns
    ///
    /// The root parent's key.
    fn find_set(&mut self, hash_set: u32) -> u32 {
        let parent = self.vertices.get(&hash_set).unwrap().parent;
        if hash_set != parent {
            let parent = self.find_set(parent);
            self.vertices.get_mut(&hash_set).unwrap().parent = parent;
            parent
        } else {
            parent
        }
    }

    /// Attempts to find a nearby parent vertex within `max_weight_edge` distance to cluster with.
    /// If no suitable parent is found, the vertex becomes its own parent.
    ///
    /// # Arguments
    ///
    /// * `hash` - The vertex key to assign a parent for.
    ///
    /// # Returns
    ///
    /// The parent vertex key assigned.
    fn set_parent_vertex(&mut self, hash: u32) -> u32 {
        let mut min_dist = u32::MAX;
        let mut parent_hash = hash;

        // Search in the range [hash - MAX_WEIGHT_EDGE, hash + MAX_WEIGHT_EDGE]
        let start = hash.saturating_sub(self.max_weight_edge);
        let end = hash.saturating_add(self.max_weight_edge);

        for other_hash in start..=end {
            if self.vertices.contains_key(&other_hash) {
                let other_parent_hash = self.find_set(other_hash);
                let dist = other_parent_hash.abs_diff(hash);
                if dist < min_dist && dist <= self.max_weight_edge {
                    min_dist = dist;
                    parent_hash = other_parent_hash;
                }
            }
        }

        self.vertices.insert(hash, Vertex::new(parent_hash));
        parent_hash
    }
}

impl<Hash: SBCHash> Clusterer<Hash> for GraphClusterer {
    /// Clusters chunks by grouping them based on proximity of their hash keys using MST logic.
    ///
    /// # Arguments
    ///
    /// * `chunk_sbc_hash` - A vector of chunk points with their similarity hashes.
    ///
    /// # Returns
    ///
    /// A map of clusters keyed by the root hash, each containing grouped chunk points.
    fn clusterize<'a>(
        &mut self,
        chunk_sbc_hash: Vec<ClusterPoint<'a, Hash>>,
    ) -> Clusters<'a, Hash> {
        let mut clusters: Clusters<Hash> = HashMap::default();

        for (sbc_hash, data_container) in chunk_sbc_hash {
            // Obtain u32 key for graph clustering from the hash
            let key = sbc_hash.get_key_for_graph_clusterer();

            // Find or assign the parent vertex for this key
            let parent_key = self.set_parent_vertex(key);

            // Group the chunk into the cluster identified by the parent's hash
            let cluster = clusters.entry(Hash::new_with_u32(parent_key)).or_default();
            cluster.push((sbc_hash, data_container));
        }

        clusters
    }
}
