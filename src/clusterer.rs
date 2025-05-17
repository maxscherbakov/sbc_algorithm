mod graph_clusterer;
mod eq_clusterer;

use crate::chunkfs_sbc::{ClusterPoint, Clusters};
use crate::SBCHash;
pub use graph_clusterer::GraphClusterer;
pub use eq_clusterer::EqClusterer;

/// A trait defining the clustering behavior for similarity-based chunking.
///
/// The `Clusterer` trait groups chunks, identified by their similarity hashes,
/// into clusters of related chunks. This is a key step in similarity-based chunking
/// workflows to identify chunks that can be efficiently encoded as deltas.
///
/// # Type Parameters
///
/// * `Hash` - The hash type implementing `SBCHash` that identifies chunks.
///
/// # Methods
///
/// * `clusterize` - Takes a vector of chunk similarity points and returns clusters grouping them.
pub trait Clusterer<Hash: SBCHash> {
    /// Groups chunks into clusters based on their similarity hashes.
    ///
    /// # Arguments
    ///
    /// * `chunk_sbc_hash` - A vector of `ClusterPoint` items representing chunks and their hashes.
    ///
    /// # Returns
    ///
    /// A collection of clusters, where each cluster is a grouping of related chunks.
    fn clusterize<'a>(&mut self, chunk_sbc_hash: Vec<ClusterPoint<'a, Hash>>)
        -> Clusters<'a, Hash>;
}
