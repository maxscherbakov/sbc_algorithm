mod graph;

use crate::chunkfs_sbc::{ClusterPoint, Clusters};
use crate::SBCHash;
pub use graph::Graph;

pub trait Clusterer<Hash: SBCHash> {
    fn clusterize<'a>(&mut self, chunk_sbc_hash: Vec<ClusterPoint<'a, Hash>>)
        -> Clusters<'a, Hash>;
}
