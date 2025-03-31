mod graph;

use crate::chunkfs_sbc::{ClusterPoint, Clusters};
pub use graph::Graph;
use crate::SBCHash;

pub trait Clusterer<Hash: SBCHash> {
    fn clusterize<'a>(&mut self, chunk_sbc_hash: Vec<ClusterPoint<'a, Hash>>) -> Clusters<'a, Hash>;
}
