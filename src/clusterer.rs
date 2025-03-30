mod graph;

use crate::chunkfs_sbc::{ClusterPoint, Clusters};
pub use graph::Graph;

pub trait Clusterer {
    fn clusterize<'a>(&mut self, chunk_sbc_hash: Vec<ClusterPoint<'a>>) -> Clusters<'a>;
}
