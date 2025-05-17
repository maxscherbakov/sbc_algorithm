use crate::chunkfs_sbc::{ClusterPoint, Clusters};
use crate::clusterer::Clusterer;
use crate::SBCHash;
use std::collections::HashMap;

pub struct EqClusterer;

impl<Hash: SBCHash> Clusterer<Hash> for EqClusterer {
    fn clusterize<'a>(&mut self, chunk_sbc_hash: Vec<ClusterPoint<'a, Hash>>) -> Clusters<'a, Hash> {
        let mut clusters: Clusters<Hash> = HashMap::default();

        for (sbc_hash, data_container) in chunk_sbc_hash {
            let cluster = clusters.entry(sbc_hash.clone()).or_default();
            cluster.push((sbc_hash, data_container));
        }

        clusters
    }
}