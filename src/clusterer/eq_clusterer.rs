use crate::chunkfs_sbc::{ClusterPoint, Clusters};
use crate::clusterer::Clusterer;
use crate::SBCHash;
use chunkfs::ClusteringMeasurements;
use std::collections::HashMap;

pub struct EqClusterer;

impl<Hash: SBCHash> Clusterer<Hash> for EqClusterer {
    fn clusterize<'a>(
        &mut self,
        chunk_sbc_hash: Vec<ClusterPoint<'a, Hash>>,
    ) -> (Clusters<'a, Hash>, ClusteringMeasurements) {
        let mut clusters: Clusters<Hash> = HashMap::default();

        let mut total_cluster_size: usize = 0;
        let mut number_of_vertices_in_cluster = HashMap::new();
        let mut parent_vertices: Vec<u32> = Vec::new();

        for (sbc_hash, data_container) in chunk_sbc_hash {
            parent_vertices.push(sbc_hash.get_key_for_graph_clusterer());

            let cluster = clusters.entry(sbc_hash.clone()).or_default();
            cluster.push((sbc_hash, data_container));

            total_cluster_size += 1;
            number_of_vertices_in_cluster.insert(total_cluster_size as u32, 1);
        }

        let distance_to_other_clusters = calculate_distance_to_other_clusters(parent_vertices);
        let distance_to_vertices_in_cluster = HashMap::new();
        let cluster_dedup_ratio = HashMap::new();
        let number_of_clusters = total_cluster_size;

        let clusterization_report = ClusteringMeasurements {
            total_cluster_size,
            number_of_clusters,
            number_of_vertices_in_cluster,
            distance_to_vertices_in_cluster,
            distance_to_other_clusters,
            cluster_dedup_ratio,
        };

        (clusters, clusterization_report)
    }
}

fn calculate_distance_to_other_clusters(parent_vertices: Vec<u32>) -> HashMap<u32, Vec<usize>> {
    let mut distance_to_other_clusters = HashMap::new();

    for i in 0..parent_vertices.len() {
        let mut distances = Vec::new();

        for j in 0..parent_vertices.len() {
            if i != j {
                let distance = parent_vertices[i].abs_diff(parent_vertices[j]) as usize;
                distances.push(distance);
            }
        }

        distance_to_other_clusters.insert(parent_vertices[i], distances);
    }

    distance_to_other_clusters
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decoder, encoder, hasher, SBCMap, SBCScrubber};
    use chunkfs::chunkers::{SizeParams, SuperChunker};
    use chunkfs::hashers::Sha256Hasher;
    use chunkfs::FileSystem;

    #[test]
    fn scrub_should_return_correct_scrub_measurements_for_copy_scrubber() {
        let test_data = vec![10; 1024 * 1024];
        let chunk_size = SizeParams::new(2 * 1024, 8 * 1024, 16 * 1024);

        let mut fs = FileSystem::new_with_scrubber(
            HashMap::default(),
            SBCMap::new(decoder::GdeltaDecoder::new(false)),
            Box::new(SBCScrubber::new(
                hasher::AronovichHasher,
                EqClusterer,
                encoder::GdeltaEncoder::new(false),
            )),
            Sha256Hasher::default(),
        );

        let mut handle = fs.create_file("file".to_string(), SuperChunker::new(chunk_size)).unwrap();
        fs.write_to_file(&mut handle, &test_data).unwrap();
        fs.close_file(handle).unwrap();

        let scrub_report = fs.scrub().unwrap();
        assert_eq!(scrub_report.data_left, 0);

        let cluster_report = &scrub_report.clusterization_report;
        assert_eq!(cluster_report.total_cluster_size, test_data.len());
        assert_eq!(cluster_report.number_of_clusters, test_data.len());
        assert!(cluster_report
            .number_of_vertices_in_cluster
            .values()
            .all(|&v| v == 1));
        assert!(cluster_report.distance_to_vertices_in_cluster.is_empty());
        assert!(cluster_report
            .distance_to_other_clusters
            .values()
            .all(|v| v.len() == test_data.len() - 1));
        assert!(cluster_report
            .cluster_dedup_ratio
            .values()
            .all(|&v| v == 0.0));
    }
}

