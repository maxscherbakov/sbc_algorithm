mod gdelta_encoder;
mod levenshtein_encoder;

use super::chunkfs_sbc::{ClusterPoint, Clusters};
use crate::decoder::Decoder;
use crate::{ChunkType, SBCHash, SBCKey, SBCMap};
use chunkfs::{Data, Database, IterableDatabase};
pub use gdelta_encoder::GdeltaEncoder;
pub(crate) use levenshtein_encoder::Action;
pub use levenshtein_encoder::LevenshteinEncoder;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::{Arc, Mutex, MutexGuard};

/// A trait for encoding data clusters using Similarity Based Chunking (SBC).
///
/// Implementors of this trait provide methods to efficiently encode data chunks
/// by creating delta codes relative to parent chunks in a hierarchy.
pub trait Encoder {
    /// Encodes a single cluster of data chunks relative to a parent hash.
    ///
    /// # Parameters
    /// - `target_map`: Mutable reference to the SBC structure tracking chunk relationships
    /// - `cluster`: Mutable slice of (hash, data container) tuples to encode
    /// - `parent_hash`: Identifier for the parent chunk used as delta reference
    ///
    /// # Returns
    /// A tuple containing:
    /// - `usize`: Amount of unprocessed data remaining in cluster
    /// - `usize`: Amount of data successfully processed and encoded
    fn encode_cluster<D: Decoder>(
        &self,
        target_map: Arc<Mutex<&mut SBCMap<D>>>,
        cluster: &mut [ClusterPoint],
        parent_hash: SBCHash,
    ) -> (usize, usize);

    /// Batch processes multiple clusters through the encoding pipeline.
    ///
    /// # Parameters
    /// - `clusters`: Mutable HashMap of parent hashes to their associated data clusters
    /// - `target_map`: Mutable reference to the SBC structure tracking relationships
    ///
    /// # Returns
    /// A tuple containing:
    /// - `usize`: Total unprocessed data across all clusters
    /// - `usize`: Total processed data across all clusters
    ///
    /// # Note
    /// Provides default implementation that iterates through all clusters,
    /// but can be overridden for optimized batch processing strategies.
    fn encode_clusters<D: Decoder + Send>(
        &self,
        clusters: &mut Clusters,
        target_map: &mut SBCMap<D>,
    ) -> (usize, usize)
    where
        Self: Sync,
    {
        let pool = ThreadPoolBuilder::new().num_threads(6).build().unwrap();

        let data_left = Mutex::new(0);
        let processed_data = Mutex::new(0);
        let target_map_ref = Arc::new(Mutex::new(target_map));
        pool.install(|| {
            clusters.par_iter_mut().for_each(|(parent_hash, cluster)| {
                let data_analyse = self.encode_cluster(
                    target_map_ref.clone(),
                    cluster.as_mut_slice(),
                    parent_hash.clone(),
                );

                let mut data_left_lock = data_left.lock().unwrap();
                *data_left_lock += data_analyse.0;

                let mut processed_data_lock = processed_data.lock().unwrap();
                *processed_data_lock += data_analyse.1;
            });
        });
        (
            data_left.into_inner().unwrap(),
            processed_data.into_inner().unwrap(),
        )
    }
}

fn count_delta_chunks_with_hash<D: Decoder>(
    target_map: &MutexGuard<&mut SBCMap<D>>,
    hash: &SBCHash,
) -> u16 {
    let count = target_map
        .iterator()
        .filter(|(sbc_key, _)| {
            sbc_key.hash == *hash
                && match sbc_key.chunk_type {
                    ChunkType::Delta {
                        parent_hash: _,
                        number: _,
                    } => true,
                    ChunkType::Simple => false,
                }
        })
        .count();
    count as u16
}

fn find_empty_cell<D: Decoder>(target_map: &MutexGuard<&mut SBCMap<D>>, hash: &SBCHash) -> SBCHash {
    let mut left = hash.clone();
    let mut right = next_hash(hash);
    loop {
        if target_map.contains(&SBCKey {
            hash: left.clone(),
            chunk_type: ChunkType::Simple,
        }) {
            left = last_hash(left);
        } else {
            return left;
        }

        if target_map.contains(&SBCKey {
            hash: right.clone(),
            chunk_type: ChunkType::Simple,
        }) {
            right = next_hash(&right);
        } else {
            return right;
        }
    }
}

fn last_hash(sbc_hash: SBCHash) -> SBCHash {
    match sbc_hash {
        SBCHash::Aronovich(key) => SBCHash::Aronovich(key.saturating_sub(1)),
        SBCHash::Broder(_) => todo!(),
    }
}

fn next_hash(sbc_hash: &SBCHash) -> SBCHash {
    match sbc_hash {
        SBCHash::Aronovich(key) => SBCHash::Aronovich(key.saturating_add(1)),
        SBCHash::Broder(_) => todo!(),
    }
}

fn encode_simple_chunk<D: Decoder>(
    target_map: &mut MutexGuard<&mut SBCMap<D>>,
    data: &[u8],
    hash: SBCHash,
) -> (usize, SBCKey) {
    let sbc_hash = SBCKey {
        hash: find_empty_cell(target_map, &hash),
        chunk_type: ChunkType::Simple,
    };

    let _ = target_map.insert(sbc_hash.clone(), data.to_vec());

    (data.len(), sbc_hash)
}

struct ParentChunkInCluster {
    index: i32,
    parent_data: Vec<u8>,
    data_left: usize,
}

fn get_parent_data<D: Decoder>(
    target_map: Arc<Mutex<&mut SBCMap<D>>>,
    parent_hash: SBCHash,
    cluster: &mut [ClusterPoint],
) -> ParentChunkInCluster {
    let mut target_map_lock = target_map.lock().unwrap();
    match target_map_lock.get(&SBCKey {
        hash: parent_hash.clone(),
        chunk_type: ChunkType::Simple,
    }) {
        Ok(parent_data) => ParentChunkInCluster {
            index: -1,
            parent_data,
            data_left: 0,
        },
        Err(_) => {
            let (_, parent_data_container) = &mut cluster[0];
            let parent_data = match parent_data_container.extract() {
                Data::Chunk(data) => data.clone(),
                Data::TargetChunk(_) => panic!(),
            };
            let (data_left, parent_sbc_hash) =
                encode_simple_chunk(&mut target_map_lock, parent_data.as_slice(), parent_hash);

            parent_data_container.make_target(vec![parent_sbc_hash]);
            ParentChunkInCluster {
                index: 0,
                parent_data,
                data_left,
            }
        }
    }
}
