use crate::clusterer::Clusterer;
use crate::decoder::Decoder;
use crate::encoder::Encoder;
use crate::hasher::Hasher;
use crate::{ChunkType, SBCHash, SBCKey, SBCMap};
use chunkfs::{
    ChunkHash, Data, DataContainer, Database, IterableDatabase, Scrub, ScrubMeasurements,
};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::io;
use std::io::{Error, ErrorKind};
use std::sync::Mutex;
use std::time::Instant;

const NUM_THREADS_FOR_HASHING: usize = 6;

pub type ClusterPoint<'a, Hash> = (Hash, &'a mut &'a mut DataContainer<SBCKey<Hash>>);
pub type Clusters<'a, Hash> = HashMap<Hash, Vec<ClusterPoint<'a, Hash>>>;

impl<D: Decoder, Hash: SBCHash> Database<SBCKey<Hash>, Vec<u8>> for SBCMap<D, Hash> {
    fn insert(&mut self, sbc_hash: SBCKey<Hash>, chunk: Vec<u8>) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, chunk);
        Ok(())
    }

    fn get(&self, sbc_hash: &SBCKey<Hash>) -> io::Result<Vec<u8>> {
        let sbc_value = self
            .sbc_hashmap
            .get(sbc_hash)
            .ok_or(Error::new(ErrorKind::NotFound, "!"))?;

        let chunk = match &sbc_hash.chunk_type {
            ChunkType::Simple {} => sbc_value.clone(),
            ChunkType::Delta {
                parent_hash,
                number: _,
            } => {
                let parent_data = self
                    .get(&SBCKey {
                        hash: parent_hash.clone(),
                        chunk_type: ChunkType::Simple,
                    })
                    .unwrap();

                self.decoder.decode_chunk(parent_data, sbc_value.as_slice())
            }
        };
        Ok(chunk)
    }

    fn contains(&self, key: &SBCKey<Hash>) -> bool {
        self.sbc_hashmap.contains_key(key)
    }
}

impl<D: Decoder, Hash: SBCHash> IterableDatabase<SBCKey<Hash>, Vec<u8>> for SBCMap<D, Hash> {
    fn iterator(&self) -> Box<dyn Iterator<Item = (&SBCKey<Hash>, &Vec<u8>)> + '_> {
        Box::new(self.sbc_hashmap.iter())
    }
    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&SBCKey<Hash>, &mut Vec<u8>)> + '_> {
        Box::new(self.sbc_hashmap.iter_mut())
    }

    fn clear(&mut self) -> io::Result<()> {
        HashMap::clear(&mut self.sbc_hashmap);
        Ok(())
    }
}

pub struct SBCScrubber<Hash, H, C, E>
where
    Hash: SBCHash,
    H: Hasher<Hash = Hash>,
    C: Clusterer<Hash>,
    E: Encoder,
{
    hasher: H,
    clusterer: C,
    encoder: E,
}

impl<Hash, H, C, E> SBCScrubber<Hash, H, C, E>
where
    Hash: SBCHash,
    H: Hasher<Hash = Hash>,
    C: Clusterer<Hash>,
    E: Encoder,
{
    pub fn new(hasher: H, clusterer: C, encoder: E) -> SBCScrubber<Hash, H, C, E> {
        SBCScrubber {
            hasher,
            clusterer,
            encoder,
        }
    }
}

impl<CDCHash, B, D, H, C, E, Hash> Scrub<CDCHash, B, SBCKey<Hash>, SBCMap<D, Hash>>
    for SBCScrubber<Hash, H, C, E>
where
    CDCHash: ChunkHash,
    for<'data> B:
        IterableDatabase<CDCHash, DataContainer<SBCKey<Hash>>> + IntoParallelRefMutIterator<'data>,
    H: Hasher<Hash = Hash> + Sync,
    C: Clusterer<Hash>,
    D: Decoder + Send,
    E: Encoder + Sync,
    Hash: SBCHash,
{
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        target_map: &mut SBCMap<D, Hash>,
    ) -> io::Result<ScrubMeasurements>
    where
        CDCHash: 'a,
    {
        let pool = ThreadPoolBuilder::new()
            .num_threads(NUM_THREADS_FOR_HASHING)
            .build()
            .unwrap();

        let mut mut_refs_database: Vec<_> = database.values_mut().collect();
        let chunk_sbc_hash: Mutex<Vec<_>> = Mutex::default();

        // 1. hashing
        let time_start = Instant::now();
        pool.install(|| {
            mut_refs_database.par_iter_mut().for_each(|data_container| {
                match data_container.extract() {
                    Data::Chunk(data) => {
                        let sbc_hash = self.hasher.calculate_hash(data.as_slice());
                        let mut chunk_sbc_hash_lock = chunk_sbc_hash.lock().unwrap();
                        chunk_sbc_hash_lock.push((sbc_hash, data_container));
                    }
                    Data::TargetChunk(_) => {
                        todo!()
                    }
                }
            });
        });
        let time_hashing = time_start.elapsed();
        println!("time for hashing: {time_hashing:?}");

        // 2. clusterize
        let time_clusterize_start = time_start.elapsed();
        let mut clusters = self
            .clusterer
            .clusterize(chunk_sbc_hash.into_inner().unwrap());
        let time_clusterize = time_start.elapsed() - time_clusterize_start;
        println!("time for clusterize: {time_clusterize:?}");

        // 3. encode
        let time_encode_start = time_start.elapsed();
        let (data_left, processed_data) = self.encoder.encode_clusters(&mut clusters, target_map);
        let time_encode = time_start.elapsed() - time_encode_start;
        println!("time for encode: {time_encode:?}");

        let running_time = time_start.elapsed();

        Ok(ScrubMeasurements {
            processed_data,
            running_time,
            data_left,
        })
    }
}
