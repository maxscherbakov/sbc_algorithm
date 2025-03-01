use crate::decoders::Decoder;
use crate::encoders::Encoder;
use crate::graph::Graph;
use crate::{hash_functions, ChunkType, SBCHash, SBCMap};
use chunkfs::{
    ChunkHash, Data, DataContainer, Database, IterableDatabase, Scrub, ScrubMeasurements,
};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::io;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const NUM_THREADS_FOR_HASHING: usize = 4;

type ClusterType<'a> = HashMap<u32, Vec<(u32, &'a mut DataContainer<SBCHash>)>>;

impl<D: Decoder> Database<SBCHash, Vec<u8>> for SBCMap<D> {
    fn insert(&mut self, sbc_hash: SBCHash, chunk: Vec<u8>) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, chunk);
        Ok(())
    }

    fn get(&self, sbc_hash: &SBCHash) -> io::Result<Vec<u8>> {
        let sbc_value = match self.sbc_hashmap.get(sbc_hash) {
            None => return Err(Error::new(ErrorKind::NotFound, "!")),
            Some(data) => data,
        };

        let chunk = match sbc_hash.chunk_type {
            ChunkType::Simple {} => sbc_value.clone(),
            ChunkType::Delta(_) => {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&sbc_value[..4]);

                let parent_hash = u32::from_be_bytes(buf);
                let parent_data = self
                    .get(&SBCHash {
                        key: parent_hash,
                        chunk_type: ChunkType::Simple,
                    })
                    .unwrap();

                self.decoder.decode_chunk(parent_data, sbc_value.as_slice())
            }
        };
        Ok(chunk)
    }

    // fn remove(&mut self, sbc_hash: &SBCHash) {
    //     self.sbc_hashmap.remove(sbc_hash);
    // }

    fn contains(&self, key: &SBCHash) -> bool {
        self.sbc_hashmap.contains_key(key)
    }
}

impl<D: Decoder> IterableDatabase<SBCHash, Vec<u8>> for SBCMap<D> {
    fn iterator(&self) -> Box<dyn Iterator<Item = (&SBCHash, &Vec<u8>)> + '_> {
        Box::new(self.sbc_hashmap.iter())
    }
    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item = (&SBCHash, &mut Vec<u8>)> + '_> {
        Box::new(self.sbc_hashmap.iter_mut())
    }

    fn clear(&mut self) -> io::Result<()> {
        HashMap::clear(&mut self.sbc_hashmap);
        Ok(())
    }
}

pub struct SBCScrubber<E>
where
    E: Encoder,
{
    graph: Arc<Mutex<Graph>>,
    encoder: E,
}

impl<E: Encoder> SBCScrubber<E> {
    pub fn new(_encoder: E) -> SBCScrubber<E> {
        SBCScrubber {
            graph: Arc::new(Mutex::new(Graph::new())),
            encoder: _encoder,
        }
    }
}

impl<Hash: ChunkHash, B, D: Decoder, E: Encoder> Scrub<Hash, B, SBCHash, SBCMap<D>>
    for SBCScrubber<E>
where
    for<'data> B:
        IterableDatabase<Hash, DataContainer<SBCHash>> + IntoParallelRefMutIterator<'data> + 'data,
{
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        target_map: &mut SBCMap<D>,
    ) -> io::Result<ScrubMeasurements>
    where
        Hash: 'a,
    {
        let mut processed_data = 0;
        let mut data_left = 0;
        let pool = ThreadPoolBuilder::new()
            .num_threads(NUM_THREADS_FOR_HASHING)
            .build()
            .unwrap();
        let clusters: Arc<Mutex<ClusterType>> = Arc::new(Mutex::new(HashMap::new()));

        let mut mut_refs_database: Vec<&mut DataContainer<SBCHash>> =
            database.iterator_mut().map(|(_, b)| b).collect();

        let time_start = Instant::now();
        pool.install(|| {
            mut_refs_database.par_iter_mut().for_each(|data_container| {
                match data_container.extract() {
                    Data::Chunk(data) => {
                        let sbc_hash = hash_functions::sbc_hashing(data.as_slice());
                        let parent_hash = self.graph.lock().unwrap().add_vertex(sbc_hash);
                        let mut clusters_lock = clusters.lock().unwrap();
                        let cluster = clusters_lock.entry(parent_hash).or_default();
                        cluster.push((sbc_hash, data_container));
                    }
                    Data::TargetChunk(_) => {
                        panic!()
                    }
                }
            });
        });
        let time_hashing = time_start.elapsed();
        println!("time for hashing: {time_hashing:?}");

        let (clusters_data_left, clusters_processed_data) = self
            .encoder
            .encode_clusters(&mut clusters.lock().unwrap(), target_map);

        data_left += clusters_data_left;
        processed_data += clusters_processed_data;
        let running_time = time_start.elapsed();

        Ok(ScrubMeasurements {
            processed_data,
            running_time,
            data_left,
        })
    }
}
