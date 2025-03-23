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

const NUM_THREADS_FOR_HASHING: usize = 6;

pub type Clusters<'a> = HashMap<u32, Vec<(u32, &'a mut DataContainer<SBCHash>)>>;

impl<D: Decoder> Database<SBCHash, Vec<u8>> for SBCMap<D> {
    fn insert(&mut self, sbc_hash: SBCHash, chunk: Vec<u8>) -> io::Result<()> {
        self.sbc_hashmap.insert(sbc_hash, chunk);
        Ok(())
    }

    fn get(&self, sbc_hash: &SBCHash) -> io::Result<Vec<u8>> {
        let sbc_value = self
            .sbc_hashmap
            .get(sbc_hash)
            .ok_or(Error::new(ErrorKind::NotFound, "!"))?;
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

impl<Hash, B, D, E> Scrub<Hash, B, SBCHash, SBCMap<D>> for SBCScrubber<E>
where
    Hash: ChunkHash,
    for<'data> B:
        IterableDatabase<Hash, DataContainer<SBCHash>> + IntoParallelRefMutIterator<'data>,
    D: Decoder + Send,
    E: Encoder + Sync,
{
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        target_map: &mut SBCMap<D>,
    ) -> io::Result<ScrubMeasurements>
    where
        Hash: 'a,
    {
        let pool = ThreadPoolBuilder::new()
            .num_threads(NUM_THREADS_FOR_HASHING)
            .build()
            .unwrap();

        let clusters_ref: Mutex<Clusters> = Mutex::new(HashMap::default());

        let mut mut_refs_database: Vec<&mut DataContainer<SBCHash>> =
            database.values_mut().collect();

        let time_start = Instant::now();
        pool.install(|| {
            mut_refs_database.par_iter_mut().for_each(|data_container| {
                match data_container.extract() {
                    Data::Chunk(data) => {
                        let sbc_hash = hash_functions::sbc_hashing(data);
                        let parent_hash = self.graph.lock().unwrap().set_parent_vertex(sbc_hash);
                        let mut clusters_lock = clusters_ref.lock().unwrap();
                        let cluster = clusters_lock.entry(parent_hash).or_default();
                        cluster.push((sbc_hash, data_container));
                    }
                    Data::TargetChunk(_) => {
                        todo!()
                    }
                }
            });
        });
        let time_hashing = time_start.elapsed();
        println!("time for hashing: {time_hashing:?}");

        let (data_left, processed_data) = self
            .encoder
            .encode_clusters(&mut clusters_ref.into_inner().unwrap(), target_map);

        let running_time = time_start.elapsed();

        Ok(ScrubMeasurements {
            processed_data,
            running_time,
            data_left,
        })
    }
}
