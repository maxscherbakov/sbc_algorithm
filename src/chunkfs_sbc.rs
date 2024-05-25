use crate::graph::{find_leader_chunk_in_cluster, Graph};
use crate::{hash_function, levenshtein_functions, match_chunk, Chunk, SBCMap};
use chunkfs::{ChunkHash, Data, DataContainer, Database, Scrub, ScrubMeasurements};
use std::io;

impl Database<u32, Vec<u8>> for SBCMap {
    fn insert(&mut self, sbc_hash: u32, data: Vec<u8>) -> io::Result<()> {
        self.graph.add_vertex(sbc_hash.clone());

        let hash_leader = self.graph.vertices.get(&sbc_hash).unwrap().parent;

        if hash_leader == sbc_hash {
            self.sbc_hashmap.insert(sbc_hash, Chunk::Simple { data });
        } else {
            let chunk_data_1 = match_chunk(&self.sbc_hashmap, &hash_leader);

            self.sbc_hashmap.insert(
                sbc_hash,
                Chunk::Delta {
                    parent_hash: hash_leader,
                    delta_code: levenshtein_functions::encode(
                        chunk_data_1.as_slice(),
                        data.as_slice(),
                    ),
                },
            );
        }
        Ok(())
    }

    fn get(&self, sbc_hash: &u32) -> io::Result<Vec<u8>> {
        Ok(match_chunk(&self.sbc_hashmap, sbc_hash))
    }

    fn remove(&mut self, sbc_hash: &u32) {
        let parent_hash = self.graph.vertices.get(sbc_hash).unwrap().parent;

        if *sbc_hash == parent_hash {
            let mut cluster = Vec::new();
            for (hash, vertex) in &self.graph.vertices {
                if vertex.parent == parent_hash && *hash != *sbc_hash {
                    cluster.push(*hash);
                }
            }
            if !cluster.is_empty() {
                let new_parent =
                    find_leader_chunk_in_cluster(&self.sbc_hashmap, cluster.as_slice());
                let new_parent_data = match_chunk(&self.sbc_hashmap, &new_parent);
                self.sbc_hashmap.insert(
                    new_parent,
                    Chunk::Simple {
                        data: new_parent_data.clone(),
                    },
                );

                for hash in cluster {
                    if hash == new_parent {
                        continue;
                    }
                    let chunk_data = match_chunk(&self.sbc_hashmap, &hash);
                    self.sbc_hashmap.insert(
                        hash,
                        Chunk::Delta {
                            parent_hash: new_parent,
                            delta_code: levenshtein_functions::encode(
                                chunk_data.as_slice(),
                                new_parent_data.as_slice(),
                            ),
                        },
                    );

                    let vertex = self.graph.vertices.get_mut(&hash).unwrap();
                    vertex.parent = new_parent;
                }
            }
        }

        self.graph.vertices.remove(sbc_hash);
        self.sbc_hashmap.remove(sbc_hash);
    }

    fn insert_multi(&mut self, pairs: Vec<(u32, Vec<u8>)>) -> io::Result<()> {
        for (key, value) in pairs {
            self.sbc_hashmap.insert(
                key.clone(),
                Chunk::Simple {
                    data: value.clone(),
                },
            );
        }

        self.graph = Graph::new(&self.sbc_hashmap);
        self.encode();
        Ok(())
    }
}

pub struct SBCScrubber;

impl<Hash: ChunkHash, B> Scrub<Hash, B, u32> for SBCScrubber
where
    B: Database<Hash, DataContainer<u32>>,
    for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<u32>)>,
{
    fn scrub<'a>(
        &mut self,
        database: &mut B,
        sbc_map: &mut Box<dyn Database<u32, Vec<u8>>>,
    ) -> ScrubMeasurements
    where
        Hash: 'a,
    {
        let mut pairs = Vec::new();

        for (_, data_container) in database {
            let chunk = data_container.extract();
            match chunk {
                Data::Chunk(data) => {
                    let sbc_hash = hash_function::hash(data.as_slice());
                    pairs.push((sbc_hash, data.clone()));
                    data_container.make_target(vec![sbc_hash]);
                }
                Data::TargetChunk(_) => {}
            }
        }

        let _ = sbc_map.insert_multi(pairs);

        ScrubMeasurements::default()
    }
}
