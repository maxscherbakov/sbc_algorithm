use chunkfs::{Scrub, ScrubMeasurements, DataContainer, Database, ChunkHash};
use crate::{Chunk, hash_function, levenshtein_functions, match_chunk, SBCMap};
use std::io;
use crate::graph::{find_leader_chunk_in_cluster, Graph};




impl<Hash: ChunkHash> Database<Hash, Vec<u8>> for SBCMap<Hash> {
    fn insert(&mut self, cdc_hash: Hash, data: Vec<u8>) -> io::Result<()> {

        let sbc_hash = hash_function::hash(data.as_slice());

        self.hashmap_transitions.insert(cdc_hash, sbc_hash);
        self.graph.add_vertex(sbc_hash);

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

    fn get(&self, cdc_hash: &Hash) -> io::Result<Vec<u8>> {
        let sbc_hash = self.hashmap_transitions.get(&cdc_hash).unwrap();
        Ok(match_chunk(&self.sbc_hashmap, sbc_hash))
    }

    fn remove(&mut self, cdc_hash: &Hash) {
        let sbc_hash = self.hashmap_transitions.get(cdc_hash).unwrap();
        let parent_hash = self.graph.vertices.get(sbc_hash).unwrap().parent;

        if *sbc_hash == parent_hash {
            let mut cluster = Vec::new();
            for (hash, vertex) in &self.graph.vertices {
                if vertex.parent == parent_hash && *hash != parent_hash {
                    cluster.push(*hash);
                }
            }
            let new_parent = find_leader_chunk_in_cluster(&self.sbc_hashmap, cluster.as_slice());
            let new_parent_data = match_chunk(&self.sbc_hashmap, &new_parent);
            self.sbc_hashmap.insert(new_parent, Chunk::Simple { data : new_parent_data.clone()});

            for hash in cluster {
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

        self.graph.vertices.remove(sbc_hash);
        self.sbc_hashmap.remove(sbc_hash);
        self.hashmap_transitions.remove(cdc_hash);
    }

    fn insert_multi(&mut self, keys: Vec<Hash>, values: Vec<Vec<u8>>) -> io::Result<()> {
        for chunk_index in 0..keys.len() {
            let sbc_hash = hash_function::hash(values[chunk_index].as_slice());
            self.hashmap_transitions.insert(keys[chunk_index].clone(), sbc_hash);
            self.sbc_hashmap.insert(sbc_hash, Chunk::Simple { data: values[chunk_index].clone() });
        }

        self.graph = Graph::new(&self.sbc_hashmap);
        self.encode();
        Ok(())
    }

}

pub struct SBCScrubber;

impl<Hash: ChunkHash, B> Scrub<Hash, Hash, B> for SBCScrubber
    where
        B: Database<Hash, DataContainer<Hash>>,
        for<'a> &'a mut B: IntoIterator<Item = (&'a Hash, &'a mut DataContainer<Hash>)>,
{
    fn scrub<'a>(
        &mut self,
        cdc_map: <&'a mut B as IntoIterator>::IntoIter,
        sbc_map: &mut Box<dyn Database<Hash, Vec<u8>>>,
    ) -> ScrubMeasurements
        where
            Hash: 'a,
    {
        let mut keys = Vec::new();
        let mut values = Vec::new();

        for (cdc_hash, chunk) in cdc_map {
            keys.push(cdc_hash.clone());
            let chunk = chunk.extract();
            match chunk {
                Data::Chunk(data) => { values.push(data.clone()); }
                Data::TargetChunk(_) => { values.push(Vec::new()); }
            }
        }

        let _ = sbc_map.insert_multi(keys, values);

        ScrubMeasurements::default()
    }
}

