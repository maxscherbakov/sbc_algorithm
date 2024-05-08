#[cfg(test)]
mod tests;
mod hash_function;
mod clusters;

use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read};
use crate::hash_function::hash;
use crate::clusters::chunk::Chunk;
use crate::clusters::chunk_with_full_code::ChunkWithFullCode;
use clusters::*;
use std::fs::File;
use std::rc::Rc;

fn main() -> Result<(), std::io::Error> {
    let path = "files/test1.txt";
    let input = File::open(path)?;
    println!("size before chunking: {}", input.metadata().unwrap().len());

    let mut buffer = BufReader::new(input);
    let contents = fs::read(path).unwrap();
    let chunks = fastcdc::v2020::FastCDC::new(&contents, 1000, 2000, 65536);
    let mut chunks_hashmap: HashMap<u32, Rc<dyn Chunk>> = HashMap::new();
    let mut vec_with_hash_for_file = Vec::new();

    for chunk in chunks {
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes)?;
        let chunk_hash = hash(bytes.as_slice());
        vec_with_hash_for_file.push(chunk_hash);

        chunks_hashmap.insert(chunk_hash, Rc::new(ChunkWithFullCode::new(bytes)));
    }

    encoding(&mut chunks_hashmap);
    let _ = decode(&chunks_hashmap, vec_with_hash_for_file);

    println!("size after chunking: {}", size_hashmap(&chunks_hashmap));
    Ok(())
}

