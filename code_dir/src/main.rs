use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read};
mod my_lib;
use crate::hash_function::hash;
use crate::my_lib::chunk::Chunk;
use crate::my_lib::chunk_with_full_code::ChunkWithFullCode;
use my_lib::*;

mod hash_function;
#[cfg(test)]
mod tests;
use std::fs::File;

use std::rc::Rc;

fn main() -> Result<(), std::io::Error> {
    let path = "test/test1.txt";
    let input = File::open(path)?;
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

    Ok(())
}

