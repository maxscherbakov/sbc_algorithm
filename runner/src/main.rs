extern crate sbc_algorithm;
extern crate chunkfs;

use sbc_algorithm::{SBCMap, SBCScrubber};
use std::io;
use chunkfs::FileSystem;
use std::collections::HashMap;
use chunkfs::hashers::Sha256Hasher;
use chunkfs::chunkers::SuperChunker;


fn main() -> io::Result<()> {
    let mut fs = FileSystem::new(HashMap::default(), Box::new(SBCMap::new()), Box::new(SBCScrubber::new()), Sha256Hasher::default());

    let mut handle = fs.create_file("file".to_string(), SuperChunker::new(), true)?;
    let data = generate_data(4);
    fs.write_to_file(&mut handle, &data)?;
    fs.close_file(handle)?;

    let res = fs.scrub().unwrap();
    println!("{res:?}");

    let mut handle = fs.open_file("file", SuperChunker::new())?;
    let read = fs.read_file_complete(&mut handle)?;
    assert_eq!(read.len(), data.len());
    Ok(())
}

const MB: usize = 1024 * 1024;

fn generate_data(mb_size: usize) -> Vec<u8> {
    let bytes = mb_size * MB;
    (0..bytes).map(|_| rand::random::<u8>()).collect()
}

