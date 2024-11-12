extern crate chunkfs;
extern crate sbc_algorithm;

use chunkfs::chunkers::{SuperChunker, RabinChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::FileSystem;
use sbc_algorithm::{SBCMap, SBCScrubber};
use std::collections::HashMap;
use std::io;

#[allow(dead_code)]
const MB: usize = 1024 * 1024;

#[allow(dead_code)]
fn generate_data(mb_size: usize) -> Vec<u8> {
    let bytes = mb_size * MB;
    (0..bytes).map(|_| rand::random::<u8>()).collect()
}

fn main() -> io::Result<()> {
    let mut fs = FileSystem::new_with_scrubber(
        HashMap::default(),
        Box::new(SBCMap::new()),
        Box::new(SBCScrubber::new()),
        Sha256Hasher::default(),
    );
    let mut handle = fs.create_file("file".to_string(), SuperChunker::new(), true)?;
    let data = std::fs::read("runner/files/emails_test.csv")?;
    println!("data was read");
    fs.write_to_file(&mut handle, &data)?;
    fs.close_file(handle)?;
    println!("CDChunking complete");

    let res = fs.scrub().unwrap();
    println!("Scrubber results: {res:?}");

    let mut handle = fs.open_file("file", SuperChunker::new())?;
    let read = fs.read_file_complete(&mut handle)?;
    assert_eq!(read.len(), data.len());
    Ok(())
}
