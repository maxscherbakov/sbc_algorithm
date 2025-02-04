extern crate chunkfs;
extern crate sbc_algorithm;

#[allow(unused_imports)]
use chunkfs::chunkers::{FSChunker, RabinChunker, SizeParams, SuperChunker};
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
        SBCMap::new(),
        Box::new(SBCScrubber::new()),
        Sha256Hasher::default(),
    );
    let chunk_size = SizeParams::new(2000, 12000, 16384);
    let mut handle = fs.create_file("file".to_string(), RabinChunker::new(chunk_size), true)?;
    let data = std::fs::read("runner/files/my_data")?;
    println!("data was read");
    fs.write_to_file(&mut handle, &data)?;
    fs.close_file(handle)?;
    let cdc_dedup_ratio = fs.cdc_dedup_ratio();
    println!("CDChunking complete, dedup_ratio: {}", cdc_dedup_ratio);

    let res = fs.scrub().unwrap();
    println!("Scrubber results: {res:?}");
    let sbc_dedup_ratio = data.len() as f64 / (res.data_left + res.processed_data) as f64;
    println!("SBC dedup ratio: {}", sbc_dedup_ratio);
    println!("delta: {}", sbc_dedup_ratio - cdc_dedup_ratio);

    let mut handle = fs.open_file("file", RabinChunker::default())?;
    let read = fs.read_file_complete(&mut handle)?;
    assert_eq!(read.len(), data.len());
    Ok(())
}
