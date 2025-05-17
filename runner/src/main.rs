mod bench;

extern crate chunkfs;
extern crate sbc_algorithm;

#[allow(unused_imports)]
use chunkfs::chunkers::{
    FSChunker, LeapChunker, RabinChunker, SeqChunker, SizeParams, SuperChunker, UltraChunker,
};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::FileSystem;
use sbc_algorithm::{clusterer, decoder, encoder, hasher};
use sbc_algorithm::{SBCMap, SBCScrubber};
use std::collections::HashMap;
use std::fs;
use std::io;

#[allow(dead_code)]
const MB: usize = 1024 * 1024;

#[allow(dead_code)]
fn generate_data(mb_size: usize) -> Vec<u8> {
    let bytes = mb_size * MB;
    (0..bytes).map(|_| rand::random::<u8>()).collect()
}

fn main() -> io::Result<()> {
    let data = fs::read("runner/files/my_data")?;
    let chunk_size = SizeParams::new(2 * 1024, 8 * 1024, 16 * 1024);
    let mut fs = FileSystem::new_with_scrubber(
        HashMap::default(),
        SBCMap::new(decoder::GdeltaDecoder::default()),
        Box::new(SBCScrubber::new(
            hasher::OdessHasher::default(),
            clusterer::EqClusterer,
            encoder::GdeltaEncoder::default(),
        )),
        Sha256Hasher::default(),
    );
    let mut handle = fs.create_file("file".to_string(), SuperChunker::new(chunk_size))?;

    fs.write_to_file(&mut handle, &data)?;
    fs.close_file(handle)?;
    let cdc_dedup_ratio = fs.cdc_dedup_ratio();
    let res = fs.scrub()?;
    let sbc_dedup_ratio = fs.total_dedup_ratio();
    println!("{}, {:?}, {}", cdc_dedup_ratio, res, sbc_dedup_ratio);

    let handle = fs.open_file_readonly("file".to_string());
    let read_data = fs.read_file_complete(&handle?)?;

    assert_eq!(data.len(), read_data.len());
    assert_eq!(data, read_data);
    Ok(())
}
