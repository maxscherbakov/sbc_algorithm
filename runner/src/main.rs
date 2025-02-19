extern crate chunkfs;
extern crate sbc_algorithm;

#[allow(unused_imports)]
use chunkfs::chunkers::{
    FSChunker, LeapChunker, RabinChunker, SeqChunker, SizeParams, SuperChunker, UltraChunker,
};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::{ChunkerRef, FileSystem};
use chunkfs::chunkers::seq::OperationMode;
use sbc_algorithm::decoders;
use sbc_algorithm::encoders;
use sbc_algorithm::{SBCMap, SBCScrubber};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::time::Instant;
#[allow(dead_code)]
const MB: usize = 1024 * 1024;

#[allow(dead_code)]
fn generate_data(mb_size: usize) -> Vec<u8> {
    let bytes = mb_size * MB;
    (0..bytes).map(|_| rand::random::<u8>()).collect()
}

fn main() -> io::Result<()> {
    let chunk_size = SizeParams::new(2 * 1024, 8 * 1024, 12 * 1024);
    let chunkers: Vec<(&str, ChunkerRef)> = vec![
        ("super", SuperChunker::new(chunk_size.clone()).into()),
        ("fs", FSChunker::default().into()),
        ("leap", LeapChunker::new(chunk_size.clone()).into()),
        ("rabin", RabinChunker::new(chunk_size.clone()).into()),
        (
            "seq",
            SeqChunker::new(OperationMode::Increasing, chunk_size.clone(), Default::default()).into(),
        ),
        ("ultra", UltraChunker::new(chunk_size.clone()).into()),
    ];
    let dataset_path = "runner/files/kernels.tar";
    let mut out_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open("out-sbc-kernels-gdelta1.csv")?;
    writeln!(
        out_file,
        "Chunker\tCDCReadTime(s)\tCDCWriteTime(s)\tCDCDedupRatio\tScrubTime\tSBCReadTime\tSBCDedupRatio"
    )?;

    for (chunker_name, chunker_ref) in chunkers {
        let data = std::fs::read(dataset_path)?;
        let mut fs = FileSystem::new_with_scrubber(
            HashMap::default(),
            SBCMap::new(decoders::GdeltaDecoder),
            Box::new(SBCScrubber::new(encoders::GdeltaEncoder)),
            Sha256Hasher::default(),
        );
        let mut handle = fs.create_file("file".to_string(), chunker_ref)?;
        let now = Instant::now();
        fs.write_to_file(&mut handle, &data)?;
        let cdc_write_time = now.elapsed();
        fs.close_file(handle)?;
        let cdc_dedup_ratio = fs.cdc_dedup_ratio();

        let handle = fs.open_file_readonly("file".to_string())?;
        let now = Instant::now();
        fs.read_file_complete(&handle)?;
        let cdc_read_time = now.elapsed();

        let res = fs.scrub().unwrap();
        let sbc_dedup_ratio = fs.total_dedup_ratio();

        let now = Instant::now();
        fs.read_file_complete(&handle)?;
        let sbc_read_time = now.elapsed();

        writeln!(
            out_file,
            "{}\t{:.10}\t{:.10}\t{:.10}\t{:.10}\t{:.10}\t{:.10}",
            chunker_name,
            cdc_read_time.as_secs_f64(),
            cdc_write_time.as_secs_f64(),
            cdc_dedup_ratio,
            res.running_time.as_secs_f64(),
            sbc_read_time.as_secs_f64(),
            sbc_dedup_ratio
        )?;
        fs.clear_file_system()?;
        println!("Scrubber results: {res:?}");
        println!("CDChunking complete, dedup_ratio: {}", cdc_dedup_ratio);
        println!("SBC dedup ratio: {}", sbc_dedup_ratio);
        println!("delta: {}", sbc_dedup_ratio - cdc_dedup_ratio);
    }
    Ok(())
}
