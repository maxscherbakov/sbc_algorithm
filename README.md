## Similarity Based Chunking Scrubber
SBC Scrubber is a scrubber that can be used to implement different SBC algorithms with ChunkFS

SBC Scrubber is currently under active development, breaking changes can always happen.

## Usage

Add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
chunkfs = { version = "0.1", features = ["chunkers", "hashers"] }
sbc_algorithm = { git = "https://github.com/maxscherbakov/sbc_algorithm.git" }
```

## Example
	
```rust
extern crate chunkfs;
extern crate sbc_algorithm;

use chunkfs::chunkers::{FSChunker, RabinChunker, SizeParams, SuperChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::FileSystem;
use sbc_algorithm::{SBCMap, SBCScrubber};
use std::collections::HashMap;
use std::io;

fn main() -> io::Result<()> {
    let mut fs = FileSystem::new_with_scrubber(
        HashMap::default(),
        SBCMap::new(),
        Box::new(SBCScrubber::new()),
        Sha256Hasher::default(),
    );
    let chunk_size = SizeParams::new(2000, 12000, 16384);
    let mut handle = fs.create_file("file".to_string(), RabinChunker::new(chunk_size))?;
    let data = vec![10; 1024 * 1024];
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
```
