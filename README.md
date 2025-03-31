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

use chunkfs::chunkers::{SizeParams, SuperChunker};
use chunkfs::hashers::Sha256Hasher;
use chunkfs::FileSystem;
use sbc_algorithm::{SBCMap, SBCScrubber};
use sbc_algorithm::{decoder, encoder, hasher, clusterer};
use std::collections::HashMap;
use std::io;

fn main() -> io::Result<()> {
    let data = vec![10; 1024 * 1024];
    let chunk_size = SizeParams::new(2 * 1024, 8 * 1024, 16 * 1024);
    let mut fs = FileSystem::new_with_scrubber(
        HashMap::default(),
        SBCMap::new(decoder::GdeltaDecoder),
        Box::new(SBCScrubber::new(
            hasher::AronovichHasher,
            clusterer::Graph::new(),
            encoder::GdeltaEncoder,
        )),
        Sha256Hasher::default(),
    );
    let mut handle = fs.create_file("file".to_string(), SuperChunker::new(chunk_size))?;
    fs.write_to_file(&mut handle, &data)?;
    fs.close_file(handle)?;

    let read_handle = fs.open_file_readonly("file")?;
    let read = fs.read_file_complete(&read_handle)?;

    let cdc_dedup_ratio = fs.cdc_dedup_ratio();
    let res = fs.scrub().unwrap();
    let sbc_dedup_ratio = fs.total_dedup_ratio();
    println!("CDC dedup ratio: {}", cdc_dedup_ratio);
    println!("SBC dedup ratio: {}", cdc_dedup_ratio);
    println!("ScrubMeasure: {:?}", res);
    assert_eq!(read.len(), data.len());
    Ok(())
}
```
