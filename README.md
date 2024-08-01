## Similarity Based Chunking Scrubber
SBC Scrubber is a scrubber that can be used to implement different SBC algorithms with ChunkFS

SBC Scrubber is currently under active development, breaking changes can always happen.

## Usage

Add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
chunkfs = { git = "https://github.com/Piletskii-Oleg/chunkfs.git", features = ["chunkers", "hashers"] }
sbc_algorithm = { git = "https://github.com/maxscherbakov/sbc_algorithm.git" }
```

## Example
	
```toml
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
    let data = vec![10; 1024 * 1024];
    fs.write_to_file(&mut handle, &data)?;
    fs.close_file(handle)?;

    let res = fs.scrub().unwrap();
    println!("{res:?}");

    let mut handle = fs.open_file("file", SuperChunker::new())?;
    let read = fs.read_file_complete(&mut handle)?;
    assert_eq!(read.len(), data.len());
    Ok(())
}
```
