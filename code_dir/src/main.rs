mod my_lib;
use memmap2::Mmap;
use my_lib::chunk::Chunk;
use my_lib::*;
use std::fs::File;

fn main() {
    let path = "test/test1.txt";

    let input = File::open(path).expect("file not open");
    let memory_map = unsafe { Mmap::map(&input).expect("Failed to create memory map") };

    let mut chunks_with_full_code = Vec::new();
    let contents = std::fs::read(path).unwrap();
    let chunks = fastcdc::v2020::FastCDC::new(&contents, 1000, 2000, 65536);

    for chunk in chunks {
        println!("offset={} length={}", chunk.offset, chunk.length);
        chunks_with_full_code.push(Chunk::new(
            chunk.offset,
            chunk.length,
            &memory_map[chunk.offset..chunk.length + chunk.offset],
        ));
    }

    encode(chunks_with_full_code.as_mut_slice(), "test_out.chunks");
    decode::decode_file_with_chunks("test_out.chunks", "test_decode.txt")
}
