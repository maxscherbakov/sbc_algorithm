use std::fs;
use std::io::{BufRead, BufReader};
mod my_lib;
use crate::my_lib::chunk::Chunk;
use crate::my_lib::chunk_with_delta_code::ChunkWithDeltaCode;
use crate::my_lib::chunk_with_full_code::ChunkWithFullCode;
use my_lib::*;

fn main() {
    let file = fs::File::open("test1.txt").expect("file not open");
    let buffer = BufReader::new(file);
    let mut chunks: Vec<&dyn Chunk> = Vec::new();
    let mut chunks_with_full_code: Vec<ChunkWithFullCode> = Vec::new();
    let mut chunk_index: usize = 0;
    for line in buffer.lines() {
        chunk_index += 1;
        let chunk_data: Vec<u8> = line.unwrap().bytes().collect();
        let chunk_size = chunk_data.len();
        let chunk = ChunkWithFullCode::new(chunk_index, chunk_size, chunk_data);
        chunks_with_full_code.push(chunk);
    }

    for chunk in &chunks_with_full_code {
        chunks.push(chunk);
    }

    let mut chunks_with_delta_code: Vec<ChunkWithDeltaCode> = Vec::new();
    encoding(chunks.as_mut_slice(), &mut chunks_with_delta_code);

    for chunk in chunks {
        chunk.decode();
        println!();
    }
}