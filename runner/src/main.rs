extern crate sbc_algorithm;
use sbc_algorithm::{hash, SBCMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};

pub fn main() -> Result<(), std::io::Error> {
    let path = "files/test1.txt";
    let input = File::open(path)?;
    println!("size before chunking: {}", input.metadata().unwrap().len());

    let mut buffer = BufReader::new(input);
    let contents = fs::read(path).unwrap();
    let chunks = fastcdc::v2020::FastCDC::new(&contents, 1000, 2000, 65536);
    let mut cdc_vec = Vec::new();

    for chunk in chunks {
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes)?;

        cdc_vec.push((hash(bytes.as_slice()), bytes));
    }

    let _sbc_map = SBCMap::new();

    Ok(())
}
