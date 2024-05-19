extern crate sbc_algorithm;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use sbc_algorithm::SBCMap;

pub fn main() -> Result<(), std::io::Error> {
    let path = "files/test1.txt";
    let input = File::open(path)?;
    println!("size before chunking: {}", input.metadata().unwrap().len());

    let mut buffer = BufReader::new(input);
    let contents = fs::read(path).unwrap();
    let chunks = fastcdc::v2020::FastCDC::new(&contents, 1000, 2000, 65536);
    let mut cdc_vec = Vec::new();

    let mut index = 0;
    for chunk in chunks {
        index += 1;
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes)?;
        cdc_vec.push((index, bytes));
    }

    let mut sbc_map = SBCMap::new(cdc_vec.clone());
    sbc_map.encode();

    Ok(())
}
