use fastcdc::v2016::FastCDC;
use sbc_algorithm::{SBCMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use chunkfs::{Database};

const PATH: &str = "runner/files/test1.txt";

#[test]
fn test_data_recovery() -> Result<(), std::io::Error> {
    let contents = fs::read(PATH).unwrap();
    let chunks = FastCDC::new(&contents, 1000, 2000, 65536);

    let input = File::open(PATH)?;
    let mut cdc_vec : Vec<(u64, Vec<u8>)> = Vec::new();
    let mut buffer = BufReader::new(input);

    let mut index = 1;
    for chunk in chunks {
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes)?;
        cdc_vec.push((index, bytes));

        index += 1;
    }

    let mut sbc_map = SBCMap::new(cdc_vec.clone());
    sbc_map.encode();

    for (cdc_hash, data) in cdc_vec.iter() {
        assert_eq!(*data, sbc_map.get(cdc_hash).unwrap())
    }
    let text = "qwerty";
    let new_chunk = text.as_bytes().to_vec();
    let _ = sbc_map.insert(index, new_chunk.clone());
    assert_eq!(new_chunk, sbc_map.get(&index).unwrap());

    sbc_map.remove(&index);
    for (cdc_hash, data) in cdc_vec {
        assert_eq!(data, sbc_map.get(&cdc_hash).unwrap())
    }

    Ok(())
}

#[test]
fn test_insert_multi() -> Result<(), std::io::Error> {
    let contents = fs::read(PATH).unwrap();
    let cdc_map = FastCDC::new(&contents, 1000, 2000, 65536);

    let input = File::open(PATH)?;
    let mut buffer = BufReader::new(input);

    let mut keys = Vec::new();
    let mut values = Vec::new();

    let mut index = 0;
    for chunk in cdc_map {
        index += 1;
        keys.push(index);
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes)?;
        values.push(bytes);
    }

    let mut sbc_map = SBCMap::new(Vec::new());
    let _ = sbc_map.insert_multi(keys.clone(), values.clone());


    for chunk_index in 0..keys.len() {
        assert_eq!(values[chunk_index], sbc_map.get(&keys[chunk_index]).unwrap())
    }

    Ok(())
}
