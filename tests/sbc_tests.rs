use chunkfs::Database;
use fastcdc::v2016::FastCDC;
use sbc_algorithm::{hash, SBCMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};

const PATH: &str = "runner/files/test1.txt";

#[test]
fn test_data_recovery() -> Result<(), std::io::Error> {
    let contents = fs::read(PATH).unwrap();
    let chunks = FastCDC::new(&contents, 1000, 2000, 65536);

    let input = File::open(PATH)?;
    let mut sbc_vec = Vec::new();
    let mut buffer = BufReader::new(input);

    for chunk in chunks {
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes)?;

        let sbc_hash = hash(bytes.as_slice());
        sbc_vec.push((sbc_hash, bytes));
    }

    let mut sbc_map = SBCMap::new(sbc_vec.clone());
    sbc_map.encode();

    for (sbc_hash, data) in sbc_vec.iter() {
        assert_eq!(*data, sbc_map.get(sbc_hash).unwrap())
    }
    let text = "qwerty";
    let new_chunk = text.as_bytes().to_vec();
    let new_chunk_hash = hash(new_chunk.as_slice());
    let _ = sbc_map.insert(new_chunk_hash, new_chunk.clone());
    assert_eq!(new_chunk, sbc_map.get(&new_chunk_hash).unwrap());

    sbc_map.remove(&new_chunk_hash);
    for (cdc_hash, data) in sbc_vec {
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

    let mut pairs = Vec::new();

    for chunk in cdc_map {
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes)?;
        let sbc_hash = hash(bytes.as_slice());
        pairs.push((sbc_hash, bytes));
    }

    let mut sbc_map = SBCMap::new(Vec::new());
    let _ = sbc_map.insert_multi(pairs.clone());

    for (key, value) in pairs {
        assert_eq!(value, sbc_map.get(&key).unwrap())
    }

    Ok(())
}
