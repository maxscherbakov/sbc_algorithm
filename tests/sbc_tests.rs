use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use fastcdc::v2016::FastCDC;
use sbc_algorithm::{Map, SBCMap, Chunk};
const PATH : &str = "runner/files/test1.txt";

fn create_cdc_vec(input : File, chunks : FastCDC) -> Vec<(u64, Vec<u8>)>{
    let mut cdc_vec = Vec::new();
    let mut buffer = BufReader::new(input);

    for chunk in chunks {
        let length = chunk.length;
        let mut bytes = vec![0; length];
        buffer.read_exact(&mut bytes).expect("buffer crash");
        cdc_vec.push((chunk.hash, bytes));
    }
    cdc_vec
}

fn crate_sbc_map(path : &str) -> SBCMap {
    let contents = fs::read(path).unwrap();
    let chunks = FastCDC::new(&contents, 1000, 2000, 65536);
    let input = File::open(path).expect("File not open");

    let cdc_vec = create_cdc_vec(input, chunks);
    let mut sbc_map = SBCMap::new(cdc_vec);
    sbc_map.encode();
    sbc_map
}




#[test]
fn test_data_recovery() -> Result<(), std::io::Error> {
    let contents = fs::read(PATH).unwrap();
    let chunks = FastCDC::new(&contents, 1000, 2000, 65536);

    let input = File::open(PATH)?;
    let mut cdc_vec = Vec::new();
    let mut buffer = BufReader::new(input);

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

    for (cdc_hash, data) in cdc_vec {
        assert_eq!(data, sbc_map.get(cdc_hash))
    }

    Ok(())
}

#[test]
fn checking_for_simple_chunks() {
    let sbc_map = crate_sbc_map(PATH);
    let mut count_simple_chunk = 0;
    for (sbc_hash, chunk) in sbc_map.sbc_hashmap {
        match chunk {
            Chunk::Simple { .. } => {count_simple_chunk += 1}
            Chunk::Delta { .. } => {}
        }
    }
    assert!(count_simple_chunk > 0)
}

#[test]
fn checking_for_delta_chunks() {
    let path = "runner/files/test1.txt";
    let sbc_map = crate_sbc_map(path);
    let mut count_delta_chunk = 0;
    for (sbc_hash, chunk) in sbc_map.sbc_hashmap {
        match chunk {
            Chunk::Simple { .. } => {}
            Chunk::Delta { .. } => {count_delta_chunk += 1}
        }
    }
    assert!(count_delta_chunk > 0)
}