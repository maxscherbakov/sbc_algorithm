use fastcdc::v2016::FastCDC;
use sbc_algorithm::{Map, SBCMap};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
const PATH: &str = "runner/files/test1.txt";

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
