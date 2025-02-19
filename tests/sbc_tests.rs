#[cfg(test)]
mod test {
    extern crate chunkfs;
    extern crate sbc_algorithm;
    use chunkfs::chunkers::SuperChunker;
    use chunkfs::hashers::Sha256Hasher;
    use chunkfs::FileSystem;
    use sbc_algorithm::encoders::LevenshteinEncoder;
    use sbc_algorithm::{SBCMap, SBCScrubber};
    use std::collections::HashMap;
    use sbc_algorithm::decoders::LevenshteinDecoder;

    #[test]
    fn test_data_recovery() {
        let mut fs = FileSystem::new_with_scrubber(
            HashMap::default(),
            SBCMap::new(LevenshteinDecoder),
            Box::new(SBCScrubber::new(LevenshteinEncoder)),
            Sha256Hasher::default(),
        );
        let mut handle = fs
            .create_file("file".to_string(), SuperChunker::default())
            .unwrap();
        let data = generate_data(8);
        fs.write_to_file(&mut handle, &data).unwrap();
        fs.close_file(handle).unwrap();

        let _res = fs.scrub().unwrap();

        let mut handle = fs.open_file("file", SuperChunker::default()).unwrap();
        let read = fs.read_file_complete(&mut handle).unwrap();
        assert_eq!(read, data);
    }

    const MB: usize = 1024 * 1024;

    fn generate_data(mb_size: usize) -> Vec<u8> {
        let bytes = mb_size * MB;
        (0..bytes).map(|_| rand::random::<u8>()).collect()
    }
}
