#[cfg(test)]
mod test {
    extern crate sbc_algorithm;
    extern crate chunkfs;
    use sbc_algorithm::{SBCMap, SBCScrubber};
    use chunkfs::FileSystem;
    use std::collections::HashMap;
    use chunkfs::hashers::Sha256Hasher;
    use chunkfs::chunkers::SuperChunker;
    #[test]
    fn test_data_recovery() {
        let mut fs = FileSystem::new(HashMap::default(), Box::new(SBCMap::new()), Box::new(SBCScrubber::new()), Sha256Hasher::default());

        let mut handle = fs.create_file("file".to_string(), SuperChunker::new(), true).unwrap();
        let data = generate_data(4);
        fs.write_to_file(&mut handle, &data).unwrap();
        fs.close_file(handle).unwrap();

        let res = fs.scrub().unwrap();
        println!("{res:?}");

        let mut handle = fs.open_file("file", SuperChunker::new()).unwrap();
        let read = fs.read_file_complete(&mut handle).unwrap();
        assert_eq!(read, data);
    }

    const MB: usize = 1024 * 1024;

    fn generate_data(mb_size: usize) -> Vec<u8> {
        let bytes = mb_size * MB;
        (0..bytes).map(|_| rand::random::<u8>()).collect()
    }

}
