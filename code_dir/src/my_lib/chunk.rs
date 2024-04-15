pub(crate) trait Chunk {
    fn decode(&self);

    fn get_data(&self) -> Vec<u8>;
    fn get_index(&self) -> usize;

    fn get_type(&self);

    fn size(&self) -> usize;
}
