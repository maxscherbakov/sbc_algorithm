pub(crate) struct Chunk<'a> {
    pub(crate) offset: usize,
    length: usize,
    pub(crate) data: &'a [u8],
}

impl Chunk<'_> {
    pub(crate) fn new(
        offset: usize,
        length: usize,
        data: &[u8],
    ) -> Chunk {
        Chunk {
            offset,
            length,
            data,
        }
    }

    pub(crate) fn get_data(&self) -> Vec<u8> {
        self.data.to_vec()
    }

    pub(crate) fn get_offset(&self) -> usize {
        self.offset
    }


    pub(crate) fn get_length(&self) -> usize {
        self.length
    }
}
