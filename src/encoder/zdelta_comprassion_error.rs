use thiserror::Error;

#[derive(Debug, Error)]
pub enum ZdeltaCompressionError {
    #[error("Match encoding error: {0}")]
    MatchEncoding(#[from] MatchEncodingError),

    #[error("Data conversion error: {0}")]
    DataConversion(#[from] DataConversionError),

    #[error("IO/Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Pointer error: {0}")]
    Pointer(#[from] PointerError),
}

#[derive(Debug, Error)]
pub enum DataConversionError {
    #[error("Chunk too small: got {actual_size} bytes, need at least {required_size}")]
    ChunkTooSmall {
        actual_size: usize,
        required_size: usize,
    },
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Lock acquisition failed: {0}")]
    LockFailed(String),

    #[error("Data insertion failed: {0}")]
    InsertionFailed(String),
}

#[derive(Debug, Error, PartialEq)]
pub enum MatchEncodingError {
    #[error("Invalid match length {0} (allowed {1}-{2})")]
    InvalidLength(usize, usize, usize),

    #[error("Invalid parameter combination")]
    InvalidParameterCombination,

    #[error("Huffman encoding failed")]
    HuffmanEncodingFailed,

    #[error("Huffman book not initialized")]
    HuffmanBookNotInitialized,
}

/// Error type for pointer operations
#[derive(Debug, Error, PartialEq)]
pub enum PointerError {
    #[error("Match end position would cause overflow")]
    PositionOverflow,
}