use crate::SBCHash;
pub use aronovich_hash::AronovichHasher;
mod aronovich_hash;

pub trait Hasher {
    fn calculate_hash(&self, chunk_data: &[u8]) -> SBCHash;
}
