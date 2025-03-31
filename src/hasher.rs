use std::hash;
pub use aronovich_hash::{AronovichHasher, AronovichHash};
mod aronovich_hash;

pub trait Hasher {
    type Hash: SBCHash;
    fn calculate_hash(&self, chunk_data: &[u8]) -> Self::Hash;

}


pub trait SBCHash: hash::Hash + Clone + Eq + PartialEq + Default {
    fn new(key: u32) -> Self;
    fn next_hash(&self) -> Self;

    fn last_hash(&self) -> Self;

    fn get_key_for_graph_clusterer(&self) -> u32;
}