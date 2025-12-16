use crate::encoder::GEAR;
use crate::hasher::{SBCHash, SBCHasher};
use std::hash::Hash;
#[derive(Default)]
pub struct OdessHash {
    hash: [u64; 3],
}

impl Hash for OdessHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl Clone for OdessHash {
    fn clone(&self) -> Self {
        OdessHash { hash: self.hash }
    }
}

impl Eq for OdessHash {}

impl PartialEq<Self> for OdessHash {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl SBCHash for OdessHash {
    fn new_with_u32(_: u32) -> Self {
        todo!()
    }

    fn next_hash(&self) -> Self {
        let mut odess_hash = self.clone();
        if odess_hash.hash[0] < u64::MAX {
            odess_hash.hash[0] += 1;
        } else if odess_hash.hash[1] < u64::MAX {
            odess_hash.hash[0] = 0;
            odess_hash.hash[1] += 1;
        } else if odess_hash.hash[2] < u64::MAX {
            odess_hash.hash[0] = 0;
            odess_hash.hash[1] = 0;
            odess_hash.hash[2] += 1;
        } else {
            odess_hash.hash = [u64::MAX; 3]
        }
        odess_hash
    }

    fn last_hash(&self) -> Self {
        let mut odess_hash = self.clone();
        if odess_hash.hash[0] > 0 {
            odess_hash.hash[0] -= 1;
        } else if odess_hash.hash[1] > 0 {
            odess_hash.hash[0] = u64::MAX;
            odess_hash.hash[1] -= 1;
        } else if odess_hash.hash[2] > 0 {
            odess_hash.hash[0] = u64::MAX;
            odess_hash.hash[1] = u64::MAX;
            odess_hash.hash[2] -= 1;
        } else {
            odess_hash.hash = [0u64; 3]
        }
        odess_hash
    }

    fn get_key_for_graph_clusterer(&self) -> u32 {
        todo!()
    }
}

/// Реализация метода Odess для вычисления признаков чанка
pub struct OdessHasher {
    sampling_rate: u64,
    linear_coeffs: [u64; 3],
}

impl SBCHasher for OdessHasher {
    type Hash = OdessHash;
    fn calculate_hash(&self, chunk: &[u8]) -> OdessHash {
        let mut features = [u64::MAX; 3];
        let mask = self.sampling_rate - 1;
        let mut fp = 0u64;

        for &byte in chunk {
            // Gear rolling hash: FP = (FP << 1) + Gear[byte]
            fp = (fp << 1).wrapping_add(GEAR[byte as usize]);

            // Content-defined sampling
            if fp & mask == 0 {
                for (i, feature) in features.iter_mut().enumerate() {
                    let transform = self.linear_coeffs[i]
                        .wrapping_mul(fp)
                        .wrapping_add(byte as u64)
                        % (1u64 << 32);
                    if *feature >= transform {
                        *feature = transform;
                    }
                }
            }
        }
        OdessHash { hash: features }
    }
}

impl Default for OdessHasher {
    fn default() -> Self {
        Self::new(7)
    }
}

impl OdessHasher {
    pub fn new(sampling_ratio: u32) -> Self {
        // Инициализация коэффициентов для линейных преобразований
        let linear_coeffs = [0x3f9c9a5d4e8a3b2a, 0x7d4f1b2c3a6e5d8c, 0x1a2b3c4d5e6f7a8b];

        OdessHasher {
            sampling_rate: 1u64 << sampling_ratio,
            linear_coeffs,
        }
    }
}
