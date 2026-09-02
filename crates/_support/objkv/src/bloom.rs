//! A plain bloom filter over run keys.
//!
//! This is what bounds how many sorted runs a cold read has to touch. Blooms
//! and indexes are small and stay cached locally, so a lookup consults every
//! run's filter in memory and then issues at most one ranged GET for real data.
//! At 10 bits per key the false-positive rate is roughly 1%.

pub const BITS_PER_KEY: usize = 10;
const HASHES: u32 = 7; // ~ BITS_PER_KEY * ln(2)

fn fnv1a64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

/// Kirsch-Mitzenmacher: derive k hashes from two, which is as accurate as k
/// independent hashes for this purpose.
fn probes(key: &[u8], bits: u64) -> impl Iterator<Item = u64> {
    let h1 = fnv1a64(key);
    let h2 = h1.rotate_left(31) | 1;
    (0..HASHES).map(move |i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % bits)
}

pub struct Bloom {
    bits: Vec<u8>,
}

impl Bloom {
    pub fn build(keys: &[&[u8]]) -> Bloom {
        let nbits = (keys.len() * BITS_PER_KEY).max(64);
        let mut bits = vec![0u8; nbits.div_ceil(8)];
        let nbits = (bits.len() * 8) as u64;
        for k in keys {
            for p in probes(k, nbits) {
                bits[(p / 8) as usize] |= 1 << (p % 8);
            }
        }
        Bloom { bits }
    }

    pub fn from_bytes(bits: Vec<u8>) -> Bloom {
        Bloom { bits }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    /// False positives are possible; false negatives are not.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return true;
        }
        let nbits = (self.bits.len() * 8) as u64;
        probes(key, nbits).all(|p| self.bits[(p / 8) as usize] & (1 << (p % 8)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("key{i:06}").into_bytes()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let b = Bloom::build(&refs);
        for k in &refs {
            assert!(b.may_contain(k), "false negative is impossible");
        }
    }

    #[test]
    fn false_positive_rate_is_near_one_percent() {
        let keys: Vec<Vec<u8>> = (0..2000).map(|i| format!("key{i:06}").into_bytes()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let b = Bloom::build(&refs);
        let fp = (0..10_000)
            .filter(|i| b.may_contain(format!("absent{i:06}").as_bytes()))
            .count();
        // Theory says ~1%; allow generous slack so this never flakes.
        assert!(fp < 400, "false positive rate too high: {fp}/10000");
    }

    #[test]
    fn survives_a_round_trip_through_bytes() {
        let keys: Vec<&[u8]> = vec![b"alpha", b"beta"];
        let b = Bloom::build(&keys);
        let b2 = Bloom::from_bytes(b.as_bytes().to_vec());
        assert!(b2.may_contain(b"alpha") && b2.may_contain(b"beta"));
    }
}
