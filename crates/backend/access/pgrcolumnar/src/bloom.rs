//! Per-granule bloom filters for equality pruning on unclustered high-NDV
//! int columns (format v3, CHUNK_FLAG_BLOOM). Pruning is advisory-only: a
//! "maybe present" verdict falls through to the ordinary scan, so a false
//! positive costs time, never rows.

use crate::hll::mix64;

// m/n = 16 bits per key at GRANULE_ROWS distinct keys, k = 8:
// fp = (1 - e^(-1/2))^8 ~= 5.7e-4.
pub const BLOOM_BYTES: usize = 16_384;
pub const BLOOM_K: u32 = 8;
const BLOOM_BITS: u64 = (BLOOM_BYTES as u64) * 8;

// Kirsch-Mitzenmacher double hashing over the splitmix64-mixed value.
#[inline]
fn hashes(v: i64) -> (u64, u64) {
    let h = mix64(v as u64);
    (h, (h >> 32) | 1)
}

pub fn bloom_insert(buf: &mut [u8], v: i64) {
    debug_assert_eq!(buf.len(), BLOOM_BYTES);
    let (mut x, h2) = hashes(v);
    for _ in 0..BLOOM_K {
        let bit = x % BLOOM_BITS;
        buf[(bit / 8) as usize] |= 1 << (bit % 8);
        x = x.wrapping_add(h2);
    }
}

pub fn bloom_may_contain(buf: &[u8], v: i64) -> bool {
    debug_assert_eq!(buf.len(), BLOOM_BYTES);
    let (mut x, h2) = hashes(v);
    for _ in 0..BLOOM_K {
        let bit = x % BLOOM_BITS;
        if buf[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
            return false;
        }
        x = x.wrapping_add(h2);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filled(keys: impl Iterator<Item = i64>) -> Vec<u8> {
        let mut buf = vec![0u8; BLOOM_BYTES];
        for k in keys {
            bloom_insert(&mut buf, k);
        }
        buf
    }

    #[test]
    fn no_false_negatives() {
        let keys: Vec<i64> =
            (0..8192u64).map(|i| i.wrapping_mul(0x9e37_79b9_7f4a_7c15) as i64 ^ i as i64).collect();
        let buf = filled(keys.iter().copied());
        for &k in &keys {
            assert!(bloom_may_contain(&buf, k));
        }
    }

    #[test]
    fn fp_rate_band() {
        let buf = filled((0..8192).map(|i| i * 3));
        let mut fp = 0u32;
        let probes = 100_000;
        for i in 0..probes {
            let v = 1_000_000_000 + i;
            if bloom_may_contain(&buf, v) {
                fp += 1;
            }
        }
        // Expected ~57 at 5.7e-4; generous band against hash unluckiness.
        assert!(fp < 500, "fp {fp}/{probes}");
    }

    #[test]
    fn adversarial_collision_exists_and_is_admitting_only() {
        // A false positive must exist and must read as "maybe present" —
        // the scan then falls through and finds no matching rows. Absence
        // of the value from the key set is what makes it adversarial.
        let keys: Vec<i64> = (0..8192).collect();
        let buf = filled(keys.iter().copied());
        let mut collision = None;
        for v in 10_000..100_000_000i64 {
            if bloom_may_contain(&buf, v) {
                collision = Some(v);
                break;
            }
        }
        let v = collision.expect("no bloom collision found in probe range");
        assert!(!keys.contains(&v));
        assert!(bloom_may_contain(&buf, v));
    }
}
