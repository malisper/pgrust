//! Ingest-time per-column NDV sketch (HyperLogLog, p=14: ~0.8% standard
//! error at 16KiB/column — exact tracking would need GB-scale sets at
//! analytics-bank text cardinalities, and planner NDV tolerates ~1% error).

use std::hash::Hasher;

const P: u32 = 14;
const M: usize = 1 << P;

pub struct Hll {
    regs: Box<[u8; M]>,
}

impl Default for Hll {
    fn default() -> Self {
        Hll { regs: vec![0u8; M].into_boxed_slice().try_into().unwrap() }
    }
}

impl Hll {
    pub fn add_hash(&mut self, h: u64) {
        let idx = (h >> (64 - P)) as usize;
        // rho over the low 64-P bits; +1 per the HLL definition, saturating
        // when those bits are all zero.
        let rho = ((h << P) | 1u64 << (P - 1)).leading_zeros() as u8 + 1;
        if rho > self.regs[idx] {
            self.regs[idx] = rho;
        }
    }

    pub fn add_i64(&mut self, v: i64) {
        self.add_hash(mix64(v as u64));
    }

    pub fn add_bytes(&mut self, b: &[u8]) {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        h.write(b);
        self.add_hash(h.finish());
    }

    /// Union with another sketch (parallel COPY: per-chunk worker sketches
    /// fold into the part sketch). Registers are per-bucket maxima, so a
    /// merge of per-chunk adds is REGISTER-IDENTICAL to the serial add
    /// sequence — the footer NDV estimate is byte-identical either way.
    pub fn merge(&mut self, other: &Hll) {
        for (r, &o) in self.regs.iter_mut().zip(other.regs.iter()) {
            if o > *r {
                *r = o;
            }
        }
    }

    /// Serialize for the v8 footer NDV-registers blob: 4-byte header
    /// (tag u8 | p u8 | reserved u16 LE zero), then registers. tag 1 =
    /// dense (2^p bytes), tag 2 = sparse (u32 n LE, then n x (u16 idx LE,
    /// u8 val), nonzero registers ascending by index). Sparse wins while
    /// 4 + 3n < 2^p; low-NDV columns serialize in tens of bytes.
    pub fn to_blob(&self) -> Vec<u8> {
        let nz = self.regs.iter().filter(|&&r| r != 0).count();
        if 4 + 3 * nz < M {
            let mut b = Vec::with_capacity(8 + 3 * nz);
            b.extend_from_slice(&[2, P as u8, 0, 0]);
            b.extend_from_slice(&(nz as u32).to_le_bytes());
            for (i, &r) in self.regs.iter().enumerate() {
                if r != 0 {
                    b.extend_from_slice(&(i as u16).to_le_bytes());
                    b.push(r);
                }
            }
            b
        } else {
            let mut b = Vec::with_capacity(4 + M);
            b.extend_from_slice(&[1, P as u8, 0, 0]);
            b.extend_from_slice(&self.regs[..]);
            b
        }
    }

    /// Parse a v8 NDV-registers blob. None on any unknown/mismatched shape
    /// (unknown tag, precision != P, truncated body): the caller treats the
    /// sketch as absent and falls down the v2-scalar/Duj1 ladder — a
    /// future-precision part degrades, never errors.
    pub fn from_blob(b: &[u8]) -> Option<Hll> {
        if b.len() < 4 || b[1] as u32 != P {
            return None;
        }
        let mut h = Hll::default();
        match b[0] {
            1 => {
                if b.len() != 4 + M {
                    return None;
                }
                h.regs.copy_from_slice(&b[4..4 + M]);
            }
            2 => {
                if b.len() < 8 {
                    return None;
                }
                let n = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
                if b.len() != 8 + 3 * n {
                    return None;
                }
                for e in 0..n {
                    let o = 8 + 3 * e;
                    let idx = u16::from_le_bytes(b[o..o + 2].try_into().unwrap()) as usize;
                    if idx >= M {
                        return None;
                    }
                    h.regs[idx] = b[o + 2];
                }
            }
            _ => return None,
        }
        Some(h)
    }

    pub fn estimate(&self) -> u64 {
        let m = M as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &r in self.regs.iter() {
            sum += (-(r as f64)).exp2();
            if r == 0 {
                zeros += 1;
            }
        }
        let e = alpha * m * m / sum;
        let est = if e <= 2.5 * m && zeros > 0 { m * (m / zeros as f64).ln() } else { e };
        est.round() as u64
    }
}

// splitmix64 finalizer.
pub(crate) fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_exact_band() {
        let mut h = Hll::default();
        for i in 0..1000i64 {
            h.add_i64(i);
        }
        let e = h.estimate() as i64;
        assert!((990..=1010).contains(&e), "est {e}");
    }

    #[test]
    fn large_error_band() {
        let mut h = Hll::default();
        for i in 0..5_000_000i64 {
            h.add_i64(i);
        }
        let e = h.estimate() as f64;
        assert!((e / 5_000_000.0 - 1.0).abs() < 0.03, "est {e}");
    }

    #[test]
    fn blob_roundtrip_sparse_and_dense() {
        // Sparse: few distincts, tiny blob, register-identical roundtrip.
        let mut s = Hll::default();
        for i in 0..50i64 {
            s.add_i64(i);
        }
        let sb = s.to_blob();
        assert_eq!(sb[0], 2, "expected sparse tag");
        assert!(sb.len() < 200, "sparse blob {}B", sb.len());
        assert_eq!(Hll::from_blob(&sb).unwrap().regs[..], s.regs[..]);

        // Dense: enough distincts to saturate past the sparse threshold.
        let mut d = Hll::default();
        for i in 0..100_000i64 {
            d.add_i64(i);
        }
        let db = d.to_blob();
        assert_eq!(db[0], 1, "expected dense tag");
        assert_eq!(db.len(), 4 + M);
        assert_eq!(Hll::from_blob(&db).unwrap().regs[..], d.regs[..]);

        // Continue-after-roundtrip == never-serialized (the reopen-append
        // contract): registers are order-insensitive maxima.
        let mut cont = Hll::from_blob(&db).unwrap();
        let mut straight = d;
        for i in 100_000..150_000i64 {
            cont.add_i64(i);
            straight.add_i64(i);
        }
        assert_eq!(cont.regs[..], straight.regs[..]);
    }

    #[test]
    fn blob_rejects_malformed() {
        assert!(Hll::from_blob(&[]).is_none());
        assert!(Hll::from_blob(&[1, P as u8, 0]).is_none()); // short header
        assert!(Hll::from_blob(&[3, P as u8, 0, 0]).is_none()); // unknown tag
        assert!(Hll::from_blob(&[1, 13, 0, 0]).is_none()); // precision mismatch
        assert!(Hll::from_blob(&[1, P as u8, 0, 0, 7]).is_none()); // truncated dense
        // Sparse with out-of-range index.
        let mut b = vec![2, P as u8, 0, 0];
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&(M as u16).to_le_bytes());
        b.push(1);
        assert!(Hll::from_blob(&b).is_none());
    }

    #[test]
    fn text_dedup() {
        let mut h = Hll::default();
        for i in 0..10_000u32 {
            h.add_bytes(format!("key-{}", i % 100).as_bytes());
        }
        let e = h.estimate();
        assert!((95..=105).contains(&e), "est {e}");
    }
}
