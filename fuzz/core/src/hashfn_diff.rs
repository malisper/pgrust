//! hashfn_diff: differential fuzz driver — shipped Rust `hashfn` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_hashfn_io.c). Crate under test: crates/common/hashfn.
//!
//! Comparison planes: value bits (exact u32/u64). There is no error plane:
//! neither side allocates or raises (pg_diff_errcode asserted 0 anyway).
//!
//! Input layout: [selector][align][payload]; selector % 12 picks the arm,
//! align % 4 steers the BYTE ALIGNMENT of the key pointer handed to C —
//! the vendored hash_bytes has distinct aligned and unaligned word-fetch
//! paths, and the Rust crate's claim is that one little-endian path equals
//! both; the align byte makes BOTH C paths live under one driver.
//!   0  hash_bytes                key = payload            -> u32
//!   1  hash_bytes_extended       seed = first 8 LE bytes, key = rest -> u64
//!   2  hash_bytes_uint32         k = first 4 LE bytes     -> u32
//!   3  hash_bytes_uint32_extended k u32, seed u64         -> u64
//!   4  string_hash               keysize = first 8 LE bytes (folded),
//!                                key = rest + guaranteed NUL sentinel -> u32
//!   5  tag_hash                  keysize = fold into [0, len], key -> u32
//!   6  uint32_hash               k = first 4 LE bytes     -> u32
//!   7  hash_combine              a, b u32s                -> u32
//!   8  hash_combine64            a, b u64s                -> u64
//!   9  murmurhash32              h u32 (+ Rust-only inverse roundtrip)
//!   10 murmurhash64              h u64                    -> u64
//!   11 rotate_high_and_low_32bits v u64                   -> u64
//!
//! string_hash NUL contract: C strlen()s the key, so the driver always
//! appends a NUL sentinel to the buffer and hands Rust the same
//! sentinel-terminated slice — interior NULs (when present in the payload)
//! and the sentinel then terminate both sides identically. keysize runs the
//! full usize plane including the keysize=0 wrap (C `keysize - 1` on
//! unsigned Size == usize::MAX cap; Rust wrapping_sub — same fold).
//!
//! FC-WRAPPER PLANE: not applicable — crates/common/hashfn has no
//! builtins.rs / fc_* surface (non-SQL common helper crate; the C
//! Datum-returning hash_any* wrappers are equally not ported).
//!
//! SKIPPED: murmurhash32_inverse has NO C counterpart (pgrust-only helper);
//! it is checked here as a roundtrip property on arm 9 and carries a
//! full-u32 Kani bijection proof (proofs/hashfn) as its verification of
//! record.

#![allow(dead_code)]

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;

    fn pg_diff_hash_bytes(k: *const u8, keylen: i32) -> u32;
    fn pg_diff_hash_bytes_extended(k: *const u8, keylen: i32, seed: u64) -> u64;
    fn pg_diff_hash_bytes_uint32(k: u32) -> u32;
    fn pg_diff_hash_bytes_uint32_extended(k: u32, seed: u64) -> u64;
    fn pg_diff_string_hash(key: *const u8, keysize: usize) -> u32;
    fn pg_diff_tag_hash(key: *const u8, keysize: usize) -> u32;
    fn pg_diff_uint32_hash(k: u32) -> u32;
    fn pg_diff_hash_combine(a: u32, b: u32) -> u32;
    fn pg_diff_hash_combine64(a: u64, b: u64) -> u64;
    fn pg_diff_murmurhash32(h: u32) -> u32;
    fn pg_diff_murmurhash64(h: u64) -> u64;
    fn pg_diff_rotate_high_and_low_32bits(v: u64) -> u64;
}

/// Max key bytes per exec (iteration cost cap; lengths 0..=4096 cover every
/// tail arm and hundreds of 12-byte mix rounds).
const MAX_KEY: usize = 4096;

fn u32_at(payload: &[u8], off: usize) -> u32 {
    let mut b = [0u8; 4];
    for (i, slot) in b.iter_mut().enumerate() {
        if let Some(&v) = payload.get(off + i) {
            *slot = v;
        }
    }
    u32::from_le_bytes(b)
}

fn u64_at(payload: &[u8], off: usize) -> u64 {
    let mut b = [0u8; 8];
    for (i, slot) in b.iter_mut().enumerate() {
        if let Some(&v) = payload.get(off + i) {
            *slot = v;
        }
    }
    u64::from_le_bytes(b)
}

/// Copy `key` into a scratch buffer so the resulting pointer has residue
/// `align` (mod 4). C's hash_bytes takes its aligned fast path only when
/// (ptr & 3) == 0; this makes both C paths reachable deterministically.
struct AlignedKey {
    buf: Vec<u8>,
    off: usize,
    len: usize,
}

impl AlignedKey {
    fn new(key: &[u8], align: usize) -> Self {
        let mut buf = vec![0u8; key.len() + 8];
        let base = buf.as_ptr() as usize;
        let off = align.wrapping_sub(base) % 4; // (base+off) % 4 == align
        buf[off..off + key.len()].copy_from_slice(key);
        Self {
            buf,
            off,
            len: key.len(),
        }
    }
    fn slice(&self) -> &[u8] {
        &self.buf[self.off..self.off + self.len]
    }
    fn ptr(&self) -> *const u8 {
        self.buf[self.off..].as_ptr()
    }
}

pub fn hashfn_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&al, payload)) = rest.split_first() else {
        return;
    };
    let align = (al % 4) as usize;

    match sel % 12 {
        0 => {
            let key = &payload[..payload.len().min(MAX_KEY)];
            let ak = AlignedKey::new(key, align);
            let rv = hashfn::hash_bytes(ak.slice());
            let cv = unsafe { pg_diff_hash_bytes(ak.ptr(), key.len() as i32) };
            assert_eq!(rv, cv, "hash_bytes value");
        }
        1 => {
            let seed = u64_at(payload, 0);
            let key = &payload[payload.len().min(8)..payload.len().min(8 + MAX_KEY)];
            let ak = AlignedKey::new(key, align);
            let rv = hashfn::hash_bytes_extended(ak.slice(), seed);
            let cv =
                unsafe { pg_diff_hash_bytes_extended(ak.ptr(), key.len() as i32, seed) };
            assert_eq!(rv, cv, "hash_bytes_extended value");
            if seed == 0 {
                // header contract: zero-seed low word == hash_bytes
                assert_eq!(rv as u32, hashfn::hash_bytes(ak.slice()));
            }
        }
        2 => {
            let k = u32_at(payload, 0);
            assert_eq!(
                hashfn::hash_bytes_uint32(k),
                unsafe { pg_diff_hash_bytes_uint32(k) },
                "hash_bytes_uint32 value"
            );
        }
        3 => {
            let k = u32_at(payload, 0);
            let seed = u64_at(payload, 4);
            assert_eq!(
                hashfn::hash_bytes_uint32_extended(k, seed),
                unsafe { pg_diff_hash_bytes_uint32_extended(k, seed) },
                "hash_bytes_uint32_extended value"
            );
        }
        4 => {
            // string_hash: key from payload[8..], NUL sentinel appended.
            let ks_raw = u64_at(payload, 0);
            let key = &payload[payload.len().min(8)..payload.len().min(8 + MAX_KEY)];
            let mut ak = AlignedKey::new(key, align);
            ak.buf[ak.off + ak.len] = 0; // guaranteed NUL sentinel (buf has +8 slack)
            ak.len += 1;
            // keysize plane: tiny values, len-adjacent, 0, and huge
            let keysize = match (ks_raw >> 61) % 4 {
                0 => 0usize,
                1 => (ks_raw as usize) % (ak.len + 2),
                2 => usize::MAX - ((ks_raw as usize) % 3),
                _ => ks_raw as usize,
            };
            let rv = hashfn::string_hash(ak.slice(), keysize);
            let cv = unsafe { pg_diff_string_hash(ak.ptr(), keysize) };
            assert_eq!(rv, cv, "string_hash value (keysize {keysize})");
        }
        5 => {
            let key = &payload[..payload.len().min(MAX_KEY)];
            let ak = AlignedKey::new(key, align);
            // C reads exactly keysize bytes: fold into [0, len]
            let keysize = if key.is_empty() {
                0
            } else {
                (u64_at(payload, 0) as usize) % (key.len() + 1)
            };
            let rv = hashfn::tag_hash(ak.slice(), keysize);
            let cv = unsafe { pg_diff_tag_hash(ak.ptr(), keysize) };
            assert_eq!(rv, cv, "tag_hash value");
        }
        6 => {
            let k = u32_at(payload, 0);
            assert_eq!(
                hashfn::uint32_hash(k),
                unsafe { pg_diff_uint32_hash(k) },
                "uint32_hash value"
            );
        }
        7 => {
            let a = u32_at(payload, 0);
            let b = u32_at(payload, 4);
            assert_eq!(
                hashfn::hash_combine(a, b),
                unsafe { pg_diff_hash_combine(a, b) },
                "hash_combine value"
            );
        }
        8 => {
            let a = u64_at(payload, 0);
            let b = u64_at(payload, 8);
            assert_eq!(
                hashfn::hash_combine64(a, b),
                unsafe { pg_diff_hash_combine64(a, b) },
                "hash_combine64 value"
            );
        }
        9 => {
            let h = u32_at(payload, 0);
            let rv = hashfn::murmurhash32(h);
            assert_eq!(rv, unsafe { pg_diff_murmurhash32(h) }, "murmurhash32 value");
            // pgrust-only inverse: roundtrip property both directions
            assert_eq!(hashfn::murmurhash32_inverse(rv), h, "inverse(murmur(h))");
            assert_eq!(
                hashfn::murmurhash32(hashfn::murmurhash32_inverse(h)),
                h,
                "murmur(inverse(h))"
            );
        }
        10 => {
            let h = u64_at(payload, 0);
            assert_eq!(
                hashfn::murmurhash64(h),
                unsafe { pg_diff_murmurhash64(h) },
                "murmurhash64 value"
            );
        }
        _ => {
            let v = u64_at(payload, 0);
            assert_eq!(
                hashfn::rotate_high_and_low_32bits(v),
                unsafe { pg_diff_rotate_high_and_low_32bits(v) },
                "rotate_high_and_low_32bits value"
            );
        }
    }
    assert_eq!(unsafe { pg_diff_errcode_get() }, 0, "oracle raised");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed sweep: every arm x every alignment x length classes, incl. all
    /// hash_bytes tail arms (len 0..=13) and a multi-round key.
    #[test]
    fn arm_sweep() {
        let _serial = crate::c_oracle_serial();
        let long: Vec<u8> = (0..64u8).map(|i| i.wrapping_mul(37)).collect();
        for sel in 0u8..12 {
            for al in 0u8..4 {
                for len in 0..=13usize {
                    let mut data = vec![sel, al];
                    data.extend_from_slice(&long[..len]);
                    hashfn_diff(&data);
                }
                let mut data = vec![sel, al];
                data.extend_from_slice(&long);
                hashfn_diff(&data);
            }
        }
    }

    /// Single-byte witness pairs (skill obligation): hash_core packs bytes
    /// into u32 words (tail arms for len 1..=12) and hash_bytes_extended
    /// packs (b << 32) | c — every byte position within every tail length
    /// and each packed half must independently steer the verdict.
    #[test]
    fn single_byte_witness_pairs() {
        let _serial = crate::c_oracle_serial();
        for len in 1..=13usize {
            let base_key: Vec<u8> = (0..len as u8).map(|i| i.wrapping_add(1)).collect();
            let h0 = hashfn::hash_bytes(&base_key);
            for pos in 0..len {
                let mut k = base_key.clone();
                k[pos] ^= 1;
                assert_ne!(
                    hashfn::hash_bytes(&k),
                    h0,
                    "byte {pos} of len {len} not witnessed"
                );
                // C agrees on the mutated key too, at every alignment
                for al in 0..4u8 {
                    let mut data = vec![0u8, al];
                    data.extend_from_slice(&k);
                    hashfn_diff(&data);
                }
            }
        }
        // (b<<32)|c packing halves of hash_bytes_extended
        let k = b"witness-pair-key";
        let e = hashfn::hash_bytes_extended(k, 0);
        let (hi, lo) = ((e >> 32) as u32, e as u32);
        assert_ne!(hi, 0);
        assert_ne!(lo, 0);
        assert_eq!(lo, hashfn::hash_bytes(k), "low word is hash_bytes");
        // seed halves: high-32 and low-32 of the seed independently steer
        let s1 = hashfn::hash_bytes_extended(k, 1);
        let s2 = hashfn::hash_bytes_extended(k, 1 << 32);
        assert_ne!(s1, e);
        assert_ne!(s2, e);
        assert_ne!(s1, s2, "seed halves alias");
    }

    /// string_hash truncation semantics against C, incl. keysize 0 wrap.
    #[test]
    fn string_hash_keysize_plane() {
        let _serial = crate::c_oracle_serial();
        for (key, keysize) in [
            (&b"abc\0def"[..], 16usize),
            (&b"abcdef"[..], 4),
            (&b"abcdef"[..], 0),
            (&b"abcdef"[..], usize::MAX),
            (&b""[..], 0),
            (&b"\0"[..], 5),
        ] {
            let mut cbuf = key.to_vec();
            cbuf.push(0);
            let rv = hashfn::string_hash(&cbuf, keysize);
            let cv = unsafe { pg_diff_string_hash(cbuf.as_ptr(), keysize) };
            assert_eq!(rv, cv, "string_hash({key:?}, {keysize})");
        }
    }

    /// Replay every checked-in seed. Corpus is COMMITTED.
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/hashfn_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/hashfn_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                hashfn_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }
}
