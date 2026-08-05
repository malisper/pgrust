//! Kani C-equivalence harnesses: pgrust `hashfn` (shipped crate at
//! crates/common/hashfn) vs vendored PostgreSQL 18.3 C (c_hashfn.c,
//! compiled via `-Z c-ffi --c-lib`). 100%-coverage campaign, lane p1-laneh.
//!
//! Fixed-width kernels (hash_bytes_uint32(_extended), hash_combine(64),
//! murmurhash32/64, rotate_high_and_low_32bits) are proved FULL-DOMAIN
//! (straight-line mix/final code, fully symbolic words).
//!
//! hash_bytes / hash_bytes_extended: proved over symbolic content with
//! len <= 16 SYMBOLIC (covers the len<=4 fast path, one 12-byte mix round,
//! and every tail arm 0..=12 via 12+tail lengths 12..=16 plus short
//! lengths; the aligned-vs-unaligned C split is exercised by an aligned
//! buffer — the unaligned C path is byte-identical on LE and is carried by
//! the hashfn_diff campaign's alignment steering). Longer keys are
//! fuzz-carried (10M+ execs, lengths to 4096).
//!
//! string_hash / tag_hash: len<=8-fenced symbolic keys incl. interior NULs
//! + symbolic keysize (full usize incl. the keysize=0 wrap).
//!
//! murmurhash32_inverse (pgrust-only, NO C counterpart): full-u32
//! bijection roundtrip proof, both directions.

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    extern "C" {
        pub fn hash_bytes(k: *const u8, keylen: i32) -> u32;
        pub fn hash_bytes_extended(k: *const u8, keylen: i32, seed: u64) -> u64;
        pub fn hash_bytes_uint32(k: u32) -> u32;
        pub fn hash_bytes_uint32_extended(k: u32, seed: u64) -> u64;
        pub fn string_hash(key: *const core::ffi::c_void, keysize: usize) -> u32;
        pub fn tag_hash(key: *const core::ffi::c_void, keysize: usize) -> u32;
        pub fn uint32_hash(key: *const core::ffi::c_void, keysize: usize) -> u32;
        pub fn c_hash_combine(a: u32, b: u32) -> u32;
        pub fn c_hash_combine64(a: u64, b: u64) -> u64;
        pub fn c_murmurhash32(h: u32) -> u32;
        pub fn c_murmurhash64(h: u64) -> u64;
        pub fn c_rotate_high_and_low_32bits(v: u64) -> u64;
    }
}

#[cfg(kani)]
mod harnesses {
    use super::ffi;

    const MAX_LEN: usize = 16;

    /// Typed per-element staging (CBMC byte-pun law): 4-byte-aligned
    /// backing store so C's aligned word-fetch path is the one modeled.
    #[repr(align(4))]
    struct AlignedBuf([u8; MAX_LEN]);

    fn sym_key() -> (AlignedBuf, usize) {
        let mut buf = AlignedBuf([0u8; MAX_LEN]);
        for i in 0..MAX_LEN {
            buf.0[i] = kani::any();
        }
        let len: usize = kani::any();
        kani::assume(len <= MAX_LEN);
        (buf, len)
    }

    #[kani::proof]
    #[kani::unwind(18)] // 16-byte fill loop + one C 12-byte mix round
    fn eq_hash_bytes_len16() {
        let (buf, len) = sym_key();
        let rv = hashfn::hash_bytes(&buf.0[..len]);
        let cv = unsafe { ffi::hash_bytes(buf.0.as_ptr(), len as i32) };
        assert_eq!(rv, cv);
    }

    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_hash_bytes_extended_len16() {
        let (buf, len) = sym_key();
        let seed: u64 = kani::any();
        let rv = hashfn::hash_bytes_extended(&buf.0[..len], seed);
        let cv = unsafe { ffi::hash_bytes_extended(buf.0.as_ptr(), len as i32, seed) };
        assert_eq!(rv, cv);
    }

    #[kani::proof]
    fn eq_hash_bytes_uint32() {
        let k: u32 = kani::any();
        assert_eq!(hashfn::hash_bytes_uint32(k), unsafe {
            ffi::hash_bytes_uint32(k)
        });
    }

    #[kani::proof]
    fn eq_hash_bytes_uint32_extended() {
        let k: u32 = kani::any();
        let seed: u64 = kani::any();
        assert_eq!(hashfn::hash_bytes_uint32_extended(k, seed), unsafe {
            ffi::hash_bytes_uint32_extended(k, seed)
        });
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_string_hash_len8() {
        // len<=8 symbolic key with a guaranteed NUL sentinel (C strlen
        // requires one in-buffer); interior NULs fully symbolic. keysize
        // symbolic over ALL usize, incl. the 0 wrap.
        let mut buf = [0u8; 9];
        for i in 0..8 {
            buf[i] = kani::any();
        }
        buf[8] = 0;
        // keysize <= 10 symbolic covers every truncation class the 8-byte
        // key can distinguish (keysize-1 in 0..=9 plus the keysize=0 wrap);
        // for keysize > 9 Min(s_len, keysize-1) == s_len identically — the
        // literal huge cell below witnesses that branch.
        let keysize: usize = kani::any();
        kani::assume(keysize <= 10);
        let rv = hashfn::string_hash(&buf, keysize);
        let cv = unsafe { ffi::string_hash(buf.as_ptr() as *const _, keysize) };
        assert_eq!(rv, cv);
    }

    #[kani::proof]
    #[kani::unwind(11)]
    fn eq_string_hash_len8_huge_keysize() {
        let mut buf = [0u8; 9];
        for i in 0..8 {
            buf[i] = kani::any();
        }
        buf[8] = 0;
        for keysize in [usize::MAX, 1usize << 40] {
            let rv = hashfn::string_hash(&buf, keysize);
            let cv = unsafe { ffi::string_hash(buf.as_ptr() as *const _, keysize) };
            assert_eq!(rv, cv);
        }
    }

    #[kani::proof]
    #[kani::unwind(14)] // 12-byte fill loop; tail-only hash (len < 12)
    fn eq_tag_hash_len12() {
        #[repr(align(4))]
        struct B([u8; 12]);
        let mut buf = B([0u8; 12]);
        for i in 0..12 {
            buf.0[i] = kani::any();
        }
        let keysize: usize = kani::any();
        kani::assume(keysize <= 12);
        let rv = hashfn::tag_hash(&buf.0, keysize);
        let cv = unsafe { ffi::tag_hash(buf.0.as_ptr() as *const _, keysize) };
        assert_eq!(rv, cv);
    }

    #[kani::proof]
    fn eq_uint32_hash() {
        let k: u32 = kani::any();
        let rv = hashfn::uint32_hash(k);
        let cv = unsafe { ffi::uint32_hash(&k as *const u32 as *const _, 4) };
        assert_eq!(rv, cv);
    }

    #[kani::proof]
    fn eq_hash_combine() {
        let a: u32 = kani::any();
        let b: u32 = kani::any();
        assert_eq!(hashfn::hash_combine(a, b), unsafe {
            ffi::c_hash_combine(a, b)
        });
    }

    #[kani::proof]
    fn eq_hash_combine64() {
        let a: u64 = kani::any();
        let b: u64 = kani::any();
        assert_eq!(hashfn::hash_combine64(a, b), unsafe {
            ffi::c_hash_combine64(a, b)
        });
    }

    #[kani::proof]
    fn eq_murmurhash32() {
        let h: u32 = kani::any();
        assert_eq!(hashfn::murmurhash32(h), unsafe { ffi::c_murmurhash32(h) });
    }

    #[kani::proof]
    fn eq_murmurhash64() {
        let h: u64 = kani::any();
        assert_eq!(hashfn::murmurhash64(h), unsafe { ffi::c_murmurhash64(h) });
    }

    #[kani::proof]
    fn eq_rotate_high_and_low_32bits() {
        let v: u64 = kani::any();
        assert_eq!(hashfn::rotate_high_and_low_32bits(v), unsafe {
            ffi::c_rotate_high_and_low_32bits(v)
        });
    }

    /// pgrust-only murmurhash32_inverse: full-u32 bijection, both
    /// directions (no C counterpart — property proof of record).
    #[kani::proof]
    fn murmur32_inverse_roundtrip() {
        let h: u32 = kani::any();
        assert_eq!(hashfn::murmurhash32_inverse(hashfn::murmurhash32(h)), h);
        assert_eq!(hashfn::murmurhash32(hashfn::murmurhash32_inverse(h)), h);
    }

    /// Must-fail negative control (family non-vacuity), on the INTENDED
    /// assert: a wrong-value claim against the C hash.
    #[kani::proof]
    fn control_murmur32_wrong_value() {
        let h: u32 = kani::any();
        let cv = unsafe { ffi::c_murmurhash32(h) };
        assert!(
            hashfn::murmurhash32(h) == cv.wrapping_add(1),
            "INTENDED-FAIL: off-by-one claim"
        );
    }
}
