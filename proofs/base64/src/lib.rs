//! Kani C≡Rust equivalence proofs: PostgreSQL 18.3 src/common/base64.c
//! (pg_b64_encode / pg_b64_decode / pg_b64_enc_len / pg_b64_dec_len) vs the
//! shipped pgrust `pg_b64` crate.
//!
//! Domains (bounded stated as bounded):
//!   - encode: src [u8; 6] fully symbolic, len <= 6 symbolic, dstlen
//!     symbolic 0..=12 (covers exact-fit, oversized, and every short-buffer
//!     error cell; both whole dst images compared so the "-1 zeroes dst"
//!     hygiene contract is in-theorem).
//!   - decode: src [u8; 8] fully symbolic, len <= 8 symbolic, dstlen
//!     symbolic 0..=8 (8 symbolic input bytes = two full quads: reaches
//!     the '=' end-flag machine, the C `char`-signedness lookup fence,
//!     whitespace rejects, and every short-buffer error cell).
//!   - enc_len/dec_len: full i32 fenced to the overflow-free domain
//!     (srclen in 0..=3/4*i32::MAX-2 resp. 0..=i32::MAX/3; beyond it the C
//!     expression is signed-overflow UB, not a defined behavior to match).
//!
//! The larger campaign evidence for full-length inputs is the
//! enc_tables_diff differential fuzz family (fuzz/core/src/enc_tables.rs);
//! these proofs pin the state machines exhaustively on the bounded domain.

#[cfg(kani)]
mod proofs {
    use std::os::raw::{c_char, c_int};

    extern "C" {
        fn pg_b64_encode(src: *const u8, len: c_int, dst: *mut c_char, dstlen: c_int) -> c_int;
        fn pg_b64_decode(src: *const c_char, len: c_int, dst: *mut u8, dstlen: c_int) -> c_int;
        fn pg_b64_enc_len(srclen: c_int) -> c_int;
        fn pg_b64_dec_len(srclen: c_int) -> c_int;
    }

    const FILL: u8 = 0x5a;

    #[kani::proof]
    #[kani::unwind(14)]
    fn eq_b64_encode() {
        let src: [u8; 6] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 6);
        let dstlen: usize = kani::any();
        kani::assume(dstlen <= 12);

        let mut c_dst = [FILL; 12];
        let mut r_dst = [FILL; 12];

        let c_rc = unsafe {
            pg_b64_encode(src.as_ptr(), len as c_int, c_dst.as_mut_ptr().cast(), dstlen as c_int)
        };
        let r_rc = pg_b64::pg_b64_encode(&src[..len], len as i32, &mut r_dst[..dstlen], dstlen as i32);
        assert!(c_rc == r_rc, "divergence: encode rc");
        // Whole-buffer image parity: written cells, error zeroing, and the
        // untouched FILL tail beyond dstlen.
        assert!(c_dst == r_dst, "divergence: encode dst image");
    }

    #[kani::proof]
    #[kani::unwind(11)]
    fn eq_b64_decode() {
        let src: [u8; 8] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let dstlen: usize = kani::any();
        kani::assume(dstlen <= 8);

        let mut c_dst = [FILL; 8];
        let mut r_dst = [FILL; 8];

        let c_rc = unsafe {
            pg_b64_decode(src.as_ptr().cast(), len as c_int, c_dst.as_mut_ptr(), dstlen as c_int)
        };
        let r_rc = pg_b64::pg_b64_decode(&src[..len], len as i32, &mut r_dst[..dstlen], dstlen as i32);
        assert!(c_rc == r_rc, "divergence: decode rc");
        assert!(c_dst == r_dst, "divergence: decode dst image");
    }

    /// Fence: (srclen + 2) / 3 * 4 stays in i32 for
    /// srclen <= 3 * (i32::MAX / 4) - 2; beyond it C is signed-overflow UB.
    #[kani::proof]
    fn eq_b64_enc_len() {
        let srclen: i32 = kani::any();
        kani::assume((0..=3 * (i32::MAX / 4) - 2).contains(&srclen));
        let c = unsafe { pg_b64_enc_len(srclen) };
        let r = pg_b64::pg_b64_enc_len(srclen);
        assert!(c == r, "divergence: enc_len");
    }

    /// Fence: srclen * 3 stays in i32 for srclen <= i32::MAX / 3.
    #[kani::proof]
    fn eq_b64_dec_len() {
        let srclen: i32 = kani::any();
        kani::assume((0..=i32::MAX / 3).contains(&srclen));
        let c = unsafe { pg_b64_dec_len(srclen) };
        let r = pg_b64::pg_b64_dec_len(srclen);
        assert!(c == r, "divergence: dec_len");
    }

    /// NEGATIVE CONTROL — must FAIL (proves the rig is non-vacuous):
    /// the C encoder deliberately compared against the Rust DECODER.
    #[kani::proof]
    #[kani::unwind(9)]
    fn control_encode_vs_decode_must_fail() {
        let src: [u8; 4] = kani::any();
        let mut c_dst = [FILL; 8];
        let mut r_dst = [FILL; 8];
        let c_rc = unsafe { pg_b64_encode(src.as_ptr(), 4, c_dst.as_mut_ptr().cast(), 8) };
        let r_rc = pg_b64::pg_b64_decode(&src, 4, &mut r_dst, 8);
        assert!(c_rc == r_rc && c_dst == r_dst, "expected failure: encode vs decode");
    }
}
