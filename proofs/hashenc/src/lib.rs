//! Kani C-equivalence harnesses: the CBMC-tractable members of the p1-lanee
//! encoding+crypto/hash batch vs vendored PostgreSQL 18.3 C
//! (c/pg_hashenc_kani.c, compiled via `-Z c-ffi --c-lib`).
//!
//! In scope here (fast-class, small loops / finite domains):
//!   - common/base64: pg_b64_enc_len / pg_b64_dec_len (closed forms, fenced
//!     below i32-overflow — the C expressions overflow identically but UB
//!     vs wrap is not a surface PG reaches: callers pass buffer lengths),
//!     pg_b64_encode / pg_b64_decode (symbolic bytes, len<=6/8 incl the
//!     '=' end-flag arms, whitespace/invalid rejection, short-dst zeroing).
//!   - common/md5: bytes_to_hex (full symbolic 16 bytes vs verbatim
//!     bytesToHex).
//!   - adt/adt_ascii: ascii_safe_strlcpy (symbolic src len<=6, symbolic
//!     destsiz<=8) — pg_to_ascii's core is already proved in
//!     proofs/name-ascii (enc case-split), not duplicated here.
//!
//! Fuzz-routed (NOT here, see fuzz/core/src/hashenc.rs): md5/sha1/sha2
//! digest rounds (64-80 round compression = the classic CBMC wall), hmac,
//! scram (iterated hmac), crc tables, and every SQL wrapper.
//!
//! Run: sh run-all.sh (kissat for expected-green; the negative control
//! runs under the DEFAULT solver — controls validate by counterexample).

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    use core::ffi::{c_char, c_int};

    extern "C" {
        pub fn pg_b64_encode(src: *const u8, len: c_int, dst: *mut c_char, dstlen: c_int) -> c_int;
        pub fn pg_b64_decode(src: *const c_char, len: c_int, dst: *mut u8, dstlen: c_int) -> c_int;
        pub fn pg_b64_enc_len(srclen: c_int) -> c_int;
        pub fn pg_b64_dec_len(srclen: c_int) -> c_int;
        pub fn pg_kani_bytes_to_hex(b: *const u8, s: *mut c_char) -> c_int;
        pub fn pg_kani_ascii_safe_strlcpy(
            dest: *mut c_char,
            src: *const c_char,
            destsiz: usize,
        ) -> c_int;
    }
}

#[cfg(kani)]
mod harnesses {
    use crate::ffi;

    // ---- length helpers (closed forms) -----------------------------------
    // Fences keep both sides off i32 overflow: enc_len's `*4` overflows
    // above (i32::MAX/4)*3-ish; dec_len's `*3` above i32::MAX/3. PG callers
    // pass non-negative in-memory buffer lengths, far below both.

    #[kani::proof]
    fn eq_b64_enc_len() {
        let srclen: i32 = kani::any();
        kani::assume((0..=1_610_000_000).contains(&srclen));
        let c = unsafe { ffi::pg_b64_enc_len(srclen) };
        assert_eq!(pg_b64::pg_b64_enc_len(srclen), c);
    }

    #[kani::proof]
    fn eq_b64_dec_len() {
        let srclen: i32 = kani::any();
        kani::assume((0..=715_000_000).contains(&srclen));
        let c = unsafe { ffi::pg_b64_dec_len(srclen) };
        assert_eq!(pg_b64::pg_b64_dec_len(srclen), c);
    }

    // ---- encode: symbolic bytes, len<=6, exact-cap and short dst ---------

    fn encode_cell(len: usize, dstlen_delta: i32) {
        let mut src = [0u8; 6];
        for b in src.iter_mut() {
            *b = kani::any();
        }
        let cap = pg_b64::pg_b64_enc_len(len as i32) + dstlen_delta;
        let cap_u = cap.max(0) as usize;
        assert!(cap_u <= 12);
        let mut r_dst = [0xAAu8; 12];
        let mut c_dst = [0xAAu8; 12];
        let rn = pg_b64::pg_b64_encode(&src[..len], len as i32, &mut r_dst[..cap_u], cap);
        let cn = unsafe {
            ffi::pg_b64_encode(src.as_ptr(), len as i32, c_dst.as_mut_ptr() as *mut _, cap)
        };
        assert_eq!(rn, cn);
        // Compare the full window (written bytes + error zeroing).
        for i in 0..cap_u {
            assert!(r_dst[i] == c_dst[i]);
        }
    }

    #[kani::proof]
    fn eq_b64_encode_len0() { encode_cell(0, 0); }
    #[kani::proof]
    fn eq_b64_encode_len1() { encode_cell(1, 0); }
    #[kani::proof]
    fn eq_b64_encode_len2() { encode_cell(2, 0); }
    #[kani::proof]
    fn eq_b64_encode_len3() { encode_cell(3, 0); }
    #[kani::proof]
    fn eq_b64_encode_len4() { encode_cell(4, 0); }
    #[kani::proof]
    fn eq_b64_encode_len5() { encode_cell(5, 0); }
    #[kani::proof]
    fn eq_b64_encode_len6() { encode_cell(6, 0); }
    // Short-dst error arms (zeroing parity).
    #[kani::proof]
    fn eq_b64_encode_len4_short() { encode_cell(4, -1); }
    #[kani::proof]
    fn eq_b64_encode_len6_short() { encode_cell(6, -3); }

    // ---- decode: symbolic bytes len<=8 (covers '='/whitespace/invalid) ---

    fn decode_cell(len: usize) {
        let mut src = [0u8; 8];
        for b in src.iter_mut() {
            *b = kani::any();
        }
        let cap = pg_b64::pg_b64_dec_len(len as i32);
        let cap_u = cap as usize;
        assert!(cap_u <= 6);
        let mut r_dst = [0x55u8; 6];
        let mut c_dst = [0x55u8; 6];
        let rn = pg_b64::pg_b64_decode(&src[..len], len as i32, &mut r_dst[..cap_u], cap);
        let cn = unsafe {
            ffi::pg_b64_decode(src.as_ptr() as *const _, len as i32, c_dst.as_mut_ptr(), cap)
        };
        assert_eq!(rn, cn);
        for i in 0..cap_u {
            assert!(r_dst[i] == c_dst[i]);
        }
    }

    #[kani::proof]
    fn eq_b64_decode_len0() { decode_cell(0); }
    #[kani::proof]
    fn eq_b64_decode_len1() { decode_cell(1); }
    #[kani::proof]
    fn eq_b64_decode_len2() { decode_cell(2); }
    #[kani::proof]
    fn eq_b64_decode_len3() { decode_cell(3); }
    #[kani::proof]
    fn eq_b64_decode_len4() { decode_cell(4); }
    #[kani::proof]
    fn eq_b64_decode_len5() { decode_cell(5); }
    #[kani::proof]
    fn eq_b64_decode_len6() { decode_cell(6); }
    #[kani::proof]
    fn eq_b64_decode_len7() { decode_cell(7); }
    #[kani::proof]
    fn eq_b64_decode_len8() { decode_cell(8); }

    // ---- md5 bytes_to_hex: full symbolic 16 bytes -------------------------

    #[kani::proof]
    fn eq_bytes_to_hex() {
        let mut b = [0u8; 16];
        for x in b.iter_mut() {
            *x = kani::any();
        }
        let r = pg_md5::bytes_to_hex(b);
        let mut c = [0u8; 33];
        let _ = unsafe { ffi::pg_kani_bytes_to_hex(b.as_ptr(), c.as_mut_ptr() as *mut _) };
        for i in 0..32 {
            assert!(r[i] == c[i]);
        }
        assert!(c[32] == 0);
    }

    // ---- ascii_safe_strlcpy: symbolic src len<=6, destsiz<=8 --------------

    #[kani::proof]
    fn eq_ascii_safe_strlcpy() {
        let destsiz: usize = kani::any();
        kani::assume(destsiz <= 8);
        let mut src = [0u8; 7];
        for b in src.iter_mut().take(6) {
            *b = kani::any();
        }
        src[6] = 0; // C reads until NUL or destsiz-1 bytes
        let mut r_dest = [0xAAu8; 8];
        let mut c_dest = [0xAAu8; 8];
        adt_ascii::ascii_safe_strlcpy(&mut r_dest[..destsiz], &src);
        let _ = unsafe {
            ffi::pg_kani_ascii_safe_strlcpy(
                c_dest.as_mut_ptr() as *mut _,
                src.as_ptr() as *const _,
                destsiz,
            )
        };
        for i in 0..8 {
            assert!(r_dest[i] == c_dest[i]);
        }
    }

    // ---- negative control (must FAIL on the intended assert) -------------
    // Claims bytes_to_hex emits UPPERCASE hex; the verbatim C (and the
    // shipped Rust) emit lowercase, so the assert! must produce a
    // counterexample — validating the whole dual-execution linkage.

    #[kani::proof]
    fn control_bytes_to_hex_uppercase_must_fail() {
        let mut b = [0u8; 16];
        for x in b.iter_mut() {
            *x = kani::any();
        }
        let mut c = [0u8; 33];
        let _ = unsafe { ffi::pg_kani_bytes_to_hex(b.as_ptr(), c.as_mut_ptr() as *mut _) };
        // INTENDED FAILURE: 0x0a nibble renders 'a', not 'A'.
        for i in 0..32 {
            assert!(!(c[i] as char).is_ascii_lowercase());
        }
    }
}
