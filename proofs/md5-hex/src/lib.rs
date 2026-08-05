//! Dual-execution proof: pg_md5::bytes_to_hex ≡ PostgreSQL 18.3
//! md5_common.c bytesToHex, full 16-byte input domain.
//!
//! Scope note: the C function NUL-terminates s[32]; the Rust function
//! returns exactly 32 hex bytes (its callers add "md5" prefixes or copy
//! into text varlenas — no C-string plumbing survives in the shipped
//! surface). The claim compared is the 32 hex bytes; the terminator is
//! C-string plumbing outside the Rust type's contract.

extern "C" {
    fn pg_bytesToHex(b: *mut u8, s: *mut core::ffi::c_char) -> core::ffi::c_int;
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// Full domain: all 2^128 inputs (bitvector-parallel; no case split).
    #[kani::proof]
    #[kani::unwind(33)]
    fn eq_bytes_to_hex() {
        let b: [u8; 16] = kani::any();
        let r = pg_md5::bytes_to_hex(b);

        let mut cb = b;
        let mut cs = [0i8; 33];
        // SAFETY: pg_bytesToHex reads 16 bytes from cb, writes 33 to cs.
        let _ = unsafe { pg_bytesToHex(cb.as_mut_ptr(), cs.as_mut_ptr()) };

        for i in 0..32 {
            assert_eq!(r[i], cs[i] as u8);
        }
        assert_eq!(cs[32], 0); // C contract: NUL-terminated
    }

    /// Must-fail negative control: a corrupted nibble must be caught by the
    /// INTENDED equality assert (fails on `assert!`, proving the harness
    /// actually compares).
    #[kani::proof]
    #[kani::unwind(33)]
    fn control_bytes_to_hex_detects_corruption() {
        let b: [u8; 16] = kani::any();
        let r = pg_md5::bytes_to_hex(b);

        let mut cb = b;
        cb[0] ^= 0x10; // corrupt one nibble
        let mut cs = [0i8; 33];
        // SAFETY: as above.
        let _ = unsafe { pg_bytesToHex(cb.as_mut_ptr(), cs.as_mut_ptr()) };

        // INTENDED failure: first hex char differs for the corrupted nibble.
        assert!(r[0] == cs[0] as u8);
    }
}
