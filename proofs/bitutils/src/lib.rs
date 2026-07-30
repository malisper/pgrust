//! Kani C-equivalence harnesses: pgrust `pg_bitutils` (shipped crate at
//! crates/port/pg_bitutils) vs vendored PostgreSQL portable C
//! (c_bitutils.c, compiled via `-Z c-ffi --c-lib`).
//!
//! Each harness draws fully symbolic inputs, fences exactly the C
//! contract's preconditions (word != 0 etc.) with kani::assume, calls
//! both sides, and asserts equality.

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    extern "C" {
        pub fn pg_popcount32(word: u32) -> i32;
        pub fn pg_popcount64(word: u64) -> i32;
        pub fn pg_leftmost_one_pos32(word: u32) -> i32;
        pub fn pg_leftmost_one_pos64(word: u64) -> i32;
        pub fn pg_rightmost_one_pos32(word: u32) -> i32;
        pub fn pg_rightmost_one_pos64(word: u64) -> i32;
        pub fn pg_nextpower2_32(num: u32) -> u32;
        pub fn pg_nextpower2_64(num: u64) -> u64;
        pub fn pg_prevpower2_32(num: u32) -> u32;
        pub fn pg_prevpower2_64(num: u64) -> u64;
        pub fn pg_ceil_log2_32(num: u32) -> u32;
        pub fn pg_ceil_log2_64(num: u64) -> u64;
        pub fn pg_rotate_right32(word: u32, n: i32) -> u32;
        pub fn pg_rotate_left32(word: u32, n: i32) -> u32;
        pub fn pg_popcount_c(buf: *const core::ffi::c_char, bytes: i32) -> u64;
        pub fn pg_popcount_masked_c(
            buf: *const core::ffi::c_char,
            bytes: i32,
            mask: u8,
        ) -> u64;
    }
}

#[cfg(kani)]
mod harnesses {
    use crate::ffi;

    #[kani::proof]
    fn eq_popcount32() {
        let x: u32 = kani::any();
        assert_eq!(pg_bitutils::pg_popcount32(x), unsafe { ffi::pg_popcount32(x) });
    }

    #[kani::proof]
    fn eq_popcount64() {
        let x: u64 = kani::any();
        assert_eq!(pg_bitutils::pg_popcount64(x), unsafe { ffi::pg_popcount64(x) });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_leftmost_one_pos32() {
        let x: u32 = kani::any();
        kani::assume(x != 0); // C contract: word must not be 0
        assert_eq!(pg_bitutils::pg_leftmost_one_pos32(x), unsafe {
            ffi::pg_leftmost_one_pos32(x)
        });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_leftmost_one_pos64() {
        let x: u64 = kani::any();
        kani::assume(x != 0);
        assert_eq!(pg_bitutils::pg_leftmost_one_pos64(x), unsafe {
            ffi::pg_leftmost_one_pos64(x)
        });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_rightmost_one_pos32() {
        let x: u32 = kani::any();
        kani::assume(x != 0);
        assert_eq!(pg_bitutils::pg_rightmost_one_pos32(x), unsafe {
            ffi::pg_rightmost_one_pos32(x)
        });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_rightmost_one_pos64() {
        let x: u64 = kani::any();
        kani::assume(x != 0);
        assert_eq!(pg_bitutils::pg_rightmost_one_pos64(x), unsafe {
            ffi::pg_rightmost_one_pos64(x)
        });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_nextpower2_32() {
        let x: u32 = kani::any();
        kani::assume(x > 0 && x <= u32::MAX / 2 + 1); // C contract
        assert_eq!(pg_bitutils::pg_nextpower2_32(x), unsafe { ffi::pg_nextpower2_32(x) });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_nextpower2_64() {
        let x: u64 = kani::any();
        kani::assume(x > 0 && x <= u64::MAX / 2 + 1);
        assert_eq!(pg_bitutils::pg_nextpower2_64(x), unsafe { ffi::pg_nextpower2_64(x) });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_prevpower2_32() {
        let x: u32 = kani::any();
        kani::assume(x != 0);
        assert_eq!(pg_bitutils::pg_prevpower2_32(x), unsafe { ffi::pg_prevpower2_32(x) });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_prevpower2_64() {
        let x: u64 = kani::any();
        kani::assume(x != 0);
        assert_eq!(pg_bitutils::pg_prevpower2_64(x), unsafe { ffi::pg_prevpower2_64(x) });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_ceil_log2_32() {
        // Full domain: both sides define num < 2 => 0, no precondition.
        let x: u32 = kani::any();
        assert_eq!(pg_bitutils::pg_ceil_log2_32(x), unsafe { ffi::pg_ceil_log2_32(x) });
    }

    #[kani::proof]
    #[kani::unwind(12)] // C table-walk loop: <=4 (32-bit) / <=8 (64-bit) iterations
    fn eq_ceil_log2_64() {
        let x: u64 = kani::any();
        assert_eq!(pg_bitutils::pg_ceil_log2_64(x), unsafe { ffi::pg_ceil_log2_64(x) });
    }

    // Rotates: C `(word >> n) | (word << (32 - n))` is UB at n == 0
    // (shift by 32) and for n outside 0..32; Rust rotate_right/left is
    // total. Fence to the C-defined domain 1..=31 -- outside it there is
    // no C behavior to be equivalent to.
    #[kani::proof]
    fn eq_rotate_right32() {
        let x: u32 = kani::any();
        let n: i32 = kani::any();
        kani::assume(n >= 1 && n <= 31);
        assert_eq!(pg_bitutils::pg_rotate_right32(x, n), unsafe {
            ffi::pg_rotate_right32(x, n)
        });
    }

    #[kani::proof]
    fn eq_rotate_left32() {
        let x: u32 = kani::any();
        let n: i32 = kani::any();
        kani::assume(n >= 1 && n <= 31);
        assert_eq!(pg_bitutils::pg_rotate_left32(x, n), unsafe {
            ffi::pg_rotate_left32(x, n)
        });
    }

    // ---- byte-buffer popcount ----
    // Rust pg_popcount takes the NEON path for len >= 8 on aarch64
    // hosts; the _small harnesses (len <= 7) exercise the shared scalar
    // table path, the _full harnesses (len <= 32) additionally cover the
    // word-chunk loops.
    //
    // RESULT (2026-07-28, Kani 0.67.0, aarch64-apple-darwin): the _small
    // harnesses PROVE (~3s). The _full harnesses FAIL with
    // "unsupported_construct" on std::arch::aarch64::vcntq_s8 /
    // vaddvq_u64 -- Kani cannot codegen NEON intrinsics, so the shipped
    // len>=8 SIMD path is OUT OF KANI'S REACH on this host. This is a
    // toolchain limitation, NOT a divergence. The scalar fallback
    // (words_then_tail) is cfg'd to non-aarch64 targets and Kani has no
    // cross-target mode, so it is equally unreachable from here; proving
    // it would require an x86_64 host.

    const BUF_MAX: usize = 32;

    #[kani::proof]
    #[kani::unwind(40)]
    fn eq_popcount_buf_small() {
        let buf: [u8; 7] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 7);
        let r = pg_bitutils::pg_popcount(&buf[..len]);
        let c = unsafe { ffi::pg_popcount_c(buf.as_ptr().cast(), len as i32) };
        assert_eq!(r, c);
    }

    #[kani::proof]
    #[kani::unwind(40)]
    fn eq_popcount_buf_full() {
        let buf: [u8; BUF_MAX] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= BUF_MAX);
        let r = pg_bitutils::pg_popcount(&buf[..len]);
        let c = unsafe { ffi::pg_popcount_c(buf.as_ptr().cast(), len as i32) };
        assert_eq!(r, c);
    }

    #[kani::proof]
    #[kani::unwind(40)]
    fn eq_popcount_masked_buf_small() {
        let buf: [u8; 7] = kani::any();
        let mask: u8 = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= 7);
        let r = pg_bitutils::pg_popcount_masked(&buf[..len], mask);
        let c = unsafe { ffi::pg_popcount_masked_c(buf.as_ptr().cast(), len as i32, mask) };
        assert_eq!(r, c);
    }

    #[kani::proof]
    #[kani::unwind(40)]
    fn eq_popcount_masked_buf_full() {
        let buf: [u8; BUF_MAX] = kani::any();
        let mask: u8 = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= BUF_MAX);
        let r = pg_bitutils::pg_popcount_masked(&buf[..len], mask);
        let c = unsafe { ffi::pg_popcount_masked_c(buf.as_ptr().cast(), len as i32, mask) };
        assert_eq!(r, c);
    }
}
