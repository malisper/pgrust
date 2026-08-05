//! Kani C≡Rust equivalence: the LOCAL `pg_nextpower2_32` transcription in
//! adt array_userfuncs (crates/backend/utils/adt/array_userfuncs/src/lib.rs)
//! vs vendored PostgreSQL 18.3 pg_bitutils.h portable C
//! (c/pg_arrayuser_np2.c; provenance + shims in its header).
//!
//! Claim: full contract domain [1, 2^31] (the C Assert precondition, fenced
//! with kani::assume; Assert is compiled out, NDEBUG semantics). The shipped
//! crates/port/pg_bitutils copy is separately proved by proofs/bitutils —
//! this harness pins the DUPLICATE transcription array_userfuncs carries
//! (combine_array_build_state_arr growth sizing).
//!
//! Control: control_np2_off_by_one must FAIL (asserts C == rust+1 for a
//! non-power-of-two point), failing on the intended assert!.
//!
//! Run recipe:
//!   cd proofs/arrayuser
//!   timeout 60 cargo kani -Z c-ffi --c-lib c/pg_arrayuser_np2.c \
//!       --solver kissat --harness harnesses::eq_local_nextpower2_32
//!   timeout 60 cargo kani -Z c-ffi --c-lib c/pg_arrayuser_np2.c \
//!       --harness harnesses::control_np2_off_by_one   # expect FAILED

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    extern "C" {
        pub fn pg_nextpower2_32(num: u32) -> u32;
    }
}

#[cfg(kani)]
mod harnesses {
    use crate::ffi;

    #[kani::proof]
    #[kani::unwind(8)] // C table-walk loop: <=4 iterations
    fn eq_local_nextpower2_32() {
        let num: u32 = kani::any();
        // C contract (Assert, compiled out): num in [1, PG_UINT32_MAX/2 + 1].
        kani::assume(num > 0 && num <= 0x8000_0000);
        assert_eq!(array_userfuncs::pg_nextpower2_32(num), unsafe {
            ffi::pg_nextpower2_32(num)
        });
    }

    /// Negative control: MUST FAIL (rig non-vacuity).
    #[kani::proof]
    #[kani::unwind(8)]
    fn control_np2_off_by_one() {
        let num: u32 = kani::any();
        kani::assume(num > 1 && num < 0x8000_0000 && (num & (num - 1)) != 0);
        let c = unsafe { ffi::pg_nextpower2_32(num) };
        assert!(c == array_userfuncs::pg_nextpower2_32(num) + 1);
    }
}
