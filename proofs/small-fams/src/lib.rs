//! Kani C≡Rust equivalence: small remaining pg_proc families batch —
//! enum_eq/enum_ne (oids 3508/3509), oidlarger/oidsmaller (1965/1966),
//! int8inc/int8dec (1219/3546 — the count(*) transition functions),
//! bit_bit_count (6162), bytea_bit_count (6163).
//!
//! Rust side: the SHIPPED fmgr wrappers, invoked through real
//! `LocalFcinfo` frames (proof_support::call*), so each proof covers the
//! whole shipped path: datum unwrap → core → Datum pack. C side: vendored
//! c/pg_small_fams.c (REL_18_STABLE; provenance + shims there).
//!
//! Coverage:
//!  - enum_eq/enum_ne: full symbolic Oid (u32) pairs. Plain Oid equality —
//!    catalog lookups do not exist on this path in either implementation.
//!  - oidlarger/oidsmaller: full symbolic u32 pairs.
//!  - int8inc/int8dec: full symbolic i64; VALUE + VERDICT + SQLSTATE parity
//!    (cash pattern): Rust Ok ⇔ C non-overflow with equal values; on Err
//!    the shipped sqlstate (22003 ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
//!    applied by the real `with_sqlstate` call in bigint_out_of_range) +
//!    level against C's ereport errcode. PgError::error is stubbed
//!    field-identically (`-Z stubbing`; Location::caller is
//!    Kani-unsupported): value-space + verdict + sqlstate are in the
//!    theorem, message text/location are not.
//!  - bit_bit_count: symbolic payload ≤ 8 bytes, symbolic bitlen ≤ 64,
//!    fenced to VALID varbit images (bytelen == ceil(bitlen/8) — the
//!    VarBit invariant; pad bits are NOT fenced to zero: both sides count
//!    whole bytes, so parity holds over arbitrary pad bits and the proof
//!    is stronger without the fence). The harness constructs a real
//!    4B-header varlena image, so the shipped detoast-check + header/
//!    payload-slicing path (arg_varlena_packed → data() → payload_bits)
//!    is inside the theorem. C side counts VARBITBYTES(bitlen) bytes via
//!    the vendored pg_popcount (identical vendoring to the PROVED
//!    proofs/bitutils kernel); at payload == 8 that takes the portable
//!    word path — also proved in proofs/bitutils. The Rust wrapper's
//!    inline count_ones loop has no SIMD dispatch.
//!  - bytea_bit_count: symbolic payload ≤ 7 bytes (4B-header image).
//!    LENGTH FENCE = SIMD dispatch trap (proofs/bitutils precedent): the
//!    shipped Rust pg_popcount takes the NEON path for len >= 8 on
//!    aarch64, and core::arch intrinsics are Kani unsupported_construct.
//!    PROVEN PATH: the scalar per-byte table loop (len < 8). len >= 8 =
//!    excluded(blocked:simd) — same ruling as proofs/bitutils
//!    pg_popcount len>=8.
//!  - negative control: Rust fc_enum_eq vs C enum_ne — must FAIL with a
//!    counterexample (rig non-vacuity). Run with the DEFAULT solver
//!    (kissat is non-incremental and never terminates on failing
//!    harnesses).
//!
//! Run: timeout 30 cargo kani -Z c-ffi -Z stubbing \
//!        --c-lib c/pg_small_fams.c --solver kissat --harness <h>
//!      (default solver for the negative control)

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    #[allow(unused_imports)] // stubs is referenced from #[kani::stub(..)] paths
    use proof_support::{call_ok, call1, stubs};
    use types_error::{ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR};

    use std::os::raw::c_int;

    extern "C" {
        fn pg_enum_eq(a: u32, b: u32) -> c_int;
        fn pg_enum_ne(a: u32, b: u32) -> c_int;
        fn pg_oidlarger(arg1: u32, arg2: u32) -> u32;
        fn pg_oidsmaller(arg1: u32, arg2: u32) -> u32;
        fn pg_int8inc(arg: i64, result: *mut i64) -> c_int;
        fn pg_int8dec(arg: i64, result: *mut i64) -> c_int;
        fn pg_bit_bit_count(varbits: *const u8, bitlen: i32) -> i64;
        fn pg_bytea_bit_count(data: *const u8, len: c_int) -> i64;
    }

    // ---------- enum_eq/ne + oidlarger/smaller: full symbolic u32 × u32 ----------

    proof_support::eq_op2! {
        eq_enum_eq: adt_enum::builtins::fc_enum_eq, pg_enum_eq, u32, as_bool as c_int;
        eq_enum_ne: adt_enum::builtins::fc_enum_ne, pg_enum_ne, u32, as_bool as c_int;
        eq_oidlarger: adt_scalar::builtins::fc_oidlarger, pg_oidlarger, u32, as_u32 as u32;
        eq_oidsmaller: adt_scalar::builtins::fc_oidsmaller, pg_oidsmaller, u32, as_u32 as u32;
    }

    // ---------- int8inc / int8dec: value + verdict + sqlstate, full i64 ----------

    macro_rules! inc_dec_harness {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let arg: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(arg, &mut cval) };
                match call1(adt_int8::builtins::$fc, arg) {
                    Ok(d) => {
                        // C succeeded too, with the identical value.
                        assert!(cerr == 0);
                        assert!(d.as_i64() == cval);
                    }
                    Err(e) => {
                        // C raised ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE):
                        // verdict + sqlstate parity (sqlstate set by the SHIPPED
                        // with_sqlstate call in bigint_out_of_range, not the stub).
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        // Box<PgError> drop glue alone costs ~50-85s of symex
                        // (varbit-rows measured trap); the error value has been
                        // fully adjudicated above — leak it.
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    inc_dec_harness! {
        eq_int8inc: fc_int8inc / pg_int8inc;
        eq_int8dec: fc_int8dec / pg_int8dec;
    }

    // ---------- bit_bit_count / bytea_bit_count ----------

    /// bit_bit_count: valid varbit image (bytelen == ceil(bitlen/8)),
    /// payload fully symbolic (pad bits included — both sides count whole
    /// bytes). Payload cap 8 = bitlen <= 64.
    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bit_bit_count() {
        const CAP: usize = 8;
        let payload: [u8; CAP] = kani::any();
        let bitlen: i32 = kani::any();
        kani::assume(bitlen >= 0 && bitlen <= (CAP * 8) as i32);
        let bytelen = ((bitlen as usize) + 7) / 8;

        // VarBit image: [varsize 4B][bit_len i32 ne][bit_dat...]
        let mut image = [0u8; 8 + CAP];
        let total = 8 + bytelen;
        image[..4].copy_from_slice(&datum::set_varsize_4b(total));
        image[4..8].copy_from_slice(&bitlen.to_ne_bytes());
        let mut i = 0;
        while i < bytelen {
            image[8 + i] = payload[i];
            i += 1;
        }

        let d = Datum::from_usize(image.as_ptr() as usize);
        let r = call_ok(adt_varbit::fc_bit_bit_count, [d]);
        let c = unsafe { pg_bit_bit_count(image.as_ptr().add(8), bitlen) };
        assert!(r.as_i64() == c);
    }

    /// bytea_bit_count: symbolic payload, len <= 7 (Rust scalar popcount
    /// path; len >= 8 = NEON = excluded(blocked:simd), bitutils ruling).
    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_bytea_bit_count() {
        const CAP: usize = 7;
        let payload: [u8; CAP] = kani::any();
        let plen: usize = kani::any();
        kani::assume(plen <= CAP);

        // 4B-uncompressed varlena image: [varsize 4B][payload...]
        let mut image = [0u8; 4 + CAP];
        image[..4].copy_from_slice(&datum::set_varsize_4b(4 + plen));
        let mut i = 0;
        while i < plen {
            image[4 + i] = payload[i];
            i += 1;
        }

        let d = Datum::from_usize(image.as_ptr() as usize);
        let r = call_ok(varlena::builtins::fc_bytea_bit_count, [d]);
        let c = unsafe { pg_bytea_bit_count(image.as_ptr().add(4), plen as c_int) };
        assert!(r.as_i64() == c);
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_enum_eq vs C enum_ne. MUST FAIL with
    /// a counterexample (every pair fails). Run with the DEFAULT solver.
    #[kani::proof]
    fn control_enum_eq_vs_c_ne_must_fail() {
        let a: u32 = kani::any();
        let b: u32 = kani::any();
        let r = call_ok(adt_enum::builtins::fc_enum_eq, [Datum::from_u32(a), Datum::from_u32(b)]);
        let c = unsafe { pg_enum_ne(a, b) };
        assert!(r.as_bool() as c_int == c); // wrong on purpose: eq vs ne
    }
}
