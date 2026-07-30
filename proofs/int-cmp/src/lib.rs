//! Kani C≡Rust equivalence: integer/oid comparator family (64 pg_proc rows).
//!
//! Rust side (shipped, path-deps — never copied):
//!   - adt_int      cmp_ops!  (crates/backend/utils/adt/int/src/lib.rs ~:350)
//!   - adt_int8     relops!   (crates/backend/utils/adt/int8/src/lib.rs ~:135)
//!   - adt_scalar   oid_cmp_ops! (crates/backend/utils/adt/scalar/src/lib.rs)
//!   - nbt_compare  threeway! / btint2cmp / btoidcmp
//! C side: c/pg_intcmp.c (verbatim postgres master int.c/int8.c/oid.c/
//! nbtcompare.c cores; see its header for shims).
//!
//! All harnesses are full-domain symbolic over both arguments (mixed-width
//! variants get both widths symbolic). Expected class: fast (ms each).
//! One negative control (`neg_control_int4lt_is_not_le`) proves the rig is
//! non-vacuous — run it with the DEFAULT solver, not kissat.

#[cfg(kani)]
mod proofs {
    use core::ffi::c_int;

    /// bool-returning comparators: C returns 0/1 int (PG_RETURN_BOOL),
    /// Rust returns bool. Equivalence: c == r as c_int, all inputs symbolic.
    macro_rules! prove_bool_cmp {
        ($($h:ident : $cfn:ident / $rfn:path => ($ta:ty, $tb:ty);)*) => {$(
            extern "C" { fn $cfn(x: $ta, y: $tb) -> c_int; }
            #[kani::proof]
            fn $h() {
                let x: $ta = kani::any();
                let y: $tb = kani::any();
                let c = unsafe { $cfn(x, y) };
                let r = $rfn(x, y);
                assert!(c == r as c_int);
            }
        )*};
    }

    /// three-way btree comparators: both sides return i32.
    macro_rules! prove_threeway_cmp {
        ($($h:ident : $cfn:ident / $rfn:path => ($ta:ty, $tb:ty);)*) => {$(
            extern "C" { fn $cfn(x: $ta, y: $tb) -> i32; }
            #[kani::proof]
            fn $h() {
                let x: $ta = kani::any();
                let y: $tb = kani::any();
                let c = unsafe { $cfn(x, y) };
                let r = $rfn(x, y);
                assert!(c == r);
            }
        )*};
    }

    prove_bool_cmp! {
        // int.c: int2/int4 same-width
        eq_int2eq: pg_int2eq / adt_int::int2eq => (i16, i16);
        eq_int2ne: pg_int2ne / adt_int::int2ne => (i16, i16);
        eq_int2lt: pg_int2lt / adt_int::int2lt => (i16, i16);
        eq_int2le: pg_int2le / adt_int::int2le => (i16, i16);
        eq_int2gt: pg_int2gt / adt_int::int2gt => (i16, i16);
        eq_int2ge: pg_int2ge / adt_int::int2ge => (i16, i16);
        eq_int4eq: pg_int4eq / adt_int::int4eq => (i32, i32);
        eq_int4ne: pg_int4ne / adt_int::int4ne => (i32, i32);
        eq_int4lt: pg_int4lt / adt_int::int4lt => (i32, i32);
        eq_int4le: pg_int4le / adt_int::int4le => (i32, i32);
        eq_int4gt: pg_int4gt / adt_int::int4gt => (i32, i32);
        eq_int4ge: pg_int4ge / adt_int::int4ge => (i32, i32);
        // int.c: mixed 2/4
        eq_int24eq: pg_int24eq / adt_int::int24eq => (i16, i32);
        eq_int24ne: pg_int24ne / adt_int::int24ne => (i16, i32);
        eq_int24lt: pg_int24lt / adt_int::int24lt => (i16, i32);
        eq_int24le: pg_int24le / adt_int::int24le => (i16, i32);
        eq_int24gt: pg_int24gt / adt_int::int24gt => (i16, i32);
        eq_int24ge: pg_int24ge / adt_int::int24ge => (i16, i32);
        eq_int42eq: pg_int42eq / adt_int::int42eq => (i32, i16);
        eq_int42ne: pg_int42ne / adt_int::int42ne => (i32, i16);
        eq_int42lt: pg_int42lt / adt_int::int42lt => (i32, i16);
        eq_int42le: pg_int42le / adt_int::int42le => (i32, i16);
        eq_int42gt: pg_int42gt / adt_int::int42gt => (i32, i16);
        eq_int42ge: pg_int42ge / adt_int::int42ge => (i32, i16);
        // int8.c: same-width
        eq_int8eq: pg_int8eq / adt_int8::int8eq => (i64, i64);
        eq_int8ne: pg_int8ne / adt_int8::int8ne => (i64, i64);
        eq_int8lt: pg_int8lt / adt_int8::int8lt => (i64, i64);
        eq_int8gt: pg_int8gt / adt_int8::int8gt => (i64, i64);
        eq_int8le: pg_int8le / adt_int8::int8le => (i64, i64);
        eq_int8ge: pg_int8ge / adt_int8::int8ge => (i64, i64);
        // int8.c: mixed 8/4
        eq_int84eq: pg_int84eq / adt_int8::int84eq => (i64, i32);
        eq_int84ne: pg_int84ne / adt_int8::int84ne => (i64, i32);
        eq_int84lt: pg_int84lt / adt_int8::int84lt => (i64, i32);
        eq_int84gt: pg_int84gt / adt_int8::int84gt => (i64, i32);
        eq_int84le: pg_int84le / adt_int8::int84le => (i64, i32);
        eq_int84ge: pg_int84ge / adt_int8::int84ge => (i64, i32);
        eq_int48eq: pg_int48eq / adt_int8::int48eq => (i32, i64);
        eq_int48ne: pg_int48ne / adt_int8::int48ne => (i32, i64);
        eq_int48lt: pg_int48lt / adt_int8::int48lt => (i32, i64);
        eq_int48gt: pg_int48gt / adt_int8::int48gt => (i32, i64);
        eq_int48le: pg_int48le / adt_int8::int48le => (i32, i64);
        eq_int48ge: pg_int48ge / adt_int8::int48ge => (i32, i64);
        // int8.c: mixed 8/2
        eq_int82eq: pg_int82eq / adt_int8::int82eq => (i64, i16);
        eq_int82ne: pg_int82ne / adt_int8::int82ne => (i64, i16);
        eq_int82lt: pg_int82lt / adt_int8::int82lt => (i64, i16);
        eq_int82gt: pg_int82gt / adt_int8::int82gt => (i64, i16);
        eq_int82le: pg_int82le / adt_int8::int82le => (i64, i16);
        eq_int82ge: pg_int82ge / adt_int8::int82ge => (i64, i16);
        eq_int28eq: pg_int28eq / adt_int8::int28eq => (i16, i64);
        eq_int28ne: pg_int28ne / adt_int8::int28ne => (i16, i64);
        eq_int28lt: pg_int28lt / adt_int8::int28lt => (i16, i64);
        eq_int28gt: pg_int28gt / adt_int8::int28gt => (i16, i64);
        eq_int28le: pg_int28le / adt_int8::int28le => (i16, i64);
        eq_int28ge: pg_int28ge / adt_int8::int28ge => (i16, i64);
        // oid.c (Oid = u32 on both sides)
        eq_oideq: pg_oideq / adt_scalar::oideq => (u32, u32);
        eq_oidne: pg_oidne / adt_scalar::oidne => (u32, u32);
        eq_oidlt: pg_oidlt / adt_scalar::oidlt => (u32, u32);
        eq_oidle: pg_oidle / adt_scalar::oidle => (u32, u32);
        eq_oidgt: pg_oidgt / adt_scalar::oidgt => (u32, u32);
        eq_oidge: pg_oidge / adt_scalar::oidge => (u32, u32);
    }

    prove_threeway_cmp! {
        eq_btint2cmp: pg_btint2cmp / nbt_compare::btint2cmp => (i16, i16);
        eq_btint4cmp: pg_btint4cmp / nbt_compare::btint4cmp => (i32, i32);
        eq_btint8cmp: pg_btint8cmp / nbt_compare::btint8cmp => (i64, i64);
        eq_btoidcmp:  pg_btoidcmp  / nbt_compare::btoidcmp  => (u32, u32);
    }

    // ---- tail-triage warm-ups (added 2026-07-29, compile-gated only):
    // nbtcompare.c mixed-width cmps (rows 2188-2193), btboolcmp (1693),
    // datum.c btequalimage (5051). C section appended to c/pg_intcmp.c. ----

    prove_threeway_cmp! {
        eq_btint48cmp: pg_btint48cmp / nbt_compare::btint48cmp => (i32, i64);
        eq_btint84cmp: pg_btint84cmp / nbt_compare::btint84cmp => (i64, i32);
        eq_btint24cmp: pg_btint24cmp / nbt_compare::btint24cmp => (i16, i32);
        eq_btint42cmp: pg_btint42cmp / nbt_compare::btint42cmp => (i32, i16);
        eq_btint28cmp: pg_btint28cmp / nbt_compare::btint28cmp => (i16, i64);
        eq_btint82cmp: pg_btint82cmp / nbt_compare::btint82cmp => (i64, i16);
    }

    extern "C" {
        fn pg_btboolcmp(a: u8, b: u8) -> i32;
        fn pg_btequalimage(opcintype: u32) -> c_int;
    }

    /// row 1693: C bool rides as unsigned char {0,1}; full bool x bool
    /// domain, raw (int32)a - (int32)b difference in-theorem.
    #[kani::proof]
    fn eq_btboolcmp() {
        let a: bool = kani::any();
        let b: bool = kani::any();
        let c = unsafe { pg_btboolcmp(a as u8, b as u8) };
        let r = nbt_compare::btboolcmp(a, b);
        assert!(c == r);
    }

    /// row 5051: fc-level (LocalFcinfo frame + Datum bool pack in-theorem)
    /// vs C's unconditional PG_RETURN_BOOL(true), full-u32 opcintype.
    #[kani::proof]
    fn eq_btequalimage() {
        let opcintype: u32 = kani::any();
        let c = unsafe { pg_btequalimage(opcintype) };
        let d = proof_support::call1_ok(nbt_compare::builtins::fc_btequalimage, opcintype);
        assert!((c != 0) == d.as_bool());
    }

    /// Negative control for the appended C section: C btboolcmp against
    /// Rust with SWAPPED arguments — MUST fail (any a != b input is a
    /// counterexample). Run with the default solver.
    #[kani::proof]
    fn neg_control_btboolcmp_argswap() {
        let a: bool = kani::any();
        let b: bool = kani::any();
        let c = unsafe { pg_btboolcmp(a as u8, b as u8) };
        let r = nbt_compare::btboolcmp(b, a);
        assert!(c == r);
    }

    /// Negative control: deliberately compares C int4lt against Rust int4le.
    /// MUST fail with a counterexample (any x == y input) — proves the rig
    /// actually distinguishes implementations. Run with the default solver.
    #[kani::proof]
    fn neg_control_int4lt_is_not_le() {
        let x: i32 = kani::any();
        let y: i32 = kani::any();
        let c = unsafe { pg_int4lt(x, y) };
        let r = adt_int::int4le(x, y);
        assert!(c == r as c_int);
    }
}
