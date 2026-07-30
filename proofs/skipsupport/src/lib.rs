//! Kani C≡Rust equivalence: the skip-support family (pg_proc oids
//! 6402-6409) — btree skip-scan increment/decrement kernels + opclass
//! sentinel constants.
//!
//! Rust side (shipped code, path-dep — never copied):
//!  - nbt_compare::{bool,int2,int4,int8,oid,char}_{decrement,increment}
//!    (crates/backend/access/nbtree/compare/src/lib.rs:74-121)
//!  - adt_date::{date_decrement,date_increment} + DATEVAL_NOBEGIN/NOEND
//!    (crates/backend/utils/adt/adt_date/src/lib.rs:61-82)
//!  - the per-oid low_elem/high_elem/decrement/increment wiring these
//!    theorems pin is the dispatch table in
//!    crates/backend/utils/adt/skipsupport/src/lib.rs:25-73.
//!
//! C side: proofs/skipsupport/c/pg_skipsupport.c (REL_18_STABLE
//! nbtcompare.c + date.c + timestamp.c; provenance + shims documented
//! there).
//!
//! CLAIM SHAPE
//!  - Kernels: full 64-bit-symbolic datum WORD in (strictly wider than the
//!    type domain — the truncating DatumGet* read is in-theorem on both
//!    sides), then (a) overflow/underflow FLAG PARITY over the full domain
//!    and (b) result datum-word equality on the non-flag arm.  On the flag
//!    arm the C contract says "return value is undefined" (both sides
//!    happen to return 0, but that is a non-surface and is deliberately
//!    NOT asserted).
//!  - char kernels compare at VALUE level (low byte) instead of word
//!    level: C's own word padding for char is platform-char-signedness-
//!    dependent (see the C file header) — the low byte is the only
//!    C-defined surface.  Rust zero-extends (matches unsigned-char
//!    platforms, e.g. the ARM64 Linux fleet target).
//!  - Sentinels: each C btXXXskipsupport / date_skipsupport /
//!    timestamp_skipsupport setter runs against a real SkipSupportData
//!    frame and its low_elem/high_elem are asserted equal to the LITERAL
//!    values the shipped Rust dispatcher installs for that proc oid
//!    (skipsupport/src/lib.rs match arms).  The dispatcher fn itself
//!    (prepare_skip_support_from_opclass) is NOT invoked — it requires the
//!    lsyscache catalog substrate; per-oid ROUTING stays in the tested
//!    tier.  Callback-identity (C assigns function pointers; Rust installs
//!    fn items) is covered by the per-kernel equivalence theorems.
//!  - oid 6409 (timestamp_skipsupport): the shipped dispatch routes 6409
//!    to the int8 kernels under the comment claim "DT_NOBEGIN/DT_NOEND
//!    are i64::MIN/MAX, so the int8 kernels are exact"
//!    (skipsupport/src/lib.rs:38-39).  eq_timestamp_* state that claim as
//!    theorems: C timestamp_decrement/increment ≡ Rust int8_decrement/
//!    int8_increment over the full 64-bit word domain, and C's
//!    DT_NOBEGIN/DT_NOEND (vendored macro chain, evaluated in C) ==
//!    i64::MIN/MAX == the timestamp_skipsupport sentinels.
//!
//! Bounds: full 64-bit symbolic datum word everywhere; no fences, no
//! loops (no unwind attributes needed).  Expected class: fast (<1s,
//! pure branch+add circuits).
//!
//! Negative control: control_int4_increment_vs_c_decrement pits Rust
//! int4_increment against C int4_decrement — MUST FAIL with a decodable
//! counterexample (run with the DEFAULT solver; kissat never terminates
//! on failing harnesses).

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use std::os::raw::{c_int, c_void};

    /// utils/skipsupport.h SkipSupportData mirror (callback fields opaque
    /// on the Rust side — the harness never calls through them).
    #[repr(C)]
    struct CSkipSupportData {
        low_elem: u64,
        high_elem: u64,
        decrement: *const c_void,
        increment: *const c_void,
    }

    impl CSkipSupportData {
        fn zeroed() -> Self {
            CSkipSupportData {
                low_elem: 0,
                high_elem: 0,
                decrement: core::ptr::null(),
                increment: core::ptr::null(),
            }
        }
    }

    extern "C" {
        // kernels: (Relation, Datum word, flag out) -> Datum word
        fn pg_bool_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_bool_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_int2_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_int2_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_int4_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_int4_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_int8_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_int8_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_oid_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_oid_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_char_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_char_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_date_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_date_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_timestamp_decrement(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        fn pg_timestamp_increment(rel: *mut c_void, existing: u64, flag: *mut c_int) -> u64;
        // sentinel setters (int return = shimmed PG_RETURN_VOID)
        fn pg_btboolskipsupport(sksup: *mut CSkipSupportData) -> c_int;
        fn pg_btint2skipsupport(sksup: *mut CSkipSupportData) -> c_int;
        fn pg_btint4skipsupport(sksup: *mut CSkipSupportData) -> c_int;
        fn pg_btint8skipsupport(sksup: *mut CSkipSupportData) -> c_int;
        fn pg_btoidskipsupport(sksup: *mut CSkipSupportData) -> c_int;
        fn pg_btcharskipsupport(sksup: *mut CSkipSupportData) -> c_int;
        fn pg_date_skipsupport(sksup: *mut CSkipSupportData) -> c_int;
        fn pg_timestamp_skipsupport(sksup: *mut CSkipSupportData) -> c_int;
        // DT_NOBEGIN/DT_NOEND macro chain, evaluated in C (6409 claim)
        fn pg_dt_nobegin() -> i64;
        fn pg_dt_noend() -> i64;
    }

    /// Full-word kernel equivalence at DATUM-WORD level: flag parity over
    /// the full 64-bit domain + result-word equality on the non-flag arm.
    macro_rules! word_kernel {
        ($($h:ident: $cfn:ident / $rfn:path;)*) => {$(
            #[kani::proof]
            fn $h() {
                let word: u64 = kani::any();
                let mut cflag: c_int = 0;
                let cword = unsafe { $cfn(core::ptr::null_mut(), word, &mut cflag) };
                let mut rflag = false;
                let r = $rfn(Datum::from_u64(word), &mut rflag);
                // both arms reachable — the gate cannot silently narrow
                kani::cover!(cflag != 0);
                kani::cover!(cflag == 0);
                assert!((cflag != 0) == rflag);
                if !rflag {
                    assert!(r.as_u64() == cword);
                }
                // flag-arm return value is C-contract "undefined": not asserted
            }
        )*};
    }

    word_kernel! {
        // oid 6408
        eq_bool_decrement: pg_bool_decrement / nbt_compare::bool_decrement;
        eq_bool_increment: pg_bool_increment / nbt_compare::bool_increment;
        // oid 6402
        eq_int2_decrement: pg_int2_decrement / nbt_compare::int2_decrement;
        eq_int2_increment: pg_int2_increment / nbt_compare::int2_increment;
        // oid 6403
        eq_int4_decrement: pg_int4_decrement / nbt_compare::int4_decrement;
        eq_int4_increment: pg_int4_increment / nbt_compare::int4_increment;
        // oid 6404 (and 6409 via the shipped 6404|6409 routing)
        eq_int8_decrement: pg_int8_decrement / nbt_compare::int8_decrement;
        eq_int8_increment: pg_int8_increment / nbt_compare::int8_increment;
        // oid 6405
        eq_oid_decrement: pg_oid_decrement / nbt_compare::oid_decrement;
        eq_oid_increment: pg_oid_increment / nbt_compare::oid_increment;
        // oid 6407
        eq_date_decrement: pg_date_decrement / adt_date::date_decrement;
        eq_date_increment: pg_date_increment / adt_date::date_increment;
    }

    /// oid 6406 char kernels: VALUE-level (low byte) comparison — C's word
    /// padding for char is platform-char-signedness-dependent (signed x86
    /// sign-extends values > 127; the vendored C pins unsigned char, and
    /// the shipped Rust from_u8 zero-extends, matching the unsigned-char
    /// fleet target).  The low byte is the only C-defined surface.
    macro_rules! char_kernel {
        ($($h:ident: $cfn:ident / $rfn:path;)*) => {$(
            #[kani::proof]
            fn $h() {
                let word: u64 = kani::any();
                let mut cflag: c_int = 0;
                let cword = unsafe { $cfn(core::ptr::null_mut(), word, &mut cflag) };
                let mut rflag = false;
                let r = $rfn(Datum::from_u64(word), &mut rflag);
                kani::cover!(cflag != 0);
                kani::cover!(cflag == 0);
                assert!((cflag != 0) == rflag);
                if !rflag {
                    assert!(r.as_u8() == cword as u8);
                }
            }
        )*};
    }

    char_kernel! {
        eq_char_decrement: pg_char_decrement / nbt_compare::char_decrement;
        eq_char_increment: pg_char_increment / nbt_compare::char_increment;
    }

    /// Sentinel parity: C setter's low_elem/high_elem against the LITERAL
    /// datum expressions the shipped dispatcher installs for that oid
    /// (skipsupport/src/lib.rs match arms — see module doc for why the
    /// dispatcher fn itself is out of reach).
    macro_rules! sentinels {
        ($($h:ident: $cfn:ident, low = $low:expr, high = $high:expr;)*) => {$(
            #[kani::proof]
            fn $h() {
                let mut s = CSkipSupportData::zeroed();
                unsafe { $cfn(&mut s) };
                assert!(s.low_elem == ($low).as_u64());
                assert!(s.high_elem == ($high).as_u64());
            }
        )*};
    }

    sentinels! {
        // oid 6402: Datum::from_i16(i16::MIN/MAX)
        eq_sentinels_btint2: pg_btint2skipsupport,
            low = Datum::from_i16(i16::MIN), high = Datum::from_i16(i16::MAX);
        // oid 6403
        eq_sentinels_btint4: pg_btint4skipsupport,
            low = Datum::from_i32(i32::MIN), high = Datum::from_i32(i32::MAX);
        // oid 6404
        eq_sentinels_btint8: pg_btint8skipsupport,
            low = Datum::from_i64(i64::MIN), high = Datum::from_i64(i64::MAX);
        // oid 6405: Datum::from_u32(0 / u32::MAX)
        eq_sentinels_btoid: pg_btoidskipsupport,
            low = Datum::from_u32(0), high = Datum::from_u32(u32::MAX);
        // oid 6406: Datum::from_u8(0 / u8::MAX)
        eq_sentinels_btchar: pg_btcharskipsupport,
            low = Datum::from_u8(0), high = Datum::from_u8(u8::MAX);
        // oid 6408: Datum::from_bool(false / true)
        eq_sentinels_btbool: pg_btboolskipsupport,
            low = Datum::from_bool(false), high = Datum::from_bool(true);
        // oid 6407: shipped adt_date sentinel CONSTANTS in-theorem
        eq_sentinels_date: pg_date_skipsupport,
            low = Datum::from_i32(adt_date::DATEVAL_NOBEGIN),
            high = Datum::from_i32(adt_date::DATEVAL_NOEND);
        // oid 6409: shipped dispatch installs the int8 row (i64::MIN/MAX)
        eq_sentinels_timestamp: pg_timestamp_skipsupport,
            low = Datum::from_i64(i64::MIN), high = Datum::from_i64(i64::MAX);
    }

    // ---- oid 6409: the DT_NOBEGIN/DT_NOEND comment claim as theorems ----
    //
    // Shipped comment (skipsupport/src/lib.rs:38-39): "timestamp_skipsupport:
    // DT_NOBEGIN/DT_NOEND are i64::MIN/MAX, so the int8 kernels are exact."
    // Three theorems make that a fact:
    //  (1) eq_timestamp_dt_sentinel_constants — C's DT_NOBEGIN/DT_NOEND
    //      macro chain evaluates to i64::MIN/MAX, and those are exactly the
    //      timestamp_skipsupport low/high sentinels;
    //  (2/3) eq_timestamp_decrement_is_int8 / eq_timestamp_increment_is_int8
    //      — C timestamp.c kernels ≡ the SHIPPED int8 kernels the Rust
    //      dispatcher routes 6409 to, over the full word domain.

    #[kani::proof]
    fn eq_timestamp_dt_sentinel_constants() {
        assert!(unsafe { pg_dt_nobegin() } == i64::MIN);
        assert!(unsafe { pg_dt_noend() } == i64::MAX);
        let mut s = CSkipSupportData::zeroed();
        unsafe { pg_timestamp_skipsupport(&mut s) };
        assert!(s.low_elem == unsafe { pg_dt_nobegin() } as u64);
        assert!(s.high_elem == unsafe { pg_dt_noend() } as u64);
    }

    word_kernel! {
        eq_timestamp_decrement_is_int8: pg_timestamp_decrement / nbt_compare::int8_decrement;
        eq_timestamp_increment_is_int8: pg_timestamp_increment / nbt_compare::int8_increment;
    }

    // ---- negative control: rig is non-vacuous ----
    // Rust int4_increment vs C int4_decrement — MUST FAIL (any word whose
    // low 32 bits are not at a sentinel gives x+1 vs x-1).  Run with the
    // DEFAULT solver, never kissat.
    #[kani::proof]
    fn control_int4_increment_vs_c_decrement() {
        let word: u64 = kani::any();
        let mut cflag: c_int = 0;
        let cword = unsafe { pg_int4_decrement(core::ptr::null_mut(), word, &mut cflag) };
        let mut rflag = false;
        let r = nbt_compare::int4_increment(Datum::from_u64(word), &mut rflag);
        assert!((cflag != 0) == rflag);
        if !rflag {
            assert!(r.as_u64() == cword);
        }
    }
}
