//! Kani C≡Rust equivalence: mixed small-scalar comparator batch (~31 pg_proc
//! rows): "char" comparators + btcharcmp, tid comparators + bttidcmp,
//! xid equality (xideq/xidneq), xid8 comparators + xid8cmp, and the
//! array-shaped oidvector comparator family.
//!
//! Rust side: the SHIPPED fmgr wrappers, invoked through a real
//! `LocalFcinfo<2>` frame (datetime-cmp precedent), so datum unwrap →
//! core → Datum pack is inside every theorem:
//!   - adt_char::builtins::fc_char{eq,ne,lt,le,gt,ge}
//!   - nbt_compare::builtins::fc_btcharcmp, fc_btoidvectorcmp
//!   - adt_scalar::builtins::fc_tid{eq,ne,lt,le,gt,ge}, fc_bttidcmp,
//!     fc_xideq, fc_xidneq, fc_xid8{eq,ne,lt,le,gt,ge}, fc_xid8cmp,
//!     fc_oidvector{eq,ne,lt,le,ge,gt}
//! C side: c/pg_scalar_misc.c (verbatim postgres master char.c/tid.c/xid.c/
//! oid.c/nbtcompare.c/itemptr.c cores; see its header for shims).
//!
//! Domains:
//!   - char: full symbolic i8 × i8 (C compares as unsigned uint8 in the
//!     ordering ops, plain equality in eq/ne — the proof adjudicates the
//!     Rust `as u8` casts against master's exact casts).
//!   - tid: full symbolic ItemPointerData structs — both (bi_hi, bi_lo,
//!     ip_posid) u16 triples symbolic; the Rust side gets the same three
//!     u16s serialized into the 6-byte on-tuple image arg_tid parses
//!     ((hi << 16) | lo block-number composition on both sides).
//!   - xid: full symbolic u32 × u32 (equality only; xid ORDERING is modular
//!     and out of scope).
//!   - xid8: full symbolic u64 × u64.
//!   - oidvector: symbolic dim1 in 0..=4 for BOTH args independently (no
//!     per-n case-split needed — symbolic-n solved well inside budget),
//!     4 fully-symbolic u32 elements each, symbolic lbound1 (C ignores it —
//!     the proof checks the Rust does too); header fields ndim/dataoffset/
//!     elemtype pinned to the layout-valid values (1, 0, OIDOID) so the
//!     C ereport shim / Rust check_valid_oidvector error arm is unreachable:
//!     value-space only over layout-valid oidvectors, error paths leave the
//!     proof. dim1<=4 is the FLEXIBLE_ARRAY_MEMBER shim cap.
//!
//! Negative controls (run with the DEFAULT solver, kissat never terminates
//! on failing harnesses): control_charlt_vs_c_charle (scalar rig) and
//! control_oidvectorlt_vs_c_le (array rig) — both MUST fail.
//!
//! WAVE 5 (2026-07-28) adds two sibling modules (see their module docs and
//! runqueue.txt for the per-harness run recipes):
//!   - `wave5`   — cid rows (cideq/cidout), xidout digit bands, the
//!     oid/xid/cid/xid8/tid recv+send wire rows, and the xid_age/mxid_age
//!     state-seam rows.
//!   - `xid8snap` — the xid8funcs pg_snapshot family (epoch-arithmetic
//!     core, xmin/xmax accessors, visibility linear+bsearch arms, in/out/
//!     recv/send).

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use std::os::raw::c_int;
    use types_fmgr::LocalFcinfo;

    extern "C" {
        fn pg_chareq(a: i8, b: i8) -> c_int;
        fn pg_charne(a: i8, b: i8) -> c_int;
        fn pg_charlt(a: i8, b: i8) -> c_int;
        fn pg_charle(a: i8, b: i8) -> c_int;
        fn pg_chargt(a: i8, b: i8) -> c_int;
        fn pg_charge(a: i8, b: i8) -> c_int;
        fn pg_btcharcmp(a: i8, b: i8) -> i32;

        fn pg_tideq(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
        fn pg_tidne(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
        fn pg_tidlt(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
        fn pg_tidle(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
        fn pg_tidgt(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
        fn pg_tidge(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
        fn pg_bttidcmp(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> i32;

        fn pg_xideq(a: u32, b: u32) -> c_int;
        fn pg_xidneq(a: u32, b: u32) -> c_int;

        fn pg_xid8eq(a: u64, b: u64) -> c_int;
        fn pg_xid8ne(a: u64, b: u64) -> c_int;
        fn pg_xid8lt(a: u64, b: u64) -> c_int;
        fn pg_xid8gt(a: u64, b: u64) -> c_int;
        fn pg_xid8le(a: u64, b: u64) -> c_int;
        fn pg_xid8ge(a: u64, b: u64) -> c_int;
        fn pg_xid8cmp(a: u64, b: u64) -> i32;

        fn pg_btoidvectorcmp(a: *const core::ffi::c_void, b: *const core::ffi::c_void) -> i32;
        fn pg_oidvectoreq(a: *const core::ffi::c_void, b: *const core::ffi::c_void) -> c_int;
        fn pg_oidvectorne(a: *const core::ffi::c_void, b: *const core::ffi::c_void) -> c_int;
        fn pg_oidvectorlt(a: *const core::ffi::c_void, b: *const core::ffi::c_void) -> c_int;
        fn pg_oidvectorle(a: *const core::ffi::c_void, b: *const core::ffi::c_void) -> c_int;
        fn pg_oidvectorge(a: *const core::ffi::c_void, b: *const core::ffi::c_void) -> c_int;
        fn pg_oidvectorgt(a: *const core::ffi::c_void, b: *const core::ffi::c_void) -> c_int;
    }

    /// Run a shipped fc_* wrapper on a 2-arg frame; these comparators only
    /// error via check_valid_oidvector, unreachable under the harness fence.
    fn call<E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        a: Datum,
        b: Datum,
    ) -> Datum {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("comparator errored"),
        }
    }

    // ---------- "char": full symbolic i8 × i8 ----------

    macro_rules! char_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: i8 = kani::any();
                let b: i8 = kani::any();
                let r = call(adt_char::builtins::$fc, Datum::from_char(a), Datum::from_char(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    char_op! {
        eq_chareq: fc_chareq / pg_chareq;
        eq_charne: fc_charne / pg_charne;
        eq_charlt: fc_charlt / pg_charlt;
        eq_charle: fc_charle / pg_charle;
        eq_chargt: fc_chargt / pg_chargt;
        eq_charge: fc_charge / pg_charge;
    }

    /// fc_btcharcmp reads the datum via as_u8; a char datum's payload byte is
    /// the same either way, so feed the identical i8 bit-pattern both sides.
    #[kani::proof]
    fn eq_btcharcmp() {
        let a: i8 = kani::any();
        let b: i8 = kani::any();
        let r = call(
            nbt_compare::builtins::fc_btcharcmp,
            Datum::from_u8(a as u8),
            Datum::from_u8(b as u8),
        );
        let c = unsafe { pg_btcharcmp(a, b) };
        assert!(r.as_i32() == c);
    }

    // ---------- tid: full symbolic (bi_hi, bi_lo, ip_posid) structs ----------

    /// The 6-byte on-tuple image fmgr hands the wrapper: BlockIdData
    /// {bi_hi, bi_lo} then OffsetNumber, native-endian u16 each — exactly
    /// what adt_scalar's arg_tid parses.
    fn tid_img(hi: u16, lo: u16, off: u16) -> [u8; 6] {
        let (h, l, o) = (hi.to_ne_bytes(), lo.to_ne_bytes(), off.to_ne_bytes());
        [h[0], h[1], l[0], l[1], o[0], o[1]]
    }

    macro_rules! tid_op {
        ($($h:ident: $fc:ident / $pg:ident $extract:ident $cast:ty;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (ah, al, ao): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
                let (bh, bl, bo): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
                let ia = tid_img(ah, al, ao);
                let ib = tid_img(bh, bl, bo);
                let r = call(
                    adt_scalar::builtins::$fc,
                    Datum::from_usize(ia.as_ptr() as usize),
                    Datum::from_usize(ib.as_ptr() as usize),
                );
                let c = unsafe { $pg(ah, al, ao, bh, bl, bo) };
                assert!(r.$extract() as $cast == c);
            }
        )*};
    }

    tid_op! {
        eq_tideq: fc_tideq / pg_tideq as_bool c_int;
        eq_tidne: fc_tidne / pg_tidne as_bool c_int;
        eq_tidlt: fc_tidlt / pg_tidlt as_bool c_int;
        eq_tidle: fc_tidle / pg_tidle as_bool c_int;
        eq_tidgt: fc_tidgt / pg_tidgt as_bool c_int;
        eq_tidge: fc_tidge / pg_tidge as_bool c_int;
        eq_bttidcmp: fc_bttidcmp / pg_bttidcmp as_i32 i32;
    }

    // ---------- xid (equality only) + xid8: full symbolic ----------

    macro_rules! word_op {
        ($($h:ident: $fc:ident / $pg:ident, $ty:ty, $from:ident, $extract:ident, $cast:ty;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $ty = kani::any();
                let b: $ty = kani::any();
                let r = call(adt_scalar::builtins::$fc, Datum::$from(a), Datum::$from(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.$extract() as $cast == c);
            }
        )*};
    }

    word_op! {
        eq_xideq:  fc_xideq  / pg_xideq,  u32, from_u32, as_bool, c_int;
        eq_xidneq: fc_xidneq / pg_xidneq, u32, from_u32, as_bool, c_int;
        eq_xid8eq: fc_xid8eq / pg_xid8eq, u64, from_u64, as_bool, c_int;
        eq_xid8ne: fc_xid8ne / pg_xid8ne, u64, from_u64, as_bool, c_int;
        eq_xid8lt: fc_xid8lt / pg_xid8lt, u64, from_u64, as_bool, c_int;
        eq_xid8gt: fc_xid8gt / pg_xid8gt, u64, from_u64, as_bool, c_int;
        eq_xid8le: fc_xid8le / pg_xid8le, u64, from_u64, as_bool, c_int;
        eq_xid8ge: fc_xid8ge / pg_xid8ge, u64, from_u64, as_bool, c_int;
        eq_xid8cmp: fc_xid8cmp / pg_xid8cmp, u64, from_u64, as_i32, i32;
    }

    // ---------- tidlarger / tidsmaller (2795/2796) ----------
    // Winning-input identity over the pure selection cores tid_larger /
    // tid_smaller (factored out of the fc wrappers for provability — the
    // wrapper only adds the byref-result copy of the winner). Full
    // symbolic u16 triples both sides; Tid built with arg_tid's
    // (hi << 16) | lo block composition.

    extern "C" {
        fn pg_tidlarger(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
        fn pg_tidsmaller(ah: u16, al: u16, ao: u16, bh: u16, bl: u16, bo: u16) -> c_int;
    }

    macro_rules! tid_minmax {
        ($h:ident: $core:ident / $pg:ident;)  => {
            #[kani::proof]
            fn $h() {
                let (ah, al, ao): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
                let (bh, bl, bo): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
                let ta = adt_scalar::Tid {
                    block: ((ah as u32) << 16) | (al as u32),
                    offset: ao,
                };
                let tb = adt_scalar::Tid {
                    block: ((bh as u32) << 16) | (bl as u32),
                    offset: bo,
                };
                let r = adt_scalar::$core(ta, tb);
                let c = unsafe { $pg(ah, al, ao, bh, bl, bo) };
                assert!(r == if c == 1 { ta } else { tb });
            }
        };
    }

    tid_minmax!(eq_tidlarger: tid_larger / pg_tidlarger;);
    tid_minmax!(eq_tidsmaller: tid_smaller / pg_tidsmaller;);

    // ---------- xid8_larger/smaller (5097/5098) + xid8toxid (5071) ----------

    extern "C" {
        fn pg_xid8_larger(a: u64, b: u64) -> u64;
        fn pg_xid8_smaller(a: u64, b: u64) -> u64;
        fn pg_xid8toxid(a: u64) -> u32;
    }

    #[kani::proof]
    fn eq_xid8_larger() {
        let a: u64 = kani::any();
        let b: u64 = kani::any();
        let r = call(
            adt_scalar::builtins::fc_xid8_larger,
            Datum::from_u64(a),
            Datum::from_u64(b),
        );
        let c = unsafe { pg_xid8_larger(a, b) };
        assert!(r.as_u64() == c);
    }

    #[kani::proof]
    fn eq_xid8_smaller() {
        let a: u64 = kani::any();
        let b: u64 = kani::any();
        let r = call(
            adt_scalar::builtins::fc_xid8_smaller,
            Datum::from_u64(a),
            Datum::from_u64(b),
        );
        let c = unsafe { pg_xid8_smaller(a, b) };
        assert!(r.as_u64() == c);
    }

    #[kani::proof]
    fn eq_xid8toxid() {
        let a: u64 = kani::any();
        // 1-arg frame; second slot of the shared 2-arg `call` helper is not
        // read by a 1-arg wrapper (args_n::<1>() takes the prefix).
        let r = call(
            adt_scalar::builtins::fc_xid8toxid,
            Datum::from_u64(a),
            Datum::from_u64(0),
        );
        let c = unsafe { pg_xid8toxid(a) };
        assert!(r.as_u32() == c);
    }

    // ---------- oidvector: symbolic dim1 0..=4, symbolic elements ----------

    /// Layout-locked image matching C's oidvector (24-byte header + values
    /// tail); values[4] mirrors the C shim cap.
    #[repr(C)]
    struct OidVec4 {
        hdr: array::oidvector,
        values: [u32; 4],
    }

    const OIDOID: u32 = 26;

    fn ovec(dim1: i32, lbound1: i32, values: [u32; 4]) -> OidVec4 {
        OidVec4 {
            hdr: array::oidvector {
                vl_len_: 0, // never read by the compared code
                ndim: 1,
                dataoffset: 0,
                elemtype: OIDOID,
                dim1,
                lbound1,
            },
            values,
        }
    }

    macro_rules! ovec_op {
        ($($h:ident[$unwind:literal]: $path:path, $pg:ident $extract:ident $cast:ty;)*) => {$(
            #[kani::proof]
            #[kani::unwind($unwind)]
            fn $h() {
                let (na, nb): (i32, i32) = (kani::any(), kani::any());
                kani::assume((0..=4).contains(&na) && (0..=4).contains(&nb));
                let a = ovec(na, kani::any(), kani::any());
                let b = ovec(nb, kani::any(), kani::any());
                let r = call(
                    $path,
                    Datum::from_usize(&a as *const OidVec4 as usize),
                    Datum::from_usize(&b as *const OidVec4 as usize),
                );
                let c = unsafe {
                    $pg(
                        &a as *const OidVec4 as *const core::ffi::c_void,
                        &b as *const OidVec4 as *const core::ffi::c_void,
                    )
                };
                assert!(r.$extract() as $cast == c);
            }
        )*};
    }

    ovec_op! {
        eq_btoidvectorcmp[6]: nbt_compare::builtins::fc_btoidvectorcmp, pg_btoidvectorcmp as_i32 i32;
        eq_oidvectoreq[18]: adt_scalar::builtins::fc_oidvectoreq, pg_oidvectoreq as_bool c_int;
        eq_oidvectorne[6]: adt_scalar::builtins::fc_oidvectorne, pg_oidvectorne as_bool c_int;
        eq_oidvectorlt[6]: adt_scalar::builtins::fc_oidvectorlt, pg_oidvectorlt as_bool c_int;
        eq_oidvectorle[6]: adt_scalar::builtins::fc_oidvectorle, pg_oidvectorle as_bool c_int;
        eq_oidvectorge[6]: adt_scalar::builtins::fc_oidvectorge, pg_oidvectorge as_bool c_int;
        eq_oidvectorgt[6]: adt_scalar::builtins::fc_oidvectorgt, pg_oidvectorgt as_bool c_int;
    }

    // ---------- negative controls: the rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_charlt vs C charle. MUST fail with a
    /// counterexample at a == b. DEFAULT solver only.
    #[kani::proof]
    fn control_charlt_vs_c_charle() {
        let a: i8 = kani::any();
        let b: i8 = kani::any();
        let r = call(adt_char::builtins::fc_charlt, Datum::from_char(a), Datum::from_char(b));
        let c = unsafe { pg_charle(a, b) };
        assert!(r.as_bool() as c_int == c);
    }

    /// Deliberate mismatch on the array-shaped rig: shipped fc_oidvectorlt
    /// vs C oidvectorle. MUST fail (any a == b input). DEFAULT solver only.
    #[kani::proof]
    #[kani::unwind(6)]
    fn control_oidvectorlt_vs_c_le() {
        let (na, nb): (i32, i32) = (kani::any(), kani::any());
        kani::assume((0..=4).contains(&na) && (0..=4).contains(&nb));
        let a = ovec(na, kani::any(), kani::any());
        let b = ovec(nb, kani::any(), kani::any());
        let r = call(
            adt_scalar::builtins::fc_oidvectorlt,
            Datum::from_usize(&a as *const OidVec4 as usize),
            Datum::from_usize(&b as *const OidVec4 as usize),
        );
        let c = unsafe {
            pg_oidvectorle(
                &a as *const OidVec4 as *const core::ffi::c_void,
                &b as *const OidVec4 as *const core::ffi::c_void,
            )
        };
        assert!(r.as_bool() as c_int == c);
    }
}

// ===========================================================================
// WAVE 5 — scalar rows: cideq/cidout, xidout bands, wire recv/send, and the
// xid_age/mxid_age state-seam rows.
//
// C side: c/pg_scalar_misc.c WAVE 5 section (+ ../intout/c/pg_intout.c for
// the pg_ultoa_n decimal reference — pass BOTH files via --c-lib).
//
// Theorem shapes (all documented per-harness):
//   - cideq (69): wrapper-level, full symbolic u32 pair.
//   - xidout (51): CORE-level (crate::xidout) against PostgreSQL's own
//     pg_ultoa_n — C xidout is snprintf("%lu"), which has no CBMC model;
//     pg_ultoa_n is a documented SPEC-LEVEL ANCHOR for %lu's canonical
//     decimal (see the C header note).  intout digit-band ladder: symbolic
//     [0,1e7) in 5 bands + coverage + concrete spots d8-d10 incl u32::MAX.
//   - cidout (53): WRAPPER-level (fc_cidout: TLS scratch + pg_ultoa_n +
//     NUL), same band ladder, NUL terminator in-theorem.
//   - sends (2419 oidsend / 2441 xidsend [2443 cidsend shares fc_xidsend] /
//     5083 xid8send / 2439 tidsend): wrapper-level over a real result-mcx
//     frame (int-arith send precedent, RELEASE-GATE tier expected); the
//     ENTIRE wire image (4B varlena header + BE payload) byte-compared.
//   - recvs (2418 oidrecv / 2440 xidrecv [2442 cidrecv shares the body] /
//     5082 xid8recv / 2438 tidrecv): CORE-level — the harness states the
//     one-call wrapper body (pq_getmsgint/pq_getmsgint64 on a directly-held
//     StringInfo) exactly as shipped (fc_oidrecv/fc_xidrecv/fc_xid8recv/
//     fc_tidrecv builtins.rs bodies), avoiding the datum->&mut StringInfo
//     pointer round-trip that WALLED the int-arith wrapper-level recv
//     harnesses (symex provenance checks).  Ledger wording: "core-level
//     (direct StringInfo); fc datum plumbing out of proof (int-arith
//     recv-wall precedent)".  Value + cursor advance + verdict + sqlstate
//     08P01 parity.
//   - xid_age (1181) / mxid_age (3939): state-seam pattern (nextval
//     precedent) — the GetStableLatestTransactionId()/ReadNextMultiXactId()
//     read is ONE shared symbolic value fed to both sides (Rust via
//     kani::stub of the seam call, C via parameter); proof quantifies over
//     ALL seam outputs; control_xid_age_seam_skew must FAIL.
//
// Run recipes: see runqueue.txt (kissat for expected-green; DEFAULT solver
// for the control_* must-fail harnesses; mcx-stub recipe needs -Z stubbing).
// ===========================================================================

#[cfg(kani)]
mod wave5 {
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
    use types_error::{ERRCODE_PROTOCOL_VIOLATION, ERROR};
    use types_fmgr::LocalFcinfo;

    extern "C" {
        // WAVE 5 section of c/pg_scalar_misc.c
        fn pg_cideq(a: u32, b: u32) -> c_int;
        fn pg_xid_age(now: u32, xid: u32) -> i32;
        fn pg_mxid_age(now: u32, xid: u32) -> i32;
        fn pg_getmsguint32(data: *const u8, len: i32, cursor: *mut i32, out: *mut u32) -> c_int;
        fn pg_getmsguint64(data: *const u8, len: i32, cursor: *mut i32, out: *mut u64) -> c_int;
        fn pg_tidrecv(
            data: *const u8,
            len: i32,
            cursor: *mut i32,
            block: *mut u32,
            offset: *mut u16,
        ) -> c_int;
        fn pg_send_uint32(arg1: u32, out: *mut u8) -> i32;
        fn pg_send_uint64(v: u64, out: *mut u8) -> i32;
        fn pg_tidsend(block: u32, offset: u16, out: *mut u8) -> i32;
        // ../intout/c/pg_intout.c (second --c-lib): the decimal reference
        fn pg_ultoa_n(value: u32, a: *mut u8) -> c_int;
    }

    // ---------------- cideq (69): full symbolic u32 x u32 ----------------

    #[kani::proof]
    fn eq_cideq() {
        let a: u32 = kani::any();
        let b: u32 = kani::any();
        let r = proof_support::call2_ok(adt_scalar::builtins::fc_cideq, a, b);
        let c = unsafe { pg_cideq(a, b) };
        assert!(r.as_bool() as c_int == c);
    }

    // ---------------- xidout (51): core vs pg_ultoa_n, intout bands -------

    fn xidout_case(v: u32) {
        let mut cbuf = [0u8; 16];
        let clen = unsafe { pg_ultoa_n(v, cbuf.as_mut_ptr()) } as usize;
        let mut rbuf = [0u8; 16];
        let rlen = adt_scalar::xidout(v, &mut rbuf);
        assert!(clen == rlen);
        let mut i = 0;
        while i < rlen {
            assert!(cbuf[i] == rbuf[i]);
            i += 1;
        }
    }

    macro_rules! xidout_band {
        ($($h:ident[$uw:literal]: $lo:literal .. $hi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)] // digit loops <= band digit count + 1; the
                                 // C side's pg_leftmost_one_pos32 table walk
                                 // runs <= 4 (u32) — unwind covers both
            fn $h() {
                let v: u32 = kani::any();
                kani::assume(v >= $lo && v < $hi);
                xidout_case(v);
            }
        )*};
    }

    xidout_band! {
        eq_xidout_r1_lt1e4[7]: 0u32 .. 10_000u32;
        eq_xidout_d5[8]:  10_000u32 .. 100_000u32;
        eq_xidout_d6[9]:  100_000u32 .. 1_000_000u32;
        eq_xidout_d7a[10]: 1_000_000u32 .. 5_000_000u32;
        eq_xidout_d7b[10]: 5_000_000u32 .. 10_000_000u32;
    }

    /// Concrete spots for the d8-d10 regimes (sloped-wall remainder,
    /// intout precedent).
    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_xidout_spots() {
        xidout_case(10_000_000);
        xidout_case(99_999_999);
        xidout_case(100_000_000);
        xidout_case(999_999_999);
        xidout_case(1_000_000_000);
        xidout_case(u32::MAX);
    }

    /// MANDATORY union coverage for the symbolic domain [0, 1e7).
    #[kani::proof]
    fn cover_xidout_split() {
        let v: u32 = kani::any();
        kani::assume(v < 10_000_000);
        assert!(
            v < 10_000
                || (v >= 10_000 && v < 100_000)
                || (v >= 100_000 && v < 1_000_000)
                || (v >= 1_000_000 && v < 5_000_000)
                || (v >= 5_000_000 && v < 10_000_000)
        );
    }

    // ---------------- cidout (53): wrapper-level, same bands --------------

    fn cidout_case(v: u32) {
        let mut cbuf = [0u8; 16];
        let clen = unsafe { pg_ultoa_n(v, cbuf.as_mut_ptr()) } as usize;
        let r = proof_support::call1_ok(adt_scalar::builtins::fc_cidout, v);
        // cstring result datum: points at the wrapper's TLS scratch.
        let img = unsafe { core::slice::from_raw_parts(r.as_usize() as *const u8, clen + 1) };
        let mut i = 0;
        while i < clen {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        assert!(img[clen] == 0, "cstring NUL terminator");
    }

    macro_rules! cidout_band {
        ($($h:ident[$uw:literal]: $lo:literal .. $hi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            fn $h() {
                let v: u32 = kani::any();
                kani::assume(v >= $lo && v < $hi);
                cidout_case(v);
            }
        )*};
    }

    cidout_band! {
        eq_cidout_r1_lt1e4[7]: 0u32 .. 10_000u32;
        eq_cidout_d5[8]:  10_000u32 .. 100_000u32;
        eq_cidout_d6[9]:  100_000u32 .. 1_000_000u32;
        eq_cidout_d7a[10]: 1_000_000u32 .. 5_000_000u32;
        eq_cidout_d7b[10]: 5_000_000u32 .. 10_000_000u32;
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_cidout_spots() {
        cidout_case(10_000_000);
        cidout_case(999_999_999);
        cidout_case(1_000_000_000);
        cidout_case(u32::MAX);
    }

    /// Same symbolic domain claim as cover_xidout_split (bands identical).
    #[kani::proof]
    fn cover_cidout_split() {
        let v: u32 = kani::any();
        kani::assume(v < 10_000_000);
        assert!(
            v < 10_000
                || (10_000..100_000).contains(&v)
                || (100_000..1_000_000).contains(&v)
                || (1_000_000..5_000_000).contains(&v)
                || (5_000_000..10_000_000).contains(&v)
        );
    }

    // ---------------- sends: full wire image over a result-mcx frame ------
    // Scaffolding = the proof_support mcx-stubs recipe (int-arith send
    // precedent); theorem qualifier "modulo static-buffer allocator model".

    macro_rules! send_harness {
        ($($h:ident: $krate:ident :: $fc:ident ($ta:ty) $from:ident / $pg:ident, total=$total:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let a: $ta = kani::any();
                let mut cbuf = [0u8; $total];
                let clen = unsafe { $pg(a, cbuf.as_mut_ptr()) };

                let ctx = mcx::MemoryContext::new_bump("kani-wave5-send");
                let mut f = LocalFcinfo::<1>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] = NullableDatum::value(Datum::$from(a));
                let d = match $krate::builtins::$fc(None, &mut f) {
                    Ok(d) => d,
                    Err(e) => { core::mem::forget(e); panic!("send errored") }
                };
                let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, $total) };
                assert!(clen == $total as i32);
                let mut i = 0;
                while i < $total {
                    assert!(img[i] == cbuf[i]);
                    i += 1;
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    send_harness! {
        eq_oidsend:  adt_scalar::fc_oidsend  (u32) from_oid / pg_send_uint32, total=8,  unwind=10;
        // xidsend; row 2443 cidsend registers the SAME fc_xidsend body.
        eq_xidsend:  adt_scalar::fc_xidsend  (u32) from_u32 / pg_send_uint32, total=8,  unwind=10;
        eq_xid8send: adt_scalar::fc_xid8send (u64) from_u64 / pg_send_uint64, total=12, unwind=14;
    }

    /// tidsend (2439): the 6-byte tid image arg -> int4 block + int2 offset
    /// wire payload (10-byte image incl header).
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_tidsend() {
        let (hi, lo, off): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
        let block = ((hi as u32) << 16) | lo as u32;
        let mut cbuf = [0u8; 10];
        let clen = unsafe { pg_tidsend(block, off, cbuf.as_mut_ptr()) };

        // the 6-byte on-tuple image fmgr hands the wrapper (arg_tid layout)
        let (h, l, o) = (hi.to_ne_bytes(), lo.to_ne_bytes(), off.to_ne_bytes());
        let tid_img = [h[0], h[1], l[0], l[1], o[0], o[1]];

        let ctx = mcx::MemoryContext::new_bump("kani-wave5-tidsend");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(tid_img.as_ptr() as usize));
        let d = match adt_scalar::builtins::fc_tidsend(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("tidsend errored")
            }
        };
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 10) };
        assert!(clen == 10);
        let mut i = 0;
        while i < 10 {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }

    /// MUST FAIL (control for the new wire C section): shipped fc_xid8send
    /// image vs C's uint32 send of the low half — header + width mismatch.
    /// DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_xid8send_vs_c_uint32send() {
        let a: u64 = kani::any();
        let mut cbuf = [0u8; 8];
        let _ = unsafe { pg_send_uint32(a as u32, cbuf.as_mut_ptr()) };
        let ctx = mcx::MemoryContext::new_bump("kani-wave5-ctl");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_u64(a));
        let d = match adt_scalar::builtins::fc_xid8send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("send errored")
            }
        };
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 8) };
        let mut i = 0;
        while i < 8 {
            assert!(img[i] == cbuf[i]); // expected failure at the header byte
            i += 1;
        }
        core::mem::forget(ctx);
    }

    // ---------------- recvs: core-level, direct StringInfo ----------------
    // States the shipped one-call wrapper bodies (builtins.rs fc_oidrecv /
    // fc_xidrecv / fc_xid8recv) on a directly-held StringInfo — the datum
    // round-trip that walls symex stays out (int-arith recv-wall lesson).
    // Full symbolic message bytes, symbolic data length AND cursor (incl.
    // cursor > len and short-buffer planes).

    macro_rules! recv_core {
        ($($h:ident: $call:expr, $pg:ident -> $ty:ty, cap=$cap:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                const CAP: usize = $cap;
                let data: [u8; CAP] = kani::any();
                let dlen: usize = kani::any();
                kani::assume(dlen <= CAP);
                let cur: usize = kani::any();
                kani::assume(cur <= CAP);

                let mut ccur: i32 = cur as i32;
                let mut cout: $ty = 0;
                let cst = unsafe { $pg(data.as_ptr(), dlen as i32, &mut ccur, &mut cout) };

                let ctx = mcx::MemoryContext::new_bump("kani-wave5-recv");
                let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
                    Ok(s) => s,
                    Err(e) => { core::mem::forget(e); panic!("stub alloc failed") }
                };
                if let Err(e) = si.append_bytes(&data[..dlen]) {
                    core::mem::forget(e);
                    panic!("append within capacity failed");
                }
                si.cursor = cur;
                let call: fn(&mut stringinfo::StringInfo<'_>) -> types_error::PgResult<$ty> = $call;
                match call(&mut si) {
                    Ok(v) => {
                        assert!(cst == 0);
                        assert!(v == cout);
                        assert!(si.cursor == ccur as usize);
                    }
                    Err(e) => {
                        assert!(cst == 4);
                        assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
                core::mem::forget(si);
                core::mem::forget(ctx);
            }
        )*};
    }

    recv_core! {
        // fc_oidrecv body: pq_getmsgint(buf, 4) as u32 (rows 2418; 2440
        // xidrecv and 2442 cidrecv state the identical body).
        eq_oidrecv_core: |si| Ok(::pqformat::pq_getmsgint(si, 4)? as u32), pg_getmsguint32 -> u32, cap=8, unwind=12;
        // fc_xid8recv body: pq_getmsgint64(buf) as u64 (row 5082).
        eq_xid8recv_core: |si| Ok(::pqformat::pq_getmsgint64(si)? as u64), pg_getmsguint64 -> u64, cap=12, unwind=16;
    }

    // RUN-VERIFY ladder rung 3 (per-length cells): the symbolic-dlen
    // harnesses above wall in symex on the `append_bytes(&data[..dlen])`
    // symbolic-length copy (derived-length copy wall class; the control's
    // concrete-length append completes in 58s).  Per-length cells make the
    // copy length concrete; cursor stays fully symbolic.  Union coverage:
    // dlen ranges are exhaustive over 0..=CAP by construction (one cell per
    // length), so no separate coverage harness is required — the cells
    // partition the assumed domain syntactically.
    macro_rules! recv_core_len {
        ($($h:ident: $call:expr, $pg:ident -> $ty:ty, cap=$cap:expr, dlen=$dl:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                const CAP: usize = $cap;
                const DLEN: usize = $dl;
                let data: [u8; CAP] = kani::any();
                let cur: usize = kani::any();
                kani::assume(cur <= CAP);

                let mut ccur: i32 = cur as i32;
                let mut cout: $ty = 0;
                let cst = unsafe { $pg(data.as_ptr(), DLEN as i32, &mut ccur, &mut cout) };

                let ctx = mcx::MemoryContext::new_bump("kani-wave5-recv");
                let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
                    Ok(s) => s,
                    Err(e) => { core::mem::forget(e); panic!("stub alloc failed") }
                };
                if let Err(e) = si.append_bytes(&data[..DLEN]) {
                    core::mem::forget(e);
                    panic!("append within capacity failed");
                }
                si.cursor = cur;
                let call: fn(&mut stringinfo::StringInfo<'_>) -> types_error::PgResult<$ty> = $call;
                match call(&mut si) {
                    Ok(v) => {
                        assert!(cst == 0);
                        assert!(v == cout);
                        assert!(si.cursor == ccur as usize);
                    }
                    Err(e) => {
                        assert!(cst == 4);
                        assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
                core::mem::forget(si);
                core::mem::forget(ctx);
            }
        )*};
    }

    recv_core_len! {
        // probe cells first; full 0..=CAP ladders added on probe success
        eq_oidrecv_core_len8: |si| Ok(::pqformat::pq_getmsgint(si, 4)? as u32), pg_getmsguint32 -> u32, cap=8, dlen=8, unwind=12;
        eq_oidrecv_core_len3: |si| Ok(::pqformat::pq_getmsgint(si, 4)? as u32), pg_getmsguint32 -> u32, cap=8, dlen=3, unwind=12;
    }

    /// tidrecv (2438) core: block = pq_getmsgint(buf,4); offset =
    /// pq_getmsgint(buf,2) — the fc_tidrecv body before the byref result
    /// copy.  Verdict + both fields + cursor.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_tidrecv_core() {
        const CAP: usize = 8;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);
        let cur: usize = kani::any();
        kani::assume(cur <= CAP);

        let mut ccur: i32 = cur as i32;
        let (mut cblock, mut coff): (u32, u16) = (0, 0);
        let cst = unsafe { pg_tidrecv(data.as_ptr(), dlen as i32, &mut ccur, &mut cblock, &mut coff) };

        let ctx = mcx::MemoryContext::new_bump("kani-wave5-tidrecv");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(&data[..dlen]) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        si.cursor = cur;
        let r: types_error::PgResult<(u32, u16)> = (|| {
            let block = ::pqformat::pq_getmsgint(&mut si, 4)? as u32;
            let offset = ::pqformat::pq_getmsgint(&mut si, 2)? as u16;
            Ok((block, offset))
        })();
        match r {
            Ok((block, offset)) => {
                assert!(cst == 0);
                assert!(block == cblock);
                assert!(offset == coff);
                assert!(si.cursor == ccur as usize);
            }
            Err(e) => {
                assert!(cst == 4);
                assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// Both-arm reachability for the recv-core rig (gate insurance; covers
    /// hoisted into ONE harness per the kissat property-batch lesson).
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn cover_recv_core_both_arms() {
        const CAP: usize = 8;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);
        let cur: usize = kani::any();
        kani::assume(cur <= CAP);
        let ctx = mcx::MemoryContext::new_bump("kani-wave5-recv-cover");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(&data[..dlen]) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        si.cursor = cur;
        match ::pqformat::pq_getmsgint(&mut si, 4) {
            Ok(_) => kani::cover!(true, "recv-core Ok arm reachable"),
            Err(e) => {
                kani::cover!(true, "recv-core Err arm reachable");
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// MUST FAIL (recv-core control): C reads from cursor+1. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_oidrecv_core_cursor_skew() {
        const CAP: usize = 8;
        let data: [u8; CAP] = kani::any();
        let mut ccur: i32 = 1; // deliberate skew
        let mut cout: u32 = 0;
        let cst = unsafe { pg_getmsguint32(data.as_ptr(), CAP as i32, &mut ccur, &mut cout) };

        let ctx = mcx::MemoryContext::new_bump("kani-wave5-recv-ctl");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(&data) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        si.cursor = 0;
        match ::pqformat::pq_getmsgint(&mut si, 4) {
            Ok(v) => {
                assert!(cst == 0);
                assert!(v as u32 == cout); // expected failure: skewed read
            }
            Err(e) => {
                assert!(cst == 4);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    // ------------- xid_age (1181) / mxid_age (3939): state seam -----------

    static SEAM_NOW: AtomicU32 = AtomicU32::new(0);

    /// Seam stub for xact_seams::get_stable_latest_transaction_id::call —
    /// returns the harness-chosen symbolic `now` (universally quantified;
    /// seam internals out of proof, state-seam pattern).
    fn stub_stable_latest_xid() -> types_error::PgResult<u32> {
        Ok(SEAM_NOW.load(Relaxed))
    }

    /// Seam stub for multixact::ReadNextMultiXactId — same contract.
    fn stub_read_next_multixact() -> types_error::PgResult<u32> {
        Ok(SEAM_NOW.load(Relaxed))
    }

    #[kani::proof]
    #[kani::stub(xact_seams::get_stable_latest_transaction_id::call, stub_stable_latest_xid)]
    fn eq_xid_age() {
        let now: u32 = kani::any();
        let xid: u32 = kani::any();
        SEAM_NOW.store(now, Relaxed);
        let r = proof_support::call1_ok(adt_scalar::builtins::fc_xid_age, xid);
        let c = unsafe { pg_xid_age(now, xid) };
        assert!(r.as_i32() == c);
    }

    #[kani::proof]
    #[kani::stub(multixact::ReadNextMultiXactId, stub_read_next_multixact)]
    fn eq_mxid_age() {
        let now: u32 = kani::any();
        let xid: u32 = kani::any();
        SEAM_NOW.store(now, Relaxed);
        let r = proof_support::call1_ok(adt_scalar::builtins::fc_mxid_age, xid);
        let c = unsafe { pg_mxid_age(now, xid) };
        assert!(r.as_i32() == c);
    }

    /// MUST FAIL (seam-model-is-load-bearing witness): C is fed now^1.
    /// DEFAULT solver.
    #[kani::proof]
    #[kani::stub(xact_seams::get_stable_latest_transaction_id::call, stub_stable_latest_xid)]
    fn control_xid_age_seam_skew() {
        let now: u32 = kani::any();
        let xid: u32 = kani::any();
        SEAM_NOW.store(now, Relaxed);
        let r = proof_support::call1_ok(adt_scalar::builtins::fc_xid_age, xid);
        let c = unsafe { pg_xid_age(now ^ 1, xid) };
        assert!(r.as_i32() == c); // expected failure on any normal xid
    }
}

// ===========================================================================
// WAVE 5 — xid8funcs pg_snapshot family.
//
// C side: c/pg_xid8funcs.c (verbatim REL_18_STABLE xid8funcs.c + transam.h
// cores; provenance + shims X1-X7 documented there) — snapshot_out
// additionally needs ../intout/c/pg_intout.c (pg_ulltoa_n decimal
// reference) and snapshot_recv needs c/pg_scalar_misc.c (the shared
// pg_getmsguint32/64 wire shims).  Pass every needed file via --c-lib; see
// runqueue.txt per harness.
//
// Rows and theorem shapes:
//   - full_xid_from_allowable_at (epoch arithmetic core used by every
//     current-state row): full symbolic (next u64, xid u32) FENCED to the
//     C-documented precondition Assert(TransactionIdPrecedesOrEquals(xid,
//     next32)) — modular-compare fence; outside it C has Assert-compiled-out
//     epoch underflow (debug-assert-masking law: the fence IS the contract).
//   - pg_snapshot_xmin/xmax (2945/5062, 2946/5063): WRAPPER-level over a
//     real varlena image (4B header via datum::varlena::set_varsize_4b),
//     symbolic nxip 0..=4 + fully symbolic xmin/xmax/xips.
//   - pg_visible_in_snapshot (2948/5065): WRAPPER-level; the linear arm
//     (nxip <= 4, fully symbolic UNSORTED xips — neither side requires
//     order on this arm) and the bsearch arm (nxip == 32 > 30, xips fenced
//     to the type invariant sorted-strict + in-range, which parse/recv
//     enforce; C bsearch = documented libc-model shim X3).
//   - pg_snapshot_send (2942/5058): wrapper-level result-mcx frame (send
//     precedent), per-nxip 0/1/2, full image byte-compare.
//   - pg_snapshot_recv (2941/5057): CORE-level (crate::snapshot_recv on a
//     directly-held StringInfo — int-arith recv-wall lesson); verdict +
//     sqlstate class (08P01 short-data vs 22P03 invalid) + all decoded
//     fields.  EXPECTED LADDER: PgVec push + image build inside (STD-VEC
//     trap class) — budget release-gate, fallback = per-shape split.
//   - pg_snapshot_out (2940/5056): CORE-level (crate::snapshot_out_bytes),
//     nxip 0..=2, values fenced < 1e4 (u64 digit-emission sloped wall,
//     intout law) + concrete wide-digit spot.
//   - pg_snapshot_in (2939/5055): CORE-level (crate::parse_snapshot),
//     symbolic ASCII bytes len <= 6 (covers accept AND reject shapes at
//     that length) + strtou64 core harness len <= 8 vs the documented
//     glibc-model shim X4.  EXPECTED LADDER (sscanf-cascade cost class).
//     NOTE (shim X4): Rust strtou64 negates its ERANGE-saturated value
//     where glibc does not — unreachable below 20 digits, OUTSIDE every
//     cap here; flagged for the >=20-digit follow-up.
//   - SKIPPED rows (excluded, reasons in the report): pg_current_xact_id /
//     pg_current_snapshot / _if_assigned (live xact state), pg_snapshot_xip
//     (SRF), pg_export_snapshot (file I/O), pg_xact_status (clog+lwlock
//     state seams — state-seam-lane follow-up).
// ===========================================================================

#[cfg(kani)]
mod xid8snap {
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use std::os::raw::c_int;
    use types_error::{
        ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_TEXT_REPRESENTATION,
        ERRCODE_PROTOCOL_VIOLATION, ERROR,
    };
    use types_fmgr::LocalFcinfo;
    use xid8funcs::SnapView;

    extern "C" {
        fn pg_full_xid_from_allowable_at(next_full_xid: u64, xid: u32) -> u64;
        fn pg_visible_in_snapshot(
            value: u64,
            nxip: u32,
            xmin: u64,
            xmax: u64,
            xip: *const u64,
        ) -> c_int;
        fn pg_snapshot_xmin_c(nxip: u32, xmin: u64, xmax: u64, xip: *const u64) -> u64;
        fn pg_snapshot_xmax_c(nxip: u32, xmin: u64, xmax: u64, xip: *const u64) -> u64;
        fn pg_proof_strtou64(str_: *const u8, endptr: *mut *const u8) -> u64;
        fn pg_parse_snapshot(
            str_: *const u8,
            out_nxip: *mut u32,
            out_xmin: *mut u64,
            out_xmax: *mut u64,
            out_xip: *mut u64,
        ) -> c_int;
        fn pg_snapshot_out_c(
            nxip: u32,
            xmin: u64,
            xmax: u64,
            xip: *const u64,
            out: *mut u8,
        ) -> i32;
        fn pg_snapshot_recv_c(
            data: *const u8,
            len: i32,
            cursor: *mut i32,
            out_nxip: *mut u32,
            out_xmin: *mut u64,
            out_xmax: *mut u64,
            out_xip: *mut u64,
        ) -> c_int;
        fn pg_snapshot_send_c(
            nxip: u32,
            xmin: u64,
            xmax: u64,
            xip: *const u64,
            out: *mut u8,
        ) -> i32;
    }

    /// Payload layout the shipped SnapView decodes: nxip u32 @0, xmin u64
    /// @4, xmax u64 @12, xips from @20 (native-endian).  IMG = payload cap
    /// (20 + 8*max_nxip); the varlena header (4B, set_varsize_4b) prefixes
    /// it and reflects the ACTUAL nxip.
    fn snap_image<const IMG: usize>(nxip: u32, xmin: u64, xmax: u64, xips: &[u64]) -> [u8; IMG] {
        let mut img = [0u8; IMG];
        let total = 4 + 20 + 8 * nxip as usize;
        assert!(total <= IMG);
        img[0..4].copy_from_slice(&datum::varlena::set_varsize_4b(total));
        img[4..8].copy_from_slice(&nxip.to_ne_bytes());
        img[8..16].copy_from_slice(&xmin.to_ne_bytes());
        img[16..24].copy_from_slice(&xmax.to_ne_bytes());
        let mut i = 0;
        while i < nxip as usize {
            img[24 + 8 * i..32 + 8 * i].copy_from_slice(&xips[i].to_ne_bytes());
            i += 1;
        }
        img
    }

    // ---------- full_xid_from_allowable_at (epoch arithmetic core) --------

    #[kani::proof]
    fn eq_full_xid_from_allowable_at() {
        let next: u64 = kani::any();
        let xid: u32 = kani::any();
        // C contract fence (transam.h Assert, compiled out in production):
        // xid precedes-or-equals next's low word under MODULAR xid order,
        // and the epoch-0 underflow plane is excluded (second Assert).
        let next32 = next as u32;
        if types_core::TransactionIdIsNormal(xid) {
            kani::assume(types_core::xact::TransactionIdPrecedesOrEquals(xid, next32));
            kani::assume(!((next >> 32) == 0 && xid > next32));
        }
        // regime witnesses: both epoch arms + the special-xid arm
        kani::cover!(!types_core::TransactionIdIsNormal(xid));
        kani::cover!(types_core::TransactionIdIsNormal(xid) && xid > next32);
        kani::cover!(types_core::TransactionIdIsNormal(xid) && xid <= next32);
        let r = xid8funcs::full_xid_from_allowable_at(next, xid);
        let c = unsafe { pg_full_xid_from_allowable_at(next, xid) };
        assert!(r == c);
    }

    // ---------- xmin/xmax accessors: wrapper-level over the image ---------

    macro_rules! snap_accessor {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(6)]
            fn $h() {
                let nxip: u32 = kani::any();
                kani::assume(nxip <= 4);
                let xmin: u64 = kani::any();
                let xmax: u64 = kani::any();
                let xips: [u64; 4] = kani::any();
                let img = snap_image::<56>(nxip, xmin, xmax, &xips);
                let r = proof_support::call1_ok(
                    xid8funcs::builtins::$fc,
                    img.as_ptr(),
                );
                let c = unsafe { $pg(nxip, xmin, xmax, xips.as_ptr()) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    snap_accessor! {
        eq_pg_snapshot_xmin: fc_pg_snapshot_xmin / pg_snapshot_xmin_c;
        eq_pg_snapshot_xmax: fc_pg_snapshot_xmax / pg_snapshot_xmax_c;
    }

    // ---------- pg_visible_in_snapshot: linear + bsearch arms --------------

    /// Linear arm (nxip <= 4 <= 30): fully symbolic value/xmin/xmax/xips —
    /// no order fence needed, both sides scan linearly.
    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_pg_visible_in_snapshot_linear() {
        let value: u64 = kani::any();
        let nxip: u32 = kani::any();
        kani::assume(nxip <= 4);
        let xmin: u64 = kani::any();
        let xmax: u64 = kani::any();
        let xips: [u64; 4] = kani::any();
        let img = snap_image::<56>(nxip, xmin, xmax, &xips);
        let r = proof_support::call2_ok(
            xid8funcs::builtins::fc_pg_visible_in_snapshot,
            value,
            img.as_ptr(),
        );
        let c = unsafe { pg_visible_in_snapshot(value, nxip, xmin, xmax, xips.as_ptr()) };
        assert!(r.as_bool() as c_int == c);
    }

    /// bsearch arm (nxip == 32 > USE_BSEARCH_IF_NXIP_GREATER): xips fenced
    /// to the pg_snapshot type invariant (strictly ascending, xmin <=
    /// xip[i] < xmax) that parse_snapshot/recv enforce — C's bsearch (libc
    /// model, shim X3) requires sorted input; the Rust binary-search arm is
    /// in-theorem.
    #[kani::proof]
    #[kani::unwind(36)]
    fn eq_pg_visible_in_snapshot_bsearch() {
        const N: usize = 32;
        let value: u64 = kani::any();
        let xmin: u64 = kani::any();
        let xmax: u64 = kani::any();
        let xips: [u64; N] = kani::any();
        let mut i = 0;
        while i < N {
            kani::assume(xips[i] >= xmin && xips[i] < xmax);
            if i > 0 {
                kani::assume(xips[i - 1] < xips[i]);
            }
            i += 1;
        }
        let img = snap_image::<{ 4 + 20 + 8 * N }>(N as u32, xmin, xmax, &xips);
        let r = proof_support::call2_ok(
            xid8funcs::builtins::fc_pg_visible_in_snapshot,
            value,
            img.as_ptr(),
        );
        let c = unsafe { pg_visible_in_snapshot(value, N as u32, xmin, xmax, xips.as_ptr()) };
        assert!(r.as_bool() as c_int == c);
    }

    /// MUST FAIL (control for the new xid8funcs C section): C is fed
    /// xmax^1 — the boundary plane diverges. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(6)]
    fn control_visible_xmax_skew() {
        let value: u64 = kani::any();
        let xmin: u64 = kani::any();
        let xmax: u64 = kani::any();
        let xips: [u64; 4] = kani::any();
        let img = snap_image::<56>(2, xmin, xmax, &xips);
        let r = proof_support::call2_ok(
            xid8funcs::builtins::fc_pg_visible_in_snapshot,
            value,
            img.as_ptr(),
        );
        let c = unsafe { pg_visible_in_snapshot(value, 2, xmin, xmax ^ 1, xips.as_ptr()) };
        assert!(r.as_bool() as c_int == c); // expected failure at value == min(xmax, xmax^1)
    }

    // ---------- pg_snapshot_send: wrapper-level, per-nxip ------------------

    macro_rules! snap_send {
        ($($h:ident: n=$n:literal, total=$total:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let xmin: u64 = kani::any();
                let xmax: u64 = kani::any();
                let xips: [u64; 2] = kani::any();
                let img = snap_image::<56>($n, xmin, xmax, &xips);

                let mut cbuf = [0u8; $total];
                let clen = unsafe { pg_snapshot_send_c($n, xmin, xmax, xips.as_ptr(), cbuf.as_mut_ptr()) };

                let ctx = mcx::MemoryContext::new_bump("kani-snap-send");
                let mut f = LocalFcinfo::<1>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
                let d = match xid8funcs::builtins::fc_pg_snapshot_send(None, &mut f) {
                    Ok(d) => d,
                    Err(e) => { core::mem::forget(e); panic!("snapshot_send errored") }
                };
                let out = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, $total) };
                assert!(clen == $total as i32);
                let mut i = 0;
                while i < $total {
                    assert!(out[i] == cbuf[i]);
                    i += 1;
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    snap_send! {
        eq_pg_snapshot_send_n0: n=0, total=24, unwind=28;
        eq_pg_snapshot_send_n1: n=1, total=32, unwind=36;
        eq_pg_snapshot_send_n2: n=2, total=40, unwind=44;
    }

    // ---------- pg_snapshot_recv: core-level, direct StringInfo ------------

    /// Verdict + sqlstate class + all decoded fields, message cap = one
    /// xip (28 payload bytes).  EXPECTED LADDER (PgVec/image build in
    /// symex); see the module doc.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_pg_snapshot_recv_core() {
        const CAP: usize = 28;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);

        let mut ccur: i32 = 0;
        let (mut cnxip, mut cxmin, mut cxmax): (u32, u64, u64) = (0, 0, 0);
        let mut cxips = [0u64; 36];
        let cst = unsafe {
            pg_snapshot_recv_c(
                data.as_ptr(),
                dlen as i32,
                &mut ccur,
                &mut cnxip,
                &mut cxmin,
                &mut cxmax,
                cxips.as_mut_ptr(),
            )
        };

        let ctx = mcx::MemoryContext::new_bump("kani-snap-recv");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(&data[..dlen]) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        match xid8funcs::snapshot_recv(ctx.mcx(), &mut si) {
            Ok(v) => {
                assert!(cst == 0);
                let snap = SnapView::new(v.data());
                assert!(snap.nxip() == cnxip);
                assert!(snap.xmin() == cxmin);
                assert!(snap.xmax() == cxmax);
                let mut i = 0;
                while i < snap.nxip() as usize {
                    assert!(snap.xip(i) == cxips[i]);
                    i += 1;
                }
                core::mem::forget(v);
            }
            Err(e) => {
                // 4 = short data (08P01), 1 = failed validation (22P03)
                assert!(cst == 4 || cst == 1);
                if cst == 4 {
                    assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                } else {
                    assert!(e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION);
                }
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    // ---------- pg_snapshot_out: core-level, digit bands --------------------

    /// Values fenced < 1e4 (u64 digit-emission sloped wall — intout law);
    /// symbolic nxip 0..=2.  The trailing NUL the fc wrapper appends is
    /// wrapper plumbing (cstring_result), out of the core theorem.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_pg_snapshot_out_lt1e4() {
        let nxip: u32 = kani::any();
        kani::assume(nxip <= 2);
        let xmin: u64 = kani::any();
        let xmax: u64 = kani::any();
        let xips: [u64; 2] = kani::any();
        kani::assume(xmin < 10_000 && xmax < 10_000);
        kani::assume(xips[0] < 10_000 && xips[1] < 10_000);
        snapshot_out_case(nxip, xmin, xmax, &xips);
    }

    /// Concrete wide-digit spot (20-digit u64::MAX regime).
    #[kani::proof]
    #[kani::unwind(48)] // RUN-VERIFY fix: spot output is 42B; 24 truncated the byte-compare loop (unwinding assertion, not a divergence)
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_pg_snapshot_out_spots() {
        snapshot_out_case(1, 1, u64::MAX, &[9_999_999_999_999_999_999, 0]);
        snapshot_out_case(0, 1_000_000_000, u64::MAX, &[0, 0]);
    }

    fn snapshot_out_case(nxip: u32, xmin: u64, xmax: u64, xips: &[u64; 2]) {
        let mut cbuf = [0u8; 128];
        let clen =
            unsafe { pg_snapshot_out_c(nxip, xmin, xmax, xips.as_ptr(), cbuf.as_mut_ptr()) }
                as usize;

        let ctx = mcx::MemoryContext::new_bump("kani-snap-out");
        let img = snap_image::<56>(nxip, xmin, xmax, xips);
        let snap = SnapView::new(&img[4..4 + 20 + 8 * nxip as usize]);
        let out = match xid8funcs::snapshot_out_bytes(ctx.mcx(), &snap) {
            Ok(o) => o,
            Err(e) => {
                core::mem::forget(e);
                panic!("snapshot_out errored")
            }
        };
        assert!(out.len() == clen);
        let mut i = 0;
        while i < clen {
            assert!(out[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(out);
        core::mem::forget(ctx);
    }

    // ---------- strtou64 core + pg_snapshot_in ------------------------------

    /// strtou64 (the pg_snapshot_in number scanner) vs the documented
    /// glibc-model shim X4: value + consumed-length parity over all byte
    /// strings len <= 8 (no interior NUL; C sees the same bytes
    /// NUL-terminated).
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_strtou64_cap8() {
        const CAP: usize = 8;
        let bytes: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut i = 0;
        while i < CAP {
            kani::assume(i >= len || bytes[i] != 0);
            i += 1;
        }
        let mut cbuf = [0u8; CAP + 1];
        cbuf[..len].copy_from_slice(&bytes[..len]);
        cbuf[len] = 0;

        let mut endp: *const u8 = core::ptr::null();
        let cval = unsafe { pg_proof_strtou64(cbuf.as_ptr(), &mut endp) };
        let cconsumed = (endp as usize) - (cbuf.as_ptr() as usize);

        let (rval, rconsumed) = xid8funcs::strtou64(&bytes[..len]);
        // Rust's (0, 0) no-digits convention == C's endptr == str.
        assert!(rconsumed == cconsumed);
        if rconsumed > 0 {
            assert!(rval == cval);
        }
    }

    /// pg_snapshot_in / parse_snapshot: symbolic ASCII strings len <= 6 —
    /// covers accept ("1:2:", "1:1:1", ...) and every reject shape at that
    /// length.  Verdict + parsed fields + dedup behavior.  EXPECTED LADDER.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_pg_snapshot_in_cap6() {
        const CAP: usize = 6;
        let bytes: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut i = 0;
        while i < CAP {
            // ASCII, no interior NUL: the shared cstring/&str value domain
            kani::assume(i >= len || (bytes[i] != 0 && bytes[i] < 0x80));
            i += 1;
        }
        let mut cbuf = [0u8; CAP + 1];
        cbuf[..len].copy_from_slice(&bytes[..len]);
        cbuf[len] = 0;

        let (mut cnxip, mut cxmin, mut cxmax): (u32, u64, u64) = (0, 0, 0);
        let mut cxips = [0u64; 36];
        let cst = unsafe {
            pg_parse_snapshot(
                cbuf.as_ptr(),
                &mut cnxip,
                &mut cxmin,
                &mut cxmax,
                cxips.as_mut_ptr(),
            )
        };

        let ctx = mcx::MemoryContext::new_bump("kani-snap-in");
        let s = match core::str::from_utf8(&bytes[..len]) {
            Ok(s) => s,
            Err(_) => panic!("ASCII fence violated"),
        };
        match xid8funcs::parse_snapshot(ctx.mcx(), s, None) {
            Ok(Some(v)) => {
                assert!(cst == 0);
                let snap = SnapView::new(v.data());
                assert!(snap.nxip() == cnxip);
                assert!(snap.xmin() == cxmin);
                assert!(snap.xmax() == cxmax);
                let mut i = 0;
                while i < snap.nxip() as usize {
                    assert!(snap.xip(i) == cxips[i]);
                    i += 1;
                }
                core::mem::forget(v);
            }
            Ok(None) => {
                // soft-error path requires an escontext; None passed
                panic!("unreachable: hard-error mode returns Err")
            }
            Err(e) => {
                assert!(cst == 1);
                assert!(e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(ctx);
    }
}
