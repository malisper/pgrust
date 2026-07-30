//! Kani C≡Rust equivalence FEASIBILITY PROBE: the numeric comparator
//! family (numeric_eq/ne/lt/le/gt/ge, numeric_cmp) — first proofs over
//! the variable-length base-10000 numeric representation.
//!
//! Rust side (shipped code, path-dep — never copied):
//!   adt_numeric::{cmp_numerics, numeric_eq..numeric_le} (ops.rs:409..),
//!   called on adt_numeric::Num views built from REAL PACKED PAYLOAD
//!   IMAGES — so the shipped packed-header decode (Num::sign/weight/
//!   ndigits/digits, BOTH NumericShort and NumericLong forms, lib.rs:104-)
//!   is inside the theorem, plus cmp_var_common/cmp_abs_common
//!   (arith.rs:33/88).
//!
//! C side: c/pg_numeric_cmp.c — verbatim REL_18_STABLE numeric.c
//! cmp_numerics/cmp_var_common/cmp_abs_common (provenance + shims there).
//!
//! Fences and claims (mirror into the ledger):
//!  - C-DECODE FENCE: the C packed-header macros (NUMERIC_SIGN/WEIGHT/
//!    DIGITS/NDIGITS bit extraction) are shimmed to explicit params; the
//!    harness feeds C the spec-level (sign, weight, digits, ndigits) of
//!    the SAME value whose packed image Rust decodes.  The Rust decode is
//!    proven against the on-disk spec; the C decode is out of scope.
//!  - DETOASTING OUT OF SCOPE: images model the post-PG_GETARG_NUMERIC
//!    caller contract (bytea-cmp varlena precedent).  fmgr datum plumbing
//!    (num_arg/Datum::from_bool) stays in the tested tier — value-space
//!    proof at the cmp_numerics/numeric_xx layer.
//!  - DIGIT INVARIANT FENCE (contract domain): each base-NBASE digit in
//!    [0, 9999], and per REL_18 numeric.c ("NOTE: by convention, values in
//!    the packed form have been stripped of all leading and trailing zero
//!    digits ... if the value is zero, there will be no digits at all!
//!    The weight is arbitrary in that case") first/last digit nonzero,
//!    ndigits==0 allowed with arbitrary weight.  A separate out-of-contract
//!    harness drops the leading/trailing-zero fence (digit range kept —
//!    outside [0,9999] the value itself is meaningless and Rust's i16
//!    digit subtraction could overflow where C computes in int).
//!  - Each side of every harness is symbolic over the FULL value lattice:
//!    {NaN, +Inf, -Inf, finite-long-form, finite-short-form} selector ×
//!    symbolic sign/weight/dscale/digits within the fence.  Long form:
//!    full i16 weight, dscale 0..=0x3FFF.  Short form: weight -64..=63,
//!    dscale 0..=63 (field widths per the on-disk spec).
//!  - cmp asserts exact SQL-visible int32 VALUE equality; boolean rows
//!    assert verdict equality.
//!  - kani::cover! witnesses: special×finite, finite×finite, both header
//!    forms, equal-value, unequal-weight, and sign-mix regimes reachable.
//!
//! Negative control (DEFAULT solver, must FAIL): C sees ndigits-1 on the
//! left side (last digit nonzero under the fence ⇒ values differ).
//!
//! Bounds ladder (feasibility probe): ndigits<=1, <=2, <=3, <=4 per side —
//! wall curve recorded in the probe report / ledger notes.

#[cfg(kani)]
mod proofs {
    use adt_numeric::{
        cmp_numerics, numeric_eq, numeric_ge, numeric_gt, numeric_le, numeric_lt, numeric_ne, Num,
        NUMERIC_NAN, NUMERIC_NEG, NUMERIC_NINF, NUMERIC_PINF, NUMERIC_POS,
    };
    use std::os::raw::c_int;

    extern "C" {
        fn pg_cmp_numerics(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_numeric_eq(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_numeric_ne(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_numeric_lt(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_numeric_le(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_numeric_gt(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_numeric_ge(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
    }

    /// Max digits per side across all harnesses (per-harness cap is an
    /// assume; the buffer is sized for the max).
    const ND: usize = 8;

    /// Packed numeric payload buffer.  2-byte aligned so Num::digits()'s
    /// alignment guard holds (NumericImage is u16-backed for the same
    /// reason).
    #[repr(align(2))]
    struct Buf([u8; 4 + 2 * ND]);

    /// One symbolic numeric: the packed payload image (fed to Rust) plus
    /// the spec-level fields the C packed-header macros would produce
    /// (fed to the shimmed C).
    struct Side {
        buf: Buf,
        len: usize,      // payload length
        sign: c_int,     // NUMERIC_POS/NEG or NAN/PINF/NINF code
        weight: c_int,
        dscale: c_int,
        digits: [i16; ND],
        nd: c_int,
        sel: u8, // 0 NaN, 1 +Inf, 2 -Inf, 3 finite long, 4 finite short
    }

    fn put_u16(buf: &mut Buf, off: usize, v: u16) {
        let b = v.to_ne_bytes();
        buf.0[off] = b[0];
        buf.0[off + 1] = b[1];
    }

    /// Build one fully symbolic numeric within the packed-form invariant
    /// (see module docs).  `nd_cap` bounds ndigits; `strip_fence` applies
    /// the no-leading/trailing-zero convention.
    fn sym_num(nd_cap: usize, strip_fence: bool) -> Side {
        let neg: bool = kani::any();
        let sel: u8 = kani::any();
        kani::assume(sel <= 4);
        let mut buf = Buf([0; 4 + 2 * ND]);
        let digits: [i16; ND] = kani::any();

        if sel <= 2 {
            // Specials: two-byte payload, header word only (REL_18 stores
            // specials as bare n_header; low bits zero).
            let code: u16 = match sel {
                0 => NUMERIC_NAN,
                1 => NUMERIC_PINF,
                _ => NUMERIC_NINF,
            };
            put_u16(&mut buf, 0, code);
            return Side {
                buf,
                len: 2,
                sign: code as c_int,
                weight: 0,
                dscale: 0,
                digits,
                nd: 0,
                sel,
            };
        }

        // Finite value: symbolic ndigits/sign/weight/dscale/digits.
        let nd: usize = kani::any();
        kani::assume(nd <= nd_cap);
        for (i, d) in digits.iter().enumerate() {
            if i < nd {
                // base-NBASE digit: 0..=9999
                kani::assume(*d >= 0 && *d < 10000);
            }
        }
        if strip_fence && nd > 0 {
            // packed-form convention: no leading or trailing zero digits
            kani::assume(digits[0] != 0 && digits[nd - 1] != 0);
        }
        let (weight, dscale, digits_off): (i32, u16, usize) = if sel == 3 {
            // NumericLong: header = sign|dscale, then i16 weight, digits at 4.
            let weight: i16 = kani::any();
            let dscale: u16 = kani::any();
            kani::assume(dscale <= 0x3FFF);
            let header: u16 = (if neg { NUMERIC_NEG } else { NUMERIC_POS }) | dscale;
            put_u16(&mut buf, 0, header);
            put_u16(&mut buf, 2, weight as u16);
            (weight as i32, dscale, 4)
        } else {
            // NumericShort: 0x8000 | sign(0x2000) | dscale<<7 (6 bits) |
            // weight (7-bit two's complement), digits at 2.
            let weight: i16 = kani::any();
            kani::assume((-64..=63).contains(&weight));
            let dscale: u16 = kani::any();
            kani::assume(dscale <= 63);
            let header: u16 = 0x8000
                | if neg { 0x2000 } else { 0 }
                | (dscale << 7)
                | ((weight as u16) & 0x7F);
            put_u16(&mut buf, 0, header);
            (weight as i32, dscale, 2)
        };
        for i in 0..ND {
            if i < nd {
                put_u16(&mut buf, digits_off + 2 * i, digits[i] as u16);
            }
        }
        Side {
            buf,
            len: digits_off + 2 * nd,
            sign: (if neg { NUMERIC_NEG } else { NUMERIC_POS }) as c_int,
            weight,
            dscale: dscale as c_int,
            digits,
            nd: nd as c_int,
            sel,
        }
    }

    fn c_args(s: &Side) -> (c_int, c_int, *const i16, c_int) {
        (s.sign, s.weight, s.digits.as_ptr(), s.nd)
    }

    // ---- wall-curve probes: cmp exact value, ndigits cap 1..=4 ----

    macro_rules! cmp_probe {
        ($name:ident, $cap:expr) => {
            #[kani::proof]
            #[kani::unwind(12)]
            fn $name() {
                let a = sym_num($cap, true);
                let b = sym_num($cap, true);
                let (s1, w1, d1, n1) = c_args(&a);
                let (s2, w2, d2, n2) = c_args(&b);
                let c = unsafe { pg_cmp_numerics(s1, w1, d1, n1, s2, w2, d2, n2) };
                let r = cmp_numerics(
                    Num::from_payload(&a.buf.0[..a.len]),
                    Num::from_payload(&b.buf.0[..b.len]),
                );
                assert!(c == r);
                // regime reachability witnesses
                kani::cover!(a.sel <= 2 && b.sel >= 3); // special vs finite
                kani::cover!(a.sel >= 3 && b.sel >= 3); // finite vs finite
                kani::cover!(a.sel == 3 && b.sel == 4); // long vs short form
                kani::cover!(a.sel >= 3 && b.sel >= 3 && r == 0); // equal values
                kani::cover!(a.sel >= 3 && b.sel >= 3 && a.weight != b.weight); // weight skip
                kani::cover!(a.sign != b.sign && a.sel >= 3 && b.sel >= 3); // sign mix
            }
        };
    }

    cmp_probe!(eq_numeric_cmp_nd1, 1);
    cmp_probe!(eq_numeric_cmp_nd2, 2);
    cmp_probe!(eq_numeric_cmp_nd3, 3);
    cmp_probe!(eq_numeric_cmp_nd4, 4);
    cmp_probe!(eq_numeric_cmp_nd6, 6);
    cmp_probe!(eq_numeric_cmp_nd8, 8);

    // ---- out-of-contract: leading/trailing zero digits allowed ----
    // (digit range fence kept; see module docs)

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_numeric_cmp_nd4_unstripped() {
        let a = sym_num(4, false);
        let b = sym_num(4, false);
        let (s1, w1, d1, n1) = c_args(&a);
        let (s2, w2, d2, n2) = c_args(&b);
        let c = unsafe { pg_cmp_numerics(s1, w1, d1, n1, s2, w2, d2, n2) };
        let r = cmp_numerics(
            Num::from_payload(&a.buf.0[..a.len]),
            Num::from_payload(&b.buf.0[..b.len]),
        );
        assert!(c == r);
        // zero-skip tails are only reachable out-of-fence
        kani::cover!(a.nd > 0 && a.digits[0] == 0);
        kani::cover!(a.nd > 0 && a.digits[(a.nd - 1) as usize] == 0);
    }

    // ---- per-row boolean harnesses (full lattice, ndigits<=cap) ----

    macro_rules! bool_harness {
        ($name:ident, $cfn:ident, $rfn:ident, $cap:expr) => {
            #[kani::proof]
            #[kani::unwind(12)]
            fn $name() {
                let a = sym_num($cap, true);
                let b = sym_num($cap, true);
                let (s1, w1, d1, n1) = c_args(&a);
                let (s2, w2, d2, n2) = c_args(&b);
                let c = unsafe { $cfn(s1, w1, d1, n1, s2, w2, d2, n2) };
                let r = $rfn(
                    Num::from_payload(&a.buf.0[..a.len]),
                    Num::from_payload(&b.buf.0[..b.len]),
                );
                assert!((c != 0) == r);
            }
        };
    }

    bool_harness!(eq_numeric_eq_nd4, pg_numeric_eq, numeric_eq, 4);
    bool_harness!(eq_numeric_ne_nd4, pg_numeric_ne, numeric_ne, 4);
    bool_harness!(eq_numeric_lt_nd4, pg_numeric_lt, numeric_lt, 4);
    bool_harness!(eq_numeric_le_nd4, pg_numeric_le, numeric_le, 4);
    bool_harness!(eq_numeric_gt_nd4, pg_numeric_gt, numeric_gt, 4);
    bool_harness!(eq_numeric_ge_nd4, pg_numeric_ge, numeric_ge, 4);

    // ---- negative control: rig is non-vacuous ----
    // C sees one digit fewer on the left; under the strip fence the dropped
    // trailing digit is nonzero, so the values differ.  MUST FAIL (run with
    // the DEFAULT solver, not kissat).
    #[kani::proof]
    #[kani::unwind(12)]
    fn control_numeric_cmp_short_c_ndigits() {
        let a = sym_num(2, true);
        kani::assume(a.sel == 3 && a.nd >= 1);
        let b = sym_num(2, true);
        let (s1, w1, d1, n1) = c_args(&a);
        let (s2, w2, d2, n2) = c_args(&b);
        let c = unsafe { pg_cmp_numerics(s1, w1, d1, n1 - 1, s2, w2, d2, n2) };
        let r = cmp_numerics(
            Num::from_payload(&a.buf.0[..a.len]),
            Num::from_payload(&b.buf.0[..b.len]),
        );
        assert!(c == r);
    }

    // ======================================================================
    // SIBLING-ROW EXTENSION (2026-07-28): numeric_smaller/larger,
    // numeric_abs/uminus/uplus, hash_numeric[_extended],
    // in_range_numeric_numeric.  C side: c/pg_numeric_rows.c (provenance +
    // shims there).  Same packed-image builder and fences as above; the
    // smaller/larger/hash/in_range rows are WRAPPER-LEVEL (real LocalFcinfo
    // frame, 4B-header inline varlena args, shipped num_arg decode inside
    // the theorem — datetime-cmp scope-expansion + bytea-cmp varlena
    // precedents); abs/uminus/uplus are value-level at the ops layer (their
    // fc_* wrappers return through the mcx result arena, which stays in the
    // tested tier).
    // ======================================================================

    use adt_numeric::builtins::{
        fc_hash_numeric, fc_hash_numeric_extended, fc_numeric_larger, fc_numeric_smaller,
    };
    use adt_numeric::{in_range_numeric_numeric, numeric_abs, numeric_uminus, numeric_uplus};
    use datum::Datum;
    use proof_support::fcinfo::{call1_ok, call2_ok};
    use proof_support::stubs;

    extern "C" {
        fn pg_numeric_smaller(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_numeric_larger(
            sign1: c_int, weight1: c_int, digits1: *const i16, ndigits1: c_int,
            sign2: c_int, weight2: c_int, digits2: *const i16, ndigits2: c_int,
        ) -> c_int;
        fn pg_hash_numeric(
            sign: c_int, weight: c_int, digits: *const i16, ndigits: c_int,
        ) -> u32;
        fn pg_hash_numeric_extended(
            sign: c_int, weight: c_int, digits: *const i16, ndigits: c_int, seed: u64,
        ) -> u64;
        fn pg_numeric_abs_hdr(header: u16) -> u16;
        fn pg_numeric_uminus_hdr(header: u16, ndigits: c_int) -> u16;
        #[allow(clippy::too_many_arguments)]
        fn pg_in_range_numeric_numeric(
            val_sign: c_int, val_weight: c_int, val_dscale: c_int,
            val_digits: *const i16, val_ndigits: c_int,
            base_sign: c_int, base_weight: c_int, base_dscale: c_int,
            base_digits: *const i16, base_ndigits: c_int,
            offset_sign: c_int, offset_weight: c_int, offset_dscale: c_int,
            offset_digits: *const i16, offset_ndigits: c_int,
            sub: c_int, less: c_int, err: *mut c_int,
        ) -> c_int;
    }

    /// Full inline varlena image: 4-byte varlena header + numeric payload.
    /// 4-aligned so the payload u16s stay 2-aligned and the low header bits
    /// read as 4B-uncompressed, keeping the shipped arg_varlena_packed on
    /// its inline path (detoast/mcx provably not taken).
    #[repr(align(4))]
    struct VBuf([u8; 8 + 2 * ND]);

    fn varlena_of(s: &Side) -> VBuf {
        let mut v = VBuf([0; 8 + 2 * ND]);
        v.0[..4].copy_from_slice(&::datum::varlena::set_varsize_4b(4 + s.len));
        v.0[4..4 + s.len].copy_from_slice(&s.buf.0[..s.len]);
        v
    }

    fn spec5(s: &Side) -> (c_int, c_int, c_int, *const i16, c_int) {
        (s.sign, s.weight, s.dscale, s.digits.as_ptr(), s.nd)
    }

    /// C's offset-reject condition (NaN, -Inf, or negative), on the same
    /// spec-level sign carrier the C shim reads.
    fn offset_valid(o: &Side) -> bool {
        !(o.sel == 0 || o.sel == 2 || o.sign == NUMERIC_NEG as c_int)
    }

    // ---- numeric_smaller / numeric_larger: wrapper-level winning-input-
    // datum identity (datetime-cmp timetz precedent), FULL plane including
    // ties.  The tie plane (cmp==0) carried a REAL identity divergence —
    // C keeps num2, the wrapper used to keep arg 0 — value-visible when
    // the two images differ (e.g. 1.0 vs 1.00 dscale).  Adjudicated
    // FIX-IN-PGRUST (divergence #9); the wrapper now uses C's strict
    // comparison, so the identity theorem is unfenced and the former
    // confinement harness is a tie-plane regression gate.

    macro_rules! minmax_harness {
        ($name:ident, $tie:ident, $fc:path, $cfn:ident) => {
            #[kani::proof]
            #[kani::unwind(12)]
            fn $name() {
                let a = sym_num(4, true);
                let b = sym_num(4, true);
                let va = varlena_of(&a);
                let vb = varlena_of(&b);
                let (s1, w1, d1, n1) = c_args(&a);
                let (s2, w2, d2, n2) = c_args(&b);
                let c = unsafe { $cfn(s1, w1, d1, n1, s2, w2, d2, n2) };
                let r = call2_ok($fc, va.0.as_ptr(), vb.0.as_ptr());
                let want = if c == 0 { va.0.as_ptr() } else { vb.0.as_ptr() } as usize;
                assert!(r.as_usize() == want);
            }

            // Tie-plane regression gate for divergence #9: for every
            // cmp==0 input BOTH sides must keep num2 (C's deliberate
            // choice; value-visible through dscale, native replay below).
            #[kani::proof]
            #[kani::unwind(12)]
            fn $tie() {
                let a = sym_num(4, true);
                let b = sym_num(4, true);
                let va = varlena_of(&a);
                let vb = varlena_of(&b);
                let (s1, w1, d1, n1) = c_args(&a);
                let (s2, w2, d2, n2) = c_args(&b);
                let cmp = cmp_numerics(
                    Num::from_payload(&a.buf.0[..a.len]),
                    Num::from_payload(&b.buf.0[..b.len]),
                );
                kani::assume(cmp == 0);
                let c = unsafe { $cfn(s1, w1, d1, n1, s2, w2, d2, n2) };
                assert!(c == 1); // C keeps num2 on every tie
                let r = call2_ok($fc, va.0.as_ptr(), vb.0.as_ptr());
                assert!(r.as_usize() == vb.0.as_ptr() as usize); // so does Rust now
                // ties between DIFFERENT images exist (identity is
                // value-visible): cover_minmax_tie_images + native replay
            }
        };
    }

    minmax_harness!(
        eq_numeric_smaller_nd4,
        tie_numeric_smaller_nd4,
        fc_numeric_smaller,
        pg_numeric_smaller
    );
    // Witness that the tie plane contains DIFFERENT images (e.g. dscale
    // differs) — the identity flip is value-visible through numeric_out.
    // Hoisted out of the tie harnesses (cover = extra SAT call; byte-compare
    // loop needs unwind 24).
    #[kani::proof]
    #[kani::unwind(24)]
    fn cover_minmax_tie_images() {
        let a = sym_num(4, true);
        let b = sym_num(4, true);
        let cmp = cmp_numerics(
            Num::from_payload(&a.buf.0[..a.len]),
            Num::from_payload(&b.buf.0[..b.len]),
        );
        kani::cover!(cmp == 0 && a.buf.0[..a.len] != b.buf.0[..b.len]);
    }

    minmax_harness!(
        eq_numeric_larger_nd4,
        tie_numeric_larger_nd4,
        fc_numeric_larger,
        pg_numeric_larger
    );

    // ---- numeric_abs / numeric_uminus / numeric_uplus: header-flag ops
    // on the packed image (no digit arithmetic).  C side transcribes the
    // REL_18 header macros onto the explicit header word (its bit decode
    // IS in the theorem — SHIM 2); duplicate_numeric's only other effect
    // is byte-copying the rest of the image, asserted on the Rust output.
    // Domain: full value lattice, digit-range fence only (the header ops
    // are digit-blind; uminus' zero test is ndigits==0, checked as C does).

    /// Kani stub for `adt_numeric::var::word_buf_take`: the shipped fn pops
    /// a recycled buffer from a TLS pool — a `thread_local!` holding a
    /// Drop-carrying value, whose first-use destructor registration reaches
    /// `_tlv_atexit` (Kani-unsupported).  This stub IS the shipped pool-miss
    /// arm (`pop()` on an empty pool -> `unwrap_or_default()` ->
    /// `Vec::new()`); buffer recycling leaves the proof — ledger wording
    /// "modulo TLS word-pool model (pool-miss arm)".
    fn stub_word_buf_take() -> Vec<u16> {
        Vec::new()
    }

    /// Kani stub for `adt_numeric::var::word_buf_put` — the shipped
    /// `capacity() == 0` early-return arm (recycling out of proof).
    fn stub_word_buf_put(_v: Vec<u16>) {}

    macro_rules! hdr_op_harness {
        ($name:ident, $rfn:ident, $ccall:expr) => {
            #[kani::proof]
            #[kani::unwind(24)] // payload byte-compare loop: up to 4+2*ND=20 iters
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            fn $name() {
                let a = sym_num(4, false);
                let n = Num::from_payload(&a.buf.0[..a.len]);
                let r = $rfn(n);
                let h = u16::from_ne_bytes([a.buf.0[0], a.buf.0[1]]);
                #[allow(clippy::redundant_closure_call)]
                let c: u16 = unsafe { ($ccall)(h, a.nd) };
                let p = r.payload();
                assert!(p.len() == a.len);
                assert!(u16::from_ne_bytes([p[0], p[1]]) == c);
                assert!(p[2..] == a.buf.0[2..a.len]); // duplicate_numeric parity
                kani::cover!(a.sel == 2); // -Inf header flip reachable
                kani::cover!(a.sel == 3 && a.sign == NUMERIC_NEG as c_int);
                kani::cover!(a.sel == 4 && a.nd == 0); // short zero
                core::mem::forget(r);
            }
        };
    }

    hdr_op_harness!(eq_numeric_abs_nd4, numeric_abs, |h: u16, _nd: c_int| {
        pg_numeric_abs_hdr(h)
    });
    hdr_op_harness!(eq_numeric_uminus_nd4, numeric_uminus, |h: u16, nd: c_int| {
        pg_numeric_uminus_hdr(h, nd)
    });

    // numeric_uplus: the C body is PG_RETURN_NUMERIC(duplicate_numeric(num))
    // — identity on the packed image — so byte-identity of the Rust output
    // to the input IS the C spec; no C call needed (would be vacuous).
    #[kani::proof]
    #[kani::unwind(24)] // payload byte-compare loop: up to 4+2*ND=20 iters
    #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
    #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
    fn eq_numeric_uplus_nd4() {
        let a = sym_num(4, false);
        let r = numeric_uplus(Num::from_payload(&a.buf.0[..a.len]));
        assert!(r.payload() == &a.buf.0[..a.len]);
        core::mem::forget(r);
    }

    // ---- hash_numeric / hash_numeric_extended: the zero-strip loop
    // composed with the vendored hash_bytes kernel (already PROVED against
    // the shipped hashfn crate in proofs/hash-rows).  Wrapper-level.
    // Domain: full lattice, digit-range fence only (strip_fence OFF so the
    // paranoia strip loops are live — leading/trailing zeros reachable).

    // Case-split (escalation ladder step 4): with the packed-form strip
    // fence the C/Rust strip loops exit immediately and hash_len is exactly
    // 2*nd bytes, so per-nd harnesses are fixed-length hash circuits (the
    // cheap regime measured in proofs/hashfn).  The paranoia strip loops
    // themselves are proven in the out-of-contract unstripped nd<=2 pair.
    // Union coverage: cover_hash_numeric_split.
    macro_rules! hash_harness_nd {
        ($name:ident, $ext:ident, $nd:expr) => {
            #[kani::proof]
            #[kani::unwind(12)]
            fn $name() {
                let a = sym_num(4, true);
                kani::assume(a.nd == $nd);
                let va = varlena_of(&a);
                let (s, w, d, n) = c_args(&a);
                let c = unsafe { pg_hash_numeric(s, w, d, n) };
                let r = call1_ok(fc_hash_numeric, va.0.as_ptr());
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind(12)]
            fn $ext() {
                let a = sym_num(4, true);
                kani::assume(a.nd == $nd);
                let va = varlena_of(&a);
                let seed: i64 = kani::any();
                let (s, w, d, n) = c_args(&a);
                let c = unsafe { pg_hash_numeric_extended(s, w, d, n, seed as u64) };
                let r = call2_ok(fc_hash_numeric_extended, va.0.as_ptr(), seed);
                assert!(r.as_u64() == c);
            }
        };
    }

    hash_harness_nd!(eq_hash_numeric_snd0, eq_hash_numeric_ext_snd0, 0);
    hash_harness_nd!(eq_hash_numeric_snd1, eq_hash_numeric_ext_snd1, 1);
    hash_harness_nd!(eq_hash_numeric_snd2, eq_hash_numeric_ext_snd2, 2);
    hash_harness_nd!(eq_hash_numeric_snd3, eq_hash_numeric_ext_snd3, 3);
    hash_harness_nd!(eq_hash_numeric_snd4, eq_hash_numeric_ext_snd4, 4);

    // Union coverage for the per-nd split: within the stripped contract
    // domain (specials have nd == 0) every input hits one of the five
    // harnesses above.
    #[kani::proof]
    #[kani::unwind(12)]
    fn cover_hash_numeric_split() {
        let a = sym_num(4, true);
        assert!(a.nd == 0 || a.nd == 1 || a.nd == 2 || a.nd == 3 || a.nd == 4);
    }

    // Out-of-contract: leading/trailing zero digits allowed (nd<=2) — the
    // C paranoia strip loops and the Rust live_digit_span walk are live and
    // symbolic here (see cover_hash_numeric_regimes for reachability).
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_hash_numeric_unstripped_nd2() {
        let a = sym_num(2, false);
        let va = varlena_of(&a);
        let (s, w, d, n) = c_args(&a);
        let c = unsafe { pg_hash_numeric(s, w, d, n) };
        let r = call1_ok(fc_hash_numeric, va.0.as_ptr());
        assert!(r.as_u32() == c);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_hash_numeric_ext_unstripped_nd2() {
        let a = sym_num(2, false);
        let va = varlena_of(&a);
        let seed: i64 = kani::any();
        let (s, w, d, n) = c_args(&a);
        let c = unsafe { pg_hash_numeric_extended(s, w, d, n, seed as u64) };
        let r = call2_ok(fc_hash_numeric_extended, va.0.as_ptr(), seed);
        assert!(r.as_u64() == c);
    }

    // Regime-reachability witnesses for the two hash harnesses, HOISTED
    // into one harness (each inline cover = one extra SAT call under
    // kissat — varbit lesson).  Same builder, same fences (nd<=4,
    // strip_fence OFF), no extra assumes in the eq harnesses, so
    // reachability here transfers.
    #[kani::proof]
    #[kani::unwind(12)]
    fn cover_hash_numeric_regimes() {
        let a = sym_num(4, false);
        kani::cover!(a.sel <= 2); // specials arm (returns 0 / seed)
        kani::cover!(a.sel >= 3 && a.nd > 0 && a.digits[0] == 0); // leading strip
        kani::cover!(a.sel >= 3 && a.nd > 1 && a.digits[(a.nd - 1) as usize] == 0); // trailing strip
        kani::cover!(a.sel >= 3 && a.nd == 1 && a.digits[0] == 0); // zero value (-1 / seed-1 arm)
    }


    // ---- in_range_numeric_numeric ----------------------------------------
    // Case-split (union coverage harness below):
    //   ERR:     invalid offset (NaN / -Inf / negative) — error-arm harness,
    //            verdict + sqlstate + level parity, message out of proof.
    //   SPECIAL: valid offset, NOT all three finite — pure comparison arms,
    //            full lattice / full weights (no arithmetic reachable).
    //   FINITE:  all three finite, base/offset weight in [-4, 4] — the
    //            add_var/sub_var + cmp_var arm, digit loops bounded by the
    //            band (res_ndigits <= 13).
    // These three harnesses are VALUE-LEVEL at the ops layer (fmgr datum
    // plumbing in the tested tier): the 5-arg wrapper frame + three varlena
    //  decodes pushed CBMC past the 6GB memory cap (wall(memory) x2).
    //   GAP:     all three finite, weight outside the band — recorded
    //            remainder (the C add/sub result length grows with weight
    //            range; unbounded weights are the digit-arithmetic wall).
    // PgError::error is #[track_caller] (Location::caller is
    // Kani-unsupported) — stubbed with the field-identical proof_support
    // constructor in every harness where the error arm is reachable;
    // sqlstate stays set by shipped .with_sqlstate.

    const IN_RANGE_ND: usize = 2; // error/partition-arm ndigits cap

    /// Literal special Side — concrete header bytes so the C and Rust
    /// special tests CONSTANT-FOLD and the dead arithmetic arm leaves the
    /// formula (i128 lesson: an ASSUMED selector does not fold; the whole
    /// in_range body at symbolic selectors overflowed the 6GB memory cap
    /// even value-level — wall(memory) x3 before this split).
    fn lit_special(code: u16) -> Side {
        let mut buf = Buf([0; 4 + 2 * ND]);
        put_u16(&mut buf, 0, code);
        Side {
            buf,
            len: 2,
            sign: code as c_int,
            weight: 0,
            dscale: 0,
            digits: [0; ND],
            nd: 0,
            sel: match code {
                NUMERIC_NAN => 0,
                NUMERIC_PINF => 1,
                _ => 2,
            },
        }
    }

    /// Finite NEGATIVE side with a fully LITERAL header word (weight -3,
    /// dscale 7 spots in both header forms), symbolic digits.  A masked-
    /// symbolic header (NEG | sym dscale) was tried first and WALLED
    /// >9.6GB: the constant flag bits don't constant-fold through the
    /// byte store/reload, so the reject stays symbolic and the dead
    /// arithmetic arm re-enters the formula.  The digits stay symbolic
    /// (they don't feed the reject); used by the error-arm harnesses.
    fn lit_neg_finite(short_form: bool) -> Side {
        let mut buf = Buf([0; 4 + 2 * ND]);
        let digits: [i16; ND] = kani::any();
        let nd: usize = kani::any();
        kani::assume(nd <= IN_RANGE_ND);
        for (i, d) in digits.iter().enumerate() {
            if i < nd {
                kani::assume(*d >= 0 && *d < 10000);
            }
        }
        let (weight, dscale, digits_off): (i32, u16, usize) = if short_form {
            // weight -3 -> 7-bit two's complement 0x7D; dscale 7
            let header: u16 = 0x8000 | 0x2000 | (7 << 7) | 0x7D;
            put_u16(&mut buf, 0, header);
            (-3, 7, 2)
        } else {
            let header: u16 = NUMERIC_NEG | 7; // dscale 7
            put_u16(&mut buf, 0, header);
            put_u16(&mut buf, 2, (-3i16) as u16);
            (-3, 7, 4)
        };
        for i in 0..ND {
            if i < nd {
                put_u16(&mut buf, digits_off + 2 * i, digits[i] as u16);
            }
        }
        Side {
            buf,
            len: digits_off + 2 * nd,
            sign: NUMERIC_NEG as c_int,
            weight,
            dscale: dscale as c_int,
            digits,
            nd: nd as c_int,
            sel: if short_form { 4 } else { 3 },
        }
    }

    /// Shared in_range comparison body: run C (shimmed) and shipped Rust on
    /// the same triple, assert verdict parity and no error.
    fn in_range_check(val: &Side, base: &Side, off: &Side, sub: bool, less: bool) {
        let (vs, vw, vd, vp, vn) = spec5(val);
        let (bs, bw, bd, bp, bn) = spec5(base);
        let (os, ow, od, op, on) = spec5(off);
        let mut err: c_int = 0;
        let c = unsafe {
            pg_in_range_numeric_numeric(
                vs, vw, vd, vp, vn, bs, bw, bd, bp, bn, os, ow, od, op, on,
                sub as c_int, less as c_int, &mut err,
            )
        };
        assert!(err == 0);
        let r = in_range_numeric_numeric(
            Num::from_payload(&val.buf.0[..val.len]),
            Num::from_payload(&base.buf.0[..base.len]),
            Num::from_payload(&off.buf.0[..off.len]),
            sub,
            less,
        );
        let d = match r {
            Ok(d) => d,
            Err(_) => panic!("in_range errored inside the valid-offset fence"),
        };
        assert!(d == (c != 0));
    }

    // SPECIAL comparison arms, tiled by "one operand is a LITERAL special"
    // (the other two fully symbolic; offset fenced valid).  Union of the 7
    // harnesses = the whole valid-offset not-all-finite region (see
    // cover_in_range_partition): any such input has a special val, or a
    // special base, or offset == +Inf (the only valid special offset).
    // ($slot: 0 = val, 1 = base, 2 = off — a literal index rather than a
    // slot NAME because macro hygiene keeps a caller-supplied ident from
    // resolving to locals declared inside the macro body.)
    macro_rules! in_range_special {
        ($($name:ident: $slot:expr, $code:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                let mut sides = [sym_num(4, true), sym_num(4, true), sym_num(4, true)];
                sides[$slot] = lit_special($code);
                let [val, base, off] = &sides;
                kani::assume(offset_valid(off));
                let sub: bool = kani::any();
                let less: bool = kani::any();
                in_range_check(val, base, off, sub, less);
            }
        )*};
    }

    in_range_special! {
        eq_in_range_val_nan: 0, NUMERIC_NAN;
        eq_in_range_val_pinf: 0, NUMERIC_PINF;
        eq_in_range_val_ninf: 0, NUMERIC_NINF;
        eq_in_range_base_nan: 1, NUMERIC_NAN;
        eq_in_range_base_pinf: 1, NUMERIC_PINF;
        eq_in_range_base_ninf: 1, NUMERIC_NINF;
        eq_in_range_off_pinf: 2, NUMERIC_PINF;
    }

    // FINITE arm: all three finite — add_var/sub_var + cmp_var live.
    // ARITHMETIC WALL RECORD (measured here, 4 runs): the all-finite arm
    // with a NONZERO symbolic offset is a memory wall even value-level —
    // symex completes (~20-35s) but the SAT phase exceeds the 6GB cap on
    // BOTH solvers (kissat 6.4-7.3GB grinding per-property batches;
    // default reaches UNSATISFIABLE at ~278s then balloons to 9.1GB in
    // its second propositional-reduction pass).  Ladder exhausted:
    // sub case-split (1168k->719k steps), literal sign bits (folded
    // NOTHING — identical formula), |w|<=1 band + exact unwind(9).
    // The wall is INSENSITIVE to the digit-loop bounds: formula size was
    // IDENTICAL at nd<=2 vs nd<=1 and with the offset pinned to the packed
    // zero (649k steps / 13.8k VCCs each; both walled >6GB).  The fixed
    // cost is DigitBuf::realloc_uninit's SYMBOLIC-n branch: `n <=
    // INLINE_DIGITS` cannot fold while n is data-dependent, so the
    // heap/DIGIT_POOL arm's symbolic-size Vec machinery enters the SAT
    // formula even though every in-band n takes the inline arm.
    // This is the presumed numeric digit-arithmetic wall (proofs/TRIAGE.md).
    // Per charter the finite arm therefore carries CONCRETE-SPOT proofs
    // (ladder step 5): base and offset are LITERAL images — so every
    // allocation size is concrete and the heap arm folds away — while VAL
    // STAYS FULLY SYMBOLIC (finite lattice, nd<=2, full weights, both
    // header forms) and `less` stays symbolic; add_var/sub_var run
    // end-to-end on the literal pair and the cmp-after-arithmetic
    // composition is proven for EVERY val against that sum.  Spot shapes:
    // zero offset (both sub arms), carry (9999+1 -> digit growth), borrow
    // (10000-1 -> 9999), exact cancellation (5-5 -> zero_var arm).
    // word_buf_take/put stubbed to the shipped pool-miss/no-recycle arms
    // (same soundness note as the hdr-op harnesses — "modulo TLS
    // word-pool model").  The symbolic finite region remains the recorded
    // GAP (see cover_in_range_partition).

    /// Literal finite numeric (long form) from concrete parts.
    fn lit_finite(digits_in: &[i16], weight: i16, dscale: u16, neg: bool) -> Side {
        let mut buf = Buf([0; 4 + 2 * ND]);
        let mut digits = [0i16; ND];
        let header: u16 = (if neg { NUMERIC_NEG } else { NUMERIC_POS }) | dscale;
        put_u16(&mut buf, 0, header);
        put_u16(&mut buf, 2, weight as u16);
        for (i, d) in digits_in.iter().enumerate() {
            digits[i] = *d;
            put_u16(&mut buf, 4 + 2 * i, *d as u16);
        }
        Side {
            buf,
            len: 4 + 2 * digits_in.len(),
            sign: (if neg { NUMERIC_NEG } else { NUMERIC_POS }) as c_int,
            weight: weight as i32,
            dscale: dscale as c_int,
            digits,
            nd: digits_in.len() as c_int,
            sel: 3,
        }
    }

    macro_rules! in_range_finite_spot {
        ($($name:ident: base = $base:expr, off = $off:expr, sub = $sub:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(9)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            fn $name() {
                let val = sym_num(IN_RANGE_ND, true);
                kani::assume(val.sel >= 3);
                let base: Side = $base;
                let off: Side = $off;
                let less: bool = kani::any();
                in_range_check(&val, &base, &off, $sub, less);
            }
        )*};
    }

    in_range_finite_spot! {
        // offset == 0 plane representatives (1.5 +/- 0)
        eq_in_range_spot_zero_off_add:
            base = lit_finite(&[1, 5000], 0, 1, false), off = lit_finite(&[], 0, 0, false), sub = false;
        eq_in_range_spot_zero_off_sub:
            base = lit_finite(&[1, 5000], 0, 1, true), off = lit_finite(&[], 0, 0, false), sub = true;
        // carry: 9999 + 1 = 10000 (result digit count grows, strip runs)
        eq_in_range_spot_carry:
            base = lit_finite(&[9999], 0, 0, false), off = lit_finite(&[1], 0, 0, false), sub = false;
        // borrow: 10000 - 1 = 9999 (weight drops)
        eq_in_range_spot_borrow:
            base = lit_finite(&[1], 1, 0, false), off = lit_finite(&[1], 0, 0, false), sub = true;
        // exact cancellation: 5 - 5 = 0 (zero_var arm)
        eq_in_range_spot_cancel:
            base = lit_finite(&[5], 0, 0, false), off = lit_finite(&[5], 0, 0, false), sub = true;
        // negative base: -3 + 2 = -1 (sub_abs via add_var sign switch)
        eq_in_range_spot_neg_base:
            base = lit_finite(&[3], 0, 0, true), off = lit_finite(&[2], 0, 0, false), sub = false;
    }

    // ERROR arm, tiled by LITERAL invalid-offset shape (NaN / -Inf /
    // negative-finite in both header forms); verdict + sqlstate + level
    // parity, message text out of proof (PgError::error stub).
    macro_rules! in_range_error {
        ($($name:ident: $mk:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                let val = sym_num(4, true);
                let base = sym_num(4, true);
                let off: Side = $mk;
                let sub: bool = kani::any();
                let less: bool = kani::any();
                let (vs, vw, vd, vp, vn) = spec5(&val);
                let (bs, bw, bd, bp, bn) = spec5(&base);
                let (os, ow, od, op, on) = spec5(&off);
                let mut err: c_int = 0;
                let _ = unsafe {
                    pg_in_range_numeric_numeric(
                        vs, vw, vd, vp, vn, bs, bw, bd, bp, bn, os, ow, od, op, on,
                        sub as c_int, less as c_int, &mut err,
                    )
                };
                assert!(err == 1);
                let r = in_range_numeric_numeric(
                    Num::from_payload(&val.buf.0[..val.len]),
                    Num::from_payload(&base.buf.0[..base.len]),
                    Num::from_payload(&off.buf.0[..off.len]),
                    sub,
                    less,
                );
                match r {
                    Ok(_) => panic!("in_range succeeded where C ereports"),
                    Err(e) => {
                        assert!(
                            e.sqlstate
                                == ::types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
                        );
                        assert!(e.level == ::types_error::ERROR);
                        core::mem::forget(e); // Box<PgError> drop glue walls symex
                    }
                }
            }
        )*};
    }

    in_range_error! {
        eq_in_range_err_nan: lit_special(NUMERIC_NAN);
        eq_in_range_err_ninf: lit_special(NUMERIC_NINF);
        eq_in_range_err_neg_long: lit_neg_finite(false);
        eq_in_range_err_neg_short: lit_neg_finite(true);
    }

    // Union coverage (MANDATORY for the case-split): every input lands in
    // ERR ∪ SPECIAL ∪ FINITE-GAP.  The whole all-finite valid-offset
    // region is the recorded GAP (the measured digit-arithmetic memory
    // wall — see the wall record above the spot harnesses); the spot
    // proofs are exact witnesses INSIDE the gap (concrete base/offset,
    // fully symbolic val), not a tiling of it.
    #[kani::proof]
    #[kani::unwind(12)]
    fn cover_in_range_partition() {
        let val = sym_num(4, true);
        let base = sym_num(4, true);
        let off = sym_num(4, true);
        let p_err = !offset_valid(&off);
        let all_finite = val.sel >= 3 && base.sel >= 3 && off.sel >= 3;
        let p_special = !p_err && !all_finite;
        let p_gap = !p_err && all_finite; // documented remainder (+ spots)
        assert!(p_err || p_special || p_gap);
        // tiling of the SPECIAL region by the 7 literal-special harnesses:
        // some operand is special, and a valid special offset is +Inf only
        assert!(!p_special || val.sel <= 2 || base.sel <= 2 || off.sel == 1);
        // tiling of the ERROR region by the 4 literal-shape harnesses
        assert!(
            !p_err
                || off.sel == 0
                || off.sel == 2
                || (off.sel >= 3 && off.sign == NUMERIC_NEG as c_int)
        );
        // vacuity insurance: every partition (and the gap) is non-empty,
        // and the interesting arms inside them are reachable.
        kani::cover!(p_gap);
        kani::cover!(p_err);
        kani::cover!(p_special && val.sel == 0); // NaN val arm
        kani::cover!(p_special && base.sel == 0); // NaN base arm
        kani::cover!(p_special && off.sel == 1); // +Inf offset arm
        kani::cover!(p_special && val.sel == 1 && base.sel >= 3); // PINF val, finite base
        kani::cover!(p_special && base.sel == 2 && val.sel >= 3); // NINF base, finite val
        kani::cover!(p_gap && base.sign == NUMERIC_NEG as c_int); // neg-base regime
        kani::cover!(p_gap && off.nd == 0); // zero-offset plane (spot-witnessed)
    }

    // ---- negative control #2 (new C file rule): C fed weight+1 on a
    // nonzero finite value — the weight XOR makes the hashes differ for
    // EVERY such input, so this MUST FAIL (run with the DEFAULT solver).
    #[kani::proof]
    #[kani::unwind(12)]
    fn control_hash_numeric_weight() {
        let a = sym_num(2, true);
        kani::assume(a.sel >= 3 && a.nd >= 1);
        let va = varlena_of(&a);
        let (s, w, d, n) = c_args(&a);
        let c = unsafe { pg_hash_numeric(s, w + 1, d, n) };
        let r = call1_ok(fc_hash_numeric, va.0.as_ptr());
        assert!(r.as_u32() == c);
    }

    // ======================================================================
    // TAIL-LANE EXTENSION (2026-07-29): the tail-triage numeric groups
    //   N1 header/inspection: numerictypmodin (2917) + the typmod codec trio
    //      (numerictypmodout 2918's scalar core), numeric_sign (1706),
    //      numeric_scale (3281);
    //   N2 int->numeric: int2_numeric (1782), int4_numeric (1740),
    //      int8_numeric (1781) — int64_to_numeric with its CONSTANT
    //      alloc(20/DEC_DIGITS) (inline DigitBuf arm; NOT DigitBuf-blocked)
    //      and the C digit emission + packed-header ENCODE in-theorem,
    //      full-image byte compare;
    //   N3 int-agg transarray: int2/int4_sum (1840/1841), int2/int4_avg_accum
    //      (1962/1963), the _inv pair (3570/3571), int4_avg_combine (3324) —
    //      WRAPPER-LEVEL over real LocalFcinfo frames with real _int8 array
    //      images built per the on-disk spec; the agg-context demux
    //      (fcinfo->context AggStateNode tag check) is in-theorem.
    // C side: c/pg_numeric_tail.c (provenance + shims there).
    // /NBASE sloped-divider treatment (intout law): per-digit-count magnitude
    // bands with exact unwind + concrete spot proofs for wide regimes +
    // mandatory union-coverage harness.
    // ======================================================================

    use adt_numeric::builtins::{
        fc_int2_avg_accum, fc_int2_avg_accum_inv, fc_int2_sum, fc_int4_avg_accum,
        fc_int4_avg_accum_inv, fc_int4_avg_combine, fc_int4_sum, fc_numeric_scale,
    };
    use adt_numeric::{int2_numeric, int4_numeric, int8_numeric, numeric_sign,
        numerictypmodin_core};
    use datum::NullableDatum;
    use proof_support::fcinfo::fci;
    use proof_support::mcx_stubs;
    use types_fmgr::{AggStateNode, LocalFcinfo};

    extern "C" {
        fn pg_int64_to_numeric_image(val: i64, out: *mut u8, err: *mut c_int) -> c_int;
        fn pg_is_valid_numeric_typmod(typmod: i32) -> c_int;
        fn pg_numeric_typmod_precision(typmod: i32) -> i32;
        fn pg_numeric_typmod_scale(typmod: i32) -> i32;
        fn pg_numerictypmodin(tl: *const i32, n: c_int, err: *mut c_int) -> i32;
        fn pg_numeric_sign_image(sign: c_int, ndigits: c_int, out: *mut u8, err: *mut c_int)
            -> c_int;
        fn pg_numeric_scale(sign: c_int, dscale: i32, isnull: *mut c_int) -> i32;
        fn pg_int2_sum(
            oldsum: i64, arg0null: c_int, newval: i16, arg1null: c_int, retnull: *mut c_int,
        ) -> i64;
        fn pg_int4_sum(
            oldsum: i64, arg0null: c_int, newval: i32, arg1null: c_int, retnull: *mut c_int,
        ) -> i64;
        // int returns (value unused): Kani lowers Rust () as `struct Unit`,
        // which goto-cc rejects against C void (skill trap).
        fn pg_int2_avg_accum(
            count: i64, sum: i64, hasnull: c_int, size: u32, newval: i16,
            out_count: *mut i64, out_sum: *mut i64, err: *mut c_int,
        ) -> c_int;
        fn pg_int4_avg_accum(
            count: i64, sum: i64, hasnull: c_int, size: u32, newval: i32,
            out_count: *mut i64, out_sum: *mut i64, err: *mut c_int,
        ) -> c_int;
        fn pg_int2_avg_accum_inv(
            count: i64, sum: i64, hasnull: c_int, size: u32, newval: i16,
            out_count: *mut i64, out_sum: *mut i64, err: *mut c_int,
        ) -> c_int;
        fn pg_int4_avg_accum_inv(
            count: i64, sum: i64, hasnull: c_int, size: u32, newval: i32,
            out_count: *mut i64, out_sum: *mut i64, err: *mut c_int,
        ) -> c_int;
        #[allow(clippy::too_many_arguments)]
        fn pg_int4_avg_combine(
            in_agg: c_int,
            count1: i64, sum1: i64, hasnull1: c_int, size1: u32,
            count2: i64, sum2: i64, hasnull2: c_int, size2: u32,
            out_count: *mut i64, out_sum: *mut i64, err: *mut c_int,
        ) -> c_int;
        fn pg_numeric_send(
            sign: c_int, weight: c_int, dscale: c_int,
            digits: *const i16, ndigits: c_int, out: *mut u8,
        ) -> c_int;
        fn pg_apply_typmod_special(sign: c_int, typmod: i32, err: *mut c_int) -> c_int;
    }

    // ---- N2: int->numeric conversions -------------------------------------
    // Value-level at the ops layer (int2/int4/int8_numeric -> int64_to_numeric);
    // the fc wrappers' datum plumbing stays in the tested tier (fmgr result-mcx
    // arena).  The FULL varlena image (header included) is compared
    // byte-for-byte; C's digit-emission loop and header encode are in-theorem.
    // Stubs: TLS word pool -> shipped pool-miss arm ("modulo TLS word-pool
    // model", family convention).

    /// Shared conversion claim body: shipped image vs vendored-C image.
    fn conv_check(r: adt_numeric::var::NumericImage, v: i64) {
        let mut out = [0u8; 32];
        let mut err: c_int = 0;
        let clen = unsafe { pg_int64_to_numeric_image(v, out.as_mut_ptr(), &mut err) };
        assert!(err == 0);
        let rb = r.as_bytes();
        assert!(rb.len() == clen as usize);
        let mut i = 0;
        while i < rb.len() {
            assert!(rb[i] == out[i]);
            i += 1;
        }
        core::mem::forget(r);
    }

    /// int2_numeric: FULL i16 domain in one harness (<=2 NBASE digits; the
    /// /NBASE dividend is 16-bit — the proven-fast division class).
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
    #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
    fn eq_int2_numeric_full() {
        let v: i16 = kani::any();
        conv_check(int2_numeric(v), v as i64);
        kani::cover!(v == 0);
        kani::cover!(v == i16::MIN);
    }

    /// Magnitude-band cells (sloped-divider law): the digit-emission loop
    /// shrinks structurally with the band, so bands + exact unwind work.
    macro_rules! conv_band {
        ($($name:ident: $call:ident($ty:ty), $lo:expr, $hi:expr, unwind = $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            fn $name() {
                let v: $ty = kani::any();
                let m = (v as i64).unsigned_abs();
                kani::assume(m >= $lo && m < $hi);
                conv_check($call(v), v as i64);
            }
        )*};
    }

    conv_band! {
        // nd<=1 cells (|v| < NBASE)
        eq_int4_numeric_nd1: int4_numeric(i32), 0, 10_000, unwind = 10;
        eq_int8_numeric_nd1: int8_numeric(i64), 0, 10_000, unwind = 10;
        // nd=2 cells (NBASE <= |v| < NBASE^2)
        eq_int4_numeric_nd2: int4_numeric(i32), 10_000, 100_000_000, unwind = 12;
        eq_int8_numeric_nd2: int8_numeric(i64), 10_000, 100_000_000, unwind = 12;
        // nd=3 remainder of the i32 domain (1e8 <= |v| <= 2^31)
        eq_int4_numeric_nd3: int4_numeric(i32), 100_000_000, 0x8000_0001, unwind = 14;
        // int8 wide bands (sloped-divider ladder; measured shallower than
        // the 1e7-width law here because the loop shrinks structurally)
        eq_int8_numeric_nd3: int8_numeric(i64), 100_000_000, 1_000_000_000_000, unwind = 14;
        eq_int8_numeric_nd4: int8_numeric(i64), 1_000_000_000_000, 10_000_000_000_000_000, unwind = 16;
        eq_int8_numeric_nd5: int8_numeric(i64), 10_000_000_000_000_000, 0x8000_0000_0000_0001, unwind = 18;
    }

    /// Concrete spot proofs for the wide-magnitude int8 regimes (nd 3..5;
    /// fully literal input, so the divider chain constant-folds): digit-count
    /// transitions, extremes, and dense-digit interior points.
    macro_rules! conv_spot {
        ($($name:ident: $v:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(20)]
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            fn $name() {
                conv_check(int8_numeric($v), $v);
            }
        )*};
    }

    conv_spot! {
        eq_int8_numeric_spot_nd3_lo: 100_000_000;               // 1e8: nd2->nd3 edge
        eq_int8_numeric_spot_nd3_hi: 999_999_999_999;           // nd3 top, dense digits
        eq_int8_numeric_spot_nd4_lo: 1_000_000_000_000;         // 1e12: nd3->nd4 edge
        eq_int8_numeric_spot_nd4_hi: -9_999_999_999_999_999;    // nd4 top, negative
        eq_int8_numeric_spot_nd5_lo: 10_000_000_000_000_000;    // 1e16: nd4->nd5 edge
        eq_int8_numeric_spot_i64_min: i64::MIN;                 // pg_abs_s64 edge
        eq_int8_numeric_spot_i64_max: i64::MAX;
        eq_int8_numeric_spot_trailzero: -1_234_000_000_000_000; // zero low digits
    }

    /// Union coverage for the band split (MANDATORY): the i32 domain is
    /// covered exactly by nd1|nd2|nd3; the i64 domain by nd1|nd2 plus the
    /// documented wide-magnitude remainder (spot-witnessed).
    #[kani::proof]
    fn cover_int_numeric_bands() {
        let v4: i32 = kani::any();
        let m4 = (v4 as i64).unsigned_abs();
        assert!(m4 < 10_000 || (10_000..100_000_000).contains(&m4) || m4 >= 100_000_000);
        assert!(m4 <= 0x8000_0000); // nd3 cell's upper fence covers all i32
        let v8: i64 = kani::any();
        let m8 = v8.unsigned_abs();
        let gap = m8 >= 100_000_000; // int8 wide-magnitude remainder
        assert!(m8 < 10_000 || (10_000..100_000_000).contains(&m8) || gap);
        kani::cover!(gap);
    }

    // ---- negative control #3 (new C file rule): C converts v+1 — the images
    // differ for EVERY v, so this MUST FAIL (DEFAULT solver).
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
    #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
    fn control_int2_numeric_off_by_one() {
        let v: i16 = kani::any();
        conv_check(int2_numeric(v), v as i64 + 1);
    }

    // ---- N1: typmod codec + numerictypmodin --------------------------------

    /// The typmod codec trio over the FULL i32 typmod domain — this is
    /// numerictypmodout's scalar core (row 2918; its "(p,s)" rendering is
    /// std format! and stays out of proof) and the decode used by
    /// apply_typmod and 8 sibling rows.
    #[kani::proof]
    fn eq_numeric_typmod_codec() {
        let typmod: i32 = kani::any();
        let v = adt_numeric::ops::is_valid_numeric_typmod(typmod);
        assert!(v == (unsafe { pg_is_valid_numeric_typmod(typmod) } != 0));
        // CALL-CONTRACT FENCE: every shipped precision/scale call site gates
        // on is_valid_numeric_typmod first (apply_typmod, numerictypmodout),
        // so typmod >= VARHDRSZ is the codec's domain.  Below it, Rust's
        // `typmod - VARHDRSZ` overflow check fires under Kani (C wraps via
        // -fwrapv; shipped release Rust wraps identically) — out-of-contract,
        // not a parity plane.
        kani::assume(v);
        let p = adt_numeric::ops::numeric_typmod_precision(typmod);
        assert!(p == unsafe { pg_numeric_typmod_precision(typmod) });
        let s = adt_numeric::ops::numeric_typmod_scale(typmod);
        assert!(s == unsafe { pg_numeric_typmod_scale(typmod) });
        kani::cover!(s < 0); // negative-scale sign-extension arm
        kani::cover!(p > 0x7ff); // precision/scale field independence
    }

    /// numerictypmodin over the decoded typmod list (the shipped wrapper's
    /// array decode is arrayfuncs plumbing in the tested tier): full i32
    /// element values, list length 0..=3; value + error verdict + sqlstate
    /// parity (message text/location out of proof — PgError stubs).
    #[kani::proof]
    #[kani::unwind(6)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_numerictypmodin() {
        let tl: [i32; 3] = kani::any();
        let n: usize = kani::any();
        kani::assume(n <= 3);
        let mut err: c_int = 0;
        let c = unsafe { pg_numerictypmodin(tl.as_ptr(), n as c_int, &mut err) };
        match numerictypmodin_core(&tl[..n]) {
            Ok(r) => {
                assert!(err == 0);
                assert!(r == c);
            }
            Err(e) => {
                assert!(err == 1);
                assert!(e.sqlstate == ::types_error::ERRCODE_INVALID_PARAMETER_VALUE);
                assert!(e.level == ::types_error::ERROR);
                core::mem::forget(e);
            }
        }
        kani::cover!(err == 0 && n == 2);
        kani::cover!(err == 0 && n == 1);
        kani::cover!(err == 1 && n == 2); // range reject
        kani::cover!(err == 1 && n == 0); // bad list length
    }

    // ---- N1: numeric_sign ---------------------------------------------------
    // Value-level; result images (make_result of the numeric.c const vars)
    // compared byte-for-byte, with the C header ENCODE in-theorem
    // (pg_tail_make_result_opt_error).  Contract domain (stripped packed form:
    // zero <=> ndigits == 0, per the on-disk convention).

    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
    #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
    // make_result's overflow arm is value-dead here (literal const vars) but
    // its PgError::error is #[track_caller] (Location::caller unsupported)
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_numeric_sign_nd4() {
        let a = sym_num(4, true);
        let mut out = [0u8; 32];
        let mut err: c_int = 0;
        let clen = unsafe { pg_numeric_sign_image(a.sign, a.nd, out.as_mut_ptr(), &mut err) };
        assert!(err == 0);
        let r = match numeric_sign(Num::from_payload(&a.buf.0[..a.len])) {
            Ok(img) => img,
            Err(_) => panic!("numeric_sign errored"),
        };
        let rb = r.as_bytes();
        assert!(rb.len() == clen as usize);
        let mut i = 0;
        while i < rb.len() {
            assert!(rb[i] == out[i]);
            i += 1;
        }
        core::mem::forget(r);
        kani::cover!(a.sel == 0); // NaN passthrough
        kani::cover!(a.sel == 2); // -Inf -> -1
        kani::cover!(a.sel >= 3 && a.nd == 0); // zero
        kani::cover!(a.sel >= 3 && a.sign == NUMERIC_NEG as c_int && a.nd > 0);
    }

    // ---- N1: numeric_scale ---------------------------------------------------
    // WRAPPER-LEVEL (real LocalFcinfo frame + inline varlena arg; shipped
    // num_arg packed-header decode in-theorem); C side takes the spec tuple
    // (SHIM T1).  Full value lattice, both header forms.

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_numeric_scale_nd4() {
        let a = sym_num(4, true);
        let va = varlena_of(&a);
        let mut isnull: c_int = 0;
        let c = unsafe { pg_numeric_scale(a.sign, a.dscale, &mut isnull) };
        let mut f = fci::<1>([Datum::from_usize(va.0.as_ptr() as usize)]);
        let r = match fc_numeric_scale(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("numeric_scale errored"),
        };
        assert!(f.isnull == (isnull != 0));
        if isnull == 0 {
            assert!(r.as_i32() == c);
        }
        kani::cover!(isnull != 0); // specials -> NULL
        kani::cover!(isnull == 0 && a.sel == 3 && a.dscale > 63); // long-form dscale
        kani::cover!(isnull == 0 && a.sel == 4); // short-form dscale
    }

    // ---- N3: int2_sum / int4_sum ---------------------------------------------
    // WRAPPER-LEVEL, all four null combinations symbolic in one harness;
    // -fwrapv wrap parity (the shipped wrapping_add comment-claim) is
    // machine-checked because CBMC wraps the vendored C's int64 addition.

    macro_rules! sum_harness {
        ($($name:ident: $fc:path, $pg:ident, $ty:ty, $from:ident;)*) => {$(
            #[kani::proof]
            fn $name() {
                let s: i64 = kani::any();
                let v: $ty = kani::any();
                let a0null: bool = kani::any();
                let a1null: bool = kani::any();
                let mut retnull: c_int = 0;
                let c = unsafe {
                    $pg(s, a0null as c_int, v, a1null as c_int, &mut retnull)
                };
                let mut f = LocalFcinfo::<2>::new(0);
                f.args[0] = if a0null {
                    NullableDatum::null()
                } else {
                    NullableDatum::value(Datum::from_i64(s))
                };
                f.args[1] = if a1null {
                    NullableDatum::null()
                } else {
                    NullableDatum::value(Datum::$from(v))
                };
                let r = match $fc(None, &mut f) {
                    Ok(d) => d,
                    Err(_) => panic!("sum errored"),
                };
                assert!(f.isnull == (retnull != 0));
                if retnull == 0 {
                    assert!(r.as_i64() == c);
                }
                kani::cover!(retnull != 0); // both-null arm
                kani::cover!(!a0null && !a1null); // accumulate arm (wrap plane)
                kani::cover!(a0null && !a1null); // first-input arm
                kani::cover!(!a0null && a1null); // null-input passthrough
            }
        )*};
    }

    sum_harness! {
        eq_int2_sum: fc_int2_sum, pg_int2_sum, i16, from_i16;
        eq_int4_sum: fc_int4_sum, pg_int4_sum, i32, from_i32;
    }

    // ---- N3: int8[2] transarray family ----------------------------------------
    // Real _int8 array images per the on-disk spec (4B-U varlena header, ndim 1,
    // dataoffset 0 (no nulls), elemtype int8=20, dims {2}, lbound {1}, then
    // {count, sum}); the shipped varatt demux + hasnull/size validation is
    // in-theorem.  The agg-context seam is the REAL fcinfo->context demux: a
    // live AggStateNode for the in-place arm, None for the copy arm.

    const INT8_ARR_SIZE: u32 = 40; // ARR_OVERHEAD_NONULLS(1)=24 + 2*int8

    #[repr(align(8))]
    struct ArrBuf([u8; 40]);

    fn int8_arr_image(count: i64, sum: i64, size: u32, dataoffset: i32) -> ArrBuf {
        let mut b = ArrBuf([0; 40]);
        b.0[0..4].copy_from_slice(&(size << 2).to_ne_bytes()); // 4B-U varlena header
        b.0[4..8].copy_from_slice(&1i32.to_ne_bytes()); // ndim
        b.0[8..12].copy_from_slice(&dataoffset.to_ne_bytes()); // 0 = no nulls bitmap
        b.0[12..16].copy_from_slice(&20u32.to_ne_bytes()); // elemtype = int8
        b.0[16..20].copy_from_slice(&2i32.to_ne_bytes()); // dims[0]
        b.0[20..24].copy_from_slice(&1i32.to_ne_bytes()); // lbound[0]
        b.0[24..32].copy_from_slice(&count.to_ne_bytes());
        b.0[32..40].copy_from_slice(&sum.to_ne_bytes());
        b
    }

    fn arr_count_sum(b: &ArrBuf) -> (i64, i64) {
        // SAFETY: ArrBuf is 8-aligned, slots 24/32 in-bounds.
        unsafe {
            (
                b.0.as_ptr().add(24).cast::<i64>().read(),
                b.0.as_ptr().add(32).cast::<i64>().read(),
            )
        }
    }

    /// The in-place (agg) arm: result datum must alias the input array and
    /// carry the C-computed {count,sum}.
    ///
    /// OVERFLOW FENCE (finding packaged, not fixed): the shipped transarray
    /// `*td += 1; *td.add(1) += newval` uses PLAIN i64 arithmetic where C
    /// wraps under -fwrapv.  Release Rust wraps identically (parity holds in
    /// shipped binaries), but debug builds PANIC on the wrap plane — the
    /// int4_sum sibling in the same file got the explicit wrapping_add
    /// treatment for exactly this (builtins.rs:27-30 comment); the
    /// transarray family did not.  Kani (forced overflow checks) surfaces
    /// that panic plane, so the theorems are fenced to the non-overflowing
    /// domain: count +/- 1 and sum +/- newval representable.  The wrap
    /// plane itself is out of these theorems, recorded in the ledger rows.
    fn accum_fence(count: i64, sum: i64, delta_bound: i64) -> (i64, i64) {
        let _ = (count, sum);
        // constant-bound fences (checked_* fences cost ~25x — dt-minmax law)
        kani::assume(count > i64::MIN && count < i64::MAX);
        kani::assume(sum > i64::MIN + delta_bound && sum < i64::MAX - delta_bound);
        (count, sum)
    }
    macro_rules! avg_accum_agg_harness {
        ($($name:ident: $fc:path, $pg:ident, $ty:ty, $from:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(8)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                let (count, sum) =
                    accum_fence(kani::any(), kani::any(), 1i64 << 32);
                let v: $ty = kani::any();
                let (mut oc, mut os): (i64, i64) = (0, 0);
                let mut err: c_int = 0;
                unsafe {
                    $pg(count, sum, 0, INT8_ARR_SIZE, v, &mut oc, &mut os, &mut err)
                };
                assert!(err == 0);

                let mut arr = int8_arr_image(count, sum, INT8_ARR_SIZE, 0);
                let ctx = mcx::MemoryContext::new_bump("kani-tail-agg");
                let mut node = AggStateNode::new(ctx);
                let mut f = LocalFcinfo::<2>::new(0);
                f.context = node.fm_node_ptr();
                f.args[0] =
                    NullableDatum::value(Datum::from_usize(arr.0.as_mut_ptr() as usize));
                f.args[1] = NullableDatum::value(Datum::$from(v));
                let r = match $fc(None, &mut f) {
                    Ok(d) => d,
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("avg_accum errored on a valid transarray")
                    }
                };
                // in-place cheat: the returned datum IS the input array
                assert!(r.as_usize() == arr.0.as_ptr() as usize);
                let (nc, ns) = arr_count_sum(&arr);
                assert!(nc == oc);
                assert!(ns == os);
                core::mem::forget(node);
            }
        )*};
    }

    avg_accum_agg_harness! {
        eq_int2_avg_accum_agg: fc_int2_avg_accum, pg_int2_avg_accum, i16, from_i16;
        eq_int4_avg_accum_agg: fc_int4_avg_accum, pg_int4_avg_accum, i32, from_i32;
        eq_int2_avg_accum_inv_agg: fc_int2_avg_accum_inv, pg_int2_avg_accum_inv, i16, from_i16;
        eq_int4_avg_accum_inv_agg: fc_int4_avg_accum_inv, pg_int4_avg_accum_inv, i32, from_i32;
    }

    /// The copy (non-agg) arm, int4 representative (the macro body is
    /// identical across int2/int4/inv modulo the $get cast, proven separately
    /// above): result is a FRESH image (mcx-stubs recipe, "modulo
    /// static-buffer allocator model"), the input is untouched, and the copy
    /// carries the C-computed {count,sum}.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_int4_avg_accum_copy() {
        let (count, sum) = accum_fence(kani::any(), kani::any(), 1i64 << 32);
        let v: i32 = kani::any();
        let (mut oc, mut os): (i64, i64) = (0, 0);
        let mut err: c_int = 0;
        unsafe {
            pg_int4_avg_accum(count, sum, 0, INT8_ARR_SIZE, v, &mut oc, &mut os, &mut err)
        };
        assert!(err == 0);

        let arr = int8_arr_image(count, sum, INT8_ARR_SIZE, 0);
        let ctx = mcx::MemoryContext::new_bump("kani-tail-copy");
        let mut f = LocalFcinfo::<2>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(arr.0.as_ptr() as usize));
        f.args[1] = NullableDatum::value(Datum::from_i32(v));
        let r = match fc_int4_avg_accum(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("avg_accum errored on a valid transarray")
            }
        };
        // copy arm: fresh image, input untouched
        assert!(r.as_usize() != arr.0.as_ptr() as usize);
        let (ic, is) = arr_count_sum(&arr);
        assert!(ic == count && is == sum);
        let rp = r.as_usize() as *const u8;
        // SAFETY: byref result is the 40-byte copied image.
        let (nc, ns) = unsafe {
            (
                rp.add(24).cast::<i64>().read_unaligned(),
                rp.add(32).cast::<i64>().read_unaligned(),
            )
        };
        assert!(nc == oc);
        assert!(ns == os);
        core::mem::forget(ctx);
    }

    /// Validation error class, int4-accum representative under the agg arm
    /// (no copy => no derived-length machinery): hasnull (dataoffset != 0) or
    /// wrong size must error on BOTH sides (C elog / shipped PgError; message
    /// out of proof).
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_int4_avg_accum_badarr() {
        let count: i64 = kani::any();
        let sum: i64 = kani::any();
        let v: i32 = kani::any();
        let size: u32 = kani::any();
        let dataoffset: i32 = kani::any();
        kani::assume(size >= 12 && size <= 40); // header-readable, in-buffer
        kani::assume(size != INT8_ARR_SIZE || dataoffset != 0);
        let mut oc: i64 = 0;
        let mut os: i64 = 0;
        let mut err: c_int = 0;
        unsafe {
            pg_int4_avg_accum(
                count, sum, (dataoffset != 0) as c_int, size, v, &mut oc, &mut os, &mut err,
            )
        };
        assert!(err == 1);

        let mut arr = int8_arr_image(count, sum, size, dataoffset);
        let ctx = mcx::MemoryContext::new_bump("kani-tail-bad");
        let mut node = AggStateNode::new(ctx);
        let mut f = LocalFcinfo::<2>::new(0);
        f.context = node.fm_node_ptr();
        f.args[0] = NullableDatum::value(Datum::from_usize(arr.0.as_mut_ptr() as usize));
        f.args[1] = NullableDatum::value(Datum::from_i32(v));
        match fc_int4_avg_accum(None, &mut f) {
            Ok(_) => panic!("accum accepted a transarray C rejects"),
            Err(e) => core::mem::forget(e),
        }
        core::mem::forget(node);
    }

    /// int4_avg_combine, agg arm: in-place merge into transarray 1.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_int4_avg_combine_agg() {
        // same overflow fence, pairwise: c1+c2 / s1+s2 representable
        let (c1, s1): (i64, i64) = (kani::any(), kani::any());
        let (c2, s2): (i64, i64) = (kani::any(), kani::any());
        kani::assume(c1 > i64::MIN / 2 && c1 < i64::MAX / 2);
        kani::assume(c2 > i64::MIN / 2 && c2 < i64::MAX / 2);
        kani::assume(s1 > i64::MIN / 2 && s1 < i64::MAX / 2);
        kani::assume(s2 > i64::MIN / 2 && s2 < i64::MAX / 2);
        let (mut oc, mut os): (i64, i64) = (0, 0);
        let mut err: c_int = 0;
        unsafe {
            pg_int4_avg_combine(
                1, c1, s1, 0, INT8_ARR_SIZE, c2, s2, 0, INT8_ARR_SIZE, &mut oc, &mut os,
                &mut err,
            )
        };
        assert!(err == 0);

        let mut a1 = int8_arr_image(c1, s1, INT8_ARR_SIZE, 0);
        let mut a2 = int8_arr_image(c2, s2, INT8_ARR_SIZE, 0);
        let ctx = mcx::MemoryContext::new_bump("kani-tail-comb");
        let mut node = AggStateNode::new(ctx);
        let mut f = LocalFcinfo::<2>::new(0);
        f.context = node.fm_node_ptr();
        f.args[0] = NullableDatum::value(Datum::from_usize(a1.0.as_mut_ptr() as usize));
        f.args[1] = NullableDatum::value(Datum::from_usize(a2.0.as_mut_ptr() as usize));
        let r = match fc_int4_avg_combine(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("combine errored in agg context")
            }
        };
        assert!(r.as_usize() == a1.0.as_ptr() as usize);
        let (nc, ns) = arr_count_sum(&a1);
        assert!(nc == oc);
        assert!(ns == os);
        let (uc, us) = arr_count_sum(&a2); // state2 untouched
        assert!(uc == c2 && us == s2);
        core::mem::forget(node);
    }

    // ---- N1 ladder: numeric_send (2461) ------------------------------------
    // WRAPPER-LEVEL (fc_numeric_send over a real frame; shipped num_arg decode
    // + pqformat StringInfo emission in-theorem, "modulo static-buffer
    // allocator model").  Per-nd cells (hash-harness split); specials live in
    // the nd==0 cell (2-byte payloads decode to ndigits 0).  C side emits the
    // verbatim field order in network byte order (SHIM T2/T4).

    macro_rules! send_harness_nd {
        ($($name:ident: $nd:expr, unwind = $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                let a = sym_num(4, true);
                kani::assume(a.nd == $nd);
                let va = varlena_of(&a);
                let mut out = [0u8; 24];
                let (s, w, d, n) = c_args(&a);
                let clen = unsafe {
                    pg_numeric_send(s, w, a.dscale, d, n, out.as_mut_ptr())
                };
                let ctx = mcx::MemoryContext::new_bump("kani-tail-send");
                let mut f = LocalFcinfo::<1>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] =
                    NullableDatum::value(Datum::from_usize(va.0.as_ptr() as usize));
                let r = match adt_numeric::builtins::fc_numeric_send(None, &mut f) {
                    Ok(d) => d,
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("numeric_send errored")
                    }
                };
                let rp = r.as_usize() as *const u8;
                // SAFETY: result is a 4B-header bytea image of clen payload.
                unsafe {
                    let total = (rp.cast::<u32>().read() >> 2) as usize;
                    assert!(total == 4 + clen as usize);
                    let mut i = 0;
                    while i < clen as usize {
                        assert!(*rp.add(4 + i) == out[i]);
                        i += 1;
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    send_harness_nd! {
        eq_numeric_send_snd0: 0, unwind = 10;
        eq_numeric_send_snd1: 1, unwind = 12;
        eq_numeric_send_snd2: 2, unwind = 14;
        eq_numeric_send_snd3: 3, unwind = 16;
        eq_numeric_send_snd4: 4, unwind = 18;
    }

    // Union coverage: within the stripped contract domain nd is 0..=4
    // (cover_hash_numeric_split proves the identical split on the same
    // builder — reused; specials have nd == 0).

    // ---- N1 ladder: numeric (apply typmod, 1703) ----------------------------
    // Planes (finite valid-typmod arms = documented remainder — the
    // NumericVar::from_view/round path re-enters the DigitBuf symbolic-n
    // machinery, the measured in_range digit-arithmetic memory wall):
    //   SPECIAL: literal special operand (folds the finite arms out —
    //     wave-6 literal-fold law), FULL symbolic typmod; NaN passes any
    //     typmod, Inf errors iff typmod is valid; passthrough duplicates
    //     the image.  WRAPPER-LEVEL.
    //   FINITE/INVALID-TYPMOD: literal invalid typmod spots (folds the
    //     valid arm out), fully symbolic finite operand: passthrough image
    //     identity.

    macro_rules! numeric_typmod_special {
        ($($name:ident: $code:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                let a = lit_special($code);
                let va = varlena_of(&a);
                let typmod: i32 = kani::any();
                let mut err: c_int = 0;
                let _ok = unsafe { pg_apply_typmod_special(a.sign, typmod, &mut err) };
                let ctx = mcx::MemoryContext::new_bump("kani-tail-typmod");
                let mut f = LocalFcinfo::<2>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] =
                    NullableDatum::value(Datum::from_usize(va.0.as_ptr() as usize));
                f.args[1] = NullableDatum::value(Datum::from_i32(typmod));
                match adt_numeric::builtins::fc_numeric(None, &mut f) {
                    Ok(r) => {
                        assert!(err == 0);
                        // duplicate_numeric: result image == input image
                        let rp = r.as_usize() as *const u8;
                        // SAFETY: result is a numeric varlena image.
                        unsafe {
                            let total = (rp.cast::<u32>().read() >> 2) as usize;
                            assert!(total == 4 + a.len);
                            let mut i = 0;
                            while i < a.len {
                                assert!(*rp.add(4 + i) == a.buf.0[i]);
                                i += 1;
                            }
                        }
                    }
                    Err(e) => {
                        assert!(err == 1);
                        assert!(
                            e.sqlstate == ::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
                        );
                        assert!(e.level == ::types_error::ERROR);
                        core::mem::forget(e);
                    }
                }
                kani::cover!(err == 0);
                core::mem::forget(ctx);
            }
        )*};
    }

    numeric_typmod_special! {
        eq_numeric_typmod_nan: NUMERIC_NAN;
    }

    // Inf x fully-symbolic typmod walled >9.8GB (mixed pass/error arms under
    // a symbolic selector — the cold-error-arm trap; NaN proved because its
    // error arm folds dead).  Case-split to LITERAL typmod arms; the
    // typmod-validity verdict itself is proved full-i32 by
    // eq_numeric_typmod_codec, so [codec + these arm spots] tile the plane.
    macro_rules! numeric_typmod_inf_arm {
        ($($name:ident: $code:expr, $typmod:expr, err = $want:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                let a = lit_special($code);
                let va = varlena_of(&a);
                let typmod: i32 = $typmod;
                let mut err: c_int = 0;
                let _ok = unsafe { pg_apply_typmod_special(a.sign, typmod, &mut err) };
                assert!(err == $want);
                let ctx = mcx::MemoryContext::new_bump("kani-tail-typmod-arm");
                let mut f = LocalFcinfo::<2>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] =
                    NullableDatum::value(Datum::from_usize(va.0.as_ptr() as usize));
                f.args[1] = NullableDatum::value(Datum::from_i32(typmod));
                match adt_numeric::builtins::fc_numeric(None, &mut f) {
                    Ok(r) => {
                        assert!(err == 0);
                        let rp = r.as_usize() as *const u8;
                        // SAFETY: result is a numeric varlena image.
                        unsafe {
                            let total = (rp.cast::<u32>().read() >> 2) as usize;
                            assert!(total == 4 + a.len);
                            let mut i = 0;
                            while i < a.len {
                                assert!(*rp.add(4 + i) == a.buf.0[i]);
                                i += 1;
                            }
                        }
                    }
                    Err(e) => {
                        assert!(err == 1);
                        assert!(
                            e.sqlstate
                                == ::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
                        );
                        assert!(e.level == ::types_error::ERROR);
                        core::mem::forget(e);
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    numeric_typmod_inf_arm! {
        // invalid typmod (0 < VARHDRSZ): Inf passes through
        eq_numeric_typmod_pinf_pass: NUMERIC_PINF, 0, err = 0;
        eq_numeric_typmod_ninf_pass: NUMERIC_NINF, 0, err = 0;
        // valid typmod (precision 1, scale 0): Inf errors
        eq_numeric_typmod_pinf_err: NUMERIC_PINF, (1 << 16) + 4, err = 1;
        eq_numeric_typmod_ninf_err: NUMERIC_NINF, (1 << 16) + 4, err = 1;
    }

    /// Finite operand, LITERAL invalid typmod (0 < VARHDRSZ): both header
    /// forms, passthrough image identity.  C arm: is_valid_numeric_typmod(0)
    /// is false on both sides (proved full-domain in eq_numeric_typmod_codec),
    /// so the C contract here is the duplicate itself.
    #[kani::proof]
    #[kani::unwind(24)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
    #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_numeric_typmod_invalid_finite() {
        let a = sym_num(4, true);
        kani::assume(a.sel >= 3);
        let va = varlena_of(&a);
        assert!(unsafe { pg_is_valid_numeric_typmod(0) } == 0);
        let ctx = mcx::MemoryContext::new_bump("kani-tail-typmod-inv");
        let mut f = LocalFcinfo::<2>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(va.0.as_ptr() as usize));
        f.args[1] = NullableDatum::value(Datum::from_i32(0));
        let r = match adt_numeric::builtins::fc_numeric(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("numeric() errored on an invalid typmod")
            }
        };
        let rp = r.as_usize() as *const u8;
        // SAFETY: result is a numeric varlena image.
        unsafe {
            let total = (rp.cast::<u32>().read() >> 2) as usize;
            assert!(total == 4 + a.len);
            let mut i = 0;
            while i < a.len {
                assert!(*rp.add(4 + i) == a.buf.0[i]);
                i += 1;
            }
        }
        core::mem::forget(ctx);
    }

    /// WALL RECORD (measured 2026-07-29): the fully-symbolic finite
    /// passthrough plane above (eq_numeric_typmod_invalid_finite) is a CNF
    /// MEMORY wall — symex completes (188s, 1.92M steps, 14.3k VCCs) but
    /// propositional reduction was watchdog-killed at 13.2GB RSS (6GiB-cap
    /// protocol; kissat).  Shape = result-image law: `a.len` is symbolic
    /// (nd x header form), so every passthrough image byte-compare reads at
    /// symbolic offsets.  Assume-based nd/form pinning is refuted by the
    /// measured "assumes do not constant-fold symbolic offsets" law, so the
    /// remedy is concrete spot cells (below); the symbolic plane stays the
    /// recorded gap.
    ///
    /// SPOT RESULT (2026-07-29): even the CONCRETE spot cells wall on memory
    /// (6.5-7.4GB, BOTH solvers, with the full mcx-stub recipe incl
    /// grow/deallocate) — symex completes (~13.4k VCCs) and the SAT phase
    /// balloons.  The finite-arm fc_numeric wrapper shape itself is the wall
    /// (the 2-byte special passthroughs prove at ~90s through the same
    /// wrapper).  Ladder exhausted at the 6GiB laptop cap; these cells are
    /// FLEET 40GB RETRY candidates (memory walls are cap-relative).  1703's
    /// finite arms (valid AND invalid typmod) are the recorded gap; the
    /// codec theorem (full i32) + special planes stand.
    macro_rules! numeric_typmod_invalid_spot {
        ($($name:ident: $mk:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(24)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                let a: Side = $mk;
                let va = varlena_of(&a);
                assert!(unsafe { pg_is_valid_numeric_typmod(0) } == 0);
                let ctx = mcx::MemoryContext::new_bump("kani-tail-typmod-spot");
                let mut f = LocalFcinfo::<2>::new(0);
                // SAFETY: ctx outlives the call (forgotten, never freed).
                unsafe { f.set_result_mcx(ctx.mcx()) };
                f.args[0] = NullableDatum::value(Datum::from_usize(va.0.as_ptr() as usize));
                f.args[1] = NullableDatum::value(Datum::from_i32(0));
                let r = match adt_numeric::builtins::fc_numeric(None, &mut f) {
                    Ok(d) => d,
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("numeric() errored on an invalid typmod")
                    }
                };
                let rp = r.as_usize() as *const u8;
                // SAFETY: result is a numeric varlena image.
                unsafe {
                    let total = (rp.cast::<u32>().read() >> 2) as usize;
                    assert!(total == 4 + a.len);
                    let mut i = 0;
                    while i < a.len {
                        assert!(*rp.add(4 + i) == a.buf.0[i]);
                        i += 1;
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    /// Literal short-form finite numeric from concrete parts (spot cells).
    fn lit_short_finite(digits_in: &[i16], weight: i16, dscale: u16, neg: bool) -> Side {
        let mut buf = Buf([0; 4 + 2 * ND]);
        let mut digits = [0i16; ND];
        let header: u16 = 0x8000
            | if neg { 0x2000 } else { 0 }
            | (dscale << 7)
            | ((weight as u16) & 0x7F);
        put_u16(&mut buf, 0, header);
        for (i, d) in digits_in.iter().enumerate() {
            digits[i] = *d;
            put_u16(&mut buf, 2 + 2 * i, *d as u16);
        }
        Side {
            buf,
            len: 2 + 2 * digits_in.len(),
            sign: (if neg { NUMERIC_NEG } else { NUMERIC_POS }) as c_int,
            weight: weight as i32,
            dscale: dscale as c_int,
            digits,
            nd: digits_in.len() as c_int,
            sel: 4,
        }
    }

    numeric_typmod_invalid_spot! {
        eq_numeric_typmod_invalid_spot_long: lit_finite(&[7, 1234], 1, 3, true);
        eq_numeric_typmod_invalid_spot_short: lit_short_finite(&[42, 9999], -2, 5, false);
    }

    /// int4_avg_combine outside an aggregate: both sides must reject
    /// (verdict parity; C err code 1 = the non-aggregate-context elog).
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_int4_avg_combine_nonagg() {
        let (c1, s1): (i64, i64) = (kani::any(), kani::any());
        let (c2, s2): (i64, i64) = (kani::any(), kani::any());
        let (mut oc, mut os): (i64, i64) = (0, 0);
        let mut err: c_int = 0;
        unsafe {
            pg_int4_avg_combine(
                0, c1, s1, 0, INT8_ARR_SIZE, c2, s2, 0, INT8_ARR_SIZE, &mut oc, &mut os,
                &mut err,
            )
        };
        assert!(err == 1);
        let mut a1 = int8_arr_image(c1, s1, INT8_ARR_SIZE, 0);
        let mut a2 = int8_arr_image(c2, s2, INT8_ARR_SIZE, 0);
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(a1.0.as_mut_ptr() as usize));
        f.args[1] = NullableDatum::value(Datum::from_usize(a2.0.as_mut_ptr() as usize));
        match fc_int4_avg_combine(None, &mut f) {
            Ok(_) => panic!("combine succeeded outside an aggregate"),
            Err(e) => core::mem::forget(e),
        }
    }
}

/// Native replay of the numeric_smaller/larger tie-plane identity
/// divergence found by the Kani harnesses (replay-divergences-natively
/// rule): value-equal inputs with different images (1.0 vs 1.00 — dscale
/// differs, so the returned image is SQL-visible via numeric_out).
/// C (REL_18 numeric.c): smaller = `cmp < 0 ? num1 : num2`, larger =
/// `cmp > 0 ? num1 : num2` — ties keep num2.  Adjudicated FIX-IN-PGRUST
/// (divergence #9); the shipped wrappers now match, and this replay is
/// the native regression witness.
#[cfg(test)]
mod tie_replay {
    use adt_numeric::builtins::{fc_numeric_larger, fc_numeric_smaller};
    use datum::Datum;
    use proof_support::fcinfo::call2;

    /// packed payload for 1.0  (long form: header POS|dscale=1, weight 0,
    /// one digit 1) prefixed by a 4B varlena header
    fn img(dscale: u16) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&::datum::varlena::set_varsize_4b(4 + 6));
        v.extend_from_slice(&(dscale).to_ne_bytes()); // POS(0x0000) | dscale
        v.extend_from_slice(&0i16.to_ne_bytes()); // weight 0
        v.extend_from_slice(&1i16.to_ne_bytes()); // digit "1"
        v
    }

    #[test]
    fn tie_identity_matches_c() {
        let a = img(1); // 1.0
        let b = img(2); // 1.00  — equal value, different image
        assert_ne!(a, b);
        let ra = call2(fc_numeric_smaller, a.as_ptr(), b.as_ptr()).unwrap();
        // C returns num2 (b, "1.00") on ties; the fixed wrapper matches.
        assert_eq!(ra.as_usize(), b.as_ptr() as usize, "tie keeps num2 as C");
        let rl = call2(fc_numeric_larger, a.as_ptr(), b.as_ptr()).unwrap();
        assert_eq!(rl.as_usize(), b.as_ptr() as usize, "tie keeps num2 as C");
    }
}
