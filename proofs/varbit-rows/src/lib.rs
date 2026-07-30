//! Kani C≡Rust equivalence: the varbit operator rows — biteq/bitne/bitlt/
//! bitle/bitgt/bitge/bitcmp (and their 1666-1672 varbit pg_proc aliases,
//! same fc_* functions), bit_and/bit_or/bitxor, bitnot, bitshiftleft/
//! bitshiftright.
//!
//! Rust side (shipped code, path-deps — never copied):
//!  - adt_varbit::bit_cmp_payload (already proved kernel, proofs/bytea-varbit;
//!    composed here with each operator's test exactly as the shipped
//!    fc_biteq..fc_bitcmp wrappers compose it via fc_bit_cmp_body)
//!  - adt_varbit::{bit_logic_verdict, bit_logic_body} (slice cores factored
//!    out of the Mcx-bound bit_logic for this proof — behavior identical,
//!    bit_logic calls them; the Mcx/PgVec path is harness-blocked per the
//!    measured hex-family trap)
//!  - adt_varbit::bitnot_body, adt_varbit::{bitshiftleft_body,
//!    bitshiftright_body} (same factoring; the shift bodies carry the
//!    negative-shift clamp + cross-dispatch that the cores recursed through)
//!  - adt_varbit::pad_last (already proved kernel, proofs/bytea-varbit) is
//!    inside bitnot_body/bitshiftright_body — its VARBIT_PAD_LAST parity is
//!    re-proved here composed, and result padding is ALSO asserted directly
//!    in-theorem (assert_padded).
//!
//! C side: proofs/varbit-rows/c/pg_varbit_rows.c (REL_18_STABLE varbit.c —
//! provenance + shims documented there).
//!
//! Claims and fences:
//!  - Inputs are fenced to VALID varbit values: bitlen >= 0,
//!    ceil(bitlen/8) == bytelen, and pad bits of the last byte zero — the
//!    invariant C asserts via VARBIT_CORRECTLY_PADDED / the Assert in
//!    VARBIT_PAD_LAST (vendored file, shim notes; varbit.c lines ~47-77).
//!    Payloads <= 8 bytes (bitlen <= 64), both lengths symbolic.
//!  - Comparators: verdict (bool) equality for biteq..bitge; SIGN equality
//!    for bitcmp — C returns raw memcmp magnitudes (implementation-defined
//!    by libc; CBMC's model is one witness), Rust returns -1/0/1. The
//!    user-visible int4 magnitude of bitcmp is a ratified non-surface: C
//!    itself is platform-dependent there. The proofs cover the value space
//!    below the fmgr wrappers (datum plumbing stays in the tested tier).
//!  - bit_and/bit_or/bitxor: split along the shipped verdict/body seam —
//!    eq_bit_logic_verdict proves verdict parity across the length-mismatch
//!    boundary for all three ops (C ereport -> -1 sentinel vs Rust Err;
//!    message text + F/L location outside the claim: size_mismatch_err is
//!    stubbed field-identically minus format!/#[track_caller], and the
//!    result is mem::forget-ed — Box<PgError> DROP GLUE alone cost ~50-85s,
//!    forget takes the same theorem to 0.8s); eq_bit_and/or/xor prove full
//!    byte value parity on the matched-length domain, with the
//!    VARBIT_PAD-equivalent last-byte mask asserted in-theorem.
//!  - bitnot: full byte parity incl. the final-byte mask (VARBIT_PAD_LAST),
//!    plus in-theorem result-padding assert (assert_padded).
//!  - shifts: shift amount fully symbolic i32 — covers 0, >= bitlen (C
//!    zero-fills; asserted equal), and negatives (cross-dispatch with the
//!    -VARBITMAXLEN clamp). Both sides dispatch acyclically into positive
//!    cores: the original left<->right recursion cycle made CBMC unwind
//!    both loop bodies ~10 deep (measured 81-103s; acyclic = 6.8s). C
//!    result buffers start as symbolic garbage, so the proofs also
//!    establish C fully writes its output on every path.
//!
//! Run with: timeout 30 cargo kani -Z c-ffi -Z stubbing \
//!   --c-lib c/pg_varbit_rows.c [--solver kissat] --harness <name>
//! Solver per harness (measured): kissat for the comparator/logic-value/
//! bitnot/cover harnesses; DEFAULT (CaDiCaL, incremental) for
//! eq_bit_logic_verdict, the shifts, and the negative control — kissat
//! re-solves per property batch and walls on the many-assert shift
//! harnesses.
//!
//! Negative control: control_bitnot_bitlen_off_by_one gives C a one-smaller
//! bitlen (different pad mask) and must FAIL. Verified failing 1.1s,
//! 2026-07-28; re-verified failing 1.05s, 2026-07-29 (relaunch).

#[cfg(kani)]
mod proofs {
    extern "C" {
        fn pg_biteq(b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32) -> i32;
        fn pg_bitne(b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32) -> i32;
        fn pg_bitlt(b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32) -> i32;
        fn pg_bitle(b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32) -> i32;
        fn pg_bitgt(b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32) -> i32;
        fn pg_bitge(b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32) -> i32;
        fn pg_bitcmp(b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32) -> i32;
        fn pg_bit_and(
            b1: *const u8, y1: i32, l1: i32,
            b2: *const u8, y2: i32, l2: i32,
            r: *mut u8,
        ) -> i32;
        fn pg_bit_or(
            b1: *const u8, y1: i32, l1: i32,
            b2: *const u8, y2: i32, l2: i32,
            r: *mut u8,
        ) -> i32;
        fn pg_bitxor(
            b1: *const u8, y1: i32, l1: i32,
            b2: *const u8, y2: i32, l2: i32,
            r: *mut u8,
        ) -> i32;
        fn pg_bitnot(b: *const u8, y: i32, l: i32, r: *mut u8) -> i32;
        fn pg_bitshiftleft(b: *const u8, y: i32, l: i32, shft: i32, r: *mut u8) -> i32;
        fn pg_bitshiftright(b: *const u8, y: i32, l: i32, shft: i32, r: *mut u8) -> i32;
    }

    /// Payload byte cap (bitlen <= 64).
    const N: usize = 8;

    /// A valid varbit: symbolic bytes + symbolic bitlen fenced to the C
    /// invariant ceil(bitlen/8) == bytelen with zero pad bits
    /// (VARBIT_CORRECTLY_PADDED / the Assert in VARBIT_PAD_LAST — see the
    /// vendored C header comment).
    fn sym_varbit() -> ([u8; N], usize, usize) {
        let bytes: [u8; N] = kani::any();
        let bitlen: usize = kani::any();
        kani::assume(bitlen <= N * 8);
        let bytelen = bitlen.div_ceil(8);
        let pad = bytelen * 8 - bitlen;
        if pad > 0 {
            kani::assume(bytes[bytelen - 1] & !(0xFFu8 << pad) == 0);
        }
        (bytes, bytelen, bitlen)
    }

    /// Rust-side varbit payload image: [bitlen i32 ne][bytes..bytelen].
    fn payload(bytes: &[u8; N], bytelen: usize, bitlen: usize) -> [u8; 4 + N] {
        let mut p = [0u8; 4 + N];
        p[..4].copy_from_slice(&(bitlen as i32).to_ne_bytes());
        let mut i = 0usize;
        while i < bytelen {
            p[4 + i] = bytes[i];
            i += 1;
        }
        p
    }

    /// Assert the result body is correctly padded (VARBIT_PAD equivalent,
    /// in-theorem last-byte mask check).
    fn assert_padded(r: &[u8], bytelen: usize, bitlen: usize) {
        let pad = bytelen * 8 - bitlen;
        if pad > 0 {
            assert!(r[bytelen - 1] & !(0xFFu8 << pad) == 0);
        }
    }

    // ---------------- comparators (biteq..bitge, bitcmp) ----------------
    // Rust side composes the proved bit_cmp_payload kernel with each
    // operator's test exactly as the shipped fc_* wrappers do
    // (fc_bit_cmp_body + the bit_cmp_fns! macro ops).

    macro_rules! cmp_harness {
        ($name:ident, $cfn:ident, $op:tt) => {
            #[kani::proof]
            #[kani::unwind(10)]
            fn $name() {
                let (ab, ay, al) = sym_varbit();
                let (bb, by, bl) = sym_varbit();
                kani::cover!(al != bl); // different-length fast path reachable
                kani::cover!(al == bl && al > 0); // compared-content regime
                let pa = payload(&ab, ay, al);
                let pb = payload(&bb, by, bl);
                let r = adt_varbit::bit_cmp_payload(&pa[..4 + ay], &pb[..4 + by]);
                let c = unsafe {
                    $cfn(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(), by as i32, bl as i32)
                };
                assert!((c != 0) == (r $op 0));
            }
        };
    }

    cmp_harness!(eq_biteq, pg_biteq, ==);
    cmp_harness!(eq_bitne, pg_bitne, !=);
    cmp_harness!(eq_bitlt, pg_bitlt, <);
    cmp_harness!(eq_bitle, pg_bitle, <=);
    cmp_harness!(eq_bitgt, pg_bitgt, >);
    cmp_harness!(eq_bitge, pg_bitge, >=);

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bitcmp() {
        let (ab, ay, al) = sym_varbit();
        let (bb, by, bl) = sym_varbit();
        let pa = payload(&ab, ay, al);
        let pb = payload(&bb, by, bl);
        let r = adt_varbit::bit_cmp_payload(&pa[..4 + ay], &pb[..4 + by]);
        let c = unsafe {
            pg_bitcmp(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(), by as i32, bl as i32)
        };
        // Sign equality: C exposes raw memcmp magnitudes (libc-defined),
        // Rust -1/0/1 — the magnitude is a ratified non-surface.
        assert!((c < 0) == (r < 0));
        assert!((c == 0) == (r == 0));
        assert!((c > 0) == (r > 0));
    }

    // ---------------- bit_and / bit_or / bitxor ----------------

    // Stub for PgError::new on the mismatch error path: identical value
    // construction minus the #[track_caller] Location::caller() capture
    // (Kani unsupported construct, measured trap). Only the error F/L
    // location field leaves the claim; level and sqlstate stay in (the
    // harness only checks the verdict here).
    fn stub_pg_error_new(
        level: types_error::ErrorLevel,
        message: impl Into<String>,
    ) -> types_error::PgError {
        types_error::PgError {
            level,
            sqlstate: types_error::default_sqlstate_for_level(level),
            message: message.into(),
            message_raw: None,
            detail: None,
            detail_log: None,
            hint: None,
            context: None,
            backtrace: None,
            message_id: None,
            domain: None,
            context_domain: None,
            hide_statement: false,
            hide_context: false,
            location: None,
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            plpgsql_context_attached: false,
        }
    }

    // Stub for the mismatch-error constructor: field-identical (level ERROR,
    // sqlstate ERRCODE_STRING_DATA_LENGTH_MISMATCH) minus the format!-built
    // message text and the #[track_caller] location — std fmt machinery
    // walls symex (measured trap; the fmt::write recursion hung unwinding).
    // Message TEXT and F/L location leave the claim; the harness asserts the
    // error VERDICT only.
    fn stub_size_mismatch_err(opname: &'static str) -> types_error::PgError {
        let _ = opname;
        // String::new() allocates nothing — keeps alloc machinery out of the
        // formula. Text is outside the claim either way.
        let mut e = stub_pg_error_new(types_error::ERROR, String::new());
        e.sqlstate = types_error::ERRCODE_STRING_DATA_LENGTH_MISMATCH;
        e
    }

    // The shipped bit_logic = bit_logic_verdict THEN bit_logic_body; the
    // proof splits along that exact seam (a monolithic harness was 71-85s —
    // over budget; the PgError/Box machinery in the same formula as the
    // byte-compare is what costs). The op argument does not enter
    // bit_logic_verdict, and the C mismatch check is textually identical in
    // bit_and/bit_or/bitxor — the verdict harness runs all three C fns.

    /// Verdict parity across the length-mismatch boundary, all three ops
    /// (C ereport -> -1 sentinel vs Rust Err; both lengths fully symbolic).
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(adt_varbit::size_mismatch_err, stub_size_mismatch_err)]
    fn eq_bit_logic_verdict() {
        let (ab, ay, al) = sym_varbit();
        let (bb, by, bl) = sym_varbit();
        let mut cr: [u8; N] = kani::any();
        let c_and = unsafe {
            pg_bit_and(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(), by as i32, bl as i32,
                       cr.as_mut_ptr())
        };
        let c_or = unsafe {
            pg_bit_or(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(), by as i32, bl as i32,
                      cr.as_mut_ptr())
        };
        let c_xor = unsafe {
            pg_bitxor(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(), by as i32, bl as i32,
                      cr.as_mut_ptr())
        };
        let pa = payload(&ab, ay, al);
        let pb = payload(&bb, by, bl);
        let r = adt_varbit::bit_logic_verdict(&pa[..4 + ay], &pb[..4 + by], "AND");
        match &r {
            Err(_) => {
                assert!(c_and == -1);
                assert!(c_or == -1);
                assert!(c_xor == -1);
            }
            Ok(bitlen1) => {
                assert!(c_and == 0);
                assert!(c_or == 0);
                assert!(c_xor == 0);
                assert!(*bitlen1 == al);
            }
        }
        core::mem::forget(r); // keep Box<PgError> drop glue out of the formula
    }

    macro_rules! logic_value_harness {
        ($name:ident, $cfn:ident, $op:expr) => {
            /// Value parity on the matched-length domain (the Ok arm of
            /// bit_logic_verdict, proved above). C result buffer starts as
            /// symbolic garbage: proves C fully writes its output.
            #[kani::proof]
            #[kani::unwind(10)]
            fn $name() {
                let (ab, ay, al) = sym_varbit();
                let (bb, by, bl) = sym_varbit();
                kani::assume(al == bl);
                let mut cr: [u8; N] = kani::any();
                let c = unsafe {
                    $cfn(
                        ab.as_ptr(), ay as i32, al as i32,
                        bb.as_ptr(), by as i32, bl as i32,
                        cr.as_mut_ptr(),
                    )
                };
                assert!(c == 0);
                let mut rr = [0u8; N];
                adt_varbit::bit_logic_body(&mut rr[..ay], &ab[..ay], &bb[..by], $op);
                let mut i = 0usize;
                while i < ay {
                    assert!(rr[i] == cr[i]);
                    i += 1;
                }
                // VARBIT_PAD equivalent asserted in-theorem.
                assert_padded(&rr, ay, al);
            }
        };
    }

    logic_value_harness!(eq_bit_and, pg_bit_and, |a: u8, b: u8| a & b);
    logic_value_harness!(eq_bit_or, pg_bit_or, |a: u8, b: u8| a | b);
    logic_value_harness!(eq_bitxor, pg_bitxor, |a: u8, b: u8| a ^ b);

    // ---------------- bitnot ----------------

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bitnot() {
        let (ab, ay, al) = sym_varbit();
        kani::cover!(al % 8 != 0 && al > 0); // final-byte mask actually fires
        let mut cr: [u8; N] = kani::any();
        unsafe { pg_bitnot(ab.as_ptr(), ay as i32, al as i32, cr.as_mut_ptr()) };
        let mut rr = [0u8; N];
        adt_varbit::bitnot_body(&mut rr[..ay], &ab[..ay], al);
        let mut i = 0usize;
        while i < ay {
            assert!(rr[i] == cr[i]);
            i += 1;
        }
        assert_padded(&rr, ay, al);
    }

    // ---------------- bitshiftleft / bitshiftright ----------------

    macro_rules! shift_harness {
        ($name:ident, $cfn:ident, $rfn:path) => {
            #[kani::proof]
            #[kani::unwind(10)]
            fn $name() {
                let (ab, ay, al) = sym_varbit();
                let shft: i32 = kani::any(); // fully symbolic, incl. i32::MIN
                // Regime reachability witnessed by cover_shift_regimes —
                // inline covers cost one SAT call each (4 covers pushed this
                // harness to ~100s).
                let mut cr: [u8; N] = kani::any();
                unsafe { $cfn(ab.as_ptr(), ay as i32, al as i32, shft, cr.as_mut_ptr()) };
                let mut rr = [0u8; N];
                $rfn(&mut rr[..ay], &ab[..ay], al, shft);
                let mut i = 0usize;
                while i < ay {
                    assert!(rr[i] == cr[i]);
                    i += 1;
                }
                assert_padded(&rr, ay, al);
            }
        };
    }

    shift_harness!(eq_bitshiftleft, pg_bitshiftleft, adt_varbit::bitshiftleft_body);
    shift_harness!(eq_bitshiftright, pg_bitshiftright, adt_varbit::bitshiftright_body);

    // ---------------- input-regime coverage (vacuity insurance) ----------------
    // The eq_bit_and/or/xor harnesses dropped their inline kani::cover!s
    // (one SAT call each under the property-batch cost law); this shared
    // harness witnesses that the SAME input distribution (sym_varbit x2)
    // reaches both the mismatch-verdict arm and a nonempty value arm.

    #[kani::proof]
    #[kani::unwind(10)]
    fn cover_input_regimes() {
        let (_ab, _ay, al) = sym_varbit();
        let (_bb, _by, bl) = sym_varbit();
        kani::cover!(al != bl); // mismatch verdict arm reachable
        kani::cover!(al == bl && al > 0); // value arm reachable
    }

    // Same for the shift harnesses: identical input distribution
    // (sym_varbit + fully-symbolic i32 shift), witnessing every C branch
    // regime the eq_bitshift* theorems quantify over.
    #[kani::proof]
    #[kani::unwind(10)]
    fn cover_shift_regimes() {
        let (_ab, _ay, al) = sym_varbit();
        let shft: i32 = kani::any();
        kani::cover!(shft == 0);
        kani::cover!(shft > 0 && (shft as usize) >= al); // zero-fill arm
        kani::cover!(shft > 0 && (shft as usize) < al && shft % 8 == 0); // memcpy arm
        kani::cover!(shft > 0 && (shft as usize) < al && shft % 8 != 0); // bit-carry arm
        kani::cover!(shft < 0 && al > 0); // negative cross-dispatch arm
        kani::cover!(shft == i32::MIN); // clamp arm
    }

    // ------- bitlength/bitoctetlength (1681/1682) + bittoint4/8 (1684/2076) -------
    // Valid-varbit fence (sym_varbit), len<=N sym. The length rows prove
    // the shipped payload decode (payload_bitlen / payload_bits.len) equals
    // C's VARBITLEN/VARBITBYTES accessors over the same image; the int
    // rows prove the full shift-accumulate loop + pad shift + overflow
    // verdict (error VALUE arm only reachable at bitlen>32/64, outside the
    // N=8-byte cap for int8 — covered for int4).

    extern "C" {
        fn pg_bitlength(bitlen: i32) -> i32;
        fn pg_bitoctetlength(bytelen: i32) -> i32;
        fn pg_bittoint4(bits: *const u8, bytelen: i32, bitlen: i32, err: *mut i32) -> i32;
        fn pg_bittoint8(bits: *const u8, bytelen: i32, bitlen: i32, err: *mut i32) -> i64;
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bitlength_octetlength() {
        let (ab, ay, al) = sym_varbit();
        let p = payload(&ab, ay, al);
        let cl = unsafe { pg_bitlength(al as i32) };
        let cb = unsafe { pg_bitoctetlength(ay as i32) };
        assert!(adt_varbit::payload_bitlen(&p[..4 + ay]) as i32 == cl);
        assert!(adt_varbit::payload_bits(&p[..4 + ay]).len() as i32 == cb);
    }

    /// Stub for the out-of-range constructor (same contract as
    /// stub_size_mismatch_err below: field-identical minus format!-message
    /// and location; sqlstate 22003 stays in).
    fn stub_out_of_range_err(what: &'static str) -> types_error::PgError {
        let _ = what;
        let mut e = stub_pg_error_new(types_error::ERROR, String::new());
        e.sqlstate = types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
        e
    }

    macro_rules! bittoint_harness {
        ($h:ident, $core:ident, $cfn:ident, $ty:ty, $maxbits:literal) => {
            #[kani::proof]
            #[kani::unwind(10)]
            #[kani::stub(adt_varbit::out_of_range_err, stub_out_of_range_err)]
            fn $h() {
                let (ab, ay, al) = sym_varbit();
                let p = payload(&ab, ay, al);
                let mut cerr: i32 = -1;
                let c = unsafe { $cfn(ab.as_ptr(), ay as i32, al as i32, &mut cerr) };
                match adt_varbit::$core(&p[..4 + ay]) {
                    Ok(v) => assert!(cerr == 0 && v == c),
                    Err(e) => {
                        let ok = cerr == 1;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
            }
        };
    }

    bittoint_harness!(eq_bittoint4, bittoint4_core, pg_bittoint4, i32, 32);
    bittoint_harness!(eq_bittoint8, bittoint8_core, pg_bittoint8, i64, 64);

    // ---------------- negative control (must FAIL) ----------------

    #[kani::proof]
    #[kani::unwind(10)]
    fn control_bitnot_bitlen_off_by_one() {
        let (ab, ay, al) = sym_varbit();
        kani::assume(al >= 1 && al.div_ceil(8) == (al - 1).div_ceil(8));
        // C gets a one-smaller bitlen -> different final-byte pad mask.
        let mut cr: [u8; N] = kani::any();
        unsafe { pg_bitnot(ab.as_ptr(), ay as i32, (al - 1) as i32, cr.as_mut_ptr()) };
        let mut rr = [0u8; N];
        adt_varbit::bitnot_body(&mut rr[..ay], &ab[..ay], al);
        let mut i = 0usize;
        while i < ay {
            assert!(rr[i] == cr[i]);
            i += 1;
        }
    }
}

// ======================================================================
// WAVE-10 (2026-07-28/29): remaining varbit rows — I/O, recv/send, casts,
// concat/substring/overlay, get/set bit, position, bit_count, typmodin,
// length coercions. C side: the WAVE-10 section of c/pg_varbit_rows.c
// (REL_18_STABLE varbit.c; err sentinel map documented there).
//
// Claims and fences (per harness, summarized):
//  - result images compared byte-for-byte over the written extent; varlena
//    headers are the same two integers both sides (carried explicitly) and
//    are not part of the byte compare.
//  - Mcx-bound cores run under the proof_support mcx-stubs recipe
//    (allocate/grow/deallocate -> static bump, env::var -> "0", OnceLock ->
//    recompute, fmt stubbed, teardown forgotten): theorems are "modulo
//    static-buffer allocator model"; allocation strategy out of scope.
//  - Error arms: PgError::error stubbed (proof_support contract) — message
//    text + F/L location out of proof; LEVEL and SQLSTATE parity asserted
//    per the C sentinel map.
//
// STRUCTURAL LAW (measured across this whole wave, 6 independent shapes):
// any harness whose formula contains an allocation/copy/loop whose LENGTH
// is symbolic — even when fenced, even when "pinned" by an assumed
// equality, even when the branch computing it is fenced out — OOMs CBMC in
// the propositional reduction. Assumed ranges/equalities NEVER
// constant-fold (interval-cmp lesson generalized to alloc sizes, message
// lengths, prefix-branch phis, and dead branches). The working shape is
// per-cell LITERAL scalars (lengths, typmods, positions, wire headers,
// first bytes that select a parse form) with CONTENT fully symbolic, plus
// literal-scalar cells for each error/identity plane. Scalar-verdict
// harnesses with no allocation (getbit, typmodin, bit_count, position
// early-returns) prove with everything symbolic.
//
// Documented WALLS (kept as wall_* non-proof fns): the fully-symbolic
// verdict harnesses for coerce/substr/overlay/bitposition, and the
// bitoverlay IMAGE composition (2 substrings + 2 catenates in one formula;
// symex OOM even at 8/9-bit fully concrete-scalar cells — its kernels
// bitsubstring/bit_catenate and both error planes are proved separately).
//
// RELAUNCH RE-DERIVATION (2026-07-29, all verdicts re-run serially on this
// box, 6GiB full-tree watchdog): every #[kani::proof] harness in both
// modules GREEN and all three negative controls FAILED on the intended
// parity assertion, EXCEPT five memory walls, each confirmed on BOTH
// solvers (default CaDiCaL + kissat; CBMC self-abort "run out of memory"
// at the ~6GiB cap):
//   - eq_bitoverlay_c_mid / _c_tail / _no_len_c — the <=9-bit resize did
//     NOT clear the composed 2-substring+2-catenate image wall;
//   - eq_bit_recv_c12 / eq_varbit_recv_c12 — the recv Ok-IMAGE arm
//     (StringInfo read + image build in one formula); the protocol/short/
//     invalid-length/typmod-mismatch recv planes all proved.
// Memory walls are CAP-RELATIVE: these five stay as live #[kani::proof]
// harnesses for the fleet 40GB retry tier (uuid[] precedent).
#[cfg(kani)]
mod proofs_w10 {
    use proof_support::{mcx_stubs, stubs};
    use types_error::{
        ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_BINARY_REPRESENTATION,
        ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        ERRCODE_PROTOCOL_VIOLATION, ERRCODE_STRING_DATA_LENGTH_MISMATCH,
        ERRCODE_STRING_DATA_RIGHT_TRUNCATION, ERRCODE_SUBSTRING_ERROR, ERROR,
    };

    extern "C" {
        fn pg_anybit_typmodin(tl: *const i32, n: i32, err: *mut i32) -> i32;
        fn pg_bit_in(
            s: *const u8, atttypmod: i32, rbuf: *mut u8, rbitlen: *mut i32, err: *mut i32,
        ) -> i32;
        fn pg_varbit_in(
            s: *const u8, atttypmod: i32, rbuf: *mut u8, rbitlen: *mut i32, err: *mut i32,
        ) -> i32;
        fn pg_varbit_out(bits: *const u8, bytelen: i32, bitlen: i32, result: *mut u8) -> i32;
        fn pg_bit_recv(
            data: *const u8, len_msg: i32, cursor: *mut i32, atttypmod: i32,
            rbuf: *mut u8, rbitlen: *mut i32, err: *mut i32,
        ) -> i32;
        fn pg_varbit_recv(
            data: *const u8, len_msg: i32, cursor: *mut i32, atttypmod: i32,
            rbuf: *mut u8, rbitlen: *mut i32, err: *mut i32,
        ) -> i32;
        fn pg_varbit_send(bits: *const u8, bytelen: i32, bitlen: i32, out: *mut u8) -> i32;
        fn pg_bit_coerce(
            bits: *const u8, bytelen: i32, bitlen: i32, len: i32, is_explicit: i32,
            r: *mut u8, err: *mut i32,
        ) -> i32;
        fn pg_varbit_coerce(
            bits: *const u8, bytelen: i32, bitlen: i32, len: i32, is_explicit: i32,
            r: *mut u8, err: *mut i32,
        ) -> i32;
        fn pg_bit_catenate(
            b1: *const u8, y1: i32, l1: i32, b2: *const u8, y2: i32, l2: i32,
            r: *mut u8, err: *mut i32,
        ) -> i32;
        fn pg_bitsubstring(
            bits: *const u8, bytelen: i32, bitlen: i32, s: i32, l: i32,
            length_not_specified: i32, r: *mut u8, rbitlen: *mut i32, err: *mut i32,
        ) -> i32;
        fn pg_bit_overlay(
            t1: *const u8, y1: i32, l1: i32, t2: *const u8, y2: i32, l2: i32,
            sp: i32, sl: i32, r: *mut u8, rbitlen: *mut i32, err: *mut i32,
        ) -> i32;
        fn pg_bit_bit_count(bits: *const u8, bytelen: i32) -> i64;
        fn pg_bitfromint4(a: i32, typmod: i32, r: *mut u8) -> i32;
        fn pg_bitfromint8(a: i64, typmod: i32, r: *mut u8) -> i32;
        fn pg_bitsetbit(
            bits: *const u8, bytelen: i32, bitlen: i32, n: i32, new_bit: i32,
            r: *mut u8, err: *mut i32,
        ) -> i32;
        fn pg_bitgetbit(bits: *const u8, bytelen: i32, bitlen: i32, n: i32, err: *mut i32)
            -> i32;
        fn pg_bitposition(
            sb: *const u8, sy: i32, sl: i32, pb: *const u8, py: i32, pl: i32,
        ) -> i32;
    }

    /// Payload byte cap (bitlen <= 64), as in the module above.
    const N: usize = 8;
    /// varbit.h VARBITMAXLEN as i32.
    const VARBITMAXLEN_I32: i32 = i32::MAX - 8 + 1;
    /// [varsize][bitlen] header bytes of a full varbit image.
    const HDR: usize = 8;

    fn sym_varbit() -> ([u8; N], usize, usize) {
        let bytes: [u8; N] = kani::any();
        let bitlen: usize = kani::any();
        kani::assume(bitlen <= N * 8);
        let bytelen = bitlen.div_ceil(8);
        let pad = bytelen * 8 - bitlen;
        if pad > 0 {
            kani::assume(bytes[bytelen - 1] & !(0xFFu8 << pad) == 0);
        }
        (bytes, bytelen, bitlen)
    }

    /// Rust-side varbit payload image: [bitlen i32 ne][bytes..bytelen].
    fn payload(bytes: &[u8; N], bytelen: usize, bitlen: usize) -> [u8; 4 + N] {
        let mut p = [0u8; 4 + N];
        p[..4].copy_from_slice(&(bitlen as i32).to_ne_bytes());
        let mut i = 0usize;
        while i < bytelen {
            p[4 + i] = bytes[i];
            i += 1;
        }
        p
    }

    /// Smaller payload cap for the Mcx-image harnesses (bitlen <= 32):
    /// symbolic-length image stores at the N=8 cap OOM CBMC in the
    /// propositional reduction (measured pass 1, whole family) — the
    /// heap-array width is the cost driver, not the logic.
    const MB: usize = 4;

    fn sym_varbit_m() -> ([u8; MB], usize, usize) {
        let bytes: [u8; MB] = kani::any();
        let bitlen: usize = kani::any();
        kani::assume(bitlen <= MB * 8);
        let bytelen = bitlen.div_ceil(8);
        let pad = bytelen * 8 - bitlen;
        if pad > 0 {
            kani::assume(bytes[bytelen - 1] & !(0xFFu8 << pad) == 0);
        }
        (bytes, bytelen, bitlen)
    }

    fn payload_m(bytes: &[u8; MB], bytelen: usize, bitlen: usize) -> [u8; 4 + MB] {
        let mut p = [0u8; 4 + MB];
        p[..4].copy_from_slice(&(bitlen as i32).to_ne_bytes());
        let mut i = 0usize;
        while i < bytelen {
            p[4 + i] = bytes[i];
            i += 1;
        }
        p
    }

    /// Compare a full shipped image ([varsize][bitlen][bytes]) against the
    /// C result: bitlen + the written payload bytes.
    fn assert_image(img: &[u8], want_bitlen: usize, cbytes: &[u8]) {
        assert!(adt_varbit::payload_bitlen(&img[4..]) == want_bitlen);
        let by = want_bitlen.div_ceil(8);
        let mut i = 0usize;
        while i < by {
            assert!(img[HDR + i] == cbytes[i]);
            i += 1;
        }
    }

    // ---------------- bitgetbit (3032) ----------------

    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bitgetbit() {
        let (ab, ay, al) = sym_varbit();
        let n: i32 = kani::any(); // full i32, both arms
        let p = payload(&ab, ay, al);
        let mut cerr: i32 = -1;
        let c = unsafe { pg_bitgetbit(ab.as_ptr(), ay as i32, al as i32, n, &mut cerr) };
        match adt_varbit::bitgetbit_core(&p[..4 + ay], n) {
            Ok(v) => {
                assert!(cerr == 0);
                assert!(v == c);
            }
            Err(e) => {
                let ok = cerr == 8
                    && e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR
                    && e.level == ERROR;
                core::mem::forget(e);
                assert!(ok);
            }
        }
    }

    // ---------------- bitsetbit (3033) ----------------
    // Fully-symbolic bitlen OOMs CBMC (pass 1 + MB=4 probe, measured):
    // concrete-bitlen cells, everything else (content, n, new_bit) fully
    // symbolic. Cells 12 (pad > 0) and 16 (pad == 0) cover both final-byte
    // regimes; both error arms live in each cell (n fully symbolic).

    /// Symbolic content at a CONCRETE bitlen: pad bits zeroed, tail zeroed.
    fn sym_varbit_c(bitlen: usize) -> [u8; N] {
        let mut bytes: [u8; N] = kani::any();
        let bytelen = bitlen.div_ceil(8);
        let pad = bytelen * 8 - bitlen;
        if pad > 0 {
            kani::assume(bytes[bytelen - 1] & !(0xFFu8 << pad) == 0);
        }
        let mut i = bytelen;
        while i < N {
            bytes[i] = 0;
            i += 1;
        }
        bytes
    }

    /// Rust payload image at a concrete bitlen (N-cap buffer).
    fn payload_c(bytes: &[u8; N], bitlen: usize) -> [u8; 4 + N] {
        let mut p = [0u8; 4 + N];
        p[..4].copy_from_slice(&(bitlen as i32).to_ne_bytes());
        let mut i = 0usize;
        while i < bitlen.div_ceil(8) {
            p[4 + i] = bytes[i];
            i += 1;
        }
        p
    }

    macro_rules! setbit_cell {
        ($($h:ident: bitlen=$bl:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let al: usize = $bl;
                let ay = al.div_ceil(8);
                let ab = sym_varbit_c(al);
                let n: i32 = kani::any();
                let nb: i32 = kani::any();
                let p = payload_c(&ab, al);
                let mut cr: [u8; N] = kani::any(); // C fully writes on the Ok arm
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    pg_bitsetbit(ab.as_ptr(), ay as i32, al as i32, n, nb, cr.as_mut_ptr(),
                                 &mut cerr)
                };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::bitsetbit_core(ctx.mcx(), &p[..4 + ay], n, nb) {
                    Ok(img) => {
                        assert!(cst == 0 && cerr == 0);
                        assert_image(&img, al, &cr);
                        core::mem::forget(img);
                    }
                    Err(e) => {
                        let ok = cst == -1
                            && ((cerr == 8 && e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                                || (cerr == 9 && e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE))
                            && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    setbit_cell! {
        eq_bitsetbit_l16: bitlen=16;
    }

    /// Concrete-bitlen probe cell (bitlen=12, bytelen=2, pad=4): content,
    /// n and new_bit fully symbolic.
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bitsetbit_l12() {
        let mut ab: [u8; MB] = kani::any();
        let (ay, al) = (2usize, 12usize);
        kani::assume(ab[1] & 0x0F == 0); // pad bits zero
        ab[2] = 0;
        ab[3] = 0;
        let n: i32 = kani::any();
        let nb: i32 = kani::any();
        let p = payload_m(&ab, ay, al);
        let mut cr: [u8; MB] = kani::any();
        let mut cerr: i32 = -1;
        let cst = unsafe {
            pg_bitsetbit(ab.as_ptr(), ay as i32, al as i32, n, nb, cr.as_mut_ptr(), &mut cerr)
        };
        let ctx = mcx::MemoryContext::new_bump("kani-varbit");
        match adt_varbit::bitsetbit_core(ctx.mcx(), &p[..4 + ay], n, nb) {
            Ok(img) => {
                assert!(cst == 0 && cerr == 0);
                assert_image(&img, al, &cr);
                core::mem::forget(img);
            }
            Err(e) => {
                let ok = cst == -1
                    && ((cerr == 8 && e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                        || (cerr == 9 && e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE))
                    && e.level == ERROR;
                core::mem::forget(e);
                assert!(ok);
            }
        }
        core::mem::forget(ctx);
    }

    // ---------------- bit_bit_count (6162): shipped fc wrapper ----------------
    // The counting loop lives in fc_bit_bit_count itself; the harness builds
    // a real 4B-header varlena image and calls the SHIPPED wrapper through a
    // LocalFcinfo frame (datum unwrap inside the theorem).

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_bit_bit_count() {
        let (ab, ay, al) = sym_varbit();
        let mut arg = [0u8; HDR + N];
        arg[..4].copy_from_slice(&datum::varlena::set_varsize_4b(HDR + ay));
        arg[4..8].copy_from_slice(&(al as i32).to_ne_bytes());
        let mut i = 0usize;
        while i < ay {
            arg[HDR + i] = ab[i];
            i += 1;
        }
        let c = unsafe { pg_bit_bit_count(ab.as_ptr(), ay as i32) };
        let d = proof_support::call1_ok(adt_varbit::fc_bit_bit_count, arg.as_ptr());
        assert!(d.as_i64() == c);
    }

    // ---------------- bitfromint4 (1683) / bitfromint8 (2075) ----------------
    // FULLY SYMBOLIC typmod OOMs CBMC in the propositional reduction (pass 1,
    // measured): a symbolic typmod makes every image length/store offset
    // symbolic. Cells: `a` fully symbolic everywhere; typmod per-cell
    // concrete at the loop-structure boundaries {1, 33, 47, 64} (int4:
    // 33/47 exercise the sign-fill + first-fractional arms, 47 the pad arm,
    // 64 the max image) plus the two clamp PLANES with typmod symbolic
    // (<= 0 and > VARBITMAXLEN both clamp to bitlen 1 — concrete image).
    // int8 under the 8-byte cap never reaches its sign-fill arm (needs
    // typmod >= 72) — recorded in the ledger bounds.

    macro_rules! fromint_cell {
        ($($h:ident: $core:ident / $cfn:ident ($ty:ty) typmod=$tm:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let a: $ty = kani::any();
                let typmod: i32 = $tm;
                let mut cr: [u8; N] = kani::any(); // C fully writes ceil(typmod'/8)
                let cbitlen = unsafe { $cfn(a, typmod, cr.as_mut_ptr()) };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::$core(ctx.mcx(), a, typmod) {
                    Ok(img) => {
                        assert_image(&img, cbitlen as usize, &cr);
                        core::mem::forget(img);
                    }
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("fromint errored under fence");
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    // Clamp planes as CONCRETE boundary cells (a symbolic typmod under an
    // assumed range still leaves the symbolic-length else-branch in the
    // formula — assumed ranges don't constant-fold, measured pass 2).
    fromint_cell! {
        eq_bitfromint4_t1:  bitfromint4_core / pg_bitfromint4 (i32) typmod=1;
        eq_bitfromint4_t33: bitfromint4_core / pg_bitfromint4 (i32) typmod=33;
        eq_bitfromint4_t47: bitfromint4_core / pg_bitfromint4 (i32) typmod=47;
        eq_bitfromint4_t64: bitfromint4_core / pg_bitfromint4 (i32) typmod=64;
        eq_bitfromint4_t0:  bitfromint4_core / pg_bitfromint4 (i32) typmod=0;
        eq_bitfromint4_tmin: bitfromint4_core / pg_bitfromint4 (i32) typmod=i32::MIN;
        eq_bitfromint4_tmax: bitfromint4_core / pg_bitfromint4 (i32) typmod=i32::MAX;
        eq_bitfromint8_t1:  bitfromint8_core / pg_bitfromint8 (i64) typmod=1;
        eq_bitfromint8_t47: bitfromint8_core / pg_bitfromint8 (i64) typmod=47;
        eq_bitfromint8_t64: bitfromint8_core / pg_bitfromint8 (i64) typmod=64;
        eq_bitfromint8_t0:  bitfromint8_core / pg_bitfromint8 (i64) typmod=0;
        eq_bitfromint8_tmax: bitfromint8_core / pg_bitfromint8 (i64) typmod=i32::MAX;
    }

    // -------- bit()/varbit() length coercions (1685 / 1687) --------
    // Split (symbolic len OOMs on the image arm, pass 1 measured):
    //  - *_verdict: len + everything FULLY symbolic, fenced to the two
    //    non-allocating arms (identity None + implicit-cast Err) — the
    //    typmod checks and error verdict/sqlstate over the whole plane;
    //  - *_l{9,16} image cells: concrete len (pad > 0 / pad == 0 regimes),
    //    input image + is_explicit fully symbolic.

    // WALL — kept for documentation, NOT a proof (wall_ prefix convention):
    // even with the image arm fenced out, its symbolic-length allocation
    // machinery stays in the formula and OOMs CBMC (measured pass 2 under
    // load AND pass 3 on a quiet box). The same planes are proved by the
    // concrete-len coerce_plane_cell harnesses below.
    macro_rules! coerce_verdict {
        ($h:ident, $core:ident, $cfn:ident, $errcode:expr, $identity:expr) => {
            #[allow(dead_code)]
            fn $h() {
                let (ab, ay, al) = sym_varbit();
                let len: i32 = kani::any();
                let expl: bool = kani::any();
                // Fence OUT the allocating Some-image arm only.
                let identity: fn(i32, usize) -> bool = $identity;
                kani::assume(identity(len, al) || !expl);
                let p = payload(&ab, ay, al);
                let mut cr: [u8; N] = [0; N];
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    $cfn(ab.as_ptr(), ay as i32, al as i32, len, expl as i32,
                         cr.as_mut_ptr(), &mut cerr)
                };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::$core(ctx.mcx(), &p[..4 + ay], len, expl) {
                    Ok(None) => assert!(cst == 0),
                    Ok(Some(img)) => {
                        core::mem::forget(img);
                        panic!("image arm inside verdict fence")
                    }
                    Err(e) => {
                        let ok = cst == -1 && cerr == 2 && e.sqlstate == $errcode
                            && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
                core::mem::forget(ctx);
            }
        };
    }

    coerce_verdict!(
        eq_bit_coerce_verdict,
        bit_coerce,
        pg_bit_coerce,
        ERRCODE_STRING_DATA_LENGTH_MISMATCH,
        |len, al| len <= 0 || len > VARBITMAXLEN_I32 || len as usize == al
    );
    coerce_verdict!(
        eq_varbit_coerce_verdict,
        varbit_coerce,
        pg_varbit_coerce,
        ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
        |len, al| len <= 0 || len as usize >= al
    );

    macro_rules! coerce_image_cell {
        ($($h:ident: $core:ident / $cfn:ident len=$len:literal varbit=$vb:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (ab, ay, al) = sym_varbit();
                let len: i32 = $len;
                if $vb {
                    kani::assume((len as usize) < al); // varbit(): image arm needs len < bitlen
                } else {
                    kani::assume(len as usize != al); // bit(): image arm needs len != bitlen
                }
                let p = payload(&ab, ay, al);
                let mut cr: [u8; N] = kani::any();
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    $cfn(ab.as_ptr(), ay as i32, al as i32, len, 1, cr.as_mut_ptr(),
                         &mut cerr)
                };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::$core(ctx.mcx(), &p[..4 + ay], len, true) {
                    Ok(Some(img)) => {
                        assert!(cst == 1);
                        assert_image(&img, len as usize, &cr);
                        core::mem::forget(img);
                    }
                    Ok(None) => panic!("identity arm inside image fence"),
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("error arm with is_explicit");
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    coerce_image_cell! {
        eq_bit_coerce_l9:     bit_coerce / pg_bit_coerce len=9 varbit=false;
        eq_bit_coerce_l16:    bit_coerce / pg_bit_coerce len=16 varbit=false;
        eq_varbit_coerce_l9:  varbit_coerce / pg_varbit_coerce len=9 varbit=true;
        eq_varbit_coerce_l16: varbit_coerce / pg_varbit_coerce len=16 varbit=true;
    }

    // Identity + error PLANES at concrete len (the fully-symbolic verdict
    // harnesses wall: even a fenced-out image arm keeps symbolic-length
    // alloc machinery in the formula — measured pass 2/3). With len
    // concrete, the dead image branch is concrete-length and harmless.
    // expl symbolic where the arm admits both values.
    macro_rules! coerce_plane_cell {
        ($($h:ident: $core:ident / $cfn:ident len=$len:expr, fence=$fence:expr,
           expect=$expect:tt, code=$code:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (ab, ay, al) = sym_varbit();
                let len: i32 = $len;
                let expl: bool = if stringify!($expect) == "err" { false } else { kani::any() };
                let fence: fn(usize) -> bool = $fence;
                kani::assume(fence(al));
                let p = payload(&ab, ay, al);
                let mut cr: [u8; N] = [0; N];
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    $cfn(ab.as_ptr(), ay as i32, al as i32, len, expl as i32,
                         cr.as_mut_ptr(), &mut cerr)
                };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::$core(ctx.mcx(), &p[..4 + ay], len, expl) {
                    Ok(None) => {
                        assert!(stringify!($expect) == "none");
                        assert!(cst == 0);
                    }
                    Ok(Some(img)) => {
                        core::mem::forget(img);
                        panic!("image arm inside plane fence")
                    }
                    Err(e) => {
                        let expect_err = stringify!($expect) == "err";
                        let ok = expect_err && cst == -1 && cerr == 2
                            && e.sqlstate == $code && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    coerce_plane_cell! {
        eq_bit_coerce_ident0: bit_coerce / pg_bit_coerce len=0,
            fence=|_al| true, expect=none, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH;
        eq_bit_coerce_ident_eq: bit_coerce / pg_bit_coerce len=12,
            fence=|al| al == 12, expect=none, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH;
        eq_bit_coerce_err9: bit_coerce / pg_bit_coerce len=9,
            fence=|al| al != 9, expect=err, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH;
        eq_varbit_coerce_ident0: varbit_coerce / pg_varbit_coerce len=0,
            fence=|_al| true, expect=none, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION;
        eq_varbit_coerce_ident_ge: varbit_coerce / pg_varbit_coerce len=64,
            fence=|_al| true, expect=none, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION;
        eq_varbit_coerce_err9: varbit_coerce / pg_varbit_coerce len=9,
            fence=|al| al > 9, expect=err, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION;
    }

    // ---------------- bitcat (1679) ----------------
    // The too-long error arm requires bitlen1 > VARBITMAXLEN - bitlen2 —
    // unreachable under the <=64-bit fences (arm outside the covered
    // domain; recorded in the ledger bounds).

    // Concrete (bitlen1, bitlen2) cells, content fully symbolic: (12,9)
    // exercises the shifted join loop (pad1 > 0), (16,8) the memcpy arm
    // (pad1 == 0), (5,0) the empty-arg2 skip, (0,7) the empty-arg1 plane.
    macro_rules! bitcat_cell {
        ($($h:ident: l1=$l1:literal l2=$l2:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(20)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (al, bl) = ($l1 as usize, $l2 as usize);
                let (ay, by) = (al.div_ceil(8), bl.div_ceil(8));
                let ab = sym_varbit_c(al);
                let bb = sym_varbit_c(bl);
                let pa = payload_c(&ab, al);
                let pb = payload_c(&bb, bl);
                let mut cr: [u8; 2 * N] = kani::any(); // C fully writes the result
                let mut cerr: i32 = -1;
                let cbitlen = unsafe {
                    pg_bit_catenate(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(),
                                    by as i32, bl as i32, cr.as_mut_ptr(), &mut cerr)
                };
                assert!(cbitlen as usize == al + bl && cerr == 0);
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::bit_catenate(ctx.mcx(), &pa[..4 + ay], &pb[..4 + by]) {
                    Ok(img) => {
                        assert_image(&img, al + bl, &cr);
                        core::mem::forget(img);
                    }
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("bitcat errored under fence");
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    bitcat_cell! {
        eq_bitcat_l12_9: l1=12 l2=9;
        eq_bitcat_l16_8: l1=16 l2=8;
        eq_bitcat_l5_0:  l1=5 l2=0;
        eq_bitcat_l0_7:  l1=0 l2=7;
    }

    // -------- bitsubstr (1680) / bitsubstr_no_len (1699) --------

    // Split (data-dependent result length = the measured OOM):
    //  - eq_bitsubstr_verdict: input, s, l FULLY symbolic, fenced to the
    //    non-allocating / zero-length-allocating arms (l < 0 error, s past
    //    end, l == 0 empty) — error verdict + sqlstate + empty-image parity;
    //  - image cells: concrete (bitlen, s, l) at the loop-structure
    //    boundaries (shifted copy / byte-aligned copy / clamp-to-end /
    //    overflow-to-end), content fully symbolic.

    // WALL — kept for documentation, NOT a proof: same dead-branch class as
    // the coerce verdicts (the fenced-out image arm's s/l-derived symbolic
    // alloc stays in the formula; OOM measured pass 3). The same planes are
    // proved by the all-concrete-scalar cells in substr_cell! below
    // (eq_bitsubstr_c_err / c_empty_past / c_empty_zero).
    #[allow(dead_code)]
    fn wall_bitsubstr_verdict() {
        let (ab, ay, al) = sym_varbit();
        let s: i32 = kani::any();
        let l: i32 = kani::any();
        // Non-allocating / empty-image arms only.
        kani::assume(l < 0 || s > al as i32 || l == 0);
        let p = payload(&ab, ay, al);
        let mut cr: [u8; N] = [0; N];
        let mut crbitlen: i32 = -1;
        let mut cerr: i32 = -1;
        let cst = unsafe {
            pg_bitsubstring(ab.as_ptr(), ay as i32, al as i32, s, l, 0,
                            cr.as_mut_ptr(), &mut crbitlen, &mut cerr)
        };
        let ctx = mcx::MemoryContext::new_bump("kani-varbit");
        match adt_varbit::bitsubstring(ctx.mcx(), &p[..4 + ay], s, l, false) {
            Ok(img) => {
                assert!(cst == 0);
                assert!(crbitlen == 0);
                assert!(adt_varbit::payload_bitlen(&img[4..]) == 0);
                core::mem::forget(img);
            }
            Err(e) => {
                let ok = cst == -1 && cerr == 6 && e.sqlstate == ERRCODE_SUBSTRING_ERROR
                    && e.level == ERROR;
                core::mem::forget(e);
                assert!(ok);
            }
        }
        core::mem::forget(ctx);
    }

    macro_rules! substr_cell {
        ($($h:ident: bitlen=$bl:literal s=$s:literal l=$l:expr, not_spec=$ns:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(20)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let al: usize = $bl;
                let ay = al.div_ceil(8);
                let ab = sym_varbit_c(al);
                let (s, l): (i32, i32) = ($s, $l);
                let p = payload_c(&ab, al);
                let mut cr: [u8; N] = kani::any();
                let mut crbitlen: i32 = -1;
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    pg_bitsubstring(ab.as_ptr(), ay as i32, al as i32, s, l,
                                    $ns as i32, cr.as_mut_ptr(), &mut crbitlen, &mut cerr)
                };
                assert!(cst == 0);
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::bitsubstring(ctx.mcx(), &p[..4 + ay], s, l, $ns) {
                    Ok(img) => {
                        assert_image(&img, crbitlen as usize, &cr);
                        core::mem::forget(img);
                    }
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("substr cell errored");
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    substr_cell! {
        eq_bitsubstr_c_shift:   bitlen=16 s=4 l=7, not_spec=false;  // shifted copy
        eq_bitsubstr_c_aligned: bitlen=16 s=9 l=8, not_spec=false;  // byte-aligned copy
        eq_bitsubstr_c_clamp:   bitlen=12 s=1 l=100, not_spec=false; // clamp to end
        eq_bitsubstr_c_ovf:     bitlen=12 s=3 l=i32::MAX, not_spec=false; // s+l overflow
        eq_bitsubstr_no_len_c:  bitlen=13 s=6 l=-1, not_spec=true;  // no-len variant
        eq_bitsubstr_c_empty_past: bitlen=12 s=13 l=5, not_spec=false; // empty (s past end)
        eq_bitsubstr_c_empty_zero: bitlen=12 s=4 l=0, not_spec=false;  // empty (zero length)
        eq_bitsubstr_c_neg_s:   bitlen=12 s=-3 l=8, not_spec=false; // s<1 clamp (Max arm)
    }

    /// Negative-length error plane, all scalars concrete (verdict + 2202E).
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bitsubstr_c_err() {
        let al: usize = 12;
        let ay = al.div_ceil(8);
        let ab = sym_varbit_c(al);
        let (s, l): (i32, i32) = (2, -5);
        let p = payload_c(&ab, al);
        let mut cr: [u8; N] = [0; N];
        let mut crbitlen: i32 = -1;
        let mut cerr: i32 = -1;
        let cst = unsafe {
            pg_bitsubstring(ab.as_ptr(), ay as i32, al as i32, s, l, 0,
                            cr.as_mut_ptr(), &mut crbitlen, &mut cerr)
        };
        let ctx = mcx::MemoryContext::new_bump("kani-varbit");
        match adt_varbit::bitsubstring(ctx.mcx(), &p[..4 + ay], s, l, false) {
            Ok(img) => {
                core::mem::forget(img);
                panic!("ok arm on the error plane")
            }
            Err(e) => {
                let ok = cst == -1 && cerr == 6 && e.sqlstate == ERRCODE_SUBSTRING_ERROR
                    && e.level == ERROR;
                core::mem::forget(e);
                assert!(ok);
            }
        }
        core::mem::forget(ctx);
    }

    // -------- bitoverlay (3030) / bitoverlay_no_len (3031) --------
    // Direct composition, exactly as both sides implement it.

    // Split (composition of four allocations — symbolic lengths OOM):
    //  - eq_bitoverlay_verdict: everything FULLY symbolic, fenced to the
    //    two pre-composition error arms (sp <= 0, sp + sl overflow);
    //  - image cells: concrete (l1, l2, sp, sl), content fully symbolic —
    //    covers mid-string replace, replace-past-end (empty s2), and the
    //    no-len variant (sl = bitlen2).

    // WALL — kept for documentation, NOT a proof: same dead-branch class
    // (the fenced-out composition's sp/sl-derived symbolic allocs stay in
    // the formula). Planes proved by eq_bitoverlay_c_err_sp0 /
    // eq_bitoverlay_c_err_ovf below (all scalars concrete).
    #[allow(dead_code)]
    fn wall_bitoverlay_verdict() {
        let (ab, ay, al) = sym_varbit();
        let (bb, by, bl) = sym_varbit();
        let sp: i32 = kani::any();
        let sl: i32 = kani::any();
        kani::assume(sp <= 0 || sp.checked_add(sl).is_none()); // error arms only
        let pa = payload(&ab, ay, al);
        let pb = payload(&bb, by, bl);
        let mut cr: [u8; 3 * N] = [0; 3 * N];
        let mut crbitlen: i32 = -1;
        let mut cerr: i32 = -1;
        let cst = unsafe {
            pg_bit_overlay(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(), by as i32,
                           bl as i32, sp, sl, cr.as_mut_ptr(), &mut crbitlen, &mut cerr)
        };
        let ctx = mcx::MemoryContext::new_bump("kani-varbit");
        match adt_varbit::bit_overlay(ctx.mcx(), &pa[..4 + ay], &pb[..4 + by], sp, sl) {
            Ok(img) => {
                core::mem::forget(img);
                panic!("ok arm inside error fence")
            }
            Err(e) => {
                let ok = cst == -1
                    && ((cerr == 6 && e.sqlstate == ERRCODE_SUBSTRING_ERROR)
                        || (cerr == 7 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
                    && e.level == ERROR;
                core::mem::forget(e);
                assert!(ok);
            }
        }
        core::mem::forget(ctx);
    }

    macro_rules! overlay_cell {
        ($($h:ident: l1=$l1:literal l2=$l2:literal sp=$sp:literal sl=$sl:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(28)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (al, bl) = ($l1 as usize, $l2 as usize);
                let (ay, by) = (al.div_ceil(8), bl.div_ceil(8));
                let ab = sym_varbit_c(al);
                let bb = sym_varbit_c(bl);
                let (sp, sl): (i32, i32) = ($sp, $sl);
                let pa = payload_c(&ab, al);
                let pb = payload_c(&bb, bl);
                let mut cr: [u8; 3 * N] = kani::any();
                let mut crbitlen: i32 = -1;
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    pg_bit_overlay(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(),
                                   by as i32, bl as i32, sp, sl, cr.as_mut_ptr(),
                                   &mut crbitlen, &mut cerr)
                };
                assert!(cst == 0);
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::bit_overlay(ctx.mcx(), &pa[..4 + ay], &pb[..4 + by], sp, sl)
                {
                    Ok(img) => {
                        assert_image(&img, crbitlen as usize, &cr);
                        core::mem::forget(img);
                    }
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("overlay cell errored");
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    // Cell sizes: 16-bit inputs OOM symex even fully concrete (the composed
    // two-substring + two-catenate chain is the heaviest image shape in the
    // family) — cells sized down to <= 9-bit inputs. RE-DERIVED 2026-07-29:
    // still OOM at the 6GiB cap on BOTH solvers even at <= 9 bits — kept as
    // live proofs for the fleet 40GB retry tier; bitoverlay's ledger claim
    // rests on the proved kernels (bitsubstring/bit_catenate image cells)
    // plus the overlay_err_cell planes below.
    overlay_cell! {
        eq_bitoverlay_c_mid:  l1=9 l2=4 sp=3 sl=4;    // mid-string replace, shifted
        eq_bitoverlay_c_tail: l1=8 l2=4 sp=7 sl=8;    // sp+sl past end -> empty s2
        eq_bitoverlay_no_len_c: l1=9 l2=8 sp=9 sl=8;  // no-len (sl = bitlen2)
    }

    /// Overlay error planes, all scalars concrete (verdict + sqlstate).
    macro_rules! overlay_err_cell {
        ($($h:ident: sp=$sp:expr, sl=$sl:expr, cerr=$ce:literal, code=$code:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let (al, bl) = (12usize, 4usize);
                let (ay, by) = (al.div_ceil(8), bl.div_ceil(8));
                let ab = sym_varbit_c(al);
                let bb = sym_varbit_c(bl);
                let (sp, sl): (i32, i32) = ($sp, $sl);
                let pa = payload_c(&ab, al);
                let pb = payload_c(&bb, bl);
                let mut cr: [u8; 3 * N] = [0; 3 * N];
                let mut crbitlen: i32 = -1;
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    pg_bit_overlay(ab.as_ptr(), ay as i32, al as i32, bb.as_ptr(),
                                   by as i32, bl as i32, sp, sl, cr.as_mut_ptr(),
                                   &mut crbitlen, &mut cerr)
                };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::bit_overlay(ctx.mcx(), &pa[..4 + ay], &pb[..4 + by], sp, sl)
                {
                    Ok(img) => {
                        core::mem::forget(img);
                        panic!("ok arm on the error plane")
                    }
                    Err(e) => {
                        let ok = cst == -1 && cerr == $ce && e.sqlstate == $code
                            && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    overlay_err_cell! {
        eq_bitoverlay_c_err_sp0: sp=0, sl=5, cerr=6, code=ERRCODE_SUBSTRING_ERROR;
        eq_bitoverlay_c_err_spneg: sp=-7, sl=5, cerr=6, code=ERRCODE_SUBSTRING_ERROR;
        eq_bitoverlay_c_err_ovf: sp=2, sl=i32::MAX, cerr=7, code=ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
    }

    // ---------------- bitposition (1698) ----------------

    // Fully-symbolic lengths = memory wall (measured: triple nested
    // data-dependent loop over two symbolic byte lengths OOMs CBMC even
    // with no allocation in sight). Split:
    //  - eq_bitposition_verdict: both inputs fully symbolic, fenced to the
    //    loop-free early-return planes (empty string / longer substring /
    //    empty substring);
    //  - cells at concrete BYTE lengths (loop bounds), bit lengths symbolic
    //    within the last byte (masks stay symbolic), content fully symbolic.

    // WALL — kept for documentation, NOT a proof: dead-branch class again
    // (the fenced-out match loops over symbolic byte lengths stay in the
    // formula; OOM measured pass 4). Planes proved by the concrete
    // eq_bitposition_p* cells below.
    #[allow(dead_code)]
    fn wall_bitposition_verdict() {
        let (sb, sy, sl) = sym_varbit();
        let (pb, py, pl) = sym_varbit();
        kani::assume(sl == 0 || pl > sl || pl == 0); // loop-free planes
        let ps = payload(&sb, sy, sl);
        let pp = payload(&pb, py, pl);
        let c = unsafe {
            pg_bitposition(sb.as_ptr(), sy as i32, sl as i32, pb.as_ptr(), py as i32,
                           pl as i32)
        };
        let r = adt_varbit::bitposition_core(&ps[..4 + sy], &pp[..4 + py]);
        assert!(r == c);
    }

    /// Symbolic content + symbolic bitlen pinned inside a CONCRETE byte
    /// length (bitlen in (8*(bytelen-1), 8*bytelen], pad bits zero).
    fn sym_varbit_y(bytelen: usize) -> ([u8; N], usize) {
        let mut bytes: [u8; N] = kani::any();
        let bitlen: usize = kani::any();
        kani::assume(bitlen > (bytelen - 1) * 8 && bitlen <= bytelen * 8);
        let pad = bytelen * 8 - bitlen;
        if pad > 0 {
            kani::assume(bytes[bytelen - 1] & !(0xFFu8 << pad) == 0);
        }
        let mut i = bytelen;
        while i < N {
            bytes[i] = 0;
            i += 1;
        }
        (bytes, bitlen)
    }

    macro_rules! bitposition_cell {
        ($($h:ident: sy=$sy:literal py=$py:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            fn $h() {
                let (sy, py) = ($sy as usize, $py as usize);
                let (sb, sl) = sym_varbit_y(sy);
                let (pb, pl) = sym_varbit_y(py);
                kani::assume(pl <= sl); // otherwise the (proved) verdict plane
                let mut ps = [0u8; 4 + N];
                ps[..4].copy_from_slice(&(sl as i32).to_ne_bytes());
                let mut i = 0usize;
                while i < sy { ps[4 + i] = sb[i]; i += 1; }
                let mut pp = [0u8; 4 + N];
                pp[..4].copy_from_slice(&(pl as i32).to_ne_bytes());
                let mut i = 0usize;
                while i < py { pp[4 + i] = pb[i]; i += 1; }
                let c = unsafe {
                    pg_bitposition(sb.as_ptr(), sy as i32, sl as i32, pb.as_ptr(),
                                   py as i32, pl as i32)
                };
                let r = adt_varbit::bitposition_core(&ps[..4 + sy], &pp[..4 + py]);
                assert!(r == c);
            }
        )*};
    }

    bitposition_cell! {
        eq_bitposition_y1_1: sy=1 py=1;
        eq_bitposition_y2_1: sy=2 py=1;
        eq_bitposition_y3_2: sy=3 py=2;
    }

    /// Early-return planes at concrete bit lengths (content symbolic):
    /// empty string -> 0, longer substring -> 0, empty substring -> 1.
    macro_rules! bitposition_plane_cell {
        ($($h:ident: sl=$sl:literal pl=$pl:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            fn $h() {
                let (sl, pl) = ($sl as usize, $pl as usize);
                let (sy, py) = (sl.div_ceil(8), pl.div_ceil(8));
                let sb = sym_varbit_c(sl);
                let pb = sym_varbit_c(pl);
                let ps = payload_c(&sb, sl);
                let pp = payload_c(&pb, pl);
                let c = unsafe {
                    pg_bitposition(sb.as_ptr(), sy as i32, sl as i32, pb.as_ptr(),
                                   py as i32, pl as i32)
                };
                let r = adt_varbit::bitposition_core(&ps[..4 + sy], &pp[..4 + py]);
                assert!(r == c);
            }
        )*};
    }

    bitposition_plane_cell! {
        eq_bitposition_p_empty_str: sl=0 pl=3; // -> 0
        eq_bitposition_p_long_sub:  sl=5 pl=9; // -> 0
        eq_bitposition_p_empty_sub: sl=5 pl=0; // -> 1
    }

    // -------- anybit_typmodin (2919 bittypmodin / 2902 varbittypmodin) --------
    // The cstring[]-literal parse (C ArrayGetIntegerTypmods / shipped
    // array_get_integer_typmods) stays in the tested tier both sides; the
    // theorem covers the shared check+value core over (tl, n).

    #[kani::proof]
    #[kani::unwind(4)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_anybit_typmodin() {
        let tl: [i32; 2] = kani::any();
        let n: usize = kani::any();
        kani::assume(n <= 2);
        let mut cerr: i32 = -1;
        let c = unsafe { pg_anybit_typmodin(tl.as_ptr(), n as i32, &mut cerr) };
        match adt_varbit::anybit_typmodin(&tl[..n], "bit") {
            Ok(v) => {
                assert!(cerr == 0);
                assert!(v == c);
            }
            Err(e) => {
                let ok = cerr == 9 && e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE
                    && e.level == ERROR;
                core::mem::forget(e);
                assert!(ok);
            }
        }
    }

    // ---------------- bit_in (1564) / varbit_in (1579) ----------------
    // Input cap: 6 symbolic non-NUL chars (+ NUL); covers the b/B/x/X
    // prefixes, binary and hex parse loops, and all reachable error arms.
    // atttypmod fenced <= 24 (image cap); the hex too-long arm (slen >
    // VARBITMAXLEN/4) is outside the cap — recorded in bounds.

    const IN: usize = 6;

    /// Symbolic non-NUL content at a CONCRETE cstring length (concrete
    /// length keeps every reachable image length concrete — bit_in's alloc
    /// length is bitlen-or-checked-equal-typmod on all non-error arms).
    fn sym_cstr_c(len: usize) -> [u8; IN + 1] {
        let mut s: [u8; IN + 1] = kani::any();
        let mut i = 0usize;
        while i < IN + 1 {
            if i < len {
                kani::assume(s[i] != 0);
            } else {
                s[i] = 0;
            }
            i += 1;
        }
        s
    }

    macro_rules! bits_in_cell {
        ($($h:ident: $cfn:ident fixed=$fixed:expr, code=$mismatch_code:expr, len=$len:literal, tm=$tm:expr, s0=$s0:expr;)*) => {$(
            // Content fully symbolic; len and atttypmod per-cell CONCRETE
            // (a symbolic atttypmod pinned == bitlen by the path condition
            // does not constant-fold the alloc length — measured pass 4
            // OOM; the interval-cmp "assumed equal" lesson again). The hex
            // too-long arm (slen > VARBITMAXLEN/4) is outside the 6-char
            // cap — bounds.
            #[kani::proof]
            #[kani::unwind(14)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let len: usize = $len;
                let mut s = sym_cstr_c(len);
                // Pin the input FORM: a symbolic first byte makes bitlen a
                // content-dependent phi at the single alloc site (bare vs
                // b-prefix vs hex), i.e. a symbolic-length alloc — measured
                // OOM even with len and atttypmod concrete. A LITERAL first
                // byte constant-folds the branch. 0 = keep (len-0 cell).
                let s0: u8 = $s0;
                if s0 != 0 {
                    s[0] = s0;
                }
                let atttypmod: i32 = $tm;
                let mut cbuf = [0u8; N];
                let mut crbitlen: i32 = -1;
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    $cfn(s.as_ptr(), atttypmod, cbuf.as_mut_ptr(), &mut crbitlen, &mut cerr)
                };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::bits_in(ctx.mcx(), &s[..len], atttypmod, $fixed, None) {
                    Ok(Some(img)) => {
                        assert!(cst == 0);
                        assert_image(&img, crbitlen as usize, &cbuf);
                        core::mem::forget(img);
                    }
                    Ok(None) => panic!("soft-error return without escontext"),
                    Err(e) => {
                        let ok = cst == -1
                            && ((cerr == 2 && e.sqlstate == $mismatch_code)
                                || (cerr == 3
                                    && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION)
                                || (cerr == 1
                                    && e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED))
                            && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    bits_in_cell! {
        eq_bit_in_len0: pg_bit_in fixed=true, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH, len=0, tm=-1, s0=0;
        eq_bit_in_bare_l3: pg_bit_in fixed=true, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH, len=3, tm=-1, s0=b'1';
        eq_bit_in_bpfx_l6: pg_bit_in fixed=true, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH, len=6, tm=-1, s0=b'b';
        eq_bit_in_xpfx_l6: pg_bit_in fixed=true, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH, len=6, tm=-1, s0=b'x';
        // typmod planes: tm == the form's bitlen (Ok + digit errs) and a
        // never-matching tm (mismatch err)
        eq_bit_in_bare_l6_tm6: pg_bit_in fixed=true, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH, len=6, tm=6, s0=b'0';
        eq_bit_in_bare_l3_tm7: pg_bit_in fixed=true, code=ERRCODE_STRING_DATA_LENGTH_MISMATCH, len=3, tm=7, s0=b'1';
        eq_varbit_in_len0: pg_varbit_in fixed=false, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION, len=0, tm=-1, s0=0;
        eq_varbit_in_bare_l3: pg_varbit_in fixed=false, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION, len=3, tm=-1, s0=b'0';
        eq_varbit_in_xpfx_l6: pg_varbit_in fixed=false, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION, len=6, tm=-1, s0=b'X';
        // truncation plane: hex form (bitlen 20) overflows tm=4
        eq_varbit_in_xpfx_l6_tm4: pg_varbit_in fixed=false, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION, len=6, tm=4, s0=b'x';
        // Ok-under-max plane: bare 3 bits <= tm 8
        eq_varbit_in_bare_l3_tm8: pg_varbit_in fixed=false, code=ERRCODE_STRING_DATA_RIGHT_TRUNCATION, len=3, tm=8, s0=b'1';
    }

    // ---------------- bit_out / varbit_out (1565 / 1580) ----------------
    // Same fc body both rows (C bit_out delegates to varbit_out). Cap:
    // bitlen <= 16 (the full-byte loop, the partial-byte loop, and the
    // boundary between them).

    // Concrete-bitlen cells (a symbolic bitlen = symbolic alloc + symbolic
    // per-bit output offsets, measured OOM): 12 covers the full-byte loop +
    // the partial-byte tail, 16 the pure full-byte path, 0 the empty image.
    macro_rules! bits_out_cell {
        ($($h:ident: bitlen=$bl:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(20)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let bitlen: usize = $bl;
                let bytelen = bitlen.div_ceil(8);
                let bytes = sym_varbit_c(bitlen);
                let p = payload_c(&bytes, bitlen);
                let mut cbuf: [u8; 17] = kani::any(); // C fully writes bitlen+1
                unsafe { pg_varbit_out(bytes.as_ptr(), bytelen as i32, bitlen as i32,
                                       cbuf.as_mut_ptr()) };
                let ctx = mcx::MemoryContext::new_bump("kani-varbit");
                match adt_varbit::bits_out(ctx.mcx(), &p[..4 + bytelen]) {
                    Ok(out) => {
                        assert!(out.len() == bitlen + 1);
                        let mut k = 0usize;
                        while k < bitlen + 1 {
                            assert!(out[k] == cbuf[k]);
                            k += 1;
                        }
                        core::mem::forget(out);
                    }
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("bits_out errored");
                    }
                }
                core::mem::forget(ctx);
            }
        )*};
    }

    bits_out_cell! {
        eq_bits_out_l12: bitlen=12;
        eq_bits_out_l16: bitlen=16;
        eq_bits_out_l0:  bitlen=0;
    }

    // ---------------- bit_recv (2456) / varbit_recv (2458) ----------------
    // Core-level (bits_recv takes &mut StringInfo directly — no pointer
    // datum round-trip, dodging the int-arith recv provenance wall). Full
    // symbolic message bytes (cap 12), symbolic data length AND cursor;
    // wire bitlen fenced to <= 64 UNION its two error planes (negative /
    // > VARBITMAXLEN).

    // Per-cell LITERAL wire header + cursor 0 + literal atttypmod (an
    // assumed-equal wire bitlen does not constant-fold the derived alloc —
    // measured OOM). RE-DERIVED 2026-07-29: the Ok-IMAGE cells
    // (eq_bit_recv_c12 / eq_varbit_recv_c12, concrete dlen=12) OOM at the
    // 6GiB cap on BOTH solvers — the recv Ok arm is UNPROVEN at this cap
    // (fleet 40GB retry owed); the short-header, short-body, negative-len,
    // over-max and typmod-mismatch/truncation planes all proved.
    macro_rules! recv_harness {
        ($h:ident, $cfn:ident, $fixed:expr, $mismatch_code:expr,
         hdr=$hdr:expr, tm=$tm:expr, dlen=$dl:expr) => {
            #[kani::proof]
            #[kani::unwind(16)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                const CAP: usize = 12;
                let mut data: [u8; CAP] = kani::any();
                let hdr: [u8; 4] = $hdr;
                data[0] = hdr[0];
                data[1] = hdr[1];
                data[2] = hdr[2];
                data[3] = hdr[3];
                // dlen also per-cell CONCRETE (a symbolic message length =
                // symbolic-length StringInfo append; measured OOM).
                let dlen: usize = $dl;
                let cur: usize = 0;
                let atttypmod: i32 = $tm;

                let mut ccur: i32 = cur as i32;
                let mut cbuf = [0u8; N];
                let mut crbitlen: i32 = -1;
                let mut cerr: i32 = -1;
                let cst = unsafe {
                    $cfn(data.as_ptr(), dlen as i32, &mut ccur, atttypmod,
                         cbuf.as_mut_ptr(), &mut crbitlen, &mut cerr)
                };

                let ctx = mcx::MemoryContext::new_bump("kani-varbit-recv");
                let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2)
                {
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
                match adt_varbit::bits_recv(ctx.mcx(), &mut si, atttypmod, $fixed) {
                    Ok(img) => {
                        assert!(cst == 0);
                        assert!(si.cursor == ccur as usize);
                        assert_image(&img, crbitlen as usize, &cbuf);
                        core::mem::forget(img);
                    }
                    Err(e) => {
                        let ok = cst == -1
                            && ((cerr == 4 && e.sqlstate == ERRCODE_PROTOCOL_VIOLATION)
                                || (cerr == 5
                                    && e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION)
                                || (cerr == 2 && e.sqlstate == $mismatch_code))
                            && e.level == ERROR;
                        core::mem::forget(e);
                        assert!(ok);
                    }
                }
                core::mem::forget(si);
                core::mem::forget(ctx);
            }
        };
    }

    recv_harness!(eq_bit_recv_c12, pg_bit_recv, true, ERRCODE_STRING_DATA_LENGTH_MISMATCH,
                  hdr=[0, 0, 0, 12], tm=-1, dlen=12); // Ok image
    recv_harness!(eq_bit_recv_c12_short, pg_bit_recv, true,
                  ERRCODE_STRING_DATA_LENGTH_MISMATCH, hdr=[0, 0, 0, 12], tm=-1,
                  dlen=5); // short body -> 08P01
    recv_harness!(eq_bit_recv_c_hdr_short, pg_bit_recv, true,
                  ERRCODE_STRING_DATA_LENGTH_MISMATCH, hdr=[0, 0, 0, 12], tm=-1,
                  dlen=2); // short header -> 08P01
    recv_harness!(eq_bit_recv_c_neg, pg_bit_recv, true, ERRCODE_STRING_DATA_LENGTH_MISMATCH,
                  hdr=[0x80, 0, 0, 0], tm=-1, dlen=12); // negative wire len -> 22P03
    recv_harness!(eq_bit_recv_c_max, pg_bit_recv, true, ERRCODE_STRING_DATA_LENGTH_MISMATCH,
                  hdr=[0x7F, 0xFF, 0xFF, 0xFF], tm=-1, dlen=12); // > VARBITMAXLEN -> 22P03
    recv_harness!(eq_bit_recv_c_mismatch, pg_bit_recv, true,
                  ERRCODE_STRING_DATA_LENGTH_MISMATCH, hdr=[0, 0, 0, 12], tm=5, dlen=12);
    recv_harness!(eq_varbit_recv_c12, pg_varbit_recv, false,
                  ERRCODE_STRING_DATA_RIGHT_TRUNCATION, hdr=[0, 0, 0, 12], tm=-1, dlen=12);
    recv_harness!(eq_varbit_recv_c_trunc, pg_varbit_recv, false,
                  ERRCODE_STRING_DATA_RIGHT_TRUNCATION, hdr=[0, 0, 0, 12], tm=5, dlen=12);
    recv_harness!(eq_varbit_recv_c_neg, pg_varbit_recv, false,
                  ERRCODE_STRING_DATA_RIGHT_TRUNCATION, hdr=[0xFF, 0xFF, 0xFF, 0xF4],
                  tm=-1, dlen=12);

    // ---------------- bit_send / varbit_send (2457 / 2459) ----------------
    // Shipped fc wrapper (same fc body both rows) over a real varlena arg
    // image; the ENTIRE wire image (4B varlena header + BE bitlen + bytes)
    // is byte-compared (int-arith send precedent).

    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bits_send() {
        eq_bits_send_cell(12); // concrete bitlen (symbolic walls, same class)
    }

    #[kani::proof]
    #[kani::unwind(20)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_bits_send_l0() {
        eq_bits_send_cell(0);
    }

    fn eq_bits_send_cell(al: usize) {
        use datum::{Datum, NullableDatum};
        use types_fmgr::LocalFcinfo;
        let ay = al.div_ceil(8);
        let ab = sym_varbit_c(al);
        let mut arg = [0u8; HDR + N];
        arg[..4].copy_from_slice(&datum::varlena::set_varsize_4b(HDR + ay));
        arg[4..8].copy_from_slice(&(al as i32).to_ne_bytes());
        let mut i = 0usize;
        while i < ay {
            arg[HDR + i] = ab[i];
            i += 1;
        }
        let mut cbuf = [0u8; HDR + N];
        let ctotal = unsafe {
            pg_varbit_send(ab.as_ptr(), ay as i32, al as i32, cbuf.as_mut_ptr())
        };
        let ctx = mcx::MemoryContext::new_bump("kani-varbit-send");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(arg.as_ptr() as usize));
        let d = match adt_varbit::fc_bit_send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("send errored")
            }
        };
        let total = HDR + ay;
        assert!(ctotal as usize == total);
        // SAFETY: varlena_result leaks the image in the stub bump buffer.
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, total) };
        let mut k = 0usize;
        while k < total {
            assert!(img[k] == cbuf[k]);
            k += 1;
        }
        core::mem::forget(ctx);
    }

    // ------------- input-regime coverage (vacuity insurance) -------------
    // Shared witnesses over the SAME input distributions + fences as the
    // harnesses above (inline covers cost one SAT call each — hoisted).

    #[kani::proof]
    #[kani::unwind(10)]
    fn cover_w10_scalar_arms() {
        let (_ab, _ay, al) = sym_varbit();
        let n: i32 = kani::any();
        let nb: i32 = kani::any();
        kani::cover!(n >= 0 && (n as usize) < al && (nb == 0 || nb == 1)); // set/get Ok
        kani::cover!(n < 0 || (n as usize) >= al); // index err
        kani::cover!(n >= 0 && (n as usize) < al && nb != 0 && nb != 1); // new-bit err
        let tl: [i32; 2] = kani::any();
        let tn: usize = kani::any();
        kani::assume(tn <= 2);
        kani::cover!(tn == 1 && tl[0] >= 1 && tl[0] <= 10 * 1024 * 1024 * 8); // typmod Ok
        kani::cover!(tn != 1); // modifier-count err
        kani::cover!(tn == 1 && tl[0] < 1); // too-small err
        kani::cover!(tn == 1 && tl[0] > 10 * 1024 * 1024 * 8); // too-big err
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn cover_w10_image_arms() {
        // Witness that each verdict-harness FENCE admits every arm it
        // claims, over the same input distribution.
        let (_ab, _ay, al) = sym_varbit();
        let len: i32 = kani::any();
        let expl: bool = kani::any();
        // bit()/varbit() coerce verdict fences: identity + err both reachable
        let bit_identity = len <= 0 || len > VARBITMAXLEN_I32 || len as usize == al;
        kani::assume(bit_identity || !expl);
        kani::cover!(bit_identity); // None arm
        kani::cover!(!bit_identity && !expl); // Err arm (bit)
        kani::cover!(len > 0 && (len as usize) < al && !expl); // Err arm (varbit)
        kani::cover!(len <= 0 || len as usize >= al); // None arm (varbit)
        // substring verdict fence: err / empty arms
        let s: i32 = kani::any();
        let l: i32 = kani::any();
        kani::assume(l < 0 || s > al as i32 || l == 0);
        kani::cover!(l < 0); // negative-length err
        kani::cover!(l >= 0 && s > al as i32); // empty (past end)
        kani::cover!(l == 0 && s >= 1 && s <= al as i32); // empty (zero length)
        // overlay verdict fence: both error arms
        let sp: i32 = kani::any();
        let sl: i32 = kani::any();
        kani::assume(sp <= 0 || sp.checked_add(sl).is_none());
        kani::cover!(sp <= 0);
        kani::cover!(sp > 0 && sp.checked_add(sl).is_none());
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn cover_w10_io_arms() {
        // bit_in/varbit_in cell distribution (concrete len 3, the mid cell)
        let s = sym_cstr_c(3);
        let atttypmod: i32 = kani::any();
        kani::assume(atttypmod <= 24);
        kani::cover!(s[0] == b'b' || s[0] == b'B'); // b prefix
        kani::cover!(s[0] == b'x' || s[0] == b'X'); // hex prefix
        kani::cover!(s[0] == b'0'); // bare binary
        kani::cover!(s[1] != b'0' && s[1] != b'1'); // bad binary digit plane
        kani::cover!(atttypmod > 0 && atttypmod != 3); // mismatch plane
        kani::cover!(atttypmod <= 0); // no-typmod plane
        // recv distribution (same fence as the recv harnesses)
        const CAP: usize = 12;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);
        let cur: usize = kani::any();
        kani::assume(cur <= CAP);
        kani::cover!(cur + 4 > dlen); // protocol err (short header)
        if cur + 4 <= dlen {
            let wb = i32::from_be_bytes([data[cur], data[cur + 1], data[cur + 2],
                                         data[cur + 3]]);
            kani::assume(wb == 12 || wb < 0 || wb > VARBITMAXLEN_I32);
            kani::cover!(wb < 0); // invalid external length
            kani::cover!(wb > VARBITMAXLEN_I32); // invalid external length (high)
            kani::cover!(wb == 12 && cur + 4 + 2 <= dlen); // Ok arm
            kani::cover!(wb == 12 && cur + 4 + 2 > dlen); // short body
        }
    }

    // ---------------- negative controls (must FAIL) ----------------
    // Verified failing 2026-07-29 (relaunch re-derivation): index_skew
    // 0.63s on "assertion failed: v == c"; flipped_digit 85.5s on
    // assert_image byte parity. Run with the DEFAULT solver.

    /// C reads the neighboring bit — the scalar rig must catch it.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_bitgetbit_index_skew() {
        let (ab, ay, al) = sym_varbit();
        let n: i32 = kani::any();
        kani::assume(n >= 1 && (n as usize) < al);
        let p = payload(&ab, ay, al);
        let mut cerr: i32 = -1;
        let c = unsafe { pg_bitgetbit(ab.as_ptr(), ay as i32, al as i32, n - 1, &mut cerr) };
        match adt_varbit::bitgetbit_core(&p[..4 + ay], n) {
            Ok(v) => assert!(v == c),
            Err(e) => core::mem::forget(e),
        }
    }

    /// C parses a first byte with bit 0 flipped — the image rig must catch it.
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_bit_in_flipped_digit() {
        let len: usize = 3;
        let mut s = sym_cstr_c(len);
        let mut i = 1usize;
        while i < IN {
            if i < len {
                kani::assume(s[i] == b'0' || s[i] == b'1');
            }
            i += 1;
        }
        let mut cbuf = [0u8; N];
        let mut crbitlen: i32 = -1;
        let mut cerr: i32 = -1;
        s[0] = b'0'; // literal first byte (form pinned, see bits_in_cell)
        let cst = unsafe { pg_bit_in(s.as_ptr(), -1, cbuf.as_mut_ptr(), &mut crbitlen,
                                     &mut cerr) };
        s[0] = b'1'; // Rust parses the flipped digit -> bit 0 differs
        let ctx = mcx::MemoryContext::new_bump("kani-varbit-ctl");
        match adt_varbit::bits_in(ctx.mcx(), &s[..len], -1, true, None) {
            Ok(Some(img)) => {
                assert!(cst == 0);
                assert_image(&img, crbitlen as usize, &cbuf);
                core::mem::forget(img);
            }
            Ok(None) => panic!("soft-error return without escontext"),
            Err(e) => core::mem::forget(e),
        }
        core::mem::forget(ctx);
    }
}
