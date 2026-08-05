//! Kani C≡Rust equivalence: misc-ops batch (11 pg_proc rows):
//! booland_statefunc (2515) / boolor_statefunc (2516), date_pli (1141) /
//! date_mii (1142), tidin (48) / tidout (49), oidin (1798) / oidout (1799),
//! xidin (50), xid8in (5070), xid8out (5081).
//!
//! C side: c/pg_misc_ops.c — verbatim REL_18_STABLE bodies; see its header
//! for provenance and the full shim manifest (fmgr unwrap, ereport/ereturn
//! -> error-class codes, hand-vendored strtoul model with per-call-site
//! narrowing argument, %u/%llu modeled by the verbatim pg_ultoa_n /
//! pg_ulltoa_n emitters).
//!
//! Rust side (SHIPPED code, never copied):
//!   adt_bool::{booland_statefunc, boolor_statefunc}   (bool/src/lib.rs:138,142)
//!   adt_date::{date_pli, date_mii}                    (adt_date/src/lib.rs:363,374)
//!   adt_scalar::{tidin, tidout}                       (scalar/src/lib.rs:140,173)
//!   numutils::{uint32in_subr, uint64in_subr}          (numutils/src/lib.rs:460,470)
//!   adt_scalar::builtins::{fc_oidout, fc_xid8out}     (builtins.rs:96,204) —
//!     WRAPPER-LEVEL (datetime-cmp precedent): real LocalFcinfo<1> frame, so
//!     datum unwrap -> pg_ultoa_n/pg_ulltoa_n -> NUL -> Datum(ptr) is inside
//!     the theorem (including the thread-local scratch buffer plumbing).
//!   oidin/xidin both reduce to uint32in_subr(s, false, <typname>, esc) —
//!     one shared harness set is THE theorem for both rows (typname only
//!     feeds message text, which is out of scope); xid8in likewise reduces
//!     to uint64in_subr. The String::from_utf8_lossy in the fc_* in-wrappers
//!     stays OUT of these theorems (harnesses hand a &str to the subr, the
//!     shipped public entry).
//!
//! Error-path coverage (cash/strtoint precedent): message TEXT leaves every
//! proof (alloc::fmt::format + PgError::new stubbed with a field-identical
//! constructor minus Location::caller(), which Kani cannot execute);
//! sqlstate CLASS + level, set by the SHIPPED with_sqlstate calls, stay in
//! the theorem and are asserted against the C error-class code.
//!
//! Input-domain fences (documented, deliberate; strtoint precedent):
//! string inputs are symbolic bytes 1..=127 — no interior NUL (C is
//! NUL-terminated, Rust length-delimited: representational mismatch, not a
//! parser question) and no >=128 bytes (keeps the &str UTF-8-valid without
//! a multibyte side condition; high bytes are never digits/space/delims in
//! either implementation). C buffers are OVERSIZED (32/24 bytes) relative
//! to the unwind bound so infeasible unwound iterations cannot generate
//! pointer-UB blowup (strtoint measured trap).
//!
//! DIVERGENCE (proved, adjudicated bug — see eq_tidin_* fence):
//! C tidin accepts an EMPTY numeric field when the byte at the field start
//! is exactly the required terminating delimiter, because libc strtoul
//! with no digits returns 0 with endptr == nptr and tidin's only check is
//! *endptr == delimiter: "(,5)" -> (0,5), "(5,)" -> (5,0), "(,)" -> (0,0).
//! The shipped Rust tidin rejects (strtoul_c returns None on empty digit
//! runs). Witnessed by census_tidin_empty_field (expected-FAIL harness,
//! default solver). Standing eq harnesses fence the class out via
//! fence_no_empty_tid_field and prove full parity on the remainder.
//!
//! MEASURED SOLVER INVERSION (this crate, 2026-07-28): the DEFAULT
//! incremental solver BEATS `--solver kissat` on every harness here —
//! eq_tidout_r1_lt1e4 default 22s GREEN vs kissat >30s wall; eq_u32in_len4
//! default 18s GREEN vs kissat wall. kissat wedges in per-batch
//! propositional reduction on these many-property mixed-arm harnesses. The
//! prove-target skill's kissat-first rule does NOT hold for this shape;
//! run-all.sh therefore uses the default solver.
//!
//! UNWIND FLOOR (measured): setting unwind to the emitters' own exact trip
//! counts (5) FABRICATED byte-level counterexamples in the tidout/oidout
//! bands — CBMC's builtin memcpy expansion was silently truncated, and
//! inconsistently across bands. intout's calibrated 13 is the floor here:
//! too-tight is not merely slow, it looks unsound.
//!
//! CBMC DESTINATION-OFFSET ARTIFACT (bounds the tidout claim): when
//! pg_ultoa_n's `while value >= 10000` loop runs with the destination
//! pointer at `base + 1` (which is exactly how tidout emits the block, at
//! buf+1), the harness reports byte counterexamples that (a) replay GREEN as
//! concrete cases, and (b) vanish when the identical comparison uses
//! destination offset 0 — where the same band is exhaustively GREEN.
//! Witnessed by artifact_probe_ultoa_dest_offset1 (FAILS) vs
//! artifact_control_ultoa_dest_offset0 (PASSES). NOT a pgrust divergence.
//!
//! Negative controls (DEFAULT solver — kissat never terminates on failing
//! harnesses): control_booland_vs_c_boolor, control_date_pli_vs_c_mii,
//! census_tidin_empty_field. All MUST fail.

#![allow(dead_code)]

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use types_error::{
        ErrorLevel, PgError, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
        ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
    };
    use types_fmgr::LocalFcinfo;

    use core::ffi::c_char;
    use std::os::raw::c_int;

    extern "C" {
        fn pg_booland_statefunc(a: c_int, b: c_int) -> c_int;
        fn pg_boolor_statefunc(a: c_int, b: c_int) -> c_int;

        fn pg_date_pli(date_val: i32, days: i32, result_out: *mut i32) -> c_int;
        fn pg_date_mii(date_val: i32, days: i32, result_out: *mut i32) -> c_int;

        fn pg_tidin(str_: *const c_char, block_out: *mut u32, offset_out: *mut u16) -> c_int;
        fn pg_tidout(block: u32, offset: u16, buf: *mut u8) -> c_int;

        fn pg_uint32in_subr(
            s: *const c_char,
            result_out: *mut u32,
            want_endloc: c_int,
            endloc_out: *mut *const c_char,
        ) -> c_int;
        fn pg_uint64in_subr(
            s: *const c_char,
            result_out: *mut u64,
            want_endloc: c_int,
            endloc_out: *mut *const c_char,
        ) -> c_int;

        fn pg_oidout(o: u32, buf: *mut u8) -> c_int;
        // exposed for the destination-offset artifact probes below
        fn pg_c_ultoa_n(value: u32, a: *mut u8) -> c_int;
        fn pg_xid8out(fxid: u64, buf: *mut u8) -> c_int;
    }

    const C_OK: c_int = 0;
    const C_ERR_SYNTAX: c_int = 1; // ERRCODE_INVALID_TEXT_REPRESENTATION (22P02)
    const C_ERR_RANGE: c_int = 2; // ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (22003)

    // ---------------- stubs (strtoint precedent, copied) ----------------

    /// Stub for `alloc::fmt::format`: error paths build messages via
    /// `format!`, whose fmt machinery walls CBMC. Message text is NOT part
    /// of any claim here (only sqlstate class + level, set independently).
    fn stub_format(_args: core::fmt::Arguments<'_>) -> String {
        String::new()
    }

    /// Stub for `types_error::PgError::new`: the shipped constructor is
    /// #[track_caller] and reads core::panic::Location::caller() (Kani
    /// unsupported, kani#374). Field-identical minus location; sqlstate
    /// starts at the same default_sqlstate_for_level(level), so the shipped
    /// .with_sqlstate at every call site under proof stays load-bearing.
    fn stub_pg_error_new(level: ErrorLevel, message: impl Into<String>) -> PgError {
        PgError {
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

    // ============ A. booland_statefunc / boolor_statefunc ============
    // Full finite domain (bool x bool).

    #[kani::proof]
    fn eq_booland_statefunc() {
        let a: bool = kani::any();
        let b: bool = kani::any();
        let r = adt_bool::booland_statefunc(a, b);
        let c = unsafe { pg_booland_statefunc(a as c_int, b as c_int) };
        assert!(r as c_int == c);
    }

    #[kani::proof]
    fn eq_boolor_statefunc() {
        let a: bool = kani::any();
        let b: bool = kani::any();
        let r = adt_bool::boolor_statefunc(a, b);
        let c = unsafe { pg_boolor_statefunc(a as c_int, b as c_int) };
        assert!(r as c_int == c);
    }

    /// NEGATIVE CONTROL — must FAIL (counterexample at a != b): shipped
    /// booland vs C boolor. DEFAULT solver only.
    #[kani::proof]
    fn control_booland_vs_c_boolor() {
        let a: bool = kani::any();
        let b: bool = kani::any();
        let r = adt_bool::booland_statefunc(a, b);
        let c = unsafe { pg_boolor_statefunc(a as c_int, b as c_int) };
        assert!(r as c_int == c);
    }

    // ============ B. date_pli / date_mii ============
    // Full symbolic i32 x i32 (cash fallible-op pattern): Ok <=> C ok with
    // equal values (including the infinity passthrough), Err <=> C ereport
    // with sqlstate ERRCODE_DATETIME_VALUE_OUT_OF_RANGE + level ERROR set
    // by the SHIPPED date_out_of_range_plain (message text stubbed out).

    macro_rules! date_op {
        ($($h:ident: $rf:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
            fn $h() {
                let date: i32 = kani::any();
                let days: i32 = kani::any();
                let mut cval: i32 = 0;
                let cerr = unsafe { $pg(date, days, &mut cval) };
                match adt_date::$rf(date, days) {
                    Ok(v) => {
                        assert!(cerr == C_OK);
                        assert!(v == cval);
                    }
                    Err(e) => {
                        assert!(cerr != C_OK);
                        assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                    }
                }
            }
        )*};
    }

    date_op! {
        eq_date_pli: date_pli / pg_date_pli;
        eq_date_mii: date_mii / pg_date_mii;
    }

    /// NEGATIVE CONTROL — must FAIL: shipped date_pli vs C date_mii
    /// (counterexample at any days != 0 with finite date). Proves the
    /// stubbed fallible rig is non-vacuous. DEFAULT solver only.
    #[kani::proof]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn control_date_pli_vs_c_mii() {
        let date: i32 = kani::any();
        let days: i32 = kani::any();
        let mut cval: i32 = 0;
        let cerr = unsafe { pg_date_mii(date, days, &mut cval) };
        match adt_date::date_pli(date, days) {
            Ok(v) => {
                assert!(cerr == C_OK);
                assert!(v == cval);
            }
            Err(_) => assert!(cerr != C_OK),
        }
    }

    // ============ C1. tidin ============

    /// Harness-side mirror of the divergence fence (see module doc): true
    /// iff neither tid field is empty-with-lucky-delimiter, i.e. excludes
    /// exactly the inputs where C strtoul performs no conversion yet tidin's
    /// *endptr check still passes (C accepts, shipped Rust rejects —
    /// adjudicated bug, witnessed by census_tidin_empty_field). Mirrors the
    /// C/Rust delimiter scan verbatim; used ONLY to partition the domain.
    fn fence_no_empty_tid_field(s: &[u8]) -> bool {
        let mut coord = [0usize; 2];
        let mut n = 0;
        for (p, &c) in s.iter().enumerate() {
            if n >= 2 || c == b')' {
                break;
            }
            if c == b',' || (c == b'(' && n == 0) {
                coord[n] = p + 1;
                n += 1;
            }
        }
        if n < 2 {
            return true; // both sides reject before any strtoul: no fence needed
        }
        s.get(coord[0]).copied() != Some(b',') && s.get(coord[1]).copied() != Some(b')')
    }

    /// Fully-symbolic bytes (1..=127) at concrete length N, fenced per the
    /// adjudicated divergence; verdict + block/offset value parity.
    fn tidin_check<const N: usize>() {
        let mut bytes = [0u8; N];
        let mut cbuf = [0 as c_char; 32]; // oversized on purpose (module doc)
        let mut i = 0;
        while i < N {
            let b: u8 = kani::any();
            kani::assume(b >= 1 && b <= 127);
            bytes[i] = b;
            cbuf[i] = b as c_char;
            i += 1;
        }
        kani::assume(fence_no_empty_tid_field(&bytes[..N]));

        let mut cb: u32 = 0;
        let mut co: u16 = 0;
        let cerr = unsafe { pg_tidin(cbuf.as_ptr(), &mut cb, &mut co) };
        match adt_scalar::tidin(&bytes[..N]) {
            Some(t) => {
                assert!(cerr == C_OK);
                assert!(t.block == cb);
                assert!(t.offset == co);
            }
            None => assert!(cerr != C_OK),
        }
    }

    macro_rules! tidin_len {
        ($($h:ident[$u:literal]: $n:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $h() {
                tidin_check::<$n>();
            }
        )*};
    }

    tidin_len! {
        eq_tidin_len0[4]: 0;
        eq_tidin_len1[5]: 1;
        eq_tidin_len2[6]: 2;
        eq_tidin_len3[7]: 3;
        eq_tidin_len4[8]: 4;
        eq_tidin_len5[9]: 5;
        eq_tidin_len6[10]: 6;
        eq_tidin_len7[11]: 7;
        eq_tidin_len8[12]: 8;
        eq_tidin_len9[13]: 9;
        eq_tidin_len10[14]: 10;
        eq_tidin_len11[15]: 11;
        eq_tidin_len12[16]: 12;
        eq_tidin_len13[17]: 13;
        eq_tidin_len14[18]: 14;
        eq_tidin_len15[19]: 15;
        eq_tidin_len16[20]: 16;
        eq_tidin_len17[21]: 17;
        eq_tidin_len18[22]: 18;
        eq_tidin_len19[23]: 19;
        eq_tidin_len20[24]: 20;
    }

    /// UNION COVERAGE for the tidin per-length split: the CLAIMED symbolic
    /// domain is every NUL-free ASCII input of length <= 10 satisfying the
    /// divergence fence; each such input has exactly one length, covered by
    /// exactly one eq_tidin_len{N}. (len 11..=20 harnesses exist but WALL —
    /// see run-all.sh; they are NOT part of the claim.)
    #[kani::proof]
    fn cover_tidin_len_split() {
        let len: usize = kani::any();
        kani::assume(len <= 10);
        let mut hit = false;
        let mut n = 0usize;
        while n <= 10 {
            if len == n {
                hit = true;
            }
            n += 1;
        }
        assert!(hit);
    }

    /// Accept-shape grid member: "(" + D1 symbolic digits + "," + D2
    /// symbolic digits + ")" — concrete field widths (pg_lsn precedent:
    /// concrete split points keep the parse loops concretely bounded).
    /// Covers the full-syntax accept surface beyond any general-grammar
    /// length wall, including every 10-digit block / 5-digit offset form.
    /// (No fence needed: fields are non-empty by construction.)
    fn tidin_accept_shape<const D1: usize, const D2: usize>() {
        let n = 1 + D1 + 1 + D2 + 1;
        let mut bytes = [0u8; 20];
        let mut cbuf = [0 as c_char; 32];
        bytes[0] = b'(';
        let mut i = 1;
        while i <= D1 {
            let b: u8 = kani::any();
            kani::assume(b.is_ascii_digit());
            bytes[i] = b;
            i += 1;
        }
        bytes[1 + D1] = b',';
        i = 2 + D1;
        while i < 2 + D1 + D2 {
            let b: u8 = kani::any();
            kani::assume(b.is_ascii_digit());
            bytes[i] = b;
            i += 1;
        }
        bytes[2 + D1 + D2] = b')';
        i = 0;
        while i < n {
            cbuf[i] = bytes[i] as c_char;
            i += 1;
        }

        let mut cb: u32 = 0;
        let mut co: u16 = 0;
        let cerr = unsafe { pg_tidin(cbuf.as_ptr(), &mut cb, &mut co) };
        match adt_scalar::tidin(&bytes[..n]) {
            Some(t) => {
                assert!(cerr == C_OK);
                assert!(t.block == cb);
                assert!(t.offset == co);
            }
            None => assert!(cerr != C_OK),
        }
    }

    macro_rules! tidin_accept {
        ($($h:ident[$u:literal]: $d1:literal $d2:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $h() {
                tidin_accept_shape::<$d1, $d2>();
            }
        )*};
    }

    // Grid restricted to shapes LONGER than the general per-length coverage
    // would cheaply reach, plus the max-width corners. d1 <= 11 / d2 <= 6
    // would only add all-reject shapes; 10 and 5 are the last widths with
    // accepts (11-digit fields can still parse — value-overflow rejects —
    // covered by the wider d1 spots below).
    // unwind = 1 + D1 + 1 + D2 + 1 + 2 (fill/scan/parse loops peak at the
    // total shape length + guard iteration; slack kept minimal per the
    // intout dead-copy trap).
    tidin_accept! {
        eq_tidin_accept_10_5[20]: 10 5;
        eq_tidin_accept_10_4[19]: 10 4;
        eq_tidin_accept_10_3[18]: 10 3;
        eq_tidin_accept_10_2[17]: 10 2;
        eq_tidin_accept_10_1[16]: 10 1;
        eq_tidin_accept_9_5[19]: 9 5;
        eq_tidin_accept_9_4[18]: 9 4;
        eq_tidin_accept_8_5[18]: 8 5;
        eq_tidin_accept_7_5[17]: 7 5;
        eq_tidin_accept_6_5[16]: 6 5;
        eq_tidin_accept_5_5[15]: 5 5;
        eq_tidin_accept_11_5[21]: 11 5;
        eq_tidin_accept_12_5[22]: 12 5;
        eq_tidin_accept_10_6[21]: 10 6;
    }

    /// Concrete witness spots: representative accepts/rejects on BOTH sides
    /// (prompt-mandated set + delimiter-scan edge shapes).
    fn tidin_spot(s: &[u8], expect: Option<(u32, u16)>) {
        let mut cbuf = [0 as c_char; 32];
        let mut i = 0;
        while i < s.len() {
            cbuf[i] = s[i] as c_char;
            i += 1;
        }
        let mut cb: u32 = 0;
        let mut co: u16 = 0;
        let cerr = unsafe { pg_tidin(cbuf.as_ptr(), &mut cb, &mut co) };
        let r = adt_scalar::tidin(s);
        match expect {
            Some((eb, eo)) => {
                assert!(cerr == C_OK && cb == eb && co == eo);
                match r {
                    Some(t) => assert!(t.block == eb && t.offset == eo),
                    None => panic!("rust rejected an expected accept"),
                }
            }
            None => {
                assert!(cerr != C_OK);
                assert!(r.is_none());
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(28)]
    fn eq_tidin_spots() {
        tidin_spot(b"(0,0)", Some((0, 0)));
        tidin_spot(b"(4294967295,65535)", Some((4294967295, 65535)));
        tidin_spot(b"( +1 , 2 )", None); // trailing space after digits: badp != delim
        tidin_spot(b"( +1,2)", Some((1, 2))); // strtoul skips space + sign
        tidin_spot(b"(-1,0)", Some((4294967295, 0))); // negative wrap accept (i32 sign-extension window)
        tidin_spot(b"(99999999999999999999,0)", None); // strtoul ERANGE
        tidin_spot(b"(4294967296,0)", None); // narrowing reject (not u32/i32-extendable)
        tidin_spot(b"(-2147483648,0)", Some((2147483648, 0))); // last wrap accept
        tidin_spot(b"(-2147483649,0)", None); // one past the sign-extension window
        tidin_spot(b"(1,65536)", None); // offset > USHRT_MAX
        tidin_spot(b"junk(7,8)", Some((7, 8))); // pre-'(' junk allowed by the scan
        tidin_spot(b"(1,2)trailing", Some((1, 2))); // post-')' junk ignored
        tidin_spot(b"(1 2)", None);
        tidin_spot(b"1,2)", None); // no '(' -> only one coord found
        tidin_spot(b"", None);
    }

    // ============ C2. tidout ============
    // '(' + %u(block) + ',' + %u(offset) + ')' — C snprintf modeled by the
    // verbatim pg_ultoa_n emitter (SHIM 5). Rust: shipped adt_scalar::tidout
    // (which itself calls the SHIPPED numutils::pg_ultoa_n). Trailing NUL is
    // C-only plumbing (intout convention): compare len + bytes[..len].
    // Block magnitude bands per the intout sloped-wall calibration
    // ([0,1e7) symbolic + spots beyond); offset FULLY symbolic u16 in every
    // band harness (5-digit emitter fits the budget alongside).

    /// Straight-line full-buffer compare (NO loop): keeps the harness
    /// unwind bound decoupled from the output length, so it can sit at the
    /// emitter loops' exact trip counts (dead divider-loop copies are the
    /// intout catastrophic-slack trap; a length-driven compare loop forced
    /// unwind 13+ and walled these bands). Sound because both buffers are
    /// zero-initialized and equal lengths are asserted: bytes past the
    /// output are 0 on both sides (C's NUL == Rust's untouched 0).
    macro_rules! assert_prefix_eq {
        ($a:expr, $b:expr; $($i:literal)*) => { $(assert!($a[$i] == $b[$i]);)* };
    }

    fn tidout_case(block: u32, offset: u16) {
        let mut cbuf = [0u8; 32];
        let mut rbuf = [0u8; 32];
        let clen = unsafe { pg_tidout(block, offset, cbuf.as_mut_ptr()) } as usize;
        let rlen = adt_scalar::tidout(adt_scalar::Tid { block, offset }, &mut rbuf);
        assert!(clen == rlen);
        // max tidout length is 18 ("(4294967295,65535)"); compare 0..=18.
        assert_prefix_eq!(cbuf, rbuf; 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18);
    }

    macro_rules! tidout_band {
        ($($h:ident[$u:literal]: $lo:literal .. $hi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $h() {
                let block: u32 = kani::any();
                kani::assume(block >= $lo && block < $hi);
                let offset: u16 = kani::any();
                tidout_case(block, offset);
            }
        )*};
    }

    // unwind 13 = intout's calibrated 32-bit number (covers the C
    // leftmost_one_pos32 shift walk, both /10000 emitter loops, and CBMC's
    // builtin memcpy expansions). MEASURED TRAP: unwind(5) — the emitters'
    // own trip counts — silently truncated CBMC's *builtin memcpy* loops
    // and produced a SPURIOUS counterexample (block 26501/offset 41987,
    // byte 4) that is GREEN when re-run at unwind 20 as a concrete spot.
    // Under-bounding here fabricates divergences; the compare is loop-free
    // so 13 costs nothing.
    // STANDING: only the r1 band. Blocks >= 1e4 enter pg_ultoa_n's
    // `while value >= 10000` loop, whose backward `pos.sub(4)` write on a
    // NON-ZERO-OFFSET destination (tidout emits the block at buf+1) trips a
    // demonstrated CBMC pointer-arithmetic ARTIFACT — see
    // artifact_probe_ultoa_dest_offset1 below. Not a divergence: every
    // reported counterexample replays GREEN concretely, and the identical
    // comparison at destination offset 0 is exhaustively GREEN over the same
    // band. d5..d7b harnesses retained (NOT standing) for re-measurement.
    tidout_band! {
        eq_tidout_r1_lt1e4[13]: 0 .. 10_000;
        artifact_probe_tidout_d5[20]: 10_000 .. 100_000;
        artifact_probe_tidout_d6[13]: 100_000 .. 1_000_000;
        artifact_probe_tidout_d7a[13]: 1_000_000 .. 5_000_000;
        artifact_probe_tidout_d7b[13]: 5_000_000 .. 10_000_000;
    }

    /// ARTIFACT WITNESS (expected FAIL; DEFAULT solver). Same shipped
    /// numutils::pg_ultoa_n vs the same vendored C body that proofs/intout
    /// proves equivalent over [0,1e7) — but with the destination pointer at
    /// `base + 1` instead of `base`. This FAILS while
    /// artifact_control_ultoa_dest_offset0 (identical except dest offset 0)
    /// PASSES, and every counterexample it emits verifies GREEN when replayed
    /// as a concrete case. Kept so the finding is reproducible and so nobody
    /// mistakes the tidout d5+ failures for a pgrust defect.
    #[kani::proof]
    #[kani::unwind(24)]
    fn artifact_probe_ultoa_dest_offset1() {
        let block: u32 = kani::any();
        kani::assume(block >= 10_000 && block < 100_000);
        let mut cbuf = [0u8; 8];
        let mut rbuf = [0u8; 8];
        // SAFETY: cbuf has 8 bytes; the emitter writes at most 5 from +1.
        let clen = unsafe { pg_c_ultoa_n(block, cbuf.as_mut_ptr().add(1)) } as usize;
        let rlen = numutils::pg_ultoa_n(block, &mut rbuf[1..]);
        assert!(clen == rlen);
        let mut i = 1;
        while i < 7 {
            assert!(cbuf[i] == rbuf[i]);
            i += 1;
        }
    }

    /// Control for the artifact above: destination offset 0, everything else
    /// identical. GREEN (0.5s) — proves the failure is the offset, not the
    /// emitters.
    #[kani::proof]
    #[kani::unwind(24)]
    fn artifact_control_ultoa_dest_offset0() {
        let block: u32 = kani::any();
        kani::assume(block >= 10_000 && block < 100_000);
        let mut cbuf = [0u8; 8];
        let mut rbuf = [0u8; 8];
        let clen = unsafe { pg_c_ultoa_n(block, cbuf.as_mut_ptr()) } as usize;
        let rlen = numutils::pg_ultoa_n(block, &mut rbuf[..]);
        assert!(clen == rlen);
        let mut i = 0;
        while i < 5 {
            assert!(cbuf[i] == rbuf[i]);
            i += 1;
        }
    }

    /// MANDATORY union-coverage: the STANDING tidout symbolic claim is the
    /// single band [0, 1e4) with offset full-domain. Predicate mirrors the
    /// harness assume VERBATIM. (d5..d7b are artifact-probes, not claims.)
    #[kani::proof]
    fn cover_tidout_block_split() {
        let block: u32 = kani::any();
        kani::assume(block < 10_000);
        assert!(block < 10_000);
    }

    /// Concrete block spots across the wall region (d8-d10 + extremes),
    /// with offset boundary values. unwind covers the 8-iteration spot
    /// loop; the concrete emitter values constant-propagate.
    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_tidout_spots() {
        const BLOCKS: [u32; 8] = [
            10_000_000,
            49_999_999,
            50_000_000,
            99_999_999,
            100_000_000,
            999_999_999,
            1_000_000_000,
            u32::MAX,
        ];
        const OFFS: [u16; 4] = [0, 9, 10_000, u16::MAX];
        let mut k = 0;
        while k < BLOCKS.len() {
            tidout_case(BLOCKS[k], OFFS[k % 4]);
            k += 1;
        }
        // offset digit-count boundaries at a fixed in-band block
        tidout_case(7, 99);
        tidout_case(7, 100);
        tidout_case(7, 9_999);
        tidout_case(7, 10_000);
    }

    // ============ D1. uint32in_subr (oidin 1798 / xidin 50 core) ============
    // Rust entry: numutils::uint32in_subr(s, false, "oid", None) — the exact
    // shipped call shape of fc_oidin/fc_xidin/fc_cidin (escontext None =>
    // hard PgResult). Parity: verdict CLASS (ok / 22P02 / 22003, C shim
    // codes 0/1/2) + value on accept + level ERROR on reject.

    fn u32in_verdict(s: &str) -> (c_int, u32) {
        match numutils::uint32in_subr(s, false, "oid", None) {
            Ok((v, _rest)) => (C_OK, v),
            Err(e) => {
                assert!(e.level == ERROR);
                let ss = e.sqlstate;
                if ss == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
                    (C_ERR_RANGE, 0)
                } else if ss == ERRCODE_INVALID_TEXT_REPRESENTATION {
                    (C_ERR_SYNTAX, 0)
                } else {
                    (99, 0) // unreachable if the port is faithful
                }
            }
        }
    }

    fn u32in_check<const N: usize>() {
        let mut bytes = [0u8; N];
        let mut cbuf = [0 as c_char; 32];
        let mut i = 0;
        while i < N {
            let b: u8 = kani::any();
            kani::assume(b >= 1 && b <= 127);
            bytes[i] = b;
            cbuf[i] = b as c_char;
            i += 1;
        }
        let s = unsafe { core::str::from_utf8_unchecked(&bytes[..N]) };

        let mut cval: u32 = 0;
        let cerr = unsafe {
            pg_uint32in_subr(cbuf.as_ptr(), &mut cval, 0, core::ptr::null_mut())
        };
        let (rk, rv) = u32in_verdict(s);
        assert!(rk == cerr);
        assert!(cerr != C_OK || rv == cval);
    }

    macro_rules! u32in_len {
        ($($h:ident[$u:literal]: $n:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(alloc::fmt::format, stub_format)]
            #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
            fn $h() {
                u32in_check::<$n>();
            }
        )*};
    }

    u32in_len! {
        eq_u32in_len0[4]: 0;
        eq_u32in_len1[5]: 1;
        eq_u32in_len2[6]: 2;
        eq_u32in_len3[7]: 3;
        eq_u32in_len4[8]: 4;
        eq_u32in_len5[9]: 5;
        eq_u32in_len6[10]: 6;
        eq_u32in_len7[11]: 7;
        eq_u32in_len8[12]: 8;
        eq_u32in_len9[13]: 9;
        eq_u32in_len10[14]: 10;
        eq_u32in_len11[15]: 11;
        eq_u32in_len12[16]: 12;
    }

    /// UNION COVERAGE for the uint32in per-length split. CLAIMED symbolic
    /// domain: all NUL-free ASCII inputs of length <= 5. (len5 measured
    /// 23-26s unloaded, >30s under concurrent proof load — inside the claim
    /// but over the 10s standing budget; len 6..=12 harnesses WALL.)
    #[kani::proof]
    fn cover_u32in_len_split() {
        let len: usize = kani::any();
        kani::assume(len <= 5);
        let mut hit = false;
        let mut n = 0usize;
        while n <= 5 {
            if len == n {
                hit = true;
            }
            n += 1;
        }
        assert!(hit);
    }

    /// Concrete witnesses: base detection, extensions, error classes.
    fn u32in_spot(s: &[u8], expect: Result<u32, c_int>) {
        let mut cbuf = [0 as c_char; 32];
        let mut i = 0;
        while i < s.len() {
            cbuf[i] = s[i] as c_char;
            i += 1;
        }
        let rs = unsafe { core::str::from_utf8_unchecked(s) };
        let mut cval: u32 = 0;
        let cerr = unsafe {
            pg_uint32in_subr(cbuf.as_ptr(), &mut cval, 0, core::ptr::null_mut())
        };
        let (rk, rv) = u32in_verdict(rs);
        match expect {
            Ok(v) => {
                assert!(cerr == C_OK && cval == v);
                assert!(rk == C_OK && rv == v);
            }
            Err(class) => {
                assert!(cerr == class);
                assert!(rk == class);
            }
        }
    }

    // SPLIT into small batches: a 20-spot single harness WALLED at 30s
    // (measured) -- CBMC re-solves per property batch, so few-assertion
    // harnesses are the ladder step here, not a bigger unwind.
    macro_rules! u32in_spot_batch {
        ($($h:ident: { $($lit:literal => $exp:expr;)* } )*) => {$(
            #[kani::proof]
            #[kani::unwind(34)]
            #[kani::stub(alloc::fmt::format, stub_format)]
            #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
            fn $h() {
                $(u32in_spot($lit, $exp);)*
            }
        )*};
    }

    u32in_spot_batch! {
        eq_u32in_spots_dec: {
            b"0" => Ok(0);
            b"4294967295" => Ok(4294967295);
            b"+42" => Ok(42);
        }
        eq_u32in_spots_base: {
            b"0xFF" => Ok(255);
            b"0XfF" => Ok(255);
            b"010" => Ok(8);
        }
        eq_u32in_spots_base_edge: {
            b"08" => Err(C_ERR_SYNTAX);
            b"0x" => Err(C_ERR_SYNTAX);
            b"037777777777" => Ok(4294967295);
        }
        eq_u32in_spots_neg: {
            b"-1" => Ok(4294967295);
            b"-2147483648" => Ok(2147483648);
            b"-2147483649" => Err(C_ERR_RANGE);
        }
        eq_u32in_spots_space: {
            b"  17  " => Ok(17);
            b"5  " => Ok(5);
            b"5x" => Err(C_ERR_SYNTAX);
        }
        eq_u32in_spots_reject: {
            b"" => Err(C_ERR_SYNTAX);
            b" - " => Err(C_ERR_SYNTAX);
            b"-4294967295" => Err(C_ERR_RANGE);
        }
        eq_u32in_spots_range: {
            b"4294967296" => Err(C_ERR_RANGE);
            b"99999999999999999999" => Err(C_ERR_RANGE);
        }
        eq_u32in_spots_hexmax: {
            b"0xFFFFFFFF" => Ok(4294967295);
            b"0x100000000" => Err(C_ERR_RANGE);
        }
    }

    // ============ D2. uint64in_subr (xid8in 5070 core) ============

    fn u64in_verdict(s: &str) -> (c_int, u64) {
        match numutils::uint64in_subr(s, false, "xid8", None) {
            Ok((v, _rest)) => (C_OK, v),
            Err(e) => {
                assert!(e.level == ERROR);
                let ss = e.sqlstate;
                if ss == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
                    (C_ERR_RANGE, 0)
                } else if ss == ERRCODE_INVALID_TEXT_REPRESENTATION {
                    (C_ERR_SYNTAX, 0)
                } else {
                    (99, 0)
                }
            }
        }
    }

    fn u64in_check<const N: usize>() {
        let mut bytes = [0u8; N];
        let mut cbuf = [0 as c_char; 32];
        let mut i = 0;
        while i < N {
            let b: u8 = kani::any();
            kani::assume(b >= 1 && b <= 127);
            bytes[i] = b;
            cbuf[i] = b as c_char;
            i += 1;
        }
        let s = unsafe { core::str::from_utf8_unchecked(&bytes[..N]) };

        let mut cval: u64 = 0;
        let cerr = unsafe {
            pg_uint64in_subr(cbuf.as_ptr(), &mut cval, 0, core::ptr::null_mut())
        };
        let (rk, rv) = u64in_verdict(s);
        assert!(rk == cerr);
        assert!(cerr != C_OK || rv == cval);
    }

    macro_rules! u64in_len {
        ($($h:ident[$u:literal]: $n:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(alloc::fmt::format, stub_format)]
            #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
            fn $h() {
                u64in_check::<$n>();
            }
        )*};
    }

    u64in_len! {
        eq_u64in_len0[4]: 0;
        eq_u64in_len1[5]: 1;
        eq_u64in_len2[6]: 2;
        eq_u64in_len3[7]: 3;
        eq_u64in_len4[8]: 4;
        eq_u64in_len5[9]: 5;
        eq_u64in_len6[10]: 6;
        eq_u64in_len7[11]: 7;
        eq_u64in_len8[12]: 8;
        eq_u64in_len9[13]: 9;
        eq_u64in_len10[14]: 10;
        eq_u64in_len11[15]: 11;
        eq_u64in_len12[16]: 12;
    }

    /// UNION COVERAGE for the uint64in per-length split. CLAIMED symbolic
    /// domain: all NUL-free ASCII inputs of length <= 5; longer lengths are
    /// covered by the concrete spot harness only (len 6..=12 and the
    /// digits-only 13..=21 shapes WALL — see run-all.sh).
    #[kani::proof]
    fn cover_u64in_len_split() {
        let len: usize = kani::any();
        kani::assume(len <= 5);
        let mut hit = false;
        let mut n = 0usize;
        while n <= 5 {
            if len == n {
                hit = true;
            }
            n += 1;
        }
        assert!(hit);
    }

    /// Sign+digits exact-length shape (strtoint check_digits_exact
    /// precedent) for the widths past the general-grammar budget — covers
    /// the full-width decimal accept/overflow surface (first byte sign or
    /// digit, rest digits; base is always 10 unless the first digit is '0',
    /// in which case the remaining digits still parse — as octal or as
    /// trailing garbage — identically on both sides).
    fn u64in_digits_exact<const N: usize>() {
        let mut bytes = [0u8; N];
        let mut cbuf = [0 as c_char; 32];
        let mut i = 0;
        while i < N {
            let b: u8 = kani::any();
            if i == 0 {
                kani::assume(b == b'-' || b == b'+' || b.is_ascii_digit());
            } else {
                kani::assume(b.is_ascii_digit());
            }
            bytes[i] = b;
            cbuf[i] = b as c_char;
            i += 1;
        }
        let s = unsafe { core::str::from_utf8_unchecked(&bytes[..N]) };

        let mut cval: u64 = 0;
        let cerr = unsafe {
            pg_uint64in_subr(cbuf.as_ptr(), &mut cval, 0, core::ptr::null_mut())
        };
        let (rk, rv) = u64in_verdict(s);
        assert!(rk == cerr);
        assert!(cerr != C_OK || rv == cval);
    }

    macro_rules! u64in_digits {
        ($($h:ident[$u:literal]: $n:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(alloc::fmt::format, stub_format)]
            #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
            fn $h() {
                u64in_digits_exact::<$n>();
            }
        )*};
    }

    u64in_digits! {
        eq_u64in_digits13[16]: 13;
        eq_u64in_digits14[17]: 14;
        eq_u64in_digits15[18]: 15;
        eq_u64in_digits16[19]: 16;
        eq_u64in_digits17[20]: 17;
        eq_u64in_digits18[21]: 18;
        eq_u64in_digits19[22]: 19;
        eq_u64in_digits20[23]: 20;
        eq_u64in_digits21[24]: 21;
    }

    fn u64in_spot(s: &[u8], expect: Result<u64, c_int>) {
        let mut cbuf = [0 as c_char; 32];
        let mut i = 0;
        while i < s.len() {
            cbuf[i] = s[i] as c_char;
            i += 1;
        }
        let rs = unsafe { core::str::from_utf8_unchecked(s) };
        let mut cval: u64 = 0;
        let cerr = unsafe {
            pg_uint64in_subr(cbuf.as_ptr(), &mut cval, 0, core::ptr::null_mut())
        };
        let (rk, rv) = u64in_verdict(rs);
        match expect {
            Ok(v) => {
                assert!(cerr == C_OK && cval == v);
                assert!(rk == C_OK && rv == v);
            }
            Err(class) => {
                assert!(cerr == class);
                assert!(rk == class);
            }
        }
    }

    macro_rules! u64in_spot_batch {
        ($($h:ident: { $($lit:literal => $exp:expr;)* } )*) => {$(
            #[kani::proof]
            #[kani::unwind(34)]
            #[kani::stub(alloc::fmt::format, stub_format)]
            #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
            fn $h() {
                $(u64in_spot($lit, $exp);)*
            }
        )*};
    }

    u64in_spot_batch! {
        eq_u64in_spots_dec: {
            b"0" => Ok(0);
            b"18446744073709551615" => Ok(u64::MAX);
        }
        eq_u64in_spots_range: {
            b"18446744073709551616" => Err(C_ERR_RANGE);
            b"-1" => Ok(u64::MAX);
        }
        eq_u64in_spots_hex: {
            b"0xFFFFFFFFFFFFFFFF" => Ok(u64::MAX);
            b"0x10000000000000000" => Err(C_ERR_RANGE);
        }
        eq_u64in_spots_octal: {
            b"01777777777777777777777" => Ok(u64::MAX);
        }
        eq_u64in_spots_misc: {
            b"  42  " => Ok(42);
            b"42x" => Err(C_ERR_SYNTAX);
            b"" => Err(C_ERR_SYNTAX);
        }
    }

    // ============ D3. oidout (1799) / xid8out (5081) ============
    // WRAPPER-LEVEL: shipped fc_oidout / fc_xid8out through a real
    // LocalFcinfo<1> (datum unwrap, thread-local scratch, pg_ultoa_n /
    // pg_ulltoa_n, NUL, pointer Datum — all inside the theorem). C side:
    // snprintf("%u"/"%llu") modeled per SHIM 5 (verbatim emitters + NUL).
    // Bands per intout calibration; the NUL byte IS compared here (both
    // sides write one).

    fn oidout_case(v: u32) {
        let mut cbuf = [0u8; 24];
        let clen = unsafe { pg_oidout(v, cbuf.as_mut_ptr()) } as usize;

        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_oid(v));
        let d = match adt_scalar::builtins::fc_oidout(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("fc_oidout errored"),
        };
        let p = d.as_usize() as *const u8;
        // loop-free compare of the full 12-byte scratch (max "4294967295"
        // + NUL = 11): bytes past clen are 0 in both (TLS zeros / cbuf
        // zeros), so full-width equality subsumes len + NUL parity.
        assert!(clen <= 10);
        let r = unsafe { core::slice::from_raw_parts(p, 12) };
        assert_prefix_eq!(r, cbuf; 0 1 2 3 4 5 6 7 8 9 10 11);
    }

    macro_rules! oidout_band {
        ($($h:ident[$u:literal]: $lo:literal .. $hi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $h() {
                let v: u32 = kani::any();
                kani::assume(v >= $lo && v < $hi);
                oidout_case(v);
            }
        )*};
    }

    // unwind 13: intout's calibrated 32-bit number (see the tidout
    // under-bounding trap note above).
    oidout_band! {
        eq_oidout_r1_lt1e4[13]: 0 .. 10_000;
        eq_oidout_d5[13]: 10_000 .. 100_000;
        eq_oidout_d6[13]: 100_000 .. 1_000_000;
        eq_oidout_d7a[13]: 1_000_000 .. 5_000_000;
        eq_oidout_d7b[13]: 5_000_000 .. 10_000_000;
    }

    /// MANDATORY union-coverage: oidout band split covers exactly [0, 1e7).
    #[kani::proof]
    fn cover_oidout_split() {
        let v: u32 = kani::any();
        kani::assume(v < 10_000_000);
        assert!(
            (v < 10_000)
                || (v >= 10_000 && v < 100_000)
                || (v >= 100_000 && v < 1_000_000)
                || (v >= 1_000_000 && v < 5_000_000)
                || (v >= 5_000_000 && v < 10_000_000)
        );
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_oidout_spots() {
        const SPOTS: [u32; 8] = [
            10_000_000,
            49_999_999,
            50_000_000,
            99_999_999,
            100_000_000,
            999_999_999,
            1_000_000_000,
            u32::MAX,
        ];
        let mut k = 0;
        while k < SPOTS.len() {
            oidout_case(SPOTS[k]);
            k += 1;
        }
    }

    fn xid8out_case(v: u64) {
        let mut cbuf = [0u8; 24];
        let clen = unsafe { pg_xid8out(v, cbuf.as_mut_ptr()) } as usize;

        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_u64(v));
        let d = match adt_scalar::builtins::fc_xid8out(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("fc_xid8out errored"),
        };
        let p = d.as_usize() as *const u8;
        // loop-free compare of the full 21-byte scratch (max u64 20 digits
        // + NUL); bytes past clen are 0 in both.
        assert!(clen <= 20);
        let r = unsafe { core::slice::from_raw_parts(p, 21) };
        assert_prefix_eq!(r, cbuf;
            0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20);
    }

    macro_rules! xid8out_band {
        ($($h:ident[$u:literal]: $lo:literal .. $hi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $h() {
                let v: u64 = kani::any();
                kani::assume(v >= $lo && v < $hi);
                xid8out_case(v);
            }
        )*};
    }

    // 64-bit emitter: unwind(9) per intout calibration (shift loop 8 +
    // <=7 digits in the <1e7 band; slack here is catastrophic).
    xid8out_band! {
        eq_xid8out_r1_lt1e4[9]: 0 .. 10_000;
        eq_xid8out_d5[9]: 10_000 .. 100_000;
        eq_xid8out_d6[9]: 100_000 .. 1_000_000;
        eq_xid8out_d7a[9]: 1_000_000 .. 5_000_000;
        eq_xid8out_d7b[9]: 5_000_000 .. 10_000_000;
    }

    /// MANDATORY union-coverage: xid8out band split covers exactly [0, 1e7).
    #[kani::proof]
    fn cover_xid8out_split() {
        let v: u64 = kani::any();
        kani::assume(v < 10_000_000);
        assert!(
            (v < 10_000)
                || (v >= 10_000 && v < 100_000)
                || (v >= 100_000 && v < 1_000_000)
                || (v >= 1_000_000 && v < 5_000_000)
                || (v >= 5_000_000 && v < 10_000_000)
        );
    }

    /// Spots across the u64 wall region incl. full two-iteration /1e8 loop
    /// exercise and 19/20-digit extremes (intout spot set).
    #[kani::proof]
    #[kani::unwind(22)]
    fn eq_xid8out_spots() {
        const SPOTS: [u64; 10] = [
            10_000_000,
            99_999_999,
            100_000_000, // loop entry
            999_999_999,
            9_999_999_999,
            123_456_789_012,
            9_007_199_254_740_993,
            9_999_999_999_999_999_999,  // 19 digits, 2 loop iterations
            10_000_000_000_000_000_000, // 20 digits
            u64::MAX,
        ];
        let mut k = 0;
        while k < SPOTS.len() {
            xid8out_case(SPOTS[k]);
            k += 1;
        }
    }

    // ============ divergence census (expected FAIL, default solver) ============

    /// PROVED-DIVERGENCE WITNESS (adjudicated bug, see module doc): inside
    /// the empty-field class C tidin ACCEPTS while shipped Rust rejects.
    /// This harness asserts parity WITHOUT the fence on the class C accepts
    /// — it MUST FAIL with a counterexample like "(,5)". Not standing;
    /// default solver.
    #[kani::proof]
    #[kani::unwind(10)]
    fn census_tidin_empty_field() {
        const N: usize = 6;
        let mut bytes = [0u8; N];
        let mut cbuf = [0 as c_char; 32];
        let mut i = 0;
        while i < N {
            let b: u8 = kani::any();
            kani::assume(b >= 1 && b <= 127);
            bytes[i] = b;
            cbuf[i] = b as c_char;
            i += 1;
        }
        kani::assume(!fence_no_empty_tid_field(&bytes[..N]));

        let mut cb: u32 = 0;
        let mut co: u16 = 0;
        let cerr = unsafe { pg_tidin(cbuf.as_ptr(), &mut cb, &mut co) };
        let r = adt_scalar::tidin(&bytes[..N]);
        // parity assertion, deliberately unfenced: fails exactly on the
        // divergence class (C accepts, Rust rejects).
        assert!((cerr == C_OK) == r.is_some());
    }
}



