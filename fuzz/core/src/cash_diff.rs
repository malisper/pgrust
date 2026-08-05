//! cash_diff: differential fuzz driver — shipped adt_cash (money type) vs
//! verbatim vendored PostgreSQL 18.3 C (upstream sha 62d6c7d3df, cash.c;
//! oracle in csrc/pg_cash_io.c).
//!
//! ============================ C-LOCALE FENCE ============================
//! cash.c is locale-adjacent. Both sides of this differential run under
//! C-locale monetary conventions: the shipped Rust reads
//! pg_locale::pglc_localeconv(), which serves the static C-locale PgLconv
//! (all symbols "", all char fields CHAR_MAX) because the fuzz process
//! never sets lc_monetary/lc_numeric; the C oracle pins the identical
//! values through its PGLC_localeconv() seam (the proofs/cash/c/pg_cash.c
//! pglc seam pattern). So the compared behavior is cash.c's fallback arms:
//! dsymbol '.', ssymbol ",", csymbol "$", psymbol "+", nsymbol "-",
//! fpoint 2, mon_group 3, default sign_posn/cs_precedes/no-sep-space.
//! The shipped Rust exposes NO fpoint/frac_digits parameter on
//! cash_in/cash_out (they read the process lconv), so non-default fpoint
//! is not a reachable shipped surface and is not fuzzed here (the Kani
//! harnesses in proofs/cash quantify over the lconv fields instead).
//! ========================================================================
//!
//! Comparator planes (message text out of scope): value bits/bytes
//! (i64 / f64-to_bits / exact output byte image), error-verdict
//! (err vs ok), and errcode class (22003 / 22012 / 22P02 / 08P01).
//!
//! Function inventory (dispatch-grain, selector % 21):
//!   0 cash_in            8 cash_div_int4*     16 cmp family + larger/smaller
//!   1 cash_out           9 cash_mul_int2      17 int4_cash
//!   2 cash_words        10 cash_div_int2*     18 int8_cash
//!   3 cash_pl           11 cash_mul_flt8      19 cash_recv
//!   4 cash_mi           12 cash_div_flt8      20 cash_send (+recv roundtrip)
//!   5 cash_mul_int64    13 cash_mul_flt4
//!   6 cash_div_int64*   14 cash_div_flt4
//!   7 cash_mul_int4*
//! (*) division arms carry the MIN/-1 carve below. The int8_mul_cash /
//! int4_mul_cash / int2_mul_cash / flt8_mul_cash / flt4_mul_cash swapped
//! arg-order builtins reduce to the same commutative cores driven here (and
//! their fc_* wrappers execute in the fc plane with swapped Datum args).
//! Skipped: cash_numeric / numeric_cash and their fc_cash_numeric /
//! fc_numeric_cash wrappers (parked DigitBuf proofs-ledger rows; the
//! numeric comparison plane is owned by that lane).
//!
//! fc-wrapper plane: every selector arm ALSO routes its input through the
//! crate's builtins.rs fmgr wrappers (fc_*) on a native LocalFcinfo frame
//! (the proofs/uuid wrapper-level pattern, run natively with a real
//! mcx::MemoryContext), asserting wrapper ≡ the already-C-checked outcome
//! (i64/f64-bits/byte-image values, error verdict + class). Cash args ride
//! as i64 Datums; cash_in takes a cstring Datum with no soft-error context.
//!
//! KNOWN-DIVERGENCE CARVE (proofs ledger rows 865/867/3345): cash_div_int2/
//! int4/int8 at c == i64::MIN, divisor == -1. C cash_div_int64 lacks the
//! guard (2024 sweep 4f96281587 covered mul + float only): x86-64 traps
//! SIGFPE (server crash), aarch64 silently returns i64::MIN. pgrust raises
//! 22003 like int8div (ruling 2026-07-29; reported upstream). The carve is
//! EXACTLY that one cell per division arm; the comparator is at full
//! strength everywhere else.

use std::ffi::{c_char, CStr, CString};

use adt_cash::builtins as cb;
use datum::{Datum, NullableDatum};
use types_error::{
    PgError, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROTOCOL_VIOLATION,
};
use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    fn pg_diff_cash_in(str_: *const c_char, err: *mut i32) -> i64;
    fn pg_diff_cash_out(value: i64, out: *mut c_char) -> i32;
    fn pg_diff_cash_words(value: i64, out: *mut c_char) -> i32;
    fn pg_diff_cash_eq(c1: i64, c2: i64) -> i32;
    fn pg_diff_cash_ne(c1: i64, c2: i64) -> i32;
    fn pg_diff_cash_lt(c1: i64, c2: i64) -> i32;
    fn pg_diff_cash_le(c1: i64, c2: i64) -> i32;
    fn pg_diff_cash_gt(c1: i64, c2: i64) -> i32;
    fn pg_diff_cash_ge(c1: i64, c2: i64) -> i32;
    fn pg_diff_cash_cmp(c1: i64, c2: i64) -> i32;
    fn pg_diff_cash_pl(c1: i64, c2: i64, out: *mut i64) -> i32;
    fn pg_diff_cash_mi(c1: i64, c2: i64, out: *mut i64) -> i32;
    fn pg_diff_cash_mul_flt8(c: i64, f: f64, out: *mut i64) -> i32;
    fn pg_diff_cash_div_flt8(c: i64, f: f64, out: *mut i64) -> i32;
    fn pg_diff_cash_mul_flt4(c: i64, f: f32, out: *mut i64) -> i32;
    fn pg_diff_cash_div_flt4(c: i64, f: f32, out: *mut i64) -> i32;
    fn pg_diff_cash_mul_int64(c: i64, i: i64, out: *mut i64) -> i32;
    fn pg_diff_cash_div_int64(c: i64, i: i64, out: *mut i64) -> i32;
    fn pg_diff_cash_mul_int4(c: i64, i: i32, out: *mut i64) -> i32;
    fn pg_diff_cash_div_int4(c: i64, i: i32, out: *mut i64) -> i32;
    fn pg_diff_cash_mul_int2(c: i64, s: i16, out: *mut i64) -> i32;
    fn pg_diff_cash_div_int2(c: i64, s: i16, out: *mut i64) -> i32;
    fn pg_diff_cash_div_cash(dividend: i64, divisor: i64, out: *mut f64) -> i32;
    fn pg_diff_cashlarger(c1: i64, c2: i64) -> i64;
    fn pg_diff_cashsmaller(c1: i64, c2: i64) -> i64;
    fn pg_diff_int4_cash(amount: i32, out: *mut i64) -> i32;
    fn pg_diff_int8_cash(amount: i64, out: *mut i64) -> i32;
    fn pg_diff_cash_recv(buf: *const u8, len: u64, out: *mut i64) -> i32;
    fn pg_diff_cash_send(arg1: i64, out8: *mut u8) -> i32;
}

/// Oracle error classes (must match the defines in csrc/pg_cash_io.c).
const C_ERR_OUT_OF_RANGE: i32 = 1; /* 22003 */
const C_ERR_DIV_ZERO: i32 = 2; /* 22012 */
const C_ERR_INVALID_TEXT: i32 = 3; /* 22P02 */
const C_ERR_PROTOCOL: i32 = 4; /* 08P01 */

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        C_ERR_OUT_OF_RANGE
    } else if e.sqlstate == ERRCODE_DIVISION_BY_ZERO {
        C_ERR_DIV_ZERO
    } else if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
        C_ERR_PROTOCOL
    } else {
        99
    }
}

/// Three-plane comparator for the i64-result arithmetic family.
fn compare_i64(name: &str, cerr: i32, cval: i64, rres: types_error::PgResult<i64>, dbg: &str) {
    match rres {
        Ok(r) => assert!(
            cerr == 0 && r == cval,
            "{name} DIVERGENCE {dbg}: C=(err {cerr}, {cval}) Rust=Ok({r})"
        ),
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cerr == rerr,
                "{name} DIVERGENCE {dbg}: C=(err {cerr}, {cval}) Rust=Err({rerr} {})",
                e.message
            );
        }
    }
}

fn le_i64(b: &[u8]) -> i64 {
    i64::from_le_bytes(b[..8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (deterministic; result allocations ride the
// caller's arena and die with it, no hot-loop leaks).
// ---------------------------------------------------------------------------

/// One native fmgr call: N value-arg Datums, optional armed result mcx.
/// Wrappers under test never return SQL NULL.
fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    mcx: Option<mcx::Mcx<'_>>,
    args: [Datum; N],
) -> types_error::PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    if let Some(m) = mcx {
        // SAFETY: the arming context outlives this single call.
        unsafe { fcinfo.set_result_mcx(m) };
    }
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(flinfo, &mut fcinfo);
    if r.is_ok() {
        assert!(!fcinfo.isnull, "fc wrapper returned SQL NULL unexpectedly");
    }
    r
}

/// Three-plane wrapper check for i64-Datum-returning fc arms against the
/// already-core-checked C outcome (wrapper ≡ C ≡ core).
fn fc_check_i64<const N: usize>(
    name: &str,
    f: PGFunction,
    args: [Datum; N],
    cerr: i32,
    cval: i64,
    dbg: &str,
) {
    match fc_call(f, None, None, args) {
        Ok(d) => assert!(
            cerr == 0 && d.as_i64() == cval,
            "{name} fc-wrapper DIVERGENCE {dbg}: C=(err {cerr}, {cval}) wrapper=Ok({})",
            d.as_i64()
        ),
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cerr == rerr,
                "{name} fc-wrapper DIVERGENCE {dbg}: C err {cerr} vs wrapper err {rerr} ({})",
                e.message
            );
        }
    }
}

/// Varlena result readback (text/bytea payload bytes).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

pub fn cash_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    match sel % 21 {
        // ---- cash_in: locale-adjacent text parse (C-locale fence above) ----
        0 => {
            if rest.len() > 64 || rest.contains(&0) {
                return;
            }
            let Ok(s) = std::str::from_utf8(rest) else {
                return;
            };
            let cs = CString::new(rest).unwrap();
            let mut cerr = 0i32;
            let cval = unsafe { pg_diff_cash_in(cs.as_ptr(), &mut cerr) };
            match adt_cash::cash_in(s, None) {
                Ok(r) => assert!(
                    cerr == 0 && r == cval,
                    "cash_in DIVERGENCE input={s:?}: C=(err {cerr}, {cval}) Rust=Ok({r})"
                ),
                Err(e) => {
                    let rerr = rust_err_class(&e);
                    assert!(
                        cerr == rerr,
                        "cash_in DIVERGENCE input={s:?}: C=(err {cerr}, {cval}) Rust=Err({rerr} {})",
                        e.message
                    );
                }
            }
            // fc-wrapper plane: fc_cash_in over the same cstring (esc = None,
            // hard-error path; wrapper ≡ C ≡ core).
            fc_check_i64(
                "fc_cash_in",
                cb::fc_cash_in,
                [Datum::from_usize(cs.as_ptr() as usize)],
                cerr,
                cval,
                &format!("input={s:?}"),
            );
        }
        // ---- cash_out: exact byte-image parity (points/grouping arms) ----
        1 => {
            if rest.len() < 8 {
                return;
            }
            let v = le_i64(rest);
            let mut cbuf = [0u8; 192];
            let clen = unsafe { pg_diff_cash_out(v, cbuf.as_mut_ptr().cast()) } as usize;
            let mut rbuf = [0u8; adt_cash::CASH_OUT_BUFLEN];
            // Infallible under the C locale (pglc_localeconv cannot fail there).
            let rlen = adt_cash::cash_out_into(v, &mut rbuf).expect("cash_out C-locale");
            assert!(
                cbuf[..clen] == rbuf[..rlen],
                "cash_out DIVERGENCE value={v}: C={:?} Rust={:?}",
                std::str::from_utf8(&cbuf[..clen]),
                std::str::from_utf8(&rbuf[..rlen])
            );
            // fc-wrapper plane: fc_cash_out cstring image (TLS scratch).
            let d = fc_call(cb::fc_cash_out, None, None, [Datum::from_i64(v)])
                .expect("fc_cash_out C-locale");
            // SAFETY: fc_cash_out returns the live NUL-terminated TLS scratch.
            let out = unsafe { CStr::from_ptr(d.as_usize() as *const c_char) };
            assert!(
                out.to_bytes() == &rbuf[..rlen],
                "fc_cash_out vs core DIVERGENCE value={v}: wrapper={:?}",
                out
            );
        }
        // ---- cash_words: exact text-image parity ----
        2 => {
            if rest.len() < 8 {
                return;
            }
            let v = le_i64(rest);
            let mut cbuf = [0u8; 512];
            let clen = unsafe { pg_diff_cash_words(v, cbuf.as_mut_ptr().cast()) } as usize;
            let cx = mcx::MemoryContext::new("cash_fuzz");
            let text = adt_cash::cash_words(cx.mcx(), v).expect("cash_words is infallible");
            assert!(
                cbuf[..clen] == *text.data(),
                "cash_words DIVERGENCE value={v}: C={:?} Rust={:?}",
                std::str::from_utf8(&cbuf[..clen]),
                std::str::from_utf8(text.data())
            );
            // fc-wrapper plane: fc_cash_words text image (wrapper ≡ core).
            let d = fc_call(cb::fc_cash_words, None, Some(cx.mcx()), [Datum::from_i64(v)])
                .expect("fc_cash_words is infallible");
            assert!(
                read_varlena_data(d) == text.data(),
                "fc_cash_words vs core DIVERGENCE value={v}"
            );
        }
        // ---- cash_pl / cash_mi ----
        3 => {
            if rest.len() < 16 {
                return;
            }
            let (a, b) = (le_i64(rest), le_i64(&rest[8..]));
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_pl(a, b, &mut cval) };
            compare_i64("cash_pl", cerr, cval, adt_cash::cash_pl(a, b), &format!("a={a} b={b}"));
            let dbg = format!("a={a} b={b}");
            let (da, db) = (Datum::from_i64(a), Datum::from_i64(b));
            fc_check_i64("fc_cash_pl", cb::fc_cash_pl, [da, db], cerr, cval, &dbg);
        }
        4 => {
            if rest.len() < 16 {
                return;
            }
            let (a, b) = (le_i64(rest), le_i64(&rest[8..]));
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_mi(a, b, &mut cval) };
            compare_i64("cash_mi", cerr, cval, adt_cash::cash_mi(a, b), &format!("a={a} b={b}"));
            let dbg = format!("a={a} b={b}");
            let (da, db) = (Datum::from_i64(a), Datum::from_i64(b));
            fc_check_i64("fc_cash_mi", cb::fc_cash_mi, [da, db], cerr, cval, &dbg);
        }
        // ---- cash_mul_int8 / cash_div_int8 (i64 core) ----
        5 => {
            if rest.len() < 16 {
                return;
            }
            let (c, i) = (le_i64(rest), le_i64(&rest[8..]));
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_mul_int64(c, i, &mut cval) };
            compare_i64(
                "cash_mul_int64",
                cerr,
                cval,
                adt_cash::cash_mul_int64(c, i),
                &format!("c={c} i={i}"),
            );
            let dbg = format!("c={c} i={i}");
            let (dc, di) = (Datum::from_i64(c), Datum::from_i64(i));
            fc_check_i64("fc_cash_mul_int8", cb::fc_cash_mul_int8, [dc, di], cerr, cval, &dbg);
            fc_check_i64("fc_int8_mul_cash", cb::fc_int8_mul_cash, [di, dc], cerr, cval, &dbg);
        }
        6 => {
            if rest.len() < 16 {
                return;
            }
            let (c, i) = (le_i64(rest), le_i64(&rest[8..]));
            if c == i64::MIN && i == -1 {
                // KNOWN-DIVERGENCE CARVE, ledger rows 865/867/3345: C is
                // platform-dependent UB here (see module header); pgrust
                // raises 22003. Never call the oracle with this cell.
                assert_eq!(
                    adt_cash::cash_div_int64(c, i).err().map(|e| rust_err_class(&e)),
                    Some(C_ERR_OUT_OF_RANGE),
                    "carved MIN/-1 cell must stay a 22003 error in pgrust"
                );
                // The wrapper carries the same carved verdict (oracle
                // still never called on this cell).
                assert_eq!(
                    fc_call(cb::fc_cash_div_int8, None, None, [Datum::from_i64(c), Datum::from_i64(i)])
                        .err()
                        .map(|e| rust_err_class(&e)),
                    Some(C_ERR_OUT_OF_RANGE),
                    "carved MIN/-1 cell must stay 22003 through fc_cash_div_int8"
                );
                return;
            }
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_div_int64(c, i, &mut cval) };
            compare_i64(
                "cash_div_int64",
                cerr,
                cval,
                adt_cash::cash_div_int64(c, i),
                &format!("c={c} i={i}"),
            );
            fc_check_i64(
                "fc_cash_div_int8",
                cb::fc_cash_div_int8,
                [Datum::from_i64(c), Datum::from_i64(i)],
                cerr,
                cval,
                &format!("c={c} i={i}"),
            );
        }
        // ---- cash_mul_int4 / cash_div_int4 (C wrapper casts i32 -> i64) ----
        7 => {
            if rest.len() < 12 {
                return;
            }
            let c = le_i64(rest);
            let i = i32::from_le_bytes(rest[8..12].try_into().unwrap());
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_mul_int4(c, i, &mut cval) };
            compare_i64(
                "cash_mul_int4",
                cerr,
                cval,
                adt_cash::cash_mul_int64(c, i as i64),
                &format!("c={c} i={i}"),
            );
            let dbg = format!("c={c} i={i}");
            let (dc, di) = (Datum::from_i64(c), Datum::from_i32(i));
            fc_check_i64("fc_cash_mul_int4", cb::fc_cash_mul_int4, [dc, di], cerr, cval, &dbg);
            fc_check_i64("fc_int4_mul_cash", cb::fc_int4_mul_cash, [di, dc], cerr, cval, &dbg);
        }
        8 => {
            if rest.len() < 12 {
                return;
            }
            let c = le_i64(rest);
            let i = i32::from_le_bytes(rest[8..12].try_into().unwrap());
            if c == i64::MIN && i == -1 {
                // KNOWN-DIVERGENCE CARVE, ledger row 865 (cash_div_int4).
                assert_eq!(
                    adt_cash::cash_div_int64(c, i as i64).err().map(|e| rust_err_class(&e)),
                    Some(C_ERR_OUT_OF_RANGE),
                );
                assert_eq!(
                    fc_call(cb::fc_cash_div_int4, None, None, [Datum::from_i64(c), Datum::from_i32(i)])
                        .err()
                        .map(|e| rust_err_class(&e)),
                    Some(C_ERR_OUT_OF_RANGE),
                    "carved MIN/-1 cell must stay 22003 through fc_cash_div_int4"
                );
                return;
            }
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_div_int4(c, i, &mut cval) };
            compare_i64(
                "cash_div_int4",
                cerr,
                cval,
                adt_cash::cash_div_int64(c, i as i64),
                &format!("c={c} i={i}"),
            );
            fc_check_i64(
                "fc_cash_div_int4",
                cb::fc_cash_div_int4,
                [Datum::from_i64(c), Datum::from_i32(i)],
                cerr,
                cval,
                &format!("c={c} i={i}"),
            );
        }
        // ---- cash_mul_int2 / cash_div_int2 (C wrapper casts i16 -> i64) ----
        9 => {
            if rest.len() < 10 {
                return;
            }
            let c = le_i64(rest);
            let s = i16::from_le_bytes(rest[8..10].try_into().unwrap());
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_mul_int2(c, s, &mut cval) };
            compare_i64(
                "cash_mul_int2",
                cerr,
                cval,
                adt_cash::cash_mul_int64(c, s as i64),
                &format!("c={c} s={s}"),
            );
            let dbg = format!("c={c} s={s}");
            let (dc, ds) = (Datum::from_i64(c), Datum::from_i16(s));
            fc_check_i64("fc_cash_mul_int2", cb::fc_cash_mul_int2, [dc, ds], cerr, cval, &dbg);
            fc_check_i64("fc_int2_mul_cash", cb::fc_int2_mul_cash, [ds, dc], cerr, cval, &dbg);
        }
        10 => {
            if rest.len() < 10 {
                return;
            }
            let c = le_i64(rest);
            let s = i16::from_le_bytes(rest[8..10].try_into().unwrap());
            if c == i64::MIN && s == -1 {
                // KNOWN-DIVERGENCE CARVE, ledger row 867 (cash_div_int2).
                assert_eq!(
                    adt_cash::cash_div_int64(c, s as i64).err().map(|e| rust_err_class(&e)),
                    Some(C_ERR_OUT_OF_RANGE),
                );
                assert_eq!(
                    fc_call(cb::fc_cash_div_int2, None, None, [Datum::from_i64(c), Datum::from_i16(s)])
                        .err()
                        .map(|e| rust_err_class(&e)),
                    Some(C_ERR_OUT_OF_RANGE),
                    "carved MIN/-1 cell must stay 22003 through fc_cash_div_int2"
                );
                return;
            }
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_div_int2(c, s, &mut cval) };
            compare_i64(
                "cash_div_int2",
                cerr,
                cval,
                adt_cash::cash_div_int64(c, s as i64),
                &format!("c={c} s={s}"),
            );
            fc_check_i64(
                "fc_cash_div_int2",
                cb::fc_cash_div_int2,
                [Datum::from_i64(c), Datum::from_i16(s)],
                cerr,
                cval,
                &format!("c={c} s={s}"),
            );
        }
        // ---- cash_mul_flt8 / cash_div_flt8 ----
        11 => {
            if rest.len() < 16 {
                return;
            }
            let c = le_i64(rest);
            let f = f64::from_le_bytes(rest[8..16].try_into().unwrap());
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_mul_flt8(c, f, &mut cval) };
            compare_i64(
                "cash_mul_flt8",
                cerr,
                cval,
                adt_cash::cash_mul_float8(c, f),
                &format!("c={c} f={f:e}[{:016x}]", f.to_bits()),
            );
            let dbg = format!("c={c} f={f:e}[{:016x}]", f.to_bits());
            let (dc, df) = (Datum::from_i64(c), Datum::from_f64(f));
            fc_check_i64("fc_cash_mul_flt8", cb::fc_cash_mul_flt8, [dc, df], cerr, cval, &dbg);
            fc_check_i64("fc_flt8_mul_cash", cb::fc_flt8_mul_cash, [df, dc], cerr, cval, &dbg);
        }
        12 => {
            if rest.len() < 16 {
                return;
            }
            let c = le_i64(rest);
            let f = f64::from_le_bytes(rest[8..16].try_into().unwrap());
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_div_flt8(c, f, &mut cval) };
            compare_i64(
                "cash_div_flt8",
                cerr,
                cval,
                adt_cash::cash_div_float8(c, f),
                &format!("c={c} f={f:e}[{:016x}]", f.to_bits()),
            );
            fc_check_i64(
                "fc_cash_div_flt8",
                cb::fc_cash_div_flt8,
                [Datum::from_i64(c), Datum::from_f64(f)],
                cerr,
                cval,
                &format!("c={c} f={f:e}[{:016x}]", f.to_bits()),
            );
        }
        // ---- cash_mul_flt4 / cash_div_flt4 (wrapper casts f32 -> f64) ----
        13 => {
            if rest.len() < 12 {
                return;
            }
            let c = le_i64(rest);
            let f = f32::from_le_bytes(rest[8..12].try_into().unwrap());
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_mul_flt4(c, f, &mut cval) };
            compare_i64(
                "cash_mul_flt4",
                cerr,
                cval,
                adt_cash::cash_mul_float8(c, f as f64),
                &format!("c={c} f={f:e}[{:08x}]", f.to_bits()),
            );
            let dbg = format!("c={c} f={f:e}[{:08x}]", f.to_bits());
            let (dc, df) = (Datum::from_i64(c), Datum::from_f32(f));
            fc_check_i64("fc_cash_mul_flt4", cb::fc_cash_mul_flt4, [dc, df], cerr, cval, &dbg);
            fc_check_i64("fc_flt4_mul_cash", cb::fc_flt4_mul_cash, [df, dc], cerr, cval, &dbg);
        }
        14 => {
            if rest.len() < 12 {
                return;
            }
            let c = le_i64(rest);
            let f = f32::from_le_bytes(rest[8..12].try_into().unwrap());
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_div_flt4(c, f, &mut cval) };
            compare_i64(
                "cash_div_flt4",
                cerr,
                cval,
                adt_cash::cash_div_float8(c, f as f64),
                &format!("c={c} f={f:e}[{:08x}]", f.to_bits()),
            );
            fc_check_i64(
                "fc_cash_div_flt4",
                cb::fc_cash_div_flt4,
                [Datum::from_i64(c), Datum::from_f32(f)],
                cerr,
                cval,
                &format!("c={c} f={f:e}[{:08x}]", f.to_bits()),
            );
        }
        // ---- cash_div_cash: f64 quotient, exact bits ----
        15 => {
            if rest.len() < 16 {
                return;
            }
            let (a, b) = (le_i64(rest), le_i64(&rest[8..]));
            let mut cval = 0f64;
            let cerr = unsafe { pg_diff_cash_div_cash(a, b, &mut cval) };
            match adt_cash::cash_div_cash(a, b) {
                Ok(r) => assert!(
                    cerr == 0 && r.to_bits() == cval.to_bits(),
                    "cash_div_cash DIVERGENCE a={a} b={b}: C=(err {cerr}, {cval:e}) Rust=Ok({r:e})"
                ),
                Err(e) => {
                    let rerr = rust_err_class(&e);
                    assert!(
                        cerr == rerr,
                        "cash_div_cash DIVERGENCE a={a} b={b}: C err {cerr} vs Rust err {rerr} ({})",
                        e.message
                    );
                }
            }
            // fc-wrapper plane: f64-Datum result, exact bits.
            match fc_call(cb::fc_cash_div_cash, None, None, [Datum::from_i64(a), Datum::from_i64(b)]) {
                Ok(d) => assert!(
                    cerr == 0 && d.as_f64().to_bits() == cval.to_bits(),
                    "fc_cash_div_cash DIVERGENCE a={a} b={b}: C=(err {cerr}, {cval:e}) wrapper=Ok({:e})",
                    d.as_f64()
                ),
                Err(e) => {
                    let rerr = rust_err_class(&e);
                    assert!(
                        cerr == rerr,
                        "fc_cash_div_cash DIVERGENCE a={a} b={b}: C err {cerr} vs wrapper err {rerr} ({})",
                        e.message
                    );
                }
            }
        }
        // ---- comparison family + cashlarger/cashsmaller (all in one arm) ----
        16 => {
            if rest.len() < 16 {
                return;
            }
            let (a, b) = (le_i64(rest), le_i64(&rest[8..]));
            unsafe {
                assert_eq!(adt_cash::cash_eq(a, b) as i32, pg_diff_cash_eq(a, b), "cash_eq {a} {b}");
                assert_eq!(adt_cash::cash_ne(a, b) as i32, pg_diff_cash_ne(a, b), "cash_ne {a} {b}");
                assert_eq!(adt_cash::cash_lt(a, b) as i32, pg_diff_cash_lt(a, b), "cash_lt {a} {b}");
                assert_eq!(adt_cash::cash_le(a, b) as i32, pg_diff_cash_le(a, b), "cash_le {a} {b}");
                assert_eq!(adt_cash::cash_gt(a, b) as i32, pg_diff_cash_gt(a, b), "cash_gt {a} {b}");
                assert_eq!(adt_cash::cash_ge(a, b) as i32, pg_diff_cash_ge(a, b), "cash_ge {a} {b}");
                assert_eq!(adt_cash::cash_cmp(a, b), pg_diff_cash_cmp(a, b), "cash_cmp {a} {b}");
                assert_eq!(adt_cash::cashlarger(a, b), pg_diff_cashlarger(a, b), "cashlarger {a} {b}");
                assert_eq!(
                    adt_cash::cashsmaller(a, b),
                    pg_diff_cashsmaller(a, b),
                    "cashsmaller {a} {b}"
                );
            }
            // ---- fc-wrapper plane (wrapper ≡ core; all infallible) ----
            let (da, db) = (Datum::from_i64(a), Datum::from_i64(b));
            let fc_bools: [(&str, PGFunction, bool); 6] = [
                ("fc_cash_eq", cb::fc_cash_eq, adt_cash::cash_eq(a, b)),
                ("fc_cash_ne", cb::fc_cash_ne, adt_cash::cash_ne(a, b)),
                ("fc_cash_lt", cb::fc_cash_lt, adt_cash::cash_lt(a, b)),
                ("fc_cash_le", cb::fc_cash_le, adt_cash::cash_le(a, b)),
                ("fc_cash_gt", cb::fc_cash_gt, adt_cash::cash_gt(a, b)),
                ("fc_cash_ge", cb::fc_cash_ge, adt_cash::cash_ge(a, b)),
            ];
            for (fname, f, want) in fc_bools {
                let d = fc_call(f, None, None, [da, db]).expect("cash cmp wrappers are infallible");
                assert!(
                    d.as_bool() == want,
                    "{fname} fc-wrapper DIVERGENCE a={a} b={b}: wrapper={} core={want}",
                    d.as_bool()
                );
            }
            let d = fc_call(cb::fc_cash_cmp, None, None, [da, db]).expect("fc_cash_cmp");
            assert_eq!(d.as_i32(), adt_cash::cash_cmp(a, b), "fc_cash_cmp {a} {b}");
            let d = fc_call(cb::fc_cashlarger, None, None, [da, db]).expect("fc_cashlarger");
            assert_eq!(d.as_i64(), adt_cash::cashlarger(a, b), "fc_cashlarger {a} {b}");
            let d = fc_call(cb::fc_cashsmaller, None, None, [da, db]).expect("fc_cashsmaller");
            assert_eq!(d.as_i64(), adt_cash::cashsmaller(a, b), "fc_cashsmaller {a} {b}");
        }
        // ---- int4_cash / int8_cash (via int8mul, "bigint out of range") ----
        17 => {
            if rest.len() < 4 {
                return;
            }
            let a = i32::from_le_bytes(rest[..4].try_into().unwrap());
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_int4_cash(a, &mut cval) };
            compare_i64("int4_cash", cerr, cval, adt_cash::int4_cash(a), &format!("amount={a}"));
            fc_check_i64(
                "fc_int4_cash",
                cb::fc_int4_cash,
                [Datum::from_i32(a)],
                cerr,
                cval,
                &format!("amount={a}"),
            );
        }
        18 => {
            if rest.len() < 8 {
                return;
            }
            let a = le_i64(rest);
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_int8_cash(a, &mut cval) };
            compare_i64("int8_cash", cerr, cval, adt_cash::int8_cash(a), &format!("amount={a}"));
            fc_check_i64(
                "fc_int8_cash",
                cb::fc_int8_cash,
                [Datum::from_i64(a)],
                cerr,
                cval,
                &format!("amount={a}"),
            );
        }
        // ---- cash_recv: big-endian wire decode incl. short-message error ----
        19 => {
            let msg = if rest.len() > 16 { &rest[..16] } else { rest };
            let mut cval = 0i64;
            let cerr = unsafe { pg_diff_cash_recv(msg.as_ptr(), msg.len() as u64, &mut cval) };
            let cx = mcx::MemoryContext::new("cash_fuzz");
            let mcx = cx.mcx();
            let Ok(mut vec) = mcx::vec_with_capacity_in::<u8>(mcx, msg.len()) else {
                return;
            };
            if mcx::vec_append_bytes(&mut vec, msg).is_err() {
                return;
            }
            let Ok(mut si) = stringinfo::StringInfo::from_vec(vec) else {
                return;
            };
            compare_i64(
                "cash_recv",
                cerr,
                cval,
                adt_cash::cash_recv(&mut si),
                &format!("msg={msg:02x?}"),
            );
            // fc-wrapper plane over its own StringInfo image of the same
            // wire bytes (wrapper ≡ C ≡ core).
            let Ok(mut vec2) = mcx::vec_with_capacity_in::<u8>(mcx, msg.len()) else {
                return;
            };
            if mcx::vec_append_bytes(&mut vec2, msg).is_err() {
                return;
            }
            let Ok(mut si2) = stringinfo::StringInfo::from_vec(vec2) else {
                return;
            };
            let dsi = Datum::from_usize(core::ptr::from_mut(&mut si2) as usize);
            fc_check_i64("fc_cash_recv", cb::fc_cash_recv, [dsi], cerr, cval, &format!("msg={msg:02x?}"));
        }
        // ---- cash_send: big-endian wire image + recv∘send == id ----
        _ => {
            if rest.len() < 8 {
                return;
            }
            let v = le_i64(rest);
            let mut cimg = [0u8; 8];
            unsafe { pg_diff_cash_send(v, cimg.as_mut_ptr()) };
            let cx = mcx::MemoryContext::new("cash_fuzz");
            let mcx = cx.mcx();
            let bytes = adt_cash::cash_send(mcx, v).expect("cash_send is infallible");
            assert!(
                *bytes.data() == cimg,
                "cash_send DIVERGENCE value={v}: C={cimg:02x?} Rust={:02x?}",
                bytes.data()
            );
            // recv(send(v)) == v through the shipped recv path.
            let Ok(mut vec) = mcx::vec_with_capacity_in::<u8>(mcx, 8) else {
                return;
            };
            if mcx::vec_append_bytes(&mut vec, bytes.data()).is_err() {
                return;
            }
            if let Ok(mut si) = stringinfo::StringInfo::from_vec(vec) {
                assert_eq!(adt_cash::cash_recv(&mut si).ok(), Some(v), "cash send/recv roundtrip");
            };
            // fc-wrapper plane: fc_cash_send bytea image (wrapper ≡ C ≡ core).
            let d = fc_call(cb::fc_cash_send, None, Some(mcx), [Datum::from_i64(v)])
                .expect("fc_cash_send is infallible");
            assert!(
                read_varlena_data(d) == cimg,
                "fc_cash_send DIVERGENCE value={v}: C={cimg:02x?} wrapper={:02x?}",
                read_varlena_data(d)
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke tests: drive every arm over an edge-case grid so
// `cargo test` exercises the C link + comparators without cargo-fuzz. The
// same grids seed the libFuzzer corpus (fuzz/corpus/cash_diff).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Money-literal corpus: signs, parens, currency symbols, separators,
    /// rounding digit, i64-boundary magnitudes, junk.
    pub const CASH_STR_CORPUS: &[&str] = &[
        "0",
        "1",
        "-1",
        "$1,234.56",
        "($123.45)",
        "( $ 1.5 )",
        "-$92233720368547758.08",
        "$-92233720368547758.08",
        "(92233720368547758.08)",
        "-92233720368547758.09",
        "92233720368547758.07",
        "92233720368547758.08",
        "92233720368547758.075",
        "92233720368547758.074",
        "-92233720368547758.085",
        "922337203685477580.8",
        "9223372036854775807",
        "-9223372036854775808",
        "+$0.99",
        " \t $ + 12,345.678 ",
        "$$1",
        "1$",
        "1 $",
        "1)",
        "((1",
        "1.2.3",
        "1,,,2",
        "0.005",
        "0.004",
        ".5",
        ".",
        ",",
        "$",
        "",
        " ",
        "-",
        "+",
        "()",
        "(1)-",
        "1-",
        "1+",
        "abc",
        "12abc",
        "12 34",
        "1e5",
        "0.99999999999",
        "123.4 )  $ - ",
        "\u{00a0}1",
        "١٢٣",
    ];

    pub const CASH_VAL_CORPUS: &[i64] = &[
        0,
        1,
        -1,
        99,
        100,
        101,
        150,
        199,
        -100,
        1000,
        1010,
        1100,
        2000,
        110000,
        111213,
        123456789,
        10_000_000_000,
        100_000_000_000_000,
        100_000_000_000_000_000,
        i64::MAX,
        i64::MIN,
        i64::MIN + 1,
        i64::MAX - 1,
        i64::MAX / 2 + 1,
        -4611686018427387904,
        3037000499, /* isqrt(i64::MAX) neighborhood for mul overflow */
        3037000500,
        -3037000500,
    ];

    fn drive(sel: u8, payload: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(payload);
        cash_diff(&d);
    }

    #[test]
    fn cash_in_corpus() {
        let _serial = crate::c_oracle_serial();
        for s in CASH_STR_CORPUS {
            drive(0, s.as_bytes());
        }
    }

    #[test]
    fn cash_out_words_send_corpus() {
        let _serial = crate::c_oracle_serial();
        for &v in CASH_VAL_CORPUS {
            drive(1, &v.to_le_bytes());
            drive(2, &v.to_le_bytes());
            drive(20, &v.to_le_bytes());
            drive(17, &(v as i32).to_le_bytes());
            drive(18, &v.to_le_bytes());
        }
    }

    #[test]
    fn cash_pair_arms_corpus() {
        let _serial = crate::c_oracle_serial();
        for &a in CASH_VAL_CORPUS {
            for &b in CASH_VAL_CORPUS {
                let mut p = a.to_le_bytes().to_vec();
                p.extend_from_slice(&b.to_le_bytes());
                for sel in [3u8, 4, 5, 6, 15, 16] {
                    drive(sel, &p);
                }
                // narrow-int arms read the low 4/2 bytes of b's slot
                for sel in [7u8, 8, 9, 10] {
                    drive(sel, &p);
                }
            }
        }
    }

    #[test]
    fn cash_float_arms_corpus() {
        let _serial = crate::c_oracle_serial();
        let floats: &[f64] = &[
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.5,
            -0.5,
            1.5,
            2.5,
            0.1,
            100.0,
            1e18,
            -1e18,
            1e300,
            5e-324,
            9.223372036854776e18, /* 2^63 boundary */
            -9.223372036854776e18,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ];
        for &c in CASH_VAL_CORPUS {
            for &f in floats {
                let mut p = c.to_le_bytes().to_vec();
                p.extend_from_slice(&f.to_le_bytes());
                drive(11, &p);
                drive(12, &p);
                let mut p4 = c.to_le_bytes().to_vec();
                p4.extend_from_slice(&(f as f32).to_le_bytes());
                drive(13, &p4);
                drive(14, &p4);
            }
        }
    }

    #[test]
    fn cash_recv_corpus() {
        let _serial = crate::c_oracle_serial();
        drive(19, b"");
        drive(19, &[1, 2, 3]);
        drive(19, &[0; 7]); /* short -> 08P01 both sides */
        drive(19, &[0x80, 0, 0, 0, 0, 0, 0, 0]); /* i64::MIN */
        drive(19, &[0xff; 12]);
        drive(19, &[0, 0, 0, 0, 0, 0, 0, 42, 9, 9]);
    }

    /// fc-wrapper plane smoke: every fc arm executes over seeds — value,
    /// error (overflow / div-zero / 22P02 / short-recv 08P01), and swapped
    /// arg-order wrappers included.
    #[test]
    fn fc_wrapper_plane_smoke() {
        let _serial = crate::c_oracle_serial();
        // fc_cash_in ok + 22P02
        drive(0, b"$1,234.56");
        drive(0, b"abc");
        for &v in &[0i64, -1, 123456789, i64::MAX, i64::MIN] {
            drive(1, &v.to_le_bytes()); // fc_cash_out
            drive(2, &v.to_le_bytes()); // fc_cash_words
            drive(17, &(v as i32).to_le_bytes()); // fc_int4_cash
            drive(18, &v.to_le_bytes()); // fc_int8_cash
            drive(20, &v.to_le_bytes()); // fc_cash_send
        }
        // pair arms: ok cell + overflow cell + div-by-zero cell
        for (a, b) in [(1000i64, 3i64), (i64::MAX, 2), (1000, 0)] {
            let mut p = a.to_le_bytes().to_vec();
            p.extend_from_slice(&b.to_le_bytes());
            for sel in [3u8, 4, 5, 6, 7, 8, 9, 10, 15, 16] {
                drive(sel, &p);
            }
        }
        // float arms: ok + overflow (inf) + NaN 22P02-class + div-zero
        for f in [1.5f64, f64::INFINITY, f64::NAN, 0.0] {
            let mut p = 1000i64.to_le_bytes().to_vec();
            p.extend_from_slice(&f.to_le_bytes());
            drive(11, &p);
            drive(12, &p);
            let mut p4 = 1000i64.to_le_bytes().to_vec();
            p4.extend_from_slice(&(f as f32).to_le_bytes());
            drive(13, &p4);
            drive(14, &p4);
        }
        // fc_cash_recv ok + short-message 08P01
        drive(19, &[0, 0, 0, 0, 0, 0, 0, 42]);
        drive(19, &[0; 7]);
    }

    /// The carved MIN/-1 division cell stays pinned: pgrust must keep
    /// raising 22003 (ledger rows 865/867/3345; C is platform-UB there).
    #[test]
    fn div_min_by_minus_one_carve_pinned() {
        let _serial = crate::c_oracle_serial();
        let mut p = i64::MIN.to_le_bytes().to_vec();
        p.extend_from_slice(&(-1i64).to_le_bytes());
        drive(6, &p); /* exercises the carve branch, incl. its assert */
        drive(8, &p);
        drive(10, &p);
        // …and the neighboring uncarved cells go through the full comparator.
        let mut q = (i64::MIN + 1).to_le_bytes().to_vec();
        q.extend_from_slice(&(-1i64).to_le_bytes());
        drive(6, &q);
        let mut r = i64::MIN.to_le_bytes().to_vec();
        r.extend_from_slice(&(-2i64).to_le_bytes());
        drive(6, &r);
    }
}
