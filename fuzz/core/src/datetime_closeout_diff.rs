//! datetime_closeout_diff: the p1-lanel2 closeout target for the adt_date /
//! adt_datetime 100%-coverage campaign. Two comparison planes in one target:
//!
//! (A) EXTRACT NUMERIC FACES vs vendored PostgreSQL 18.3 C (Stamp-18.3,
//! upstream sha 62d6c7d3df, csrc/pg_datetime_closeout.c): adt_date's
//! `extract_date`, the retnumeric=true faces of `time_part_common` /
//! `timetz_part_common` (and their float faces, re-compared bit-exact), and
//! the date skip-support callbacks `date_decrement` / `date_increment`
//! (vendored verbatim from 18.3 date.c). The C oracle uses lane p1-laney's
//! NUMERIC ARG-CAPTURE boundary shims: `int64_to_numeric` /
//! `int64_div_fast_to_numeric` record their (value, decimal-scale) arguments
//! and `numeric_in` records the ±Infinity literal, and this driver compares
//! pgrust's RENDERED numeric text (adt_numeric::io::numeric_out_into over the
//! returned NumericImage) against the exact decimal string those arguments
//! determine (timestamp_diff::expected_numeric_text — int64_to_numeric(v)
//! renders as the plain integer, int64_div_fast_to_numeric(v, k) as v/10^k
//! with exactly k fractional digits). Numeric ENCODING itself is adt/numeric
//! crate surface, verified by its own lane. Planes: numeric text, float bits,
//! SQL-NULL flag, error verdict + errcode class.
//!
//! (B) FC-WRAPPER PLANE (wrapper vs already-verified core; native
//! LocalFcinfo — the datetime_io_diff fc_call pattern; NO C comparison):
//! adt_date builtins.rs fc_* wrappers whose glue lines the lanel/laney
//! floor-clean campaigns left owed. The CORES these wrappers call are
//! C-verified by those campaigns (datetime_io_diff / datetime_convert_diff /
//! datetime_engine_diff / timestamp_diff, each ≥10M-exec floor-clean), so the
//! compared claim here is "wrapper glue == core composition": argument
//! unpacking, by-ref result imaging, null returns, and the soft-error
//! (escontext) face. Wrappers driven:
//!   - recv: fc_date_recv fc_time_recv fc_timetz_recv fc_timestamp_recv
//!     fc_timestamptz_recv fc_interval_recv (StringInfo frames, separate
//!     cursors for wrapper and core over the same bytes)
//!   - in (hard + SOFT via an ErrorSaveNode context on the fcinfo):
//!     fc_date_in fc_time_in fc_timetz_in fc_timestamp_in fc_timestamptz_in
//!     fc_interval_in — soft-vs-hard consistency asserted per exec
//!   - out: fc_timestamp_out fc_timestamptz_out fc_interval_out
//!   - conversions: fc_timestamp_date fc_timestamptz_date fc_date_timestamptz
//!     fc_timestamp_time fc_timestamptz_time fc_timestamptz_timetz (incl. the
//!     PG_RETURN_NULL non-finite path), fc_timestamp_bin, fc_interval_mul,
//!     fc_mul_d_interval, fc_interval_div
//!   - cmp macro families, EVERY generated fn: date_cmp_ops(6+cmp),
//!     time_cmp_ops(6+cmp), timetz_cmp_ops(6+cmp), interval_cmp_ops(6+cmp),
//!     ts_cmp_ops(6+cmp), date_ts_cross(28), ts_tstz_cross(14)
//!   - typmod: fc_timetypmodin/fc_timetztypmodin over a driver-built 1-D
//!     cstring[] ArrayType image (n=1 valid/out-of-range and the n!=1
//!     "invalid type modifier" arm), fc_timetypmodout/fc_timetztypmodout vs
//!     adt_timestamp::builtins::typmod_paren_suffix_out
//!   - in_range: fc_in_range_date_interval (vs the core composition
//!     date2timestamp -> in_range_timestamp_interval), fc_in_range_time_
//!     interval / fc_in_range_timetz_interval (no core exists: compared
//!     against an inline reference replicating only the argument unpacking
//!     around timetz_cmp_internal / i64 saturation — the honest reading of
//!     "wrapper glue == documented C semantics"), incl. the negative-offset
//!     error arm and the +overflow saturation arm
//!
//! PINNED ENVIRONMENT: super::datetime_io_diff::init_env_for_siblings() —
//! session timezone GMT, tz database {GMT} only (PGRUST_TZDIR at a
//! nonexistent dir), current instant pinned 2026-06-15 12:30:45.123456 GMT.
//! DateStyle/DateOrder/IntervalStyle fuzzed from selector bytes in the in-
//! arm (affecting core and wrapper identically), pinned ISO/YMD elsewhere
//! (no arm here emits or parses style-dependent text against C).
//!
//! TZ-DATABASE CARVE (arm 4 only): the in-wrappers parse fuzzer text that
//! can reach pg_tzset with invented POSIX zone names; pgrust's zone cache is
//! process-lifetime BY DESIGN (C parity), so an unbounded distinct-name
//! stream OOMs a campaign (datetime_io_diff precedent). The arm calls the
//! ALREADY-LINKED lanel/laney C oracles purely as tzset-name detectors and
//! admits a bounded set of distinct names (the sibling admission budget);
//! the wrapper-vs-core comparison itself is Rust-vs-Rust and stays valid for
//! every admitted exec.
//!
//! DOMAIN FENCES (each matches a PG on-disk invariant, the sibling fences):
//! fold_date / fold_time / fold_zone from datetime_convert_diff.
//! Wire bytes (recv), interval fields, timestamps handed to out/conversion
//! wrappers, and f64 factors are deliberately UNFENCED (the entry points
//! validate). Units text < NAMEDATALEN so C's truncate_identifier stub never
//! fires (identifier truncation is unreachable through these callers).
//!
//! RESIDUAL LINES NOT DRIVEN (reported to the coordinator, not forced):
//!   - builtins.rs 48-49: inside `if in_fastutf8()`, env-gated
//!     PGRUST_ADT_IN_FASTUTF8 DEFAULT OFF (a load-speed prototype). Enabling
//!     the env here would fuzz a non-default configuration — excluded-state
//!     candidates for the exception ledger.
//!   - lib.rs 251-252: date_in's `_ =>` dtype arm is DEFENSIVE-C-PARITY
//!     unreachable in 18.3 — DecodeDateTime can only return
//!     DTK_DATE/EPOCH/LATE/EARLY (its RESERV default elogs first; verified
//!     against the vendored datetime.c), so C date.c date_in's own
//!     `default:` arm is equally dead. Exception-row candidate
//!     (class defensive-c-parity, C counterpart date.c date_in default).
//!   - lib.rs 1250/1260/1343: the `?` Err edges over
//!     int64_div_fast_to_numeric, which cannot fail for an i64 value at
//!     decimal scale 3/6 (make_result of a small-weight var; the C
//!     counterpart can't ereport for those arguments either).
//!     Exception-row candidates (class defensive-c-parity).
//!
//! Input layout: [selector][payload]; selector % 8 picks the arm:
//!   0 extract_date + date_decrement/date_increment — [date i32][units]
//!   1 time_part_common, BOTH retnumeric faces      — [time i64][units]
//!   2 timetz_part_common, BOTH faces               — [time i64][zone i32][units]
//!   3 recv wrappers (all six)                      — [tb][tb2][wire ≤32]
//!   4 in wrappers hard+soft                        — [which][style][tb][tb2][text]
//!   5 out + conversion wrappers                    — [date i32][ts i64][origin i64][iv 16][factor f64]
//!   6 cmp macro families (all 76 fns)              — [d1 i32][d2 i32][t1 i64][t2 i64][z1 i32][z2 i32][iv1 16][iv2 16]
//!   7 typmod + in_range wrappers                   — [b0][b1][v0 i32][tmout i32][val i64][base i64][iv 16][flags]

use std::ffi::CString;

use adt_date::builtins as db;
use adt_date::TimeTzADT;
use adt_datetime::consts::{
    INTSTYLE_ISO_8601, INTSTYLE_POSTGRES, INTSTYLE_POSTGRES_VERBOSE, INTSTYLE_SQL_STANDARD,
};
use adt_datetime::{
    set_date_order, set_date_style, set_interval_style, Interval, DATEORDER_DMY, DATEORDER_MDY,
    DATEORDER_YMD, MAXDATELEN, USE_GERMAN_DATES, USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES,
    USE_XSD_DATES,
};
use adt_timestamp::interval as tsiv;
use adt_timestamp::{PartValue, TsBuf};
use datum::{Datum, NullableDatum};
use types_error::{PgError, PgResult, SoftErrorContext};
use types_fmgr::{ErrorSaveNode, LocalFcinfo, PGFunction};

use super::timestamp_diff::{expected_numeric_text, numeric_image_text};

/// Identity-preserving date fence: already-valid dates (and the two infinity
/// sentinels) pass through unchanged — seeds mean what they say — and only
/// out-of-invariant raws are folded (datetime_convert_diff::fold_date folds
/// EVERY value, which silently shifted seeded dates off their targets).
fn fold_date(raw: i32) -> i32 {
    if raw == i32::MIN || raw == i32::MAX || adt_date::IS_VALID_DATE(raw) {
        return raw;
    }
    super::datetime_convert_diff::fold_date(raw)
}

/// TimeADT invariant fold (identity on 0..=USECS_PER_DAY).
fn fold_time(raw: i64) -> i64 {
    super::datetime_convert_diff::fold_time(raw)
}

/// Identity-preserving zone fence (|zone| < 16h passes through).
fn fold_zone(raw: i32) -> i32 {
    const LIM: i32 = 16 * 60 * 60;
    if raw > -LIM && raw < LIM {
        return raw;
    }
    super::datetime_convert_diff::fold_zone(raw)
}

extern "C" {
    // this target's own oracle TU (csrc/pg_datetime_closeout.c)
    fn pg_dtclo_extract_date(
        units: *const u8,
        ulen: i32,
        date: i32,
        isnull: *mut i32,
        nval: *mut i64,
        nlog10: *mut i32,
        numset: *mut i32,
        inf: *mut i32,
    ) -> i32;
    fn pg_dtclo_time_part(
        units: *const u8,
        ulen: i32,
        time: i64,
        retnumeric: i32,
        fval: *mut f64,
        nval: *mut i64,
        nlog10: *mut i32,
        numset: *mut i32,
    ) -> i32;
    fn pg_dtclo_timetz_part(
        units: *const u8,
        ulen: i32,
        time: i64,
        zone: i32,
        retnumeric: i32,
        fval: *mut f64,
        nval: *mut i64,
        nlog10: *mut i32,
        numset: *mut i32,
    ) -> i32;
    fn pg_dtclo_date_decrement(date: i32, underflow: *mut i32, out: *mut i32) -> i32;
    fn pg_dtclo_date_increment(date: i32, overflow: *mut i32, out: *mut i32) -> i32;

    // lanel's io oracle, reused ONLY as a tzset-name detector for arm 4
    fn pg_diff_date_in(s: *const std::ffi::c_char, style: i32, order: i32, out: *mut i32) -> i32;
    fn pg_diff_time_in(
        s: *const std::ffi::c_char,
        typmod: i32,
        style: i32,
        order: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_diff_timetz_in(
        s: *const std::ffi::c_char,
        typmod: i32,
        style: i32,
        order: i32,
        out_time: *mut i64,
        out_zone: *mut i32,
    ) -> i32;
    fn pg_diff_datetime_tzset_nongmt() -> i32;
    fn pg_diff_datetime_tzset_name() -> *const std::ffi::c_char;

    // laney's oracle, same detector role for the timestamp[tz] in-faces
    fn pg_tsdiff_timestamp_in(
        s: *const std::ffi::c_char,
        typmod: i32,
        style: i32,
        order: i32,
        tz: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_tz_carved() -> i32;
    fn pg_tsdiff_tz_carved_name() -> *const std::ffi::c_char;
}

/// C oracle errcode classes (csrc/pg_datetime_closeout.c header — identical
/// numbering to pg_timestamp_io.c).
fn rust_err_class(e: &PgError) -> i32 {
    use types_error::*;
    if e.sqlstate == ERRCODE_INVALID_DATETIME_FORMAT {
        1
    } else if e.sqlstate == ERRCODE_DATETIME_FIELD_OVERFLOW
        || e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
    {
        2
    } else if e.sqlstate == ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE {
        3
    } else if e.sqlstate == ERRCODE_INTERVAL_FIELD_OVERFLOW {
        4
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        5
    } else if e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED {
        6
    } else {
        98 /* unmapped: always a divergence against the C classes */
    }
}

/// Units payload: < NAMEDATALEN keeps C's truncate_identifier stub out
/// (unreachable through SQL: extract units are always short identifiers).
fn units_payload(b: &[u8]) -> Option<(&[u8], CString)> {
    if b.is_empty() || b.len() > 63 || b.contains(&0) {
        return None;
    }
    Some((b, CString::new(b).unwrap()))
}

/// Text payload for the in-arm (sibling guard: interior-NUL-free UTF-8).
fn text_payload(b: &[u8]) -> Option<(&str, CString)> {
    if b.len() > 200 || b.contains(&0) {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    Some((s, CString::new(b).unwrap()))
}

/// (DateStyle, DateOrder) from a style byte — core and wrapper read the same
/// thread-local, so this only widens the fuzzed state space (no C plane).
fn styles(b: u8) -> (i32, i32) {
    let style = match b % 5 {
        0 => USE_POSTGRES_DATES,
        1 => USE_ISO_DATES,
        2 => USE_SQL_DATES,
        3 => USE_GERMAN_DATES,
        _ => USE_XSD_DATES,
    };
    let order = match (b / 5) % 3 {
        0 => DATEORDER_YMD,
        1 => DATEORDER_DMY,
        _ => DATEORDER_MDY,
    };
    set_date_style(style);
    set_date_order(order);
    (style, order)
}

fn istyle(b: u8) -> i32 {
    let s = match b % 4 {
        0 => INTSTYLE_POSTGRES,
        1 => INTSTYLE_POSTGRES_VERBOSE,
        2 => INTSTYLE_SQL_STANDARD,
        _ => INTSTYLE_ISO_8601,
    };
    set_interval_style(s);
    s
}

/// timestamp[tz] typmod domain (timestamp_diff::ts_typmod, copied: -1 + 0..=6).
fn ts_typmod(b: u8) -> i32 {
    match b % 8 {
        0 => -1,
        n => (n - 1) as i32,
    }
}

/// Interval typmod domain (timestamp_diff::interval_typmod, copied — the
/// values intervaltypmodin can actually produce).
fn interval_typmod(b: u8, b2: u8) -> i32 {
    const MONTH: i32 = 1;
    const YEAR: i32 = 2;
    const DAY: i32 = 3;
    const HOUR: i32 = 10;
    const MINUTE: i32 = 11;
    const SECOND: i32 = 12;
    const fn m(f: i32) -> i32 {
        1 << f
    }
    let range = match b % 14 {
        0 => 0x7FFF,
        1 => m(YEAR),
        2 => m(MONTH),
        3 => m(DAY),
        4 => m(HOUR),
        5 => m(MINUTE),
        6 => m(SECOND),
        7 => m(YEAR) | m(MONTH),
        8 => m(DAY) | m(HOUR),
        9 => m(DAY) | m(HOUR) | m(MINUTE),
        10 => m(DAY) | m(HOUR) | m(MINUTE) | m(SECOND),
        11 => m(HOUR) | m(MINUTE),
        12 => m(HOUR) | m(MINUTE) | m(SECOND),
        _ => m(MINUTE) | m(SECOND),
    };
    let precision = match b2 % 8 {
        0 => 0xFFFF,
        n => (n - 1) as i32,
    };
    if b2 & 0x80 != 0 {
        -1
    } else {
        (range << 16) | precision
    }
}

fn rd_i32(b: &[u8], o: usize) -> i32 {
    i32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}

fn rd_i64(b: &[u8], o: usize) -> i64 {
    i64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

fn rd_f64(b: &[u8], o: usize) -> f64 {
    f64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (the datetime_io_diff fc_call pattern)
// ---------------------------------------------------------------------------

fn fc_call<const N: usize>(f: PGFunction, args: [Datum; N]) -> (PgResult<Datum>, bool) {
    let cx = mcx::MemoryContext::new("datetime_closeout_fc");
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: cx outlives this single call (function scope).
    unsafe { fcinfo.set_result_mcx(cx.mcx()) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// fc_call with a live ErrorSaveNode wired as the call context (the SQL
/// soft-input face, e.g. COPY ... ON_ERROR ignore).
fn fc_call_soft<const N: usize>(
    f: PGFunction,
    args: [Datum; N],
    esn: &mut ErrorSaveNode,
) -> (PgResult<Datum>, bool) {
    let cx = mcx::MemoryContext::new("datetime_closeout_fc_soft");
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: cx outlives this single call (function scope).
    unsafe { fcinfo.set_result_mcx(cx.mcx()) };
    fcinfo.context = esn.fm_node_ptr();
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

fn datum_cstr_bytes<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: the wrapper returned a NUL-terminated cstring allocation live
    // in the fc-call context / backend scratch (read before the next call).
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const std::ffi::c_char).to_bytes() }
}

fn datum_interval(d: Datum) -> (i64, i32, i32) {
    // SAFETY: interval wrappers return a by-ref 16-byte image live in the
    // fc-call context; read field-wise (never a &Interval over raw bytes).
    unsafe {
        let p = d.as_usize() as *const u8;
        (
            (p as *const i64).read_unaligned(),
            (p.add(8) as *const i32).read_unaligned(),
            (p.add(12) as *const i32).read_unaligned(),
        )
    }
}

fn datum_timetz(d: Datum) -> (i64, i32) {
    // SAFETY: timetz wrappers return a by-ref 12-byte image live in the
    // fc-call context.
    unsafe {
        let p = d.as_usize() as *const u8;
        ((p as *const i64).read_unaligned(), (p.add(8) as *const i32).read_unaligned())
    }
}

/// 4B-header text varlena image for wrapper text args.
fn text_varlena(b: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + b.len());
    v.extend_from_slice(&(((4 + b.len()) as u32) << 2).to_le_bytes());
    v.extend_from_slice(b);
    v
}

/// Interval arg image (16B, native layout) for wrapper interval args.
fn interval_arg_img(iv: &Interval) -> [u8; 16] {
    let mut img = [0u8; 16];
    img[..8].copy_from_slice(&iv.time.to_ne_bytes());
    img[8..12].copy_from_slice(&iv.day.to_ne_bytes());
    img[12..].copy_from_slice(&iv.month.to_ne_bytes());
    img
}

/// TimeTz arg image (12B) for wrapper timetz args.
fn timetz_arg_img(t: &TimeTzADT) -> [u8; 12] {
    let mut img = [0u8; 12];
    img[..8].copy_from_slice(&t.time.to_ne_bytes());
    img[8..].copy_from_slice(&t.zone.to_ne_bytes());
    img
}

/// Wrapper-plane check for PartValue-returning fc wrappers (copied from
/// timestamp_diff::fc_check_part, same conventions).
fn fc_check_part(arm: &str, core: &PgResult<PartValue>, fc: (PgResult<Datum>, bool)) {
    match (core, &fc.0) {
        (Ok(PartValue::Null), Ok(_)) => assert!(fc.1, "{arm} FC-PLANE null mismatch"),
        (Ok(PartValue::Float(cv)), Ok(fv)) => assert!(
            cv.to_bits() == fv.as_f64().to_bits(),
            "{arm} FC-PLANE float mismatch"
        ),
        (Ok(PartValue::Numeric(img)), Ok(fv)) => {
            // SAFETY: live 4B-header numeric varlena in the fc-call context.
            let hdr = unsafe { std::slice::from_raw_parts(fv.as_usize() as *const u8, 4) };
            let vlen = (u32::from_le_bytes(hdr.try_into().unwrap()) >> 2) as usize;
            // SAFETY: payload follows the 4B header.
            let payload =
                unsafe { std::slice::from_raw_parts((fv.as_usize() + 4) as *const u8, vlen - 4) };
            let mut out = Vec::new();
            adt_numeric::io::numeric_out_into(adt_numeric::Num::from_payload(payload), &mut out);
            assert!(
                numeric_image_text(img) == String::from_utf8(out).unwrap(),
                "{arm} FC-PLANE numeric mismatch"
            );
        }
        (Err(ce), Err(fe)) => {
            assert!(ce.sqlstate == fe.sqlstate, "{arm} FC-PLANE sqlstate mismatch")
        }
        _ => panic!("{arm} FC-PLANE verdict mismatch"),
    }
}

/// The C extract record folded into the expected numeric text.
fn c_expected_numeric(nval: i64, nlog10: i32, numset: i32, inf: i32, arm: &str) -> String {
    if inf > 0 {
        "Infinity".to_string()
    } else if inf < 0 {
        "-Infinity".to_string()
    } else {
        assert!(numset == 1, "{arm}: C recorded no numeric constructor");
        expected_numeric_text(nval, nlog10)
    }
}

/// Zone-name admission budget for the tz-database carve (arm 4): pgrust's
/// pg_tzset cache is process-lifetime by design (~21KB per admitted name);
/// bound the set of DISTINCT fuzzer-invented names (sibling precedent).
fn admit_zone_name(name: &[u8]) -> bool {
    const BUDGET: usize = 2048;
    use std::cell::RefCell;
    use std::collections::HashSet;
    std::thread_local! {
        static SEEN: RefCell<HashSet<Vec<u8>>> = RefCell::new(HashSet::new());
    }
    SEEN.with(|s| {
        let mut s = s.borrow_mut();
        if s.contains(name) {
            return true; /* already cached: cannot grow RSS further */
        }
        if s.len() >= BUDGET {
            return false;
        }
        s.insert(name.to_vec());
        true
    })
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn datetime_closeout_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    super::datetime_io_diff::init_env_for_siblings();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    // arms outside the in-arm run under pinned ISO/YMD + postgres interval
    // style (matching the oracle's pg_dtclo_reset).
    set_date_style(USE_ISO_DATES);
    set_date_order(DATEORDER_YMD);
    set_interval_style(INTSTYLE_POSTGRES);
    match sel % 8 {
        0 => extract_date_arm(payload),
        1 => time_part_arm(payload),
        2 => timetz_part_arm(payload),
        3 => recv_arm(payload),
        4 => in_arm(payload),
        5 => out_convert_arm(payload),
        6 => cmp_arm(payload),
        _ => typmod_inrange_arm(payload),
    }
}

/// Arm 0: extract_date vs C + fc_extract_date + date skip-support callbacks.
fn extract_date_arm(payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let date = fold_date(rd_i32(payload, 0));

    // skip-support callbacks first (no units needed): pure value plane.
    let (mut cuf, mut cval) = (0i32, 0i32);
    unsafe { pg_dtclo_date_decrement(date, &mut cuf, &mut cval) };
    let mut ruf = false;
    let rv = adt_date::date_decrement(Datum::from_i32(date), &mut ruf);
    assert!(
        (cuf != 0) == ruf,
        "date_decrement UNDERFLOW DIVERGENCE date={date}: C={cuf} Rust={ruf}"
    );
    if !ruf {
        assert!(
            cval == rv.as_i32(),
            "date_decrement VALUE DIVERGENCE date={date}: C={cval} Rust={}",
            rv.as_i32()
        );
    }
    let (mut cof, mut cval) = (0i32, 0i32);
    unsafe { pg_dtclo_date_increment(date, &mut cof, &mut cval) };
    let mut rof = false;
    let rv = adt_date::date_increment(Datum::from_i32(date), &mut rof);
    assert!(
        (cof != 0) == rof,
        "date_increment OVERFLOW DIVERGENCE date={date}: C={cof} Rust={rof}"
    );
    if !rof {
        assert!(
            cval == rv.as_i32(),
            "date_increment VALUE DIVERGENCE date={date}: C={cval} Rust={}",
            rv.as_i32()
        );
    }

    let Some((units, _cu)) = units_payload(&payload[4..]) else { return };

    let (mut cisnull, mut cnval, mut cnlog10, mut cnumset, mut cinf) = (0i32, 0i64, 0i32, 0i32, 0i32);
    // SAFETY: units buffer + out pointers valid for the call.
    let cerr = unsafe {
        pg_dtclo_extract_date(
            units.as_ptr(),
            units.len() as i32,
            date,
            &mut cisnull,
            &mut cnval,
            &mut cnlog10,
            &mut cnumset,
            &mut cinf,
        )
    };
    let r = adt_date::extract_date(units, date);
    match &r {
        Ok(PartValue::Null) => assert!(
            cerr == 0 && cisnull == 1,
            "extract_date NULL DIVERGENCE units={:?} date={date}: C(err={cerr}, isnull={cisnull})",
            String::from_utf8_lossy(units)
        ),
        Ok(PartValue::Numeric(img)) => {
            assert!(
                cerr == 0 && cisnull == 0,
                "extract_date VERDICT DIVERGENCE units={:?} date={date}: C err={cerr}",
                String::from_utf8_lossy(units)
            );
            let expect = c_expected_numeric(cnval, cnlog10, cnumset, cinf, "extract_date");
            let got = numeric_image_text(img);
            assert!(
                expect == got,
                "extract_date NUMERIC DIVERGENCE units={:?} date={date}: \
                 C-determined {expect:?} vs Rust {got:?}",
                String::from_utf8_lossy(units)
            );
        }
        Ok(PartValue::Float(_)) => panic!("extract_date returned Float"),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc && cerr != 0,
                "extract_date DIVERGENCE units={:?} date={date}: C err={cerr} vs \
                 Rust class={rc} (sqlstate={:?})",
                String::from_utf8_lossy(units),
                e.sqlstate
            );
        }
    }

    // fc plane: fc_extract_date(units text, date)
    let uv = text_varlena(units);
    let fc = fc_call(
        db::fc_extract_date,
        [Datum::from_usize(uv.as_ptr() as usize), Datum::from_i32(date)],
    );
    fc_check_part("extract_date", &r, fc);
}

/// Shared tail for the time/timetz part arms: one (C record, Rust PartValue)
/// compare per retnumeric face.
#[allow(clippy::too_many_arguments)]
fn check_part_face(
    arm: &str,
    cerr: i32,
    cfval: f64,
    cnval: i64,
    cnlog10: i32,
    cnumset: i32,
    retnumeric: bool,
    r: &PgResult<PartValue>,
) {
    match r {
        Ok(PartValue::Float(rv)) => {
            assert!(!retnumeric, "{arm}: Float with retnumeric=true");
            assert!(cerr == 0, "{arm} VERDICT: C err={cerr} vs Rust Ok(Float)");
            assert!(
                rv.to_bits() == cfval.to_bits(),
                "{arm} FLOAT DIVERGENCE: C {cfval:?}({:x}) vs Rust {rv:?}({:x})",
                cfval.to_bits(),
                rv.to_bits()
            );
        }
        Ok(PartValue::Numeric(img)) => {
            assert!(retnumeric, "{arm}: Numeric with retnumeric=false");
            assert!(cerr == 0, "{arm} VERDICT: C err={cerr} vs Rust Ok(Numeric)");
            let expect = c_expected_numeric(cnval, cnlog10, cnumset, 0, arm);
            let got = numeric_image_text(img);
            assert!(
                expect == got,
                "{arm} NUMERIC DIVERGENCE: C-determined {expect:?} vs Rust {got:?}"
            );
        }
        Ok(PartValue::Null) => panic!("{arm} returned Null (impossible for this entry)"),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc && cerr != 0,
                "{arm} DIVERGENCE: C err={cerr} vs Rust class={rc} (sqlstate={:?})",
                e.sqlstate
            );
        }
    }
}

/// Arm 1: time_part_common, both faces, + fc_time_part / fc_extract_time.
fn time_part_arm(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let time = fold_time(rd_i64(payload, 0));
    let Some((units, _cu)) = units_payload(&payload[8..]) else { return };
    let uv = text_varlena(units);

    for retnumeric in [false, true] {
        let (mut cfval, mut cnval, mut cnlog10, mut cnumset) = (0f64, 0i64, 0i32, 0i32);
        // SAFETY: units buffer + out pointers valid for the call.
        let cerr = unsafe {
            pg_dtclo_time_part(
                units.as_ptr(),
                units.len() as i32,
                time,
                retnumeric as i32,
                &mut cfval,
                &mut cnval,
                &mut cnlog10,
                &mut cnumset,
            )
        };
        let r = adt_date::time_part_common(units, time, retnumeric);
        check_part_face("time_part", cerr, cfval, cnval, cnlog10, cnumset, retnumeric, &r);

        let f: PGFunction = if retnumeric { db::fc_extract_time } else { db::fc_time_part };
        let fc = fc_call(f, [Datum::from_usize(uv.as_ptr() as usize), Datum::from_i64(time)]);
        fc_check_part("time_part", &r, fc);
    }
}

/// Arm 2: timetz_part_common, both faces, + fc_timetz_part / fc_extract_timetz.
fn timetz_part_arm(payload: &[u8]) {
    if payload.len() < 12 {
        return;
    }
    let t = TimeTzADT { time: fold_time(rd_i64(payload, 0)), zone: fold_zone(rd_i32(payload, 8)) };
    let Some((units, _cu)) = units_payload(&payload[12..]) else { return };
    let uv = text_varlena(units);
    let ti = timetz_arg_img(&t);

    for retnumeric in [false, true] {
        let (mut cfval, mut cnval, mut cnlog10, mut cnumset) = (0f64, 0i64, 0i32, 0i32);
        // SAFETY: units buffer + out pointers valid for the call.
        let cerr = unsafe {
            pg_dtclo_timetz_part(
                units.as_ptr(),
                units.len() as i32,
                t.time,
                t.zone,
                retnumeric as i32,
                &mut cfval,
                &mut cnval,
                &mut cnlog10,
                &mut cnumset,
            )
        };
        let r = adt_date::timetz_part_common(units, &t, retnumeric);
        check_part_face("timetz_part", cerr, cfval, cnval, cnlog10, cnumset, retnumeric, &r);

        let f: PGFunction = if retnumeric { db::fc_extract_timetz } else { db::fc_timetz_part };
        let fc = fc_call(
            f,
            [Datum::from_usize(uv.as_ptr() as usize), Datum::from_usize(ti.as_ptr() as usize)],
        );
        fc_check_part("timetz_part", &r, fc);
    }
}

/// Build a StringInfo over `wire` in `cx` (the recv ABI frame).
fn wire_stringinfo<'a>(cx: &'a mcx::MemoryContext, wire: &[u8]) -> stringinfo::StringInfo<'a> {
    let mut vec = mcx::PgVec::new_in(cx.mcx());
    vec.try_reserve_exact(wire.len() + 1).unwrap();
    vec.extend_from_slice(wire);
    stringinfo::StringInfo::from_vec(vec).unwrap()
}

/// One recv wrapper-vs-core compare over independent cursors.
fn recv_face<T: PartialEq + std::fmt::Debug>(
    arm: &str,
    cx: &mcx::MemoryContext,
    wire: &[u8],
    typmod: i32,
    core: impl FnOnce(&mut stringinfo::StringInfo<'_>) -> PgResult<T>,
    wrapper: PGFunction,
    read: impl FnOnce(Datum) -> T,
) {
    let mut si = wire_stringinfo(cx, wire);
    let r = core(&mut si);
    let mut si2 = wire_stringinfo(cx, wire);
    let fc = fc_call(
        wrapper,
        [
            Datum::from_usize(&mut si2 as *mut _ as usize),
            Datum::from_i32(0),
            Datum::from_i32(typmod),
        ],
    );
    match (&r, &fc.0) {
        (Ok(cv), Ok(fv)) => {
            let fv = read(*fv);
            assert!(*cv == fv, "{arm} FC-PLANE value mismatch: core={cv:?} fc={fv:?}");
        }
        (Err(ce), Err(fe)) => {
            assert!(ce.sqlstate == fe.sqlstate, "{arm} FC-PLANE sqlstate mismatch")
        }
        _ => panic!("{arm} FC-PLANE verdict mismatch: core.ok={} fc.ok={}", r.is_ok(), fc.0.is_ok()),
    }
}

/// Arm 3: all six recv wrappers vs their cores over the same wire bytes.
fn recv_arm(payload: &[u8]) {
    let Some((&tb, rest)) = payload.split_first() else { return };
    let Some((&tb2, wire)) = rest.split_first() else { return };
    if wire.len() > 32 {
        return;
    }
    let ttm = (tb % 8) as i32 - 1; /* time/timetz typmod -1..=6 */
    let tstm = ts_typmod(tb);
    let ivtm = interval_typmod(tb, tb2);
    let cx = mcx::MemoryContext::new("datetime_closeout_recv");

    recv_face("date_recv", &cx, wire, -1, adt_date::date_recv, db::fc_date_recv, |d| d.as_i32());
    recv_face(
        "time_recv",
        &cx,
        wire,
        ttm,
        |si| adt_date::time_recv(si, ttm),
        db::fc_time_recv,
        |d| d.as_i64(),
    );
    recv_face(
        "timetz_recv",
        &cx,
        wire,
        ttm,
        |si| adt_date::timetz_recv(si, ttm).map(|t| (t.time, t.zone)),
        db::fc_timetz_recv,
        datum_timetz,
    );
    recv_face(
        "timestamp_recv",
        &cx,
        wire,
        tstm,
        |si| adt_timestamp::timestamp_recv(si, tstm),
        db::fc_timestamp_recv,
        |d| d.as_i64(),
    );
    recv_face(
        "timestamptz_recv",
        &cx,
        wire,
        tstm,
        |si| adt_timestamp::timestamptz_recv(si, tstm),
        db::fc_timestamptz_recv,
        |d| d.as_i64(),
    );
    recv_face(
        "interval_recv",
        &cx,
        wire,
        ivtm,
        |si| tsiv::interval_recv(si, ivtm).map(|iv| (iv.time, iv.day, iv.month)),
        db::fc_interval_recv,
        datum_interval,
    );
}

/// One in-wrapper face: hard + soft, wrapper vs core, soft-vs-hard
/// consistency. `core` runs the shipped core with an optional escontext;
/// `read` decodes the wrapper's result datum.
fn in_face<T: PartialEq + std::fmt::Debug + Copy>(
    arm: &str,
    core: impl Fn(Option<&mut SoftErrorContext>) -> PgResult<T>,
    wrapper: PGFunction,
    args: [Datum; 3],
    read: impl Fn(Datum) -> T,
) {
    // hard plane
    let r = core(None);
    let fc = fc_call(wrapper, args);
    match (&r, &fc.0) {
        (Ok(cv), Ok(fv)) => {
            let fv = read(*fv);
            assert!(*cv == fv, "{arm} FC-PLANE value mismatch: core={cv:?} fc={fv:?}");
        }
        (Err(ce), Err(fe)) => {
            assert!(ce.sqlstate == fe.sqlstate, "{arm} FC-PLANE sqlstate mismatch")
        }
        _ => panic!("{arm} FC-PLANE verdict mismatch: core.ok={} fc.ok={}", r.is_ok(), fc.0.is_ok()),
    }

    // soft plane: core with SoftErrorContext vs wrapper with ErrorSaveNode
    let mut esc = SoftErrorContext::new(true);
    let rs = core(Some(&mut esc));
    let mut esn = ErrorSaveNode::new(true);
    let fcs = fc_call_soft(wrapper, args, &mut esn);
    match (&rs, &fcs.0) {
        (Ok(cv), Ok(fv)) => {
            let fv = read(*fv);
            assert!(*cv == fv, "{arm} SOFT FC-PLANE value mismatch");
            assert!(
                esc.error_occurred() == esn.ctx.error_occurred(),
                "{arm} SOFT FC-PLANE flag mismatch"
            );
            if esc.error_occurred() {
                assert!(
                    esc.error().map(|e| e.sqlstate) == esn.ctx.error().map(|e| e.sqlstate),
                    "{arm} SOFT FC-PLANE sqlstate mismatch"
                );
            }
        }
        (Err(ce), Err(fe)) => {
            assert!(ce.sqlstate == fe.sqlstate, "{arm} SOFT FC-PLANE hard-error mismatch")
        }
        _ => panic!("{arm} SOFT FC-PLANE verdict mismatch"),
    }

    // soft-vs-hard consistency (timestamp_diff conventions)
    match (&r, &rs) {
        (Ok(hv), Ok(sv)) => assert!(
            esc.error_occurred() || hv == sv,
            "{arm} SOFT-PLANE value mismatch without soft error"
        ),
        (Err(he), Ok(_)) => assert!(
            esc.error_occurred() && esc.error().map(|e| e.sqlstate) == Some(he.sqlstate),
            "{arm} SOFT-PLANE verdict/sqlstate mismatch"
        ),
        (_, Err(se)) => assert!(
            r.as_ref().err().map(|e| e.sqlstate) == Some(se.sqlstate),
            "{arm} SOFT-PLANE hard-error mismatch"
        ),
    }
}

/// Arm 4: the six in-wrappers, hard + soft, behind the tz-database carve.
fn in_arm(payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let which = payload[0] % 6;
    let (style, order) = styles(payload[1]);
    let is = istyle(payload[1]);
    let tb = payload[2];
    let tb2 = payload[3];
    let Some((s, cs)) = text_payload(&payload[4..]) else { return };

    // tz-database carve detection (see header): consult the linked C oracles
    // as tzset-name detectors, admitting a bounded distinct-name set.
    let carved_name: Option<&[u8]> = match which {
        0..=2 => {
            let (mut o32, mut o64, mut oz) = (0i32, 0i64, 0i32);
            // SAFETY: NUL-terminated cstring + out pointers valid for the call.
            unsafe {
                match which {
                    0 => {
                        pg_diff_date_in(cs.as_ptr(), style, order, &mut o32);
                    }
                    1 => {
                        pg_diff_time_in(cs.as_ptr(), (tb % 8) as i32 - 1, style, order, &mut o64);
                    }
                    _ => {
                        pg_diff_timetz_in(
                            cs.as_ptr(),
                            (tb % 8) as i32 - 1,
                            style,
                            order,
                            &mut o64,
                            &mut oz,
                        );
                    }
                }
                if pg_diff_datetime_tzset_nongmt() != 0 {
                    Some(std::ffi::CStr::from_ptr(pg_diff_datetime_tzset_name()).to_bytes())
                } else {
                    None
                }
            }
        }
        3 | 4 => {
            let mut o64 = 0i64;
            // SAFETY: as above.
            unsafe {
                pg_tsdiff_timestamp_in(
                    cs.as_ptr(),
                    ts_typmod(tb),
                    style,
                    order,
                    (which == 4) as i32,
                    &mut o64,
                );
                if pg_tsdiff_tz_carved() != 0 {
                    Some(std::ffi::CStr::from_ptr(pg_tsdiff_tz_carved_name()).to_bytes())
                } else {
                    None
                }
            }
        }
        _ => None, /* interval text carries no zone names */
    };
    if let Some(name) = carved_name {
        if !admit_zone_name(name) {
            return;
        }
    }
    // the detector calls above set the C oracles' thread-local DateStyle;
    // re-pin the Rust side (styles() already set it, detector is C-only).

    let args = [
        Datum::from_usize(cs.as_ptr() as usize),
        Datum::from_u32(0),
        Datum::from_i32(match which {
            1 | 2 => (tb % 8) as i32 - 1,
            3 | 4 => ts_typmod(tb),
            5 => interval_typmod(tb, tb2),
            _ => -1,
        }),
    ];
    let _ = is;
    match which {
        0 => in_face("date_in", |esc| adt_date::date_in(s, esc), db::fc_date_in, args, |d| {
            d.as_i32()
        }),
        1 => {
            let tm = (tb % 8) as i32 - 1;
            in_face("time_in", |esc| adt_date::time_in(s, tm, esc), db::fc_time_in, args, |d| {
                d.as_i64()
            })
        }
        2 => {
            let tm = (tb % 8) as i32 - 1;
            in_face(
                "timetz_in",
                |esc| adt_date::timetz_in(s, tm, esc).map(|t| (t.time, t.zone)),
                db::fc_timetz_in,
                args,
                datum_timetz,
            )
        }
        3 => {
            let tm = ts_typmod(tb);
            in_face(
                "timestamp_in",
                |esc| adt_timestamp::timestamp_in(s, tm, esc),
                db::fc_timestamp_in,
                args,
                |d| d.as_i64(),
            )
        }
        4 => {
            let tm = ts_typmod(tb);
            in_face(
                "timestamptz_in",
                |esc| adt_timestamp::timestamptz_in(s, tm, esc),
                db::fc_timestamptz_in,
                args,
                |d| d.as_i64(),
            )
        }
        _ => {
            let tm = interval_typmod(tb, tb2);
            in_face(
                "interval_in",
                |esc| tsiv::interval_in(s, tm, esc).map(|iv| (iv.time, iv.day, iv.month)),
                db::fc_interval_in,
                args,
                datum_interval,
            )
        }
    }
}

/// Generic i64-Datum wrapper-vs-core compare (out/conversion faces).
fn fc_check_i64(arm: &str, core: &PgResult<i64>, fc: (PgResult<Datum>, bool)) {
    match (core, &fc.0) {
        (Ok(cv), Ok(fv)) => assert!(
            *cv == fv.as_i64(),
            "{arm} FC-PLANE value mismatch: core={cv:x} fc={:x}",
            fv.as_i64()
        ),
        (Err(ce), Err(fe)) => {
            assert!(ce.sqlstate == fe.sqlstate, "{arm} FC-PLANE sqlstate mismatch")
        }
        _ => panic!("{arm} FC-PLANE verdict mismatch"),
    }
}

fn fc_check_i32(arm: &str, core: &PgResult<i32>, fc: (PgResult<Datum>, bool)) {
    fc_check_i64(arm, &core.as_ref().map(|v| *v as i64).map_err(|e| e.clone()), fc)
}

fn fc_check_interval(arm: &str, core: &PgResult<Interval>, fc: (PgResult<Datum>, bool)) {
    match (core, &fc.0) {
        (Ok(cv), Ok(fv)) => {
            let fv = datum_interval(*fv);
            assert!(
                (cv.time, cv.day, cv.month) == fv,
                "{arm} FC-PLANE interval value mismatch"
            );
        }
        (Err(ce), Err(fe)) => {
            assert!(ce.sqlstate == fe.sqlstate, "{arm} FC-PLANE sqlstate mismatch")
        }
        _ => panic!("{arm} FC-PLANE verdict mismatch"),
    }
}

/// Arm 5: out wrappers + conversion wrappers vs cores.
fn out_convert_arm(payload: &[u8]) {
    if payload.len() < 44 {
        return;
    }
    let date = fold_date(rd_i32(payload, 0));
    let ts = rd_i64(payload, 4);
    let origin = rd_i64(payload, 12);
    let iv = Interval { time: rd_i64(payload, 20), day: rd_i32(payload, 28), month: rd_i32(payload, 32) };
    let factor = rd_f64(payload, 36);
    let ii = interval_arg_img(&iv);

    // out wrappers: cstring image vs core buffer render
    for tstz in [false, true] {
        let mut rbuf: TsBuf = [0u8; MAXDATELEN + 1];
        let r = if tstz {
            adt_timestamp::timestamptz_out(ts, &mut rbuf)
        } else {
            adt_timestamp::timestamp_out(ts, &mut rbuf)
        };
        let f: PGFunction = if tstz { db::fc_timestamptz_out } else { db::fc_timestamp_out };
        let fc = fc_call(f, [Datum::from_i64(ts)]);
        match (&r, &fc.0) {
            (Ok(len), Ok(d)) => assert!(
                datum_cstr_bytes(*d) == &rbuf[..*len],
                "timestamp_out FC-PLANE image mismatch"
            ),
            (Err(ce), Err(fe)) => {
                assert!(ce.sqlstate == fe.sqlstate, "timestamp_out FC-PLANE sqlstate")
            }
            _ => panic!("timestamp_out FC-PLANE verdict mismatch"),
        }
    }
    {
        let mut rbuf: TsBuf = [0u8; MAXDATELEN + 1];
        let len = tsiv::interval_out(&iv, &mut rbuf);
        let fc = fc_call(db::fc_interval_out, [Datum::from_usize(ii.as_ptr() as usize)]);
        let d = fc.0.expect("fc_interval_out cannot fail");
        assert!(datum_cstr_bytes(d) == &rbuf[..len], "interval_out FC-PLANE image mismatch");
    }

    // conversions
    fc_check_i32(
        "timestamp_date",
        &adt_date::timestamp_date(ts),
        fc_call(db::fc_timestamp_date, [Datum::from_i64(ts)]),
    );
    fc_check_i32(
        "timestamptz_date",
        &adt_date::timestamptz_date(ts),
        fc_call(db::fc_timestamptz_date, [Datum::from_i64(ts)]),
    );
    fc_check_i64(
        "date_timestamptz",
        &adt_date::date2timestamptz(date),
        fc_call(db::fc_date_timestamptz, [Datum::from_i32(date)]),
    );

    // Option-returning conversions: the PG_RETURN_NULL face rides isnull.
    for (arm, core, f) in [
        (
            "timestamp_time",
            adt_date::timestamp_time(ts),
            db::fc_timestamp_time as PGFunction,
        ),
        ("timestamptz_time", adt_date::timestamptz_time(ts), db::fc_timestamptz_time),
    ] {
        let fc = fc_call(f, [Datum::from_i64(ts)]);
        match (&core, &fc.0) {
            (Ok(Some(cv)), Ok(fv)) => {
                assert!(!fc.1 && *cv == fv.as_i64(), "{arm} FC-PLANE value mismatch")
            }
            (Ok(None), Ok(_)) => assert!(fc.1, "{arm} FC-PLANE null mismatch"),
            (Err(ce), Err(fe)) => {
                assert!(ce.sqlstate == fe.sqlstate, "{arm} FC-PLANE sqlstate")
            }
            _ => panic!("{arm} FC-PLANE verdict mismatch"),
        }
    }
    {
        let core = adt_date::timestamptz_timetz(ts);
        let fc = fc_call(db::fc_timestamptz_timetz, [Datum::from_i64(ts)]);
        match (&core, &fc.0) {
            (Ok(Some(cv)), Ok(fv)) => {
                let fv = datum_timetz(*fv);
                assert!(
                    !fc.1 && (cv.time, cv.zone) == fv,
                    "timestamptz_timetz FC-PLANE value mismatch"
                );
            }
            (Ok(None), Ok(_)) => assert!(fc.1, "timestamptz_timetz FC-PLANE null mismatch"),
            (Err(ce), Err(fe)) => {
                assert!(ce.sqlstate == fe.sqlstate, "timestamptz_timetz FC-PLANE sqlstate")
            }
            _ => panic!("timestamptz_timetz FC-PLANE verdict mismatch"),
        }
    }

    // timestamp_bin(stride, ts, origin)
    fc_check_i64(
        "timestamp_bin",
        &tsiv::timestamp_bin(&iv, ts, origin),
        fc_call(
            db::fc_timestamp_bin,
            [
                Datum::from_usize(ii.as_ptr() as usize),
                Datum::from_i64(ts),
                Datum::from_i64(origin),
            ],
        ),
    );

    // interval x factor
    fc_check_interval(
        "interval_mul",
        &tsiv::interval_mul(&iv, factor),
        fc_call(
            db::fc_interval_mul,
            [Datum::from_usize(ii.as_ptr() as usize), Datum::from_f64(factor)],
        ),
    );
    fc_check_interval(
        "mul_d_interval",
        &tsiv::interval_mul(&iv, factor),
        fc_call(
            db::fc_mul_d_interval,
            [Datum::from_f64(factor), Datum::from_usize(ii.as_ptr() as usize)],
        ),
    );
    fc_check_interval(
        "interval_div",
        &tsiv::interval_div(&iv, factor),
        fc_call(
            db::fc_interval_div,
            [Datum::from_usize(ii.as_ptr() as usize), Datum::from_f64(factor)],
        ),
    );
}

/// Call a 2-arg bool/i32-returning cmp wrapper and check against `expect`.
fn cmp_bool(arm: &str, f: PGFunction, a: Datum, b: Datum, expect: bool) {
    let fc = fc_call(f, [a, b]);
    let d = fc.0.unwrap_or_else(|e| panic!("{arm} cmp wrapper errored: {}", e.message));
    assert!(
        d.as_i64() == expect as i64,
        "{arm} FC-PLANE cmp mismatch: expect={expect} got={:x}",
        d.as_i64()
    );
}

fn cmp_i32(arm: &str, f: PGFunction, a: Datum, b: Datum, expect: i32) {
    let fc = fc_call(f, [a, b]);
    let d = fc.0.unwrap_or_else(|e| panic!("{arm} cmp wrapper errored: {}", e.message));
    assert!(d.as_i32() == expect, "{arm} FC-PLANE cmp mismatch");
}

/// Arm 6: every generated cmp-family fn vs the core comparison value.
fn cmp_arm(payload: &[u8]) {
    if payload.len() < 64 {
        return;
    }
    let d1 = fold_date(rd_i32(payload, 0));
    let d2 = fold_date(rd_i32(payload, 4));
    let t1 = rd_i64(payload, 8);
    let t2 = rd_i64(payload, 16);
    let z1 = fold_zone(rd_i32(payload, 24));
    let z2 = fold_zone(rd_i32(payload, 28));
    let iv1 =
        Interval { time: rd_i64(payload, 32), day: rd_i32(payload, 40), month: rd_i32(payload, 44) };
    let iv2 =
        Interval { time: rd_i64(payload, 48), day: rd_i32(payload, 56), month: rd_i32(payload, 60) };
    let tt1 = TimeTzADT { time: fold_time(t1), zone: z1 };
    let tt2 = TimeTzADT { time: fold_time(t2), zone: z2 };

    // date +- int wrappers ride this arm's (d1, raw-int) inputs so the
    // date_pli/date_mii overflow arms (date_out_of_range_plain, adt_date
    // lib.rs) are in the fuzzed domain — the raw i32 addend is deliberately
    // UNfenced, matching SQL ('5874897-12-31'::date + 1 errors 22008).
    {
        let addend = rd_i32(payload, 4); /* raw, pre-fold */
        for (nm, f, core) in [
            ("date_pli", db::fc_date_pli as types_fmgr::PGFunction, adt_date::date_pli(d1, addend)),
            ("date_mii", db::fc_date_mii, adt_date::date_mii(d1, addend)),
        ] {
            let fc = fc_call(f, [Datum::from_i32(d1), Datum::from_i32(addend)]);
            match (&core, &fc.0) {
                (Ok(cv), Ok(fv)) => assert!(
                    *cv == fv.as_i32(),
                    "{nm} FC-PLANE value: core={cv} fc={}",
                    fv.as_i32()
                ),
                (Err(ce), Err(fe)) => {
                    assert!(ce.sqlstate == fe.sqlstate, "{nm} FC-PLANE sqlstate mismatch")
                }
                _ => panic!("{nm} FC-PLANE verdict mismatch"),
            }
        }
    }

    // date_cmp_ops
    let (da, db_) = (Datum::from_i32(d1), Datum::from_i32(d2));
    cmp_bool("date_eq", db::fc_date_eq, da, db_, d1 == d2);
    cmp_bool("date_ne", db::fc_date_ne, da, db_, d1 != d2);
    cmp_bool("date_lt", db::fc_date_lt, da, db_, d1 < d2);
    cmp_bool("date_le", db::fc_date_le, da, db_, d1 <= d2);
    cmp_bool("date_gt", db::fc_date_gt, da, db_, d1 > d2);
    cmp_bool("date_ge", db::fc_date_ge, da, db_, d1 >= d2);
    cmp_i32("date_cmp", db::fc_date_cmp, da, db_, adt_date::date_cmp_internal(d1, d2));

    // time_cmp_ops (raw i64 comparisons: no arithmetic, unfenced)
    let (ta, tb) = (Datum::from_i64(t1), Datum::from_i64(t2));
    cmp_bool("time_eq", db::fc_time_eq, ta, tb, t1 == t2);
    cmp_bool("time_ne", db::fc_time_ne, ta, tb, t1 != t2);
    cmp_bool("time_lt", db::fc_time_lt, ta, tb, t1 < t2);
    cmp_bool("time_le", db::fc_time_le, ta, tb, t1 <= t2);
    cmp_bool("time_gt", db::fc_time_gt, ta, tb, t1 > t2);
    cmp_bool("time_ge", db::fc_time_ge, ta, tb, t1 >= t2);
    cmp_i32("time_cmp", db::fc_time_cmp, ta, tb, adt_date::time_cmp_internal(t1, t2));

    // timetz_cmp_ops (folded: cmp_internal does zone arithmetic)
    let i1 = timetz_arg_img(&tt1);
    let i2 = timetz_arg_img(&tt2);
    let (za, zb) = (Datum::from_usize(i1.as_ptr() as usize), Datum::from_usize(i2.as_ptr() as usize));
    let zc = adt_date::timetz_cmp_internal(&tt1, &tt2);
    cmp_bool("timetz_eq", db::fc_timetz_eq, za, zb, zc == 0);
    cmp_bool("timetz_ne", db::fc_timetz_ne, za, zb, zc != 0);
    cmp_bool("timetz_lt", db::fc_timetz_lt, za, zb, zc < 0);
    cmp_bool("timetz_le", db::fc_timetz_le, za, zb, zc <= 0);
    cmp_bool("timetz_gt", db::fc_timetz_gt, za, zb, zc > 0);
    cmp_bool("timetz_ge", db::fc_timetz_ge, za, zb, zc >= 0);
    cmp_i32("timetz_cmp", db::fc_timetz_cmp, za, zb, zc);

    // interval_cmp_ops (unfenced: cmp_value is widening)
    let ii1 = interval_arg_img(&iv1);
    let ii2 = interval_arg_img(&iv2);
    let (ia, ib) =
        (Datum::from_usize(ii1.as_ptr() as usize), Datum::from_usize(ii2.as_ptr() as usize));
    let ic = tsiv::interval_cmp_internal(&iv1, &iv2);
    cmp_bool("interval_eq", db::fc_interval_eq, ia, ib, ic == 0);
    cmp_bool("interval_ne", db::fc_interval_ne, ia, ib, ic != 0);
    cmp_bool("interval_lt", db::fc_interval_lt, ia, ib, ic < 0);
    cmp_bool("interval_le", db::fc_interval_le, ia, ib, ic <= 0);
    cmp_bool("interval_gt", db::fc_interval_gt, ia, ib, ic > 0);
    cmp_bool("interval_ge", db::fc_interval_ge, ia, ib, ic >= 0);
    cmp_i32("interval_cmp", db::fc_interval_cmp, ia, ib, ic);

    // ts_cmp_ops
    cmp_bool("timestamp_eq", db::fc_timestamp_eq, ta, tb, t1 == t2);
    cmp_bool("timestamp_ne", db::fc_timestamp_ne, ta, tb, t1 != t2);
    cmp_bool("timestamp_lt", db::fc_timestamp_lt, ta, tb, t1 < t2);
    cmp_bool("timestamp_le", db::fc_timestamp_le, ta, tb, t1 <= t2);
    cmp_bool("timestamp_gt", db::fc_timestamp_gt, ta, tb, t1 > t2);
    cmp_bool("timestamp_ge", db::fc_timestamp_ge, ta, tb, t1 >= t2);
    cmp_i32(
        "timestamp_cmp",
        db::fc_timestamp_cmp,
        ta,
        tb,
        adt_date::timestamp_cmp_internal(t1, t2),
    );

    // date_ts_cross: all 28 (both operand orders, ts and tstz)
    let cts = adt_date::date_cmp_timestamp_internal(d1, t2);
    cmp_bool("date_eq_timestamp", db::fc_date_eq_timestamp, da, tb, cts == 0);
    cmp_bool("date_ne_timestamp", db::fc_date_ne_timestamp, da, tb, cts != 0);
    cmp_bool("date_lt_timestamp", db::fc_date_lt_timestamp, da, tb, cts < 0);
    cmp_bool("date_le_timestamp", db::fc_date_le_timestamp, da, tb, cts <= 0);
    cmp_bool("date_gt_timestamp", db::fc_date_gt_timestamp, da, tb, cts > 0);
    cmp_bool("date_ge_timestamp", db::fc_date_ge_timestamp, da, tb, cts >= 0);
    cmp_i32("date_cmp_timestamp", db::fc_date_cmp_timestamp, da, tb, cts);
    cmp_bool("timestamp_eq_date", db::fc_timestamp_eq_date, tb, da, cts == 0);
    cmp_bool("timestamp_ne_date", db::fc_timestamp_ne_date, tb, da, cts != 0);
    cmp_bool("timestamp_lt_date", db::fc_timestamp_lt_date, tb, da, cts > 0);
    cmp_bool("timestamp_le_date", db::fc_timestamp_le_date, tb, da, cts >= 0);
    cmp_bool("timestamp_gt_date", db::fc_timestamp_gt_date, tb, da, cts < 0);
    cmp_bool("timestamp_ge_date", db::fc_timestamp_ge_date, tb, da, cts <= 0);
    cmp_i32("timestamp_cmp_date", db::fc_timestamp_cmp_date, tb, da, -cts);
    let ctz = adt_date::date_cmp_timestamptz_internal(d1, t2);
    cmp_bool("date_eq_timestamptz", db::fc_date_eq_timestamptz, da, tb, ctz == 0);
    cmp_bool("date_ne_timestamptz", db::fc_date_ne_timestamptz, da, tb, ctz != 0);
    cmp_bool("date_lt_timestamptz", db::fc_date_lt_timestamptz, da, tb, ctz < 0);
    cmp_bool("date_le_timestamptz", db::fc_date_le_timestamptz, da, tb, ctz <= 0);
    cmp_bool("date_gt_timestamptz", db::fc_date_gt_timestamptz, da, tb, ctz > 0);
    cmp_bool("date_ge_timestamptz", db::fc_date_ge_timestamptz, da, tb, ctz >= 0);
    cmp_i32("date_cmp_timestamptz", db::fc_date_cmp_timestamptz, da, tb, ctz);
    cmp_bool("timestamptz_eq_date", db::fc_timestamptz_eq_date, tb, da, ctz == 0);
    cmp_bool("timestamptz_ne_date", db::fc_timestamptz_ne_date, tb, da, ctz != 0);
    cmp_bool("timestamptz_lt_date", db::fc_timestamptz_lt_date, tb, da, ctz > 0);
    cmp_bool("timestamptz_le_date", db::fc_timestamptz_le_date, tb, da, ctz >= 0);
    cmp_bool("timestamptz_gt_date", db::fc_timestamptz_gt_date, tb, da, ctz < 0);
    cmp_bool("timestamptz_ge_date", db::fc_timestamptz_ge_date, tb, da, ctz <= 0);
    cmp_i32("timestamptz_cmp_date", db::fc_timestamptz_cmp_date, tb, da, -ctz);

    // ts_tstz_cross: all 14. The core conversion can error (out-of-range
    // timestamp); the wrappers propagate the same error.
    match tsiv::timestamp_cmp_timestamptz_internal(t1, t2) {
        Ok(x) => {
            cmp_bool("timestamp_eq_timestamptz", db::fc_timestamp_eq_timestamptz, ta, tb, x == 0);
            cmp_bool("timestamp_ne_timestamptz", db::fc_timestamp_ne_timestamptz, ta, tb, x != 0);
            cmp_bool("timestamp_lt_timestamptz", db::fc_timestamp_lt_timestamptz, ta, tb, x < 0);
            cmp_bool("timestamp_le_timestamptz", db::fc_timestamp_le_timestamptz, ta, tb, x <= 0);
            cmp_bool("timestamp_gt_timestamptz", db::fc_timestamp_gt_timestamptz, ta, tb, x > 0);
            cmp_bool("timestamp_ge_timestamptz", db::fc_timestamp_ge_timestamptz, ta, tb, x >= 0);
            cmp_i32("timestamp_cmp_timestamptz", db::fc_timestamp_cmp_timestamptz, ta, tb, x);
            cmp_bool("timestamptz_eq_timestamp", db::fc_timestamptz_eq_timestamp, tb, ta, x == 0);
            cmp_bool("timestamptz_ne_timestamp", db::fc_timestamptz_ne_timestamp, tb, ta, x != 0);
            cmp_bool("timestamptz_lt_timestamp", db::fc_timestamptz_lt_timestamp, tb, ta, x > 0);
            cmp_bool("timestamptz_le_timestamp", db::fc_timestamptz_le_timestamp, tb, ta, x >= 0);
            cmp_bool("timestamptz_gt_timestamp", db::fc_timestamptz_gt_timestamp, tb, ta, x < 0);
            cmp_bool("timestamptz_ge_timestamp", db::fc_timestamptz_ge_timestamp, tb, ta, x <= 0);
            cmp_i32("timestamptz_cmp_timestamp", db::fc_timestamptz_cmp_timestamp, tb, ta, -x);
        }
        Err(e) => {
            // every wrapper must fail with the same sqlstate (swapped
            // wrappers take swapped operands so the SAME timestamp converts)
            for (f, a, b) in [
                (db::fc_timestamp_eq_timestamptz as PGFunction, ta, tb),
                (db::fc_timestamp_cmp_timestamptz, ta, tb),
                (db::fc_timestamptz_cmp_timestamp, tb, ta),
            ] {
                let fc = fc_call(f, [a, b]);
                assert!(
                    fc.0.as_ref().err().map(|fe| fe.sqlstate) == Some(e.sqlstate),
                    "ts_tstz_cross FC-PLANE error mismatch"
                );
            }
        }
    }
}

/// Build a minimal valid 1-D cstring[] ArrayType image (the layout
/// adt_timestamp::builtins::array_get_integer_typmods reads: varlena hdr,
/// ndim, dataoffset=0 (no nulls), elemtype=CSTRINGOID, dim, lbound,
/// NUL-terminated element payloads).
fn cstring_array_image(vals: &[i32]) -> Vec<u8> {
    let mut payload = Vec::new();
    for v in vals {
        payload.extend_from_slice(v.to_string().as_bytes());
        payload.push(0);
    }
    let mut img = Vec::new();
    img.extend_from_slice(&0u32.to_le_bytes()); /* hdr patched below */
    img.extend_from_slice(&1i32.to_le_bytes()); /* ndim */
    img.extend_from_slice(&0i32.to_le_bytes()); /* dataoffset: no nulls */
    img.extend_from_slice(&types_core::CSTRINGOID.to_le_bytes());
    img.extend_from_slice(&(vals.len() as i32).to_le_bytes()); /* dim */
    img.extend_from_slice(&1i32.to_le_bytes()); /* lbound */
    img.extend_from_slice(&payload);
    let hdr = ((img.len() as u32) << 2).to_le_bytes();
    img[..4].copy_from_slice(&hdr);
    img
}

/// Arm 7: typmod in/out wrappers + the three in_range wrappers.
fn typmod_inrange_arm(payload: &[u8]) {
    if payload.len() < 47 {
        return;
    }
    let b0 = payload[0];
    let flags = payload[1];
    let v0 = rd_i32(payload, 2).rem_euclid(13) - 3; /* -3..=9: error/warn/valid arms */
    let tmout = rd_i32(payload, 6);
    let val_raw = rd_i64(payload, 14);
    let base_raw = rd_i64(payload, 22);
    let iv =
        Interval { time: rd_i64(payload, 30), day: rd_i32(payload, 38), month: rd_i32(payload, 42) };
    let sub = flags & 1 != 0;
    let less = flags & 2 != 0;

    // typmodin: n = 1 (value arms), n = 2 (the "invalid type modifier" arm),
    // or a corrupted ArrayType image (the array_get_integer_typmods error
    // arms, riding the wrappers' `?` edge).
    let n2 = b0 & 1 != 0;
    let corrupt = (b0 >> 1) % 4; /* 0 valid, 1 elemtype, 2 ndim, 3 nulls */
    let vals: &[i32] = if n2 { &[v0, 3] } else { &[v0] };
    let mut img = cstring_array_image(vals);
    match corrupt {
        1 => img[12..16].copy_from_slice(&25u32.to_le_bytes()), /* TEXTOID */
        2 => img[4..8].copy_from_slice(&2i32.to_le_bytes()),
        3 => img[8..12].copy_from_slice(&1i32.to_le_bytes()),
        _ => {}
    }
    let expected_corrupt_sqlstate = match corrupt {
        1 => Some(types_error::ERRCODE_DATATYPE_MISMATCH),
        2 => Some(types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR),
        3 => Some(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
        _ => None,
    };
    let d = Datum::from_usize(img.as_ptr() as usize);
    for (arm, f, istz) in [
        ("timetypmodin", db::fc_timetypmodin as PGFunction, false),
        ("timetztypmodin", db::fc_timetztypmodin, true),
    ] {
        let fc = fc_call(f, [d]);
        if let Some(sql) = expected_corrupt_sqlstate {
            assert!(
                fc.0.as_ref().err().map(|e| e.sqlstate) == Some(sql),
                "{arm} FC-PLANE: corrupted array image must raise its arm's sqlstate"
            );
        } else if n2 {
            assert!(
                fc.0.as_ref().err().map(|e| e.sqlstate)
                    == Some(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
                "{arm} FC-PLANE: n=2 must raise invalid type modifier"
            );
        } else {
            let core = adt_date::anytime_typmod_check(istz, v0);
            fc_check_i32(arm, &core, fc);
        }
    }

    // typmodout vs the core suffix renderer
    for (arm, f, suffix) in [
        ("timetypmodout", db::fc_timetypmodout as PGFunction, &b" without time zone"[..]),
        ("timetztypmodout", db::fc_timetztypmodout, &b" with time zone"[..]),
    ] {
        let fc = fc_call(f, [Datum::from_i32(tmout)]);
        let d = fc.0.unwrap_or_else(|e| panic!("{arm} errored: {}", e.message));
        let mut buf = [0u8; 64];
        let len = adt_timestamp::builtins::typmod_paren_suffix_out(tmout, suffix, &mut buf);
        assert!(datum_cstr_bytes(d) == &buf[..len], "{arm} FC-PLANE image mismatch");
    }

    // in_range_date_interval vs the core composition it wraps
    {
        let vd = fold_date(val_raw as i32);
        let bd = fold_date(base_raw as i32);
        let ii = interval_arg_img(&iv);
        let fc = fc_call(
            db::fc_in_range_date_interval,
            [
                Datum::from_i32(vd),
                Datum::from_i32(bd),
                Datum::from_usize(ii.as_ptr() as usize),
                Datum::from_bool(sub),
                Datum::from_bool(less),
            ],
        );
        let reference = (|| -> PgResult<bool> {
            let v = adt_date::date2timestamp(vd)?;
            let b = adt_date::date2timestamp(bd)?;
            tsiv::in_range_timestamp_interval(v, b, &iv, sub, less)
        })();
        match (&reference, &fc.0) {
            (Ok(cv), Ok(fv)) => assert!(
                *cv == (fv.as_i64() != 0),
                "in_range_date_interval FC-PLANE value mismatch"
            ),
            (Err(ce), Err(fe)) => assert!(
                ce.sqlstate == fe.sqlstate,
                "in_range_date_interval FC-PLANE sqlstate mismatch"
            ),
            _ => panic!("in_range_date_interval FC-PLANE verdict mismatch"),
        }
    }

    // in_range_time_interval vs the inline reference (see header)
    {
        let vt = fold_time(val_raw);
        let bt = fold_time(base_raw);
        let ii = interval_arg_img(&iv);
        let fc = fc_call(
            db::fc_in_range_time_interval,
            [
                Datum::from_i64(vt),
                Datum::from_i64(bt),
                Datum::from_usize(ii.as_ptr() as usize),
                Datum::from_bool(sub),
                Datum::from_bool(less),
            ],
        );
        let reference: PgResult<bool> = if iv.time < 0 {
            Err(Box::new(
                PgError::error("reference")
                    .with_sqlstate(types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
            ))
        } else if sub {
            let sum = bt - iv.time;
            Ok(if less { vt <= sum } else { vt >= sum })
        } else {
            match bt.checked_add(iv.time) {
                Some(sum) => Ok(if less { vt <= sum } else { vt >= sum }),
                None => Ok(less), /* +overflow saturation arm */
            }
        };
        match (&reference, &fc.0) {
            (Ok(cv), Ok(fv)) => assert!(
                *cv == (fv.as_i64() != 0),
                "in_range_time_interval FC-PLANE value mismatch"
            ),
            (Err(ce), Err(fe)) => assert!(
                ce.sqlstate == fe.sqlstate,
                "in_range_time_interval FC-PLANE sqlstate mismatch"
            ),
            _ => panic!("in_range_time_interval FC-PLANE verdict mismatch"),
        }
    }

    // in_range_timetz_interval vs the inline reference. The former
    // WRAP-BAND FENCE here is RETIRED: the fuzzer found timetz_cmp_internal
    // trapping in overflow-checked builds when the sum lands within ~16h of
    // usecs of an i64 boundary (crash-6e2deff3/-81da0752, SQL-reachable via
    // a huge window RANGE offset); the product now wraps exactly as C's
    // -fwrapv does (adt_date lib.rs timetz_cmp_internal wrapping_add fix,
    // p1-lanel2), so the whole band is back in the compared domain.
    {
        let vtt = TimeTzADT { time: fold_time(val_raw), zone: fold_zone((val_raw >> 32) as i32) };
        let btt =
            TimeTzADT { time: fold_time(base_raw), zone: fold_zone((base_raw >> 32) as i32) };
        let vi = timetz_arg_img(&vtt);
        let bi = timetz_arg_img(&btt);
        let ii = interval_arg_img(&iv);
        let fc = fc_call(
            db::fc_in_range_timetz_interval,
            [
                Datum::from_usize(vi.as_ptr() as usize),
                Datum::from_usize(bi.as_ptr() as usize),
                Datum::from_usize(ii.as_ptr() as usize),
                Datum::from_bool(sub),
                Datum::from_bool(less),
            ],
        );
        let reference: PgResult<bool> = if iv.time < 0 {
            Err(Box::new(
                PgError::error("reference")
                    .with_sqlstate(types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
            ))
        } else {
            let time = if sub {
                Some(btt.time - iv.time)
            } else {
                btt.time.checked_add(iv.time)
            };
            match time {
                Some(time) => {
                    let sum = TimeTzADT { time, zone: btt.zone };
                    let cmp = adt_date::timetz_cmp_internal(&vtt, &sum);
                    Ok(if less { cmp <= 0 } else { cmp >= 0 })
                }
                None => Ok(less),
            }
        };
        match (&reference, &fc.0) {
            (Ok(cv), Ok(fv)) => assert!(
                *cv == (fv.as_i64() != 0),
                "in_range_timetz_interval FC-PLANE value mismatch"
            ),
            (Err(ce), Err(fe)) => assert!(
                ce.sqlstate == fe.sqlstate,
                "in_range_timetz_interval FC-PLANE sqlstate mismatch"
            ),
            _ => panic!("in_range_timetz_interval FC-PLANE verdict mismatch"),
        }
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/datetime_closeout_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/datetime_closeout_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                datetime_closeout_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 800, "expected >=800 seeds, found {n}");
    }

    fn arm(sel: u8, tail: &[u8]) -> Vec<u8> {
        let mut v = vec![sel];
        v.extend_from_slice(tail);
        v
    }

    const UNITS: [&str; 22] = [
        "microseconds", "milliseconds", "second", "minute", "hour", "day", "month", "quarter",
        "week", "year", "decade", "century", "millennium", "julian", "isoyear", "isodow", "dow",
        "doy", "epoch", "timezone", "timezone_hour", "junk",
    ];

    const DATES: [i32; 8] =
        [0, 1, -1, i32::MIN, i32::MAX, -2_451_545, 2_932_896, 10_957];

    #[test]
    fn arms_smoke_extract_date() {
        let _serial = crate::c_oracle_serial();
        for d in DATES {
            for u in UNITS {
                let mut p = d.to_le_bytes().to_vec();
                p.extend_from_slice(u.as_bytes());
                datetime_closeout_diff(&arm(0, &p));
            }
        }
        // sweep for the fold + skip-support callbacks
        for k in 0..2000i32 {
            let mut p = k.wrapping_mul(1_000_003).to_le_bytes().to_vec();
            p.extend_from_slice(b"epoch");
            datetime_closeout_diff(&arm(0, &p));
        }
    }

    #[test]
    fn arms_smoke_time_parts() {
        let _serial = crate::c_oracle_serial();
        for t in [0i64, 1, 45_296_789_000, 86_400_000_000, 86_399_999_999, 43_200_000_000] {
            for u in UNITS {
                let mut p = t.to_le_bytes().to_vec();
                p.extend_from_slice(u.as_bytes());
                datetime_closeout_diff(&arm(1, &p));
                let mut q = t.to_le_bytes().to_vec();
                q.extend_from_slice(&3600i32.to_le_bytes());
                q.extend_from_slice(u.as_bytes());
                datetime_closeout_diff(&arm(2, &q));
                let mut q = t.to_le_bytes().to_vec();
                q.extend_from_slice(&(-57_599i32).to_le_bytes());
                q.extend_from_slice(u.as_bytes());
                datetime_closeout_diff(&arm(2, &q));
            }
        }
    }

    #[test]
    fn arms_smoke_recv() {
        let _serial = crate::c_oracle_serial();
        // valid date wire (4 bytes BE), valid time (8 bytes), valid timetz
        // (12), interval (16), short/garbage frames
        let frames: [&[u8]; 8] = [
            &[0, 0, 0, 0],
            &[0, 0, 42, 105],
            &[0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 0, 0, 10, 0, 0, 0, 1, 0, 0, 14, 16],
            &[0; 16],
            &[0xff; 16],
            &[1, 2],
            &[],
        ];
        for (i, f) in frames.iter().enumerate() {
            let mut p = vec![i as u8, (i * 7) as u8];
            p.extend_from_slice(f);
            datetime_closeout_diff(&arm(3, &p));
        }
    }

    #[test]
    fn arms_smoke_in() {
        let _serial = crate::c_oracle_serial();
        for which in 0u8..6 {
            for text in [
                &b"2024-01-05"[..],
                b"12:34:56.789",
                b"04:05:06+08",
                b"2024-01-05 12:34:56",
                b"1 year 2 mons 3 days 04:05:06",
                b"infinity",
                b"-infinity",
                b"epoch",
                b"now",
                b"allballs",
                b"not-a-datetime",
                b"25:99:99",
                b"@ 2 hours ago",
            ] {
                let mut p = vec![which, 1, 0, 0];
                p.extend_from_slice(text);
                datetime_closeout_diff(&arm(4, &p));
                // second style/typmod posture
                let mut p = vec![which, 7, 3, 0x80];
                p.extend_from_slice(text);
                datetime_closeout_diff(&arm(4, &p));
            }
        }
    }

    /// date_in's SOFT parse-error return (lib.rs 240-241): a time-only
    /// input fails in DecodeDateTime (missing date fields -> 22007), and the
    /// soft face must flag it without throwing — both directly and through
    /// the wrapper-with-ErrorSaveNode plane. (The `_ =>` dtype arm at
    /// 251-252 is defensive-c-parity unreachable — see the module header.)
    #[test]
    fn date_in_soft_parse_error() {
        let _serial = crate::c_oracle_serial();
        super::super::datetime_io_diff::init_env_for_siblings();
        set_date_style(USE_ISO_DATES);
        set_date_order(DATEORDER_YMD);
        let mut p = vec![0u8, 1, 0, 0];
        p.extend_from_slice(b"04:05:06");
        datetime_closeout_diff(&arm(4, &p));
        // and directly: the soft face must flag 22007 without throwing
        let mut esc = SoftErrorContext::new(true);
        let r = adt_date::date_in("04:05:06", Some(&mut esc));
        assert!(r.is_ok() && esc.error_occurred());
        assert_eq!(
            esc.error().map(|e| e.sqlstate),
            Some(types_error::ERRCODE_INVALID_DATETIME_FORMAT)
        );
    }

    fn a5(date: i32, ts: i64, origin: i64, iv: (i64, i32, i32), factor: f64) -> Vec<u8> {
        let mut p = date.to_le_bytes().to_vec();
        p.extend_from_slice(&ts.to_le_bytes());
        p.extend_from_slice(&origin.to_le_bytes());
        p.extend_from_slice(&iv.0.to_le_bytes());
        p.extend_from_slice(&iv.1.to_le_bytes());
        p.extend_from_slice(&iv.2.to_le_bytes());
        p.extend_from_slice(&factor.to_le_bytes());
        arm(5, &p)
    }

    #[test]
    fn arms_smoke_out_convert() {
        let _serial = crate::c_oracle_serial();
        for ts in [0i64, 1, -1, i64::MAX, i64::MIN, 9_662 * 86_400_000_000] {
            datetime_closeout_diff(&a5(0, ts, 0, (3_600_000_000, 1, 2), 2.5));
        }
        // error shapes: out-of-range ts, div-by-zero factor, nan factor
        datetime_closeout_diff(&a5(0, 9_223_371_331_200_000_000 - 1, 0, (1, 0, 0), 0.0));
        datetime_closeout_diff(&a5(i32::MAX, 0, 0, (1, 0, 0), f64::NAN));
        datetime_closeout_diff(&a5(2_932_896, 0, 86_400_000_000, (0, 0, 0), 1.0));
    }

    #[test]
    fn arms_smoke_cmp() {
        let _serial = crate::c_oracle_serial();
        let mut p = Vec::new();
        for pair in [(0i64, 0i64), (1, 2), (-1, 1), (i64::MAX, i64::MIN), (5, 5)] {
            p.clear();
            p.extend_from_slice(&(pair.0 as i32).to_le_bytes());
            p.extend_from_slice(&(pair.1 as i32).to_le_bytes());
            p.extend_from_slice(&pair.0.to_le_bytes());
            p.extend_from_slice(&pair.1.to_le_bytes());
            p.extend_from_slice(&(pair.0 as i32).to_le_bytes());
            p.extend_from_slice(&(pair.1 as i32).to_le_bytes());
            p.extend_from_slice(&pair.0.to_le_bytes());
            p.extend_from_slice(&(pair.1 as i32).to_le_bytes());
            p.extend_from_slice(&(pair.0 as i32).to_le_bytes());
            p.extend_from_slice(&pair.1.to_le_bytes());
            p.extend_from_slice(&(pair.0 as i32).to_le_bytes());
            p.extend_from_slice(&(pair.1 as i32).to_le_bytes());
            datetime_closeout_diff(&arm(6, &p));
        }
    }

    #[test]
    fn arms_smoke_typmod_inrange() {
        let _serial = crate::c_oracle_serial();
        for b0 in 0u8..8 {
            for flags in 0u8..4 {
                for v in [-3i32, -1, 0, 3, 6, 7, 9] {
                    let mut p = vec![b0, flags];
                    p.extend_from_slice(&v.to_le_bytes());
                    p.extend_from_slice(&v.to_le_bytes()); /* tmout */
                    p.extend_from_slice(&12i64.to_le_bytes()); /* val */
                    p.extend_from_slice(&34i64.to_le_bytes()); /* base */
                    p.extend_from_slice(&3_600_000_000i64.to_le_bytes());
                    p.extend_from_slice(&0i32.to_le_bytes());
                    p.extend_from_slice(&0i32.to_le_bytes());
                    p.push(flags);
                    datetime_closeout_diff(&arm(7, &p));
                    // negative-offset error arm + overflow saturation arm
                    let mut q = vec![b0, flags];
                    q.extend_from_slice(&v.to_le_bytes());
                    q.extend_from_slice(&(-1i32).to_le_bytes());
                    q.extend_from_slice(&0i64.to_le_bytes());
                    q.extend_from_slice(&i64::MAX.to_le_bytes());
                    q.extend_from_slice(&(-5i64).to_le_bytes());
                    q.extend_from_slice(&0i32.to_le_bytes());
                    q.extend_from_slice(&0i32.to_le_bytes());
                    q.push(flags);
                    datetime_closeout_diff(&arm(7, &q));
                    let mut r = vec![b0, flags];
                    r.extend_from_slice(&v.to_le_bytes());
                    r.extend_from_slice(&v.to_le_bytes());
                    r.extend_from_slice(&0i64.to_le_bytes());
                    r.extend_from_slice(&i64::MAX.to_le_bytes());
                    r.extend_from_slice(&i64::MAX.to_le_bytes());
                    r.extend_from_slice(&0i32.to_le_bytes());
                    r.extend_from_slice(&0i32.to_le_bytes());
                    r.push(flags);
                    datetime_closeout_diff(&arm(7, &r));
                }
            }
        }
    }

    /// The arms must actually REACH their compared entry points (the
    /// campaign's dead-arm class): each case here has a known non-trivial
    /// C-side verdict.
    #[test]
    fn arms_are_not_vacuous() {
        let _serial = crate::c_oracle_serial();
        super::super::datetime_io_diff::init_env_for_siblings();

        // extract_date('epoch', date 0) = 946684800 as a plain integer
        let (mut isnull, mut nval, mut nlog10, mut numset, mut inf) = (0, 0i64, 0, 0, 0);
        let rc = unsafe {
            pg_dtclo_extract_date(
                b"epoch".as_ptr(), 5, 0, &mut isnull, &mut nval, &mut nlog10, &mut numset,
                &mut inf,
            )
        };
        assert_eq!((rc, isnull, numset, inf), (0, 0, 1, 0));
        assert_eq!((nval, nlog10), (946_684_800, 0), "2000-01-01 unix epoch seconds");

        // extract_date('year', +infinity) records the Infinity literal
        let rc = unsafe {
            pg_dtclo_extract_date(
                b"year".as_ptr(), 4, i32::MAX, &mut isnull, &mut nval, &mut nlog10, &mut numset,
                &mut inf,
            )
        };
        assert_eq!((rc, isnull, inf), (0, 0, 1), "monotonic unit on +inf date");

        // extract_date('day', +infinity) is SQL NULL (oscillating unit)
        let rc = unsafe {
            pg_dtclo_extract_date(
                b"day".as_ptr(), 3, i32::MAX, &mut isnull, &mut nval, &mut nlog10, &mut numset,
                &mut inf,
            )
        };
        assert_eq!((rc, isnull), (0, 1));

        // extract_time('epoch', 0) = int64_div_fast_to_numeric(0, 6) -> 0.000000
        let (mut fval, mut nval, mut nlog10, mut numset) = (0f64, 0i64, 0, 0);
        let rc = unsafe {
            pg_dtclo_time_part(b"epoch".as_ptr(), 5, 0, 1, &mut fval, &mut nval, &mut nlog10,
                &mut numset)
        };
        assert_eq!((rc, numset, nval, nlog10), (0, 1, 0, 6));
        assert_eq!(expected_numeric_text(0, 6), "0.000000");

        // FRACTIONAL-PART witness: extract second of 12:34:56.789 has a real
        // fractional numeric text (56.789000) — the plane the lanel floor
        // never drove.
        let t = ((12 * 60 + 34) * 60 + 56) * 1_000_000i64 + 789_000;
        let rc = unsafe {
            pg_dtclo_time_part(b"second".as_ptr(), 6, t, 1, &mut fval, &mut nval, &mut nlog10,
                &mut numset)
        };
        assert_eq!((rc, numset, nlog10), (0, 1, 6));
        assert_eq!(expected_numeric_text(nval, nlog10), "56.789000");
        // and the whole plane end-to-end through the driver
        let mut p = t.to_le_bytes().to_vec();
        p.extend_from_slice(b"second");
        datetime_closeout_diff(&arm(1, &p));

        // timetz epoch folds the zone in: 01:00:00+00... zone -3600 (east 1h)
        let rc = unsafe {
            pg_dtclo_timetz_part(b"epoch".as_ptr(), 5, 3_600_000_000, -3600, 1, &mut fval,
                &mut nval, &mut nlog10, &mut numset)
        };
        assert_eq!((rc, numset, nlog10), (0, 1, 6));
        assert_eq!(nval, 3_600_000_000 - 3_600_000_000, "epoch of 01:00-east-1h is 0");

        // timetz 'timezone' unit: west-positive sign flip
        let rc = unsafe {
            pg_dtclo_timetz_part(b"timezone".as_ptr(), 8, 0, -3600, 0, &mut fval, &mut nval,
                &mut nlog10, &mut numset)
        };
        assert_eq!(rc, 0);
        assert_eq!(fval, 3600.0);

        // skip-support: NOBEGIN underflows, 0 decrements to -1
        let (mut uf, mut out) = (0, 0);
        unsafe { pg_dtclo_date_decrement(i32::MIN, &mut uf, &mut out) };
        assert_eq!(uf, 1);
        unsafe { pg_dtclo_date_decrement(0, &mut uf, &mut out) };
        assert_eq!((uf, out), (0, -1));
        let (mut of, mut out) = (0, 0);
        unsafe { pg_dtclo_date_increment(i32::MAX, &mut of, &mut out) };
        assert_eq!(of, 1);

        // wrapper sample: fc_date_recv over a valid BE frame returns the value
        let cx = mcx::MemoryContext::new("dtclo-test");
        let wire = 42i32.to_be_bytes();
        let mut si = wire_stringinfo(&cx, &wire);
        let fc = fc_call(
            db::fc_date_recv,
            [
                Datum::from_usize(&mut si as *mut _ as usize),
                Datum::from_i32(0),
                Datum::from_i32(-1),
            ],
        );
        assert_eq!(fc.0.unwrap().as_i32(), 42);
    }
}
