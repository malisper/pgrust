//! timestamp_diff: differential fuzz driver — shipped Rust `adt_timestamp`
//! vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_timestamp_io.c). Crate under test:
//! crates/backend/utils/adt/adt_timestamp.
//!
//! Environment pins (identical to datetime_io_diff, the family precedent):
//! session timezone GMT (Rust: real pgtz GMT zone; C: localtime-boundary
//! shims), current instant pinned to 2026-06-15 12:30:45.123456 GMT,
//! DateStyle/DateOrder/IntervalStyle fuzzed from selector bytes on BOTH
//! sides, tz database carved (non-GMT `pg_tzset` names leave the compared
//! domain; Rust still runs for panic-safety under a distinct-name
//! admission budget).
//!
//! Comparison planes: value bits/bytes + error-verdict + errcode/sqlstate
//! class; message text out of scope. extract_* arms compare pgrust's
//! rendered numeric text against the exact decimal the C numeric-boundary
//! shim recorded (int64 value + decimal scale); execs whose C path runs a
//! full numeric-op chain (epoch/julian) are verdict+sqlstate-only
//! (documented value-plane carve — see the oracle header).
//!
//! SKIPPED rows (state/SRF/planner carves; exception rows at gate):
//! now/statement_timestamp/clock_timestamp/timeofday (real clock/xact
//! state), generate_series_* (SRF machinery), pg_timezone_names/abbrevs
//! (SRF + tz database), timestamp_support/interval_support/
//! generate_series_timestamp_support (planner nodes).
//!
//! Input layout: [sel][payload]; sel % 26 picks the arm (see dispatch).

use std::ffi::CString;
use std::sync::Once;

use adt_datetime::consts::{
    INTSTYLE_ISO_8601, INTSTYLE_POSTGRES, INTSTYLE_POSTGRES_VERBOSE, INTSTYLE_SQL_STANDARD,
};
use adt_datetime::{
    set_date_order, set_date_style, set_interval_style, Interval, DATEORDER_DMY, DATEORDER_MDY,
    DATEORDER_YMD, USE_GERMAN_DATES, USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES,
    USE_XSD_DATES,
};
use adt_timestamp::builtins as tsb;
use adt_timestamp::interval as tsiv;
use adt_timestamp::{PartValue, TsBuf};
use datum::Datum;
use types_error::PgError;
use datum::NullableDatum;
use types_fmgr::{FunctionCallInfoBaseData, LocalFcinfo};

type PGFunction = fn(
    Option<&mut types_fmgr::FmgrInfo>,
    &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum>;

extern "C" {
    fn pg_tsdiff_timestamp_in(
        s: *const std::ffi::c_char,
        typmod: i32,
        style: i32,
        order: i32,
        tz: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_timestamp_out(ts: i64, style: i32, order: i32, tz: i32, buf: *mut u8) -> i32;
    fn pg_tsdiff_interval_in(
        s: *const std::ffi::c_char,
        typmod: i32,
        istyle: i32,
        t: *mut i64,
        day: *mut i32,
        month: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_out(t: i64, day: i32, month: i32, istyle: i32, buf: *mut u8) -> i32;
    fn pg_tsdiff_timestamp_recv(
        bytes: *const u8,
        len: i32,
        typmod: i32,
        tz: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_timestamp_send(ts: i64, out8: *mut u8) -> i32;
    fn pg_tsdiff_interval_recv(
        bytes: *const u8,
        len: i32,
        typmod: i32,
        t: *mut i64,
        day: *mut i32,
        month: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_send(t: i64, day: i32, month: i32, out16: *mut u8) -> i32;
    fn pg_tsdiff_timestamp_scale(ts: i64, typmod: i32, out: *mut i64) -> i32;
    fn pg_tsdiff_interval_scale(
        t: i64,
        day: i32,
        month: i32,
        typmod: i32,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_timestamp_trunc(
        units: *const std::ffi::c_char,
        ulen: i32,
        ts: i64,
        tz: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_timestamptz_trunc_zone(
        units: *const std::ffi::c_char,
        ulen: i32,
        zone: *const std::ffi::c_char,
        zlen: i32,
        ts: i64,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_interval_trunc(
        units: *const std::ffi::c_char,
        ulen: i32,
        t: i64,
        day: i32,
        month: i32,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_ts_part(
        units: *const std::ffi::c_char,
        ulen: i32,
        ts: i64,
        tz: i32,
        retnumeric: i32,
        fval: *mut f64,
        isnull: *mut i32,
        nval: *mut i64,
        nlog10: *mut i32,
        numset: *mut i32,
        numchain: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_part(
        units: *const std::ffi::c_char,
        ulen: i32,
        t: i64,
        day: i32,
        month: i32,
        retnumeric: i32,
        fval: *mut f64,
        isnull: *mut i32,
        nval: *mut i64,
        nlog10: *mut i32,
        numset: *mut i32,
        numchain: *mut i32,
    ) -> i32;
    fn pg_tsdiff_timestamp_age(
        a: i64,
        b: i64,
        tz: i32,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_make_timestamp(
        y: i32,
        mo: i32,
        d: i32,
        h: i32,
        mi: i32,
        sec: f64,
        tz: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_make_timestamptz_at_timezone(
        y: i32,
        mo: i32,
        d: i32,
        h: i32,
        mi: i32,
        sec: f64,
        zone: *const std::ffi::c_char,
        zlen: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_make_interval(
        y: i32,
        mo: i32,
        w: i32,
        d: i32,
        h: i32,
        mi: i32,
        sec: f64,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_muldiv(
        isdiv: i32,
        t: i64,
        day: i32,
        month: i32,
        factor: f64,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_timestamp_mi(a: i64, b: i64, ot: *mut i64, od: *mut i32, om: *mut i32) -> i32;
    fn pg_tsdiff_timestamp_difference(
        start: i64,
        stop: i64,
        osecs: *mut i64,
        ousecs: *mut i32,
    ) -> i32;
    fn pg_tsdiff_timestamp_difference_ms(start: i64, stop: i64) -> i64;
    fn pg_tsdiff_timestamp_difference_exceeds(start: i64, stop: i64, msec: i32) -> i32;
    fn pg_tsdiff_timestamp_difference_exceeds_secs(
        start: i64,
        stop: i64,
        threshold_sec: i32,
    ) -> i32;
    fn pg_tsdiff_timestamp_plmi_interval(
        tz: i32,
        ismi: i32,
        ts: i64,
        t: i64,
        day: i32,
        month: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_justify(
        which: i32,
        t: i64,
        day: i32,
        month: i32,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_timestamp_bin(
        tz: i32,
        st: i64,
        sd: i32,
        sm: i32,
        ts: i64,
        origin: i64,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_interval_um(
        t: i64,
        day: i32,
        month: i32,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_plmi(
        ismi: i32,
        t1: i64,
        d1: i32,
        m1: i32,
        t2: i64,
        d2: i32,
        m2: i32,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_minmax(
        larger: i32,
        t1: i64,
        d1: i32,
        m1: i32,
        t2: i64,
        d2: i32,
        m2: i32,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
        cmp: *mut i32,
    ) -> i32;
    fn pg_tsdiff_timestamp_izone(
        tz: i32,
        zt: i64,
        zd: i32,
        zm: i32,
        ts: i64,
        out: *mut i64,
    ) -> i32;
    fn pg_tsdiff_interval_agg(
        op: i32,
        n: *mut i64,
        st: *mut i64,
        sd: *mut i32,
        sm: *mut i32,
        pinf: *mut i64,
        ninf: *mut i64,
        t: i64,
        day: i32,
        month: i32,
    ) -> i32;
    fn pg_tsdiff_interval_avg_final(
        issum: i32,
        n: i64,
        st: i64,
        sd: i32,
        sm: i32,
        pinf: i64,
        ninf: i64,
        ot: *mut i64,
        od: *mut i32,
        om: *mut i32,
        isnull: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_avg_combine(
        n1: i64,
        st1: i64,
        sd1: i32,
        sm1: i32,
        pinf1: i64,
        ninf1: i64,
        n2: i64,
        st2: i64,
        sd2: i32,
        sm2: i32,
        pinf2: i64,
        ninf2: i64,
        n: *mut i64,
        st: *mut i64,
        sd: *mut i32,
        sm: *mut i32,
        pinf: *mut i64,
        ninf: *mut i64,
    ) -> i32;
    fn pg_tsdiff_interval_avg_serialize(
        n: i64,
        st: i64,
        sd: i32,
        sm: i32,
        pinf: i64,
        ninf: i64,
        out: *mut u8,
        outlen: *mut i32,
    ) -> i32;
    fn pg_tsdiff_interval_avg_deserialize(
        bytes: *const u8,
        len: i32,
        n: *mut i64,
        st: *mut i64,
        sd: *mut i32,
        sm: *mut i32,
        pinf: *mut i64,
        ninf: *mut i64,
    ) -> i32;
    fn pg_tsdiff_tz_carved() -> i32;
    fn pg_tsdiff_tz_carved_name() -> *const std::ffi::c_char;
}

/// Pinned "current" instant: 2026-06-15 12:30:45.123456 GMT (matches the C
/// shims and lanel's datetime family pins).
const PINNED_NOW_USECS: i64 = 9662 * 86_400_000_000 + 45_045_000_000 + 123_456;

fn init_env() {
    // ONE definition of the pinned environment (lane merge, 2026-07-31):
    // this used to be a byte-identical copy of datetime_io_diff's init_env
    // (same GMT/clock/tz-database pins mirroring the same C shims), and with
    // both lanes' modules linked into one test binary the duplicate
    // `pgtz::init_seams()` panicked ("seam installed twice"). Delegate to
    // the shared sibling init instead — the PGRUST_TZDIR value differs only
    // in the (nonexistent) directory name, which is semantics-free.
    super::datetime_io_diff::init_env_for_siblings();
}

/// Rust-side run admission for tz-carved execs (RSS bound; see
/// datetime_io_diff — pgrust's pg_tzset cache is process-lifetime by
/// design, C parity with pgtz.c's never-evicted HTAB).
fn admit_tz_carved_exec() -> bool {
    const BUDGET: usize = 2048;
    use std::cell::RefCell;
    use std::collections::HashSet;
    std::thread_local! {
        static SEEN: RefCell<HashSet<Vec<u8>>> = RefCell::new(HashSet::new());
    }
    // SAFETY: the oracle keeps this NUL-terminated static for the exec.
    let name = unsafe { std::ffi::CStr::from_ptr(pg_tsdiff_tz_carved_name()) }.to_bytes();
    SEEN.with(|s| {
        let mut s = s.borrow_mut();
        if s.contains(name) {
            return true;
        }
        if s.len() >= BUDGET {
            return false;
        }
        s.insert(name.to_vec());
        true
    })
}

/// C oracle errcode classes (csrc/pg_timestamp_io.c header).
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
    } else if e.sqlstate == ERRCODE_CONFIG_FILE_ERROR {
        7
    } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
        8
    } else if e.sqlstate == ERRCODE_DIVISION_BY_ZERO {
        9
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        10
    } else {
        98 /* unmapped: always a divergence against the C classes */
    }
}

/// (DateStyle, DateOrder) from the style byte — applied to BOTH sides.
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

/// timestamp[tz] typmod domain: -1 (unconstrained) + 0..=6 (the
/// catalog-reachable set; >6 raises through typmodin, proved 2905/2907).
fn ts_typmod(b: u8) -> i32 {
    match b % 8 {
        0 => -1,
        n => (n - 1) as i32,
    }
}

/// Interval typmod: the values intervaltypmodin can actually produce —
/// (range << 16) | precision, range from the mask set, precision 0..6 or
/// full; high bit = -1 (unconstrained).
fn interval_typmod(b: u8, b2: u8) -> i32 {
    // datetime.h token-type codes (utils/timestamp.h INTERVAL_MASK basis)
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
        0 => 0x7FFF, /* INTERVAL_FULL_RANGE */
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
        0 => 0xFFFF, /* INTERVAL_FULL_PRECISION */
        n => (n - 1) as i32,
    };
    if b2 & 0x80 != 0 {
        -1
    } else {
        (range << 16) | precision
    }
}

/// Text payload guard: interior-NUL-free valid UTF-8 (the server validates
/// client encoding long before datatype input), capped at 200 bytes.
fn text_payload(b: &[u8]) -> Option<(&str, CString)> {
    if b.len() > 200 || b.contains(&0) {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    Some((s, CString::new(b).unwrap()))
}

/// Unit-name payload: ≤ 32 bytes keeps C's NAMEDATALEN truncation arm out
/// (unreachable-by-cap on both sides; the truncate stub aborts loudly).
fn units_payload(b: &[u8]) -> Option<(&str, CString)> {
    if b.len() > 32 || b.contains(&0) {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    Some((s, CString::new(b).unwrap()))
}

/// Zone-name payload for the zone-taking arms: names of NAMEDATALEN-1 (63)
/// and longer reach C's truncate_identifier (stubbed abort in the oracle),
/// so the COMPARED domain is < 64 bytes; longer names run Rust-only for
/// panic-safety (returned with cstring None), bounded by the same
/// admission budget as tz-carved execs.
fn zone_payload(b: &[u8]) -> Option<(&str, Option<CString>)> {
    if b.len() > 200 || b.contains(&0) {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    if b.is_empty() {
        return None;
    }
    if b.len() >= 64 {
        return Some((s, None));
    }
    Some((s, Some(CString::new(b).unwrap())))
}

fn rd_i64(b: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

fn rd_i32(b: &[u8], off: usize) -> i32 {
    i32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}

fn rd_f64(b: &[u8], off: usize) -> f64 {
    f64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

/// Exact decimal string determined by the C numeric-constructor record
/// (int64_div_fast_to_numeric semantics: val1 / 10^log10, dscale=log10).
/// pub(crate): shared with datetime_closeout_diff's extract planes.
pub(crate) fn expected_numeric_text(val: i64, log10: i32) -> String {
    let neg = val < 0;
    let abs = (val as i128).unsigned_abs();
    let pow = 10u128.pow(log10 as u32);
    let ip = abs / pow;
    let fp = abs % pow;
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    s.push_str(&ip.to_string());
    if log10 > 0 {
        s.push('.');
        s.push_str(&format!("{:0width$}", fp, width = log10 as usize));
    }
    s
}

/// Render pgrust's NumericImage as PG text (numeric encoding is
/// adt/numeric's verified surface; here it is the comparison channel).
/// pub(crate): shared with datetime_closeout_diff's extract planes.
pub(crate) fn numeric_image_text(img: &adt_numeric::NumericImage) -> String {
    let mut out = Vec::new();
    adt_numeric::io::numeric_out_into(adt_numeric::Num::from_payload(img.payload()), &mut out);
    String::from_utf8(out).unwrap()
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo)
// ---------------------------------------------------------------------------

fn fc_call<const N: usize>(
    f: PGFunction,
    args: [Datum; N],
) -> (types_error::PgResult<Datum>, bool) {
    let cx = mcx::MemoryContext::new("timestamp_diff_fc");
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: cx outlives this single call (function scope).
    unsafe { fcinfo.set_result_mcx(cx.mcx()) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

fn fc_check_i64(
    arm: &str,
    core: &types_error::PgResult<i64>,
    fc: (types_error::PgResult<Datum>, bool),
) {
    match (core, &fc.0) {
        (Ok(cv), Ok(fv)) => assert!(
            *cv == fv.as_i64(),
            "{arm} FC-PLANE DIVERGENCE: core={cv:x} fc={:x}",
            fv.as_i64()
        ),
        (Err(ce), Err(fe)) => assert!(
            ce.sqlstate == fe.sqlstate,
            "{arm} FC-PLANE sqlstate: core={:?} fc={:?}",
            ce.sqlstate,
            fe.sqlstate
        ),
        _ => panic!(
            "{arm} FC-PLANE verdict mismatch: core.ok={} fc.ok={}",
            core.is_ok(),
            fc.0.is_ok()
        ),
    }
}

fn datum_cstr_bytes<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: the wrapper returned a NUL-terminated cstring allocation live
    // in the fc-call context (read before the context drops).
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const std::ffi::c_char).to_bytes() }
}

fn datum_interval<'a>(d: Datum) -> &'a Interval {
    // SAFETY: interval wrappers return a by-ref 16-byte Interval allocation
    // live in the fc-call context.
    unsafe { &*(d.as_usize() as *const Interval) }
}

/// Build a 4B-header text varlena image for wrapper text args (lives for
/// the fc call; the wrapper only borrows it).
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

/// Wrapper-plane check for Interval-returning fc wrappers against the
/// already-C-checked core result.
fn fc_check_interval(
    arm: &str,
    core: &types_error::PgResult<Interval>,
    fc: (types_error::PgResult<Datum>, bool),
) {
    match (core, &fc.0) {
        (Ok(cv), Ok(fv)) => {
            let fv = datum_interval(*fv);
            assert!(
                (cv.time, cv.day, cv.month) == (fv.time, fv.day, fv.month),
                "{arm} FC-PLANE interval value mismatch"
            );
        }
        (Err(ce), Err(fe)) => {
            assert!(ce.sqlstate == fe.sqlstate, "{arm} FC-PLANE sqlstate mismatch")
        }
        _ => panic!("{arm} FC-PLANE verdict mismatch"),
    }
}

/// Wrapper-plane check for PartValue-returning fc wrappers.
fn fc_check_part(
    arm: &str,
    core: &types_error::PgResult<PartValue>,
    fc: (types_error::PgResult<Datum>, bool),
) {
    match (core, &fc.0) {
        (Ok(PartValue::Null), Ok(_)) => assert!(fc.1, "{arm} FC-PLANE null mismatch"),
        (Ok(PartValue::Float(cv)), Ok(fv)) => assert!(
            cv.to_bits() == fv.as_f64().to_bits(),
            "{arm} FC-PLANE float mismatch"
        ),
        (Ok(PartValue::Numeric(img)), Ok(fv)) => {
            // wrapper returns a by-ref numeric varlena; compare payloads
            // via the rendered text (same channel as the C plane).
            // SAFETY: live varlena in the fc-call context.
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

/// One (errclass, value) verdict compare for i64-valued arms.
fn check_i64(arm: &str, cerr: i32, cval: i64, rres: &types_error::PgResult<i64>) {
    match rres {
        Ok(v) => assert!(
            cerr == 0 && cval == *v,
            "{arm} DIVERGENCE: C(err={cerr}, val={cval:x}) vs Rust Ok({v:x})"
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc && cerr != 0,
                "{arm} DIVERGENCE: C err={cerr} vs Rust Err(class={rc}, sqlstate={:?})",
                e.sqlstate
            );
        }
    }
}

fn check_interval(
    arm: &str,
    cerr: i32,
    cv: (i64, i32, i32),
    rres: &types_error::PgResult<Interval>,
) {
    match rres {
        Ok(v) => assert!(
            cerr == 0 && cv == (v.time, v.day, v.month),
            "{arm} DIVERGENCE: C(err={cerr}, val={cv:?}) vs Rust Ok(({},{},{}))",
            v.time,
            v.day,
            v.month
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc && cerr != 0,
                "{arm} DIVERGENCE: C err={cerr} vs Rust Err(class={rc}, sqlstate={:?})",
                e.sqlstate
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn timestamp_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init_env();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 26 {
        0 => ts_in_arm(payload),
        1 => ts_out_arm(payload),
        2 => interval_in_arm(payload),
        3 => interval_out_arm(payload),
        4 => ts_recv_arm(payload),
        5 => ts_send_arm(payload),
        6 => interval_recv_arm(payload),
        7 => interval_send_arm(payload),
        8 => ts_scale_arm(payload),
        9 => interval_scale_arm(payload),
        10 => ts_trunc_arm(payload),
        11 => tstz_trunc_zone_arm(payload),
        12 => interval_trunc_arm(payload),
        13 => ts_part_arm(payload),
        14 => interval_part_arm(payload),
        15 => ts_age_arm(payload),
        16 => make_ts_arm(payload),
        17 => make_tstz_at_zone_arm(payload),
        18 => make_interval_arm(payload),
        19 => interval_muldiv_arm(payload),
        20 => timestamp_mi_arm(payload),
        21 => ts_plmi_interval_arm(payload),
        22 => justify_arm(payload),
        23 => ts_bin_arm(payload),
        24 => interval_unops_arm(payload),
        _ => interval_agg_arm(payload),
    }
}

/// tz-carve check: false when the exec left the compared domain (the C
/// side consulted pg_tzset with a non-GMT name).
fn tz_in_domain() -> bool {
    // SAFETY: plain TLS read on the oracle side.
    unsafe { pg_tsdiff_tz_carved() == 0 }
}

fn ts_in_arm(payload: &[u8]) {
    let Some((&sb, rest)) = payload.split_first() else { return };
    let Some((&tb, rest)) = rest.split_first() else { return };
    let Some((&tzb, text)) = rest.split_first() else { return };
    let (style, order) = styles(sb);
    let typmod = ts_typmod(tb);
    let tz = (tzb & 1) as i32;
    let Some((s, cs)) = text_payload(text) else { return };

    let mut cval = 0i64;
    // SAFETY: NUL-terminated cstring + out pointer valid for the call.
    let cerr = unsafe { pg_tsdiff_timestamp_in(cs.as_ptr(), typmod, style, order, tz, &mut cval) };
    if !tz_in_domain() {
        if admit_tz_carved_exec() {
            let _ = if tz == 1 {
                adt_timestamp::timestamptz_in(s, typmod, None)
            } else {
                adt_timestamp::timestamp_in(s, typmod, None)
            };
        }
        return;
    }
    let r = if tz == 1 {
        adt_timestamp::timestamptz_in(s, typmod, None)
    } else {
        adt_timestamp::timestamp_in(s, typmod, None)
    };
    check_i64("timestamp_in", cerr, cval, &r);

    // fc plane
    let f: PGFunction = if tz == 1 { tsb::fc_timestamptz_in } else { tsb::fc_timestamp_in };
    let fc = fc_call(
        f,
        [Datum::from_usize(cs.as_ptr() as usize), Datum::from_i32(0), Datum::from_i32(typmod)],
    );
    fc_check_i64("timestamp_in", &r, fc);

    // soft-error plane: SQL soft input (COPY ... ON_ERROR ignore) must
    // reach the same verdict/sqlstate as the hard path, never throw.
    let mut esc = types_error::SoftErrorContext::new(true);
    let rs = if tz == 1 {
        adt_timestamp::timestamptz_in(s, typmod, Some(&mut esc))
    } else {
        adt_timestamp::timestamp_in(s, typmod, Some(&mut esc))
    };
    match (&r, &rs) {
        (Ok(hv), Ok(sv)) => {
            assert!(!esc.error_occurred() && hv == sv, "timestamp_in SOFT-PLANE value mismatch")
        }
        (Err(he), Ok(_)) => assert!(
            esc.error_occurred()
                && esc.error().map(|e| e.sqlstate) == Some(he.sqlstate),
            "timestamp_in SOFT-PLANE verdict/sqlstate mismatch"
        ),
        (_, Err(se)) => {
            // soft mode may still hard-error for non-softenable raisers;
            // then it must be the SAME error.
            assert!(
                r.as_ref().err().map(|e| e.sqlstate) == Some(se.sqlstate),
                "timestamp_in SOFT-PLANE hard-error mismatch"
            );
        }
    }
}

fn ts_out_arm(payload: &[u8]) {
    if payload.len() < 10 {
        return;
    }
    let (style, order) = styles(payload[0]);
    let tz = (payload[1] & 1) as i32;
    let ts = rd_i64(payload, 2);

    let mut cbuf = [0u8; 160];
    // SAFETY: MAXDATELEN+1 <= 160; C writes a NUL-terminated string.
    let cerr = unsafe { pg_tsdiff_timestamp_out(ts, style, order, tz, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();

    let mut rbuf: TsBuf = [0u8; adt_datetime::MAXDATELEN + 1];
    let r = if tz == 1 {
        adt_timestamp::timestamptz_out(ts, &mut rbuf)
    } else {
        adt_timestamp::timestamp_out(ts, &mut rbuf)
    };
    match &r {
        Ok(len) => assert!(
            cerr == 0 && cbuf[..clen] == rbuf[..*len],
            "timestamp_out DIVERGENCE ts={ts}: C(err={cerr}, {:?}) vs Rust({:?})",
            String::from_utf8_lossy(&cbuf[..clen]),
            String::from_utf8_lossy(&rbuf[..*len])
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(cerr == rc, "timestamp_out verdict: C err={cerr} vs Rust class={rc}");
        }
    }

    // fc plane
    let f: PGFunction = if tz == 1 { tsb::fc_timestamptz_out } else { tsb::fc_timestamp_out };
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

/// PLATFORM CARVE (documented; oracle of record = glibc): strtod's ERANGE
/// on underflow uses tininess-BEFORE-rounding on glibc but AFTER-rounding
/// on macOS, so a token whose true value sits just below DBL_MIN and
/// rounds UP to it ('0x1.fffffffffffffp-1023') gets errno=ERANGE on glibc
/// (real 18.3 answers 22007 — docker-verified) and errno=0 on macOS. The
/// shipped Rust model follows glibc; execs whose text carries a token
/// rounding to ±DBL_MIN leave the LOCAL (macOS) compared domain. On the
/// fleet (glibc) the carve never fires with a differing verdict.
fn dblmin_boundary(text: &[u8]) -> bool {
    for i in 0..text.len() {
        if let Some(tok) = adt_float::scan_number(&text[i..]) {
            let t = &text[i..i + tok.len];
            let v = match tok.kind {
                adt_float::NumKind::Decimal => {
                    std::str::from_utf8(t).ok().and_then(|s| s.parse::<f64>().ok())
                }
                adt_float::NumKind::Hex => Some(adt_float::parse_hex_float(t)),
            };
            if v.is_some_and(|v| v.abs() == f64::MIN_POSITIVE) {
                return true;
            }
        }
    }
    false
}

fn interval_in_arm(payload: &[u8]) {
    let Some((&ib, rest)) = payload.split_first() else { return };
    let Some((&tb, rest)) = rest.split_first() else { return };
    let Some((&tb2, text)) = rest.split_first() else { return };
    let is = istyle(ib);
    let typmod = interval_typmod(tb, tb2);
    let Some((s, cs)) = text_payload(text) else { return };

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: cstring + out pointers valid for the call.
    let cerr = unsafe { pg_tsdiff_interval_in(cs.as_ptr(), typmod, is, &mut ct, &mut cd, &mut cm) };
    let r = tsiv::interval_in(s, typmod, None);
    if dblmin_boundary(s.as_bytes()) {
        return; /* strtod tininess platform carve — see dblmin_boundary */
    }
    check_interval("interval_in", cerr, (ct, cd, cm), &r);

    // soft-error plane (as ts_in_arm)
    let mut esc = types_error::SoftErrorContext::new(true);
    let rs = tsiv::interval_in(s, typmod, Some(&mut esc));
    match (&r, &rs) {
        (Ok(hv), Ok(sv)) => assert!(
            !esc.error_occurred() && (hv.time, hv.day, hv.month) == (sv.time, sv.day, sv.month),
            "interval_in SOFT-PLANE value mismatch"
        ),
        (Err(he), Ok(_)) => assert!(
            esc.error_occurred()
                && esc.error().map(|e| e.sqlstate) == Some(he.sqlstate),
            "interval_in SOFT-PLANE verdict/sqlstate mismatch"
        ),
        (_, Err(se)) => assert!(
            r.as_ref().err().map(|e| e.sqlstate) == Some(se.sqlstate),
            "interval_in SOFT-PLANE hard-error mismatch"
        ),
    }
}

fn interval_out_arm(payload: &[u8]) {
    if payload.len() < 17 {
        return;
    }
    let is = istyle(payload[0]);
    let t = rd_i64(payload, 1);
    let d = rd_i32(payload, 9);
    let m = rd_i32(payload, 13);
    let iv = Interval { time: t, day: d, month: m };

    let mut cbuf = [0u8; 160];
    // SAFETY: buffer covers MAXDATELEN+1; C writes NUL-terminated.
    let cerr = unsafe { pg_tsdiff_interval_out(t, d, m, is, cbuf.as_mut_ptr()) };
    assert!(cerr == 0, "interval_out C unexpectedly errored: {cerr}");
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();

    let mut rbuf: TsBuf = [0u8; adt_datetime::MAXDATELEN + 1];
    let len = tsiv::interval_out(&iv, &mut rbuf);
    assert!(
        cbuf[..clen] == rbuf[..len],
        "interval_out DIVERGENCE ({t},{d},{m}) style={is}: C {:?} vs Rust {:?}",
        String::from_utf8_lossy(&cbuf[..clen]),
        String::from_utf8_lossy(&rbuf[..len])
    );
}

fn ts_recv_arm(payload: &[u8]) {
    let Some((&tb, rest)) = payload.split_first() else { return };
    let Some((&tzb, wire)) = rest.split_first() else { return };
    if wire.len() > 24 {
        return;
    }
    let typmod = ts_typmod(tb);
    let tz = (tzb & 1) as i32;

    let mut cval = 0i64;
    // SAFETY: wire buffer + out pointer valid for the call.
    let cerr = unsafe {
        pg_tsdiff_timestamp_recv(wire.as_ptr(), wire.len() as i32, typmod, tz, &mut cval)
    };

    let cx = mcx::MemoryContext::new("timestamp_diff_recv");
    let mut vec = mcx::PgVec::new_in(cx.mcx());
    vec.try_reserve_exact(wire.len() + 1).unwrap();
    vec.extend_from_slice(wire);
    let mut si = stringinfo::StringInfo::from_vec(vec).unwrap();
    let r = if tz == 1 {
        adt_timestamp::timestamptz_recv(&mut si, typmod)
    } else {
        adt_timestamp::timestamp_recv(&mut si, typmod)
    };
    check_i64("timestamp_recv", cerr, cval, &r);

    // fc plane: recv wrappers take the live StringInfo pointer (recv ABI).
    let mut vec2 = mcx::PgVec::new_in(cx.mcx());
    vec2.try_reserve_exact(wire.len() + 1).unwrap();
    vec2.extend_from_slice(wire);
    let mut si2 = stringinfo::StringInfo::from_vec(vec2).unwrap();
    let f: PGFunction = if tz == 1 { tsb::fc_timestamptz_recv } else { tsb::fc_timestamp_recv };
    let fc = fc_call(
        f,
        [
            Datum::from_usize(&mut si2 as *mut _ as usize),
            Datum::from_i32(0),
            Datum::from_i32(typmod),
        ],
    );
    fc_check_i64("timestamp_recv", &r, fc);
}

fn ts_send_arm(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let ts = rd_i64(payload, 0);
    let mut cw = [0u8; 8];
    // SAFETY: out buffer is 8 bytes as the entry requires.
    let cerr = unsafe { pg_tsdiff_timestamp_send(ts, cw.as_mut_ptr()) };
    assert!(cerr == 0);

    let cx = mcx::MemoryContext::new("timestamp_diff_send");
    let b = adt_timestamp::timestamp_send(cx.mcx(), ts).unwrap();
    assert!(
        b.data() == cw,
        "timestamp_send DIVERGENCE ts={ts}: C {:02x?} vs Rust {:02x?}",
        cw,
        b.data()
    );

    // fc plane: wrapper's varlena payload equals the core image.
    let fc = fc_call(tsb::fc_timestamp_send, [Datum::from_i64(ts)]);
    let d = fc.0.expect("fc_timestamp_send cannot fail");
    // SAFETY: live 4B-header varlena in the fc-call context.
    let hdr = unsafe { std::slice::from_raw_parts(d.as_usize() as *const u8, 4) };
    let vlen = (u32::from_le_bytes(hdr.try_into().unwrap()) >> 2) as usize;
    // SAFETY: payload follows the header.
    let pay = unsafe { std::slice::from_raw_parts((d.as_usize() + 4) as *const u8, vlen - 4) };
    assert!(pay == cw, "timestamp_send FC-PLANE image mismatch");
}

fn interval_recv_arm(payload: &[u8]) {
    let Some((&tb, rest)) = payload.split_first() else { return };
    let Some((&tb2, wire)) = rest.split_first() else { return };
    if wire.len() > 32 {
        return;
    }
    let typmod = interval_typmod(tb, tb2);

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: wire buffer + out pointers valid for the call.
    let cerr = unsafe {
        pg_tsdiff_interval_recv(wire.as_ptr(), wire.len() as i32, typmod, &mut ct, &mut cd, &mut cm)
    };

    let cx = mcx::MemoryContext::new("timestamp_diff_ivrecv");
    let mut vec = mcx::PgVec::new_in(cx.mcx());
    vec.try_reserve_exact(wire.len() + 1).unwrap();
    vec.extend_from_slice(wire);
    let mut si = stringinfo::StringInfo::from_vec(vec).unwrap();
    let r = tsiv::interval_recv(&mut si, typmod);
    check_interval("interval_recv", cerr, (ct, cd, cm), &r);
}

fn interval_send_arm(payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let iv =
        Interval { time: rd_i64(payload, 0), day: rd_i32(payload, 8), month: rd_i32(payload, 12) };
    let mut cw = [0u8; 16];
    // SAFETY: out buffer is 16 bytes as the entry requires.
    let cerr = unsafe { pg_tsdiff_interval_send(iv.time, iv.day, iv.month, cw.as_mut_ptr()) };
    assert!(cerr == 0);

    let cx = mcx::MemoryContext::new("timestamp_diff_ivsend");
    let b = tsiv::interval_send(cx.mcx(), &iv).unwrap();
    assert!(
        b.data() == cw,
        "interval_send DIVERGENCE ({},{},{}): C {:02x?} vs Rust {:02x?}",
        iv.time,
        iv.day,
        iv.month,
        cw,
        b.data()
    );
}

/// timestamp_scale's argument carries the timestamp type INVARIANT (every
/// SQL route into it — cast from a stored/computed timestamp — is already
/// range-checked), so fold raw i64 into valid ∪ {NOBEGIN, NOEND}. Outside
/// that domain the C rounding expression wraps (-fwrapv) while Rust's
/// debug build panics; release Rust wraps identically, so the SQL-visible
/// behavior matches — the domain is folded rather than the check weakened
/// (datetime_io_diff fold_date/fold_time precedent).
fn fold_ts(raw: i64) -> i64 {
    const MIN_TS: i64 = -211_813_488_000_000_000;
    const END_TS: i64 = 9_223_371_331_200_000_000;
    if raw == i64::MIN || raw == i64::MAX {
        return raw;
    }
    ((raw as i128).rem_euclid((END_TS as i128) - (MIN_TS as i128)) + (MIN_TS as i128)) as i64
}

fn ts_scale_arm(payload: &[u8]) {
    if payload.len() < 9 {
        return;
    }
    let typmod = ts_typmod(payload[0]);
    let ts = fold_ts(rd_i64(payload, 1));
    let mut cval = 0i64;
    // SAFETY: out pointer valid for the call.
    let cerr = unsafe { pg_tsdiff_timestamp_scale(ts, typmod, &mut cval) };
    let r = adt_timestamp::timestamp_scale(ts, typmod);
    check_i64("timestamp_scale", cerr, cval, &r);

    let fc = fc_call(tsb::fc_timestamp_scale, [Datum::from_i64(ts), Datum::from_i32(typmod)]);
    fc_check_i64("timestamp_scale", &r, fc);
}

fn interval_scale_arm(payload: &[u8]) {
    if payload.len() < 18 {
        return;
    }
    let typmod = interval_typmod(payload[0], payload[1]);
    let iv =
        Interval { time: rd_i64(payload, 2), day: rd_i32(payload, 10), month: rd_i32(payload, 14) };
    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: out pointers valid for the call.
    let cerr = unsafe {
        pg_tsdiff_interval_scale(iv.time, iv.day, iv.month, typmod, &mut ct, &mut cd, &mut cm)
    };
    let r = tsiv::interval_scale(&iv, typmod);
    check_interval("interval_scale", cerr, (ct, cd, cm), &r);
}

fn ts_trunc_arm(payload: &[u8]) {
    let Some((&tzb, rest)) = payload.split_first() else { return };
    if rest.len() < 8 {
        return;
    }
    let tz = (tzb & 1) as i32;
    let ts = rd_i64(rest, 0);
    let Some((units, cu)) = units_payload(&rest[8..]) else { return };

    let mut cval = 0i64;
    // SAFETY: units buffer + out pointer valid for the call.
    let cerr =
        unsafe { pg_tsdiff_timestamp_trunc(cu.as_ptr(), units.len() as i32, ts, tz, &mut cval) };
    let r = if tz == 1 {
        adt_timestamp::timestamptz_trunc(units.as_bytes(), ts)
    } else {
        adt_timestamp::timestamp_trunc(units.as_bytes(), ts)
    };
    check_i64("timestamp_trunc", cerr, cval, &r);

    // fc plane
    let uv = text_varlena(units.as_bytes());
    let f: PGFunction = if tz == 1 { tsb::fc_timestamptz_trunc } else { tsb::fc_timestamp_trunc };
    let fc = fc_call(f, [Datum::from_usize(uv.as_ptr() as usize), Datum::from_i64(ts)]);
    fc_check_i64("timestamp_trunc", &r, fc);
}

fn tstz_trunc_zone_arm(payload: &[u8]) {
    if payload.len() < 10 {
        return;
    }
    let ts = rd_i64(payload, 0);
    let ulen = (payload[8] % 16) as usize;
    let rest = &payload[9..];
    if rest.len() < ulen {
        return;
    }
    let Some((units, cu)) = units_payload(&rest[..ulen]) else { return };
    let Some((zone, cz)) = zone_payload(&rest[ulen..]) else { return };
    let Some(cz) = cz else {
        if admit_tz_carved_exec() {
            let _ = adt_timestamp::timestamptz_trunc_zone(units.as_bytes(), ts, zone.as_bytes());
        }
        return;
    };

    let mut cval = 0i64;
    // SAFETY: unit/zone buffers + out pointer valid for the call.
    let cerr = unsafe {
        pg_tsdiff_timestamptz_trunc_zone(
            cu.as_ptr(),
            units.len() as i32,
            cz.as_ptr(),
            zone.len() as i32,
            ts,
            &mut cval,
        )
    };
    if !tz_in_domain() {
        if admit_tz_carved_exec() {
            let _ = adt_timestamp::timestamptz_trunc_zone(units.as_bytes(), ts, zone.as_bytes());
        }
        return;
    }
    let r = adt_timestamp::timestamptz_trunc_zone(units.as_bytes(), ts, zone.as_bytes());
    check_i64("timestamptz_trunc_zone", cerr, cval, &r);

    // fc plane
    let uv = text_varlena(units.as_bytes());
    let zv = text_varlena(zone.as_bytes());
    let fc = fc_call(
        tsb::fc_timestamptz_trunc_zone,
        [
            Datum::from_usize(uv.as_ptr() as usize),
            Datum::from_i64(ts),
            Datum::from_usize(zv.as_ptr() as usize),
        ],
    );
    fc_check_i64("timestamptz_trunc_zone", &r, fc);
}

fn interval_trunc_arm(payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let iv =
        Interval { time: rd_i64(payload, 0), day: rd_i32(payload, 8), month: rd_i32(payload, 12) };
    let Some((units, cu)) = units_payload(&payload[16..]) else { return };

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: units buffer + out pointers valid for the call.
    let cerr = unsafe {
        pg_tsdiff_interval_trunc(
            cu.as_ptr(),
            units.len() as i32,
            iv.time,
            iv.day,
            iv.month,
            &mut ct,
            &mut cd,
            &mut cm,
        )
    };
    let r = tsiv::interval_trunc(units.as_bytes(), &iv);
    check_interval("interval_trunc", cerr, (ct, cd, cm), &r);

    // fc plane
    let uv = text_varlena(units.as_bytes());
    let ii = interval_arg_img(&iv);
    let fc = fc_call(
        tsb::fc_interval_trunc,
        [Datum::from_usize(uv.as_ptr() as usize), Datum::from_usize(ii.as_ptr() as usize)],
    );
    fc_check_interval("interval_trunc", &r, fc);
}

/// Shared part/extract compare tail for PartValue results.
#[allow(clippy::too_many_arguments)]
fn check_part(
    arm: &str,
    cerr: i32,
    cfval: f64,
    cisnull: i32,
    cnval: i64,
    cnlog10: i32,
    cnumset: i32,
    cnumchain: i32,
    retnumeric: bool,
    r: &types_error::PgResult<PartValue>,
) {
    match r {
        Ok(v) => {
            assert!(cerr == 0, "{arm} verdict: C err={cerr} vs Rust Ok({v:?})");
            if cnumchain != 0 {
                /* numeric-chain carve: verdict-only (documented in header) */
                return;
            }
            match v {
                PartValue::Null => {
                    assert!(cisnull == 1, "{arm} NULL plane: C isnull={cisnull} vs Rust Null")
                }
                PartValue::Float(rv) => {
                    assert!(!retnumeric && cisnull == 0);
                    assert!(
                        rv.to_bits() == cfval.to_bits(),
                        "{arm} DIVERGENCE: C {cfval:?}({:x}) vs Rust {rv:?}({:x})",
                        cfval.to_bits(),
                        rv.to_bits()
                    );
                }
                PartValue::Numeric(img) => {
                    assert!(retnumeric && cisnull == 0);
                    assert!(cnumset == 1, "{arm}: C recorded no numeric constructor");
                    let expect = expected_numeric_text(cnval, cnlog10);
                    let got = numeric_image_text(img);
                    assert!(
                        expect == got,
                        "{arm} NUMERIC DIVERGENCE: C-determined {expect:?} vs Rust {got:?}"
                    );
                }
            }
        }
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc && cerr != 0,
                "{arm} DIVERGENCE: C err={cerr} vs Rust Err(class={rc}, sqlstate={:?})",
                e.sqlstate
            );
        }
    }
}

fn ts_part_arm(payload: &[u8]) {
    let Some((&fb, rest)) = payload.split_first() else { return };
    if rest.len() < 8 {
        return;
    }
    let tz = (fb & 1) as i32;
    let retnumeric = fb & 2 != 0;
    let ts = rd_i64(rest, 0);
    let Some((units, cu)) = units_payload(&rest[8..]) else { return };

    let (mut cfval, mut cisnull, mut cnval, mut cnlog10, mut cnumset, mut cnumchain) =
        (0f64, 0i32, 0i64, 0i32, 0i32, 0i32);
    // SAFETY: units buffer + out pointers valid for the call.
    let cerr = unsafe {
        pg_tsdiff_ts_part(
            cu.as_ptr(),
            units.len() as i32,
            ts,
            tz,
            retnumeric as i32,
            &mut cfval,
            &mut cisnull,
            &mut cnval,
            &mut cnlog10,
            &mut cnumset,
            &mut cnumchain,
        )
    };
    let r = if tz == 1 {
        adt_timestamp::timestamptz_part_common(units.as_bytes(), ts, retnumeric)
    } else {
        adt_timestamp::timestamp_part_common(units.as_bytes(), ts, retnumeric)
    };
    check_part("ts_part", cerr, cfval, cisnull, cnval, cnlog10, cnumset, cnumchain, retnumeric, &r);

    // fc plane
    let uv = text_varlena(units.as_bytes());
    let f: PGFunction = match (tz, retnumeric) {
        (0, false) => tsb::fc_timestamp_part,
        (0, true) => tsb::fc_extract_timestamp,
        (_, false) => tsb::fc_timestamptz_part,
        _ => tsb::fc_extract_timestamptz,
    };
    let fc = fc_call(f, [Datum::from_usize(uv.as_ptr() as usize), Datum::from_i64(ts)]);
    fc_check_part("ts_part", &r, fc);
}

fn interval_part_arm(payload: &[u8]) {
    let Some((&fb, rest)) = payload.split_first() else { return };
    if rest.len() < 16 {
        return;
    }
    let retnumeric = fb & 2 != 0;
    let iv = Interval { time: rd_i64(rest, 0), day: rd_i32(rest, 8), month: rd_i32(rest, 12) };
    let Some((units, cu)) = units_payload(&rest[16..]) else { return };

    let (mut cfval, mut cisnull, mut cnval, mut cnlog10, mut cnumset, mut cnumchain) =
        (0f64, 0i32, 0i64, 0i32, 0i32, 0i32);
    // SAFETY: units buffer + out pointers valid for the call.
    let cerr = unsafe {
        pg_tsdiff_interval_part(
            cu.as_ptr(),
            units.len() as i32,
            iv.time,
            iv.day,
            iv.month,
            retnumeric as i32,
            &mut cfval,
            &mut cisnull,
            &mut cnval,
            &mut cnlog10,
            &mut cnumset,
            &mut cnumchain,
        )
    };
    let r = tsiv::interval_part_common(units.as_bytes(), &iv, retnumeric);
    check_part(
        "interval_part",
        cerr,
        cfval,
        cisnull,
        cnval,
        cnlog10,
        cnumset,
        cnumchain,
        retnumeric,
        &r,
    );

    // fc plane
    let uv = text_varlena(units.as_bytes());
    let ii = interval_arg_img(&iv);
    let f: PGFunction =
        if retnumeric { tsb::fc_extract_interval } else { tsb::fc_interval_part };
    let fc = fc_call(
        f,
        [Datum::from_usize(uv.as_ptr() as usize), Datum::from_usize(ii.as_ptr() as usize)],
    );
    fc_check_part("interval_part", &r, fc);
}

fn ts_age_arm(payload: &[u8]) {
    if payload.len() < 17 {
        return;
    }
    let tz = (payload[0] & 1) as i32;
    let a = rd_i64(payload, 1);
    let b = rd_i64(payload, 9);

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: out pointers valid for the call.
    let cerr = unsafe { pg_tsdiff_timestamp_age(a, b, tz, &mut ct, &mut cd, &mut cm) };
    let r = if tz == 1 { tsiv::timestamptz_age(a, b) } else { tsiv::timestamp_age(a, b) };
    check_interval("timestamp_age", cerr, (ct, cd, cm), &r);

    // fc plane
    let f: PGFunction = if tz == 1 { tsb::fc_timestamptz_age } else { tsb::fc_timestamp_age };
    let fc = fc_call(f, [Datum::from_i64(a), Datum::from_i64(b)]);
    fc_check_interval("timestamp_age", &r, fc);
}

fn make_ts_arm(payload: &[u8]) {
    if payload.len() < 29 {
        return;
    }
    let tz = (payload[0] & 1) as i32;
    let y = rd_i32(payload, 1);
    let mo = rd_i32(payload, 5);
    let d = rd_i32(payload, 9);
    let h = rd_i32(payload, 13);
    let mi = rd_i32(payload, 17);
    let sec = rd_f64(payload, 21);

    let mut cval = 0i64;
    // SAFETY: out pointer valid for the call.
    let cerr = unsafe { pg_tsdiff_make_timestamp(y, mo, d, h, mi, sec, tz, &mut cval) };
    let r = if tz == 1 {
        adt_timestamp::make_timestamptz(y, mo, d, h, mi, sec)
    } else {
        adt_timestamp::make_timestamp(y, mo, d, h, mi, sec)
    };
    check_i64("make_timestamp", cerr, cval, &r);

    // fc plane
    let f: PGFunction = if tz == 1 { tsb::fc_make_timestamptz } else { tsb::fc_make_timestamp };
    let fc = fc_call(
        f,
        [
            Datum::from_i32(y),
            Datum::from_i32(mo),
            Datum::from_i32(d),
            Datum::from_i32(h),
            Datum::from_i32(mi),
            Datum::from_f64(sec),
        ],
    );
    fc_check_i64("make_timestamp", &r, fc);
}

fn make_tstz_at_zone_arm(payload: &[u8]) {
    if payload.len() < 29 {
        return;
    }
    let y = rd_i32(payload, 0);
    let mo = rd_i32(payload, 4);
    let d = rd_i32(payload, 8);
    let h = rd_i32(payload, 12);
    let mi = rd_i32(payload, 16);
    let sec = rd_f64(payload, 20);
    let Some((zone, cz)) = zone_payload(&payload[28..]) else { return };
    let Some(cz) = cz else {
        if admit_tz_carved_exec() {
            let _ =
                adt_timestamp::make_timestamptz_at_timezone(y, mo, d, h, mi, sec, zone.as_bytes());
        }
        return;
    };

    let mut cval = 0i64;
    // SAFETY: zone buffer + out pointer valid for the call.
    let cerr = unsafe {
        pg_tsdiff_make_timestamptz_at_timezone(
            y,
            mo,
            d,
            h,
            mi,
            sec,
            cz.as_ptr(),
            zone.len() as i32,
            &mut cval,
        )
    };
    if !tz_in_domain() {
        if admit_tz_carved_exec() {
            let _ =
                adt_timestamp::make_timestamptz_at_timezone(y, mo, d, h, mi, sec, zone.as_bytes());
        }
        return;
    }
    let r = adt_timestamp::make_timestamptz_at_timezone(y, mo, d, h, mi, sec, zone.as_bytes());
    check_i64("make_timestamptz_at_timezone", cerr, cval, &r);

    // fc plane
    let zv = text_varlena(zone.as_bytes());
    let fc = fc_call(
        tsb::fc_make_timestamptz_at_timezone,
        [
            Datum::from_i32(y),
            Datum::from_i32(mo),
            Datum::from_i32(d),
            Datum::from_i32(h),
            Datum::from_i32(mi),
            Datum::from_f64(sec),
            Datum::from_usize(zv.as_ptr() as usize),
        ],
    );
    fc_check_i64("make_timestamptz_at_timezone", &r, fc);
}

fn make_interval_arm(payload: &[u8]) {
    if payload.len() < 32 {
        return;
    }
    let y = rd_i32(payload, 0);
    let mo = rd_i32(payload, 4);
    let w = rd_i32(payload, 8);
    let d = rd_i32(payload, 12);
    let h = rd_i32(payload, 16);
    let mi = rd_i32(payload, 20);
    let sec = rd_f64(payload, 24);

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: out pointers valid for the call.
    let cerr =
        unsafe { pg_tsdiff_make_interval(y, mo, w, d, h, mi, sec, &mut ct, &mut cd, &mut cm) };
    let r = tsiv::make_interval(y, mo, w, d, h, mi, sec);
    check_interval("make_interval", cerr, (ct, cd, cm), &r);

    // fc plane
    let fc = fc_call(
        tsb::fc_make_interval,
        [
            Datum::from_i32(y),
            Datum::from_i32(mo),
            Datum::from_i32(w),
            Datum::from_i32(d),
            Datum::from_i32(h),
            Datum::from_i32(mi),
            Datum::from_f64(sec),
        ],
    );
    fc_check_interval("make_interval", &r, fc);
}

fn interval_muldiv_arm(payload: &[u8]) {
    if payload.len() < 25 {
        return;
    }
    let isdiv = (payload[0] & 1) as i32;
    let iv =
        Interval { time: rd_i64(payload, 1), day: rd_i32(payload, 9), month: rd_i32(payload, 13) };
    let factor = rd_f64(payload, 17);

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: out pointers valid for the call.
    let cerr = unsafe {
        pg_tsdiff_interval_muldiv(
            isdiv, iv.time, iv.day, iv.month, factor, &mut ct, &mut cd, &mut cm,
        )
    };
    let r =
        if isdiv == 1 { tsiv::interval_div(&iv, factor) } else { tsiv::interval_mul(&iv, factor) };
    check_interval("interval_muldiv", cerr, (ct, cd, cm), &r);
}

fn timestamp_mi_arm(payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let a = rd_i64(payload, 0);
    let b = rd_i64(payload, 8);

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: out pointers valid for the call.
    let cerr = unsafe { pg_tsdiff_timestamp_mi(a, b, &mut ct, &mut cd, &mut cm) };
    let r = tsiv::timestamp_mi(a, b);
    check_interval("timestamp_mi", cerr, (ct, cd, cm), &r);

    // fc plane — the wrapper rides adt_date's builtins table; its lines
    // were recorded there as owed to this lane (claims note).
    let fc =
        fc_call(adt_date::builtins::fc_timestamp_mi, [Datum::from_i64(a), Datum::from_i64(b)]);
    match (&r, &fc.0) {
        (Ok(cv), Ok(fv)) => {
            let fv = datum_interval(*fv);
            assert!(
                (cv.time, cv.day, cv.month) == (fv.time, fv.day, fv.month),
                "timestamp_mi FC-PLANE value mismatch"
            );
        }
        (Err(ce), Err(fe)) => assert!(ce.sqlstate == fe.sqlstate, "timestamp_mi FC-PLANE sqlstate"),
        _ => panic!("timestamp_mi FC-PLANE verdict mismatch"),
    }

    // TimestampDifference family (pure arithmetic over (a, b); measured
    // here instead of the retired excluded-state carve — exceptions-ledger
    // flagfix 2026-07-31). msec/threshold ride optional tail bytes so the
    // existing 16-byte corpus keeps driving every entry.
    let msec = if payload.len() >= 20 { rd_i32(payload, 16) } else { b as i32 };
    // Domain fence: TimestampDifference/TimestampDifferenceExceeds subtract
    // raw; the C twins wrap (-fwrapv) where the shipped Rust (and this
    // build's overflow checks) would not. Backend callers only pass sane
    // clock pairs; compare on the non-overflowing domain.
    if b.checked_sub(a).is_some() {
        let (mut cs, mut cus) = (0i64, 0i32);
        // SAFETY: out pointers valid for the call.
        unsafe { pg_tsdiff_timestamp_difference(a, b, &mut cs, &mut cus) };
        let (rs, rus) = adt_timestamp::TimestampDifference(a, b);
        assert!(
            (cs, cus) == (rs, rus),
            "TimestampDifference DIVERGENCE: C({cs},{cus}) vs Rust({rs},{rus})"
        );

        // SAFETY: pure C call.
        let ce = unsafe { pg_tsdiff_timestamp_difference_exceeds(a, b, msec) } != 0;
        let re = adt_timestamp::TimestampDifferenceExceeds(a, b, msec);
        assert!(ce == re, "TimestampDifferenceExceeds DIVERGENCE: C {ce} vs Rust {re}");

        // SAFETY: pure C call.
        let cx = unsafe { pg_tsdiff_timestamp_difference_exceeds_secs(a, b, msec) } != 0;
        let rx = adt_timestamp::TimestampDifferenceExceedsSeconds(a, b, msec);
        assert!(cx == rx, "TimestampDifferenceExceedsSeconds DIVERGENCE: C {cx} vs Rust {rx}");
    }
    // Full-domain: TimestampDifferenceMilliseconds detects its own overflow
    // on both sides (pg_sub_s64_overflow / checked_sub).
    // SAFETY: pure C call.
    let cms = unsafe { pg_tsdiff_timestamp_difference_ms(a, b) };
    let rms = adt_timestamp::TimestampDifferenceMilliseconds(a, b);
    assert!(cms == rms, "TimestampDifferenceMilliseconds DIVERGENCE: C {cms} vs Rust {rms}");
}

fn ts_plmi_interval_arm(payload: &[u8]) {
    if payload.len() < 25 {
        return;
    }
    let tz = (payload[0] & 1) as i32;
    let ismi = ((payload[0] >> 1) & 1) as i32;
    let ts = rd_i64(payload, 1);
    let iv =
        Interval { time: rd_i64(payload, 9), day: rd_i32(payload, 17), month: rd_i32(payload, 21) };

    let mut cval = 0i64;
    // SAFETY: out pointer valid for the call.
    let cerr = unsafe {
        pg_tsdiff_timestamp_plmi_interval(tz, ismi, ts, iv.time, iv.day, iv.month, &mut cval)
    };
    let r = match (tz, ismi) {
        (0, 0) => tsiv::timestamp_pl_interval(ts, &iv),
        (0, _) => tsiv::timestamp_mi_interval(ts, &iv),
        (_, 0) => tsiv::timestamptz_pl_interval(ts, &iv),
        _ => tsiv::timestamptz_mi_interval(ts, &iv),
    };
    check_i64("ts_plmi_interval", cerr, cval, &r);

    // fc plane (tz variants; the non-tz wrappers ride adt_date's builtins).
    if tz == 1 {
        let ii = interval_arg_img(&iv);
        let f: PGFunction =
            if ismi == 1 { tsb::fc_timestamptz_mi_interval } else { tsb::fc_timestamptz_pl_interval };
        let fc = fc_call(f, [Datum::from_i64(ts), Datum::from_usize(ii.as_ptr() as usize)]);
        fc_check_i64("ts_plmi_interval", &r, fc);
        // 3-arg at-zone form pinned to the session zone: same value by
        // construction (proved 6222/6273 planes); drives the wrapper lines.
        let zv = text_varlena(b"GMT");
        let f: PGFunction = if ismi == 1 {
            tsb::fc_timestamptz_mi_interval_at_zone
        } else {
            tsb::fc_timestamptz_pl_interval_at_zone
        };
        let fc = fc_call(
            f,
            [
                Datum::from_i64(ts),
                Datum::from_usize(ii.as_ptr() as usize),
                Datum::from_usize(zv.as_ptr() as usize),
            ],
        );
        fc_check_i64("ts_plmi_interval_at_zone", &r, fc);
    }
}

fn justify_arm(payload: &[u8]) {
    if payload.len() < 17 {
        return;
    }
    let which = (payload[0] % 3) as i32;
    let iv =
        Interval { time: rd_i64(payload, 1), day: rd_i32(payload, 9), month: rd_i32(payload, 13) };

    let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
    // SAFETY: out pointers valid for the call.
    let cerr =
        unsafe { pg_tsdiff_justify(which, iv.time, iv.day, iv.month, &mut ct, &mut cd, &mut cm) };
    let r = match which {
        0 => tsiv::interval_justify_interval(&iv),
        1 => tsiv::interval_justify_hours(&iv),
        _ => tsiv::interval_justify_days(&iv),
    };
    check_interval("justify", cerr, (ct, cd, cm), &r);
}

fn ts_bin_arm(payload: &[u8]) {
    if payload.len() < 33 {
        return;
    }
    let tz = (payload[0] & 1) as i32;
    let stride =
        Interval { time: rd_i64(payload, 1), day: rd_i32(payload, 9), month: rd_i32(payload, 13) };
    let ts = rd_i64(payload, 17);
    let origin = rd_i64(payload, 25);

    let mut cval = 0i64;
    // SAFETY: out pointer valid for the call.
    let cerr = unsafe {
        pg_tsdiff_timestamp_bin(tz, stride.time, stride.day, stride.month, ts, origin, &mut cval)
    };
    // Rust ships one body for both SQL entries (6177/6178).
    let r = tsiv::timestamp_bin(&stride, ts, origin);
    check_i64("timestamp_bin", cerr, cval, &r);
}

fn interval_unops_arm(payload: &[u8]) {
    if payload.len() < 33 {
        return;
    }
    let op = payload[0] % 6;
    let a =
        Interval { time: rd_i64(payload, 1), day: rd_i32(payload, 9), month: rd_i32(payload, 13) };
    let b = Interval {
        time: rd_i64(payload, 17),
        day: rd_i32(payload, 25),
        month: rd_i32(payload, 29),
    };

    match op {
        0 => {
            let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
            // SAFETY: out pointers valid for the call.
            let cerr =
                unsafe { pg_tsdiff_interval_um(a.time, a.day, a.month, &mut ct, &mut cd, &mut cm) };
            let r = tsiv::interval_um(&a);
            check_interval("interval_um", cerr, (ct, cd, cm), &r);
        }
        1 => {
            let ismi = (payload[0] >> 2 & 1) as i32;
            let (mut ct, mut cd, mut cm) = (0i64, 0i32, 0i32);
            // SAFETY: out pointers valid for the call.
            let cerr = unsafe {
                pg_tsdiff_interval_plmi(
                    ismi, a.time, a.day, a.month, b.time, b.day, b.month, &mut ct, &mut cd, &mut cm,
                )
            };
            let r = if ismi == 1 { tsiv::interval_mi(&a, &b) } else { tsiv::interval_pl(&a, &b) };
            check_interval("interval_plmi", cerr, (ct, cd, cm), &r);
        }
        2 => {
            let larger = (payload[0] >> 2 & 1) as i32;
            let (mut ct, mut cd, mut cm, mut ccmp) = (0i64, 0i32, 0i32, 0i32);
            // SAFETY: out pointers valid for the call.
            let cerr = unsafe {
                pg_tsdiff_interval_minmax(
                    larger, a.time, a.day, a.month, b.time, b.day, b.month, &mut ct, &mut cd,
                    &mut cm, &mut ccmp,
                )
            };
            assert!(cerr == 0);
            let r =
                if larger == 1 { tsiv::interval_larger(a, b) } else { tsiv::interval_smaller(a, b) };
            assert!(
                (ct, cd, cm) == (r.time, r.day, r.month),
                "interval_minmax DIVERGENCE: C ({ct},{cd},{cm}) vs Rust ({},{},{})",
                r.time,
                r.day,
                r.month
            );
            let rcmp = tsiv::interval_cmp_internal(&a, &b);
            assert!(
                ccmp.signum() == rcmp.signum(),
                "interval_cmp DIVERGENCE: C {ccmp} vs Rust {rcmp}"
            );
        }
        3 => {
            // izone: interval-typed zone displacement
            let tz = (payload[0] >> 2 & 1) as i32;
            let ts = rd_i64(payload, 17);
            let mut cval = 0i64;
            // SAFETY: out pointer valid for the call.
            let cerr =
                unsafe { pg_tsdiff_timestamp_izone(tz, a.time, a.day, a.month, ts, &mut cval) };
            let r = if tz == 1 {
                tsiv::timestamptz_izone(&a, ts)
            } else {
                tsiv::timestamp_izone(&a, ts)
            };
            check_i64("timestamp_izone", cerr, cval, &r);
        }
        4 => {
            // typmod fc wrappers vs the PROVED cores (2903-2908): the C
            // plane rides those proofs; here the wrapper's cstring[]-array
            // decode + out-image plumbing gets the wrapper==core check.
            let n = 1 + (payload[0] >> 2 & 1) as usize; /* 1 or 2 elems */
            let v0 = rd_i32(payload, 1) % 100_000;
            let v1 = rd_i32(payload, 9) % 100_000;
            let mut img = Vec::new();
            let mut payload_bytes = Vec::new();
            for v in [v0, v1].iter().take(n) {
                payload_bytes.extend_from_slice(v.to_string().as_bytes());
                payload_bytes.push(0);
            }
            img.extend_from_slice(&0u32.to_le_bytes()); /* varlena hdr patched below */
            img.extend_from_slice(&1i32.to_le_bytes()); /* ndim */
            img.extend_from_slice(&0i32.to_le_bytes()); /* no nulls */
            img.extend_from_slice(&(types_core::CSTRINGOID).to_le_bytes());
            img.extend_from_slice(&(n as i32).to_le_bytes());
            img.extend_from_slice(&1i32.to_le_bytes()); /* lbound */
            img.extend_from_slice(&payload_bytes);
            let hdr = ((img.len() as u32) << 2).to_le_bytes();
            img[..4].copy_from_slice(&hdr);
            let d = Datum::from_usize(img.as_ptr() as usize);

            // interval typmodin: wrapper vs core over the same ints
            let fc = fc_call(tsb::fc_intervaltypmodin, [d]);
            let core = if n == 1 {
                tsiv::intervaltypmodin(&[v0])
            } else {
                tsiv::intervaltypmodin(&[v0, v1])
            };
            match (&core, &fc.0) {
                (Ok(cv), Ok(fv)) => {
                    assert!(*cv == fv.as_i32(), "intervaltypmodin FC-PLANE value")
                }
                (Err(ce), Err(fe)) => {
                    assert!(ce.sqlstate == fe.sqlstate, "intervaltypmodin FC-PLANE sqlstate")
                }
                _ => panic!("intervaltypmodin FC-PLANE verdict mismatch"),
            }
            // timestamp[tz] typmodin: wrapper vs proved core check
            for (f, istz) in [
                (tsb::fc_timestamptypmodin as PGFunction, false),
                (tsb::fc_timestamptztypmodin as PGFunction, true),
            ] {
                let fc = fc_call(f, [d]);
                if n == 1 {
                    let core = adt_timestamp::anytimestamp_typmod_check(istz, v0);
                    match (&core, &fc.0) {
                        (Ok(cv), Ok(fv)) => {
                            assert!(*cv == fv.as_i32(), "ts typmodin FC-PLANE value")
                        }
                        (Err(ce), Err(fe)) => assert!(
                            ce.sqlstate == fe.sqlstate,
                            "ts typmodin FC-PLANE sqlstate"
                        ),
                        _ => panic!("ts typmodin FC-PLANE verdict mismatch"),
                    }
                } else {
                    assert!(fc.0.is_err(), "ts typmodin must reject n!=1");
                }
            }
            // typmodout family (proved 2904/2906/2908 whole-image): the
            // wrapper plumbing runs; intervaltypmodout gets core compare.
            let tmod = interval_typmod(payload[0], payload[13]);
            let fc = fc_call(tsb::fc_intervaltypmodout, [Datum::from_i32(tmod)]);
            let mut buf64 = [0u8; 64];
            match (tsiv::intervaltypmodout(tmod, &mut buf64), &fc.0) {
                (Ok(len), Ok(fv)) => assert!(
                    datum_cstr_bytes(*fv) == &buf64[..len],
                    "intervaltypmodout FC-PLANE image"
                ),
                (Err(ce), Err(fe)) => {
                    assert!(ce.sqlstate == fe.sqlstate, "intervaltypmodout FC-PLANE sqlstate")
                }
                _ => panic!("intervaltypmodout FC-PLANE verdict mismatch"),
            }
            let tsmod = ts_typmod(payload[13]);
            let _ = fc_call(tsb::fc_timestamptypmodout, [Datum::from_i32(tsmod)]);
            let _ = fc_call(tsb::fc_timestamptztypmodout, [Datum::from_i32(tsmod)]);
        }
        _ => {
            // float8_timestamptz wrapper vs the proved core (1158); covers
            // the %g out-of-range message formatter (fmt_g6) error arm.
            let secs = rd_f64(payload, 1);
            let core = adt_timestamp::float8_timestamptz(secs);
            let fc = fc_call(tsb::fc_float8_timestamptz, [Datum::from_f64(secs)]);
            fc_check_i64("float8_timestamptz", &core, fc);
        }
    }
}

fn interval_agg_arm(payload: &[u8]) {
    if payload.len() < 57 {
        return;
    }
    let op = payload[0] % 5;
    // state: N, sumX(time,day,month), pinf, ninf — counts folded small so
    // the accumulate paths stay mostly finite-meaningful.
    let n = rd_i64(payload, 1).rem_euclid(1 << 20);
    let st = rd_i64(payload, 9);
    let sd = rd_i32(payload, 17);
    let sm = rd_i32(payload, 21);
    let pinf = rd_i64(payload, 25).rem_euclid(4);
    let ninf = rd_i64(payload, 33).rem_euclid(4);
    let nv = Interval {
        time: rd_i64(payload, 41),
        day: rd_i32(payload, 49),
        month: rd_i32(payload, 53),
    };

    let state0 = tsiv::IntervalAggState {
        N: n,
        pInfcount: pinf,
        nInfcount: ninf,
        sumX: Interval { time: st, day: sd, month: sm },
    };

    match op {
        0 | 1 => {
            // discard demands a matching prior accum (C Asserts on
            // impossible discards — out of SQL-reachable domain).
            if op == 1 {
                let can_discard = if nv == Interval::NOBEGIN {
                    ninf > 0
                } else if nv == Interval::NOEND {
                    pinf > 0
                } else {
                    n > 0
                };
                if !can_discard {
                    return;
                }
            }
            let (mut cn, mut cst, mut csd, mut csm, mut cpinf, mut cninf) =
                (n, st, sd, sm, pinf, ninf);
            // SAFETY: in/out pointers valid for the call.
            let cerr = unsafe {
                pg_tsdiff_interval_agg(
                    op as i32, &mut cn, &mut cst, &mut csd, &mut csm, &mut cpinf, &mut cninf,
                    nv.time, nv.day, nv.month,
                )
            };
            let mut state = state0;
            let r = if op == 0 {
                tsiv::do_interval_accum(&mut state, &nv)
            } else {
                tsiv::do_interval_discard(&mut state, &nv)
            };
            match r {
                Ok(()) => {
                    assert!(cerr == 0, "interval_agg verdict: C err={cerr} vs Rust Ok");
                    assert!(
                        (cn, cst, csd, csm, cpinf, cninf)
                            == (
                                state.N,
                                state.sumX.time,
                                state.sumX.day,
                                state.sumX.month,
                                state.pInfcount,
                                state.nInfcount
                            ),
                        "interval_agg state DIVERGENCE"
                    );
                }
                Err(e) => {
                    let rc = rust_err_class(&e);
                    assert!(cerr == rc && cerr != 0, "interval_agg err: C {cerr} vs Rust {rc}");
                }
            }

            // fc plane: the transfn wrappers under a real agg frame,
            // starting from a copy of the same state (in-place contract).
            let mut agg =
                types_fmgr::AggStateNode::new(mcx::MemoryContext::new_bump("tsdiff-aggacc"));
            let mut st_copy = state0;
            let ii = interval_arg_img(&nv);
            let mut fci = LocalFcinfo::<2>::new(0);
            fci.context = agg.fm_node_ptr();
            fci.args[0] =
                NullableDatum::value(Datum::from_usize(&mut st_copy as *mut _ as usize));
            fci.args[1] = NullableDatum::value(Datum::from_usize(ii.as_ptr() as usize));
            let f: PGFunction =
                if op == 0 { tsb::fc_interval_avg_accum } else { tsb::fc_interval_avg_accum_inv };
            let fr = f(None, &mut fci);
            let mut state2 = state0;
            let rr = if op == 0 {
                tsiv::do_interval_accum(&mut state2, &nv)
            } else {
                tsiv::do_interval_discard(&mut state2, &nv)
            };
            match (&rr, &fr) {
                (Ok(()), Ok(_)) => assert!(
                    (st_copy.N, st_copy.sumX.time, st_copy.pInfcount, st_copy.nInfcount)
                        == (state2.N, state2.sumX.time, state2.pInfcount, state2.nInfcount),
                    "interval_agg FC-PLANE state mismatch"
                ),
                (Err(ce), Err(fe)) => {
                    assert!(ce.sqlstate == fe.sqlstate, "interval_agg FC-PLANE sqlstate")
                }
                _ => panic!("interval_agg FC-PLANE verdict mismatch"),
            }
            agg.reset();
        }
        2 => {
            let issum = (payload[0] >> 3 & 1) as i32;
            let (mut ct, mut cd, mut cm, mut cisnull) = (0i64, 0i32, 0i32, 0i32);
            // SAFETY: out pointers valid for the call.
            let cerr = unsafe {
                pg_tsdiff_interval_avg_final(
                    issum, n, st, sd, sm, pinf, ninf, &mut ct, &mut cd, &mut cm, &mut cisnull,
                )
            };
            let r = if issum == 1 {
                tsiv::interval_sum_final(&state0)
            } else {
                tsiv::interval_avg_final(&state0)
            };
            match &r {
                Ok(None) => assert!(cerr == 0 && cisnull == 1, "agg_final NULL plane"),
                Ok(Some(v)) => assert!(
                    cerr == 0 && cisnull == 0 && (ct, cd, cm) == (v.time, v.day, v.month),
                    "agg_final DIVERGENCE: C(err={cerr},null={cisnull},({ct},{cd},{cm}))"
                ),
                Err(e) => {
                    let rc = rust_err_class(e);
                    assert!(cerr == rc && cerr != 0, "agg_final err: C {cerr} vs Rust {rc}");
                }
            }

            // fc plane: finals over a pointer state
            let f: PGFunction = if issum == 1 { tsb::fc_interval_sum } else { tsb::fc_interval_avg };
            let fc = fc_call(f, [Datum::from_usize(&state0 as *const _ as usize)]);
            match (&r, &fc.0) {
                (Ok(None), Ok(_)) => assert!(fc.1, "agg_final FC-PLANE null mismatch"),
                (Ok(Some(v)), Ok(fv)) => {
                    let fv = datum_interval(*fv);
                    assert!(
                        (v.time, v.day, v.month) == (fv.time, fv.day, fv.month),
                        "agg_final FC-PLANE value mismatch"
                    );
                }
                (Err(ce), Err(fe)) => {
                    assert!(ce.sqlstate == fe.sqlstate, "agg_final FC-PLANE sqlstate")
                }
                _ => panic!("agg_final FC-PLANE verdict mismatch"),
            }
        }
        3 => {
            let n2 = rd_i64(payload, 41).rem_euclid(1 << 20);
            let st2 = rd_i64(payload, 49);
            let (mut cn, mut cst, mut csd, mut csm, mut cpinf, mut cninf) =
                (0i64, 0i64, 0i32, 0i32, 0i64, 0i64);
            // SAFETY: out pointers valid for the call.
            let cerr = unsafe {
                pg_tsdiff_interval_avg_combine(
                    n, st, sd, sm, pinf, ninf, n2, st2, sd, sm, pinf, ninf, &mut cn, &mut cst,
                    &mut csd, &mut csm, &mut cpinf, &mut cninf,
                )
            };
            let mut s1 = state0;
            let s2 = tsiv::IntervalAggState {
                N: n2,
                pInfcount: pinf,
                nInfcount: ninf,
                sumX: Interval { time: st2, day: sd, month: sm },
            };
            let rcomb = tsiv::interval_agg_combine(&mut s1, &s2);
            match &rcomb {
                Ok(()) => {
                    assert!(cerr == 0, "agg_combine verdict: C err={cerr} vs Rust Ok");
                    assert!(
                        (cn, cst, csd, csm, cpinf, cninf)
                            == (
                                s1.N,
                                s1.sumX.time,
                                s1.sumX.day,
                                s1.sumX.month,
                                s1.pInfcount,
                                s1.nInfcount
                            ),
                        "agg_combine state DIVERGENCE"
                    );
                }
                Err(e) => {
                    let rc = rust_err_class(e);
                    assert!(cerr == rc && cerr != 0, "agg_combine err: C {cerr} vs Rust {rc}");
                }
            }

            // fc plane under a real agg frame (state1 mutated in place)
            let mut agg =
                types_fmgr::AggStateNode::new(mcx::MemoryContext::new_bump("tsdiff-aggcmb"));
            let mut fs1 = state0;
            let fs2 = tsiv::IntervalAggState {
                N: n2,
                pInfcount: pinf,
                nInfcount: ninf,
                sumX: Interval { time: st2, day: sd, month: sm },
            };
            let mut fci = LocalFcinfo::<2>::new(0);
            fci.context = agg.fm_node_ptr();
            fci.args[0] = NullableDatum::value(Datum::from_usize(&mut fs1 as *mut _ as usize));
            fci.args[1] = NullableDatum::value(Datum::from_usize(&fs2 as *const _ as usize));
            let fr = tsb::fc_interval_avg_combine(None, &mut fci);
            match (&rcomb, &fr) {
                (Ok(()), Ok(_)) => assert!(
                    (fs1.N, fs1.sumX.time, fs1.pInfcount, fs1.nInfcount)
                        == (s1.N, s1.sumX.time, s1.pInfcount, s1.nInfcount),
                    "agg_combine FC-PLANE state mismatch"
                ),
                (Err(ce), Err(fe)) => {
                    assert!(ce.sqlstate == fe.sqlstate, "agg_combine FC-PLANE sqlstate")
                }
                _ => panic!("agg_combine FC-PLANE verdict mismatch"),
            }
            agg.reset();
        }
        _ => {
            // serialize image + deserialize roundtrip (fc wrappers)
            let mut cbytes = [0u8; 64];
            let mut clen = 0i32;
            // SAFETY: out buffer covers the 40-byte image.
            let cerr = unsafe {
                pg_tsdiff_interval_avg_serialize(
                    n,
                    st,
                    sd,
                    sm,
                    pinf,
                    ninf,
                    cbytes.as_mut_ptr(),
                    &mut clen,
                )
            };
            assert!(cerr == 0);
            let cimg = &cbytes[..clen as usize];

            // Rust serialize via the fc wrapper (state passed by pointer).
            let cx = mcx::MemoryContext::new("timestamp_diff_aggser");
            let mut fcinfo = LocalFcinfo::<1>::new(0);
            // SAFETY: cx outlives the call.
            unsafe { fcinfo.set_result_mcx(cx.mcx()) };
            fcinfo.args[0] = NullableDatum::value(Datum::from_usize(&state0 as *const _ as usize));
            let d = tsb::fc_interval_avg_serialize(None, &mut fcinfo).unwrap();
            // SAFETY: wrapper returned a live 4B-header varlena in cx.
            let hdr = unsafe { std::slice::from_raw_parts(d.as_usize() as *const u8, 4) };
            let vlen = (u32::from_le_bytes(hdr.try_into().unwrap()) >> 2) as usize;
            // SAFETY: payload of vlen-4 bytes follows the header.
            let rimg =
                unsafe { std::slice::from_raw_parts((d.as_usize() + 4) as *const u8, vlen - 4) };
            assert!(
                cimg == rimg,
                "interval_avg_serialize IMAGE DIVERGENCE: C {cimg:02x?} vs Rust {rimg:02x?}"
            );

            // C deserialize roundtrip on its own image.
            let (mut dn, mut dst, mut dsd, mut dsm, mut dpinf, mut dninf) =
                (0i64, 0i64, 0i32, 0i32, 0i64, 0i64);
            // SAFETY: image + out pointers valid for the call.
            let derr = unsafe {
                pg_tsdiff_interval_avg_deserialize(
                    cimg.as_ptr(),
                    clen,
                    &mut dn,
                    &mut dst,
                    &mut dsd,
                    &mut dsm,
                    &mut dpinf,
                    &mut dninf,
                )
            };
            assert!(
                derr == 0 && (dn, dst, dsd, dsm, dpinf, dninf) == (n, st, sd, sm, pinf, ninf),
                "C serialize/deserialize roundtrip broke"
            );

            // Rust deserialize via the fc wrapper under an agg frame.
            let mut agg = types_fmgr::AggStateNode::new(mcx::MemoryContext::new_bump("tsdiff-aggctx"));
            let mut vbuf = Vec::with_capacity(4 + cimg.len());
            vbuf.extend_from_slice(&(((4 + cimg.len()) as u32) << 2).to_le_bytes());
            vbuf.extend_from_slice(cimg);
            let cx2 = mcx::MemoryContext::new("timestamp_diff_aggdeser");
            let mut fci = LocalFcinfo::<2>::new(0);
            // SAFETY: cx2 outlives the call.
            unsafe { fci.set_result_mcx(cx2.mcx()) };
            fci.context = agg.fm_node_ptr();
            fci.args[0] = NullableDatum::value(Datum::from_usize(vbuf.as_ptr() as usize));
            fci.args[1] = NullableDatum::value(Datum::from_i32(0));
            let rd = tsb::fc_interval_avg_deserialize(None, &mut fci).unwrap();
            // SAFETY: wrapper returned a live IntervalAggState allocation.
            let rs = unsafe { &*(rd.as_usize() as *const tsiv::IntervalAggState) };
            assert!(
                (rs.N, rs.sumX.time, rs.sumX.day, rs.sumX.month, rs.pInfcount, rs.nInfcount)
                    == (n, st, sd, sm, pinf, ninf),
                "Rust deserialize DIVERGENCE from C image"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Rails
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn arm(sel: u8, payload: &[u8]) {
        let mut v = vec![sel];
        v.extend_from_slice(payload);
        timestamp_diff(&v);
    }

    #[test]
    fn smoke_ts_in_iso() {
        let _serial = crate::c_oracle_serial();
        arm(0, b"\x01\x00\x002024-01-02 03:04:05.6");
        arm(0, b"\x01\x00\x012024-01-02 03:04:05.6+07");
        arm(0, b"\x00\x00\x00infinity");
        arm(0, b"\x00\x00\x00epoch");
    }

    #[test]
    fn smoke_ts_in_error() {
        let _serial = crate::c_oracle_serial();
        arm(0, b"\x01\x00\x00nope");
    }

    #[test]
    fn smoke_ts_in_tz_carve() {
        let _serial = crate::c_oracle_serial();
        arm(0, b"\x01\x00\x012024-01-02 03:04:05 America/New_York");
    }

    #[test]
    fn smoke_ts_out_styles() {
        let _serial = crate::c_oracle_serial();
        for style in 0..15u8 {
            let mut p = vec![style, 0];
            p.extend_from_slice(&789_012_345_678i64.to_le_bytes());
            arm(1, &p);
            let mut p = vec![style, 1];
            p.extend_from_slice(&(-210_866_803_200_000_000i64 + 12345).to_le_bytes());
            arm(1, &p);
        }
    }

    #[test]
    fn smoke_interval_io() {
        let _serial = crate::c_oracle_serial();
        arm(2, b"\x00\x80\x001 year 2 mons 3 days 04:05:06.789");
        arm(2, b"\x03\x80\x00P1Y2M3DT4H5M6S");
        for is in 0..4u8 {
            let mut p = vec![is];
            p.extend_from_slice(&123_456_789i64.to_le_bytes());
            p.extend_from_slice(&5i32.to_le_bytes());
            p.extend_from_slice(&14i32.to_le_bytes());
            arm(3, &p);
        }
    }

    #[test]
    fn smoke_wire() {
        let _serial = crate::c_oracle_serial();
        let mut p = vec![0, 0];
        p.extend_from_slice(&42i64.to_be_bytes());
        arm(4, &p);
        arm(5, &42i64.to_le_bytes());
        let mut p = vec![0, 0x80];
        p.extend_from_slice(&42i64.to_be_bytes());
        p.extend_from_slice(&1i32.to_be_bytes());
        p.extend_from_slice(&2i32.to_be_bytes());
        arm(6, &p);
        let mut p = 42i64.to_le_bytes().to_vec();
        p.extend_from_slice(&1i32.to_le_bytes());
        p.extend_from_slice(&2i32.to_le_bytes());
        arm(7, &p);
    }

    #[test]
    fn smoke_trunc_part() {
        let _serial = crate::c_oracle_serial();
        for (unit, ts) in [(&b"hour"[..], 1234567890123456i64), (b"week", -987654321i64)] {
            let mut p = vec![0];
            p.extend_from_slice(&ts.to_le_bytes());
            p.extend_from_slice(unit);
            arm(10, &p);
            let mut p = vec![2]; /* retnumeric extract */
            p.extend_from_slice(&ts.to_le_bytes());
            p.extend_from_slice(unit);
            arm(13, &p);
            let mut p = vec![0]; /* float part */
            p.extend_from_slice(&ts.to_le_bytes());
            p.extend_from_slice(unit);
            arm(13, &p);
        }
        // seconds extract: int64_div_fast_to_numeric value plane
        let mut p = vec![2];
        p.extend_from_slice(&1234567890123456i64.to_le_bytes());
        p.extend_from_slice(b"second");
        arm(13, &p);
        // epoch: numeric-chain carve path (verdict-only)
        let mut p = vec![2];
        p.extend_from_slice(&1234567890123456i64.to_le_bytes());
        p.extend_from_slice(b"epoch");
        arm(13, &p);
        // interval variants
        let mut p = vec![2];
        p.extend_from_slice(&98_765_432_101i64.to_le_bytes());
        p.extend_from_slice(&12i32.to_le_bytes());
        p.extend_from_slice(&25i32.to_le_bytes());
        p.extend_from_slice(b"second");
        arm(14, &p);
        // trunc_zone with numeric offset (in-domain) and named zone (carve)
        let mut p = 1234567890123456i64.to_le_bytes().to_vec();
        p.push(4);
        p.extend_from_slice(b"hour");
        p.extend_from_slice(b"+05:30");
        arm(11, &p);
        let mut p = 1234567890123456i64.to_le_bytes().to_vec();
        p.push(4);
        p.extend_from_slice(b"hour");
        p.extend_from_slice(b"Asia/Tokyo");
        arm(11, &p);
    }

    #[test]
    fn smoke_arith() {
        let _serial = crate::c_oracle_serial();
        let mut p = Vec::new();
        p.extend_from_slice(&100_000_000i64.to_le_bytes());
        p.extend_from_slice(&200_000_000i64.to_le_bytes());
        arm(20, &p);
        let mut p = vec![0];
        p.extend_from_slice(&100_000_000i64.to_le_bytes());
        p.extend_from_slice(&200_000_000i64.to_le_bytes());
        arm(15, &p);
        let mut p = vec![0];
        p.extend_from_slice(&1_000_000i64.to_le_bytes());
        p.extend_from_slice(&2_000_000i64.to_le_bytes());
        p.extend_from_slice(&3i32.to_le_bytes());
        p.extend_from_slice(&4i32.to_le_bytes());
        arm(21, &p);
        let mut p = vec![3]; /* tz + mi */
        p.extend_from_slice(&1_000_000i64.to_le_bytes());
        p.extend_from_slice(&2_000_000i64.to_le_bytes());
        p.extend_from_slice(&3i32.to_le_bytes());
        p.extend_from_slice(&4i32.to_le_bytes());
        arm(21, &p);
        let mut p = vec![0];
        p.extend_from_slice(&90_000_000_000i64.to_le_bytes());
        p.extend_from_slice(&35i32.to_le_bytes());
        p.extend_from_slice(&2i32.to_le_bytes());
        arm(22, &p);
        let mut p = vec![1];
        p.extend_from_slice(&90_000_000_000i64.to_le_bytes());
        p.extend_from_slice(&35i32.to_le_bytes());
        p.extend_from_slice(&2i32.to_le_bytes());
        p.extend_from_slice(&2.5f64.to_le_bytes());
        arm(19, &p);
        // bin
        let mut p = vec![0];
        p.extend_from_slice(&3_600_000_000i64.to_le_bytes());
        p.extend_from_slice(&0i32.to_le_bytes());
        p.extend_from_slice(&0i32.to_le_bytes());
        p.extend_from_slice(&1234567890123456i64.to_le_bytes());
        p.extend_from_slice(&0i64.to_le_bytes());
        arm(23, &p);
        // unops: um / plmi / minmax / izone
        for opb in [0u8, 1, 5, 2, 6, 3, 7] {
            let mut p = vec![opb];
            p.extend_from_slice(&90_000_000_000i64.to_le_bytes());
            p.extend_from_slice(&35i32.to_le_bytes());
            p.extend_from_slice(&2i32.to_le_bytes());
            p.extend_from_slice(&(-4_000_000i64).to_le_bytes());
            p.extend_from_slice(&1i32.to_le_bytes());
            p.extend_from_slice(&0i32.to_le_bytes());
            arm(24, &p);
        }
    }

    #[test]
    fn smoke_scale() {
        let _serial = crate::c_oracle_serial();
        for tm in 0..8u8 {
            let mut p = vec![tm];
            p.extend_from_slice(&1234567890123456i64.to_le_bytes());
            arm(8, &p);
            let mut p = vec![tm, 3];
            p.extend_from_slice(&98_765_432_101i64.to_le_bytes());
            p.extend_from_slice(&12i32.to_le_bytes());
            p.extend_from_slice(&25i32.to_le_bytes());
            arm(9, &p);
        }
    }

    #[test]
    fn smoke_make() {
        let _serial = crate::c_oracle_serial();
        let mut p = vec![0];
        for v in [2024i32, 2, 29, 12, 30] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p.extend_from_slice(&45.5f64.to_le_bytes());
        arm(16, &p);
        let mut p = vec![1];
        for v in [2024i32, 2, 29, 12, 30] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p.extend_from_slice(&45.5f64.to_le_bytes());
        arm(16, &p);
        // at timezone: numeric offset stays in-domain
        let mut p = Vec::new();
        for v in [2024i32, 2, 29, 12, 30] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p.extend_from_slice(&45.5f64.to_le_bytes());
        p.extend_from_slice(b"+02");
        arm(17, &p);
        // make_interval
        let mut p = Vec::new();
        for v in [1i32, 2, 3, 4, 5, 6] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p.extend_from_slice(&7.25f64.to_le_bytes());
        arm(18, &p);
    }

    #[test]
    fn smoke_agg() {
        let _serial = crate::c_oracle_serial();
        for op in 0..5u8 {
            let mut p = vec![op];
            p.extend_from_slice(&3i64.to_le_bytes());
            p.extend_from_slice(&1_000_000i64.to_le_bytes());
            p.extend_from_slice(&2i32.to_le_bytes());
            p.extend_from_slice(&1i32.to_le_bytes());
            p.extend_from_slice(&0i64.to_le_bytes());
            p.extend_from_slice(&0i64.to_le_bytes());
            p.extend_from_slice(&500_000i64.to_le_bytes());
            p.extend_from_slice(&1i32.to_le_bytes());
            p.extend_from_slice(&0i32.to_le_bytes());
            arm(25, &p);
        }
    }

    #[test]
    fn replay_committed_corpus() {
        // NO test-level c_oracle_serial here: the driver takes oracle_serial
        // at entry (each unit is its own critical section), and the
        // attribution probe below spawns a FRESH THREAD calling the driver —
        // under an outer guard held by this thread that probe deadlocks the
        // whole suite (fleet job ...61114 hung 1800s exactly this way).
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/timestamp_diff");
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        let mut n = 0usize;
        let mut failed: Vec<String> = Vec::new();
        for e in entries.flatten() {
            if let Ok(bytes) = std::fs::read(e.path()) {
                // Name the failing UNIT (fix/mutants-rail): the fleet rail
                // baseline reproduces a divergence here that no sorted-order
                // replay does — replay order is readdir order, so the finding
                // is (state-setting unit, victim unit) and the panic alone
                // names neither. Catch per unit, print file + bytes + ordinal,
                // keep the test red.
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    timestamp_diff(&bytes)
                }));
                if r.is_err() {
                    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                    eprintln!(
                        "REPLAY-FAIL unit={} ordinal={n} len={} hex={hex}",
                        e.path().display(),
                        bytes.len()
                    );
                    // Attribute the poisoned SIDE (fleet job ...59580 named
                    // this unit; it passes fresh/sorted, fails at pod-order
                    // ordinal 1034 — so some earlier exec poisons persistent
                    // state). Same-thread retry vs fresh-thread retry split
                    // the hypotheses: Rust session state is thread-local
                    // (fresh thread = fresh Rust state) while the C oracle's
                    // datetime caches are process-global statics (a fresh
                    // thread still sees them).
                    let same = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        timestamp_diff(&bytes)
                    }));
                    let b2 = bytes.clone();
                    let fresh = std::thread::spawn(move || {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            timestamp_diff(&b2)
                        }))
                        .is_err()
                    })
                    .join()
                    .unwrap_or(true);
                    eprintln!(
                        "REPLAY-FAIL-ATTRIB same_thread_retry_fails={} fresh_thread_fails={} (true/true=C process-global poison; true/false=Rust thread-local poison; false/*=transient)",
                        same.is_err(),
                        fresh
                    );
                    failed.push(format!("{} (ordinal {n})", e.path().display()));
                }
                n += 1;
            }
        }
        eprintln!("replayed {n} corpus inputs, {} failed", failed.len());
        assert!(failed.is_empty(), "corpus units diverged (order-dependent state suspected — see REPLAY-FAIL lines above): {failed:?}");
    }
}
