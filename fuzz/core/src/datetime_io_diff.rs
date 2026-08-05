//! datetime_io_diff: differential fuzz driver — shipped Rust `adt_date` +
//! `adt_datetime` parse/encode engine vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C (csrc/pg_datetime_io_io.c).
//! Crates under test: crates/backend/utils/adt/adt_date (entry points) and
//! crates/backend/utils/adt/adt_datetime (ParseDateTime / DecodeDateTime /
//! DecodeTimeOnly / Encode* engine, driven through them).
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits (date
//! i32 / time i64 / timetz (i64,i32) fields / *_out text-image bytes),
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! PINNED ENVIRONMENT (mirrors csrc/pg_datetime_io_io.c's header exactly —
//! environment, never computation):
//!   - DateStyle x DateOrder fuzzed from a style byte on BOTH sides
//!     (5 styles x 3 orders).
//!   - session timezone = GMT; tz database = {GMT} only (PGRUST_TZDIR is
//!     pointed at a nonexistent directory, so tzload fails for every named
//!     zone while pg_tzset's GMT special case still works — the C oracle's
//!     pg_tzset shim answers identically).
//!   - current date/time pinned to 2026-06-15 12:30:45.123456 GMT via
//!     xact_seams::get_current_transaction_start_timestamp (Rust) and the
//!     GetCurrentDateTime/GetCurrentTimeUsec shims (C), making
//!     "now"/"today"/"yesterday"/"tomorrow" and zone-less timetz input
//!     deterministic.
//!   - database encoding UTF-8 (Rust &str API; C oracle pins
//!     pg_database_encoding_max_length() == 4).
//!
//! Input layout: [selector][payload]; selector % 9 picks the arm:
//!   0 date_in    (oid 1084) — [style][text]
//!   1 date_out   (oid 1085) — [style][date i32 LE], folded into
//!     valid-or-special domain (PG's on-disk invariant; out-of-range date
//!     datums are unreachable through SQL and hit C UB in j2date)
//!   2 time_in    (oid 1143) — [style][typmod][text]
//!   3 time_out   (oid 1144) — [style][time i64 LE] folded into
//!     0..=USECS_PER_DAY (PG invariant)
//!   4 timetz_in  (oid 1350) — [style][typmod][text]
//!   5 timetz_out (oid 1351) — [style][time i64][zone i32] folded into the
//!     PG invariants (time in-range, |zone| < 16h)
//!   6 time_part  (oid 1385) — [time i64][units bytes] (units < 64 bytes =
//!     NAMEDATALEN, so identifier truncation never fires; float8 plane)
//!   7 make_time  (oid 3847) — [hour i32][min i32][sec f64 raw bits]
//!   8 make_date  (oid 3846) — [year i32][month i32][day i32]
//!
//! FC-WRAPPER PLANE: arms route their (already core-vs-C checked) input
//! through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and assert wrapper == core (Datum value /
//! returned bytes / error verdict + sqlstate).
//!   - time_part's fc wrapper (fc_time_part) needs a packed text varlena
//!     argument; the core plane fully covers time_part_common, and the
//!     4-line macro wrapper body is exercised by the crate's unit tests —
//!     recorded as a routes-row note, not silently skipped.
//!
//! SKIPPED (state-seam carves, per the phase-1 filter and the routes rows):
//! named-timezone resolution beyond GMT (tz database pinned; execs that
//! consult pg_tzset with a non-GMT name — tzdata or POSIX "UTC+10" forms —
//! are flagged by the oracle and their plane comparisons skipped, the Rust
//! side still executing for panic-safety), dynamic abbreviations
//! (zoneabbrevtbl never installed), and the retnumeric (extract_*) plane —
//! numeric result images belong to the extract_* rows.

use std::ffi::CString;
use std::sync::Once;

use datum::{Datum, NullableDatum};
use types_error::PgError;
use types_fmgr::{LocalFcinfo, PGFunction};

use adt_date::{DateADT, TimeADT, TimeTzADT};
use adt_datetime::MAXDATELEN;
use adt_timestamp::PartValue;
use adt_datetime::{
    set_date_order, set_date_style, DATEORDER_DMY, DATEORDER_MDY, DATEORDER_YMD,
    USE_GERMAN_DATES, USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES, USE_XSD_DATES,
};

extern "C" {
    fn pg_diff_date_in(str_: *const std::ffi::c_char, style: i32, order: i32, out: *mut i32) -> i32;
    fn pg_diff_date_out(date: i32, style: i32, order: i32, buf: *mut u8) -> i32;
    fn pg_diff_time_in(str_: *const std::ffi::c_char, typmod: i32, style: i32, order: i32, out: *mut i64)
        -> i32;
    fn pg_diff_time_out(time: i64, style: i32, order: i32, buf: *mut u8) -> i32;
    fn pg_diff_timetz_in(
        str_: *const std::ffi::c_char,
        typmod: i32,
        style: i32,
        order: i32,
        out_time: *mut i64,
        out_zone: *mut i32,
    ) -> i32;
    fn pg_diff_timetz_out(time: i64, zone: i32, style: i32, order: i32, buf: *mut u8) -> i32;
    fn pg_diff_time_part(units: *const u8, units_len: i32, time: i64, out: *mut f64) -> i32;
    fn pg_diff_make_time(hour: i32, min: i32, sec: f64, out: *mut i64) -> i32;
    fn pg_diff_make_date(year: i32, month: i32, day: i32, out: *mut i32) -> i32;
    /// Nonzero when the exec consulted pg_tzset with a non-GMT name: the
    /// input left the compared domain (tz-database carve) — skip all plane
    /// comparisons (see the C oracle header).
    fn pg_diff_datetime_tzset_nongmt() -> i32;
    /// The non-GMT zone name this exec asked for (NUL-terminated, empty when
    /// none) — keys the zone-name admission budget below.
    fn pg_diff_datetime_tzset_name() -> *const std::ffi::c_char;
}

/// Should the Rust engine run for a tz-carved exec?
///
/// Carved execs compare NOTHING (the oracle does not vendor tzparse/tzload),
/// so their only value is Rust-side panic-safety. But pgrust's `pg_tzset`
/// cache is process-lifetime BY DESIGN — exact parity with 18.3 pgtz.c, whose
/// `timezone_cache` HTAB is likewise never evicted — and each admitted name
/// leaks a ~21KB `PgTz` (`TzState` carries `ats[2000]`) for the life of the
/// process. An unbounded stream of fuzzer-invented POSIX zone names therefore
/// grows RSS without bound: a 7.5M-exec fleet campaign died at
/// `libFuzzer: out-of-memory (used: 2060Mb)` on exactly this.
///
/// So admit a bounded set of DISTINCT names: every name already admitted keeps
/// running forever (it can no longer grow the cache — it is already in it), and
/// genuinely-new names are admitted only while the budget lasts. Memory is
/// bounded at BUDGET entries; panic-safety coverage is retained for every
/// admitted name and for all non-carved execs (the entire compared domain).
fn admit_tz_carved_exec() -> bool {
    /// 2048 x ~21KB ~= 43MB steady-state ceiling.
    const BUDGET: usize = 2048;
    use std::cell::RefCell;
    use std::collections::HashSet;
    std::thread_local! {
        static SEEN: RefCell<HashSet<Vec<u8>>> = RefCell::new(HashSet::new());
    }
    // SAFETY: the oracle keeps this NUL-terminated static for the exec.
    let name = unsafe { std::ffi::CStr::from_ptr(pg_diff_datetime_tzset_name()) }.to_bytes();
    SEEN.with(|s| {
        let mut s = s.borrow_mut();
        if s.contains(name) {
            return true; /* already cached: running it cannot grow RSS */
        }
        if s.len() >= BUDGET {
            return false;
        }
        s.insert(name.to_vec());
        true
    })
}

/// Pinned "current" instant: 2026-06-15 12:30:45.123456 GMT as a PG
/// timestamp(tz) — 9662 days after 2000-01-01 (matches the C shim).
const PINNED_NOW_USECS: i64 = 9662 * 86_400_000_000 + 45_045_000_000 + 123_456;

const USECS_PER_DAY: i64 = 86_400_000_000;
const TZDISP_LIMIT: i32 = 16 * 3600; /* datetime.h: max zone displacement */

/// date.c valid DateADT domain: -2451545 (4714-11-24 BC) ..= 2932896
/// (5874897-12-31), plus the NOBEGIN/NOEND sentinels.
const DATE_MIN: i64 = -2_451_545;
const DATE_MAX: i64 = 2_932_896;

/// The pinned environment is shared with the sibling `datetime_convert_diff`
/// target (identical GMT / clock / tz-database pins, mirroring the same C
/// oracle); exposed so there is ONE definition of it rather than two that can
/// silently drift apart.
pub fn init_env_for_siblings() {
    init_env();
}

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // SAFETY: single-threaded libFuzzer init / first-test init, before
        // any getenv (adt_date tests.rs gmt_session precedent).
        unsafe {
            std::env::set_var("PGRUST_TZDIR", "/nonexistent-pgrust-tzdir-datetime-io-diff")
        };
        pgtz::init_seams();
        guc_tables::init_seams();
        // CROSS-FAMILY DOUBLE INSTALL (fixed 2026-08-03). elog::init_seams()
        // does an unconditional elog_seams::ereport::set(), and cryptbe_diff
        // (fuzz/core/src/cryptbe_diff.rs:162) plus contriba_diff (:194) also
        // call it. Whoever runs second panics "seam installed twice:
        // elog_seams::ereport" -- and because this call sits INSIDE the Once
        // below, that panic POISONS the Once, so every later datetime/timestamp
        // test dies with "Once instance has previously been poisoned". That is
        // the 30-test cascade whose appearance depends only on test ORDERING,
        // which is why the same tree read 450/2 on one whole-lib run and 419/33
        // on the next. Deterministic reproducer (0.46s):
        //     cargo test -p decoder_fuzz --lib -- cryptbe_diff datetime_closeout_diff
        //
        // Guard on the sentinel rather than swallowing the panic with
        // catch_unwind (what the two sibling families do): re-installing the
        // same implementation is a no-op, while a genuinely CONFLICTING install
        // should still be loud. ereport is the sentinel for the trio
        // init_seams() sets together.
        //
        // RESIDUAL, not fixed here: if a sibling's catch_unwind swallowed a
        // PARTIAL install (ereport set, ereport_msg not), this guard would skip
        // the rest. Pre-existing and equally true of the catch_unwind siblings;
        // the real cure is making elog::init_seams() idempotent per-seam, which
        // is product code and a separate change.
        if !::elog_seams::ereport::is_installed() {
            elog::init_seams();
        }
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        xact_seams::get_current_transaction_start_timestamp::set(|| PINNED_NOW_USECS);
        // Pinned-clock snapshot installed directly (NOT
        // adt_timestamp::init_seams, whose impls read the real xact
        // thread-state clock): 2026-06-15 12:30:45.123456 GMT, the same
        // constants as the C GetCurrentDateTime/GetCurrentTimeUsec shims.
        fn pinned_now() -> types_error::PgResult<timestamp_seams::CurrentTimeUsec> {
            let jd = adt_datetime::calendar::date2j(2026, 6, 15);
            Ok(timestamp_seams::CurrentTimeUsec {
                tm_sec: 45,
                tm_min: 30,
                tm_hour: 12,
                tm_mday: 15,
                tm_mon: 6,
                tm_year: 2026,
                tm_wday: adt_datetime::calendar::j2day(jd),
                tm_yday: jd - adt_datetime::calendar::date2j(2026, 1, 1),
                tm_isdst: 0,
                tm_gmtoff: 0,
                tm_zone: Some("GMT"),
                fsec: 123_456,
                tz: 0,
            })
        }
        timestamp_seams::get_current_timestamp::set(|| PINNED_NOW_USECS);
        timestamp_seams::get_current_datetime::set(pinned_now);
        timestamp_seams::get_current_time_usec::set(pinned_now);
        timestamp_seams::timestamptz_to_str::set(|_| String::from("(pinned)"));
    });
    // Session-timezone cells are per-thread: (re)pin on every thread.
    std::thread_local! {
        static TZ_PINNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    TZ_PINNED.with(|c| {
        if !c.get() {
            pgtz::pg_timezone_initialize();
            c.set(true);
        }
    });
}

/// C oracle errcode classes (csrc/pg_datetime_io_io.c header).
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

/// Text payload guard: the shipped Rust APIs take &str and the C oracle a
/// cstring, so the comparable domain is interior-NUL-free valid UTF-8 (the
/// server validates client encoding long before datatype input).
fn text_payload(b: &[u8]) -> Option<(&str, CString)> {
    if b.len() > 200 || b.contains(&0) {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    Some((s, CString::new(b).unwrap()))
}

fn fold_date(raw: i32) -> DateADT {
    if raw == i32::MIN || raw == i32::MAX {
        return raw; /* DATEVAL_NOBEGIN / DATEVAL_NOEND */
    }
    ((raw as i64).rem_euclid(DATE_MAX - DATE_MIN + 1) + DATE_MIN) as DateADT
}

fn fold_time(raw: i64) -> TimeADT {
    raw.rem_euclid(USECS_PER_DAY + 1)
}

fn fold_zone(raw: i32) -> i32 {
    (raw as i64).rem_euclid(2 * (TZDISP_LIMIT as i64) - 1) as i32 - (TZDISP_LIMIT - 1)
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo — the proofs wrapper-level
// pattern run without kani).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(f: PGFunction, args: [Datum; N]) -> (types_error::PgResult<Datum>, bool) {
    let cx = mcx::MemoryContext::new("datetime_io_diff_fc");
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: cx outlives this single call (function scope).
    unsafe { fcinfo.set_result_mcx(cx.mcx()) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// Assert the fc wrapper's verdict+value matches the already-C-checked core
/// result for a by-value Datum result.
fn fc_check_value(
    arm: &str,
    f: PGFunction,
    args_core: (&types_error::PgResult<Datum>, ),
    fc: (types_error::PgResult<Datum>, bool),
) {
    let core = args_core.0;
    match (core, &fc.0) {
        (Ok(cv), Ok(fv)) => assert!(
            cv.as_u64() == fv.as_u64(),
            "{arm} FC-PLANE DIVERGENCE: core={:x} fc={:x}",
            cv.as_u64(),
            fv.as_u64()
        ),
        (Err(ce), Err(fe)) => assert!(
            ce.sqlstate == fe.sqlstate,
            "{arm} FC-PLANE sqlstate: core={:?} fc={:?}",
            ce.sqlstate,
            fe.sqlstate
        ),
        _ => panic!("{arm} FC-PLANE verdict mismatch: core.ok={} fc.ok={}", core.is_ok(), fc.0.is_ok()),
    }
}

/// cstring result bytes behind a wrapper's returned Datum.
fn datum_cstr_bytes<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: the wrapper returned a NUL-terminated cstring allocation live
    // in the fc-call context (read before the context drops).
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const std::ffi::c_char).to_bytes() }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn datetime_io_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init_env();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 9 {
        0 => date_in_diff(payload),
        1 => date_out_diff(payload),
        2 => time_in_diff(payload),
        3 => time_out_diff(payload),
        4 => timetz_in_diff(payload),
        5 => timetz_out_diff(payload),
        6 => time_part_diff(payload),
        7 => make_time_diff(payload),
        _ => make_date_diff(payload),
    }
}

fn date_in_diff(payload: &[u8]) {
    let Some((&sb, text)) = payload.split_first() else {
        return;
    };
    let Some((s, cs)) = text_payload(text) else {
        return;
    };
    let (style, order) = styles(sb);

    let mut cval: i32 = 0;
    let cerr = unsafe { pg_diff_date_in(cs.as_ptr(), style, order, &mut cval) };
    if unsafe { pg_diff_datetime_tzset_nongmt() } != 0 {
        /* tz-database domain carve (see module header): nothing is compared;
         * run Rust for panic-safety only while the name budget allows. */
        if admit_tz_carved_exec() {
            let _ = adt_date::date_in(s, None);
        }
        return;
    }
    let r = adt_date::date_in(s, None);
    match &r {
        Ok(v) => assert!(
            cerr == 0 && *v == cval,
            "date_in DIVERGENCE input={s:?} style={style}/{order}: C=(err {cerr}, {cval}) Rust=Ok({v})"
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc,
                "date_in DIVERGENCE input={s:?} style={style}/{order}: C err {cerr} (val {cval}) vs Rust err {rc} ({})",
                e.message
            );
        }
    }

    // fc plane: fc_date_in(cstring, oid, typmod)
    let core = r.map(Datum::from_i32);
    let fc = fc_call::<3>(
        adt_date::builtins::fc_date_in,
        [
            Datum::from_usize(cs.as_ptr() as usize),
            Datum::from_u32(0),
            Datum::from_i32(-1),
        ],
    );
    fc_check_value("date_in", adt_date::builtins::fc_date_in, (&core,), fc);
}

fn date_out_diff(payload: &[u8]) {
    if payload.len() < 5 {
        return;
    }
    let (style, order) = styles(payload[0]);
    let date = fold_date(i32::from_le_bytes(payload[1..5].try_into().unwrap()));

    let mut cbuf = [0u8; 256];
    let cerr = unsafe { pg_diff_date_out(date, style, order, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();

    let mut rbuf = [0u8; MAXDATELEN + 1];
    let n = adt_date::date_out(date, &mut rbuf);
    assert!(
        cerr == 0 && &rbuf[..n] == &cbuf[..clen],
        "date_out DIVERGENCE date={date} style={style}/{order}: C=(err {cerr}, {:?}) Rust={:?}",
        String::from_utf8_lossy(&cbuf[..clen]),
        String::from_utf8_lossy(&rbuf[..n])
    );

    // fc plane: fc_date_out(date) -> cstring datum
    let fc = fc_call::<1>(adt_date::builtins::fc_date_out, [Datum::from_i32(date)]);
    match fc.0 {
        Ok(d) => {
            let b = datum_cstr_bytes(d);
            assert!(
                b == &rbuf[..n],
                "date_out FC-PLANE: core={:?} fc={:?}",
                String::from_utf8_lossy(&rbuf[..n]),
                String::from_utf8_lossy(b)
            );
        }
        Err(e) => panic!("date_out FC-PLANE unexpected error: {}", e.message),
    }
}

fn time_in_diff(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let (style, order) = styles(payload[0]);
    let typmod = (payload[1] % 8) as i32 - 1; /* -1..=6 */
    let Some((s, cs)) = text_payload(&payload[2..]) else {
        return;
    };

    let mut cval: i64 = 0;
    let cerr = unsafe { pg_diff_time_in(cs.as_ptr(), typmod, style, order, &mut cval) };
    if unsafe { pg_diff_datetime_tzset_nongmt() } != 0 {
        if admit_tz_carved_exec() {
            let _ = adt_date::time_in(s, typmod, None);
        }
        return; /* tz-database domain carve (see module header) */
    }
    let r = adt_date::time_in(s, typmod, None);
    match &r {
        Ok(v) => assert!(
            cerr == 0 && *v == cval,
            "time_in DIVERGENCE input={s:?} typmod={typmod} style={style}/{order}: C=(err {cerr}, {cval}) Rust=Ok({v})"
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc,
                "time_in DIVERGENCE input={s:?} typmod={typmod} style={style}/{order}: C err {cerr} vs Rust err {rc} ({})",
                e.message
            );
        }
    }

    let core = r.map(Datum::from_i64);
    let fc = fc_call::<3>(
        adt_date::builtins::fc_time_in,
        [
            Datum::from_usize(cs.as_ptr() as usize),
            Datum::from_u32(0),
            Datum::from_i32(typmod),
        ],
    );
    fc_check_value("time_in", adt_date::builtins::fc_time_in, (&core,), fc);
}

fn time_out_diff(payload: &[u8]) {
    if payload.len() < 9 {
        return;
    }
    let (style, order) = styles(payload[0]);
    let time = fold_time(i64::from_le_bytes(payload[1..9].try_into().unwrap()));

    let mut cbuf = [0u8; 256];
    let cerr = unsafe { pg_diff_time_out(time, style, order, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();

    let mut rbuf = [0u8; MAXDATELEN + 1];
    let n = adt_date::time_out(time, &mut rbuf);
    assert!(
        cerr == 0 && &rbuf[..n] == &cbuf[..clen],
        "time_out DIVERGENCE time={time} style={style}/{order}: C={:?} Rust={:?}",
        String::from_utf8_lossy(&cbuf[..clen]),
        String::from_utf8_lossy(&rbuf[..n])
    );

    let fc = fc_call::<1>(adt_date::builtins::fc_time_out, [Datum::from_i64(time)]);
    match fc.0 {
        Ok(d) => assert!(
            datum_cstr_bytes(d) == &rbuf[..n],
            "time_out FC-PLANE mismatch"
        ),
        Err(e) => panic!("time_out FC-PLANE unexpected error: {}", e.message),
    }
}

fn timetz_in_diff(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let (style, order) = styles(payload[0]);
    let typmod = (payload[1] % 8) as i32 - 1;
    let Some((s, cs)) = text_payload(&payload[2..]) else {
        return;
    };

    let mut ct: i64 = 0;
    let mut cz: i32 = 0;
    let cerr = unsafe { pg_diff_timetz_in(cs.as_ptr(), typmod, style, order, &mut ct, &mut cz) };
    if unsafe { pg_diff_datetime_tzset_nongmt() } != 0 {
        if admit_tz_carved_exec() {
            let _ = adt_date::timetz_in(s, typmod, None);
        }
        return; /* tz-database domain carve (see module header) */
    }
    let r = adt_date::timetz_in(s, typmod, None);
    match &r {
        Ok(v) => assert!(
            cerr == 0 && v.time == ct && v.zone == cz,
            "timetz_in DIVERGENCE input={s:?} typmod={typmod} style={style}/{order}: C=(err {cerr}, t={ct} z={cz}) Rust=Ok(t={} z={})",
            v.time,
            v.zone
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc,
                "timetz_in DIVERGENCE input={s:?} typmod={typmod} style={style}/{order}: C err {cerr} vs Rust err {rc} ({})",
                e.message
            );
        }
    }

    // fc plane: fc_timetz_in returns a by-ref TimeTzADT datum.
    let fc = fc_call::<3>(
        adt_date::builtins::fc_timetz_in,
        [
            Datum::from_usize(cs.as_ptr() as usize),
            Datum::from_u32(0),
            Datum::from_i32(typmod),
        ],
    );
    match (&r, &fc.0) {
        (Ok(v), Ok(d)) => {
            // SAFETY: wrapper returns &TimeTzADT allocated in the fc context.
            let w = unsafe { &*(d.as_usize() as *const TimeTzADT) };
            assert!(
                w.time == v.time && w.zone == v.zone,
                "timetz_in FC-PLANE value mismatch"
            );
        }
        (Err(ce), Err(fe)) => assert!(ce.sqlstate == fe.sqlstate, "timetz_in FC-PLANE sqlstate"),
        _ => panic!("timetz_in FC-PLANE verdict mismatch"),
    }
}

fn timetz_out_diff(payload: &[u8]) {
    if payload.len() < 13 {
        return;
    }
    let (style, order) = styles(payload[0]);
    let t = TimeTzADT {
        time: fold_time(i64::from_le_bytes(payload[1..9].try_into().unwrap())),
        zone: fold_zone(i32::from_le_bytes(payload[9..13].try_into().unwrap())),
    };

    let mut cbuf = [0u8; 256];
    let cerr = unsafe { pg_diff_timetz_out(t.time, t.zone, style, order, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();

    let mut rbuf = [0u8; MAXDATELEN + 1];
    let n = adt_date::timetz_out(&t, &mut rbuf);
    assert!(
        cerr == 0 && &rbuf[..n] == &cbuf[..clen],
        "timetz_out DIVERGENCE t={} z={} style={style}/{order}: C={:?} Rust={:?}",
        t.time,
        t.zone,
        String::from_utf8_lossy(&cbuf[..clen]),
        String::from_utf8_lossy(&rbuf[..n])
    );

    let fc = fc_call::<1>(
        adt_date::builtins::fc_timetz_out,
        [Datum::from_usize(&t as *const TimeTzADT as usize)],
    );
    match fc.0 {
        Ok(d) => assert!(
            datum_cstr_bytes(d) == &rbuf[..n],
            "timetz_out FC-PLANE mismatch"
        ),
        Err(e) => panic!("timetz_out FC-PLANE unexpected error: {}", e.message),
    }
}

fn time_part_diff(payload: &[u8]) {
    if payload.len() < 9 {
        return;
    }
    let time = fold_time(i64::from_le_bytes(payload[0..8].try_into().unwrap()));
    let units = &payload[8..];
    if units.is_empty() || units.len() > 63 || units.contains(&0) {
        return; /* < NAMEDATALEN: identifier truncation never fires */
    }

    let mut cval: f64 = 0.0;
    let cerr = unsafe { pg_diff_time_part(units.as_ptr(), units.len() as i32, time, &mut cval) };
    match adt_date::time_part_common(units, time, false) {
        Ok(PartValue::Float(v)) => assert!(
            cerr == 0 && v.to_bits() == cval.to_bits(),
            "time_part DIVERGENCE units={:?} time={time}: C=(err {cerr}, {cval:e}) Rust=Ok({v:e})",
            String::from_utf8_lossy(units)
        ),
        Ok(_) => panic!("time_part returned numeric with retnumeric=false"),
        Err(e) => {
            let rc = rust_err_class(&e);
            assert!(
                cerr == rc,
                "time_part DIVERGENCE units={:?} time={time}: C err {cerr} vs Rust err {rc} ({})",
                String::from_utf8_lossy(units),
                e.message
            );
        }
    }
}

fn make_time_diff(payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let hour = i32::from_le_bytes(payload[0..4].try_into().unwrap());
    let min = i32::from_le_bytes(payload[4..8].try_into().unwrap());
    let sec = f64::from_le_bytes(payload[8..16].try_into().unwrap());

    let mut cval: i64 = 0;
    let cerr = unsafe { pg_diff_make_time(hour, min, sec, &mut cval) };
    let r = adt_date::make_time(hour, min, sec);
    match &r {
        Ok(v) => assert!(
            cerr == 0 && *v == cval,
            "make_time DIVERGENCE h={hour} m={min} s={sec:e}: C=(err {cerr}, {cval}) Rust=Ok({v})"
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc,
                "make_time DIVERGENCE h={hour} m={min} s={sec:e}: C err {cerr} vs Rust err {rc} ({})",
                e.message
            );
        }
    }

    let core = r.map(Datum::from_i64);
    let fc = fc_call::<3>(
        adt_date::builtins::fc_make_time,
        [Datum::from_i32(hour), Datum::from_i32(min), Datum::from_f64(sec)],
    );
    fc_check_value("make_time", adt_date::builtins::fc_make_time, (&core,), fc);
}

fn make_date_diff(payload: &[u8]) {
    if payload.len() < 12 {
        return;
    }
    let y = i32::from_le_bytes(payload[0..4].try_into().unwrap());
    let m = i32::from_le_bytes(payload[4..8].try_into().unwrap());
    let d = i32::from_le_bytes(payload[8..12].try_into().unwrap());

    let mut cval: i32 = 0;
    let cerr = unsafe { pg_diff_make_date(y, m, d, &mut cval) };
    let r = adt_date::make_date(y, m, d);
    match &r {
        Ok(v) => assert!(
            cerr == 0 && *v == cval,
            "make_date DIVERGENCE y={y} m={m} d={d}: C=(err {cerr}, {cval}) Rust=Ok({v})"
        ),
        Err(e) => {
            let rc = rust_err_class(e);
            assert!(
                cerr == rc,
                "make_date DIVERGENCE y={y} m={m} d={d}: C err {cerr} vs Rust err {rc} ({})",
                e.message
            );
        }
    }

    let core = r.map(Datum::from_i32);
    let fc = fc_call::<3>(
        adt_date::builtins::fc_make_date,
        [Datum::from_i32(y), Datum::from_i32(m), Datum::from_i32(d)],
    );
    fc_check_value("make_date", adt_date::builtins::fc_make_date, (&core,), fc);
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/datetime_io_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/datetime_io_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                datetime_io_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    fn arm(sel: u8, tail: &[u8]) -> Vec<u8> {
        let mut v = vec![sel];
        v.extend_from_slice(tail);
        v
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // in-arms: style byte + (typmod) + text; ok and error shapes
        for style in 0u8..15 {
            datetime_io_diff(&arm(0, &[style, b'2', b'0', b'2', b'4', b'-', b'1', b'-', b'5']));
            datetime_io_diff(&arm(0, &[style, b'e', b'p', b'o', b'c', b'h']));
            datetime_io_diff(&arm(0, &[style, b'n', b'o', b'w']));
            datetime_io_diff(&arm(0, &[style, b'z', b'z']));
            datetime_io_diff(&arm(2, &[style, 3, b'1', b'2', b':', b'3', b'4', b':', b'5', b'6']));
            datetime_io_diff(&arm(4, &[style, 0, b'0', b'4', b':', b'0', b'5', b':', b'0', b'6', b'+', b'0', b'8']));
            datetime_io_diff(&arm(4, &[style, 0, b'0', b'4', b':', b'0', b'5', b':', b'0', b'6']));
            datetime_io_diff(&arm(4, &[style, 0, b'0', b'4', b':', b'0', b'5', b':', b'0', b'6', b' ', b'G', b'M', b'T']));
        }
        // out-arms over a value grid
        for (i, raw) in [0i64, 1, -1, 86_400_000_000, i64::MAX, i64::MIN, 45_296_789_000].iter().enumerate() {
            let mut p = vec![3, (i as u8) * 3];
            p.extend_from_slice(&raw.to_le_bytes());
            datetime_io_diff(&p);
            let mut q = vec![5, (i as u8) * 3];
            q.extend_from_slice(&raw.to_le_bytes());
            q.extend_from_slice(&(*raw as i32).to_le_bytes());
            datetime_io_diff(&q);
        }
        for raw in [0i32, 1, -1, 8780, -10957, i32::MAX, i32::MIN, 2_932_896, -2_451_545] {
            let mut p = vec![1, 7];
            p.extend_from_slice(&raw.to_le_bytes());
            datetime_io_diff(&p);
        }
        // time_part over the full units table
        for u in ["microseconds", "milliseconds", "second", "minute", "hour",
                  "epoch", "timezone", "timezone_hour", "day", "year", "junk"] {
            let mut p = vec![6];
            p.extend_from_slice(&45_296_789_000i64.to_le_bytes());
            p.extend_from_slice(u.as_bytes());
            datetime_io_diff(&p);
        }
        // constructors incl. error + non-finite shapes
        for (h, m, s) in [(12, 30, 45.5), (25, 0, 0.0), (0, 0, f64::NAN),
                          (0, 0, f64::INFINITY), (23, 59, 59.999_999_5), (-1, 0, 0.0)] {
            let mut p = vec![7];
            p.extend_from_slice(&(h as i32).to_le_bytes());
            p.extend_from_slice(&(m as i32).to_le_bytes());
            p.extend_from_slice(&f64::to_le_bytes(s));
            datetime_io_diff(&p);
        }
        for (y, m, d) in [(2024, 2, 29), (2023, 2, 29), (0, 1, 1), (-44, 3, 15),
                          (5874897, 12, 31), (i32::MIN, 1, 1), (1, 13, 1), (1, 0, 1)] {
            let mut p = vec![8];
            p.extend_from_slice(&i32::to_le_bytes(y));
            p.extend_from_slice(&i32::to_le_bytes(m));
            p.extend_from_slice(&i32::to_le_bytes(d));
            datetime_io_diff(&p);
        }
    }

    fn rss_kb() -> usize {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0)
    }

    /// Leak regression probe for the zone-name admission budget: feeds an
    /// endless stream of DISTINCT invented POSIX zone names and reports the
    /// RSS slope per window. The budget makes the slope decay toward zero;
    /// without it this sustains ~100 B/exec (pgrust's pg_tzset cache is
    /// process-lifetime by C parity, ~21KB per entry) and a fleet campaign
    /// OOMs. `#[ignore]`d: it is an instrument, minutes long, and reads RSS
    /// via `ps`. Run with `--ignored --nocapture`.
    #[test]
    #[ignore]
    fn probe_tzname_leak() {
        let _serial = crate::c_oracle_serial();
        // Warm.
        for i in 0..1000u32 {
            let mut p = vec![4];
            p.extend_from_slice(format!("12:30:45 W{i}5").as_bytes());
            datetime_io_diff(&p);
        }
        let n = 50_000u32;
        let mut base = 1000u32;
        for w in 0..4 {
            let before = rss_kb();
            for i in base..base + n {
                let mut p = vec![4];
                p.extend_from_slice(format!("12:30:45 W{i}5").as_bytes());
                datetime_io_diff(&p);
            }
            let after = rss_kb();
            base += n;
            eprintln!(
                "window {w}: RSS {before}KB -> {after}KB ({:.2} B/exec over {n} distinct-name execs)",
                (after as f64 - before as f64) * 1024.0 / n as f64
            );
        }
    }

}
