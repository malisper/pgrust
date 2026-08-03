//! fmt_dch_diff: differential fuzz driver — shipped Rust `adt_formatting` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_fmt_dch_io.c). Crate under test: crates/backend/utils/adt/formatting
//! (DCH = date/time format-picture half; the NUM half is fmt_num_diff).
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! ENVIRONMENT PINS (mirrored in csrc/pg_fmt_dch_io.c — see its header):
//!   - session timezone = GMT (pgtz::pg_tzset(b"GMT")), zoneabbrevtbl empty;
//!   - database encoding = UTF8; inputs restricted to interior-NUL-free
//!     valid UTF-8 (the only shape reachable through the SQL layer — the
//!     server validates client encoding long before to_char runs);
//!   - collation = C_COLLATION_OID (950) on every call; lc_time boots "C"
//!     on the Rust side and the C oracle's localized_* arrays are the
//!     lc_time=C English names, so TM-prefixed patterns stay IN-plane.
//!
//! Input layout: [selector][payload]; selector % 7 picks the arm:
//!   0 timestamp_to_char   (oid 2049): payload = i64 LE ts, rest = fmt.
//!   1 timestamptz_to_char (oid 1770): payload = i64 LE ts, rest = fmt.
//!   2 interval_to_char    (oid 1768): payload = i64 LE time + i32 LE day +
//!     i32 LE month, rest = fmt.
//!   3 to_timestamp        (oid 1778): payload = u16 LE input_len + input +
//!     rest = fmt.
//!   4 to_date             (oid 1780): same layout as 3.
//!   5 parse_datetime      (no oid; SQL/JSON datetime()): u16 LE input_len +
//!     input + 1 strict byte + rest = fmt. Hard-error shape only
//!     (escontext = None on both sides).
//!   6 datetime_format_has_tz (no oid): payload = fmt.
//!
//! Primary comparison = the fc_* wrapper plane (the SQL-visible surface,
//! including PG_RETURN_NULL parity for empty formats / non-finite inputs)
//! for arms 0-4; the core entry point is additionally cross-checked against
//! the wrapper for the to_char arms on every exec (wrapper==core except the
//! documented empty-fmt/not-finite NULL guard, where the core yields b""
//! and the wrapper yields SQL NULL exactly like C). Arms 5-6 call the pub
//! core entries directly (no fc wrapper exists; C compares the same
//! surface).
//!
//! SKIPPED (documented carves):
//!   - Interior NUL / invalid UTF-8 inputs (not server-reachable; C oracle
//!     is cstring-based downstream of text_to_cstring exactly like PG).
//!   - Soft-error (escontext) shapes: the Rust driver passes None
//!     everywhere, matching the C oracle's NULL escontext. The soft lanes
//!     share the identical parse cascade; error-verdict parity is carried
//!     by the hard lane.

use datum::{Datum, NullableDatum, VarlenaRef};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{LocalFcinfo, PGFunction};

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    fn pg_diff_fmt_timestamp_to_char(
        ts: i64,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_timestamptz_to_char(
        ts: i64,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_interval_to_char(
        time_usec: i64,
        day: i32,
        month: i32,
        fmt: *const u8,
        fmt_len: i32,
        out: *mut u8,
        out_cap: i32,
        out_len: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_to_timestamp(
        txt: *const u8,
        txt_len: i32,
        fmt: *const u8,
        fmt_len: i32,
        out_ts: *mut i64,
    ) -> i32;
    fn pg_diff_fmt_to_date(
        txt: *const u8,
        txt_len: i32,
        fmt: *const u8,
        fmt_len: i32,
        out_date: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_parse_datetime(
        txt: *const u8,
        txt_len: i32,
        fmt: *const u8,
        fmt_len: i32,
        strict: i32,
        out_kind: *mut i32,
        out_typmod: *mut i32,
        out_tz: *mut i32,
        out_v: *mut i64,
        out_v2: *mut i32,
    ) -> i32;
    fn pg_diff_fmt_datetime_format_has_tz(fmt: *const u8, fmt_len: i32) -> i32;
}

const COLLID: Oid = types_core::catalog::C_COLLATION_OID;
const FMT_MAX: usize = 256;
const INPUT_MAX: usize = 256;
const OUT_CAP: usize = 8192;

// ---------------------------------------------------------------------------
// Environment pins (once per process; see module header).
// ---------------------------------------------------------------------------

/// The vendored C oracle's DCH cache (formatting.c statics) is not
/// thread-safe; the fuzz binary is single-threaded, but `cargo test` is not.
/// Every arm holds this lock across its C call + Rust call so cross-thread
/// cache churn cannot fabricate verdicts (observed as a one-off smoke flake
/// after the 2026-07-31 rebase).
static ORACLE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn oracle_lock() -> std::sync::MutexGuard<'static, ()> {
    ORACLE_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn pin_environment() {
    // session_timezone is per-thread state; pin it on every thread that
    // drives the harness (libFuzzer = one thread; cargo test = many).
    thread_local! {
        static PINNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    PINNED.with(|p| {
        if !p.get() {
            pgtz::pg_timezone_initialize();
            let gmt = pgtz::pg_tzset(b"GMT").expect("GMT loads without tzdata files");
            pgtz::set_session_timezone(Some(gmt));
            // ENCODING PIN: mbutils boots SQL_ASCII (per-thread); the C
            // oracle is pinned to UTF8, so pin the Rust side to match.
            // (Found the hard way: unpinned, parse_format split one
            // multibyte literal into per-byte CHAR nodes and the from_char
            // cursor walk diverged.)
            mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("UTF8 encoding pin");
            p.set(true);
        }
    });
}

// ---------------------------------------------------------------------------
// C-oracle errcode class table (mirror of csrc/pg_fmt_dch_io.c).
// ---------------------------------------------------------------------------

fn c_err_to_sqlstate(c: i32) -> Option<types_error::SqlState> {
    use types_error::*;
    Some(match c {
        101 => ERRCODE_SYNTAX_ERROR,
        102 => ERRCODE_INVALID_DATETIME_FORMAT,
        103 => ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
        104 => ERRCODE_DATETIME_FIELD_OVERFLOW,
        105 => ERRCODE_INVALID_TEXT_REPRESENTATION,
        106 => ERRCODE_FEATURE_NOT_SUPPORTED,
        107 => ERRCODE_INDETERMINATE_COLLATION,
        108 => ERRCODE_INVALID_PARAMETER_VALUE,
        109 => ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        110 => ERRCODE_DIVISION_BY_ZERO,
        111 => ERRCODE_INTERVAL_FIELD_OVERFLOW,
        114 => ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        115 => ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
        116 => ERRCODE_CONFIG_FILE_ERROR,
        117 => ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
        _ => return None,
    })
}

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

/// Panic unless the Rust error's sqlstate matches the C oracle's class.
fn assert_err_parity(arm: &str, ctx: &dyn core::fmt::Debug, cerr: i32, e: &PgError) {
    match c_err_to_sqlstate(cerr) {
        Some(expect) => assert!(
            e.sqlstate == expect,
            "{arm} ERRCODE DIVERGENCE {ctx:?}: C class {cerr} vs Rust sqlstate {:?} ({})",
            e.sqlstate,
            e.message
        ),
        None => panic!(
            "{arm} ERRCODE DIVERGENCE {ctx:?}: C raised unmapped class {cerr}, Rust ({})",
            e.message
        ),
    }
}

// ---------------------------------------------------------------------------
// fc-wrapper plumbing (native LocalFcinfo, real mcx).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(COLLID);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// Build a 4B-header text varlena image; pass its pointer as the arg Datum.
fn text_image(payload: &[u8]) -> Vec<u8> {
    let len = payload.len() + 4;
    let mut v = Vec::with_capacity(len);
    v.extend_from_slice(&((len as u32) << 2).to_le_bytes());
    v.extend_from_slice(payload);
    v
}

/// Payload bytes behind a wrapper-returned text Datum (4B-U image).
fn datum_text_bytes<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: wrapper text results are live 4B-header varlenas in the
    // driver-owned result mcx for the duration of the exec.
    unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn fmt_dch_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let _oracle_guard = oracle_lock();
    pin_environment();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 7 {
        0 => tochar_diff(payload, false),
        1 => tochar_diff(payload, true),
        2 => interval_to_char_diff(payload),
        3 => fromchar_diff(payload, false),
        4 => fromchar_diff(payload, true),
        5 => parse_datetime_diff(payload),
        _ => has_tz_diff(payload),
    }
}

/// Shared guard: server-reachable text shape only.
fn text_ok(b: &[u8], cap: usize) -> bool {
    b.len() <= cap && !b.contains(&0) && core::str::from_utf8(b).is_ok()
}

// ---------------------------------------------------------------------------
// Arms 0/1: timestamp_to_char / timestamptz_to_char (oids 2049 / 1770).
// ---------------------------------------------------------------------------

fn tochar_diff(payload: &[u8], with_tz: bool) {
    let arm = if with_tz { "timestamptz_to_char" } else { "timestamp_to_char" };
    if payload.len() < 8 {
        return;
    }
    let ts = i64::from_le_bytes(payload[..8].try_into().unwrap());
    let fmt = &payload[8..];
    if !text_ok(fmt, FMT_MAX) {
        return;
    }

    let mut out = [0u8; OUT_CAP];
    let mut out_len: i32 = 0;
    let cst = unsafe {
        if with_tz {
            pg_diff_fmt_timestamptz_to_char(
                ts,
                fmt.as_ptr(),
                fmt.len() as i32,
                out.as_mut_ptr(),
                OUT_CAP as i32,
                &mut out_len,
            )
        } else {
            pg_diff_fmt_timestamp_to_char(
                ts,
                fmt.as_ptr(),
                fmt.len() as i32,
                out.as_mut_ptr(),
                OUT_CAP as i32,
                &mut out_len,
            )
        }
    };
    if cst == -2 {
        return; /* out_cap sizing guard, not a verdict */
    }
    let cerr = c_errcode();
    let cbytes = &out[..out_len.max(0) as usize];

    let ctxmgr = mcx::MemoryContext::new("fmt_dch_diff");
    let m = ctxmgr.mcx();

    // fc-wrapper plane (SQL-visible surface incl. NULL parity).
    let fmt_img = text_image(fmt);
    let f: PGFunction = if with_tz {
        adt_formatting::fmgr_builtins::fc_timestamptz_to_char
    } else {
        adt_formatting::fmgr_builtins::fc_timestamp_to_char
    };
    let (wres, wnull) = fc_call::<2>(
        f,
        m,
        [Datum::from_i64(ts), Datum::from_usize(fmt_img.as_ptr() as usize)],
    );
    match (cst, &wres) {
        (1, Ok(_)) => assert!(
            wnull,
            "{arm} NULL DIVERGENCE ts={ts} fmt={fmt:?}: C NULL vs Rust non-null"
        ),
        (0, Ok(d)) => {
            assert!(!wnull, "{arm} NULL DIVERGENCE ts={ts} fmt={fmt:?}: Rust NULL vs C ok");
            let rbytes = datum_text_bytes(*d);
            assert!(
                rbytes == cbytes,
                "{arm} VALUE DIVERGENCE ts={ts} fmt={:?}: C={:?} Rust={:?}",
                String::from_utf8_lossy(fmt),
                String::from_utf8_lossy(cbytes),
                String::from_utf8_lossy(rbytes)
            );
        }
        (-1, Err(e)) => assert_err_parity(arm, &(ts, fmt), cerr, e),
        _ => panic!(
            "{arm} VERDICT DIVERGENCE ts={ts} fmt={:?}: C status {cst} (err {cerr}) vs Rust {:?}",
            String::from_utf8_lossy(fmt),
            wres.as_ref().map(|_| ()).map_err(|e| e.message.clone())
        ),
    }

    // core-vs-wrapper consistency (Rust-only invariant; the empty-fmt /
    // not-finite guard is wrapper-level NULL, core-level b"").
    let core = if with_tz {
        adt_formatting::dch_entry::timestamptz_to_char(m, COLLID, ts, fmt)
    } else {
        adt_formatting::dch_entry::timestamp_to_char(m, COLLID, ts, fmt)
    };
    match (&wres, &core) {
        (Ok(d), Ok(v)) => {
            let expect: &[u8] = if wnull { b"" } else { datum_text_bytes(*d) };
            assert!(v.data() == expect, "{arm} WRAPPER!=CORE ts={ts} fmt={fmt:?}");
        }
        (Err(we), Err(ce)) => assert!(
            we.sqlstate == ce.sqlstate,
            "{arm} WRAPPER!=CORE sqlstate ts={ts} fmt={fmt:?}"
        ),
        // wrapper NULL guard returns Ok before the core parse; a core error
        // with a wrapper Ok is only consistent in that guard shape.
        (Ok(_), Err(_)) => assert!(
            wnull,
            "{arm} WRAPPER!=CORE verdict ts={ts} fmt={fmt:?}"
        ),
        _ => panic!("{arm} WRAPPER!=CORE verdict ts={ts} fmt={fmt:?}"),
    }
}

// ---------------------------------------------------------------------------
// Arm 2: interval_to_char (oid 1768).
// ---------------------------------------------------------------------------

fn interval_to_char_diff(payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let time = i64::from_le_bytes(payload[..8].try_into().unwrap());
    let day = i32::from_le_bytes(payload[8..12].try_into().unwrap());
    let month = i32::from_le_bytes(payload[12..16].try_into().unwrap());
    let fmt = &payload[16..];
    if !text_ok(fmt, FMT_MAX) {
        return;
    }

    let mut out = [0u8; OUT_CAP];
    let mut out_len: i32 = 0;
    let cst = unsafe {
        pg_diff_fmt_interval_to_char(
            time,
            day,
            month,
            fmt.as_ptr(),
            fmt.len() as i32,
            out.as_mut_ptr(),
            OUT_CAP as i32,
            &mut out_len,
        )
    };
    if cst == -2 {
        return;
    }
    let cerr = c_errcode();
    let cbytes = &out[..out_len.max(0) as usize];

    let ctxmgr = mcx::MemoryContext::new("fmt_dch_diff");
    let m = ctxmgr.mcx();

    // 16-byte on-disk interval image for the fc wrapper's arg_ptr(0).
    let mut iv_img = [0u8; 16];
    iv_img[..8].copy_from_slice(&time.to_le_bytes());
    iv_img[8..12].copy_from_slice(&day.to_le_bytes());
    iv_img[12..16].copy_from_slice(&month.to_le_bytes());
    let fmt_img = text_image(fmt);
    let (wres, wnull) = fc_call::<2>(
        adt_formatting::fmgr_builtins::fc_interval_to_char,
        m,
        [
            Datum::from_usize(iv_img.as_ptr() as usize),
            Datum::from_usize(fmt_img.as_ptr() as usize),
        ],
    );
    match (cst, &wres) {
        (1, Ok(_)) => assert!(
            wnull,
            "interval_to_char NULL DIVERGENCE iv=({time},{day},{month}) fmt={fmt:?}: C NULL vs Rust non-null"
        ),
        (0, Ok(d)) => {
            assert!(
                !wnull,
                "interval_to_char NULL DIVERGENCE iv=({time},{day},{month}) fmt={fmt:?}: Rust NULL vs C ok"
            );
            let rbytes = datum_text_bytes(*d);
            assert!(
                rbytes == cbytes,
                "interval_to_char VALUE DIVERGENCE iv=({time},{day},{month}) fmt={:?}: C={:?} Rust={:?}",
                String::from_utf8_lossy(fmt),
                String::from_utf8_lossy(cbytes),
                String::from_utf8_lossy(rbytes)
            );
        }
        (-1, Err(e)) => assert_err_parity("interval_to_char", &(time, day, month, fmt), cerr, e),
        _ => panic!(
            "interval_to_char VERDICT DIVERGENCE iv=({time},{day},{month}) fmt={:?}: C status {cst} (err {cerr}) vs Rust {:?}",
            String::from_utf8_lossy(fmt),
            wres.as_ref().map(|_| ()).map_err(|e| e.message.clone())
        ),
    }
}

// ---------------------------------------------------------------------------
// Arms 3/4: to_timestamp / to_date (oids 1778 / 1780).
// ---------------------------------------------------------------------------

/// RATIFIED DIVERGENCE (ledger rows 1778/1780, ratified 2026-07-31, landed
/// via fix/y-yyy-range): pgrust range-checks the Y,YYY millennia field and
/// rejects out-of-int values with the 22008 class; C 18.3 wrap-accepts and
/// proceeds — it can then succeed OR fail on ANY downstream field (22007;
/// 22009 via a tz field — the seed-div-007 shape found by this target at
/// ~40M execs, recorded in the ledger note). The carve is therefore keyed
/// ONLY on (format contains Y,YYY) + (Rust error is the ratified 22008
/// rejection); the C-side outcome is deliberately not consulted. Seeds
/// seed-yyyy-* / seed-div-004/005/007-* keep the surface hot.
fn yyyy_range_carve(fmt: &[u8], _cerr: i32, e: &PgError) -> bool {
    let has_yyyy = fmt
        .windows(5)
        .any(|w| w.eq_ignore_ascii_case(b"Y,YYY"));
    has_yyyy
        && (e.sqlstate == types_error::ERRCODE_DATETIME_FIELD_OVERFLOW
            || e.sqlstate == types_error::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

fn fromchar_diff(payload: &[u8], is_date: bool) {
    let arm = if is_date { "to_date" } else { "to_timestamp" };
    if payload.len() < 2 {
        return;
    }
    let ilen = u16::from_le_bytes(payload[..2].try_into().unwrap()) as usize;
    let rest = &payload[2..];
    if ilen > rest.len() {
        return;
    }
    let (input, fmt) = rest.split_at(ilen);
    if !text_ok(input, INPUT_MAX) || !text_ok(fmt, FMT_MAX) {
        return;
    }

    let mut c_ts: i64 = 0;
    let mut c_date: i32 = 0;
    let cst = unsafe {
        if is_date {
            pg_diff_fmt_to_date(
                input.as_ptr(),
                input.len() as i32,
                fmt.as_ptr(),
                fmt.len() as i32,
                &mut c_date,
            )
        } else {
            pg_diff_fmt_to_timestamp(
                input.as_ptr(),
                input.len() as i32,
                fmt.as_ptr(),
                fmt.len() as i32,
                &mut c_ts,
            )
        }
    };
    let cerr = c_errcode();

    let ctxmgr = mcx::MemoryContext::new("fmt_dch_diff");
    let m = ctxmgr.mcx();
    let input_img = text_image(input);
    let fmt_img = text_image(fmt);
    let f: PGFunction = if is_date {
        adt_formatting::fmgr_builtins::fc_to_date
    } else {
        adt_formatting::fmgr_builtins::fc_to_timestamp
    };
    let (wres, wnull) = fc_call::<2>(
        f,
        m,
        [
            Datum::from_usize(input_img.as_ptr() as usize),
            Datum::from_usize(fmt_img.as_ptr() as usize),
        ],
    );
    match (cst, &wres) {
        (0, Ok(d)) => {
            assert!(!wnull, "{arm} NULL DIVERGENCE input={input:?} fmt={fmt:?}");
            if is_date {
                let r = d.as_i32();
                assert!(
                    r == c_date,
                    "{arm} VALUE DIVERGENCE input={:?} fmt={:?}: C={c_date} Rust={r}",
                    String::from_utf8_lossy(input),
                    String::from_utf8_lossy(fmt)
                );
            } else {
                let r = d.as_i64();
                assert!(
                    r == c_ts,
                    "{arm} VALUE DIVERGENCE input={:?} fmt={:?}: C={c_ts} Rust={r}",
                    String::from_utf8_lossy(input),
                    String::from_utf8_lossy(fmt)
                );
            }
        }
        (-1, Err(e)) => {
            if yyyy_range_carve(fmt, cerr, e) {
                return; /* RATIFIED divergence, see yyyy_range_carve */
            }
            assert_err_parity(arm, &(input, fmt), cerr, e)
        }
        /* C accepts+wraps huge Y,YYY (real 18.3: to_date('4294969320,0Y4',
         * 'Y,YYY') = 2024000-01-01, docker-confirmed); Rust rejects
         * out-of-range — the exact defect fix/y-yyy-range is fixing. */
        (0, Err(e)) if yyyy_range_carve(fmt, 102, e) => {}
        _ => panic!(
            "{arm} VERDICT DIVERGENCE input={:?} fmt={:?}: C status {cst} (err {cerr}) vs Rust {:?}",
            String::from_utf8_lossy(input),
            String::from_utf8_lossy(fmt),
            wres.as_ref().map(|_| ()).map_err(|e| e.message.clone())
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 5: parse_datetime (SQL/JSON datetime(); pub entry, no fc wrapper).
// ---------------------------------------------------------------------------

fn parse_datetime_diff(payload: &[u8]) {
    use adt_formatting::dch_entry::ParsedDatetime;

    if payload.len() < 3 {
        return;
    }
    let ilen = u16::from_le_bytes(payload[..2].try_into().unwrap()) as usize;
    let rest = &payload[2..];
    if ilen + 1 > rest.len() {
        return;
    }
    let (input, rest) = rest.split_at(ilen);
    let strict = rest[0] & 1 != 0;
    let fmt = &rest[1..];
    if !text_ok(input, INPUT_MAX) || !text_ok(fmt, FMT_MAX) {
        return;
    }

    let (mut kind, mut typmod, mut tz, mut v, mut v2) = (0i32, 0i32, 0i32, 0i64, 0i32);
    let cst = unsafe {
        pg_diff_fmt_parse_datetime(
            input.as_ptr(),
            input.len() as i32,
            fmt.as_ptr(),
            fmt.len() as i32,
            strict as i32,
            &mut kind,
            &mut typmod,
            &mut tz,
            &mut v,
            &mut v2,
        )
    };
    let cerr = c_errcode();

    let ctxmgr = mcx::MemoryContext::new("fmt_dch_diff");
    let m = ctxmgr.mcx();
    let mut r_typmod: i32 = 0;
    let mut r_tz: i32 = 0;
    let rres = adt_formatting::dch_entry::parse_datetime(
        m,
        input,
        fmt,
        COLLID,
        strict,
        &mut r_typmod,
        &mut r_tz,
        None,
    );
    match (cst, &rres) {
        (0, Ok(Some(pd))) => {
            let (rkind, rv, rv2): (i32, i64, i32) = match pd {
                ParsedDatetime::Date(d) => (1, *d as i64, 0),
                ParsedDatetime::Time(t) => (2, *t, 0),
                ParsedDatetime::TimeTz(tt) => (3, tt.time, tt.zone),
                ParsedDatetime::Timestamp(t) => (4, *t, 0),
                ParsedDatetime::TimestampTz(t) => (5, *t, 0),
            };
            assert!(
                (rkind, rv, rv2, r_typmod) == (kind, v, v2, typmod)
                    && (rkind != 5 || r_tz == tz),
                "parse_datetime VALUE DIVERGENCE input={:?} fmt={:?} strict={strict}: \
                 C=(kind {kind}, v {v}, v2 {v2}, typmod {typmod}, tz {tz}) \
                 Rust=(kind {rkind}, v {rv}, v2 {rv2}, typmod {r_typmod}, tz {r_tz})",
                String::from_utf8_lossy(input),
                String::from_utf8_lossy(fmt)
            );
        }
        (-1, Err(e)) => {
            if yyyy_range_carve(fmt, cerr, e) {
                return; /* RATIFIED divergence, see yyyy_range_carve */
            }
            assert_err_parity("parse_datetime", &(input, fmt, strict), cerr, e)
        }
        (0, Err(e)) if yyyy_range_carve(fmt, 102, e) => {} /* ratified, see carve */
        _ => panic!(
            "parse_datetime VERDICT DIVERGENCE input={:?} fmt={:?} strict={strict}: C status {cst} (err {cerr}) vs Rust {:?}",
            String::from_utf8_lossy(input),
            String::from_utf8_lossy(fmt),
            rres.as_ref()
                .map(|r| r.is_some())
                .map_err(|e| e.message.clone())
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 6: datetime_format_has_tz.
// ---------------------------------------------------------------------------

fn has_tz_diff(fmt: &[u8]) {
    if !text_ok(fmt, FMT_MAX) {
        return;
    }
    let cst = unsafe { pg_diff_fmt_datetime_format_has_tz(fmt.as_ptr(), fmt.len() as i32) };
    let cerr = c_errcode();
    let rres = adt_formatting::dch_entry::datetime_format_has_tz(fmt);
    match (cst, &rres) {
        (0, Ok(false)) | (1, Ok(true)) => {}
        (-1, Err(e)) => assert_err_parity("datetime_format_has_tz", &fmt, cerr, e),
        _ => panic!(
            "datetime_format_has_tz DIVERGENCE fmt={:?}: C status {cst} (err {cerr}) vs Rust {rres:?}",
            String::from_utf8_lossy(fmt)
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests: per-arm ok+error smoke + seed replay.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sel: u8, payload: &[u8]) {
        let mut v = vec![sel];
        v.extend_from_slice(payload);
        fmt_dch_diff(&v);
    }

    fn tochar_payload(ts: i64, fmt: &[u8]) -> Vec<u8> {
        let mut p = ts.to_le_bytes().to_vec();
        p.extend_from_slice(fmt);
        p
    }

    fn fromchar_payload(input: &[u8], fmt: &[u8]) -> Vec<u8> {
        let mut p = (input.len() as u16).to_le_bytes().to_vec();
        p.extend_from_slice(input);
        p.extend_from_slice(fmt);
        p
    }

    #[test]
    fn smoke_timestamp_to_char() {
        // 2000-01-01 00:00:00 = ts 0
        run(0, &tochar_payload(0, b"YYYY-MM-DD HH24:MI:SS"));
        run(0, &tochar_payload(0, b"Day, DD Month YYYY"));
        run(0, &tochar_payload(0, b"FMDay, FMDD FMMonth YYYY BC AD"));
        run(0, &tochar_payload(789793848437387, b"IYYY-IW-ID J Q RM CC"));
        run(0, &tochar_payload(-6857352000000000, b"YYYY-MM-DD AD BC"));
        run(0, &tochar_payload(0, b"")); // NULL parity
        run(0, &tochar_payload(i64::MAX, b"YYYY")); // infinity -> NULL parity
        run(0, &tochar_payload(i64::MAX - 1, b"YYYY")); // out of range error
        run(0, &tochar_payload(0, b"TMDay TMMonth")); // TM under C locale
        run(0, &tochar_payload(0, b"HH24:MI:SS.MS.US SSSS TZ OF"));
        run(0, &tochar_payload(0, b"Y,YYY \"lit\" \\\"esc"));
        run(0, &tochar_payload(0, b"DDD IDDD WW W D ID"));
        run(0, &tochar_payload(0, b"YYYYTH yyyyth SP"));
    }

    #[test]
    fn smoke_timestamptz_to_char() {
        run(1, &tochar_payload(0, b"YYYY-MM-DD HH24:MI:SS TZ OF TZH:TZM"));
        run(1, &tochar_payload(-1, b"US us"));
    }

    #[test]
    fn smoke_interval_to_char() {
        let mut p = 3661000001i64.to_le_bytes().to_vec(); // 1h1m1.000001s
        p.extend_from_slice(&5i32.to_le_bytes());
        p.extend_from_slice(&14i32.to_le_bytes());
        p.extend_from_slice(b"YYYY MM DD HH24:MI:SS");
        run(2, &p);
        // not-finite interval -> NULL parity
        let mut p = i64::MAX.to_le_bytes().to_vec();
        p.extend_from_slice(&i32::MAX.to_le_bytes());
        p.extend_from_slice(&i32::MAX.to_le_bytes());
        p.extend_from_slice(b"HH24");
        run(2, &p);
    }

    #[test]
    fn smoke_to_timestamp() {
        run(3, &fromchar_payload(b"2000-01-01 12:34:56", b"YYYY-MM-DD HH24:MI:SS"));
        run(3, &fromchar_payload(b"1 4713 BC", b"DDD YYYY BC"));
        run(3, &fromchar_payload(b"notadate", b"YYYY-MM-DD"));
        run(3, &fromchar_payload(b"2000-13-40", b"YYYY-MM-DD"));
        run(3, &fromchar_payload(b"  15:30:20 +05:30", b"  HH24:MI:SS OF"));
        run(3, &fromchar_payload(b"gmt 10:00", b"TZ HH24:MI"));
        run(3, &fromchar_payload(b"est 10:00", b"TZ HH24:MI")); // unknown abbrev under pin
        run(3, &fromchar_payload(b"2454337", b"J"));
        run(3, &fromchar_payload(b"05 3", b"IW ID"));
    }

    #[test]
    fn smoke_to_date() {
        run(4, &fromchar_payload(b"2000-01-01", b"YYYY-MM-DD"));
        run(4, &fromchar_payload(b"5874897-12-31", b"YYYY-MM-DD"));
        run(4, &fromchar_payload(b"5874898-01-01", b"YYYY-MM-DD")); // date oob
        run(4, &fromchar_payload(b"IV", b"RM"));
    }

    #[test]
    fn smoke_parse_datetime() {
        // [u16 len][input][strict][fmt]
        let mk = |input: &[u8], strict: u8, fmt: &[u8]| {
            let mut p = (input.len() as u16).to_le_bytes().to_vec();
            p.extend_from_slice(input);
            p.push(strict);
            p.extend_from_slice(fmt);
            p
        };
        run(5, &mk(b"2000-01-01", 0, b"YYYY-MM-DD"));
        run(5, &mk(b"12:34:56", 0, b"HH24:MI:SS"));
        run(5, &mk(b"12:34:56 +03:00", 0, b"HH24:MI:SS OF"));
        run(5, &mk(b"2000-01-01 12:34:56 +03:00", 0, b"YYYY-MM-DD HH24:MI:SS OF"));
        run(5, &mk(b"2000-01-01 12:34", 1, b"YYYY-MM-DD HH24:MI:SS"));
        run(5, &mk(b"junk", 0, b"YYYY"));
    }

    #[test]
    fn smoke_has_tz() {
        run(6, b"YYYY-MM-DD");
        run(6, b"YYYY TZ");
        run(6, b"OF");
        run(6, b"TZH");
        run(6, b"\xff\xfe"); // rejected by text_ok, no-op
    }

    /// Replay every committed seed through the driver.
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/fmt_dch_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/fmt_dch_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                fmt_dch_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }
}

#[cfg(test)]
mod probe {
    use super::*;

    #[test]
    #[ignore = "manual probe"]
    fn node_probe() {
        use adt_formatting::tables::*;
        let fmt = "\u{01bb}MM".as_bytes();
        let nodes = adt_formatting::parse::parse_format(
            fmt,
            &DCH_KEYWORDS,
            &DCH_SUFF,
            &DCH_INDEX,
            DCH_FLAG,
            None,
        )
        .unwrap();
        for n in nodes.iter() {
            println!("typ={} key={} suffix={} character={:?}", n.typ, n.key, n.suffix, &n.character);
        }
    }

    #[test]
    #[ignore = "manual probe"]
    fn of_sign_probe() {
        let _serial = crate::c_oracle_serial();
        pin_environment();
        let cases: Vec<(&[u8], &[u8])> = vec![
            (b"312", "\u{01bb}MM".as_bytes()),
            (b"312", b"XMM"),
            ("\u{01bb}12".as_bytes(), "\u{01bb}MM".as_bytes()),
        ];
        for (inp, fmt) in cases {
            let mut c: i64 = 0;
            let cst = unsafe {
                pg_diff_fmt_to_timestamp(inp.as_ptr(), inp.len() as i32, fmt.as_ptr(), fmt.len() as i32, &mut c)
            };
            let cerr = c_errcode();
            let ctx = mcx::MemoryContext::new("probe");
            let m = ctx.mcx();
            let r = adt_formatting::dch_entry::to_timestamp(m, COLLID, inp, fmt);
            println!(
                "input={:?} fmt={:?}  C=(st {cst} err {cerr} val {c})  Rust={:?}",
                String::from_utf8_lossy(inp),
                String::from_utf8_lossy(fmt),
                r
            );
        }
    }
}
