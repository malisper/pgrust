//! tzfam_diff: differential fuzz driver for the three "tz family" crates
//! vs verbatim vendored PostgreSQL 18.3 C (csrc/pg_tzfam_io.c, upstream sha
//! 62d6c7d3df; lane p1-mb-tzfam). Selector = data[0] % 4:
//!
//!   0 strftime   — crates/backend/timezone/strftime: pg_strftime over a
//!                  field-decoded pg_tm + arbitrary format bytes + a caller
//!                  buffer of fuzz-chosen size (0..300). Compared: verdict
//!                  (Some(len) vs C size_t; None vs C ERANGE 0-return) and
//!                  the buffer bytes (through the trailing NUL on success;
//!                  the whole truncated buffer on overflow).
//!   1 tzparser   — crates/backend/utils/misc/tzparser: load_tzoffsets over
//!                  fixture abbrev files written per exec into
//!                  $PGRUST_PGSHAREDIR/timezonesets ("aaa" + optional
//!                  "bbb"/"ccc"/"ddd" split on 0xFF for @INCLUDE chains;
//!                  both sides read the SAME env-pinned dir — C via the
//!                  get_share_path shim, Rust via tzsets_dir()). Compared:
//!                  verdict, the converted TimeZoneAbbrevTable (token bytes,
//!                  type TZ/DTZ/DYNTZ, fixed offset value, dynamic zone
//!                  string), and the GUC check-error channel (message +
//!                  detail exact for parse arms; prefix-only for the two
//!                  filesystem %m arms, hint presence only — message TAILS
//!                  there are strerror vs io::Error platform text, and the
//!                  C hint interpolates my_exec_path where the Rust hint
//!                  names the directory: ratified message-text non-surface).
//!                  A flag byte can remove the timezonesets dir for the
//!                  exec, driving the could-not-open-directory arm on both
//!                  sides.
//!   2 ts_locale  — crates/backend/tsearch/ts_locale predicates: t_isalpha /
//!                  t_isalnum / t_iseq / byte_isspace over every suffix of
//!                  the payload vs the verbatim C macros + C-locale isspace;
//!                  lowerstr postcondition (ASCII inputs == ASCII downcase
//!                  under the pinned C default collation). database_ctype_is_c
//!                  pinned TRUE on both sides (the census C-locale arm; the
//!                  wide char2wchar/mbstowcs path is the locale carve —
//!                  exception rows, C counterpart ts_locale.c:44-45).
//!                  Encoding pinned UTF8 on both sides. Both sides call the
//!                  ONE in-process libc ctype table (same posture as the
//!                  one-libm earthdistance arm in miscfam_diff): the diff
//!                  validates the dispatch logic, not libc.
//!   3 ts_locale  — file/stoplist faces (Rust-postcondition arm; the C
//!                  counterparts live in ts_utils.c/define.c outside this
//!                  crate's census C file): get_tsearch_config_filename
//!                  (name validation error 22023 + path assembly incl. the
//!                  staged-dir hit, driven by a staged file created at
//!                  init), readstoplist/searchstoplist (sortedness,
//!                  membership == linear scan, every parsed word found),
//!                  tsearch_readlines (line split-inclusive reassembly),
//!                  def_get_boolean (full spec asserted in-driver),
//!                  lexize_result_ref.
//!
//! Comparison planes: value bytes + error verdict + GUC check-error channel
//! (tzparser's soft-error protocol — its errors never carry a sqlstate
//! beyond the default; guc.c owns the 22023 wrap) + no-panic everywhere.
//!
//! DOMAIN CARVES (documented, C caller/platform contract):
//!   - strftime: tm fields decoded i16-wide (year full i32, gmtoff i32,
//!     isdst i8): the C caller domain is pg_localtime/pg_gmtime output;
//!     outside these widths C -fwrapv arithmetic in %j/%U/%W/V-G-g is an
//!     implementation artifact no pg_tm from the timezone engine produces.
//!     Format bytes are NUL-free (C cstring contract).
//!   - tzparser: fixture bytes are NUL-free. RULED BUG-FOR-BUG / ratified
//!     NON-SURFACE (Michael 2026-08-01; ledger of record =
//!     docs/verification/phase1-claims.tsv row backend/utils/misc/tzparser;
//!     went AGAINST the investigating lane's MATCH recommendation — pgrust
//!     keeps its behavior, do NOT make it match C's truncation): C's
//!     ParseTzFile fgets a line into tzbuf and every consumer (strlen,
//!     pg_strncasecmp, splitTzLine) treats it as a cstring, so an interior
//!     NUL makes C silently ignore the remainder of the line (fgets itself
//!     does NOT stop at the NUL — the bytes are read, file position stays
//!     correct, the parse never desynchronizes; the NUL also defeats C's
//!     own line-too-long guard, tzparser.c:375 `strlen(tzbuf) ==
//!     sizeof(tzbuf) - 1`). The Rust port parses raw bytes to the newline.
//!     BIDIRECTIONAL — both directions reproduced in
//!     nul_probe::tzparser_interior_nul_split below. Non-surface rationale
//!     of record: admin-authored text files in $PGSHAREDIR/timezonesets/
//!     loaded via the timezone_abbreviations GUC; not SQL-reachable; no
//!     reachable input produces an interior NUL. The NUL carve STAYS.
//!     Witness-loss check (wparserfam_diff tparser_init law): no fixed
//!     tzparser defect is reachable only inside this carve — the lane
//!     shipped no tzparser code fix and its only product divergence is the
//!     ruled one, so the carve currently shadows no fix witness.
//!     Line count capped indirectly by input size.
//!   - rss: the Rust side leaks each exec's converted table by design
//!     (ConvertTimeZoneAbbrevs Box::leak, C guc_malloc counterpart is
//!     freed by the GUC machinery) — long campaigns run with a raised
//!     -rss_limit_mb.

#![allow(dead_code)]

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_long};
use std::sync::Once;

use localtime::PgTm;
use mcx::MemoryContext;

extern "C" {
    fn pg_tzf_strftime(
        s: *mut c_char,
        maxsize: usize,
        format: *const c_char,
        tm_sec: c_int,
        tm_min: c_int,
        tm_hour: c_int,
        tm_mday: c_int,
        tm_mon: c_int,
        tm_year: c_int,
        tm_wday: c_int,
        tm_yday: c_int,
        tm_isdst: c_int,
        tm_gmtoff: c_long,
        tm_zone: *const c_char,
    ) -> i64;
    fn pg_tzf_load_tzoffsets(filename: *const c_char) -> c_int;
    fn pg_tzf_abbrev(i: c_int, token_out: *mut c_char, type_out: *mut c_int, value_out: *mut c_int);
    fn pg_tzf_dynzone(value: c_int) -> *const c_char;
    fn pg_tzf_guc_msg() -> *const c_char;
    fn pg_tzf_guc_detail() -> *const c_char;
    fn pg_tzf_guc_hint() -> *const c_char;
    fn pg_tzf_reset();
    fn pg_tzf_t_isalpha(ptr: *const c_char) -> c_int;
    fn pg_tzf_t_isalnum(ptr: *const c_char) -> c_int;
    fn pg_tzf_t_iseq(x: *const c_char, c: c_char) -> c_int;
    fn pg_tzf_isspace_c(c: c_int) -> c_int;
    fn wfam_x_set_db_encoding(encoding: c_int);
}

const TZ: i32 = adt_datetime::consts::TZ;
const DTZ: i32 = adt_datetime::consts::DTZ;
const DYNTZ: i32 = adt_datetime::consts::DYNTZ;
const TOKMAXLEN: usize = adt_datetime::consts::TOKMAXLEN;

/// Byte-cursor over the fuzz payload; exhausted reads return zeros so every
/// input length is valid.
struct Rdr<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, pos: 0 }
    }
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    fn i16(&mut self) -> i16 {
        self.u16() as i16
    }
    fn u32(&mut self) -> u32 {
        let mut b = [0u8; 4];
        for s in &mut b {
            *s = self.u8();
        }
        u32::from_le_bytes(b)
    }
    fn rest(&self) -> &'a [u8] {
        &self.d[self.pos.min(self.d.len())..]
    }
}

// ---------------- shared fixture environment ----------------

fn share_dir() -> &'static str {
    static DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("tzfam-share-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("timezonesets")).unwrap();
        std::fs::create_dir_all(dir.join("tsearch_data")).unwrap();
        dir.to_str().unwrap().to_owned()
    })
}

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Both sides resolve share/ through this env var (tzsets_dir /
        // tsearch_data_dir on the Rust side, the get_share_path shim on
        // the C side). Must precede the first load_tzoffsets call: the
        // Rust dir is a OnceLock.
        std::env::set_var("PGRUST_PGSHAREDIR", share_dir());
        // Staged tsearch_data dir beside the fuzz binary: makes
        // staged_tsearch_data_dir() return Some and lets arm 3 drive the
        // staged-candidate hit (crate line coverage; harmless otherwise).
        if let Ok(exe) = std::env::current_exe() {
            if let Some(parent) = exe.parent() {
                let staged = parent.join("share/tsearch_data");
                let _ = std::fs::create_dir_all(&staged);
                let _ = std::fs::write(staged.join("stagedword.stop"), b"staged\n");
            }
        }
        // Pinned C default collation (jsonpath_diff precedent) for
        // lowerstr's str_tolower path.
        let _ = std::panic::catch_unwind(pg_locale::init_seams);
        pg_locale::set_default_locale_c_for_tests();
    });
}

fn pin_ctype_and_encoding() {
    // Thread-locals: pin on the executing thread, every exec (cheap).
    pg_locale::set_database_ctype_is_c(true);
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("PG_UTF8 valid");
    unsafe { wfam_x_set_db_encoding(wchar::PG_UTF8) };
}

// ---------------- arm 0: strftime ----------------

fn strftime_case(r: &mut Rdr<'_>) {
    let maxsize = (r.u16() % 300) as usize;
    let tm_sec = r.i16() as i32;
    let tm_min = r.i16() as i32;
    let tm_hour = r.i16() as i32;
    let tm_mday = r.i16() as i32;
    let tm_mon = r.i16() as i32;
    let tm_year = r.u32() as i32;
    let tm_wday = r.i16() as i32;
    let tm_yday = r.i16() as i32;
    let tm_isdst = r.u8() as i8 as i32;
    let tm_gmtoff = r.u32() as i32 as i64;

    let zflag = r.u8();
    let zone: Option<String> = if zflag & 1 != 0 {
        let n = ((zflag >> 1) % 9) as usize;
        let mut z = String::new();
        for _ in 0..n {
            // printable ASCII so the C side gets a clean cstring and the
            // Rust side a valid &str ('-' reachable for the %z sign rule)
            z.push((0x20 + (r.u8() % 0x5f)) as char);
        }
        Some(z)
    } else {
        None
    };

    // C cstring contract: format is NUL-free (domain carve, header)
    let format: Vec<u8> = r.rest().iter().copied().filter(|&b| b != 0).take(512).collect();

    let tm = PgTm {
        tm_sec,
        tm_min,
        tm_hour,
        tm_mday,
        tm_mon,
        tm_year,
        tm_wday,
        tm_yday,
        tm_isdst,
        tm_gmtoff,
        tm_zone: zone.as_deref(),
    };

    let mut rbuf = vec![0u8; maxsize];
    let r_ret = strftime::pg_strftime(&mut rbuf, &format, &tm);

    let mut cbuf = vec![0u8; maxsize + 1]; // +1 so as_mut_ptr is valid at maxsize 0
    let c_format = CString::new(format.clone()).unwrap();
    let c_zone = zone.as_ref().map(|z| CString::new(z.as_str()).unwrap());
    let c_ret = unsafe {
        pg_tzf_strftime(
            cbuf.as_mut_ptr().cast(),
            maxsize,
            c_format.as_ptr(),
            tm_sec,
            tm_min,
            tm_hour,
            tm_mday,
            tm_mon,
            tm_year,
            tm_wday,
            tm_yday,
            tm_isdst,
            tm_gmtoff,
            c_zone.as_ref().map_or(std::ptr::null(), |z| z.as_ptr()),
        )
    };

    match r_ret {
        Some(len) => {
            assert_eq!(c_ret, len as i64, "strftime verdict (fmt {:?})", fmtdbg(&format));
            assert_eq!(
                &rbuf[..=len],
                &cbuf[..=len],
                "strftime bytes (fmt {:?})",
                fmtdbg(&format)
            );
        }
        None => {
            assert_eq!(c_ret, -1, "strftime ERANGE verdict (fmt {:?})", fmtdbg(&format));
            assert_eq!(
                &rbuf[..maxsize],
                &cbuf[..maxsize],
                "strftime truncated bytes (fmt {:?})",
                fmtdbg(&format)
            );
        }
    }
}

fn fmtdbg(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

// ---------------- arm 1: tzparser ----------------

const TZ_FILES: [&str; 4] = ["aaa", "bbb", "ccc", "ddd"];

fn cstr_opt(p: *const c_char) -> Option<String> {
    if p.is_null() {
        None
    } else {
        Some(unsafe { std::ffi::CStr::from_ptr(p) }.to_string_lossy().into_owned())
    }
}

/// The two filesystem arms interpolate %m / io::Error text (and the C hint
/// interpolates my_exec_path): compare through the last `": "` only.
fn fs_arm_norm(m: &str) -> String {
    for pfx in ["could not open directory \"", "could not read time zone file \""] {
        if m.starts_with(pfx) {
            if let Some(i) = m.rfind("\": ") {
                return m[..i + 3].to_owned();
            }
        }
    }
    m.to_owned()
}

fn tzparser_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    let tzdir = format!("{}/timezonesets", share_dir());

    if flags & 1 != 0 {
        // Drive the could-not-open-directory arm on both sides.
        let _ = std::fs::remove_dir_all(&tzdir);
    } else {
        let _ = std::fs::create_dir_all(&tzdir);
        // NUL-free fixture bytes (domain carve, header); 0xFF splits into
        // up to 4 files for @INCLUDE chains.
        let payload: Vec<u8> = r.rest().iter().copied().filter(|&b| b != 0).collect();
        let mut chunks = payload.split(|&b| b == 0xFF);
        for name in TZ_FILES {
            let path = format!("{tzdir}/{name}");
            match chunks.next() {
                Some(c) => std::fs::write(&path, c).unwrap(),
                None => {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }

    // C side first (fills its own capture slots; independent of Rust state)
    unsafe { pg_tzf_reset() };
    let c_n = unsafe { pg_tzf_load_tzoffsets(c"aaa".as_ptr()) };

    guc::reset_guc_check_error();
    let r_tbl = tzparser::load_tzoffsets("aaa");
    let r_err = guc::take_guc_check_error();

    // Restore the dir for subsequent execs
    if flags & 1 != 0 {
        let _ = std::fs::create_dir_all(&tzdir);
    }

    match r_tbl {
        Some(tbl) => {
            assert!(c_n >= 0, "verdict: pgrust Some, C error msg={:?}", cstr_opt(unsafe { pg_tzf_guc_msg() }));
            assert_eq!(tbl.abbrevs.len(), c_n as usize, "abbrev count");
            for (i, tk) in tbl.abbrevs.iter().enumerate() {
                let mut ctok = [0u8; TOKMAXLEN + 1];
                let (mut ctyp, mut cval): (c_int, c_int) = (0, 0);
                unsafe {
                    pg_tzf_abbrev(i as c_int, ctok.as_mut_ptr().cast(), &mut ctyp, &mut cval)
                };
                // token is a NUL-terminated cstring field: C's strlcpy
                // leaves post-NUL bytes uninitialized in the guc_malloc
                // chunk (Rust zero-fills) — compare through the NUL.
                let rtok = &tk.token[..tk.token.iter().position(|&b| b == 0).unwrap() + 1];
                assert_eq!(rtok, &ctok[..rtok.len()], "token {i}");
                assert_eq!(tk.typ as i32, ctyp, "type {i}");
                if ctyp == DYNTZ {
                    let czone =
                        unsafe { std::ffi::CStr::from_ptr(pg_tzf_dynzone(cval)) }.to_bytes();
                    assert_eq!(tbl.dynamic_zone(tk.value), czone, "dyn zone {i}");
                } else {
                    assert_eq!(tk.value, cval, "offset value {i} (type {ctyp})");
                }
            }
        }
        None => {
            assert!(c_n < 0, "verdict: pgrust None (msg {:?}), C ok n={c_n}", r_err.message);
            let c_msg = cstr_opt(unsafe { pg_tzf_guc_msg() });
            let c_detail = cstr_opt(unsafe { pg_tzf_guc_detail() });
            let c_hint = cstr_opt(unsafe { pg_tzf_guc_hint() });
            match (&r_err.message, &c_msg) {
                (Some(rm), Some(cm)) => {
                    assert_eq!(fs_arm_norm(rm), fs_arm_norm(cm), "guc errmsg");
                }
                // level-0 silent failures (guc.c's own complaint suffices):
                // both sides must be silent together
                (None, None) => {}
                (rm, cm) => panic!("guc errmsg presence: pgrust {rm:?} vs C {cm:?}"),
            }
            assert_eq!(r_err.detail, c_detail, "guc errdetail");
            assert_eq!(r_err.hint.is_some(), c_hint.is_some(), "guc errhint presence");
        }
    }
}

// ---------------- arm 2: ts_locale predicates ----------------

fn ts_pred_case(r: &mut Rdr<'_>) {
    pin_ctype_and_encoding();
    let bytes: Vec<u8> = r.rest().iter().take(256).copied().collect();
    if bytes.is_empty() {
        return;
    }
    // C reads via unbounded cstring entries: NUL-pad the tail so mblen
    // walks stay in bounds (wcharfam `padded` precedent)
    let mut cbuf = bytes.clone();
    cbuf.extend_from_slice(&[0, 0, 0, 0]);

    for i in 0..bytes.len() {
        let s = &bytes[i..];
        let cp = unsafe { cbuf.as_ptr().add(i).cast::<c_char>() };
        // NUL byte at s[0] never happens (payload unfiltered!) — it can:
        // C's *_unbounded on a leading NUL is isalpha(0)=false with the
        // ctype pin; Rust classify sees clen 1. Both defined; compare.
        assert_eq!(
            ts_locale::t_isalpha(s),
            unsafe { pg_tzf_t_isalpha(cp) } != 0,
            "t_isalpha byte {i} ({:#x})",
            bytes[i]
        );
        assert_eq!(
            ts_locale::t_isalnum(s),
            unsafe { pg_tzf_t_isalnum(cp) } != 0,
            "t_isalnum byte {i} ({:#x})",
            bytes[i]
        );
        for probe in [bytes[i], b'x'] {
            assert_eq!(
                ts_locale::t_iseq(s, probe),
                unsafe { pg_tzf_t_iseq(cp, probe as c_char) } != 0,
                "t_iseq byte {i} probe {probe:#x}"
            );
        }
        assert_eq!(
            ts_locale::byte_isspace(bytes[i]),
            unsafe { pg_tzf_isspace_c(bytes[i] as c_int) } != 0,
            "byte_isspace {:#x}",
            bytes[i]
        );
    }

    // lowerstr postcondition (C default collation pinned): NUL-free ASCII
    // inputs downcase ASCII-only (str_tolower is cstring-semantics: a NUL
    // truncates, as C's palloc'd-string caller contract implies); other
    // inputs exercised for no-panic.
    let ctx = MemoryContext::new("tzfam-lowerstr");
    {
        let low = ts_locale::lowerstr(ctx.mcx(), &bytes);
        if let Ok(low) = &low {
            if bytes.is_ascii() && !bytes.contains(&0) {
                assert_eq!(low.as_slice(), bytes.to_ascii_lowercase(), "lowerstr ascii");
            }
        }
    }
}

// ---------------- arm 3: ts_locale file/stoplist faces ----------------

fn ts_file_case(r: &mut Rdr<'_>) {
    pin_ctype_and_encoding();
    let flags = r.u8();
    let share = share_dir();
    let ctx = MemoryContext::new("tzfam-tsfile");
    let mcx = ctx.mcx();

    // get_tsearch_config_filename: fuzz-chosen basename
    let name_len = (r.u8() % 13) as usize;
    let mut name = Vec::with_capacity(name_len);
    for _ in 0..name_len {
        name.push(r.u8());
    }
    let valid = name
        .iter()
        .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    match ts_locale::get_tsearch_config_filename(mcx, &name, "stop") {
        Ok(path) => {
            assert!(valid, "invalid name accepted: {name:?}");
            if name == b"stagedword" {
                // the one file staged beside the binary at init: the
                // staged dir wins over the share dir for it
                assert!(
                    path.as_slice().ends_with(b"share/tsearch_data/stagedword.stop")
                        && !path.as_slice().starts_with(share.as_bytes()),
                    "staged config path: {:?}",
                    String::from_utf8_lossy(&path)
                );
            } else {
                assert_eq!(
                    path.as_slice(),
                    format!("{share}/tsearch_data/{}.stop", String::from_utf8_lossy(&name))
                        .as_bytes(),
                    "config path"
                );
            }
        }
        Err(e) => {
            assert!(!valid, "valid name rejected: {name:?}");
            assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
        }
    }
    // staged-dir hit: the file staged at init resolves to the exe-side dir
    if flags & 4 != 0 {
        if let Ok(p) = ts_locale::get_tsearch_config_filename(mcx, b"stagedword", "stop") {
            assert!(
                p.as_slice().ends_with(b"share/tsearch_data/stagedword.stop"),
                "staged path: {:?}",
                String::from_utf8_lossy(&p)
            );
        }
    }

    // stop-word file faces over the payload bytes
    let payload = r.rest();
    let stop_path = format!("{share}/tsearch_data/fuzzstop.stop");
    std::fs::write(&stop_path, payload).unwrap();
    let lower = flags & 1 != 0;
    match ts_locale::readstoplist(mcx, Some(b"fuzzstop"), lower) {
        Ok(sl) => {
            for w in sl.stop.windows(2) {
                assert!(w[0].as_slice() <= w[1].as_slice(), "stoplist sorted");
            }
            for w in &sl.stop {
                assert!(ts_locale::searchstoplist(&sl, w), "own word found");
            }
            let probe: &[u8] = if flags & 2 != 0 { b"zz_probe" } else { b"a" };
            assert_eq!(
                ts_locale::searchstoplist(&sl, probe),
                sl.stop.iter().any(|w| w.as_slice() == probe),
                "probe membership == linear scan"
            );
        }
        Err(_) => {
            // encoding-verification failure from pg_any_to_server on
            // non-UTF-8 payloads: verdict-only (no C counterpart in-crate)
        }
    }
    // empty-name and missing-file arms
    let empty = ts_locale::readstoplist(mcx, None, lower).expect("None fname is empty list");
    assert!(empty.stop.is_empty());
    let missing = ts_locale::readstoplist(mcx, Some(b"nosuchstop"), false);
    assert!(missing.is_err(), "missing stop file must error");

    // tsearch_readlines reassembly on the same file
    if let Ok(Some(lines)) = ts_locale::tsearch_readlines(mcx, stop_path.as_bytes()) {
        if std::str::from_utf8(payload).is_ok() {
            let mut cat = Vec::new();
            for l in &lines {
                cat.extend_from_slice(l);
            }
            assert_eq!(cat, payload, "readlines reassembly");
        }
    }
    assert!(
        ts_locale::tsearch_readlines(mcx, b"/nonexistent/tzfam".as_slice())
            .expect("open failure is Ok(None)")
            .is_none()
    );
    // NOT driven: the cross-encoding pg_any_to_server conversion arm
    // (tsearch_readlines' Some passthrough) — a non-UTF8 database encoding
    // routes into the conversion-proc machinery, which requires installed
    // xact/catalog seams (server environment). excluded-state exception
    // row of record for public.rs:168.

    // dict_api faces
    let int_sel = flags >> 3;
    let int_value: Option<i64> = match int_sel % 4 {
        0 => None,
        1 => Some(0),
        2 => Some(1),
        _ => Some(int_sel as i64 + 2),
    };
    let value: Vec<u8> = payload.iter().take(8).copied().collect();
    match ts_locale::dict_api::def_get_boolean(b"flag", &value, int_value) {
        Ok(b) => match int_value {
            Some(0) => assert!(!b),
            Some(1) => assert!(b),
            Some(_) => panic!("int {int_value:?} must error"),
            None => {
                let expect = value.eq_ignore_ascii_case(b"true") || value.eq_ignore_ascii_case(b"on");
                let expect_false =
                    value.eq_ignore_ascii_case(b"false") || value.eq_ignore_ascii_case(b"off");
                assert!(if b { expect } else { expect_false }, "bool text {value:?}");
            }
        },
        Err(e) => {
            assert_eq!(e.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
            match int_value {
                None => {
                    assert!(
                        !(value.eq_ignore_ascii_case(b"true")
                            || value.eq_ignore_ascii_case(b"on")
                            || value.eq_ignore_ascii_case(b"false")
                            || value.eq_ignore_ascii_case(b"off")),
                        "recognized bool text errored: {value:?}"
                    );
                }
                Some(v) => assert!(v != 0 && v != 1),
            }
        }
    }
    assert!(unsafe { ts_locale::dict_api::lexize_result_ref(0) }.is_none());
    let lr = ts_locale::dict_api::LexizeResult(mcx::PgVec::new_in(mcx));
    let got = unsafe { ts_locale::dict_api::lexize_result_ref(&lr as *const _ as usize) };
    assert!(got.is_some_and(|g| g.0.is_empty()));
}

pub fn tzfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.is_empty() {
        return;
    }
    init_env();
    let mut r = Rdr::new(data);
    let sel = r.u8() % 4;
    match sel {
        0 => strftime_case(&mut r),
        1 => tzparser_case(&mut r),
        2 => ts_pred_case(&mut r),
        _ => ts_file_case(&mut r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EXHAUSTIVE-DIFF (a0): full single-byte domain for the ts_locale
    /// byte predicates under the pinned C-locale/ctype arm — every byte
    /// value as the lead byte, vs the verbatim C macros / C-locale isspace,
    /// plus the full 256x256 t_iseq product. Asserts in-test that the loop
    /// covered the whole domain.
    #[test]
    fn ts_locale_bytes_exhaustive() {
        let _serial = crate::c_oracle_serial();
        init_env();
        pin_ctype_and_encoding();
        let mut count = 0usize;
        for b in 0..=255u8 {
            let buf = [b, 0, 0, 0, 0];
            let s = &buf[..1];
            let cp = buf.as_ptr().cast::<c_char>();
            assert_eq!(
                ts_locale::t_isalpha(s),
                unsafe { pg_tzf_t_isalpha(cp) } != 0,
                "t_isalpha {b:#x}"
            );
            assert_eq!(
                ts_locale::t_isalnum(s),
                unsafe { pg_tzf_t_isalnum(cp) } != 0,
                "t_isalnum {b:#x}"
            );
            assert_eq!(
                ts_locale::byte_isspace(b),
                unsafe { pg_tzf_isspace_c(b as c_int) } != 0,
                "byte_isspace {b:#x}"
            );
            for c in 0..=255u8 {
                assert_eq!(
                    ts_locale::t_iseq(s, c),
                    unsafe { pg_tzf_t_iseq(cp, c as c_char) } != 0,
                    "t_iseq {b:#x} {c:#x}"
                );
                count += 1;
            }
        }
        assert_eq!(count, 256 * 256, "full domain swept");
    }

    /// Every conversion specifier byte through both sides on a fixed tm
    /// (seed-strength; the fuzz arm owns the domain).
    #[test]
    fn strftime_all_specs() {
        init_env();
        for spec in 0u8..=255 {
            if spec == 0 {
                continue;
            }
            let mut data = vec![0u8]; // selector 0
            data.extend_from_slice(&100u16.to_le_bytes()); // maxsize 100
            data.extend_from_slice(&30i16.to_le_bytes()); // sec
            data.extend_from_slice(&45i16.to_le_bytes()); // min
            data.extend_from_slice(&13i16.to_le_bytes()); // hour
            data.extend_from_slice(&15i16.to_le_bytes()); // mday
            data.extend_from_slice(&6i16.to_le_bytes()); // mon
            data.extend_from_slice(&126i32.to_le_bytes()); // year (2026)
            data.extend_from_slice(&3i16.to_le_bytes()); // wday
            data.extend_from_slice(&195i16.to_le_bytes()); // yday
            data.push(1); // isdst
            data.extend_from_slice(&(-25200i32).to_le_bytes()); // gmtoff
            data.push(0); // no zone
            data.push(b'%');
            data.push(spec);
            tzfam_diff(&data);
        }
    }

    /// tzparser smoke through the fixture plumbing: a real Default-style
    /// file with fixed + dynamic + DST entries, @OVERRIDE and @INCLUDE.
    #[test]
    fn tzparser_basic() {
        init_env();
        let mut data = vec![1u8, 0u8]; // selector 1, flags 0
        data.extend_from_slice(
            b"# comment\nacst 34200 D\nacdt Australia/Adelaide # dyn\n@INCLUDE bbb\n\xFF\
              @OVERRIDE\nacst 34201 D\nest -18000\n",
        );
        tzfam_diff(&data);
    }
}

#[cfg(test)]
mod nul_probe {
    use super::*;

    /// Documentation probe (not a gate): C fgets/strlen machinery truncates
    /// a tz-file line at an interior NUL; the Rust port tokenizes raw bytes.
    /// The driver carves NULs out of arm-1 fixtures; this reproduces the
    /// ruled behavior split in BOTH directions.
    ///
    /// RULED BUG-FOR-BUG / ratified NON-SURFACE (Michael 2026-08-01,
    /// against the lane's MATCH recommendation): pgrust keeps its raw-byte
    /// parse; do NOT make it match C's truncation. Ledger of record =
    /// docs/verification/phase1-claims.tsv row backend/utils/misc/tzparser;
    /// full mechanism note in this file's DOMAIN CARVES header.
    ///
    ///   - `ab\0cd ZONEX\n`: C truncates to "ab" (abbrev with no offset,
    ///     "missing time zone offset" check-error, load_tzoffsets = -1);
    ///     pgrust SUCCEEDS with a DYNTZ entry whose abbrev embeds the NUL.
    ///   - `aaa 3600\0junk\n`: C truncates to a valid "aaa 3600" (1 abbrev);
    ///     pgrust FAILS ("invalid number for time zone offset").
    fn nul_case(fixture: &[u8]) -> (c_int, Option<String>, bool, Option<String>) {
        let tzdir = format!("{}/timezonesets", share_dir());
        std::fs::create_dir_all(&tzdir).unwrap();
        std::fs::write(format!("{tzdir}/aaa"), fixture).unwrap();
        unsafe { pg_tzf_reset() };
        let c_n = unsafe { pg_tzf_load_tzoffsets(c"aaa".as_ptr()) };
        let c_msg = cstr_opt(unsafe { pg_tzf_guc_msg() });
        guc::reset_guc_check_error();
        let r_tbl = tzparser::load_tzoffsets("aaa");
        let r_err = guc::take_guc_check_error();
        (c_n, c_msg, r_tbl.is_some(), r_err.message)
    }

    #[test]
    #[ignore]
    fn tzparser_interior_nul_split() {
        let _serial = crate::c_oracle_serial();
        init_env();

        // Direction 1: C FAILS, pgrust SUCCEEDS.
        let (c_n, c_msg, r_ok, r_msg) = nul_case(b"ab\0cd ZONEX\n");
        eprintln!("dir1 C: n={c_n} msg={c_msg:?}");
        eprintln!("dir1 R: ok={r_ok} msg={r_msg:?}");
        assert_eq!(c_n, -1);
        assert!(r_ok);

        // Direction 2: C SUCCEEDS, pgrust FAILS.
        let (c_n, c_msg, r_ok, r_msg) = nul_case(b"aaa 3600\0junk\n");
        eprintln!("dir2 C: n={c_n} msg={c_msg:?}");
        eprintln!("dir2 R: ok={r_ok} msg={r_msg:?}");
        assert_eq!(c_n, 1);
        assert!(!r_ok);
    }
}
