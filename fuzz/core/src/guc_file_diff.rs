//! guc_file_diff: differential fuzz driver — shipped Rust `guc_file` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_guc_file_io.c + the flex-generated verbatim scanner
//! csrc/gucfile/guc-file.c). Crate under test:
//! crates/backend/utils/misc/guc_file — the postgresql.conf parser.
//! Lane p1-wavef, 2026-08-01.
//!
//! ONE ARM (whole-parse family): the entire input language of
//! ParseConfigFp is one grammar, so the target is a single entry driven at
//! the shipped `ParseConfigFp(bytes, "conf", 0, elevel, &mut vars)` and the
//! C `gucf_ParseConfigFp(fmemopen(bytes), "conf", 0, elevel, &head, &tail)`.
//! DeescapeQuotedString, the Lexer, all match_* helpers, logical_lines and
//! parse_line are exercised through it on every quoted/unquoted/numeric
//! token (they have no other callers in the carve).
//!
//! Input layout: [selector][config-file bytes]; selector % 3 picks elevel:
//!   0 -> LOG    (record errors into the list, keep parsing)
//!   1 -> ERROR  (first ereport throws: PgResult::Err vs C longjmp)
//!   2 -> DEBUG1 (abandon file after the first syntax error)
//!
//! DOMAIN RESTRICTION (census carve of record): any payload containing the
//! case-insensitive byte substring "include" is skipped before either side
//! runs. Every parsed include/include_dir/include_if_exists directive name
//! necessarily contains those bytes (guc_name_compare folds ASCII case
//! only), so the filter soundly over-approximates the excluded
//! include-directive plane (ParseConfigFile / ParseConfigDirectory /
//! file IO). The C oracle backs this with abort() stubs — a breach is a
//! loud crash, never a silent divergence.
//!
//! Comparison planes (all per exec):
//!   1. verdict     — Ok(bool) vs C return path (returned/longjmp) and the
//!                    returned OK flag.
//!   2. errcode     — thrown PgError.sqlstate (packed i32) == C captured
//!                    packed sqlstate (identical MAKE_SQLSTATE packing),
//!                    plus thrown level.
//!   3. message     — thrown PgError.message == C's formatted errmsg
//!                    (UTF-8-lossy; carries file/line/token identity).
//!   4. parsed list — item count and per-item name / value / errmsg /
//!                    filename / sourceline / ignore / applied.
//!   5. log channel — count of sub-ERROR ereports C emitted == count of
//!                    error records Rust appended for non-throw paths
//!                    (they pair 1:1 in every guc-file.l arm reachable in
//!                    the include-free domain: syntax errors and the
//!                    too-many-errors abandon report, the latter checked
//!                    via gucf_logged counters).
//!
//! RATIFIED REPRESENTATION CARVE (documented, not a behavior claim): the
//! shipped crate stores token text as String via String::from_utf8_lossy
//! (the scanner is %option 8bit; high-bit bytes are LETTERs). The C oracle
//! keeps raw bytes. All string planes are therefore compared modulo
//! String::from_utf8_lossy applied to the C bytes. Distinct invalid-UTF-8
//! C outputs that lossy-fold together could in principle mask a high-bit
//! divergence; parse structure (token boundaries, sourceline, verdicts)
//! stays fully compared for those inputs.
//!
//! NO fc-wrapper plane: guc_file has no builtins.rs / pg_proc oids — these
//! are non-SQL config-parsing entry points (ledger oid `-`).

#![allow(non_snake_case)]

use std::ffi::CStr;
use std::os::raw::c_char;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, Once};

use types_error::{DEBUG1, ERROR, LOG};

extern "C" {
    fn pg_gucf_run(buf: *const u8, len: usize, elevel: i32) -> i32;
    fn pg_gucf_ok() -> i32;
    fn pg_gucf_item_count() -> usize;
    fn pg_gucf_item_name(i: usize) -> *const c_char;
    fn pg_gucf_item_value(i: usize) -> *const c_char;
    fn pg_gucf_item_errmsg(i: usize) -> *const c_char;
    fn pg_gucf_item_filename(i: usize) -> *const c_char;
    fn pg_gucf_item_sourceline(i: usize) -> i32;
    fn pg_gucf_item_ignore(i: usize) -> i32;
    fn pg_gucf_item_applied(i: usize) -> i32;
    fn pg_gucf_thrown_get_code() -> i32;
    fn pg_gucf_thrown_get_elevel() -> i32;
    fn pg_gucf_thrown_get_msg() -> *const c_char;
    fn pg_gucf_logged_get_count() -> i32;
}

/// C `char*` -> Option<String> through the ratified lossy carve.
fn c_str_lossy(p: *const c_char) -> Option<String> {
    if p.is_null() {
        None
    } else {
        // SAFETY: oracle accessors return NUL-terminated arena strings that
        // stay live until the next pg_gucf_run.
        Some(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
    }
}

/// The include-directive domain filter (see module header).
fn contains_include(payload: &[u8]) -> bool {
    payload
        .windows(7)
        .any(|w| w.eq_ignore_ascii_case(b"include"))
}

static INIT: Once = Once::new();

/// The verbatim flex scanner owns plain (non-thread-local) statics
/// (ConfigFileLineno, GUC_flex_fatal_jmp) and must stay byte-verbatim, so
/// the whole oracle call + accessor read-out is one critical section.
/// libFuzzer is single-threaded; this only matters for `cargo test`, which
/// runs targets in parallel threads (it corrupted results before the lock).
static ORACLE: Mutex<()> = Mutex::new(());

fn lock_oracle() -> MutexGuard<'static, ()> {
    ORACLE.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn guc_file_diff(data: &[u8]) {
    INIT.call_once(|| {
        // Silence the Rust server-log emission path (fuzz-process hygiene
        // only: recording into the ConfigVariable list — the compared
        // plane — is unconditional in record_or_throw).
        elog::config::set_log_min_messages(types_error::PANIC);
    });

    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let elevel = match sel % 3 {
        0 => LOG,
        1 => ERROR,
        _ => DEBUG1,
    };

    if contains_include(payload) {
        return; // census domain restriction; see module header
    }

    // ---- C oracle ---- (held until the last accessor read below)
    let _oracle = lock_oracle();
    let c_thrown = unsafe { pg_gucf_run(payload.as_ptr(), payload.len(), elevel.0) } != 0;

    // ---- shipped Rust ----
    let mut vars: Vec<guc_file::ConfigVariable> = Vec::new();
    let rust = guc_file::ParseConfigFp(payload, Path::new("conf"), 0, elevel, &mut vars);

    // ---- plane 1: verdict ----
    match &rust {
        Ok(ok) => {
            assert!(
                !c_thrown,
                "verdict: C threw (code {:08x}) but Rust returned Ok({ok})",
                unsafe { pg_gucf_thrown_get_code() },
            );
            let c_ok = unsafe { pg_gucf_ok() } != 0;
            assert_eq!(c_ok, *ok, "OK flag diverged (elevel {})", elevel.0);
        }
        Err(e) => {
            assert!(
                c_thrown,
                "verdict: Rust threw ({:?} {}) but C returned",
                e.sqlstate, e.message
            );
            // ---- plane 2: errcode + level ----
            assert_eq!(
                e.sqlstate.0,
                unsafe { pg_gucf_thrown_get_code() },
                "thrown sqlstate diverged"
            );
            assert_eq!(
                e.level.0,
                unsafe { pg_gucf_thrown_get_elevel() },
                "thrown level diverged"
            );
            // ---- plane 3: message identity ----
            let c_msg = c_str_lossy(unsafe { pg_gucf_thrown_get_msg() }).unwrap_or_default();
            assert_eq!(e.message, c_msg, "thrown message diverged");
        }
    }

    // ---- plane 4: parsed list (also compared for the thrown case: both
    // sides keep the items appended before the throw) ----
    let c_count = unsafe { pg_gucf_item_count() };
    assert_eq!(
        c_count,
        vars.len(),
        "item count diverged (elevel {}, c_thrown {c_thrown})",
        elevel.0
    );
    for (i, item) in vars.iter().enumerate() {
        assert_eq!(
            c_str_lossy(unsafe { pg_gucf_item_name(i) }),
            item.name,
            "item {i} name"
        );
        assert_eq!(
            c_str_lossy(unsafe { pg_gucf_item_value(i) }),
            item.value,
            "item {i} value"
        );
        assert_eq!(
            c_str_lossy(unsafe { pg_gucf_item_errmsg(i) }),
            item.errmsg,
            "item {i} errmsg"
        );
        let c_file = c_str_lossy(unsafe { pg_gucf_item_filename(i) });
        let r_file = item
            .filename
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned());
        assert_eq!(c_file, r_file, "item {i} filename");
        assert_eq!(
            unsafe { pg_gucf_item_sourceline(i) },
            item.sourceline,
            "item {i} sourceline"
        );
        assert_eq!(
            unsafe { pg_gucf_item_ignore(i) } != 0,
            item.ignore,
            "item {i} ignore"
        );
        assert_eq!(
            unsafe { pg_gucf_item_applied(i) } != 0,
            item.applied,
            "item {i} applied"
        );
    }

    // ---- plane 5: sub-ERROR log channel pairing ----
    if !c_thrown {
        let c_logged = unsafe { pg_gucf_logged_get_count() };
        let r_error_records = vars.iter().filter(|v| v.errmsg.is_some()).count() as i32;
        // Every recorded list error pairs with exactly one C log report;
        // the too-many-errors abandon report logs WITHOUT a list record,
        // as does nothing else in the include-free domain.
        assert!(
            c_logged == r_error_records || c_logged == r_error_records + 1,
            "log-channel pairing broke: C logged {c_logged}, Rust recorded {r_error_records}"
        );
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/guc_file_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/guc_file_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                guc_file_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Ok + error + throw shapes, one per elevel arm; plus the boundary
    /// cases named by the campaign brief (truncated line, embedded NUL,
    /// absurd token length, comment/quote interplay, empty input).
    #[test]
    fn arms_smoke() {
        // empty payload (synthesized C fixed point) + newline-only (real path)
        guc_file_diff(&[0u8]);
        guc_file_diff(b"\x00\n");
        guc_file_diff(b"\x00# comment only\n");
        // simple settings, all three elevels
        for sel in [0u8, 1, 2] {
            guc_file_diff(&[&[sel][..], b"work_mem = '64MB'\n"].concat());
            guc_file_diff(&[&[sel][..], b"shared_buffers 128MB\nport=5432\n"].concat());
            // syntax errors: bare value, doubled equals, trailing junk
            guc_file_diff(&[&[sel][..], b"= 42\n"].concat());
            guc_file_diff(&[&[sel][..], b"a == b\n"].concat());
            guc_file_diff(&[&[sel][..], b"a = 1 extra\n"].concat());
            // near end of line
            guc_file_diff(&[&[sel][..], b"name =\n"].concat());
            guc_file_diff(&[&[sel][..], b"name"].concat());
        }
        // quoting and escapes through DeescapeQuotedString
        guc_file_diff(b"\x00s = 'it''s'\n");
        guc_file_diff(b"\x00s = 'a\\n\\t\\101\\7\\''\n");
        guc_file_diff(b"\x00s = ''\n");
        // unterminated quote, embedded NUL, high-bit letters, no trailing \n
        guc_file_diff(b"\x00s = 'oops\n");
        guc_file_diff(b"\x00a\x00b = 1\n");
        guc_file_diff(b"\x00\xc3\xa9 = \xff\n");
        guc_file_diff(b"\x00last = 1");
        // qualified ids, numbers, reals, units
        guc_file_diff(b"\x00ext.opt = -0x1fF\n");
        guc_file_diff(b"\x00r = -1.5e+3\n");
        guc_file_diff(b"\x00r = .5\n");
        guc_file_diff(b"\x00t = 100ms\n");
        // absurd token length (spill the flex read buffer boundary)
        let mut long = b"\x00x = '".to_vec();
        long.extend(std::iter::repeat(b'y').take(20000));
        long.extend(b"'\n");
        guc_file_diff(&long);
        // 100-syntax-error abandonment (LOG) and first-error abandonment (DEBUG1)
        let many = b"?\n".repeat(120);
        guc_file_diff(&[&[0u8][..], &many].concat());
        guc_file_diff(&[&[2u8][..], &many].concat());
        guc_file_diff(&[&[1u8][..], &many].concat());
        // include filter: must be skipped, not compared (stubs abort on breach)
        guc_file_diff(b"\x00include 'other.conf'\n");
        guc_file_diff(b"\x00InClUdE_dir 'd'\n");
    }
}
