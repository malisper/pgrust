//! Tests for the `timeline.c` port.
//!
//! The pure parsing helpers run without any seam. The read/exists probes run
//! through the `backend-storage-file-fd` seams, backed by an in-memory map. The
//! *write* path (`writeTimeLineHistory`/`writeTimeLineHistoryFile`) keeps its
//! temp-file/fsync/durable_rename orchestration in-crate and performs real
//! syscalls (`open`/`read`/`write`/`unlink`/`rename`); to exercise it without a
//! ported `fd.c`, the individual fd primitives are stubbed to plain libc and the
//! tests run inside a private temp directory containing a `pg_wal` subdir. The
//! seam slots are process-global `OnceLock`s, installed once via `Once`; each
//! test resets the backing state and chdir's into its own scratch dir.

use super::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::CString;
use std::sync::{Mutex, Once};

use ::mcx::MemoryContext;
use ::types_error::FATAL;

thread_local! {
    static FILES: RefCell<BTreeMap<String, Vec<u8>>> = const { RefCell::new(BTreeMap::new()) };
    // histfnames passed to xlog_archive_notify.
    static ARCHIVED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

static INSTALL: Once = Once::new();
// Serializes the write tests, which chdir into a shared scratch directory.
static WRITE_LOCK: Mutex<()> = Mutex::new(());

/// Install the in-memory seam providers once; reset the backing state.
fn setup() {
    INSTALL.call_once(|| {
        // No archive in these tests: RestoreArchivedFile never finds anything.
        xlogarchive::restore_archived_history_file::set(|_mcx, _xlogfname| Ok(None));
        xlogarchive::keep_file_restored_from_archive::set(|_path, _xlogfname| Ok(()));
        xlogarchive::xlog_archive_notify::set(|xlog| {
            ARCHIVED.with(|c| c.borrow_mut().push(xlog));
            Ok(())
        });

        fd::read_file_or_absent::set(|mcx, path| {
            FILES.with(|c| match c.borrow().get(path) {
                Some(bytes) => Ok(Some(::mcx::slice_in(mcx, bytes)?)),
                None => Ok(None),
            })
        });
        fd::file_exists::set(|path| Ok(FILES.with(|c| c.borrow().contains_key(path))));

        // fd.c primitives backing the in-crate write orchestration: plain libc
        // against the (chdir'd) scratch directory.
        file::open_transient_file::set(|path, flags| {
            let c = CString::new(path).unwrap();
            Ok(unsafe { libc::open(c.as_ptr(), flags, 0o600) })
        });
        file::close_transient_file::set(|fd| unsafe { libc::close(fd) });
        file::pg_fsync::set(|fd| unsafe { libc::fsync(fd) });
        file::data_sync_elevel::set(|elevel| elevel);
        file::durable_rename::set(|oldfile, newfile, _elevel| {
            let o = CString::new(oldfile).unwrap();
            let n = CString::new(newfile).unwrap();
            if unsafe { libc::rename(o.as_ptr(), n.as_ptr()) } != 0 {
                return Err(ereport(ERROR)
                    .with_saved_errno(current_errno())
                    .errcode_for_file_access()
                    .errmsg(format!("could not rename file \"{oldfile}\" to \"{newfile}\": %m"))
                    .into_error()
                    .with_error_location(here()));
            }
            Ok(())
        });

        waitevent::pgstat_report_wait_start::set(|_| {});
        waitevent::pgstat_report_wait_end::set(|| {});
    });

    FILES.with(|c| c.borrow_mut().clear());
    ARCHIVED.with(|c| c.borrow_mut().clear());
}

/// Enter a fresh scratch dir (with a `pg_wal` subdir) for a write test; the
/// returned guard restores the original cwd on drop and holds `WRITE_LOCK`.
fn write_scratch() -> impl Drop {
    struct Guard {
        _lock: std::sync::MutexGuard<'static, ()>,
        orig: std::path::PathBuf,
        dir: std::path::PathBuf,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.orig);
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    let lock = WRITE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let orig = std::env::current_dir().unwrap();
    let dir = std::env::temp_dir().join(format!("tl_write_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("pg_wal")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    Guard { _lock: lock, orig, dir }
}

fn put_file(path: &str, bytes: &[u8]) {
    FILES.with(|c| {
        c.borrow_mut().insert(path.to_string(), bytes.to_vec());
    });
}

/// Read a file relative to the current (scratch) directory.
fn read_scratch(path: &str) -> Option<Vec<u8>> {
    std::fs::read(path).ok()
}

/// Seed a real file in the scratch dir (parent-history input for the write path).
fn put_scratch(path: &str, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
}

// ---------------------------------------------------------------------------
// Pure helpers (no seam).
// ---------------------------------------------------------------------------

#[test]
fn invalid_xlogrecptr_is_zero() {
    assert!(XLogRecPtrIsInvalid(InvalidXLogRecPtr));
    assert!(!XLogRecPtrIsInvalid(1));
}

#[test]
fn tl_history_file_name_format() {
    assert_eq!(TLHistoryFileName(5), "00000005.history");
    assert_eq!(TLHistoryFileName(0xABCDEF12), "ABCDEF12.history");
}

#[test]
fn tl_history_file_path_format() {
    assert_eq!(TLHistoryFilePath(5), "pg_wal/00000005.history");
}

#[test]
fn sscanf_parses_full_line() {
    let (n, tli, hi, lo) = sscanf_history_line("2\t0/2000000\tsome reason\n");
    assert_eq!((n, tli, hi, lo), (3, 2, 0, 0x2000000));
}

#[test]
fn sscanf_tab_matches_any_whitespace() {
    let (n, tli, hi, lo) = sscanf_history_line("1ABC/DEF");
    assert_eq!((n, tli, hi, lo), (3, 1, 0xABC, 0xDEF));
}

#[test]
fn sscanf_missing_timeline_id() {
    let (n, ..) = sscanf_history_line("abc\n");
    assert_eq!(n, 0);
}

#[test]
fn sscanf_missing_switchpoint() {
    let (n, tli, ..) = sscanf_history_line("1\n");
    assert_eq!((n, tli), (1, 1));
}

#[test]
fn sscanf_missing_low_half() {
    let (n, tli, hi, _lo) = sscanf_history_line("1\t0/\n");
    assert_eq!((n, tli, hi), (2, 1, 0));
}

#[test]
fn comment_test_skips_hash_and_blank() {
    let lines = history_file_lines(b" \t\n# comment\n\t1 0/2\n");
    assert_eq!(lines.len(), 3);
    assert!(lines[0].chars().all(is_c_space));
    assert_eq!(lines[1].chars().find(|c| !is_c_space(*c)), Some('#'));
}

// ---------------------------------------------------------------------------
// readTimeLineHistory
// ---------------------------------------------------------------------------

#[test]
fn read_timeline1_has_no_history_file() {
    setup();
    let ctx = MemoryContext::new("test");
    let entries = readTimeLineHistory(ctx.mcx(), 1, false).unwrap();
    assert_eq!(entries.as_slice(), &[TimeLineHistoryEntry { tli: 1, begin: 0, end: 0 }]);
}

#[test]
fn read_missing_history_assumes_no_parents() {
    setup();
    let ctx = MemoryContext::new("test");
    let entries = readTimeLineHistory(ctx.mcx(), 3, false).unwrap();
    assert_eq!(entries.as_slice(), &[TimeLineHistoryEntry { tli: 3, begin: 0, end: 0 }]);
}

#[test]
fn read_history_newest_first_with_tip() {
    setup();
    put_file(
        "pg_wal/00000003.history",
        b"1\t0/16B6C50\tno reason\n2\t0/2000000\tno reason\n",
    );
    let ctx = MemoryContext::new("test");
    let entries = readTimeLineHistory(ctx.mcx(), 3, false).unwrap();
    assert_eq!(
        entries.as_slice(),
        &[
            TimeLineHistoryEntry { tli: 3, begin: 0x2000000, end: 0 },
            TimeLineHistoryEntry { tli: 2, begin: 0x16B6C50, end: 0x2000000 },
            TimeLineHistoryEntry { tli: 1, begin: 0, end: 0x16B6C50 },
        ]
    );
}

#[test]
fn read_history_rejects_non_increasing() {
    setup();
    put_file("pg_wal/00000003.history", b"2\t0/1\n2\t0/2\n");
    let ctx = MemoryContext::new("test");
    let err = readTimeLineHistory(ctx.mcx(), 3, false).unwrap_err();
    assert!(err.message().contains("invalid data in history file"));
    assert_eq!(err.level(), FATAL);
}

#[test]
fn read_history_rejects_target_not_after() {
    setup();
    put_file("pg_wal/00000002.history", b"2\t0/1\n");
    let ctx = MemoryContext::new("test");
    let err = readTimeLineHistory(ctx.mcx(), 2, false).unwrap_err();
    assert!(err
        .message()
        .contains("invalid data in history file \"pg_wal/00000002.history\""));
}

#[test]
fn read_history_syntax_error_numeric_id() {
    setup();
    put_file("pg_wal/00000002.history", b"abc\n");
    let ctx = MemoryContext::new("test");
    let err = readTimeLineHistory(ctx.mcx(), 2, false).unwrap_err();
    assert!(err.message().contains("syntax error in history file"));
    assert_eq!(err.level(), FATAL);
}

#[test]
fn read_history_ignores_comments_and_blanks() {
    setup();
    put_file(
        "pg_wal/00000002.history",
        b"# a comment\n\n   \n1\t0/2000000\treason\n",
    );
    let ctx = MemoryContext::new("test");
    let entries = readTimeLineHistory(ctx.mcx(), 2, false).unwrap();
    assert_eq!(
        entries.as_slice(),
        &[
            TimeLineHistoryEntry { tli: 2, begin: 0x2000000, end: 0 },
            TimeLineHistoryEntry { tli: 1, begin: 0, end: 0x2000000 },
        ]
    );
}

// ---------------------------------------------------------------------------
// writeTimeLineHistory / writeTimeLineHistoryFile
// ---------------------------------------------------------------------------

#[test]
fn write_history_appends_line_and_copies_parent() {
    setup();
    let _g = write_scratch();
    put_scratch("pg_wal/00000001.history", b"# header\n");
    let ctx = MemoryContext::new("test");
    writeTimeLineHistory(2, 1, 0x2000000, "no recovery target specified", false, false, ctx.mcx())
        .unwrap();
    let written = read_scratch("pg_wal/00000002.history").unwrap();
    let text = String::from_utf8(written).unwrap();
    assert_eq!(text, "# header\n\n1\t0/2000000\tno recovery target specified\n");
}

#[test]
fn write_history_no_parent_omits_leading_newline() {
    setup();
    let _g = write_scratch();
    let ctx = MemoryContext::new("test");
    writeTimeLineHistory(2, 1, 0x16B6C50, "reason", false, false, ctx.mcx()).unwrap();
    let text = String::from_utf8(read_scratch("pg_wal/00000002.history").unwrap()).unwrap();
    assert_eq!(text, "1\t0/16B6C50\treason\n");
}

#[test]
fn write_history_archives_when_active() {
    setup();
    let _g = write_scratch();
    let ctx = MemoryContext::new("test");
    writeTimeLineHistory(2, 1, 0x16B6C50, "reason", false, true, ctx.mcx()).unwrap();
    ARCHIVED.with(|c| assert_eq!(c.borrow().as_slice(), ["00000002.history"]));
}

#[test]
fn write_history_file_replaces() {
    setup();
    let _g = write_scratch();
    // A pre-existing destination must be replaced.
    put_scratch("pg_wal/00000004.history", b"stale");
    writeTimeLineHistoryFile(4, b"contents").unwrap();
    assert_eq!(read_scratch("pg_wal/00000004.history").unwrap(), b"contents");
}

// ---------------------------------------------------------------------------
// findNewestTimeLine / existsTimeLineHistory
// ---------------------------------------------------------------------------

#[test]
fn exists_timeline1_is_false() {
    setup();
    let ctx = MemoryContext::new("test");
    assert!(!existsTimeLineHistory(1, false, ctx.mcx()).unwrap());
}

#[test]
fn find_newest_timeline_probes() {
    setup();
    put_file("pg_wal/00000002.history", b"x");
    put_file("pg_wal/00000003.history", b"x");
    let ctx = MemoryContext::new("test");
    assert_eq!(findNewestTimeLine(1, false, ctx.mcx()).unwrap(), 3);
}

#[test]
fn find_newest_timeline_no_successors() {
    setup();
    let ctx = MemoryContext::new("test");
    assert_eq!(findNewestTimeLine(5, false, ctx.mcx()).unwrap(), 5);
}

// ---------------------------------------------------------------------------
// Pure in-memory lookups (no seam needed).
// ---------------------------------------------------------------------------

#[test]
fn tli_in_history_lookup() {
    let history = [
        TimeLineHistoryEntry { tli: 2, begin: 0, end: 0 },
        TimeLineHistoryEntry { tli: 1, begin: 0, end: 0 },
    ];
    assert!(tliInHistory(1, &history));
    assert!(!tliInHistory(3, &history));
}

#[test]
fn tli_of_point_in_history() {
    let history = [
        TimeLineHistoryEntry { tli: 2, begin: 0x100, end: InvalidXLogRecPtr },
        TimeLineHistoryEntry { tli: 1, begin: InvalidXLogRecPtr, end: 0x100 },
    ];
    assert_eq!(tliOfPointInHistory(0x50, &history).unwrap(), 1);
    assert_eq!(tliOfPointInHistory(0x100, &history).unwrap(), 2);
}

#[test]
fn tli_of_point_not_contiguous_errors() {
    let history = [TimeLineHistoryEntry { tli: 1, begin: 0x10, end: 0x20 }];
    let err = tliOfPointInHistory(0x5, &history).unwrap_err();
    assert!(err.message().contains("timeline history was not contiguous"));
}

#[test]
fn tli_switch_point_returns_end_and_next() {
    let history = [
        TimeLineHistoryEntry { tli: 3, begin: 0x200, end: InvalidXLogRecPtr },
        TimeLineHistoryEntry { tli: 2, begin: 0x100, end: 0x200 },
        TimeLineHistoryEntry { tli: 1, begin: 0, end: 0x100 },
    ];
    assert_eq!(tliSwitchPoint(2, &history).unwrap(), (0x200, 3));
    // The first entry (tli=3) matches before nextTLI is updated, so nextTLI
    // stays 0 -- exactly as the C function does.
    assert_eq!(tliSwitchPoint(3, &history).unwrap(), (InvalidXLogRecPtr, 0));
}

#[test]
fn tli_switch_point_unknown_errors() {
    let history = [TimeLineHistoryEntry { tli: 1, begin: 0, end: InvalidXLogRecPtr }];
    let err = tliSwitchPoint(9, &history).unwrap_err();
    assert!(err
        .message()
        .contains("requested timeline 9 is not in this server's history"));
}
