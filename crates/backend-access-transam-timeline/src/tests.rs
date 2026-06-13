//! Tests for the `timeline.c` port.
//!
//! The pure parsing helpers run without any seam. The file-touching paths run
//! through the `backend-storage-file-fd` / `backend-access-transam-xlogarchive`
//! seams; these tests install in-memory providers (backed by thread-local
//! state) exactly once. Because the seam slots are process-global `OnceLock`s,
//! the providers are installed by a single `std::sync::Once` and every test
//! that touches files clears and seeds the backing state before running.

use super::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Once;

use mcx::MemoryContext;
use types_error::FATAL;

thread_local! {
    static FILES: RefCell<BTreeMap<String, Vec<u8>>> = const { RefCell::new(BTreeMap::new()) };
    // (final_path, content, replace_existing) tuples recorded by durable_write_file.
    static WRITTEN: RefCell<Vec<(String, Vec<u8>, bool)>> = const { RefCell::new(Vec::new()) };
    // histfnames passed to xlog_archive_notify.
    static ARCHIVED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

static INSTALL: Once = Once::new();

/// Install the in-memory seam providers once; reset the backing state.
fn setup() {
    INSTALL.call_once(|| {
        // No archive in these tests: RestoreArchivedFile never finds anything.
        xlogarchive::restore_archived_history_file::set(|_mcx, _xlogfname| Ok(None));
        xlogarchive::keep_file_restored_from_archive::set(|_path, _xlogfname| Ok(()));
        xlogarchive::xlog_archive_notify::set(|xlog| {
            ARCHIVED.with(|c| c.borrow_mut().push(xlog.to_string()));
            Ok(())
        });

        fd::read_file_or_absent::set(|mcx, path| {
            FILES.with(|c| match c.borrow().get(path) {
                Some(bytes) => Ok(Some(mcx::slice_in(mcx, bytes)?)),
                None => Ok(None),
            })
        });
        fd::file_exists::set(|path| Ok(FILES.with(|c| c.borrow().contains_key(path))));
        fd::durable_write_file::set(|final_path, content, replace_existing| {
            FILES.with(|c| {
                c.borrow_mut().insert(final_path.to_string(), content.to_vec());
            });
            WRITTEN.with(|c| {
                c.borrow_mut()
                    .push((final_path.to_string(), content.to_vec(), replace_existing))
            });
            Ok(())
        });
    });

    FILES.with(|c| c.borrow_mut().clear());
    WRITTEN.with(|c| c.borrow_mut().clear());
    ARCHIVED.with(|c| c.borrow_mut().clear());
}

fn put_file(path: &str, bytes: &[u8]) {
    FILES.with(|c| {
        c.borrow_mut().insert(path.to_string(), bytes.to_vec());
    });
}

fn get_file(path: &str) -> Option<Vec<u8>> {
    FILES.with(|c| c.borrow().get(path).cloned())
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
    put_file("pg_wal/00000001.history", b"# header\n");
    let ctx = MemoryContext::new("test");
    writeTimeLineHistory(2, 1, 0x2000000, "no recovery target specified", false, false, ctx.mcx())
        .unwrap();
    let written = get_file("pg_wal/00000002.history").unwrap();
    let text = String::from_utf8(written).unwrap();
    assert_eq!(text, "# header\n\n1\t0/2000000\tno recovery target specified\n");
}

#[test]
fn write_history_no_parent_omits_leading_newline() {
    setup();
    let ctx = MemoryContext::new("test");
    writeTimeLineHistory(2, 1, 0x16B6C50, "reason", false, false, ctx.mcx()).unwrap();
    let text = String::from_utf8(get_file("pg_wal/00000002.history").unwrap()).unwrap();
    assert_eq!(text, "1\t0/16B6C50\treason\n");
}

#[test]
fn write_history_archives_when_active() {
    setup();
    let ctx = MemoryContext::new("test");
    writeTimeLineHistory(2, 1, 0x16B6C50, "reason", false, true, ctx.mcx()).unwrap();
    ARCHIVED.with(|c| assert_eq!(c.borrow().as_slice(), ["00000002.history"]));
}

#[test]
fn write_history_file_replaces() {
    setup();
    writeTimeLineHistoryFile(4, b"contents").unwrap();
    WRITTEN.with(|c| {
        let entry = c
            .borrow()
            .iter()
            .find(|(p, ..)| p == "pg_wal/00000004.history")
            .cloned()
            .unwrap();
        assert!(entry.2, "writeTimeLineHistoryFile must replace existing");
        assert_eq!(entry.1, b"contents");
    });
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
