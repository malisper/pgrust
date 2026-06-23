//! Unit tests for the WAL-archiver port's OWN logic: the priority comparator,
//! the bounded max-heap, and the path/truncation/string helpers. The seamed
//! boundaries are exercised by the audit and the integration smoke, not here.

use super::*;
use std::cell::Cell as StdCell;

// Deterministic clock for the timer helpers.
thread_local! {
    static CLOCK: StdCell<i64> = const { StdCell::new(0) };
}

pub(crate) fn test_clock() -> i64 {
    CLOCK.with(|c| c.get())
}

// ---------------------------------------------------------------------------
// ready_file_comparator / is_tl_history_file_name
// ---------------------------------------------------------------------------

#[test]
fn history_files_outrank_non_history() {
    // "00000002.history" is a history file; a regular segment is not.
    assert!(is_tl_history_file_name(b"00000002.history"));
    assert!(!is_tl_history_file_name(b"000000010000000000000001"));
    // a_history && !b_history => a higher priority => negative.
    assert!(ready_file_comparator("00000002.history", "000000010000000000000001") < 0);
    // symmetric.
    assert!(ready_file_comparator("000000010000000000000001", "00000002.history") > 0);
}

#[test]
fn lowercase_hex_is_not_a_history_file() {
    // C uses upper-case hex for the timeline id (VALID_XFN_CHARS).
    assert!(!is_tl_history_file_name(b"0000000a.history"));
}

#[test]
fn older_segments_have_higher_priority() {
    // Among equal-class files, strcmp orders them; the lexicographically
    // smaller (older) name wins (negative).
    assert!(ready_file_comparator(
        "000000010000000000000001",
        "000000010000000000000002"
    ) < 0);
    assert_eq!(
        ready_file_comparator(
            "000000010000000000000001",
            "000000010000000000000001"
        ),
        0
    );
}

// ---------------------------------------------------------------------------
// ArchHeap (the bounded binaryheap specialization)
// ---------------------------------------------------------------------------

#[test]
fn heap_root_is_lowest_priority() {
    let mut h = ArchHeap::allocate(NUM_FILES_PER_DIRECTORY_SCAN);
    for name in [
        "000000010000000000000003",
        "000000010000000000000001",
        "000000010000000000000002",
    ] {
        h.add(String::from(name));
    }
    // Root is the comparator-maximum = lowest archival priority = newest name.
    assert_eq!(h.first(), Some("000000010000000000000003"));
}

#[test]
fn heap_build_then_drain_ascending_priority() {
    let mut h = ArchHeap::allocate(NUM_FILES_PER_DIRECTORY_SCAN);
    for name in [
        "000000010000000000000002",
        "000000010000000000000004",
        "000000010000000000000001",
        "000000010000000000000003",
    ] {
        h.add_unordered(String::from(name));
    }
    h.build();
    // remove_first repeatedly yields names from lowest to highest priority
    // (newest first), exactly what pgarch_readyXlog fills arch_files with.
    let mut drained = Vec::new();
    while let Some(f) = h.remove_first() {
        drained.push(f);
    }
    assert_eq!(
        drained,
        vec![
            "000000010000000000000004".to_string(),
            "000000010000000000000003".to_string(),
            "000000010000000000000002".to_string(),
            "000000010000000000000001".to_string(),
        ]
    );
}

#[test]
fn heap_reset_empties() {
    let mut h = ArchHeap::allocate(4);
    h.add(String::from("000000010000000000000001"));
    assert!(!h.is_empty());
    h.reset();
    assert!(h.is_empty());
    assert_eq!(h.len(), 0);
}

// ---------------------------------------------------------------------------
// helpers: c_strspn / c_strcmp / status_file_path / truncate
// ---------------------------------------------------------------------------

#[test]
fn strspn_matches_valid_prefix() {
    // "00AB.r" are all in VALID_XFN_CHARS ('.' and 'r' both present); 'e' is
    // not, so the run stops at index 6.
    assert_eq!(c_strspn(b"00AB.ready", VALID_XFN_CHARS), 6);
    assert_eq!(c_strspn(b"zzz", VALID_XFN_CHARS), 0);
}

#[test]
fn strcmp_byte_lexicographic() {
    assert!(c_strcmp(b"abc", b"abd") < 0);
    assert!(c_strcmp(b"abc", b"ab") > 0);
    assert_eq!(c_strcmp(b"abc", b"abc"), 0);
}

#[test]
fn status_file_path_layout() {
    assert_eq!(
        status_file_path("000000010000000000000001", ".ready"),
        "pg_wal/archive_status/000000010000000000000001.ready"
    );
}

#[test]
fn truncate_respects_buffer_size() {
    // n includes the NUL, so at most n-1 content bytes survive.
    assert_eq!(truncate("abcdef".to_string(), 4), "abc");
    assert_eq!(truncate("abc".to_string(), 8), "abc");
}
