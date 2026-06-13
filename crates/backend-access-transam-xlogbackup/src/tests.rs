//! Tests for the `xlogbackup.c` port.
//!
//! These exercise `build_backup_content` output. All cases pin GMT (built via
//! the lastditch `tzparse`) so the `%Z` zone abbreviation is deterministic.

use super::*;
use backend_timezone_localtime::{state, tzparse};
use types_core::MAXPGPATH;

/// Substring search over the raw bytes, mirroring C `strstr` over the
/// `char *` returned by `build_backup_content`.
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn gmt() -> pg_tz {
    let mut sp = state::default();
    assert!(tzparse("GMT", &mut sp, true), "GMT must parse");
    pg_tz::new("GMT".to_owned(), sp)
}

#[test]
fn builds_backup_label_content() {
    let state = backup_state(b"nightly", false);
    let content = build_backup_content(&state, false, DEFAULT_XLOG_SEG_SIZE, &gmt()).unwrap();

    assert!(contains(
        &content,
        b"START WAL LOCATION: 1/23456789 (file 000000070000000100000023)\n"
    ));
    assert!(contains(&content, b"CHECKPOINT LOCATION: 1/23456000\n"));
    assert!(contains(&content, b"BACKUP METHOD: streamed\n"));
    assert!(contains(&content, b"BACKUP FROM: primary\n"));
    assert!(contains(&content, b"START TIME: 2024-01-01 00:00:00 GMT\n"));
    assert!(contains(&content, b"LABEL: nightly\n"));
    assert!(contains(&content, b"START TIMELINE: 7\n"));
    assert!(!contains(&content, b"STOP WAL LOCATION"));
}

#[test]
fn builds_backup_history_content() {
    let state = backup_state(b"incremental", true);
    let content = build_backup_content(&state, true, DEFAULT_XLOG_SEG_SIZE, &gmt()).unwrap();

    assert!(contains(
        &content,
        b"STOP WAL LOCATION: 2/100 (file 000000080000000200000000)\n"
    ));
    assert!(contains(&content, b"BACKUP FROM: standby\n"));
    assert!(contains(&content, b"STOP TIME: 2024-01-01 01:00:00 GMT\n"));
    assert!(contains(&content, b"STOP TIMELINE: 8\n"));
    assert!(contains(&content, b"INCREMENTAL FROM LSN: 1/1\n"));
    assert!(contains(&content, b"INCREMENTAL FROM TLI: 6\n"));
}

#[test]
fn emits_non_utf8_label_verbatim() {
    // A LATIN1 label such as "caf\xE9" is not valid UTF-8; C copies it verbatim
    // via `appendStringInfo(..., "LABEL: %s\n", state->name)` (xlogbackup.c:65).
    let state = backup_state(b"caf\xE9", false);
    let content = build_backup_content(&state, false, DEFAULT_XLOG_SEG_SIZE, &gmt()).unwrap();

    assert!(contains(&content, b"LABEL: caf\xE9\n"));
}

#[test]
fn history_file_omits_when_not_requested() {
    // Without ishistoryfile, no STOP TIME / STOP TIMELINE lines are emitted.
    let state = backup_state(b"nightly", false);
    let content = build_backup_content(&state, false, DEFAULT_XLOG_SEG_SIZE, &gmt()).unwrap();
    assert!(!contains(&content, b"STOP TIME:"));
    assert!(!contains(&content, b"STOP TIMELINE:"));
    // Non-incremental backup omits the INCREMENTAL lines entirely.
    assert!(!contains(&content, b"INCREMENTAL FROM LSN:"));
    assert!(!contains(&content, b"INCREMENTAL FROM TLI:"));
}

fn backup_state(name: &[u8], incremental: bool) -> BackupState {
    let mut label = [0u8; MAXPGPATH + 1];
    for (dst, src) in label.iter_mut().zip(name.iter()) {
        *dst = *src;
    }

    BackupState::new(
        label,
        0x0000_0001_2345_6789,
        7,
        0x0000_0001_2345_6000,
        1_704_067_200,
        incremental,
        if incremental {
            0x0000_0001_0000_0001
        } else {
            0
        },
        if incremental { 6 } else { 0 },
        0x0000_0002_0000_0100,
        8,
        1_704_070_800,
    )
}
