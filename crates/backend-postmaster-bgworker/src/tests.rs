//! Unit tests for the pure (seam-free) helpers and constants.

use super::*;

fn cstr(s: &str, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    v[..s.len()].copy_from_slice(s.as_bytes());
    v
}

#[test]
fn cstr_len_stops_at_nul() {
    assert_eq!(cstr_len(b"abc\0\0\0"), 3);
    assert_eq!(cstr_len(b"abc"), 3);
    assert_eq!(cstr_len(b"\0abc"), 0);
}

#[test]
fn cstr_str_and_eq() {
    let buf = cstr("postgres", 16);
    assert_eq!(cstr_str(&buf), "postgres");
    assert!(cstr_eq(&buf, b"postgres"));
    assert!(!cstr_eq(&buf, b"postgre"));
    assert!(!cstr_eq(&buf, b"postgresql"));
}

#[test]
fn cstr_lossy_owns() {
    let buf = cstr("worker", 12);
    assert_eq!(cstr_lossy(&buf), "worker".to_string());
}

#[test]
fn strcpy_zeroes_remainder() {
    let mut dst = [0xFFu8; 8];
    let mut src = [0u8; 8];
    src[..3].copy_from_slice(b"abc");
    strcpy(&mut dst, &src);
    assert_eq!(&dst[..3], b"abc");
    assert!(dst[3..].iter().all(|&b| b == 0));
}

#[test]
fn ascii_safe_strlcpy_replaces_high_bit_and_nul_terminates() {
    let mut src = [0u8; 8];
    src[0] = b'a';
    src[1] = 0xC3; // non-ASCII -> '?'
    src[2] = b'b';
    let out = ascii_safe_strlcpy(&src, 8);
    assert_eq!(out[0], b'a');
    assert_eq!(out[1], b'?');
    assert_eq!(out[2], b'b');
    assert_eq!(out[3], 0);
    // Always NUL-terminated within bounds.
    assert_eq!(out[7], 0);
}

#[test]
fn ascii_safe_strlcpy_drops_control_chars_keeps_whitespace() {
    // ascii.c: keep 32..=127 and \n/\r/\t; every other byte (incl. low control
    // bytes 0x01-0x08, 0x0b, 0x0c, 0x0e-0x1f) becomes '?'.
    let mut src = [0u8; 8];
    src[0] = 0x01; // SOH -> '?'
    src[1] = b'\t'; // kept
    src[2] = 0x1f; // US -> '?'
    src[3] = b'\n'; // kept
    src[4] = b' '; // 32 -> kept
    src[5] = 0x7f; // DEL (127) -> kept
    let out = ascii_safe_strlcpy(&src, 8);
    assert_eq!(out[0], b'?');
    assert_eq!(out[1], b'\t');
    assert_eq!(out[2], b'?');
    assert_eq!(out[3], b'\n');
    assert_eq!(out[4], b' ');
    assert_eq!(out[5], 0x7f);
    assert_eq!(out[7], 0);
}

#[test]
fn internal_bgworker_names_in_declaration_order() {
    assert_eq!(INTERNAL_BGWORKER_NAMES.len(), 5);
    assert_eq!(INTERNAL_BGWORKER_NAMES[0], "ParallelWorkerMain");
    assert_eq!(INTERNAL_BGWORKER_NAMES[4], "TablesyncWorkerMain");
}

#[test]
fn background_worker_lock_offset_matches_lwlocklist() {
    // PG_LWLOCK(33, BackgroundWorker) -> MainLWLockArray[32].
    assert_eq!(BACKGROUND_WORKER_LWLOCK_OFFSET, 32);
}

#[test]
fn slot_size_matches_c_field_sum() {
    // 16 (header) + 16 (slot prefix) + worker fields.
    let worker = BGW_MAXLEN * 3 + MAXPGPATH + 4 + 4 + 4 + 8 + BGW_EXTRALEN + 4;
    assert_eq!(BACKGROUND_WORKER_SLOT_SIZE, 16 + worker);
    assert_eq!(BGW_ARRAY_HEADER_SIZE, 16);
}
