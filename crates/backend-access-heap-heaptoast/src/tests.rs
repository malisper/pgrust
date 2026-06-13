//! Unit tests for the pure (seam-free) pieces: the threshold constants
//! derived from the page layout, the varlena-header predicates, and the
//! ScanKey initializer. The seam-driven routines panic until their owning
//! subsystems land, so they are not exercised here.

use super::*;

#[test]
fn thresholds_match_c_values() {
    // Values on the default 8 KiB page (cross-checked against the c2rust
    // rendering's constant folding).
    assert_eq!(TOAST_TUPLE_THRESHOLD, 2032);
    assert_eq!(TOAST_TUPLE_TARGET, 2032);
    assert_eq!(TOAST_TUPLE_TARGET_MAIN, 8160);
    assert_eq!(EXTERN_TUPLE_MAX_SIZE, 2032);
    assert_eq!(TOAST_MAX_CHUNK_SIZE, 1996);
}

#[test]
fn maxalign_matches_c() {
    assert_eq!(MAXALIGN(0), 0);
    assert_eq!(MAXALIGN(1), 8);
    assert_eq!(MAXALIGN(8), 8);
    assert_eq!(MAXALIGN(9), 16);
    assert_eq!(MAXALIGN(23), 24);
    assert_eq!(MAXALIGN_DOWN(2038), 2032);
}

#[test]
fn varlena_predicates_match_header_bits() {
    // Plain 4-byte header (low bits 0b00): length word 0x40 >> 2 == 16.
    let plain: [u8; 8] = [0x40, 0x00, 0x00, 0x00, 0, 0, 0, 0];
    assert!(varatt_is_4b(&plain));
    assert!(!varatt_is_extended(&plain));
    assert!(!varatt_is_external(&plain));
    assert!(!varatt_is_compressed(&plain));
    assert_eq!(varsize_4b(&plain), 16);

    // Compressed 4-byte header (low bits 0b10).
    let comp: [u8; 8] = [0x02, 0x00, 0x00, 0x00, 0, 0, 0, 0];
    assert!(varatt_is_compressed(&comp));
    assert!(varatt_is_extended(&comp));
    assert!(!varatt_is_short(&comp));

    // Short 1-byte header (low bit 1, not the external tag 0x01):
    // va_header 0x07 -> VARSIZE_1B == 3.
    let short: [u8; 4] = [0x07, 0, 0, 0];
    assert!(varatt_is_short(&short));
    assert!(varatt_is_extended(&short));
    assert!(!varatt_is_external(&short));
    assert_eq!(varsize_1b(&short), 3);

    // External TOAST pointer (va_header == 0x01).
    let ext: [u8; 2] = [0x01, 18];
    assert!(varatt_is_external(&ext));
    assert!(varatt_is_extended(&ext));
}

// NB: the former `scankey_init_stamps_fields` test covered this crate's local
// `ScanKeyInit`. That initializer is now the shared
// `backend_access_common_scankey::ScanKeyInit` (it resolves `sk_func` through
// the fmgr seam), so the field-stamping behavior is owned and tested there.

#[test]
fn att_isnull_matches_c_bitmap_sense() {
    // Bit set means NOT null.
    let bits: [u8; 2] = [0b0000_0101, 0b0000_0001];
    assert!(!att_isnull(0, &bits));
    assert!(att_isnull(1, &bits));
    assert!(!att_isnull(2, &bits));
    assert!(!att_isnull(8, &bits));
    assert!(att_isnull(9, &bits));
}
