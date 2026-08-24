use super::*;

fn make_page(lower: u16, upper: u16, lower_fill: u8, upper_fill: u8, hole_fill: u8) -> Vec<u8> {
    let mut p = vec![hole_fill; BLCKSZ];
    p[..lower as usize].fill(lower_fill);
    p[upper as usize..].fill(upper_fill);
    p[12..14].copy_from_slice(&lower.to_ne_bytes());
    p[14..16].copy_from_slice(&upper.to_ne_bytes());
    p
}

// Hand-derived against C writeFragment (generic_xlog.c:89-108): OffsetNumber
// offset, OffsetNumber length, then `length` data bytes, native-endian.
#[test]
fn fragment_encoding_vectors() {
    let mut delta = [0u8; MAX_DELTA_SIZE];
    let mut len = 0usize;
    write_fragment(&mut delta, &mut len, 0x0105, 3, &[0x10, 0x20, 0x30]);
    assert_eq!(len, 7);
    assert_eq!(&delta[..7], &[0x05, 0x01, 0x03, 0x00, 0x10, 0x20, 0x30]);

    write_fragment(&mut delta, &mut len, 0x1FFE, 1, &[0xAB]);
    assert_eq!(len, 12);
    assert_eq!(&delta[7..12], &[0xFE, 0x1F, 0x01, 0x00, 0xAB]);
}

// C generic_xlog.c:194: a matched run > MATCH_THRESHOLD splits fragments.
#[test]
fn match_threshold_split_and_merge() {
    let mut cur = vec![0u8; 64];
    let mut tgt = vec![0u8; 64];
    // two changed bytes separated by exactly MATCH_THRESHOLD matches
    tgt[10] = 1;
    tgt[10 + 1 + MATCH_THRESHOLD] = 2;
    let mut delta = [0u8; MAX_DELTA_SIZE];
    let mut len = 0usize;
    compute_region_delta(&mut delta, &mut len, &cur, &tgt, 0, 64, 0, 64);
    let span = 1 + MATCH_THRESHOLD + 1;
    assert_eq!(len, FRAGMENT_HEADER_SIZE + span);
    assert_eq!(u16::from_ne_bytes([delta[0], delta[1]]), 10);
    assert_eq!(u16::from_ne_bytes([delta[2], delta[3]]), span as u16);
    assert_eq!(&delta[4..4 + span], &tgt[10..10 + span]);

    // separated by MATCH_THRESHOLD + 1 matches: two fragments
    cur.fill(0);
    tgt.fill(0);
    tgt[10] = 1;
    tgt[10 + 1 + MATCH_THRESHOLD + 1] = 2;
    len = 0;
    compute_region_delta(&mut delta, &mut len, &cur, &tgt, 0, 64, 0, 64);
    assert_eq!(len, 2 * (FRAGMENT_HEADER_SIZE + 1));
    assert_eq!(u16::from_ne_bytes([delta[0], delta[1]]), 10);
    assert_eq!(u16::from_ne_bytes([delta[2], delta[3]]), 1);
    assert_eq!(delta[4], 1);
    assert_eq!(u16::from_ne_bytes([delta[5], delta[6]]), 16);
    assert_eq!(u16::from_ne_bytes([delta[7], delta[8]]), 1);
    assert_eq!(delta[9], 2);
}

// C generic_xlog.c:132-136/205-210: invalid-region bytes are always written.
#[test]
fn invalid_regions_always_emitted() {
    let cur = vec![7u8; 64];
    let tgt = vec![7u8; 64];
    let mut delta = [0u8; MAX_DELTA_SIZE];
    let mut len = 0usize;
    // valid part of cur is 8..56; the matched middle closes the leading
    // fragment, so head and tail come out as fragments [0,8) and [56,64)
    compute_region_delta(&mut delta, &mut len, &cur, &tgt, 0, 64, 8, 56);
    assert_eq!(len, 2 * (FRAGMENT_HEADER_SIZE + 8));
    assert_eq!(u16::from_ne_bytes([delta[0], delta[1]]), 0);
    assert_eq!(u16::from_ne_bytes([delta[2], delta[3]]), 8);
    assert_eq!(u16::from_ne_bytes([delta[12], delta[13]]), 56);
    assert_eq!(u16::from_ne_bytes([delta[14], delta[15]]), 8);
}

#[test]
fn delta_round_trip_reproduces_target() {
    let cur = make_page(100, 7000, 0xAA, 0xBB, 0x77);
    let mut tgt = make_page(120, 6800, 0xCC, 0xDD, 0x00);
    tgt[40] = 0x11;
    tgt[7900] = 0x33;

    let mut delta = [0u8; MAX_DELTA_SIZE];
    let mut len = 0usize;
    compute_delta(&mut delta, &mut len, &cur, &tgt);
    assert!(len > 0);

    let mut applied = cur.clone();
    redo_page_transform(&mut applied, &delta[..len], 0x1122_3344_5566_7788).unwrap();
    page_set_lsn(&mut tgt, 0x1122_3344_5566_7788);
    assert_eq!(applied, tgt);
}

#[test]
fn identical_pages_yield_empty_delta() {
    let cur = make_page(100, 7000, 0xAA, 0xBB, 0);
    let mut delta = [0u8; MAX_DELTA_SIZE];
    let mut len = 0usize;
    compute_delta(&mut delta, &mut len, &cur, &cur.clone());
    assert_eq!(len, 0);
}

// C worst case: two fragment headers + a full page of data fit MAX_DELTA_SIZE.
#[test]
fn worst_case_delta_fits() {
    let mut cur = make_page(4000, 4200, 0x00, 0x00, 0);
    let mut tgt = make_page(4000, 4200, 0xFF, 0xEE, 0);
    cur[0..8].copy_from_slice(&tgt[0..8]);
    cur[12..16].copy_from_slice(&tgt[12..16]);
    let mut delta = [0u8; MAX_DELTA_SIZE];
    let mut len = 0usize;
    compute_delta(&mut delta, &mut len, &cur, &tgt);
    assert!(len <= MAX_DELTA_SIZE);
    let mut applied = cur.clone();
    apply_page_redo(&mut applied, &delta[..len]).unwrap();
    assert_eq!(applied[..4000], tgt[..4000]);
    assert_eq!(applied[4200..], tgt[4200..]);
}

// Untrusted WAL: malformed fragments must be rejected as ERRCODE_DATA_CORRUPTED
// rather than panicking / writing out of bounds in the startup redo thread.
#[test]
fn malformed_fragments_rejected_as_corruption() {
    let mut page = vec![0u8; BLCKSZ];

    // offset + length exceeds BLCKSZ (offset=0x2000, length=0x2000).
    let mut delta = Vec::new();
    delta.extend_from_slice(&0x2000u16.to_ne_bytes());
    delta.extend_from_slice(&0x2000u16.to_ne_bytes());
    delta.resize(delta.len() + 0x2000, 0);
    let err = apply_page_redo(&mut page, &delta).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // declared length runs past the end of the delta payload.
    let mut delta = Vec::new();
    delta.extend_from_slice(&0u16.to_ne_bytes());
    delta.extend_from_slice(&16u16.to_ne_bytes());
    delta.extend_from_slice(&[0u8; 4]); // only 4 bytes, not 16
    let err = apply_page_redo(&mut page, &delta).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // truncated fragment header (< FRAGMENT_HEADER_SIZE trailing bytes).
    let err = apply_page_redo(&mut page, &[0x00, 0x00, 0x01]).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
}

// A crafted delta that writes an invalid pd_lower/pd_upper must not panic the
// hole-fill; redo_page_transform reports corruption instead.
#[test]
fn invalid_hole_bounds_rejected_as_corruption() {
    let mut page = vec![0u8; BLCKSZ];
    // fragment writes pd_lower=0x2000 (> pd_upper=0) into the header at 12..16.
    let mut delta = Vec::new();
    delta.extend_from_slice(&12u16.to_ne_bytes()); // offset of pd_lower
    delta.extend_from_slice(&4u16.to_ne_bytes()); // length: lower+upper
    delta.extend_from_slice(&0x2000u16.to_ne_bytes()); // pd_lower
    delta.extend_from_slice(&0u16.to_ne_bytes()); // pd_upper
    let err = redo_page_transform(&mut page, &delta, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
}
