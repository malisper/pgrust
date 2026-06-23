//! Unit tests for the pure helpers (the seam-driven paths panic until their
//! owners land, so only the allocation-free arithmetic/parsing is tested).

use super::*;

#[test]
fn timestamp_difference_milliseconds_rounds_up_and_clamps() {
    // Equal / decreasing times => 0 (never negative).
    assert_eq!(TimestampDifferenceMilliseconds(100, 100), 0);
    assert_eq!(TimestampDifferenceMilliseconds(200, 100), 0);
    // (diff + 999) / 1000 rounding (microseconds -> whole ms).
    assert_eq!(TimestampDifferenceMilliseconds(0, 1), 1);
    assert_eq!(TimestampDifferenceMilliseconds(0, 1000), 1);
    assert_eq!(TimestampDifferenceMilliseconds(0, 1001), 2);
    assert_eq!(TimestampDifferenceMilliseconds(0, 2_000_000), 2000);
}

#[test]
fn timestamptz_plus_milliseconds() {
    assert_eq!(TimestampTzPlusMilliseconds(0, 10), 10_000);
    assert_eq!(TimestampTzPlusMilliseconds(5, 0), 5);
}

#[test]
fn xlog_segno_offset_to_rec_ptr() {
    // dest = segno * wal_segsz + offset.
    assert_eq!(XLogSegNoOffsetToRecPtr(0, 0, 16 * 1024 * 1024), 0);
    assert_eq!(XLogSegNoOffsetToRecPtr(2, 100, 16 * 1024 * 1024), 2 * 16 * 1024 * 1024 + 100);
}

#[test]
fn lsn_fmt_high_low_split() {
    assert_eq!(lsn_fmt(0), "0/0");
    assert_eq!(lsn_fmt((1u64 << 32) | 0xABCD), "1/ABCD");
}

#[test]
fn byte_readers_native_endian_and_bounds() {
    let data = 0x1122_3344u32.to_ne_bytes().to_vec();
    assert_eq!(read_u32(&data, 0), 0x1122_3344);
    assert_eq!(read_i32(&data, 0), 0x1122_3344);
    assert_eq!(read_oid(&data, 0), 0x1122_3344);
    // Out-of-bounds reads zero-fill (the C struct reads are bounds-checked here).
    assert_eq!(read_u32(&data, 100), 0);
    let short = vec![0xFFu8, 0x00];
    assert_eq!(read_u32(&short, 0), u32::from_ne_bytes([0xFF, 0x00, 0, 0]));
}

#[test]
fn read_rlocator_layout() {
    let mut data = Vec::new();
    data.extend_from_slice(&1u32.to_ne_bytes()); // spc
    data.extend_from_slice(&2u32.to_ne_bytes()); // db
    data.extend_from_slice(&3u32.to_ne_bytes()); // rel
    let rl = read_rlocator(&data, 0);
    assert_eq!(rl.spcOid, 1);
    assert_eq!(rl.dbOid, 2);
    assert_eq!(rl.relNumber, 3);
}

#[test]
fn read_forknum_maps_values() {
    assert_eq!(read_forknum(&0i32.to_ne_bytes(), 0), MAIN_FORKNUM);
    assert_eq!(read_forknum(&1i32.to_ne_bytes(), 0), FSM_FORKNUM);
    assert_eq!(read_forknum(&2i32.to_ne_bytes(), 0), VISIBILITYMAP_FORKNUM);
    assert_eq!(read_forknum(&3i32.to_ne_bytes(), 0), ForkNumber::INIT_FORKNUM);
}

#[test]
fn next_forknum_walks_enum_values() {
    assert_eq!(next_forknum(MAIN_FORKNUM), FSM_FORKNUM);
    assert_eq!(next_forknum(FSM_FORKNUM), VISIBILITYMAP_FORKNUM);
    assert_eq!(next_forknum(VISIBILITYMAP_FORKNUM), ForkNumber::INIT_FORKNUM);
}

#[test]
fn truncate_path_caps_at_maxpgpath() {
    let short = "pg_wal/summaries/temp.summary".to_string();
    assert_eq!(truncate_path(short.clone()), short);
    let long = "x".repeat(MAXPGPATH + 50);
    assert_eq!(truncate_path(long).len(), MAXPGPATH - 1);
}
