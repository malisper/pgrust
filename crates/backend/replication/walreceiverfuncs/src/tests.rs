use super::*;

#[test]
fn xlog_segment_offset_is_low_bits() {
    // 16 MB segment: offset is the low 24 bits.
    let segsz = 16 * 1024 * 1024;
    assert_eq!(XLogSegmentOffset(0, segsz), 0);
    assert_eq!(XLogSegmentOffset(segsz as u64, segsz), 0);
    assert_eq!(XLogSegmentOffset(segsz as u64 + 123, segsz), 123);
    assert_eq!(XLogSegmentOffset(segsz as u64 - 1, segsz), segsz as u32 - 1);
}

#[test]
fn strlcpy_field_truncates_and_stops_at_nul() {
    let mut dst = String::new();
    // Stops at the embedded NUL.
    strlcpy_field(&mut dst, b"abc\0def", 64);
    assert_eq!(dst, "abc");

    // Truncates to size-1 bytes.
    let mut dst2 = String::new();
    strlcpy_field(&mut dst2, b"abcdef", 4);
    assert_eq!(dst2, "abc");

    // Empty cap yields empty string.
    let mut dst3 = String::from("old");
    strlcpy_field(&mut dst3, b"xyz", 0);
    assert_eq!(dst3, "");
}

#[test]
fn shmem_size_is_the_block_size() {
    let sz = WalRcvShmemSize().expect("size");
    assert_eq!(sz, core::mem::size_of::<WalRcvShared>());
}

#[test]
fn wait_event_exit_precedes_wait_start() {
    // WAL_RECEIVER_EXIT sorts immediately before WAL_RECEIVER_WAIT_START.
    assert_eq!(
        WAIT_EVENT_WAL_RECEIVER_EXIT + 1,
        types_pgstat::wait_event::WAIT_EVENT_WAL_RECEIVER_WAIT_START
    );
}
