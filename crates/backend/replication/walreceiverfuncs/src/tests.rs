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
fn walrcv_strlcpy_truncates_and_stops_at_nul() {
    // strlcpy into a fixed shmem `char[]` field: stop at an embedded NUL,
    // truncate to fit cap-1 bytes, always NUL-terminate, and round-trip back
    // through walrcv_cstr_to_string. Mirrors the C `strlcpy` the shmem
    // WalRcvData char-array fields use.
    let mut dst = [0xFFu8; 8];
    walrcv_strlcpy(&mut dst, b"abc\0def");
    assert_eq!(::types_walreceiver::walrcv_cstr_to_string(&dst), "abc");

    // Truncates to cap-1 bytes.
    let mut dst2 = [0xFFu8; 4];
    walrcv_strlcpy(&mut dst2, b"abcdef");
    assert_eq!(::types_walreceiver::walrcv_cstr_to_string(&dst2), "abc");

    // Empty source yields an empty string.
    let mut dst3 = [0xFFu8; 4];
    walrcv_strlcpy(&mut dst3, b"");
    assert_eq!(::types_walreceiver::walrcv_cstr_to_string(&dst3), "");
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
