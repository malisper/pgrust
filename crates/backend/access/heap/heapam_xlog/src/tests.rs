use super::*;

// The write side (heapam::dml) keeps its own copies of the WAL shape
// constants; divergence would silently corrupt replay.
#[test]
fn wal_constants_match_write_side() {
    assert_eq!(XLOG_HEAP_INSERT, heapam::dml::XLOG_HEAP_INSERT);
    assert_eq!(XLOG_HEAP_DELETE, heapam::dml::XLOG_HEAP_DELETE);
    assert_eq!(XLOG_HEAP_UPDATE, heapam::dml::XLOG_HEAP_UPDATE);
    assert_eq!(XLOG_HEAP_HOT_UPDATE, heapam::dml::XLOG_HEAP_HOT_UPDATE);
    assert_eq!(XLOG_HEAP_LOCK, heapam::dml::XLOG_HEAP_LOCK);
    assert_eq!(XLOG_HEAP_INIT_PAGE, heapam::dml::XLOG_HEAP_INIT_PAGE);
    assert_eq!(XLOG_HEAP_INPLACE, heapam::dml::XLOG_HEAP_INPLACE);
    assert_eq!(XLH_INSERT_ALL_VISIBLE_CLEARED, heapam::dml::XLH_INSERT_ALL_VISIBLE_CLEARED);
    assert_eq!(
        XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED,
        heapam::dml::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED
    );
    assert_eq!(
        XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED,
        heapam::dml::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED
    );
    assert_eq!(XLH_LOCK_ALL_FROZEN_CLEARED, heapam::dml::XLH_LOCK_ALL_FROZEN_CLEARED);
    assert_eq!(XLH_DELETE_ALL_VISIBLE_CLEARED, heapam::dml::XLH_DELETE_ALL_VISIBLE_CLEARED);
    assert_eq!(XLH_DELETE_IS_PARTITION_MOVE, heapam::dml::XLH_DELETE_IS_PARTITION_MOVE);
    assert_eq!(XLHL_XMAX_IS_MULTI, heapam::dml::XLHL_XMAX_IS_MULTI);
    assert_eq!(XLHL_XMAX_LOCK_ONLY, heapam::dml::XLHL_XMAX_LOCK_ONLY);
    assert_eq!(XLHL_XMAX_EXCL_LOCK, heapam::dml::XLHL_XMAX_EXCL_LOCK);
    assert_eq!(XLHL_XMAX_KEYSHR_LOCK, heapam::dml::XLHL_XMAX_KEYSHR_LOCK);
    assert_eq!(XLHL_KEYS_UPDATED, heapam::dml::XLHL_KEYS_UPDATED);
}

#[test]
fn fix_infomask_from_infobits_bit_mapping() {
    let (mut im, mut im2) = (0u16, 0u16);
    fix_infomask_from_infobits(XLHL_XMAX_EXCL_LOCK | XLHL_KEYS_UPDATED, &mut im, &mut im2);
    assert_eq!(im, HEAP_XMAX_EXCL_LOCK);
    assert_eq!(im2, HEAP_KEYS_UPDATED);

    let (mut im, mut im2) = (HEAP_XMAX_EXCL_LOCK, HEAP_KEYS_UPDATED);
    fix_infomask_from_infobits(XLHL_XMAX_IS_MULTI | XLHL_XMAX_LOCK_ONLY | XLHL_XMAX_KEYSHR_LOCK, &mut im, &mut im2);
    assert_eq!(im, HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK);
    assert_eq!(im2, 0);
}

// Record-declared length/count validation for the heap/heap2 redo arms:
// malformed WAL payloads must produce a catchable ERRCODE_DATA_CORRUPTED error
// (not a slice-index panic on the startup redo thread). See finding: "Heap redo
// trusts record-declared lengths and counts".

#[test]
fn read_u16_u32_reject_overrun_with_data_corrupted() {
    // Valid reads at the exact edge succeed.
    assert_eq!(read_u16(&[1, 0, 9], 0, "t").unwrap(), 1);
    assert_eq!(read_u16(&[0, 7, 0], 1, "t").unwrap(), 7);
    assert_eq!(read_u32(&[2, 0, 0, 0], 0, "t").unwrap(), 2);

    // Reads that run past the payload error instead of panicking.
    let e = read_u16(&[0u8; 3], 2, "t").err().unwrap();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
    assert!(read_u16(&[], 0, "t").is_err());
    let e = read_u32(&[0u8; 3], 0, "t").err().unwrap();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
    // Offset arithmetic near usize::MAX must not overflow-panic.
    assert!(read_u16(&[0u8; 8], usize::MAX, "t").is_err());
}

#[test]
fn insert_tuple_body_len_validates_bounds() {
    // A record declaring only 4 bytes of block data (< SizeOfHeapHeader == 5),
    // as in the finding's "INSERT with 4-byte block data" repro, must error
    // rather than wrap `newlen` to ~usize::MAX.
    let e = checked_heap_tuple_body_len(4, "heap_xlog_insert").err().unwrap();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
    // Exactly SizeOfHeapHeader still leaves a zero-length body -> error.
    assert!(checked_heap_tuple_body_len(SizeOfHeapHeader, "heap_xlog_insert").is_err());
    // Oversized: a body larger than MaxHeapTupleSize would overrun the fixed
    // tuple buffer.
    let too_big = SizeOfHeapHeader + MaxHeapTupleSize + 1;
    let e = checked_heap_tuple_body_len(too_big, "heap_xlog_insert").err().unwrap();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
    // A well-formed length returns the tuple body size unchanged.
    assert_eq!(
        checked_heap_tuple_body_len(SizeOfHeapHeader + 10, "heap_xlog_insert").unwrap(),
        10
    );
    assert_eq!(
        checked_heap_tuple_body_len(SizeOfHeapHeader + MaxHeapTupleSize, "heap_xlog_insert").unwrap(),
        MaxHeapTupleSize
    );
}
