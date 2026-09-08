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
    // upstream f581fa729d8e (18.5): the VM block-reference ids redo reads
    // are the ones the write side registers.
    assert_eq!(HEAP_INSERT_BLKREF_VM, heapam::dml::HEAP_INSERT_BLKREF_VM);
    assert_eq!(HEAP_MULTI_INSERT_BLKREF_VM, heapam::dml::HEAP_MULTI_INSERT_BLKREF_VM);
    assert_eq!(HEAP_DELETE_BLKREF_VM, heapam::dml::HEAP_DELETE_BLKREF_VM);
    assert_eq!(HEAP_LOCK_BLKREF_VM, heapam::dml::HEAP_LOCK_BLKREF_VM);
    assert_eq!(HEAP_UPDATE_BLKREF_VM_NEW, heapam::dml::HEAP_UPDATE_BLKREF_VM_NEW);
    assert_eq!(HEAP_UPDATE_BLKREF_VM_OLD, heapam::dml::HEAP_UPDATE_BLKREF_VM_OLD);
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

// --- audit-18.6 w2-034 (a186-candidate-fp-heap-rewriteheap-e01e8c21d9efbd22f6c5-1):
// rewriteheap.c:1100/1114/1131 heap_xlog_logical_rewrite brackets its
// ftruncate / pg_pwrite / pg_fsync with WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE,
// _MAPPING_WRITE and _MAPPING_SYNC (pgstat_report_wait_start/_end). Ids are
// the positions in waitevent's IO name table (wait_event_names.txt order).

static LRW_WAIT_EVENTS: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());

fn install_wait_event_recorder() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        waitevent_seams::pgstat_report_wait_start::set(|info| {
            LRW_WAIT_EVENTS.lock().unwrap().push(info);
        });
        waitevent_seams::pgstat_report_wait_end::set(|| {
            LRW_WAIT_EVENTS.lock().unwrap().push(0);
        });
    });
    LRW_WAIT_EVENTS.lock().unwrap().clear();
}

#[test]
fn heap_xlog_logical_rewrite_reports_truncate_write_sync_wait_events() {
    const PG_WAIT_IO: u32 = 0x0A00_0000;
    const WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC: u32 = PG_WAIT_IO | 35;
    const WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE: u32 = PG_WAIT_IO | 36;
    const WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE: u32 = PG_WAIT_IO | 38;
    const LOGICAL_REWRITE_MAPPING_SIZE: usize = 36;

    install_wait_event_recorder();
    let dir = std::env::temp_dir().join(format!("heapam-xlog-lrw-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("pg_logical/mappings")).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());

    // xl_heap_rewrite_mapping (40 bytes, C layout) + one mapping entry.
    let mut md = vec![0u8; 40 + LOGICAL_REWRITE_MAPPING_SIZE];
    md[0..4].copy_from_slice(&700u32.to_ne_bytes()); // mapped_xid
    md[4..8].copy_from_slice(&5u32.to_ne_bytes()); // mapped_db
    md[8..12].copy_from_slice(&16384u32.to_ne_bytes()); // mapped_rel
    md[16..24].copy_from_slice(&0i64.to_ne_bytes()); // offset
    md[24..28].copy_from_slice(&1u32.to_ne_bytes()); // num_mappings
    md[32..40].copy_from_slice(&0x0000_0001_0000_0010u64.to_ne_bytes()); // start_lsn
    for b in md[40..].iter_mut() {
        *b = 0xAB;
    }
    let rec = xlogreader_seams::DecodedXLogRecord {
        xl_info: XLOG_HEAP2_REWRITE,
        xl_xid: 900,
        main_data: md.as_ptr(),
        main_data_len: md.len() as u32,
        ..Default::default()
    };
    let mut state = XLogReaderState { record: Some(rec), ..Default::default() };
    heap_xlog_logical_rewrite(&mut state).unwrap();

    let path = dir.join("pg_logical/mappings/map-5-4000-1_10-2bc-384");
    assert_eq!(std::fs::read(&path).unwrap(), vec![0xABu8; LOGICAL_REWRITE_MAPPING_SIZE]);
    assert_eq!(
        core::mem::take(&mut *LRW_WAIT_EVENTS.lock().unwrap()),
        vec![
            WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE,
            0,
            WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE,
            0,
            WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC,
            0,
        ],
        "ftruncate, pg_pwrite and pg_fsync each run under their own wait event"
    );
}
