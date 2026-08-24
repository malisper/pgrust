use crate::*;

// standbydefs.h: xcnt 0, subxcnt 4, overflow 8, nextXid 12, oldest 16, latestCompleted 20.
#[test]
fn running_xacts_header_matches_c_layout() {
    let xids = [900u32, 905];
    let running = procarray::RunningTransactions {
        xids: &xids,
        xcnt: 1,
        subxcnt: 1,
        subxid_overflow: false,
        next_xid: 910,
        oldest_running_xid: 900,
        latest_completed_xid: 899,
        oldest_database_running_xid: 900,
    };
    let hdr = running_xacts_header(&running);
    assert_eq!(hdr.len(), 24);
    assert_eq!(i32::from_ne_bytes(hdr[0..4].try_into().unwrap()), 1);
    assert_eq!(i32::from_ne_bytes(hdr[4..8].try_into().unwrap()), 1);
    assert_eq!(hdr[8], 0);
    assert_eq!(&hdr[9..12], &[0, 0, 0]);
    assert_eq!(u32::from_ne_bytes(hdr[12..16].try_into().unwrap()), 910);
    assert_eq!(u32::from_ne_bytes(hdr[16..20].try_into().unwrap()), 900);
    assert_eq!(u32::from_ne_bytes(hdr[20..24].try_into().unwrap()), 899);

    let overflowed = procarray::RunningTransactions {
        subxid_overflow: true,
        subxcnt: 0,
        ..running
    };
    assert_eq!(running_xacts_header(&overflowed)[8], 1);
}

// standbydefs.h xl_standby_lock: xid 0, dbOid 4, relOid 8; 12 bytes each.
#[test]
fn standby_lock_body_matches_c_layout() {
    let locks = [
        xl_standby_lock { xid: 700, dbOid: 5, relOid: 16384 },
        xl_standby_lock { xid: 701, dbOid: 5, relOid: 16385 },
    ];
    let body = standby_locks_body(&locks);
    assert_eq!(body.len(), 2 * SIZE_OF_XL_STANDBY_LOCK);
    assert_eq!(u32::from_ne_bytes(body[0..4].try_into().unwrap()), 700);
    assert_eq!(u32::from_ne_bytes(body[4..8].try_into().unwrap()), 5);
    assert_eq!(u32::from_ne_bytes(body[8..12].try_into().unwrap()), 16384);
    assert_eq!(u32::from_ne_bytes(body[12..16].try_into().unwrap()), 701);
    assert_eq!(u32::from_ne_bytes(body[20..24].try_into().unwrap()), 16385);
}

#[test]
fn recovery_lock_table_bookkeeping_dedupes_and_chains() {
    use crate::recovery::test_support as ts;
    ts::init_lock_tables_only();

    assert!(ts::insert_entry(700, 5, 16384));
    // Checkpoints re-report held locks; the dedupe hash absorbs them.
    assert!(!ts::insert_entry(700, 5, 16384));
    assert!(ts::insert_entry(700, 5, 16385));
    assert!(ts::insert_entry(701, 5, 16384));

    assert_eq!(ts::recovery_lock_table_counts(), (3, 2));
    assert_eq!(ts::chain(700), vec![(5, 16384), (5, 16385)]);
    assert_eq!(ts::chain(701), vec![(5, 16384)]);
    assert_eq!(ts::chain(702), vec![]);
}

// A hostile primary/archive can set arbitrary counts in a RM_STANDBY_ID
// record (the WAL CRC is a checksum, not a MAC). validate_count must reject
// every out-of-range count as a catchable ERRCODE_DATA_CORRUPTED error rather
// than letting it reach slice-index/allocation panics on the redo thread.
#[test]
fn validate_count_rejects_hostile_counts() {
    use types_error::ERRCODE_DATA_CORRUPTED;

    // Well-formed: 2 * 12-byte locks backed by a 4-byte header.
    let ok = validate_count(4 + 2 * SIZE_OF_XL_STANDBY_LOCK, 4, 2, SIZE_OF_XL_STANDBY_LOCK, "t");
    assert_eq!(ok.unwrap(), 2);

    // Negative count (sign-extends to a huge usize in the buggy path).
    let e = validate_count(4, 4, -1, SIZE_OF_XL_STANDBY_LOCK, "t").err().unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // Positive but unbacked by the record (would OOB-index).
    let e = validate_count(4 + SIZE_OF_XL_STANDBY_LOCK, 4, 2, SIZE_OF_XL_STANDBY_LOCK, "t")
        .err()
        .unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // count * elem_size overflows usize (would wrap/allocate absurdly).
    let e = validate_count(64, 16, i32::MAX, SHARED_INVALIDATION_MESSAGE_SIZE, "t")
        .err()
        .unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);

    // Zero-count records are valid (header only).
    assert_eq!(validate_count(16, 16, 0, SHARED_INVALIDATION_MESSAGE_SIZE, "t").unwrap(), 0);
}

#[test]
fn require_len_rejects_truncated_header() {
    use types_error::ERRCODE_DATA_CORRUPTED;
    assert!(require_len(&[0u8; 24], MIN_SIZE_OF_XACT_RUNNING_XACTS, "t").is_ok());
    let e = require_len(&[0u8; 8], MIN_SIZE_OF_XACT_RUNNING_XACTS, "t").err().unwrap();
    assert_eq!(e.sqlstate(), ERRCODE_DATA_CORRUPTED);
}

static FROM_STREAM: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

#[test]
fn standby_limit_time_matches_c_arithmetic() {
    use crate::recovery::test_support as ts;
    use std::sync::atomic::Ordering::Relaxed;
    xlogrecovery_seams::get_xlog_receipt_time::set(|| (1_000_000, FROM_STREAM.load(Relaxed)));

    ts::set_delay_gucs(7_000, 30_000);
    assert_eq!(ts::standby_limit_time(), 1_000_000 + 30_000i64 * 1000);

    // fromStream=false picks the archive delay.
    FROM_STREAM.store(false, Relaxed);
    assert_eq!(ts::standby_limit_time(), 1_000_000 + 7_000i64 * 1000);

    // -1 = wait forever = 0 sentinel.
    ts::set_delay_gucs(-1, -1);
    assert_eq!(ts::standby_limit_time(), 0);
}
