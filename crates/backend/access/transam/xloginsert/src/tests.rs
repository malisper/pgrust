//! Tests for the WAL-record assembler. The register/begin/insert path needs the
//! xlog / xact / bufmgr / origin seams installed; the tests install lightweight
//! fakes for exactly the seams the exercised path touches.

use super::*;

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

static REDO: AtomicU64 = AtomicU64::new(0);
static LAST_INSERT_RDATA_LEN: AtomicU64 = AtomicU64::new(0);

/// Install fakes for the outward seams the begin/register/assemble/insert path
/// uses. The `xlog_insert_record` boundary records the assembled record length
/// and returns a fixed end LSN.
fn install_fakes() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        xlog_seam::xlog_insert_allowed::set(|| true);
        xlog_seam::get_full_page_write_info::set(|| (REDO.load(Ordering::Relaxed), true));
        xlog_seam::get_redo_rec_ptr::set(|| REDO.load(Ordering::Relaxed));
        xlog_seam::wal_compression::set(|| WAL_COMPRESSION_NONE);
        xlog_seam::wal_consistency_checking::set(|_rmid| false);
        xlog_seam::xlog_insert_record::set(|rdata, _fpw, _flags, _nfpi, _topxid| {
            let total: usize = rdata.iter().map(|s| s.len()).sum();
            LAST_INSERT_RDATA_LEN.store(total as u64, Ordering::Relaxed);
            Ok(0x1_0000_u64)
        });

        xact_seam::is_subxact_top_xid_log_pending::set(|| false);
        xact_seam::get_top_transaction_id_if_any::set(|| 0);
        xact_seam::get_current_transaction_id_if_any::set(|| 0);

        origin_seam::replorigin_session_origin::set(|| 0);

        miscinit_seam::is_bootstrap_processing_mode::set(|| false);
    });
}

/// Reset the backend-local working area between tests (each test thread gets its
/// own thread-local, but tests on the same thread must not bleed state).
fn reset_state() {
    XLOG_INSERT_STATE.with(|cell| {
        *cell.borrow_mut() = Some(new_insert_state());
    });
}

#[test]
fn header_scratch_size_matches_c() {
    // SizeOfXLogRecord(24) + MaxSizeOfXLogRecordBlockHeader(27) * 33
    //   + SizeOfXLogRecordDataHeaderLong(5) + SizeOfXlogOrigin(3)
    //   + SizeOfXLogTransactionId(5).
    assert_eq!(MAX_SIZE_OF_XLOG_RECORD_BLOCK_HEADER, 27);
    assert_eq!(SIZE_OF_XLOG_RECORD, 24);
    assert_eq!(HEADER_SCRATCH_SIZE, 24 + 27 * 33 + 5 + 3 + 5);
}

#[test]
fn page_lsn_roundtrip() {
    let mut page = vec![0u8; BLCKSZ];
    let lsn: XLogRecPtr = 0x0123_4567_89AB_CDEF;
    PageSetLSN(&mut page, lsn);
    assert_eq!(PageGetLSN(&page), lsn);
    // pd_lsn is stored as xlogid(high)@0, xrecoff(low)@4.
    assert_eq!(
        u32::from_ne_bytes(page[0..4].try_into().unwrap()),
        (lsn >> 32) as u32
    );
    assert_eq!(
        u32::from_ne_bytes(page[4..8].try_into().unwrap()),
        (lsn & 0xFFFF_FFFF) as u32
    );
}

#[test]
fn rel_file_locator_byte_layout() {
    let rl = RelFileLocator {
        spcOid: 0x1111_2222,
        dbOid: 0x3333_4444,
        relNumber: 0x5555_6666,
    };
    let b = rel_file_locator_bytes(&rl);
    assert_eq!(u32::from_ne_bytes(b[0..4].try_into().unwrap()), 0x1111_2222);
    assert_eq!(u32::from_ne_bytes(b[4..8].try_into().unwrap()), 0x3333_4444);
    assert_eq!(u32::from_ne_bytes(b[8..12].try_into().unwrap()), 0x5555_6666);
}

#[test]
fn page_is_new_predicate() {
    let mut page = vec![0u8; BLCKSZ];
    assert!(PageIsNew(&page)); // pd_upper == 0
    // pd_upper @14
    page[14..16].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
    assert!(!PageIsNew(&page));
}

#[test]
fn begin_then_data_only_record_assembles_and_inserts() {
    install_fakes();
    reset_state();

    // A pure main-data record (no buffers): begin, register two data chunks,
    // insert. The assembled record is header + short-data-header + the two
    // chunks; xlog_insert_record records the total fragment length.
    let recptr = xlog_insert(
        RM_XLOG_ID,
        0,
        0,
        &[b"hello".as_slice(), b"world".as_slice()],
    )
    .expect("insert");
    assert_eq!(recptr, 0x1_0000);

    // The record was reset after insertion.
    let pending = XLOG_INSERT_STATE.with(|c| c.borrow().as_ref().unwrap().begininsert_called);
    assert!(!pending);

    // The inserted record carried the 10 data bytes (plus header + data
    // sub-header), so the total exceeds 10 and is at least SizeOfXLogRecord+10.
    let total = LAST_INSERT_RDATA_LEN.load(Ordering::Relaxed);
    assert!(total >= (SIZE_OF_XLOG_RECORD as u64) + 10);
}

#[test]
fn double_begin_is_an_error() {
    install_fakes();
    reset_state();
    XLogBeginInsert().expect("first begin");
    let err = XLogBeginInsert();
    assert!(err.is_err(), "second XLogBeginInsert must error");
    XLogResetInsertion();
}

#[test]
fn insert_without_begin_is_an_error() {
    install_fakes();
    reset_state();
    let err = XLogInsert(RM_XLOG_ID, 0);
    assert!(err.is_err(), "XLogInsert without begin must error");
}

#[test]
fn invalid_info_mask_rejected() {
    install_fakes();
    reset_state();
    XLogBeginInsert().expect("begin");
    // 0x04 is a reserved (low-nibble, non-special) bit.
    let err = XLogInsert(RM_XLOG_ID, 0x04);
    assert!(err.is_err(), "reserved info bits must be rejected");
    XLogResetInsertion();
}

#[test]
fn ensure_record_space_grows_buffers() {
    install_fakes();
    reset_state();
    // Default cap is XLR_NORMAL_MAX_BLOCK_ID + 1 = 5 buffers. Grow to 10.
    XLogEnsureRecordSpace(9, 0).expect("grow");
    let n = XLOG_INSERT_STATE.with(|c| c.borrow().as_ref().unwrap().max_registered_buffers());
    assert_eq!(n, 10);
}

#[test]
fn ensure_record_space_rejects_too_many_blocks() {
    install_fakes();
    reset_state();
    let err = XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID + 1, 0);
    assert!(err.is_err());
}
