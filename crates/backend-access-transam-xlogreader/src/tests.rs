//! Unit tests for the xlogreader decode core.

use super::*;
use mcx::MemoryContext;
use types_wal::rmgr::XLogReaderState;

/// Build a reader wired to a context arena, with a fixed segment size.
fn reader_in<'mcx>(arena: mcx::Mcx<'mcx>, segsize: i32) -> XLogReaderState<'mcx> {
    let mut state = XLogReaderState {
        decode_arena: Some(arena),
        ..Default::default()
    };
    state.segcxt.ws_segsize = segsize;
    state
}

#[test]
fn required_space_matches_c_formula() {
    // Spot-check DecodeXLogRecordRequiredSpace is monotone and includes the
    // fixed + per-block + padding terms.
    let a = DecodeXLogRecordRequiredSpace(0);
    let b = DecodeXLogRecordRequiredSpace(1000);
    assert!(b == a + 1000);
    assert!(a >= SIZEOF_DECODED_XLOG_RECORD_FIXED);
}

#[test]
fn maxalign_rounds_up_to_8() {
    assert_eq!(MAXALIGN(0), 0);
    assert_eq!(MAXALIGN(1), 8);
    assert_eq!(MAXALIGN(8), 8);
    assert_eq!(MAXALIGN(9), 16);
}

#[test]
fn page_header_size_short_vs_long() {
    assert_eq!(XLogPageHeaderSize(0), SIZE_OF_XLOG_SHORT_PHD);
    assert_eq!(XLogPageHeaderSize(XLP_LONG_HEADER), SIZE_OF_XLOG_LONG_PHD);
}

#[test]
fn begin_read_resets_cursors() {
    let cx = MemoryContext::new("xlogreader-test");
    let mut state = reader_in(cx.mcx(), 16 * 1024 * 1024);
    XLogBeginRead(&mut state, 0x1000);
    assert_eq!(state.EndRecPtr, 0x1000);
    assert_eq!(state.NextRecPtr, 0x1000);
    assert_eq!(state.ReadRecPtr, InvalidXLogRecPtr);
    assert_eq!(state.DecodeRecPtr, InvalidXLogRecPtr);
}

#[test]
fn empty_queue_has_no_record_or_error() {
    let cx = MemoryContext::new("xlogreader-test");
    let state = reader_in(cx.mcx(), 16 * 1024 * 1024);
    assert!(!XLogReaderHasQueuedRecordOrError(&state));
    assert_eq!(decode_queue_head_lsn(&state), None);
    assert_eq!(decode_queue_tail_lsn(&state), None);
}

#[test]
fn release_previous_with_no_record_is_invalid() {
    let cx = MemoryContext::new("xlogreader-test");
    let mut state = reader_in(cx.mcx(), 16 * 1024 * 1024);
    assert_eq!(XLogReleasePreviousRecord(&mut state), InvalidXLogRecPtr);
}

#[test]
fn rel_file_locator_roundtrips_wire_bytes() {
    let mut b = [0u8; 12];
    b[0..4].copy_from_slice(&1234u32.to_ne_bytes());
    b[4..8].copy_from_slice(&5678u32.to_ne_bytes());
    b[8..12].copy_from_slice(&9012u32.to_ne_bytes());
    let loc = parse_rel_file_locator(&b);
    assert_eq!(loc.spc_oid(), 1234);
    assert_eq!(loc.db_oid(), 5678);
    assert_eq!(loc.rel_number(), 9012);
}

#[test]
fn validate_page_header_rejects_bad_magic() {
    let cx = MemoryContext::new("xlogreader-test");
    let mut state = reader_in(cx.mcx(), 16 * 1024 * 1024);
    // A zeroed page has magic 0 != XLOG_PAGE_MAGIC.
    let page = alloc::vec![0u8; XLOG_BLCKSZ];
    assert!(!XLogReaderValidatePageHeader(&mut state, 0, &page));
}

// --- Handle-based logical-decoding registry (handle.rs) ---

use std::sync::Once;
use types_logical::XLogReaderRoutineHandle;
use types_wal::rmgr::XLogReaderRoutine;

/// Install a routine resolver for the handle tests (stands in for xlogutils'
/// routine, which is the downstream contract keystone). All-None is fine: the
/// allocate/begin_read/end_rec_ptr/free paths don't drive the page-read
/// callback.
fn install_test_routine_resolver() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        backend_access_transam_xlogreader_seams::xlog_reader_routine_for_handle::set(
            |_h: XLogReaderRoutineHandle| XLogReaderRoutine::default(),
        );
    });
}

#[test]
fn handle_allocate_begin_read_end_rec_ptr_free() {
    install_test_routine_resolver();

    let h = handle::XLogReaderAllocate(16 * 1024 * 1024, XLogReaderRoutineHandle::default())
        .expect("allocate");
    assert_ne!(h.0, 0, "handle is never NULL");

    // EndRecPtr starts at zero (the C all-zero reader).
    assert_eq!(handle::reader_EndRecPtr(h), 0);

    // XLogBeginRead positions EndRecPtr at the requested LSN.
    handle::XLogBeginRead(h, 0x4000);
    assert_eq!(handle::reader_EndRecPtr(h), 0x4000);

    // Free reclaims the slot; a fresh allocation reuses the freed index.
    handle::XLogReaderFree(h);
    let h2 = handle::XLogReaderAllocate(16 * 1024 * 1024, XLogReaderRoutineHandle::default())
        .expect("re-allocate");
    assert_eq!(h2.0, h.0, "freed slot is reused");
    handle::XLogReaderFree(h2);
}

#[test]
fn handle_double_free_is_noop() {
    install_test_routine_resolver();
    let h = handle::XLogReaderAllocate(16 * 1024 * 1024, XLogReaderRoutineHandle::default())
        .expect("allocate");
    handle::XLogReaderFree(h);
    // Second free of the same (now-empty) slot must not panic.
    handle::XLogReaderFree(h);
}

#[test]
fn arena_copy_borrows_arena_not_caller() {
    let cx = MemoryContext::new("xlogreader-test");
    let arena = cx.mcx();
    let src = [1u8, 2, 3, 4];
    let s = arena_copy(arena, &src).unwrap();
    assert_eq!(s, &[1, 2, 3, 4]);
    // empty input yields an empty slice without allocating.
    assert_eq!(arena_copy(arena, &[]).unwrap(), &[] as &[u8]);
}
