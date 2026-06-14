//! Unit tests for the `heapam_visibility.c` port.
//!
//! The seam-free paths (`SNAPSHOT_ANY`, the pure `TransactionId*` predicates,
//! `TransactionIdInArray` / `pg_lfind32`, the `XidInMVCCSnapshot` range
//! short-circuits, and the infomask-only `HeapTupleIsSurelyDead` /
//! `HeapTupleHeaderIsOnlyLocked` fast paths) are tested directly. Seam-driven
//! paths panic until their owners land, so they are not exercised here.

extern crate alloc;

use super::*;
use alloc::vec;
use alloc::vec::Vec;
use alloc::vec;
use mcx::MemoryContext;
use types_snapshot::snapshot::{GlobalVisStateHandle, SnapshotData, SnapshotType};
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleField3, HeapTupleFields, HeapTupleHeaderChoice, HeapTupleHeaderData,
    ItemPointerData,
};

fn header<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    infomask: u16,
    xmin: TransactionId,
    xmax: TransactionId,
) -> HeapTupleHeaderData<'mcx> {
    HeapTupleHeaderData {
        t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
            t_xmin: xmin,
            t_xmax: xmax,
            t_field3: HeapTupleField3::TCid(0),
        }),
        t_ctid: ItemPointerData::new(0, 1),
        t_infomask2: 0,
        t_infomask: infomask,
        t_hoff: 0,
        t_bits: mcx::PgVec::new_in(mcx),
    }
}

fn tuple<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    infomask: u16,
    xmin: TransactionId,
    xmax: TransactionId,
) -> HeapTupleData<'mcx> {
    HeapTupleData {
        t_len: 32,
        t_self: ItemPointerData::new(0, 1),
        t_tableOid: 12345,
        t_data: Some(mcx::alloc_in(mcx, header(mcx, infomask, xmin, xmax)).unwrap()),
    }
}

fn empty_snapshot(ty: SnapshotType) -> SnapshotData {
    let mut s = SnapshotData::sentinel(ty);
    s.active_count = 1;
    s
}

#[test]
fn satisfies_any_is_always_visible() {
    let ctx = MemoryContext::new("test");
    let mut t = tuple(ctx.mcx(), 0, 100, 0);
    assert!(HeapTupleSatisfiesAny(&mut t, 0).unwrap());
    let mut snap = empty_snapshot(SnapshotType::SNAPSHOT_ANY);
    assert!(HeapTupleSatisfiesVisibility(&mut t, &mut snap, 0).unwrap());
}

#[test]
fn transam_predicates_modulo_2_32() {
    assert!(TransactionIdPrecedes(5, 6));
    assert!(!TransactionIdPrecedes(6, 5));
    assert!(TransactionIdFollowsOrEquals(6, 5));
    assert!(TransactionIdFollowsOrEquals(5, 5));
    // A very large xid "precedes" a small one due to modulo-2^32 wraparound.
    assert!(TransactionIdPrecedes(0xFFFF_FFF0, 3 + 5));
    // Bootstrap/frozen (non-normal) xids use plain unsigned compare.
    assert!(TransactionIdPrecedes(0, 3));
    assert!(!TransactionIdPrecedes(3, 2));
}

#[test]
fn transaction_id_in_array_binary_search() {
    let xip = vec![10u32, 20, 30, 40, 50];
    assert!(TransactionIdInArray(30, &xip, 5));
    assert!(!TransactionIdInArray(35, &xip, 5));
    // Respects the `num` bound: 50 is past the first 4 entries.
    assert!(!TransactionIdInArray(50, &xip, 4));
    assert!(TransactionIdInArray(40, &xip, 4));
    // Empty.
    assert!(!TransactionIdInArray(10, &xip, 0));
    assert!(!TransactionIdInArray(10, &[], 0));
}

#[test]
fn pg_lfind32_linear_search() {
    let xs = vec![7u32, 1, 9, 4];
    assert!(pg_lfind32(9, &xs, 4));
    assert!(!pg_lfind32(2, &xs, 4));
    // nelem bounds the scan.
    assert!(!pg_lfind32(4, &xs, 3));
    assert!(pg_lfind32(9, &xs, 3));
    assert!(!pg_lfind32(7, &xs, 0));
}

#[test]
fn xid_in_mvcc_snapshot_range_short_circuits() {
    let mut snap = empty_snapshot(SnapshotType::SNAPSHOT_MVCC);
    snap.xmin = 100;
    snap.xmax = 200;
    // xid < xmin -> not in progress (no seam consulted).
    assert!(!XidInMVCCSnapshot(50, &snap).unwrap());
    // xid >= xmax -> in progress (no seam consulted).
    assert!(XidInMVCCSnapshot(200, &snap).unwrap());
    assert!(XidInMVCCSnapshot(250, &snap).unwrap());
    // In [xmin, xmax) and present in subxip (full data, not overflowed).
    snap.subxip = vec![150];
    snap.subxcnt = 1;
    assert!(XidInMVCCSnapshot(150, &snap).unwrap());
    // In range but absent from both arrays -> not in progress.
    assert!(!XidInMVCCSnapshot(160, &snap).unwrap());
}

#[test]
fn surely_dead_fast_paths_need_no_clog() {
    let ctx = MemoryContext::new("test");
    let none = GlobalVisStateHandle::new(0);

    // xmin not committed, marked invalid -> definitely dead.
    let t = tuple(ctx.mcx(), HEAP_XMIN_INVALID, 100, 0);
    assert!(HeapTupleIsSurelyDead(&t, none).unwrap());

    // xmin committed, xmax invalid -> alive.
    let t = tuple(ctx.mcx(), HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID, 100, 0);
    assert!(!HeapTupleIsSurelyDead(&t, none).unwrap());

    // xmin committed, xmax is a multi -> can't know cheaply -> not surely dead.
    let t = tuple(ctx.mcx(), HEAP_XMIN_COMMITTED | HEAP_XMAX_IS_MULTI, 100, 5);
    assert!(!HeapTupleIsSurelyDead(&t, none).unwrap());

    // xmin committed, xmax present but not known committed -> not surely dead.
    let t = tuple(ctx.mcx(), HEAP_XMIN_COMMITTED, 100, 5);
    assert!(!HeapTupleIsSurelyDead(&t, none).unwrap());

    // xmin not committed, not invalid -> assume alive (false).
    let t = tuple(ctx.mcx(), 0, 100, 0);
    assert!(!HeapTupleIsSurelyDead(&t, none).unwrap());
}

#[test]
fn header_is_only_locked_infomask_fast_paths() {
    let ctx = MemoryContext::new("test");
    // No valid xmax -> only locked.
    assert!(HeapTupleHeaderIsOnlyLocked(&header(ctx.mcx(), HEAP_XMAX_INVALID, 0, 0)).unwrap());
    // LOCK_ONLY hint -> only locked.
    assert!(HeapTupleHeaderIsOnlyLocked(&header(ctx.mcx(), HEAP_XMAX_LOCK_ONLY, 0, 5)).unwrap());
    // xmax == 0 (invalid) -> only locked.
    assert!(HeapTupleHeaderIsOnlyLocked(&header(ctx.mcx(), 0, 0, 0)).unwrap());
    // Valid xmax, not lock-only, not multi -> must have been updated.
    assert!(!HeapTupleHeaderIsOnlyLocked(&header(ctx.mcx(), 0, 0, 5)).unwrap());
}
