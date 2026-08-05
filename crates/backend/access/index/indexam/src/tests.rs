use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{Oid, BTREE_AM_OID, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_rel::{
    AccessShareLock, FormData_pg_class, LockInfoData, LockRelId, NoLock, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use types_scan::sdir::ForwardScanDirection;
use types_slot::{BufferHeapTupleTableSlot, HeapTupleTableSlot, SlotData, TupleSlotKind, TupleTableSlot};
use types_snapshot::{SnapshotData, SNAPSHOT_MVCC};
use types_tuple::itemptr::ItemPointerData;
use types_tuple::{NameData, TupleDescData};

use crate::*;

const TBL: Oid = 1;
const IDX: Oid = 2;
const PIDX: Oid = 3;

thread_local! {
    static LAST_CLOSED: Cell<(Oid, LOCKMODE)> = const { Cell::new((0, -1)) };
}

fn record_close(oid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    LAST_CLOSED.set((oid, lockmode));
    Ok(())
}

fn make<'mcx>(mcx: Mcx<'mcx>, oid: Oid, name: &str, relkind: u8, relam: Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: oid, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: Rc::new(TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: PgVec::new_in(mcx),
            attrs: PgVec::new_in(mcx),
        }),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    Relation::open(data, Some(record_close))
}

fn fake_relation_open(mcx: Mcx<'_>, oid: Oid, _lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    match oid {
        TBL => Ok(make(mcx, oid, "tbl", RELKIND_RELATION, 2)),
        IDX => Ok(make(mcx, oid, "idx", RELKIND_INDEX, MOCK_AM_OID)),
        PIDX => Ok(make(mcx, oid, "pidx", RELKIND_PARTITIONED_INDEX, BTREE_AM_OID)),
        _ => Err(PgError::error(format!("relation {oid} does not exist")).into()),
    }
}

fn fake_try_relation_open(
    mcx: Mcx<'_>,
    oid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'_>>> {
    match oid {
        TBL | IDX | PIDX => fake_relation_open(mcx, oid, lockmode).map(Some),
        _ => Ok(None),
    }
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        relation_seams::relation_open::set(fake_relation_open);
        relation_seams::try_relation_open::set(fake_try_relation_open);
        crate::init_seams();
    });
}

fn tid(blk: u32, pos: u16) -> ItemPointerData {
    ItemPointerData::new(blk, pos)
}

fn snapshot(mcx: Mcx<'_>) -> Rc<SnapshotData<'_>> {
    Rc::new(SnapshotData::sentinel(mcx, SNAPSHOT_MVCC))
}

fn scan_pair<'mcx>(
    mcx: Mcx<'mcx>,
) -> (Relation<'mcx>, Relation<'mcx>, IndexScanDescData<'mcx>) {
    let heap = make(mcx, TBL, "tbl", RELKIND_RELATION, 2);
    let idx = make(mcx, IDX, "idx", RELKIND_INDEX, MOCK_AM_OID);
    let scan = index_beginscan(mcx, &heap, &idx, snapshot(mcx), 1, 0).unwrap();
    (heap, idx, scan)
}

fn mock<'a>(scan: &'a mut IndexScanDescData<'_>) -> &'a mut MockOpaque {
    let IndexScanOpaque::Mock(m) = &mut scan.opaque else {
        unreachable!()
    };
    m
}

#[test]
fn open_accepts_indexes_rejects_tables() {
    install();
    let ctx = MemoryContext::new("t");
    let r = index_open(ctx.mcx(), IDX, AccessShareLock).unwrap();
    assert_eq!(r.name(), "idx");
    index_close(r, AccessShareLock).unwrap();
    assert_eq!(LAST_CLOSED.get(), (IDX, AccessShareLock));

    assert!(index_open(ctx.mcx(), PIDX, AccessShareLock).is_ok());

    let err = index_open(ctx.mcx(), TBL, AccessShareLock).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    assert_eq!(err.message, "\"tbl\" is not an index");
}

#[test]
fn try_open_missing_is_none_wrong_kind_errors() {
    install();
    let ctx = MemoryContext::new("t");
    assert!(try_index_open(ctx.mcx(), 999, AccessShareLock).unwrap().is_none());
    assert!(try_index_open(ctx.mcx(), IDX, AccessShareLock).unwrap().is_some());
    assert!(try_index_open(ctx.mcx(), TBL, AccessShareLock).is_err());
}

#[test]
fn seams_installed_by_init() {
    install();
    assert!(indexam_seams::index_open::is_installed());
    assert!(indexam_seams::try_index_open::is_installed());
    let ctx = MemoryContext::new("t");
    let r = indexam_seams::index_open::call(ctx.mcx(), IDX, NoLock).unwrap();
    assert_eq!(r.rd_id, IDX);
}

#[test]
fn close_lockmode_routes_through_closer() {
    install();
    let ctx = MemoryContext::new("t");
    let r = make(ctx.mcx(), IDX, "idx", RELKIND_INDEX, MOCK_AM_OID);
    index_close(r, NoLock).unwrap();
    assert_eq!(LAST_CLOSED.get(), (IDX, NoLock));
}

#[test]
fn from_relam_resolves_btree() {
    assert_eq!(IndexAmKind::from_relam(BTREE_AM_OID), IndexAmKind::Btree);
}

#[test]
#[should_panic(expected = "unported: index AM 111")]
fn from_relam_panics_on_unknown_am() {
    let _ = IndexAmKind::from_relam(111);
}

#[test]
fn beginscan_arms_scan_and_holds_relcache_ref() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, idx, scan) = scan_pair(ctx.mcx());
    assert_eq!(Rc::strong_count(idx.data_rc()), 2);
    assert!(scan.xs_heapfetch.is_some());
    assert!(scan.xs_snapshot.is_some());
    assert!(!scan.xs_temp_snap);
    assert_eq!(scan.numberOfKeys, 1);
    index_endscan(scan).unwrap();
    assert_eq!(Rc::strong_count(idx.data_rc()), 1);
}

#[test]
fn getnext_tid_sequence_kill_reset_and_exhaustion() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, _idx, mut scan) = scan_pair(ctx.mcx());
    mock(&mut scan).tids = vec![tid(0, 1), tid(0, 2)];

    assert_eq!(
        index_getnext_tid(&mut scan, ForwardScanDirection).unwrap(),
        Some(tid(0, 1))
    );
    assert_eq!(scan.xs_heaptid, tid(0, 1));
    assert!(!scan.kill_prior_tuple);

    // amgettuple must observe the kill flag; this layer resets it after.
    scan.kill_prior_tuple = true;
    assert_eq!(
        index_getnext_tid(&mut scan, ForwardScanDirection).unwrap(),
        Some(tid(0, 2))
    );
    assert!(!scan.kill_prior_tuple);
    assert_eq!(mock(&mut scan).kill_seen, vec![false, true]);

    // Exhaustion resets the heap fetch (buffer-pin release in C).
    assert_eq!(index_getnext_tid(&mut scan, ForwardScanDirection).unwrap(), None);
    assert_eq!(scan.xs_heapfetch.as_ref().unwrap().mock().resets, 1);
}

#[test]
fn pgstat_probe_counts_only_when_enabled() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, idx, mut scan) = scan_pair(ctx.mcx());
    mock(&mut scan).tids = vec![tid(0, 1), tid(0, 2)];

    index_getnext_tid(&mut scan, ForwardScanDirection).unwrap();
    assert_eq!(scan.xs_pgstat_index_tuples, 0);

    idx.pgstat_enabled.set(true);
    index_getnext_tid(&mut scan, ForwardScanDirection).unwrap();
    assert_eq!(scan.xs_pgstat_index_tuples, 1);
}

#[test]
fn fetch_heap_sets_kill_prior_tuple_except_in_recovery() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, _idx, mut scan) = scan_pair(ctx.mcx());
    let mut slot = SlotData::BufferHeap(BufferHeapTupleTableSlot {
        base: HeapTupleTableSlot {
            base: TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::BufferHeapTuple),
            tuple: None,
            off: 0,
            jit_deform: None,
        },
        buffer: types_core::InvalidBuffer,
    });
    scan.xs_heaptid = tid(3, 7);

    scan.xs_heapfetch.as_mut().unwrap().mock_mut().mock_fetch = vec![(true, false, false)];
    assert!(index_fetch_heap(ctx.mcx(), &mut scan, &mut slot).unwrap());
    assert_eq!(slot.base().tts_tid, tid(3, 7));
    assert!(!scan.kill_prior_tuple);

    scan.xs_heapfetch.as_mut().unwrap().mock_mut().mock_fetch = vec![(false, false, true)];
    assert!(!index_fetch_heap(ctx.mcx(), &mut scan, &mut slot).unwrap());
    assert!(scan.kill_prior_tuple);

    scan.xactStartedInRecovery = true;
    scan.kill_prior_tuple = false;
    scan.xs_heapfetch.as_mut().unwrap().mock_mut().mock_fetch = vec![(false, false, true)];
    assert!(!index_fetch_heap(ctx.mcx(), &mut scan, &mut slot).unwrap());
    assert!(!scan.kill_prior_tuple);
}

#[test]
fn getnext_slot_skips_dead_chains_and_propagates_kill() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, _idx, mut scan) = scan_pair(ctx.mcx());
    let mut slot = SlotData::BufferHeap(BufferHeapTupleTableSlot {
        base: HeapTupleTableSlot {
            base: TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::BufferHeapTuple),
            tuple: None,
            off: 0,
            jit_deform: None,
        },
        buffer: types_core::InvalidBuffer,
    });
    mock(&mut scan).tids = vec![tid(0, 1), tid(0, 2)];
    // First TID's chain is all-dead; the second yields a visible tuple.
    scan.xs_heapfetch.as_mut().unwrap().mock_mut().mock_fetch =
        vec![(false, false, true), (true, false, false)];

    assert!(index_getnext_slot(ctx.mcx(), &mut scan, ForwardScanDirection, &mut slot).unwrap());
    assert_eq!(slot.base().tts_tid, tid(0, 2));
    // The second amgettuple saw kill_prior_tuple from the dead chain.
    assert_eq!(mock(&mut scan).kill_seen, vec![false, true]);
}

#[test]
fn getnext_slot_continues_hot_chain_without_new_tid() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, _idx, mut scan) = scan_pair(ctx.mcx());
    let mut slot = SlotData::BufferHeap(BufferHeapTupleTableSlot {
        base: HeapTupleTableSlot {
            base: TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::BufferHeapTuple),
            tuple: None,
            off: 0,
            jit_deform: None,
        },
        buffer: types_core::InvalidBuffer,
    });
    mock(&mut scan).tids = vec![tid(0, 1)];
    scan.xs_heapfetch.as_mut().unwrap().mock_mut().mock_fetch =
        vec![(true, true, false), (true, false, false)];

    assert!(index_getnext_slot(ctx.mcx(), &mut scan, ForwardScanDirection, &mut slot).unwrap());
    assert!(scan.xs_heap_continue);
    assert!(index_getnext_slot(ctx.mcx(), &mut scan, ForwardScanDirection, &mut slot).unwrap());
    assert_eq!(mock(&mut scan).next, 1);
    assert!(!scan.xs_heap_continue);
}

#[test]
fn getnext_slot_exhausted_returns_false() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, _idx, mut scan) = scan_pair(ctx.mcx());
    let mut slot = SlotData::BufferHeap(BufferHeapTupleTableSlot {
        base: HeapTupleTableSlot {
            base: TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::BufferHeapTuple),
            tuple: None,
            off: 0,
            jit_deform: None,
        },
        buffer: types_core::InvalidBuffer,
    });
    assert!(!index_getnext_slot(ctx.mcx(), &mut scan, ForwardScanDirection, &mut slot).unwrap());
}

#[test]
fn rescan_resets_flags_fetch_and_calls_am() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, _idx, mut scan) = scan_pair(ctx.mcx());
    scan.kill_prior_tuple = true;
    scan.xs_heap_continue = true;

    index_rescan(&mut scan, None, None).unwrap();
    assert!(!scan.kill_prior_tuple);
    assert!(!scan.xs_heap_continue);
    assert_eq!(scan.xs_heapfetch.as_ref().unwrap().mock().resets, 1);
    assert_eq!(mock(&mut scan).rescans, 1);
}

#[test]
fn markpos_dispatches_restrpos_reports_missing_procedure() {
    install();
    let ctx = MemoryContext::new("t");
    let (_heap, _idx, mut scan) = scan_pair(ctx.mcx());

    index_markpos(&mut scan).unwrap();
    assert_eq!(mock(&mut scan).markpos_calls, 1);

    let err = index_restrpos(&mut scan).unwrap_err();
    assert_eq!(
        err.message,
        "function \"amrestrpos\" is not defined for index \"idx\""
    );
}

#[test]
fn insert_dispatches_through_am() {
    install();
    let ctx = MemoryContext::new("t");
    let heap = make(ctx.mcx(), TBL, "tbl", RELKIND_RELATION, 2);
    let idx = make(ctx.mcx(), IDX, "idx", RELKIND_INDEX, MOCK_AM_OID);
    let t = tid(1, 1);
    let mut am_cache: Option<Box<dyn core::any::Any>> = None;
    let ok = index_insert(
        ctx.mcx(),
        &idx,
        &[],
        &[],
        &t,
        &heap,
        types_nbtree::UNIQUE_CHECK_NO,
        false,
        &mut am_cache,
    )
    .unwrap();
    assert!(ok);
    index_insert_cleanup(&idx, &mut am_cache).unwrap();
}
