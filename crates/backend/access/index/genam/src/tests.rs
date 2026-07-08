use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{Oid, BTREE_AM_OID, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_error::{PgResult, ERRCODE_TRANSACTION_ROLLBACK};
use types_nbtree::BTScanOpaqueData;
use types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use types_scan::scankey::ScanKeyData;
use types_snapshot::{SnapshotData, SNAPSHOT_MVCC};
use types_tuple::{NameData, TupleDescData};

use crate::*;

const TBL: Oid = 1259;
const IDX: Oid = 2662;
const HEAP_AM: Oid = 2;

thread_local! {
    static REGISTERED: Cell<u32> = const { Cell::new(0) };
    static UNREGISTERED: Cell<u32> = const { Cell::new(0) };
    static XID_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
    static XID_COMMITTED: Cell<bool> = const { Cell::new(false) };
}

fn static_snapshot() -> Rc<SnapshotData<'static>> {
    thread_local! {
        static SNAP_CX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("test snapshots")));
    }
    SNAP_CX.with(|cx| Rc::new(SnapshotData::sentinel(cx.mcx(), SNAPSHOT_MVCC)))
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        relation_seams::relation_open::set(fake_relation_open);
        snapmgr_seams::get_catalog_snapshot::set(|_relid| Ok(static_snapshot()));
        snapmgr_seams::register_snapshot::set(|s| {
            REGISTERED.with(|c| c.set(c.get() + 1));
            Ok(s)
        });
        snapmgr_seams::unregister_snapshot::set(|_s| {
            UNREGISTERED.with(|c| c.set(c.get() + 1));
        });
        procarray_seams::transaction_id_is_in_progress::set(|_| {
            Ok(XID_IN_PROGRESS.with(Cell::get))
        });
        transam_seams::transaction_id_did_commit::set(|_| Ok(XID_COMMITTED.with(Cell::get)));
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        // BTORDER_PROC lookup for the live btree read path: btint4cmp.
        syscache_seams::lookup_pg_amproc::set(|_, _, _, _| Ok(351));
        crate::init_seams();
    });
}

fn make<'mcx>(mcx: Mcx<'mcx>, oid: Oid, name: &str, relkind: u8, relam: Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    let rd_index = (relkind == RELKIND_INDEX).then(|| {
        let mut indkey = PgVec::new_in(mcx);
        // Index columns 1..=2 cover heap attributes (3, 1).
        indkey.push(3);
        indkey.push(1);
        FormData_pg_index {
            indexrelid: oid,
            indrelid: TBL,
            indnatts: 2,
            indnkeyatts: 2,
            indisunique: true,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisvalid: true,
            indisready: true,
            indkey,
            has_indpred: false,
        indexprs_src: None,
        indpred_src: None,
        }
    });
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
            relnamespace: 11,
            reltype: 0,
            relowner: 10,
            relam,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: relkind == RELKIND_RELATION,
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
        rd_index,
        rd_opcintype: two_col_vec(mcx, relkind, 23),
        rd_opfamily: two_col_vec(mcx, relkind, 1976),
        rd_indoption: two_col_vec(mcx, relkind, 0i16),
        rd_indcollation: two_col_vec(mcx, relkind, 0),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: two_col_vec(mcx, relkind, 0),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    Relation::open(data, Some(record_close))
}

fn two_col_vec<T: Copy>(mcx: Mcx<'_>, relkind: u8, v: T) -> PgVec<'_, T> {
    let mut vec = PgVec::new_in(mcx);
    if relkind == RELKIND_INDEX {
        vec.push(v);
        vec.push(v);
    }
    vec
}

fn record_close(_oid: Oid, _lockmode: LOCKMODE) -> PgResult<()> {
    Ok(())
}

fn fake_relation_open(mcx: Mcx<'_>, oid: Oid, _lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    match oid {
        TBL => Ok(make(mcx, TBL, "pg_class", RELKIND_RELATION, HEAP_AM)),
        IDX => Ok(make(mcx, IDX, "pg_class_oid_index", RELKIND_INDEX, BTREE_AM_OID)),
        _ => panic!("unexpected relation {oid}"),
    }
}

fn key_on(attno: i16) -> ScanKeyData {
    let mut k = ScanKeyData::empty();
    k.sk_attno = attno;
    k.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    k
}

#[test]
fn relation_get_index_scan_matches_c_init() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();
    let idx = make(mcx, IDX, "idx", RELKIND_INDEX, BTREE_AM_OID);

    let opaque = IndexScanOpaque::Btree(BTScanOpaqueData::alloc_in(mcx).unwrap());
    let scan = RelationGetIndexScan(mcx, &idx, 2, 1, opaque).unwrap();

    assert!(scan.heapRelation.is_none());
    assert!(scan.xs_snapshot.is_none());
    assert_eq!(scan.numberOfKeys, 2);
    assert_eq!(scan.numberOfOrderBys, 1);
    assert_eq!(scan.keyData.capacity() >= 2, true);
    assert!(!scan.xs_want_itup);
    assert!(!scan.kill_prior_tuple);
    assert!(!scan.xactStartedInRecovery);
    assert!(scan.ignore_killed_tuples);
    assert!(!scan.xs_recheck);
    assert!(scan.xs_heapfetch.is_none());
    assert_eq!(scan.index_rel().rd_id, IDX);
}

#[test]
fn convert_scan_keys_maps_heap_attnos_to_index_columns() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();
    let idx = make(mcx, IDX, "idx", RELKIND_INDEX, BTREE_AM_OID);

    // Heap attnos (3, 1) are index columns (1, 2).
    let keys = [key_on(1), key_on(3)];
    let idxkey = convert_scan_keys(mcx, &idx, &keys).unwrap();
    assert_eq!(idxkey.len(), 2);
    assert_eq!(idxkey[0].sk_attno, 2);
    assert_eq!(idxkey[1].sk_attno, 1);
}

#[test]
fn convert_scan_keys_rejects_non_index_column() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();
    let idx = make(mcx, IDX, "idx", RELKIND_INDEX, BTREE_AM_OID);

    let Err(err) = convert_scan_keys(mcx, &idx, &[key_on(7)]) else {
        panic!("expected error");
    };
    assert!(err.message().contains("column is not in index"));
}

#[test]
fn setup_snapshot_registers_catalog_snapshot_only_when_caller_passes_none() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();

    let before = REGISTERED.with(Cell::get);
    let (_snap, registered) = setup_snapshot(mcx, TBL, None).unwrap();
    assert!(registered.is_some());
    assert_eq!(REGISTERED.with(Cell::get), before + 1);

    let own = static_snapshot();
    let (snap, registered) = setup_snapshot(mcx, TBL, Some(own.clone())).unwrap();
    assert!(registered.is_none());
    assert!(Rc::ptr_eq(&snap, &own));
    assert_eq!(REGISTERED.with(Cell::get), before + 1);
}

#[test]
fn concurrent_abort_error_shape() {
    install();
    // Invalid xid: no probe, no error.
    xact::SetCheckXidAlive(0);
    assert!(handle_concurrent_abort().is_ok());

    xact::SetCheckXidAlive(1000);
    XID_IN_PROGRESS.with(|c| c.set(true));
    XID_COMMITTED.with(|c| c.set(false));
    assert!(handle_concurrent_abort().is_ok());

    XID_IN_PROGRESS.with(|c| c.set(false));
    XID_COMMITTED.with(|c| c.set(true));
    assert!(handle_concurrent_abort().is_ok());

    XID_COMMITTED.with(|c| c.set(false));
    let err = handle_concurrent_abort().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_TRANSACTION_ROLLBACK);
    assert!(err
        .message()
        .contains("transaction aborted during system catalog scan"));
    xact::SetCheckXidAlive(0);
}

// Both scan arms end at the correct layer: the heap arm runs the real heapam
// read lane and stops at the first backend seam this crate leaves uninstalled
// (predicate locking), the index arm at nbtree's btbeginscan.
#[test]
#[should_panic(expected = "seam not installed: predicate_seams::predicate_lock_relation")]
fn beginscan_heap_arm_runs_real_read_lane() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();
    let tbl = make(mcx, TBL, "pg_class", RELKIND_RELATION, HEAP_AM);
    let _ = systable_beginscan(mcx, &tbl, IDX, false, Some(static_snapshot()), &[key_on(1)]);
}

#[test]
fn beginscan_index_arm_reaches_btree() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();
    let tbl = make(mcx, TBL, "pg_class", RELKIND_RELATION, HEAP_AM);
    let _ = systable_beginscan(mcx, &tbl, IDX, true, Some(static_snapshot()), &[key_on(1)]);
}

// The btree read path is live: a leading-column scan now stops at the
// uninstalled bufmgr seam instead of an nbtree dispatch panic. (Heap attr 3
// remaps to index column 1; heap attr 1 alone would be a skip scan, phase 2.)
#[test]
#[should_panic(expected = "read_buffer")]
fn seam_scan_reaches_the_buffer_layer() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();
    let tbl = make(mcx, TBL, "pg_class", RELKIND_RELATION, HEAP_AM);
    let mut consume = |_: &HeapTupleData<'_>| Ok(true);
    let _ = genam_seams::systable_scan_catalog::call(&tbl, IDX, true, &[key_on(3)], &mut consume);
}

#[test]
fn ignore_system_indexes_forces_the_heap_arm() {
    install();
    let cx = MemoryContext::new("test");
    let mcx = cx.mcx();
    let tbl = make(mcx, TBL, "pg_class", RELKIND_RELATION, HEAP_AM);

    miscinit::SetIgnoreSystemIndexes(true);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ =
            systable_beginscan(mcx, &tbl, IDX, true, Some(static_snapshot()), &[key_on(1)]);
    }));
    miscinit::SetIgnoreSystemIndexes(false);

    let err = r.unwrap_err();
    // Forced heap arm now runs real heap_beginscan; it stops at the first
    // uninstalled backend seam rather than an unported-handler panic.
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&'static str>().copied())
        .unwrap();
    assert!(msg.contains("predicate_lock_relation"), "{msg}");
}
