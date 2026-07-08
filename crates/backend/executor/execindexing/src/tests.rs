//! e2e: heap_insert (real heapam) + ExecOpenIndices/ExecInsertIndexTuples
//! (real nbtree btinsert, WAL through the recorder seam) + the committed
//! nodeindexscan path reading the tree back, across forced page splits.

use std::cell::Cell;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::{Mutex, Once};

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, InvalidBlockNumber, InvalidBuffer, Oid, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use ::types_error::ERRCODE_UNIQUE_VIOLATION;
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_META, BTREE_MAGIC, BTREE_VERSION, P_NONE,
    XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_L, XLOG_BTREE_SPLIT_R,
};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::{IndexScan, Plan, Scan};
use ::types_nodes::primnodes::OpExpr;
use ::types_nodes::NodeList;
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    RELKIND_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use ::types_slot::TupleSlotKind;
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::SizeOfPageHeaderData;
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::{NameData, TupleDescData};
use executils::EStateData;
use nodeindexscan::{exec_end_index_scan, exec_index_scan, exec_init_index_scan_rel};

const HEAP_OID: Oid = 61000;
const IDX_OID: Oid = 61001;
const INT4OID: Oid = 23;
const INT4_BTREE_OPFAMILY: Oid = 1976;
const OP_INT4EQ: Oid = 96;
const F_INT4EQ: Oid = 65;
const OP_INT4GT: Oid = 521;
const F_INT4GT: Oid = 147;
const F_BTINT4CMP: Oid = 351;
const INDEX_VAR: i32 = -3;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    tables: HashMap<Oid, Vec<Buffer>>,
    pages: Vec<usize>,
    pins: Vec<i32>,
    wal: Vec<(u8, u8)>, // (rmid, info)
    lsn: u64,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(|| Fake {
        tables: HashMap::new(),
        pages: Vec::new(),
        pins: Vec::new(),
        wal: Vec::new(),
        lsn: 0x1000,
    }))
}

fn add_page(f: &mut Fake, relid: Oid, page: Box<TestPage>) -> Buffer {
    let addr = Box::leak(page).0.as_mut_ptr() as usize;
    f.pages.push(addr);
    f.pins.push(0);
    let buf = f.pages.len() as Buffer;
    f.tables.entry(relid).or_default().push(buf);
    buf
}

fn empty_bt_meta() -> Box<TestPage> {
    let mut p = Box::new(TestPage([0u8; BLCKSZ]));
    let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    p.0[12..14].copy_from_slice(&(SizeOfPageHeaderData as u16).to_ne_bytes());
    p.0[14..16].copy_from_slice(&(special as u16).to_ne_bytes());
    p.0[16..18].copy_from_slice(&(special as u16).to_ne_bytes());
    let metad = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: P_NONE,
        btm_level: 0,
        btm_fastroot: P_NONE,
        btm_fastlevel: 0,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: false, // dedup lane is loud; splits stay live
    };
    // SAFETY: owned page, in-bounds aligned writes.
    unsafe {
        p.0.as_mut_ptr()
            .add(SizeOfPageHeaderData)
            .cast::<BTMetaPageData>()
            .write(metad);
        p.0.as_mut_ptr()
            .add(special)
            .cast::<BTPageOpaqueData>()
            .write(BTPageOpaqueData {
                btpo_prev: P_NONE,
                btpo_next: P_NONE,
                btpo_level: 0,
                btpo_flags: BTP_META,
                btpo_cycleid: 0,
            });
    }
    p
}

fn reset_fixture() {
    with_fake(|f| {
        f.tables.clear();
        f.pages.clear();
        f.pins.clear();
        f.wal.clear();
        add_page(f, IDX_OID, empty_bt_meta());
    });
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        genam_seams::build_index_value_description::set(|_, _, _| Ok(None));
        syscache_seams::pg_namespace_nspname::set(|_| Ok(None));
        bufmgr_seams::read_buffer::set(|rel, block| {
            with_fake(|f| {
                let buf = f.tables[&rel.rd_id][block as usize];
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::release_buffer::set(|buf| {
            with_fake(|f| {
                let p = &mut f.pins[(buf - 1) as usize];
                assert!(*p > 0, "double release of buffer {buf}");
                *p -= 1;
            });
            Ok(())
        });
        bufmgr_seams::release_and_read_buffer::set(|buf, rel, blkno| {
            if buf != InvalidBuffer {
                let same = with_fake(|f| f.tables[&rel.rd_id].get(blkno as usize) == Some(&buf));
                if same {
                    return Ok(buf);
                }
                bufmgr_seams::release_buffer::call(buf)?;
            }
            bufmgr_seams::read_buffer::call(rel, blkno)
        });
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            with_fake(|f| {
                for pages in f.tables.values() {
                    if let Some(i) = pages.iter().position(|b| *b == buf) {
                        return i as BlockNumber;
                    }
                }
                panic!("unknown buffer {buf}")
            })
        });
        bufmgr_seams::buffer_get_page::set(|buf| {
            let addr = with_fake(|f| {
                assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
                f.pages[(buf - 1) as usize]
            });
            NonNull::new(addr as *mut u8).unwrap()
        });
        bufmgr_seams::incr_buffer_ref_count::set(|buf| {
            with_fake(|f| f.pins[(buf - 1) as usize] += 1);
        });
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::conditional_lock_buffer::set(|_buf| Ok(true));
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
        bufmgr_seams::buffer_get_lsn_atomic::set(|_buf| 0x1234);
        bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, _flags, n| {
            assert_eq!(n, 1);
            let buf = with_fake(|f| {
                let buf = add_page(f, rel.rd_id, Box::new(TestPage([0u8; BLCKSZ])));
                f.pins[(buf - 1) as usize] += 1;
                buf
            });
            Ok((buf, 1))
        });
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
            Ok(with_fake(|f| {
                f.tables.get(&rel.rd_id).map_or(0, |v| v.len()) as BlockNumber
            }))
        });
        bufmgr_seams::relation_smgr_locator::set(|rel| ::types_storage::RelFileLocatorBackend {
            locator: ::types_storage::RelFileLocator::new(1663, 5, rel.rd_id),
            backend: INVALID_PROC_NUMBER,
        });
        smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| 0);
        smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
        smgr_seams::smgr_exists::set(|_loc, _fork| Ok(false));

        xloginsert_seams::xlog_insert_record::set(|rmid, info, _flags, _main, _bufs| {
            with_fake(|f| {
                f.wal.push((rmid, info));
                f.lsn += 8;
                Ok(f.lsn)
            })
        });

        xact_seams::get_current_transaction_id::set(|| Ok(10));
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));

        predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
        predicate_seams::predicate_lock_relation::set(|_r, _s| Ok(()));
        predicate_seams::predicate_lock_page::set(|_r, _b, _s| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_r, _t, _s, _x| Ok(()));
        predicate_seams::predicate_lock_page_split::set(|_r, _o, _n| Ok(()));
        pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
        freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
        freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
            Ok(InvalidBlockNumber)
        });
        catalog_seams::is_catalog_relation::set(|_rel| false);

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(|_h, _s, _b| Ok(true));
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(|_h, _s, _b, _m| Ok(true));
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_h, _v| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_h| Ok(false));

        relation_seams::relation_open::set(|mcx, relid, _lockmode| match relid {
            HEAP_OID => Ok(Relation::open(heap_relation_data(mcx), None)),
            IDX_OID => {
                let unique = UNIQUE_IDX.with(Cell::get);
                Ok(Relation::open(index_relation_data(mcx, unique), noop_closer()))
            }
            other => panic!("unknown relation oid {other}"),
        });
        relcache_seams::relation_get_index_list::set(|mcx, relid| {
            assert_eq!(relid, HEAP_OID);
            let mut v = PgVec::new_in(mcx);
            v.push(IDX_OID);
            Ok(v)
        });

        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(::types_tuple::PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: ::types_tuple::TYPALIGN_INT,
                    typstorage: ::types_tuple::TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                16 => Some(::types_tuple::PgTypeShape {
                    typlen: 1,
                    typbyval: true,
                    typalign: b'c' as i8,
                    typstorage: ::types_tuple::TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_amop_by_operator::set(|opno, purpose, opfamily| {
            assert_eq!(purpose, b's');
            assert_eq!(opfamily, INT4_BTREE_OPFAMILY);
            let strategy = match opno {
                OP_INT4EQ => 3,
                OP_INT4GT => 5,
                _ => return Ok(None),
            };
            Ok(Some(syscache_seams::PgAmopShape {
                amopstrategy: strategy,
                amopsortfamily: 0,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
            }))
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
            assert_eq!(
                (opfamily, lefttype, righttype, procnum),
                (INT4_BTREE_OPFAMILY, INT4OID, INT4OID, 1)
            );
            Ok(F_BTINT4CMP)
        });
    });
    test_boot::boot_wal("execindexing");
}

thread_local! {
    static UNIQUE_IDX: Cell<bool> = const { Cell::new(false) };
}

fn noop_closer() -> Option<::types_rel::RelationCloser> {
    fn close(_oid: Oid, _mode: ::types_rel::LOCKMODE) -> ::types_error::PgResult<()> {
        Ok(())
    }
    Some(close)
}

fn int4_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let att = ::types_tuple::FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        atttypmod: -1,
        attlen: 4,
        attbyval: true,
        attalign: ::types_tuple::TYPALIGN_INT,
        attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

fn pg_class(relname: &str, oid: Oid, relam: Oid, relkind: u8, hasindex: bool) -> FormData_pg_class {
    let mut name = NameData::default();
    name.namestrcpy(relname);
    FormData_pg_class {
        relname: name,
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
        relhasindex: hasindex,
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
    }
}

fn heap_relation_data(mcx: Mcx<'_>) -> RelationData<'_> {
    RelationData {
        rd_locator: Cell::new(::types_storage::RelFileLocator::new(1663, 5, HEAP_OID)),
        rd_smgr: Default::default(),
        rd_id: HEAP_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: HEAP_OID, dbId: 5 },
        },
        rd_rel: pg_class("t", HEAP_OID, ::tableam::HEAP_TABLE_AM_OID, RELKIND_RELATION, true),
        rd_att: Rc::new(int4_tupdesc(mcx)),
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
    }
}

fn index_relation_data(mcx: Mcx<'_>, unique: bool) -> RelationData<'_> {
    let one = |v: Oid| {
        let mut vec = PgVec::new_in(mcx);
        vec.push(v);
        vec
    };
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1i16);
    let mut indoption = PgVec::new_in(mcx);
    indoption.push(0i16);
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: IDX_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: IDX_OID, dbId: 5 },
        },
        rd_rel: pg_class("t_idx", IDX_OID, ::types_core::BTREE_AM_OID, RELKIND_INDEX, false),
        rd_att: Rc::new(int4_tupdesc(mcx)),
        rd_index: Some(FormData_pg_index {
            indexrelid: IDX_OID,
            indrelid: HEAP_OID,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: unique,
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
        }),
        rd_opcintype: one(INT4OID),
        rd_opfamily: one(INT4_BTREE_OPFAMILY),
        rd_indoption: indoption,
        rd_indcollation: one(0),
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
    }
}

// heap_insert a row, then run it through ExecInsertIndexTuples via a virtual
// slot carrying the new TID (ExecInsert's shape in nodeModifyTable).
fn insert_row<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    idxstate: &mut crate::ResultRelIndexState<'mcx>,
    val: i32,
) -> ::types_error::PgResult<()> {
    let mut tuple =
        ::heaptuple::heap_form_tuple(mcx, &heap.rd_att, &[Datum::from_i32(val)], &[false])?;
    ::heapam::heap_insert(heap, tuple.as_tuple_mut(), 0, 0, None)?;

    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::Virtual,
        Some(heap.rd_att.clone()),
    );
    slot.base_mut().tts_values[0] = Datum::from_i32(val);
    slot.base_mut().tts_isnull[0] = false;
    exectuples::exec_store_virtual_tuple(&mut slot);
    slot.base_mut().tts_tid = tuple.as_tuple_mut().t_self;
    slot.base_mut().tts_tableOid = HEAP_OID;

    crate::ExecInsertIndexTuples(mcx, mcx, idxstate, heap, &mut slot, false, None, &[], false).map(|_| ())
}

fn static_mvcc_snapshot() -> Rc<SnapshotData<'static>> {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap-test")));
    Rc::new(SnapshotData::sentinel(ctx.mcx(), SnapshotType::SNAPSHOT_MVCC))
}

fn matching_tlist<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

fn indexqual<'mcx>(mcx: Mcx<'mcx>, varno: i32, opno: Oid, opfuncid: Oid, k: i32) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, varno, 1, INT4OID, -1, 0, 0).unwrap();
    let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(k), false, true).unwrap();
    let args = NodeList::make2(mcx, var, c).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno,
            opfuncid,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: -1,
        },
    )
    .unwrap();
    NodeList::make1(mcx, op).unwrap()
}

fn mk_index_scan<'mcx>(mcx: Mcx<'mcx>, opno: Oid, opfuncid: Oid, k: i32) -> IndexScan<'mcx> {
    IndexScan {
        scan: Scan {
            plan: Plan { targetlist: matching_tlist(mcx), ..Default::default() },
            scanrelid: 1,
        },
        indexid: IDX_OID,
        indexqual: indexqual(mcx, INDEX_VAR, opno, opfuncid, k),
        indexqualorig: indexqual(mcx, 1, opno, opfuncid, k),
        indexorderby: NodeList::nil(),
        indexorderbyorig: NodeList::nil(),
        indexorderbyops: Default::default(),
        indexorderdir: 1,
    }
}

fn scan_values<'mcx>(mcx: Mcx<'mcx>, opno: Oid, opfuncid: Oid, k: i32) -> Vec<i32> {
    let node = mk_index_scan(mcx, opno, opfuncid, k);
    let rel = Relation::open(heap_relation_data(mcx), None);
    let index_rel = Relation::open(index_relation_data(mcx, UNIQUE_IDX.with(Cell::get)), None);
    let mut estate = EStateData::new_in(mcx);
    estate.es_snapshot = Some(static_mvcc_snapshot());
    let mut state = exec_init_index_scan_rel(mcx, &node, &mut estate, rel, index_rel).unwrap();
    let mut out = Vec::new();
    while let Some(id) = exec_index_scan(&mut state, &mut estate).unwrap() {
        let mut isnull = false;
        let v = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
        assert!(!isnull);
        out.push(v.as_i32());
    }
    exec_end_index_scan(&mut state).unwrap();
    estate.exec_reset_tuple_table(false);
    out
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
    });
}

#[test]
fn insert_rows_then_index_scan_finds_them() {
    let _g = serial();
    install();
    UNIQUE_IDX.with(|c| c.set(false));
    reset_fixture();

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let heap = Relation::open(heap_relation_data(mcx), None);
    let mut idxstate = crate::ExecOpenIndices(mcx, &heap, false).unwrap();
    assert_eq!(idxstate.num_indices(), 1);

    for v in [30, 10, 20, 50, 40] {
        insert_row(mcx, &heap, &mut idxstate, v).unwrap();
    }
    crate::ExecCloseIndices(idxstate).unwrap();

    assert_eq!(scan_values(mcx, OP_INT4EQ, F_INT4EQ, 20), vec![20]);
    assert_eq!(scan_values(mcx, OP_INT4GT, F_INT4GT, 15), vec![20, 30, 40, 50]);
    quiesced();
}

#[test]
fn forced_page_splits_stay_navigable_through_executor() {
    let _g = serial();
    install();
    UNIQUE_IDX.with(|c| c.set(false));
    reset_fixture();

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let heap = Relation::open(heap_relation_data(mcx), None);
    let mut idxstate = crate::ExecOpenIndices(mcx, &heap, false).unwrap();

    let n = 1200;
    for v in 1..=n {
        insert_row(mcx, &heap, &mut idxstate, v).unwrap();
    }
    crate::ExecCloseIndices(idxstate).unwrap();

    let (splits, newroots) = with_fake(|f| {
        let btree = ::rmgr::RM_BTREE_ID as u8;
        (
            f.wal
                .iter()
                .filter(|(r, i)| {
                    *r == btree && (*i == XLOG_BTREE_SPLIT_L || *i == XLOG_BTREE_SPLIT_R)
                })
                .count(),
            f.wal
                .iter()
                .filter(|(r, i)| *r == btree && *i == XLOG_BTREE_NEWROOT)
                .count(),
        )
    });
    assert!(splits >= 2, "expected leaf splits, saw {splits}");
    assert_eq!(newroots, 2, "root creation + root split");

    let all = scan_values(mcx, OP_INT4GT, F_INT4GT, 0);
    assert_eq!(all.len(), n as usize);
    assert!(all.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(scan_values(mcx, OP_INT4EQ, F_INT4EQ, 777), vec![777]);
    quiesced();
}

#[test]
fn unique_index_conflict_surfaces_23505() {
    let _g = serial();
    install();
    UNIQUE_IDX.with(|c| c.set(true));
    reset_fixture();

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let heap = Relation::open(heap_relation_data(mcx), None);
    let mut idxstate = crate::ExecOpenIndices(mcx, &heap, false).unwrap();

    insert_row(mcx, &heap, &mut idxstate, 7).unwrap();
    insert_row(mcx, &heap, &mut idxstate, 8).unwrap();
    let err = insert_row(mcx, &heap, &mut idxstate, 7).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNIQUE_VIOLATION);

    crate::ExecCloseIndices(idxstate).unwrap();
    quiesced();
}
