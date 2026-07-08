use super::*;

use core::ptr::NonNull;
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Mutex, Once};

use ::datum::Datum;
use ::mcx::MemoryContext;
use ::types_core::{
    BlockNumber, Buffer, GlobalVisStateHandle, InvalidBuffer, OffsetNumber, Oid, BLCKSZ,
    BTREE_AM_OID, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_LEAF, BTP_META, BTP_ROOT, BTREE_MAGIC, BTREE_VERSION,
    P_NONE,
};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::{Plan, Scan};
use ::types_nodes::primnodes::OpExpr;
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, RelationData, LOCKMODE,
    RELKIND_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use ::types_scan::scankey::BTEqualStrategyNumber;
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL};
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, NameData, PgTypeShape, TupleDescData,
    HEAP_XMAX_INVALID, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};
use executils::EStateData;
use syscache_seams::PgAmopShape;

const INT4OID: Oid = 23;
const BOOLOID: Oid = 16;
const INT4_BTREE_OPFAMILY: Oid = 1976;
const OP_INT4EQ: Oid = 96;
const OP_INT4GT: Oid = 521;
const F_INT4EQ: Oid = 65;
const F_INT4GT: Oid = 147;
const F_BTINT4CMP: Oid = 351;

struct Fake {
    tables: HashMap<Oid, Vec<Buffer>>,
    pages: Vec<usize>,
    pins: Vec<i32>,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());
static INIT: Once = Once::new();

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(|| Fake {
        tables: HashMap::new(),
        pages: Vec::new(),
        pins: Vec::new(),
    }))
}

fn install_seams() {
    INIT.call_once(|| {
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
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
        bufmgr_seams::buffer_get_lsn_atomic::set(|_buf| 0x1234);
        transam_xlog_seams::xlog_standby_info_active::set(|| false);

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(
            |_htup, _snap, _buf| Ok(true),
        );
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(
            |_htup, _snap, _buf, _memo| Ok(true),
        );
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_htup, _vt| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_hdr| Ok(false));

        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_relation::set(|_rel, _snap| Ok(()));
        predicate_seams::predicate_lock_page::set(|_rel, _blkno, _snap| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));

        pruneheap_seams::heap_page_prune_opt::set(|_rel, _buf| Ok(()));
        procarray_seams::global_vis_test_for::set(|_rel| GlobalVisStateHandle::new(0));

        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));

        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                BOOLOID => Some(PgTypeShape {
                    typlen: 1,
                    typbyval: true,
                    typalign: b'c' as i8,
                    typstorage: TYPSTORAGE_PLAIN,
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
            Ok(Some(PgAmopShape {
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
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
    });
}

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

fn tuple_image(val: i32) -> Vec<u8> {
    let mut img = vec![0u8; 28];
    img[0..4].copy_from_slice(&10u32.to_ne_bytes()); // xmin
    img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts = 1
    img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
    img[22] = 24; // t_hoff
    img[24..28].copy_from_slice(&val.to_ne_bytes());
    img
}

fn build_heap_page(vals: &[i32]) -> Box<TestPage> {
    let mut page = Box::new(TestPage([0u8; BLCKSZ]));
    let n = vals.len();
    let lower = SizeOfPageHeaderData + n * 4;
    let mut upper = BLCKSZ;
    for (i, val) in vals.iter().enumerate() {
        let img = tuple_image(*val);
        upper = (upper - img.len()) & !7;
        page.0[upper..upper + img.len()].copy_from_slice(&img);
        let id = ItemIdData::new(upper as u16, LP_NORMAL, img.len() as u16);
        let off = SizeOfPageHeaderData + i * 4;
        // SAFETY: repr(transparent) over u32.
        let raw: u32 = unsafe { core::mem::transmute(id) };
        page.0[off..off + 4].copy_from_slice(&raw.to_ne_bytes());
    }
    page.0[12..14].copy_from_slice(&(lower as u16).to_ne_bytes());
    page.0[14..16].copy_from_slice(&(upper as u16).to_ne_bytes());
    page.0[16..18].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
    page.0[18..20].copy_from_slice(&((BLCKSZ as u16) | 4).to_ne_bytes());
    page
}

fn put_u16(p: &mut TestPage, off: usize, v: u16) {
    p.0[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

fn new_bt_page(special_flags: u16, level: u32) -> Box<TestPage> {
    let mut p = Box::new(TestPage([0u8; BLCKSZ]));
    let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    put_u16(&mut p, 12, SizeOfPageHeaderData as u16); // pd_lower
    put_u16(&mut p, 14, special as u16); // pd_upper
    put_u16(&mut p, 16, special as u16); // pd_special
    let opaque = BTPageOpaqueData {
        btpo_prev: P_NONE,
        btpo_next: P_NONE,
        btpo_level: level,
        btpo_flags: special_flags,
        btpo_cycleid: 0,
    };
    // SAFETY: in-bounds, aligned special area write on an owned page.
    unsafe {
        p.0.as_mut_ptr()
            .add(special)
            .cast::<BTPageOpaqueData>()
            .write(opaque)
    };
    p
}

fn meta_page(root: BlockNumber, level: u32) -> Box<TestPage> {
    let mut p = new_bt_page(BTP_META, 0);
    let metad = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: root,
        btm_level: level,
        btm_fastroot: root,
        btm_fastlevel: level,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: true,
    };
    // SAFETY: metapage contents at +24 on an owned page.
    unsafe {
        p.0.as_mut_ptr()
            .add(SizeOfPageHeaderData)
            .cast::<BTMetaPageData>()
            .write(metad)
    };
    p
}

// One 16-byte int4 index tuple (t_info alt-TID bits unset).
fn add_index_tuple(p: &mut TestPage, tid: ItemPointerData, value: i32) -> OffsetNumber {
    let itupsz = 16usize;
    let pd_lower = u16::from_ne_bytes([p.0[12], p.0[13]]) as usize;
    let pd_upper = u16::from_ne_bytes([p.0[14], p.0[15]]) as usize;
    let off = pd_upper - itupsz;
    // SAFETY: owned page bytes; ItemPointerData is a 6B POD.
    unsafe {
        p.0.as_mut_ptr()
            .add(off)
            .cast::<ItemPointerData>()
            .write_unaligned(tid);
    }
    p.0[off + 6..off + 8].copy_from_slice(&(itupsz as u16).to_ne_bytes());
    p.0[off + 8..off + 12].copy_from_slice(&value.to_ne_bytes());
    let mut iid = ItemIdData::new(0, 0, 0);
    iid.set_normal(off as u16, itupsz as u16);
    // SAFETY: line-pointer slot in the owned page.
    unsafe {
        p.0.as_mut_ptr()
            .add(pd_lower)
            .cast::<ItemIdData>()
            .write(iid)
    };
    put_u16(p, 12, (pd_lower + 4) as u16);
    put_u16(p, 14, off as u16);
    ((pd_lower - SizeOfPageHeaderData) / 4 + 1) as OffsetNumber
}

fn register_pages(relid: Oid, pages: Vec<Box<TestPage>>) {
    with_fake(|f| {
        let mut bufs = Vec::new();
        for p in pages {
            let addr = Box::leak(p).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(0);
            bufs.push(f.pages.len() as Buffer);
        }
        f.tables.insert(relid, bufs);
    });
}

// Heap page 0 holds `vals` at offsets 1..=n; a root leaf indexes them in
// ascending key order.
fn register_indexed_table(heap_oid: Oid, index_oid: Oid, vals: &[i32]) {
    register_pages(heap_oid, vec![build_heap_page(vals)]);

    let mut keyed: Vec<(i32, u16)> = vals
        .iter()
        .enumerate()
        .map(|(i, v)| (*v, (i + 1) as u16))
        .collect();
    keyed.sort();
    let mut leaf = new_bt_page(BTP_LEAF | BTP_ROOT, 0);
    for (v, off) in keyed {
        add_index_tuple(&mut leaf, ItemPointerData::new(0, off), v);
    }
    register_pages(index_oid, vec![meta_page(1, 0), leaf]);
}

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: INT4OID,
        atttypmod: -1,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        attstorage: TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn heap_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: tableam::HEAP_TABLE_AM_OID,
        relfilenode: oid,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: true,
        relisshared: false,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    };
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId {
                relId: oid,
                dbId: 5,
            },
        },
        rd_rel,
        rd_att: int4_tupdesc(mcx),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    };
    Relation::open(data, None)
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> types_error::PgResult<()> {
    Ok(())
}

fn index_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid, heap_oid: Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t_idx");
    let one = |v: Oid| {
        let mut vec = PgVec::new_in(mcx);
        vec.push(v);
        vec
    };
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1);
    let mut indoption = PgVec::new_in(mcx);
    indoption.push(0i16);
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId {
                relId: oid,
                dbId: 5,
            },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: BTREE_AM_OID,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_INDEX,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: int4_tupdesc(mcx),
        rd_index: Some(FormData_pg_index {
            indexrelid: oid,
            indrelid: heap_oid,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: false,
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
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    };
    Relation::open(data, Some(noop_close))
}

fn static_mvcc_snapshot() -> Rc<SnapshotData<'static>> {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap-test")));
    Rc::new(SnapshotData::sentinel(
        ctx.mcx(),
        SnapshotType::SNAPSHOT_MVCC,
    ))
}

static NEXT_OID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(80000);
fn fresh_oid() -> Oid {
    NEXT_OID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    install_seams();
    let ctx = MemoryContext::new("nodeindexscan-test");
    f(ctx.mcx())
}

fn matching_tlist<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

fn const_tlist<'mcx>(mcx: Mcx<'mcx>, v: i32) -> NodeList<'mcx> {
    let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, c, 1, None, false).unwrap();
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
            opresulttype: BOOLOID,
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

fn mk_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    targetlist: NodeList<'mcx>,
    opno: Oid,
    opfuncid: Oid,
    k: i32,
) -> IndexScan<'mcx> {
    IndexScan {
        scan: Scan {
            plan: Plan {
                targetlist,
                ..Default::default()
            },
            scanrelid: 1,
        },
        indexid: 0,
        indexqual: indexqual(mcx, INDEX_VAR, opno, opfuncid, k),
        indexqualorig: indexqual(mcx, 1, opno, opfuncid, k),
        indexorderby: NodeList::nil(),
        indexorderbyorig: NodeList::nil(),
        indexorderbyops: Default::default(),
        indexorderdir: 1,
    }
}

fn setup<'mcx>(
    mcx: Mcx<'mcx>,
    vals: &[i32],
    node: &IndexScan<'mcx>,
) -> (EStateData<'mcx>, IndexScanState<'mcx>) {
    let heap_oid = fresh_oid();
    let index_oid = fresh_oid();
    register_indexed_table(heap_oid, index_oid, vals);
    let rel = heap_relation(mcx, heap_oid);
    let index_rel = index_relation(mcx, index_oid, heap_oid);
    let mut estate = EStateData::new_in(mcx);
    estate.es_snapshot = Some(static_mvcc_snapshot());
    let state = exec_init_index_scan_rel(mcx, node, &mut estate, rel, index_rel).unwrap();
    (estate, state)
}

fn drain<'mcx>(node: &mut IndexScanState<'mcx>, estate: &mut EStateData<'mcx>) -> Vec<i32> {
    let mut out = Vec::new();
    while let Some(id) = exec_index_scan(node, estate).unwrap() {
        let mut isnull = false;
        let v = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
        assert!(!isnull);
        out.push(v.as_i32());
    }
    out
}

fn teardown<'mcx>(mut node: IndexScanState<'mcx>, estate: &mut EStateData<'mcx>) {
    exec_end_index_scan(&mut node).unwrap();
    estate.exec_reset_tuple_table(false);
    quiesced();
}

#[test]
fn point_lookup_returns_matching_row() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, matching_tlist(mcx), OP_INT4EQ, F_INT4EQ, 20);
        let (mut estate, mut state) = setup(mcx, &[30, 20, 10], &node);
        assert!(state.iss_ScanDesc.is_none());
        assert_eq!(drain(&mut state, &mut estate), vec![20]);
        assert!(state.iss_ScanDesc.is_some());
        teardown(state, &mut estate);
    });
}

#[test]
fn scan_keys_built_once_at_init() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, matching_tlist(mcx), OP_INT4EQ, F_INT4EQ, 42);
        let (mut estate, state) = setup(mcx, &[1], &node);
        assert_eq!(state.iss_ScanKeys.len(), 1);
        let key = &state.iss_ScanKeys[0];
        assert_eq!(key.sk_attno, 1);
        assert_eq!(key.sk_strategy, BTEqualStrategyNumber);
        assert_eq!(key.sk_subtype, INT4OID);
        assert_eq!(key.sk_flags, 0);
        assert_eq!(key.sk_func.fn_oid, F_INT4EQ);
        assert_eq!(key.sk_argument.as_i32(), 42);
        teardown(state, &mut estate);
    });
}

#[test]
fn range_scan_returns_index_order() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, matching_tlist(mcx), OP_INT4GT, F_INT4GT, 10);
        let (mut estate, mut state) = setup(mcx, &[30, 20, 10, 40], &node);
        assert_eq!(drain(&mut state, &mut estate), vec![20, 30, 40]);
        teardown(state, &mut estate);
    });
}

#[test]
fn no_match_returns_none() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, matching_tlist(mcx), OP_INT4EQ, F_INT4EQ, 99);
        let (mut estate, mut state) = setup(mcx, &[1, 2, 3], &node);
        assert_eq!(drain(&mut state, &mut estate), Vec::<i32>::new());
        teardown(state, &mut estate);
    });
}

#[test]
fn projection_evaluates_targetlist() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, const_tlist(mcx, 7), OP_INT4GT, F_INT4GT, 15);
        let (mut estate, mut state) = setup(mcx, &[10, 20, 30], &node);
        assert!(state.ss.ps_ProjInfo.is_some());
        assert_eq!(drain(&mut state, &mut estate), vec![7, 7]);
        teardown(state, &mut estate);
    });
}

#[test]
fn rescan_restarts_from_first_tuple() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, matching_tlist(mcx), OP_INT4GT, F_INT4GT, 0);
        let (mut estate, mut state) = setup(mcx, &[10, 20, 30], &node);
        assert!(exec_index_scan(&mut state, &mut estate).unwrap().is_some());
        assert!(exec_index_scan(&mut state, &mut estate).unwrap().is_some());
        exec_rescan_index_scan(&mut state, &mut estate).unwrap();
        assert_eq!(drain(&mut state, &mut estate), vec![10, 20, 30]);
        teardown(state, &mut estate);
    });
}

#[test]
fn rescan_before_first_fetch_is_noop() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, matching_tlist(mcx), OP_INT4EQ, F_INT4EQ, 8);
        let (mut estate, mut state) = setup(mcx, &[8, 9], &node);
        exec_rescan_index_scan(&mut state, &mut estate).unwrap();
        assert!(state.iss_ScanDesc.is_none());
        assert_eq!(drain(&mut state, &mut estate), vec![8]);
        teardown(state, &mut estate);
    });
}

#[test]
fn end_scan_without_fetch_releases_nothing() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_scan(mcx, matching_tlist(mcx), OP_INT4EQ, F_INT4EQ, 1);
        let (mut estate, state) = setup(mcx, &[1], &node);
        teardown(state, &mut estate);
    });
}

#[test]
fn runtime_key_qual_builds_deferred_scan_key() {
    let _g = serial();
    with_mcx(|mcx| {
        let heap_oid = fresh_oid();
        let index_oid = fresh_oid();
        register_indexed_table(heap_oid, index_oid, &[1]);
        let index_rel = index_relation(mcx, index_oid, heap_oid);
        // indexkey op (non-Const): Var on the right => runtime key.
        let lvar = Node::mk_var(mcx, INDEX_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let rvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let args = NodeList::make2(mcx, lvar, rvar).unwrap();
        let op = Node::mk(
            mcx,
            OpExpr {
                opno: OP_INT4EQ,
                opfuncid: F_INT4EQ,
                opresulttype: BOOLOID,
                opretset: false,
                opcollid: 0,
                inputcollid: 0,
                args,
                location: -1,
            },
        )
        .unwrap();
        let quals = NodeList::make1(mcx, op).unwrap();
        let mut runtime = ::mcx::PgVec::new_in(mcx);
        let keys =
            exec_index_build_scan_keys(mcx, &index_rel, &quals, ParamBind::NONE, false, &mut runtime, None)
                .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(runtime.len(), 1);
        assert_eq!(runtime[0].scan_key, 0);
        assert!(!runtime[0].key_toastable);
        assert_eq!(keys[0].sk_flags & SK_ISNULL, 0);
        assert_eq!(keys[0].sk_argument, Datum::from_usize(0));
        assert_eq!(keys[0].sk_strategy, BTEqualStrategyNumber);
    });
}

#[test]
fn saop_runtime_array_qual_builds_deferred_search_array_key() {
    let _g = serial();
    with_mcx(|mcx| {
        let heap_oid = fresh_oid();
        let index_oid = fresh_oid();
        register_indexed_table(heap_oid, index_oid, &[1]);
        let index_rel = index_relation(mcx, index_oid, heap_oid);
        let lvar = Node::mk_var(mcx, INDEX_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let rvar = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let saop = Node::mk(
            mcx,
            ::types_nodes::primnodes::ScalarArrayOpExpr {
                opno: OP_INT4EQ,
                opfuncid: F_INT4EQ,
                hashfuncid: 0,
                negfuncid: 0,
                useOr: true,
                inputcollid: 0,
                args: NodeList::make2(mcx, lvar, rvar).unwrap(),
                location: -1,
            },
        )
        .unwrap();
        let quals = NodeList::make1(mcx, saop).unwrap();
        let mut runtime = ::mcx::PgVec::new_in(mcx);
        let keys =
            exec_index_build_scan_keys(mcx, &index_rel, &quals, ParamBind::NONE, false, &mut runtime, None)
                .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(runtime.len(), 1);
        assert_eq!(runtime[0].scan_key, 0);
        assert!(runtime[0].key_toastable);
        assert_eq!(keys[0].sk_flags, SK_SEARCHARRAY);
        assert_eq!(keys[0].sk_argument, Datum::from_usize(0));
        assert_eq!(keys[0].sk_strategy, BTEqualStrategyNumber);
    });
}

// A reused generic plan reaches the executor with no planner locks and
// AcquireExecutorLocks covers tables only, so the scan-index open must carry
// the RTE's rellockmode (C nodeIndexscan.c:977) — NoLock here trips the
// relation_open lock assertion (pg_regress plpgsql WSlot trigger, exec 6).
#[test]
fn index_lockmode_is_rte_rellockmode() {
    with_mcx(|mcx| {
        let rte: &::types_nodes::parsenodes::RangeTblEntry = &*::mcx::leak_in(
            ::mcx::alloc_in(
                mcx,
                ::types_nodes::parsenodes::RangeTblEntry {
                    rtekind: ::types_nodes::parsenodes::RTEKind::RTE_RELATION,
                    relid: 4242,
                    relkind: RELKIND_RELATION,
                    rellockmode: ::types_rel::RowExclusiveLock,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let mut estate = EStateData::new_in(mcx);
        estate.es_range_table.push(rte);
        estate.es_range_table_size = 1;
        assert_eq!(index_lockmode(&estate, 1), ::types_rel::RowExclusiveLock);
    });
}
