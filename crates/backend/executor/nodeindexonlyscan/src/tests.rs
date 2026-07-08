use super::*;

use core::ptr::NonNull;
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Mutex, Once};

use ::datum::Datum;
use ::mcx::MemoryContext;
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, GlobalVisStateHandle, InvalidBlockNumber, InvalidBuffer, Oid,
    BLCKSZ, BTREE_AM_OID, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_LEAF, BTP_META, BTP_ROOT, BTREE_MAGIC, BTREE_VERSION,
    P_NONE,
};
use ::types_nodes::list::NodeList;
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
use ::types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};
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
const F_INT4EQ: Oid = 65;
const F_BTINT4CMP: Oid = 351;

struct Fake {
    tables: HashMap<Oid, Vec<Buffer>>,
    vm_forks: HashMap<Oid, Vec<Buffer>>,
    vm_cached: HashMap<Oid, BlockNumber>,
    reads: HashMap<Oid, u32>,
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
        vm_forks: HashMap::new(),
        vm_cached: HashMap::new(),
        reads: HashMap::new(),
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
                *f.reads.entry(rel.rd_id).or_insert(0) += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::relation_smgr_locator::set(|rel| RelFileLocatorBackend {
            locator: RelFileLocator {
                spcOid: 1663,
                dbOid: 5,
                relNumber: rel.rd_id,
            },
            backend: INVALID_PROC_NUMBER,
        });
        bufmgr_seams::read_buffer_extended::set(|rel, fork, blkno, mode, _strategy| {
            assert_eq!(fork, ForkNumber::VISIBILITYMAP_FORKNUM);
            assert_eq!(mode, ReadBufferMode::ZeroOnError);
            with_fake(|f| {
                let buf = f.vm_forks[&rel.rd_id][blkno as usize];
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        smgr_seams::smgr_exists::set(|rloc, fork| {
            assert_eq!(fork, ForkNumber::VISIBILITYMAP_FORKNUM);
            with_fake(|f| Ok(f.vm_forks.contains_key(&rloc.locator.relNumber)))
        });
        smgr_seams::smgr_cached_nblocks::set(|rloc, _fork| {
            with_fake(|f| {
                f.vm_cached
                    .get(&rloc.locator.relNumber)
                    .copied()
                    .unwrap_or(InvalidBlockNumber)
            })
        });
        smgr_seams::smgr_set_cached_nblocks::set(|rloc, _fork, v| {
            with_fake(|f| f.vm_cached.insert(rloc.locator.relNumber, v));
            Ok(())
        });
        smgr_seams::smgr_nblocks::set(|rloc, _fork| {
            with_fake(|f| {
                let n = f.vm_forks[&rloc.locator.relNumber].len() as BlockNumber;
                f.vm_cached.insert(rloc.locator.relNumber, n);
                Ok(n)
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

        predicate_seams::predicate_lock_page::set(|_rel, _blkno, _snap| Ok(()));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_relation::set(|_rel, _snap| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(
            |_htup, _snap, _buf| Ok(true),
        );
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(
            |_htup, _snap, _buf, _memo| Ok(true),
        );
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_htup, _vt| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_hdr| Ok(false));
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
                _ => return Ok(None),
            };
            Ok(Some(PgAmopShape {
                amopstrategy: strategy,
                amopsortfamily: 0,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
            }))
        });
        syscache_seams::lookup_pg_amproc::set(|_opfamily, _lt, _rt, _procnum| Ok(F_BTINT4CMP));
    });
}

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

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

// 16-byte int4 index-tuple image (t_info alt-TID bits unset).
#[repr(align(8))]
struct Img([u8; 16]);

fn itup_image(tid: ItemPointerData, value: i32) -> Box<Img> {
    let mut img = Box::new(Img([0u8; 16]));
    // SAFETY: owned image bytes; ItemPointerData is a 6B POD.
    unsafe {
        img.0
            .as_mut_ptr()
            .cast::<ItemPointerData>()
            .write_unaligned(tid)
    };
    img.0[6..8].copy_from_slice(&16u16.to_ne_bytes());
    img.0[8..12].copy_from_slice(&value.to_ne_bytes());
    img
}

fn add_index_tuple(p: &mut TestPage, tid: ItemPointerData, value: i32) {
    let img = itup_image(tid, value);
    let pd_lower = u16::from_ne_bytes([p.0[12], p.0[13]]) as usize;
    let pd_upper = u16::from_ne_bytes([p.0[14], p.0[15]]) as usize;
    let off = pd_upper - 16;
    p.0[off..off + 16].copy_from_slice(&img.0);
    let mut iid = ItemIdData::new(0, 0, 0);
    iid.set_normal(off as u16, 16);
    // SAFETY: line-pointer slot in the owned page.
    unsafe {
        p.0.as_mut_ptr()
            .add(pd_lower)
            .cast::<ItemIdData>()
            .write(iid)
    };
    put_u16(p, 12, (pd_lower + 4) as u16);
    put_u16(p, 14, off as u16);
}

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

// One initialized VM-fork page; 2 status bits per heap block, low-to-high.
fn vm_page(all_visible_blocks: &[BlockNumber]) -> Box<TestPage> {
    let contents_off = (SizeOfPageHeaderData + 7) & !7;
    let mut p = Box::new(TestPage([0u8; BLCKSZ]));
    put_u16(&mut p, 12, contents_off as u16); // pd_lower
    put_u16(&mut p, 14, BLCKSZ as u16); // pd_upper (non-zero: not PageIsNew)
    put_u16(&mut p, 16, BLCKSZ as u16); // pd_special
    for &blk in all_visible_blocks {
        p.0[contents_off + (blk / 4) as usize] |= 0x01 << ((blk % 4) * 2);
    }
    p
}

fn register_pages_in(relid: Oid, pages: Vec<Box<TestPage>>, vm: bool) {
    with_fake(|f| {
        let mut bufs = Vec::new();
        for p in pages {
            let addr = Box::leak(p).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(0);
            bufs.push(f.pages.len() as Buffer);
        }
        if vm {
            f.vm_forks.insert(relid, bufs);
        } else {
            f.tables.insert(relid, bufs);
        }
    });
}

fn register_pages(relid: Oid, pages: Vec<Box<TestPage>>) {
    register_pages_in(relid, pages, false);
}

// Heap page 0 holds `vals` at offsets 1..=n; a root leaf indexes them in
// ascending key order; the VM fork marks `all_visible_blocks`.
fn register_indexed_table(
    heap_oid: Oid,
    index_oid: Oid,
    vals: &[i32],
    all_visible_blocks: &[BlockNumber],
) {
    register_pages(heap_oid, vec![build_heap_page(vals)]);
    register_pages_in(heap_oid, vec![vm_page(all_visible_blocks)], true);

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

fn heap_reads(heap_oid: Oid) -> u32 {
    with_fake(|f| f.reads.get(&heap_oid).copied().unwrap_or(0))
}

fn quiesced() {
    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
    });
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

static NEXT_OID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(90000);
fn fresh_oid() -> Oid {
    NEXT_OID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    install_seams();
    let ctx = MemoryContext::new("nodeindexonlyscan-test");
    f(ctx.mcx())
}

fn index_var_tlist<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, INDEX_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

fn indexqual<'mcx>(mcx: Mcx<'mcx>, k: i32) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, INDEX_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let c = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(k), false, true).unwrap();
    let args = NodeList::make2(mcx, var, c).unwrap();
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
    NodeList::make1(mcx, op).unwrap()
}

fn mk_index_only_scan<'mcx>(mcx: Mcx<'mcx>, k: i32) -> IndexOnlyScan<'mcx> {
    IndexOnlyScan {
        scan: Scan {
            plan: Plan {
                targetlist: index_var_tlist(mcx),
                ..Default::default()
            },
            scanrelid: 1,
        },
        indexid: 0,
        indexqual: indexqual(mcx, k),
        recheckqual: indexqual(mcx, k),
        indexorderby: NodeList::nil(),
        indextlist: index_var_tlist(mcx),
        indexorderdir: 1,
    }
}

fn setup<'mcx>(
    mcx: Mcx<'mcx>,
    vals: &[i32],
    node: &IndexOnlyScan<'mcx>,
    all_visible_blocks: &[BlockNumber],
) -> (Oid, EStateData<'mcx>, IndexOnlyScanState<'mcx>) {
    let heap_oid = fresh_oid();
    let index_oid = fresh_oid();
    register_indexed_table(heap_oid, index_oid, vals, all_visible_blocks);
    let rel = heap_relation(mcx, heap_oid);
    let index_rel = index_relation(mcx, index_oid, heap_oid);
    let mut estate = EStateData::new_in(mcx);
    estate.es_snapshot = Some(static_mvcc_snapshot());
    let state = exec_init_index_only_scan_rel(mcx, node, &mut estate, rel, index_rel).unwrap();
    (heap_oid, estate, state)
}

fn drain<'mcx>(node: &mut IndexOnlyScanState<'mcx>, estate: &mut EStateData<'mcx>) -> Vec<i32> {
    let mut out = Vec::new();
    while let Some(id) = exec_index_only_scan(node, estate).unwrap() {
        let mut isnull = false;
        let v = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
        assert!(!isnull);
        out.push(v.as_i32());
    }
    out
}

fn teardown<'mcx>(mut node: IndexOnlyScanState<'mcx>, estate: &mut EStateData<'mcx>) {
    exec_end_index_only_scan(&mut node).unwrap();
    estate.exec_reset_tuple_table(false);
    quiesced();
}

#[test]
fn init_builds_scan_keys_and_slots() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_only_scan(mcx, 42);
        let (_heap_oid, estate, mut state) = setup(mcx, &[1, 2, 3], &node, &[]);
        assert_eq!(state.ioss_ScanKeys.len(), 1);
        let key = &state.ioss_ScanKeys[0];
        assert_eq!(key.sk_attno, 1);
        assert_eq!(key.sk_strategy, BTEqualStrategyNumber);
        assert_eq!(key.sk_argument.as_i32(), 42);
        // Scan slot is virtual (indextlist type), table slot is the AM's kind.
        assert!(matches!(
            estate.slot(state.ss.ss_ScanTupleSlot),
            types_slot::SlotData::Virtual(_)
        ));
        assert_ne!(state.ss.ss_ScanTupleSlot, state.ioss_TableSlot);
        // Matching INDEX_VAR targetlist elides the projection.
        assert!(state.ss.ps_ProjInfo.is_none());
        assert!(state.ioss_NameCStringAttNums.is_empty());
        let mut estate = estate;
        exec_end_index_only_scan(&mut state).unwrap();
        estate.exec_reset_tuple_table(false);
    });
}

#[test]
fn all_visible_scan_never_touches_the_heap() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_only_scan(mcx, 20);
        let (heap_oid, mut estate, mut state) = setup(mcx, &[30, 20, 10], &node, &[0]);
        assert_eq!(drain(&mut state, &mut estate), vec![20]);
        assert_eq!(
            heap_reads(heap_oid),
            0,
            "heap page fetched on the VM fast path"
        );
        let scandesc = state.ioss_ScanDesc.as_ref().unwrap();
        assert!(scandesc.xs_want_itup);
        assert!(scandesc.xs_itup.is_some());
        // ExecEnd releases the retained VM pin.
        assert!(state.ioss_VMBuffer.is_valid());
        teardown(state, &mut estate);
    });
}

#[test]
fn vm_clear_falls_back_to_heap_fetch() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_only_scan(mcx, 20);
        let (heap_oid, mut estate, mut state) = setup(mcx, &[30, 20, 10], &node, &[]);
        assert_eq!(drain(&mut state, &mut estate), vec![20]);
        assert!(
            heap_reads(heap_oid) >= 1,
            "VM clear must fall through to the heap"
        );
        teardown(state, &mut estate);
    });
}

#[test]
fn store_index_tuple_deforms_btree_int4() {
    let _g = serial();
    with_mcx(|mcx| {
        let desc = int4_tupdesc(mcx);
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
        let img = itup_image(ItemPointerData::new(0, 1), 777);
        // SAFETY: MAXALIGNed 16-byte int4 tuple image matching `desc`.
        unsafe { store_index_tuple(&mut slot, mcx, img.0.as_ptr(), &desc, &[]) };
        let mut isnull = false;
        let v = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
        assert!(!isnull);
        assert_eq!(v.as_i32(), 777);
    });
}
