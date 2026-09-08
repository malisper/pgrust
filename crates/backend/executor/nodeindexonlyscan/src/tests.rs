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
    HEAP_XMAX_INVALID, TYPALIGN_CHAR, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};
use executils::EStateData;
use syscache_seams::PgAmopShape;

const INT4OID: Oid = 23;
const BOOLOID: Oid = 16;
const INT4_BTREE_OPFAMILY: Oid = 1976;
const OP_INT4EQ: Oid = 96;
const F_INT4EQ: Oid = 65;
const F_BTINT4CMP: Oid = 351;
const NAME_BTREE_OPFAMILY: Oid = 1986;
const NAMEDATALEN: usize = 64;

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
                NAMEOID => Some(PgTypeShape {
                    typlen: NAMEDATALEN as i16,
                    typbyval: false,
                    typalign: TYPALIGN_CHAR,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                CSTRINGOID => Some(PgTypeShape {
                    typlen: -2,
                    typbyval: false,
                    typalign: TYPALIGN_CHAR,
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
    add_index_image(p, &img.0);
}

// MAXALIGNed cstring index-tuple image (btree name_ops storage form):
// header, then the NUL-terminated bytes; t_info carries the varwidth bit.
fn cstring_itup_image(tid: ItemPointerData, s: &str) -> Vec<u8> {
    let size = (8 + s.len() + 1 + 7) & !7;
    let mut img = vec![0u8; size];
    // SAFETY: owned image bytes; ItemPointerData is a 6B POD.
    unsafe {
        img.as_mut_ptr()
            .cast::<ItemPointerData>()
            .write_unaligned(tid)
    };
    img[6..8].copy_from_slice(&(size as u16 | ::nbtree::itup::INDEX_VAR_MASK).to_ne_bytes());
    img[8..8 + s.len()].copy_from_slice(s.as_bytes());
    img
}

fn add_index_image(p: &mut TestPage, img: &[u8]) {
    let len = img.len();
    let pd_lower = u16::from_ne_bytes([p.0[12], p.0[13]]) as usize;
    let pd_upper = u16::from_ne_bytes([p.0[14], p.0[15]]) as usize;
    let off = pd_upper - len;
    p.0[off..off + len].copy_from_slice(img);
    let mut iid = ItemIdData::new(0, 0, 0);
    iid.set_normal(off as u16, len as u16);
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

// Heap page 0 holds one placeholder row per name (never fetched: the VM
// marks block 0 all-visible); a root leaf indexes the names as cstrings in
// ascending order, as btree name_ops stores NAME keys.
fn register_indexed_name_table(heap_oid: Oid, index_oid: Oid, names: &[&str]) {
    register_pages(heap_oid, vec![build_heap_page(&vec![0; names.len()])]);
    register_pages_in(heap_oid, vec![vm_page(&[0])], true);

    let mut keyed: Vec<(&str, u16)> = names
        .iter()
        .enumerate()
        .map(|(i, s)| (*s, (i + 1) as u16))
        .collect();
    keyed.sort();
    let mut leaf = new_bt_page(BTP_LEAF | BTP_ROOT, 0);
    for (s, off) in keyed {
        add_index_image(&mut leaf, &cstring_itup_image(ItemPointerData::new(0, off), s));
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
    one_att_tupdesc(
        mcx,
        FormData_pg_attribute {
            attnum: 1,
            atttypid: INT4OID,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        },
    )
}

// btree name_ops physical storage: NAME keys are stored as cstrings.
fn cstring_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    one_att_tupdesc(
        mcx,
        FormData_pg_attribute {
            attnum: 1,
            atttypid: CSTRINGOID,
            atttypmod: -1,
            attlen: -2,
            attbyval: false,
            attalign: TYPALIGN_CHAR,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        },
    )
}

fn one_att_tupdesc<'mcx>(mcx: Mcx<'mcx>, att: FormData_pg_attribute) -> Rc<TupleDescData<'mcx>> {
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
    index_relation_keyed_am(
        mcx,
        oid,
        heap_oid,
        int4_tupdesc(mcx),
        INT4OID,
        INT4_BTREE_OPFAMILY,
        BTREE_AM_OID,
    )
}

// Single-key btree index: `rd_att` is the physical storage descriptor,
// `opcintype`/`opfamily` the opclass input type and family.
fn index_relation_keyed<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    heap_oid: Oid,
    rd_att: Rc<TupleDescData<'mcx>>,
    opcintype: Oid,
    opfamily: Oid,
) -> Relation<'mcx> {
    index_relation_keyed_am(mcx, oid, heap_oid, rd_att, opcintype, opfamily, BTREE_AM_OID)
}

// As above with the access method chosen by the caller (the scripted Mock AM
// serves the lossy-recheck witness).
fn index_relation_keyed_am<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    heap_oid: Oid,
    rd_att: Rc<TupleDescData<'mcx>>,
    opcintype: Oid,
    opfamily: Oid,
    relam: Oid,
) -> Relation<'mcx> {
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
            relkind: RELKIND_INDEX,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att,
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
            indcheckxmin: false,
            indxmin: 0,
            indkey,
            has_indpred: false,
            indexprs_src: None,
            indpred_src: None,
        }),
        rd_opcintype: one(opcintype),
        rd_opfamily: one(opfamily),
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
    static_snapshot(SnapshotType::SNAPSHOT_MVCC)
}

fn static_snapshot(kind: SnapshotType) -> Rc<SnapshotData<'static>> {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("snap-test")));
    Rc::new(SnapshotData::sentinel(ctx.mcx(), kind))
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

fn name_var_tlist<'mcx>(mcx: Mcx<'mcx>) -> NodeList<'mcx> {
    let var = Node::mk_var(mcx, INDEX_VAR, 1, NAMEOID, -1, 0, 0).unwrap();
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

// Unqualified index-only scan over a NAME key (btree name_ops).
fn mk_name_index_only_scan<'mcx>(mcx: Mcx<'mcx>) -> IndexOnlyScan<'mcx> {
    IndexOnlyScan {
        scan: Scan {
            plan: Plan {
                targetlist: name_var_tlist(mcx),
                ..Default::default()
            },
            scanrelid: 1,
        },
        indexid: 0,
        indexqual: NodeList::nil(),
        recheckqual: NodeList::nil(),
        indexorderby: NodeList::nil(),
        indextlist: name_var_tlist(mcx),
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
        unsafe { store_index_tuple(&mut slot, mcx, mcx, img.0.as_ptr(), &desc, &[]) };
        let mut isnull = false;
        let v = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
        assert!(!isnull);
        assert_eq!(v.as_i32(), 777);
    });
}

// C nodeIndexonlyscan.c:307 (StoreIndexTuple): the NAMEDATALEN block a
// cstring-stored NAME key is re-inflated into is MemoryContextAlloc'd in
// ps_ExprContext->ecxt_per_tuple_memory, which ExecScan resets per row —
// never in the query context, where it would leak one block per row.
#[test]
fn name_columns_reinflate_into_per_tuple_memory() {
    let _g = serial();
    with_mcx(|mcx| {
        let names = ["gamma", "alpha", "delta", "beta", "zeta", "eta", "theta", "iota"];
        let node = mk_name_index_only_scan(mcx);
        let heap_oid = fresh_oid();
        let index_oid = fresh_oid();
        register_indexed_name_table(heap_oid, index_oid, &names);
        let rel = heap_relation(mcx, heap_oid);
        let index_rel = index_relation_keyed(
            mcx,
            index_oid,
            heap_oid,
            cstring_tupdesc(mcx),
            NAMEOID,
            NAME_BTREE_OPFAMILY,
        );
        let mut estate = EStateData::new_in(mcx);
        estate.es_snapshot = Some(static_mvcc_snapshot());
        let mut state =
            exec_init_index_only_scan_rel(mcx, &node, &mut estate, rel, index_rel).unwrap();
        assert_eq!(&*state.ioss_NameCStringAttNums, &[0]);
        let ecxt = state.ss.ps_ExprContext;

        let mut got = Vec::new();
        let mut query_used_after_first = None;
        loop {
            // ExecScan resets the per-tuple context on entry, freeing the
            // previous row's NAME block; the query context (aset, charged
            // per chunk) must not grow per row.
            estate.ecxt_mut(ecxt).reset();
            let Some(id) = exec_index_only_scan(&mut state, &mut estate).unwrap() else {
                break;
            };
            let mut isnull = false;
            let v = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
            assert!(!isnull);
            // SAFETY: a NAME datum points at a NAMEDATALEN block.
            let bytes =
                unsafe { core::slice::from_raw_parts(v.as_usize() as *const u8, NAMEDATALEN) };
            let end = bytes.iter().position(|b| *b == 0).expect("NUL-terminated name");
            assert!(bytes[end..].iter().all(|b| *b == 0), "namestrcpy zero-pads");
            got.push(String::from_utf8(bytes[..end].to_vec()).unwrap());
            match query_used_after_first {
                None => query_used_after_first = Some(mcx.context().used()),
                Some(base) => assert!(
                    mcx.context().used() - base < NAMEDATALEN * (names.len() - 1),
                    "query context grows per row: NAME blocks leak into es_query_cxt"
                ),
            }
        }
        assert_eq!(
            got,
            ["alpha", "beta", "delta", "eta", "gamma", "iota", "theta", "zeta"]
        );
        teardown(state, &mut estate);
    });
}

// C nodeIndexonlyscan.c:181 (IndexOnlyNext): a heap fetch that leaves
// xs_heap_continue set (only a non-MVCC snapshot can make more than one
// HOT-chain member visible) is elog(ERROR), in every build.
#[test]
fn non_mvcc_snapshot_heap_continuation_is_an_error() {
    let _g = serial();
    with_mcx(|mcx| {
        let node = mk_index_only_scan(mcx, 20);
        let heap_oid = fresh_oid();
        let index_oid = fresh_oid();
        register_indexed_table(heap_oid, index_oid, &[30, 20, 10], &[]);
        let rel = heap_relation(mcx, heap_oid);
        let index_rel = index_relation(mcx, index_oid, heap_oid);
        let mut estate = EStateData::new_in(mcx);
        estate.es_snapshot = Some(static_snapshot(SnapshotType::SNAPSHOT_ANY));
        let mut state =
            exec_init_index_only_scan_rel(mcx, &node, &mut estate, rel, index_rel).unwrap();
        let err = exec_index_only_scan(&mut state, &mut estate).unwrap_err();
        assert_eq!(
            err.message(),
            "non-MVCC snapshots are not supported in index-only scans"
        );
        assert!(state.ioss_ScanDesc.as_ref().unwrap().xs_heap_continue);
        teardown(state, &mut estate);
    });
}

// audit-18.6 w2-011 (row nodeIndexonlyscan-2f14f94c): IndexOnlyNext counts a
// tuple the lossy-index recheck rejects as InstrCountFiltered2
// (nodeIndexonlyscan.c:206 — EXPLAIN ANALYZE "Rows Removed by Index
// Recheck"). No 18.6 returnable opclass sets xs_recheck on an index-only
// scan (btree resets it; gist/spgist leaf consistents are exact), so the
// witness scripts a lossy AM: indexam's Mock kind returns three index
// tuples with xs_recheck = true and the recheck qual keeps one of them.
mod rem_w2_011_recheck {
    use super::*;
    use ::types_relscan::{IndexScanOpaque, MOCK_AM_OID};

    fn mock_index_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid, heap_oid: Oid) -> Relation<'mcx> {
        index_relation_keyed_am(
            mcx,
            oid,
            heap_oid,
            int4_tupdesc(mcx),
            INT4OID,
            INT4_BTREE_OPFAMILY,
            MOCK_AM_OID,
        )
    }

    // The scripted scan: tids (0,1..=3) carrying values 10, 20, 30, all
    // rechecked; heap block 0 is all-visible so nothing is fetched.
    fn scripted<'mcx>(
        mcx: Mcx<'mcx>,
        recheck: bool,
    ) -> (EStateData<'mcx>, IndexOnlyScanState<'mcx>) {
        let node = mk_index_only_scan(mcx, 20);
        let heap_oid = fresh_oid();
        let index_oid = fresh_oid();
        register_pages(heap_oid, vec![build_heap_page(&[10, 20, 30])]);
        register_pages_in(heap_oid, vec![vm_page(&[0])], true);
        let rel = heap_relation(mcx, heap_oid);
        let index_rel = mock_index_relation(mcx, index_oid, heap_oid);
        let mut estate = EStateData::new_in(mcx);
        estate.es_snapshot = Some(static_mvcc_snapshot());
        // EXPLAIN ANALYZE shape: one instrumentation slot for the scan node.
        estate.es_instrument = ::types_core::instrument::INSTRUMENT_ROWS;
        estate
            .es_instrumentation
            .resize(1, ::types_core::instrument::Instrumentation::default());
        let mut state =
            exec_init_index_only_scan_rel(mcx, &node, &mut estate, rel, index_rel).unwrap();
        state.ss.instr_idx = Some(0);
        state.open_scandesc(&mut estate).unwrap();
        let scandesc = state.ioss_ScanDesc.as_deref_mut().expect("opened");
        assert!(scandesc.xs_want_itup);
        let IndexScanOpaque::Mock(m) = &mut scandesc.opaque else {
            panic!("the index relation must dispatch to the Mock AM");
        };
        for (i, v) in [10, 20, 30].into_iter().enumerate() {
            let tid = ItemPointerData::new(0, (i + 1) as u16);
            m.tids.push(tid);
            m.itups.push(itup_image(tid, v).0.to_vec());
        }
        m.recheck = recheck;
        (estate, state)
    }

    #[test]
    fn recheck_rejections_count_as_rows_removed_by_index_recheck() {
        let _g = serial();
        with_mcx(|mcx| {
            let (mut estate, mut state) = scripted(mcx, true);
            assert_eq!(drain(&mut state, &mut estate), vec![20], "recheck keeps k = 20 only");
            assert_eq!(
                estate.es_instrumentation[0].nfiltered2, 2.0,
                "nodeIndexonlyscan.c:206 InstrCountFiltered2 per recheck rejection"
            );
            teardown(state, &mut estate);
        });
    }

    // Control: an exact AM (xs_recheck false) never rechecks, so every
    // scripted tuple is returned and nothing is counted.
    #[test]
    fn exact_index_never_rechecks() {
        let _g = serial();
        with_mcx(|mcx| {
            let (mut estate, mut state) = scripted(mcx, false);
            assert_eq!(drain(&mut state, &mut estate), vec![10, 20, 30]);
            assert_eq!(estate.es_instrumentation[0].nfiltered2, 0.0);
            teardown(state, &mut estate);
        });
    }
}
