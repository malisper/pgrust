use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, InvalidBuffer, Oid, OffsetNumber, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use ::types_error::PgResult;
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_LEAF, BTP_META, BTP_ROOT, BTREE_MAGIC, BTREE_METAPAGE,
    BTREE_VERSION, P_HIKEY, P_NONE,
};
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, REPLICA_IDENTITY_DEFAULT,
};
use ::types_relscan::{IndexScanDescData, IndexScanOpaque};
use ::types_scan::scankey::{ScanKeyData, BTEqualStrategyNumber, BTGreaterStrategyNumber};
use ::types_scan::sdir::ForwardScanDirection;
use ::types_storage::bufpage::SizeOfPageHeaderData;
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumber};
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::TupleDescData;

// Fake buffer manager: pages are 8KB boxes; Buffer = block+1.

// MAXALIGNed like real buffer pages (the PageRef contract).
#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

// Index-tuple images are MAXALIGNed on real pages (itup module contract).
#[repr(C, align(8))]
struct Img<const N: usize>([u8; N]);

const HEAP_BUF_BASE: Buffer = 10000;

fn leak_page(p: Box<FakePage>) -> core::ptr::NonNull<FakePage> {
    core::ptr::NonNull::from(Box::leak(p))
}
const HEAP_OID: Oid = 4999;

// Pages are leaked and stored as raw pointers with one stable tag: repeated
// borrow_mut()+as_mut_ptr retags invalidated outstanding page pointers under
// Miri stacked borrows.
thread_local! {
    static PAGES: RefCell<Vec<core::ptr::NonNull<FakePage>>> = const { RefCell::new(Vec::new()) };
    static HEAP_PAGES: RefCell<Vec<core::ptr::NonNull<FakePage>>> = const { RefCell::new(Vec::new()) };
    static PINS: Cell<i32> = const { Cell::new(0) };
    static READS: Cell<u32> = const { Cell::new(0) };
    static DIRTY_HINTS: Cell<u32> = const { Cell::new(0) };
    static WAL: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static NEXT_LSN: Cell<u64> = const { Cell::new(0x1000) };
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        genam_seams::build_index_value_description::set(|_, _, _| Ok(None));
        syscache_seams::pg_namespace_nspname::set(|_| Ok(None));
        bufmgr_seams::read_buffer::set(|rel, blkno| {
            READS.with(|c| c.set(c.get() + 1));
            PINS.with(|c| c.set(c.get() + 1));
            if rel.rd_id == HEAP_OID {
                Ok(HEAP_BUF_BASE + blkno as Buffer + 1)
            } else {
                Ok(blkno as Buffer + 1)
            }
        });
        bufmgr_seams::release_buffer::set(|_buf| {
            PINS.with(|c| c.set(c.get() - 1));
            Ok(())
        });
        bufmgr_seams::release_and_read_buffer::set(|buf, rel, blkno| {
            if buf != InvalidBuffer {
                if buf == blkno as Buffer + 1 && rel.rd_id != HEAP_OID {
                    return Ok(buf); // C's same-block pin-keeping fastpath
                }
                bufmgr_seams::release_buffer::call(buf)?;
            }
            bufmgr_seams::read_buffer::call(rel, blkno)
        });
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::conditional_lock_buffer::set(|_buf| Ok(true));
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            if buf > HEAP_BUF_BASE {
                (buf - HEAP_BUF_BASE - 1) as BlockNumber
            } else {
                (buf - 1) as BlockNumber
            }
        });
        bufmgr_seams::buffer_get_page::set(|buf| {
            if buf > HEAP_BUF_BASE {
                HEAP_PAGES.with(|p| p.borrow()[(buf - HEAP_BUF_BASE - 1) as usize].cast::<u8>())
            } else {
                PAGES.with(|p| p.borrow()[(buf - 1) as usize].cast::<u8>())
            }
        });
        bufmgr_seams::incr_buffer_ref_count::set(|_buf| PINS.with(|c| c.set(c.get() + 1)));
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| {
            DIRTY_HINTS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        bufmgr_seams::buffer_get_lsn_atomic::set(|_buf| 0x1234);
        bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, flags, n| {
            assert!(rel.rd_id != HEAP_OID);
            assert_eq!(n, 1);
            assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
            let buf = PAGES.with(|p| {
                let mut pages = p.borrow_mut();
                pages.push(leak_page(Box::new(FakePage([0u8; BLCKSZ]))));
                pages.len() as Buffer
            });
            PINS.with(|c| c.set(c.get() + 1));
            Ok((buf, 1))
        });
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        xloginsert_seams::xlog_insert_record::set(|rmid, info, _flags, _main, _bufs| {
            assert_eq!(rmid, ::rmgr::RM_BTREE_ID as u8);
            WAL.with(|w| w.borrow_mut().push(info));
            let lsn = NEXT_LSN.get() + 8;
            NEXT_LSN.set(lsn);
            Ok(lsn)
        });
        predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_page_split::set(|_rel, _o, _n| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));
        pruneheap_seams::heap_page_prune_opt::set(|_rel, _buf| Ok(()));
        bufmgr_seams::relation_smgr_locator::set(|rel| ::types_storage::RelFileLocatorBackend {
            locator: ::types_storage::RelFileLocator {
                spcOid: 0,
                dbOid: 5,
                relNumber: rel.rd_rel.relfilenode,
            },
            backend: INVALID_PROC_NUMBER,
        });
        smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| 0);
        smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
        smgr_seams::smgr_exists::set(|_loc, _fork| Ok(false));
        heapam_visibility::init_seams();
        procarray_seams::global_vis_test_for::set(|_rel| {
            ::types_core::GlobalVisStateHandle::new(1)
        });
        procarray_seams::global_vis_test_is_removable_xid::set(|_vistest, xid| Ok(xid < 1000));
    });
}

fn wal_infos() -> Vec<u8> {
    WAL.with(|w| w.borrow().clone())
}

fn reset_wal() {
    WAL.with(|w| w.borrow_mut().clear());
}

// Page builders (int4 single-key-column index).

fn put_u16(p: &mut FakePage, off: usize, v: u16) {
    p.0[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

fn new_page(
    special_flags: u16,
    level: u32,
    prev: BlockNumber,
    next: BlockNumber,
) -> Box<FakePage> {
    let mut p = Box::new(FakePage([0u8; BLCKSZ]));
    let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    put_u16(&mut p, 12, SizeOfPageHeaderData as u16); // pd_lower
    put_u16(&mut p, 14, special as u16); // pd_upper
    put_u16(&mut p, 16, special as u16); // pd_special
    let opaque = BTPageOpaqueData {
        btpo_prev: prev,
        btpo_next: next,
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

fn meta_page(root: BlockNumber, level: u32) -> Box<FakePage> {
    meta_page_opts(root, level, true)
}

fn meta_page_opts(root: BlockNumber, level: u32, allequalimage: bool) -> Box<FakePage> {
    let mut p = new_page(BTP_META, 0, P_NONE, P_NONE);
    let metad = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: root,
        btm_level: level,
        btm_fastroot: root,
        btm_fastlevel: level,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: allequalimage,
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

// Append one 16-byte int4 index tuple (t_info & INDEX_ALT_TID_MASK unset).
fn add_tuple(p: &mut FakePage, tid: ItemPointerData, value: i32) -> OffsetNumber {
    let itupsz = 16usize;
    let pd_lower = u16::from_ne_bytes([p.0[12], p.0[13]]) as usize;
    let pd_upper = u16::from_ne_bytes([p.0[14], p.0[15]]) as usize;
    let off = pd_upper - itupsz;
    let t_info: u16 = itupsz as u16;
    // SAFETY: owned page bytes; ItemPointerData is a 6B POD.
    unsafe {
        p.0.as_mut_ptr()
            .add(off)
            .cast::<ItemPointerData>()
            .write_unaligned(tid);
    }
    p.0[off + 6..off + 8].copy_from_slice(&t_info.to_ne_bytes());
    p.0[off + 8..off + 12].copy_from_slice(&value.to_ne_bytes());
    let mut iid = ::types_storage::bufpage::ItemIdData::new(0, 0, 0);
    iid.set_normal(off as u16, itupsz as u16);
    // SAFETY: line-pointer slot in the owned page.
    unsafe {
        p.0.as_mut_ptr()
            .add(pd_lower)
            .cast::<::types_storage::bufpage::ItemIdData>()
            .write(iid)
    };
    put_u16(p, 12, (pd_lower + 4) as u16);
    put_u16(p, 14, off as u16);
    ((pd_lower - SizeOfPageHeaderData) / 4 + 1) as OffsetNumber
}

fn tid(blk: u32, pos: u16) -> ItemPointerData {
    ItemPointerData::new(blk, pos)
}

// Single leaf that is also the root: values in ascending order.
fn build_single_leaf_index(values: &[i32]) {
    let mut leaf = new_page(BTP_LEAF | BTP_ROOT, 0, P_NONE, P_NONE);
    for (i, v) in values.iter().enumerate() {
        add_tuple(&mut leaf, tid(10 + i as u32, 1), *v);
    }
    PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.clear();
        pages.push(leak_page(meta_page(1, 0)));
        pages.push(leak_page(leaf));
    });
    READS.with(|c| c.set(0));
}

fn int4_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen: 4,
        attbyval: true,
        attispackable: false,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: 4,
    });
    TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: compact,
        attrs: PgVec::new_in(mcx),
    }
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
    Ok(())
}

fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
    index_rel_opts(mcx, false)
}

fn index_rel_opts(mcx: Mcx<'_>, unique: bool) -> Relation<'_> {
    let mut relname = ::types_tuple::NameData::default();
    relname.namestrcpy("t_idx");
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1);
    let one = |v: Oid| {
        let mut vec = PgVec::new_in(mcx);
        vec.push(v);
        vec
    };
    let mut indoption = PgVec::new_in(mcx);
    indoption.push(0i16);
    let data = RelationData {
        rd_locator: Cell::new(::types_storage::RelFileLocator::new(1663, 5, 5000)),
        rd_smgr: Default::default(),
        rd_id: 5000,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: 5000, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: ::types_core::BTREE_AM_OID,
            relfilenode: 5000,
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
        rd_att: Rc::new(int4_tupdesc(mcx)),
        rd_index: Some(FormData_pg_index {
            indexrelid: 5000,
            indrelid: 4999,
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
        rd_opcintype: one(23),
        rd_opfamily: one(1976),
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
    };
    Relation::open(data, Some(noop_close))
}

// A test BTORDER_PROC: btint4cmp over by-value datums.
fn test_int4cmp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let a = fcinfo.arg(0).as_i32();
    let b = fcinfo.arg(1).as_i32();
    Ok(Datum::from_i32((a > b) as i32 - (a < b) as i32))
}

fn test_int4eq(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(
        fcinfo.arg(0).as_i32() == fcinfo.arg(1).as_i32(),
    ))
}

fn test_int4gt(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(
        fcinfo.arg(0).as_i32() > fcinfo.arg(1).as_i32(),
    ))
}

fn prime_supportinfo(rel: &Relation<'_>) {
    rel.rd_supportinfo
        .borrow_mut()
        .push(Some(FmgrInfo::new(test_int4cmp, 351, 2, true, false)));
}

fn key(attno: i16, arg: i32, func: ::types_fmgr::PGFunction, strategy: u16) -> ScanKeyData {
    let mut k = ScanKeyData::empty();
    k.sk_attno = attno;
    k.sk_strategy = strategy;
    k.sk_func = FmgrInfo::new(func, 65, 2, true, false);
    k.sk_argument = Datum::from_i32(arg);
    k
}

fn begin_scan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    keys: &[ScanKeyData],
) -> IndexScanDescData<'mcx> {
    let mut scan = crate::btbeginscan(mcx, rel, keys.len() as i32, 0).unwrap();
    scan.heapRelation = Some(rel.alias()); // stand-in: only is_some() is read
    crate::btrescan(&mut scan, Some(keys)).unwrap();
    scan
}

#[test]
fn metaversion_uses_and_primes_amcache() {
    install();
    build_single_leaf_index(&[1, 2, 3]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());

    assert!(rel.rd_amcache.get().is_none());
    let (heapkeyspace, allequalimage) = crate::bt_metaversion(&rel).unwrap();
    assert!(heapkeyspace && allequalimage);
    assert!(rel.rd_amcache.get().is_some());
    let reads = READS.with(Cell::get);
    // Cached: no further metapage reads.
    let _ = crate::bt_metaversion(&rel).unwrap();
    assert_eq!(crate::bt_getrootheight(&rel).unwrap(), 0);
    assert_eq!(READS.with(Cell::get), reads);
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
fn point_lookup_returns_matching_tids() {
    install();
    build_single_leaf_index(&[10, 20, 20, 30]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let keys = [key(1, 20, test_int4eq, BTEqualStrategyNumber)];
    let mut scan = begin_scan(cx.mcx(), &rel, &keys);

    assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    assert_eq!(ItemPointerGetBlockNumber(&scan.xs_heaptid), 11);
    assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    assert_eq!(ItemPointerGetBlockNumber(&scan.xs_heaptid), 12);
    assert!(!crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());

    crate::btendscan(&mut scan).unwrap();
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
    assert_eq!(scan.xs_pgstat_index_scans, 0, "pgstat disabled: no counts");
}

#[test]
fn want_itup_publishes_page_copied_tuples() {
    install();
    build_single_leaf_index(&[10, 20, 20, 30]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let keys = [key(1, 20, test_int4eq, BTEqualStrategyNumber)];
    let mut scan = crate::btbeginscan(cx.mcx(), &rel, keys.len() as i32, 0).unwrap();
    scan.heapRelation = Some(rel.alias());
    scan.xs_want_itup = true;
    crate::btrescan(&mut scan, Some(&keys)).unwrap();
    assert!(scan.xs_itupdesc.is_some());
    {
        let IndexScanOpaque::Btree(so) = &scan.opaque else { unreachable!() };
        assert!(so.currTuples.is_some() && so.markTuples.is_some());
        assert!(!so.dropPin);
    }

    let mut vals = Vec::new();
    while crate::btgettuple(&mut scan, ForwardScanDirection).unwrap() {
        let itup = scan.xs_itup.expect("xs_want_itup publishes xs_itup").as_ptr();
        let desc = scan.xs_itupdesc.as_deref().unwrap();
        let mut isnull = false;
        // SAFETY: xs_itup points at a MAXALIGNed copy in so.currTuples.
        let v = unsafe { crate::itup::index_getattr(itup, 1, desc, &mut isnull) };
        assert!(!isnull);
        // xs_itup is a currTuples copy, not a page pointer.
        {
            let IndexScanOpaque::Btree(so) = &scan.opaque else { unreachable!() };
            let buf = so.currTuples.as_ref().unwrap();
            let off = itup as usize - buf.as_ptr() as usize;
            assert!(off < ::types_core::BLCKSZ as usize);
        }
        vals.push(v.as_i32());
    }
    assert_eq!(vals, vec![20, 20]);

    crate::btendscan(&mut scan).unwrap();
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
fn missing_key_returns_false() {
    install();
    build_single_leaf_index(&[10, 20, 30]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let keys = [key(1, 25, test_int4eq, BTEqualStrategyNumber)];
    let mut scan = begin_scan(cx.mcx(), &rel, &keys);
    assert!(!crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    crate::btendscan(&mut scan).unwrap();
    assert_eq!(PINS.with(Cell::get), 0);
}

#[test]
fn qualless_scan_walks_from_the_endpoint() {
    install();
    build_single_leaf_index(&[7, 8]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());

    let mut scan = begin_scan(cx.mcx(), &rel, &[]);
    let mut seen = Vec::new();
    while crate::btgettuple(&mut scan, ForwardScanDirection).unwrap() {
        seen.push(ItemPointerGetBlockNumber(&scan.xs_heaptid));
    }
    assert_eq!(seen, vec![10, 11]);
    crate::btendscan(&mut scan).unwrap();
    assert_eq!(PINS.with(Cell::get), 0);
}

#[test]
fn backward_scan_from_rightmost() {
    install();
    build_single_leaf_index(&[7, 8, 9]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());

    let mut scan = begin_scan(cx.mcx(), &rel, &[]);
    let mut seen = Vec::new();
    while crate::btgettuple(&mut scan, ::types_scan::sdir::BackwardScanDirection).unwrap() {
        seen.push(ItemPointerGetBlockNumber(&scan.xs_heaptid));
    }
    assert_eq!(seen, vec![12, 11, 10]);
    crate::btendscan(&mut scan).unwrap();
    assert_eq!(PINS.with(Cell::get), 0);
}

#[test]
fn contradictory_quals_end_scan_without_io() {
    install();
    build_single_leaf_index(&[1, 2, 3]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let keys = [
        key(1, 1, test_int4eq, BTEqualStrategyNumber),
        key(1, 5, test_int4gt, BTGreaterStrategyNumber),
    ];
    let mut scan = begin_scan(cx.mcx(), &rel, &keys);
    READS.with(|c| c.set(0));
    // x = 1 AND x > 5: preprocessing proves it unsatisfiable (1 > 5 is false).
    assert!(!crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    assert_eq!(READS.with(Cell::get), 0, "no descent for a false qual");
    crate::btendscan(&mut scan).unwrap();
}

#[test]
fn mark_restore_on_one_page() {
    install();
    build_single_leaf_index(&[5, 6, 7]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());

    let mut scan = begin_scan(cx.mcx(), &rel, &[]);
    assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    crate::btmarkpos(&mut scan).unwrap();
    assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    assert_eq!(ItemPointerGetBlockNumber(&scan.xs_heaptid), 11);
    crate::btrestrpos(&mut scan).unwrap();
    assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    assert_eq!(ItemPointerGetBlockNumber(&scan.xs_heaptid), 11);
    crate::btendscan(&mut scan).unwrap();
    assert_eq!(PINS.with(Cell::get), 0);
}

#[test]
fn kill_prior_tuple_marks_lp_dead() {
    install();
    build_single_leaf_index(&[10, 20, 30]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);
    DIRTY_HINTS.with(|c| c.set(0));

    let keys = [key(1, 20, test_int4eq, BTEqualStrategyNumber)];
    let mut scan = begin_scan(cx.mcx(), &rel, &keys);
    assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    scan.kill_prior_tuple = true;
    assert!(!crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
    crate::btendscan(&mut scan).unwrap();

    assert_eq!(DIRTY_HINTS.with(Cell::get), 1);
    // offnum 2 (value 20) is LP_DEAD; BTP_HAS_GARBAGE set.
    PAGES.with(|p| {
        let pages = p.borrow();
        // SAFETY: leaked page, stable tag.
        let leaf = &unsafe { pages[1].as_ref() }.0;
        let iid_off = SizeOfPageHeaderData + 4; // second line pointer
        // SAFETY: reading the owned page image.
        let iid = unsafe {
            leaf.as_ptr()
                .add(iid_off)
                .cast::<::types_storage::bufpage::ItemIdData>()
                .read()
        };
        assert!(iid.is_dead());
        let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
        let flags = u16::from_ne_bytes([leaf[special + 12], leaf[special + 13]]);
        assert!(flags & ::types_nbtree::BTP_HAS_GARBAGE != 0);
    });
    assert_eq!(PINS.with(Cell::get), 0);
}

#[test]
fn redundant_inequalities_are_eliminated() {
    install();
    build_single_leaf_index(&[1, 2, 3, 4]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    // x > 2 AND x > 3: preprocessing keeps only the tighter x > 3.
    let keys = [
        key(1, 2, test_int4gt, BTGreaterStrategyNumber),
        key(1, 3, test_int4gt, BTGreaterStrategyNumber),
    ];
    let mut scan = begin_scan(cx.mcx(), &rel, &keys);
    let mut seen = Vec::new();
    while crate::btgettuple(&mut scan, ForwardScanDirection).unwrap() {
        seen.push(ItemPointerGetBlockNumber(&scan.xs_heaptid));
    }
    assert_eq!(seen, vec![13]); // only value 4
    let ::types_relscan::IndexScanOpaque::Btree(so) = &scan.opaque else {
        panic!()
    };
    assert_eq!(so.numberOfKeys, 1);
    assert_eq!(so.keyData[0].sk_argument.as_i32(), 3);
    crate::btendscan(&mut scan).unwrap();
}

fn heap_relation(mcx: Mcx<'_>) -> Relation<'_> {
    let mut relname = ::types_tuple::NameData::default();
    relname.namestrcpy("t");
    let data = RelationData {
        rd_locator: Default::default(),
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
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: ::tableam::HEAP_TABLE_AM_OID,
            relfilenode: HEAP_OID,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: true,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: ::types_rel::RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
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
    };
    Relation::open(data, Some(noop_close))
}

// Committed-hint heap tuple (28B header+int4): the dirty-snapshot recheck
// resolves it without transam/procarray probes.
fn heap_tuple_image(val: i32) -> [u8; 28] {
    let mut img = [0u8; 28];
    img[0..4].copy_from_slice(&10u32.to_ne_bytes()); // xmin
    img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts
    let infomask = ::types_tuple::HEAP_XMAX_INVALID | ::types_tuple::HEAP_XMIN_COMMITTED;
    img[20..22].copy_from_slice(&infomask.to_ne_bytes());
    img[22] = 24; // t_hoff
    img[24..28].copy_from_slice(&val.to_ne_bytes());
    img
}

fn build_heap_page(vals: &[i32]) -> Box<FakePage> {
    let mut page = Box::new(FakePage([0u8; BLCKSZ]));
    let n = vals.len();
    let lower = SizeOfPageHeaderData + n * 4;
    let mut upper = BLCKSZ;
    for (i, val) in vals.iter().enumerate() {
        let img = heap_tuple_image(*val);
        upper = (upper - img.len()) & !7;
        page.0[upper..upper + img.len()].copy_from_slice(&img);
        let mut id = ::types_storage::bufpage::ItemIdData::new(0, 0, 0);
        id.set_normal(upper as u16, img.len() as u16);
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

fn build_empty_index(allequalimage: bool) {
    PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.clear();
        pages.push(leak_page(meta_page_opts(P_NONE, 0, allequalimage)));
    });
    HEAP_PAGES.with(|p| p.borrow_mut().clear());
    READS.with(|c| c.set(0));
    reset_wal();
}

fn insert_key(rel: &Relation<'_>, heap: &Relation<'_>, key: i32, heap_tid: ItemPointerData) {
    let cx = MemoryContext::new("ins");
    crate::btinsert(
        cx.mcx(),
        rel,
        &[Datum::from_i32(key)],
        &[false],
        &heap_tid,
        heap,
        ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_NO,
        false,
    )
    .unwrap_or_else(|e| panic!("assertion: insert_key({key}, {heap_tid:?}) -> {e:?}"));
}

fn drain_forward(mcx: Mcx<'_>, rel: &Relation<'_>) -> Vec<ItemPointerData> {
    let mut scan = begin_scan(mcx, rel, &[]);
    let mut seen = Vec::new();
    while crate::btgettuple(&mut scan, ForwardScanDirection).unwrap() {
        seen.push(scan.xs_heaptid);
    }
    crate::btendscan(&mut scan).unwrap();
    seen
}

#[test]
fn insert_into_empty_index_builds_root() {
    install();
    build_empty_index(false);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    for k in [30, 10, 20, 40, 5] {
        insert_key(&rel, &rel, k, tid(100 + k as u32, 1));
    }

    let seen = drain_forward(cx.mcx(), &rel);
    let blocks: Vec<u32> = seen.iter().map(ItemPointerGetBlockNumber).collect();
    assert_eq!(blocks, vec![105, 110, 120, 130, 140]); // key order

    // root creation (NEWROOT) then five leaf inserts.
    assert_eq!(
        wal_infos(),
        vec![
            ::types_nbtree::XLOG_BTREE_NEWROOT,
            ::types_nbtree::XLOG_BTREE_INSERT_LEAF,
            ::types_nbtree::XLOG_BTREE_INSERT_LEAF,
            ::types_nbtree::XLOG_BTREE_INSERT_LEAF,
            ::types_nbtree::XLOG_BTREE_INSERT_LEAF,
            ::types_nbtree::XLOG_BTREE_INSERT_LEAF,
        ]
    );
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn sequential_inserts_split_and_stay_navigable() {
    install();
    build_empty_index(false);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let n = 1500u32;
    for k in 1..=n {
        insert_key(&rel, &rel, k as i32, tid(k, 1));
    }

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), n as usize);
    for (i, t) in seen.iter().enumerate() {
        assert_eq!(ItemPointerGetBlockNumber(t), i as u32 + 1);
    }

    let infos = wal_infos();
    let splits = infos
        .iter()
        .filter(|i| {
            **i == ::types_nbtree::XLOG_BTREE_SPLIT_R || **i == ::types_nbtree::XLOG_BTREE_SPLIT_L
        })
        .count();
    let newroots = infos
        .iter()
        .filter(|i| **i == ::types_nbtree::XLOG_BTREE_NEWROOT)
        .count();
    let uppers = infos
        .iter()
        .filter(|i| **i == ::types_nbtree::XLOG_BTREE_INSERT_UPPER)
        .count();
    assert!(splits >= 3, "expected several leaf splits, saw {splits}");
    assert_eq!(newroots, 2, "root creation + root split");
    assert_eq!(uppers, splits - 1, "each non-root split posts a downlink");

    assert_eq!(crate::bt_getrootheight(&rel).unwrap(), 1);

    for probe in [1i32, 366, 367, 1000, 1500] {
        let keys = [key(1, probe, test_int4eq, BTEqualStrategyNumber)];
        let mut scan = begin_scan(cx.mcx(), &rel, &keys);
        assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
        assert_eq!(ItemPointerGetBlockNumber(&scan.xs_heaptid), probe as u32);
        assert!(!crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
        crate::btendscan(&mut scan).unwrap();
    }

    let mut scan = begin_scan(cx.mcx(), &rel, &[]);
    let mut back = Vec::new();
    while crate::btgettuple(&mut scan, ::types_scan::sdir::BackwardScanDirection).unwrap() {
        back.push(ItemPointerGetBlockNumber(&scan.xs_heaptid));
    }
    crate::btendscan(&mut scan).unwrap();
    assert_eq!(back.len(), n as usize);
    assert!(back.windows(2).all(|w| w[0] > w[1]));

    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn interleaved_inserts_split_interior_pages() {
    install();
    build_empty_index(false);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    for k in (0..2000i32).step_by(2) {
        insert_key(&rel, &rel, k, tid(k as u32 + 1, 1));
    }
    for k in (1..2000i32).step_by(2).rev() {
        insert_key(&rel, &rel, k, tid(k as u32 + 1, 1));
    }

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), 2000);
    for (i, t) in seen.iter().enumerate() {
        assert_eq!(ItemPointerGetBlockNumber(t), i as u32 + 1);
    }
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn rightmost_fastpath_arms_on_a_three_level_tree() {
    install();
    build_empty_index(false);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    // Enough sequential inserts for a level-2 root (~400 leaves), the
    // BTREE_FASTPATH_MIN_LEVEL gate for the rightmost-block cache.
    let n = 160_000u32;
    for k in 1..=n {
        insert_key(&rel, &rel, k as i32, tid(k, 1));
    }
    assert!(crate::bt_getrootheight(&rel).unwrap() >= 2);

    // cached-target insert: ONE index page touched, no root descent.
    READS.with(|c| c.set(0));
    insert_key(&rel, &rel, (n + 1) as i32, tid(n + 1, 1));
    assert_eq!(READS.with(Cell::get), 1, "fastpath skipped the descent");

    for probe in [1u32, 12345, 100_000, n + 1] {
        let keys = [key(1, probe as i32, test_int4eq, BTEqualStrategyNumber)];
        let mut scan = begin_scan(cx.mcx(), &rel, &keys);
        assert!(crate::btgettuple(&mut scan, ForwardScanDirection).unwrap());
        assert_eq!(ItemPointerGetBlockNumber(&scan.xs_heaptid), probe);
        crate::btendscan(&mut scan).unwrap();
    }
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
fn unique_index_rejects_live_duplicate() {
    install();
    build_empty_index(false);
    HEAP_PAGES.with(|p| {
        p.borrow_mut().push(leak_page(build_heap_page(&[10, 20, 30])));
    });
    let cx = MemoryContext::new("t");
    let rel = index_rel_opts(cx.mcx(), true);
    prime_supportinfo(&rel);
    let heap = heap_relation(cx.mcx());

    let unique_insert = |k: i32, htid: ItemPointerData| {
        let icx = MemoryContext::new("ins");
        crate::btinsert(
            icx.mcx(),
            &rel,
            &[Datum::from_i32(k)],
            &[false],
            &htid,
            &heap,
            ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_YES,
            false,
        )
    };

    assert!(unique_insert(10, tid(0, 1)).unwrap());
    assert!(unique_insert(20, tid(0, 2)).unwrap());
    assert!(unique_insert(30, tid(0, 3)).unwrap());

    // key 20 again, pointing at another live row: 23505.
    let err = unique_insert(20, tid(0, 3)).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_UNIQUE_VIOLATION);
    assert!(err.message().contains("duplicate key value"));

    // distinct key still fine after the failure.
    assert!(unique_insert(25, tid(0, 1)).unwrap());

    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
fn unique_check_partial_reports_conflict_and_inserts_anyway() {
    install();
    build_empty_index(false);
    HEAP_PAGES.with(|p| {
        p.borrow_mut().push(leak_page(build_heap_page(&[10, 20, 30])));
    });
    let cx = MemoryContext::new("t");
    let rel = index_rel_opts(cx.mcx(), true);
    prime_supportinfo(&rel);
    let heap = heap_relation(cx.mcx());

    let partial_insert = |k: i32, htid: ItemPointerData| {
        let icx = MemoryContext::new("ins");
        crate::btinsert(
            icx.mcx(),
            &rel,
            &[Datum::from_i32(k)],
            &[false],
            &htid,
            &heap,
            ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_PARTIAL,
            false,
        )
    };

    assert!(partial_insert(10, tid(0, 1)).unwrap());
    assert!(partial_insert(20, tid(0, 2)).unwrap());
    assert!(partial_insert(30, tid(0, 3)).unwrap());

    // Duplicate under PARTIAL: no error, is_unique=false, entry still inserted.
    assert!(!partial_insert(20, tid(0, 3)).unwrap());
    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), 4, "conflicting entry was inserted");

    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
fn unique_check_existing_rechecks_without_inserting() {
    install();
    build_empty_index(false);
    HEAP_PAGES.with(|p| {
        p.borrow_mut().push(leak_page(build_heap_page(&[10, 20, 30])));
    });
    let cx = MemoryContext::new("t");
    let rel = index_rel_opts(cx.mcx(), true);
    prime_supportinfo(&rel);
    let heap = heap_relation(cx.mcx());

    use ::types_nbtree::genam::IndexUniqueCheck::{UNIQUE_CHECK_EXISTING, UNIQUE_CHECK_YES};
    let insert = |k: i32, htid: ItemPointerData, mode| {
        let icx = MemoryContext::new("ins");
        crate::btinsert(
            icx.mcx(),
            &rel,
            &[Datum::from_i32(k)],
            &[false],
            &htid,
            &heap,
            mode,
            false,
        )
    };

    assert!(insert(10, tid(0, 1), UNIQUE_CHECK_YES).unwrap());
    assert!(insert(20, tid(0, 2), UNIQUE_CHECK_YES).unwrap());
    assert!(insert(30, tid(0, 3), UNIQUE_CHECK_YES).unwrap());

    // Recheck of a non-conflicting entry: re-finds itself, inserts nothing.
    assert!(insert(20, tid(0, 2), UNIQUE_CHECK_EXISTING).unwrap());
    assert_eq!(drain_forward(cx.mcx(), &rel).len(), 3, "recheck never inserts");

    // Recheck that finds another live row under its key: 23505.
    let err = insert(20, tid(0, 3), UNIQUE_CHECK_EXISTING).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_UNIQUE_VIOLATION);
    assert!(err.message().contains("duplicate key value"));

    // Recheck that cannot re-find its tuple: internal re-find failure.
    let err = insert(99, tid(0, 1), UNIQUE_CHECK_EXISTING).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    assert!(err.message().contains("failed to re-find tuple"));

    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn unique_check_walks_posting_list_tids() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel_opts(cx.mcx(), true);
    prime_supportinfo(&rel);
    let heap = heap_relation(cx.mcx());

    HEAP_PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.push(leak_page(build_dead_heap_page(220)));
        pages.push(leak_page(build_dead_heap_page(220)));
        pages.push(leak_page(build_heap_page(&[20, 20])));
    });

    // 440 dead-TID duplicates: page-full dedup folds them into posting lists.
    for k in 1..=440u32 {
        let (blk, pos) = ((k - 1) / 220, ((k - 1) % 220 + 1) as u16);
        insert_key(&rel, &heap, 20, tid(blk, pos));
    }
    assert!(
        wal_infos().contains(&::types_nbtree::XLOG_BTREE_DEDUP),
        "posting lists formed"
    );

    let unique_insert = |k: i32, htid: ItemPointerData| {
        let icx = MemoryContext::new("ins");
        crate::btinsert(
            icx.mcx(),
            &rel,
            &[Datum::from_i32(k)],
            &[false],
            &htid,
            &heap,
            ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_YES,
            false,
        )
    };

    assert!(unique_insert(20, tid(2, 1)).unwrap());
    // a live duplicate sitting past the dead posting lists: 23505.
    let err = unique_insert(20, tid(2, 2)).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_UNIQUE_VIOLATION);

    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn posting_split_page_split_coincidence_keeps_every_tid() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    // even posids dedup into postings; the odd pass splits them until the
    // page can no longer dedup and _bt_split runs with postingoff != 0.
    for i in 1..=800u16 {
        insert_key(&rel, &rel, 7, tid(1, i * 2));
    }
    for i in 1..=799u16 {
        insert_key(&rel, &rel, 7, tid(1, i * 2 + 1));
    }

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), 1599);
    for (i, t) in seen.iter().enumerate() {
        assert_eq!(ItemPointerGetBlockNumber(t), 1);
        assert_eq!(t.ip_posid, i as u16 + 2);
    }
    let infos = wal_infos();
    assert!(
        infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_L)
            || infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_R),
        "churn must split: {infos:?}"
    );
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn bottomup_deletion_avoids_split_when_chains_are_dead() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);
    let heap = heap_relation(cx.mcx());

    HEAP_PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.push(leak_page(build_dead_heap_page(220)));
        pages.push(leak_page(build_dead_heap_page(220)));
    });

    // indexUnchanged inserts over all-dead HOT chains: page fill triggers
    // _bt_bottomupdel_pass (no LP_DEAD bits anywhere), which must free space
    // instead of splitting.
    let unchanged_insert = |k: i32, htid: ItemPointerData| {
        let icx = MemoryContext::new("ins");
        crate::btinsert(
            icx.mcx(),
            &rel,
            &[Datum::from_i32(k)],
            &[false],
            &htid,
            &heap,
            ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_NO,
            true,
        )
        .unwrap();
    };

    for k in 1..=440u32 {
        let (blk, pos) = ((k - 1) / 220, ((k - 1) % 220 + 1) as u16);
        unchanged_insert(k as i32, tid(blk, pos));
    }

    let infos = wal_infos();
    assert!(
        infos.contains(&::types_nbtree::XLOG_BTREE_DELETE),
        "bottom-up deletion must have fired: {infos:?}"
    );
    assert!(
        !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_L)
            && !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_R),
        "page split avoided: {infos:?}"
    );
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn allequalimage_distinct_keys_dedup_is_noop_then_split() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    // all-distinct page full: zero intervals, no WAL, split proceeds.
    for k in 1..=500i32 {
        insert_key(&rel, &rel, k, tid(k as u32, 1));
    }

    let infos = wal_infos();
    assert!(!infos.contains(&::types_nbtree::XLOG_BTREE_DEDUP));
    assert!(infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_R));

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), 500);
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn dedup_pass_merges_duplicates_onto_one_leaf() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let n = 1200u32;
    for i in 1..=n {
        insert_key(&rel, &rel, 42, tid(i, 1));
    }

    let infos = wal_infos();
    let dedups = infos.iter().filter(|i| **i == ::types_nbtree::XLOG_BTREE_DEDUP).count();
    assert!(dedups >= 1, "expected dedup passes, saw none");
    assert!(
        !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_R)
            && !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_L),
        "1200 duplicates of one int4 key must fit a single deduplicated leaf"
    );

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), n as usize);
    for (i, t) in seen.iter().enumerate() {
        assert_eq!(ItemPointerGetBlockNumber(t), i as u32 + 1, "TID order preserved");
    }
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn single_value_strategy_splits_after_six_capped_postings() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let n = 4000u32;
    for i in 1..=n {
        insert_key(&rel, &rel, 42, tid(i, 1));
    }

    let infos = wal_infos();
    assert!(infos.iter().any(|i| *i == ::types_nbtree::XLOG_BTREE_DEDUP));
    assert!(infos
        .iter()
        .any(|i| *i == ::types_nbtree::XLOG_BTREE_SPLIT_R || *i == ::types_nbtree::XLOG_BTREE_SPLIT_L));

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), n as usize);
    for (i, t) in seen.iter().enumerate() {
        assert_eq!(ItemPointerGetBlockNumber(t), i as u32 + 1);
    }
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn dedup_mixed_keys_only_merges_equal_runs() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let mut expect: Vec<(i32, u32)> = Vec::new();
    for round in 0..30u32 {
        for k in 0..40i32 {
            insert_key(&rel, &rel, k * 2, tid(1 + k as u32 * 1000 + round, 1));
            expect.push((k * 2, 1 + k as u32 * 1000 + round));
        }
    }
    for k in 0..40i32 {
        insert_key(&rel, &rel, k * 2 + 1, tid(500_000 + k as u32, 1));
        expect.push((k * 2 + 1, 500_000 + k as u32));
    }
    expect.sort();

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), expect.len());
    for (t, (_, blk)) in seen.iter().zip(expect.iter()) {
        assert_eq!(ItemPointerGetBlockNumber(t), *blk);
    }
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

#[test]
fn index_getattr_reads_values_and_caches_offsets() {
    let cx = MemoryContext::new("t");
    let tupdesc = int4_tupdesc(cx.mcx());
    // 16B tuple image: 6B tid + 2B info + 4-byte int4 at offset 8.
    let mut img = Img([0u8; 16]);
    let img = &mut img.0;
    img[6..8].copy_from_slice(&16u16.to_ne_bytes());
    img[8..12].copy_from_slice(&777i32.to_ne_bytes());

    let mut isnull = true;
    // SAFETY: img is a live, aligned index-tuple image.
    let d = unsafe { crate::itup::index_getattr(img.as_ptr(), 1, &tupdesc, &mut isnull) };
    assert!(!isnull);
    assert_eq!(d.as_i32(), 777);
    // attcacheoff (rule-5) primed by the nocache walk.
    assert_eq!(tupdesc.compact_attrs[0].attcacheoff.get(), 0);
    let d2 = unsafe { crate::itup::index_getattr(img.as_ptr(), 1, &tupdesc, &mut isnull) };
    assert_eq!(d2.as_i32(), 777);
}

#[test]
fn index_getattr_null_bitmap() {
    let cx = MemoryContext::new("t");
    let tupdesc = int4_tupdesc(cx.mcx());
    // Nulls bitmap present: 8B header + bitmap (attr 1 null) + pad to 16.
    let mut img = Img([0u8; 16]);
    let img = &mut img.0;
    let t_info: u16 = 16 | crate::itup::INDEX_NULL_MASK;
    img[6..8].copy_from_slice(&t_info.to_ne_bytes());
    img[8] = 0; // bit 0 clear => attr 1 is NULL

    let mut isnull = false;
    // SAFETY: img is a live, aligned index-tuple image.
    let d = unsafe { crate::itup::index_getattr(img.as_ptr(), 1, &tupdesc, &mut isnull) };
    assert!(isnull);
    assert_eq!(d.as_usize(), 0);
}

#[test]
fn bt_tuple_shape_decoders() {
    // Posting tuple: INDEX_ALT_TID_MASK + BT_IS_POSTING in ip_posid.
    let mut img = Img([0u8; 32]);
    let img = &mut img.0;
    let t_info: u16 = 32 | 0x2000; // INDEX_ALT_TID_MASK
    img[6..8].copy_from_slice(&t_info.to_ne_bytes());
    // t_tid: posting offset 16 in the block field; nposting=2 | BT_IS_POSTING.
    let tid0 = ItemPointerData::new(16, 0x2000 | 2);
    let (t1, t2) = (tid(7, 1), tid(9, 2));
    // SAFETY: owned image writes/reads within bounds.
    unsafe {
        img.as_mut_ptr()
            .cast::<ItemPointerData>()
            .write_unaligned(tid0);
        img.as_mut_ptr()
            .add(16)
            .cast::<ItemPointerData>()
            .write_unaligned(t1);
        img.as_mut_ptr()
            .add(22)
            .cast::<ItemPointerData>()
            .write_unaligned(t2);
        let p = img.as_ptr();
        assert!(crate::itup::bt_tuple_is_posting(p));
        assert!(!crate::itup::bt_tuple_is_pivot(p));
        assert_eq!(crate::itup::bt_tuple_get_nposting(p), 2);
        assert_eq!(crate::itup::bt_tuple_get_heap_tid(p), Some(t1));
        assert_eq!(crate::itup::bt_tuple_get_max_heap_tid(p), t2);
    }
}

#[test]
fn high_key_offset_constant() {
    assert_eq!(P_HIKEY, 1);
    assert_eq!(BTREE_METAPAGE, 0);
}

#[test]
fn mkscankey_builds_insertion_key() {
    install();
    build_single_leaf_index(&[1]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let mut img = Img([0u8; 16]);
    let heap_tid = tid(42, 3);
    // SAFETY: owned image writes within bounds.
    unsafe {
        img.0
            .as_mut_ptr()
            .cast::<ItemPointerData>()
            .write_unaligned(heap_tid)
    };
    img.0[6..8].copy_from_slice(&16u16.to_ne_bytes());
    img.0[8..12].copy_from_slice(&555i32.to_ne_bytes());

    let mut key = crate::bt_mkscankey(&rel, Some(img.0.as_ptr())).unwrap();
    assert!(key.heapkeyspace && key.allequalimage);
    assert!(!key.anynullkeys && !key.nextkey && !key.backward);
    assert_eq!(key.scantid, Some(heap_tid));
    let keys = key.keys_mut();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].sk_attno, 1);
    assert_eq!(keys[0].sk_argument.as_i32(), 555);
    assert_eq!(keys[0].sk_flags, 0);
    assert_eq!(keys[0].sk_func.fn_oid, 351);

    // Utility-statement arm: no tuple, no metapage read; keys are built
    // SK_ISNULL with unset arguments (nbtsort reads sk_func/sk_collation).
    let mut key = crate::bt_mkscankey(&rel, None).unwrap();
    assert!(key.heapkeyspace && !key.allequalimage);
    assert!(key.anynullkeys, "truncated attributes count as null keys");
    assert_eq!(key.scantid, None);
    let keys = key.keys_mut();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].sk_flags & types_scan::scankey::SK_ISNULL, types_scan::scankey::SK_ISNULL);
    assert_eq!(keys[0].sk_func.fn_oid, 351);
}

// Committed-delete heap tuple: xmin/xmax hinted committed, xmax removable per
// the vistest seam, so SnapshotNonVacuumable sees it as DEAD.
fn dead_heap_tuple_image(val: i32) -> [u8; 28] {
    let mut img = [0u8; 28];
    img[0..4].copy_from_slice(&10u32.to_ne_bytes()); // xmin
    img[4..8].copy_from_slice(&20u32.to_ne_bytes()); // xmax
    img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts
    let infomask = ::types_tuple::HEAP_XMIN_COMMITTED | ::types_tuple::HEAP_XMAX_COMMITTED;
    img[20..22].copy_from_slice(&infomask.to_ne_bytes());
    img[22] = 24; // t_hoff
    img[24..28].copy_from_slice(&val.to_ne_bytes());
    img
}

fn build_dead_heap_page(n: usize) -> Box<FakePage> {
    let mut page = Box::new(FakePage([0u8; BLCKSZ]));
    let lower = SizeOfPageHeaderData + n * 4;
    let mut upper = BLCKSZ;
    for i in 0..n {
        let img = dead_heap_tuple_image(i as i32);
        upper = (upper - img.len()) & !7;
        page.0[upper..upper + img.len()].copy_from_slice(&img);
        let mut id = ::types_storage::bufpage::ItemIdData::new(0, 0, 0);
        id.set_normal(upper as u16, img.len() as u16);
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

// killitems-shape LP_DEAD stores over every data item on leaf block `blk`.
fn mark_leaf_items_dead(blk: usize) {
    PAGES.with(|p| {
        let pages = p.borrow();
        // SAFETY: leaked page, stable tag; harness is single-threaded.
        let page = unsafe { &mut *pages[blk].as_ptr() };
        let lower = u16::from_ne_bytes([page.0[12], page.0[13]]) as usize;
        let nitems = (lower - SizeOfPageHeaderData) / 4;
        for i in 0..nitems {
            let off = SizeOfPageHeaderData + i * 4;
            let raw = u32::from_ne_bytes(page.0[off..off + 4].try_into().unwrap());
            // SAFETY: repr(transparent) over u32.
            let mut id: ::types_storage::bufpage::ItemIdData = unsafe { core::mem::transmute(raw) };
            id.mark_dead();
            let raw: u32 = unsafe { core::mem::transmute(id) };
            page.0[off..off + 4].copy_from_slice(&raw.to_ne_bytes());
        }
    });
}

#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn lp_dead_page_fill_runs_simple_deletion_instead_of_split() {
    install();
    build_empty_index(false); // allequalimage=false: dedup can't mask deletion
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);
    let heap = heap_relation(cx.mcx());

    // 220 x (28B tuple, 32B stride) + 220 line pointers fits one page
    HEAP_PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.push(leak_page(build_dead_heap_page(220)));
        pages.push(leak_page(build_dead_heap_page(220)));
    });

    let setup = 380u32;
    for k in 1..=setup {
        let (blk, pos) = (((k - 1) / 220) as u32, ((k - 1) % 220 + 1) as u16);
        insert_key(&rel, &heap, k as i32, tid(blk, pos));
    }
    mark_leaf_items_dead(1);
    reset_wal();

    // TIDs continue the sequence: heap_index_delete_tuples' shellsort asserts
    // strict TID uniqueness on a leaf, as C
    for k in setup + 1..=setup + 60 {
        let (blk, pos) = (((k - 1) / 220) as u32, ((k - 1) % 220 + 1) as u16);
        insert_key(&rel, &heap, k as i32, tid(blk, pos));
    }

    let infos = wal_infos();
    assert!(
        infos.contains(&::types_nbtree::XLOG_BTREE_DELETE),
        "simple deletion must have fired: {infos:?}"
    );
    assert!(
        !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_L)
            && !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_R),
        "page split avoided: {infos:?}"
    );

    let seen = drain_forward(cx.mcx(), &rel);
    assert!(seen.len() <= 60, "deleted tuples stay deleted: {}", seen.len());
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}
