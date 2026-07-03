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

// ------------------------------------------------------------------
// Fake buffer manager: pages are 8KB boxes; Buffer = block+1.
// ------------------------------------------------------------------

// MAXALIGNed like real buffer pages (the PageRef contract).
#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

// Index-tuple images are MAXALIGNed on real pages (itup module contract).
#[repr(C, align(8))]
struct Img<const N: usize>([u8; N]);

thread_local! {
    static PAGES: RefCell<Vec<Box<FakePage>>> = const { RefCell::new(Vec::new()) };
    static PINS: Cell<i32> = const { Cell::new(0) };
    static READS: Cell<u32> = const { Cell::new(0) };
    static DIRTY_HINTS: Cell<u32> = const { Cell::new(0) };
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        bufmgr_seams::read_buffer::set(|_rel, blkno| {
            READS.with(|c| c.set(c.get() + 1));
            PINS.with(|c| c.set(c.get() + 1));
            Ok(blkno as Buffer + 1)
        });
        bufmgr_seams::release_buffer::set(|_buf| {
            PINS.with(|c| c.set(c.get() - 1));
            Ok(())
        });
        bufmgr_seams::release_and_read_buffer::set(|buf, rel, blkno| {
            if buf != InvalidBuffer {
                if buf == blkno as Buffer + 1 {
                    return Ok(buf); // C's same-block pin-keeping fastpath
                }
                bufmgr_seams::release_buffer::call(buf)?;
            }
            bufmgr_seams::read_buffer::call(rel, blkno)
        });
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
        bufmgr_seams::buffer_get_page::set(|buf| {
            PAGES.with(|p| {
                core::ptr::NonNull::new(p.borrow_mut()[(buf - 1) as usize].0.as_mut_ptr())
                    .expect("page")
            })
        });
        bufmgr_seams::incr_buffer_ref_count::set(|_buf| PINS.with(|c| c.set(c.get() + 1)));
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| {
            DIRTY_HINTS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        bufmgr_seams::buffer_get_lsn_atomic::set(|_buf| 0x1234);
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
    });
}

// ------------------------------------------------------------------
// Page builders (int4 single-key-column index).
// ------------------------------------------------------------------

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
        pages.push(meta_page(1, 0));
        pages.push(leaf);
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
            indisunique: false,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisvalid: true,
            indisready: true,
            indkey,
            has_indpred: false,
        }),
        rd_opcintype: one(23),
        rd_opfamily: one(1976),
        rd_indoption: indoption,
        rd_indcollation: one(0),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        rd_amcache: Default::default(),
        rd_supportinfo: Default::default(),
        rd_indexlist: Default::default(),
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

// Test operator procs.
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

// ------------------------------------------------------------------

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
        let leaf = &pages[1].0;
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

// ------------------------------------------------------------------
// itup kernel (Miri target: no seams, pure in-memory bytes).
// ------------------------------------------------------------------

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

    // Utility-statement arm: no tuple, no metapage read.
    let mut key = crate::bt_mkscankey(&rel, None).unwrap();
    assert!(key.heapkeyspace && !key.allequalimage);
    assert!(key.anynullkeys, "truncated attributes count as null keys");
    assert_eq!(key.scantid, None);
    assert_eq!(key.keys_mut().len(), 0);
}
