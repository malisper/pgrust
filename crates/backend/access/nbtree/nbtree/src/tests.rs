use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, InvalidBuffer, OffsetNumber, Oid, BLCKSZ, INVALID_PROC_NUMBER,
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
use ::types_scan::scankey::{BTEqualStrategyNumber, BTGreaterStrategyNumber, ScanKeyData};
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
    // CritSectionCount observed at each xlog_insert_record call (audit
    // b002: C brackets every btree page mutation + XLogInsert in
    // START/END_CRIT_SECTION; a WAL insert seen at count 0 is a divergence).
    static WAL_CRIT: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static NEXT_LSN: Cell<u64> = const { Cell::new(0x1000) };
    static PRED_LOCK_RELATION_CALLS: Cell<u32> = const { Cell::new(0) };
    // A key a concurrent writer inserts into the (empty) index while the
    // reader is taking its relation-level predicate lock.
    static INSERT_ON_PRED_LOCK: Cell<Option<i32>> = const { Cell::new(None) };
}

// Every ereport the crate emits below ERROR (audit w2-023: the nbtpage.c /
// nbtree.c index-corruption LOG sites) lands here, keyed by the emitting
// thread — the elog seam is process-wide while tests run in parallel.
pgsync::process_global! {
    static EREPORTS: pgsync::Mutex<Vec<(std::thread::ThreadId, ::types_error::PgError)>> =
        pgsync::Mutex::new(Vec::new());
}

fn take_ereports() -> Vec<::types_error::PgError> {
    let me = std::thread::current().id();
    let mut all = pgsync::lock(&EREPORTS);
    let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut *all)
        .into_iter()
        .partition(|e| e.0 == me);
    *all = rest;
    mine.into_iter().map(|e| e.1).collect()
}

// The writer's root creation, as _bt_newroot leaves it: one leaf that is
// also the root, and a metapage pointing at it.
fn create_root_leaf(value: i32) {
    let mut leaf = new_page(BTP_LEAF | BTP_ROOT, 0, P_NONE, P_NONE);
    add_tuple(&mut leaf, tid(10, 1), value);
    PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.push(leak_page(leaf));
        let root = (pages.len() - 1) as BlockNumber;
        // SAFETY: metapage contents at +24 on the leaked metapage.
        unsafe {
            let metad = pages[0]
                .as_ptr()
                .cast::<u8>()
                .add(SizeOfPageHeaderData)
                .cast::<BTMetaPageData>();
            (*metad).btm_root = root;
            (*metad).btm_fastroot = root;
            (*metad).btm_level = 0;
            (*metad).btm_fastlevel = 0;
        }
    });
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        genam_seams::build_index_value_description::set(|_, _, _| Ok(None));
        syscache_seams::pg_namespace_nspname::set(|_| Ok(None));
        ::elog_seams::ereport::set(|err| {
            assert!(
                err.level().0 < ::types_error::ERROR.0,
                "ereport seam: {}",
                err.message()
            );
            pgsync::lock(&EREPORTS).push((std::thread::current().id(), err));
            Ok(())
        });
        // Shared-memory registry + LWLocks (BtreeVacuumLock): the same boot
        // a real cluster gets from CreateSharedMemoryAndSemaphores.
        init_small::globals::SetMyProcNumber(0);
        init_small::globals::SetMaxBackends(8);
        shmem::init_seams();
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        lwlock::CreateLWLocks(false).unwrap();
        crate::BTreeShmemInit().unwrap();
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
        bufmgr_seams::lock_buffer_for_cleanup::set(|_buf| Ok(()));
        bufmgr_seams::conditional_lock_buffer::set(|_buf| Ok(true));
        bufmgr_seams::read_buffer_extended::set(|rel, _fork, blkno, _mode, _strategy| {
            bufmgr_seams::read_buffer::call(rel, blkno)
        });
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
        transam_xlog_seams::xlog_logical_info_active::set(|| false);
        catalog_seams::is_catalog_relation::set(|_rel| false);
        xloginsert_seams::xlog_insert_record::set(|rmid, info, _flags, _main, _bufs| {
            assert_eq!(rmid, ::rmgr::RM_BTREE_ID as u8);
            WAL.with(|w| w.borrow_mut().push(info));
            WAL_CRIT.with(|w| w.borrow_mut().push(init_small::globals::CritSectionCount()));
            let lsn = NEXT_LSN.get() + 8;
            NEXT_LSN.set(lsn);
            Ok(lsn)
        });
        // toast_compress_datum stand-in (index_form_tuple's TOAST_INDEX_HACK
        // arm): a run of one byte "compresses" to a 16-byte image with a
        // compressed 4B header; anything else is incompressible (None), the
        // way pglz reports random bytes.
        heaptoast_seams::toast_compress_datum::set(|mcx, value, _cmethod| {
            let payload = &value[4..];
            if payload.len() < 32 || payload.iter().any(|b| *b != payload[0]) {
                return Ok(None);
            }
            let mut v: PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, 16)?;
            // VARATT_IS_4B_C: little-endian (len << 2) | 0x02
            ::mcx::vec_append_bytes(&mut v, &((16u32 << 2) | 0x02).to_le_bytes())?;
            // va_tcinfo: rawsize (cmethod bits 0 = pglz)
            ::mcx::vec_append_bytes(&mut v, &(payload.len() as u32).to_le_bytes())?;
            ::mcx::vec_append_bytes(&mut v, &[payload[0]; 8])?;
            Ok(Some(v))
        });
        predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_page_split::set(|_rel, _o, _n| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_rel, _tid, _snap, _xid| Ok(()));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_rel, _snap| Ok(false));
        predicate_seams::predicate_lock_page::set(|_rel, _blk, _snap| Ok(()));
        predicate_seams::predicate_lock_relation::set(|_rel, _snap| {
            PRED_LOCK_RELATION_CALLS.with(|c| c.set(c.get() + 1));
            if let Some(v) = INSERT_ON_PRED_LOCK.with(Cell::take) {
                create_root_leaf(v);
            }
            Ok(())
        });
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
    WAL_CRIT.with(|w| w.borrow_mut().clear());
}

fn wal_crit_counts() -> Vec<u32> {
    WAL_CRIT.with(|w| w.borrow().clone())
}

// Page builders (int4 single-key-column index).

fn put_u16(p: &mut FakePage, off: usize, v: u16) {
    p.0[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

fn new_page(special_flags: u16, level: u32, prev: BlockNumber, next: BlockNumber) -> Box<FakePage> {
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
    // A typed store leaves padding uninitialized; page_meta reads bytes.
    let img = metad.page_image();
    p.0[SizeOfPageHeaderData..SizeOfPageHeaderData + img.len()].copy_from_slice(&img);
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
            lockRelId: LockRelId {
                relId: 5000,
                dbId: 5,
            },
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
            indcheckxmin: false,
            indxmin: 0,
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
        let IndexScanOpaque::Btree(so) = &scan.opaque else {
            unreachable!()
        };
        assert!(so.currTuples.is_some() && so.markTuples.is_some());
        assert!(!so.dropPin);
    }

    let mut vals = Vec::new();
    while crate::btgettuple(&mut scan, ForwardScanDirection).unwrap() {
        let itup = scan
            .xs_itup
            .expect("xs_want_itup publishes xs_itup")
            .as_ptr();
        let desc = scan.xs_itupdesc.as_deref().unwrap();
        let mut isnull = false;
        // SAFETY: xs_itup points at a MAXALIGNed copy in so.currTuples.
        let v = unsafe { crate::itup::index_getattr(itup, 1, desc, &mut isnull) };
        assert!(!isnull);
        // xs_itup is a currTuples copy, not a page pointer.
        {
            let IndexScanOpaque::Btree(so) = &scan.opaque else {
                unreachable!()
            };
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
            lockRelId: LockRelId {
                relId: HEAP_OID,
                dbId: 5,
            },
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
        p.borrow_mut()
            .push(leak_page(build_heap_page(&[10, 20, 30])));
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
    let err = unique_insert(20, tid(0, 3)).err().unwrap();
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
        p.borrow_mut()
            .push(leak_page(build_heap_page(&[10, 20, 30])));
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
        p.borrow_mut()
            .push(leak_page(build_heap_page(&[10, 20, 30])));
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
    assert_eq!(
        drain_forward(cx.mcx(), &rel).len(),
        3,
        "recheck never inserts"
    );

    // Recheck that finds another live row under its key: 23505.
    let err = insert(20, tid(0, 3), UNIQUE_CHECK_EXISTING).err().unwrap();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_UNIQUE_VIOLATION);
    assert!(err.message().contains("duplicate key value"));

    // Recheck that cannot re-find its tuple: internal re-find failure.
    let err = insert(99, tid(0, 1), UNIQUE_CHECK_EXISTING).err().unwrap();
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
    let err = unique_insert(20, tid(2, 2)).err().unwrap();
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
    let dedups = infos
        .iter()
        .filter(|i| **i == ::types_nbtree::XLOG_BTREE_DEDUP)
        .count();
    assert!(dedups >= 1, "expected dedup passes, saw none");
    assert!(
        !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_R)
            && !infos.contains(&::types_nbtree::XLOG_BTREE_SPLIT_L),
        "1200 duplicates of one int4 key must fit a single deduplicated leaf"
    );

    let seen = drain_forward(cx.mcx(), &rel);
    assert_eq!(seen.len(), n as usize);
    for (i, t) in seen.iter().enumerate() {
        assert_eq!(
            ItemPointerGetBlockNumber(t),
            i as u32 + 1,
            "TID order preserved"
        );
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
        .any(|i| *i == ::types_nbtree::XLOG_BTREE_SPLIT_R
            || *i == ::types_nbtree::XLOG_BTREE_SPLIT_L));

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
    assert_eq!(
        keys[0].sk_flags & types_scan::scankey::SK_ISNULL,
        types_scan::scankey::SK_ISNULL
    );
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
    assert!(
        seen.len() <= 60,
        "deleted tuples stay deleted: {}",
        seen.len()
    );
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

// _bt_upgrademetapage: a v2 (pre-pg_upgrade) metapage upgrades in place to
// BTREE_NOVAC_VERSION with the v3 fields filled and pd_lower re-covering the
// full payload; the field-wise page_meta parse must tolerate the garbage
// byte where v4's btm_allequalimage lives.
#[test]
fn upgrademetapage_lifts_v2_meta_to_novac_v3() {
    install();
    PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.clear();
        let mut mp = meta_page_opts(P_NONE, 0, false);
        // Rewrite as a v2 image: version 2, short pd_lower, arbitrary byte
        // where the (v4-only) allequalimage bool would sit.
        mp.0[SizeOfPageHeaderData + 4..SizeOfPageHeaderData + 8]
            .copy_from_slice(&2u32.to_ne_bytes());
        mp.0[SizeOfPageHeaderData + 40] = 0xAA;
        let lower = (SizeOfPageHeaderData + 24) as u16;
        mp.0[12..14].copy_from_slice(&lower.to_ne_bytes());
        pages.push(leak_page(mp));
    });
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    let pin = ::bufmgr_seams::BufferPin::adopt(
        bufmgr_seams::read_buffer::call(&rel, ::types_nbtree::BTREE_METAPAGE).unwrap(),
    )
    .unwrap();

    let mut metad = crate::page::page_meta(&pin.page());
    assert_eq!(metad.btm_version, 2);

    crate::page::bt_upgrademetapage(&pin, &mut metad);

    assert_eq!(metad.btm_version, ::types_nbtree::BTREE_NOVAC_VERSION);
    let on_page = crate::page::page_meta(&pin.page());
    assert_eq!(on_page.btm_version, ::types_nbtree::BTREE_NOVAC_VERSION);
    assert_eq!(on_page.btm_last_cleanup_num_delpages, 0);
    assert_eq!(on_page.btm_last_cleanup_num_heap_tuples, -1.0);
    assert!(
        !on_page.btm_allequalimage,
        "only a REINDEX can set allequalimage"
    );
    // pd_lower re-covers the whole (48B) metadata payload, as
    // _bt_initmetapage lays it out.
    let lower = PAGES.with(|p| {
        let pages = p.borrow();
        // SAFETY: leaked page, single-threaded test access.
        let page = unsafe { &*pages[0].as_ptr() };
        u16::from_ne_bytes([page.0[12], page.0[13]]) as usize
    });
    assert_eq!(
        lower,
        SizeOfPageHeaderData + core::mem::size_of::<BTMetaPageData>()
    );
    pin.release();
    assert_eq!(PINS.with(Cell::get), 0);
}

// bug_f4d66247: bt_parallel_seize's NeedPrimscan arm sets page_status =
// Advancing BEFORE the fallible restore_arrays. Before the fix, an Err from
// restore_arrays left the scan stuck in Advancing forever with no wake, so
// every other seizer parked on shared.lot indefinitely (nothing on the
// error/unwind path calls bt_parallel_done). The fix rolls the state back to
// the terminal Done and wake_all()s before propagating. This drives the REAL
// bt_parallel_seize with restore_arrays faulted and asserts (a) the state
// ends terminal Done and (b) the lot epoch advanced — the wake signal that
// releases any parked seizer (park() loops while epoch == the value it
// captured), so a worker parked before the call cannot be lost. A real
// thread parked on that pre-call epoch is joined to prove it terminates.
#[test]
fn parallel_seize_restore_failure_rolls_back_to_done_and_wakes() {
    use std::sync::Arc;

    use ::types_core::InvalidBlockNumber;
    use ::types_relscan::parallel::{BTParallelScanShared, BtParallelScanState, BtPsState};

    use crate::parallel::{arm_restore_arrays_fault, bt_parallel_seize};

    let cx = MemoryContext::new("seize");
    let mcx = cx.mcx();

    // A minimal scan opaque: one array key (satisfies the NeedPrimscan
    // debug_assert) but no serialized elems — restore_arrays is faulted
    // before it would touch them.
    let mut so = ::types_nbtree::BTScanOpaqueData::alloc_in(mcx).unwrap();
    so.numArrayKeys = 1;

    let shared = Arc::new(BTParallelScanShared {
        state: pgsync::Mutex::new(BtParallelScanState {
            next_scan_page: InvalidBlockNumber,
            last_curr_page: InvalidBlockNumber,
            page_status: BtPsState::NeedPrimscan,
            arr_elems: Vec::new(),
        }),
        lot: pgsync::ParkLot::new(),
    });

    // A worker that captured the pre-call epoch and parks on it. When seize
    // wakes (epoch bump), it must return — no lost wakeup / no deadlock.
    let seen = shared.lot.epoch();
    let waiter = {
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || shared.lot.park(seen))
    };

    arm_restore_arrays_fault(true);
    let res = bt_parallel_seize(&mut so, &shared, /* first = */ true);
    arm_restore_arrays_fault(false);

    // The faulted restore_arrays propagated as an error...
    assert!(res.is_err(), "faulted restore_arrays must propagate Err");
    // (a) ...with the scan rolled back to the terminal Done state...
    assert_eq!(
        shared.state.lock().unwrap().page_status,
        BtPsState::Done,
        "restore failure must leave the scan Done, not stuck in Advancing",
    );
    // (b) ...and the wake delivered: the epoch advanced past what any earlier
    // seizer captured, so none can park forever.
    assert_ne!(shared.lot.epoch(), seen, "wake_all must bump the lot epoch");

    // The concurrently-parked worker was woken and terminates.
    waiter.join().unwrap();
}

/// Regression for the CRITICAL parallel-scan uninterruptible-park hang: a
/// worker parked in `bt_parallel_seize`'s Advancing arm must be able to
/// observe a leader-initiated die and unwind, even though the error/unwind
/// path never reaches `bt_parallel_release`/`bt_parallel_done` and therefore
/// never bumps the parklot epoch to wake it.
///
/// This transcribes the Advancing-arm wait loop (the same shape the loom
/// models `relscan_seize_release_wake_never_lost` transcribe, since the real
/// `bt_parallel_seize` needs Relation machinery no unit test can build) over
/// the REAL `types_relscan::BTParallelScanShared`, using the interruptible
/// `park_timeout` + a die poll — exactly what production's `park_timeout(seen,
/// SEIZE_PARK_POLL)` followed by `crate::check_for_interrupts()?` does over
/// `init_small::globals::InterruptPending`. The leader sets the die flag
/// WITHOUT touching the shared state, so nothing bumps the epoch; the seizer
/// must still wake and unwind.
///
/// Pre-fix (bare `ParkLot::park`) the parked seizer would never wake and this
/// test's `join()` would hang forever — the deadlock the fix removes. The
/// concurrency proof of the same property lives in the loom model
/// `relscan_seize_die_interrupt_wakes_waiter` (runtime/tests/loom.rs).
#[test]
fn parallel_seize_park_is_interruptible_on_die() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use ::types_relscan::{BTParallelScanShared, BtPsState};

    // Short poll in the test; production uses parallel.rs::SEIZE_PARK_POLL.
    const POLL: core::time::Duration = core::time::Duration::from_millis(5);

    let sh = Arc::new(BTParallelScanShared::new());
    // A winner backend is advancing the scan; our seizer parks on Advancing.
    sh.state.lock().unwrap().page_status = BtPsState::Advancing;

    // The per-worker die flag the seizer polls after each bounded park (the
    // InterruptPending stand-in; check_for_interrupts turns it into a FATAL).
    let die = Arc::new(AtomicBool::new(false));

    let seizer = {
        let sh = Arc::clone(&sh);
        let die = Arc::clone(&die);
        std::thread::spawn(move || -> Result<(), ()> {
            loop {
                let (st, seen) = {
                    let g = sh.state.lock().unwrap_or_else(|e| e.into_inner());
                    (g.page_status, sh.lot.epoch())
                };
                match st {
                    BtPsState::Advancing => {
                        // The interruptible park: bounded wait, then poll the
                        // die flag (== production's check_for_interrupts()?).
                        sh.lot.park_timeout(seen, POLL);
                        if die.load(Ordering::SeqCst) {
                            return Err(()); // the FATAL unwind
                        }
                    }
                    BtPsState::Done => return Ok(()),
                    other => unreachable!("model never enters {other:?}"),
                }
            }
        })
    };

    // Leader error-unwind: ask the worker to die, WITHOUT bumping the epoch
    // (the general error path never reaches bt_parallel_done). A bare park
    // would strand the seizer here forever.
    std::thread::sleep(core::time::Duration::from_millis(20));
    die.store(true, Ordering::SeqCst);

    let got = seizer.join().expect("seizer thread did not panic");
    assert_eq!(got, Err(()), "parked seizer observed the die and unwound");
    // The die path never touched shared state, so the wake came purely from
    // the interruptible park's timeout poll — not from an epoch bump.
    assert_eq!(
        sh.state.lock().unwrap().page_status,
        BtPsState::Advancing,
        "shared state untouched: the seizer woke via interruptibility, not a release/done"
    );
}

// bug idx86: _bt_saveitem copied IndexTupleSize(itup) bytes from an on-page
// tuple into so->currTuples (a fixed BLCKSZ work area) without bounding the
// cumulative nextTupleOffset. A crafted page whose tuple's t_info encodes a
// near-maximal size (up to INDEX_SIZE_MASK = 8191) drives writes past the
// 8 KiB allocation — a heap buffer overflow. The fix bounds the copy against
// the buffer capacity and raises a catchable ERRCODE_INDEX_CORRUPTED error.
// Here we drive the REAL bt_saveitem with a partially-filled work area and a
// max-size tuple header; the guard must reject it (Err) BEFORE any copy,
// while an in-bounds tuple still saves normally.
#[test]
fn bt_saveitem_rejects_oversized_tuple_instead_of_overflowing() {
    let cx = MemoryContext::new("saveitem-guard");
    let mcx = cx.mcx();

    let mut so = ::types_nbtree::BTScanOpaqueData::alloc_in(mcx).unwrap();
    so.currTuples = Some(::mcx::vec_with_capacity_in(mcx, BLCKSZ).unwrap());

    // A minimal 16-byte MAXALIGNed tuple image: t_info at bytes 6..8 encodes a
    // near-maximal size (8191) with no INDEX_ALT_TID_MASK bit, so it reads as a
    // plain non-pivot / non-posting tuple. The guard fires before the copy, so
    // the image need not actually be 8191 bytes long.
    let mut img = Img::<16>([0u8; 16]);
    img.0[6..8].copy_from_slice(&(0x1FFFu16).to_ne_bytes());
    let itup = img.0.as_ptr();

    // Only 16 bytes consumed so far: 16 + MAXALIGN(8191) = 8208 > BLCKSZ.
    so.currPos.nextTupleOffset = 16;
    let res = unsafe { crate::search::bt_saveitem(&mut so, 0, 1, itup) };
    assert!(
        res.is_err(),
        "oversized tuple must be rejected, not copied OOB"
    );
    assert_eq!(
        so.currPos.nextTupleOffset, 16,
        "rejected save must not advance the work-area cursor"
    );

    // An in-bounds tuple (size 16) at offset 0 still saves and advances.
    let mut ok_img = Img::<16>([0u8; 16]);
    ok_img.0[6..8].copy_from_slice(&(16u16).to_ne_bytes());
    so.currPos.nextTupleOffset = 0;
    let ok = unsafe { crate::search::bt_saveitem(&mut so, 0, 1, ok_img.0.as_ptr()) };
    assert!(ok.is_ok(), "an in-bounds tuple must save normally");
    assert_eq!(
        so.currPos.nextTupleOffset, 16,
        "cursor advances by MAXALIGN(itupsz)"
    );
}

// upstream d560e730e813 (18.5): Fix another empty nbtree index SSI race. A
// qualless serializable scan over an empty index takes the relation
// predicate lock and then re-runs _bt_get_endpoint: a key inserted between
// the empty answer and the lock must be returned (before the fix the scan
// ended without seeing it, and the writer never conflicted with it).
#[test]
fn serializable_qualless_scan_rechecks_empty_index_after_relation_lock() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    let saved_iso = ::xact::XactIsoLevel();
    ::xact::SetXactIsoLevel(::types_core::xact::XACT_SERIALIZABLE);
    PRED_LOCK_RELATION_CALLS.with(|c| c.set(0));
    INSERT_ON_PRED_LOCK.with(|c| c.set(Some(42)));

    let mut scan = begin_scan(cx.mcx(), &rel, &[]);
    scan.xs_snapshot = Some(Rc::new(::types_snapshot::SnapshotData::sentinel(
        cx.mcx(),
        ::types_snapshot::SnapshotType::SNAPSHOT_MVCC,
    )));
    let mut seen = Vec::new();
    while crate::btgettuple(&mut scan, ForwardScanDirection).unwrap() {
        seen.push(ItemPointerGetBlockNumber(&scan.xs_heaptid));
    }
    crate::btendscan(&mut scan).unwrap();
    ::xact::SetXactIsoLevel(saved_iso);

    assert_eq!(PRED_LOCK_RELATION_CALLS.with(Cell::get), 1);
    assert_eq!(
        seen,
        vec![10],
        "key inserted before the relation lock is seen"
    );
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

// ---------------------------------------------------------------------------
// Audit 18.6 remediation, batch b002-backend-access-nbtree-1 witnesses.
// Each test pins a C-18.6 behaviour the audit found diverging; the C site is
// cited on the assertion.

/// Tupdesc with BOTH attribute arrays populated (the way relcache tupdescs
/// arrive): `(attlen, attbyval, attstorage)` per column, 4-byte alignment.
fn audit_tupdesc<'m>(mcx: Mcx<'m>, specs: &[(i16, bool, i8)]) -> TupleDescData<'m> {
    let mut compact = PgVec::new_in(mcx);
    let mut attrs = PgVec::new_in(mcx);
    for (i, (attlen, attbyval, attstorage)) in specs.iter().enumerate() {
        compact.push(CompactAttribute {
            attcacheoff: Cell::new(-1),
            attlen: *attlen,
            attbyval: *attbyval,
            attispackable: *attlen == -1 && *attstorage != ::types_tuple::TYPSTORAGE_PLAIN,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: 4,
        });
        attrs.push(::types_tuple::FormData_pg_attribute {
            attnum: (i + 1) as i16,
            attlen: *attlen,
            attbyval: *attbyval,
            attalign: ::types_tuple::TYPALIGN_INT,
            attstorage: *attstorage,
            attcompression: 0,
            ..Default::default()
        });
    }
    TupleDescData {
        natts: specs.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

/// A 4B-header varlena datum over `payload`, allocated in `mcx`.
fn varlena_datum<'m>(mcx: Mcx<'m>, payload: &[u8]) -> Datum {
    let mut v: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, payload.len() + 4).unwrap();
    ::mcx::vec_append_bytes(&mut v, &::datum::varlena::set_varsize_4b(payload.len() + 4)).unwrap();
    ::mcx::vec_append_bytes(&mut v, payload).unwrap();
    Datum::from_usize(v.leak().as_ptr() as usize)
}

fn patch_metapage(f: impl FnOnce(&mut BTMetaPageData)) {
    PAGES.with(|p| {
        let pages = p.borrow();
        // SAFETY: metapage contents at +24 on the leaked metapage.
        unsafe {
            let m = pages[0]
                .as_ptr()
                .cast::<u8>()
                .add(SizeOfPageHeaderData)
                .cast::<BTMetaPageData>();
            let mut md = m.read();
            f(&mut md);
            m.write(md);
        }
    });
}

// index_form_tuple_context (indextuple.c:121): a varlena wider than
// TOAST_INDEX_TARGET = MaxHeapTupleSize / 16 = 510 bytes (heaptoast.h:68) is
// compressed in-line before it reaches the page. The 2040-byte target the
// port used left every 511..2040-byte value raw (index rows up to 4x wider
// than C's; multi-column rows C accepts fail the 2704-byte limit).
#[test]
fn index_form_tuple_compresses_varlenas_over_toast_index_target() {
    install();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let td = audit_tupdesc(mcx, &[(-1, false, ::types_tuple::TYPSTORAGE_EXTENDED)]);

    let big = varlena_datum(mcx, &[b'a'; 1500]);
    let tup = crate::itup::index_form_tuple(mcx, &td, &[big], &[false]).unwrap();
    let sz = unsafe { crate::itup::index_tuple_size(tup.as_ptr()) };
    assert!(
        sz < 64,
        "1500-byte compressible value must be stored compressed, got {sz} bytes"
    );

    let small = varlena_datum(mcx, &[b'a'; 400]);
    let tup = crate::itup::index_form_tuple(mcx, &td, &[small], &[false]).unwrap();
    let sz = unsafe { crate::itup::index_tuple_size(tup.as_ptr()) };
    assert!(
        sz >= 404,
        "400-byte value is under the target and stays raw, got {sz} bytes"
    );
}

// index_truncate_tuple (indextuple.c:591): CreateTupleDescTruncatedCopy
// copies pg_attribute for the kept columns, so index_form_tuple can read
// attstorage of a retained, still-raw varlena wider than TOAST_INDEX_TARGET
// (incompressible data). The port built the truncated tupdesc with an empty
// pg_attribute array and panicked (index out of bounds) on every page split
// whose pivot kept such a column.
#[test]
fn index_truncate_tuple_keeps_pg_attribute_for_wide_raw_varlena() {
    install();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let td = audit_tupdesc(
        mcx,
        &[
            (-1, false, ::types_tuple::TYPSTORAGE_EXTENDED),
            (4, true, ::types_tuple::TYPSTORAGE_PLAIN),
        ],
    );
    let payload: Vec<u8> = (0..2200u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    let wide = varlena_datum(mcx, &payload);
    let src = crate::itup::index_form_tuple(mcx, &td, &[wide, Datum::from_i32(7)], &[false, false])
        .unwrap();

    let trunc = unsafe { crate::itup::index_truncate_tuple(mcx, &td, src.as_ptr(), 1) }
        .expect("truncation keeps the wide first column");

    let td1 = audit_tupdesc(mcx, &[(-1, false, ::types_tuple::TYPSTORAGE_EXTENDED)]);
    let mut isnull = true;
    let d = unsafe { crate::itup::index_getattr(trunc.as_ptr(), 1, &td1, &mut isnull) };
    assert!(!isnull);
    let p = d.as_usize() as *const u8;
    let img = unsafe { core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p)) };
    assert_eq!(&img[4..], &payload[..], "kept column is byte-identical");
    // (both images MAXALIGN to the same size here: 2204 + 8 vs 2204 + 4 + 8)
    assert!(
        unsafe { crate::itup::index_tuple_size(trunc.as_ptr()) }
            <= unsafe { crate::itup::index_tuple_size(src.as_ptr()) },
        "indextuple.c:625 Assert(IndexTupleSize(truncated) <= IndexTupleSize(source))"
    );
}

// Every btree page mutation + XLogInsert runs between START_CRIT_SECTION and
// END_CRIT_SECTION in C: _bt_getroot (nbtpage.c:455/504), _bt_insertonpg
// (nbtinsert.c:1275/1399), _bt_split (nbtinsert.c:1932/2064), _bt_newlevel
// (nbtinsert.c:2501/2600), _bt_dedup_pass (nbtdedup.c:240/270). An ERROR
// raised inside (WAL insertion failure) must escalate to PANIC instead of
// leaving a modified, unlogged page in shared buffers. The xlog seam records
// CritSectionCount at every record it accepts.
#[test]
#[cfg_attr(miri, ignore)] // bulk-insert loop: not Miri-feasible
fn every_btree_wal_insert_runs_inside_a_critical_section() {
    install();
    build_empty_index(true);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    // root creation, leaf inserts, dedup passes (1200 duplicates), then
    // sequential keys: rightmost splits, downlink posts and the root split.
    for i in 1..=1200u32 {
        insert_key(&rel, &rel, 42, tid(i, 1));
    }
    for k in 1..=1500u32 {
        insert_key(&rel, &rel, 100 + k as i32, tid(2000 + k, 1));
    }

    let infos = wal_infos();
    let crit = wal_crit_counts();
    assert_eq!(infos.len(), crit.len());
    for kind in [
        ::types_nbtree::XLOG_BTREE_NEWROOT,
        ::types_nbtree::XLOG_BTREE_INSERT_LEAF,
        ::types_nbtree::XLOG_BTREE_DEDUP,
        ::types_nbtree::XLOG_BTREE_SPLIT_R,
        ::types_nbtree::XLOG_BTREE_INSERT_UPPER,
    ] {
        assert!(infos.contains(&kind), "scenario must emit xlinfo {kind:#x}");
    }
    assert!(
        infos
            .iter()
            .filter(|i| **i == ::types_nbtree::XLOG_BTREE_NEWROOT)
            .count()
            >= 2,
        "root creation (_bt_getroot) and root split (_bt_newlevel)"
    );
    let outside: Vec<u8> = infos
        .iter()
        .zip(crit.iter())
        .filter(|(_, c)| **c == 0)
        .map(|(i, _)| *i)
        .collect();
    assert!(
        outside.is_empty(),
        "btree WAL records inserted with CritSectionCount == 0 (xlinfo): {outside:#x?}"
    );
    assert_eq!(
        init_small::globals::CritSectionCount(),
        0,
        "critical sections balanced"
    );
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

// _bt_getmeta (nbtpage.c:155-167): both metapage sanity errors are plain
// ereport(ERROR, errcode(ERRCODE_INDEX_CORRUPTED), errmsg(...)) — no errhint.
#[test]
fn getmeta_errors_match_c_without_a_reindex_hint() {
    install();
    let cx = MemoryContext::new("t");

    build_empty_index(true);
    patch_metapage(|m| m.btm_magic = 0);
    let rel = index_rel(cx.mcx());
    let err = crate::bt_metaversion(&rel).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INDEX_CORRUPTED);
    assert_eq!(err.message(), "index \"t_idx\" is not a btree");
    assert_eq!(err.hint(), None, "nbtpage.c:157 carries no errhint");

    build_empty_index(true);
    patch_metapage(|m| m.btm_version = 1);
    let rel = index_rel(cx.mcx());
    let err = crate::bt_metaversion(&rel).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INDEX_CORRUPTED);
    assert_eq!(
        err.message(),
        "version mismatch in index \"t_idx\": file version 1, current version 4, minimal supported version 2"
    );
    assert_eq!(err.hint(), None, "nbtpage.c:164 carries no errhint");
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

// _bt_findinsertloc (nbtinsert.c:908-979) keeps a !heapkeyspace lane for
// version-2/3 indexes that reach a cluster through pg_upgrade. That lane is
// not ported: inserting must be a typed refusal, never a backend panic.
#[test]
fn insert_into_a_version3_index_is_a_typed_refusal_not_a_panic() {
    install();
    build_empty_index(false);
    patch_metapage(|m| m.btm_version = ::types_nbtree::BTREE_NOVAC_VERSION);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    prime_supportinfo(&rel);

    let err = crate::btinsert(
        cx.mcx(),
        &rel,
        &[Datum::from_i32(1)],
        &[false],
        &tid(10, 1),
        &rel,
        ::types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_NO,
        false,
    )
    .unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert!(
        err.message().contains("version 2/3"),
        "message names the on-disk format: {}",
        err.message()
    );
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked on the refusal");
}

// _bt_endpoint (nbtsearch.c:2753): a direction that is neither forward nor
// backward is elog(ERROR, "invalid scan direction: %d"), not a backward read.
#[test]
fn endpoint_rejects_no_movement_scan_direction() {
    install();
    build_single_leaf_index(&[1, 2, 3]);
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());
    let mut scan = begin_scan(cx.mcx(), &rel, &[]);
    let err = crate::btgettuple(&mut scan, ::types_scan::sdir::NoMovementScanDirection)
        .expect_err("NoMovementScanDirection is an error in _bt_endpoint");
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "invalid scan direction: 0");
    crate::btendscan(&mut scan).unwrap();
    assert_eq!(PINS.with(Cell::get), 0, "no pins leaked");
}

// _bt_check_third_page (nbtutils.c:4245): the 1/3-of-a-page ereport carries
// errtableconstraint(heap, indexname) — PG_DIAG_TABLE_NAME / CONSTRAINT_NAME
// (and SCHEMA_NAME when the namespace resolves) on the wire.
#[test]
fn oversized_tuple_error_carries_errtableconstraint_fields() {
    install();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let rel = index_rel(mcx);
    let heap = heap_relation(mcx);

    #[repr(C, align(8))]
    struct Oversized([u8; 2712]);
    let mut img = Oversized([0u8; 2712]);
    assert!(2712 > ::types_nbtree::BTMaxItemSize);
    unsafe {
        crate::itup::set_t_tid(img.0.as_mut_ptr(), tid(42, 7));
        crate::itup::set_t_info(img.0.as_mut_ptr(), 2712);
    }
    let mut leaf = new_page(BTP_LEAF, 0, P_NONE, P_NONE);
    let page = unsafe {
        ::types_storage::bufpage::PageMut::from_raw(core::ptr::NonNull::new_unchecked(
            leaf.0.as_mut_ptr(),
        ))
    };

    let err = unsafe {
        crate::bt_check_third_page(mcx, &rel, &heap, true, &page.as_ref(), img.0.as_ptr())
    }
    .unwrap_err();
    assert_eq!(
        err.sqlstate(),
        ::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED
    );
    assert_eq!(
        err.table_name(),
        Some("t"),
        "errtableconstraint: heap relation name"
    );
    assert_eq!(
        err.constraint_name(),
        Some("t_idx"),
        "errtableconstraint: index name as constraint"
    );
}

// ---------------------------------------------------------------------------
// audit-18.6 w2-023: index-corruption LOG parity (nbtpage.c / nbtree.c).
// C ereport(LOG, ERRCODE_INDEX_CORRUPTED, ...) at the abandon-and-continue
// corruption arms of page deletion and VACUUM backtracking; pgrust kept the
// control flow but emitted nothing.

fn set_cycleid(p: &mut FakePage, cycleid: u16) {
    let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    // SAFETY: in-bounds, aligned special area of an owned page.
    unsafe {
        (*p.0.as_mut_ptr().add(special).cast::<BTPageOpaqueData>()).btpo_cycleid = cycleid;
    }
}

fn vacuum_info<'a, 'mcx>(
    rel: &'a Relation<'mcx>,
    heap: &'a Relation<'mcx>,
) -> crate::vacuum::IndexVacuumInfo<'a, 'mcx> {
    crate::vacuum::IndexVacuumInfo {
        index: rel,
        heaprel: heap,
        analyze_only: false,
        report_progress: false,
        estimated_count: false,
        message_level: ::types_error::DEBUG2,
        num_heap_tuples: 0.0,
        strategy: None,
    }
}

// nbtpage.c:1859 _bt_pagedel: a half-dead INTERNAL page (pre-9.4 deletion
// residue) is LOGged with ERRCODE_INDEX_CORRUPTED and the REINDEX hint, then
// abandoned. The message and hint bytes are C's.
#[test]
fn pagedel_logs_half_dead_internal_page_like_c() {
    install();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let rel = index_rel(mcx);
    let heap = heap_relation(mcx);
    PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.clear();
        pages.push(leak_page(meta_page(1, 1)));
        pages.push(leak_page(new_page(
            ::types_nbtree::BTP_HALF_DEAD,
            1,
            P_NONE,
            P_NONE,
        )));
    });
    let info = vacuum_info(&rel, &heap);
    let mut stats = ::types_nbtree::IndexBulkDeleteResult::default();
    let mut vstate = crate::vacuum::BTVacState {
        info: &info,
        stats: &mut stats,
        dead_items: None,
        collect: None,
        cycleid: 0,
        pendingpages: Vec::new(),
        maxbufsize: 0,
    };
    let pins_before = PINS.with(Cell::get);
    let leafbuf = crate::page::bt_getbuf(&rel, 1, ::types_nbtree::BT_WRITE).unwrap();
    let _ = take_ereports();
    crate::pagedel::bt_pagedel(mcx, &rel, leafbuf, &mut vstate).unwrap();
    assert_eq!(
        PINS.with(Cell::get),
        pins_before,
        "leafbuf released on the abandon path"
    );

    let logs = take_ereports();
    assert_eq!(
        logs.len(),
        1,
        "exactly one LOG for the half-dead internal page: {logs:?}"
    );
    let e = &logs[0];
    assert_eq!(e.level(), ::types_error::LOG);
    assert_eq!(e.sqlstate(), ::types_error::ERRCODE_INDEX_CORRUPTED);
    assert_eq!(
        e.message(),
        "index \"t_idx\" contains a half-dead internal page"
    );
    assert_eq!(
        e.hint(),
        Some(
            "This can be caused by an interrupted VACUUM in version 9.3 or older, before upgrade. Please REINDEX it."
        )
    );
    let loc = e.location().expect("C-parity location");
    assert_eq!(
        (loc.filename.as_deref(), loc.lineno),
        (Some("nbtpage.c"), 1859)
    );
}

// nbtree.c:1413 btvacuumpage: a leaf carrying the current cycle ID whose
// right sibling has a LOWER block number makes VACUUM backtrack; a sibling
// that is not a live leaf is LOGged (errmsg_internal, ERRCODE_INDEX_CORRUPTED)
// and the scan of that page ends. C's Assert(false) is a cassert-only trap;
// the shipped behaviour is the LOG.
#[test]
fn vacuumpage_logs_inconsistent_backtracked_right_sibling_like_c() {
    install();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let rel = index_rel(mcx);
    let heap = heap_relation(mcx);
    const CYCLE: u16 = 7;
    PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.clear();
        pages.push(leak_page(meta_page(2, 0)));
        // block 1: an internal page where a live leaf sibling is expected
        pages.push(leak_page(new_page(0, 1, P_NONE, P_NONE)));
        // block 2: the scanblkno leaf, split during this cycle, right link -> 1;
        // a non-rightmost leaf carries its high key at P_HIKEY, then one live
        // tuple (so the page is neither empty nor a deletion candidate).
        let mut leaf = new_page(BTP_LEAF, 0, P_NONE, 1);
        add_tuple(&mut leaf, tid(90, 1), 900);
        add_tuple(&mut leaf, tid(10, 1), 100);
        set_cycleid(&mut leaf, CYCLE);
        pages.push(leak_page(leaf));
    });
    let info = vacuum_info(&rel, &heap);
    let mut stats = ::types_nbtree::IndexBulkDeleteResult::default();
    let dead: [ItemPointerData; 0] = [];
    let mut vstate = crate::vacuum::BTVacState {
        info: &info,
        stats: &mut stats,
        dead_items: Some(&dead),
        collect: None,
        cycleid: CYCLE,
        pendingpages: Vec::new(),
        maxbufsize: 0,
    };
    let mut scratch = MemoryContext::new("btvacuumpage");
    let pins_before = PINS.with(Cell::get);
    let pin = ::bufmgr_seams::BufferPin::adopt(bufmgr_seams::read_buffer::call(&rel, 2).unwrap())
        .unwrap();
    let _ = take_ereports();
    crate::vacuum::btvacuumpage(&mut vstate, &mut scratch, pin).unwrap();
    assert_eq!(PINS.with(Cell::get), pins_before, "every pin released");

    let logs = take_ereports();
    assert_eq!(
        logs.len(),
        1,
        "exactly one LOG for the inconsistent sibling: {logs:?}"
    );
    let e = &logs[0];
    assert_eq!(e.level(), ::types_error::LOG);
    assert_eq!(e.sqlstate(), ::types_error::ERRCODE_INDEX_CORRUPTED);
    assert_eq!(
        e.message(),
        "right sibling 1 of scanblkno 2 unexpectedly in an inconsistent state in index \"t_idx\""
    );
    let loc = e.location().expect("C-parity location");
    assert_eq!(
        (loc.filename.as_deref(), loc.lineno),
        (Some("nbtree.c"), 1413)
    );
}

// nbtutils.c:3641-3680 BTreeShmemSize/BTreeShmemInit: the VACUUM cycle-ID
// table is a ShmemInitStruct("BTree Vacuum State") allocation of
// offsetof(BTVacInfo, vacuums) + MaxBackends * sizeof(BTOneVacInfo) bytes
// (12 + 12 * MaxBackends), so pg_shmem_allocations lists it. The harness
// runs BTreeShmemInit at boot with MaxBackends = 8.
#[test]
fn btree_shmem_init_registers_vacuum_state_in_shmem_index() {
    install();
    // MaxBackends is a per-thread global: this thread mirrors the boot value.
    init_small::globals::SetMaxBackends(8);
    assert_eq!(crate::BTreeShmemSize().unwrap(), 12 + 12 * 8);
    let (_, found) = shmem::ShmemInitStruct("BTree Vacuum State", 12 + 12 * 8).unwrap();
    assert!(found, "ShmemIndex carries the BTree Vacuum State row");
}

// _bt_start_vacuum / _bt_vacuum_cycleid / _bt_end_vacuum over the shared
// table: a nonzero cycle ID while registered, "multiple active vacuums"
// (nbtutils.c:3577) on a second registration, zero after the end.
#[test]
fn vacuum_cycleid_registry_roundtrip_over_the_shared_table() {
    install();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let rel = index_rel(mcx);
    assert_eq!(crate::utils::bt_vacuum_cycleid(&rel).unwrap(), 0);
    let id = crate::utils::bt_start_vacuum(&rel).unwrap();
    assert!(id != 0 && id <= ::types_nbtree::MAX_BT_CYCLE_ID);
    assert_eq!(crate::utils::bt_vacuum_cycleid(&rel).unwrap(), id);
    let err = crate::utils::bt_start_vacuum(&rel).unwrap_err();
    assert_eq!(err.message(), "multiple active vacuums for index \"t_idx\"");
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    crate::utils::bt_end_vacuum(&rel).unwrap();
    assert_eq!(crate::utils::bt_vacuum_cycleid(&rel).unwrap(), 0);
    // Silent when no entry exists, as C.
    crate::utils::bt_end_vacuum(&rel).unwrap();
}
