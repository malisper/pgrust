//! Unit witnesses for the hash AM's C contracts (audit-18.6 remediation
//! batch b004): a fake buffer pool (the nbtree tests' shape: 8KB boxes,
//! Buffer = block + 1) under a real two-bucket index built by `_hash_init`,
//! so the write paths can be driven to their error arms without a server.
//! Each test names the C site it witnesses.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidTransactionId, Oid, OffsetNumber, BLCKSZ,
    HASH_AM_OID, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use ::types_hash::*;
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL};
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::TupleDescData;

use crate::insert::{_hash_pgaddmultitup, _hash_pgaddtup, _hash_vacuum_one_page};
use crate::ovfl::{_hash_addovflpage, _hash_freeovflpage};
use crate::page::{
    page_mut, page_ref, with_meta_mut, _hash_getbuf, _hash_getbuf_with_condlock_cleanup,
    _hash_getbuf_with_strategy, _hash_getinitbuf, _hash_getnewbuf, P_NEW,
};

// Fake buffer manager: pages are 8KB boxes; Buffer = block + 1.

// MAXALIGNed like real buffer pages (the PageRef contract).
#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

// A 16-byte hash index tuple image (6B t_tid + 2B t_info + uint32 hashkey +
// pad), MAXALIGNed like an on-page tuple.
#[repr(C, align(8))]
struct Img([u8; 16]);

const INDEX_OID: Oid = 5000;
const HEAP_OID: Oid = 4999;
const WAL_FAIL_MSG: &str = "simulated WAL insert failure";

// Pages are leaked and stored as raw pointers with one stable tag (see the
// nbtree tests for the Miri stacked-borrows rationale).
thread_local! {
    static PAGES: RefCell<Vec<core::ptr::NonNull<FakePage>>> = const { RefCell::new(Vec::new()) };
    // Some(n) makes RelationGetNumberOfBlocksInFork report n instead of the
    // pool size (the getnewbuf / init "relation size" arms).
    static NBLOCKS_OVERRIDE: Cell<Option<BlockNumber>> = const { Cell::new(None) };
    // The next XLogInsert fails with WAL_FAIL_MSG (an ERROR-class failure
    // inside the AM's update phase).
    static WAL_FAIL: Cell<bool> = const { Cell::new(false) };
    // (info, main data) of every record inserted.
    static WAL: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
    static NEXT_LSN: Cell<u64> = const { Cell::new(0x1000) };
    // XLogLogicalInfoActive() and IsCatalogRelation(hrel) as the test wants them.
    static LOGICAL_DECODING: Cell<bool> = const { Cell::new(false) };
    static IS_CATALOG: Cell<bool> = const { Cell::new(false) };
}

fn leak_page(p: Box<FakePage>) -> core::ptr::NonNull<FakePage> {
    core::ptr::NonNull::from(Box::leak(p))
}

fn push_page() -> Buffer {
    PAGES.with(|p| {
        let mut pages = p.borrow_mut();
        pages.push(leak_page(Box::new(FakePage([0u8; BLCKSZ]))));
        pages.len() as Buffer
    })
}

fn page_bytes(buf: Buffer) -> core::ptr::NonNull<u8> {
    PAGES.with(|p| {
        let pages = p.borrow();
        let idx = (buf - 1) as usize;
        assert!(idx < pages.len(), "fake pool has no buffer {buf}");
        pages[idx].cast::<u8>()
    })
}

fn zero_page(buf: Buffer) {
    // SAFETY: exclusive fake-pool page, BLCKSZ bytes.
    unsafe { core::ptr::write_bytes(page_bytes(buf).as_ptr(), 0, BLCKSZ) };
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        bufmgr_seams::read_buffer::set(|rel, blkno| {
            assert_eq!(rel.rd_id, INDEX_OID, "only the index is read through the fake pool");
            let _ = page_bytes(blkno as Buffer + 1);
            Ok(blkno as Buffer + 1)
        });
        bufmgr_seams::read_buffer_extended::set(|rel, _fork, blkno, mode, _strategy| {
            assert_eq!(rel.rd_id, INDEX_OID);
            let buf = blkno as Buffer + 1;
            let _ = page_bytes(buf);
            if matches!(mode, ReadBufferMode::ZeroAndLock | ReadBufferMode::ZeroAndCleanupLock) {
                zero_page(buf);
            }
            Ok(buf)
        });
        bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, flags, n| {
            assert_eq!(rel.rd_id, INDEX_OID);
            assert_eq!(n, 1);
            assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
            Ok((push_page(), 1))
        });
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| {
            Ok(NBLOCKS_OVERRIDE
                .with(Cell::get)
                .unwrap_or_else(|| PAGES.with(|p| p.borrow().len() as BlockNumber)))
        });
        bufmgr_seams::release_buffer::set(|_buf| Ok(()));
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::lock_buffer_for_cleanup::set(|_buf| Ok(()));
        bufmgr_seams::conditional_lock_buffer_for_cleanup::set(|_buf| Ok(true));
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        bufmgr_seams::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
        bufmgr_seams::buffer_get_page::set(page_bytes);
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        transam_xlog_seams::xlog_logical_info_active::set(|| LOGICAL_DECODING.with(Cell::get));
        xloginsert_seams::xlog_insert_record::set(|_rmid, info, _flags, main, _bufs| {
            if WAL_FAIL.with(Cell::get) {
                return Err(Box::new(PgError::error(WAL_FAIL_MSG)));
            }
            let data: Vec<u8> = main.iter().flat_map(|f| f.iter().copied()).collect();
            WAL.with(|w| w.borrow_mut().push((info, data)));
            let lsn = NEXT_LSN.get() + 8;
            NEXT_LSN.set(lsn);
            Ok(lsn)
        });
        predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
        genam_seams::index_compute_xid_horizon_for_tuples::set(
            |_mcx, _irel, _hrel, _ibuf, itemnos| {
                assert!(!itemnos.is_empty());
                Ok(InvalidTransactionId)
            },
        );
        catalog_seams::is_catalog_relation::set(|_rel| IS_CATALOG.with(Cell::get));
    });
}

// Relation stand-ins (int4 single-key-column hash index over heap `t`).

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

// Stand-in HASHSTANDARD_PROC (hashint4): only its fn_oid is read here.
fn stub_hashint4(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(Datum::from_i32(fcinfo.arg(0).as_i32()))
}

fn pg_class(relname: &str, relam: Oid, relkind: u8, oid: Oid, relhasindex: bool) -> FormData_pg_class {
    let mut name = ::types_tuple::NameData::default();
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
        relhasindex,
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

fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
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
        rd_locator: Cell::new(::types_storage::RelFileLocator::new(1663, 5, INDEX_OID)),
        rd_smgr: Default::default(),
        rd_id: INDEX_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: INDEX_OID, dbId: 5 } },
        rd_rel: pg_class("t_hidx", HASH_AM_OID, RELKIND_INDEX, INDEX_OID, false),
        rd_att: Rc::new(int4_tupdesc(mcx)),
        rd_index: Some(FormData_pg_index {
            indexrelid: INDEX_OID,
            indrelid: HEAP_OID,
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
        rd_opcintype: one(23),
        rd_opfamily: one(1977),
        rd_indoption: indoption,
        rd_indcollation: one(0),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: Cell::new((0, core::ptr::null_mut())),
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
    let rel = Relation::open(data, Some(noop_close));
    // index_getprocid(HASHSTANDARD_PROC) reads the primed support info.
    rel.rd_supportinfo
        .borrow_mut()
        .push(Some(FmgrInfo::new(stub_hashint4, 425, 1, true, false)));
    rel
}

fn heap_rel(mcx: Mcx<'_>) -> Relation<'_> {
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
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: HEAP_OID, dbId: 5 } },
        // 2 = HEAP_TABLE_AM_OID.
        rd_rel: pg_class("t", 2, RELKIND_RELATION, HEAP_OID, true),
        rd_att: Rc::new(int4_tupdesc(mcx)),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: Cell::new((0, core::ptr::null_mut())),
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

// Fresh two-bucket index in the fake pool: metapage (block 0, buffer 1),
// buckets 0/1 (blocks 1/2, buffers 2/3), bitmap page (block 3, buffer 4).
const METABUF: Buffer = 1;
const BUCKET0_BLKNO: BlockNumber = 1;
const BUCKET0_BUF: Buffer = 2;

fn reset_pool() {
    PAGES.with(|p| p.borrow_mut().clear());
    NBLOCKS_OVERRIDE.with(|c| c.set(None));
    WAL_FAIL.with(|c| c.set(false));
    WAL.with(|w| w.borrow_mut().clear());
    LOGICAL_DECODING.with(|c| c.set(false));
    IS_CATALOG.with(|c| c.set(false));
    init_small::globals::SetCritSectionCount(0);
}

fn build_index(rel: &Relation<'_>) {
    reset_pool();
    let num_buckets = crate::_hash_init(rel, 0.0, ForkNumber::MAIN_FORKNUM)
        .unwrap_or_else(|e| panic!("_hash_init on the fake pool: {e:?}"));
    assert_eq!(num_buckets, 2);
    assert_eq!(PAGES.with(|p| p.borrow().len()), 4);
    WAL.with(|w| w.borrow_mut().clear());
}

fn hash_itup(hashkey: u32, tid: ItemPointerData) -> Img {
    let mut b = [0u8; 16];
    // SAFETY: ItemPointerData is a 6-byte POD written at the image start.
    unsafe { b.as_mut_ptr().cast::<ItemPointerData>().write_unaligned(tid) };
    b[6..8].copy_from_slice(&16u16.to_ne_bytes()); // t_info: size 16, no flags
    b[8..12].copy_from_slice(&hashkey.to_ne_bytes());
    Img(b)
}

// Append `n` tuples (hashkey `key`) to a bucket page; returns their offsets.
fn add_tuples(buf: Buffer, key: u32, n: u16) -> Vec<OffsetNumber> {
    (1..=n)
        .map(|i| {
            let img = hash_itup(key, ItemPointerData::new(10, i));
            // SAFETY: fake-pool page, exclusively used by this test.
            let mut page = unsafe { page_mut(buf) };
            page.add_item(&img.0, i, 0).unwrap_or_else(|| panic!("add_item {i}"))
        })
        .collect()
}

fn mark_dead(buf: Buffer, offsets: &[OffsetNumber]) {
    // SAFETY: as above.
    let mut page = unsafe { page_mut(buf) };
    for &off in offsets {
        let mut id = page.as_ref().item_id(off);
        id.mark_dead();
        page.set_item_id(off, id);
    }
}

fn fill_page(buf: Buffer) {
    // SAFETY: as above.
    let mut page = unsafe { page_mut(buf) };
    let lower = page.as_ref().pd_lower();
    page.set_pd_upper(lower);
}

// A bucket page carrying `n` 8-byte index-tuple images (t_tid (10, i), t_info
// size 8, no key bytes) written straight into the line-pointer and tuple
// areas. The hash AM never writes such a page (its tuples are 16 bytes, so
// at most ~407 fit), so n > MaxIndexTuplesPerPage (408) is the crafted /
// corrupt page of the audit rows; C sizes deletable[] by MaxOffsetNumber
// (2048) so it reaps it whole. The 8 bytes below the special area stay
// unused so the hashkey read at itup+8 of the topmost item stays on-page.
// Returns the TIDs in offset order (sorted, as a dead-items array).
fn craft_dense_page(buf: Buffer, n: u16) -> Vec<ItemPointerData> {
    // SAFETY: fake-pool page, exclusively used by this test.
    let mut page = unsafe { page_mut(buf) };
    let special = page.as_ref().pd_special() as usize;
    let mut tids = Vec::with_capacity(n as usize);
    for i in 1..=n {
        let off = special - 8 * (i as usize + 1);
        let tid = ItemPointerData::new(10, i);
        let mut img = [0u8; 8];
        // SAFETY: ItemPointerData is a 6-byte POD written at the image start.
        unsafe { img.as_mut_ptr().cast::<ItemPointerData>().write_unaligned(tid) };
        img[6..8].copy_from_slice(&8u16.to_ne_bytes()); // t_info: size 8, no flags
        // SAFETY: off + 8 <= pd_special, inside the BLCKSZ page.
        unsafe { core::ptr::copy_nonoverlapping(img.as_ptr(), page.as_mut_ptr().add(off), 8) };
        page.set_item_id(i, ItemIdData::new(off as u16, LP_NORMAL, 8));
        tids.push(tid);
    }
    page.set_pd_lower((SizeOfPageHeaderData + n as usize * 4) as u16);
    page.set_pd_upper((special - 8 * (n as usize + 1)) as u16);
    tids
}

// The escape check after an Err surfaced from an update phase: C promotes
// any ereport(ERROR) raised between START_CRIT_SECTION and END_CRIT_SECTION
// to PANIC (elog.c errstart, CritSectionCount > 0); pgrust does the same for
// an escaping Err at the recovery frame (elog::panic_on_crit_section_escape)
// provided the count is still raised when the Err leaves the AM.
fn take_crit_section_count() -> u32 {
    let count = init_small::globals::CritSectionCount();
    init_small::globals::SetCritSectionCount(0);
    WAL_FAIL.with(|c| c.set(false));
    count
}

fn assert_internal_error(err: &PgError, message: &str) {
    assert_eq!(err.level(), ERROR, "elog(ERROR) level: {err:?}");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR, "elog(ERROR) sqlstate: {err:?}");
    assert_eq!(err.message(), message);
    assert_eq!(err.hint(), None, "elog(ERROR) carries no HINT: {err:?}");
}

// hashinsert.c:199-237 _hash_doinsert: the page add, metapage update and
// XLogInsert sit between START_CRIT_SECTION and END_CRIT_SECTION.
#[test]
fn doinsert_wal_failure_escapes_inside_the_critical_section() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    let heap = heap_rel(cx.mcx());
    build_index(&idx);

    let img = hash_itup(0, ItemPointerData::new(10, 1));
    WAL_FAIL.with(|c| c.set(true));
    let err = crate::_hash_doinsert(&idx, &img.0, &heap, false)
        .expect_err("the simulated XLogInsert failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), WAL_FAIL_MSG);
    assert!(
        count > 0,
        "hashinsert.c:199 START_CRIT_SECTION: the Err escaped _hash_doinsert with \
         CritSectionCount = {count}, so it would be recovered as a plain ERROR \
         (torn dirty page + metapage left in shared buffers) instead of PANIC"
    );
}

// hashinsert.c:406-457 _hash_vacuum_one_page: PageIndexMultiDelete, the
// hint-flag clear, the metapage decrement and XLogInsert are one critical
// section.
#[test]
fn vacuum_one_page_wal_failure_escapes_inside_the_critical_section() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    let heap = heap_rel(cx.mcx());
    build_index(&idx);
    let offs = add_tuples(BUCKET0_BUF, 0, 2);
    mark_dead(BUCKET0_BUF, &offs);

    WAL_FAIL.with(|c| c.set(true));
    let err = _hash_vacuum_one_page(&idx, &heap, METABUF, BUCKET0_BUF)
        .expect_err("the simulated XLogInsert failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), WAL_FAIL_MSG);
    assert!(
        count > 0,
        "hashinsert.c:406 START_CRIT_SECTION: the Err escaped _hash_vacuum_one_page \
         with CritSectionCount = {count}"
    );
}

// hashinsert.c:433 xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(hrel):
// with wal_level = logical and a catalog heap, XLOG_HASH_VACUUM_ONE_PAGE
// carries isCatalogRel = true (the standby's recovery-conflict input).
#[test]
fn vacuum_one_page_records_catalog_relations_for_logical_decoding() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    let heap = heap_rel(cx.mcx());
    build_index(&idx);
    let offs = add_tuples(BUCKET0_BUF, 0, 1);
    mark_dead(BUCKET0_BUF, &offs);

    LOGICAL_DECODING.with(|c| c.set(true));
    IS_CATALOG.with(|c| c.set(true));
    _hash_vacuum_one_page(&idx, &heap, METABUF, BUCKET0_BUF).unwrap();
    init_small::globals::SetCritSectionCount(0);
    LOGICAL_DECODING.with(|c| c.set(false));
    IS_CATALOG.with(|c| c.set(false));

    let records = WAL.with(|w| w.borrow().clone());
    let (_, data) = records
        .iter()
        .find(|(info, _)| *info == XLOG_HASH_VACUUM_ONE_PAGE)
        .expect("XLOG_HASH_VACUUM_ONE_PAGE emitted");
    // xl_hash_vacuum_one_page: snapshotConflictHorizon u32 @0, ntuples u16 @4,
    // isCatalogRel bool @6.
    assert_eq!(
        data[6], 1,
        "hashinsert.c:433: isCatalogRel must be RelationIsAccessibleInLogicalDecoding(hrel)"
    );
}

// hashinsert.c:372 OffsetNumber deletable[MaxOffsetNumber]: a page carrying
// more LP_DEAD line pointers than MaxIndexTuplesPerPage (408) is vacuumed
// whole (C's array holds any offset a page can carry; PageIndexMultiDelete
// then empties it, bufpage.c:1160 indexing its scratch by the kept count).
#[test]
fn vacuum_one_page_reaps_more_dead_items_than_max_index_tuples_per_page() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    let heap = heap_rel(cx.mcx());
    build_index(&idx);
    const N: u16 = 500;
    assert!(N as usize > MaxIndexTuplesPerPage);
    craft_dense_page(BUCKET0_BUF, N);
    let offs: Vec<OffsetNumber> = (1..=N).collect();
    mark_dead(BUCKET0_BUF, &offs);

    _hash_vacuum_one_page(&idx, &heap, METABUF, BUCKET0_BUF).unwrap_or_else(|e| {
        panic!("hashinsert.c:372: a {N}-item dead page must be vacuumed whole: {e:?}")
    });
    assert_eq!(take_crit_section_count(), 0, "balanced critical section");

    // SAFETY: fake-pool page.
    let page = unsafe { page_ref(BUCKET0_BUF) };
    assert_eq!(page.max_offset_number(), 0, "every dead item removed");
    assert_eq!(page.pd_upper(), page.pd_special());
    let records = WAL.with(|w| w.borrow().clone());
    let (_, data) = records
        .iter()
        .find(|(info, _)| *info == XLOG_HASH_VACUUM_ONE_PAGE)
        .expect("XLOG_HASH_VACUUM_ONE_PAGE emitted");
    // xl_hash_vacuum_one_page: ntuples u16 @4.
    assert_eq!(u16::from_ne_bytes([data[4], data[5]]), N, "ntuples in the WAL record");
}

// hash.c:717 OffsetNumber deletable[MaxOffsetNumber]: hashbucketcleanup over
// a page whose every line pointer the callback reaps collects all of them
// (> MaxIndexTuplesPerPage) and PageIndexMultiDelete empties the page. The
// fake pool cannot answer IsBufferCleanupOK for the closing squeeze, so the
// walk ends at the simulated XLogInsert failure — after the page update.
#[test]
fn bucketcleanup_reaps_more_dead_items_than_max_index_tuples_per_page() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    build_index(&idx);
    const N: u16 = 500;
    assert!(N as usize > MaxIndexTuplesPerPage);
    let tids = craft_dense_page(BUCKET0_BUF, N);

    let mut removed = 0.0f64;
    let mut kept = 0.0f64;
    let mut callback = crate::HashVacDelete::DeadItems(&tids);
    WAL_FAIL.with(|c| c.set(true));
    let err = crate::hashbucketcleanup(
        &idx,
        0,
        BUCKET0_BUF,
        BUCKET0_BLKNO,
        None,
        1,
        3,
        1,
        Some(&mut removed),
        Some(&mut kept),
        false,
        Some(&mut callback),
    )
    .expect_err("the simulated XLogInsert failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), WAL_FAIL_MSG, "hash.c:717: the {N}-item walk must reach XLogInsert");
    assert!(count > 0, "hash.c:800 START_CRIT_SECTION still open at the escape");
    assert_eq!(removed, N as f64, "tuples_removed counts every reaped item");
    assert_eq!(kept, 0.0, "num_index_tuples counts none");
    // SAFETY: fake-pool page.
    let page = unsafe { page_ref(BUCKET0_BUF) };
    assert_eq!(page.max_offset_number(), 0, "PageIndexMultiDelete removed every item");
    assert_eq!(page.pd_upper(), page.pd_special());
}

// hash.c:800-851 hashbucketcleanup: PageIndexMultiDelete + XLOG_HASH_DELETE
// are one critical section (the split-cleanup arm deletes the moved tuples).
#[test]
fn bucketcleanup_wal_failure_escapes_inside_the_critical_section() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    build_index(&idx);
    // Three tuples of hashkey 1 sit in bucket 0 as if left behind by the split
    // into bucket 1 (maxbucket 1, highmask 3, lowmask 1): split cleanup of
    // bucket 0 removes them.
    add_tuples(BUCKET0_BUF, 1, 3);

    WAL_FAIL.with(|c| c.set(true));
    let err = crate::hashbucketcleanup(
        &idx, 0, BUCKET0_BUF, BUCKET0_BLKNO, None, 1, 3, 1, None, None, true, None,
    )
    .expect_err("the simulated XLogInsert failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), WAL_FAIL_MSG);
    assert!(
        count > 0,
        "hash.c:800 START_CRIT_SECTION: the Err escaped hashbucketcleanup with \
         CritSectionCount = {count}"
    );
}

// hashovfl.c:321-424 _hash_addovflpage and :581-751 _hash_freeovflpage: the
// bitmap/metapage/chain updates and their XLogInsert are critical sections.
#[test]
fn ovflpage_wal_failures_escape_inside_the_critical_section() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());

    build_index(&idx);
    WAL_FAIL.with(|c| c.set(true));
    let err = _hash_addovflpage(&idx, METABUF, BUCKET0_BUF, true)
        .expect_err("the simulated XLogInsert failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), WAL_FAIL_MSG);
    assert!(
        count > 0,
        "hashovfl.c:321 START_CRIT_SECTION: the Err escaped _hash_addovflpage with \
         CritSectionCount = {count}"
    );

    // A clean index with one real overflow page on bucket 0, then free it.
    build_index(&idx);
    let ovflbuf = _hash_addovflpage(&idx, METABUF, BUCKET0_BUF, true).unwrap();
    assert_eq!(init_small::globals::CritSectionCount(), 0, "balanced on the Ok path");
    WAL_FAIL.with(|c| c.set(true));
    let err = _hash_freeovflpage(&idx, BUCKET0_BUF, ovflbuf, BUCKET0_BUF, &[], &[], &mut [], None)
        .expect_err("the simulated XLogInsert failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), WAL_FAIL_MSG);
    assert!(
        count > 0,
        "hashovfl.c:581 START_CRIT_SECTION: the Err escaped _hash_freeovflpage with \
         CritSectionCount = {count}"
    );
}

// hashovfl.c:562 elog(ERROR, "invalid overflow bit number %u", ovflbitno):
// XX000, the bare message, no HINT.
#[test]
fn freeovflpage_invalid_bit_number_is_c_shaped() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    build_index(&idx);
    let ovflbuf = _hash_addovflpage(&idx, METABUF, BUCKET0_BUF, true).unwrap();
    // Bit 1 (the first overflow page) now lives on bitmap page 0; a metapage
    // claiming zero bitmap pages makes that bit unaddressable.
    with_meta_mut(METABUF, |m| m.hashm_nmaps = 0);

    let err = _hash_freeovflpage(&idx, BUCKET0_BUF, ovflbuf, BUCKET0_BUF, &[], &[], &mut [], None)
        .expect_err("hashovfl.c:562 must reject the unaddressable bit");
    init_small::globals::SetCritSectionCount(0);
    assert_internal_error(&err, "invalid overflow bit number 1");
}

// hashinsert.c:315 / :357: PageAddItem failure is elog(ERROR, "failed to add
// index item to \"%s\"") from _hash_pgaddtup and _hash_pgaddmultitup.
#[test]
fn pgaddtup_on_a_full_page_is_a_catchable_error() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    build_index(&idx);
    fill_page(BUCKET0_BUF);

    let img = hash_itup(0, ItemPointerData::new(10, 1));
    let err = _hash_pgaddtup(&idx, BUCKET0_BUF, &img.0, false)
        .expect_err("hashinsert.c:315: a failed PageAddItem is an ERROR");
    assert_internal_error(&err, "failed to add index item to \"t_hidx\"");

    let mut offsets = [0 as OffsetNumber; 1];
    let err = _hash_pgaddmultitup(&idx, BUCKET0_BUF, &img.0, &[(0, 16)], &mut offsets)
        .expect_err("hashinsert.c:357: a failed PageAddItem is an ERROR");
    assert_internal_error(&err, "failed to add index item to \"t_hidx\"");
}

// hashpage.c:75/101/140/204/246: every _hash_getbuf* refuses P_NEW with
// elog(ERROR, "hash AM does not use P_NEW").
#[test]
fn p_new_is_a_catchable_error_at_every_getbuf_site() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    build_index(&idx);
    const MSG: &str = "hash AM does not use P_NEW";

    let err = _hash_getbuf(&idx, P_NEW, HASH_READ, LH_BUCKET_PAGE).expect_err("hashpage.c:75");
    assert_internal_error(&err, MSG);
    let err = _hash_getbuf_with_condlock_cleanup(&idx, P_NEW, LH_BUCKET_PAGE)
        .expect_err("hashpage.c:101");
    assert_internal_error(&err, MSG);
    let err = _hash_getinitbuf(&idx, P_NEW).expect_err("hashpage.c:140");
    assert_internal_error(&err, MSG);
    let err = _hash_getnewbuf(&idx, P_NEW, ForkNumber::MAIN_FORKNUM).expect_err("hashpage.c:204");
    assert_internal_error(&err, MSG);
    let err = _hash_getbuf_with_strategy(&idx, P_NEW, HASH_READ, LH_BUCKET_PAGE, None)
        .expect_err("hashpage.c:246");
    assert_internal_error(&err, MSG);
}

// hashpage.c:206 "access to noncontiguous page in hash index \"%s\"" and
// :213 "unexpected hash relation size: %u, should be %u" are elog(ERROR).
#[test]
fn getnewbuf_beyond_eof_is_a_catchable_error() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    build_index(&idx);

    NBLOCKS_OVERRIDE.with(|c| c.set(Some(2)));
    let err = _hash_getnewbuf(&idx, 5, ForkNumber::MAIN_FORKNUM).expect_err("hashpage.c:206");
    assert_internal_error(&err, "access to noncontiguous page in hash index \"t_hidx\"");

    // blkno == nblocks, but the extension lands on block 4 of the 4-block pool.
    let err = _hash_getnewbuf(&idx, 2, ForkNumber::MAIN_FORKNUM).expect_err("hashpage.c:213");
    assert_internal_error(&err, "unexpected hash relation size: 4, should be 2");
    NBLOCKS_OVERRIDE.with(|c| c.set(None));
}

// hashpage.c:344 elog(ERROR, "cannot initialize non-empty hash index \"%s\"").
#[test]
fn init_on_a_nonempty_fork_is_a_catchable_error() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    reset_pool();
    NBLOCKS_OVERRIDE.with(|c| c.set(Some(1)));
    let err = crate::_hash_init(&idx, 0.0, ForkNumber::MAIN_FORKNUM).expect_err("hashpage.c:344");
    NBLOCKS_OVERRIDE.with(|c| c.set(None));
    assert_internal_error(&err, "cannot initialize non-empty hash index \"t_hidx\"");
}

// Control: the same write paths, with WAL succeeding, leave the count balanced.
#[test]
fn successful_update_phases_leave_no_critical_section_open() {
    install();
    let cx = MemoryContext::new("t");
    let idx = index_rel(cx.mcx());
    let heap = heap_rel(cx.mcx());
    build_index(&idx);

    let img = hash_itup(0, ItemPointerData::new(10, 1));
    crate::_hash_doinsert(&idx, &img.0, &heap, false).unwrap();
    let offs = add_tuples(BUCKET0_BUF, 0, 2);
    mark_dead(BUCKET0_BUF, &offs);
    _hash_vacuum_one_page(&idx, &heap, METABUF, BUCKET0_BUF).unwrap();
    let ovflbuf = _hash_addovflpage(&idx, METABUF, BUCKET0_BUF, true).unwrap();
    _hash_freeovflpage(&idx, BUCKET0_BUF, ovflbuf, BUCKET0_BUF, &[], &[], &mut [], None).unwrap();
    assert_eq!(init_small::globals::CritSectionCount(), 0);
    // SAFETY: fake-pool page.
    let opaque = crate::page::page_opaque(&unsafe { page_ref(ovflbuf) });
    assert_eq!(opaque.hasho_flag & LH_PAGE_TYPE, LH_UNUSED_PAGE);
}
