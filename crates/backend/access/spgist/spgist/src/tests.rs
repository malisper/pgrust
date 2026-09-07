//! Unit witnesses for the SP-GiST AM's C contracts (audit-18.6 remediation
//! wave 2, batch w2-001): a fake buffer pool (the hash/nbtree tests' shape:
//! 8KB boxes, Buffer = block + 1) so the page-update paths can be driven to
//! their error arms without a server. Each test names the C site it
//! witnesses. Fixture state lives in plain statics serialized by `lock()`
//! (no `thread_local!`: the session census pin).

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, Once};

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    Buffer, InvalidBlockNumber, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
    SPGIST_AM_OID,
};
use ::types_error::{PgError, ERRCODE_INTERNAL_ERROR, ERROR, PANIC};
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use ::types_spgist::state::SpGistState;
use ::types_spgist::*;
use ::types_storage::bufpage::{ItemIdData, PageMut, LP_NORMAL};
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::TupleDescData;

use crate::doinsert::{addLeafTuple, addNode, SPPageDesc};
use crate::utils::{
    buf_page_mut, InvalidBuffer, InvalidOffsetNumber, ItupExt, SpGistPageAddNewItem,
};

// Fake buffer manager: pages are 8KB boxes; Buffer = block + 1.

// MAXALIGNed like real buffer pages (the PageRef contract).
#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

const INDEX_OID: Oid = 5100;
const HEAP_OID: Oid = 5099;
pub(crate) const WAL_FAIL_MSG: &str = "simulated WAL insert failure";

// Pages are leaked and stored as addresses (one stable tag, never freed).
static PAGES: Mutex<Vec<usize>> = Mutex::new(Vec::new());
// The next XLogInsert fails with WAL_FAIL_MSG (an ERROR-class failure
// inside the AM's update phase).
static WAL_FAIL: AtomicBool = AtomicBool::new(false);
// The fixture flags and CritSectionCount are shared: one test at a time.
static TEST_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn lock() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn pages() -> MutexGuard<'static, Vec<usize>> {
    PAGES.lock().unwrap_or_else(|e| e.into_inner())
}

fn leak_page() -> usize {
    let p: &'static mut FakePage = Box::leak(Box::new(FakePage([0u8; BLCKSZ])));
    p as *mut FakePage as usize
}

pub(crate) fn push_page() -> Buffer {
    let addr = leak_page();
    let mut pages = pages();
    pages.push(addr);
    pages.len() as Buffer
}

fn page_bytes(buf: Buffer) -> core::ptr::NonNull<u8> {
    let pages = pages();
    let idx = (buf - 1) as usize;
    assert!(idx < pages.len(), "fake pool has no buffer {buf}");
    // SAFETY: leaked FakePage, never freed, non-null.
    unsafe { core::ptr::NonNull::new_unchecked(pages[idx] as *mut u8) }
}

pub(crate) fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        bufmgr_seams::buffer_get_page::set(page_bytes);
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        xloginsert_seams::xlog_insert_record::set(|_rmid, _info, _flags, _main, _bufs| {
            if WAL_FAIL.load(Relaxed) {
                return Err(Box::new(PgError::error(WAL_FAIL_MSG)));
            }
            Ok(0x1000)
        });
    });
}

pub(crate) fn set_wal_fail(fail: bool) {
    WAL_FAIL.store(fail, Relaxed);
}

// The escape check after an Err surfaced from an update phase: C promotes
// any ereport(ERROR) raised between START_CRIT_SECTION and END_CRIT_SECTION
// to PANIC (elog.c errstart, CritSectionCount > 0); pgrust does the same for
// an escaping Err at the recovery frame (elog::panic_on_crit_section_escape)
// provided the count is still raised when the Err leaves the AM.
pub(crate) fn take_crit_section_count() -> u32 {
    let count = init_small::globals::CritSectionCount();
    init_small::globals::SetCritSectionCount(0);
    WAL_FAIL.store(false, Relaxed);
    count
}

// Relation stand-ins (single-key-column SP-GiST index over heap `t`).

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

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> ::types_error::PgResult<()> {
    Ok(())
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

pub(crate) fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
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
        rd_rel: pg_class("t_spgidx", SPGIST_AM_OID, RELKIND_INDEX, INDEX_OID, false),
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
            indkey,
            has_indpred: false,
            indexprs_src: None,
            indpred_src: None,
        }),
        rd_opcintype: one(23),
        rd_opfamily: one(4015),
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
    Relation::open(data, Some(noop_close))
}

pub(crate) fn heap_rel(mcx: Mcx<'_>) -> Relation<'_> {
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

// SpGistState stand-in: the node_label_tests fixture (text prefix/leaf, int2
// labels, no-op support procs) — one field list to maintain.
pub(crate) fn spg_state(mcx: Mcx<'_>) -> SpGistState<'_> {
    crate::utils::node_label_tests::state(mcx)
}

// A 16-byte LIVE leaf tuple image (12-byte header, MAXALIGN padding).
pub(crate) fn leaf_tuple(tid: ItemPointerData) -> Vec<u8> {
    let mut v = vec![0u8; 16];
    SpGistLeafTupleHeader { tupstate: SPGIST_LIVE, size: 16, t_info: 0, heapPtr: tid }
        .encode(&mut v);
    v
}

// An inner tuple image with two label-less (8-byte) node tuples.
fn two_node_inner() -> Vec<u8> {
    let mut v = vec![0u8; SGITHDRSZ + 16];
    SpGistInnerTupleHeader {
        tupstate: SPGIST_LIVE,
        allTheSame: false,
        nNodes: 2,
        prefixSize: 0,
        size: (SGITHDRSZ + 16) as u16,
    }
    .encode(&mut v);
    v[SGITHDRSZ + 6..SGITHDRSZ + 8].copy_from_slice(&8u16.to_ne_bytes());
    v[SGITHDRSZ + 14..SGITHDRSZ + 16].copy_from_slice(&8u16.to_ne_bytes());
    v
}

fn init_leaf_buffer(buffer: Buffer) {
    let mut pm = buf_page_mut(buffer);
    SpGistInitPage(&mut pm, SPGIST_LEAF);
}

// spgdoinsert.c:217-319 addLeafTuple: the page add, the parent downlink
// update and XLogInsert sit between START_CRIT_SECTION and END_CRIT_SECTION
// (row a186-candidate-fp-spgist-spgdoinsert-6b3767d0b9948dec42e6-1).
#[test]
fn add_leaf_tuple_wal_failure_escapes_inside_the_critical_section() {
    let _serial = lock();
    install();
    let buffer = push_page();
    init_leaf_buffer(buffer);
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let index = index_rel(mcx);
    let mut state = spg_state(mcx);
    let mut leaf = leaf_tuple(ItemPointerData::new(10, 1));
    // A non-root leaf page holding no chain yet: the "not part of a chain"
    // arm, no parent downlink.
    let mut current = SPPageDesc { blkno: 4, buffer, offnum: InvalidOffsetNumber, node: 0 };
    let parent = SPPageDesc {
        blkno: InvalidBlockNumber,
        buffer: InvalidBuffer,
        offnum: InvalidOffsetNumber,
        node: 0,
    };

    init_small::globals::SetCritSectionCount(0);
    set_wal_fail(true);
    let err = addLeafTuple(&index, &mut state, &mut leaf, &mut current, &parent, false, false)
        .expect_err("the simulated XLogInsert failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), WAL_FAIL_MSG);
    assert!(
        count > 0,
        "spgdoinsert.c:217 START_CRIT_SECTION: the Err escaped addLeafTuple with \
         CritSectionCount = {count}, so it would be recovered as a plain ERROR \
         (dirty, unlogged leaf page left in shared buffers) instead of PANIC"
    );
}

// The Ok path leaves the section balanced (spgdoinsert.c:319 END_CRIT_SECTION)
// and the leaf tuple on the page.
#[test]
fn add_leaf_tuple_balances_the_critical_section() {
    let _serial = lock();
    install();
    let buffer = push_page();
    init_leaf_buffer(buffer);
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let index = index_rel(mcx);
    let mut state = spg_state(mcx);
    let mut leaf = leaf_tuple(ItemPointerData::new(10, 1));
    let mut current = SPPageDesc { blkno: 4, buffer, offnum: InvalidOffsetNumber, node: 0 };
    let parent = SPPageDesc {
        blkno: InvalidBlockNumber,
        buffer: InvalidBuffer,
        offnum: InvalidOffsetNumber,
        node: 0,
    };

    init_small::globals::SetCritSectionCount(0);
    set_wal_fail(false);
    addLeafTuple(&index, &mut state, &mut leaf, &mut current, &parent, false, false)
        .unwrap_or_else(|e| panic!("addLeafTuple on the fake pool: {e:?}"));
    assert_eq!(take_crit_section_count(), 0, "END_CRIT_SECTION after the WAL record");
    assert_eq!(current.offnum, 1);
    assert_eq!(buf_page_mut(buffer).as_ref().max_offset_number(), 1);
}

// spgutils.c:1273 SpGistPageAddNewItem: when the replacement of a
// placeholder fails after PageIndexTupleDelete already removed it, C raises
// elog(PANIC) — the page is already torn and the damage must not reach
// disk (row a186-candidate-fp-spgist-spgutils-abb74a413333aaf6b869-1). The
// port must surface that as a PANIC-level error, not unwind as a panic the
// statement-level handler demotes to a recoverable ERROR.
#[test]
fn page_add_new_item_replacement_failure_is_panic_level() {
    let addr = leak_page();
    // SAFETY: leaked, MAXALIGNed, BLCKSZ-byte page owned by this test.
    let mut pm = unsafe { PageMut::from_raw(core::ptr::NonNull::new_unchecked(addr as *mut u8)) };
    SpGistInitPage(&mut pm, SPGIST_LEAF);
    let placeholder =
        spgFormDeadTuple(0, SPGIST_PLACEHOLDER, InvalidBlockNumber, InvalidOffsetNumber);
    assert_eq!(pm.add_item(&placeholder, InvalidOffsetNumber, 0), Some(1));
    page_opaque_update(&mut pm, |op| op.nPlaceholder = 1);
    // A corrupted line pointer: the placeholder's lp_len understates its
    // storage, so deleting it frees less than the size check assumed.
    let id = pm.as_ref().item_id(1);
    pm.set_item_id(1, ItemIdData::new(id.lp_off(), LP_NORMAL, 8));
    // No free space left beyond what the placeholder gives back.
    let lower = pm.as_ref().pd_lower();
    pm.set_pd_upper(lower);

    let item = leaf_tuple(ItemPointerData::new(10, 1));
    let err = SpGistPageAddNewItem(&mut pm, &item, None, false)
        .expect_err("the replacement add must fail on the crafted page");
    assert_eq!(err.level(), PANIC, "spgutils.c:1273 is elog(PANIC): {err:?}");
    assert_eq!(err.message(), "failed to add item of size 16 to SPGiST index page");
    // The placeholder is gone: exactly the torn state C PANICs to contain.
    assert_eq!(pm.as_ref().max_offset_number(), 0);
}

// spgdoinsert.c:90 addNode: an offset past nNodes is elog(ERROR, "invalid
// offset for adding node to SPGiST inner tuple") — catchable XX000, never a
// panic (row a186-candidate-fp-spgist-spgdoinsert-03580db59cd021b4e7c5-1).
#[test]
fn add_node_out_of_range_offset_errors_not_panics() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut state = spg_state(mcx);
    let inner = two_node_inner();
    let err = addNode(mcx, &mut state, &inner, Datum::from_i32(0), 3)
        .err()
        .expect("offset 3 > nNodes 2 must be rejected");
    assert_eq!(err.level(), ERROR);
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "invalid offset for adding node to SPGiST inner tuple");
    // In range (append) still works: three nodes come back.
    let grown = addNode(mcx, &mut state, &inner, Datum::from_i32(0), -1)
        .unwrap_or_else(|e| panic!("append: {e:?}"));
    assert_eq!(SpGistInnerTupleHeader::decode(grown.as_slice()).nNodes, 3);
}
