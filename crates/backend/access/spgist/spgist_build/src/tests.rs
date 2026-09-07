//! Unit witnesses for spgbuild's C contracts (audit-18.6 remediation wave 2,
//! batch w2-001): a fake buffer pool (8KB boxes, Buffer = block + 1) and a
//! relation stand-in so the build prologue can be driven to its error arms
//! without a server. Fixture state lives in plain statics serialized by
//! `lock()` (no `thread_local!`: the session census pin).

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, Once};

use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    Buffer, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT, SPGIST_AM_OID,
};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, REPLICA_IDENTITY_DEFAULT,
};
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::TupleDescData;

// MAXALIGNed like real buffer pages (the PageRef contract).
#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

const INDEX_OID: Oid = 5200;
const HEAP_OID: Oid = 5199;
const DIRTY_FAIL_MSG: &str = "simulated MarkBufferDirty failure";

static PAGES: Mutex<Vec<usize>> = Mutex::new(Vec::new());
// RelationGetNumberOfBlocks(index) as the test wants it.
static NBLOCKS: AtomicU32 = AtomicU32::new(0);
// The next MarkBufferDirty fails with DIRTY_FAIL_MSG (an ERROR-class failure
// inside the page-initialization phase).
static DIRTY_FAIL: AtomicBool = AtomicBool::new(false);
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn pages() -> MutexGuard<'static, Vec<usize>> {
    PAGES.lock().unwrap_or_else(|e| e.into_inner())
}

fn push_page() -> Buffer {
    let p: &'static mut FakePage = Box::leak(Box::new(FakePage([0u8; BLCKSZ])));
    let mut pages = pages();
    pages.push(p as *mut FakePage as usize);
    pages.len() as Buffer
}

fn page_bytes(buf: Buffer) -> core::ptr::NonNull<u8> {
    let pages = pages();
    let idx = (buf - 1) as usize;
    assert!(idx < pages.len(), "fake pool has no buffer {buf}");
    // SAFETY: leaked FakePage, never freed, non-null.
    unsafe { core::ptr::NonNull::new_unchecked(pages[idx] as *mut u8) }
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| {
            Ok(NBLOCKS.load(Relaxed))
        });
        bufmgr_seams::buffer_get_page::set(page_bytes);
        bufmgr_seams::mark_buffer_dirty::set(|_buf| {
            if DIRTY_FAIL.load(Relaxed) {
                return Err(Box::new(PgError::error(DIRTY_FAIL_MSG)));
            }
            Ok(())
        });
    });
}

fn take_crit_section_count() -> u32 {
    let count = init_small::globals::CritSectionCount();
    init_small::globals::SetCritSectionCount(0);
    DIRTY_FAIL.store(false, Relaxed);
    count
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
    let mut name = ::types_tuple::NameData::default();
    name.namestrcpy("t_spgidx");
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
        rd_rel: FormData_pg_class {
            relname: name,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: SPGIST_AM_OID,
            relfilenode: INDEX_OID,
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

// spginsert.c:82-84 spgbuild: a main fork that already has blocks is
// elog(ERROR, "index \"%s\" already contains data") — catchable XX000, never
// a panic (row a186-candidate-fp-spgist-b1-3fb63a08dcb5da788d25-1).
#[test]
fn build_on_nonempty_index_is_a_c_shaped_error() {
    let _serial = lock();
    install();
    let ctx = MemoryContext::new("t");
    let index = index_rel(ctx.mcx());

    NBLOCKS.store(1, Relaxed);
    let res = crate::spgbuild_check_empty(&index);
    NBLOCKS.store(0, Relaxed);
    let err = res.expect_err("a non-empty main fork must be refused");
    assert_eq!(err.level(), ERROR, "elog(ERROR) level: {err:?}");
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR, "elog(ERROR) sqlstate: {err:?}");
    assert_eq!(err.message(), "index \"t_spgidx\" already contains data");
    assert_eq!(err.hint(), None, "elog(ERROR) carries no HINT: {err:?}");

    // The empty fork passes.
    crate::spgbuild_check_empty(&index).unwrap_or_else(|e| panic!("empty fork: {e:?}"));
}

// spginsert.c:97-107 spgbuild: metapage/root/nulls initialization and their
// MarkBufferDirty calls are one critical section (row
// a186-candidate-fp-spgist-b1-4643841959809d628a2e-1).
#[test]
fn init_pages_dirty_failure_escapes_inside_the_critical_section() {
    let _serial = lock();
    install();
    let metabuffer = push_page();
    let rootbuffer = push_page();
    let nullbuffer = push_page();

    init_small::globals::SetCritSectionCount(0);
    DIRTY_FAIL.store(true, Relaxed);
    let err = crate::spgbuild_init_pages(metabuffer, rootbuffer, nullbuffer)
        .expect_err("the simulated MarkBufferDirty failure must surface");
    let count = take_crit_section_count();
    assert_eq!(err.message(), DIRTY_FAIL_MSG);
    assert!(
        count > 0,
        "spginsert.c:97 START_CRIT_SECTION: the Err escaped spgbuild's page \
         initialization with CritSectionCount = {count}, so it would be recovered \
         as a plain ERROR (half-initialized fixed pages in shared buffers) instead \
         of PANIC"
    );
}

// The Ok path leaves the section balanced (spginsert.c:107) with the three
// fixed pages initialized.
#[test]
fn init_pages_balance_the_critical_section() {
    let _serial = lock();
    install();
    let metabuffer = push_page();
    let rootbuffer = push_page();
    let nullbuffer = push_page();

    init_small::globals::SetCritSectionCount(0);
    DIRTY_FAIL.store(false, Relaxed);
    crate::spgbuild_init_pages(metabuffer, rootbuffer, nullbuffer)
        .unwrap_or_else(|e| panic!("init pages on the fake pool: {e:?}"));
    assert_eq!(take_crit_section_count(), 0, "END_CRIT_SECTION after the last dirty");
    let root = spgist::spg_buf_page_mut(rootbuffer);
    assert!(::types_spgist::SpGistPageIsLeaf(&root.as_ref()));
    let nulls = spgist::spg_buf_page_mut(nullbuffer);
    assert!(::types_spgist::SpGistPageStoresNulls(&nulls.as_ref()));
}
