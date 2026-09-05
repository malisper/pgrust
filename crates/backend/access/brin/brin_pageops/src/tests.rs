//! Critical-section witnesses for the BRIN page/revmap mutators (audit
//! remediation batch b005, rows brin_pageops-ce98865b / brin_revmap-2359a9ed /
//! brin_revmap-8c1646f4 / brin_revmap-d886818b).
//!
//! C (brin_pageops.c:178/242/408/892, brin_revmap.c:398/602) brackets every
//! page mutation + XLogInsert in START_CRIT_SECTION()/END_CRIT_SECTION(), so an
//! error raised between the mutation and the WAL record (WAL disk full, ...)
//! is promoted to PANIC instead of leaving a dirty, unlogged page in shared
//! buffers. pgrust's equivalent is `init_small::globals::CritSectionCount`
//! (StartCriticalSection/EndCriticalSection): an Err escaping while it is
//! elevated is promoted at the catch boundary
//! (`elog::panic_on_crit_section_escape`). These tests drive the mutators over
//! a fake buffer manager and observe the counter from inside the WAL seam.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Once;

use ::init_small::globals as g;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, InvalidBuffer, Oid, BLCKSZ, BRIN_AM_OID, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, REPLICA_IDENTITY_DEFAULT,
};
use ::types_storage::bufpage::PageMut;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::TupleDescData;

use super::*;

// Fake buffer manager: pages are 8KB MAXALIGNed boxes; Buffer = block + 1.
#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

fn leak_page(p: Box<FakePage>) -> core::ptr::NonNull<FakePage> {
    core::ptr::NonNull::from(Box::leak(p))
}

thread_local! {
    static PAGES: RefCell<Vec<core::ptr::NonNull<FakePage>>> = const { RefCell::new(Vec::new()) };
    // CritSectionCount observed by every xlog_insert_record call.
    static XLOG_CRIT: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    // Make the next xlog_insert_record fail (WAL write failure simulation).
    static XLOG_FAIL: Cell<bool> = const { Cell::new(false) };
    static NBLOCKS: Cell<BlockNumber> = const { Cell::new(0) };
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        bufmgr_seams::read_buffer::set(|_rel, blkno| Ok(blkno as Buffer + 1));
        bufmgr_seams::release_buffer::set(|_buf| Ok(()));
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
        bufmgr_seams::buffer_get_page::set(|buf| {
            PAGES.with(|p| p.borrow()[(buf - 1) as usize].cast::<u8>())
        });
        bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| {
            Ok(NBLOCKS.with(Cell::get))
        });
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        xloginsert_seams::xlog_insert_record::set(|rmid, _info, _flags, _main, _bufs| {
            assert_eq!(rmid, RmgrIds::RM_BRIN_ID as u8);
            XLOG_CRIT.with(|c| c.borrow_mut().push(g::CritSectionCount()));
            if XLOG_FAIL.with(Cell::get) {
                return Err(Box::new(
                    PgError::error("could not write to WAL (simulated)")
                        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
                ));
            }
            Ok(0x1000)
        });
    });
}

fn xlog_crit_counts() -> Vec<u32> {
    XLOG_CRIT.with(|c| c.borrow().clone())
}

fn reset() {
    install();
    PAGES.with(|p| p.borrow_mut().clear());
    XLOG_CRIT.with(|c| c.borrow_mut().clear());
    XLOG_FAIL.with(|c| c.set(false));
    g::SetCritSectionCount(0);
}

fn push_page(p: Box<FakePage>) -> Buffer {
    PAGES.with(|pages| {
        let mut pages = pages.borrow_mut();
        pages.push(leak_page(p));
        pages.len() as Buffer
    })
}

fn zero_page() -> Box<FakePage> {
    Box::new(FakePage([0u8; BLCKSZ]))
}

fn with_page_mut(p: &mut FakePage, f: impl FnOnce(&mut PageMut<'_>)) {
    let ptr = core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap();
    // SAFETY: owned MAXALIGNed BLCKSZ image, exclusively borrowed.
    let mut pm = unsafe { PageMut::from_raw(ptr) };
    f(&mut pm);
}

// Block 0: metapage with pages_per_range = 1 and lastRevmapPage = 1.
fn meta_page() -> Box<FakePage> {
    let mut p = zero_page();
    with_page_mut(&mut p, |pm| {
        brin_metapage_init(pm, 1, BRIN_CURRENT_VERSION);
        let mut meta = brin_meta_read(&pm.as_ref());
        meta.lastRevmapPage = 1;
        brin_meta_write(pm, &meta);
    });
    p
}

// Block 1: the first revmap page; slot 0 (heap block 0) -> `tid`.
fn revmap_page(tid: ItemPointerData) -> Box<FakePage> {
    let mut p = zero_page();
    with_page_mut(&mut p, |pm| {
        brin_page_init(pm, BRIN_PAGETYPE_REVMAP);
        revmap_set_tid(pm, 0, tid);
    });
    p
}

// A regular page carrying the given tuples at offsets 1..
fn regular_page(tuples: &[&[u8]]) -> Box<FakePage> {
    let mut p = zero_page();
    with_page_mut(&mut p, |pm| {
        brin_page_init(pm, BRIN_PAGETYPE_REGULAR);
        for t in tuples {
            pm.add_item(t, InvalidOffsetNumber, 0).expect("page has room");
        }
    });
    p
}

// 16-byte MAXALIGNed opaque BRIN tuple image.
#[repr(C, align(8))]
struct Tup([u8; 16]);

fn tup(fill: u8) -> Tup {
    Tup([fill; 16])
}

fn revmap(meta_buf: Buffer) -> BrinRevmap {
    BrinRevmap {
        rm_pagesPerRange: 1,
        rm_lastRevmapPage: Cell::new(1),
        rm_metaBuf: meta_buf,
        rm_currBuf: Cell::new(InvalidBuffer),
    }
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
    Ok(())
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

// A permanent BRIN index relation (relation_needs_wal = true).
fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
    let mut relname = ::types_tuple::NameData::default();
    relname.namestrcpy("t_brin_idx");
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
            relam: BRIN_AM_OID,
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
            indexprs_src: None,
            indpred_src: None,
        }),
        rd_opcintype: one(23),
        rd_opfamily: one(4054),
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

// brin_pageops.c:408-451 — brin_doinsert's page mutation + XLogInsert run
// inside one critical section.
#[test]
fn doinsert_mutates_and_logs_inside_a_critical_section() {
    reset();
    let ctx = MemoryContext::new("t");
    let rel = index_rel(ctx.mcx());
    let meta = push_page(meta_page());
    let _rm = push_page(revmap_page(ItemPointerData::invalid()));
    let mut buffer = push_page(regular_page(&[]));
    let rm = revmap(meta);

    let t = tup(0x5A);
    let off = brin_doinsert(&rel, 1, &rm, &mut buffer, 0, &t.0).expect("insert");
    assert_eq!(off, 1);
    assert_eq!(xlog_crit_counts(), vec![1], "XLogInsert must run with CritSectionCount > 0");
    assert_eq!(g::CritSectionCount(), 0, "END_CRIT_SECTION after the WAL record");
}

// The promotion contract: a WAL failure after the page was mutated must
// escape with the critical section still open (C: errfinish promotes the
// ERROR to PANIC because CritSectionCount > 0), never as a recoverable ERROR
// that leaves the dirty, unlogged page behind.
#[test]
fn doinsert_wal_failure_escapes_with_the_critical_section_open() {
    reset();
    let ctx = MemoryContext::new("t");
    let rel = index_rel(ctx.mcx());
    let meta = push_page(meta_page());
    let _rm = push_page(revmap_page(ItemPointerData::invalid()));
    let mut buffer = push_page(regular_page(&[]));
    let rm = revmap(meta);

    XLOG_FAIL.with(|c| c.set(true));
    let t = tup(0x5A);
    let err = brin_doinsert(&rel, 1, &rm, &mut buffer, 0, &t.0)
        .expect_err("the simulated WAL failure must propagate");
    assert_eq!(err.message, "could not write to WAL (simulated)");
    assert!(
        g::CritSectionCount() > 0,
        "an Err escaping the mutation+WAL window must leave the section open for PANIC promotion"
    );
    g::SetCritSectionCount(0);
}

// brin_pageops.c:178-203 — same-page update.
#[test]
fn samepage_update_mutates_and_logs_inside_a_critical_section() {
    reset();
    let ctx = MemoryContext::new("t");
    let rel = index_rel(ctx.mcx());
    let meta = push_page(meta_page());
    let old = tup(0x11);
    let _rm = push_page(revmap_page(ItemPointerData::new(2, 1)));
    let oldbuf = push_page(regular_page(&[&old.0[..]]));
    let rm = revmap(meta);

    let new = tup(0x22);
    let done = brin_doupdate(&rel, 1, &rm, 0, oldbuf, 1, &old.0, &new.0, true).expect("update");
    assert!(done);
    assert_eq!(xlog_crit_counts(), vec![1]);
    assert_eq!(g::CritSectionCount(), 0);
}

// brin_revmap.c:398-427 — brinRevmapDesummarizeRange.
#[test]
fn desummarize_mutates_and_logs_inside_a_critical_section() {
    reset();
    let ctx = MemoryContext::new("t");
    let rel = index_rel(ctx.mcx());
    let _meta = push_page(meta_page());
    let old = tup(0x11);
    let _rm = push_page(revmap_page(ItemPointerData::new(2, 1)));
    let _reg = push_page(regular_page(&[&old.0[..]]));

    let done = brinRevmapDesummarizeRange(&rel, 0).expect("desummarize");
    assert!(done);
    assert_eq!(xlog_crit_counts(), vec![1]);
    assert_eq!(g::CritSectionCount(), 0);
}

// brin_revmap.c:602-640 — revmap_physical_extend (via brinRevmapExtend).
#[test]
fn revmap_extend_mutates_and_logs_inside_a_critical_section() {
    reset();
    let ctx = MemoryContext::new("t");
    let rel = index_rel(ctx.mcx());
    let meta = push_page(meta_page());
    let _rm = push_page(revmap_page(ItemPointerData::invalid()));
    let _fresh = push_page(zero_page()); // block 2: the page the revmap grows into
    NBLOCKS.with(|c| c.set(3));
    let rm = revmap(meta);

    // The first heap block whose revmap slot lives on revmap page 2.
    let heap_blk = REVMAP_PAGE_MAXITEMS as BlockNumber;
    brinRevmapExtend(&rel, &rm, heap_blk).expect("extend");
    assert_eq!(rm.rm_lastRevmapPage.get(), 2);
    assert_eq!(xlog_crit_counts(), vec![1]);
    assert_eq!(g::CritSectionCount(), 0);
}

// brin_revmap.c:471 — a heap block the revmap does not cover is an
// elog(ERROR) (catchable, XX000), not a panic.
#[test]
fn uncovered_heap_block_is_an_error_not_a_panic() {
    reset();
    let ctx = MemoryContext::new("t");
    let rel = index_rel(ctx.mcx());
    let meta = push_page(meta_page());
    let _rm = push_page(revmap_page(ItemPointerData::invalid()));
    let rm = revmap(meta);

    let heap_blk = 2 * REVMAP_PAGE_MAXITEMS as BlockNumber;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        brinLockRevmapPageForUpdate(&rel, &rm, heap_blk)
    }));
    let err = outcome
        .expect("revmap_get_buffer must not panic")
        .expect_err("the uncovered block must be reported");
    assert_eq!(err.message, format!("revmap does not cover heap block {heap_blk}"));
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
}
