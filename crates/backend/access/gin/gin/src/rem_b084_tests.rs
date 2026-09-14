//! audit-18.6 remediation batch b084 witnesses (backend/access/gin).
//!
//! Layered on tests.rs's `fake_bufmgr` page table. The added seams record,
//! per WAL record and per MarkBufferDirty, the `CritSectionCount` in force at
//! the call, which is the C invariant these witnesses assert (xlog.c
//! `Assert(CritSectionCount > 0)` under XLogInsert; the START/END_CRIT_SECTION
//! brackets of ginutil.c:666/705, ginvacuum.c:161/231 and :660/665,
//! gininsert.c:643/652). Recorders are process-wide statics keyed by thread
//! id (no new `thread_local!`s: the session census pins the tree-wide count).

use std::cell::Cell;
use std::rc::Rc;
use std::sync::{Mutex, Once};
use std::thread::ThreadId;

use ::bufmgr_seams as bm;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, InvalidBlockNumber, InvalidOid, Oid, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use ::types_error::{PgError, PgResult, DEBUG1};
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
    RELKIND_INDEX, REPLICA_IDENTITY_DEFAULT,
};
use ::types_tuple::itemptr::{InvalidOffsetNumber, ItemPointerData};
use ::types_tuple::tupdesc::CompactAttribute;
use ::types_tuple::TupleDescData;

use crate::btree::{ginFinishOldSplitAt, Frame, GinStack};
use crate::datapage::{gin_data_page_add_posting_item, DataBtree};
use crate::tests::fake_bufmgr;
use crate::util::{
    gin_build_init_pages, gin_init_metapage_bytes, gin_init_page_bytes, ginUpdateStats,
};
use crate::vacuum::{
    ginbulkdelete_restore_page, ginDeletePage, ginvacuumcleanup, GinVacDelete, GinVacuumState,
};
use crate::write_opaque_to;

#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

/// (thread, record info, CritSectionCount at XLogInsert).
static WAL: Mutex<Vec<(ThreadId, u8, u32)>> = Mutex::new(Vec::new());
/// (thread, buffer, CritSectionCount at MarkBufferDirty).
static DIRTY: Mutex<Vec<(ThreadId, Buffer, u32)>> = Mutex::new(Vec::new());
/// (thread, level, message) per emitted log report.
static LOGGED: Mutex<Vec<(ThreadId, i32, String)>> = Mutex::new(Vec::new());

/// Process-wide MarkBufferDirty ledger entry for the calling thread (also
/// fed by the b006 rig's seam closure when that rig installed the seam first).
pub(crate) fn record_dirty(buf: Buffer) {
    DIRTY.lock().unwrap().push((
        std::thread::current().id(),
        buf,
        init_small::globals::CritSectionCount(),
    ));
}

/// Process-wide XLogInsert ledger entry for the calling thread (also fed by
/// the b006 rig's seam closure).
pub(crate) fn record_wal(info: u8) {
    WAL.lock().unwrap().push((
        std::thread::current().id(),
        info,
        init_small::globals::CritSectionCount(),
    ));
}

pub(crate) fn install() {
    fake_bufmgr::install();
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // The b006 rig (rem_b006_tests::rig) records the same two seams into
        // its thread-local ledgers; a seam is installed once per process, so
        // whichever rig wins the race feeds BOTH recorders.
        if !bm::mark_buffer_dirty::is_installed() {
            bm::mark_buffer_dirty::set(|buf| {
                record_dirty(buf);
                crate::rem_b006_tests::rig::record_dirty(buf);
                Ok(())
            });
        }
        if !xloginsert_seams::xlog_insert_record::is_installed() {
            xloginsert_seams::xlog_insert_record::set(|_rmid, info, _flags, _data, _bufs| {
                record_wal(info);
                crate::rem_b006_tests::rig::record_wal(info)?;
                Ok(0x1000)
            });
        }
        if !transam_xlog_seams::xlog_standby_info_active::is_installed() {
            transam_xlog_seams::xlog_standby_info_active::set(|| false);
        }
        if !predicate_seams::predicate_lock_page_combine::is_installed() {
            predicate_seams::predicate_lock_page_combine::set(|_rel, _old, _new| Ok(()));
        }
        if !varsup_seams::read_next_transaction_id::is_installed() {
            varsup_seams::read_next_transaction_id::set(|| Ok(1234));
        }
        if !lock_seams::lock_acquire_extended::is_installed() {
            lock_seams::lock_acquire_extended::set(|_tag, _mode, _s, _dw, _r, _l| {
                Ok(::types_storage::lock::LOCKACQUIRE_OK)
            });
        }
        if !lock_seams::lock_release::is_installed() {
            lock_seams::lock_release::set(|_tag, _mode, _s| Ok(true));
        }
        if !syscache_seams::lookup_pg_amproc::is_installed() {
            // A tsvector_ops-shaped opclass (procs 1-4 present, no proc 5/6)
            // for this rig's opfamily 3659; the w2_043 rig's custom opclass
            // shapes are keyed by their own opfamily oids.
            syscache_seams::lookup_pg_amproc::set(|opfamily, _left, _right, procnum| {
                if let Some(proc_oid) = crate::w2_043_tests::amproc_of(opfamily, procnum as u16) {
                    return Ok(proc_oid);
                }
                Ok(match procnum as u16 {
                    GIN_COMPARE_PROC => crate::opclass::F_GIN_CMP_TSLEXEME,
                    GIN_EXTRACTVALUE_PROC => crate::opclass::F_GIN_EXTRACT_TSVECTOR,
                    GIN_EXTRACTQUERY_PROC => crate::opclass::F_GIN_EXTRACT_TSQUERY,
                    GIN_CONSISTENT_PROC => 3658,
                    _ => InvalidOid,
                })
            });
        }
        guc_tables::vars::autovacuum_work_mem.install_if_absent(guc_tables::GucVarAccessors {
            get: || -1,
            set: |_| {},
        });
    });
}

fn this_thread_wal() -> Vec<(u8, u32)> {
    let me = std::thread::current().id();
    WAL.lock().unwrap().iter().filter(|r| r.0 == me).map(|r| (r.1, r.2)).collect()
}

fn this_thread_dirty() -> Vec<(Buffer, u32)> {
    let me = std::thread::current().id();
    DIRTY.lock().unwrap().iter().filter(|r| r.0 == me).map(|r| (r.1, r.2)).collect()
}

fn clear_recorders() {
    let me = std::thread::current().id();
    WAL.lock().unwrap().retain(|r| r.0 != me);
    DIRTY.lock().unwrap().retain(|r| r.0 != me);
    LOGGED.lock().unwrap().retain(|r| r.0 != me);
}

fn leak(page: Box<FakePage>) -> core::ptr::NonNull<u8> {
    core::ptr::NonNull::from(Box::leak(page)).cast::<u8>()
}

fn blank_page() -> Box<FakePage> {
    Box::new(FakePage([0u8; BLCKSZ]))
}

fn gin_page(flags: u16, rightlink: BlockNumber) -> Box<FakePage> {
    let mut p = blank_page();
    gin_init_page_bytes(&mut p.0, flags);
    write_opaque_to(
        &mut p.0,
        &GinPageOpaqueData {
            rightlink,
            maxoff: 0,
            flags,
        },
    );
    p
}

fn metapage() -> Box<FakePage> {
    let mut p = blank_page();
    gin_init_metapage_bytes(&mut p.0);
    p
}

/// Block `n` of the current page table (fake_bufmgr: buffer = blkno + 1).
fn page_bytes_of(blkno: BlockNumber) -> &'static mut [u8] {
    let ptr = bm::buffer_get_page::call(blkno as Buffer + 1);
    // SAFETY: leaked BLCKSZ image of this thread's page table.
    unsafe { core::slice::from_raw_parts_mut(ptr.as_ptr(), BLCKSZ) }
}

fn opaque_flags(bytes: &[u8]) -> u16 {
    u16::from_ne_bytes([bytes[BLCKSZ - 2], bytes[BLCKSZ - 1]])
}

fn tsvector_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen: -1,
        attbyval: false,
        attispackable: true,
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

/// A permanent GIN index relation ("b084_gin", oid 6084) over one text-keyed
/// column (tsvector_ops shape; opfamily 3659).
fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
    let mut relname = ::types_tuple::NameData::default();
    relname.namestrcpy("b084_gin");
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
        rd_locator: Cell::new(::types_storage::RelFileLocator::new(1663, 5, 6084)),
        rd_smgr: Default::default(),
        rd_id: 6084,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: 6084, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: ::types_core::catalog::GIN_AM_OID,
            relfilenode: 6084,
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
        rd_att: Rc::new(tsvector_tupdesc(mcx)),
        rd_index: Some(FormData_pg_index {
            indexrelid: 6084,
            indrelid: 6083,
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
        rd_opcintype: one(3614),
        rd_opfamily: one(3659),
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

fn text_col() -> GinColState {
    GinColState {
        compare_partial: None,
        can_partial_match: false,
        ..GinColState::tsvector_ops(0)
    }
}

fn one_col_state() -> GinState {
    GinState {
        natts: 1,
        one_col: true,
        cols: [text_col(); GIN_MAX_KEY_COLS],
    }
}

fn assert_all_inside_crit_section(what: &str, recs: &[(impl core::fmt::Debug, u32)]) {
    assert!(!recs.is_empty(), "no {what} recorded");
    for (which, count) in recs {
        assert!(
            *count > 0,
            "{what} {which:?} outside a critical section (CritSectionCount = {count}); all: {recs:?}"
        );
    }
}

// ginutil.c:666-705: ginUpdateStats brackets the metapage update, the
// XLOG_GIN_UPDATE_META_PAGE insert and the buffer release in a critical
// section.
#[test]
fn update_stats_wal_record_is_inside_critical_section() {
    install();
    fake_bufmgr::set_pages(vec![leak(metapage())]);
    clear_recorders();

    let ctx = MemoryContext::new("b084");
    let rel = index_rel(ctx.mcx());
    let stats = GinStatsData {
        nTotalPages: 7,
        nEntryPages: 3,
        nDataPages: 2,
        nEntries: 40,
        ..Default::default()
    };
    ginUpdateStats(&rel, &stats, false).unwrap();

    let wal = this_thread_wal();
    assert_eq!(wal.iter().map(|r| r.0).collect::<Vec<_>>(), vec![XLOG_GIN_UPDATE_META_PAGE]);
    assert_all_inside_crit_section("XLogInsert", &wal);
    assert_all_inside_crit_section("MarkBufferDirty", &this_thread_dirty());
    assert_eq!(init_small::globals::CritSectionCount(), 0, "critical section left open");
    assert_eq!(fake_bufmgr::pins(), 0, "pins leaked");
}

// ginvacuum.c:161-231: ginDeletePage unlinks the page, deletes the parent's
// downlink, marks it deleted and logs XLOG_GIN_DELETE_PAGE inside one
// critical section.
#[test]
fn delete_page_wal_record_is_inside_critical_section() {
    install();
    // 0 = metapage stand-in, 1 = left leaf -> 2, 2 = the empty leaf to delete,
    // 3 = the (data, non-leaf) parent holding one downlink to block 2.
    let mut parent = gin_page(GIN_DATA, InvalidBlockNumber);
    let mut item = PostingItem {
        child_blkno: Default::default(),
        key: ItemPointerData::new(1, 1),
    };
    PostingItemSetBlockNumber(&mut item, 2);
    gin_data_page_add_posting_item(&mut parent.0, &item, InvalidOffsetNumber);
    fake_bufmgr::set_pages(vec![
        leak(metapage()),
        leak(gin_page(GIN_DATA | GIN_LEAF | GIN_COMPRESSED, 2)),
        leak(gin_page(GIN_DATA | GIN_LEAF | GIN_COMPRESSED, InvalidBlockNumber)),
        leak(parent),
    ]);
    clear_recorders();

    let ctx = MemoryContext::new("b084");
    let rel = index_rel(ctx.mcx());
    let state = one_col_state();
    let mut stats = IndexBulkDeleteResult::default();
    let mut gvs = GinVacuumState {
        rel: &rel,
        state: &state,
        delete: GinVacDelete::DeadItems(&[]),
        stats: &mut stats,
        strategy: None,
    };
    ginDeletePage(&mut gvs, 2, 1, 3, 1).unwrap();

    let wal = this_thread_wal();
    assert_eq!(wal.iter().map(|r| r.0).collect::<Vec<_>>(), vec![XLOG_GIN_DELETE_PAGE]);
    assert_all_inside_crit_section("XLogInsert", &wal);
    let dirty = this_thread_dirty();
    assert_eq!(dirty.len(), 3, "parent, left and deleted page are dirtied: {dirty:?}");
    assert_all_inside_crit_section("MarkBufferDirty", &dirty);
    assert_eq!(init_small::globals::CritSectionCount(), 0, "critical section left open");
    assert_eq!(opaque_flags(page_bytes_of(2)), GIN_DELETED);
    assert_eq!(stats.pages_deleted, 1);
    assert_eq!(fake_bufmgr::pins(), 0, "pins leaked");
}

// ginvacuum.c:658-666: ginbulkdelete restores the vacuumed entry page, marks
// it dirty, logs XLOG_GIN_VACUUM_PAGE and releases it in a critical section.
#[test]
fn bulkdelete_restore_page_wal_record_is_inside_critical_section() {
    install();
    fake_bufmgr::set_pages(vec![leak(metapage()), leak(gin_page(GIN_LEAF, InvalidBlockNumber))]);
    clear_recorders();

    let ctx = MemoryContext::new("b084");
    let rel = index_rel(ctx.mcx());
    let tmp: Vec<u8> = page_bytes_of(1).to_vec();
    // The buffer arrives pinned + exclusively locked (the entry-tree sweep).
    let buffer = bm::read_buffer::call(&rel, 1).unwrap();
    ginbulkdelete_restore_page(&rel, buffer, &tmp).unwrap();

    let wal = this_thread_wal();
    assert_eq!(wal.iter().map(|r| r.0).collect::<Vec<_>>(), vec![XLOG_GIN_VACUUM_PAGE]);
    assert_all_inside_crit_section("XLogInsert", &wal);
    assert_all_inside_crit_section("MarkBufferDirty", &this_thread_dirty());
    assert_eq!(init_small::globals::CritSectionCount(), 0, "critical section left open");
    assert_eq!(fake_bufmgr::pins(), 0, "the restore step releases its buffer");
}

// gininsert.c:643-652: ginbuild initializes and dirties the metapage and the
// root page inside a critical section (ginbuildempty's shape).
#[test]
fn build_init_pages_dirties_inside_critical_section() {
    install();
    fake_bufmgr::set_pages(vec![leak(blank_page()), leak(blank_page())]);
    clear_recorders();

    let ctx = MemoryContext::new("b084");
    let rel = index_rel(ctx.mcx());
    // GinNewBuffer's result: pinned + exclusively locked buffers.
    let meta = bm::read_buffer::call(&rel, 0).unwrap();
    let root = bm::read_buffer::call(&rel, 1).unwrap();
    gin_build_init_pages(meta, root).unwrap();

    let dirty = this_thread_dirty();
    assert_eq!(dirty.iter().map(|r| r.0).collect::<Vec<_>>(), vec![meta, root]);
    assert_all_inside_crit_section("MarkBufferDirty", &dirty);
    assert_eq!(init_small::globals::CritSectionCount(), 0, "critical section left open");
    assert_eq!(opaque_flags(page_bytes_of(0)), GIN_META);
    assert_eq!(opaque_flags(page_bytes_of(1)), GIN_LEAF);
    assert_eq!(fake_bufmgr::pins(), 0, "both buffers released");
}

// ginvacuum.c:708-716: in an autovacuum ANALYZE, ginvacuumcleanup runs the
// pending-list cleanup and returns the caller's stats pointer unchanged —
// NULL from analyze.c, so no "index ... now contains" report is produced.
#[test]
fn vacuumcleanup_analyze_only_returns_caller_stats() {
    install();
    // A metapage with no pending list: the cleanup takes the early exit.
    fake_bufmgr::set_pages(vec![leak(metapage())]);
    clear_recorders();

    let ctx = MemoryContext::new("b084");
    let rel = index_rel(ctx.mcx());
    let info = ::nbtree::IndexVacuumInfo {
        index: &rel,
        heaprel: &rel,
        analyze_only: true,
        report_progress: false,
        estimated_count: true,
        message_level: ::types_error::DEBUG2,
        num_heap_tuples: 10.0,
        strategy: None,
    };
    let before = miscinit::GetMyBackendType();
    miscinit::SetMyBackendType(::types_core::BackendType::AutovacWorker);
    let none = ginvacuumcleanup(ctx.mcx(), &info, None);
    let some = ginvacuumcleanup(
        ctx.mcx(),
        &info,
        Some(IndexBulkDeleteResult {
            num_pages: 9,
            ..Default::default()
        }),
    );
    miscinit::SetMyBackendType(before);

    assert!(none.unwrap().is_none(), "NULL stats in -> NULL stats out (C returns `stats`)");
    assert_eq!(some.unwrap().map(|s| s.num_pages), Some(9));
    assert_eq!(fake_bufmgr::pins(), 0, "pins leaked");
}

fn record_log(error: &PgError, _output_to_server: &mut bool) {
    LOGGED.lock().unwrap().push((
        std::thread::current().id(),
        error.level.0,
        error.message().to_string(),
    ));
}

// ginbtree.c:782: ginFinishOldSplit logs DEBUG1 "finishing incomplete split
// of block %u in gin index \"%s\"" before re-checking the page (the lock
// upgrade path may find the split already finished).
#[test]
fn finish_old_split_logs_debug1() {
    install();
    fake_bufmgr::set_pages(vec![
        leak(metapage()),
        leak(gin_page(GIN_DATA | GIN_LEAF | GIN_COMPRESSED, InvalidBlockNumber)),
    ]);
    clear_recorders();

    let ctx = MemoryContext::new("b084");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx);
    let buffer = bm::read_buffer::call(&rel, 1).unwrap();
    let mut stack = GinStack {
        frames: PgVec::new_in(mcx),
        top: 0,
    };
    stack.frames.push(Frame {
        blkno: 1,
        buffer,
        off: 0,
        predictNumber: 1,
        parent: 0,
    });
    let mut btree = DataBtree::new(&rel, 1, mcx);

    let prev_level = elog::config::log_min_messages();
    elog::config::set_log_min_messages(DEBUG1);
    let prev_hook = elog::set_emit_log_hook(Some(record_log));
    let res = ginFinishOldSplitAt(mcx, &rel, &mut btree, &mut stack, 0, None, crate::GIN_SHARE);
    elog::set_emit_log_hook(prev_hook);
    elog::config::set_log_min_messages(prev_level);
    res.unwrap();
    bm::release_buffer::call(buffer).unwrap();

    let me = std::thread::current().id();
    let logged: Vec<(i32, String)> = LOGGED
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.0 == me)
        .map(|r| (r.1, r.2.clone()))
        .collect();
    assert_eq!(
        logged,
        vec![(DEBUG1.0, "finishing incomplete split of block 1 in gin index \"b084_gin\"".to_string())]
    );
    assert_eq!(fake_bufmgr::pins(), 0, "pins leaked");
}
