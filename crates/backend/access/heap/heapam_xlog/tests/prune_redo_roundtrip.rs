// Prune/freeze/visible redo round-trip per the redo_roundtrip precedent, over
// a fork-aware fake bufmgr (fsm_reuse/prune_roundtrip harnesses): real heapam
// DML builds dead tuples on three pages, the real write sides emit the WAL —
// heap_page_prune_opt (PRUNE_ON_ACCESS), heap_page_prune_and_freeze both with
// and without MARK_UNUSED_NOW (PRUNE_VACUUM_SCAN), the lazy_vacuum_heap_page
// shape (PRUNE_VACUUM_CLEANUP + line-pointer truncation), visibilitymap_set
// x2 (VISIBLE, FPI'd and live-arm variants) — plus a hand-encoded freeze-plan
// record (the C-writer shape our freeze lane does not emit yet). Storage is
// wiped and every record replays through the real rmgr dispatch; heap and VM
// pages must match the write side byte-exact.
#![allow(non_upper_case_globals)]
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;

use heapam::{heap_delete, heap_insert, heap_update};
use heapam_xlog::{
    XLHP_CLEANUP_LOCK, XLHP_HAS_CONFLICT_HORIZON, XLHP_HAS_FREEZE_PLANS, XLOG_HEAP2_MULTI_INSERT,
    XLOG_HEAP2_PRUNE_ON_ACCESS, XLOG_HEAP2_PRUNE_VACUUM_CLEANUP, XLOG_HEAP2_PRUNE_VACUUM_SCAN,
    XLOG_HEAP2_VISIBLE, XLOG_HEAP_OPMASK,
};
use mcx::{Mcx, MemoryContext, PgVec};
use pruneheap::{
    heap_page_prune_and_freeze, log_heap_prune_and_freeze, PruneFreezeResult, PruneReason,
    HEAP_PAGE_PRUNE_MARK_UNUSED_NOW,
};
use tableam_vocab::{LockTupleMode, TM_FailureData, TM_Result, TU_UpdateIndexes};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, Buffer, ForkNumber, Oid, TimeLineID,
    XLogRecPtr, XLogSegNo, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_RELATION};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::bufpage::{PageMut, PageRef, LP_DEAD, LP_NORMAL, LP_REDIRECT, LP_UNUSED};
use types_storage::{ReadBufferMode, RelFileLocator};
use types_tuple::{
    HeapTupleData, HeapTupleHeaderData, InvalidOffsetNumber, ItemPointerData, HEAP_MOVED,
    HEAP_XMAX_BITS, HEAP_XMAX_INVALID, HEAP_XMIN_FROZEN,
};
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;
use xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AAD1;
const REL_OID: Oid = 61020;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const RM_HEAP2_ID: u8 = rmgr::RmgrIds::RM_HEAP2_ID as u8;
// Real xact xids, in assignment order: committed inserts, an aborted insert,
// the page-0 updater/deleter, the page-1/2 inserter, the page-1/2 deleter.
const COMMITTED_XID: u32 = 3;
const ABORTED_XID: u32 = 4;
const UPDATER_XID: u32 = 5;
const LAST_XID: u32 = 7;
const WIDE: usize = 1536;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    // buffer id - 1 indexes all three vectors; forks share the id space.
    bufs: Vec<(ForkNumber, BlockNumber, usize)>,
    pins: Vec<i32>,
    locks: Vec<i32>,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake { bufs: Vec::new(), pins: Vec::new(), locks: Vec::new() });

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn create_page(f: &mut Fake, fork: ForkNumber, blkno: BlockNumber) -> Buffer {
    let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
    f.bufs.push((fork, blkno, addr));
    f.pins.push(0);
    f.locks.push(0);
    f.bufs.len() as Buffer
}

fn find_buf(f: &Fake, fork: ForkNumber, blkno: BlockNumber) -> Buffer {
    f.bufs
        .iter()
        .position(|&(fk, b, _)| fk == fork && b == blkno)
        .map(|i| i as Buffer + 1)
        .unwrap_or_else(|| panic!("no page for fork {fork:?} block {blkno}"))
}

fn nblocks(f: &Fake, fork: ForkNumber) -> BlockNumber {
    f.bufs.iter().filter(|&&(fk, _, _)| fk == fork).count() as BlockNumber
}

fn zero_lock(mode: ReadBufferMode) -> bool {
    matches!(mode, ReadBufferMode::ZeroAndLock | ReadBufferMode::ZeroAndCleanupLock)
}

fn install_fake_bufmgr() {
    bufmgr_seams::read_buffer::set(|_rel, block| {
        with_fake(|f| {
            let buf = find_buf(f, ForkNumber::MAIN_FORKNUM, block);
            f.pins[(buf - 1) as usize] += 1;
            Ok(buf)
        })
    });
    bufmgr_seams::read_buffer_extended::set(|_rel, fork, block, _mode, _strategy| {
        with_fake(|f| {
            let buf = find_buf(f, fork, block);
            f.pins[(buf - 1) as usize] += 1;
            Ok(buf)
        })
    });
    bufmgr_seams::read_buffer_without_relcache::set(|_loc, fork, blkno, mode, _strat, _perm| {
        with_fake(|f| {
            let buf = find_buf(f, fork, blkno);
            f.pins[(buf - 1) as usize] += 1;
            if zero_lock(mode) {
                f.locks[(buf - 1) as usize] += 1;
            }
            Ok(buf)
        })
    });
    bufmgr_seams::release_and_read_buffer::set(|buf, rel, blkno| {
        if buf != types_core::InvalidBuffer {
            let (fork, blk, _) = with_fake(|f| f.bufs[(buf - 1) as usize]);
            if fork == ForkNumber::MAIN_FORKNUM && blk == blkno {
                return Ok(buf);
            }
            bufmgr_seams::release_buffer::call(buf)?;
        }
        bufmgr_seams::read_buffer::call(rel, blkno)
    });
    bufmgr_seams::incr_buffer_ref_count::set(|buf| {
        with_fake(|f| f.pins[(buf - 1) as usize] += 1);
    });
    bufmgr_seams::buffer_get_block_number::set(|buf| with_fake(|f| f.bufs[(buf - 1) as usize].1));
    bufmgr_seams::buffer_get_page::set(|buf| {
        let addr = with_fake(|f| {
            assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
            f.bufs[(buf - 1) as usize].2
        });
        NonNull::new(addr as *mut u8).unwrap()
    });
    bufmgr_seams::release_buffer::set(|buf| {
        with_fake(|f| {
            let p = &mut f.pins[(buf - 1) as usize];
            assert!(*p > 0, "double release of buffer {buf}");
            *p -= 1;
        });
        Ok(())
    });
    bufmgr_seams::lock_buffer::set(|buf, mode| {
        with_fake(|f| {
            let l = &mut f.locks[(buf - 1) as usize];
            match mode {
                bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                    assert!(*l > 0, "unlock without lock");
                    *l -= 1;
                }
                _ => {
                    assert_eq!(*l, 0, "double content lock");
                    *l += 1;
                }
            }
        });
        Ok(())
    });
    bufmgr_seams::conditional_lock_buffer::set(|buf| {
        with_fake(|f| f.locks[(buf - 1) as usize] += 1);
        Ok(true)
    });
    bufmgr_seams::conditional_lock_buffer_for_cleanup::set(|buf| {
        Ok(with_fake(|f| {
            let l = &mut f.locks[(buf - 1) as usize];
            if *l != 0 || f.pins[(buf - 1) as usize] != 1 {
                return false;
            }
            *l += 1;
            true
        }))
    });
    bufmgr_seams::lock_buffer_for_cleanup::set(|buf| {
        bufmgr_seams::lock_buffer::call(buf, bufmgr_seams::BUFFER_LOCK_EXCLUSIVE)
    });
    bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
    bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
    bufmgr_seams::buffer_is_permanent::set(|_buf| true);
    bufmgr_seams::buffer_get_lsn_atomic::set(|buf| {
        let addr = with_fake(|f| f.bufs[(buf - 1) as usize].2);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::buffer_page_is_new::set(|buf| {
        let addr = with_fake(|f| f.bufs[(buf - 1) as usize].2);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.is_new()
    });
    bufmgr_seams::buffer_page_get_lsn::set(|buf| {
        let addr = with_fake(|f| f.bufs[(buf - 1) as usize].2);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::buffer_page_set_lsn::set(|buf, lsn| {
        let addr = with_fake(|f| f.bufs[(buf - 1) as usize].2);
        // SAFETY: leaked test page; replay is single-threaded.
        let mut pm = unsafe { PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
        pm.set_lsn(lsn);
    });
    bufmgr_seams::flush_one_buffer::set(|_| Ok(()));
    bufmgr_seams::overwrite_buffer_page::set(|buf, page| {
        let addr = with_fake(|f| f.bufs[(buf - 1) as usize].2);
        // SAFETY: leaked test page; replay is single-threaded.
        unsafe { core::ptr::copy_nonoverlapping(page.as_ptr(), addr as *mut u8, BLCKSZ) };
    });
    bufmgr_seams::read_recent_buffer::set(|_, _, _, _| Ok(false));
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, fork| {
        Ok(with_fake(|f| nblocks(f, fork)))
    });
    bufmgr_seams::relation_smgr_locator::set(|_rel| types_storage::RelFileLocatorBackend {
        locator: RLOC,
        backend: INVALID_PROC_NUMBER,
    });
    bufmgr_seams::extend_buffered_rel_by::set(|_rel, fork, _strategy, flags, extend_by| {
        assert_eq!(extend_by, 1);
        assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
        Ok(with_fake(|f| {
            let blkno = nblocks(f, fork);
            let buf = create_page(f, fork, blkno);
            f.pins[(buf - 1) as usize] = 1;
            f.locks[(buf - 1) as usize] = 1;
            (buf, 1)
        }))
    });
    bufmgr_seams::extend_buffered_rel_to::set(|_smgr, fork, _strat, _flags, extend_to, mode| {
        with_fake(|f| {
            while nblocks(f, fork) < extend_to {
                let blkno = nblocks(f, fork);
                create_page(f, fork, blkno);
            }
            let buf = find_buf(f, fork, extend_to - 1);
            f.pins[(buf - 1) as usize] += 1;
            if zero_lock(mode) {
                f.locks[(buf - 1) as usize] += 1;
            }
            Ok(buf)
        })
    });

    bufmgr_seams::extend_buffered_rel_to_rel::set(|_rel, fork, strat, flags, extend_to, mode| {
        bufmgr_seams::extend_buffered_rel_to::call(
            types_storage::RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER },
            fork,
            strat,
            flags,
            extend_to,
            mode,
        )
    });

    smgr_seams::smgr_create::set(|_, _, _| Ok(()));
    smgr_seams::smgr_exists::set(|_loc, fork| with_fake(|f| Ok(nblocks(f, fork) > 0)));
    smgr_seams::smgr_nblocks::set(|_loc, fork| with_fake(|f| Ok(nblocks(f, fork))));
    smgr_seams::smgr_cached_nblocks::set(|_loc, fork| with_fake(|f| nblocks(f, fork)));
    smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));

    combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
    combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
    combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::register_predicate_locking_xid::set(|_| Ok(()));
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    catalog_seams::is_catalog_relation::set(|_rel| false);
    origin_seams::replorigin_session_origin::set(|| 0);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    xlogrecovery_seams::reached_consistency::set(|| false);

    xloginsert_seams::xlog_reset_insertion::set(xloginsert::XLogResetInsertion);
    xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_with_flags::set(|rmid, info, flags, fragments| {
        xloginsert::insert_record(rmid, info, flags, fragments, &[])
    });
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let (fork, block, addr) = with_fake(|f| f.bufs[(b.buffer - 1) as usize]);
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: RLOC,
                forknum: fork,
                block,
                // SAFETY: leaked test page, BLCKSZ, pinned by the caller.
                page: unsafe { core::slice::from_raw_parts(addr as *const u8, BLCKSZ) },
                flags: b.flags,
                bufdata: b.bufdata,
            });
        }
        xloginsert::insert_record(rmid, info, flags, main_data, &blocks)
    });
}

fn install_proc_boot_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(781);
    g::SetMyDatabaseId(5);
    g::SetNBuffers(128);
    g::set_transaction_buffers(64);
    g::set_subtransaction_buffers(64);

    pg_sema_seams::pg_semaphore_create::set(|_| {});
    pg_sema_seams::pg_semaphore_reset::set(|_| {});
    pg_sema_seams::pg_semaphore_lock::set(|_| {});
    pg_sema_seams::pg_semaphore_unlock::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    s_lock_seams::set_spins_per_delay::set(|_| {});
    s_lock_seams::update_spins_per_delay::set(|v| v);
    latch_seams::own_latch::set(|_| {});
    latch_seams::disown_latch::set(|_| {});
    latch_seams::set_latch::set(|_| {});
    latch_seams::set_latch_my_latch::set(|| {});
    latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
    latch_seams::reset_latch_my_latch::set(|| {});
    miscinit_seams::switch_to_shared_latch::set(|| {});
    miscinit_seams::switch_back_to_local_latch::set(|| {});
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
    condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
    autovacuum_seams::wake_autovacuum_launcher::set(|| {});
    lock_seams::abort_strong_lock_acquire::set(|| {});
    lock_seams::get_awaited_lock_hashcode::set(|| None);
    lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
    lock_seams::lock_release::set(|_, _, _| Ok(true));
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });
    timeout_seams::disable_timeouts::set(|_| {});
    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_begin_sub_xact::set(|| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_end_sub_xact::set(|_| Ok(()));
    sync_seams::register_sync_request::set(|_, _, _| Ok(true));
    parallel_seams::is_parallel_worker::set(|| false);
    parallel_seams::at_eoxact_parallel::set(|_| Ok(()));
    parallel_seams::at_eosubxact_parallel::set(|_, _| Ok(()));
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
    backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
    miscinit_seams::get_user_id::set(|| 10);
    aio_seams::pgaio_error_cleanup::set(|| {});
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
    spi_seams::at_eoxact_spi::set(|_| Ok(()));
    be_fsstubs_seams::at_eoxact_large_object::set(|_| Ok(()));
    namespace_seams::at_eoxact_namespace::set(|_, _| {});
    catalog_index_seams::reset_reindex_state::set(|_| {});
    catalog_storage_seams::smgr_get_pending_deletes::set(|mcx, _for_commit| {
        Ok(PgVec::new_in(mcx))
    });
    catalog_storage_seams::smgr_do_pending_deletes::set(|_| Ok(()));
    catalog_storage_seams::smgr_do_pending_syncs::set(|_, _| Ok(()));
    combocid_seams::at_eoxact_combocid::set(|| {});
    multixact_seams::at_eoxact_multixact::set(|| {});
    pg_enum_seams::at_eoxact_enum::set(|| {});
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    relcache_seams::relation_cache_init_file_remove::set(|| {});
    typcache_seams::at_eoxact_type_cache::set(|| {});
    logical_seams::reset_logical_streaming_state::set(|| {});
    logical_worker_seams::at_eoxact_logical_rep_workers::set(|_| {});
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(|| {});
    origin_seams::replorigin_session_origin_lsn::set(|| 0);
    origin_seams::replorigin_session_origin_timestamp::set(|| 0);
    origin_seams::set_replorigin_session_origin_timestamp::set(|_| {});
    commit_ts_seams::transaction_tree_set_commit_ts_data::set(|_, _, _, _| Ok(()));
    commit_ts_seams::extend_commit_ts::set(|_| Ok(()));
    syncrep_seams::sync_rep_wait_for_lsn::set(|_, _| Ok(()));
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
}

fn install_real() {
    shmem::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    transam_xlog::init_seams();
    heapam_visibility::init_seams();
    pruneheap::init_seams();
    freespace::init_seams();
    clog::init_seams();
    subtrans::init_seams();
    transam::init_seams();
    varsup::init_seams();
    xact::init_seams();
    walsender_config::init_seams();
    twophase_config::init_seams();
    guc_tables::vars::max_locks_per_xact.install(guc_tables::GucVarAccessors {
        get: || 64,
        set: |_| {},
    });
    guc_tables::vars::WalWriterFlushAfter.install(guc_tables::GucVarAccessors {
        get: || 128,
        set: |_| {},
    });
    snapmgr::init_seams();
    procarray::init_seams();
    inval::init_seams();
    pgstat::init_seams();
    relpath::init_seams();
    xlogreader::init_seams();
    xlogutils::init_seams();
    guc::store::initialize_guc_options().unwrap();

    fd::init_seams();
    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    varsup::VarsupShmemInit();
    procarray::ProcArrayShmemInit();
    clog::CLOGShmemInit().unwrap();
    subtrans::SUBTRANSShmemInit().unwrap();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    if resowner::CurrentResourceOwner().is_null() {
        let owner = resowner::ResourceOwnerCreate(
            types_resowner::ResourceOwner::NULL,
            "prune-redo-roundtrip",
        )
        .unwrap();
        resowner::SetCurrentResourceOwner(owner);
    }
}

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<types_tuple::TupleDescData<'mcx>> {
    let att = types_tuple::FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: types_tuple::TYPALIGN_INT,
        attstorage: types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(types_tuple::CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(types_tuple::TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn test_relation<'mcx>(mcx: Mcx<'mcx>) -> RelationData<'mcx> {
    let mut relname = types_tuple::NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: tableam_vocab::HEAP_TABLE_AM_OID,
        relfilenode: REL_OID,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
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
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: REL_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_hastriggers: false, rd_hasrules: false,
        rd_trigdesc: Default::default(),
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: REL_OID, dbId: 5 } },
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
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
    }
}

fn write_control_file(dir: &std::path::Path) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = SEG as u64 + 40;
    cf.checkPointCopy.redo = SEG as u64 + 40;
    cf.checkPointCopy.ThisTimeLineID = 1;
    cf.checkPointCopy.PrevTimeLineID = 1;
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 6);
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();
}

struct SegFileRead {
    wal_dir: std::path::PathBuf,
}

impl XLogSegmentRoutine for SegFileRead {
    fn segment_open(
        &mut self,
        _v: &mut ReaderView,
        _segno: XLogSegNo,
        _tli: &mut TimeLineID,
    ) -> PgResult<()> {
        unreachable!()
    }
    fn segment_close(&mut self, _v: &mut ReaderView) {}
}

impl XLogReaderRoutine for SegFileRead {
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        _req_len: i32,
        _target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        let segno = target_page_ptr / SEG as u64;
        let off = (target_page_ptr % SEG as u64) as usize;
        let name = transam_xlog::XLogFileName(1, segno, SEG);
        let bytes = std::fs::read(self.wal_dir.join(name)).expect("segment readable");
        cur_page[..BLCKSZ].copy_from_slice(&bytes[off..off + BLCKSZ]);
        v.seg.ws_tli = 1;
        Ok(BLCKSZ as i32)
    }
}

fn heap_addr(blk: BlockNumber) -> usize {
    with_fake(|f| {
        let buf = find_buf(f, ForkNumber::MAIN_FORKNUM, blk);
        f.bufs[(buf - 1) as usize].2
    })
}

fn page_bytes(fork: ForkNumber, blk: BlockNumber) -> [u8; BLCKSZ] {
    let addr = with_fake(|f| {
        let buf = find_buf(f, fork, blk);
        f.bufs[(buf - 1) as usize].2
    });
    // SAFETY: leaked test page, always live.
    unsafe { *(addr as *const [u8; BLCKSZ]) }
}

fn page_mut(blk: BlockNumber) -> PageMut<'static> {
    // SAFETY: leaked test page; single-threaded test.
    unsafe { PageMut::from_raw(NonNull::new(heap_addr(blk) as *mut u8).unwrap()) }
}

fn page_ref(blk: BlockNumber) -> PageRef<'static> {
    // SAFETY: leaked test page, always live.
    unsafe { PageRef::from_raw(NonNull::new(heap_addr(blk) as *mut u8).unwrap()) }
}

fn tuple_hdr(blk: BlockNumber, off: u16) -> &'static mut HeapTupleHeaderData {
    let page = page_ref(blk);
    let id = page.item_id(off);
    let (ptr, _len) = page.item_raw(id);
    // SAFETY: in-page normal item; the test is single-threaded.
    unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) }
}

// hoff 24, natts 1, data bytes follow; total = 24 + data.len().
fn raw_tuple(xmin: u32, data: &[u8]) -> Vec<u8> {
    let mut img = vec![0u8; 24 + data.len()];
    img[0..4].copy_from_slice(&xmin.to_ne_bytes());
    img[18..20].copy_from_slice(&1u16.to_ne_bytes());
    img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
    img[22] = 24;
    img[24..].copy_from_slice(data);
    img
}

fn make_writable_tuple(img: &[u8]) -> HeapTupleData<'static> {
    let words = img.len().div_ceil(8);
    // Leaked (test-only): moving a Box would invalidate the derived pointer.
    let buf: &'static mut [u64] = Box::leak(vec![0u64; words].into_boxed_slice());
    // SAFETY: buf is words*8 >= img.len() writable bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(img.as_ptr(), buf.as_mut_ptr().cast::<u8>(), img.len())
    };
    // SAFETY: 8-aligned leaked image, header-complete, unique.
    unsafe {
        HeapTupleData::from_raw_parts(
            buf.as_mut_ptr().cast::<u8>(),
            img.len() as u32,
            ItemPointerData::invalid(),
            0,
        )
    }
}

fn mvcc_snapshot<'m>(mcx: Mcx<'m>) -> SnapshotData<'m> {
    let s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    s.regd_count.set(1);
    s
}

// cmin/cmax are not WAL-logged (replay stamps FirstCommandId), pd_prune_xid
// and HTSV hint bits are writer-side hints C redo leaves stale; normalize
// them out of the comparison on both sides.
fn normalize(page: &mut [u8; BLCKSZ]) {
    const HINT_BITS: u16 = 0x0F00; // HEAP_XMIN/XMAX_{COMMITTED,INVALID}
    page[10] &= !0x02; // PD_PAGE_FULL
    page[20..24].fill(0); // pd_prune_xid
    // The hole is dead space: the writer leaves stale bytes where
    // compactified tuples used to live, FPIs elide it, redo zero-fills.
    let lower = u16::from_ne_bytes(page[12..14].try_into().unwrap()) as usize;
    let upper = u16::from_ne_bytes(page[14..16].try_into().unwrap()) as usize;
    page[lower..upper].fill(0);
    let r =
        // SAFETY: BLCKSZ page copy owned by the caller.
        unsafe { PageRef::from_raw(NonNull::new(page.as_mut_ptr()).unwrap()) };
    for off in 1..=r.max_offset_number() {
        let lp = r.item_id(off);
        if !lp.is_normal() {
            continue;
        }
        let o = lp.lp_off() as usize;
        page[o + 8..o + 12].fill(0); // t_field3 (cid/xvac union)
        let im = u16::from_ne_bytes(page[o + 20..o + 22].try_into().unwrap());
        page[o + 20..o + 22].copy_from_slice(&(im & !HINT_BITS).to_ne_bytes());
    }
}

#[test]
fn prune_freeze_visible_redo_rebuilds_pages_byte_exact() {
    let dir =
        std::env::temp_dir().join(format!("pgrust_prune_redo_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal", "pg_xact", "pg_subtrans"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    install_proc_boot_seams();
    install_real();
    install_fake_bufmgr();

    write_control_file(&dir);
    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();
    clog::BootStrapCLOG().unwrap();
    subtrans::BootStrapSUBTRANS().unwrap();
    {
        use std::sync::atomic::Ordering::Relaxed as R;
        let tv = procarray::TransamVariables();
        tv.nextXid
            .store(types_core::FullTransactionId::from_epoch_and_xid(0, COMMITTED_XID).value, R);
        tv.latestCompletedXid
            .store(types_core::FullTransactionId::from_epoch_and_xid(0, COMMITTED_XID - 1).value, R);
    }
    subtrans::StartupSUBTRANS(COMMITTED_XID).unwrap();

    let end_of_log: XLogRecPtr = 2 * SEG as u64;
    let prev_rec: XLogRecPtr = SEG as u64 + 40;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(prev_rec), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    xlogutils::set_in_recovery(false);
    assert!(transam_xlog::XLogInsertAllowed());

    let ctx = MemoryContext::new("prune_redo_roundtrip");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx);

    // Page 0 (on-access prune target): 3 committed wide inserts + 1 aborted,
    // a HOT update of (0,2) -> (0,5), and a delete of (0,3): a redirect and
    // dead + unused line pointers in one PRUNE_ON_ACCESS record.
    xact::StartTransactionCommand().unwrap();
    for v in 1u8..=3 {
        let img = raw_tuple(0, &vec![v; WIDE - 24]);
        let mut tup = make_writable_tuple(&img);
        heap_insert(&rel, &mut tup, 7, 0, None).unwrap();
        assert_eq!(tup.t_self, ItemPointerData::new(0, v as u16));
    }
    assert_eq!(xact::GetTopTransactionIdIfAny(), COMMITTED_XID);
    xact::CommitTransactionCommand().unwrap();

    xact::StartTransactionCommand().unwrap();
    {
        let img = raw_tuple(0, &vec![4u8; WIDE - 24]);
        let mut tup = make_writable_tuple(&img);
        heap_insert(&rel, &mut tup, 7, 0, None).unwrap();
        assert_eq!(tup.t_self, ItemPointerData::new(0, 4));
    }
    assert_eq!(xact::GetTopTransactionIdIfAny(), ABORTED_XID);
    xact::AbortCurrentTransaction().unwrap();

    xact::StartTransactionCommand().unwrap();
    let mut tmfd = TM_FailureData::default();
    {
        let img = raw_tuple(0, &vec![0x22; WIDE - 24]);
        let mut newtup = make_writable_tuple(&img);
        let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;
        let mut update_indexes = TU_UpdateIndexes::TU_None;
        let r = heap_update(
            &rel,
            &ItemPointerData::new(0, 2),
            &mut newtup,
            8,
            None,
            true,
            &mut tmfd,
            &mut lockmode,
            &mut update_indexes,
        )
        .unwrap();
        assert_eq!(r, TM_Result::TM_Ok);
        assert_eq!(newtup.t_self, ItemPointerData::new(0, 5));
        assert_eq!(update_indexes, TU_UpdateIndexes::TU_None); // HOT
    }
    let r =
        heap_delete(&rel, &ItemPointerData::new(0, 3), 9, None, true, &mut tmfd, false).unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    assert_eq!(xact::GetTopTransactionIdIfAny(), UPDATER_XID);
    xact::CommitTransactionCommand().unwrap();

    // Page 1 (vacuum-scan, no-index lane): 5 wide inserts fill it; deletes at
    // (1,2) and the trailing (1,5). Page 2 (vacuum-scan-with-indexes +
    // cleanup + freeze shapes): 4 wide inserts, the trailing (2,4) deleted.
    xact::StartTransactionCommand().unwrap();
    for v in 5u8..=13 {
        let img = raw_tuple(0, &vec![v; WIDE - 24]);
        let mut tup = make_writable_tuple(&img);
        heap_insert(&rel, &mut tup, 7, 0, None).unwrap();
        let want = if v <= 9 {
            ItemPointerData::new(1, (v - 4) as u16)
        } else {
            ItemPointerData::new(2, (v - 9) as u16)
        };
        assert_eq!(tup.t_self, want);
    }
    xact::CommitTransactionCommand().unwrap();

    xact::StartTransactionCommand().unwrap();
    for (blk, off) in [(1u32, 2u16), (1, 5), (2, 4)] {
        let r = heap_delete(&rel, &ItemPointerData::new(blk, off), 10, None, true, &mut tmfd, false)
            .unwrap();
        assert_eq!(r, TM_Result::TM_Ok);
    }
    xact::CommitTransactionCommand().unwrap();

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });

    // Advance the horizon through the real procarray: a fresh snapshot moves
    // RecentXmin past every completed xid above.
    {
        let mut snap = mvcc_snapshot(mcx);
        procarray::GetSnapshotData(&mut snap, mcx).unwrap();
    }
    assert!(types_core::xact::TransactionIdPrecedes(LAST_XID, procarray::RecentXmin()));

    let vistest = procarray_seams::global_vis_test_for::call(&rel);

    // PRUNE_ON_ACCESS through the real guard chain.
    let buf0 = bufmgr_seams::read_buffer::call(&rel, 0).unwrap();
    page_mut(0).set_full();
    pruneheap::heap_page_prune_opt(&rel, buf0).unwrap();
    {
        let p = page_ref(0);
        assert_eq!(p.item_id(2).lp_flags(), LP_REDIRECT);
        assert_eq!(p.item_id(2).lp_off(), 5);
        assert_eq!(p.item_id(3).lp_flags(), LP_DEAD);
        assert_eq!(p.item_id(4).lp_flags(), LP_DEAD); // aborted insert
        assert_eq!(p.item_id(5).lp_flags(), LP_NORMAL);
    }

    // PRUNE_VACUUM_SCAN, no-index lane (vacuumlazy's MARK_UNUSED_NOW call).
    let buf1 = bufmgr_seams::read_buffer::call(&rel, 1).unwrap();
    bufmgr_seams::lock_buffer::call(buf1, bufmgr_seams::BUFFER_LOCK_EXCLUSIVE).unwrap();
    let mut presult = PruneFreezeResult::default();
    let mut off_loc = InvalidOffsetNumber;
    heap_page_prune_and_freeze(
        &rel,
        buf1,
        vistest,
        HEAP_PAGE_PRUNE_MARK_UNUSED_NOW,
        None,
        &mut presult,
        PruneReason::PruneVacuumScan,
        &mut off_loc,
        None,
        None,
    )
    .unwrap();
    bufmgr_seams::lock_buffer::call(buf1, bufmgr_seams::BUFFER_LOCK_UNLOCK).unwrap();
    {
        let p = page_ref(1);
        assert_eq!(p.max_offset_number(), 4, "trailing unused truncated");
        assert_eq!(p.item_id(2).lp_flags(), LP_UNUSED);
    }

    // PRUNE_VACUUM_SCAN leaving LP_DEAD (the with-indexes lane), then the
    // lazy_vacuum_heap_page cleanup shape: LP_DEAD -> LP_UNUSED, line-pointer
    // truncation, PRUNE_VACUUM_CLEANUP WAL without a cleanup lock.
    let buf2 = bufmgr_seams::read_buffer::call(&rel, 2).unwrap();
    bufmgr_seams::lock_buffer::call(buf2, bufmgr_seams::BUFFER_LOCK_EXCLUSIVE).unwrap();
    let mut presult = PruneFreezeResult::default();
    heap_page_prune_and_freeze(
        &rel,
        buf2,
        vistest,
        0,
        None,
        &mut presult,
        PruneReason::PruneVacuumScan,
        &mut off_loc,
        None,
        None,
    )
    .unwrap();
    assert_eq!(page_ref(2).item_id(4).lp_flags(), LP_DEAD);
    {
        let mut pm = page_mut(2);
        let mut lp = pm.as_ref().item_id(4);
        lp.set_unused();
        pm.set_item_id(4, lp);
        pm.truncate_line_pointer_array();
    }
    bufmgr_seams::mark_buffer_dirty::call(buf2).unwrap();
    log_heap_prune_and_freeze(
        &rel,
        buf2,
        types_core::InvalidTransactionId,
        false,
        PruneReason::PruneVacuumCleanup,
        &mut [],
        &[],
        &[],
        &[4],
    )
    .unwrap();
    assert_eq!(page_ref(2).max_offset_number(), 3);

    // Freeze plans, hand-encoded per heapam_xlog.h (the emit side is covered
    // by pruneheap's own tests), applied to (2,1) and (2,2).
    let (frz_im, frz_im2) = {
        let htup = tuple_hdr(2, 1);
        let im = (htup.t_infomask & !(HEAP_XMAX_BITS | HEAP_MOVED))
            | HEAP_XMIN_FROZEN
            | HEAP_XMAX_INVALID;
        (im, htup.t_infomask2)
    };
    for off in [1u16, 2] {
        let htup = tuple_hdr(2, off);
        htup.set_xmax(types_core::InvalidTransactionId);
        htup.t_infomask = frz_im;
        htup.t_infomask2 = frz_im2;
    }
    {
        let mut main = [0u8; 6];
        main[0] = PruneReason::PruneVacuumScan as u8;
        main[1] = XLHP_CLEANUP_LOCK | XLHP_HAS_CONFLICT_HORIZON | XLHP_HAS_FREEZE_PLANS;
        main[2..6].copy_from_slice(&UPDATER_XID.to_ne_bytes());

        // xlhp_freeze_plans { uint16 nplans; [2 pad]; plans[] }; plan =
        // { xmax u32; t_infomask2 u16; t_infomask u16; frzflags u8; [pad];
        // ntuples u16 }; frz_offsets trail the block data.
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_ne_bytes());
        data.extend_from_slice(&[0u8; 2]);
        data.extend_from_slice(&0u32.to_ne_bytes());
        data.extend_from_slice(&frz_im2.to_ne_bytes());
        data.extend_from_slice(&frz_im.to_ne_bytes());
        data.push(0);
        data.push(0);
        data.extend_from_slice(&2u16.to_ne_bytes());
        data.extend_from_slice(&1u16.to_ne_bytes());
        data.extend_from_slice(&2u16.to_ne_bytes());

        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_HEAP2_ID,
            XLOG_HEAP2_PRUNE_VACUUM_SCAN,
            0,
            &[&main],
            &[XLogRegBuf { block_id: 0, buffer: buf2, flags: REGBUF_STANDARD, bufdata: &[&data] }],
        )
        .unwrap();
        page_mut(2).set_lsn(recptr);
    }
    bufmgr_seams::lock_buffer::call(buf2, bufmgr_seams::BUFFER_LOCK_UNLOCK).unwrap();

    // VISIBLE x2 through the real write side: the first record carries the
    // VM page's FPI, the second replays through the live visibilitymap_set
    // arm.
    with_fake(|f| {
        let buf = create_page(f, ForkNumber::VISIBILITYMAP_FORKNUM, 0);
        f.pins[(buf - 1) as usize] += 1;
        // SAFETY: fresh leaked page.
        unsafe {
            PageMut::from_raw(NonNull::new(f.bufs[(buf - 1) as usize].2 as *mut u8).unwrap())
        }
        .init(0);
        f.pins[(buf - 1) as usize] -= 1;
    });
    let mut vmbuf = visibilitymap::VmBuffer::new();
    visibilitymap::visibilitymap_pin(&rel, 0, &mut vmbuf).unwrap();
    page_mut(0).set_all_visible();
    visibilitymap::visibilitymap_set(
        &rel,
        0,
        buf0,
        0,
        &vmbuf,
        UPDATER_XID,
        visibilitymap::VISIBILITYMAP_ALL_VISIBLE,
    )
    .unwrap();
    page_mut(2).set_all_visible();
    visibilitymap::visibilitymap_set(
        &rel,
        2,
        buf2,
        0,
        &vmbuf,
        types_core::InvalidTransactionId,
        visibilitymap::VISIBILITYMAP_ALL_VISIBLE,
    )
    .unwrap();
    vmbuf.release();
    bufmgr_seams::release_buffer::call(buf0).unwrap();
    bufmgr_seams::release_buffer::call(buf1).unwrap();
    bufmgr_seams::release_buffer::call(buf2).unwrap();

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });

    let expected: Vec<[u8; BLCKSZ]> =
        (0..3).map(|b| page_bytes(ForkNumber::MAIN_FORKNUM, b)).collect();
    let expected_vm = page_bytes(ForkNumber::VISIBILITYMAP_FORKNUM, 0);
    let last_lsn = {
        let vm =
            // SAFETY: stack copy of a page image.
            unsafe { PageRef::from_raw(NonNull::new(expected_vm.as_ptr().cast_mut()).unwrap()) };
        vm.lsn()
    };
    assert_ne!(last_lsn, 0);
    transam_xlog::XLogFlush(last_lsn).unwrap();

    // Wipe the storage: replay must rebuild every block purely from WAL.
    with_fake(|f| {
        f.bufs.clear();
        f.pins.clear();
        f.locks.clear();
    });
    xlogutils::set_in_recovery(true);

    let reader_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead { wal_dir: dir.join("pg_wal") };
    reader.XLogBeginRead(end_of_log + 40);

    let mut heap2_seen = [0u32; 8];
    while reader.v.EndRecPtr < last_lsn {
        reader.XLogReadRecord(&mut routine).unwrap().unwrap();
        let rmid = reader.XLogRecGetRmid();
        if rmid == RM_HEAP2_ID {
            let op = ((reader.XLogRecGetInfo() & XLOG_HEAP_OPMASK) >> 4) as usize;
            heap2_seen[op] += 1;
        }
        // The stream also carries xact commit/abort + clog periphery; replay
        // it through the same rmgr dispatch.
        (rmgr::GetRmgr(rmid).unwrap().rm_redo)(&mut reader.v).unwrap();
    }
    assert_eq!(reader.v.EndRecPtr, last_lsn);

    assert_eq!(heap2_seen[(XLOG_HEAP2_PRUNE_ON_ACCESS >> 4) as usize], 1, "PRUNE_ON_ACCESS");
    assert_eq!(heap2_seen[(XLOG_HEAP2_PRUNE_VACUUM_SCAN >> 4) as usize], 3, "VACUUM_SCAN x3 (mark-unused-now, lpdead, freeze)");
    assert_eq!(heap2_seen[(XLOG_HEAP2_PRUNE_VACUUM_CLEANUP >> 4) as usize], 1, "VACUUM_CLEANUP");
    assert_eq!(heap2_seen[(XLOG_HEAP2_VISIBLE >> 4) as usize], 2, "VISIBLE x2");
    assert_eq!(heap2_seen[(XLOG_HEAP2_MULTI_INSERT >> 4) as usize], 0);

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "replay leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "replay leaked locks: {:?}", f.locks);
    });

    // The freeze plan and truncation replayed exactly (asserted before the
    // hint-bit normalization masks the frozen infomask).
    {
        let p = page_ref(2);
        assert_eq!(p.max_offset_number(), 3);
        for off in [1u16, 2] {
            let htup = tuple_hdr(2, off);
            assert_eq!(htup.t_infomask, frz_im, "frozen infomask at (2,{off})");
            assert_eq!(htup.t_infomask2, frz_im2);
        }
        assert_eq!(page_ref(1).max_offset_number(), 4);
        assert_eq!(page_ref(0).item_id(2).lp_flags(), LP_REDIRECT);
    }

    for blk in 0..3u32 {
        let mut got = page_bytes(ForkNumber::MAIN_FORKNUM, blk);
        let mut want = expected[blk as usize];
        normalize(&mut got);
        normalize(&mut want);
        if got != want {
            let first = got.iter().zip(want.iter()).position(|(a, b)| a != b).unwrap();
            panic!(
                "replayed block {blk} differs at byte {first}: got {:02x?} want {:02x?}",
                &got[first..(first + 16).min(BLCKSZ)],
                &want[first..(first + 16).min(BLCKSZ)]
            );
        }
    }
    let got_vm = page_bytes(ForkNumber::VISIBILITYMAP_FORKNUM, 0);
    assert_eq!(got_vm, expected_vm, "VM page bytes differ after replay");

    let _ = std::fs::remove_dir_all(&dir);
}
