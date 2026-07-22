// Start/Commit/Prepare/Abort/Cleanup (top-level and sub), the durable
// Record* routines, the command/block state machines, and the
// parallel-worker serialize/restore path. Call order within each function is
// conformance-critical and mirrors the C statement-for-statement.

use crate::*;
use std::sync::atomic::Ordering::Relaxed;
use types_core::VirtualTransactionId;
use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;
use types_storage::DELAY_CHKPT_START;

fn RecordTransactionCommit(xp: XsPtr) -> PgResult<TransactionId> {
    thread_local! {
        // Const-init + !needs_drop payload: plain TLS access, no lazy-init or
        // dtor-registration branch (lock's LocalState shape); leaks at thread
        // exit like C's TopMemoryContext.
        static RECORD_COMMIT_SCRATCH: core::cell::UnsafeCell<
            Option<core::mem::ManuallyDrop<MemoryContext>>,
        > = const { core::cell::UnsafeCell::new(None) };
    }
    RECORD_COMMIT_SCRATCH.with(|cell| {
        // SAFETY: single-threaded backend TLS; the &mut is confined to this
        // call, which cannot recurse (commit records nothing that commits).
        let scratch = unsafe { &mut *cell.get() }.get_or_insert_with(|| {
            // Session-memory teardown (FPBUDGET-1): freed at clean task end.
            ::mcx::register_session_cleanup(Box::new(|| {
                RECORD_COMMIT_SCRATCH.with(|c| {
                    // SAFETY: task-end teardown; no commit is in flight.
                    if let Some(ctx) = unsafe { &mut *c.get() }.take() {
                        drop(core::mem::ManuallyDrop::into_inner(ctx));
                    }
                });
            }));
            core::mem::ManuallyDrop::new(MemoryContext::new("RecordTransactionCommit"))
        });
        let out = RecordTransactionCommitGuts(xp, scratch.mcx());
        // The empty commit allocates nothing (C has no scratch on this path
        // at all): reset only if something was charged since the last reset.
        if scratch.peak() != 0 {
            scratch.reset();
        }
        out
    })
}

fn RecordTransactionCommitGuts(xp: XsPtr, mcx: mcx::Mcx<'_>) -> PgResult<TransactionId> {
    let xid = GetTopTransactionIdIfAny();
    let mark_xid_committed = xid != InvalidTransactionId;
    #[allow(unused_assignments)]
    let mut latest_xid = InvalidTransactionId;

    if xlog_seams::xlog_logical_info_active::call() {
        inval::eoxact::LogLogicalInvalidations()?;
    }

    // storage.c's pending lists have no writer while RelationCreateStorage/
    // DropStorage are unported; guarded like the other provably-empty registries.
    let rels = if catalog_storage_seams::smgr_get_pending_deletes::is_installed() {
        catalog_storage_seams::smgr_get_pending_deletes::call(mcx, true)?
    } else {
        mcx::PgVec::new_in(mcx)
    };
    let children = committed_children_in(xp)?;
    let dropped_stats = pgstat::xact::pgstat_get_transactional_drops(mcx, true)?;
    let (inval_msgs, relcache_init_file_inval) = if xlog_seams::xlog_standby_info_active::call() {
        inval::eoxact::xactGetCommittedInvalidationMessages(mcx)?
    } else {
        (mcx::PgVec::new_in(mcx), false)
    };
    let mut wrote_xlog = xlog_seams::xact_last_rec_end::call() != 0;

    if !mark_xid_committed {
        if !rels.is_empty() || !dropped_stats.is_empty() {
            return Err(Box::new(PgError::error(
                "cannot commit a transaction that deleted files but has no xid",
            )));
        }
        debug_assert!(children.is_empty());

        if !inval_msgs.is_empty() {
            standby_seams::log_standby_invalidations::call(
                &inval_msgs,
                relcache_init_file_inval,
            )?;
            wrote_xlog = true; // not strictly necessary
        }

        if !wrote_xlog {
            return Ok(InvalidTransactionId); // goto cleanup
        }
    } else {
        // Uninstalled origin seams = C defaults (origin.c globals); origin.c
        // is unported (wal.rs/xloginsert precedent).
        let session_origin = crate::wal::session_origin_or_default();
        let replorigin = session_origin != types_core::InvalidRepOriginId
            && session_origin != DoNotReplicateId;

        // Commit critical section: force any concurrent checkpoint to wait
        // until we've updated pg_xact.
        let proc = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("MyProc is not set"));
        debug_assert_eq!(proc.delayChkptFlags.load(Relaxed) & DELAY_CHKPT_START, 0);
        init_small::globals::StartCriticalSection();
        proc.delayChkptFlags.fetch_or(DELAY_CHKPT_START, Relaxed);

        let commit_time = GetCurrentTransactionStopTimestamp();
        crate::wal::XactLogCommitRecord(
            commit_time,
            &children,
            &rels,
            &dropped_stats,
            &inval_msgs,
            relcache_init_file_inval,
            MyXactFlags(),
            InvalidTransactionId,
            None, // plain commit
        )?;

        if replorigin {
            origin_seams::replorigin_session_advance::call(
                origin_seams::replorigin_session_origin_lsn::call(),
                xlog_seams::xact_last_rec_end::call(),
            )?;
        }

        let origin_timestamp = if origin_seams::replorigin_session_origin_timestamp::is_installed()
        {
            if !replorigin || origin_seams::replorigin_session_origin_timestamp::call() == 0 {
                origin_seams::set_replorigin_session_origin_timestamp::call(
                    GetCurrentTransactionStopTimestamp(),
                );
            }
            origin_seams::replorigin_session_origin_timestamp::call()
        } else {
            GetCurrentTransactionStopTimestamp()
        };

        if commit_ts_seams::transaction_tree_set_commit_ts_data::is_installed() {
            commit_ts_seams::transaction_tree_set_commit_ts_data::call(
                xid,
                &children,
                origin_timestamp,
                session_origin,
            )?;
        }
    }

    if (wrote_xlog && mark_xid_committed && synchronous_commit() > SYNCHRONOUS_COMMIT_OFF)
        || xp.with(|s| s.force_sync_commit)
        || !rels.is_empty()
    {
        xlog_seams::xlog_flush::call(xlog_seams::xact_last_rec_end::call())?;
        if mark_xid_committed {
            transam_seams::transaction_id_commit_tree::call(xid, &children)?;
        }
    } else {
        xlog_seams::xlog_set_async_xact_lsn::call(xlog_seams::xact_last_rec_end::call());
        if mark_xid_committed {
            transam_seams::transaction_id_async_commit_tree::call(
                xid,
                &children,
                xlog_seams::xact_last_rec_end::call(),
            )?;
        }
    }

    if mark_xid_committed {
        let proc = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("MyProc is not set"));
        proc.delayChkptFlags.fetch_and(!DELAY_CHKPT_START, Relaxed);
        init_small::globals::EndCriticalSection();
    }

    latest_xid = transam_seams::transaction_id_latest::call(xid, &children);

    // C SyncRepWaitForLSN no-ops without sync standbys; syncrep unported.
    if wrote_xlog && mark_xid_committed && syncrep_seams::sync_rep_wait_for_lsn::is_installed() {
        syncrep_seams::sync_rep_wait_for_lsn::call(xlog_seams::xact_last_rec_end::call(), true)?;
    }

    xlog_seams::set_xact_last_commit_end::call(xlog_seams::xact_last_rec_end::call());
    xlog_seams::set_xact_last_rec_end::call(0);

    Ok(latest_xid)
}

fn RecordTransactionAbort(is_subxact: bool) -> PgResult<TransactionId> {
    let xid = GetCurrentTransactionIdIfAny();

    if xid == InvalidTransactionId {
        if !is_subxact {
            xlog_seams::set_xact_last_rec_end::call(0);
        }
        return Ok(InvalidTransactionId);
    }

    if transam_seams::transaction_id_did_commit::call(xid)? {
        return Err(Box::new(PgError::new(
            types_error::PANIC,
            format!("cannot abort transaction {xid}, it was already committed"),
        )));
    }

    let session_origin = crate::wal::session_origin_or_default();
    let replorigin =
        session_origin != types_core::InvalidRepOriginId && session_origin != DoNotReplicateId;

    thread_local! {
        static RECORD_ABORT_SCRATCH: core::cell::RefCell<MemoryContext> =
            core::cell::RefCell::new(MemoryContext::new("RecordTransactionAbort"));
    }
    RECORD_ABORT_SCRATCH.with(|cell| {
        let mut scratch = cell.borrow_mut();
        let out = record_transaction_abort_guts(scratch.mcx(), is_subxact, xid, replorigin);
        scratch.reset();
        out
    })
}

fn record_transaction_abort_guts(
    mcx: mcx::Mcx<'_>,
    is_subxact: bool,
    xid: TransactionId,
    replorigin: bool,
) -> PgResult<TransactionId> {
    let rels = if catalog_storage_seams::smgr_get_pending_deletes::is_installed() {
        catalog_storage_seams::smgr_get_pending_deletes::call(mcx, false)?
    } else {
        mcx::PgVec::new_in(mcx)
    };
    let children = xactGetCommittedChildren()?;
    let dropped_stats = pgstat::xact::pgstat_get_transactional_drops(mcx, false)?;

    init_small::globals::StartCriticalSection();

    let xact_time = if is_subxact {
        timestamp_seams::get_current_timestamp::call()
    } else {
        GetCurrentTransactionStopTimestamp()
    };
    let result: PgResult<()> = (|| {
        crate::wal::XactLogAbortRecord(
            xact_time,
            &children,
            &rels,
            &dropped_stats,
            MyXactFlags(),
            InvalidTransactionId,
            None,
        )?;

        if replorigin {
            origin_seams::replorigin_session_advance::call(
                origin_seams::replorigin_session_origin_lsn::call(),
                xlog_seams::xact_last_rec_end::call(),
            )?;
        }

        if !is_subxact {
            xlog_seams::xlog_set_async_xact_lsn::call(xlog_seams::xact_last_rec_end::call());
        }

        transam_seams::transaction_id_abort_tree::call(xid, &children)?;
        Ok(())
    })();

    init_small::globals::EndCriticalSection();
    result?;

    let latest_xid = transam_seams::transaction_id_latest::call(xid, &children);

    if is_subxact {
        procarray_seams::xid_cache_remove_running_xids::call(xid, &children, latest_xid)?;
    }

    if !is_subxact {
        xlog_seams::set_xact_last_rec_end::call(0);
    }

    Ok(latest_xid)
}

fn StartTransaction(xp: XsPtr) -> PgResult<()> {
    debug_assert!(xp.with(|s| s.stack_len() == 1));
    debug_assert!(!xp.with(|s| s.top_full_xid().is_valid()));
    debug_assert!(xp.with(|s| s.current().state == TRANS_DEFAULT));

    xp.with(|s| {
        s.current_mut().state = TRANS_START;
        s.current_mut().full_transaction_id = InvalidFullTransactionId; // until assigned
    });

    {
        let rate = (guc_tables::vars::log_xact_sample_rate.get().get)();
        let sampled = rate != 0.0
            && (rate == 1.0 || pg_prng::global_prng(pg_prng::PgPrng::next_f64) <= rate);
        // One state borrow for the adjacent field writes (no seam between).
        xp.with(|s| {
            s.xact_is_sampled = sampled;
            let mut n = s.current_mut();
            n.nesting_level = 1;
            n.guc_nest_level = 1;
            n.child_xids = Vec::new();
        });
    }

    let (prev_user, prev_sec_context) = miscinit::GetUserIdAndSecContext();
    debug_assert_eq!(prev_sec_context, 0);
    xp.with(|s| {
        s.current_mut().prev_user = prev_user;
        s.current_mut().prev_sec_context = prev_sec_context;
    });

    let in_recovery = xlog_seams::recovery_in_progress::call();
    xp.with(|s| {
        if in_recovery {
            s.current_mut().started_in_recovery = true;
            s.XactReadOnly = true;
        } else {
            s.current_mut().started_in_recovery = false;
            s.XactReadOnly = s.DefaultXactReadOnly;
        }
        s.XactDeferrable = s.DefaultXactDeferrable;
        s.XactIsoLevel = s.DefaultXactIsoLevel;
        s.force_sync_commit = false;
        s.MyXactFlags = 0;

        s.current_mut().sub_transaction_id = TopSubTransactionId;
        s.current_sub_transaction_id = TopSubTransactionId;
        s.set_command_id(FirstCommandId);
        s.set_command_id_used(false);

        s.unreported_xids.clear();
        s.current_mut().did_log_xid = false;
    });

    AtStart_Memory(xp);
    AtStart_ResourceOwner(xp)?;

    let vxid = VirtualTransactionId {
        procNumber: init_small::globals::MyProcNumber(),
        localTransactionId: sinval::GetNextLocalTransactionId(),
    };
    lock::VirtualXactLockTableInsert(vxid)?;
    {
        let proc = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("MyProc is not set"));
        debug_assert_eq!(proc.vxid.procNumber.load(Relaxed), vxid.procNumber);
        proc.vxid.lxid.store(vxid.localTransactionId, Relaxed);
    }

    // One borrow covers start/stop timestamp bookkeeping and hands the seam
    // its argument (the seam reads nothing from this state).
    let xact_start = if !parallel_seams::is_parallel_worker::call() {
        let ts = if !spi_seams::spi_inside_nonatomic_context::call() {
            None
        } else {
            Some(timestamp_seams::get_current_timestamp::call())
        };
        xp.with(|s| {
            s.xact_start_timestamp = ts.unwrap_or(s.stmt_start_timestamp);
            s.xact_stop_timestamp = 0;
            s.xact_start_timestamp
        })
    } else {
        xp.with(|s| {
            debug_assert!(s.xact_start_timestamp != 0);
            s.xact_stop_timestamp = 0;
            s.xact_start_timestamp
        })
    };
    backend_status_seams::pgstat_report_xact_timestamp::call(xact_start);

    guc::AtStart_GUC();
    AtStart_Cache()?;
    trigger_seams::after_trigger_begin_xact::call()?;

    xp.with(|s| s.current_mut().state = TRANS_INPROGRESS);

    let transaction_timeout = lmgr_proc::globals::TransactionTimeout();
    if transaction_timeout > 0 {
        timeout::enable_timeout_after(
            timeout_seams::TRANSACTION_TIMEOUT,
            transaction_timeout,
        );
    }

    ShowTransactionState("StartTransaction");
    Ok(())
}

fn CommitTransaction(xp: XsPtr) -> PgResult<()> {
    let is_parallel_worker = cur_block_state() == TBLOCK_PARALLEL_INPROGRESS;

    if is_parallel_worker {
        EnterParallelMode();
    }

    ShowTransactionState("CommitTransaction");

    let cur_state = xp.with(|s| s.current().state);
    if cur_state != TRANS_INPROGRESS {
        let st = TransStateAsString(cur_state);
        warn_internal(&format!("CommitTransaction while in {st} state"));
    }
    debug_assert!(!xp.with(|s| s.is_subxact()));

    loop {
        trigger_seams::after_trigger_fire_deferred::call()?;
        if !portalmem::PreCommit_Portals(false)? {
            break;
        }
    }

    CallXactCallbacks(xp, if is_parallel_worker {
        XACT_EVENT_PARALLEL_PRE_COMMIT
    } else {
        XACT_EVENT_PRE_COMMIT
    })?;

    parallel_seams::at_eoxact_parallel::call(true)?;
    let level = xp.with(|s| s.current().parallel_mode_level);
    if is_parallel_worker {
        if level != 1 {
            warn_internal(&format!(
                "parallelModeLevel is {level} not 1 at end of parallel worker transaction"
            ));
        }
    } else if level != 0 {
        warn_internal(&format!(
            "parallelModeLevel is {level} not 0 at end of transaction"
        ));
    }

    trigger_seams::after_trigger_end_xact::call(true)?;

    // No on_commits writer exists while tablecmds is unported; guarded like
    // the other provably-empty registries.
    if tablecmds_seams::pre_commit_on_commit_actions::is_installed() {
        tablecmds_seams::pre_commit_on_commit_actions::call()?;
    }

    // Sync files created but not WAL-logged; must precede
    // AtEOXact_RelationMap to avoid committed-but-broken files.
    if catalog_storage_seams::smgr_do_pending_syncs::is_installed() {
        catalog_storage_seams::smgr_do_pending_syncs::call(true, is_parallel_worker)?;
    }

    // No LO descriptor can exist while lo_open is unported; guarded.
    if be_fsstubs_seams::at_eoxact_large_object::is_installed() {
        be_fsstubs_seams::at_eoxact_large_object::call(true)?;
    }

    // NOTIFY enqueue late (minimize lock hold time; may take a snapshot, so
    // before serializable cleanup).
    // No pending LISTEN/NOTIFY state can exist while async.c is unported; guarded.
    if async_seams::pre_commit_notify::is_installed() {
        async_seams::pre_commit_notify::call()?;
    }

    if !is_parallel_worker {
        predicate_seams::pre_commit_check_for_serialization_failure::call()?;
    }

    init_small::globals::HoldInterrupts();

    relmapper::AtEOXact_RelationMap(true, is_parallel_worker)?;

    xp.with(|s| {
        s.current_mut().state = TRANS_COMMIT;
        s.current_mut().parallel_mode_level = 0;
        s.current_mut().parallel_child_xact = false; // should be false already
    });

    if lmgr_proc::globals::TransactionTimeout() > 0 {
        timeout::disable_timeout(timeout_seams::TRANSACTION_TIMEOUT, false);
    }

    let latest_xid = if !is_parallel_worker {
        RecordTransactionCommit(xp)?
    } else {
        parallel_seams::parallel_worker_report_last_rec_end::call(
            xlog_seams::xact_last_rec_end::call(),
        )?;
        InvalidTransactionId
    };

    // Announce no transaction in progress: _before_ releasing locks and
    // _after_ RecordTransactionCommit.
    procarray_seams::proc_array_end_transaction::call(
        lmgr_proc::MyProc().expect("MyProc is not set"),
        latest_xid,
    )?;

    CallXactCallbacks(xp, if is_parallel_worker {
        XACT_EVENT_PARALLEL_COMMIT
    } else {
        XACT_EVENT_COMMIT
    })?;

    resowner::SetCurrentResourceOwner(types_resowner::ResourceOwner::NULL);
    release_transaction_owner_before_locks(true)?;

    aio_seams::at_eoxact_aio::call(true);

    bufmgr::AtEOXact_Buffers(true);

    relcache_seams::at_eoxact_relation_cache::call(true)?;

    typcache_seams::at_eoxact_type_cache::call();

    // Make catalog changes visible to all backends: after relcache refs are
    // dropped, before locks are released.
    inval::eoxact::AtEOXact_Inval(true)?;

    multixact_seams::at_eoxact_multixact::call();

    release_transaction_owner_locks(true)?;

    // Drop deleted files (after relcache/buffer pins and locks are gone).
    if catalog_storage_seams::smgr_do_pending_deletes::is_installed() {
        catalog_storage_seams::smgr_do_pending_deletes::call(true)?;
    }

    if async_seams::at_commit_notify::is_installed() {
        async_seams::at_commit_notify::call()?;
    }

    guc::AtEOXact_GUC(true, 1);
    spi_seams::at_eoxact_spi::call(true)?;
    if pg_enum_seams::at_eoxact_enum::is_installed() {
        pg_enum_seams::at_eoxact_enum::call();
    }
    if tablecmds_seams::at_eoxact_on_commit_actions::is_installed() {
        tablecmds_seams::at_eoxact_on_commit_actions::call(true);
    }
    namespace_seams::at_eoxact_namespace::call(true, is_parallel_worker);
    { let _ = smgr::AtEOXact_SMgr(); }
    fd::AtEOXact_Files(true)?;
    // No combo CID can exist while combocid.c is unported (heapam's adjust_cmax
    // arm panics first); guarded.
    if combocid_seams::at_eoxact_combocid::is_installed() {
        combocid_seams::at_eoxact_combocid::call();
    }
    // AtEOXact_HashTables dissolves (crate docs).
    pgstat::xact::AtEOXact_PgStat(true, is_parallel_worker);
    snapmgr_seams::at_eoxact_snapshot::call(true, false)?;
    if launcher_seams::at_eoxact_apply_launcher::is_installed() {
        launcher_seams::at_eoxact_apply_launcher::call(true);
    }
    // AtEOXact_LogicalRepWorkers (worker.c, hosted by the launcher crate):
    // wake the workers of subscriptions this transaction altered.
    if logical_worker_seams::at_eoxact_logical_rep_workers::is_installed() {
        logical_worker_seams::at_eoxact_logical_rep_workers::call(true);
    }
    backend_status_seams::pgstat_report_xact_timestamp::call(0);

    delete_transaction_owner()?;
    xp.with(|s| s.current_mut().has_resource_owner = false);

    AtCommit_Memory(xp);

    xp.with(|s| {
        {
            let mut n = s.current_mut();
            n.full_transaction_id = InvalidFullTransactionId;
            n.sub_transaction_id = InvalidSubTransactionId;
            n.nesting_level = 0;
            n.guc_nest_level = 0;
            n.child_xids = Vec::new();
        }
        s.set_top_full_xid(InvalidFullTransactionId);
        s.parallel_current_xids = Vec::new();
        s.current_mut().state = TRANS_DEFAULT;
    });

    init_small::globals::ResumeInterrupts();
    Ok(())
}

fn PrepareTransaction(xp: XsPtr) -> PgResult<()> {
    let xid = GetCurrentTransactionId()?;
    debug_assert!(!IsInParallelMode());

    ShowTransactionState("PrepareTransaction");

    if xp.with(|s| s.current().state) != TRANS_INPROGRESS {
        let st = TransStateAsString(xp.with(|s| s.current().state));
        warn_internal(&format!("PrepareTransaction while in {st} state"));
    }
    debug_assert!(!xp.with(|s| s.is_subxact()));

    loop {
        trigger_seams::after_trigger_fire_deferred::call()?;
        if !portalmem::PreCommit_Portals(true)? {
            break;
        }
    }

    CallXactCallbacks(xp, XACT_EVENT_PRE_PREPARE)?;

    trigger_seams::after_trigger_end_xact::call(true)?;

    // No on_commits writer exists while tablecmds is unported; guarded like
    // the other provably-empty registries.
    if tablecmds_seams::pre_commit_on_commit_actions::is_installed() {
        tablecmds_seams::pre_commit_on_commit_actions::call()?;
    }

    if catalog_storage_seams::smgr_do_pending_syncs::is_installed() {
        catalog_storage_seams::smgr_do_pending_syncs::call(true, false)?;
    }

    // No LO descriptor can exist while lo_open is unported; guarded.
    if be_fsstubs_seams::at_eoxact_large_object::is_installed() {
        be_fsstubs_seams::at_eoxact_large_object::call(true)?;
    }


    predicate_seams::pre_commit_check_for_serialization_failure::call()?;

    if (MyXactFlags() & XACT_FLAGS_ACCESSEDTEMPNAMESPACE) != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot PREPARE a transaction that has operated on temporary objects")
            .finish(xact_location("PrepareTransaction"));
    }

    if snapmgr_seams::xact_has_exported_snapshots::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot PREPARE a transaction that has exported snapshots")
            .finish(xact_location("PrepareTransaction"));
    }

    init_small::globals::HoldInterrupts();

    xp.with(|s| s.current_mut().state = TRANS_PREPARE);

    if lmgr_proc::globals::TransactionTimeout() > 0 {
        timeout::disable_timeout(timeout_seams::TRANSACTION_TIMEOUT, false);
    }

    let prepared_at = timestamp_seams::get_current_timestamp::call();

    let gid = xp.with(|s| s.prepare_gid.take())
        .ok_or_else(|| PgError::error("PrepareTransaction: no prepared-transaction GID set"))?;
    let databaseid = init_small::globals::MyDatabaseId();
    twophase_seams::mark_as_preparing::call(
        xid,
        &gid,
        prepared_at,
        miscinit::GetUserId(),
        databaseid,
    )?;

    // Collect the 2PC state-file data; segment order — and thus the replay
    // order at COMMIT/ROLLBACK PREPARED — must match the calls that follow.
    let prep_ws = MemoryContext::new("StartPrepare");
    let prep_mcx = prep_ws.mcx();
    let commitrels = catalog_storage_seams::smgr_get_pending_deletes::call(prep_mcx, true)?;
    let abortrels = catalog_storage_seams::smgr_get_pending_deletes::call(prep_mcx, false)?;
    let children = xactGetCommittedChildren()?;
    let commitstats = pgstat::xact::pgstat_get_transactional_drops(prep_mcx, true)?;
    let abortstats = pgstat::xact::pgstat_get_transactional_drops(prep_mcx, false)?;
    let (invalmsgs, initfileinval) = inval::eoxact::xactGetCommittedInvalidationMessages(prep_mcx)?;

    let start_args = twophase_seams::StartPrepareArgs {
        xid,
        gid: gid.clone(),
        prepared_at,
        owner: miscinit::GetUserId(),
        databaseid,
        children,
        ncommitrels: commitrels.len() as i32,
        commitrels: crate::wal::rels_bytes(&commitrels)?,
        nabortrels: abortrels.len() as i32,
        abortrels: crate::wal::rels_bytes(&abortrels)?,
        ncommitstats: commitstats.len() as i32,
        commitstats: crate::wal::stats_bytes(&commitstats)?,
        nabortstats: abortstats.len() as i32,
        abortstats: crate::wal::stats_bytes(&abortstats)?,
        ninvalmsgs: invalmsgs.len() as i32,
        invalmsgs: crate::wal::inval_msgs_bytes(&invalmsgs)?,
        initfileinval,
    };
    twophase_seams::start_prepare::call(&start_args)?;

    async_seams::at_prepare_notify::call()?;
    lock::AtPrepare_Locks()?;
    predicate_seams::at_prepare_predicate_locks::call()?;
    pgstat::xact::AtPrepare_PgStat()?;
    multixact_seams::at_prepare_multixact::call()?;
    relmapper::AtPrepare_RelationMap()?;

    twophase_seams::end_prepare::call()?;

    xlog_seams::set_xact_last_rec_end::call(0);

    // Transfer locks to a dummy PGPROC before ProcArrayClearTransaction, so
    // GetLockConflicts can't conclude "xact already ended" for our locks.
    lock::PostPrepare_Locks(xid)?;

    procarray_seams::proc_array_clear_transaction::call()?;

    CallXactCallbacks(xp, XACT_EVENT_PREPARE)?;

    // Unlike Commit/Abort, Prepare does NOT reset CurrentResourceOwner here
    // (it clears it at the tail, with the delete).
    release_transaction_owner_before_locks(true)?;

    aio_seams::at_eoxact_aio::call(true);

    bufmgr::AtEOXact_Buffers(true);

    relcache_seams::at_eoxact_relation_cache::call(true)?;

    typcache_seams::at_eoxact_type_cache::call();


    pgstat::xact::PostPrepare_PgStat();

    inval::eoxact::PostPrepare_Inval()?;

    catalog_storage_seams::post_prepare_smgr::call();

    multixact_seams::post_prepare_multixact::call(xid);

    predicate_seams::post_prepare_predicate_locks::call(xid)?;

    release_transaction_owner_locks(true)?;

    twophase_seams::post_prepare_twophase::call();

    guc::AtEOXact_GUC(true, 1);
    spi_seams::at_eoxact_spi::call(true)?;
    if pg_enum_seams::at_eoxact_enum::is_installed() {
        pg_enum_seams::at_eoxact_enum::call();
    }
    if tablecmds_seams::at_eoxact_on_commit_actions::is_installed() {
        tablecmds_seams::at_eoxact_on_commit_actions::call(true);
    }
    namespace_seams::at_eoxact_namespace::call(true, false);
    { let _ = smgr::AtEOXact_SMgr(); }
    fd::AtEOXact_Files(true)?;
    // No combo CID can exist while combocid.c is unported (heapam's adjust_cmax
    // arm panics first); guarded.
    if combocid_seams::at_eoxact_combocid::is_installed() {
        combocid_seams::at_eoxact_combocid::call();
    }
    // AtEOXact_HashTables dissolves; no AtEOXact_PgStat (pgstat fixed above).
    snapmgr_seams::at_eoxact_snapshot::call(true, true)?;
    if launcher_seams::at_eoxact_apply_launcher::is_installed() {
        launcher_seams::at_eoxact_apply_launcher::call(false);
    }
    if logical_worker_seams::at_eoxact_logical_rep_workers::is_installed() {
        logical_worker_seams::at_eoxact_logical_rep_workers::call(false);
    }
    backend_status_seams::pgstat_report_xact_timestamp::call(0);

    resowner::SetCurrentResourceOwner(types_resowner::ResourceOwner::NULL);
    delete_transaction_owner()?;
    xp.with(|s| s.current_mut().has_resource_owner = false);

    AtCommit_Memory(xp);

    xp.with(|s| {
        {
            let mut n = s.current_mut();
            n.full_transaction_id = InvalidFullTransactionId;
            n.sub_transaction_id = InvalidSubTransactionId;
            n.nesting_level = 0;
            n.guc_nest_level = 0;
            n.child_xids = Vec::new();
        }
        s.set_top_full_xid(InvalidFullTransactionId);
        s.parallel_current_xids = Vec::new();
        s.current_mut().state = TRANS_DEFAULT;
    });

    init_small::globals::ResumeInterrupts();
    Ok(())
}

fn AbortTransaction(xp: XsPtr) -> PgResult<()> {
    init_small::globals::HoldInterrupts();

    if lmgr_proc::globals::TransactionTimeout() > 0 {
        timeout::disable_timeout(timeout_seams::TRANSACTION_TIMEOUT, false);
    }

    AtAbort_Memory(xp);
    AtAbort_ResourceOwner();

    let _ = lwlock::LWLockReleaseAll();

    waitevent::pgstat_report_wait_end();
    // No progress command can start while backend_progress is unported; guarded.
    if backend_progress_seams::pgstat_progress_end_command::is_installed() {
        backend_progress_seams::pgstat_progress_end_command::call();
    }

    aio_seams::pgaio_error_cleanup::call();

    bufmgr::UnlockBuffers();

    xloginsert_seams::xlog_reset_insertion::call();

    let _ = condition_variable_seams::condition_variable_cancel_sleep::call();

    lmgr_proc::LockErrorCleanup()?;

    timeout::reschedule_timeouts();

    libpq_pqsignal::unblock_signals();

    let is_parallel_worker = cur_block_state() == TBLOCK_PARALLEL_INPROGRESS;
    let st = xp.with(|s| s.current().state);
    if st != TRANS_INPROGRESS && st != TRANS_PREPARE {
        warn_internal(&format!(
            "AbortTransaction while in {} state",
            TransStateAsString(st)
        ));
    }
    debug_assert!(!xp.with(|s| s.is_subxact()));

    xp.with(|s| s.current_mut().state = TRANS_ABORT);

    let (prev_user, prev_sec) = xp.with(|s| (s.current().prev_user, s.current().prev_sec_context));
    miscinit::SetUserIdAndSecContext(prev_user, prev_sec);

    // No REINDEX state can exist while catalog/index.c is unported; guarded.
    if catalog_index_seams::reset_reindex_state::is_installed() {
        catalog_index_seams::reset_reindex_state::call(xp.with(|s| s.current().nesting_level));
    }

    // No logical decoding can be in progress while reorderbuffer is unported; guarded.
    if logical_seams::reset_logical_streaming_state::is_installed() {
        logical_seams::reset_logical_streaming_state::call();
    }

    // No exported logical snapshot can exist while snapbuild is unported; guarded.
    if snapbuild_seams::snap_build_reset_exported_snapshot_state::is_installed() {
        snapbuild_seams::snap_build_reset_exported_snapshot_state::call();
    }

    parallel_seams::at_eoxact_parallel::call(false)?;
    xp.with(|s| {
        s.current_mut().parallel_mode_level = 0;
        s.current_mut().parallel_child_xact = false; // should be false already
    });

    trigger_seams::after_trigger_end_xact::call(false)?;
    portalmem::AtAbort_Portals()?;
    if catalog_storage_seams::smgr_do_pending_syncs::is_installed() {
        catalog_storage_seams::smgr_do_pending_syncs::call(false, is_parallel_worker)?;
    }
    if be_fsstubs_seams::at_eoxact_large_object::is_installed() {
        be_fsstubs_seams::at_eoxact_large_object::call(false)?;
    }
    if async_seams::at_abort_notify::is_installed() {
        async_seams::at_abort_notify::call();
    }
    relmapper::AtEOXact_RelationMap(false, is_parallel_worker)?;
    // No prepared-xact gxact can be locked while twophase is unported; guarded.
    if twophase_seams::at_abort_twophase::is_installed() {
        twophase_seams::at_abort_twophase::call();
    }

    let latest_xid = if !is_parallel_worker {
        RecordTransactionAbort(false)?
    } else {
        xlog_seams::xlog_set_async_xact_lsn::call(xlog_seams::xact_last_rec_end::call());
        InvalidTransactionId
    };

    // Announce no transaction in progress: _before_ releasing locks and
    // _after_ RecordTransactionAbort.
    procarray_seams::proc_array_end_transaction::call(
        lmgr_proc::MyProc().expect("MyProc is not set"),
        latest_xid,
    )?;

    if xp.with(|s| s.current().has_resource_owner) {
        CallXactCallbacks(xp, if is_parallel_worker {
            XACT_EVENT_PARALLEL_ABORT
        } else {
            XACT_EVENT_ABORT
        })?;

        release_transaction_owner_before_locks(false)?;
        aio_seams::at_eoxact_aio::call(false);
        bufmgr::AtEOXact_Buffers(false);
        relcache_seams::at_eoxact_relation_cache::call(false)?;
        typcache_seams::at_eoxact_type_cache::call();
        inval::eoxact::AtEOXact_Inval(false)?;
        multixact_seams::at_eoxact_multixact::call();
        release_transaction_owner_locks(false)?;
        if catalog_storage_seams::smgr_do_pending_deletes::is_installed() {
            catalog_storage_seams::smgr_do_pending_deletes::call(false)?;
        }

        guc::AtEOXact_GUC(false, 1);
        spi_seams::at_eoxact_spi::call(false)?;
        if pg_enum_seams::at_eoxact_enum::is_installed() {
            pg_enum_seams::at_eoxact_enum::call();
        }
        if tablecmds_seams::at_eoxact_on_commit_actions::is_installed() {
            tablecmds_seams::at_eoxact_on_commit_actions::call(false);
        }
        namespace_seams::at_eoxact_namespace::call(false, is_parallel_worker);
        { let _ = smgr::AtEOXact_SMgr(); }
        fd::AtEOXact_Files(false)?;
        if combocid_seams::at_eoxact_combocid::is_installed() {
            combocid_seams::at_eoxact_combocid::call();
        }
        // AtEOXact_HashTables dissolves.
        pgstat::xact::AtEOXact_PgStat(false, is_parallel_worker);
        if launcher_seams::at_eoxact_apply_launcher::is_installed() {
            launcher_seams::at_eoxact_apply_launcher::call(false);
        }
        if logical_worker_seams::at_eoxact_logical_rep_workers::is_installed() {
            logical_worker_seams::at_eoxact_logical_rep_workers::call(false);
        }
        backend_status_seams::pgstat_report_xact_timestamp::call(0);
    }

    init_small::globals::ResumeInterrupts();
    Ok(())
}

fn CleanupTransaction(xp: XsPtr) -> PgResult<()> {
    if xp.with(|s| s.current().state) != TRANS_ABORT {
        return Err(Box::new(PgError::new(
            FATAL,
            format!(
                "CleanupTransaction: unexpected state {}",
                TransStateAsString(xp.with(|s| s.current().state))
            ),
        )));
    }

    portalmem::AtCleanup_Portals()?; // now safe to release portal memory
    snapmgr_seams::at_eoxact_snapshot::call(false, true)?; // release the xact's snapshots

    resowner::SetCurrentResourceOwner(types_resowner::ResourceOwner::NULL);
    delete_transaction_owner()?;
    xp.with(|s| s.current_mut().has_resource_owner = false);

    AtCleanup_Memory(xp); // and transaction memory

    xp.with(|s| {
        {
            let mut n = s.current_mut();
            n.full_transaction_id = InvalidFullTransactionId;
            n.sub_transaction_id = InvalidSubTransactionId;
            n.nesting_level = 0;
            n.guc_nest_level = 0;
            n.child_xids = Vec::new();
            n.parallel_mode_level = 0;
            n.parallel_child_xact = false;
        }
        s.set_top_full_xid(InvalidFullTransactionId);
        s.parallel_current_xids = Vec::new();
        s.current_mut().state = TRANS_DEFAULT;
    });
    Ok(())
}

pub fn StartTransactionCommand() -> PgResult<()> {
    let xp = xs_ptr();
    match cur_block_state() {
        TBLOCK_DEFAULT => {
            StartTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_STARTED);
        }

        TBLOCK_INPROGRESS | TBLOCK_IMPLICIT_INPROGRESS | TBLOCK_SUBINPROGRESS => {}

        TBLOCK_ABORT | TBLOCK_SUBABORT => {}

        other => {
            return Err(Box::new(PgError::new(
                ERROR,
                format!(
                    "StartTransactionCommand: unexpected state {}",
                    BlockStateAsString(other)
                ),
            )));
        }
    }
    Ok(())
}

pub fn SaveTransactionCharacteristics() -> SavedTransactionCharacteristics {
    save_transaction_characteristics_in(xs_ptr())
}

pub(crate) fn save_transaction_characteristics_in(xp: XsPtr) -> SavedTransactionCharacteristics {
    xp.with(|s| SavedTransactionCharacteristics {
        save_XactIsoLevel: s.XactIsoLevel,
        save_XactReadOnly: s.XactReadOnly,
        save_XactDeferrable: s.XactDeferrable,
    })
}

pub fn RestoreTransactionCharacteristics(saved: SavedTransactionCharacteristics) {
    xs(|s| {
        s.XactIsoLevel = saved.save_XactIsoLevel;
        s.XactReadOnly = saved.save_XactReadOnly;
        s.XactDeferrable = saved.save_XactDeferrable;
    });
}

pub fn CommitTransactionCommand() -> PgResult<()> {
    while !CommitTransactionCommandInternal()? {}
    Ok(())
}

/// One iteration; false means loop again (C's `return false` arms).
fn CommitTransactionCommandInternal() -> PgResult<bool> {
    let xp = xs_ptr();
    let savetc = save_transaction_characteristics_in(xp);

    match cur_block_state() {
        TBLOCK_DEFAULT | TBLOCK_PARALLEL_INPROGRESS => {
            return Err(Box::new(PgError::new(
                FATAL,
                format!(
                    "CommitTransactionCommand: unexpected state {}",
                    BlockStateAsString(cur_block_state())
                ),
            )));
        }

        TBLOCK_STARTED => {
            CommitTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_BEGIN => {
            xp.with(|s| s.current_mut().block_state = TBLOCK_INPROGRESS);
        }

        TBLOCK_INPROGRESS | TBLOCK_IMPLICIT_INPROGRESS | TBLOCK_SUBINPROGRESS => {
            CommandCounterIncrement()?;
        }

        TBLOCK_END => {
            CommitTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            if xp.with(|s| s.current().chain) {
                StartTransaction(xp)?;
                xp.with(|s| {
                    s.current_mut().block_state = TBLOCK_INPROGRESS;
                    s.current_mut().chain = false;
                });
                RestoreTransactionCharacteristics(savetc);
            }
        }

        TBLOCK_ABORT | TBLOCK_SUBABORT => {}

        TBLOCK_ABORT_END => {
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            if xp.with(|s| s.current().chain) {
                StartTransaction(xp)?;
                xp.with(|s| {
                    s.current_mut().block_state = TBLOCK_INPROGRESS;
                    s.current_mut().chain = false;
                });
                RestoreTransactionCharacteristics(savetc);
            }
        }

        TBLOCK_ABORT_PENDING => {
            AbortTransaction(xp)?;
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            if xp.with(|s| s.current().chain) {
                StartTransaction(xp)?;
                xp.with(|s| {
                    s.current_mut().block_state = TBLOCK_INPROGRESS;
                    s.current_mut().chain = false;
                });
                RestoreTransactionCharacteristics(savetc);
            }
        }

        TBLOCK_PREPARE => {
            PrepareTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_SUBBEGIN => {
            StartSubTransaction()?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_SUBINPROGRESS);
        }

        TBLOCK_SUBRELEASE => {
            loop {
                CommitSubTransaction()?;
                if cur_block_state() != TBLOCK_SUBRELEASE {
                    break;
                }
            }
            debug_assert!(matches!(
                cur_block_state(),
                TBLOCK_INPROGRESS | TBLOCK_SUBINPROGRESS
            ));
        }

        TBLOCK_SUBCOMMIT => {
            loop {
                CommitSubTransaction()?;
                if cur_block_state() != TBLOCK_SUBCOMMIT {
                    break;
                }
            }
            match cur_block_state() {
                TBLOCK_END => {
                    CommitTransaction(xp)?;
                    xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
                    if xp.with(|s| s.current().chain) {
                        StartTransaction(xp)?;
                        xp.with(|s| {
                            s.current_mut().block_state = TBLOCK_INPROGRESS;
                            s.current_mut().chain = false;
                        });
                        RestoreTransactionCharacteristics(savetc);
                    }
                }
                TBLOCK_PREPARE => {
                    PrepareTransaction(xp)?;
                    xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
                }
                other => {
                    return Err(Box::new(PgError::new(
                        ERROR,
                        format!(
                            "CommitTransactionCommand: unexpected state {}",
                            BlockStateAsString(other)
                        ),
                    )));
                }
            }
        }

        TBLOCK_SUBABORT_END => {
            CleanupSubTransaction()?;
            return Ok(false);
        }

        TBLOCK_SUBABORT_PENDING => {
            AbortSubTransaction()?;
            CleanupSubTransaction()?;
            return Ok(false);
        }

        TBLOCK_SUBRESTART => {
            let (name, savepoint_level) = xp.with(|s| {
                let name = s.current_mut().name.take();
                (name, s.current().savepoint_level)
            });
            AbortSubTransaction()?;
            CleanupSubTransaction()?;
            DefineSavepoint(None)?;
            xp.with(|s| {
                s.current_mut().name = name;
                s.current_mut().savepoint_level = savepoint_level;
            });
            debug_assert_eq!(cur_block_state(), TBLOCK_SUBBEGIN);
            StartSubTransaction()?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_SUBINPROGRESS);
        }

        TBLOCK_SUBABORT_RESTART => {
            let (name, savepoint_level) = xp.with(|s| {
                let name = s.current_mut().name.take();
                (name, s.current().savepoint_level)
            });
            CleanupSubTransaction()?;
            DefineSavepoint(None)?;
            xp.with(|s| {
                s.current_mut().name = name;
                s.current_mut().savepoint_level = savepoint_level;
            });
            debug_assert_eq!(cur_block_state(), TBLOCK_SUBBEGIN);
            StartSubTransaction()?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_SUBINPROGRESS);
        }
    }

    Ok(true)
}

pub fn AbortCurrentTransaction() -> PgResult<()> {
    while !AbortCurrentTransactionInternal()? {}
    Ok(())
}

fn AbortCurrentTransactionInternal() -> PgResult<bool> {
    let xp = xs_ptr();
    match cur_block_state() {
        TBLOCK_DEFAULT => {
            if xp.with(|s| s.current().state) == TRANS_DEFAULT {
            } else {
                if xp.with(|s| s.current().state) == TRANS_START {
                    xp.with(|s| s.current_mut().state = TRANS_INPROGRESS);
                }
                AbortTransaction(xp)?;
                CleanupTransaction(xp)?;
            }
        }

        TBLOCK_STARTED | TBLOCK_IMPLICIT_INPROGRESS => {
            AbortTransaction(xp)?;
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_BEGIN => {
            AbortTransaction(xp)?;
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_INPROGRESS | TBLOCK_PARALLEL_INPROGRESS => {
            AbortTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_ABORT);
            // CleanupTransaction happens when we exit TBLOCK_ABORT_END
        }

        TBLOCK_END => {
            AbortTransaction(xp)?;
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_ABORT | TBLOCK_SUBABORT => {}

        TBLOCK_ABORT_END => {
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_ABORT_PENDING => {
            AbortTransaction(xp)?;
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_PREPARE => {
            AbortTransaction(xp)?;
            CleanupTransaction(xp)?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        TBLOCK_SUBINPROGRESS => {
            AbortSubTransaction()?;
            xp.with(|s| s.current_mut().block_state = TBLOCK_SUBABORT);
        }

        TBLOCK_SUBBEGIN | TBLOCK_SUBRELEASE | TBLOCK_SUBCOMMIT | TBLOCK_SUBABORT_PENDING
        | TBLOCK_SUBRESTART => {
            AbortSubTransaction()?;
            CleanupSubTransaction()?;
            return Ok(false);
        }

        TBLOCK_SUBABORT_END | TBLOCK_SUBABORT_RESTART => {
            CleanupSubTransaction()?;
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn BeginTransactionBlock() -> PgResult<()> {
    match cur_block_state() {
        TBLOCK_STARTED | TBLOCK_IMPLICIT_INPROGRESS => {
            xs(|s| s.current_mut().block_state = TBLOCK_BEGIN);
            Ok(())
        }
        TBLOCK_INPROGRESS
        | TBLOCK_PARALLEL_INPROGRESS
        | TBLOCK_SUBINPROGRESS
        | TBLOCK_ABORT
        | TBLOCK_SUBABORT => ereport(WARNING)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg("there is already a transaction in progress")
            .finish(xact_location("BeginTransactionBlock")),
        other => Err(unexpected_block_state("BeginTransactionBlock", other)),
    }
}

pub fn PrepareTransactionBlock(gid: &str) -> PgResult<bool> {
    let mut result = EndTransactionBlock(false)?;

    if result {
        let top_state = xs(|s| s.node(0).block_state);
        if top_state == TBLOCK_END {
            let gid = try_strdup(gid, "out of memory saving prepared-transaction GID")?;
            xs(|s| {
                s.prepare_gid = Some(gid);
                s.node_mut(0).block_state = TBLOCK_PREPARE;
            });
        } else {
            debug_assert!(matches!(
                top_state,
                TBLOCK_STARTED | TBLOCK_IMPLICIT_INPROGRESS
            ));
            result = false; // don't send back a PREPARE result tag
        }
    }
    Ok(result)
}

pub fn EndTransactionBlock(chain: bool) -> PgResult<bool> {
    let mut result = false;
    match cur_block_state() {
        TBLOCK_INPROGRESS => {
            xs(|s| s.current_mut().block_state = TBLOCK_END);
            result = true;
        }

        TBLOCK_IMPLICIT_INPROGRESS => {
            if chain {
                return ereport(ERROR)
                    .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                    // translator: %s represents an SQL statement name
                    .errmsg("COMMIT AND CHAIN can only be used in transaction blocks")
                    .finish(xact_location("EndTransactionBlock"))
                    .map(|()| false);
            }
            ereport(WARNING)
                .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                .errmsg("there is no transaction in progress")
                .finish(xact_location("EndTransactionBlock"))?;
            xs(|s| s.current_mut().block_state = TBLOCK_END);
            result = true;
        }

        TBLOCK_ABORT => {
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT_END);
        }

        TBLOCK_SUBINPROGRESS => {
            let bad: Option<TBlockState> = xs(|s| {
                let last = s.stack_len() - 1;
                for i in (1..=last).rev() {
                    let mut n = s.node_mut(i);
                    if n.block_state == TBLOCK_SUBINPROGRESS {
                        n.block_state = TBLOCK_SUBCOMMIT;
                    } else {
                        return Some(n.block_state);
                    }
                }
                let mut top = s.node_mut(0);
                if top.block_state == TBLOCK_INPROGRESS {
                    top.block_state = TBLOCK_END;
                    None
                } else {
                    Some(top.block_state)
                }
            });
            if let Some(bs) = bad {
                return Err(unexpected_block_state("EndTransactionBlock", bs));
            }
            result = true;
        }

        TBLOCK_SUBABORT => {
            let bad: Option<TBlockState> = xs(|s| {
                let last = s.stack_len() - 1;
                for i in (1..=last).rev() {
                    let mut n = s.node_mut(i);
                    match n.block_state {
                        TBLOCK_SUBINPROGRESS => n.block_state = TBLOCK_SUBABORT_PENDING,
                        TBLOCK_SUBABORT => n.block_state = TBLOCK_SUBABORT_END,
                        other => return Some(other),
                    }
                }
                let mut top = s.node_mut(0);
                match top.block_state {
                    TBLOCK_INPROGRESS => {
                        top.block_state = TBLOCK_ABORT_PENDING;
                        None
                    }
                    TBLOCK_ABORT => {
                        top.block_state = TBLOCK_ABORT_END;
                        None
                    }
                    other => Some(other),
                }
            });
            if let Some(bs) = bad {
                return Err(unexpected_block_state("EndTransactionBlock", bs));
            }
        }

        TBLOCK_STARTED => {
            if chain {
                return ereport(ERROR)
                    .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                    .errmsg("COMMIT AND CHAIN can only be used in transaction blocks")
                    .finish(xact_location("EndTransactionBlock"))
                    .map(|()| false);
            }
            ereport(WARNING)
                .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                .errmsg("there is no transaction in progress")
                .finish(xact_location("EndTransactionBlock"))?;
            result = true;
        }

        TBLOCK_PARALLEL_INPROGRESS => {
            return ereport(FATAL)
                .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
                .errmsg("cannot commit during a parallel operation")
                .finish(xact_location("EndTransactionBlock"))
                .map(|()| false);
        }

        other => return Err(unexpected_block_state("EndTransactionBlock", other)),
    }

    xs(|s| s.node_mut(0).chain = chain);
    Ok(result)
}

pub fn UserAbortTransactionBlock(chain: bool) -> PgResult<()> {
    match cur_block_state() {
        TBLOCK_INPROGRESS => {
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT_PENDING);
        }

        TBLOCK_ABORT => {
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT_END);
        }

        TBLOCK_SUBINPROGRESS | TBLOCK_SUBABORT => {
            let bad: Option<TBlockState> = xs(|s| {
                let last = s.stack_len() - 1;
                for i in (1..=last).rev() {
                    let mut n = s.node_mut(i);
                    match n.block_state {
                        TBLOCK_SUBINPROGRESS => n.block_state = TBLOCK_SUBABORT_PENDING,
                        TBLOCK_SUBABORT => n.block_state = TBLOCK_SUBABORT_END,
                        other => return Some(other),
                    }
                }
                let mut top = s.node_mut(0);
                match top.block_state {
                    TBLOCK_INPROGRESS => {
                        top.block_state = TBLOCK_ABORT_PENDING;
                        None
                    }
                    TBLOCK_ABORT => {
                        top.block_state = TBLOCK_ABORT_END;
                        None
                    }
                    other => Some(other),
                }
            });
            if let Some(bs) = bad {
                return Err(unexpected_block_state("UserAbortTransactionBlock", bs));
            }
        }

        TBLOCK_STARTED | TBLOCK_IMPLICIT_INPROGRESS => {
            if chain {
                return ereport(ERROR)
                    .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                    // translator: %s represents an SQL statement name
                    .errmsg("ROLLBACK AND CHAIN can only be used in transaction blocks")
                    .finish(xact_location("UserAbortTransactionBlock"));
            }
            ereport(WARNING)
                .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                .errmsg("there is no transaction in progress")
                .finish(xact_location("UserAbortTransactionBlock"))?;
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT_PENDING);
        }

        TBLOCK_PARALLEL_INPROGRESS => {
            return ereport(FATAL)
                .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
                .errmsg("cannot abort during a parallel operation")
                .finish(xact_location("UserAbortTransactionBlock"));
        }

        other => return Err(unexpected_block_state("UserAbortTransactionBlock", other)),
    }

    xs(|s| s.node_mut(0).chain = chain);
    Ok(())
}

pub fn BeginImplicitTransactionBlock() {
    xs(|s| {
        if s.current().block_state == TBLOCK_STARTED {
            s.current_mut().block_state = TBLOCK_IMPLICIT_INPROGRESS;
        }
    });
}

pub fn EndImplicitTransactionBlock() {
    xs(|s| {
        if s.current().block_state == TBLOCK_IMPLICIT_INPROGRESS {
            s.current_mut().block_state = TBLOCK_STARTED;
        }
    });
}

/// `DefineSavepoint`; `None` is C's NULL name (the SUBRESTART arms).
pub fn DefineSavepoint(name: Option<&str>) -> PgResult<()> {
    if IsInParallelMode() || parallel_seams::is_parallel_worker::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot define savepoints during a parallel operation")
            .finish(xact_location("DefineSavepoint"));
    }

    match cur_block_state() {
        TBLOCK_INPROGRESS | TBLOCK_SUBINPROGRESS => {
            PushTransaction()?;
            if let Some(name) = name {
                let name = try_strdup(name, "out of memory saving savepoint name")?;
                xs(|s| s.current_mut().name = Some(name));
            }
            Ok(())
        }
        TBLOCK_IMPLICIT_INPROGRESS => ereport(ERROR)
            .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
            // translator: %s represents an SQL statement name
            .errmsg("SAVEPOINT can only be used in transaction blocks")
            .finish(xact_location("DefineSavepoint")),
        other => Err(unexpected_block_state("DefineSavepoint", other)),
    }
}

pub fn ReleaseSavepoint(name: &str) -> PgResult<()> {
    if IsInParallelMode() || parallel_seams::is_parallel_worker::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot release savepoints during a parallel operation")
            .finish(xact_location("ReleaseSavepoint"));
    }

    match cur_block_state() {
        TBLOCK_INPROGRESS => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!("savepoint \"{name}\" does not exist"))
                .finish(xact_location("ReleaseSavepoint"));
        }
        TBLOCK_IMPLICIT_INPROGRESS => {
            return ereport(ERROR)
                .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                // translator: %s represents an SQL statement name
                .errmsg("RELEASE SAVEPOINT can only be used in transaction blocks")
                .finish(xact_location("ReleaseSavepoint"));
        }
        TBLOCK_SUBINPROGRESS => {}
        other => return Err(unexpected_block_state("ReleaseSavepoint", other)),
    }

    let target = find_savepoint(name, "ReleaseSavepoint")?;

    xs(|s| {
        let last = s.stack_len() - 1;
        for i in (target..=last).rev() {
            let mut n = s.node_mut(i);
            debug_assert_eq!(n.block_state, TBLOCK_SUBINPROGRESS);
            n.block_state = TBLOCK_SUBRELEASE;
        }
    });
    Ok(())
}

fn find_savepoint(name: &str, function: &'static str) -> PgResult<usize> {
    enum Find {
        NotFound,
        WrongLevel,
        At(usize),
    }
    let found = xs(|s| {
        let cur_level = s.current().savepoint_level;
        match s.rposition_node(|node| node.name.as_deref() == Some(name)) {
            None => Find::NotFound,
            Some(t) if s.node(t).savepoint_level != cur_level => Find::WrongLevel,
            Some(t) => Find::At(t),
        }
    });
    match found {
        Find::NotFound => ereport(ERROR)
            .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
            .errmsg(format!("savepoint \"{name}\" does not exist"))
            .finish(xact_location(function))
            .map(|()| 0),
        Find::WrongLevel => ereport(ERROR)
            .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
            .errmsg(format!(
                "savepoint \"{name}\" does not exist within current savepoint level"
            ))
            .finish(xact_location(function))
            .map(|()| 0),
        Find::At(t) => Ok(t),
    }
}

pub fn RollbackToSavepoint(name: &str) -> PgResult<()> {
    if IsInParallelMode() || parallel_seams::is_parallel_worker::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot rollback to savepoints during a parallel operation")
            .finish(xact_location("RollbackToSavepoint"));
    }

    match cur_block_state() {
        TBLOCK_INPROGRESS | TBLOCK_ABORT => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!("savepoint \"{name}\" does not exist"))
                .finish(xact_location("RollbackToSavepoint"));
        }
        TBLOCK_IMPLICIT_INPROGRESS => {
            return ereport(ERROR)
                .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                // translator: %s represents an SQL statement name
                .errmsg("ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
                .finish(xact_location("RollbackToSavepoint"));
        }
        TBLOCK_SUBINPROGRESS | TBLOCK_SUBABORT => {}
        other => return Err(unexpected_block_state("RollbackToSavepoint", other)),
    }

    let target = find_savepoint(name, "RollbackToSavepoint")?;

    let bad: Option<TBlockState> = xs(|s| {
        let last = s.stack_len() - 1;
        for i in ((target + 1)..=last).rev() {
            let mut n = s.node_mut(i);
            match n.block_state {
                TBLOCK_SUBINPROGRESS => n.block_state = TBLOCK_SUBABORT_PENDING,
                TBLOCK_SUBABORT => n.block_state = TBLOCK_SUBABORT_END,
                other => return Some(other),
            }
        }
        let mut t = s.node_mut(target);
        match t.block_state {
            TBLOCK_SUBINPROGRESS => {
                t.block_state = TBLOCK_SUBRESTART;
                None
            }
            TBLOCK_SUBABORT => {
                t.block_state = TBLOCK_SUBABORT_RESTART;
                None
            }
            other => Some(other),
        }
    });
    if let Some(bs) = bad {
        return Err(unexpected_block_state("RollbackToSavepoint", bs));
    }
    Ok(())
}

/// Like DefineSavepoint, but allowed in implicit blocks, parallel mode, and
/// the STARTED/END/PREPARE states; immediately starts the subtransaction.
pub fn BeginInternalSubTransaction(name: Option<&str>) -> PgResult<()> {
    let save_exit_on_any_error = elog::config::exit_on_any_error();
    elog::config::set_exit_on_any_error(true);

    let result = (|| -> PgResult<()> {
        match cur_block_state() {
            TBLOCK_STARTED
            | TBLOCK_INPROGRESS
            | TBLOCK_IMPLICIT_INPROGRESS
            | TBLOCK_PARALLEL_INPROGRESS
            | TBLOCK_END
            | TBLOCK_PREPARE
            | TBLOCK_SUBINPROGRESS => {
                PushTransaction()?;
                if let Some(name) = name {
                    let name = try_strdup(name, "out of memory saving savepoint name")?;
                    xs(|s| s.current_mut().name = Some(name));
                }
            }
            other => {
                return Err(unexpected_block_state("BeginInternalSubTransaction", other));
            }
        }

        CommitTransactionCommand()?;
        StartTransactionCommand()
    })();

    elog::config::set_exit_on_any_error(save_exit_on_any_error);
    result
}

pub fn ReleaseCurrentSubTransaction() -> PgResult<()> {
    if cur_block_state() != TBLOCK_SUBINPROGRESS {
        return Err(Box::new(PgError::new(
            ERROR,
            format!(
                "ReleaseCurrentSubTransaction: unexpected state {}",
                BlockStateAsString(cur_block_state())
            ),
        )));
    }
    debug_assert!(xs(|s| s.current().state == TRANS_INPROGRESS));
    CommitSubTransaction()?;
    debug_assert!(xs(|s| s.current().state == TRANS_INPROGRESS));
    Ok(())
}

/// `RollbackAndReleaseCurrentSubTransaction` (OK in a parallel worker).
pub fn RollbackAndReleaseCurrentSubTransaction() -> PgResult<()> {
    match cur_block_state() {
        TBLOCK_SUBINPROGRESS | TBLOCK_SUBABORT => {}
        other => {
            return Err(Box::new(PgError::new(
                FATAL,
                format!(
                    "RollbackAndReleaseCurrentSubTransaction: unexpected state {}",
                    BlockStateAsString(other)
                ),
            )));
        }
    }

    if cur_block_state() == TBLOCK_SUBINPROGRESS {
        AbortSubTransaction()?;
    }

    CleanupSubTransaction()?;

    debug_assert!(matches!(
        cur_block_state(),
        TBLOCK_SUBINPROGRESS
            | TBLOCK_INPROGRESS
            | TBLOCK_IMPLICIT_INPROGRESS
            | TBLOCK_PARALLEL_INPROGRESS
            | TBLOCK_STARTED
    ));
    Ok(())
}

pub fn AbortOutOfAnyTransaction() -> PgResult<()> {
    let xp = xs_ptr();
    AtAbort_Memory(xp);

    loop {
        match cur_block_state() {
            TBLOCK_DEFAULT => {
                if xp.with(|s| s.current().state) == TRANS_DEFAULT {
                    // Not in a transaction, do nothing.
                } else {
                    if xp.with(|s| s.current().state) == TRANS_START {
                        xp.with(|s| s.current_mut().state = TRANS_INPROGRESS);
                    }
                    AbortTransaction(xp)?;
                    CleanupTransaction(xp)?;
                }
            }
            TBLOCK_STARTED
            | TBLOCK_BEGIN
            | TBLOCK_INPROGRESS
            | TBLOCK_IMPLICIT_INPROGRESS
            | TBLOCK_PARALLEL_INPROGRESS
            | TBLOCK_END
            | TBLOCK_ABORT_PENDING
            | TBLOCK_PREPARE => {
                AbortTransaction(xp)?;
                CleanupTransaction(xp)?;
                xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            }
            TBLOCK_ABORT | TBLOCK_ABORT_END => {
                portalmem::AtAbort_Portals()?;
                CleanupTransaction(xp)?;
                xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            }
            TBLOCK_SUBBEGIN
            | TBLOCK_SUBINPROGRESS
            | TBLOCK_SUBRELEASE
            | TBLOCK_SUBCOMMIT
            | TBLOCK_SUBABORT_PENDING
            | TBLOCK_SUBRESTART => {
                AbortSubTransaction()?;
                CleanupSubTransaction()?;
            }
            TBLOCK_SUBABORT | TBLOCK_SUBABORT_END | TBLOCK_SUBABORT_RESTART => {
                if xp.with(|s| s.current().has_resource_owner) {
                    let (my, parent) = subxact_ids();
                    portalmem::AtSubAbort_Portals(
                        my,
                        parent,
                        resowner::CurTransactionResourceOwner(),
                        resowner::ResourceOwnerGetParent(resowner::CurTransactionResourceOwner()),
                    )?;
                }
                CleanupSubTransaction()?;
            }
        }
        if cur_block_state() == TBLOCK_DEFAULT {
            break;
        }
    }

    debug_assert!(xp.with(|s| s.stack_len() == 1));
    Ok(())
}

/// (mySubid, parentSubid) for the current node.
fn subxact_ids() -> (SubTransactionId, SubTransactionId) {
    xs(|s| {
        let last = s.stack_len() - 1;
        let my = s.node(last).sub_transaction_id;
        let parent = if last > 0 {
            s.node(last - 1).sub_transaction_id
        } else {
            InvalidSubTransactionId
        };
        (my, parent)
    })
}

fn StartSubTransaction() -> PgResult<()> {
    if xs(|s| s.current().state) != TRANS_DEFAULT {
        warn_internal(&format!(
            "StartSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }
    xs(|s| s.current_mut().state = TRANS_START);

    AtSubStart_Memory();
    AtSubStart_ResourceOwner()?;
    trigger_seams::after_trigger_begin_sub_xact::call()?;

    xs(|s| s.current_mut().state = TRANS_INPROGRESS);

    let (my, parent) = subxact_ids();
    CallSubXactCallbacks(SUBXACT_EVENT_START_SUB, my, parent)?;

    ShowTransactionState("StartSubTransaction");
    Ok(())
}

fn CommitSubTransaction() -> PgResult<()> {
    ShowTransactionState("CommitSubTransaction");

    if xs(|s| s.current().state) != TRANS_INPROGRESS {
        warn_internal(&format!(
            "CommitSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }

    let (my, parent) = subxact_ids();
    CallSubXactCallbacks(SUBXACT_EVENT_PRE_COMMIT_SUB, my, parent)?;

    parallel_seams::at_eosubxact_parallel::call(true, my)?;
    let level = xs(|s| s.current().parallel_mode_level);
    if level != 0 {
        warn_internal(&format!(
            "parallelModeLevel is {level} not 0 at end of subtransaction"
        ));
        xs(|s| s.current_mut().parallel_mode_level = 0);
    }

    xs(|s| s.current_mut().state = TRANS_COMMIT);

    CommandCounterIncrement()?;

    if xs(|s| s.current().full_transaction_id.is_valid()) {
        AtSubCommit_childXids()?;
    }
    trigger_seams::after_trigger_end_sub_xact::call(true)?;
    let parent_nesting = xs(|s| {
        let last = s.stack_len() - 1;
        s.node(last - 1).nesting_level
    });
    portalmem::AtSubCommit_Portals(
        my,
        parent,
        parent_nesting,
        resowner::ResourceOwnerGetParent(resowner::CurTransactionResourceOwner()),
    );
    if be_fsstubs_seams::at_eosubxact_large_object::is_installed() {
        be_fsstubs_seams::at_eosubxact_large_object::call(true, my, parent)?;
    }
    if async_seams::at_subcommit_notify::is_installed() {
        async_seams::at_subcommit_notify::call()?;
    }

    CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, my, parent)?;

    release_subxact_owner_before_locks(true)?;
    relcache_seams::at_eosubxact_relation_cache::call(true, my, parent)?;
    typcache_seams::at_eosubxact_type_cache::call();
    inval::eoxact::AtEOSubXact_Inval(true)?;
    if catalog_storage_seams::at_subcommit_smgr::is_installed() {
        catalog_storage_seams::at_subcommit_smgr::call();
    }

    if xs(|s| s.current().full_transaction_id.is_valid()) {
        let xid = xs(|s| s.current().full_transaction_id.xid());
        lmgr::XactLockTableDelete(xid)?;
    }

    release_subxact_owner_locks(true)?;

    let (guc_nest_level, nesting_level) =
        xs(|s| (s.current().guc_nest_level, s.current().nesting_level));
    guc::AtEOXact_GUC(true, guc_nest_level);
    spi_seams::at_eosubxact_spi::call(true, my)?;
    if tablecmds_seams::at_eosubxact_on_commit_actions::is_installed() {
        tablecmds_seams::at_eosubxact_on_commit_actions::call(true, my, parent);
    }
    namespace_seams::at_eosubxact_namespace::call(true, my, parent);
    fd::AtEOSubXact_Files(true, my, parent);
    // AtEOSubXact_HashTables dissolves.
    pgstat::xact::AtEOSubXact_PgStat(true, nesting_level);
    snapmgr_seams::at_subcommit_snapshot::call(nesting_level);

    xs(|s| {
        s.XactReadOnly = s.current().prev_xact_read_only;
    });

    cleanup_subxact_owner()?;
    xs(|s| s.current_mut().has_resource_owner = false);

    AtSubCommit_Memory()?;

    xs(|s| s.current_mut().state = TRANS_DEFAULT);

    PopTransaction()
}

fn AbortSubTransaction() -> PgResult<()> {
    init_small::globals::HoldInterrupts();

    AtSubAbort_Memory();
    AtSubAbort_ResourceOwner();

    let _ = lwlock::LWLockReleaseAll();

    waitevent::pgstat_report_wait_end();
    // No progress command can start while backend_progress is unported; guarded.
    if backend_progress_seams::pgstat_progress_end_command::is_installed() {
        backend_progress_seams::pgstat_progress_end_command::call();
    }

    aio_seams::pgaio_error_cleanup::call();

    bufmgr::UnlockBuffers();

    xloginsert_seams::xlog_reset_insertion::call();

    let _ = condition_variable_seams::condition_variable_cancel_sleep::call();

    lmgr_proc::LockErrorCleanup()?;

    timeout::reschedule_timeouts();
    libpq_pqsignal::unblock_signals();

    ShowTransactionState("AbortSubTransaction");

    if xs(|s| s.current().state) != TRANS_INPROGRESS {
        warn_internal(&format!(
            "AbortSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }

    xs(|s| s.current_mut().state = TRANS_ABORT);

    let (prev_user, prev_sec) = xs(|s| (s.current().prev_user, s.current().prev_sec_context));
    miscinit::SetUserIdAndSecContext(prev_user, prev_sec);

    if catalog_index_seams::reset_reindex_state::is_installed() {
        catalog_index_seams::reset_reindex_state::call(xs(|s| s.current().nesting_level));
    }

    // No logical decoding can be in progress while reorderbuffer is unported; guarded.
    if logical_seams::reset_logical_streaming_state::is_installed() {
        logical_seams::reset_logical_streaming_state::call();
    }


    let (my, parent) = subxact_ids();
    parallel_seams::at_eosubxact_parallel::call(false, my)?;
    xs(|s| s.current_mut().parallel_mode_level = 0);

    if xs(|s| s.current().has_resource_owner) {
        trigger_seams::after_trigger_end_sub_xact::call(false)?;
        portalmem::AtSubAbort_Portals(
                        my,
                        parent,
                        resowner::CurTransactionResourceOwner(),
                        resowner::ResourceOwnerGetParent(resowner::CurTransactionResourceOwner()),
                    )?;
        if be_fsstubs_seams::at_eosubxact_large_object::is_installed() {
            be_fsstubs_seams::at_eosubxact_large_object::call(false, my, parent)?;
        }
        if async_seams::at_subabort_notify::is_installed() {
            async_seams::at_subabort_notify::call();
        }

        // Advertise the fact that we aborted in pg_xact.
        RecordTransactionAbort(true)?;

        if xs(|s| s.current().full_transaction_id.is_valid()) {
            AtSubAbort_childXids();
        }

        CallSubXactCallbacks(SUBXACT_EVENT_ABORT_SUB, my, parent)?;

        release_subxact_owner_before_locks(false)?;
        aio_seams::at_eoxact_aio::call(false);
        relcache_seams::at_eosubxact_relation_cache::call(false, my, parent)?;
        typcache_seams::at_eosubxact_type_cache::call();
        inval::eoxact::AtEOSubXact_Inval(false)?;
        release_subxact_owner_locks(false)?;
        if catalog_storage_seams::at_subabort_smgr::is_installed() {
            catalog_storage_seams::at_subabort_smgr::call()?;
        }

        let (guc_nest_level, nesting_level) =
            xs(|s| (s.current().guc_nest_level, s.current().nesting_level));
        guc::AtEOXact_GUC(false, guc_nest_level);
        spi_seams::at_eosubxact_spi::call(false, my)?;
        if tablecmds_seams::at_eosubxact_on_commit_actions::is_installed() {
            tablecmds_seams::at_eosubxact_on_commit_actions::call(false, my, parent);
        }
        namespace_seams::at_eosubxact_namespace::call(false, my, parent);
        fd::AtEOSubXact_Files(false, my, parent);
        // AtEOSubXact_HashTables dissolves.
        pgstat::xact::AtEOSubXact_PgStat(false, nesting_level);
        snapmgr_seams::at_subabort_snapshot::call(nesting_level)?;
    }

    xs(|s| s.XactReadOnly = s.current().prev_xact_read_only);

    init_small::globals::ResumeInterrupts();
    Ok(())
}

fn CleanupSubTransaction() -> PgResult<()> {
    ShowTransactionState("CleanupSubTransaction");

    if xs(|s| s.current().state) != TRANS_ABORT {
        warn_internal(&format!(
            "CleanupSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }

    let (my, _parent) = subxact_ids();
    portalmem::AtSubCleanup_Portals(my)?;

    if xs(|s| s.current().has_resource_owner) {
        cleanup_subxact_owner()?;
    }
    xs(|s| s.current_mut().has_resource_owner = false);

    AtSubCleanup_Memory();

    xs(|s| s.current_mut().state = TRANS_DEFAULT);

    PopTransaction()
}

fn PushTransaction() -> PgResult<()> {
    let wrapped = xs(|s| {
        s.current_sub_transaction_id = s.current_sub_transaction_id.wrapping_add(1);
        if s.current_sub_transaction_id == InvalidSubTransactionId {
            s.current_sub_transaction_id = s.current_sub_transaction_id.wrapping_sub(1);
            true
        } else {
            false
        }
    });
    if wrapped {
        return ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("cannot have more than 2^32-1 subtransactions in a transaction")
            .finish(xact_location("PushTransaction"));
    }

    let guc_nest_level = guc::NewGUCNestLevel();
    let (prev_user, prev_sec_context) = miscinit::GetUserIdAndSecContext();

    xs(|s| {
        let parent = s.current();
        let parent_nesting = parent.nesting_level;
        let parent_savepoint = parent.savepoint_level;
        let parent_started_in_recovery = parent.started_in_recovery;
        let parent_parallel_child =
            parent.parallel_mode_level != 0 || parent.parallel_child_xact;
        let subid = s.current_sub_transaction_id;
        let prev_xact_read_only = s.XactReadOnly;

        s.try_push_node(TransactionNode {
            full_transaction_id: InvalidFullTransactionId, // until assigned
            sub_transaction_id: subid,
            name: None,
            savepoint_level: parent_savepoint,
            state: TRANS_DEFAULT,
            block_state: TBLOCK_SUBBEGIN,
            nesting_level: parent_nesting + 1,
            guc_nest_level,
            child_xids: Vec::new(),
            prev_user,
            prev_sec_context,
            prev_xact_read_only,
            started_in_recovery: parent_started_in_recovery,
            did_log_xid: false,
            parallel_mode_level: 0,
            parallel_child_xact: parent_parallel_child,
            chain: false,
            top_xid_logged: false,
            has_resource_owner: false,
            cur_transaction_context: None,
            retained_child_contexts: Vec::new(),
        })
        .map_err(|_| PgError::error("out of memory pushing transaction state"))?;
        Ok(())
    })
}

fn PopTransaction() -> PgResult<()> {
    if xs(|s| s.current().state) != TRANS_DEFAULT {
        warn_internal(&format!(
            "PopTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }
    if xs(|s| s.stack_len()) <= 1 {
        return Err(Box::new(PgError::new(FATAL, "PopTransaction with no parent")));
    }
    xs(|s| s.pop_node());
    Ok(())
}

/// `SerializedTransactionStateHeaderSize`: int + bool(+3 pad) + two 8-aligned
/// FullTransactionIds + CommandId + int.
const SERIALIZED_HEADER_SIZE: usize = 32;

pub fn EstimateTransactionStateSpace() -> usize {
    xs(|s| {
        let mut nxids = 0usize;
        for node in s.nodes() {
            if node.full_transaction_id.is_valid() {
                nxids += 1;
            }
            nxids += node.child_xids.len();
        }
        SERIALIZED_HEADER_SIZE + nxids * std::mem::size_of::<TransactionId>()
    })
}

pub fn SerializeTransactionState(out: &mut [u8]) -> PgResult<usize> {
    let (iso, deferrable, top_full, cur_full, cur_cid, xids) = xs(|s| {
        let xids: Vec<TransactionId> = if !s.parallel_current_xids.is_empty() {
            let mut xids = Vec::new();
            if xids.try_reserve_exact(s.parallel_current_xids.len()).is_err() {
                return Err(PgError::error("out of memory serializing transaction state"));
            }
            xids.extend_from_slice(&s.parallel_current_xids);
            xids
        } else {
            let mut workspace: Vec<TransactionId> = Vec::new();
            for node in s.nodes() {
                let extra =
                    usize::from(node.full_transaction_id.is_valid()) + node.child_xids.len();
                if workspace.try_reserve(extra).is_err() {
                    return Err(PgError::error("out of memory serializing transaction state"));
                }
                if node.full_transaction_id.is_valid() {
                    workspace.push(node.full_transaction_id.xid());
                }
                workspace.extend_from_slice(&node.child_xids);
            }
            workspace.sort_unstable();
            workspace
        };
        Ok((
            s.XactIsoLevel,
            s.XactDeferrable,
            s.top_full_xid(),
            s.current().full_transaction_id,
            s.command_id(),
            xids,
        ))
    })?;

    let total = SERIALIZED_HEADER_SIZE + xids.len() * 4;
    if out.len() < total {
        return Err(Box::new(PgError::error("transaction state buffer is too small")));
    }
    out[0..4].copy_from_slice(&iso.to_ne_bytes());
    out[4] = u8::from(deferrable);
    out[5..8].fill(0);
    out[8..16].copy_from_slice(&top_full.value.to_ne_bytes());
    out[16..24].copy_from_slice(&cur_full.value.to_ne_bytes());
    out[24..28].copy_from_slice(&cur_cid.to_ne_bytes());
    out[28..32].copy_from_slice(&(xids.len() as i32).to_ne_bytes());
    let mut offset = SERIALIZED_HEADER_SIZE;
    for xid in &xids {
        out[offset..offset + 4].copy_from_slice(&xid.to_ne_bytes());
        offset += 4;
    }
    Ok(total)
}

pub fn StartParallelWorkerTransaction(tstatespace: &[u8]) -> PgResult<()> {
    debug_assert_eq!(cur_block_state(), TBLOCK_DEFAULT);
    let xp = xs_ptr();
    StartTransaction(xp)?;

    if tstatespace.len() < SERIALIZED_HEADER_SIZE {
        return Err(Box::new(PgError::error("invalid serialized transaction state")));
    }
    let n_xids = i32::from_ne_bytes(tstatespace[28..32].try_into().unwrap());
    if n_xids < 0 {
        return Err(Box::new(PgError::error("invalid serialized transaction state")));
    }
    let total = SERIALIZED_HEADER_SIZE + n_xids as usize * 4;
    if tstatespace.len() < total {
        return Err(Box::new(PgError::error("invalid serialized transaction state")));
    }
    let mut xids: Vec<TransactionId> = Vec::new();
    xids.try_reserve(n_xids as usize)
        .map_err(|_| PgError::error("out of memory restoring transaction state"))?;
    let mut offset = SERIALIZED_HEADER_SIZE;
    for _ in 0..n_xids {
        xids.push(TransactionId::from_ne_bytes(
            tstatespace[offset..offset + 4].try_into().unwrap(),
        ));
        offset += 4;
    }

    xp.with(|s| {
        s.XactIsoLevel = i32::from_ne_bytes(tstatespace[0..4].try_into().unwrap());
        s.XactDeferrable = tstatespace[4] != 0;
        s.set_top_full_xid(FullTransactionId {
            value: u64::from_ne_bytes(tstatespace[8..16].try_into().unwrap()),
        });
        s.current_mut().full_transaction_id = FullTransactionId {
            value: u64::from_ne_bytes(tstatespace[16..24].try_into().unwrap()),
        };
        s.set_command_id(CommandId::from_ne_bytes(tstatespace[24..28].try_into().unwrap()));
        s.parallel_current_xids = xids;
        s.current_mut().block_state = TBLOCK_PARALLEL_INPROGRESS;
    });
    Ok(())
}

pub fn EndParallelWorkerTransaction() -> PgResult<()> {
    debug_assert_eq!(cur_block_state(), TBLOCK_PARALLEL_INPROGRESS);
    let xp = xs_ptr();
    CommitTransaction(xp)?;
    xp.with(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
    Ok(())
}
