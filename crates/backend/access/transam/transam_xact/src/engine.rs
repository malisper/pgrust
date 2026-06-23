//! The transaction engine: Start/Commit/Prepare/Abort/Cleanup (top-level and
//! sub), the durable Record* routines, the command/block state machines, and
//! the parallel-worker serialize/restore path (xact.c:1315-5641).

use crate::*;

use commit_ts_seams as commit_ts_seams;
use multixact_seams as multixact_seams;
use transam_seams as transam_seams;
use twophase_seams as twophase_seams;
use index_seams as index_seams;
use namespace_seams as namespace_seams;
use pg_enum_seams as pg_enum_seams;
use catalog_storage_seams as storage_seams;
use async_seams as async_seams;
use tablecmds_seams as tablecmds_seams;
use trigger_seams as trigger_seams;
use be_fsstubs_seams as fsstubs_seams;
use launcher_seams as launcher_seams;
use logical_seams as logical_seams;
use origin_seams as origin_seams;
use logical_snapbuild_seams as snapbuild_seams;
use worker_seams as lrworker_seams;
use syncrep_seams as syncrep_seams;
use aio_seams_2 as aio_seams;
use bufmgr_seams as bufmgr_seams;
use fd_seams as fd_seams;
use procarray_seams as procarray_seams;
use standby_seams as standby_seams;
use condition_variable_seams as condvar_seams;
use lock_seams as lock_seams;
use lwlock as lwlock;
use status_seams as status_seams;
use waitevent_seams as waitevent_seams;
use activity_xact_seams as pgstat_xact_seams;
use relcache_seams as relcache_seams;
use typcache_seams as typcache_seams;
use miscinit_seams as miscinit_seams;
use init_small_seams as globals_seams;
use more2_seams as timeout_seams;
use portalmem_seams as portal_seams;
use snapmgr_pc_seams as snapmgr_pc_seams;

use types_core::VirtualTransactionId;
use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;

// ---------------------------------------------------------------------------
//  RecordTransactionCommit (xact.c:1315)
// ---------------------------------------------------------------------------

/// `RecordTransactionCommit` (xact.c:1315). Returns latest XID among the xact
/// and its children, or InvalidTransactionId if the xact has no XID.
fn RecordTransactionCommit() -> PgResult<TransactionId> {
    let xid = GetTopTransactionIdIfAny();
    let mark_xid_committed = xid != InvalidTransactionId;
    #[allow(unused_assignments)]
    let mut latest_xid = InvalidTransactionId;

    // Log pending invalidations for logical decoding of in-progress
    // transactions (covers direct system-table updates without a block).
    if xlog_seams::xlog_logical_info_active::call() {
        inval_seams::log_logical_invalidations::call()?;
    }

    // Get data needed for the commit record (C pallocs in the current
    // context; a local workspace context carries the same lifetime).
    let workspace = MemoryContext::new("RecordTransactionCommit");
    let mcx = workspace.mcx();
    let rels = storage_seams::smgr_get_pending_deletes::call(mcx, true)?;
    let children = xactGetCommittedChildren()?;
    let dropped_stats = pgstat_xact_seams::pgstat_get_transactional_drops::call(mcx, true)?;
    let (inval_msgs, relcache_init_file_inval) = if xlog_seams::xlog_standby_info_active::call() {
        inval_seams::xact_get_committed_invalidation_messages::call(mcx)?
    } else {
        (mcx::PgVec::new_in(mcx), false)
    };
    let mut wrote_xlog = xlog_seams::xact_last_rec_end::call() != 0;

    if !mark_xid_committed {
        // If we haven't been assigned an XID, we neither can nor want to
        // write a COMMIT record. Every RelationDropStorage is expected to be
        // followed by a catalog update (hence XID assignment), so there
        // should be no pending deletes; real test, not just an Assert.
        if !rels.is_empty() || !dropped_stats.is_empty() {
            return Err(PgError::error(
                "cannot commit a transaction that deleted files but has no xid",
            ));
        }
        // Can't have child XIDs either; AssignTransactionId enforces this.
        debug_assert!(children.is_empty());

        // Transactions without an assigned xid can still carry invalidation
        // messages (inplace updates; extensions); emit a bespoke record.
        if !inval_msgs.is_empty() {
            standby_seams::log_standby_invalidations::call(
                mcx,
                &inval_msgs,
                relcache_init_file_inval,
            )?;
            wrote_xlog = true; // not strictly necessary
        }

        // If we didn't create XLOG entries we're done; otherwise flush them
        // the same as a commit record would (HOT pruning and the like).
        if !wrote_xlog {
            return Ok(InvalidTransactionId); // goto cleanup
        }
    } else {
        // Are we using the replication origins feature (replaying remote
        // actions)?
        let session_origin = origin_seams::replorigin_session_origin::call();
        let replorigin =
            session_origin != types_core::InvalidRepOriginId && session_origin != DoNotReplicateId;

        // Mark ourselves as within our "commit critical section": forces any
        // concurrent checkpoint to wait until we've updated pg_xact.
        globals_seams::start_critical_section::call();
        proc_seams::my_proc_set_delay_chkpt_start::call(true);

        // Insert the commit XLOG record.
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
            // Move LSNs forward for this replication origin.
            origin_seams::replorigin_session_advance::call(
                origin_seams::replorigin_session_origin_lsn::call(),
                xlog_seams::xact_last_rec_end::call(),
            )?;
        }

        // Record commit timestamp: plain commit timestamp unless replication
        // already set replorigin_session_origin_timestamp.
        if !replorigin || origin_seams::replorigin_session_origin_timestamp::call() == 0 {
            origin_seams::set_replorigin_session_origin_timestamp::call(
                GetCurrentTransactionStopTimestamp(),
            );
        }

        commit_ts_seams::transaction_tree_set_commit_ts_data::call(
            xid,
            &children,
            origin_seams::replorigin_session_origin_timestamp::call(),
            session_origin,
        )?;
    }

    // Check if we want to commit asynchronously: allowed if
    // synchronous_commit=off or the transaction wrote no WAL / has no xid;
    // forced synchronous for non-temp rel cleanup or ForceSyncCommit.
    if (wrote_xlog && mark_xid_committed && synchronous_commit() > SYNCHRONOUS_COMMIT_OFF)
        || xs(|s| s.force_sync_commit)
        || !rels.is_empty()
    {
        xlog_seams::xlog_flush::call(xlog_seams::xact_last_rec_end::call())?;
        // Now we may update the CLOG, if we wrote a COMMIT record above.
        if mark_xid_committed {
            transam_seams::transaction_id_commit_tree::call(xid, &children)?;
        }
    } else {
        // Asynchronous commit: report the latest async commit LSN so the WAL
        // writer knows to flush it; CLOG update is deferred behind that LSN.
        xlog_seams::xlog_set_async_xact_lsn::call(xlog_seams::xact_last_rec_end::call());
        if mark_xid_committed {
            transam_seams::transaction_id_async_commit_tree::call(
                xid,
                &children,
                xlog_seams::xact_last_rec_end::call(),
            )?;
        }
    }

    // If we entered a commit critical section, leave it now.
    if mark_xid_committed {
        proc_seams::my_proc_set_delay_chkpt_start::call(false);
        globals_seams::end_critical_section::call();
    }

    // Compute latestXid while we have the child XIDs handy.
    latest_xid = transam_seams::transaction_id_latest::call(xid, &children);

    // Wait for synchronous replication if this backend assigned an xid and
    // wrote WAL. (clog is marked, but we still show as running in the
    // procarray and continue to hold locks.)
    if wrote_xlog && mark_xid_committed {
        syncrep_seams::sync_rep_wait_for_lsn::call(xlog_seams::xact_last_rec_end::call(), true)?;
    }

    // remember end of last commit record; reset XactLastRecEnd.
    xlog_seams::set_xact_last_commit_end::call(xlog_seams::xact_last_rec_end::call());
    xlog_seams::set_xact_last_rec_end::call(0);

    Ok(latest_xid)
}

// ---------------------------------------------------------------------------
//  RecordTransactionAbort (xact.c:1754)
// ---------------------------------------------------------------------------

/// `RecordTransactionAbort` (xact.c:1754).
fn RecordTransactionAbort(is_subxact: bool) -> PgResult<TransactionId> {
    let xid = GetCurrentTransactionIdIfAny();

    // If we haven't been assigned an XID, nobody cares whether we aborted.
    if xid == InvalidTransactionId {
        // Reset XactLastRecEnd until the next transaction writes something.
        if !is_subxact {
            xlog_seams::set_xact_last_rec_end::call(0);
        }
        return Ok(InvalidTransactionId);
    }

    // Check that we haven't aborted halfway through RecordTransactionCommit.
    // C reads the TransactionXmin global inside TransactionIdDidCommit; here it
    // is threaded explicitly, so source it from snapmgr.
    let transaction_xmin = snapmgr_pc_seams::transaction_xmin::call()?;
    if transam_seams::transaction_id_did_commit::call(xid, transaction_xmin)? {
        return Err(PgError::new(
            types_error::PANIC,
            format!("cannot abort transaction {xid}, it was already committed"),
        ));
    }

    let session_origin = origin_seams::replorigin_session_origin::call();
    let replorigin =
        session_origin != types_core::InvalidRepOriginId && session_origin != DoNotReplicateId;

    // Fetch the data we need for the abort record.
    let workspace = MemoryContext::new("RecordTransactionAbort");
    let mcx = workspace.mcx();
    let rels = storage_seams::smgr_get_pending_deletes::call(mcx, false)?;
    let children = xactGetCommittedChildren()?;
    let dropped_stats = pgstat_xact_seams::pgstat_get_transactional_drops::call(mcx, false)?;

    globals_seams::start_critical_section::call();

    // Write the ABORT record.
    let xact_time = if is_subxact {
        timestamp_seams::get_current_timestamp::call()
    } else {
        GetCurrentTransactionStopTimestamp()
    };
    let insert_result = crate::wal::XactLogAbortRecord(
        xact_time,
        &children,
        &rels,
        &dropped_stats,
        MyXactFlags(),
        InvalidTransactionId,
        None,
    );

    let result: PgResult<TransactionId> = (|| {
        insert_result?;

        if replorigin {
            // Move LSNs forward for this replication origin.
            origin_seams::replorigin_session_advance::call(
                origin_seams::replorigin_session_origin_lsn::call(),
                xlog_seams::xact_last_rec_end::call(),
            )?;
        }

        // Report the latest async abort LSN, so the WAL writer knows to
        // flush this abort (keeps the streaming-replication backlog short).
        if !is_subxact {
            xlog_seams::xlog_set_async_xact_lsn::call(xlog_seams::xact_last_rec_end::call());
        }

        // Mark the transaction aborted in clog (helpful for
        // XactLockTableWait; OK without flushing the ABORT record).
        transam_seams::transaction_id_abort_tree::call(xid, &children)?;
        Ok(InvalidTransactionId)
    })();

    globals_seams::end_critical_section::call();
    result?;

    // Compute latestXid while we have the child XIDs handy.
    let latest_xid = transam_seams::transaction_id_latest::call(xid, &children);

    // Aborting a subtransaction: immediately remove failed XIDs from
    // PGPROC's cache of running child XIDs.
    if is_subxact {
        procarray_seams::xid_cache_remove_running_xids::call(xid, &children, latest_xid)?;
    }

    // Reset XactLastRecEnd until the next transaction writes something.
    if !is_subxact {
        xlog_seams::set_xact_last_rec_end::call(0);
    }

    Ok(latest_xid)
}

// ---------------------------------------------------------------------------
//  StartTransaction (xact.c:2064)
// ---------------------------------------------------------------------------

/// `StartTransaction` (xact.c:2064)
fn StartTransaction() -> PgResult<()> {
    // Let's just make sure the state stack is empty.
    debug_assert!(xs(|s| s.transaction_stack.len() == 1));
    debug_assert!(!xs(|s| s.xact_top_full_transaction_id.is_valid()));
    debug_assert!(xs(|s| s.current().state == TRANS_DEFAULT));

    xs(|s| {
        s.current_mut().state = TRANS_START;
        s.current_mut().full_transaction_id = InvalidFullTransactionId; // until assigned
    });

    // Determine if statements are logged in this transaction.
    {
        let rate = guc_seams::log_xact_sample_rate::call();
        let sampled =
            rate != 0.0 && (rate == 1.0 || prng::global_prng(|p| p.next_f64()) <= rate);
        xs(|s| s.xact_is_sampled = sampled);
    }

    // initialize current transaction state fields
    // (note: prevXactReadOnly is not used at the outermost level)
    xs(|s| {
        let n = s.current_mut();
        n.nesting_level = 1;
        n.guc_nest_level = 1;
        n.child_xids = Vec::new();
    });

    // Once the current user ID and the security context flags are fetched,
    // both will be properly reset even if transaction startup fails.
    let (prev_user, prev_sec_context) = miscinit_seams::get_user_id_and_sec_context::call();
    debug_assert_eq!(prev_sec_context, 0);
    xs(|s| {
        s.current_mut().prev_user = prev_user;
        s.current_mut().prev_sec_context = prev_sec_context;
    });

    // If recovery is still in progress, mark this transaction as read-only.
    if xlog_seams::recovery_in_progress::call() {
        xs(|s| {
            s.current_mut().started_in_recovery = true;
            s.XactReadOnly = true;
        });
    } else {
        xs(|s| {
            s.current_mut().started_in_recovery = false;
            s.XactReadOnly = s.DefaultXactReadOnly;
        });
    }
    xs(|s| {
        s.XactDeferrable = s.DefaultXactDeferrable;
        s.XactIsoLevel = s.DefaultXactIsoLevel;
        s.force_sync_commit = false;
        s.MyXactFlags = 0;

        // reinitialize within-transaction counters
        s.current_mut().sub_transaction_id = TopSubTransactionId;
        s.current_sub_transaction_id = TopSubTransactionId;
        s.current_command_id = FirstCommandId;
        s.current_command_id_used = false;

        // initialize reported xid accounting
        s.unreported_xids.clear();
        s.current_mut().did_log_xid = false;
    });

    // must initialize resource-management stuff first
    AtStart_Memory();
    AtStart_ResourceOwner()?;

    // Assign a new LocalTransactionId, combine with the proc number to form
    // a virtual transaction id, lock it, and advertise it in the proc array.
    let vxid = VirtualTransactionId {
        procNumber: globals_seams::my_proc_number::call(),
        localTransactionId: sinval_seams::get_next_local_transaction_id::call(),
    };
    lock_seams::virtual_xact_lock_table_insert::call(vxid)?;
    proc_seams::set_my_proc_lxid::call(vxid.localTransactionId);

    // set transaction_timestamp() (a/k/a now()): normally the same as the
    // first command's statement_timestamp(); advance it for transactions
    // started inside nonatomic SPI contexts (procedures); a parallel worker
    // got it via SetParallelStartTimestamps().
    if !parallel_seams::is_parallel_worker() {
        let ts = if !spi_seams::spi_inside_nonatomic_context::call() {
            xs(|s| s.stmt_start_timestamp)
        } else {
            timestamp_seams::get_current_timestamp::call()
        };
        xs(|s| s.xact_start_timestamp = ts);
    } else {
        debug_assert!(xs(|s| s.xact_start_timestamp) != 0);
    }
    status_seams::pgstat_report_xact_timestamp::call(xs(|s| s.xact_start_timestamp));
    // Mark xactStopTimestamp as unset.
    xs(|s| s.xact_stop_timestamp = 0);

    // initialize other subsystems for new transaction
    guc_seams::at_start_guc::call();
    AtStart_Cache()?;
    trigger_seams::after_trigger_begin_xact::call()?;

    // done with start processing, set state to "in progress"
    xs(|s| s.current_mut().state = TRANS_INPROGRESS);

    // Schedule transaction timeout.
    let transaction_timeout = proc_seams::transaction_timeout::call();
    if transaction_timeout > 0 {
        timeout_seams::enable_timeout_after::call(TRANSACTION_TIMEOUT, transaction_timeout)?;
    }

    ShowTransactionState("StartTransaction");
    Ok(())
}

// ---------------------------------------------------------------------------
//  CommitTransaction (xact.c:2228)
// ---------------------------------------------------------------------------

/// `CommitTransaction` (xact.c:2228)
fn CommitTransaction() -> PgResult<()> {
    let is_parallel_worker = cur_block_state() == TBLOCK_PARALLEL_INPROGRESS;

    // Enforce parallel mode restrictions during parallel worker commit.
    if is_parallel_worker {
        EnterParallelMode();
    }

    ShowTransactionState("CommitTransaction");

    if xs(|s| s.current().state) != TRANS_INPROGRESS {
        let st = TransStateAsString(xs(|s| s.current().state));
        warn_internal(&format!("CommitTransaction while in {st} state"));
    }
    debug_assert!(!xs(|s| s.is_subxact()));

    // Pre-commit processing that involves calling user-defined code (closing
    // cursors could queue trigger actions, triggers could open cursors, ...):
    // loop until there's nothing left to do.
    loop {
        // Fire all currently pending deferred triggers.
        trigger_seams::after_trigger_fire_deferred::call()?;

        // Close open portals (converting holdable ones into static portals).
        if !portal_seams::pre_commit_portals::call(false)? {
            break;
        }
    }

    // The remaining actions cannot call any user-defined code; but most of
    // this could still throw an error, switching us to the abort path.
    CallXactCallbacks(if is_parallel_worker {
        XACT_EVENT_PARALLEL_PRE_COMMIT
    } else {
        XACT_EVENT_PRE_COMMIT
    })?;

    // Clean up any unfinished parallel operation's workers, warning about
    // leaked resources. (parallelModeLevel itself resets at TRANS_COMMIT.)
    parallel_seams::at_eoxact_parallel(true)?;
    let level = xs(|s| s.current().parallel_mode_level);
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

    // Shut down the deferred-trigger manager.
    trigger_seams::after_trigger_end_xact::call(true)?;

    // Let ON COMMIT management do its thing (after closing cursors, to avoid
    // dangling-reference problems).
    tablecmds_seams::pre_commit_on_commit_actions::call()?;

    // Synchronize files created and not WAL-logged in this transaction; must
    // precede AtEOXact_RelationMap to avoid committed-but-broken files.
    storage_seams::smgr_do_pending_syncs::call(true, is_parallel_worker)?;

    // close large objects before lower-level cleanup
    fsstubs_seams::at_eoxact_large_object::call(true)?;

    // Insert NOTIFY notifications into the queue (late, to minimize lock
    // hold time; may create a snapshot, so before serializable cleanup).
    async_seams::pre_commit_notify::call()?;

    // Mark serializable transaction as complete for predicate locking — as
    // late as possible while still allowing commit-time failures; not in a
    // parallel worker (the leader's serializable state lives on).
    if !is_parallel_worker {
        predicate_seams::pre_commit_check_for_serialization_failure::call()?;
    }

    // Prevent cancel/die interrupt while cleaning up.
    globals_seams::hold_interrupts::call();

    // Commit updates to the relation map --- do this as late as possible.
    relmapper_seams::at_eoxact_relation_map::call(true, is_parallel_worker)?;

    // set the transaction state information appropriately during commit
    xs(|s| {
        s.current_mut().state = TRANS_COMMIT;
        s.current_mut().parallel_mode_level = 0;
        s.current_mut().parallel_child_xact = false; // should be false already
    });

    // Disable transaction timeout.
    if proc_seams::transaction_timeout::call() > 0 {
        timeout_seams::disable_timeout::call(TRANSACTION_TIMEOUT, false)?;
    }

    let latest_xid = if !is_parallel_worker {
        // Mark our XIDs as committed in pg_xact: this is where we durably
        // commit.
        RecordTransactionCommit()?
    } else {
        // We must not mark our XID committed; the parallel leader does that.
        // But make sure the leader knows about any WAL we wrote.
        parallel_seams::parallel_worker_report_last_rec_end(
            xlog_seams::xact_last_rec_end::call(),
        )?;
        InvalidTransactionId
    };

    // Let others know about no transaction in progress by me: _before_
    // releasing locks and _after_ RecordTransactionCommit.
    procarray_seams::proc_array_end_transaction::call(latest_xid)?;

    // Post-commit cleanup: release resources visible to other backends, then
    // locks, then backend-local resources. (Query-scoped resources are RAII
    // guards; the ResourceOwnerRelease phases dissolve into the owner value.)
    CallXactCallbacks(if is_parallel_worker {
        XACT_EVENT_PARALLEL_COMMIT
    } else {
        XACT_EVENT_COMMIT
    })?;

    // CurrentResourceOwner = NULL;
    // ResourceOwnerRelease(TopTransactionResourceOwner, BEFORE_LOCKS, true, true).
    resowner_seams::reset_current_resource_owner::call();
    resowner_seams::release_transaction_owner_before_locks::call(true)?;

    aio_seams::at_eoxact_aio::call(true);

    // Check we've released all buffer pins.
    bufmgr_seams::at_eoxact_buffers::call(true);

    // Clean up the relation cache.
    relcache_seams::at_eoxact_relation_cache::call(true)?;

    // Clean up the type cache.
    typcache_seams::at_eoxact_type_cache::call();

    // Make catalog changes visible to all backends: after relcache refs are
    // dropped, before locks are released.
    inval_seams::at_eoxact_inval::call(true)?;

    multixact_seams::at_eoxact_multixact::call();

    // ResourceOwnerRelease(TopTransactionResourceOwner, LOCKS, true, true);
    // ResourceOwnerRelease(TopTransactionResourceOwner, AFTER_LOCKS, true, true).
    resowner_seams::release_transaction_owner_locks::call(true)?;

    // Drop files deleted during the transaction (after releasing relcache
    // and buffer pins, and after releasing locks).
    storage_seams::smgr_do_pending_deletes::call(true)?;

    // Send out notification signals to other backends; not until our
    // transaction is fully done from their viewpoint.
    async_seams::at_commit_notify::call()?;

    // Everything after this is purely internal-to-this-backend cleanup.
    guc_core_seams::at_eoxact_guc::call(true, 1)?;
    spi_seams::at_eoxact_spi::call(true)?;
    pg_enum_seams::at_eoxact_enum::call();
    tablecmds_seams::at_eoxact_on_commit_actions::call(true);
    namespace_seams::at_eoxact_namespace::call(true, is_parallel_worker);
    smgr_seams::at_eoxact_smgr::call();
    fd_seams::at_eoxact_files::call(true);
    combocid_seams::at_eoxact_combocid::call();
    // AtEOXact_HashTables dissolves (see crate docs).
    pgstat_xact_seams::at_eoxact_pgstat::call(true, is_parallel_worker);
    snapmgr_seams::at_eoxact_snapshot::call(true, false)?;
    launcher_seams::at_eoxact_apply_launcher::call(true);
    lrworker_seams::at_eoxact_logical_rep_workers::call(true);
    status_seams::pgstat_report_xact_timestamp::call(0);

    // ResourceOwnerDelete(TopTransactionResourceOwner);
    // CurTransactionResourceOwner = NULL; TopTransactionResourceOwner = NULL.
    resowner_seams::delete_transaction_owner::call()?;
    xs(|s| s.current_mut().has_resource_owner = false);

    AtCommit_Memory();

    xs(|s| {
        let n = s.current_mut();
        n.full_transaction_id = InvalidFullTransactionId;
        n.sub_transaction_id = InvalidSubTransactionId;
        n.nesting_level = 0;
        n.guc_nest_level = 0;
        n.child_xids = Vec::new();
        s.xact_top_full_transaction_id = InvalidFullTransactionId;
        s.parallel_current_xids = Vec::new();
        // done with commit processing, set state back to default
        s.current_mut().state = TRANS_DEFAULT;
    });

    globals_seams::resume_interrupts::call();
    Ok(())
}

// ---------------------------------------------------------------------------
//  PrepareTransaction (xact.c:2515)
// ---------------------------------------------------------------------------

/// `PrepareTransaction` (xact.c:2515)
fn PrepareTransaction() -> PgResult<()> {
    let xid = GetCurrentTransactionId()?;
    debug_assert!(!IsInParallelMode());

    ShowTransactionState("PrepareTransaction");

    if xs(|s| s.current().state) != TRANS_INPROGRESS {
        let st = TransStateAsString(xs(|s| s.current().state));
        warn_internal(&format!("PrepareTransaction while in {st} state"));
    }
    debug_assert!(!xs(|s| s.is_subxact()));

    // Pre-commit processing that involves calling user-defined code.
    loop {
        trigger_seams::after_trigger_fire_deferred::call()?;
        if !portal_seams::pre_commit_portals::call(true)? {
            break;
        }
    }

    CallXactCallbacks(XACT_EVENT_PRE_PREPARE)?;

    // Shut down the deferred-trigger manager.
    trigger_seams::after_trigger_end_xact::call(true)?;

    // Let ON COMMIT management do its thing.
    tablecmds_seams::pre_commit_on_commit_actions::call()?;

    // Synchronize files created and not WAL-logged; before EndPrepare().
    storage_seams::smgr_do_pending_syncs::call(true, false)?;

    // close large objects before lower-level cleanup
    fsstubs_seams::at_eoxact_large_object::call(true)?;

    // NOTIFY requires no work at this point

    // Mark serializable transaction as complete for predicate locking.
    predicate_seams::pre_commit_check_for_serialization_failure::call()?;

    // Don't allow PREPARE TRANSACTION if we've accessed a temporary table in
    // this transaction (checked after ON COMMIT actions, which might still
    // access a temp relation).
    if (MyXactFlags() & XACT_FLAGS_ACCESSEDTEMPNAMESPACE) != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot PREPARE a transaction that has operated on temporary objects")
            .finish(xact_location("PrepareTransaction"));
    }

    // Likewise, don't allow PREPARE after pg_export_snapshot.
    if snapmgr_seams::xact_has_exported_snapshots::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot PREPARE a transaction that has exported snapshots")
            .finish(xact_location("PrepareTransaction"));
    }

    // Prevent cancel/die interrupt while cleaning up.
    globals_seams::hold_interrupts::call();

    xs(|s| s.current_mut().state = TRANS_PREPARE);

    // Disable transaction timeout.
    if proc_seams::transaction_timeout::call() > 0 {
        timeout_seams::disable_timeout::call(TRANSACTION_TIMEOUT, false)?;
    }

    let prepared_at = timestamp_seams::get_current_timestamp::call();

    // Reserve the GID for this transaction (fails if invalid or in use).
    let gid = xs(|s| s.prepare_gid.take())
        .ok_or_else(|| PgError::error("PrepareTransaction: no prepared-transaction GID set"))?;
    let databaseid = globals_seams::my_database_id::call();
    twophase_seams::mark_as_preparing::call(
        xid,
        &gid,
        prepared_at,
        miscinit_seams::get_user_id::call(),
        databaseid,
    )?;

    // Collect data for the 2PC state file (C `StartPrepare` reads these from
    // the current backend transaction); the order of the file segments — and
    // thus the replay order at COMMIT/ROLLBACK PREPARED — must match the calls
    // that follow. Allocated in a local workspace (C: palloc in the caller's
    // context).
    let prep_ws = MemoryContext::new("StartPrepare");
    let prep_mcx = prep_ws.mcx();
    let commitrels = storage_seams::smgr_get_pending_deletes::call(prep_mcx, true)?;
    let abortrels = storage_seams::smgr_get_pending_deletes::call(prep_mcx, false)?;
    let children = xactGetCommittedChildren()?;
    let commitstats = pgstat_xact_seams::pgstat_get_transactional_drops::call(prep_mcx, true)?;
    let abortstats = pgstat_xact_seams::pgstat_get_transactional_drops::call(prep_mcx, false)?;
    let (invalmsgs, initfileinval) =
        inval_seams::xact_get_committed_invalidation_messages::call(prep_mcx)?;

    let start_args = twophase_seams::StartPrepareArgs {
        xid,
        gid: gid.clone(),
        prepared_at,
        owner: miscinit_seams::get_user_id::call(),
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
    lock_seams::at_prepare_locks::call()?;
    predicate_seams::at_prepare_predicate_locks::call()?;
    pgstat_xact_seams::at_prepare_pgstat::call()?;
    multixact_seams::at_prepare_multixact::call()?;
    relmapper_seams::at_prepare_relation_map::call()?;

    // Here is where we really truly prepare.
    twophase_seams::end_prepare::call()?;

    // Now clean up backend-internal state and release internal resources.

    // Reset XactLastRecEnd until the next transaction writes something.
    xlog_seams::set_xact_last_rec_end::call(0);

    // Transfer our locks to a dummy PGPROC; before
    // ProcArrayClearTransaction so GetLockConflicts can't conclude "xact
    // already committed or aborted" for our locks.
    lock_seams::post_prepare_locks::call(xid)?;

    // Let others know about no transaction in progress by me: only after the
    // prepared transaction has been marked valid.
    procarray_seams::proc_array_clear_transaction::call()?;

    // Per-backend resources transfer to the prepared transaction's PGPROC;
    // too late to abort if an error is raised here.
    CallXactCallbacks(XACT_EVENT_PREPARE)?;

    // ResourceOwnerRelease(TopTransactionResourceOwner, BEFORE_LOCKS, true, true)
    // (xact.c PrepareTransaction): releases the transaction's buffer pins (and
    // other before-lock resources) before AtEOXact_Buffers asserts they are all
    // gone. Unlike Commit/Abort, Prepare does NOT reset CurrentResourceOwner
    // here (it clears it at the tail, with the delete).
    resowner_seams::release_transaction_owner_before_locks::call(true)?;

    aio_seams::at_eoxact_aio::call(true);

    // Check we've released all buffer pins.
    bufmgr_seams::at_eoxact_buffers::call(true);

    // Clean up the relation cache.
    relcache_seams::at_eoxact_relation_cache::call(true)?;

    // Clean up the type cache.
    typcache_seams::at_eoxact_type_cache::call();

    // notify doesn't need a postprepare call

    pgstat_xact_seams::post_prepare_pgstat::call();

    inval_seams::post_prepare_inval::call();

    storage_seams::post_prepare_smgr::call();

    multixact_seams::post_prepare_multixact::call(xid);

    predicate_seams::post_prepare_predicate_locks::call(xid)?;

    // ResourceOwnerRelease(TopTransactionResourceOwner, LOCKS, true, true) +
    // (AFTER_LOCKS, true, true) — xact.c PrepareTransaction.
    resowner_seams::release_transaction_owner_locks::call(true)?;

    // Allow another backend to finish the transaction; after this the
    // transaction is completely detached from our backend.
    twophase_seams::post_prepare_twophase::call();

    // PREPARE acts the same as COMMIT as far as GUC is concerned.
    guc_core_seams::at_eoxact_guc::call(true, 1)?;
    spi_seams::at_eoxact_spi::call(true)?;
    pg_enum_seams::at_eoxact_enum::call();
    tablecmds_seams::at_eoxact_on_commit_actions::call(true);
    namespace_seams::at_eoxact_namespace::call(true, false);
    smgr_seams::at_eoxact_smgr::call();
    fd_seams::at_eoxact_files::call(true);
    combocid_seams::at_eoxact_combocid::call();
    // AtEOXact_HashTables dissolves.
    // don't call AtEOXact_PgStat here; we fixed pgstat state above.
    snapmgr_seams::at_eoxact_snapshot::call(true, true)?;
    // we treat PREPARE as ROLLBACK so far as waking workers goes
    launcher_seams::at_eoxact_apply_launcher::call(false);
    lrworker_seams::at_eoxact_logical_rep_workers::call(false);
    status_seams::pgstat_report_xact_timestamp::call(0);

    // CurrentResourceOwner = NULL; ResourceOwnerDelete(TopTransactionResourceOwner);
    // s->curTransactionOwner = NULL; CurTransactionResourceOwner = NULL;
    // TopTransactionResourceOwner = NULL (xact.c PrepareTransaction). Prepare
    // clears CurrentResourceOwner here (Commit/Abort do it earlier).
    resowner_seams::reset_current_resource_owner::call();
    resowner_seams::delete_transaction_owner::call()?;
    xs(|s| s.current_mut().has_resource_owner = false);

    AtCommit_Memory();

    xs(|s| {
        let n = s.current_mut();
        n.full_transaction_id = InvalidFullTransactionId;
        n.sub_transaction_id = InvalidSubTransactionId;
        n.nesting_level = 0;
        n.guc_nest_level = 0;
        n.child_xids = Vec::new();
        s.xact_top_full_transaction_id = InvalidFullTransactionId;
        s.parallel_current_xids = Vec::new();
        s.current_mut().state = TRANS_DEFAULT;
    });

    globals_seams::resume_interrupts::call();
    Ok(())
}

// ---------------------------------------------------------------------------
//  AbortTransaction / CleanupTransaction (xact.c:2809 / 3009)
// ---------------------------------------------------------------------------

/// `AbortTransaction` (xact.c:2809)
fn AbortTransaction() -> PgResult<()> {
    // Prevent cancel/die interrupt while cleaning up.
    globals_seams::hold_interrupts::call();

    // Disable transaction timeout.
    if proc_seams::transaction_timeout::call() > 0 {
        timeout_seams::disable_timeout::call(TRANSACTION_TIMEOUT, false)?;
    }

    // Make sure we have a valid memory context and resource owner.
    AtAbort_Memory();
    AtAbort_ResourceOwner();

    // Release any LW locks we might be holding as quickly as possible
    // (regular locks are held till we finish aborting). The abort path
    // swallows the release error, as C's error-recovery LWLockReleaseAll does.
    let _ = lwlock::LWLockReleaseAll();

    // Clear wait information and command progress indicator.
    waitevent_seams::pgstat_report_wait_end::call();
    activity_small::backend_progress::pgstat_progress_end_command();

    aio_seams::pgaio_error_cleanup::call();

    // Clean up buffer content locks, too.
    bufmgr_seams::unlock_buffers::call();

    // Reset WAL record construction state.
    xloginsert_seams::xlog_reset_insertion::call();

    // Cancel condition variable sleep.
    let _ = condvar_seams::condition_variable_cancel_sleep::call();

    // Clean up any open wait for lock (the lock manager would choke on a new
    // wait otherwise).
    proc_seams::lock_error_cleanup::call();

    // If any timeout events are still active, make sure the timeout
    // interrupt is scheduled (after LockErrorCleanup, to avoid uselessly
    // rescheduling lock/deadlock timeouts).
    timeout_seams::reschedule_timeouts::call()?;

    // Re-enable signals, in case we got here by longjmp'ing out of a signal
    // handler.
    {
        let masks = libpq_pqsignal::signal_masks();
        unsafe {
            libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), std::ptr::null_mut());
        }
    }

    // check the current transaction state
    let is_parallel_worker = cur_block_state() == TBLOCK_PARALLEL_INPROGRESS;
    let st = xs(|s| s.current().state);
    if st != TRANS_INPROGRESS && st != TRANS_PREPARE {
        warn_internal(&format!(
            "AbortTransaction while in {} state",
            TransStateAsString(st)
        ));
    }
    debug_assert!(!xs(|s| s.is_subxact()));

    xs(|s| s.current_mut().state = TRANS_ABORT);

    // Reset user ID which might have been changed transiently (SECURITY
    // DEFINER escape); restore SecurityRestrictionContext too.
    let (prev_user, prev_sec) = xs(|s| (s.current().prev_user, s.current().prev_sec_context));
    miscinit_seams::set_user_id_and_sec_context::call(prev_user, prev_sec);

    // Forget about any active REINDEX.
    index_seams::reset_reindex_state::call(xs(|s| s.current().nesting_level));

    // Reset logical streaming state.
    logical_seams::reset_logical_streaming_state::call();

    // Reset snapshot export state.
    snapbuild_seams::snap_build_reset_exported_snapshot_state::call();

    // Clean up any unfinished parallel operation and exit parallel mode;
    // don't warn about leaked resources.
    parallel_seams::at_eoxact_parallel(false)?;
    xs(|s| {
        s.current_mut().parallel_mode_level = 0;
        s.current_mut().parallel_child_xact = false; // should be false already
    });

    // do abort processing
    trigger_seams::after_trigger_end_xact::call(false)?;
    portal_seams::at_abort_portals::call()?;
    storage_seams::smgr_do_pending_syncs::call(false, is_parallel_worker)?;
    fsstubs_seams::at_eoxact_large_object::call(false)?;
    async_seams::at_abort_notify::call()?;
    relmapper_seams::at_eoxact_relation_map::call(false, is_parallel_worker)?;
    twophase_seams::at_abort_twophase::call();

    // Advertise the fact that we aborted in pg_xact (if we got an XID);
    // inside a parallel worker the leader writes the abort record, so just
    // nudge the WAL writer.
    let latest_xid = if !is_parallel_worker {
        RecordTransactionAbort(false)?
    } else {
        xlog_seams::xlog_set_async_xact_lsn::call(xlog_seams::xact_last_rec_end::call());
        InvalidTransactionId
    };

    // Let others know about no transaction in progress by me: _before_
    // releasing locks and _after_ RecordTransactionAbort.
    procarray_seams::proc_array_end_transaction::call(latest_xid)?;

    // Post-abort cleanup; skippable if the transaction failed before
    // creating a resource owner.
    if xs(|s| s.current().has_resource_owner) {
        CallXactCallbacks(if is_parallel_worker {
            XACT_EVENT_PARALLEL_ABORT
        } else {
            XACT_EVENT_ABORT
        })?;

        // ResourceOwnerRelease(TopTransactionResourceOwner, BEFORE_LOCKS, true, false).
        resowner_seams::release_transaction_owner_before_locks::call(false)?;
        aio_seams::at_eoxact_aio::call(false);
        bufmgr_seams::at_eoxact_buffers::call(false);
        relcache_seams::at_eoxact_relation_cache::call(false)?;
        typcache_seams::at_eoxact_type_cache::call();
        inval_seams::at_eoxact_inval::call(false)?;
        multixact_seams::at_eoxact_multixact::call();
        // ResourceOwnerRelease(LOCKS, true, false); ResourceOwnerRelease(AFTER_LOCKS, true, false).
        resowner_seams::release_transaction_owner_locks::call(false)?;
        storage_seams::smgr_do_pending_deletes::call(false)?;

        guc_core_seams::at_eoxact_guc::call(false, 1)?;
        spi_seams::at_eoxact_spi::call(false)?;
        pg_enum_seams::at_eoxact_enum::call();
        tablecmds_seams::at_eoxact_on_commit_actions::call(false);
        namespace_seams::at_eoxact_namespace::call(false, is_parallel_worker);
        smgr_seams::at_eoxact_smgr::call();
        fd_seams::at_eoxact_files::call(false);
        combocid_seams::at_eoxact_combocid::call();
        // AtEOXact_HashTables dissolves.
        pgstat_xact_seams::at_eoxact_pgstat::call(false, is_parallel_worker);
        launcher_seams::at_eoxact_apply_launcher::call(false);
        lrworker_seams::at_eoxact_logical_rep_workers::call(false);
        status_seams::pgstat_report_xact_timestamp::call(0);
    }

    // State remains TRANS_ABORT until CleanupTransaction().
    globals_seams::resume_interrupts::call();
    Ok(())
}

/// `CleanupTransaction` (xact.c:3009)
fn CleanupTransaction() -> PgResult<()> {
    // State should still be TRANS_ABORT from AbortTransaction().
    if xs(|s| s.current().state) != TRANS_ABORT {
        return Err(PgError::new(
            FATAL,
            format!(
                "CleanupTransaction: unexpected state {}",
                TransStateAsString(xs(|s| s.current().state))
            ),
        ));
    }

    // do abort cleanup processing
    portal_seams::at_cleanup_portals::call()?; // now safe to release portal memory
    snapmgr_seams::at_eoxact_snapshot::call(false, true)?; // release the transaction's snapshots

    // CurrentResourceOwner = NULL;
    // if (TopTransactionResourceOwner) ResourceOwnerDelete(TopTransactionResourceOwner);
    // CurTransactionResourceOwner = NULL; TopTransactionResourceOwner = NULL.
    resowner_seams::reset_current_resource_owner::call();
    resowner_seams::delete_transaction_owner::call()?;
    xs(|s| s.current_mut().has_resource_owner = false);

    AtCleanup_Memory(); // and transaction memory

    xs(|s| {
        let n = s.current_mut();
        n.full_transaction_id = InvalidFullTransactionId;
        n.sub_transaction_id = InvalidSubTransactionId;
        n.nesting_level = 0;
        n.guc_nest_level = 0;
        n.child_xids = Vec::new();
        n.parallel_mode_level = 0;
        n.parallel_child_xact = false;
        s.xact_top_full_transaction_id = InvalidFullTransactionId;
        s.parallel_current_xids = Vec::new();
        // done with abort processing, set state back to default
        s.current_mut().state = TRANS_DEFAULT;
    });
    Ok(())
}

// ---------------------------------------------------------------------------
//  StartTransactionCommand / Save+Restore characteristics (xact.c:3059-3151)
// ---------------------------------------------------------------------------

/// `StartTransactionCommand` (xact.c:3059)
pub fn StartTransactionCommand() -> PgResult<()> {
    match cur_block_state() {
        // not in a transaction block: do our usual start transaction.
        TBLOCK_DEFAULT => {
            StartTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_STARTED);
        }

        // Somewhere in a transaction block or subtransaction, starting a new
        // command: nothing to do (CommandCounterIncrement happened in the
        // previous CommitTransactionCommand).
        TBLOCK_INPROGRESS | TBLOCK_IMPLICIT_INPROGRESS | TBLOCK_SUBINPROGRESS => {}

        // In a failed transaction block: remain in the abort state until a
        // ROLLBACK gets us out of it.
        TBLOCK_ABORT | TBLOCK_SUBABORT => {}

        // These cases are invalid.
        other => {
            return Err(PgError::new(
                ERROR,
                format!(
                    "StartTransactionCommand: unexpected state {}",
                    BlockStateAsString(other)
                ),
            ));
        }
    }
    // (C: switch to CurTransactionContext before returning — no ambient
    // context here.)
    Ok(())
}

/// `SaveTransactionCharacteristics` (xact.c:3136)
pub fn SaveTransactionCharacteristics() -> SavedTransactionCharacteristics {
    xs(|s| SavedTransactionCharacteristics {
        save_XactIsoLevel: s.XactIsoLevel,
        save_XactReadOnly: s.XactReadOnly,
        save_XactDeferrable: s.XactDeferrable,
    })
}

/// `RestoreTransactionCharacteristics` (xact.c:3144)
pub fn RestoreTransactionCharacteristics(saved: SavedTransactionCharacteristics) {
    xs(|s| {
        s.XactIsoLevel = saved.save_XactIsoLevel;
        s.XactReadOnly = saved.save_XactReadOnly;
        s.XactDeferrable = saved.save_XactDeferrable;
    });
}

// ---------------------------------------------------------------------------
//  CommitTransactionCommand (xact.c:3157) + the internal state machine
// ---------------------------------------------------------------------------

/// `CommitTransactionCommand` (xact.c:3157)
pub fn CommitTransactionCommand() -> PgResult<()> {
    while !CommitTransactionCommandInternal()? {}
    Ok(())
}

/// `CommitTransactionCommandInternal` (xact.c:3175) — one iteration; returns
/// false when the loop must run again (C's `return false` arms).
fn CommitTransactionCommandInternal() -> PgResult<bool> {
    // Repeatedly saved by the chain cases; cheap enough to save always.
    let savetc = SaveTransactionCharacteristics();

    match cur_block_state() {
        // These shouldn't happen: someone forgot StartTransactionCommand.
        TBLOCK_DEFAULT | TBLOCK_PARALLEL_INPROGRESS => {
            return Err(PgError::new(
                FATAL,
                format!(
                    "CommitTransactionCommand: unexpected state {}",
                    BlockStateAsString(cur_block_state())
                ),
            ));
        }

        // Not in a transaction block: shut down the whole transaction.
        TBLOCK_STARTED => {
            CommitTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // BEGIN received: enter the in-progress state.
        TBLOCK_BEGIN => {
            xs(|s| s.current_mut().block_state = TBLOCK_INPROGRESS);
        }

        // Completed a command in a live (sub)transaction block: increment
        // the command counter and stay in the same state.
        TBLOCK_INPROGRESS | TBLOCK_IMPLICIT_INPROGRESS | TBLOCK_SUBINPROGRESS => {
            CommandCounterIncrement()?;
        }

        // COMMIT received: commit, return to default; chain if requested.
        TBLOCK_END => {
            CommitTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            if xs(|s| s.current().chain) {
                StartTransaction()?;
                xs(|s| {
                    s.current_mut().block_state = TBLOCK_INPROGRESS;
                    s.current_mut().chain = false;
                });
                RestoreTransactionCharacteristics(savetc);
            }
        }

        // Aborted block: stay in the abort state until ROLLBACK.
        TBLOCK_ABORT | TBLOCK_SUBABORT => {}

        // ROLLBACK received in an already-aborted block: clean up.
        TBLOCK_ABORT_END => {
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            if xs(|s| s.current().chain) {
                StartTransaction()?;
                xs(|s| {
                    s.current_mut().block_state = TBLOCK_INPROGRESS;
                    s.current_mut().chain = false;
                });
                RestoreTransactionCharacteristics(savetc);
            }
        }

        // ROLLBACK received in a live block: abort + clean up.
        TBLOCK_ABORT_PENDING => {
            AbortTransaction()?;
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            if xs(|s| s.current().chain) {
                StartTransaction()?;
                xs(|s| {
                    s.current_mut().block_state = TBLOCK_INPROGRESS;
                    s.current_mut().chain = false;
                });
                RestoreTransactionCharacteristics(savetc);
            }
        }

        // PREPARE received.
        TBLOCK_PREPARE => {
            PrepareTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // Just completed a SAVEPOINT: start the subtransaction.
        TBLOCK_SUBBEGIN => {
            StartSubTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_SUBINPROGRESS);
        }

        // RELEASE: commit subtransactions up to (and including) the target.
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

        // COMMIT: pop all open subtransactions, then finish the main xact.
        TBLOCK_SUBCOMMIT => {
            loop {
                CommitSubTransaction()?;
                if cur_block_state() != TBLOCK_SUBCOMMIT {
                    break;
                }
            }
            match cur_block_state() {
                TBLOCK_END => {
                    CommitTransaction()?;
                    xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
                    if xs(|s| s.current().chain) {
                        StartTransaction()?;
                        xs(|s| {
                            s.current_mut().block_state = TBLOCK_INPROGRESS;
                            s.current_mut().chain = false;
                        });
                        RestoreTransactionCharacteristics(savetc);
                    }
                }
                TBLOCK_PREPARE => {
                    PrepareTransaction()?;
                    xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
                }
                other => {
                    return Err(PgError::new(
                        ERROR,
                        format!(
                            "CommitTransactionCommand: unexpected state {}",
                            BlockStateAsString(other)
                        ),
                    ));
                }
            }
        }

        // Failed subtransaction with ROLLBACK TO eventually: clean up and
        // loop (C `return false`).
        TBLOCK_SUBABORT_END => {
            CleanupSubTransaction()?;
            return Ok(false);
        }

        // RELEASE/ROLLBACK of a live subtransaction pending abort.
        TBLOCK_SUBABORT_PENDING => {
            AbortSubTransaction()?;
            CleanupSubTransaction()?;
            return Ok(false);
        }

        // ROLLBACK TO: abort + pop the live subtransaction, then restart it.
        TBLOCK_SUBRESTART => {
            let (name, savepoint_level) = xs(|s| {
                (
                    s.current_mut().name.take(),
                    s.current().savepoint_level,
                )
            });
            AbortSubTransaction()?;
            CleanupSubTransaction()?;
            DefineSavepoint(None)?;
            xs(|s| {
                s.current_mut().name = name;
                s.current_mut().savepoint_level = savepoint_level;
            });
            debug_assert_eq!(cur_block_state(), TBLOCK_SUBBEGIN);
            StartSubTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_SUBINPROGRESS);
        }

        // Same as above, but the subtransaction had already failed.
        TBLOCK_SUBABORT_RESTART => {
            let (name, savepoint_level) = xs(|s| {
                (
                    s.current_mut().name.take(),
                    s.current().savepoint_level,
                )
            });
            CleanupSubTransaction()?;
            DefineSavepoint(None)?;
            xs(|s| {
                s.current_mut().name = name;
                s.current_mut().savepoint_level = savepoint_level;
            });
            debug_assert_eq!(cur_block_state(), TBLOCK_SUBBEGIN);
            StartSubTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_SUBINPROGRESS);
        } // C's elog(FATAL, "...unexpected state...") default arm is
          // statically unreachable: the match is exhaustive over TBlockState.
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
//  AbortCurrentTransaction (xact.c:3451) + the internal state machine
// ---------------------------------------------------------------------------

/// `AbortCurrentTransaction` (xact.c:3451)
pub fn AbortCurrentTransaction() -> PgResult<()> {
    while !AbortCurrentTransactionInternal()? {}
    Ok(())
}

/// `AbortCurrentTransactionInternal` (xact.c:3469)
fn AbortCurrentTransactionInternal() -> PgResult<bool> {
    match cur_block_state() {
        TBLOCK_DEFAULT => {
            if xs(|s| s.current().state) == TRANS_DEFAULT {
                // we are idle, so nothing to do
            } else {
                // We can get here after an error during transaction start
                // (TRANS_START): adjust the low-level state to suppress the
                // warning from AbortTransaction.
                if xs(|s| s.current().state) == TRANS_START {
                    xs(|s| s.current_mut().state = TRANS_INPROGRESS);
                }
                AbortTransaction()?;
                CleanupTransaction()?;
            }
        }

        // Not in a transaction block / implicit block: abort the whole
        // transaction and return to default.
        TBLOCK_STARTED | TBLOCK_IMPLICIT_INPROGRESS => {
            AbortTransaction()?;
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // BEGIN itself failed.
        TBLOCK_BEGIN => {
            AbortTransaction()?;
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // Failure in a live transaction block: abort, await ROLLBACK.
        TBLOCK_INPROGRESS | TBLOCK_PARALLEL_INPROGRESS => {
            AbortTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT);
            // CleanupTransaction happens when we exit TBLOCK_ABORT_END
        }

        // COMMIT failed: abort and return to default.
        TBLOCK_END => {
            AbortTransaction()?;
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // Error while in an aborted block: nothing more to do.
        TBLOCK_ABORT | TBLOCK_SUBABORT => {}

        // ROLLBACK failed after AbortTransaction had run: just clean up.
        TBLOCK_ABORT_END => {
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // ROLLBACK itself failed mid-flight.
        TBLOCK_ABORT_PENDING => {
            AbortTransaction()?;
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // PREPARE failed.
        TBLOCK_PREPARE => {
            AbortTransaction()?;
            CleanupTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
        }

        // Error in a live subtransaction: abort it, await ROLLBACK.
        TBLOCK_SUBINPROGRESS => {
            AbortSubTransaction()?;
            xs(|s| s.current_mut().block_state = TBLOCK_SUBABORT);
        }

        // Failure while completing a subtransaction operation: abort + pop,
        // then loop to deal with the parent (C `return false`).
        TBLOCK_SUBBEGIN | TBLOCK_SUBRELEASE | TBLOCK_SUBCOMMIT | TBLOCK_SUBABORT_PENDING
        | TBLOCK_SUBRESTART => {
            AbortSubTransaction()?;
            CleanupSubTransaction()?;
            return Ok(false);
        }

        // As above, but AbortSubTransaction already ran.
        TBLOCK_SUBABORT_END | TBLOCK_SUBABORT_RESTART => {
            CleanupSubTransaction()?;
            return Ok(false);
        } // C's elog(FATAL, "...unexpected state...") default arm is
          // statically unreachable: the match is exhaustive over TBlockState.
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
//  Transaction-block support (xact.c:3924-4365)
// ---------------------------------------------------------------------------

/// `BeginTransactionBlock` (xact.c:3924)
pub fn BeginTransactionBlock() -> PgResult<()> {
    match cur_block_state() {
        // Not in a block: set it to enter one.
        TBLOCK_STARTED | TBLOCK_IMPLICIT_INPROGRESS => {
            xs(|s| s.current_mut().block_state = TBLOCK_BEGIN);
            Ok(())
        }
        // Already in a block (or subtransaction, or failed block): warn.
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

/// `PrepareTransactionBlock` (xact.c:3992) — returns true if the PREPARE was
/// scheduled, false if it degraded to a plain commit.
pub fn PrepareTransactionBlock(gid: &str) -> PgResult<bool> {
    // Set up to commit the transaction.
    let mut result = EndTransactionBlock(false)?;

    if result {
        // its the topmost transaction state that carries the END/PREPARE mark
        let top_state = xs(|s| s.transaction_stack[0].block_state);
        if top_state == TBLOCK_END {
            // Save GID where PrepareTransaction can find it.
            // (C: MemoryContextStrdup(TopTransactionContext, gid))
            let gid = try_strdup(gid, "out of memory saving prepared-transaction GID")?;
            xs(|s| {
                s.prepare_gid = Some(gid);
                s.transaction_stack[0].block_state = TBLOCK_PREPARE;
            });
        } else {
            // Ignore case where we are not in a transaction:
            // EndTransactionBlock already issued a warning.
            debug_assert!(matches!(
                top_state,
                TBLOCK_STARTED | TBLOCK_IMPLICIT_INPROGRESS
            ));
            // Don't send back a PREPARE result tag.
            result = false;
        }
    }
    Ok(result)
}

/// `EndTransactionBlock` (xact.c:4044) — returns true if a commit is
/// scheduled, false if the block must abort instead.
pub fn EndTransactionBlock(chain: bool) -> PgResult<bool> {
    let mut result = false;
    match cur_block_state() {
        // In a block: set state so CommitTransactionCommand commits.
        TBLOCK_INPROGRESS => {
            xs(|s| s.current_mut().block_state = TBLOCK_END);
            result = true;
        }

        // In an implicit block: same, but warn (or error for AND CHAIN).
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

        // Failed block: COMMIT works like ROLLBACK here.
        TBLOCK_ABORT => {
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT_END);
        }

        // Live subtransactions: subcommit them all, then commit main.
        TBLOCK_SUBINPROGRESS => {
            let bad: Option<TBlockState> = xs(|s| {
                let last = s.transaction_stack.len() - 1;
                for i in (1..=last).rev() {
                    if s.transaction_stack[i].block_state == TBLOCK_SUBINPROGRESS {
                        s.transaction_stack[i].block_state = TBLOCK_SUBCOMMIT;
                    } else {
                        return Some(s.transaction_stack[i].block_state);
                    }
                }
                if s.transaction_stack[0].block_state == TBLOCK_INPROGRESS {
                    s.transaction_stack[0].block_state = TBLOCK_END;
                    None
                } else {
                    Some(s.transaction_stack[0].block_state)
                }
            });
            if let Some(bs) = bad {
                return Err(unexpected_block_state("EndTransactionBlock", bs));
            }
            result = true;
        }

        // Failed subtransaction: abort everything, exit the main xact too.
        TBLOCK_SUBABORT => {
            let bad: Option<TBlockState> = xs(|s| {
                let last = s.transaction_stack.len() - 1;
                for i in (1..=last).rev() {
                    match s.transaction_stack[i].block_state {
                        TBLOCK_SUBINPROGRESS => {
                            s.transaction_stack[i].block_state = TBLOCK_SUBABORT_PENDING
                        }
                        TBLOCK_SUBABORT => {
                            s.transaction_stack[i].block_state = TBLOCK_SUBABORT_END
                        }
                        other => return Some(other),
                    }
                }
                match s.transaction_stack[0].block_state {
                    TBLOCK_INPROGRESS => {
                        s.transaction_stack[0].block_state = TBLOCK_ABORT_PENDING;
                        None
                    }
                    TBLOCK_ABORT => {
                        s.transaction_stack[0].block_state = TBLOCK_ABORT_END;
                        None
                    }
                    other => Some(other),
                }
            });
            if let Some(bs) = bad {
                return Err(unexpected_block_state("EndTransactionBlock", bs));
            }
        }

        // COMMIT outside a block: warn (error for AND CHAIN) but allow the
        // transaction to commit.
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

        // The user issued COMMIT in a parallel worker: error out hard.
        TBLOCK_PARALLEL_INPROGRESS => {
            return ereport(FATAL)
                .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
                .errmsg("cannot commit during a parallel operation")
                .finish(xact_location("EndTransactionBlock"))
                .map(|()| false);
        }

        other => return Err(unexpected_block_state("EndTransactionBlock", other)),
    }

    // In C, s is walked up to the TOP node before `s->chain = chain`, so the
    // chain flag always lands on the top-level transaction state.
    xs(|s| s.transaction_stack[0].chain = chain);
    Ok(result)
}

/// `UserAbortTransactionBlock` (xact.c:4204)
pub fn UserAbortTransactionBlock(chain: bool) -> PgResult<()> {
    match cur_block_state() {
        // Live block: schedule abort + cleanup.
        TBLOCK_INPROGRESS => {
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT_PENDING);
        }

        // Already-failed block: only cleanup remains.
        TBLOCK_ABORT => {
            xs(|s| s.current_mut().block_state = TBLOCK_ABORT_END);
        }

        // In a subtransaction: abort all of them + the main transaction.
        TBLOCK_SUBINPROGRESS | TBLOCK_SUBABORT => {
            let bad: Option<TBlockState> = xs(|s| {
                let last = s.transaction_stack.len() - 1;
                for i in (1..=last).rev() {
                    match s.transaction_stack[i].block_state {
                        TBLOCK_SUBINPROGRESS => {
                            s.transaction_stack[i].block_state = TBLOCK_SUBABORT_PENDING
                        }
                        TBLOCK_SUBABORT => {
                            s.transaction_stack[i].block_state = TBLOCK_SUBABORT_END
                        }
                        other => return Some(other),
                    }
                }
                match s.transaction_stack[0].block_state {
                    TBLOCK_INPROGRESS => {
                        s.transaction_stack[0].block_state = TBLOCK_ABORT_PENDING;
                        None
                    }
                    TBLOCK_ABORT => {
                        s.transaction_stack[0].block_state = TBLOCK_ABORT_END;
                        None
                    }
                    other => Some(other),
                }
            });
            if let Some(bs) = bad {
                return Err(unexpected_block_state("UserAbortTransactionBlock", bs));
            }
        }

        // ROLLBACK outside a block: warn (error for AND CHAIN) and abort the
        // transaction anyway.
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

        // The user issued ABORT in a parallel worker: error out hard.
        TBLOCK_PARALLEL_INPROGRESS => {
            return ereport(FATAL)
                .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
                .errmsg("cannot abort during a parallel operation")
                .finish(xact_location("UserAbortTransactionBlock"));
        }

        other => return Err(unexpected_block_state("UserAbortTransactionBlock", other)),
    }

    // chain always lands on the top-level state (see EndTransactionBlock).
    xs(|s| s.transaction_stack[0].chain = chain);
    Ok(())
}

/// `BeginImplicitTransactionBlock` (xact.c:4326)
pub fn BeginImplicitTransactionBlock() {
    // If we are in STARTED state (not in a block), switch to the implicit-
    // block state; otherwise leave it alone.
    xs(|s| {
        if s.current().block_state == TBLOCK_STARTED {
            s.current_mut().block_state = TBLOCK_IMPLICIT_INPROGRESS;
        }
    });
}

/// `EndImplicitTransactionBlock` (xact.c:4351)
pub fn EndImplicitTransactionBlock() {
    xs(|s| {
        if s.current().block_state == TBLOCK_IMPLICIT_INPROGRESS {
            s.current_mut().block_state = TBLOCK_STARTED;
        }
    });
}

/// `DefineSavepoint` (xact.c:4373). A `None` name corresponds to C's NULL
/// (used by the SUBRESTART arms).
pub fn DefineSavepoint(name: Option<&str>) -> PgResult<()> {
    // Workers synchronize transaction state at the beginning of each parallel
    // operation, so we can't account for new subtransactions after that.
    if IsInParallelMode() || parallel_seams::is_parallel_worker() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot define savepoints during a parallel operation")
            .finish(xact_location("DefineSavepoint"));
    }

    match cur_block_state() {
        TBLOCK_INPROGRESS | TBLOCK_SUBINPROGRESS => {
            // Normal subtransaction start.
            PushTransaction()?;
            // Note that we are allocating the savepoint name in the parent
            // transaction's memory lifetime, since we don't yet have a
            // transaction context for the new guy.
            // (C: MemoryContextStrdup(TopTransactionContext, name))
            if let Some(name) = name {
                let name = try_strdup(name, "out of memory saving savepoint name")?;
                xs(|s| s.current_mut().name = Some(name));
            }
            Ok(())
        }
        // SAVEPOINT inside an implicit block is disallowed: the savepoint
        // would be unreleasable after the multi-statement command ends.
        TBLOCK_IMPLICIT_INPROGRESS => ereport(ERROR)
            .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
            // translator: %s represents an SQL statement name
            .errmsg("SAVEPOINT can only be used in transaction blocks")
            .finish(xact_location("DefineSavepoint")),
        other => Err(unexpected_block_state("DefineSavepoint", other)),
    }
}

/// `ReleaseSavepoint` (xact.c:4458)
pub fn ReleaseSavepoint(name: &str) -> PgResult<()> {
    if IsInParallelMode() || parallel_seams::is_parallel_worker() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot release savepoints during a parallel operation")
            .finish(xact_location("ReleaseSavepoint"));
    }

    match cur_block_state() {
        // In a transaction block with no savepoints defined.
        TBLOCK_INPROGRESS => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!("savepoint \"{name}\" does not exist"))
                .finish(xact_location("ReleaseSavepoint"));
        }
        TBLOCK_IMPLICIT_INPROGRESS => {
            // See comment about implicit transactions in DefineSavepoint.
            return ereport(ERROR)
                .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                // translator: %s represents an SQL statement name
                .errmsg("RELEASE SAVEPOINT can only be used in transaction blocks")
                .finish(xact_location("ReleaseSavepoint"));
        }
        // We are in a non-aborted subtransaction: the expected case.
        TBLOCK_SUBINPROGRESS => {}
        other => return Err(unexpected_block_state("ReleaseSavepoint", other)),
    }

    enum Find {
        NotFound,
        WrongLevel,
        At(usize),
    }
    let found = xs(|s| {
        let cur_level = s.current().savepoint_level;
        match s
            .transaction_stack
            .iter()
            .rposition(|node| node.name.as_deref() == Some(name))
        {
            None => Find::NotFound,
            Some(t) if s.transaction_stack[t].savepoint_level != cur_level => Find::WrongLevel,
            Some(t) => Find::At(t),
        }
    });
    let target = match found {
        Find::NotFound => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!("savepoint \"{name}\" does not exist"))
                .finish(xact_location("ReleaseSavepoint"));
        }
        Find::WrongLevel => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!(
                    "savepoint \"{name}\" does not exist within current savepoint level"
                ))
                .finish(xact_location("ReleaseSavepoint"));
        }
        Find::At(t) => t,
    };

    // Mark "commit pending" all subtransactions up to the target; the actual
    // commits happen when control returns to the main loop.
    xs(|s| {
        let last = s.transaction_stack.len() - 1;
        for i in (target..=last).rev() {
            debug_assert_eq!(s.transaction_stack[i].block_state, TBLOCK_SUBINPROGRESS);
            s.transaction_stack[i].block_state = TBLOCK_SUBRELEASE;
        }
    });
    Ok(())
}

/// `RollbackToSavepoint` (xact.c:4567)
pub fn RollbackToSavepoint(name: &str) -> PgResult<()> {
    if IsInParallelMode() || parallel_seams::is_parallel_worker() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot rollback to savepoints during a parallel operation")
            .finish(xact_location("RollbackToSavepoint"));
    }

    match cur_block_state() {
        // In a transaction block with no savepoints defined.
        TBLOCK_INPROGRESS | TBLOCK_ABORT => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!("savepoint \"{name}\" does not exist"))
                .finish(xact_location("RollbackToSavepoint"));
        }
        TBLOCK_IMPLICIT_INPROGRESS => {
            // See comment about implicit transactions in DefineSavepoint.
            return ereport(ERROR)
                .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
                // translator: %s represents an SQL statement name
                .errmsg("ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
                .finish(xact_location("RollbackToSavepoint"));
        }
        // In a subtransaction: the expected cases.
        TBLOCK_SUBINPROGRESS | TBLOCK_SUBABORT => {}
        other => return Err(unexpected_block_state("RollbackToSavepoint", other)),
    }

    enum Find {
        NotFound,
        WrongLevel,
        At(usize),
    }
    let found = xs(|s| {
        let cur_level = s.current().savepoint_level;
        match s
            .transaction_stack
            .iter()
            .rposition(|node| node.name.as_deref() == Some(name))
        {
            None => Find::NotFound,
            Some(t) if s.transaction_stack[t].savepoint_level != cur_level => Find::WrongLevel,
            Some(t) => Find::At(t),
        }
    });
    let target = match found {
        Find::NotFound => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!("savepoint \"{name}\" does not exist"))
                .finish(xact_location("RollbackToSavepoint"));
        }
        Find::WrongLevel => {
            return ereport(ERROR)
                .errcode(ERRCODE_S_E_INVALID_SPECIFICATION)
                .errmsg(format!(
                    "savepoint \"{name}\" does not exist within current savepoint level"
                ))
                .finish(xact_location("RollbackToSavepoint"));
        }
        Find::At(t) => t,
    };

    // Mark "abort pending" all subtransactions up to the target, and the
    // target itself as "restart pending".
    let bad: Option<TBlockState> = xs(|s| {
        let last = s.transaction_stack.len() - 1;
        for i in ((target + 1)..=last).rev() {
            match s.transaction_stack[i].block_state {
                TBLOCK_SUBINPROGRESS => {
                    s.transaction_stack[i].block_state = TBLOCK_SUBABORT_PENDING
                }
                TBLOCK_SUBABORT => s.transaction_stack[i].block_state = TBLOCK_SUBABORT_END,
                other => return Some(other),
            }
        }
        match s.transaction_stack[target].block_state {
            TBLOCK_SUBINPROGRESS => {
                s.transaction_stack[target].block_state = TBLOCK_SUBRESTART;
                None
            }
            TBLOCK_SUBABORT => {
                s.transaction_stack[target].block_state = TBLOCK_SUBABORT_RESTART;
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

/// `BeginInternalSubTransaction` (xact.c:4694) — like DefineSavepoint, but
/// allowed in implicit blocks, parallel mode, and the STARTED/END/PREPARE
/// states; immediately starts the subtransaction.
pub fn BeginInternalSubTransaction(name: Option<&str>) -> PgResult<()> {
    // Errors within this function are improbable, but if one happens C
    // forces a FATAL exit (ExitOnAnyError): callers can't handle losing
    // control and the transaction state is probably corrupted.
    let save_exit_on_any_error = globals_seams::exit_on_any_error::call();
    globals_seams::set_exit_on_any_error::call(true);

    // We do not check for parallel mode here: starting/ending "internal"
    // subtransactions in parallel mode is fine as long as no new XIDs or
    // command IDs are assigned (enforced in AssignTransactionId / CCI).
    let result = (|| -> PgResult<()> {
        match cur_block_state() {
            TBLOCK_STARTED
            | TBLOCK_INPROGRESS
            | TBLOCK_IMPLICIT_INPROGRESS
            | TBLOCK_PARALLEL_INPROGRESS
            | TBLOCK_END
            | TBLOCK_PREPARE
            | TBLOCK_SUBINPROGRESS => {
                // Normal subtransaction start.
                PushTransaction()?;
                // (C: MemoryContextStrdup(TopTransactionContext, name))
                if let Some(name) = name {
                    let name = try_strdup(name, "out of memory saving savepoint name")?;
                    xs(|s| s.current_mut().name = Some(name));
                }
            }
            other => {
                return Err(unexpected_block_state(
                    "BeginInternalSubTransaction",
                    other,
                ));
            }
        }

        CommitTransactionCommand()?;
        StartTransactionCommand()
    })();

    globals_seams::set_exit_on_any_error::call(save_exit_on_any_error);
    result
}

/// `ReleaseCurrentSubTransaction` (xact.c:4768) — RELEASE (commit) the
/// innermost subtransaction regardless of its savepoint name. (No parallel-
/// mode check: ending "internal" subtransactions in parallel mode is fine.)
pub fn ReleaseCurrentSubTransaction() -> PgResult<()> {
    if cur_block_state() != TBLOCK_SUBINPROGRESS {
        return Err(PgError::new(
            ERROR,
            format!(
                "ReleaseCurrentSubTransaction: unexpected state {}",
                BlockStateAsString(cur_block_state())
            ),
        ));
    }
    debug_assert!(xs(|s| s.current().state == TRANS_INPROGRESS));
    CommitSubTransaction()?;
    debug_assert!(xs(|s| s.current().state == TRANS_INPROGRESS));
    Ok(())
}

/// `RollbackAndReleaseCurrentSubTransaction` (xact.c:4796)
pub fn RollbackAndReleaseCurrentSubTransaction() -> PgResult<()> {
    // unlike ReleaseCurrentSubTransaction, this is OK in a parallel worker
    match cur_block_state() {
        TBLOCK_SUBINPROGRESS | TBLOCK_SUBABORT => {}
        other => {
            return Err(PgError::new(
                FATAL,
                format!(
                    "RollbackAndReleaseCurrentSubTransaction: unexpected state {}",
                    BlockStateAsString(other)
                ),
            ));
        }
    }

    // Abort the current subtransaction, if needed.
    if cur_block_state() == TBLOCK_SUBINPROGRESS {
        AbortSubTransaction()?;
    }

    // And clean it up, popping back to the parent.
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

/// `AbortOutOfAnyTransaction` (xact.c:4862) — abort any active transaction or
/// block, leaving the system in a known idle state.
pub fn AbortOutOfAnyTransaction() -> PgResult<()> {
    // Ensure we're not running in a doomed memory context.
    AtAbort_Memory();

    // Get out of any transaction or nested transaction.
    loop {
        match cur_block_state() {
            TBLOCK_DEFAULT => {
                if xs(|s| s.current().state) == TRANS_DEFAULT {
                    // Not in a transaction, do nothing.
                } else {
                    // Error during transaction start (TRANS_START): clean up
                    // the incompletely started transaction; adjust the state
                    // to suppress AbortTransaction's warning.
                    if xs(|s| s.current().state) == TRANS_START {
                        xs(|s| s.current_mut().state = TRANS_INPROGRESS);
                    }
                    AbortTransaction()?;
                    CleanupTransaction()?;
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
                // In a transaction, so clean up.
                AbortTransaction()?;
                CleanupTransaction()?;
                xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            }
            TBLOCK_ABORT | TBLOCK_ABORT_END => {
                // AbortTransaction is already done, still need Cleanup. If we
                // failed partway through ROLLBACK, an active portal may still
                // be running that command: shut portals down first.
                portal_seams::at_abort_portals::call()?;
                CleanupTransaction()?;
                xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
            }
            // In a subtransaction: clean it up and abort the parent too.
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
                // As above, but AbortSubTransaction already done; might still
                // have a live portal to zap.
                if xs(|s| s.current().has_resource_owner) {
                    let (my, parent) = subxact_ids();
                    portal_seams::at_subabort_portals::call(my, parent)?;
                }
                CleanupSubTransaction()?;
            } // C's elog(FATAL, "...unexpected state...") default arm is
              // statically unreachable: the match is exhaustive over
              // TBlockState.
        }
        if cur_block_state() == TBLOCK_DEFAULT {
            break;
        }
    }

    // Should be out of all subxacts now.
    debug_assert!(xs(|s| s.transaction_stack.len() == 1));
    // (C reverts to TopMemoryContext here — no ambient context.)
    Ok(())
}

// ---------------------------------------------------------------------------
//  Sub-transaction engine (xact.c:5067-5503)
// ---------------------------------------------------------------------------

/// Helper: (mySubid, parentSubid) for the current node.
fn subxact_ids() -> (SubTransactionId, SubTransactionId) {
    xs(|s| {
        let last = s.transaction_stack.len() - 1;
        let my = s.transaction_stack[last].sub_transaction_id;
        let parent = if last > 0 {
            s.transaction_stack[last - 1].sub_transaction_id
        } else {
            InvalidSubTransactionId
        };
        (my, parent)
    })
}

/// `StartSubTransaction` (xact.c:5067)
fn StartSubTransaction() -> PgResult<()> {
    if xs(|s| s.current().state) != TRANS_DEFAULT {
        warn_internal(&format!(
            "StartSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }
    xs(|s| s.current_mut().state = TRANS_START);

    // Initialize subsystems for the new subtransaction; resource-management
    // stuff first.
    AtSubStart_Memory();
    AtSubStart_ResourceOwner()?;
    trigger_seams::after_trigger_begin_sub_xact::call()?;

    xs(|s| s.current_mut().state = TRANS_INPROGRESS);

    // Call start-of-subxact callbacks.
    let (my, parent) = subxact_ids();
    CallSubXactCallbacks(SUBXACT_EVENT_START_SUB, my, parent)?;

    ShowTransactionState("StartSubTransaction");
    Ok(())
}

/// `CommitSubTransaction` (xact.c:5104)
fn CommitSubTransaction() -> PgResult<()> {
    ShowTransactionState("CommitSubTransaction");

    if xs(|s| s.current().state) != TRANS_INPROGRESS {
        warn_internal(&format!(
            "CommitSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }

    // Pre-commit processing goes here.
    let (my, parent) = subxact_ids();
    CallSubXactCallbacks(SUBXACT_EVENT_PRE_COMMIT_SUB, my, parent)?;

    // Clean up any unfinished parallel operation; warn about leaks.
    parallel_seams::at_eosubxact_parallel(true, my)?;
    let level = xs(|s| s.current().parallel_mode_level);
    if level != 0 {
        warn_internal(&format!(
            "parallelModeLevel is {level} not 0 at end of subtransaction"
        ));
        xs(|s| s.current_mut().parallel_mode_level = 0);
    }

    // Do the actual "commit", such as it is.
    xs(|s| s.current_mut().state = TRANS_COMMIT);

    // Must CCI to ensure commands of the subtransaction are seen as done.
    CommandCounterIncrement()?;

    // Post-commit cleanup. (Subcommit-in-clog now happens, if required, as
    // part of the atomic transaction-tree update at top-level commit/abort.)
    if xs(|s| s.current().full_transaction_id.is_valid()) {
        AtSubCommit_childXids()?;
    }
    trigger_seams::after_trigger_end_sub_xact::call(true)?;
    let parent_nesting = xs(|s| {
        let last = s.transaction_stack.len() - 1;
        s.transaction_stack[last - 1].nesting_level
    });
    portal_seams::at_subcommit_portals::call(my, parent, parent_nesting)?;
    fsstubs_seams::at_eosubxact_large_object::call(true, my, parent)?;
    async_seams::at_subcommit_notify::call()?;

    CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, my, parent)?;

    // ResourceOwnerRelease(s->curTransactionOwner, BEFORE_LOCKS, true, false).
    resowner_seams::release_subxact_owner_before_locks::call(true)?;
    relcache_seams::at_eosubxact_relation_cache::call(true, my, parent)?;
    typcache_seams::at_eosubxact_type_cache::call();
    inval_seams::at_eosubxact_inval::call(true)?;
    storage_seams::at_subcommit_smgr::call();

    // The only lock we actually release here is the subtransaction XID lock.
    // (CurrentResourceOwner = s->curTransactionOwner — already the live owner.)
    if xs(|s| s.current().full_transaction_id.is_valid()) {
        let xid = xs(|s| s.current().full_transaction_id.xid());
        lmgr_seams::xact_lock_table_delete::call(xid)?;
    }

    // Other locks transfer to the parent resource owner.
    // ResourceOwnerRelease(LOCKS) + ResourceOwnerRelease(AFTER_LOCKS).
    resowner_seams::release_subxact_owner_locks::call(true)?;

    let (guc_nest_level, nesting_level) =
        xs(|s| (s.current().guc_nest_level, s.current().nesting_level));
    guc_core_seams::at_eoxact_guc::call(true, guc_nest_level)?;
    spi_seams::at_eosubxact_spi::call(true, my)?;
    tablecmds_seams::at_eosubxact_on_commit_actions::call(true, my, parent);
    namespace_seams::at_eosubxact_namespace::call(true, my, parent);
    fd_seams::at_eosubxact_files::call(true, my, parent);
    // AtEOSubXact_HashTables dissolves.
    pgstat_xact_seams::at_eosubxact_pgstat::call(true, nesting_level);
    snapmgr_seams::at_subcommit_snapshot::call(nesting_level);

    // Restore the upper transaction's read-only state (the upper may be
    // read-write while the child is read-only; GUC would leave the child
    // state in place).
    xs(|s| {
        s.XactReadOnly = s.current().prev_xact_read_only;
    });

    // CurrentResourceOwner = CurTransactionResourceOwner = s->parent->
    // curTransactionOwner; ResourceOwnerDelete(s->curTransactionOwner).
    resowner_seams::cleanup_subxact_owner::call()?;
    xs(|s| s.current_mut().has_resource_owner = false);

    AtSubCommit_Memory()?;

    xs(|s| s.current_mut().state = TRANS_DEFAULT);

    PopTransaction()
}

/// `AbortSubTransaction` (xact.c:5219)
fn AbortSubTransaction() -> PgResult<()> {
    // Prevent cancel/die interrupt while cleaning up.
    globals_seams::hold_interrupts::call();

    // Make sure we have a valid memory context and resource owner.
    AtSubAbort_Memory();
    AtSubAbort_ResourceOwner();

    // Release any LW locks we might be holding as quickly as possible.
    let _ = lwlock::LWLockReleaseAll();

    waitevent_seams::pgstat_report_wait_end::call();
    activity_small::backend_progress::pgstat_progress_end_command();

    aio_seams::pgaio_error_cleanup::call();

    bufmgr_seams::unlock_buffers::call();

    // Reset WAL record construction state.
    xloginsert_seams::xlog_reset_insertion::call();

    // Cancel condition variable sleep.
    let _ = condvar_seams::condition_variable_cancel_sleep::call();

    // Clean up any open wait for lock.
    proc_seams::lock_error_cleanup::call();

    // Re-arm timeouts, then re-enable signals (see AbortTransaction).
    timeout_seams::reschedule_timeouts::call()?;
    {
        let masks = libpq_pqsignal::signal_masks();
        unsafe {
            libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), std::ptr::null_mut());
        }
    }

    // check the current transaction state
    ShowTransactionState("AbortSubTransaction");

    if xs(|s| s.current().state) != TRANS_INPROGRESS {
        warn_internal(&format!(
            "AbortSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }

    xs(|s| s.current_mut().state = TRANS_ABORT);

    // Reset user ID which might have been changed transiently.
    let (prev_user, prev_sec) = xs(|s| (s.current().prev_user, s.current().prev_sec_context));
    miscinit_seams::set_user_id_and_sec_context::call(prev_user, prev_sec);

    // Forget about any active REINDEX.
    index_seams::reset_reindex_state::call(xs(|s| s.current().nesting_level));

    // Reset logical streaming state.
    logical_seams::reset_logical_streaming_state::call();

    // (No SnapBuildResetExportedSnapshotState: snapshot exports are not
    // supported in subtransactions.)

    // Clean up any unfinished parallel operation; no leak warnings.
    let (my, parent) = subxact_ids();
    parallel_seams::at_eosubxact_parallel(false, my)?;
    xs(|s| s.current_mut().parallel_mode_level = 0);

    // We can skip all of this if the subxact failed before creating a
    // ResourceOwner.
    if xs(|s| s.current().has_resource_owner) {
        trigger_seams::after_trigger_end_sub_xact::call(false)?;
        portal_seams::at_subabort_portals::call(my, parent)?;
        fsstubs_seams::at_eosubxact_large_object::call(false, my, parent)?;
        async_seams::at_subabort_notify::call();

        // Advertise the fact that we aborted in pg_xact.
        RecordTransactionAbort(true)?;

        // Post-abort cleanup.
        if xs(|s| s.current().full_transaction_id.is_valid()) {
            AtSubAbort_childXids();
        }

        CallSubXactCallbacks(SUBXACT_EVENT_ABORT_SUB, my, parent)?;

        // ResourceOwnerRelease(s->curTransactionOwner, BEFORE_LOCKS, false,
        // false) — releases the subtransaction's buffer pins, etc.
        resowner_seams::release_subxact_owner_before_locks::call(false)?;
        aio_seams::at_eoxact_aio::call(false);
        relcache_seams::at_eosubxact_relation_cache::call(false, my, parent)?;
        typcache_seams::at_eosubxact_type_cache::call();
        inval_seams::at_eosubxact_inval::call(false)?;
        // ResourceOwnerRelease(LOCKS) + ResourceOwnerRelease(AFTER_LOCKS).
        resowner_seams::release_subxact_owner_locks::call(false)?;
        storage_seams::at_subabort_smgr::call()?;

        let (guc_nest_level, nesting_level) =
            xs(|s| (s.current().guc_nest_level, s.current().nesting_level));
        guc_core_seams::at_eoxact_guc::call(false, guc_nest_level)?;
        spi_seams::at_eosubxact_spi::call(false, my)?;
        tablecmds_seams::at_eosubxact_on_commit_actions::call(false, my, parent);
        namespace_seams::at_eosubxact_namespace::call(false, my, parent);
        fd_seams::at_eosubxact_files::call(false, my, parent);
        // AtEOSubXact_HashTables dissolves.
        pgstat_xact_seams::at_eosubxact_pgstat::call(false, nesting_level);
        snapmgr_seams::at_subabort_snapshot::call(nesting_level)?;
    }

    // Restore the upper transaction's read-only state (redundant with GUC's
    // cleanup, but consistent with the commit case).
    xs(|s| s.XactReadOnly = s.current().prev_xact_read_only);

    globals_seams::resume_interrupts::call();
    Ok(())
}

/// `CleanupSubTransaction` (xact.c:5383)
fn CleanupSubTransaction() -> PgResult<()> {
    ShowTransactionState("CleanupSubTransaction");

    if xs(|s| s.current().state) != TRANS_ABORT {
        warn_internal(&format!(
            "CleanupSubTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }

    let (my, _parent) = subxact_ids();
    portal_seams::at_subcleanup_portals::call(my)?;

    // CurrentResourceOwner = CurTransactionResourceOwner = parent's owner;
    // if (s->curTransactionOwner) ResourceOwnerDelete(s->curTransactionOwner).
    if xs(|s| s.current().has_resource_owner) {
        resowner_seams::cleanup_subxact_owner::call()?;
    }
    xs(|s| s.current_mut().has_resource_owner = false);

    AtSubCleanup_Memory();

    xs(|s| s.current_mut().state = TRANS_DEFAULT);

    PopTransaction()
}

/// `PushTransaction` (xact.c:5416) — create a state-stack entry for a
/// subtransaction.
fn PushTransaction() -> PgResult<()> {
    // Assign a subtransaction ID, watching out for counter wraparound.
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

    let guc_nest_level = guc_core_seams::new_guc_nest_level::call();
    let (prev_user, prev_sec_context) = miscinit_seams::get_user_id_and_sec_context::call();

    // We can now stack a minimally valid subtransaction without fear of
    // failure (AbortSubTransaction/CleanupSubTransaction can cope with it
    // from here on: no transaction context, resource owner, or XID yet).
    xs(|s| {
        let parent = s.current();
        let parent_nesting = parent.nesting_level;
        let parent_savepoint = parent.savepoint_level;
        let parent_started_in_recovery = parent.started_in_recovery;
        let parent_parallel_child =
            parent.parallel_mode_level != 0 || parent.parallel_child_xact;
        let subid = s.current_sub_transaction_id;
        let prev_xact_read_only = s.XactReadOnly;

        s.transaction_stack
            .try_reserve(1)
            .map_err(|_| PgError::error("out of memory pushing transaction state"))?;
        s.transaction_stack.push(TransactionNode {
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
        });
        Ok(())
    })
}

/// `PopTransaction` (xact.c:5478) — pop back to the parent state.
fn PopTransaction() -> PgResult<()> {
    if xs(|s| s.current().state) != TRANS_DEFAULT {
        warn_internal(&format!(
            "PopTransaction while in {} state",
            TransStateAsString(xs(|s| s.current().state))
        ));
    }
    if xs(|s| s.transaction_stack.len()) <= 1 {
        return Err(PgError::new(FATAL, "PopTransaction with no parent"));
    }
    // (CurTransactionContext / resource-owner relinking dissolve; the name
    // and node free with the pop.)
    xs(|s| {
        s.transaction_stack.pop();
    });
    Ok(())
}

// ---------------------------------------------------------------------------
//  Parallel-worker serialize/restore (xact.c:5512-5641)
// ---------------------------------------------------------------------------

/// `SerializedTransactionStateHeaderSize`: int + bool(+3 pad) + two
/// FullTransactionIds (8-aligned) + CommandId + int.
const SERIALIZED_HEADER_SIZE: usize = 32;

/// `EstimateTransactionStateSpace` (xact.c:5512)
pub fn EstimateTransactionStateSpace() -> usize {
    xs(|s| {
        let mut nxids = 0usize;
        for node in &s.transaction_stack {
            if node.full_transaction_id.is_valid() {
                nxids += 1;
            }
            nxids += node.child_xids.len();
        }
        SERIALIZED_HEADER_SIZE + nxids * std::mem::size_of::<TransactionId>()
    })
}

/// `SerializeTransactionState` (xact.c:5540) — write the transaction-state
/// details a parallel worker needs into `out` (at least
/// `EstimateTransactionStateSpace()` bytes); XIDs are emitted sorted.
pub fn SerializeTransactionState(out: &mut [u8]) -> PgResult<usize> {
    let (iso, deferrable, top_full, cur_full, cur_cid, xids) = xs(|s| {
        let xids: Vec<TransactionId> = if !s.parallel_current_xids.is_empty() {
            // Already in a parallel worker: pass along what we were given.
            let mut xids = Vec::new();
            if xids.try_reserve_exact(s.parallel_current_xids.len()).is_err() {
                return Err(PgError::error(
                    "out of memory serializing transaction state",
                ));
            }
            xids.extend_from_slice(&s.parallel_current_xids);
            xids
        } else {
            let mut workspace: Vec<TransactionId> = Vec::new();
            for node in &s.transaction_stack {
                let extra =
                    usize::from(node.full_transaction_id.is_valid()) + node.child_xids.len();
                if workspace.try_reserve(extra).is_err() {
                    return Err(PgError::error(
                        "out of memory serializing transaction state",
                    ));
                }
                if node.full_transaction_id.is_valid() {
                    workspace.push(node.full_transaction_id.xid());
                }
                workspace.extend_from_slice(&node.child_xids);
            }
            // qsort(..., xidComparator): plain numeric order.
            workspace.sort_unstable();
            workspace
        };
        Ok((
            s.XactIsoLevel,
            s.XactDeferrable,
            s.xact_top_full_transaction_id,
            s.current().full_transaction_id,
            s.current_command_id,
            xids,
        ))
    })?;

    let total = SERIALIZED_HEADER_SIZE + xids.len() * 4;
    if out.len() < total {
        return Err(PgError::error("transaction state buffer is too small"));
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

/// `StartParallelWorkerTransaction` (xact.c:5611) — start a parallel worker
/// transaction, restoring the state serialized by `SerializeTransactionState`.
pub fn StartParallelWorkerTransaction(tstatespace: &[u8]) -> PgResult<()> {
    debug_assert_eq!(cur_block_state(), TBLOCK_DEFAULT);
    StartTransaction()?;

    if tstatespace.len() < SERIALIZED_HEADER_SIZE {
        return Err(PgError::error("invalid serialized transaction state"));
    }
    let n_xids = i32::from_ne_bytes(tstatespace[28..32].try_into().unwrap());
    if n_xids < 0 {
        return Err(PgError::error("invalid serialized transaction state"));
    }
    let total = SERIALIZED_HEADER_SIZE + n_xids as usize * 4;
    if tstatespace.len() < total {
        return Err(PgError::error("invalid serialized transaction state"));
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

    xs(|s| {
        s.XactIsoLevel = i32::from_ne_bytes(tstatespace[0..4].try_into().unwrap());
        s.XactDeferrable = tstatespace[4] != 0;
        s.xact_top_full_transaction_id = FullTransactionId {
            value: u64::from_ne_bytes(tstatespace[8..16].try_into().unwrap()),
        };
        s.current_mut().full_transaction_id = FullTransactionId {
            value: u64::from_ne_bytes(tstatespace[16..24].try_into().unwrap()),
        };
        s.current_command_id = CommandId::from_ne_bytes(tstatespace[24..28].try_into().unwrap());
        s.parallel_current_xids = xids;
        s.current_mut().block_state = TBLOCK_PARALLEL_INPROGRESS;
    });
    Ok(())
}

/// `EndParallelWorkerTransaction` (xact.c:5636)
pub fn EndParallelWorkerTransaction() -> PgResult<()> {
    debug_assert_eq!(cur_block_state(), TBLOCK_PARALLEL_INPROGRESS);
    CommitTransaction()?;
    xs(|s| s.current_mut().block_state = TBLOCK_DEFAULT);
    Ok(())
}
