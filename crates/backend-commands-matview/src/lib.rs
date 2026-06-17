#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `backend/commands/matview.c` — REFRESH MATERIALIZED VIEW (and the CREATE
//! MATERIALIZED VIEW populate path).
//!
//! This crate owns the refresh orchestration, the match-merge SQL-text
//! construction with its per-unique-index equality-qual loop, the
//! populated-state control flow, `is_usable_unique_index`'s predicate, and the
//! `matview_maintenance_depth` counter. Every genuine cross-subsystem external
//! crosses a seam in `backend_commands_matview_deps_seams` (all owners are still
//! unported, so those seams panic until they land — mirror-PG-and-panic). The
//! 16 C functions (5 extern + 11 static) are all present.

use std::cell::{Cell, RefCell};

use backend_utils_error::ereport;
use mcx::{Mcx, PgBox, PgString, PgVec};

use backend_commands_matview_deps_seams as seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::Oid;
use types_core::xact::CommandId;
use types_dest::CommandDest;
use types_error::{
    PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_matview::{
    CommandTag, IndexUsabilityInfo, QueryCompletion, QueryHandle, RefreshMatViewStmt,
};
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_rel::Relation;
use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, NoLock, RowExclusiveLock,
};
use types_tableam::tableam::BulkInsertStateData;
use types_tuple::access::{RELKIND_MATVIEW, RELPERSISTENCE_TEMP};
use types_tuple::heaptuple::{MaxHeapAttributeNumber, TupleDescData};

/// `RelationRelationId` — `pg_class` OID (`catalog/pg_class.h`).
const RelationRelationId: Oid = 1259;

/// `RELKIND_MATVIEW` as the signed `char` `pg_class.relkind` carries.
const RELKIND_MATVIEW_I8: i8 = RELKIND_MATVIEW as i8;

/// `SECURITY_RESTRICTED_OPERATION` / `SECURITY_LOCAL_USERID_CHANGE`
/// (`miscadmin.h`).
const SECURITY_RESTRICTED_OPERATION: i32 = 0x2;
const SECURITY_LOCAL_USERID_CHANGE: i32 = 0x1;

/// `TABLE_INSERT_SKIP_FSM` / `TABLE_INSERT_FROZEN` (`access/tableam.h`) — the
/// `ti_options` `transientrel_startup` sets (matview.c 499).
const TABLE_INSERT_SKIP_FSM: i32 = 0x0002;
const TABLE_INSERT_FROZEN: i32 = 0x0004;

/* SPI result codes (`executor/spi.h`). */
const SPI_OK_SELECT: i32 = 5;
const SPI_OK_INSERT: i32 = 7;
const SPI_OK_DELETE: i32 = 8;
const SPI_OK_UTILITY: i32 = 4;
const SPI_OK_FINISH: i32 = 2;

/// `ObjectAddressSet(addr, class, oid)` (`catalog/objectaddress.h`).
#[inline]
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

// ---------------------------------------------------------------------------
// File-local state (matview.c line 56).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static int matview_maintenance_depth = 0;` (matview.c line 56). A
    /// backend is single-threaded, so this per-backend counter is a thread-local.
    static MATVIEW_MAINTENANCE_DEPTH: Cell<i32> = const { Cell::new(0) };
}

// ---------------------------------------------------------------------------
// SetMatViewPopulatedState (matview.c 78-110)
// ---------------------------------------------------------------------------

/// `SetMatViewPopulatedState` — mark a materialized view as populated, or not.
///
/// NOTE: caller must be holding an appropriate lock on the relation.
pub fn SetMatViewPopulatedState(mcx: Mcx<'_>, relation: Oid, newstate: bool) -> PgResult<()> {
    debug_assert_eq!(
        seam::relation_get_relkind::call(relation)?,
        RELKIND_MATVIEW_I8
    );

    /*
     * Update relation's pg_class entry.  Crucial side-effect: other backends
     * (and this one too!) are sent SI message to make them rebuild relcache
     * entries.
     */
    let relid = seam::relation_get_relid::call(relation)?;
    if !seam::update_pg_class_populated::call(relid, newstate)? {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("cache lookup failed for relation {relid}"))?)
            .into_error());
    }

    /* Advance command counter to make the updated pg_class row locally visible. */
    seam::command_counter_increment::call()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// ExecRefreshMatView (matview.c 120-140)
// ---------------------------------------------------------------------------

/// `ExecRefreshMatView` — execute a REFRESH MATERIALIZED VIEW command.
pub fn ExecRefreshMatView(
    mcx: Mcx<'_>,
    stmt: &RefreshMatViewStmt,
    query_string: &str,
    qc: Option<QueryCompletion>,
) -> PgResult<(ObjectAddress, Option<QueryCompletion>)> {
    /* Determine strength of lock needed. */
    let lockmode = if stmt.concurrent {
        ExclusiveLock
    } else {
        AccessExclusiveLock
    };

    /*
     * Get a lock until end of transaction.
     *
     * RangeVarGetRelidExtended(stmt->relation, lockmode, 0,
     *                          RangeVarCallbackMaintainsTable, NULL);
     */
    let matviewOid = seam::rangevar_get_relid_extended::call(
        stmt.relation.schemaname.clone(),
        stmt.relation.relname.clone(),
        lockmode,
    )?;

    RefreshMatViewByOid(
        mcx,
        matviewOid,
        false,
        stmt.skipData,
        stmt.concurrent,
        query_string,
        qc,
    )
}

// ---------------------------------------------------------------------------
// RefreshMatViewByOid (matview.c 164-394)
// ---------------------------------------------------------------------------

/// `RefreshMatViewByOid` — refresh a materialized view by OID (also used to
/// populate a freshly created matview from CREATE MATERIALIZED VIEW).
pub fn RefreshMatViewByOid(
    mcx: Mcx<'_>,
    matviewOid: Oid,
    is_create: bool,
    skipData: bool,
    concurrent: bool,
    query_string: &str,
    qc: Option<QueryCompletion>,
) -> PgResult<(ObjectAddress, Option<QueryCompletion>)> {
    let mut processed: u64 = 0;

    let matviewRel = seam::table_open::call(matviewOid, NoLock)?;
    let info = seam::matview_rel_info::call(matviewRel)?;
    let relowner = info.relowner;

    /*
     * Switch to the owner's userid, so that any functions are run as that user.
     * Also lock down security-restricted operations and arrange to make GUC
     * variable changes local to this command.
     */
    let (save_userid, save_sec_context) = seam::get_user_id_and_sec_context::call()?;
    seam::set_user_id_and_sec_context::call(
        relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    )?;
    let save_nestlevel = seam::new_guc_nest_level::call()?;
    backend_utils_misc_guc_seams::restrict_search_path::call()?;

    /* Make sure it is a materialized view. */
    if info.relkind != RELKIND_MATVIEW_I8 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(fmt(mcx, format_args!("\"{}\" is not a materialized view", info.relname))?)
            .into_error());
    }

    /* Check that CONCURRENTLY is not specified if not populated. */
    if concurrent && !info.is_populated {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("CONCURRENTLY cannot be used when the materialized view is not populated")
            .into_error());
    }

    /* Check that conflicting options have not been specified. */
    if concurrent && skipData {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(fmt(
                mcx,
                format_args!("{} and {} options cannot be used together", "CONCURRENTLY", "WITH NO DATA"),
            )?)
            .into_error());
    }

    /*
     * Check that everything is correct for a refresh. Problems at this point are
     * internal errors, so elog is sufficient.
     */
    if !info.relhasrules || info.num_rules < 1 {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(
                mcx,
                format_args!("materialized view \"{}\" is missing rewrite information", info.relname),
            )?)
            .into_error());
    }

    if info.num_rules > 1 {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(
                mcx,
                format_args!("materialized view \"{}\" has too many rules", info.relname),
            )?)
            .into_error());
    }

    if !info.rule_is_select || !info.rule_is_instead {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(
                mcx,
                format_args!(
                    "the rule for materialized view \"{}\" is not a SELECT INSTEAD OF rule",
                    info.relname
                ),
            )?)
            .into_error());
    }

    if info.rule_actions_length != 1 {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(
                mcx,
                format_args!("the rule for materialized view \"{}\" is not a single action", info.relname),
            )?)
            .into_error());
    }

    /*
     * Check that there is a unique index with no WHERE clause on one or more
     * columns of the materialized view if CONCURRENTLY is specified.
     */
    if concurrent {
        let indexoidlist = seam::relation_get_index_list::call(matviewRel)?;
        let mut hasUniqueIndex = false;

        debug_assert!(!is_create);

        for &indexoid in &indexoidlist {
            let indexRel = seam::index_open::call(indexoid, AccessShareLock)?;
            hasUniqueIndex = is_usable_unique_index(indexRel)?;
            seam::index_close::call(indexRel, AccessShareLock)?;
            if hasUniqueIndex {
                break;
            }
        }

        drop(indexoidlist); /* list_free(indexoidlist) */

        if !hasUniqueIndex {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(fmt(
                    mcx,
                    format_args!(
                        "cannot refresh materialized view \"{}\" concurrently",
                        seam::quote_qualified_relname::call(matviewRel)?
                    ),
                )?)
                .errhint("Create a unique index with no WHERE clause on one or more columns of the materialized view.")
                .into_error());
        }
    }

    /*
     * The stored query was rewritten at the time of the MV definition, but has
     * not been scribbled on by the planner.
     */
    let dataQuery = seam::matview_data_query::call(matviewRel)?;

    /*
     * Check for active uses of the relation in the current transaction, such as
     * open scans.
     *
     * NB: We count on this to protect us against problems with refreshing the
     * data using TABLE_INSERT_FROZEN.
     */
    seam::check_table_not_in_use::call(
        matviewRel,
        if is_create {
            "CREATE MATERIALIZED VIEW".to_string()
        } else {
            "REFRESH MATERIALIZED VIEW".to_string()
        },
    )?;

    /*
     * Tentatively mark the matview as populated or not (this will roll back if
     * we fail later).
     */
    SetMatViewPopulatedState(mcx, matviewRel, !skipData)?;

    /* Concurrent refresh builds new data in temp tablespace, and does diff. */
    let tableSpace;
    let relpersistence;
    if concurrent {
        tableSpace = seam::get_default_tablespace::call(RELPERSISTENCE_TEMP as i8)?;
        relpersistence = RELPERSISTENCE_TEMP as i8;
    } else {
        tableSpace = info.reltablespace;
        relpersistence = info.relpersistence;
    }

    /*
     * Create the transient table that will receive the regenerated data. Lock it
     * against access by any other process until commit (by which time it will be
     * gone).
     */
    let OIDNewHeap = seam::make_new_heap::call(matviewOid, tableSpace, info.relam, relpersistence)?;

    /* Generate the data, if wanted. */
    if !skipData {
        let dest = CreateTransientRelDestReceiver(OIDNewHeap)?;
        processed = refresh_matview_datafill(mcx, dest, dataQuery, query_string, is_create)?;
    }

    /* Make the matview match the newly generated data. */
    if concurrent {
        let old_depth = MATVIEW_MAINTENANCE_DEPTH.with(Cell::get);

        /*
         * PG_TRY()/PG_CATCH(): on error, restore matview_maintenance_depth to
         * old_depth and re-throw.
         */
        match refresh_by_match_merge(mcx, matviewOid, OIDNewHeap, relowner, save_sec_context) {
            Ok(()) => {}
            Err(e) => {
                MATVIEW_MAINTENANCE_DEPTH.with(|d| d.set(old_depth));
                return Err(e);
            }
        }
        debug_assert_eq!(MATVIEW_MAINTENANCE_DEPTH.with(Cell::get), old_depth);
    } else {
        refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence)?;

        /*
         * Inform cumulative stats system about our activity: basically, we
         * truncated the matview and inserted some new data.  (The concurrent code
         * path above doesn't need to worry about this because the inserts and
         * deletes it issues get counted by lower-level code.)
         */
        seam::pgstat_count_truncate::call(matviewRel)?;
        if !skipData {
            seam::pgstat_count_heap_insert::call(matviewRel, processed)?;
        }
    }

    seam::table_close::call(matviewRel, NoLock)?;

    /* Roll back any GUC changes */
    seam::at_eoxact_guc::call(false, save_nestlevel)?;

    /* Restore userid and security context */
    seam::set_user_id_and_sec_context::call(save_userid, save_sec_context)?;

    let address = ObjectAddressSet(RelationRelationId, matviewOid);

    /*
     * Save the rowcount so that pg_stat_statements can track the total number of
     * rows processed by REFRESH MATERIALIZED VIEW command. When called from
     * CREATE MATERIALIZED VIEW command, the rowcount is displayed with the
     * command tag CMDTAG_SELECT.
     */
    let qc = qc.map(|mut qc| {
        qc.set(
            if is_create {
                CommandTag::SELECT
            } else {
                CommandTag::REFRESH_MATERIALIZED_VIEW
            },
            processed,
        );
        qc
    });

    Ok((address, qc))
}

// ---------------------------------------------------------------------------
// refresh_matview_datafill (matview.c 404-462)
// ---------------------------------------------------------------------------

/// `refresh_matview_datafill` — execute the given query, sending result rows to
/// `dest` (which inserts them into the target matview). Returns the number of
/// rows inserted.
fn refresh_matview_datafill(
    mcx: Mcx<'_>,
    dest: DestReceiverHandle,
    query: QueryHandle,
    query_string: &str,
    is_create: bool,
) -> PgResult<u64> {
    /*
     * Lock and rewrite, using a copy to preserve the original query.
     * copied_query = copyObject(query); AcquireRewriteLocks(...);
     * rewritten = QueryRewrite(copied_query);  `query` is rebound to the single
     * rewritten Query.
     */
    let (rewritten_len, query) = seam::rewrite_data_query::call(query)?;

    /* SELECT should never rewrite to more or less than one SELECT query */
    if rewritten_len != 1 {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(
                mcx,
                format_args!(
                    "unexpected rewrite result for {}",
                    if is_create {
                        "CREATE MATERIALIZED VIEW "
                    } else {
                        "REFRESH MATERIALIZED VIEW"
                    }
                ),
            )?)
            .into_error());
    }

    /* Check for user-requested abort. */
    seam::check_for_interrupts::call()?;

    /* Plan the query which will generate data for the refresh. */
    let plan = seam::pg_plan_query::call(query, query_string.to_string())?;

    /*
     * Use a snapshot with an updated command ID to ensure this query sees results
     * of any previously executed queries.
     * PushCopiedSnapshot(GetActiveSnapshot()); UpdateActiveSnapshotCommandId();
     */
    snapmgr_seam::push_copied_snapshot_and_bump::call()?;

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    let queryDesc = seam::create_query_desc::call(plan, query_string.to_string(), dest)?;

    /*
     * Bind the run-scoped DR_transientrel state for the executor run. C's
     * `CreateTransientRelDestReceiver` `palloc0`s the receiver and its private
     * fields are filled in `transientrel_startup`; the owned model has no
     * per-query arena at receiver-creation time, so the run owner allocates the
     * state here (carrying the receiver's `transientoid`) and binds it to the
     * receiver token for the duration of the run. `transientrel_startup` then
     * opens the relation and fills the rest, exactly as C does.
     */
    receiver_setup_run(dest.0, mcx)?;

    /* call ExecutorStart to prepare the plan for execution */
    seam::executor_start::call(queryDesc)?;

    /* run the plan */
    seam::executor_run::call(queryDesc)?;

    let processed = seam::query_desc_es_processed::call(queryDesc)?;

    /* and clean up: ExecutorFinish; ExecutorEnd; FreeQueryDesc */
    seam::executor_finish_end_free::call(queryDesc)?;

    snapmgr_seam::pop_active_snapshot::call()?;

    Ok(processed)
}

// ===========================================================================
// DR_transientrel state + per-backend receiver registry (matview.c 58-66)
// ===========================================================================
//
// matview.c's `DR_transientrel` is `palloc0`'d by `CreateTransientRelDestReceiver`
// and downcast (`(DR_transientrel *) self`) inside each callback. The owned model
// mirrors `createas.c`'s `DR_intorel`: a per-backend registry keyed by the
// router's `state` token holds the run-bound private fields, registered into the
// `backend-tcop-dest` value-router (the executor's dest dispatch).

/// The private `DR_transientrel` fields `transientrel_startup` fills and the
/// later callbacks consume (matview.c 58-66). `'mcx`-bound because `transientrel`
/// (an open relcache handle from `table_open`) and `bistate` live in the
/// per-query arena.
struct TransientRelStateData<'mcx> {
    /// `Oid transientoid` — the OID of the transient heap, set at receiver
    /// creation and read by `transientrel_startup`'s `table_open`.
    transientoid: Oid,
    /// `Relation transientrel` — the open transient relation (`None` until
    /// startup / once closed).
    transientrel: Option<Relation<'mcx>>,
    /// `CommandId output_cid` — cmin to stamp on inserted tuples.
    output_cid: CommandId,
    /// `int ti_options` — `table_tuple_insert` performance options.
    ti_options: i32,
    /// `BulkInsertState bistate` — bulk-insert state (`None` until startup).
    bistate: Option<PgBox<'mcx, BulkInsertStateData>>,
}

/// One registered `DR_transientrel` receiver. `transientoid` is set at receiver
/// creation (the C `self->transientoid = transientoid`); `state` is the raw
/// pointer to the `'mcx`-bound [`TransientRelStateData`] bound for the run.
struct ReceiverSlot {
    /// `((DR_transientrel *) self)->transientoid` — the transient heap OID the
    /// receiver was created for. Threaded into the run-bound state at startup.
    transientoid: Oid,
    /// Raw pointer to the live [`TransientRelStateData`] (set by
    /// `transientrel_startup`, cleared by `transientrel_shutdown`). Null when no
    /// run is in progress.
    state: *mut (),
}

thread_local! {
    static RECEIVERS: RefCell<Vec<Option<ReceiverSlot>>> =
        const { RefCell::new(Vec::new()) };
}

/// Allocate a fresh receiver slot carrying `transientoid` (1-based token; 0 is
/// never handed out).
fn receiver_register(transientoid: Oid) -> u64 {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        reg.push(Some(ReceiverSlot {
            transientoid,
            state: core::ptr::null_mut(),
        }));
        reg.len() as u64
    })
}

/// Read the `transientoid` the receiver was created for.
fn receiver_transientoid(token: u64) -> Oid {
    RECEIVERS.with(|r| {
        let reg = r.borrow();
        reg.get((token - 1) as usize)
            .and_then(|s| s.as_ref())
            .map(|s| s.transientoid)
            .unwrap_or_default()
    })
}

/// Bind the live `TransientRelStateData` pointer to a receiver token for the run.
fn receiver_bind(token: u64, state: *mut ()) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(Some(slot)) = reg.get_mut((token - 1) as usize) {
            slot.state = state;
        }
    });
}

/// Clear the bound state pointer after the run (the C `pfree(self)` invalidation).
fn receiver_unbind(token: u64) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(Some(slot)) = reg.get_mut((token - 1) as usize) {
            slot.state = core::ptr::null_mut();
        }
    });
}

/// Recover the live `TransientRelStateData` for a bound receiver token (the C
/// `(DR_transientrel *) self`).
///
/// SAFETY: the pointer is the arena-allocated state `transientrel_startup` bound
/// for the synchronous executor run; it is valid until `transientrel_shutdown`
/// clears it.
fn receiver_state<'mcx>(token: u64) -> &'mcx mut TransientRelStateData<'mcx> {
    let ptr = RECEIVERS.with(|r| {
        let reg = r.borrow();
        reg.get((token - 1) as usize)
            .and_then(|s| s.as_ref())
            .map(|s| s.state)
            .unwrap_or(core::ptr::null_mut())
    });
    if ptr.is_null() {
        panic!(
            "backend-commands-matview: transientrel callback on an unbound \
             DR_transientrel receiver"
        );
    }
    unsafe { &mut *(ptr as *mut TransientRelStateData<'mcx>) }
}

// ---------------------------------------------------------------------------
// CreateTransientRelDestReceiver (matview.c 464-477)
// ---------------------------------------------------------------------------

/// `CreateTransientRelDestReceiver` — allocate + wire the `DR_transientrel`
/// receiver that bulk-loads regenerated data into the transient heap, and
/// register it into the `backend-tcop-dest` value-router (mirroring `createas.c`'s
/// `CreateIntoRelDestReceiver`).
///
/// The C body `palloc0`s a `DR_transientrel`, wires its
/// `receiveSlot`/`rStartup`/`rShutdown`/`rDestroy` to the `transientrel_*`
/// callbacks with `mydest = DestTransientRel`, and stores `self->transientoid`.
/// The owned model records `transientoid` on the receiver slot; the per-query
/// `TransientRelStateData` is set up by `refresh_matview_datafill` (the run owner)
/// before the executor invokes `transientrel_startup`.
pub fn CreateTransientRelDestReceiver(transientoid: Oid) -> PgResult<DestReceiverHandle> {
    let token = receiver_register(transientoid);
    Ok(backend_tcop_dest::register_dest_receiver(
        CommandDest::TransientRel,
        backend_tcop_dest::ReceiverVtable {
            rStartup: transientrel_startup,
            receiveSlot: transientrel_receive,
            rShutdown: transientrel_shutdown,
        },
        token,
    ))
}

/// Set up the run-bound `TransientRelStateData` for a receiver: allocate it in
/// the per-query arena (carrying the receiver's `transientoid`, with
/// `transientrel`/`bistate` filled later by `transientrel_startup`), leak it to a
/// stable `'mcx` reference (it lives for the query, as the C `palloc`'d
/// `DR_transientrel` does), and bind its raw pointer to the receiver token for the
/// duration of the run.
///
/// This is the owned-model stand-in for C's `CreateTransientRelDestReceiver`
/// `palloc0`'ing the receiver: the receiver-creation site has no per-query arena,
/// so the run owner (`refresh_matview_datafill`) threads the arena in here.
fn receiver_setup_run<'mcx>(token: u64, mcx: Mcx<'mcx>) -> PgResult<()> {
    let transientoid = receiver_transientoid(token);
    let state = mcx::leak_in(mcx::alloc_in(
        mcx,
        TransientRelStateData {
            transientoid,
            transientrel: None,
            output_cid: 0,
            ti_options: 0,
            bistate: None,
        },
    )?);
    receiver_bind(token, state as *mut TransientRelStateData<'mcx> as *mut ());
    Ok(())
}

// ---------------------------------------------------------------------------
// transientrel_startup (matview.c 482-503)
// ---------------------------------------------------------------------------

/// `transientrel_startup` — executor startup for the transient-rel receiver (the
/// `rStartup` callback). `operation`/`typeinfo` are unused by the C body.
fn transientrel_startup<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    _operation: CmdType,
    _typeinfo: &TupleDescData<'mcx>,
) -> PgResult<()> {
    let st = receiver_state::<'mcx>(state);

    /* Open the transient relation we are about to fill. */
    let transientrel = backend_access_table_table::table_open(mcx, st.transientoid, NoLock)?;

    /*
     * Valid smgr_targblock implies something already wrote to the relation. This
     * may be harmless, but this function hasn't planned for it.
     *
     * C: Assert(RelationGetTargetBlock(transientrel) == InvalidBlockNumber).
     * `RelationGetTargetBlock` reads `rd_smgr->smgr_targblock`, which the trimmed
     * relcache projection does not carry; the C check is a debug-only `Assert`
     * (a no-op in release), so it is elided here — no logic depends on it.
     */

    st.transientrel = Some(transientrel);
    st.output_cid = backend_access_transam_xact::GetCurrentCommandId(true)?;
    st.ti_options = TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN;
    st.bistate = Some(mcx::alloc_in(
        mcx,
        backend_access_heap_heapam::GetBulkInsertState()?,
    )?);

    Ok(())
}

// ---------------------------------------------------------------------------
// transientrel_receive (matview.c 508-531)
// ---------------------------------------------------------------------------

/// `transientrel_receive` — receive one tuple (insert it into the transient
/// heap); returns the C `true` (the `receiveSlot` callback).
fn transientrel_receive<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let st = receiver_state::<'mcx>(state);

    /*
     * Note that the input slot might not be of the type of the target relation.
     * That's supported by table_tuple_insert(), but slightly less efficient than
     * inserting with the right slot - but the alternative would be to copy into a
     * slot of the right type, which would not be cheap either.
     */
    let rel = st
        .transientrel
        .as_ref()
        .expect("transientrel_receive: DR_transientrel relation is open");
    let bistate = st.bistate.as_deref_mut();
    backend_access_table_tableam::table_tuple_insert(
        mcx,
        rel,
        slot,
        st.output_cid,
        st.ti_options,
        bistate,
    )?;

    /* We know this is a newly created relation, so there are no indexes. */
    Ok(true)
}

// ---------------------------------------------------------------------------
// transientrel_shutdown (matview.c 536-548)
// ---------------------------------------------------------------------------

/// `transientrel_shutdown` — executor end for the transient-rel receiver (the
/// `rShutdown` callback).
fn transientrel_shutdown<'mcx>(mcx: Mcx<'mcx>, state: u64) -> PgResult<()> {
    let _ = mcx;
    let st = receiver_state::<'mcx>(state);

    if let Some(bistate) = st.bistate.as_deref_mut() {
        backend_access_heap_heapam::FreeBulkInsertState(bistate);
    }

    {
        let rel = st
            .transientrel
            .as_ref()
            .expect("transientrel_shutdown: DR_transientrel relation is open");
        /*
         * table_finish_bulk_insert(transientrel, ti_options): the heap AM's
         * `finish_bulk_insert` vtable slot is not yet ported, so this stays on the
         * frontier (mirror-PG-and-panic), matching `backend-commands-createas`.
         */
        seam::table_finish_bulk_insert::call(rel, st.ti_options)?;
    }

    /* close transientrel, but keep lock until commit */
    if let Some(rel) = st.transientrel.take() {
        backend_access_table_table::table_close(rel, NoLock)?;
    }

    /* myState->transientrel = NULL: release the run-bound state pointer. */
    receiver_unbind(state);

    Ok(())
}

// ---------------------------------------------------------------------------
// transientrel_destroy (matview.c 553-557)
// ---------------------------------------------------------------------------

/// `transientrel_destroy` — release the DestReceiver object (C: `pfree(self)`).
///
/// In the owned model the `DR_transientrel` is a [`ReceiverSlot`] in the
/// per-backend registry plus the arena-allocated [`TransientRelStateData`]; the
/// latter is freed when its query context resets (as the C `palloc` is), and the
/// slot is a small fixed entry, so there is nothing to `pfree` here. The
/// dest-router vtable does not carry `rDestroy` (it is the owner's teardown path;
/// see `backend_tcop_dest`), so this is not wired into the vtable — it exists for
/// C-function parity and to document the no-op (matching `createas.c`'s
/// `intorel_destroy`).
#[allow(dead_code)]
fn transientrel_destroy(_self: DestReceiverHandle) {
    /* pfree(self): the registry slot + arena state are reclaimed by context
     * reset; nothing to free explicitly. */
}

// ---------------------------------------------------------------------------
// make_temptable_name_n (matview.c 570-579)
// ---------------------------------------------------------------------------

/// `make_temptable_name_n` — append `_n` to a qualified temp-table name. C
/// builds it in a `StringInfoData` and returns the palloc'd buffer; the port
/// builds it into a context-allocated [`PgString`] and returns its `&str`-clone.
fn make_temptable_name_n(mcx: Mcx<'_>, tempname: &str, n: i32) -> PgResult<String> {
    /*
     * initStringInfo(&namebuf); appendStringInfoString(&namebuf, tempname);
     * appendStringInfo(&namebuf, "_%d", n); return namebuf.data;
     */
    let mut namebuf = PgString::new_in(mcx);
    namebuf.try_push_str(tempname)?;
    use core::fmt::Write;
    write!(namebuf, "_{n}").map_err(|_| mcx.oom(0))?;
    Ok(namebuf.as_str().to_string())
}

// ---------------------------------------------------------------------------
// refresh_by_match_merge (matview.c 613-897)
// ---------------------------------------------------------------------------

/// `refresh_by_match_merge` — refresh with transactional semantics while
/// allowing concurrent reads, via a FULL OUTER JOIN diff against the old data
/// and set-based DELETE / INSERT.
fn refresh_by_match_merge(
    mcx: Mcx<'_>,
    matviewOid: Oid,
    tempOid: Oid,
    relowner: Oid,
    save_sec_context: i32,
) -> PgResult<()> {
    /* StringInfoData querybuf — the working SQL buffer, rebuilt at each use. */
    let mut querybuf = PgString::new_in(mcx);

    let matviewRel = seam::table_open::call(matviewOid, NoLock)?;
    let matviewname = seam::quote_qualified_relname::call(matviewRel)?;
    let tempRel = seam::table_open::call(tempOid, NoLock)?;
    let tempname = seam::quote_qualified_relname::call(tempRel)?;
    let diffname = make_temptable_name_n(mcx, &tempname, 2)?;

    let relnatts = seam::relation_num_attrs::call(matviewRel)?;

    /* `resetStringInfo(&querybuf); appendStringInfo(&querybuf, ...)` */
    macro_rules! set_querybuf {
        ($($arg:tt)*) => {{
            querybuf.clear();
            use core::fmt::Write;
            write!(querybuf, $($arg)*).map_err(|_| mcx.oom(0))?;
        }};
    }
    /* `appendStringInfoString(&querybuf, s)` */
    macro_rules! append_querybuf {
        ($($arg:tt)*) => {{
            use core::fmt::Write;
            write!(querybuf, $($arg)*).map_err(|_| mcx.oom(0))?;
        }};
    }

    /* Open SPI context. */
    seam::spi_connect::call()?;

    /* Analyze the temp table with the new contents. */
    set_querybuf!("ANALYZE {tempname}");
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_UTILITY {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }

    /*
     * We need to ensure that there are not duplicate rows without NULLs in the
     * new data set before we can count on the "diff" results.  Check for that in
     * a way that allows showing the first duplicated row found.  Even after we
     * pass this test, a unique index on the materialized view may find a
     * duplicate key problem.
     *
     * Note: here and below, we use "tablename.*::tablerowtype" as a hack to keep
     * ".*" from being expanded into multiple columns in a SELECT list.  Compare
     * ruleutils.c's get_variable().
     */
    set_querybuf!(
        "SELECT newdata.*::{tempname} FROM {tempname} newdata \
         WHERE newdata.* IS NOT NULL AND EXISTS \
         (SELECT 1 FROM {tempname} newdata2 WHERE newdata2.* IS NOT NULL \
         AND newdata2.* OPERATOR(pg_catalog.*=) newdata.* \
         AND newdata2.ctid OPERATOR(pg_catalog.<>) \
         newdata.ctid)"
    );
    if seam::spi_execute::call(querybuf.as_str().to_string(), false, 1)? != SPI_OK_SELECT {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }
    if seam::spi_processed::call()? > 0 {
        /*
         * Note that this ereport() is returning data to the user.  Generally, we
         * would want to make sure that the user has been granted access to this
         * data.  However, REFRESH MAT VIEW is only able to be run by the owner of
         * the mat view (or a superuser) and therefore there is no need to check
         * for access to data in the mat view.
         */
        let matviewRelName = seam::relation_get_relname::call(matviewRel)?;
        let row = seam::spi_getvalue_first::call()?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_CARDINALITY_VIOLATION)
            .errmsg(fmt(
                mcx,
                format_args!(
                    "new data for materialized view \"{matviewRelName}\" contains duplicate rows without any null columns"
                ),
            )?)
            .errdetail(fmt(mcx, format_args!("Row: {row}"))?)
            .into_error());
    }

    /*
     * Create the temporary "diff" table.
     *
     * Temporarily switch out of the SECURITY_RESTRICTED_OPERATION context,
     * because you cannot create temp tables in SRO context.  For extra paranoia,
     * add the composite type column only after switching back to SRO context.
     */
    seam::set_user_id_and_sec_context::call(
        relowner,
        save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
    )?;
    set_querybuf!("CREATE TEMP TABLE {diffname} (tid pg_catalog.tid)");
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_UTILITY {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }
    seam::set_user_id_and_sec_context::call(
        relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    )?;
    set_querybuf!("ALTER TABLE {diffname} ADD COLUMN newdata {tempname}");
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_UTILITY {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }

    /* Start building the query for populating the diff table. */
    set_querybuf!(
        "INSERT INTO {diffname} \
         SELECT mv.ctid AS tid, newdata.*::{tempname} AS newdata \
         FROM {matviewname} mv FULL JOIN {tempname} newdata ON ("
    );

    /*
     * Get the list of index OIDs for the table from the relcache, and look up
     * each one in the pg_index syscache.  We will test for equality on all
     * columns present in all unique indexes which only reference columns and
     * include all rows.
     *
     * tupdesc = matviewRel->rd_att;
     * opUsedForQual = (Oid *) palloc0(sizeof(Oid) * relnatts);
     */
    let relnatts = relnatts.max(0) as usize;
    if relnatts > MaxHeapAttributeNumber as usize {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(
                mcx,
                format_args!(
                    "materialized view has too many attributes: {relnatts} > {MaxHeapAttributeNumber}"
                ),
            )?)
            .into_error());
    }
    let mut op_used_for_qual: PgVec<Oid> = PgVec::new_in(mcx);
    op_used_for_qual
        .try_reserve(relnatts)
        .map_err(|_| mcx.oom(relnatts * core::mem::size_of::<Oid>()))?;
    for _ in 0..relnatts {
        op_used_for_qual.push(Oid::default());
    }
    let mut foundUniqueIndex = false;

    let indexoidlist = seam::relation_get_index_list::call(matviewRel)?;

    for &indexoid in &indexoidlist {
        let indexRel = seam::index_open::call(indexoid, RowExclusiveLock)?;
        if is_usable_unique_index(indexRel)? {
            /*
             * Resolve, for each key column of this usable unique index, the
             * equality operator + the leftop/rightop/attrtype (matview.c
             * 741-817).  The opclass / pg_opclass / get_opfamily_member /
             * attribute reads are the genuine cross-subsystem externals; the
             * opUsedForQual de-dup and the generate_operator_clause emission stay
             * in-crate.
             */
            let quals = seam::index_match_merge_quals::call(indexRel, matviewRel)?;

            /* Add quals for all columns from this index. */
            for qual in &quals {
                let attnum = qual.attnum;
                let op = qual.op;

                /*
                 * If we find the same column with the same equality semantics in
                 * more than one index, we only need to emit the equality clause
                 * once.
                 *
                 * Since we only remember the last equality operator, this code
                 * could be fooled into emitting duplicate clauses given multiple
                 * indexes with several different opclasses ... but that's so
                 * unlikely it doesn't seem worth spending extra code to avoid.
                 */
                if op_used_for_qual[(attnum - 1) as usize] == op {
                    continue;
                }
                op_used_for_qual[(attnum - 1) as usize] = op;

                /* Actually add the qual, ANDed with any others. */
                if foundUniqueIndex {
                    append_querybuf!(" AND ");
                }

                /*
                 * leftop  = quote_qualified_identifier("newdata", attr->attname);
                 * rightop = quote_qualified_identifier("mv",      attr->attname);
                 * generate_operator_clause(&querybuf, leftop, attrtype, op,
                 *                          rightop, attrtype);
                 */
                append_querybuf!(
                    "{}",
                    seam::generate_operator_clause::call(qual.clone())?
                );

                foundUniqueIndex = true;
            }
        }

        /* Keep the locks, since we're about to run DML which needs them. */
        seam::index_close::call(indexRel, NoLock)?;
    }

    drop(indexoidlist); /* list_free(indexoidlist) */

    /*
     * There must be at least one usable unique index on the matview.
     *
     * ExecRefreshMatView() checks that after taking the exclusive lock on the
     * matview. So at least one unique index is guaranteed to exist here because
     * the lock is still being held.  (One known exception is if a function
     * called as part of refreshing the matview drops the index.  That's a pretty
     * silly thing to do.)
     */
    if !foundUniqueIndex {
        let matviewRelName = seam::relation_get_relname::call(matviewRel)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(fmt(
                mcx,
                format_args!("could not find suitable unique index on materialized view \"{matviewRelName}\""),
            )?)
            .into_error());
    }

    append_querybuf!(
        " AND newdata.* OPERATOR(pg_catalog.*=) mv.*) \
         WHERE newdata.* IS NULL OR mv.* IS NULL \
         ORDER BY tid"
    );

    /* Populate the temporary "diff" table. */
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_INSERT {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }

    /*
     * We have no further use for data from the "full-data" temp table, but we
     * must keep it around because its type is referenced from the diff table.
     */

    /* Analyze the diff table. */
    set_querybuf!("ANALYZE {diffname}");
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_UTILITY {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }

    OpenMatViewIncrementalMaintenance();

    /* Deletes must come before inserts; do them first. */
    set_querybuf!(
        "DELETE FROM {matviewname} mv WHERE ctid OPERATOR(pg_catalog.=) ANY \
         (SELECT diff.tid FROM {diffname} diff \
         WHERE diff.tid IS NOT NULL \
         AND diff.newdata IS NULL)"
    );
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_DELETE {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }

    /* Inserts go last. */
    set_querybuf!(
        "INSERT INTO {matviewname} SELECT (diff.newdata).* \
         FROM {diffname} diff WHERE tid IS NULL"
    );
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_INSERT {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }

    /* We're done maintaining the materialized view. */
    CloseMatViewIncrementalMaintenance();
    seam::table_close::call(tempRel, NoLock)?;
    seam::table_close::call(matviewRel, NoLock)?;

    /* Clean up temp tables. */
    set_querybuf!("DROP TABLE {diffname}, {tempname}");
    if seam::spi_exec::call(querybuf.as_str().to_string())? != SPI_OK_UTILITY {
        return Err(ereport(ERROR)
            .errmsg_internal(fmt(mcx, format_args!("SPI_exec failed: {}", querybuf.as_str()))?)
            .into_error());
    }

    /* Close SPI context. */
    if seam::spi_finish::call()? != SPI_OK_FINISH {
        return Err(ereport(ERROR)
            .errmsg_internal("SPI_finish failed")
            .into_error());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// refresh_by_heap_swap (matview.c 904-909)
// ---------------------------------------------------------------------------

/// `refresh_by_heap_swap` — swap the physical files of the target and transient
/// tables, rebuild the target's indexes, and throw away the transient table.
fn refresh_by_heap_swap(matviewOid: Oid, OIDNewHeap: Oid, relpersistence: i8) -> PgResult<()> {
    /*
     * finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
     *                  RecentXmin, ReadNextMultiXactId(), relpersistence);
     * The fixed boolean flags and the RecentXmin / ReadNextMultiXactId() args are
     * read by the runtime; only the variable args cross the seam.
     */
    seam::finish_heap_swap::call(matviewOid, OIDNewHeap, relpersistence)
}

// ---------------------------------------------------------------------------
// is_usable_unique_index (matview.c 914-949)
// ---------------------------------------------------------------------------

/// `is_usable_unique_index` — whether the specified index is usable for match
/// merge (unique, valid, immediate, non-partial, plain user columns only).
fn is_usable_unique_index(indexRel: Oid) -> PgResult<bool> {
    /* Form_pg_index indexStruct = indexRel->rd_index; (+ index predicate) */
    let indexStruct: IndexUsabilityInfo = seam::index_usability_info::call(indexRel)?;

    /*
     * Must be unique, valid, immediate, non-partial, and be defined over plain
     * user columns (not expressions).
     */
    if indexStruct.indisunique
        && indexStruct.indimmediate
        && indexStruct.indisvalid
        && indexStruct.pred_is_nil
        && indexStruct.indnatts > 0
    {
        /*
         * The point of groveling through the index columns individually is to
         * reject both index expressions and system columns.  Currently, matviews
         * couldn't have OID columns so there's no way to create an index on a
         * system column; but maybe someday that wouldn't be true, so let's be
         * safe.
         */
        let numatts = indexStruct.indnatts;

        for i in 0..numatts {
            let attnum = indexStruct.indkey[i as usize];

            if attnum <= 0 {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// MatViewIncrementalMaintenanceIsEnabled (matview.c 963-967)
// ---------------------------------------------------------------------------

/// `MatViewIncrementalMaintenanceIsEnabled` — whether the backend is in a
/// context where DML may modify materialized views.
pub fn MatViewIncrementalMaintenanceIsEnabled() -> bool {
    /* return matview_maintenance_depth > 0; */
    MATVIEW_MAINTENANCE_DEPTH.with(Cell::get) > 0
}

// ---------------------------------------------------------------------------
// OpenMatViewIncrementalMaintenance (matview.c 969-973)
// ---------------------------------------------------------------------------

/// `OpenMatViewIncrementalMaintenance` — increment the maintenance depth.
fn OpenMatViewIncrementalMaintenance() {
    /* matview_maintenance_depth++; */
    MATVIEW_MAINTENANCE_DEPTH.with(|d| d.set(d.get() + 1));
}

// ---------------------------------------------------------------------------
// CloseMatViewIncrementalMaintenance (matview.c 975-980)
// ---------------------------------------------------------------------------

/// `CloseMatViewIncrementalMaintenance` — decrement the maintenance depth.
fn CloseMatViewIncrementalMaintenance() {
    /* matview_maintenance_depth--; Assert(matview_maintenance_depth >= 0); */
    MATVIEW_MAINTENANCE_DEPTH.with(|d| {
        let v = d.get() - 1;
        d.set(v);
        debug_assert!(v >= 0);
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an error-message `String` from `format_args!` into a context-allocated
/// [`PgString`], surfacing an allocator refusal as the context's OOM error
/// (every C palloc in these message paths can `ereport(ERROR, OUT_OF_MEMORY)`).
fn fmt(mcx: Mcx<'_>, args: core::fmt::Arguments<'_>) -> PgResult<String> {
    use core::fmt::Write;
    let mut s = PgString::new_in(mcx);
    s.write_fmt(args).map_err(|_| mcx.oom(0))?;
    Ok(s.as_str().to_string())
}

// ---------------------------------------------------------------------------
// Seam installation (every inward seam in backend-commands-matview-seams)
// ---------------------------------------------------------------------------

/// Install every seam this crate owns. The `ExecRefreshMatView` /
/// `RefreshMatViewByOid` / `SetMatViewPopulatedState` shims marshal the
/// `Mcx`-free seam signatures onto the `Mcx`-taking implementations by spinning
/// up a per-call working memory context (the C `CurrentMemoryContext`).
pub fn init_seams() {
    use backend_commands_matview_seams as s;

    s::ExecRefreshMatView::set(|stmt, query_string, qc| {
        let ctx = mcx::MemoryContext::new("ExecRefreshMatView");
        ExecRefreshMatView(ctx.mcx(), &stmt, &query_string, qc)
    });
    s::RefreshMatViewByOid::set(
        |matview_oid, is_create, skip_data, concurrent, query_string, qc| {
            let ctx = mcx::MemoryContext::new("RefreshMatViewByOid");
            RefreshMatViewByOid(
                ctx.mcx(),
                matview_oid,
                is_create,
                skip_data,
                concurrent,
                &query_string,
                qc,
            )
        },
    );
    s::SetMatViewPopulatedState::set(|relation, newstate| {
        let ctx = mcx::MemoryContext::new("SetMatViewPopulatedState");
        SetMatViewPopulatedState(ctx.mcx(), relation, newstate)
    });
    s::MatViewIncrementalMaintenanceIsEnabled::set(MatViewIncrementalMaintenanceIsEnabled);

    // The DR_transientrel receiver (CreateTransientRelDestReceiver +
    // transientrel_startup/receive/shutdown/destroy) is owned in-crate and
    // registered into the backend-tcop-dest value-router at receiver-creation
    // time (see CreateTransientRelDestReceiver), mirroring createas's DR_intorel.
    // It is not a cross-crate seam, so nothing is installed here for it.
}
