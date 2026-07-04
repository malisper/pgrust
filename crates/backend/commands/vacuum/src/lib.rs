//! vacuum.c: ExecVacuum -> vacuum -> vacuum_rel for named tables with
//! partition/inheritance expansion plus the TOAST recursion. parallel and
//! database-wide/database-stats arms are loud named panics;
//! vac_update_datfrozenxid is a recorded gap.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use ::elog::ereport;
use ::mcx::Mcx;
use ::tableam_vocab::{
    VacOptValue, VacuumCutoffs, VacuumParams, VACOPT_ANALYZE, VACOPT_DISABLE_PAGE_SKIPPING,
    VACOPT_FREEZE, VACOPT_FULL, VACOPT_ONLY_DATABASE_STATS, VACOPT_PROCESS_MAIN,
    VACOPT_PROCESS_TOAST,
    VACOPT_SKIP_DATABASE_STATS, VACOPT_SKIP_LOCKED, VACOPT_VACUUM, VACOPT_VERBOSE,
};
use ::types_core::xact::{
    FirstNormalTransactionId, InvalidTransactionId, MultiXactIdPrecedes,
    MultiXactIdPrecedesOrEquals, TransactionIdIsNormal, TransactionIdPrecedes,
    TransactionIdPrecedesOrEquals,
};
use ::types_core::{BlockNumber, InvalidOid, MultiXactId, Oid};
use ::types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_TABLE, ERROR, WARNING,
};
use ::types_nodes::parsenodes::VacuumStmt;
use ::types_nodes::NodeList;
use ::types_rel::lock::{AccessShareLock, NoLock, ShareUpdateExclusiveLock};
use ::types_rel::pg_class::{
    RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use ::types_rel::{Relation, RelationData, LOCKMODE};
use ::types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};

use multixact::{
    FirstMultiXactId, GetOldestMultiXactId, MultiXactIdIsValid, MultiXactMemberFreezeThreshold,
    ReadNextMultiXactId,
};

/// The two shared counters C keeps in PVShared and points
/// VacuumSharedCostBalance/VacuumActiveNWorkers at (vacuum.h externs).
/// Thread-native home: one Arc shared by leader and workers.
pub struct VacuumSharedCost {
    pub cost_balance: std::sync::atomic::AtomicU32,
    pub active_nworkers: std::sync::atomic::AtomicU32,
}

thread_local! {
    static IN_VACUUM: Cell<bool> = const { Cell::new(false) };
    static VACUUM_FAILSAFE_ACTIVE: Cell<bool> = const { Cell::new(false) };
    // Some = VacuumSharedCostBalance/VacuumActiveNWorkers non-NULL in C.
    static VACUUM_SHARED_COST: std::cell::RefCell<Option<std::sync::Arc<VacuumSharedCost>>> =
        const { std::cell::RefCell::new(None) };
    static VACUUM_COST_BALANCE_LOCAL: Cell<i32> = const { Cell::new(0) };
    // C's working copies (vacuum.c `vacuum_cost_delay`/`vacuum_cost_limit`),
    // distinct from the VacuumCostDelay/VacuumCostLimit GUC storage:
    // VacuumUpdateCosts writes these, never the GUC vars.
    static VACUUM_COST_DELAY: Cell<f64> = const { Cell::new(0.0) };
    static VACUUM_COST_LIMIT: Cell<i32> = const { Cell::new(200) };
}

pub fn vacuum_cost_delay() -> f64 {
    VACUUM_COST_DELAY.get()
}

pub fn set_vacuum_cost_delay(v: f64) {
    VACUUM_COST_DELAY.set(v);
}

pub fn vacuum_cost_limit() -> i32 {
    VACUUM_COST_LIMIT.get()
}

pub fn set_vacuum_cost_limit(v: i32) {
    VACUUM_COST_LIMIT.set(v);
}

pub fn VacuumFailsafeActive() -> bool {
    VACUUM_FAILSAFE_ACTIVE.get()
}

pub fn SetVacuumFailsafeActive(v: bool) {
    VACUUM_FAILSAFE_ACTIVE.set(v);
}

pub fn vacuum_shared_cost() -> Option<std::sync::Arc<VacuumSharedCost>> {
    VACUUM_SHARED_COST.with(|c| c.borrow().clone())
}

pub fn set_vacuum_shared_cost(v: Option<std::sync::Arc<VacuumSharedCost>>) {
    VACUUM_SHARED_COST.with(|c| *c.borrow_mut() = v);
}

pub fn set_vacuum_cost_balance_local(v: i32) {
    VACUUM_COST_BALANCE_LOCAL.set(v);
}

// C's static in_vacuum (vacuum.c); commands_analyze's ANALYZE entry shares it.
pub fn in_vacuum() -> bool {
    IN_VACUUM.get()
}

pub fn set_in_vacuum(v: bool) {
    IN_VACUUM.set(v);
}

pub fn ExecVacuum<'mcx>(
    mcx: Mcx<'mcx>,
    vacstmt: &VacuumStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<()> {
    let mut params = VacuumParams {
        options: 0,
        freeze_min_age: -1,
        freeze_table_age: -1,
        multixact_freeze_min_age: -1,
        multixact_freeze_table_age: -1,
        is_wraparound: false,
        log_min_duration: -1,
        index_cleanup: VacOptValue::Unspecified,
        truncate: VacOptValue::Unspecified,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: 0.0,
        nworkers: 0,
    };

    let mut verbose = false;
    let mut skip_locked = false;
    let mut full = false;
    let mut analyze = false;
    let mut freeze = false;
    let mut disable_page_skipping = false;
    let mut process_main = true;
    let mut process_toast = true;
    let mut skip_database_stats = false;
    let mut only_database_stats = false;
    for opt_node in vacstmt.options.iter() {
        let opt = opt_node.as_def_elem().expect("VacuumStmt option is DefElem");
        match opt.defname.unwrap_or("") {
            "verbose" => verbose = explain::defGetBoolean(opt)?,
            "skip_locked" => skip_locked = explain::defGetBoolean(opt)?,
            "analyze" => analyze = explain::defGetBoolean(opt)?,
            "index_cleanup" => {
                params.index_cleanup = if opt.arg.is_none() {
                    VacOptValue::Auto
                } else if explain::defGetString(mcx, opt)?.eq_ignore_ascii_case("auto") {
                    VacOptValue::Auto
                } else if explain::defGetBoolean(opt)? {
                    VacOptValue::Enabled
                } else {
                    VacOptValue::Disabled
                };
            }
            "full" => full = explain::defGetBoolean(opt)?,
            "freeze" => freeze = explain::defGetBoolean(opt)?,
            "disable_page_skipping" => {
                disable_page_skipping = explain::defGetBoolean(opt)?
            }
            "truncate" => {
                params.truncate = if explain::defGetBoolean(opt)? {
                    VacOptValue::Enabled
                } else {
                    VacOptValue::Disabled
                };
            }
            "process_main" => process_main = explain::defGetBoolean(opt)?,
            "process_toast" => process_toast = explain::defGetBoolean(opt)?,
            "skip_database_stats" => skip_database_stats = explain::defGetBoolean(opt)?,
            "only_database_stats" => only_database_stats = explain::defGetBoolean(opt)?,
            "parallel" => {
                // MAX_PARALLEL_WORKER_LIMIT (bgworker_internals.h)
                const MAX_PARALLEL_WORKER_LIMIT: i32 = 1024;
                if opt.arg.is_none() {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!(
                            "parallel option requires a value between 0 and {MAX_PARALLEL_WORKER_LIMIT}"
                        ))
                        .into_error()
                        .into());
                }
                let nworkers = commands_define::defGetInt32(opt)?;
                if !(0..=MAX_PARALLEL_WORKER_LIMIT).contains(&nworkers) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!(
                            "parallel workers for vacuum must be between 0 and {MAX_PARALLEL_WORKER_LIMIT}"
                        ))
                        .into_error()
                        .into());
                }
                params.nworkers = if nworkers == 0 { -1 } else { nworkers };
            }
            name @ "buffer_usage_limit" => {
                if explain::defGetBoolean(opt).unwrap_or(true) {
                    unported_option(name);
                }
            }
            name => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized VACUUM option \"{name}\""))
                    .into_error()
                    .into())
            }
        }
    }

    if !vacstmt.is_vacuumcmd {
        unported("ExecVacuum: ANALYZE statement (analyze.c lane)");
    }

    params.options = VACOPT_VACUUM
        | (if process_main { VACOPT_PROCESS_MAIN } else { 0 })
        | (if process_toast { VACOPT_PROCESS_TOAST } else { 0 })
        | (if verbose { VACOPT_VERBOSE } else { 0 })
        | (if skip_locked { VACOPT_SKIP_LOCKED } else { 0 })
        | (if freeze { VACOPT_FREEZE } else { 0 })
        | (if disable_page_skipping { VACOPT_DISABLE_PAGE_SKIPPING } else { 0 })
        | (if full { VACOPT_FULL } else { 0 })
        | (if analyze { VACOPT_ANALYZE } else { 0 })
        | (if skip_database_stats { VACOPT_SKIP_DATABASE_STATS } else { 0 })
        | (if only_database_stats { VACOPT_ONLY_DATABASE_STATS } else { 0 });

    if full && disable_page_skipping {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL")
            .into_error()
            .into());
    }

    if full && !process_toast {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("PROCESS_TOAST required with VACUUM FULL")
            .into_error()
            .into());
    }

    if full && params.nworkers > 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("VACUUM FULL cannot be performed in parallel")
            .into_error()
            .into());
    }

    if freeze {
        params.freeze_min_age = 0;
        params.freeze_table_age = 0;
        params.multixact_freeze_min_age = 0;
        params.multixact_freeze_table_age = 0;
    }

    let bstrategy = bufmgr_seams::get_access_strategy::call(BufferAccessStrategyType::BasVacuum);

    vacuum(mcx, &vacstmt.rels, &params, bstrategy, is_top_level)
}

pub fn vacuum<'mcx>(
    mcx: Mcx<'mcx>,
    relations: &NodeList<'mcx>,
    params: &VacuumParams,
    bstrategy: BufferAccessStrategy,
    is_top_level: bool,
) -> PgResult<()> {
    debug_assert!(params.options & (VACOPT_VACUUM | VACOPT_ANALYZE) != 0);
    // ANALYZE-only callers here are the autovacuum worker (never in a
    // transaction block); ANALYZE statements go through commands_analyze.
    if params.options & VACOPT_VACUUM != 0 {
        xact::PreventInTransactionBlock(is_top_level, "VACUUM")?;
    } else {
        debug_assert!(
            miscinit::GetMyBackendType() == types_core::BackendType::AutovacWorker,
            "ANALYZE-only vacuum() caller must be the autovacuum worker"
        );
        if xact::IsInTransactionBlock(is_top_level) {
            unported("vacuum: ANALYZE inside a transaction block (use_own_xacts=false)");
        }
    }

    if IN_VACUUM.get() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("VACUUM cannot be executed from VACUUM or ANALYZE")
            .into_error()
            .into());
    }

    if params.options & VACOPT_ONLY_DATABASE_STATS != 0 {
        unported("vacuum: ONLY_DATABASE_STATS");
    }
    if relations.is_nil() {
        unported("get_all_vacuum_rels (database-wide VACUUM)");
    }

    let mut vacrels: ::mcx::PgVec<'mcx, ExpandedVacRel<'mcx>> = ::mcx::PgVec::new_in(mcx);
    for vrel_node in relations.iter() {
        let vrel = vrel_node
            .as_vacuum_relation()
            .expect("vacuum relation list holds VacuumRelation");
        if !vrel.va_cols.is_nil() && params.options & VACOPT_ANALYZE == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("ANALYZE option must be specified when column lists are provided")
                .into_error()
                .into());
        }
        expand_vacuum_rel(mcx, vrel, params.options, &mut vacrels)?;
    }

    if snapmgr::ActiveSnapshotSet() {
        snapmgr::PopActiveSnapshot()?;
    }
    xact::CommitTransactionCommand()?;

    IN_VACUUM.set(true);
    VACUUM_FAILSAFE_ACTIVE.set(false);
    autovacuum_seams::vacuum_update_costs::call()?;
    init_small::globals::SetVacuumCostBalance(0);
    // catch_unwind = C's PG_FINALLY: panics become ERRORs at the tcop
    // boundary and the session survives, so in_vacuum must reset here too.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PgResult<()> {
        for vrel in vacrels.iter() {
            if params.options & VACOPT_VACUUM != 0 {
                let params_copy = *params;
                if !vacuum_rel(mcx, vrel.oid, vrel.relname, &params_copy, bstrategy.clone())? {
                    continue;
                }
            }
            if params.options & VACOPT_ANALYZE != 0 {
                xact::StartTransactionCommand()?;
                let snapshot = snapmgr::GetTransactionSnapshot()?;
                snapmgr::PushActiveSnapshot(&snapshot)?;
                commands_analyze_seams::analyze_rel::call(
                    mcx,
                    vrel.oid,
                    vrel.relname,
                    vrel.va_cols,
                    params.options,
                    false,
                )?;
                snapmgr::PopActiveSnapshot()?;
                xact::CommandCounterIncrement()?;
                xact::CommitTransactionCommand()?;
            }
            // Reset before vacuuming the next relation (C loop tail).
            VACUUM_FAILSAFE_ACTIVE.set(false);
        }
        Ok(())
    }));
    IN_VACUUM.set(false);
    init_small::globals::SetVacuumCostActive(false);
    VACUUM_FAILSAFE_ACTIVE.set(false);
    init_small::globals::SetVacuumCostBalance(0);
    match result {
        Ok(r) => r?,
        Err(p) => std::panic::resume_unwind(p),
    }

    // Matches the CommitTransaction waiting in PostgresMain.
    xact::StartTransactionCommand()?;

    if params.options & VACOPT_VACUUM != 0
        && params.options & VACOPT_SKIP_DATABASE_STATS == 0
    {
        vac_update_datfrozenxid(mcx)?;
    }
    Ok(())
}

pub struct ExpandedVacRel<'mcx> {
    pub oid: Oid,
    pub relname: Option<&'mcx str>,
    pub va_cols: &'mcx NodeList<'mcx>,
}

/// expand_vacuum_rel (vacuum.c): resolve the named table and, unless ONLY,
/// append its partitions/inheritance children. The transient AccessShareLock
/// is released before return, C-exact. vacuum_is_permitted_for_relation is
/// skipped (single-user milestone).
pub fn expand_vacuum_rel<'mcx>(
    mcx: Mcx<'mcx>,
    vrel: &'mcx types_nodes::parsenodes::VacuumRelation<'mcx>,
    options: u32,
    vacrels: &mut ::mcx::PgVec<'mcx, ExpandedVacRel<'mcx>>,
) -> PgResult<()> {
    if vrel.oid != InvalidOid {
        vacrels.push(ExpandedVacRel { oid: vrel.oid, relname: None, va_cols: &vrel.va_cols });
        return Ok(());
    }
    let rv = vrel
        .relation
        .and_then(|n| n.as_range_var())
        .expect("VacuumRelation.relation is RangeVar");
    let relname = rv.relname.expect("RangeVar.relname");
    // RVR_SKIP_LOCKED elided: single-backend, the lock is always available.
    let relid = namespace_seams::range_var_get_relid::call(
        mcx,
        &rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname,
            inh: rv.inh,
            relpersistence: rv.relpersistence,
            location: rv.location,
        },
        AccessShareLock,
        false,
    )?;
    let class_shape = syscache_seams::lookup_pg_class_ls_shape::call(relid)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));

    vacrels.push(ExpandedVacRel { oid: relid, relname: Some(relname), va_cols: &vrel.va_cols });

    let include_children = rv.inh;
    let is_partitioned_table =
        class_shape.relkind as u8 == types_rel::pg_class::RELKIND_PARTITIONED_TABLE;
    if options & VACOPT_VACUUM != 0 && is_partitioned_table && !include_children {
        ereport(WARNING)
            .errmsg(format!(
                "VACUUM ONLY of partitioned table \"{relname}\" has no effect"
            ))
            .finish(loc("expand_vacuum_rel"))?;
    }

    if include_children {
        for &part_oid in pg_inherits::find_all_inheritors(mcx, relid, NoLock)?.iter() {
            if part_oid == relid {
                continue;
            }
            vacrels.push(ExpandedVacRel { oid: part_oid, relname: None, va_cols: &vrel.va_cols });
        }
    }
    lmgr_seams::unlock_relation_oid::call(relid, AccessShareLock)?;
    Ok(())
}

fn vacuum_rel<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relname: Option<&str>,
    params: &VacuumParams,
    bstrategy: BufferAccessStrategy,
) -> PgResult<bool> {
    xact::StartTransactionCommand()?;
    // C divergence (recorded): PROC_IN_VACUUM/PROC_VACUUM_FOR_WRAPAROUND
    // statusFlags are not set (single-backend milestone; they only shape how
    // concurrent backends compute their horizons).
    let snapshot = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snapshot)?;

    let lmode = if params.options & VACOPT_FULL != 0 {
        types_rel::lock::AccessExclusiveLock
    } else {
        ShareUpdateExclusiveLock
    };
    let rel = match vacuum_open_relation(
        mcx,
        relid,
        relname,
        params.options & !VACOPT_ANALYZE,
        lmode,
    )? {
        Some(rel) => rel,
        None => {
            snapmgr::PopActiveSnapshot()?;
            xact::CommitTransactionCommand()?;
            return Ok(false);
        }
    };

    if !matches!(
        rel.rd_rel.relkind,
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE | RELKIND_PARTITIONED_TABLE
    ) {
        ereport(WARNING)
            .errmsg(format!(
                "skipping \"{}\" --- cannot vacuum non-tables or special system tables",
                rel.name()
            ))
            .finish(loc("vacuum_rel"))?;
        rel.close(lmode)?;
        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
        return Ok(false);
    }

    // Partitioned tables have no storage; the useful work is on the child
    // partitions queued separately. Returning true lets ANALYZE proceed.
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        rel.close(lmode)?;
        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
        return Ok(true);
    }

    // C divergence (recorded): LockRelationIdForSession is skipped — no toast
    // recursion happens (loud below), so no cross-transaction lock is needed.

    let mut params = *params;
    let std_opts = rel.rd_options.as_ref().and_then(|o| o.std()).copied();
    if params.index_cleanup == VacOptValue::Unspecified {
        params.index_cleanup = match std_opts.map(|o| o.vacuum_index_cleanup) {
            Some(types_rel::STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON) => VacOptValue::Enabled,
            Some(types_rel::STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF) => VacOptValue::Disabled,
            _ => VacOptValue::Auto,
        };
    }
    if let Some(o) = &std_opts {
        if o.vacuum_max_eager_freeze_failure_rate >= 0.0 {
            params.max_eager_freeze_failure_rate = o.vacuum_max_eager_freeze_failure_rate;
        }
    }
    if params.truncate == VacOptValue::Unspecified {
        params.truncate = match &std_opts {
            Some(o) if o.vacuum_truncate_set => {
                if o.vacuum_truncate {
                    VacOptValue::Enabled
                } else {
                    VacOptValue::Disabled
                }
            }
            _ => {
                if guc_tables::vars::vacuum_truncate.read() {
                    VacOptValue::Enabled
                } else {
                    VacOptValue::Disabled
                }
            }
        };
    }

    let toast_relid = if params.options & VACOPT_PROCESS_TOAST != 0
        && (params.options & VACOPT_FULL == 0 || params.options & VACOPT_PROCESS_MAIN == 0)
    {
        rel.rd_rel.reltoastrelid
    } else {
        InvalidOid
    };

    if params.options & VACOPT_PROCESS_MAIN != 0 {
        if params.options & VACOPT_FULL != 0 {
            // VACUUM FULL is a variant of CLUSTER (cluster.c); cluster_rel
            // closes the relation but keeps the lock.
            let cluster_options: u32 =
                if params.options & VACOPT_VERBOSE != 0 { 0x01 } else { 0 };
            cluster_seams::cluster_rel::call(mcx, rel, InvalidOid, cluster_options)?;
        } else {
            // C divergence (recorded): SetUserIdAndSecContext/NewGUCNestLevel/
            // RestrictSearchPath are skipped (single-user milestone).
            tableam_seams::table_relation_vacuum::call(mcx, &rel, &params, bstrategy.clone())?;
            rel.close(NoLock)?;
        }
    } else {
        rel.close(NoLock)?;
    }
    snapmgr::PopActiveSnapshot()?;
    xact::CommitTransactionCommand()?;

    if toast_relid != InvalidOid {
        let mut toast_params = params;
        toast_params.options |= VACOPT_PROCESS_MAIN;
        toast_params.toast_parent = relid;
        vacuum_rel(mcx, toast_relid, None, &toast_params, bstrategy)?;
    }

    Ok(true)
}

/// vacuum_open_relation (vacuum.c); commands_analyze enters with
/// options & !VACOPT_VACUUM. `relname` None = the caller wants the skip
/// silent (expanded partitions, toast recursion), C's NULL RangeVar.
pub fn vacuum_open_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relname: Option<&str>,
    options: u32,
    lmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    debug_assert!(options & (VACOPT_VACUUM | VACOPT_ANALYZE) != 0);
    let mut rel_lock = true;
    let rel = if options & VACOPT_SKIP_LOCKED == 0 {
        relation::try_relation_open(mcx, relid, lmode)?
    } else if lmgr_seams::conditional_lock_relation_oid::call(relid, lmode)? {
        relation::try_relation_open(mcx, relid, NoLock)?
    } else {
        rel_lock = false;
        None
    };
    if rel.is_some() {
        return Ok(rel);
    }
    let Some(relname) = relname else {
        return Ok(None);
    };
    // C: autovacuum workers stay silent here unless verbose (divergence:
    // keyed off VACOPT_VERBOSE, C keys off log_min_duration >= 0).
    if miscinit::GetMyBackendType() == types_core::BackendType::AutovacWorker
        && options & VACOPT_VERBOSE == 0
    {
        return Ok(None);
    }
    let verb = if options & VACOPT_VACUUM != 0 { "vacuum" } else { "analyze" };
    let (code, why) = if rel_lock {
        (ERRCODE_UNDEFINED_TABLE, "relation no longer exists")
    } else {
        (ERRCODE_LOCK_NOT_AVAILABLE, "lock not available")
    };
    ereport(WARNING)
        .errcode(code)
        .errmsg(format!("skipping {verb} of \"{relname}\" --- {why}"))
        .finish(loc("vacuum_open_relation"))?;
    Ok(None)
}

/// Returns (aggressive, cutoffs).
pub fn vacuum_get_cutoffs(
    rel: &RelationData<'_>,
    params: &VacuumParams,
) -> PgResult<(bool, VacuumCutoffs)> {
    let mut freeze_min_age = params.freeze_min_age;
    let mut multixact_freeze_min_age = params.multixact_freeze_min_age;
    let mut freeze_table_age = params.freeze_table_age;
    let mut multixact_freeze_table_age = params.multixact_freeze_table_age;

    let mut cutoffs = VacuumCutoffs {
        relfrozenxid: rel.rd_rel.relfrozenxid,
        relminmxid: rel.rd_rel.relminmxid,
        OldestXmin: procarray::GetOldestNonRemovableTransactionId(rel)?,
        OldestMxact: GetOldestMultiXactId()?,
        FreezeLimit: InvalidTransactionId,
        MultiXactCutoff: 0,
    };
    debug_assert!(TransactionIdIsNormal(cutoffs.OldestXmin));
    debug_assert!(MultiXactIdIsValid(cutoffs.OldestMxact));

    let next_xid = varsup::ReadNextTransactionId()?;
    let next_mxid = ReadNextMultiXactId()?;
    let effective_multixact_freeze_max_age = MultiXactMemberFreezeThreshold()?;
    let autovacuum_freeze_max_age = init_small::globals::autovacuum_freeze_max_age();

    let mut safe_oldest_xmin = next_xid.wrapping_sub(autovacuum_freeze_max_age as u32);
    if !TransactionIdIsNormal(safe_oldest_xmin) {
        safe_oldest_xmin = FirstNormalTransactionId;
    }
    let mut safe_oldest_mxact: MultiXactId =
        next_mxid.wrapping_sub(effective_multixact_freeze_max_age as u32);
    if safe_oldest_mxact < FirstMultiXactId {
        safe_oldest_mxact = FirstMultiXactId;
    }
    if TransactionIdPrecedes(cutoffs.OldestXmin, safe_oldest_xmin) {
        ereport(WARNING)
            .errmsg("cutoff for removing and freezing tuples is far in the past")
            .finish(loc("vacuum_get_cutoffs"))?;
    }
    if MultiXactIdPrecedes(cutoffs.OldestMxact, safe_oldest_mxact) {
        ereport(WARNING)
            .errmsg("cutoff for freezing multixacts is far in the past")
            .finish(loc("vacuum_get_cutoffs"))?;
    }

    if freeze_min_age < 0 {
        freeze_min_age = guc_tables::vars::vacuum_freeze_min_age.read();
    }
    freeze_min_age = freeze_min_age.min(autovacuum_freeze_max_age / 2);
    debug_assert!(freeze_min_age >= 0);

    cutoffs.FreezeLimit = next_xid.wrapping_sub(freeze_min_age as u32);
    if !TransactionIdIsNormal(cutoffs.FreezeLimit) {
        cutoffs.FreezeLimit = FirstNormalTransactionId;
    }
    if TransactionIdPrecedes(cutoffs.OldestXmin, cutoffs.FreezeLimit) {
        cutoffs.FreezeLimit = cutoffs.OldestXmin;
    }

    if multixact_freeze_min_age < 0 {
        multixact_freeze_min_age = guc_tables::vars::vacuum_multixact_freeze_min_age.read();
    }
    multixact_freeze_min_age =
        multixact_freeze_min_age.min(effective_multixact_freeze_max_age / 2);
    debug_assert!(multixact_freeze_min_age >= 0);

    cutoffs.MultiXactCutoff = next_mxid.wrapping_sub(multixact_freeze_min_age as u32);
    if cutoffs.MultiXactCutoff < FirstMultiXactId {
        cutoffs.MultiXactCutoff = FirstMultiXactId;
    }
    if MultiXactIdPrecedes(cutoffs.OldestMxact, cutoffs.MultiXactCutoff) {
        cutoffs.MultiXactCutoff = cutoffs.OldestMxact;
    }

    if freeze_table_age < 0 {
        freeze_table_age = guc_tables::vars::vacuum_freeze_table_age.read();
    }
    freeze_table_age = freeze_table_age.min((autovacuum_freeze_max_age as f64 * 0.95) as i32);
    debug_assert!(freeze_table_age >= 0);
    let mut aggressive_xid_cutoff = next_xid.wrapping_sub(freeze_table_age as u32);
    if !TransactionIdIsNormal(aggressive_xid_cutoff) {
        aggressive_xid_cutoff = FirstNormalTransactionId;
    }
    if TransactionIdPrecedesOrEquals(cutoffs.relfrozenxid, aggressive_xid_cutoff) {
        return Ok((true, cutoffs));
    }

    if multixact_freeze_table_age < 0 {
        multixact_freeze_table_age = guc_tables::vars::vacuum_multixact_freeze_table_age.read();
    }
    multixact_freeze_table_age = multixact_freeze_table_age
        .min((effective_multixact_freeze_max_age as f64 * 0.95) as i32);
    debug_assert!(multixact_freeze_table_age >= 0);
    let mut aggressive_mxid_cutoff: MultiXactId =
        next_mxid.wrapping_sub(multixact_freeze_table_age as u32);
    if aggressive_mxid_cutoff < FirstMultiXactId {
        aggressive_mxid_cutoff = FirstMultiXactId;
    }
    if MultiXactIdPrecedesOrEquals(cutoffs.relminmxid, aggressive_mxid_cutoff) {
        return Ok((true, cutoffs));
    }

    Ok((false, cutoffs))
}

pub fn vacuum_xid_failsafe_check(cutoffs: &VacuumCutoffs) -> PgResult<bool> {
    debug_assert!(TransactionIdIsNormal(cutoffs.relfrozenxid));
    debug_assert!(MultiXactIdIsValid(cutoffs.relminmxid));

    let autovacuum_freeze_max_age = init_small::globals::autovacuum_freeze_max_age();
    let skip_index_vacuum = guc_tables::vars::vacuum_failsafe_age
        .read()
        .max((autovacuum_freeze_max_age as f64 * 1.05) as i32);
    let mut xid_skip_limit =
        varsup::ReadNextTransactionId()?.wrapping_sub(skip_index_vacuum as u32);
    if !TransactionIdIsNormal(xid_skip_limit) {
        xid_skip_limit = FirstNormalTransactionId;
    }
    if TransactionIdPrecedes(cutoffs.relfrozenxid, xid_skip_limit) {
        return Ok(true);
    }

    let multixact_freeze_max_age = guc_tables::vars::autovacuum_multixact_freeze_max_age.read();
    let skip_multixact_vacuum = guc_tables::vars::vacuum_multixact_failsafe_age
        .read()
        .max((multixact_freeze_max_age as f64 * 1.05) as i32);
    let mut multi_skip_limit: MultiXactId =
        ReadNextMultiXactId()?.wrapping_sub(skip_multixact_vacuum as u32);
    if multi_skip_limit < FirstMultiXactId {
        multi_skip_limit = FirstMultiXactId;
    }
    if MultiXactIdPrecedes(cutoffs.relminmxid, multi_skip_limit) {
        return Ok(true);
    }
    Ok(false)
}

pub fn vac_estimate_reltuples(
    rel: &RelationData<'_>,
    total_pages: BlockNumber,
    scanned_pages: BlockNumber,
    scanned_tuples: f64,
) -> f64 {
    let old_rel_pages = rel.rd_rel.relpages;
    let old_rel_tuples = rel.rd_rel.reltuples as f64;

    if scanned_pages >= total_pages {
        return scanned_tuples;
    }
    if old_rel_pages == total_pages as i32 && (scanned_pages as f64) < total_pages as f64 * 0.02 {
        return old_rel_tuples;
    }
    if scanned_pages <= 1 {
        return old_rel_tuples;
    }
    if old_rel_tuples < 0.0 || old_rel_pages == 0 {
        return ((scanned_tuples / scanned_pages as f64) * total_pages as f64 + 0.5).floor();
    }

    let old_density = old_rel_tuples / old_rel_pages as f64;
    let unscanned_pages = total_pages as f64 - scanned_pages as f64;
    (old_density * unscanned_pages + scanned_tuples + 0.5).floor()
}

const RelationRelationId: Oid = 1259;
const ClassOidIndexId: Oid = 2662;
const Natts_pg_class: usize = 34;
const DatabaseRelationId: Oid = 1262;
const Anum_pg_class_relkind: usize = 18;
const Anum_pg_class_relfrozenxid: usize = 30;
const Anum_pg_class_relminmxid: usize = 31;
const Anum_pg_class_oid: usize = 1;
const Anum_pg_class_relpages: usize = 10;
const Anum_pg_class_reltuples: usize = 11;
const Anum_pg_class_relallvisible: usize = 12;
const Anum_pg_class_relallfrozen: usize = 13;
const Anum_pg_class_relhasindex: usize = 15;
const Anum_pg_class_relhasrules: usize = 21;
const Anum_pg_class_relhastriggers: usize = 22;
const RowExclusiveLock: LOCKMODE = 3;

fn getattr(
    tup: &::types_tuple::HeapTupleData<'_>,
    attnum: usize,
    desc: &::types_tuple::TupleDescData<'_>,
) -> ::datum::Datum {
    let mut isnull = false;
    // SAFETY: pg_class row copied under pg_class's descriptor; fixed columns
    // are never null.
    let d = unsafe { ::types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

/// vac_update_relstats (vacuum.c). `frozenxid`/`minmulti` advance
/// relfrozenxid/relminmxid (Invalid = leave alone, C's ANALYZE shape).
#[allow(clippy::too_many_arguments)]
pub fn vac_update_relstats(
    relation: &RelationData<'_>,
    num_pages: BlockNumber,
    num_tuples: f64,
    num_all_visible_pages: BlockNumber,
    num_all_frozen_pages: BlockNumber,
    hasindex: bool,
    frozenxid: ::types_core::TransactionId,
    minmulti: MultiXactId,
    in_outer_xact: bool,
) -> PgResult<()> {
    let relid = relation.rd_id;
    let cx = ::mcx::MemoryContext::new("vac_update_relstats");
    let mcx = cx.mcx();
    let rd = table::table_open(mcx, RelationRelationId, RowExclusiveLock)?;

    let mut key = ::types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_class_oid as i16;
    key.sk_strategy = ::types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(::types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = ::datum::Datum::from_oid(relid);

    let Some((ctup, inplace_state)) =
        genam::systable_inplace_update_begin(mcx, &rd, ClassOidIndexId, true, &[key])?
    else {
        return Err(::types_error::PgError::error(format!(
            "pg_class entry for relid {relid} vanished during vacuuming"
        ))
        .into());
    };

    let desc = rd.descr();
    let old = ctup.as_tuple();
    let mut values = [::datum::Datum::null(); Natts_pg_class];
    let nulls = [false; Natts_pg_class];
    let mut replaces = [false; Natts_pg_class];
    let mut dirty = false;
    let set = |anum: usize, d: ::datum::Datum, values: &mut [::datum::Datum],
                   replaces: &mut [bool], dirty: &mut bool| {
        values[anum - 1] = d;
        replaces[anum - 1] = true;
        *dirty = true;
    };

    if getattr(old, Anum_pg_class_relpages, desc).as_i32() != num_pages as i32 {
        set(Anum_pg_class_relpages, ::datum::Datum::from_i32(num_pages as i32), &mut values, &mut replaces, &mut dirty);
    }
    if getattr(old, Anum_pg_class_reltuples, desc).as_f32() != num_tuples as f32 {
        set(Anum_pg_class_reltuples, ::datum::Datum::from_f32(num_tuples as f32), &mut values, &mut replaces, &mut dirty);
    }
    if getattr(old, Anum_pg_class_relallvisible, desc).as_i32() != num_all_visible_pages as i32 {
        set(Anum_pg_class_relallvisible, ::datum::Datum::from_i32(num_all_visible_pages as i32), &mut values, &mut replaces, &mut dirty);
    }
    if getattr(old, Anum_pg_class_relallfrozen, desc).as_i32() != num_all_frozen_pages as i32 {
        set(Anum_pg_class_relallfrozen, ::datum::Datum::from_i32(num_all_frozen_pages as i32), &mut values, &mut replaces, &mut dirty);
    }

    if !in_outer_xact {
        if getattr(old, Anum_pg_class_relhasindex, desc).as_bool() && !hasindex {
            set(Anum_pg_class_relhasindex, ::datum::Datum::from_bool(false), &mut values, &mut replaces, &mut dirty);
        }
        // C clears relhasrules/relhastriggers off rd_rules/trigdesc; neither
        // exists here (rules/trigger lanes unported), so a set flag is loud.
        if getattr(old, Anum_pg_class_relhasrules, desc).as_bool()
            || getattr(old, Anum_pg_class_relhastriggers, desc).as_bool()
        {
            unported("vac_update_relstats relhasrules/relhastriggers clear (rules/trigger lanes)");
        }
    }

    // relfrozenxid advances only forward, except a stored value in the future
    // (corruption) is overwritten with a WARNING; same for relminmxid.
    let oldfrozenxid = getattr(old, Anum_pg_class_relfrozenxid, desc).as_u32();
    let mut futurexid = false;
    if TransactionIdIsNormal(frozenxid) && oldfrozenxid != frozenxid {
        let mut update = false;
        if TransactionIdPrecedes(oldfrozenxid, frozenxid) {
            update = true;
        } else if TransactionIdPrecedes(varsup::ReadNextTransactionId()?, oldfrozenxid) {
            futurexid = true;
            update = true;
        }
        if update {
            set(Anum_pg_class_relfrozenxid, ::datum::Datum::from_u32(frozenxid), &mut values, &mut replaces, &mut dirty);
        }
    }

    let oldminmulti = getattr(old, Anum_pg_class_relminmxid, desc).as_u32();
    let mut futuremxid = false;
    if MultiXactIdIsValid(minmulti) && oldminmulti != minmulti {
        let mut update = false;
        if MultiXactIdPrecedes(oldminmulti, minmulti) {
            update = true;
        } else if MultiXactIdPrecedes(ReadNextMultiXactId()?, oldminmulti) {
            futuremxid = true;
            update = true;
        }
        if update {
            set(Anum_pg_class_relminmxid, ::datum::Datum::from_u32(minmulti), &mut values, &mut replaces, &mut dirty);
        }
    }

    if dirty {
        let newtup =
            heaptuple::heap_modify_tuple(mcx, old, desc, &values, &nulls, &replaces)?;
        genam::systable_inplace_update_finish(mcx, inplace_state, newtup.as_tuple())?;
    } else {
        genam::systable_inplace_update_cancel(mcx, inplace_state)?;
    }
    table::table_close(rd, RowExclusiveLock)?;

    if futurexid {
        ereport(WARNING)
            .errcode(::types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "overwrote invalid relfrozenxid value {oldfrozenxid} with new value {frozenxid} for table \"{}\"",
                relation.name()
            ))
            .finish(loc("vac_update_relstats"))?;
    }
    if futuremxid {
        ereport(WARNING)
            .errcode(::types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "overwrote invalid relminmxid value {oldminmulti} with new value {minmulti} for table \"{}\"",
                relation.name()
            ))
            .finish(loc("vac_update_relstats"))?;
    }
    Ok(())
}

pub fn vac_update_datfrozenxid(mcx: Mcx<'_>) -> PgResult<()> {
    use init_small::globals::MyDatabaseId;

    // One backend per database at a time; released at transaction end (C shape).
    lmgr::LockDatabaseFrozenIds(::types_rel::lock::ExclusiveLock)?;

    let mut new_frozen_xid = procarray::GetOldestNonRemovableTransactionIdShared()?;
    let mut new_min_multi = GetOldestMultiXactId()?;
    let last_sane_frozen_xid = varsup::ReadNextTransactionId()?;
    let last_sane_min_multi = ReadNextMultiXactId()?;

    let rd = table::table_open(mcx, RelationRelationId, AccessShareLock)?;
    let desc = rd.descr();
    let mut scan = genam::systable_beginscan(mcx, &rd, InvalidOid, false, None, &[])?;
    let mut bogus = false;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let relkind = getattr(tup, Anum_pg_class_relkind, desc).as_u8();
        let relfrozenxid = getattr(tup, Anum_pg_class_relfrozenxid, desc).as_u32();
        let relminmxid: MultiXactId = getattr(tup, Anum_pg_class_relminmxid, desc).as_u32();
        if !matches!(relkind, RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE) {
            debug_assert!(!::types_core::xact::TransactionIdIsValid(relfrozenxid));
            debug_assert!(!MultiXactIdIsValid(relminmxid));
            continue;
        }
        if ::types_core::xact::TransactionIdIsValid(relfrozenxid) {
            debug_assert!(TransactionIdIsNormal(relfrozenxid));
            if TransactionIdPrecedes(last_sane_frozen_xid, relfrozenxid) {
                bogus = true;
                break;
            }
            if TransactionIdPrecedes(relfrozenxid, new_frozen_xid) {
                new_frozen_xid = relfrozenxid;
            }
        }
        if MultiXactIdIsValid(relminmxid) {
            if MultiXactIdPrecedes(last_sane_min_multi, relminmxid) {
                bogus = true;
                break;
            }
            if MultiXactIdPrecedes(relminmxid, new_min_multi) {
                new_min_multi = relminmxid;
            }
        }
    }
    genam::systable_endscan(mcx, scan)?;
    table::table_close(rd, AccessShareLock)?;

    if bogus {
        return Ok(());
    }
    debug_assert!(TransactionIdIsNormal(new_frozen_xid));
    debug_assert!(MultiXactIdIsValid(new_min_multi));

    let rd = table::table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;
    let mut key = ::types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = pg_database::Anum_pg_database_oid as i16;
    key.sk_strategy = ::types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(::types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = ::datum::Datum::from_oid(MyDatabaseId());

    let Some((ctup, inplace_state)) = genam::systable_inplace_update_begin(
        mcx,
        &rd,
        pg_database::DatabaseOidIndexId,
        true,
        &[key],
    )?
    else {
        return Err(::types_error::PgError::error(format!(
            "could not find tuple for database {}",
            MyDatabaseId()
        ))
        .into());
    };

    let desc = rd.descr();
    let old = ctup.as_tuple();
    let datfrozenxid = getattr(old, pg_database::Anum_pg_database_datfrozenxid as usize, desc).as_u32();
    let datminmxid: MultiXactId =
        getattr(old, pg_database::Anum_pg_database_datminmxid as usize, desc).as_u32();

    let mut values = [::datum::Datum::null(); pg_database::Natts_pg_database];
    let nulls = [false; pg_database::Natts_pg_database];
    let mut replaces = [false; pg_database::Natts_pg_database];
    let mut dirty = false;

    // Never let the value go backward unless the stored one is "in the future"
    // (corrupt) — C's exact rule.
    if datfrozenxid != new_frozen_xid
        && (TransactionIdPrecedes(datfrozenxid, new_frozen_xid)
            || TransactionIdPrecedes(last_sane_frozen_xid, datfrozenxid))
    {
        values[pg_database::Anum_pg_database_datfrozenxid as usize - 1] =
            ::datum::Datum::from_u32(new_frozen_xid);
        replaces[pg_database::Anum_pg_database_datfrozenxid as usize - 1] = true;
        dirty = true;
    } else {
        new_frozen_xid = datfrozenxid;
    }
    if datminmxid != new_min_multi
        && (MultiXactIdPrecedes(datminmxid, new_min_multi)
            || MultiXactIdPrecedes(last_sane_min_multi, datminmxid))
    {
        values[pg_database::Anum_pg_database_datminmxid as usize - 1] =
            ::datum::Datum::from_u32(new_min_multi);
        replaces[pg_database::Anum_pg_database_datminmxid as usize - 1] = true;
        dirty = true;
    } else {
        new_min_multi = datminmxid;
    }

    if dirty {
        let newtup = heaptuple::heap_modify_tuple(mcx, old, desc, &values, &nulls, &replaces)?;
        genam::systable_inplace_update_finish(mcx, inplace_state, newtup.as_tuple())?;
    } else {
        genam::systable_inplace_update_cancel(mcx, inplace_state)?;
    }
    table::table_close(rd, RowExclusiveLock)?;

    if dirty || varsup::ForceTransactionIdLimitUpdate()? {
        vac_truncate_clog(mcx, new_frozen_xid, new_min_multi, last_sane_frozen_xid, last_sane_min_multi)?;
    }
    Ok(())
}

// C: WrapLimitsVacuumLock LWLock (one truncation task per cluster).
static WRAP_LIMITS_VACUUM_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn vac_truncate_clog(
    mcx: Mcx<'_>,
    mut frozen_xid: ::types_core::TransactionId,
    mut min_multi: MultiXactId,
    last_sane_frozen_xid: ::types_core::TransactionId,
    last_sane_min_multi: MultiXactId,
) -> PgResult<()> {
    use init_small::globals::MyDatabaseId;

    let next_xid = varsup::ReadNextTransactionId()?;
    let _guard = WRAP_LIMITS_VACUUM_LOCK.lock().unwrap();

    let mut oldestxid_datoid = MyDatabaseId();
    let mut minmulti_datoid = MyDatabaseId();
    let mut bogus = false;
    let mut frozen_already_wrapped = false;

    let rd = table::table_open(mcx, DatabaseRelationId, AccessShareLock)?;
    let desc = rd.descr();
    let mut scan = genam::systable_beginscan(mcx, &rd, InvalidOid, false, None, &[])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let oid = getattr(tup, pg_database::Anum_pg_database_oid as usize, desc).as_oid();
        let datconnlimit =
            getattr(tup, pg_database::Anum_pg_database_datconnlimit as usize, desc).as_i32();
        let datfrozenxid =
            getattr(tup, pg_database::Anum_pg_database_datfrozenxid as usize, desc).as_u32();
        let datminmxid: MultiXactId =
            getattr(tup, pg_database::Anum_pg_database_datminmxid as usize, desc).as_u32();

        debug_assert!(TransactionIdIsNormal(datfrozenxid));
        debug_assert!(MultiXactIdIsValid(datminmxid));

        // Databases being dropped can't be connected to or autovacuumed.
        if datconnlimit == pg_database::DATCONNLIMIT_INVALID_DB {
            continue;
        }

        if TransactionIdPrecedes(last_sane_frozen_xid, datfrozenxid)
            || MultiXactIdPrecedes(last_sane_min_multi, datminmxid)
        {
            bogus = true;
        }

        if TransactionIdPrecedes(next_xid, datfrozenxid) {
            frozen_already_wrapped = true;
        } else if TransactionIdPrecedes(datfrozenxid, frozen_xid) {
            frozen_xid = datfrozenxid;
            oldestxid_datoid = oid;
        }

        if MultiXactIdPrecedes(datminmxid, min_multi) {
            min_multi = datminmxid;
            minmulti_datoid = oid;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    table::table_close(rd, AccessShareLock)?;

    if frozen_already_wrapped {
        ereport(WARNING)
            .errmsg("some databases have not been vacuumed in over 2 billion transactions")
            .errdetail("You might have already suffered transaction-wraparound data loss.")
            .finish(loc("vac_truncate_clog"))?;
        return Ok(());
    }
    if bogus {
        return Ok(());
    }

    async_seams::async_notify_freeze_xids::call(frozen_xid)?;

    // Slot uninstalled == commit_ts unported == off (startup.rs shape).
    if guc_tables::vars::track_commit_timestamp.installed()
        && guc_tables::vars::track_commit_timestamp.read()
    {
        unported("vac_truncate_clog: commit_ts truncation (AdvanceOldestCommitTsXid/TruncateCommitTs)");
    }

    clog::TruncateCLOG(frozen_xid, oldestxid_datoid)?;
    multixact::TruncateMultiXact(min_multi, minmulti_datoid)?;

    varsup::SetTransactionIdLimit(frozen_xid, oldestxid_datoid)?;
    multixact::SetMultiXactIdLimit(min_multi, minmulti_datoid, false)?;
    Ok(())
}

macro_rules! vacuum_guc_int {
    ($($cell:ident, $var:ident, $boot:expr;)+) => {
        $( static $cell: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new($boot); )+
        fn install_guc_ints() {
            use std::sync::atomic::Ordering::Relaxed;
            $(
                guc_tables::vars::$var.install(guc_tables::GucVarAccessors {
                    get: || $cell.load(Relaxed),
                    set: |v| $cell.store(v, Relaxed),
                });
            )+
        }
    };
}

vacuum_guc_int! {
    VACUUM_FREEZE_MIN_AGE, vacuum_freeze_min_age, 50000000;
    VACUUM_FREEZE_TABLE_AGE, vacuum_freeze_table_age, 150000000;
    VACUUM_MXID_FREEZE_MIN_AGE, vacuum_multixact_freeze_min_age, 5000000;
    VACUUM_MXID_FREEZE_TABLE_AGE, vacuum_multixact_freeze_table_age, 150000000;
    VACUUM_FAILSAFE_AGE, vacuum_failsafe_age, 1600000000;
    VACUUM_MXID_FAILSAFE_AGE, vacuum_multixact_failsafe_age, 1600000000;
}

static VACUUM_TRUNCATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
static VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0.03f64.to_bits());

pub fn init_seams() {
    use std::sync::atomic::Ordering::Relaxed;
    install_guc_ints();
    guc_tables::vars::vacuum_truncate.install(guc_tables::GucVarAccessors {
        get: || VACUUM_TRUNCATE.load(Relaxed),
        set: |v| VACUUM_TRUNCATE.store(v, Relaxed),
    });
    guc_tables::vars::vacuum_max_eager_freeze_failure_rate.install(guc_tables::GucVarAccessors {
        get: || f64::from_bits(VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE.load(Relaxed)),
        set: |v| VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE.store(v.to_bits(), Relaxed),
    });
    // Fixture tests pre-install a relstats sink (no pg_class there); keep it.
    if !vacuum_seams::vac_update_relstats::is_installed() {
        vacuum_seams::vac_update_relstats::set(vac_update_relstats);
    }
}

/// vac_open_indexes: just the ready indexes, each locked with `lockmode`.
pub fn vac_open_indexes<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RelationData<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<::mcx::PgVec<'mcx, Relation<'mcx>>> {
    debug_assert!(lockmode != NoLock);
    let indexoidlist = relcache_seams::relation_get_index_list::call(mcx, relation.rd_id)?;
    let mut irel = ::mcx::PgVec::with_capacity_in(indexoidlist.len(), mcx);
    for &indexoid in indexoidlist.iter() {
        let indrel = indexam::index_open(mcx, indexoid, lockmode)?;
        if indrel.rd_index.as_ref().is_some_and(|i| i.indisready) {
            irel.push(indrel);
        } else {
            indexam::index_close(indrel, lockmode)?;
        }
    }
    Ok(irel)
}

pub fn vac_close_indexes(irel: ::mcx::PgVec<'_, Relation<'_>>, lockmode: LOCKMODE) -> PgResult<()> {
    for ind in irel {
        indexam::index_close(ind, lockmode)?;
    }
    Ok(())
}

/// vac_bulkdel_one_index (ereport chatter elided; logging lane).
pub fn vac_bulkdel_one_index<'mcx>(
    mcx: Mcx<'mcx>,
    ivinfo: &nbtree::IndexVacuumInfo<'_, 'mcx>,
    istat: Option<::types_nbtree::IndexBulkDeleteResult>,
    dead_items: &[::types_tuple::itemptr::ItemPointerData],
) -> PgResult<::types_nbtree::IndexBulkDeleteResult> {
    indexam::index_bulk_delete(mcx, ivinfo, istat, dead_items)
}

/// vac_cleanup_one_index (ereport chatter elided; logging lane).
pub fn vac_cleanup_one_index<'mcx>(
    mcx: Mcx<'mcx>,
    ivinfo: &nbtree::IndexVacuumInfo<'_, 'mcx>,
    istat: Option<::types_nbtree::IndexBulkDeleteResult>,
) -> PgResult<Option<::types_nbtree::IndexBulkDeleteResult>> {
    indexam::index_vacuum_cleanup(mcx, ivinfo, istat)
}

pub fn vacuum_delay_point(_is_analyze: bool) -> PgResult<()> {
    use init_small::globals as g;

    postgres_seams::check_for_interrupts::call()?;

    if g::InterruptPending() || (!g::VacuumCostActive() && !interrupt::ConfigReloadPending()) {
        return Ok(());
    }

    if interrupt::ConfigReloadPending()
        && miscinit::GetMyBackendType() == types_core::BackendType::AutovacWorker
    {
        interrupt::SetConfigReloadPending(false);
        guc_file_seams::process_config_file::call(::types_guc::GucContext::PGC_SIGHUP)?;
        autovacuum_seams::vacuum_update_costs::call()?;
    }

    if !g::VacuumCostActive() {
        return Ok(());
    }

    let mut msec = 0.0f64;
    if let Some(shared) = vacuum_shared_cost() {
        msec = compute_parallel_delay(&shared);
    } else if g::VacuumCostBalance() >= vacuum_cost_limit() {
        msec = vacuum_cost_delay() * g::VacuumCostBalance() as f64 / vacuum_cost_limit() as f64;
    }

    if msec > 0.0 {
        if msec > vacuum_cost_delay() * 4.0 {
            msec = vacuum_cost_delay() * 4.0;
        }
        // track_cost_delay_timing progress increments: progress-reporting lane.
        std::thread::sleep(std::time::Duration::from_micros((msec * 1000.0) as u64));
        g::SetVacuumCostBalance(0);
        autovacuum_seams::auto_vacuum_update_cost_limit::call()?;
        postgres_seams::check_for_interrupts::call()?;
    }
    Ok(())
}

// compute_parallel_delay (vacuum.c): balance accumulates into the shared
// counter; a worker sleeps only once its own contribution passes half its
// fair share of the limit.
fn compute_parallel_delay(shared: &VacuumSharedCost) -> f64 {
    use init_small::globals as g;
    use std::sync::atomic::Ordering::SeqCst;

    let mut msec = 0.0f64;
    let nworkers = shared.active_nworkers.load(SeqCst) as i32;
    debug_assert!(nworkers >= 1);

    let shared_balance = shared
        .cost_balance
        .fetch_add(g::VacuumCostBalance() as u32, SeqCst)
        .wrapping_add(g::VacuumCostBalance() as u32);

    let local = VACUUM_COST_BALANCE_LOCAL.get() + g::VacuumCostBalance();
    VACUUM_COST_BALANCE_LOCAL.set(local);

    if shared_balance >= vacuum_cost_limit() as u32
        && local as f64 > 0.5 * (vacuum_cost_limit() as f64 / nworkers as f64)
    {
        msec = vacuum_cost_delay() * local as f64 / vacuum_cost_limit() as f64;
        shared.cost_balance.fetch_sub(local as u32, SeqCst);
        VACUUM_COST_BALANCE_LOCAL.set(0);
    }

    g::SetVacuumCostBalance(0);
    msec
}

#[cold]
#[inline(never)]
fn loc(routine: &'static str) -> ::types_error::ErrorLocation {
    ::types_error::ErrorLocation::new("vacuum.c", 0, routine)
}

#[cold]
#[inline(never)]
fn unported(unit: &str) -> ! {
    panic!("unported callee reached from vacuum.c: {unit}");
}

#[cold]
#[inline(never)]
fn unported_option(name: &str) -> ! {
    panic!("unported callee reached from vacuum.c: ExecVacuum option \"{name}\"");
}
