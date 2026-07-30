//! vacuum.c: ExecVacuum -> vacuum -> vacuum_rel for named tables with
//! partition/inheritance expansion plus the TOAST recursion. parallel and
//! database-wide/database-stats arms are loud named panics;
//! vac_update_datfrozenxid is a recorded gap.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use ::backend_progress::progress::{PROGRESS_ANALYZE_DELAY_TIME, PROGRESS_VACUUM_DELAY_TIME};
use ::backend_progress::{pgstat_progress_incr_param, pgstat_progress_parallel_incr_param};
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
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_TABLE, ERROR, WARNING,
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
    static PARALLEL_VACUUM_WORKER_DELAY_NS: Cell<i64> = const { Cell::new(0) };
    // C's zero-initialized static last_report_time: None forces an immediate
    // first report.
    // DST P2 (contract §1.3): cost-delay pacing stamps in pg_clock's mono domain.
    static LAST_DELAY_REPORT: Cell<Option<pg_clock::MonoStamp>> = const { Cell::new(None) };
}

const PARALLEL_VACUUM_DELAY_REPORT_INTERVAL_NS: i64 = 1_000_000_000;

// vacuumparallel.c's worker-exit flush reads C's vacuum.c global.
pub fn parallel_vacuum_worker_delay_ns() -> i64 {
    PARALLEL_VACUUM_WORKER_DELAY_NS.get()
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

// MIN_BAS_VAC_RING_SIZE_KB (miscadmin.h:278); MAX in guc_tables consts.
const MIN_BAS_VAC_RING_SIZE_KB: i32 = 128;
const MAX_BAS_VAC_RING_SIZE_KB: i32 = 16 * 1024 * 1024;

fn errpos(src: &str, location: ::types_core::ParseLoc) -> i32 {
    parser_small1::parser_errposition_source(
        Some(src.as_bytes()),
        location,
        mbutils::GetDatabaseEncoding(),
    )
}

/// ExecVacuum's BUFFER_USAGE_LIMIT arm (vacuum.c), shared with the ANALYZE
/// entry in commands_analyze.
pub fn exec_vacuum_buffer_usage_limit<'mcx>(
    mcx: Mcx<'mcx>,
    opt: &types_nodes::parsenodes::DefElem<'_>,
) -> PgResult<i32> {
    let vac_buffer_size = explain::defGetString(mcx, opt)?;
    let (result, hint) = match guc::units::parse_int(vac_buffer_size, ::types_guc::GUC_UNIT_KB) {
        guc::units::ParseNum::Ok(v) => (Some(v), None),
        guc::units::ParseNum::Err { hint } => (None, hint),
    };
    match result {
        Some(v) if v == 0 || (MIN_BAS_VAC_RING_SIZE_KB..=MAX_BAS_VAC_RING_SIZE_KB).contains(&v) => {
            Ok(v)
        }
        _ => {
            let mut e = ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "BUFFER_USAGE_LIMIT option must be 0 or between \
                     {MIN_BAS_VAC_RING_SIZE_KB} kB and {MAX_BAS_VAC_RING_SIZE_KB} kB"
                ));
            if let Some(h) = hint {
                e = e.errhint(h);
            }
            Err(e.into_error().into())
        }
    }
}

pub fn ExecVacuum<'mcx>(
    mcx: Mcx<'mcx>,
    vacstmt: &VacuumStmt<'mcx>,
    source_text: &str,
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
    let mut ring_size: i32 = -1;
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
                        .with_cursor_position(errpos(source_text, opt.location))
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
                        .with_cursor_position(errpos(source_text, opt.location))
                        .into());
                }
                params.nworkers = if nworkers == 0 { -1 } else { nworkers };
            }
            "buffer_usage_limit" => ring_size = exec_vacuum_buffer_usage_limit(mcx, opt)?,
            name => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized VACUUM option \"{name}\""))
                    .into_error()
                    .with_cursor_position(errpos(source_text, opt.location))
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

    if full && params.nworkers > 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("VACUUM FULL cannot be performed in parallel")
            .into_error()
            .into());
    }

    // vacuum.c:342: VACUUM (FULL, ANALYZE) may use the ring; plain FULL errors.
    if ring_size != -1
        && params.options & VACOPT_FULL != 0
        && params.options & VACOPT_ANALYZE == 0
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL")
            .into_error()
            .into());
    }

    if params.options & VACOPT_ANALYZE == 0 {
        for vrel_node in vacstmt.rels.iter() {
            let vrel = vrel_node
                .as_vacuum_relation()
                .expect("vacuum relation list holds VacuumRelation");
            if !vrel.va_cols.is_nil() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("ANALYZE option must be specified when a column list is provided")
                    .into_error()
                    .into());
            }
        }
    }

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

    if params.options & VACOPT_ONLY_DATABASE_STATS != 0 {
        if !vacstmt.rels.is_nil() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("ONLY_DATABASE_STATS cannot be specified with a list of tables")
                .into_error()
                .into());
        }
        if params.options
            & !(VACOPT_VACUUM
                | VACOPT_VERBOSE
                | VACOPT_PROCESS_MAIN
                | VACOPT_PROCESS_TOAST
                | VACOPT_ONLY_DATABASE_STATS)
            != 0
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("ONLY_DATABASE_STATS cannot be specified with other VACUUM options")
                .into_error()
                .into());
        }
    }

    if freeze {
        params.freeze_min_age = 0;
        params.freeze_table_age = 0;
        params.multixact_freeze_min_age = 0;
        params.multixact_freeze_table_age = 0;
    }

    // vacuum.c:440: no strategy for FULL / ONLY_DATABASE_STATS unless ANALYZE.
    let bstrategy = if params.options & (VACOPT_ONLY_DATABASE_STATS | VACOPT_FULL) == 0
        || params.options & VACOPT_ANALYZE != 0
    {
        let ring_size = if ring_size == -1 {
            init_small::globals::VacuumBufferUsageLimit()
        } else {
            ring_size
        };
        bufmgr_seams::get_access_strategy_with_size::call(
            BufferAccessStrategyType::BasVacuum,
            ring_size,
        )
    } else {
        None
    };

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

    let mut vacrels: ::mcx::PgVec<'mcx, ExpandedVacRel<'mcx>> = ::mcx::PgVec::new_in(mcx);
    if params.options & VACOPT_ONLY_DATABASE_STATS != 0 {
        debug_assert!(relations.is_nil());
    } else if !relations.is_nil() {
        for vrel_node in relations.iter() {
            let vrel = vrel_node
                .as_vacuum_relation()
                .expect("vacuum relation list holds VacuumRelation");
            expand_vacuum_rel(mcx, vrel, params.options, &mut vacrels)?;
        }
    } else {
        get_all_vacuum_rels(mcx, params.options, &mut vacrels)?;
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

/// vacuum_is_permitted_for_relation (vacuum.c): db owner (non-shared rel) or
/// MAINTAIN privilege; WARNING + false otherwise. `relid` may be the TOAST
/// parent while `relname` names the relation being processed (vacuum_rel).
pub fn vacuum_is_permitted_for_relation(
    relid: Oid,
    relname: &str,
    relisshared: bool,
    options: u32,
) -> PgResult<bool> {
    debug_assert!(options & (VACOPT_VACUUM | VACOPT_ANALYZE) != 0);
    let roleid = miscinit_seams::get_user_id::call();
    if (aclchk::object_ownercheck(
        ::types_core::catalog::DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        roleid,
    )? && !relisshared)
        || aclchk::pg_class_aclcheck(
            relid,
            roleid,
            types_nodes::parsenodes::ACL_MAINTAIN as u64,
        )? == aclchk::ACLCHECK_OK
    {
        return Ok(true);
    }
    let verb = if options & VACOPT_VACUUM != 0 { "vacuum" } else { "analyze" };
    ereport(WARNING)
        .errmsg(format!("permission denied to {verb} \"{relname}\", skipping it"))
        .finish(loc("vacuum_is_permitted_for_relation"))?;
    Ok(false)
}

/// expand_vacuum_rel (vacuum.c): resolve the named table and, unless ONLY,
/// append its partitions/inheritance children. The transient AccessShareLock
/// is released before return, C-exact.
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
    let rvr_opts =
        if options & VACOPT_SKIP_LOCKED != 0 { namespace_seams::RVR_SKIP_LOCKED } else { 0 };
    let relid = namespace_seams::range_var_get_relid_extended::call(
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
        rvr_opts,
    )?;
    // C: lock unavailable — emit the same log statement vacuum_rel()/
    // analyze_rel() would.
    if relid == InvalidOid {
        let verb = if options & VACOPT_VACUUM != 0 { "vacuum" } else { "analyze" };
        ereport(WARNING)
            .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
            .errmsg(format!("skipping {verb} of \"{relname}\" --- lock not available"))
            .finish(loc("expand_vacuum_rel"))?;
        return Ok(());
    }
    let class_shape = syscache_seams::lookup_pg_class_by_relid::call(relid)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));

    if vacuum_is_permitted_for_relation(relid, relname, class_shape.relisshared, options)? {
        vacrels.push(ExpandedVacRel { oid: relid, relname: Some(relname), va_cols: &vrel.va_cols });
    }

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

/// get_all_vacuum_rels (vacuum.c): every vacuumable rel in the database the
/// user has privileges for.
pub fn get_all_vacuum_rels<'mcx>(
    mcx: Mcx<'mcx>,
    options: u32,
    vacrels: &mut ::mcx::PgVec<'mcx, ExpandedVacRel<'mcx>>,
) -> PgResult<()> {
    let nil_cols = ::mcx::alloc_leak_in(mcx, NodeList::nil())?;
    let pgclass = table::table_open(mcx, RelationRelationId, AccessShareLock)?;
    let desc = pgclass.descr();
    let mut scan = genam::systable_beginscan(mcx, &pgclass, InvalidOid, false, None, &[])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let relkind = getattr(tup, Anum_pg_class_relkind, desc).as_u8();
        if !matches!(relkind, RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_PARTITIONED_TABLE) {
            continue;
        }
        let relid = getattr(tup, Anum_pg_class_oid, desc).as_oid();
        let relname = name_from_datum(getattr(tup, Anum_pg_class_relname, desc));
        let relisshared = getattr(tup, Anum_pg_class_relisshared, desc).as_bool();
        if !vacuum_is_permitted_for_relation(
            relid,
            core::str::from_utf8(relname.name_str()).unwrap_or(""),
            relisshared,
            options,
        )? {
            continue;
        }
        vacrels.push(ExpandedVacRel { oid: relid, relname: None, va_cols: nil_cols });
    }
    genam::systable_endscan(mcx, scan)?;
    table::table_close(pgclass, AccessShareLock)?;
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

    // vacuum.c:2041-2072. Lazy vacuum only: a FULL vacuum may run user-defined
    // functions for functional indexes, and those may read other tables through
    // the snapshot taken below, so C deliberately keeps its xmin visible to
    // other backends' horizons rather than breaking transaction semantics.
    // Must precede GetTransactionSnapshot (see the setter's own note).
    if params.options & VACOPT_FULL == 0 {
        procarray::ProcSetStatusFlagInVacuum(params.is_wraparound)?;
    }

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

    // vacuum.c:2119: privileges are re-checked per relation because VACUUM
    // spans transactions; priv_relid is the TOAST parent when recursing.
    let priv_relid = if params.toast_parent != InvalidOid {
        params.toast_parent
    } else {
        rel.rd_id
    };
    if !vacuum_is_permitted_for_relation(
        priv_relid,
        rel.name(),
        rel.rd_rel.relisshared,
        params.options & !VACOPT_ANALYZE,
    )? {
        rel.close(lmode)?;
        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
        return Ok(false);
    }

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

    // Other backends' temp tables are silently skipped — their contents are
    // not reliably on disk, and warning would be database-wide-VACUUM chatter.
    if rel.is_other_temp() {
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
const Anum_pg_class_relname: usize = 2;
const Anum_pg_class_relisshared: usize = 16;
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

fn name_from_datum(d: ::datum::Datum) -> ::types_tuple::NameData {
    // SAFETY: name column datum points at an in-tuple NameData image.
    unsafe { core::ptr::read_unaligned(d.as_usize() as *const ::types_tuple::NameData) }
}

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
) -> PgResult<(bool, bool)> {
    let relid = relation.rd_id;
    let cx = ::mcx::MemoryContext::new("vac_update_relstats");
    let mcx = cx.mcx();

    // The DDL-flag inputs are gathered BEFORE the inplace window opens, and
    // that placement is load-bearing rather than stylistic.
    //
    // C (vacuum.c:1515, :1520) tests `relation->rd_rules == NULL` and
    // `relation->trigdesc == NULL` -- two pointers RelationBuildDesc filled in
    // when the relation was opened, so C's window is pure arithmetic. Ours
    // builds both lazily, so the equivalent read is a catalog scan:
    // table_open(pg_rewrite/pg_trigger) plus a systable index scan, which
    // takes heavyweight relation locks, buffer content locks on those
    // catalogs, and AcceptInvalidationMessages.
    //
    // Inside the window that is a hang, not a slowdown: the window holds
    // pg_class's buffer content lock EXCLUSIVE, and building a cold catalog's
    // relcache entry index-scans pg_class -- so if the scanned row shares a
    // page with this relation's own pg_class row, the scan waits on an LWLock
    // this very thread already holds. No deadlock detector sees an LWLock and
    // no CHECK_FOR_INTERRUPTS breaks that wait. heap_inplace_lock's own
    // comment (heapam.c) rules the shape out for exactly this reason:
    // registering invals "might reach a CatalogCacheInitializeCache() that
    // locks \"buffer\" ... would hang indefinitely if running after our own
    // LockBuffer()".
    //
    // Hoisting is value-identical to C, not merely safe: C's two pointers were
    // themselves computed at relation-open time, strictly earlier than this
    // point, and the tuple-side test (`relhasrules`/`relhastriggers` on the
    // freshly read pg_class row) still happens inside the window as in C.
    // Gated on !in_outer_xact so the ANALYZE-in-a-transaction-block path does
    // no work C does not do (C's own gate, vacuum.c:1501).
    let (rules_empty, trigdesc_none) = if in_outer_xact {
        (false, false)
    } else {
        (
            relcache_seams::relation_get_rules::call(relid)?.is_empty(),
            relcache_seams::relation_get_trigger_desc::call(relid)?.is_none(),
        )
    };

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
        // C clears relhasrules/relhastriggers off rd_rules/trigdesc; the
        // relcache seams stand in for the open relcache entry, and are read
        // before the window opens (see the note at the top of this function --
        // a catalog scan in here self-deadlocks on the pg_class page).
        if getattr(old, Anum_pg_class_relhasrules, desc).as_bool() && rules_empty {
            set(Anum_pg_class_relhasrules, ::datum::Datum::from_bool(false), &mut values, &mut replaces, &mut dirty);
        }
        if getattr(old, Anum_pg_class_relhastriggers, desc).as_bool() && trigdesc_none {
            set(Anum_pg_class_relhastriggers, ::datum::Datum::from_bool(false), &mut values, &mut replaces, &mut dirty);
        }
    }

    // relfrozenxid advances only forward, except a stored value in the future
    // (corruption) is overwritten with a WARNING; same for relminmxid.
    let oldfrozenxid = getattr(old, Anum_pg_class_relfrozenxid, desc).as_u32();
    let mut futurexid = false;
    let mut frozenxid_updated = false;
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
            frozenxid_updated = true;
        }
    }

    let oldminmulti = getattr(old, Anum_pg_class_relminmxid, desc).as_u32();
    let mut futuremxid = false;
    let mut minmulti_updated = false;
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
            minmulti_updated = true;
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
    Ok((frozenxid_updated, minmulti_updated))
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

    // Advance the commit-ts oldest value before truncating so concurrent
    // lookups of a just-truncated xid get NULL, not a file-not-found error.
    commit_ts::AdvanceOldestCommitTsXid(frozen_xid)?;

    clog::TruncateCLOG(frozen_xid, oldestxid_datoid)?;
    commit_ts::TruncateCommitTs(frozen_xid)?;
    multixact::TruncateMultiXact(min_multi, minmulti_datoid)?;

    varsup::SetTransactionIdLimit(frozen_xid, oldestxid_datoid)?;
    multixact::SetMultiXactIdLimit(min_multi, minmulti_datoid, false)?;
    Ok(())
}

macro_rules! vacuum_guc_int {
    ($($cell:ident, $get:ident, $set:ident, $var:ident, $boot:expr;)+) => {
        $( guc_tables::session_guc_int!($cell, $get, $set, $boot); )+
        fn install_guc_ints() {
            $(
                guc_tables::vars::$var.install(guc_tables::GucVarAccessors {
                    get: $get,
                    set: $set,
                });
            )+
        }
    };
}

vacuum_guc_int! {
    VACUUM_FREEZE_MIN_AGE, vacuum_freeze_min_age_guc, set_vacuum_freeze_min_age_guc, vacuum_freeze_min_age, 50000000;
    VACUUM_FREEZE_TABLE_AGE, vacuum_freeze_table_age_guc, set_vacuum_freeze_table_age_guc, vacuum_freeze_table_age, 150000000;
    VACUUM_MXID_FREEZE_MIN_AGE, vacuum_multixact_freeze_min_age_guc, set_vacuum_multixact_freeze_min_age_guc, vacuum_multixact_freeze_min_age, 5000000;
    VACUUM_MXID_FREEZE_TABLE_AGE, vacuum_multixact_freeze_table_age_guc, set_vacuum_multixact_freeze_table_age_guc, vacuum_multixact_freeze_table_age, 150000000;
    VACUUM_FAILSAFE_AGE, vacuum_failsafe_age_guc, set_vacuum_failsafe_age_guc, vacuum_failsafe_age, 1600000000;
    VACUUM_MXID_FAILSAFE_AGE, vacuum_multixact_failsafe_age_guc, set_vacuum_multixact_failsafe_age_guc, vacuum_multixact_failsafe_age, 1600000000;
}

guc_tables::session_guc_bool!(VACUUM_TRUNCATE, vacuum_truncate_guc, set_vacuum_truncate_guc, true);
// C home: vacuum.c `bool track_cost_delay_timing`.
guc_tables::session_guc_bool!(
    TRACK_COST_DELAY_TIMING,
    track_cost_delay_timing_guc,
    set_track_cost_delay_timing_guc,
    false
);
guc_tables::session_guc_real!(
    VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE,
    vacuum_max_eager_freeze_failure_rate_guc,
    set_vacuum_max_eager_freeze_failure_rate_guc,
    0.03
);

pub fn init_seams() {
    install_guc_ints();
    guc_tables::vars::vacuum_truncate.install(guc_tables::GucVarAccessors {
        get: vacuum_truncate_guc,
        set: set_vacuum_truncate_guc,
    });
    guc_tables::vars::vacuum_max_eager_freeze_failure_rate.install(guc_tables::GucVarAccessors {
        get: vacuum_max_eager_freeze_failure_rate_guc,
        set: set_vacuum_max_eager_freeze_failure_rate_guc,
    });
    guc_tables::vars::track_cost_delay_timing.install(guc_tables::GucVarAccessors {
        get: track_cost_delay_timing_guc,
        set: set_track_cost_delay_timing_guc,
    });
    // Fixture tests pre-install a relstats sink (no pg_class there); keep it.
    if !vacuum_seams::vac_update_relstats::is_installed() {
        vacuum_seams::vac_update_relstats::set(vac_update_relstats);
    }
    vacuum_seams::vacuum_delay_point::set(vacuum_delay_point);
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

pub fn vacuum_delay_point(is_analyze: bool) -> PgResult<()> {
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
        let delay_start = guc_tables::vars::track_cost_delay_timing
            .read()
            .then(pg_clock::MonoStamp::now);
        std::thread::sleep(std::time::Duration::from_micros((msec * 1000.0) as u64));
        if let Some(delay_start) = delay_start {
            let delay_end = pg_clock::MonoStamp::now();
            let delay_ns = delay_end.since_ns(delay_start) as i64;
            if parallel_seams::is_parallel_worker::call() {
                debug_assert!(!is_analyze);
                let accum = PARALLEL_VACUUM_WORKER_DELAY_NS.get() + delay_ns;
                PARALLEL_VACUUM_WORKER_DELAY_NS.set(accum);
                let since_last_report = LAST_DELAY_REPORT
                    .get()
                    .map_or(i64::MAX, |t| delay_end.since_ns(t) as i64);
                if since_last_report >= PARALLEL_VACUUM_DELAY_REPORT_INTERVAL_NS {
                    pgstat_progress_parallel_incr_param(PROGRESS_VACUUM_DELAY_TIME, accum);
                    LAST_DELAY_REPORT.set(Some(delay_end));
                    PARALLEL_VACUUM_WORKER_DELAY_NS.set(0);
                }
            } else if is_analyze {
                pgstat_progress_incr_param(PROGRESS_ANALYZE_DELAY_TIME, delay_ns);
            } else {
                pgstat_progress_incr_param(PROGRESS_VACUUM_DELAY_TIME, delay_ns);
            }
        }
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
#[track_caller]
fn loc(routine: &'static str) -> ::types_error::ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ::types_error::ErrorLocation::new(site.file(), site.line() as i32, routine)
}

#[cold]
#[inline(never)]
fn unported(unit: &str) -> ! {
    panic!("unported callee reached from vacuum.c: {unit}");
}
