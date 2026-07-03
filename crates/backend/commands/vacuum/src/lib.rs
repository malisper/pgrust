//! vacuum.c lazy lane: ExecVacuum -> vacuum -> vacuum_rel for named tables.
//! FULL/ANALYZE/FREEZE/parallel/database-wide/toast-recursion arms are loud
//! named panics; pg_class relstats + datfrozenxid updates are recorded gaps
//! (heap inplace-update lane unported).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use ::elog::ereport;
use ::mcx::Mcx;
use ::tableam_vocab::{
    VacOptValue, VacuumCutoffs, VacuumParams, VACOPT_FULL, VACOPT_ONLY_DATABASE_STATS,
    VACOPT_PROCESS_MAIN, VACOPT_PROCESS_TOAST, VACOPT_SKIP_DATABASE_STATS, VACOPT_SKIP_LOCKED,
    VACOPT_VACUUM, VACOPT_VERBOSE,
};
use ::types_core::xact::{
    FirstNormalTransactionId, InvalidTransactionId, MultiXactIdPrecedes,
    MultiXactIdPrecedesOrEquals, TransactionIdIsNormal, TransactionIdPrecedes,
    TransactionIdPrecedesOrEquals,
};
use ::types_core::{BlockNumber, InvalidOid, MultiXactId, Oid};
use ::types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERROR, WARNING};
use ::types_nodes::parsenodes::VacuumStmt;
use ::types_nodes::NodeList;
use ::types_rel::lock::{AccessShareLock, NoLock, ShareUpdateExclusiveLock};
use ::types_rel::pg_class::{RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_TOASTVALUE};
use ::types_rel::{Relation, RelationData, LOCKMODE};
use ::types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};

use multixact::{
    FirstMultiXactId, GetOldestMultiXactId, MultiXactIdIsValid, MultiXactMemberFreezeThreshold,
    ReadNextMultiXactId,
};

thread_local! {
    static IN_VACUUM: Cell<bool> = const { Cell::new(false) };
    static VACUUM_FAILSAFE_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

pub fn VacuumFailsafeActive() -> bool {
    VACUUM_FAILSAFE_ACTIVE.get()
}

pub fn SetVacuumFailsafeActive(v: bool) {
    VACUUM_FAILSAFE_ACTIVE.set(v);
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
    for opt_node in vacstmt.options.iter() {
        let opt = opt_node.as_def_elem().expect("VacuumStmt option is DefElem");
        match opt.defname.unwrap_or("") {
            "verbose" => verbose = explain::defGetBoolean(opt)?,
            "skip_locked" => skip_locked = explain::defGetBoolean(opt)?,
            name @ ("analyze" | "freeze" | "full" | "disable_page_skipping" | "index_cleanup"
            | "process_main" | "process_toast" | "truncate" | "parallel"
            | "buffer_usage_limit" | "skip_database_stats" | "only_database_stats") => {
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
        | VACOPT_PROCESS_MAIN
        | VACOPT_PROCESS_TOAST
        | (if verbose { VACOPT_VERBOSE } else { 0 })
        | (if skip_locked { VACOPT_SKIP_LOCKED } else { 0 });

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
    debug_assert!(params.options & VACOPT_VACUUM != 0);
    xact::PreventInTransactionBlock(is_top_level, "VACUUM")?;

    if IN_VACUUM.get() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("VACUUM cannot be executed from VACUUM or ANALYZE")
            .into_error()
            .into());
    }

    if params.options & (VACOPT_ONLY_DATABASE_STATS | VACOPT_SKIP_DATABASE_STATS) != 0 {
        unported("vacuum: database-stats options");
    }
    if relations.is_nil() {
        unported("get_all_vacuum_rels (database-wide VACUUM)");
    }

    // expand_vacuum_rel minimal: named-table lookup under AccessShareLock,
    // held until the pre-pass transaction commits below (C shape). The
    // partitioned-table expansion and permission pre-filter are unported.
    let mut relids: ::mcx::PgVec<'_, Oid> = ::mcx::PgVec::with_capacity_in(relations.len(), mcx);
    for vrel_node in relations.iter() {
        let vrel = vrel_node
            .as_vacuum_relation()
            .expect("vacuum relation list holds VacuumRelation");
        if !vrel.va_cols.is_nil() {
            unported("vacuum: column list (ANALYZE lane)");
        }
        if vrel.oid != InvalidOid {
            relids.push(vrel.oid);
            continue;
        }
        let rv = vrel
            .relation
            .and_then(|n| n.as_range_var())
            .expect("VacuumRelation.relation is RangeVar");
        let rv = rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname: rv.relname.expect("RangeVar.relname"),
            inh: rv.inh,
            relpersistence: rv.relpersistence,
            location: rv.location,
        };
        relids.push(namespace_seams::range_var_get_relid::call(
            mcx,
            &rv,
            AccessShareLock,
            false,
        )?);
    }

    if snapmgr::ActiveSnapshotSet() {
        snapmgr::PopActiveSnapshot()?;
    }
    xact::CommitTransactionCommand()?;

    IN_VACUUM.set(true);
    VACUUM_FAILSAFE_ACTIVE.set(false);
    let result = (|| -> PgResult<()> {
        for relid in relids {
            let params_copy = *params;
            vacuum_rel(mcx, relid, &params_copy, bstrategy.clone())?;
            VACUUM_FAILSAFE_ACTIVE.set(false);
        }
        Ok(())
    })();
    IN_VACUUM.set(false);
    VACUUM_FAILSAFE_ACTIVE.set(false);
    result?;

    // Matches the CommitTransaction waiting in PostgresMain.
    xact::StartTransactionCommand()?;

    // C divergence (recorded): vac_update_datfrozenxid is skipped — the
    // pg_database inplace-update lane is unported.
    Ok(())
}

fn vacuum_rel<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    params: &VacuumParams,
    bstrategy: BufferAccessStrategy,
) -> PgResult<bool> {
    debug_assert!(params.options & VACOPT_FULL == 0);

    xact::StartTransactionCommand()?;
    // C divergence (recorded): PROC_IN_VACUUM/PROC_VACUUM_FOR_WRAPAROUND
    // statusFlags are not set (single-backend milestone; they only shape how
    // concurrent backends compute their horizons).
    let snapshot = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snapshot)?;

    let lmode = ShareUpdateExclusiveLock;
    let rel = match vacuum_open_relation(mcx, relid, params.options, lmode)? {
        Some(rel) => rel,
        None => {
            snapmgr::PopActiveSnapshot()?;
            xact::CommitTransactionCommand()?;
            return Ok(false);
        }
    };

    if !matches!(
        rel.rd_rel.relkind,
        RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE
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

    // C divergence (recorded): LockRelationIdForSession is skipped — no toast
    // recursion happens (loud below), so no cross-transaction lock is needed.

    let mut params = *params;
    if params.index_cleanup == VacOptValue::Unspecified {
        // StdRdOptions (reloptions) unported: AUTO is C's no-reloption default.
        params.index_cleanup = VacOptValue::Auto;
    }
    if params.truncate == VacOptValue::Unspecified {
        params.truncate = if guc_tables::vars::vacuum_truncate.read() {
            VacOptValue::Enabled
        } else {
            VacOptValue::Disabled
        };
    }

    if params.options & VACOPT_PROCESS_TOAST != 0 && rel.rd_rel.reltoastrelid != InvalidOid {
        unported("vacuum_rel: TOAST table recursion");
    }

    if params.options & VACOPT_PROCESS_MAIN != 0 {
        // C divergence (recorded): SetUserIdAndSecContext/NewGUCNestLevel/
        // RestrictSearchPath are skipped (single-user milestone).
        tableam_seams::table_relation_vacuum::call(mcx, &rel, &params, bstrategy)?;
    }

    rel.close(NoLock)?;
    snapmgr::PopActiveSnapshot()?;
    xact::CommitTransactionCommand()?;
    Ok(true)
}

fn vacuum_open_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    options: u32,
    lmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    debug_assert!(options & VACOPT_VACUUM != 0);
    if options & VACOPT_SKIP_LOCKED != 0 {
        unported("vacuum_open_relation: SKIP_LOCKED (ConditionalLockRelationOid)");
    }
    let rel = relation::try_relation_open(mcx, relid, lmode)?;
    if rel.is_none() {
        ereport(WARNING)
            .errmsg(format!(
                "skipping vacuum of relation {relid} --- relation no longer exists"
            ))
            .finish(loc("vacuum_open_relation"))?;
    }
    Ok(rel)
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

/// vac_update_relstats (vacuum.c), ANALYZE subset: the seam carries no
/// frozenxid/minmulti (C's ANALYZE passes Invalid); the VACUUM freeze arm
/// rides with its lane.
pub fn vac_update_relstats(
    relation: &RelationData<'_>,
    num_pages: BlockNumber,
    num_tuples: f64,
    num_all_visible_pages: BlockNumber,
    num_all_frozen_pages: BlockNumber,
    hasindex: bool,
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

    if dirty {
        let newtup =
            heaptuple::heap_modify_tuple(mcx, old, desc, &values, &nulls, &replaces)?;
        genam::systable_inplace_update_finish(mcx, inplace_state, newtup.as_tuple())?;
    } else {
        genam::systable_inplace_update_cancel(mcx, inplace_state)?;
    }
    table::table_close(rd, RowExclusiveLock)?;
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

pub fn init_seams() {
    use std::sync::atomic::Ordering::Relaxed;
    install_guc_ints();
    guc_tables::vars::vacuum_truncate.install(guc_tables::GucVarAccessors {
        get: || VACUUM_TRUNCATE.load(Relaxed),
        set: |v| VACUUM_TRUNCATE.store(v, Relaxed),
    });
    vacuum_seams::vac_update_relstats::set(vac_update_relstats);
}

pub fn vacuum_delay_point(_is_analyze: bool) -> PgResult<()> {
    if init_small::globals::VacuumCostActive() {
        unported("vacuum_delay_point: cost-based delay (VacuumCostActive)");
    }
    Ok(())
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
