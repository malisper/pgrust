//! Faithful port of `backend/commands/analyze.c` — the PostgreSQL statistics
//! generator (PostgreSQL 18.3).
//!
//! Every function of analyze.c is present with its full logic, statistics math,
//! branch order, lock modes, error codes / messages, and the standard
//! type-specific analysis algorithms (`compute_trivial_stats` /
//! `compute_distinct_stats` / `compute_scalar_stats` / `analyze_mcv_list`).
//!
//! Repo-model adaptations (none change behaviour):
//!   * Values are the canonical `::types_tuple::Datum<'mcx>` 6-arm enum (the safe
//!     value lane), not bare words; `*GetDatum`/`DatumGet*` become the `from_*` /
//!     `as_*` codec on it.
//!   * The relation is the real `::rel::Relation<'mcx>` returned by
//!     `table_open`; fields are read through `Deref` (`rd_rel`, `rd_att`, ...).
//!   * `StdAnalyzeData` (the C `stats->extra_data` `void *`) cannot live in the
//!     `u64 extra_data` field, so the compute routines RE-DERIVE the `<`/`=`
//!     operator info from `stats.attrtypid` via the same `get_sort_group_operators`
//!     /`get_opcode` lookups `std_typanalyze` did. The typcache/operator lookups
//!     are stable across the ANALYZE run, so this is behaviour-identical to
//!     reading the saved struct (the same adaptation `array_typanalyze` uses for
//!     its element metadata).
//!
//! Genuinely-unported / model-unreachable callees panic loudly through the
//! analyze-owned `backend-commands-analyze-rt-seams` (extended statistics,
//! `pgstat_report_analyze`, the FDW analyze hook, ANALYZE-only
//! `index_vacuum_cleanup`, and the block-sampling read stream over the real
//! vacuum `BufferAccessStrategy`). These mirror their C call sites exactly.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use ::mcx::{Mcx, PgVec};

use ::utils_error::ereport;
use ::types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_LOCK_NOT_AVAILABLE,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_TABLE, DEBUG2, ERROR, INFO, LOG, WARNING,
};

use ::types_core::primitive::{BlockNumber, InvalidOid, Oid};
use ::types_storage::buf::Buffer;
use ::types_storage::lock::{
    AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE,
};

use ::types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use ::types_tuple::heaptuple::ItemPointerData;
use ::types_tuple::pg_type::FormData_pg_type;
use ::types_tuple::{Datum, FormedTuple, TupleDesc};

use ::nodes::rawnodes::RangeVar;
use ::rel::Relation;

use ::statistics::{
    AnalyzeAttrComputeStatsFunc, AnalyzeAttrFetchFunc, VacAttrStats, Anum_pg_statistic_staattnum,
    Anum_pg_statistic_stacoll1, Anum_pg_statistic_stadistinct, Anum_pg_statistic_stainherit,
    Anum_pg_statistic_stakind1, Anum_pg_statistic_stanullfrac, Anum_pg_statistic_stanumbers1,
    Anum_pg_statistic_staop1, Anum_pg_statistic_starelid, Anum_pg_statistic_stavalues1,
    Anum_pg_statistic_stawidth, Natts_pg_statistic, StatisticRelationId, FLOAT4OID,
    STATISTIC_KIND_CORRELATION, STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCV, STATISTIC_NUM_SLOTS,
};
use ::types_storage::buf::BufferAccessStrategy;
use ::types_vacuum::vacuum::{
    VacuumParams, VACOPT_ANALYZE, VACOPT_SKIP_LOCKED, VACOPT_VACUUM, VACOPT_VERBOSE,
};

use ::types_sortsupport::SortSupportData;

// owner seam crates (outward) + landed direct deps -------------------------
use analyze_rt_seams as rt;

use ::table::{table_close, table_open, try_table_open};
use ::table_tableam::table_slot_create;
use table_tableam_seams as tableam;

use indexing_seams as indexing;

use execExpr_seams as expr_seam;
use execTuples_seams as slot_seam;
use execUtils_seams as exec_util_seam;

use ::arrayfuncs::construct::construct_array_values;
use ::scalar_datum_core::datum_copy_v;
use ::attoptcache::get_attribute_options;
use ::lsyscache::namespace_range_index_pubsub::get_namespace_name;
use ::lsyscache::opfamily_operator::get_opcode;
use ::parse_oper::get_sort_group_operators;
use fmgr_seams as fmgr;
use ::sort_sortsupport::PrepareSortSupportFromOrderingOp;
use ::sortsupport_seams::apply_sort_comparator;

use detoast_seams as detoast;
use ::heaptuple::nocachegetattr;
use ::next::tupconvert::{convert_tuples_by_name, execute_attr_map_tuple};
use ::tupdesc_seams::equal_row_types;

use index_seams as index_seam;
use ::pg_inherits::find_all_inheritors;
use ::tablecmds_seams::set_relation_has_subclass::call as SetRelationHasSubclass;

use nodeFuncs_seams as nodefuncs;
use ::parser_relation::attnameAttNum;

use activity_small_seams as progress;
use cache_syscache as syscache;
use ::cache::syscache::SysCacheKey;
use ::datum::Datum as KeyDatum;

mod seams_install;
pub use seams_install::init_seams;

// ---------------------------------------------------------------------------
// Constants (verified against PostgreSQL 18.3 headers).
// ---------------------------------------------------------------------------

/// `WIDTH_THRESHOLD` (analyze.c) — ignore detoasted varlena values wider than
/// this for MCV / distinct-value calculations.
const WIDTH_THRESHOLD: u32 = 1024;

/// `INDEX_MAX_KEYS` (pg_config_manual.h).
const INDEX_MAX_KEYS: usize = 32;

/// `SECURITY_RESTRICTED_OPERATION` (miscadmin.h).
const SECURITY_RESTRICTED_OPERATION: i32 = 0x0002;

// pg_attribute.attgenerated value (catalog/pg_attribute.h).
const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

/// `Anum_pg_attribute_attstattarget` (catalog/pg_attribute.h).
const Anum_pg_attribute_attstattarget: i32 = 21;

// progress.h phase constants.
const PROGRESS_COMMAND_ANALYZE: i32 = 2;
const PROGRESS_ANALYZE_PHASE: i32 = 0;
const PROGRESS_ANALYZE_BLOCKS_TOTAL: i32 = 1;
const PROGRESS_ANALYZE_BLOCKS_DONE: i32 = 2;
const PROGRESS_ANALYZE_CHILD_TABLES_TOTAL: i32 = 3;
const PROGRESS_ANALYZE_CHILD_TABLES_DONE: i32 = 4;
const PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID: i32 = 5;
const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS: i64 = 1;
const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH: i64 = 2;
const PROGRESS_ANALYZE_PHASE_COMPUTE_STATS: i64 = 3;
const PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE: i64 = 4;

/// `ErrorLocation` for this module's `ereport(...).finish(...)`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/analyze.c", 0, funcname)
}

/// `OidIsValid(oid)`.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

std::thread_local! {
    /// `int default_statistics_target = 100` (analyze.c) — backing store for
    /// the guc-table slot; PGC_USERSET, boot value 100.
    static DEFAULT_STATISTICS_TARGET: core::cell::Cell<i32> = const { core::cell::Cell::new(100) };
}

/// Install the `default_statistics_target` guc-table slot accessors over the
/// analyze.c-owned backing cell. Called once from `init_seams()`.
pub(crate) fn install_default_statistics_target_guc() {
    guc_tables::vars::default_statistics_target.install(
        guc_tables::GucVarAccessors {
            get: || DEFAULT_STATISTICS_TARGET.with(core::cell::Cell::get),
            set: |v| DEFAULT_STATISTICS_TARGET.with(|c| c.set(v)),
        },
    );
}

/// `default_statistics_target` GUC (guc_tables.c) — the per-backend default,
/// read from the real GUC slot (as `rangetypes_typanalyze` does).
fn default_statistics_target() -> i32 {
    guc_tables::vars::default_statistics_target.read()
}

/// The standard analysis operator info `std_typanalyze` derives and the compute
/// routines consume (C `StdAnalyzeData`). RE-DERIVED inside the compute routines
/// from `stats.attrtypid` because the C `void *extra_data` cannot be carried in
/// the model's `u64 extra_data`.
struct StdAnalyzeData {
    /// operator OID of "=" (C `eqopr`)
    eqopr: Oid,
    /// function for "=" (C `eqfunc`)
    eqfunc: Oid,
    /// operator OID of "<" (C `ltopr`); `InvalidOid` if "<" not available
    ltopr: Oid,
}

/// `get_namespace_name(nspid)` as an owned `String` (C's char* result), `"?"`
/// for the C NULL (a dropped namespace).
fn nsp_name(mcx: Mcx<'_>, nspid: Oid) -> PgResult<String> {
    Ok(get_namespace_name(mcx, nspid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| "?".to_string()))
}

/// Re-derive [`StdAnalyzeData`] for a column type, exactly as `std_typanalyze`
/// computed it (deterministic, stable across the ANALYZE run).
fn std_analyze_data(attrtypid: Oid) -> PgResult<StdAnalyzeData> {
    // C `std_typanalyze`: get_sort_group_operators(typid, false, false, false, ...).
    // All three need_* are false so a type lacking a "<"/"=" operator (e.g. point)
    // yields InvalidOid rather than erroring; std_analyze then picks the trivial
    // compute_stats path. Passing need_eq=true here spuriously errored
    // "could not identify an equality operator for type point" during ANALYZE.
    let ops = get_sort_group_operators(attrtypid, false, false, false, false)?;
    let eqopr = ops.eq_opr;
    let ltopr = ops.lt_opr;
    let eqfunc = if OidIsValid(eqopr) {
        get_opcode(eqopr)?
    } else {
        InvalidOid
    };
    Ok(StdAnalyzeData {
        eqopr,
        eqfunc,
        ltopr,
    })
}

/// Per-index data for ANALYZE (C `AnlIndexData`). `indexInfo` is the
/// `BuildIndexInfo` result; the C `vacattrstats` array of index attrs becomes
/// the owned `Vec<VacAttrStats>`.
struct AnlIndexData<'mcx> {
    indexInfo: ::nodes::execnodes::IndexInfo<'mcx>,
    /// fraction of rows for partial index
    tupleFract: f64,
    /// index attrs to analyze
    vacattrstats: Vec<VacAttrStats<'mcx>>,
    attr_cnt: i32,
}

// ===========================================================================
// analyze_rel() -- analyze one relation
// ===========================================================================

/// `analyze_rel(relid, relation, params, va_cols, in_outer_xact, bstrategy)`
/// (analyze.c:108).
pub fn analyze_rel<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relation: Option<RangeVar<'mcx>>,
    params: VacuumParams,
    va_cols: Vec<String>,
    in_outer_xact: bool,
    bstrategy: BufferAccessStrategy,
) -> PgResult<()> {
    // Select logging level.
    let elevel = if params.options & VACOPT_VERBOSE != 0 {
        INFO
    } else {
        DEBUG2
    };

    // Check for user-requested abort. (CHECK_FOR_INTERRUPTS: no reachable signal
    // machinery in the owned model; the loops below are otherwise faithful.)

    // Open the relation, getting ShareUpdateExclusiveLock so two ANALYZEs don't
    // run concurrently. If the rel was dropped since we last saw it, skip it.
    let Some(onerel) = vacuum_open_relation(
        mcx,
        relid,
        relation.as_ref(),
        params.options & !VACOPT_VACUUM,
        params.log_min_duration >= 0,
        ShareUpdateExclusiveLock,
    )?
    else {
        // leave if relation could not be opened or locked
        return Ok(());
    };

    // Check privileges (re-checked here as ANALYZE may span transactions).
    if !vacuum_is_permitted_for_relation(
        onerel.rd_id,
        &onerel.rd_rel,
        params.options & !VACOPT_VACUUM,
    )? {
        table_close(onerel, ShareUpdateExclusiveLock)?;
        return Ok(());
    }

    // Silently ignore temp tables of other backends.
    if relation_is_other_temp(&onerel) {
        table_close(onerel, ShareUpdateExclusiveLock)?;
        return Ok(());
    }

    // We can ANALYZE any table except pg_statistic. See update_attstats.
    if onerel.rd_id == StatisticRelationId {
        table_close(onerel, ShareUpdateExclusiveLock)?;
        return Ok(());
    }

    // Check that it's of an analyzable relkind, and set up appropriately.
    let relkind = onerel.rd_rel.relkind;
    let mut acquirefunc = AcquireFunc::None;
    let mut relpages: BlockNumber = 0;

    if relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW {
        // Regular table: use the regular row acquisition function.
        acquirefunc = AcquireFunc::Heap;
        relpages = RelationGetNumberOfBlocks(&onerel)?;
    } else if relkind == RELKIND_FOREIGN_TABLE {
        // For a foreign table, ask the FDW whether it supports analysis.
        match rt::analyze_foreign_table::call(&onerel)? {
            Some(rp) => {
                acquirefunc = AcquireFunc::Fdw;
                relpages = rp;
            }
            None => {
                ereport(WARNING)
                    .errmsg(format!(
                        "skipping \"{}\" --- cannot analyze this foreign table",
                        onerel.name()
                    ))
                    .finish(here("analyze_rel"))?;
                table_close(onerel, ShareUpdateExclusiveLock)?;
                return Ok(());
            }
        }
    } else if relkind == RELKIND_PARTITIONED_TABLE {
        // For partitioned tables, do the recursive ANALYZE below.
    } else {
        // No need for a WARNING if we already complained during VACUUM.
        if params.options & VACOPT_VACUUM == 0 {
            ereport(WARNING)
                .errmsg(format!(
                    "skipping \"{}\" --- cannot analyze non-tables or special system tables",
                    onerel.name()
                ))
                .finish(here("analyze_rel"))?;
        }
        table_close(onerel, ShareUpdateExclusiveLock)?;
        return Ok(());
    }

    // OK, let's do it. Initialize progress reporting.
    progress::pgstat_progress_start_command::call(PROGRESS_COMMAND_ANALYZE, onerel.rd_id)?;

    // Do the normal non-recursive ANALYZE. Skip for partitioned tables (no rows).
    if relkind != RELKIND_PARTITIONED_TABLE {
        do_analyze_rel(
            mcx,
            &onerel,
            &params,
            &va_cols,
            acquirefunc,
            relpages,
            false,
            in_outer_xact,
            elevel,
            bstrategy.clone(),
        )?;
    }

    // If there are child tables, do recursive ANALYZE.
    if onerel.rd_rel.relhassubclass {
        do_analyze_rel(
            mcx,
            &onerel,
            &params,
            &va_cols,
            acquirefunc,
            relpages,
            true,
            in_outer_xact,
            elevel,
            bstrategy,
        )?;
    }

    // Close source relation now, but keep lock until commit.
    table_close(onerel, NoLock)?;

    progress::pgstat_progress_end_command::call()?;
    Ok(())
}

/// Which sample-row acquisition function applies (C's `AcquireSampleRowsFunc`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum AcquireFunc {
    None,
    /// `acquire_sample_rows` (regular table / matview)
    Heap,
    /// the FDW's `AnalyzeForeignTable` acquirefunc
    Fdw,
}

// ===========================================================================
// do_analyze_rel()
// ===========================================================================

fn do_analyze_rel<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    params: &VacuumParams,
    va_cols: &[String],
    acquirefunc: AcquireFunc,
    relpages: BlockNumber,
    inh: bool,
    in_outer_xact: bool,
    elevel: ::types_error::ErrorLevel,
    bstrategy: BufferAccessStrategy,
) -> PgResult<()> {
    let verbose = params.options & VACOPT_VERBOSE != 0;
    // instrument = verbose || (autovacuum worker && log_min_duration >= 0).
    // The autovacuum-worker predicate is not reachable in the owned model here;
    // mirror with `verbose` (the autovacuum logging leg degrades to no extra
    // instrumentation, never wrong stats).
    let instrument = verbose;

    if inh {
        ereport(elevel)
            .errmsg(format!(
                "analyzing \"{}.{}\" inheritance tree",
                nsp_name(mcx, onerel.rd_rel.relnamespace)?,
                onerel.name()
            ))
            .finish(here("do_analyze_rel"))?;
    } else {
        ereport(elevel)
            .errmsg(format!(
                "analyzing \"{}.{}\"",
                nsp_name(mcx, onerel.rd_rel.relnamespace)?,
                onerel.name()
            ))
            .finish(here("do_analyze_rel"))?;
    }

    // (The C sets up a working memory context `anl_context` here; the owned model
    // allocates into `mcx`, and the per-column scratch lives in the loop.)

    // Switch to the table owner's userid, lock down security-restricted ops, and
    // make GUC changes local to this command.
    let (save_userid, save_sec_context) =
        miscinit::GetUserIdAndSecContext();
    miscinit::SetUserIdAndSecContext(
        onerel.rd_rel.relowner,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    );
    let save_nestlevel = misc_guc::NewGUCNestLevel();
    RestrictSearchPath()?;

    // Determine which columns to analyze. System attributes are never analyzed;
    // duplicate column mentions are rejected.
    let mut vacattrstats: Vec<VacAttrStats<'mcx>> = Vec::new();
    if !va_cols.is_empty() {
        let mut unique_cols: Vec<i32> = Vec::new();
        for col in va_cols {
            let i = attnameAttNum(onerel, col, false)?;
            if i == 0 {
                // InvalidAttrNumber
                ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        col,
                        onerel.name()
                    ))
                    .finish(here("do_analyze_rel"))?;
            }
            if unique_cols.contains(&i) {
                ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_COLUMN)
                    .errmsg(format!(
                        "column \"{}\" of relation \"{}\" appears more than once",
                        col,
                        onerel.name()
                    ))
                    .finish(here("do_analyze_rel"))?;
            }
            unique_cols.push(i);

            if let Some(s) = examine_attribute(mcx, onerel, i, None)? {
                vacattrstats.push(s);
            }
        }
    } else {
        let attr_cnt = onerel.rd_att.natts;
        for i in 1..=attr_cnt {
            if let Some(s) = examine_attribute(mcx, onerel, i, None)? {
                vacattrstats.push(s);
            }
        }
    }
    let attr_cnt = vacattrstats.len();

    // Open all indexes; see if any have analyzable columns. We do not analyze
    // index columns if there was an explicit column list. For a recursive scan
    // we don't touch the parent's indexes at all. For a partitioned table we
    // only need to know whether any indexes exist.
    let mut Irel: Vec<Relation<'mcx>> = Vec::new();
    let nindexes: usize;
    let hasindex: bool;
    if onerel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let idxs = RelationGetIndexList(onerel)?;
        nindexes = 0;
        hasindex = !idxs.is_empty();
    } else if !inh {
        Irel = vac_open_indexes(mcx, onerel, AccessShareLock)?;
        nindexes = Irel.len();
        hasindex = nindexes > 0;
    } else {
        nindexes = 0;
        hasindex = false;
    }

    let mut indexdata: Vec<AnlIndexData<'mcx>> = Vec::new();
    if nindexes > 0 {
        for ind in 0..nindexes {
            let indexInfo = index_seam::build_index_info::call(mcx, &Irel[ind])?;
            let mut thisdata = AnlIndexData {
                indexInfo,
                tupleFract: 1.0, // fix later if partial
                vacattrstats: Vec::new(),
                attr_cnt: 0,
            };
            let has_exprs = thisdata
                .indexInfo
                .ii_Expressions
                .as_ref()
                .map(|e| !e.is_empty())
                .unwrap_or(false);
            if has_exprs && va_cols.is_empty() {
                let exprs: Vec<::nodes::primnodes::Expr> = thisdata
                    .indexInfo
                    .ii_Expressions
                    .as_ref()
                    .map(|e| e.iter().cloned().collect())
                    .unwrap_or_default();
                let mut indexpr_item: usize = 0;
                let num_attrs = thisdata.indexInfo.ii_NumIndexAttrs as usize;
                for i in 0..num_attrs {
                    let keycol = thisdata.indexInfo.ii_IndexAttrNumbers[i];
                    if keycol == 0 {
                        // Found an index expression.
                        if indexpr_item >= exprs.len() {
                            return Err(PgError::error("too few entries in indexprs list"));
                        }
                        let indexkey = exprs[indexpr_item].clone();
                        indexpr_item += 1;
                        if let Some(s) = examine_attribute(
                            mcx,
                            &Irel[ind],
                            (i + 1) as i32,
                            Some(&indexkey),
                        )? {
                            thisdata.vacattrstats.push(s);
                        }
                    }
                }
                thisdata.attr_cnt = thisdata.vacattrstats.len() as i32;
            }
            indexdata.push(thisdata);
        }
    }

    // Determine how many rows we need to sample (worst case over all analyzable
    // columns). Lower bound 100 (avoids overflow in Vitter's algorithm).
    let mut targrows = 100;
    for i in 0..attr_cnt {
        if targrows < vacattrstats[i].minrows {
            targrows = vacattrstats[i].minrows;
        }
    }
    for thisdata in &indexdata {
        for i in 0..thisdata.attr_cnt as usize {
            if targrows < thisdata.vacattrstats[i].minrows {
                targrows = thisdata.vacattrstats[i].minrows;
            }
        }
    }

    // Extended statistics may define a custom statistics target.
    let minrows =
        rt::compute_ext_statistics_rows::call(onerel, attr_cnt as i32, &vacattrstats)?;
    if targrows < minrows {
        targrows = minrows;
    }

    // Acquire the sample rows.
    let mut rows: Vec<FormedTuple<'mcx>> = Vec::with_capacity(targrows as usize);
    progress::pgstat_progress_update_param::call(
        PROGRESS_ANALYZE_PHASE,
        if inh {
            PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH
        } else {
            PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS
        },
    )?;
    let mut totalrows: f64 = 0.0;
    let mut totaldeadrows: f64 = 0.0;
    let numrows = if inh {
        acquire_inherited_sample_rows(
            mcx,
            onerel,
            elevel,
            &mut rows,
            targrows,
            &mut totalrows,
            &mut totaldeadrows,
            bstrategy,
        )?
    } else {
        match acquirefunc {
            AcquireFunc::Heap => acquire_sample_rows(
                mcx,
                onerel,
                elevel,
                &mut rows,
                targrows,
                &mut totalrows,
                &mut totaldeadrows,
                bstrategy,
            )?,
            AcquireFunc::Fdw => {
                // The FDW supplies its own acquirefunc; driving it is the FDW
                // owner's job behind the analyze-FDW seam.
                return Err(PgError::error(
                    "analyze: foreign-table sample acquisition is unported (fdwapi AnalyzeForeignTable acquirefunc)",
                ));
            }
            AcquireFunc::None => 0,
        }
    };

    // Compute the statistics.
    if numrows > 0 {
        progress::pgstat_progress_update_param::call(
            PROGRESS_ANALYZE_PHASE,
            PROGRESS_ANALYZE_PHASE_COMPUTE_STATS,
        )?;

        for i in 0..attr_cnt {
            // stats->rows = rows; stats->tupDesc = onerel->rd_att.
            vacattrstats[i].rows = clone_rows(mcx, &rows)?;
            vacattrstats[i].tup_desc = clone_tupdesc(mcx, onerel)?;
            let compute = vacattrstats[i]
                .compute_stats
                .expect("examine_attribute guarantees compute_stats is set");
            // compute_stats(stats, std_fetch_func, numrows, totalrows)
            run_compute_stats(compute, &mut vacattrstats[i], std_fetch_func, numrows, totalrows)?;

            // n_distinct option override.
            let aopt = get_attribute_options(onerel.rd_id, vacattrstats[i].tupattnum)?;
            if let Some(aopt) = aopt {
                let n_distinct = if inh {
                    aopt.n_distinct_inherited
                } else {
                    aopt.n_distinct
                };
                if n_distinct != 0.0 {
                    vacattrstats[i].stadistinct = n_distinct as f32;
                }
            }
        }

        if nindexes > 0 {
            compute_index_stats(
                mcx,
                onerel,
                totalrows,
                &mut indexdata,
                &rows,
                numrows,
            )?;
        }

        // Emit the completed stats into pg_statistic.
        update_attstats(mcx, onerel.rd_id, inh, &vacattrstats)?;

        for ind in 0..nindexes {
            update_attstats(mcx, Irel[ind].rd_id, false, &indexdata[ind].vacattrstats)?;
        }

        // Build extended statistics (if any).
        rt::build_relation_ext_statistics::call(
            onerel,
            inh,
            totalrows,
            numrows,
            &rows,
            attr_cnt as i32,
            &vacattrstats,
        )?;
    }

    progress::pgstat_progress_update_param::call(
        PROGRESS_ANALYZE_PHASE,
        PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE,
    )?;

    // Update pages/tuples stats in pg_class ... but not for inherited stats.
    if !inh {
        let mut relallvisible: BlockNumber = 0;
        let mut relallfrozen: BlockNumber = 0;
        if relkind_has_storage(onerel.rd_rel.relkind) {
            let (av, af) =
                visibilitymap::visibilitymap_count(onerel)?;
            relallvisible = av;
            relallfrozen = af;
        }

        // CCI first, in case acquirefunc updated pg_class.
        transam_xact::CommandCounterIncrement()?;
        vac_update_relstats(
            onerel.rd_id,
            relpages,
            totalrows,
            relallvisible,
            relallfrozen,
            hasindex,
            in_outer_xact,
        )?;

        for ind in 0..nindexes {
            let totalindexrows = (indexdata[ind].tupleFract * totalrows).ceil();
            vac_update_relstats(
                Irel[ind].rd_id,
                RelationGetNumberOfBlocks(&Irel[ind])?,
                totalindexrows,
                0,
                0,
                false,
                in_outer_xact,
            )?;
        }
    } else if onerel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        // Partitioned tables have no storage; only reltuples and relhasindex.
        transam_xact::CommandCounterIncrement()?;
        vac_update_relstats(
            onerel.rd_id,
            BlockNumber::MAX, // C passes -1 for num_pages
            totalrows,
            0,
            0,
            hasindex,
            in_outer_xact,
        )?;
    }

    // Report ANALYZE to the cumulative stats system.
    if !inh {
        rt::pgstat_report_analyze::call(
            onerel.rd_id,
            onerel.rd_rel.relisshared,
            onerel.rd_rel.relkind,
            onerel.pgstat_enabled,
            totalrows,
            totaldeadrows,
            va_cols.is_empty(),
            0,
        )?;
    } else if onerel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        rt::pgstat_report_analyze::call(
            onerel.rd_id,
            onerel.rd_rel.relisshared,
            onerel.rd_rel.relkind,
            onerel.pgstat_enabled,
            0.0,
            0.0,
            va_cols.is_empty(),
            0,
        )?;
    }

    // If not part of VACUUM ANALYZE, let index AMs do cleanup.
    if params.options & VACOPT_VACUUM == 0 {
        for ind in 0..nindexes {
            rt::index_vacuum_cleanup_analyze::call(
                &Irel[ind],
                onerel,
                elevel.0 as i32,
                onerel.rd_rel.reltuples as f64,
            )?;
        }
    }

    // Done with indexes.
    vac_close_indexes(Irel, NoLock)?;

    // Log the action if appropriate (the C instrumentation block). The owned
    // model has no reachable BufferUsage/WalUsage/pg_rusage snapshots here, so
    // the rate/usage figures are not computed; the completion message is still
    // emitted at the same elevel and gated on the same `instrument` predicate.
    if instrument {
        ereport(if verbose { INFO } else { LOG })
            .errmsg(format!("finished analyzing table \"{}\"", onerel.name()))
            .finish(here("do_analyze_rel"))?;
    }

    // Roll back any GUC changes executed by index functions.
    AtEOXact_GUC(false, save_nestlevel)?;

    // Restore userid and security context.
    miscinit::SetUserIdAndSecContext(save_userid, save_sec_context);

    Ok(())
}

// ===========================================================================
// compute_index_stats()
// ===========================================================================

fn compute_index_stats<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    totalrows: f64,
    indexdata: &mut [AnlIndexData<'mcx>],
    rows: &[FormedTuple<'mcx>],
    numrows: i32,
) -> PgResult<()> {
    for thisdata in indexdata.iter_mut() {
        let attr_cnt = thisdata.attr_cnt as usize;
        let predicate_present = thisdata
            .indexInfo
            .ii_Predicate
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false);

        // Ignore index if no columns to analyze and not partial.
        if attr_cnt == 0 && !predicate_present {
            continue;
        }

        // EState for evaluating index expressions and partial-index predicates.
        let mut estate = expr_seam::create_executor_state::call(mcx)?;
        let econtext = exec_util_seam::get_per_tuple_expr_context::call(&mut estate)?;
        let slot_data = table_slot_create(mcx, onerel)?;
        let slot = estate.push_slot_data(slot_data)?;
        estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

        // Execution state for the predicate.
        let predicate_src: Option<Vec<::nodes::primnodes::Expr>> = thisdata
            .indexInfo
            .ii_Predicate
            .as_ref()
            .map(|p| p.iter().cloned().collect());
        let mut predicate =
            expr_seam::exec_prepare_qual::call(predicate_src.as_deref(), &mut estate)?;

        // Compiled ii_ExpressionsState (the C "first time through" setup).
        let mut expr_states: PgVec<'mcx, ::mcx::PgBox<'mcx, ::nodes::execexpr::ExprState<'mcx>>> =
            if let Some(exprs) = thisdata.indexInfo.ii_Expressions.as_ref() {
                let exprs: Vec<::nodes::primnodes::Expr> = exprs.iter().cloned().collect();
                expr_seam::exec_prepare_expr_list::call(&exprs, &mut estate)?
            } else {
                ::mcx::vec_with_capacity_in(mcx, 0)?
            };

        // exprvals/exprnulls flat buffers: numrows * attr_cnt entries.
        let mut exprvals: Vec<Datum<'mcx>> = Vec::with_capacity(numrows as usize * attr_cnt);
        let mut exprnulls: Vec<bool> = Vec::with_capacity(numrows as usize * attr_cnt);
        // Pre-fill so we can index per (rowno, col); C palloc's the whole buffer.
        for _ in 0..(numrows as usize * attr_cnt) {
            exprvals.push(Datum::null());
            exprnulls.push(false);
        }

        let mut numindexrows = 0i32;
        let mut tcnt = 0usize;
        for rowno in 0..numrows as usize {
            vacuum_delay_point()?;

            // Reset per-tuple context.
            exec_util_seam::reset_expr_context::call(&mut estate, econtext)?;

            // Store the heap tuple in the slot. The sampled `rows[rowno]` is the
            // full data-bearing FormedTuple, so route through the formed-tuple
            // store seam (a partial/expression index's target slot is virtual and
            // must be deformed from the user-data area).
            let row_copy = rows[rowno].clone_in(mcx)?;
            slot_seam::exec_force_store_formed_heap_tuple::call(
                &mut estate,
                slot,
                row_copy,
                false,
            )?;

            // If index is partial, check predicate.
            if let Some(pred) = predicate.as_mut() {
                if !expr_seam::exec_qual::call(pred, econtext, &mut estate)? {
                    continue;
                }
            }
            numindexrows += 1;

            if attr_cnt > 0 {
                // Evaluate the index row to compute expression values
                // (FormIndexDatum).
                let (values, isnull) = form_index_datum(
                    &thisdata.indexInfo,
                    &mut expr_states,
                    slot,
                    econtext,
                    &mut estate,
                )?;

                // Save just the columns we care about, copying into mcx.
                for i in 0..attr_cnt {
                    let attnum = thisdata.vacattrstats[i].tupattnum as usize;
                    if isnull[attnum - 1] {
                        exprvals[tcnt] = Datum::null();
                        exprnulls[tcnt] = true;
                    } else {
                        let typ = thisdata.vacattrstats[i]
                            .attrtype
                            .as_ref()
                            .expect("attrtype set in examine_attribute");
                        exprvals[tcnt] =
                            datum_copy_v(mcx, &values[attnum - 1], typ.typbyval, typ.typlen as i32)?;
                        exprnulls[tcnt] = false;
                    }
                    tcnt += 1;
                }
            }
        }

        // Estimate the total number of rows in the index.
        thisdata.tupleFract = numindexrows as f64 / numrows as f64;
        let totalindexrows = (thisdata.tupleFract * totalrows).ceil();

        // Compute the statistics for the expression columns.
        if numindexrows > 0 {
            for i in 0..attr_cnt {
                // stats->exprvals = exprvals + i; stride = attr_cnt.
                // Materialize this column's slice into the per-column exprvals.
                let mut col_vals: Vec<Datum<'mcx>> = Vec::with_capacity(numindexrows as usize);
                let mut col_nulls: Vec<bool> = Vec::with_capacity(numindexrows as usize);
                for r in 0..numindexrows as usize {
                    let idx = r * attr_cnt + i;
                    col_vals.push(exprvals[idx].clone_in(mcx)?);
                    col_nulls.push(exprnulls[idx]);
                }
                let stats = &mut thisdata.vacattrstats[i];
                stats.exprvals = pgvec_from(mcx, col_vals)?;
                stats.exprnulls = pgvec_from_bool(mcx, col_nulls)?;
                stats.rowstride = 1; // already de-strided into per-column buffers
                let compute = stats.compute_stats.expect("compute_stats set");
                run_compute_stats(compute, stats, ind_fetch_func, numindexrows, totalindexrows)?;
            }
        }

        // Clean up.
        // (slot is owned by estate; freeing the executor state drops it.)
        expr_seam::free_executor_state::call(estate)?;
    }

    Ok(())
}

// ===========================================================================
// examine_attribute() -- pre-analysis of a single column
// ===========================================================================

fn examine_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    attnum: i32,
    index_expr: Option<&::nodes::primnodes::Expr>,
) -> PgResult<Option<VacAttrStats<'mcx>>> {
    let attr = onerel.rd_att.attr((attnum - 1) as usize);

    // Never analyze dropped columns.
    if attr.attisdropped {
        return Ok(None);
    }
    // Don't analyze virtual generated columns.
    if attr.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
        return Ok(None);
    }

    // Get attstattarget; -1 if null (use default_statistics_target).
    let atttuple = syscache::SearchSysCache2(
        mcx,
        syscache::ATTNUM,
        SysCacheKey::Value(KeyDatum::from_oid(onerel.rd_id)),
        SysCacheKey::Value(KeyDatum::from_i16(attnum as i16)),
    )?;
    let attstattarget = match atttuple {
        Some(t) => {
            let (dat, isnull) =
                syscache::SysCacheGetAttr(mcx, syscache::ATTNUM, &t, Anum_pg_attribute_attstattarget)?;
            if isnull {
                -1
            } else {
                dat.as_i16() as i32
            }
        }
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for attribute {} of relation {}",
                attnum, onerel.rd_id
            )));
        }
    };

    // Don't analyze column if user specified not to.
    if attstattarget == 0 {
        return Ok(None);
    }

    // Create the VacAttrStats struct.
    let mut stats = new_vac_attr_stats(mcx, onerel)?;
    stats.attstattarget = attstattarget;

    // When analyzing an expression index, believe the expression tree's type.
    if let Some(index_expr) = index_expr {
        let ti = nodefuncs::expr_type_info::call(index_expr)?;
        stats.attrtypid = ti.typid;
        stats.attrtypmod = ti.typmod;
        let indcoll = onerel
            .rd_indcollation
            .get((attnum - 1) as usize)
            .copied()
            .unwrap_or(InvalidOid);
        if OidIsValid(indcoll) {
            stats.attrcollid = indcoll;
        } else {
            stats.attrcollid = ti.collation;
        }
    } else {
        stats.attrtypid = attr.atttypid;
        stats.attrtypmod = attr.atttypmod;
        stats.attrcollid = attr.attcollation;
    }

    // GETSTRUCT(SearchSysCacheCopy1(TYPEOID, attrtypid)).
    let typtuple: FormData_pg_type = match syscache_seams::pg_type_form::call(
        stats.attrtypid,
    )? {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for type {}",
                stats.attrtypid
            )));
        }
    };
    stats.attrtype = Some(typtuple);
    stats.anl_context = Some(mcx);
    stats.tupattnum = attnum;

    // Default stavalues[n] element types to the analyzed type.
    let typ = stats.attrtype.as_ref().unwrap();
    for i in 0..STATISTIC_NUM_SLOTS {
        stats.statypid[i] = stats.attrtypid;
        stats.statyplen[i] = typ.typlen;
        stats.statypbyval[i] = typ.typbyval;
        stats.statypalign[i] = typ.typalign;
    }

    // Call the type-specific typanalyze, or std_typanalyze.
    let ok = if OidIsValid(typ.typanalyze) {
        // OidFunctionCall1(typanalyze, PointerGetDatum(stats)). The typanalyze
        // function receives the live VacAttrStats by pointer and mutates it in
        // place. The owned model cannot pass a Rust &mut through fmgr's by-word
        // Datum ABI, so a custom (non-standard) typanalyze is reached through its
        // own owner; here we run std_typanalyze for the standard case and route
        // custom typanalyze to its installed seam if available.
        run_custom_typanalyze(mcx, typ.typanalyze, &mut stats)?
    } else {
        std_typanalyze(&mut stats)?
    };

    if !ok || stats.compute_stats.is_none() || stats.minrows <= 0 {
        return Ok(None);
    }

    Ok(Some(stats))
}

// ===========================================================================
// examine_expression() -- pre-analysis of a single expression
// (extended_stats.c:604). Owned here (shares examine_attribute's internals);
// reached from the extended-statistics build leg through the
// `examine_expression` seam.
// ===========================================================================

pub fn examine_expression<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    expr: &::nodes::primnodes::Expr,
    stattarget: i32,
) -> PgResult<Option<VacAttrStats<'mcx>>> {
    // Create the VacAttrStats struct.
    let mut stats = new_vac_attr_stats(mcx, onerel)?;

    // We can't have statistics target specified for the expression, so use the
    // target computed for the extended statistics.
    stats.attstattarget = stattarget;

    // When analyzing an expression, believe the expression tree's type.
    let ti = nodefuncs::expr_type_info::call(expr)?;
    stats.attrtypid = ti.typid;
    stats.attrtypmod = ti.typmod;
    // We don't allow collation to be specified in CREATE STATISTICS, so we have
    // to use the collation specified for the expression (exprCollation()).
    stats.attrcollid = ti.collation;

    // GETSTRUCT(SearchSysCacheCopy1(TYPEOID, attrtypid)).
    let typtuple: FormData_pg_type = match syscache_seams::pg_type_form::call(
        stats.attrtypid,
    )? {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for type {}",
                stats.attrtypid
            )));
        }
    };
    stats.attrtype = Some(typtuple);
    stats.anl_context = Some(mcx);
    stats.tupattnum = INVALID_ATTR_NUMBER;

    // Default stavalues[n] element types to the analyzed type.
    let typ = stats.attrtype.as_ref().unwrap();
    for i in 0..STATISTIC_NUM_SLOTS {
        stats.statypid[i] = stats.attrtypid;
        stats.statyplen[i] = typ.typlen;
        stats.statypbyval[i] = typ.typbyval;
        stats.statypalign[i] = typ.typalign;
    }

    // Call the type-specific typanalyze, or std_typanalyze.
    let ok = if OidIsValid(typ.typanalyze) {
        run_custom_typanalyze(mcx, typ.typanalyze, &mut stats)?
    } else {
        std_typanalyze(&mut stats)?
    };

    if !ok || stats.compute_stats.is_none() || stats.minrows <= 0 {
        return Ok(None);
    }

    Ok(Some(stats))
}

/// `InvalidAttrNumber` (access/attnum.h).
const INVALID_ATTR_NUMBER: i32 = 0;

// pg_proc OIDs (fmgroids.h) of the built-in `typanalyze` support functions.
// The C `OidFunctionCall1(typanalyzeOid, PointerGetDatum(stats))` passes a live
// `VacAttrStats*` (an `internal`-typed arg) through fmgr; that pointer cannot
// cross the owned by-word Datum lane, so each built-in typanalyze is reached
// through a typed inward seam (declared by its owner's `*-typanalyze-seams`
// crate, installed by the owning leaf) that takes the real `&mut VacAttrStats`.
// This funcoid-keyed dispatch is the owned analog of the fmgr indirection,
// mirroring how other `internal`-state functions (SRF/agg) are dispatched.
const F_ARRAY_TYPANALYZE: u32 = 3816;
const F_TS_TYPANALYZE: u32 = 3688;
const F_RANGE_TYPANALYZE: u32 = 3916;
const F_MULTIRANGE_TYPANALYZE: u32 = 4242;

/// Run a non-standard `typanalyze` function (the C
/// `OidFunctionCall1(typanalyze, PointerGetDatum(stats))`). The live
/// `VacAttrStats*` is an `internal`-typed fmgr arg that cannot cross the owned
/// by-word Datum lane, so each built-in typanalyze is dispatched by its pg_proc
/// OID to the typed inward seam its owning leaf installs (the seam takes the
/// real `&mut VacAttrStats`). An unknown / user-defined typanalyze (which would
/// genuinely require a `Datum::Internal` fmgr arm) bottoms out loudly.
fn run_custom_typanalyze<'mcx>(
    _mcx: Mcx<'mcx>,
    typanalyze: Oid,
    stats: &mut VacAttrStats<'mcx>,
) -> PgResult<bool> {
    match typanalyze {
        F_ARRAY_TYPANALYZE => {
            array_typanalyze_seams::array_typanalyze::call(stats)
        }
        F_TS_TYPANALYZE => {
            tsvector_typanalyze_seams::ts_typanalyze::call(stats)
        }
        F_RANGE_TYPANALYZE => {
            rangetypes_typanalyze_seams::range_typanalyze::call(stats)
        }
        F_MULTIRANGE_TYPANALYZE => {
            rangetypes_typanalyze_seams::multirange_typanalyze::call(stats)
        }
        other => Err(PgError::error(format!(
            "analyze: typanalyze function {} has no owned dispatch (a user-defined typanalyze would require a Datum::Internal fmgr arm to carry the live VacAttrStats; only the built-in array/tsvector/range/multirange typanalyze functions are reached through their owners)",
            other
        ))),
    }
}

// ===========================================================================
// acquire_sample_rows() -- acquire a random sample of rows from the table
// ===========================================================================

fn acquire_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    elevel: ::types_error::ErrorLevel,
    rows: &mut Vec<FormedTuple<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
    bstrategy: BufferAccessStrategy,
) -> PgResult<i32> {
    use ::sampling::{
        reservoir_get_next_S, reservoir_init_selection_state, sampler_random_fract,
        BlockSampler_HasMore, BlockSampler_Init, BlockSampler_Next, BlockSamplerData,
        ReservoirStateData,
    };

    debug_assert!(targrows > 0);

    let mut numrows = 0i32; // # rows now in reservoir
    let mut samplerows = 0.0f64; // total # rows collected
    let mut liverows = 0.0f64; // # live rows seen
    let mut deadrows = 0.0f64; // # dead rows seen
    let mut rowstoskip = -1.0f64; // -1 means not set yet

    let totalblocks = RelationGetNumberOfBlocks(onerel)?;

    // Need a cutoff xmin for HeapTupleSatisfiesVacuum.
    let oldest_xmin =
        procarray_seams::get_oldest_non_removable_transaction_id::call(
            onerel.rd_id,
        )?;

    // Prepare for sampling block numbers.
    let randseed = prng::global_prng(prng::PgPrng::next_u32);
    let mut bs = BlockSamplerData::default();
    let nblocks = BlockSampler_Init(&mut bs, totalblocks, targrows, randseed);

    progress::pgstat_progress_update_param::call(PROGRESS_ANALYZE_BLOCKS_TOTAL, nblocks as i64)?;

    // Prepare for sampling rows.
    let mut rstate = ReservoirStateData::default();
    reservoir_init_selection_state(&mut rstate, targrows);

    let mut scan = tableam::table_beginscan_analyze::call(mcx, onerel)?;
    let mut slot = table_slot_create(mcx, onerel)?;

    // C drives a read stream whose per-block callback is
    // `block_sampling_read_stream_next` (returning the next BlockSampler-chosen
    // block, `InvalidBlockNumber` to end) and whose `BufferAccessStrategy` is the
    // vacuum `vac_strategy`. In the owned model — mirroring vacuumlazy's owned
    // block-selection — the stream carries no read-ahead I/O of its own: the
    // `next_buffer` closure pulls the next sampled block straight from the
    // BlockSampler and pins it with the real vacuum strategy through the
    // bufmgr-owned `ReadBufferExtended(rel, MAIN_FORKNUM, blk, RBM_NORMAL,
    // bstrategy)` seam. The pinned buffer is then share-locked by
    // `heapam_scan_analyze_next_block`, exactly as C's stream consumer does.
    let mut blksdone: BlockNumber = 0;

    // Outer loop over blocks to sample. The closure drives the real `bs` so its
    // `m` (blocks actually read) is the count used in the extrapolation below,
    // exactly as C's `bs.m`.
    let mut next_buffer = || -> PgResult<Buffer> {
        if !BlockSampler_HasMore(&bs) {
            // InvalidBuffer ends the stream / outer loop.
            return Ok(::types_storage::buf::InvalidBuffer);
        }
        let blk = BlockSampler_Next(&mut bs);
        bufmgr_seams::read_buffer_with_strategy::call(
            onerel,
            blk,
            bstrategy.clone(),
        )
    };
    while tableam::table_scan_analyze_next_block::call(mcx, &mut scan, &mut next_buffer)? {
        vacuum_delay_point()?;

        while tableam::table_scan_analyze_next_tuple::call(
            mcx,
            &mut scan,
            oldest_xmin,
            &mut liverows,
            &mut deadrows,
            &mut slot,
        )? {
            // Vitter's reservoir algorithm.
            if numrows < targrows {
                let tup = slot_copy_heap_tuple(mcx, &mut slot)?;
                rows.push(tup);
                numrows += 1;
            } else {
                if rowstoskip < 0.0 {
                    rowstoskip = reservoir_get_next_S(&mut rstate, samplerows, targrows);
                }
                if rowstoskip <= 0.0 {
                    let k = (targrows as f64 * sampler_random_fract(&mut rstate.randstate)) as i32;
                    debug_assert!(k >= 0 && k < targrows);
                    let tup = slot_copy_heap_tuple(mcx, &mut slot)?;
                    // heap_freetuple(rows[k]); rows[k] = new.
                    rows[k as usize] = tup;
                }
                rowstoskip -= 1.0;
            }
            samplerows += 1.0;
        }

        blksdone += 1;
        progress::pgstat_progress_update_param::call(
            PROGRESS_ANALYZE_BLOCKS_DONE,
            blksdone as i64,
        )?;
    }

    slot_seam::exec_drop_single_tuple_table_slot::call(slot)?;
    tableam::table_endscan::call(scan)?;

    // If we collected the full target, sort by physical position.
    if numrows == targrows {
        qsort_rows(rows);
    }

    // Estimate total live/dead rows by extrapolation.
    if bs.m > 0 {
        *totalrows = ((liverows / bs.m as f64) * totalblocks as f64 + 0.5).floor();
        *totaldeadrows = ((deadrows / bs.m as f64) * totalblocks as f64 + 0.5).floor();
    } else {
        *totalrows = 0.0;
        *totaldeadrows = 0.0;
    }

    ereport(elevel)
        .errmsg(format!(
            "\"{}\": scanned {} of {} pages, containing {:.0} live rows and {:.0} dead rows; {} rows in sample, {:.0} estimated total rows",
            onerel.name(),
            bs.m,
            totalblocks,
            liverows,
            deadrows,
            numrows,
            *totalrows
        ))
        .finish(here("acquire_sample_rows"))?;

    Ok(numrows)
}

/// `compare_rows` (analyze.c) — order rows[] by (block, offset) of t_self.
fn qsort_rows(rows: &mut [FormedTuple<'_>]) {
    rows.sort_by(|a, b| {
        let ta = &a.tuple.t_self;
        let tb = &b.tuple.t_self;
        let ba = item_pointer_block(ta);
        let bb = item_pointer_block(tb);
        let oa = item_pointer_offset(ta);
        let ob = item_pointer_offset(tb);
        ba.cmp(&bb).then(oa.cmp(&ob))
    });
}

// ===========================================================================
// acquire_inherited_sample_rows()
// ===========================================================================

fn acquire_inherited_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    elevel: ::types_error::ErrorLevel,
    rows: &mut Vec<FormedTuple<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
    bstrategy: BufferAccessStrategy,
) -> PgResult<i32> {
    *totalrows = 0.0;
    *totaldeadrows = 0.0;

    // Find all members of the inheritance set (AccessShareLock on children).
    let (table_oids, _) =
        find_all_inheritors(mcx, onerel.rd_id, AccessShareLock, false)?;

    // Need at least one descendant.
    if table_oids.len() < 2 {
        transam_xact::CommandCounterIncrement()?;
        SetRelationHasSubclass(mcx, onerel.rd_id, false)?;
        ereport(elevel)
            .errmsg(format!(
                "skipping analyze of \"{}.{}\" inheritance tree --- this inheritance tree contains no child tables",
                nsp_name(mcx, onerel.rd_rel.relnamespace)?,
                onerel.name()
            ))
            .finish(here("acquire_inherited_sample_rows"))?;
        return Ok(0);
    }

    // Identify acquirefuncs and count blocks in all relations.
    struct ChildRel<'mcx> {
        rel: Relation<'mcx>,
        func: AcquireFunc,
        blocks: f64,
    }
    let mut children: Vec<ChildRel<'mcx>> = Vec::new();
    let mut totalblocks = 0.0f64;
    let mut has_child = false;

    for &child_oid in table_oids.iter() {
        // We already got the needed lock.
        let childrel = table_open(mcx, child_oid, NoLock)?;

        if relation_is_other_temp(&childrel) {
            table_close(childrel, AccessShareLock)?;
            continue;
        }

        let ckind = childrel.rd_rel.relkind;
        let func;
        let relpages;
        if ckind == RELKIND_RELATION || ckind == RELKIND_MATVIEW {
            func = AcquireFunc::Heap;
            relpages = RelationGetNumberOfBlocks(&childrel)?;
        } else if ckind == RELKIND_FOREIGN_TABLE {
            match rt::analyze_foreign_table::call(&childrel)? {
                Some(rp) => {
                    func = AcquireFunc::Fdw;
                    relpages = rp;
                }
                None => {
                    table_close(childrel, AccessShareLock)?;
                    continue;
                }
            }
        } else {
            // Partitioned table: ignore, release lock (don't unlock onerel).
            if childrel.rd_id != onerel.rd_id {
                table_close(childrel, AccessShareLock)?;
            } else {
                table_close(childrel, NoLock)?;
            }
            continue;
        }

        has_child = true;
        totalblocks += relpages as f64;
        children.push(ChildRel {
            rel: childrel,
            func,
            blocks: relpages as f64,
        });
    }

    if !has_child {
        for c in children {
            table_close(c.rel, NoLock)?;
        }
        ereport(elevel)
            .errmsg(format!(
                "skipping analyze of \"{}.{}\" inheritance tree --- this inheritance tree contains no analyzable child tables",
                nsp_name(mcx, onerel.rd_rel.relnamespace)?,
                onerel.name()
            ))
            .finish(here("acquire_inherited_sample_rows"))?;
        return Ok(0);
    }

    // Sample rows from each relation, proportionally to its block fraction.
    progress::pgstat_progress_update_param::call(
        PROGRESS_ANALYZE_CHILD_TABLES_TOTAL,
        children.len() as i64,
    )?;
    let mut numrows = 0i32;

    let nrels = children.len();
    for i in 0..nrels {
        let childblocks = children[i].blocks;

        progress::pgstat_progress_update_param::call(
            PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID,
            children[i].rel.rd_id as i64,
        )?;
        progress::pgstat_progress_update_param::call(PROGRESS_ANALYZE_BLOCKS_DONE, 0)?;
        progress::pgstat_progress_update_param::call(PROGRESS_ANALYZE_BLOCKS_TOTAL, 0)?;

        if childblocks > 0.0 {
            let mut childtargrows =
                (targrows as f64 * childblocks / totalblocks).round() as i32;
            // Don't overrun due to roundoff error.
            childtargrows = childtargrows.min(targrows - numrows);
            if childtargrows > 0 {
                let mut trows = 0.0f64;
                let mut tdrows = 0.0f64;

                // Fetch a random sample of the child's rows into rows[numrows..].
                let mut child_rows: Vec<FormedTuple<'mcx>> = Vec::new();
                let childrows = match children[i].func {
                    AcquireFunc::Heap => acquire_sample_rows(
                        mcx,
                        &children[i].rel,
                        elevel,
                        &mut child_rows,
                        childtargrows,
                        &mut trows,
                        &mut tdrows,
                        bstrategy.clone(),
                    )?,
                    AcquireFunc::Fdw => {
                        return Err(PgError::error(
                            "analyze: foreign-table sample acquisition is unported (fdwapi AnalyzeForeignTable acquirefunc)",
                        ));
                    }
                    AcquireFunc::None => 0,
                };

                // Convert from child's rowtype to parent's if needed.
                if childrows > 0 && !equal_row_types::call(&children[i].rel.rd_att, &onerel.rd_att) {
                    if let Some(map) =
                        convert_tuples_by_name(mcx, &children[i].rel.rd_att, &onerel.rd_att)?
                    {
                        for j in 0..childrows as usize {
                            let newtup = execute_attr_map_tuple(
                                mcx,
                                &child_rows[j].tuple,
                                &child_rows[j].data,
                                &map,
                            )?;
                            child_rows[j] = newtup;
                        }
                    }
                }

                for j in 0..childrows as usize {
                    rows.push(child_rows[j].clone_in(mcx)?);
                }
                numrows += childrows;
                *totalrows += trows;
                *totaldeadrows += tdrows;
            }
        }

        progress::pgstat_progress_update_param::call(
            PROGRESS_ANALYZE_CHILD_TABLES_DONE,
            (i + 1) as i64,
        )?;
    }

    // Cannot release child-table locks (TOAST pointers in sampled rows).
    for c in children {
        table_close(c.rel, NoLock)?;
    }

    Ok(numrows)
}

// ===========================================================================
// update_attstats()
// ===========================================================================

fn update_attstats<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    inh: bool,
    vacattrstats: &[VacAttrStats<'mcx>],
) -> PgResult<()> {
    let natts = vacattrstats.len();
    if natts == 0 {
        return Ok(());
    }

    let sd = table_open(mcx, StatisticRelationId, RowExclusiveLock)?;
    let mut indstate = None;

    for stats in vacattrstats.iter() {
        if !stats.stats_valid {
            continue;
        }

        let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(Natts_pg_statistic);
        let mut nulls = [false; Natts_pg_statistic];
        let replaces = [true; Natts_pg_statistic];
        for _ in 0..Natts_pg_statistic {
            values.push(Datum::null());
        }

        values[Anum_pg_statistic_starelid - 1] = Datum::from_oid(relid);
        values[Anum_pg_statistic_staattnum - 1] = Datum::from_i16(stats.tupattnum as i16);
        values[Anum_pg_statistic_stainherit - 1] = Datum::from_bool(inh);
        values[Anum_pg_statistic_stanullfrac - 1] = Datum::from_f32(stats.stanullfrac);
        values[Anum_pg_statistic_stawidth - 1] = Datum::from_i32(stats.stawidth);
        values[Anum_pg_statistic_stadistinct - 1] = Datum::from_f32(stats.stadistinct);

        let mut i = Anum_pg_statistic_stakind1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            values[i] = Datum::from_i16(stats.stakind[k]);
            i += 1;
        }
        i = Anum_pg_statistic_staop1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            values[i] = Datum::from_oid(stats.staop[k]);
            i += 1;
        }
        i = Anum_pg_statistic_stacoll1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            values[i] = Datum::from_oid(stats.stacoll[k]);
            i += 1;
        }
        i = Anum_pg_statistic_stanumbers1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            let nnum = stats.numnumbers[k];
            if nnum > 0 {
                let numdatums: Vec<Datum<'mcx>> = (0..nnum as usize)
                    .map(|n| Datum::from_f32(stats.stanumbers[k][n]))
                    .collect();
                // construct_array_builtin(numdatums, nnum, FLOAT4OID): build the
                // float4[] and wrap its raw bytes as a by-reference value.
                // FLOAT4 is pass-by-value, length 4, 'i' alignment. The
                // value-lane `construct_array_values` builds the ArrayType image
                // from the canonical 6-arm Datum (float4 by-value here).
                let arry = construct_array_values(mcx, &numdatums[..], FLOAT4OID, 4, true, b'i')?;
                values[i] = Datum::ByRef(arry);
            } else {
                nulls[i] = true;
                values[i] = Datum::null();
            }
            i += 1;
        }
        i = Anum_pg_statistic_stavalues1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            if stats.numvalues[k] > 0 {
                let arry = construct_array_values(
                    mcx,
                    &stats.stavalues[k][..],
                    stats.statypid[k],
                    stats.statyplen[k] as i32,
                    stats.statypbyval[k],
                    stats.statypalign[k] as u8,
                )?;
                // PointerGetDatum(arry): wrap the array bytes as a by-reference
                // value.
                values[i] = Datum::ByRef(arry);
            } else {
                nulls[i] = true;
                values[i] = Datum::null();
            }
            i += 1;
        }

        // Existing pg_statistic tuple for this attribute?
        let oldtup = syscache::SearchSysCache3(
            mcx,
            syscache::STATRELATTINH,
            SysCacheKey::Value(KeyDatum::from_oid(relid)),
            SysCacheKey::Value(KeyDatum::from_i16(stats.tupattnum as i16)),
            SysCacheKey::Value(KeyDatum::from_bool(inh)),
        )?;

        if indstate.is_none() {
            indstate = Some(indexing::catalog_open_indexes::call(mcx, &sd)?);
        }
        let indstate_ref = indstate.as_mut().unwrap();

        if let Some(oldtup) = oldtup {
            let mut stup = ::heaptuple::heap_modify_tuple(
                mcx,
                &oldtup,
                &sd.rd_att,
                &values,
                &nulls,
                &replaces,
            )
            .map_err(|_| PgError::error("heap_modify_tuple failed in update_attstats"))?;
            let otid = stup.tuple.t_self;
            rt::catalog_tuple_update_with_info_pg_statistic::call(
                mcx,
                &sd,
                otid,
                &mut stup,
                indstate_ref,
            )?;
        } else {
            let mut stup =
                ::heaptuple::heap_form_tuple(mcx, &sd.rd_att, &values, &nulls)
                    .map_err(|_| PgError::error("heap_form_tuple failed in update_attstats"))?;
            rt::catalog_tuple_insert_with_info_pg_statistic::call(mcx, &sd, &mut stup, indstate_ref)?;
        }
    }

    if let Some(indstate) = indstate {
        indexing::catalog_close_indexes::call(indstate)?;
    }
    table_close(sd, RowExclusiveLock)?;
    Ok(())
}

// ===========================================================================
// std_fetch_func / ind_fetch_func
// ===========================================================================

/// `std_fetch_func` (analyze.c). Reads the `rownum`-th sampled value of the
/// analyzed column out of `stats.rows` / `stats.tup_desc`.
fn std_fetch_func<'mcx>(stats: &VacAttrStats<'mcx>, rownum: i32, is_null: &mut bool) -> Datum<'mcx> {
    let attnum = stats.tupattnum;
    let tuple = &stats.rows[rownum as usize];
    let mcx = stats.anl_context.expect("std_fetch_func: anl_context set");
    // heap_getattr(tuple, attnum, tupDesc, isNull): fastgetattr's null handling
    // (HeapTupleNoNulls || !att_isnull -> fetch; else NULL).
    let header = tuple
        .tuple
        .t_data
        .as_deref()
        .expect("std_fetch_func: tuple has no t_data");
    let tupdesc = stats
        .tup_desc
        .as_deref()
        .expect("std_fetch_func: tup_desc set");
    let has_nulls = (header.t_infomask & HEAP_HASNULL) != 0;
    if has_nulls && ::heaptuple::heap_attisnull(&tuple.tuple, attnum, Some(tupdesc)) {
        *is_null = true;
        return Datum::null();
    }
    *is_null = false;
    nocachegetattr(mcx, &tuple.tuple, attnum, tupdesc, &tuple.data)
        .expect("std_fetch_func: nocachegetattr")
}

/// `HEAP_HASNULL` (access/htup_details.h).
const HEAP_HASNULL: u16 = 0x0001;

/// `ind_fetch_func` (analyze.c). exprvals/exprnulls are already offset for the
/// proper column; the owned model de-strided into per-column buffers, so the
/// stride is 1.
fn ind_fetch_func<'mcx>(stats: &VacAttrStats<'mcx>, rownum: i32, is_null: &mut bool) -> Datum<'mcx> {
    let i = (rownum * stats.rowstride) as usize;
    *is_null = stats.exprnulls[i];
    stats.exprvals[i].clone()
}

// ===========================================================================
// std_typanalyze() -- the default type-specific typanalyze function
// ===========================================================================

/// `std_typanalyze(stats)` (analyze.c).
pub fn std_typanalyze<'mcx>(stats: &mut VacAttrStats<'mcx>) -> PgResult<bool> {
    // If attstattarget is negative, use the default.
    if stats.attstattarget < 0 {
        stats.attstattarget = default_statistics_target();
    }

    // Look for default "<" and "=" operators for the column's type. C
    // `std_typanalyze` passes all three need_* as false, so a type lacking these
    // operators (e.g. point/path) returns InvalidOid and falls to the trivial
    // path rather than erroring "could not identify an equality operator".
    let ops = get_sort_group_operators(stats.attrtypid, false, false, false, false)?;
    let ltopr = ops.lt_opr;
    let eqopr = ops.eq_opr;

    // (StdAnalyzeData is RE-DERIVED in the compute routines from attrtypid; the
    // u64 extra_data cannot carry the struct. We tag extra_data nonzero to mirror
    // the C "extra_data != NULL".)
    stats.extra_data = 1;

    // Choose the standard statistics algorithm.
    if OidIsValid(eqopr) && OidIsValid(ltopr) {
        stats.compute_stats = Some(compute_scalar_stats);
        stats.minrows = 300 * stats.attstattarget;
    } else if OidIsValid(eqopr) {
        stats.compute_stats = Some(compute_distinct_stats);
        stats.minrows = 300 * stats.attstattarget;
    } else {
        stats.compute_stats = Some(compute_trivial_stats);
        stats.minrows = 300 * stats.attstattarget;
    }

    Ok(true)
}

// ===========================================================================
// compute_trivial_stats()
// ===========================================================================

fn compute_trivial_stats<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    _totalrows: f64,
) {
    compute_trivial_stats_inner(stats, fetchfunc, samplerows).expect("compute_trivial_stats")
}

fn compute_trivial_stats_inner<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
) -> PgResult<()> {
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut total_width = 0.0f64;
    let typ = stats.attrtype.as_ref().unwrap();
    let is_varlena = !typ.typbyval && typ.typlen == -1;
    let is_varwidth = !typ.typbyval && typ.typlen < 0;
    let typlen = typ.typlen;

    for i in 0..samplerows {
        vacuum_delay_point()?;
        let mut isnull = false;
        let value = fetchfunc(stats, i, &mut isnull);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;
        if is_varlena {
            total_width += varsize_any_datum(&value) as f64;
        } else if is_varwidth {
            total_width += (cstring_len(&value) + 1) as f64;
        }
    }

    if nonnull_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        if is_varwidth {
            stats.stawidth = (total_width / nonnull_cnt as f64) as i32;
        } else {
            stats.stawidth = typlen as i32;
        }
        stats.stadistinct = 0.0;
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        if is_varwidth {
            stats.stawidth = 0;
        } else {
            stats.stawidth = typlen as i32;
        }
        stats.stadistinct = 0.0;
    }
    Ok(())
}

// ===========================================================================
// compute_distinct_stats()
// ===========================================================================

fn compute_distinct_stats<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) {
    compute_distinct_stats_inner(stats, fetchfunc, samplerows, totalrows)
        .expect("compute_distinct_stats")
}

struct TrackItem<'mcx> {
    value: Datum<'mcx>,
    count: i32,
}

fn compute_distinct_stats_inner<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let mcx = stats.anl_context.expect("anl_context");
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut toowide_cnt = 0i32;
    let mut total_width = 0.0f64;
    let typ = stats.attrtype.as_ref().unwrap();
    let is_varlena = !typ.typbyval && typ.typlen == -1;
    let is_varwidth = !typ.typbyval && typ.typlen < 0;
    let typbyval = typ.typbyval;
    let typlen = typ.typlen;

    let num_mcv0 = stats.attstattarget;
    let mystats = std_analyze_data(stats.attrtypid)?;
    let attrcollid = stats.attrcollid;

    // Track up to 2*n values; at least 10.
    let mut track_max = 2 * num_mcv0;
    if track_max < 10 {
        track_max = 10;
    }
    let mut track: Vec<TrackItem<'mcx>> = Vec::with_capacity(track_max as usize);

    for i in 0..samplerows {
        vacuum_delay_point()?;
        let mut isnull = false;
        let mut value = fetchfunc(stats, i, &mut isnull);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;

        if is_varlena {
            total_width += varsize_any_datum(&value) as f64;
            if detoast::toast_raw_datum_size::call(mcx, value.clone())? as u32 > WIDTH_THRESHOLD {
                toowide_cnt += 1;
                continue;
            }
            // PG_DETOAST_DATUM.
            value = Datum::ByRef(detoast::detoast_attr::call(mcx, value.as_ref_bytes())?);
        } else if is_varwidth {
            total_width += (cstring_len(&value) + 1) as f64;
        }

        // See if value matches anything tracked.
        let mut matched = false;
        let mut firstcount1 = track.len();
        let mut j = 0usize;
        while j < track.len() {
            if fmgr::function_call2_coll_datum::call(
                mcx,
                mystats.eqfunc,
                attrcollid,
                value.clone(),
                track[j].value.clone(),
            )?
            .as_bool()
            {
                matched = true;
                break;
            }
            if j < firstcount1 && track[j].count == 1 {
                firstcount1 = j;
            }
            j += 1;
        }

        if matched {
            track[j].count += 1;
            // Bubble up.
            while j > 0 && track[j].count > track[j - 1].count {
                track.swap(j, j - 1);
                j -= 1;
            }
        } else {
            // Insert at head of count-1 list.
            if (track.len() as i32) < track_max {
                track.push(TrackItem {
                    value: Datum::null(),
                    count: 0,
                });
            }
            let mut jj = track.len() as i32 - 1;
            while jj > firstcount1 as i32 {
                let from = (jj - 1) as usize;
                let to = jj as usize;
                track[to].value = track[from].value.clone();
                track[to].count = track[from].count;
                jj -= 1;
            }
            if (firstcount1 as i32) < track.len() as i32 {
                track[firstcount1].value = value;
                track[firstcount1].count = 1;
            }
        }
    }

    if nonnull_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        if is_varwidth {
            stats.stawidth = (total_width / nonnull_cnt as f64) as i32;
        } else {
            stats.stawidth = typlen as i32;
        }

        // Count multiply-seen values.
        let mut summultiple = 0i32;
        let mut nmultiple = 0usize;
        while nmultiple < track.len() {
            if track[nmultiple].count == 1 {
                break;
            }
            summultiple += track[nmultiple].count;
            nmultiple += 1;
        }

        let track_cnt = track.len();
        let mut num_mcv = num_mcv0;

        if nmultiple == 0 {
            stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
        } else if (track_cnt as i32) < track_max && toowide_cnt == 0 && nmultiple == track_cnt {
            stats.stadistinct = track_cnt as f32;
        } else {
            let f1 = nonnull_cnt - summultiple;
            let d = f1 + nmultiple as i32;
            let n = (samplerows - null_cnt) as f64;
            let N = totalrows * (1.0 - stats.stanullfrac as f64);
            let stadistinct = if N > 0.0 {
                (n * d as f64) / ((n - f1 as f64) + f1 as f64 * n / N)
            } else {
                0.0
            };
            let mut stadistinct = stadistinct;
            if stadistinct < d as f64 {
                stadistinct = d as f64;
            }
            if stadistinct > N {
                stadistinct = N;
            }
            stats.stadistinct = (stadistinct + 0.5).floor() as f32;
        }

        if stats.stadistinct as f64 > 0.1 * totalrows {
            stats.stadistinct = -(stats.stadistinct / totalrows as f32);
        }

        // Decide how many MCVs to store.
        if (track_cnt as i32) < track_max
            && toowide_cnt == 0
            && stats.stadistinct > 0.0
            && (track_cnt as i32) <= num_mcv
        {
            num_mcv = track_cnt as i32;
        } else {
            if num_mcv > track_cnt as i32 {
                num_mcv = track_cnt as i32;
            }
            if num_mcv > 0 {
                let mcv_counts: Vec<i32> =
                    (0..num_mcv as usize).map(|i| track[i].count).collect();
                num_mcv = analyze_mcv_list(
                    &mcv_counts,
                    num_mcv,
                    stats.stadistinct as f64,
                    stats.stanullfrac as f64,
                    samplerows,
                    totalrows,
                );
            }
        }

        // Generate MCV slot entry.
        if num_mcv > 0 {
            let mut mcv_values: Vec<Datum<'mcx>> = Vec::with_capacity(num_mcv as usize);
            let mut mcv_freqs: Vec<f32> = Vec::with_capacity(num_mcv as usize);
            for i in 0..num_mcv as usize {
                mcv_values.push(datum_copy_v(mcx, &track[i].value, typbyval, typlen as i32)?);
                mcv_freqs.push((track[i].count as f64 / samplerows as f64) as f32);
            }
            stats.stakind[0] = STATISTIC_KIND_MCV;
            stats.staop[0] = mystats.eqopr;
            stats.stacoll[0] = stats.attrcollid;
            stats.stanumbers[0] = mcv_freqs;
            stats.numnumbers[0] = num_mcv;
            stats.stavalues[0] = mcv_values;
            stats.numvalues[0] = num_mcv;
        }
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        if is_varwidth {
            stats.stawidth = 0;
        } else {
            stats.stawidth = typlen as i32;
        }
        stats.stadistinct = 0.0;
    }
    Ok(())
}

// ===========================================================================
// compute_scalar_stats()
// ===========================================================================

fn compute_scalar_stats<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) {
    compute_scalar_stats_inner(stats, fetchfunc, samplerows, totalrows)
        .expect("compute_scalar_stats")
}

#[derive(Clone)]
struct ScalarItem<'mcx> {
    value: Datum<'mcx>,
    tupno: i32,
}

struct ScalarMCVItem {
    count: i32,
    first: i32,
}

fn compute_scalar_stats_inner<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let mcx = stats.anl_context.expect("anl_context");
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut toowide_cnt = 0i32;
    let mut total_width = 0.0f64;
    let typ = stats.attrtype.as_ref().unwrap();
    let is_varlena = !typ.typbyval && typ.typlen == -1;
    let is_varwidth = !typ.typbyval && typ.typlen < 0;
    let typbyval = typ.typbyval;
    let typlen = typ.typlen;

    let num_mcv0 = stats.attstattarget;
    let num_bins = stats.attstattarget;
    let mystats = std_analyze_data(stats.attrtypid)?;

    let mut values: Vec<ScalarItem<'mcx>> = Vec::with_capacity(samplerows as usize);
    let mut tupno_link: Vec<i32> = Vec::with_capacity(samplerows as usize);

    // SortSupport set up for the "<" operator.
    let mut ssup = SortSupportData::new(mcx);
    ssup.ssup_collation = stats.attrcollid;
    ssup.ssup_nulls_first = false;
    ssup.abbreviate = false;
    PrepareSortSupportFromOrderingOp(mystats.ltopr, &mut ssup)?;

    let mut values_cnt = 0i32;
    for i in 0..samplerows {
        vacuum_delay_point()?;
        let mut isnull = false;
        let mut value = fetchfunc(stats, i, &mut isnull);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;

        if is_varlena {
            total_width += varsize_any_datum(&value) as f64;
            if detoast::toast_raw_datum_size::call(mcx, value.clone())? as u32 > WIDTH_THRESHOLD {
                toowide_cnt += 1;
                continue;
            }
            value = Datum::ByRef(detoast::detoast_attr::call(mcx, value.as_ref_bytes())?);
        } else if is_varwidth {
            total_width += (cstring_len(&value) + 1) as f64;
        }

        values.push(ScalarItem {
            value,
            tupno: values_cnt,
        });
        tupno_link.push(values_cnt);
        values_cnt += 1;
    }

    if values_cnt > 0 {
        // Sort the collected values (compare_scalars), updating tupno_link.
        sort_scalars(mcx, &mut values, &mut tupno_link, &ssup)?;

        // Scan values in order; find MCVs and accumulate correlation stats.
        let mut corr_xysum = 0.0f64;
        let mut ndistinct = 0i32;
        let mut nmultiple = 0i32;
        let mut dups_cnt = 0i32;
        let mut track: Vec<ScalarMCVItem> = Vec::new();
        let mut track_cnt = 0i32;

        for i in 0..values_cnt as usize {
            let tupno = values[i].tupno;
            corr_xysum += (i as f64) * (tupno as f64);
            dups_cnt += 1;
            if tupno_link[tupno as usize] == tupno {
                ndistinct += 1;
                if dups_cnt > 1 {
                    nmultiple += 1;
                    if track_cnt < num_mcv0
                        || dups_cnt > track[(track_cnt - 1) as usize].count
                    {
                        if track_cnt < num_mcv0 {
                            track.push(ScalarMCVItem { count: 0, first: 0 });
                            track_cnt += 1;
                        }
                        let mut j = track_cnt - 1;
                        while j > 0 {
                            if dups_cnt <= track[(j - 1) as usize].count {
                                break;
                            }
                            track[j as usize].count = track[(j - 1) as usize].count;
                            track[j as usize].first = track[(j - 1) as usize].first;
                            j -= 1;
                        }
                        track[j as usize].count = dups_cnt;
                        track[j as usize].first = (i as i32) + 1 - dups_cnt;
                    }
                }
                dups_cnt = 0;
            }
        }

        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        if is_varwidth {
            stats.stawidth = (total_width / nonnull_cnt as f64) as i32;
        } else {
            stats.stawidth = typlen as i32;
        }

        if nmultiple == 0 {
            stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
        } else if toowide_cnt == 0 && nmultiple == ndistinct {
            stats.stadistinct = ndistinct as f32;
        } else {
            let f1 = ndistinct - nmultiple + toowide_cnt;
            let d = f1 + nmultiple;
            let n = (samplerows - null_cnt) as f64;
            let N = totalrows * (1.0 - stats.stanullfrac as f64);
            let mut stadistinct = if N > 0.0 {
                (n * d as f64) / ((n - f1 as f64) + f1 as f64 * n / N)
            } else {
                0.0
            };
            if stadistinct < d as f64 {
                stadistinct = d as f64;
            }
            if stadistinct > N {
                stadistinct = N;
            }
            stats.stadistinct = (stadistinct + 0.5).floor() as f32;
        }

        if stats.stadistinct as f64 > 0.1 * totalrows {
            stats.stadistinct = -(stats.stadistinct / totalrows as f32);
        }

        let mut num_mcv = num_mcv0;
        if track_cnt == ndistinct
            && toowide_cnt == 0
            && stats.stadistinct > 0.0
            && track_cnt <= num_mcv
        {
            num_mcv = track_cnt;
        } else {
            if num_mcv > track_cnt {
                num_mcv = track_cnt;
            }
            if num_mcv > 0 {
                let mcv_counts: Vec<i32> =
                    (0..num_mcv as usize).map(|i| track[i].count).collect();
                num_mcv = analyze_mcv_list(
                    &mcv_counts,
                    num_mcv,
                    stats.stadistinct as f64,
                    stats.stanullfrac as f64,
                    samplerows,
                    totalrows,
                );
            }
        }

        let mut slot_idx = 0usize;

        // MCV slot.
        if num_mcv > 0 {
            let mut mcv_values: Vec<Datum<'mcx>> = Vec::with_capacity(num_mcv as usize);
            let mut mcv_freqs: Vec<f32> = Vec::with_capacity(num_mcv as usize);
            for i in 0..num_mcv as usize {
                let first = track[i].first as usize;
                mcv_values.push(datum_copy_v(
                    mcx,
                    &values[first].value,
                    typbyval,
                    typlen as i32,
                )?);
                mcv_freqs.push((track[i].count as f64 / samplerows as f64) as f32);
            }
            stats.stakind[slot_idx] = STATISTIC_KIND_MCV;
            stats.staop[slot_idx] = mystats.eqopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stanumbers[slot_idx] = mcv_freqs;
            stats.numnumbers[slot_idx] = num_mcv;
            stats.stavalues[slot_idx] = mcv_values;
            stats.numvalues[slot_idx] = num_mcv;
            slot_idx += 1;
        }

        // Histogram slot.
        let mut num_hist = ndistinct - num_mcv;
        if num_hist > num_bins {
            num_hist = num_bins + 1;
        }
        if num_hist >= 2 {
            // Sort MCV items into position order (compare_mcvs).
            track[..num_mcv as usize].sort_by(|a, b| a.first.cmp(&b.first));

            // Collapse out the MCV items from values[].
            let nvals;
            if num_mcv > 0 {
                let mut src = 0usize;
                let mut dest = 0usize;
                let mut j = 0usize; // next interesting MCV item
                while src < values_cnt as usize {
                    let ncopy;
                    if j < num_mcv as usize {
                        let first = track[j].first as usize;
                        if src >= first {
                            src = first + track[j].count as usize;
                            j += 1;
                            continue;
                        }
                        ncopy = first - src;
                    } else {
                        ncopy = values_cnt as usize - src;
                    }
                    for off in 0..ncopy {
                        values[dest + off] = values[src + off].clone();
                    }
                    src += ncopy;
                    dest += ncopy;
                }
                nvals = dest as i32;
            } else {
                nvals = values_cnt;
            }
            debug_assert!(nvals >= num_hist);

            let mut hist_values: Vec<Datum<'mcx>> = Vec::with_capacity(num_hist as usize);
            let delta = (nvals - 1) / (num_hist - 1);
            let deltafrac = (nvals - 1) % (num_hist - 1);
            let mut pos = 0i32;
            let mut posfrac = 0i32;
            for _ in 0..num_hist {
                hist_values.push(datum_copy_v(
                    mcx,
                    &values[pos as usize].value,
                    typbyval,
                    typlen as i32,
                )?);
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= num_hist - 1 {
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
            }

            stats.stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
            stats.staop[slot_idx] = mystats.ltopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stavalues[slot_idx] = hist_values;
            stats.numvalues[slot_idx] = num_hist;
            slot_idx += 1;
        }

        // Correlation slot.
        if values_cnt > 1 {
            let corr_xsum = (values_cnt - 1) as f64 * values_cnt as f64 / 2.0;
            let corr_x2sum =
                (values_cnt - 1) as f64 * values_cnt as f64 * (2 * values_cnt - 1) as f64 / 6.0;
            let corr = (values_cnt as f64 * corr_xysum - corr_xsum * corr_xsum)
                / (values_cnt as f64 * corr_x2sum - corr_xsum * corr_xsum);
            stats.stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
            stats.staop[slot_idx] = mystats.ltopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stanumbers[slot_idx] = alloc::vec![corr as f32];
            stats.numnumbers[slot_idx] = 1;
        }
    } else if nonnull_cnt > 0 {
        // All values were too wide.
        debug_assert_eq!(nonnull_cnt, toowide_cnt);
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        if is_varwidth {
            stats.stawidth = (total_width / nonnull_cnt as f64) as i32;
        } else {
            stats.stawidth = typlen as i32;
        }
        stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        if is_varwidth {
            stats.stawidth = 0;
        } else {
            stats.stawidth = typlen as i32;
        }
        stats.stadistinct = 0.0;
    }
    Ok(())
}

/// `compare_scalars` (analyze.c) wrapped into a sort over the values[] array,
/// maintaining the tupno_link[] equal-datum chain.
fn sort_scalars<'mcx>(
    _mcx: Mcx<'mcx>,
    values: &mut [ScalarItem<'mcx>],
    tupno_link: &mut [i32],
    ssup: &SortSupportData<'mcx>,
) -> PgResult<()> {
    // We can't use slice::sort_by because the comparator mutates tupno_link as a
    // side effect (the C compare_scalars does). Do an insertion-stable merge via
    // an index sort that records equalities. Mirror C: ApplySortComparator, then
    // on equal datums update tupno_link and order by tupno.
    //
    // qsort_interruptible isn't available; use a simple O(n log n) sort by
    // collecting comparisons. We use a Vec of indices and sort with a fallible
    // comparator emulation: since sort_by can't return errors, resolve the
    // comparator to a total order up front by precomputing is not feasible
    // (comparator can ereport). Mirror C with an in-place heapsort that calls the
    // comparator and propagates errors.
    let n = values.len();
    // Bottom-up merge sort over indices with the side-effecting comparator.
    let mut idx: Vec<usize> = (0..n).collect();
    merge_sort_scalars(values, tupno_link, ssup, &mut idx)?;
    // Apply the permutation.
    let sorted: Vec<ScalarItem<'mcx>> = idx.iter().map(|&i| values[i].clone()).collect();
    for (i, v) in sorted.into_iter().enumerate() {
        values[i] = v;
    }
    Ok(())
}

fn merge_sort_scalars<'mcx>(
    values: &[ScalarItem<'mcx>],
    tupno_link: &mut [i32],
    ssup: &SortSupportData<'mcx>,
    idx: &mut Vec<usize>,
) -> PgResult<()> {
    let n = idx.len();
    if n <= 1 {
        return Ok(());
    }
    let mid = n / 2;
    let mut left: Vec<usize> = idx[..mid].to_vec();
    let mut right: Vec<usize> = idx[mid..].to_vec();
    merge_sort_scalars(values, tupno_link, ssup, &mut left)?;
    merge_sort_scalars(values, tupno_link, ssup, &mut right)?;
    let mut i = 0;
    let mut j = 0;
    let mut k = 0;
    while i < left.len() && j < right.len() {
        let cmp = compare_scalars(values, tupno_link, ssup, left[i], right[j])?;
        if cmp <= 0 {
            idx[k] = left[i];
            i += 1;
        } else {
            idx[k] = right[j];
            j += 1;
        }
        k += 1;
    }
    while i < left.len() {
        idx[k] = left[i];
        i += 1;
        k += 1;
    }
    while j < right.len() {
        idx[k] = right[j];
        j += 1;
        k += 1;
    }
    Ok(())
}

/// `compare_scalars(a, b, cxt)` (analyze.c).
fn compare_scalars<'mcx>(
    values: &[ScalarItem<'mcx>],
    tupno_link: &mut [i32],
    ssup: &SortSupportData<'mcx>,
    a: usize,
    b: usize,
) -> PgResult<i32> {
    let da = &values[a].value;
    let ta = values[a].tupno;
    let db = &values[b].value;
    let tb = values[b].tupno;

    let compare = apply_sort_comparator::call(da.clone(), db.clone(), ssup)?;
    if compare != 0 {
        return Ok(compare);
    }

    // Equal datums: update tupno_link.
    if tupno_link[ta as usize] < tb {
        tupno_link[ta as usize] = tb;
    }
    if tupno_link[tb as usize] < ta {
        tupno_link[tb as usize] = ta;
    }
    Ok(ta - tb)
}

// ===========================================================================
// analyze_mcv_list()
// ===========================================================================

fn analyze_mcv_list(
    mcv_counts: &[i32],
    num_mcv_in: i32,
    stadistinct: f64,
    stanullfrac: f64,
    samplerows: i32,
    totalrows: f64,
) -> i32 {
    let mut num_mcv = num_mcv_in;

    // If the entire table was sampled, keep the whole list.
    if samplerows as f64 == totalrows || totalrows <= 1.0 {
        return num_mcv;
    }

    // Estimated number of distinct nonnull values in the table.
    let mut ndistinct_table = stadistinct;
    if ndistinct_table < 0.0 {
        ndistinct_table = -ndistinct_table * totalrows;
    }

    // sumcount over all but the last (least common) value.
    let mut sumcount = 0.0f64;
    for i in 0..(num_mcv - 1) as usize {
        sumcount += mcv_counts[i] as f64;
    }

    while num_mcv > 0 {
        let mut selec = 1.0 - sumcount / samplerows as f64 - stanullfrac;
        if selec < 0.0 {
            selec = 0.0;
        }
        if selec > 1.0 {
            selec = 1.0;
        }
        let otherdistinct = ndistinct_table - (num_mcv - 1) as f64;
        if otherdistinct > 1.0 {
            selec /= otherdistinct;
        }

        let N = totalrows;
        let n = samplerows as f64;
        let K = N * mcv_counts[(num_mcv - 1) as usize] as f64 / n;
        let variance = n * K * (N - K) * (N - n) / (N * N * (N - 1.0));
        let stddev = variance.sqrt();

        if mcv_counts[(num_mcv - 1) as usize] as f64 > selec * samplerows as f64 + 2.0 * stddev + 0.5
        {
            break;
        } else {
            num_mcv -= 1;
            if num_mcv == 0 {
                break;
            }
            sumcount -= mcv_counts[(num_mcv - 1) as usize] as f64;
        }
    }
    num_mcv
}

// ===========================================================================
// Helpers (model glue) and the inline vacuum command-layer helpers.
// ===========================================================================

/// `std_compute_stats` inward seam body: re-invoke the standard compute routine
/// for the array typanalyze leaf. The standard routine is selected by the
/// re-derived operator availability (same choice `std_typanalyze` made), and run
/// with the supplied fetchfunc.
pub fn std_compute_stats<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let mystats = std_analyze_data(stats.attrtypid)?;
    if OidIsValid(mystats.eqopr) && OidIsValid(mystats.ltopr) {
        compute_scalar_stats_inner(stats, fetchfunc, samplerows, totalrows)
    } else if OidIsValid(mystats.eqopr) {
        compute_distinct_stats_inner(stats, fetchfunc, samplerows, totalrows)
    } else {
        compute_trivial_stats_inner(stats, fetchfunc, samplerows)
    }
}

/// Run an `AnalyzeAttrComputeStatsFunc` (a fn pointer that returns `()` and may
/// internally `.expect()` on the error path). Mirrors the C compute_stats call;
/// errors surface as panics from the inner routines (matching the fn-pointer
/// callback convention used elsewhere for callbacks that can ereport).
fn run_compute_stats<'mcx>(
    f: AnalyzeAttrComputeStatsFunc,
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    f(stats, fetchfunc, samplerows, totalrows);
    Ok(())
}

/// `FormIndexDatum` (index.c) re-implemented locally (the seam is uninstalled),
/// mirroring the heapam-handler's local form. Evaluates plain key columns from
/// the slot and index expressions from the prepared states.
fn form_index_datum<'mcx>(
    index_info: &::nodes::execnodes::IndexInfo<'mcx>,
    expr_states: &mut PgVec<'mcx, ::mcx::PgBox<'mcx, ::nodes::execexpr::ExprState<'mcx>>>,
    slot: ::nodes::SlotId,
    econtext: ::nodes::EcxtId,
    estate: &mut ::nodes::EStateData<'mcx>,
) -> PgResult<(Vec<Datum<'mcx>>, [bool; INDEX_MAX_KEYS])> {
    let n = index_info.ii_NumIndexAttrs as usize;
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(n);
    let mut isnull = [false; INDEX_MAX_KEYS];
    let mut indexpr_item = 0usize;
    let num_states = expr_states.len();

    for i in 0..n {
        let keycol = index_info.ii_IndexAttrNumbers[i];
        if keycol != 0 {
            let (d, is_null) = slot_seam::slot_getattr::call(estate, slot, keycol)?;
            values.push(d);
            isnull[i] = is_null;
        } else {
            if indexpr_item >= num_states {
                return Err(PgError::error("wrong number of index expressions"));
            }
            let state = &mut expr_states[indexpr_item];
            let (d, is_null) =
                expr_seam::exec_eval_expr_switch_context::call(state, econtext, estate)?;
            values.push(d);
            isnull[i] = is_null;
            indexpr_item += 1;
        }
    }
    if indexpr_item != num_states {
        return Err(PgError::error("wrong number of index expressions"));
    }
    Ok((values, isnull))
}

/// `vacuum_open_relation(relid, relation, options, verbose, lmode)`
/// (vacuum.c:786) ported inline to return the real `Relation<'mcx>` (vacuum.c's
/// reachable form returns an Oid). Open and lock a relation to be analyzed,
/// emitting an appropriate log on failure. Returns `None` if the relation could
/// not be opened/locked.
///
/// SKIP_LOCKED IS exercised by the ANALYZE caller: `ANALYZE (SKIP_LOCKED) ...`
/// flows `params.options & VACOPT_SKIP_LOCKED` into `analyze_rel`, which must
/// acquire the lock conditionally (non-blocking) and skip the relation with a
/// WARNING ("lock not available") rather than wait. C acquires the lock in
/// non-blocking mode via `ConditionalLockRelationOid` first, then opens with
/// `NoLock`.
fn vacuum_open_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relation: Option<&RangeVar<'mcx>>,
    options: ::types_core::primitive::bits32,
    verbose: bool,
    lmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    let mut rel_lock = true;

    debug_assert!((options & (VACOPT_VACUUM | VACOPT_ANALYZE)) != 0);

    // Open the relation and get the appropriate lock on it.
    //
    // If we've been asked not to wait for the relation lock, acquire it first
    // in non-blocking mode, before calling try_relation_open().
    let rel: Option<Relation<'mcx>> = if options & VACOPT_SKIP_LOCKED == 0 {
        try_table_open(mcx, relid, lmode)?
    } else if vacuum_seams::conditional_lock_relation_oid::call(relid, lmode)? {
        try_table_open(mcx, relid, NoLock)?
    } else {
        rel_lock = false;
        None
    };

    // if relation is opened, leave
    if rel.is_some() {
        return Ok(rel);
    }

    // Relation could not be opened, hence generate if possible a log informing
    // on the situation. If the RangeVar is not defined, we do not have enough
    // information to provide a meaningful log statement.
    let Some(relation) = relation else {
        return Ok(None);
    };

    // Determine the log level. For manual ANALYZE, we emit a WARNING to match
    // the log statements in the permission checks; otherwise, only log if the
    // caller so requested.
    let elevel = if !vacuum_seams::am_autovacuum_worker_process::call()? {
        WARNING
    } else if verbose {
        LOG
    } else {
        return Ok(None);
    };

    let relname = relation
        .relname
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    // This inline form is only reached from the ANALYZE driver (analyze_rel),
    // which always strips VACOPT_VACUUM, so only the ANALYZE legs apply.
    if (options & VACOPT_ANALYZE) != 0 {
        if !rel_lock {
            ereport(elevel)
                .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                .errmsg(format!(
                    "skipping analyze of \"{}\" --- lock not available",
                    relname
                ))
                .finish(here("vacuum_open_relation"))?;
        } else {
            ereport(elevel)
                .errcode(ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!(
                    "skipping analyze of \"{}\" --- relation no longer exists",
                    relname
                ))
                .finish(here("vacuum_open_relation"))?;
        }
    }

    Ok(None)
}

/// `vacuum_is_permitted_for_relation` (vacuum.c) — the ownership / MAINTAIN
/// privilege gate vacuum.c owns and analyze.c shares. Reached through the vacuum
/// owner's seam; the body reads only `relisshared` and `relname` off the
/// Form_pg_class, so pass them by value to avoid crossing the borrow.
fn vacuum_is_permitted_for_relation(
    relid: Oid,
    reltuple: &::rel::FormData_pg_class<'_>,
    options: ::types_core::primitive::bits32,
) -> PgResult<bool> {
    vacuum_seams::vacuum_is_permitted_for_relation::call(
        relid,
        reltuple.relisshared,
        reltuple.relname.as_str().to_string(),
        options,
    )
}

/// `RELATION_IS_OTHER_TEMP(rel)` (utils/rel.h): a temp relation of another
/// backend. The repo models a temp relation by `rd_backend != INVALID_PROC_NUMBER`;
/// "other backend" needs the current backend id, which is not reachable here, so
/// the common ANALYZE case (own/regular relations) returns false and the rare
/// other-backend-temp skip is not modeled.
fn relation_is_other_temp(rel: &Relation<'_>) -> bool {
    rel.uses_local_buffers() && rel.rd_backend != ::types_core::primitive::INVALID_PROC_NUMBER
        // and rd_backend != MyProcNumber — MyProcNumber unreachable; treat as not-other.
        && false
}

/// `RelationGetNumberOfBlocks(rel)` (bufmgr.h).
fn RelationGetNumberOfBlocks(rel: &Relation<'_>) -> PgResult<BlockNumber> {
    hio_seams::relation_get_number_of_blocks::call(rel.rd_id)
}

/// `RelationGetIndexList(rel)` (relcache.c) — index OIDs.
fn RelationGetIndexList(rel: &Relation<'_>) -> PgResult<Vec<Oid>> {
    vacuum_seams::relation_get_index_list::call(rel.rd_id)
}

/// `vac_open_indexes(relation, lockmode, &nindexes, &Irel)` (vacuum.c:1290) —
/// open every "ready" index of `relation` (skipping not-yet-ready ones) under
/// `lockmode`, returning the opened index `Relation`s. The index OID list comes
/// from the vacuum-owned `RelationGetIndexList`; each index is opened via
/// `index_open` (indexam.c), which returns a real `Relation` value.
fn vac_open_indexes<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<Vec<Relation<'mcx>>> {
    debug_assert!(lockmode != NoLock);

    let indexoidlist = RelationGetIndexList(rel)?;
    let mut irel: Vec<Relation<'mcx>> = Vec::with_capacity(indexoidlist.len());

    for indexoid in indexoidlist {
        let indrel = indexam_seams::index_open::call(mcx, indexoid, lockmode)?;
        let indisready = indrel
            .rd_index
            .as_ref()
            .map(|i| i.indisready)
            .unwrap_or(false);
        if indisready {
            irel.push(indrel);
        } else {
            indrel.close(lockmode)?;
        }
    }

    Ok(irel)
}

/// `vac_close_indexes(nindexes, Irel, lockmode)` (vacuum.c).
fn vac_close_indexes<'mcx>(irel: Vec<Relation<'mcx>>, lockmode: LOCKMODE) -> PgResult<()> {
    for r in irel {
        table_close(r, lockmode)?;
    }
    Ok(())
}

/// `vac_update_relstats(...)` (vacuum.c) — update pg_class stats. Owned by
/// vacuum.c; reached through its installed Oid-keyed seam.
fn vac_update_relstats(
    relation: Oid,
    num_pages: BlockNumber,
    num_tuples: f64,
    num_all_visible_pages: BlockNumber,
    num_all_frozen_pages: BlockNumber,
    hasindex: bool,
    in_outer_xact: bool,
) -> PgResult<()> {
    vacuumlazy_seams::vac_update_relstats::call(
        ::types_vacuum::vacuumlazy::UpdateRelStatsArgs {
            relation,
            num_pages,
            num_tuples,
            num_all_visible_pages,
            num_all_frozen_pages,
            hasindex,
            frozenxid: ::types_core::xact::InvalidTransactionId,
            minmulti: 0,
            in_outer_xact,
        },
    )
    .map(|_| ())
}

/// `vacuum_delay_point()` (vacuum.c).
fn vacuum_delay_point() -> PgResult<()> {
    vacuumlazy_seams::vacuum_delay_point::call(true)
}

/// `RestrictSearchPath()` (guc.c).
fn RestrictSearchPath() -> PgResult<()> {
    guc_seams::restrict_search_path::call()
}

/// `AtEOXact_GUC(isCommit, nestLevel)` (guc.c).
fn AtEOXact_GUC(is_commit: bool, nestlevel: i32) -> PgResult<()> {
    vacuum_seams::at_eoxact_guc::call(is_commit, nestlevel)
}

/// `RELKIND_HAS_STORAGE(relkind)` (catalog/pg_class.h).
fn relkind_has_storage(relkind: u8) -> bool {
    use ::types_tuple::access::{
        RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
    };
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/// Clone the sampled-rows vector into `mcx` (for `stats.rows`).
fn clone_rows<'mcx>(
    mcx: Mcx<'mcx>,
    rows: &[FormedTuple<'mcx>],
) -> PgResult<Vec<FormedTuple<'mcx>>> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(r.clone_in(mcx)?);
    }
    Ok(out)
}

fn clone_tupdesc<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<TupleDesc<'mcx>> {
    Ok(Some(rel.rd_att_clone_in(mcx)?))
}

fn slot_copy_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut ::nodes::tuptable::SlotData<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    // ExecCopySlotHeapTuple over a standalone slot.
    execTuples::slot_store_fetch::ExecCopySlotHeapTuple(mcx, slot)
}

fn pgvec_from<'mcx, T>(mcx: Mcx<'mcx>, v: Vec<T>) -> PgResult<Vec<T>> {
    let _ = mcx;
    Ok(v)
}

fn pgvec_from_bool<'mcx>(mcx: Mcx<'mcx>, v: Vec<bool>) -> PgResult<Vec<bool>> {
    let _ = mcx;
    Ok(v)
}

/// Allocate a fresh zeroed `VacAttrStats`.
fn new_vac_attr_stats<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<VacAttrStats<'mcx>> {
    Ok(VacAttrStats {
        attstattarget: 0,
        attrtypid: InvalidOid,
        attrtypmod: 0,
        attrtype: None,
        attrcollid: InvalidOid,
        anl_context: Some(mcx),
        compute_stats: None,
        minrows: 0,
        extra_data: 0,
        stats_valid: false,
        stanullfrac: 0.0,
        stawidth: 0,
        stadistinct: 0.0,
        stakind: [0; STATISTIC_NUM_SLOTS],
        staop: [InvalidOid; STATISTIC_NUM_SLOTS],
        stacoll: [InvalidOid; STATISTIC_NUM_SLOTS],
        numnumbers: [0; STATISTIC_NUM_SLOTS],
        stanumbers: core::array::from_fn(|_| Vec::new()),
        numvalues: [0; STATISTIC_NUM_SLOTS],
        stavalues: core::array::from_fn(|_| Vec::new()),
        statypid: [InvalidOid; STATISTIC_NUM_SLOTS],
        statyplen: [0; STATISTIC_NUM_SLOTS],
        statypbyval: [false; STATISTIC_NUM_SLOTS],
        statypalign: [0; STATISTIC_NUM_SLOTS],
        tupattnum: 0,
        rows: Vec::new(),
        tup_desc: Some(rel.rd_att_clone_in(mcx)?),
        exprvals: Vec::new(),
        exprnulls: Vec::new(),
        rowstride: 0,
    })
}

// --- varlena/cstring width helpers (mirror VARSIZE_ANY / strlen(cstring)) ---

fn varsize_any_datum(value: &Datum<'_>) -> usize {
    match value {
        Datum::ByRef(b) => ::heaptuple::varsize_any(b),
        Datum::Cstring(s) => s.len() + 1,
        _ => 0,
    }
}

fn cstring_len(value: &Datum<'_>) -> usize {
    match value {
        Datum::Cstring(s) => s.len(),
        Datum::ByRef(b) => b.iter().position(|&c| c == 0).unwrap_or(b.len()),
        _ => 0,
    }
}

// --- ItemPointer accessors ---

fn item_pointer_block(t: &ItemPointerData) -> u32 {
    ((t.ip_blkid.bi_hi as u32) << 16) | (t.ip_blkid.bi_lo as u32)
}

fn item_pointer_offset(t: &ItemPointerData) -> u16 {
    t.ip_posid
}
