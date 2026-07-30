#![allow(non_snake_case)]

mod array_typanalyze;
mod qsort;
mod range_typanalyze;
mod ts_typanalyze;
pub mod sampling;

use backend_progress::progress::*;
use backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command,
    pgstat_progress_update_multi_param, pgstat_progress_update_param, PROGRESS_COMMAND_ANALYZE,
};
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{AttrNumber, BlockNumber, ForkNumber, InvalidOid, InvalidTransactionId, Oid};
use types_core::fmgr::{F_BOOLEQ, F_INT2EQ, F_OIDEQ};
use types_error::{PgError, PgResult};
use types_nodes::parsenodes::{VacuumRelation, VacuumStmt};
use types_rel::{
    Relation, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_slot::SlotData;
use types_tuple::{FormData_pg_attribute, HeapTupleData, TupleDescData};

const VACOPT_VACUUM: i32 = tableam_vocab::VACOPT_VACUUM as i32;
const VACOPT_ANALYZE: i32 = tableam_vocab::VACOPT_ANALYZE as i32;
const VACOPT_VERBOSE: i32 = tableam_vocab::VACOPT_VERBOSE as i32;
const VACOPT_SKIP_LOCKED: i32 = tableam_vocab::VACOPT_SKIP_LOCKED as i32;

const STATISTIC_RELATION_ID: Oid = 2619;
const STATISTIC_NUM_SLOTS: usize = 5;
const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;

const NATTS_PG_STATISTIC: usize = 31;
const ANUM_PG_STATISTIC_STARELID: usize = 1;
const ANUM_PG_STATISTIC_STAATTNUM: usize = 2;
const ANUM_PG_STATISTIC_STAINHERIT: usize = 3;
const ANUM_PG_STATISTIC_STANULLFRAC: usize = 4;
const ANUM_PG_STATISTIC_STAWIDTH: usize = 5;
const ANUM_PG_STATISTIC_STADISTINCT: usize = 6;
const ANUM_PG_STATISTIC_STAKIND1: usize = 7;
const ANUM_PG_STATISTIC_STAOP1: usize = 12;
const ANUM_PG_STATISTIC_STACOLL1: usize = 17;
const ANUM_PG_STATISTIC_STANUMBERS1: usize = 22;
const ANUM_PG_STATISTIC_STAVALUES1: usize = 27;

const FLOAT4OID: Oid = 700;
const WIDTH_THRESHOLD: usize = 1024;

const SHARE_UPDATE_EXCLUSIVE_LOCK: types_rel::LOCKMODE = 4;
const ROW_EXCLUSIVE_LOCK: types_rel::LOCKMODE = 3;
const ACCESS_SHARE_LOCK: types_rel::LOCKMODE = 1;
const NO_LOCK: types_rel::LOCKMODE = 0;

pub struct VacuumParams {
    pub options: i32,
}

/// `AcquireSampleRowsFunc` (fdwapi.h), shaped like this crate's native
/// `acquire_sample_rows` row sink plus C's elevel.
pub type AcquireSampleRowsFn = for<'mcx> fn(
    Mcx<'mcx>,
    &Relation<'mcx>,
    types_error::ErrorLevel,
    &mut PgVec<'mcx, HeapTupleData<'mcx>>,
    i32,
    &mut f64,
    &mut f64,
) -> PgResult<i32>;

/// `AnalyzeForeignTable` (fdwapi.h): the analyze half of the FdwRoutine,
/// keyed by FdwKind (planner fdwplan.rs precedent). `None` = the provider
/// declines (C's `return false`): analyze_rel warns and skips.
pub struct FdwAnalyzeRoutine {
    pub analyze_foreign_table: for<'mcx> fn(
        Mcx<'mcx>,
        &Relation<'mcx>,
    )
        -> PgResult<Option<(AcquireSampleRowsFn, BlockNumber)>>,
}

static FDW_ANALYZE_ROUTINES: [std::sync::OnceLock<&'static FdwAnalyzeRoutine>;
    types_nodes::NUM_FDW_KINDS] = [const { std::sync::OnceLock::new() }; types_nodes::NUM_FDW_KINDS];

pub fn install_fdw_analyze_routine(kind: types_nodes::FdwKind, r: &'static FdwAnalyzeRoutine) {
    if FDW_ANALYZE_ROUTINES[kind.index()].set(r).is_err() {
        panic!("install_fdw_analyze_routine: FdwAnalyzeRoutine already installed for {kind:?}");
    }
}

enum ComputeStats {
    Scalar,
    Distinct,
    Trivial,
    Range { is_multirange: bool },
    Array { std: StdCompute, elem_typeid: Oid },
    TsVector,
}

// std_typanalyze's pick, saved by array_typanalyze (C's std_compute_stats).
#[derive(Clone, Copy)]
enum StdCompute {
    Scalar,
    Distinct,
    Trivial,
}

struct StdAnalyzeData {
    eqopr: Oid,
    ltopr: Oid,
}

pub(crate) struct VacAttrStats<'mcx> {
    tupattnum: i32,
    attstattarget: i32,
    attrtypid: Oid,
    attrcollid: Oid,
    typlen: i16,
    typbyval: bool,
    typalign: u8,
    compute: ComputeStats,
    extra: StdAnalyzeData,
    minrows: i32,

    stats_valid: bool,
    stanullfrac: f32,
    stawidth: i32,
    stadistinct: f32,
    stakind: [i16; STATISTIC_NUM_SLOTS],
    staop: [Oid; STATISTIC_NUM_SLOTS],
    stacoll: [Oid; STATISTIC_NUM_SLOTS],
    stanumbers: [PgVec<'mcx, f32>; STATISTIC_NUM_SLOTS],
    stavalues: [PgVec<'mcx, Datum>; STATISTIC_NUM_SLOTS],
    // C's stavalues-vs-NULL distinction: an empty stored array (range length
    // histogram) is not a NULL column.
    stavalues_set: [bool; STATISTIC_NUM_SLOTS],
    statypid: [Oid; STATISTIC_NUM_SLOTS],
    statyplen: [i16; STATISTIC_NUM_SLOTS],
    statypbyval: [bool; STATISTIC_NUM_SLOTS],
    statypalign: [u8; STATISTIC_NUM_SLOTS],
}

pub fn ExecVacuum<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &'mcx VacuumStmt<'mcx>,
    source_text: &str,
    is_top_level: bool,
) -> PgResult<()> {
    if stmt.is_vacuumcmd {
        panic!("ExecVacuum (vacuum.c): VACUUM lane (commands_vacuum unit)");
    }
    let mut verbose = false;
    let mut skip_locked = false;
    // BUFFER_USAGE_LIMIT is validated C-exactly; the ANALYZE path has no
    // buffer-strategy plumbing yet, so the accepted value is unused.
    let mut _ring_size: i32 = -1;
    for opt in stmt.options.iter() {
        let d = opt.as_def_elem().expect("utility option DefElem");
        match d.defname.expect("option name") {
            "verbose" => verbose = def_get_boolean(d)?,
            "skip_locked" => skip_locked = def_get_boolean(d)?,
            "buffer_usage_limit" => {
                _ring_size = commands_vacuum::exec_vacuum_buffer_usage_limit(mcx, d)?
            }
            other => {
                return Err(Box::new(
                    PgError::error(format!("unrecognized ANALYZE option \"{other}\""))
                        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
                        .with_cursor_position(parser_small1::parser_errposition_source(
                            Some(source_text.as_bytes()),
                            d.location,
                            mbutils::GetDatabaseEncoding(),
                        )),
                ))
            }
        }
    }
    let params = VacuumParams {
        options: VACOPT_ANALYZE
            | if verbose { VACOPT_VERBOSE } else { 0 }
            | if skip_locked { VACOPT_SKIP_LOCKED } else { 0 },
    };
    vacuum(mcx, &stmt.rels, &params, is_top_level)
}

fn analyze_rel_seam<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relname: Option<&'a str>,
    va_cols: &'a types_nodes::NodeList<'mcx>,
    options: u32,
    in_outer_xact: bool,
) -> PgResult<()> {
    analyze_rel(
        mcx,
        relid,
        relname,
        va_cols,
        &VacuumParams { options: options as i32 },
        in_outer_xact,
    )
}

fn vacuum<'mcx>(
    mcx: Mcx<'mcx>,
    rels: &'mcx types_nodes::NodeList<'mcx>,
    params: &VacuumParams,
    is_top_level: bool,
) -> PgResult<()> {
    if commands_vacuum::in_vacuum() {
        return Err(PgError::error("ANALYZE cannot be executed from VACUUM or ANALYZE")
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .into());
    }
    let in_outer_xact = xact::IsInTransactionBlock(is_top_level);

    let mut vacrels: mcx::PgVec<'mcx, commands_vacuum::ExpandedVacRel<'mcx>> =
        mcx::PgVec::new_in(mcx);
    if rels.is_nil() {
        commands_vacuum::get_all_vacuum_rels(mcx, params.options as u32, &mut vacrels)?;
    } else {
        for reln in rels.iter() {
            let vrel: &VacuumRelation<'_> = reln.as_vacuum_relation().expect("VacuumRelation");
            commands_vacuum::expand_vacuum_rel(mcx, vrel, params.options as u32, &mut vacrels)?;
        }
    }

    let use_own_xacts = !in_outer_xact && vacrels.len() > 1;
    if use_own_xacts {
        if snapmgr::ActiveSnapshotSet() {
            snapmgr::PopActiveSnapshot()?;
        }
        xact::CommitTransactionCommand()?;
    }

    commands_vacuum::set_in_vacuum(true);
    // catch_unwind = C's PG_FINALLY (panics become ERRORs at tcop and the
    // session survives, so in_vacuum must reset on every exit path).
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PgResult<()> {
        for vrel in vacrels.iter() {
            if use_own_xacts {
                xact::StartTransactionCommand()?;
                let snapshot = snapmgr::GetTransactionSnapshot()?;
                snapmgr::PushActiveSnapshot(&snapshot)?;
            }
            analyze_rel(mcx, vrel.oid, vrel.relname, vrel.va_cols, params, in_outer_xact)?;
            if use_own_xacts {
                snapmgr::PopActiveSnapshot()?;
                xact::CommandCounterIncrement()?;
                xact::CommitTransactionCommand()?;
            } else {
                // Separating CCIs keep "ANALYZE t, t" self-consistent (C).
                xact::CommandCounterIncrement()?;
            }
        }
        Ok(())
    }));
    commands_vacuum::set_in_vacuum(false);
    match result {
        Ok(r) => r?,
        Err(p) => std::panic::resume_unwind(p),
    }
    if use_own_xacts {
        // Matches the CommitTransaction waiting in PostgresMain.
        xact::StartTransactionCommand()?;
    }
    Ok(())
}

pub fn analyze_rel(
    mcx: Mcx<'_>,
    relid: Oid,
    relname: Option<&str>,
    va_cols: &types_nodes::NodeList<'_>,
    params: &VacuumParams,
    in_outer_xact: bool,
) -> PgResult<()> {
    let Some(onerel) = commands_vacuum::vacuum_open_relation(
        mcx,
        relid,
        relname,
        (params.options & !VACOPT_VACUUM) as u32,
        SHARE_UPDATE_EXCLUSIVE_LOCK,
    )?
    else {
        return Ok(());
    };
    if !commands_vacuum::vacuum_is_permitted_for_relation(
        onerel.rd_id,
        onerel.name(),
        onerel.rd_rel.relisshared,
        (params.options & !VACOPT_VACUUM) as u32,
    )? {
        onerel.close(SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        return Ok(());
    }
    if onerel.rd_id == STATISTIC_RELATION_ID {
        onerel.close(SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        return Ok(());
    }
    let relkind = onerel.rd_rel.relkind;
    let mut acquirefunc: Option<AcquireSampleRowsFn> = None;
    let relpages = match relkind {
        RELKIND_RELATION | RELKIND_MATVIEW => bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            &onerel,
            ForkNumber::MAIN_FORKNUM,
        )?,
        RELKIND_PARTITIONED_TABLE => 0,
        RELKIND_FOREIGN_TABLE => {
            let kind = foreigncmds_seams::get_fdw_routine_by_rel_id::call(mcx, onerel.rd_id)?;
            let ok = match FDW_ANALYZE_ROUTINES[kind.index()].get() {
                Some(r) => (r.analyze_foreign_table)(mcx, &onerel)?,
                None => None,
            };
            match ok {
                Some((f, pages)) => {
                    acquirefunc = Some(f);
                    pages
                }
                None => {
                    elog::ereport(types_error::WARNING)
                        .errmsg(format!(
                            "skipping \"{}\" --- cannot analyze this foreign table",
                            onerel.name()
                        ))
                        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "analyze_rel"))?;
                    onerel.close(SHARE_UPDATE_EXCLUSIVE_LOCK)?;
                    return Ok(());
                }
            }
        }
        _ => {
            if params.options & VACOPT_VACUUM == 0 {
                elog::ereport(types_error::WARNING)
                    .errmsg(format!(
                        "skipping \"{}\" --- cannot analyze non-tables or special system tables",
                        onerel.name()
                    ))
                    .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "analyze_rel"))?;
            }
            onerel.close(SHARE_UPDATE_EXCLUSIVE_LOCK)?;
            return Ok(());
        }
    };

    pgstat_progress_start_command(PROGRESS_COMMAND_ANALYZE, onerel.rd_id);

    if relkind != RELKIND_PARTITIONED_TABLE {
        do_analyze_rel(
            mcx, &onerel, va_cols, params, acquirefunc, relpages, false, in_outer_xact, None,
        )?;
    }
    if onerel.rd_rel.relhassubclass {
        do_analyze_rel(
            mcx, &onerel, va_cols, params, acquirefunc, relpages, true, in_outer_xact, None,
        )?;
    }

    onerel.close(NO_LOCK)?;

    pgstat_progress_end_command();
    Ok(())
}

/// GL-COPYFAST-1 lever 3 (analyze-during-load): the sample size the standard
/// ANALYZE would use for this relation — the per-column examine pass
/// (attstattarget-driven minrows, 300x target) maxed with the
/// extended-statistics floor, exactly do_analyze_rel's targrows arithmetic.
/// Index-expression stats could only raise it via indexes, and the caller's
/// pipeline refuses indexed relations (debug-asserted).
pub fn inline_analyze_targrows(onerel: &Relation<'_>) -> PgResult<i32> {
    debug_assert!(
        !onerel.rd_rel.relhasindex,
        "inline analyze targrows on an indexed relation"
    );
    let anl = MemoryContext::new("InlineAnalyzeTargrows");
    let anl_mcx = anl.mcx();
    let tupdesc = onerel.descr();
    let mut targrows: i32 = 100;
    let mut colstats: PgVec<'_, statistics::ColStats> = PgVec::new_in(anl_mcx);
    for i in 1..=tupdesc.natts {
        if let Some(s) = examine_attribute(anl_mcx, onerel, i, None)? {
            targrows = targrows.max(s.minrows);
            colstats.push(statistics::ColStats {
                tupattnum: s.tupattnum,
                attstattarget: s.attstattarget,
                attrtypid: s.attrtypid,
                attrcollid: s.attrcollid,
                typlen: s.typlen,
                typbyval: s.typbyval,
            });
        }
    }
    targrows = targrows.max(statistics::ComputeExtStatisticsRows(
        anl_mcx,
        onerel.rd_id,
        &colstats,
    )?);
    Ok(targrows)
}

/// GL-COPYFAST-1 lever 3: the ANALYZE write half driven from a pre-acquired
/// in-stream sample (rows in physical/stream order, formed against the
/// relation's descriptor; totalrows = the exact loaded row count). Runs the
/// standard do_analyze_rel body minus acquisition, so pg_statistic,
/// extended statistics, and pg_class relstats land exactly as a post-load
/// ANALYZE would compute them from the same sample — without re-reading the
/// table. Permission/relkind refusals skip silently-with-warning exactly
/// like ANALYZE's (vacuum_is_permitted_for_relation emits C's WARNING).
///
/// Locking: ANALYZE's own ShareUpdateExclusive (self-conflicting: two
/// inline writers serialize like two ANALYZEs), held to end of transaction
/// like analyze_rel's. in_outer_xact=true: the caller's (COPY's)
/// transaction is an outer transaction relative to this stats write — the
/// pg_class flag-clearing C gates on !in_outer_xact stays off, matching an
/// in-transaction-block ANALYZE. The inheritance (inh) pass is not run: the
/// sample covers exactly this relation's stream.
pub fn analyze_rel_inline_sample<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    rows: &[HeapTupleData<'_>],
    totalrows: f64,
) -> PgResult<()> {
    let onerel = table::table_open(mcx, relid, SHARE_UPDATE_EXCLUSIVE_LOCK)?;
    if onerel.rd_id == STATISTIC_RELATION_ID
        || onerel.rd_rel.relkind != RELKIND_RELATION
        || !commands_vacuum::vacuum_is_permitted_for_relation(
            onerel.rd_id,
            onerel.name(),
            onerel.rd_rel.relisshared,
            VACOPT_ANALYZE as u32,
        )?
    {
        onerel.close(SHARE_UPDATE_EXCLUSIVE_LOCK)?;
        return Ok(());
    }
    let relpages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        &onerel,
        ForkNumber::MAIN_FORKNUM,
    )?;
    let params = VacuumParams { options: VACOPT_ANALYZE };
    let nil = types_nodes::NodeList::nil();
    do_analyze_rel(
        mcx,
        &onerel,
        &nil,
        &params,
        None,
        relpages,
        false,
        true,
        Some((rows, totalrows)),
    )?;
    onerel.close(NO_LOCK)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn do_analyze_rel<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    va_cols: &types_nodes::NodeList<'_>,
    params: &VacuumParams,
    acquirefunc: Option<AcquireSampleRowsFn>,
    relpages: BlockNumber,
    inh: bool,
    in_outer_xact: bool,
    // GL-COPYFAST-1 lever 3 (analyze-during-load): Some((rows, totalrows)) =
    // the sample was already acquired in-stream by the load pipeline (rows in
    // physical/stream order, formed against onerel's descriptor, alive for
    // the whole call); the acquisition phase is skipped and totaldeadrows is
    // 0 (a fresh load has none). Everything downstream — compute, footer-NDV
    // override, pg_statistic write, relstats — is the standard body, so the
    // catalog rows written are the ones ANALYZE itself would write from the
    // same sample. Progress reporting stays with the caller's command.
    inline_sample: Option<(&[HeapTupleData<'_>], f64)>,
) -> PgResult<()> {
    let anl = MemoryContext::new("Analyze");
    let anl_mcx = anl.mcx();

    // Run index functions as the table owner, locked down (analyze.c:340-344).
    let guard = miscinit::SecContextGuard::security_restricted(onerel.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let starttime = timestamp_seams::get_current_timestamp::call();

    // Partitioned: hasindex only (nothing to open); inh recursion: leave the
    // parent's indexes alone entirely (C).
    let is_partitioned = onerel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    let irel = if is_partitioned || inh {
        PgVec::new_in(mcx)
    } else {
        commands_vacuum::vac_open_indexes(mcx, onerel, ROW_EXCLUSIVE_LOCK)?
    };
    let hasindex = if is_partitioned {
        !relcache_seams::relation_get_index_list::call(mcx, onerel.rd_id)?.is_empty()
    } else {
        !irel.is_empty()
    };

    let tupdesc = onerel.descr();
    let mut vacattrstats: PgVec<'_, VacAttrStats<'_>> = PgVec::new_in(anl_mcx);
    if !va_cols.is_nil() {
        let mut seen: PgVec<'_, AttrNumber> = PgVec::new_in(anl_mcx);
        for c in va_cols.iter() {
            let name = c.as_string().expect("column name String").sval;
            // attnameAttNum with sysColOK=false: dropped columns don't match.
            let i = (1..=tupdesc.natts).find(|&i| {
                let att = tupdesc.attr(i as usize - 1);
                att.attname.name_str() == name.as_bytes() && !att.attisdropped
            });
            let Some(i) = i else {
                return Err(PgError::error(format!(
                    "column \"{name}\" of relation \"{}\" does not exist",
                    onerel.name()
                ))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN)
                .into());
            };
            let i = i as AttrNumber;
            if seen.contains(&i) {
                return Err(PgError::error(format!(
                    "column \"{name}\" of relation \"{}\" appears more than once",
                    onerel.name()
                ))
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_COLUMN)
                .into());
            }
            seen.push(i);
            if let Some(s) = examine_attribute(anl_mcx, onerel, i as i32, None)? {
                vacattrstats.push(s);
            }
        }
    } else {
        for i in 1..=tupdesc.natts {
            if let Some(s) = examine_attribute(anl_mcx, onerel, i, None)? {
                vacattrstats.push(s);
            }
        }
    }

    let mut indexdata: PgVec<'_, AnlIndexData<'_>> = PgVec::new_in(anl_mcx);
    for ind in irel.iter() {
        let index_info = execindexing::BuildIndexInfo(anl_mcx, ind)?;
        let mut thisdata = AnlIndexData {
            index_info,
            tuple_fract: 1.0,
            vacattrstats: PgVec::new_in(anl_mcx),
        };
        if !thisdata.index_info.ii_Expressions.is_nil() && va_cols.is_nil() {
            let mut indexpr_item = thisdata.index_info.ii_Expressions.iter();
            for i in 0..thisdata.index_info.ii_NumIndexAttrs as usize {
                if thisdata.index_info.ii_IndexAttrNumbers[i] == 0 {
                    let indexkey = indexpr_item
                        .next()
                        .unwrap_or_else(|| panic!("too few entries in indexprs list"));
                    if let Some(s) =
                        examine_attribute(anl_mcx, ind, (i + 1) as i32, Some(indexkey))?
                    {
                        thisdata.vacattrstats.push(s);
                    }
                }
            }
        }
        indexdata.push(thisdata);
    }

    let mut colstats: PgVec<'_, statistics::ColStats> = PgVec::new_in(anl_mcx);
    for s in vacattrstats.iter() {
        colstats.push(statistics::ColStats {
            tupattnum: s.tupattnum,
            attstattarget: s.attstattarget,
            attrtypid: s.attrtypid,
            attrcollid: s.attrcollid,
            typlen: s.typlen,
            typbyval: s.typbyval,
        });
    }

    let mut targrows: i32 = 100;
    for s in vacattrstats.iter() {
        targrows = targrows.max(s.minrows);
    }
    for thisdata in indexdata.iter() {
        for s in thisdata.vacattrstats.iter() {
            targrows = targrows.max(s.minrows);
        }
    }
    targrows = targrows.max(statistics::ComputeExtStatisticsRows(
        anl_mcx,
        onerel.rd_id,
        &colstats,
    )?);

    let mut totalrows = 0.0f64;
    let mut totaldeadrows = 0.0f64;
    let mut rows: PgVec<'_, HeapTupleData<'_>> = PgVec::new_in(anl_mcx);
    if inline_sample.is_none() {
        pgstat_progress_update_param(
            PROGRESS_ANALYZE_PHASE,
            if inh {
                PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH
            } else {
                PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS
            },
        );
    }
    let anl_trace = analyze_trace();
    let t_acquire = std::time::Instant::now();
    let numrows = if let Some((prows, ptotal)) = inline_sample {
        debug_assert!(!inh, "inline sample is a single-relation (non-inh) pass");
        for t in prows {
            // SAFETY: the caller's sample images outlive this call (they
            // live in the load statement's context); the reborrow mirrors
            // compute_index_stats' shouldFree=false pattern.
            let tup = unsafe {
                HeapTupleData::from_raw_parts(
                    core::ptr::from_ref(t.t_data()).cast::<u8>(),
                    t.t_len,
                    t.t_self,
                    t.t_tableOid,
                )
            };
            rows.push(tup);
        }
        totalrows = ptotal;
        totaldeadrows = 0.0;
        prows.len() as i32
    } else if inh {
        acquire_inherited_sample_rows(
            anl_mcx,
            onerel,
            &mut rows,
            targrows,
            &mut totalrows,
            &mut totaldeadrows,
        )?
    } else if let Some(f) = acquirefunc {
        let elevel = if params.options & VACOPT_VERBOSE != 0 {
            types_error::INFO
        } else {
            types_error::DEBUG2
        };
        f(anl_mcx, onerel, elevel, &mut rows, targrows, &mut totalrows, &mut totaldeadrows)?
    } else {
        acquire_sample_rows(
            anl_mcx,
            onerel,
            &mut rows,
            targrows,
            &mut totalrows,
            &mut totaldeadrows,
        )?
    };

    let acquire_wall = t_acquire.elapsed().as_secs_f64();
    let t_compute = std::time::Instant::now();
    let mut maxcol: (i32, f64) = (0, 0.0);
    if numrows > 0 {
        if inline_sample.is_none() {
            pgstat_progress_update_param(
                PROGRESS_ANALYZE_PHASE,
                PROGRESS_ANALYZE_PHASE_COMPUTE_STATS,
            );
        }

        // Bump: armed cmp frames detoast packed by-ref args here per comparison,
        // freed wholesale at the per-column reset (C pfrees per call).
        let mut col_cx = anl.new_child_bump("Analyze Column");
        let src = FetchSource::Heap { tupdesc, rows: &rows[..numrows as usize] };
        // Ingest-time footer NDV (whole-stream HLL): sampled Duj1 underestimates
        // heavy-tailed text NDV 100-1500x, so a present footer count wins.
        // The inherited pass unions the children's v8 REGISTER sketches
        // (scalars cannot be combined — a sum double-counts values shared
        // across children) into an honest parent-level NDV.
        let footer_ndv = if !inh {
            if tableam::TableAm::of(onerel) == Some(tableam::TableAm::Pgrcolumnar) {
                tableam::pgrcolumnar_footer_ndv(onerel)?
            } else {
                None
            }
        } else {
            inherited_pgrcolumnar_footer_ndv(anl_mcx, onerel)?
        };
        for s in vacattrstats.iter_mut() {
            let t_col = anl_trace.then(std::time::Instant::now);
            match s.compute {
                ComputeStats::Scalar => {
                    compute_scalar_stats(anl_mcx, col_cx.mcx(), s, &src, numrows, totalrows)?
                }
                ComputeStats::Distinct => {
                    compute_distinct_stats(anl_mcx, col_cx.mcx(), s, &src, numrows, totalrows)?
                }
                ComputeStats::Trivial => {
                    compute_trivial_stats(s, &src, numrows)?
                }
                ComputeStats::Range { is_multirange } => {
                    range_typanalyze::compute_range_stats(
                        anl_mcx, s, is_multirange, &src, numrows, totalrows,
                    )?
                }
                ComputeStats::Array { std, elem_typeid } => {
                    array_typanalyze::compute_array_stats(
                        anl_mcx, col_cx.mcx(), s, std, elem_typeid, &src, numrows,
                        totalrows,
                    )?
                }
                ComputeStats::TsVector => {
                    ts_typanalyze::compute_tsvector_stats(anl_mcx, col_cx.mcx(), s, &src, numrows)?
                }
            }
            if let Some(ndv) = &footer_ndv {
                if s.stats_valid && s.tupattnum >= 1 {
                    let d = ndv.get(s.tupattnum as usize - 1).copied().unwrap_or(0);
                    if d > 0 {
                        let d = (d as f64).min(totalrows.max(1.0));
                        // Same negative-fraction convention as compute_scalar_stats.
                        s.stadistinct = if d > 0.1 * totalrows {
                            -(d / totalrows) as f32
                        } else {
                            d as f32
                        };
                    }
                }
            }
            // n_distinct / n_distinct_inherited attoptions override
            // (analyze.c:568-582).
            if let Some(aopt) =
                attoptcache::get_attribute_options(anl_mcx, onerel.rd_id, s.tupattnum as i16)?
            {
                let n_distinct = if inh { aopt.n_distinct_inherited } else { aopt.n_distinct };
                if n_distinct != 0.0 {
                    s.stadistinct = n_distinct as f32;
                }
            }
            if let Some(t) = t_col {
                let dt = t.elapsed().as_secs_f64();
                if dt > maxcol.1 {
                    maxcol = (s.tupattnum, dt);
                }
            }
            col_cx.reset();
        }
        if hasindex {
            compute_index_stats(
                anl_mcx,
                &anl,
                onerel,
                totalrows,
                &mut indexdata,
                &rows[..numrows as usize],
                &mut col_cx,
            )?;
        }
        update_attstats(onerel.rd_id, inh, &vacattrstats)?;
        for (ind, thisdata) in irel.iter().zip(indexdata.iter()) {
            update_attstats(ind.rd_id, false, &thisdata.vacattrstats)?;
        }

        statistics::BuildRelationExtStatistics(
            anl_mcx,
            onerel,
            inh,
            totalrows,
            &rows[..numrows as usize],
            &colstats,
            &mut ExtStatsExprCompute,
        )?;
    }
    if anl_trace {
        // compute bucket includes index/ext stats + the pg_statistic write
        // (both ~0 on the load shape); acquire has its own pgrcolumnar line.
        eprintln!(
            "ANALYZE|TRACE|rel={} rows={} acquire={:.3}s compute={:.3}s maxcol=(att {}, {:.3}s)",
            onerel.rd_id,
            numrows,
            acquire_wall,
            t_compute.elapsed().as_secs_f64(),
            maxcol.0,
            maxcol.1
        );
    }

    if inline_sample.is_none() {
        pgstat_progress_update_param(
            PROGRESS_ANALYZE_PHASE,
            PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE,
        );
    }

    if !inh {
        // Storage-less relkinds (foreign tables) have no visibility map.
        let (relallvisible, relallfrozen) =
            if types_rel::pg_class::RELKIND_HAS_STORAGE(onerel.rd_rel.relkind) {
                visibilitymap::visibilitymap_count(onerel)?
            } else {
                (0, 0)
            };
        xact::CommandCounterIncrement()?;
        vacuum_seams::vac_update_relstats::call(
            onerel,
            relpages,
            totalrows,
            relallvisible,
            relallfrozen,
            hasindex,
            InvalidTransactionId,
            0,
            in_outer_xact,
        )?;

        for (ind, thisdata) in irel.iter().zip(indexdata.iter()) {
            let ind_pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                ind,
                ForkNumber::MAIN_FORKNUM,
            )?;
            let totalindexrows = (thisdata.tuple_fract * totalrows).ceil();
            vacuum_seams::vac_update_relstats::call(
                ind,
                ind_pages,
                totalindexrows,
                0,
                0,
                false,
                InvalidTransactionId,
                0,
                in_outer_xact,
            )?;
        }
    } else if is_partitioned {
        // Storage-less shell: relpages stays C's -1 sentinel; only reltuples
        // and relhasindex carry information.
        xact::CommandCounterIncrement()?;
        vacuum_seams::vac_update_relstats::call(
            onerel,
            BlockNumber::MAX,
            totalrows,
            0,
            0,
            hasindex,
            InvalidTransactionId,
            0,
            in_outer_xact,
        )?;
    }

    // C reports !inh counts here; partitioned-with-inh zeros; plain
    // inheritance parents report nothing.
    if !inh {
        pgstat::pgstat_report_analyze(
            onerel.rd_id,
            onerel.rd_rel.relisshared,
            onerel.rd_rel.relkind,
            onerel.pgstat_enabled.get(),
            totalrows as i64,
            totaldeadrows as i64,
            va_cols.is_nil(),
            starttime,
        );
    } else if is_partitioned {
        pgstat::pgstat_report_analyze(
            onerel.rd_id,
            onerel.rd_rel.relisshared,
            onerel.rd_rel.relkind,
            onerel.pgstat_enabled.get(),
            0,
            0,
            va_cols.is_nil(),
            starttime,
        );
    }

    if params.options & VACOPT_VACUUM == 0 {
        for ind in irel.iter() {
            let ivinfo = nbtree::IndexVacuumInfo {
                index: ind,
                heaprel: &**onerel,
                analyze_only: true,
                estimated_count: true,
                num_heap_tuples: onerel.rd_rel.reltuples as f64,
                strategy: bufmgr_seams::get_access_strategy::call(
                    types_storage::buf::BufferAccessStrategyType::BasVacuum,
                ),
            };
            commands_vacuum::vac_cleanup_one_index(mcx, &ivinfo, None)?;
        }
    }
    commands_vacuum::vac_close_indexes(irel, NO_LOCK)?;

    // Roll back GUC changes from index functions; restore userid (analyze.c:850-853).
    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();
    Ok(())
}

struct AnlIndexData<'mcx> {
    index_info: execindexing::IndexInfo<'mcx>,
    tuple_fract: f64,
    vacattrstats: PgVec<'mcx, VacAttrStats<'mcx>>,
}

#[allow(clippy::too_many_arguments)]
fn compute_index_stats<'mcx>(
    anl_mcx: Mcx<'mcx>,
    anl: &MemoryContext,
    onerel: &Relation<'mcx>,
    totalrows: f64,
    indexdata: &mut [AnlIndexData<'mcx>],
    rows: &[HeapTupleData<'mcx>],
    col_cx: &mut MemoryContext,
) -> PgResult<()> {
    const MAX_KEYS: usize = types_core::fmgr::INDEX_MAX_KEYS as usize;
    let mut values = [Datum::null(); MAX_KEYS];
    let mut isnull = [false; MAX_KEYS];
    let mut ind_cx = anl.new_child_bump("Analyze Index");
    for thisdata in indexdata.iter_mut() {
        let attr_cnt = thisdata.vacattrstats.len();
        if attr_cnt == 0 && thisdata.index_info.ii_Predicate.is_nil() {
            continue;
        }
        {
            let ind_mcx = ind_cx.mcx();
            let mut per_tuple = ind_cx.new_child_bump("Analyze Index per-tuple");
            let mut slot = exectuples::make_tuple_table_slot(
                anl_mcx,
                types_slot::TupleSlotKind::HeapTuple,
                Some(onerel.rd_att.clone()),
            );
            let mut exprvals: PgVec<'_, Datum> =
                mcx::vec_with_capacity_in(ind_mcx, rows.len() * attr_cnt)?;
            let mut exprnulls: PgVec<'_, bool> =
                mcx::vec_with_capacity_in(ind_mcx, rows.len() * attr_cnt)?;
            // C analyze.c compute_index_stats: ExecPrepareQual runs before
            // the sample-row loop.
            execindexing::prepare_index_predicate(anl_mcx, &mut thisdata.index_info)?;
            let mut numindexrows = 0usize;
            for row in rows {
                per_tuple.reset();
                // SAFETY: the sample image lives in anl_mcx across this loop;
                // the reborrow mirrors C's shouldFree=false ExecStoreHeapTuple.
                let tuple = unsafe {
                    HeapTupleData::from_raw_parts(
                        core::ptr::from_ref(row.t_data()).cast::<u8>(),
                        row.t_len,
                        row.t_self,
                        row.t_tableOid,
                    )
                };
                exectuples::exec_store_heap_tuple(&mut slot, anl_mcx, tuple);
                if !thisdata.index_info.ii_Predicate.is_nil()
                    && !execindexing::index_predicate_passes(
                        anl_mcx,
                        per_tuple.mcx(),
                        &mut thisdata.index_info,
                        &mut slot,
                    )?
                {
                    continue;
                }
                numindexrows += 1;
                if attr_cnt > 0 {
                    execindexing::FormIndexDatum(
                        anl_mcx,
                        per_tuple.mcx(),
                        &mut thisdata.index_info,
                        &mut slot,
                        &mut values,
                        &mut isnull,
                    )?;
                    for stats in thisdata.vacattrstats.iter() {
                        let attnum = stats.tupattnum as usize;
                        if isnull[attnum - 1] {
                            exprvals.push(Datum::null());
                            exprnulls.push(true);
                        } else {
                            exprvals.push(datum_copy_in(
                                ind_mcx,
                                values[attnum - 1],
                                stats.typbyval,
                                stats.typlen,
                            )?);
                            exprnulls.push(false);
                        }
                    }
                }
            }
            thisdata.tuple_fract = numindexrows as f64 / rows.len() as f64;
            let totalindexrows = (thisdata.tuple_fract * totalrows).ceil();
            if numindexrows > 0 {
                for (i, stats) in thisdata.vacattrstats.iter_mut().enumerate() {
                    let src = FetchSource::Expr {
                        vals: &exprvals,
                        nulls: &exprnulls,
                        stride: attr_cnt,
                        off: i,
                    };
                    match stats.compute {
                        ComputeStats::Scalar => compute_scalar_stats(
                            anl_mcx,
                            col_cx.mcx(),
                            stats,
                            &src,
                            numindexrows as i32,
                            totalindexrows,
                        )?,
                        ComputeStats::Distinct => compute_distinct_stats(
                            anl_mcx,
                            col_cx.mcx(),
                            stats,
                            &src,
                            numindexrows as i32,
                            totalindexrows,
                        )?,
                        ComputeStats::Trivial => {
                            compute_trivial_stats(stats, &src, numindexrows as i32)?
                        }
                        ComputeStats::Range { is_multirange } => {
                            range_typanalyze::compute_range_stats(
                                anl_mcx,
                                stats,
                                is_multirange,
                                &src,
                                numindexrows as i32,
                                totalindexrows,
                            )?
                        }
                        ComputeStats::TsVector => ts_typanalyze::compute_tsvector_stats(
                            anl_mcx,
                            col_cx.mcx(),
                            stats,
                            &src,
                            numindexrows as i32,
                        )?,
                        ComputeStats::Array { std, elem_typeid } => {
                            array_typanalyze::compute_array_stats(
                                anl_mcx,
                                col_cx.mcx(),
                                stats,
                                std,
                                elem_typeid,
                                &src,
                                numindexrows as i32,
                                totalindexrows,
                            )?
                        }
                    }
                    col_cx.reset();
                }
            }
        }
        ind_cx.reset();
    }
    Ok(())
}

fn examine_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'_>,
    attnum: i32,
    index_expr: Option<types_nodes::Node<'_>>,
) -> PgResult<Option<VacAttrStats<'mcx>>> {
    let attr: &FormData_pg_attribute = onerel.descr().attr(attnum as usize - 1);
    if attr.attisdropped {
        return Ok(None);
    }
    if attr.attgenerated == b'v' as i8 {
        return Ok(None);
    }
    let attstattarget = syscache_seams::lookup_pg_attribute_stattarget::call(
        onerel.rd_id,
        attnum as AttrNumber,
    )?
    .map_or(-1, |t| t as i32);
    if attstattarget == 0 {
        return Ok(None);
    }
    let (atttypid, attcollid) = match index_expr {
        Some(e) => {
            // Explicit index-column collation wins over the expression's.
            let coll = if onerel.rd_indcollation[attnum as usize - 1] != InvalidOid {
                onerel.rd_indcollation[attnum as usize - 1]
            } else {
                nodes_core::node_funcs::expr_collation(e)
            };
            (nodes_core::node_funcs::expr_type(e), coll)
        }
        None => (attr.atttypid, attr.attcollation),
    };
    let typanalyze = syscache_seams::pg_type_typanalyze::call(atttypid)?;
    let ty = syscache_seams::lookup_pg_type_shape::call(atttypid)?
        .expect("attribute type row");

    let mut stats = VacAttrStats {
        tupattnum: attnum,
        attstattarget,
        attrtypid: atttypid,
        attrcollid: attcollid,
        typlen: ty.typlen,
        typbyval: ty.typbyval,
        typalign: ty.typalign as u8,
        compute: ComputeStats::Trivial,
        extra: StdAnalyzeData { eqopr: InvalidOid, ltopr: InvalidOid },
        minrows: 0,
        stats_valid: false,
        stanullfrac: 0.0,
        stawidth: 0,
        stadistinct: 0.0,
        stakind: [0; STATISTIC_NUM_SLOTS],
        staop: [InvalidOid; STATISTIC_NUM_SLOTS],
        stacoll: [InvalidOid; STATISTIC_NUM_SLOTS],
        stanumbers: [
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
        ],
        stavalues: [
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
        ],
        stavalues_set: [false; STATISTIC_NUM_SLOTS],
        statypid: [atttypid; STATISTIC_NUM_SLOTS],
        statyplen: [ty.typlen; STATISTIC_NUM_SLOTS],
        statypbyval: [ty.typbyval; STATISTIC_NUM_SLOTS],
        statypalign: [ty.typalign as u8; STATISTIC_NUM_SLOTS],
    };

    // Closed-set typanalyze dispatch (rule 4): std, array 3816, range 3916,
    // multirange 4242, tsvector 3688; anything else is an unported analyze lane.
    let ok = match typanalyze {
        InvalidOid => std_typanalyze(&mut stats)?,
        3816 => array_typanalyze::setup(&mut stats)?,
        3688 => ts_typanalyze::setup(&mut stats)?,
        3916 => {
            stats.compute = ComputeStats::Range { is_multirange: false };
            range_typanalyze::setup(&mut stats)?
        }
        4242 => {
            stats.compute = ComputeStats::Range { is_multirange: true };
            range_typanalyze::setup(&mut stats)?
        }
        other => {
            panic!("examine_attribute (analyze.c): custom typanalyze {other}; typanalyze lane")
        }
    };
    if !ok {
        return Ok(None);
    }
    Ok(Some(stats))
}

// examine_expression (extended_stats.c): VacAttrStats typed from the
// expression tree; attstattarget comes from the statistics object.
fn examine_expression<'mcx>(
    mcx: Mcx<'mcx>,
    expr: types_nodes::Node<'_>,
    stattarget: i32,
) -> PgResult<Option<VacAttrStats<'mcx>>> {
    let atttypid = nodes_core::node_funcs::expr_type(expr);
    let attcollid = nodes_core::node_funcs::expr_collation(expr);
    let typanalyze = syscache_seams::pg_type_typanalyze::call(atttypid)?;
    let ty = syscache_seams::lookup_pg_type_shape::call(atttypid)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {atttypid}"));

    let mut stats = VacAttrStats {
        tupattnum: 0,
        attstattarget: stattarget,
        attrtypid: atttypid,
        attrcollid: attcollid,
        typlen: ty.typlen,
        typbyval: ty.typbyval,
        typalign: ty.typalign as u8,
        compute: ComputeStats::Trivial,
        extra: StdAnalyzeData { eqopr: InvalidOid, ltopr: InvalidOid },
        minrows: 0,
        stats_valid: false,
        stanullfrac: 0.0,
        stawidth: 0,
        stadistinct: 0.0,
        stakind: [0; STATISTIC_NUM_SLOTS],
        staop: [InvalidOid; STATISTIC_NUM_SLOTS],
        stacoll: [InvalidOid; STATISTIC_NUM_SLOTS],
        stanumbers: [
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
        ],
        stavalues: [
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
            PgVec::new_in(mcx),
        ],
        stavalues_set: [false; STATISTIC_NUM_SLOTS],
        statypid: [atttypid; STATISTIC_NUM_SLOTS],
        statyplen: [ty.typlen; STATISTIC_NUM_SLOTS],
        statypbyval: [ty.typbyval; STATISTIC_NUM_SLOTS],
        statypalign: [ty.typalign as u8; STATISTIC_NUM_SLOTS],
    };
    let ok = match typanalyze {
        InvalidOid => std_typanalyze(&mut stats)?,
        3816 => array_typanalyze::setup(&mut stats)?,
        3688 => ts_typanalyze::setup(&mut stats)?,
        3916 => {
            stats.compute = ComputeStats::Range { is_multirange: false };
            range_typanalyze::setup(&mut stats)?
        }
        4242 => {
            stats.compute = ComputeStats::Range { is_multirange: true };
            range_typanalyze::setup(&mut stats)?
        }
        other => {
            panic!("examine_expression (extended_stats.c): custom typanalyze {other}")
        }
    };
    if !ok || stats.minrows <= 0 {
        return Ok(None);
    }
    Ok(Some(stats))
}

pub struct ExtStatsExprCompute;

impl<'mcx> statistics::ExprStatsCompute<'mcx> for ExtStatsExprCompute {
    // compute_expr_stats + serialize_expr_stats row extraction
    // (extended_stats.c); expression stats are computed against the sample
    // only, so totalrows == samplerows per C.
    fn compute(
        &mut self,
        mcx: Mcx<'mcx>,
        onerel: &Relation<'mcx>,
        exprs: &[types_nodes::Node<'mcx>],
        stattarget: i32,
        rows: &[HeapTupleData<'_>],
    ) -> PgResult<PgVec<'mcx, Option<statistics::ExprStatsRow<'mcx>>>> {
        let mut out: PgVec<'mcx, Option<statistics::ExprStatsRow<'mcx>>> = PgVec::new_in(mcx);
        let col_scratch = MemoryContext::new("Analyze Expression");
        let mut col_cx = col_scratch.new_child_bump("Analyze Expression scratch");
        let mut per_tuple = col_scratch.new_child_bump("Analyze Expression per-tuple");
        for &expr in exprs {
            let Some(mut stats) = examine_expression(mcx, expr, stattarget)? else {
                out.push(None);
                continue;
            };
            let mut state = execexpr::exec_init_expr(mcx, Some(expr), execexpr::ParamBind::NONE)?
                .expect("statistics expression");
            let mut slot = exectuples::make_tuple_table_slot(
                mcx,
                types_slot::TupleSlotKind::HeapTuple,
                Some(onerel.rd_att.clone()),
            );
            let mut exprvals: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, rows.len())?;
            let mut exprnulls: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, rows.len())?;
            for row in rows {
                per_tuple.reset();
                // SAFETY: per_tuple outlives the eval; the result is copied
                // into mcx before the next reset.
                unsafe { state.arm_result_mcx_raw(per_tuple.mcx()) };
                // SAFETY: the sample image lives across this loop; the
                // reborrow mirrors C's shouldFree=false ExecStoreHeapTuple.
                let tuple = unsafe {
                    HeapTupleData::from_raw_parts(
                        core::ptr::from_ref(row.t_data()).cast::<u8>(),
                        row.t_len,
                        row.t_self,
                        row.t_tableOid,
                    )
                };
                exectuples::exec_store_heap_tuple(&mut slot, mcx, tuple);
                let nd = {
                    let mut slots =
                        execexpr::EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
                    execexpr::exec_eval_expr(&mut *state, &mut slots)?
                };
                if nd.isnull {
                    exprvals.push(Datum::null());
                    exprnulls.push(true);
                } else {
                    exprvals.push(datum_copy_in(mcx, nd.value, stats.typbyval, stats.typlen)?);
                    exprnulls.push(false);
                }
            }
            let tcnt = rows.len() as i32;
            if tcnt > 0 {
                let src =
                    FetchSource::Expr { vals: &exprvals, nulls: &exprnulls, stride: 1, off: 0 };
                match stats.compute {
                    ComputeStats::Scalar => compute_scalar_stats(
                        mcx,
                        col_cx.mcx(),
                        &mut stats,
                        &src,
                        tcnt,
                        tcnt as f64,
                    )?,
                    ComputeStats::Distinct => compute_distinct_stats(
                        mcx,
                        col_cx.mcx(),
                        &mut stats,
                        &src,
                        tcnt,
                        tcnt as f64,
                    )?,
                    ComputeStats::Trivial => compute_trivial_stats(&mut stats, &src, tcnt)?,
                    ComputeStats::Range { is_multirange } => {
                        range_typanalyze::compute_range_stats(
                            mcx,
                            &mut stats,
                            is_multirange,
                            &src,
                            tcnt,
                            tcnt as f64,
                        )?
                    }
                    ComputeStats::Array { std, elem_typeid } => {
                        array_typanalyze::compute_array_stats(
                            mcx,
                            col_cx.mcx(),
                            &mut stats,
                            std,
                            elem_typeid,
                            &src,
                            tcnt,
                            tcnt as f64,
                        )?
                    }
                    ComputeStats::TsVector => ts_typanalyze::compute_tsvector_stats(
                        mcx,
                        col_cx.mcx(),
                        &mut stats,
                        &src,
                        tcnt,
                    )?,
                }
                col_cx.reset();
            }
            out.push(expr_stats_row(mcx, &stats)?);
        }
        Ok(out)
    }
}

fn expr_stats_row<'b>(
    mcx: Mcx<'b>,
    stats: &VacAttrStats<'_>,
) -> PgResult<Option<statistics::ExprStatsRow<'b>>> {
    if !stats.stats_valid {
        return Ok(None);
    }
    let mut row = statistics::ExprStatsRow {
        stanullfrac: stats.stanullfrac,
        stawidth: stats.stawidth,
        stadistinct: stats.stadistinct,
        stakind: stats.stakind,
        staop: stats.staop,
        stacoll: stats.stacoll,
        stanumbers: [None, None, None, None, None],
        stavalues: [None, None, None, None, None],
    };
    for k in 0..STATISTIC_NUM_SLOTS {
        if !stats.stanumbers[k].is_empty() {
            let mut dat: PgVec<'b, Datum> =
                mcx::vec_with_capacity_in(mcx, stats.stanumbers[k].len())?;
            dat.extend(stats.stanumbers[k].iter().map(|&f| Datum::from_f32(f)));
            row.stanumbers[k] = Some(datum::array_build::construct_array_image(
                mcx, &dat, FLOAT4OID, 4, true, b'i',
            )?);
        }
        if !stats.stavalues[k].is_empty() {
            row.stavalues[k] = Some(datum::array_build::construct_array_image(
                mcx,
                &stats.stavalues[k],
                stats.statypid[k],
                stats.statyplen[k],
                stats.statypbyval[k],
                stats.statypalign[k],
            )?);
        } else if stats.stavalues_set[k] {
            row.stavalues[k] =
                Some(datum::array_build::construct_empty_array_image(mcx, stats.statypid[k])?);
        }
    }
    Ok(Some(row))
}

fn std_typanalyze(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    if stats.attstattarget < 0 {
        stats.attstattarget = guc_tables::vars::default_statistics_target.read();
    }
    let entry = typcache::lookup_type_cache(
        stats.attrtypid,
        typcache::TYPECACHE_EQ_OPR | typcache::TYPECACHE_LT_OPR,
    )?;
    let eqopr = entry.eq_opr();
    let ltopr = entry.lt_opr();
    stats.extra = StdAnalyzeData { eqopr, ltopr };
    // 300*target sample floor per Chaudhuri/Motwani/Narasayya (analyze.c).
    stats.minrows = 300 * stats.attstattarget;
    if eqopr != InvalidOid && ltopr != InvalidOid {
        stats.compute = ComputeStats::Scalar;
    } else if eqopr != InvalidOid {
        stats.compute = ComputeStats::Distinct;
    } else {
        stats.compute = ComputeStats::Trivial;
    }
    Ok(true)
}

/// Appends up to `targrows` sampled rows; the inherited caller reuses one
/// vec across children, so replacement/sort indexes are relative to the
/// vec length at entry (C's rows + numrows subarray).
fn acquire_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    rows: &mut PgVec<'mcx, HeapTupleData<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
) -> PgResult<i32> {
    debug_assert!(targrows > 0);
    if tableam::TableAm::of(onerel) == Some(tableam::TableAm::Pgrcolumnar) {
        return pgrcolumnar_acquire_sample_rows(mcx, onerel, rows, targrows, totalrows, totaldeadrows);
    }
    let base = rows.len();
    let mut numrows: i32 = 0;
    let mut samplerows = 0.0f64;
    let mut liverows = 0.0f64;
    let mut deadrows = 0.0f64;
    let mut rowstoskip = -1.0f64;

    let totalblocks =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(onerel, ForkNumber::MAIN_FORKNUM)?;
    let oldest_xmin = procarray::GetOldestNonRemovableTransactionId(onerel)?;

    let randseed = pg_prng::global_prng(|p| p.next_u32());
    let (mut bs, nblocks) = sampling::block_sampler_init(totalblocks, targrows as u32, randseed);

    pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL, nblocks as i64);

    let mut rstate =
        sampling::reservoir_init_selection_state(pg_prng::global_prng(|p| p.next_u64()), targrows as u32);

    let mut scan = tableam::table_beginscan_analyze(mcx, onerel)?;
    let mut slot = tableam::table_slot_create(mcx, onerel)?;

    let mut next_buffer = |bs: &mut sampling::BlockSamplerData| -> PgResult<types_core::Buffer> {
        if !bs.has_more() {
            return Ok(types_core::InvalidBuffer);
        }
        bufmgr_seams::read_buffer::call(onerel, bs.next())
    };

    let mut blksdone: i64 = 0;
    loop {
        let buf = next_buffer(&mut bs)?;
        if !tableam::table_scan_analyze_next_block(mcx, &mut scan, &mut || Ok(buf))? {
            break;
        }
        while tableam::table_scan_analyze_next_tuple(
            mcx,
            &mut scan,
            oldest_xmin,
            &mut liverows,
            &mut deadrows,
            &mut slot,
        )? {
            if numrows < targrows {
                rows.push(copy_slot_tuple(mcx, &slot)?);
                numrows += 1;
            } else {
                if rowstoskip < 0.0 {
                    rowstoskip = sampling::reservoir_get_next_s(&mut rstate, samplerows, targrows as u32);
                }
                if rowstoskip <= 0.0 {
                    let k = (targrows as f64 * sampling::sampler_random_fract(&mut rstate.randstate))
                        as usize;
                    debug_assert!(k < targrows as usize);
                    // C heap_freetuple's the replaced copy; here it stays in
                    // the Analyze arena until context teardown.
                    rows[base + k] = copy_slot_tuple(mcx, &slot)?;
                }
                rowstoskip -= 1.0;
            }
            samplerows += 1.0;
        }

        blksdone += 1;
        pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_DONE, blksdone);
    }
    drop(slot);
    tableam::table_endscan(scan)?;

    if numrows == targrows {
        rows[base..].sort_unstable_by_key(|t| {
            (
                types_tuple::ItemPointerGetBlockNumberNoCheck(&t.t_self),
                types_tuple::ItemPointerGetOffsetNumberNoCheck(&t.t_self),
            )
        });
    }

    if bs.m > 0 {
        *totalrows = ((liverows / bs.m as f64) * totalblocks as f64 + 0.5).floor();
        *totaldeadrows = ((deadrows / bs.m as f64) * totalblocks as f64 + 0.5).floor();
    } else {
        *totalrows = 0.0;
        *totaldeadrows = 0.0;
    }

    Ok(numrows)
}

// ---- loadfinal lane knobs (default OFF, fail-closed) -----------------------

// PGRUST_ANALYZE_SAMPLE_POOL=<n>: decode the pgrcolumnar sample's granules on n
// worker threads (0/unset = the serial per-ref gather path, untouched). The
// sample positions and row values are identical either way — the reservoir
// arithmetic is pure PRNG/row-count bookkeeping, run up front.
fn analyze_sample_pool() -> usize {
    std::env::var("PGRUST_ANALYZE_SAMPLE_POOL")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
        .min(64)
}

// PGRUST_ANALYZE_TRACE=1: decomposition trace lines (acquire / compute /
// update walls) to stderr — the load-lane postmaster.log harvest channel.
fn analyze_trace() -> bool {
    matches!(
        std::env::var("PGRUST_ANALYZE_TRACE").ok().as_deref().map(str::trim),
        Some("1") | Some("on") | Some("true")
    )
}

/// The pgrcolumnar reservoir's sample POSITIONS, computed without fetching:
/// exactly `pgrcolumnar_acquire_sample_rows`'s serial loop with the two fetch
/// sites recording (ord, rg, rg-global row) instead of materializing the
/// row — the PRNG call sequence and f64 bookkeeping are verbatim, so the
/// positions (and their post-sort order) are IDENTICAL to what the serial
/// path samples from the same reservoir state. Returned sorted by ord
/// (physical file order — the correlation stat's required rows order).
fn pgrcolumnar_sample_positions(
    rgs: &[(u32, u32)],
    targrows: i32,
    rstate: &mut sampling::ReservoirStateData,
) -> Vec<(u64, u32, u32)> {
    let mut sample: Vec<(u64, u32, u32)> = Vec::new();
    let mut samplerows = 0.0f64;
    let mut rowstoskip = -1.0f64;
    let mut ord: u64 = 0;
    for &(rg, nrows) in rgs {
        let mut row: u32 = 0;
        while row < nrows {
            if (sample.len() as i32) < targrows {
                sample.push((ord, rg, row));
            } else {
                if rowstoskip < 0.0 {
                    rowstoskip =
                        sampling::reservoir_get_next_s(rstate, samplerows, targrows as u32);
                }
                let skip = rowstoskip.ceil();
                let span = (nrows - row) as f64;
                if skip >= span {
                    rowstoskip -= span;
                    samplerows += span;
                    ord += (nrows - row) as u64;
                    break;
                }
                row += skip as u32;
                ord += skip as u64;
                samplerows += skip;
                rowstoskip = -1.0;
                let k = (targrows as f64 * sampling::sampler_random_fract(&mut rstate.randstate))
                    as usize;
                debug_assert!(k < targrows as usize);
                sample[k] = (ord, rg, row);
            }
            row += 1;
            ord += 1;
            samplerows += 1.0;
        }
    }
    sample.sort_unstable_by_key(|&(o, _, _)| o);
    sample
}

// pgrcolumnar's acquirefunc (C table_relation_analyze lets the AM supply it):
// uniform Vitter reservoir over the visible row stream; skipped spans are
// never decoded (gather_row decodes granules on demand). totalrows is exact
// from the part footer; pgrcolumnar parts carry no dead rows.
fn pgrcolumnar_acquire_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    rows: &mut PgVec<'mcx, HeapTupleData<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
) -> PgResult<i32> {
    let tupdesc = onerel.descr();
    let mut scan = tableam::table_beginscan_analyze(mcx, onerel)?;
    let mut slot = tableam::table_slot_create(mcx, onerel)?;
    let rgs = tableam::pgrcolumnar_analyze_visible_rgs(&scan)?;
    let total: u64 = rgs.iter().map(|&(_, n)| n as u64).sum();
    *totalrows = total as f64;
    *totaldeadrows = 0.0;

    pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL, rgs.len() as i64);

    let mut fetch = |scan: &mut tableam::TableScanDesc<'mcx>,
                     slot: &mut SlotData<'mcx>,
                     rg: u32,
                     row: u32|
     -> PgResult<HeapTupleData<'mcx>> {
        if !tableam::pgrcolumnar_analyze_fetch_row(scan, rg, row, slot) {
            panic!("cbstore analyze: sampled row ref out of range");
        }
        let b = slot.base();
        let owned = heaptuple::heap_form_tuple(mcx, tupdesc, &b.tts_values, &b.tts_isnull)?;
        let (ptr, len, tid, oid) = (
            owned.image().as_ptr(),
            owned.as_tuple().t_len,
            owned.as_tuple().t_self,
            owned.as_tuple().t_tableOid,
        );
        core::mem::forget(owned);
        // SAFETY: the image was just formed in `mcx` and, forgotten, lives
        // until that context's teardown; nothing else writes it.
        Ok(unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, oid) })
    };

    let mut rstate = sampling::reservoir_init_selection_state(
        pg_prng::global_prng(|p| p.next_u64()),
        targrows as u32,
    );

    // Parallel acquisition (PGRUST_ANALYZE_SAMPLE_POOL >= 1): sample
    // positions first (pure arithmetic, identical PRNG stream), then decode
    // the sampled granules on the pool. Rows, values, and final order match
    // the serial arm below by construction; positions come back ord-sorted,
    // so tuples append in physical order without the end sort.
    let pool = analyze_sample_pool();
    let trace = analyze_trace();
    let t0 = std::time::Instant::now();
    if pool >= 1 {
        let positions = pgrcolumnar_sample_positions(&rgs, targrows, &mut rstate);
        let refs: Vec<(u32, u32)> =
            positions.iter().map(|&(_, rg, row)| (rg, row)).collect();
        let mut formed: i32 = 0;
        let tasks = {
            let rows = &mut *rows;
            let formed = &mut formed;
            let mut per_row = |slot: &mut SlotData<'_>| -> PgResult<()> {
                let b = slot.base();
                let owned =
                    heaptuple::heap_form_tuple(mcx, tupdesc, &b.tts_values, &b.tts_isnull)?;
                let (ptr, len, tid, oid) = (
                    owned.image().as_ptr(),
                    owned.as_tuple().t_len,
                    owned.as_tuple().t_self,
                    owned.as_tuple().t_tableOid,
                );
                core::mem::forget(owned);
                // SAFETY: as the serial arm's fetch — the image was just
                // formed in `mcx` and, forgotten, lives until that context's
                // teardown; nothing else writes it.
                rows.push(unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, oid) });
                *formed += 1;
                Ok(())
            };
            tableam::pgrcolumnar_analyze_gather_rows(&mut scan, &refs, pool, &mut slot, &mut per_row)?
        };
        drop(slot);
        tableam::table_endscan(scan)?;
        if trace {
            eprintln!(
                "ANALYZE|TRACE|cbstore acquire: pool={} refs={} granule_tasks={} wall={:.3}s",
                pool,
                refs.len(),
                tasks,
                t0.elapsed().as_secs_f64()
            );
        }
        return Ok(formed);
    }

    // (ordinal, tuple): replacements land at random slots; physical order is
    // restored by the final sort (the correlation stat reads rows order).
    let mut sample: Vec<(u64, HeapTupleData<'mcx>)> = Vec::new();
    let mut samplerows = 0.0f64;
    let mut rowstoskip = -1.0f64;
    let mut ord: u64 = 0;
    let mut rgs_done: i64 = 0;
    for &(rg, nrows) in &rgs {
        let mut row: u32 = 0;
        while row < nrows {
            if (sample.len() as i32) < targrows {
                let t = fetch(&mut scan, &mut slot, rg, row)?;
                sample.push((ord, t));
            } else {
                if rowstoskip < 0.0 {
                    rowstoskip =
                        sampling::reservoir_get_next_s(&mut rstate, samplerows, targrows as u32);
                }
                // Skip rowstoskip rows (integer count), then take the next;
                // spans crossing row groups keep the residual skip.
                let skip = rowstoskip.ceil();
                let span = (nrows - row) as f64;
                if skip >= span {
                    rowstoskip -= span;
                    samplerows += span;
                    ord += (nrows - row) as u64;
                    break;
                }
                row += skip as u32;
                ord += skip as u64;
                samplerows += skip;
                rowstoskip = -1.0;
                let k = (targrows as f64 * sampling::sampler_random_fract(&mut rstate.randstate))
                    as usize;
                debug_assert!(k < targrows as usize);
                let t = fetch(&mut scan, &mut slot, rg, row)?;
                sample[k] = (ord, t);
            }
            row += 1;
            ord += 1;
            samplerows += 1.0;
        }
        rgs_done += 1;
        pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_DONE, rgs_done);
    }
    drop(slot);
    tableam::table_endscan(scan)?;

    sample.sort_unstable_by_key(|&(o, _)| o);
    let numrows = sample.len() as i32;
    for (_, t) in sample {
        rows.push(t);
    }
    if trace {
        eprintln!(
            "ANALYZE|TRACE|cbstore acquire: pool=0 refs={} wall={:.3}s",
            numrows,
            t0.elapsed().as_secs_f64()
        );
    }
    Ok(numrows)
}

/// Inherited-pass footer NDV (pgrust-only divergence, the !inh footer-NDV
/// override's parent-level sibling): union the v8 per-column HLL register
/// sketches across every pgrcolumnar leaf child, per parent column matched
/// by name (the convert_tuples_by_name vocabulary). Strictly monotone
/// fallback: any child that is not pgrcolumnar disqualifies the whole
/// override (None -> sampled Duj1, exactly as before); a child without a
/// sketch for a mapped column zeroes that column (0 = unknown, the v2
/// scalar contract); a child with no committed footer has no committed
/// rows and contributes nothing. Register union is lossless (per-bucket
/// maxima), so the parent estimate equals a single sketch built over all
/// children's rows.
fn inherited_pgrcolumnar_footer_ndv<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
) -> PgResult<Option<Vec<u64>>> {
    let table_oids = pg_inherits::find_all_inheritors(mcx, onerel.rd_id, ACCESS_SHARE_LOCK)?;
    let parent_atts = &onerel.rd_att.attrs;
    // Per parent column: the running union (None until first contribution)
    // and a poison flag (a mapped child column without a sketch).
    let mut acc: Vec<Option<tableam::PgrcolumnarNdvSketch>> =
        (0..parent_atts.len()).map(|_| None).collect();
    let mut poisoned = vec![false; parent_atts.len()];
    let mut any_child = false;
    for &child_oid in table_oids.iter() {
        let childrel = table::table_open(mcx, child_oid, NO_LOCK)?;
        match childrel.rd_rel.relkind {
            RELKIND_RELATION | RELKIND_MATVIEW => {}
            _ => {
                // Partitioned intermediates (incl. a partitioned onerel
                // itself) carry no storage.
                table::table_close(childrel, NO_LOCK)?;
                continue;
            }
        }
        if tableam::TableAm::of(&childrel) != Some(tableam::TableAm::Pgrcolumnar) {
            // Mixed hierarchy: keep C-parity sampling for the whole parent.
            table::table_close(childrel, NO_LOCK)?;
            return Ok(None);
        }
        let sketches = tableam::pgrcolumnar_footer_ndv_hll(&childrel)?;
        if let Some(sketches) = sketches {
            any_child = true;
            let child_atts = &childrel.rd_att.attrs;
            for (pi, patt) in parent_atts.iter().enumerate() {
                if patt.attisdropped || poisoned[pi] {
                    continue;
                }
                // Child column by name (attnums may diverge across the
                // hierarchy); pgrcolumnar column index = attnum - 1 (the
                // footer_ndv consumer's existing contract).
                let child_sk = child_atts
                    .iter()
                    .position(|a| {
                        !a.attisdropped && a.attname.name_str() == patt.attname.name_str()
                    })
                    .and_then(|ci| sketches.get(ci))
                    .and_then(|s| s.as_ref());
                match child_sk {
                    Some(sk) => match &mut acc[pi] {
                        Some(u) => u.merge(sk),
                        None => {
                            let mut u = tableam::PgrcolumnarNdvSketch::default();
                            u.merge(sk);
                            acc[pi] = Some(u);
                        }
                    },
                    None => poisoned[pi] = true,
                }
            }
        }
        table::table_close(childrel, NO_LOCK)?;
    }
    if !any_child {
        return Ok(None);
    }
    Ok(Some(
        acc.iter()
            .zip(&poisoned)
            .map(|(u, &p)| match (u, p) {
                (Some(u), false) => u.estimate().max(1),
                _ => 0,
            })
            .collect(),
    ))
}

/// acquire_inherited_sample_rows (analyze.c): the sampled union across all
/// live children, block-proportional, child rows converted to the parent
/// rowtype where the descriptors diverge.
fn acquire_inherited_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    rows: &mut PgVec<'mcx, HeapTupleData<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
) -> PgResult<i32> {
    *totalrows = 0.0;
    *totaldeadrows = 0.0;

    let table_oids =
        pg_inherits::find_all_inheritors(mcx, onerel.rd_id, ACCESS_SHARE_LOCK)?;
    if table_oids.len() < 2 {
        // A child existed when relhassubclass was set but is gone; clear the
        // flag so the inh pass stops firing (safe under our SUE lock).
        xact::CommandCounterIncrement()?;
        tablecmds_seams::set_relation_has_subclass::call(mcx, onerel.rd_id, false)?;
        return Ok(0);
    }

    let mut children: PgVec<'_, (Relation<'mcx>, f64)> =
        PgVec::with_capacity_in(table_oids.len(), mcx);
    let mut totalblocks = 0.0f64;
    for &child_oid in table_oids.iter() {
        let childrel = table::table_open(mcx, child_oid, NO_LOCK)?;
        match childrel.rd_rel.relkind {
            RELKIND_RELATION | RELKIND_MATVIEW => {
                let relpages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                    &childrel,
                    ForkNumber::MAIN_FORKNUM,
                )?;
                totalblocks += relpages as f64;
                children.push((childrel, relpages as f64));
            }
            RELKIND_FOREIGN_TABLE => panic!(
                "acquire_inherited_sample_rows (analyze.c): foreign-table child (FDW lane)"
            ),
            _ => {
                debug_assert!(childrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
                let lmode = if child_oid == onerel.rd_id { NO_LOCK } else { ACCESS_SHARE_LOCK };
                table::table_close(childrel, lmode)?;
            }
        }
    }

    if children.is_empty() {
        return Ok(0);
    }

    pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_TOTAL, children.len() as i64);

    let mut numrows: i32 = 0;
    for (i, (childrel, childblocks)) in children.into_iter().enumerate() {
        pgstat_progress_update_multi_param(
            &[
                PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID,
                PROGRESS_ANALYZE_BLOCKS_DONE,
                PROGRESS_ANALYZE_BLOCKS_TOTAL,
            ],
            &[childrel.rd_id as i64, 0, 0],
        );

        if childblocks > 0.0 {
            let mut childtargrows =
                (targrows as f64 * childblocks / totalblocks).round() as i32;
            childtargrows = childtargrows.min(targrows - numrows);
            if childtargrows > 0 {
                let base = rows.len();
                let mut trows = 0.0f64;
                let mut tdrows = 0.0f64;
                let childrows = acquire_sample_rows(
                    mcx,
                    &childrel,
                    rows,
                    childtargrows,
                    &mut trows,
                    &mut tdrows,
                )?;

                if childrows > 0 && !tupdesc::equalRowTypes(childrel.descr(), onerel.descr()) {
                    if let Some(map) =
                        tupdesc::convert_tuples_by_name(mcx, childrel.descr(), onerel.descr())?
                    {
                        for row in rows[base..].iter_mut() {
                            let newtup = heaptuple::execute_attr_map_tuple(
                                mcx,
                                row,
                                childrel.descr(),
                                onerel.descr(),
                                &map,
                            )?;
                            let (ptr, len, tid, oid) = (
                                newtup.image().as_ptr(),
                                newtup.as_tuple().t_len,
                                newtup.as_tuple().t_self,
                                newtup.as_tuple().t_tableOid,
                            );
                            core::mem::forget(newtup);
                            // SAFETY: the converted image lives in `mcx`
                            // (Analyze arena) until context teardown.
                            *row = unsafe {
                                HeapTupleData::from_raw_parts(ptr, len, tid, oid)
                            };
                        }
                    }
                }
                numrows += childrows;
                *totalrows += trows;
                *totaldeadrows += tdrows;
            }
        }
        table::table_close(childrel, NO_LOCK)?;
        pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_DONE, (i + 1) as i64);
    }
    Ok(numrows)
}

fn copy_slot_tuple<'mcx>(mcx: Mcx<'mcx>, slot: &SlotData<'mcx>) -> PgResult<HeapTupleData<'mcx>> {
    let SlotData::BufferHeap(s) = slot else {
        panic!("acquire_sample_rows: non-buffer slot from analyze scan")
    };
    let src = s.base.tuple.as_ref().expect("stored sample tuple");
    let owned = heaptuple::heap_copytuple(mcx, src)?;
    let (ptr, len, tid, oid) =
        (owned.image().as_ptr(), owned.as_tuple().t_len, owned.as_tuple().t_self, owned.as_tuple().t_tableOid);
    core::mem::forget(owned);
    // SAFETY: the image was just copied into `mcx` and, forgotten, lives until
    // that context's teardown; nothing else writes it.
    Ok(unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, oid) })
}

pub(crate) fn fetch_attr(
    row: &HeapTupleData<'_>,
    attnum: i32,
    tupdesc: &TupleDescData<'_>,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: sampled rows are this relation's tuples under its descriptor.
    let d = unsafe { types_tuple::heap_getattr(row, attnum, tupdesc, &mut isnull) };
    (d, isnull)
}

// VARSIZE_ANY: stored size of any varlena form (toast pointers included).
pub(crate) fn varlena_stored_size(d: Datum) -> usize {
    // SAFETY: non-null varlena datum readable through its header.
    unsafe { types_tuple::varatt::varsize_any(d.as_usize() as *const u8) }
}

fn varlena_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) }
}

// PG_DETOAST_DATUM into `mcx`; the image rides the bump column context until
// its per-column reset, C's col_context cadence.
fn detoast_sample_value<'m>(mcx: Mcx<'m>, raw: &[u8]) -> PgResult<Datum> {
    let img = detoast_seams::detoast_attr::call(mcx, raw)?;
    Ok(Datum::from_usize(adt_multirangetypes::leak_image(img).as_ptr() as usize))
}

fn datum_copy_in<'mcx>(mcx: Mcx<'mcx>, d: Datum, typbyval: bool, typlen: i16) -> PgResult<Datum> {
    if typbyval {
        return Ok(d);
    }
    let len = if typlen > 0 { typlen as usize } else { varlena_stored_size(d) };
    let p = d.as_usize() as *const u8;
    // SAFETY: byref datum addresses `len` live bytes.
    let src = unsafe { core::slice::from_raw_parts(p, len) };
    let copy = mcx::slice_borrow_in(mcx, src)?;
    Ok(Datum::from_usize(copy.as_ptr() as usize))
}

fn compute_trivial_stats(
    stats: &mut VacAttrStats<'_>,
    src: &FetchSource<'_, '_>,
    samplerows: i32,
) -> PgResult<()> {
    let is_varlena = !stats.typbyval && stats.typlen == -1;
    let is_varwidth = !stats.typbyval && stats.typlen < 0;
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut total_width = 0.0f64;
    for rowno in 0..samplerows as usize {
        let (value, isnull) = src.fetch(rowno, stats.tupattnum);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;
        if is_varlena {
            total_width += varlena_stored_size(value) as f64;
        } else if is_varwidth {
            panic!("compute_trivial_stats (analyze.c): cstring-width type lane");
        }
    }
    if nonnull_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = if is_varwidth {
            (total_width / nonnull_cnt as f64) as i32
        } else {
            stats.typlen as i32
        };
        stats.stadistinct = 0.0;
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = if is_varwidth { 0 } else { stats.typlen as i32 };
        stats.stadistinct = 0.0;
    }
    Ok(())
}

// C's track array: descending-count prefix, then count-1 entries in reverse
// insertion order; a new value enters at the head of the count-1 run, and the
// bottommost (oldest) singleton drops off when the list is full.
fn distinct_track_update(
    track: &mut PgVec<'_, (Datum, i32)>,
    track_max: usize,
    value: Datum,
    datum_eq: &mut dyn FnMut(Datum, Datum) -> bool,
) {
    let mut firstcount1 = track.len();
    let mut matched = None;
    for j in 0..track.len() {
        if datum_eq(value, track[j].0) {
            matched = Some(j);
            break;
        }
        if j < firstcount1 && track[j].1 == 1 {
            firstcount1 = j;
        }
    }
    match matched {
        Some(mut j) => {
            track[j].1 += 1;
            while j > 0 && track[j].1 > track[j - 1].1 {
                track.swap(j, j - 1);
                j -= 1;
            }
        }
        None => {
            if track.len() < track_max {
                track.push((Datum::null(), 0));
            }
            let mut j = track.len();
            while j > firstcount1 + 1 {
                track[j - 1] = track[j - 2];
                j -= 1;
            }
            if firstcount1 < track.len() {
                track[firstcount1] = (value, 1);
            }
        }
    }
}

fn compute_distinct_stats<'mcx>(
    anl_mcx: Mcx<'mcx>,
    col_mcx: Mcx<'_>,
    stats: &mut VacAttrStats<'mcx>,
    src: &FetchSource<'_, '_>,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let is_varlena = !stats.typbyval && stats.typlen == -1;
    let is_varwidth = !stats.typbyval && stats.typlen < 0;
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut toowide_cnt = 0i32;
    let mut total_width = 0.0f64;
    let mut num_mcv = stats.attstattarget;
    let track_max = (2 * num_mcv).max(10) as usize;
    let mut track: PgVec<'_, (Datum, i32)> = mcx::vec_with_capacity_in(col_mcx, track_max)?;

    let eqfunc = lsyscache::get_opcode(stats.extra.eqopr)?;
    let mut f_cmpeq = fmgr_seams::fmgr_info::call(eqfunc)?;
    let collation = stats.attrcollid;
    let mut datum_eq = |a: Datum, b: Datum| -> bool {
        types_fmgr::function_call2_coll_in(&mut f_cmpeq, collation, col_mcx, a, b)
            .unwrap_or_else(|e| panic!("compute_distinct_stats: equality failed: {e:?}"))
            .as_bool()
    };

    for rowno in 0..samplerows as usize {
        let (mut value, isnull) = src.fetch(rowno, stats.tupattnum);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;
        if is_varlena {
            let raw = varlena_image(value);
            total_width += raw.len() as f64;
            if detoast::toast_raw_datum_size(raw) > WIDTH_THRESHOLD {
                toowide_cnt += 1;
                continue;
            }
            if raw[0] & 0x03 != 0 {
                value = detoast_sample_value(col_mcx, raw)?;
            }
        } else if is_varwidth {
            panic!("compute_distinct_stats (analyze.c): cstring-width type lane");
        }
        distinct_track_update(&mut track, track_max, value, &mut datum_eq);
    }

    if nonnull_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = if is_varwidth {
            (total_width / nonnull_cnt as f64) as i32
        } else {
            stats.typlen as i32
        };

        let track_cnt = track.len() as i32;
        let mut nmultiple = 0i32;
        let mut summultiple = 0i32;
        for &(_, count) in track.iter() {
            if count == 1 {
                break;
            }
            nmultiple += 1;
            summultiple += count;
        }

        if nmultiple == 0 {
            stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
        } else if track_cnt < track_max as i32 && toowide_cnt == 0 && nmultiple == track_cnt {
            stats.stadistinct = track_cnt as f32;
        } else {
            // Haas-Stokes Duj1: n*d / (n - f1 + f1*n/N); too-wide values count
            // as once-seen through nonnull_cnt.
            let f1 = (nonnull_cnt - summultiple) as f64;
            let d = f1 + nmultiple as f64;
            let n = (samplerows - null_cnt) as f64;
            let n_total = totalrows * (1.0 - stats.stanullfrac as f64);
            let mut stadistinct = if n_total > 0.0 {
                (n * d) / ((n - f1) + f1 * n / n_total)
            } else {
                0.0
            };
            if stadistinct < d {
                stadistinct = d;
            }
            if stadistinct > n_total {
                stadistinct = n_total;
            }
            stats.stadistinct = (stadistinct + 0.5).floor() as f32;
        }
        if stats.stadistinct as f64 > 0.1 * totalrows {
            stats.stadistinct = -(stats.stadistinct as f64 / totalrows) as f32;
        }

        // MCV completeness cut differs from compute_scalar_stats: judged by
        // the track list never filling, not by track_cnt == ndistinct.
        if track_cnt < track_max as i32
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
                let mut mcv_counts: PgVec<'_, i32> =
                    mcx::vec_with_capacity_in(col_mcx, num_mcv as usize)?;
                for &(_, count) in track.iter().take(num_mcv as usize) {
                    mcv_counts.push(count);
                }
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

        if num_mcv > 0 {
            let mut mcv_values: PgVec<'mcx, Datum> =
                mcx::vec_with_capacity_in(anl_mcx, num_mcv as usize)?;
            let mut mcv_freqs: PgVec<'mcx, f32> =
                mcx::vec_with_capacity_in(anl_mcx, num_mcv as usize)?;
            for &(value, count) in track.iter().take(num_mcv as usize) {
                mcv_values.push(datum_copy_in(anl_mcx, value, stats.typbyval, stats.typlen)?);
                mcv_freqs.push(count as f32 / samplerows as f32);
            }
            stats.stakind[0] = STATISTIC_KIND_MCV;
            stats.staop[0] = stats.extra.eqopr;
            stats.stacoll[0] = stats.attrcollid;
            stats.stanumbers[0] = mcv_freqs;
            stats.stavalues[0] = mcv_values;
            stats.stavalues_set[0] = true;
        }
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = if is_varwidth { 0 } else { stats.typlen as i32 };
        stats.stadistinct = 0.0;
    }
    Ok(())
}

struct ScalarMCVItem {
    first: i32,
    count: i32,
}

// C's fetchfunc pair (std_fetch_func / ind_fetch_func).
pub(crate) enum FetchSource<'a, 'd> {
    Heap {
        tupdesc: &'a TupleDescData<'d>,
        rows: &'a [HeapTupleData<'d>],
    },
    Expr {
        vals: &'a [Datum],
        nulls: &'a [bool],
        stride: usize,
        off: usize,
    },
}

impl FetchSource<'_, '_> {
    pub(crate) fn fetch(&self, rowno: usize, attnum: i32) -> (Datum, bool) {
        match self {
            FetchSource::Heap { tupdesc, rows } => fetch_attr(&rows[rowno], attnum, tupdesc),
            FetchSource::Expr { vals, nulls, stride, off } => {
                let i = rowno * stride + off;
                (vals[i], nulls[i])
            }
        }
    }
}

fn compute_scalar_stats<'mcx>(
    anl_mcx: Mcx<'mcx>,
    col_mcx: Mcx<'_>,
    stats: &mut VacAttrStats<'mcx>,
    src: &FetchSource<'_, '_>,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let is_varlena = !stats.typbyval && stats.typlen == -1;
    let is_varwidth = !stats.typbyval && stats.typlen < 0;
    let mut null_cnt = 0i32;
    let mut nonnull_cnt = 0i32;
    let mut toowide_cnt = 0i32;
    let mut total_width = 0.0f64;
    let num_mcv0 = stats.attstattarget;
    let num_bins = stats.attstattarget;

    let mut values: PgVec<'_, (Datum, i32)> = mcx::vec_with_capacity_in(col_mcx, samplerows as usize)?;
    for rowno in 0..samplerows as usize {
        let (mut value, isnull) = src.fetch(rowno, stats.tupattnum);
        if isnull {
            null_cnt += 1;
            continue;
        }
        nonnull_cnt += 1;
        if is_varlena {
            let raw = varlena_image(value);
            total_width += raw.len() as f64;
            if detoast::toast_raw_datum_size(raw) > WIDTH_THRESHOLD {
                toowide_cnt += 1;
                continue;
            }
            if raw[0] & 0x03 != 0 {
                value = detoast_sample_value(col_mcx, raw)?;
            }
        } else if is_varwidth {
            panic!("compute_scalar_stats (analyze.c): cstring-width type lane");
        }
        let tupno = values.len() as i32;
        values.push((value, tupno));
    }
    let values_cnt = values.len() as i32;

    if values_cnt > 0 {
        let entry = typcache::lookup_type_cache(stats.attrtypid, typcache::TYPECACHE_CMP_PROC_FINFO)?;
        let collation = stats.attrcollid;
        // Finfo copy: comparators may re-enter typcache (range_cmp fn_extra
        // fill), so the entry's RefCell must stay unborrowed across the call.
        let cmp_finfo = core::cell::RefCell::new(entry.cmp_proc_finfo().clone());
        // Armed with col_mcx: packed by-ref args (short numerics) expand there
        // and die at the per-column reset — C's col_context cadence.
        let cmp = |a: Datum, b: Datum| -> core::cmp::Ordering {
            let mut finfo = cmp_finfo.borrow_mut();
            let r = types_fmgr::function_call2_coll_in(&mut finfo, collation, col_mcx, a, b)
                .unwrap_or_else(|e| panic!("compute_scalar_stats: comparison failed: {e:?}"))
                .as_i32();
            r.cmp(&0)
        };
        // C's compare_scalars piggybacks dup detection on the sort via
        // tupnoLink; here an explicit adjacent-equality pass replaces it
        // (identical output, N-1 extra comparisons, cold path).
        // C's qsort_interruptible, NOT std sort: user btree comparators may
        // be intransitive (contrib cube's cube_cmp over mixed-dimension
        // values is) — C produces an arbitrary-but-safe order where std's
        // driver panics with "does not correctly implement a total order".
        qsort::pg_qsort(&mut values, |a, b| {
            match cmp(a.0, b.0) {
                core::cmp::Ordering::Less => -1,
                core::cmp::Ordering::Greater => 1,
                core::cmp::Ordering::Equal => a.1 - b.1,
            }
        });

        let mut corr_xysum = 0.0f64;
        let mut ndistinct = 0i32;
        let mut nmultiple = 0i32;
        let mut dups_cnt = 0i32;
        let mut track: PgVec<'_, ScalarMCVItem> = mcx::vec_with_capacity_in(col_mcx, num_mcv0 as usize)?;
        let mut num_mcv = num_mcv0;
        for i in 0..values_cnt {
            corr_xysum += i as f64 * values[i as usize].1 as f64;
            dups_cnt += 1;
            let group_end = i == values_cnt - 1
                || cmp(values[i as usize].0, values[i as usize + 1].0) != core::cmp::Ordering::Equal;
            if group_end {
                ndistinct += 1;
                if dups_cnt > 1 {
                    nmultiple += 1;
                    if (track.len() as i32) < num_mcv
                        || dups_cnt > track[track.len() - 1].count
                    {
                        if (track.len() as i32) < num_mcv {
                            track.push(ScalarMCVItem { first: 0, count: 0 });
                        }
                        let mut j = track.len() - 1;
                        while j > 0 {
                            if dups_cnt <= track[j - 1].count {
                                break;
                            }
                            track[j] = ScalarMCVItem { first: track[j - 1].first, count: track[j - 1].count };
                            j -= 1;
                        }
                        track[j] = ScalarMCVItem { first: i + 1 - dups_cnt, count: dups_cnt };
                    }
                }
                dups_cnt = 0;
            }
        }
        let track_cnt_all = track.len() as i32;

        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = if is_varwidth {
            (total_width / nonnull_cnt as f64) as i32
        } else {
            stats.typlen as i32
        };

        if nmultiple == 0 {
            stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
        } else if toowide_cnt == 0 && nmultiple == ndistinct {
            stats.stadistinct = ndistinct as f32;
        } else {
            // Haas-Stokes Duj1: n*d / (n - f1 + f1*n/N).
            let f1 = (ndistinct - nmultiple + toowide_cnt) as f64;
            let d = f1 + nmultiple as f64;
            let n = (samplerows - null_cnt) as f64;
            let n_total = totalrows * (1.0 - stats.stanullfrac as f64);
            let mut stadistinct = if n_total > 0.0 {
                (n * d) / ((n - f1) + f1 * n / n_total)
            } else {
                0.0
            };
            if stadistinct < d {
                stadistinct = d;
            }
            if stadistinct > n_total {
                stadistinct = n_total;
            }
            stats.stadistinct = (stadistinct + 0.5).floor() as f32;
        }
        if stats.stadistinct as f64 > 0.1 * totalrows {
            stats.stadistinct = -(stats.stadistinct as f64 / totalrows) as f32;
        }

        let mut track_cnt = track_cnt_all;
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
                let mut mcv_counts: PgVec<'_, i32> = mcx::vec_with_capacity_in(col_mcx, num_mcv as usize)?;
                for item in track.iter().take(num_mcv as usize) {
                    mcv_counts.push(item.count);
                }
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
        track_cnt = num_mcv;
        let _ = track_cnt;

        let mut slot_idx = 0usize;
        if num_mcv > 0 {
            let mut mcv_values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(anl_mcx, num_mcv as usize)?;
            let mut mcv_freqs: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(anl_mcx, num_mcv as usize)?;
            for item in track.iter().take(num_mcv as usize) {
                mcv_values.push(datum_copy_in(
                    anl_mcx,
                    values[item.first as usize].0,
                    stats.typbyval,
                    stats.typlen,
                )?);
                mcv_freqs.push(item.count as f32 / samplerows as f32);
            }
            stats.stakind[slot_idx] = STATISTIC_KIND_MCV;
            stats.staop[slot_idx] = stats.extra.eqopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stanumbers[slot_idx] = mcv_freqs;
            stats.stavalues[slot_idx] = mcv_values;
            stats.stavalues_set[slot_idx] = true;
            slot_idx += 1;
        }

        let mut num_hist = ndistinct - num_mcv;
        if num_hist > num_bins {
            num_hist = num_bins + 1;
        }
        if num_hist >= 2 {
            track.truncate(num_mcv as usize);
            track.sort_unstable_by_key(|it| it.first);

            let nvals = if num_mcv > 0 {
                let mut src = 0usize;
                let mut dest = 0usize;
                let mut j = 0usize;
                let values_cnt = values_cnt as usize;
                while src < values_cnt {
                    let ncopy = if j < num_mcv as usize {
                        let first = track[j].first as usize;
                        if src >= first {
                            src = first + track[j].count as usize;
                            j += 1;
                            continue;
                        }
                        first - src
                    } else {
                        values_cnt - src
                    };
                    values.copy_within(src..src + ncopy, dest);
                    src += ncopy;
                    dest += ncopy;
                }
                dest as i32
            } else {
                values_cnt
            };
            debug_assert!(nvals >= num_hist);

            let mut hist_values: PgVec<'mcx, Datum> =
                mcx::vec_with_capacity_in(anl_mcx, num_hist as usize)?;
            let delta = (nvals - 1) / (num_hist - 1);
            let deltafrac = (nvals - 1) % (num_hist - 1);
            let mut pos = 0i32;
            let mut posfrac = 0i32;
            for _ in 0..num_hist {
                hist_values.push(datum_copy_in(
                    anl_mcx,
                    values[pos as usize].0,
                    stats.typbyval,
                    stats.typlen,
                )?);
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= num_hist - 1 {
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
            }
            stats.stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
            stats.staop[slot_idx] = stats.extra.ltopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stavalues[slot_idx] = hist_values;
            stats.stavalues_set[slot_idx] = true;
            slot_idx += 1;
        }

        if values_cnt > 1 {
            let vc = values_cnt as f64;
            let corr_xsum = (vc - 1.0) * vc / 2.0;
            let corr_x2sum = (vc - 1.0) * vc * (2.0 * vc - 1.0) / 6.0;
            let corr = (vc * corr_xysum - corr_xsum * corr_xsum)
                / (vc * corr_x2sum - corr_xsum * corr_xsum);
            let mut corrs: PgVec<'mcx, f32> = mcx::vec_with_capacity_in(anl_mcx, 1)?;
            corrs.push(corr as f32);
            stats.stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
            stats.staop[slot_idx] = stats.extra.ltopr;
            stats.stacoll[slot_idx] = stats.attrcollid;
            stats.stanumbers[slot_idx] = corrs;
        }
    } else if nonnull_cnt > 0 {
        debug_assert!(nonnull_cnt == toowide_cnt);
        stats.stats_valid = true;
        stats.stanullfrac = null_cnt as f32 / samplerows as f32;
        stats.stawidth = if is_varwidth {
            (total_width / nonnull_cnt as f64) as i32
        } else {
            stats.typlen as i32
        };
        stats.stadistinct = -1.0 * (1.0 - stats.stanullfrac);
    } else if null_cnt > 0 {
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = if is_varwidth { 0 } else { stats.typlen as i32 };
        stats.stadistinct = 0.0;
    }
    Ok(())
}

fn analyze_mcv_list(
    mcv_counts: &[i32],
    mut num_mcv: i32,
    stadistinct: f64,
    stanullfrac: f64,
    samplerows: i32,
    totalrows: f64,
) -> i32 {
    if samplerows as f64 == totalrows || totalrows <= 1.0 {
        return num_mcv;
    }
    let mut ndistinct_table = stadistinct;
    if ndistinct_table < 0.0 {
        ndistinct_table = -ndistinct_table * totalrows;
    }
    let mut sumcount = 0.0f64;
    for &c in &mcv_counts[..num_mcv as usize - 1] {
        sumcount += c as f64;
    }
    while num_mcv > 0 {
        let mut selec = 1.0 - sumcount / samplerows as f64 - stanullfrac;
        selec = selec.clamp(0.0, 1.0);
        let otherdistinct = ndistinct_table - (num_mcv - 1) as f64;
        if otherdistinct > 1.0 {
            selec /= otherdistinct;
        }
        // Hypergeometric continuity-corrected Wald bound (analyze.c).
        let n_total = totalrows;
        let n = samplerows as f64;
        let k = n_total * mcv_counts[num_mcv as usize - 1] as f64 / n;
        let variance = n * k * (n_total - k) * (n_total - n) / (n_total * n_total * (n_total - 1.0));
        let stddev = variance.sqrt();
        if mcv_counts[num_mcv as usize - 1] as f64 > selec * n + 2.0 * stddev + 0.5 {
            break;
        }
        num_mcv -= 1;
        if num_mcv == 0 {
            break;
        }
        sumcount -= mcv_counts[num_mcv as usize - 1] as f64;
    }
    num_mcv
}

fn update_attstats(relid: Oid, inh: bool, vacattrstats: &[VacAttrStats<'_>]) -> PgResult<()> {
    if vacattrstats.is_empty() {
        return Ok(());
    }
    let scratch = MemoryContext::new("update_attstats");
    let mcx = scratch.mcx();
    let sd = table::table_open(mcx, STATISTIC_RELATION_ID, ROW_EXCLUSIVE_LOCK)?;
    let mut indstate: Option<catalog_indexing::CatalogIndexState<'_>> = None;

    for stats in vacattrstats {
        if !stats.stats_valid {
            continue;
        }
        let mut values = [Datum::null(); NATTS_PG_STATISTIC];
        let mut nulls = [false; NATTS_PG_STATISTIC];
        values[ANUM_PG_STATISTIC_STARELID - 1] = Datum::from_oid(relid);
        values[ANUM_PG_STATISTIC_STAATTNUM - 1] = Datum::from_i16(stats.tupattnum as i16);
        values[ANUM_PG_STATISTIC_STAINHERIT - 1] = Datum::from_bool(inh);
        values[ANUM_PG_STATISTIC_STANULLFRAC - 1] = Datum::from_f32(stats.stanullfrac);
        values[ANUM_PG_STATISTIC_STAWIDTH - 1] = Datum::from_i32(stats.stawidth);
        values[ANUM_PG_STATISTIC_STADISTINCT - 1] = Datum::from_f32(stats.stadistinct);
        for k in 0..STATISTIC_NUM_SLOTS {
            values[ANUM_PG_STATISTIC_STAKIND1 - 1 + k] = Datum::from_i16(stats.stakind[k]);
            values[ANUM_PG_STATISTIC_STAOP1 - 1 + k] = Datum::from_oid(stats.staop[k]);
            values[ANUM_PG_STATISTIC_STACOLL1 - 1 + k] = Datum::from_oid(stats.stacoll[k]);
        }
        let mut images: PgVec<'_, PgVec<'_, u8>> = PgVec::new_in(mcx);
        for k in 0..STATISTIC_NUM_SLOTS {
            let i = ANUM_PG_STATISTIC_STANUMBERS1 - 1 + k;
            if !stats.stanumbers[k].is_empty() {
                let mut dat: PgVec<'_, Datum> =
                    mcx::vec_with_capacity_in(mcx, stats.stanumbers[k].len())?;
                dat.extend(stats.stanumbers[k].iter().map(|&f| Datum::from_f32(f)));
                let img = datum::array_build::construct_array_image(mcx, &dat, FLOAT4OID, 4, true, b'i')?;
                values[i] = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
            } else {
                nulls[i] = true;
            }
        }
        for k in 0..STATISTIC_NUM_SLOTS {
            let i = ANUM_PG_STATISTIC_STAVALUES1 - 1 + k;
            if !stats.stavalues[k].is_empty() {
                let img = datum::array_build::construct_array_image(
                    mcx,
                    &stats.stavalues[k],
                    stats.statypid[k],
                    stats.statyplen[k],
                    stats.statypbyval[k],
                    stats.statypalign[k],
                )?;
                values[i] = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
            } else if stats.stavalues_set[k] {
                let img =
                    datum::array_build::construct_empty_array_image(mcx, stats.statypid[k])?;
                values[i] = Datum::from_usize(img.as_ptr() as usize);
                images.push(img);
            } else {
                nulls[i] = true;
            }
        }

        let old = find_stats_tuple(mcx, &sd, relid, stats.tupattnum as i16, inh)?;
        if indstate.is_none() {
            indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &sd)?);
        }
        let ind = indstate.as_mut().expect("opened above");
        match old {
            Some((otid, oldtup)) => {
                let replaces = [true; NATTS_PG_STATISTIC];
                let mut newtup = heaptuple::heap_modify_tuple(
                    mcx,
                    &oldtup,
                    sd.descr(),
                    &values,
                    &nulls,
                    &replaces,
                )?;
                catalog_indexing::CatalogTupleUpdateWithInfo(mcx, &sd, &otid, &mut newtup, ind)?;
            }
            None => {
                let mut stup = heaptuple::heap_form_tuple(mcx, sd.descr(), &values, &nulls)?;
                catalog_indexing::CatalogTupleInsertWithInfo(mcx, &sd, &mut stup, ind)?;
            }
        }
    }
    if let Some(ind) = indstate {
        catalog_indexing::CatalogCloseIndexes(ind)?;
    }
    table::table_close(sd, ROW_EXCLUSIVE_LOCK)?;
    Ok(())
}

type FoundStats<'a> = (types_tuple::ItemPointerData, heaptuple::HeapTuple<'a>);

fn find_stats_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    sd: &Relation<'mcx>,
    relid: Oid,
    attnum: i16,
    inh: bool,
) -> PgResult<Option<FoundStats<'mcx>>> {
    let keys = [
        stat_key(ANUM_PG_STATISTIC_STARELID as i32, F_OIDEQ, Datum::from_oid(relid)),
        stat_key(ANUM_PG_STATISTIC_STAATTNUM as i32, F_INT2EQ, Datum::from_i16(attnum)),
        stat_key(ANUM_PG_STATISTIC_STAINHERIT as i32, F_BOOLEQ, Datum::from_bool(inh)),
    ];
    let mut scan = genam::systable_beginscan(mcx, sd, InvalidOid, false, None, &keys)?;
    let found = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => Some((tup.t_self, heaptuple::heap_copytuple(mcx, tup)?)),
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    Ok(found)
}

// defGetBoolean (define.c).
fn def_get_boolean(def: &types_nodes::parsenodes::DefElem<'_>) -> PgResult<bool> {
    use types_nodes::NodeTag;
    let Some(arg) = def.arg else {
        return Ok(true);
    };
    if arg.node_tag() == NodeTag::T_Integer {
        match arg.as_integer().unwrap().ival {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {}
        }
    } else {
        let sval = match arg.node_tag() {
            NodeTag::T_Float => arg.as_float().unwrap().fval,
            NodeTag::T_Boolean => {
                if arg.as_boolean().unwrap().boolval { "true" } else { "false" }
            }
            NodeTag::T_String => arg.as_string().unwrap().sval,
            t => panic!("defGetBoolean (define.c): {t:?} arg unported (define lane)"),
        };
        if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
            return Ok(true);
        }
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(false);
        }
    }
    Err(PgError::error(format!(
        "{} requires a Boolean value",
        def.defname.unwrap_or("")
    ))
    .into())
}

fn stat_key(attno: i32, func: types_core::primitive::RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

#[cfg(test)]
mod tests {
    use super::{
        analyze_mcv_list, compute_scalar_stats, compute_trivial_stats, distinct_track_update,
        varlena_stored_size, ComputeStats, FetchSource, StdAnalyzeData, VacAttrStats,
        STATISTIC_NUM_SLOTS,
    };
    use datum::Datum;
    use mcx::{Mcx, MemoryContext, PgVec};
    use types_core::InvalidOid;

    fn plain_image(payload: &[u8]) -> Vec<u8> {
        let total = payload.len() + 4;
        let mut image = ((total as u32) << 2).to_ne_bytes().to_vec();
        image.extend_from_slice(payload);
        image
    }

    fn compressed_image(rawsize: u32, payload: &[u8]) -> Vec<u8> {
        let total = payload.len() + 8;
        let mut image = (((total as u32) << 2) | 0x02).to_ne_bytes().to_vec();
        image.extend_from_slice(&(rawsize - 4).to_ne_bytes());
        image.extend_from_slice(payload);
        image
    }

    fn ondisk_image(rawsize: i32, extsize: u32) -> Vec<u8> {
        let mut image = vec![0x01, 18];
        image.extend_from_slice(&rawsize.to_ne_bytes());
        image.extend_from_slice(&extsize.to_ne_bytes());
        image.extend_from_slice(&0xBEEFu32.to_ne_bytes());
        image.extend_from_slice(&0xCAFEu32.to_ne_bytes());
        image
    }

    fn as_datum(image: &[u8]) -> Datum {
        Datum::from_usize(image.as_ptr() as usize)
    }

    fn text_stats<'m>(mcx: Mcx<'m>) -> VacAttrStats<'m> {
        VacAttrStats {
            tupattnum: 1,
            attstattarget: 100,
            attrtypid: 25,
            attrcollid: 100,
            typlen: -1,
            typbyval: false,
            typalign: b'i',
            compute: ComputeStats::Trivial,
            extra: StdAnalyzeData { eqopr: 98, ltopr: 664 },
            minrows: 30000,
            stats_valid: false,
            stanullfrac: 0.0,
            stawidth: 0,
            stadistinct: 0.0,
            stakind: [0; STATISTIC_NUM_SLOTS],
            staop: [InvalidOid; STATISTIC_NUM_SLOTS],
            stacoll: [InvalidOid; STATISTIC_NUM_SLOTS],
            stanumbers: core::array::from_fn(|_| PgVec::new_in(mcx)),
            stavalues: core::array::from_fn(|_| PgVec::new_in(mcx)),
            stavalues_set: [false; STATISTIC_NUM_SLOTS],
            statypid: [InvalidOid; STATISTIC_NUM_SLOTS],
            statyplen: [0; STATISTIC_NUM_SLOTS],
            statypbyval: [false; STATISTIC_NUM_SLOTS],
            statypalign: [0; STATISTIC_NUM_SLOTS],
        }
    }

    #[test]
    fn stored_size_covers_all_varlena_forms() {
        let plain = plain_image(&[7u8; 60]);
        assert_eq!(varlena_stored_size(as_datum(&plain)), 64);
        let short = {
            let mut v = vec![(6u8 << 1) | 0x01];
            v.extend_from_slice(b"short");
            v
        };
        assert_eq!(varlena_stored_size(as_datum(&short)), 6);
        let compressed = compressed_image(900, &[3u8; 100]);
        assert_eq!(varlena_stored_size(as_datum(&compressed)), 108);
        let ondisk = ondisk_image(5000, 3000);
        assert_eq!(varlena_stored_size(as_datum(&ondisk)), 18);
    }

    #[test]
    fn trivial_stats_width_counts_toast_pointers_stored() {
        let cx = MemoryContext::new("trivial toast test");
        let mut stats = text_stats(cx.mcx());
        let compressed = compressed_image(2000, &[9u8; 92]);
        let ondisk = ondisk_image(8000, 5000);
        let vals = [as_datum(&compressed), as_datum(&ondisk)];
        let nulls = [false, false];
        let src = FetchSource::Expr { vals: &vals, nulls: &nulls, stride: 1, off: 0 };
        compute_trivial_stats(&mut stats, &src, 2).unwrap();
        assert!(stats.stats_valid);
        assert_eq!(stats.stawidth, (100 + 18) / 2);
    }

    #[test]
    fn scalar_stats_toowide_toast_pointers_excluded() {
        let anl = MemoryContext::new("scalar toast test");
        let col = MemoryContext::new("scalar toast col");
        let mut stats = text_stats(anl.mcx());
        stats.compute = ComputeStats::Scalar;
        let wide = ondisk_image(5000, 3000);
        let vals = [as_datum(&wide), as_datum(&wide), Datum::null(), as_datum(&wide)];
        let nulls = [false, false, true, false];
        let src = FetchSource::Expr { vals: &vals, nulls: &nulls, stride: 1, off: 0 };
        compute_scalar_stats(anl.mcx(), col.mcx(), &mut stats, &src, 4, 4.0).unwrap();
        assert!(stats.stats_valid);
        assert_eq!(stats.stanullfrac, 0.25);
        assert_eq!(stats.stawidth, 18);
        assert_eq!(stats.stadistinct, -0.75);
        assert_eq!(stats.stakind[0], 0);
    }

    fn run_track(values: &[i32], track_max: usize, cx: &MemoryContext) -> Vec<(i32, i32)> {
        let mut track: PgVec<'_, (Datum, i32)> = PgVec::new_in(cx.mcx());
        let mut eq = |a: Datum, b: Datum| a.as_i32() == b.as_i32();
        for &v in values {
            distinct_track_update(&mut track, track_max, Datum::from_i32(v), &mut eq);
        }
        track.iter().map(|&(d, c)| (d.as_i32(), c)).collect()
    }

    #[test]
    fn distinct_track_orders_by_count_and_evicts_oldest_singleton() {
        let cx = MemoryContext::new("distinct track test");
        // 1 (the oldest singleton) drops off when 3 enters the full list.
        assert_eq!(
            run_track(&[7, 7, 7, 5, 5, 1, 2, 3], 4, &cx),
            vec![(7, 3), (5, 2), (3, 1), (2, 1)]
        );
    }

    #[test]
    fn distinct_track_bubbles_matched_value_up() {
        let cx = MemoryContext::new("distinct track test");
        assert_eq!(run_track(&[1, 2, 2, 2, 1], 10, &cx), vec![(2, 3), (1, 2)]);
    }

    #[test]
    fn distinct_track_full_of_multiples_drops_new_singleton() {
        let cx = MemoryContext::new("distinct track test");
        assert_eq!(
            run_track(&[1, 1, 2, 2, 3, 3, 4], 3, &cx),
            vec![(1, 2), (2, 2), (3, 2)]
        );
    }

    #[test]
    fn mcv_list_kept_whole_when_sample_is_table() {
        let counts = [5000, 3000, 2000];
        assert_eq!(analyze_mcv_list(&counts, 3, 3.0, 0.0, 10000, 10000.0), 3);
    }

    #[test]
    fn mcv_list_drops_insignificant_tail() {
        // 100 distinct, uniform-ish tail: a value seen twice in 30000 of 1e6
        // rows is not significantly above the non-MCV estimate.
        let counts = [900, 2];
        assert_eq!(analyze_mcv_list(&counts, 2, -0.0001 * 1_000_000.0, 0.0, 30000, 1_000_000.0), 1);
    }

    #[test]
    fn mcv_list_keeps_significant_values() {
        let counts = [15000, 9000, 6000];
        assert_eq!(analyze_mcv_list(&counts, 3, 103.0, 0.0, 30000, 1_000_000.0), 3);
    }
}

guc_tables::session_guc_int!(
    DEFAULT_STATISTICS_TARGET,
    default_statistics_target_guc,
    set_default_statistics_target_guc,
    100
);

pub fn init_seams() {
    guc_tables::vars::default_statistics_target.install(guc_tables::GucVarAccessors {
        get: default_statistics_target_guc,
        set: set_default_statistics_target_guc,
    });
    commands_analyze_seams::analyze_rel::set(analyze_rel_seam);
}

#[cfg(test)]
mod sample_positions_tests {
    use super::{pgrcolumnar_sample_positions, sampling};

    // Literal transcription of the SERIAL pgrcolumnar_acquire_sample_rows loop
    // with the two fetch sites recording (ord, rg, row) instead of
    // materializing tuples, ending with the same ord sort — the pre-refactor
    // behavior the parallel path must reproduce position-for-position.
    fn reference_positions(
        rgs: &[(u32, u32)],
        targrows: i32,
        rstate: &mut sampling::ReservoirStateData,
    ) -> Vec<(u64, u32, u32)> {
        let mut sample: Vec<(u64, u32, u32)> = Vec::new();
        let mut samplerows = 0.0f64;
        let mut rowstoskip = -1.0f64;
        let mut ord: u64 = 0;
        for &(rg, nrows) in rgs {
            let mut row: u32 = 0;
            while row < nrows {
                if (sample.len() as i32) < targrows {
                    sample.push((ord, rg, row));
                } else {
                    if rowstoskip < 0.0 {
                        rowstoskip = sampling::reservoir_get_next_s(
                            rstate,
                            samplerows,
                            targrows as u32,
                        );
                    }
                    let skip = rowstoskip.ceil();
                    let span = (nrows - row) as f64;
                    if skip >= span {
                        rowstoskip -= span;
                        samplerows += span;
                        ord += (nrows - row) as u64;
                        break;
                    }
                    row += skip as u32;
                    ord += skip as u64;
                    samplerows += skip;
                    rowstoskip = -1.0;
                    let k = (targrows as f64
                        * sampling::sampler_random_fract(&mut rstate.randstate))
                        as usize;
                    sample[k] = (ord, rg, row);
                }
                row += 1;
                ord += 1;
                samplerows += 1.0;
            }
        }
        sample.sort_unstable_by_key(|&(o, _, _)| o);
        sample
    }

    fn check(rgs: &[(u32, u32)], targrows: i32, seed: u64) {
        let mut rs_a = sampling::reservoir_init_selection_state(seed, targrows as u32);
        let mut rs_b = sampling::reservoir_init_selection_state(seed, targrows as u32);
        let want = reference_positions(rgs, targrows, &mut rs_a);
        let got = pgrcolumnar_sample_positions(rgs, targrows, &mut rs_b);
        assert_eq!(want, got, "rgs={rgs:?} targrows={targrows} seed={seed}");
        let total: u64 = rgs.iter().map(|&(_, n)| n as u64).sum();
        assert_eq!(got.len() as u64, total.min(targrows as u64));
        // Ascending physical positions, no duplicates.
        for w in got.windows(2) {
            assert!(w[0].0 < w[1].0 && (w[0].1, w[0].2) < (w[1].1, w[1].2));
        }
        // Both PRNG streams consumed identically.
        assert_eq!(rs_a.randstate.next_u64(), rs_b.randstate.next_u64());
    }

    #[test]
    fn positions_match_serial_reference() {
        // Table smaller than / equal to / crossing targrows; uneven RG
        // geometry incl. 1-row RGs and empty-rg-list; many-RG spans where
        // skips cross row groups.
        check(&[], 100, 7);
        check(&[(0, 50)], 100, 1);
        check(&[(0, 100)], 100, 2);
        check(&[(0, 101)], 100, 3);
        check(&[(0, 1), (1, 1), (2, 1)], 2, 4);
        check(&[(0, 65_536), (1, 65_536), (2, 40_000)], 300, 5);
        check(&[(0, 9), (1, 65_536), (2, 1), (3, 12_345), (4, 65_536)], 150, 6);
        for seed in [11u64, 22, 33, 44] {
            check(&[(0, 65_536), (1, 65_536), (2, 65_536), (3, 21_111)], 1000, seed);
        }
        // Large-N shape: skips are long relative to RGs.
        check(&[(0, 65_536); 32].iter().enumerate().map(|(i, &(_, n))| (i as u32, n)).collect::<Vec<_>>().as_slice(), 100, 12);
    }
}
