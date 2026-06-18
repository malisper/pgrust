//! `commands/explain.c` — the structural `EXPLAIN` implementation.
//!
//! This unit owns the 7 inward seams declared in
//! `backend-commands-explain-seams` (the EXPLAIN-EXECUTE driver's bookkeeping +
//! the per-plan/per-utility/separator entries the `prepare.c` `ExplainExecuteQuery`
//! driver calls) and installs them from [`init_seams`].
//!
//! The structural slice ports `ExplainNode` (node-name switch, cost block,
//! Parallel-Aware / Async-Capable / Disabled flags, child recursion) and
//! `ExplainPrintPlan` (set the plan-tree fields, walk the tree). The executor
//! lifecycle inside `ExplainOnePlan` (CreateQueryDesc + ExecutorStart, the
//! ANALYZE run, ExecutorEnd, FreeQueryDesc) routes through the installed
//! `backend-executor-execMain` seams and the snapshot seams; the VERBOSE / qual
//! / relation-named / ANALYZE detail surface (ruleutils deparse + lsyscache +
//! instrumentation) is unported and reaches loud seam-and-panic boundaries that
//! a non-verbose, no-qual structural plan never hits.

#![allow(non_snake_case)]

extern crate alloc;

use mcx::Mcx;
use types_core::instrument::{instr_time, BufferUsage};
use types_error::{PgError, PgResult};
use types_explain::{ExplainFormat, ExplainState};
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::{IntoClause, ParamListInfoHandle};
use types_nodes::queryenvironment::QueryEnvironment;
use types_scan::sdir::ForwardScanDirection;

use backend_commands_explain_format as fmt;
use backend_commands_explain_seams as seams;
use backend_commands_explain_seams::Bookkeeping;
use backend_executor_execMain_seams as execmain_s;
use backend_executor_instrument as instr;
use backend_utils_time_snapmgr_seams as snapmgr_s;

pub mod details;
pub mod scantarget;
pub mod walk;

#[cfg(test)]
mod tests;

// ===========================================================================
// EXPLAIN-EXECUTE bookkeeping seams (prologue / accounting). prepare.c calls
// these.
//
// The C `ExplainExecuteQuery` keeps `instr_time planstart/planduration`,
// `BufferUsage bufusage_start/bufusage`, the planner `MemoryContext` and
// `MemoryContextCounters` on its stack and threads them through `ExplainOnePlan`
// by value / pointer — no handle, no token, no registry. We mirror that: the
// driver owns one [`Bookkeeping`] local and threads it through these seams by
// value / `&mut`.
// ===========================================================================

/// `explain_execute_begin` — the pre-lookup EXPLAIN-EXECUTE bookkeeping:
/// `if (es->memory) { ... planner ctx ... }; if (es->buffers) bufusage_start =
/// pgBufferUsage; INSTR_TIME_SET_CURRENT(planstart);`.
fn explain_execute_begin(es: &ExplainState<'_>) -> PgResult<Bookkeeping> {
    // if (es->memory) { mem_ctx = AllocSetContextCreate(..., "explain analyze
    //     planner context", ...); MemoryContextSwitchTo(mem_ctx); }
    if es.memory {
        panic!(
            "explain_execute_begin: es->memory needs the planner MemoryContext + \
             MemoryContextMemConsumed (unported)"
        );
    }
    let mut bk = Bookkeeping::default();
    // if (es->buffers) bufusage_start = pgBufferUsage;
    if es.buffers {
        bk.buffers = true;
        bk.bufusage_start = instr::pgBufferUsage();
    }
    // INSTR_TIME_SET_CURRENT(planstart);
    portability_instr_time::instr_time_set_current(&mut bk.planstart);
    Ok(bk)
}

/// `explain_planduration` — `INSTR_TIME_SET_CURRENT(planduration);
/// INSTR_TIME_SUBTRACT(planduration, planstart);`.
fn explain_planduration(bk: &mut Bookkeeping) -> PgResult<()> {
    let mut now = instr_time::default();
    portability_instr_time::instr_time_set_current(&mut now);
    now.subtract(bk.planstart);
    bk.planduration = now;
    Ok(())
}

/// `explain_memory_accounting` — the `es->memory` branch
/// (`MemoryContextSwitchTo(saved); MemoryContextMemConsumed(planner_ctx, &mc);`).
fn explain_memory_accounting(_bk: &mut Bookkeeping) -> PgResult<()> {
    panic!(
        "explain_memory_accounting: MemoryContextMemConsumed / planner context \
         restore (es->memory) unported"
    );
}

/// `explain_buffer_accounting` — the `es->buffers` branch
/// (`BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);`).
fn explain_buffer_accounting(bk: &mut Bookkeeping) -> PgResult<()> {
    let now = instr::pgBufferUsage();
    let mut diff = BufferUsage::default();
    instr::BufferUsageAccumDiff(&mut diff, &now, &bk.bufusage_start);
    bk.bufusage = diff;
    Ok(())
}

/// `show_buffer_usage(es, usage)` (explain.c:4084) — show buffer usage details.
/// Kept in sync with `peek_buffer_usage`. In text format only positive counters
/// are shown; in structured formats every counter is emitted.
fn show_buffer_usage(es: &mut ExplainState<'_>, usage: &BufferUsage) -> PgResult<()> {
    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        let has_shared = usage.shared_blks_hit > 0
            || usage.shared_blks_read > 0
            || usage.shared_blks_dirtied > 0
            || usage.shared_blks_written > 0;
        let has_local = usage.local_blks_hit > 0
            || usage.local_blks_read > 0
            || usage.local_blks_dirtied > 0
            || usage.local_blks_written > 0;
        let has_temp = usage.temp_blks_read > 0 || usage.temp_blks_written > 0;
        let has_shared_timing =
            !usage.shared_blk_read_time.is_zero() || !usage.shared_blk_write_time.is_zero();
        let has_local_timing =
            !usage.local_blk_read_time.is_zero() || !usage.local_blk_write_time.is_zero();
        let has_temp_timing =
            !usage.temp_blk_read_time.is_zero() || !usage.temp_blk_write_time.is_zero();

        // Show only positive counter values.
        if has_shared || has_local || has_temp {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str("Buffers:")?;

            if has_shared {
                es.str.try_push_str(" shared")?;
                if usage.shared_blks_hit > 0 {
                    es.str.try_push_str(&format!(" hit={}", usage.shared_blks_hit))?;
                }
                if usage.shared_blks_read > 0 {
                    es.str
                        .try_push_str(&format!(" read={}", usage.shared_blks_read))?;
                }
                if usage.shared_blks_dirtied > 0 {
                    es.str
                        .try_push_str(&format!(" dirtied={}", usage.shared_blks_dirtied))?;
                }
                if usage.shared_blks_written > 0 {
                    es.str
                        .try_push_str(&format!(" written={}", usage.shared_blks_written))?;
                }
                if has_local || has_temp {
                    es.str.try_push(',')?;
                }
            }
            if has_local {
                es.str.try_push_str(" local")?;
                if usage.local_blks_hit > 0 {
                    es.str.try_push_str(&format!(" hit={}", usage.local_blks_hit))?;
                }
                if usage.local_blks_read > 0 {
                    es.str
                        .try_push_str(&format!(" read={}", usage.local_blks_read))?;
                }
                if usage.local_blks_dirtied > 0 {
                    es.str
                        .try_push_str(&format!(" dirtied={}", usage.local_blks_dirtied))?;
                }
                if usage.local_blks_written > 0 {
                    es.str
                        .try_push_str(&format!(" written={}", usage.local_blks_written))?;
                }
                if has_temp {
                    es.str.try_push(',')?;
                }
            }
            if has_temp {
                es.str.try_push_str(" temp")?;
                if usage.temp_blks_read > 0 {
                    es.str
                        .try_push_str(&format!(" read={}", usage.temp_blks_read))?;
                }
                if usage.temp_blks_written > 0 {
                    es.str
                        .try_push_str(&format!(" written={}", usage.temp_blks_written))?;
                }
            }
            es.str.try_push('\n')?;
        }

        // As above, show only positive counter values.
        if has_shared_timing || has_local_timing || has_temp_timing {
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str("I/O Timings:")?;

            if has_shared_timing {
                es.str.try_push_str(" shared")?;
                if !usage.shared_blk_read_time.is_zero() {
                    es.str.try_push_str(&format!(
                        " read={:.3}",
                        usage.shared_blk_read_time.get_millisec()
                    ))?;
                }
                if !usage.shared_blk_write_time.is_zero() {
                    es.str.try_push_str(&format!(
                        " write={:.3}",
                        usage.shared_blk_write_time.get_millisec()
                    ))?;
                }
                if has_local_timing || has_temp_timing {
                    es.str.try_push(',')?;
                }
            }
            if has_local_timing {
                es.str.try_push_str(" local")?;
                if !usage.local_blk_read_time.is_zero() {
                    es.str.try_push_str(&format!(
                        " read={:.3}",
                        usage.local_blk_read_time.get_millisec()
                    ))?;
                }
                if !usage.local_blk_write_time.is_zero() {
                    es.str.try_push_str(&format!(
                        " write={:.3}",
                        usage.local_blk_write_time.get_millisec()
                    ))?;
                }
                if has_temp_timing {
                    es.str.try_push(',')?;
                }
            }
            if has_temp_timing {
                es.str.try_push_str(" temp")?;
                if !usage.temp_blk_read_time.is_zero() {
                    es.str.try_push_str(&format!(
                        " read={:.3}",
                        usage.temp_blk_read_time.get_millisec()
                    ))?;
                }
                if !usage.temp_blk_write_time.is_zero() {
                    es.str.try_push_str(&format!(
                        " write={:.3}",
                        usage.temp_blk_write_time.get_millisec()
                    ))?;
                }
            }
            es.str.try_push('\n')?;
        }
    } else {
        fmt::ExplainPropertyInteger("Shared Hit Blocks", None, usage.shared_blks_hit, es)?;
        fmt::ExplainPropertyInteger("Shared Read Blocks", None, usage.shared_blks_read, es)?;
        fmt::ExplainPropertyInteger("Shared Dirtied Blocks", None, usage.shared_blks_dirtied, es)?;
        fmt::ExplainPropertyInteger("Shared Written Blocks", None, usage.shared_blks_written, es)?;
        fmt::ExplainPropertyInteger("Local Hit Blocks", None, usage.local_blks_hit, es)?;
        fmt::ExplainPropertyInteger("Local Read Blocks", None, usage.local_blks_read, es)?;
        fmt::ExplainPropertyInteger("Local Dirtied Blocks", None, usage.local_blks_dirtied, es)?;
        fmt::ExplainPropertyInteger("Local Written Blocks", None, usage.local_blks_written, es)?;
        fmt::ExplainPropertyInteger("Temp Read Blocks", None, usage.temp_blks_read, es)?;
        fmt::ExplainPropertyInteger("Temp Written Blocks", None, usage.temp_blks_written, es)?;
        if backend_utils_misc_guc_tables::vars::track_io_timing.read() {
            fmt::ExplainPropertyFloat(
                "Shared I/O Read Time",
                Some("ms"),
                usage.shared_blk_read_time.get_millisec(),
                3,
                es,
            )?;
            fmt::ExplainPropertyFloat(
                "Shared I/O Write Time",
                Some("ms"),
                usage.shared_blk_write_time.get_millisec(),
                3,
                es,
            )?;
            fmt::ExplainPropertyFloat(
                "Local I/O Read Time",
                Some("ms"),
                usage.local_blk_read_time.get_millisec(),
                3,
                es,
            )?;
            fmt::ExplainPropertyFloat(
                "Local I/O Write Time",
                Some("ms"),
                usage.local_blk_write_time.get_millisec(),
                3,
                es,
            )?;
            fmt::ExplainPropertyFloat(
                "Temp I/O Read Time",
                Some("ms"),
                usage.temp_blk_read_time.get_millisec(),
                3,
                es,
            )?;
            fmt::ExplainPropertyFloat(
                "Temp I/O Write Time",
                Some("ms"),
                usage.temp_blk_write_time.get_millisec(),
                3,
                es,
            )?;
        }
    }
    Ok(())
}

// ===========================================================================
// ExplainOnePlan (explain.c:849) — the structural slice.
// ===========================================================================

/// `explain_one_plan` seam — `ExplainOnePlan(pstmt, into, es, queryString,
/// params, queryEnv, &planduration, bufusage?, mem_counters?)`.
#[allow(clippy::too_many_arguments)]
fn explain_one_plan<'mcx>(
    pstmt: &PlannedStmt<'mcx>,
    into: Option<&IntoClause<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListInfoHandle,
    _query_env: Option<&QueryEnvironment<'mcx>>,
    bk: &Bookkeeping,
    es_buffers: bool,
    es_memory: bool,
) -> PgResult<()> {
    // Assert(plannedstmt->commandType != CMD_UTILITY);  (caller guarantees.)

    // CREATE TABLE AS / SERIALIZE use a non-discard receiver; unported here.
    if into.is_some() {
        panic!(
            "explain_one_plan: EXPLAIN ... CREATE TABLE AS (IntoClause) needs \
             CreateIntoRelDestReceiver (unported)"
        );
    }
    if es.serialize != types_explain::ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE {
        panic!("explain_one_plan: SERIALIZE needs CreateExplainSerializeDestReceiver (unported)");
    }

    // if (es->analyze && es->timing) INSTRUMENT_TIMER; else if (es->analyze)
    // INSTRUMENT_ROWS; if (es->buffers) INSTRUMENT_BUFFERS; if (es->wal)
    // INSTRUMENT_WAL;  — INSTRUMENT_* bits.
    const INSTRUMENT_TIMER: i32 = 1 << 0;
    const INSTRUMENT_ROWS: i32 = 1 << 1;
    const INSTRUMENT_BUFFERS: i32 = 1 << 2;
    const INSTRUMENT_WAL: i32 = 1 << 3;
    let mut instrument_option = 0i32;
    if es.analyze && es.timing {
        instrument_option |= INSTRUMENT_TIMER;
    } else if es.analyze {
        instrument_option |= INSTRUMENT_ROWS;
    }
    if es.buffers {
        instrument_option |= INSTRUMENT_BUFFERS;
    }
    if es.wal {
        instrument_option |= INSTRUMENT_WAL;
    }

    // INSTR_TIME_SET_CURRENT(starttime);
    let mut starttime = instr_time::default();
    portability_instr_time::instr_time_set_current(&mut starttime);

    // PushCopiedSnapshot(GetActiveSnapshot()); UpdateActiveSnapshotCommandId();
    snapmgr_s::push_copied_active_snapshot::call()?;
    snapmgr_s::update_active_snapshot_command_id::call()?;
    let snapshot = snapmgr_s::get_active_snapshot::call()?;

    // EXEC flags: es->analyze ? 0 : EXEC_FLAG_EXPLAIN_ONLY; |= EXPLAIN_GENERIC.
    const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
    const EXEC_FLAG_EXPLAIN_GENERIC: i32 = 0x0002;
    let mut eflags = if es.analyze { 0 } else { EXEC_FLAG_EXPLAIN_ONLY };
    if es.generic {
        eflags |= EXEC_FLAG_EXPLAIN_GENERIC;
    }

    // dest = None_Receiver; queryDesc = CreateQueryDesc(...); ExecutorStart(...).
    let parent = es.str.allocator().context();
    let mut query_desc = execmain_s::create_query_desc_and_start_explain::call(
        parent,
        pstmt,
        query_string,
        snapshot,
        params,
        instrument_option,
        eflags,
    )?;

    // Execute the plan for statistics if asked for (ANALYZE).
    if es.analyze {
        // dir = (into && into->skipData) ? NoMovement : Forward; into is None.
        instr::pgBufferUsage(); // touch — keep import live if analyze path used.
        execmain_s::executor_run::call(&mut query_desc, ForwardScanDirection, 0)?;
        execmain_s::executor_finish::call(&mut query_desc)?;
        // totaltime += elapsed_time(&starttime);  (summary path, gated below.)
    }

    // dest->rDestroy(dest): the discard receiver has no destroy side effect.

    fmt::ExplainOpenGroup("Query", None, true, es)?;

    // Create textual dump of plan tree.
    explain_print_plan(es, &mut query_desc)?;

    // Planning buffer/memory usage block: peek_buffer_usage(es, bufusage) ||
    // mem_counters. mem_counters is the es->memory branch (unported, gated at
    // begin); bufusage is the planning buffer usage from `bk`.
    let _ = (es_buffers, es_memory);
    let planning_bufusage = if es_buffers { Some(bk.bufusage) } else { None };
    let peek = planning_bufusage
        .map(|b| b != BufferUsage::default())
        .unwrap_or(false);
    if peek {
        // ExplainOpenGroup("Planning", "Planning", true, es);
        // show_buffer_usage(es, bufusage); ExplainCloseGroup(...).
        fmt::ExplainOpenGroup("Planning", Some("Planning"), true, es)?;
        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            // ExplainIndentText(es); appendStringInfo("Planning:\n"); es->indent++.
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str("Planning:\n")?;
            es.indent += 1;
        }
        show_buffer_usage(es, &bk.bufusage)?;
        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            es.indent -= 1;
        }
        fmt::ExplainCloseGroup("Planning", Some("Planning"), true, es)?;
    }

    // if (es->summary && planduration) Planning Time.
    if es.summary {
        let planduration = bk.planduration;
        let plantime_ms = planduration.get_millisec();
        fmt::ExplainPropertyFloat("Planning Time", Some("ms"), plantime_ms, 3, es)?;
    }

    // ExplainPrintTriggers / ExplainPrintJITSummary / ExplainPrintSerialize:
    // trigger stats (ANALYZE), JIT summary (costs), serialize metrics — all
    // gated. ANALYZE is rejected upstream (ExplainNode panics); JIT/serialize
    // need unported owners. Skip for the structural slice.

    // INSTR_TIME_SET_CURRENT(starttime); ExecutorEnd; FreeQueryDesc;
    // PopActiveSnapshot();
    portability_instr_time::instr_time_set_current(&mut starttime);
    execmain_s::executor_end::call(&mut query_desc)?;
    execmain_s::free_query_desc::call(query_desc)?;
    snapmgr_s::pop_active_snapshot::call()?;

    // if (es->analyze) CommandCounterIncrement(); — ANALYZE rejected upstream.

    // Execution Time (es->summary && es->analyze) — analyze rejected upstream.

    fmt::ExplainCloseGroup("Query", None, true, es)?;
    Ok(())
}

/// `ExplainPrintPlan(es, queryDesc)` (explain.c) — opens the McxOwned executor
/// bundle to reach the started plan-state tree (its interior is only reachable
/// via a `for<'mcx>` closure) and runs the two-lifetime walk inside it: the walk
/// reads the bundle-lifetime plan tree (`'p`) and writes owned copies into the
/// formatting-lifetime `es` (`'es`). Applies the invisible-Gather skip.
fn explain_print_plan<'es>(
    es: &mut ExplainState<'es>,
    query_desc: &mut types_nodes::querydesc::QueryDesc,
) -> PgResult<()> {
    let es_mcx: Mcx<'es> = es.str.allocator();
    query_desc.work.with(|w| {
        let plannedstmt: &PlannedStmt<'_> = &w.plannedstmt;
        let planstate = w
            .planstate
            .as_deref()
            .expect("ExplainPrintPlan: queryDesc->planstate is NULL (ExecutorStart not run)");
        // ps = queryDesc->planstate; if (IsA(ps, GatherState) &&
        //     ((Gather *) ps->plan)->invisible) { ps = outerPlanState(ps);
        //     es->hide_workers = true; }
        let (top, skipped) = match planstate.ps_head().plan {
            Some(Node::Gather(g)) if g.invisible => (
                planstate
                    .outer_plan_state()
                    .expect("invisible Gather without outer plan state"),
                true,
            ),
            _ => (planstate, false),
        };
        walk::ExplainPrintPlan(es, es_mcx, plannedstmt, top, skipped)
    })
}

// ===========================================================================
// ExplainOneUtility / ExplainSeparatePlans
// ===========================================================================

/// `explain_one_utility` seam — `ExplainOneUtility(utilityStmt, into, es,
/// pstate, params)`. The utility-statement EXPLAIN legs (CTAS / DECLARE CURSOR /
/// EXECUTE / CALL) deparse / re-plan through unported owners; reaching one is a
/// loud boundary.
fn explain_one_utility<'mcx>(
    _utility_stmt: &Node<'mcx>,
    _into: Option<&IntoClause<'mcx>>,
    es: &mut ExplainState<'mcx>,
    _source_text: &str,
    _query_env: Option<&QueryEnvironment<'mcx>>,
    _params: ParamListInfoHandle,
) -> PgResult<()> {
    let _ = es;
    panic!(
        "explain_one_utility: ExplainOneUtility (CTAS / DECLARE CURSOR / EXECUTE / \
         CALL legs) needs the utility-statement re-plan/deparse owners (unported)"
    );
}

/// `explain_separate_plans(es)` seam — `ExplainSeparatePlans(es)` (delegates to
/// the format crate's separator).
fn explain_separate_plans<'mcx>(es: &mut ExplainState<'mcx>) -> PgResult<()> {
    fmt::ExplainSeparatePlans(es)
}

// ===========================================================================
// SQL-EXPLAIN driver helpers (ExplainResultDesc / ExplainBeginOutput etc.)
// ===========================================================================

/// `ExplainResultDesc(stmt)` faithful note: builds a single-column tuple
/// descriptor named "QUERY PLAN" whose type is TEXT/XML/JSON per the format
/// option. The `ExplainStmt` parse node and `CreateTemplateTupleDesc` path are
/// unported (no parser), so this is a documented placeholder; the SQL-EXPLAIN
/// entry (`ExplainQuery`) that consumes it is not part of the inward seam set
/// (it is driven by tcop utility, unported).
#[allow(dead_code)]
fn explain_result_desc_note() {}

// Keep the `PgError`/`ExplainFormat` imports live for the seam bodies above.
#[allow(dead_code)]
fn _imports_witness(_e: PgError, _f: ExplainFormat) {}

// ===========================================================================
// init_seams
// ===========================================================================

/// Install this unit's 7 inward seams.
pub fn init_seams() {
    seams::explain_execute_begin::set(explain_execute_begin);
    seams::explain_planduration::set(explain_planduration);
    seams::explain_memory_accounting::set(explain_memory_accounting);
    seams::explain_buffer_accounting::set(explain_buffer_accounting);
    seams::explain_one_plan::set(explain_one_plan);
    seams::explain_one_utility::set(explain_one_utility);
    seams::explain_separate_plans::set(explain_separate_plans);
}
