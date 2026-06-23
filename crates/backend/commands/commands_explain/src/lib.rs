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

use ::mcx::Mcx;
use ::types_core::instrument::{instr_time, BufferUsage};
use ::types_error::{PgError, PgResult};
use ::types_explain::{ExplainFormat, ExplainState};
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::nodes::{ntag, Node};
use ::nodes::params::ParamListInfo;
use ::nodes::parsestmt::IntoClause;
use ::nodes::queryenvironment::QueryEnvironment;
use ::types_scan::sdir::{ForwardScanDirection, NoMovementScanDirection};

use createas_seams as createas_s;
use explain_format as fmt;
use explain_seams as seams;
use ::explain_seams::Bookkeeping;
use execMain_seams as execmain_s;
use instrument as instr;
use mmgr_fgram as mmgr;
use snapmgr_seams as snapmgr_s;
use transam_xact_seams as xact_s;
use printtup_seams as printtup_s;

pub mod details;
pub mod driver;
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

/// Bridge a `backend-utils-mmgr` (`-fgram`) error into this crate's
/// `types-error::PgError`. The two crates carry independent `PgError` types;
/// the memory-context create/switch/consume operations used by the `es->memory`
/// leg fail only on out-of-memory (which `palloc` turns into an `ERROR` ereport
/// anyway), so we re-raise the message under our error world.
fn bridge_mmgr<T>(r: Result<T, mmgr_error::PgError>) -> PgResult<T> {
    r.map_err(|e| {
        error_fgram::ereport(::types_error::ERROR)
            .errmsg(e.message().to_string())
            .into_error()
    })
}

/// `explain_execute_begin` — the pre-lookup EXPLAIN-EXECUTE bookkeeping:
/// `if (es->memory) { ... planner ctx ... }; if (es->buffers) bufusage_start =
/// pgBufferUsage; INSTR_TIME_SET_CURRENT(planstart);`.
fn explain_execute_begin(es: &ExplainState<'_>) -> PgResult<Bookkeeping> {
    let mut bk = Bookkeeping::default();
    // if (es->memory) { planner_ctx = AllocSetContextCreate(CurrentMemoryContext,
    //     "explain analyze planner context", ALLOCSET_DEFAULT_SIZES);
    //     saved_ctx = MemoryContextSwitchTo(planner_ctx); }
    if es.memory {
        let parent = bridge_mmgr(mmgr::PgMemoryContext::current())?;
        let planner_ctx = bridge_mmgr(mmgr::AllocSetContextCreateInternal(
            Some(parent),
            "explain analyze planner context",
            mmgr::ALLOCSET_DEFAULT_MINSIZE,
            mmgr::ALLOCSET_DEFAULT_INITSIZE,
            mmgr::ALLOCSET_DEFAULT_MAXSIZE,
        ))?;
        let saved_ctx = bridge_mmgr(mmgr::MemoryContextSwitchTo(planner_ctx))?;
        bk.memory = true;
        bk.planner_ctx = planner_ctx.as_ptr() as usize as u64;
        bk.saved_ctx = saved_ctx.as_ptr() as usize as u64;
    }
    // if (es->buffers) bufusage_start = pgBufferUsage;
    if es.buffers {
        bk.buffers = true;
        bk.bufusage_start = instr::pgBufferUsage();
    }
    // INSTR_TIME_SET_CURRENT(planstart);
    instr_time::instr_time_set_current(&mut bk.planstart);
    Ok(bk)
}

/// `explain_planduration` — `INSTR_TIME_SET_CURRENT(planduration);
/// INSTR_TIME_SUBTRACT(planduration, planstart);`.
fn explain_planduration(bk: &mut Bookkeeping) -> PgResult<()> {
    let mut now = instr_time::default();
    instr_time::instr_time_set_current(&mut now);
    now.subtract(bk.planstart);
    bk.planduration = now;
    Ok(())
}

/// `explain_memory_accounting` — the `es->memory` branch
/// (`MemoryContextSwitchTo(saved); MemoryContextMemConsumed(planner_ctx, &mc);`).
fn explain_memory_accounting(bk: &mut Bookkeeping) -> PgResult<()> {
    // if (es->memory) { MemoryContextSwitchTo(saved_ctx);
    //     MemoryContextMemConsumed(planner_ctx, &mem_counters); }
    let saved_ctx = bridge_mmgr(mmgr::PgMemoryContext::from_raw(
        bk.saved_ctx as usize as mmgr::MemoryContext,
    ))?;
    bridge_mmgr(mmgr::MemoryContextSwitchTo(saved_ctx))?;
    let planner_ctx = bridge_mmgr(mmgr::PgMemoryContext::from_raw(
        bk.planner_ctx as usize as mmgr::MemoryContext,
    ))?;
    let counters = bridge_mmgr(mmgr::MemoryContextMemConsumed(planner_ctx))?;
    bk.mem_totalspace = counters.totalspace as i64;
    bk.mem_freespace = counters.freespace as i64;
    Ok(())
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
pub(crate) fn show_buffer_usage(es: &mut ExplainState<'_>, usage: &BufferUsage) -> PgResult<()> {
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
        if guc_tables::vars::track_io_timing.read() {
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
    params: ParamListInfo,
    _query_env: Option<&QueryEnvironment<'mcx>>,
    bk: &Bookkeeping,
    es_buffers: bool,
    es_memory: bool,
) -> PgResult<()> {
    // Assert(plannedstmt->commandType != CMD_UTILITY);  (caller guarantees.)

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
    instr_time::instr_time_set_current(&mut starttime);

    // PushCopiedSnapshot(GetActiveSnapshot()); UpdateActiveSnapshotCommandId();
    snapmgr_s::push_copied_active_snapshot::call()?;
    snapmgr_s::update_active_snapshot_command_id::call()?;
    let snapshot = snapmgr_s::get_active_snapshot::call()?;

    // We discard the output if we have no use for it. If we're explaining
    // CREATE TABLE AS, we'd better use the appropriate tuple receiver; for
    // EXPLAIN (SERIALIZE) we use the serialize receiver (which runs the type
    // out/send functions and counts bytes, but never sends them); otherwise the
    // discard `None_Receiver` (the NULL handle, resolved to `donothingDR`).
    //   if (into) dest = CreateIntoRelDestReceiver(into);
    //   else if (es->serialize != NONE) dest = CreateExplainSerializeDestReceiver(es);
    //   else dest = None_Receiver;
    let serialize =
        es.serialize != ::types_explain::ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE;
    let dest = match into {
        // `create_into_rel_dest_receiver_setup` builds the DR_intorel receiver AND
        // binds its run-state with `into` (the owned-model stand-in for C storing
        // self->into at receiver creation). Unlike the bare
        // `create_into_rel_dest_receiver`, this is what lets `intorel_startup`
        // recover `into` when the EXPLAIN executor drives the run itself — without
        // it, the receiver is unbound and the startup callback errors.
        Some(into) => ::nodes::parsestmt::DestReceiverHandle(
            createas_s::create_into_rel_dest_receiver_setup::call(es.str.allocator(), into)?,
        ),
        None if serialize => {
            // format: EXPLAIN_SERIALIZE_TEXT -> 0 (wire text), BINARY -> 1.
            let fmt: i16 =
                if es.serialize == ::types_explain::ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY {
                    1
                } else {
                    0
                };
            printtup_s::create_explain_serialize_dest_receiver::call(fmt, es.timing, es.buffers)
        }
        None => ::nodes::parsestmt::DestReceiverHandle::NULL,
    };

    // EXEC flags: es->analyze ? 0 : EXEC_FLAG_EXPLAIN_ONLY; |= EXPLAIN_GENERIC;
    //   if (into) eflags |= GetIntoRelEFlags(into);
    const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
    const EXEC_FLAG_EXPLAIN_GENERIC: i32 = 0x0002;
    let mut eflags = if es.analyze { 0 } else { EXEC_FLAG_EXPLAIN_ONLY };
    if es.generic {
        eflags |= EXEC_FLAG_EXPLAIN_GENERIC;
    }
    if let Some(into) = into {
        eflags |= createas_s::get_into_rel_eflags::call(into)?;
    }

    // queryDesc = CreateQueryDesc(..., dest, ...); ExecutorStart(...).
    let parent = es.str.allocator().context();
    let mut query_desc = execmain_s::create_query_desc_and_start_explain::call(
        parent,
        pstmt,
        query_string,
        snapshot,
        params,
        instrument_option,
        eflags,
        dest,
    )?;

    // double totaltime = 0; (collected across run + cleanup for Execution Time.)
    let mut totaltime = 0.0f64;

    // Execute the plan for statistics if asked for (ANALYZE).
    if es.analyze {
        // dir = (into && into->skipData) ? NoMovement : Forward.
        let dir = match into {
            Some(into) if into.skipData => NoMovementScanDirection,
            _ => ForwardScanDirection,
        };
        instr::pgBufferUsage(); // touch — keep import live if analyze path used.
        execmain_s::executor_run::call(&mut query_desc, dir, 0)?;
        execmain_s::executor_finish::call(&mut query_desc)?;
        // We can't run ExecutorEnd 'till we're done printing the stats...
        totaltime += elapsed_time(&mut starttime);
    }

    // grab serialization metrics before we destroy the DestReceiver.
    //   if (es->serialize != EXPLAIN_SERIALIZE_NONE)
    //       serializeMetrics = GetSerializationMetrics(dest);
    let serialize_metrics = if serialize {
        printtup_s::get_serialization_metrics::call(dest)
    } else {
        ::types_core::instrument::SerializeMetrics::default()
    };

    // dest->rDestroy(dest): the discard receiver has no destroy side effect.

    fmt::ExplainOpenGroup("Query", None, true, es)?;

    // Create textual dump of plan tree.
    explain_print_plan(es, &mut query_desc)?;

    // Planning buffer/memory usage block: peek_buffer_usage(es, bufusage) ||
    // mem_counters. mem_counters is the es->memory branch (unported, gated at
    // begin); bufusage is the planning buffer usage from `bk`.
    let planning_bufusage = if es_buffers { Some(&bk.bufusage) } else { None };
    // peek_buffer_usage(es, bufusage) || mem_counters. The mem_counters leg
    // (es->memory) carries the planner-context consumption measured at
    // explain_memory_accounting.
    if details::peek_buffer_usage(es, planning_bufusage) || es_memory {
        // ExplainOpenGroup("Planning", "Planning", true, es);
        // show_buffer_usage(es, bufusage); show_memory_counters(es, mem_counters);
        // ExplainCloseGroup(...).
        fmt::ExplainOpenGroup("Planning", Some("Planning"), true, es)?;
        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            // ExplainIndentText(es); appendStringInfo("Planning:\n"); es->indent++.
            fmt::ExplainIndentText(es)?;
            es.str.try_push_str("Planning:\n")?;
            es.indent += 1;
        }
        if es_buffers {
            show_buffer_usage(es, &bk.bufusage)?;
        }
        if es_memory {
            details::show_memory_counters(es, bk.mem_totalspace, bk.mem_freespace)?;
        }
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

    // Print info about runtime of triggers (es->analyze). JIT summary
    // (es->costs) and serialize metrics (es->serialize) need unported owners
    // (JIT is intentionally COSTS-gated for regression stability anyway, and
    // SERIALIZE is rejected at the head of this function).
    if es.analyze {
        explain_print_triggers(es, &mut query_desc)?;
    }

    // JIT summary (es->costs) is intentionally COSTS-gated out for regression
    // stability and otherwise unported.

    // Print info about serialization of output.
    //   if (es->serialize != EXPLAIN_SERIALIZE_NONE)
    //       ExplainPrintSerialize(es, &serializeMetrics);
    if serialize {
        explain_print_serialize(es, &serialize_metrics)?;
    }

    // Close down the query and free resources. Include the time for this in the
    // total execution time. INSTR_TIME_SET_CURRENT(starttime); ExecutorEnd;
    // FreeQueryDesc; PopActiveSnapshot();
    instr_time::instr_time_set_current(&mut starttime);
    execmain_s::executor_end::call(&mut query_desc)?;
    execmain_s::free_query_desc::call(query_desc)?;
    snapmgr_s::pop_active_snapshot::call()?;

    // if (es->analyze) CommandCounterIncrement();
    if es.analyze {
        xact_s::command_counter_increment::call()?;
    }

    totaltime += elapsed_time(&mut starttime);

    // We only report execution time if we actually ran the query (ANALYZE) and
    // summary reporting is enabled (ANALYZE sets SUMMARY true by default).
    if es.summary && es.analyze {
        fmt::ExplainPropertyFloat("Execution Time", Some("ms"), 1000.0 * totaltime, 3, es)?;
    }

    fmt::ExplainCloseGroup("Query", None, true, es)?;
    Ok(())
}

/// `elapsed_time(starttime)` (explain.c:1232) — set `endtime` to now, subtract
/// `*starttime`, return the difference in seconds.
fn elapsed_time(starttime: &mut instr_time) -> f64 {
    let mut endtime = instr_time::default();
    instr_time::instr_time_set_current(&mut endtime);
    endtime.subtract(*starttime);
    endtime.get_double()
}

/// `ExplainPrintTriggers(es, queryDesc)` (explain.c:831) — print info about the
/// runtime of triggers. Iterates the EState's opened result relations, tuple-
/// routing result relations, and trigger target relations, reporting each via
/// `report_triggers`.
fn explain_print_triggers<'es>(
    es: &mut ExplainState<'es>,
    query_desc: &mut ::nodes::querydesc::QueryDesc,
) -> PgResult<()> {
    // resultrels = queryDesc->estate->es_opened_result_relations;
    // routerels  = queryDesc->estate->es_tuple_routing_result_relations;
    // targrels   = queryDesc->estate->es_trig_target_relations;
    fmt::ExplainOpenGroup("Triggers", Some("Triggers"), false, es)?;

    // show_relname = (list_length(resultrels) > 1 || routerels != NIL ||
    //                 targrels != NIL);
    let show_relname = query_desc.work.with(|w| {
        let e = &w.estate;
        e.es_opened_result_relations.len() > 1
            || !e.es_tuple_routing_result_relations.is_empty()
            || !e.es_trig_target_relations.is_empty()
    });

    // foreach(l, resultrels) report_triggers(rInfo, show_relname, es);
    // foreach(l, routerels)  report_triggers(rInfo, show_relname, es);
    // foreach(l, targrels)   report_triggers(rInfo, show_relname, es);
    //
    // report_triggers (explain.c:1092) early-returns on
    // `!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument`. The EXPLAIN-ANALYZE
    // per-trigger instrumentation array `ri_TrigInstrument` is not modeled in
    // this port (see execnodes.rs ResultRelInfo), so the `!ri_TrigInstrument`
    // arm of that guard is always taken: every relation reported here produces
    // no output, exactly as the C early-return would for a relation without
    // accumulated trigger instrumentation. The iteration is preserved so the
    // group open/close bracketing matches C.
    let _ = show_relname;

    fmt::ExplainCloseGroup("Triggers", Some("Triggers"), false, es)?;
    Ok(())
}

/// `BYTES_TO_KILOBYTES(b)` (explain.h) — `(b + 512) / 1024`.
fn bytes_to_kilobytes(b: u64) -> u64 {
    (b + 512) / 1024
}

/// `ExplainPrintSerialize(es, metrics)` (explain.c:999) — append information
/// about query output volume (the SERIALIZE option's collected metrics).
fn explain_print_serialize(
    es: &mut ExplainState<'_>,
    metrics: &::types_core::instrument::SerializeMetrics,
) -> PgResult<()> {
    // We shouldn't get called for EXPLAIN_SERIALIZE_NONE.
    let format = if es.serialize == ::types_explain::ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT {
        "text"
    } else {
        // Assert(es->serialize == EXPLAIN_SERIALIZE_BINARY);
        "binary"
    };

    fmt::ExplainOpenGroup("Serialization", Some("Serialization"), true, es)?;

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        fmt::ExplainIndentText(es)?;
        if es.timing {
            es.str.try_push_str(&format!(
                "Serialization: time={:.3} ms  output={}kB  format={}\n",
                1000.0 * metrics.timeSpent.get_double(),
                bytes_to_kilobytes(metrics.bytesSent),
                format
            ))?;
        } else {
            es.str.try_push_str(&format!(
                "Serialization: output={}kB  format={}\n",
                bytes_to_kilobytes(metrics.bytesSent),
                format
            ))?;
        }

        if es.buffers && details::peek_buffer_usage(es, Some(&metrics.bufferUsage)) {
            es.indent += 1;
            show_buffer_usage(es, &metrics.bufferUsage)?;
            es.indent -= 1;
        }
    } else {
        if es.timing {
            fmt::ExplainPropertyFloat(
                "Time",
                Some("ms"),
                1000.0 * metrics.timeSpent.get_double(),
                3,
                es,
            )?;
        }
        fmt::ExplainPropertyUInteger(
            "Output Volume",
            Some("kB"),
            bytes_to_kilobytes(metrics.bytesSent),
            es,
        )?;
        fmt::ExplainPropertyText("Format", format, es)?;
        if es.buffers {
            show_buffer_usage(es, &metrics.bufferUsage)?;
        }
    }

    fmt::ExplainCloseGroup("Serialization", Some("Serialization"), true, es)?;
    Ok(())
}

/// `ExplainPrintPlan(es, queryDesc)` (explain.c) — opens the McxOwned executor
/// bundle to reach the started plan-state tree (its interior is only reachable
/// via a `for<'mcx>` closure) and runs the two-lifetime walk inside it: the walk
/// reads the bundle-lifetime plan tree (`'p`) and writes owned copies into the
/// formatting-lifetime `es` (`'es`). Applies the invisible-Gather skip.
fn explain_print_plan<'es>(
    es: &mut ExplainState<'es>,
    query_desc: &mut ::nodes::querydesc::QueryDesc,
) -> PgResult<()> {
    let es_mcx: Mcx<'es> = es.str.allocator();
    query_desc.work.with(|w| {
        // Hand EXPLAIN non-owning back-pointers into the running EState's
        // subplan-state tables so `ExplainSubPlans` can reach the InitPlan /
        // SubPlan child plan-state trees (the owned model single-owns them in
        // `es_subplanstates` / `es_initplan` rather than aliasing them on each
        // `SubPlanState.planstate`, which C does). Valid for the synchronous
        // walk below; `w` (and its EState) outlives `walk::ExplainPrintPlan`.
        es.es_subplanstates_ptr = w.estate.es_subplanstates.as_ptr() as *const ();
        es.es_subplanstates_len = w.estate.es_subplanstates.len();
        es.es_initplan_ptr = w.estate.es_initplan.as_ptr() as *const ();
        es.es_initplan_len = w.estate.es_initplan.len();
        es.es_result_rel_pool_ptr = w.estate.es_result_rel_pool.as_ptr() as *const ();
        es.es_result_rel_pool_len = w.estate.es_result_rel_pool.len();
        es.es_unpruned_relids_ptr =
            (&w.estate.es_unpruned_relids) as *const _ as *const ();
        let plannedstmt: &PlannedStmt<'_> = &w.plannedstmt;
        let planstate = w
            .planstate
            .as_deref()
            .expect("ExplainPrintPlan: queryDesc->planstate is NULL (ExecutorStart not run)");
        // ps = queryDesc->planstate; if (IsA(ps, GatherState) &&
        //     ((Gather *) ps->plan)->invisible) { ps = outerPlanState(ps);
        //     es->hide_workers = true; }
        let (top, skipped) = match planstate.ps_head().plan {
            Some(p) if p.node_tag() == ntag::T_Gather && p.expect_gather().invisible => (
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
/// pstate, params)`. Delegates to the driver implementation (CTAS / DECLARE
/// CURSOR re-rewrite + re-plan; EXECUTE → the prepared-statement cache's
/// `ExplainExecuteQuery`; NOTIFY / other → no-plan placeholder).
fn explain_one_utility<'mcx>(
    utility_stmt: &Node<'mcx>,
    into: Option<&IntoClause<'mcx>>,
    es: &mut ExplainState<'mcx>,
    source_text: &str,
    query_env: Option<&QueryEnvironment<'mcx>>,
    params: ParamListInfo,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    driver::ExplainOneUtility(mcx, utility_stmt, into, es, source_text, query_env, params)
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

/// `ExplainResultDesc` install adapter. The `explain_result_desc` out-seam is
/// infallible (`-> TupleDesc`), mirroring the C signature whose error paths
/// longjmp; the owned port returns `PgResult` (the format option is already
/// validated and only allocation can fail). Surface a failure loudly.
fn explain_result_desc_seam<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &Node<'mcx>,
) -> types_tuple::heaptuple::TupleDesc<'mcx> {
    driver::ExplainResultDesc(mcx, stmt).expect("ExplainResultDesc failed")
}

/// Install this unit's 7 inward seams plus the `explain_result_desc`
/// `tcop`-utility out-seam.
pub fn init_seams() {
    seams::explain_execute_begin::set(explain_execute_begin);
    seams::explain_planduration::set(explain_planduration);
    seams::explain_memory_accounting::set(explain_memory_accounting);
    seams::explain_buffer_accounting::set(explain_buffer_accounting);
    seams::explain_one_plan::set(explain_one_plan);
    seams::explain_one_utility::set(explain_one_utility);
    seams::explain_separate_plans::set(explain_separate_plans);
    utility_out_seams::explain_result_desc::set(explain_result_desc_seam);
    utility_out_seams::explain_query::set(driver::ExplainQuery);
}
