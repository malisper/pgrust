// explain.c / explain_state.c / explain_format.c, M1 lane: text format,
// costs on/off, VERBOSE tlist, ANALYZE/BUFFERS over the ported node set;
// non-text formats, wal/memory/settings and ruleutils deparse are loud.
#![allow(non_snake_case)]

use std::rc::Rc;
use std::time::Instant;

use mcx::Mcx;
use tcop_dest::DestReceiver;
use types_core::instrument::{BufferUsage, INSTRUMENT_BUFFERS, INSTRUMENT_ROWS, INSTRUMENT_TIMER};
use types_core::{Oid, TEXTOID};
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{ExplainStmt, Query};
use types_nodes::plannodes::PlannedStmt;
use types_nodes::{Node, NodeTag};
use types_portal::{ParamListHandle, QueryEnvHandle, CURSOR_OPT_PARALLEL_OK};
use types_slot::{EXEC_FLAG_EXPLAIN_GENERIC, EXEC_FLAG_EXPLAIN_ONLY};
use types_tuple::TupleDescData;
use tupdesc::{CreateTemplateTupleDesc, TupleDescInitEntry};

mod format;
mod node;
mod state;
#[cfg(test)]
mod tests;

pub use format::*;
pub use node::{ExplainNode, ExplainPrintPlan};
pub use define::{defGetBoolean, defGetString};
pub use state::*;

// pg_type.dat pins.
const XMLOID: Oid = 142;
const JSONOID: Oid = 114;

pub(crate) mod gucs {
    use std::sync::atomic::{AtomicBool, Ordering};

    static QUOTE_ALL_IDENTIFIERS: AtomicBool = AtomicBool::new(false);

    pub fn quote_all_identifiers() -> bool {
        QUOTE_ALL_IDENTIFIERS.load(Ordering::Relaxed)
    }

    pub fn set_quote_all_identifiers(v: bool) {
        QUOTE_ALL_IDENTIFIERS.store(v, Ordering::Relaxed);
    }
}

// quote_all_identifiers backing moves to ruleutils when that unit lands
// (GucSlot double-install flags the move).
pub fn init_seams() {
    guc_tables::vars::quote_all_identifiers.install(guc_tables::GucVarAccessors {
        get: gucs::quote_all_identifiers,
        set: gucs::set_quote_all_identifiers,
    });
}

// C signature takes a ParseState; its uses arrive as query_string and
// query_env (error cursor positions are omitted repo-wide).
pub fn ExplainQuery<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ExplainStmt<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    dest: &mut DestReceiver<'mcx>,
) -> PgResult<()> {
    let mut es = NewExplainState(mcx)?;
    ParseExplainOptionList(&mut es, mcx, &stmt.options)?;
    if es.format != EXPLAIN_FORMAT_TEXT {
        // Unported emission arms panic; a panic here unwinds through
        // plpgsql/SPI callers — gate with a clean error at the boundary.
        return Err(elog::ereport(types_error::ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "EXPLAIN {:?} output unported (explain non-text format lane)",
                es.format
            ))
            .into_error()
            .into());
    }

    let query_node = stmt.query.expect("ExplainQuery: stmt->query is NULL");

    // C rewrites stmt->query through the shared pointer and never reads it
    // again outside the plancache-held EXPLAIN EXECUTE path (loud upstream):
    // move the Query out of the node.
    // SAFETY: this call holds the only live access to the ExplainStmt tree.
    let mut query: Query<'mcx> = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
        .expect("ExplainQuery: statement is not an analyzed Query");
    if queryjumble::IsQueryIdEnabled() {
        queryjumble::JumbleQueryDiscard(mcx, &mut query)?;
    }
    // post_parse_analyze_hook: no plugin surface exists.
    let rewritten = rewrite_handler_seams::query_rewrite::call(mcx, query)?;

    ExplainBeginOutput(&mut es);
    if rewritten.is_empty() {
        if es.format == EXPLAIN_FORMAT_TEXT {
            es.str.append_str("Query rewrites to nothing\n")?;
        }
    } else {
        let last = rewritten.len() - 1;
        for (i, q) in rewritten.into_iter().enumerate() {
            ExplainOneQuery(mcx, q, CURSOR_OPT_PARALLEL_OK, &mut es, query_string, params, query_env)?;
            if i != last {
                ExplainSeparatePlans(&mut es)?;
            }
        }
    }
    ExplainEndOutput(&mut es);
    debug_assert_eq!(es.indent, 0);

    let tupdesc = ExplainResultDesc(mcx, stmt)?;
    let mut tstate = exectuples_output::begin_tup_output_tupdesc(mcx, dest, Rc::new(tupdesc))?;
    // SAFETY: StringInfo invariant — only ever whole &str appends.
    let text = unsafe { core::str::from_utf8_unchecked(es.str.as_bytes()) };
    if es.format == EXPLAIN_FORMAT_TEXT {
        exectuples_output::do_text_output_multiline(&mut tstate, mcx, text)?;
    } else {
        exectuples_output::do_text_output_oneline(&mut tstate, mcx, text)?;
    }
    exectuples_output::end_tup_output(tstate)?;
    Ok(())
}

pub fn ExplainResultDesc<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ExplainStmt<'_>,
) -> PgResult<TupleDescData<'mcx>> {
    let mut result_type = TEXTOID;
    for opt_node in stmt.options.iter() {
        let opt = opt_node.as_def_elem().expect("EXPLAIN options are DefElems");
        if opt.defname == Some("format") {
            result_type = match defGetString(mcx, opt)? {
                "xml" => XMLOID,
                "json" => JSONOID,
                _ => TEXTOID,
            };
            // no break: ExplainQuery uses the last value.
        }
    }
    let mut tupdesc = CreateTemplateTupleDesc(mcx, 1)?;
    TupleDescInitEntry(&mut tupdesc, 1, Some("QUERY PLAN"), result_type, -1, 0)?;
    Ok(tupdesc)
}

#[allow(clippy::too_many_arguments)]
fn ExplainOneQuery<'mcx>(
    mcx: Mcx<'mcx>,
    query: Query<'mcx>,
    cursor_options: i32,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
) -> PgResult<()> {
    if query.commandType == CmdType::CMD_UTILITY {
        return ExplainOneUtility(mcx, query.utilityStmt, es, query_string, params, query_env);
    }
    // ExplainOneQuery_hook: no plugin surface exists.
    standard_ExplainOneQuery(mcx, query, cursor_options, es, query_string, params, query_env)
}

#[allow(clippy::too_many_arguments)]
pub fn standard_ExplainOneQuery<'mcx>(
    mcx: Mcx<'mcx>,
    query: Query<'mcx>,
    cursor_options: i32,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
) -> PgResult<()> {
    if es.memory {
        panic!(
            "standard_ExplainOneQuery (explain.c): MEMORY needs \
             MemoryContextMemConsumed accounting (mcxt lane)"
        );
    }
    let bufusage_start = if es.buffers { Some(instrument::pg_buffer_usage()) } else { None };
    let planstart = Instant::now();
    let plan = postgres::simple_query::pg_plan_query(mcx, query, query_string, cursor_options, params)?
        .expect("planner will not cope with utility statements");
    let planduration = planstart.elapsed();
    let bufusage = bufusage_start.map(|start| {
        let mut b = BufferUsage::default();
        instrument::buffer_usage_accum_diff(&mut b, &instrument::pg_buffer_usage(), &start);
        b
    });
    ExplainOnePlan(mcx, plan, es, query_string, params, query_env, planduration, bufusage.as_ref())
}

// "into" (CreateTableAsStmt) callers are loud in the CTAS arm.
fn ExplainOneUtility<'mcx>(
    mcx: Mcx<'mcx>,
    utility_stmt: Option<Node<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
) -> PgResult<()> {
    let Some(stmt) = utility_stmt else {
        return Ok(());
    };
    match stmt.node_tag() {
        NodeTag::T_CreateTableAsStmt => panic!(
            "ExplainOneUtility (explain.c): CREATE TABLE AS arm needs \
             CreateTableAsStmt vocabulary (CTAS lane)"
        ),
        NodeTag::T_DeclareCursorStmt => {
            let options = stmt.as_declare_cursor_stmt().expect("tag checked").options;
            // C copyObject(dcs->query) then rewrites the copy; EXPLAIN never
            // reads dcs->query again (no portal is created), so the Query
            // moves out instead.
            // SAFETY: this call holds the only live access to the
            // DeclareCursorStmt tree.
            let query_node = unsafe {
                stmt.with_mut::<types_nodes::parsenodes::DeclareCursorStmt, _>(|d| {
                    d.query.take()
                })
            }
            .flatten()
            .expect("EXPLAIN DECLARE CURSOR without analyzed query");
            // SAFETY: as above.
            let query: Query<'mcx> = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
                .expect("DECLARE CURSOR query is not an analyzed Query");
            let rewritten = rewrite_handler_seams::query_rewrite::call(mcx, query)?;
            assert!(rewritten.len() == 1, "DECLARE rewrite yielded {} queries", rewritten.len());
            let query = rewritten.into_iter().next().expect("len == 1");
            return ExplainOneQuery(mcx, query, options, es, query_string, params, query_env);
        }
        NodeTag::T_ExecuteStmt => {
            if es.memory {
                panic!(
                    "ExplainOneUtility (explain.c): MEMORY needs \
                     MemoryContextMemConsumed accounting (mcxt lane)"
                );
            }
            let exec_stmt = stmt.as_execute_stmt().expect("tag checked");
            let bufusage_start =
                if es.buffers { Some(instrument::pg_buffer_usage()) } else { None };
            let mut bufusage: Option<BufferUsage> = None;
            return prepare::ExplainExecuteQuery(
                mcx,
                exec_stmt,
                query_string,
                params,
                query_env,
                &mut |pstmt, prepared_query, param_li, planduration, is_last| {
                    // SAFETY: retention contract (dispatch unify_stmt_lifetime
                    // precedent) — the cplan refcount held across this call
                    // pins the registry-'static plan tree past 'mcx; nothing
                    // derived from it escapes the render.
                    let pstmt: &'mcx PlannedStmt<'mcx> = unsafe {
                        core::mem::transmute::<
                            &PlannedStmt<'_>,
                            &'mcx PlannedStmt<'mcx>,
                        >(pstmt)
                    };
                    if bufusage.is_none() {
                        bufusage = bufusage_start.map(|start| {
                            let mut b = BufferUsage::default();
                            instrument::buffer_usage_accum_diff(
                                &mut b,
                                &instrument::pg_buffer_usage(),
                                &start,
                            );
                            b
                        });
                    }
                    ExplainOnePlanRef(
                        mcx,
                        pstmt,
                        es,
                        prepared_query,
                        param_li,
                        query_env,
                        planduration,
                        bufusage.as_ref(),
                    )?;
                    if !is_last {
                        ExplainSeparatePlans(es)?;
                    }
                    Ok(())
                },
            );
        }
        NodeTag::T_NotifyStmt => {
            if es.format == EXPLAIN_FORMAT_TEXT {
                es.str.append_str("NOTIFY\n")?;
            } else {
                format::nontext_gap(es, "ExplainDummyGroup");
            }
        }
        _ => {
            if es.format == EXPLAIN_FORMAT_TEXT {
                es.str.append_str("Utility statements have no plan structure\n")?;
            } else {
                format::nontext_gap(es, "ExplainDummyGroup");
            }
        }
    }
    Ok(())
}

// C's abort path frees an EXPLAIN QueryDesc with the portal context and its
// executor refs via CurrentResourceOwner; the registry entry is owning here,
// so an error or loud panic between create and free must release it or the
// EState's relcache refs leak past AtEOXact (a later DROP TABLE fails 55006).
struct QueryDescOwner(types_portal::QueryDescHandle);

impl QueryDescOwner {
    fn disarm(&mut self) {
        self.0 = types_portal::QueryDescHandle::NULL;
    }
}

impl Drop for QueryDescOwner {
    fn drop(&mut self) {
        if !self.0.is_null() {
            execmain_seams::release_query_desc::call(self.0);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn ExplainOnePlan<'mcx>(
    mcx: Mcx<'mcx>,
    plan: PlannedStmt<'mcx>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    planduration: std::time::Duration,
    bufusage: Option<&BufferUsage>,
) -> PgResult<()> {
    let pstmt: &'mcx PlannedStmt<'mcx> = mcx::alloc_leak_in(mcx, plan)?;
    ExplainOnePlanRef(mcx, pstmt, es, query_string, params, query_env, planduration, bufusage)
}

#[allow(clippy::too_many_arguments)]
fn ExplainOnePlanRef<'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &'mcx PlannedStmt<'mcx>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    planduration: std::time::Duration,
    bufusage: Option<&BufferUsage>,
) -> PgResult<()> {
    debug_assert!(pstmt.commandType != CmdType::CMD_UTILITY);

    let mut instrument_option = 0;
    if es.analyze && es.timing {
        instrument_option |= INSTRUMENT_TIMER;
    } else if es.analyze {
        instrument_option |= INSTRUMENT_ROWS;
    }
    if es.buffers {
        instrument_option |= INSTRUMENT_BUFFERS;
    }
    if es.wal {
        panic!(
            "ExplainOnePlan (explain.c): WAL needs pgWalUsage counters + \
             show_wal_usage (xloginsert lane)"
        );
    }

    // C: statement-level timing is always collected for SUMMARY, even with
    // node-level TIMING OFF.
    let mut starttime = instrument::instr_time_current();
    let mut totaltime = 0.0f64;

    snapmgr::PushCopiedSnapshot(&snapmgr::GetActiveSnapshot())?;
    snapmgr::UpdateActiveSnapshotCommandId()?;

    // C: into's CreateIntoRelDestReceiver arm is loud upstream (no CTAS lane).
    let cmd_dest = if es.serialize != EXPLAIN_SERIALIZE_NONE {
        types_dest::CommandDest::ExplainSerialize
    } else {
        types_dest::CommandDest::None
    };
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        query_string,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        cmd_dest,
        params,
        query_env,
        instrument_option,
    )?;
    let mut qd_owner = QueryDescOwner(qd);
    let mut eflags = if es.analyze { 0 } else { EXEC_FLAG_EXPLAIN_ONLY };
    if es.generic {
        eflags |= EXEC_FLAG_EXPLAIN_GENERIC;
    }
    execmain_seams::executor_start::call(qd, eflags)?;

    let mut serializeMetrics = explain_dr::SerializeMetrics::default();
    if es.analyze {
        // CTAS WITH NO DATA's NoMovement arm is loud upstream (no CTAS lane).
        let mut dest = if es.serialize != EXPLAIN_SERIALIZE_NONE {
            DestReceiver::ExplainSerialize(explain_dr::CreateExplainSerializeDestReceiver(
                mcx,
                es.serialize == EXPLAIN_SERIALIZE_BINARY,
                es.timing,
                es.buffers,
            ))
        } else {
            DestReceiver::DoNothing
        };
        execmain_seams::executor_run::call(
            qd,
            types_scan::sdir::ScanDirection::ForwardScanDirection,
            0,
            &mut dest,
        )?;
        execmain_seams::executor_finish::call(qd)?;
        // C: GetSerializationMetrics(dest) before dest->rDestroy(dest); the
        // IntoRel else-arm (all-zero metrics) is the initializer above.
        if let DestReceiver::ExplainSerialize(dr) = &dest {
            serializeMetrics = dr.metrics;
        }
        totaltime += elapsed_time(&starttime);
    }

    ExplainOpenGroup("Query", None, true, es);
    es.qd = qd;
    ExplainPrintPlan(mcx, es, pstmt)?;
    es.qd = types_portal::QueryDescHandle::NULL;

    if bufusage.is_some_and(|bu| peek_buffer_usage(es, bu)) {
        ExplainOpenGroup("Planning", Some("Planning"), true, es);
        ExplainIndentText(es);
        es.str.append_str("Planning:\n")?;
        es.indent += 1;
        show_buffer_usage(es, bufusage.expect("peeked above"));
        es.indent -= 1;
        ExplainCloseGroup("Planning", Some("Planning"), true, es);
    }

    if es.summary {
        ExplainPropertyFloat("Planning Time", Some("ms"), 1000.0 * planduration.as_secs_f64(), 3, es);
    }

    // ExplainPrintTriggers: no CREATE TRIGGER path exists, so every result
    // relation's ri_TrigDesc is provably absent and report_triggers emits
    // nothing; the Triggers Open/CloseGroup pair is a text-format no-op.

    // ExplainPrintJITSummary: jitFlags is pinned 0 by the planner lane, so
    // C's PGJIT_PERFORM check makes it a no-op.
    if es.costs && pstmt.jitFlags != 0 {
        panic!("ExplainPrintJITSummary (explain.c): JIT display unported (jit lane)");
    }

    if es.serialize != EXPLAIN_SERIALIZE_NONE {
        ExplainPrintSerialize(es, &serializeMetrics);
    }

    starttime = instrument::instr_time_current();
    execmain_seams::executor_end::call(qd)?;
    qd_owner.disarm();
    execmain_seams::free_query_desc::call(qd);
    snapmgr::PopActiveSnapshot()?;
    if es.analyze {
        xact::CommandCounterIncrement()?;
    }
    totaltime += elapsed_time(&starttime);

    if es.summary && es.analyze {
        ExplainPropertyFloat("Execution Time", Some("ms"), 1000.0 * totaltime, 3, es);
    }
    ExplainCloseGroup("Query", None, true, es);
    Ok(())
}

// explain.c elapsed_time().
fn elapsed_time(starttime: &types_core::instrument::instr_time) -> f64 {
    let mut endtime = instrument::instr_time_current();
    endtime.subtract(*starttime);
    endtime.get_double()
}

// explain.c BYTES_TO_KILOBYTES.
fn bytes_to_kilobytes(b: u64) -> u64 {
    (b + 1023) / 1024
}

// explain.c ExplainPrintSerialize.
fn ExplainPrintSerialize(es: &mut ExplainState<'_>, metrics: &explain_dr::SerializeMetrics) {
    let format = if es.serialize == EXPLAIN_SERIALIZE_TEXT {
        "text"
    } else {
        debug_assert_eq!(es.serialize, EXPLAIN_SERIALIZE_BINARY);
        "binary"
    };

    ExplainOpenGroup("Serialization", Some("Serialization"), true, es);

    if es.format == EXPLAIN_FORMAT_TEXT {
        ExplainIndentText(es);
        if es.timing {
            append!(
                es,
                "Serialization: time={:.3} ms  output={}kB  format={}\n",
                1000.0 * metrics.timeSpent.get_double(),
                bytes_to_kilobytes(metrics.bytesSent),
                format
            );
        } else {
            append!(
                es,
                "Serialization: output={}kB  format={}\n",
                bytes_to_kilobytes(metrics.bytesSent),
                format
            );
        }
        if es.buffers && peek_buffer_usage(es, &metrics.bufferUsage) {
            es.indent += 1;
            show_buffer_usage(es, &metrics.bufferUsage);
            es.indent -= 1;
        }
    } else {
        if es.timing {
            ExplainPropertyFloat("Time", Some("ms"), 1000.0 * metrics.timeSpent.get_double(), 3, es);
        }
        ExplainPropertyUInteger("Output Volume", Some("kB"), bytes_to_kilobytes(metrics.bytesSent), es);
        ExplainPropertyText("Format", format, es);
        if es.buffers {
            show_buffer_usage(es, &metrics.bufferUsage);
        }
    }

    ExplainCloseGroup("Serialization", Some("Serialization"), true, es);
}

pub(crate) fn peek_buffer_usage(es: &ExplainState<'_>, usage: &BufferUsage) -> bool {
    if es.format != EXPLAIN_FORMAT_TEXT {
        return true;
    }
    buffer_usage_flags(usage) != (false, false, false, false, false, false)
}

fn buffer_usage_flags(u: &BufferUsage) -> (bool, bool, bool, bool, bool, bool) {
    (
        u.shared_blks_hit > 0
            || u.shared_blks_read > 0
            || u.shared_blks_dirtied > 0
            || u.shared_blks_written > 0,
        u.local_blks_hit > 0
            || u.local_blks_read > 0
            || u.local_blks_dirtied > 0
            || u.local_blks_written > 0,
        u.temp_blks_read > 0 || u.temp_blks_written > 0,
        !u.shared_blk_read_time.is_zero() || !u.shared_blk_write_time.is_zero(),
        !u.local_blk_read_time.is_zero() || !u.local_blk_write_time.is_zero(),
        !u.temp_blk_read_time.is_zero() || !u.temp_blk_write_time.is_zero(),
    )
}

pub(crate) fn show_buffer_usage(es: &mut ExplainState<'_>, usage: &BufferUsage) {
    if es.format != EXPLAIN_FORMAT_TEXT {
        format::nontext_gap(es, "show_buffer_usage");
    }
    let (has_shared, has_local, has_temp, has_shared_timing, has_local_timing, has_temp_timing) =
        buffer_usage_flags(usage);

    if has_shared || has_local || has_temp {
        ExplainIndentText(es);
        append!(es, "Buffers:");
        if has_shared {
            append!(es, " shared");
            if usage.shared_blks_hit > 0 {
                append!(es, " hit={}", usage.shared_blks_hit);
            }
            if usage.shared_blks_read > 0 {
                append!(es, " read={}", usage.shared_blks_read);
            }
            if usage.shared_blks_dirtied > 0 {
                append!(es, " dirtied={}", usage.shared_blks_dirtied);
            }
            if usage.shared_blks_written > 0 {
                append!(es, " written={}", usage.shared_blks_written);
            }
            if has_local || has_temp {
                append!(es, ",");
            }
        }
        if has_local {
            append!(es, " local");
            if usage.local_blks_hit > 0 {
                append!(es, " hit={}", usage.local_blks_hit);
            }
            if usage.local_blks_read > 0 {
                append!(es, " read={}", usage.local_blks_read);
            }
            if usage.local_blks_dirtied > 0 {
                append!(es, " dirtied={}", usage.local_blks_dirtied);
            }
            if usage.local_blks_written > 0 {
                append!(es, " written={}", usage.local_blks_written);
            }
            if has_temp {
                append!(es, ",");
            }
        }
        if has_temp {
            append!(es, " temp");
            if usage.temp_blks_read > 0 {
                append!(es, " read={}", usage.temp_blks_read);
            }
            if usage.temp_blks_written > 0 {
                append!(es, " written={}", usage.temp_blks_written);
            }
        }
        append!(es, "\n");
    }

    if has_shared_timing || has_local_timing || has_temp_timing {
        ExplainIndentText(es);
        append!(es, "I/O Timings:");
        if has_shared_timing {
            append!(es, " shared");
            if !usage.shared_blk_read_time.is_zero() {
                append!(es, " read={:.3}", usage.shared_blk_read_time.get_millisec());
            }
            if !usage.shared_blk_write_time.is_zero() {
                append!(es, " write={:.3}", usage.shared_blk_write_time.get_millisec());
            }
            if has_local_timing || has_temp_timing {
                append!(es, ",");
            }
        }
        if has_local_timing {
            append!(es, " local");
            if !usage.local_blk_read_time.is_zero() {
                append!(es, " read={:.3}", usage.local_blk_read_time.get_millisec());
            }
            if !usage.local_blk_write_time.is_zero() {
                append!(es, " write={:.3}", usage.local_blk_write_time.get_millisec());
            }
            if has_temp_timing {
                append!(es, ",");
            }
        }
        if has_temp_timing {
            append!(es, " temp");
            if !usage.temp_blk_read_time.is_zero() {
                append!(es, " read={:.3}", usage.temp_blk_read_time.get_millisec());
            }
            if !usage.temp_blk_write_time.is_zero() {
                append!(es, " write={:.3}", usage.temp_blk_write_time.get_millisec());
            }
        }
        append!(es, "\n");
    }
}
