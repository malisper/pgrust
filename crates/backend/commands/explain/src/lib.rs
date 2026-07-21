// explain.c / explain_state.c / explain_format.c: all four output formats,
// costs on/off, VERBOSE tlist, ANALYZE/BUFFERS over the ported node set;
// wal/memory/settings are loud.
#![allow(non_snake_case)]

use std::rc::Rc;
use std::time::Instant;

use mcx::Mcx;
use tcop_dest::DestReceiver;
use types_core::instrument::{BufferUsage, INSTRUMENT_BUFFERS, INSTRUMENT_ROWS, INSTRUMENT_TIMER};
use types_core::{Oid, TEXTOID};
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{ExplainStmt, ObjectType, Query};
use types_nodes::rawnodes::{CreateTableAsStmt, IntoClause};
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
    guc_tables::session_guc_bool!(
        QUOTE_ALL_IDENTIFIERS,
        quote_all_identifiers,
        set_quote_all_identifiers,
        false
    );
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
    ParseExplainOptionList(&mut es, mcx, &stmt.options, query_string)?;

    let query_node = stmt.query.expect("ExplainQuery: stmt->query is NULL");

    // C copyObject(stmt->query) before QueryRewrite (explain.c): the
    // statement tree lives in the caller's arena (bump MessageContext), but
    // rewrite and planning run under this portal-context mcx and grow the
    // tree's lists in place — List growth deallocates the old cell buffer
    // into the growing context, freeing memory this arena does not own
    // (C's repalloc frees into the chunk's own context; this port must own
    // the tree first).
    let query_node = copyfuncs::copy_object(mcx, query_node)?;
    // SAFETY: fresh copy; this call holds its only live access.
    let mut query: Query<'mcx> = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
        .expect("ExplainQuery: statement is not an analyzed Query");
    if parser_analyze::tap_post_parse_analyze::is_installed() && queryjumble::IsQueryIdEnabled()
    {
        let js = queryjumble::JumbleQuery(mcx, &mut query)?;
        parser_analyze::tap_post_parse_analyze::call_if(|f| f(&mut query, &js, query_string));
    } else if queryjumble::IsQueryIdEnabled() {
        queryjumble::JumbleQueryDiscard(mcx, &mut query)?;
    }
    let rewritten = rewrite_handler_seams::query_rewrite::call(mcx, query)?;

    ExplainBeginOutput(&mut es);
    if rewritten.is_empty() {
        if es.format == EXPLAIN_FORMAT_TEXT {
            es.str.append_str("Query rewrites to nothing\n")?;
        }
    } else {
        let last = rewritten.len() - 1;
        for (i, q) in rewritten.into_iter().enumerate() {
            ExplainOneQuery(mcx, q, CURSOR_OPT_PARALLEL_OK, None, &mut es, query_string, params, query_env)?;
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

// "into" is None unless explaining the contents of a CreateTableAsStmt;
// it is the IntoClause node handle.
#[allow(clippy::too_many_arguments)]
fn ExplainOneQuery<'mcx>(
    mcx: Mcx<'mcx>,
    query: Query<'mcx>,
    cursor_options: i32,
    into: Option<Node<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
) -> PgResult<()> {
    if query.commandType == CmdType::CMD_UTILITY {
        return ExplainOneUtility(mcx, query.utilityStmt, into, es, query_string, params, query_env);
    }
    // ExplainOneQuery_hook: no plugin surface exists.
    standard_ExplainOneQuery(mcx, query, cursor_options, into, es, query_string, params, query_env)
}

#[allow(clippy::too_many_arguments)]
pub fn standard_ExplainOneQuery<'mcx>(
    mcx: Mcx<'mcx>,
    query: Query<'mcx>,
    cursor_options: i32,
    into: Option<Node<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
) -> PgResult<()> {
    // C plans inside a dedicated "explain analyze planner context" AllocSet
    // and reads MemoryContextMemConsumed; the arena analogue is a stats delta
    // over the statement context across planning (values diverge from C's
    // palloc numbers — regress masks them via explain_filter).
    let mem_before = if es.memory { Some(mem_snapshot(mcx)) } else { None };
    let bufusage_start = if es.buffers { Some(instrument::pg_buffer_usage()) } else { None };
    let planstart = Instant::now();
    let plan = postgres::simple_query::pg_plan_query(mcx, mcx::leak_in(mcx::alloc_in(mcx, query)?), query_string, cursor_options, params)?
        .expect("planner will not cope with utility statements");
    let planduration = planstart.elapsed();
    // Step-2 cost-shadow EXPLAIN sample (runtime-cost-model design §5 step
    // 2): take the sample this planning recorded (the planner cleared the
    // slot at entry, so a query that never classified covered yields None).
    // One cached-bool load when PGRUST_M5_COST_EXPLAIN is off.
    if planner::m5_suppress::cost_shadow::explain_armed() {
        es.m5_cost_route = planner::m5_suppress::cost_shadow::take_last_sample();
    }
    let mem_counters = mem_before.map(|b| mem_counters_since(mcx, b));
    let bufusage = bufusage_start.map(|start| {
        let mut b = BufferUsage::default();
        instrument::buffer_usage_accum_diff(&mut b, &instrument::pg_buffer_usage(), &start);
        b
    });
    ExplainOnePlan(
        mcx,
        plan,
        into,
        es,
        query_string,
        params,
        query_env,
        planduration,
        bufusage.as_ref(),
        mem_counters.as_ref(),
    )
}

// MemoryContextCounters (memutils.h) projection: (totalspace, totalspace -
// freespace) come from the statement arena's (footprint, subtree_used) delta.
pub struct MemCounters {
    totalspace: u64,
    freespace: u64,
}

fn mem_snapshot(mcx: Mcx<'_>) -> (usize, usize) {
    let s = mcx.context().stats();
    (s.subtree_used, s.arena_footprint)
}

fn mem_counters_since(mcx: Mcx<'_>, before: (usize, usize)) -> MemCounters {
    let (used0, foot0) = before;
    let (used1, foot1) = mem_snapshot(mcx);
    let used = used1.saturating_sub(used0) as u64;
    let totalspace = (foot1.saturating_sub(foot0) as u64).max(used);
    MemCounters { totalspace, freespace: totalspace - used }
}

// explain.c show_memory_counters.
fn show_memory_counters(es: &mut ExplainState<'_>, mem_counters: &MemCounters) {
    let mem_used_kb = bytes_to_kilobytes(mem_counters.totalspace - mem_counters.freespace);
    let mem_allocated_kb = bytes_to_kilobytes(mem_counters.totalspace);
    if es.format == EXPLAIN_FORMAT_TEXT {
        ExplainIndentText(es);
        append!(es, "Memory: used={mem_used_kb}kB  allocated={mem_allocated_kb}kB\n");
    } else {
        ExplainPropertyUInteger("Memory Used", Some("kB"), mem_used_kb, es);
        ExplainPropertyUInteger("Memory Allocated", Some("kB"), mem_allocated_kb, es);
    }
}

// "into" is None unless explaining the contents of a CreateTableAsStmt.
#[allow(clippy::too_many_arguments)]
fn ExplainOneUtility<'mcx>(
    mcx: Mcx<'mcx>,
    utility_stmt: Option<Node<'mcx>>,
    into: Option<Node<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
) -> PgResult<()> {
    let Some(stmt) = utility_stmt else {
        return Ok(());
    };
    match stmt.node_tag() {
        NodeTag::T_CreateTableAsStmt => {
            let ctas = stmt.as_variant::<CreateTableAsStmt>().expect("tag checked");
            if createas_seams::create_table_as_rel_exists::call(mcx, ctas)? {
                match ctas.objtype {
                    ObjectType::OBJECT_TABLE => {
                        ExplainDummyGroup("CREATE TABLE AS", None, es);
                    }
                    ObjectType::OBJECT_MATVIEW => {
                        ExplainDummyGroup("CREATE MATERIALIZED VIEW", None, es);
                    }
                    other => panic!("unexpected object type: {}", other as i32),
                }
                return Ok(());
            }
            let ctas_into = ctas.into;
            // C copyObject: for matviews into->viewQuery aliases ctas->query
            // in this port, and intorel_startup still reads it under ANALYZE.
            let query_node = copyfuncs::copy_object(
                mcx,
                ctas.query.expect("EXPLAIN CREATE TABLE AS without analyzed query"),
            )?;
            // SAFETY: fresh copy; this call holds its only live access.
            let mut query: Query<'mcx> =
                unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
                    .expect("CTAS query is not an analyzed Query");
            if parser_analyze::tap_post_parse_analyze::is_installed()
                && queryjumble::IsQueryIdEnabled()
            {
                let js = queryjumble::JumbleQuery(mcx, &mut query)?;
                parser_analyze::tap_post_parse_analyze::call_if(|f| f(&mut query, &js, query_string));
            } else if queryjumble::IsQueryIdEnabled() {
                queryjumble::JumbleQueryDiscard(mcx, &mut query)?;
            }
            let rewritten = rewrite_handler_seams::query_rewrite::call(mcx, query)?;
            assert!(rewritten.len() == 1, "CTAS rewrite yielded {} queries", rewritten.len());
            let query = rewritten.into_iter().next().expect("len == 1");
            return ExplainOneQuery(
                mcx,
                query,
                CURSOR_OPT_PARALLEL_OK,
                ctas_into,
                es,
                query_string,
                params,
                query_env,
            );
        }
        NodeTag::T_DeclareCursorStmt => {
            let options = stmt.as_declare_cursor_stmt().expect("tag checked").options;
            // C copyObject(dcs->query) then rewrites the copy: the statement
            // tree lives in the caller's arena; rewrite/planning grow its
            // lists under this mcx (see ExplainQuery).
            let query_node = copyfuncs::copy_object(
                mcx,
                stmt.as_declare_cursor_stmt()
                    .expect("tag checked")
                    .query
                    .expect("EXPLAIN DECLARE CURSOR without analyzed query"),
            )?;
            // SAFETY: fresh copy; this call holds its only live access.
            let mut query: Query<'mcx> =
                unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
                    .expect("DECLARE CURSOR query is not an analyzed Query");
            if parser_analyze::tap_post_parse_analyze::is_installed()
                && queryjumble::IsQueryIdEnabled()
            {
                let js = queryjumble::JumbleQuery(mcx, &mut query)?;
                parser_analyze::tap_post_parse_analyze::call_if(|f| f(&mut query, &js, query_string));
            } else if queryjumble::IsQueryIdEnabled() {
                queryjumble::JumbleQueryDiscard(mcx, &mut query)?;
            }
            let rewritten = rewrite_handler_seams::query_rewrite::call(mcx, query)?;
            assert!(rewritten.len() == 1, "DECLARE rewrite yielded {} queries", rewritten.len());
            let query = rewritten.into_iter().next().expect("len == 1");
            return ExplainOneQuery(mcx, query, options, None, es, query_string, params, query_env);
        }
        NodeTag::T_ExecuteStmt => {
            let exec_stmt = stmt.as_execute_stmt().expect("tag checked");
            // C meters GetCachedPlan inside ExplainExecuteQuery; the delta
            // here also spans parameter evaluation (value divergence only).
            let mem_before = if es.memory { Some(mem_snapshot(mcx)) } else { None };
            let bufusage_start =
                if es.buffers { Some(instrument::pg_buffer_usage()) } else { None };
            let mut bufusage: Option<BufferUsage> = None;
            let mut mem_counters: Option<MemCounters> = None;
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
                    if mem_counters.is_none() {
                        mem_counters = mem_before.map(|b| mem_counters_since(mcx, b));
                    }
                    ExplainOnePlanRef(
                        mcx,
                        pstmt,
                        into,
                        es,
                        prepared_query,
                        param_li,
                        query_env,
                        planduration,
                        bufusage.as_ref(),
                        mem_counters.as_ref(),
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
                ExplainDummyGroup("Notify", None, es);
            }
        }
        _ => {
            if es.format == EXPLAIN_FORMAT_TEXT {
                es.str.append_str("Utility statements have no plan structure\n")?;
            } else {
                ExplainDummyGroup("Utility Statement", None, es);
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

// "into" is None unless explaining the contents of a CreateTableAsStmt, in
// which case executing the query creates that table.
#[allow(clippy::too_many_arguments)]
pub fn ExplainOnePlan<'mcx>(
    mcx: Mcx<'mcx>,
    plan: PlannedStmt<'mcx>,
    into: Option<Node<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    planduration: std::time::Duration,
    bufusage: Option<&BufferUsage>,
    mem_counters: Option<&MemCounters>,
) -> PgResult<()> {
    let pstmt: &'mcx PlannedStmt<'mcx> = mcx::alloc_leak_in(mcx, plan)?;
    ExplainOnePlanRef(
        mcx,
        pstmt,
        into,
        es,
        query_string,
        params,
        query_env,
        planduration,
        bufusage,
        mem_counters,
    )
}

#[allow(clippy::too_many_arguments)]
fn ExplainOnePlanRef<'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &'mcx PlannedStmt<'mcx>,
    into: Option<Node<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    planduration: std::time::Duration,
    bufusage: Option<&BufferUsage>,
    mem_counters: Option<&MemCounters>,
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

    let into_clause: Option<&IntoClause<'_>> =
        into.map(|n| n.as_variant::<IntoClause>().expect("IntoClause"));
    let cmd_dest = if into.is_some() {
        types_dest::CommandDest::IntoRel
    } else if es.serialize != EXPLAIN_SERIALIZE_NONE {
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
    // pgrust-only EXPLAIN (ENGINE): arm per-node engine attribution capture.
    // Absent (the default), the flag never sets and es_engine_events stays
    // empty — default EXPLAIN output is byte-identical.
    if es.engine {
        eflags |= types_slot::EXEC_FLAG_ENGINE_REPORT;
    }
    if let Some(ic) = into_clause {
        eflags |= createas_seams::get_into_rel_eflags::call(ic.skipData);
    }
    execmain_seams::executor_start::call(qd, eflags)?;

    let mut serializeMetrics = explain_dr::SerializeMetrics::default();
    if es.analyze {
        let mut dest = if let Some(into_node) = into {
            DestReceiver::IntoRel(createas_seams::IntoRelState::new(mcx, into_node))
        } else if es.serialize != EXPLAIN_SERIALIZE_NONE {
            DestReceiver::ExplainSerialize(explain_dr::CreateExplainSerializeDestReceiver(
                mcx,
                es.serialize == EXPLAIN_SERIALIZE_BINARY,
                es.timing,
                es.buffers,
            ))
        } else {
            DestReceiver::DoNothing
        };
        // C: EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird.
        let dir = if into_clause.is_some_and(|ic| ic.skipData) {
            types_scan::sdir::ScanDirection::NoMovementScanDirection
        } else {
            types_scan::sdir::ScanDirection::ForwardScanDirection
        };
        execmain_seams::executor_run::call(qd, dir, 0, &mut dest)?;
        execmain_seams::executor_finish::call(qd)?;
        // C: GetSerializationMetrics(dest) before dest->rDestroy(dest); the
        // IntoRel else-arm (all-zero metrics) is the initializer above.
        if let DestReceiver::ExplainSerialize(dr) = &dest {
            serializeMetrics = dr.metrics;
        }
        dest.destroy();
        totaltime += elapsed_time(&starttime);
    }

    ExplainOpenGroup("Query", None, true, es);
    es.qd = qd;
    ExplainPrintPlan(mcx, es, pstmt)?;
    es.qd = types_portal::QueryDescHandle::NULL;

    if bufusage.is_some_and(|bu| peek_buffer_usage(es, bu)) || mem_counters.is_some() {
        ExplainOpenGroup("Planning", Some("Planning"), true, es);
        if es.format == EXPLAIN_FORMAT_TEXT {
            ExplainIndentText(es);
            es.str.append_str("Planning:\n")?;
            es.indent += 1;
        }
        if let Some(bu) = bufusage {
            show_buffer_usage(es, bu);
        }
        if let Some(mc) = mem_counters {
            show_memory_counters(es, mc);
        }
        if es.format == EXPLAIN_FORMAT_TEXT {
            es.indent -= 1;
        }
        ExplainCloseGroup("Planning", Some("Planning"), true, es);
    }

    // pgrust-only "M5 Cost Route" line (knob-gated: the field is only ever
    // filled while PGRUST_M5_COST_EXPLAIN is armed — default OFF keeps every
    // EXPLAIN golden byte-identical). Shows the shadow verdict pair of the
    // planning that produced this plan: the whitelist/floor verdict, the
    // cost-model verdict, and which mechanism decided.
    if let Some(s) = es.m5_cost_route {
        ExplainPropertyText(
            "M5 Cost Route",
            &format!(
                "class={} r_pred={:.3} model={} whitelist={} decided_by={} rows={:.0} dop={}",
                s.class,
                s.ratio,
                if s.model_suppress { "suppress" } else { "gather" },
                if s.whitelist_suppress { "suppress" } else { "gather" },
                s.decided_by,
                s.rows,
                s.dop,
            ),
            es,
        );
    }

    if es.summary {
        ExplainPropertyFloat("Planning Time", Some("ms"), 1000.0 * planduration.as_secs_f64(), 3, es);
    }

    // ExplainPrintTriggers: no CREATE TRIGGER path exists, so every result
    // relation's ri_TrigDesc is provably absent and report_triggers emits
    // nothing; non-text formats still print the empty Triggers group.
    if es.analyze {
        ExplainOpenGroup("Triggers", Some("Triggers"), false, es);
        ExplainCloseGroup("Triggers", Some("Triggers"), false, es);
    }

    if es.costs {
        ExplainPrintJITSummary(es, qd);
    }

    // ExplainPrintJITSummary: C prints a JIT block only when a loaded JIT
    // provider actually compiled functions (ExplainPrintJIT returns on
    // created_functions == 0; without the llvmjit provider es_jit stays NULL
    // even with PGJIT_PERFORM set). No LLVM provider exists here — the deform
    // JIT is an availability-gated deform implementation outside C's jit
    // machinery and never reports as created functions — so the summary is
    // always absent, matching C run without the llvmjit package
    // (docs/optimizations/jit-deform.md, GUC/EXPLAIN mapping).

    if es.serialize != EXPLAIN_SERIALIZE_NONE {
        ExplainPrintSerialize(es, &serializeMetrics);
    }

    if let Some(hook) = crate::state::explain_per_plan_hook() {
        hook(pstmt, es, query_string)?;
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

// jit.h pins; the planner lane keeps its own private copies.
const PGJIT_PERFORM: i32 = 1 << 0;
const PGJIT_OPT3: i32 = 1 << 1;
const PGJIT_INLINE: i32 = 1 << 2;
const PGJIT_EXPR: i32 = 1 << 3;
const PGJIT_DEFORM: i32 = 1 << 4;

// explain.c ExplainPrintJITSummary. The worker-instr merge is absent: jit
// runs leader-only, so the leader's counters are the total.
fn ExplainPrintJITSummary(es: &mut ExplainState<'_>, qd: types_portal::QueryDescHandle) {
    let (jit_flags, created_functions, generation_nanos) =
        execmain_seams::query_desc_jit_instr::call(qd);
    if jit_flags & PGJIT_PERFORM == 0 {
        return;
    }
    ExplainPrintJIT(es, jit_flags, created_functions, generation_nanos);
}

// explain.c ExplainPrintJIT.
fn ExplainPrintJIT(
    es: &mut ExplainState<'_>,
    jit_flags: i32,
    created_functions: i32,
    generation_nanos: u64,
) {
    if created_functions == 0 {
        return;
    }
    // Copy-and-patch has no inlining/optimization/emission phases and no
    // separate deform counter; those print 0.000 and Total == Generation.
    let generation_ms = 1000.0 * (generation_nanos as f64 / 1e9);
    let onoff = |set: bool| if set { "true" } else { "false" };

    ExplainOpenGroup("JIT", Some("JIT"), true, es);

    if es.format == EXPLAIN_FORMAT_TEXT {
        ExplainIndentText(es);
        es.str.append_str("JIT:\n").expect("explain output append");
        es.indent += 1;

        ExplainPropertyInteger("Functions", None, created_functions as i64, es);

        ExplainIndentText(es);
        append!(
            es,
            "Options: Inlining {}, Optimization {}, Expressions {}, Deforming {}\n",
            onoff(jit_flags & PGJIT_INLINE != 0),
            onoff(jit_flags & PGJIT_OPT3 != 0),
            onoff(jit_flags & PGJIT_EXPR != 0),
            onoff(jit_flags & PGJIT_DEFORM != 0),
        );

        if es.analyze && es.timing {
            ExplainIndentText(es);
            append!(
                es,
                "Timing: Generation {generation_ms:.3} ms (Deform 0.000 ms), \
                 Inlining 0.000 ms, Optimization 0.000 ms, Emission 0.000 ms, \
                 Total {generation_ms:.3} ms\n",
            );
        }

        es.indent -= 1;
    } else {
        ExplainPropertyInteger("Functions", None, created_functions as i64, es);

        ExplainOpenGroup("Options", Some("Options"), true, es);
        ExplainPropertyBool("Inlining", jit_flags & PGJIT_INLINE != 0, es);
        ExplainPropertyBool("Optimization", jit_flags & PGJIT_OPT3 != 0, es);
        ExplainPropertyBool("Expressions", jit_flags & PGJIT_EXPR != 0, es);
        ExplainPropertyBool("Deforming", jit_flags & PGJIT_DEFORM != 0, es);
        ExplainCloseGroup("Options", Some("Options"), true, es);

        if es.analyze && es.timing {
            ExplainOpenGroup("Timing", Some("Timing"), true, es);

            ExplainOpenGroup("Generation", Some("Generation"), true, es);
            ExplainPropertyFloat("Deform", Some("ms"), 0.0, 3, es);
            ExplainPropertyFloat("Total", Some("ms"), generation_ms, 3, es);
            ExplainCloseGroup("Generation", Some("Generation"), true, es);

            ExplainPropertyFloat("Inlining", Some("ms"), 0.0, 3, es);
            ExplainPropertyFloat("Optimization", Some("ms"), 0.0, 3, es);
            ExplainPropertyFloat("Emission", Some("ms"), 0.0, 3, es);
            ExplainPropertyFloat("Total", Some("ms"), generation_ms, 3, es);

            ExplainCloseGroup("Timing", Some("Timing"), true, es);
        }
    }
    ExplainCloseGroup("JIT", Some("JIT"), true, es);
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
        ExplainPropertyInteger("Shared Hit Blocks", None, usage.shared_blks_hit, es);
        ExplainPropertyInteger("Shared Read Blocks", None, usage.shared_blks_read, es);
        ExplainPropertyInteger("Shared Dirtied Blocks", None, usage.shared_blks_dirtied, es);
        ExplainPropertyInteger("Shared Written Blocks", None, usage.shared_blks_written, es);
        ExplainPropertyInteger("Local Hit Blocks", None, usage.local_blks_hit, es);
        ExplainPropertyInteger("Local Read Blocks", None, usage.local_blks_read, es);
        ExplainPropertyInteger("Local Dirtied Blocks", None, usage.local_blks_dirtied, es);
        ExplainPropertyInteger("Local Written Blocks", None, usage.local_blks_written, es);
        ExplainPropertyInteger("Temp Read Blocks", None, usage.temp_blks_read, es);
        ExplainPropertyInteger("Temp Written Blocks", None, usage.temp_blks_written, es);
        if guc_tables::vars::track_io_timing.read() {
            ExplainPropertyFloat(
                "Shared I/O Read Time",
                Some("ms"),
                usage.shared_blk_read_time.get_millisec(),
                3,
                es,
            );
            ExplainPropertyFloat(
                "Shared I/O Write Time",
                Some("ms"),
                usage.shared_blk_write_time.get_millisec(),
                3,
                es,
            );
            ExplainPropertyFloat(
                "Local I/O Read Time",
                Some("ms"),
                usage.local_blk_read_time.get_millisec(),
                3,
                es,
            );
            ExplainPropertyFloat(
                "Local I/O Write Time",
                Some("ms"),
                usage.local_blk_write_time.get_millisec(),
                3,
                es,
            );
            ExplainPropertyFloat(
                "Temp I/O Read Time",
                Some("ms"),
                usage.temp_blk_read_time.get_millisec(),
                3,
                es,
            );
            ExplainPropertyFloat(
                "Temp I/O Write Time",
                Some("ms"),
                usage.temp_blk_write_time.get_millisec(),
                3,
                es,
            );
        }
        return;
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
