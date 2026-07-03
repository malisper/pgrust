// explain.c / explain_state.c / explain_format.c, M1 lane: text format,
// costs on/off, VERBOSE tlist, Result/SeqScan nodes; ANALYZE, non-text
// formats, buffers/wal/memory/settings and ruleutils deparse are loud.
#![allow(non_snake_case)]

use std::rc::Rc;
use std::time::Instant;

use mcx::Mcx;
use tcop_dest::DestReceiver;
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
mod options;
mod state;
#[cfg(test)]
mod tests;

pub use format::*;
pub use node::{ExplainNode, ExplainPrintPlan};
pub use options::{defGetBoolean, defGetString};
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

// C IsQueryIdEnabled (queryjumble.h): the AUTO arm reads query_id_enabled,
// which only unported jumble consumers ever set.
fn is_query_id_enabled() -> bool {
    use guc_tables::consts::{COMPUTE_QUERY_ID_ON, COMPUTE_QUERY_ID_REGRESS};
    matches!(
        guc_tables::backing::compute_query_id(),
        COMPUTE_QUERY_ID_ON | COMPUTE_QUERY_ID_REGRESS
    )
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

    let query_node = stmt.query.expect("ExplainQuery: stmt->query is NULL");
    if is_query_id_enabled() {
        panic!("ExplainQuery (explain.c): JumbleQuery unported — compute_query_id is on");
    }
    // post_parse_analyze_hook: no plugin surface exists.

    // C rewrites stmt->query through the shared pointer and never reads it
    // again outside the plancache-held EXPLAIN EXECUTE path (loud upstream):
    // move the Query out of the node.
    // SAFETY: this call holds the only live access to the ExplainStmt tree.
    let query: Query<'mcx> = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
        .expect("ExplainQuery: statement is not an analyzed Query");
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
        return ExplainOneUtility(query.utilityStmt, es);
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
             MemoryContextMemConsumed accounting (instrument lane)"
        );
    }
    if es.buffers {
        panic!(
            "standard_ExplainOneQuery (explain.c): BUFFERS needs pgBufferUsage \
             accounting (instrument lane)"
        );
    }
    let planstart = Instant::now();
    let plan = postgres::simple_query::pg_plan_query(mcx, query, query_string, cursor_options, params)?
        .expect("planner will not cope with utility statements");
    let planduration = planstart.elapsed();
    ExplainOnePlan(mcx, plan, es, query_string, params, query_env, planduration)
}

// "into" (CreateTableAsStmt) callers are loud in the CTAS arm.
fn ExplainOneUtility<'mcx>(
    utility_stmt: Option<Node<'mcx>>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let Some(stmt) = utility_stmt else {
        return Ok(());
    };
    match stmt.node_tag() {
        NodeTag::T_CreateTableAsStmt => panic!(
            "ExplainOneUtility (explain.c): CREATE TABLE AS arm needs \
             CreateTableAsStmt vocabulary (CTAS lane)"
        ),
        NodeTag::T_DeclareCursorStmt => panic!(
            "ExplainOneUtility (explain.c): DECLARE CURSOR arm needs \
             DeclareCursorStmt vocabulary (portalcmds lane)"
        ),
        NodeTag::T_ExecuteStmt => panic!(
            "ExplainOneUtility (explain.c): ExplainExecuteQuery unported (prepare lane)"
        ),
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

#[allow(clippy::too_many_arguments)]
pub fn ExplainOnePlan<'mcx>(
    mcx: Mcx<'mcx>,
    plan: PlannedStmt<'mcx>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    planduration: std::time::Duration,
) -> PgResult<()> {
    debug_assert!(plan.commandType != CmdType::CMD_UTILITY);
    if es.analyze {
        panic!("ExplainOnePlan (explain.c): ANALYZE needs the instrument unit (instrument lane)");
    }
    debug_assert_eq!(es.serialize, EXPLAIN_SERIALIZE_NONE);

    snapmgr::PushCopiedSnapshot(&snapmgr::GetActiveSnapshot())?;
    snapmgr::UpdateActiveSnapshotCommandId()?;

    // C: dest = None_Receiver (no CTAS/SERIALIZE receivers in this lane).
    let pstmt: &'mcx PlannedStmt<'mcx> = mcx::alloc_leak_in(mcx, plan)?;
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        query_string,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        types_dest::CommandDest::None,
        params,
        query_env,
        0,
    )?;
    let mut eflags = EXEC_FLAG_EXPLAIN_ONLY;
    if es.generic {
        eflags |= EXEC_FLAG_EXPLAIN_GENERIC;
    }
    execmain_seams::executor_start::call(qd, eflags)?;

    ExplainOpenGroup("Query", None, true, es);
    ExplainPrintPlan(mcx, es, pstmt)?;

    if es.summary {
        ExplainPropertyFloat("Planning Time", Some("ms"), 1000.0 * planduration.as_secs_f64(), 3, es);
    }

    // ExplainPrintJITSummary: jitFlags is pinned 0 by the planner lane, so
    // C's PGJIT_PERFORM check makes it a no-op.
    if es.costs && pstmt.jitFlags != 0 {
        panic!("ExplainPrintJITSummary (explain.c): JIT display unported (jit lane)");
    }

    execmain_seams::executor_end::call(qd)?;
    execmain_seams::free_query_desc::call(qd);
    snapmgr::PopActiveSnapshot()?;
    ExplainCloseGroup("Query", None, true, es);
    Ok(())
}
