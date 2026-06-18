//! The SQL-`EXPLAIN` driver of `commands/explain.c`: `ExplainQuery`,
//! `ExplainOneQuery`, `standard_ExplainOneQuery`, and the `ExplainResultDesc`
//! result tuple descriptor (the `explain_result_desc` `tcop`-utility out-seam).
//!
//! Plain `EXPLAIN` (no ANALYZE, no VERBOSE) prints the plan tree + costs. The
//! option parser ([`ParseExplainOptionList`]) runs over the analyzed-tree
//! `ddlnodes::DefElem` options the executable `ExplainStmt` carries, reading
//! each value through the `def_get_boolean` / `def_get_string` seams (the same
//! `DefElemArg` projection `ExplainResultDesc` uses). ANALYZE / VERBOSE reach
//! loud boundaries downstream (instrumentation / ruleutils deparse, unported).

extern crate alloc;

use alloc::string::String;

use mcx::Mcx;
use types_core::Oid;
use backend_utils_error::ereport;
use types_error::{
    PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_explain::{ExplainFormat, ExplainSerializeOption, ExplainState};
use types_nodes::copy_query::{Query, CURSOR_OPT_PARALLEL_OK};
use types_nodes::ddlnodes::DefElem;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsestmt::{DestReceiverHandle, ParseState};
use types_nodes::portalcmds::ParamListInfo;
use types_nodes::parsestmt::IntoClause;
use types_nodes::queryenvironment::QueryEnvironment;
use types_slot::TupleSlotKind;
use types_tuple::heaptuple::TupleDesc;

use backend_commands_define_seams::DefElemArg;
use backend_commands_explain_format as fmt;
use backend_commands_explain_seams as seams;
use backend_commands_explain_state as state;

// TEXTOID / XMLOID / JSONOID (pg_type.h).
const TEXTOID: Oid = 25;
const XMLOID: Oid = 142;
const JSONOID: Oid = 114;

/// `defGetString`'s value projection: map a `ddlnodes::DefElem` arg `Node` to
/// the `DefElemArg` the `def_get_string`/`def_get_boolean` seams consume.
fn def_elem_arg(node: &Node<'_>) -> DefElemArg {
    match node {
        Node::Integer(i) => DefElemArg::Integer(i.ival as i64),
        Node::Float(f) => DefElemArg::Float(String::from(f.fval.as_str())),
        Node::Boolean(b) => DefElemArg::Boolean(b.boolval),
        Node::String(s) => DefElemArg::String(String::from(s.sval.as_str())),
        Node::A_Star(_) => DefElemArg::AStar,
        other => panic!("EXPLAIN def_elem_arg: unsupported option arg node {other:?}"),
    }
}

/// `def->defname` as a `&str` (the C reads `def->defname` directly).
fn defname_str<'a>(opt: &'a DefElem<'_>) -> &'a str {
    opt.defname.as_ref().map(|s| s.as_str()).unwrap_or("")
}

/// `defGetBoolean(opt)` over the analyzed-tree `DefElem`, routed through the
/// `def_get_boolean` seam (owned by `backend-commands-define`).
fn def_get_boolean(opt: &DefElem<'_>) -> PgResult<bool> {
    let arg = opt.arg.as_deref().map(def_elem_arg);
    backend_commands_define_seams::def_get_boolean::call(String::from(defname_str(opt)), arg)
}

/// `defGetString(opt)` over the analyzed-tree `DefElem`, routed through the
/// `def_get_string` seam.
fn def_get_string<'mcx>(mcx: Mcx<'mcx>, opt: &DefElem<'_>) -> PgResult<String> {
    let arg = opt.arg.as_deref().map(def_elem_arg);
    let p = backend_commands_define_seams::def_get_string::call(
        mcx,
        String::from(defname_str(opt)),
        arg,
    )?;
    Ok(String::from(p.as_str()))
}

/// `ParseExplainOptionList(es, options, pstate)` (explain_state.c:77) over the
/// analyzed-tree `ddlnodes::DefElem` options the executable `ExplainStmt`
/// carries. Updates `es` per the option list.
pub(crate) fn ParseExplainOptionList<'mcx>(
    es: &mut ExplainState<'mcx>,
    options: &[types_nodes::nodes::NodePtr<'mcx>],
    _pstate: &mut ParseState<'_>,
) -> PgResult<()> {
    let mcx = es.str.allocator();

    for opt_node in options {
        let opt = match &**opt_node {
            Node::DefElem(d) => d,
            other => panic!("ParseExplainOptionList: option is not a DefElem: {other:?}"),
        };
        let defname = defname_str(opt);
        if defname == "analyze" {
            es.analyze = def_get_boolean(opt)?;
        } else if defname == "verbose" {
            es.verbose = def_get_boolean(opt)?;
        } else if defname == "costs" {
            es.costs = def_get_boolean(opt)?;
        } else if defname == "buffers" {
            es.buffers = def_get_boolean(opt)?;
        } else if defname == "wal" {
            es.wal = def_get_boolean(opt)?;
        } else if defname == "settings" {
            es.settings = def_get_boolean(opt)?;
        } else if defname == "generic_plan" {
            es.generic = def_get_boolean(opt)?;
        } else if defname == "timing" {
            es.timing = def_get_boolean(opt)?;
        } else if defname == "summary" {
            es.summary = def_get_boolean(opt)?;
        } else if defname == "memory" {
            es.memory = def_get_boolean(opt)?;
        } else if defname == "serialize" {
            if opt.arg.is_some() {
                let p = def_get_string(mcx, opt)?;
                es.serialize = match p.as_str() {
                    "off" | "none" => ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE,
                    "text" => ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT,
                    "binary" => ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
                    _ => {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!(
                                "unrecognized value for EXPLAIN option \"{defname}\": \"{p}\""
                            ))
                            .into_error())
                    }
                };
            } else {
                es.serialize = ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT;
            }
        } else if defname == "format" {
            let p = def_get_string(mcx, opt)?;
            es.format = match p.as_str() {
                "text" => ExplainFormat::EXPLAIN_FORMAT_TEXT,
                "xml" => ExplainFormat::EXPLAIN_FORMAT_XML,
                "json" => ExplainFormat::EXPLAIN_FORMAT_JSON,
                "yaml" => ExplainFormat::EXPLAIN_FORMAT_YAML,
                _ => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!(
                            "unrecognized value for EXPLAIN option \"{defname}\": \"{p}\""
                        ))
                        .into_error())
                }
            };
        } else {
            // ApplyExtensionExplainOption (registered EXPLAIN extensions): no
            // extension options are registered in this build, so an unrecognized
            // option is a syntax error (the C falls through to this after the
            // extension hook returns false).
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized EXPLAIN option \"{defname}\""))
                .into_error());
        }
    }

    // check that WAL is used with EXPLAIN ANALYZE
    if es.wal && !es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("EXPLAIN option WAL requires ANALYZE".to_string())
            .into_error());
    }

    // if the timing was not set explicitly, set default value
    es.timing = if es.timing { true } else { es.analyze };

    // if the buffers was not set explicitly, set default value
    if !es.buffers {
        es.buffers = es.analyze;
    }

    // check that timing is used with EXPLAIN ANALYZE
    if es.timing && !es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("EXPLAIN option TIMING requires ANALYZE".to_string())
            .into_error());
    }

    // check that serialize is used with EXPLAIN ANALYZE
    if es.serialize != ExplainSerializeOption::EXPLAIN_SERIALIZE_NONE && !es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("EXPLAIN option SERIALIZE requires ANALYZE".to_string())
            .into_error());
    }

    // check that GENERIC_PLAN is not used with EXPLAIN ANALYZE
    if es.generic && es.analyze {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together".to_string())
            .into_error());
    }

    // if the summary was not set explicitly, set default value
    if !es.summary {
        es.summary = es.analyze;
    }

    Ok(())
}

/// `standard_ExplainOneQuery(query, cursorOptions, into, es, queryString,
/// params, queryEnv)` (explain.c:318) — plan the query, then hand the plan to
/// `ExplainOnePlan`. The `es->memory` planner-context accounting is gated (it
/// reaches an unported `MemoryContextMemConsumed`).
fn standard_ExplainOneQuery<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    cursor_options: i32,
    into: Option<&IntoClause<'mcx>>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
    params: ParamListInfo,
    query_env: Option<&QueryEnvironment<'mcx>>,
) -> PgResult<()> {
    // Planner memory/buffer accounting bookkeeping, threaded by value through
    // the explain_one_plan seam (mirrors C's stack-locals planstart/planduration
    // /bufusage). The es->memory branch is unported (gated in explain_execute_begin).
    let mut bk = seams::explain_execute_begin::call(es)?;

    // plan = pg_plan_query(query, queryString, cursorOptions, params);
    let plan = backend_optimizer_plan_planner_seams::pg_plan_query::call(
        mcx,
        query,
        query_string,
        cursor_options,
    )?;

    // INSTR_TIME_SET_CURRENT(planduration); INSTR_TIME_SUBTRACT(planduration, planstart);
    seams::explain_planduration::call(&mut bk)?;

    if es.memory {
        seams::explain_memory_accounting::call(&mut bk)?;
    }
    if es.buffers {
        seams::explain_buffer_accounting::call(&mut bk)?;
    }

    let es_buffers = es.buffers;
    let es_memory = es.memory;
    // ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration, ...);
    seams::explain_one_plan::call(
        &plan,
        into,
        es,
        query_string,
        params,
        query_env,
        &bk,
        es_buffers,
        es_memory,
    )
}

/// `ExplainOneQuery(query, cursorOptions, into, es, pstate, params)`
/// (explain.c:294) — explain one analyzed `Query`. Utility queries route to
/// `ExplainOneUtility`; optimizable queries to `standard_ExplainOneQuery`
/// (`ExplainOneQuery_hook` is never set in this build).
fn ExplainOneQuery<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    cursor_options: i32,
    into: Option<&IntoClause<'mcx>>,
    es: &mut ExplainState<'mcx>,
    pstate: &ParseState<'mcx>,
    params: ParamListInfo,
) -> PgResult<()> {
    // planner will not cope with utility statements
    if query.commandType == CmdType::CMD_UTILITY {
        let utility = query
            .utilityStmt
            .as_deref()
            .expect("ExplainOneQuery: CMD_UTILITY query with NULL utilityStmt");
        return seams::explain_one_utility::call(
            utility,
            into,
            es,
            pstate.p_sourcetext.as_ref().map(|s| s.as_str()).unwrap_or(""),
            pstate.p_queryEnv.as_deref(),
            params,
        );
    }

    // ExplainOneQuery_hook is unset; call standard_ExplainOneQuery.
    standard_ExplainOneQuery(
        mcx,
        query,
        cursor_options,
        into,
        es,
        pstate.p_sourcetext.as_ref().map(|s| s.as_str()).unwrap_or(""),
        params,
        pstate.p_queryEnv.as_deref(),
    )
}

/// `ExplainQuery(pstate, stmt, params, dest)` (explain.c:163) — the SQL-`EXPLAIN`
/// utility entry. Configures an `ExplainState` from the options, runs the rule
/// rewriter over the (already analyzed) inner query, explains each rewritten
/// plan, and writes the textual output as one-text-column rows to `dest`.
pub fn ExplainQuery<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &Node<'mcx>,
    params: ParamListInfo,
    dest: DestReceiverHandle,
) -> PgResult<()> {
    let explain = match stmt {
        Node::ExplainStmt(e) => e,
        other => panic!("ExplainQuery: not an ExplainStmt: {other:?}"),
    };

    let mut es = state::NewExplainState(mcx);

    // Configure the ExplainState based on the provided options.
    ParseExplainOptionList(&mut es, explain.options.as_slice(), pstate)?;

    // Extract the query (analyze stored a Query node into stmt->query).
    let query = explain
        .query
        .as_deref()
        .and_then(|n| match n {
            Node::Query(q) => Some(q),
            _ => None,
        })
        .expect("ExplainQuery: ExplainStmt->query is not an analyzed Query node");

    // IsQueryIdEnabled() jumble + post_parse_analyze_hook: query-id jumbling is
    // off by default (compute_query_id = auto/off → disabled) and no hook is
    // installed in this build; the C path is a no-op here.

    // Parse analysis was done already; run the rule rewriter. We do not do
    // AcquireRewriteLocks (the query came straight from the parser).
    let query_copy = query.clone_in(mcx)?;
    let rewritten = backend_rewrite_rewritehandler_seams::query_rewrite_canonical::call(
        mcx, query_copy,
    )?;

    // emit opening boilerplate
    fmt::ExplainBeginOutput(&mut es)?;

    if rewritten.is_empty() {
        // INSTEAD NOTHING: say so (text format only; structured output is delimited).
        if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
            es.str.try_push_str("Query rewrites to nothing\n")?;
        }
    } else {
        let n = rewritten.len();
        for (i, q) in rewritten.iter().enumerate() {
            ExplainOneQuery(mcx, q, CURSOR_OPT_PARALLEL_OK, None, &mut es, pstate, params.clone())?;
            // Separate plans with an appropriate separator.
            if i + 1 < n {
                seams::explain_separate_plans::call(&mut es)?;
            }
        }
    }

    // emit closing boilerplate
    fmt::ExplainEndOutput(&mut es)?;
    debug_assert_eq!(es.indent, 0);

    // output tuples
    let tupdesc = ExplainResultDesc(mcx, stmt)?;
    let mut tstate = backend_executor_execTuples::exectype_tupoutput::begin_tup_output_tupdesc(
        mcx,
        dest,
        tupdesc,
        TupleSlotKind::Virtual,
    )?;
    // The plan text lives in es.str; move it out before es is borrowed again.
    let plan_text = String::from(es.str.as_str());
    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        backend_executor_execTuples::exectype_tupoutput::do_text_output_multiline(
            mcx,
            &mut tstate,
            &plan_text,
        )?;
    } else {
        // do_text_output_oneline: emit the whole buffer as a single row.
        do_text_output_oneline(mcx, &mut tstate, &plan_text)?;
    }
    backend_executor_execTuples::exectype_tupoutput::end_tup_output(mcx, tstate)?;

    Ok(())
}

/// `do_text_output_oneline(tstate, str_to_emit)` (execTuples.c) — emit the whole
/// string as one single-text-column row (the trailing newline, if any, is part
/// of the value). Local mirror used by the structured-format path.
fn do_text_output_oneline<'mcx>(
    mcx: Mcx<'mcx>,
    tstate: &mut types_nodes::tuptable::TupOutputState<'mcx>,
    txt: &str,
) -> PgResult<()> {
    let datum = backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, txt)?;
    let values = [datum];
    let isnull = [false];
    backend_executor_execTuples::exectype_tupoutput::do_tup_output(
        mcx, tstate, &values, &isnull,
    )
}

/// `ExplainResultDesc(stmt)` (explain.c:254) — the single-column "QUERY PLAN"
/// result tuple descriptor. Its column type is TEXT / XML / JSON per the last
/// `format` option (the C "don't break, last value wins").
pub fn ExplainResultDesc<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<TupleDesc<'mcx>> {
    let explain = match stmt {
        Node::ExplainStmt(e) => e,
        other => panic!("ExplainResultDesc: not an ExplainStmt: {other:?}"),
    };

    // Check for XML/JSON format option (last value wins).
    let mut result_type = TEXTOID;
    for opt in explain.options.iter() {
        if let Node::DefElem(d) = &**opt {
            if d.defname.as_ref().map(|s| s.as_str()) == Some("format") {
                let p = def_get_string(mcx, d)?;
                result_type = match p.as_str() {
                    "xml" => XMLOID,
                    "json" => JSONOID,
                    _ => TEXTOID,
                };
            }
        }
    }

    // Single TEXT/XML/JSON column named "QUERY PLAN".
    let mut tupdesc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
    backend_access_common_tupdesc::TupleDescInitEntry(
        &mut tupdesc,
        1,
        Some("QUERY PLAN"),
        result_type,
        -1,
        0,
    )?;
    Ok(Some(mcx::alloc_in(mcx, tupdesc)?))
}
