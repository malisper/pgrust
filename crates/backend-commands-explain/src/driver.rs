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
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_explain::{ExplainFormat, ExplainSerializeOption, ExplainState};
use types_nodes::copy_query::{Query, CURSOR_OPT_PARALLEL_OK};
use types_nodes::ddlnodes::DefElem;
use types_nodes::parsenodes::{OBJECT_MATVIEW, OBJECT_TABLE};
use types_nodes::nodes::{ntag, CmdType, Node};
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
use backend_rewrite_rewritehandler_seams as rewrite;
use backend_commands_createas_seams;

// TEXTOID / XMLOID / JSONOID (pg_type.h).
const TEXTOID: Oid = 25;
const XMLOID: Oid = 142;
const JSONOID: Oid = 114;

/// `defGetString`'s value projection: map a `ddlnodes::DefElem` arg `Node` to
/// the `DefElemArg` the `def_get_string`/`def_get_boolean` seams consume.
fn def_elem_arg(node: &Node<'_>) -> DefElemArg {
    match node.node_tag() {
        ntag::T_Integer => DefElemArg::Integer(node.expect_integer().ival as i64),
        ntag::T_Float => DefElemArg::Float(String::from(node.expect_float().fval.as_str())),
        ntag::T_Boolean => DefElemArg::Boolean(node.expect_boolean().boolval),
        ntag::T_String => DefElemArg::String(String::from(node.expect_string().sval.as_str())),
        ntag::T_A_Star => DefElemArg::AStar,
        _ => panic!("EXPLAIN def_elem_arg: unsupported option arg node {node:?}"),
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

    let mut timing_set = false;
    let mut buffers_set = false;
    let mut summary_set = false;

    for opt_node in options {
        let opt = match (**opt_node).node_tag() {
            ntag::T_DefElem => (**opt_node).expect_defelem(),
            _ => panic!(
                "ParseExplainOptionList: option is not a DefElem: {:?}",
                **opt_node
            ),
        };
        let defname = defname_str(opt);
        if defname == "analyze" {
            es.analyze = def_get_boolean(opt)?;
        } else if defname == "verbose" {
            es.verbose = def_get_boolean(opt)?;
        } else if defname == "costs" {
            es.costs = def_get_boolean(opt)?;
        } else if defname == "buffers" {
            buffers_set = true;
            es.buffers = def_get_boolean(opt)?;
        } else if defname == "wal" {
            es.wal = def_get_boolean(opt)?;
        } else if defname == "settings" {
            es.settings = def_get_boolean(opt)?;
        } else if defname == "generic_plan" {
            es.generic = def_get_boolean(opt)?;
        } else if defname == "timing" {
            timing_set = true;
            es.timing = def_get_boolean(opt)?;
        } else if defname == "summary" {
            summary_set = true;
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
    es.timing = if timing_set { es.timing } else { es.analyze };

    // if the buffers was not set explicitly, set default value
    es.buffers = if buffers_set { es.buffers } else { es.analyze };

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
    es.summary = if summary_set { es.summary } else { es.analyze };

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
    source_text: &str,
    query_env: Option<&QueryEnvironment<'mcx>>,
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
            source_text,
            query_env,
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
        source_text,
        params,
        query_env,
    )
}

/// `ExplainOneUtility(utilityStmt, into, es, pstate, params)` (explain.c:439) —
/// explain a utility statement (the `EXPLAIN <utility>` legs). CTAS and DECLARE
/// CURSOR re-rewrite their contained `Query` and route back through
/// `ExplainOneQuery`; EXECUTE delegates to the prepared-statement cache's
/// `ExplainExecuteQuery` (installed by `backend-commands-prepare`); NOTIFY and
/// everything else emit a no-plan placeholder.
pub fn ExplainOneUtility<'mcx>(
    mcx: Mcx<'mcx>,
    utility_stmt: &Node<'mcx>,
    into: Option<&IntoClause<'mcx>>,
    es: &mut ExplainState<'mcx>,
    source_text: &str,
    query_env: Option<&QueryEnvironment<'mcx>>,
    params: ParamListInfo,
) -> PgResult<()> {
    // if (utilityStmt == NULL) return; — the seam is only reached with a node.
    match utility_stmt.node_tag() {
        ntag::T_CreateTableAsStmt => {
            // We have to rewrite the contained SELECT and then pass it back to
            // ExplainOneQuery.  Copy to be safe in the EXPLAIN EXECUTE case.
            let ctas = utility_stmt.expect_createtableasstmt();

            // Check if the relation exists or not.  This is done at this stage
            // to avoid query planning or execution.
            if backend_commands_createas_seams::create_table_as_rel_exists::call(mcx, ctas)? {
                match ctas.objtype {
                    OBJECT_TABLE => {
                        fmt::ExplainDummyGroup("CREATE TABLE AS", None, es)?;
                    }
                    OBJECT_MATVIEW => {
                        fmt::ExplainDummyGroup("CREATE MATERIALIZED VIEW", None, es)?;
                    }
                    other => {
                        return Err(PgError::error(alloc::format!(
                            "unexpected object type: {}",
                            other as i32
                        )));
                    }
                }
                return Ok(());
            }

            // ctas_query = castNode(Query, copyObject(ctas->query));
            let ctas_query = ctas
                .query
                .as_deref()
                .and_then(|n| n.as_query())
                .expect("ExplainOneUtility: CreateTableAsStmt->query is not a Query");
            let mut ctas_query = ctas_query.clone_in(mcx)?;

            // if (IsQueryIdEnabled()) jstate = JumbleQuery(ctas_query); (explain.c:424)
            if backend_nodes_queryjumble_seams::is_query_id_enabled::call() {
                ctas_query.queryId =
                    backend_nodes_queryjumble_seams::jumble_query_compute::call(&ctas_query);
            }

            // rewritten = QueryRewrite(ctas_query); Assert(length == 1);
            let rewritten = rewrite::query_rewrite_canonical::call(mcx, ctas_query)?;
            assert_eq!(rewritten.len(), 1, "QueryRewrite(CTAS query) produced != 1 query");

            // ctas->into — the CTAS target IntoClause. The downstream EXPLAIN
            // consumers carry the trimmed `parsestmt::IntoClause` (skipData +
            // the opaque createas-owned `IntoClause` node payload); build it
            // from the full `ddlnodes::IntoClause` the statement holds.
            let into_node = ctas
                .into
                .as_deref()
                .expect("ExplainOneUtility: CreateTableAsStmt->into is NULL");
            let skip_data = into_node
                .as_intoclause()
                .expect("ExplainOneUtility: CreateTableAsStmt->into is not an IntoClause")
                .skipData;
            let ctas_into = IntoClause {
                skipData: skip_data,
                node: mcx::alloc_in(mcx, into_node.clone_in(mcx)?)?,
            };

            ExplainOneQuery(
                mcx,
                &rewritten[0],
                CURSOR_OPT_PARALLEL_OK,
                Some(&ctas_into),
                es,
                source_text,
                query_env,
                params,
            )
        }
        ntag::T_DeclareCursorStmt => {
            // Likewise for DECLARE CURSOR. EXPLAIN ANALYZE DECLARE CURSOR runs
            // the query (no cursor is created, however).
            let dcs = utility_stmt.expect_declarecursorstmt();

            let dcs_query = dcs
                .query
                .as_deref()
                .and_then(|n| n.as_query())
                .expect("ExplainOneUtility: DeclareCursorStmt->query is not a Query");
            let mut dcs_query = dcs_query.clone_in(mcx)?;

            // if (IsQueryIdEnabled()) jstate = JumbleQuery(dcs_query); (explain.c:450)
            if backend_nodes_queryjumble_seams::is_query_id_enabled::call() {
                dcs_query.queryId =
                    backend_nodes_queryjumble_seams::jumble_query_compute::call(&dcs_query);
            }

            let rewritten = rewrite::query_rewrite_canonical::call(mcx, dcs_query)?;
            assert_eq!(rewritten.len(), 1, "QueryRewrite(DECLARE CURSOR query) produced != 1 query");

            ExplainOneQuery(
                mcx,
                &rewritten[0],
                dcs.options,
                None,
                es,
                source_text,
                query_env,
                params,
            )
        }
        ntag::T_ExecuteStmt => {
            let execstmt = utility_stmt.expect_executestmt();
            seams::explain_execute_query::call(
                execstmt,
                into,
                es,
                source_text,
                query_env,
                params,
            )
        }
        ntag::T_NotifyStmt => {
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                es.str.try_push_str("NOTIFY\n")?;
            } else {
                fmt::ExplainDummyGroup("Notify", None, es)?;
            }
            Ok(())
        }
        _ => {
            if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
                es.str
                    .try_push_str("Utility statements have no plan structure\n")?;
            } else {
                fmt::ExplainDummyGroup("Utility Statement", None, es)?;
            }
            Ok(())
        }
    }
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
    let explain = match stmt.node_tag() {
        ntag::T_ExplainStmt => stmt.expect_explainstmt(),
        _ => panic!("ExplainQuery: not an ExplainStmt: {stmt:?}"),
    };

    let mut es = state::NewExplainState(mcx);

    // Configure the ExplainState based on the provided options.
    ParseExplainOptionList(&mut es, explain.options.as_slice(), pstate)?;

    // Extract the query (analyze stored a Query node into stmt->query).
    let query = explain
        .query
        .as_deref()
        .and_then(|n| n.as_query())
        .expect("ExplainQuery: ExplainStmt->query is not an analyzed Query node");

    // Parse analysis was done already; run the rule rewriter. We do not do
    // AcquireRewriteLocks (the query came straight from the parser).
    let mut query_copy = query.clone_in(mcx)?;

    // if (IsQueryIdEnabled()) jstate = JumbleQuery(query);  (explain.c:190)
    // Under compute_query_id, jumble the analyzed inner query and store the
    // resulting id into query.queryId; the planner copies it into
    // PlannedStmt.queryId, and ExplainPrintPlan prints it as "Query Identifier".
    // (post_parse_analyze_hook is NULL by default — no JumbleState consumer.)
    if backend_nodes_queryjumble_seams::is_query_id_enabled::call() {
        query_copy.queryId =
            backend_nodes_queryjumble_seams::jumble_query_compute::call(&query_copy);
    }

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
            ExplainOneQuery(
                mcx,
                q,
                CURSOR_OPT_PARALLEL_OK,
                None,
                &mut es,
                pstate.p_sourcetext.as_ref().map(|s| s.as_str()).unwrap_or(""),
                pstate.p_queryEnv.as_deref(),
                params.clone(),
            )?;
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
    let explain = match stmt.node_tag() {
        ntag::T_ExplainStmt => stmt.expect_explainstmt(),
        _ => panic!("ExplainResultDesc: not an ExplainStmt: {stmt:?}"),
    };

    // Check for XML/JSON format option (last value wins).
    let mut result_type = TEXTOID;
    for opt in explain.options.iter() {
        if (**opt).node_tag() == ntag::T_DefElem {
            let d = (**opt).expect_defelem();
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
