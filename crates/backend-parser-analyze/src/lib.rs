//! `parser/analyze.c` — transform a raw parse tree into an analyzed
//! `Query` tree.
//!
//! Milestone scope (Workstream-A): the SELECT path end to end — the
//! `parse_analyze_*` drivers, `transformStmt` dispatch, `transformTopLevelStmt`,
//! `transformOptionalSelectInto`, `parse_sub_analyze`, `transformSelectStmt`,
//! the FOR UPDATE/SHARE locking family, and the `*_requires_*` predicates.
//! SQL text -> `raw_parser` -> `transformStmt` -> an owned, walkable
//! `types_nodes::copy_query::Query<'mcx>`.
//!
//! VALUES, set-operations, and the DML statements (INSERT/UPDATE/DELETE/MERGE,
//! RETURN, PL/pgSQL assignment, DECLARE CURSOR, EXPLAIN, CREATE TABLE AS, CALL)
//! are a follow-on family — they dispatch through `transformStmt` to a
//! seam-and-panic until their decomposition lands (see the crate notes).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use backend_utils_error::ereport;
use mcx::{Mcx, PgBox, PgVec};
use types_error::{PgResult, ERROR};
use types_nodes::copy_query::{Query, QuerySource};
use types_nodes::nodes::{ntag, CmdType, Node, NodePtr};
use types_nodes::parsestmt::{ParseState, RawStmt};
use types_nodes::rawnodes::SelectStmt;

mod inline_sql;
mod inline_srf;
pub use inline_srf::check_sql_fn_retval_public as check_sql_fn_retval;
mod insert;
pub use insert::build_on_conflict_excluded_targetlist as BuildOnConflictExcludedTargetlist;
mod locking;
mod merge;
mod plassign;
mod select;
mod setop;
mod special;
mod update_delete;

pub use locking::{applyLockingClause, transformLockingClause, CheckSelectLocking, LCS_asString};
pub use setop::makeSortGroupClauseForSetOp;

/// `ereport(ERROR, errmsg_internal(...))` shorthand for the panics-as-errors in
/// logic this unit owns.
fn elog_error(msg: impl Into<alloc::string::String>) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.into()).into_error()
}

/// `pstate->p_queryEnv = queryEnv` — set the parse state's query environment from
/// the per-backend query-environment home (the live AFTER-trigger transition-table
/// env, if any). C threads `queryEnv` explicitly through `parse_analyze_*`; the
/// owned model holds it in a thread-local home (like SPI's other backend globals).
///
/// Only the per-ENR **metadata** (name / reliddesc / tupdesc / type / estimate) is
/// copied into `mcx` via `clone_for_child` — that is the entirety of what parse
/// analysis reads (ENRs are looked up by name and the RTE is built from the
/// metadata's tupdesc). The live `reldata` tuplestore is an execution-time
/// resource read only by the executor, which sources it from the same home.
fn set_query_env_from_home<'mcx>(mcx: Mcx<'mcx>, pstate: &mut ParseState<'mcx>) -> PgResult<()> {
    let child = backend_utils_misc_queryenvironment_home::with_top_query_env(|env| match env {
        Some(env) => env.clone_for_child(mcx).map(Some),
        None => Ok(None),
    })?;
    if let Some(child) = child {
        pstate.p_queryEnv = Some(PgBox::try_new_in(child, mcx).map_err(|_| mcx.oom(0))?);
    }
    Ok(())
}

/* ===========================================================================
 * Entry points: parse_analyze_*
 * =========================================================================== */

/// `parse_analyze_fixedparams(parseTree, sourceText, paramTypes, numParams,
/// queryEnv)` — analyze a raw statement with the given fixed parameter types.
///
/// In the milestone scope the COPY/PREPARE drivers pass a `None` query
/// environment; `setup_parse_fixed_parameters` is applied when `param_types` is
/// non-empty (delegated to the small1 param owner, which installs the fixed
/// paramref hook + ref-hook state on the owned `ParseState`).
pub fn parse_analyze_fixedparams<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &RawStmt<'mcx>,
    source_text: &str,
    param_types: &[types_core::primitive::Oid],
) -> PgResult<Query<'mcx>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    // Assert(sourceText != NULL); pstate->p_sourcetext = sourceText;
    pstate.p_sourcetext = Some(mcx::PgString::from_str_in(source_text, mcx)?);

    // if (numParams > 0) setup_parse_fixed_parameters(pstate, paramTypes, numParams);
    if !param_types.is_empty() {
        backend_parser_small1::setup_parse_fixed_parameters(&mut pstate, param_types);
    }

    // pstate->p_queryEnv = queryEnv;  — sourced from the per-backend home so an
    // AFTER trigger's transition-table ENRs (OLD/NEW TABLE) resolve by name.
    set_query_env_from_home(mcx, &mut pstate)?;

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    // IsQueryIdEnabled() -> JumbleQuery(query): query-id jumbling is a separate
    // unported subsystem; the hook (post_parse_analyze_hook) is NULL by default.
    // pgstat_report_query_id is a no-op for queryId == 0. None of these change
    // the returned Query in the default configuration.

    backend_parser_small1::free_parsestate(pstate)?;

    Ok(query)
}

/// `pg_analyze_and_rewrite_withcb(parsetree, sourceText, sql_fn_parser_setup,
/// pinfo, NULL)` (functions.c) — analyze a SQL-function body statement with the
/// SQL-function parser hooks installed, so a `$n` `ParamRef` and a bareword that
/// names a function argument both resolve to the matching `Param`. The rewrite
/// leg (`pg_rewrite_query`) is applied by the caller; this returns the analyzed
/// `Query`.
pub fn parse_analyze_sql_function<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &RawStmt<'mcx>,
    source_text: &str,
    pinfo: types_nodes::parsestmt::SqlFnParseInfo,
) -> PgResult<Query<'mcx>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    pstate.p_sourcetext = Some(mcx::PgString::from_str_in(source_text, mcx)?);

    // sql_fn_parser_setup(pstate, pinfo): install the SQL-function hooks.
    backend_parser_small1::setup_parse_sql_function(&mut pstate, pinfo);

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    backend_parser_small1::free_parsestate(pstate)?;

    Ok(query)
}

/// `pg_analyze_and_rewrite_withcb(parsetree, sourceText, plpgsql_parser_setup,
/// expr, NULL)` (pl_exec.c `exec_prepare_plan`) — analyze a PL/pgSQL expression
/// (or statement) with the PL/pgSQL parser hooks installed, so a bareword (or
/// `block.var`) that names a PL/pgSQL variable resolves to its `PARAM_EXTERN`
/// `Param` (paramid = dno+1). Returns the analyzed `Query`; the referenced datum
/// numbers are recorded into the passed-in `PlpgsqlExprParseState` (read back by
/// `setup_param_list`). The rewrite leg is applied by the caller.
pub fn parse_analyze_plpgsql_expr<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &RawStmt<'mcx>,
    source_text: &str,
    state: types_nodes::parsestmt::PlpgsqlExprParseState,
) -> PgResult<Query<'mcx>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    pstate.p_sourcetext = Some(mcx::PgString::from_str_in(source_text, mcx)?);

    // plpgsql_parser_setup(pstate, expr): install the PL/pgSQL parser hooks +
    // ref-hook state.
    backend_parser_parse_expr::setup_parse_plpgsql_expr(&mut pstate, state);

    // pstate->p_queryEnv = queryEnv;  — sourced from the per-backend home so an
    // AFTER trigger's transition-table ENRs (OLD/NEW TABLE) resolve by name in a
    // `SELECT * FROM newtab` issued from the trigger function body.
    set_query_env_from_home(mcx, &mut pstate)?;

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    backend_parser_small1::free_parsestate(pstate)?;

    Ok(query)
}

/// `parse_analyze_varparams(parseTree, sourceText, paramTypes, numParams,
/// queryEnv)` (analyze.c:144) — analyze a raw statement deducing unknown `$n`
/// parameter types from context. The passed-in type array can be grown/replaced
/// as `$n` refs appear; the resolved array is read back through the shared
/// `VarParamState` carrier (its `Vec`'s length is the C `*numParams`).
///
/// The milestone caller (PREPARE) passes a `None` query environment. The query
/// returned is the analyzed tree; the post-parse-analyze hook (NULL by default)
/// is run by the rewrite wrapper's caller, mirroring the C call at analyze.c:169
/// — modeled here through the `run_post_parse_analyze_hook` seam.
pub fn parse_analyze_varparams<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &RawStmt<'mcx>,
    source_text: &str,
    arg_types: &[types_core::primitive::Oid],
) -> PgResult<(Query<'mcx>, types_nodes::parsestmt::VarParamState)> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    // Assert(sourceText != NULL); pstate->p_sourcetext = sourceText;
    pstate.p_sourcetext = Some(mcx::PgString::from_str_in(source_text, mcx)?);

    // setup_parse_variable_parameters(pstate, paramTypes, numParams);
    //
    // C seeds the growable VarParamState from the caller's `Oid **paramTypes` /
    // `int *numParams` and aliases it. The owned carrier is a shared `Rc<RefCell
    // <Vec<Oid>>>`; seed it with the caller's fixed arg-type prefix (the PREPARE
    // driver's declared `$n` types), which the variable_paramref_hook then grows
    // and resolves in place.
    let parstate = types_nodes::parsestmt::VarParamState::from_shared(
        alloc::rc::Rc::new(core::cell::RefCell::new(arg_types.to_vec())),
    );
    backend_parser_small1::setup_parse_variable_parameters(&mut pstate, parstate.clone());

    // pstate->p_queryEnv = queryEnv;  (milestone caller passes None)

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    // make sure all is well with parameter types
    backend_parser_small1::check_variable_parameters(&pstate, &query)?;

    // IsQueryIdEnabled() -> JumbleQuery(query): query-id jumbling is a separate
    // unported subsystem (queryId stays 0, jstate stays NULL).
    //
    //   if (post_parse_analyze_hook)
    //       (*post_parse_analyze_hook) (pstate, query, jstate);
    // The post_parse_analyze_hook is a per-backend `fn` pointer extensions install
    // (NULL by default). With no extension loaded it is unset, so this is a no-op
    // — exactly the C `if (hook)` guard falling through. (The portalcmds consumer
    // models the same call through the `run_post_parse_analyze_hook` seam over its
    // trimmed ParseState/Query view; that seam is the canonical NULL-hook no-op.)

    backend_parser_small1::free_parsestate(pstate)?;

    Ok((query, parstate))
}

/* ===========================================================================
 * interpret_sql_body (functioncmds.c:910) — the inline SQL-body branch
 * =========================================================================== */

/// `IsPolymorphicType(typid)` (pg_type.h) — the pseudo-types a SQL function with
/// an unquoted body may not have as an argument.
fn is_polymorphic_type(typid: types_core::primitive::Oid) -> bool {
    use types_core::primitive::Oid;
    // ANYELEMENT/ANYARRAY/ANYNONARRAY/ANYENUM/ANYRANGE/ANYMULTIRANGE +
    // ANYCOMPATIBLE family (pg_type.dat).
    const ANYELEMENTOID: Oid = 2283;
    const ANYARRAYOID: Oid = 2277;
    const ANYNONARRAYOID: Oid = 2776;
    const ANYENUMOID: Oid = 3500;
    const ANYRANGEOID: Oid = 3831;
    const ANYMULTIRANGEOID: Oid = 4537;
    const ANYCOMPATIBLEOID: Oid = 5077;
    const ANYCOMPATIBLEARRAYOID: Oid = 5078;
    const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
    const ANYCOMPATIBLERANGEOID: Oid = 5080;
    const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

/// Transform one body statement (`transformStmt`) under the SQL-function parser
/// hooks, raising the C "X is not yet supported in unquoted SQL function body"
/// error for a `CMD_UTILITY` result.
fn transform_one_body_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    pinfo: &types_nodes::parsestmt::SqlFnParseInfo,
    query_string: Option<&str>,
    stmt: &Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;
    if let Some(s) = query_string {
        pstate.p_sourcetext = Some(mcx::PgString::from_str_in(s, mcx)?);
    }
    backend_parser_small1::setup_parse_sql_function(&mut pstate, pinfo.clone());
    let q = transformStmt(mcx, &mut pstate, stmt)?;
    if q.commandType == CmdType::CMD_UTILITY {
        // C: errmsg("%s is not yet supported in unquoted SQL function body",
        //           GetCommandTagName(CreateCommandTag(q->utilityStmt)))
        let tagname = match &q.utilityStmt {
            Some(ustmt) => {
                let tag = backend_tcop_utility_seams::create_command_tag::call(ustmt.as_ref())?;
                backend_tcop_cmdtag::get_command_tag_name(tag.0)
            }
            None => "statement",
        };
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "{tagname} is not yet supported in unquoted SQL function body"
            ))
            .into_error());
    }
    backend_parser_small1::free_parsestate(pstate)?;
    Ok(q)
}

/// `interpret_AS_clause`'s inline-SQL-body branch (functioncmds.c:910). Build the
/// SQL-function parse info, transform the raw `sql_body_in` (a `ReturnStmt` for
/// `RETURN expr`, or — for `BEGIN ATOMIC ... END` — a `List` whose single element
/// is the statement list) into the cooked SQL-body node-tree, and return its
/// serialized `pg_node_tree` text (`nodeToString`). The cooked shape matches
/// what `fmgr_sql` / planner inlining read back via `stringToNode`:
///   * `RETURN expr`  -> a bare `Query` node.
///   * `BEGIN ATOMIC` -> `list_make1(list_of_Query)` (a `List` of one `List`).
pub fn interpret_sql_body<'mcx>(
    mcx: Mcx<'mcx>,
    funcname: String,
    sql_body_in: &Node<'mcx>,
    parameter_types: Vec<types_core::primitive::Oid>,
    in_parameter_names: Vec<String>,
    query_string: Option<String>,
) -> PgResult<backend_commands_functioncmds_seams::InterpretedSqlBody> {
    // pinfo->argtypes / argnames, with the polymorphic-arg check.
    let nargs = parameter_types.len();
    let mut argnames: Vec<Option<String>> = Vec::with_capacity(nargs);
    for (i, &typ) in parameter_types.iter().enumerate() {
        if is_polymorphic_type(typ) {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(
                    "SQL function with unquoted function body cannot have \
                     polymorphic arguments",
                )
                .into_error());
        }
        // pinfo->argnames[i] = (s[0] != '\0') ? s : NULL;
        match in_parameter_names.get(i) {
            Some(s) if !s.is_empty() => argnames.push(Some(s.clone())),
            _ => argnames.push(None),
        }
    }
    let argnames_opt = if nargs > 0 { Some(argnames) } else { None };
    let pinfo = types_nodes::parsestmt::SqlFnParseInfo::new(
        funcname,
        types_core::InvalidOid,
        parameter_types,
        argnames_opt,
    );
    let qstr = query_string.as_deref();

    // if (IsA(sql_body_in, List)) { ... BEGIN ATOMIC ... } else { ... RETURN ... }
    let cooked: Node<'mcx> = if sql_body_in.node_tag() == ntag::T_List {
        // stmts = linitial_node(List, castNode(List, sql_body_in));
        let outer = sql_body_in.expect_list();
        let stmts: &[NodePtr<'mcx>] = match outer.first() {
            Some(first) => match (**first).node_tag() {
                ntag::T_List => &(**first).expect_list()[..],
                // A grammar that already produced the bare statement list.
                _ => &outer[..],
            },
            None => &outer[..],
        };
        let mut transformed = mcx::PgVec::new_in(mcx);
        for stmt in stmts {
            let q = transform_one_body_stmt(mcx, &pinfo, qstr, stmt.as_ref())?;
            transformed.push(mcx::alloc_in(mcx, Node::mk_query(mcx, q)?)?);
        }
        // *sql_body_out = (Node *) list_make1(transformed_stmts);
        let inner = Node::mk_list(mcx, transformed)?;
        let mut outer_vec = mcx::PgVec::new_in(mcx);
        outer_vec.push(mcx::alloc_in(mcx, inner)?);
        Node::mk_list(mcx, outer_vec)?
    } else {
        // q = transformStmt(pstate, sql_body_in); *sql_body_out = (Node *) q;
        let q = transform_one_body_stmt(mcx, &pinfo, qstr, sql_body_in)?;
        Node::mk_query(mcx, q)?
    };

    // *prosrc_str_p = ""; *probin = NULL; nodeToString(*sql_body_out).
    // Use the `nodes::Node` serializer (outfuncs) — its text is what the read
    // path (`backend_nodes_core::read::string_to_node`) consumes.
    let text = backend_nodes_outfuncs::nodeToString(mcx, &cooked)?
        .as_str()
        .to_string();

    // recordDependencyOnExpr's reference-collection half (dependency.c:1697),
    // run over the *in-memory* cooked node so the CREATE FUNCTION dependency
    // recording never has to round-trip the stored text through `stringToNode`.
    // The depender OID isn't known until `ProcedureCreate` inserts the row, so
    // the references travel back with the serialized body and are recorded
    // there.
    let mut refs_ctx =
        backend_catalog_dependency::find_expr::FindExprReferencesContext::new(mcx);
    backend_catalog_dependency::find_expr::find_expr_references_walker(&cooked, &mut refs_ctx)?;
    if let Some(e) = refs_ctx.err.take() {
        return Err(e);
    }
    let body_refs = refs_ctx.addrs.refs;

    Ok(backend_commands_functioncmds_seams::InterpretedSqlBody { text, body_refs })
}

/* ===========================================================================
 * Parameter DEFAULT expressions (functioncmds.c:419-447 / pg_proc.c:549-573)
 * =========================================================================== */

/// `transformExpr(pstate, fp->defexpr, EXPR_KIND_FUNCTION_DEFAULT)` +
/// `coerce_to_specific_type(pstate, def, toid, "DEFAULT")` +
/// `assign_expr_collations(pstate, def)`, then the
/// `pstate->p_rtable != NIL || contain_var_clause(def)` no-table-references
/// check (functioncmds.c:419-433). `p_rtable` is always NIL for the fresh
/// DEFAULT parse state, so the check reduces to `contain_var_clause`. Returns
/// the cooked default as a rich `Node` (the `parameterDefaults` `List` element).
fn transform_parameter_default<'mcx>(
    mcx: Mcx<'mcx>,
    defexpr: &Node<'mcx>,
    toid: types_core::primitive::Oid,
    location: i32,
    query_string: Option<String>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::parsestmt::ParseExprKind;

    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;
    if let Some(s) = query_string.as_deref() {
        pstate.p_sourcetext = Some(mcx::PgString::from_str_in(s, mcx)?);
    }

    // def = transformExpr(pstate, fp->defexpr, EXPR_KIND_FUNCTION_DEFAULT);
    let raw = defexpr.clone_in(mcx)?;
    let def = backend_parser_parse_expr::transformExpr(
        &mut pstate,
        Some(raw),
        ParseExprKind::EXPR_KIND_FUNCTION_DEFAULT,
    )?;
    let def = def.ok_or_else(|| elog_error("transform_parameter_default: NULL default expr"))?;

    // def = coerce_to_specific_type(pstate, def, toid, "DEFAULT");
    let def = backend_parser_coerce::coerce_to_specific_type(
        mcx,
        Some(&mut pstate),
        def,
        toid,
        "DEFAULT",
    )?;

    // assign_expr_collations(pstate, def);
    // Bring the parser-arena `'static` coerced default into `mcx` for the
    // in-place collation pass and the `'mcx` node wrap below (invariant `Expr`).
    let mut def: types_nodes::primnodes::Expr<'mcx> = def.clone_in(mcx)?;
    backend_parser_parse_collate::assign_expr_collations(Some(&pstate), &mut def)?;

    // Wrap the cooked Expr as a Node for the var-clause check and serialization.
    let cooked = Node::mk_expr(mcx, def)?;

    // if (pstate->p_rtable != NIL || contain_var_clause(def))
    if !pstate.p_rtable.is_empty()
        || backend_optimizer_util_vars::var::contain_var_clause(&cooked)
    {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg("cannot use table references in parameter default value")
            .errposition(backend_parser_small1::parser_errposition(&pstate, location))
            .into_error());
    }

    backend_parser_small1::free_parsestate(pstate)?;
    Ok(cooked)
}

/// Serialize the cooked `parameterDefaults` `List` to its `pg_node_tree` text
/// (`nodeToString`, pg_proc.c:360) and collect its object references
/// (`recordDependencyOnExpr`'s reference half, pg_proc.c:670). An empty list
/// yields `text: None`, `nargdefaults: 0`, `refs: []`.
fn cook_parameter_defaults<'mcx>(
    mcx: Mcx<'mcx>,
    defaults: Vec<&Node<'mcx>>,
) -> PgResult<backend_commands_functioncmds_seams::CookedParameterDefaults> {
    let nargdefaults = defaults.len() as i32;
    if defaults.is_empty() {
        return Ok(backend_commands_functioncmds_seams::CookedParameterDefaults::default());
    }

    // Build the `List *` of cooked default nodes.
    let mut elems: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    elems.try_reserve(defaults.len()).map_err(|_| mcx.oom(defaults.len()))?;
    for d in defaults {
        elems.push(mcx::alloc_in(mcx, d.clone_in(mcx)?)?);
    }
    let list = Node::mk_list(mcx, elems)?;

    // nodeToString((Node *) parameterDefaults).
    let text = backend_nodes_outfuncs::nodeToString(mcx, &list)?
        .as_str()
        .to_string();

    // recordDependencyOnExpr's reference-collection half over the in-memory list.
    let mut refs_ctx =
        backend_catalog_dependency::find_expr::FindExprReferencesContext::new(mcx);
    backend_catalog_dependency::find_expr::find_expr_references_walker(&list, &mut refs_ctx)?;
    if let Some(e) = refs_ctx.err.take() {
        return Err(e);
    }
    let refs = refs_ctx.addrs.refs;

    Ok(backend_commands_functioncmds_seams::CookedParameterDefaults {
        text: Some(text),
        nargdefaults,
        refs,
    })
}

/// `check_for_default_argument_drop` half of `ProcedureCreate` (pg_proc.c:549-573):
/// `stringToNode` both old and new `proargdefaults` `List`s and compare the
/// retained old defaults' `exprType` against the corresponding new defaults
/// (the new list may have more, advanced over from the tail).
fn check_defaults_compatible(
    old_proargdefaults: String,
    old_nargdefaults: i16,
    new_proargdefaults: String,
) -> PgResult<backend_catalog_pg_proc_seams::DefaultCompat> {
    use backend_catalog_pg_proc_seams::DefaultCompat;

    let scratch = mcx::MemoryContext::new("check_defaults_compatible");
    let mcx = scratch.mcx();

    let old_node = backend_nodes_core::read::string_to_node(mcx, &old_proargdefaults)?;
    let new_node = backend_nodes_core::read::string_to_node(mcx, &new_proargdefaults)?;

    if old_node.node_tag() != ntag::T_List {
        return Err(elog_error("old proargdefaults is not a List"));
    }
    if new_node.node_tag() != ntag::T_List {
        return Err(elog_error("new proargdefaults is not a List"));
    }
    let old_defaults = old_node.expect_list();
    let new_defaults = new_node.expect_list();

    // Assert(list_length(oldDefaults) == oldproc->pronargdefaults);
    debug_assert_eq!(old_defaults.len(), old_nargdefaults as usize);

    // newlc = list_nth_cell(parameterDefaults, len(new) - oldproc->pronargdefaults)
    let skip = new_defaults.len().saturating_sub(old_nargdefaults as usize);
    for (oldlc, newlc) in old_defaults.iter().zip(new_defaults.iter().skip(skip)) {
        let old_ty = backend_nodes_core::nodefuncs::expr_type((**oldlc).as_expr())?;
        let new_ty = backend_nodes_core::nodefuncs::expr_type((**newlc).as_expr())?;
        if old_ty != new_ty {
            return Ok(DefaultCompat::TypeChanged);
        }
    }
    Ok(DefaultCompat::Ok)
}

/* ===========================================================================
 * parse_sub_analyze
 * =========================================================================== */

/// `parse_sub_analyze(parseTree, parentParseState, parentCTE,
/// locked_from_parent, resolve_unknowns)` — recursively analyze a sub-statement
/// in a child `ParseState` built off `parent_pstate`. Returns the resulting
/// `Query` wrapped as `Node::Query` (C `(Node *) query`), the contract the
/// parse_cte / parse_clause consumers read.
pub fn parse_sub_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &Node<'mcx>,
    parent_pstate: &mut ParseState<'mcx>,
    parent_cte: Option<&types_nodes::rawnodes::CommonTableExpr<'mcx>>,
    locked_from_parent: bool,
    resolve_unknowns: bool,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    let mut pstate = backend_parser_small1::make_parsestate(mcx, Some(parent_pstate))?;

    pstate.p_parent_cte = match parent_cte {
        Some(c) => Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?),
        None => None,
    };
    pstate.p_locked_from_parent = locked_from_parent;
    pstate.p_resolve_unknowns = resolve_unknowns;

    let query = transformStmt(mcx, &mut pstate, parse_tree)?;

    // The owned model holds `parentParseState` by value (a deep copy of the
    // parent's spine), so SELECT-privilege marks a LATERAL/correlated subquery
    // applies to outer-query RTEs (via markVarForSelectPriv walking up
    // `varlevelsup` levels) land on the *clone*. Merge the immediate parent
    // level's permission marks back into the live `parent_pstate` before the
    // child state is freed. Deeper levels are merged when their own
    // `parse_sub_analyze` frame returns (each level's clone is the next level's
    // `parent_pstate`). C needs no such step because `parentParseState` is a
    // live back-pointer.
    if let Some(cloned_parent) = pstate.parentParseState.as_deref() {
        merge_perminfo_marks(mcx, parent_pstate, cloned_parent)?;
        // Same divergence as the permission marks: a CTE reference inside this
        // subquery (`addRangeTableEntryForCTE` with `levelsup > 0`) does
        // `cte->cterefcount++` on the *clone's* `p_ctenamespace`, because the
        // owned `parentParseState` is a deep copy of `parent_pstate`'s spine.
        // C bumps the single shared CommonTableExpr, so the parent sees it
        // directly. Merge the clone's bumped refcounts back into the live
        // `parent_pstate.p_ctenamespace` (positionally / by-name identical, the
        // clone only ever increments) so `sync_cte_refcounts` at the defining
        // level reads the true count and does not drop the CTE plan. Deeper
        // levels propagate as each frame's `parse_sub_analyze` returns.
        merge_cte_refcounts(parent_pstate, cloned_parent);
        // An aggregate or GROUPING() written inside this subquery but referencing
        // only outer-query vars belongs to an OUTER level: `check_agglevels_and_
        // constraints` (parse_agg.c:353) walks up `parentParseState` `agglevelsup`
        // levels and sets that ancestor's `p_hasAggs = true`. In the owned model
        // those walked-up ancestors are clones in `src`'s `parentParseState` chain,
        // so the flag landed on a clone and is lost when this frame frees it. The
        // defining level later reads `qry->hasAggs = pstate->p_hasAggs` (analyze.c)
        // and would see `false`, so the planner never builds the Agg path and the
        // hoisted outer-level Aggref keeps `aggno = -1`. Merge the clone chain's
        // `p_hasAggs` back into the live parent chain. (C needs none of this: the
        // parent ParseState is a live back-pointer, so the set is seen directly.)
        merge_uplevel_has_aggs(parent_pstate, cloned_parent);
    }

    backend_parser_small1::free_parsestate(pstate)?;

    mcx::alloc_in(mcx, Node::mk_query(mcx, query)?)
}

/// Merge SELECT/INSERT/UPDATE permission marks recorded on a cloned parent
/// `ParseState` (`src`) back into the live parent (`dst`). The two
/// `p_rteperminfos` lists are positionally identical (the clone was made by
/// `clone_read_spine`), so a same-index OR of `requiredPerms` and a copy of the
/// (superset) clone's column sets transfers the marks: the clone started as a
/// copy of the live parent's lists and only added members.
fn merge_perminfo_marks<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    dst: &mut ParseState<'mcx>,
    src: &ParseState<'mcx>,
) -> PgResult<()> {
    debug_assert!(dst.p_rteperminfos.len() == src.p_rteperminfos.len());
    let n = dst.p_rteperminfos.len().min(src.p_rteperminfos.len());
    for i in 0..n {
        let s = &src.p_rteperminfos[i];
        if s.requiredPerms == 0
            && s.selectedCols.is_none()
            && s.insertedCols.is_none()
            && s.updatedCols.is_none()
        {
            continue;
        }
        dst.p_rteperminfos[i].requiredPerms |= s.requiredPerms;
        if let Some(c) = s.selectedCols.as_deref() {
            dst.p_rteperminfos[i].selectedCols = Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?);
        }
        if let Some(c) = s.insertedCols.as_deref() {
            dst.p_rteperminfos[i].insertedCols = Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?);
        }
        if let Some(c) = s.updatedCols.as_deref() {
            dst.p_rteperminfos[i].updatedCols = Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?);
        }
    }
    Ok(())
}

/// Merge `cterefcount` bumps recorded on a cloned parent `ParseState` (`src`,
/// the child subquery's owned `parentParseState`) back into the live parent
/// (`dst`). A CTE referenced from inside the subquery did `cterefcount++` on the
/// clone's `p_ctenamespace` entry (see `addRangeTableEntryForCTE` /
/// `bump_cte_namespace_refcount`); the clone started as a copy of `dst`'s
/// namespace and only ever increments, so adopting the clone's (>= live) value
/// per matching CTE name transfers the bump. C does not need this because the
/// namespace and `qry->cteList` alias the single shared `CommonTableExpr`.
fn merge_cte_refcounts<'mcx>(dst: &mut ParseState<'mcx>, src: &ParseState<'mcx>) {
    // Merge this level's namespace bumps.
    for d in dst.p_ctenamespace.iter_mut() {
        let name = match d.ctename.as_deref() {
            Some(n) => n,
            None => continue,
        };
        for s in src.p_ctenamespace.iter() {
            if s.ctename.as_deref() == Some(name) {
                if s.cterefcount > d.cterefcount {
                    d.cterefcount = s.cterefcount;
                }
                break;
            }
        }
    }

    // A CTE reference that crossed more than one query level (e.g. a sublink
    // inside a sibling CTE's body referencing an outer-WITH CTE) did
    // `cterefcount++` `levelsup` ParseStates up — which, in the owned model,
    // landed on the corresponding ancestor *clone* in `src`'s
    // `parentParseState` chain rather than on this level's namespace. A single
    // level of merge therefore misses it. Recurse down both clone/live chains
    // in lockstep so every level's bumps propagate to the live ancestor that
    // eventually becomes `qry->cteList` at its defining level. (C needs none of
    // this: the namespace and cteList alias the single shared CommonTableExpr,
    // so one `cterefcount++` is visible everywhere.)
    if let (Some(dst_parent), Some(src_parent)) =
        (dst.parentParseState.as_deref_mut(), src.parentParseState.as_deref())
    {
        merge_cte_refcounts(dst_parent, src_parent);
    }
}

/// Merge the `p_hasAggs` flag set on a cloned parent `ParseState` chain (`src`,
/// the child subquery's owned `parentParseState`) back into the live parent
/// chain (`dst`). An outer-level aggregate/`GROUPING()` written inside the child
/// subquery sets `p_hasAggs = true` `agglevelsup` levels up
/// (`check_agglevels_and_constraints`, parse_agg.c:353); in the owned model that
/// walked-up ancestor is a clone, so the flag must be OR'd back so the defining
/// level's `qry->hasAggs` reflects it. Recurse down both chains in lockstep so a
/// reference crossing more than one level (a deeply nested sublink) propagates to
/// every live ancestor it set. Only ever sets `true` (the flag is monotone), so
/// it never clobbers a flag the live parent already set for its own aggregates.
fn merge_uplevel_has_aggs<'mcx>(dst: &mut ParseState<'mcx>, src: &ParseState<'mcx>) {
    if src.p_hasAggs {
        dst.p_hasAggs = true;
    }
    if let (Some(dst_parent), Some(src_parent)) =
        (dst.parentParseState.as_deref_mut(), src.parentParseState.as_deref())
    {
        merge_uplevel_has_aggs(dst_parent, src_parent);
    }
}

/* ===========================================================================
 * transformTopLevelStmt / transformOptionalSelectInto
 * =========================================================================== */

/// `transformTopLevelStmt(pstate, parseTree)` — transform a `RawStmt` into a
/// `Query`, transferring statement-location data from the `RawStmt`.
pub fn transformTopLevelStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parse_tree: &RawStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    /* We're at top level, so allow SELECT INTO */
    let mut result = transformOptionalSelectInto(mcx, pstate, &parse_tree.stmt)?;

    result.stmt_location = parse_tree.stmt_location;
    result.stmt_len = parse_tree.stmt_len;

    Ok(result)
}

/// `transformOptionalSelectInto(pstate, parseTree)` — if a top-level SELECT has
/// INTO, rewrite it to CREATE TABLE AS; otherwise transform unchanged.
fn transformOptionalSelectInto<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parse_tree: &Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    if let Some(stmt) = parse_tree.as_selectstmt() {
        /* drill down to leftmost SelectStmt of a set-op tree */
        let mut leaf = stmt;
        while leaf.op != types_nodes::rawnodes::SETOP_NONE {
            match leaf.larg.as_deref() {
                Some(l) => leaf = l,
                None => break,
            }
        }
        debug_assert!(leaf.larg.is_none());

        if leaf.intoClause.is_some() {
            // Build a CREATE TABLE AS wrapping a copy of the SELECT with the
            // INTO clause removed from its leftmost leaf, mirroring the C
            // in-place edit (we deep-copy because the input is borrowed).
            let mut select_copy = stmt.clone_in(mcx)?;
            clear_leftmost_into(&mut select_copy);

            let into = leaf.intoClause.as_ref().map(|i| i.clone_in(mcx)).transpose()?;
            let into = match into {
                Some(n) => Some(mcx::alloc_in(mcx, n)?),
                None => None,
            };

            let ctas = types_nodes::ddlnodes::CreateTableAsStmt {
                query: Some(mcx::alloc_in(mcx, Node::mk_select_stmt(mcx, select_copy)?)?),
                into,
                objtype: types_nodes::parsenodes::OBJECT_TABLE,
                is_select_into: true,
                if_not_exists: false,
            };
            let ctas_node = Node::mk_create_table_as_stmt(mcx, ctas)?;
            return transformStmt(mcx, pstate, &ctas_node);
        }
    }

    transformStmt(mcx, pstate, parse_tree)
}

/// `transformExplainStmt(pstate, stmt)` (analyze.c:3093) — analyze an
/// `ExplainStmt`. Parse analysis of EXPLAIN just transforms the contained query
/// (allowing SELECT INTO) and represents the command as a CMD_UTILITY `Query`
/// wrapping the `ExplainStmt`. The C edits `stmt->query` in place; we deep-copy
/// the (borrowed) options and store the transformed inner `Query` back into a
/// fresh `ExplainStmt` so the executor (`ExplainQuery`) reads the analyzed query.
/// Project a `DefElem`-arg value node into the `def_get_boolean` seam's
/// `DefElemArg` (mirrors the EXPLAIN driver's `def_elem_arg`).
fn def_elem_arg(node: &Node<'_>) -> backend_commands_define_seams::DefElemArg {
    use backend_commands_define_seams::DefElemArg;
    match node.node_tag() {
        ntag::T_Integer => DefElemArg::Integer(node.expect_integer().ival as i64),
        ntag::T_Float => DefElemArg::Float(String::from(node.expect_float().fval.as_str())),
        ntag::T_Boolean => DefElemArg::Boolean(node.expect_boolean().boolval),
        ntag::T_String => DefElemArg::String(String::from(node.expect_string().sval.as_str())),
        ntag::T_A_Star => DefElemArg::AStar,
        _ => panic!("transformExplainStmt def_elem_arg: unsupported option arg node {node:?}"),
    }
}

fn transformExplainStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &types_nodes::ddlnodes::ExplainStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    // If we have no external source of parameter definitions, and the
    // GENERIC_PLAN option is specified, then accept variable parameter
    // definitions (similarly to PREPARE).
    let mut generic_plan = false;
    if pstate.p_paramref_hook.is_none() {
        for opt in stmt.options.iter() {
            if let Some(d) = opt.as_defelem() {
                if d.defname.as_ref().map(|s| s.as_str()) == Some("generic_plan") {
                    // generic_plan = defGetBoolean(opt);
                    // (don't "break", as we want the last value.)
                    let arg = d
                        .arg
                        .as_deref()
                        .map(def_elem_arg);
                    generic_plan = backend_commands_define_seams::def_get_boolean::call(
                        "generic_plan".to_string(),
                        arg,
                    )?;
                }
            }
        }
        if generic_plan {
            // setup_parse_variable_parameters(pstate, &paramTypes, &numParams);
            // The owned VarParamState is a shared growable Oid Vec, seeded empty
            // (EXPLAIN supplies no fixed declared types); the
            // variable_paramref_hook grows and resolves it in place.
            let parstate = types_nodes::parsestmt::VarParamState::from_shared(
                alloc::rc::Rc::new(core::cell::RefCell::new(alloc::vec::Vec::new())),
            );
            backend_parser_small1::setup_parse_variable_parameters(pstate, parstate);
        }
    }

    // transform contained query, allowing SELECT INTO.
    let inner = stmt
        .query
        .as_deref()
        .expect("transformExplainStmt: ExplainStmt->query is NULL");
    let transformed = transformOptionalSelectInto(mcx, pstate, inner)?;

    // make sure all is well with parameter types.
    if generic_plan {
        backend_parser_small1::check_variable_parameters(pstate, &transformed)?;
    }

    // represent the command as a utility Query wrapping a fresh ExplainStmt that
    // carries the transformed inner Query (mirrors C's `stmt->query = <Query>`).
    let mut new_stmt = stmt.clone_in(mcx)?;
    new_stmt.query = Some(mcx::alloc_in(mcx, Node::mk_query(mcx, transformed)?)?);

    let mut result = Query::new(mcx);
    result.commandType = CmdType::CMD_UTILITY;
    result.utilityStmt = Some(mcx::alloc_in(mcx, Node::mk_explain_stmt(mcx, new_stmt)?)?);
    Ok(result)
}

/// `transformCreateTableAsStmt(pstate, stmt)` (analyze.c) — transform a
/// CREATE TABLE AS, SELECT ... INTO, or CREATE MATERIALIZED VIEW statement.
///
/// As with DECLARE CURSOR and EXPLAIN, the contained statement is transformed
/// now; the result is represented as a CMD_UTILITY `Query` wrapping the
/// `CreateTableAsStmt` (whose `query` now holds the analyzed inner `Query`).
fn transformCreateTableAsStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &types_nodes::ddlnodes::CreateTableAsStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;

    /* transform contained query, not allowing SELECT INTO */
    let inner = stmt
        .query
        .as_deref()
        .expect("transformCreateTableAsStmt: stmt->query is NULL");
    let query = transformStmt(mcx, pstate, inner)?;

    /* stmt->query = (Node *) query — rebuild the statement carrying it. */
    let mut new_stmt = stmt.clone_in(mcx)?;

    /* additional work needed for CREATE MATERIALIZED VIEW */
    if stmt.objtype == types_nodes::parsenodes::ObjectType::Matview {
        /*
         * Prohibit a data-modifying CTE in the query used to create a
         * materialized view.
         */
        if query.hasModifyingCTE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("materialized views must not use data-modifying statements in WITH")
                .into_error());
        }

        /*
         * Check whether any temporary database objects are used in the
         * creation query.
         */
        if backend_parser_relation::isQueryUsingTempRelation(mcx, &query)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("materialized views must not use temporary tables or views")
                .into_error());
        }

        /*
         * A materialized view may not be defined using bound parameters.
         */
        if backend_parser_small1::query_contains_extern_params(&query) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("materialized views may not be defined using bound parameters")
                .into_error());
        }

        /*
         * For now, we disallow unlogged materialized views.
         * stmt->into->rel->relpersistence == RELPERSISTENCE_UNLOGGED
         */
        let persistence = new_stmt
            .into
            .as_deref()
            .and_then(Node::as_intoclause)
            .and_then(|into| into.rel.as_deref())
            .and_then(Node::as_rangevar)
            .map(|rv| rv.relpersistence)
            .expect("transformCreateTableAsStmt: stmt->into->rel is not a RangeVar");
        if persistence as u8 == types_tuple::access::RELPERSISTENCE_UNLOGGED {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("materialized views cannot be unlogged")
                .into_error());
        }

        /*
         * At runtime, we'll need a copy of the parsed-but-not-rewritten Query
         * for purposes of creating the view's ON SELECT rule.  Stash it in the
         * IntoClause where intorel_startup() can get it from.
         */
        let view_query = query.clone_in(mcx)?;
        let into = new_stmt
            .into
            .as_deref_mut()
            .and_then(Node::as_intoclause_mut)
            .expect("transformCreateTableAsStmt: stmt->into is not an IntoClause");
        into.viewQuery = Some(mcx::alloc_in(mcx, Node::mk_query(mcx, view_query)?)?);
    }

    /* stmt->query = (Node *) query */
    new_stmt.query = Some(mcx::alloc_in(mcx, Node::mk_query(mcx, query)?)?);

    /* represent the command as a utility Query */
    let mut result = Query::new(mcx);
    result.commandType = CmdType::CMD_UTILITY;
    result.utilityStmt = Some(mcx::alloc_in(mcx, Node::mk_create_table_as_stmt(mcx, new_stmt)?)?);
    Ok(result)
}

/// Helper for the INTO rewrite: clear `intoClause` on the leftmost leaf of a
/// (possibly set-op) `SelectStmt`, matching the C `stmt->intoClause = NULL`.
fn clear_leftmost_into(stmt: &mut SelectStmt<'_>) {
    let mut cur = stmt;
    while cur.op != types_nodes::rawnodes::SETOP_NONE {
        match cur.larg.as_deref_mut() {
            Some(l) => cur = l,
            None => break,
        }
    }
    cur.intoClause = None;
}

/* ===========================================================================
 * transformStmt dispatch
 * =========================================================================== */

/// `transformStmt(pstate, parseTree)` — recursively transform a parse tree into
/// a `Query` tree.
pub fn transformStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    parse_tree: &Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut result: Query<'mcx> = match parse_tree.node_tag() {
        ntag::T_SelectStmt => {
            let n = parse_tree.expect_selectstmt();
            if !n.valuesLists.is_empty() {
                select::transformValuesClause(mcx, pstate, n)?
            } else if n.op == types_nodes::rawnodes::SETOP_NONE {
                select::transformSelectStmt(mcx, pstate, n)?
            } else {
                setop::transformSetOperationStmt(mcx, pstate, n)?
            }
        }
        ntag::T_InsertStmt => insert::transformInsertStmt(mcx, pstate, parse_tree.expect_insertstmt())?,
        ntag::T_ExplainStmt => transformExplainStmt(mcx, pstate, parse_tree.expect_explainstmt())?,
        ntag::T_DeleteStmt => update_delete::transformDeleteStmt(mcx, pstate, parse_tree.expect_deletestmt())?,
        ntag::T_UpdateStmt => update_delete::transformUpdateStmt(mcx, pstate, parse_tree.expect_updatestmt())?,
        ntag::T_CreateTableAsStmt => {
            transformCreateTableAsStmt(mcx, pstate, parse_tree.expect_createtableasstmt())?
        }
        ntag::T_ReturnStmt => {
            select::transformReturnStmt(mcx, pstate, parse_tree.expect_returnstmt())?
        }
        ntag::T_DeclareCursorStmt => {
            special::transformDeclareCursorStmt(mcx, pstate, parse_tree.expect_declarecursorstmt())?
        }
        ntag::T_CallStmt => {
            special::transformCallStmt(mcx, pstate, parse_tree.expect_callstmt())?
        }
        ntag::T_MergeStmt => {
            merge::transformMergeStmt(mcx, pstate, parse_tree.expect_mergestmt())?
        }
        ntag::T_PLAssignStmt => {
            plassign::transformPLAssignStmt(mcx, pstate, parse_tree.expect_plassignstmt())?
        }
        _ => {
            // Other statements don't require transformation: wrap a CMD_UTILITY
            // Query around the original parse tree.
            let mut q = Query::new(mcx);
            q.commandType = CmdType::CMD_UTILITY;
            q.utilityStmt = Some(mcx::alloc_in(mcx, parse_tree.clone_in(mcx)?)?);
            q
        }
    };

    /* Mark as original query until we learn differently */
    result.querySource = QuerySource::QSRC_ORIGINAL;
    result.canSetTag = true;

    Ok(result)
}

/* ===========================================================================
 * stmt_requires_parse_analysis / analyze_requires_snapshot /
 * query_requires_rewrite_plan
 * =========================================================================== */

/// `stmt_requires_parse_analysis(parseTree)` — true if parse analysis does
/// anything non-trivial (more than wrapping a CMD_UTILITY Query).
pub fn stmt_requires_parse_analysis(parse_tree: &RawStmt<'_>) -> bool {
    match parse_tree.stmt.as_ref().node_tag() {
        ntag::T_InsertStmt
        | ntag::T_DeleteStmt
        | ntag::T_UpdateStmt
        | ntag::T_MergeStmt
        | ntag::T_SelectStmt
        | ntag::T_ReturnStmt
        | ntag::T_PLAssignStmt
        | ntag::T_DeclareCursorStmt
        | ntag::T_ExplainStmt
        | ntag::T_CreateTableAsStmt
        | ntag::T_CallStmt => true,
        _ => false,
    }
}

/// `analyze_requires_snapshot(parseTree)` — true if parse analysis requires a
/// snapshot to be set.
pub fn analyze_requires_snapshot(parse_tree: &RawStmt<'_>) -> bool {
    // The C function: result = stmt_requires_parse_analysis(parseTree). (The
    // historical special-casing of A_Expr etc. was removed; it now exactly
    // tracks stmt_requires_parse_analysis.)
    stmt_requires_parse_analysis(parse_tree)
}

/// `query_requires_rewrite_plan(query)` — true unless the Query is a no-op
/// CMD_UTILITY that the rewriter/planner ignore.
pub fn query_requires_rewrite_plan(query: &Query<'_>) -> bool {
    if query.commandType == CmdType::CMD_UTILITY {
        match query.utilityStmt.as_deref().map(|n| n.node_tag()) {
            // These utility statements are optimizable through the
            // rewriter/planner (they embed an optimizable query).
            Some(ntag::T_CreateTableAsStmt)
            | Some(ntag::T_DeclareCursorStmt)
            | Some(ntag::T_ExplainStmt)
            | Some(ntag::T_CallStmt) => true,
            _ => false,
        }
    } else {
        true
    }
}

/* ===========================================================================
 * Seam installation
 * =========================================================================== */

/// Install this crate's inward seams. Currently the cross-cycle consumer
/// contract is `parse_sub_analyze` (consumed by parse_cte and parse_clause).
pub fn init_seams() {
    backend_parser_analyze_seams::parse_sub_analyze::set(parse_sub_analyze);
    // VALUE "requires" predicates plancache calls on the owned RawStmt/Query it
    // stores (the de-handle replaces the handle pc-seam forms). Thin PgResult
    // wrappers over the infallible value bodies above.
    backend_parser_analyze_seams::stmt_requires_parse_analysis_value::set(
        stmt_requires_parse_analysis_value_impl,
    );
    backend_parser_analyze_seams::analyze_requires_snapshot_value::set(
        analyze_requires_snapshot_value_impl,
    );
    backend_parser_analyze_seams::query_requires_rewrite_plan_value::set(
        query_requires_rewrite_plan_value_impl,
    );
    // The inline SQL-function body interpreter (functioncmds.c:910): this is the
    // lowest crate that owns `transformStmt` and the rich node serializer.
    backend_commands_functioncmds_seams::interpret_sql_body::set(interpret_sql_body);
    // Parameter DEFAULT expressions (functioncmds.c:419-447): this crate owns
    // transformExpr, coerce_to_specific_type, assign_expr_collations, and
    // contain_var_clause. It transforms/coerces each raw DEFAULT and serializes
    // the cooked `parameterDefaults` `List` up front (mirrors interpret_sql_body).
    backend_commands_functioncmds_seams::transform_parameter_default::set(
        transform_parameter_default,
    );
    backend_commands_functioncmds_seams::cook_parameter_defaults::set(cook_parameter_defaults);
    // The redefinition default-type compatibility check (pg_proc.c:549-573): owned
    // here because it `stringToNode`s both serialized `proargdefaults` and
    // `exprType`s the elements.
    backend_catalog_pg_proc_seams::check_defaults_compatible::set(check_defaults_compatible);
    // `if (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query,
    // jstate);` (analyze.c:127/169/206). The hook is a per-backend `fn` pointer
    // extensions install; it is NULL by default. With no extension loaded the C
    // `if` guard falls through, so the canonical seam body is a no-op. analyze.c
    // owns this call site, so this crate installs the seam.
    backend_parser_analyze_seams::run_post_parse_analyze_hook::set(
        run_post_parse_analyze_hook_impl,
    );
    // CTAS query-jumble + post-parse-analyze preamble (createas.c 244-249):
    //   if (IsQueryIdEnabled()) jstate = JumbleQuery(query);
    //   if (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query, jstate);
    // queryId jumbling is unported (queryId stays 0, jstate NULL) and the hook is
    // NULL by default, so this is the same no-op as run_post_parse_analyze_hook.
    backend_commands_createas_seams::jumble_and_post_analyze::set(jumble_and_post_analyze_impl);

    // The SQL-function inliner body (clauses.c inline_function): clauses.c runs
    // the catalog gates + active_fns guard + re-simplification in-crate, and
    // rides this seam for the parser-dependent parse/gate/coerce/substitute
    // middle. Installed here — the lowest crate owning both the parser and the
    // fold crate's contain_* walkers without a cycle.
    backend_optimizer_util_clauses_seams::inline_sql_function::set(
        inline_sql::inline_sql_function,
    );

    // The set-returning SQL-function inliner body
    // (clauses.c inline_set_returning_function SQL leg): clauses.c runs the gate
    // ladder in-crate and rides this seam for the parser-dependent body —
    // parse/rewrite/validate the single-SELECT body, check_sql_fn_retval, and
    // substitute the actual call arguments for the body Params. Installed here —
    // the lowest crate owning the parser + rewriter without a cycle.
    backend_optimizer_util_clauses_seams::inline_set_returning_function_sql_body::set(
        inline_srf::inline_set_returning_function_sql_body,
    );
}

/// Seam impl for the CTAS jumble + post-parse-analyze preamble (createas.c
/// 244-249). With query-id jumbling unported and the post-parse-analyze hook
/// NULL by default, both C `if` guards fall through, so this is a no-op.
fn jumble_and_post_analyze_impl<'mcx>(
    _mcx: Mcx<'mcx>,
    _query: &Query<'mcx>,
    _query_string: &str,
) -> PgResult<()> {
    Ok(())
}

/// Seam impl for the post-parse-analyze hook (analyze.c:127/169/206). The hook
/// (`post_parse_analyze_hook`) is NULL unless an extension installs it; with no
/// extension loaded the C `if (post_parse_analyze_hook)` guard falls through, so
/// this is a no-op. Extensions wiring their own hook is a follow-on (the hook
/// `fn`-pointer slot is not modeled until a loadable-module consumer needs it).
fn run_post_parse_analyze_hook_impl(
    _pstate: &types_nodes::portalcmds::ParseState,
    _query: &types_nodes::portalcmds::Query,
    _jstate: Option<&types_nodes::portalcmds::JumbleState>,
) -> PgResult<()> {
    Ok(())
}

/// VALUE seam impl for `stmt_requires_parse_analysis` (infallible body, wrapped
/// in `Ok` for the seam contract).
fn stmt_requires_parse_analysis_value_impl(raw: &RawStmt<'_>) -> PgResult<bool> {
    Ok(stmt_requires_parse_analysis(raw))
}

/// VALUE seam impl for `analyze_requires_snapshot`.
fn analyze_requires_snapshot_value_impl(raw: &RawStmt<'_>) -> PgResult<bool> {
    Ok(analyze_requires_snapshot(raw))
}

/// VALUE seam impl for `query_requires_rewrite_plan`.
fn query_requires_rewrite_plan_value_impl(query: &Query<'_>) -> PgResult<bool> {
    Ok(query_requires_rewrite_plan(query))
}

/* ---- shared assembly helpers ---------------------------------------------- */

/// Wrap a `Vec<SortGroupClause>` (a dep's typed return) into the `List *` of
/// `Node`s the `Query` carries (`PgVec<NodePtr>`).
pub(crate) fn sgc_vec_to_nodes<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<types_nodes::rawnodes::SortGroupClause>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for sgc in v {
        out.push(mcx::alloc_in(mcx, Node::mk_sort_group_clause(mcx, sgc)?)?);
    }
    Ok(out)
}

/// Wrap a `Vec<NodePtr>` (e.g. groupingSets) — already nodes — into a `PgVec`.
pub(crate) fn node_vec_to_pgvec<'mcx>(
    mcx: Mcx<'mcx>,
    v: Vec<NodePtr<'mcx>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for n in v {
        out.push(n);
    }
    Ok(out)
}

/// Wrap an optional `Expr` (a dep's typed clause return) into the `Node *`
/// (`Option<NodePtr>`) a `Query` carries.
pub(crate) fn opt_expr_to_node<'mcx>(
    mcx: Mcx<'mcx>,
    e: Option<types_nodes::primnodes::Expr<'static>>,
) -> PgResult<Option<NodePtr<'mcx>>> {
    match e {
        Some(expr) => Ok(Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, expr.clone_in(mcx)?)?)?)),
        None => Ok(None),
    }
}

/// Wrap an optional `Expr` (a dep's typed clause return) into the
/// concretely-typed `Option<PgBox<Expr>>` an expression-only `Query` field
/// (`havingQual`/`limitOffset`/`limitCount`/`mergeJoinCondition`) carries.
pub(crate) fn opt_expr_to_box<'mcx>(
    mcx: Mcx<'mcx>,
    e: Option<types_nodes::primnodes::Expr<'static>>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::primnodes::Expr<'mcx>>>> {
    match e {
        Some(expr) => Ok(Some(mcx::alloc_in(mcx, expr.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// Wrap a `PgVec<CommonTableExpr>` (transformWithClause return) into the
/// `cteList` (`PgVec<NodePtr>`).
pub(crate) fn cte_vec_to_nodes<'mcx>(
    mcx: Mcx<'mcx>,
    v: PgVec<'mcx, types_nodes::rawnodes::CommonTableExpr<'mcx>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for cte in v {
        out.push(mcx::alloc_in(mcx, Node::mk_common_table_expr(mcx, cte)?)?);
    }
    Ok(out)
}

/// Refresh each `qry->cteList` CTE's `cterefcount` from the final
/// `pstate->p_ctenamespace` (matched by name). In C the cteList and the
/// ctenamespace hold pointers to one shared `CommonTableExpr`, so the
/// `cte->cterefcount++` that `addRangeTableEntryForCTE` does while transforming
/// the body is automatically visible in `qry->cteList`. In the owned model the
/// two are separate clones, so the bumped count must be copied back here, after
/// the whole statement body (FROM / targetlist / WHERE / sublinks) is
/// transformed. Without this the planner sees `cterefcount == 0` and drops
/// every CTE plan.
pub(crate) fn sync_cte_refcounts<'mcx>(
    pstate: &ParseState<'mcx>,
    cte_list: &mut [NodePtr<'mcx>],
) {
    for node in cte_list.iter_mut() {
        if let Some(cte) = node.as_commontableexpr_mut() {
            let name = match cte.ctename.as_deref() {
                Some(n) => n,
                None => continue,
            };
            for ns in pstate.p_ctenamespace.iter() {
                if ns.ctename.as_deref() == Some(name) {
                    cte.cterefcount = ns.cterefcount;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
