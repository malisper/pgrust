#![allow(non_snake_case)]

pub mod parse_cte;
mod set_op;

#[cfg(test)]
mod tests;

use core::mem;

use guc_tables::consts::{
    COMPUTE_QUERY_ID_OFF, COMPUTE_QUERY_ID_ON, COMPUTE_QUERY_ID_REGRESS,
};
use mcx::Mcx;
use parse_clause::{
    transformFromClause, transformGroupClause, transformLimitClause, transformSortClause,
    transformWhereClause, transformWindowDefinitions,
};
use parse_collate::assign_query_collations;
use parse_target::{markTargetListOrigins, resolveTargetListUnknowns, transformTargetList};
use parser_small1::{
    free_parsestate, make_parsestate, setup_parse_fixed_parameters, ParseExprKind, ParseState,
};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, QuerySource};
use types_nodes::primnodes::FromExpr;
use types_nodes::rawnodes::{RawStmt, SelectStmt};
use types_nodes::{Node, NodeTag};
use types_portal::QueryEnvHandle;

pub fn init_seams() {
    analyze_seams::parse_analyze_fixedparams::set(parse_analyze_fixedparams);
    analyze_seams::parse_analyze_sql_fn::set(parse_analyze_sql_fn);
    analyze_seams::parse_analyze_varparams::set(parse_analyze_varparams);
    analyze_seams::analyze_requires_snapshot::set(analyze_requires_snapshot);
    analyze_seams::parse_sub_analyze::set(parse_sub_analyze);
}

pub fn parse_analyze_fixedparams<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &'a RawStmt<'mcx>,
    source_text: &'a str,
    param_types: &'a [Oid],
    query_env: QueryEnvHandle,
) -> PgResult<Query<'mcx>> {
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(mcx::slice_in(mcx, source_text.as_bytes())?.leak());

    if !param_types.is_empty() {
        setup_parse_fixed_parameters(&mut pstate, param_types);
    }

    if !query_env.is_null() {
        panic!(
            "parse_analyze_fixedparams (analyze.c): QueryEnvHandle resolution unported \
             (SPI/trigger transition tables) — unit backend-parser-analyze"
        );
    }

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    if is_query_id_enabled() {
        panic!(
            "parse_analyze_fixedparams (analyze.c): JumbleQuery (queryjumble.c) unported \
             — compute_query_id is on"
        );
    }

    free_parsestate(pstate)?;

    backend_status::pgstat_report_query_id(query.queryId, false);

    Ok(query)
}

pub fn parse_analyze_sql_fn<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &'a RawStmt<'mcx>,
    source_text: &'a str,
    fname: &'a str,
    argtypes: &'a [Oid],
    argnames: &'a [&'a str],
    query_env: QueryEnvHandle,
) -> PgResult<Query<'mcx>> {
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(mcx::slice_in(mcx, source_text.as_bytes())?.leak());

    parser_small1::setup_parse_sql_fn_parameters(
        &mut pstate,
        parser_small1::SqlFnParamState { fname, argtypes, argnames },
    );

    if !query_env.is_null() {
        panic!(
            "parse_analyze_sql_fn (analyze.c): QueryEnvHandle resolution unported \
             (SPI/trigger transition tables) — unit backend-parser-analyze"
        );
    }

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    if is_query_id_enabled() {
        panic!(
            "parse_analyze_sql_fn (analyze.c): JumbleQuery (queryjumble.c) unported \
             — compute_query_id is on"
        );
    }

    free_parsestate(pstate)?;

    backend_status::pgstat_report_query_id(query.queryId, false);

    Ok(query)
}

/// C parse_analyze_varparams: `paramTypes`/`numParams` are in-out there; here
/// the resolved types come back as the second tuple element.
pub fn parse_analyze_varparams<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: &'a RawStmt<'mcx>,
    source_text: &'a str,
    param_types: &'a [Oid],
    query_env: QueryEnvHandle,
) -> PgResult<(Query<'mcx>, mcx::PgVec<'mcx, Oid>)> {
    let mut pstate = make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(mcx::slice_in(mcx, source_text.as_bytes())?.leak());

    let parstate = parser_small1::VarParamState {
        param_types: std::rc::Rc::new(core::cell::RefCell::new(param_types.to_vec())),
    };
    parser_small1::setup_parse_variable_parameters(&mut pstate, parstate.clone());

    if !query_env.is_null() {
        panic!(
            "parse_analyze_varparams (analyze.c): QueryEnvHandle resolution unported \
             (SPI/trigger transition tables) — unit backend-parser-analyze"
        );
    }

    let query = transformTopLevelStmt(mcx, &mut pstate, parse_tree)?;

    // check_variable_parameters walks &'mcx nodes; park the Query in the
    // arena for the walk, then move it back out of the (dead) slot.
    let slot: *mut Query<'mcx> = mcx::leak_in(mcx::alloc_in(mcx, query)?);
    // SAFETY: the walk's shared borrows end before the take; the arena slot
    // is reachable only through `slot`.
    parser_small1::check_variable_parameters(
        &pstate,
        unsafe { &*slot },
        mbutils::GetDatabaseEncoding(),
    )?;
    let query = mem::take(unsafe { &mut *slot });

    if is_query_id_enabled() {
        panic!(
            "parse_analyze_varparams (analyze.c): JumbleQuery (queryjumble.c) unported \
             — compute_query_id is on"
        );
    }

    free_parsestate(pstate)?;

    backend_status::pgstat_report_query_id(query.queryId, false);

    let resolved = parstate.param_types.borrow();
    let mut out: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    out.try_reserve_exact(resolved.len()).map_err(|_| mcx.oom(resolved.len()))?;
    for &t in resolved.iter() {
        out.push(t);
    }
    Ok((query, out))
}

// C `IsQueryIdEnabled` (queryjumble.h); the AUTO arm reads `query_id_enabled`,
// which only `EnableQueryId()` (unported jumble consumers) ever sets.
fn is_query_id_enabled() -> bool {
    match guc_tables::backing::compute_query_id() {
        COMPUTE_QUERY_ID_OFF => false,
        COMPUTE_QUERY_ID_ON | COMPUTE_QUERY_ID_REGRESS => true,
        _ => false,
    }
}

pub fn parse_sub_analyze<'mcx>(
    mcx: Mcx<'mcx>,
    parse_tree: Node<'mcx>,
    parent_parse_state: &ParseState<'_, 'mcx>,
    parent_cte: Option<Node<'mcx>>,
    locked_from_parent: bool,
    resolve_unknowns: bool,
) -> PgResult<Query<'mcx>> {
    let mut pstate = make_parsestate(mcx, Some(parent_parse_state));
    pstate.p_parent_cte = parent_cte;
    pstate.p_locked_from_parent = locked_from_parent;
    pstate.p_resolve_unknowns = resolve_unknowns;

    let query = transformStmt(mcx, &mut pstate, parse_tree)?;

    free_parsestate(pstate)?;
    Ok(query)
}

pub fn transformTopLevelStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    parse_tree: &RawStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let stmt = parse_tree.stmt.expect("RawStmt.stmt is never NULL");
    let mut result = transformOptionalSelectInto(mcx, pstate, stmt)?;
    result.stmt_location = parse_tree.stmt_location;
    result.stmt_len = parse_tree.stmt_len;
    Ok(result)
}

fn transformOptionalSelectInto<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    parse_tree: Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut parse_tree = parse_tree;
    if let Some(stmt) = parse_tree.as_select_stmt() {
        let mut leftmost = stmt;
        while leftmost.op != types_nodes::parsenodes::SetOperation::SETOP_NONE {
            leftmost = leftmost.larg.expect("set-op tree always has a leftmost SelectStmt");
        }
        let has_into = leftmost.intoClause.is_some();
        let is_setop = stmt.op != types_nodes::parsenodes::SetOperation::SETOP_NONE;
        if has_into {
            if is_setop {
                // C NULLs the leftmost's intoClause through its pointer;
                // larg is a shared borrow here, so the clear cannot reach it.
                panic!(
                    "transformOptionalSelectInto (analyze.c): SELECT INTO on a \
                     set-operation tree needs mutable larg access — \
                     unit backend-parser-analyze"
                );
            }
            // SAFETY: raw tree owned by this analysis; no derived reference
            // is live (leftmost/stmt dropped above).
            let into = unsafe {
                parse_tree.with_mut::<SelectStmt, _>(|s| s.intoClause.take())
            }
            .expect("node checked as SelectStmt");
            let mut ctas = Node::build::<types_nodes::rawnodes::CreateTableAsStmt>(mcx)?;
            ctas.query = Some(parse_tree);
            ctas.into = into;
            ctas.objtype = types_nodes::parsenodes::ObjectType::OBJECT_TABLE;
            ctas.is_select_into = true;
            parse_tree = ctas.seal();
        }
    }
    transformStmt(mcx, pstate, parse_tree)
}

pub fn transformStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    parse_tree: Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut result = match parse_tree.node_tag() {
        NodeTag::T_SelectStmt => {
            let n = parse_tree.as_select_stmt().unwrap();
            if !n.valuesLists.is_nil() {
                transformValuesClause(mcx, pstate, n)?
            } else if n.op == types_nodes::parsenodes::SetOperation::SETOP_NONE {
                transformSelectStmt(mcx, pstate, n)?
            } else {
set_op::transformSetOperationStmt(mcx, pstate, n)?
            }
        }
        NodeTag::T_InsertStmt => {
            transformInsertStmt(mcx, pstate, parse_tree.as_insert_stmt().unwrap())?
        }
        NodeTag::T_DeleteStmt => {
            transformDeleteStmt(mcx, pstate, parse_tree.as_delete_stmt().unwrap())?
        }
        NodeTag::T_UpdateStmt => {
            transformUpdateStmt(mcx, pstate, parse_tree.as_update_stmt().unwrap())?
        }
        NodeTag::T_ExplainStmt => {
            transformExplainStmt(mcx, pstate, parse_tree)?
        }
        NodeTag::T_DeclareCursorStmt => {
            transformDeclareCursorStmt(mcx, pstate, parse_tree)?
        }
        NodeTag::T_CreateTableAsStmt => {
            transformCreateTableAsStmt(mcx, pstate, parse_tree)?
        }
        t @ (NodeTag::T_MergeStmt
        | NodeTag::T_ReturnStmt
        | NodeTag::T_PLAssignStmt
        | NodeTag::T_CallStmt) => panic!(
            "transformStmt (analyze.c): transform arm for {t:?} unported — \
             unit backend-parser-analyze"
        ),
        _ => {
            let mut result = Query::default();
            result.commandType = CmdType::CMD_UTILITY;
            result.utilityStmt = Some(parse_tree);
            result
        }
    };

    result.querySource = QuerySource::QSRC_ORIGINAL;
    result.canSetTag = true;
    Ok(result)
}

fn transformCreateTableAsStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    ctas_node: Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let stmt = ctas_node
        .as_variant::<types_nodes::rawnodes::CreateTableAsStmt>()
        .expect("CreateTableAsStmt");
    let is_matview = stmt.objtype == types_nodes::parsenodes::ObjectType::OBJECT_MATVIEW;
    if stmt.objtype != types_nodes::parsenodes::ObjectType::OBJECT_TABLE && !is_matview {
        panic!(
            "transformCreateTableAsStmt (analyze.c): objtype {:?} arm unported — \
             unit backend-parser-analyze",
            stmt.objtype
        );
    }
    let into_node = stmt.into.expect("CreateTableAsStmt.into");
    let inner = stmt.query.expect("CreateTableAsStmt has a query");
    let query = transformStmt(mcx, pstate, inner)?;
    if is_matview {
        let matview_err = |msg: &'static str| {
            elog::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(msg)
                .into_error()
        };
        if query.hasModifyingCTE {
            return Err(matview_err(
                "materialized views must not use data-modifying statements in WITH",
            )
            .into());
        }
        if is_query_using_temp_relation(mcx, &query)? {
            return Err(
                matview_err("materialized views must not use temporary tables or views").into()
            );
        }
        // query_contains_extern_params: PARAM_EXTERN is unreachable — every
        // live analysis entry passes zero fixed params.
        let into = into_node
            .as_variant::<types_nodes::rawnodes::IntoClause>()
            .expect("IntoClause");
        let rel = into.rel.expect("IntoClause.rel").as_range_var().expect("RangeVar");
        if rel.relpersistence == types_core::catalog::RELPERSISTENCE_UNLOGGED {
            return Err(matview_err("materialized views cannot be unlogged").into());
        }
    }
    let query_node = Node::mk(mcx, query)?;
    if is_matview {
        // C: into->viewQuery = copyObject(query). The one handle doubles as
        // the is_matview discriminator; ExecCreateTableAs is the single
        // consumer and takes the Query exactly once.
        // SAFETY: raw tree owned by this analysis; no derived reference is live.
        unsafe {
            into_node
                .with_mut::<types_nodes::rawnodes::IntoClause, _>(|ic| {
                    ic.viewQuery = Some(query_node)
                })
                .expect("node built as IntoClause");
        }
    }
    // SAFETY: raw tree owned by this analysis; no derived reference is live.
    unsafe {
        ctas_node
            .with_mut::<types_nodes::rawnodes::CreateTableAsStmt, _>(|c| {
                c.query = Some(query_node)
            })
            .expect("node built as CreateTableAsStmt");
    }

    let mut result = Query::default();
    result.commandType = CmdType::CMD_UTILITY;
    result.utilityStmt = Some(ctas_node);
    Ok(result)
}

// isQueryUsingTempRelation (rewriteManip.c). C also walks sublinks via
// query_tree_walker; that leg is loud, gated on a live temp namespace so it
// cannot fire while no temp relation can exist in-session.
fn is_query_using_temp_relation<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
) -> PgResult<bool> {
    use types_nodes::parsenodes::RTEKind;
    if query.hasSubLinks && catalog_namespace::my_temp_namespace() != types_core::InvalidOid {
        panic!(
            "isQueryUsingTempRelation (rewriteManip.c): sublink walk unported \
             (query_tree_walker) — unit backend-commands-matview"
        );
    }
    for node in query.rtable.iter() {
        let rte = node.as_range_tbl_entry().expect("rtable holds RangeTblEntry nodes");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                if lsyscache::get_rel_persistence(rte.relid)?
                    == types_core::catalog::RELPERSISTENCE_TEMP as i8
                {
                    return Ok(true);
                }
            }
            RTEKind::RTE_SUBQUERY => {
                let sub = rte.subquery.expect("subquery RTE carries a Query");
                if is_query_using_temp_relation(mcx, sub)? {
                    return Ok(true);
                }
            }
            _ => {}
        }
    }
    for cte in query.cteList.iter() {
        let cte = cte.as_common_table_expr().expect("cteList holds CommonTableExpr");
        let q = cte.ctequery.expect("analyzed CTE query");
        if is_query_using_temp_relation(mcx, q.as_query().expect("Query"))? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn transformDeclareCursorStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cursor_node: Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_error::{ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_CURSOR_DEFINITION};
    use types_portal::{
        CURSOR_OPT_ASENSITIVE, CURSOR_OPT_HOLD, CURSOR_OPT_INSENSITIVE, CURSOR_OPT_NO_SCROLL,
        CURSOR_OPT_SCROLL,
    };

    let stmt = cursor_node.as_declare_cursor_stmt().unwrap();

    if stmt.options & CURSOR_OPT_SCROLL != 0 && stmt.options & CURSOR_OPT_NO_SCROLL != 0 {
        return Err(elog::ereport(types_error::ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg("cannot specify both SCROLL and NO SCROLL")
            .into_error()
            .into());
    }
    if stmt.options & CURSOR_OPT_ASENSITIVE != 0 && stmt.options & CURSOR_OPT_INSENSITIVE != 0 {
        return Err(elog::ereport(types_error::ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg("cannot specify both ASENSITIVE and INSENSITIVE")
            .into_error()
            .into());
    }

    let inner = stmt.query.expect("DECLARE CURSOR has a query");
    let query = transformStmt(mcx, pstate, inner)?;

    if query.commandType != CmdType::CMD_SELECT {
        return Err(elog::ereport(types_error::ERROR)
            .errmsg_internal("unexpected non-SELECT command in DECLARE CURSOR")
            .into_error()
            .into());
    }

    if query.hasModifyingCTE {
        return Err(elog::ereport(types_error::ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("DECLARE CURSOR must not contain data-modifying statements in WITH")
            .into_error()
            .into());
    }

    // C's three FOR UPDATE conflict ereports (WITH HOLD / SCROLL /
    // INSENSITIVE): rowMarks is always NIL today — transformLockingClause is
    // a loud panic upstream — so a non-NIL list here is a missed wire-up.
    if !query.rowMarks.is_nil() {
        assert!(
            stmt.options & (CURSOR_OPT_HOLD | CURSOR_OPT_SCROLL | CURSOR_OPT_INSENSITIVE) == 0,
            "transformDeclareCursorStmt (analyze.c): FOR UPDATE conflict ereports need \
             LCS_asString (LockingClause vocabulary) — unit backend-parser-analyze"
        );
    }

    let query_node = Node::mk(mcx, query)?;
    // SAFETY: raw tree owned by this analysis; no derived reference is live.
    unsafe {
        cursor_node
            .with_mut::<types_nodes::parsenodes::DeclareCursorStmt, _>(|d| {
                d.query = Some(query_node)
            })
            .expect("node built as DeclareCursorStmt");
    }

    let mut result = Query::default();
    result.commandType = CmdType::CMD_UTILITY;
    result.utilityStmt = Some(cursor_node);
    Ok(result)
}

fn transformExplainStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    explain_node: Node<'mcx>,
) -> PgResult<Query<'mcx>> {
    let stmt = explain_node.as_explain_stmt().unwrap();
    if matches!(pstate.p_ref_hook_state, parser_small1::ParseRefHookState::None) {
        for opt in stmt.options.iter() {
            let opt = opt.as_def_elem().expect("EXPLAIN options are DefElems");
            // C checks defGetBoolean; GENERIC_PLAN in any form is loud here.
            if opt.defname == Some("generic_plan") {
                panic!(
                    "transformExplainStmt (analyze.c): GENERIC_PLAN variable-parameter \
                     setup unported"
                );
            }
        }
    }
    let inner = stmt.query.expect("ExplainStmt has a query");
    let analyzed = transformOptionalSelectInto(mcx, pstate, inner)?;
    let query_node = Node::mk(mcx, analyzed)?;
    // SAFETY: raw tree owned by this analysis; no derived reference is live.
    unsafe {
        explain_node
            .with_mut::<types_nodes::parsenodes::ExplainStmt, _>(|e| e.query = Some(query_node))
            .expect("node built as ExplainStmt");
    }
    let mut result = Query::default();
    result.commandType = CmdType::CMD_UTILITY;
    result.utilityStmt = Some(explain_node);
    Ok(result)
}

pub fn stmt_requires_parse_analysis(parse_tree: &RawStmt<'_>) -> bool {
    let stmt = parse_tree.stmt.expect("RawStmt.stmt is never NULL");
    matches!(
        stmt.node_tag(),
        NodeTag::T_InsertStmt
            | NodeTag::T_DeleteStmt
            | NodeTag::T_UpdateStmt
            | NodeTag::T_MergeStmt
            | NodeTag::T_SelectStmt
            | NodeTag::T_ReturnStmt
            | NodeTag::T_PLAssignStmt
            | NodeTag::T_DeclareCursorStmt
            | NodeTag::T_ExplainStmt
            | NodeTag::T_CreateTableAsStmt
            | NodeTag::T_CallStmt
    )
}

pub fn analyze_requires_snapshot(parse_tree: &RawStmt<'_>) -> bool {
    stmt_requires_parse_analysis(parse_tree)
}

fn transformSelectStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_SELECT;

    if let Some(with) = stmt.withClause {
        qry.hasRecursive = with.as_with_clause().expect("withClause").recursive;
        qry.cteList = parse_cte::transformWithClause(mcx, pstate, with)?;
        qry.hasModifyingCTE = pstate.p_hasModifyingCTE;
    }

    if let Some(into) = stmt.intoClause {
        return Err(select_into_error(pstate, into, "SELECT ... INTO is not allowed here"));
    }

    // C aliases the raw lists into pstate; header-clone until a shared-list
    // story exists (both are on loud-panic statement shapes today).
    if !stmt.lockingClause.is_nil() {
        pstate.p_locking_clause = stmt.lockingClause.clone_in(mcx)?;
    }
    if !stmt.windowClause.is_nil() {
        pstate.p_windowdefs = stmt.windowClause.clone_in(mcx)?;
    }

    transformFromClause(mcx, pstate, &stmt.fromClause)?;

    qry.targetList =
        transformTargetList(mcx, pstate, &stmt.targetList, ParseExprKind::EXPR_KIND_SELECT_TARGET)?;

    markTargetListOrigins(pstate, &qry.targetList)?;

    let qual = transformWhereClause(
        mcx,
        pstate,
        stmt.whereClause,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    qry.havingQual = transformWhereClause(
        mcx,
        pstate,
        stmt.havingClause,
        ParseExprKind::EXPR_KIND_HAVING,
        "HAVING",
    )?;

    qry.sortClause = transformSortClause(
        mcx,
        pstate,
        &stmt.sortClause,
        &mut qry.targetList,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;

    qry.groupClause = transformGroupClause(
        mcx,
        pstate,
        &stmt.groupClause,
        &mut qry.groupingSets,
        &mut qry.targetList,
        &qry.sortClause,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )?;
    qry.groupDistinct = stmt.groupDistinct;

    match &stmt.distinctClause {
        types_nodes::rawnodes::DistinctClause::None => {}
        types_nodes::rawnodes::DistinctClause::All => {
            qry.distinctClause = parse_clause::transformDistinctClause(
                mcx,
                pstate,
                &mut qry.targetList,
                &qry.sortClause,
                false,
            )?;
            qry.hasDistinctOn = false;
        }
        types_nodes::rawnodes::DistinctClause::On(_) => panic!(
            "transformSelectStmt (analyze.c): transformDistinctOnClause (DISTINCT ON) \
             unported — unit backend-parser-clause"
        ),
    }

    qry.limitOffset = transformLimitClause(
        mcx,
        pstate,
        stmt.limitOffset,
        ParseExprKind::EXPR_KIND_OFFSET,
        "OFFSET",
        stmt.limitOption,
    )?;
    qry.limitCount = transformLimitClause(
        mcx,
        pstate,
        stmt.limitCount,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        stmt.limitOption,
    )?;
    qry.limitOption = stmt.limitOption;

    let windowdefs = mem::take(&mut pstate.p_windowdefs);
    qry.windowClause = transformWindowDefinitions(mcx, pstate, &windowdefs, &mut qry.targetList)?;

    if pstate.p_resolve_unknowns {
        resolveTargetListUnknowns(mcx, pstate, &qry.targetList)?;
    }

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: qual })?
            .seal_ref(),
    );

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    for lc_node in &stmt.lockingClause {
        let lc = lc_node.as_locking_clause().expect("lockingClause cell");
        transformLockingClause(mcx, pstate, &mut qry, lc, false)?;
    }

    assign_query_collations(mcx, pstate, &qry)?;

    if pstate.p_hasAggs
        || !qry.groupClause.is_nil()
        || !qry.groupingSets.is_nil()
        || qry.havingQual.is_some()
    {
        parse_agg::parseCheckAggregates(mcx, pstate, &mut qry)?;
    }

    Ok(qry)
}

fn transformValuesClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &SelectStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_SELECT;

    debug_assert!(matches!(stmt.distinctClause, types_nodes::rawnodes::DistinctClause::None));
    debug_assert!(stmt.intoClause.is_none());
    debug_assert!(stmt.targetList.is_nil());
    debug_assert!(stmt.fromClause.is_nil());
    debug_assert!(stmt.whereClause.is_none());
    debug_assert!(stmt.groupClause.is_nil());
    debug_assert!(stmt.havingClause.is_none());
    debug_assert!(stmt.windowClause.is_nil());
    debug_assert!(stmt.op == types_nodes::parsenodes::SetOperation::SETOP_NONE);

    if stmt.withClause.is_some() {
        panic!(
            "transformValuesClause (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }

    let mut colexprs: mcx::PgVec<'mcx, types_nodes::NodeList<'mcx>> = mcx::PgVec::new_in(mcx);
    let mut sublist_length: i64 = -1;
    for sublist_node in &stmt.valuesLists {
        let sublist = sublist_node.as_list().expect("VALUES row is a List");
        let sublist = parse_target::transformExpressionList(
            mcx,
            pstate,
            sublist,
            ParseExprKind::EXPR_KIND_VALUES,
            false,
        )?;
        if sublist_length < 0 {
            sublist_length = sublist.len() as i64;
            colexprs.try_reserve_exact(sublist.len()).map_err(|_| mcx.oom(sublist.len()))?;
            for _ in 0..sublist.len() {
                colexprs.push(types_nodes::NodeList::nil());
            }
        } else if sublist_length != sublist.len() as i64 {
            return Err(values_length_mismatch(pstate, &sublist));
        }
        for (i, col) in sublist.iter().enumerate() {
            colexprs[i].lappend(mcx, col)?;
        }
    }

    let mut coltypes = types_nodes::list::OidList::nil();
    let mut coltypmods = types_nodes::list::IntList::nil();
    let mut colcollations = types_nodes::list::OidList::nil();
    for column in colexprs.iter_mut() {
        let mut type_locs: mcx::PgVec<'_, (Oid, types_core::ParseLoc)> =
            mcx::vec_with_capacity_in(mcx, column.len())?;
        for col in column.iter() {
            type_locs.push((parse_expr::expr_type(col), parse_expr::expr_location(col)));
        }
        let coltype = coerce::select_common_type(pstate, &type_locs, Some("VALUES"))?;

        let mut coerced = types_nodes::NodeList::nil();
        let mut type_mods: mcx::PgVec<'_, (Oid, i32)> =
            mcx::vec_with_capacity_in(mcx, column.len())?;
        for col in column.iter() {
            let col = coerce::coerce_to_common_type(
                mcx,
                pstate,
                col,
                parse_expr::expr_type(col),
                parse_expr::expr_location(col),
                coltype,
                "VALUES",
            )?;
            type_mods.push((parse_expr::expr_type(col), parse_expr::expr_typmod(col)));
            coerced.lappend(mcx, col)?;
        }
        *column = coerced;

        let coltypmod = coerce::select_common_typmod(&type_mods, coltype);
        let colcoll = parse_collate::select_common_collation(mcx, pstate, column, true)?;

        coltypes.lappend(mcx, coltype)?;
        coltypmods.lappend(mcx, coltypmod)?;
        colcollations.lappend(mcx, colcoll)?;
    }

    let mut exprs_lists = types_nodes::NodeList::nil();
    for r in 0..stmt.valuesLists.len() {
        let mut row = types_nodes::NodeList::nil();
        for column in colexprs.iter() {
            row.lappend(mcx, column.nth(r))?;
        }
        exprs_lists.lappend(mcx, Node::mk_list(mcx, row)?)?;
    }

    // C marks the RTE LATERAL when NEW/OLD Vars appear (CREATE RULE only);
    // no rule machinery can put an RTE in this pstate yet.
    if !pstate.p_rtable.is_nil() {
        panic!(
            "transformValuesClause (analyze.c): contain_vars_of_level LATERAL arm \
             (CREATE RULE NEW/OLD) unported — unit backend-parser-analyze"
        );
    }

    let nsitem = parse_relation::addRangeTableEntryForValues(
        mcx,
        pstate,
        exprs_lists,
        coltypes,
        coltypmods,
        colcollations,
        None,
        false,
        true,
    )?;
    // C calls addNSItemToQuery before expandNSItemAttrs; swapped because the
    // former consumes the &mut nsitem, and neither reads the other's effects.
    debug_assert_eq!(pstate.p_next_resno, 1);
    qry.targetList = parse_relation::expandNSItemAttrs(mcx, pstate, nsitem, 0, true, -1)?;
    parse_relation::addNSItemToQuery(mcx, pstate, nsitem, true, true, true)?;

    qry.sortClause = transformSortClause(
        mcx,
        pstate,
        &stmt.sortClause,
        &mut qry.targetList,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        false,
    )?;

    qry.limitOffset = transformLimitClause(
        mcx,
        pstate,
        stmt.limitOffset,
        ParseExprKind::EXPR_KIND_OFFSET,
        "OFFSET",
        stmt.limitOption,
    )?;
    qry.limitCount = transformLimitClause(
        mcx,
        pstate,
        stmt.limitCount,
        ParseExprKind::EXPR_KIND_LIMIT,
        "LIMIT",
        stmt.limitOption,
    )?;
    qry.limitOption = stmt.limitOption;

    if !stmt.lockingClause.is_nil() {
        return Err(locking_not_applicable_to(
            first_locking_strength(&stmt.lockingClause),
            "VALUES",
            "transformValuesClause",
        ));
    }

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: None })?
            .seal_ref(),
    );

    qry.hasSubLinks = pstate.p_hasSubLinks;

    assign_query_collations(mcx, pstate, &qry)?;

    Ok(qry)
}

fn transformInsertStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &types_nodes::InsertStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::parsenodes::ACL_INSERT;

    let mut qry = Query::default();
    debug_assert!(pstate.p_ctenamespace.is_nil());
    qry.commandType = CmdType::CMD_INSERT;
    pstate.p_is_insert = true;

    if stmt.withClause.is_some() {
        panic!(
            "transformInsertStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }
    qry.r#override = stmt.r#override;

    let select_stmt = stmt.selectStmt.map(|n| n.as_select_stmt().expect("grammar builds SelectStmt"));
    let is_general_select = select_stmt.is_some_and(|s| {
        s.valuesLists.is_nil()
            || !s.sortClause.is_nil()
            || s.limitOffset.is_some()
            || s.limitCount.is_some()
            || !s.lockingClause.is_nil()
            || s.withClause.is_some()
    });
    // The CREATE RULE rtable pass-down only matters for isGeneralSelect.
    debug_assert!(pstate.p_rtable.is_nil());

    let relation = stmt
        .relation
        .expect("grammar always sets InsertStmt.relation")
        .as_range_var()
        .expect("insert_target is a RangeVar");
    qry.resultRelation =
        parse_clause::setTargetTable(mcx, pstate, relation, false, false, ACL_INSERT)?;

    let (icolumns, attrnos) = parse_target::checkInsertTargets(mcx, pstate, &stmt.cols)?;
    debug_assert_eq!(icolumns.len(), attrnos.len());

    let expr_list: types_nodes::NodeList<'mcx> = match select_stmt {
        None => types_nodes::NodeList::nil(),
        Some(_) if is_general_select => {
            // C hands the sub-SELECT an unknowns-unresolved pstate so target
            // columns drive the coercion below.
            let select_query = parse_sub_analyze(
                mcx,
                stmt.selectStmt.expect("isGeneralSelect implies selectStmt"),
                pstate,
                None,
                false,
                false,
            )?;
            if select_query.commandType != CmdType::CMD_SELECT {
                return Err(Box::new(types_error::PgError::error(
                    "unexpected non-SELECT command in INSERT ... SELECT".to_string(),
                )));
            }

            let query_node = Node::mk(mcx, select_query)?;
            let select_query = query_node.as_query().expect("just built");

            let mut columns: mcx::PgVec<'mcx, (Option<&'mcx str>, Oid, i32, Oid)> =
                mcx::vec_with_capacity_in(mcx, select_query.targetList.len())?;
            for tle_node in &select_query.targetList {
                let te = tle_node.as_target_entry().expect("tlist cell");
                if te.resjunk {
                    continue;
                }
                columns.push((
                    te.resname,
                    parse_expr::expr_type(te.expr),
                    parse_expr::expr_typmod(te.expr),
                    parse_expr::expr_collation(te.expr),
                ));
            }

            let alias = Node::mk_mut(
                mcx,
                types_nodes::primnodes::Alias {
                    aliasname: Some("*SELECT*"),
                    colnames: types_nodes::NodeList::nil(),
                },
            )?
            .seal_ref();
            let nsitem = parse_relation::addRangeTableEntryForSubquery(
                mcx,
                pstate,
                select_query,
                &columns,
                Some(alias),
                false,
                false,
            )?;
            let rtindex = nsitem.p_rtindex;
            parse_relation::addNSItemToQuery(mcx, pstate, nsitem, true, false, false)?;

            // Unknown-type Consts/Params are copied up as-is so coerce_type
            // can resolve them to the target column's type (C's HACK note).
            let mut sub_exprs = types_nodes::NodeList::nil();
            for tle_node in &select_query.targetList {
                let te = tle_node.as_target_entry().expect("tlist cell");
                if te.resjunk {
                    continue;
                }
                let expr = if matches!(te.expr.node_tag(), NodeTag::T_Const | NodeTag::T_Param)
                    && parse_expr::expr_type(te.expr) == types_core::catalog::UNKNOWNOID
                {
                    te.expr
                } else {
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::Var {
                            varno: rtindex,
                            varattno: te.resno,
                            vartype: parse_expr::expr_type(te.expr),
                            vartypmod: parse_expr::expr_typmod(te.expr),
                            varcollid: parse_expr::expr_collation(te.expr),
                            varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                            varlevelsup: 0,
                            varreturningtype:
                                types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                            varnosyn: rtindex as types_core::Index,
                            varattnosyn: te.resno,
                            location: parse_expr::expr_location(te.expr),
                        },
                    )?
                };
                sub_exprs.lappend(mcx, expr)?;
            }
            transformInsertRow(mcx, pstate, sub_exprs, &stmt.cols, &icolumns, &attrnos, false)?
        }
        Some(sel) if sel.valuesLists.len() > 1 => {
            let mut exprs_lists = types_nodes::NodeList::nil();
            let mut coltypes = types_nodes::list::OidList::nil();
            let mut coltypmods = types_nodes::list::IntList::nil();
            let mut colcollations = types_nodes::list::OidList::nil();
            let mut sublist_length: i64 = -1;
            for sublist_node in &sel.valuesLists {
                let sublist = sublist_node.as_list().expect("VALUES row is a List");
                let sublist = parse_target::transformExpressionList(
                    mcx,
                    pstate,
                    sublist,
                    ParseExprKind::EXPR_KIND_VALUES,
                    true,
                )?;
                if sublist_length < 0 {
                    sublist_length = sublist.len() as i64;
                } else if sublist_length != sublist.len() as i64 {
                    return Err(values_length_mismatch(pstate, &sublist));
                }
                let sublist =
                    transformInsertRow(mcx, pstate, sublist, &stmt.cols, &icolumns, &attrnos, true)?;
                parse_collate::assign_list_collations(mcx, pstate, &sublist)?;
                exprs_lists.lappend(mcx, Node::mk_list(mcx, sublist)?)?;
            }

            for val in exprs_lists.nth(0).as_list().expect("row list").iter() {
                coltypes.lappend(mcx, parse_expr::expr_type(val))?;
                coltypmods.lappend(mcx, parse_expr::expr_typmod(val))?;
                colcollations.lappend(mcx, 0)?;
            }

            // contain_vars_of_level lateral marking only fires inside CREATE
            // RULE (NEW/OLD in the rtable); the target rel is the only RTE.
            debug_assert_eq!(pstate.p_rtable.len(), 1);
            let nsitem = parse_relation::addRangeTableEntryForValues(
                mcx,
                pstate,
                exprs_lists,
                coltypes,
                coltypmods,
                colcollations,
                None,
                false,
                true,
            )?;
            let (vars, _names) = parse_relation::expandNSItemVars(mcx, pstate, nsitem, 0, -1)?;
            parse_relation::addNSItemToQuery(mcx, pstate, nsitem, true, false, false)?;
            transformInsertRow(mcx, pstate, vars, &stmt.cols, &icolumns, &attrnos, false)?
        }
        Some(sel) => {
            debug_assert_eq!(sel.valuesLists.len(), 1);
            debug_assert!(sel.intoClause.is_none());
            let values = sel.valuesLists.nth(0).as_list().expect("VALUES row is a List");
            let expr_list = parse_target::transformExpressionList(
                mcx,
                pstate,
                values,
                ParseExprKind::EXPR_KIND_VALUES_SINGLE,
                true,
            )?;
            transformInsertRow(mcx, pstate, expr_list, &stmt.cols, &icolumns, &attrnos, false)?
        }
    };

    let perminfo = pstate
        .p_target_nsitem
        .expect("setTargetTable set p_target_nsitem")
        .p_perminfo
        .expect("target nsitem has perminfo");
    debug_assert!(expr_list.len() <= icolumns.len());
    let mut target_list = types_nodes::NodeList::nil();
    for (i, (expr, icol)) in expr_list.iter().zip(icolumns.iter()).enumerate() {
        let col = icol.as_res_target().expect("icolumns are ResTargets");
        let attr_num = attrnos[i] as i16;
        let tle = Node::mk_target_entry(mcx, expr, attr_num, col.name, false)?;
        target_list.lappend(mcx, tle)?;
        // SAFETY: perminfo nodes are read only through transient as_*
        // lookups; no derived reference is live across this write.
        unsafe {
            perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                p.insertedCols.add_member(
                    mcx,
                    attr_num as i32 - types_tuple::htup::FirstLowInvalidHeapAttributeNumber,
                )
            })
        }
        .expect("p_perminfo is RTEPermissionInfo")?;
    }
    qry.targetList = target_list;

    // Clauses past this point see only the target relation, removing any
    // entries added in a sub-SELECT or VALUES list.
    if stmt.onConflictClause.is_some() || stmt.returningClause.is_some() {
        pstate.p_namespace.clear();
        let nsitem = returning_target_nsitem(mcx, pstate)?;
        parse_relation::addNSItemToQuery(mcx, pstate, nsitem, false, true, true)?;
    }

    if let Some(clause) = stmt.onConflictClause {
        qry.onConflict = Some(transformOnConflictClause(mcx, pstate, clause)?);
    }

    if stmt.returningClause.is_some() {
        transformReturningClause(
            mcx,
            pstate,
            &mut qry,
            stmt.returningClause,
            ParseExprKind::EXPR_KIND_RETURNING,
        )?;
    }

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: None })?
            .seal_ref(),
    );
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    assign_query_collations(mcx, pstate, &qry)?;
    Ok(qry)
}

// C `transformOnConflictClause` (analyze.c).
fn transformOnConflictClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    clause_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes::OnConflictAction;

    let clause = clause_node.as_on_conflict_clause().expect("grammar builds OnConflictClause");

    let mut excl_rel_index = 0i32;
    let mut excl_rel_tlist = types_nodes::NodeList::nil();
    let mut excl_nsitem = None;

    if clause.action == OnConflictAction::ONCONFLICT_UPDATE {
        // The EXCLUDED pseudo relation must exist while arbiter expressions
        // are processed (referencing it from there draws a useful error).
        let targetrel = pstate
            .p_target_relation
            .as_ref()
            .expect("setTargetTable set p_target_relation")
            .alias();
        let alias = mcx::leak_in(mcx::alloc_in(
            mcx,
            types_nodes::Alias { aliasname: Some("excluded"), colnames: types_nodes::NodeList::nil() },
        )?);
        let nsitem = parse_relation::addRangeTableEntryForRelation(
            mcx,
            pstate,
            &targetrel,
            types_rel::RowExclusiveLock,
            Some(alias),
            false,
            false,
        )?;
        excl_rel_index = nsitem.p_rtindex;
        excl_nsitem = Some(nsitem);
        // Composite relkind signals this is not an actual relation and needs
        // no permission checks; the real target relation is checked instead.
        let rte_node = pstate.p_rtable.nth(excl_rel_index as usize - 1);
        // SAFETY: parser-owned rtable; nsitem's p_rte probe is not read
        // before the namespace lookups that follow rebuild it.
        unsafe {
            rte_node.with_mut::<types_nodes::RangeTblEntry, _>(|r| {
                r.relkind = types_rel::RELKIND_COMPOSITE_TYPE
            })
        }
        .expect("rtable holds RangeTblEntry");

        excl_rel_tlist = BuildOnConflictExcludedTargetlist(mcx, &targetrel, excl_rel_index)?;
    }

    let (arbiter_elems, arbiter_where, constraint) =
        parse_clause::transformOnConflictArbiter(mcx, pstate, clause)?;

    let mut on_conflict_set = types_nodes::NodeList::nil();
    let mut on_conflict_where = None;
    if clause.action == OnConflictAction::ONCONFLICT_UPDATE {
        // The UPDATE subexpressions are handled like UPDATE, not INSERT; all
        // INSERT expressions were parsed already.
        pstate.p_is_insert = false;

        parse_relation::addNSItemToQuery(
            mcx,
            pstate,
            excl_nsitem.take().expect("built above"),
            false,
            true,
            true,
        )?;

        on_conflict_set = transformUpdateTargetList(mcx, pstate, &clause.targetList)?;
        on_conflict_where = transformWhereClause(
            mcx,
            pstate,
            clause.whereClause,
            ParseExprKind::EXPR_KIND_WHERE,
            "WHERE",
        )?;

        // EXCLUDED is not visible in RETURNING.
        let popped = pstate.p_namespace.pop().expect("excluded nsitem was pushed");
        debug_assert_eq!(popped.p_rtindex, excl_rel_index);
    }

    Node::mk(
        mcx,
        types_nodes::OnConflictExpr {
            action: clause.action,
            arbiterElems: arbiter_elems,
            arbiterWhere: arbiter_where,
            constraint,
            onConflictSet: on_conflict_set,
            onConflictWhere: on_conflict_where,
            exclRelIndex: excl_rel_index,
            exclRelTlist: excl_rel_tlist,
        },
    )
}

// C `BuildOnConflictExcludedTargetlist` (analyze.c): resnos must equal the
// Var varattnos (including the resjunk whole-row Var at resno 0) — setrefs
// resolves EXCLUDED references positionally; this is never a real tlist.
fn BuildOnConflictExcludedTargetlist<'mcx>(
    mcx: Mcx<'mcx>,
    targetrel: &types_rel::Relation<'mcx>,
    excl_rel_index: i32,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    use types_core::catalog::INT4OID;

    let mut tlist = types_nodes::NodeList::nil();
    for i in 0..targetrel.rd_att.natts as usize {
        let attr = targetrel.rd_att.attr(i);
        let var = if attr.attisdropped {
            // Any claimed type works for the placeholder null.
            Node::mk_const(mcx, INT4OID, -1, 0, 4, datum::Datum::from_i32(0), true, true)?
        } else {
            Node::mk_var(
                mcx,
                excl_rel_index,
                (i + 1) as types_core::AttrNumber,
                attr.atttypid,
                attr.atttypmod,
                attr.attcollation,
                0,
            )?
        };
        tlist.lappend(mcx, Node::mk_target_entry(mcx, var, (i + 1) as i16, None, false)?)?;
    }

    let whole_row =
        Node::mk_var(mcx, excl_rel_index, 0, targetrel.rd_rel.reltype, -1, 0, 0)?;
    tlist.lappend(mcx, Node::mk_target_entry(mcx, whole_row, 0, None, true)?)?;
    Ok(tlist)
}

fn transformDeleteStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &types_nodes::DeleteStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::parsenodes::ACL_DELETE;

    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_DELETE;

    if stmt.withClause.is_some() {
        panic!(
            "transformDeleteStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }

    let relation = stmt
        .relation
        .expect("grammar always sets DeleteStmt.relation")
        .as_range_var()
        .expect("relation_expr_opt_alias is a RangeVar");
    qry.resultRelation =
        parse_clause::setTargetTable(mcx, pstate, relation, relation.inh, true, ACL_DELETE)?;

    // C toggles p_lateral_only around the USING transform; the nsitem is
    // shared immutably here, so a USING list is loud until that lane lands.
    if !stmt.usingClause.is_nil() {
        panic!(
            "transformDeleteStmt (analyze.c): USING list (p_lateral_only toggle + \
             join planning) unported — M2 join lane"
        );
    }

    let qual = transformWhereClause(
        mcx,
        pstate,
        stmt.whereClause,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    transformReturningClause(
        mcx,
        pstate,
        &mut qry,
        stmt.returningClause,
        ParseExprKind::EXPR_KIND_RETURNING,
    )?;

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: qual })?
            .seal_ref(),
    );

    qry.hasSubLinks = pstate.p_hasSubLinks;
    qry.hasWindowFuncs = pstate.p_hasWindowFuncs;
    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasAggs = pstate.p_hasAggs;

    assign_query_collations(mcx, pstate, &qry)?;

    if pstate.p_hasAggs {
        parse_agg::parseCheckAggregates(mcx, pstate, &mut qry)?;
    }
    Ok(qry)
}

fn transformUpdateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &types_nodes::UpdateStmt<'mcx>,
) -> PgResult<Query<'mcx>> {
    use types_nodes::parsenodes::ACL_UPDATE;

    let mut qry = Query::default();
    qry.commandType = CmdType::CMD_UPDATE;
    pstate.p_is_insert = false;

    if stmt.withClause.is_some() {
        panic!(
            "transformUpdateStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }

    let relation = stmt
        .relation
        .expect("grammar always sets UpdateStmt.relation")
        .as_range_var()
        .expect("relation_expr_opt_alias is a RangeVar");
    qry.resultRelation =
        parse_clause::setTargetTable(mcx, pstate, relation, relation.inh, true, ACL_UPDATE)?;

    // C toggles p_lateral_only around the FROM transform; the nsitem is
    // shared immutably here, so a FROM list is loud until that lane lands.
    if !stmt.fromClause.is_nil() {
        panic!(
            "transformUpdateStmt (analyze.c): FROM list (p_lateral_only toggle + \
             join planning) unported — M2 join lane"
        );
    }

    let qual = transformWhereClause(
        mcx,
        pstate,
        stmt.whereClause,
        ParseExprKind::EXPR_KIND_WHERE,
        "WHERE",
    )?;

    transformReturningClause(
        mcx,
        pstate,
        &mut qry,
        stmt.returningClause,
        ParseExprKind::EXPR_KIND_RETURNING,
    )?;

    qry.targetList = transformUpdateTargetList(mcx, pstate, &stmt.targetList)?;

    qry.rtable = mem::take(&mut pstate.p_rtable);
    qry.rteperminfos = mem::take(&mut pstate.p_rteperminfos);
    qry.jointree = Some(
        Node::mk_mut(mcx, FromExpr { fromlist: mem::take(&mut pstate.p_joinlist), quals: qual })?
            .seal_ref(),
    );

    qry.hasTargetSRFs = pstate.p_hasTargetSRFs;
    qry.hasSubLinks = pstate.p_hasSubLinks;

    assign_query_collations(mcx, pstate, &qry)?;
    Ok(qry)
}

// C transformUpdateTargetList (analyze.c): resnos become the target attribute
// numbers; resjunk entries renumber past the relation's column count.
fn transformUpdateTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    orig_tlist: &types_nodes::NodeList<'mcx>,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let tlist = parse_target::transformTargetList(
        mcx,
        pstate,
        orig_tlist,
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE,
    )?;

    let numattrs = pstate
        .p_target_relation
        .as_ref()
        .expect("transformUpdateTargetList with no target relation")
        .rd_att
        .natts as i32;
    if pstate.p_next_resno <= numattrs {
        pstate.p_next_resno = numattrs + 1;
    }

    let perminfo = pstate
        .p_target_nsitem
        .expect("setTargetTable set p_target_nsitem")
        .p_perminfo
        .expect("target nsitem has perminfo");

    let mut orig_iter = orig_tlist.iter();
    for tle_node in &tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if tle.resjunk {
            let resno = pstate.p_next_resno as i16;
            pstate.p_next_resno += 1;
            // SAFETY: parser-owned tlist; the `tle` probe above is dead here.
            unsafe {
                tle_node.with_mut::<types_nodes::TargetEntry, _>(|t| {
                    t.resno = resno;
                    t.resname = None;
                })
            }
            .expect("TargetEntry");
            continue;
        }
        let orig = orig_iter
            .next()
            .unwrap_or_else(|| panic!("UPDATE target count mismatch --- internal error"));
        let orig_target = orig.as_res_target().expect("ResTarget");
        let colname = orig_target.name.expect("set_target always has a name");

        let attrno = {
            let rel = pstate.p_target_relation.as_ref().unwrap();
            parse_relation::attnameAttNum(rel, colname, true)
        };
        if attrno == 0 {
            return Err(undefined_update_column(pstate, colname, orig_target.location));
        }

        parse_target::updateTargetListEntry(
            mcx,
            pstate,
            tle_node,
            colname,
            attrno as i32,
            &orig_target.indirection,
            orig_target.location,
        )?;

        // SAFETY: perminfo nodes are read only through transient as_*
        // lookups; no derived reference is live across this write.
        unsafe {
            perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                p.updatedCols.add_member(
                    mcx,
                    attrno as i32 - types_tuple::htup::FirstLowInvalidHeapAttributeNumber,
                )
            })
        }
        .expect("p_perminfo is RTEPermissionInfo")?;
    }
    assert!(orig_iter.next().is_none(), "UPDATE target count mismatch --- internal error");
    Ok(tlist)
}

#[cold]
fn undefined_update_column(
    pstate: &ParseState<'_, '_>,
    colname: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_COLUMN, ERROR};
    let relname = pstate
        .p_target_relation
        .as_ref()
        .map(|r| std::string::String::from_utf8_lossy(r.rd_rel.relname.name_str()).into_owned())
        .unwrap_or_default();
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{colname}\" of relation \"{relname}\" does not exist"
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformUpdateTargetList")),
    )
}

// C transformInsertRow (analyze.c). strip_indirection is inert: FieldStore/
// SubscriptingRef construction panics upstream (transformAssignedExpr).
fn transformInsertRow<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    exprlist: types_nodes::NodeList<'mcx>,
    stmtcols: &types_nodes::NodeList<'mcx>,
    icolumns: &types_nodes::NodeList<'mcx>,
    attrnos: &[i32],
    strip_indirection: bool,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let _ = strip_indirection;
    if exprlist.len() > icolumns.len() {
        return Err(insert_row_length_error(
            pstate,
            "INSERT has more expressions than target columns",
            parse_expr::expr_location(exprlist.nth(icolumns.len())),
        ));
    }
    if !stmtcols.is_nil() && exprlist.len() < icolumns.len() {
        let col = icolumns.nth(exprlist.len()).as_res_target().expect("ResTarget");
        return Err(insert_row_length_error(
            pstate,
            "INSERT has more target columns than expressions",
            col.location,
        ));
    }

    let mut result = types_nodes::NodeList::nil();
    for (i, (expr, icol)) in exprlist.iter().zip(icolumns.iter()).enumerate() {
        let col = icol.as_res_target().expect("icolumns are ResTargets");
        let expr = parse_target::transformAssignedExpr(
            mcx,
            pstate,
            expr,
            ParseExprKind::EXPR_KIND_INSERT_TARGET,
            col.name,
            attrnos[i],
            &col.indirection,
            col.location,
        )?;
        result.lappend(mcx, expr)?;
    }
    Ok(result)
}

// exprLocation(IntoClause) resolves to its rel's location (nodeFuncs.c).
#[cold]
pub(crate) fn select_into_error(
    pstate: &ParseState<'_, '_>,
    into: Node<'_>,
    msg: &str,
) -> Box<types_error::PgError> {
    use types_error::{ERRCODE_SYNTAX_ERROR, ERROR};
    let loc = into
        .as_variant::<types_nodes::rawnodes::IntoClause>()
        .and_then(|ic| ic.rel)
        .and_then(|r| r.as_range_var())
        .map(|r| r.location)
        .unwrap_or(-1);
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg.to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                loc,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error(),
    )
}

#[cold]
fn insert_row_length_error(
    pstate: &ParseState<'_, '_>,
    msg: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg.to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformInsertRow")),
    )
}

// C transformReturningClause (analyze.c). Divergences: the WITH(...) options
// list (PG18 OLD/NEW aliases) is loud, and instead of adding old/new namespace
// items we panic when a RETURNING expression would resolve through one; the
// Query alias fields still get C's "old"/"new" defaults when unmasked.
fn transformReturningClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    qry: &mut Query<'mcx>,
    returning_clause: Option<Node<'mcx>>,
    expr_kind: ParseExprKind,
) -> PgResult<()> {
    let Some(rc_node) = returning_clause else {
        return Ok(());
    };
    let rc = rc_node.as_returning_clause().expect("grammar builds ReturningClause");
    if !rc.options.is_nil() {
        panic!(
            "transformReturningClause (analyze.c): RETURNING WITH (OLD/NEW AS ...) \
             — PG18 OLD/NEW alias lane unported (addNSItemForReturning)"
        );
    }

    if parse_relation::refnameNamespaceItem(pstate, None, "old", -1, None)?.is_none() {
        check_no_old_new_columnref(&rc.exprs, "old")?;
        qry.returningOldAlias = Some("old");
    }
    if parse_relation::refnameNamespaceItem(pstate, None, "new", -1, None)?.is_none() {
        check_no_old_new_columnref(&rc.exprs, "new")?;
        qry.returningNewAlias = Some("new");
    }

    let save_next_resno = pstate.p_next_resno;
    pstate.p_next_resno = 1;

    qry.returningList = transformTargetList(mcx, pstate, &rc.exprs, expr_kind)?;

    if qry.returningList.is_nil() {
        return Err(returning_no_columns(pstate, &rc.exprs));
    }

    markTargetListOrigins(pstate, &qry.returningList)?;
    if pstate.p_resolve_unknowns {
        resolveTargetListUnknowns(mcx, pstate, &qry.returningList)?;
    }

    pstate.p_next_resno = save_next_resno;
    Ok(())
}

// The loud stand-in for C's implicit addNSItemForReturning("old"/"new"): any
// ColumnRef whose leading field is the (unmasked) alias would have resolved
// through the OLD/NEW namespace item in C.
fn check_no_old_new_columnref<'mcx>(
    exprs: &types_nodes::NodeList<'mcx>,
    alias: &'static str,
) -> PgResult<()> {
    struct W {
        alias: &'static str,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(cr) = node.as_column_ref() {
                if let Some(s) = cr.fields.iter().next().and_then(|f| f.as_string()) {
                    if s.sval == self.alias {
                        panic!(
                            "transformReturningClause (analyze.c): RETURNING {0}.* / \
                             {0}.<col> — PG18 OLD/NEW alias lane unported",
                            self.alias
                        );
                    }
                }
                return Ok(false);
            }
            nodes_core::raw_expression_tree_walker(node, self)
        }
    }
    nodes_core::walk_list(exprs, &mut W { alias })?;
    Ok(())
}

#[cold]
fn returning_no_columns(
    pstate: &ParseState<'_, '_>,
    exprs: &types_nodes::NodeList<'_>,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    let location =
        exprs.iter().next().and_then(|n| n.as_res_target()).map(|r| r.location).unwrap_or(-1);
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("RETURNING must have at least one column".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformReturningClause")),
    )
}

// The INSERT arm's stand-in for C's addNSItemToQuery(p_target_nsitem, ...):
// p_target_nsitem is a shared borrow here, so the namespace entry is a fresh
// copy with the visibility flags C would have scribbled in place.
fn returning_target_nsitem<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
) -> PgResult<&'mcx mut parser_small1::ParseNamespaceItem<'mcx>> {
    let t = pstate.p_target_nsitem.expect("setTargetTable set p_target_nsitem");
    let nsitem = parser_small1::ParseNamespaceItem {
        p_names: t.p_names,
        p_rte: t.p_rte,
        p_rtindex: t.p_rtindex,
        p_perminfo: t.p_perminfo,
        p_nscolumns: t.p_nscolumns,
        p_rel_visible: true,
        p_cols_visible: true,
        p_lateral_only: false,
        p_lateral_ok: true,
        p_returning_type: t.p_returning_type,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

#[cold]
fn values_length_mismatch(
    pstate: &ParseState<'_, '_>,
    sublist: &types_nodes::NodeList<'_>,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    let location = sublist.iter().next().map(parse_expr::expr_location).unwrap_or(-1);
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("VALUES lists must all be the same length".to_string())
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformInsertStmt")),
    )
}

pub fn LCS_asString(strength: types_nodes::LockClauseStrength) -> &'static str {
    use types_nodes::LockClauseStrength::*;
    match strength {
        LCS_NONE => {
            debug_assert!(false);
            "FOR some"
        }
        LCS_FORKEYSHARE => "FOR KEY SHARE",
        LCS_FORSHARE => "FOR SHARE",
        LCS_FORNOKEYUPDATE => "FOR NO KEY UPDATE",
        LCS_FORUPDATE => "FOR UPDATE",
    }
}

pub(crate) fn first_locking_strength(
    locking_clause: &types_nodes::NodeList<'_>,
) -> types_nodes::LockClauseStrength {
    locking_clause
        .nth(0)
        .as_locking_clause()
        .expect("lockingClause cell")
        .strength
}

#[cold]
pub(crate) fn locking_not_allowed_with(
    strength: types_nodes::LockClauseStrength,
    what: &str,
    funcname: &'static str,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("{} is not allowed with {what}", LCS_asString(strength)))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, funcname)),
    )
}

#[cold]
fn locking_not_applicable_to(
    strength: types_nodes::LockClauseStrength,
    what: &str,
    funcname: &'static str,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("{} cannot be applied to {what}", LCS_asString(strength)))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, funcname)),
    )
}

/// `CheckSelectLocking` (analyze.c); the planner re-checks after rewriting.
pub fn CheckSelectLocking(
    qry: &Query<'_>,
    strength: types_nodes::LockClauseStrength,
) -> PgResult<()> {
    debug_assert!(strength != types_nodes::LockClauseStrength::LCS_NONE);
    const F: &str = "CheckSelectLocking";
    if qry.setOperations.is_some() {
        return Err(locking_not_allowed_with(strength, "UNION/INTERSECT/EXCEPT", F));
    }
    if !qry.distinctClause.is_nil() {
        return Err(locking_not_allowed_with(strength, "DISTINCT clause", F));
    }
    if !qry.groupClause.is_nil() || !qry.groupingSets.is_nil() {
        return Err(locking_not_allowed_with(strength, "GROUP BY clause", F));
    }
    if qry.havingQual.is_some() {
        return Err(locking_not_allowed_with(strength, "HAVING clause", F));
    }
    if qry.hasAggs {
        return Err(locking_not_allowed_with(strength, "aggregate functions", F));
    }
    if qry.hasWindowFuncs {
        return Err(locking_not_allowed_with(strength, "window functions", F));
    }
    if qry.hasTargetSRFs {
        return Err(locking_not_allowed_with(
            strength,
            "set-returning functions in the target list",
            F,
        ));
    }
    Ok(())
}

/// `transformLockingClause` (analyze.c). The RTE_SUBQUERY propagation arm
/// (markQueryForLocking's sibling) is loud: sealed sub-Queries can't take
/// rowmark appends yet.
fn transformLockingClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    qry: &mut Query<'mcx>,
    lc: &types_nodes::LockingClause<'mcx>,
    pushed_down: bool,
) -> PgResult<()> {
    use types_nodes::parsenodes::{RTEKind, ACL_SELECT_FOR_UPDATE};

    CheckSelectLocking(qry, lc.strength)?;

    if lc.lockedRels.is_nil() {
        for idx in 0..qry.rtable.len() {
            let rte = qry.rtable.nth(idx).as_range_tbl_entry().expect("rtable cell");
            let i = idx as u32 + 1;
            if !rte.inFromCl {
                continue;
            }
            match rte.rtekind {
                RTEKind::RTE_RELATION => {
                    applyLockingClause(mcx, qry, i, lc.strength, lc.waitPolicy, pushed_down)?;
                    let perminfo =
                        parse_relation::getRTEPermissionInfo(&qry.rteperminfos, rte)?;
                    // SAFETY: parser-owned tree; no derived refs live.
                    unsafe {
                        perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                            p.requiredPerms |= ACL_SELECT_FOR_UPDATE
                        })
                    }
                    .expect("RTEPermissionInfo");
                }
                RTEKind::RTE_SUBQUERY => panic!(
                    "transformLockingClause (analyze.c): FOR UPDATE/SHARE propagation \
                     into subquery RTEs unported — unit backend-parser-analyze"
                ),
                _ => {}
            }
        }
        return Ok(());
    }

    'rels: for rel_node in &lc.lockedRels {
        let thisrel = rel_node.as_range_var().expect("lockedRels cell");
        if thisrel.catalogname.is_some() || thisrel.schemaname.is_some() {
            return Err(locking_unqualified_names(pstate, lc.strength, thisrel.location));
        }
        for idx in 0..qry.rtable.len() {
            let rte = qry.rtable.nth(idx).as_range_tbl_entry().expect("rtable cell");
            let i = idx as u32 + 1;
            if !rte.inFromCl {
                continue;
            }
            let mut rtename = rte.eref.expect("rte has eref").aliasname;
            if rte.alias.is_none() {
                match rte.rtekind {
                    RTEKind::RTE_JOIN => match rte.join_using_alias {
                        None => continue,
                        Some(ja) => rtename = ja.aliasname,
                    },
                    RTEKind::RTE_SUBQUERY | RTEKind::RTE_VALUES => continue,
                    _ => {}
                }
            }
            if rtename != thisrel.relname {
                continue;
            }
            match rte.rtekind {
                RTEKind::RTE_RELATION => {
                    applyLockingClause(mcx, qry, i, lc.strength, lc.waitPolicy, pushed_down)?;
                    let perminfo =
                        parse_relation::getRTEPermissionInfo(&qry.rteperminfos, rte)?;
                    // SAFETY: parser-owned tree; no derived refs live.
                    unsafe {
                        perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
                            p.requiredPerms |= ACL_SELECT_FOR_UPDATE
                        })
                    }
                    .expect("RTEPermissionInfo");
                }
                RTEKind::RTE_SUBQUERY => panic!(
                    "transformLockingClause (analyze.c): FOR UPDATE/SHARE propagation \
                     into subquery RTEs unported — unit backend-parser-analyze"
                ),
                RTEKind::RTE_JOIN => {
                    return Err(locking_bad_target(pstate, lc.strength, "a join", thisrel.location))
                }
                RTEKind::RTE_FUNCTION => {
                    return Err(locking_bad_target(
                        pstate, lc.strength, "a function", thisrel.location,
                    ))
                }
                RTEKind::RTE_TABLEFUNC => {
                    return Err(locking_bad_target(
                        pstate, lc.strength, "a table function", thisrel.location,
                    ))
                }
                RTEKind::RTE_VALUES => {
                    return Err(locking_bad_target(pstate, lc.strength, "VALUES", thisrel.location))
                }
                RTEKind::RTE_CTE => {
                    return Err(locking_bad_target(
                        pstate, lc.strength, "a WITH query", thisrel.location,
                    ))
                }
                RTEKind::RTE_NAMEDTUPLESTORE => {
                    return Err(locking_bad_target(
                        pstate, lc.strength, "a named tuplestore", thisrel.location,
                    ))
                }
                other => panic!("unrecognized RTE type: {}", other as i32),
            }
            continue 'rels;
        }
        return Err(locking_rel_not_found(
            pstate,
            thisrel.relname.expect("grammar always sets relname"),
            lc.strength,
            thisrel.location,
        ));
    }
    Ok(())
}

/// `applyLockingClause` (analyze.c).
fn applyLockingClause<'mcx>(
    mcx: Mcx<'mcx>,
    qry: &mut Query<'mcx>,
    rtindex: u32,
    strength: types_nodes::LockClauseStrength,
    wait_policy: types_nodes::LockWaitPolicy,
    pushed_down: bool,
) -> PgResult<()> {
    use types_nodes::parsenodes::RowMarkClause;

    debug_assert!(strength != types_nodes::LockClauseStrength::LCS_NONE);
    if !pushed_down {
        qry.hasForUpdate = true;
    }
    if let Some(rc) = parse_relation::get_parse_rowmark(qry, rtindex) {
        // Strongest strength wins; NOWAIT > SKIP LOCKED > block; pushedDown
        // clears once any clause is explicit (C's Max/&= merge).
        // SAFETY: parser-owned tree; no derived refs live.
        unsafe {
            rc.with_mut::<RowMarkClause, _>(|rc| {
                rc.strength = rc.strength.max(strength);
                rc.waitPolicy = rc.waitPolicy.max(wait_policy);
                rc.pushedDown &= pushed_down;
            })
        }
        .expect("RowMarkClause");
        return Ok(());
    }
    let rc = Node::mk(
        mcx,
        RowMarkClause { rti: rtindex, strength, waitPolicy: wait_policy, pushedDown: pushed_down },
    )?;
    qry.rowMarks.lappend(mcx, rc)?;
    Ok(())
}

#[cold]
fn locking_unqualified_names(
    pstate: &ParseState<'_, '_>,
    strength: types_nodes::LockClauseStrength,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_SYNTAX_ERROR, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "{} must specify unqualified relation names",
                LCS_asString(strength)
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformLockingClause")),
    )
}

#[cold]
fn locking_bad_target(
    pstate: &ParseState<'_, '_>,
    strength: types_nodes::LockClauseStrength,
    what: &str,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("{} cannot be applied to {what}", LCS_asString(strength)))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformLockingClause")),
    )
}

#[cold]
fn locking_rel_not_found(
    pstate: &ParseState<'_, '_>,
    relname: &str,
    strength: types_nodes::LockClauseStrength,
    location: i32,
) -> Box<types_error::PgError> {
    use types_error::{ErrorLocation, ERRCODE_UNDEFINED_TABLE, ERROR};
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!(
                "relation \"{relname}\" in {} clause not found in FROM clause",
                LCS_asString(strength)
            ))
            .errposition(parser_small1::parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("analyze.c", 0, "transformLockingClause")),
    )
}
