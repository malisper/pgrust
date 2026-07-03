#![allow(non_snake_case)]

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
    analyze_seams::analyze_requires_snapshot::set(analyze_requires_snapshot);
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
    if let Some(mut stmt) = parse_tree.as_select_stmt() {
        while stmt.op != types_nodes::parsenodes::SetOperation::SETOP_NONE {
            stmt = stmt.larg.expect("set-op tree always has a leftmost SelectStmt");
        }
        if stmt.intoClause.is_some() {
            panic!(
                "transformOptionalSelectInto (analyze.c): SELECT INTO -> CREATE TABLE AS \
                 rewrite unported (CreateTableAsStmt vocabulary + \
                 transformCreateTableAsStmt) — unit backend-parser-analyze"
            );
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
                panic!(
                    "transformStmt (analyze.c): transformValuesClause unported — \
                     unit backend-parser-analyze"
                );
            } else if n.op == types_nodes::parsenodes::SetOperation::SETOP_NONE {
                transformSelectStmt(mcx, pstate, n)?
            } else {
                panic!(
                    "transformStmt (analyze.c): transformSetOperationStmt unported — \
                     unit backend-parser-analyze"
                );
            }
        }
        t @ (NodeTag::T_InsertStmt
        | NodeTag::T_DeleteStmt
        | NodeTag::T_UpdateStmt
        | NodeTag::T_MergeStmt
        | NodeTag::T_ReturnStmt
        | NodeTag::T_PLAssignStmt
        | NodeTag::T_DeclareCursorStmt
        | NodeTag::T_ExplainStmt
        | NodeTag::T_CreateTableAsStmt
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

    if stmt.withClause.is_some() {
        panic!(
            "transformSelectStmt (analyze.c): transformWithClause (parse_cte.c) \
             unported — unit backend-parser-medium1"
        );
    }

    if stmt.intoClause.is_some() {
        panic!(
            "transformSelectStmt (analyze.c): \"SELECT ... INTO is not allowed here\" \
             ereport needs IntoClause vocabulary — unit backend-parser-analyze"
        );
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
        pstate,
        &stmt.groupClause,
        &mut qry.groupingSets,
        &mut qry.targetList,
        &qry.sortClause,
        ParseExprKind::EXPR_KIND_GROUP_BY,
        false,
    )?;
    qry.groupDistinct = stmt.groupDistinct;

    if !stmt.distinctClause.is_none() {
        panic!(
            "transformSelectStmt (analyze.c): transformDistinctClause/\
             transformDistinctOnClause unported (gram repr: DistinctClause::All = \
             C's one-NULL-cell list, DistinctClause::On = DISTINCT ON exprs) — \
             unit backend-parser-clause"
        );
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
    qry.windowClause = transformWindowDefinitions(pstate, &windowdefs, &mut qry.targetList)?;

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

    if !pstate.p_locking_clause.is_nil() {
        panic!(
            "transformSelectStmt (analyze.c): transformLockingClause unported — \
             unit backend-parser-analyze"
        );
    }

    assign_query_collations(mcx, pstate, &qry)?;

    if pstate.p_hasAggs
        || !qry.groupClause.is_nil()
        || !qry.groupingSets.is_nil()
        || qry.havingQual.is_some()
    {
        parse_agg::parseCheckAggregates(pstate, &qry)?;
    }

    Ok(qry)
}
