#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use parse_expr::transformExpr;
use parser_small1::{ParseExprKind, ParseState};
use types_error::PgResult;
use types_nodes::nodes_enums::LimitOption;
use types_nodes::{Node, NodeList};

pub fn transformFromClause<'mcx>(
    _mcx: Mcx<'mcx>,
    _pstate: &mut ParseState<'_, 'mcx>,
    frm_list: &NodeList<'mcx>,
) -> PgResult<()> {
    for item in frm_list {
        panic!(
            "transformFromClause (parse_clause.c): transformFromClauseItem for {:?} \
             unported (needs parse_relation/addRangeTableEntry) — unit backend-parser-clause",
            item.node_tag()
        );
    }
    Ok(())
}

pub fn transformWhereClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    clause: Option<Node<'mcx>>,
    expr_kind: ParseExprKind,
    construct_name: &'static str,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(clause) = clause else {
        return Ok(None);
    };
    let _qual = transformExpr(mcx, pstate, clause, expr_kind)?;
    panic!(
        "transformWhereClause (parse_clause.c): coerce_to_boolean for {construct_name} \
         unported — unit backend-parser-coerce"
    );
}

pub fn transformLimitClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    clause: Option<Node<'mcx>>,
    expr_kind: ParseExprKind,
    construct_name: &'static str,
    _limit_option: LimitOption,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(clause) = clause else {
        return Ok(None);
    };
    let _qual = transformExpr(mcx, pstate, clause, expr_kind)?;
    panic!(
        "transformLimitClause (parse_clause.c): coerce_to_specific_type(INT8)/\
         checkExprIsVarFree for {construct_name} unported — unit backend-parser-clause"
    );
}

pub fn transformSortClause<'mcx>(
    _pstate: &mut ParseState<'_, 'mcx>,
    orderby: &NodeList<'mcx>,
    _targetlist: &mut NodeList<'mcx>,
    _expr_kind: ParseExprKind,
    _use_sql99: bool,
) -> PgResult<NodeList<'mcx>> {
    if !orderby.is_nil() {
        panic!(
            "transformSortClause (parse_clause.c): SortBy/addTargetToSortList unported — \
             unit backend-parser-clause"
        );
    }
    Ok(NodeList::nil())
}

pub fn transformGroupClause<'mcx>(
    _pstate: &mut ParseState<'_, 'mcx>,
    grouplist: &NodeList<'mcx>,
    grouping_sets: &mut NodeList<'mcx>,
    _targetlist: &mut NodeList<'mcx>,
    _sort_clause: &NodeList<'mcx>,
    _expr_kind: ParseExprKind,
    _use_sql99: bool,
) -> PgResult<NodeList<'mcx>> {
    if !grouplist.is_nil() {
        panic!(
            "transformGroupClause (parse_clause.c): GROUP BY transformation unported — \
             unit backend-parser-clause"
        );
    }
    *grouping_sets = NodeList::nil();
    Ok(NodeList::nil())
}

pub fn transformDistinctClause<'mcx>(
    _pstate: &mut ParseState<'_, 'mcx>,
    _targetlist: &mut NodeList<'mcx>,
    _sort_clause: &NodeList<'mcx>,
    _is_agg: bool,
) -> PgResult<NodeList<'mcx>> {
    panic!(
        "transformDistinctClause (parse_clause.c): DISTINCT transformation unported — \
         unit backend-parser-clause"
    );
}

pub fn transformDistinctOnClause<'mcx>(
    _pstate: &mut ParseState<'_, 'mcx>,
    _distinctlist: &NodeList<'mcx>,
    _targetlist: &mut NodeList<'mcx>,
    _sort_clause: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    panic!(
        "transformDistinctOnClause (parse_clause.c): DISTINCT ON transformation unported — \
         unit backend-parser-clause"
    );
}

pub fn transformWindowDefinitions<'mcx>(
    _pstate: &mut ParseState<'_, 'mcx>,
    windowdefs: &NodeList<'mcx>,
    _targetlist: &mut NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    if !windowdefs.is_nil() {
        panic!(
            "transformWindowDefinitions (parse_clause.c): WindowDef transformation \
             unported — unit backend-parser-clause"
        );
    }
    Ok(NodeList::nil())
}
