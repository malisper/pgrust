#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use parse_expr::{expr_location, expr_type, transformExpr, ParseExprKindName};
use parse_relation::{addRangeTableEntry, checkNameSpaceConflicts};
use parser_small1::{parser_errposition, ParseExprKind, ParseNamespaceItem, ParseState};
use types_core::catalog::{INT8OID, TEXTOID, UNKNOWNOID};
use types_core::{Index, InvalidOid, Oid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE, ERRCODE_QUERY_CANCELED, ERRCODE_SYNTAX_ERROR,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::nodes_enums::LimitOption;
use types_nodes::parsenodes::SortGroupClause;
use types_nodes::primnodes::TargetEntry;
use types_nodes::rawnodes::{SortBy, SortByDir, SortByNulls, ValUnion};
use types_nodes::{CoercionForm, Node, NodeList, NodeTag};

pub fn transformFromClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    frm_list: &NodeList<'mcx>,
) -> PgResult<()> {
    for item in frm_list {
        let (n, nsitem) = transformFromClauseItem(mcx, pstate, item)?;

        checkNameSpaceConflicts(pstate.p_namespace.as_slice(), &[nsitem])?;

        // C toggles the new items lateral_only=true here and resets every
        // item to lateral_only=false after the loop; with only the plain
        // relation arm live no expression is transformed in between, so the
        // interim flag state is unobservable and items keep their
        // buildNSItemFromTupleDesc defaults.
        pstate.p_joinlist.lappend(mcx, n)?;
        pstate.p_namespace.push(nsitem);
    }
    Ok(())
}

fn transformFromClauseItem<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    n: Node<'mcx>,
) -> PgResult<(Node<'mcx>, &'mcx ParseNamespaceItem<'mcx>)> {
    stack_depth::check_stack_depth()?;

    match n.node_tag() {
        NodeTag::T_RangeVar => {
            let rv = n.as_range_var().unwrap();
            if rv.schemaname.is_none()
                && (!pstate.p_ctenamespace.is_nil()
                    || !pstate.p_future_ctes.is_nil()
                    || pstate.p_queryEnv.is_some())
            {
                panic!(
                    "transformFromClauseItem (parse_clause.c): \
                     getNSItemForSpecialRelationTypes (CTE/ENR reference) unported — \
                     unit backend-parser-medium1"
                );
            }
            let nsitem = addRangeTableEntry(mcx, pstate, rv, rv.alias, rv.inh, true)?;
            let rtr = Node::mk_range_tbl_ref(mcx, nsitem.p_rtindex)?;
            Ok((rtr, nsitem))
        }
        NodeTag::T_RangeFunction => {
            let nsitem = transformRangeFunction(mcx, pstate, n.as_range_function().unwrap())?;
            let rtr = Node::mk_range_tbl_ref(mcx, nsitem.p_rtindex)?;
            Ok((rtr, nsitem))
        }
        other => panic!(
            "transformFromClauseItem (parse_clause.c): arm for {other:?} \
             (subselect/tablesample/tablefunc/JOIN) unported — \
             unit backend-parser-clause"
        ),
    }
}

// Single plain-function slice of C transformRangeFunction; the unnest()
// multi-arg kluge never fires (unnest itself is unported).
fn transformRangeFunction<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    r: &types_nodes::RangeFunction<'mcx>,
) -> PgResult<&'mcx ParseNamespaceItem<'mcx>> {
    if r.is_rowsfrom || r.functions.len() != 1 {
        panic!(
            "transformRangeFunction (parse_clause.c): ROWS FROM / multiple functions \
             unported — unit backend-parser-clause"
        );
    }
    if r.ordinality {
        panic!(
            "transformRangeFunction (parse_clause.c): WITH ORDINALITY unported — \
             unit backend-parser-clause"
        );
    }
    if r.lateral {
        panic!(
            "transformRangeFunction (parse_clause.c): LATERAL functions unported \
             (contain_vars_of_level / lateral namespace) — unit backend-parser-clause"
        );
    }
    if !r.coldeflist.is_nil() {
        panic!(
            "transformRangeFunction (parse_clause.c): column definition lists \
             unported — coldeflist lane"
        );
    }

    debug_assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = true;

    let fexpr = r.functions.nth(0);
    let last_srf = pstate.p_last_srf;
    let newfexpr = transformExpr(mcx, pstate, fexpr, ParseExprKind::EXPR_KIND_FROM_FUNCTION)?;
    let moved = match (pstate.p_last_srf, last_srf) {
        (None, None) => false,
        (Some(a), Some(b)) => !a.ptr_eq(b),
        _ => true,
    };
    if moved && !pstate.p_last_srf.expect("moved implies Some").ptr_eq(newfexpr) {
        pstate.p_lateral_active = false;
        return Err(srf_not_top_level(
            pstate,
            expr_location(pstate.p_last_srf.expect("moved implies Some")),
        ));
    }
    let funcname = parse_target::FigureColname(fexpr);

    pstate.p_lateral_active = false;

    parse_collate::assign_expr_collations(mcx, pstate, newfexpr)?;

    parse_relation::addRangeTableEntryForFunction(
        mcx,
        pstate,
        funcname,
        newfexpr,
        r.alias,
        r.ordinality,
        false,
        true,
    )
    .map(|nsitem| &*nsitem)
}

#[cold]
#[inline(never)]
fn srf_not_top_level(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("set-returning functions must appear at top level of FROM")
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_clause.c", 0, "transformRangeFunction")),
    )
}

/// C `setTargetTable` (parse_clause.c); returns the target rangetable index.
pub fn setTargetTable<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    relation: &types_nodes::RangeVar<'mcx>,
    inh: bool,
    alsoSource: bool,
    requiredPerms: types_nodes::parsenodes::AclMode,
) -> PgResult<i32> {
    if relation.schemaname.is_none() && pstate.p_queryEnv.is_some() {
        panic!(
            "setTargetTable (parse_clause.c): scanNameSpaceForENR unported — \
             unit backend-parser-clause"
        );
    }
    if let Some(old) = pstate.p_target_relation.take() {
        table::table_close(old, types_rel::NoLock)?;
    }

    let rel =
        parse_relation::parserOpenTable(mcx, pstate, relation, types_rel::RowExclusiveLock)?;
    let nsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        pstate,
        &rel,
        types_rel::RowExclusiveLock,
        relation.alias,
        inh,
        false,
    )?;
    pstate.p_target_relation = Some(rel);

    let perminfo = nsitem.p_perminfo.expect("relation nsitem has perminfo");
    // SAFETY: perminfo nodes are read only through transient as_* lookups; no
    // derived reference is live across this write.
    unsafe {
        perminfo.with_mut::<types_nodes::RTEPermissionInfo, _>(|p| {
            p.requiredPerms = requiredPerms
        })
    }
    .expect("p_perminfo is RTEPermissionInfo");

    let rtindex = nsitem.p_rtindex;
    if alsoSource {
        parse_relation::addNSItemToQuery(mcx, pstate, nsitem, true, true, true)?;
        pstate.p_target_nsitem = pstate.p_namespace.last().copied();
    } else {
        pstate.p_target_nsitem = Some(nsitem);
    }
    Ok(rtindex)
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
    let qual = transformExpr(mcx, pstate, clause, expr_kind)?;
    let qual = coerce::coerce_to_boolean(
        mcx,
        pstate,
        qual,
        expr_type(qual),
        expr_location(qual),
        construct_name,
    )?;
    Ok(Some(qual))
}

pub fn transformLimitClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    clause: Option<Node<'mcx>>,
    expr_kind: ParseExprKind,
    construct_name: &'static str,
    limit_option: LimitOption,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(clause) = clause else {
        return Ok(None);
    };
    let qual = transformExpr(mcx, pstate, clause, expr_kind)?;
    let qual = coerce::coerce_to_specific_type(
        mcx,
        pstate,
        qual,
        expr_type(qual),
        expr_location(qual),
        INT8OID,
        construct_name,
    )?;
    checkExprIsVarFree(pstate, qual, construct_name)?;

    if expr_kind == ParseExprKind::EXPR_KIND_LIMIT
        && limit_option == LimitOption::LIMIT_OPTION_WITH_TIES
        && clause.as_a_const().is_some_and(|c| c.isnull())
    {
        return Err(null_row_count_with_ties());
    }
    Ok(Some(qual))
}

fn checkExprIsVarFree(
    pstate: &ParseState<'_, '_>,
    n: Node<'_>,
    construct_name: &str,
) -> PgResult<()> {
    if vars::contain_vars_of_level(n, 0)? {
        return Err(contains_variables(pstate, construct_name, vars::locate_var_of_level(n, 0)?));
    }
    Ok(())
}

pub fn transformSortClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    orderby: &NodeList<'mcx>,
    targetlist: &mut NodeList<'mcx>,
    expr_kind: ParseExprKind,
    use_sql99: bool,
) -> PgResult<NodeList<'mcx>> {
    let mut sortlist = NodeList::nil();
    for item in orderby {
        let sortby = item.as_sort_by().expect("ORDER BY list holds SortBy nodes");
        let sort_node = sortby.node.expect("SortBy.node is never NULL");
        if use_sql99 {
            panic!(
                "transformSortClause (parse_clause.c): findTargetlistEntrySQL99 \
                 (window/aggregate ORDER BY) unported — unit backend-parser-clause"
            );
        }
        let tle = findTargetlistEntrySQL92(mcx, pstate, sort_node, targetlist, expr_kind)?;
        sortlist = addTargetToSortList(mcx, pstate, tle, sortlist, targetlist, sortby)?;
    }
    Ok(sortlist)
}

fn findTargetlistEntrySQL92<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    tlist: &mut NodeList<'mcx>,
    expr_kind: ParseExprKind,
) -> PgResult<Node<'mcx>> {
    if let Some(cref) = node.as_column_ref() {
        if let [field1] = cref.fields.as_slice() {
            if let Some(name) = field1.as_string().map(|s| s.sval) {
                // GROUP BY prefers a FROM-clause column over a targetlist
                // alias; a FROM match falls through to the SQL99 leg.
                let mut name = Some(name);
                if expr_kind == ParseExprKind::EXPR_KIND_GROUP_BY
                    && parse_relation::colNameToVar(mcx, pstate, name.unwrap(), true, cref.location)?
                        .is_some()
                {
                    name = None;
                }
                let mut target_result: Option<Node<'mcx>> = None;
                for tle_node in &*tlist {
                    let tle = tle_node.as_target_entry().expect("tlist holds TargetEntry");
                    if !tle.resjunk && name.is_some() && tle.resname == name {
                        // Duplicate names naming the same value are allowed.
                        match target_result {
                            Some(prev) => {
                                if !types_nodes::equal(
                                    prev.as_target_entry().unwrap().expr,
                                    tle.expr,
                                ) {
                                    return Err(ambiguous_column(
                                        pstate,
                                        expr_kind,
                                        name.unwrap(),
                                        cref.location,
                                    ));
                                }
                            }
                            None => target_result = Some(tle_node),
                        }
                    }
                }
                if let Some(tle_node) = target_result {
                    checkTargetlistEntrySQL92(
                        pstate,
                        tle_node.as_target_entry().unwrap().expr,
                        expr_kind,
                    )?;
                    return Ok(tle_node);
                }
            }
        }
    }
    if let Some(aconst) = node.as_a_const() {
        let target_pos = match aconst.val {
            Some(ValUnion::Integer(i)) => i.ival,
            _ => return Err(non_integer_constant(pstate, expr_kind, aconst.location)),
        };
        let mut targetlist_pos = 0;
        for tle_node in &*tlist {
            let tle = tle_node.as_target_entry().expect("tlist holds TargetEntry");
            if !tle.resjunk {
                targetlist_pos += 1;
                if targetlist_pos == target_pos {
                    checkTargetlistEntrySQL92(pstate, tle.expr, expr_kind)?;
                    return Ok(tle_node);
                }
            }
        }
        return Err(position_not_in_select_list(pstate, expr_kind, target_pos, aconst.location));
    }
    findTargetlistEntrySQL99(mcx, pstate, node, tlist, expr_kind)
}

// C findTargetlistEntrySQL99, Var-equality leg: the transformed expression
// matches an existing tlist Var or lands as a resjunk entry; non-Var equal()
// matching (equalfuncs.c) is loud.
fn findTargetlistEntrySQL99<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    tlist: &mut NodeList<'mcx>,
    expr_kind: ParseExprKind,
) -> PgResult<Node<'mcx>> {
    let expr = transformExpr(mcx, pstate, node, expr_kind)?;
    let Some(evar) = expr.as_var() else {
        panic!(
            "findTargetlistEntrySQL99 (parse_clause.c): non-Var expression needs equal() \
             (equalfuncs.c) — unit backend-parser-clause"
        );
    };
    for tle_node in &*tlist {
        let tle = tle_node.as_target_entry().expect("tlist holds TargetEntry");
        if let Some(tvar) = tle.expr.as_var() {
            if tvar.varno == evar.varno
                && tvar.varattno == evar.varattno
                && tvar.varlevelsup == evar.varlevelsup
                && tvar.vartype == evar.vartype
            {
                return Ok(tle_node);
            }
        }
    }
    // transformTargetEntry (parse_target.c) resjunk arm.
    let resno = (tlist.len() + 1) as i16;
    let tle = Node::mk_target_entry(mcx, expr, resno, None, true)?;
    tlist.lappend(mcx, tle)?;
    Ok(tle)
}

fn checkTargetlistEntrySQL92(
    pstate: &ParseState<'_, '_>,
    tle_expr: Node<'_>,
    expr_kind: ParseExprKind,
) -> PgResult<()> {
    match expr_kind {
        ParseExprKind::EXPR_KIND_GROUP_BY => {
            if pstate.p_hasAggs && contains_aggref(tle_expr) {
                return Err(aggregate_in_group_by(pstate, expr_kind, tle_expr));
            }
            debug_assert!(!pstate.p_hasWindowFuncs, "window functions are a loud lane upstream");
            Ok(())
        }
        ParseExprKind::EXPR_KIND_ORDER_BY | ParseExprKind::EXPR_KIND_DISTINCT_ON => Ok(()),
        _ => Err(Box::new(PgError::error(
            "unexpected exprKind in checkTargetlistEntrySQL92".to_string(),
        ))),
    }
}

// contain_aggs_of_level(expr, 0) over the ported families (rewriteManip.c);
// outer-level aggs are a loud lane upstream so any Aggref counts.
fn contains_aggref(node: Node<'_>) -> bool {
    match node.node_tag() {
        NodeTag::T_Aggref => true,
        NodeTag::T_Var | NodeTag::T_Const | NodeTag::T_Param => false,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().args.iter().any(contains_aggref),
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().args.iter().any(contains_aggref),
        NodeTag::T_RelabelType => contains_aggref(node.as_relabel_type().unwrap().arg),
        NodeTag::T_BoolExpr => node.as_bool_expr().unwrap().args.iter().any(contains_aggref),
        NodeTag::T_NullTest => {
            node.as_null_test().unwrap().arg.is_some_and(contains_aggref)
        }
        tag => panic!(
            "contain_aggs_of_level (rewriteManip.c): node family {tag:?} unported — \
             unit backend-parser-clause"
        ),
    }
}

// locate_agg_of_level's job is the errposition; the first Aggref's location.
fn locate_aggref(node: Node<'_>) -> ParseLoc {
    match node.node_tag() {
        NodeTag::T_Aggref => node.as_aggref().unwrap().location,
        NodeTag::T_FuncExpr => node
            .as_func_expr()
            .unwrap()
            .args
            .iter()
            .find(|&a| contains_aggref(a))
            .map_or(-1, locate_aggref),
        NodeTag::T_OpExpr => node
            .as_op_expr()
            .unwrap()
            .args
            .iter()
            .find(|&a| contains_aggref(a))
            .map_or(-1, locate_aggref),
        NodeTag::T_RelabelType => locate_aggref(node.as_relabel_type().unwrap().arg),
        _ => -1,
    }
}

fn addTargetToSortList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    tle_node: Node<'mcx>,
    mut sortlist: NodeList<'mcx>,
    targetlist: &NodeList<'mcx>,
    sortby: &SortBy<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let tle = tle_node.as_target_entry().unwrap();
    let mut restype = expr_type(tle.expr);

    if restype == UNKNOWNOID {
        let new_expr = coerce::coerce_type(
            mcx,
            pstate,
            tle.expr,
            restype,
            TEXTOID,
            -1,
            coerce::COERCION_IMPLICIT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        // SAFETY: parse analysis holds exclusive access to the targetlist it
        // is transforming; the `tle` borrow above is dead before this write.
        unsafe {
            tle_node.with_mut::<TargetEntry, _>(|t| t.expr = new_expr).unwrap();
        }
        restype = TEXTOID;
    }
    let tle = tle_node.as_target_entry().unwrap();

    let mut location = sortby.location;
    if location < 0 {
        location = expr_location(sortby.node.expect("SortBy.node is never NULL"));
    }

    // C wraps the lookups in a parser errposition callback; the retired
    // pattern attaches the position on Err (only when none is set).
    let attach_pos = |e: Box<PgError>| -> Box<PgError> {
        if e.sqlstate() == ERRCODE_QUERY_CANCELED || e.cursor_position().is_some() {
            return e;
        }
        let pos = parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
        Box::new((*e).with_cursor_position(pos))
    };

    let (sortop, eqop, hashable, reverse) = match sortby.sortby_dir {
        SortByDir::SORTBY_DEFAULT | SortByDir::SORTBY_ASC => {
            let ops = parse_oper::get_sort_group_operators(restype, true, true, false, true)
                .map_err(attach_pos)?;
            (ops.lt_opr, ops.eq_opr, ops.hashable, false)
        }
        SortByDir::SORTBY_DESC => {
            let ops = parse_oper::get_sort_group_operators(restype, false, true, true, true)
                .map_err(attach_pos)?;
            (ops.gt_opr, ops.eq_opr, ops.hashable, true)
        }
        SortByDir::SORTBY_USING => {
            debug_assert!(!sortby.useOp.is_nil());
            let sortop =
                parse_oper::compatible_oper_opid(pstate, &sortby.useOp, restype, restype, false)
                    .map_err(attach_pos)?;
            let Some((eqop, reverse)) =
                lsyscache::amop::get_equality_op_for_ordering_op(sortop)?.filter(|(eq, _)| *eq != InvalidOid)
            else {
                let opname = sortby
                    .useOp
                    .nth(sortby.useOp.len() - 1)
                    .as_string()
                    .expect("operator name list holds String nodes")
                    .sval;
                return Err(Box::new(
                    elog::ereport(ERROR)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!("operator {opname} is not a valid ordering operator"))
                        .errhint(
                            "Ordering operators must be \"<\" or \">\" members of btree operator \
                             families."
                                .to_string(),
                        )
                        .into_error(),
                ));
            };
            let hashable = lsyscache::op_hashjoinable(eqop, restype)?;
            (sortop, eqop, hashable, reverse)
        }
    };

    if !targetIsInSortList(tle, sortop, &sortlist)? {
        let tleSortGroupRef = assignSortGroupRef(tle_node, targetlist);
        let nulls_first = match sortby.sortby_nulls {
            SortByNulls::SORTBY_NULLS_DEFAULT => reverse,
            SortByNulls::SORTBY_NULLS_FIRST => true,
            SortByNulls::SORTBY_NULLS_LAST => false,
        };
        sortlist.lappend(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef,
                    eqop,
                    sortop,
                    reverse_sort: reverse,
                    nulls_first,
                    hashable,
                },
            )?,
        )?;
    }
    Ok(sortlist)
}

pub fn assignSortGroupRef<'mcx>(tle_node: Node<'mcx>, tlist: &NodeList<'mcx>) -> Index {
    let tle = tle_node.as_target_entry().unwrap();
    if tle.ressortgroupref != 0 {
        return tle.ressortgroupref;
    }
    let mut max_ref: Index = 0;
    for n in tlist {
        let r = n.as_target_entry().expect("tlist holds TargetEntry").ressortgroupref;
        if r > max_ref {
            max_ref = r;
        }
    }
    // SAFETY: parse analysis holds exclusive access to the targetlist it is
    // transforming; the `tle` borrow above is dead before this write.
    unsafe {
        tle_node.with_mut::<TargetEntry, _>(|t| t.ressortgroupref = max_ref + 1).unwrap();
    }
    max_ref + 1
}

pub fn targetIsInSortList(
    tle: &TargetEntry<'_>,
    sortop: Oid,
    sort_list: &NodeList<'_>,
) -> PgResult<bool> {
    let tle_ref = tle.ressortgroupref;
    if tle_ref == 0 {
        return Ok(false);
    }
    for n in sort_list {
        let scl = n.as_sort_group_clause().expect("sortlist holds SortGroupClause");
        if scl.tleSortGroupRef == tle_ref
            && (sortop == InvalidOid
                || sortop == scl.sortop
                || sortop == lsyscache::get_commutator(scl.sortop)?)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// C `transformGroupClause`, simple-expression arm: GROUPING SETS/CUBE/
/// ROLLUP (and the implicit-RowExpr flattening they ride on) are loud.
pub fn transformGroupClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    grouplist: &NodeList<'mcx>,
    grouping_sets: &mut NodeList<'mcx>,
    targetlist: &mut NodeList<'mcx>,
    sort_clause: &NodeList<'mcx>,
    expr_kind: ParseExprKind,
    use_sql99: bool,
) -> PgResult<NodeList<'mcx>> {
    *grouping_sets = NodeList::nil();
    let mut result = NodeList::nil();
    let mut seen_local: mcx::PgVec<'_, Index> = mcx::PgVec::new_in(mcx);
    for gexpr in grouplist {
        match gexpr.node_tag() {
            NodeTag::T_GroupingSet => panic!(
                "transformGroupClause (parse_clause.c): GROUPING SETS/CUBE/ROLLUP \
                 (transformGroupingSet) unported — unit backend-parser-clause"
            ),
            NodeTag::T_RowExpr => panic!(
                "flatten_grouping_sets (parse_clause.c): implicit RowExpr arm unported — \
                 unit backend-parser-clause"
            ),
            _ => {}
        }
        let r#ref = transformGroupClauseExpr(
            &mut result,
            &seen_local,
            mcx,
            pstate,
            gexpr,
            targetlist,
            sort_clause,
            expr_kind,
            use_sql99,
        )?;
        if r#ref > 0 {
            seen_local.push(r#ref);
        }
    }
    Ok(result)
}

// C transformGroupClauseExpr, toplevel arm (grouping sets are loud upstream).
#[allow(clippy::too_many_arguments)]
fn transformGroupClauseExpr<'mcx>(
    flatresult: &mut NodeList<'mcx>,
    seen_local: &[Index],
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    gexpr: Node<'mcx>,
    targetlist: &mut NodeList<'mcx>,
    sort_clause: &NodeList<'mcx>,
    expr_kind: ParseExprKind,
    use_sql99: bool,
) -> PgResult<Index> {
    if use_sql99 {
        panic!(
            "transformGroupClauseExpr (parse_clause.c): findTargetlistEntrySQL99 \
             unported — unit backend-parser-clause"
        );
    }
    let tle_node = findTargetlistEntrySQL92(mcx, pstate, gexpr, targetlist, expr_kind)?;
    let tle = tle_node.as_target_entry().unwrap();

    let mut found = false;
    if tle.ressortgroupref > 0 {
        // GROUP BY x, x: local duplicates drop out.
        if seen_local.contains(&tle.ressortgroupref) {
            return Ok(0);
        }
        found = targetIsInSortList(tle, InvalidOid, flatresult)?;
        if !found {
            // A matching ORDER BY item donates its operator info (C copies
            // the SortGroupClause node).
            for sc_node in sort_clause {
                let sc = sc_node.as_sort_group_clause().expect("sortClause cell");
                if sc.tleSortGroupRef == tle.ressortgroupref {
                    flatresult.lappend(mcx, Node::mk(mcx, *sc)?)?;
                    found = true;
                    break;
                }
            }
        }
    }
    if !found {
        addTargetToGroupList(mcx, pstate, tle_node, flatresult, targetlist, expr_location(gexpr))?;
    }
    Ok(tle_node.as_target_entry().unwrap().ressortgroupref)
}

// C addTargetToGroupList: default grouping semantics via
// get_sort_group_operators (sortop optional, eqop required).
fn addTargetToGroupList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    tle_node: Node<'mcx>,
    grouplist: &mut NodeList<'mcx>,
    targetlist: &NodeList<'mcx>,
    location: ParseLoc,
) -> PgResult<()> {
    let tle = tle_node.as_target_entry().unwrap();
    let mut restype = expr_type(tle.expr);

    if restype == UNKNOWNOID {
        let new_expr = coerce::coerce_type(
            mcx,
            pstate,
            tle.expr,
            restype,
            TEXTOID,
            -1,
            coerce::COERCION_IMPLICIT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        // SAFETY: parse analysis holds exclusive access to the targetlist it
        // is transforming; the `tle` borrow above is dead before this write.
        unsafe {
            tle_node.with_mut::<TargetEntry, _>(|t| t.expr = new_expr).unwrap();
        }
        restype = TEXTOID;
    }
    let tle = tle_node.as_target_entry().unwrap();

    if !targetIsInSortList(tle, InvalidOid, grouplist)? {
        let attach_pos = |e: Box<PgError>| -> Box<PgError> {
            if e.sqlstate() == ERRCODE_QUERY_CANCELED || e.cursor_position().is_some() {
                return e;
            }
            let pos = parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
            Box::new((*e).with_cursor_position(pos))
        };
        let ops = parse_oper::get_sort_group_operators(restype, false, true, false, true)
            .map_err(attach_pos)?;
        let tleSortGroupRef = assignSortGroupRef(tle_node, targetlist);
        grouplist.lappend(
            mcx,
            Node::mk(
                mcx,
                SortGroupClause {
                    tleSortGroupRef,
                    eqop: ops.eq_opr,
                    sortop: ops.lt_opr,
                    reverse_sort: false,
                    nulls_first: false,
                    hashable: ops.hashable,
                },
            )?,
        )?;
    }
    Ok(())
}

/// C `transformDistinctClause`: all ORDER BY items (SortGroupClause copies)
/// followed by every remaining non-resjunk tlist item.
pub fn transformDistinctClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    targetlist: &mut NodeList<'mcx>,
    sort_clause: &NodeList<'mcx>,
    is_agg: bool,
) -> PgResult<NodeList<'mcx>> {
    let mut result = NodeList::nil();
    for sc_node in sort_clause {
        let scl = sc_node.as_sort_group_clause().expect("sortClause cell");
        let tle_node = targetlist
            .iter()
            .find(|n| {
                n.as_target_entry().expect("tlist cell").ressortgroupref == scl.tleSortGroupRef
            })
            .unwrap_or_else(|| panic!("ORDER/GROUP BY expression not found in targetlist"));
        let tle = tle_node.as_target_entry().unwrap();
        if tle.resjunk {
            return Err(distinct_orderby_mismatch(pstate, is_agg, expr_location(tle.expr)));
        }
        result.lappend(mcx, Node::mk(mcx, *scl)?)?;
    }
    let n = targetlist.len();
    for i in 0..n {
        let tle_node = targetlist.nth(i);
        if tle_node.as_target_entry().expect("tlist cell").resjunk {
            continue;
        }
        let location = expr_location(tle_node.as_target_entry().unwrap().expr);
        addTargetToGroupList(mcx, pstate, tle_node, &mut result, targetlist, location)?;
    }
    if result.is_nil() {
        return Err(distinct_no_columns(is_agg));
    }
    Ok(result)
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

#[cold]
#[inline(never)]
fn aggregate_in_group_by(
    pstate: &ParseState<'_, '_>,
    expr_kind: ParseExprKind,
    tle_expr: Node<'_>,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_GROUPING_ERROR)
            .errmsg(format!(
                "aggregate functions are not allowed in {}",
                ParseExprKindName(expr_kind)
            ))
            .errposition(parser_errposition(
                pstate,
                locate_aggref(tle_expr),
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "checkTargetlistEntrySQL92",
            )),
    )
}

#[cold]
#[inline(never)]
fn ambiguous_column(
    pstate: &ParseState<'_, '_>,
    expr_kind: ParseExprKind,
    name: &str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_AMBIGUOUS_COLUMN)
            .errmsg(format!("{} \"{name}\" is ambiguous", ParseExprKindName(expr_kind)))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "findTargetlistEntrySQL92",
            )),
    )
}

#[cold]
#[inline(never)]
fn non_integer_constant(
    pstate: &ParseState<'_, '_>,
    expr_kind: ParseExprKind,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("non-integer constant in {}", ParseExprKindName(expr_kind)))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "findTargetlistEntrySQL92",
            )),
    )
}

#[cold]
#[inline(never)]
fn position_not_in_select_list(
    pstate: &ParseState<'_, '_>,
    expr_kind: ParseExprKind,
    target_pos: i32,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "{} position {target_pos} is not in select list",
                ParseExprKindName(expr_kind)
            ))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "findTargetlistEntrySQL92",
            )),
    )
}

#[cold]
#[inline(never)]
fn contains_variables(
    pstate: &ParseState<'_, '_>,
    construct_name: &str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!("argument of {construct_name} must not contain variables"))
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_clause.c", 0, "checkExprIsVarFree")),
    )
}

#[cold]
#[inline(never)]
fn distinct_orderby_mismatch(
    pstate: &ParseState<'_, '_>,
    is_agg: bool,
    location: ParseLoc,
) -> Box<PgError> {
    let msg = if is_agg {
        "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
    } else {
        "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(msg.to_string())
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "transformDistinctClause",
            )),
    )
}

#[cold]
#[inline(never)]
fn distinct_no_columns(is_agg: bool) -> Box<PgError> {
    let msg = if is_agg {
        "an aggregate with DISTINCT must have at least one argument"
    } else {
        "SELECT DISTINCT must have at least one column"
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg.to_string())
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "transformDistinctClause",
            )),
    )
}

#[cold]
#[inline(never)]
fn null_row_count_with_ties() -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE)
            .errmsg("row count cannot be null in FETCH FIRST ... WITH TIES clause".to_string())
            .into_error()
            .with_error_location(ErrorLocation::new("parse_clause.c", 0, "transformLimitClause")),
    )
}
