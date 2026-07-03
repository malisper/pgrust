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
        other => panic!(
            "transformFromClauseItem (parse_clause.c): arm for {other:?} \
             (subselect/function/tablesample/tablefunc/JOIN) unported — \
             unit backend-parser-clause"
        ),
    }
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
    if alsoSource {
        panic!(
            "setTargetTable (parse_clause.c): UPDATE/DELETE alsoSource lane \
             (addNSItemToQuery of the target) unported — unit backend-parser-clause"
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
    pstate.p_target_nsitem = Some(nsitem);
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
    _mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    tlist: &mut NodeList<'mcx>,
    expr_kind: ParseExprKind,
) -> PgResult<Node<'mcx>> {
    if let Some(cref) = node.as_column_ref() {
        if let [field1] = cref.fields.as_slice() {
            if let Some(name) = field1.as_string().map(|s| s.sval) {
                if expr_kind == ParseExprKind::EXPR_KIND_GROUP_BY {
                    panic!(
                        "findTargetlistEntrySQL92 (parse_clause.c): GROUP BY colNameToVar \
                         precedence arm unported — unit backend-parser-clause"
                    );
                }
                let mut target_result: Option<Node<'mcx>> = None;
                for tle_node in &*tlist {
                    let tle = tle_node.as_target_entry().expect("tlist holds TargetEntry");
                    if !tle.resjunk && tle.resname == Some(name) {
                        if target_result.is_some() {
                            // C compares equal(target_result->expr, tle->expr)
                            // and errors only on distinct values.
                            panic!(
                                "findTargetlistEntrySQL92 (parse_clause.c): duplicate-name \
                                 disambiguation needs equal() (equalfuncs.c) — 42702 \
                                 \"{} \\\"{name}\\\" is ambiguous\" when values differ — \
                                 unit backend-nodes-equalfuncs",
                                ParseExprKindName(expr_kind)
                            );
                        }
                        target_result = Some(tle_node);
                    }
                }
                if let Some(tle_node) = target_result {
                    checkTargetlistEntrySQL92(pstate, expr_kind)?;
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
                    checkTargetlistEntrySQL92(pstate, expr_kind)?;
                    return Ok(tle_node);
                }
            }
        }
        return Err(position_not_in_select_list(pstate, expr_kind, target_pos, aconst.location));
    }
    panic!(
        "findTargetlistEntrySQL92 (parse_clause.c): SQL99 expression fallthrough \
         (findTargetlistEntrySQL99: equal()/strip_implicit_coercions + resjunk \
         transformTargetEntry) unported — unit backend-parser-clause"
    );
}

fn checkTargetlistEntrySQL92(
    _pstate: &ParseState<'_, '_>,
    expr_kind: ParseExprKind,
) -> PgResult<()> {
    match expr_kind {
        ParseExprKind::EXPR_KIND_GROUP_BY => panic!(
            "checkTargetlistEntrySQL92 (parse_clause.c): GROUP BY aggregate/window rejection \
             needs contain_aggs_of_level/contain_windowfuncs — unit backend-parser-agg"
        ),
        ParseExprKind::EXPR_KIND_ORDER_BY | ParseExprKind::EXPR_KIND_DISTINCT_ON => Ok(()),
        _ => Err(Box::new(PgError::error(
            "unexpected exprKind in checkTargetlistEntrySQL92".to_string(),
        ))),
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
fn null_row_count_with_ties() -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE)
            .errmsg("row count cannot be null in FETCH FIRST ... WITH TIES clause".to_string())
            .into_error()
            .with_error_location(ErrorLocation::new("parse_clause.c", 0, "transformLimitClause")),
    )
}
