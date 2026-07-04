#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use mcx::Mcx;
use parse_expr::{
    expr_collation, expr_location, expr_location_list, expr_type, transformExpr,
    ParseExprKindName,
};
use parse_relation::{addRangeTableEntry, checkNameSpaceConflicts};
use parser_small1::{parser_errposition, ParseExprKind, ParseNamespaceItem, ParseState};
use types_core::catalog::{INT8OID, TEXTOID, UNKNOWNOID};
use types_core::{Index, InvalidOid, Oid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE, ERRCODE_QUERY_CANCELED, ERRCODE_SYNTAX_ERROR,
    ERRCODE_TOO_MANY_COLUMNS, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::nodes_enums::LimitOption;
use types_nodes::parsenodes::{GroupingSet, GroupingSetKind, SortGroupClause, WindowClause};
use types_nodes::primnodes::TargetEntry;
use types_nodes::rawnodes::{
    SortBy, SortByDir, SortByNulls, ValUnion, FRAMEOPTION_DEFAULTS, FRAMEOPTION_END_OFFSET,
    FRAMEOPTION_GROUPS, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS, FRAMEOPTION_START_OFFSET,
};
use types_nodes::{CoercionForm, Node, NodeList, NodeTag};

pub fn init_seams() {
    parse_clause_seams::transform_agg_order_distinct::set(transform_agg_order_distinct);
}

// transformAggregateCall's ordered/DISTINCT arm (parse_agg.c), hosted here
// because transformSortClause/transformDistinctClause/exprType all live above
// parse_agg; parse_agg reaches it through parse_clause_seams.
fn transform_agg_order_distinct<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    tlist: &mut NodeList<'mcx>,
    agg_order: &NodeList<'mcx>,
    agg_distinct: bool,
) -> PgResult<(NodeList<'mcx>, NodeList<'mcx>, mcx::PgVec<'mcx, Oid>)> {
    let torder = transformSortClause(
        mcx,
        pstate,
        agg_order,
        tlist,
        ParseExprKind::EXPR_KIND_ORDER_BY,
        true,
    )?;
    let mut tdistinct = NodeList::nil();
    if agg_distinct {
        tdistinct = transformDistinctClause(mcx, pstate, tlist, &torder, true)?;
        for sc_node in &tdistinct {
            let scl = sc_node.as_sort_group_clause().expect("aggdistinct cell");
            if scl.sortop == InvalidOid {
                let expr = tlist
                    .iter()
                    .find(|n| {
                        n.as_target_entry().expect("tlist cell").ressortgroupref
                            == scl.tleSortGroupRef
                    })
                    .map(|n| n.as_target_entry().unwrap().expr)
                    .expect("DISTINCT expression not found in targetlist");
                return Err(no_distinct_ordering_operator(pstate, expr));
            }
        }
    }
    let mut argtypes = mcx::PgVec::new_in(mcx);
    for tle_node in &*tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if !tle.resjunk {
            argtypes.push(expr_type(tle.expr));
        }
    }
    Ok((torder, tdistinct, argtypes))
}

#[cold]
fn no_distinct_ordering_operator(pstate: &ParseState<'_, '_>, expr: Node<'_>) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify an ordering operator for type {}",
                format_type::format_type_be(expr_type(expr)).unwrap_or_default()
            ))
            .errdetail("Aggregates with DISTINCT must be able to sort their inputs.".to_string())
            .errposition(parser_errposition(
                pstate,
                expr_location(expr),
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_agg.c", 0, "transformAggregateCall")),
    )
}

pub fn transformFromClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    frm_list: &NodeList<'mcx>,
) -> PgResult<()> {
    for item in frm_list {
        let (n, namespace) = transformFromClauseItemNs(mcx, pstate, item)?;

        checkNameSpaceConflicts(pstate.p_namespace.as_slice(), namespace.as_slice())?;

        setNamespaceLateralState(namespace.as_slice(), true, true);

        pstate.p_joinlist.lappend(mcx, n)?;
        for nsitem in namespace {
            pstate.p_namespace.push(nsitem);
        }
    }

    setNamespaceLateralState(pstate.p_namespace.as_slice(), false, true);
    Ok(())
}

fn setNamespaceLateralState(
    namespace: &[&ParseNamespaceItem<'_>],
    lateral_only: bool,
    lateral_ok: bool,
) {
    for nsitem in namespace {
        nsitem.p_lateral_only.set(lateral_only);
        nsitem.p_lateral_ok.set(lateral_ok);
    }
}

// C transformFromClauseItem's (node, *top_nsitem, *namespace) contract with
// the top nsitem folded in as the namespace's last element (true for every
// arm: single-item lists for the scan arms, my_namespace + join nsitem for
// the join arm).
fn transformFromClauseItemNs<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    n: Node<'mcx>,
) -> PgResult<(Node<'mcx>, mcx::PgVec<'mcx, &'mcx ParseNamespaceItem<'mcx>>)> {
    if n.node_tag() == NodeTag::T_JoinExpr {
        return transformJoinExpr(mcx, pstate, n.as_join_expr().unwrap());
    }
    let (node, nsitem) = transformFromClauseItem(mcx, pstate, n)?;
    let mut namespace: mcx::PgVec<'mcx, &'mcx ParseNamespaceItem<'mcx>> =
        mcx::PgVec::new_in(mcx);
    namespace.push(nsitem);
    Ok((node, namespace))
}

// JoinExpr arm of C transformFromClauseItem, INNER JOIN ... ON slice.
// C divergences on this lane: the temporary exposure of l_namespace to the
// RHS exists only for LATERAL (loud on every FROM arm), so it is skipped; the
// post-quals visibility mutations C applies in place land here as rebuilt
// namespace items carrying the final flags.
fn transformJoinExpr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    j: &types_nodes::JoinExpr<'mcx>,
) -> PgResult<(Node<'mcx>, mcx::PgVec<'mcx, &'mcx ParseNamespaceItem<'mcx>>)> {
    if j.isNatural || !j.usingClause.is_nil() || j.join_using_alias.is_some() {
        panic!(
            "transformFromClauseItem (parse_clause.c): JOIN USING/NATURAL \
             (transformJoinUsingClause/buildMergedJoinVar) unported — join-using lane"
        );
    }
    if !matches!(
        j.jointype,
        types_nodes::JoinType::JOIN_INNER
            | types_nodes::JoinType::JOIN_LEFT
            | types_nodes::JoinType::JOIN_RIGHT
            | types_nodes::JoinType::JOIN_FULL
    ) {
        panic!(
            "transformFromClauseItem (parse_clause.c): unrecognized join type {:?}",
            j.jointype
        );
    }

    let (larg, l_namespace) = transformFromClauseItemNs(mcx, pstate, j.larg)?;
    let l_nsitem = *l_namespace.last().expect("child namespace is never empty");

    // Left-side names are LATERAL-visible to the RHS; per SQL:2008 they are
    // exposed but not referenceable unless the join type is INNER or LEFT.
    let lateral_ok = matches!(
        j.jointype,
        types_nodes::JoinType::JOIN_INNER | types_nodes::JoinType::JOIN_LEFT
    );
    setNamespaceLateralState(l_namespace.as_slice(), true, lateral_ok);
    let sv_namespace_length = pstate.p_namespace.len();
    for it in &l_namespace {
        pstate.p_namespace.push(*it);
    }

    let (rarg, r_namespace) = transformFromClauseItemNs(mcx, pstate, j.rarg)?;

    pstate.p_namespace.truncate(sv_namespace_length);
    let r_nsitem = *r_namespace.last().expect("child namespace is never empty");

    checkNameSpaceConflicts(l_namespace.as_slice(), r_namespace.as_slice())?;

    let mut my_namespace = l_namespace;
    for it in r_namespace {
        my_namespace.push(it);
    }

    let quals = match j.quals {
        Some(q) => Some(transformJoinOnClause(mcx, pstate, q, &my_namespace)?),
        None => None,
    };

    let rtindex = pstate.p_rtable.len() as i32 + 1;

    // Child joins and this join's quals are transformed; every later Var
    // referencing the nullable side (including the alias Vars built below)
    // must carry this join's nulling bit.
    match j.jointype {
        types_nodes::JoinType::JOIN_LEFT => markRelsAsNulledBy(mcx, pstate, rarg, rtindex)?,
        types_nodes::JoinType::JOIN_RIGHT => markRelsAsNulledBy(mcx, pstate, larg, rtindex)?,
        types_nodes::JoinType::JOIN_FULL => {
            markRelsAsNulledBy(mcx, pstate, larg, rtindex)?;
            markRelsAsNulledBy(mcx, pstate, rarg, rtindex)?;
        }
        _ => {}
    }

    let l_count = l_nsitem.p_names.colnames.len();
    let r_count = r_nsitem.p_names.colnames.len();
    let mut res_colnames = NodeList::nil();
    let mut res_colvars = NodeList::nil();
    let mut res_nscolumns: mcx::PgVec<'mcx, parser_small1::ParseNamespaceColumn> =
        mcx::vec_with_capacity_in(mcx, l_count + r_count)?;
    let mut l_colnos = types_nodes::list::IntList::nil();
    let mut r_colnos = types_nodes::list::IntList::nil();
    extractRemainingColumns(
        mcx,
        pstate,
        l_nsitem.p_nscolumns,
        &l_nsitem.p_names.colnames,
        &mut l_colnos,
        &mut res_colnames,
        &mut res_colvars,
        &mut res_nscolumns,
    )?;
    extractRemainingColumns(
        mcx,
        pstate,
        r_nsitem.p_nscolumns,
        &r_nsitem.p_names.colnames,
        &mut r_colnos,
        &mut res_colnames,
        &mut res_colvars,
        &mut res_nscolumns,
    )?;

    // A join alias syntactically hides all inputs.
    if j.alias.is_some() {
        for (k, nscol) in res_nscolumns.iter_mut().enumerate() {
            nscol.p_varnosyn = rtindex as Index;
            nscol.p_varattnosyn = k as i16 + 1;
        }
    }

    let nsitem = parse_relation::addRangeTableEntryForJoin(
        mcx,
        pstate,
        &res_colnames,
        res_nscolumns,
        j.jointype,
        0,
        res_colvars,
        l_colnos,
        r_colnos,
        None,
        j.alias,
        true,
    )?;
    assert_eq!(nsitem.p_rtindex, rtindex, "predicted join RT index");

    let jnode = Node::mk(
        mcx,
        types_nodes::JoinExpr {
            jointype: j.jointype,
            isNatural: false,
            larg,
            rarg,
            usingClause: NodeList::nil(),
            join_using_alias: None,
            quals,
            alias: j.alias,
            rtindex,
        },
    )?;

    while pstate.p_joinexprs.len() + 1 < rtindex as usize {
        pstate.p_joinexprs.push(None);
    }
    pstate.p_joinexprs.push(Some(jnode));
    debug_assert_eq!(pstate.p_joinexprs.len(), rtindex as usize);

    // With an alias the contained RTEs are hidden completely; otherwise they
    // stay visible as table names but not for unqualified column access.
    let mut namespace: mcx::PgVec<'mcx, &'mcx ParseNamespaceItem<'mcx>> =
        mcx::PgVec::new_in(mcx);
    if j.alias.is_none() {
        for item in &my_namespace {
            namespace.push(&*mcx::leak_in(mcx::alloc_in(
                mcx,
                ParseNamespaceItem {
                    p_names: item.p_names,
                    p_rte: item.p_rte,
                    p_rtindex: item.p_rtindex,
                    p_perminfo: item.p_perminfo,
                    p_nscolumns: item.p_nscolumns,
                    p_rel_visible: item.p_rel_visible,
                    p_cols_visible: false,
                    p_lateral_only: core::cell::Cell::new(false),
                    p_lateral_ok: core::cell::Cell::new(true),
                    p_returning_type: item.p_returning_type,
                },
            )?));
        }
    }
    nsitem.p_rel_visible = j.alias.is_some();
    nsitem.p_cols_visible = true;
    nsitem.p_lateral_only.set(false);
    nsitem.p_lateral_ok.set(true);
    namespace.push(nsitem);

    Ok((jnode, namespace))
}

// markRelsAsNulledBy (parse_clause.c) over the transformed jointree node.
fn markRelsAsNulledBy<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    n: Node<'mcx>,
    jindex: i32,
) -> PgResult<()> {
    let varno = match n.node_tag() {
        NodeTag::T_RangeTblRef => n.as_range_tbl_ref().unwrap().rtindex,
        NodeTag::T_JoinExpr => {
            let j = n.as_join_expr().unwrap();
            markRelsAsNulledBy(mcx, pstate, j.larg, jindex)?;
            markRelsAsNulledBy(mcx, pstate, j.rarg, jindex)?;
            j.rtindex
        }
        other => panic!("unrecognized node type: {other:?}"),
    };
    while pstate.p_nullingrels.len() < varno as usize {
        pstate.p_nullingrels.push(types_nodes::Bitmapset::empty());
    }
    pstate.p_nullingrels[varno as usize - 1].add_member(mcx, jindex)
}

// transformJoinOnClause (parse_clause.c): the ON expression sees exactly the
// join's two subtrees plus upper levels.
fn transformJoinOnClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    quals: Node<'mcx>,
    my_namespace: &mcx::PgVec<'mcx, &'mcx ParseNamespaceItem<'mcx>>,
) -> PgResult<Node<'mcx>> {
    setNamespaceLateralState(my_namespace.as_slice(), false, true);
    let mut ns: mcx::PgVec<'mcx, &'mcx ParseNamespaceItem<'mcx>> = mcx::PgVec::new_in(mcx);
    for it in my_namespace {
        ns.push(it);
    }
    let save_namespace = core::mem::replace(&mut pstate.p_namespace, ns);
    let result =
        transformWhereClause(mcx, pstate, Some(quals), ParseExprKind::EXPR_KIND_JOIN_ON, "JOIN/ON");
    pstate.p_namespace = save_namespace;
    Ok(result?.expect("quals in, quals out"))
}

// extractRemainingColumns (parse_clause.c); USING is loud upstream so no
// columns are ever pre-merged and the prevcols bitmapset degenerates.
#[allow(clippy::too_many_arguments)]
fn extractRemainingColumns<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    src_nscolumns: &[parser_small1::ParseNamespaceColumn],
    src_colnames: &NodeList<'mcx>,
    src_colnos: &mut types_nodes::list::IntList<'mcx>,
    res_colnames: &mut NodeList<'mcx>,
    res_colvars: &mut NodeList<'mcx>,
    res_nscolumns: &mut mcx::PgVec<'mcx, parser_small1::ParseNamespaceColumn>,
) -> PgResult<()> {
    debug_assert!(src_colnos.is_nil());
    for (i, colname_node) in src_colnames.iter().enumerate() {
        let colname = colname_node.as_string().expect("eref colnames are String nodes").sval;
        let attnum = i as i32 + 1;
        // Dropped columns carry empty names.
        if colname.is_empty() || src_nscolumns[i].p_dontexpand {
            continue;
        }
        src_colnos.lappend(mcx, attnum)?;
        res_colnames.lappend(mcx, colname_node)?;
        res_colvars
            .lappend(mcx, parse_relation::buildVarFromNSColumn(mcx, pstate, &src_nscolumns[i])?)?;
        res_nscolumns.push(src_nscolumns[i]);
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
            // getNSItemForSpecialRelationTypes: an unqualified name is a CTE
            // reference if any ctenamespace up the chain carries it (before
            // plain-relation resolution); ENRs (p_queryEnv) stay loud.
            if rv.schemaname.is_none() {
                if pstate.p_queryEnv.is_some() {
                    panic!(
                        "transformFromClauseItem (parse_clause.c): ENR reference \
                         (scanNameSpaceForENR) unported — unit backend-parser-clause"
                    );
                }
                let refname = rv.relname.expect("grammar always sets relname");
                if let Some((cte, levelsup)) =
                    parse_relation::scanNameSpaceForCTE(pstate, refname)
                {
                    let nsitem =
                        parse_relation::addRangeTableEntryForCTE(mcx, pstate, cte, levelsup, rv, true)?;
                    let rtr = Node::mk_range_tbl_ref(mcx, nsitem.p_rtindex)?;
                    return Ok((rtr, nsitem));
                }
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
        NodeTag::T_RangeSubselect => {
            let nsitem = transformRangeSubselect(mcx, pstate, n.as_range_subselect().unwrap())?;
            let rtr = Node::mk_range_tbl_ref(mcx, nsitem.p_rtindex)?;
            Ok((rtr, nsitem))
        }
        other => panic!(
            "transformFromClauseItem (parse_clause.c): arm for {other:?} \
             (tablesample/tablefunc) unported — \
             unit backend-parser-clause"
        ),
    }
}

fn transformRangeSubselect<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    r: &types_nodes::rawnodes::RangeSubselect<'mcx>,
) -> PgResult<&'mcx ParseNamespaceItem<'mcx>> {
    debug_assert_eq!(pstate.p_expr_kind, ParseExprKind::EXPR_KIND_NONE);
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_FROM_SUBSELECT;

    debug_assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = r.lateral;

    let locked = parse_relation::isLockedRefname(pstate, r.alias.and_then(|a| a.aliasname));
    let query = analyze_seams::parse_sub_analyze::call(
        mcx,
        r.subquery.expect("grammar always sets RangeSubselect.subquery"),
        pstate,
        None,
        locked,
        true,
    )?;

    pstate.p_lateral_active = false;
    pstate.p_expr_kind = ParseExprKind::EXPR_KIND_NONE;

    if query.commandType != types_nodes::nodes_enums::CmdType::CMD_SELECT {
        return Err(Box::new(PgError::error(
            "unexpected non-SELECT command in subquery in FROM".to_string(),
        )));
    }

    let mut columns: mcx::PgVec<'mcx, (Option<&'mcx str>, Oid, i32, Oid)> =
        mcx::vec_with_capacity_in(mcx, query.targetList.len())?;
    for tle_node in &query.targetList {
        let te = tle_node.as_target_entry().expect("tlist cell");
        if te.resjunk {
            continue;
        }
        columns.push((
            te.resname,
            expr_type(te.expr),
            parse_expr::expr_typmod(te.expr),
            parse_expr::expr_collation(te.expr),
        ));
    }

    let query = &*mcx::leak_in(mcx::alloc_in(mcx, query)?);
    let nsitem = parse_relation::addRangeTableEntryForSubquery(
        mcx, pstate, query, &columns, r.alias, r.lateral, true,
    )?;
    Ok(&*nsitem)
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

    let is_lateral = r.lateral || vars::contain_vars_of_level(newfexpr, 0)?;

    parse_relation::addRangeTableEntryForFunction(
        mcx,
        pstate,
        funcname,
        newfexpr,
        r.alias,
        r.ordinality,
        is_lateral,
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

// transformIndexStmt (parse_utilcmd.c), hosted here because parse_utilcmd ->
// parse_expr would cycle (parse_expr uses parse_utilcmd::typenameTypeIdAndMod).
pub fn transformIndexStmt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    stmt_node: Node<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    use types_nodes::rawnodes::{IndexElem, IndexStmt};
    let (transformed, where_clause, params) = {
        let stmt = stmt_node.as_variant::<IndexStmt>().expect("IndexStmt");
        let mut params: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
        params.extend(stmt.indexParams.iter());
        (stmt.transformed, stmt.whereClause, params)
    };
    if transformed {
        return Ok(());
    }

    let mut pstate = parser_small1::make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(bytes_in(mcx, query_string.as_bytes())?);

    let rel = table::table_open(mcx, relid, types_rel::NoLock)?;
    let nsitem = parse_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        &rel,
        types_rel::AccessShareLock,
        None,
        false,
        true,
    )?;
    parse_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;

    if let Some(wc) = where_clause {
        let qual = transformWhereClause(
            mcx,
            &mut pstate,
            Some(wc),
            ParseExprKind::EXPR_KIND_INDEX_PREDICATE,
            "WHERE",
        )?
        .expect("WHERE clause present");
        parse_collate::assign_expr_collations(mcx, &mut pstate, qual)?;
        // SAFETY: analyze-owned parse tree; no derived refs live.
        unsafe { stmt_node.with_mut::<IndexStmt, _>(|s| s.whereClause = Some(qual)) }
            .expect("IndexStmt");
    }

    for node in params.iter() {
        let raw = node.as_variant::<IndexElem>().expect("IndexElem").expr;
        let Some(raw) = raw else { continue };
        let figured = parse_target::FigureIndexColname(raw);
        let expr = transformExpr(mcx, &mut pstate, raw, ParseExprKind::EXPR_KIND_INDEX_EXPRESSION)?;
        parse_collate::assign_expr_collations(mcx, &mut pstate, expr)?;
        // SAFETY: analyze-owned parse tree; no derived refs live.
        unsafe {
            node.with_mut::<IndexElem, _>(|e| {
                if e.indexcolname.is_none() {
                    e.indexcolname = figured;
                }
                e.expr = Some(expr);
            })
        }
        .expect("IndexElem");
    }

    if pstate.p_rtable.len() != 1 {
        return Err(index_expr_other_table());
    }
    parser_small1::free_parsestate(pstate)?;
    rel.close(types_rel::NoLock)?;
    // SAFETY: analyze-owned parse tree; no derived refs live.
    unsafe { stmt_node.with_mut::<IndexStmt, _>(|s| s.transformed = true) }.expect("IndexStmt");
    Ok(())
}

#[cold]
#[inline(never)]
fn index_expr_other_table() -> Box<PgError> {
    Box::new(
        PgError::error("index expressions and predicates can refer only to the table being indexed")
            .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE),
    )
}

fn bytes_in<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<&'mcx [u8]> {
    let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, b.len())?;
    mcx::vec_append_bytes(&mut v, b)?;
    Ok(v.leak())
}

/// C `transformOnConflictArbiter` (parse_clause.c); returns
/// (arbiterElems, arbiterWhere, constraint).
pub fn transformOnConflictArbiter<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    onConflictClause: &types_nodes::OnConflictClause<'mcx>,
) -> PgResult<(NodeList<'mcx>, Option<Node<'mcx>>, Oid)> {
    use types_nodes::OnConflictAction;

    let infer = onConflictClause
        .infer
        .map(|n| n.as_infer_clause().expect("grammar builds InferClause"));

    if onConflictClause.action == OnConflictAction::ONCONFLICT_UPDATE && infer.is_none() {
        return Err(on_conflict_requires_inference(pstate, onConflictClause.location));
    }

    // Speculative insertion into system catalogs is disallowed.
    let target = pstate.p_target_relation.as_ref().expect("ON CONFLICT with no target relation");
    if catalog::IsCatalogRelation(target) {
        return Err(on_conflict_on_catalog(pstate, onConflictClause.location));
    }
    // C also rejects RelationIsUsedAsCatalogTable; the user_catalog_table
    // reloption has no storage here, so the check has nothing to test.

    let mut arbiter_elems = NodeList::nil();
    let mut arbiter_where = None;
    let constraint = InvalidOid;

    if let Some(infer) = infer {
        // Arbiter expressions see only non-qualified references to the
        // target table; hide every other relation.
        let save_namespace =
            core::mem::replace(&mut pstate.p_namespace, mcx::PgVec::new_in(mcx));
        let target_nsitem = arbiter_target_nsitem(mcx, pstate)?;
        parse_relation::addNSItemToQuery(mcx, pstate, target_nsitem, false, false, true)?;

        if !infer.indexElems.is_nil() {
            arbiter_elems = resolve_unique_index_expr(mcx, pstate, infer)?;
        }
        if let Some(where_clause) = infer.whereClause {
            arbiter_where = Some(transformExpr(
                mcx,
                pstate,
                where_clause,
                ParseExprKind::EXPR_KIND_INDEX_PREDICATE,
            )?);
        }
        pstate.p_namespace = save_namespace;

        if infer.conname.is_some() {
            panic!(
                "transformOnConflictArbiter (parse_clause.c): ON CONSTRAINT arbiter \
                 (get_relation_constraint_attnos) unported — upsert lane"
            );
        }
    }

    Ok((arbiter_elems, arbiter_where, constraint))
}

// C `resolve_unique_index_expr` (parse_clause.c).
fn resolve_unique_index_expr<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    infer: &types_nodes::InferClause<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    use types_nodes::rawnodes::ColumnRef;

    let mut result = NodeList::nil();
    for elem_node in &infer.indexElems {
        let ielem = elem_node.as_variant::<types_nodes::IndexElem>().expect("index_params cell");

        if ielem.ordering != SortByDir::SORTBY_DEFAULT {
            return Err(on_conflict_bad_index_elem(
                pstate,
                "ASC/DESC is not allowed in ON CONFLICT clause",
                infer.location,
            ));
        }
        if ielem.nulls_ordering != SortByNulls::SORTBY_NULLS_DEFAULT {
            return Err(on_conflict_bad_index_elem(
                pstate,
                "NULLS FIRST/LAST is not allowed in ON CONFLICT clause",
                infer.location,
            ));
        }

        let parse = match ielem.expr {
            Some(expr) => expr,
            None => {
                let name = ielem.name.expect("IndexElem without expr has a name");
                let mut fields = NodeList::nil();
                fields.lappend(mcx, Node::mk_string(mcx, name)?)?;
                Node::mk(mcx, ColumnRef { fields, location: infer.location })?
            }
        };
        let expr = transformExpr(mcx, pstate, parse, ParseExprKind::EXPR_KIND_INDEX_EXPRESSION)?;

        if !ielem.collation.is_nil() || !ielem.opclass.is_nil() || !ielem.opclassopts.is_nil() {
            panic!(
                "resolve_unique_index_expr (parse_clause.c): COLLATE/opclass arbiter \
                 elements (LookupCollation/ResolveOpClass) unported — upsert lane"
            );
        }

        result.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::InferenceElem {
                    expr: Some(expr),
                    infercollid: InvalidOid,
                    inferopclass: InvalidOid,
                },
            )?,
        )?;
    }
    Ok(result)
}

// The target nsitem is stored as a shared ref; addNSItemToQuery needs an
// owned-mutable copy to set visibility flags on.
fn arbiter_target_nsitem<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
) -> PgResult<&'mcx mut ParseNamespaceItem<'mcx>> {
    let t = pstate.p_target_nsitem.expect("setTargetTable set p_target_nsitem");
    let nsitem = ParseNamespaceItem {
        p_names: t.p_names,
        p_rte: t.p_rte,
        p_rtindex: t.p_rtindex,
        p_perminfo: t.p_perminfo,
        p_nscolumns: t.p_nscolumns,
        p_rel_visible: false,
        p_cols_visible: true,
        p_lateral_only: core::cell::Cell::new(false),
        p_lateral_ok: core::cell::Cell::new(true),
        p_returning_type: t.p_returning_type,
    };
    Ok(mcx::leak_in(mcx::alloc_in(mcx, nsitem)?))
}

#[cold]
#[inline(never)]
fn on_conflict_requires_inference(
    pstate: &ParseState<'_, '_>,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("ON CONFLICT DO UPDATE requires inference specification or constraint name")
            .errhint("For example, ON CONFLICT (column_name).")
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "transformOnConflictArbiter",
            )),
    )
}

#[cold]
#[inline(never)]
fn on_conflict_on_catalog(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("ON CONFLICT is not supported with system catalog tables")
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "transformOnConflictArbiter",
            )),
    )
}

#[cold]
#[inline(never)]
fn on_conflict_bad_index_elem(
    pstate: &ParseState<'_, '_>,
    msg: &'static str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(msg)
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "resolve_unique_index_expr",
            )),
    )
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
        let tle = if use_sql99 {
            findTargetlistEntrySQL99(mcx, pstate, sort_node, targetlist, expr_kind)?
        } else {
            findTargetlistEntrySQL92(mcx, pstate, sort_node, targetlist, expr_kind)?
        };
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

// C strip_implicit_coercions (nodeFuncs.c) over the ported coercion families.
fn strip_implicit_coercions(node: Node<'_>) -> Node<'_> {
    use types_nodes::primnodes::CoercionForm;
    match node.node_tag() {
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if f.funcformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(f.args.nth(0));
            }
            node
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            if r.relabelformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(r.arg);
            }
            node
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            if c.coerceformat == CoercionForm::COERCE_IMPLICIT_CAST {
                return strip_implicit_coercions(c.arg);
            }
            node
        }
        _ => node,
    }
}

// C findTargetlistEntrySQL99: equal() match against implicit-coercion-
// stripped tlist exprs, else a resjunk entry.
fn findTargetlistEntrySQL99<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    tlist: &mut NodeList<'mcx>,
    expr_kind: ParseExprKind,
) -> PgResult<Node<'mcx>> {
    let expr = transformExpr(mcx, pstate, node, expr_kind)?;
    for tle_node in &*tlist {
        let tle = tle_node.as_target_entry().expect("tlist holds TargetEntry");
        let texpr = strip_implicit_coercions(tle.expr);
        if types_nodes::equal::equal(expr, texpr) {
            return Ok(tle_node);
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
            if pstate.p_hasWindowFuncs && parse_agg::contain_windowfuncs(tle_expr) {
                return Err(window_in_group_by(pstate, expr_kind, tle_expr));
            }
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
        // C's walker matches GroupingFunc agglevelsup like an Aggref.
        NodeTag::T_Aggref | NodeTag::T_GroupingFunc => true,
        NodeTag::T_Var | NodeTag::T_Const | NodeTag::T_Param => false,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().args.iter().any(contains_aggref),
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().args.iter().any(contains_aggref),
        NodeTag::T_RelabelType => contains_aggref(node.as_relabel_type().unwrap().arg),
        NodeTag::T_CollateExpr => contains_aggref(node.as_collate_expr().unwrap().arg),
        NodeTag::T_BoolExpr => node.as_bool_expr().unwrap().args.iter().any(contains_aggref),
        NodeTag::T_NullTest => {
            node.as_null_test().unwrap().arg.is_some_and(contains_aggref)
        }
        NodeTag::T_BooleanTest => {
            node.as_boolean_test().unwrap().arg.is_some_and(contains_aggref)
        }
        NodeTag::T_DistinctExpr => {
            node.as_distinct_expr().unwrap().args.iter().any(contains_aggref)
        }
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().args.iter().any(contains_aggref),
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
        NodeTag::T_GroupingFunc => node.as_grouping_func().unwrap().location,
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
        NodeTag::T_CollateExpr => locate_aggref(node.as_collate_expr().unwrap().arg),
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

/// The three shapes C `flatten_grouping_sets` returns through `Node *`:
/// `(Node *) NIL`, one node, or a `List *`.
enum Flattened<'mcx> {
    Nil,
    One(Node<'mcx>),
    Many(NodeList<'mcx>),
}

fn flatten_grouping_sets<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Node<'mcx>,
    toplevel: bool,
    has_grouping_sets: Option<&mut bool>,
) -> PgResult<Flattened<'mcx>> {
    match expr.node_tag() {
        NodeTag::T_RowExpr => {
            let r = expr.as_row_expr().unwrap();
            if r.row_format == CoercionForm::COERCE_IMPLICIT_CAST {
                return Ok(Flattened::Many(flatten_grouping_sets_list(
                    mcx, &r.args, false, None,
                )?));
            }
            Ok(Flattened::One(expr))
        }
        NodeTag::T_GroupingSet => {
            let gset = expr.as_grouping_set().unwrap();
            if let Some(flag) = has_grouping_sets {
                *flag = true;
            }
            // At top level, skip over all empty grouping sets; the caller
            // supplies the canonical GROUP BY () if nothing is left.
            if toplevel && gset.kind == GroupingSetKind::GROUPING_SET_EMPTY {
                return Ok(Flattened::Nil);
            }
            let mut result_set = NodeList::nil();
            for n1 in &gset.content {
                let n2 = flatten_grouping_sets(mcx, n1, false, None)?;
                let n1_is_sets = n1
                    .as_grouping_set()
                    .is_some_and(|g| g.kind == GroupingSetKind::GROUPING_SET_SETS);
                if n1_is_sets {
                    match n2 {
                        Flattened::Nil => {}
                        Flattened::One(node) => result_set.lappend(mcx, node)?,
                        Flattened::Many(nodes) => result_set.concat(mcx, &nodes)?,
                    }
                } else {
                    match n2 {
                        Flattened::One(node) => result_set.lappend(mcx, node)?,
                        // C lappends the RowExpr arm's List* as one cell.
                        Flattened::Many(nodes) => {
                            result_set.lappend(mcx, Node::mk_list(mcx, nodes)?)?
                        }
                        Flattened::Nil => unreachable!(
                            "flatten_grouping_sets: NIL cell in a grouping list"
                        ),
                    }
                }
            }
            // At top level keep the node; a simply-nested (non-SETS) set also
            // stays one node, while nested SETS concat into the outer list.
            if toplevel || gset.kind != GroupingSetKind::GROUPING_SET_SETS {
                Ok(Flattened::One(Node::mk_grouping_set(
                    mcx,
                    gset.kind,
                    result_set,
                    gset.location,
                )?))
            } else {
                Ok(Flattened::Many(result_set))
            }
        }
        _ => Ok(Flattened::One(expr)),
    }
}

// The C T_List arm of flatten_grouping_sets: the grouping list itself.
fn flatten_grouping_sets_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    toplevel: bool,
    has_grouping_sets: Option<&mut bool>,
) -> PgResult<NodeList<'mcx>> {
    let mut result = NodeList::nil();
    let mut flag = has_grouping_sets;
    for l in list {
        match flatten_grouping_sets(mcx, l, toplevel, flag.as_deref_mut())? {
            Flattened::Nil => {}
            Flattened::One(node) => result.lappend(mcx, node)?,
            Flattened::Many(nodes) => result.concat(mcx, &nodes)?,
        }
    }
    Ok(result)
}

/// C `transformGroupClause`: flat SortGroupClause list out, grouping-set tree
/// through the `grouping_sets` out-param (SIMPLE content = Integer refs).
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
    let mut result = NodeList::nil();
    let mut gsets = NodeList::nil();
    let mut has_grouping_sets = false;
    let mut seen_local: mcx::PgVec<'_, Index> = mcx::PgVec::new_in(mcx);

    let mut flat_grouplist =
        flatten_grouping_sets_list(mcx, grouplist, true, Some(&mut has_grouping_sets))?;

    // Only redundant empty grouping sets were elided: restore the canonical
    // form GROUP BY ().
    if flat_grouplist.is_nil() && has_grouping_sets {
        flat_grouplist = NodeList::make1(
            mcx,
            Node::mk_grouping_set(
                mcx,
                GroupingSetKind::GROUPING_SET_EMPTY,
                NodeList::nil(),
                expr_location_list(grouplist),
            )?,
        )?;
    }

    for gexpr in &flat_grouplist {
        if let Some(gset) = gexpr.as_grouping_set() {
            match gset.kind {
                GroupingSetKind::GROUPING_SET_EMPTY => gsets.lappend(mcx, gexpr)?,
                GroupingSetKind::GROUPING_SET_SIMPLE => {
                    unreachable!("SIMPLE grouping set ahead of transformGroupingSet")
                }
                GroupingSetKind::GROUPING_SET_SETS
                | GroupingSetKind::GROUPING_SET_CUBE
                | GroupingSetKind::GROUPING_SET_ROLLUP => {
                    let tg = transformGroupingSet(
                        &mut result,
                        mcx,
                        pstate,
                        gset,
                        targetlist,
                        sort_clause,
                        expr_kind,
                        use_sql99,
                    )?;
                    gsets.lappend(mcx, tg)?;
                }
            }
        } else {
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
                true,
            )?;
            if r#ref > 0 {
                seen_local.push(r#ref);
                if has_grouping_sets {
                    let content =
                        NodeList::make1(mcx, Node::mk_integer(mcx, r#ref as i32)?)?;
                    gsets.lappend(
                        mcx,
                        Node::mk_grouping_set(
                            mcx,
                            GroupingSetKind::GROUPING_SET_SIMPLE,
                            content,
                            expr_location(gexpr),
                        )?,
                    )?;
                }
            }
        }
    }

    *grouping_sets = gsets;
    Ok(result)
}

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
    toplevel: bool,
) -> PgResult<Index> {
    let tle_node = if use_sql99 {
        findTargetlistEntrySQL99(mcx, pstate, gexpr, targetlist, expr_kind)?
    } else {
        findTargetlistEntrySQL92(mcx, pstate, gexpr, targetlist, expr_kind)?
    };
    let tle = tle_node.as_target_entry().unwrap();

    let mut found = false;
    if tle.ressortgroupref > 0 {
        // GROUP BY x, x: local duplicates drop out.  (Duplicates in grouping
        // sets can affect the number of returned rows, so the caller passes a
        // per-clause seen_local.)
        if seen_local.contains(&tle.ressortgroupref) {
            return Ok(0);
        }
        found = targetIsInSortList(tle, InvalidOid, flatresult)?;
        if !found {
            // A matching ORDER BY item donates its operator info (C copies
            // the SortGroupClause node); inside a grouping set the requested
            // ordering is forced to NULLS LAST.
            for sc_node in sort_clause {
                let sc = sc_node.as_sort_group_clause().expect("sortClause cell");
                if sc.tleSortGroupRef == tle.ressortgroupref {
                    let mut grpc = *sc;
                    if !toplevel {
                        grpc.nulls_first = false;
                    }
                    flatresult.lappend(mcx, Node::mk(mcx, grpc)?)?;
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

// C transformGroupClauseList: one grouping-set sublist, local dup elimination;
// returns the sublist's ressortgrouprefs as Integer nodes (SIMPLE content).
#[allow(clippy::too_many_arguments)]
fn transformGroupClauseList<'mcx>(
    flatresult: &mut NodeList<'mcx>,
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    list: &NodeList<'mcx>,
    targetlist: &mut NodeList<'mcx>,
    sort_clause: &NodeList<'mcx>,
    expr_kind: ParseExprKind,
    use_sql99: bool,
) -> PgResult<NodeList<'mcx>> {
    let mut seen_local: mcx::PgVec<'_, Index> = mcx::PgVec::new_in(mcx);
    let mut result = NodeList::nil();
    for gexpr in list {
        let r#ref = transformGroupClauseExpr(
            flatresult,
            &seen_local,
            mcx,
            pstate,
            gexpr,
            targetlist,
            sort_clause,
            expr_kind,
            use_sql99,
            false,
        )?;
        if r#ref > 0 {
            seen_local.push(r#ref);
            result.lappend(mcx, Node::mk_integer(mcx, r#ref as i32)?)?;
        }
    }
    Ok(result)
}

// C transformGroupingSet: SETS-in-SETS already flattened; SIMPLE children now
// carry ressortgroupref Integer lists rather than expressions.
#[allow(clippy::too_many_arguments)]
fn transformGroupingSet<'mcx>(
    flatresult: &mut NodeList<'mcx>,
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    gset: &GroupingSet<'mcx>,
    targetlist: &mut NodeList<'mcx>,
    sort_clause: &NodeList<'mcx>,
    expr_kind: ParseExprKind,
    use_sql99: bool,
) -> PgResult<Node<'mcx>> {
    let mut content = NodeList::nil();
    for n in &gset.content {
        match n.node_tag() {
            NodeTag::T_List => {
                let sublist = n.as_list().unwrap();
                let l = transformGroupClauseList(
                    flatresult, mcx, pstate, sublist, targetlist, sort_clause, expr_kind,
                    use_sql99,
                )?;
                content.lappend(
                    mcx,
                    Node::mk_grouping_set(
                        mcx,
                        GroupingSetKind::GROUPING_SET_SIMPLE,
                        l,
                        expr_location_list(sublist),
                    )?,
                )?;
            }
            NodeTag::T_GroupingSet => {
                let gset2 = n.as_grouping_set().unwrap();
                let tg = transformGroupingSet(
                    flatresult, mcx, pstate, gset2, targetlist, sort_clause, expr_kind, use_sql99,
                )?;
                content.lappend(mcx, tg)?;
            }
            _ => {
                let r#ref = transformGroupClauseExpr(
                    flatresult,
                    &[],
                    mcx,
                    pstate,
                    n,
                    targetlist,
                    sort_clause,
                    expr_kind,
                    use_sql99,
                    false,
                )?;
                content.lappend(
                    mcx,
                    Node::mk_grouping_set(
                        mcx,
                        GroupingSetKind::GROUPING_SET_SIMPLE,
                        NodeList::make1(mcx, Node::mk_integer(mcx, r#ref as i32)?)?,
                        expr_location(n),
                    )?,
                )?;
            }
        }
    }

    // Arbitrarily cap the size of CUBE, which has exponential growth.
    if gset.kind == GroupingSetKind::GROUPING_SET_CUBE && content.len() > 12 {
        return Err(cube_limit_error(pstate, gset.location));
    }

    Node::mk_grouping_set(mcx, gset.kind, content, gset.location)
}

#[cold]
#[inline(never)]
fn cube_limit_error(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg("CUBE is limited to 12 elements")
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_clause.c", 0, "transformGroupingSet")),
    )
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

/// C `transformDistinctOnClause`: ORDER BY items matching a DISTINCT ON
/// expression donate their sort semantics; DISTINCT ON must stay a prefix of
/// ORDER BY or it is an error.
pub fn transformDistinctOnClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    distinctlist: &NodeList<'mcx>,
    targetlist: &mut NodeList<'mcx>,
    sort_clause: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mut result = NodeList::nil();
    let mut sortgrouprefs: mcx::PgVec<'mcx, Index> = mcx::PgVec::new_in(mcx);
    for dexpr in distinctlist {
        let tle = findTargetlistEntrySQL92(
            mcx,
            pstate,
            dexpr,
            targetlist,
            ParseExprKind::EXPR_KIND_DISTINCT_ON,
        )?;
        sortgrouprefs.push(assignSortGroupRef(tle, targetlist));
    }

    let mut skipped_sortitem = false;
    for sc_node in sort_clause {
        let scl = sc_node.as_sort_group_clause().expect("sortClause cell");
        if sortgrouprefs.contains(&scl.tleSortGroupRef) {
            if skipped_sortitem {
                return Err(distinct_on_orderby_mismatch(
                    pstate,
                    get_matching_location(scl.tleSortGroupRef, &sortgrouprefs, distinctlist),
                ));
            }
            result.lappend(mcx, Node::mk(mcx, *scl)?)?;
        } else {
            skipped_sortitem = true;
        }
    }

    for (i, dexpr) in distinctlist.iter().enumerate() {
        let sortgroupref = sortgrouprefs[i];
        let tle_node = targetlist
            .iter()
            .find(|n| n.as_target_entry().expect("tlist cell").ressortgroupref == sortgroupref)
            .expect("DISTINCT ON expression was added to the targetlist above");
        if targetIsInSortList(tle_node.as_target_entry().unwrap(), InvalidOid, &result)? {
            continue;
        }
        if skipped_sortitem {
            return Err(distinct_on_orderby_mismatch(pstate, expr_location(dexpr)));
        }
        addTargetToGroupList(mcx, pstate, tle_node, &mut result, targetlist, expr_location(dexpr))?;
    }

    assert!(!result.is_nil(), "grammar forbids an empty DISTINCT ON list");
    Ok(result)
}

fn get_matching_location(
    sortgroupref: Index,
    sortgrouprefs: &[Index],
    exprs: &NodeList<'_>,
) -> ParseLoc {
    for (i, expr) in exprs.iter().enumerate() {
        if sortgrouprefs[i] == sortgroupref {
            return expr_location(expr);
        }
    }
    unreachable!("get_matching_location: no matching sortgroupref");
}

fn findWindowClause<'a, 'mcx>(
    wclist: &'a NodeList<'mcx>,
    name: &str,
) -> Option<&'mcx WindowClause<'mcx>> {
    let _ = wclist;
    for wc_node in wclist {
        let wc = wc_node.as_window_clause().expect("window clause list cell");
        if wc.name == Some(name) {
            return Some(wc);
        }
    }
    None
}

pub fn transformWindowDefinitions<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    windowdefs: &NodeList<'mcx>,
    targetlist: &mut NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mut result = NodeList::nil();
    let mut winref: Index = 0;

    for windef_node in windowdefs {
        let windef = windef_node.as_window_def().expect("windowdefs cell");
        winref += 1;

        if let Some(name) = windef.name {
            if findWindowClause(&result, name).is_some() {
                return Err(window_error(
                    pstate,
                    format!("window \"{name}\" is already defined"),
                    types_error::ERRCODE_WINDOWING_ERROR,
                    windef.location,
                ));
            }
        }

        let refwc = match windef.refname {
            Some(refname) => match findWindowClause(&result, refname) {
                Some(wc) => Some(wc),
                None => {
                    return Err(window_error(
                        pstate,
                        format!("window \"{refname}\" does not exist"),
                        types_error::ERRCODE_UNDEFINED_OBJECT,
                        windef.location,
                    ));
                }
            },
            None => None,
        };

        let orderClause = transformSortClause(
            mcx,
            pstate,
            &windef.orderClause,
            targetlist,
            ParseExprKind::EXPR_KIND_WINDOW_ORDER,
            true,
        )?;
        let mut grouping_sets = NodeList::nil();
        let partitionClause = transformGroupClause(
            mcx,
            pstate,
            &windef.partitionClause,
            &mut grouping_sets,
            targetlist,
            &orderClause,
            ParseExprKind::EXPR_KIND_WINDOW_PARTITION,
            true,
        )?;

        let mut wc = WindowClause {
            name: windef.name,
            refname: windef.refname,
            ..WindowClause::default()
        };

        // SQL:2008 7.11: a ref copies the previous partition clause (own one
        // forbidden), may add ORDER BY only if the previous had none, and the
        // previous must be frameless.
        if let Some(refwc) = refwc {
            if !partitionClause.is_nil() {
                return Err(window_error(
                    pstate,
                    format!(
                        "cannot override PARTITION BY clause of window \"{}\"",
                        windef.refname.unwrap()
                    ),
                    types_error::ERRCODE_WINDOWING_ERROR,
                    windef.location,
                ));
            }
            wc.partitionClause = copy_sort_group_list(mcx, &refwc.partitionClause)?;
        } else {
            wc.partitionClause = partitionClause;
        }
        if let Some(refwc) = refwc {
            if !orderClause.is_nil() && !refwc.orderClause.is_nil() {
                return Err(window_error(
                    pstate,
                    format!(
                        "cannot override ORDER BY clause of window \"{}\"",
                        windef.refname.unwrap()
                    ),
                    types_error::ERRCODE_WINDOWING_ERROR,
                    windef.location,
                ));
            }
            if !orderClause.is_nil() {
                wc.orderClause = orderClause;
                wc.copiedOrder = false;
            } else {
                wc.orderClause = copy_sort_group_list(mcx, &refwc.orderClause)?;
                wc.copiedOrder = true;
            }
        } else {
            wc.orderClause = orderClause;
            wc.copiedOrder = false;
        }
        if let Some(refwc) = refwc {
            if refwc.frameOptions != FRAMEOPTION_DEFAULTS {
                // C picks between two messages (same text, hint differs);
                // both frame-ful shapes are unreachable while the grammar's
                // explicit-frame rules panic, so the non-hint arm suffices.
                if windef.name.is_some()
                    || !wc.orderClause.is_nil()
                    || windef.frameOptions != FRAMEOPTION_DEFAULTS
                {
                    return Err(window_error(
                        pstate,
                        format!(
                            "cannot copy window \"{}\" because it has a frame clause",
                            windef.refname.unwrap()
                        ),
                        types_error::ERRCODE_WINDOWING_ERROR,
                        windef.location,
                    ));
                }
                return Err(window_error_hint(
                    pstate,
                    format!(
                        "cannot copy window \"{}\" because it has a frame clause",
                        windef.refname.unwrap()
                    ),
                    "Omit the parentheses in this OVER clause.",
                    windef.location,
                ));
            }
        }
        wc.frameOptions = windef.frameOptions;

        let mut rangeopfamily = InvalidOid;
        let mut rangeopcintype = InvalidOid;
        if (wc.frameOptions & FRAMEOPTION_RANGE) != 0
            && (wc.frameOptions & (FRAMEOPTION_START_OFFSET | FRAMEOPTION_END_OFFSET)) != 0
        {
            if wc.orderClause.len() != 1 {
                return Err(window_error(
                    pstate,
                    "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY \
                     column"
                        .into(),
                    types_error::ERRCODE_WINDOWING_ERROR,
                    windef.location,
                ));
            }
            let sortcl = wc
                .orderClause
                .first()
                .unwrap()
                .as_sort_group_clause()
                .expect("SortGroupClause");
            let sortkey = get_sortgroupclause_expr(sortcl.tleSortGroupRef, targetlist);
            let Some((opfamily, opcintype, _cmptype)) =
                lsyscache::get_ordering_op_properties(sortcl.sortop)?
            else {
                panic!("operator {} is not a valid ordering operator", sortcl.sortop);
            };
            rangeopfamily = opfamily;
            rangeopcintype = opcintype;
            wc.inRangeColl = expr_collation(sortkey);
            wc.inRangeAsc = !sortcl.reverse_sort;
            wc.inRangeNullsFirst = sortcl.nulls_first;
        }

        if (wc.frameOptions & types_nodes::rawnodes::FRAMEOPTION_GROUPS) != 0
            && wc.orderClause.is_nil()
        {
            return Err(window_error(
                pstate,
                "GROUPS mode requires an ORDER BY clause".into(),
                types_error::ERRCODE_WINDOWING_ERROR,
                windef.location,
            ));
        }

        wc.startOffset = transformFrameOffset(
            mcx,
            pstate,
            wc.frameOptions,
            rangeopfamily,
            rangeopcintype,
            &mut wc.startInRangeFunc,
            windef.startOffset,
        )?;
        wc.endOffset = transformFrameOffset(
            mcx,
            pstate,
            wc.frameOptions,
            rangeopfamily,
            rangeopcintype,
            &mut wc.endInRangeFunc,
            windef.endOffset,
        )?;
        wc.winref = winref;

        result.lappend(mcx, Node::mk(mcx, wc)?)?;
    }
    Ok(result)
}

// copyObject on a SortGroupClause list (Copy struct; fresh cells).
fn copy_sort_group_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mut out = NodeList::nil();
    for sc_node in list {
        let sc = sc_node.as_sort_group_clause().expect("SortGroupClause cell");
        out.lappend(mcx, Node::mk(mcx, *sc)?)?;
    }
    Ok(out)
}

#[cold]
#[inline(never)]
fn window_error(
    pstate: &ParseState<'_, '_>,
    msg: String,
    code: types_error::SqlState,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(code)
            .errmsg(msg)
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "transformWindowDefinitions",
            )),
    )
}

#[cold]
#[inline(never)]
fn window_error_hint(
    pstate: &ParseState<'_, '_>,
    msg: String,
    hint: &'static str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_WINDOWING_ERROR)
            .errmsg(msg)
            .errhint(hint)
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "transformWindowDefinitions",
            )),
    )
}

// get_sortgroupclause_expr (tlist.c) over the transform-time targetlist.
fn get_sortgroupclause_expr<'mcx>(
    sort_group_ref: Index,
    targetlist: &NodeList<'mcx>,
) -> Node<'mcx> {
    for n in targetlist {
        let tle = n.as_target_entry().expect("tlist holds TargetEntry");
        if tle.ressortgroupref == sort_group_ref {
            return tle.expr;
        }
    }
    panic!("ORDER/GROUP BY expression not found in targetlist");
}

// BTINRANGE_PROC (nbtree.h); single home is types_nbtree, re-stated here to
// keep the parser off the btree types crate.
const BTINRANGE_PROC: i16 = 3;

fn transformFrameOffset<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    frame_options: i32,
    rangeopfamily: Oid,
    rangeopcintype: Oid,
    in_range_func: &mut Oid,
    clause: Option<Node<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    *in_range_func = InvalidOid;
    let Some(clause) = clause else {
        return Ok(None);
    };

    let node;
    let construct_name;
    if frame_options & FRAMEOPTION_ROWS != 0 {
        construct_name = "ROWS";
        let n = transformExpr(mcx, pstate, clause, ParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS)?;
        node = coerce::coerce_to_specific_type(
            mcx,
            pstate,
            n,
            expr_type(n),
            expr_location(n),
            INT8OID,
            construct_name,
        )?;
    } else if frame_options & FRAMEOPTION_RANGE != 0 {
        construct_name = "RANGE";
        let n = transformExpr(mcx, pstate, clause, ParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE)?;
        let node_type = expr_type(n);
        let preferred_type = if node_type != UNKNOWNOID { node_type } else { rangeopcintype };

        let mut nfuncs = 0;
        let mut nmatches = 0;
        let mut selected_type = InvalidOid;
        let mut selected_func = InvalidOid;
        let procs = syscache_seams::lookup_pg_amproc_members::call(
            mcx,
            rangeopfamily,
            rangeopcintype,
        )?;
        for proc in procs.iter() {
            if proc.amprocnum != BTINRANGE_PROC {
                continue;
            }
            nfuncs += 1;
            if !coerce::can_coerce_type(
                &[node_type],
                &[proc.amprocrighttype],
                coerce::CoercionContext::COERCION_IMPLICIT,
            )? {
                continue;
            }
            nmatches += 1;
            if selected_type != preferred_type {
                selected_type = proc.amprocrighttype;
                selected_func = proc.amproc;
            }
        }

        if nfuncs == 0 {
            return Err(frame_offset_error(
                pstate,
                format!(
                    "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {}",
                    format_type::format_type_be(rangeopcintype)?
                ),
                None,
                expr_location(n),
            ));
        }
        if nmatches == 0 {
            return Err(frame_offset_error(
                pstate,
                format!(
                    "RANGE with offset PRECEDING/FOLLOWING is not supported for column type \
                     {} and offset type {}",
                    format_type::format_type_be(rangeopcintype)?,
                    format_type::format_type_be(node_type)?
                ),
                Some("Cast the offset value to an appropriate type."),
                expr_location(n),
            ));
        }
        if nmatches != 1 && selected_type != preferred_type {
            return Err(frame_offset_error(
                pstate,
                format!(
                    "RANGE with offset PRECEDING/FOLLOWING has multiple interpretations for \
                     column type {} and offset type {}",
                    format_type::format_type_be(rangeopcintype)?,
                    format_type::format_type_be(node_type)?
                ),
                Some("Cast the offset value to the exact intended type."),
                expr_location(n),
            ));
        }

        node = coerce::coerce_to_specific_type(
            mcx,
            pstate,
            n,
            expr_type(n),
            expr_location(n),
            selected_type,
            construct_name,
        )?;
        *in_range_func = selected_func;
    } else if frame_options & FRAMEOPTION_GROUPS != 0 {
        construct_name = "GROUPS";
        let n =
            transformExpr(mcx, pstate, clause, ParseExprKind::EXPR_KIND_WINDOW_FRAME_GROUPS)?;
        node = coerce::coerce_to_specific_type(
            mcx,
            pstate,
            n,
            expr_type(n),
            expr_location(n),
            INT8OID,
            construct_name,
        )?;
    } else {
        panic!("unrecognized frame_options {frame_options:#x} in transformFrameOffset");
    }

    checkExprIsVarFree(pstate, node, construct_name)?;
    Ok(Some(node))
}

#[cold]
#[inline(never)]
fn frame_offset_error(
    pstate: &ParseState<'_, '_>,
    msg: String,
    hint: Option<&'static str>,
    location: ParseLoc,
) -> Box<PgError> {
    let mut b = elog::ereport(ERROR)
        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(msg)
        .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()));
    if let Some(hint) = hint {
        b = b.errhint(hint);
    }
    Box::new(b.into_error().with_error_location(ErrorLocation::new(
        "parse_clause.c",
        0,
        "transformFrameOffset",
    )))
}

#[cold]
#[inline(never)]
fn window_in_group_by(
    pstate: &ParseState<'_, '_>,
    expr_kind: ParseExprKind,
    tle_expr: Node<'_>,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_WINDOWING_ERROR)
            .errmsg(format!(
                "window functions are not allowed in {}",
                ParseExprKindName(expr_kind)
            ))
            .errposition(parser_errposition(
                pstate,
                parse_agg::locate_windowfunc(tle_expr),
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
fn distinct_on_orderby_mismatch(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
                    .to_string(),
            )
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_clause.c",
                0,
                "transformDistinctOnClause",
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
