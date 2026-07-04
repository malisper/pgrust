//! prepjointree.c, simple-subquery slice: pull_up_subqueries over the full
//! jointree (FromExpr/JoinExpr) plus reduce_outer_joins with the LEFT->ANTI
//! reduction. Non-pullable subqueries stay as RTE_SUBQUERY for
//! set_subquery_pathlist (allpaths.rs); LATERAL is the remaining loud arm.

use mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::primnodes::{FromExpr, TargetEntry, Var};
use types_nodes::{Node, NodeTag};

// C recurses and mutates in place; here each pull-up rebuilds the jointree
// functionally (replace the RangeTblRef, substitute Vars in every qual), so
// the loop re-scans until no pullable subquery reference remains.
pub fn pull_up_subqueries<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut kept: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
    loop {
        let jt = parse.jointree.expect("jointree is a FromExpr");
        let mut target: Option<(i32, Option<Node<'mcx>>)> = None;
        for child in &jt.fromlist {
            find_pullable_subquery(parse, child, None, &mut target, &kept);
            if target.is_some() {
                break;
            }
        }
        let Some((rti, lowest_outer_join)) = target else { return Ok(()) };
        let rte_node = parse.rtable.nth(rti as usize - 1);
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        if is_simple_subquery(mcx, rte, lowest_outer_join)? {
            if !pull_up_simple_subquery(run, parse, rti, rte_node, lowest_outer_join, None)? {
                kept.push(rti);
            }
            continue;
        }
        if is_simple_union_all(rte.subquery.expect("RTE_SUBQUERY has a subquery")) {
            pull_up_simple_union_all(run, parse, rti, rte_node)?;
        }
        kept.push(rti);
    }
}

// flatten_simple_union_all (prepjointree.c): a top-level all-UNION-ALL setop
// query becomes an appendrel over its (copied) leftmost leaf RTE. The setop
// tree is only read here, never scribbled: the leftmost RangeTblRef's redirect
// to the child copy is carried as a remap into pull_up_union_leaf_queries.
pub fn flatten_simple_union_all<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let topop_node = parse.setOperations.expect("flatten_simple_union_all without setOperations");
    let topop = topop_node.as_set_operation_stmt().expect("setOperations is a SetOperationStmt");
    if run.root.hasRecursion {
        return Ok(());
    }
    if !is_simple_union_all_recurse(parse, topop_node, &topop.colTypes) {
        return Ok(());
    }
    let mut leftmost = topop.larg.expect("setop larg");
    while let Some(op) = leftmost.as_set_operation_stmt() {
        leftmost = op.larg.expect("setop larg");
    }
    let leftmost_rti = leftmost.as_range_tbl_ref().expect("setop leaf is a RangeTblRef").rtindex;
    let leftmost_rte_node = parse.rtable.nth(leftmost_rti as usize - 1);
    let leftmost_rte = leftmost_rte_node.as_range_tbl_entry().expect("rtable cell");
    debug_assert!(leftmost_rte.rtekind == RTEKind::RTE_SUBQUERY);

    let child = rte_copy_with_perminfoindex(mcx, leftmost_rte, leftmost_rte.perminfoindex)?;
    parse.rtable.lappend(mcx, child)?;
    let child_rti = parse.rtable.len() as i32;

    // SAFETY: rtable cell of this planner-owned Query; exclusive fixup.
    unsafe { leftmost_rte_node.with_mut::<RangeTblEntry, _>(|r| r.inh = true) };

    let jt = parse.jointree.expect("jointree is a FromExpr");
    debug_assert!(jt.fromlist.is_nil() && jt.quals.is_none());
    let mut fromlist = NodeList::nil();
    fromlist.lappend(mcx, Node::mk_range_tbl_ref(mcx, leftmost_rti)?)?;
    parse.jointree = Some(mcx::alloc_leak_in(mcx, FromExpr { fromlist, quals: None })?);
    parse.setOperations = None;

    let setop_tlist = parse.targetList.clone_in(mcx)?;
    pull_up_union_leaf_queries(
        run,
        parse,
        topop_node,
        leftmost_rti,
        &setop_tlist,
        0,
        Some((leftmost_rti, child_rti)),
    )
}

// pull_up_simple_union_all (prepjointree.c). C copyObject's the subquery's
// rtable and IncrementVarSublevelsUp_rtable's it in place; here a leaf is
// deep-copied only when it carries uplevel vars to adjust (the out/read copy
// panics on unported write arms, and an uncorrelated leaf needs no change).
fn pull_up_simple_union_all<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    varno: i32,
    rte_node: Node<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
    let subquery = rte.subquery.expect("RTE_SUBQUERY has a subquery");
    let rtoffset = parse.rtable.len() as i32;

    let perm_offset = parse.rteperminfos.len() as u32;
    for srte_node in &subquery.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        debug_assert!(srte.rtekind == RTEKind::RTE_SUBQUERY);
        let leaf = srte.subquery.expect("setop leaf RTE has a subquery");
        let adjusted: Option<&'mcx Query<'mcx>> = if query_has_uplevel_vars(leaf)? {
            let deep = rewrite_manip::copy_query_node(mcx, leaf)?;
            rewrite_manip::IncrementVarSublevelsUp(deep, -1, 1)?;
            Some(deep.as_query().expect("Query round trip"))
        } else {
            None
        };
        let new_index = if srte.perminfoindex > 0 {
            srte.perminfoindex + perm_offset
        } else {
            srte.perminfoindex
        };
        let copy = rte_copy_with_perminfoindex(mcx, srte, new_index)?;
        let lateral = rte.lateral;
        // SAFETY: exclusive pre-seal fixup of the fresh copy.
        unsafe {
            copy.with_mut::<RangeTblEntry, _>(|r| {
                if let Some(q) = adjusted {
                    r.subquery = Some(q);
                }
                if lateral {
                    r.lateral = true;
                }
            })
        };
        parse.rtable.lappend(mcx, copy)?;
    }
    for p in &subquery.rteperminfos {
        parse.rteperminfos.lappend(mcx, p)?;
    }

    pull_up_union_leaf_queries(
        run,
        parse,
        subquery.setOperations.expect("union subquery has setOperations"),
        varno,
        &subquery.targetList,
        rtoffset,
        None,
    )?;

    // SAFETY: rtable cell of this planner-owned Query; exclusive fixup.
    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.inh = true) };
    Ok(())
}

// pull_up_union_leaf_queries (prepjointree.c). leftmost_remap carries
// flatten_simple_union_all's redirect of the leftmost leaf to its child copy
// (C rewrites the RangeTblRef in place; the tree may be plancache-shared).
fn pull_up_union_leaf_queries<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    setop: Node<'mcx>,
    parent_rtindex: i32,
    setop_tlist: &NodeList<'mcx>,
    child_rtoffset: i32,
    leftmost_remap: Option<(i32, i32)>,
) -> PgResult<()> {
    match setop.node_tag() {
        NodeTag::T_RangeTblRef => {
            let mut idx = setop.as_range_tbl_ref().expect("RangeTblRef").rtindex;
            if let Some((from, to)) = leftmost_remap {
                if idx == from {
                    idx = to;
                }
            }
            let child_rtindex = child_rtoffset + idx;
            let mut appinfo = types_pathnodes::AppendRelInfo::new(run.mcx);
            appinfo.parent_relid = parent_rtindex as u32;
            appinfo.child_relid = child_rtindex as u32;
            make_setop_translation_list(run, setop_tlist, child_rtindex, &mut appinfo)?;
            run.root.append_rel_list.push(appinfo);
            let ai = run.root.append_rel_list.len() - 1;

            let rte_node = parse.rtable.nth(child_rtindex as usize - 1);
            let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
            debug_assert!(rte.rtekind == RTEKind::RTE_SUBQUERY);
            let sub = rte.subquery.expect("RTE_SUBQUERY has a subquery");
            if is_simple_subquery(run.mcx, rte, None)? && is_safe_append_member(sub) {
                pull_up_simple_subquery(run, parse, child_rtindex, rte_node, None, Some(ai))?;
            } else if is_simple_union_all(sub) {
                pull_up_simple_union_all(run, parse, child_rtindex, rte_node)?;
            }
            Ok(())
        }
        NodeTag::T_SetOperationStmt => {
            let op = setop.as_set_operation_stmt().expect("SetOperationStmt");
            pull_up_union_leaf_queries(
                run,
                parse,
                op.larg.expect("setop larg"),
                parent_rtindex,
                setop_tlist,
                child_rtoffset,
                leftmost_remap,
            )?;
            pull_up_union_leaf_queries(
                run,
                parse,
                op.rarg.expect("setop rarg"),
                parent_rtindex,
                setop_tlist,
                child_rtoffset,
                leftmost_remap,
            )
        }
        other => panic!("pull_up_union_leaf_queries (prepjointree.c): {other:?}"),
    }
}

// make_setop_translation_list (prepjointree.c).
fn make_setop_translation_list<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
    newvarno: i32,
    appinfo: &mut types_pathnodes::AppendRelInfo<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    appinfo.num_child_cols = tlist.len() as i32;
    appinfo.parent_colnos = mcx::vec_from_elem_in(mcx, 0i16, tlist.len());
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("tlist cell");
        if tle.resjunk {
            continue;
        }
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(tle.expr);
        let varcollid = crate::pathkeys::expr_collation(tle.expr);
        let var = Node::mk_var(mcx, newvarno, tle.resno, vartype, vartypmod, varcollid, 0)?;
        appinfo.translated_vars.push(run.intern_expr(var));
        appinfo.parent_colnos[tle.resno as usize - 1] = tle.resno;
    }
    Ok(())
}

// Any Var (or CTE reference) above the query's own level — gates the deep
// copy that stands in for C's unconditional copyObject before
// IncrementVarSublevelsUp.
pub(crate) fn query_has_uplevel_vars<'mcx>(q: &'mcx Query<'mcx>) -> PgResult<bool> {
    use nodes_core::NodeWalker;
    struct W {
        depth: u32,
        found: bool,
    }
    impl<'mcx> NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Var => {
                    if node.as_var().expect("Var").varlevelsup > self.depth {
                        self.found = true;
                        return Ok(true);
                    }
                    Ok(false)
                }
                NodeTag::T_RangeTblEntry => {
                    let rte = node.as_range_tbl_entry().expect("RangeTblEntry");
                    if rte.rtekind == RTEKind::RTE_CTE && rte.ctelevelsup > self.depth {
                        self.found = true;
                        return Ok(true);
                    }
                    Ok(false)
                }
                NodeTag::T_Query => {
                    self.depth += 1;
                    let r = nodes_core::query_tree_walker(
                        node.as_query().expect("Query"),
                        self,
                        nodes_core::QTW_EXAMINE_RTES_BEFORE,
                    )?;
                    self.depth -= 1;
                    Ok(r)
                }
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.depth += 1;
            let r =
                nodes_core::query_tree_walker(q, self, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
            self.depth -= 1;
            Ok(r)
        }
    }
    let mut w = W { depth: 0, found: false };
    nodes_core::query_tree_walker(q, &mut w, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
    Ok(w.found)
}

// is_simple_union_all + is_simple_union_all_recurse (prepjointree.c).
fn is_simple_union_all(subquery: &Query<'_>) -> bool {
    let Some(topop_node) = subquery.setOperations else { return false };
    let Some(topop) = topop_node.as_set_operation_stmt() else { return false };
    if !subquery.sortClause.is_nil()
        || subquery.limitOffset.is_some()
        || subquery.limitCount.is_some()
        || !subquery.rowMarks.is_nil()
        || !subquery.cteList.is_nil()
    {
        return false;
    }
    is_simple_union_all_recurse(subquery, topop_node, &topop.colTypes)
}

fn is_simple_union_all_recurse<'mcx>(
    set_op_query: &Query<'mcx>,
    setop: Node<'mcx>,
    col_types: &types_nodes::list::OidList<'mcx>,
) -> bool {
    match setop.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rtr = setop.as_range_tbl_ref().expect("RangeTblRef");
            let rte = set_op_query
                .rtable
                .nth(rtr.rtindex as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable cell");
            let sub = rte.subquery.expect("setop leaf RTE has a subquery");
            // tlist_same_datatypes (tlist.c), junkOK=true.
            let mut ct = col_types.iter();
            for tle_node in &sub.targetList {
                let tle = tle_node.as_target_entry().expect("tlist cell");
                if tle.resjunk {
                    continue;
                }
                let Some(t) = ct.next() else { return false };
                if crate::costsize::expr_type_typmod(tle.expr).0 != t {
                    return false;
                }
            }
            ct.next().is_none()
        }
        NodeTag::T_SetOperationStmt => {
            let op = setop.as_set_operation_stmt().expect("SetOperationStmt");
            if op.op != types_nodes::parsenodes::SetOperation::SETOP_UNION || !op.all {
                return false;
            }
            is_simple_union_all_recurse(set_op_query, op.larg.expect("setop larg"), col_types)
                && is_simple_union_all_recurse(
                    set_op_query,
                    op.rarg.expect("setop rarg"),
                    col_types,
                )
        }
        other => panic!("is_simple_union_all_recurse (prepjointree.c): {other:?}"),
    }
}

// is_safe_append_member (prepjointree.c).
fn is_safe_append_member(subquery: &Query<'_>) -> bool {
    let jt = subquery.jointree.expect("jointree is a FromExpr");
    if jt.fromlist.is_nil() && jt.quals.is_none() {
        return true;
    }
    if jt.quals.is_some() || jt.fromlist.len() != 1 {
        return false;
    }
    let mut node = jt.fromlist.nth(0);
    while let Some(f) = node.as_from_expr() {
        if f.quals.is_some() || f.fromlist.len() != 1 {
            return false;
        }
        node = f.fromlist.nth(0);
    }
    node.node_tag() == NodeTag::T_RangeTblRef
}

fn find_pullable_subquery<'mcx>(
    parse: &Query<'mcx>,
    node: Node<'mcx>,
    lowest_outer_join: Option<Node<'mcx>>,
    target: &mut Option<(i32, Option<Node<'mcx>>)>,
    kept: &[i32],
) {
    if target.is_some() {
        return;
    }
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rti = node.as_range_tbl_ref().expect("RangeTblRef").rtindex;
            let rte = parse.rtable.nth(rti as usize - 1).as_range_tbl_entry().expect("rtable cell");
            if rte.rtekind == RTEKind::RTE_SUBQUERY && !kept.contains(&rti) {
                *target = Some((rti, lowest_outer_join));
            }
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            for child in &f.fromlist {
                find_pullable_subquery(parse, child, lowest_outer_join, target, kept);
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let loj = if j.jointype == types_nodes::JoinType::JOIN_INNER {
                lowest_outer_join
            } else {
                Some(node)
            };
            find_pullable_subquery(parse, j.larg, loj, target, kept);
            find_pullable_subquery(parse, j.rarg, loj, target, kept);
        }
        other => panic!(
            "pull_up_subqueries_recurse (prepjointree.c): {other:?} jointree arm; \
             M2 join lane"
        ),
    }
}

// get_relids_in_jointree (prepjointree.c), include_outer_joins=true,
// include_inner_joins=true.
fn get_relids_in_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    out: &mut types_nodes::Bitmapset<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            out.add_member(mcx, node.as_range_tbl_ref().unwrap().rtindex)?;
        }
        NodeTag::T_FromExpr => {
            for child in &node.as_from_expr().unwrap().fromlist {
                get_relids_in_jointree(mcx, child, out)?;
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            get_relids_in_jointree(mcx, j.larg, out)?;
            get_relids_in_jointree(mcx, j.rarg, out)?;
            if j.rtindex != 0 {
                out.add_member(mcx, j.rtindex)?;
            }
        }
        other => panic!("get_relids_in_jointree (prepjointree.c): {other:?}"),
    }
    Ok(())
}

// jointree_contains_lateral_outer_refs (prepjointree.c).
fn jointree_contains_lateral_outer_refs<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    restricted: bool,
    safe_upper_varnos: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<bool> {
    let quals_unsafe = |quals: Option<Node<'mcx>>| -> PgResult<bool> {
        let Some(q) = quals else { return Ok(false) };
        Ok(!vars::pull_varnos_of_level(mcx, q, 1)?.is_subset(safe_upper_varnos))
    };
    match node.node_tag() {
        NodeTag::T_RangeTblRef => Ok(false),
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            for child in &f.fromlist {
                if jointree_contains_lateral_outer_refs(mcx, child, restricted, safe_upper_varnos)?
                {
                    return Ok(true);
                }
            }
            Ok(restricted && quals_unsafe(f.quals)?)
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let empty = types_nodes::Bitmapset::empty();
            let (restricted, safe) = if j.jointype != types_nodes::JoinType::JOIN_INNER {
                (true, &empty)
            } else {
                (restricted, safe_upper_varnos)
            };
            if jointree_contains_lateral_outer_refs(mcx, j.larg, restricted, safe)? {
                return Ok(true);
            }
            if jointree_contains_lateral_outer_refs(mcx, j.rarg, restricted, safe)? {
                return Ok(true);
            }
            let quals_unsafe = |quals: Option<Node<'mcx>>| -> PgResult<bool> {
                let Some(q) = quals else { return Ok(false) };
                Ok(!vars::pull_varnos_of_level(mcx, q, 1)?.is_subset(safe))
            };
            Ok(restricted && quals_unsafe(j.quals)?)
        }
        other => panic!("jointree_contains_lateral_outer_refs (prepjointree.c): {other:?}"),
    }
}

// The CombineRangeTables perminfoindex fixup target: a struct-level copy of
// the RTE (C's copyObject; sub-nodes stay shared) so the sublink's stored
// sub-Query — shared with the plancache — is never scribbled on. A replan of
// a cached query would otherwise re-offset the same RTE.
pub(crate) fn rte_copy_with_perminfoindex<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    perminfoindex: u32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        RangeTblEntry {
            alias: rte.alias,
            eref: rte.eref,
            rtekind: rte.rtekind,
            relid: rte.relid,
            inh: rte.inh,
            relkind: rte.relkind,
            rellockmode: rte.rellockmode,
            perminfoindex,
            tablesample: rte.tablesample,
            subquery: rte.subquery,
            security_barrier: rte.security_barrier,
            jointype: rte.jointype,
            joinmergedcols: rte.joinmergedcols,
            joinaliasvars: rte.joinaliasvars.clone_in(mcx)?,
            joinleftcols: rte.joinleftcols.clone_in(mcx)?,
            joinrightcols: rte.joinrightcols.clone_in(mcx)?,
            join_using_alias: rte.join_using_alias,
            functions: rte.functions.clone_in(mcx)?,
            funcordinality: rte.funcordinality,
            tablefunc: rte.tablefunc,
            values_lists: rte.values_lists.clone_in(mcx)?,
            ctename: rte.ctename,
            ctelevelsup: rte.ctelevelsup,
            self_reference: rte.self_reference,
            coltypes: rte.coltypes.clone_in(mcx)?,
            coltypmods: rte.coltypmods.clone_in(mcx)?,
            colcollations: rte.colcollations.clone_in(mcx)?,
            enrname: rte.enrname,
            enrtuples: rte.enrtuples,
            groupexprs: rte.groupexprs.clone_in(mcx)?,
            lateral: rte.lateral,
            inFromCl: rte.inFromCl,
            securityQuals: rte.securityQuals.clone_in(mcx)?,
        },
    )
}

// is_simple_subquery (prepjointree.c): false keeps the RTE for the
// SubqueryScan path (set_subquery_pathlist).
fn is_simple_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    lowest_outer_join: Option<Node<'mcx>>,
) -> PgResult<bool> {
    let sub = rte.subquery.expect("RTE_SUBQUERY has a subquery");
    is_simple_subquery_sub(mcx, sub, rte.lateral, rte.security_barrier, lowest_outer_join)
}

// C passes the (possibly hacked-on) subquery separately from the RTE for the
// post-pullup recheck.
fn is_simple_subquery_sub<'mcx>(
    mcx: Mcx<'mcx>,
    sub: &Query<'mcx>,
    lateral: bool,
    security_barrier: bool,
    lowest_outer_join: Option<Node<'mcx>>,
) -> PgResult<bool> {
    let blocked = if sub.setOperations.is_some() {
        Some("setOperations")
    } else if sub.hasAggs {
        Some("hasAggs")
    } else if sub.hasWindowFuncs {
        Some("hasWindowFuncs")
    } else if sub.hasTargetSRFs {
        Some("hasTargetSRFs")
    } else if !sub.groupClause.is_nil() || !sub.groupingSets.is_nil() {
        Some("GROUP BY")
    } else if sub.havingQual.is_some() {
        Some("HAVING")
    } else if !sub.sortClause.is_nil() {
        Some("ORDER BY")
    } else if !sub.distinctClause.is_nil() {
        Some("DISTINCT")
    } else if sub.limitOffset.is_some() || sub.limitCount.is_some() {
        Some("LIMIT/OFFSET")
    } else if sub.hasForUpdate {
        Some("FOR UPDATE")
    } else if !sub.cteList.is_nil() {
        Some("WITH")
    } else if security_barrier {
        Some("security_barrier")
    } else {
        None
    };
    if blocked.is_some() {
        return Ok(false);
    }
    if lateral {
        let mut safe_upper_varnos = types_nodes::Bitmapset::empty();
        let restricted = match lowest_outer_join {
            Some(loj) => {
                get_relids_in_jointree(mcx, loj, &mut safe_upper_varnos)?;
                true
            }
            None => false,
        };
        let jt = sub.jointree.expect("jointree is a FromExpr");
        let mut contains = false;
        for child in &jt.fromlist {
            if jointree_contains_lateral_outer_refs(mcx, child, restricted, &safe_upper_varnos)? {
                contains = true;
                break;
            }
        }
        if !contains && restricted {
            if let Some(q) = jt.quals {
                contains = !vars::pull_varnos_of_level(mcx, q, 1)?.is_subset(&safe_upper_varnos);
            }
        }
        if contains {
            return Ok(false);
        }
        if lowest_outer_join.is_some() {
            let mut lvarnos = types_nodes::Bitmapset::empty();
            for te in &sub.targetList {
                lvarnos.add_members(mcx, &vars::pull_varnos_of_level(mcx, te, 1)?)?;
            }
            if !lvarnos.is_subset(&safe_upper_varnos) {
                return Ok(false);
            }
        }
    }
    for te in &sub.targetList {
        if clauses::contain_volatile_functions(te)? {
            return Ok(false);
        }
    }
    Ok(true)
}

// pull_up_simple_subquery (prepjointree.c). C copyObject's rte->subquery and
// mutates the copy; here the offset pass rebuilds the pieces functionally, so
// the shared tree is never written. PlaceHolderVar wrapping is structurally
// unreachable (outer joins and grouping sets panic upstream).
fn pull_up_simple_subquery<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    varno: i32,
    rte_node: Node<'mcx>,
    lowest_outer_join: Option<Node<'mcx>>,
    containing_appendrel: Option<usize>,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
    let lateral = rte.lateral;
    let shared_sub = rte.subquery.expect("RTE_SUBQUERY has a subquery");

    let rtoffset = parse.rtable.len() as i32;
    // Nested pull-ups append their AppendRelInfos to run.root directly (C
    // keeps them on the subroot and offsets at concat); adjust the same set.
    let appinfo_snap = run.root.append_rel_list.len();
    // pre_adjusted: the sublink arm ran C's OffsetVarNodes /
    // IncrementVarSublevelsUp in place (sublevel-aware, descends into
    // retained SubLink bodies), so the functional offset passes below and the
    // per-RTE offset fixups must not run again.
    let (sub, pre_adjusted): (&Query<'mcx>, bool) = if shared_sub.hasSubLinks {
        // C copyObject (out/read round trip here): pull_up_sublinks and the
        // in-place var adjustments may write any node, and the source tree is
        // shared with the plancache.
        let deep = rewrite_manip::copy_query_node(mcx, shared_sub)?;
        let mut sub_local =
            crate::subselect::query_cells_copy(mcx, deep.as_query().expect("Query round trip"))?;
        crate::prep::replace_empty_jointree(mcx, &mut sub_local)?;
        crate::subselect::pull_up_sublinks(run, &mut sub_local)?;
        if sub_local.rtable.iter().any(|n| {
            n.as_range_tbl_entry().expect("rtable cell").rtekind == RTEKind::RTE_SUBQUERY
        }) {
            pull_up_subqueries(run, &mut sub_local)?;
        }
        // C rechecks after hacking on the copy; on failure the copy is
        // discarded and the RTE stays for set_subquery_pathlist.
        if !is_simple_subquery_sub(
            mcx,
            &sub_local,
            rte.lateral,
            rte.security_barrier,
            lowest_outer_join,
        )? || (containing_appendrel.is_some() && !is_safe_append_member(&sub_local))
        {
            return Ok(false);
        }
        let sealed = Node::mk(mcx, sub_local)?;
        rewrite_manip::OffsetVarNodes(mcx, sealed, rtoffset, 0)?;
        rewrite_manip::IncrementVarSublevelsUp(sealed, -1, 1)?;
        (sealed.as_query().expect("Query"), true)
    } else if shared_sub
        .rtable
        .iter()
        .any(|n| n.as_range_tbl_entry().expect("rtable cell").rtekind == RTEKind::RTE_SUBQUERY)
    {
        // C recursively completes pull_up_subqueries for the child before
        // splicing it in; runs on a cells-copy (C copyObject), the shared
        // tree is never written.
        let mut sub_local = crate::subselect::query_cells_copy(mcx, shared_sub)?;
        // Fresh RTE nodes: the recursive pass ends with a with_mut fixup
        // (subquery = None) that must never write a shared node.
        let mut fresh_rtable = NodeList::nil();
        for srte_node in &sub_local.rtable {
            let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
            fresh_rtable
                .lappend(mcx, rte_copy_with_perminfoindex(mcx, srte, srte.perminfoindex)?)?;
        }
        sub_local.rtable = fresh_rtable;
        pull_up_subqueries(run, &mut sub_local)?;
        (mcx::alloc_leak_in(mcx, sub_local)?, false)
    } else {
        (shared_sub, false)
    };
    let sub_jt = sub.jointree.expect("jointree is a FromExpr");

    // replace_empty_jointree (prepjointree.c): an empty-FROM subquery gets a
    // dummy RTE_RESULT to supply its one row; it lands after the subquery's
    // own rtable entries in the combined range table.
    let result_rtr = if sub_jt.fromlist.is_nil() {
        Some(Node::mk_range_tbl_ref(mcx, rtoffset + sub.rtable.len() as i32 + 1)?)
    } else {
        None
    };

    let off_tlist = if pre_adjusted {
        sub.targetList.clone_in(mcx)?
    } else {
        match clauses::walker::mutate_list(mcx, &sub.targetList, &mut |n| {
            offset_expr(mcx, n, rtoffset)
        })? {
            Some(l) => l,
            None => sub.targetList.clone_in(mcx)?,
        }
    };
    let mut off_fromlist = NodeList::nil();
    if let Some(rtr) = result_rtr {
        off_fromlist.lappend(mcx, rtr)?;
    }
    for jnode in &sub_jt.fromlist {
        if pre_adjusted {
            off_fromlist.lappend(mcx, jnode)?;
        } else {
            off_fromlist.lappend(mcx, offset_jointree(mcx, jnode, rtoffset)?)?;
        }
    }
    let off_quals = if pre_adjusted {
        sub_jt.quals
    } else {
        offset_opt(mcx, sub_jt.quals, rtoffset)?
    };
    let last_ph_id = core::cell::Cell::new(run.glob.last_ph_id);
    let rv_cache = core::cell::RefCell::new(mcx::vec_from_elem_in::<Option<Node<'mcx>>>(
        mcx,
        None,
        off_tlist.len() + 1,
    ));
    let phc = PullupPhCtx {
        wrap_all: !parse.groupingSets.is_nil(),
        last_ph_id: &last_ph_id,
        rv_cache: &rv_cache,
    };
    // subrelids (C get_relids_in_jointree(subquery->jointree, true, false)):
    // the post-pullup PHV phrels fixup target set, taken before off_fromlist
    // moves into the replacement node.
    let mut subrelids = types_nodes::Bitmapset::empty();
    for jnode in &off_fromlist {
        get_relids_in_jointree_no_inner(mcx, jnode, &mut subrelids)?;
    }

    // OffsetVarNodes/IncrementVarSublevelsUp over the subroot's
    // append_rel_list: nested pull-ups landed their AppendRelInfos in
    // run.root with sub-local relids; the translated exprs are always fresh
    // nodes outside the offset tree, so both branches adjust here.
    for i in appinfo_snap..run.root.append_rel_list.len() {
        {
            let a = &mut run.root.append_rel_list[i];
            a.parent_relid += rtoffset as u32;
            a.child_relid += rtoffset as u32;
        }
        replace_appinfo_translated_vars(run, i, &mut |n| offset_expr(mcx, n, rtoffset))?;
    }

    if let Some(ai) = containing_appendrel {
        // perform_pullup_replace_vars (prepjointree.c), appendrel arm: the
        // only upper reference to a UNION ALL member is its AppendRelInfo's
        // translated_vars (REPLACE_WRAP_NONE — no outer join between).
        replace_appinfo_translated_vars(run, ai, &mut |n| {
            replace_var_expr(mcx, n, varno, &off_tlist, lateral, Some(&phc))
        })?;
        // fix_append_rel_relids: is_safe_append_member guaranteed a single
        // base RTE (or the RESULT RTE built above), so the child relid is it.
        let mut node = off_fromlist.nth(0);
        while let Some(f) = node.as_from_expr() {
            node = f.fromlist.nth(0);
        }
        let subvarno = node.as_range_tbl_ref().expect("safe append member").rtindex;
        run.root.append_rel_list[ai].child_relid = subvarno as u32;
    } else {
        if let Some(l) = clauses::walker::mutate_list(mcx, &parse.targetList, &mut |n| {
            replace_var_expr(mcx, n, varno, &off_tlist, lateral, Some(&phc))
        })? {
            parse.targetList = l;
        }
        parse.havingQual = replace_opt(mcx, parse.havingQual, varno, &off_tlist, lateral, Some(&phc))?;
        if let Some(l) = clauses::walker::mutate_list(mcx, &parse.returningList, &mut |n| {
            replace_var_expr(mcx, n, varno, &off_tlist, lateral, Some(&phc))
        })? {
            parse.returningList = l;
        }
        // perform_pullup_replace_vars: MERGE action targetlists/quals and the
        // join condition reference the source rel too.
        for action_node in &parse.mergeActionList {
            let action = action_node.as_merge_action().expect("mergeActionList cell");
            let new_qual = replace_opt(mcx, action.qual, varno, &off_tlist, lateral, Some(&phc))?;
            let new_tlist = match clauses::walker::mutate_list(mcx, &action.targetList, &mut |n| {
                replace_var_expr(mcx, n, varno, &off_tlist, lateral, Some(&phc))
            })? {
                Some(l) => l,
                None => action.targetList.clone_in(mcx)?,
            };
            // SAFETY: pre-seal tree owned by this planner invocation.
            unsafe {
                action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(|a| {
                    a.qual = new_qual;
                    a.targetList = new_tlist;
                })
            }
            .expect("MergeAction");
        }
        parse.mergeJoinCondition =
            replace_opt(mcx, parse.mergeJoinCondition, varno, &off_tlist, lateral, Some(&phc))?;

        // pullup_replace_vars over the jointree: substitute Vars in every qual
        // and splice the offset sub-jointree in place of the RangeTblRef.
        let replacement = if off_quals.is_none() && off_fromlist.len() == 1 {
            off_fromlist.nth(0)
        } else {
            Node::mk(mcx, FromExpr { fromlist: off_fromlist, quals: off_quals })?
        };
        let jt = parse.jointree.expect("jointree is a FromExpr");
        let mut new_fromlist = NodeList::nil();
        for child in &jt.fromlist {
            new_fromlist.lappend(
                mcx,
                splice_and_replace(mcx, &parse.rtable, child, varno, &off_tlist, lateral, Some(&phc), replacement)?,
            )?;
        }
        let new_quals = replace_opt(mcx, jt.quals, varno, &off_tlist, lateral, Some(&phc))?;
        parse.jointree = Some(mcx::alloc_leak_in(
            mcx,
            FromExpr { fromlist: new_fromlist, quals: new_quals },
        )?);

        // perform_pullup_replace_vars tail: pre-existing appendrels' translated
        // exprs may reference the pulled-up rel (lateral union siblings).
        for i in 0..appinfo_snap {
            replace_appinfo_translated_vars(run, i, &mut |n| {
                replace_var_expr(mcx, n, varno, &off_tlist, lateral, Some(&phc))
            })?;
        }
    }

    // CombineRangeTables (rewriteManip.c): append rtable + rteperminfos,
    // renumbering the appended RTEs' perminfoindex. A LATERAL marker on the
    // pulled-up subquery propagates to child RTEs that can carry lateral
    // refs, and their expressions get the same offset/sublevel adjustment
    // the subquery body got.
    let perm_offset = parse.rteperminfos.len() as u32;
    for srte_node in &sub.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        let new_index = if srte.perminfoindex > 0 {
            srte.perminfoindex + perm_offset
        } else {
            srte.perminfoindex
        };
        // Copy, don't scribble: the subquery may be shared with a cached
        // parse tree (sublink pull-up) and a replan re-runs this offset.
        let copy = rte_copy_with_perminfoindex(mcx, srte, new_index)?;
        // range_table_walker's RTE legs of OffsetVarNodes: join alias vars,
        // function expressions and values lists carry Vars into the combined
        // rtable.
        let crte = copy.as_range_tbl_entry().expect("just built");
        match srte.rtekind {
            // IncrementVarSublevelsUp(subquery, -1, 1) RTE leg: an uplevel
            // CTE reference is one level closer after pull-up; the sublink
            // arm already ran the walker in place.
            RTEKind::RTE_CTE if !pre_adjusted => {
                if crte.ctelevelsup >= 1 {
                    // SAFETY: exclusive pre-seal fixup of the fresh copy.
                    unsafe { copy.with_mut::<RangeTblEntry, _>(|r| r.ctelevelsup -= 1) };
                }
            }
            RTEKind::RTE_JOIN if !pre_adjusted => {
                let off_aliasvars =
                    match clauses::walker::mutate_list(mcx, &crte.joinaliasvars, &mut |n| {
                        offset_expr(mcx, n, rtoffset)
                    })? {
                        Some(l) => l,
                        None => crte.joinaliasvars.clone_in(mcx)?,
                    };
                // SAFETY: exclusive pre-seal fixup of the fresh copy.
                unsafe { copy.with_mut::<RangeTblEntry, _>(|r| r.joinaliasvars = off_aliasvars) };
            }
            RTEKind::RTE_FUNCTION => {
                let off = if pre_adjusted {
                    crte.functions.clone_in(mcx)?
                } else {
                    match map_rtfunctions(mcx, &crte.functions, &mut |n| {
                        offset_expr(mcx, n, rtoffset)
                    })? {
                        Some(l) => l,
                        None => crte.functions.clone_in(mcx)?,
                    }
                };
                // SAFETY: pre-seal copy owned by this planner invocation.
                unsafe {
                    copy.with_mut::<RangeTblEntry, _>(|r| {
                        r.functions = off;
                        if rte.lateral {
                            r.lateral = true;
                        }
                    })
                };
            }
            RTEKind::RTE_VALUES => {
                let off = if pre_adjusted {
                    crte.values_lists.clone_in(mcx)?
                } else {
                    match clauses::walker::mutate_list(mcx, &crte.values_lists, &mut |n| {
                        offset_expr(mcx, n, rtoffset)
                    })? {
                        Some(l) => l,
                        None => crte.values_lists.clone_in(mcx)?,
                    }
                };
                // SAFETY: as above.
                unsafe {
                    copy.with_mut::<RangeTblEntry, _>(|r| {
                        r.values_lists = off;
                        if rte.lateral {
                            r.lateral = true;
                        }
                    })
                };
            }
            RTEKind::RTE_SUBQUERY => {
                // Retained (non-simple or sublink-pulled) child subqueries can
                // carry lateral cross-references once spliced under a LATERAL
                // subquery (C's lateral-propagation loop).
                if rte.lateral {
                    // SAFETY: as above.
                    unsafe { copy.with_mut::<RangeTblEntry, _>(|r| r.lateral = true) };
                }
            }
            _ => {}
        }
        // range_table_walker walks securityQuals for every RTE kind.
        if !pre_adjusted && !srte.securityQuals.is_nil() {
            if let Some(l) = clauses::walker::mutate_list(mcx, &srte.securityQuals, &mut |n| {
                offset_expr(mcx, n, rtoffset)
            })? {
                // SAFETY: exclusive pre-seal fixup of the fresh copy.
                unsafe { copy.with_mut::<RangeTblEntry, _>(|r| r.securityQuals = l) };
            }
        }
        parse.rtable.lappend(mcx, copy)?;
    }
    if result_rtr.is_some() {
        let eref = Node::mk_mut(
            mcx,
            types_nodes::Alias { aliasname: Some("*RESULT*"), colnames: NodeList::nil() },
        )?
        .seal_ref();
        parse.rtable.lappend(
            mcx,
            Node::mk(
                mcx,
                RangeTblEntry {
                    rtekind: RTEKind::RTE_RESULT,
                    eref: Some(eref),
                    inFromCl: true,
                    ..Default::default()
                },
            )?,
        )?;
    }
    for p in &sub.rteperminfos {
        parse.rteperminfos.lappend(mcx, p)?;
    }

    // parse->rowMarks = list_concat(parse->rowMarks, subquery->rowMarks). The
    // sublink arm's OffsetVarNodes already bumped the marker rtindexes on the
    // fresh copy; the functional branches offset fresh copies here (the source
    // markers may be shared with a cached parse tree).
    for rc_node in &sub.rowMarks {
        if pre_adjusted {
            parse.rowMarks.lappend(mcx, rc_node)?;
        } else {
            let rc = rc_node.as_row_mark_clause().expect("rowMarks holds RowMarkClause");
            parse.rowMarks.lappend(
                mcx,
                Node::mk(
                    mcx,
                    types_nodes::parsenodes::RowMarkClause {
                        rti: rc.rti + rtoffset as u32,
                        strength: rc.strength,
                        waitPolicy: rc.waitPolicy,
                        pushedDown: rc.pushedDown,
                    },
                )?,
            )?;
        }
    }

    // SAFETY: as above — exclusive pre-seal tree fixup.
    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = None) };
    // SubLinks can remain in the sub's tlist (now substituted into parse) and
    // in FUNCTION/VALUES RTE expressions copied up (C copies the flag for the
    // same reason).
    parse.hasSubLinks |= sub.hasSubLinks;
    parse.hasRowSecurity |= sub.hasRowSecurity;

    // PHVs made by pullup_replace_vars carry phrels = {varno}; retarget them
    // (and any pre-existing PHVs over varno) to the spliced-in subrelids.
    // fix_append_rel_relids covers the translated_vars copies of the same.
    run.glob.last_ph_id = last_ph_id.get();
    if run.glob.last_ph_id != 0 {
        crate::placeholder::substitute_phv_relids_query(mcx, parse, varno, &subrelids)?;
        for ai in 0..run.root.append_rel_list.len() {
            replace_appinfo_translated_vars(run, ai, &mut |n| {
                crate::placeholder::substitute_phv_relids(mcx, n, varno, &subrelids)?;
                Ok(None)
            })?;
        }
    }
    Ok(true)
}

// get_relids_in_jointree (prepjointree.c), include_outer_joins=true,
// include_inner_joins=false.
fn get_relids_in_jointree_no_inner<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    out: &mut types_nodes::Bitmapset<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            out.add_member(mcx, node.as_range_tbl_ref().unwrap().rtindex)?;
        }
        NodeTag::T_FromExpr => {
            for child in &node.as_from_expr().unwrap().fromlist {
                get_relids_in_jointree_no_inner(mcx, child, out)?;
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            get_relids_in_jointree_no_inner(mcx, j.larg, out)?;
            get_relids_in_jointree_no_inner(mcx, j.rarg, out)?;
            if j.rtindex != 0 && j.jointype != types_nodes::JoinType::JOIN_INNER {
                out.add_member(mcx, j.rtindex)?;
            }
        }
        other => panic!("get_relids_in_jointree (prepjointree.c): {other:?}"),
    }
    Ok(())
}

fn replace_appinfo_translated_vars<'mcx>(
    run: &mut crate::run::PlannerRun<'mcx>,
    ai: usize,
    f: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<()> {
    let n = run.root.append_rel_list[ai].translated_vars.len();
    for j in 0..n {
        let tid = run.root.append_rel_list[ai].translated_vars[j];
        if tid == types_pathnodes::NodeId::default() {
            continue;
        }
        let node = *run.root.expr_node(tid);
        if let Some(new) = f(node)? {
            let nid = run.intern_expr(new);
            run.root.append_rel_list[ai].translated_vars[j] = nid;
        }
    }
    Ok(())
}

// The functions list of an RTE holds RangeTblFunction wrappers, which the
// expression mutator does not know; map their funcexprs explicitly.
fn map_rtfunctions<'mcx>(
    mcx: Mcx<'mcx>,
    functions: &NodeList<'mcx>,
    f: &mut dyn FnMut(Node<'mcx>) -> PgResult<Option<Node<'mcx>>>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out = NodeList::nil();
    for f_node in functions {
        let rtf = f_node.as_range_tbl_function().expect("functions cell");
        let new_expr = match rtf.funcexpr {
            Some(e) => f(e)?,
            None => None,
        };
        match new_expr {
            Some(e) => {
                changed = true;
                out.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::parsenodes::RangeTblFunction {
                            funcexpr: Some(e),
                            funccolcount: rtf.funccolcount,
                            funccolnames: rtf.funccolnames.clone_in(mcx)?,
                            funccoltypes: rtf.funccoltypes.clone_in(mcx)?,
                            funccoltypmods: rtf.funccoltypmods.clone_in(mcx)?,
                            funccolcollations: rtf.funccolcollations.clone_in(mcx)?,
                            funcparams: rtf.funcparams.clone_in(mcx)?,
                        },
                    )?,
                )?;
            }
            None => out.lappend(mcx, f_node)?,
        }
    }
    Ok(if changed { Some(out) } else { None })
}

// The jointree leg of pullup_replace_vars (replace_vars_in_jointree): swap
// the pulled-up RangeTblRef for its replacement, rewrite the quals of every
// JoinExpr/FromExpr, and rewrite lateral sibling RTEs' expressions.
fn splice_and_replace<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &NodeList<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    lateral: bool,
    ph: Option<&PullupPhCtx<'_, 'mcx>>,
    replacement: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rtindex = node.as_range_tbl_ref().expect("RangeTblRef").rtindex;
            if rtindex == varno {
                return Ok(replacement);
            }
            let other_node = rtable.nth(rtindex as usize - 1);
            let other = other_node.as_range_tbl_entry().expect("rtable cell");
            if other.lateral {
                match other.rtekind {
                    RTEKind::RTE_FUNCTION => {
                        if let Some(l) =
                            map_rtfunctions(mcx, &other.functions, &mut |n| {
                                replace_var_expr(mcx, n, varno, tlist, lateral, ph)
                            })?
                        {
                            // SAFETY: pre-seal tree owned by this planner
                            // invocation; exclusive fixup.
                            unsafe {
                                other_node.with_mut::<RangeTblEntry, _>(|r| r.functions = l)
                            };
                        }
                    }
                    RTEKind::RTE_VALUES => {
                        if let Some(l) =
                            clauses::walker::mutate_list(mcx, &other.values_lists, &mut |n| {
                                replace_var_expr(mcx, n, varno, tlist, lateral, ph)
                            })?
                        {
                            // SAFETY: as above.
                            unsafe {
                                other_node.with_mut::<RangeTblEntry, _>(|r| r.values_lists = l)
                            };
                        }
                    }
                    RTEKind::RTE_SUBQUERY => {
                        let subq = other.subquery.expect("RTE_SUBQUERY has a subquery");
                        if let Some(newq) =
                            replace_vars_in_lateral_subquery(mcx, subq, varno, tlist, lateral, ph, 1)?
                        {
                            // SAFETY: as above.
                            unsafe {
                                other_node
                                    .with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(newq))
                            };
                        }
                    }
                    _ => {}
                }
            }
            Ok(node)
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(
                    mcx,
                    splice_and_replace(mcx, rtable, child, varno, tlist, lateral, ph, replacement)?,
                )?;
            }
            Node::mk(
                mcx,
                FromExpr { fromlist, quals: replace_opt(mcx, f.quals, varno, tlist, lateral, ph)? },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let larg = splice_and_replace(mcx, rtable, j.larg, varno, tlist, lateral, ph, replacement)?;
            let rarg = splice_and_replace(mcx, rtable, j.rarg, varno, tlist, lateral, ph, replacement)?;
            Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg,
                    rarg,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals: replace_opt(mcx, j.quals, varno, tlist, lateral, ph)?,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )
        }
        other => panic!("pullup_replace_vars (prepjointree.c): {other:?} jointree arm"),
    }
}

// OffsetVarNodes (rewriteManip.c), functional: changed nodes are rebuilt.
// OffsetVarNodes' jointree leg (rewriteManip.c): RangeTblRef rtindex and
// JoinExpr rtindex/quals shift by rtoffset; the tree is rebuilt, not scribbled.
fn offset_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            let r = node.as_range_tbl_ref().expect("RangeTblRef");
            Node::mk_range_tbl_ref(mcx, r.rtindex + rtoffset)
        }
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().expect("FromExpr");
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(mcx, offset_jointree(mcx, child, rtoffset)?)?;
            }
            Node::mk(
                mcx,
                FromExpr { fromlist, quals: offset_opt(mcx, f.quals, rtoffset)? },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().expect("JoinExpr");
            let quals = offset_opt(mcx, j.quals, rtoffset)?;
            Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg: offset_jointree(mcx, j.larg, rtoffset)?,
                    rarg: offset_jointree(mcx, j.rarg, rtoffset)?,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals,
                    alias: j.alias,
                    // C: if (j->rtindex) j->rtindex += offset.
                    rtindex: if j.rtindex != 0 { j.rtindex + rtoffset } else { 0 },
                },
            )
        }
        other => panic!("OffsetVarNodes (rewriteManip.c): {other:?} jointree arm"),
    }
}

fn offset_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup > 0 {
                // IncrementVarSublevelsUp(-1, 1): lateral/outer refs are one
                // level closer to their rels after pull-up; varno untouched.
                let mut nv = offset_var(mcx, v, 0)?;
                nv.varlevelsup -= 1;
                return Ok(Some(Node::mk(mcx, nv)?));
            }
            Ok(Some(Node::mk(mcx, offset_var(mcx, v, rtoffset)?)?))
        }
        NodeTag::T_RangeTblRef => {
            let r = node.as_range_tbl_ref().expect("RangeTblRef");
            Ok(Some(Node::mk_range_tbl_ref(mcx, r.rtindex + rtoffset)?))
        }
        _ => clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
            offset_expr(mcx, n, rtoffset)
        }),
    }
}

fn offset_var<'mcx>(mcx: Mcx<'mcx>, v: &Var<'mcx>, rtoffset: i32) -> PgResult<Var<'mcx>> {
    Ok(Var {
        varno: v.varno + rtoffset,
        varattno: v.varattno,
        vartype: v.vartype,
        vartypmod: v.vartypmod,
        varcollid: v.varcollid,
        varnullingrels: {
            // offset_relid_set: nulling relids (incl. ojrelids) shift too.
            let mut out = types_nodes::Bitmapset::default();
            for m in v.varnullingrels.iter() {
                out.add_member(mcx, m + rtoffset)?;
            }
            out
        },
        varlevelsup: v.varlevelsup,
        varreturningtype: v.varreturningtype,
        varnosyn: if v.varnosyn > 0 { v.varnosyn.wrapping_add(rtoffset as u32) } else { v.varnosyn },
        varattnosyn: v.varattnosyn,
        location: v.location,
    })
}

fn offset_opt<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    rtoffset: i32,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => Ok(Some(offset_expr(mcx, n, rtoffset)?.unwrap_or(n))),
    }
}

// pullup_replace_vars_callback's PHV state: wrap_all is C's REPLACE_WRAP_ALL
// (parent grouping sets); rv_cache dedups PHVs per attno so repeated
// references share one phid; last_ph_id shadows glob.last_ph_id (written back
// by pull_up_simple_subquery). None on paths that cannot make PHVs.
pub(crate) struct PullupPhCtx<'a, 'mcx> {
    wrap_all: bool,
    last_ph_id: &'a core::cell::Cell<u32>,
    rv_cache: &'a core::cell::RefCell<mcx::PgVec<'mcx, Option<Node<'mcx>>>>,
}

// pullup_replace_vars → pullup_replace_vars_callback (prepjointree.c) over
// ReplaceVarFromTargetList (rewriteManip.c), REPLACEVARS_REPORT_ERROR arm.
// sublevels_up matching is C's replace_rte_variables_mutator; non-matching
// upper vars pass through. lateral is the target RTE's lateral flag: a nulled
// Var over a LATERAL pull-up needs get_nullingrels-based wrap checks (loud).
fn replace_var_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    lateral: bool,
    ph: Option<&PullupPhCtx<'_, 'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    replace_var_expr_su(mcx, node, varno, tlist, lateral, ph, 0)
}

fn replace_var_expr_su<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    lateral: bool,
    ph: Option<&PullupPhCtx<'_, 'mcx>>,
    sublevels_up: u32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup != sublevels_up || v.varno != varno {
                return Ok(None);
            }
            if v.varattno < 0 {
                // System columns are not replaced.
                return Ok(Some(Node::mk(
                    mcx,
                    Var { varnullingrels: v.varnullingrels.clone_in(mcx)?, ..*v },
                )?));
            }
            let nulled = !v.varnullingrels.is_empty();
            if nulled && lateral {
                panic!(
                    "pullup_replace_vars_callback (prepjointree.c): nulled Var over a \
                     LATERAL pulled-up subquery (get_nullingrels wrap checks unported)"
                );
            }
            let need_phv = nulled || ph.is_some_and(|p| p.wrap_all);
            let cacheable = need_phv && (v.varattno as usize) <= tlist.len();
            let newnode = 'built: {
                if cacheable {
                    if let Some(p) = ph {
                        if let Some(cached) = p.rv_cache.borrow()[v.varattno as usize] {
                            break 'built rewrite_manip::copy_node(mcx, cached)?;
                        }
                    }
                }
                let gen = if v.varattno == 0 {
                    // ReplaceVarFromTargetList's whole-row arm, named-rowtype
                    // leg: RowExpr over the subquery outputs (RECORD/join legs
                    // stay loud).
                    if v.vartype == types_core::catalog::RECORDOID {
                        panic!(
                            "ReplaceVarFromTargetList (rewriteManip.c): RECORD whole-row \
                             Var expansion not ported"
                        );
                    }
                    let mut args = NodeList::nil();
                    for tle_node in tlist {
                        let tle = tle_node.as_target_entry().expect("tlist cell");
                        if tle.resjunk {
                            continue;
                        }
                        args.lappend(mcx, copy_expr(mcx, tle.expr, 0)?)?;
                    }
                    Node::mk(
                        mcx,
                        types_nodes::RowExpr {
                            args,
                            row_typeid: v.vartype,
                            row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                            colnames: NodeList::nil(),
                            location: v.location,
                        },
                    )?
                } else {
                    let Some(tle) = get_tle_by_resno(tlist, v.varattno) else {
                        return Err(missing_attribute(v.varattno));
                    };
                    debug_assert!(!tle.resjunk);
                    copy_expr(mcx, tle.expr, 0)?
                };
                if need_phv {
                    // A whole-row reference wraps the entire RowExpr so it
                    // yields NULL, not ROW(NULL,...), when nulled; simple
                    // Vars/PHVs escape; a strict expression over the
                    // subquery's own Vars takes the nullingrels directly
                    // (non-lateral, so every level-0 Var is the subquery's).
                    let wrap = if ph.is_some_and(|p| p.wrap_all) || v.varattno == 0 {
                        true
                    } else if gen.as_var().is_some_and(|nv| nv.varlevelsup == 0)
                        || gen.as_place_holder_var().is_some_and(|p| p.phlevelsup == 0)
                    {
                        false
                    } else {
                        !(vars::contain_vars_of_level(gen, 0)?
                            && !clauses::contain_nonstrict_functions(gen)?)
                    };
                    if wrap {
                        let Some(p) = ph else {
                            panic!(
                                "pullup_replace_vars_callback (prepjointree.c): replacement \
                                 needs a PlaceHolderVar on a path without a PHV allocator \
                                 (expand_virtual_generated_columns)"
                            );
                        };
                        p.last_ph_id.set(p.last_ph_id.get() + 1);
                        let mut phrels = types_nodes::Bitmapset::empty();
                        phrels.add_member(mcx, varno)?;
                        let wrapped = Node::mk(
                            mcx,
                            types_nodes::primnodes::PlaceHolderVar {
                                phexpr: gen,
                                phrels,
                                phnullingrels: types_nodes::Bitmapset::empty(),
                                phid: p.last_ph_id.get(),
                                phlevelsup: 0,
                            },
                        )?;
                        if cacheable {
                            p.rv_cache.borrow_mut()[v.varattno as usize] =
                                Some(rewrite_manip::copy_node(mcx, wrapped)?);
                        }
                        break 'built wrapped;
                    }
                }
                gen
            };
            // C order: propagate varnullingrels at level 0, then adjust
            // varlevelsup.
            if nulled {
                if newnode.as_var().is_some_and(|nv| nv.varlevelsup == 0) {
                    // SAFETY: fresh copy_expr/copy_node output; exclusive.
                    unsafe {
                        newnode.with_mut::<Var, _>(|w| {
                            w.varnullingrels.add_members(mcx, &v.varnullingrels)
                        })
                    }
                    .expect("Var")?;
                } else if newnode
                    .as_place_holder_var()
                    .is_some_and(|p| p.phlevelsup == 0)
                {
                    // SAFETY: as above.
                    unsafe {
                        newnode.with_mut::<types_nodes::primnodes::PlaceHolderVar, _>(|w| {
                            w.phnullingrels.add_members(mcx, &v.varnullingrels)
                        })
                    }
                    .expect("PlaceHolderVar")?;
                } else {
                    add_nulling_relids_expr(mcx, newnode, &v.varnullingrels)?;
                }
            }
            if sublevels_up > 0 {
                rewrite_manip::IncrementVarSublevelsUp(newnode, sublevels_up as i32, 0)?;
            }
            Ok(Some(newnode))
        }
        // replace_rte_variables_mutator's Query recursion: a SubLink body may
        // reference the pulled-up rel one level further out.
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().expect("SubLink");
            let new_test = match sl.testexpr {
                None => None,
                Some(te) => replace_var_expr_su(mcx, te, varno, tlist, lateral, ph, sublevels_up)?,
            };
            let subq = sl
                .subselect
                .as_query()
                .expect("transformed SubLink holds a Query");
            let new_sub =
                replace_vars_in_query_value(mcx, subq, varno, tlist, lateral, ph, sublevels_up + 1)?;
            if new_test.is_none() && new_sub.is_none() {
                return Ok(None);
            }
            let subselect = match new_sub {
                Some(q) => Node::mk(mcx, q)?,
                None => sl.subselect,
            };
            Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::SubLink {
                    subLinkType: sl.subLinkType,
                    subLinkId: sl.subLinkId,
                    testexpr: new_test.or(sl.testexpr),
                    operName: sl.operName.clone_in(mcx)?,
                    subselect,
                    location: sl.location,
                },
            )?))
        }
        NodeTag::T_Query if sublevels_up > 0 => panic!(
            "replace_rte_variables (rewriteManip.c): bare Query expression \
             during pull-up; sublevel-tracking arm unported"
        ),
        _ => clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
            replace_var_expr_su(mcx, n, varno, tlist, lateral, ph, sublevels_up)
        }),
    }
}

// pullup_replace_vars_subquery (prepjointree.c): rewrite level-1 references
// to the pulled-up rel inside a lateral sibling subquery. Returns None when
// nothing changed.
fn replace_vars_in_lateral_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    q: &'mcx Query<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    lateral: bool,
    ph: Option<&PullupPhCtx<'_, 'mcx>>,
    sublevels_up: u32,
) -> PgResult<Option<&'mcx Query<'mcx>>> {
    match replace_vars_in_query_value(mcx, q, varno, tlist, lateral, ph, sublevels_up)? {
        Some(newq) => Ok(Some(mcx::leak_in(mcx::alloc_in(mcx, newq)?))),
        None => Ok(None),
    }
}

fn replace_vars_in_query_value<'mcx>(
    mcx: Mcx<'mcx>,
    q: &'mcx Query<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    lateral: bool,
    ph: Option<&PullupPhCtx<'_, 'mcx>>,
    sublevels_up: u32,
) -> PgResult<Option<Query<'mcx>>> {
    let su = sublevels_up;
    let mut changed = false;
    let mut newq = crate::subselect::query_cells_copy(mcx, q)?;

    let rep_list = |l: &NodeList<'mcx>, changed: &mut bool| -> PgResult<NodeList<'mcx>> {
        match clauses::walker::mutate_list(mcx, l, &mut |n| {
            replace_var_expr_su(mcx, n, varno, tlist, lateral, ph, su)
        })? {
            Some(nl) => {
                *changed = true;
                Ok(nl)
            }
            None => Ok(l.clone_in(mcx)?),
        }
    };
    newq.targetList = rep_list(&newq.targetList, &mut changed)?;
    newq.returningList = rep_list(&newq.returningList, &mut changed)?;
    let mut rep_opt = |n: Option<Node<'mcx>>, changed: &mut bool| -> PgResult<Option<Node<'mcx>>> {
        match n {
            None => Ok(None),
            Some(x) => match replace_var_expr_su(mcx, x, varno, tlist, lateral, ph, su)? {
                Some(nx) => {
                    *changed = true;
                    Ok(Some(nx))
                }
                None => Ok(Some(x)),
            },
        }
    };
    newq.havingQual = rep_opt(newq.havingQual, &mut changed)?;
    newq.limitOffset = rep_opt(newq.limitOffset, &mut changed)?;
    newq.limitCount = rep_opt(newq.limitCount, &mut changed)?;

    let jt = newq.jointree.expect("jointree is a FromExpr");
    let mut jt_changed = false;
    let mut fromlist = NodeList::nil();
    for child in &jt.fromlist {
        fromlist.lappend(
            mcx,
            replace_in_sibling_jointree(mcx, child, varno, tlist, lateral, ph, su, &mut jt_changed)?,
        )?;
    }
    let quals = rep_opt(jt.quals, &mut jt_changed)?;
    if jt_changed {
        changed = true;
        newq.jointree = Some(mcx::alloc_leak_in(mcx, FromExpr { fromlist, quals })?);
    }

    for srte_node in &newq.rtable {
        let srte = srte_node.as_range_tbl_entry().expect("rtable cell");
        if !srte.lateral {
            continue;
        }
        match srte.rtekind {
            RTEKind::RTE_FUNCTION => {
                // query_cells_copy shares RTE nodes with the original query;
                // an in-place functions rewrite would scribble a shared tree.
                if map_rtfunctions(mcx, &srte.functions, &mut |n| {
                    replace_var_expr_su(mcx, n, varno, tlist, lateral, ph, su)
                })?
                .is_some()
                {
                    panic!(
                        "pullup_replace_vars_subquery (prepjointree.c): lateral function \
                         RTE inside a lateral sibling subquery references the pulled-up \
                         rel; nested-lateral rewrite arm unported"
                    );
                }
            }
            RTEKind::RTE_SUBQUERY => {
                let inner = srte.subquery.expect("RTE_SUBQUERY has a subquery");
                if contains_level_ref(inner, varno, su + 1)? {
                    panic!(
                        "pullup_replace_vars_subquery (prepjointree.c): doubly nested \
                         lateral subquery references the pulled-up rel; unported"
                    );
                }
            }
            _ => {}
        }
    }

    if !changed {
        return Ok(None);
    }
    Ok(Some(newq))
}

fn replace_in_sibling_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    lateral: bool,
    ph: Option<&PullupPhCtx<'_, 'mcx>>,
    su: u32,
    changed: &mut bool,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => Ok(node),
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().unwrap();
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(
                    mcx,
                    replace_in_sibling_jointree(mcx, child, varno, tlist, lateral, ph, su, changed)?,
                )?;
            }
            let quals = match f.quals {
                None => None,
                Some(q) => match replace_var_expr_su(mcx, q, varno, tlist, lateral, ph, su)? {
                    Some(nq) => {
                        *changed = true;
                        Some(nq)
                    }
                    None => Some(q),
                },
            };
            Node::mk(mcx, FromExpr { fromlist, quals })
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            let larg = replace_in_sibling_jointree(mcx, j.larg, varno, tlist, lateral, ph, su, changed)?;
            let rarg = replace_in_sibling_jointree(mcx, j.rarg, varno, tlist, lateral, ph, su, changed)?;
            let quals = match j.quals {
                None => None,
                Some(q) => match replace_var_expr_su(mcx, q, varno, tlist, lateral, ph, su)? {
                    Some(nq) => {
                        *changed = true;
                        Some(nq)
                    }
                    None => Some(q),
                },
            };
            Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg,
                    rarg,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )
        }
        other => panic!("replace_vars_in_jointree (prepjointree.c): {other:?} jointree arm"),
    }
}

// contain_vars_of_level-style probe for references to one specific varno.
fn contains_level_ref<'mcx>(q: &'mcx Query<'mcx>, varno: i32, level: u32) -> PgResult<bool> {
    use nodes_core::NodeWalker;
    struct W {
        varno: i32,
        level: u32,
        found: bool,
    }
    impl<'mcx> NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                if v.varlevelsup == self.level && v.varno == self.varno {
                    self.found = true;
                    return Ok(true);
                }
                return Ok(false);
            }
            if matches!(node.node_tag(), NodeTag::T_Query | NodeTag::T_SubLink) {
                self.level += 1;
                let r = nodes_core::expression_tree_walker(node, self);
                self.level -= 1;
                return r;
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { varno, level, found: false };
    for te in &q.targetList {
        w.visit(te)?;
    }
    if let Some(jt) = q.jointree {
        for n in &jt.fromlist {
            w.visit(n)?;
        }
        if let Some(qq) = jt.quals {
            w.visit(qq)?;
        }
    }
    if let Some(h) = q.havingQual {
        w.visit(h)?;
    }
    Ok(w.found)
}

fn replace_opt<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<Node<'mcx>>,
    varno: i32,
    tlist: &NodeList<'mcx>,
    lateral: bool,
    ph: Option<&PullupPhCtx<'_, 'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => Ok(Some(replace_var_expr(mcx, n, varno, tlist, lateral, ph)?.unwrap_or(n))),
    }
}

fn get_tle_by_resno<'a, 'mcx>(
    tlist: &'a NodeList<'mcx>,
    resno: i16,
) -> Option<&'mcx TargetEntry<'mcx>> {
    tlist
        .iter()
        .map(|n| n.as_target_entry().expect("tlist cell"))
        .find(|te| te.resno == resno)
}

// copyObject of the substituted expression (C copies per replacement; a
// shared node here would be double-visited by setrefs' in-place fixups).
// levels_delta is C's IncrementVarSublevelsUp(newnode, sublevels_up, 0) after
// substitution into a deeper query level.
pub(crate) fn copy_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    levels_delta: u32,
) -> PgResult<Node<'mcx>> {
    use types_nodes::primnodes as pn;
    let copy_list = |mcx: Mcx<'mcx>, l: &NodeList<'mcx>| -> PgResult<NodeList<'mcx>> {
        let mut out = NodeList::nil();
        for n in l {
            out.lappend(mcx, copy_expr(mcx, n, levels_delta)?)?;
        }
        Ok(out)
    };
    let copy_opt = |mcx: Mcx<'mcx>, n: Option<Node<'mcx>>| -> PgResult<Option<Node<'mcx>>> {
        match n {
            Some(n) => Ok(Some(copy_expr(mcx, n, levels_delta)?)),
            None => Ok(None),
        }
    };
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            let mut nv = offset_var(mcx, v, 0)?;
            nv.varlevelsup += levels_delta;
            Node::mk(mcx, nv)
        }
        NodeTag::T_Const => Node::mk(mcx, *node.as_const().expect("Const")),
        NodeTag::T_Param => Node::mk(mcx, *node.as_param().expect("Param")),
        NodeTag::T_CaseTestExpr => {
            Node::mk(mcx, *node.as_case_test_expr().expect("CaseTestExpr"))
        }
        NodeTag::T_SetToDefault => Node::mk(mcx, *node.as_set_to_default().expect("SetToDefault")),
        NodeTag::T_SQLValueFunction => {
            let s = node.as_sql_value_function().expect("SQLValueFunction");
            Node::mk(
                mcx,
                pn::SQLValueFunction {
                    op: s.op,
                    r#type: s.r#type,
                    typmod: s.typmod,
                    location: s.location,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().expect("OpExpr");
            Node::mk(
                mcx,
                pn::OpExpr {
                    opno: o.opno,
                    opfuncid: o.opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args: copy_list(mcx, &o.args)?,
                    location: o.location,
                },
            )
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().expect("DistinctExpr");
            Node::mk(
                mcx,
                pn::DistinctExpr {
                    opno: d.opno,
                    opfuncid: d.opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args: copy_list(mcx, &d.args)?,
                    location: d.location,
                },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().expect("FuncExpr");
            Node::mk(
                mcx,
                pn::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args: copy_list(mcx, &f.args)?,
                    location: f.location,
                },
            )
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().expect("ScalarArrayOpExpr");
            Node::mk(
                mcx,
                pn::ScalarArrayOpExpr {
                    opno: sa.opno,
                    opfuncid: sa.opfuncid,
                    hashfuncid: sa.hashfuncid,
                    negfuncid: sa.negfuncid,
                    useOr: sa.useOr,
                    inputcollid: sa.inputcollid,
                    args: copy_list(mcx, &sa.args)?,
                    location: sa.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().expect("BoolExpr");
            Node::mk(
                mcx,
                pn::BoolExpr {
                    boolop: b.boolop,
                    args: copy_list(mcx, &b.args)?,
                    location: b.location,
                },
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().expect("RelabelType");
            Node::mk(
                mcx,
                pn::RelabelType {
                    arg: copy_expr(mcx, r.arg, levels_delta)?,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().expect("FieldSelect");
            Node::mk(
                mcx,
                pn::FieldSelect { arg: copy_expr(mcx, f.arg, levels_delta)?, ..*f },
            )
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().expect("CoerceViaIO");
            Node::mk(
                mcx,
                pn::CoerceViaIO {
                    arg: copy_expr(mcx, c.arg, levels_delta)?,
                    resulttype: c.resulttype,
                    resultcollid: c.resultcollid,
                    coerceformat: c.coerceformat,
                    location: c.location,
                },
            )
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().expect("ArrayCoerceExpr");
            Node::mk(
                mcx,
                pn::ArrayCoerceExpr {
                    arg: copy_expr(mcx, a.arg, levels_delta)?,
                    elemexpr: copy_opt(mcx, a.elemexpr)?,
                    ..*a
                },
            )
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().expect("ConvertRowtypeExpr");
            Node::mk(
                mcx,
                pn::ConvertRowtypeExpr { arg: copy_expr(mcx, c.arg, levels_delta)?, ..*c },
            )
        }
        NodeTag::T_CoerceToDomain => {
            let c = node.as_coerce_to_domain().expect("CoerceToDomain");
            Node::mk(
                mcx,
                pn::CoerceToDomain { arg: copy_expr(mcx, c.arg, levels_delta)?, ..*c },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().expect("NullTest");
            Node::mk(
                mcx,
                pn::NullTest {
                    arg: copy_opt(mcx, nt.arg)?,
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_BooleanTest => {
            let bt = node.as_boolean_test().expect("BooleanTest");
            Node::mk(
                mcx,
                pn::BooleanTest {
                    arg: copy_opt(mcx, bt.arg)?,
                    booltesttype: bt.booltesttype,
                    location: bt.location,
                },
            )
        }
        NodeTag::T_CaseExpr => {
            let ce = node.as_case_expr().expect("CaseExpr");
            Node::mk(
                mcx,
                pn::CaseExpr {
                    casetype: ce.casetype,
                    casecollid: ce.casecollid,
                    arg: copy_opt(mcx, ce.arg)?,
                    args: copy_list(mcx, &ce.args)?,
                    defresult: copy_opt(mcx, ce.defresult)?,
                    location: ce.location,
                },
            )
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().expect("CaseWhen");
            Node::mk(
                mcx,
                pn::CaseWhen {
                    expr: copy_opt(mcx, cw.expr)?,
                    result: copy_opt(mcx, cw.result)?,
                    location: cw.location,
                },
            )
        }
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().expect("CoalesceExpr");
            Node::mk(
                mcx,
                pn::CoalesceExpr {
                    coalescetype: co.coalescetype,
                    coalescecollid: co.coalescecollid,
                    args: copy_list(mcx, &co.args)?,
                    location: co.location,
                },
            )
        }
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().expect("MinMaxExpr");
            Node::mk(
                mcx,
                pn::MinMaxExpr {
                    minmaxtype: mm.minmaxtype,
                    minmaxcollid: mm.minmaxcollid,
                    inputcollid: mm.inputcollid,
                    op: mm.op,
                    args: copy_list(mcx, &mm.args)?,
                    location: mm.location,
                },
            )
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().expect("ArrayExpr");
            Node::mk(
                mcx,
                pn::ArrayExpr {
                    array_typeid: a.array_typeid,
                    array_collid: a.array_collid,
                    element_typeid: a.element_typeid,
                    elements: copy_list(mcx, &a.elements)?,
                    multidims: a.multidims,
                    list_start: a.list_start,
                    list_end: a.list_end,
                    location: a.location,
                },
            )
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().expect("RowExpr");
            Node::mk(
                mcx,
                pn::RowExpr {
                    args: copy_list(mcx, &r.args)?,
                    row_typeid: r.row_typeid,
                    row_format: r.row_format,
                    colnames: r.colnames.clone_in(mcx)?,
                    location: r.location,
                },
            )
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().expect("JsonValueExpr");
            let raw = match j.raw_expr {
                Some(e) => Some(copy_expr(mcx, e, levels_delta)?),
                None => None,
            };
            let formatted = match j.formatted_expr {
                Some(e) => Some(copy_expr(mcx, e, levels_delta)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::JsonValueExpr {
                    raw_expr: raw,
                    formatted_expr: formatted,
                    format: j.format,
                },
            )
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().expect("JsonConstructorExpr");
            let mut args = NodeList::nil();
            for a in &c.args {
                args.lappend(mcx, copy_expr(mcx, a, levels_delta)?)?;
            }
            let func = match c.func {
                Some(f) => Some(copy_expr(mcx, f, levels_delta)?),
                None => None,
            };
            let coercion = match c.coercion {
                Some(co) => Some(copy_expr(mcx, co, levels_delta)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::JsonConstructorExpr {
                    r#type: c.r#type,
                    args,
                    func,
                    coercion,
                    returning: c.returning,
                    absent_on_null: c.absent_on_null,
                    unique: c.unique,
                    location: c.location,
                },
            )
        }
        NodeTag::T_JsonIsPredicate => {
            let p = node.as_json_is_predicate().expect("JsonIsPredicate");
            Node::mk(
                mcx,
                types_nodes::JsonIsPredicate {
                    expr: Some(copy_expr(mcx, p.expr.expect("expr"), levels_delta)?),
                    format: p.format,
                    item_type: p.item_type,
                    unique_keys: p.unique_keys,
                    location: p.location,
                },
            )
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().expect("SubscriptingRef");
            let mut upper = types_nodes::OptNodeList::nil();
            for e in &sr.refupperindexpr {
                let e = match e {
                    Some(e) => Some(copy_expr(mcx, e, levels_delta)?),
                    None => None,
                };
                upper.lappend(mcx, e)?;
            }
            let mut lower = types_nodes::OptNodeList::nil();
            for e in &sr.reflowerindexpr {
                let e = match e {
                    Some(e) => Some(copy_expr(mcx, e, levels_delta)?),
                    None => None,
                };
                lower.lappend(mcx, e)?;
            }
            let refexpr = match sr.refexpr {
                Some(e) => Some(copy_expr(mcx, e, levels_delta)?),
                None => None,
            };
            let refassgnexpr = match sr.refassgnexpr {
                Some(e) => Some(copy_expr(mcx, e, levels_delta)?),
                None => None,
            };
            Node::mk(
                mcx,
                pn::SubscriptingRef {
                    refcontainertype: sr.refcontainertype,
                    refelemtype: sr.refelemtype,
                    refrestype: sr.refrestype,
                    reftypmod: sr.reftypmod,
                    refcollid: sr.refcollid,
                    refupperindexpr: upper,
                    reflowerindexpr: lower,
                    refexpr,
                    refassgnexpr,
                },
            )
        }
        // C copyObject + IncrementVarSublevelsUp(newnode, sublevels_up, 0):
        // the out/read round trip is the deep copy, and the in-place level
        // shift is safe on it (exclusive tree).
        NodeTag::T_SubLink | NodeTag::T_PlaceHolderVar => {
            let copy = rewrite_manip::copy_node(mcx, node)?;
            if levels_delta > 0 {
                rewrite_manip::IncrementVarSublevelsUp(copy, levels_delta as i32, 0)?;
            }
            Ok(copy)
        }
        other => panic!(
            "copyObject (pullup_replace_vars): {other:?} copy arm unported \
             (simple-view expression set)"
        ),
    }
}

// reduce_outer_joins (prepjointree.c), including the LEFT -> ANTI reduction
// and partial FULL reduction.
struct RojPass1<'mcx> {
    relids: types_nodes::Bitmapset<'mcx>,
    contains_outer: bool,
    sub_states: mcx::PgVec<'mcx, RojPass1<'mcx>>,
}

struct RojPass2<'mcx> {
    inner_reduced: types_nodes::Bitmapset<'mcx>,
    partial_reduced: mcx::PgVec<'mcx, (i32, types_nodes::Bitmapset<'mcx>)>,
}

pub fn reduce_outer_joins<'mcx>(
    run: &crate::run::PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    let f = parse.jointree.expect("jointree is a FromExpr");
    let mut state1 = RojPass1 {
        relids: types_nodes::Bitmapset::empty(),
        contains_outer: false,
        sub_states: mcx::PgVec::new_in(mcx),
    };
    for child in &f.fromlist {
        let sub = reduce_outer_joins_pass1(mcx, child)?;
        state1.relids.add_members(mcx, &sub.relids)?;
        state1.contains_outer |= sub.contains_outer;
        state1.sub_states.push(sub);
    }
    assert!(state1.contains_outer, "so where are the outer joins?");

    let mut state2 = RojPass2 {
        inner_reduced: types_nodes::Bitmapset::empty(),
        partial_reduced: mcx::PgVec::new_in(mcx),
    };
    let pass_nonnullable = clauses::find_nonnullable_rels(mcx, f.quals)?;
    let pass_forced = clauses::find_forced_null_vars(mcx, f.quals)?;
    let mut fromlist = NodeList::nil();
    for (i, child) in f.fromlist.iter().enumerate() {
        let sub = &state1.sub_states[i];
        if sub.contains_outer {
            fromlist.lappend(
                mcx,
                reduce_outer_joins_pass2(
                    mcx,
                    parse,
                    child,
                    sub,
                    &mut state2,
                    &pass_nonnullable,
                    &pass_forced,
                )?,
            )?;
        } else {
            fromlist.lappend(mcx, child)?;
        }
    }
    parse.jointree = Some(mcx::alloc_leak_in(
        mcx,
        FromExpr { fromlist, quals: f.quals },
    )?);

    if !state2.inner_reduced.is_empty() {
        remove_nulling_relids(run, parse, &state2.inner_reduced, None)?;
    }
    for (full_join_rti, unreduced_side) in state2.partial_reduced.iter() {
        let single = types_nodes::Bitmapset::make_singleton(mcx, *full_join_rti)?;
        remove_nulling_relids(run, parse, &single, Some(unreduced_side))?;
    }
    Ok(())
}

fn reduce_outer_joins_pass1<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<RojPass1<'mcx>> {
    let mut result = RojPass1 {
        relids: types_nodes::Bitmapset::empty(),
        contains_outer: false,
        sub_states: mcx::PgVec::new_in(mcx),
    };
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            result.relids.add_member(mcx, node.as_range_tbl_ref().unwrap().rtindex)?;
        }
        NodeTag::T_FromExpr => {
            let f = node.as_variant::<FromExpr>().unwrap();
            for child in &f.fromlist {
                let sub = reduce_outer_joins_pass1(mcx, child)?;
                result.relids.add_members(mcx, &sub.relids)?;
                result.contains_outer |= sub.contains_outer;
                result.sub_states.push(sub);
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            if j.jointype.is_outer_join() {
                result.contains_outer = true;
            }
            for arg in [j.larg, j.rarg] {
                let sub = reduce_outer_joins_pass1(mcx, arg)?;
                result.relids.add_members(mcx, &sub.relids)?;
                result.contains_outer |= sub.contains_outer;
                result.sub_states.push(sub);
            }
        }
        other => panic!("reduce_outer_joins_pass1 (prepjointree.c): {other:?}"),
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn reduce_outer_joins_pass2<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
    node: Node<'mcx>,
    state1: &RojPass1<'mcx>,
    state2: &mut RojPass2<'mcx>,
    nonnullable_rels: &types_nodes::Bitmapset<'mcx>,
    forced_null_vars: &clauses::MultiBitmapset<'mcx>,
) -> PgResult<Node<'mcx>> {
    if let Some(f) = node.as_variant::<FromExpr>() {
        let mut pass_nonnullable = clauses::find_nonnullable_rels(mcx, f.quals)?;
        pass_nonnullable.add_members(mcx, nonnullable_rels)?;
        let mut pass_forced = clauses::find_forced_null_vars(mcx, f.quals)?;
        clauses::mbms_add_members(mcx, &mut pass_forced, forced_null_vars)?;
        debug_assert_eq!(f.fromlist.len(), state1.sub_states.len());
        let mut fromlist = NodeList::nil();
        for (child, sub) in f.fromlist.iter().zip(state1.sub_states.iter()) {
            if sub.contains_outer {
                fromlist.lappend(
                    mcx,
                    reduce_outer_joins_pass2(
                        mcx,
                        parse,
                        child,
                        sub,
                        state2,
                        &pass_nonnullable,
                        &pass_forced,
                    )?,
                )?;
            } else {
                fromlist.lappend(mcx, child)?;
            }
        }
        return Node::mk(mcx, FromExpr { fromlist, quals: f.quals });
    }
    let j = node
        .as_join_expr()
        .unwrap_or_else(|| panic!("reduce_outer_joins_pass2: reached {:?}", node.node_tag()));
    let rtindex = j.rtindex;
    let mut jointype = j.jointype;
    let (mut larg, mut rarg) = (j.larg, j.rarg);
    let mut left_ix = 0usize;
    let mut right_ix = 1usize;

    match jointype {
        types_nodes::JoinType::JOIN_INNER => {}
        types_nodes::JoinType::JOIN_LEFT => {
            if nonnullable_rels.overlap(&state1.sub_states[1].relids) {
                jointype = types_nodes::JoinType::JOIN_INNER;
            }
        }
        types_nodes::JoinType::JOIN_RIGHT => {
            if nonnullable_rels.overlap(&state1.sub_states[0].relids) {
                jointype = types_nodes::JoinType::JOIN_INNER;
            }
        }
        types_nodes::JoinType::JOIN_FULL => {
            let l = nonnullable_rels.overlap(&state1.sub_states[0].relids);
            let r = nonnullable_rels.overlap(&state1.sub_states[1].relids);
            if l {
                if r {
                    jointype = types_nodes::JoinType::JOIN_INNER;
                } else {
                    jointype = types_nodes::JoinType::JOIN_LEFT;
                    state2
                        .partial_reduced
                        .push((rtindex, state1.sub_states[1].relids.clone_in(mcx)?));
                }
            } else if r {
                jointype = types_nodes::JoinType::JOIN_RIGHT;
                state2
                    .partial_reduced
                    .push((rtindex, state1.sub_states[0].relids.clone_in(mcx)?));
            }
        }
        types_nodes::JoinType::JOIN_SEMI | types_nodes::JoinType::JOIN_ANTI => {}
        other => panic!(
            "reduce_outer_joins_pass2 (prepjointree.c): unrecognized join type {other:?}"
        ),
    }

    // JOIN_RIGHT -> JOIN_LEFT by swapping inputs.
    if jointype == types_nodes::JoinType::JOIN_RIGHT {
        core::mem::swap(&mut larg, &mut rarg);
        jointype = types_nodes::JoinType::JOIN_LEFT;
        left_ix = 1;
        right_ix = 0;
    }

    if jointype == types_nodes::JoinType::JOIN_LEFT {
        let nonnullable_vars = clauses::find_nonnullable_vars(mcx, j.quals)?;
        let overlap = clauses::mbms_overlap_sets(mcx, &nonnullable_vars, forced_null_vars)?;
        if overlap.overlap(&state1.sub_states[right_ix].relids) {
            jointype = types_nodes::JoinType::JOIN_ANTI;
        }
    }

    if rtindex != 0 && jointype != j.jointype {
        let rte_node = parse.rtable.nth(rtindex as usize - 1);
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        debug_assert_eq!(rte.rtekind, RTEKind::RTE_JOIN);
        debug_assert_eq!(rte.jointype, j.jointype);
        // SAFETY: pre-seal tree owned by this planner invocation; the shared
        // borrow is not read past this write.
        unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.jointype = jointype) };
        if jointype == types_nodes::JoinType::JOIN_INNER {
            state2.inner_reduced.add_member(mcx, rtindex)?;
        }
    }

    let left_state = &state1.sub_states[left_ix];
    let right_state = &state1.sub_states[right_ix];
    if left_state.contains_outer || right_state.contains_outer {
        // INNER passes local+upper constraints down; LEFT passes upper to the
        // outer side and local to the nullable side; FULL passes nothing
        // (C's comment block).
        let is_full = jointype == types_nodes::JoinType::JOIN_FULL;
        let mut local_nonnullable = if is_full {
            types_nodes::Bitmapset::empty()
        } else {
            clauses::find_nonnullable_rels(mcx, j.quals)?
        };
        let mut local_forced = if is_full {
            mcx::PgVec::new_in(mcx)
        } else {
            clauses::find_forced_null_vars(mcx, j.quals)?
        };
        let inner_or_semi = matches!(
            jointype,
            types_nodes::JoinType::JOIN_INNER | types_nodes::JoinType::JOIN_SEMI
        );
        if inner_or_semi {
            local_nonnullable.add_members(mcx, nonnullable_rels)?;
            clauses::mbms_add_members(mcx, &mut local_forced, forced_null_vars)?;
        }

        let empty_nn = types_nodes::Bitmapset::empty();
        let empty_fv = mcx::PgVec::new_in(mcx);
        if left_state.contains_outer {
            let (nn, fv) = if inner_or_semi {
                (&local_nonnullable, &local_forced)
            } else if !is_full {
                (nonnullable_rels, forced_null_vars)
            } else {
                (&empty_nn, &empty_fv)
            };
            larg = reduce_outer_joins_pass2(mcx, parse, larg, left_state, state2, nn, fv)?;
        }
        if right_state.contains_outer {
            let (nn, fv) = if !is_full {
                (&local_nonnullable, &local_forced)
            } else {
                (&empty_nn, &empty_fv)
            };
            rarg = reduce_outer_joins_pass2(mcx, parse, rarg, right_state, state2, nn, fv)?;
        }
    }

    Node::mk(
        mcx,
        types_nodes::JoinExpr {
            jointype,
            isNatural: j.isNatural,
            larg,
            rarg,
            usingClause: j.usingClause.clone_in(mcx)?,
            join_using_alias: j.join_using_alias,
            quals: j.quals,
            alias: j.alias,
            rtindex: j.rtindex,
        },
    )
}

// remove_nulling_relids (rewriteManip.c), in-place form: strips the reduced
// joins' bits from every Var whose varlevelsup addresses this query level,
// except Vars of rels in except_relids (partially-reduced FULL joins keep
// the bits on their unreduced side). Both C call sites also run the mutator
// over root->append_rel_list (UNION ALL pull-up populates it before these
// passes), so the walk covers its translated_vars too.
pub(crate) fn remove_nulling_relids<'mcx>(
    run: &crate::run::PlannerRun<'mcx>,
    parse: &Query<'mcx>,
    removable: &types_nodes::Bitmapset<'mcx>,
    except: Option<&types_nodes::Bitmapset<'mcx>>,
) -> PgResult<()> {
    struct W<'a, 'x> {
        removable: &'a types_nodes::Bitmapset<'x>,
        except: Option<&'a types_nodes::Bitmapset<'x>>,
        sublevels_up: u32,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Var => {
                    let v = node.as_var().unwrap();
                    if v.varlevelsup == self.sublevels_up
                        && !self.except.is_some_and(|e| e.is_member(v.varno))
                        && v.varnullingrels.overlap(self.removable)
                    {
                        // SAFETY: pre-seal tree owned by this planner
                        // invocation; the shared borrow ends before the write.
                        unsafe {
                            node.with_mut::<Var, _>(|v| {
                                v.varnullingrels.del_members(self.removable)
                            })
                        };
                    }
                    Ok(false)
                }
                NodeTag::T_Query => {
                    let q = node.as_query().expect("Query node");
                    self.sublevels_up += 1;
                    let r = nodes_core::query_tree_walker(q, self, 0);
                    self.sublevels_up -= 1;
                    r.map(|_| false)
                }
                NodeTag::T_PlaceHolderVar => {
                    let phv = node.as_place_holder_var().expect("PlaceHolderVar");
                    if phv.phlevelsup == self.sublevels_up
                        && !self.except.is_some_and(|e| e.overlap(&phv.phrels))
                    {
                        let removable = self.removable;
                        // SAFETY: pre-seal tree owned by this planner
                        // invocation; the shared borrow ends before the write.
                        unsafe {
                            node.with_mut::<types_nodes::primnodes::PlaceHolderVar, _>(|p| {
                                p.phnullingrels.del_members(removable);
                                p.phrels.del_members(removable);
                            })
                        };
                        debug_assert!(!node
                            .as_place_holder_var()
                            .expect("PlaceHolderVar")
                            .phrels
                            .is_empty());
                    }
                    nodes_core::expression_tree_walker(node, self)
                }
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.sublevels_up += 1;
            let r = nodes_core::query_tree_walker(q, self, 0);
            self.sublevels_up -= 1;
            r.map(|_| false)
        }
    }
    let mut w = W { removable, except, sublevels_up: 0 };
    nodes_core::query_tree_walker(parse, &mut w, 0)?;
    for appinfo in run.root.append_rel_list.iter() {
        for &tid in appinfo.translated_vars.iter() {
            if tid == types_pathnodes::NodeId::default() {
                continue;
            }
            nodes_core::NodeWalker::visit(&mut w, *run.root.expr_node(tid))?;
        }
    }
    Ok(())
}

// add_nulling_relids (rewriteManip.c), in-place expression form with
// target_relids = NULL: every Var whose varlevelsup addresses this level gets
// added_relids unioned into varnullingrels. Callers own the tree (fresh
// copy_expr output).
fn add_nulling_relids_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    added: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'x> {
        mcx: Mcx<'x>,
        added: &'a types_nodes::Bitmapset<'x>,
        sublevels_up: u32,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Var => {
                    if node.as_var().expect("Var").varlevelsup == self.sublevels_up {
                        let (mcx, added) = (self.mcx, self.added);
                        // SAFETY: exclusive tree (caller contract); the shared
                        // borrow ends before the write.
                        unsafe {
                            node.with_mut::<Var, _>(|v| v.varnullingrels.add_members(mcx, added))
                        }
                        .expect("Var")?;
                    }
                    Ok(false)
                }
                NodeTag::T_PlaceHolderVar => {
                    // C adds to phnullingrels without recursing: the PHV is
                    // assumed evaluated below the nulling join.
                    if node.as_place_holder_var().expect("PlaceHolderVar").phlevelsup
                        == self.sublevels_up
                    {
                        let (mcx, added) = (self.mcx, self.added);
                        // SAFETY: exclusive tree (caller contract).
                        unsafe {
                            node.with_mut::<types_nodes::primnodes::PlaceHolderVar, _>(|p| {
                                p.phnullingrels.add_members(mcx, added)
                            })
                        }
                        .expect("PlaceHolderVar")?;
                    }
                    Ok(false)
                }
                NodeTag::T_Query => {
                    let q = node.as_query().expect("Query node");
                    self.sublevels_up += 1;
                    let r = nodes_core::query_tree_walker(q, self, 0);
                    self.sublevels_up -= 1;
                    r.map(|_| false)
                }
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.sublevels_up += 1;
            let r = nodes_core::query_tree_walker(q, self, 0);
            self.sublevels_up -= 1;
            r.map(|_| false)
        }
    }
    let mut w = W { mcx, added, sublevels_up: 0 };
    nodes_core::NodeWalker::visit(&mut w, node)?;
    Ok(())
}

#[cold]
#[inline(never)]
fn missing_attribute(attno: i16) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not find attribute {attno} in subquery targetlist"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[cfg(test)]
mod tests {
    use mcx::{alloc_leak_in, Mcx, MemoryContext};
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::parsenodes::{Query, RTEKind, RTEPermissionInfo, RangeTblEntry};
    use types_nodes::primnodes::{FromExpr, OpExpr, Var};
    use types_nodes::{Node, NodeList};

    fn var<'mcx>(mcx: Mcx<'mcx>, varno: i32, attno: i16) -> Node<'mcx> {
        Node::mk(mcx, Var { varno, varattno: attno, vartype: 23, ..Default::default() }).unwrap()
    }

    fn tle<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>, resno: i16) -> Node<'mcx> {
        Node::mk_target_entry(mcx, expr, resno, None, false).unwrap()
    }

    fn perminfo<'mcx>(mcx: Mcx<'mcx>, relid: u32) -> Node<'mcx> {
        Node::mk(mcx, RTEPermissionInfo { relid, ..Default::default() }).unwrap()
    }

    fn from_expr<'mcx>(
        mcx: Mcx<'mcx>,
        rti: i32,
        quals: Option<Node<'mcx>>,
    ) -> &'mcx FromExpr<'mcx> {
        let rtr = Node::mk_range_tbl_ref(mcx, rti).unwrap();
        alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals },
        )
        .unwrap()
    }

    #[test]
    fn simple_view_subquery_flattens() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let sub_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: 77,
                relkind: b'r',
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let sub = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make1(mcx, sub_rte).unwrap(),
                rteperminfos: NodeList::make1(mcx, perminfo(mcx, 77)).unwrap(),
                targetList: NodeList::make2(
                    mcx,
                    tle(mcx, var(mcx, 1, 1), 1),
                    tle(mcx, var(mcx, 1, 2), 2),
                )
                .unwrap(),
                jointree: Some(from_expr(mcx, 1, None)),
                ..Default::default()
            },
        )
        .unwrap();

        let view_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_SUBQUERY,
                subquery: Some(sub),
                relid: 99,
                relkind: b'v',
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let qual = Node::mk(
            mcx,
            OpExpr {
                opno: 521,
                opresulttype: 16,
                args: NodeList::make2(
                    mcx,
                    var(mcx, 1, 1),
                    Node::mk_const(mcx, 23, -1, 0, 4, datum::Datum::from_i32(5), false, true)
                        .unwrap(),
                )
                .unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut parse = Query {
            commandType: CmdType::CMD_SELECT,
            rtable: NodeList::make1(mcx, view_rte).unwrap(),
            rteperminfos: NodeList::make1(mcx, perminfo(mcx, 99)).unwrap(),
            targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
            jointree: Some(from_expr(mcx, 1, Some(qual))),
            ..Default::default()
        };

        super::pull_up_subqueries(&mut crate::run::PlannerRun::new(mcx), &mut parse).unwrap();

        assert_eq!(parse.rtable.len(), 2);
        let dangling = parse.rtable.nth(0).as_range_tbl_entry().unwrap();
        assert_eq!(dangling.rtekind, RTEKind::RTE_SUBQUERY);
        assert!(dangling.subquery.is_none());
        assert_eq!(dangling.perminfoindex, 1);
        let base = parse.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(base.relid, 77);
        assert_eq!(base.perminfoindex, 2);
        assert_eq!(parse.rteperminfos.len(), 2);
        assert_eq!(
            parse.rteperminfos.nth(1).as_rte_permission_info().unwrap().relid,
            77
        );

        let jt = parse.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 2);

        let out_var = parse.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((out_var.varno, out_var.varattno), (2, 1));

        let q = jt.quals.unwrap().as_op_expr().unwrap();
        let qual_var = q.args.nth(0).as_var().unwrap();
        assert_eq!((qual_var.varno, qual_var.varattno), (2, 1));
        assert!(q.args.nth(1).as_const().is_some());
    }


    fn rel_rte<'mcx>(mcx: Mcx<'mcx>, relid: u32, perm: u32) -> Node<'mcx> {
        Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid,
                relkind: b'r',
                perminfoindex: perm,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn view_rte<'mcx>(mcx: Mcx<'mcx>, relid: u32, sub: &'mcx Query<'mcx>) -> Node<'mcx> {
        Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_SUBQUERY,
                subquery: Some(sub),
                relid,
                relkind: b'v',
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn eq_qual<'mcx>(mcx: Mcx<'mcx>, l: Node<'mcx>, r: Node<'mcx>) -> Node<'mcx> {
        Node::mk(
            mcx,
            OpExpr {
                opno: 96,
                opresulttype: 16,
                args: NodeList::make2(mcx, l, r).unwrap(),
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn join_view_subquery_flattens() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let join_rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_JOIN,
                jointype: types_nodes::JoinType::JOIN_INNER,
                joinaliasvars: NodeList::make2(mcx, var(mcx, 1, 1), var(mcx, 2, 1)).unwrap(),
                inFromCl: true,
                ..Default::default()
            },
        )
        .unwrap();
        let jexpr = Node::mk(
            mcx,
            types_nodes::JoinExpr {
                jointype: types_nodes::JoinType::JOIN_INNER,
                isNatural: false,
                larg: Node::mk_range_tbl_ref(mcx, 1).unwrap(),
                rarg: Node::mk_range_tbl_ref(mcx, 2).unwrap(),
                usingClause: NodeList::nil(),
                join_using_alias: None,
                quals: Some(eq_qual(mcx, var(mcx, 1, 1), var(mcx, 2, 1))),
                alias: None,
                rtindex: 3,
            },
        )
        .unwrap();
        let sub = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make3(mcx, rel_rte(mcx, 77, 1), rel_rte(mcx, 78, 2), join_rte)
                    .unwrap(),
                rteperminfos: NodeList::make2(mcx, perminfo(mcx, 77), perminfo(mcx, 78)).unwrap(),
                targetList: NodeList::make2(
                    mcx,
                    tle(mcx, var(mcx, 1, 1), 1),
                    tle(mcx, var(mcx, 2, 1), 2),
                )
                .unwrap(),
                jointree: Some(
                    alloc_leak_in(
                        mcx,
                        FromExpr { fromlist: NodeList::make1(mcx, jexpr).unwrap(), quals: None },
                    )
                    .unwrap(),
                ),
                ..Default::default()
            },
        )
        .unwrap();
        let mut parse = Query {
            commandType: CmdType::CMD_SELECT,
            rtable: NodeList::make1(mcx, view_rte(mcx, 99, sub)).unwrap(),
            rteperminfos: NodeList::make1(mcx, perminfo(mcx, 99)).unwrap(),
            targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 2), 1)).unwrap(),
            jointree: Some(from_expr(mcx, 1, None)),
            ..Default::default()
        };

        super::pull_up_subqueries(&mut crate::run::PlannerRun::new(mcx), &mut parse).unwrap();

        assert_eq!(parse.rtable.len(), 4);
        assert_eq!(parse.rtable.nth(1).as_range_tbl_entry().unwrap().perminfoindex, 2);
        assert_eq!(parse.rtable.nth(2).as_range_tbl_entry().unwrap().perminfoindex, 3);
        let jrte = parse.rtable.nth(3).as_range_tbl_entry().unwrap();
        assert_eq!(jrte.rtekind, RTEKind::RTE_JOIN);
        let av0 = jrte.joinaliasvars.nth(0).as_var().unwrap();
        let av1 = jrte.joinaliasvars.nth(1).as_var().unwrap();
        assert_eq!((av0.varno, av1.varno), (2, 3));

        let jt = parse.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        let j = jt.fromlist.nth(0).as_join_expr().unwrap();
        assert_eq!(j.rtindex, 4);
        assert_eq!(j.larg.as_range_tbl_ref().unwrap().rtindex, 2);
        assert_eq!(j.rarg.as_range_tbl_ref().unwrap().rtindex, 3);
        let jq = j.quals.unwrap().as_op_expr().unwrap();
        assert_eq!(jq.args.nth(0).as_var().unwrap().varno, 2);
        assert_eq!(jq.args.nth(1).as_var().unwrap().varno, 3);

        let out_var = parse.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((out_var.varno, out_var.varattno), (3, 1));
    }

    #[test]
    fn nested_view_subquery_flattens() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();

        let v1 = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make1(mcx, rel_rte(mcx, 77, 1)).unwrap(),
                rteperminfos: NodeList::make1(mcx, perminfo(mcx, 77)).unwrap(),
                targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
                jointree: Some(from_expr(mcx, 1, None)),
                ..Default::default()
            },
        )
        .unwrap();
        let v2 = alloc_leak_in(
            mcx,
            Query {
                commandType: CmdType::CMD_SELECT,
                rtable: NodeList::make1(mcx, view_rte(mcx, 88, v1)).unwrap(),
                rteperminfos: NodeList::make1(mcx, perminfo(mcx, 88)).unwrap(),
                targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
                jointree: Some(from_expr(mcx, 1, None)),
                ..Default::default()
            },
        )
        .unwrap();
        let mut parse = Query {
            commandType: CmdType::CMD_SELECT,
            rtable: NodeList::make1(mcx, view_rte(mcx, 99, v2)).unwrap(),
            rteperminfos: NodeList::make1(mcx, perminfo(mcx, 99)).unwrap(),
            targetList: NodeList::make1(mcx, tle(mcx, var(mcx, 1, 1), 1)).unwrap(),
            jointree: Some(from_expr(mcx, 1, None)),
            ..Default::default()
        };

        super::pull_up_subqueries(&mut crate::run::PlannerRun::new(mcx), &mut parse).unwrap();

        assert_eq!(parse.rtable.len(), 3);
        let mid = parse.rtable.nth(1).as_range_tbl_entry().unwrap();
        assert_eq!(mid.rtekind, RTEKind::RTE_SUBQUERY);
        assert!(mid.subquery.is_none());
        let base = parse.rtable.nth(2).as_range_tbl_entry().unwrap();
        assert_eq!(base.relid, 77);
        assert_eq!(base.perminfoindex, 3);
        assert_eq!(parse.rteperminfos.len(), 3);
        assert_eq!(parse.rteperminfos.nth(2).as_rte_permission_info().unwrap().relid, 77);

        let jt = parse.jointree.unwrap();
        assert_eq!(jt.fromlist.len(), 1);
        assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 3);
        let out_var = parse.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
        assert_eq!((out_var.varno, out_var.varattno), (3, 1));
    }
}

// transform_MERGE_to_join (prepjointree.c): replace the MERGE jointree (the
// bare source) with a join between the target and the source. WHEN NOT
// MATCHED BY SOURCE is the loud arm: it needs the outer-target join with
// source-var nulling marks and the executor's join-condition recheck.
pub fn transform_MERGE_to_join<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    use types_nodes::jointype::JoinType;
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::primnodes::{MergeMatchKind, NUM_MERGE_MATCH_KINDS};

    if parse.commandType != CmdType::CMD_MERGE {
        return Ok(());
    }

    let mut have_action = [false; NUM_MERGE_MATCH_KINDS];
    for action_node in &parse.mergeActionList {
        let action = action_node.as_merge_action().expect("mergeActionList cell");
        if action.commandType != CmdType::CMD_NOTHING {
            have_action[action.matchKind as usize] = true;
        }
    }
    if have_action[MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE as usize] {
        panic!(
            "transform_MERGE_to_join (prepjointree.c): WHEN NOT MATCHED BY SOURCE \
             (outer-target join + add_nulling_relids + executor join-condition \
             recheck) unported — MERGE by-source lane"
        );
    }
    let jointype =
        if have_action[MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize] {
            JoinType::JOIN_RIGHT
        } else {
            JoinType::JOIN_INNER
        };

    let eref = mcx::leak_in(mcx::alloc_in(
        mcx,
        types_nodes::primnodes::Alias { aliasname: Some("*MERGE*"), colnames: NodeList::nil() },
    )?);
    let joinrte = RangeTblEntry {
        rtekind: RTEKind::RTE_JOIN,
        jointype,
        eref: Some(eref),
        inFromCl: true,
        ..Default::default()
    };
    parse.rtable.lappend(mcx, Node::mk(mcx, joinrte)?)?;
    let joinrti = parse.rtable.len() as i32;

    let jt = parse.jointree.expect("MERGE jointree is a FromExpr");
    let rtr = Node::mk(
        mcx,
        types_nodes::primnodes::RangeTblRef { rtindex: parse.mergeTargetRelation },
    )?;
    let target = Node::mk(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr)?, quals: jt.quals },
    )?;
    assert_eq!(jt.fromlist.len(), 1, "MERGE jointree carries exactly the source");
    let source = jt.fromlist.nth(0);
    debug_assert!(matches!(
        source.node_tag(),
        NodeTag::T_RangeTblRef | NodeTag::T_JoinExpr
    ));

    let joinexpr = Node::mk(
        mcx,
        types_nodes::primnodes::JoinExpr {
            jointype,
            isNatural: false,
            larg: target,
            rarg: source,
            usingClause: NodeList::nil(),
            join_using_alias: None,
            quals: parse.mergeJoinCondition,
            alias: None,
            rtindex: joinrti,
        },
    )?;
    parse.jointree = Some(
        Node::mk_mut(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, joinexpr)?, quals: None },
        )?
        .seal_ref(),
    );

    // A non-empty targetList here means a trigger-updatable view target
    // (add_nulling_relids over its wholerow Var) — the view lane is loud
    // upstream in the rewriter.
    debug_assert!(parse.targetList.is_nil());

    // Without BY SOURCE actions the executor never rechecks the join
    // condition; C drops it to save planning/execution cycles.
    parse.mergeJoinCondition = None;
    Ok(())
}

// expand_virtual_generated_columns (prepjointree.c:969). Replaces every Var
// referencing a virtual generated column with its generation expression via
// the pullup replace machinery. build_generation_expression (rewriteHandler.c)
// is mirrored here: rewrite_handler depends on this crate, and cookDefault
// stored a coerced tree so build_column_default's re-coercion is a no-op.
// DIVERGENCE: C expands before pull_up_subqueries plus per pulled-up subquery
// (prepjointree.c:1360); here one pass runs after pull-up, when every merged
// relation RTE is in the parent rtable (pull-up never opens relations, and
// generation expressions are immutable, so pullability is unaffected).
// Retained RTE_SUBQUERYs expand in their own subquery_planner pass.
pub fn expand_virtual_generated_columns<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    const VIRTUAL_GEN: i8 = types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8;
    let nrte = parse.rtable.len();
    for rt_index in 1..=nrte {
        let rte_node = parse.rtable.nth(rt_index - 1);
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        if rte.rtekind != RTEKind::RTE_RELATION || !matches!(rte.relkind, b'r' | b'p') {
            continue;
        }
        let rel = table::table_open(mcx, rte.relid, types_rel::NoLock)?;
        if !rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_virtual) {
            table::table_close(rel, types_rel::NoLock)?;
            continue;
        }
        if parse.hasSubLinks {
            panic!(
                "expand_virtual_generated_columns (prepjointree.c): residual SubLink \
                 over a virtual-generated relation; sublevel replace arm unported"
            );
        }
        if !parse.groupingSets.is_nil() {
            panic!(
                "expand_virtual_generated_columns (prepjointree.c): grouping sets \
                 REPLACE_WRAP_ALL PlaceHolderVar arm unported"
            );
        }
        if parse.onConflict.is_some() {
            panic!(
                "expand_virtual_generated_columns (prepjointree.c): ON CONFLICT \
                 over a virtual-generated relation unported"
            );
        }
        if !parse.withCheckOptions.is_nil() {
            panic!(
                "expand_virtual_generated_columns (prepjointree.c): WithCheckOptions \
                 over a virtual-generated relation unported"
            );
        }
        assert!(!rte.lateral);
        let mut tlist = NodeList::nil();
        for i in 0..rel.rd_att.natts as usize {
            let att = rel.rd_att.attr(i);
            let expr = if att.attgenerated == VIRTUAL_GEN {
                let e = generation_expr(mcx, &rel, i + 1)?;
                change_varno_expr(mcx, e, 1, rt_index as i32)?.unwrap_or(e)
            } else {
                Node::mk_var(
                    mcx,
                    rt_index as i32,
                    (i + 1) as i16,
                    att.atttypid,
                    att.atttypmod,
                    att.attcollation,
                    0,
                )?
            };
            tlist.lappend(mcx, Node::mk_target_entry(mcx, expr, (i + 1) as i16, None, false)?)?;
        }
        table::table_close(rel, types_rel::NoLock)?;

        let varno = rt_index as i32;
        if let Some(l) = clauses::walker::mutate_list(mcx, &parse.targetList, &mut |n| {
            replace_var_expr(mcx, n, varno, &tlist, false, None)
        })? {
            parse.targetList = l;
        }
        parse.havingQual = replace_opt(mcx, parse.havingQual, varno, &tlist, false, None)?;
        if let Some(l) = clauses::walker::mutate_list(mcx, &parse.returningList, &mut |n| {
            replace_var_expr(mcx, n, varno, &tlist, false, None)
        })? {
            parse.returningList = l;
        }
        for action_node in &parse.mergeActionList {
            let action = action_node.as_merge_action().expect("mergeActionList cell");
            let new_qual = replace_opt(mcx, action.qual, varno, &tlist, false, None)?;
            let new_tlist = match clauses::walker::mutate_list(mcx, &action.targetList, &mut |n| {
                replace_var_expr(mcx, n, varno, &tlist, false, None)
            })? {
                Some(l) => l,
                None => action.targetList.clone_in(mcx)?,
            };
            // SAFETY: pre-seal tree owned by this planner invocation.
            unsafe {
                action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(|a| {
                    a.qual = new_qual;
                    a.targetList = new_tlist;
                })
            }
            .expect("MergeAction");
        }
        parse.mergeJoinCondition = replace_opt(mcx, parse.mergeJoinCondition, varno, &tlist, false, None)?;
        let jt = parse.jointree.expect("jointree is a FromExpr");
        let mut new_fromlist = NodeList::nil();
        for child in &jt.fromlist {
            new_fromlist.lappend(mcx, replace_vars_in_jointree_expand(mcx, child, varno, &tlist)?)?;
        }
        let new_quals = replace_opt(mcx, jt.quals, varno, &tlist, false, None)?;
        parse.jointree = Some(mcx::alloc_leak_in(
            mcx,
            FromExpr { fromlist: new_fromlist, quals: new_quals },
        )?);
    }
    Ok(())
}

fn replace_vars_in_jointree_expand<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    tlist: &NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => Ok(node),
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().expect("FromExpr");
            let mut fromlist = NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(mcx, replace_vars_in_jointree_expand(mcx, child, varno, tlist)?)?;
            }
            Node::mk(
                mcx,
                FromExpr { fromlist, quals: replace_opt(mcx, f.quals, varno, tlist, false, None)? },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().expect("JoinExpr");
            Node::mk(
                mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg: replace_vars_in_jointree_expand(mcx, j.larg, varno, tlist)?,
                    rarg: replace_vars_in_jointree_expand(mcx, j.rarg, varno, tlist)?,
                    usingClause: j.usingClause.clone_in(mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals: replace_opt(mcx, j.quals, varno, tlist, false, None)?,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )
        }
        other => panic!("expand_virtual_generated_columns: {other:?} jointree arm"),
    }
}

// build_generation_expression (rewriteHandler.c:4520), adbin-direct form.
fn generation_expr<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    attrno: usize,
) -> PgResult<Node<'mcx>> {
    let att = rel.rd_att.attr(attrno - 1);
    let constr = rel.rd_att.constr.as_deref().expect("caller checked");
    let adbin = constr
        .defval
        .iter()
        .find(|d| d.adnum == attrno as i16)
        .and_then(|d| d.adbin.as_ref())
        .unwrap_or_else(|| {
            panic!(
                "no generation expression found for column number {} of table \"{}\"",
                attrno,
                String::from_utf8_lossy(rel.rd_rel.relname.name_str())
            )
        });
    let expr = readfuncs::stringToNode(mcx, adbin.as_str())?;
    if att.attcollation != 0 && att.attcollation != nodes_core::node_funcs::expr_collation(expr) {
        return Node::mk(
            mcx,
            types_nodes::primnodes::CollateExpr {
                arg: expr,
                collOid: att.attcollation,
                location: -1,
            },
        );
    }
    Ok(expr)
}

// ChangeVarNodes (rewriteManip.c) expression leg, varlevelsup-0 only.
fn change_varno_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    from: i32,
    to: i32,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().expect("Var");
            if v.varlevelsup != 0 || v.varno != from {
                return Ok(None);
            }
            let mut nv = offset_var(mcx, v, 0)?;
            nv.varno = to;
            if nv.varnosyn == from as u32 {
                nv.varnosyn = to as u32;
            }
            Ok(Some(Node::mk(mcx, nv)?))
        }
        _ => clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
            change_varno_expr(mcx, n, from, to)
        }),
    }
}

// expand_generated_columns_in_expr (rewriteHandler.c:4493), planner-side copy
// (dep cycle: rewrite_handler depends on this crate). Only Vars naming a
// virtual generated column of rel at the given varno are replaced.
pub fn expand_generated_columns_in_expr<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    varno: i32,
) -> PgResult<Node<'mcx>> {
    const VIRTUAL_GEN: i8 = types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8;
    fn walk<'mcx>(
        mcx: Mcx<'mcx>,
        node: Node<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        varno: i32,
    ) -> PgResult<Option<Node<'mcx>>> {
        if let Some(v) = node.as_var() {
            if v.varlevelsup != 0 || v.varno != varno {
                return Ok(None);
            }
            if v.varattno == 0 {
                panic!(
                    "expand_generated_columns_in_expr (rewriteHandler.c): whole-row \
                     Var over a virtual-generated relation unported"
                );
            }
            if rel.rd_att.attr(v.varattno as usize - 1).attgenerated != VIRTUAL_GEN {
                return Ok(None);
            }
            let e = generation_expr(mcx, rel, v.varattno as usize)?;
            return Ok(Some(change_varno_expr(mcx, e, 1, varno)?.unwrap_or(e)));
        }
        clauses::walker::expression_tree_mutator(mcx, node, &mut |n| walk(mcx, n, rel, varno))
    }
    Ok(walk(mcx, node, rel, varno)?.unwrap_or(node))
}
