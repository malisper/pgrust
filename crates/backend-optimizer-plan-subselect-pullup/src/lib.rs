//! `backend/optimizer/plan/subselect.c` — the **prepjointree-facing** half:
//! the `ANY`/`EXISTS` SubLink → join conversions that
//! `pull_up_sublinks_qual_recurse` calls.
//!
//! 1:1 port of PostgreSQL 18.3 `convert_ANY_sublink_to_join` and
//! `convert_EXISTS_sublink_to_join` (and their helpers `convert_testexpr` /
//! `convert_testexpr_mutator`, `generate_subquery_vars`,
//! `simplify_EXISTS_query`) over this repo's owned `Query<'mcx>` /
//! `Node`/`Expr` model.
//!
//! ## What is here
//!
//! * [`convert_ANY_sublink_to_join`] / [`convert_EXISTS_sublink_to_join`] — the
//!   two seam-exported entry points (consumed by the unported `prepjointree.c`).
//! * `convert_testexpr` / `convert_testexpr_mutator` — the PARAM_SUBLINK →
//!   substitute-node mutator.
//! * `generate_subquery_vars` — build output Vars for the pulled-up subselect.
//! * `simplify_EXISTS_query` — strip the useless parts of an EXISTS subquery.
//! * `combine_range_tables` — `rewriteManip.c`'s `CombineRangeTables`, which is
//!   intentionally not yet ported in `backend-rewrite-core` (it belongs to the
//!   rule-rewriter path there). It is a small self-contained list merge that
//!   `convert_EXISTS_sublink_to_join` needs, so it lives here, exactly faithful
//!   to the C, until the rule engine provides it.
//! * `replace_empty_jointree` — `prepjointree.c`'s tiny non-recursive helper
//!   (same file as our caller); ported here since the caller is unported and it
//!   has no other home yet.
//!
//! ## Model notes
//!
//! `PlannerInfo.parse` is a lifetime-free `QueryId` handle; the caller resolves
//! the top `Query` (`run.resolve_mut`) and threads `&mut Query` + `&PlannerInfo`
//! in. The conversions consume the **analyzed** SubLink
//! (`Expr::SubLink(primnodes::SubLink)`): its `subselect` is an embedded owned
//! `Option<PgBox<Query>>` (mirroring `RangeTblEntry.subquery`) and its
//! `testexpr` is `Option<Box<Expr>>`; both are walked by deref. Where the
//! level/varno walkers need a `&Node` (C casts `(Node *) subselect` /
//! `(Node *) testexpr`) we wrap a clone in `Node::Query` / `Node::Expr`.
//! `RangeTblEntry.subquery` is likewise embedded owned. `pull_varnos`,
//! `contain_vars_of_level`, `contain_volatile_functions` and the `Var`
//! manipulators (`OffsetVarNodes` / `IncrementVarSublevelsUp`) come from their
//! ported owners. The `eval_const_expressions` LIMIT-folding leg of
//! `simplify_EXISTS_query` calls `clauses.c`'s ported folder.
//!
//! The SubPlan-building half (`make_subplan` / `build_subplan` /
//! `convert_EXISTS_to_ANY`) is deferred to the planmain stage (needs path
//! construction); it is NOT in this unit.

#![no_std]
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::collapsible_if)]

extern crate alloc;

use alloc::boxed::Box;

use backend_nodes_core::makefuncs::{make_alias, make_var_from_target_entry};
use backend_nodes_core::nodefuncs::expression_tree_mutator;
use mcx::{alloc_in, Mcx, PgBox, PgVec};
use types_core::primitive::Index;
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::jointype::JoinType;
use types_nodes::nodes::Node;
use types_nodes::nodes::CmdType;
use types_nodes::parsenodes::{RangeTblEntry, RTEKind};
use types_nodes::primnodes::{Expr, ParamKind, SubLink, SubLinkType};
use types_nodes::rawnodes::{FromExpr, JoinExpr, RangeTblRef};
use types_pathnodes::{Bitmapset, PlannerInfo, Relids};

// ===========================================================================
// init_seams — install the two prepjointree-facing conversions.
// ===========================================================================

/// Install this unit's owned inward seams.
pub fn init_seams() {
    backend_optimizer_plan_subselect_pullup_seams::convert_ANY_sublink_to_join::set(
        convert_ANY_sublink_to_join,
    );
    backend_optimizer_plan_subselect_pullup_seams::convert_EXISTS_sublink_to_join::set(
        convert_EXISTS_sublink_to_join,
    );
    backend_optimizer_plan_subselect_pullup_seams::convert_VALUES_to_ANY::set(
        convert_VALUES_to_ANY_panic,
    );
}

/// `convert_VALUES_to_ANY(root, testexpr, values)` (subselect.c) — **deferred to
/// the planmain stage**: its body needs `convert_testexpr` (already here) plus
/// `eval_const_expressions` and `make_SAOP_expr`, which are part of the
/// still-unported SubPlan-building half of subselect.c. Installed as a
/// seam-and-panic body; `pull_up_sublinks_qual_recurse` (the only caller) is
/// reachable only from the unported `subquery_planner`, so this panic is latent.
fn convert_VALUES_to_ANY_panic<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: &PlannerInfo,
    _sublink: &SubLink,
) -> PgResult<Option<Expr>> {
    panic!(
        "convert_VALUES_to_ANY: the VALUES-list fast path is part of the \
         SubPlan-building (planmain) half of subselect.c, not yet ported"
    )
}

// ===========================================================================
// convert_testexpr
// ===========================================================================

/// `convert_testexpr(root, testexpr, subst_nodes)` (subselect.c): convert the
/// parser's testexpr into executable form, replacing PARAM_SUBLINK Params with
/// the nodes from `subst_nodes` (here, the subquery output Vars).
///
/// The testexpr in the analyzed tree is a `Node::Expr(...)` subtree (Params and
/// OpExprs). We run the substitution over the `Expr` model; `subst_nodes` are
/// the `Var` Exprs produced by [`generate_subquery_vars`].
fn convert_testexpr<'mcx>(testexpr: &Node<'mcx>, subst_nodes: &[Expr]) -> Node<'mcx> {
    match testexpr {
        Node::Expr(e) => Node::Expr(convert_testexpr_mutator(e.clone(), subst_nodes)),
        // The C always passes an expression tree here; an ANY SubLink's testexpr
        // is an OpExpr / BoolExpr, i.e. a `Node::Expr`. Anything else is a
        // malformed parse tree.
        other => panic!(
            "convert_testexpr: ANY SubLink testexpr is not an expression node: {:?}",
            other.node_tag()
        ),
    }
}

/// `convert_testexpr_mutator(node, context)` (subselect.c).
fn convert_testexpr_mutator(node: Expr, subst_nodes: &[Expr]) -> Expr {
    if let Expr::Param(param) = &node {
        if param.paramkind == ParamKind::PARAM_SUBLINK {
            // paramid is 1-based; out-of-range is a hard internal error in C.
            let id = param.paramid;
            if id <= 0 || (id as usize) > subst_nodes.len() {
                panic!("unexpected PARAM_SUBLINK ID: {}", id);
            }
            // We copy the list item to avoid having doubly-linked substructure
            // in the modified parse tree.
            return subst_nodes[(id - 1) as usize].clone();
        }
    }
    if let Expr::SubLink(_) = &node {
        // A nested SubLink: do not recurse into it; its PARAM_SUBLINKs belong to
        // the inner SubLink. Return as-is.
        return node;
    }
    let mut f = |child: Expr| convert_testexpr_mutator(child, subst_nodes);
    expression_tree_mutator(node, &mut f)
}

// ===========================================================================
// generate_subquery_vars
// ===========================================================================

/// `generate_subquery_vars(root, tlist, varno)` (subselect.c): build a list of
/// Vars representing the output columns of a sublink's sub-select, given the
/// sub-select's targetlist. The Vars have the specified varno (RTE index).
fn generate_subquery_vars<'mcx>(
    tlist: &[types_nodes::primnodes::TargetEntry<'mcx>],
    varno: Index,
) -> PgResult<alloc::vec::Vec<Expr>> {
    let mut result = alloc::vec::Vec::new();
    for tent in tlist.iter() {
        if tent.resjunk {
            continue;
        }
        let var = make_var_from_target_entry(varno as i32, tent)?;
        result.push(Expr::Var(var));
    }
    Ok(result)
}

// ===========================================================================
// convert_ANY_sublink_to_join
// ===========================================================================

/// `convert_ANY_sublink_to_join(root, sublink, available_rels)` (subselect.c).
pub fn convert_ANY_sublink_to_join<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    parse: &mut Query<'mcx>,
    sublink: &SubLink,
    available_rels: &Relids,
) -> PgResult<Option<JoinExpr<'mcx>>> {
    debug_assert!(sublink.subLinkType == SubLinkType::Any);

    // `subselect = (Query *) sublink->subselect`. In the analyzed tree the
    // SubLink carries its sub-`Query` embedded-owned (mirroring
    // `RangeTblEntry.subquery`); walk it by deref. Wrap it in a `Node::Query`
    // for the level walkers, which take a `&Node` (C casts `(Node *) subselect`).
    let subselect = sublink
        .subselect
        .as_deref()
        .expect("convert_ANY_sublink_to_join: SubLink has no subselect");
    let subselect_node = Node::Query(subselect.clone_in(mcx)?);

    // If the sub-select contains any Vars of the parent query, we treat it as
    // LATERAL.  (Vars from higher levels don't matter here.)
    //
    // C: `pull_varnos_of_level(NULL, (Node *) subselect, 1)` — root is passed as
    // NULL here (no PlaceHolderInfo resolution), so we pass `None`.
    let sub_ref_outer_relids =
        backend_optimizer_util_vars::var::pull_varnos_of_level(None, &subselect_node, 1);
    let use_lateral = !relids_is_empty(&sub_ref_outer_relids);

    // Can't convert if the sub-select contains parent-level Vars of relations
    // not in available_rels.
    if !relids_is_subset(&sub_ref_outer_relids, available_rels) {
        return Ok(None);
    }

    // The test expression must contain some Vars of the parent query, else
    // it's not gonna be a join.  (Note that it won't have Vars referring to
    // the subquery, rather Params.)
    //
    // `sublink->testexpr` is the analyzed test expression (`Expr`), embedded as
    // `Option<Box<Expr>>`. Wrap it in a `Node::Expr` for `pull_varnos` (which
    // takes `&Node`), and hand the `&Expr` straight to
    // `contain_volatile_functions`.
    let testexpr_expr = sublink
        .testexpr
        .as_deref()
        .expect("convert_ANY_sublink_to_join: ANY SubLink has no testexpr");
    let testexpr_node = Node::Expr(testexpr_expr.clone());
    let upper_varnos =
        backend_optimizer_util_vars::var::pull_varnos(Some(root), &testexpr_node);
    if relids_is_empty(&upper_varnos) {
        return Ok(None);
    }

    // However, it can't refer to anything outside available_rels.
    if !relids_is_subset(&upper_varnos, available_rels) {
        return Ok(None);
    }

    // The combining operators and left-hand expressions mustn't be volatile.
    if backend_optimizer_util_clauses::grounded::contain_volatile_functions(Some(testexpr_expr))?
    {
        return Ok(None);
    }

    // Create a dummy ParseState for addRangeTableEntryForSubquery.
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;

    // Okay, pull up the sub-select into upper range table.  We rely on the
    // assumption that the outer query has no references to the inner.
    //
    // addRangeTableEntryForSubquery consumes the subquery by value; the owned
    // tree gives us only a borrow, so we deep-copy the subselect Query (C also
    // works on the parser-owned Query directly — the copy is behaviour-neutral
    // since the SubLink's subselect is discarded once pulled up).
    let subselect_owned = subselect.clone_in(mcx)?;
    let any_alias = make_alias(mcx, "ANY_subquery", PgVec::new_in(mcx))?;
    let nsitem = backend_parser_relation::addRangeTableEntryForSubquery(
        mcx,
        &mut pstate,
        subselect_owned,
        Some(any_alias),
        use_lateral,
        false,
    )?;
    let rte = nsitem
        .p_rte
        .expect("addRangeTableEntryForSubquery returned no RTE");
    parse.rtable.push(PgBox::into_inner(rte));
    let rtindex = parse.rtable.len() as Index;

    // Form a RangeTblRef for the pulled-up sub-select.
    let rtr = RangeTblRef {
        rtindex: rtindex as i32,
    };

    // Build a list of Vars representing the subselect outputs.  We use the just-
    // pushed RTE's subquery targetlist (identical to the original subselect's,
    // since addRangeTableEntryForSubquery stores it).
    let subquery_vars = {
        let pushed = &parse.rtable[(rtindex - 1) as usize];
        let sq = pushed
            .subquery
            .as_deref()
            .expect("pulled-up RTE has no subquery");
        generate_subquery_vars(&sq.targetList, rtindex)?
    };

    // Build the new join's qual expression, replacing Params with these Vars.
    let quals = convert_testexpr(&testexpr_node, &subquery_vars);

    // And finally, build the JoinExpr node.
    let result = JoinExpr {
        jointype: JoinType::JOIN_SEMI,
        isNatural: false,
        larg: None, // caller must fill this in
        rarg: Some(alloc_in(mcx, Node::RangeTblRef(rtr))?),
        usingClause: PgVec::new_in(mcx),
        join_using_alias: None,
        quals: Some(alloc_in(mcx, quals)?),
        alias: None,
        rtindex: 0, // we don't need an RTE for it
    };

    Ok(Some(result))
}

// ===========================================================================
// convert_EXISTS_sublink_to_join
// ===========================================================================

/// `convert_EXISTS_sublink_to_join(root, sublink, under_not, available_rels)`
/// (subselect.c).
pub fn convert_EXISTS_sublink_to_join<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    parse: &mut Query<'mcx>,
    sublink: &SubLink,
    under_not: bool,
    available_rels: &Relids,
) -> PgResult<Option<JoinExpr<'mcx>>> {
    debug_assert!(sublink.subLinkType == SubLinkType::Exists);

    // `subselect = (Query *) sublink->subselect`. In the analyzed tree the
    // SubLink carries its sub-`Query` embedded-owned (mirroring
    // `RangeTblEntry.subquery`); walk it by deref.
    let subselect_ref = sublink
        .subselect
        .as_deref()
        .expect("convert_EXISTS_sublink_to_join: SubLink has no subselect");

    // Can't flatten if it contains WITH.
    if !subselect_ref.cteList.is_empty() {
        return Ok(None);
    }

    // Copy the subquery so we can modify it safely (see comments in
    // make_subplan).
    let mut subselect = subselect_ref.clone_in(mcx)?;

    // See if the subquery can be simplified based on the knowledge that it's
    // being used in EXISTS().  If we aren't able to get rid of its targetlist,
    // we have to fail, because the pullup operation leaves us with noplace to
    // evaluate the targetlist.
    if !simplify_EXISTS_query(root, mcx, &mut subselect)? {
        return Ok(None);
    }

    // Separate out the WHERE clause.
    let jointree = subselect
        .jointree
        .as_mut()
        .expect("convert_EXISTS_sublink_to_join: subquery has no jointree");
    let mut where_clause = jointree.quals.take();

    // The rest of the sub-select must not refer to any Vars of the parent
    // query.  (Vars of higher levels should be okay, though.)
    //
    // Re-wrap the (mutated) subselect Query as a Node for the level walkers.
    {
        let subselect_as_node = Node::Query(subselect);
        if backend_optimizer_util_vars::var::contain_vars_of_level(&subselect_as_node, 1) {
            return Ok(None);
        }
        // On the other hand, the WHERE clause must contain some Vars of the
        // parent query, else it's not gonna be a join.
        match where_clause.as_deref() {
            Some(wc) => {
                if !backend_optimizer_util_vars::var::contain_vars_of_level(wc, 1) {
                    return Ok(None);
                }
                // We don't risk optimizing if the WHERE clause is volatile.
                if backend_optimizer_util_clauses::grounded::contain_volatile_functions(
                    wc.as_expr(),
                )? {
                    return Ok(None);
                }
            }
            None => {
                // contain_vars_of_level(NULL,1) is false → no parent Vars → fail.
                return Ok(None);
            }
        }
        // Unwrap the Query back out.
        subselect = match subselect_as_node {
            Node::Query(q) => q,
            _ => unreachable!(),
        };
    }

    // The subquery must have a nonempty jointree, but we can make it so.
    replace_empty_jointree(mcx, &mut subselect)?;

    // Prepare to pull up the sub-select into top range table.  Adjust all
    // level-zero varnos in the subquery to account for the rtable merger.
    let rtoffset = parse.rtable.len() as i32;
    {
        // OffsetVarNodes((Node *) subselect, rtoffset, 0)
        let mut subselect_node2 = Node::Query(subselect);
        backend_rewrite_core::offset::OffsetVarNodes(&mut subselect_node2, rtoffset, 0);
        // IncrementVarSublevelsUp((Node *) subselect, -1, 1)
        backend_rewrite_core::increment::IncrementVarSublevelsUp(&mut subselect_node2, -1, 1);
        subselect = match subselect_node2 {
            Node::Query(q) => q,
            _ => unreachable!(),
        };
    }
    if let Some(wc) = where_clause.as_deref_mut() {
        backend_rewrite_core::offset::OffsetVarNodes(wc, rtoffset, 0);
        backend_rewrite_core::increment::IncrementVarSublevelsUp(wc, -1, 1);
    }

    // Now that the WHERE clause is adjusted to match the parent query
    // environment, we can easily identify all the level-zero rels it uses.
    // The ones <= rtoffset belong to the upper query; the ones > rtoffset do
    // not.
    let clause_varnos = match where_clause.as_deref() {
        Some(wc) => backend_optimizer_util_vars::var::pull_varnos(Some(root), wc),
        None => None,
    };
    let mut upper_varnos: Relids = None;
    let mut varno: i32 = -1;
    loop {
        varno = relids_next_member(&clause_varnos, varno);
        if varno < 0 {
            break;
        }
        if varno <= rtoffset {
            upper_varnos = relids_add_member(upper_varnos, varno);
        }
    }
    debug_assert!(!relids_is_empty(&upper_varnos));

    // Now that we've got the set of upper-level varnos, we can make the last
    // check: only available_rels can be referenced.
    if !relids_is_subset(&upper_varnos, available_rels) {
        return Ok(None);
    }

    // Now we can attach the modified subquery rtable to the parent.  This also
    // adds subquery's RTEPermissionInfos into the upper query.
    combine_range_tables(mcx, parse, &mut subselect);

    // And finally, build the JoinExpr node.
    let jointype = if under_not {
        JoinType::JOIN_ANTI
    } else {
        JoinType::JOIN_SEMI
    };

    // flatten out the FromExpr node if it's useless
    let sub_jointree = subselect
        .jointree
        .take()
        .expect("convert_EXISTS_sublink_to_join: subquery lost its jointree");
    let rarg: Node = if sub_jointree.fromlist.len() == 1 {
        // linitial(subselect->jointree->fromlist)
        let mut fromexpr = PgBox::into_inner(sub_jointree);
        let first = fromexpr.fromlist.remove(0);
        PgBox::into_inner(first)
    } else {
        Node::FromExpr(PgBox::into_inner(sub_jointree))
    };

    let result = JoinExpr {
        jointype,
        isNatural: false,
        larg: None, // caller must fill this in
        rarg: Some(alloc_in(mcx, rarg)?),
        usingClause: PgVec::new_in(mcx),
        join_using_alias: None,
        quals: match where_clause {
            Some(wc) => Some(wc),
            None => None,
        },
        alias: None,
        rtindex: 0,
    };

    Ok(Some(result))
}

// ===========================================================================
// simplify_EXISTS_query
// ===========================================================================

/// `simplify_EXISTS_query(root, query)` (subselect.c): remove any useless stuff
/// in an EXISTS's subquery.  Returns true if it was able to discard the
/// targetlist, else false.  Mutates `query` in place.
fn simplify_EXISTS_query<'mcx>(
    root: &PlannerInfo,
    mcx: Mcx<'mcx>,
    query: &mut Query<'mcx>,
) -> PgResult<bool> {
    // We don't try to simplify at all if the query uses set operations,
    // aggregates, grouping sets, SRFs, modifying CTEs, HAVING, OFFSET, or FOR
    // UPDATE/SHARE.
    if query.commandType != CmdType::CMD_SELECT
        || query.setOperations.is_some()
        || query.hasAggs
        || !query.groupingSets.is_empty()
        || query.hasWindowFuncs
        || query.hasTargetSRFs
        || query.hasModifyingCTE
        || query.havingQual.is_some()
        || query.limitOffset.is_some()
        || !query.rowMarks.is_empty()
    {
        return Ok(false);
    }

    // LIMIT with a constant positive (or NULL) value doesn't affect the
    // semantics of EXISTS, so let's ignore such clauses.
    if query.limitCount.is_some() {
        // The LIMIT clause has not yet been through eval_const_expressions, so
        // we have to apply that here.  It might seem like this is a waste of
        // cycles, since the only case plausibly worth worrying about is "LIMIT
        // 1" ... but what we'll actually see is "LIMIT int8(1::int4)", so we
        // have to fold constants or we're not going to recognize it.
        let limit = query.limitCount.take().unwrap();
        let limit_node = PgBox::into_inner(limit);
        let folded: Option<Expr> = match limit_node {
            Node::Expr(e) => Some(
                backend_optimizer_util_clauses::fold::eval_const_expressions(mcx, e)?,
            ),
            // A non-Expr limit node would be a malformed parse tree; the C
            // always sees an expression here.
            other => panic!(
                "simplify_EXISTS_query: limitCount is not an expression node: {:?}",
                other.node_tag()
            ),
        };

        // Might as well update the query if we simplified the clause.
        let keep = match &folded {
            Some(Expr::Const(limit_const)) => {
                // Assert(limit->consttype == INT8OID)
                debug_assert!(limit_const.consttype == types_core::catalog::INT8OID);
                // !limit->constisnull && DatumGetInt64(limit->constvalue) <= 0
                !(!limit_const.constisnull && datum_get_int64(&limit_const.constvalue) <= 0)
            }
            // Not a Const → can't simplify.
            _ => false,
        };

        if !keep {
            // Restore the (possibly folded) limitCount and bail.
            query.limitCount = folded.map(|e| alloc_box(mcx, Node::Expr(e)));
            return Ok(false);
        }

        // Whether or not the targetlist is safe, we can drop the LIMIT.
        query.limitCount = None;
    }

    // Otherwise, we can throw away the targetlist, as well as any GROUP,
    // WINDOW, DISTINCT, and ORDER BY clauses.
    query.targetList = PgVec::new_in(mcx);
    query.groupClause = PgVec::new_in(mcx);
    query.windowClause = PgVec::new_in(mcx);
    query.distinctClause = PgVec::new_in(mcx);
    query.sortClause = PgVec::new_in(mcx);
    query.hasDistinctOn = false;

    // Since we have thrown away the GROUP BY clauses, we'd better remove the
    // RTE_GROUP RTE and clear the hasGroupRTE flag.
    for i in 0..query.rtable.len() {
        if query.rtable[i].rtekind == RTEKind::RTE_GROUP {
            debug_assert!(query.hasGroupRTE);
            query.rtable.remove(i);
            query.hasGroupRTE = false;
            break;
        }
    }

    Ok(true)
}

// ===========================================================================
// replace_empty_jointree (prepjointree.c) — small non-recursive helper.
// ===========================================================================

/// `replace_empty_jointree(parse)` (prepjointree.c): if the Query's jointree is
/// empty, replace it with a dummy RTE_RESULT relation.  Does not recurse.
pub fn replace_empty_jointree<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    // Nothing to do if jointree is already nonempty.
    {
        let jt = parse
            .jointree
            .as_ref()
            .expect("replace_empty_jointree: no jointree");
        if !jt.fromlist.is_empty() {
            return Ok(());
        }
    }

    // We mustn't change it in the top level of a setop tree, either.
    if parse.setOperations.is_some() {
        return Ok(());
    }

    // Create suitable RTE.
    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTEKind::RTE_RESULT;
    rte.eref = Some(alloc_in(
        mcx,
        make_alias(mcx, "*RESULT*", PgVec::new_in(mcx))?,
    )?);

    // Add it to rangetable.
    parse.rtable.push(rte);
    let rti = parse.rtable.len() as i32;

    // And jam a reference into the jointree.
    let rtr = RangeTblRef { rtindex: rti };
    let jt = parse.jointree.as_mut().unwrap();
    jt.fromlist.push(alloc_in(mcx, Node::RangeTblRef(rtr))?);
    Ok(())
}

// ===========================================================================
// combine_range_tables (rewriteManip.c CombineRangeTables) — local copy.
// ===========================================================================

/// `CombineRangeTables(&dst_rtable, &dst_perminfos, src_rtable, src_perminfos)`
/// (rewriteManip.c).  Appends `src`'s RTEs into `parse`'s rtable and `src`'s
/// RTEPermissionInfos into `parse`'s rteperminfos, fixing up the moved RTEs'
/// `perminfoindex` by the prior length of `dst_perminfos`.
///
/// (Not yet ported in `backend-rewrite-core`, which defers it to the rule
/// rewriter; this is a faithful local copy for the EXISTS pull-up.)
pub fn combine_range_tables<'mcx>(mcx: Mcx<'mcx>, parse: &mut Query<'mcx>, src: &mut Query<'mcx>) {
    let offset = parse.rteperminfos.len() as Index;

    if offset > 0 {
        for rte in src.rtable.iter_mut() {
            if rte.perminfoindex > 0 {
                rte.perminfoindex += offset;
            }
        }
    }

    // *dst_perminfos = list_concat(*dst_perminfos, src_perminfos);
    let src_perminfos = core::mem::replace(&mut src.rteperminfos, PgVec::new_in(mcx));
    for pi in src_perminfos {
        parse.rteperminfos.push(pi);
    }
    // *dst_rtable = list_concat(*dst_rtable, src_rtable);
    let src_rtable = core::mem::replace(&mut src.rtable, PgVec::new_in(mcx));
    for rte in src_rtable {
        parse.rtable.push(rte);
    }
}

// ===========================================================================
// small local helpers
// ===========================================================================

// ---------------------------------------------------------------------------
// Relids (types-pathnodes Bitmapset) set algebra — the same small word-vector
// helpers var.c's port keeps private. `pull_varnos` hands back this `Relids`
// type, so the bms ops on it live here (the canonical bms unit's ops are over
// the *other* (`types-nodes`) Bitmapset).
// ---------------------------------------------------------------------------

const BITS_PER_WORD: usize = 64;
#[inline]
fn wordnum(x: i32) -> usize {
    (x as usize) / BITS_PER_WORD
}
#[inline]
fn bitnum(x: i32) -> usize {
    (x as usize) % BITS_PER_WORD
}

/// `bms_is_empty(a)` — the canonical empty set is `None`/all-zero words.
fn relids_is_empty(a: &Relids) -> bool {
    match a {
        None => true,
        Some(b) => b.words.iter().all(|&w| w == 0),
    }
}

/// `bms_is_subset(a, b)` — is every member of `a` also in `b`?
fn relids_is_subset(a: &Relids, b: &Relids) -> bool {
    let aw: &[u64] = match a {
        None => return true,
        Some(a) => &a.words,
    };
    let bw: &[u64] = match b {
        None => &[],
        Some(b) => &b.words,
    };
    for (i, &w) in aw.iter().enumerate() {
        let bb = if i < bw.len() { bw[i] } else { 0 };
        if (w & !bb) != 0 {
            return false;
        }
    }
    true
}

/// `bms_add_member(a, x)` — add member `x` to `a`, recycling `a`.
fn relids_add_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let mut bms = a.unwrap_or_else(|| Box::new(Bitmapset { words: alloc::vec::Vec::new() }));
    let wnum = wordnum(x);
    if wnum >= bms.words.len() {
        bms.words.resize(wnum + 1, 0);
    }
    bms.words[wnum] |= 1u64 << bitnum(x);
    Some(bms)
}

/// `bms_next_member(a, prevbit)` — the next set bit > `prevbit`, or -2 (the C
/// returns -2 when exhausted; callers test `>= 0`).
fn relids_next_member(a: &Relids, prevbit: i32) -> i32 {
    let words: &[u64] = match a {
        None => return -2,
        Some(b) => &b.words,
    };
    let mut bit = prevbit + 1;
    while (wordnum(bit)) < words.len() {
        let wn = wordnum(bit);
        let w = words[wn] >> bitnum(bit);
        if w != 0 {
            return bit + w.trailing_zeros() as i32;
        }
        // advance to the start of the next word
        bit = ((wn + 1) * BITS_PER_WORD) as i32;
    }
    -2
}

/// `DatumGetInt64(d)` over the repo's by-value `Datum`.
#[inline]
fn datum_get_int64(d: &types_tuple::backend_access_common_heaptuple::Datum<'_>) -> i64 {
    d.as_usize() as i64
}

/// Allocate a `Node` into `mcx` as a boxed pointer.
#[inline]
fn alloc_box<'mcx>(mcx: Mcx<'mcx>, n: Node<'mcx>) -> types_nodes::nodes::NodePtr<'mcx> {
    alloc_in(mcx, n).expect("alloc_box: out of context memory")
}
