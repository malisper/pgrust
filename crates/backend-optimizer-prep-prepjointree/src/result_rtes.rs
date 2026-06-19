//! `prepjointree.c` FAMILY 5 — `remove_useless_result_rtes` and the
//! PlaceHolderVar-dependency / nullingrels machinery.
//!
//! 1:1 port over the repo's lifetime-free owned `Query<'mcx>` + embedded-`PgBox`
//! jointree model, the central `Node`-level tree walkers
//! ([`backend_nodes_core::node_walker`]), and the rewrite-core
//! `remove_nulling_relids` engine.
//!
//! ## Model notes
//!
//! * The C entry takes `PlannerInfo *root` and reads/mutates `root->parse`
//!   (the top `Query`), `root->append_rel_list`, `root->rowMarks`, and (for PHV
//!   bookkeeping) `root->glob->lastPHId`. Here `PlannerInfo` is lifetime-free;
//!   the top `Query` is threaded as a distinct `&mut Query` alongside `&mut
//!   PlannerInfo` (the planner driver resolves it), so no aliasing conflict.
//! * The jointree is the embedded `Option<PgBox<FromExpr>>` / `Vec<NodePtr>` /
//!   `JoinExpr.larg`/`rarg`, walked by deref as the C walks `Node *`.
//!   `remove_useless_results_recurse` consumes the node by value and returns its
//!   replacement (the C `return jtnode`), which the caller stores back.
//! * `f->quals` / `j->quals` are implicit-AND lists by this stage; a qual is
//!   `Option<NodePtr>` over a [`Node::List`]. The C `list_concat(child, parent)`
//!   is the concatenation of those element vectors (child first), realized by
//!   [`concat_quals`].
//! * `substitute_phv_relids` mutates `PlaceHolderVar.phrels` (the lifetime-free
//!   [`ExprRelids`]) in place over the parse tree and over each
//!   `AppendRelInfo.translated_vars` element — carried as arena
//!   [`NodeId`](types_pathnodes::NodeId) handles, resolved through
//!   `root.node_arena` and written back (same arena-walk pattern FAMILY 4's
//!   `remove_nulling_relids_in_append_rel_list` uses).
//! * `subrelids` is the `'mcx`-arena [`Bitmapset`]; PHV relid edits happen on
//!   the lifetime-free [`ExprRelids`], so [`relids_to_expr_relids`] bridges the
//!   one direction this pass needs.

use alloc::vec::Vec;

use backend_nodes_core::bitmapset::{
    bms_add_member, bms_is_empty, bms_join, bms_make_singleton, bms_next_member,
};
use backend_nodes_core::node_walker::{
    expression_tree_walker, expression_tree_walker_mut, query_tree_mutator, query_tree_walker,
    range_table_entry_walker,
};
use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::copy_query::Query;
use types_nodes::jointype::JoinType;
use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::parsenodes::RTEKind;
use types_nodes::primnodes::ExprRelids;
use types_pathnodes::{NodeId, PlannerInfo};

/// C `Relids` = `Bitmapset *`: the `'mcx`-arena relid set (NULL/empty = `None`).
type Relids<'mcx> = Option<PgBox<'mcx, Bitmapset<'mcx>>>;

/// A dummy placeholder jointree node used while moving a node out of a `&mut`
/// slot (the slot is always overwritten before being read again).
#[inline]
fn dummy_node<'mcx>(mcx: Mcx<'mcx>) -> Node<'mcx> {
    Node::mk_range_tbl_ref(mcx, types_nodes::rawnodes::RangeTblRef { rtindex: 0 })
}

/// Convert an `'mcx`-arena [`Bitmapset`] relid set to the lifetime-free
/// [`ExprRelids`] the nulling rewriter / PHV editor consumes (both are `Vec<u64>`
/// word storage; the empty/NULL set is an empty word vector).
fn relids_to_expr_relids(a: Option<&Bitmapset>) -> ExprRelids {
    match a {
        None => ExprRelids { words: Vec::new() },
        Some(bms) => {
            let mut words: Vec<u64> = Vec::with_capacity(bms.words.len());
            for &w in bms.words.iter() {
                words.push(w);
            }
            while let Some(&last) = words.last() {
                if last == 0 {
                    words.pop();
                } else {
                    break;
                }
            }
            ExprRelids { words }
        }
    }
}

/// `ExprRelids` membership test (lifetime-free word-vector analogue of
/// `bms_is_member`).
#[inline]
fn expr_relids_is_member(a: &ExprRelids, x: i32) -> bool {
    debug_assert!(x >= 0);
    let wnum = (x / 64) as usize;
    let bit = (x % 64) as u32;
    a.words.get(wnum).is_some_and(|&w| (w & (1u64 << bit)) != 0)
}

// ===========================================================================
// remove_useless_result_rtes (prepjointree.c:3596)
// ===========================================================================

/// `remove_useless_result_rtes(root)` (prepjointree.c:3596). Attempt to remove
/// `RTE_RESULT` RTEs from the join tree and elide single-child `FromExpr`s.
pub fn remove_useless_result_rtes<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    let mut dropped_outer_joins: Relids = None;

    // Top level of jointree must always be a FromExpr.
    debug_assert!(parse.jointree.is_some());

    // Recurse. Take the top FromExpr out, wrap as a Node, walk it, store the
    // (possibly replaced) result back. The recursion's single-child elision is
    // guarded by `is_top`, so the top result is still a FromExpr.
    let jt = parse
        .jointree
        .take()
        .expect("remove_useless_result_rtes: top jointree must be a FromExpr");
    let jt_node = Node::mk_from_expr(mcx, PgBox::into_inner(jt));
    let new_top = remove_useless_results_recurse(
        mcx,
        root,
        parse,
        jt_node,
        None,
        &mut dropped_outer_joins,
        true,
    )?;
    let f = match new_top.into_fromexpr() {
        Some(f) => f,
        None => panic!("remove_useless_result_rtes: top jointree node is no longer a FromExpr"),
    };
    parse.jointree = Some(mcx::alloc_in(mcx, f)?);

    // If we removed outer-join nodes, remove references to those joins as
    // nulling rels (in PHVs pulled up from the original subquery). Kosher
    // because we now know such an outer join wouldn't really have nulled.
    if !bms_is_empty(dropped_outer_joins.as_deref()) {
        let removable = relids_to_expr_relids(dropped_outer_joins.as_deref());
        let empty = ExprRelids { words: Vec::new() };
        backend_rewrite_core::remove_nulling_relids_in_query(parse, &removable, &empty);
        remove_nulling_relids_in_append_rel_list(root, &removable, &empty);
    }

    // Remove any PlanRowMark referencing an RTE_RESULT RTE: required for ones we
    // just removed, allowed (and beneficial) for surviving ones. `root->rowMarks`
    // is a `List *` of `PlanRowMark *`; in this repo it is carried as a
    // `Vec<NodeId>` of opaque handles with no backing store yet (PlanRowMarks are
    // produced by `preprocess_rowmarks` in planmain.c, still unported, which runs
    // before this pass). The list is therefore always empty on every currently
    // reachable path; when it is non-empty we cannot resolve `rc->rti` to filter
    // by RTE_RESULT, so we seam-and-panic rather than silently skip required
    // removals (the PlanRowMark-carrier keystone must land first).
    if !root.rowMarks.is_empty() {
        panic!(
            "remove_useless_result_rtes: root.rowMarks PlanRowMark filtering not yet ported — \
             PlanRowMark is carried as an unresolved NodeId handle (no arena store / `rti` \
             accessor); needs the PlanRowMark-carrier keystone (preprocess_rowmarks owner)"
        );
    }

    Ok(())
}

// ===========================================================================
// remove_useless_results_recurse (prepjointree.c:3669)
// ===========================================================================

/// `remove_useless_results_recurse(root, jtnode, parent_quals, dropped_outer_joins)`
/// (prepjointree.c:3669). Recursively process the jointree and return a modified
/// jointree; the RT indexes of removed outer-join nodes are added to
/// `*dropped_outer_joins`.
///
/// `parent_quals` is the (possibly several-levels-up) parent's quals slot into
/// which child quals may be hoisted, or `None` when that is not valid. `is_top`
/// marks the literal `root->parse->jointree` node for the elision guard.
#[allow(clippy::too_many_arguments)]
fn remove_useless_results_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Node<'mcx>,
    parent_quals: Option<&mut Option<NodePtr<'mcx>>>,
    dropped_outer_joins: &mut Relids<'mcx>,
    is_top: bool,
) -> PgResult<Node<'mcx>> {
    match jtnode.node_tag() {
        ntag::T_RangeTblRef => {
            // Can't immediately do anything with a RangeTblRef.
            Ok(jtnode)
        }
        ntag::T_FromExpr => remove_useless_results_recurse_fromexpr(
            mcx,
            root,
            parse,
            jtnode,
            parent_quals,
            dropped_outer_joins,
            is_top,
        ),
        ntag::T_JoinExpr => remove_useless_results_recurse_joinexpr(
            mcx,
            root,
            parse,
            jtnode,
            parent_quals,
            dropped_outer_joins,
        ),
        _ => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// The `IsA(jtnode, FromExpr)` arm of [`remove_useless_results_recurse`].
#[allow(clippy::too_many_arguments)]
fn remove_useless_results_recurse_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Node<'mcx>,
    parent_quals: Option<&mut Option<NodePtr<'mcx>>>,
    dropped_outer_joins: &mut Relids<'mcx>,
    is_top: bool,
) -> PgResult<Node<'mcx>> {
    let mut result_relids: Relids = None;

    let mut f = jtnode.into_fromexpr().unwrap();

    // We can drop RTE_RESULT rels from the fromlist so long as at least one
    // child remains, since joining to a one-row table changes nothing. (But we
    // can't drop one that computes PHV(s) needed by a sibling.) The C deletes
    // in-place with `foreach_delete_current` while walking, re-checking
    // `list_length(f->fromlist) > 1` against the shrinking list and passing the
    // shrinking `f` to `find_dependent_phvs_in_jointree`; we mirror that with an
    // index walk that removes the current element on a drop and does not advance.
    let mut i = 0usize;
    while i < f.fromlist.len() {
        // Recursively transform child, allowing it to push up quals into f.quals.
        let child = core::mem::replace(&mut *f.fromlist[i], dummy_node(mcx));
        let child = remove_useless_results_recurse(
            mcx,
            root,
            parse,
            child,
            Some(&mut f.quals),
            dropped_outer_joins,
            false,
        )?;
        *f.fromlist[i] = child;

        let varno = get_result_relid(root, parse, &f.fromlist[i]);
        if f.fromlist.len() > 1
            && varno != 0
            && !find_dependent_phvs_in_jointree_fromexpr(mcx, root, parse, &f, varno)?
        {
            f.fromlist.remove(i);
            result_relids = Some(bms_add_member(mcx, result_relids, varno)?);
            // Do not advance; the next element shifted into index `i`.
        } else {
            i += 1;
        }
    }

    // Clean up if we dropped any RTE_RESULT RTEs. The new jointree location for
    // their PHVs is this (post-drop) FromExpr.
    if !bms_is_empty(result_relids.as_deref()) {
        let mut varno = -1;
        loop {
            varno = bms_next_member(result_relids.as_deref(), varno);
            if varno < 0 {
                break;
            }
            remove_result_refs_fromexpr(mcx, root, parse, &f, varno)?;
        }
    }

    // If the FromExpr now has only one child, try to elide it.
    if f.fromlist.len() == 1 && !is_top && (f.quals.is_none() || parent_quals.is_some()) {
        // Merge any quals up to parent (child quals first).
        if f.quals.is_some() {
            let pq = parent_quals.expect("elision requires parent_quals when quals present");
            let merged = concat_quals(mcx, f.quals.take(), pq.take())?;
            *pq = merged;
        }
        // return (Node *) linitial(f->fromlist)
        let child = core::mem::replace(&mut *f.fromlist[0], dummy_node(mcx));
        return Ok(child);
    }

    Ok(Node::mk_from_expr(mcx, f))
}

/// The `IsA(jtnode, JoinExpr)` arm of [`remove_useless_results_recurse`].
#[allow(clippy::too_many_arguments)]
fn remove_useless_results_recurse_joinexpr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    jtnode: Node<'mcx>,
    mut parent_quals: Option<&mut Option<NodePtr<'mcx>>>,
    dropped_outer_joins: &mut Relids<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut j = jtnode.into_joinexpr().unwrap();
    let jointype = j.jointype;

    // First, recurse into larg. INNER absorbs child quals into this node; LEFT
    // lets LHS-child quals be absorbed into the parent (if any); otherwise no
    // child-qual movement.
    let larg = core::mem::replace(&mut j.larg, None);
    let larg = larg.expect("remove_useless_results_recurse: JoinExpr with NULL larg");
    let new_larg = match jointype {
        JoinType::JOIN_INNER => remove_useless_results_recurse(
            mcx,
            root,
            parse,
            PgBox::into_inner(larg),
            Some(&mut j.quals),
            dropped_outer_joins,
            false,
        )?,
        JoinType::JOIN_LEFT => remove_useless_results_recurse(
            mcx,
            root,
            parse,
            PgBox::into_inner(larg),
            parent_quals.as_deref_mut(),
            dropped_outer_joins,
            false,
        )?,
        _ => remove_useless_results_recurse(
            mcx,
            root,
            parse,
            PgBox::into_inner(larg),
            None,
            dropped_outer_joins,
            false,
        )?,
    };
    j.larg = Some(mcx::alloc_in(mcx, new_larg)?);

    // Then recurse into rarg. INNER/LEFT absorb RHS-child quals into this node;
    // otherwise no movement.
    let rarg = core::mem::replace(&mut j.rarg, None);
    let rarg = rarg.expect("remove_useless_results_recurse: JoinExpr with NULL rarg");
    let new_rarg = match jointype {
        JoinType::JOIN_INNER | JoinType::JOIN_LEFT => remove_useless_results_recurse(
            mcx,
            root,
            parse,
            PgBox::into_inner(rarg),
            Some(&mut j.quals),
            dropped_outer_joins,
            false,
        )?,
        _ => remove_useless_results_recurse(
            mcx,
            root,
            parse,
            PgBox::into_inner(rarg),
            None,
            dropped_outer_joins,
            false,
        )?,
    };
    j.rarg = Some(mcx::alloc_in(mcx, new_rarg)?);

    // Apply join-type-specific optimization rules.
    let mut jtnode: Node<'mcx> = Node::mk_join_expr(mcx, j);
    match jointype {
        JoinType::JOIN_INNER => {
            // An inner join is equivalent to a FromExpr; if either side reduced
            // to an RTE_RESULT rel, replace the join with the other side. The
            // other input can't reference PHVs to be evaluated at the RESULT rel
            // (only RHSes of inner/left joins may have LATERAL refs to it).
            let j = as_joinexpr_mut(&mut jtnode);
            let larg_ref = j.larg.as_deref().unwrap();
            let varno_l = get_result_relid(root, parse, larg_ref);
            if varno_l != 0
                && !find_dependent_phvs_in_jointree_node(
                    mcx,
                    root,
                    parse,
                    j.rarg.as_deref().unwrap(),
                    varno_l,
                )?
            {
                let rarg = j.rarg.take().unwrap();
                remove_result_refs_node(mcx, root, parse, &rarg, varno_l)?;
                jtnode = inner_collapse(mcx, &mut jtnode, PgBox::into_inner(rarg), parent_quals)?;
            } else {
                let j = as_joinexpr_mut(&mut jtnode);
                let rarg_ref = j.rarg.as_deref().unwrap();
                let varno_r = get_result_relid(root, parse, rarg_ref);
                if varno_r != 0 {
                    let larg = j.larg.take().unwrap();
                    remove_result_refs_node(mcx, root, parse, &larg, varno_r)?;
                    jtnode =
                        inner_collapse(mcx, &mut jtnode, PgBox::into_inner(larg), parent_quals)?;
                }
            }
        }
        JoinType::JOIN_LEFT => {
            // Simplify if the RHS is an RTE_RESULT. If qual is empty, the join
            // strength-reduces to inner (each LHS row has exactly one partner),
            // so discard the RHS. Otherwise each LHS row is still returned
            // exactly once and the RHS yields no columns (barring PHVs), so we
            // can ignore the qual and discard the left join.
            let j = as_joinexpr_mut(&mut jtnode);
            let rarg_ref = j.rarg.as_deref().unwrap();
            let varno = get_result_relid(root, parse, rarg_ref);
            if varno != 0 && (j.quals.is_none() || !find_dependent_phvs(mcx, root, parse, varno)?) {
                let rtindex = j.rtindex;
                let larg = j.larg.take().unwrap();
                remove_result_refs_node(mcx, root, parse, &larg, varno)?;
                *dropped_outer_joins =
                    Some(bms_add_member(mcx, dropped_outer_joins.take(), rtindex)?);
                jtnode = PgBox::into_inner(larg);
            }
        }
        JoinType::JOIN_SEMI => {
            // Simplify if the RHS is an RTE_RESULT; the join qual becomes a
            // filter qual for the LHS. PHVs to be evaluated at the RHS can only
            // appear in the semijoin qual and never go null before examination;
            // remove_result_refs relabels them to the LHS, which is fine. The
            // join has no rtindex to scrub.
            let j = as_joinexpr_mut(&mut jtnode);
            let rarg_ref = j.rarg.as_deref().unwrap();
            let varno = get_result_relid(root, parse, rarg_ref);
            if varno != 0 {
                debug_assert_eq!(j.rtindex, 0);
                let larg = j.larg.take().unwrap();
                remove_result_refs_node(mcx, root, parse, &larg, varno)?;
                jtnode = inner_collapse(mcx, &mut jtnode, PgBox::into_inner(larg), parent_quals)?;
            }
        }
        JoinType::JOIN_FULL | JoinType::JOIN_ANTI => {
            // No special smarts for these cases.
        }
        _ => {
            // JOIN_RIGHT should be gone at this point.
            return Err(types_error::PgError::error("unrecognized join type"));
        }
    }

    Ok(jtnode)
}

/// `&mut JoinExpr` view of a `Node::JoinExpr`.
#[inline]
fn as_joinexpr_mut<'a, 'mcx>(n: &'a mut Node<'mcx>) -> &'a mut types_nodes::rawnodes::JoinExpr<'mcx> {
    n.as_joinexpr_mut().unwrap_or_else(|| unreachable!("expected JoinExpr"))
}

/// The shared INNER/SEMI "collapse to the surviving side" tail (the C
/// `makeFromExpr(list_make1(side), j->quals)` / merge-to-parent block). `jtnode`
/// is the JoinExpr being collapsed; `side` is the surviving child. Returns the
/// replacement node.
fn inner_collapse<'mcx>(
    mcx: Mcx<'mcx>,
    jtnode: &mut Node<'mcx>,
    side: Node<'mcx>,
    parent_quals: Option<&mut Option<NodePtr<'mcx>>>,
) -> PgResult<Node<'mcx>> {
    let j = as_joinexpr_mut(jtnode);
    let quals = j.quals.take();
    if quals.is_some() && parent_quals.is_none() {
        // makeFromExpr(list_make1(side), j->quals)
        let mut fromlist = mcx::PgVec::new_in(mcx);
        fromlist.try_reserve(1).map_err(|_| mcx.oom(1))?;
        fromlist.push(mcx::alloc_in(mcx, side)?);
        Ok(Node::mk_from_expr(mcx, types_nodes::rawnodes::FromExpr { fromlist, quals }))
    } else {
        // Merge any quals up to parent, return the surviving side.
        if let Some(pq) = parent_quals {
            if quals.is_some() {
                let merged = concat_quals(mcx, quals, pq.take())?;
                *pq = merged;
            }
        }
        Ok(side)
    }
}

// ===========================================================================
// get_result_relid (prepjointree.c:3936)
// ===========================================================================

/// `get_result_relid(root, jtnode)` (prepjointree.c:3936). If `jtnode` is a
/// `RangeTblRef` for an `RTE_RESULT` RTE, return its relid; otherwise 0.
fn get_result_relid(_root: &PlannerInfo, parse: &Query, jtnode: &Node) -> i32 {
    let varno = match jtnode.as_rangetblref() {
        Some(r) => r.rtindex,
        None => return 0,
    };
    if parse.rtable[(varno - 1) as usize].rtekind != RTEKind::RTE_RESULT {
        return 0;
    }
    varno
}

// ===========================================================================
// remove_result_refs (prepjointree.c:3970)
// ===========================================================================

/// `remove_result_refs(root, varno, (Node *) f)` where the new jointree location
/// is a `FromExpr` value owned by the caller (the post-drop FromExpr).
fn remove_result_refs_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    f: &types_nodes::rawnodes::FromExpr<'mcx>,
    varno: i32,
) -> PgResult<()> {
    if last_ph_id(root) != 0 {
        let subrelids = get_relids_in_fromexpr(mcx, f, true, false)?;
        debug_assert!(!bms_is_empty(subrelids.as_deref()));
        let sub = relids_to_expr_relids(subrelids.as_deref());
        substitute_phv_relids_in_query(parse, varno, &sub);
        fix_append_rel_relids(mcx, root, varno, subrelids.as_deref(), &sub)?;
    }
    Ok(())
}

/// `remove_result_refs(root, varno, newjtloc)` where `newjtloc` is a jointree
/// `Node` value owned by the caller (an `larg`/`rarg` of a collapsing join).
fn remove_result_refs_node<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    newjtloc: &Node<'mcx>,
    varno: i32,
) -> PgResult<()> {
    if last_ph_id(root) != 0 {
        let subrelids = get_relids_in_jointree(mcx, newjtloc, true, false)?;
        debug_assert!(!bms_is_empty(subrelids.as_deref()));
        let sub = relids_to_expr_relids(subrelids.as_deref());
        substitute_phv_relids_in_query(parse, varno, &sub);
        fix_append_rel_relids(mcx, root, varno, subrelids.as_deref(), &sub)?;
    }
    Ok(())
}

// ===========================================================================
// find_dependent_phvs (prepjointree.c:4009)
// ===========================================================================

/// `find_dependent_phvs_context` (prepjointree.c:4002).
struct FindDependentPhvsContext {
    /// `Relids relids` — held as the single target varno (the C builds a
    /// singleton via `bms_make_singleton(varno)` and compares with `bms_equal`).
    varno: i32,
    sublevels_up: i32,
}

/// `find_dependent_phvs_walker` (prepjointree.c:4009).
fn find_dependent_phvs_walker(node: &Node, context: &mut FindDependentPhvsContext) -> bool {
    match node.node_tag() {
        ntag::T_PlaceHolderVar => {
            let phv = node.expect_placeholdervar();
            if phv.phlevelsup as i32 == context.sublevels_up
                && expr_relids_is_singleton(&phv.phrels, context.varno)
            {
                return true;
            }
            // fall through to examine children
            expression_tree_walker(node, &mut |n| find_dependent_phvs_walker(n, context))
        }
        ntag::T_Query => {
            let q = node.expect_query();
            context.sublevels_up += 1;
            let result =
                query_tree_walker(q, &mut |n| find_dependent_phvs_walker(n, context), 0);
            context.sublevels_up -= 1;
            result
        }
        _ => expression_tree_walker(node, &mut |n| find_dependent_phvs_walker(n, context)),
    }
}

/// `find_dependent_phvs(root, varno)` (prepjointree.c:4048). Are there any PHVs
/// whose relids are exactly `{varno}` anywhere in the Query (and append_rel_list)?
fn find_dependent_phvs<'mcx>(
    _mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    parse: &Query<'mcx>,
    varno: i32,
) -> PgResult<bool> {
    if last_ph_id(root) == 0 {
        return Ok(false);
    }
    let mut context = FindDependentPhvsContext {
        varno,
        sublevels_up: 0,
    };
    if query_tree_walker(parse, &mut |n| find_dependent_phvs_walker(n, &mut context), 0) {
        return Ok(true);
    }
    // The append_rel_list could be populated already; check translated_vars too.
    if find_dependent_phvs_in_append_rel_list(root, &mut context) {
        return Ok(true);
    }
    Ok(false)
}

/// `expression_tree_walker((Node *) root->append_rel_list, ...)` over the arena
/// `translated_vars` Exprs.
fn find_dependent_phvs_in_append_rel_list(
    root: &PlannerInfo,
    context: &mut FindDependentPhvsContext,
) -> bool {
    for appinfo in root.append_rel_list.iter() {
        for &id in appinfo.translated_vars.iter() {
            if id == NodeId::default() {
                continue;
            }
            let node = Node::Expr(root.node(id).clone());
            if find_dependent_phvs_walker(&node, context) {
                return true;
            }
        }
    }
    false
}

/// `find_dependent_phvs_in_jointree(root, node, varno)` (prepjointree.c:4070)
/// over a jointree `Node`.
fn find_dependent_phvs_in_jointree_node<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    parse: &Query<'mcx>,
    node: &Node<'mcx>,
    varno: i32,
) -> PgResult<bool> {
    if last_ph_id(root) == 0 {
        return Ok(false);
    }
    let mut context = FindDependentPhvsContext {
        varno,
        sublevels_up: 0,
    };
    // See if the jointree fragment itself contains references (join quals).
    if find_dependent_phvs_walker(node, &mut context) {
        return Ok(true);
    }
    let subrelids = get_relids_in_jointree(mcx, node, false, false)?;
    find_dependent_phvs_check_rtes(parse, subrelids.as_deref(), &mut context)
}

/// `find_dependent_phvs_in_jointree` applied to a `FromExpr` value (the
/// `(Node *) f` argument).
fn find_dependent_phvs_in_jointree_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    parse: &Query<'mcx>,
    f: &types_nodes::rawnodes::FromExpr<'mcx>,
    varno: i32,
) -> PgResult<bool> {
    if last_ph_id(root) == 0 {
        return Ok(false);
    }
    let mut context = FindDependentPhvsContext {
        varno,
        sublevels_up: 0,
    };
    // Walk the FromExpr's own quals + fromlist (the join-qual fragment).
    if walk_fromexpr(f, &mut |n| find_dependent_phvs_walker(n, &mut context)) {
        return Ok(true);
    }
    let subrelids = get_relids_in_fromexpr(mcx, f, false, false)?;
    find_dependent_phvs_check_rtes(parse, subrelids.as_deref(), &mut context)
}

/// Shared RTE tail of `find_dependent_phvs_in_jointree`: tediously check each
/// LATERAL RTE in `subrelids`.
fn find_dependent_phvs_check_rtes(
    parse: &Query,
    subrelids: Option<&Bitmapset>,
    context: &mut FindDependentPhvsContext,
) -> PgResult<bool> {
    let mut relid = -1;
    loop {
        relid = bms_next_member(subrelids, relid);
        if relid < 0 {
            break;
        }
        let rte = &parse.rtable[(relid - 1) as usize];
        if rte.lateral
            && range_table_entry_walker(rte, &mut |n| find_dependent_phvs_walker(n, context), 0)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Walk a `FromExpr`'s quals + fromlist children with `walker` (the
/// `expression_tree_walker`/`query_tree_walker` `FromExpr` child set).
fn walk_fromexpr(f: &types_nodes::rawnodes::FromExpr, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
    for l in f.fromlist.iter() {
        if walker(l) {
            return true;
        }
    }
    if let Some(q) = f.quals.as_deref() {
        if walker(q) {
            return true;
        }
    }
    false
}

/// `ExprRelids` equality with a singleton `{x}` (the C
/// `bms_equal(bms_make_singleton(varno), phv->phrels)`).
fn expr_relids_is_singleton(a: &ExprRelids, x: i32) -> bool {
    debug_assert!(x >= 0);
    let wnum = (x / 64) as usize;
    let bit = (x % 64) as u32;
    for (i, &w) in a.words.iter().enumerate() {
        let expect = if i == wnum { 1u64 << bit } else { 0 };
        if w != expect {
            return false;
        }
    }
    // Any trailing implicit-zero words past the singleton word are fine; the
    // singleton word itself must be present.
    a.words.get(wnum).copied().unwrap_or(0) == (1u64 << bit)
}

// ===========================================================================
// substitute_phv_relids (prepjointree.c:4146)
// ===========================================================================

/// `substitute_phv_relids_context` (prepjointree.c:4108).
struct SubstitutePhvRelidsContext<'a> {
    varno: i32,
    sublevels_up: i32,
    subrelids: &'a ExprRelids,
}

/// `substitute_phv_relids_walker` (prepjointree.c:4117). Modifies PHV nodes in
/// place (the C "cheat and modify in-place" mutator).
/// Recurse into a node's children via the in-place walker with a per-call
/// scratch arena for its transient `Node::Expr` wrappers. The walk never
/// allocates; the `Mcx` is threaded only so the future opaque-`Node` flip's
/// `mk_expr` has a context. Freed on return.
fn substitute_walk_children(
    node: &mut Node,
    context: &mut SubstitutePhvRelidsContext,
) -> bool {
    let scratch = mcx::MemoryContext::new("substitute_phv_relids scratch");
    let mcx = scratch.mcx();
    expression_tree_walker_mut(node, &mut |n| substitute_phv_relids_walker(n, context), mcx)
}

fn substitute_phv_relids_walker(node: &mut Node, context: &mut SubstitutePhvRelidsContext) -> bool {
    match node.node_tag() {
        ntag::T_PlaceHolderVar => {
            let phv = node.as_placeholdervar_mut().unwrap();
            if phv.phlevelsup as i32 == context.sublevels_up
                && expr_relids_is_member(&phv.phrels, context.varno)
            {
                // phrels = (phrels ∪ subrelids) \ {varno}
                expr_relids_union_in_place(&mut phv.phrels, context.subrelids);
                expr_relids_del_member(&mut phv.phrels, context.varno);
                debug_assert!(!phv.phrels.words.iter().all(|&w| w == 0));
            }
            // fall through to examine children
            substitute_walk_children(node, context)
        }
        ntag::T_Query => {
            let q = node.as_query_mut().unwrap();
            context.sublevels_up += 1;
            let result =
                query_tree_mutator(q, &mut |n| substitute_phv_relids_walker(n, context), 0);
            context.sublevels_up -= 1;
            result
        }
        _ => substitute_walk_children(node, context),
    }
}

/// `substitute_phv_relids((Node *) query, varno, subrelids)` (prepjointree.c:4146)
/// applied to the top `&mut Query` (level 0, no sublevels bump).
pub(crate) fn substitute_phv_relids_in_query(query: &mut Query, varno: i32, subrelids: &ExprRelids) {
    let mut context = SubstitutePhvRelidsContext {
        varno,
        sublevels_up: 0,
        subrelids,
    };
    query_tree_mutator(query, &mut |n| substitute_phv_relids_walker(n, &mut context), 0);
}

/// `substitute_phv_relids((Node *) node, varno, subrelids)` applied to a bare
/// expression `&mut Node`.
fn substitute_phv_relids_in_node(node: &mut Node, varno: i32, subrelids: &ExprRelids) {
    let mut context = SubstitutePhvRelidsContext {
        varno,
        sublevels_up: 0,
        subrelids,
    };
    // query_or_expression_tree_walker for a bare expr visits the node itself.
    if substitute_phv_relids_walker(node, &mut context) {}
}

// ===========================================================================
// fix_append_rel_relids (prepjointree.c:4174)
// ===========================================================================

/// `fix_append_rel_relids(root, varno, subrelids)` (prepjointree.c:4174). Update
/// RT-index fields of AppendRelInfo nodes and fix PHVs in their translated_vars.
///
/// `subrelids_bms` is the `'mcx` Relids (for `bms_singleton_member`); `subrelids`
/// is its lifetime-free [`ExprRelids`] form (for the PHV editor).
pub(crate) fn fix_append_rel_relids<'mcx>(
    _mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    varno: i32,
    subrelids_bms: Option<&Bitmapset>,
    subrelids: &ExprRelids,
) -> PgResult<()> {
    let mut subvarno: i32 = -1;
    let has_phvs = last_ph_id(root) != 0;

    // First pass: fix child_relid integers and remember which appinfos need PHV
    // fixups. We can't hold a borrow of append_rel_list while mutating the arena,
    // so collect the translated_vars Nodeids to edit after.
    let mut to_fix: Vec<NodeId> = Vec::new();
    for appinfo in root.append_rel_list.iter_mut() {
        debug_assert_ne!(appinfo.parent_relid as i32, varno);
        if appinfo.child_relid as i32 == varno {
            if subvarno < 0 {
                subvarno = backend_nodes_core::bitmapset::bms_singleton_member(subrelids_bms);
            }
            appinfo.child_relid = subvarno as u32;
        }
        if has_phvs {
            for &id in appinfo.translated_vars.iter() {
                if id != NodeId::default() {
                    to_fix.push(id);
                }
            }
        }
    }
    // Second pass: fix PHVs in the translated_vars arena Exprs.
    for id in to_fix {
        let mut node = Node::Expr(root.node(id).clone());
        substitute_phv_relids_in_node(&mut node, varno, subrelids);
        if let Some(e) = node.into_expr() {
            *root.node_mut(id) = e;
        }
    }
    Ok(())
}

// ===========================================================================
// get_relids_in_jointree (prepjointree.c:4234)
// ===========================================================================

/// `get_relids_in_jointree(jtnode, include_outer_joins, include_inner_joins)`
/// (prepjointree.c:4234). Set of RT indexes present in a jointree.
pub fn get_relids_in_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    jtnode: &Node<'mcx>,
    include_outer_joins: bool,
    include_inner_joins: bool,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    match jtnode.node_tag() {
        ntag::T_RangeTblRef => Ok(Some(bms_make_singleton(mcx, jtnode.expect_rangetblref().rtindex)?)),
        ntag::T_FromExpr => get_relids_in_fromexpr(mcx, jtnode.expect_fromexpr(), include_outer_joins, include_inner_joins),
        ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            let mut result = get_relids_in_jointree(
                mcx,
                j.larg.as_deref().expect("JoinExpr with NULL larg"),
                include_outer_joins,
                include_inner_joins,
            )?;
            let r = get_relids_in_jointree(
                mcx,
                j.rarg.as_deref().expect("JoinExpr with NULL rarg"),
                include_outer_joins,
                include_inner_joins,
            )?;
            result = bms_join(result, r);
            if j.rtindex != 0 {
                if j.jointype == JoinType::JOIN_INNER {
                    if include_inner_joins {
                        result = Some(bms_add_member(mcx, result, j.rtindex)?);
                    }
                } else if include_outer_joins {
                    result = Some(bms_add_member(mcx, result, j.rtindex)?);
                }
            }
            Ok(result)
        }
        _ => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// `get_relids_in_jointree` applied to a `FromExpr` value (the C `(Node *) f`).
fn get_relids_in_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    f: &types_nodes::rawnodes::FromExpr<'mcx>,
    include_outer_joins: bool,
    include_inner_joins: bool,
) -> PgResult<Relids<'mcx>> {
    let mut result: Relids = None;
    for l in f.fromlist.iter() {
        let r = get_relids_in_jointree(mcx, l, include_outer_joins, include_inner_joins)?;
        result = bms_join(result, r);
    }
    Ok(result)
}

// ===========================================================================
// find_jointree_node_for_rel / get_relids_for_join (prepjointree.c:4160-4197)
// ===========================================================================

/// `find_jointree_node_for_rel(jtnode, relid)` (prepjointree.c:4160): locate the
/// jointree node (`RangeTblRef`, or a `JoinExpr` for a join RTE) whose RT index
/// is `relid`. Returns `None` if not present.
fn find_jointree_node_for_rel<'a, 'mcx>(
    jtnode: Option<&'a Node<'mcx>>,
    relid: i32,
) -> PgResult<Option<&'a Node<'mcx>>> {
    let jtnode = match jtnode {
        None => return Ok(None),
        Some(n) => n,
    };
    match jtnode.node_tag() {
        ntag::T_RangeTblRef => {
            if relid == jtnode.expect_rangetblref().rtindex {
                Ok(Some(jtnode))
            } else {
                Ok(None)
            }
        }
        ntag::T_FromExpr => {
            let f = jtnode.expect_fromexpr();
            for l in f.fromlist.iter() {
                if let Some(found) = find_jointree_node_for_rel(Some(l), relid)? {
                    return Ok(Some(found));
                }
            }
            Ok(None)
        }
        ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            if relid == j.rtindex {
                return Ok(Some(jtnode));
            }
            if let Some(found) = find_jointree_node_for_rel(j.larg.as_deref(), relid)? {
                return Ok(Some(found));
            }
            if let Some(found) = find_jointree_node_for_rel(j.rarg.as_deref(), relid)? {
                return Ok(Some(found));
            }
            Ok(None)
        }
        _ => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// `get_relids_for_join(query, joinrelid)` (prepjointree.c:4191): find the
/// jointree node for join RTE `joinrelid` and return the base+OJ relids present
/// underneath it. Returned as the lifetime-free [`ExprRelids`] so the var.c owner
/// can assign it directly into a `PlaceHolderVar.phrels`.
pub(crate) fn get_relids_for_join<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    joinrelid: i32,
) -> PgResult<ExprRelids> {
    // The C passes `(Node *) query->jointree` (a FromExpr) directly. Here the top
    // jointree is a typed `FromExpr`; search its fromlist (the FromExpr's own
    // rtindex would never be a join RTE).
    let jtnode = match query.jointree.as_deref() {
        None => None,
        Some(f) => {
            let mut found = None;
            for l in f.fromlist.iter() {
                if let Some(n) = find_jointree_node_for_rel(Some(l), joinrelid)? {
                    found = Some(n);
                    break;
                }
            }
            found
        }
    };
    let jtnode = match jtnode {
        None => {
            return Err(types_error::PgError::error(alloc::format!(
                "could not find join node {joinrelid}"
            )))
        }
        Some(n) => n,
    };
    let relids = get_relids_in_jointree(mcx, jtnode, true, false)?;
    Ok(relids_to_expr_relids(relids.as_deref()))
}

/// `get_relids_in_jointree((Node *) query->jointree, true, false)`
/// (prepjointree.c, reached from optimizer/util/var.c `mark_nullable_by_grouping`):
/// the set of base+OJ relids present in the whole query jointree. Returned as the
/// lifetime-free [`ExprRelids`].
pub(crate) fn get_relids_in_query_jointree<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
) -> PgResult<ExprRelids> {
    let relids = match query.jointree.as_deref() {
        None => None,
        Some(f) => get_relids_in_fromexpr(mcx, f, true, false)?,
    };
    Ok(relids_to_expr_relids(relids.as_deref()))
}

// ===========================================================================
// get_nullingrels (prepjointree.c:4361)
// ===========================================================================

/// `nullingrel_info` (prepjointree.c:49). For each leaf RTE, `nullingrels[rti]`
/// is the set of relids of outer joins that potentially null that RTE.
pub struct NullingrelInfo<'mcx> {
    /// `Relids *nullingrels` — index 1..=rtlength (index 0 unused, like C).
    pub nullingrels: Vec<Relids<'mcx>>,
    /// `int rtlength` — length of the range table.
    pub rtlength: i32,
}

/// `get_nullingrels(parse)` (prepjointree.c:4361). Collect, per leaf relation,
/// the set of outer joins that potentially null it.
pub fn get_nullingrels<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &Query<'mcx>,
) -> PgResult<NullingrelInfo<'mcx>> {
    let rtlength = parse.rtable.len() as i32;
    let mut nullingrels: Vec<Relids> = Vec::new();
    nullingrels.resize_with((rtlength + 1) as usize, || None);
    let mut info = NullingrelInfo {
        nullingrels,
        rtlength,
    };
    if let Some(jt) = parse.jointree.as_deref() {
        get_nullingrels_recurse_fromexpr(mcx, jt, None, &mut info)?;
    }
    Ok(info)
}

/// Recursive guts of [`get_nullingrels`] over a jointree `Node`. `upper_nullingrels`
/// is treated as a constant at each level; it may be stored directly into `info`
/// at leaf level (the C does not copy it for leaves).
fn get_nullingrels_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    jtnode: &Node<'mcx>,
    upper_nullingrels: Option<&Bitmapset<'mcx>>,
    info: &mut NullingrelInfo<'mcx>,
) -> PgResult<()> {
    match jtnode.node_tag() {
        ntag::T_RangeTblRef => {
            let varno = jtnode.expect_rangetblref().rtindex;
            debug_assert!(varno > 0 && varno <= info.rtlength);
            info.nullingrels[varno as usize] = clone_relids(mcx, upper_nullingrels)?;
            Ok(())
        }
        ntag::T_FromExpr => get_nullingrels_recurse_fromexpr(mcx, jtnode.expect_fromexpr(), upper_nullingrels, info),
        ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            let larg = j.larg.as_deref().expect("JoinExpr with NULL larg");
            let rarg = j.rarg.as_deref().expect("JoinExpr with NULL rarg");
            match j.jointype {
                JoinType::JOIN_INNER => {
                    get_nullingrels_recurse(mcx, larg, upper_nullingrels, info)?;
                    get_nullingrels_recurse(mcx, rarg, upper_nullingrels, info)?;
                }
                JoinType::JOIN_LEFT | JoinType::JOIN_SEMI | JoinType::JOIN_ANTI => {
                    let local = add_member_to_copy(mcx, upper_nullingrels, j.rtindex)?;
                    get_nullingrels_recurse(mcx, larg, upper_nullingrels, info)?;
                    get_nullingrels_recurse(mcx, rarg, local.as_deref(), info)?;
                }
                JoinType::JOIN_FULL => {
                    let local = add_member_to_copy(mcx, upper_nullingrels, j.rtindex)?;
                    get_nullingrels_recurse(mcx, larg, local.as_deref(), info)?;
                    get_nullingrels_recurse(mcx, rarg, local.as_deref(), info)?;
                }
                JoinType::JOIN_RIGHT => {
                    let local = add_member_to_copy(mcx, upper_nullingrels, j.rtindex)?;
                    get_nullingrels_recurse(mcx, larg, local.as_deref(), info)?;
                    get_nullingrels_recurse(mcx, rarg, upper_nullingrels, info)?;
                }
                _ => {
                    return Err(types_error::PgError::error("unrecognized join type"));
                }
            }
            Ok(())
        }
        _ => Err(types_error::PgError::error("unrecognized node type")),
    }
}

/// `get_nullingrels_recurse` over a `FromExpr` value (the C `(Node *) f`).
fn get_nullingrels_recurse_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    f: &types_nodes::rawnodes::FromExpr<'mcx>,
    upper_nullingrels: Option<&Bitmapset<'mcx>>,
    info: &mut NullingrelInfo<'mcx>,
) -> PgResult<()> {
    for l in f.fromlist.iter() {
        get_nullingrels_recurse(mcx, l, upper_nullingrels, info)?;
    }
    Ok(())
}

/// `bms_add_member(bms_copy(upper), rtindex)`.
fn add_member_to_copy<'mcx>(
    mcx: Mcx<'mcx>,
    upper: Option<&Bitmapset<'mcx>>,
    rtindex: i32,
) -> PgResult<Relids<'mcx>> {
    let copy = clone_relids(mcx, upper)?;
    Ok(Some(bms_add_member(mcx, copy, rtindex)?))
}

/// `bms_copy(a)` returning an owned `'mcx` Relids.
fn clone_relids<'mcx>(mcx: Mcx<'mcx>, a: Option<&Bitmapset>) -> PgResult<Relids<'mcx>> {
    backend_nodes_core::bitmapset::bms_copy(mcx, a)
}

// ===========================================================================
// remove_nulling_relids over the append_rel_list (FAMILY 4 helper, reused)
// ===========================================================================

/// `remove_nulling_relids((Node *) root->append_rel_list, removable, except)`:
/// run the expression-tree `remove_nulling_relids` over each arena
/// `translated_vars` Expr and write it back.
fn remove_nulling_relids_in_append_rel_list(
    root: &mut PlannerInfo,
    removable: &ExprRelids,
    except: &ExprRelids,
) {
    let mut ids: Vec<NodeId> = Vec::new();
    for appinfo in root.append_rel_list.iter() {
        for &id in appinfo.translated_vars.iter() {
            if id == NodeId::default() {
                continue;
            }
            ids.push(id);
        }
    }
    for id in ids {
        let mut node = Node::Expr(root.node(id).clone());
        backend_rewrite_core::remove_nulling_relids(&mut node, removable, except);
        if let Some(e) = node.into_expr() {
            *root.node_mut(id) = e;
        }
    }
}

// ===========================================================================
// small helpers
// ===========================================================================

/// `root->glob->lastPHId`.
fn last_ph_id(root: &PlannerInfo) -> u32 {
    root.glob.as_ref().map(|g| g.last_ph_id).unwrap_or(0)
}

/// `list_concat(castNode(List, child), castNode(List, parent))` over the
/// implicit-AND qual representation: both are `Option<NodePtr>` over a
/// [`Node::List`] (a NULL qual is the empty list). Child elements come first.
fn concat_quals<'mcx>(
    mcx: Mcx<'mcx>,
    child: Option<NodePtr<'mcx>>,
    parent: Option<NodePtr<'mcx>>,
) -> PgResult<Option<NodePtr<'mcx>>> {
    let mut out: mcx::PgVec<'mcx, NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
    let push_list = |q: Option<NodePtr<'mcx>>,
                         out: &mut mcx::PgVec<'mcx, NodePtr<'mcx>>|
     -> PgResult<()> {
        match q {
            None => Ok(()),
            Some(n) => {
                let node = PgBox::into_inner(n);
                if node.is_list() {
                    let items = node.into_list().unwrap();
                    out.try_reserve(items.len()).map_err(|_| mcx.oom(items.len()))?;
                    for it in items {
                        out.push(it);
                    }
                    Ok(())
                } else {
                    // Defensive: a non-List qual (C asserts implicit-AND List by
                    // now) is treated as a one-element list.
                    out.try_reserve(1).map_err(|_| mcx.oom(1))?;
                    out.push(mcx::alloc_in(mcx, node)?);
                    Ok(())
                }
            }
        }
    };
    push_list(child, &mut out)?;
    push_list(parent, &mut out)?;
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(mcx::alloc_in(mcx, Node::mk_list(mcx, out))?))
    }
}

/// `phrels = phrels ∪ subrelids` in place over the lifetime-free word vectors.
fn expr_relids_union_in_place(a: &mut ExprRelids, b: &ExprRelids) {
    if b.words.len() > a.words.len() {
        a.words.resize(b.words.len(), 0);
    }
    for (i, &w) in b.words.iter().enumerate() {
        a.words[i] |= w;
    }
}

/// `phrels = phrels \ {x}` in place.
fn expr_relids_del_member(a: &mut ExprRelids, x: i32) {
    debug_assert!(x >= 0);
    let wnum = (x / 64) as usize;
    let bit = (x % 64) as u32;
    if let Some(w) = a.words.get_mut(wnum) {
        *w &= !(1u64 << bit);
    }
    while let Some(&last) = a.words.last() {
        if last == 0 {
            a.words.pop();
        } else {
            break;
        }
    }
}
