//! `prepjointree.c` FAMILY 5 — `remove_useless_result_rtes` and the
//! PlaceHolderVar-dependency / nullingrels machinery.
//!
//! 1:1 port over the repo's lifetime-free owned `Query<'mcx>` + embedded-`PgBox`
//! jointree model, the central `Node`-level tree walkers
//! ([`::nodes_core::node_walker`]), and the rewrite-core
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
//!   [`NodeId`](::pathnodes::NodeId) handles, resolved through
//!   `root.node_arena` and written back (same arena-walk pattern FAMILY 4's
//!   `remove_nulling_relids_in_append_rel_list` uses).
//! * `subrelids` is the `'mcx`-arena [`Bitmapset`]; PHV relid edits happen on
//!   the lifetime-free [`ExprRelids`], so [`relids_to_expr_relids`] bridges the
//!   one direction this pass needs.

use alloc::vec::Vec;

use ::nodes_core::bitmapset::{
    bms_add_member, bms_is_empty, bms_join, bms_make_singleton, bms_next_member,
};
use ::nodes_core::node_walker::{
    expression_tree_walker, expression_tree_walker_mut, query_tree_mutator, query_tree_walker,
    range_table_entry_walker,
};
use mcx::{Mcx, PgBox};
use ::types_error::PgResult;
use ::nodes::bitmapset::Bitmapset;
use ::nodes::copy_query::Query;
use ::nodes::jointype::JoinType;
use ::nodes::nodes::{ntag, Node, NodePtr};
use ::nodes::parsenodes::RTEKind;
use ::nodes::primnodes::ExprRelids;
use pathnodes::{NodeId, PlannerInfo};

/// C `Relids` = `Bitmapset *`: the `'mcx`-arena relid set (NULL/empty = `None`).
type Relids<'mcx> = Option<PgBox<'mcx, Bitmapset<'mcx>>>;

/// A dummy placeholder jointree node used while moving a node out of a `&mut`
/// slot (the slot is always overwritten before being read again).
#[inline]
fn dummy_node<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Node<'mcx>> {
    Ok(Node::mk_range_tbl_ref(mcx, ::nodes::rawnodes::RangeTblRef { rtindex: 0 })?)
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
    rowmark_rtis: &[types_core::Index],
) -> PgResult<()> {
    let mut dropped_outer_joins: Relids = None;

    // Top level of jointree must always be a FromExpr.
    debug_assert!(parse.jointree.is_some());

    // Recurse over the live `parse.jointree` in place. The top FromExpr stays
    // wired into `parse` throughout (addressed as `JtPath::Top`), so
    // `remove_result_refs`'s parse-wide PHV substitution and the whole-query
    // `find_dependent_phvs` reach the in-progress jointree — matching C, which
    // aliases the whole tree into `root->parse`. The single-child elision is
    // guarded by `is_top` (the top stays a FromExpr).
    remove_useless_results_recurse(
        mcx,
        root,
        parse,
        &JtPath::Top,
        None,
        &mut dropped_outer_joins,
    )?;

    // If we removed outer-join nodes, remove references to those joins as
    // nulling rels (in PHVs pulled up from the original subquery). Kosher
    // because we now know such an outer join wouldn't really have nulled.
    if !bms_is_empty(dropped_outer_joins.as_deref()) {
        let removable = relids_to_expr_relids(dropped_outer_joins.as_deref());
        let empty = ExprRelids { words: Vec::new() };
        rewrite_core::remove_nulling_relids_in_query(parse, &removable, &empty, mcx);
        remove_nulling_relids_in_append_rel_list(mcx, root, &removable, &empty)?;
    }

    // Remove any PlanRowMark referencing an RTE_RESULT RTE.  We obviously must do
    // that for any RTE_RESULT that we just removed.  But one for a RTE that we did
    // not remove can be dropped anyway: since the RTE has only one possible output
    // row, there is no need for EPQ to mark and restore that row.
    //
    // It's necessary, not optional, to remove the PlanRowMark for a surviving
    // RTE_RESULT RTE; otherwise we'll generate a whole-row Var for the RTE_RESULT,
    // which the executor has no support for.
    //
    //     foreach(cell, root->rowMarks)
    //     {
    //         PlanRowMark *rc = (PlanRowMark *) lfirst(cell);
    //         if (rt_fetch(rc->rti, root->parse->rtable)->rtekind == RTE_RESULT)
    //             root->rowMarks = foreach_delete_current(root->rowMarks, cell);
    //     }
    //
    // `root->rowMarks` is a `List *` of owned `PlanRowMark *`; here it is the
    // parallel pair (`root.rowMarks: Vec<PlanRowMarkId>`, handles into the
    // `PlannerRun` rowmark store) and `rowmark_rtis[i] == resolve_rowmark(root.
    // rowMarks[i]).rti` (the run-resolved `rc->rti`, supplied by the caller since
    // this owner does not hold `run`).  `rt_fetch(rti, rtable)` is the 1-based
    // `parse.rtable[rti - 1]`.  We retain by position, dropping each rowmark whose
    // `rti` resolves to an `RTE_RESULT` RTE.
    debug_assert_eq!(root.rowMarks.len(), rowmark_rtis.len());
    if !root.rowMarks.is_empty() {
        let mut kept: Vec<::pathnodes::PlanRowMarkId> =
            Vec::with_capacity(root.rowMarks.len());
        for (i, &rmid) in root.rowMarks.iter().enumerate() {
            let rti = rowmark_rtis[i];
            // rt_fetch(rti, parse.rtable) — 1-based.
            let is_result_rte = parse
                .rtable
                .get((rti - 1) as usize)
                .map(|rte| rte.rtekind == RTEKind::RTE_RESULT)
                .unwrap_or(false);
            if !is_result_rte {
                kept.push(rmid);
            }
        }
        root.rowMarks = kept;
    }

    Ok(())
}

// ===========================================================================
// remove_useless_results_recurse (prepjointree.c:3669)
// ===========================================================================
//
// Path-addressed, in-place port. The C `remove_useless_results_recurse` keeps
// the entire jointree wired into `root->parse->jointree` for the whole walk, so
// `remove_result_refs`'s `substitute_phv_relids((Node *) root->parse, …)` and
// the whole-query `find_dependent_phvs(root, varno)` reach PHVs *anywhere* in
// the live tree — including the join quals of nodes that are ancestors of the
// RTE_RESULT being dropped, and PHVs that an earlier substitution rewrote. An
// earlier by-value port `take()`-d the whole jointree out of `parse` for the
// duration; during the recursion `parse.jointree` was then `None`, so those two
// passes silently missed the jointree (they only saw the still-attached
// targetList and rtable). That broke (a) the substitution of PHVs living in
// ancestor join quals (e.g. `t1.x = subq1.d1` over a pulled-up VALUES subquery)
// and (b) the read-after-write between an earlier substitution and a later
// `find_dependent_phvs`. We therefore keep `parse.jointree` populated with the
// live (in-progress) tree throughout, addressing the node being processed by a
// [`JtPath`] from the top FromExpr and re-deriving a transient `&mut`/`&` only
// when needed (always dropped before a call that also needs `&mut parse`).

/// A location of a jointree `Node` within `parse.jointree`. The `'mcx` parameter
/// ties the path to the arena lifetime of the `parse.jointree` it addresses
/// (carried via `PhantomData`, since the path stores only structural indices).
enum JtPath<'p, 'mcx> {
    /// The top `parse.jointree` FromExpr node itself.
    Top,
    /// `fromlist[index]` of the FromExpr at `parent`.
    From { parent: &'p JtPath<'p, 'mcx>, index: usize },
    /// `larg` of the JoinExpr at `parent`.
    Larg { parent: &'p JtPath<'p, 'mcx> },
    /// `rarg` of the JoinExpr at `parent`.
    Rarg { parent: &'p JtPath<'p, 'mcx> },
    /// Never constructed — pins the `'mcx` arena lifetime.
    #[allow(dead_code)]
    Phantom(core::marker::PhantomData<&'p Node<'mcx>>),
}

/// A location of an implicit-AND quals slot (`f->quals` / `j->quals`) within
/// `parse.jointree` — the C `Node **parent_quals` target.
#[derive(Clone, Copy)]
enum QualSlot<'p, 'mcx> {
    /// `quals` of the FromExpr at `path`.
    FromExprQuals { path: &'p JtPath<'p, 'mcx> },
    /// `quals` of the JoinExpr at `path`.
    JoinExprQuals { path: &'p JtPath<'p, 'mcx> },
}

/// Resolve a `&mut Node` for `path` within `parse.jointree`. The borrow lives
/// only as long as the returned reference; callers must drop it before touching
/// `parse` otherwise.
fn jt_node_at<'a, 'mcx>(parse: &'a mut Query<'mcx>, path: &JtPath<'_, 'mcx>) -> &'a mut Node<'mcx> {
    match path {
        JtPath::Top => {
            unreachable!("jt_node_at(Top): the top FromExpr is addressed via its fromlist/slots")
        }
        JtPath::From { parent, index } => {
            let f = jt_fromexpr_at(parse, parent);
            &mut *f.fromlist[*index]
        }
        JtPath::Larg { parent } => {
            let j = jt_joinexpr_at(parse, parent);
            j.larg.as_deref_mut().expect("JoinExpr with NULL larg")
        }
        JtPath::Rarg { parent } => {
            let j = jt_joinexpr_at(parse, parent);
            j.rarg.as_deref_mut().expect("JoinExpr with NULL rarg")
        }
        JtPath::Phantom(_) => unreachable!("JtPath::Phantom is never constructed"),
    }
}

/// Resolve a `&mut FromExpr` for the FromExpr node at `path`.
fn jt_fromexpr_at<'a, 'mcx>(
    parse: &'a mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
) -> &'a mut ::nodes::rawnodes::FromExpr<'mcx> {
    match path {
        JtPath::Top => parse
            .jointree
            .as_deref_mut()
            .expect("remove_useless_result_rtes: top jointree must be a FromExpr"),
        _ => jt_node_at(parse, path)
            .as_fromexpr_mut()
            .unwrap_or_else(|| unreachable!("jt_fromexpr_at: node is not a FromExpr")),
    }
}

/// Resolve a `&mut JoinExpr` for the JoinExpr node at `path`.
fn jt_joinexpr_at<'a, 'mcx>(
    parse: &'a mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
) -> &'a mut ::nodes::rawnodes::JoinExpr<'mcx> {
    jt_node_at(parse, path)
        .as_joinexpr_mut()
        .unwrap_or_else(|| unreachable!("jt_joinexpr_at: node is not a JoinExpr"))
}


/// Read-only resolver: `&Node` for `path`.
fn jt_node_ref<'a, 'mcx>(parse: &'a Query<'mcx>, path: &JtPath<'_, 'mcx>) -> &'a Node<'mcx> {
    match path {
        JtPath::Top => {
            unreachable!("jt_node_ref(Top): the top FromExpr is addressed via its fromlist/slots")
        }
        JtPath::From { parent, index } => &*jt_fromexpr_ref(parse, parent).fromlist[*index],
        JtPath::Larg { parent } => jt_joinexpr_ref(parse, parent)
            .larg
            .as_deref()
            .expect("JoinExpr with NULL larg"),
        JtPath::Rarg { parent } => jt_joinexpr_ref(parse, parent)
            .rarg
            .as_deref()
            .expect("JoinExpr with NULL rarg"),
        JtPath::Phantom(_) => unreachable!("JtPath::Phantom is never constructed"),
    }
}

/// Read-only resolver: `&FromExpr` for the FromExpr node at `path`.
fn jt_fromexpr_ref<'a, 'mcx>(
    parse: &'a Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
) -> &'a ::nodes::rawnodes::FromExpr<'mcx> {
    match path {
        JtPath::Top => parse
            .jointree
            .as_deref()
            .expect("remove_useless_result_rtes: top jointree must be a FromExpr"),
        _ => jt_node_ref(parse, path)
            .as_fromexpr()
            .unwrap_or_else(|| unreachable!("jt_fromexpr_ref: node is not a FromExpr")),
    }
}

/// Read-only resolver: `&JoinExpr` for the JoinExpr node at `path`.
fn jt_joinexpr_ref<'a, 'mcx>(
    parse: &'a Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
) -> &'a ::nodes::rawnodes::JoinExpr<'mcx> {
    jt_node_ref(parse, path)
        .as_joinexpr()
        .unwrap_or_else(|| unreachable!("jt_joinexpr_ref: node is not a JoinExpr"))
}

/// Resolve a `&mut Option<NodePtr>` for the quals slot addressed by `qs`.
fn qual_slot_at<'a, 'mcx>(
    parse: &'a mut Query<'mcx>,
    qs: QualSlot<'_, 'mcx>,
) -> &'a mut Option<NodePtr<'mcx>> {
    match qs {
        QualSlot::FromExprQuals { path } => &mut jt_fromexpr_at(parse, path).quals,
        QualSlot::JoinExprQuals { path } => &mut jt_joinexpr_at(parse, path).quals,
    }
}

/// `remove_useless_results_recurse(root, jtnode, parent_quals, dropped_outer_joins)`
/// (prepjointree.c:3669), addressing the node it processes by `path` within the
/// live `parse.jointree`. The slot at `path` is mutated in place (and may be
/// replaced by a child via elision/collapse). RT indexes of removed outer-join
/// nodes are added to `*dropped_outer_joins`.
///
/// `parent_quals` is the (possibly several-levels-up) parent's quals slot into
/// which child quals may be hoisted, or `None` when that is not valid.
fn remove_useless_results_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
    parent_quals: Option<QualSlot<'_, 'mcx>>,
    dropped_outer_joins: &mut Relids<'mcx>,
) -> PgResult<()> {
    let tag = match path {
        JtPath::Top => ntag::T_FromExpr,
        _ => jt_node_at(parse, path).node_tag(),
    };
    match tag {
        ntag::T_RangeTblRef => {
            // Can't immediately do anything with a RangeTblRef.
            Ok(())
        }
        ntag::T_FromExpr => {
            remove_useless_results_recurse_fromexpr(mcx, root, parse, path, parent_quals, dropped_outer_joins)
        }
        ntag::T_JoinExpr => {
            remove_useless_results_recurse_joinexpr(mcx, root, parse, path, parent_quals, dropped_outer_joins)
        }
        _ => Err(::types_error::PgError::error("unrecognized node type")),
    }
}

/// The `IsA(jtnode, FromExpr)` arm of [`remove_useless_results_recurse`].
fn remove_useless_results_recurse_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
    parent_quals: Option<QualSlot<'_, 'mcx>>,
    dropped_outer_joins: &mut Relids<'mcx>,
) -> PgResult<()> {
    let is_top = matches!(path, JtPath::Top);
    let mut result_relids: Relids = None;

    // We can drop RTE_RESULT rels from the fromlist so long as at least one
    // child remains, since joining to a one-row table changes nothing. (But we
    // can't drop one that computes PHV(s) needed by a sibling.) The C deletes
    // in-place with `foreach_delete_current` while walking, re-checking
    // `list_length(f->fromlist) > 1` against the shrinking list and passing the
    // shrinking `f` to `find_dependent_phvs_in_jointree`; we mirror that with an
    // index walk that removes the current element on a drop and does not advance.
    let mut i = 0usize;
    loop {
        let len = jt_fromexpr_at(parse, path).fromlist.len();
        if i >= len {
            break;
        }
        // Recursively transform child, allowing it to push up quals into f.quals.
        let child = JtPath::From { parent: path, index: i };
        remove_useless_results_recurse(
            mcx,
            root,
            parse,
            &child,
            Some(QualSlot::FromExprQuals { path }),
            dropped_outer_joins,
        )?;

        let varno = {
            let f = jt_fromexpr_ref(parse, path);
            get_result_relid(root, parse, &f.fromlist[i])
        };
        let len = jt_fromexpr_ref(parse, path).fromlist.len();
        let droppable = len > 1
            && varno != 0
            && {
                let f = jt_fromexpr_ref(parse, path);
                !find_dependent_phvs_in_jointree_fromexpr(mcx, root, parse, f, varno)?
            };
        if droppable {
            jt_fromexpr_at(parse, path).fromlist.remove(i);
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
            // subrelids of the surviving FromExpr; compute then drop the borrow
            // before substitute (which needs `&mut parse`).
            let subrelids = {
                let f = jt_fromexpr_ref(parse, path);
                get_relids_in_fromexpr(mcx, f, true, false)?
            };
            remove_result_refs(mcx, root, parse, subrelids.as_deref(), varno)?;
        }
    }

    // If the FromExpr now has only one child, try to elide it.
    let (one_child, has_quals) = {
        let f = jt_fromexpr_at(parse, path);
        (f.fromlist.len() == 1, f.quals.is_some())
    };
    if one_child && !is_top && (!has_quals || parent_quals.is_some()) {
        // Merge any quals up to parent (child quals first).
        if has_quals {
            let pq = parent_quals.expect("elision requires parent_quals when quals present");
            let my_quals = jt_fromexpr_at(parse, path).quals.take();
            let parent_existing = qual_slot_at(parse, pq).take();
            let merged = concat_quals(mcx, my_quals, parent_existing)?;
            *qual_slot_at(parse, pq) = merged;
        }
        // Replace this slot with `linitial(f->fromlist)`.
        let child = {
            let f = jt_fromexpr_at(parse, path);
            core::mem::replace(&mut *f.fromlist[0], dummy_node(mcx)?)
        };
        store_at(parse, path, child)?;
    }

    Ok(())
}

/// The `IsA(jtnode, JoinExpr)` arm of [`remove_useless_results_recurse`].
fn remove_useless_results_recurse_joinexpr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
    parent_quals: Option<QualSlot<'_, 'mcx>>,
    dropped_outer_joins: &mut Relids<'mcx>,
) -> PgResult<()> {
    let jointype = jt_joinexpr_at(parse, path).jointype;

    // Recurse into larg. INNER absorbs child quals into this node; LEFT lets
    // LHS-child quals be absorbed into the parent (if any); otherwise none.
    let larg = JtPath::Larg { parent: path };
    let larg_pq = match jointype {
        JoinType::JOIN_INNER => Some(QualSlot::JoinExprQuals { path }),
        JoinType::JOIN_LEFT => parent_quals,
        _ => None,
    };
    remove_useless_results_recurse(mcx, root, parse, &larg, larg_pq, dropped_outer_joins)?;

    // Recurse into rarg. INNER/LEFT absorb RHS-child quals into this node;
    // otherwise none.
    let rarg = JtPath::Rarg { parent: path };
    let rarg_pq = match jointype {
        JoinType::JOIN_INNER | JoinType::JOIN_LEFT => Some(QualSlot::JoinExprQuals { path }),
        _ => None,
    };
    remove_useless_results_recurse(mcx, root, parse, &rarg, rarg_pq, dropped_outer_joins)?;

    // Apply join-type-specific optimization rules.
    match jointype {
        JoinType::JOIN_INNER => {
            // An inner join is equivalent to a FromExpr; if either side reduced
            // to an RTE_RESULT rel, replace the join with the other side.
            let varno_l = {
                let j = jt_joinexpr_ref(parse, path);
                let larg = j.larg.as_deref().unwrap();
                get_result_relid(root, parse, larg)
            };
            // The other input can't reference PHVs to be evaluated at the RESULT
            // rel (only RHSes of inner/left joins may have LATERAL refs to it).
            let larg_droppable = varno_l != 0 && {
                let j = jt_joinexpr_ref(parse, path);
                let rarg = j.rarg.as_deref().unwrap();
                !find_dependent_phvs_in_jointree_node(mcx, root, parse, rarg, varno_l)?
            };
            if larg_droppable {
                // remove_result_refs over the surviving rarg, then collapse to it.
                let subrelids = {
                    let j = jt_joinexpr_ref(parse, path);
                    get_relids_in_jointree(mcx, j.rarg.as_deref().unwrap(), true, false)?
                };
                remove_result_refs(mcx, root, parse, subrelids.as_deref(), varno_l)?;
                let side = {
                    let j = jt_joinexpr_at(parse, path);
                    PgBox::into_inner(j.rarg.take().unwrap())
                };
                inner_collapse(mcx, parse, path, side, parent_quals)?;
            } else {
                let varno_r = {
                    let j = jt_joinexpr_ref(parse, path);
                    let rarg = j.rarg.as_deref().unwrap();
                    get_result_relid(root, parse, rarg)
                };
                if varno_r != 0 {
                    let subrelids = {
                        let j = jt_joinexpr_ref(parse, path);
                        get_relids_in_jointree(mcx, j.larg.as_deref().unwrap(), true, false)?
                    };
                    remove_result_refs(mcx, root, parse, subrelids.as_deref(), varno_r)?;
                    let side = {
                        let j = jt_joinexpr_at(parse, path);
                        PgBox::into_inner(j.larg.take().unwrap())
                    };
                    inner_collapse(mcx, parse, path, side, parent_quals)?;
                }
            }
        }
        JoinType::JOIN_LEFT => {
            // Simplify if the RHS is an RTE_RESULT. If the qual is empty the join
            // strength-reduces to inner; otherwise each LHS row is still returned
            // once and the RHS yields no columns (barring PHVs), so we can ignore
            // the qual and discard the left join.
            let varno = {
                let j = jt_joinexpr_ref(parse, path);
                let rarg = j.rarg.as_deref().unwrap();
                get_result_relid(root, parse, rarg)
            };
            let no_quals = jt_joinexpr_ref(parse, path).quals.is_none();
            if varno != 0 && (no_quals || !find_dependent_phvs(mcx, root, parse, varno)?) {
                let rtindex = jt_joinexpr_at(parse, path).rtindex;
                let subrelids = {
                    let j = jt_joinexpr_ref(parse, path);
                    get_relids_in_jointree(mcx, j.larg.as_deref().unwrap(), true, false)?
                };
                remove_result_refs(mcx, root, parse, subrelids.as_deref(), varno)?;
                *dropped_outer_joins =
                    Some(bms_add_member(mcx, dropped_outer_joins.take(), rtindex)?);
                let side = {
                    let j = jt_joinexpr_at(parse, path);
                    PgBox::into_inner(j.larg.take().unwrap())
                };
                store_at(parse, path, side)?;
            }
        }
        JoinType::JOIN_SEMI => {
            // Simplify if the RHS is an RTE_RESULT; the join qual becomes a
            // filter qual for the LHS. The join has no rtindex to scrub.
            let varno = {
                let j = jt_joinexpr_ref(parse, path);
                let rarg = j.rarg.as_deref().unwrap();
                get_result_relid(root, parse, rarg)
            };
            if varno != 0 {
                debug_assert_eq!(jt_joinexpr_ref(parse, path).rtindex, 0);
                let subrelids = {
                    let j = jt_joinexpr_ref(parse, path);
                    get_relids_in_jointree(mcx, j.larg.as_deref().unwrap(), true, false)?
                };
                remove_result_refs(mcx, root, parse, subrelids.as_deref(), varno)?;
                let side = {
                    let j = jt_joinexpr_at(parse, path);
                    PgBox::into_inner(j.larg.take().unwrap())
                };
                inner_collapse(mcx, parse, path, side, parent_quals)?;
            }
        }
        JoinType::JOIN_FULL | JoinType::JOIN_ANTI => {
            // No special smarts for these cases.
        }
        _ => {
            // JOIN_RIGHT should be gone at this point.
            return Err(::types_error::PgError::error("unrecognized join type"));
        }
    }

    Ok(())
}

/// Store `node` into the jointree slot at `path` (the `parse.jointree` location
/// the C `return jtnode` writes back). The `Top` slot must remain a FromExpr.
fn store_at<'mcx>(
    parse: &mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
    node: Node<'mcx>,
) -> PgResult<()> {
    match path {
        JtPath::Top => {
            let f = node
                .into_fromexpr()
                .unwrap_or_else(|| panic!("remove_useless_result_rtes: top jointree node is no longer a FromExpr"));
            *parse
                .jointree
                .as_deref_mut()
                .expect("remove_useless_result_rtes: top jointree must be a FromExpr") = f;
        }
        _ => {
            *jt_node_at(parse, path) = node;
        }
    }
    Ok(())
}

/// The shared INNER/SEMI "collapse to the surviving side" tail (the C
/// `makeFromExpr(list_make1(side), j->quals)` / merge-to-parent block). The
/// JoinExpr being collapsed is at `path`; `side` is the surviving child. The
/// slot at `path` is replaced with the collapse result.
fn inner_collapse<'mcx>(
    mcx: Mcx<'mcx>,
    parse: &mut Query<'mcx>,
    path: &JtPath<'_, 'mcx>,
    side: Node<'mcx>,
    parent_quals: Option<QualSlot<'_, 'mcx>>,
) -> PgResult<()> {
    let quals = jt_joinexpr_at(parse, path).quals.take();
    let result = if quals.is_some() && parent_quals.is_none() {
        // makeFromExpr(list_make1(side), j->quals)
        let mut fromlist = ::mcx::PgVec::new_in(mcx);
        fromlist.try_reserve(1).map_err(|_| mcx.oom(1))?;
        fromlist.push(::mcx::alloc_in(mcx, side)?);
        Node::mk_from_expr(mcx, ::nodes::rawnodes::FromExpr { fromlist, quals })?
    } else {
        // Merge any quals up to parent, return the surviving side.
        if let Some(pq) = parent_quals {
            if quals.is_some() {
                let parent_existing = qual_slot_at(parse, pq).take();
                let merged = concat_quals(mcx, quals, parent_existing)?;
                *qual_slot_at(parse, pq) = merged;
            }
        }
        side
    };
    store_at(parse, path, result)
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

/// `remove_result_refs(root, varno, newjtloc)` (prepjointree.c:3970). Relabel
/// the relids of any PHVs/AppendRelInfos that referenced the RTE_RESULT `varno`
/// so they instead reference `subrelids` — the relids of the surviving jointree
/// location (`get_relids_in_jointree(newjtloc, …)`). The caller computes
/// `subrelids` first (from the still-attached jointree fragment) and drops the
/// borrow, since the parse-wide PHV substitution needs `&mut parse`.
///
/// Because the whole jointree stays wired into `parse` during the recursion (see
/// [`remove_useless_results_recurse`]), `substitute_phv_relids_in_query(parse,
/// …)` reaches the PHVs in ancestor join quals as well as the targetList/rtable
/// — exactly as C's `substitute_phv_relids((Node *) root->parse, …)`.
fn remove_result_refs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    subrelids: Option<&Bitmapset>,
    varno: i32,
) -> PgResult<()> {
    if last_ph_id(root) != 0 {
        debug_assert!(!bms_is_empty(subrelids));
        let sub = relids_to_expr_relids(subrelids);
        substitute_phv_relids_in_query(mcx, parse, varno, &sub);
        fix_append_rel_relids(mcx, root, varno, subrelids, &sub)?;
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
    mcx: Mcx<'mcx>,
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
    if find_dependent_phvs_in_append_rel_list(mcx, root, &mut context)? {
        return Ok(true);
    }
    Ok(false)
}

/// `expression_tree_walker((Node *) root->append_rel_list, ...)` over the arena
/// `translated_vars` Exprs.
fn find_dependent_phvs_in_append_rel_list<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    context: &mut FindDependentPhvsContext,
) -> PgResult<bool> {
    for appinfo in root.append_rel_list.iter() {
        for &id in appinfo.translated_vars.iter() {
            if id == NodeId::default() {
                continue;
            }
            // Deep-copy via `clone_in` — the derived `Expr::clone` panics on an
            // owned-subtree child.
            let node = Node::mk_expr(mcx, root.node(id).clone_in(mcx)?)?;
            if find_dependent_phvs_walker(&node, context) {
                return Ok(true);
            }
        }
    }
    Ok(false)
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
    f: &::nodes::rawnodes::FromExpr<'mcx>,
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
fn walk_fromexpr(f: &::nodes::rawnodes::FromExpr, walker: &mut dyn FnMut(&Node) -> bool) -> bool {
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
fn substitute_walk_children<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    context: &mut SubstitutePhvRelidsContext,
) -> bool {
    expression_tree_walker_mut(
        node,
        &mut |n| substitute_phv_relids_walker(mcx, n, context),
        mcx,
    )
}

fn substitute_phv_relids_walker<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    context: &mut SubstitutePhvRelidsContext,
) -> bool {
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
            substitute_walk_children(mcx, node, context)
        }
        ntag::T_Query => {
            let q = node.as_query_mut().unwrap();
            context.sublevels_up += 1;
            let result = query_tree_mutator(
                q,
                &mut |n| substitute_phv_relids_walker(mcx, n, context),
                0,
                mcx,
            );
            context.sublevels_up -= 1;
            result
        }
        _ => substitute_walk_children(mcx, node, context),
    }
}

/// `substitute_phv_relids((Node *) query, varno, subrelids)` (prepjointree.c:4146)
/// applied to the top `&mut Query` (level 0, no sublevels bump).
pub(crate) fn substitute_phv_relids_in_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: &mut Query<'mcx>,
    varno: i32,
    subrelids: &ExprRelids,
) {
    let mut context = SubstitutePhvRelidsContext {
        varno,
        sublevels_up: 0,
        subrelids,
    };
    query_tree_mutator(
        query,
        &mut |n| substitute_phv_relids_walker(mcx, n, &mut context),
        0,
        mcx,
    );
}

/// `substitute_phv_relids((Node *) node, varno, subrelids)` applied to a bare
/// expression `&mut Node`.
fn substitute_phv_relids_in_node<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut Node<'mcx>,
    varno: i32,
    subrelids: &ExprRelids,
) {
    let mut context = SubstitutePhvRelidsContext {
        varno,
        sublevels_up: 0,
        subrelids,
    };
    // query_or_expression_tree_walker for a bare expr visits the node itself.
    if substitute_phv_relids_walker(mcx, node, &mut context) {}
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
    mcx: Mcx<'mcx>,
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
                subvarno = ::nodes_core::bitmapset::bms_singleton_member(subrelids_bms);
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
        // Deep-copy via `clone_in` — the derived `Expr::clone` panics on an
        // owned-subtree child.
        let mut node = Node::mk_expr(mcx, root.node(id).clone_in(mcx)?)?;
        substitute_phv_relids_in_node(mcx, &mut node, varno, subrelids);
        if let Some(e) = node.into_expr() {
            *root.node_mut(id) = e.erase_lifetime();
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
        _ => Err(::types_error::PgError::error("unrecognized node type")),
    }
}

/// `get_relids_in_jointree` applied to a `FromExpr` value (the C `(Node *) f`).
fn get_relids_in_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    f: &::nodes::rawnodes::FromExpr<'mcx>,
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
        _ => Err(::types_error::PgError::error("unrecognized node type")),
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
            return Err(::types_error::PgError::error(alloc::format!(
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
                    return Err(::types_error::PgError::error("unrecognized join type"));
                }
            }
            Ok(())
        }
        _ => Err(::types_error::PgError::error("unrecognized node type")),
    }
}

/// `get_nullingrels_recurse` over a `FromExpr` value (the C `(Node *) f`).
fn get_nullingrels_recurse_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    f: &::nodes::rawnodes::FromExpr<'mcx>,
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
    ::nodes_core::bitmapset::bms_copy(mcx, a)
}

// ===========================================================================
// remove_nulling_relids over the append_rel_list (FAMILY 4 helper, reused)
// ===========================================================================

/// `remove_nulling_relids((Node *) root->append_rel_list, removable, except)`:
/// run the expression-tree `remove_nulling_relids` over each arena
/// `translated_vars` Expr and write it back.
fn remove_nulling_relids_in_append_rel_list<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    removable: &ExprRelids,
    except: &ExprRelids,
) -> PgResult<()> {
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
        // Deep-copy via `clone_in` — the derived `Expr::clone` panics on an
        // owned-subtree child.
        let mut node = Node::mk_expr(mcx, root.node(id).clone_in(mcx)?)?;
        rewrite_core::remove_nulling_relids(&mut node, removable, except, mcx);
        if let Some(e) = node.into_expr() {
            *root.node_mut(id) = e.erase_lifetime();
        }
    }
    Ok(())
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
    let mut out: ::mcx::PgVec<'mcx, NodePtr<'mcx>> = ::mcx::PgVec::new_in(mcx);
    let push_list = |q: Option<NodePtr<'mcx>>,
                         out: &mut ::mcx::PgVec<'mcx, NodePtr<'mcx>>|
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
                    out.push(::mcx::alloc_in(mcx, node)?);
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
        Ok(Some(::mcx::alloc_in(mcx, Node::mk_list(mcx, out)?)?))
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
