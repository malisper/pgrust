//! `backend/optimizer/prep/prepjointree.c` — FAMILY 4: `reduce_outer_joins`.
//!
//! 1:1 port of PostgreSQL 18.3 `reduce_outer_joins` (and its
//! `reduce_outer_joins_pass1` / `reduce_outer_joins_pass2` /
//! `report_reduced_full_join` helpers) over this repo's lifetime-free owned
//! `Query<'mcx>` + embedded-`PgBox` jointree model.
//!
//! ## What this unit is
//!
//! This crate is the new owner of `optimizer/prep/prepjointree.c`. prepjointree
//! is decomposed into ~6 families; this is FAMILY 4. The crate also owns the
//! three top-level `subquery_planner` entry-point seams
//! ([`backend_optimizer_prep_prepjointree_seams`]):
//!
//! * `reduce_outer_joins` — FAMILY 4, ported here, installed by [`init_seams`].
//! * `remove_useless_result_rtes` — FAMILY 5, ported here (see [`result_rtes`]).
//! * `pull_up_sublinks` — FAMILY 1, **ported here** (see [`sublinks`]) now that
//!   the SubLink owned-`Query` carrier keystone (#273) has landed; installed by
//!   [`init_seams`].
//! * `pull_up_subqueries` (FAMILY 2) — deferred to FAMILY 5 +
//!   the `AppendRelInfo.translated_vars` walkable-carrier keystone (#274);
//!   installed as a seam-and-panic body.
//!
//! `subquery_planner` (planner.c, still unported) calls all of them, so all are
//! declared and installed; only FAMILY 2 panics loudly until its keystone lands
//! (the sanctioned seam-and-panic contract).
//!
//! ## Model notes (`reduce_outer_joins`)
//!
//! * The C signature is `reduce_outer_joins(PlannerInfo *root)`, reading and
//!   mutating `root->parse` (the top `Query`) and `root->append_rel_list`. Here
//!   `PlannerInfo` is lifetime-free and the top `Query` lives in the
//!   `PlannerRun` store behind `root.parse`'s `QueryId`. The planner driver
//!   resolves it (`run.resolve_mut(root.parse)`) and threads the `&mut Query`
//!   alongside `&mut PlannerInfo`; the two are distinct objects so there's no
//!   aliasing conflict.
//! * The jointree is walked by deref over the embedded `Option<PgBox<FromExpr>>`
//!   / `Vec<NodePtr>` / `JoinExpr.larg`/`rarg`, exactly as the C walks
//!   `Node *` pointers. `pass2` mutates `JoinExpr.jointype` / swaps `larg`/`rarg`
//!   and the matching `RangeTblEntry.jointype` in place.
//! * Relids working sets (`state1.relids`, `nonnullable_rels`,
//!   `inner_reduced`, `unreduced_side`) are the `'mcx`-arena
//!   [`Bitmapset`](types_nodes::Bitmapset), matching what
//!   `find_nonnullable_rels` returns and what `bms_overlap` consumes. Forced-null
//!   Var sets are [`MultiBitmapset`](backend_nodes_core::multibitmapset).
//! * `remove_nulling_relids((Node *) root->parse, …)` becomes
//!   [`backend_rewrite_core::remove_nulling_relids_in_query`] over the `&mut
//!   Query`. `remove_nulling_relids((Node *) root->append_rel_list, …)` walks
//!   each `AppendRelInfo.translated_vars` element — carried here as arena
//!   `NodeId` handles — by resolving each to its `Expr` in `root.node_arena`,
//!   running the expression-tree `remove_nulling_relids` over it, and writing it
//!   back (the faithful analogue of the C list-of-`Node*` walk; an
//!   `AppendRelInfo`'s only relid-bearing children are its `translated_vars`).
//!   The relids fed to these become [`ExprRelids`](types_nodes::primnodes::ExprRelids)
//!   (the lifetime-free word-vector the nulling rewriter consumes).

#![no_std]
#![allow(non_snake_case)]
// The pass2 jointype `switch` and the constraint-passdown `if` ladders mirror C
// verbatim; collapsing them would obscure the 1:1 reading.
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
// The project-wide error contract is the un-boxed `PgResult`.
#![allow(clippy::result_large_err)]

extern crate alloc;

mod pullup;
mod result_rtes;
mod sublinks;

pub use sublinks::pull_up_sublinks;

// FAMILY 5 helpers re-exported for FAMILY 2 (`pull_up_subqueries`, still
// seam-and-panicked): `get_nullingrels` builds the per-RTE nullingrel table the
// `pullup_replace_vars` path reads. Ported now (a leaf read-only walker); its
// only in-crate consumer lands with FAMILY 2.
pub use result_rtes::{get_nullingrels, NullingrelInfo};

use alloc::boxed::Box;
use alloc::vec::Vec;

use backend_nodes_core::bitmapset::{bms_add_members, bms_overlap};
use backend_nodes_core::multibitmapset::{mbms_add_members, mbms_overlap_sets, MultiBitmapset};
use backend_optimizer_util_clauses::grounded::{
    find_forced_null_vars, find_nonnullable_rels, find_nonnullable_vars,
};
use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::copy_query::Query;
use types_nodes::jointype::JoinType;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::RTEKind;
use types_nodes::primnodes::ExprRelids;
use types_pathnodes::PlannerInfo;

/// C `Relids` = `Bitmapset *`: the `'mcx`-arena relid set (NULL/empty = `None`).
type Relids<'mcx> = Option<PgBox<'mcx, Bitmapset<'mcx>>>;

// ===========================================================================
// pass-1 / pass-2 state (prepjointree.c:86)
// ===========================================================================

/// `reduce_outer_joins_pass1_state` (prepjointree.c:86).
struct ReduceOuterJoinsPass1State<'mcx> {
    /// `Relids relids` — base relids within this subtree.
    relids: Relids<'mcx>,
    /// `bool contains_outer` — does the subtree contain outer join(s)?
    contains_outer: bool,
    /// `List *sub_states` — states for subtree components.
    sub_states: Vec<Box<ReduceOuterJoinsPass1State<'mcx>>>,
}

/// `reduce_outer_joins_pass2_state` (prepjointree.c:93).
struct ReduceOuterJoinsPass2State {
    /// `Relids inner_reduced` — OJ relids reduced to plain inner joins.
    inner_reduced: ExprRelids,
    /// `List *partial_reduced` — partially reduced FULL joins.
    partial_reduced: Vec<ReduceOuterJoinsPartialState>,
}

/// `reduce_outer_joins_partial_state` (prepjointree.c:99).
struct ReduceOuterJoinsPartialState {
    /// `int full_join_rti` — RT index of a formerly-FULL join.
    full_join_rti: i32,
    /// `Relids unreduced_side` — relids in its still-nullable side.
    unreduced_side: ExprRelids,
}

// ===========================================================================
// helpers
// ===========================================================================

/// `IS_OUTER_JOIN(jointype)` (nodes.h) — LEFT/FULL/RIGHT/ANTI/RIGHT_ANTI.
#[inline]
fn is_outer_join(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JoinType::JOIN_LEFT
            | JoinType::JOIN_FULL
            | JoinType::JOIN_RIGHT
            | JoinType::JOIN_ANTI
            | JoinType::JOIN_RIGHT_ANTI
    )
}

/// Convert an `'mcx`-arena [`Bitmapset`] relid set to the lifetime-free
/// [`ExprRelids`] the nulling rewriter consumes (both are `Vec<u64>` word
/// storage; the empty/NULL set is an empty word vector).
#[inline]
fn relids_to_expr_relids(a: Option<&Bitmapset>) -> ExprRelids {
    match a {
        None => ExprRelids { words: Vec::new() },
        Some(bms) => {
            let mut words: Vec<u64> = Vec::with_capacity(bms.words.len());
            for &w in bms.words.iter() {
                words.push(w);
            }
            // Trim trailing all-zero words so the empty set is `[]` (canonical),
            // matching the rewriter's `normalize`.
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

/// `&Expr` of a qual `Node` (or `None` for a `NULL` qual), for the
/// `find_nonnullable_*` walkers (`clauses.c` takes a `Node *`, always an
/// expression node in a qual position).
#[inline]
fn qual_as_expr<'a, 'mcx>(
    quals: &'a Option<types_nodes::nodes::NodePtr<'mcx>>,
) -> Option<&'a types_nodes::primnodes::Expr> {
    quals.as_deref().and_then(|n| n.as_expr())
}

// ===========================================================================
// reduce_outer_joins (prepjointree.c:3101)
// ===========================================================================

/// `reduce_outer_joins(root)` (prepjointree.c:3101).
pub fn reduce_outer_joins<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
) -> PgResult<()> {
    // To avoid doing strictness checks on more quals than necessary, we want to
    // stop descending the jointree as soon as there are no outer joins below
    // our current point. This consideration forces a two-pass process.
    let state1 = match parse.jointree.as_deref() {
        Some(jt) => {
            // The top jointree node is a FromExpr; wrap it as a Node for pass1.
            // pass1 only reads, so a borrow is sufficient.
            reduce_outer_joins_pass1_fromexpr(mcx, jt)?
        }
        None => reduce_outer_joins_pass1_empty(),
    };

    // planner.c shouldn't have called me if no outer joins.
    if !state1.contains_outer {
        return Err(types_error::PgError::error("so where are the outer joins?"));
    }

    let mut state2 = ReduceOuterJoinsPass2State {
        inner_reduced: ExprRelids { words: Vec::new() },
        partial_reduced: Vec::new(),
    };

    // pass2 mutates the jointree, so take it out, walk it, put it back.
    if let Some(mut jt) = parse.jointree.take() {
        // The top node is a FromExpr; wrap it in a Node to walk uniformly.
        let mut jt_node = Node::FromExpr(core::mem::replace(
            &mut *jt,
            types_nodes::rawnodes::FromExpr {
                fromlist: mcx::PgVec::new_in(mcx),
                quals: None,
            },
        ));
        reduce_outer_joins_pass2(mcx, &mut jt_node, &state1, &mut state2, parse, None, &empty_mbms(mcx))?;
        // Put the (possibly mutated) FromExpr back.
        if let Node::FromExpr(f) = jt_node {
            *jt = f;
        }
        parse.jointree = Some(jt);
    }

    // If we successfully reduced the strength of any outer joins, we must remove
    // references to those joins as nulling rels. This is handled as an
    // additional pass, for simplicity and because we can handle all
    // fully-reduced joins in a single pass over the parse tree.
    if !expr_relids_is_empty(&state2.inner_reduced) {
        let empty = ExprRelids { words: Vec::new() };
        backend_rewrite_core::remove_nulling_relids_in_query(parse, &state2.inner_reduced, &empty);
        // There could be references in the append_rel_list, too.
        remove_nulling_relids_in_append_rel_list(root, &state2.inner_reduced, &empty);
    }

    // Partially-reduced full joins have to be done one at a time, since they'll
    // each need a different setting of except_relids.
    for statep in state2.partial_reduced.iter() {
        let full_join_relids = expr_relids_make_singleton(statep.full_join_rti);
        backend_rewrite_core::remove_nulling_relids_in_query(
            parse,
            &full_join_relids,
            &statep.unreduced_side,
        );
        remove_nulling_relids_in_append_rel_list(
            root,
            &full_join_relids,
            &statep.unreduced_side,
        );
    }

    Ok(())
}

/// `remove_nulling_relids((Node *) root->append_rel_list, removable, except)`:
/// the `List *append_rel_list` of `AppendRelInfo`s, whose only relid-bearing
/// children are the per-`AppendRelInfo` `translated_vars` (arena `NodeId`
/// handles). Resolve each to its `Expr`, run the expression-tree
/// `remove_nulling_relids` over it, and write it back.
fn remove_nulling_relids_in_append_rel_list(
    root: &mut PlannerInfo,
    removable: &ExprRelids,
    except: &ExprRelids,
) {
    // Collect the NodeIds first (borrow of append_rel_list) to avoid holding it
    // while we mutate the node_arena.
    let mut ids: Vec<types_pathnodes::NodeId> = Vec::new();
    for appinfo in root.append_rel_list.iter() {
        for &id in appinfo.translated_vars.iter() {
            // A NULL element (dropped parent column) is NodeId::default() (0) and
            // resolves to nothing; skip it (C's `lfirst` over a NULL is a no-op).
            if id == types_pathnodes::NodeId::default() {
                continue;
            }
            ids.push(id);
        }
    }
    for id in ids {
        // Clone the arena Expr into a Node, mutate, write back. (`Expr` is not
        // `Default`, so we can't `mem::take`; the clone is the owned-tree
        // analogue of the C copy-mutator, which copies each Var/PHV before
        // editing its nullingrels anyway.)
        let mut node = Node::Expr(root.node(id).clone());
        backend_rewrite_core::remove_nulling_relids(&mut node, removable, except);
        if let Node::Expr(e) = node {
            *root.node_mut(id) = e;
        }
    }
}

// ===========================================================================
// reduce_outer_joins_pass1 (prepjointree.c:3174)
// ===========================================================================

/// An empty pass1 state (the `jtnode == NULL` arm of
/// `reduce_outer_joins_pass1`).
fn reduce_outer_joins_pass1_empty<'mcx>() -> ReduceOuterJoinsPass1State<'mcx> {
    ReduceOuterJoinsPass1State {
        relids: None,
        contains_outer: false,
        sub_states: Vec::new(),
    }
}

/// `reduce_outer_joins_pass1((Node *) f)` where `f` is the top `FromExpr`
/// (split out because our top jointree is `Option<PgBox<FromExpr>>`, not a
/// `Node`).
fn reduce_outer_joins_pass1_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    f: &types_nodes::rawnodes::FromExpr<'mcx>,
) -> PgResult<ReduceOuterJoinsPass1State<'mcx>> {
    let mut result = reduce_outer_joins_pass1_empty();
    for l in f.fromlist.iter() {
        let sub_state = reduce_outer_joins_pass1(mcx, l)?;
        result.relids = bms_add_members(mcx, result.relids, sub_state.relids.as_deref())?;
        result.contains_outer |= sub_state.contains_outer;
        result.sub_states.push(Box::new(sub_state));
    }
    Ok(result)
}

/// `reduce_outer_joins_pass1(jtnode)` (prepjointree.c:3174).
fn reduce_outer_joins_pass1<'mcx>(
    mcx: Mcx<'mcx>,
    jtnode: &Node<'mcx>,
) -> PgResult<ReduceOuterJoinsPass1State<'mcx>> {
    let mut result = reduce_outer_joins_pass1_empty();

    match jtnode {
        Node::RangeTblRef(r) => {
            let varno = r.rtindex;
            result.relids = Some(backend_nodes_core::bitmapset::bms_make_singleton(mcx, varno)?);
        }
        Node::FromExpr(f) => {
            for l in f.fromlist.iter() {
                let sub_state = reduce_outer_joins_pass1(mcx, l)?;
                result.relids = bms_add_members(mcx, result.relids, sub_state.relids.as_deref())?;
                result.contains_outer |= sub_state.contains_outer;
                result.sub_states.push(Box::new(sub_state));
            }
        }
        Node::JoinExpr(j) => {
            // join's own RT index is not wanted in result->relids
            if is_outer_join(j.jointype) {
                result.contains_outer = true;
            }

            let larg = j
                .larg
                .as_deref()
                .expect("reduce_outer_joins_pass1: JoinExpr with NULL larg");
            let sub_state = reduce_outer_joins_pass1(mcx, larg)?;
            result.relids = bms_add_members(mcx, result.relids, sub_state.relids.as_deref())?;
            result.contains_outer |= sub_state.contains_outer;
            result.sub_states.push(Box::new(sub_state));

            let rarg = j
                .rarg
                .as_deref()
                .expect("reduce_outer_joins_pass1: JoinExpr with NULL rarg");
            let sub_state = reduce_outer_joins_pass1(mcx, rarg)?;
            result.relids = bms_add_members(mcx, result.relids, sub_state.relids.as_deref())?;
            result.contains_outer |= sub_state.contains_outer;
            result.sub_states.push(Box::new(sub_state));
        }
        _ => {
            return Err(types_error::PgError::error("unrecognized node type"));
        }
    }

    Ok(result)
}

// ===========================================================================
// reduce_outer_joins_pass2 (prepjointree.c:3253)
// ===========================================================================

/// `reduce_outer_joins_pass2(jtnode, state1, state2, root, nonnullable_rels,
/// forced_null_vars)` (prepjointree.c:3253).
///
/// `nonnullable_rels` is `None` for the C `NULL` (empty set). `forced_null_vars`
/// is the `MultiBitmapset` of Vars forced null by upper quals.
#[allow(clippy::too_many_arguments)]
fn reduce_outer_joins_pass2<'mcx>(
    mcx: Mcx<'mcx>,
    jtnode: &mut Node<'mcx>,
    state1: &ReduceOuterJoinsPass1State<'mcx>,
    state2: &mut ReduceOuterJoinsPass2State,
    parse: &mut Query<'mcx>,
    nonnullable_rels: Option<&Bitmapset>,
    forced_null_vars: &MultiBitmapset<'mcx>,
) -> PgResult<()> {
    // pass 2 should never descend as far as an empty subnode or base rel,
    // because it's only called on subtrees marked as contains_outer.
    match jtnode {
        Node::RangeTblRef(_) => {
            return Err(types_error::PgError::error("reached base rel"));
        }
        Node::FromExpr(_) => {
            // Scan quals to see if we can add any constraints.
            let (mut pass_nonnullable_rels, pass_forced_null_vars) = {
                let f = match jtnode {
                    Node::FromExpr(f) => f,
                    _ => unreachable!(),
                };
                let mut pass_nonnullable_rels =
                    find_nonnullable_rels(mcx, qual_as_expr(&f.quals))?;
                pass_nonnullable_rels =
                    bms_add_members(mcx, pass_nonnullable_rels, nonnullable_rels)?;
                let local_forced = find_forced_null_vars(mcx, qual_as_expr(&f.quals))?;
                let pass_forced_null_vars =
                    mbms_add_members(mcx, local_forced, forced_null_vars)?;
                (pass_nonnullable_rels, pass_forced_null_vars)
            };

            // And recurse --- but only into interesting subtrees. Mutate the
            // fromlist children in place; the state1 sub_states are aligned.
            let f = match jtnode {
                Node::FromExpr(f) => f,
                _ => unreachable!(),
            };
            debug_assert_eq!(f.fromlist.len(), state1.sub_states.len());
            for (l, sub_state) in f.fromlist.iter_mut().zip(state1.sub_states.iter()) {
                if sub_state.contains_outer {
                    reduce_outer_joins_pass2(
                        mcx,
                        l,
                        sub_state,
                        state2,
                        parse,
                        pass_nonnullable_rels.as_deref(),
                        &pass_forced_null_vars,
                    )?;
                }
            }
            // bms_free(pass_nonnullable_rels) — owned, dropped here.
            let _ = pass_nonnullable_rels.take();
            // can't so easily clean up var lists, unfortunately
        }
        Node::JoinExpr(_) => {
            reduce_outer_joins_pass2_joinexpr(
                mcx,
                jtnode,
                state1,
                state2,
                parse,
                nonnullable_rels,
                forced_null_vars,
            )?;
        }
        _ => {
            return Err(types_error::PgError::error("unrecognized node type"));
        }
    }
    Ok(())
}

/// The `IsA(jtnode, JoinExpr)` arm of [`reduce_outer_joins_pass2`].
#[allow(clippy::too_many_arguments)]
fn reduce_outer_joins_pass2_joinexpr<'mcx>(
    mcx: Mcx<'mcx>,
    jtnode: &mut Node<'mcx>,
    state1: &ReduceOuterJoinsPass1State<'mcx>,
    state2: &mut ReduceOuterJoinsPass2State,
    parse: &mut Query<'mcx>,
    nonnullable_rels: Option<&Bitmapset>,
    forced_null_vars: &MultiBitmapset<'mcx>,
) -> PgResult<()> {
    let rtindex = match jtnode {
        Node::JoinExpr(j) => j.rtindex,
        _ => unreachable!(),
    };
    let orig_jointype = match jtnode {
        Node::JoinExpr(j) => j.jointype,
        _ => unreachable!(),
    };
    let mut jointype = orig_jointype;

    // left_state / right_state, tracked by index so they can be swapped on
    // JOIN_RIGHT -> JOIN_LEFT below.
    let mut left_state: &ReduceOuterJoinsPass1State = &state1.sub_states[0];
    let mut right_state: &ReduceOuterJoinsPass1State = &state1.sub_states[1];

    // Can we simplify this join?
    match jointype {
        JoinType::JOIN_INNER => {}
        JoinType::JOIN_LEFT => {
            if bms_overlap(nonnullable_rels, right_state.relids.as_deref()) {
                jointype = JoinType::JOIN_INNER;
            }
        }
        JoinType::JOIN_RIGHT => {
            if bms_overlap(nonnullable_rels, left_state.relids.as_deref()) {
                jointype = JoinType::JOIN_INNER;
            }
        }
        JoinType::JOIN_FULL => {
            if bms_overlap(nonnullable_rels, left_state.relids.as_deref()) {
                if bms_overlap(nonnullable_rels, right_state.relids.as_deref()) {
                    jointype = JoinType::JOIN_INNER;
                } else {
                    jointype = JoinType::JOIN_LEFT;
                    // Also report partial reduction in state2.
                    report_reduced_full_join(state2, rtindex, right_state.relids.as_deref());
                }
            } else {
                if bms_overlap(nonnullable_rels, right_state.relids.as_deref()) {
                    jointype = JoinType::JOIN_RIGHT;
                    // Also report partial reduction in state2.
                    report_reduced_full_join(state2, rtindex, left_state.relids.as_deref());
                }
            }
        }
        JoinType::JOIN_SEMI | JoinType::JOIN_ANTI => {
            // These could only have been introduced by pull_up_sublinks, so
            // there's no way that upper quals could refer to their righthand
            // sides, and no point in checking. We don't expect to see
            // JOIN_RIGHT_SEMI or JOIN_RIGHT_ANTI yet.
        }
        _ => {
            return Err(types_error::PgError::error("unrecognized join type"));
        }
    }

    // Convert JOIN_RIGHT to JOIN_LEFT. Note that in the case where we reduced
    // JOIN_FULL to JOIN_RIGHT, this will mean the JoinExpr no longer matches the
    // internal ordering of any CoalesceExpr's built to represent merged join
    // variables. We don't care about that at present, but be wary of it ...
    if jointype == JoinType::JOIN_RIGHT {
        if let Node::JoinExpr(j) = jtnode {
            let tmparg = j.larg.take();
            j.larg = j.rarg.take();
            j.rarg = tmparg;
        }
        jointype = JoinType::JOIN_LEFT;
        right_state = &state1.sub_states[0];
        left_state = &state1.sub_states[1];
    }

    // See if we can reduce JOIN_LEFT to JOIN_ANTI. This is the case if the
    // join's own quals are strict for any var that was forced null by higher
    // qual levels.
    if jointype == JoinType::JOIN_LEFT {
        let nonnullable_vars = {
            let j_quals = match jtnode {
                Node::JoinExpr(j) => &j.quals,
                _ => unreachable!(),
            };
            // Find Vars in j->quals that must be non-null in joined rows.
            find_nonnullable_vars(mcx, qual_as_expr(j_quals))?
        };

        // It's not sufficient to check whether nonnullable_vars and
        // forced_null_vars overlap: we need to know if the overlap includes any
        // RHS variables.
        let overlap = mbms_overlap_sets(mcx, &nonnullable_vars, forced_null_vars)?;
        if bms_overlap(overlap.as_deref(), right_state.relids.as_deref()) {
            jointype = JoinType::JOIN_ANTI;
        }
    }

    // Apply the jointype change, if any, to both jointree node and RTE. Also, if
    // we changed an RTE to INNER, add its RTI to inner_reduced.
    if rtindex != 0 && jointype != orig_jointype {
        let rte = &mut parse.rtable[(rtindex - 1) as usize];
        debug_assert_eq!(rte.rtekind, RTEKind::RTE_JOIN);
        debug_assert_eq!(rte.jointype, orig_jointype);
        rte.jointype = jointype;
        if jointype == JoinType::JOIN_INNER {
            state2.inner_reduced = expr_relids_add_member(
                core::mem::take(&mut state2.inner_reduced),
                rtindex,
            );
        }
    }
    if let Node::JoinExpr(j) = jtnode {
        j.jointype = jointype;
    }

    // Only recurse if there's more to do below here.
    if left_state.contains_outer || right_state.contains_outer {
        // If this join is (now) inner, we can add any constraints its quals
        // provide to those we got from above. But if it is outer, we can pass
        // down the local constraints only into the nullable side, because an
        // outer join never eliminates any rows from its non-nullable side. Also,
        // there is no point in passing upper constraints into the nullable side
        // ... The upshot is that we pass either the local or the upper
        // constraints, never both, to the children of an outer join.
        //
        // Note that a SEMI join works like an inner join here. At a FULL join we
        // just punt and pass nothing down.
        let local_nonnullable_rels: Relids;
        let local_forced_null_vars: MultiBitmapset;
        if jointype != JoinType::JOIN_FULL {
            // Compute the local constraints from j->quals under a scoped
            // immutable borrow of the JoinExpr (no clone needed; the children
            // aren't touched here).
            let (mut lnn, mut lfn) = {
                let j_quals = match jtnode {
                    Node::JoinExpr(j) => &j.quals,
                    _ => unreachable!(),
                };
                (
                    find_nonnullable_rels(mcx, qual_as_expr(j_quals))?,
                    find_forced_null_vars(mcx, qual_as_expr(j_quals))?,
                )
            };
            if jointype == JoinType::JOIN_INNER || jointype == JoinType::JOIN_SEMI {
                // OK to merge upper and local constraints.
                lnn = bms_add_members(mcx, lnn, nonnullable_rels)?;
                lfn = mbms_add_members(mcx, lfn, forced_null_vars)?;
            }
            local_nonnullable_rels = lnn;
            local_forced_null_vars = lfn;
        } else {
            // no use in calculating these
            local_nonnullable_rels = None;
            local_forced_null_vars = empty_mbms(mcx);
        }

        let empty_fnv = empty_mbms(mcx);

        if left_state.contains_outer {
            let (pass_nonnullable_rels, pass_forced_null_vars): (
                Option<&Bitmapset>,
                &MultiBitmapset,
            ) = if jointype == JoinType::JOIN_INNER || jointype == JoinType::JOIN_SEMI {
                // pass union of local and upper constraints
                (local_nonnullable_rels.as_deref(), &local_forced_null_vars)
            } else if jointype != JoinType::JOIN_FULL {
                // ie, LEFT or ANTI: can't pass local constraints to non-nullable
                // side
                (nonnullable_rels, forced_null_vars)
            } else {
                // no constraints pass through JOIN_FULL
                (None, &empty_fnv)
            };
            // Recurse into j->larg (after any JOIN_RIGHT swap above, larg holds
            // the node aligned with left_state).
            let mut larg = match jtnode {
                Node::JoinExpr(j) => j.larg.take(),
                _ => unreachable!(),
            };
            if let Some(child) = larg.as_deref_mut() {
                reduce_outer_joins_pass2(
                    mcx,
                    child,
                    left_state,
                    state2,
                    parse,
                    pass_nonnullable_rels,
                    pass_forced_null_vars,
                )?;
            }
            if let Node::JoinExpr(j) = jtnode {
                j.larg = larg;
            }
        }

        if right_state.contains_outer {
            let (pass_nonnullable_rels, pass_forced_null_vars): (
                Option<&Bitmapset>,
                &MultiBitmapset,
            ) = if jointype != JoinType::JOIN_FULL {
                // ie, INNER/LEFT/SEMI/ANTI: pass appropriate constraints, per
                // comment above
                (local_nonnullable_rels.as_deref(), &local_forced_null_vars)
            } else {
                // no constraints pass through JOIN_FULL
                (None, &empty_fnv)
            };
            let mut rarg = match jtnode {
                Node::JoinExpr(j) => j.rarg.take(),
                _ => unreachable!(),
            };
            if let Some(child) = rarg.as_deref_mut() {
                reduce_outer_joins_pass2(
                    mcx,
                    child,
                    right_state,
                    state2,
                    parse,
                    pass_nonnullable_rels,
                    pass_forced_null_vars,
                )?;
            }
            if let Node::JoinExpr(j) = jtnode {
                j.rarg = rarg;
            }
        }
        // bms_free(local_nonnullable_rels) — owned, dropped here.
        drop(local_nonnullable_rels);
    }

    Ok(())
}

/// `report_reduced_full_join(state2, rtindex, relids)` (prepjointree.c:3526).
fn report_reduced_full_join(
    state2: &mut ReduceOuterJoinsPass2State,
    rtindex: i32,
    relids: Option<&Bitmapset>,
) {
    state2.partial_reduced.push(ReduceOuterJoinsPartialState {
        full_join_rti: rtindex,
        unreduced_side: relids_to_expr_relids(relids),
    });
}

// ===========================================================================
// ExprRelids small helpers (the relids that feed remove_nulling_relids).
// ===========================================================================

#[inline]
fn expr_relids_is_empty(a: &ExprRelids) -> bool {
    a.words.iter().all(|&w| w == 0)
}

#[inline]
fn expr_relids_make_singleton(x: i32) -> ExprRelids {
    expr_relids_add_member(ExprRelids { words: Vec::new() }, x)
}

#[inline]
fn expr_relids_add_member(mut a: ExprRelids, x: i32) -> ExprRelids {
    debug_assert!(x >= 0);
    let wnum = (x / 64) as usize;
    let bit = (x % 64) as u32;
    if wnum >= a.words.len() {
        a.words.resize(wnum + 1, 0);
    }
    a.words[wnum] |= 1u64 << bit;
    a
}

/// An empty `MultiBitmapset` (C `NIL`).
#[inline]
fn empty_mbms<'mcx>(mcx: Mcx<'mcx>) -> MultiBitmapset<'mcx> {
    mcx::PgVec::new_in(mcx)
}

// ===========================================================================
// seam installation
// ===========================================================================

/// Install this unit's inward seams.
pub fn init_seams() {
    backend_optimizer_prep_prepjointree_seams::reduce_outer_joins::set(reduce_outer_joins);
    backend_optimizer_prep_prepjointree_seams::remove_useless_result_rtes::set(
        result_rtes::remove_useless_result_rtes,
    );
    backend_optimizer_prep_prepjointree_seams::pull_up_sublinks::set(sublinks::pull_up_sublinks);
    backend_optimizer_prep_prepjointree_seams::pull_up_subqueries::set(pullup::pull_up_subqueries);
}
