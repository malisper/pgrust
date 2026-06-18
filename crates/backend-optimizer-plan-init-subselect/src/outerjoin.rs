//! OUTER JOIN INFO (initsplan.c) — `make_outerjoininfo`,
//! `compute_semijoin_info`.
//!
//! # Model reconciliation (read before editing)
//!
//! C `make_outerjoininfo`/`compute_semijoin_info` are `static` helpers driven
//! by `deconstruct_distribute` (see [`crate::jointree`]). The C signatures take
//! the outer join's join condition as a `List *clause` in implicit-AND format;
//! this repo carries that as an owned conjunct list `&[Expr]` (lifetime-free
//! [`Expr`]), exactly as the jointree pass builds it via `make_ands_implicit`.
//!
//! Both entry points additionally take `run: &PlannerRun<'_>` because the
//! `root->parse->rowMarks` check needs the parse tree, and `root.parse` is the
//! opaque [`QueryId`](types_pathnodes) resolved through the
//! [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) carrier (task #264).
//!
//! ## Whole-`List` callees that have only per-`Expr` seams
//!
//! Several C calls pass `(Node *) clause` — the entire implicit-AND list as one
//! Node. The available seams operate on a single [`Expr`]; we replicate the
//! whole-list semantics conjunct-by-conjunct:
//!
//! * `pull_varnos(root, (Node *) clause)` → union of `pull_varnos` over each
//!   conjunct (the relids of an AND are the union of its arms' relids).
//! * `find_nonnullable_rels((Node *) clause)` → union over each conjunct. C's
//!   `find_nonnullable_rels` of an `AND` (the implicit-AND list view) returns
//!   the union of the per-arm nonnullable sets (a rel is non-nullable for an AND
//!   iff some conjunct is strict for it). Routed through the per-`Expr`
//!   `initext::find_nonnullable_rels_expr` cycle-break seam.
//! * `contain_placeholder_references_to(root, (Node *) clause, ojrelid)` →
//!   logical OR over each conjunct (any conjunct referencing the OJ taints the
//!   whole list). Reaches the ported joininfo owner directly (acyclic).
//! * `contain_volatile_functions((Node *) op)` is already over one `Expr` (each
//!   loop iteration's clause); `contain_volatile_functions((Node *)
//!   semi_rhs_exprs)` over the RHS list → OR over each collected expr.
//!
//! `get_mergejoin_opfamilies(opno)` needs an `Mcx`; we charge it to a transient
//! `MemoryContext` and test emptiness, mirroring the equivclass/pathkeys idiom.

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::primnodes::{Expr, ExprRelids};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    Bitmapset, PlannerInfo, Relids, SpecialJoinInfo, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT,
    JOIN_RIGHT, JOIN_SEMI,
};

use backend_optimizer_path_equivclass_ext_seams as eqext;
use backend_optimizer_plan_init_subselect_ext_seams as initext;
use backend_optimizer_util_pathnode_seams as psnode;
use backend_optimizer_util_relnode_seams as bms;
use backend_utils_cache_lsyscache_seams as lsc;

use backend_optimizer_util_joininfo::placeholder::contain_placeholder_references_to;

/// `make_outerjoininfo` (initsplan.c:1707).
///
/// Build a `SpecialJoinInfo` for the current outer join. Invoked bottom-up, so
/// `root.join_info_list` already contains entries for all syntactically-lower
/// outer joins.
///
/// Returns [`PgResult`] because the `FOR [KEY] UPDATE/SHARE on the nullable side
/// of an outer join` check `ereport(ERROR)`s, and the semijoin analysis calls
/// `?`-propagating lsyscache seams.
#[allow(clippy::too_many_arguments)]
pub fn make_outerjoininfo(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    left_rels: &Relids,
    right_rels: &Relids,
    inner_join_rels: &Relids,
    jointype: types_pathnodes::JoinType,
    rtindex: i32,
    clause: &[Expr],
) -> PgResult<SpecialJoinInfo> {
    // We should not see RIGHT JOIN here because left/right were switched earlier.
    debug_assert!(jointype != JOIN_INNER);
    debug_assert!(jointype != JOIN_RIGHT);

    let ojrelid = rtindex;

    // Presently the executor cannot support FOR [KEY] UPDATE/SHARE marking of
    // rels appearing on the nullable side of an outer join. Complain if any
    // nullable rel is FOR [KEY] UPDATE/SHARE. We use the original RowMarkClause
    // list (root->parse->rowMarks), not the PlanRowMark list.
    for rm in run.resolve(root.parse).rowMarks.iter() {
        if let Some(rc) = (**rm).as_rowmarkclause() {
            let rti = rc.rti as i32;
            if bms::relids_is_member::call(rti, right_rels)
                || (jointype == JOIN_FULL && bms::relids_is_member::call(rti, left_rels))
            {
                return Err(types_error::pg_error::PgError::error(alloc::format!(
                    "{} cannot be applied to the nullable side of an outer join",
                    lcs_as_string(rc.strength),
                )));
            }
        }
    }

    let mut sjinfo = SpecialJoinInfo {
        min_lefthand: None,
        min_righthand: None,
        syn_lefthand: bms::relids_copy::call(left_rels),
        syn_righthand: bms::relids_copy::call(right_rels),
        jointype,
        ojrelid: ojrelid as u32,
        // these fields may get added to later:
        commute_above_l: None,
        commute_above_r: None,
        commute_below_l: None,
        commute_below_r: None,
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: Vec::new(),
        semi_rhs_exprs: Vec::new(),
    };

    compute_semijoin_info(root, &mut sjinfo, clause)?;

    // If it's a full join, no need to be very smart.
    if jointype == JOIN_FULL {
        sjinfo.min_lefthand = bms::relids_copy::call(left_rels);
        sjinfo.min_righthand = bms::relids_copy::call(right_rels);
        sjinfo.lhs_strict = false; // don't care about this
        return Ok(sjinfo);
    }

    // Retrieve all relids mentioned within the join clause.
    let clause_relids = pull_varnos_clause(root, clause);

    // For which relids is the clause strict, ie, it cannot succeed if the rel's
    // columns are all NULL?
    let strict_relids = find_nonnullable_rels_clause(clause);

    // Remember whether the clause is strict for any LHS relations.
    sjinfo.lhs_strict = bms::relids_overlap::call(&strict_relids, left_rels);

    // Required LHS always includes the LHS rels mentioned in the clause. We may
    // have to add more rels based on lower outer joins; see below.
    let mut min_lefthand = bms::relids_intersect::call(&clause_relids, left_rels);

    // Similarly for required RHS. But here, we must also include any lower inner
    // joins, to ensure we don't try to commute with any of them.
    let mut min_righthand = bms::relids_int_members::call(
        bms::relids_union::call(&clause_relids, inner_join_rels),
        right_rels,
    );

    // Now check previous outer joins for ordering restrictions.
    //
    // commute_below_l and commute_below_r accumulate the relids of lower outer
    // joins that we think this one can commute with. These decisions are just
    // tentative within this loop, since we might find an intermediate outer join
    // that prevents commutation. Surviving relids will get merged into the
    // SpecialJoinInfo structs afterwards.
    let mut commute_below_l: Relids = None;
    let mut commute_below_r: Relids = None;

    // Snapshot join_info_list (immutable scan; sjinfo is not yet in it).
    let others: Vec<SpecialJoinInfo> = root.join_info_list.clone();
    for otherinfo in &others {
        // A full join is an optimization barrier: we can't associate into or out
        // of it. Hence, if it overlaps either LHS or RHS of the current rel,
        // expand that side's min relset to cover the whole full join.
        if otherinfo.jointype == JOIN_FULL {
            debug_assert!(otherinfo.ojrelid != 0);
            if bms::relids_overlap::call(left_rels, &otherinfo.syn_lefthand)
                || bms::relids_overlap::call(left_rels, &otherinfo.syn_righthand)
            {
                min_lefthand = bms::relids_add_members::call(min_lefthand, &otherinfo.syn_lefthand);
                min_lefthand =
                    bms::relids_add_members::call(min_lefthand, &otherinfo.syn_righthand);
                min_lefthand = bms::relids_add_member::call(min_lefthand, otherinfo.ojrelid as i32);
            }
            if bms::relids_overlap::call(right_rels, &otherinfo.syn_lefthand)
                || bms::relids_overlap::call(right_rels, &otherinfo.syn_righthand)
            {
                min_righthand =
                    bms::relids_add_members::call(min_righthand, &otherinfo.syn_lefthand);
                min_righthand =
                    bms::relids_add_members::call(min_righthand, &otherinfo.syn_righthand);
                min_righthand =
                    bms::relids_add_member::call(min_righthand, otherinfo.ojrelid as i32);
            }
            // Needn't do anything else with the full join.
            continue;
        }

        // If our join condition contains any PlaceHolderVars that need to be
        // evaluated above the lower OJ, then we can't commute with it.
        let have_unsafe_phvs = if otherinfo.ojrelid != 0 {
            contain_placeholder_references_to_clause(root, clause, otherinfo.ojrelid as i32)
        } else {
            false
        };

        // For a lower OJ in our LHS, if our join condition uses the lower join's
        // RHS and is not strict for that rel, we must preserve the ordering of
        // the two OJs, so add lower OJ's full syntactic relset to min_lefthand.
        // (We must use its full syntactic relset, not just its min_lefthand +
        // min_righthand. This is because there might be other OJs below this one
        // that this one can commute with, but we cannot commute with them if we
        // don't with this one.) Also, if we have unsafe PHVs or the current join
        // is a semijoin or antijoin, we must preserve ordering regardless of
        // strictness.
        //
        // When we don't need to preserve ordering, check to see if outer join
        // identity 3 applies, and if so, remove the lower OJ's ojrelid from our
        // min_lefthand so that commutation is allowed.
        if bms::relids_overlap::call(left_rels, &otherinfo.syn_righthand) {
            if bms::relids_overlap::call(&clause_relids, &otherinfo.syn_righthand)
                && (have_unsafe_phvs
                    || jointype == JOIN_SEMI
                    || jointype == JOIN_ANTI
                    || !bms::relids_overlap::call(&strict_relids, &otherinfo.min_righthand))
            {
                // Preserve ordering.
                min_lefthand =
                    bms::relids_add_members::call(min_lefthand, &otherinfo.syn_lefthand);
                min_lefthand =
                    bms::relids_add_members::call(min_lefthand, &otherinfo.syn_righthand);
                if otherinfo.ojrelid != 0 {
                    min_lefthand =
                        bms::relids_add_member::call(min_lefthand, otherinfo.ojrelid as i32);
                }
            } else if jointype == JOIN_LEFT
                && otherinfo.jointype == JOIN_LEFT
                && bms::relids_overlap::call(&strict_relids, &otherinfo.min_righthand)
                && !bms::relids_overlap::call(&clause_relids, &otherinfo.syn_lefthand)
            {
                // Identity 3 applies, so remove the ordering restriction.
                min_lefthand = del_member(min_lefthand, otherinfo.ojrelid as i32);
                // Record the (still tentative) commutability relationship.
                commute_below_l =
                    bms::relids_add_member::call(commute_below_l, otherinfo.ojrelid as i32);
            }
        }

        // For a lower OJ in our RHS, if our join condition does not use the lower
        // join's RHS and the lower OJ's join condition is strict, we can
        // interchange the ordering of the two OJs; otherwise we must add the
        // lower OJ's full syntactic relset to min_righthand.
        //
        // Also, if our join condition does not use the lower join's LHS either,
        // force the ordering to be preserved. Otherwise we can end up with
        // SpecialJoinInfos with identical min_righthands, which can confuse
        // join_is_legal.
        //
        // Also, we must preserve ordering anyway if we have unsafe PHVs, or if
        // either this join or the lower OJ is a semijoin or antijoin.
        //
        // When we don't need to preserve ordering, check to see if outer join
        // identity 3 applies, and if so, remove the lower OJ's ojrelid from our
        // min_righthand so that commutation is allowed.
        if bms::relids_overlap::call(right_rels, &otherinfo.syn_righthand) {
            if bms::relids_overlap::call(&clause_relids, &otherinfo.syn_righthand)
                || !bms::relids_overlap::call(&clause_relids, &otherinfo.min_lefthand)
                || have_unsafe_phvs
                || jointype == JOIN_SEMI
                || jointype == JOIN_ANTI
                || otherinfo.jointype == JOIN_SEMI
                || otherinfo.jointype == JOIN_ANTI
                || !otherinfo.lhs_strict
            {
                // Preserve ordering.
                min_righthand =
                    bms::relids_add_members::call(min_righthand, &otherinfo.syn_lefthand);
                min_righthand =
                    bms::relids_add_members::call(min_righthand, &otherinfo.syn_righthand);
                if otherinfo.ojrelid != 0 {
                    min_righthand =
                        bms::relids_add_member::call(min_righthand, otherinfo.ojrelid as i32);
                }
            } else if jointype == JOIN_LEFT
                && otherinfo.jointype == JOIN_LEFT
                && otherinfo.lhs_strict
            {
                // Identity 3 applies, so remove the ordering restriction.
                min_righthand = del_member(min_righthand, otherinfo.ojrelid as i32);
                // Record the (still tentative) commutability relationship.
                commute_below_r =
                    bms::relids_add_member::call(commute_below_r, otherinfo.ojrelid as i32);
            }
        }
    }

    // Examine PlaceHolderVars. If a PHV is supposed to be evaluated within this
    // join's nullable side, then ensure that min_righthand contains the full
    // eval_at set of the PHV. This ensures that the PHV actually can be evaluated
    // within the RHS. Note that this works only because we should already have
    // determined the final eval_at level for any PHV syntactically within this
    // join.
    let ph_eval: Vec<(Relids, Relids)> = root
        .placeholder_list
        .iter()
        .map(|&phid| {
            let phinfo = root.phinfo(phid);
            let ph_syn_level = expr_relids_to_relids(&phinfo.ph_var.phrels);
            (ph_syn_level, bms::relids_copy::call(&phinfo.ph_eval_at))
        })
        .collect();
    for (ph_syn_level, ph_eval_at) in ph_eval {
        // Ignore placeholder if it didn't syntactically come from RHS.
        if !bms::relids_is_subset::call(&ph_syn_level, right_rels) {
            continue;
        }
        // Else, prevent join from being formed before we eval the PHV.
        min_righthand = bms::relids_add_members::call(min_righthand, &ph_eval_at);
    }

    // If we found nothing to put in min_lefthand, punt and make it the full LHS,
    // to avoid having an empty min_lefthand which will confuse later processing.
    // (We don't try to be smart about such cases, just correct.) Likewise for
    // min_righthand.
    if bms::relids_is_empty::call(&min_lefthand) {
        min_lefthand = bms::relids_copy::call(left_rels);
    }
    if bms::relids_is_empty::call(&min_righthand) {
        min_righthand = bms::relids_copy::call(right_rels);
    }

    // Now they'd better be nonempty.
    debug_assert!(!bms::relids_is_empty::call(&min_lefthand));
    debug_assert!(!bms::relids_is_empty::call(&min_righthand));
    // Shouldn't overlap either.
    debug_assert!(!bms::relids_overlap::call(&min_lefthand, &min_righthand));

    sjinfo.min_lefthand = min_lefthand;
    sjinfo.min_righthand = min_righthand;

    // Now that we've identified the correct min_lefthand and min_righthand, any
    // commute_below_l or commute_below_r relids that have not gotten added back
    // into those sets (due to intervening outer joins) are indeed commutable with
    // this one.
    //
    // First, delete any subsequently-added-back relids (this is easier than
    // maintaining commute_below_l/r precisely through all the above).
    //
    // `bms_del_members(commute_below_l, min_lefthand)` == set difference.
    commute_below_l =
        bms::relids_difference::call(&commute_below_l, &sjinfo.min_lefthand);
    commute_below_r =
        bms::relids_difference::call(&commute_below_r, &sjinfo.min_righthand);

    // Anything left?
    if !bms::relids_is_empty::call(&commute_below_l)
        || !bms::relids_is_empty::call(&commute_below_r)
    {
        // Yup, so we must update the derived data in the SpecialJoinInfos.
        let cb_l = bms::relids_copy::call(&commute_below_l);
        let cb_r = bms::relids_copy::call(&commute_below_r);
        sjinfo.commute_below_l = commute_below_l;
        sjinfo.commute_below_r = commute_below_r;
        for otherinfo in root.join_info_list.iter_mut() {
            if bms::relids_is_member::call(otherinfo.ojrelid as i32, &cb_l) {
                otherinfo.commute_above_l =
                    bms::relids_add_member::call(otherinfo.commute_above_l.take(), ojrelid);
            } else if bms::relids_is_member::call(otherinfo.ojrelid as i32, &cb_r) {
                otherinfo.commute_above_r =
                    bms::relids_add_member::call(otherinfo.commute_above_r.take(), ojrelid);
            }
        }
    }

    Ok(sjinfo)
}

/// `compute_semijoin_info` (initsplan.c:2047).
///
/// Fill semijoin-related fields of a new `SpecialJoinInfo`. Relies on only the
/// jointype and syn_righthand fields of the SpecialJoinInfo; the rest may not be
/// set yet.
///
/// The RHS unique-ification expressions are interned into the planner node arena
/// via `root.alloc_node` (no parse-tree read is required here, so this takes no
/// `PlannerRun`).
pub fn compute_semijoin_info(
    root: &mut PlannerInfo,
    sjinfo: &mut SpecialJoinInfo,
    clause: &[Expr],
) -> PgResult<()> {
    // Initialize semijoin-related fields in case we can't unique-ify.
    sjinfo.semi_can_btree = false;
    sjinfo.semi_can_hash = false;
    sjinfo.semi_operators = Vec::new();
    sjinfo.semi_rhs_exprs = Vec::new();

    // Nothing more to do if it's not a semijoin.
    if sjinfo.jointype != JOIN_SEMI {
        return Ok(());
    }

    // Look to see whether the semijoin's join quals consist of AND'ed equality
    // operators, with (only) RHS variables on only one side of each one. If so,
    // we can figure out how to enforce uniqueness for the RHS.
    //
    // Note that the semi_operators list consists of the joinqual operators
    // themselves (but commuted if needed to put the RHS value on the right).
    // These could be cross-type operators, in which case the operator actually
    // needed for uniqueness is a related single-type operator. We assume here
    // that that operator will be available from the btree or hash opclass when
    // the time comes ... if not, create_unique_plan() will fail.
    let mut semi_operators: Vec<Oid> = Vec::new();
    let mut semi_rhs_exprs: Vec<Expr> = Vec::new();
    let mut all_btree = true;
    let mut all_hash = psnode::enable_hashagg::call(); // don't consider hash if not enabled

    // syn_righthand snapshot (immutable through the loop; sjinfo is borrowed
    // mutably only for the final stores).
    let syn_righthand = bms::relids_copy::call(&sjinfo.syn_righthand);

    for op in clause {
        // Is it a binary opclause?
        let opexpr = op.as_opexpr().filter(|o| o.args.len() == 2);
        let Some(opexpr) = opexpr else {
            // No, but does it reference both sides?
            let all_varnos = eqext::pull_varnos::call(root, op);
            if !bms::relids_overlap::call(&all_varnos, &syn_righthand)
                || bms::relids_is_subset::call(&all_varnos, &syn_righthand)
            {
                // Clause refers to only one rel, so ignore it --- unless it
                // contains volatile functions, in which case we'd better punt.
                if eqext::contain_volatile_functions::call(op) {
                    return Ok(());
                }
                continue;
            }
            // Non-operator clause referencing both sides, must punt.
            return Ok(());
        };

        // Extract data from binary opclause.
        let mut opno = opexpr.opno;
        let left_expr = opexpr.args[0].clone();
        let mut right_expr = opexpr.args[1].clone();
        let left_varnos = eqext::pull_varnos::call(root, &left_expr);
        let right_varnos = eqext::pull_varnos::call(root, &right_expr);
        let all_varnos = bms::relids_union::call(&left_varnos, &right_varnos);
        let opinputtype = eqext::expr_type::call(&left_expr);

        // Does it reference both sides?
        if !bms::relids_overlap::call(&all_varnos, &syn_righthand)
            || bms::relids_is_subset::call(&all_varnos, &syn_righthand)
        {
            // Clause refers to only one rel, so ignore it --- unless it contains
            // volatile functions, in which case we'd better punt.
            if eqext::contain_volatile_functions::call(op) {
                return Ok(());
            }
            continue;
        }

        // check rel membership of arguments
        if !bms::relids_is_empty::call(&right_varnos)
            && bms::relids_is_subset::call(&right_varnos, &syn_righthand)
            && !bms::relids_overlap::call(&left_varnos, &syn_righthand)
        {
            // typical case, right_expr is RHS variable
        } else if !bms::relids_is_empty::call(&left_varnos)
            && bms::relids_is_subset::call(&left_varnos, &syn_righthand)
            && !bms::relids_overlap::call(&right_varnos, &syn_righthand)
        {
            // flipped case, left_expr is RHS variable
            opno = lsc::get_commutator::call(opno)?;
            if !oid_is_valid(opno) {
                return Ok(());
            }
            right_expr = left_expr.clone();
        } else {
            // mixed membership of args, punt
            return Ok(());
        }

        // all operators must be btree equality or hash equality
        if all_btree {
            // oprcanmerge is considered a hint...
            if !lsc::op_mergejoinable::call(opno, opinputtype)?
                || get_mergejoin_opfamilies_is_empty(opno)?
            {
                all_btree = false;
            }
        }
        if all_hash {
            // ... but oprcanhash had better be correct
            if !lsc::op_hashjoinable::call(opno, opinputtype)? {
                all_hash = false;
            }
        }
        if !(all_btree || all_hash) {
            return Ok(());
        }

        // so far so good, keep building lists
        semi_operators.push(opno);
        semi_rhs_exprs.push(right_expr);
    }

    // Punt if we didn't find at least one column to unique-ify.
    if semi_rhs_exprs.is_empty() {
        return Ok(());
    }

    // The expressions we'd need to unique-ify mustn't be volatile.
    // `contain_volatile_functions((Node *) semi_rhs_exprs)` over the list ==
    // logical OR over each expression.
    if semi_rhs_exprs
        .iter()
        .any(|e| eqext::contain_volatile_functions::call(e))
    {
        return Ok(());
    }

    // If we get here, we can unique-ify the semijoin's RHS using at least one of
    // sorting and hashing. Save the information about how to do that.
    //
    // `semi_rhs_exprs` is the planner arena-handle list `Vec<NodeId>`; intern the
    // owned RHS expressions (the C `copyObject(right_expr)` analogue: we already
    // hold an owned clone — `root.alloc_node` takes ownership into the arena).
    let rhs_ids: Vec<types_pathnodes::NodeId> = semi_rhs_exprs
        .into_iter()
        .map(|e| root.alloc_node(e))
        .collect();

    sjinfo.semi_can_btree = all_btree;
    sjinfo.semi_can_hash = all_hash;
    sjinfo.semi_operators = semi_operators;
    sjinfo.semi_rhs_exprs = rhs_ids;
    Ok(())
}

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

/// `bms_del_member(a, x)` — relnode-seams has no del_member; mirror it as
/// `a \ {x}` (`relids_difference(a, {x})`), matching the .port-ref guidance and
/// [`crate::jointree`]'s identical helper.
fn del_member(a: Relids, x: i32) -> Relids {
    let single = bms::relids_make_singleton::call(x);
    bms::relids_difference::call(&a, &single)
}

/// `pull_varnos(root, (Node *) clause)` over an implicit-AND `List *` of quals:
/// the relids of an AND-list is the union of the per-conjunct relids.
fn pull_varnos_clause(root: &PlannerInfo, clause: &[Expr]) -> Relids {
    let mut acc: Relids = None;
    for c in clause {
        let v = eqext::pull_varnos::call(root, c);
        acc = bms::relids_add_members::call(acc, &v);
    }
    acc
}

/// `find_nonnullable_rels((Node *) clause)` over an implicit-AND `List *`: a rel
/// is non-nullable for the AND iff it is non-nullable for some conjunct, i.e. the
/// union over conjuncts (`find_nonnullable_rels` of a `BoolExpr(AND)` unions its
/// arms' results). Routed through the per-`Expr` cycle-break seam.
fn find_nonnullable_rels_clause(clause: &[Expr]) -> Relids {
    let mut acc: Relids = None;
    for c in clause {
        let v = initext::find_nonnullable_rels_expr::call(c);
        acc = bms::relids_add_members::call(acc, &v);
    }
    acc
}

/// `contain_placeholder_references_to(root, (Node *) clause, relid)` over an
/// implicit-AND `List *`: any conjunct referencing the OJ taints the whole list
/// (logical OR over conjuncts).
fn contain_placeholder_references_to_clause(
    root: &PlannerInfo,
    clause: &[Expr],
    relid: i32,
) -> bool {
    clause
        .iter()
        .any(|c| contain_placeholder_references_to(root, c, relid))
}

/// `get_mergejoin_opfamilies(opno) == NIL` — charge the `Mcx`-allocating
/// lsyscache seam to a transient context and test emptiness (mirrors the
/// equivclass/pathkeys idiom; the list lands in the planner context in C).
fn get_mergejoin_opfamilies_is_empty(opno: Oid) -> PgResult<bool> {
    let cx = mcx::MemoryContext::new("compute_semijoin_info get_mergejoin_opfamilies transient");
    let v = lsc::get_mergejoin_opfamilies::call(cx.mcx(), opno)?;
    Ok(v.is_empty())
}

/// `OidIsValid(oid)` — a valid Oid is any nonzero value.
fn oid_is_valid(oid: Oid) -> bool {
    oid != 0
}

/// Convert a [`PlaceHolderVar`]'s `phrels` (the lifetime-free [`ExprRelids`])
/// into a [`Relids`], mirroring joininfo's private `expr_relids_to_relids`.
fn expr_relids_to_relids(er: &ExprRelids) -> Relids {
    if er.words.iter().all(|&w| w == 0) {
        None
    } else {
        Some(Box::new(Bitmapset {
            words: er.words.clone(),
        }))
    }
}

/// `LCS_asString(strength)` (analyze.c) — render a `FOR [KEY] UPDATE/SHARE`
/// lock-clause strength as its SQL spelling for the error message. Inlined here
/// (the analyze.c owner is a downstream crate — depending on it would be a
/// cycle); a trivial, faithful mapping.
fn lcs_as_string(strength: types_nodes::rawnodes::LockClauseStrength) -> &'static str {
    use types_nodes::rawnodes::LockClauseStrength::*;
    match strength {
        LCS_NONE => "FOR NO KEY UPDATE", // should not happen here
        LCS_FORKEYSHARE => "FOR KEY SHARE",
        LCS_FORSHARE => "FOR SHARE",
        LCS_FORNOKEYUPDATE => "FOR NO KEY UPDATE",
        LCS_FORUPDATE => "FOR UPDATE",
    }
}
