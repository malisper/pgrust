//! Uniqueness proofs + the EC-member index-column callback (indxpath.c).

use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_nodes::primnodes::Expr;
use types_pathnodes::{
    EquivalenceClass, EquivalenceMember, IndexOptInfo, NodeId, PlannerInfo, RelId, RinfoId,
};

use backend_nodes_core::bitmapset::BMS_Membership;
use backend_utils_cache_lsyscache_seams::op_in_opfamily;

use crate::operand::match_index_to_operand;
use crate::util::{index_coll_matches_expr_coll, BTREE_AM_OID};

/// `get_leftop(clause)` (clauses.h) — the left operand of a binary `OpExpr`.
fn get_leftop<'a, 'mcx>(clause: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    clause
        .as_opexpr()
        .expect("get_leftop: not an OpExpr")
        .args
        .first()
        .expect("get_leftop: OpExpr must have a left operand")
}

/// `get_rightop(clause)` (clauses.h) — the right operand of a binary `OpExpr`.
fn get_rightop<'a, 'mcx>(clause: &'a Expr<'mcx>) -> &'a Expr<'mcx> {
    clause
        .as_opexpr()
        .expect("get_rightop: not an OpExpr")
        .args
        .get(1)
        .expect("get_rightop: OpExpr must have a right operand")
}

/// `bms_is_empty(set)` over the planner `Relids` (`Option<Box<Bitmapset>>`).
fn relids_is_empty(set: &types_pathnodes::Relids) -> bool {
    match set {
        None => true,
        Some(b) => b.words.iter().all(|w| *w == 0),
    }
}

/// `bms_membership(set)` over the planner `Relids`.
fn relids_membership(set: &types_pathnodes::Relids) -> BMS_Membership {
    match set {
        None => BMS_Membership::BMS_EMPTY_SET,
        Some(b) => {
            let n: u32 = b.words.iter().map(|w| w.count_ones()).sum();
            match n {
                0 => BMS_Membership::BMS_EMPTY_SET,
                1 => BMS_Membership::BMS_SINGLETON,
                _ => BMS_Membership::BMS_MULTIPLE,
            }
        }
    }
}

/// `ec_member_matches_indexcol(root, rel, ec, em, arg)` (indxpath.c:4091) — the
/// `generate_implied_equalities_for_column` callback: test whether an
/// `EquivalenceClass` member matches an index column. The C `arg` carries
/// `index` + `indexcol`, passed directly here. (`rel` is unused by C too.)
pub fn ec_member_matches_indexcol(
    root: &PlannerInfo,
    _rel: RelId,
    ec: &EquivalenceClass,
    em: &EquivalenceMember,
    index: &IndexOptInfo,
    indexcol: usize,
) -> bool {
    let cur_family = index.opfamily[indexcol];
    let cur_collation = index.indexcollations[indexcol];

    // For a btree index, reject if its opfamily isn't compatible with the EC.
    if index.relam == BTREE_AM_OID && !ec.ec_opfamilies.contains(&cur_family) {
        return false;
    }

    // We insist on collation match for all index types, though.
    if !index_coll_matches_expr_coll(cur_collation, ec.ec_collation) {
        return false;
    }

    // Borrow the EM expr for the read-only `match_index_to_operand` (a derived
    // `.clone()` would panic on an owned-subtree child).
    let em_expr: &Expr = root.node(em.em_expr);
    match_index_to_operand(root, em_expr, indexcol, index)
}

/// `relation_has_unique_index_for(root, rel, restrictlist, exprlist, oprlist)`
/// (indxpath.c:4149) — thin wrapper passing a NULL `extra_clauses`.
pub fn relation_has_unique_index_for(
    root: &mut PlannerInfo,
    rel: RelId,
    restrictlist: &[RinfoId],
    exprlist: &[NodeId],
    oprlist: &[Oid],
) -> bool {
    relation_has_unique_index_ext(root, rel, restrictlist, exprlist, oprlist, None)
}

/// `relation_has_unique_index_ext(root, rel, restrictlist, exprlist, oprlist,
/// extra_clauses)` (indxpath.c:4163) — return `true` if the rel is provably
/// unique for the given (already-mergejoinable) restriction clauses plus the
/// `exprlist`/`oprlist` expression conditions.
///
/// `root` is `&mut` to preserve C's side effect of stamping `outer_is_left` onto
/// each admitted baserestrictinfo clause; `extra_clauses` collects the
/// single-rel filter clauses that contributed to uniqueness.
pub fn relation_has_unique_index_ext(
    root: &mut PlannerInfo,
    rel: RelId,
    restrictlist: &[RinfoId],
    exprlist: &[NodeId],
    oprlist: &[Oid],
    mut extra_clauses: Option<&mut Vec<RinfoId>>,
) -> bool {
    debug_assert_eq!(exprlist.len(), oprlist.len());

    // Short-circuit if no indexes...
    if root.rel(rel).indexlist.is_empty() {
        return false;
    }

    // The candidate restriction clauses to match: the caller-supplied
    // `restrictlist`, plus any usable baserestrictinfo `var = const` clauses we
    // admit below.
    let mut restrictlist: Vec<RinfoId> = restrictlist.to_vec();

    // Examine the rel's restriction clauses for usable var = const clauses that
    // we can add to the restrictlist. (First pass: mutates `outer_is_left`.)
    let baserestrictinfo = root.rel(rel).baserestrictinfo.clone();
    for ric in baserestrictinfo {
        // Note: can_join won't be set for a restriction clause, but
        // mergeopfamilies will be if it has a mergejoinable operator and doesn't
        // contain volatile functions.
        if root.rinfo(ric).mergeopfamilies.is_empty() {
            continue; // not mergejoinable
        }

        // The clause certainly doesn't refer to anything but the given rel. If
        // either side is empty, it's a var = something or something = var.
        let outer_is_left = {
            let r = root.rinfo(ric);
            if relids_is_empty(&r.left_relids) {
                true // righthand side is inner
            } else if relids_is_empty(&r.right_relids) {
                false // lefthand side is inner
            } else {
                continue;
            }
        };
        root.rinfo_mut(ric).outer_is_left = outer_is_left;

        // OK, add to list.
        restrictlist.push(ric);
    }

    // Short-circuit the easy case.
    if restrictlist.is_empty() && exprlist.is_empty() {
        return false;
    }

    // Examine each index of the relation ... (immutable pass).
    let root: &PlannerInfo = root;
    let relinfo = root.rel(rel);
    for ind in &relinfo.indexlist {
        // If the index is not unique, or not immediately enforced, or if it's a
        // partial index, it's useless here.
        if !ind.unique || !ind.immediate || !ind.indpred.is_empty() {
            continue;
        }

        // Accumulated single-rel filter clauses contributing to uniqueness.
        let mut exprs: Vec<RinfoId> = Vec::new();

        // Try to find each index column in the lists of conditions.
        let mut c = 0usize;
        let nkeycolumns = ind.nkeycolumns as usize;
        while c < nkeycolumns {
            let mut matched = false;

            for &ric in &restrictlist {
                let rinfo = root.rinfo(ric);

                // The condition's equality operator must be a member of the
                // index opfamily.
                if !rinfo.mergeopfamilies.contains(&ind.opfamily[c]) {
                    continue;
                }

                // XXX at some point we may need to check collations here too.

                let clause = root.node(rinfo.clause);

                // OK, see if the condition operand matches the index key.
                let rexpr: &Expr = if rinfo.outer_is_left {
                    get_rightop(clause)
                } else {
                    get_leftop(clause)
                };

                if match_index_to_operand(root, rexpr, c, ind) {
                    matched = true; // column is unique

                    if relids_membership(&rinfo.clause_relids) == BMS_Membership::BMS_SINGLETON {
                        // Add filter clause into a list allowing caller to know
                        // if uniqueness has been made not only by join clauses.
                        debug_assert!(
                            relids_is_empty(&rinfo.left_relids)
                                || relids_is_empty(&rinfo.right_relids)
                        );
                        if extra_clauses.is_some() {
                            exprs.push(ric);
                        }
                    }

                    break;
                }
            }

            if matched {
                c += 1;
                continue;
            }

            // Otherwise, try to match the index column against the
            // exprlist/oprlist conditions. (forboth)
            for (expr_id, &opr) in exprlist.iter().zip(oprlist.iter()) {
                let expr = root.node(*expr_id);
                // See if the expression matches the index key.
                if !match_index_to_operand(root, expr, c, ind) {
                    continue;
                }

                // The equality operator must be a member of the index opfamily.
                if !op_in_opfamily::call(opr, ind.opfamily[c]).expect("op_in_opfamily") {
                    continue;
                }

                // XXX at some point we may need to check collations here too.

                matched = true; // column is unique
                break;
            }

            if !matched {
                break; // no match; this index doesn't help us
            }
            c += 1;
        }

        // Matched all key columns of this index?
        if c == nkeycolumns {
            if let Some(out) = extra_clauses.as_mut() {
                **out = exprs;
            }
            return true;
        }
    }

    // No indexes...
    false
}
