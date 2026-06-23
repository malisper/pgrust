//! ORDER BY ordering-operator matching (indxpath.c).

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;
use ::nodes::primnodes::{Expr, OpExpr};
use pathnodes::{EmId, IndexOptInfo, NodeId, PathKey, PlannerInfo};

use var_seams::contain_var_clause;
use lsyscache_seams as lsyscache;

use crate::operand::match_index_to_operand;
use crate::util::{index_coll_matches_expr_coll, INVALID_OID};

/// `match_pathkeys_to_index(index, pathkeys, &orderby_clauses, &clause_columns)`
/// (indxpath.c:3718) — for the given index and pathkeys, output a list of
/// suitable ORDER BY expressions (each `indexedcol op pseudoconstant`) along
/// with the index column numbers they'd be used with.
///
/// Returns `(orderby_clauses, clause_columns)`; the derived ordering `OpExpr`s
/// are allocated into the arena (the C list cells point at fresh nodes). `root`
/// is threaded to resolve the EC handle + EquivalenceMember `em_expr` nodes and
/// `index->rel->relids`. A partial (prefix) match is reported by a shorter list.
pub fn match_pathkeys_to_index(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    pathkeys: &[PathKey],
) -> PgResult<(Vec<NodeId>, Vec<i32>)> {
    let mut orderby_clauses: Vec<NodeId> = Vec::new();
    let mut clause_columns: Vec<i32> = Vec::new();

    // Only indexes with the amcanorderbyop property are interesting here.
    if !index.amcanorderbyop {
        return Ok((orderby_clauses, clause_columns));
    }

    let index_rel_relids = root
        .rel(index.rel.expect("IndexOptInfo without rel"))
        .relids
        .clone();

    for pathkey in pathkeys {
        let mut found = false;

        // Pathkey must request default sort order for the target opfamily.
        // PathKey.pk_cmptype is the `pathnodes::CompareType` (= i32);
        // COMPARE_LT == 1 (primnodes CompareType).
        const COMPARE_LT_I32: i32 = 1;
        if pathkey.pk_cmptype != COMPARE_LT_I32 || pathkey.pk_nulls_first {
            return Ok((orderby_clauses, clause_columns));
        }

        let ec_id = match pathkey.pk_eclass {
            Some(e) => e,
            None => return Ok((orderby_clauses, clause_columns)),
        };

        // If eclass is volatile, no hope of using an indexscan.
        if root.ec(ec_id).ec_has_volatile {
            return Ok((orderby_clauses, clause_columns));
        }

        // Collect the EC members to consider: regular members plus child members
        // belonging to the target relation (the C eclass_member_iterator over
        // `index->rel->relids`).
        let members: Vec<EmId> = collect_ec_members(root, ec_id);

        for em_id in members {
            // No possibility of match if it references other relations.
            if !relids_equal(&root.em(em_id).em_relids, &index_rel_relids) {
                continue;
            }

            // Any column of the index may match each pathkey. Deep-copy the
            // EM expr via `clone_in` — it must outlive the `&mut root`
            // `alloc_node` below, and a derived `.clone()` panics on an
            // owned-subtree child.
            let em_expr = root.node(root.em(em_id).em_expr).clone_in(mcx)?;
            let pk_opfamily = pathkey.pk_opfamily;
            let nkeycolumns = index.nkeycolumns as usize;
            for indexcol in 0..nkeycolumns {
                if let Some(expr) =
                    match_clause_to_ordering_op(mcx, root, index, indexcol, &em_expr, pk_opfamily)?
                {
                    let node_id = root.alloc_node(expr);
                    orderby_clauses.push(node_id);
                    clause_columns.push(indexcol as i32);
                    found = true;
                    break;
                }
            }

            if found {
                break;
            }
        }

        // Return the matches found so far when a pathkey couldn't be matched.
        if !found {
            return Ok((orderby_clauses, clause_columns));
        }
    }

    Ok((orderby_clauses, clause_columns))
}

/// Collect the EC members to consider for the target relation: regular members
/// plus all child members (the iterator restricts to the target relids, which we
/// honor via the `em_relids` check at the call site).
fn collect_ec_members(root: &PlannerInfo, ec_id: pathnodes::EcId) -> Vec<EmId> {
    let ec = root.ec(ec_id);
    let mut members: Vec<EmId> = ec.ec_members.clone();
    for childlist in &ec.ec_childmembers {
        members.extend(childlist.iter().copied());
    }
    members
}

/// `match_clause_to_ordering_op(index, indexcol, clause, pk_opfamily)`
/// (indxpath.c:3829) — determine whether an ordering operator expression matches
/// an index column. On success return `clause` as-is (indexkey on left) or a
/// commuted copy; on no match, `None`.
pub fn match_clause_to_ordering_op<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    index: &IndexOptInfo,
    indexcol: usize,
    clause: &Expr<'_>,
    pk_opfamily: Oid,
) -> PgResult<Option<Expr<'mcx>>> {
    debug_assert!(indexcol < index.nkeycolumns as usize);

    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];

    // Clause must be a binary opclause.
    let op = match clause.as_opexpr() {
        Some(op) => op,
        None => return Ok(None),
    };
    if op.args.len() != 2 {
        return Ok(None);
    }
    // Borrow the operands for the read-only matching tests.
    let leftop: &Expr = &op.args[0];
    let rightop: &Expr = &op.args[1];
    let mut expr_op = op.opno;
    let expr_coll = op.inputcollid;

    // Forget it right away if wrong collation.
    if !index_coll_matches_expr_coll(idxcollation, expr_coll) {
        return Ok(None);
    }

    // Check for (indexkey op constant) or (constant op indexkey).
    let commuted;
    if match_index_to_operand(root, leftop, indexcol, index)
        && !contain_var_clause::call(rightop)
        && !contain_volatile(rightop)
    {
        commuted = false;
    } else if match_index_to_operand(root, rightop, indexcol, index)
        && !contain_var_clause::call(leftop)
        && !contain_volatile(leftop)
    {
        // Might match, but we need a commuted operator.
        expr_op = lsyscache::get_commutator::call(expr_op).expect("get_commutator");
        if expr_op == INVALID_OID {
            return Ok(None);
        }
        commuted = true;
    } else {
        return Ok(None);
    }

    // Is the (commuted) operator an ordering operator with the right semantics?
    let sortfamily =
        lsyscache::get_op_opfamily_sortfamily::call(expr_op, opfamily).expect("sortfamily");
    if sortfamily != pk_opfamily {
        return Ok(None);
    }

    // We have a match. Return the clause or a commuted version thereof. The
    // operands are moved into a fresh OpExpr / the returned clause is re-owned,
    // so deep-copy via `clone_in` (a derived `.clone()` panics on an
    // owned-subtree operand).
    if commuted {
        let newclause = OpExpr {
            opno: expr_op,
            opfuncid: INVALID_OID,
            opresulttype: op.opresulttype,
            opretset: op.opretset,
            opcollid: op.opcollid,
            inputcollid: op.inputcollid,
            args: alloc::vec![rightop.clone_in(mcx)?, leftop.clone_in(mcx)?],
            location: op.location,
        };
        Ok(Some(Expr::OpExpr(newclause)))
    } else {
        Ok(Some(clause.clone_in(mcx)?))
    }
}

/// `contain_volatile_functions(node)` — local wrapper unwrapping the clauses.c
/// result (the C body never errors on a well-formed expr; OOM aside).
fn contain_volatile(node: &Expr) -> bool {
    clauses::contain_volatile_functions(Some(node))
        .expect("contain_volatile_functions")
}

/// `bms_equal(a, b)` over the planner `Relids`.
fn relids_equal(a: &pathnodes::Relids, b: &pathnodes::Relids) -> bool {
    let aw: &[u64] = match a {
        None => &[],
        Some(x) => &x.words,
    };
    let bw: &[u64] = match b {
        None => &[],
        Some(x) => &x.words,
    };
    let n = aw.len().max(bw.len());
    for i in 0..n {
        if aw.get(i).copied().unwrap_or(0) != bw.get(i).copied().unwrap_or(0) {
            return false;
        }
    }
    true
}
