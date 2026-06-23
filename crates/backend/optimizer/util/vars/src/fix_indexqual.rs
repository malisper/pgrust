//! `fix_indexqual_operand` — convert an indexqual operand to a `Var` that
//! references the index column.
//!
//! This is `createplan.c`'s static `fix_indexqual_operand`; it is homed here in
//! the var.c crate (alongside the other `Var`-construction helpers) so that
//! `create_indexscan_plan` can reach it without re-implementing the index-column
//! cross-check. Ported 1:1 over the planner arena/`NodeId` model of
//! [`PlannerInfo`].
//!
//! Index keys are represented by `Var` nodes with `varno == INDEX_VAR` and
//! `varattno` equal to the index column position (1-based). Most of the routine
//! is sanity cross-checking that the given expression actually matches the index
//! column it is claimed to (it must mirror `match_index_to_operand()`).

#![allow(non_snake_case)]

use ::nodes_core::makefuncs::make_var;
use ::nodes_core::nodefuncs::{expr_collation, expr_type};
use ::equalfuncs_seams::equal_expr;
use ::indxpath::operand::strip_phvs_in_index_operand;
use ::types_error::{PgError, PgResult};
use ::nodes::primnodes::Expr;
use ::pathnodes::{IndexOptInfo, PlannerInfo};

/// `INDEX_VAR` (primnodes.h) — the special `varno` identifying a Var that
/// references an index column rather than a heap column.
const INDEX_VAR: i32 = -3;

/// `fix_indexqual_operand(node, index, indexcol)` (createplan.c).
///
/// Convert an indexqual expression to a `Var` referencing the index column.
///
/// `root` is needed to resolve the arena handles the trimmed [`IndexOptInfo`]
/// carries: `index.indexprs` is a list of [`::pathnodes::NodeId`] expression
/// handles, and `index.rel` is a [`::pathnodes::RelId`] whose `relid` (the
/// underlying RT index) the simple-column case cross-checks against.
pub fn fix_indexqual_operand<'mcx>(
    root: &PlannerInfo,
    node: Expr<'mcx>,
    index: &IndexOptInfo,
    indexcol: i32,
) -> PgResult<Expr<'mcx>> {
    debug_assert!(indexcol >= 0 && indexcol < index.ncolumns);

    // Remove any PlaceHolderVar wrapping of the indexkey.
    let mut node = strip_phvs_in_index_operand(node);

    // Remove any binary-compatible relabeling of the indexkey.
    while let Expr::RelabelType(rt) = node {
        node = *rt
            .arg
            .expect("fix_indexqual_operand: RelabelType.arg must be set");
    }

    if index.indexkeys[indexcol as usize] != 0 {
        // It's a simple index column.
        let index_relid = {
            let relid = index
                .rel
                .expect("fix_indexqual_operand: IndexOptInfo.rel must be set");
            root.rel_arena[relid.index()].relid as i32
        };
        if let Some(var) = node.as_var() {
            if var.varno == index_relid && var.varattno as i32 == index.indexkeys[indexcol as usize]
            {
                let mut result = var.clone();
                result.varno = INDEX_VAR;
                result.varattno = (indexcol + 1) as i16;
                return Ok(Expr::Var(result));
            }
        }
        return Err(PgError::error(
            "index key does not match expected index column",
        ));
    }

    // It's an index expression, so find and cross-check the expression.
    let mut indexpr_iter = index.indexprs.iter();
    let mut indexpr_item = indexpr_iter.next();
    for pos in 0..index.ncolumns {
        if index.indexkeys[pos as usize] == 0 {
            let item = match indexpr_item {
                None => return Err(PgError::error("too few entries in indexprs list")),
                Some(&id) => id,
            };
            if pos == indexcol {
                // The stored index expression; strip a binary-compatible
                // RelabelType wrapper before comparing, exactly as C does.
                // Borrow the stored index expression and its (relabel-stripped)
                // indexkey for the read-only comparison (a derived `Expr::clone`
                // panics on a context-allocated child); no owned copy is needed.
                let stored: &Expr = root.node(item);
                let indexkey: &Expr = match stored {
                    Expr::RelabelType(rt) => rt
                        .arg
                        .as_deref()
                        .expect("fix_indexqual_operand: RelabelType.arg must be set"),
                    other => other,
                };
                if equal_expr::call(&node, indexkey) {
                    // makeVar(INDEX_VAR, indexcol + 1,
                    //         exprType(lfirst(indexpr_item)), -1,
                    //         exprCollation(lfirst(indexpr_item)), 0)
                    let vartype = expr_type(Some(stored))?;
                    let varcollid = expr_collation(Some(stored))?;
                    let result = make_var(INDEX_VAR, (indexcol + 1) as i16, vartype, -1, varcollid, 0);
                    return Ok(Expr::Var(result));
                } else {
                    return Err(PgError::error(
                        "index key does not match expected index column",
                    ));
                }
            }
            indexpr_item = indexpr_iter.next();
        }
    }

    // Oops...
    Err(PgError::error(
        "index key does not match expected index column",
    ))
}
