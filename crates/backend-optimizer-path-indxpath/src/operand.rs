//! Operand-matching + PlaceHolderVar stripping + the pseudo-constant test
//! (indxpath.c). Ported 1:1 over the unified node tree.

use mcx::Mcx;
use types_core::primitive::Index;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::{IndexOptInfo, PlannerInfo};

use backend_nodes_core::bitmapset::bms_is_member as nodes_bms_is_member;
use backend_nodes_core::nodefuncs::{expression_tree_mutator, expression_tree_walker};
use backend_nodes_equalfuncs_seams::equal_expr;
use backend_optimizer_util_clauses::contain_volatile_functions;
use backend_optimizer_util_var_seams::pull_varnos;

/// `match_index_to_operand(operand, indexcol, index)` (indxpath.c:4413) —
/// determine whether `operand` is an indexable column reference for column
/// `indexcol` of `index`.
///
/// `root` is threaded (vs. the C signature) to dereference `index.rel` (a
/// `RelId` handle) for `index->rel->relid`, and `index.indexprs` (`NodeId`s) for
/// the expression-column branch.
pub fn match_index_to_operand(
    root: &PlannerInfo,
    operand: &Expr,
    indexcol: usize,
    index: &IndexOptInfo,
) -> bool {
    // Ignore any PlaceHolderVar node contained in the operand. Only build an
    // owned, stripped copy when a strippable PHV is actually present; otherwise
    // borrow the operand as-is. (A blanket `operand.clone()` here would invoke
    // the derived `Expr::clone`, which panics on an owned-subtree child such as
    // an `Aggref`/`SubLink`/`SubPlan` index expression.)
    let operand_owned: Option<Expr> = if contain_strippable_phv_walker(operand) {
        Some(strip_phvs_in_index_operand_mutator(operand.clone()))
    } else {
        None
    };

    // Ignore any (nested) RelabelType nodes above the operand.
    let mut operand: &Expr = operand_owned.as_ref().unwrap_or(operand);
    while operand.is_relabeltype() {
        match operand.as_relabeltype().unwrap().arg.as_deref() {
            Some(arg) => operand = arg,
            None => break, // RelabelType with NULL arg: nothing more to peel
        }
    }

    // index->rel->relid (dereference the RelId handle through the arena).
    let index_relid = root
        .rel(index.rel.expect("IndexOptInfo without rel"))
        .relid;
    let indkey = index.indexkeys[indexcol];
    if indkey != 0 {
        // Simple index column; operand must be a matching Var.
        if let Some(var) = operand.as_var() {
            if index_relid == var.varno as Index
                && indkey == var.varattno as i32
                && var.varnullingrels.words.is_empty()
            {
                return true;
            }
        }
        false
    } else {
        // Index expression; find the correct expression by counting expression
        // columns (indexkeys[i] == 0) up to `indexcol`.
        let mut indexpr_idx = 0usize;
        for i in 0..indexcol {
            if index.indexkeys[i] == 0 {
                if indexpr_idx >= index.indexprs.len() {
                    panic!("wrong number of index expressions");
                }
                indexpr_idx += 1;
            }
        }
        if indexpr_idx >= index.indexprs.len() {
            panic!("wrong number of index expressions");
        }
        let indexkey_id = index.indexprs[indexpr_idx];
        let indexkey_node = root.node(indexkey_id);

        // Does it match the operand?  Again, strip any relabeling.
        let indexkey: &Expr = if indexkey_node.is_relabeltype() {
            match indexkey_node.as_relabeltype().unwrap().arg.as_deref() {
                Some(arg) => arg,
                None => indexkey_node,
            }
        } else {
            indexkey_node
        };

        equal_expr::call(indexkey, operand)
    }
}

/// `strip_phvs_in_index_operand(operand)` (indxpath.c:4508) — strip non-nullable
/// `PlaceHolderVar` nodes from an index operand to facilitate matching against
/// an index key.
pub fn strip_phvs_in_index_operand(operand: Expr) -> Expr {
    // Don't mutate/copy if no target PHVs exist.
    if !contain_strippable_phv_walker(&operand) {
        return operand;
    }
    strip_phvs_in_index_operand_mutator(operand)
}

/// `contain_strippable_phv_walker(node, context)` (indxpath.c:4526) — detect
/// whether the tree contains a `PlaceHolderVar` that is a candidate for
/// stripping (its `phnullingrels` is empty).
pub fn contain_strippable_phv_walker(node: &Expr) -> bool {
    if let Some(phv) = node.as_placeholdervar() {
        // phnullingrels is the lifetime-free ExprRelids (`words: Vec<u64>`); the
        // C `bms_is_empty(phv->phnullingrels)` is "no bits set".
        if phv.phnullingrels.words.iter().all(|w| *w == 0) {
            return true;
        }
    }
    // C: return expression_tree_walker(node, contain_strippable_phv_walker, ctx).
    let mut walker = |n: &Expr| contain_strippable_phv_walker(n);
    expression_tree_walker(Some(node), &mut walker)
}

/// `strip_phvs_in_index_operand_mutator(node, context)` (indxpath.c:4551) —
/// recursively remove non-nullable `PlaceHolderVar` nodes, replacing each with
/// its contained expression.
pub fn strip_phvs_in_index_operand_mutator(node: Expr) -> Expr {
    if let Some(phv) = node.as_placeholdervar() {
        // If it matches the criteria, strip it.
        if phv.phnullingrels.words.iter().all(|w| *w == 0) {
            // Recurse on its contained expression. C: phv->phexpr is non-NULL.
            let phexpr = node
                .expect_into_placeholdervar()
                .phexpr
                .expect("PlaceHolderVar without phexpr");
            return strip_phvs_in_index_operand_mutator(*phexpr);
        }
        // Otherwise keep this PHV but check its contained expression (fall
        // through to the generic mutator, which descends into phexpr).
    }
    let mut mutator = |n: Expr| strip_phvs_in_index_operand_mutator(n);
    expression_tree_mutator(node, &mut mutator)
}

/// `is_pseudo_constant_for_index(root, expr, index)` (indxpath.c:4596) — test
/// whether `expr` can be used as an indexscan comparison value: it must contain
/// no `Var` of the index's own table and no volatile function.
///
/// `mcx` is threaded for the `pull_varnos` allocation (var.c's collector returns
/// a freshly-allocated `Bitmapset`).
pub fn is_pseudo_constant_for_index(
    mcx: Mcx<'_>,
    root: &PlannerInfo,
    expr: &Expr,
    index: &IndexOptInfo,
) -> PgResult<bool> {
    // pull_varnos is cheaper than the volatility check, so do that first.
    let index_relid = root
        .rel(index.rel.expect("IndexOptInfo without rel"))
        .relid;
    let varnos = pull_varnos::call(mcx, expr)?;
    if nodes_bms_is_member(index_relid as i32, varnos.as_deref()) {
        return Ok(false); // no good, contains Var of table
    }
    if contain_volatile_functions(Some(expr))? {
        return Ok(false); // no good, volatile comparison value
    }
    Ok(true)
}
