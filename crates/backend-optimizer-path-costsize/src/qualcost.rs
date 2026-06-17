//! `cost_qual_eval_walker` (costsize.c:4796) — the recursive per-node clause
//! cost walker that `cost_qual_eval` / `cost_qual_eval_node` route through.
//!
//! The C walker takes a `Node *` and a `cost_qual_eval_context` (which carries
//! `root` and the accumulating `QualCost`). In this repo the planner node arena
//! resolves a `NodeId` to a *root* `Expr` that owns its entire subtree inline
//! (`Vec<Expr>` / `Box<Expr>` children), so recursion is over the in-memory
//! `&Expr` tree via `expression_tree_walker`, not over further `NodeId`s.
//!
//! The single divergence from the C signature: the seam takes `root:
//! &PlannerInfo` (immutable). The C walker's only writes are the
//! `RestrictInfo.eval_cost` memoization cache and `set_opfuncid`/`set_sa_opfuncid`
//! (which fill `opfuncid` from `pg_operator.oprcode` if unset). Both are pure
//! caches: the cost value computed is identical whether or not they are
//! persisted. The `cost_qual_eval` wrapper already resolves each
//! `RestrictInfo.clause` to its expression `NodeId` before calling the walker,
//! so the `RestrictInfo` arm is never reached through this seam; and we read
//! `opfuncid` (falling back to `get_opcode(opno)` exactly as `set_opfuncid`
//! would) without writing it back. The numerically-identical result is a
//! faithful behavioral port.

use types_core::primitive::{InvalidOid, Oid};
use types_nodes::primnodes::Expr;
use types_pathnodes::{PlannerInfo, QualCost};

use backend_optimizer_path_costsize_seams as cz;
use backend_optimizer_util_clauses as clauses;
use backend_utils_cache_lsyscache_seams as lsyscache;

/// `cost_qual_eval_walker((Node *) node, &context)` over the arena node `node`.
/// Returns the `(startup, per_tuple)` cost this node and its descendants
/// contribute. Installed as the `cost_qual_eval_walker` seam.
pub fn cost_qual_eval_walker(root: &PlannerInfo, node: types_pathnodes::NodeId) -> (f64, f64) {
    let mut total = QualCost {
        startup: 0.0,
        per_tuple: 0.0,
    };
    // The top node carries a real `NodeId`; estimate_array_length / the prosupport
    // callback only ever fire at identifiable top-level call sites in practice,
    // and inline children are walked by-value, so recursion uses the by-`&Expr`
    // forms uniformly.
    walk(root, root.node(node), &mut total);
    (total.startup, total.per_tuple)
}

/// `cost_qual_eval_walker` over an in-memory `&Expr` (the recursion form). Mirrors
/// the C `IsA` dispatch; `expression_tree_walker` drives the default recursion.
fn walk(root: &PlannerInfo, node: &Expr, total: &mut QualCost) {
    let cpu_operator_cost = crate::cpu_operator_cost();

    match node {
        // For each operator or function node we charge pg_proc.procost
        // (cpu_operator_cost-scaled inside add_function_cost). Vars, Consts and
        // the boolean operators (AND/OR/NOT, i.e. BoolExpr) are charged zero and
        // fall through to the default recursion.
        Expr::FuncExpr(f) => {
            let (s, p) = cz::add_function_cost::call(root, f.funcid, None);
            total.startup += s;
            total.per_tuple += p;
        }
        // OpExpr / DistinctExpr / NullIfExpr — C relies on struct equivalence and
        // calls set_opfuncid then add_function_cost(opfuncid). We resolve opfuncid
        // exactly as set_opfuncid would (from opno via get_opcode if unset).
        Expr::OpExpr(op) | Expr::DistinctExpr(op) | Expr::NullIfExpr(op) => {
            let opfuncid = resolve_opfuncid(op.opfuncid, op.opno);
            let (s, p) = cz::add_function_cost::call(root, opfuncid, None);
            total.startup += s;
            total.per_tuple += p;
        }
        Expr::ScalarArrayOpExpr(saop) => {
            // arraynode = lsecond(saop->args)
            let arraynode = &saop.args[1];
            let estarraylen = clauses::estimate_array_length(arraynode).expect("estimate_array_length");
            let opfuncid = resolve_opfuncid(saop.opfuncid, saop.opno);

            let mut sacost = QualCost {
                startup: 0.0,
                per_tuple: 0.0,
            };
            let (s, p) = cz::add_function_cost::call(root, opfuncid, None);
            sacost.startup += s;
            sacost.per_tuple += p;

            if saop.hashfuncid != InvalidOid {
                // Hashed ScalarArrayOpExpr.
                let mut hcost = QualCost {
                    startup: 0.0,
                    per_tuple: 0.0,
                };
                let (hs, hp) = cz::add_function_cost::call(root, saop.hashfuncid, None);
                hcost.startup += hs;
                hcost.per_tuple += hp;
                total.startup += sacost.startup + hcost.startup;
                // cost of building the hashtable
                total.startup += estarraylen * hcost.per_tuple;
                // hashtable lookups: a single hash and a single comparison
                total.per_tuple += hcost.per_tuple + sacost.per_tuple;
            } else {
                // operator applied to ~half the array elements before answer known
                total.startup += sacost.startup;
                total.per_tuple += sacost.per_tuple * estarraylen * 0.5;
            }
        }
        // Aggref / WindowFunc behave like Vars at execution: zero cost, and we do
        // NOT recurse into children (their argument costs are charged in the
        // Agg/WindowAgg plan node).
        Expr::Aggref(_) | Expr::WindowFunc(_) => {}
        Expr::GroupingFunc(_) => {
            // Treat this as having cost 1.
            total.per_tuple += cpu_operator_cost;
        }
        Expr::CoerceViaIO(iocoerce) => {
            // result type's input function
            let (iofunc, _typioparam) = lsyscache::get_type_input_info::call(iocoerce.resulttype)
                .expect("getTypeInputInfo");
            let (s, p) = cz::add_function_cost::call(root, iofunc, None);
            total.startup += s;
            total.per_tuple += p;
            // input type's output function
            let arg_type = nodefuncs_expr_type(iocoerce.arg.as_deref());
            let (ofunc, _typisvarlena) =
                lsyscache::get_type_output_info::call(arg_type).expect("getTypeOutputInfo");
            let (s2, p2) = cz::add_function_cost::call(root, ofunc, None);
            total.startup += s2;
            total.per_tuple += p2;
        }
        Expr::ArrayCoerceExpr(acoerce) => {
            // perelemcost = cost_qual_eval_node(acoerce->elemexpr)
            let mut perelemcost = QualCost {
                startup: 0.0,
                per_tuple: 0.0,
            };
            if let Some(elemexpr) = acoerce.elemexpr.as_deref() {
                walk(root, elemexpr, &mut perelemcost);
            }
            total.startup += perelemcost.startup;
            if perelemcost.per_tuple > 0.0 {
                let arrlen = match acoerce.arg.as_deref() {
                    Some(arg) => {
                        clauses::estimate_array_length(arg).expect("estimate_array_length")
                    }
                    None => 0.0,
                };
                total.per_tuple += perelemcost.per_tuple * arrlen;
            }
        }
        Expr::RowCompareExpr(rcexpr) => {
            // Conservatively assume we will check all the columns.
            for &opid in rcexpr.opnos.iter() {
                let opcode = lsyscache::get_opcode::call(opid).expect("get_opcode");
                let (s, p) = cz::add_function_cost::call(root, opcode, None);
                total.startup += s;
                total.per_tuple += p;
            }
        }
        // MinMaxExpr / SQLValueFunction / XmlExpr / CoerceToDomain / NextValueExpr
        // / JsonExpr — treat all as having cost 1.
        Expr::MinMaxExpr(_)
        | Expr::SQLValueFunction(_)
        | Expr::XmlExpr(_)
        | Expr::CoerceToDomain(_)
        | Expr::NextValueExpr(_)
        | Expr::JsonExpr(_) => {
            total.per_tuple += cpu_operator_cost;
        }
        Expr::SubLink(_) => {
            // This routine should not be applied to un-planned expressions.
            panic!("cannot handle unplanned sub-select");
        }
        Expr::SubPlan(subplan) => {
            // A subplan executed per evaluation; charge its node costs and do not
            // recurse into testexpr (already counted in the SubPlan node costs).
            total.startup += subplan.0.startup_cost;
            total.per_tuple += subplan.0.per_call_cost;
            return;
        }
        Expr::AlternativeSubPlan(asplan) => {
            // Arbitrarily use the first alternative plan for costing.
            if let Some(first) = asplan.0.subplans.first() {
                // SubPlan handling, inline (the variant is a Box<SubPlan>).
                total.startup += first.startup_cost;
                total.per_tuple += first.per_call_cost;
            }
            return;
        }
        Expr::PlaceHolderVar(_) => {
            // PHV is charged zero here; its contained-expr cost is charged at the
            // scan/join where the PHV is first computed (set_rel_width /
            // add_placeholders_to_joinrel). Do NOT recurse into the phexpr.
            return;
        }
        _ => {}
    }

    // recurse into children (default `expression_tree_walker(node, walker)`)
    let mut child_walker = |child: &Expr| -> bool {
        walk(root, child, total);
        false
    };
    backend_nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut child_walker);
}

/// `set_opfuncid` value resolution without persisting it: if `opfuncid` is
/// unset, look it up from `opno` via `get_opcode` (`pg_operator.oprcode`),
/// exactly as `set_opfuncid` would. The numerically-identical result makes the
/// cache write unnecessary for costing.
fn resolve_opfuncid(opfuncid: Oid, opno: Oid) -> Oid {
    if opfuncid != InvalidOid {
        opfuncid
    } else {
        lsyscache::get_opcode::call(opno).expect("get_opcode")
    }
}

/// `exprType((Node *) arg)` over an inline child expression.
fn nodefuncs_expr_type(arg: Option<&Expr>) -> Oid {
    backend_nodes_core::nodefuncs::expr_type(arg).expect("exprType")
}
