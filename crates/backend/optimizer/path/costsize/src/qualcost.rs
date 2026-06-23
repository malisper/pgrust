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
//! persisted. The `RestrictInfo` arm is reached when `order_qual_clauses`
//! costs a list of bare `RestrictInfo` nodes (createplan.c): we read the
//! cached `eval_cost` when set and otherwise recompute it over `orclause`/
//! `clause` (applying the `pseudoconstant` startup fold), without writing the
//! cache back. Likewise we read `opfuncid` (falling back to `get_opcode(opno)`
//! exactly as `set_opfuncid` would) without writing it back. The
//! numerically-identical result is a faithful behavioral port.

use ::types_core::primitive::{InvalidOid, Oid};
use ::nodes::primnodes::Expr;
use ::pathnodes::{PlannerInfo, QualCost};

use costsize_seams as cz;
use clauses as clauses;
use lsyscache_seams as lsyscache;

/// `cost_qual_eval_walker((Node *) node, &context)` over the arena node `node`.
/// Returns the `(startup, per_tuple)` cost this node and its descendants
/// contribute. Installed as the `cost_qual_eval_walker` seam.
pub fn cost_qual_eval_walker(root: &PlannerInfo, node: ::pathnodes::NodeId) -> (f64, f64) {
    let mut total = QualCost {
        startup: 0.0,
        per_tuple: 0.0,
    };
    // Scratch context for the `estimate_array_length` detoast
    // (`DatumGetArrayTypeP` over an IN-list array Const). The C walker allocates
    // any detoasted image in `CurrentMemoryContext` and discards it; the owned
    // model roots that scratch here and drops it at return.
    let cx = mcx::MemoryContext::new("cost_qual_eval_walker scratch");
    // The top node carries a real `NodeId`; estimate_array_length / the prosupport
    // callback only ever fire at identifiable top-level call sites in practice,
    // and inline children are walked by-value, so recursion uses the by-`&Expr`
    // forms uniformly.
    walk(cx.mcx(), Some(root), root.node(node), &mut total);
    (total.startup, total.per_tuple)
}

/// `cost_qual_eval_node(&cost, (Node *) expr, root)` (costsize.c:4782) over an
/// in-memory `&Expr` that is NOT in the planner's node arena — e.g. a
/// free-standing `elemExpr` a planner-support function (rangetypes.c's
/// `find_simplified_clause`) wants to cost before deciding to duplicate it.
/// Mirrors [`cost_qual_eval_walker`] but takes the root `Expr` by reference
/// instead of resolving it through a `NodeId`.
///
/// `root` is `Option`: C's `find_simplified_clause` runs inside
/// `eval_const_expressions` where `root` may legitimately be NULL (the
/// `estimate_expression_value` / const-fold entry paths), and the only root use
/// in costing a free-standing elemExpr is the rare per-function
/// `SupportRequestCost` (skipped when root is NULL, exactly as
/// `add_function_cost` does — "in some usages root might be NULL, too",
/// plancat.c). A free-standing elemExpr is never a `RestrictInfo` (those are
/// built later by the planner) and any `SubLink`/`SubPlan` is excluded by the
/// caller's `contain_subplans` guard, so the root-dereferencing arms are
/// unreachable here.
pub fn cost_qual_eval_expr(root: Option<&PlannerInfo>, node: &Expr) -> (f64, f64) {
    let mut total = QualCost {
        startup: 0.0,
        per_tuple: 0.0,
    };
    let cx = mcx::MemoryContext::new("cost_qual_eval_expr scratch");
    walk(cx.mcx(), root, node, &mut total);
    (total.startup, total.per_tuple)
}

/// `cost_qual_eval_expr` over a non-optional root (the `cost_qual_eval_node_expr`
/// seam: restrictinfo.c / joininfo.c always supply a real `PlannerInfo`).
pub fn cost_qual_eval_expr_with_root(root: &PlannerInfo, node: &Expr) -> (f64, f64) {
    cost_qual_eval_expr(Some(root), node)
}

/// `cost_qual_eval_walker` over an in-memory `&Expr` (the recursion form). Mirrors
/// the C `IsA` dispatch; `expression_tree_walker` drives the default recursion.
fn walk<'mcx>(mcx: mcx::Mcx<'mcx>, root: Option<&PlannerInfo>, node: &Expr, total: &mut QualCost) {
    let cpu_operator_cost = crate::cpu_operator_cost();

    match node {
        // RestrictInfo nodes contain an eval_cost field reserved for this
        // routine's use, so that it's not necessary to evaluate the qual
        // clause's cost more than once. If the clause's cost hasn't been
        // computed yet, the field's startup value will contain -1. We read the
        // cached value when present and otherwise recompute it; because `root`
        // is borrowed immutably here we cannot persist the cache, but the
        // recomputed value is numerically identical, so the result is faithful.
        Expr::RestrictInfo(rref) => {
            // A RestrictInfo is only ever costed through the arena entry
            // (`cost_qual_eval_walker`), which always supplies a real root; the
            // free-standing `cost_qual_eval_expr` (root == None) is never handed a
            // RestrictInfo (those are built later by the planner).
            let root = root.expect("cost_qual_eval: RestrictInfo requires a PlannerInfo root");
            let rinfo = root.rinfo(::pathnodes::RinfoId::from(*rref));
            let eval = if rinfo.eval_cost.startup < 0.0 {
                // For an OR clause, recurse into the marked-up tree so that we
                // would set the eval_cost for contained RestrictInfos too.
                let mut loc = QualCost {
                    startup: 0.0,
                    per_tuple: 0.0,
                };
                let inner = rinfo.orclause.unwrap_or(rinfo.clause);
                walk(mcx, Some(root), root.node(inner), &mut loc);
                // If the RestrictInfo is marked pseudoconstant, it will be
                // tested only once, so treat its cost as all startup cost.
                if rinfo.pseudoconstant {
                    loc.startup += loc.per_tuple;
                    loc.per_tuple = 0.0;
                }
                loc
            } else {
                rinfo.eval_cost
            };
            total.startup += eval.startup;
            total.per_tuple += eval.per_tuple;
            // do NOT recurse into children
            return;
        }
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
            let estarraylen =
                clauses::estimate_array_length(mcx, arraynode).expect("estimate_array_length");
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
                walk(mcx, root, elemexpr, &mut perelemcost);
            }
            total.startup += perelemcost.startup;
            if perelemcost.per_tuple > 0.0 {
                let arrlen = match acoerce.arg.as_deref() {
                    Some(arg) => {
                        clauses::estimate_array_length(mcx, arg).expect("estimate_array_length")
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
        walk(mcx, root, child, total);
        false
    };
    nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut child_walker);
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
    nodes_core::nodefuncs::expr_type(arg).expect("exprType")
}
