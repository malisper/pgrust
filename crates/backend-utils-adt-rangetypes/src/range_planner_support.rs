//! Family `range-planner-support`: the range operators' planner support
//! functions.
//!
//! Mirrors `rangetypes.c`: `elem_contained_by_range_support` /
//! `range_contains_elem_support` (the `range_support` entry points) and their
//! shared helpers `find_simplified_clause` / `build_bound_expr`. These rewrite
//! a `<@` / `@>` clause into a pair of bound comparisons. The `SupportRequest*`
//! and produced `Expr` nodes are planner `Node *` (inherited opacity from the
//! not-yet-ported optimizer/makefuncs/lsyscache neighbors); the support fns
//! reach those neighbors through their owners' seams.

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;

/// A planner `Node *` (`nodes.h`). Inherited opacity: the optimizer is a
/// genuinely-external neighbor whose node trees this crate only forwards to the
/// optimizer/makefuncs seams. `0` models C's `NULL`. Resolves to the real node
/// type when the optimizer's node vocabulary lands.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct PlannerNode(pub u64);

impl PlannerNode {
    /// C's `NULL` node.
    pub const NULL: PlannerNode = PlannerNode(0);
}

/// `elem_contained_by_range_support(arg)` body (rangetypes.c:2251): the support
/// fn for `elem <@ range`. Returns the simplified clause node (or `NULL`).
pub fn elem_contained_by_range_support<'mcx>(
    _mcx: Mcx<'mcx>,
    _request: PlannerNode,
) -> PgResult<PlannerNode> {
    todo!("elem_contained_by_range_support")
}

/// `range_contains_elem_support(arg)` body (rangetypes.c:2277): the support fn
/// for `range @> elem`.
pub fn range_contains_elem_support<'mcx>(
    _mcx: Mcx<'mcx>,
    _request: PlannerNode,
) -> PgResult<PlannerNode> {
    todo!("range_contains_elem_support")
}

/// `find_simplified_clause(root, rangeExpr, elemExpr)` (rangetypes.c:2850):
/// build `lower <= elem AND elem < upper` (per the range's inclusivity) when
/// the range argument is a constant; else `NULL`.
pub fn find_simplified_clause<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: PlannerNode,
    _range_expr: PlannerNode,
    _elem_expr: PlannerNode,
) -> PgResult<PlannerNode> {
    todo!("find_simplified_clause")
}

/// `build_bound_expr(elemExpr, val, isLowerBound, isInclusive, typeCache,
/// opfamily, rng_collation)` (rangetypes.c:2972): construct one
/// `elem <op> boundval` `OpExpr`.
pub fn build_bound_expr<'mcx>(
    _mcx: Mcx<'mcx>,
    _elem_expr: PlannerNode,
    _val: PlannerNode,
    _is_lower_bound: bool,
    _is_inclusive: bool,
    _opfamily: Oid,
    _rng_collation: Oid,
) -> PgResult<PlannerNode> {
    todo!("build_bound_expr")
}
