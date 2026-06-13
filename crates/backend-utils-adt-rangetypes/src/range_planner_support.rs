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
//!
//! The range engine itself is real: the range `Const` is detoasted through the
//! crate's own `datum_get_range_type_p` seam into a `RangeTypeP`, its
//! `TypeCacheEntry` comes from the `range_get_typcache` seam, and
//! `range_deserialize` explodes it into real `RangeBound`s. The bound-by-bound
//! control flow (`empty` / both-infinite shortcuts, the volatile/subplan/cost
//! guards that protect against evaluating `elemExpr` twice, and the AND
//! assembly) is logic this crate owns and runs over those real values; only the
//! node fabrication and clause analysis cross into the planner neighbors via
//! the seams below.

use mcx::Mcx;
use types_core::primitive::Oid;
use types_datum::datum::Datum;
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

    /// `node != NULL` test (C: pointer non-null).
    fn is_null(self) -> bool {
        self.0 == 0
    }
}

// ---------------------------------------------------------------------------
// Outward seams: the planner-neighbor primitives these support fns call.
//
// Each is owned by the neighbor that fabricates/analyzes the node (nodes core,
// nodeFuncs/primnodes, makefuncs, optimizer clauses/cost, lsyscache, typcache).
// They are declared over the inherited `PlannerNode` opacity (and the real
// `Datum`/`Oid`/cost scalars) and panic loudly until that owner lands.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `IsA(rawreq, SupportRequestSimplify)` (nodes.h): does the support request
    /// node carry the `T_SupportRequestSimplify` tag.
    pub fn is_support_request_simplify(node: PlannerNode) -> bool
);

seam_core::seam!(
    /// `req->root` of a `SupportRequestSimplify` (supportnodes.h): the
    /// `PlannerInfo *` for the query being planned.
    pub fn support_request_simplify_root(req: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `req->fcall` of a `SupportRequestSimplify` (supportnodes.h): the
    /// `FuncExpr *` for the operator's underlying function call.
    pub fn support_request_simplify_fcall(req: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `linitial(fexpr->args)` / `lsecond(fexpr->args)` (pg_list.h), with the
    /// `Assert(list_length(fexpr->args) == 2)`: the two argument `Expr *` of the
    /// binary function call, returned `(leftop, rightop)`.
    pub fn func_expr_two_args(fexpr: PlannerNode) -> (PlannerNode, PlannerNode)
);

seam_core::seam!(
    /// `IsA(expr, Const)` (nodes.h): is the expression a `Const` node.
    pub fn is_const(expr: PlannerNode) -> bool
);

seam_core::seam!(
    /// `((Const *) expr)->constisnull` (primnodes.h): the `Const`'s null flag.
    pub fn const_is_null(expr: PlannerNode) -> bool
);

seam_core::seam!(
    /// `((Const *) expr)->constvalue` (primnodes.h): the `Const`'s payload
    /// `Datum` (only meaningful when `constisnull` is false).
    pub fn const_value(expr: PlannerNode) -> Datum
);

seam_core::seam!(
    /// `makeBoolConst(value, isnull)` (makefuncs.c): a boolean `Const`,
    /// allocated in `mcx` (C: the current memory context).
    pub fn make_bool_const<'mcx>(mcx: Mcx<'mcx>, value: bool, isnull: bool) -> PlannerNode
);

seam_core::seam!(
    /// `contain_volatile_functions(node)` (clauses.c): does the expression tree
    /// contain any volatile function.
    pub fn contain_volatile_functions(node: PlannerNode) -> bool
);

seam_core::seam!(
    /// `contain_subplans(node)` (clauses.c): does the expression tree contain a
    /// `SubPlan`/`AlternativeSubPlan` (searched explicitly because
    /// `cost_qual_eval()` cannot cost unplanned subselects).
    pub fn contain_subplans(node: PlannerNode) -> bool
);

seam_core::seam!(
    /// `cost_qual_eval_node(&cost, node, root)` (costsize.c): the
    /// `(startup, per_tuple)` evaluation cost of the expression.
    pub fn cost_qual_eval_node(root: PlannerNode, node: PlannerNode) -> (f64, f64)
);

seam_core::seam!(
    /// `cpu_operator_cost` (costsize.c GUC): per-operator CPU cost estimate.
    pub fn cpu_operator_cost() -> f64
);

seam_core::seam!(
    /// `copyObject(node)` (copyfuncs.c): a deep copy of the expression tree,
    /// allocated in `mcx`.
    pub fn copy_object<'mcx>(mcx: Mcx<'mcx>, node: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `make_andclause(list_make2(a, b))` (clauses.c): a two-clause `BoolExpr`
    /// `AND`, allocated in `mcx`.
    pub fn make_andclause<'mcx>(mcx: Mcx<'mcx>, a: PlannerNode, b: PlannerNode) -> PlannerNode
);

seam_core::seam!(
    /// `rngtypcache->rng_opfamily` (typcache.h, `TYPECACHE_RANGE_INFO`): the
    /// btree opfamily backing the range type `rngtypid`. (Not carried on the
    /// trimmed `TypeCacheEntry`; owned by typcache.) `Err` carries the lookup
    /// `ereport(ERROR)`s.
    pub fn range_opfamily(rngtypid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// The `build_bound_expr` node fabrication (rangetypes.c:2972): identify the
    /// btree comparison operator from `opfamily` for the element type `elem_type`
    /// (strategy chosen by `is_lower_bound`/`is_inclusive` via
    /// `get_opfamily_member`), wrap `val` in a `makeConst` of the element type
    /// (its `typlen`/`typbyval`/`typcollation` read off `elem_type`'s typcache
    /// entry, which the trimmed `TypeCacheEntry` here does not carry), and
    /// `make_opclause(oproid, BOOLOID, false, elemExpr, constExpr, InvalidOid,
    /// rng_collation)`. `NULL` if the operator is missing. Allocated in `mcx`.
    /// `Err` carries catalog-lookup `ereport(ERROR)`s.
    pub fn build_bound_op_expr<'mcx>(
        mcx: Mcx<'mcx>,
        elem_expr: PlannerNode,
        val: Datum,
        is_lower_bound: bool,
        is_inclusive: bool,
        elem_type: Oid,
        opfamily: Oid,
        rng_collation: Oid,
    ) -> PgResult<PlannerNode>
);

/// `elem_contained_by_range_support(arg)` body (rangetypes.c:2251): the support
/// fn for `elem <@ range`. Returns the simplified clause node (or `NULL`).
pub fn elem_contained_by_range_support<'mcx>(
    mcx: Mcx<'mcx>,
    request: PlannerNode,
) -> PgResult<PlannerNode> {
    // Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    let rawreq = request;
    // Node *ret = NULL;
    let mut ret = PlannerNode::NULL;

    if is_support_request_simplify::call(rawreq) {
        // SupportRequestSimplify *req = (SupportRequestSimplify *) rawreq;
        // FuncExpr *fexpr = req->fcall;
        let fexpr = support_request_simplify_fcall::call(rawreq);

        // Assert(list_length(fexpr->args) == 2);
        // leftop = linitial(fexpr->args); rightop = lsecond(fexpr->args);
        let (leftop, rightop) = func_expr_two_args::call(fexpr);

        // ret = find_simplified_clause(req->root, rightop, leftop);
        let root = support_request_simplify_root::call(rawreq);
        ret = find_simplified_clause(mcx, root, rightop, leftop)?;
    }

    // PG_RETURN_POINTER(ret);
    Ok(ret)
}

/// `range_contains_elem_support(arg)` body (rangetypes.c:2277): the support fn
/// for `range @> elem`.
pub fn range_contains_elem_support<'mcx>(
    mcx: Mcx<'mcx>,
    request: PlannerNode,
) -> PgResult<PlannerNode> {
    // Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    let rawreq = request;
    // Node *ret = NULL;
    let mut ret = PlannerNode::NULL;

    if is_support_request_simplify::call(rawreq) {
        // SupportRequestSimplify *req = (SupportRequestSimplify *) rawreq;
        // FuncExpr *fexpr = req->fcall;
        let fexpr = support_request_simplify_fcall::call(rawreq);

        // Assert(list_length(fexpr->args) == 2);
        // leftop = linitial(fexpr->args); rightop = lsecond(fexpr->args);
        let (leftop, rightop) = func_expr_two_args::call(fexpr);

        // ret = find_simplified_clause(req->root, leftop, rightop);
        let root = support_request_simplify_root::call(rawreq);
        ret = find_simplified_clause(mcx, root, leftop, rightop)?;
    }

    // PG_RETURN_POINTER(ret);
    Ok(ret)
}

/// `find_simplified_clause(root, rangeExpr, elemExpr)` (rangetypes.c:2850):
/// build `lower <= elem AND elem < upper` (per the range's inclusivity) when
/// the range argument is a constant; else `NULL`.
pub fn find_simplified_clause<'mcx>(
    mcx: Mcx<'mcx>,
    root: PlannerNode,
    range_expr: PlannerNode,
    mut elem_expr: PlannerNode,
) -> PgResult<PlannerNode> {
    // can't do anything unless the range is a non-null constant
    // if (!IsA(rangeExpr, Const) || ((Const *) rangeExpr)->constisnull) return NULL;
    if !is_const::call(range_expr) || const_is_null::call(range_expr) {
        return Ok(PlannerNode::NULL);
    }
    // range = DatumGetRangeTypeP(((Const *) rangeExpr)->constvalue);
    let constvalue = const_value::call(range_expr);
    let range =
        backend_utils_adt_rangetypes_seams::datum_get_range_type_p::call(mcx, constvalue)?;

    // RangeTypeGetOid(range): the serialized header's range type oid.
    let rngtypid = unsafe { (*range.ptr).rangetypid };

    // rangetypcache = lookup_type_cache(RangeTypeGetOid(range), TYPECACHE_RANGE_INFO);
    // if (rangetypcache->rngelemtype == NULL)
    //     elog(ERROR, "type %u is not a range type", RangeTypeGetOid(range));
    let rangetypcache =
        backend_utils_adt_rangetypes_seams::range_get_typcache::call(rngtypid)?;
    let elem_typcache = match rangetypcache.rngelemtype.as_ref() {
        Some(e) => e.as_ref(),
        None => {
            return Err(types_error::PgError::error(format!(
                "type {rngtypid} is not a range type"
            )));
        }
    };

    // range_deserialize(rangetypcache, range, &lower, &upper, &empty);
    let (lower, upper, empty) =
        backend_utils_adt_rangetypes_seams::range_deserialize::call(&rangetypcache, range)?;

    if empty {
        // if the range is empty, then there can be no matches
        return Ok(make_bool_const::call(mcx, false, false));
    } else if lower.infinite && upper.infinite {
        // the range has infinite bounds, so it matches everything
        return Ok(make_bool_const::call(mcx, true, false));
    } else {
        // at least one bound is available, we have something to work with
        // TypeCacheEntry *elemTypcache = rangetypcache->rngelemtype;
        // Oid opfamily = rangetypcache->rng_opfamily;
        // Oid rng_collation = rangetypcache->rng_collation;
        let opfamily = range_opfamily::call(rngtypid)?;
        let rng_collation = rangetypcache.rng_collation;
        let mut lower_expr = PlannerNode::NULL;
        let mut upper_expr = PlannerNode::NULL;

        if !lower.infinite && !upper.infinite {
            // When both bounds are present, we have a problem: the "simplified"
            // clause would need to evaluate the elemExpr twice. That's definitely
            // not okay if the elemExpr is volatile, and it's also unattractive if
            // the elemExpr is expensive.
            if contain_volatile_functions::call(elem_expr) {
                return Ok(PlannerNode::NULL);
            }

            // We define "expensive" as "contains any subplan or more than 10
            // operators". Note that the subplan search has to be done explicitly,
            // since cost_qual_eval() will barf on unplanned subselects.
            if contain_subplans::call(elem_expr) {
                return Ok(PlannerNode::NULL);
            }
            let (startup, per_tuple) = cost_qual_eval_node::call(root, elem_expr);
            if startup + per_tuple > 10.0 * cpu_operator_cost::call() {
                return Ok(PlannerNode::NULL);
            }
        }

        // Okay, try to build boundary comparison expressions
        if !lower.infinite {
            lower_expr = build_bound_expr(
                mcx,
                elem_expr,
                lower.val,
                true,
                lower.inclusive,
                elem_typcache.type_id,
                opfamily,
                rng_collation,
            )?;
            if lower_expr.is_null() {
                return Ok(PlannerNode::NULL);
            }
        }

        if !upper.infinite {
            // Copy the elemExpr if we need two copies
            if !lower.infinite {
                elem_expr = copy_object::call(mcx, elem_expr);
            }
            upper_expr = build_bound_expr(
                mcx,
                elem_expr,
                upper.val,
                false,
                upper.inclusive,
                elem_typcache.type_id,
                opfamily,
                rng_collation,
            )?;
            if upper_expr.is_null() {
                return Ok(PlannerNode::NULL);
            }
        }

        if !lower_expr.is_null() && !upper_expr.is_null() {
            Ok(make_andclause::call(mcx, lower_expr, upper_expr))
        } else if !lower_expr.is_null() {
            Ok(lower_expr)
        } else if !upper_expr.is_null() {
            Ok(upper_expr)
        } else {
            // Assert(false);
            debug_assert!(false, "find_simplified_clause produced no bound expression");
            Ok(PlannerNode::NULL)
        }
    }
}

/// `build_bound_expr(elemExpr, val, isLowerBound, isInclusive, typeCache,
/// opfamily, rng_collation)` (rangetypes.c:2972): construct one
/// `elem <op> boundval` `OpExpr`.
///
/// The element-type identity that C reads off the `typeCache` argument is the
/// `type_id` threaded here (the element type's `typlen`/`typbyval`/
/// `typcollation` are resolved on the owner side, since the trimmed
/// `TypeCacheEntry` does not carry them); the operator lookup + `makeConst` +
/// `make_opclause` fabrication is routed to the planner/makefuncs owner through
/// the `build_bound_op_expr` seam.
pub fn build_bound_expr<'mcx>(
    mcx: Mcx<'mcx>,
    elem_expr: PlannerNode,
    val: Datum,
    is_lower_bound: bool,
    is_inclusive: bool,
    elem_type: Oid,
    opfamily: Oid,
    rng_collation: Oid,
) -> PgResult<PlannerNode> {
    build_bound_op_expr::call(
        mcx,
        elem_expr,
        val,
        is_lower_bound,
        is_inclusive,
        elem_type,
        opfamily,
        rng_collation,
    )
}
