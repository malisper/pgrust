//! Family `range-planner-support`: the range operators' planner support
//! functions.
//!
//! Mirrors `rangetypes.c`: `elem_contained_by_range_support` /
//! `range_contains_elem_support` (the `range_support` entry points) and their
//! shared helpers `find_simplified_clause` / `build_bound_expr`. These rewrite
//! a `<@` / `@>` clause into a pair of bound comparisons.
//!
//! ## Real-node re-sign (`#159` range-planner-support lane)
//!
//! The optimizer's node vocabulary now exists in production: `Expr`, `Const`,
//! `FuncExpr`, `OpExpr`, `PlannerInfo`, `TypeCacheEntry` are all real
//! value-typed carriers. The clause analysis runs over those real values
//! directly:
//!
//! * `IsA(expr, Const)` / `((Const*)expr)->constvalue|constisnull` →
//!   pattern-match on [`Expr::Const`].
//! * `linitial/lsecond(fexpr->args)` → index the real [`FuncExpr::args`] `Vec`.
//!
//! The node-FABRICATION and analysis primitives that genuinely belong to the
//! optimizer/makefuncs/lsyscache/costsize neighbors (`makeConst`,
//! `make_opclause`, `makeBoolConst`, `make_andclause`,
//! `contain_volatile_functions`, `contain_subplans`, `cost_qual_eval_node`,
//! `get_opfamily_member`) still cross thin outward seams below — but those seams
//! are now declared over the REAL [`Expr`]/[`Const`]/[`PlannerInfo`] types and
//! are wired in `seams-init` to their already-real owners (makefuncs.rs,
//! clauses.rs, costsize.rs, lsyscache.rs), rather than the prior bare
//! `PlannerNode(u64)` handle shim.
//!
//! The range engine itself is real: the range `Const`'s payload `Datum` is
//! detoasted through the crate's own `datum_get_range_type_p` seam into a
//! `RangeTypeP`, its `TypeCacheEntry` comes from the `range_get_typcache` seam,
//! and `range_deserialize` explodes it into real `RangeBound`s.
//!
//! ## `root` threading (faithful conservative decline)
//!
//! C's support function reads `req->root` (the `PlannerInfo *`) and feeds it to
//! `cost_qual_eval_node` for the "both bounds present" guard (which protects
//! against evaluating a volatile/expensive `elemExpr` twice). The
//! `eval_const_expressions` entry path passes `req.root == NULL` in C, and the
//! value-typed `call_support_simplify` dispatch likewise carries no
//! `PlannerInfo`. We therefore thread `root: Option<&PlannerInfo>`: when both
//! bounds are present but no root is available to cost the `elemExpr`, we take
//! the same exit C takes for an "expensive" `elemExpr` — decline the
//! simplification (`Ok(None)`). Declining is always a semantically-valid planner
//! answer (the original clause is left in place); the result is correct, only a
//! missed optimization, exactly mirroring C's expensive-`elemExpr` behavior.

use mcx::Mcx;
use types_core::catalog::BOOLOID;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::PgResult;
use types_nodes::primnodes::{Const, Expr, FuncExpr};
use types_pathnodes::PlannerInfo;
// The canonical `Const.constvalue` carrier is the `types_tuple` `Datum<'mcx>`
// enum (`ByVal(word)` / `ByRef(bytes)`), which `make_const` consumes. The range
// engine's `RangeBound.val` is the bare-word `types_datum::Datum` (a serialized
// pointer/word into the range image); we lift it into a `ByVal` word — exactly
// the `Datum` C's `makeConst(... val ...)` stores (for a by-reference element
// type the word is a pointer the makefuncs owner detoasts, as in C).
use types_tuple::backend_access_common_heaptuple::Datum as NodeDatum;
use types_scan::scankey::{
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, BTLessEqualStrategyNumber,
    BTLessStrategyNumber,
};

// ---------------------------------------------------------------------------
// Outward seams: the planner-neighbor primitives these support fns call.
//
// Each is owned by the neighbor that fabricates/analyzes the node (makefuncs,
// optimizer clauses/cost, lsyscache). They are declared over the REAL
// `Expr`/`Const`/`PlannerInfo` types and wired in `seams-init` to their
// already-real owners (panic loudly until that wiring runs).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `makeBoolConst(value, isnull)` (makefuncs.c): a boolean `Const`.
    /// Owner: `backend-nodes-core/src/makefuncs.rs::make_bool_const`.
    pub fn make_bool_const(value: bool, isnull: bool) -> Const
);

seam_core::seam!(
    /// `contain_volatile_functions(node)` (clauses.c): does the expression tree
    /// contain any volatile function. Owner:
    /// `backend-optimizer-util-clauses/src/grounded.rs::contain_volatile_functions`.
    pub fn contain_volatile_functions(node: &Expr) -> PgResult<bool>
);

seam_core::seam!(
    /// `contain_subplans(node)` (clauses.c): does the expression tree contain a
    /// `SubPlan`/`AlternativeSubPlan` (searched explicitly because
    /// `cost_qual_eval()` cannot cost unplanned subselects). Owner:
    /// `backend-optimizer-util-clauses/src/grounded.rs::contain_subplans`.
    pub fn contain_subplans(node: &Expr) -> PgResult<bool>
);

seam_core::seam!(
    /// `cost_qual_eval_node(&cost, node, root)` (costsize.c): the
    /// `(startup, per_tuple)` evaluation cost of the expression. Owner:
    /// `backend-optimizer-path-costsize` `&Expr` cost form. Only reached on the
    /// both-bounds path, and only when a real `root` is available.
    pub fn cost_qual_eval_expr(root: &PlannerInfo, node: &Expr) -> (f64, f64)
);

seam_core::seam!(
    /// `cpu_operator_cost` (costsize.c GUC): per-operator CPU cost estimate.
    /// Owner: `backend-optimizer-path-costsize::cpu_operator_cost`.
    pub fn cpu_operator_cost() -> f64
);

seam_core::seam!(
    /// `make_andclause(list_make2(a, b))` (clauses.c): a two-clause `BoolExpr`
    /// `AND`. Owner: `backend-nodes-core/src/makefuncs.rs::make_andclause`.
    pub fn make_andclause(a: Expr, b: Expr) -> Expr
);

seam_core::seam!(
    /// `rngtypcache->rng_opfamily` (typcache.h, `TYPECACHE_RANGE_INFO`): the
    /// btree opfamily backing the range type `rngtypid`. (Not carried on the
    /// trimmed `TypeCacheEntry`; owned by typcache.) `Err` carries the lookup
    /// `ereport(ERROR)`s.
    pub fn range_opfamily(rngtypid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_opfamily_member(opfamily, lefttype, righttype, strategy)`
    /// (lsyscache.c): the OID of the btree operator in `opfamily` for the given
    /// left/right input types and strategy number, or `InvalidOid` if none.
    /// Owner: `backend-utils-cache-lsyscache/src/opfamily_operator.rs`.
    pub fn get_opfamily_member(
        opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        strategy: i16,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_typcollation(typid)` (lsyscache.c): the element type's
    /// `pg_type.typcollation` (the `elemCollation` C reads off
    /// `typeCache->typcollation`; the trimmed [`TypeCacheEntry`] does not carry
    /// it). Owner: `backend-utils-cache-lsyscache/src/type_.rs`.
    pub fn get_typcollation(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `makeConst(consttype, -1, constcollid, constlen, constvalue, false,
    /// constbyval)` (makefuncs.c): fabricate a `Const` node. Owner:
    /// `backend-nodes-core/src/makefuncs.rs::make_const` (the eight-arg form;
    /// the detoast/`datumCopy` of a by-reference payload happens on that owner
    /// side).
    pub fn make_const<'mcx>(
        mcx: Mcx<'mcx>,
        consttype: Oid,
        constcollid: Oid,
        constlen: i32,
        constvalue: NodeDatum<'mcx>,
        constbyval: bool,
    ) -> PgResult<Const>
);

seam_core::seam!(
    /// `make_opclause(opno, opresulttype, opretset, leftop, rightop,
    /// opcollid, inputcollid)` (makefuncs.c): fabricate an `OpExpr` node.
    /// Called here as `make_opclause(oproid, BOOLOID, false, elemExpr,
    /// constExpr, InvalidOid, rng_collation)`. Owner:
    /// `backend-nodes-core/src/makefuncs.rs::make_opclause`.
    pub fn make_opclause(
        opno: Oid,
        opresulttype: Oid,
        opretset: bool,
        leftop: Expr,
        rightop: Expr,
        opcollid: Oid,
        inputcollid: Oid,
    ) -> Expr
);

/// `elem_contained_by_range_support` body (rangetypes.c:2251): the support fn
/// for `elem <@ range`. The `IsA(rawreq, SupportRequestSimplify)` dispatch and
/// the `Node*`→typed-request unwrap happen on the fmgr/optimizer dispatch side
/// (the `call_support_simplify` boundary, which hands us the real `root`/
/// `fcall`); this is the simplification kernel over the already-typed request.
/// Returns the simplified clause (or `None`).
pub fn elem_contained_by_range_support<'mcx>(
    mcx: Mcx<'mcx>,
    root: Option<&PlannerInfo>,
    fcall: &FuncExpr,
) -> PgResult<Option<Expr>> {
    // Assert(list_length(fexpr->args) == 2);
    // leftop = linitial(fexpr->args); rightop = lsecond(fexpr->args);
    debug_assert_eq!(fcall.args.len(), 2);
    let leftop = &fcall.args[0];
    let rightop = &fcall.args[1];

    // ret = find_simplified_clause(req->root, rightop, leftop);
    find_simplified_clause(mcx, root, rightop, leftop)
}

/// `range_contains_elem_support` body (rangetypes.c:2277): the support fn for
/// `range @> elem`.
pub fn range_contains_elem_support<'mcx>(
    mcx: Mcx<'mcx>,
    root: Option<&PlannerInfo>,
    fcall: &FuncExpr,
) -> PgResult<Option<Expr>> {
    // Assert(list_length(fexpr->args) == 2);
    // leftop = linitial(fexpr->args); rightop = lsecond(fexpr->args);
    debug_assert_eq!(fcall.args.len(), 2);
    let leftop = &fcall.args[0];
    let rightop = &fcall.args[1];

    // ret = find_simplified_clause(req->root, leftop, rightop);
    find_simplified_clause(mcx, root, leftop, rightop)
}

/// Registry adapter for `elem_contained_by_range_support` matching the
/// `backend-optimizer-util-clauses` `SupportRequestSimplify` dispatch shape
/// (`call_support_simplify` hands the decomposed `FuncExpr.args` plus the
/// per-call result/collation scalars; the request carries a NULL root, so this
/// passes `root = None` — `find_simplified_clause` then declines the
/// both-bounds cost-check case exactly as C does for an unavailable root).
#[allow(clippy::too_many_arguments)]
pub fn elem_contained_by_range_support_simplify<'mcx>(
    mcx: Mcx<'mcx>,
    _funcid: Oid,
    _result_type: Oid,
    _result_collid: Oid,
    _input_collid: Oid,
    args: &[Expr],
    _funcvariadic: bool,
    _estimate: bool,
) -> PgResult<Option<Expr>> {
    // Assert(list_length(fexpr->args) == 2);
    debug_assert_eq!(args.len(), 2);
    let leftop = &args[0];
    let rightop = &args[1];
    // ret = find_simplified_clause(req->root, rightop, leftop);
    find_simplified_clause(mcx, None, rightop, leftop)
}

/// Registry adapter for `range_contains_elem_support` (same dispatch shape).
#[allow(clippy::too_many_arguments)]
pub fn range_contains_elem_support_simplify<'mcx>(
    mcx: Mcx<'mcx>,
    _funcid: Oid,
    _result_type: Oid,
    _result_collid: Oid,
    _input_collid: Oid,
    args: &[Expr],
    _funcvariadic: bool,
    _estimate: bool,
) -> PgResult<Option<Expr>> {
    // Assert(list_length(fexpr->args) == 2);
    debug_assert_eq!(args.len(), 2);
    let leftop = &args[0];
    let rightop = &args[1];
    // ret = find_simplified_clause(req->root, leftop, rightop);
    find_simplified_clause(mcx, None, leftop, rightop)
}

/// `find_simplified_clause(root, rangeExpr, elemExpr)` (rangetypes.c:2850):
/// build `lower <= elem AND elem < upper` (per the range's inclusivity) when
/// the range argument is a constant; else `NULL`.
pub fn find_simplified_clause<'mcx>(
    mcx: Mcx<'mcx>,
    root: Option<&PlannerInfo>,
    range_expr: &Expr,
    elem_expr: &Expr,
) -> PgResult<Option<Expr>> {
    // can't do anything unless the range is a non-null constant
    // if (!IsA(rangeExpr, Const) || ((Const *) rangeExpr)->constisnull) return NULL;
    let Expr::Const(range_const) = range_expr else {
        return Ok(None);
    };
    if range_const.constisnull {
        return Ok(None);
    }

    // range = DatumGetRangeTypeP(((Const *) rangeExpr)->constvalue);
    // The `Const.constvalue` is the canonical `types_tuple` `Datum`. A range type
    // is a varlena, so the const value is carried as a `ByRef` byte image (or, if
    // it came in as a bare pointer word, a `ByVal` word); `as_byref_word()` is the
    // `DatumGetPointer(X)` view — it yields the scalar word for a `ByVal` arm and
    // the address of the owned varlena image for a `ByRef` arm. `range_const` (and
    // hence the `ByRef` bytes) stays borrowed for the whole call, so the pointer
    // is valid until `DatumGetRangeTypeP` detoasts it owner-side. The
    // range-deserialize seam takes the bare-word `types_datum::Datum`, so lift the
    // pointer word across.
    let constvalue =
        types_datum::datum::Datum::from_usize(range_const.constvalue.as_byref_word());
    let range =
        backend_utils_adt_rangetypes_seams::datum_get_range_type_p::call(mcx, constvalue)?;

    // RangeTypeGetOid(range): the serialized header's range type oid.
    let rngtypid = range.rangetypid();

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
        return Ok(Some(Expr::Const(make_bool_const::call(false, false))));
    } else if lower.infinite && upper.infinite {
        // the range has infinite bounds, so it matches everything
        return Ok(Some(Expr::Const(make_bool_const::call(true, false))));
    }

    // at least one bound is available, we have something to work with
    // TypeCacheEntry *elemTypcache = rangetypcache->rngelemtype;
    // Oid opfamily = rangetypcache->rng_opfamily;
    // Oid rng_collation = rangetypcache->rng_collation;
    let opfamily = range_opfamily::call(rngtypid)?;
    let rng_collation = rangetypcache.rng_collation;
    let mut lower_expr: Option<Expr> = None;
    let mut upper_expr: Option<Expr> = None;

    if !lower.infinite && !upper.infinite {
        // When both bounds are present, we have a problem: the "simplified"
        // clause would need to evaluate the elemExpr twice. That's definitely
        // not okay if the elemExpr is volatile, and it's also unattractive if
        // the elemExpr is expensive.
        if contain_volatile_functions::call(elem_expr)? {
            return Ok(None);
        }

        // We define "expensive" as "contains any subplan or more than 10
        // operators". Note that the subplan search has to be done explicitly,
        // since cost_qual_eval() will barf on unplanned subselects.
        if contain_subplans::call(elem_expr)? {
            return Ok(None);
        }
        // cost_qual_eval_node(&eval_cost, (Node *) elemExpr, root). C's
        // `eval_const_expressions` entry passes a NULL root; we mirror that by
        // declining when no PlannerInfo is available to cost the elemExpr
        // (declining is always a valid planner answer — same exit C takes for an
        // "expensive" elemExpr).
        let Some(root) = root else {
            return Ok(None);
        };
        let (startup, per_tuple) = cost_qual_eval_expr::call(root, elem_expr);
        if startup + per_tuple > 10.0 * cpu_operator_cost::call() {
            return Ok(None);
        }
    }

    // Okay, try to build boundary comparison expressions
    if !lower.infinite {
        lower_expr = build_bound_expr(
            mcx,
            elem_expr.clone(),
            NodeDatum::from_usize(lower.val.as_usize()),
            true,
            lower.inclusive,
            elem_typcache.type_id,
            elem_typcache.typlen as i32,
            elem_typcache.typbyval,
            opfamily,
            rng_collation,
        )?;
        if lower_expr.is_none() {
            return Ok(None);
        }
    }

    if !upper.infinite {
        upper_expr = build_bound_expr(
            mcx,
            // The C copies the elemExpr (copyObject) only when it needs two
            // copies; here each `build_bound_expr` already takes an owned `Expr`
            // by value, so we just clone the borrowed `elemExpr` for each bound
            // (the value model's deep `Clone` IS `copyObject`).
            elem_expr.clone(),
            NodeDatum::from_usize(upper.val.as_usize()),
            false,
            upper.inclusive,
            elem_typcache.type_id,
            elem_typcache.typlen as i32,
            elem_typcache.typbyval,
            opfamily,
            rng_collation,
        )?;
        if upper_expr.is_none() {
            return Ok(None);
        }
    }

    match (lower_expr, upper_expr) {
        (Some(l), Some(u)) => Ok(Some(make_andclause::call(l, u))),
        (Some(l), None) => Ok(Some(l)),
        (None, Some(u)) => Ok(Some(u)),
        (None, None) => {
            // Assert(false);
            debug_assert!(false, "find_simplified_clause produced no bound expression");
            Ok(None)
        }
    }
}

/// `build_bound_expr(elemExpr, val, isLowerBound, isInclusive, typeCache,
/// opfamily, rng_collation)` (rangetypes.c:2972): construct one
/// `elem <op> boundval` `OpExpr`.
///
/// The element-type identity C reads off the `typeCache` argument is threaded
/// here as `elem_type`/`elem_type_len`/`elem_byvalue`; the element collation is
/// resolved via the `get_typcollation` seam (C reads `typeCache->typcollation`;
/// the trimmed [`TypeCacheEntry`] does not carry it). The strategy-number
/// selection and the `OidIsValid` guard are this crate's own logic; only the
/// catalog lookup (`get_opfamily_member`) and the node fabrication (`makeConst`
/// / `make_opclause`) route to the lsyscache / makefuncs owners through thin
/// seams.
#[allow(clippy::too_many_arguments)]
pub fn build_bound_expr<'mcx>(
    mcx: Mcx<'mcx>,
    elem_expr: Expr,
    val: NodeDatum<'mcx>,
    is_lower_bound: bool,
    is_inclusive: bool,
    elem_type: Oid,
    elem_type_len: i32,
    elem_byvalue: bool,
    opfamily: Oid,
    rng_collation: Oid,
) -> PgResult<Option<Expr>> {
    // Identify the comparison operator to use. C's local `strategy` is `int16`;
    // the `BT*StrategyNumber` macros are small positive constants.
    let strategy: i16 = (if is_lower_bound {
        if is_inclusive {
            BTGreaterEqualStrategyNumber
        } else {
            BTGreaterStrategyNumber
        }
    } else if is_inclusive {
        BTLessEqualStrategyNumber
    } else {
        BTLessStrategyNumber
    }) as i16;

    // We could use exprType(elemExpr) here, if it ever becomes possible that
    // elemExpr is not the exact same type as the range elements.
    let oproid = get_opfamily_member::call(opfamily, elem_type, elem_type, strategy)?;

    // We don't really expect failure here, but just in case ...
    if !OidIsValid(oproid) {
        return Ok(None);
    }

    // OK, convert "val" to a full-fledged Const node, and make the OpExpr.
    // makeConst(elemType, -1, elemCollation, elemTypeLen, val, false, elemByValue)
    // elemCollation = typeCache->typcollation.
    let elem_collation = get_typcollation::call(elem_type)?;
    let const_expr = make_const::call(
        mcx,
        elem_type,
        elem_collation,
        elem_type_len,
        val,
        elem_byvalue,
    )?;

    // make_opclause(oproid, BOOLOID, false, elemExpr, constExpr, InvalidOid,
    //               rng_collation)
    Ok(Some(make_opclause::call(
        oproid,
        BOOLOID,
        false,
        elem_expr,
        Expr::Const(const_expr),
        InvalidOid,
        rng_collation,
    )))
}
