//! Seam declarations for the `backend-nodes-nodeFuncs` unit
//! (`nodes/nodeFuncs.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use types_core::Oid;
use types_error::PgResult;
use types_nodes::Expr;

/// The `(typid, typmod, collation)` triple `exprType`/`exprTypmod`/
/// `exprCollation` report for one expression node.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExprTypeInfo {
    /// `exprType(expr)`.
    pub typid: Oid,
    /// `exprTypmod(expr)`.
    pub typmod: i32,
    /// `exprCollation(expr)`.
    pub collation: Oid,
}
use types_fmgr::ExternalFnExpr;

seam_core::seam!(
    /// `exprType(expr)` / `exprTypmod(expr)` / `exprCollation(expr)`
    /// (nodeFuncs.c): the result type OID, type modifier, and collation of an
    /// expression node, read together. The three C functions are pure node
    /// inspections (no allocation); the bundling lets partition-key build read
    /// all three from one call. `Err` carries the C `elog(ERROR, "unrecognized
    /// node type")` for an unexpected tag.
    pub fn expr_type_info(expr: &Expr<'static>) -> PgResult<ExprTypeInfo>
);

seam_core::seam!(
    /// `exprType(node)` (nodeFuncs.c) — the result type Oid of an expression
    /// node. Used by `get_fn_expr_rettype`. Pure read; returns `InvalidOid` for
    /// an unhandled node kind, as C falls through.
    pub fn expr_type(expr: ExternalFnExpr) -> Oid
);

seam_core::seam!(
    /// `exprIsLengthCoercion(expr, &coercedTypmod)` (nodeFuncs.c) — detect
    /// whether `expr` is a length-coercion cast (a FuncExpr/ArrayCoerceExpr that
    /// applies a typmod, e.g. `numeric(16,4)` / `varchar(8)`). Returns
    /// `(true, coercedTypmod)` with the coerced type modifier, or `(false, -1)`.
    /// Pure node inspection. Used by ruleutils' cast deparse to print the typmod
    /// on the target type name. `Err` carries an unexpected-node `elog(ERROR)`.
    pub fn expr_is_length_coercion(expr: &Expr<'static>) -> PgResult<(bool, i32)>
);

seam_core::seam!(
    /// `get_call_expr_argtype(expr, argnum)` (fmgr.c) — the declared type of the
    /// `argnum`'th argument of a call expression (the `IsA` dispatch over
    /// `FuncExpr`/`OpExpr`/`DistinctExpr`/`ScalarArrayOpExpr`/`NullIfExpr`/
    /// `WindowFunc`, `exprType(list_nth(args, argnum))` with range guard and the
    /// `ScalarArrayOpExpr` element-type hack). Returns `InvalidOid` out of range.
    pub fn call_expr_argtype(expr: ExternalFnExpr, argnum: i32) -> Oid
);

seam_core::seam!(
    /// `get_call_expr_argtype(expr, argnum)` (fmgr.c:1929) over a *field-bearing*
    /// owned call-expression node (`&Expr`), as opposed to the tag-only
    /// [`call_expr_argtype`] carrier. This is the entry the fmgr `get_fn_expr_*`
    /// readers use once `fmgr_info_set_expr` stamps a real `Expr` onto the
    /// `FmgrInfo.fn_expr` erased slot: the `IsA` dispatch over
    /// `FuncExpr`/`OpExpr`/`DistinctExpr`/`ScalarArrayOpExpr`/`NullIfExpr`/
    /// `WindowFunc`, `exprType(list_nth(args, argnum))` with the range guard, and
    /// the `ScalarArrayOpExpr` `argnum == 1` element-type hack
    /// (`get_base_element_type`). Returns `InvalidOid` out of range / for an
    /// unhandled node kind (the C fall-through). `Err` carries the
    /// `get_base_element_type` / `exprType` cache-lookup `ereport`.
    pub fn get_call_expr_argtype_expr(expr: &Expr<'static>, argnum: i32) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_call_expr_arg_stable(expr, argnum)` (fmgr.c) — true iff the indexed
    /// argument is a `Const` or an external `Param` (the same `IsA` dispatch).
    pub fn call_expr_arg_stable(expr: ExternalFnExpr, argnum: i32) -> bool
);

seam_core::seam!(
    /// `get_fn_expr_variadic` body: `IsA(expr, FuncExpr) ?
    /// ((FuncExpr *) expr)->funcvariadic : false` (fmgr.c).
    pub fn expr_variadic(expr: ExternalFnExpr) -> bool
);

seam_core::seam!(
    /// `get_fn_expr_variadic` body over a *field-bearing* owned call-expression
    /// node (`&Expr`), the form the fmgr `get_fn_expr_variadic` reader uses once
    /// `fmgr_info_set_expr` stamps a real `Expr` onto `FmgrInfo.fn_expr`. C:
    /// `IsA(expr, FuncExpr) ? ((FuncExpr *) expr)->funcvariadic : false`. Unlike
    /// the tag-only [`expr_variadic`] carrier (which cannot see `funcvariadic`),
    /// this reads the real flag off the node.
    pub fn expr_variadic_expr(expr: &Expr<'static>) -> bool
);

seam_core::seam!(
    /// `get_call_expr_arg_stable(expr, argnum)` (fmgr.c) over a *field-bearing*
    /// owned call-expression node (`&Expr`), the form the fmgr
    /// `get_fn_expr_arg_stable` reader uses once `fmgr_info_set_expr` stamps a
    /// real `Expr`. C: the `IsA` arg-list dispatch
    /// (`FuncExpr`/`OpExpr`/`DistinctExpr`/`ScalarArrayOpExpr`/`NullIfExpr`/
    /// `WindowFunc`), range guard, then true iff `list_nth(args, argnum)` is a
    /// `Const` or an external (`PARAM_EXTERN`) `Param`. Unlike the tag-only
    /// [`call_expr_arg_stable`] carrier (which cannot see the argument list),
    /// this reads the real arguments off the node.
    pub fn call_expr_arg_stable_expr(expr: &Expr<'static>, argnum: i32) -> bool
);

seam_core::seam!(
    /// `get_call_expr_argtype(call_expr, argnum)` (fmgr.c:1929) keyed by the
    /// unified plan/expression `Node` the funcapi result-type cluster threads as
    /// its `call_expr` (`resolve_polymorphic_tupdesc` / `_argtypes`). The
    /// argument-bearing expression nodes (`FuncExpr`/`OpExpr`/`DistinctExpr`/
    /// `ScalarArrayOpExpr`/`NullIfExpr`/`WindowFunc`) are not yet modelled by the
    /// plan-tree `Node` enum, so this stays nodeFuncs-owned: the `IsA` dispatch,
    /// `exprType(list_nth(args, argnum))` with the range guard, and the
    /// `ScalarArrayOpExpr` element-type hack all live in nodeFuncs. Returns
    /// `InvalidOid` out of range / for an unhandled kind, as C falls through.
    pub fn get_call_expr_argtype_node<'mcx>(
        call_expr: &types_nodes::nodes::Node<'mcx>,
        argnum: i32,
    ) -> Oid
);

seam_core::seam!(
    /// `exprInputCollation(node)` (nodeFuncs.c) keyed by the unified plan/
    /// expression `Node` the funcapi cluster threads as its `call_expr`. Reads
    /// the input collation a function call uses (the `FuncExpr.inputcollid` /
    /// `OpExpr.inputcollid` / … family); a pure node inspection. The expression
    /// nodes are not yet modelled by the plan-tree `Node` enum, so this stays
    /// nodeFuncs-owned. Returns `InvalidOid` for an unhandled node kind.
    pub fn expr_input_collation_node<'mcx>(
        node: &types_nodes::nodes::Node<'mcx>,
    ) -> Oid
);

seam_core::seam!(
    /// `exprInputCollation(node)` (nodeFuncs.c) over a *field-bearing* owned
    /// call-expression node (`&Expr`) — the form the funcapi polymorphic resolver
    /// uses when the `call_expr` arrives as the erased `FmgrInfo.fn_expr` `Expr`
    /// (a polymorphic SRF/composite call resolved off `fcinfo->flinfo->fn_expr`),
    /// rather than as a plan-tree `Node`. Reads the input collation a function
    /// call uses (`FuncExpr.inputcollid` / `OpExpr.inputcollid` / …); a pure node
    /// inspection. Returns `InvalidOid` for a node kind that stores no input
    /// collation.
    pub fn expr_input_collation_expr(expr: &Expr<'static>) -> Oid
);

/* ======================================================================
 * Node-inspection helpers reached by the optimizer pathkey/equivclass leaves.
 *
 * Two shapes:
 *  - `&Expr`-keyed: pure inspections over the arena expression value (`Expr`,
 *    the element of `PlannerInfo::node_arena`).
 *  - `NodeId`+`&PlannerInfo`-keyed: the `TargetEntry` / `SortGroupClause`
 *    payload reads. Those node kinds are NOT representable by the current
 *    `node_arena: Vec<Expr<'static>>` model (`Expr` has no `TargetEntry` /
 *    `SortGroupClause` variant), so the inspection is delegated whole to the
 *    owner, which resolves the handle in its own (richer) node model.
 *
 * nodeFuncs-owned (nodeFuncs.c) except `equal` (equalfuncs.c), `copyObject`
 * (copyfuncs.c), the `is_notclause` / `get_notclausearg` clause helpers
 * (makefuncs.c / clauses.h), and the `get_sortgroupref_*` tlist helpers
 * (optimizer/util/tlist.c) — all bundled here as the pathkeys consumer reaches
 * each through this one inspection surface. Installed by their respective
 * owners; until then a call panics.
 * ==================================================================== */

seam_core::seam!(
    /// `exprCollation(node)` (nodeFuncs.c) — the collation of an expression node,
    /// over the arena `Expr` value. Pure read.
    pub fn exprCollation(expr: &Expr<'static>) -> Oid
);

seam_core::seam!(
    /// `exprLocation(node)` (nodeFuncs.c) — the parse location of an expression
    /// node (or -1 if unknown), over the arena `Expr` value. Pure read.
    /// Installed by the nodeFuncs.c owner; until then a call panics.
    pub fn exprLocation(expr: &Expr<'static>) -> i32
);

seam_core::seam!(
    /// `equal(a, b)` (equalfuncs.c) — deep structural equality of two expression
    /// nodes, over the arena `Expr` value. `equal(NULL, NULL)` is true; this
    /// helper is only called with two present nodes.
    pub fn equal(a: &Expr<'static>, b: &Expr<'static>) -> bool
);

seam_core::seam!(
    /// `copyObject(node)` (copyfuncs.c) — a deep copy of an expression node,
    /// over the arena `Expr` value (the C `copyObject(var)` in
    /// `find_var_for_subquery_tle`).
    pub fn copyObject(expr: &Expr<'static>) -> Expr<'static>
);

seam_core::seam!(
    /// `is_notclause(clause)` (nodes/makefuncs.h) — true iff `clause` is a
    /// `BoolExpr` with `boolop == NOT_EXPR`.
    pub fn is_notclause(clause: &Expr<'static>) -> bool
);

seam_core::seam!(
    /// `get_notclausearg(notclause)` (clauses.h) — the (sole) argument of a NOT
    /// `BoolExpr`. C returns `linitial(args)` — a borrowed pointer into the
    /// existing tree, never a copy — so we hand back a borrow tied to the input
    /// (a `.clone()` of a `SubLink`-bearing arg would hit its deep-copy guard).
    pub fn get_notclausearg<'a>(notclause: &'a Expr<'static>) -> &'a Expr<'static>
);

seam_core::seam!(
    /// `remove_nulling_relids(node, removable_relids, except_relids)`
    /// (nodeFuncs.c) — strip the given nulling relids from any `Var`/PHV
    /// `varnullingrels`/`phnullingrels` in `node`, returning the rewritten
    /// expression value. `except_relids` of `None` is the C `NULL`. The `node`
    /// is taken BY VALUE: C mutates the passed tree in place, so the owner
    /// moves it into the mutator (a borrow would force a `.clone()` that panics
    /// on owned-subtree Exprs like `Aggref`).
    pub fn remove_nulling_relids(
        node: Expr<'static>,
        removable_relids: &types_pathnodes::Relids,
        except_relids: &types_pathnodes::Relids,
    ) -> Expr<'static>
);

seam_core::seam!(
    /// `get_sortgroupref_tle(sortref, targetList)` (optimizer/util/tlist.c) — the
    /// `TargetEntry` in `target_list` whose `ressortgroupref == sortref`,
    /// returned as its `NodeId` handle (the C `elog(ERROR, "ORDER/GROUP BY
    /// expression not found in targetlist")` is surfaced as a loud panic in the
    /// owner's installed body). `target_list` entries are `NodeId` handles to
    /// `TargetEntry` nodes, resolved in the owner's node model.
    pub fn get_sortgroupref_tle(
        root: &types_pathnodes::PlannerInfo,
        sortref: u32,
        target_list: &[types_pathnodes::NodeId],
    ) -> types_pathnodes::NodeId
);

seam_core::seam!(
    /// `get_sortgroupclause_expr(sortcl, targetList)` (optimizer/util/tlist.c) —
    /// the expression `NodeId` of the `TargetEntry` referenced by the
    /// `SortGroupClause`'s `tleSortGroupRef`. `sortcl` is supplied as the
    /// `SortGroupClause` `NodeId`; entries are `TargetEntry` `NodeId`s, resolved
    /// in the owner's node model.
    pub fn get_sortgroupclause_expr(
        root: &types_pathnodes::PlannerInfo,
        sortcl: types_pathnodes::NodeId,
        target_list: &[types_pathnodes::NodeId],
    ) -> types_pathnodes::NodeId
);

seam_core::seam!(
    /// `get_sortgroupref_clause_noerr(sortref, clauses)`
    /// (optimizer/util/tlist.c) — the `SortGroupClause` `NodeId` in `clauses`
    /// with `tleSortGroupRef == sortref`, or `None` if none (the `_noerr`
    /// variant). `clauses` entries are `SortGroupClause` `NodeId`s.
    pub fn get_sortgroupref_clause_noerr(
        root: &types_pathnodes::PlannerInfo,
        sortref: u32,
        clauses: &[types_pathnodes::NodeId],
    ) -> Option<types_pathnodes::NodeId>
);

/// The `SortGroupClause` payload reads pathkeys needs over a `NodeId` whose node
/// is a `SortGroupClause` (resolved in the owner's node model): the
/// `tleSortGroupRef`, `sortop`, `reverse_sort`, `nulls_first` fields, plus the
/// `TargetEntry` reads (`ressortgroupref`, `resno`, `resjunk`, `expr`). Bundled
/// as one struct-returning seam per node kind so the consumer reads all fields
/// from one resolved call.
#[derive(Clone, Copy, Debug, Default)]
pub struct SortGroupClauseInfo {
    /// `tleSortGroupRef`.
    pub tle_sort_group_ref: u32,
    /// `sortop` (0 if not sortable).
    pub sortop: Oid,
    /// `reverse_sort`.
    pub reverse_sort: bool,
    /// `nulls_first`.
    pub nulls_first: bool,
}

seam_core::seam!(
    /// Read the `SortGroupClause` fields off a `NodeId` resolving to a
    /// `SortGroupClause`.
    pub fn sortgroupclause_info(
        root: &types_pathnodes::PlannerInfo,
        sortcl: types_pathnodes::NodeId,
    ) -> SortGroupClauseInfo
);

/// The `TargetEntry` payload reads pathkeys needs over a `NodeId` whose node is
/// a `TargetEntry`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TargetEntryInfo {
    /// `ressortgroupref`.
    pub ressortgroupref: u32,
    /// `resno`.
    pub resno: i16,
    /// `resjunk`.
    pub resjunk: bool,
    /// the `TargetEntry.expr` as a `NodeId` handle.
    pub expr: types_pathnodes::NodeId,
}

seam_core::seam!(
    /// Read the `TargetEntry` fields off a `NodeId` resolving to a
    /// `TargetEntry`.
    pub fn targetentry_info(
        root: &types_pathnodes::PlannerInfo,
        tle: types_pathnodes::NodeId,
    ) -> TargetEntryInfo
);
