//! Seam declarations for the `backend-optimizer-util-clauses` unit
//! (`optimizer/util/clauses.c`).
//!
//! The `contain_subplans` seam is INWARD — owned and installed by
//! `backend-optimizer-util-clauses` itself (consumed by `nodeValuesscan`).
//!
//! The rest are OUTWARD dependencies of `clauses.c`'s const-folding engine on
//! subsystems not yet ported into the layered tree (the fmgr executor-backed
//! evaluator, the `pg_proc` form read, the SQL-function inliner, planner
//! support functions, the type/domain catalog probes). They mirror C's
//! `simplify_function` / `evaluate_function` / `evaluate_expr` / `inline_*`
//! call shapes and are installed by their real owners (fmgr / funcapi / proc /
//! typcache / prepqual / funccmds) when those land; until then a call panics
//! loudly. clauses.c folds these shapes through the executor, so a silent
//! skip is not allowed — the seam panics rather than fabricate a fold.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_tuple::backend_access_common_heaptuple::Datum;

/// `Form_pg_proc` subset (`pg_proc.h`) that `clauses.c`'s folding engine reads:
/// `simplify_function` / `evaluate_function` / `expand_function_arguments` /
/// `inline_function`. The C reads these straight off the cached pg_proc tuple
/// (`GETSTRUCT`); the safe model copies the row out by value.
#[derive(Clone, Debug, Default)]
pub struct PgProcSimple {
    /// `pronargs` — number of declared arguments.
    pub pronargs: i16,
    /// `pronargdefaults` — number of arguments with defaults.
    pub pronargdefaults: i16,
    /// `prorettype` — declared result type OID.
    pub prorettype: Oid,
    /// `proretset` — does the function return a set?
    pub proretset: bool,
    /// `proisstrict` — strict (returns NULL on any NULL input)?
    pub proisstrict: bool,
    /// `provolatile` — `PROVOLATILE_{IMMUTABLE,STABLE,VOLATILE}` byte.
    pub provolatile: u8,
    /// `prosecdef` — security-definer?
    pub prosecdef: bool,
    /// `prosupport` — planner support function OID (`InvalidOid` if none).
    pub prosupport: Oid,
    /// `proargtypes` — declared argument type OIDs (`oidvector`).
    pub proargtypes: alloc::vec::Vec<Oid>,
    /// True iff `prolang` is the SQL language (`SQLlanguageId`).
    pub prolang_is_sql: bool,
    /// True iff `proconfig` is NULL (no per-function GUC settings).
    pub proconfig_isnull: bool,
    /// `prokind` — `PROKIND_{FUNCTION,AGGREGATE,WINDOW,PROCEDURE}` byte. Read by
    /// `inline_function`'s paranoia gate (clauses.c:4585) and passed to
    /// `check_sql_fn_retval`.
    pub prokind: u8,
    /// `NameStr(proname)` — the function's name. Used by `fmgr_sql`'s
    /// `prepare_sql_fn_parse_info` to qualify SQL-function-body argument-name
    /// references (`fname.param`).
    pub proname: alloc::string::String,
    /// `proargnames` (text[], nullable) decoded to per-arg names, `None` when
    /// the function has no named arguments. Used by `prepare_sql_fn_parse_info`
    /// (`get_func_input_arg_names`) to resolve a body bareword to a `$n` Param.
    pub proargnames: Option<alloc::vec::Vec<Option<alloc::string::String>>>,
    /// `proallargtypes` (oid[], nullable) — all argument types INCLUDING OUT/INOUT
    /// args, `None` when it equals `proargtypes` (no OUT args). Used by
    /// `expand_function_arguments(include_out_arguments=true)` for CALL.
    pub proallargtypes: Option<alloc::vec::Vec<Oid>>,
    /// `proargmodes` (char[], nullable) — per-argument mode bytes
    /// (`PROARGMODE_{IN,OUT,INOUT,VARIADIC,TABLE}`), `None` when all args are IN.
    /// Read by `transformCallStmt` to split input/output arguments.
    pub proargmodes: Option<alloc::vec::Vec<i8>>,
}

extern crate alloc;

seam_core::seam!(
    /// `contain_subplans(Node *clause)` (clauses.c): walk the expression tree
    /// and return whether it contains any `SubPlan` or `AlternativeSubPlan`
    /// node. `ValuesScan` init passes one VALUES row's expression list (the C
    /// `(Node *) exprs`, an implicitly-AND'd `List *`); the walker descends it
    /// like any other node. A pure structural predicate, so infallible.
    ///
    /// INWARD: installed by `backend-optimizer-util-clauses::init_seams`.
    pub fn contain_subplans<'mcx>(clause: &[types_nodes::primnodes::Expr<'mcx>]) -> bool
);

// ===========================================================================
// OUTWARD const-folding-engine seams (installed by their real owners).
// ===========================================================================

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, funcid)` + `GETSTRUCT` (clauses.c): read the
    /// `pg_proc` row for `funcid` as a [`PgProcSimple`]. `Err` carries the C
    /// `elog(ERROR, "cache lookup failed for function %u")`. Owner: pg_proc /
    /// syscache.
    pub fn get_func_form(funcid: Oid) -> PgResult<PgProcSimple>
);

seam_core::seam!(
    /// The fmgr leg of `evaluate_expr` (clauses.c): invoke `funcid` with the
    /// given argument `(value, isnull, argtype)` triples under `inputcollid`
    /// (strictness honored by the FunctionCallInvoke), returning the result
    /// `(value, isnull)`. `rettype` is the concrete result type (for the
    /// by-value/by-ref result lane and the "copy result out of context"
    /// detoast). The result `Datum` is allocated in the caller's `mcx` (a
    /// by-reference value materializes its referent there, mirroring C's "copy
    /// result out of sub-context"). `Err` carries the function's
    /// `ereport(ERROR)` surface. Owner: fmgr (`backend-utils-fmgr-fmgr`).
    pub fn fmgr_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        funcid: Oid,
        inputcollid: Oid,
        args: alloc::vec::Vec<(Datum<'mcx>, bool, Oid)>,
        rettype: Oid,
        fn_expr: Option<&Expr<'mcx>>,
    ) -> PgResult<(Datum<'mcx>, bool)>
);

seam_core::seam!(
    /// `evaluate_expr(expr, result_type, result_typmod, result_collation)`
    /// (clauses.c) — the executor-backed const-evaluator for the expression
    /// shapes the in-crate fmgr fast path does not handle (SAOP / MinMax / Row /
    /// SubscriptingRef / FieldSelect-on-Const / ConvertRowtype / ArrayCoerce /
    /// estimate-mode SQLValueFunction / multidim ArrayExpr). C runs these through
    /// a fully initialized ExprState; the safe model rides this seam. `Err`
    /// carries the evaluation `ereport(ERROR)`. Owner: executor (execExpr).
    pub fn evaluate_expr_fallback(
        expr: Expr<'static>,
        result_type: Oid,
        result_typmod: i32,
        result_collation: Oid,
    ) -> PgResult<Expr<'static>>
);

seam_core::seam!(
    /// `inline_function(...)` SQL-language body (clauses.c:4553 onward) — the
    /// prosqlbody-or-prosrc parse/analyze + "simple SELECT expression" gate +
    /// `check_sql_fn_retval` type check + `substitute_actual_parameters` +
    /// per-parameter usecount machinery that inlines a simple SQL-language
    /// function call. `Ok(None)` = "do not inline" (C's many decline paths);
    /// `Ok(Some(expr))` = the SUBSTITUTED inlined expression — NOT yet
    /// recursively re-simplified (clauses.c:4890 `eval_const_expressions_mutator`
    /// re-run, and its `active_fns` recursion guard, are run in-crate by the
    /// `inline_function` caller, which owns the fold `EceContext`). `Err`
    /// carries the parse/analyze `ereport(ERROR)`. Reached only after the cheap
    /// in-crate catalog gates pass. The caller passes the already-read pg_proc
    /// `form`, the function `prosrc`, and the cooked `prosqlbody` (`None` when
    /// the catalog attribute is NULL). Owner: the SQL-function parse/rewrite leg
    /// (functions.c / parser); installed from `backend-parser-analyze`, the
    /// lowest crate that owns both the parser and the fold-crate `contain_*`
    /// walkers without a dependency cycle.
    pub fn inline_sql_function<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        form: &PgProcSimple,
        prosrc: &str,
        prosqlbody: Option<&str>,
        funcid: Oid,
        result_type: Oid,
        result_collid: Oid,
        input_collid: Oid,
        args: &[Expr<'mcx>],
        funcvariadic: bool,
        estimate: bool,
    ) -> PgResult<Option<Expr<'mcx>>>
);

seam_core::seam!(
    /// `inline_function`'s pg_proc body read (clauses.c:4628 `prosrc` via
    /// `SysCacheGetAttrNotNull(PROCOID, .., Anum_pg_proc_prosrc)` + clauses.c:4646
    /// `prosqlbody` via `SysCacheGetAttr(PROCOID, .., Anum_pg_proc_prosqlbody)`).
    /// Returns `(prosrc, prosqlbody)` where `prosqlbody` is the cooked
    /// node-string (the catalog stores `nodeToString(prosqlbody)`), or `None`
    /// when the attribute is NULL. Owner: syscache (the pg_proc projection leg).
    pub fn get_func_sql_body<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<(mcx::PgString<'mcx>, Option<mcx::PgString<'mcx>>)>
);

seam_core::seam!(
    /// The `inline_set_returning_function` SQL-language inline core
    /// (clauses.c:5134 onward): prosrc parse + rewrite + single-SELECT
    /// querytree validation, returning the inlined query expression
    /// (`Ok(Some)`), or `Ok(None)` to decline. Reached only for an inlinable
    /// SQL-language SRF after the gate ladder passes. `Err` carries the
    /// parse/analyze `ereport(ERROR)`. Owner: clauses.c SRF-inliner leg.
    pub fn inline_set_returning_function_core(funcid: Oid) -> PgResult<Option<Expr<'static>>>
);

seam_core::seam!(
    /// The SQL-language body parse/rewrite/validate core of
    /// `inline_set_returning_function` (clauses.c:5185 onward), reached only once
    /// the gate ladder (run in-crate) has confirmed the RTE wraps a single
    /// inlinable SQL-language SRF. Parses `prosrc`/`prosqlbody`, rewrites, and
    /// validates the single-SELECT querytree, returning the inlined `Query`
    /// (`Ok(Some)`) to substitute as the RTE's subquery, or `Ok(None)` to
    /// decline. `Err` carries the parse/analyze `ereport(ERROR)`. Owner:
    /// clauses.c SRF-inliner leg (gated on the SQL-function parse/rewrite path).
    pub fn inline_set_returning_function_sql_body<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<types_nodes::copy_query::Query<'mcx>>>
);

seam_core::seam!(
    /// `FmgrHookIsNeeded(functionId)` (fmgr.c): whether a loadable module has
    /// installed an fmgr entry/exit hook for `function_id` (the
    /// `needs_fmgr_hook` plugin). Default (no plugin) is `false`. Owner: fmgr
    /// (`backend-utils-fmgr-core`).
    pub fn fmgr_hook_is_needed(function_id: Oid) -> bool
);

seam_core::seam!(
    /// `SupportRequestRows` dispatch (plancat.c:2200): call the function's
    /// planner support function `prosupport` to estimate a set-returning
    /// function's rowcount. `node` is the (const-folded) `FuncExpr`. `Ok(Some)`
    /// is the estimate (C's `req->rows`); `Ok(None)` declines (no support
    /// function or no constant arguments — the caller falls back on
    /// `pg_proc.prorows`). `Err` carries the support function's
    /// `ereport(ERROR)`. Owner: `backend-optimizer-util-clauses` (the
    /// support-rows registry; per-function kernels register from their crates).
    pub fn call_support_rows<'mcx>(prosupport: Oid, funcid: Oid, node: &Expr<'mcx>) -> PgResult<Option<f64>>
);

seam_core::seam!(
    /// `SupportRequestCost` dispatch (plancat.c:2137): call the function's
    /// planner support function `prosupport` to refine a `(startup, per_tuple)`
    /// cost. `node` is the call's `FuncExpr`/`OpExpr` (or `None`). `Ok(Some)` is
    /// the refined cost (C's `req.startup`/`req.per_tuple`); `Ok(None)` declines
    /// (no support function, or it does not handle the cost request — the caller
    /// falls back on `pg_proc.procost`). `Err` carries the support function's
    /// `ereport(ERROR)`. Owner: `backend-optimizer-util-clauses` (the
    /// support-cost registry; per-function kernels register from their crates).
    pub fn call_support_cost(
        prosupport: Oid,
        funcid: Oid,
        node: Option<&Expr<'static>>,
    ) -> PgResult<Option<(f64, f64)>>
);

seam_core::seam!(
    /// `inline_set_returning_function(root, rte)` (clauses.c:5134) — the full
    /// set-returning-function inliner: inspect the FUNCTION RTE's single
    /// `RangeTblFunction`/`FuncExpr`, run the gate ladder (LANGUAGE SQL, no
    /// SECURITY DEFINER, simple `RETURN QUERY`-able prosrc, polymorphic
    /// resolution, etc.), and on success parse + rewrite + validate the
    /// function body into a single owned `Query` to substitute as the RTE's
    /// subquery (`Ok(Some)`), or `Ok(None)` to decline. `Err` carries the
    /// parse/analyze `ereport(ERROR)`. **Owner: the clauses.c SRF-inliner leg
    /// (gated on the SQL-function parse/rewrite path); seam-and-panics until it
    /// lands.** Unlike the `_core` variant above (which returns an `Expr` keyed
    /// by funcid for the scalar-SQL-inline path), this returns the inlined
    /// `Query` that `preprocess_function_rtes` (prepjointree.c:931) stores into
    /// `rte->subquery`.
    pub fn inline_set_returning_function<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    ) -> PgResult<Option<types_nodes::copy_query::Query<'mcx>>>
);

seam_core::seam!(
    /// `SupportRequestSimplify` (clauses.c): call the function's planner
    /// support function `prosupport` to attempt a custom simplification.
    /// `Ok(None)` = no simplification. `Err` carries the support function's
    /// `ereport(ERROR)`. Owner: per-function support functions (no
    /// PlannerInfo is threaded; the request carries a NULL root). The C
    /// support function allocates its replacement clause in
    /// `CurrentMemoryContext`; `mcx` is the const-folding mutator's context
    /// (`eval_const_expressions_mutator`), into which the produced `Expr` is
    /// materialized.
    pub fn call_support_simplify<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        prosupport: Oid,
        funcid: Oid,
        result_type: Oid,
        result_collid: Oid,
        input_collid: Oid,
        args: &[Expr<'mcx>],
        funcvariadic: bool,
        estimate: bool,
    ) -> PgResult<Option<Expr<'mcx>>>
);

seam_core::seam!(
    /// `fetch_function_defaults(funcid)` (clauses.c): read and parse the
    /// `pg_proc.proargdefaults` default-expression list. `Err` carries the
    /// catalog/parse `ereport(ERROR)`. Owner: pg_proc / parser.
    pub fn fetch_function_defaults<'mcx>(mcx: mcx::Mcx<'mcx>, funcid: Oid) -> PgResult<alloc::vec::Vec<Expr<'mcx>>>
);

seam_core::seam!(
    /// `recheck_cast_function_args` (clauses.c): re-run
    /// `enforce_generic_type_consistency` + `make_fn_arguments` over the
    /// (reordered / defaulted) argument list, adding casts as needed.
    /// `proargtypes` / `prorettype` come from the pg_proc row. Returns the
    /// possibly-cast argument list. `Err` carries the type-resolution
    /// `ereport(ERROR)`. Owner: parser (parse_coerce).
    pub fn recheck_cast_function_args(
        args: alloc::vec::Vec<Expr<'static>>,
        result_type: Oid,
        proargtypes: alloc::vec::Vec<Oid>,
        prorettype: Oid,
    ) -> PgResult<alloc::vec::Vec<Expr<'static>>>
);

seam_core::seam!(
    /// `rowtype_field_matches` non-RECORD leg (clauses.c:2176): consult the
    /// composite type's tuple descriptor and report whether field `fieldnum`
    /// has the expected type/typmod/collation (and is not dropped). `Err`
    /// carries the typcache lookup `ereport(ERROR)`. Owner: typcache.
    pub fn rowtype_field_matches_lookup(
        rowtypeid: Oid,
        fieldnum: i32,
        expectedtype: Oid,
        expectedtypmod: i32,
        expectedcollation: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `DomainHasConstraints(typid)` (typcache.c): does the domain type have
    /// any CHECK / NOT NULL constraints? Used by the `CoerceToDomain` fold arm
    /// (a constraint-free domain coercion is a no-op RelabelType). `Err`
    /// carries the typcache lookup `ereport(ERROR)`. Owner: typcache.
    pub fn domain_has_constraints(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_function_rows(root, funcid, node)` (selfuncs.c): estimate the
    /// number of rows a set-returning function call produces, consulting its
    /// `pg_proc.prorows` / a support function. Used by
    /// `expression_returns_set_rows`. `Err` carries the catalog/support
    /// `ereport(ERROR)`. Owner: selfuncs / per-function support.
    pub fn get_function_rows<'mcx>(funcid: Oid, node: &Expr<'mcx>) -> PgResult<f64>
);

seam_core::seam!(
    /// `lookup_type_cache(typid, TYPECACHE_CMP_PROC)->cmp_proc` (typcache.c):
    /// the default btree comparison function OID for `typid` (`InvalidOid` if
    /// none). Used by `contain_leaked_vars`' `MinMaxExpr` arm (a MinMaxExpr is
    /// leakproof iff its comparison function is). `Err` carries the typcache
    /// lookup `ereport(ERROR)`. Owner: typcache.
    pub fn type_cmp_proc(typid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `to_json_is_immutable(typoid)` (jsonfuncs / json.c): is the cast of
    /// `typoid` to `json` immutable? Used by `contain_mutable_functions`'
    /// `JsonConstructorExpr` arm. `Err` carries the catalog `ereport(ERROR)`.
    /// Owner: json adt.
    pub fn to_json_is_immutable(typoid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `to_jsonb_is_immutable(typoid)` (jsonb.c): is the cast of `typoid` to
    /// `jsonb` immutable? Used by `contain_mutable_functions`'
    /// `JsonConstructorExpr` arm. `Err` carries the catalog `ereport(ERROR)`.
    /// Owner: jsonb adt.
    pub fn to_jsonb_is_immutable(typoid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `jspIsMutable(jsonpath, varnames, varexprs)` (jsonpath_exec.c): is the
    /// jsonpath in a `JsonExpr` mutable (references `@`-vars / non-immutable
    /// items)? Used by `contain_mutable_functions`' `JsonExpr` arm. `Err`
    /// carries the catalog `ereport(ERROR)`. Owner: jsonpath adt.
    pub fn jsp_is_mutable<'mcx>(jsonexpr: &Expr<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `getSubscriptingRoutines(refcontainertype)->sbs_fetch_strict-equivalent`
    /// — the subscripting support method's strict flag for a container type's
    /// FETCH (clauses.c `contain_nonstrict_functions` `SubscriptingRef` arm).
    /// `None` if the container type has no subscripting routines. `Err` carries
    /// the catalog `ereport(ERROR)`. Owner: per-type subscripting support.
    pub fn subscripting_fetch_strict(refcontainertype: Oid) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// The subscripting support method's `(fetch_leakproof, store_leakproof)`
    /// flags for a container type (clauses.c `contain_leaked_vars`
    /// `SubscriptingRef` arm). `None` if the container type has no subscripting
    /// routines (treated as not leakproof). `Err` carries the catalog
    /// `ereport(ERROR)`. Owner: per-type subscripting support.
    pub fn subscripting_leakproof(refcontainertype: Oid) -> PgResult<Option<(bool, bool)>>
);
