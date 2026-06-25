# Audit: backend-optimizer-util-clauses (optimizer/util/clauses.c)

Independent function-by-function audit re-derived from `postgres-18.3/src/backend/optimizer/util/clauses.c`
and `c2rust-runs/backend-optimizer-util-clauses/src/clauses.rs`, compared against the port at
`crates/backend-optimizer-util-clauses/src/` (modules: lib, grounded, fold, leaf, deferred, srf_inline).

NOTE: the port being audited lives in worktree `agent-a2f9449bbe6bb5336`; this audit file is written in
the auditor's isolated worktree `agent-a8435fb2c51489390`.

## Verdict: PASS

All 66 enumerated clauses.c functions are MATCH or legitimately SEAMED. Zero seam findings. No
own-logic was dropped or pushed across a seam. All four gates pass. CATALOG.tsv row set to `audited`.

## Model recap (faithful, not stub)

The executable-expression node is the lifetime-free `types_nodes::primnodes::Expr` enum (NOT `Node`).
There is no `Query`/`List`/`RestrictInfo`/`PathTarget`/`RangeTblEntry` arm constructible in this model,
so the C `query_tree_walker` recursion arms and the RestrictInfo/PathTarget hasvolatile-cache
look-through arms are unreachable for trees this model builds (correctly omitted). The walker
`bool (*)(Node*, void*)` becomes a named `fn(Option<&Expr>, ctx) -> PgResult<bool>`; the fallible
PgResult is faithful because the catalog reads (lsyscache) can ereport on cache miss.

## Per-function table

| # | C fn | C line | port location | verdict | notes |
|---|------|--------|---------------|---------|-------|
| 1 | contain_agg_clause | 179 | grounded:106 | MATCH | |
| 2 | contain_agg_clause_walker | 185 | grounded:110 | MATCH | Aggref/GroupingFunc -> true; Asserts dropped (debug-only) |
| 3 | contain_window_function | 216 | grounded:127 | MATCH | |
| 4 | find_window_functions | 229 | grounded:148 | MATCH | preallocs maxWinRef+1 lists |
| 5 | find_window_functions_walker | 241 | grounded:166 | MATCH | out-of-range winref elog; no recurse into matched WindowFunc |
| 6 | expression_returns_set_rows | 287 | grounded:197 | MATCH | FuncExpr.funcretset / OpExpr.opretset -> get_function_rows seam + clamp_row_est |
| 7 | contain_subplans | 328 | grounded:225 | MATCH | + contain_subplans_slice installed seam body |
| 8 | contain_subplans_walker | 334 | grounded:249 | MATCH | SubPlan/AlternativeSubPlan/SubLink -> true |
| 9 | contain_mutable_functions | 368 | grounded:262 | MATCH | |
| 10 | contain_mutable_functions_checker | 374 | grounded:266 | MATCH | func_volatile != 'i' |
| 11 | contain_mutable_functions_walker | 380 | grounded:270 | MATCH | JsonConstructorExpr/JsonExpr/SQLValueFunction/NextValueExpr arms faithful; jsp_is_mutable passed the whole node |
| 12 | contain_mutable_functions_after_planning | 488 | deferred:15 | MATCH | expression_planner == eval_const_expressions then predicate |
| 13 | contain_volatile_functions | 536 | grounded:355 | MATCH | |
| 14 | contain_volatile_functions_checker | 542 | grounded:359 | MATCH | func_volatile == 'v' |
| 15 | contain_volatile_functions_walker | 548 | grounded:363 | MATCH | NextValueExpr -> true; RestrictInfo/PathTarget/Query arms unreachable in model |
| 16 | contain_volatile_functions_after_planning | 657 | deferred:22 | MATCH | |
| 17 | contain_volatile_functions_not_nextval | 671 | grounded:395 | MATCH | |
| 18 | contain_volatile_functions_not_nextval_checker | 677 | grounded:399 | MATCH | funcid != F_NEXTVAL(1574) && 'v' |
| 19 | contain_volatile_functions_not_nextval_walker | 684 | grounded:403 | MATCH | |
| 20 | max_parallel_hazard | 732 | — | SEAMED (omitted entry) | takes Query (no walkable Query in model; no merged consumer). Expr-side machinery fully ported |
| 21 | is_parallel_safe | 751 | grounded:442 | MATCH | glob.maxParallelHazard / param-empty / safe_param_ids supplied by caller (planner wiring not threaded) |
| 22 | max_parallel_hazard_test | 792 | grounded:467 | MATCH | safe/restricted/unsafe ladder + unrecognized -> elog |
| 23 | max_parallel_hazard_checker | 820 | grounded:494 | MATCH | func_parallel |
| 24 | max_parallel_hazard_walker | 827 | grounded:508 | MATCH | CoerceToDomain/NextValueExpr/WindowFunc/SubLink/SubPlan(paramIds save+restore)/Param(EXEC) arms faithful |
| 25 | contain_nonstrict_functions | 991 | grounded:601 | MATCH | |
| 26 | contain_nonstrict_functions_checker | 997 | grounded:605 | MATCH | !func_strict |
| 27 | contain_nonstrict_functions_walker | 1003 | grounded:609 | MATCH | full node-tag switch; SubscriptingRef fetch_strict via seam; CoerceViaIO/ArrayCoerce recurse into arg only |
| 28 | contain_exec_param | 1137 | grounded:676 | MATCH | |
| 29 | contain_exec_param_walker | 1143 | grounded:680 | MATCH | PARAM_EXEC && paramid in list |
| 30 | contain_context_dependent_node | 1179 | grounded:697 | MATCH | flags=0 init |
| 31 | contain_context_dependent_node_walker | 1189 | grounded:702 | MATCH | CaseTestExpr/CaseExpr(arg)/ArrayCoerceExpr(elemexpr) flag save/restore faithful |
| 32 | contain_leaked_vars | 1263 | grounded:738 | MATCH | |
| 33 | contain_leaked_vars_checker | 1269 | grounded:742 | MATCH | !get_func_leakproof |
| 34 | contain_leaked_vars_walker | 1275 | grounded:746 | MATCH | full switch incl. SubscriptingRef/RowCompareExpr(forthree)/MinMaxExpr(type_cmp_proc)/CurrentOfExpr/default-leaky |
| 35 | find_nonnullable_rels | 1457 | grounded:867 | MATCH | result in mcx |
| 36 | find_nonnullable_rels_walker | 1463 | grounded:874 | MATCH | Var/Bool(AND top vs intersect)/Func/Op/SAOP/coercions/NullTest/BooleanTest/SubPlan/PHV(phrels singleton) faithful; ExprRelids->bms bridge |
| 37 | find_nonnullable_vars | 1708 | grounded:1035 | MATCH | multibitmapset |
| 38 | find_nonnullable_vars_walker | 1714 | grounded:1042 | MATCH | mirrors rels walker; varattno offset by FirstLowInvalidHeapAttributeNumber |
| 39 | find_forced_null_vars | 1917 | grounded:1177 | MATCH | single-clause subroutine + AND-list arm |
| 40 | find_forced_null_var | 1978 | grounded:1215 | MATCH | NullTest IS_NULL !argisrow / BooleanTest IS_UNKNOWN over level-0 Var |
| 41 | is_strict_saop | 2026 | grounded:1248 | MATCH | set_sa_opfuncid + func_strict; ANY&falseOK short-circuit; non-empty array proof via array_const_nitems / ArrayExpr |
| 42 | is_pseudo_constant_clause | 2089 | grounded:1296 | MATCH | contain_var_clause then contain_volatile_functions |
| 43 | is_pseudo_constant_clause_relids | 2108 | grounded:1309 | MATCH | bms_is_empty(relids) |
| 44 | NumRelids | 2131 | grounded:1327 (num_relids) | SEAMED + naming | needs PlannerInfo.outer_join_rels; rides var/planner num_relids seam. Renamed lower snake (C public name NumRelids) — faithfulness/naming note, not a logic fault |
| 45 | CommuteOpExpr | 2148 | grounded:1336 | MATCH | binary-arity check, get_commutator, in-place swap, opfuncid=Invalid |
| 46 | rowtype_field_matches | 2187 | fold:981 | MATCH | RECORDOID short-circuit in-crate; tupdesc probe via rowtype_field_matches_lookup seam |
| 47 | eval_const_expressions | 2255 | fold:109 | MATCH | estimate=false; root/boundParams absent == C root==NULL |
| 48 | convert_saop_to_hashed_saop | 2288 | grounded:1365 | MATCH | in-place via mutator (take/transform/put) |
| 49 | convert_saop_to_hashed_saop_walker | 2294 | grounded:1380/1408 | MATCH | useOr (get_op_hash_functions) / !useOr (negator) ; nitems>=9; hashfuncid/negfuncid set |
| 50 | estimate_expression_value | 2396 | fold:122 | MATCH | estimate=true; no boundParams -> no Param substitution (faithful to model) |
| 51 | eval_const_expressions_mutator | 2441 | fold:215/229 | MATCH | full arm cluster; T_Param copies (no boundParams); depth guard stands in for check_stack_depth |
| 52 | contain_non_const_walker | 3739 | fold:185 (ece_all_arguments_const) | MATCH | Const keep / else abort |
| 53 | ece_function_is_safe | 3755 | fold:191 | MATCH | immutable, or estimate&&stable |
| 54 | simplify_or_arguments | 3793 | fold:577 | MATCH | flatten nested OR pre+post mutate; const TRUE->force_true, NULL->have_null, drop FALSE |
| 55 | simplify_and_arguments | 3899 | fold:616 | MATCH | flatten nested AND; const FALSE->force_false, NULL->have_null, drop TRUE |
| 56 | simplify_boolean_equality | 3993 | fold:656 | MATCH | left/right const cases, negate_clause via seam; returns Ok/Err(args) |
| 57 | simplify_function | 4062 | fold:1135 | MATCH | get_func_form; expand+mutate args; evaluate_function; support (call_support_simplify); inline_function |
| 58 | expand_function_arguments | 4178 | fold:1196 | MATCH | include_out_arguments=false; named->reorder; short->add_defaults; then recheck_cast |
| 59 | reorder_function_arguments | 4259 | fold:1216 | MATCH | argarray fill, named-position, defaults from fetch_function_defaults, missing-arg error |
| 60 | add_function_defaults | 4329 | fold:1270 | MATCH | ndelete computation, drain, append |
| 61 | fetch_function_defaults | 4353 | (seam) | SEAMED | proargdefaults parse rides clauses_seam::fetch_function_defaults (parser/pg_proc owner) |
| 62 | recheck_cast_function_args | 4383 | fold:1294 | MATCH (partial-seam) | FUNC_MAX_ARGS/pronargs checks in-crate; enforce_generic_type_consistency+make_fn_arguments ride seam (parser owner) |
| 63 | evaluate_function | 4428 | fold:1312 | MATCH | proretset/RECORD reject; strict+null->NULL const; all-const+immutable(or estimate&&stable) -> fmgr_fold; else None |
| 64 | inline_function | 4554 | fold:1376 | MATCH (gates in-crate, body seamed) | SQL-lang/secdef/retset/RECORD/proconfig/nargs gates in-crate; prosrc parse+rewrite via inline_sql_function seam |
| 65 | substitute_actual_parameters(_mutator) | 4910/4923 | (inside seam) | SEAMED | only reached by inline_function SQL body, which rides the seam (owner does the Param substitution) |
| 66 | sql_inline_error_callback | 4951 | (owner) | SEAMED | error-context callback for the seamed parse path |
| 67 | evaluate_expr | 4975 | fold:1416 | MATCH (in-crate fmgr fast path + seamed executor fallback) | fix_opfuncids + makeConst reconstruction in-crate; fmgr invoke seamed; non-fast-path shapes loud-defer to evaluate_expr_fallback |
| 68 | inline_set_returning_function | 5067 | srf_inline:34 | SEAMED | input is RangeTblEntry (parser node universe, NodePtr handles; not walkable Expr); no merged consumer. See judgment note below |
| 69 | substitute_actual_srf_parameters(_mutator) | 5360/5375 | (inside seam) | SEAMED | operate on Query (unmodeled); part of the SRF-inline core behind the seam |
| 70 | pull_paramids | 5420 | grounded:1457 | MATCH | bms result in mcx |
| 71 | pull_paramids_walker | 5430 | grounded:1470 | MATCH | Param -> bms_add_member(paramid) |
| 72 | make_SAOP_expr | 5452 | fold:1578 | MATCH | get_array_type or None; ArrayExpr operand, all-const folds via evaluate_expr; useOr=true SAOP |

Note: `estimate_array_length` (prompt asked to verify) is NOT in clauses.c in PG18 — it lives in
selfuncs.c. The port keeps it in `leaf.rs` (with clamp_row_est) as a self-contained helper; faithful
to its real source (the no-stats DECHIST fall-through default of 10 is the genuine empty-pg_statistic
result). Not a clauses.c function, so not counted in the 66 but noted as correct.

## Judgment calls (scrutinized)

1. **inline_set_returning_function — legitimately SEAMED.** The entire C input is a `RangeTblEntry`
   (`rtekind`/`funcordinality`/`functions`), a parser node not constructible in the `Expr` model; the
   single `FuncExpr` is read out of `rtfunc->funcexpr`, which in this repo is a `NodePtr` handle into
   the unported parser node universe. Although the gate ladder's *predicates* (contain_volatile_functions
   / contain_subplans over fexpr->args) would run on `Expr`, there is no walkable `FuncExpr` to feed them
   here, and the substitute_actual_srf_parameters core operates on `Query`. No merged consumer calls this
   entry. This is genuinely cross-boundary-unwalkable, not own-logic pushed away. Even src-idiomatic left
   it unported. The seam panics loudly until the planner/parser SRF-inliner owner lands. ACCEPT.

2. **max_parallel_hazard(Query) top-level entry — omitted, acceptable.** No walkable Query; the entire
   Expr-side machinery (is_parallel_safe, max_parallel_hazard_walker/test/checker, SubPlan paramId
   save/restore, Param EXEC handling) is fully ported and verified. No merged consumer needs the Query
   driver. ACCEPT.

3. **evaluate_expr / evaluate_function fmgr execution — legitimately SEAMED, surrounding own-logic kept
   in-crate.** The executor invocation (FunctionCallInvoke) is genuinely unported. The port keeps the
   surrounding own-logic in-crate: argument const-ness analysis, strict+NULL folding, immutable/stable
   gating, fix_opfuncids, the get_typlenbyval + makeConst result reconstruction, and the NULLIF
   evaluate-then-pick logic. The in-crate fast path (Const / all-Const FuncExpr/OpExpr/NullIfExpr) mirrors
   the real call shape via `fmgr_call`; shapes it cannot handle loud-defer to `evaluate_expr_fallback`
   (never silently returned unsimplified — C does fold them). ACCEPT.

4. **NumRelids -> num_relids naming.** Renamed to lower snake; the C public symbol is `NumRelids`. The
   body genuinely needs `root->outer_join_rels` (PlannerInfo), so the whole routine rides the var/planner
   `num_relids` seam — legitimately SEAMED. Naming is a faithfulness note, not a logic fault.

## Seam / wiring audit

- Inward seam OWNED + INSTALLED: `backend-optimizer-util-clauses-seams::contain_subplans(&[Expr])->bool`
  is set in `init_seams()` (lib.rs:89) via `grounded::contain_subplans_slice`. `seams-init/src/lib.rs:141`
  calls `backend_optimizer_util_clauses::init_seams()`. The two recurrence guards
  (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`) both PASS.
- Outward seams (get_func_form, fmgr_call, evaluate_expr_fallback, inline_sql_function,
  inline_set_returning_function_core, call_support_simplify, fetch_function_defaults,
  recheck_cast_function_args, rowtype_field_matches_lookup, domain_has_constraints, get_function_rows,
  type_cmp_proc, to_json[b]_is_immutable, jsp_is_mutable, subscripting_fetch_strict,
  subscripting_leakproof) are DECLARED here and installed by their real owners — correct per design (this
  crate is the consumer, not the owner).
- Seam call sites are thin marshal+delegate. The surrounding branching/node-construction (argument
  expansion, const checks, makeConst reconstruction, gate ladders) lives in-crate, not across a seam.
  No function body was wholesale relocated behind a seam except inline_set_returning_function (justified
  above as cross-boundary-unwalkable).

## Design conformance

- Allocating fns take `Mcx` and return `PgResult` (find_nonnullable_rels/vars, find_forced_null_vars,
  pull_paramids, eval_const_expressions, estimate_expression_value, evaluate_expr, make_SAOP_expr,
  CommuteOpExpr-in-place). PgResult propagated faithfully (C ereports on cache miss).
- No invented opacity: typed nodes are the real `Expr` enum; the only handle-shaped residue
  (RangeTblEntry/RangeTblFunction in srf_inline, NodeId clause in costsize-family helpers) is INHERITED
  opacity for the unported parser/planner node universe, not introduced here.
- No shared statics for per-backend globals; case_val/estimate/depth live in the EceContext value.
- Recursion guard (MAX_FOLD_DEPTH=4096) stands in for check_stack_depth — faithful intent (raises
  "stack depth limit exceeded" rather than abort).

## Constants verified against PG18 source (.dat, not memory)

F_NEXTVAL=1574 (pg_proc.dat), BooleanEqualOperator=91, BooleanNotEqualOperator=85 (pg_operator.dat),
INT4OID=23, OIDOID=26, RECORDOID=2249, CSTRINGOID=2275, BOOLOID=16 (pg_type.dat),
MIN_ARRAY_SIZE_FOR_HASHED_SAOP=9, FUNC_MAX_ARGS=100, PROVOLATILE_{IMMUTABLE='i',STABLE='s',VOLATILE='v'},
PROPARALLEL_{SAFE='s',RESTRICTED='r',UNSAFE='u'}, CCDN_CASETESTEXPR_OK=0x0001 — all correct.

## Gate results (CARGO_TARGET_DIR=/tmp/clauses-audit-target, CARGO_INCREMENTAL=0)

- `cargo check -p backend-optimizer-util-clauses` — PASS (Finished dev profile).
- `cargo test -p no-todo-guard` — PASS (report_todo_count ok).
- `cargo test -p seams-init` — PASS (2 recurrence-guard tests ok).
- `cargo test -p backend-optimizer-util-clauses` — PASS (7 tests ok).

## Fixes applied

None. The port is a faithful 1:1 transcription; no own-logic gaps were found.
