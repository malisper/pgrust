# Audit: backend-parser-agg (src/backend/parser/parse_agg.c)

Audited 2026-06-15 (Opus 4.8 [1m]). Verdict: **PASS**.

C source: /Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/parser/parse_agg.c (2338 LOC).
Crate: crates/backend-parser-agg. Every C function enumerated and checked against the C; no reasoning by category.

## Constants verified against PG headers (no transcription corruption)
- FUNC_MAX_ARGS = 100 (pg_config_manual.h:43) ✓
- IsPolymorphicType OIDs {2283,2277,2776,3500,3831,4537 | 5077,5078,5079,5080,4538} == ANYELEMENT/ANYARRAY/ANYNONARRAY/ANYENUM/ANYRANGE/ANYMULTIRANGE | ANYCOMPATIBLE/…ARRAY/…NONARRAY/…RANGE/…MULTIRANGE (cross-checked vs the repo's audited type-OID constants + pg_type.h IsPolymorphicTypeFamily1/2) ✓
- FRAMEOPTION_RANGE 0x2, START_UNBOUNDED_PRECEDING 0x20, END_CURRENT_ROW 0x400 → FRAMEOPTION_DEFAULTS 0x422 (parsenodes.h) ✓
- QTW_EXAMINE_RTES_BEFORE 0x10 (nodeFuncs.h:27) ✓
- BYTEAOID 17, RECORDOID 2249, INTERNALOID 2281, AGGKIND_NORMAL 'n', COERCE_EXPLICIT_CALL ✓
- All ereport SQLSTATEs: ERRCODE_GROUPING_ERROR, _WINDOWING_ERROR, _FEATURE_NOT_SUPPORTED, _UNDEFINED_FUNCTION, _UNDEFINED_OBJECT, _TOO_MANY_ARGUMENTS, _STATEMENT_TOO_COMPLEX, _INVALID_RECURSION — match C call-for-call ✓
- All error-message strings (the two giant agg/grouping ParseExprKind switches + the window switch) verified branch-by-branch identical to C ✓

## Per-function

| C function | Port | Verdict |
|---|---|---|
| transformAggregateCall | transformAggregateCall | MATCH — ordered-set split (list_copy_tail/list_truncate via split_off; forboth aargs/aggorder → make_target_entry + addTargetToSortList(tle_idx) loop), regular path (tlist build, p_next_resno save/set/restore, transformSortClause(EXPR_KIND_ORDER_BY, SQL99), transformDistinctClause + sortop-undefined error w/ format_type_be), aggargtypes build skipping resjunk, agg.args/aggorder/aggdistinct set, check_agglevels_and_constraints tail call. Uses real backend_parser_clause owners. |
| transformGroupingFunc | transformGroupingFunc | MATCH — >31-arg cap (TOO_MANY_ARGUMENTS), transformExpr(p_expr_kind) per arg via origin's transformExpr seam, result.args/location set, check_agglevels_and_constraints. |
| check_agglevels_and_constraints | check_agglevels_and_constraints | MATCH — isAgg dispatch, check_agg_arguments → *p_levelsup, parentParseState level walk (defensive Err on short chain vs C UB; invariant-safe), full ParseExprKind switch (err/errkind, agg-vs-grouping text), ParseExprKindName for errkind path. No default arm (C relies on -Wswitch; Rust match is exhaustive). |
| check_agg_arguments | check_agg_arguments | MATCH — context init, walk args then filter, min level computation (var/agg Min), same-level nested-agg error (locate_agg_of_level fallback to filter), non-local CTE check, direct-args recheck (var<level / agg<=level / cte<level errors). |
| check_agg_arguments_walker | check_agg_arguments_walker (closure body) | MATCH — Var/Aggref/GroupingFunc levelsup-sublevels_up tracking; SRF (FuncExpr.funcretset/OpExpr.opretset) + WindowFunc rejection at sublevels_up==0; RTE_CTE ctelevelsup + min_cte; Query recursion via query_tree_walker(QTW_EXAMINE_RTES_BEFORE); else expression_tree_walker. Fallible-walk pattern stashes PgError + aborts; error order preserved. |
| transformWindowFuncCall | transformWindowFuncCall | MATCH — nested-windowfunc check (p_hasWindowFuncs && contain_windowfuncs → locate_windowfunc errpos), full window-kind ParseExprKind switch, named-window lookup (winref scan, "window does not exist"), anonymous-window de-dup (refname match + equal(partition/order/start/end) + frameOptions), append to p_windowdefs, p_hasWindowFuncs=true. wfunc.location read (field present since keystone #219). |
| parseCheckAggregates | parseCheckAggregates | MATCH — assert precondition; groupingSets expand (limit 4096 → STATEMENT_TOO_COMPLEX, gset_common intersection seeded by linitial, single-set ditch); join/selfref RTE scan; groupClauses from get_sortgroupclause_tle; flatten_join_alias_vars when hasJoinRTEs (seam); have_non_var_grouping + groupClauseCommonVars (gset membership); addRangeTableEntryForGroup + hasGroupRTE; finalize_grouping_exprs then substitute_grouped_columns for targetList and havingQual (each with flatten when join); recursive-term agg error (locate_agg_of_level). |
| substitute_grouped_columns | substitute_grouped_columns | MATCH — context seed + mutator entry. |
| substitute_grouped_columns_mutator | substitute_grouped_columns_mutator | MATCH — same-level Aggref copyObject + in_agg_direct_args recurse direct args only; higher-level agg skip; GroupingFunc skip>=level; have_non_var_grouping whole-subexpr equal match → buildGroupedVar; Const/Param accept; ungrouped-Var path (match scan, func-dep via check_functional_grouping + constraintDeps + func_grouped_rels memo, two ungrouped error texts incl in_agg_direct_args detail); Query recursion via query_tree_mutator; else expression_tree mutator (in-place per repo model). |
| finalize_grouping_exprs | finalize_grouping_exprs | MATCH. |
| finalize_grouping_exprs_walker | finalize_grouping_exprs_walker | MATCH — Const/Param skip; same-level Aggref direct-args-only; GroupingFunc at exact level: per-arg flatten (when join) + Var match / have_non_var_grouping equal match → ressortgroupref, ref==0 error, grp.refs filled; Query recursion; else expression_tree_walker. |
| buildGroupedVar | buildGroupedVar | MATCH — p_grouping_nsitem nscol = p_nscolumns[attnum-1], makeVar(nscol fields, sublevels_up), varnosyn/varattnosyn by hand, varnullingrels add when groupingSets && !gset_common member. |
| expand_groupingset_node | expand_groupingset_node | MATCH — EMPTY (list_make1(NIL)), SIMPLE (content), ROLLUP (descending prefix groups + trailing NIL), CUBE (2^n bitmask powerset), SETS (recursive concat). |
| cmp_list_len_asc | cmp_list_len_asc | MATCH (pg_cmp_s32 = i32::cmp on lengths). |
| cmp_list_len_contents_asc | cmp_list_len_contents_asc | MATCH (len then elementwise). |
| expand_grouping_sets | expand_grouping_sets | MATCH — per-set expand + numsets product limit, cartesian product with list_union_int dedup-per-set, sort: !groupDistinct||<2 → cmp_list_len_asc; else per-set sort + cmp_list_len_contents_asc + adjacent-equal dedup. |
| get_aggregate_argtypes | get_aggregate_argtypes | MATCH — copy aggargtypes into inputTypes, return count (asserts <= FUNC_MAX_ARGS). |
| resolve_aggregate_transtype | resolve_aggregate_transtype | MATCH — polymorphic → get_func_signature + enforce_generic_type_consistency. DEVIATION: gained mcx param (C takes none) because get_func_signature allocates — required by the no-ambient-context rule. Behaviour identical. |
| agg_args_support_sendreceive | agg_args_support_sendreceive | MATCH — RECORD disclaim; per non-byval type require typsend & typreceive (via lsyscache get_type_sendreceive_byval seam, replacing the raw SearchSysCache1(TYPEOID)+GETSTRUCT — owner-installed, faithful to the Form_pg_type read). |
| build_aggregate_transfn_expr | build_aggregate_transfn_expr | MATCH — state arg + agg_num_direct_inputs..agg_num_inputs args, makeFuncExpr funcvariadic, optional invtransfn. |
| build_aggregate_serialfn_expr | build_aggregate_serialfn_expr | MATCH (INTERNAL→BYTEA). |
| build_aggregate_deserialfn_expr | build_aggregate_deserialfn_expr | MATCH (BYTEA,INTERNAL→INTERNAL). |
| build_aggregate_finalfn_expr | build_aggregate_finalfn_expr | MATCH (state + num_finalfn_inputs-1 input args). |
| make_agg_arg | make_agg_arg | MATCH — PARAM_EXEC Param, paramid -1, typmod -1, location -1. |
| check_agg_arguments_context / substitute_grouped_columns_context | Rust structs / closure-captured state | MATCH — fields 1:1; C function-ptr+context → Rust closure + struct + stashed-error. |

## Seam-and-panic legs (genuinely unported owners)
- flatten_join_alias_vars → backend-rewrite-rewritemanip-seams (rewriteManip.c unported; shared with the clause lane; panic-until-owner; guard-exempt, no owner dir). Reached in parseCheckAggregates + finalize_grouping_exprs_walker when hasJoinRTEs. This is the only outward unported edge; everything else calls landed owners directly.

## residual_own_todos = 0
No todo!()/unimplemented!() in own logic (no-todo-guard PASS). Panics are either mirror-PG Assert/elog or seam-not-installed for the one unported owner above.

## Confined unsafe
tlist_into_static: a lifetime-only transmute 'mcx→'static erasing make_target_entry's arena lifetime to enter the lifetime-free Expr tree (primnodes::Aggref.args: Vec<TargetEntry<'static>>, matching SubPlanExpr(Box<SubPlan<'static>>)). Data unchanged. The sound fix (Aggref-lifetime keystone over execExpr/nodeAgg) is unbuilt and out of scope; flagged as a known model gap. No other unsafe.

## Gate
cargo check --workspace PASS; no-todo-guard PASS; seams-init PASS (every declared seam installed; rewritemanip flatten panics-until-owner, guard-exempt); cargo test --workspace PASS except sanctioned range_pair_* flake (backend-optimizer-path-small, untouched).
