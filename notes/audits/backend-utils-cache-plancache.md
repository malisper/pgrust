# Audit — backend-utils-cache-plancache

- **Verdict: PASS**
- Date: 2026-06-12
- Model: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Branch: `port/backend-utils-cache-plancache`

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Re-derived from sources; the port's comments/self-review were not trusted.

Sources:
- C: `../pgrust/postgres-18.3/src/backend/utils/cache/plancache.c` (2359 lines)
  + `src/include/utils/plancache.h`.
- c2rust: `../pgrust/c2rust-runs/backend-utils-cache-plancache/src/plancache.rs`.
- Port: `crates/backend-utils-cache-plancache/src/lib.rs` (1945 lines);
  vocabulary `crates/types-plancache`.

## 1. Function inventory & per-function verdicts

Every function defined in plancache.c (incl. statics, inline helpers, the
ResourceOwner descriptor). 42 functions + 1 static descriptor.

| # | C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|------------|-------|-------------------|---------|-------|
| 1 | `ResourceOwnerRememberPlanCacheRef` (inline) | 125 | folded into `GetCachedPlan`/`CachedPlan*Valid` via `resowner_seams::resource_owner_remember_plan` | MATCH | inline wrapper; Enlarge+refcount+++Remember sequence preserved at each call site |
| 2 | `ResourceOwnerForgetPlanCacheRef` (inline) | 130 | folded into `ReleaseCachedPlan` via `resource_owner_forget_plan` | MATCH | inline wrapper |
| 3 | `InitPlanCache` | 146 | 259 | MATCH | exactly 1 relcache + 7 syscache callbacks; cache ids (PROCOID/TYPEOID→object cb; NAMESPACEOID/OPEROID/AMOPOPID/FOREIGNSERVEROID/FOREIGNDATAWRAPPEROID→sys cb) match, same order |
| 4 | `CreateCachedPlan` | 183 | 324 | MATCH | source_context child of CurrentMemoryContext, SMALL sizes, copyObject(raw), pstrdup→String, identifier set; field inits via `new_source` (is_oneshot=false) |
| 5 | `CreateCachedPlanForQuery` | 263 | 353 | MATCH | delegates to CreateCachedPlan(NULL,…) then copies analyzed tree in source context |
| 6 | `CreateOneShotCachedPlan` | 298 | 369 | MATCH | palloc0 in CurrentMemoryContext, raw NOT copied, is_oneshot=true |
| 7 | `CompleteCachedPlan` | 391 | 393 | MATCH | oneshot/querytree_context-given/copy branches, reparent vs create, dep extraction gated on `!is_oneshot && StmtPlanRequiresRevalidation`, RLS/search_path save, param copy, resultDesc, is_complete/is_valid both set |
| 8 | `SetPostRewriteHook` | 505 | 476 | MATCH | sets postRewrite (+arg folded into PostRewriteHandle) |
| 9 | `SaveCachedPlan` | 530 | 492 | MATCH | one-shot→elog ERROR; ReleaseGenericPlan; reparent under CacheMemoryContext; push saved_plan_list; is_saved=true |
| 10 | `DropCachedPlan` | 574 | 520 | MATCH | unlink-if-saved, ReleaseGenericPlan, magic=0, MemoryContextDelete unless oneshot; registry removal models C freeing |
| 11 | `ReleaseGenericPlan` (static) | 602 | 552 | MATCH | gplan!=NULL → magic assert, clear gplan, ReleaseCachedPlan(plan,NULL) |
| 12 | `StmtPlanRequiresRevalidation` (static) | 624 | 568 | MATCH | raw→stmt_requires_parse_analysis; analyzed→query_requires_rewrite_plan; else false |
| 13 | `BuildingPlanRequiresSnapshot` (static) | 641 | 581 | MATCH | raw→analyze_requires_snapshot; analyzed→query_requires_rewrite_plan; else false |
| 14 | `RevalidateCachedQuery` (static) | 666 | 597 | MATCH | full flow: oneshot/no-reval early NIL; search_path mismatch invalidates src+gplan; RLS recheck; locked-recheck race w/ release; teardown of query_list/oids/items/search_path; query_context delete; ReleaseGenericPlan; snapshot push/pop; raw vs analyzed vs empty reparse; post-rewrite hook; resultDesc compare via `equal_row_types` w/ FEATURE_NOT_SUPPORTED on fixed_result; new query_context + dep extract + reparent; is_valid=true; returns tlist |
| 15 | `CheckCachedPlan` (static) | 935 | 774 | MATCH | no-gplan→false; role recheck; locked recheck incl saved_xmin/TransactionXmin transient check; release-on-race; ReleaseGenericPlan→false |
| 16 | `BuildCachedPlan` (static) | 1019 | 826 | MATCH | reval-if-invalid; qlist copy (oneshot vs not); snapshot gated on BuildingPlanRequiresSnapshot; pg_plan_queries; plan_context create+copy unless oneshot; planRoleId/dependsOnRole/is_transient loop (utility skip, transientPlan, dependsOnRole); saved_xmin set w/ TransactionIdIsNormal assert; refcount=0; generation=++src.generation |
| 17 | `choose_custom_plan` (static) | 1158 | 939 | MATCH | oneshot→true; no params→false; no-reval→false; FORCE_GENERIC/FORCE_CUSTOM; CURSOR_OPT flags; <5 custom→true; avg cost compare (generic_cost < avg → false) |
| 18 | `cached_plan_cost` (static) | 1215 | 991 | MATCH | sum planTree->total_cost over non-utility stmts; +1000.0*cpu_operator_cost*(nrel+1) when include_planner; nrelations = rtable length |
| 19 | `GetCachedPlan` | 1280 | 1017 | MATCH | owner-on-unsaved→elog ERROR; Revalidate; choose; generic path (CheckCachedPlan reuse vs Build+link+refcount+reparent saved/sibling+generic_cost+re-choose+qlist=NIL); custom path build+cost accumulate; counters; refcount+++resowner; saved custom reparent |
| 20 | `ReleaseCachedPlan` | 1403 | 1113 | MATCH | owner→is_saved assert + Forget; refcount--; on 0: magic=0, delete context unless oneshot; registry removal models free |
| 21 | `CachedPlanAllowsSimpleValidityCheck` | 1448 | 1144 | MATCH | asserts; oneshot→false; dependsOnRLS/dependsOnRole/saved_xmin rejects; query_list loop (utility/rtable/cteList/hasSubLinks rejects); stmt_list loop (utility reject, rtable RTE_RELATION reject); refcount bump if owner |
| 22 | `CachedPlanIsSimplyValid` | 1563 | 1216 | MATCH | dangling-safe checks (is_valid / plan!=NULL / plan==gplan / plan->is_valid) before deref; search_path match; refcount bump if owner. Port adds registry `contains_key` guard — the safe analogue of C's "don't deref dangling pointer" |
| 23 | `CachedPlanSetParentContext` | 1610 | 1256 | MATCH | saved→elog, oneshot→elog; reparent context; gplan reparent if present |
| 24 | `CopyCachedPlan` | 1648 | 1296 | MATCH | oneshot→elog; new source+querytree contexts; deep copies raw/analyzed/query_string/param_types/resultDesc/query_list/relationOids/invalItems/search_path; is_oneshot=false/is_complete=true/is_saved=false; copies cost knowledge & generation; gplan=NULL |
| 25 | `CachedPlanIsValid` | 1742 | 1441 | MATCH | returns is_valid |
| 26 | `CachedPlanGetTargetList` | 1755 | 1453 | MATCH | resultDesc==NULL→NIL; Revalidate; QueryListGetPrimaryStmt; FetchStatementTargetList |
| 27 | `GetCachedExpression` | 1792 | 1479 | MATCH | expression_planner_with_deps; cexpr_context SMALL_SIZES; copyObject(expr/relationOids/invalItems); reparent under CacheMemoryContext; push to list |
| 28 | `FreeCachedExpression` | 1849 | 1517 | MATCH | unlink from list; MemoryContextDelete; registry removal models free |
| 29 | `QueryListGetPrimaryStmt` (static) | 1868 | 1534 | MATCH | first canSetTag Query, else NULL |
| 30 | `AcquireExecutorLocks` (static) | 1887 | 1548 | MATCH | utility→UtilityContainsQuery+ScanQueryForLocks; else rtable loop, RTE_RELATION or (RTE_SUBQUERY && OidIsValid(relid)) → Lock/UnlockRelationOid(relid, rellockmode) |
| 31 | `AcquirePlannerLocks` (static) | 1943 | 1581 | MATCH | utility→UtilityContainsQuery+ScanQueryForLocks; else ScanQueryForLocks |
| 32 | `ScanQueryForLocks` (static) | 1968 | 1602 | MATCH | utility assert; rtable switch (RTE_RELATION lock; RTE_SUBQUERY view-lock if OidIsValid + recurse into subquery; default ignore); cteList recurse; hasSubLinks → walker results recursion |
| 33 | `ScanQueryWalker` (static) | 2034 | 1647 | SEAMED | body is opaque-tree walk (`expression_tree_walker`/`query_tree_walker` w/ QTW_IGNORE_RC_SUBQUERIES, no descent into Query). Delegated to `analyze_seams::walk_query_sublinks_for_locks`, which returns the SubLink subselects; recursion+lock acquisition stay in plancache's ScanQueryForLocks. See seam audit — acceptable under inherited opacity. |
| 34 | `PlanCacheComputeResultDesc` (static) | 2062 | 1656 | MATCH | ChoosePortalStrategy switch: ONE_SELECT/ONE_MOD_WITH→ExecCleanTypeFromTL(targetList of linitial); ONE_RETURNING→primary stmt returningList; UTIL_SELECT→UtilityTupleDescriptor; MULTI_QUERY→NULL |
| 35 | `PlanCacheRelCallback` (static) | 2098 | 1689 | MATCH | iterate saved; skip invalid/no-reval; querytree dep check (relid==Invalid? oids!=NIL : list_member_oid) → invalidate src+gplan; gplan stmt_list scan (utility skip, relationOids check, break); cached_expression_list same dep check |
| 36 | `PlanCacheObjectCallback` (static) | 2182 | 1776 | MATCH | iterate saved; invalItems scan (cacheId match, hashvalue==0||match) → invalidate+break; gplan stmt_list+invalItems scan w/ nested breaks (two-level break replicated via labeled `'stmt_scan`); cached expressions invalItems scan |
| 37 | `PlanCacheSysCallback` (static) | 2291 | 1867 | MATCH | ResetPlanCache() |
| 38 | `ResetPlanCache` | 2300 | 1876 | MATCH | iterate saved; skip invalid/no-reval (preserves txn-control stmts); invalidate src+gplan; invalidate all cached expressions unconditionally |
| 39 | `ReleaseAllPlanCacheRefsInOwner` | 2347 | 1918 | MATCH | ResourceOwnerReleaseAllOfKind modeled by `resource_owner_release_all_plan_refs` returning held plan ids, each re-entering ResOwnerReleaseCachedPlan→ReleaseCachedPlan(plan,NULL) — same net effect |
| 40 | `ResOwnerReleaseCachedPlan` (static) | 2355 | 1933 | MATCH | ReleaseCachedPlan(plan, NULL) |
| 41 | `planref_resowner_desc` (static const) | 115 | n/a | MATCH | descriptor (name/phase/priority/ReleaseResource) lives in the resowner owner; plancache references it only through remember/forget/release_all seams — correct, ResourceOwnerDesc registration is resowner's job |

Spot re-derivation (skill §closing): independently re-derived #14
(RevalidateCachedQuery), #19 (GetCachedPlan), #36 (PlanCacheObjectCallback's
double-break) and #18 (cost formula `1000.0 * cpu_operator_cost *
(nrelations + 1)`) line-by-line against the C — all hold.

### Constants verified against headers
- `num_custom_plans`/`num_generic_plans` = `int64` → port `i64`;
  `generic_cost`/`total_custom_cost` = `double` → `f64`; `generic_cost`
  init `-1` (plancache.h:143-146). MATCH.
- `RTE_RELATION=0`, `RTE_SUBQUERY=1` (types-plancache). MATCH.
- PLAN_CACHE_MODE_* / CURSOR_OPT_* / FirstNormalTransactionId / magic numbers
  sourced from `types-plancache`, used by value. MATCH.
- `equalRowTypes` (PG18, line 854) → port uses `equal_row_types` (not the
  pre-18 `equalTupleDescs`). MATCH.

## 2. Seam audit (step 3)

**Owned seam crates (by C-source coverage):** plancache.c maps to no inward
`*-seams` crate — there is no `backend-utils-cache-plancache-seams`, because no
ported neighbor calls *into* plancache across a cycle yet. The unit owns no
inward seam declarations, so its `init_seams()` being empty (`{}`) is correct,
not an empty-installer FAIL. Confirmed against the crate list.

**Outward seams** (owned by neighbors; plancache only calls them): all 15
`*-pc-seams` crates inspected. Each declaration is a thin field-read / copy /
list-op / single delegate:
- mcxt: context create/switch/setparent/delete/identifier — thin.
- copyfuncs (node): copy_*, *_elements, list_member_oid,
  extract_query_dependencies, expression_planner_with_deps — opaque-tree
  copies/walks owned by nodes/planner (inherited opacity). Thin.
- analyze: stmt_requires_parse_analysis, analyze_requires_snapshot,
  query_requires_rewrite_plan, analyze_and_rewrite_*, Query field reads
  (commandType/canSetTag/utilityStmt/rtable/cteList/hasSubLinks/targetList/
  returningList) — thin. `walk_query_sublinks_for_locks` — see below.
- rewriteHandler: acquire_rewrite_locks (bakes C's constant `true,false` args in
  the owner impl — acceptable), rewrite_query, invoke_post_rewrite — thin.
- planner: plan_queries + PlannedStmt field reads (commandType/transientPlan/
  dependsOnRole/planTree total_cost/rtable length+fields/relationOids/invalItems/
  utilityStmt) + cpu_operator_cost GUC — thin.
- pquery: choose_portal_strategy, exec_clean_type_from_tl,
  fetch_statement_target_list — thin.
- utility: utility_contains_query, utility_tuple_descriptor — thin.
- tupdesc: equal_row_types, create_tuple_desc_copy, free_tuple_desc — thin.
- snapmgr: active_snapshot_set, push/pop, transaction_xmin — thin.
- lmgr: lock/unlock_relation_oid — thin.
- namespace: get/copy_search_path_matcher,
  search_path_matches_current_environment — thin.
- inval: register relcache/syscache callbacks — thin.
- syscache: syscache_id mapping — thin.
- resowner: enlarge/remember/forget/release_all_plan_refs — thin.
- backendstate: get_user_id, row_security, plan_cache_mode — thin zero-arg
  getters (backend-global accessors; ledgered as design debt).

No outward seam contains plancache decision logic: every branch, loop, cost
formula, refcount step, invalidation predicate and control-flow path in the
table above lives in `crates/backend-utils-cache-plancache`.

### Seam observation (resolved, non-blocking): `walk_query_sublinks_for_locks`
`ScanQueryWalker`'s C body is purely opaque-node-tree traversal
(`expression_tree_walker`/`query_tree_walker`, nodes/nodeFuncs.c, unported and
opaque) carrying the plancache rule "on a `SubLink`, hand the subselect to
`ScanQueryForLocks`; never recurse into `Query`." The port delegates the
*traversal* to the owner's `walk_query_sublinks_for_locks`, which returns the
ordered set of `SubLink` subselect `Query*`s; plancache then performs the
`ScanQueryForLocks` recursion and all lock-acquisition decisions itself
(lib.rs:1635-1637). Because plancache structurally cannot walk an opaque node
tree (inherited opacity, types.md 6-7; "Mirror PG and panic"), and the surviving
plancache logic (recursion + locking) stays in-crate, this is a correct SEAMED
delegation, not MISSING logic. The seam is a pure enumerator with no branching or
locking of its own; the set of subselects (and thus lock behavior) is identical.
Noted, not blocking.

## 3b. Design conformance

- Opacity: all node/tupdesc/plan/search-path/resowner/snapshot handles are
  inherited opacity over unported owners (typed newtypes in `types-plancache`);
  no invented stand-ins. PASS (types.md 6-7).
- Allocating routines (`CreateCachedPlan`, `BuildCachedPlan`, `CopyCachedPlan`,
  `GetCachedExpression`, …) return `PgResult` and route allocation through the
  mcxt seam; no bare allocation. PASS.
- Backend-global lists (`saved_plan_list`, `cached_expression_list`) are
  `thread_local!` per-backend with owned `String`/`Vec` per mctx-design
  decision-5 backend-global exception (not shared statics). PASS.
- Locks: lmgr lock/unlock cross the seam as discrete calls; no lock object held
  across `?`. PASS.
- The `Rc<RefCell<…>>` registry models C's shared-mutable `CachedPlan*`/
  `CachedPlanSource*` aliasing (gplan + caller + ResourceOwner); the C refcount
  stays authoritative; entries are removed exactly when C frees (refcount→0 /
  DropCachedPlan / FreeCachedExpression). It is the faithful model of
  plancache's own allocations, not a registry-shaped side table. PASS.
- Error paths: `elog(ERROR,…)` → `PgError(ERROR,…)`; the one ereport
  (`cached plan must not change result type`) carries
  `ERRCODE_FEATURE_NOT_SUPPORTED`. SQLSTATE/severity match. PASS.
- Design debt (zero-arg backend-global getter seams; ambient memory-context
  model deferred to mctx-remainder owner) ledgered in DESIGN_DEBT.md per the
  catalog row. PASS.

## Verdict

**PASS.** All 42 functions + the resowner descriptor are MATCH or properly
SEAMED. The single seam observation (`walk_query_sublinks_for_locks`) is a
correct inherited-opacity delegation with the surviving logic in-crate, not a
blocking finding. `init_seams()` is correctly empty (no owned inward seam
crate). No MISSING / PARTIAL / DIVERGES.
