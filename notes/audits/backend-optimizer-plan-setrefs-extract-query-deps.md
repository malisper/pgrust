# Audit: backend-optimizer-plan-setrefs — extract_query_dependencies slice

Scope: the newly added VALUE dependency-extraction entry points, audited
independently against `optimizer/plan/setrefs.c` and `tcop/utility.c` (18.3).

## Function table

| C function (loc) | Port (loc) | Verdict | Notes |
|---|---|---|---|
| `record_plan_function_dependency` (setrefs.c:3554) | `record_plan_function_dependency_value` (lib.rs ~2877) | MATCH | `funcid >= FirstUnpinnedObjectId` guard; builds `(PROCOID, GetSysCacheHashValue1(PROCOID,funcid))`. Existing glob/NodeId form (lib.rs:706) unchanged and reused by set_plan_references. |
| `fix_expr_common` (setrefs.c:2030) | `fix_expr_common_value` (lib.rs ~2897) | MATCH | Aggref/WindowFunc/FuncExpr/OpExpr/DistinctExpr/NullIfExpr/ScalarArrayOpExpr(+hashfuncid/+negfuncid)/Const(regclass)/GroupingFunc. opfuncid resolved via `set_opfuncid`/`set_sa_opfuncid` on a clone (C scribbles in place; same OID read). GroupingFunc cols fixup is a no-op here (zeroed root → grouping_map NULL), matching C. |
| `extract_query_dependencies_walker` (setrefs.c:3671) | same name (lib.rs ~2935) | MATCH | CMD_UTILITY → CallStmt(funcexpr,outargs) / UtilityContainsQuery; `hasRowSecurity` → depends_on_role; rtable OID collection predicate `RTE_RELATION || (RTE_SUBQUERY && OidIsValid) || (RTE_NAMEDTUPLESTORE && OidIsValid)`; then `query_tree_walker(...,0)`. Non-Query → `fix_expr_common` + `expression_tree_walker`. Recursion + error-threading via the find_expr.rs closure idiom. |
| `extract_query_dependencies` (setrefs.c:3635) | `extract_query_dependencies_value` (lib.rs ~3060) | MATCH | C runs the walker on `(Node*) querytree_list` (a List → expression_tree_walker List arm visits each Query). Port walks each `Query` element as `Node::Query`, identical visitation. Dummy zeroed glob/root realized as the local `ExtractDepsCtx`. |
| `UtilityContainsQuery` (utility.c:2179) | `utility_contains_query` (lib.rs ~3035) | MATCH | Pure structural recursion over the 3 utility-stmt Node variants (all in types-nodes); mirrors the ported `UtilityContainsQuery`. No catalog/runtime dep → inlined, not seamed. |

## Constants (verified against C headers, not memory)

- `FirstUnpinnedObjectId = 12000` — access/transam.h:196. MATCH.
- `OIDOID = 26`, `REGCLASSOID = 2205` — pg_type_d.h. MATCH.
- `PROCOID = 47`, `TYPEOID = 82` — **BUG FOUND AND FIXED.** The pre-existing
  setrefs constants were `11` / `13` (wrong; only ever fed to the never-installed
  `record_inval_item` seam, so latent). `catalog/syscache_ids.h` (alphabetical
  generated enum) places PROCOID at ordinal 47 and TYPEOID at 82, matching
  `backend-utils-cache-syscache/cacheinfo.rs` (PROCOID=47, TYPEOID=82). The value
  path is the first to feed PROCOID to `GetSysCacheHashValue1`, which validates
  cacheId against `sys_cache_exists[]` and indexes the catcache — `11` would have
  hashed against the wrong cache (silent invalidation bug). Corrected to 47/82;
  this also repairs the latent bug in the existing `record_plan_function_dependency`.

## Seam audit

- Outward call `backend_utils_cache_syscache_seams::get_syscache_hash_value_oid`
  — justified: setrefs cannot depend on backend-utils-cache-syscache (catalog
  layer, would cycle); the seam is thin marshal+delegate to `GetSysCacheHashValue1`,
  no logic in the seam path. Declared in syscache-seams (its owner, who owns
  GetSysCacheHashValue1), installed by syscache `init_seams`. Generic over cache_id
  (mirrors the existing get_syscache_hash_value_constroid/typeoid projections).
- Inward seam `extract_query_dependencies_value` declared in
  backend-optimizer-plan-setrefs-seams (this unit's own seam crate), installed by
  this crate's `init_seams` (previously a no-op). `seams-init::init_all` calls it
  (seams-init:301). Recurrence guard `every_declared_seam_is_installed_by_its_owner`
  passes.
- The handle-based `backend_nodes_copyfuncs_pc_seams::extract_query_dependencies`
  (plancache F0) is a DIFFERENT seam, still uninstalled (allowlist seams-init:1638),
  left untouched — this lane is additive (STEP A).

## Verdict: PASS

Every audited function MATCH after the PROCOID/TYPEOID constant fix; zero seam
findings. Workspace builds; seams-init (incl. recurrence guard), no-todo-guard,
and the 3 milestone smoke queries (SELECT 1→1, pg_class→415, relkind='r'→68,
-c max_stack_depth=7000, no seam-miss) all green.
