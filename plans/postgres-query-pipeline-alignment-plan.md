# Align pgrust's Query Pipeline and Names with PostgreSQL

## Current Status

### Done

- Semantic `Query` / `RangeTblEntry` / `TargetEntry` / `SortGroupClause` live under `parsenodes` / `primnodes`.
- SELECT-family analysis builds `Query` directly instead of `BoundSelectPlan` / `BoundFromPlan`.
- Semantic expressions use PostgreSQL-shaped nodes:
  - `Var`
  - `OpExpr`
  - `BoolExpr`
  - `FuncExpr`
  - `Aggref`
  - `SubLink`
  - `SubPlan`
- Legacy semantic `Expr::*` operator/function variants are gone.
- `SubLink` exists before planning and `SubPlan` only after planning.
- `backend/rewrite` exists as an explicit `Query -> Vec<Query>` boundary.
- View expansion moved to rewrite time.
- The planner returns `PlannedStmt`.
- `QueryDesc` exists and top-level execution goes through `PlannedStmt` / `QueryDesc`.
- `PlannerInfo`, `RelOptInfo`, `RestrictInfo`, `PathTarget`, `PathKey`, and `SpecialJoinInfo` exist in `pathnodes`.
- Planner paths now carry plain semantic `Expr`/`Var`.
- The optimizer no longer uses `PlannerJoinExpr` or planner-only identity nodes like:
  - `BaseColumn`
  - `SyntheticColumn`
  - `InputColumn`
  - `LeftColumn`
  - `RightColumn`
- Costing/selectivity/index matching now inspect semantic `Expr::Var` directly.
- Lowering to executor positional `Expr::Column(n)` now happens at `into_plan()` / create-plan time.
- Base relation paths are generated directly into `RelOptInfo.pathlist`:
  - seq scan
  - index scan
  - values/function/subquery base paths
- `query_planner()` / `make_one_rel()` now assemble cheapest paths from relation pathlists rather than from a prebuilt recursive `PlannerPath::from_query(...)` skeleton.
- Join relations are now built from planner state and child `pathlist`s instead of the old recursive planner-path builder.
- Upper planning (`projection`, `aggregate`, `order`, `limit`, `project-set`) now runs as planner relation/pathlist construction on top of the scan/join relation.
- The old recursive `PlannerPathBuilder` / `PlannerPath::from_query(...)` scaffold has been removed.

### Partially Done

- Planner state exists and drives path generation, but `RelOptInfo.pathlist` / joinrel search / upper-rel planning are still much simpler than PostgreSQL.
- Join search still follows the parsed jointree recursively and compares local join alternatives; it is not yet PostgreSQL-style dynamic-programming joinrel enumeration across all legal relation subsets.
- Upper-rel planning is now relation-driven, but it still works over the project-local `PlannerPath` variants rather than a fuller PostgreSQL-style `Path` hierarchy.
- `plannodes.rs` is much narrower than before, but the planner still uses the project-local `PlannerPath` type rather than a fully PostgreSQL-shaped `Path` hierarchy.
- Runtime entry is closer to PostgreSQL, but execution is still not a full `QueryDesc -> EState / PlanState / ExprState` match.

### Remaining Major Milestones

1. Replace recursive jointree-driven join construction with PostgreSQL-style joinrel enumeration over legal relation subsets.
2. Make join legality and outer-join restrictions drive search more directly from `SpecialJoinInfo`, instead of mostly preserving syntactic join shape.
3. Expand upper-rel planning toward PostgreSQL’s `grouping_planner` / upper-rel model instead of the current simpler relation wrappers.
4. Rename the remaining planner types closer to PostgreSQL once semantics match well enough:
   - `PlannerPath` -> `Path`
   - more concrete PG-style path variants where helpful
5. Continue narrowing `plannodes.rs` so it stays strictly final executable plan state.
6. Decide how far to take runtime PG fidelity after planner architecture is stable.

## Summary

Realign pgrust around PostgreSQL's actual query representations and stage names, using PostgreSQL's source as the contract:

- raw syntax AST in `parsenodes`
- semantic `Query` / `RangeTblEntry` in `parsenodes`
- semantic expressions in `primnodes`
- planner search state in `pathnodes`
- final `PlannedStmt` / `Plan` in `plannodes`
- runtime `QueryDesc` / `PlanState` / tuple slots in `execdesc` + `execnodes`

This is a hard cutover for SELECT-family planning. No analyzer path for `SELECT` should emit executor plans or positional `Column(n)` expressions before `create_plan`, and no planner identity should depend on tuple-position placeholders.

The core stage-boundary cutover is now mostly done, and the planner already builds base/join/upper paths from `PlannerInfo` / `RelOptInfo`. The main unfinished planner work is now join-search fidelity and path-hierarchy fidelity, not semantic query representation or the old recursive path-builder cutover.

## Key Changes

### 1. Re-split shared nodes to mirror PostgreSQL

- Keep raw SQL AST in `src/include/nodes/parsenodes.rs`, but add PostgreSQL-shaped semantic query nodes there too:
  - `Query`
  - `RangeTblEntry`
  - `RTEKind`
  - `FromExpr`
  - `JoinExpr`
  - `RangeTblRef`
  - `CommonTableExpr`
  - `SetOperationStmt`
- Add new `src/include/nodes/primnodes.rs` for semantic and planned expression nodes:
  - `Var`
  - `Const`
  - `OpExpr`
  - `FuncExpr`
  - `Aggref`
  - `BoolExpr`
  - `RelabelType`
  - `SubLink`
  - `SubPlan`
  - `TargetEntry`
  - `SortGroupClause`
- Add new `src/include/nodes/pathnodes.rs` for planner-only state:
  - `PlannerGlobal`
  - `PlannerInfo`
  - `RelOptInfo`
  - `RestrictInfo`
  - `PathTarget`
  - `PathKey`
  - `SpecialJoinInfo`
  - `Path` and concrete path variants
- Narrow `src/include/nodes/plannodes.rs` to final executable plan structures only:
  - `PlannedStmt`
  - `Plan`
  - physical plan node structs/enums
- Keep `src/include/nodes/execnodes.rs` runtime-only.

### 2. Rename current abstractions to PostgreSQL names where semantics match

- Replace current SELECT-family analyzer output names:
  - remove `BoundSelectPlan`
  - remove `BoundFromPlan`
  - remove `DeferredSelectPlan`
- Replace optimizer naming:
  - `optimize_bound_query` -> `planner` / `standard_planner`
  - `PlannerPath` -> `Path`
- Add PG-shaped top-level wrappers:
  - `parse_analyze(...) -> Query`
  - `pg_rewrite_query(...) -> Vec<Query>`
  - `pg_plan_query(...) -> PlannedStmt`
  - internal `create_plan(...) -> Plan`
- Keep names that already match and are semantically correct:
  - `Plan`
  - `PlanState`
- Do not rename non-equivalent helper types just for cosmetics:
  - do not rename `ExecutorContext` to `EState` in this slice
  - do not rename `BoundRelation` to `RangeTblEntry`

### 3. Make analyze emit a real PostgreSQL-shaped `Query`

- `parser/analyze` must stop producing plan-shaped IR for SELECT-family planning.
- Build `Query` with:
  - `rtable`
  - `jointree`
  - `target_list`
  - `sort_clause`
  - `group_clause`
  - `having_qual`
  - `limit_count` / `limit_offset`
  - `cte_list`
  - `set_operations`
- Replace positional `Expr::Column(n)` with real `Var { varno, varattno, varlevelsup, vartype }`.
- Add real `TargetEntry` semantics:
  - `expr`
  - `resno`
  - `resname`
  - `ressortgroupref`
  - `resjunk`
- Add real `SortGroupClause` linkage:
  - `tle_sort_group_ref`
  - operator/collation/null-order metadata needed for current `ORDER BY`, `GROUP BY`, and `DISTINCT` support
- Keep `SubLink` strict:
  - semantic query trees use `SubLink`
  - no `SubPlan` before planning
- Remove analyzer production of executor `Plan` entirely for SELECT-family statements.

### 4. Add real range-table and join metadata, including join RTEs

- Implement `RangeTblEntry` kinds needed by current features:
  - `RTE_RELATION`
  - `RTE_SUBQUERY`
  - `RTE_JOIN`
  - `RTE_VALUES`
  - `RTE_FUNCTION`
  - `RTE_CTE`
- For joins, add PG-shaped join RTE metadata from the start:
  - `jointype`
  - `joinmergedcols`
  - `joinaliasvars`
  - `joinleftcols`
  - `joinrightcols`
- Use this join RTE metadata to represent `JOIN USING`, `NATURAL JOIN`, and FULL JOIN merged-column identity correctly, instead of relying on late projection hacks.
- Analyzer should always build a top-level `FromExpr` jointree, like PostgreSQL.

### 5. Add an explicit rewrite boundary

- Add `backend/rewrite` with a real pass-through boundary:
  - `rewrite_query(query, catalog) -> Vec<Query>`
- This boundary always exists even when it returns one unchanged query.
- Move current limited view expansion to rewrite time:
  - analyzer resolves the relation reference
  - rewrite converts view references into subquery RTEs
- Do not implement full rule actions or generic `CREATE RULE` in this slice.

### 6. Replace planner input with `Query` and PG-shaped planner state

- Planner takes `Query`, not bound plan nodes and not executor `Plan`.
- Build planner search state exactly around:
  - `PlannerInfo`
  - `RelOptInfo`
  - `RestrictInfo`
  - `PathTarget`
  - `Path`
  - `PathKey`
  - `SpecialJoinInfo`
- Use `varno` and relid sets as planner identity.
- Remove the transitional planner-expression layer:
  - `PlannerJoinExpr`
  - scan-instance synthetic var hacks
  - layout-based `InputColumn` mapping as the primary representation
- Planner searches over current executor capability only:
  - seq scan
  - index scan
  - nested loop
  - sort
  - aggregate
  - limit
  - projection
  - values
  - function scan
  - project-set

### 7. Keep PG's expression contract across Query, planner, and Plan

- `Query` and final `Plan` both reference `primnodes` expressions and `TargetEntry`, like PostgreSQL.
- `create_plan` is still the only phase that picks physical `Plan` nodes, but it does not collapse expressions into ad hoc executor-only trees.
- After planning, expression subqueries become `SubPlan`, not earlier.
- Final plan expressions should use PostgreSQL-shaped var semantics:
  - base vars by `varno`/`varattno`
  - plan-local vars rewritten in the PG spirit rather than reverting to a project-wide `Column(n)` convention

### 8. Add a lightweight `QueryDesc` runtime boundary

- Add `execdesc.rs`-style wrapper:
  - `QueryDesc { planned_stmt, source_text, params, destination, snapshot? }`
- Route top-level execution/explain/cache through `QueryDesc` and `PlannedStmt`, not a bare `Plan`.
- Keep runtime internals incremental:
  - keep existing `PlanState`/tuple-slot executor structure
  - do not force a full `EState` redesign yet
- Be explicit that runtime fidelity is still partial compared to PG.

## Test Plan

- Structural stage tests:
  - parse/analyze returns `Query`
  - rewrite returns `Vec<Query>`
  - planner returns `PlannedStmt`
  - `create_plan` is the only phase creating `Plan`
  - expression subqueries are `SubLink` before planning and `SubPlan` after planning
- Query-shape tests:
  - resolved columns are `Var(varno, varattno)`
  - target lists are `TargetEntry` with `ressortgroupref`
  - `ORDER BY`/`GROUP BY`/`DISTINCT` use `SortGroupClause`
  - joins create `RTE_JOIN` entries with alias-var metadata
  - `JOIN USING` and FULL JOIN preserve merged-column identity correctly
- Planner tests:
  - planner consumes `Query`, not `BoundSelectPlan`
  - self-joins and aliases remain distinct by `varno`
  - path selection comes from `PlannerInfo`/`RelOptInfo`/`Path`
- Runtime boundary tests:
  - top-level execution takes `PlannedStmt` via `QueryDesc`
  - `EXPLAIN` formats from `PlannedStmt`
- Acceptance checks:
  - no SELECT-family analyzer path emits executor `Plan`
  - no SELECT-family planner path depends on `Expr::Column(n)`
  - no production planner entrypoint accepts `Plan`
  - `rg "BoundSelectPlan|BoundFromPlan|DeferredSelectPlan|PlannerJoinExpr"` is empty outside migration shims/tests
  - `cargo check`
  - `cargo test --lib --quiet`

## Assumptions and Defaults

- Hard cutover is intentional. No long-lived dual path for SELECT-family planning.
- Rewrite scope is explicit but pass-through. This slice aims for PostgreSQL-shaped boundaries, not full rule-system fidelity.
- The raw parser AST can stay close to today's `Statement`/`SelectStatement` shapes for now. The main alignment starts at semantic analysis and after.
- Runtime target is “PostgreSQL-shaped execution entry boundary” rather than full PostgreSQL executor-state parity.
- Naming alignment is semantic, not cosmetic. Use PostgreSQL names only when the underlying abstraction now really matches PostgreSQL's role.
