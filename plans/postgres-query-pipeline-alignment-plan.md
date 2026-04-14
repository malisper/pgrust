# Align pgrust's Query Pipeline and Planner with PostgreSQL

## Current Status

The semantic query pipeline cutover is complete enough that the planner now
looks like a PostgreSQL-style planner, not a recursive planner-path builder.

### Done

- Semantic analysis produces `Query` / `RangeTblEntry` / `TargetEntry` /
  `SortGroupClause` trees.
- Planner search state lives in `src/include/nodes/pathnodes.rs`:
  - `PlannerInfo`
  - `RelOptInfo`
  - `RestrictInfo`
  - `PathTarget`
  - `PathKey`
  - `SpecialJoinInfo`
  - `Path`
- Base rels, join rels, and upper rels all build `pathlist`s directly.
- Join search enumerates joinrels across legal relation subsets instead of
  recursing down a `PlannerPath::from_query(...)` skeleton.
- The old `PlannerPath` / recursive path-builder scaffold is gone.
- Planner behavior now lives in optimizer modules instead of shared-node files:
  - `root.rs` for planner-root construction and target derivation
  - `joininfo.rs` for `SpecialJoinInfo` construction and relid helpers
  - `bestpath.rs` for cheapest-path selection
  - `upperrels.rs` for upper-rel lookup/creation
- `RelOptInfo` tracks both:
  - `cheapest_startup_path`
  - `cheapest_total_path`
- Best-path selection now supports:
  - cheapest total path
  - cheapest startup path
  - cheapest path satisfying required pathkeys
  - final-path choice based on required ordering instead of always taking
    `cheapest_total_path`
- Planner targets are derived once on `PlannerInfo`:
  - `scanjoin_target`
  - `group_input_target`
  - `grouped_target`
  - `sort_input_target`
  - `final_target`
- Upper planning uses explicit upper rels keyed by:
  - `UpperRelKind`
  - relid set
  - `PathTarget`
- Implemented upper rel kinds:
  - `GroupAgg`
  - `ProjectSet`
  - `Ordered`
  - `Final`
- Ordered-path reuse works:
  - ordered base/index paths can satisfy final ordering directly
  - final projections can preserve hidden order keys
  - `make_ordered_rel()` now considers the cheapest matching ordered path and
    a single sort on the cheapest total path instead of sorting every input
    path
- `SpecialJoinInfo` tracks more PG-like outer-join metadata:
  - `ojrelid`
  - `commute_above_l`
  - `commute_above_r`
  - `commute_below_l`
  - `commute_below_r`
- Join legality is driven by `SpecialJoinInfo` plus FULL JOIN barriers, not by
  the old exact-inner-join legality shortcut.
- Exact join-RTE lookup remains only for output shaping:
  - join `reltarget`
  - join alias projection
  - top-join output normalization

### Planner Pipeline Today

1. Analyze builds a semantic `Query`.
2. `PlannerInfo::new()` derives planner targets, pathkeys, simple rels, and
   `SpecialJoinInfo`.
3. `query_planner()` builds the scan/join relation from base rel and joinrel
   `pathlist`s.
4. `grouping_planner()` builds explicit upper rels for:
   - filter
   - aggregate
   - project-set
   - ordered
   - limit
   - final projection
5. `standard_planner()` picks the cheapest final path satisfying required
   ordering when possible, then lowers `Path` to executable `Plan`.

## Remaining Planner Work

The major pipeline-alignment work is done. The remaining planner work is now
about fidelity and breadth, not about replacing the old planner architecture.

### Still In Scope for Planner Fidelity

- Keep tightening outer-join legality toward PostgreSQL in corner cases beyond
  the current LEFT/FULL barrier model.
- Push more of the target-shape logic down safely so lower planning can request
  alternative `scanjoin_target` shapes without regressing grouped or aliased
  queries.
- Remove the remaining planner-side defensive `Expr::Column` handling outside
  final plan lowering.
- Add PG-style path domination pruning in `add_path()` once the planner has
  more path families.

### Intentionally Deferred

- parameterized paths
- parallel paths
- merge join / hash join
- pathkeys/equivalence-class machinery at PostgreSQL depth
- lateral join planning
- plan cache / `QueryDesc` runtime fidelity work

## Summary

pgrust now has a real planner-state pipeline:

- semantic `Query`
- planner state in `pathnodes`
- dynamic-programming joinrel construction
- explicit upper rel planning
- final `PlannedStmt`

The main architectural cutover is complete. Future work should treat the
current `Path`/`RelOptInfo` planner as the base to refine, not as a temporary
bridge back to the removed recursive planner builder.
