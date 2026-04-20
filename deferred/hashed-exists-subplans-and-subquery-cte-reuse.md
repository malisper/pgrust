## Context

The Mandelbrot query exposed a large executor performance gap between `pgrust`
and PostgreSQL for correlated `EXISTS` over a recursive CTE. On native
`pgrust_server`, the 15-iteration version took about 13.66s and sampling showed
the hot path repeatedly re-entering:

- `eval_case_expr -> eval_exists_subquery -> ... -> CteScanState -> RecursiveUnionState`

The expensive part is not just the recursive math. The correlated `EXISTS`
inside `marked_points` causes `final_iteration -> iterations -> points` to be
rebuilt for many outer `points p` rows.

Relevant local code paths:

- [src/backend/executor/exec_expr/subquery.rs](src/backend/executor/exec_expr/subquery.rs:46)
- [src/backend/executor/exec_expr/subquery.rs](src/backend/executor/exec_expr/subquery.rs:131)
- [src/backend/executor/nodes.rs](src/backend/executor/nodes.rs:2399)

Today `eval_exists_subquery` starts a fresh executor state for the subplan, and
`with_scoped_subquery_runtime` snapshots/restores `ctx.cte_tables`,
`ctx.cte_producers`, and `ctx.recursive_worktables`. That means CTE state first
created inside the subquery is discarded when control returns to the outer row.

## Goal

Make simple correlated `EXISTS` / membership checks behave more like PostgreSQL:
build once, probe many, instead of rebuilding recursive CTE state per outer row.

## PostgreSQL Reference

PostgreSQL 18.3 does not rerun the recursive CTE for each point row here. Its
plan for the same Mandelbrot query shows:

- top-level `CTE iterations -> Recursive Union`
- outer `CTE Scan on points p`
- `hashed SubPlan`
- subplan scanning `iterations` filtered to the target final iteration

Relevant upstream implementation points:

- planner rewrite / hashing eligibility:
  [../postgres/src/backend/optimizer/plan/subselect.c](~/postgres/src/backend/optimizer/plan/subselect.c:243)
- hashed subplan flag:
  [../postgres/src/backend/optimizer/plan/subselect.c](~/postgres/src/backend/optimizer/plan/subselect.c:518)
- executor hashed subplan path:
  [../postgres/src/backend/executor/nodeSubplan.c](~/postgres/src/backend/executor/nodeSubplan.c:62)
- hash-table build:
  [../postgres/src/backend/executor/nodeSubplan.c](~/postgres/src/backend/executor/nodeSubplan.c:477)
- shared CTE tuplestore behavior:
  [../postgres/src/backend/executor/nodeCtescan.c](~/postgres/src/backend/executor/nodeCtescan.c:30)
  [../postgres/src/backend/executor/nodeCtescan.c](~/postgres/src/backend/executor/nodeCtescan.c:220)

## Likely Approaches

- add a narrow planner/executor optimization for simple equality-correlated
  `EXISTS`, lowering it to a build-once membership test
- add PostgreSQL-style hashed subplan support for `EXISTS` / `ANY` cases that
  are hashable
- avoid the tempting generic fix of persisting all subquery-local CTE caches;
  some subqueries legitimately capture outer values and must recompute per row

## Constraints

There is already at least one correctness-sensitive case where a subquery-local
CTE captures an outer value and should not be blindly reused:

- [src/backend/executor/tests.rs](src/backend/executor/tests.rs:13571)

So "just keep `cte_tables` alive longer" is likely wrong as a general fix.

## Why Deferred

This is a real executor performance bug, but it is separate from the parser
compatibility work that unblocked the Lisp interpreter query. The right fix
likely needs planner/executor work rather than another local workaround in the
demo query.
