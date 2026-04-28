Goal:
Count and classify failure reasons in `/tmp/diffs/rowsecurity.diff`.

Key decisions:
Counted 132 diff hunks. Repeated symptoms were counted both by matching hunks
and by visible occurrences where useful. Categories can overlap when one hunk
contains both a real row mismatch and an EXPLAIN-format mismatch.

Largest buckets:
- RLS policy qual application/EXPLAIN text differences: 47 hunks, 185 matched
  filter lines.
- COPY option parser failures: 3 hunks, 24 `DELIMITER ','` errors.
- PREPARE/EXECUTE unsupported or EXPLAIN EXECUTE parse errors: 7 hunks, 25
  errors.
- MERGE on RLS tables unsupported: 16 hunks, 18 errors.
- INSERT ON CONFLICT DO UPDATE on RLS tables unsupported: 5 hunks, 16 errors.
- Partitioned-table parent handling gaps: 5 hunks, 11 `"..." is not a table`
  errors.

Files touched:
`.codex/task-notes/rowsecurity-failure-counts.md`

Tests run:
No tests run; this was diff triage only.

Remaining:
Likely next implementation areas are RLS qual propagation/planning, COPY option
parsing/RLS behavior, prepared statement support, MERGE/ON CONFLICT RLS support,
and partitioned-table catalog/planner treatment.

Investigation: RLS qual missing/reordered/duplicated:
- Reordered restrictive/permissive quals: `visibility_policy_clauses` in
  `src/backend/rewrite/row_security.rs` builds `[permissive, restrictive...]`.
  PostgreSQL `add_security_quals()` appends restrictive policy quals first,
  then the combined permissive OR qual. This explains `dlevel <= ...` appearing
  before `cid <> 44`/`cid < 50` in document plans.
- Duplicated quals through recursive rewrite: `rewrite_query()` recursively
  rewrites subqueries/CTEs/set-operation inputs, and each recursive call applies
  row security. The later top-level `apply_query_row_security_with_active_relations`
  also recurses into those same subqueries/CTEs/set-operation inputs, so RLS is
  applied twice. This explains duplicate `a % 2` filters for view/subquery/UNION
  cases.
- Duplicated target visibility quals: UPDATE/DELETE analysis calls
  `build_target_relation_row_security(... include_select_visibility=true ...)`.
  A `FOR ALL` policy matches both UPDATE/DELETE and SELECT, and pgrust simply
  concatenates both results. PostgreSQL uses `list_append_unique()` while adding
  security quals, so the repeated policy expression is not duplicated.
- Missing PostgreSQL security ordering model: pgrust `RestrictInfo` has no
  `security_level` or `leakproof` flag, and plan creation has no equivalent of
  PostgreSQL `order_qual_clauses()`. PostgreSQL assigns successive
  `securityQuals` increasing security levels, then sorts by security level and
  cost, allowing cheap leakproof quals to move earlier. pgrust flattens bare
  expressions and preserves construction/path order.
- View owner/security-invoker mismatch: PostgreSQL runs RLS checks with
  `checkAsUser` for normal views and current user for security-invoker views.
  pgrust only sets `permission.check_as_user_oid`; `check_enable_rls()` always
  uses `catalog.current_user_oid()`. This causes some view plans to have missing
  or extra RLS filters depending on owner/current-user role.

Implementation:
- RLS rewrite now applies direct relation RTEs only for the current query node
  and propagates already-rewritten nested `depends_on_row_security` flags.
- Visibility policy quals now emit restrictive policies by name before the
  combined permissive/default-deny qual, with exact `Expr` append-unique
  dedupe.
- UPDATE/DELETE target RLS stores ordered `visibility_quals`, prepends them to
  user predicates, and exposes target RLS as query security quals for UPDATE
  FROM planning.
- Planner `RestrictInfo` now carries `security_level` and `leakproof`, preserves
  both through inheritance/partition translation, and orders scan filters by
  effective security level plus cost.
- View permission context now recurses through nested query nodes, so normal
  views use view-owner RLS and security-invoker views use current-user RLS.

Tests run:
- `scripts/cargo_isolated.sh test --lib --quiet row_security`
- `scripts/cargo_isolated.sh test --lib --quiet base_restrict_expr_order_respects_security_levels_and_leakproof`
- `scripts/cargo_isolated.sh check`
- `scripts/run_regression.sh --test rowsecurity --timeout 120 --jobs 1`

Latest regression result:
- `rowsecurity` still fails due expected unsupported/remaining areas.
- Matched queries improved to `568/774`; diff lines reduced to `3226`.
- New diff copied to `/tmp/diffs/rowsecurity.diff`.

Follow-up implementation bucket:
- Added focused support for COPY delimiter/RLS behavior, PREPARE/EXECUTE and
  EXPLAIN EXECUTE, ON CONFLICT DO UPDATE RLS checks, `row_security_active()`,
  public catalog reads needed by `pg_policies`, partitioned-table policy DDL,
  and faster schema-drop cleanup.
- Focused tests passed for row security, COPY FROM, prepared execution,
  ON CONFLICT RLS, `row_security_active`, pg_policies, and drop-schema cleanup.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `636/774` matched, `138` mismatches,
  `2336` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

Current largest remaining blockers:
- MERGE on RLS tables is still unsupported and accounts for a large contiguous
  block.
- Catalog/meta-command parity still has pg_class visibility and pg_policy
  node-string formatting differences.
- TABLESAMPLE, writable CTEs, WHERE CURRENT OF, DELETE USING/RETURNING, EXPLAIN
  INSERT, and SQL-function body support still produce unsupported-feature
  cascades.
- Some semantic RLS gaps remain around COPY TO permission ordering,
  UPDATE/RETURNING SELECT-policy checks, `pg_stats` RLS filtering, policy role
  dependencies, and inherited/partitioned DML display/projection.

MERGE RLS bucket:
- Added action-specific MERGE RLS checks: UPDATE/DELETE target USING checks,
  UPDATE final-row checks, INSERT final-row checks, and SELECT-policy rewrite
  for match visibility.
- Changed RLS write checks to require true; false and null now both fail.
- Adjusted MERGE `RETURNING *` expansion to emit source columns before target
  columns like PostgreSQL.
- Focused MERGE/RLS tests and existing MERGE returning tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `655/774` matched, `119` mismatches,
  `2158` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

RETURNING/generated-column WCO bucket:
- INSERT/UPDATE now add SELECT policy checks as write checks when RETURNING is
  present, so invisible returned rows raise RLS errors.
- Policy expression binding now uses generated-column output expressions, so
  virtual generated columns work inside RLS write checks.
- Focused `row_security` and ON CONFLICT tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `655/774` matched, `119` mismatches,
  `2098` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

Catalog readability bucket:
- Whitelisted public reads for core `pg_catalog` metadata relations used by
  `\d`, `\dp`, and policy/catalog inspection queries.
- Updated the relation privilege test to expect PostgreSQL-compatible `pg_class`
  visibility for ordinary users.
- Focused privilege and pg_policies tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `655/774` matched, `119` mismatches,
  `2083` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

COPY FROM RLS bucket:
- Protocol `COPY FROM STDIN` start validation now rejects ordinary RLS users
  before entering copy mode, matching PostgreSQL's "COPY FROM not supported
  with row-level security" behavior.
- Focused COPY wire-protocol and row-loading tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `656/774` matched, `118` mismatches,
  `2068` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

COPY TO privilege-order bucket:
- COPY TO relation sources now check SELECT privileges before running the
  generated SELECT, so plain permission errors win over RLS errors for users
  without table access.
- Focused COPY TO and relation privilege tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `658/774` matched, `116` mismatches,
  `2053` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

Auto-view DML RLS bucket:
- Auto-updatable view INSERT/UPDATE/DELETE rewrites now rebuild base-table RLS
  under the base relation's view permission user. Base RLS write checks run
  before view CHECK OPTION checks, and UPDATE/DELETE view predicates include
  base target visibility quals.
- Added a focused test for the `bv1`-style case where INSERT through a
  security-barrier view must report the base-table RLS error before the view
  CHECK OPTION error and must reject rows that only violate base RLS.
- Focused auto-view/RLS test passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `661/774` matched, `113` mismatches, `2033` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Policy subquery privilege bucket:
- Planned-statement privilege collection now walks query expression trees,
  including RLS security quals and sublink subqueries, instead of collecting
  only top-level range table permissions.
- Policy expression subqueries are tagged with the effective RLS user, so
  direct table RLS checks policy subqueries as the caller while normal views
  check them as the view owner.
- `EXPLAIN SELECT` now checks SELECT privileges before rendering the plan.
- Added a focused test covering permission denial from an RLS policy subquery
  for both SELECT and EXPLAIN.
- Focused policy-subquery test, `row_security`, and
  `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `672/774` matched, `102` mismatches, `1908` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

EXPLAIN INSERT SELECT bucket:
- Added `EXPLAIN INSERT ... SELECT` support that binds the INSERT, checks INSERT
  and source SELECT privileges, rewrites/plans the SELECT source, and renders it
  as a child plan under `Insert on ...`.
- Added child-plan renderers for normal and verbose logical EXPLAIN output so
  INSERT sources show PostgreSQL-style `->` prefixes.
- Focused EXPLAIN INSERT test passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `673/774` matched, `101` mismatches, `1897` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

TABLESAMPLE bucket:
- Added parser/analyzer support for `TABLESAMPLE BERNOULLI(percent)
  REPEATABLE(seed)` on base relations and materialized views.
- Implemented PostgreSQL-compatible Bernoulli tuple selection for the regression
  path by hashing `(block, offset, seed)` with PostgreSQL's `hash_any` mixing and
  applying the percent cutoff before normal WHERE predicates.
- Added a focused deterministic heap-offset sampling test.
- Focused TABLESAMPLE test passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `675/774` matched, `99` mismatches, `1854` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

EXPLAIN DELETE bucket:
- Added `EXPLAIN DELETE` support for direct heap/index target scans and inherited
  target append plans, including RLS/user filter display.
- Reuses the auto-updatable view DELETE rewrite before rendering so view DELETE
  plans are at least based on the rewritten target relation.
- Added a focused test covering `ONLY` and inherited DELETE plan rendering with
  RLS filters.
- Focused EXPLAIN DELETE test passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `677/774` matched, `97` mismatches, `1825` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
