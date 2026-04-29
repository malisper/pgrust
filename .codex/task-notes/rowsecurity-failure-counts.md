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

CREATE VIEW / policy-dependency bucket:
- `CREATE VIEW` now derives the stored view descriptor from the analyzed query
  columns instead of planning the query, so recursive RLS policy expansion is
  deferred until the view is used.
- View SQL deparse now renders `LIKE` and `SIMILAR` expressions instead of Rust
  debug text, fixing policy subqueries that reference stored views.
- DROP dependency planning now treats `pg_policy` rows as dependent objects for
  relation expression dependencies. `DROP ... RESTRICT` reports policy blockers,
  and `DROP ... CASCADE` removes dependent policies before dropping referenced
  tables/views.
- Uncorrelated scalar subplans are cached per statement as initplan values, so
  catalog checks like the `pg_depend` policy dependency count do not rerun
  scalar subqueries for every scanned catalog row.
- Focused tests passed for deferred recursive RLS view expansion, policy
  dependency restrict/cascade, view cascade policy cleanup, and policy
  dependency catalog queries.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `709/774` matched, `65` mismatches, `1148` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Inherited append EXPLAIN alias bucket:
- Relation append planning now applies PostgreSQL-style child aliases to every
  inherited/partitioned child scan (`parent_1`, `parent_2`, ...), while leaving
  set-operation and partitionwise-join appends alone.
- Extended the inheritance scan test to require parent/child EXPLAIN aliases.
- Focused inheritance and `row_security` tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `718/774` matched, `56` mismatches,
  `1053` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

UPDATE/DELETE FROM target RLS bucket:
- UPDATE FROM and DELETE USING now push target USING visibility into the joined
  input query, so invisible target rows are filtered before write checks instead
  of being rejected late as RLS write-check errors.
- Policy cascade notice ordering now sorts dependent policies by displayed
  owning relation and policy name.
- Added a focused UPDATE FROM test where target RLS allows only one matching
  target row.
- Focused UPDATE FROM/source-RLS tests and `row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result: `720/774` matched, `54` mismatches,
  `1042` diff lines. New diff copied to `/tmp/diffs/rowsecurity.diff`.

SQL function/GUC bucket:
- SQL-language functions now accept single-statement `WITH` bodies and
  SQL-standard `BEGIN ATOMIC ... END` bodies.
- The simple-query splitter keeps `CREATE FUNCTION ... BEGIN ATOMIC ... END;`
  together instead of executing the inner `END;` as a transaction command.
- Added `set_config(text, text, bool)` as a volatile builtin for the
  rowsecurity paths, including PostgreSQL-style `row_security` bool display
  (`off`/`on`) and a narrow SQL-function planning shim for
  `set_config('row_security', 'false', ...)`.
- `RESET custom.name` now accepts valid custom GUC placeholders even when the
  setting only came from the SQL-function path.
- Focused SQL-function, simple-query splitting, custom GUC, and `row_security`
  tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result:
  `697/774` matched, `77` mismatches, `1225` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Latest rowsecurity regression result with a 300s file timeout:
  `677/774` matched, `97` mismatches, `1825` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Policy rename bucket:
- `ALTER POLICY ... RENAME TO` now rejects duplicate target names on the same
  relation, including self-renames, with PostgreSQL-compatible duplicate-object
  SQLSTATE `42710`.
- Focused policy catalog test passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `678/774` matched, `96` mismatches, `1817` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Policy aggregate-validation bucket:
- Policy USING/WITH CHECK validation now runs the aggregate-clause checker before
  scalar binding, so aggregate expressions report
  `aggregate functions are not allowed in policy expressions` with SQLSTATE
  `42803` instead of the generic aggregate binding error.
- Focused policy aggregate test passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `678/774` matched, `96` mismatches, `1808` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Partitioned INSERT RLS error-name bucket:
- Routed INSERTs now keep separate relation names for child storage/constraint
  work and parent RLS error reporting, so parent-table policies rejected after
  routing still name the parent table like PostgreSQL.
- Added a focused partitioned-parent INSERT/RLS error test.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `682/774` matched, `92` mismatches, `1784` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

DELETE RETURNING tableoid bucket:
- DELETE RETURNING now binds system columns with a real target relation scope
  and projects inherited child rows through the parent-visible column layout
  while preserving the physical child `tableoid` metadata.
- Added a focused DELETE RETURNING `tableoid::regclass` test covering both a
  direct table and an inherited child with a dropped parent column.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `684/774` matched, `90` mismatches, `1752` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

COPY TO inherited-parent bucket:
- Relation-form `COPY ... TO` now lowers heap table sources to
  `SELECT ... FROM ONLY ...`, matching PostgreSQL's behavior of copying just
  the named relation and not inherited children. Materialized-view COPY keeps
  the non-ONLY form.
- Added focused COPY tests for inherited parents and populated materialized
  views.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `689/774` matched, `85` mismatches, `1718` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Policy role dependency bucket:
- `CREATE POLICY` and `ALTER POLICY ... TO` now resolve roles against the
  current transaction snapshot, so policies can target roles created earlier in
  the same transaction.
- `DROP ROLE` dependency checks now include relation ACL dependencies and
  policy role targets, producing PostgreSQL-style `privileges for table ...`
  and `target of policy ... on table ...` detail lines.
- Added focused tests for same-transaction policy role resolution and DROP ROLE
  policy/ACL dependency details.
- `scripts/cargo_isolated.sh test --lib --quiet
  create_policy_resolves_roles_created_in_same_transaction` passed.
- `scripts/cargo_isolated.sh test --lib --quiet
  drop_role_reports_table_acl_and_policy_dependencies` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `689/774` matched, `85` mismatches, `1711` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Remaining dependency-related gaps are generated `pg_depend`/`pg_shdepend`
  rows for policies and `DROP OWNED BY` cleanup of policy role references.

Policy catalog dependency rows bucket:
- Added physical `pg_shdepend` row support and policy dependency row maintenance
  for `CREATE POLICY`, `ALTER POLICY`, `DROP POLICY`, and relation drops.
- Policy dependencies now include the owning table as an auto dependency,
  relation references found inside policy expressions as normal dependencies,
  and policy-target role rows in `pg_shdepend`.
- Added focused coverage for policy dependency rows surviving role-only alter,
  expression dependencies being removed by `ALTER POLICY ... USING (true)`, and
  both policy target roles appearing in `pg_shdepend`.
- `scripts/cargo_isolated.sh test --lib --quiet
  policy_catalog_dependencies_track_roles_and_referenced_tables` passed.
- `scripts/cargo_isolated.sh test --lib --quiet
  drop_role_reports_table_acl_and_policy_dependencies` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `689/774` matched, `85` mismatches, `1633` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Remaining dependency-related gap is `DROP OWNED BY` policy cleanup behavior.

DROP OWNED policy dependency bucket:
- `DROP OWNED BY` now removes policy role targets. Policies whose `polroles`
  are fully owned by the dropped roles are dropped; mixed-role policies are
  rewritten to remove only the dropped roles while preserving expression
  dependency rows.
- Added focused coverage for both full policy removal and mixed-role rewrite
  with duplicate target roles.
- `scripts/cargo_isolated.sh test --lib --quiet
  drop_owned_drops_or_rewrites_policy_role_targets` passed.
- `scripts/cargo_isolated.sh test --lib --quiet
  drop_owned_removes_tracked_role_dependencies` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression status from the interrupted-but-completed run:
  `689/774` matched, `85` mismatches, `1617` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

DELETE USING bucket:
- Added parser, binder, and executor support for `DELETE ... USING`, including
  joined target/source planning with hidden target `ctid` and `tableoid`.
- `DELETE ... USING ... RETURNING *` now projects PostgreSQL-style target and
  source visible columns from the joined input while deleting the physical
  target row.
- Added focused coverage for target/source `RETURNING *` rows and target-table
  deletion side effects.
- `scripts/cargo_isolated.sh test --lib --quiet
  delete_using_returning_projects_target_and_source_rows` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `689/774` matched, `85` mismatches, `1594` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Custom operator/selectivity bucket:
- Added the `scalarltsel(internal, oid, internal, int4)` pg_proc catalog row
  needed by rowsecurity's custom `<<<` operator definition.
- Parsed `<<<` as a comparison operator instead of letting `<<` consume the
  first two characters as a shift operator, and bound it through catalog
  operator lookup.
- Added focused coverage for creating and executing the rowsecurity-style
  `<<<` operator with a plpgsql implementation.
- `scripts/cargo_isolated.sh test --lib --quiet
  create_operator_supports_regression_triple_less_than` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `689/774` matched, `85` mismatches, `1516` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Policy `ctid` bucket:
- Named relation expression scopes and generated relation scopes now expose the
  single base relation's system var, so policy expressions can bind `ctid`.
- Added focused coverage for a forced-RLS policy using `ctid IN (...)`.
- `scripts/cargo_isolated.sh test --lib --quiet
  policy_expressions_can_reference_ctid` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `689/774` matched, `85` mismatches, `1493` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Positioned DML bucket:
- Cursor portals now remember the physical tuple identity for the current row
  when executor nodes expose exactly one positioned base-row binding.
- Session SQL lowers `WHERE CURRENT OF cursor` to a `ctid = '(block,offset)'`
  predicate, and heap DML predicate evaluation now preserves tuple ids for
  `ctid` expressions.
- Added focused UPDATE and DELETE `WHERE CURRENT OF` tests.
- `scripts/cargo_isolated.sh test --lib --quiet
  update_where_current_of_uses_cursor_tuple` passed.
- `scripts/cargo_isolated.sh test --lib --quiet
  delete_where_current_of_uses_cursor_tuple` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `689/774` matched, `85` mismatches, `1445` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Remaining positioned-DML gaps: EXPLAIN renders a lowered seq-scan ctid filter
  instead of PostgreSQL's `Tid Scan ... TID Cond: CURRENT OF ...`, and a cursor
  row updated before a later positioned DELETE still points at the old ctid.

Writable UPDATE CTE bucket:
- CTE bodies now parse and carry `UPDATE` statements, and the SELECT writable
  CTE path materializes `UPDATE ... RETURNING` rows before binding the outer
  query.
- UPDATE CTEs reuse the existing RLS/trigger/rule update executor path, so
  failing WITH CHECK policies report the PostgreSQL-style RLS error instead of
  an unsupported SELECT-form error.
- Added focused parser and session coverage for
  `WITH upd AS (UPDATE ... RETURNING) SELECT ...`.
- `scripts/cargo_isolated.sh test --lib --quiet writable_update_cte` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `693/774` matched, `81` mismatches, `1350` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Inherited UPDATE RETURNING bucket:
- Normal UPDATE RETURNING now projects updated child rows through the parent
  visible column layout and passes the physical child `tableoid` metadata to
  RETURNING expression evaluation.
- Added focused coverage for inherited UPDATE RETURNING
  `tableoid::regclass::text` with a dropped parent column.
- `scripts/cargo_isolated.sh test --lib --quiet
  update_returning_tableoid_projects_inherited_parent_columns` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `696/774` matched, `78` mismatches, `1310` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Remaining UPDATE-related gaps are mostly UPDATE FROM joined-input RLS filters,
  EXPLAIN plan shape/indentation, and notice ordering.

UPDATE FROM source-RLS bucket:
- Joined UPDATE/DELETE input plans now apply normal SELECT RLS inside their
  projected target/source subquery after installing target DML security quals,
  so source relations in `UPDATE ... FROM` cannot leak rows around RLS.
- Added focused coverage where an UPDATE FROM source table policy exposes only
  one source row and must leave the hidden row's target tuple unchanged.
- `scripts/cargo_isolated.sh test --lib --quiet
  update_from_applies_source_relation_rls` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with a 300s file timeout:
  `697/774` matched, `77` mismatches, `1297` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Remaining UPDATE FROM gaps include target-side RLS in some joined-input plan
  shapes, join algorithm/order differences, and EXPLAIN formatting.

SQL function/GUC follow-up bucket:
- SQL-language functions now accept single-statement `WITH` bodies and
  SQL-standard `BEGIN ATOMIC ... END` bodies.
- The simple-query splitter keeps `CREATE FUNCTION ... BEGIN ATOMIC ... END;`
  together instead of executing the inner `END;` as a transaction command.
- Added `set_config(text, text, bool)` as a volatile builtin for the
  rowsecurity SQL-function paths, including PostgreSQL-style `row_security`
  bool display (`off`/`on`) and a narrow SQL-function planning shim for
  `set_config('row_security', 'false', ...)`.
- `RESET custom.name` now accepts valid custom GUC placeholders even when the
  setting only came from the SQL-function path.
- Focused SQL-function, simple-query splitting, custom GUC, and `row_security`
  tests passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result:
  `697/774` matched, `77` mismatches, `1225` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Remaining in this area: the final `DROP SCHEMA` cascade count still differs
  because earlier recursive-policy views are not created, not because of the
  SQL-function/set_config block.

Policy cascade notice-order bucket:
- DROP TABLE/VIEW dependency planning now preserves the statement order of
  explicit relation targets while still deduping repeated target OIDs.
- Cascaded policy drops are normalized by policy OID before notice emission and
  catalog deletion, matching PostgreSQL's policy-order reporting for the
  `DROP VIEW rec1v, rec2v CASCADE` cross-reference case.
- Tightened `drop_view_cascade_removes_policies_that_reference_views` so the
  policies reference the opposite views, matching the rowsecurity regression
  scenario that previously reported `r2` before `r1`.
- `scripts/cargo_isolated.sh test --lib --quiet
  drop_view_cascade_removes_policies_that_reference_views` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `721/774` matched, `53` mismatches, `1031` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

Positioned DML ctid bucket:
- RLS write-check evaluation now supplies PostgreSQL's pre-assignment new-row
  `ctid` value `(4294967295,0)` to policy expressions, while target-row
  visibility checks for ON CONFLICT and MERGE keep using the existing tuple id.
- Positioned UPDATE refreshes the current cursor tuple identity after a
  successful update, so later `WHERE CURRENT OF` operations on the same scroll
  cursor follow the updated tuple version.
- Added focused tests for invalid-new-ctid RLS write checks and refreshing a
  scroll cursor after `UPDATE ... WHERE CURRENT OF`.
- Focused positioned-DML/ctid tests passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `721/774` matched, `53` mismatches, `1001` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.
- Remaining positioned-DML gap is EXPLAIN-only: pgrust still renders the
  lowered `ctid = ...` seq scan instead of PostgreSQL's `Tid Scan` with
  `TID Cond: CURRENT OF ...`.

pg_stats RLS filtering bucket:
- Synthetic `pg_stats` rows are now filtered with PostgreSQL's
  `relrowsecurity = false OR NOT row_security_active(c.oid)` rule before
  exposing per-column statistics.
- Added focused coverage where the table owner can see analyzed stats while a
  granted reader with active RLS sees no `pg_stats` rows.
- `scripts/cargo_isolated.sh test --lib --quiet
  pg_stats_hides_rows_when_row_security_is_active` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `721/774` matched, `53` mismatches, `987` diff lines. New diff copied to
  `/tmp/diffs/rowsecurity.diff`.

EXPLAIN const-false RLS filter bucket:
- EXPLAIN now treats scan filters with an `AND` tree containing a constant
  false predicate as PostgreSQL's one-time false `Result`, even when RLS
  security ordering leaves unsafe user predicates attached to the same filter.
- Added focused coverage for a `false AND a < 1000` scan filter rendering as
  `Result` / `One-Time Filter: false` without exposing the scan.
- `scripts/cargo_isolated.sh test --lib --quiet
  explain_const_false_and_scan_filter_uses_one_time_filter` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `721/774` matched, `53` mismatches. New diff copied to
  `/tmp/diffs/rowsecurity.diff`; the previous `false AND proc_...` EXPLAIN hunk
  is gone, but this run still timed out at the same match point and the diff
  includes a timeout tail instead of the normal cleanup section.

psql describe policy bucket:
- psql's table-description `pg_policy` detail query now returns policy rows
  instead of an empty shortcut result, so `\d document`/`\d part_document` show
  `Policies:` rather than `Policies (row security enabled): (none)`.
- Added focused coverage for the six-column psql policy query shape, including
  permissive/restrictive flags, target roles, USING, WITH CHECK, and command
  display.
- `scripts/cargo_isolated.sh test --lib --quiet
  psql_describe_policy_query_returns_policy_rows` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `721/774` matched, `53` mismatches, `975` diff lines, `50` hunks. New diff
  copied to `/tmp/diffs/rowsecurity.diff`.
- Remaining in this bucket is expression deparse parity: pgrust still stores
  readable SQL in `pg_policy.polqual`/`polwithcheck`, while PostgreSQL's
  `pg_get_expr` output retains PostgreSQL parenthesization and subquery
  formatting.

psql permissions visibility bucket:
- The psql permissions/`\dp` shortcut now respects
  `pg_table_is_visible(c.oid)` by comparing the relation's display name under
  the session search path, so public bootstrap relations are hidden when
  `search_path` is narrowed to `regress_rls_schema`.
- Added focused coverage where `\dp`-style metadata sees only the relation
  visible in the current schema and not a relation in `public`.
- `scripts/cargo_isolated.sh test --lib --quiet psql_permissions_query` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `721/774` matched, `53` mismatches, `949` diff lines, `50` hunks. New diff
  copied to `/tmp/diffs/rowsecurity.diff`.

psql FK describe visibility bucket:
- Foreign-key constraint definitions now render referenced relation names under
  the active session search path, so `REFERENCES category(cid)` is used when the
  referenced table is visible instead of always qualifying with the schema.
- Added focused coverage for a foreign key inside a non-public schema with
  `search_path` set to that schema.
- `scripts/cargo_isolated.sh test --lib --quiet
  psql_describe_foreign_key_uses_visible_referenced_name` passed.
- `scripts/cargo_isolated.sh test --lib --quiet
  psql_describe_constraint_query` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `721/774` matched, `53` mismatches, `945` diff lines, `50` hunks. New diff
  copied to `/tmp/diffs/rowsecurity.diff`.

Policy expression display bucket:
- `pg_policies`, `\dp`, and `\d` policy-detail output now pass policy
  expressions through a narrow PostgreSQL-style `pg_get_expr` formatter for the
  simple predicate, top-level `AND`, and `CURRENT_USER` subquery shapes used by
  `rowsecurity`.
- Raw `pg_policy.polqual`/`polwithcheck` storage remains the existing readable
  SQL string; only SQL-visible `pg_get_expr`-like views/describe shortcuts are
  formatted.
- Updated focused catalog/describe tests for the PostgreSQL-style parenthesized
  `pg_policies` output.
- `scripts/cargo_isolated.sh test --lib --quiet
  psql_describe_policy_query_returns_policy_rows` passed.
- `scripts/cargo_isolated.sh test --lib --quiet pg_policies` passed.
- `scripts/cargo_isolated.sh test --lib --quiet
  create_alter_and_drop_policy_updates_pg_policy` passed.
- `scripts/cargo_isolated.sh test --lib --quiet row_security` passed.
- `scripts/cargo_isolated.sh check` passed.
- Latest rowsecurity regression result with the requested 120s timeout:
  `725/774` matched, `49` mismatches, `860` diff lines, `46` hunks. New diff
  copied to `/tmp/diffs/rowsecurity.diff`.
