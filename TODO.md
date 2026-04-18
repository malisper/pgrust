# TODO

## Regression test files

Counts from `/tmp/pgrust_regress_todo_20260417` on 2026-04-17; `test_setup.sql` comes from `/tmp/pgrust_regress_test_setup_todo_20260417` with `--upstream-setup`.

Targeted reruns on 2026-04-17:

- numeric.sql: FAIL, 957/1057 queries matched from `/tmp/pgrust_numeric_regress_55433`
- numeric.sql first mismatch is unordered cross-join row order; substantive mismatches are `width_bucket(float8, ...)`, numeric display scale/rendering, and PostgreSQL-specific error text/detail for numeric overflow and numeric-to-int casts

- advisory_lock.sql: 8/38
- aggregates.sql: 215/583
- alter_generic.sql: 54/333
- alter_operator.sql: 3/65
- alter_table.sql: 433/1683
  - [done] expression indexes and `ALTER INDEX` operations used by `alter_table.sql`
  - partitioned tables, including `PARTITION OF`, `ATTACH PARTITION`, and `DETACH PARTITION`
  - `SET ROLE` / `RESET ROLE`
  - `ALTER VIEW` forms exercised by `alter_table.sql`
  - `ALTER TABLE` `NO INHERIT` / `INHERIT`
  - multi-action `ALTER TABLE` statements
  - `ALTER TABLE ... ALTER CONSTRAINT` deferrability changes
  - `ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT`
  - `ALTER TABLE ... ALTER COLUMN ... SET STORAGE`
  - `ALTER TABLE ... ALTER COLUMN ... SET STATISTICS`
  - `ALTER TABLE ... ALTER COLUMN ... SET (...)` / `RESET (...)`
  - `ALTER TABLE ... SET|RESET WITHOUT/WITH OIDS`
  - `ALTER TABLE ... SET SCHEMA`
  - `ALTER TABLE ... SET LOGGED` / `SET UNLOGGED`
  - `ALTER TABLE ... REPLICA IDENTITY USING INDEX`
  - `ALTER TABLE ... CLUSTER ON` / `SET WITHOUT CLUSTER`
  - `ALTER TABLE ONLY ...` variants beyond current support
  - `ALTER TABLE IF EXISTS ...` variants beyond current support
  - `CHECK ... NO INHERIT`
  - `CHECK ENFORCED` / `NOT ENFORCED`
  - foreign keys with `MATCH FULL`
  - deferrable foreign keys
  - `COMMENT ON COLUMN`
  - `COMMENT ON INDEX`
  - `COMMENT ON CONSTRAINT`
  - `CREATE RULE` / `DROP RULE`
  - `COPY ... TO STDOUT` forms used by `alter_table.sql`
  - `SELECT ... INTO`
  - typed tables: `CREATE TABLE OF`, `ALTER TABLE ... OF`, and `ALTER TABLE ... NOT OF`
  - enum creation forms required by `alter_table.sql`
  - domain constraints/defaults required by `alter_table.sql`
  - `CREATE OR REPLACE FUNCTION`
  - `DROP TABLE ... CASCADE`
  - `DROP TYPE ... CASCADE`
  - `CREATE TEMP TABLE ... PARTITION BY ...`
  - `regtype` support
  - `inet` and `cidr` type support
  - composite/type lookup support for user-defined and partitioned relation row types
  - `trigger` type support
  - `lseg` input syntax used by `alter_table.sql`
  - table rename handling for array-type name collisions in `pg_type`
- amutils.sql: 0/10
- arrays.sql: ERROR, 187/526 queries matched from `/tmp/pgrust_nested_arrays_regress_fresh`
  - [done] array subscript result names should inherit the base column name instead of defaulting to `?column?`
  - [done] array read semantics need executor fixes for slice/subscript edge cases
  - [done] widen verification for array read semantics against more `arrays.sql` slice/subscript cases
  - [done] array write semantics need slice-assignment fixes, especially for multidimensional arrays
  - [done] correct multidimensional slice shape/extent checks during assignment in `src/backend/commands/tablecmds.rs`
  - [done] PostgreSQL-compatible SQL-visible error text for array assignment/type mismatches instead of leaking raw internal `TypeMismatch` formatting
  - [done] preserve PostgreSQL's distinction between empty arrays and `NULL` results when slicing/subscripting array columns
  - [x] normalize SQL-visible errors for unsubscriptable and fixed-length array-like types such as `timestamp with time zone` and `point`
  - [x] fix scalar array assignment overflow/bounds handling that currently panics in `src/backend/commands/tablecmds.rs`
  - support `RETURNING` with subscripted array/fixed-length assignments used by `point_tbl`
  - [done] support `CREATE TEMP TABLE` column definitions with fixed-length array syntax like `integer ARRAY[4]`
  - [done] support deeper array constructors and nested `ARRAY[...]` expressions in `SELECT` targets and `INSERT` values
  - support `ARRAY(SELECT ...)` forms used later in `arrays.sql`
  - implement missing array builtins exercised by `arrays.sql` such as `array_append`, `array_prepend`, and `array_cat`
  - support row/composite array expressions and comparisons, including `ARRAY(SELECT ...)` and `array_agg(record) || array_agg(record)`
- async.sql: 0/11
- bit.sql: 74/132
- bitmapops.sql: 3/12
- boolean.sql: 46/98
- box.sql: 17/101
- brin.sql: 56/125
- brin_bloom.sql: 50/88
- brin_multi.sql: 67/220
- btree_index.sql: 22/133
- case.sql: 17/67
- char.sql: 12/32
- circle.sql: 1/22
- cluster.sql: 26/204
- collate.icu.utf8.sql: 0/588
- collate.linux.utf8.sql: 0/211
- collate.sql: 7/144
- collate.utf8.sql: 0/59
- collate.windows.win1252.sql: 0/182
- combocid.sql: 8/62
- comments.sql: 11/13
- compression.sql: 2/87
- constraints.sql: 30/565
- conversion.sql: 84/149
- copy.sql: 11/113
- copy2.sql: 48/215
- copydml.sql: 5/64
- copyencoding.sql: 7/17
- copyselect.sql: 0/21
- create_aggregate.sql: 0/59
- create_am.sql: 25/142
- create_cast.sql: 0/24
- create_function_c.sql: 0/5
- create_function_sql.sql: 22/180
- create_index.sql: 58/687
- create_index_spgist.sql: 14/202
- create_misc.sql: 0/88
- create_operator.sql: 38/99
- create_procedure.sql: 23/125
- create_role.sql: 95/144
- create_schema.sql: 2/27
- create_table.sql: 10/330
- create_table_like.sql: 3/152
- create_type.sql: 2/86
- create_view.sql: 14/311
- database.sql: 7/16
- date.sql: 117/271
- dbsize.sql: 0/25
- delete.sql: 0/10
- dependency.sql: 11/62
- domain.sql: 70/507
- drop_if_exists.sql: 14/161
- drop_operator.sql: 0/12
- encoding.sql: 0/133
- enum.sql: 16/172
- equivclass.sql: 20/96
- errors.sql: 13/87
- euc_kr.sql: 3/3
- event_trigger.sql: 64/281
- event_trigger_login.sql: 3/14
- explain.sql: 31/75
- expressions.sql: 21/81
- fast_default.sql: 27/296
- float4.sql: 57/100
- float8.sql: 131/184
- foreign_data.sql: 16/540
- foreign_key.sql: 78/1252
- functional_deps.sql: 0/40
- generated_stored.sql: 1/131
- generated_virtual.sql: 1/131
- geometry.sql: 5/162
- gin.sql: 21/71
- gist.sql: 6/62
- groupingsets.sql: 28/219
- guc.sql: 72/229
- hash_func.sql: 0/43
- hash_index.sql: 10/100
- hash_part.sql: 0/28
- horology.sql: 15/399
- identity.sql: 3/271
- incremental_sort.sql: 79/169
- index_including.sql: 4/135
- index_including_gist.sql: 4/50
- indexing.sql: 7/570
- indirect_toast.sql: 4/30
- inet.sql: 6/116
- infinite_recurse.sql: 0/3
- inherit.sql: 42/884
- init_privs.sql: 0/4
- insert.sql: 7/390
- insert_conflict.sql: 15/266
- int4.sql: 57/94
- int8.sql: 87/174
- interval.sql: 23/450
- join.sql: 110/918
- join_hash.sql: 162/315
- json.sql: 138/470
- json_encoding.sql: 23/44
- jsonb.sql: 577/1084
  - [done] PostgreSQL-compatible jsonb input errors with `LINE` / `DETAIL` / `CONTEXT`
  - [done] stack depth limit handling for deeply nested jsonb input
  - [done] aggregate-local `ORDER BY` support for `jsonb_agg` / `jsonb_object_agg`
  - [done] jsonb containment and existence builtin semantics (`@>`, `<@`, `?`, `?|`, `?&`, helper funcs)
  - [done] jsonb object/key construction semantics and SQL-visible errors (`jsonb_object_keys`, `jsonb_build_object`, `jsonb_object`, `jsonb_object_agg`)
  - jsonb subscripting semantics
  - record-expansion semantics for `jsonb_to_record` / `jsonb_populate_record`
- jsonb_jsonpath.sql: 188/830
- jsonpath.sql: 31/224
- jsonpath_encoding.sql: 0/32
- largeobject.sql: 27/129
- limit.sql: 3/80
- line.sql: 11/35
- lock.sql: 42/131
- lseg.sql: 16/16
- macaddr.sql: 0/35
- macaddr8.sql: 0/71
- maintain_every.sql: 5/16
- matview.sql: 18/185
- md5.sql: 14/14
- memoize.sql: 38/88
- merge.sql: 206/641
  1. `MERGE` grammar and AST support (`MERGE INTO`, `USING`, `WHEN MATCHED`, `WHEN NOT MATCHED`, `DO NOTHING`, `UPDATE`, `DELETE`, `INSERT`)
  2. `MERGE` binder/planner support, including PostgreSQL-compatible name resolution, branch validation, and `EXPLAIN` output
  3. `MERGE` executor support, including branch execution, permissions, and SQL-visible errors
  4. Table-object `GRANT`/`REVOKE` privilege parsing for forms used by `merge.sql` (`INSERT`, `UPDATE`, `DELETE` on tables)
  5. Data-modifying statement integration for `MERGE` in `WITH`/`COPY` contexts and the corresponding `RETURNING` validation errors
  6. Materialized view DDL/support checks needed by the `MERGE` unsupported-relation tests
- misc.sql: 0/61
- misc_functions.sql: 23/160
- misc_sanity.sql: 0/5
- money.sql: 53/109
- multirangetypes.sql: 23/605
- mvcc.sql: 11/17
- name.sql: 6/46
- namespace.sql: 14/45
- numa.sql: 0/3
- numeric.sql: 335/1057
- numeric_big.sql: 16/552
- numerology.sql: 3/92
- object_address.sql: 23/97
- oid.sql: 10/37
- oidjoins.sql: 29/30
- opr_sanity.sql: 0/131
- partition_aggregate.sql: 19/137
- partition_info.sql: 0/73
- partition_join.sql: 16/614
- partition_prune.sql: 68/750
- password.sql: 37/55
- path.sql: 4/23
- pg_lsn.sql: 0/31
- plancache.sql: 23/113
- plpgsql.sql: 1589/2271
  - typmod-aware `bpchar`/`char(n)` type resolution during index creation
  - explicit index opclass handling for `CREATE INDEX ... (col bpchar_ops)`
  - `trigger` pseudotype support in `CREATE FUNCTION ... RETURNS trigger`
  - `CREATE TRIGGER` parser, binder, catalog, and execution support
  - PL/pgSQL trigger runtime support for row triggers (`NEW`/`OLD`, trigger invocation)
- point.sql: 12/43
- polygon.sql: 15/62
- polymorphism.sql: 37/455
- portals.sql: 70/349
- portals_p2.sql: 2/41
- predicate.sql: 0/42
- prepare.sql: 0/33
- prepared_xacts.sql: 0/96
- privileges.sql: 303/1295
- psql.sql: 141/464
- psql_crosstab.sql: 13/35
- psql_pipeline.sql: 73/124
- publication.sql: 80/710
- random.sql: 47/73
- rangefuncs.sql: 24/437
- rangetypes.sql: 70/407
- regex.sql: 96/105
- regproc.sql: 0/105
- reindex_catalog.sql: 3/20
- reloptions.sql: 2/66
- replica_identity.sql: 0/66
- returning.sql: 9/150
- roleattributes.sql: 15/80
- rowsecurity.sql: 222/774
- rowtypes.sql: 12/241
- rules.sql: 17/626
- sanity_check.sql: 0/3
- security_label.sql: 7/28
- select.sql: 11/87
- select_distinct.sql: 50/105
- select_distinct_on.sql: 2/23
- select_having.sql: 0/23
- select_implicit.sql: 0/44
- select_into.sql: 8/70
- select_parallel.sql: 110/265
- select_views.sql: 6/52
- sequence.sql: 25/261
- spgist.sql: 1/31
- sqljson.sql: 3/221
- sqljson_jsontable.sql: 1/117
- sqljson_queryfuncs.sql: 5/314
- stats.sql: 103/479
- stats_ext.sql: 50/866
- stats_import.sql: 5/132
- strings.sql: 363/508
- subscription.sql: 37/158
- subselect.sql: 37/334
- sysviews.sql: 4/29
- tablesample.sql: 2/56
- tablespace.sql: 26/205
- temp.sql: 56/216
- test_setup.sql: 58/69
- text.sql: 57/73
- tid.sql: 0/41
- tidrangescan.sql: 4/45
- tidscan.sql: 9/49
- time.sql: 0/44
- timestamp.sql: 12/177
- timestamptz.sql: 41/404
- timetz.sql: 3/57
- transactions.sql: 78/439
- triggers.sql: 259/1262
- truncate.sql: 22/201
- tsdicts.sql: 0/131
- tsearch.sql: 43/464
- tsrf.sql: 8/74
- tstypes.sql: 69/238
- tuplesort.sql: 31/108
- txid.sql: 14/51

## strings.sql follow-up

- Raise a PostgreSQL-style syntax error for illegal string continuation when a comment appears between adjacent string literals across lines.
- [done] Fold unquoted identifiers consistently so `CHAR_TBL`, `VARCHAR_TBL`, and `TEXT_TBL` resolve like PostgreSQL in `strings.sql`.
- [done] Match PostgreSQL `bytea` input diagnostics for malformed hex and escape sequences, including `pg_input_error_info()` messages and SQLSTATEs.
- Tighten `SIMILAR TO` and `SUBSTRING ... SIMILAR` behavior for one-separator patterns, `ESCAPE NULL`, and PostgreSQL-compatible error text.
- [done] Add PostgreSQL `CONTEXT` output for the `SUBSTRING ... SIMILAR` too-many-separators error.
- [done] Implement `OVERLAY(text, text, integer[/FOR ...])` semantics to match PostgreSQL in `strings.sql`.

## JSONPath follow-ups

- Done: `jsonb_jsonpath.sql` and `jsonpath.sql` `@?` / `jsonb_path_exists` semantics now return `NULL` for silent evaluation errors and propagate non-silent strict errors.
- Done: jsonpath parser/runtime now supports index lists and computed subscripts for
  `$[0,1]`, `$[last - 1]`, `$[2.5 - 1 to $.size() - 2]`, and `$[last ? (...)]`.
- In progress: jsonpath expression/forms support.
  Done: `exists(...)`, `.size()`, `.type()`, and expression-level method chaining for
  `.abs()`, `.ceiling()`, and `.floor()`.
  Done: builtin item methods `.double()`, `.boolean()`, and `.string()`.
  Done: builtin numeric cast methods `.number()`, `.integer()`, and `.decimal(...)`.
  Done: builtin datetime-related methods `.bigint()`, `.date()`, `.time()`, `.time_tz()`,
  `.timestamp()`, `.timestamp_tz()`, and `.datetime()`.
  Done: `.datetime("template")` for the currently implemented PostgreSQL-style template subset,
  including `DD`, `MM`, `YYYY`, `HH24`, `MI`, `SS`, `TZH`, `TZM`, and quoted literals.
  Done: string predicate operators `starts with` and `like_regex ... flag ...`.
  Remaining: other currently-rejected valid jsonpath syntax.
- In progress: PostgreSQL lax-mode auto-unwrapping for array/scalar access.
  Done: `lax $[0]` on scalar values now matches upstream behavior.
  Done: `lax $[*]` on scalar values now matches upstream behavior.
  Remaining: related scalar/array unwrapping cases.
- Done: jsonpath three-valued predicate semantics now preserve `unknown` for comparisons, `is unknown`, and filter evaluation instead of reducing everything to Rust `bool`.
- Done: jsonpath comparison semantics for mixed types and multi-item sequences no longer incorrectly return `true` in strict comparisons.
- Done: recursive descent depth handling for `**` now includes the current item at depth `0`, matching `$.**`, `$.**{0}`, and `$.**{0 to last}`.
- Align jsonpath runtime error behavior and messages with PostgreSQL where possible, especially around structural errors, out-of-range subscripts, and numeric/arithmetic failures.
- type_sanity.sql: 0/63
- typed_table.sql: 1/32
- unicode.sql: 0/17
- union.sql: 75/197
- union.sql follow-up:
  - [done] accept PostgreSQL-style mixed set-operation chains such as `SELECT 1 UNION SELECT 2 UNION ALL SELECT 2` instead of rejecting them in the parser
  - [done] support `SELECT DISTINCT` in set-operation inputs such as `EXCEPT ALL SELECT DISTINCT ...`
  - [done] match PostgreSQL's `FOR NO KEY UPDATE` set-operation error text instead of routing it through the generic unsupported-feature wrapper
  - investigate why bootstrap fixture tables from `scripts/test_setup_pgrust.sql` like `float8_tbl`, `int8_tbl`, and `tenk1` are not consistently resolvable during regression runs
- updatable_views.sql: 109/1139
- update.sql: 28/300
- uuid.sql: 0/63
- vacuum.sql: 26/328
- vacuum_parallel.sql: 4/14
- varchar.sql: DONE
- window.sql: 7/388
- with.sql: 31/312
- without_overlaps.sql: 24/643
- write_parallel.sql: 6/22
- xid.sql: 14/88
- xml.sql: 15/281
- xmlmap.sql: 3/40

## Features

- [done] PostgreSQL-compatible jsonb input errors with `LINE` / `DETAIL` / `CONTEXT`
- [done] stack depth limit handling for deeply nested jsonb input
- [done] aggregate-local `ORDER BY` support for `jsonb_agg` / `jsonb_object_agg`
- [done] jsonb containment and existence builtin semantics (`@>`, `<@`, `?`, `?|`, `?&`, helper funcs)
- [done] jsonb object/key construction semantics and SQL-visible errors (`jsonb_object_keys`, `jsonb_build_object`, `jsonb_object`, `jsonb_object_agg`)
- jsonb subscripting semantics
- record-expansion semantics for `jsonb_to_record` / `jsonb_populate_record`

- partitioned tables, including `PARTITION OF`, `ATTACH PARTITION`, and `DETACH PARTITION`
- `SET ROLE` / `RESET ROLE`
- `ALTER VIEW` forms exercised by `alter_table.sql`
- `ALTER TABLE` `NO INHERIT` / `INHERIT`
- multi-action `ALTER TABLE` statements
- `ALTER TABLE ... ALTER CONSTRAINT` deferrability changes
- `ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT`
- `ALTER TABLE ... ALTER COLUMN ... SET STORAGE`
- `ALTER TABLE ... ALTER COLUMN ... SET STATISTICS`
- `ALTER TABLE ... ALTER COLUMN ... SET (...)` / `RESET (...)`
- `ALTER TABLE ... SET|RESET WITHOUT/WITH OIDS`
- `ALTER TABLE ... SET SCHEMA`
- `ALTER TABLE ... SET LOGGED` / `SET UNLOGGED`
- `ALTER TABLE ... REPLICA IDENTITY USING INDEX`
- `ALTER TABLE ... CLUSTER ON` / `SET WITHOUT CLUSTER`
- `ALTER TABLE ONLY ...` variants beyond current support
- `ALTER TABLE IF EXISTS ...` variants beyond current support
- `CHECK ... NO INHERIT`
- `CHECK ENFORCED` / `NOT ENFORCED`
- foreign keys with `MATCH FULL`
- deferrable foreign keys
- `COMMENT ON COLUMN`
- `COMMENT ON INDEX`
- `COMMENT ON CONSTRAINT`
- `CREATE RULE` / `DROP RULE`
- `COPY ... TO STDOUT` forms used by `alter_table.sql`
- `SELECT ... INTO`
- typed tables: `CREATE TABLE OF`, `ALTER TABLE ... OF`, and `ALTER TABLE ... NOT OF`
- enum creation forms required by `alter_table.sql`
- domain constraints/defaults required by `alter_table.sql`
- `CREATE OR REPLACE FUNCTION`
- `DROP TABLE ... CASCADE`
- `DROP TYPE ... CASCADE`
- `CREATE TEMP TABLE ... PARTITION BY ...`
- `regtype` support
- `inet` and `cidr` type support
- composite/type lookup support for user-defined and partitioned relation row types
- `trigger` type support
- `lseg` input syntax used by `alter_table.sql`
- table rename handling for array-type name collisions in `pg_type`

- plpgsql.sql
  - typmod-aware `bpchar`/`char(n)` type resolution during index creation
  - explicit index opclass handling for `CREATE INDEX ... (col bpchar_ops)`
  - `trigger` pseudotype support in `CREATE FUNCTION ... RETURNS trigger`
  - `CREATE TRIGGER` parser, binder, catalog, and execution support
  - PL/pgSQL trigger runtime support for row triggers (`NEW`/`OLD`, trigger invocation)

- test_setup.sql: emit PostgreSQL-compatible inheritance merge notices for multi-parent `INHERITS` merges like `stud_emp`
- test_setup.sql: done - support `CREATE TYPE ... AS ENUM`
- test_setup.sql: support `CREATE TYPE ... AS RANGE`
- test_setup.sql: extend `CREATE FUNCTION` parsing to accept unnamed arguments such as `binary_coercible(oid, oid)` and `fipshash(bytea)`
- test_setup.sql: extend `CREATE FUNCTION` parsing/catalog writes for routine attributes used by upstream setup: `STRICT`, `IMMUTABLE`, `STABLE`, `PARALLEL SAFE`, and `LEAKPROOF`
- test_setup.sql: support `LANGUAGE sql` functions in addition to the current `LANGUAGE plpgsql` path
- test_setup.sql: support SQL-standard function bodies like `RETURN substr(...)` without requiring `AS $$...$$`
- test_setup.sql: add either real `LANGUAGE C` function registration or a narrow compatibility shim for upstream `binary_coercible`
- test_setup.sql: support `CREATE OPERATOR CLASS` for the hash opclass forms used by upstream setup
- date.sql:
  make ambiguous date input parsing respect PostgreSQL `DateStyle` semantics across `YMD`, `DMY`, and `MDY`
- date.sql:
  reject ambiguous slash-, dash-, and space-separated date forms that PostgreSQL rejects, and parse accepted forms with PostgreSQL-compatible field ordering and two-digit year rules
- date.sql:
  [done] tighten named-month and BC-date acceptance rules to match PostgreSQL for forms like `99-Jan-08`, `08-Jan-99`, `99-08-Jan`, and `January 8, 99 BC`
- date.sql:
  [done] preserve PostgreSQL-style `LINE`/caret context for date input range and syntax errors in simple-query output
- date.sql:
  make `EXTRACT(... FROM date)` use PostgreSQL-compatible default column labels and unsupported-unit diagnostics
- date.sql:
  support `date_trunc(text, timestamp)` and match PostgreSQL `date_trunc` output semantics for date and timestamp inputs
- date.sql:
  fix `make_date` / `make_time` SQL-visible behavior, including overflow handling and proper error messages for invalid arguments

- stats.sql
  - [done] Teach `SHOW`/stats GUC handling to return PostgreSQL-like values for `track_counts`, `track_functions`, and `stats_fetch_consistency` instead of the generic `"default"` fallback.
  - Add parser support for transaction savepoint statements: `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT`.
  - [done] Add SQL-visible stats relations/views needed by the test: `pg_stat_io`, `pg_stat_user_tables`, `pg_statio_user_tables`, and `pg_stat_user_functions`.
  - [done] Implement the builtin stats functions used by the test, including `pg_stat_force_next_flush()`, `pg_stat_get_snapshot_timestamp()`, function-call stats accessors, and relation tuple/block counters.
  - Add runtime tracking and transactional accounting for relation stats: seq/index scan counts, tuple read/fetch counts, block read/hit counts, tuple insert/update/delete counts, live/dead tuple counts, and `TRUNCATE` effects across commit/rollback/savepoint boundaries.
  - Add runtime tracking for function execution stats, including xact-local counters and correct visibility/drop behavior across rollback, subtransactions, and committed drops.
  - [done] Add support for SQL-visible object lookup/types used by the file’s function-stat queries: `void`, `regprocedure`, and the corresponding cast/lookup path for `'func()'::regprocedure::oid`.
  - Extend SELECT/binder support enough for the early stats queries, including querying the new stats relations and handling `ORDER BY ... COLLATE "C"` on projected text columns.
- numeric.sql:
  Retest source: `/tmp/pgrust_numeric_regress_55433/diff/numeric.diff`
- Preserve PostgreSQL-compatible row order for the unordered `WITH v AS (VALUES ...) FROM v1, v2` cross-join cases in `numeric.sql`, or otherwise make the planner/executor match upstream join/input ordering closely enough for regression parity
- [x] Remaining `to_number(...)` / Roman numeral formatting and validation parity in `expr_format.rs`, including `V`-scaled output, `RN` aggregate validation, and PostgreSQL-style `DETAIL` / caret output for invalid Roman formats
- [x] Fix `width_bucket(float8, low, high, count)` boundary behavior for huge ranges; current float math can round into bucket `count + 1` or the wrong descending bucket near the upper edge
- [x] Make `to_char(numeric, ...)` formatting match PostgreSQL more closely when the input numeric carries excess display scale
- [x] Add PostgreSQL-style `DETAIL` output for numeric typmod overflow, including fractional-only numerics and infinite values rejected by typmod constraints
- [x] Add dedicated numeric-to-integer cast errors for `NaN` and `Infinity` instead of collapsing them into generic `smallint/integer/bigint out of range`
- [x] Audit the remaining `numeric.sql` formatting mismatches after the display-scale fix; many later hunks appear to be the same root cause repeated across `to_char` cases
- [x] Mixed set-operation chains: accept PostgreSQL-style left-associative chains such as `SELECT 1 UNION SELECT 2 UNION ALL SELECT 2` instead of rejecting them in the parser.
- Shared regression fixture visibility: investigate why bootstrap tables from `scripts/test_setup_pgrust.sql` like `float8_tbl`, `int8_tbl`, and `tenk1` are not consistently resolvable during regression runs.
- privileges.sql parity:
  - expose privilege-related system catalogs in SQL, including `pg_auth_members` and `pg_largeobject_metadata`
  - [done] add parser/analyzer support for role membership `GRANTED BY`
  - add parser/analyzer/executor support for `CASCADE` in role membership revokes
  - implement `SET ROLE` and `RESET ROLE`
  - implement SQL-visible `session_user`, `current_user`, and `current_role` semantics used by the regression
  - add parser/executor support for `DROP OWNED`
  - add parser support for `DROP USER`, `CREATE GROUP`, and `ALTER GROUP`
  - make role membership grant/revoke execution honor explicit grantors and dependent membership chains
  - align duplicate-role and role-grant error text with PostgreSQL where practical

## DONE

- expression indexes and `ALTER INDEX` operations used by `alter_table.sql`
- `ALTER TABLE ... RENAME CONSTRAINT`
- int2.sql
- [done] Establish a working `numeric.sql` rerun path on current `HEAD`; `run_regression.sh` can false-fail startup because cluster bootstrap fsync work can delay TCP bind past the default readiness wait
- [done] Normalize numeric display scale before result rendering so aggregates and scalar outputs do not keep extra trailing zeros; this affects `AVG(val)` output and many `to_char(numeric, ...)` cases
