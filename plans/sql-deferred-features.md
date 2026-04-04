# SQL / Executor / Parser — Deferred Features

This note records what is intentionally missing from the current SQL layer.
It supersedes the SQL section in `heap-deferred-features.md`, which was written
before many features were implemented.

The current code supports:

- `CREATE TABLE`, `DROP TABLE`
- `INSERT` (single and multi-row `VALUES`)
- `UPDATE` with `WHERE`
- `DELETE` with `WHERE`
- `SELECT` with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- `SELECT *` expansion
- `EXPLAIN` and `EXPLAIN (ANALYZE, BUFFERS)`
- `SHOW TABLES`
- inner joins (`JOIN ... ON`) and cross joins (`FROM a, b`)
- qualified column names (`table.column`)
- `GROUP BY`, `HAVING`
- aggregate functions: `COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`
- boolean operators: `AND`, `OR`, `NOT`
- comparison operators: `=`, `<`, `>`
- arithmetic: `+`, unary `-`
- null predicates: `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`,
  `IS NOT DISTINCT FROM`
- three scalar types: `INT4`, `TEXT`, `BOOL`
- durable catalog (schema persisted to disk)
- durable transaction status (persisted to disk)
- wire protocol server (psql-compatible)
- `TRUNCATE` and `TRUNCATE TABLE`
- `VACUUM` accepted as a no-op compatibility shim
- limited `COPY FROM STDIN` compatibility for pgbench-style data loading

## Data types

Only `INT4`, `TEXT`, and `BOOL` are supported. PostgreSQL has dozens of built-in
types. Key missing types include:

- `INT8` / `BIGINT`
- `INT2` / `SMALLINT`
- `FLOAT4` / `FLOAT8` / `REAL` / `DOUBLE PRECISION`
- `NUMERIC` / `DECIMAL`
- `VARCHAR(n)` / `CHAR(n)`
- `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`
- `BYTEA`
- `UUID`
- `JSON` / `JSONB`
- `ARRAY` types
- composite / row types
- enum types
- range types
- domain types

**To add:** Extend `ScalarType`, the parser's type resolution, the tuple
storage format, and the expression evaluator for each new type.

## Comparison and arithmetic operators

Only `=`, `<`, `>`, `+`, and unary `-` are supported. Missing operators include:

- `!=` / `<>`
- `<=`, `>=`
- binary `-`, `*`, `/`, `%`
- `||` (string concatenation)
- `LIKE`, `ILIKE`
- `SIMILAR TO`
- `~` (regex match)
- `BETWEEN`
- `IN (list)` and `IN (subquery)`
- `ANY` / `ALL`
- `CASE WHEN ... THEN ... ELSE ... END`
- `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`
- type cast (`::` and `CAST`)

## Subqueries

No subquery support of any kind:

- scalar subqueries in `SELECT` or `WHERE`
- `IN (SELECT ...)`
- `EXISTS (SELECT ...)`
- `ANY` / `ALL` with subquery
- derived tables / subqueries in `FROM`
- lateral joins
- correlated subqueries
- common table expressions (`WITH`)

## JOIN types

Only `INNER JOIN` and implicit cross join (`FROM a, b`) are supported.

- `LEFT JOIN` / `LEFT OUTER JOIN`
- `RIGHT JOIN` / `RIGHT OUTER JOIN`
- `FULL OUTER JOIN`
- `CROSS JOIN` (explicit syntax)
- `NATURAL JOIN`
- `USING` clause
- multi-way joins (more than two tables)
- self-joins

## Index access methods

No indexes exist. All scans are sequential scans. PostgreSQL supports:

- B-tree indexes
- Hash indexes
- GiST indexes
- GIN indexes
- BRIN indexes
- SP-GiST indexes

**To add:** An index AM interface, a B-tree implementation, `CREATE INDEX`
syntax, and an index scan plan node.

## Planner / optimizer

There is no cost-based optimizer. The planner always produces:
- sequential scan for table access
- nested loop for joins
- in-memory sort for `ORDER BY`

Missing planner features:

- cost model and statistics
- index scan selection
- merge join, hash join
- join reordering
- predicate pushdown
- projection pushdown
- common subexpression elimination
- plan caching

## ALTER TABLE

No schema modification after table creation. Missing DDL:

- `ALTER TABLE ADD COLUMN`
- `ALTER TABLE DROP COLUMN`
- `ALTER TABLE ALTER COLUMN TYPE`
- `ALTER TABLE RENAME`
- `ALTER TABLE ADD CONSTRAINT`
- `CREATE INDEX` / `DROP INDEX`
- `CREATE SCHEMA` / `DROP SCHEMA`
- `CREATE VIEW` / `DROP VIEW`
- `CREATE SEQUENCE` / `DROP SEQUENCE`

## Constraints

No constraint support:

- `PRIMARY KEY`
- `UNIQUE`
- `FOREIGN KEY` / `REFERENCES`
- `CHECK`
- `DEFAULT` values
- `NOT NULL` is parsed but not enforced at the storage level (only at the
  executor's encode-value step)

## INSERT ... SELECT

`INSERT INTO ... SELECT ...` is not supported. Only `INSERT INTO ... VALUES`
works.

## RETURNING clause

`INSERT ... RETURNING`, `UPDATE ... RETURNING`, and `DELETE ... RETURNING`
are not supported.

## Table and column aliases

`SELECT t.id FROM people AS t` and `SELECT id AS person_id FROM people`
are not supported. Column aliases in the select list are auto-generated
from the expression.

## UNION / INTERSECT / EXCEPT

Set operations are not supported.

## Expressions in target list

Only column references, constants, aggregate calls, and `+` are supported
in the target list. Function calls, `CASE`, casts, and complex expressions
are missing.

## Built-in functions

No built-in scalar functions: `length()`, `upper()`, `lower()`, `substring()`,
`now()`, `current_timestamp`, `pg_typeof()`, etc.

## pg_catalog system tables

Catalog metadata is stored in a simple file format, not in heap-backed system
tables. psql's `\d`, `\dt`, `\l`, and tab completion query `pg_catalog` tables
which do not exist, causing errors.

**To add:** Virtual tables or a synthetic response mechanism for common
`pg_catalog` queries (`pg_class`, `pg_attribute`, `pg_type`, `pg_namespace`,
`pg_database`).

## Transactions in SQL

`BEGIN`, `COMMIT`, `ROLLBACK` are not exposed as SQL commands. The `Database`
handle auto-commits DML statements. Users cannot run multi-statement
transactions.

**To add:** Parse `BEGIN`/`COMMIT`/`ROLLBACK`, maintain per-connection
transaction state, and adjust the `ReadyForQuery` status byte (`T` for in
transaction, `E` for failed transaction).

## VACUUM and dead space reclamation

`VACUUM` is accepted only as a compatibility no-op. Dead tuple versions from
updates and deletes are never reclaimed, and the heap grows monotonically.

**To add:** real `VACUUM` execution, tuple pruning during scans, free space map
integration, and eventually autovacuum.

## COPY

`COPY FROM STDIN` is not implemented as a real bulk-load path.

Current status:

- the wire protocol has enough copy-in support for pgbench-style client-side
  initialization
- incoming copy rows are buffered and then executed row-by-row as ordinary
  `INSERT INTO ... VALUES (...)` statements through the existing SQL/executor
  path

What is still missing:

- no direct tuple-loading path that bypasses SQL parsing/execution
- no efficient batched heap insert path
- no server-side `COPY TO`
- no binary `COPY`
- no general SQL-level `COPY` implementation beyond the narrow compatibility
  shim used by the wire server

**To add:** Parse and execute `COPY` as a first-class command, decode rows
directly into tuples, and insert them in bulk inside a dedicated load path
rather than reissuing one SQL insert per row.

## Sequences

`CREATE SEQUENCE`, `nextval()`, `currval()`, `SERIAL` / `BIGSERIAL` types
are not implemented.

## EXPLAIN output format

`EXPLAIN` produces a simplified plan tree. Missing details compared to
PostgreSQL:

- estimated vs actual cost
- row width estimates
- memory usage
- output columns per node
- `EXPLAIN (FORMAT JSON)` / `EXPLAIN (FORMAT YAML)` / `EXPLAIN (FORMAT XML)`

## Multi-table write operations

`UPDATE ... FROM`, `DELETE ... USING` (join-based writes) are not supported.

## ON CONFLICT (upsert)

`INSERT ... ON CONFLICT DO NOTHING` and `INSERT ... ON CONFLICT DO UPDATE`
are not supported.
