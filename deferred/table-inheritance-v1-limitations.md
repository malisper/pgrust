# Table Inheritance v1 Limitations

The v1 inheritance work intentionally matches PostgreSQL most closely in catalog shape, `CREATE TABLE ... INHERITS`, inherited `SELECT`, `ONLY` in `FROM`, and inherited `ANALYZE`.

The following pieces are still deferred:

## Recursive DML on Parents

Parent-targeted `UPDATE`, `DELETE`, and `TRUNCATE` are rejected when the target has inherited children.

Why this is deferred:
- PostgreSQL routes these through the same inheritance expansion machinery used by utility execution and partitioning.
- `pgrust` has the read-path appendrel architecture now, but not the full recursive write-path semantics or `ONLY` handling on DML targets.

## Recursive Column ALTER TABLE

`ALTER TABLE ADD COLUMN`, `DROP COLUMN`, and `RENAME COLUMN` are rejected on inheritance tree members.

Why this is deferred:
- PostgreSQL propagates these changes across the tree while maintaining `attinhcount`, `attislocal`, and dependent metadata.
- v1 keeps catalog state correct for creation time and scanning, but does not yet implement recursive column-shape mutations.

## DROP TABLE CASCADE / RESTRICT for Inheritance Trees

Dropping a parent table with inherited children errors with a "requires CASCADE, not supported yet" message.

Why this is deferred:
- PostgreSQL uses dependency-driven `DROP ... RESTRICT/CASCADE` behavior here.
- `pgrust` records the right dependency edges, but does not yet expose the full user-visible drop semantics for inheritance trees.

## Append Ordering / Pathkeys

Inherited scans use `Append`, but v1 does not preserve per-child ordering through the appendrel.

Current behavior:
- Child scans can use their own indexes and filters.
- If a query needs ordering above an inherited scan, the planner adds a sort above `Append`.

Why this is deferred:
- PostgreSQL has more sophisticated append pathkey handling and related planning optimizations.

## Partitioning-Specific Semantics

The catalog surface now includes the inheritance pieces partitioning will need, but declarative partitioning itself is not implemented.

Still deferred:
- partition bounds and pruning
- partition-specific dependency semantics
- `relispartition` and `inhdetachpending` behavior beyond stored `false` values
- executor/planner partition optimizations beyond generic inherited `Append`

## Scope of v1 ANALYZE

`ANALYZE parent` now records both root-only and inherited statistics rows, and `ANALYZE ONLY parent` records only the root-only row.

Still deferred:
- any PostgreSQL follow-on work that depends on partition-aware analyze behavior rather than generic inheritance-tree sampling
