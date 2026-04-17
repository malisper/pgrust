# Serial / Sequence Deferred Follow-ups

`pgrust` now supports `smallserial` / `serial` / `bigserial`, standalone
sequence DDL, `nextval` / `currval` / `setval`, sequence scans in `FROM`, and
`ALTER TABLE ... ADD COLUMN serial` with backfill for existing rows.

The following pieces are still deferred:

## Real `pg_sequence` Catalog Rows

Sequence structure is still stored in the runtime sequence subsystem rather than
surfaced as a first-class `pg_sequence` catalog relation.

Why this is deferred:
- the current serial/sequence work needed durable runtime state and SQL-visible
  behavior first
- wiring a real system catalog adds loader, relcache, rowcodec, bootstrap, and
  `psql` describe surface area beyond the core feature path

## Full Dependency Semantics

Sequence ownership/default behavior is still modeled pragmatically from
`default_sequence_oid` instead of complete PostgreSQL-style dependency rows for
both owned-by and default references.

Why this is deferred:
- the current implementation already supports the operational cases needed by
  `DROP SEQUENCE ... RESTRICT/CASCADE`, implicit serial cleanup, and
  `pg_get_serial_sequence(...)`
- matching PostgreSQL exactly requires broader `pg_depend` modeling and more
  utility-command plumbing than the initial sequence milestone needed

## `psql` / Default Pretty-Printing Fidelity

Serial defaults are stored and executed correctly, but the remaining `psql`
compatibility work is still deferred:

- pretty-printing OID-backed serial defaults as
  `nextval('schema.seq'::regclass)`
- broader `\d` / describe coverage that would naturally consume the missing
  `pg_sequence` metadata

Why this is deferred:
- it depends on the same catalog-shape work as real `pg_sequence`
- it is compatibility polish rather than a blocker for SQL behavior

## PostgreSQL Sequence Features Still Out of Scope

The current sequence work still does not implement:

- identity columns
- `lastval()`
- `DISCARD SEQUENCES`
- `ALTER SEQUENCE ... SET SCHEMA`
- `ALTER SEQUENCE ... AS`
- PostgreSQL-style cached allocation semantics beyond parsing/storing `CACHE`

Why this is deferred:
- these are follow-on PostgreSQL compatibility features, not prerequisites for
  first-class serial columns and basic sequences
- they each pull in separate catalog, parser, or session-state work that is
  easier to land after the base sequence path is stable
