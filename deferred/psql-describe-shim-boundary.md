## Context

`pgrust` now answers more `psql` `\d` describe queries from visible catalog
state, but [src/backend/tcop/postgres.rs](src/backend/tcop/postgres.rs:528)
still contains a narrow describe shim for catalog-heavy query shapes.

The remaining shim exists because the main SQL engine still does not natively
cover pieces of PostgreSQL’s hidden `psql` describe workload, including:

- `LEFT JOIN` shapes used by describe queries
- `format_type(...)`
- regex operators used for pattern matching
- parts of collation/describe formatting behavior
- publication / inheritance footer queries

## Goal

Shrink and eventually remove the `psql` describe shim so `\d`-style metadata
queries run through the normal SQL/catalog pipeline.

## Likely Approaches

- continue moving shim outputs onto real visible catalog metadata first
- add the minimum missing SQL-engine features needed by the hidden `psql`
  describe queries
- delete special-case query recognizers once the native SQL path can answer the
  corresponding shapes directly

## Why Deferred

The shim is already narrow and targeted. Removing it entirely requires SQL
engine work that is broader than the core catalog milestone itself.
