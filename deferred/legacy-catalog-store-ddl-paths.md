## Context

Permanent runtime DDL now goes through the MVCC catalog path in
`CatalogStore::{create_table_mvcc, create_index_for_relation_mvcc, drop_relation_by_oid_mvcc}`.

The older non-MVCC helpers in [src/backend/catalog/store.rs](/src/backend/catalog/store.rs:213)
still exist:

- `create_table`
- `create_index`
- `drop_table`
- `drop_relation_by_oid`

They are still used by local tools and tests, especially `query_repl` and
catalog/cache unit tests.

## Goal

Make the remaining `CatalogStore` DDL surface clearly bootstrap/test-only, or
remove it entirely once local tooling can use the MVCC path too.

## Likely Approaches

- keep a narrow bootstrap/rebuild API and stop exposing general-purpose
  non-MVCC create/drop helpers
- move local-tool callers onto the MVCC path with a lightweight transaction
  wrapper
- update tests that currently rely on direct non-MVCC mutation so they either
  use the MVCC helpers or explicitly opt into a bootstrap-style path
- document the remaining non-MVCC helpers as bootstrap-only if they must stay

## Why Deferred

This is mostly an API/cleanup follow-up now that runtime correctness uses the
MVCC path. Removing the legacy helpers is worthwhile, but it is not blocking
catalog behavior parity for normal SQL execution.
