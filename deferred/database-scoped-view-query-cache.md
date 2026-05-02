## Context

View creation stores the analyzed `_RETURN` query in an in-memory cache so later
view expansion can reuse the already-bound `Query` instead of reparsing the
serialized rule action.

The current compatibility fix scopes the process-global cache key by a
database-specific identity plus `pg_rewrite.oid`. That prevents parallel test
databases from colliding when they independently reuse the same rewrite OIDs.

This is still a transitional shape. The cache itself is not logically global:
it belongs to a specific database/catalog instance and should follow that
instance's lifetime.

## Goal

Move stored view query caching onto the owning database/catalog state instead of
keeping a process-global cache in `backend::rewrite::views`.

## Likely Approaches

- add a `stored_view_queries` map to `DatabaseShared`, `ClusterShared`, or the
  catalog/cache state that owns view/rule metadata
- key the map by `pg_rewrite.oid` within that database-owned state
- register cached queries through the same path that inserts/replaces
  `pg_rewrite` rows
- have `load_view_return_query` fetch cached queries through `CatalogLookup` or
  a concrete catalog/cache handle instead of consulting static state
- drop the static `OnceLock<RwLock<...>>` once all callers can access the
  database-owned cache

## Why Deferred

The scoped global key fixes the observed parallel-test correctness bug without a
larger API change. Moving the cache into database-owned state is the cleaner
long-term design, but it requires threading database/cache ownership through
rewrite/analyzer call paths that currently accept only `&dyn CatalogLookup`.
