## Context

The catalog migration now keeps common parse, plan, execute, relation lookup,
and describe paths off broad `CatCache`/`Catalog` materialization.

Many command modules still call `backend_catcache()` or `CatalogStore::catcache()`
directly. Some of those broad reads are legitimate temporary compatibility paths,
especially dependency traversal and command code that still syncs full catalog
rows, but many only need one relation, one catalog tuple, or one keyed row set.

Current high-value areas include:

- role, privilege, owner, and session-authorization commands
- foreign data wrapper, server, user mapping, and table option commands
- publication and text-search commands
- type, collation, operator, conversion, and statistics commands
- drop/dependency handling where broad traversal should be made explicit

## Goal

Move command-facing catalog reads onto keyed syscache and per-relation relcache
helpers where they only need targeted metadata. Leave broad materialization only
for real dependency walks, bootstrap/template-copy, tests, catalog views, and
temporary compatibility bridges.

## Likely Approaches

- add command-oriented keyed helpers around `SearchSysCache*`,
  `SearchSysCacheList*`, and `RelationIdGetRelation`
- convert command modules one domain at a time so behavior remains easy to test
- annotate remaining broad paths with `:HACK:` and the intended keyed
  replacement, or document why the path is intentionally broad
- extend the broad-cache test counters to cover common DDL flows that should not
  materialize full catalog state
- keep system catalog SQL view row producers broad until the SQL engine can
  answer those views through normal keyed metadata paths

## Why Deferred

The hot read paths are already guarded against accidental broad materialization.
Command conversion is still important for PostgreSQL-shaped catalog behavior and
startup/test latency, but it is a broader mechanical cleanup across many command
modules rather than a blocker for the main catalog-cache migration.
