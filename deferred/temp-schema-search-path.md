## Context

The first temp-table slice only supports `pg_temp` for table creation and lookup.
It does not implement full temporary-schema behavior or `search_path` interaction.
It also uses a synthetic per-client temp database OID in pgrust
(`TEMP_DB_OID_BASE`) as an internal shortcut.

PostgreSQL does not appear to model temp tables that way. Upstream keeps temp
objects in the current real database and gives each backend its own temp
namespace (`pg_temp_<procnum>`), with `pg_temp` acting as an alias to that
session-local namespace.

## Goal

Make temp schemas behave more like PostgreSQL:

- per-session temp tables backed by a real temp namespace (`pg_temp_<procnum>`)
- real `pg_temp` namespace semantics
- `search_path` resolution rules
- schema-qualified temp lookup beyond simple table names
- temp namespace visibility through `SHOW search_path`, `SET search_path`, and related paths

## Likely Approaches

- add explicit namespace objects instead of table-name normalization
- create and track a session-local temp namespace object, then attach temp relations to it instead of using the current synthetic temp-database shortcut
- model `pg_temp` as a per-session schema entry in catalog lookup
- resolve unqualified names through a search-path list rather than the current overlay shortcut
- remove reliance on synthetic temp database OIDs once temp namespaces are modeled more directly

## Why Deferred

The regression-focused temp-table slice only needs session-local tables and masking.
Full temp schema behavior is broader and tightly coupled to future namespace and `search_path` work.
