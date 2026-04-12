## Context

Permanent catalog publication now uses first-class stored `pg_constraint` and
`pg_depend` rows.

Temp catalog materialization still synthesizes those rows on the fly in
[src/pgrust/database.rs](src/pgrust/database.rs:472)
when building the visible temp overlay.

That means permanent and temp paths do not yet use the same source-of-truth
shape for constraint/dependency metadata.

## Goal

Make temp catalog overlays follow the same first-class row model as the
permanent catalog paths where practical.

## Likely Approaches

- store temp constraint/dependency rows explicitly alongside temp relations
  instead of re-deriving them during overlay sync
- route temp overlay materialization through a shared helper that consumes
  already-materialized `PhysicalCatalogRows`
- keep the per-session temp materialization model, but reduce duplicated
  derivation logic between temp and permanent paths

## Why Deferred

The current temp overlay behavior is correct enough for the supported temp-table
surface. This is mostly consistency cleanup and becomes more valuable as temp
schema behavior grows closer to PostgreSQL.
