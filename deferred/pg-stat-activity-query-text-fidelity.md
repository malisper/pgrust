## Context

`pgrust` now exposes a builtin `pg_stat_activity` view backed by in-memory
session activity tracking.

The `query` column is operationally useful, but it is not yet a strict
byte-for-byte mirror of PostgreSQL's `pg_stat_activity.query`.

In particular, the stored query text currently comes from the execution path
after the existing regression SQL rewrite step in the cases where that rewrite
is applied.

## Goal

Make `pg_stat_activity.query` reflect the original client-submitted SQL text as
closely as PostgreSQL does, rather than a post-rewrite execution string.

## Likely Approaches

- capture and retain the original statement text before any regression-compat
  rewrites are applied
- thread both original and rewritten SQL through execution paths that currently
  mutate statement text in place
- keep `pg_stat_activity` reporting the original SQL while executor/planner
  paths continue using rewritten SQL internally when needed

## Why Deferred

The current behavior is good enough for debugging stuck or active queries, which
was the immediate need. Matching PostgreSQL's query-text fidelity is a useful
follow-up, but it is not required for the activity view to be operationally
valuable today.
