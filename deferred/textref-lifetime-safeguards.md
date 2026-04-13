## Context

`TextRef` is a useful executor optimization, but the current type does not
encode its lifetime. It is easy to clone a borrowed `Value::TextRef` out of a
pinned heap-backed slot and accidentally store it in detached executor state.

That already caused real bugs in join/sort caches, correlated-subquery
`outer_rows`, `ProjectSet`, and scalar SRF/subquery outputs.

## Goal

Add guardrails so borrowed `TextRef` values cannot silently escape the contexts
where they are valid.

## Likely Approaches

- audit all executor paths that turn heap-backed rows into detached virtual rows
  and centralize the materialization boundary
- add a helper for “clone row for detached storage” that always materializes
  borrowed values, and stop open-coding `slot.values()?.iter().cloned()`
- add debug assertions or targeted tests that exercise join, sort, subquery,
  and SRF caching with borrowed text inputs
- consider a stronger type split between borrowed slot-local values and owned
  values if the current `Value` enum keeps making these lifetime mistakes easy

## Why Deferred

The specific known escape paths are fixed, and the library suite is green. The
remaining work is structural hardening to prevent regressions, not a blocker
for current correctness.
