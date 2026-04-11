`pg_class` is currently special-cased in `src/backend/parser/analyze/scope.rs`.

Current behavior:
- `FROM pg_class` does not bind through the catalog like normal relations.
- It lowers to a synthetic one-row `Plan::Values` containing only:
  - `oid = 1259`
  - `relname = 'pg_class'`

Why this exists:
- It was added as a narrow regression fix for `int8.sql`.
- The codebase does not yet have a general system-catalog or metadata-relation layer.

Why this is deferred:
- This should eventually be replaced by a real mechanism for exposing system relations and their metadata through normal binding/planning paths.
- The current special case is intentionally minimal and should not be expanded into an ad hoc catalog framework.
