Context
The engine now supports `json_build_array`, `json_build_object`, `json_object`, `json_agg`, `json_object_agg`, `jsonb_build_array`, `jsonb_build_object`, `jsonb_agg`, and `jsonb_object_agg` for the basic scalar/array/json inputs currently exercised by the regression suite.

Deferred
- `VARIADIC` builder signatures like `json_build_array(VARIADIC text[])` and `jsonb_build_object(VARIADIC int[])`
- the broader PostgreSQL `json_object(...)` array and multidimensional-array surface beyond the currently supported one-array and two-array forms
- row/composite arguments to builder functions and object aggregates
- exact `NULL` key behavior polish for `json_build_object`, `json_object`, `json_object_agg`, and `jsonb_object_agg`
- exact duplicate-key behavior polish for plain `json` vs canonicalized `jsonb` object construction and aggregation
- `json_strip_nulls` / `jsonb_strip_nulls`

Why Deferred
The current builder and object-aggregate surface is enough to unlock the core regression cases. The remaining gaps require extra parser work (`VARIADIC`), row/composite support, or PostgreSQL-compatibility edge-case cleanup.

Likely Approach
Extend the builtin function registry and aggregate plumbing used by the current JSON/JSONB functions, then add dedicated parser support for `VARIADIC` function arguments and composite/row JSON conversion.
