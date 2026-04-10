Context
`jsonb` support now covers scalar values, arrays, extraction, builders, SRFs in `FROM`, and `jsonb_agg` over non-row inputs.

Deferred
- `row_to_json` / `to_jsonb(record)`
- row/composite `jsonb_agg(q)`
- record expansion helpers like `jsonb_to_record*` and `jsonb_populate_record*`

Why Deferred
These require composite/record plumbing the engine does not have yet, and they are separable from the core `jsonb` query surface.

Likely Approach
Add a first-class row/composite runtime representation, then teach JSON conversion and aggregate code to serialize it.
