Context
The first `jsonb` slice is read-focused: extraction, comparison, containment, existence, builders, and aggregates.

Deferred
- mutation operators/functions like `||`, `jsonb_set`, `jsonb_insert`, `#-`
- jsonpath operators and functions
- subscripting updates

Why Deferred
These are a separate semantic block with a lot of edge cases and do not materially affect the early regression unlock from basic `jsonb`.

Likely Approach
Build them on top of the existing internal `JsonbValue` tree utilities once the core read/query surface is stable.
