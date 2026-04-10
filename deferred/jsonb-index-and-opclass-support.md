Context
`jsonb` values are queryable now, but there is no index support or operator class work.

Deferred
- GIN/GiST/hash/btree opclass behavior for `jsonb`
- planner/index integration for containment and existence

Why Deferred
Index support is much larger than plain type/runtime compatibility and depends on broader index/planner work that is still incomplete elsewhere in the engine.

Likely Approach
Add stable binary comparator and hashing hooks first, then implement opclasses once index infrastructure is broad enough.
