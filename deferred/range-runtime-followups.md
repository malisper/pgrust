## Context

The range runtime now uses catalog-backed range descriptors for both builtin and
user-defined single-range types. `CREATE TYPE ... AS RANGE` works through the
generic runtime path instead of a builtin-only `RangeTypeId` model.

## Deferred

- first-class multirange types and runtime semantics for builtin and user-defined
  ranges
- `anyrange` / `anymultirange` polymorphic resolution in function/operator lookup
- generic range proc/operator coverage needed to move `rangetypes.sql`,
  `rangefuncs.sql`, and `multirangetypes.sql`

## Why Deferred

The single-range runtime refactor is complete enough to unblock real
catalog-backed range behavior, but multiranges and polymorphic range SQL are a
separate semantic expansion with their own parser, binder, executor, storage,
and catalog surface.

## Likely Approach

- add multirange `pg_type` rows and runtime values alongside the existing range
  descriptors
- teach function resolution about `anyrange` / `anymultirange` instead of
  continuing to rely on concrete builtin signatures
- drive the remaining work directly against `rangetypes.sql`,
  `rangefuncs.sql`, and `multirangetypes.sql`
