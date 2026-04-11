## Context

The planner/analyzer currently accepts `&dyn CatalogLookup` and supports both
direct lookup from `Catalog` and lookup through `RelCache`.

That keeps the API flexible, but it also means the planning layer is written
against two metadata paths at once:

- `CatalogLookup for Catalog`
- `CatalogLookup for RelCache`

In practice, this blurs the intended boundary between the source-of-truth
catalog representation and the cached relation-lookup representation.

## Goal

Converge on a single concrete catalog-lookup path for planning and analysis
instead of supporting both direct `Catalog` lookup and `RelCache` lookup.

## Likely Approaches

- pick the canonical planning input type explicitly, most likely `RelCache` or a
  concrete wrapper around it
- remove `CatalogLookup for Catalog` once callers are routed through the chosen
  concrete lookup structure
- replace `&dyn CatalogLookup` planner entry points with concrete references so
  the planner API reflects the real dependency
- keep `Catalog` as the source of truth, but require callers to materialize the
  concrete lookup form before binding/planning

## Why Deferred

The current trait-based API is working and keeps refactors moving, but it
preserves ambiguity about which metadata layer is canonical for planning.
Cleaning this up is worthwhile, but it is a structural follow-up rather than an
immediate correctness issue.
