This note records the remaining PostgreSQL-vs-`pgrust` catalog differences after the recent core-catalog refactor.

What is already PostgreSQL-shaped:
- `pgrust` now uses cluster-level `global/pg_control`, not a catalog-specific sidecar file.
- `pg_namespace`, `pg_class`, `pg_attribute`, and `pg_type` have physical heap relfiles.
- relcache/catcache can rebuild from those physical core catalog heaps.
- normal SQL can read the core catalog relations instead of going through the old synthetic `pg_class` special case.

Remaining differences from PostgreSQL:
- Catalog breadth:
  - PostgreSQL uses many more system catalogs as first-class data (`pg_proc`, `pg_cast`, operators, auth/dependency catalogs, etc.).
  - `pgrust` only has the core relation/type catalogs in this shape today.
- Control file scope:
  - PostgreSQL `global/pg_control` stores cluster identity, checkpoint/WAL state, and compatibility-critical control metadata.
  - `pgrust` `global/pg_control` is intentionally much smaller and currently only tracks bootstrap state plus OID/relfilenode allocation.
- Bootstrap mechanism:
  - PostgreSQL bootstraps catalogs from generated catalog metadata (`.dat`/`genbki`) and bootstrap descriptors.
  - `pgrust` uses declarative Rust catalog definitions under `src/include/catalog/*`.
- Catalog write path:
  - PostgreSQL updates catalog tuples directly and transactionally.
  - `pgrust` still round-trips through a compatibility `Catalog` snapshot and rewrites the physical core catalog heaps from derived row sets.
- Runtime metadata shape:
  - PostgreSQL does not have a direct equivalent of `Catalog` / `CatalogEntry`; the source of truth is catalog tuples plus relcache/catcache/syscache.
  - `pgrust` still carries `Catalog` / `CatalogEntry` as compatibility types for store mutation paths, some helper APIs, and tests.
- Cache/index machinery:
  - PostgreSQL relies heavily on system-catalog indexes plus syscache/invalidation machinery.
  - `pgrust` cache warming still depends on scanning the physical core catalogs; there are no physical system-catalog indexes yet.
- Temp catalog visibility:
  - PostgreSQL integrates temp namespaces and temp catalog visibility directly into its namespace/catalog machinery.
  - `pgrust` projects temp catalog rows into session-specific core catalog heaps so catalog queries see them, which is simpler and more ad hoc.
- Catalog-driven semantics:
  - PostgreSQL resolves functions, casts, operators, and much of type behavior through catalogs.
  - `pgrust` still hardcodes most of that logic outside the catalog layer.
- Transactional behavior:
  - PostgreSQL catalog updates participate in full MVCC/WAL/checkpoint semantics.
  - `pgrust` durable catalog storage is much simpler and does not yet match PostgreSQL’s transactional durability model.

Why this matters:
- The durable source of truth is now much closer to PostgreSQL, but the implementation still mixes two models:
  - physical core catalogs for persisted data and query visibility
  - compatibility `Catalog` snapshots for mutation and some internal APIs
- The biggest remaining gap is not catalog visibility anymore; it is making catalog tuples, relcache, and catcache the only real metadata path.

Preferred follow-up:
- remove `Catalog` / `CatalogEntry` from production mutation paths and replace them with direct catalog-tuple mutation helpers
- stop rebuilding core catalog heaps from derived row sets and instead update the relevant catalog relations incrementally
- add more catalog breadth only after the write path matches the read path more closely
- eventually move hardcoded function/cast/operator lookup onto real catalogs
