`catalog/control` was introduced as a local metadata sidecar for the in-progress catalog work.

What it currently stores:
- catalog format version
- next OID to allocate
- bootstrap-complete flag

How this differs from PostgreSQL:
- PostgreSQL does have a control file, but it is cluster-wide `global/pg_control`, not a catalog-specific side file.
- `pg_control` stores cluster/bootstrap/WAL/checkpoint compatibility state, not per-feature catalog metadata.
- PostgreSQL system catalogs are normally bootstrapped from fixed catalog definitions and then persisted as actual catalog relations.
- PostgreSQL does not keep a separate `catalog/control` file as the source of truth for relation/type catalog state.

Why this matters:
- If `pgrust` is moving toward PostgreSQL-shaped real catalog support, this extra file is the wrong long-term shape.
- The source of truth should become the bootstrapped catalog relations themselves, with only genuinely cluster-level control metadata living in a control file.

Preferred follow-up:
- remove `catalog/control`
- stop carrying catalog-specific versioning/upgrade logic there
- bootstrap fresh catalog relations directly and derive ongoing state from them
