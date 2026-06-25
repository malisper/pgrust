# Audit: backend-catalog-pg-largeobject

C source: `src/backend/catalog/pg_largeobject.c` (PG 18.3). Independently
re-derived from the C, the c2rust rendering
(`c2rust-runs/backend-catalog-pg-largeobject/src/pg_largeobject.rs`), and the
catalog headers `pg_largeobject.h` / `pg_largeobject_metadata.h`.

## Function inventory

The TU has exactly four functions and no file-static / inline helpers
(confirmed in both the C and the c2rust run).

| Function | C location | Port location | Verdict | Notes |
|---|---|---|---|---|
| `LargeObjectCreate` | pg_largeobject.c:35-88 | lib.rs `LargeObjectCreate` | MATCH | `table_open(LargeObjectMetadataRelationId, RowExclusiveLock)`; `OidIsValid(loid) ? loid : GetNewOidWithIndex(rel, LargeObjectMetadataOidIndexId, Anum_pg_largeobject_metadata_oid)` (direct call into merged `backend-catalog-catalog`); `GetUserId()`; `get_user_default_acl(OBJECT_LARGEOBJECT, ownerId, InvalidOid)`; the `values[]`/`nulls[]` + `heap_form_tuple` + `CatalogTupleInsert` + `heap_freetuple` value layer SEAMED to `catalog_tuple_insert_pg_largeobject_metadata` (indexing.c-owned, panics until indexing lands — same deferral as merged pg_namespace/pg_am inserts); `table_close(RowExclusiveLock)`; `recordDependencyOnNewAcl(LargeObjectRelationId, loid_new, 0, ownerId, lomacl)`. Branch order, lock level, dep-record order all preserved. `Natts_pg_largeobject_metadata == 3` verified. |
| `LargeObjectDrop` | pg_largeobject.c:94-152 | lib.rs `LargeObjectDrop` | MATCH | Two `table_open(..., RowExclusiveLock)` (metadata then data); metadata scan keyed `(Anum_pg_largeobject_metadata_oid, BTEqualStrategyNumber, F_OIDEQ, loid)` on `LargeObjectMetadataOidIndexId`; first `systable_getnext` `None` ⇒ `ereport(ERROR, ERRCODE_UNDEFINED_OBJECT, "large object %u does not exist")`; `CatalogTupleDelete(pg_lo_meta, &tuple->t_self)`; `systable_endscan`; data scan keyed `(Anum_pg_largeobject_loid, ...)` on `LargeObjectLOidPNIndexId`, `while` loop deleting each `t_self`; `systable_endscan`; `table_close(pg_largeobject)`; `table_close(pg_lo_meta)`. Scan order, key columns, error SQLSTATE/message, close order preserved. |
| `LargeObjectExists` | pg_largeobject.c:166-170 | lib.rs `LargeObjectExists` | MATCH | `LargeObjectExistsWithSnapshot(loid, None)` (C `NULL`). |
| `LargeObjectExistsWithSnapshot` | pg_largeobject.c:175-205 | lib.rs `LargeObjectExistsWithSnapshot` | MATCH | scankey `(Anum_pg_largeobject_metadata_oid, BTEqualStrategyNumber, F_OIDEQ, loid)`; `table_open(..., AccessShareLock)`; `systable_beginscan(..., LargeObjectMetadataOidIndexId, true, snapshot, ...)` with the caller's `Option<Rc<SnapshotData>>` (`None` = C `NULL`); `retval = tuple.is_some()`; `systable_endscan`; `table_close(..., AccessShareLock)`. `retval` defaults `false`. |

## Constants (verified vs headers)

- `LargeObjectMetadataRelationId = 2995`, `LargeObjectMetadataOidIndexId = 2996`
  (`pg_largeobject_metadata.h`).
- `LargeObjectRelationId = 2613`, `LargeObjectLOidPNIndexId = 2683`
  (`pg_largeobject.h`).
- `Anum_pg_largeobject_metadata_oid = 1`, `Anum_pg_largeobject_loid = 1`;
  `Natts_pg_largeobject_metadata = 3` (oid / lomowner / lomacl).
- `BTEqualStrategyNumber = 3`, `F_OIDEQ = 184`.
- `RowExclusiveLock = 3`, `AccessShareLock = 1`.
- `ERRCODE_UNDEFINED_OBJECT`, severity `ERROR`.

## Seam audit

- Owned seam crate: `backend-catalog-pg-largeobject-seams`, one declaration
  `large_object_exists_with_snapshot(loid, Option<Rc<SnapshotData>>) ->
  PgResult<bool>`. Installed by this crate's `init_seams()` (a single `set()`
  delegating to `LargeObjectExistsWithSnapshot`), and `init_seams()` is wired
  into `seams-init::init_all`. Live consumer:
  `backend-utils-adt-acl::has_lo_priv_byid`.
- Outward seams are all thin marshal+delegate: `table_open` (direct dep),
  `GetNewOidWithIndex` (direct dep into merged catalog), `get_user_id`
  (miscinit), `get_user_default_acl` / `record_dependency_on_new_acl` (aclchk),
  the genam `systable_beginscan`/`getnext`/`endscan`, `catalog_tuple_delete`
  (indexing), and the new `catalog_tuple_insert_pg_largeobject_metadata`
  (indexing). No branching/computation lives in a seam path. The value layer is
  the sanctioned project-wide deferral (`Datum`/`values[]`); its owner
  `backend-catalog-indexing` is `todo`, so the uninstalled insert seam is
  exempt under `every_declared_seam_is_installed_by_its_owner` (mirror-pg-and-
  panic), exactly as the merged pg_namespace/pg_am inserts.

## Design conformance

- No invented opacity: the default ACL crosses as the real
  `types_array::ArrayType` (`None` = C `lomacl == NULL`); the snapshot as
  `Option<Rc<SnapshotData>>` (`None` = C `NULL`). No handle/token stand-ins.
- `table_open` threaded with `Mcx`; every fallible path returns `PgResult`.
  `get_user_id::call()` is infallible in C and stays bare.
- No shared statics, no registry side tables, no locks across `?`.

## Verdict: PASS

All four functions MATCH; the single SEAMED value-layer call is the sanctioned
deferral whose logic is owned by indexing.c (not absent from this crate). Inward
seam installed and wired; zero seam findings; no design violations. Gates:
`cargo check --workspace` clean; `no-todo-guard` and both `seams-init`
recurrence guards green; broad `cargo test --workspace` green.
