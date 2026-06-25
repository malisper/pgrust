# Audit: backend-catalog-pg-namespace

C source: `src/backend/catalog/pg_namespace.c` (PostgreSQL 18.3)
Port crate: `crates/backend-catalog-pg-namespace`
Method: re-derived from the C + the c2rust rendering
(`c2rust-runs/backend-catalog-pg-namespace/src/pg_namespace.rs`), independent of
the port's comments.

## Function inventory

The C file defines exactly one function. The c2rust run additionally renders the
inline datum-conversion macros (`ObjectIdGetDatum`, `PointerGetDatum`,
`CStringGetDatum`, `NameGetDatum`) as functions; these are not pg_namespace.c
logic — they are part of the `values[]`/`heap_form_tuple` value layer, which is
owned by `catalog/indexing.c` and crosses the seam.

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `NamespaceCreate` | pg_namespace.c:42-120 | lib.rs `NamespaceCreate` | MATCH | full branch-order parity, see below |

## NamespaceCreate — line-by-line

| C | Port | Verdict |
|---|---|---|
| `if (!nspName) elog(ERROR, "no namespace name supplied")` | non-null `&str` makes the state unrepresentable | MATCH (invariant moved to the type) |
| `if (SearchSysCacheExists1(NAMESPACENAME, nspName))` → `ereport(ERROR, errcode(ERRCODE_DUPLICATE_SCHEMA), errmsg("schema \"%s\" already exists"))` | `namespace_name_exists::call(nspName)?` then `ereport(ERROR).errcode(ERRCODE_DUPLICATE_SCHEMA).errmsg(format!("schema \"{nspName}\" already exists"))` | MATCH — SQLSTATE 42P06 verified vs errcodes.txt; message string identical |
| `if (!isTemp) nspacl = get_user_default_acl(OBJECT_SCHEMA, ownerId, InvalidOid) else nspacl = NULL` | `if !isTemp { get_user_default_acl::call(ObjectType::Schema, ownerId, InvalidOid)? } else { None }` | MATCH (SEAMED to aclchk) |
| `table_open(NamespaceRelationId, RowExclusiveLock)` | `table_open(mcx, NamespaceRelationId, RowExclusiveLock)?` (direct dep) | MATCH |
| init nulls/values; `nspoid = GetNewOidWithIndex(rel, NamespaceOidIndexId, Anum_pg_namespace_oid)`; set oid/nspname(namestrcpy)/nspowner; nspacl set-or-null; `tup = heap_form_tuple`; `CatalogTupleInsert(rel, tup)` | `nspoid = catalog_tuple_insert_pg_namespace::call(&nspdesc, nspName, ownerId, nspacl)?` (SEAMED to indexing; assigns OID + forms + inserts, mirrors `catalog_tuple_insert_pg_opclass`) | MATCH (SEAMED) — value layer is the indexing owner's; nspacl==None ⇒ nulls[nspacl] documented in the seam |
| `Assert(OidIsValid(nspoid))` | `debug_assert!(OidIsValid(nspoid))` | MATCH |
| `table_close(nspdesc, RowExclusiveLock)` | `nspdesc.close(RowExclusiveLock)?` | MATCH |
| `myself = {NamespaceRelationId, nspoid, 0}` | `ObjectAddress { classId: NamespaceRelationId, objectId: nspoid, objectSubId: 0 }` | MATCH |
| `recordDependencyOnOwner(NamespaceRelationId, nspoid, ownerId)` | `recordDependencyOnOwner::call(...)?` (SEAMED to pg-shdepend) | MATCH |
| `recordDependencyOnNewAcl(NamespaceRelationId, nspoid, 0, ownerId, nspacl)` | `record_dependency_on_new_acl::call(NamespaceRelationId, nspoid, 0, ownerId, nspacl)?` (SEAMED to aclchk) | MATCH |
| `if (!isTemp) recordDependencyOnCurrentExtension(&myself, false)` | `if !isTemp { recordDependencyOnCurrentExtension::call(mcx, &myself, false)? }` (SEAMED to pg-depend) | MATCH |
| `InvokeObjectPostCreateHook(NamespaceRelationId, nspoid, 0)` = `if (object_access_hook) RunObjectPostCreateHook(..., is_internal=false)` | `if object_access_hook_present::call() { run_object_post_create_hook::call(NamespaceRelationId, nspoid, 0, false)? }` (SEAMED to objectaccess) | MATCH — macro guard + body split per the objectaccess-seams contract and the opclasscmds precedent |
| `return nspoid` | `Ok(nspoid)` | MATCH |

## Constants (verified vs headers)

- `NamespaceRelationId` = 2615 — pg_namespace.h:35 (`CATALOG(pg_namespace,2615,...)`). ✓
- `NamespaceOidIndexId` = 2685 — pg_namespace.h:57 (`DECLARE_UNIQUE_INDEX_PKEY(pg_namespace_oid_index, 2685, ...)`). Added to types-catalog. ✓
- `ERRCODE_DUPLICATE_SCHEMA` = 42P06 — errcodes.txt:382. ✓ (matches types-error)
- `OBJECT_SCHEMA` → `ObjectType::Schema = 36`. ✓
- `Anum_pg_namespace_oid` = 1 — used inside the indexing seam owner, not this crate.

## Seam / wiring audit

Owned seam crate (by C-source coverage of pg_namespace.c): `backend-catalog-pg-namespace-seams`, declaring `namespace_create`. `init_seams()` installs exactly that one seam (`set(NamespaceCreate)`) and nothing else; `seams-init::init_all()` calls it (recurrence_guard tests pass: both `every_seam_installing_crate_is_wired_into_init_all` and `every_declared_seam_is_installed_by_its_owner`). The existing 3-arg seam contract `namespace_create(nsp_name, owner_id, is_temp)` is preserved (live consumer in `backend-catalog-namespace`); `NamespaceCreate` therefore takes no `Mcx` param and builds a local scratch `MemoryContext` for `table_open` / `recordDependencyOnCurrentExtension`, mirroring the C `CurrentMemoryContext` and the pg-shdepend installer pattern.

Outward seams (all thin marshal + delegate, justified by real cross-subsystem/cycle boundaries; owners unported ⇒ panic, which is correct):
- `namespace_name_exists` — NEW in `backend-utils-cache-syscache-seams` (SearchSysCacheExists1(NAMESPACENAME)).
- `get_user_default_acl`, `record_dependency_on_new_acl` — NEW in `backend-catalog-aclchk-seams` (both aclchk.c-owned). Acl* crosses as real `Option<types_array::ArrayType>` (Acl = ArrayType); no invented handle.
- `catalog_tuple_insert_pg_namespace` — NEW in `backend-catalog-indexing-seams`, mirroring `catalog_tuple_insert_pg_opclass` (GetNewOidWithIndex + namestrcpy + heap_form_tuple + CatalogTupleInsert, returns the assigned OID).
- `recordDependencyOnOwner` (pg-shdepend-seams), `recordDependencyOnCurrentExtension` (pg-depend-seams), `object_access_hook_present` + `run_object_post_create_hook` (objectaccess-seams) — existing decls.

No logic lives on any seam path. No own-logic stub, no `todo!`/`unimplemented!`/`unreachable!`. The only `format!` is error-message construction at the duplicate-schema `Err` site (the allowed exception).

## Design conformance

- Opacity: nspacl is the real C `Acl` (= `ArrayType`), not an invented handle; threaded producer→consumers without inspection. PASS.
- Mcx + PgResult: every fallible/allocating crossing returns `PgResult`; the local `MemoryContext` mirrors the C ambient context. PASS.
- No shared statics, no ambient-global getter seams, no locks across `?` (the `Relation` handle is closed explicitly mirroring `table_close`; `close` is a method that releases). PASS.
- No type-alias stand-ins, no `&[u8]` blobs. PASS.

## Verdict: PASS

Every function MATCH (or MATCH-via-SEAMED per step 3). Zero seam findings, zero
design findings. `cargo check --workspace` clean; recurrence_guard green.
