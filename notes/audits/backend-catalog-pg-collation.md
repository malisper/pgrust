# Audit: backend-catalog-pg-collation

C source: `src/backend/catalog/pg_collation.c` (PostgreSQL 18.3)
Port crate: `crates/backend-catalog-pg-collation`
Method: re-derived from the C + the c2rust rendering
(`c2rust-runs/backend-catalog-pg-collation`), and the header
`src/include/catalog/pg_collation.h`, independent of the port's comments.

## Function inventory

pg_collation.c defines exactly one function, `CollationCreate`. The c2rust run
additionally renders the inline datum-conversion macros (`ObjectIdGetDatum`,
`PointerGetDatum`, `Int32GetDatum`, `CharGetDatum`, `BoolGetDatum`,
`NameGetDatum`, `CStringGetTextDatum`) as functions; these are not
pg_collation.c logic — they are part of the `values[]`/`heap_form_tuple` value
layer (built in this crate against the relation tupdesc, then handed to the
`catalog/indexing.c` `CatalogTupleInsert` keystone).

`RemoveCollationById` is named in the task scope but **does not exist anywhere
in PostgreSQL 18.3** (`grep -rn RemoveCollationById src/` over the full 18.3 tree
returns nothing; collation drop runs through the generic `doDeletion` machinery
in `dependency.c`, not a catalog-specific remover). Only `CollationCreate` is
ported, which is 100% of pg_collation.c.

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `CollationCreate` | pg_collation.c:41-236 | lib.rs `CollationCreate` | MATCH | full branch-order parity, see below |

## CollationCreate — line-by-line

| C | Port | Verdict |
|---|---|---|
| `Assert(collname)` | `debug_assert!(!collname.is_empty())` (non-null `&str` makes a null unrepresentable) | MATCH (invariant in the type) |
| `Assert(collnamespace)` / `Assert(collowner)` | `debug_assert!(OidIsValid(collnamespace))` / `OidIsValid(collowner)` | MATCH |
| `Assert((collprovider == COLLPROVIDER_LIBC && collcollate && collctype && !colllocale) || (... != LIBC && !collcollate && !collctype && colllocale))` | identical `debug_assert!` over `is_some()`/`is_none()` | MATCH |
| `oid = GetSysCacheOid3(COLLNAMEENCNSP, Anum_pg_collation_oid, PointerGetDatum(collname), Int32GetDatum(collencoding), ObjectIdGetDatum(collnamespace))` | `get_syscache_oid3_collnameencnsp(mcx, collname, collencoding, collnamespace)?` = `GetSysCacheOid(mcx, COLLNAMEENCNSP, Anum_pg_collation_oid, Str(collname), Value(i32), Value(oid), UNUSED)` | MATCH — `GetSysCacheOid3` is the macro `GetSysCacheOid(..., 0)`; key1 string / key2 int4 / key3 oid order preserved |
| `if (OidIsValid(oid))` → `if (quiet) return InvalidOid;` | `if OidIsValid(oid) { if quiet { return Ok(InvalidOid); }` | MATCH |
| `else if (if_not_exists) { ObjectAddressSet(myself, CollationRelationId, oid); checkMembershipInCurrentExtension(&myself); ereport(NOTICE, errcode(ERRCODE_DUPLICATE_OBJECT), encoding==-1 ? "...already exists, skipping" : "...for encoding \"%s\" already exists, skipping", pg_encoding_to_char(enc)); return InvalidOid; }` | same: `object_address_set`, `checkMembershipInCurrentExtension(mcx,&myself)?`, the `encoding == -1` two-arm message via `pg_encoding_to_char::call`, `ereport(NOTICE).errcode(ERRCODE_DUPLICATE_OBJECT).errmsg(msg).finish(here("CollationCreate"))?`, `return Ok(InvalidOid)` | MATCH — SQLSTATE 42710 (`ERRCODE_DUPLICATE_OBJECT`) + both message strings identical |
| `else ereport(ERROR, errcode(ERRCODE_DUPLICATE_OBJECT), encoding==-1 ? "...already exists" : "...for encoding \"%s\" already exists")` | `return Err(ereport(ERROR).errcode(ERRCODE_DUPLICATE_OBJECT).errmsg(msg).into_error())` (two-arm message via `pg_encoding_to_char`) | MATCH |
| `rel = table_open(CollationRelationId, ShareRowExclusiveLock)` | `table_seams::table_open::call(mcx, CollationRelationId, ShareRowExclusiveLock)?` | MATCH (SEAMED to table-table) |
| `if (collencoding == -1) oid = GetSysCacheOid3(..., Int32GetDatum(GetDatabaseEncoding()), ...) else oid = GetSysCacheOid3(..., Int32GetDatum(-1), ...)` | same branch: `get_syscache_oid3_collnameencnsp(mcx, collname, mbutils_seams::get_database_encoding::call(), nsp)?` vs `(..., -1, nsp)?` | MATCH — `GetDatabaseEncoding` SEAMED to mbutils |
| `if (OidIsValid(oid)) { if (quiet) { table_close(rel, NoLock); return InvalidOid; }` | `if OidIsValid(oid) { if quiet { rel.close(NoLock)?; return Ok(InvalidOid); }` | MATCH |
| `else if (if_not_exists) { ObjectAddressSet(...); checkMembershipInCurrentExtension(&myself); table_close(rel, NoLock); ereport(NOTICE, ..., "...already exists, skipping"); return InvalidOid; }` | same order: object_address_set, checkMembership, `rel.close(NoLock)?`, NOTICE ereport, `return Ok(InvalidOid)` | MATCH — this NOTICE has no encoding arm (matches C) |
| `else ereport(ERROR, ..., "collation \"%s\" already exists")` | `return Err(ereport(ERROR).errcode(ERRCODE_DUPLICATE_OBJECT).errmsg(...).into_error())` | MATCH (note: C does NOT close rel here; the error unwinds via the resource owner — port returns Err identically without an explicit close) |
| `tupDesc = RelationGetDescr(rel)` | `let tupdesc = rel.rd_att_clone_in(mcx)?` | MATCH |
| `memset(nulls, 0, sizeof(nulls))` | `let mut nulls = [false; Natts_pg_collation]` | MATCH |
| `namestrcpy(&name_name, collname)` / `oid = GetNewOidWithIndex(rel, CollationOidIndexId, Anum_pg_collation_oid)` | `oid = GetNewOidWithIndex(&rel, CollationOidIndexId, Anum_pg_collation_oid)?`; name placed via `name_datum` below | MATCH (direct dep on catalog.c) |
| `values[oid-1] = ObjectIdGetDatum(oid)` | `values[idx(Anum_pg_collation_oid)] = Datum::from_oid(oid)` | MATCH |
| `values[collname-1] = NameGetDatum(&name_name)` | `= name_datum(mcx, collname)?` (64-byte NUL-padded NameData image) | MATCH |
| `values[collnamespace-1] = ObjectIdGetDatum(collnamespace)` | `Datum::from_oid(collnamespace)` | MATCH |
| `values[collowner-1] = ObjectIdGetDatum(collowner)` | `Datum::from_oid(collowner)` | MATCH |
| `values[collprovider-1] = CharGetDatum(collprovider)` | `Datum::from_char(collprovider)` | MATCH |
| `values[collisdeterministic-1] = BoolGetDatum(collisdeterministic)` | `Datum::from_bool(collisdeterministic)` | MATCH |
| `values[collencoding-1] = Int32GetDatum(collencoding)` | `Datum::from_i32(collencoding)` | MATCH |
| `if (collcollate) values[..] = CStringGetTextDatum(collcollate) else nulls[..] = true` | `match collcollate { Some(s) => text_datum(mcx,s)?, None => nulls[..]=true }` | MATCH |
| same for `collctype` / `colllocale` / `collicurules` / `collversion` | same `match` per column | MATCH (5 nullable text columns, C order) |
| `tup = heap_form_tuple(tupDesc, values, nulls)` | `let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?` | MATCH (direct dep on heaptuple) |
| `CatalogTupleInsert(rel, tup)` | `CatalogTupleInsert(mcx, &rel, &mut tup)?` | MATCH — the indexing.c keystone (direct `pub` call, carrier model) |
| `Assert(OidIsValid(oid))` | `debug_assert!(OidIsValid(oid))` | MATCH |
| `myself = {CollationRelationId, oid, 0}` | `ObjectAddress { classId: CollationRelationId, objectId: oid, objectSubId: 0 }` | MATCH |
| `referenced = {NamespaceRelationId, collnamespace, 0}; recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL)` | `recordDependencyOn(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?` (referenced built {NAMESPACE_RELATION_ID, collnamespace, 0}) | MATCH (direct dep on pg-depend) |
| `recordDependencyOnOwner(CollationRelationId, oid, collowner)` | `recordDependencyOnOwner(CollationRelationId, oid, collowner)?` | MATCH (direct dep on pg-shdepend) |
| `recordDependencyOnCurrentExtension(&myself, false)` | `recordDependencyOnCurrentExtension(mcx, &myself, false)?` | MATCH (direct dep on pg-depend) |
| `InvokeObjectPostCreateHook(CollationRelationId, oid, 0)` | `objectaccess_seams::invoke_object_post_create_hook::call(CollationRelationId, oid, 0)?` | MATCH (SEAMED to objectaccess; the seam wraps the macro's `if (object_access_hook)` guard + `RunObjectPostCreateHook(..., is_internal=false)`) |
| `heap_freetuple(tup)` | `drop(tup)` | MATCH (owned value freed at scope end) |
| `table_close(rel, NoLock)` | `rel.close(NoLock)?` | MATCH |
| `return oid` | `Ok(oid)` | MATCH |

## Constants (verified vs `pg_collation.h` / `pg_collation_d.h`)

K0 vocab added to `types-catalog::pg_collation`:

- `CollationRelationId` = 3456 — `CATALOG(pg_collation,3456,CollationRelationId)`. ✓
- `CollationOidIndexId` = 3085 — `DECLARE_UNIQUE_INDEX_PKEY(pg_collation_oid_index, 3085, ...)`. ✓
- `CollationNameEncNspIndexId` = 3164 — `DECLARE_UNIQUE_INDEX(pg_collation_name_enc_nsp_index, 3164, ...)`. ✓
- `Anum_pg_collation_oid` = 1, `collname` = 2, `collnamespace` = 3, `collowner` = 4, `collprovider` = 5, `collisdeterministic` = 6, `collencoding` = 7, `collcollate` = 8, `collctype` = 9, `colllocale` = 10, `collicurules` = 11, `collversion` = 12 — field-for-field with `FormData_pg_collation` order in pg_collation.h (oid, collname, collnamespace, collowner, collprovider, collisdeterministic, collencoding, then CATALOG_VARLEN collcollate/collctype/colllocale/collicurules/collversion). ✓ (cross-checked against the c2rust `ANUM_PG_COLLATION_*` in pgrust-pg-ffi-fgram and against `Anum_pg_collation_*` already present in objectaddress consts for 1..4.)
- `Natts_pg_collation` = 12. ✓
- `COLLPROVIDER_DEFAULT='d'`, `COLLPROVIDER_BUILTIN='b'`, `COLLPROVIDER_ICU='i'`, `COLLPROVIDER_LIBC='c'` — pg_collation.h `EXPOSE_TO_CLIENT_CODE`. ✓
- `COLLNAMEENCNSP` cache id = 15 — already in syscache `cacheinfo.rs` (MAKE_SYSCACHE COLLNAMEENCNSP), registered to CollationRelationId/CollationNameEncNspIndexId. ✓
- `ERRCODE_DUPLICATE_OBJECT` = 42710 — matches types-error. ✓
- `ShareRowExclusiveLock` = 6, `NoLock` = 0, `DEPENDENCY_NORMAL` = 'n'. ✓

## Seam / wiring audit

The inward seam this unit OWNS and installs is `collation_create` (declared in
`backend-commands-collationcmds-seams`, the C-source home being the consumer
collationcmds.c whose CREATE/ALTER COLLATION drivers and
`pg_import_system_collations` call it). The pre-existing 13-field
`CollationCreateArgs`/`collation_create(args) -> PgResult<Oid>` contract is
preserved exactly (live consumer: `backend-commands-collationcmds`). The seam
threads no `Mcx`, so the handler builds a scratch `MemoryContext`
("CollationCreate") and derives `mcx` from it for the tuple-forming + catalog
mutation — mirroring the C `CurrentMemoryContext` and the pg-namespace /
relfilenumbermap installer precedent. The new OID is a scalar the caller keeps;
the scratch context drops on return.

`init_seams()` installs exactly `collation_create` (`set(collation_create_handler)`)
and nothing else; it is wired into `seams-init::init_all()`. Recurrence guards
pass: both `every_seam_installing_crate_is_wired_into_init_all` and
`every_declared_seam_is_installed_by_its_owner` (the latter previously had
`collation_create` declared-but-uninstalled; now installed).

## Gate

- `cargo check --workspace` — green (only pre-existing warnings in unrelated crates; zero in this crate).
- `no-todo-guard` — green (no `todo!`/`unimplemented!`).
- `seams-init` recurrence guards — both green.
- No stubs, no `panic!` own-logic; the only delegations are seams into the
  correct owners (table-table, objectaccess, varlena, mbutils, encnames) and
  direct `pub` calls into the catalog-mutation engine (indexing.c keystone),
  catalog.c, heaptuple, syscache, pg-depend, pg-shdepend.

Verdict: **PASS** — `CollationCreate` is a branch-for-branch faithful port on
the real carrier model. 100% of pg_collation.c covered. ported 2026-06-15.
