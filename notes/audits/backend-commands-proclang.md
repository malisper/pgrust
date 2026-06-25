# Audit: backend-commands-proclang

C source: `src/backend/commands/proclang.c` (PostgreSQL 18.3).
c2rust reference: per the manifest, proclang.c is the sole C file.

The file defines exactly two public functions and no file-static state:
`CreateProceduralLanguage` and `get_language_oid`. (The DropProceduralLanguageById
named in the task prompt does NOT exist in proclang.c for PG 18.3 — drop is
handled generically via the dependency machinery; the C file has no such
function. The port mirrors the actual C file.)

## Function-by-function

### CreateProceduralLanguage (proclang.c:36-217)

| C step | Port | Verdict |
|---|---|---|
| `languageName = stmt->plname` | `stmt.plname.as_deref().unwrap_or("")` | OK (owned `Option<PgString>`; parser always fills it) |
| `languageOwner = GetUserId()` | `get_user_id::call()` (miscinit seam) | OK |
| `if (!superuser()) ereport(ERRCODE_INSUFFICIENT_PRIVILEGE, "must be superuser to create custom procedural language")` | `superuser::call()?` + `PgError::new(ERROR,...).with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)` | OK — message + SQLSTATE (42501) match |
| `Assert(stmt->plhandler)` | `debug_assert!(!stmt.plhandler.is_empty())` | OK |
| `handlerOid = LookupFuncName(stmt->plhandler, 0, NULL, false)` | `lookup_func_name::call(&plhandler_names, 0, &[], false)?` | OK — nargs=0, no argtypes, missing_ok=false |
| `funcrettype = get_func_rettype(handlerOid)` | `get_func_rettype::call(handlerOid)?` | OK |
| `if (funcrettype != LANGUAGE_HANDLEROID) ereport(ERRCODE_WRONG_OBJECT_TYPE, "function %s must return type %s", NameListToString(plhandler), "language_handler")` | `LANGUAGE_HANDLEROID = 2280`; same message/format; SQLSTATE 42809 | OK — OID 2280 verified vs pg_type.dat; the historical OPAQUE branch is removed in 18, so a non-handler rettype is a hard error (matches C) |
| inline: `if (stmt->plinline) { funcargtypes[0]=INTERNALOID; inlineOid=LookupFuncName(plinline,1,funcargtypes,false);} else inlineOid=InvalidOid;` | same; `INTERNALOID` (2281); nargs=1; missing_ok=false | OK |
| validator: `if (stmt->plvalidator) { funcargtypes[0]=OIDOID; valOid=LookupFuncName(plvalidator,1,funcargtypes,false);} else valOid=InvalidOid;` | same; `OIDOID` (26) | OK |
| `rel = table_open(LanguageRelationId, RowExclusiveLock)` | `table_open(mcx, LanguageRelationId, RowExclusiveLock)?` | OK (LanguageRelationId=2612) |
| build `values[]`/`nulls[]` (lanname via namestrcpy, lanowner, lanispl=true, lanpltrusted, lanplcallfoid, laninline, lanvalidator; `nulls[lanacl]=true`) | `PgLanguageInsertRow{...}` carrier built in the port; the values array + the lanacl-null are formed inside the indexing seam in catalog column order (oid,lanname,lanowner,lanispl,lanpltrusted,lanplcallfoid,laninline,lanvalidator,lanacl) | OK — column order + null mask verified vs pg_language.h |
| `oldtup = SearchSysCache1(LANGNAME, languageName)` | `language_tuple_by_name::call(mcx, languageName)?` (syscache seam returns the writable tuple + `{oid,lanowner}`) | OK |
| `if (HeapTupleIsValid(oldtup))` replace branch | `if let Some((oldtuple, oldform))` | OK |
| `if (!stmt->replace) ereport(ERRCODE_DUPLICATE_OBJECT, "language \"%s\" already exists")` | same; SQLSTATE 42710 | OK |
| `#ifdef NOT_USED` ownership recheck | omitted (as in C) | OK |
| `replaces[oid/lanowner/lanacl]=false; tup=heap_modify_tuple(oldtup,...); CatalogTupleUpdate(rel,&tup->t_self,tup); langoid=oldform->oid; is_update=true` | `catalog_tuple_update_pg_language::call(mcx,&rel,&oldtuple,&row)?` (indexing seam: replaces mask oid/lanowner/lanacl=false, heap_modify_tuple over oldtuple, CatalogTupleUpdate at tup.t_self); `langoid=oldform.oid; is_update=true` | OK — oid/owner/ACL preserved from the old tuple via the false mask; faithful |
| create branch: `langoid=GetNewOidWithIndex(rel,LanguageOidIndexId,Anum_pg_language_oid); values[oid]=langoid; tup=heap_form_tuple(...); CatalogTupleInsert(rel,tup); is_update=false` | `catalog_tuple_insert_pg_language::call(mcx,&rel,&row)?` (indexing seam: GetNewOidWithIndex over LanguageOidIndexId=2682 + Anum oid=1, stamps oid, heap_form_tuple, CatalogTupleInsert), returns langoid; `is_update=false` | OK |
| `myself = {LanguageRelationId, langoid, 0}` | same | OK |
| `if (is_update) deleteDependencyRecordsFor(classId, objectId, true)` | same (direct dep; return i64 discarded as C does) | OK |
| `if (!is_update) recordDependencyOnOwner(classId, objectId, languageOwner)` | same (direct dep) | OK |
| `recordDependencyOnCurrentExtension(&myself, is_update)` | `recordDependencyOnCurrentExtension(mcx, &myself, is_update)?` (repo sig threads mcx) | OK |
| `addrs = new_object_addresses()` | same | OK |
| handler dep: `ObjectAddressSet(referenced, ProcedureRelationId, handlerOid); add_exact_object_address(&referenced, addrs)` | `ObjectAddress{PROCEDURE_RELATION_ID,handlerOid,0}` + add_exact | OK (ProcedureRelationId=1255) |
| inline dep: `if (OidIsValid(inlineOid)) add_exact(...)` | same | OK |
| validator dep: `if (OidIsValid(valOid)) add_exact(...)` | same | OK |
| `record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL); free_object_addresses(addrs)` | same; `drop(addrs)` for the free | OK (DEPENDENCY_NORMAL = 'n') |
| `InvokeObjectPostCreateHook(LanguageRelationId, myself.objectId, 0)` | `invoke_object_post_create_hook::call(LanguageRelationId, objectId, 0)?` | OK |
| `table_close(rel, RowExclusiveLock)` | `rel.close(RowExclusiveLock)?` | OK |
| `return myself` | `Ok(myself)` | OK |

Ordering of the dependency wiring (delete → owner → extension → addrs/handler/inline/validator → record → hook → close) is preserved exactly.

### get_language_oid (proclang.c:225-237)

| C | Port | Verdict |
|---|---|---|
| `oid = GetSysCacheOid1(LANGNAME, Anum_pg_language_oid, langname)` | `language_oid_by_name::call(langname)?` (syscache seam: GetSysCacheOid over LANGNAME, Anum oid=1) | OK |
| `if (!OidIsValid(oid) && !missing_ok) ereport(ERRCODE_UNDEFINED_OBJECT, "language \"%s\" does not exist")` | same; SQLSTATE 42704 | OK |
| `return oid` | `Ok(oid)` | OK |

## Seam / ownership notes

- New types: `types-catalog::pg_language` (LanguageRelationId/NameIndexId/OidIndexId,
  Anum_pg_language_*, Natts=9, FormData_pg_language{oid,lanowner}, PgLanguageInsertRow).
  All OIDs/attnums verified vs `catalog/pg_language.h` and `pg_type.dat`.
- New indexing seams `catalog_tuple_insert_pg_language` / `catalog_tuple_update_pg_language`
  (owner `backend-catalog-indexing`, installed from `family1::install`). They keep the
  C catalog-tuple build (heap_form_tuple / GetNewOidWithIndex / heap_modify_tuple /
  CatalogTupleInsert / CatalogTupleUpdate) in the indexing keystone, matching the repo's
  per-catalog typed-insert convention (pg_cast/pg_inherits/pg_enum).
- New syscache seams `language_oid_by_name` / `language_tuple_by_name`
  (owner `backend-utils-cache-syscache`, installed from its init_seams).
- `get_language_oid` seam (`backend-commands-proclang-seams`) is now installed by the
  REAL owner `backend-commands-proclang::init_seams` (previously cross-installed by the
  syscache crate as a placeholder; that install was retired and the proclang-seams dep
  removed from the syscache crate). One installer per seam preserved.
- No `todo!`/`unimplemented!`. No CONTRACT_RECONCILE_PENDING introduced.

## Divergences

None of substance. Two faithful adaptations:
1. `recordDependencyOnCurrentExtension` takes an explicit `Mcx` (repo signature) — C
   uses ambient context; behaviour identical.
2. `NameListToString` for the wrong-rettype error message is computed inline (join the
   `String`-node name parts with '.') rather than calling the namespace owner — the C
   function is a pure list-join with no catalog access, so this is behaviour-identical
   and avoids a needless cross-crate dep.
