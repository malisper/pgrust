# Audit: backend-catalog-aclchk (F1 — the aclchk.c check-half)

Source: `../pgrust/postgres-18.3/src/backend/catalog/aclchk.c` (4995 LOC).
Status: **scaffold** (F1 landed; F2 GRANT executor + F3 DEFAULT PRIVILEGES /
init-privs still mirror-and-panic). Ported over the F0 syscache ACL/owner
projection seams (#312) + `backend-utils-adt-acl`'s `aclmask`/`acldefault`
`&[AclItem]`+`Mcx` slice model.

## Function-by-function (F1 scope)

| C function | Rust | Notes |
|---|---|---|
| `string_to_privilege` | `string_to_privilege` | exact `strcmp` chain incl. "temp"/"temporary", "alter system"; unrecognized -> ERRCODE_SYNTAX_ERROR. |
| `privilege_to_string` | `privilege_to_string` | exact switch incl. ACL_CREATE_TEMP->"TEMP"; default -> `elog(ERROR, "unrecognized privilege: %d")`. |
| `errorConflictingDefElem` | `errorConflictingDefElem` | ERRCODE_SYNTAX_ERROR + "conflicting or redundant options" + errdetail. |
| `pg_aclmask` | `pg_aclmask` | full objtype relay; COLUMN = `pg_class_aclmask | pg_attribute_aclmask`; STATISTIC_EXT/EVENT_TRIGGER elog; default elog. |
| `object_aclmask` / `_ext` | `object_aclmask` / `object_aclmask_ext` | NAMESPACE/TYPE redirect; `Assert(classid != Relation/LargeObjectMetadata)`; superuser bypass; generic `object_owner_acl` F0 projection (cacheid/owner_attnum/acl_attnum resolved via objectaddress); null ACL -> `acldefault(get_object_type(...))`; cache miss -> is_missing fast path or "cache lookup failed for %s %u". |
| `pg_attribute_aclmask` / `_ext` | same | `pg_attribute_owner_acl` (attisdropped + attacl); missing/dropped -> ERRCODE_UNDEFINED_COLUMN; null attacl -> 0 (hard-wired); owner from `pg_class_owner_acl` (concurrent-drop -> UNDEFINED_TABLE). |
| `pg_class_aclmask` / `_ext` | same | `pg_class_owner_acl`; system-catalog deny on INSERT|UPDATE|DELETE|TRUNCATE|USAGE via `is_system_class_by_namespace` (new catalog seam) + relkind!=VIEW + !superuser; superuser bypass; null relacl -> `acldefault(SEQUENCE|TABLE)`; pg_read_all_data/pg_write_all_data/pg_maintain role overlays in exact order. |
| `pg_parameter_aclmask` | `pg_parameter_aclmask` | superuser bypass; `convert_guc_name_for_parameter_acl`; `parameter_acl_by_name` (None=no entry->ACL_NO_RIGHTS); null paracl -> `acldefault(PARAMETER_ACL, BOOTSTRAP_SUPERUSERID)`. |
| `pg_parameter_acl_aclmask` | `pg_parameter_acl_aclmask` | superuser bypass; `parameter_acl_by_oid`; miss -> "parameter ACL with OID %u does not exist". |
| `pg_largeobject_aclmask_snapshot` | same | superuser bypass; `largeobject_owner_acl` (NEW seam in merged pg-largeobject domain: snapshot systable scan + GETSTRUCT(lomowner) + heap_getattr(lomacl) + DatumGetAclP decode); miss -> "large object %u does not exist"; null lomacl -> `acldefault(LARGEOBJECT)`. |
| `pg_namespace_aclmask_ext` | same | superuser bypass; temp-namespace branch (`is_temp_namespace` + `object_aclcheck_ext(Database, MyDatabaseId, ACL_CREATE_TEMP)` -> ACL_ALL_RIGHTS_SCHEMA else ACL_USAGE); `pg_namespace_owner_acl`; miss -> UNDEFINED_SCHEMA; null -> `acldefault(SCHEMA)`; pg_read/write_all_data USAGE overlay. |
| `pg_type_aclmask_ext` | same | superuser bypass; `pg_type_owner_acl` (array-element + multirange redirects resolved INSIDE the F0 projection); miss -> UNDEFINED_OBJECT; null -> `acldefault(TYPE)`. |
| `object_aclcheck` / `_ext` | same | `object_aclmask_ext(.., ACLMASK_ANY)` != 0 ? OK : NO_PRIV. |
| `pg_attribute_aclcheck` / `_ext` | same | over `pg_attribute_aclmask_ext`. |
| `pg_attribute_aclcheck_all` / `_ext` | same | owner+relacl from `pg_class_owner_acl`, relnatts from `pg_class_extra`; per-column loop 1..=relnatts; missing/dropped col `continue`; null attacl -> mask 0; ANY breaks on success, ALL breaks on failure; result init NO_PRIV. |
| `pg_class_aclcheck` / `_ext` | same | over `pg_class_aclmask_ext`. |
| `pg_parameter_aclcheck` | same | over `pg_parameter_aclmask`. |
| `pg_largeobject_aclcheck_snapshot` | same | over `pg_largeobject_aclmask_snapshot`. |
| `object_ownercheck` | `object_ownercheck` | superuser bypass; LargeObject->LargeObjectMetadata; `get_object_catcache_oid`; cacheid!=-1 -> `object_owner_acl` F0 projection; cacheid==-1 -> `table_open`+`systable_beginscan(get_object_oid_index)`+`heap_deform_tuple(get_object_attnum_owner)` fallback; then `has_privs_of_role(roleid, ownerId)`. |
| `aclcheck_error` | `aclcheck_error` | full ACLCHECK_NO_PRIV + ACLCHECK_NOT_OWNER objtype->message tables (incl. NOT_OWNER "relation %s" special cases for COLUMN/POLICY/RULE/TABCONSTRAINT/TRIGGER); unsupported objtype -> elog; ERRCODE_INSUFFICIENT_PRIVILEGE. |
| `aclcheck_error_col` | `aclcheck_error_col` | NO_PRIV -> "permission denied for column ... of relation ..."; NOT_OWNER -> delegate to `aclcheck_error`. |
| `aclcheck_error_type` | `aclcheck_error_type` | `get_element_type` ? element : type; `format_type_be`; delegate to `aclcheck_error(.., OBJECT_TYPE, ..)`. Re-homed off functioncmds-seams. |

## NOT ported here (deliberate)

- `has_createrole_privilege` / `has_bypassrls_privilege` — already ported +
  installed by `backend-utils-adt-acl` (role_membership.rs) over the AUTHOID
  `lookup_authid_by_oid` projection. Not duplicated.
- `get_user_default_acl` / `recordDependencyOnNewAcl` — **F1 STOP**, declared
  seams left uninstalled (CATALOG `scaffold` exempts them). Blocked on (1) a
  `DEFACLROLENSPOBJ` 3-key default-ACL syscache projection (F0 only delivered
  the by-OID `default_acl_*` projections), and (2) the declared seam payload is
  the *header-only* `types_array::ArrayType` (no `aclitem[]` data area), so a
  freshly merged ACL cannot cross the seam as a value nor be read back by
  `aclmembers`. Needs the F0 default-acl projection + a real `Acl` carrier.
- F2 (`ExecuteGrantStmt` etc.) and F3 (`ExecAlterDefaultPrivilegesStmt`,
  pg_init_privs family incl. the 3 declared executor/init-privs seams
  `remove_role_from_object_acl`/`remove_role_from_init_priv`/
  `replace_role_in_init_priv`) — mirror-and-panic; CATALOG held at `scaffold`.

## Seams installed (16, via init_seams wired into seams-init)

object_aclcheck, object_aclcheck_ext, pg_class_aclmask, pg_class_aclcheck,
pg_class_aclcheck_ext, pg_attribute_aclcheck, pg_attribute_aclcheck_ext,
pg_attribute_aclcheck_all, pg_attribute_aclcheck_all_ext, pg_parameter_aclcheck,
pg_largeobject_aclcheck_snapshot, object_ownercheck, aclcheck_error,
aclcheck_error_col, aclcheck_error_type, error_conflicting_def_elem.

New supporting seams: `largeobject_owner_acl` (pg-largeobject domain, installed
there), `is_system_class_by_namespace` (catalog owner, installed there).

## Allowlist entries removed

- `("backend_utils_adt_acl", "object_ownercheck")` — now installed by the aclchk
  owner; the duplicate `object_ownercheck` decl in adt-acl-seams was removed and
  its lone consumer (ri-triggers) re-pointed to aclchk-seams.
- `("backend_commands_functioncmds", "aclcheck_error_type")` — re-homed to
  aclchk-seams; functioncmds + objectaddress consumers re-pointed.

## Gates

`cargo check --workspace`, `cargo test -p no-todo-guard`, `cargo test -p
seams-init` all green. No `todo!`/`unimplemented!`.
