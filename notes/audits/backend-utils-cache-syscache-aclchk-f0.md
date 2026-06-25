# Audit: aclchk F0 keystone — syscache ACL/owner projection seams (#312)

Scope: the F0 keystone for `catalog/aclchk.c`'s aclmask/aclcheck check-half —
catalog-row ACL/owner projection seams added to the (merged) owner
`backend-utils-cache-syscache` + its `-seams` crate, installed from
`init_seams()`. NOT aclchk.c itself (F1).

## Seams added + installed (7)

All declared in `backend-utils-cache-syscache-seams`, installed from
`backend-utils-cache-syscache::init_seams()`, bodies in `projections.rs`. Each
returns owned values in the caller's `mcx`: the object's owner `Oid` plus a real
decoded `Acl` (`PgVec<AclItem>` / `&[AclItem]`), never an opaque handle or
Datum-word. A SQL-null ACL column crosses as `None` (aclchk builds the hardwired
`acldefault`); a cache miss is `Ok(None)`.

| seam | C source read | projects |
|---|---|---|
| `pg_class_owner_acl` | `pg_class_aclmask_ext` `SearchSysCache1(RELOID)` | `relowner`/`relkind`/`relnamespace` + decoded `relacl` |
| `pg_attribute_owner_acl` | `pg_attribute_aclmask_ext` `SearchSysCache2(ATTNUM)` | `attisdropped` + decoded `attacl` |
| `pg_namespace_owner_acl` | `pg_namespace_aclmask_ext` `SearchSysCache1(NAMESPACEOID)` | `nspowner` + decoded `nspacl` |
| `pg_type_owner_acl` | `pg_type_aclmask_ext` `SearchSysCache1(TYPEOID)` | effective `typowner` + decoded `typacl`, after the true-array (`IsTrueArrayType` -> `typelem`) and multirange (`TYPTYPE_MULTIRANGE` -> `get_multirange_range`) redirects |
| `object_owner_acl` | `object_aclmask_ext` `SearchSysCache1(cacheid)` | generic owner + decoded ACL by caller-supplied `cacheid`/`owner_attnum`/`acl_attnum` |
| `parameter_acl_by_name` | `pg_parameter_aclmask` `SearchSysCache1(PARAMETERACLNAME)` | decoded `paracl` (outer `None` = cache miss / `ACL_NO_RIGHTS`) |
| `parameter_acl_by_oid` | `pg_parameter_acl_aclmask` `SearchSysCache1(PARAMETERACLOID)` | decoded `paracl` |

## Faithfulness vs aclchk.c

- Each projection mirrors the C `SearchSysCache* + GETSTRUCT + SysCacheGetAttr`
  read structure exactly. The owner is the `GETSTRUCT`/`SysCacheGetAttrNotNull`
  field; the ACL column is `SysCacheGetAttr` (nullable).
- `DatumGetAclP(aclDatum)` (detoast + cast) is `decode_acl`: detoast the stored
  varlena (reusing the in-crate `detoast_array_header`), then read
  `ACL_NUM(acl) = ARR_DIMS[0]` fixed-16-byte `aclitem`s from `ACL_DAT(acl) =
  ARR_DATA_PTR(acl)`. `aclitem` is `typlen=16`, `typalign='d'` so
  `ARR_DATA_PTR` is `MAXALIGN(hdr + 2*4*ndim)` (handled by `arr_data_offset`).
- `pg_type_owner_acl` performs the two redirects INSIDE the projection (the C
  does them on the held tuple), re-`SearchSysCache1(TYPEOID)` for the element /
  range type and returning the *effective* `(owner, acl)`. `IsTrueArrayType` =
  `OidIsValid(typelem) && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER` (oid 6179).
- `pg_attribute_owner_acl` does NOT fetch the relation owner — the C reads it
  from a separate `SearchSysCache1(RELOID)` lookup, which the caller (F1) does
  via `pg_class_owner_acl`, matching the C two-lookup shape.
- `object_owner_acl` is class-agnostic: the caller (F1, which owns
  `object_aclmask_ext`) resolves `cacheid`/`owner_attnum`/`acl_attnum` via the
  objectaddress `get_object_catcache_oid`/`get_object_attnum_owner`/
  `get_object_attnum_acl` helpers and passes them in. Keeps the seam a thin
  projection with no objectaddress dep.

## What does NOT belong here (faithful placement)

- **`largeobject_owner_acl` is NOT a syscache projection.**
  `pg_largeobject_aclmask_snapshot` uses `table_open` + `systable_beginscan`
  with a caller-supplied snapshot — `pg_largeobject_metadata` has NO syscache
  (no `MAKE_SYSCACHE`). Its owner is the merged `backend-catalog-pg-largeobject`
  domain (which already does snapshot-aware metadata scans); F1 routes the
  largeobject ACL read there, not through syscache. Excluded from this F0 task
  by faithful placement.
- **`IsSystemClass` stays in F1/aclchk.** It is a `catalog.c` predicate
  (`IsCatalogRelationOid || IsToastClass`), available as
  `backend-catalog-catalog-seams`. F0 surfaces `relnamespace` so F1 can compute
  it; the seam performs no predicate logic.
- **`acldefault`/`aclmask`/superuser/role-membership** are F1 (aclchk) +
  adt-acl logic, not projections.

## Allowlist / CONTRACT_RECONCILE

No entries to remove: the 7 seams are freshly declared + installed (none were
previously allowlisted or in `CONTRACT_RECONCILE_PENDING`). The aclchk allowlist
entries (`object_ownercheck`, `aclcheck_error_type`) are F1's to retire.

## Types added

`types-cache::syscache`: `ClassOwnerAcl`/`NamespaceOwnerAcl`/`TypeOwnerAcl`/
`ObjectOwnerAcl` (owner + `Option<PgVec<AclItem>>`). `types-cache` gains a
`types-acl` dep (acyclic; `types-acl` deps only `types-core`).

## Gates

`cargo check --workspace` clean; `no-todo-guard` green; `seams-init` green
(both `every_declared_seam_is_installed_by_its_owner` and
`every_seam_installing_crate_is_wired_into_init_all` pass — the 7 new seams are
installed). Full `cargo test --workspace` green except the documented allowed
flakes (`range_pair_*`, `gram-core` LALR). No `todo!`/`unimplemented!`.
