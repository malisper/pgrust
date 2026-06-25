# Audit: backend-commands-alter

C source: `src/backend/commands/alter.c` (1063 lines, 10 functions).
Port: `crates/backend-commands-alter/src/lib.rs`.
Audited independently against the C and the c2rust rendering.

## Function inventory and verdicts

| C function (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `report_name_conflict` (75-108, static) | `report_name_conflict` | MATCH | The `classId` → message switch (EventTrigger/FDW/Server/Language/Publication/Subscription) reproduced verbatim with `ERRCODE_DUPLICATE_OBJECT`; the default arm raises the internal "unsupported object class: %u". Public so the in-crate dup-name probes can call it. |
| `report_namespace_conflict` (110-151, static) | `report_namespace_conflict` | MATCH | `debug_assert!(OidIsValid(nspOid))` mirrors the C `Assert`; the Conversion/StatisticExt/TSParser/TSDictionary/TSTemplate/TSConfig switch with `... in schema "%s"` + `ERRCODE_DUPLICATE_OBJECT`; `get_namespace_name(nspOid)` resolved in-crate (C passes it as the `errmsg` argument). |
| `AlterObjectRename_internal` (164-364, static) | `AlterObjectRename_internal` | MATCH | In-crate: `get_object_catcache_*`/`get_object_attnum_*` metadata block; `SearchSysCache1(oidCacheId, objectId)` → "cache lookup failed" on miss; name + namespace reads; superuser-bypass permission block (no-explicit-owner → "must be superuser to rename %s"; `has_privs_of_role` → `aclcheck_error(NOT_OWNER, get_object_type, old_name)`; namespace `ACL_CREATE`; the SUBSCRIPTION DB-`ACL_CREATE` + `subpasswordrequired` superuser-only guard with exact errmsg/errhint); the dup-name friendliness probes (proc via `IsThereFunctionInNamespace`, collation/opclass/opfamily via their `IsThere*`, subscription via `SearchSysCacheExists2(SUBSCRIPTIONNAME, MyDatabaseId, new)` → `report_name_conflict`, else `nameCacheId` namespace/name existence → report); `LogicalRepWorkersWakeupAtCommit(objectId)` for subscriptions; name-column `heap_modify_tuple` (`namestrcpy` into a `NameData`) + `CatalogTupleUpdate`; `InvokeObjectPostAlterHook`; the PUBLICATION `InvalidatePubRelSyncCache(pub->oid, pub->puballtables)` post-task. The `#ifdef ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS` WARNING is compile-disabled in PG → correctly omitted. `IsThere*` called directly (acyclic). `renametrig`/`InvalidatePubRelSyncCache`/`LogicalRepWorkersWakeupAtCommit` cross to unported owners via their `-seams` (seam-and-panic). |
| `ExecRenameStmt` (372-461) | `ExecRenameStmt` | MATCH | The full `renameType` dispatch with the exact case groupings. Direct calls to ported owners: `RenameRole`, `RenameSchema`, `RenameTableSpace`, `RenameRewriteRule`. Seam-and-panic to unported owners: `RenameConstraint`/`RenameRelation`/`renameatt`/`renametrig`/`rename_policy`/`RenameType` and `RenameDatabase`. The generic AGGREGATE/.../SUBSCRIPTION tail: `get_object_address(AccessExclusiveLock)` → `table_open(RowExclusiveLock)` → `AlterObjectRename_internal` → `table_close(RowExclusiveLock)`; default → "unrecognized rename stmt type". |
| `ExecAlterObjectDependsStmt` (470-522) | `ExecAlterObjectDependsStmt` | MATCH | `get_object_address_rv` → `check_object_ownership(GetUserId, objectType, address, object, rel)` → `table_close(rel, NoLock)` (keep lock to commit) → `get_object_address(OBJECT_EXTENSION, extname, AccessExclusiveLock)`; the `*refAddress` out-param; the `remove` → `deleteDependencyRecordsForSpecific(.., DEPENDENCY_AUTO_EXTENSION, ..)` vs `getAutoExtensionsOfObject` + `!list_member_oid` → `recordDependencyOn(DEPENDENCY_AUTO_EXTENSION)`. All called directly (acyclic). |
| `ExecAlterObjectSchemaStmt` (533-608) | `ExecAlterObjectSchemaStmt` | MATCH | `objectType` dispatch: EXTENSION→`AlterExtensionNamespace` (seam), table-family→`AlterTableNamespace` (seam), DOMAIN/TYPE→`AlterTypeNamespace` (seam), generic→`get_object_address`→`table_open`→`LookupCreationNamespace`→`AlterObjectNamespace_internal`→`table_close`; the `oldSchemaAddr ? &oldNspOid : NULL` carried as a `want_old` bool + the second tuple slot; `ObjectAddressSet(*oldSchemaAddr, NamespaceRelationId, oldNspOid)`; default → "unrecognized AlterObjectSchemaStmt type". |
| `AlterObjectNamespace_oid` (624-678) | `AlterObjectNamespace_oid` | MATCH | RELATION arm: `relation_open(AccessExclusiveLock)` → `RelationGetNamespace` (`rd_rel.relnamespace`) → `AlterTableNamespaceInternal` (seam) → `relation_close(NoLock)` (via `table_close`); TYPE arm: `AlterTypeNamespace_oid(objid, nspOid, true, objsMoved)` (seam); the generic procedure/collation/.../TSConfig list: `table_open(RowExclusiveLock)` → `AlterObjectNamespace_internal` → `table_close`; default arm `Assert(get_object_attnum_namespace == InvalidAttrNumber)` mirrored as a `debug_assert_eq!`. |
| `AlterObjectNamespace_internal` (691-830, static) | `AlterObjectNamespace_internal` | MATCH | In-crate: metadata block; `SearchSysCache1` → "cache lookup failed"; name + namespace reads; the already-in-namespace short-circuit (fire hook, return old); `CheckSetNamespace`; superuser-bypass permission block ("must be superuser to set schema of %s"; owner `has_privs_of_role`; new-namespace `ACL_CREATE`); dup-name probes against `nspOid` (proc/collation/opclass/opfamily `IsThere*`, else `nameCacheId` `SearchSysCacheExists2(name, nspOid)` → `report_namespace_conflict`); namespace-column `heap_modify_tuple` + `CatalogTupleUpdate`; `changeDependencyFor(.., NamespaceRelationId, oldNspOid, nspOid) != 1` → "could not change schema dependency"; `InvokeObjectPostAlterHook`. |
| `ExecAlterOwnerStmt` (836-911) | `ExecAlterOwnerStmt` | MATCH | `get_rolespec_oid(newowner, false)` (RoleSpec view bridged to the acl helper's `parsenodes::RoleSpec`); the `objectType` dispatch — DATABASE→`AlterDatabaseOwner` (seam), SCHEMA→`AlterSchemaOwner` (direct), TYPE/DOMAIN→`AlterTypeOwner` (seam), FDW/FOREIGN_SERVER→`AlterForeign*Owner` (direct), EVENT_TRIGGER/PUBLICATION/SUBSCRIPTION→seams; generic AGGREGATE/.../TSCONFIGURATION→`get_object_address`→`AlterObjectOwner_internal`; default → "unrecognized AlterOwnerStmt type". |
| `AlterObjectOwner_internal` (925-1063) | `AlterObjectOwner_internal` | MATCH (one encapsulated primitive) | In-crate: large-object `catalogId` redirect to `pg_largeobject_metadata`; the five `get_object_attnum_*(catalogId)` reads; `table_open(catalogId, RowExclusiveLock)`; `get_catalog_object_by_oid_extended(rel, anum_oid, objectId, true)` → "cache lookup failed"; owner + namespace column reads (`heap_deform_tuple` by attnum); the `old != new` block — superuser-bypass (`has_privs_of_role(old)` → `aclcheck_error(NOT_OWNER, get_object_type, name|"%u")`; `check_can_set_role(new)`; new-owner namespace `ACL_CREATE`), then the modified-tuple write (owner column + `aclnewowner` ACL rewrite + `heap_modify_tuple` + `CatalogTupleUpdate` + `UnlockTuple`) via `update_object_owner_tuple`, then `changeDependencyOnOwner(classId, ..)`; the same-owner `else` → `UnlockTuple(oldtup->t_self, InplaceUpdateTupleLock)` (direct lmgr seam); `InvokeObjectPostAlterHook(classId, ..)` (classId, not catalogId); `table_close`. |

No functions MISSING / PARTIAL / DIVERGES.

## Independent re-audit (2026-06-15)

An independent pass (re-derived from the C + c2rust-generated `Anum_*`) found
two wrong hardcoded catalog attribute numbers in the dup-name / password probes,
now FIXED and re-verified against `c2rust-runs/.../pg_proc.rs` /
`subscriptioncmds.rs`:
* `Anum_pg_proc_pronargs` 19 → **17** (19 is `prorettype`; c2rust:
  `Anum_pg_proc_pronargs = 17`). Affected `AlterObjectRename_internal` and
  `AlterObjectNamespace_internal` function dup-name probes.
* `Anum_pg_subscription_subpasswordrequired` 10 → **11** (10 is
  `subdisableonerr`; c2rust: `subpasswordrequired = 11`). Affected the
  subscription non-superuser password-required guard in
  `AlterObjectRename_internal`.

All other constants (proargtypes=20, pronamespace=3, proname=2, collname=2,
collnamespace=3, opcname=3/opcnamespace=4/opcmethod=2,
opfname=3/opfnamespace=4/opfmethod=2, publication oid=1/puballtables=4) verified
correct. Every dispatch switch, every error SQLSTATE/message, the case
groupings, and the seam delegation were confirmed faithful.

## Reconciliation note (the one encapsulated primitive)

`AlterObjectOwner_internal`'s modified-tuple write delegates to a new generic
seam `backend_catalog_indexing_seams::update_object_owner_tuple(rel, anum_oid,
object_id, anum_owner, anum_acl, old_owner, new_owner)`. Reason: re-serializing
an arbitrary `aclitem[]` varlena (`aclnewowner`'s result) back into a modified
tuple column has no owned-model counterpart at this layer. Every existing
owner-change in the repo (schemacmds `update_namespace_owner_tuple`, foreigncmds'
owner writers) uses an equivalent per-catalog typed seam for exactly this step;
this is the generic analogue, declared on the indexing owner (`todo`, so the
recurrence guard exempts it) and loud-panicking until indexing fills it. All
dispatch, metadata, permission, and dependency logic of the owner path is
in-crate. The RENAME and SET-SCHEMA `_internal` bodies touch no ACL and are
fully in-crate (name/namespace column replace + `changeDependencyFor`).

## Cross-crate seams installed/added

This crate owns one inward seam, `backend-commands-alter-seams`'s
`alter_object_owner_internal(class_id, object_id, new_owner_id)`, consumed by
`pg_shdepend.c`'s `shdepReassignOwned`. `init_seams()` installs it onto
`AlterObjectOwner_internal` (an mcx is created in the installer closure) and is
wired into `init_all()`. The other ALTER drivers (`ExecRenameStmt` /
`ExecAlterObjectSchemaStmt` / `ExecAlterOwnerStmt` / `ExecAlterObjectDependsStmt`)
are reached only from the still-unported utility.c, so they need no inward seam
yet.

New seam DECLARATIONS added on the per-type ALTER subroutine owners (all
loud-panic until the owner lands; owners are `todo`/`in-progress`/`needs-decomp`
so the recurrence guard exempts them):
`tablecmds-seams`: `RenameRelation`/`renameatt`/`RenameConstraint`/
`AlterTableNamespace`/`AlterTableNamespaceInternal`;
`typecmds-seams`: `RenameType`/`AlterTypeNamespace`/`AlterTypeNamespace_oid`/
`AlterTypeOwner`;
`dbcommands-seams`: `RenameDatabase`/`AlterDatabaseOwner`;
`extension-seams`: `AlterExtensionNamespace`;
`trigger-seams`: `renametrig`;
`policy-seams`: `rename_policy`;
`event-trigger-seams`: `AlterEventTriggerOwner`;
`publicationcmds-seams`: `AlterPublicationOwner`/`InvalidatePubRelSyncCache`;
`subscriptioncmds-seams`: `AlterSubscriptionOwner`;
`logical-worker-seams`: `LogicalRepWorkersWakeupAtCommit` (worker.c is its real
owner, not launcher.c);
`indexing-seams`: `update_object_owner_tuple`.

New consumer-shape statement nodes added to `types-parsenodes`: `RenameStmt`,
`AlterObjectDependsStmt`, `AlterObjectSchemaStmt`, `AlterOwnerStmt` (the trimmed
command-driver views, matching the `CommentStmt`/`DropStmt` precedent; `relation`
modeled as `Option<types_tuple::access::RangeVar>`, `object`/`newowner`/`extname`
as `Option<Box<Node>>`).

## Helpers

`get_object_class_meta` = the `get_object_catcache_*`/`get_object_attnum_*`
block; `object_address_set` = `ObjectAddressSet`; `str_val`/`role_spec_oid`/
`owner_obj_str` = the `strVal`/`castNode(RoleSpec)` reads (the RoleSpec view is
reprojected onto the acl helper's `parsenodes::RoleSpec`); `name_text_of`/`oid_of`
= `NameStr(DatumGetName)`/`DatumGetObjectId`; `read_attr`/`read_attr_oid` =
`heap_getattr` against the opened relation's `rd_att` (one-shot
`heap_deform_tuple`); `check_duplicate_name` = the shared dup-name probe switch
of both `_internal` bodies.

## Gate

`cargo check --workspace` green; `cargo test -p backend-commands-alter` (2 tests)
green; recurrence guards (`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) green; no `todo!()`/
`unimplemented!()`/`unreachable!()` in the crate.

## Verdict: PASS

Every function MATCH or correctly SEAMED (the single `update_object_owner_tuple`
varlena-write delegation per the rules). Zero seam findings. The two attnum
findings from the independent pass are fixed and re-verified.
