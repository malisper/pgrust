# Audit: backend-commands-schemacmds

C source: `src/backend/commands/schemacmds.c` (442 lines).
Port: `crates/backend-commands-schemacmds/src/lib.rs`.
Audited independently against the C and the c2rust rendering.

## Function inventory and verdicts

| C function (location) | Port location | Verdict | Notes |
|---|---|---|---|
| `CreateSchemaCommand` (51-242) | `CreateSchemaCommand` | MATCH | Branch order, permission ordering (DB ACL_CREATE → check_can_set_role → reserved-name → IF NOT EXISTS), SQLSTATEs (RESERVED_NAME/DUPLICATE_SCHEMA), the SetUserIdAndSecContext flip, NamespaceCreate, CCI, NewGUCNestLevel, quote_identifier + search-path trim/prepend, set_search_path_save (GUC_ACTION_SAVE), EventTriggerCollectSimpleCommand, the transform + ProcessUtility-subcommand + CCI loop, AtEOXact_GUC(true), final SetUserIdAndSecContext restore — all reproduced. NOTICE skip returns InvalidOid; reserved-name `errdetail` text exact. |
| `RenameSchema` (248-304) | `RenameSchema` | MATCH | NAMESPACENAME lookup → UNDEFINED_SCHEMA; new-name existence → DUPLICATE_SCHEMA; object_ownercheck → ACLCHECK_NOT_OWNER OBJECT_SCHEMA(oldname); DB ACL_CREATE → aclcheck_error OBJECT_DATABASE; reserved-name guard; rename tuple write; InvokeObjectPostAlterHook; ObjectAddressSet. table_open/SearchSysCacheCopy1/namestrcpy/CatalogTupleUpdate/table_close/heap_freetuple are encapsulated by the indexing-owned `rename_namespace_tuple` seam (same pattern as foreigncmds' owner-change). |
| `AlterSchemaOwner_oid` (306-323) | `AlterSchemaOwner_oid` | MATCH | NAMESPACEOID lookup → `elog(ERROR, "cache lookup failed for schema %u")` on miss; AlterSchemaOwner_internal; (ReleaseSysCache/table_close encapsulated). Installed as the crate's inward seam. |
| `AlterSchemaOwner` (329-358) | `AlterSchemaOwner` | MATCH | NAMESPACENAME lookup → UNDEFINED_SCHEMA; AlterSchemaOwner_internal; ObjectAddressSet(nspOid). |
| `AlterSchemaOwner_internal` (360-442, static) | `AlterSchemaOwner_internal` | MATCH | same-owner short-circuit; object_ownercheck → ACLCHECK_NOT_OWNER OBJECT_SCHEMA(nspname); check_can_set_role(GetUserId, new); DB ACL_CREATE check (current user, per the C NOTE); the nspowner replacement + nspacl `aclnewowner` rewrite + heap_modify_tuple/CatalogTupleUpdate encapsulated by `update_namespace_owner_tuple`; changeDependencyOnOwner; InvokeObjectPostAlterHook. The two `Assert(... == NamespaceRelationId)` are static invariants subsumed by the typed seam. |

No functions MISSING / PARTIAL / DIVERGES.

## Helpers

`object_address_set` / `invalid_object_address` = `ObjectAddressSet` /
`InvalidObjectAddress`; `scanner_isspace` = scansup.c's class (space/tab/nl/cr/ff);
`aclcheck_error_database` = the shared `aclcheck_error(_, OBJECT_DATABASE,
get_database_name(MyDatabaseId))`; `stmt_authrole` downcasts the owned
`Node::RoleSpec` arena node to the `parsenodes::RoleSpec` view the
`get_rolespec_oid` seam reads (same roletype/rolename), malformed tag →
`errmsg_internal`.

## Seam audit

Inward (owned) — `backend-commands-schemacmds-seams::alter_schema_owner_oid`:
installed by this crate's `init_seams()` (the only owned seam), wired into
`seams-init::init_all()`. The installer is a thin closure (new MemoryContext +
delegate to `AlterSchemaOwner_oid`); consumed by `backend-catalog-pg-shdepend`
(shdepReassignOwned).

Outward calls — all justified by real cycles or unported owners; each is a thin
marshal+delegate:
- Direct deps (no cycle): `IsReservedName` (catalog), `get_namespace_oid` +
  `namespace_search_path` (namespace), `NamespaceCreate` (pg-namespace),
  `checkMembershipInCurrentExtension` (pg-depend).
- Existing owner seams: miscinit (get/set_user_id_and_sec_context, get_user_id),
  init-small (my_database_id), guc (allow_system_table_mods, new_guc_nest_level,
  at_eoxact_guc, **set_search_path_save** [new; installed by guc owner]),
  syscache (authid_rolname, **namespace_owner_row_by_{name,oid}** [new]),
  acl (get_rolespec_oid, check_can_set_role), ruleutils (quote_identifier),
  aclchk (object_aclcheck, object_ownercheck, aclcheck_error),
  objectaccess (invoke_object_post_alter_hook), xact (command_counter_increment),
  dbcommands (get_database_name), pg-shdepend (changeDependencyOnOwner).
- New outward seams into unported owners (mirror-and-panic): indexing
  (**rename_namespace_tuple**, **update_namespace_owner_tuple**), event-trigger
  (**event_trigger_collect_simple_command_create_schema**), tcop-utility-fc
  (**process_utility_create_schema_subcommand**), parse-utilcmd
  (**transformCreateSchemaStmtElements**, new -seams crate).

The `rename_namespace_tuple` / `update_namespace_owner_tuple` seams bundle
table_open + SearchSysCacheCopy + namestrcpy/heap_modify_tuple +
CatalogTupleUpdate at the catalog owner — the established repo encapsulation
(foreigncmds' `catalog_tuple_update_owner_pg_foreign_server`); behavior-
preserving (re-fetch by oid under RowExclusiveLock). All policy/permission/
ordering logic stays in this crate.

## Design conformance

- Allocating paths take `Mcx<'mcx>` and return `PgResult` (authid_rolname,
  get_database_name, quote_identifier, transform, the RoleSpec view copy). No
  ambient-context assumption crosses a seam.
- No invented opacity: the namespace row crosses as `(Oid, Oid, PgString)`
  projected fields, not a handle. No shared statics, no registry side tables, no
  locks held across `?` (table_open/close encapsulated atomically in the seam).
- `elog(ERROR, "cache lookup failed ...")` modeled as `PgError::error` (default
  ERROR severity), matching siblings.

## Verdict: PASS

All five C functions MATCH or are SEAMED per the rules. Owned inward seam
installed and wired. No seam or design findings.
