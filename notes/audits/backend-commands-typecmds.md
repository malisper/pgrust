# Audit: backend-commands-typecmds (commands/typecmds.c)

Status: **merged** — all families (F2 CREATE, F3 DOMAIN, F4 ALTER/RENAME/namespace) ported.

C source: `src/backend/commands/typecmds.c` (4706 LOC). Ported function-by-function against PostgreSQL 18.3. No `todo!`/`unimplemented!`. All deferrals are real seam `::call` into their owner crates (mirror-PG-and-panic), or genuine `ereport(ERROR)` mapped through `PgResult` with the post-ereport `unreachable!()` idiom.

Gate: `cargo check --workspace` clean; `no-todo-guard` green; both `seams-init::recurrence_guard` tests (`every_seam_installing_crate_is_wired_into_init_all`, `every_declared_seam_is_installed_by_its_owner`) green.

## Architecture: zero datum writes in typecmds

All `pg_type` catalog read-modify-write crosses through the **pg_type owner** (`backend-catalog-pg-type`) via mutate seams added to `backend-catalog-pg-type-seams` and installed in that owner's `init_seams()`:

- `set_type_owner(type_oid, new_owner_id)` — `AlterTypeOwnerInternal`'s single-row typowner + `aclnewowner` typacl recompute (the array/multirange recursion is driven from typecmds).
- `set_type_namespace(type_oid, nsp)` — `AlterTypeNamespaceInternal`'s typnamespace write.
- `set_type_not_null(type_oid, bool)` — `AlterDomain{NotNull,AddConstraint,DropConstraint}`.
- `set_domain_default(type_oid, value, bin)` — `AlterDomainDefault`'s typdefault/typdefaultbin write + `GenerateTypeDependencies` rebuild + post-alter hook.
- `alter_type_recurse_update(type_oid, is_implicit_array, attr)` — `AlterType`/`AlterTypeRecurse` column update + deps + hook, returns typarray for recursion.
- `scan_domains_over_basetype(...)` — `get_rels_with_domain`'s pg_type scan.

These call narrow indexing seams (`catalog_tuple_update_{typowner_typacl,typnamespace,typnotnull,typdefault,attrs}_pg_type`) declared in `backend-catalog-indexing-seams`, **uninstalled = panic** — consistent with the pre-existing uninstalled `catalog_tuple_{insert,update}_pg_type` (the catalog-indexing decomposition installs them later; the guard exempts indexing-seams).

## F2 — CREATE paths (landed earlier, unchanged)

DefineType / RemoveTypeById / DefineEnum / DefineRange / DefineCompositeType + all `find*` and `AssignType*` helpers. Verified faithful.

## F3 — DOMAIN family (ported)

- **checkDomainOwner** (C 3485): typtype==DOMAIN check + `object_ownercheck` / `aclcheck_error_type`. Faithful.
- **DefineDomain** (C 697): full base-type inheritance, the `saw_default`/`typNotNull` DefElem loop, the not-null + check constraint sub-loops, base + array `TypeCreate`, CCI. The default-cook and CHECK-constraint expression paths seam-and-panic (see below).
- **AlterDomainDefault** (C 2614): TypeName lookup, checkDomainOwner, DROP-DEFAULT vs SET-DEFAULT; the set-default expression cook (`cook_default` → `deparse_expression` → `node_to_string`) seam-and-panics; the catalog write is `set_domain_default`.
- **AlterDomainNotNull** (C 2743): SET → `domain_add_not_null_constraint` + `validate_domain_not_null_constraint` (seams); DROP → `drop_domain_constraint` (pg_constraint owner, installed); catalog flag via `set_type_not_null`.
- **AlterDomainDropConstraint** (C 2829): pg_constraint scan + `performDeletion` delegated to the installed `drop_domain_constraint` seam returning `(found, was_notnull)`; the `missing_ok` NOTICE/ERROR + `set_type_not_null` clear + `cache_invalidate_heap_tuple` driven in typecmds.
- **AlterDomainAddConstraint** (C 2935): CHECK arm → `domain_add_check_constraint` + `validate_domain_check_constraint`; NOTNULL arm → `domain_add_not_null_constraint` + `validate_domain_not_null_constraint` + `set_type_not_null`. Faithful dispatch.
- **AlterDomainValidateConstraint** (C 3032): `find_domain_check_constraint` (installed; scan + conbin detoast) → executor `validate_domain_check_constraint` (seam) → `set_constraint_validated` (installed) → post-alter hook.

## F4 — ALTER/RENAME/namespace (ported)

- **RenameType** (C 3740): ownership, ALTER-DOMAIN-on-non-domain / table-rowtype / array / composite branches; composite → `rename_relation_internal` (tablecmds seam, uninstalled), else `RenameTypeInternal` (pg_type owner pub fn). Faithful.
- **AlterTypeOwner** (C 3821): uses the new shell-allowing resolver `lookup_type_name_oid_from_names` (parse-type owner) — matches C `LookupTypeName(...false)` semantics so a shell type is reassignable. All wrong-object-type guards + superuser/owner/CREATE-on-namespace ACL checks; delegates to `AlterTypeOwner_oid`.
- **AlterTypeOwner_oid** (C 3946): the **real inward seam body** (was an F4 panic placeholder). Composite → `at_exec_change_owner` (tablecmds seam), else `AlterTypeOwnerInternal`; `changeDependencyOnOwner` when `hasDependEntry`; post-alter hook.
- **AlterTypeOwnerInternal** (C 3986): `set_type_owner` then recurses to typarray and (for RANGE) the multirange via `get_range_multirange`. Faithful recursion.
- **AlterTypeNamespace / _oid / Internal** (C 4054-4310): ownership + array-type guard; `AlterTypeNamespaceInternal` does the objsMoved-dedup (in-crate `PgVec<ObjectAddress>` + `object_address_present`/`add_exact_object_address`), CheckSetNamespace, duplicate-name check, composite vs domain constraint-namespace moves (`alter_relation_namespace_internal` tablecmds seam / `alter_constraint_namespaces` pg_constraint owner), `changeDependencyFor`, post-alter hook, and the implicit-array recursion. Faithful.
- **AlterType / AlterTypeRecurse** (C 4311-end): the SET (...) option decode + `alter_type_recurse_update` recursion over the implicit array. Faithful.

## Seam-and-panic leaves (sanctioned mirror-PG-and-panic; genuinely unported owners)

All co-located in the default-cook + constraint-execution paths:

- typecmds-owned statics declared in `backend-commands-typecmds-seams`: `cook_default` (parser parse_expr unported), `deparse_expression` (ruleutils.c NEEDS_DECOMP), `node_to_string` (co-gated with the above two — the whole default-store block panics on the first `cook_default` call), `domain_add_check_constraint` / `domain_add_not_null_constraint` (pg_constraint CreateConstraintEntry + parser cook), `validate_domain_not_null_constraint` / `validate_domain_check_constraint` (executor scan over `get_rels_with_domain` rels, executor unported).
- cross-owner: `at_exec_change_owner` / `rename_relation_internal` / `alter_relation_namespace_internal` (no tablecmds owner crate yet), `cache_invalidate_heap_tuple` (inval owner installs later).
- F2 carryover: `make_range_constructors` / `make_multirange_constructors` (ProcedureCreate), `define_relation_composite` (DefineRelation).

## Cross-crate changes made by this port

- `backend-catalog-pg-type` / `-seams`: 6 new installed mutate/scan seams + their narrow indexing-seam decls.
- `backend-catalog-pg-constraint` / `-seams`: `drop_domain_constraint`, `find_domain_check_constraint`, `set_constraint_validated`, `find_domain_not_null_constraint_oid`, `alter_constraint_namespaces` — all INSTALLED in the owner's `init_seams()` (no allowlist entries; the recurrence guard confirms). `types_catalog::pg_constraint::ConstraintFieldUpdate` widened with `convalidated` (5 existing construction sites carry the row's existing value through).
- `backend-parser-parse-type` / `-seams`: new installed `lookup_type_name_oid_from_names` (shell-allowing OID resolver).
