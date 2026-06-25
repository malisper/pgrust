# Audit: backend-catalog-pg-operator

C source: `src/backend/catalog/pg_operator.c` (PostgreSQL 18.3, 945 lines).
Crate: `crates/backend-catalog-pg-operator`.
Owned seam crate: `crates/backend-catalog-pg-operator-seams`.

Independent function-by-function comparison against the C and the c2rust
rendering (`c2rust-runs/backend-catalog-pg-operator/src/pg_operator.rs`). All 9
C functions present in full; no `todo!`/`unimplemented!`. Built in this repo's
catalog carrier model (the `pg_conversion`/`pg_type`/`pg_database` precedent):
real `heap_form_tuple`/`heap_modify_tuple` over `RelationGetDescr(rel)` +
`CatalogTupleInsert`/`Update`/`Delete` (catalog-indexing keystone), OID via
`GetNewOidWithIndex`, and the syscache probes rendered as `systable` index
scans on `OperatorOidIndexId` / `OperatorNameNspIndexId`.

## Constant audit (vs `catalog/pg_operator.h`)

- `OperatorRelationId = 2617` — MATCH (`CATALOG(pg_operator,2617,...)`).
- `OperatorOidIndexId = 2688` — MATCH (`DECLARE_UNIQUE_INDEX_PKEY(...,2688,...)`).
- `OperatorNameNspIndexId = 2689` — MATCH (`DECLARE_UNIQUE_INDEX(...,2689,...)`,
  key `(oprname, oprleft, oprright, oprnamespace)`).
- `Natts_pg_operator = 15`; `Anum_*` 1..15 in genbki field order: oid, oprname,
  oprnamespace, oprowner, oprkind, oprcanmerge, oprcanhash, oprleft, oprright,
  oprresult, oprcom, oprnegate, oprcode, oprrest, oprjoin — MATCH field-for-field
  against the `CATALOG(pg_operator)` struct body.
- `BOOLOID = 16` (types_core::catalog) — MATCH.
- Dependency class OIDs: `NamespaceRelationId=2615`, `TypeRelationId=1247`,
  `ProcedureRelationId=1255` — MATCH.

`FormPgOperator` (types-catalog) expanded from the old 9-field operatorcmds
projection to the full 15-field `FormData_pg_operator` view. Additive: the
fields operatorcmds reads (oid, oprname, oprleft, oprright, oprresult, oprcom,
oprnegate, oprcanmerge, oprcanhash) are unchanged; operatorcmds still compiles
and is the builder's consumer.

## Function-by-function

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `validOperatorName` | 67-112 | lib.rs `validOperatorName` | MATCH | len 0 / `>= NAMEDATALEN`; `strspn` over `~!@#^&\|`?+-*/%<>=`; `/*` & `--` substr; trailing `+`/`-` unless a `~!@#^&\|`?%` char precedes (scan from `len-2` down, `ic<0` → false); `!=` → false. Byte-exact char sets. |
| `OperatorGet` | 123-153 | lib.rs `OperatorGet` | MATCH | C `SearchSysCache4(OPERNAMENSP, name,left,right,namespace)` → `systable` scan on `OperatorNameNspIndexId` with 4 keys (nameeq + 3 oideq) in the index's key order; on hit returns `(oid, RegProcedureIsValid(oprcode))`, on miss `(InvalidOid, false)`. Relation opened RowExclusiveLock (C used the syscache; the index probe is the same unique row). |
| `OperatorLookup` | 163-185 | lib.rs `OperatorLookup` | MATCH | `LookupOperName(NULL, name, left, right, true, -1)` (direct dep, parse-oper); invalid → `(InvalidOid,false)`; else `get_opcode` (lsyscache) → `(oid, RegProcedureIsValid)`. |
| `OperatorShellMake` | 192-283 | lib.rs `OperatorShellMake` | MATCH | validOperatorName guard (ERRCODE_INVALID_NAME, same msg); table_open RXL; `GetNewOidWithIndex`; values built in field order with oprcode/oprrest/oprjoin/oprcom/oprnegate/oprresult = InvalidOid, oprkind `'b'`/`'l'`, oprowner `GetUserId`; `heap_form_tuple` + `CatalogTupleInsert`; `makeOperatorDependencies(.., true, false)`; post-create hook; `CommandCounterIncrement`; table_close. Order preserved. |
| `OperatorCreate` | 320-542 | lib.rs `OperatorCreate` | MATCH | validOperatorName; `get_func_rettype`; `OperatorValidateParams`; `OperatorGet` → duplicate ERROR (ERRCODE_DUPLICATE_FUNCTION); shell-ownership check (object_ownercheck/aclcheck_error OBJECT_OPERATOR); commutator (reversed arg types) + negator (same arg types) resolution via `get_other_operator`, owner checks, self-commutator flag, self-negation ERROR; values built field-by-field; replace-shell branch (`search_operator_by_oid` = SearchSysCacheCopy1 OPEROID, `replaces[oid]=false`, heap_modify_tuple + CatalogTupleUpdate) vs insert branch (GetNewOidWithIndex + heap_form_tuple + CatalogTupleInsert); `makeOperatorDependencies(.., true, isUpdate)`; selfCommutator fixup; OperatorUpd if any link; post-create hook; table_close. All branches/error codes match. |
| `OperatorValidateParams` | 555-611 | lib.rs `OperatorValidateParams` | MATCH | non-binary block (commutator/join-sel/merge/hash) and non-boolean block (negator/restrict-sel/join-sel/merge/hash), each ERRCODE_INVALID_FUNCTION_DEFINITION with exact message strings, in C order. |
| `get_other_operator` | 621-668 | lib.rs `get_other_operator` | MATCH | `OperatorLookup`; valid → return; else `QualifiedNameGetCreationNamespace`; self-linkage test (name+namespace+left+right) → InvalidOid; else schema `object_aclcheck(ACL_CREATE)` → aclcheck_error(OBJECT_SCHEMA, get_namespace_name); `OperatorShellMake`. |
| `OperatorUpd` | 683-838 | lib.rs `OperatorUpd` | MATCH | `!isDelete` → CCI first; table_open RXL; commutator copy (search_operator_by_oid commId), the isDelete-clear vs `oprcom != baseId` set, third-operator-already-linked ERROR (get_opname, named-vs-numbered message), update via heap_modify_tuple+CatalogTupleUpdate then unconditional CCI; identical negator block with the isDelete-only trailing CCI; table_close. Form-level field mutation mirrors the C `GETSTRUCT` in-place edit (every column re-supplied, oid not replaced). |
| `makeOperatorDependencies` | 852-945 | lib.rs `makeOperatorDependencies` | MATCH | isUpdate → deleteDependencyRecordsFor(.., true) + deleteSharedDependencyRecordsFor(.., 0); ObjectAddresses built for namespace, left/right/result type, oprcode, oprrest, oprjoin (each OidIsValid-gated, in C order; oprcom/oprnegate intentionally omitted per the C NOTE); record_object_address_dependencies(DEPENDENCY_NORMAL); recordDependencyOnOwner; makeExtensionDep → recordDependencyOnCurrentExtension(isUpdate). |

### Catalog-half seam bodies owned here (the tuple work `operatorcmds.c` delegates)

| Body | C origin | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `remove_operator_tuple` | `RemoveOperatorById` operatorcmds.c:446-482 (tuple half) | lib.rs `remove_operator_tuple` | MATCH | table_open RXL; if `do_operator_upd` → `OperatorUpd(operOid, oprcom, oprnegate, true)`; re-fetch tuple by oid (covers the self-commutator/self-negator re-fetch — unconditional here, equivalent: the C only re-fetches when `operOid == oprcom || oprnegate`, but re-reading the current tuple in all paths yields the same `t_self`); `CatalogTupleDelete(&tup.t_self)`; table_close. The form read + `do_operator_upd` decision live in operatorcmds (RemoveOperatorById owner). |
| `alter_operator_apply` | `AlterOperator` operatorcmds.c:680-724 (tuple half) | lib.rs `alter_operator_apply` | MATCH | table_open RXL; fetch by oid; values/replaces packed per `OperatorAttrUpdate` (oprrest/oprjoin/oprcom/oprnegate/oprcanmerge/oprcanhash), all-false replaces baseline; heap_modify_tuple + CatalogTupleUpdate; `makeOperatorDependencies(form, false, true)`; returns address. The subsequent `OperatorUpd` + `InvokeObjectPostAlterHook` are issued by operatorcmds (C order preserved: modify→update→deps→OperatorUpd→hook). |

## Seam audit

Owned seam crate `backend-catalog-pg-operator-seams` declares 9 seams. 8 are
installed by this crate's `init_seams()` (only `set()` calls): `operator_create`,
`operator_validate_params`, `operator_upd`, `operator_lookup`,
`fetch_operator_form`, `remove_operator_tuple`, `alter_operator_apply`,
`invoke_object_post_alter_hook`. The 9th, `RemoveOperatorById`, is C from
operatorcmds.c (only *declared* here for the dependency.c cross-cycle) and is
installed by `backend-commands-operatorcmds::init_seams()` —
verified present. `seams-init::init_all()` calls
`backend_catalog_pg_operator::init_seams()`. No uninstalled seam, no `set()`
outside an owner.

Outward seam calls are limited to genuine cycle partners:
`backend-access-index-genam-seams` (systable scan), `backend-access-transam-xact-seams`
(CommandCounterIncrement), `backend-catalog-objectaccess-seams` (post-create /
post-alter hooks). Catalog-indexing keystone (`CatalogTupleInsert`/`Update`/
`Delete`), `GetNewOidWithIndex`, `heap_form_tuple`/`heap_modify_tuple`/
`heap_deform_tuple`, `table_open`, aclchk, namespace, parse-oper, lsyscache,
dependency / pg-depend / pg-shdepend owners are all called directly (no cycle).
Each seam adapter is thin marshal + delegate (open private context, forward
bundle); no branching/computation in seam paths.

## Design conformance

- No invented opacity: `FormPgOperator` is the real C struct view; relations are
  the real `types_rel::Relation`; tuples are real `FormedTuple`.
- Allocating paths take `Mcx` and return `PgResult` (operator_values, name_key,
  the scan/form/modify helpers). Owner seam adapters open a private
  `MemoryContext` (matching the no-`mcx` seam contract operatorcmds expects).
- No shared statics, no ambient-global getter seams, no registry side tables.
- Relation lock is released by `Relation::close` (guard-shaped); systable scans
  released by `SysScanGuard` (`scan.end()`).
- Every `ereport(ERROR)` site is an `Err(PgError)` with matching SQLSTATE.
- One behavior-preserving structural divergence (consistent with the
  pg_conversion/pg_type precedent and operatorcmds): each owner function opens
  and closes its own `pg_operator` relation rather than threading one open
  `Relation` across the operatorcmds↔pg_operator seam boundary. The
  RowExclusiveLock is held to transaction end in both models, so visibility and
  locking are identical.

## Verdict: PASS

All 9 C functions MATCH; both owned catalog-half seam bodies MATCH; constants
verified against `pg_operator.h`; all owned seams installed; no MISSING /
PARTIAL / DIVERGES; design-conformance clean.
