# Audit: backend-commands-dropcmds

Unit: `backend-commands-dropcmds` (`src/backend/commands/dropcmds.c`, PostgreSQL 18.3)
Branch: `port/backend-commands-dropcmds`
Auditor: independent re-derivation from C + c2rust (`../pgrust/c2rust-runs/backend-commands-dropcmds/src/dropcmds.rs`).

## Verdict: PASS

Every dropcmds.c function is present with logic matching the C exactly; all
cross-subsystem callees cross real seams into named unported owners or call
directly into the ported namespace crate. No own-logic stubs, no
deferred/SEAMED-equivalent escapes for in-crate logic. `init_seams()` is wired
into `seams-init::init_all()`; both recurrence_guard tests pass.

## Function inventory

dropcmds.c defines 5 functions (1 extern + 4 statics). c2rust additionally
emitted three inlined `pg_list.h` helpers (`list_length`, `list_nth_cell`,
`list_last_cell`) used by the `linitial`/`lsecond`/`llast`/`list_copy_*` macros;
the port realizes these as in-crate slice helpers.

| C function | C lines | Port location | Verdict |
|---|---|---|---|
| `RemoveObjects` | 52-126 | lib.rs:114 `remove_objects` | MATCH |
| `owningrel_does_not_exist_skipping` | 138-160 | lib.rs:210 | MATCH |
| `schema_does_not_exist_skipping` | 173-190 | lib.rs:243 | MATCH |
| `type_in_list_does_not_exist_skipping` | 205-232 | lib.rs:278 | MATCH |
| `does_not_exist_skipping` | 242-524 | lib.rs:313 | MATCH |
| `list_length`/`list_nth_cell`/`list_last_cell` (c2rust inlined macros) | pg_list.h | lib.rs:720-749 helpers | MATCH |

### Per-function detail

**RemoveObjects** — per-object loop over `stmt->objects`; resolve via
`get_object_address` (AccessExclusiveLock, missing_ok); `!OidIsValid(objectId)`
→ `does_not_exist_skipping` + continue (with `debug_assert!(missing_ok)` mirroring
the C `Assert`); OBJECT_FUNCTION + `get_func_prokind == PROKIND_AGGREGATE` →
`ereport(ERROR, ERRCODE_WRONG_OBJECT_TYPE, "...is an aggregate function" + hint)`.
The C has no `else` after this ereport (it longjmps); the port `return`s the Err,
behaviorally identical since ereport(ERROR) never returns. Ownership shortcut
`!OidIsValid(namespaceId) || !object_ownercheck(NamespaceRelationId, namespaceId,
GetUserId())` → `check_object_ownership`. Temp-namespace flag via
`set_xact_accessed_temp_namespace` (== `MyXactFlags |=
XACT_FLAGS_ACCESSEDTEMPNAMESPACE`). `table_close(relation, NoLock)` via
`Relation::close`. `add_exact_object_address` → `objects.push`.
`performMultipleDeletions(objects, behavior, 0)`. `free_object_addresses` is the
owned Vec drop. Constant `PROKIND_AGGREGATE = b'a'` verified vs pg_proc.h.
`NamespaceRelationId` = `NAMESPACE_RELATION_ID` (2615). ErrorLocation lineno 94
(the C ereport statement; c2rust's errfinish carries 98 — both name dropcmds.c;
cosmetic).

**owningrel_does_not_exist_skipping** — `parent_object = list_copy_head(object,
len-1)`; schema probe; else `makeRangeVarFromNameList` + `RangeVarGetRelid(rel,
NoLock, true)` invalid → relation-does-not-exist msg with
`NameListToString(parent_object)`. Returns `Ok(None)` (false) / `Ok(Some)` (true)
matching the C bool + out-params (msg,name).

**schema_does_not_exist_skipping** — `makeRangeVarFromNameList`; if `schemaname`
present and `LookupNamespaceNoError(schemaname)` invalid → schema-does-not-exist
msg with the schemaname. Matches.

**type_in_list_does_not_exist_skipping** — iterate typenames; NULL-cell guard
(`if let Some(type_name) = ..as_typename()` == C `if (typeName != NULL)`);
`LookupTypeNameOid(NULL, typeName, true)` invalid → schema probe on
`typeName->names`, else type-does-not-exist + `TypeNameToString`. Matches.

**does_not_exist_skipping** — full ObjectType `switch` re-derived case-by-case
against C 249-516. All `gettext_noop` format strings transcribed verbatim
(verified char-for-char). `strVal`/`castNode(TypeName)`/`castNode(List)`/
`castNode(ObjectWithArgs)` map to typed-Node accessors. Verified subtleties:
OBJECT_OPERATOR sets only msg+name (no args) — matches C 383-385; CAST/TRANSFORM
use `list_make1(linitial(...))`/`list_make1(lsecond(...))` for the probe but
`linitial_node(TypeName, <outer list>)`/`lsecond` for the final name/args (port
extracts source/target from the outer list before wrapping — correct); OPCLASS/
OPFAMILY use `list_copy_tail(list, 1)` for the name and `linitial` for the AM
args; TRIGGER/POLICY/RULE use `owningrel_does_not_exist_skipping` then
`llast`/`list_copy_head(len-1)`. The two `elog(ERROR, "unsupported object type")`
groups (handled-elsewhere + not-used) and the trailing `unrecognized object
type` and the `if(!msg)` guard all map to `elog_error` (non-translatable
internal error). Final emit: `args==NULL` → 1-arg errmsg, else 2-arg, via a
fixed-format `%s`/`%%` renderer (all dropcmds.c formats are `%s`-only; verified).
`name.unwrap_or_default()` is safe — every msg-setting branch also sets name.

## Seam audit

Owned seam crate: `backend-commands-dropcmds-seams` declares one inward seam
`remove_objects` (called by the utility dispatcher across the command boundary).
`init_seams()` (lib.rs:101) installs exactly it; `seams-init::init_all()` calls
`backend_commands_dropcmds::init_seams()` (seams-init/src/lib.rs:53). No
uninstalled owned seams; no `set()` outside the owner.

Outward seams (all thin marshal+delegate, each justified by a real dependency
cycle into a named unported owner):
- `get_object_address` / `get_object_namespace` / `check_object_ownership`
  (backend-catalog-objectaddress-seams)
- `object_ownercheck` (backend-catalog-aclchk-seams)
- `perform_multiple_deletions` (backend-catalog-dependency-seams)
- `is_temp_namespace` (backend-catalog-namespace-seams)
- `get_func_prokind` (backend-utils-cache-lsyscache-seams)
- `get_user_id` (backend-utils-init-miscinit-seams)
- `set_xact_accessed_temp_namespace` (backend-access-transam-xact-seams)
- `lookup_type_name_oid` / `type_name_list_to_string` / `typename_to_string_node`
  (backend-parser-parse-type-seams)

Direct calls into the ported `backend-catalog-namespace`:
`makeRangeVarFromNameList`, `LookupNamespaceNoError`, `NameListToString`,
`RangeVarGetRelid`. No branching/node-construction/computation lives in any seam
path; the Node→NameList/TypeName projection adapters are in-crate marshalling.

## Design conformance

- Allocating helpers (`remove_objects`, the probes, `does_not_exist_skipping`)
  take `Mcx` and return `PgResult` — matches the C failure surface (every
  ereport(ERROR)/elog(ERROR) carried on Err). No `&'static mut`, no invented
  opacity (real `types_parsenodes::Node`/`DropStmt`, real
  `types_catalog::ObjectAddress`). No shared statics for per-backend globals.
  No locks held across `?`. No registry-shaped side tables.

## Gates

- `cargo check --workspace` — clean (only pre-existing warnings in unrelated
  crates: backend-access-common-printtup).
- `cargo test -p backend-commands-dropcmds` — 4 passed.
- `cargo test -p seams-init` — 2 passed (both recurrence_guard tests:
  every-crate-wired + every-declared-seam-installed).
