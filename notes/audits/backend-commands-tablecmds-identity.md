# Audit: backend-commands-tablecmds — ALTER COLUMN ADD/SET/DROP IDENTITY

Scope: the new `crates/backend-commands-tablecmds/src/at_identity.rs` and the
identity prep + exec dispatch in `at_phase.rs`, audited independently against
`postgres-18.3/src/backend/commands/tablecmds.c`.

## Function inventory (identity slice of tablecmds.c)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ATExecAddIdentity` | tablecmds.c:8240 | at_identity.rs:74 `ATExecAddIdentity` | MATCH | see below |
| `ATExecSetIdentity` | tablecmds.c:8371 | at_identity.rs:235 `ATExecSetIdentity` | MATCH | see below |
| `ATExecDropIdentity` | tablecmds.c:8488 | at_identity.rs:393 `ATExecDropIdentity` | MATCH | see below |
| prep `case AT_AddIdentity` | tablecmds.c:4984 | at_phase.rs:776 | MATCH | recurse→cmd.recurse, pass=ADD_OTHERCONSTR |
| prep `case AT_SetIdentity` | tablecmds.c:4993 | at_phase.rs:789 | MATCH | pass=MISC (runs after AddIdentity) |
| prep `case AT_DropIdentity` | tablecmds.c:5003 | at_phase.rs:805 | MATCH | pass=DROP |
| exec `case AT_AddIdentity` | tablecmds.c:5397 | at_phase.rs:1429 | MATCH | ATParseTransformCmd(false) then exec |
| exec `case AT_SetIdentity` | tablecmds.c:5403 | at_phase.rs:1469 | MATCH | ATParseTransformCmd(false) then exec |
| exec `case AT_DropIdentity` | tablecmds.c:5409 | at_phase.rs:1510 | MATCH | no transform; missing_ok passed |

`ATParseTransformCmd`, `transformAlterTableStmt` (AT_AddIdentity/AT_SetIdentity),
`generateSerialExtraStmts`, `getIdentitySequence` are pre-existing and were
verified present/complete during scoping — not re-audited here beyond confirming
the call shapes.

## Per-function detail

### ATExecAddIdentity (MATCH)
Branch-for-branch:
- `ispartitioned && !recurse` → INVALID_TABLE_DEFINITION "...only the
  partitioned table" + hint "Do not specify the ONLY keyword." ✓
- `relispartition && !recursing` → INVALID_TABLE_DEFINITION "...a partition" ✓
- `SearchSysCacheCopyAttName`; not found → UNDEFINED_COLUMN ✓
- `attnum <= 0` → FEATURE_NOT_SUPPORTED "cannot alter system column" ✓
- `!attnotnull` → OBJECT_NOT_IN_PREREQUISITE_STATE "must be declared NOT NULL
  before identity can be added" ✓
- not-null-compatible check: `findNotNullConstraintAttnum`; NULL →
  elog(ERROR) cache-lookup (errmsg_internal); `!convalidated` →
  OBJECT_NOT_IN_PREREQUISITE_STATE "incompatible NOT VALID constraint" + hint ✓
  (C reads conForm->conname/convalidated via read_constraint_form seam).
- `attidentity` set → "already an identity column" ✓
- `atthasdef` → "already has a default value" ✓
- write: `attidentity = cdef->identity` via PgAttributeUpdateRow{attidentity}
  + catalog_tuple_update_pg_attribute ✓; InvokeObjectPostAlterHook ✓;
  ObjectAddressSubSet(rel, attnum) ✓.
- partition recursion: `recurse && ispartitioned` →
  find_inheritance_children(lockmode), open each NoLock, recurse recursing=true,
  close NoLock ✓.
The C reads attTup fields off the GETSTRUCT of the *copied* syscache tuple; the
port projects the same columns (attnum/attnotnull/attidentity/atthasdef) via
SysCacheGetAttrNotNull(ATTNAME,...). The write targets the same tuple. Equivalent.

### ATExecSetIdentity (MATCH)
- partitioned/partition guards identical to Add (distinct messages "cannot
  change identity column of ...") ✓
- option loop: only "generated" accepted; duplicate → SYNTAX_ERROR "conflicting
  or redundant options"; other → elog "option \"%s\" not recognized"
  (errmsg_internal) ✓. `defGetInt32(generatedEl)` ported inline: require arg,
  require Integer, take ival (matches define.c:148; the grammar stores the
  ATTRIBUTE_IDENTITY_* char as an Integer). ✓
- "Even if nothing to change, run all checks" — port opens attrelation and
  runs the lookups unconditionally before the generatedEl branch ✓.
- not found → UNDEFINED_COLUMN; attnum<=0 → FEATURE_NOT_SUPPORTED;
  `!attidentity` → "is not an identity column" ✓.
- `if generatedEl`: write attidentity=defGetInt32; hook; ObjectAddressSubSet;
  else address = InvalidObjectAddress ✓.
- recursion gated on `generatedEl && recurse && ispartitioned` ✓.

### ATExecDropIdentity (MATCH)
- partitioned/partition guards (messages "cannot drop identity from ...") ✓
- lookups; attnum<=0 guard ✓
- `!attidentity`: if `!missing_ok` → ERROR "is not an identity column"; else
  NOTICE "...is not an identity column, skipping", free tuple/close, return
  InvalidObjectAddress ✓.
- write attidentity='\0' (0); hook; ObjectAddressSubSet ✓.
- partition recursion (missing_ok=false on children, recursing=true) ✓.
- `if !recursing`: getIdentitySequence(rel, attnum, false) →
  deleteDependencyRecordsForClass(Rel, seqid, Rel, DEPENDENCY_INTERNAL='i') →
  CommandCounterIncrement → performDeletion(seqid, DROP_RESTRICT,
  PERFORM_DELETION_INTERNAL=0x0001) ✓. seqaddress built as {Rel, seqid, 0}; the
  perform_deletion seam takes (classId, objectId, objectSubId, behavior, flags)
  — same triple. ✓

## Constants verified against C headers
- `PERFORM_DELETION_INTERNAL = 0x0001` (dependency.h:92) ✓
- `DEPENDENCY_INTERNAL = 'i'` (dependency.h:35) ✓
- `DROP_RESTRICT` is the behavior C passes literally ✓
- `ObjectAddressSubSet(addr, classId, objId, subId)` → {classId, objId, subId} ✓
- `RelationRelationId` used for both classId and refclassId as in C ✓
- SQLSTATEs: INVALID_TABLE_DEFINITION, OBJECT_NOT_IN_PREREQUISITE_STATE,
  FEATURE_NOT_SUPPORTED, UNDEFINED_COLUMN, SYNTAX_ERROR — match the C ereports.

## Prep / exec dispatch
Prep passes match tablecmds.c:4984-5011 exactly (ADD_OTHERCONSTR / MISC / DROP,
`if recurse { cmd.recurse = true }`). Exec dispatch matches tablecmds.c:5397-5410:
ADD and SET run `ATParseTransformCmd(..., recurse=false, ...)` then assert
non-None and call the exec with the transformed cmd's name/def/recurse; DROP
calls exec directly with `cmd.missing_ok`. The `&mut wqueue` borrow needed by
ATParseTransformCmd is obtained by re-opening `rel` by relid (the AddColumn
pattern already in this file), faithful to C where `rel` is the caller's open
relation and wqueue/tab are separately mutable.

## Seam / design conformance
- All called seams are installed: `invoke_object_post_alter_hook`
  (objectaccess lib.rs:492), `getIdentitySequence` / `deleteDependencyRecordsForClass`
  (pg-depend lib.rs:1232/1243), `perform_deletion` (dependency seams.rs:129),
  `catalog_tuple_update_pg_attribute` (indexing), `read_constraint_form`
  (syscache). No new seam declarations introduced.
- Allocating calls take `Mcx` and return `PgResult`; no shared statics; no
  ambient-global seams; no locks held across `?` (attrelation is a relation_open
  RAII carrier dropped before recursion, matching C's table_close ordering —
  C closes attrelation before the partition recursion in all three functions).
- No `todo!`/`unimplemented!`/`unported` in the identity path; the three
  `unported(...)` stubs were removed.

## Verdict: PASS
Every identity function and dispatch arm is MATCH; constants verified against
headers; all seams installed; no design-rule violations.
