# Audit: backend-commands-operatorcmds

C source: `src/backend/commands/operatorcmds.c` (PostgreSQL 18.3, 734 lines).
Crate: `crates/backend-commands-operatorcmds`.

Function-by-function comparison against the C and the src-idiomatic port. All
six C functions present; no `todo!`/`unimplemented!`.

## DefineOperator (C 66-267) — PASS

- Name → (namespace, name) via `QualifiedNameGetCreationNamespace` (direct).
- Namespace ACL_CREATE check, `aclcheck_error(.., OBJECT_SCHEMA,
  get_namespace_name(..))` — exact order and objtype.
- DefElem loop: leftarg/rightarg (with SETOF → ERRCODE_INVALID_FUNCTION_DEFINITION),
  function/procedure (equivalent), commutator/negator/restrict/join,
  hashes/merges, obsolete sort1/sort2/ltcmp/gtcmp → canMerge, and the
  WARNING-not-ERROR fall-through (ERRCODE_SYNTAX_ERROR) — all branches and
  message text match.
- Required-function check, typenameTypeId for present type names.
- Both-args-missing vs right-arg-missing (postfix hint, errdetail "Postfix
  operators are not supported.") — both messages and SQLSTATE match.
- Per-arg ACL_USAGE via `aclcheck_error_type` (inlined: get_element_type +
  format_type_be + aclcheck_error(OBJECT_TYPE), exactly aclchk.c:2974-2979).
- nargs/typeId selection (1 vs 2 args), `LookupFuncName(.., false)`.
- Function ACL_EXECUTE → aclcheck_error(OBJECT_FUNCTION, NameListToString);
  rettype ACL_USAGE → aclcheck_error_type.
- restriction/join estimator lookups (only when specified).
- `OperatorCreate(...)` via the pg_operator.c seam, full arg bundle in C order.

## ValidateRestrictionEstimator (C 274-323) — PASS

- typeId template {INTERNAL, OID, INTERNAL, INT4}, 4-arg LookupFuncName(false).
- float8 return check (ERRCODE_INVALID_OBJECT_DEFINITION, exact message).
- FirstGenbkiObjectId (10000) gate: non-built-in → superuser() else ERROR
  (ERRCODE_INSUFFICIENT_PRIVILEGE); built-in → ACL_EXECUTE.

## ValidateJoinEstimator (C 330-393) — PASS

- typeId template {INTERNAL, OID, INTERNAL, INT2, INTERNAL}.
- 5-arg then 4-arg lookup (both missing_ok); ambiguity → ERRCODE_AMBIGUOUS_FUNCTION;
  else fall back to 5-arg with error. float8 check + same privilege checks.
- 4-arg lookup reads the first 4 template slots (C array shared; port slices
  `typeId[..4]`).

## ValidateOperatorReference (C 404-439) — PASS

- `OperatorLookup` seam → (oid, defined). Missing → "operator does not exist",
  shell → "operator is only a shell" (both ERRCODE_UNDEFINED_FUNCTION,
  op_signature_string rendering matches parse_oper.c). Ownership check →
  aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR, NameListToString).

## RemoveOperatorById (C 445-482) — PASS

- fetch_operator_form (SearchSysCache1 OPEROID) → None ⇒ internal
  "cache lookup failed for operator %u".
- do_operator_upd = OidIsValid(oprcom) || OidIsValid(oprnegate); the
  OperatorUpd + self-(comm|neg) re-fetch + CatalogTupleDelete under
  RowExclusiveLock are bundled into the `remove_operator_tuple` seam
  (pg_operator.c owns the catalog write; faithful self-link re-fetch lives with
  the tuple it mutates). Owned + installed by this crate.

## AlterOperator (C 494-734) — PASS

- LookupOperWithArgs(opername, false) via parse-oper seam; SearchSysCacheCopy1
  → fetch_operator_form (None ⇒ internal error).
- Options loop: restrict/join (arg==NULL ⇒ NIL/remove), commutator/negator,
  merges/hashes (with update flags); leftarg/rightarg/function/procedure ⇒
  "cannot be changed"; else "not recognized" (both ERRCODE_SYNTAX_ERROR).
- Must-be-owner check; estimator/commutator(reversed args)/negator lookups;
  self-negation rejection (ERRCODE_INVALID_FUNCTION_DEFINITION).
- "cannot be changed if it has already been set" guards for
  commutator/negator/merges/hashes — exact messages.
- OperatorValidateParams seam (matching OperatorCreate's extra checks).
- values/replaces packing modelled as an OperatorAttrUpdate list →
  alter_operator_apply (heap_modify_tuple + CatalogTupleUpdate +
  makeOperatorDependencies(tup,false,true)) returning ObjectAddress.
- Post-update OperatorUpd when commutator/negator valid; InvokeObjectPostAlterHook;
  table_close(NoLock).

## Seam ownership

- `RemoveOperatorById` (operatorcmds.c) — owned + installed by this crate,
  declared in pg-operator-seams only because dependency.c calls it cross-cycle.
- All pg_operator.c catalog routines (OperatorCreate, OperatorUpd,
  OperatorValidateParams, makeOperatorDependencies, OperatorLookup, the operator
  tuple I/O) are NEW seams in `backend-catalog-pg-operator-seams`, panicking
  until pg_operator.c lands (mirror-PG-and-panic). pg_operator.c is the
  per-catalog carrier owner and is unported — this matches the typecmds/pg_type
  and aggregatecmds/pg_aggregate precedent.

## Divergences

None of contract. The pg_operator catalog seams carry value bundles
(`OperatorCreateArgs`, `FormPgOperator`, `OperatorAttrUpdate`) instead of the C
`Datum values[]`/`Form_pg_operator`/raw `HeapTuple` — the standard owned-tree
reconciliation, deferring the heap-level packing to the catalog owner.
