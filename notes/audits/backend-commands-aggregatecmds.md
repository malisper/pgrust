# Audit: backend-commands-aggregatecmds

Independent function-by-function audit of `port/backend-commands-aggregatecmds`
(@3ad6fffa) against `src/backend/commands/aggregatecmds.c` (PostgreSQL 18.3) and
the c2rust rendering at `c2rust-runs/backend-commands-aggregatecmds/`.

C source has exactly two function definitions; both are enumerated below.

## Function inventory

| C fn (location) | port location | verdict | notes |
|---|---|---|---|
| `DefineAggregate` (aggregatecmds.c:52) | `crates/backend-commands-aggregatecmds/src/lib.rs:121` | MATCH | see detail |
| `extractModify` (static, aggregatecmds.c:477) | `lib.rs:587` | MATCH | see detail |

Helper functions added by the port (idiomatic renderings of C macros, not
separate C functions): `intVal` (intVal macro), `nodeAsList` (linitial_node),
`lfirstAsDefElem` (lfirst_node), `pg_strcasecmp` (port port/pg_strcasecmp.c
inline), `IsPolymorphicType` (pg_type.h macro), `to_resolver_typename`
(TypeName marshalling). All verified.

## DefineAggregate — detail

- Local var inits all match the C (aggKind=AGGKIND_NORMAL, all name lists NIL/
  empty, finalfuncModify/mfinalfuncModify=0, mtransTypeId=InvalidOid,
  mtransTypeType=0, proparallel=PROPARALLEL_UNSAFE, numDirectArgs=0). MATCH.
- `QualifiedNameGetCreationNamespace(name, &aggName)` -> seam into namespace
  (real direct dep). MATCH.
- `object_aclcheck(NamespaceRelationId, aggNamespace, GetUserId(), ACL_CREATE)`
  + `aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(...))` —
  same predicate (`!= ACLCHECK_OK`), same class/objtype, args via aclchk +
  miscinit + lsyscache seams. MATCH.
- New-style deconstruction: `numDirectArgs = intVal(lsecond(args))`;
  `>=0 => AGGKIND_ORDERED_SET else 0`; `args = linitial_node(List,args)`. The
  C `Assert(list_length(args)==2)` is rendered as `debug_assert_eq!`. MATCH.
- `foreach(pl, parameters)` clause dispatch: all 26 `strcmp` arms present in the
  same order with identical clause names (sfunc, sfunc1, finalfunc, combinefunc,
  serialfunc, deserialfunc, msfunc, minvfunc, mfinalfunc, finalfunc_extra,
  mfinalfunc_extra, finalfunc_modify, mfinalfunc_modify, sortop, basetype,
  hypothetical, stype, stype1, sspace, mstype, msspace, initcond, initcond1,
  minitcond, parallel) + the else WARNING. The `hypothetical` branch fires
  ERROR "only ordered-set aggregates can be hypothetical" only when
  `aggKind==AGGKIND_NORMAL` then sets AGGKIND_HYPOTHETICAL. MATCH.
  - WARNING for unrecognized attribute: `ereport(WARNING)` is non-throwing in C
    (logs and continues). Port uses `ereport(WARNING)...finish(errloc(190,...))?`
    which returns Ok and continues the loop — correct severity/behaviour.
    Errcode ERRCODE_SYNTAX_ERROR (42601, verified vs c2rust:1537-1546). MATCH.
- Required-defs checks: transType==NULL and transfuncName==NIL ERRORs, both
  ERRCODE_INVALID_FUNCTION_DEFINITION (42P13), same messages. MATCH.
- mtransType present/absent block: 2 ERRORs when present (msfunc/minvfunc
  required), 5 ERRORs when absent (msfunc/minvfunc/mfinalfunc/msspace/minitcond
  must-not). Same predicates (`is_empty()`/`!=0`/`is_some()`), order, messages,
  errcodes. MATCH.
- Modify-flag defaults: `==0 => (aggKind==NORMAL ? READ_ONLY : READ_WRITE)` for
  both final/mfinal. MATCH.
- oldstyle branch: baseType==NULL ERROR; `pg_strcasecmp(TypeNameToString(bt),
  "ANY")==0 => numArgs=0,InvalidOid else numArgs=1, typenameTypeId(NULL,bt)`;
  buildoidvector rendered as the natural `Vec<Oid>` (empty when numArgs==0, else
  single-element). all*/modes/names=None, defaults empty, variadic=InvalidOid.
  MATCH.
- new-style branch: baseType!=NULL ERROR "basetype is redundant ...";
  `numArgs=list_length(args)`; `interpret_function_parameter_list(...)` seam into
  functioncmds returns the collected bundle; the two C Asserts
  (parameterDefaults==NIL, requiredResultType==InvalidOid) -> debug_assert.
  MATCH.
- transtype lookup: `typenameTypeId`, `get_typtype`; pseudo-and-not-polymorphic
  guard with the INTERNALOID+superuser() escape, else ERROR "aggregate
  transition data type cannot be %s" via format_type_be. IsPolymorphicType
  reproduces all 11 OIDs (verified vs pg_type.h families 1+2 and the OID values
  in types_tuple). MATCH.
- serial/deserial: both-set requires transTypeId==INTERNALOID else ERROR; XOR =>
  ERROR "must specify both or neither". MATCH.
- moving transtype lookup mirrors transtype (only when mtransType set). MATCH.
- initval/minitval validation: when non-null and transtype not pseudo,
  getTypeInputInfo + OidInputFunctionCall(..., -1) result discarded. MATCH.
- parallel: safe/restricted/unsafe -> PROPARALLEL_*, else ERRCODE_SYNTAX_ERROR.
  MATCH.
- final `AggregateCreate(...)` 32-arg call: rendered as the `AggregateCreateArgs`
  bundle handed across `aggregate_create::call` (seam into pg_aggregate.c, an
  unported owner). All 32 fields mapped 1:1 in C parameter order;
  PointerGetDatum(array/oidvector) C marshalling replaced by the natural owned
  forms. MATCH/SEAMED-callee.

## extractModify — detail

`defGetString` -> "read_only"/"shareable"/"read_write" => AGGMODIFY_* (r/s/w,
verified vs pg_aggregate.h); else ERROR "parameter \"%s\" must be READ_ONLY,
SHAREABLE, or READ_WRITE" with ERRCODE_SYNTAX_ERROR (42601). The C dead
`return 0` after ereport(ERROR) is correctly dropped. MATCH.

## Constants verified vs C headers

- AGGKIND_NORMAL='n', _ORDERED_SET='o', _HYPOTHETICAL='h' (pg_aggregate.h). OK.
- AGGMODIFY_READ_ONLY='r', _SHAREABLE='s', _READ_WRITE='w' (pg_aggregate.h). OK.
- PROPARALLEL_SAFE='s', _RESTRICTED='r', _UNSAFE='u' (pg_proc.h). OK.
- TYPTYPE_PSEUDO='p' (pg_type.h). OK.
- INTERNALOID=2281. OK.
- 11 polymorphic OIDs (2283/2277/2776/3500/3831/4537/5077/5078/5079/5080/4538)
  match pg_type well-known OIDs and IsPolymorphicType families 1+2. OK.
- errcodes: 42P13 (INVALID_FUNCTION_DEFINITION) for the definitional ERRORs,
  42601 (SYNTAX_ERROR) for the unrecognized-attribute WARNING, the parallel
  ERROR, and extractModify — all verified against the c2rust SQLSTATE digits.

## Seam audit

Owned C source: `aggregatecmds.c` only. There is no inward-seam crate
`backend-commands-aggregatecmds-seams` (no dependency cycle has this crate as a
seamed callee), so this crate's `init_seams()` legitimately does not exist —
matches the functioncmds/opclasscmds precedent. recurrence_guard's
`every_seam_installing_crate_is_wired_into_init_all` and
`every_declared_seam_is_installed_by_its_owner` both pass.

The crate owns one OUTWARD seam crate, `backend-catalog-pg-aggregate-seams`
(maps to the unported `catalog/pg_aggregate.c`). It declares
`aggregate_create(AggregateCreateArgs) -> PgResult<ObjectAddress>`, a thin
mirror-PG-and-panic seam installed by pg_aggregate.c's owner when it lands.
Field order mirrors the 32-arg C parameter list; C pointer/Datum marshalling
resolves to natural owned forms (no invented opacity). Correct.

All other cross-crate calls (namespace, define, functioncmds, aclchk, parse_type,
format_type, lsyscache, fmgr, miscinit) are thin marshal+delegate into real
direct-dependency owners. No logic lives in any seam path. No own-logic stubs,
no todo!()/unimplemented!(), no deferred/unsupported escapes.

## Design conformance

- Mcx + PgResult threaded wherever the C ereports / allocates (DefineAggregate,
  extractModify, the namespace/format/seam calls). OK.
- No invented opacity: AggregateCreateArgs carries real owned values, TypeName
  marshalling converts between the two real node representations. OK.
- No shared statics, no ambient-global seams, no locks across `?`. OK.

## Gate results

- `cargo check --workspace`: clean (only pre-existing unrelated warnings in
  backend-access-common-printtup).
- `cargo test -p backend-commands-aggregatecmds`: 3 passed.
- `cargo test -p seams-init`: 2 passed (both recurrence_guard checks).

## Verdict: PASS
