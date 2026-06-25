# Audit: backend-commands-conversioncmds

C source: `src/backend/commands/conversioncmds.c` (135 lines, one function).
Port: `crates/backend-commands-conversioncmds/src/lib.rs`.
Audited 2026-06-13 against the C, the src-idiomatic rendering, and the headers.

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `CreateConversionCommand` | conversioncmds.c:31-134 | lib.rs `CreateConversionCommand` | MATCH | see below |

`conversioncmds.c` defines exactly one function (no statics, no inline helpers).

## CreateConversionCommand — detailed comparison

Branch-by-branch parity:

1. encoding-name defaulting — C reads `stmt->for_encoding_name`/`to_encoding_name`
   as `char *`; the owned node carries `Option<String>`, `unwrap_or("")` mirrors
   the null→"" path (pg_char_to_encoding then reports the empty name as
   nonexistent). MATCH.
2. `QualifiedNameGetCreationNamespace(conversion_name, &conversion_name)` — direct
   call into backend-catalog-namespace; conversion_name list wrapped to
   `&[Option<String>]` (None=A_Star), returns `(namespaceId, &str)`. MATCH.
3. ACL_CREATE check: `object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(),
   ACL_CREATE)`; on `!= ACLCHECK_OK` → `aclcheck_error(aclresult, OBJECT_SCHEMA,
   get_namespace_name(namespaceId))`. SEAMED (aclchk/lsyscache/miscinit owners);
   `aclcheck_error` is `pg_noreturn` in C, modeled as always-`Err` via `?`. MATCH.
4. `from_encoding = pg_char_to_encoding(from)`; `< 0` →
   `ERRCODE_UNDEFINED_OBJECT` "source encoding \"%s\" does not exist". MATCH.
5. `to_encoding = pg_char_to_encoding(to)`; `< 0` → `ERRCODE_UNDEFINED_OBJECT`
   "destination encoding ...". MATCH.
6. `from == PG_SQL_ASCII || to == PG_SQL_ASCII` → `ERRCODE_INVALID_OBJECT_DEFINITION`
   "encoding conversion to or from \"SQL_ASCII\" is not supported". PG_SQL_ASCII=0
   verified vs pg_wchar.h:242. MATCH.
7. `funcoid = LookupFuncName(func_name, sizeof(funcargs)/sizeof(Oid)=6, funcargs,
   false)`. funcargs = {INT4OID,INT4OID,CSTRINGOID,INTERNALOID,INT4OID,BOOLOID}
   (16/23/2275/2281 canonical). nargs passed as `FUNCARGS.len() as i32` = 6.
   SEAMED (parse_func). MATCH.
8. `get_func_rettype(funcoid) != INT4OID` → `ERRCODE_INVALID_OBJECT_DEFINITION`
   "encoding conversion function %s must return type %s" with NameListToString +
   literal "integer". SEAMED (lsyscache) + direct NameListToString. MATCH.
9. ACL_EXECUTE check: `object_aclcheck(ProcedureRelationId, funcoid, GetUserId(),
   ACL_EXECUTE)`; `!= ACLCHECK_OK` → `aclcheck_error(aclresult, OBJECT_FUNCTION,
   NameListToString(func_name))`. SEAMED + pg_noreturn-as-`?`. MATCH.
10. empty-input self-test: C `OidFunctionCall6(funcoid, Int32GetDatum(from),
    Int32GetDatum(to), CStringGetDatum(""), CStringGetDatum(result),
    Int32GetDatum(0), BoolGetDatum(false))`. Modeled as the
    `conversion_proc_empty_input_test(funcoid, from, to)` fmgr seam returning
    `DatumGetInt32(funcresult)`. SEAMED (fmgr owns the two cstring-Datum framings
    + the dispatch — the documented project Datum-framing deferral). MATCH.
11. `DatumGetInt32(funcresult) != 0` → `ERRCODE_INVALID_OBJECT_DEFINITION`
    "encoding conversion function %s returned incorrect result for empty input".
    MATCH.
12. `return ConversionCreate(conversion_name, namespaceId, GetUserId(),
    from_encoding, to_encoding, funcoid, stmt->def)`. SEAMED (pg_conversion;
    records its own dependencies — port records none separately, as the C). MATCH.

Constants verified: ACL_CREATE=1<<9, ACL_EXECUTE=1<<7 (parsenodes.h:83/85);
PG_SQL_ASCII=0 (pg_wchar.h:242); T_CreateConversionStmt=249 (nodetags.h:266);
type OIDs canonical pg_type.dat values; NAMESPACE_RELATION_ID=2615,
PROCEDURE_RELATION_ID=1255 canonical.

## Seam / wiring audit

Outward seams, each justified (these owners are unported or would cycle through
the command/catalog frontier; all thin marshal+delegate):

- `pg_char_to_encoding` — new `common-encnames-seams` (encnames.c owner; pure
  table lookup, non-fallible `i32`, panics until owner lands).
- `object_aclcheck`, `aclcheck_error` — `backend-catalog-aclchk-seams`.
- `get_namespace_name`, `get_func_rettype` — `backend-utils-cache-lsyscache-seams`.
- `lookup_func_name` — `backend-parser-parse-func-seams`.
- `get_user_id` — `backend-utils-init-miscinit-seams`.
- `conversion_create` — new in `backend-catalog-pg-conversion-seams` (pg_conversion.c
  owner): single delegate, no logic on the call path.
- `conversion_proc_empty_input_test` — new in `backend-utils-fmgr-fmgr-seams`
  (fmgr owner): builds the cstring Datums + runs OidFunctionCall6; the
  result!=0 check and its error stay in this crate.

Direct deps (acyclic): `backend-catalog-namespace`
(QualifiedNameGetCreationNamespace, NameListToString).

Ownership: this unit's only C file is conversioncmds.c. No `crates/X-seams`
maps to it (it is a leaf utility command invoked directly by the dispatcher, not
across a cycle), so it owns no inward seam crate. `init_seams()` is empty and is
wired into `seams-init::init_all()` — recurrence_guard's both directions pass
(every-seam-installed and every-installer-wired), confirmed by `cargo test -p
seams-init`.

## Design conformance

- No invented opacity: CreateConversionStmt is the real node (field-for-field vs
  parsenodes.h:4089); encoding ids are `i32`/`pg_enc`, OIDs are `Oid`.
- Allocations on the error/message paths only (format! into the PgError carrier,
  PgString::from_str_in is fallible `PgResult`); the function takes `Mcx` and
  returns `PgResult<ObjectAddress>` exactly where the C ereports.
- No statics, no locks, no registries, no unledgered divergence markers.
- Zero `todo!`/`unimplemented!`/`unreachable!`/own-logic `panic!`.

## Verdict: PASS

One function, MATCH; all delegations are legitimate SEAMED owner calls; zero
seam/design findings; gate + recurrence_guard green.
