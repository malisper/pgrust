# Audit: backend-utils-adt-enum (src/backend/utils/adt/enum.c)

Auditor: Claude Opus 4.8 (1M). Date: 2026-06-15.
C reference: ../pgrust/postgres-18.3/src/backend/utils/adt/enum.c
Result: PASS. Every enum.c function is present and 1:1, or a real seam/direct
call into a ported owner. No todo!()/unimplemented!()/deferral-stub. residual_own_todos = 0.

## Model

Owned-value rewrite (no FFI). An enum value is a 4-byte pass-by-value type whose
Datum word is its OID. Cores return decoded values (Oid / String / PgVec<u8>);
errors are PgResult; enum_in's soft path uses types_error::ereturn (C ereturn).

The C reads pg_enum tuples three ways; each crosses a boundary:
- SearchSysCache1(ENUMOID) -> syscache-seams lookup_enum_by_oid (NEW)
- SearchSysCache2(ENUMTYPOIDNAME) -> syscache-seams lookup_enum_by_typoid_name (NEW)
- table_open/index_open(EnumTypIdSortOrderIndexId)+systable_beginscan_ordered ->
  pg-enum-seams scan_enum_typid_sorted (NEW; real ordered scan in the pg_enum owner)

Each projects to types-catalog::pg_enum::EnumTupleData (NEW): the Form_pg_enum
columns enum.c reads (oid/enumtypid/enumlabel) PLUS the header facts
check_safe_enum_use needs — xmin_committed (HeapTupleHeaderXminCommitted) and xmin
(HeapTupleHeaderGetXmin, frozen-aware; NEW helpers added to types-tuple).

## Function-by-function

- check_safe_enum_use (enum.c:60) — MATCH. Branch order exact: (1) xmin_committed
  fast path; (2) !TransactionIdIsInProgress(xmin) && TransactionIdDidCommit(xmin)
  -> safe; (3) !EnumUncommitted(en.oid) -> safe; (4) ereport(ERROR,
  ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE, "unsafe use of new value \"%s\" of enum type
  %s" + errhint). TransactionIdIsInProgress -> procarray-seams; TransactionIdDidCommit
  -> transam-seams (TransactionXmin threaded as a param per no-ambient-global rule);
  EnumUncommitted -> pg-enum-seams enum_uncommitted (NEW, infallible). xmin/xmin_committed
  read off the projected row. HeapTupleHeaderXminCommitted/GetXmin verified vs
  htup_details.h (HEAP_XMIN_COMMITTED=0x0100, HEAP_XMIN_FROZEN, FrozenTransactionId=2).
- enum_in (enum.c:107) — MATCH. strlen>=NAMEDATALEN(64) soft error; ENUMTYPOIDNAME
  lookup; miss -> soft error; check_safe_enum_use (hard even with escontext, per
  enum.c comment); returns en.oid. SQLSTATE ERRCODE_INVALID_TEXT_REPRESENTATION,
  message "invalid input value for enum %s: \"%s\"" with format_type_be.
- enum_out (enum.c:155) — MATCH. ENUMOID lookup; miss -> ERROR
  ERRCODE_INVALID_BINARY_REPRESENTATION "invalid internal value for enum: %u";
  returns pstrdup(NameStr(enumlabel)) as owned String.
- enum_recv (enum.c:179) — MATCH. pq_getmsgtext(buf, len-cursor) (direct pqformat
  call); cstring NUL-truncation; length check; ENUMTYPOIDNAME lookup; miss -> ERROR;
  check_safe_enum_use; returns oid. C pfree(name) subsumed by context drop.
- enum_send (enum.c:221) — MATCH. ENUMOID lookup; miss -> ERROR; pq_begintypsend +
  pq_sendtext(NameStr(enumlabel)) + pq_endtypsend (direct pqformat); returns bytea image.
- enum_cmp_internal (enum.c:264) — MATCH. equal-OID -> 0; both even -> raw value
  (-1/1); else ENUMOID lookup for arg1 -> enumtypid (miss -> ERROR), then
  compare_values_of_enum (typcache-seams, NEW). C's Assert(flinfo!=NULL) and the
  fn_extra typcache caching are debug-only / folded into the OID-keyed typcache
  seam (SQL-behavior-identical; documented).
- enum_lt/le/eq/ne/ge/gt (enum.c:306-351) — MATCH. eq/ne pure OID identity (no
  catalog); the rest delegate to enum_cmp_internal with the C comparator.
- enum_smaller/larger (enum.c:360/369) — MATCH (<0 ? a : b / >0 ? a : b).
- enum_cmp (enum.c:378) — MATCH.
- enum_endpoint (enum.c:394) — MATCH. scan_enum_typid_sorted; Forward -> first,
  Backward -> last; check_safe_enum_use on the chosen member; InvalidOid for empty.
- enum_first/last (enum.c:437/466) — MATCH. InvalidOid enumtypoid -> ERROR
  ERRCODE_FEATURE_NOT_SUPPORTED "could not determine actual enum type"; endpoint;
  !OidIsValid -> ERROR ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE "enum %s contains
  no values". (enumtypoid is C get_fn_expr_argtype, supplied by the fmgr boundary.)
- enum_range_bounds (enum.c:496) — MATCH. PG_ARGISNULL -> InvalidOid (Option->unwrap_or);
  type-OID check; enum_range_internal.
- enum_range_all (enum.c:527) — MATCH. type-OID check; enum_range_internal(_, Invalid, Invalid).
- enum_range_internal (enum.c:553) — MATCH. ordered scan; left_found=!OidIsValid(lower);
  collect from left_found onward (check_safe_enum_use each), stop after upper;
  construct_array(elems, enumtypoid, sizeof(Oid)=4, byval=true, TYPALIGN_INT='i') —
  DIRECT call into backend-utils-adt-arrayfuncs. C's palloc/repalloc working array
  is a PgVec<Datum> grown via try_reserve (OOM -> mcx.oom).

## Constants / SQLSTATEs verified

NAMEDATALEN=64; ENUMOID=23, ENUMTYPOIDNAME=24 (syscache cacheinfo); EnumRelationId=3501,
EnumTypIdSortOrderIndexId=3534 (pg_enum.h); TYPALIGN_INT='i'; sizeof(Oid)=4. SQLSTATEs:
ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE (55P04), INVALID_TEXT_REPRESENTATION,
INVALID_BINARY_REPRESENTATION, FEATURE_NOT_SUPPORTED, OBJECT_NOT_IN_PREREQUISITE_STATE.

## Seams / wiring

Owns NO inward seams (leaf consumer) -> empty/no init_seams, no seams-init line.
NEW seams installed by their owners: syscache lookup_enum_by_oid + lookup_enum_by_typoid_name;
pg_enum enum_uncommitted + scan_enum_typid_sorted; typcache compare_values_of_enum.
seams-init recurrence guard GREEN (every declared seam installed by its owner; owners
wired into init_all).

## Notes

- format_type_be is a diagnostic-only seam; UTF-8-lossy rendering of server-encoded
  label/type text in messages is the repo-wide adt convention (ASCII path byte-identical).
- Typed fmgr boundary in boundary.rs; the bare-word PGFunction (fmgr_builtins[]) registry
  is deferred per task scope.
