# Audit: backend-utils-adt-jsonb-op

Unit: `backend-utils-adt-json-small` (partial — only `jsonb_op.c`; `jsonbsubs.c` still todo).
C source: `postgres-18.3/src/backend/utils/adt/jsonb_op.c`.
Crate: `crates/backend-utils-adt-jsonb-op`.

Audit re-derived independently from the C and the engine signatures in
`backend-utils-adt-jsonb-util`. The crate is a thin SQL-facing operator layer;
all real work delegates to the sibling jsonb engine via a normal path dep (no
cycle).

## Function inventory (every function in jsonb_op.c)

| # | C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `jsonb_exists` | jsonb_op.c:18 | lib.rs `jsonb_exists` | MATCH | Builds `jbvString` over key bytes; `findJsonbValueFromContainer(root, JB_FOBJECT\|JB_FARRAY, &kval)`; returns `v != NULL` → `v.is_some()`. Comment-block semantics (top-level only, no recursion) preserved. |
| 2 | `jsonb_exists_any` | jsonb_op.c:42 | lib.rs `jsonb_exists_any` | MATCH | `deconstruct_array_builtin(keys, TEXTOID)` → `deconstruct_text_array` seam (arrayfuncs-owned). Loop: skip nulls, build `jbvString`, return true on first found; else false. |
| 3 | `jsonb_exists_all` | jsonb_op.c:73 | lib.rs `jsonb_exists_all` | MATCH | Same deconstruct; return false on first not-found; else true. Null elements skipped (so all-null array → true, matching C). |
| 4 | `jsonb_contains` | jsonb_op.c:104 | lib.rs `jsonb_contains` | MATCH | arg0=val, arg1=tmpl. `JB_ROOT_IS_OBJECT(val) != JB_ROOT_IS_OBJECT(tmpl)` short-circuit false; else `JsonbIteratorInit` both + `JsonbDeepContains(&val, &tmpl)`. |
| 5 | `jsonb_contained` | jsonb_op.c:121 | lib.rs `jsonb_contained` | MATCH | Commutator: arg0=tmpl, arg1=val. Same object/array mismatch guard; `JsonbDeepContains(&val, &tmpl)` (it1=val, it2=tmpl). Order preserved. |
| 6 | `jsonb_ne` | jsonb_op.c:138 | lib.rs `jsonb_ne` | MATCH | `compareJsonbContainers(a,b) != 0`. |
| 7 | `jsonb_lt` | jsonb_op.c:153 | lib.rs `jsonb_lt` | MATCH | `< 0`. |
| 8 | `jsonb_gt` | jsonb_op.c:166 | lib.rs `jsonb_gt` | MATCH | `> 0`. |
| 9 | `jsonb_le` | jsonb_op.c:179 | lib.rs `jsonb_le` | MATCH | `<= 0`. |
| 10 | `jsonb_ge` | jsonb_op.c:192 | lib.rs `jsonb_ge` | MATCH | `>= 0`. |
| 11 | `jsonb_eq` | jsonb_op.c:205 | lib.rs `jsonb_eq` | MATCH | `== 0`. |
| 12 | `jsonb_cmp` | jsonb_op.c:218 | lib.rs `jsonb_cmp` | MATCH | Returns the raw i32 compare result. |
| 13 | `jsonb_hash` | jsonb_op.c:234 | lib.rs `jsonb_hash` | MATCH | `JB_ROOT_COUNT==0` → 0; iterate; `WJB_BEGIN_ARRAY`→`hash ^= JB_FARRAY`, `WJB_BEGIN_OBJECT`→`hash ^= JB_FOBJECT`, KEY/VALUE/ELEM→`JsonbHashScalarValue`, END_* nop, default→`elog(ERROR,"invalid JsonbIteratorNext rc: %d")`. u32 hash returned as i32. |
| 14 | `jsonb_hash_extended` | jsonb_op.c:276 | lib.rs `jsonb_hash_extended` | MATCH | `JB_ROOT_COUNT==0` → seed; xor masks `((u64)JB_FARRAY)<<32 \| JB_FARRAY` and FOBJECT variant; KEY/VALUE/ELEM→`JsonbHashScalarValueExtended(&v,&hash,seed)`; same default error. u64 hash returned. |

## Constants verified against headers

- `JB_FOBJECT = 0x20000000`, `JB_FARRAY = 0x40000000` — match `jsonb.h:202-203` and `types-jsonb`.
- `JB_ROOT_COUNT` = `JsonContainerSize(&root)` = `header & JB_CMASK`; `JB_ROOT_IS_OBJECT` = `header & JB_FOBJECT`. Implemented via `jb_root_count`/`jb_root_is_object` reading the leading 4-byte header word from the root container bytes — matches the `&jb->root` slice convention of the engine.
- Hash error message `"invalid JsonbIteratorNext rc: %d"` reproduced with `format!` + `ERRCODE_INTERNAL_ERROR` (default `elog(ERROR)` SQLSTATE). MATCH.

## Behavioral notes

- C's `PG_FREE_IF_COPY` calls (detoast cleanup of the input varlena copies) have
  no analog and no behavioral effect in the owned-`&[u8]` value model — the
  caller owns input lifetime. Not a divergence.
- C's `PG_GETARG_*`/`PG_RETURN_*` (fmgr ABI marshaling) are out of scope: bare-word
  `PGFunction` registry entry points are deferred per the workspace plan; these
  are the plain workers the dispatcher will call.

## Seam / wiring audit

- Owned seam crates: none. The unit's C files are `jsonb_op.c` (ported) and
  `jsonbsubs.c` (not ported); no `crates/backend-utils-adt-jsonb-op-seams` and no
  jsonbsubs seam crate exist, so this crate owns nothing to install and has no
  `init_seams()`. Correct (pure consumer leaf).
- Outward seam: `backend_utils_adt_array_more_seams::deconstruct_text_array`
  (`deconstruct_array_builtin(arr, TEXTOID)` from array.c) — a genuine cross-unit
  call into arrayfuncs, installed by `backend-utils-adt-arrayfuncs::init_seams()`.
  Thin marshal+delegate (one call, iterate result). Justified and correct.
- Engine calls (`findJsonbValueFromContainer`, `compareJsonbContainers`,
  `JsonbDeepContains`, `JsonbIteratorInit/Next`, `JsonbHashScalarValue[Extended]`)
  go through a direct path dependency on `backend-utils-adt-jsonb-util` — no cycle,
  so no seam needed (per the direct-dep-by-default rule).

## Design conformance

- No invented opacity, no shared statics, no ambient-global seams, no locks held
  across `?`, no registry side tables, no `todo!`/`unimplemented!`.
- No `Mcx`/`PgResult` allocation-seam concerns: the only allocation is owned
  `Vec<u8>` key copies for the transient `jbvString` (the engine's
  `JsonbValue` field is an owned `Vec<u8>` we cannot change); fallible engine
  calls already return `PgResult`.
- No `CONTRACT_RECONCILE_PENDING` introduced or outstanding for this crate.

## Verdict: PASS

All 14 functions MATCH; zero seam findings; design-conformant. Tests: 6 unit
tests (existence object/array, exists_any/all incl. null handling, contains/
contained incl. commutator + type-mismatch, all seven BT comparators, hash
equality + empty-container seed) pass.
