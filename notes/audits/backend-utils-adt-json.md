# Audit: backend-utils-adt-json

- **Verdict:** PASS
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **Branch:** `port/backend-utils-adt-json`
- **C source:** `postgres-18.3/src/backend/utils/adt/json.c`
- **Port crate:** `crates/backend-utils-adt-json/src/lib.rs`

Independent, function-by-function audit re-derived from the C source and the
c2rust rendering (`c2rust-runs/backend-utils-adt-json`). One FAIL finding on the
first pass (missing `check_stack_depth()` guard in `datum_to_json_internal`) was
fixed on the branch and re-audited; the table below reflects the fixed state.

## Function inventory & verdicts

Every function definition in `json.c` (statics, fmgr entries, inline helpers):

| C function | C loc | Port location | Verdict | Notes |
|---|---|---|---|---|
| `json_in` | 107 | `json_in` (120) | MATCH | parse_validate seam; soft error → `Ok(None)`, mirrors `pg_parse_json_or_errsave` → false. |
| `json_out` | 126 | `json_out` (131) | MATCH | content bytes verbatim (TextDatumGetCString). |
| `json_send` | 138 | `json_send` (138) | MATCH | body = content bytes; wire framing is caller's. |
| `json_recv` | 152 | `json_recv` (144) | MATCH | validate then return text; hard-error path guarded by `unreached_soft_error`. |
| `datum_to_json_internal` | 179 | `datum_to_json_internal` (163) | MATCH | All switch arms, key_scalar guard, numeric quote open-coding, default text fast-path. **Fixed:** `check_stack_depth()` seam call added at top (was missing). |
| `JsonEncodeDateTime` | 310 | `JsonEncodeDateTime` (286) | SEAMED | Entire body is the datetime subsystem (`Encode*`, field conv); delegated to `backend-utils-adt-timestamp-seams::json_encode_datetime` — thin marshal+delegate. |
| `array_dim_to_json` | 431 | `array_dim_to_json` (296) | MATCH | sep selection, `i<=dims[dim]` loop, innermost vs recursive dim, valcount advance. |
| `array_to_json_internal` | 474 | `array_to_json_internal` (356) | MATCH | nitems<=0 → `[]`; `deconstruct_array` (catalog half) seamed; structural assembly in-crate. NItems overflow guard owned by the seam (ArrayGetNItemsSafe); in-crate product only drives the `<=0` early return. |
| `composite_to_json` | 521 | `composite_to_json` (399) | MATCH | sep, needsep, dropped-attr skip (filtered by `walk_composite`), per-field render. Catalog half (`lookup_rowtype_tupdesc`/`heap_getattr`/`json_categorize_type`) seamed. |
| `add_json` | 602 | `add_json` (439) | MATCH | InvalidOid error; is_null → JSONTYPE_NULL/InvalidOid; else categorize_type seam. |
| `array_to_json` | 630 | `array_to_json` (503) | MATCH | |
| `array_to_json_pretty` | 646 | `array_to_json_pretty` (508) | MATCH | |
| `row_to_json` | 663 | `row_to_json` (517) | MATCH | |
| `row_to_json_pretty` | 679 | `row_to_json_pretty` (522) | MATCH | |
| `to_json_is_immutable` | 700 | `to_json_is_immutable` (461) | MATCH | category switch incl. TODO-recurse arrays/composites → false (faithful to C). func_volatile seam for numeric/cast/other. |
| `to_json` | 739 | `to_json` (533) | MATCH | InvalidOid error, categorize_type, datum_to_json. |
| `datum_to_json` | 763 | `datum_to_json` (483) | MATCH | |
| `json_agg_transfn_worker` | 779 | `json_agg_transfn_worker` (796) | MATCH | first-call init (`[`, categorize), absent_on_null skip, `len>1`→`, `, NULL fast path, structured-type whitespace (`!PG_ARGISNULL(0)` ↔ `!first_call`). |
| `json_agg_transfn` | 861 | `json_agg_transfn` (862) | MATCH | |
| `json_agg_strict_transfn` | 870 | `json_agg_strict_transfn` (873) | MATCH | |
| `json_agg_finalfn` | 879 | `json_agg_finalfn` (885) | MATCH | NULL state → None; else catenate `]`. |
| `json_unique_hash` | 900 | `json_unique_hash` (579) | MATCH | `hash_bytes_uint32(object_id) ^ hash_bytes(key,key_len)`; `tag_hash` seam == `hash_bytes`. |
| `json_unique_hash_match` | 911 | `json_unique_hash_match` (587) | MATCH | object_id, key_len, then strncmp ordering. |
| `json_unique_check_init` | 932 | `json_unique_check_init` (608) | MATCH | reset table. |
| `json_unique_builder_init` | 950 | `json_unique_builder_init` (613) | MATCH | check init + skipped_keys=None (memset 0). |
| `json_unique_check_key` | 958 | `json_unique_check_key` (621) | MATCH | HASH_ENTER + found semantics via hash+match equality probe; returns `!found`. |
| `json_unique_builder_get_throwawaybuf` | 978 | `json_unique_builder_get_throwawaybuf` (645) | MATCH | lazy init / reset (data==NULL ↔ None). |
| `json_object_agg_transfn_worker` | 1002 | `json_object_agg_transfn_worker` (898) | MATCH | first-call key/val categorize + `{ `; null-key error; skip/throwaway buf; key_offset copy-before-hash; comma `len>2`; ` : `; value render. |
| `json_object_agg_transfn` | 1150 | (998) | MATCH | |
| `json_object_agg_strict_transfn` | 1159 | (1014) | MATCH | |
| `json_object_agg_unique_transfn` | 1168 | (1030) | MATCH | |
| `json_object_agg_unique_strict_transfn` | 1177 | (1046) | MATCH | |
| `json_object_agg_finalfn` | 1186 | `json_object_agg_finalfn` (1063) | MATCH | NULL → None; else catenate ` }`. |
| `catenate_stringinfo_string` | 1209 | `catenate_stringinfo_string` (1076) | MATCH | buffer + addon. |
| `json_build_object_worker` | 1224 | `json_build_object_worker` (1094) | MATCH | even-args errhint, `{`, per-pair skip/throwaway, null-key error, sep advance only on non-skip, key uniqueness copy, ` : `, value, `}`. |
| `json_build_object` | 1318 | `json_build_object` (1187) | MATCH | nargs<0 → None == `extracted==None`. |
| `json_build_object_noargs` | 1338 | `json_build_object_noargs` (1200) | MATCH | `{}`. |
| `json_build_array_worker` | 1344 | `json_build_array_worker` (1206) | MATCH | absent_on_null skip, sep, add_json, `]`. |
| `json_build_array` | 1374 | `json_build_array` (1234) | MATCH | |
| `json_build_array_noargs` | 1394 | `json_build_array_noargs` (1247) | MATCH | `[]`. |
| `json_object` | 1406 | `json_object` (1260) | MATCH | ndims 0/1/2/default; even/two-column/subscript errors; null-key error; escape_json_text; null → `null`. |
| `json_object_two_arg` | 1490 | `json_object_two_arg` (1319) | MATCH | nkdims>1 / mismatch errors; nkdims==0 → `{}`; count mismatch; per-pair render. |
| `escape_json_char` | 1562 | `escape_json_char` (1377) | MATCH | `\b \f \n \r \t \" \\`, `<0x20` → `\u00xx` (lowercase hex), else passthrough. |
| `escape_json` | 1602 | `escape_json` (1403) | MATCH | quotes; loop stops at first NUL. |
| `escape_json_with_len` | 1631 | `escape_json_with_len` (1426) | MATCH | SIMD fast path reduced to scalar run-flush; identical output. enlargeStringInfo(len+2) → fallible MaxAllocSize check. Escape predicate `<=0x1F \|\| '"' \|\| '\\'`. |
| `escape_json_text` | 1736 | `escape_json_text` (1467) | MATCH | detoast is caller's; escapes via escape_json_with_len. |
| `json_unique_object_start` | 1754 | `json_unique_object_start` (700) | MATCH | unique guard, push id, increment counter. |
| `json_unique_object_end` | 1772 | `json_unique_object_end` (714) | MATCH | unique guard, pop. |
| `json_unique_object_field_start` | 1787 | `json_unique_object_field_start` (725) | MATCH | unique guard, top-of-stack object_id, on collision set unique=false + clear stack. |
| `json_validate` | 1812 | `json_validate` (1477) | MATCH | check_unique vs plain validate; throw_error errsave; not-unique → dup-key error or false. |
| `json_typeof` | 1874 | `json_typeof` (1504) | MATCH | lex first token; token→type string; default unexpected-token elog (XX000) via token_type_int. |

Helpers with no standalone C counterpart (idiomatic infra; not divergences):
`alloc_failure`, `buf_extend`, `buf_push`, `build` (StringInfo/append/MaxAllocSize
model), `DatumGetBool`, `token_type_int`, `unreached_soft_error`,
`escape_json_into_pgstring` / `escape_json_with_len_into_pgstring` (inward-seam
adapters), `JsonAggState::new`, `JsonUniqueParsingState::new`.

## Seam audit

**Owned seam crate** (maps to `json.c`): `backend-utils-adt-json-seams` — three
declarations (`escape_json`, `escape_json_with_len`, `json_encode_datetime`).
All three are installed by `backend_utils_adt_json::init_seams()`, which is
nothing but `set()` calls and is called from `seams-init::init_all()`
(seams-init/src/lib.rs:117). No uninstalled owned seam.

**Outward seam calls** — each justified by a real dependency cycle / unported
neighbor and is thin marshal+delegate (no branching/node-construction/computation
in the seam path):
- `common-jsonapi-seams` (`parse_validate`, `parse_validate_unique`,
  `errsave_error`, `lex_first_token`) — the JSON lexer/parser in
  `src/common/jsonapi.c`.
- `backend-utils-adt-jsonfuncs-seams` (`categorize_type`, `func_volatile`,
  `output_function_call`, `cast_function_call`, `text_datum_bytes`,
  `is_text_output_func`, `deconstruct_array`, `walk_composite`) — type
  classification + fmgr output/cast + array/composite catalog work, anchored by
  `json_categorize_type` in the cycle-partner `jsonfuncs.c`. The structural
  `[ … ]` / `{ … }` assembly stays in-crate; only the catalog/fmgr halves cross.
- `backend-utils-adt-timestamp-seams` (`json_encode_datetime`) — datetime
  field-conversion + `Encode*`.
- `common-hashfn-seams` (`hash_bytes_uint32`, `tag_hash`) — hash primitives.
- `backend-utils-misc-stack-depth-seams` (`check_stack_depth`) — the recursion
  guard (added in the fix; same pattern as partcache/tsearch-spell).

No function body was replaced by a seam to "somewhere else": all of json.c's own
logic (escaping, builders, unique-check, aggregate state machine, validate,
typeof) lives in-crate.

## Design conformance (§3b)

- Allocating functions/builders take `Mcx<'mcx>` and return `PgResult<…>`; OOM /
  over-`MaxAllocSize` surface as recoverable `PgError` (PROGRAM_LIMIT_EXCEEDED). OK.
- No shared statics for per-backend globals: unique-check / agg state is passed by
  `&mut` / owned value, not ambient. OK.
- No invented opacity: no fake handles; `JsonAggState`/`JsonUniqueCheckState`/
  `JsonUniqueParsingState` are real structs mirroring the C structs. OK.
- No ambient-global seams, no locks across `?`, no registry-shaped side tables. OK.
- Error SQLSTATEs verified against the C `errcode(...)`: INVALID_PARAMETER_VALUE,
  NULL_VALUE_NOT_ALLOWED, DUPLICATE_JSON_OBJECT_KEY_VALUE, ARRAY_SUBSCRIPT_ERROR,
  INTERNAL_ERROR (unexpected token), STATEMENT_TOO_COMPLEX (stack depth). OK.

## Findings (resolved)

1. **(FIXED) Missing `check_stack_depth()` in `datum_to_json_internal`** — the C
   calls `check_stack_depth()` (json.c:186) before recursing through
   array/composite rendering; the port omitted it, dropping the
   `ERRCODE_STATEMENT_TOO_COMPLEX` error path and the stack-overflow guard.
   Fixed by adding the `backend-utils-misc-stack-depth-seams::check_stack_depth`
   dependency and the seam call at the top of the function. Re-audited: MATCH.

## Conclusion

Every C function is MATCH or properly SEAMED; zero outstanding seam findings;
design-conformance clean. **PASS.**
