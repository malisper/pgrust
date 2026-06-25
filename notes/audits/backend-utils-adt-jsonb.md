# Audit: backend-utils-adt-jsonb

- **Verdict:** PASS
- **Date:** 2026-06-15
- **Model:** Claude Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **Branch:** `wf-jsonb`
- **C source:** `postgres-18.3/src/backend/utils/adt/jsonb.c` (2254 lines)
- **Port crate:** `crates/backend-utils-adt-jsonb/src/lib.rs`
- **Seams crate:** `crates/backend-utils-adt-jsonb-seams/src/lib.rs`

Independent, function-by-function audit re-derived from the C source and the
PostgreSQL headers (constants verified against headers, not the port's
comments). All 57 `jsonb.c` functions plus the 2 borrowed `json.c` escape
helpers were enumerated and compared.

## Constants / SQLSTATE verification (against headers)

| Constant | C value (header) | Port | Verdict |
|---|---|---|---|
| `JENTRY_OFFLENMASK` | `0x0FFFFFFF` (jsonb.h) | `0x0FFF_FFFF` | MATCH |
| `PROVOLATILE_IMMUTABLE` | `'i'` (pg_proc.h) | `b'i'` | MATCH |
| `DATEOID/TIMEOID/TIMETZOID/TIMESTAMPOID/TIMESTAMPTZOID` | 1082/1083/1266/1114/1184 | same | MATCH |
| `JsonTypeCategory` order | NULL,BOOL,NUMERIC,DATE,TIMESTAMP,TIMESTAMPTZ,JSON,JSONB,ARRAY,COMPOSITE,CAST,OTHER | same | MATCH |

The C recursive-work gate `tcategory >= JSONTYPE_JSON && tcategory <= JSONTYPE_CAST`
is the contiguous set `{JSON, JSONB, ARRAY, COMPOSITE, CAST}` (exactly 5); the
port expresses it as a `matches!` over those 5 arms — MATCH. The same 5-arm set
is reused for the `key_scalar` rejection gate, matching C's explicit `==` list.

## Function inventory & verdicts (57 + 2 helpers)

I/O: `jsonb_in`/`jsonb_recv`/`jsonb_out`/`jsonb_send`/`jsonb_from_text` — MATCH;
`jsonb_from_cstring` — SEAMED (parse loop → `parse_to_jsonb`, jsonapi lexer).

Semantic actions: `jsonb_in_object_start`/`_object_end`/`_array_start`/`_array_end`/`_object_field_start`/`_scalar` — MATCH (NUMBER uses hard `numeric_in`; the `escontext` soft-error path is not modeled, a documented model simplification shared with `jsonb_from_cstring`).

Type names: `JsonbContainerTypeName`/`JsonbTypeName`/`jsonb_typeof` — MATCH.

Rendering: `JsonbToCString`/`JsonbToCStringIndent`/`JsonbToCStringWorker`
(redo_switch/typ-persist/last_was_key/raw_scalar/ispaces all faithful)/`add_indent`/`jsonb_put_escaped_value` — MATCH; in-crate `escape_json_char`/`escape_json_with_len` ported 1:1 from json.c — MATCH.

Scalar/unquote: `JsonbExtractScalar`/`JsonbUnquote` — MATCH.

Casts: `cannotCastJsonbValue`/`jsonb_bool`/`jsonb_numeric`/`jsonb_float4`/`jsonb_float8` — MATCH (float via landed `numeric::convert::numeric_to_float4/8`); `jsonb_int2`/`jsonb_int4`/`jsonb_int8` — SEAMED (`numeric_intN`, unported in numeric crate).

Datum→jsonb: `datum_to_jsonb_internal` — MATCH (check_stack_depth mirrored; CAST pre-conversion; `'N'/'n'` numeric detection; all switch arms; final-insert gate); `array_dim_to_jsonb` — MATCH; `array_to_jsonb_internal`/`composite_to_jsonb` — SEAMED (catalog half via `deconstruct_array`/`walk_composite`; structural assembly in-crate); `add_jsonb`/`to_jsonb`/`to_jsonb_is_immutable`/`datum_to_jsonb` — MATCH.

Builders: `jsonb_build_object[_worker/_noargs]`/`jsonb_build_object`/`jsonb_build_array[_worker/_noargs]`/`jsonb_build_array`/`jsonb_object`/`jsonb_object_two_arg` — MATCH (all dimension checks + SQLSTATEs verified).

Aggregates: `clone_parse_state` — MATCH (structural deep clone, output-equivalent for append-only finalfn); `jsonb_agg_transfn[_worker]`/`_strict_transfn`/`_finalfn` and `jsonb_object_agg_transfn_worker`/`_transfn`/`_strict`/`_unique`/`_unique_strict`/`_finalfn` — MATCH (both splice loops with `single_scalar` verified, key-string gate, skip-null WJB_VALUE-null branch).

## Seam audit

All seams are justified: `parse_to_jsonb` (jsonapi lexer owns the parse loop),
`oid_function_call1` (fmgr), `jsonb_datum_bytes` (detoast), `numeric_int2/4/8`
(unported numeric callee) — these are in the owned `backend-utils-adt-jsonb-seams`
crate and are all OUTWARD calls made by the owner itself (seam-and-panic for
genuinely-unported callees, per `mirror-pg-and-panic`); the owner's `init_seams()`
is correctly empty (it owns no INWARD contract). The catalog half
(`categorize_type`/`output_function_call`/`func_volatile`/`text_datum_bytes`/
`deconstruct_array`/`walk_composite`) is consumed from the landed
`jsonfuncs-seams` (jsonb's real cycle partner); `json_encode_datetime` from
`timestamp-seams`; `check_stack_depth` from `misc-stack-depth-seams`. `numeric_in`/
`numeric_out`/`numeric_to_float4/8` call the landed numeric crate directly (no
seam — owner present, no cycle). No seam hides in-crate logic.

## MISSING in-crate logic check
None. Every jsonb-specific algorithm lives in-crate; delegations are only to the
jsonapi lexer (parse loop), the jsonfuncs cycle partner, and fmgr/detoast/numeric
primitives.

## Verdict: PASS
All functions MATCH or justified-SEAMED; zero MISSING/PARTIAL/DIVERGES; all
constants and SQLSTATEs verified against headers. 20 unit tests pass.
