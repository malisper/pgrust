# Audit: backend-utils-adt-tsvector-core

Date: 2026-06-15
Verdict: PASS (no correctness defects)

Covers C files `utils/adt/tsvector.c`, `utils/adt/tsvector_op.c`,
`utils/adt/tsvector_parser.c`. Function-by-function comparison against the C
ground truth and the src-idiomatic reference port (different memory model, same
logic).

## tsvector.c (io.rs) — PASS

| C function | Status |
|---|---|
| compareWordEntryPos | PASS |
| uniquePos | PASS (MAXNUMPOS-1 / MAXENTRYPOS-1 break, higher-weight retention) |
| compareentry | PASS |
| uniqueentry | PASS (dedup, position merge, SHORTALIGN buflen accounting) |
| tsvectorin | PASS (MAXSTRLEN/MAXSTRPOS checks, CALCDATASIZE layout, position copy) |
| tsvectorout | PASS (escape of `'`/`\`, multibyte walk via pg_mblen_range, weight A/B/C) |
| tsvectorsend | PASS (uint32 count, NUL-terminated lexemes, uint16 npos+positions) |
| tsvectorrecv | PASS (size cap, lexeme/pos validation, needSort + final compareentry sort) |

`tsvectorin` has no soft-error context at the byte entry point, so a parser soft
error is thrown as a hard `Err` — the faithful behavior of C `ereturn` when
`escontext` is not an `ErrorSaveContext`.

## tsvector_parser.c (parser.rs) — PASS

Full `gettoken_tsvector` state machine (WAITWORD/WAITENDWORD/WAITNEXTCHAR/
WAITENDCMPLX/WAITPOSINFO/INPOSINFO/WAITPOSDELIM/WAITCHARCMPLX) ported 1:1 with
identical SQLSTATEs and message text. `init/reset/close` keep the real
`TSVectorParseStateData` in a `thread_local!` registry keyed by the
`TsVectorParseStateHandle(u64)` token the owner mints — the contract the landed
tsquery-core consumes. `io.rs` calls the richer in-crate `gettoken_tsvector_full`
(want_pos=true) directly; the seam wrapper passes want_pos=false for tsquery
callers. atoi/LIMITPOS clamping, RESIZEPRSBUF bookkeeping, eml from
pg_database_encoding_max_length, ISOPERATOR/isspace/isdigit all faithful.

## tsvector_op.c (op.rs) — PASS

All 48 functions present with equivalent logic. High-scrutiny (downstream-
consumed) functions verified in detail:

- `tsCompareString` — all four sign branches (empty-a, empty-b, prefix,
  non-prefix length tiebreak) match C.
- `tsquery_requires_match` — OP_NOT→false, OP_PHRASE folds into OP_AND,
  QI_VAL→true; check_stack_depth present.
- `TS_execute` / `TS_execute_ternary` / `TS_execute_recurse` — OP_NOT TS_MAYBE
  passthrough, OP_AND/OP_OR short-circuit, OP_PHRASE MAYBE→NO unless
  TS_EXEC_PHRASE_NO_POS, TS_EXEC_SKIP_NOT early TS_YES; check_stack_depth +
  CHECK_FOR_INTERRUPTS present in all recursive functions.
- `TS_phrase_execute` / `TS_phrase_output` — distance-window position
  intersection, allocated/negate handling.
- `checkcondition_str` / `checkclass_str` — weight masking, prefix bsearch,
  TS_MAYBE corners.
- `tsvector_concat` — entry merge, position renumbering by maxpos, dedup.

No todo!/unimplemented!/silent stubs. Constants/OIDs/SQLSTATEs correct
(TSVECTOROID=3614, REGCONFIGOID=3734, TEXTOID=25). The two items flagged for a
second glance (relocation of add_pos's `haspos=1` side-effect to the caller; the
ts_accum nbit loop) were verified behavior-equivalent, not bugs.

## Seams

Owner installs all 8 declared `backend-utils-adt-tsvector-core-seams`
(init/reset/gettoken/close_tsvector_parser, ts_compare_string,
tsquery_requires_match, ts_execute, ts_execute_ternary) from `init_seams()`,
wired into seams-init. Installed signatures match the declarations.

Genuinely-external boundaries (SRF emission, ts_stat_sql SPI cursor,
lookup_ts_config, the tsvector_update_trigger trigger/SPI pipeline) are seamed to
a new `backend-utils-adt-tsvector-ext-seams` (decls only) — their owners (SPI,
funcapi, trigger manager, tsearch dictionary pipeline) are unported, so these
panic loudly until those land. The seams-init guard exempts them (no `-ext`
owner crate exists).
