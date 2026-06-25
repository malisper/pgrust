# Audit: backend-utils-adt-jsonpath

Scope: `postgres-18.3/src/backend/utils/adt/jsonpath.c` (1529 LOC) — the
`jsonpath` SQL type's input/output, the on-disk flatten/unflatten of the parsed
expression, the textual printer, and the `JsonPathItem` reader API, plus
`jspIsMutable` (planner support) and `jspConvertRegexFlags` (defined in
`jsonpath_gram.y`).

NOT in scope (separate CATALOG units, still `todo`): `jsonpath_exec.c`
(`backend-utils-adt-jsonpath-exec`), `jsonpath_gram.y`
(`backend-utils-adt-jsonpath-gram`), `jsonpath_scan.l`
(`backend-utils-adt-jsonpath-scan`). See "Decomposition" below.

## Function-by-function vs C

| C function (jsonpath.c) | Port | Status |
|---|---|---|
| `jsonpath_in` (fmgr entry) | `jsonpath_in` core | ported (fmgr/Datum envelope deferred project-wide; core takes bytes + escontext) |
| `jsonpath_out` (fmgr entry) | `jsonpath_out` core | ported |
| `jsonpath_recv` (fmgr entry) | `jsonpath_recv` core | ported; libpq binary framing (pq_getmsgint/pq_getmsgtext) deferred, version check + dispatch implemented |
| `jsonpath_send` (fmgr entry) | `jsonpath_send` core | ported; libpq framing deferred, returns (version byte, rendered text) |
| `jsonPathFromCstring` | `jsonPathFromCstring` | ported; `parsejsonpath` → `parse` seam |
| `jsonPathToCstring` | `jsonPathToCstring` | ported |
| `flattenJsonPathParseItem` | `flattenJsonPathParseItem` | ported, all 1:1 cases incl. soft-error branches (`@`/`LAST`), Args/Arg/LikeRegex/IndexArray/Any/Numeric/Bool/String |
| `alignStringInfoInt` | `alignStringInfoInt` | ported |
| `reserveSpaceForItemPointer` | `reserveSpaceForItemPointer` | ported |
| `printJsonPathItem` | `printJsonPathItem` | ported, all node-type arms (incl. the `flag "ismxq"` like_regex flag spelling, `**{first to last}` any-bounds variants) |
| `jspOperationName` | `jspOperationName` | ported, full table + `elog(ERROR)` default |
| `operationPriority` | `operationPriority` | ported |
| `jspInit` | `jspInit` | ported |
| `jspInitByBuffer` | `jspInitByBuffer` | ported, all per-type decode arms; INTALIGN-relative-to-region offset math verified |
| `jspGetArg` | `jspGetArg` | ported (+ debug_assert of valid types) |
| `jspGetNext` | `jspGetNext` | ported |
| `jspGetLeftArg` / `jspGetRightArg` | same | ported |
| `jspGetBool` | `jspGetBool` | ported |
| `jspGetNumeric` | `jspGetNumeric` | ported; bounds the returned slice to the numeric's own VARSIZE (slice model) |
| `jspGetString` | `jspGetString` | ported |
| `jspGetArraySubscript` | `jspGetArraySubscript` | ported; returns `(from, Option<to>)` mirroring C's bool + out-params |
| `jspIsMutable` | `jspIsMutable` | ported; `varexprs: &[ExternalFnExpr]` (the node-tag carrier the installed `expr_type` seam consumes) |
| `jspIsMutableWalker` | `jspIsMutableWalker` | ported, all arms; note: C sets `jpdsDateTimeNonZoned` for BOTH `jpiTimeTz`/`jpiTimestampTz` (a C quirk) — mirrored verbatim |
| `jspConvertRegexFlags` (gram.y) | `jspConvertRegexFlags` | ported; XQuery `x`-flag → `ERRCODE_FEATURE_NOT_SUPPORTED` Err |

All 1529-LOC jsonpath.c functions are accounted for. No `todo!`/`unimplemented!`.

## Constants / parity

- `JsonPathItemType` discriminants, `JSONPATH_VERSION`=1, `JSONPATH_LAX`=0x80000000,
  `JSONPATH_HDRSZ`=8, `JSP_REGEX_*` bits — all in `types-jsonpath`, value-exact
  vs `jsonpath.h`; `jpiNull..jpiBool` alias `jbvType` discriminants.
- SQLSTATEs: `ERRCODE_INVALID_TEXT_REPRESENTATION` (bad input),
  `ERRCODE_SYNTAX_ERROR` (`@`/`LAST` misuse), `ERRCODE_FEATURE_NOT_SUPPORTED`
  (XQuery x-flag), `ERRCODE_OUT_OF_MEMORY` (buffer over-limit) — all match C.
- OIDs DATE/TIME/TIMESTAMP/TIMETZ/TIMESTAMPTZ from `types-tuple`; REG_* cflags
  from `backend-regex-core` (value-exact).

## Model reconciliations (vs src-idiomatic base)

- StringInfo → `mcx::PgVec<'mcx, u8>` (context-charged byte spine), every
  append fallible (`try_reserve` + `mcx.oom`), guarded against `MAX_ALLOC_SIZE`
  (the `AllocSizeIsValid` 1 GiB bound) — matches the repo's other adt crates
  (json.c). The base's bespoke `AppendBuf` is replaced by the repo idiom.
- Errors via `types_error::{PgError, ereturn}` (the repo's soft/hard pattern),
  not `backend-utils-error`'s builder (avoids a heavier dep), same as cash.c.
- `escape_json_with_len` escapes into a scratch `PgString` (the seam's typed
  buffer) then byte-appends to the output spine.

## Seams

OUTWARD (this crate consumes; installed by owners):
- `parse` (parsejsonpath) — `backend-utils-adt-jsonpath-gram-seams` (NEW), owner
  unported → panics until gram/scan lands.
- `escape_json_with_len` — `backend-utils-adt-json-seams`, owner json.c ported.
- `expr_type` — `backend-nodes-nodeFuncs-seams`, installed by backend-nodes-core.

DIRECT deps (no cycle, so no seam, per AGENTS.md):
- `numeric_out` — `backend-utils-adt-numeric::io`.
- `datetime_format_has_tz` — `backend-utils-adt-formatting`.

INWARD: none. Every public jsonpath.c function is consumed only by unported
units (jsonpath_exec.c, jsonfuncs.c, optimizer clauses.c), so this crate owns
no `-seams` crate yet and its `init_seams()` installs nothing (empty body, not
wired — the seams-init guard skips zero-install crates). The companion seam
crate is created by the first cross-cycle consumer when it lands.

## Decomposition

The full jsonpath family is 7449 LOC across 4 C units with disjoint dependency
surfaces; this task ports only the `jsonpath.c` unit (the I/O + compile + print
+ reader layer, ~1529 LOC). The other three are genuinely blocked:

- `jsonpath_exec.c` (4493 LOC) — the `jsonb_path_{exists,query,match,value}`
  executor — depends on the jsonb iteration API, numeric arithmetic, datetime
  adt, regex execution, fmgr `FunctionCall`, and the `TableFunc` scan machinery
  (`JsonTable*`), most of which are unported. Separate unit, kept `todo`.
- `jsonpath_gram.y` / `jsonpath_scan.l` — the bison/flex grammar+scanner. A
  large LALR-table port (the `parse` seam's owner). Separate units, kept `todo`.

## Tests

11 unit tests pass (regex-flag conversion incl. quote-override & x-flag error,
operation name/priority tables, jsp_is_scalar bounds, and flatten→reader
round-trips for root.key / bool / index-array-subscript built without the
parser, plus the `@`/`LAST` hard-error paths). Parser-dependent round-trips are
deferred to when the gram/scan unit installs the `parse` seam.

## Independent re-derivation (spot-checks)

Re-derived from the C, not the port's comments:
- `printJsonPathItem` jpiAny: all six bound spellings (`**`, `**{last}`,
  `**{%u}`, `**{last to %u}`, `**{%u to last}`, `**{%u to %u}`) match.
- `flattenJsonPathParseItem` jpiIndexArray: offset patch math (`offset=buf.len`,
  zeros `4*2*nelems`, `frompos/topos -= pos`, `ppos=offset+i*2*4`,
  `ppos[0]=from ppos[1]=to`) and the subscript recursion uses `nestingLevel`
  (not arg-adjusted) — match.
- `jspGetArraySubscript`: `from` always built, `to` only when offset nonzero —
  match.
- `jspIsMutableWalker`: jpiTimeTz/jpiTimestampTz set `jpdsDateTimeNonZoned` (C
  quirk) — mirrored.
- `jspInitByBuffer`: INTALIGN-relative-to-region offset arithmetic and
  child_at(`base+off`) verified equivalent to C's `v->base + off`.

## Verdict: PASS

Every jsonpath.c function MATCH or (the 3 external ops) correctly SEAMED/direct
per the cycle rules. No MISSING/PARTIAL/DIVERGES. No seam findings (no inward
seam owed; outward seams are thin marshal+delegate). No design-rule violations.

## Gate

cargo check --workspace: PASS. no-todo-guard: PASS. seams-init (both recurrence
guards): PASS. crate unit tests: 11/11 PASS.
