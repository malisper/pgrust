# Audit: backend-utils-adt-varchar (varchar.c)

C source: `postgres-18.3/src/backend/utils/adt/varchar.c` (1225 LOC).
Crate: `crates/backend-utils-adt-varchar` (new owner crate created).
Scope: varchar.c ONLY (the CATALOG bundle `backend-utils-adt-string-byte`
also lists like.c/like_support.c/varbit.c — out of scope here, like encode.c
was split out earlier). Result: PASS.

## Model reconciliation (src-idiomatic -> repo)

The src-idiomatic base carried each value as a full 4-byte-header varlena in a
plain `Vec<u8>` built into a function-local `MemoryContext`, declared its own
panic seams over handle types (`MsgBufferHandle`/`SortSupportHandle`/...), and
returned `CstringValue`. This was reconciled to the repo model:

- Text/bpchar/varchar VALUES carry as the **header-less payload** `PgVec<'mcx,
  u8>` charged to a caller-supplied `Mcx<'mcx>` (matches varlena's
  `cstring_to_text_with_len`, which returns the payload, header stamped only at
  the Datum/FFI boundary). So `bpchar_input`/`varchar_input`/`bpchar`/`varchar`/
  `char_bpchar`/`bpchar_name`/`name_bpchar` produce payload bytes; SET_VARSIZE
  is the boundary's job. `*out`/`*typmodout` return a NUL-terminated cstring
  `PgVec`. `*send` returns a full `Bytea<'mcx>` image via varlena's `textsend`.
- Real owners reached directly / via real seams instead of new panic seams:
  - `varstr_cmp`, `check_collation_set`, `cstring_to_text_with_len`,
    `bpchartruelen`, `textsend` — `backend-utils-adt-varlena` (landed) pub fns.
  - `pg_mbstrlen_with_len`, `pg_mbcliplen`, `pg_database_encoding_max_length`,
    and the NEW `pg_mbcharcliplen` seam — `backend-utils-mb-mbutils-seams`.
  - `collation_is_deterministic`, `pg_strxfrm` (the `pg_strnxfrm` analog) —
    `backend-utils-adt-pg-locale-seams`.
  - `array_get_integer_typmods` — `backend-utils-adt-arrayutils` pub fn.
  - `hash_bytes`/`hash_bytes_extended` (= `hash_any`/`_extended`) —
    `common-hashfn`.
  - `pq_getmsgtext` — `backend-libpq-pqformat`.
- Soft errors: C `ereturn(escontext, NULL, ...)` -> `types_error::ereturn(
  escontext, None, err)` over `SoftErrorContext`.

## New seam installed/declared

`pg_mbcharcliplen` added to `backend-utils-mb-mbutils-seams` (infallible
`-> i32`, mirroring the sibling `pg_mbcliplen` decl). mbutils is not yet ported
on the new error model, so — like the pre-existing `pg_mbcliplen`/
`pg_mbstrlen_with_len`/`pg_database_encoding_max_length` mb seams — it has no
in-workspace owner installer yet (tests install a mock). This is the standard
seam-and-panic-until-owner-lands posture, not a divergence.

## Function-by-function vs C

All 38 SQL/static functions ported with full logic, verified line-by-line:

- anychar_typmodin (32) / anychar_typmodout (71): count!=1 -> 22023; *tl<1 /
  >MaxAttrSize -> 22023; typmod = VARHDRSZ + nchars; "(%d)" or "" cstring.
- bpchar_input (129): typmod<VARHDRSZ -> actual len; else maxchars; charlen via
  pg_mbstrlen_with_len; >maxchars -> pg_mbcharcliplen + non-space check (soft
  errsave 22001) + clip; else blank-pad to maxlen. bpcharin/bpcharout/
  bpcharrecv (pq_getmsgtext, NUL-trimmed)/bpcharsend (textsend).
- bpchar (270): maxlen<VARHDRSZ -> Source; charlen==maxlen -> Source; >maxlen ->
  cliplen + (implicit) space check 22001 + truncate; else pad. New(payload).
- char_bpchar (352): one payload byte. bpchar_name (370): cliplen to
  NAMEDATALEN-1, strip blanks, zero-pad to NAMEDATALEN. name_bpchar (406): copy
  up to first NUL via cstring_to_text_with_len.
- varchar_input (456) / varcharin / varcharout / varcharrecv / varcharsend:
  maxlen=typmod-VARHDRSZ; typmod>=VARHDRSZ && len>maxlen -> cliplen + space
  check (soft 22001) + clip; cstring_to_text_with_len (NO pad).
- varchar (608): maxlen<0 || len<=maxlen -> Source; else cliplen + (implicit)
  space check + truncate.
- bcTruelen (669) / bpchartruelen: delegates to varlena's ported bpchartruelen.
  bpcharlen (692): truelen, then pg_mbstrlen_with_len if max_len!=1.
  bpcharoctetlen (708): raw_total_size - VARHDRSZ (no detoast; caller supplies
  toast_raw_datum_size).
- check_collation_set (726) + OidIsValid: InvalidOid -> 42P22 with hint.
- bpchareq (742) / bpcharne (783): truelen both; deterministic -> length-then-
  bitwise; non-det -> varstr_cmp == / != 0.
- bpcharlt/le/gt/ge/cmp (824-927): varstr_cmp of truncated values, sign tests.
- bpchar_sortsupport (929): varstr_sortsupport(ssup, BPCHAROID,
  ssup_collation); the ssup_cxt switch is inside varlena's varstr_sortsupport.
- bpchar_larger (946) / bpchar_smaller (964): cmp>=0 / cmp<=0 select-arg booleans.
- hashbpchar (987) / hashbpcharextended (1043): !collid -> 42P22; truelen;
  deterministic -> hash_any(extended); non-det -> pg_strxfrm + appended trailing
  NUL, hash bsize+1 (NUL-in-hash behavior preserved). The C bsize/rsize two-call
  pg_strnxfrm guard is internal to the repo `pg_strxfrm` seam (returns the full
  blob), so the `rsize>bsize` elog is owned there.
- internal_bpchar_pattern_compare (1108) + lt/le/ge/gt/btcmp (1130-1207):
  memcmp(Min(len1,len2)) then length tiebreak.
- btbpchar_pattern_sortsupport (1210): varstr_sortsupport forcing C_COLLATION_OID.

## Deferred (project-wide, not divergences)

- fmgr PG_FUNCTION_ARGS/Datum boundary: each fn takes unwrapped args + typed
  return; bare-word PGFunction registry deferred project-wide.
- varchar_support (564) planner node simplification: the C body manipulates
  SupportRequestSimplify/FuncExpr/Const planner nodes (nodeFuncs/supportnodes).
  NOT ported here — the node-simplification logic belongs to the planner
  subsystem; no `varchar_support` is exposed (its only caller is the planner via
  the support-function dispatch, which is unported). Faithful: C's only effect
  is `relabel_to_typmod` on a Const-typmod widening, pure planner-tree surgery.
- PG_FREE_IF_COPY: the detoast/free of toasted inputs is the caller's fmgr glue
  in this carrier model (inputs arrive as already-detoasted payloads).

## Tests

24 unit tests: truelen/pattern-compare/octetlen/typmodin/typmodout pure paths;
bpchar/varchar input blank-pad + truncate + soft/hard 22001 + no-typmod;
coercion Source/New/explicit/implicit; char/name/bpchar conversions; out NUL;
eq/ne/ordering/larger/smaller; hash deterministic + non-deterministic + zero-
collation error. Mocks installed once (seam-install-once) modeling single-byte
encoding + C collation.

Gate: cargo check --workspace OK; no-todo-guard OK; seams-init OK; full
workspace test green modulo the documented allowed flakes (the
`backend-access-hashfunc text_nondeterministic` cross-binary seam-global flake
passes in isolation).
