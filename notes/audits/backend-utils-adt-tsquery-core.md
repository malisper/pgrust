# Audit: backend-utils-adt-tsquery-core

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Branch:** `port/backend-utils-adt-tsquery-core`

C sources (`c_sources` glob → `../pgrust/postgres-18.3/src/backend/utils/adt/`):
`tsquery.c`, `tsquery_gist.c`, `tsquery_op.c`. Cross-checked against
`../pgrust/c2rust-runs/backend-utils-adt-tsquery-core/src/` — same function set,
no missing core.

Port crate: `crates/backend-utils-adt-tsquery-core/` (`tsquery.rs`, `op.rs`,
`gist.rs`). The `QTNode` expression-tree toolkit and the cleanup helpers
(`clean_NOT`, `cleanup_tsquery_stopwords`) are reused from the sibling
`backend-utils-adt-ts-small` crate (its C sources are the other three `tsquery_*`
TUs). A `tsquery` value is its flat varlena image (`&[u8]` in / `Vec<u8>` out),
matching the established repo model for this datum.

## tsquery.c → tsquery.rs

| C function | port | verdict | notes |
|---|---|---|---|
| `get_modifiers` | `get_modifiers` | MATCH | `:AB*` weight/prefix bits; `pg_mblen==1` guard via mb seam |
| `parse_phrase_operator` | `parse_phrase_operator` | MATCH | PHRASE_OPEN/DIST/CLOSE/FINISH FSM; `strtol` + ERANGE/0..MAXENTRYPOS soft-error |
| `parse_or_operator` | `parse_or_operator` | MATCH | `pg_strncasecmp("or",2)`; `-`/`_`/`t_isalnum` reject; operand probe; `buf += 2` |
| `gettoken_query_standard` | `gettoken_query_standard` | MATCH | `!isspace` (NUL-not-space) value branch; `:` PT_ERR; phrase/and/or/close/end |
| `gettoken_query_websearch` | `gettoken_query_websearch` | MATCH | `-`=NOT, `"`=quoted single token (`count++`), ISOPERATOR skip, implicit AND, pushStop+END |
| `gettoken_query_plain` | `gettoken_query_plain` | MATCH | whole remaining string as one operand; `count++` |
| `pushOperator` | `push_operator` | MATCH | QI_OPR, distance only for OP_PHRASE; lcons |
| `pushValue_internal` | `push_value_internal` | MATCH | MAXSTRPOS/MAXSTRLEN soft-error guards; len/distance bitfields |
| `pushValue` | `push_value` | MATCH | MAXSTRLEN guard; legacy CRC32 seam; operand append + NUL; sumlen |
| `pushStop` | `push_stop` | MATCH | QI_VALSTOP placeholder |
| `pushOpStack` | `push_op_stack` | MATCH | STACKDEPTH=32 overflow elog |
| `cleanOpStack` | `clean_op_stack` | MATCH | NOT right-assoc (`>=`) vs others (`>`) priority |
| `makepol` | `makepol` | MATCH | recursive; check_stack_depth; PT_VAL/OPR/OPEN/CLOSE/ERR; soft-error early returns. `pushval_asis` (the only in-tree PushFunction) inlined to `pushValue`. |
| `findoprnd_recurse` | `findoprnd_recurse` | MATCH | NOT left=1; binary sets left=pos-tmp; needcleanup on QI_VALSTOP; range/type elog |
| `findoprnd` | `findoprnd` | MATCH | extra-nodes elog |
| `parse_tsquery` | `parse_tsquery` | MATCH | tokenizer select + tsv_flags; init/close parser; empty→NOTICE+size0; TSQUERY_TOO_BIG; pack QueryItems + operands; findoprnd; cleanup if needcleanup. `noisy = escontext.is_none()` mirrors `!IsA(ErrorSaveContext)`. |
| `tsqueryin` | `tsqueryin` | MATCH | `parse_tsquery(in, asis, NULL, 0, escontext)` |
| `infix` | `infix` | MATCH | recursive printer: QI_VAL quote-escape (`'`/`\`) + `:*ABCD`; NOT paren; binary right-buf then left + ` | `/` & `/` <N>`/` <-> `; parens on priority or right-phrase |
| `tsqueryout` | `tsqueryout` | MATCH | size0→empty; infix from priority -1; bytes without trailing NUL |
| `tsquerysend` | `tsquerysend` | MATCH | int32 size; per-item int8 type; QI_VAL weight/prefix/sendstring; QI_OPR oper + phrase distance int16 |
| `tsqueryrecv` | `tsqueryrecv` | MATCH | size cap; per-item decode + sanity (weight>0xF / MAXSTRLEN / MAXSTRPOS / oper / right-operand); CRC; findoprnd; operand copy; SET_VARSIZE |
| `tsquerytree` | `tsquerytree` | MATCH | size0→empty; clean_NOT; empty tree→"T"; else infix |
| `pushval_asis` | inlined in `makepol` | MATCH | only PushFunction in this TU; documented inline |

## tsquery_op.c → op.rs

| C function | port | verdict | notes |
|---|---|---|---|
| `tsquery_numnode` | `tsquery_numnode` | MATCH | `query->size` |
| `join_tsqueries` | `join_tsqueries` | MATCH | 2-child OPR node: child[0]=QT2QTN(b), child[1]=QT2QTN(a); phrase distance |
| `tsquery_and` | `tsquery_and` | MATCH | empty→other; else QTN2QT(join OP_AND) |
| `tsquery_or` | `tsquery_or` | MATCH | empty→other; OP_OR |
| `tsquery_phrase_distance` | `tsquery_phrase_distance` | MATCH | 0..MAXENTRYPOS hard-error; empty→other; OP_PHRASE |
| `tsquery_phrase` | `tsquery_phrase` | MATCH | distance 1 |
| `tsquery_not` | `tsquery_not` | MATCH | size0→a; else 1-child OP_NOT node |
| `CompareTSQ` | `compare_tsq` | MATCH | size, then VARSIZE, then QTNodeCompare |
| `tsquery_cmp` | `tsquery_cmp` | MATCH | |
| `tsquery_lt/le/eq/ge/gt/ne` (CMPFUNC) | `tsquery_lt`..`ne` | MATCH | `<0`/`<=0`/`==0`/`>=0`/`>0`/`!=0` |
| `makeTSQuerySign` | `makeTSQuerySign` | MATCH | `1u64 << (valcrc % TSQS_SIGLEN)` over QI_VAL (TSQS_SIGLEN=64; distinct from QT2QTN's %32 node sign) |
| `collectTSQueryValues` | `collect_tsquery_values` | MATCH | one owned operand copy per QI_VAL |
| `cmp_string` | inlined byte cmp | MATCH | `strcmp` == byte ordering |
| `tsq_mcontains` | `tsq_mcontains` | MATCH | sort+qunique both sides; size-check then ordered containment scan |
| `tsq_mcontained` | `tsq_mcontained` | MATCH | `tsq_mcontains(ex, query)` |

## tsquery_gist.c → gist.rs

| C function | port | verdict | notes |
|---|---|---|---|
| `gtsquery_compress` (leaf) | `gtsquery_compress_leaf` | MATCH | leaf→makeTSQuerySign; non-leaf identity is the AM boundary |
| `gtsquery_consistent` | `gtsquery_consistent` | MATCH | recheck=true; RTContains(7)/RTContainedBy(8) leaf/inner bit tests; default false |
| `gtsquery_union` | `gtsquery_union` | MATCH | OR of entry signatures (`*size` is AM's, fixed) |
| `gtsquery_same` | `gtsquery_same` | MATCH | `a == b` |
| `sizebitvec` | `sizebitvec` | MATCH | popcount over TSQS_SIGLEN bits |
| `hemdist` | `hemdist` | MATCH | `sizebitvec(a ^ b)` |
| `gtsquery_penalty` | `gtsquery_penalty` | MATCH | `hemdist(orig,new) as f32` |
| `comparecost` | `comparecost` | MATCH | `pg_cmp_s32` over cost |
| `gtsquery_picksplit` | `gtsquery_picksplit` | MATCH | seed search; cost vector sort; WISH_F split; owned PickSplit replaces GIST_SPLITVEC arrays (final scratch sentinel write inert) |
| `gtsquery_consistent_oldsig` | `gtsquery_consistent_oldsig` | MATCH | tail-call to consistent |

## Constants verified against headers

- `TSQuerySign = uint64`, `TSQS_SIGLEN = 64` (ts_utils.h) ✓
- `OP_NOT=1/OP_AND=2/OP_OR=3/OP_PHRASE=4`, `tsearch_op_priority = {4,2,1,3}` (ts_type.h / tsquery.c) ✓
- `MAXSTRLEN=(1<<11)-1`, `MAXSTRPOS=(1<<20)-1`, `MAXENTRYPOS=1<<14`, `HDRSIZETQ=VARHDRSZ+4`, `QI_VAL/QI_OPR/QI_VALSTOP` ✓ (types-tsearch, verified vs ts_type.h)
- `P_TSV_OPR_IS_DELIM=1/P_TSV_IS_TSQUERY=2/P_TSV_IS_WEB=4`, `P_TSQ_PLAIN=1/P_TSQ_WEB=2` (ts_utils.h) ✓
- `RTContainsStrategyNumber=7`, `RTContainedByStrategyNumber=8` (stratnum.h) ✓
- `STACKDEPTH=32` (tsquery.c) ✓

## Seam audit

This unit owns **no** inward seam crate (no other crate calls into the `tsquery`
core across a cycle; the shared `QTNode` toolkit lives in `ts-small`), so
`init_seams()` is a documented no-op and the crate is correctly **not** added to
`seams-init::init_all()` (same shape as `ts-small`). The recurrence guard
(`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) passes.

Outward seams (all justified by the owner being a *different, unported* TU —
mirror-and-panic until they land, never own logic):

- `backend-utils-adt-tsvector-core-seams` (NEW DECLS): `init_tsvector_parser`,
  `reset_tsvector_parser`, `gettoken_tsvector`, `close_tsvector_parser` — the
  stateful `tsvector_parser.c` engine `parse_tsquery` drives. C declares
  `struct TSVectorParseStateData` opaque, so the state rides an opaque
  `TsVectorParseStateHandle` token minted by the owner (opacity inherited, not
  introduced — cf the existing `SpellHandle`). Owner = the unported
  `backend-utils-adt-tsvector-core` unit.
- `backend-tsearch-ts-locale-seams` (NEW DECL): `t_isalnum` — sibling of the
  already-present `t_isalpha`; owner = unported `ts_locale.c`.
- Reused existing seams: `legacy_crc32_lexeme` (hash-small-seams),
  `pg_mblen_range` / `pg_database_encoding_max_length` (mbutils-seams),
  `check_stack_depth` (postgres-seams).

Each outward call is thin marshal + delegate (no branching/computation in a seam
path). The GiST `GISTENTRY`/`GistEntryVector`/`GIST_SPLITVEC` fmgr framing is the
GiST AM boundary (deferred project-wide), exactly as the merged sibling
`backend-utils-adt-tsgistidx` handles it; `picksplit` returns an owned
`PickSplit`.

## Design conformance

- No invented opacity (the one handle mirrors a C-opaque struct).
- `Mcx` + `PgResult` on every allocating / ereport-capable path; soft errors via
  `SoftErrorContext` + `ereturn` (matching numutils/copyfromparse).
- No shared statics for per-backend globals; no ambient-global seams; no locks.
- No `todo!`/`unimplemented!`; the only `unwrap()`s are in `#[cfg(test)]`.
- No unledgered divergence markers.

`cargo check --workspace` clean; crate tests pass (6/6); seams-init guard passes.

## Independent re-audit (2026-06-13)

Re-derived the full function inventory from the three C TUs and the c2rust run
(`../pgrust/c2rust-runs/backend-utils-adt-tsquery-core/src/`) from scratch — all
34 public fns + every static helper (`get_modifiers`, `parse_phrase_operator`,
`parse_or_operator`, the three `gettoken_query_*`, `pushValue_internal`,
`pushOpStack`, `cleanOpStack`, `makepol`, `findoprnd_recurse`/`findoprnd`,
`infix`, `pushval_asis`, `join_tsqueries`, `CompareTSQ`, `collectTSQueryValues`,
`cmp_string`, `sizebitvec`, `hemdist`, `comparecost`) accounted for. Spot-checked
in depth: `tsq_mcontains` merge scan (inner `break` does NOT advance `j`, mirrored
exactly), `ts_copychar_cstr` confirmed a raw `memcpy(dest,src,pg_mblen(src))` (no
sanitization) so the port's `pg_mblen`+`extend_from_slice` is identical, `not_space`
NUL-not-space semantics at EOS, `noisy = escontext.is_none()` ⟺ C
`!IsA(ErrorSaveContext)`, and `findoprnd` on the `items` Vec before the buffer
build (vs C's in-buffer findoprnd) is behaviorally identical. All header constants
re-verified against `ts_type.h`/`ts_utils.h`/`stratnum.h` (not from memory). No
own-logic stubs, no `todo!`/`unimplemented!`, no deferred-escape; the only
out-of-crate calls are thin seam delegates to unported owners. Gates re-run green:
`cargo check --workspace`, `cargo test -p backend-utils-adt-tsquery-core` (6/6),
`cargo test -p seams-init` (recurrence guard 2/2). Independent verdict: **PASS**.
