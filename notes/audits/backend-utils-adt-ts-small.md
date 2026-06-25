# Audit: backend-utils-adt-ts-small

- **Verdict: PASS**
- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Branch:** `port/backend-utils-adt-ts-small`

C sources (`c_sources` glob → `../pgrust/postgres-18.3/src/backend/utils/adt/`):
`tsquery_cleanup.c`, `tsquery_rewrite.c`, `tsquery_util.c`.
Cross-checked against `../pgrust/c2rust-runs/backend-utils-adt-ts-small/src/`
(`tsquery_cleanup.rs`, `tsquery_rewrite.rs`, `tsquery_util.rs`) — same function
set, no missing core.

Port crate: `crates/backend-utils-adt-ts-small/` (`util.rs`, `cleanup.rs`,
`rewrite.rs`).

## Per-function table

### tsquery_util.c → util.rs

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| QT2QTN | :24 | util.rs `qt2qtn`/`qt2qtn_inner` | MATCH | Recursive polish-order build; `child[0]=in+1`, `child[1]=in+left`; OP_NOT → 1 child; sign = OR of child signs; leaf `sign = 1 << (valcrc % 32)`, `word = operand+distance`. `check_stack_depth` via tcop seam. |
| QTNFree | :64 | RAII `Drop` of `QTNode`/`PgVec` | MATCH | `pfree`/free-hints dissolve into ownership; `QTN_WORDFREE`/`QTN_NEEDFREE` inert under Rust ownership (documented). |
| QTNodeCompare | :96 | util.rs `QTNodeCompare` | MATCH | type order (greater→-1), oper, nchild, recursive child compare, OP_PHRASE distance tiebreak, QI_VAL valcrc then `tsCompareString(...,false)`; else-branch → `elog(ERROR,"unrecognized QueryItem type")`. |
| cmpQTN | :152 | folded into `QTNSort` comparator | MATCH | `sort_by` closure delegates to `QTNodeCompare`; first error captured + propagated (comparator can't return Result). Total order ⇒ stable sort is a harmless refinement of qsort. |
| QTNSort | :163 | util.rs `QTNSort` | MATCH | recurse children, then sort when `nchild>1 && oper!=OP_PHRASE`. |
| QTNEq | :183 | util.rs `QTNEq` | MATCH | `sign=a&b; if !(sign==a.sign && sign==b.sign) false; else compare==0`. |
| QTNTernary | :201 | util.rs `QTNTernary` | MATCH | recurse; only AND/OR flatten; splice same-operator children in place. Grandchildren pre-flattened by recursion, so the rebuild-list form is equivalent to C's in-place `i += cc->nchild-1` splice. |
| QTNBinary | :250 | util.rs `QTNBinary` | MATCH | recurse; `while nchild>2` insert `nn=OPR(child[0],child[1])`, set `child=[nn, c_last, c2..c_{m-2}]`; `nn.flags=QTN_NEEDFREE`, `nn.sign=c0.sign|c1.sign`; nn.valnode has only type+oper. Rebuild form reproduces C's index shuffle exactly. |
| cntsize | :292 | util.rs `cntsize` | MATCH | `*nnode+=1`; OPR recurse; else `*sumlen += length+1`. |
| fillQT | :323 | util.rs `fill_qt` | MATCH | QI_VAL: copy QueryOperand, copy word, set distance, NUL, advance; OPR: copy QueryOperator, emit child[0], then for 2-child set `left = curitem-curitem0` and emit child[1]. |
| QTN2QT | :363 | util.rs `qtn2qt` | MATCH | cntsize; `TSQUERY_TOO_BIG` → ERRCODE_PROGRAM_LIMIT_EXCEEDED "tsquery is too large"; COMPUTESIZE; SET_VARSIZE; size=nnode; fill; encode. Result `Vec<u8>` is the owned datum. |
| QTNCopy | :396 | `QTNode::clone_in` | MATCH | deep copy; `flags |= QTN_NEEDFREE`; QI_VAL → copy word + `QTN_WORDFREE`; else recurse children; preserves sign + source flags (incl. QTN_NOCHANGE). |
| QTNClearFlags | :434 | util.rs `QTNClearFlags` | MATCH | `flags &= ~flags`; recurse unless QI_VAL. See note 1 (dropped stack check — benign). |

Supporting: `tsCompareString` (tsvector_op.c:1152) reproduced exactly in
util.rs (empty-string and prefix rules verified byte-for-byte). ABI codec
(`decode_record`/`encode_record`, `get_query`/`get_operand`) verified against
the `QueryItem` union layout (QI_SIZE=12; QueryOperand bitfield `length:12,
distance:20` at offset 8; valcrc at offset 4).

### tsquery_cleanup.c → cleanup.rs

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| maketree | :33 | cleanup.rs `maketree` | MATCH | `right=in+1`; if `oper!=OP_NOT` `left=in+left`; index-based over the decoded array. |
| plainnode | :62 | cleanup.rs `plainnode` | MATCH | grow-on-`cur==len`; copy valnode; QI_VAL → cur++; OP_NOT → `left=1`, cur++, recurse right; else cur++, recurse right, `ptr[cur0].left=cur-cur0`, recurse left; node consumed (pfree). |
| plaintree | :97 | cleanup.rs `plaintree` | MATCH | only build when root is VAL/OPR; else empty (NULL); returns `(ptr, len)`. |
| freetree | :115 | cleanup.rs `freetree` | MATCH | recurse left/right then drop; `check_stack_depth` preserved. |
| clean_NOT_intree | :136 | cleanup.rs `clean_NOT_intree` | MATCH | QI_VAL passthrough; OP_NOT → freetree+NULL; OP_OR short-circuit (on left==NULL, restores original right so freetree frees it, matching C); AND/PHRASE collapse rules (both/left/right NULL). |
| clean_NOT | :190 | cleanup.rs `clean_NOT` | MATCH | maketree → clean_NOT_intree → plaintree. |
| clean_stopword_intree | :238 | cleanup.rs `clean_stopword_intree` | MATCH | `*ladd=*radd=0`; QI_VAL passthrough; QI_VALSTOP → NULL; OP_NOT propagate child distances; operator branch: recurse both, ndistance for phrase, the four collapse cases with exact ladd/radd phrase-distance arithmetic and `Max(lladd,rladd)` for non-phrase both-null. |
| calcstrlen | :363 | cleanup.rs `calcstrlen` | MATCH | QI_VAL → length+1; OPR → right (+ left unless OP_NOT). |
| cleanup_tsquery_stopwords | :387 | cleanup.rs `cleanup_tsquery_stopwords` | MATCH | size==0 → return input copy; clean_stopword_intree(maketree); NULL → NOTICE (when noisy) + empty HDRSIZETQ datum; else calcstrlen/plaintree/COMPUTESIZE, build datum, relocate operands rewriting distances. NOTICE message + ERRCODE path verified. |

### tsquery_rewrite.c → rewrite.rs

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| findeq | :34 | rewrite.rs `findeq` | MATCH | signature/type/NOCHANGE guards; OPR same-oper; equal-nchild QTNEq replace; subset match (sorted one-pass, `matched[]`, nmatched==ex.nchild → collapse + insert subs copy w/ NOCHANGE + QTNSort); QI_VAL valcrc-then-QTNEq replace. `*isfind` set on every replacement. |
| dofindsubquery | :205 | rewrite.rs `dofindsubquery` | MATCH | stack + interrupt checks (tcop seams); findeq at node; if !NOCHANGE && OPR recurse children dropping NULLs; nchild==0 → NULL; nchild==1 && !OP_NOT → promote sole child. |
| findsubquery | :266 | rewrite.rs `findsubquery` | MATCH | dofindsubquery + optional `*isfind`. |
| tsquery_rewrite_query | :279 | rewrite.rs `tsquery_rewrite_query` | MATCH (SEAMED dep) | size==0 → copy; QT2QTN/Ternary/Sort; SPI execution via `backend-executor-spi-seams::tsquery_rewrite_run`; type check `natts==2 && both==TSQUERYOID` else ERRCODE_INVALID_PARAMETER_VALUE; batch/row loop with isnull handling, per-pair QT2QTN/Ternary/Sort/findsubquery + QTNClearFlags(NOCHANGE)/Ternary/Sort re-prep; tree==NULL → empty datum else QTNBinary/QTN2QT. |
| tsquery_rewrite | :409 | rewrite.rs `tsquery_rewrite` | MATCH | size==0 on query OR ex → copy; QT2QTN/Ternary/Sort on query+ex; subs iff subst.size; findsubquery; NULL → empty datum else QTNBinary/QTN2QT. |

## Notes / minor divergences (non-blocking)

1. **`QTNClearFlags` drops `check_stack_depth()`.** C calls it (the fn is
   `void`); the port omits it to keep the signature infallible, justified in a
   doc comment: every call site is preceded by `QTNTernary`/`QTNSort` over the
   same tree, both of which already run `check_stack_depth` at the same depth,
   so the bounded-depth guarantee is unchanged. Behavior on every input is
   identical (the only observable effect of the C call would be a
   stack-depth ereport, which the immediately-preceding traversals already
   raise). Accepted as MATCH.

## Seam audit (skill §3)

- **Owned seam crates (by C-source coverage):** after the fix below, this unit
  owns **none**. `tsquery_*.c` declares no functions that other crates call
  back into.
- **`init_seams()`** is a no-op — correct, since the unit owns no seam crate.
  `seams-init::init_all()` still calls it (uniform shape).
- **Outward seam calls** (all thin marshal+delegate, justified by real
  dependency on an unported owner):
  - `backend-tcop-postgres-seams::check_stack_depth` / `check_for_interrupts`
    — owned by `tcop/postgres.c`.
  - `backend-executor-spi-seams::tsquery_rewrite_run` — the SPI
    `SPI_connect…SPI_finish` execution capability of `ts_rewrite(query,text)`.
    Declared in **SPI's** seam crate (SPI owns the capability), installed by
    the SPI owner when it lands; the two-`tsquery`-column type-check decision
    and the entire rewrite algorithm stay in-crate.
- No branching/node-construction/computation lives in any seam path.

### Finding fixed during this audit (was merge-blocking)

The port originally declared a unit-owned seam crate
`backend-utils-adt-ts-small-seams` holding `tsquery_rewrite_run`, with an empty
`init_seams()` and a comment that "the SPI owner installs it." This violates
AGENTS.md §"Per-owner seam crates" (declarations for a function live only in
that function's owner's `-seams` crate; ownership is by C-source coverage) and
the audit skill §3 (an owned seam crate with an empty installer is an automatic
FAIL — this unit owns `…-ts-small-seams` by name/coverage yet could never fill
it, because the implementation is SPI's). The SPI-execution capability belongs
to the SPI owner.

**Fix applied:** moved the `tsquery_rewrite_run` seam and its
`TsRewriteResult`/`TsRewriteRow` types into `backend-executor-spi-seams`
(alongside the other SPI seams); repointed `rewrite.rs` and the crate's
`Cargo.toml` at `backend-executor-spi-seams`; deleted the now-orphaned
`crates/backend-utils-adt-ts-small-seams`; updated the crate docs and
`init_seams()` rationale. Build + the crate's 10 unit tests pass.

## Design conformance (skill §3b)

- Allocating functions/seams take `Mcx` and return `PgResult`; OOM is guarded
  (`try_reserve`) on every data-derived allocation.
- No invented opacity: `QTNode`/`NODE` are real owned structs (PgVec/PgBox);
  no `void*` stand-ins.
- No shared statics for per-backend state; no ambient-global seams (the SPI
  seam takes its command as an explicit `String` arg and returns owned bytes).
- Seam signatures mirror the C failure surface (`PgResult`).
- Constants verified against headers: OP_NOT/AND/OR/PHRASE = 1/2/3/4,
  QI_VAL/OPR/VALSTOP = 1/2/3, QTN_NEEDFREE/NOCHANGE/WORDFREE = 0x01/0x02/0x04,
  HDRSIZETQ = 8, TSQUERYOID = 3615, QI_SIZE = 12, QueryOperand bitfield
  `length:12,distance:20`.

## Verdict

**PASS.** Every C function MATCHes (or is MATCH with a deferred unported
*callee* via a justified, thin seam). The one merge-blocking seam-ownership
violation was fixed and re-audited from scratch. No MISSING/PARTIAL/DIVERGES.
