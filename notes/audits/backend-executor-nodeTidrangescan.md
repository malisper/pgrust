# Audit: backend-executor-nodeTidrangescan

- **Verdict:** PASS
- **Date:** 2026-06-12
- **Model:** claude-opus-4-8[1m] (Opus 4.8, 1M context)
- **Unit:** backend-executor-nodeTidrangescan
- **Branch:** port/backend-executor-nodeTidrangescan
- **C source:** `src/backend/executor/nodeTidrangescan.c` (PostgreSQL 18.3)

## Method

Independent re-derivation per `.claude/skills/audit-crate/SKILL.md`. Every
function definition in `nodeTidrangescan.c` was enumerated and cross-checked
against the c2rust rendering
(`c2rust-runs/backend-executor-nodeTidrangescan/src/nodeTidrangescan.rs`) and
the Rust port. Constants (operator OIDs, `PG_UINT16_MAX`,
`SelfItemPointerAttributeNumber`) and the `itemptr.h`/`itemptr.c` helpers were
verified against the C headers, not from memory. The inlined `execScan.c`
driver (`ExecScan`/`ExecScanExtended`/`ExecScanFetch`) was checked against
`execScan.c` + `execScan.h`.

The first pass found one design-conformance violation (introduced opacity,
types.md rule 6); it was fixed and the affected functions re-audited from
scratch. This report reflects the post-fix state.

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `IsCTIDVar` (macro) | nodeTidrangescan.c:33 | lib.rs `is_ctid_var` | MATCH | `IsA(Var) && varattno == SelfItemPointerAttributeNumber` (-1, verified `sysattr.h:21`); NULL arg → false mirrored by missing operand. |
| `MakeTidOpExpr` | :56 | lib.rs:183 | MATCH | leftop/rightop CTID classification; `invert`; opno switch 2801/2799/2802/2800 with `<=`/`>=` setting `inclusive` and fall-through; both `elog(ERROR)` paths return `Err(ERRCODE_INTERNAL_ERROR)`. OIDs verified `pg_operator.dat:236-249`. |
| `TidExprListCreate` | :105 | lib.rs:262 | MATCH | foreach over `tidrangequals`; `IsA(OpExpr)` guard → "could not identify CTID expression"; appends one `TidOpExpr` per qual into the per-query-context `PgVec`. |
| `TidRangeEval` | :138 | lib.rs:301 | MATCH | bounds init to `[0,0]`..`[InvalidBlockNumber, PG_UINT16_MAX]`; per-expr eval, NULL→`Ok(false)`; lower/upper narrowing with `ItemPointerInc`/`Dec` and `ItemPointerCompare`; copy to mintid/maxtid; `Ok(true)`. Reads `node.ss.ps.ps_ExprContext` like C. |
| `TidRangeNext` | :221 | lib.rs:378 | MATCH | `!trss_inScan` → eval (false→stop); beginscan vs rescan on `ss_currentScanDesc`; set `trss_inScan`; getnextslot; on exhaustion clear `trss_inScan` + clear slot. |
| `TidRangeRecheck` | :274 | lib.rs:417 | MATCH | re-eval (false→false); `Assert(ItemPointerIsValid(tts_tid))` → `debug_assert`; range check `< mintid || > maxtid` → false. |
| `ExecTidRangeScan` | :305 | lib.rs:637 | MATCH | `ExecScan(&ss, TidRangeNext, TidRangeRecheck)`. |
| `ExecReScanTidRangeScan` | :319 | lib.rs:645 | MATCH | `trss_inScan = false`; `ExecScanReScan` (seam). |
| `ExecEndTidRangeScan` | :338 | lib.rs:657 | MATCH | `if scan != NULL table_endscan` (seam). |
| `ExecInitTidRangeScan` | :358 | lib.rs:675 | MATCH | makeNode-shaped state; plan/state/ExecProcNode links (seam); ExprContext; open rel; scan slot; result type/proj; init qual; `TidExprListCreate`. |

### Inlined `execScan.c` driver (linked into `nodeTidrangescan.o` in C)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ExecScan` | execScan.c | lib.rs:590 | MATCH | reads `es_epq_active`, `ps.qual`, `ps_ProjInfo` → `ExecScanExtended`. |
| `ExecScanExtended` | execScan.h | lib.rs:525 | MATCH | no-qual/no-proj fast path; reset-econtext; loop fetch → TupIsNull handling (clears proj result slot if projecting) → set scantuple → qual → project/return → `InstrCountFiltered1` → reset and retry. |
| `ExecScanFetch` | execScan.h | lib.rs:453 | MATCH | `CHECK_FOR_INTERRUPTS`; EPQ branch (scanrelid==0 ext-param; relsubs_done; relsubs_slot; relsubs_rowmark) all faithful, then `accessMtd`. |

### In-crate helpers (`itemptr.h`/`itemptr.c`, `c.h`)

| Helper | Source | Verdict | Notes |
|---|---|---|---|
| `ItemPointerGetBlockNumberNoCheck` / `…OffsetNumberNoCheck` / `ItemPointerIsValid` / `ItemPointerSet` / `ItemPointerCopy` | itemptr.h | MATCH | verified against header. |
| `ItemPointerCompare` | itemptr.c | MATCH | block-then-offset, NoCheck accessors. |
| `ItemPointerInc` | itemptr.c | MATCH | `off==PG_UINT16_MAX && blk!=InvalidBlockNumber` rollover. |
| `ItemPointerDec` | itemptr.c | MATCH | `off==0 && blk!=0` rollunder. |
| `InstrCountFiltered1` | instrument.h | MATCH | bumps `nfiltered1` when instrument present. |
| `TupIsNull` (`scan_tuple_is_null`) | tuptable.h | MATCH | absent slot or `TTS_FLAG_EMPTY`. |
| operator OIDs `2799/2800/2801/2802`, `PG_UINT16_MAX = u16::MAX` | pg_operator.dat / c.h | MATCH | values verified against headers. |

## Seam audit

Owned seam crate: `crates/backend-executor-nodeTidrangescan-seams`. Every
declaration stands in for an operation in a subsystem *below* the executor node
layer (expression compile/eval — execExpr; execUtils/execScan init helpers;
table-AM `table_*`; the `execScan.c` leaf operations / EvalPlanQual machinery).
None is an inward-facing operation that `nodeTidrangescan.c` implements for
others, so this crate installs no seams of its own — `init_seams()` is
legitimately empty and is wired into `seams-init::init_all()`
(`seams-init/src/lib.rs:29`). The outward seams are installed by their owning
subsystems when they land. Each seam body is a thin declaration (panic default);
no branching/construction/computation lives on any seam path. No findings.

## Design conformance

**Finding (fixed): introduced opacity — types.md rule 6.** The initial port
modelled `TidOpExpr.exprstate` (C `ExprState *`) as an invented
`ExprStateHandle(u64)` generation-index newtype in `types-tidrange`, with the
`exec_init_expr` seam returning the handle and `exec_eval_expr_switch_context`
taking it. `ExprState` is a real struct C spells out (not a `void *`) and
already exists in the repo (`types-nodes::execexpr::ExprState`), with the
established `ExprState *` representation being `Option<PgBox<'mcx, ExprState>>`
(e.g. `nodeMergejoin`'s `MergeJoinClause.lexpr/rexpr`). This is exactly the
rule-6 "opacity is inherited, never introduced" violation and the memory note
"Opacity inherited, never introduced."

Fix applied:
- `types-tidrange`: `TidOpExpr` gained `'mcx` and `exprstate:
  Option<PgBox<'mcx, ExprState>>`; `trss_tidexprs: PgVec<'mcx,
  TidOpExpr<'mcx>>`; removed `ExprStateHandle`.
- seams: `exec_init_expr` now returns `PgResult<PgBox<'mcx, ExprState>>`;
  `exec_eval_expr_switch_context` now takes `&ExprState` + the `EcxtId`
  (the established `ExprContext *` representation, not invented opacity),
  matching `nodeMergejoin`'s `exec_qual::call(state, econtext, estate)` idiom.
- `MakeTidOpExpr`/`TidRangeEval`: store/borrow the real `PgBox<ExprState>`;
  `TidRangeEval` reads `node.ss.ps.ps_ExprContext` (as C does) and passes it
  through the eval seam.

No remaining design findings:
- Allocating paths (`TidExprListCreate`, `ExecInitTidRangeScan`) use `Mcx` +
  `PgResult` (per-query context, fallible `vec_with_capacity_in`).
- No shared statics, no ambient-global seams, no locks across `?`, no
  registry-shaped side tables, no unledgered divergence markers.
- Error severities/SQLSTATEs match (`elog(ERROR)` → `ERRCODE_INTERNAL_ERROR`).

## Verification

- `cargo build -p types-tidrange -p backend-executor-nodeTidrangescan-seams -p
  backend-executor-nodeTidrangescan -p seams-init` — clean.
- `cargo test -p backend-executor-nodeTidrangescan` — 17 passed, 0 failed
  (itemptr parity, CTID classification, eval narrowing/normalize/null,
  next/recheck/exec/end/rescan/init).

## Conclusion

Every C function is MATCH; the inlined execScan driver is MATCH; seam audit is
clean with a correctly-empty owner installer; the one design-conformance
violation (rule 6 introduced opacity) was fixed and re-audited. **PASS.**
