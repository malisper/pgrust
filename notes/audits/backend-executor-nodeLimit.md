# Audit: backend-executor-nodeLimit

- **Unit:** `backend-executor-nodeLimit`
- **Branch:** `worktree-wf_b13db517-93c-2` (port of `backend-executor-nodeLimit`)
- **C source:** `src/backend/executor/nodeLimit.c` (PostgreSQL 18.3)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-executor-nodeLimit/src/nodeLimit.rs`
- **Rust port:** `crates/backend-executor-nodeLimit/src/lib.rs`
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

## Context

The CATALOG row claimed `audited` while `crates/backend-executor-nodeLimit`
was ABSENT from the tree (the audit/types/seams artifacts existed on main but
the crate itself was never committed). This audit covers the now-created crate.

## Top-line verdict: **PASS**

Independent re-derivation from the C source, the c2rust rendering, and the
ported headers. All 6 C functions are `MATCH` (interface-routine logic in-crate;
every call below the node layer is a justified `SEAMED` delegate). Constants
(NodeTags, errcodes, enum orderings) verified against the PG 18.3 headers, not
memory. No seam findings; no design-conformance findings.

## Function inventory & verdicts

C functions enumerated from `nodeLimit.c` (`grep '^[A-Za-z].*('` plus statics),
cross-checked against the c2rust crate (which carried all 6):

| # | C function | C kind | Port location | Verdict |
|---|-----------|--------|---------------|---------|
| 1 | `ExecLimit` | static (ExecProcNode cb) | `ExecLimit` + `exec_limit_node` | MATCH |
| 2 | `recompute_limits` | static | `recompute_limits` | MATCH |
| 3 | `compute_tuples_needed` | static | `compute_tuples_needed` | MATCH |
| 4 | `ExecInitLimit` | extern | `ExecInitLimit` | MATCH |
| 5 | `ExecEndLimit` | extern | `ExecEndLimit` | MATCH |
| 6 | `ExecReScanLimit` | extern | `ExecReScanLimit` | MATCH |

### 1. `ExecLimit` — MATCH

The 8-state machine (`LIMIT_INITIAL/RESCAN/EMPTY/INWINDOW/WINDOWEND_TIES/
SUBPLANEOF/WINDOWEND/WINDOWSTART`) is modeled by a `'state` loop over `lstate`;
the C `switch` FALL-THRU edges (`INITIAL`→`RESCAN`, `INWINDOW`→`WINDOWEND_TIES`)
become `continue 'state`, the "return current tuple" tail becomes `break 'state`.
Verified branch-for-branch:

- `CHECK_FOR_INTERRUPTS()` first (SEAMED → tcop-postgres).
- `direction = es_direction`; `ScanDirectionIsForward` computed once, matching C
  reading the field once at entry.
- `LIMIT_RESCAN`: backward → NULL (no state change); empty window
  (`count <= 0 && !noCount`) → `LIMIT_EMPTY`/NULL; fetch loop with WITH-TIES copy
  test `position - offset == count - 1` BEFORE `position++`, and the
  `if (++position > offset) break` reproduced as `position += 1; if position >
  offset { break }` (pre-increment compare preserved). MATCH.
- `LIMIT_INWINDOW` forward: `!noCount && position - offset >= count` → either
  `LIMIT_WINDOWEND`+NULL (LIMIT_OPTION_COUNT) or fall through to `WINDOWEND_TIES`;
  else fetch (EOF→`SUBPLANEOF`+NULL), WITH-TIES last-tuple copy, `position++`.
  Backward: `position <= offset + 1` → `WINDOWSTART`+NULL; else fetch (NULL→
  elog backwards), `position--`. The `Assert(lstate == WINDOWEND_TIES)` before
  the fall-through is a `debug_assert!`. MATCH.
- `LIMIT_WINDOWEND_TIES` forward: fetch (EOF→`SUBPLANEOF`+NULL); tie test via
  `econtext->ecxt_innertuple/outertuple` + `ExecQualAndReset` → match keeps tuple
  & `position++`, mismatch → `WINDOWEND`+NULL. Backward mirrors INWINDOW backward
  but sets `LIMIT_INWINDOW`. MATCH.
- `LIMIT_SUBPLANEOF`: forward→NULL; backward re-fetch (NULL→elog) → `INWINDOW`.
  MATCH.
- `LIMIT_WINDOWEND`: forward→NULL; backward WITH-TIES re-fetch (NULL→elog) vs.
  non-ties re-return `subSlot` → `INWINDOW`. The non-ties branch correctly does
  NOT re-fetch (re-uses `subSlot`), matching C exactly (an earlier extra
  `is_none()` guard was removed during this audit to match C, which only asserts
  at the tail). MATCH.
- `LIMIT_WINDOWSTART`: backward→NULL; forward re-return `subSlot` → `INWINDOW`.
  MATCH.
- Tail `Assert(!TupIsNull(slot))` → `debug_assert!(subSlot.is_some())`;
  `return slot` → `Ok(node.subSlot)`. The C `default: elog(ERROR, "impossible
  LIMIT state")` is unrepresentable (`lstate` is an exhaustive Rust enum), so no
  arm is needed. MATCH.

`TupIsNull` (NULL pointer OR `TTS_EMPTY`) is normalized in `fetch_subplan_tuple`
via `estate.slot(id).is_empty()`, matching `ExecProcNode` + `TupIsNull`.

### 2. `recompute_limits` — MATCH

OFFSET: `ExecEvalExprSwitchContext` (SEAMED), NULL→`offset=0`, else
`DatumGetInt64` (`val.as_i64()`), `< 0` → `ereport(ERROR, ...
ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE, "OFFSET must not be
negative")`. No-OFFSET→`offset=0`. COUNT: same shape, NULL→`count=0,
noCount=true`, `< 0` → `ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE` "LIMIT must
not be negative", else `noCount=false`. No-COUNT→`count=0, noCount=true`. Reset
`position=0, subSlot=NULL, lstate=LIMIT_RESCAN`. Always calls
`ExecSetTupleBound(compute_tuples_needed(node), outerPlanState(node))` (SEAMED →
execProcnode) — the "do not skip" comment honoured (unconditional). MATCH.

### 3. `compute_tuples_needed` — MATCH

`(noCount || limitOption == LIMIT_OPTION_WITH_TIES) → -1`; else `count + offset`
with overflow tolerated (`wrapping_add`, matching the C comment "if this
overflows, we'll return a negative value, which is OK"). MATCH.

### 4. `ExecInitLimit` — MATCH

`castNode(Limit)`; `Assert(!(eflags & EXEC_FLAG_MARK))` (debug_assert);
`makeNode(LimitState)` with `ps.plan`, `ExecProcNode = ExecLimit`,
`lstate = LIMIT_INITIAL`; `ExecAssignExprContext` (SEAMED → execUtils);
`outerPlanState = ExecInitNode(outerPlan(node), ...)` (SEAMED → execProcnode);
`limitOffset/limitCount = ExecInitExpr(...)` (SEAMED → execExpr), `limitOption`
copied; `ExecInitResultTypeTL` (SEAMED → execTuples); `resultopsset = true`,
`resultops/resultopsfixed = ExecGetResultSlotOps(outer, &isfixed)` (SEAMED →
execUtils via the new `exec_get_result_slot_ops_isfixed`, which returns BOTH the
ops and the isfixed out-flag — the full C semantics incl. the `ps_ResultTupleSlot`
`TTS_FIXED` fallback, rather than naively reading `resultopsfixed`);
`ps_ProjInfo = NULL`. WITH-TIES: `last_slot = ExecInitExtraTupleSlot(estate,
desc, ops)` and `eqfunction = execTuplesMatchPrepare(desc, uniqNumCols,
uniqColIdx, uniqOperators, uniqCollations, &ps)` (SEAMED → execTuples /
execGrouping). MATCH.

### 5. `ExecEndLimit` — MATCH

`ExecEndNode(outerPlanState(node))` (SEAMED → execProcnode). MATCH.

### 6. `ExecReScanLimit` — MATCH

`recompute_limits(node)` then `if (outerPlan->chgParam == NULL) ExecReScan(outerPlan)`
(SEAMED → execAmi). The chgParam read precedes the conditional rescan exactly as
C. MATCH.

## Constants verified (against PG 18.3 headers, not memory)

- `T_Limit = 373`, `T_LimitState = 437` — `src/backend/nodes/nodetags.h:390,454`.
- `ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE = 2201W`,
  `..._RESULT_OFFSET_CLAUSE = 2201X` — `src/backend/utils/errcodes.txt:188-189`
  (`make_sqlstate(*b"2201W"/*b"2201X")` in `types-error`).
- `LimitStateCond` ordering INITIAL=0 … WINDOWSTART=7 — `execnodes.h:2897-2904`.
- `LimitOption` COUNT=0, WITH_TIES=1 — `nodes.h:437-438`.
- `EXEC_FLAG_MARK = 0x0010` — `executor.h` (matches `types-nodes::executor`).

## Seam audit

**Owned seam crates:** none. `nodeLimit.c` is a leaf executor node; no
`crates/X-seams` maps to it. `init_seams()` is empty, which is correct — and is
wired into `seams-init::init_all()` (`every_seam_installing_crate_is_wired_into_init_all`
recurrence_guard passes). The empty installer is NOT a finding because there are
zero owned seam declarations outstanding.

**Outward seams** (all thin marshal + delegate, justified by real dependency
cycles — the dispatch crates depend on this crate, so this crate cannot depend
back on them):
- execProcnode-seams: `exec_proc_node`, `exec_init_node`, `exec_end_node`,
  `exec_set_tuple_bound`.
- execTuples-seams: `exec_copy_slot`, `exec_init_result_type_tl`,
  `exec_get_result_type`, `exec_get_result_slot_ops`, `exec_init_extra_tuple_slot`.
- execUtils-seams: `exec_assign_expr_context`, `exec_get_result_slot_ops_isfixed`
  (NEW — added to execUtils-seams, installed by execUtils::init_seams; carries
  the full ExecGetResultSlotOps isfixed semantics so the logic stays in execUtils
  rather than being duplicated here).
- execAmi-seams: `exec_re_scan`.
- execExpr-seams: `exec_init_expr`, `exec_eval_expr_switch_context`,
  `exec_qual_and_reset`.
- execGrouping-seams: `exec_tuples_match_prepare`.
- tcop-postgres-seams: `check_for_interrupts`.

No branching/construction/computation occurs on any outward seam path here; the
WITH-TIES equality primitives panic until execGrouping/execExprInterp install
real implementations (correct mirror-pg-and-panic for unported callees — the
crate's OWN logic is complete).

**Dispatch wiring** (this crate's interface routines are reached by direct call
from the dispatch crates — acyclic, since nodeLimit depends only on `*-seams`):
- `execProcnode_init.rs` T_Limit arm: builds `PlanStateNode::Limit` from
  `ExecInitLimit`.
- `execProcnode_run_end.rs` T_LimitState arm: `ExecEndLimit`.
- `execAmi/src/lib.rs` ExecReScan T_LimitState arm: `ExecReScanLimit`.
- `ExecSetTupleBound`: no Limit arm needed — Limit is not a bound-propagation
  target in C (only Sort/IncrementalSort/Append/MergeAppend/Result/SubqueryScan/
  Gather/GatherMerge), so the C fall-through (no-op) is the `_ =>` arm.

## Design-conformance pass

- No invented opacity: `subSlot`/`last_slot` are `Option<SlotId>` (the established
  stage-2 slot model), not new handles. All node/expr types are the genuine
  `types-nodes` structs.
- Allocating paths (`alloc_in`, `clone_in`) are `Mcx`+`PgResult` fallible.
- No shared statics / ambient-global seams / locks across `?`.
- FATAL/PANIC C sites: the `elog(ERROR, "LIMIT subplan failed to run backwards")`
  paths map to `PgError` (internal-error sqlstate), not a Rust panic; the
  `castNode`/uninitialized-invariant panics mirror C `Assert`/`castNode`.
- No `for now`/`simplified`/`TODO`/`hack` markers.

## Verdict: **PASS**

All 6 functions MATCH; all delegated calls are justified SEAMED; zero seam
findings; zero design findings. Constants verified against headers.
