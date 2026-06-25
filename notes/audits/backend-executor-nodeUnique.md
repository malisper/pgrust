# Audit: backend-executor-nodeUnique

- **Unit:** `backend-executor-nodeUnique`
- **Branch:** `port/backend-executor-nodeUnique`
- **C source:** `src/backend/executor/nodeUnique.c` (PostgreSQL 18.3)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-executor-nodeUnique/src/nodeUnique.rs`
- **Rust port:** `crates/backend-executor-nodeUnique/src/lib.rs`
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

## Function inventory and verdicts

The C file defines 4 externally-visible / static functions plus the
`executor.h` static-inline helpers it inlines (`TupIsNull`, `ExecQualAndReset`).

| C function | C location | Port location | Verdict | Notes |
|---|---|---|---|---|
| `ExecUnique` (static, ExecProcNode cb) | nodeUnique.c:45 | lib.rs `ExecUnique` + `exec_unique_node` | MATCH | CHECK_FOR_INTERRUPTS; loop: ExecProcNode(outer)→TupIsNull→clear result + return NULL; TupIsNull(result)→break (first tuple); set ecxt_innertuple=slot / ecxt_outertuple=result; `!ExecQualAndReset`→break; final ExecCopySlot(result,slot). Returns `Option<SlotId>` = result slot / None. |
| `ExecInitUnique` | nodeUnique.c:113 | lib.rs `ExecInitUnique` | MATCH | Assert(!(eflags & (BACKWARD|MARK))) as debug_assert; makeNode + ps.plan/ExecProcNode wire; ExecAssignExprContext; ExecInitNode(outerPlan); ExecInitResultTupleSlotTL(MinimalTuple); ps_ProjInfo=None; eqfunction=execTuplesMatchPrepare(ExecGetResultType(outer), numCols, uniqColIdx, uniqOperators, uniqCollations, &ps). |
| `ExecEndUnique` | nodeUnique.c:166 | lib.rs `ExecEndUnique` | MATCH | ExecEndNode(outerPlanState); guarded `if let Some` since C ExecEndNode(NULL) is a no-op. |
| `ExecReScanUnique` | nodeUnique.c:179 | lib.rs `ExecReScanUnique` | MATCH | ExecClearTuple(ps_ResultTupleSlot); if outerPlan->chgParam==NULL → ExecReScan(outerPlan). C dereferences outerPlan unconditionally; `.expect()` matches that never-NULL invariant. |
| `TupIsNull` (executor.h inline) | tuptable.h | `fetch_outer_tuple` / `.is_empty()` checks | MATCH | NULL-pointer-OR-TTS_EMPTY → `Option<SlotId>` None / `TupleTableSlot::is_empty`. |
| `ExecQualAndReset` (executor.h inline) | executor.h | lib.rs `exec_qual_and_reset` | MATCH | ExecQual(state,ec) then ResetExprContext. NULL eqfunction (numCols==0 → execTuplesMatchPrepare None) → ExecQual true, reset still runs (local `ecxt_per_tuple_memory.reset()`); non-NULL → `execExpr::exec_qual_and_reset` (qual + reset in the owner). |

## Constants / NodeTags

- `T_Unique = 367`, `T_UniqueState = 431` — verified against
  `../pgrust/postgres-18.3/src/backend/nodes/nodetags.h` (`T_Unique = 367`,
  `T_UniqueState = 431`).
- Result slot ops `&TTSOpsMinimalTuple` → `TupleSlotKind::MinimalTuple`
  (vs nodeResult's `Virtual`) — matches the C.
- `EXEC_FLAG_BACKWARD`/`EXEC_FLAG_MARK` taken from `types_nodes::executor`.

## Seam audit

Ownership by C-source coverage: `nodeUnique.c` is a leaf executor node and
owns NO `*-seams` crate (same as nodeLimit/nodeResult). `init_seams()` is empty
and is wired into `seams-init::init_all()` — correct (no owned seam crate ⇒
nothing to install; guard's owned-seam-installed check is vacuously satisfied).

Outward seam calls (each a real cross-crate cycle partner, thin marshal +
delegate, panic until owner lands):
- execProcnode-seams: `exec_proc_node`, `exec_init_node`, `exec_end_node`
- execTuples-seams: `exec_clear_tuple`, `exec_copy_slot`,
  `exec_init_result_tuple_slot_tl`, `exec_get_result_type`
- execUtils-seams: `exec_assign_expr_context`
- execAmi-seams: `exec_re_scan`
- execExpr-seams: `exec_qual_and_reset`
- execGrouping-seams: `exec_tuples_match_prepare`
- backend-tcop-postgres-seams: `check_for_interrupts`

No branching/node-construction/computation inside any seam-call path; all
node-layer logic (the dup-skip loop, the ExecQualAndReset inline, the
descriptor derivation) lives in this crate.

## Design conformance

- No `todo!()`/`unimplemented!()` (grep clean).
- No stand-in type aliases for typed C pointers (Unique/UniqueState are real
  structs; eqfunction is `PgBox<ExprState>`, slots are `SlotId`).
- No infallible alloc on palloc paths; the one allocation (TupleDesc clone for
  execTuplesMatchPrepare) goes through fallible `alloc_in`/`clone_in` + `?`.
- No shared statics / ambient-global seams / locks-across-`?`.

## Verdict

**PASS** — all functions MATCH; zero seam findings; `cargo check --workspace`
clean.

## Independent re-audit (2026-06-13)

Re-derived function-by-function from the three sources without trusting the
prior verdict. Confirmations:

- c2rust defines exactly 5 fns (`ExecQualAndReset`, `ExecUnique`,
  `ExecInitUnique`, `ExecEndUnique`, `ExecReScanUnique` — plus the `ExecQual`/
  `ExecEvalExprSwitchContext`/`ExecProcNode` inlines pulled in from the
  headers); every one has a port counterpart. No MISSING/PARTIAL/DIVERGES.
- Control flow of the `ExecUnique` dup-skip loop matches the c2rust rendering
  branch-for-branch: CHECK_FOR_INTERRUPTS → ExecProcNode(outer) → TupIsNull
  (null OR TTS_FLAG_EMPTY) clear+return None → TupIsNull(result) break →
  set innertuple=slot/outertuple=result → `!ExecQualAndReset` break →
  ExecCopySlot(result, slot).
- `ExecQualAndReset` NULL-state leg: ExecQual(NULL)→true, MemoryContextReset
  still runs (port resets `ecxt_per_tuple_memory` locally). Matches.
- Constants re-checked against `nodetags.h` (T_Unique=367, T_UniqueState=431)
  and `executor.h` (EXEC_FLAG_BACKWARD=0x0008, EXEC_FLAG_MARK=0x0010); Unique /
  UniqueStateData struct field layout matches plannodes.h / execnodes.h.
- No owned `*-seams` crate (leaf node); empty `init_seams()` wired into
  `seams-init::init_all()`; both `recurrence_guard` checks pass.
- Gates: `cargo check --workspace` clean (only pre-existing unrelated
  printtup warnings); `cargo test -p backend-executor-nodeUnique` and
  `cargo test -p seams-init` pass.

Verdict unchanged: **PASS**.
