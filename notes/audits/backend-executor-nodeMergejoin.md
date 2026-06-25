# Audit: backend-executor-nodeMergejoin

Unit: `backend-executor-nodeMergejoin` (+ `types-sortsupport`,
`backend-utils-sort-sortsupport-seams`)
C source: `src/backend/executor/nodeMergejoin.c`
c2rust: `c2rust-runs/backend-executor-nodeMergejoin/src/nodeMergejoin.rs`
Port: `crates/backend-executor-nodeMergejoin/src/lib.rs`

## Function inventory and verdicts

| C function / construct | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `MarkInnerTuple` (macro → `ExecCopySlot`) | 151 | `MarkInnerTuple` 431 | MATCH | thin copy of inner→marked slot via execTuples seam |
| `MJExamineQuals` | 175 | `MJExamineQuals` 133 | MATCH | OpExpr check, ExecInitExpr x2, ssup setup, get_op_opfamily_properties, IndexAmTranslateStrategy != COMPARE_EQ check, BTSORTSUPPORT_PROC then BTORDER_PROC fallback + shim. All elog messages + format strings preserved. |
| `MJEvalOuterValues` | 288 | `MJEvalOuterValues` 239 | MATCH | end-of-input, reset OuterEContext, per-clause eval, i==0 nulls-first FillOuter early-end logic identical |
| `MJEvalInnerValues` | 335 | `MJEvalInnerValues` 291 | MATCH | Live/Marked slot selection via `InnerSlot`; rest identical to outer variant |
| `MJCompare` | 386 | `MJCompare` 389 | MATCH | NULL-vs-NULL skip, ApplySortComparator, nulleqnull/ConstFalseJoin → result=1 |
| `ApplySortComparator` (inline, sortsupport.h) | sortsupport.h:200 | `ApplySortComparator` 348 | MATCH | null/reverse arithmetic inlined exactly; comparator call via seam; INVERT on reverse |
| `INVERT_COMPARE_RESULT` (macro) | sortsupport.h | `invert_compare_result` 108 | MATCH | `<0 ? 1 : -var` with wrapping_neg (matches C INT_MIN behavior) |
| `TupIsNull` (macro) | tuptable.h | `tup_is_null` 98 | MATCH | None or TTS_EMPTY |
| `MJFillOuter` | 447 | `MJFillOuter` 451 | MATCH | reset ps_ExprContext, set outer=live/inner=null slots, ExecQual(otherqual), ExecProject, InstrCountFiltered2 |
| `MJFillInner` | 478 | `MJFillInner` 485 | MATCH | mirror of MJFillOuter |
| `check_constant_qual` | 514 | `check_constant_qual` 561 | MATCH | NIL→true, non-Const→false, null/false const sets is_const_false |
| `ExecMergeTupleDumpOuter/Inner/Marked/Dump` | 541-587 | — | n/a | `#ifdef EXEC_MERGEJOINDEBUG` only; not in build config; correctly omitted |
| `ExecMergeJoin` | 594 | `ExecMergeJoin` 588 | MATCH (after fix) | full EXEC_MJ_* state machine; see fix below |
| `ExecInitMergeJoin` | 1439 | `ExecInitMergeJoin` 1150 | MATCH | econtexts, child init, eflags|EXEC_FLAG_MARK gating, Material ExtraMarks, result slot/proj, marked slot, qual/joinqual init, single_match, per-jointype null slots + check_constant_qual, MJExamineQuals, state reset |
| `ExecEndMergeJoin` | 1636 | `ExecEndMergeJoin` 1389 | MATCH | ExecEndNode inner then outer |
| `ExecReScanMergeJoin` | 1652 | `ExecReScanMergeJoin` 1407 | MATCH | clear marked slot, state reset, chgParam==NULL → ExecReScan each child |

## Fix applied (round 1)

**EXEC_MJ_JOINTUPLES — econtext slot links set unconditionally (FIXED).**
C lines 781-784 set `econtext->ecxt_outertuple`/`ecxt_innertuple` from the
current outer/inner slots *unconditionally* at the top of the JOINTUPLES arm,
before evaluating the joinqual, the otherqual, *or* the projection. The original
port set those links only inside `exec_joinqual`, which is skipped when the node
has no joinqual (`joinqual == NULL`). In that case both `ExecQual(otherqual)` and
`ExecProject` ran against stale econtext links left over from a previous tuple
cycle, producing wrong otherqual results and a wrong projected tuple for any
mergejoin without an extra joinqual (the common case). Fixed by setting the
links unconditionally in the JOINTUPLES arm and removing the link assignment from
`exec_joinqual` (whose doc now notes the caller sets them), exactly mirroring the
C order.

## Constants verified against headers

- `BTORDER_PROC = 1`, `BTSORTSUPPORT_PROC = 2` (access/nbtree.h:717-718) — match `types-sortsupport`.
- `COMPARE_EQ = 3` (access/cmptype.h:36) — matches.
- `EXEC_MJ_*` 1..11 (nodeMergejoin.c:105-115) — match `types-nodes::nodemergejoin`.
- `JOIN_INNER=0 LEFT=1 FULL=2 RIGHT=3 SEMI=4 ANTI=5 RIGHT_SEMI=6 RIGHT_ANTI=7` (nodes.h:299-316) — match `types-nodes::jointype`.

## Seam audit

- `backend-utils-sort-sortsupport-seams`: `oid_function_call_1_sortsupport`,
  `prepare_sort_support_comparison_shim`, `apply_sort_comparator` — all thin
  marshal+delegate to the unported `utils/sort/sortsupport.c`; no branching or
  computation in the seam path. Owner unported, so uninstalled (calls panic) —
  acceptable per skill (unported callee).
- Outward calls to execProcnode/execAmi/execExpr/execUtils/execTuples/
  lsyscache/amapi/tcop-postgres are all separate units with genuine
  dependency relationships; each seam path is argument-convert + one call +
  result-convert.
- nodeMergejoin's own `init_seams()` is empty and wired into
  `seams-init::init_all()` (seams-init/src/lib.rs:21). Correct: the crate has no
  `<unit>-seams` crate because it is reached through execProcnode dispatch, which
  can depend on it directly without a cycle.
- No crate logic was replaced by a seam call to "somewhere else"; the entire
  state machine, comparison, fill, mark/restore, and const-qual classifier live
  in this crate.

## Design conformance

- Allocating functions (`MJExamineQuals`, `ExecInitMergeJoin`) take `Mcx` and
  return `PgResult`. No shared statics for per-backend state (mergestate is
  owned). No ambient-global seams, no locks across `?`, no registry side tables.
  `types-sortsupport` carries only the fields merge join reads; the comparator is
  a `Copy` token (`SortComparatorId`) interpreted by the owner — no invented
  opacity. No unledgered divergence markers.

## Verdict: PASS

All functions MATCH (debug-only dump helpers correctly omitted); one logic
divergence found and fixed (JOINTUPLES econtext links); constants verified
against headers; seams justified and thin; build + tests green.
