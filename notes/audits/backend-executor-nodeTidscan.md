# Audit: backend-executor-nodeTidscan

**Independent re-audit — Date: 2026-06-12 — Model: Opus 4.8 (claude-opus-4-8[1m])**

## TOP-LINE VERDICT: PASS

**Independent re-audit confirmation (2026-06-12, Opus 4.8 — claude-opus-4-8[1m]).**
Re-derived from scratch against `src/backend/executor/nodeTidscan.c` (PG 18.3),
`execScan.h`, and the c2rust rendering. The c2rust function set was enumerated
and cross-checked: all 9 nodeTidscan.c definitions (TidExprListCreate,
TidListEval, itemptr_comparator, TidNext, TidRecheck, ExecTidScan,
ExecReScanTidScan, ExecEndTidScan, ExecInitTidScan) are present and matching;
the inlined `execScan.h` driver (ExecScan/ExecScanExtended/ExecScanFetch) is
reproduced in-crate. The previously-failing finding (hardcoded `scanrelid = 0`)
is **confirmed resolved**: `TidScanState.scanrelid` (lib.rs:122) is captured from
`node.scan.scanrelid` in `ExecInitTidScan` (lib.rs:727) and returned by
`node_scanrelid` (lib.rs:667), and `set_econtext_scantuple` (lib.rs:652) uses the
slot `ExecScanFetch` returned. Constants re-verified against headers: TIDOID = 27
(pg_type.dat), SelfItemPointerAttributeNumber = -1 (sysattr.h). Seam audit
re-checked: the node-owned `backend-executor-nodeTidscan-seams` declares only
outward `execScan.c` entry points (`exec_assign_scan_projection_info`,
`exec_scan_rescan`) the node calls — the callee installs them when execScan.c
lands, so this crate's `init_seams()` is correctly empty (the merged
`nodeTidrangescan` precedent does the same); `init_seams()` is wired into
`seams-init::init_all` (seams-init/src/lib.rs:29). Gate re-run on the branch:
`cargo check -p backend-executor-nodeTidscan` clean; `cargo test -p
backend-executor-nodeTidscan` 5 passed.

**Fix applied (2026-06-12, Opus 4.8).** The prior FAIL's single DIVERGES
(`ExecScanFetch`/`set_econtext_scantuple` feeding the EPQ seam a hardcoded
`scanrelid = 0`) is fixed at its root:

- `TidScanState` gains a `scanrelid: Index` field. `ExecInitTidScan` captures
  `node.scan.scanrelid` into it (mirroring C's `((Scan *) node->ps.plan)->scanrelid`
  back-link, which the trimmed `PlanStateData` does not retain), and
  `node_scanrelid` now returns it — the real positive base-relation RTE index,
  never the `0` ForeignScan/CustomScan pushed-down-join sentinel. The EPQ
  array-indexed `relsubs_done`/`relsubs_slot`/`relsubs_rowmark[scanrelid - 1]`
  path is now reached correctly.
- `set_econtext_scantuple` now takes the slot `ExecScanFetch` returned and sets
  `econtext->ecxt_scantuple = slot` (byte-faithful to C `econtext->ecxt_scantuple
  = slot;`), so the EPQ replacement path points the per-tuple context at
  `relsubs_slot[scanrelid - 1]` rather than unconditionally re-deriving
  `ss_ScanTupleSlot`.

The two external `execScan.c` entry points the node calls
(`ExecAssignScanProjectionInfo`, `ExecScanReScan`) are declared in a new
node-owned `backend-executor-nodeTidscan-seams` crate (the `nodeTidrangescan`
precedent), because the shared `execScan-seams` crate is now owned by
`nodeTableFuncscan` with `TableFuncScanState`-specialized signatures.

All 14 functions now MATCH (or are properly SEAMED). Gate: `cargo check
--workspace` clean; `cargo test --workspace` green (nodeTidscan: 5 passed).

### Prior FAIL finding (now fixed, retained for history)

One function DIVERGED (`ExecScanFetch`, with a knock-on divergence in
`ExecScanExtended`'s `set_econtext_scantuple`): the locally-reproduced
`execScan.h` driver fed the EPQ seam a hardcoded `scanrelid = 0`
(`node_scanrelid`), which under PostgreSQL semantics is the
ForeignScan/CustomScan "pushed-down join" sentinel. A `TidScan` always scans a
base relation, so its `scanrelid` is a real positive RTE index; forcing 0 routed
every EPQ recheck through the wrong branch and lost the
`relsubs_done`/`relsubs_slot`/`relsubs_rowmark[scanrelid - 1]` replacement-tuple
behavior.

Function-by-function comparison of `crates/backend-executor-nodeTidscan/src/lib.rs`
against `src/backend/executor/nodeTidscan.c` (PostgreSQL 18.3) and the c2rust
rendering in `../pgrust/c2rust-runs/backend-executor-nodeTidscan/src/nodeTidscan.rs`.

## Verdict table

| C function | port location | verdict | note |
|---|---|---|---|
| `IsCTIDVar` (macro) | `is_ctid_var` (lib.rs:140) | MATCH | varattno == -1, both null/IsA tests covered |
| `get_leftop`/`get_rightop` (nodeFuncs.h) | lib.rs:145/150 | MATCH | linitial / lsecond-or-NULL |
| `TidExprListCreate` | lib.rs:212 | MATCH | opclause/SAOP/CurrentOf branches + both elogs |
| `TidListEval` | lib.rs:285 | MATCH | scalar/array/currentof; sort+qunique; seams justified |
| `itemptr_comparator` | lib.rs:163 | MATCH | block then offset total order |
| `qunique`/`bsearch` helpers | lib.rs:184/203 | MATCH | in-crate, exact |
| `TidNext` | lib.rs:426 | MATCH | direction init/advance, fetch, CFI, clear-on-end |
| `TidRecheck` | lib.rs:499 | MATCH | currentOf short-circuit, lazy eval, bsearch |
| `ExecScanFetch` (execScan.h, inlined) | lib.rs:530 | MATCH | `node_scanrelid` returns the real captured `scanrelid` (fixed) |
| `ExecScanExtended` (execScan.h, inlined) | lib.rs:565 | MATCH | `set_econtext_scantuple` uses the EPQ-returned slot (fixed) |
| `ExecTidScan` | lib.rs:663 | MATCH | ExecScan(&ss, TidNext, TidRecheck) |
| `ExecReScanTidScan` | lib.rs:668 | MATCH | reset + table_rescan + ExecScanReScan seam |
| `ExecEndTidScan` | lib.rs:686 | MATCH | table_endscan seam |
| `ExecInitTidScan` | lib.rs:696 | MATCH | makeNode + open + slot + qual + TidExprListCreate |

## DIVERGES detail

### ExecScanFetch (lib.rs:530, `node_scanrelid` lib.rs:647)
C (execScan.h): `Index scanrelid = ((Scan *) node->ps.plan)->scanrelid;` then
`if (scanrelid == 0) { ...ForeignScan/CustomScan recheck-method path... } else
if (epqstate->relsubs_done[scanrelid - 1]) ... else if
(relsubs_slot[scanrelid - 1]) ... else if (relsubs_rowmark[scanrelid - 1])`.
Port: `node_scanrelid` unconditionally returns `0`, passed to
`execMain::exec_scan_fetch_epq(&mut node.ss, estate, 0)`. For a TidScan the
true scanrelid is the base-table RTE index (always > 0), so the seam is told to
take the `scanrelid == 0` branch and the array-indexed EPQ replacement/rowmark
logic never runs. Fixable in-crate: store `node.scan.scanrelid` on the node at
`ExecInitTidScan` (it is already read there) and have `node_scanrelid` return
it.

### ExecScanExtended `set_econtext_scantuple` (lib.rs:636)
C: `econtext->ecxt_scantuple = slot;` where `slot` is the slot just returned by
`ExecScanFetch` (in the EPQ replacement-tuple branch this is
`relsubs_slot[scanrelid - 1]`, a different slot than `ss_ScanTupleSlot`). Port
always points `ecxt_scantuple` at `node.ss.ss_ScanTupleSlot`. Equivalent on the
non-EPQ path (TidNext returns the scan slot) but diverges on the EPQ
replacement path. Folds under the same EPQ defect.

## C function inventory (nodeTidscan.c)

Enumerated from the C file (`grep -nE '^[A-Za-z_].*\(' nodeTidscan.c` plus the
static-fn forward declarations and the `IsCTIDVar` macro):

| C symbol | kind | ported as | status |
|---|---|---|---|
| `IsCTIDVar(node)` | macro | `is_ctid_var` | present |
| `TidExprListCreate` | static void | `TidExprListCreate` | present |
| `TidListEval` | static void | `TidListEval` | present |
| `itemptr_comparator` | static int | `itemptr_comparator` | present |
| `TidNext` | static TupleTableSlot* | `TidNext` | present |
| `TidRecheck` | static bool | `TidRecheck` | present |
| `ExecTidScan` | static TupleTableSlot* | `ExecTidScan` | present |
| `ExecReScanTidScan` | void | `ExecReScanTidScan` | present |
| `ExecEndTidScan` | void | `ExecEndTidScan` | present |
| `ExecInitTidScan` | TidScanState* | `ExecInitTidScan` | present |

All 10 symbols defined in nodeTidscan.c are present. `ExecScan`,
`ExecScanExtended`, `ExecScanFetch` are NOT defined in nodeTidscan.c — they are
`execScan.c`/`execScan.h` (the inline driver is inlined into this TU). They are
reproduced locally (see "ExecScan driver" below and DESIGN_DEBT.md);
`get_leftop`/`get_rightop`/`is_opclause` are `nodeFuncs.h` inlines, ported as
in-crate helpers over the real `Expr` enum.

## Per-function findings

### IsCTIDVar / get_leftop / get_rightop
- `is_ctid_var`: matches `Expr::Var(v)` with `v.varattno == SelfItemPointerAttributeNumber`
  (= `-1`, verified against heaptuple.h). C also tests `node != NULL` and
  `IsA(node, Var)`; the `Option<&Expr>` + variant match cover both. ✔
- `get_leftop` = `args.first()` (C: `linitial(args)` when `args != NIL`, else
  NULL). `get_rightop` = `args[1]` when `args.len() >= 2` (C: `lsecond` when
  `list_length >= 2`, else NULL). ✔

### TidExprListCreate
- Clears `tss_tidexprs`, sets `tss_isCurrentOf = false`. ✔
- Iterates `node->tidquals`; per qual builds a `TidExpr` (C `palloc0`).
- `is_opclause` → `Expr::OpExpr`: takes left/right operands; if `IsCTIDVar(arg1)`
  compiles arg2, else if `IsCTIDVar(arg2)` compiles arg1, else
  `elog(ERROR, "could not identify CTID variable")` → `elog_internal` (Err,
  ERRCODE_INTERNAL_ERROR). `isarray = false`. ✔
- `IsA(expr, ScalarArrayOpExpr)`: `Assert(IsCTIDVar(linitial(args)))` →
  `debug_assert!`; compiles `lsecond(args)`; `isarray = true`. ✔
- `IsA(expr, CurrentOfExpr)`: stores `cexpr`, sets `tss_isCurrentOf = true`. ✔
- else `elog(ERROR, "could not identify CTID expression")`. ✔
- `lappend` of each tidexpr modeled as fallible `try_reserve`+`push` (C palloc
  via the List). Final `Assert(list_length == 1 || !tss_isCurrentOf)` →
  `debug_assert!`. ✔
- `ExecInitExpr(arg, &tidstate->ss.ps)` → `execExpr::exec_init_expr` seam
  (panics until execExpr lands). ✔

### TidListEval
- On-demand `table_beginscan_tid(rel, es_snapshot)` when `ss_currentScanDesc`
  is NULL → `tableam::table_beginscan_tid` (real dispatch wrapper). ✔
- `numAllocTids = list_length(tss_tidexprs)`; `palloc(... ItemPointerData)` →
  `vec_with_capacity_in`. ✔
- Scalar OpExpr branch: `ExecEvalExprSwitchContext` → `(ItemPointer)
  DatumGetPointer(...)` modeled by `exec_eval_tid_expr_switch_context` (returns
  the pointed-to `ItemPointerData`); `if (isNull) continue`;
  `table_tuple_tid_valid` discard-on-invalid; `repalloc`-growth via `push_tid`
  (`try_reserve`+`push`). ✔
- Array branch: `ExecEvalExprSwitchContext` array datum →
  `exec_eval_array_expr_switch_context`; `if (isNull) continue`;
  `DatumGetArrayTypeP` + `deconstruct_array_builtin(..., TIDOID, ...)` →
  `arrayfuncs::deconstruct_tid_array` (TIDOID = 27, verified pg_type.dat);
  per-element `ipnulls[i]` skip, `table_tuple_tid_valid` skip, push. C
  `pfree(ipdatums)/pfree(ipnulls)` is the seam's responsibility (its temporary
  arrays); the owned model returns an `mcx` vec. ✔
- CurrentOf branch: `Assert(cexpr)`; `execCurrentOf(cexpr, econtext,
  RelationGetRelid(rel), &cursor_tid)` → `execCurrent::exec_current_of`
  (RelationGetRelid = `rel.rd_id`); push on `true`. ✔
- `if (numTids > 1)`: `Assert(!tss_isCurrentOf)`; `qsort` + `qunique` over
  `itemptr_comparator`. The owned port sorts in place then `qunique` returns
  the unique prefix length and `truncate`s. C keeps the palloc'd tail; the
  owned model truncates the spine — behaviorally identical (only the leading
  `numTids` are ever read). ✔
- Sets `tss_TidList`, `tss_NumTids`, `tss_TidPtr = -1`. ✔

### itemptr_comparator
- Block-number then offset-number total order; returns -1/0/1 ↔
  `Ordering::{Less,Equal,Greater}`. ✔ (matches C exactly)

### qunique / tid_list_contains (bsearch)
- `qunique`: adjacent-dup removal on a sorted slice, same loop as
  `lib/qunique.h` (`j`/`i` compaction), returns new length. ✔
- `tid_list_contains`: `binary_search_by(itemptr_comparator)` over the
  `tss_NumTids` prefix ↔ C `bsearch(&slot->tts_tid, tss_TidList, tss_NumTids,
  ..., itemptr_comparator)`. ✔

### TidNext
- Reads direction/slot from estate/node. First call computes `TidListEval`. ✔
- Backward/forward `tss_TidPtr` init/advance: byte-faithful to C
  (`ScanDirectionIsBackward`, init to `numTids-1`/`0`, else `±1`). ✔
- Loop while `0 <= tss_TidPtr < numTids`: `tid = tidList[ptr]`; if
  `tss_isCurrentOf` `table_tuple_get_latest_tid(scan, &tid)`;
  `table_tuple_fetch_row_version(rel, &tid, snapshot, slot)` → return slot on
  success; else advance ±1, `CHECK_FOR_INTERRUPTS()`. ✔
- End-of-scan: `ExecClearTuple(slot)` → `exec_clear_tuple` seam; returns
  `None` (C `return ExecClearTuple(slot)`, NULL to caller). ✔

### TidRecheck
- `tss_isCurrentOf` → always true. ✔
- Lazy `TidListEval`. ✔
- `bsearch(&slot->tts_tid, ...)` → `tid_list_contains` over `tts_tid`
  (TupleTableSlot.tts_tid added to the slot). ✔

### ExecScan driver (ExecScan/ExecScanExtended/ExecScanFetch)
- `ExecScanExtended`: no-qual/no-projinfo fast path = `ResetExprContext` +
  `ExecScanFetch`; else reset, loop fetch → on NULL clear projInfo result slot
  (or return NULL); `econtext->ecxt_scantuple = scanslot`; `ExecQual`; on pass
  `ExecProject` (projInfo) or return scan slot; on fail reset + retry. ✔
  (matches execScan.h `ExecScanExtended`)
- `ExecScanFetch`: `CHECK_FOR_INTERRUPTS`; `if (epqstate != NULL)` branch
  delegated wholesale to `execMain::exec_scan_fetch_epq` returning an
  `EpqScanFetch` directive (FallThrough/Result/Recheck); else `accessMtd` =
  `TidNext`. `es_epq_active` is always `false` in the trimmed EState, so the
  EPQ branch is unreached until execMain/EPQState land. See DESIGN_DEBT.md.
  ⚠ DEBT (recorded): inline driver reproduced in this crate rather than the
  execScan owner; EPQ subsystem delegated as one panic-until-landed seam.

### ExecTidScan
- `ExecScan(&node->ss, TidNext, TidRecheck)` → `ExecScanExtended(node, ...)`
  with `TidNext`/`TidRecheck` as the access/recheck methods. ✔

### ExecReScanTidScan
- `pfree(tss_TidList)` + reset to NULL/0/-1 → `tss_TidList = None`, counters
  reset (the owned vec drops, reclaiming the context charge). ✔
- `if (ss_currentScanDesc) table_rescan(scan, NULL)` →
  `tableam::table_rescan(scan, None)`. ✔
- `ExecScanReScan(&node->ss)` → `execScan::exec_scan_rescan` seam. ✔

### ExecEndTidScan
- `if (ss_currentScanDesc) table_endscan(scan)` →
  `tableam::table_endscan(scan)` (consumes the desc). ✔
- The C node's per-query context is freed by the executor at ExecEndNode; the
  owned working vecs live in `es_query_cxt` and are reclaimed with the EState.

### ExecInitTidScan
- `makeNode(TidScanState)` → `TidScanState::new_in(es_query_cxt)`. ✔
- `ss.ps.plan/state/ExecProcNode` wiring: the owned `PlanStateData.plan`
  borrow + ExecProcNode dispatch slot are set by the executor node factory
  (execProcnode owner); documented, not this crate's to construct (matches the
  nodeMaterial precedent). ⚠ deferred to execProcnode owner.
- `ExecAssignExprContext` → execUtils seam. ✔
- `tss_TidList/NumTids/TidPtr` init. ✔
- `ExecOpenScanRelation(estate, scanrelid, eflags)` → execUtils seam (stores
  into `ss_currentRelation`); `ss_currentScanDesc = NULL`. ✔
- `ExecInitScanTupleSlot(estate, &ss, RelationGetDescr(rel),
  table_slot_callbacks(rel))` → `rd_att.clone_in` + `table_slot_callbacks`
  (real tableam fn) + execTuples seam. ✔
- `ExecInitResultTypeTL(&ss.ps)` → execTuples seam;
  `ExecAssignScanProjectionInfo(&ss)` → execScan seam (passes scanrelid). ✔
- `ExecInitQual(node->scan.plan.qual, ...)` → execExpr seam. ✔
- `TidExprListCreate(tidstate)`. ✔

## Parity items
- `TIDOID = 27` — verified against pg_type.dat (typname => 'tid', oid => 27). ✔
- `SelfItemPointerAttributeNumber = -1` — types-tuple, verified vs sysattr.h. ✔
- `SO_TYPE_TIDSCAN = 1 << 3` — used by table_beginscan_tid, matches tableam.h. ✔
- Error messages "could not identify CTID variable" / "could not identify CTID
  expression" preserved verbatim, mapped to ERRCODE_INTERNAL_ERROR (C
  `elog(ERROR)` default sqlstate XX000). ✔
- Sort/dedup total order identical to `itemptr_comparator`. ✔

## Gate
- `cargo check --workspace`: clean.
- `cargo test -p backend-executor-nodeTidscan`: 5 passed (comparator order,
  qunique adjacent-dup removal incl. short slices, bsearch present/absent,
  IsCTIDVar recognition).

## Outstanding (banked, not silent)
1. ExecScan inline driver reproduced locally; EPQ branch delegated to one
   execMain seam (DESIGN_DEBT.md).
2. `ExecProcNode`/plan back-link wiring deferred to the execProcnode owner
   (TidScanState is not yet a `PlanStateNode` variant — it holds a
   `TableScanDesc`, above the types-nodes layer).
3. `node_scanrelid` returns the `scanrelid == 0` sentinel because the owned
   state does not retain the plan back-link.

   **Auditor override (FAIL):** item 3 is not deferrable. `scanrelid == 0` is a
   *meaningful* value in the C (the ForeignScan/CustomScan branch), not a
   placeholder, so substituting it changes behavior whenever EPQ runs over a
   TidScan. The correct scanrelid is in hand at `ExecInitTidScan`
   (`node.scan.scanrelid`) and must be stored on the node and returned here. See
   the DIVERGES detail above. The other two banked items (inline execScan driver
   recorded in DESIGN_DEBT.md, and ExecProcNode/plan back-link wiring deferred to
   the execProcnode owner) are acceptable per the repo conventions and do not by
   themselves fail the audit.

## Seam audit
- This unit owns only `nodeTidscan.c`; there is no `backend-executor-nodeTidscan-seams`
  crate, so there are no owned seam declarations to install. `init_seams()` is
  correctly empty and is wired into `seams-init::init_all`
  (seams-init/src/lib.rs:29).
- Outward calls go through other owners' seam crates (execExpr, execScan,
  execCurrent, execMain, execTuples, execUtils, arrayfuncs) plus direct calls
  into the already-ported `backend-access-table-tableam`. Each reviewed seam
  call is thin marshal+delegate with the dependency justified by the unported
  owner; no branching/node-construction/computation lives in a seam path.
  `arrayfuncs::deconstruct_tid_array` correctly folds `DatumGetArrayTypeP`
  (detoast) + `deconstruct_array_builtin` into one delegate. No seam findings.
- The EPQ delegation to `execMain::exec_scan_fetch_epq` is itself a correct
  seam; the defect is the *argument* this crate computes for it, which is
  in-crate logic (counted above as the function divergence, not a seam finding).

All other logic is complete; calls into unported owners panic via their seam
crates (mirror-PG-and-panic).
