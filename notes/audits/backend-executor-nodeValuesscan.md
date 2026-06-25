# Audit: backend-executor-nodeValuesscan

Independent function-by-function audit of `src/backend/executor/nodeValuesscan.c`
against the C source, the c2rust rendering
(`../pgrust/c2rust-runs/backend-executor-nodeValuesscan/src/nodeValuesscan.rs`),
and the port (`crates/backend-executor-nodeValuesscan/src/lib.rs`).

Re-derived from sources; the port's own comments / prior audit were not trusted.

## Function inventory (nodeValuesscan.c)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ValuesNext` (static) | nodeValuesscan.c:46 | lib.rs:80 `ValuesNext` | MATCH | direction advance (fwd: `curr_idx<array_len ⇒ ++`; back: `curr_idx>=0 ⇒ --`), unconditional `ExecClearTuple(slot)`, in-range guard `0<=curr_idx<array_len`, `ReScanExprContext`, lazy `ExecInitExprList(exprlist, NULL)` only when `exprstatelist==NIL` (empty-cell), per-col `ExecEvalExpr` + `MakeExpandedObjectReadOnly(value, isnull, attr->attlen)`, `ExecStoreVirtualTuple`. attlen read via execTuples `exec_scan_slot_descriptor` seam (slot payload owned below this layer). `Assert(list_length==natts)` preserved as `debug_assert_eq!`. |
| `ValuesRecheck` (static) | nodeValuesscan.c:179 | lib.rs:245 `ValuesRecheck` | MATCH | `return true;` |
| `ExecValuesScan` (static) | nodeValuesscan.c:195 | lib.rs:300 `ExecValuesScan` (+ `exec_values_scan_node` dispatch wrapper) | MATCH | `castNode(ValuesScanState)` (panic on mismatch) then `ExecScan(&ss, ValuesNext, ValuesRecheck)`. |
| `ExecInitValuesScan` | nodeValuesscan.c:209 | lib.rs:319 `ExecInitValuesScan` | MATCH | both `Assert(outer/innerPlan==NULL)`; makeNode; plan/state/ExecProcNode set; two `ExecAssignExprContext` (rowcontext = first ps_ExprContext); `ExecTypeFromExprList(linitial(values_lists))`; `ExecInitScanTupleSlot(...,&TTSOpsVirtual)`; `ExecInitResultTypeTL`; `ExecAssignScanProjectionInfo`; `ExecInitQual(plan.qual)`; `curr_idx=-1`; `array_len=list_length`; palloc/palloc0 arrays; per-row SubPlan detect `es_subplanstates && contain_subplans` ⇒ save/clear `es_jit_flags=PGJIT_NONE`, `ExecInitExprList(exprs, &ss.ps)`, restore. eflags unused (matches C). |
| `ExecReScanValuesScan` | nodeValuesscan.c:327 | lib.rs:473 `ExecReScanValuesScan` | MATCH | `if ps_ResultTupleSlot ⇒ ExecClearTuple`; `ExecScanReScan(&ss)`; `curr_idx=-1`. |

## execScan.h / execScan.c driver (reproduced in-crate)

`ExecScan` is out-of-line in execScan.c; `ExecScanExtended`/`ExecScanFetch` are
`pg_attribute_always_inline` static-inlines in execScan.h that the compiler folds
into `nodeValuesscan.o`. execScan is a separate, unported unit; the driver is
reproduced in-crate (same model as nodeForeignscan) so the owned-tree callback
ABI can be driven without a generic function-pointer indirection. Leaf ops are
seamed to their owners. Verified line-by-line against the C:

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ExecScan` | execScan.c:46 | lib.rs:683 `ExecScan` | MATCH | reads `es_epq_active` / `ps.qual` / `ps_ProjInfo`, delegates to ExecScanExtended. Trivial field-read + delegate (acceptable in-crate reproduction of unported owner). |
| `ExecScanExtended` | execScan.h:159 | lib.rs:598 `ExecScanExtended` | MATCH | `!qual && !projInfo` fast path (ResetExprContext + ExecScanFetch); else ResetExprContext, loop: fetch ⇒ TupIsNull ⇒ (projInfo ⇒ clear resultslot / else NULL); set `ecxt_scantuple`; `qual==NULL || ExecQual`; pass ⇒ (projInfo ⇒ ExecProject / else slot); fail ⇒ ResetExprContext + retry. `InstrCountFiltered1` is instrumentation-only (no control-flow effect) — correctly omitted. |
| `ExecScanFetch` | execScan.h:31 | lib.rs:497 `ExecScanFetch` | MATCH | `CHECK_FOR_INTERRUPTS`; full EPQ decision tree faithfully reproduced: `scanrelid==0` ⇒ `bms_is_member(epqParam, extParam)` ⇒ recheck+clear+return slot; `relsubs_done[i]` ⇒ clear+return; `relsubs_slot[i]!=NULL` ⇒ `Assert(rowmark==NULL)`, mark done, TupIsNull⇒NULL, recheck; `relsubs_rowmark[i]!=NULL` ⇒ mark done, `EvalPlanQualFetchRowMark`, TupIsNull, recheck; else `accessMtd`. `scanrelid-1` indexing preserved. |

## Constants verified against headers

- `PGJIT_NONE = 0x00` — `types_execparallel::PGJIT_NONE`, matches `jit/jit.h`.
- `MakeExpandedObjectReadOnly(d,isnull,typlen)` macro: short-circuit
  `(isnull || typlen != -1) ? d : MakeExpandedObjectReadOnlyInternal(d)` —
  reproduced in-crate (lib.rs:266); the `typlen != -1` node-layer test stays
  here, only the varlena deref (`typlen==-1`, non-null) crosses the
  misc2/expandeddatum seam (`make_expanded_object_read_only_internal`).
  Verified against `utils/expandeddatum.h`.
- `T_ValuesScan=349`, `T_ValuesScanState=413` — carried in types-nodes (per
  CATALOG); the node-tag table is owned by nodes-core.

## Seam audit

Ownership by C-source coverage: nodeValuesscan.c has **no inward callers**
needing a seam (its entry points are reached directly by execProcnode dispatch,
like nodeTableFuncscan), so there is **no `backend-executor-nodeValuesscan-seams`
crate** and `init_seams()` is correctly empty. Confirmed `crates/` has no such
seam crate. `seams-init::init_all()` calls `init_seams()` (lib.rs:82).

All outward seam calls are thin marshal+delegate into real unported owners:

- execTuples: `exec_clear_tuple`, `exec_type_from_expr_list`,
  `exec_init_scan_tuple_slot`, `exec_init_result_type_tl`,
  `exec_scan_slot_descriptor`, `store_virtual_values`.
- execUtils: `exec_assign_expr_context`, `re_scan_expr_context`,
  `reset_per_tuple_expr_context` (resolves to `ps_ExprContext`, verified — not
  rowcontext, matching C `ResetExprContext(node->ps.ps_ExprContext)`).
- execExpr: `exec_init_qual`, `exec_init_expr_list`,
  `exec_init_expr_list_no_parent`, `exec_eval_expr_switch_context`, `exec_qual`,
  `exec_project`.
- execScan: `exec_assign_scan_projection_info`, `exec_scan_rescan_ss`.
- execMain: `eval_plan_qual_fetch_row_mark`.
- nodes-core: `bms_is_member`.
- tcop-postgres: `check_for_interrupts`.
- misc2/expandeddatum: `make_expanded_object_read_only_internal`.
- optimizer/clauses: `contain_subplans`.

No branching/node-construction/computation lives in a seam path; no function
body was hollowed out into a cross-seam delegate (the C logic of all 5 funcs +
the driver lives in this crate). New owner seams the port required are installed
by their owners (verified): execExpr `exec_init_expr_list_no_parent` (execExpr
lib.rs:83), execUtils `re_scan_expr_context` (execUtils lib.rs:164).

No `todo!()` / `unimplemented!()` / own-logic stubs. Unported callees panic via
their owners' seams (correct — absent *callee*, not absent logic).

## Gates

- `cargo check --workspace` — clean (warnings only, pre-existing, in unrelated
  crates).
- `cargo test -p backend-executor-nodeValuesscan` — ok (0 tests).
- `cargo test -p seams-init` — ok; both `recurrence_guard` tests pass
  (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`).

## Verdict: PASS

Every function MATCH; zero seam findings; init_seams wired; gates green.
CATALOG set to `audited`.
