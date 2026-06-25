# Audit: backend-executor-nodeGroup

C source: `src/backend/executor/nodeGroup.c` (PostgreSQL 18.3).
Port: `crates/backend-executor-nodeGroup/src/lib.rs`; types added to
`crates/types-nodes/src/nodegroup.rs` (`Group`, `GroupStateData`, `T_Group`,
`T_GroupState`) plus Node / PlanStateNode enum wiring.

## Function inventory

C functions (cross-checked against `c2rust-runs/backend-executor-nodeGroup/src/nodeGroup.rs`,
which has exactly these four):

| C function (loc) | Port (loc) | Verdict | Notes |
|---|---|---|---|
| `ExecGroup` (nodeGroup.c:35, static `ExecProcNode` cb) | `ExecGroup` (lib.rs) + `exec_group_node` dispatch cb | MATCH | See detail below |
| `ExecInitGroup` (nodeGroup.c:160) | `ExecInitGroup` (lib.rs) | MATCH | See detail below |
| `ExecEndGroup` (nodeGroup.c:215) | `ExecEndGroup` (lib.rs) | MATCH | `ExecEndNode(outerPlanState)`; guarded `if let Some` is a safe superset of C's unconditional `ExecEndNode(NULL)`-no-op |
| `ExecReScanGroup` (nodeGroup.c:221) | `ExecReScanGroup` (lib.rs) | MATCH | See detail below |

In-crate helpers (no C counterpart of their own; they inline C macros/inlines):
`instr_count_filtered1` = `InstrCountFiltered1` (execnodes.h macro);
`exec_qual_having` = `ExecQual(node->ss.ps.qual, econtext)` with the C
`state == NULL => true` short-circuit (executor.h:515-522) handled in-crate;
`copy_into_first` = `ExecCopySlot(firsttupleslot, outerslot)` marshalling the
two distinct estate slots through `slot_pair_mut` + the execTuples seam.

## Per-function detail

### ExecGroup — MATCH
- `CHECK_FOR_INTERRUPTS()` → `tcop_postgres::check_for_interrupts::call()?` (SEAMED, tcop owner unported).
- `if (node->grp_done) return NULL;` → `if node.grp_done { return Ok(None); }`.
- `econtext = node->ss.ps.ps_ExprContext` / `firsttupleslot = node->ss.ss_ScanTupleSlot` → read as ids; invariant `.expect` mirrors C's unconditional deref (set by init).
- First-time path `if (TupIsNull(firsttupleslot))`: `TupIsNull` = `slot == NULL || TTS_EMPTY` → `estate.slot(firsttupleslot).is_empty()` (slot always present post-init, so the NULL leg is the empty leg).
  - `ExecProcNode(outerPlanState(node))` + `TupIsNull(outerslot)` → seam call + `Some(id) if !is_empty()` match; EOF sets `grp_done=true; return None`.
  - `ExecCopySlot` → `copy_into_first`; `econtext->ecxt_outertuple = firsttupleslot` → `ecxt_mut(econtext).ecxt_outertuple = Some(firsttupleslot)`.
  - `if (ExecQual(...)) return ExecProject(...) else InstrCountFiltered1(node,1)` → matched, including the `1.0` delta.
- Outer `for(;;)` with inner `for(;;)` scan: per iteration fetch outer / EOF→grp_done+None; set `ecxt_innertuple=first`, `ecxt_outertuple=current`; `if (!ExecQualAndReset(node->eqfunction, econtext)) break;` → `exec_qual_and_reset` seam; break carries the boundary `outerslot`. Then `ExecCopySlot(first, outerslot)`, `ecxt_outertuple=first`, HAVING qual → Project or InstrCountFiltered1. Loop structure and branch order identical.
- Returns `Option<SlotId>`: `Some(slot)` = C `return ExecProject(...)`, `None` = C `return NULL`.

### ExecInitGroup — MATCH
Order matches C exactly:
1. `Assert(!(eflags & (EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK)))` → `debug_assert!`. Flag bits `0x0008`/`0x0010` verified vs executor.h (types-nodes::executor).
2. `makeNode(GroupState)` + plan/ExecProcNode install + `grp_done=false` → `alloc_in(mcx, GroupStateData::default())` in `es_query_cxt`; `plan = Some(node)`; `ExecProcNode = Some(exec_group_node)`.
3. `ExecAssignExprContext` (SEAMED execUtils).
4. `ExecInitNode(outerPlan(node))` (SEAMED execProcnode).
5. `tts_ops = ExecGetResultSlotOps(outerPlanState, NULL)` → single-arg `exec_get_result_slot_ops` seam (NULL-isfixed variant, correct: Group does not record resultopsfixed). `ExecCreateScanSlotFromOuterPlan(estate, &ss, tts_ops)` (SEAMED execUtils).
6. `ExecInitResultTupleSlotTL(&ss.ps, &TTSOpsVirtual)` → seam with `TupleSlotKind::Virtual`.
7. `ExecAssignProjectionInfo(&ss.ps, NULL)` → seam with `None` input desc.
8. `qual = ExecInitQual(node->plan.qual, ...)` (SEAMED execExpr).
9. `eqfunction = execTuplesMatchPrepare(ExecGetResultType(outerPlanState), numCols, grpColIdx, grpOperators, grpCollations, &ss.ps)` (SEAMED execGrouping). Descriptor `clone_in(mcx)` to satisfy the owned `TupleDesc<'mcx>` seam contract (behavior-preserving; same as nodeLimit).

`castNode(Group, node)` failure → `panic!` (mirrors C `castNode` assertion; planner/compiler bug, not a runtime error path).

### ExecReScanGroup — MATCH
`node->grp_done = false;` → matched. `ExecClearTuple(ss_ScanTupleSlot)` → `exec_clear_tuple` seam (guarded `if let Some`, superset of C). `if (outerPlan->chgParam == NULL) ExecReScan(outerPlan)` → `if outer.ps_head().chgParam.is_none() { exec_re_scan }`.

## Constants
- `T_Group = 364`, `T_GroupState = 428`: verified against the c2rust rendering (`nodeGroup.rs:226` / `:162`). Sibling tags `T_SetOp=371`/`T_SetOpState=435` already in-tree corroborate the generated nodetags ordering.
- `EXEC_FLAG_BACKWARD = 0x0008`, `EXEC_FLAG_MARK = 0x0010`: from `types-nodes::executor` (executor.h).
- `TTS_FLAG_EMPTY = 1<<1`: from `types-nodes::executor` (tuptable.h).

## Seam / wiring audit
Owned seam crates: none. `nodeGroup.c` is the only c_source; no `crates/backend-executor-nodeGroup-seams` exists, and no other crate needs to call into Group across a cycle (the executor dispatch reaches `ExecGroup` through the installed `PlanState.ExecProcNode` callback / a direct dependency from execProcnode). `init_seams()` is therefore correctly empty (mirrors nodeResult). It is wired into `seams-init::init_all()` and the crate is a dependency of seams-init; both recurrence guards pass (`every_seam_installing_crate_is_wired_into_init_all`, `every_declared_seam_is_installed_by_its_owner`).

Outward seam calls (each justified by an unported owner, all thin marshal+delegate, no logic in the seam path):
- tcop_postgres: `check_for_interrupts`.
- execProcnode: `exec_init_node`, `exec_proc_node`, `exec_end_node`.
- execAmi: `exec_re_scan`.
- execUtils: `exec_assign_expr_context`, `exec_create_scan_slot_from_outer_plan`, `exec_assign_projection_info`.
- execTuples: `exec_init_result_tuple_slot_tl`, `exec_copy_slot`, `exec_clear_tuple`, `exec_get_result_slot_ops`, `exec_get_result_type`.
- execExpr: `exec_init_qual`, `exec_qual`, `exec_qual_and_reset`, `exec_project`.
- execGrouping: `exec_tuples_match_prepare`.

## Design conformance
- No `todo!`/`unimplemented!` (grep clean).
- No stand-in type aliases; `Group`/`GroupStateData` reuse the real `Plan`/`ScanStateData`/`ExprState` vocabulary (no invented opacity).
- Allocating init path is `Mcx` + `PgResult` throughout (`alloc_in`, fallible seams).
- No statics/atomics/mutexes; per-node state is owned. (Tests use `thread_local!`, never shared statics.)
- The only `panic!`s are `castNode` mismatches (C-assertion analog); the only `.expect`s mirror C's unconditional dereferences of init-established invariants, not error paths.
- No locks held across `?`.

## Verdict: PASS

All four functions MATCH; helpers correctly inline the C macros/inlines; constants verified against the c2rust run / headers; seam set complete and installed (empty installer is correct — no owned seam crate). 8 in-crate state-machine tests pass; `cargo check --workspace` clean; recurrence guards pass.
