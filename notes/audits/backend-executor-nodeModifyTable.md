# Audit: backend-executor-nodeModifyTable

- **Verdict:** **PASS**
- **Date:** 2026-06-13
- **Model:** Claude Fable 5
- **Unit:** `backend-executor-nodeModifyTable`
- **Branch:** `fix/diverge-backend-executor-nodeModifyTable` (off current `main`)
- **C source:** `src/backend/executor/nodeModifyTable.c` (postgres-18.3, 5282 lines)
- **c2rust:** `c2rust-runs/backend-executor-nodeModifyTable/src/*.rs`
- **Port:** `crates/backend-executor-nodeModifyTable/src/{lib,init,insert,insert_exec,
  update,delete,delete_exec,merge,merge_matched,exec,lifecycle,partition_init}.rs`,
  owned seam crate `crates/backend-executor-nodeModifyTable-seams`,
  canonical types in `crates/types-nodes/src/{execnodes,modifytable,nodes}.rs`

## Method

Independent re-audit after the blocker-fix round (commit
`fix(nodeModifyTable): resolve MERGE matched/init + ON CONFLICT init blockers`).
The 39 definitions were re-enumerated against the C and the c2rust run. The
three FAIL findings from the prior audit (`fix/epqstate-canonical`) — Findings A
(ExecMergeMatched uninstalled own-logic seams), B (ExecInitMerge control flow in
a seam), C (ExecInitModifyTable ON CONFLICT deferral) — were each re-derived from
scratch for *real* resolution. SQLSTATEs, NodeTags, flag bits were re-checked
against the C headers (`nodetags.h`, `execnodes.h`, `errcodes`). The 30 prior
MATCH / 3 SEAMED verdicts on the untouched functions carry forward; the EPQState
canonicalization the branch exists to land remains correct.

## Function inventory & verdicts (38 definitions + ExecReScan stub)

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | ExecCheckPlanOutput | 195 | lifecycle.rs:55 | MATCH | tlist walk, both column-count errors; now also called in-crate from ExecInitMerge's CMD_INSERT arm. |
| 2 | ExecProcessReturning | 288 | lifecycle.rs:160 | MATCH | unchanged. |
| 3 | ExecCheckTupleVisible | 363 | insert.rs:827 | MATCH | unchanged. |
| 4 | ExecCheckTIDVisible | 397 | insert.rs:787 | MATCH | unchanged. |
| 5 | ExecInitGenerated | 430 | lifecycle.rs:281 | MATCH | owned-seam-installed. |
| 6 | ExecComputeStoredGenerated | 544 | lifecycle.rs:430 | MATCH | dispatch+early-exit in-crate; slot loop seamed to execTuples. |
| 7 | ExecInitInsertProjection | 640 | insert.rs:476 | MATCH | unchanged. |
| 8 | ExecInitUpdateProjection | 710 | update.rs:888 | MATCH | unchanged. |
| 9 | ExecGetInsertNewTuple | 768 | insert.rs:439 | MATCH | unchanged. |
| 10 | ExecGetUpdateNewTuple | 812 | update.rs:875 | MATCH | unchanged. |
| 11 | ExecInsert | 850 | insert_exec.rs:168 | MATCH | unchanged. Reads `mtstate->mt_merge_action->mas_action->commandType` (C 1080, insert_exec.rs:342-358) to pick the RLS WCO kind under MERGE — now correctly fed by the restored `mt_merge_action` assignment (rows 27/28). |
| 12 | ExecBatchInsert | 1367 | insert.rs:341 | MATCH | unchanged. |
| 13 | ExecPendingInserts | 1427 | insert.rs:402 | MATCH | unchanged. |
| 14 | ExecDeletePrologue | 1460 | delete.rs:19 | MATCH | unchanged. |
| 15 | ExecDeleteAct | 1492 | delete.rs:67 | MATCH | unchanged. |
| 16 | ExecDeleteEpilogue | 1514 | delete.rs:98 | MATCH | unchanged. |
| 17 | ExecDelete | 1572 | delete_exec.rs:274 | MATCH | unchanged; EPQ path threads canonical mt_epqstate. |
| 18 | ExecCrossPartitionUpdate | 1930 | update.rs:637 | MATCH | unchanged. |
| 19 | ExecUpdatePrologue | 2092 | update.rs:332 | MATCH | unchanged. |
| 20 | ExecUpdatePrepareSlot | 2135 | update.rs:393 | MATCH | unchanged. |
| 21 | ExecUpdateAct | 2170 | update.rs:422 | MATCH | unchanged. |
| 22 | ExecUpdateEpilogue | 2321 | update.rs:569 | MATCH | unchanged. |
| 23 | ExecCrossPartitionUpdateForeignKey | 2368 | update.rs:800 | MATCH | unchanged. |
| 24 | ExecUpdate | 2461 | update.rs:36 | MATCH | unchanged; EPQ path threads canonical mt_epqstate. |
| 25 | ExecOnConflictUpdate | 2713 | insert.rs:578 | MATCH | all 6 TM_Result arms / SQLSTATEs. |
| 26 | ExecMerge | 2931 | merge.rs:17 | MATCH | matched→ExecMergeMatched, !matched dispatch. |
| 27 | ExecMergeMatched | 3057 | merge_matched.rs:107 | **MATCH** | **RESOLVED (was PARTIAL→FAIL).** The matched-action core now reads the owned `ri_MergeActions[]` / `ri_MergeJoinCondition` / `ri_RowIdAttNo` / `ri_WithCheckOptions` / `ri_projectReturning` directly in-crate (mirroring ExecMergeNotMatched): the empty-list early return, join-cond `ExecQual` (NULL=true), the per-action snapshot of `commandType`/`matchKind`/`mas_whenqual`/`mas_proj`, the WCO check, the CMD_UPDATE/DELETE/NOTHING dispatch, all TM_Result arms (TM_Ok/SelfModified/Deleted/Updated incl. the EPQ recheck + list switch + instrument count), and the RETURNING block are all local. The genuine foreign primitives go through their real owner `-seams`: `ExecQual`/`ExecProject` (execExpr-seams), `ExecForceStoreHeapTuple` + `slot_getattr` (=ExecGetJunkAttribute, execTuples-seams), `ExecIRDeleteTriggers` (trigger-seams), `InstrUpdateTupleCount` (instrument-seams), `table_tuple_lock`/`fetch_row_version` (tableam), `LockTuple`/`UnlockTuple` (lmgr-seams), `EvalPlanQual*` (execMain-seams). **Zero inline/uninstalled own-logic seams remain.** SQLSTATEs verified (21000/27000/40001); moved-partition sentinel verified. `mtstate->mt_merge_action = relaction` (C 3192 CMD_UPDATE, 3245 CMD_DELETE) is now RESTORED (was wrongly dropped): each arm materializes an owned `MergeActionState{type_=T_MergeActionState, mas_action: Some(MergeAction{matchKind, commandType, overriding, ..None})}` from the per-action snapshot before the prologue. The owned-by-value tree can't alias the pooled state's `&'mcx` borrow, so an equivalent owned node carrying the active action's identity (the only fields any consumer reads) is stored — mirroring ExecInitMerge's construction. `mas_proj`/`mas_whenqual` are `None` (already consumed by ExecProject/ExecQual before the assignment; no `mt_merge_action` reader touches them). |
| 28 | ExecMergeNotMatched | 3597 | merge.rs:76 | MATCH | in-crate action loop over the owned `ri_MergeActions[NOT_MATCHED_BY_TARGET]`. CMD_INSERT arm now RESTORES `mtstate->mt_merge_action = action` (C 3653) before `ExecInsert` — materializes the owned `MergeActionState` (matchKind/commandType/overriding) so `ExecInsert`'s MERGE RLS WCO-kind selection (`mt_merge_action->mas_action->commandType`, C 1080 / insert_exec.rs:342-358) sees the active action instead of a stale/`None` value. |
| 29 | ExecInitMerge | 3680 | merge.rs:219 | **MATCH** | **RESOLVED (was PARTIAL→FAIL).** The per-action `foreach(l, mergeActionList)` is back in-crate: the `switch(action->commandType)`, the `mt_merge_subcommands |= MERGE_INSERT/UPDATE/DELETE` accumulation, the CMD_INSERT partitioned-vs-inherited decision (`relkind==RELKIND_PARTITIONED_TABLE`, lazy `mt_partition_tuple_routing`/`mt_root_tuple_slot` via `table_slot_create`+`ExecSetupPartitionTupleRouting`, lazy `rootRelInfo->ri_newTupleSlot`), the `ExecCheckPlanOutput(rootRelInfo)` call, the default `elog(ERROR, "unknown action in MERGE WHEN clause")`, the `makeNode(MergeActionState)` construction (`type_=T_MergeActionState`), and the append into `ri_MergeActions[action->matchKind]` are all local. Only the leaf primitives are seamed: `exec_init_merge_when_qual` (=ExecInitQual), `exec_build_merge_insert_projection` (=ExecBuildProjectionInfo over explicit tgtslot/tgtdesc), `exec_build_merge_update_projection` (=ExecBuildUpdateProjection) in execExpr-seams. MERGE_INSERT/UPDATE/DELETE = 0x01/0x02/0x04 verified. The inherited-root WCO/RETURNING dispatch and `ri_MergeJoinCondition` build remain thin owner seams (real foreign machinery). |
| 30 | ExecInitMergeTupleSlots | 3960 | merge.rs:339 | MATCH | two `table_slot_create`, `ri_projectNewInfoValid=true`. |
| 31 | fireBSTriggers | 3980 | lifecycle.rs | SEAMED | operation/ONCONFLICT/MERGE-mask dispatch in-crate; each ExecBS{Insert,Update,Delete}Triggers fired via real `backend_commands_trigger_seams::exec_bs_*_triggers::call` (trigger.c genuinely unported). **Real seam — was an `unported()` Err stub under the strict rule (MISLABELED→MISSING); now fixed.** |
| 32 | fireASTriggers | 4017 | lifecycle.rs | SEAMED | dispatch + transition-capture selection (mt_oc_transition_capture for the ONCONFLICT_UPDATE AS-update, else mt_transition_capture) in-crate; each ExecAS{Insert,Update,Delete}Triggers via real `exec_as_*_triggers::call`. **Real seam — was an `unported()` Err stub; now fixed.** |
| 33 | ExecSetupTransitionCaptureState | 4062 | lifecycle.rs:491 | SEAMED | mt_transition_capture / mt_oc_transition_capture assignment + CMD_INSERT&ONCONFLICT_UPDATE decision in-crate; MakeTransitionCaptureState(operation / CMD_UPDATE) via real `make_transition_capture_state::call` (trigger.c genuinely unported). **Real seam — was an `unported()` Err stub; now fixed.** |
| 34 | ExecPrepareTupleRouting | 4091 | lifecycle.rs:528 | MATCH | `*partRelInfo` written, map==NULL fast path. |
| 35 | ExecModifyTable | 4151 | exec.rs:98 | MATCH | es_epq_active guard fires as Err. |
| 36 | ExecLookupResultRelByOid | 4583 | lifecycle.rs:620 | MATCH | unchanged. |
| 37 | ExecInitModifyTable | 4632 | init.rs:31 | **MATCH** | **RESOLVED (was PARTIAL→FAIL).** The ON CONFLICT DO UPDATE block (C 5037-5088) is now ported in-crate: `makeNode(OnConflictSetState)` (`type_=T_OnConflictSetState`), reuse-or-`ExecAssignExprContext` of `ps_ExprContext`, `oc_Existing`/`oc_ProjSlot` via `table_slot_create(ri_RelationDesc)`, `oc_ProjInfo` via `ExecBuildUpdateProjection(onConflictSet, true, onConflictCols, relDesc, econtext, oc_ProjSlot)` (the new thin `exec_build_on_conflict_set_projection` execExpr leaf), `oc_WhereClause` via `ExecInitQual(onConflictWhere)` when present (the `exec_init_on_conflict_where` leaf), stored on `resultRelInfo->ri_onConflict`. The arbiter-index store (5025-5031) was already present. The remaining `Err(unported)` deferrals (FDW direct-modify, FDW per-rel/batch, es_auxmodifytables) are all owner-blocked on genuinely unported neighbors — acceptable. |
| 38 | ExecEndModifyTable | 5221 | lifecycle.rs:687 | MATCH | unchanged. |
| (39) | ExecReScanModifyTable | 5275 | lifecycle.rs | MATCH | `elog(ERROR, "...not implemented")` mirrored. |

**Net: 35 MATCH, 3 SEAMED, 0 PARTIAL/MISSING/DIVERGES.**

## Resolution of the mt_merge_action divergence (this round)

The prior audit recorded `mtstate->mt_merge_action = relaction/action` (C 3192,
3245, 3653) as "documented residue, not a logic gap," asserting its only readers
were transition-capture / ON CONFLICT (both seamed). That was **wrong**: the
in-crate `ExecInsert` reads `mt_merge_action->mas_action->commandType`
(insert_exec.rs:342-358, mirroring C 1080) to choose the RLS WCO kind
(`WCO_RLS_UPDATE_CHECK` vs `WCO_RLS_INSERT_CHECK`) for every MERGE INSERT. With
the assignment dropped, `mt_merge_action` stayed `None` (or stale), so the
`.expect(...)` would fire — an observable divergence from PostgreSQL.

**Fix:** all three sites now set `mtstate.mt_merge_action` to an owned
`MergeActionState` carrying the active action's identity
(`mas_action.{matchKind, commandType, overriding}`), built from the per-action
snapshot exactly as `ExecInitMerge` builds the pooled state. The C pointer alias
is expressed as owned trimmed data (inherited opacity; `mas_proj`/`mas_whenqual`
are already-consumed and read by no `mt_merge_action` consumer, so `None`). No
new seam, no new constant. Re-derived arm-for-arm against C 3175-3260 / 3630-3665.

## Resolution of the 3 blockers

1. **Finding A — ExecMergeMatched uninstalled own-logic seams → RESOLVED.** All
   ~16 inline `seam!` slots that fronted now-owned `ResultRelInfo` fields are
   gone; the matched-action core reads the owned fields directly in-crate, and
   the genuine foreign primitives route through their real owner `-seams`
   crates. Grep confirms zero `seam_core::seam!` declarations in
   `merge_matched.rs` and no `ri_merge_*`/`delete_exec::*` own-logic calls.
2. **Finding B — ExecInitMerge control flow in a seam → RESOLVED.** The
   per-action commandType switch + `mt_merge_subcommands` accumulation +
   partitioned/inherited INSERT init + the `ri_MergeActions` append are back
   in-crate; `exec_init_merge_actions_for_rel` is deleted and the seam surface is
   thin `ExecInitQual`/`ExecBuildProjectionInfo`/`ExecBuildUpdateProjection`
   leaves.
3. **Finding C — ExecInitModifyTable ON CONFLICT → RESOLVED.** The
   `OnConflictSetState` build is ported in-crate against the now-present
   `ri_onConflict` field; only the projection/qual builders are leaf seams.

## Seam audit

**Owned seam crate** `crates/backend-executor-nodeModifyTable-seams`: 7
declarations (`exec_init_generated`, `exec_get_on_conflict_action`,
`exec_open_partition_indices`, `exec_init_partition_with_check_options`,
`exec_init_partition_returning`, `exec_init_partition_on_conflict`,
`exec_init_partition_merge`). `init_seams()` (lib.rs:119) installs **all 7**
(only `set()` calls); `seams-init::init_all()` calls `init_seams()`. **Clean.**

**Outward seams:**
- ExecMergeMatched: no inline seams (Finding A resolved). All outward calls front
  genuinely foreign machinery (execExpr/execTuples/trigger/instrument/tableam/
  lmgr/execMain) and are thin marshal+delegate.
- ExecInitMerge: `exec_init_merge_when_qual` / `exec_build_merge_insert_projection`
  / `exec_build_merge_update_projection` (execExpr-seams) are thin
  `ExecInitQual`/`ExecBuild*Projection` leaves — no control flow (Finding B
  resolved).
- New outward seams added this round: `exec_build_on_conflict_set_projection` /
  `exec_init_on_conflict_where` (execExpr-seams), `exec_ir_delete_triggers`
  (trigger-seams), `instr_update_tuple_count` (instrument-seams). The first three
  front unported owners (execExpr/trigger) and panic-until-landed per the
  mirror-and-panic contract; `instr_update_tuple_count`'s owner
  (`backend-executor-instrument`) **is** ported and installs it in its
  `init_seams()` (lib.rs:355). **No uninstalled own-logic seams.**

## Design conformance (§3b)

- **EPQState opacity:** clean — canonical owned struct, no handle stand-in.
- **Inherited opacity:** the per-action data is read from the owned
  `ri_MergeActions` and the `&'mcx` plan-tree borrow of `MergeAction`; no invented
  handles. The constructed `MergeActionState.mas_action` carries the
  command/match-kind fields the executor reads (the C pointer alias to the plan
  `MergeAction` is expressed as owned trimmed data) — consistent with the
  owned-by-value tree.
- **New NodeTag constants** (`T_MergeActionState=387`, `T_OnConflictSetState=386`,
  `T_MergeAction=54`) verified against PostgreSQL 18.3 `nodetags.h`.
- **Mcx + PgResult on allocation / ereport:** the new in-crate builders thread
  `mcx` and return `PgResult`; the new seams allocate via `Mcx`/`PgResult`.
- **Failure mode:** the remaining init.rs deferrals return `Err`, not `panic!`,
  and are all owner-blocked on unported neighbors.
- **Shared statics / ambient globals / locks-across-`?`:** none introduced.

## Spot-check of the auditor

Re-derived in detail: ExecMergeMatched SQLSTATEs (21000/27000/40001 confirmed in
types-error), the TM_Updated EPQ-recheck list-switch + instrument-count
predicate (`both NOT_MATCHED_BY_SOURCE and NOT_MATCHED_BY_TARGET non-empty`,
matches C L3458-3475), and the ON CONFLICT slot-create/projection ordering
(matches C L5052-5085). MERGE subcommand bits and NodeTag values verified
against headers, not memory.

## Conclusion

**PASS.** 39 definitions: 35 MATCH, 3 SEAMED (fireBSTriggers, fireASTriggers,
ExecSetupTransitionCaptureState — all trigger.c-owned), 0 PARTIAL/MISSING/
DIVERGES. The `mt_merge_action` divergence flagged this round is fixed at all
three sites (rows 11/27/28); the three prior blockers remain resolved; the owned
seam crate is fully installed; no uninstalled own-logic seams remain. `cargo
check --workspace` and `cargo test --workspace` are green.
