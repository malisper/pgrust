# Audit: backend-executor-execPartition

- Unit: `backend-executor-execPartition`
- C source: `src/backend/executor/execPartition.c` (PostgreSQL 18.3, 2635 lines)
- c2rust: `pgrust/c2rust-runs/backend-executor-execPartition/src/`
- Port crate: `crates/backend-executor-execPartition` (family modules:
  `routing_setup.rs`, `routing_find.rs`, `routing_init_info.rs`, `colnos.rs`,
  `pruning.rs`, `lib.rs`)
- Owned seam crate: `crates/backend-executor-execPartition-seams`
- Independent from-scratch audit (function inventory re-derived from C; seams and
  design conformance re-checked). Did **not** trust the prior in-tree audit or
  the port's comments/green build.
- Gate: `cargo check --workspace` PASS, `cargo test --workspace` PASS (only
  pre-existing dead-code warnings for `colnos::adjust_partition_colnos*`).
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Verdict: **PASS**

This re-audit follows the prior FAIL audit (commit 6fbc76be), which named three
findings in the run-time-pruning seam/contract family. All three are now closed
(§3, §4). The routing family and the pruning internals re-derive clean (MATCH).

## 1. Function inventory (every C definition gets a row — 18 functions)

| # | C function (location) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExecSetupPartitionTupleRouting` (L218) | routing_setup.rs:24 | MATCH | proute/dispatch[0] build + grow logic mirrored; root set; calls ExecInitPartitionDispatchInfo(parent=NULL, partidx=0). |
| 2 | `ExecFindPartition` (L265) | routing_find.rs:23 | MATCH | full while-loop; root-constraint check; FormPartitionKeyDatum; nparts==0/no-part error (ERRCODE_CHECK_VIOLATION + errdetail + errtable); leaf/sub-partition branches; default-partition re-check; scantuple save/restore. |
| 3 | `ExecInitPartitionInfo` (L502) | routing_init_info.rs:20 | MATCH | Prologue (table_open, InitResultRelInfo, CheckValidResultRel, es_tuple_routing_result_relations append) ported in-crate; per-command tail (L543–982: WCO, RETURNING, ON CONFLICT, MERGE, OpenIndices) mirrors C block ordering with the five plan-node-driven legs routed through dedicated nodeModifyTable seams that panic until that owner lands (unported-callee owner, not absent OWN logic — standing adjudication, see §5). |
| 4 | `ExecInitRoutingInfo` (L994) | routing_setup.rs:63 | MATCH | RootToChildMap → ri_PartitionTupleSlot (table_slot_create + EState reg) or NULL; array-tracking grow (8, *2); FDW BeginForeignInsert/ri_BatchSize block is a faithful no-op (ri_FdwRoutine absent from trimmed ResultRelInfo → C ri_BatchSize=1 path). |
| 5 | `ExecInitPartitionDispatchInfo` (L1102) | routing_setup.rs:165 | MATCH | es_partition_directory lazy-init gated on !IsolationUsesXactSnapshot(); root-vs-subpart open; partdesc lookup; key/keystate/tupmap/tupslot; indexes memset -1; grow (4, *2); nonleaf RRI for sub-parts; parent downlink. |
| 6 | `ExecCleanupTupleRouting` (L1241) | routing_setup.rs:379 | MATCH | dispatch close loop (i=1..) ported; non-borrowed leaf close panics on the unported `ExecCloseIndices` owner (allowed unported-callee panic); FDW EndForeignInsert skipped (faithful no-op). |
| 7 | `FormPartitionKeyDatum` (L1302) | routing_find.rs:347 | MATCH | partexprs/keystate first-time compile; plain col (slot_getattr) vs expr (ExecEvalExprSwitchContext); both "wrong number of partition key expressions" elog paths. |
| 8 | `get_partition_for_tuple` (L1399) | routing_find.rs:448 | MATCH | HASH (rowHash % nindexes), LIST (null_index, cached-find threshold, bsearch), RANGE (null→default, cached lower/upper checks, bsearch +1), default fallthrough, last_found cache update. |
| 9 | `ExecBuildSlotPartitionKeyDescription` (L1619) | routing_find.rs:683 | MATCH | RLS_ENABLED→None; table acl else per-col acl (InvalidAttrNumber/no-SELECT→None); buf build; getTypeOutputInfo + OidOutputFunctionCall; mbcliplen truncation. |
| 10 | `adjust_partition_colnos` (L1707) | colnos.rs:15 | MATCH (dead) | ChildToRootMap + delegate; no in-crate caller (its C caller, the ON CONFLICT leg, is delegated out per row 3). |
| 11 | `adjust_partition_colnos_using_map` (L1724) | colnos.rs:35 | MATCH (dead) | attno bounds/zero checks → "unexpected attno" elog; lappend_int; no in-crate caller (MERGE leg delegated out). |
| 12 | `ExecDoInitialPruning` (L1824) | pruning.rs:161 | MATCH | foreach pruneinfo: CreatePartitionPruneState → push to es_part_prune_states; do_initial_prune → internal find worker (with rtis out-param); bms_add_members → es_unpruned_relids; push results. **Estate-threaded, standalone, NOT collapsed into the seam path — C timing preserved (runs before child-node init).** |
| 13 | `ExecInitPartitionExecPruning` (L1880) | pruning.rs:230 | MATCH | relids bms_equal check → elog; **moves prebuilt owned PgBox<PartitionPruneState> out of es_part_prune_states (None tombstone keeps parallel indexing)**; do_initial_prune → bms_copy result else bms_add_range(0,n-1); do_exec_prune → InitExecPartitionPruneContexts; returns owned PgBox. |
| 14 | `CreatePartitionPruneState` (L1973) | pruning.rs:292 | MATCH | econtext; es_partition_directory(false); per-hierarchy/per-rel build; quick-compare memcmp path AND full pd_idx/pp_idx recheck-goto remap; present_parts copy; initial-context init gated on EXPLAIN_GENERIC; execparamids accumulation; EXPLAIN-skip leaf-rti collection. |
| 15 | `InitPartitionPruneContext` (L2239) | pruning.rs:651 | MATCH | strategy/partnatts/nparts; **context->boundinfo = partdesc->boundinfo restored as a real PartitionBoundInfo, moved out of the owned PartitionDirectoryLookup result** (was Opaque(None) — Finding 3 closed); partcollation/partsupfunc; stepcmpfuncs palloc0; per-step IsA(StepOp) skip, nullkeys bms_is_member skip, Const skip, ExecInitExpr vs ExecInitExprWithParams by planstate presence; PruneCxtStateIdx. |
| 16 | `InitExecPartitionPruneContexts` (L2341) | pruning.rs:780 | MATCH | bms_num_members<n_total → fix_subplan_map + 1-based new index array; back-to-front (rev) per-rel loop; exec-context init; present_parts rebuild (oldidx remap, subpart present check); other_subplans remap. Now takes `&mut PartitionPruneState` (no pool remove/insert dance). |
| 17 | `ExecFindMatchingSubPlans` (L2498) | pruning.rs:1002 (worker `find_matching_subplans` 1018) | MATCH | Asserts; per-hierarchy recurse from pprune[0]; other_subplans add; bms_copy out; prune_context reset. Trimmed public seam passes validsubplan_rtis=NULL (MergeAppend contract); the rtis-carrying internal worker serves ExecDoInitialPruning. Operates on `&mut PartitionPruneState` directly. |
| 18 | `find_matching_subplans_recurse` (L2571) | pruning.rs:1090 | MATCH | check_stack_depth; initial/exec/present_parts partset selection via get_matching_partitions; subplan_map≥0 → add subplan (+leaf rti when requested) else subpart_map recurse, else silently ignore. |

## 2. Design conformance

- Opacity inherited, not introduced: `boundinfo` was changed from `Opaque(None)`
  to the real `PartitionBoundInfo` type (resolving an introduced-opacity finding);
  `partsupfunc`/`planstate`/`initial_pruning_steps`/`exec_pruning_steps` remain
  `Opaque` carrying the unported planner/relcache payloads (inherited).
- Allocating functions take `Mcx` and return `PgResult` (CreatePartitionPruneState,
  InitPartitionPruneContext, the two seam entry points, find worker).
- No shared statics for per-backend globals; EState is threaded explicitly.
- Outward seam calls (bms_*, partcache, partdesc, partprune, execExpr, execUtils,
  stack-depth) are thin marshal+delegate.

## 3. Seam audit — PASS

Owned seam crate (by C-source coverage: execPartition.c) =
`backend-executor-execPartition-seams`, declaring exactly two seams:
`exec_init_partition_exec_pruning` and `exec_find_matching_subplans`.

- `init_seams()` (lib.rs:115) installs **both** with `set()`-only and nothing
  else. `seams-init/src/lib.rs:25` calls `backend_executor_execPartition::init_seams()`.
  No uninstalled declaration; no `set()` outside the owner. The prior FAIL
  (empty installer with owned seams outstanding) is **CLOSED**.
- Both seam signatures take/return an owned `PgBox<PartitionPruneState>` and
  thread `estate: &mut EStateData` (faithful: C reaches EState transitively via
  `planstate->state` / `context->exprcontext->ecxt_estate`, the owned model
  threads EState explicitly tree-wide, and the foreign `get_matching_partitions`
  seam requires `&mut EStateData`). The two nodeMergeAppend `::call` sites
  (lib.rs:133, :342) pass `estate` and store the returned box as
  `ms_prune_state: Option<PgBox<PartitionPruneState>>`.

## 4. Findings from the prior FAIL audit — all closed

1. **SEAM-WIRING — PASS.** Both pruning seams installed in `init_seams()`
   (set()-only); seams-init calls it. Run-time-pruned MergeAppend no longer
   panics on the uninstalled seam.
2. **CONTRACT DIVERGENCE — PASS.** The two entry points were rewritten from the
   by-value EState-pool / usize-index model to the authoritative nodeMergeAppend
   owned-`PgBox<PartitionPruneState>` contract. `es_part_prune_states` is now
   `PgVec<Option<PgBox<PartitionPruneState>>>` (the node owns its entry; a `None`
   tombstone preserves parallel indexing). `ExecDoInitialPruning` keeps its C
   timing (estate-threaded, before child-node init, not folded into the seam
   path).
3. **PARTIAL boundinfo — PASS.** `PartitionPruneContext.boundinfo` is the real
   `PartitionBoundInfo` type; `InitPartitionPruneContext` moves it out of the
   owned `PartitionDirectoryLookup` result, mirroring C
   `context->boundinfo = partdesc->boundinfo` (L2255). `clone_boundinfo_handle`
   (which returned `Opaque(None)`) is removed.

## 5. Standing adjudication (not a regression)

`ExecInitPartitionInfo`'s five plan-node-driven legs (WCO, RETURNING, ON CONFLICT,
MERGE, OpenIndices) are delegated to `backend-executor-nodeModifyTable-seams`.
This was deliberately implemented (commit 80fc7fe4) and adjudicated **MATCH** by
the prior audit as a neighbor-ownership split (those legs read the unported
`ModifyTable` plan-node type and write nodeModifyTable's per-command
ResultRelInfo fields), not absent OWN logic. The current fix did not touch this
path; the adjudication stands.

## Verdict

**PASS** — every function MATCH (or the standing-adjudicated neighbor split for
ExecInitPartitionInfo); both owned pruning seams installed; the pruning contract
conforms to the authoritative nodeMergeAppend consumer; boundinfo restored to a
real type. Gates green.
