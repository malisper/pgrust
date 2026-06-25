# Audit: #165 P0 — AggStateData ExprContext storage-model reconcile

- **Verdict:** PASS (self-audit)
- **Date:** 2026-06-15
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Scope:** Keystone #165 step P0 ONLY (ExprContext field retype). P1-P4 out
  of scope by design.

## Goal

The repo models `ExprContext *` as an `EcxtId(u32)` pool index into
`EStateData::es_exprcontexts` (e.g. `PlanStateData.ps_ExprContext`,
`nodemergejoin`, `nodewindowagg`, `nodeindexscan`). `AggStateData`'s
`ExprContext *` fields were the lone holdout, typed `Option<PgBox<ExprContext>>`
/ `Option<PgVec<PgBox<ExprContext>>>` — inexpressible against the pool model and
the reason `ExecInitAgg` loud-panicked at the first ExprContext assignment.

## Fields retyped (field-for-field vs C `execnodes.h`)

`crates/backend-executor-nodeAgg/src/aggstate.rs`:

| C field (`AggState`)          | C type          | was                              | now                       |
|-------------------------------|-----------------|----------------------------------|---------------------------|
| `hashcontext`                 | `ExprContext *` | `Option<PgBox<ExprContext>>`     | `Option<EcxtId>`          |
| `aggcontexts` (per gset)      | `ExprContext **`| `Option<PgVec<PgBox<ExprContext>>>` | `Option<PgVec<EcxtId>>` |
| `tmpcontext`                  | `ExprContext *` | `Option<PgBox<ExprContext>>`     | `Option<EcxtId>`          |
| `curaggcontext`               | `ExprContext *` (alias = index) | `i32` (unchanged) | `i32` (unchanged)         |

`curaggcontext` deliberately stays `i32`: in this port it is an INDEX into
`aggcontexts`, not a distinct pooled context (the C aliases one of the
`aggcontexts[]` entries).

## Consumers updated (all resolve EcxtId through the EState pool)

- `node_lifecycle.rs`: `rescan_expr_context_aggcontext` / `_hashcontext` now take
  `&mut EStateData` and call the `re_scan_expr_context` execUtils seam; all 3
  call sites (`ExecEndAgg`, `ExecReScanAgg`) thread `estate`.
- `hash_grouping.rs`: `reset_tmpcontext` / `rescan_hashcontext` take estate and
  use the `reset_expr_context` / `re_scan_expr_context` seams; `ecxt_outertuple`
  read/write resolve via `estate.ecxt(_mut)`; `hash_create_memory` now assigns
  `hashcontext = CreateWorkExprContext(estate)` (the P0 line) via the new seam,
  still panics afterward on the unported `hash_metacxt`/`hash_tablecxt` mmgr
  context factory.
- `transition.rs`: `tmpcontext_ecxt` now returns the stored id (was a panic);
  `curaggcontext_assert_built` checks the `aggcontexts` EcxtId vector;
  `process_ordered_aggregate_single/multi` + their 4 tmpcontext helpers thread
  estate (callers in `finalize.rs` updated).
- `sorted_grouping.rs`: the 5 context helpers (`set_tmpcontext_*`,
  `set_econtext_outertuple`, `econtext_outertuple_slot`, `reset_expr_context_tmp`)
  thread estate; `reset_expr_context_tmp` now uses the `reset_expr_context` seam
  (was an unconditional panic); `hashagg_finish_initial_spills` call updated.
- `spill.rs`: `hashcontext.ecxt_per_tuple_memory.subtree_used()` reads resolve
  via `estate.ecxt`; `hash_agg_update_metrics` / `hashagg_finish_initial_spills`
  gain an `estate` param (callers updated).
- `aggapi.rs`: `AggGetTempMemoryContext` field access fixed (still a fmgr
  back-reference panic — estate not threaded into the fmgr call frame; not P0).
- `exec_init_agg.rs`: tmpcontext assignment now lands; `aggcontexts` local is
  `PgVec<EcxtId>` and is populated + stored; panic reworded (model resolved, rest
  blocked on unported owners).
- `execExprInterp/eval_agg.rs`: `aggstate_tmpcontext` and
  `ecxt_id_to_aggcontext_index` (previously stale panics on the old box model)
  now resolve directly off the EcxtId fields.

## New seam (owner-installed)

`backend-executor-execUtils-seams::create_work_expr_context(estate, work_mem_kb)
-> EcxtId`, installed in `backend-executor-execUtils::init_seams` pointing at the
already-ported `CreateWorkExprContext`. Verified by the seams-init recurrence
guard (`every_declared_seam_is_installed_by_its_owner`).

## Faithfulness

- No `todo!`/`unimplemented!` introduced (no-todo-guard green; only `panic!`
  seam-and-panic for genuinely unported downstream owners).
- No new handles/registries/opaque tokens — the change REMOVES a divergent
  carrier and conforms `AggStateData` to the existing repo-wide `EcxtId` model.
- Remaining panics are mirror-PG-and-panic for unported owners (mmgr context
  factory, execTuples slot payload, catalog reads, ExecInitNode, ExecBuildAggTrans),
  documented in DESIGN_DEBT as the #165 P1-P4 chain.

## Gate

- `cargo check --workspace`: green (only pre-existing unrelated warnings).
- `cargo test -p no-todo-guard`: green (`report_todo_count` ok; tree-clean test
  still ignored as before — count unchanged).
- `cargo test -p seams-init`: green (both recurrence guards, including the new
  seam install check).

## Follow-on status (2026-06-17 reconcile)

The #165 **ownership boundary** has since LANDED — this is the AggState-as-
PlanState + `ExecInitExpr`/`ExecEvalExpr` seam boundary + `ExprState.parent`
non-owning back-pointer, the part the keystone was actually gating. Verified in
current tree:

- `PlanStateNode::Agg(Box<dyn AggStateLive>)` is a live variant
  (`types-nodes/src/planstate.rs:36`); `as_agg_state` returns `Some(..)` for it
  (`planstate.rs:383`), NOT `None`.
- `ExecInitAgg` is constructed and erased into that variant by `ExecInitNode`'s
  `Node::Agg` arm (`backend-executor-execProcnode/src/execProcnode_init.rs:321`);
  it no longer loud-panics at the ExprContext model boundary.
- The `ExprState.parent` cycle is resolved by `PlanStateLink`, a lifetime-free
  raw back-pointer (`planstate.rs:589-613`), mirroring `EStateLink`. Parents are
  back-filled by `PlanStateNode::stamp_expr_parents` (`planstate.rs:293`), called
  from `ExecInitNode` (`execProcnode_init.rs:393`).
- The `exec_init_expr` / `exec_init_qual` / `exec_build_projection_info` seams
  are installed by their owner (`backend-executor-execExpr/src/lib.rs:79-81`) and
  consumed by the per-node init paths.

Consequently the old DESIGN_DEBT "PlanStateNode::Agg / ::SampleScan blocked"
entry (premises: `as_agg_state = None`, `ExecInitAgg` panics, no `Agg` variant)
no longer reflects the tree and has been removed from DESIGN_DEBT. NOTE there is
no `PlanStateNode::SampleScan` variant: SampleScan is blocked one layer down at
the trimmed central `Node` Plan enum (no `T_SampleScan` Plan node;
`execProcnode_init.rs:128-134`), a distinct issue unrelated to the #165
expression-ownership boundary.

### Remaining nodeAgg downstream chain (NOT the ownership boundary)

What is still unported is the nodeAgg **execution machinery** below the boundary,
each its own bounded follow-on unit, not a keystone:

- **P1/P2 — hash context factory**: `hash_metacxt` / `hash_tablecxt` mmgr context
  creation in `hash_create_memory` (`nodeAgg/src/hash_grouping.rs`; the
  `CreateWorkExprContext` P0 line lands, the factory after it panics).
- **P3 — `ExecBuildAggTrans`**: per-phase transition-expression compilation
  (`exec_init_agg.rs:1366-1411`, the `phase->evaltrans = ExecBuildAggTrans(...)`
  loop).
- **P4 — `AggGetTempMemoryContext` fmgr back-reference**: the fmgr-call-frame
  back-reference to the AggState's temp context (`aggapi.rs`; estate is not
  threaded into the fmgr frame).

These are downstream of, and decoupled from, the (now landed) AggState-as-
PlanState ownership boundary.
