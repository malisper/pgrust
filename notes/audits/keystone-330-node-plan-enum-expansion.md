# Audit: Keystone #330 — central Node/Plan enum expansion + exec-dispatch

- **Task:** EXECUTOR KEYSTONE #330 (Node/Plan enum variants + Plan.initPlan +
  dispatch wiring; prerequisite for #165, resolves #328/#329 defects)
- **Branch:** worktree-agent (this lane)
- **Date:** 2026-06-15
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PARTIAL — landed the no-cycle subset; cycle-blocked variants STOPped with notes**

## What the gap actually was (verified against the tree, not the prompt)

The prompt listed Agg/IncrementalSort/FunctionScan/TidScan/SampleScan/
WorkTableScan/NamedTuplestoreScan as omitted. Re-derived against
`crates/types-nodes/src/nodes.rs` (plan-tree `Node`) and
`crates/types-nodes/src/planstate.rs` (`PlanStateNode`) + PG 18.3 nodetags.h:

Plan-tree `Node` enum already had FunctionScan, TidScan, SampleScan,
WorkTableScan, NamedTuplestoreScan, TidRangeScan, Sort, Group, WindowAgg.
**Genuinely missing: `Agg`** (and `IncrementalSort`, whose plan struct does not
exist anywhere — owner unported).

`PlanStateNode` was missing: Agg, IncrementalSort, FunctionScan, SampleScan,
WorkTableScan. (TidScan, NamedTuplestoreScan already present.)

`Plan` struct was missing `initPlan`.

## Carrier-cycle analysis (the deciding factor)

`PlanStateNode` variants carry the concrete `<Node>State` **by value** (needs
`.ss.ps`/`.ps`/`.js.ps` access for `ps_head`/`as_scan_state`). A variant is
only addable if its state struct lives in `types-nodes` (or below).

| State struct | Home crate | Direction | Verdict |
|---|---|---|---|
| `AggStateData` | `backend-executor-nodeAgg` | nodeAgg → types-nodes | **CYCLE — blocked** |
| `SampleScanState` | `types-samplescan` | types-samplescan → types-nodes | **CYCLE — blocked** |
| `FunctionScanState` | (does not exist; owner todo) | — | **blocked (unported)** |
| `IncrementalSortState` / `IncrementalSort` plan | (do not exist; owner todo) | — | **blocked (unported)** |
| `WorkTableScanStateData` | `types-nodes` (`nodeworktablescan`) | in-crate | **OK — landed** |

The Agg/SampleScan cycle is exactly the #200/#165 `AggStateData`-relocation
keystone. It cannot be resolved inside this task.

## What landed (faithful, no stubs)

1. **`Node::Agg` plan-tree variant** — `crates/types-nodes/src/nodeagg.rs`:
   added `pub const T_Agg = NodeTag(365)` (verified: Group=364, Agg=365,
   WindowAgg=366 against repo's adjacent tags) + `Agg::clone_in` (deep-copies
   plan/grp arrays/grouping_sets/agg_params Bitmapset/chain). Wired into
   `Node::{tag, plan_head, clone_in}` in `nodes.rs`.

2. **`Plan.initPlan` field** — `crates/types-nodes/src/nodeindexscan.rs`:
   `Option<PgVec<SubPlan>>`, defaulted via the struct's `#[derive(Default)]`,
   threaded through `Plan::clone_in` (clones each `SubPlan`).

3. **`PlanStateNode::WorkTableScan`** — `planstate.rs`: variant +
   `T_WorkTableScanState = NodeTag(417)` (verified: CteScanState=415,
   NamedTuplestoreScanState=416, WorkTableScanState=417) + `tag`/`ps_head`/
   `ps_head_mut`/`as_scan_state` arms.

4. **#328 fixed** — `execProcnode_init.rs`: the `ExecInitNode` initPlan walk
   (was a hard panic "ExecInitSubPlan not ported / Plan.initPlan not modeled")
   now clones each `node->initPlan` SubPlan into mcx and routes to the merged
   `backend-executor-nodeSubplan::ExecInitSubPlan`, gathering the
   `SubPlanState`s into `result->initPlan`. Mirrors C
   `foreach(l,node->initPlan){...ExecInitSubPlan(subplan,result)...}` incl. the
   `Assert(subplan->args == NIL)`. Removed the dead `node_has_init_plan` helper.

5. **WorkTableScan dispatch** — fully wired (the owner's pieces all exist):
   - `ExecInitNode` T_WorkTableScan arm → `ExecInitWorkTableScan` +
     `PlanStateNode::WorkTableScan`.
   - `ExecEndNode`: added to the C "no clean up actions" fall-through group
     (`T_ValuesScanState | T_NamedTuplestoreScanState | T_WorkTableScanState ->
     break`) — verified against execProcnode.c (there is NO
     `ExecEndWorkTableScan` in C; it is a `break` case).
   - `ExecReScan` (execAmi) T_WorkTableScanState arm →
     `ExecReScanWorkTableScan`.

## What is STOPped (precise blocker per variant)

- **`PlanStateNode::Agg` + the #329 `ExecReScanAgg` arm + the `Node::Agg`
  ExecInit wiring + `as_agg_state` returning the real AggState** — blocked on
  the `AggStateData`-relocation keystone (#200/#165): `types-nodes` cannot name
  nodeAgg's `AggStateData` without a crate cycle. The `Node::Agg` ExecInit arm
  is a loud seam-and-panic (mirror PG and panic) carrying that exact reason.
  `ExecReScanAgg` (#329) likewise has no `PlanStateNode::Agg` to dispatch to.
- **`PlanStateNode::SampleScan`** — same cycle (`SampleScanState` in
  `types-samplescan`).
- **`PlanStateNode::FunctionScan` + `Node::IncrementalSort` /
  `PlanStateNode::IncrementalSort`** — owners (nodeFunctionscan,
  nodeIncrementalSort) are `todo`; the state structs (and IncrementalSort's
  plan struct) do not exist. Nothing to carry.

## Gate result

- `cargo check --workspace`: **green**.
- `no-todo-guard`: **green** (10 passed / 0 failed).
- `seams-init` `every_declared_seam_is_installed_by_its_owner`: **RED, NOT
  mine** — every missing seam in the failure is `backend_catalog_indexing::*`
  (the separate concurrent catalog-indexing fix called out in the task). My
  commits touch zero catalog files (`git diff HEAD~2..HEAD --name-only`); the
  non-catalog_indexing missing count is 0.

## Self-audit notes

- All NodeTag values cross-checked against repo-adjacent constants (no guessing
  from memory): T_Agg=365, T_WorkTableScanState=417.
- No existing variant regressed (workspace builds; the `Node`/`PlanStateNode`
  match machinery in `tag`/`plan_head`/`clone_in`/`ps_head*`/`as_scan_state`
  all gained the new arms; external `#[non_exhaustive]` consumers unaffected).
- No `todo!`/`unimplemented!`; the one new panic is a mirror-PG-and-panic
  seam-and-panic for the cycle-blocked `Node::Agg` ExecInit.
