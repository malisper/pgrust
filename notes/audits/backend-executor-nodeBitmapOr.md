# Audit: backend-executor-nodeBitmapOr

Independent function-by-function audit of `src/backend/executor/nodeBitmapOr.c`
(PostgreSQL 18.3) against the c2rust rendering
(`../pgrust/c2rust-runs/backend-executor-nodeBitmapOr/`) and the Rust port
(`crates/backend-executor-nodeBitmapOr/{src/lib.rs,src/nodes.rs}`).
Audited: 2026-06-13 (Claude Fable 5).

## Function inventory

The C TU defines exactly 5 functions; all are present in the c2rust run and the
port.

| C function (nodeBitmapOr.c) | Port location | Verdict | Notes |
|---|---|---|---|
| `ExecBitmapOr` (static, L42-47) | `lib.rs` `ExecBitmapOr` | MATCH | Pro-forma stub. C `elog(ERROR, "BitmapOr node does not support ExecProcNode call convention")` → `Err` with `ERRCODE_INTERNAL_ERROR` (elog default). Returns `PgResult<()>` because the C error path returns no slot. The owned dispatch never installs/calls this; behaviour preserved. |
| `ExecInitBitmapOr` (L55-104) | `lib.rs` `ExecInitBitmapOr` | MATCH | See detail below. |
| `MultiExecBitmapOr` (L110-185) | `lib.rs` `MultiExecBitmapOr` | MATCH | See detail below. |
| `ExecEndBitmapOr` (L195-216) | `lib.rs` `ExecEndBitmapOr` | MATCH | `for i in 0..nplans`, per-slot null-guard (`if let Some` slot + `as_deref_mut`), `ExecEndNode` via execProcnode seam. nplans == vec length, so the C `if (bitmapplans[i])` guard maps to the `Option` slot check. |
| `ExecReScanBitmapOr` (L218-241) | `lib.rs` `ExecReScanBitmapOr` | MATCH | See detail below. |

## Detailed comparisons

### ExecInitBitmapOr
- `Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)))` → `debug_assert!` with
  the same mask (`EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK` from `types_nodes::executor`).
- `nplans = list_length(node->bitmapplans)` → `node.bitmapplans.len()`.
- `palloc0(nplans * sizeof(PlanState *))` → `PgVec` with `try_reserve(nplans)`, OOM via `mcx.oom`.
- `ps.plan/state/ExecProcNode` assignments: the owned `PlanStateData::default()` head
  models the zeroed `makeNode` struct; the NodeTag is the struct identity (no field).
  `ExecProcNode` left `None` (the C stub is only dispatched on misuse; the owned model
  never dispatches a BitmapOr through the tuple-at-a-time path). Behaviour-preserving.
- `foreach` calling `ExecInitNode` storing into the array → iterate `&node.bitmapplans`,
  `execProcnode::exec_init_node::call(...)?`, push. C fills index `i` of the pre-zeroed
  array; the port pushes in order — identical resulting array.
- Returns the built `BitmapOrState` (PgBox) with `nplans` (checked i32 conversion) and
  `isshared` snapshotted from the plan node.

### MultiExecBitmapOr
- `if (node->ps.instrument) InstrStartNode(...)` → `if let Some(instr) … instr_start_node::call`.
- Loop `for (i = 0; i < nplans; i++)`:
  - `IsA(subnode, BitmapIndexScanState)` → `subnode.tag() == T_BitmapIndexScanState`
    (`T_BitmapIndexScanState = 407`, verified against c2rust constant table).
  - first-subplan create: `tbm_create(work_mem * (Size) 1024, isshared ? es_query_dsa : NULL)`
    → `(work_mem as usize).wrapping_mul(1024)`, dsa = `es_query_dsa` if `isshared` else `None`,
    via `tidbitmap::tbm_create::call`; `work_mem` from the init-small GUC seam.
  - `biss_result = result; subresult = MultiExecProcNode(subnode); if (subresult != result) elog(ERROR…)`
    → delegated to `nodeBitmapIndexscan::multi_exec_bitmap_index_child::call(subnode, result_ref, estate)`
    — a real seam-and-panic into the BitmapIndexScan owner (which holds the private
    `biss_result` field); OR-in-place + identity check live in the owner. SEAMED-correct:
    a cross-unit private-field access into an unported neighbour, not relocated own-logic.
  - standard branch: `subresult = MultiExecProcNode(subnode); if (!subresult || !IsA(subresult, TIDBitmap)) elog(ERROR…)`
    → `multi_exec_proc_node::call` returns a typed `PgBox<TIDBitmap>`, so the NULL /
    wrong-tag error paths are statically unreachable (the C `IsA(…, TIDBitmap)` is
    satisfied by the return type). `if (result == NULL) result = subresult else { tbm_union(result, subresult); tbm_free(subresult); }`
    → `match result { None => set, Some => tbm_union::call(result, &subresult) }`;
    `subresult` dropped at scope end (the idiomatic `tbm_free`).
- `if (result == NULL) elog(ERROR, "BitmapOr doesn't support zero inputs")` →
  `result.ok_or_else(... "BitmapOr doesn't support zero inputs")`.
- `if (node->ps.instrument) InstrStopNode(node->ps.instrument, 0)` → `instr_stop_node::call(instr, 0.0)`.
- `return (Node *) result` → `Ok(result)`.

### ExecReScanBitmapOr
- `for (i = 0; i < node->nplans; i++)`:
  - `if (node->ps.chgParam != NULL) UpdateChangedParamSet(subnode, node->ps.chgParam)`
    → guarded by `chg_param_present`; the parent chgParam set is cloned (`clone_in`) so
    the child borrow and the read-only parent set coexist (the C reads the live set; the
    clone is value-identical), then `execUtils::update_changed_param_set::call`.
  - `if (subnode->chgParam == NULL) ExecReScan(subnode)` → `subnode.ps_head().chgParam.is_none()`
    then `execAmi::exec_re_scan::call`. Order of the two checks matches the C.

## Seam audit

`nodeBitmapOr` **owns no inward seam crate** (no `crates/backend-executor-nodeBitmapOr-seams`;
no other unit calls into it across a cycle — the executor reaches it through the
`execProcnode` arm these functions back). `init_seams()` is correctly empty, and the
recurrence guard's inverse check (`every_declared_seam_is_installed_by_its_owner`)
passes. `init_seams()` is wired into `seams-init::init_all()` (lib.rs:67) and the
guard `every_seam_installing_crate_is_wired_into_init_all` passes.

Outward seam calls — all real dependency-cycle delegations, thin marshal+delegate,
into named owner units:
- `execProcnode::{exec_init_node, multi_exec_proc_node, exec_end_node}` (child lifecycle/dispatch);
- `execAmi::exec_re_scan` (rescan);
- `execUtils::update_changed_param_set` (changed-param signaling);
- `instrument::{instr_start_node, instr_stop_node}` (per-node instrumentation);
- `tidbitmap::{tbm_create, tbm_union}` (running result bitmap, owned by backend-nodes-core);
- `nodeBitmapIndexscan::multi_exec_bitmap_index_child` (BitmapIndexScan special-case child run);
- `init-small::work_mem` (GUC).

No branching/node-construction/computation lives in any seam path.

## Owned nodes

`nodes.rs` defines `BitmapOr` (plannodes.h) and `BitmapOrState` (execnodes.h)
field-for-field with the C (plan base + isshared + bitmapplans; ps + bitmapplans
array + nplans). Per the owned-tree model (nodeAppend pattern) these are not yet
threaded into the central `Node`/`PlanStateNode` enums; `isshared` is snapshotted
onto the state at init because the BitmapOr plan node is not a `Node` variant the
head can alias (the C re-reads via `ps.plan`). The `palloc0` possibly-NULL array
maps to `Option<PgBox<PlanStateNode>>` slots. `T_BitmapOrState = 401`,
`T_BitmapAndState = 400` verified against the c2rust constant table.

## Gates

- `cargo check --workspace` — green (only pre-existing unrelated warnings in
  backend-access-common-printtup).
- `cargo test -p backend-executor-nodeBitmapOr` — green (0 tests; logic crate).
- `cargo test -p seams-init` — green; both recurrence_guard tests pass.

## Verdict: PASS

All 5 functions MATCH (or SEAMED per step-3 rules). Zero seam findings. No
own-logic stubs, no `todo!`/`unimplemented!`, no deferred/SEAMED-equivalent
escape of in-crate logic. CATALOG row set to `audited`.
