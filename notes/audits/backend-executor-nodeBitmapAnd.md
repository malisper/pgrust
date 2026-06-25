# Audit: backend-executor-nodeBitmapAnd

C source: `src/backend/executor/nodeBitmapAnd.c` (PostgreSQL 18.3).
Port crate: `crates/backend-executor-nodeBitmapAnd`.
c2rust: `c2rust-runs/backend-executor-nodeBitmapAnd/src/nodeBitmapAnd.rs`.

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ExecBitmapAnd` (static) | nodeBitmapAnd.c:41-47 | lib.rs `ExecBitmapAnd` | MATCH | Pro-forma `ExecProcNode` stub; `elog(ERROR,...)` → `Err(elog_error(...))` with `ERRCODE_INTERNAL_ERROR` (bare `elog ERROR` ⇒ XX000). Installed as the node's `ExecProcNode` callback (signature matches `ExecProcNodeMtd`). |
| `ExecInitBitmapAnd` | nodeBitmapAnd.c:55-107 | lib.rs `ExecInitBitmapAnd` | MATCH | `Assert(!(eflags & (BACKWARD|MARK)))` → `debug_assert!`. `palloc0(nplans*ptr)` → `bitmapplans` PgVec (allocated in `mcx`). `makeNode` → `alloc_in(BitmapAndState::new_in)`. Sets `ps.plan = Some(node)`, `ps.ExecProcNode = Some(ExecBitmapAnd)`, `nplans`. `foreach(ExecInitNode)` → loop calling `exec_init_node` seam, pushing each child. `ps.state` back-link reconstructed by executor (repo threads estate explicitly — established convention). Returns `PlanStateNode::BitmapAnd`. |
| `MultiExecBitmapAnd` | nodeBitmapAnd.c:110-175 | lib.rs `MultiExecBitmapAnd` | MATCH | `if (ps.instrument) InstrStartNode` → `instr_start_node` seam under `Option`. Loop over `nplans`: `MultiExecProcNode(subnode)` → `multi_exec_proc_node` seam returning the child's real `TIDBitmap` (the C `IsA(subresult, TIDBitmap)` tag check is satisfied by the type system). First subplan → `result`; else `tbm_intersect(result, subresult)` then `tbm_free(subresult)` (both seams). `if (tbm_is_empty(result)) break` checked every iteration via `tbm_is_empty` seam (matches C, which checks after the first iteration too). `result == NULL` ⇒ `elog(ERROR, "BitmapAnd doesn't support zero inputs")`. `InstrStopNode(instr, 0)` → `instr_stop_node(instr, 0.0)`. Returns the bitmap. |
| `ExecEndBitmapAnd` | nodeBitmapAnd.c:178-198 | lib.rs `ExecEndBitmapAnd` | MATCH | Loop over `nplans`; `if (bitmapplans[i]) ExecEndNode(...)` → per-slot `Option` guard calling `exec_end_node` seam. |
| `ExecReScanBitmapAnd` | nodeBitmapAnd.c:201-216 | lib.rs `ExecReScanBitmapAnd` | MATCH | Loop over `nplans`; `if (ps.chgParam != NULL) UpdateChangedParamSet(subnode, ps.chgParam)` → `update_changed_param_set` seam (parent chgParam cloned in `mcx` to coexist with the child mut-borrow — read-only in C, identical copy). `if (subnode->chgParam == NULL) ExecReScan(subnode)` → `exec_re_scan` seam, reading the child's `ps_head().chgParam`. |

## Constants verified vs headers

- `T_BitmapAnd = 337`, `T_BitmapAndState = 400` — verified against `src/backend/nodes/nodetags.h` (PG 18.3).
- `EXEC_FLAG_BACKWARD = 0x0008`, `EXEC_FLAG_MARK = 0x0010` — from `types-nodes::executor` (executor.h).
- c2rust extern decls confirm `tbm_intersect(a: *mut, b: *const)`, `tbm_is_empty(tbm: *const) -> bool`, `tbm_free(tbm: *mut)` — match the seam signatures wired.

## Seam audit

Owned seam crate (by c_source coverage): `backend-executor-nodeBitmapAnd-seams`
(declares the 4 cross-cycle dispatch entry points
`exec_init_bitmap_and`/`multi_exec_bitmap_and`/`exec_end_bitmap_and`/`exec_rescan_bitmap_and`).
All 4 are installed by this crate's `init_seams()` (1:1, only `set()` calls),
and `seams-init::init_all()` calls `backend_executor_nodeBitmapAnd::init_seams()`
(recurrence_guard tests pass).

Outward seams — each justified by a real cycle or unported/owner boundary, all
thin marshal+delegate (no logic in any seam path):

- `execProcnode-seams`: `exec_init_node`/`multi_exec_proc_node`/`exec_end_node`
  — execProcnode dispatches INTO this node (true cycle).
- `execAmi-seams`: `exec_re_scan` — execAmi dispatches INTO this node (cycle).
- `execUtils-seams`: `update_changed_param_set` — owner is execUtils.
- `instrument-seams`: `instr_start_node`/`instr_stop_node` — owner is instrument.c.
- `backend-nodes-core-tidbitmap-seams`: `tbm_intersect`/`tbm_is_empty`/`tbm_free`
  — tidbitmap owner. `tbm_intersect`/`tbm_is_empty` declarations added here and
  installed by `backend-nodes-core::init_seams()` (the real, already-ported
  owner — not a stub); `tbm_free` reused. The C `tbm_free(subresult)` is the
  seam call (the owned bitmap is freed via its owner, consistent with
  nodeBitmapHeapscan).

## Design conformance

- Opacity: no invented handles; `BitmapAnd`/`BitmapAndState` are real structs in
  types-nodes mirroring the C field-for-field; children are real
  `PgBox<PlanStateNode>`. No stand-in aliases.
- Allocation: the init path carries `Mcx` and returns `PgResult`; the only
  allocations (`alloc_in`, `PgVec::push`, `chgParam.clone_in`) are fallible.
- No shared statics, no ambient-global zero-arg getter seams, no locks held
  across `?`, no registry side-tables.
- No `todo!`/`unimplemented!`; the only `unreachable`-shaped paths are real
  `Err(...)` for impossible-tag/missing-slot conditions.

## Verdict: PASS

All 5 functions MATCH; all owned seams installed; no seam findings; no design
violations. `cargo check --workspace` clean.
