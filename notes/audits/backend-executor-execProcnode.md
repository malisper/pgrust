# Audit: backend-executor-execProcnode

- **Unit:** backend-executor-execProcnode (`src/backend/executor/execProcnode.c`)
- **Branch:** port/backend-executor-execProcnode
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context)
- **Verdict:** PASS

Independent audit (re-derived from the C source, the c2rust run under
`c2rust-runs/backend-executor-execProcnode/`, and the port). execProcnode.c is
the executor's node-dispatch layer: thin `nodeTag` switches fanning out to each
`node*.c` owner's `ExecInit*`/`Exec*`/`MultiExec*`/`ExecEnd*`/`ExecShutdown*`
routines, plus the `ExecProcNode` wrapper machinery. Per the repo's owned model
the C `PlanState *` is the `PlanStateNode` tagged enum, the C `nodeTag` switches
become `match` arms, the `PlanState.state` back-pointer is threaded as
`&mut EStateData`, and each per-node owner is reached through that owner's
per-node seam (loud panic until it lands).

## 1. Function inventory

All function definitions in execProcnode.c (cross-checked against the c2rust
run, which renders exactly these nine):

| # | C function (line) | Kind | Port location | Verdict |
|---|---|---|---|---|
| 1 | `ExecInitNode` (142) | extern | `execProcnode_init.rs::exec_init_node` | MATCH |
| 2 | `ExecSetExecProcNode` (430) | extern | `execProcnode_init.rs::ExecSetExecProcNode` | MATCH (fixed) |
| 3 | `ExecProcNodeFirst` (448) | static | `execProcnode_run_end.rs::exec_proc_node_first` | MATCH |
| 4 | `ExecProcNodeInstr` (479) | static | `execProcnode_run_end.rs::exec_proc_node_instr` | MATCH |
| 5 | `MultiExecProcNode` (507) | extern | `execProcnode_run_end.rs::multi_exec_proc_node` | MATCH |
| 6 | `ExecEndNode` (562) | extern | `execProcnode_run_end.rs::exec_end_node` | MATCH |
| 7 | `ExecShutdownNode` (772) | extern | `execProcnode_run_end.rs::exec_shutdown_node` | MATCH |
| 8 | `ExecShutdownNode_walker` (778) | static | `execProcnode_run_end.rs::exec_shutdown_node_walker` | SEAMED |
| 9 | `ExecSetTupleBound` (848) | extern | `execProcnode_run_end.rs::exec_set_tuple_bound` | MATCH (ported this round) |

`ExecProcNode` itself is the `executor.h` inline macro (`node->ExecProcNode(node)`),
realized as the always-installed `exec_proc_node` seam entry that invokes the
node's installed callback. The inline `outerPlanState`/`innerPlanState`/`IsA`
helpers are localized.

## 2. Per-function comparison

### 1. `ExecInitNode` — MATCH
- `if (node == NULL) return NULL` → `let Some(node) = node else { return Ok(None) }`.
- `check_stack_depth()` → `stack_depth::check_stack_depth::call()?` (utils/misc/stack_depth.c, seamed neighbor).
- The 35-arm `switch (nodeTag(node))` is a `match` over the `Node` enum. Each arm
  routes to the owning node unit's `ExecInit*`. None of those owners has declared
  an `ExecInit*` seam in this scaffold, so every present arm is a loud
  unported-owner `panic!` naming the owner + routine (the "Mirror PG and panic"
  rule); the `Node` enum is non-exhaustive, so tags with no variant fall to the
  catch-all = the C `default: elog(ERROR, "unrecognized node type: %d")`
  (`unrecognized_node_type`, ERRCODE_INTERNAL_ERROR — matches a bare `elog(ERROR)`).
  Arms swap to real seam calls as each node owner lands. **Panic on unported
  callee is allowed; the dispatch structure itself is faithfully present.**
- `ExecSetExecProcNode(result, result->ExecProcNode)` → passes the
  ExecInit*-installed callback through (see #2).
- `initPlan` walk: the trimmed `Plan` struct does not model `List *initPlan`;
  `node_has_init_plan()` returns `false` (the common C `NIL` no-op walk), and
  fires the `ExecInitSubPlan` unported-owner panic only once the field lands
  and is non-empty. Faithful: a leaf with no initplans is the C `foreach` over
  `NIL`.
- Instrumentation tail: `if (estate->es_instrument) result->instrument =
  InstrAlloc(...)` → `if estate.es_instrument != 0 { panic!(InstrAlloc unported) }`.
  `InstrAlloc` (instrument.c) has no seam declared; routed through that owner.
  The es_instrument == 0 fast path (no instrumentation requested) is the
  reachable no-op, faithful.

### 2. `ExecSetExecProcNode` — MATCH (fixed this round)
- **Finding (fixed): DIVERGES → MATCH.** The merged init family shipped this as
  a no-op claiming `PlanStateData` carried no `ExecProcNodeReal` field and no
  `ExecProcNodeFirst` slot. The struct *does* carry both `ExecProcNode` and
  `ExecProcNodeReal` (execnodes.rs:425/432), and the run/teardown family's
  `exec_proc_node_first`/`exec_proc_node_instr` depend on them. The no-op left
  `ExecProcNode` pointing straight at the real routine and `ExecProcNodeReal`
  unset, so the first-call stack-depth check + instrumentation swap would never
  run — a behavioral divergence from C.
- Fixed to the faithful body: `node->ExecProcNodeReal = function;
  node->ExecProcNode = ExecProcNodeFirst;` → `ps_head_mut().ExecProcNodeReal =
  function; ps_head_mut().ExecProcNode = Some(exec_proc_node_first)`. The call
  site passes `result.ps_head().ExecProcNode` (the C `result->ExecProcNode`).

### 3. `ExecProcNodeFirst` — MATCH
- `check_stack_depth()` → seam call.
- `if (node->instrument) node->ExecProcNode = ExecProcNodeInstr; else
  node->ExecProcNode = node->ExecProcNodeReal;` → exact branch on
  `instrument.is_some()`, writing `Some(exec_proc_node_instr)` / `ExecProcNodeReal`.
- `return node->ExecProcNode(node)` → re-read + dispatch.

### 4. `ExecProcNodeInstr` — MATCH
- `InstrStartNode(node->instrument)` → `instr_start_node::call(instr)?`.
- `result = node->ExecProcNodeReal(node)` → dispatch through `ExecProcNodeReal`.
- `InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : 1.0)` →
  `n_tuples = if result.is_none() { 0.0 } else { 1.0 }` then `instr_stop_node`.
  `TupIsNull` ≡ slot is NULL ≡ `Option::None`. Faithful.

### 5. `MultiExecProcNode` — MATCH
- `check_stack_depth()` / `CHECK_FOR_INTERRUPTS()` → seam calls in order.
- `if (node->chgParam != NULL) ExecReScan(node)` → `if chgParam.is_some() {
  execAmi::exec_re_scan::call(...) }` (execAmi neighbor, seamed).
- The 4-arm switch: `T_HashState` is a recognized arm (loud panic routing to
  nodeHash `MultiExecHash`; that owner has not exposed a `Node`-returning
  multiexec seam yet). `T_BitmapIndexScanState`/`T_BitmapAndState`/
  `T_BitmapOrState` state variants are not yet in `PlanStateNode`, so their tags
  cannot occur; `default: elog(ERROR, "unrecognized node type")` is the
  catch-all. Faithful — the dispatch is present; the unported-owner arm panics.
- Return type narrowed from C `Node *` to the owned seam's `TIDBitmap` (the lone
  landed multiexec consumer, nodeBitmapHeapscan, always demands a `TIDBitmap`,
  folding its `IsA(result, TIDBitmap)` guard into the seam type). No reachable
  arm produces a value (all `MultiExec*` owners unported), so the narrowing is
  behaviorally inert today. Recorded as inherited debt (Findings).

### 6. `ExecEndNode` — MATCH
- `if (node == NULL) return` leaf guard handled by callers (the typed
  `&mut PlanStateNode` is always non-NULL; C `if (child) ExecEndNode(child)`
  call sites elide the recursion for absent children).
- `check_stack_depth()` → seam call.
- `if (node->chgParam != NULL) { bms_free(node->chgParam); node->chgParam = NULL; }`
  → `chgParam.take()` then `nodes_core::bms_free::call(chg)`. The `take()`
  performs the C `= NULL` and hands the freed set to bms_free. Faithful.
- The ~40-arm teardown switch: arms for the landed state variants
  (Append/Material/MergeAppend/MergeJoin/Memoize/IndexOnlyScan/Limit/Sort/
  TableFuncScan/NestLoop/HashJoin/SeqScan/ForeignScan/Hash) loud-panic routing
  to the owner's `ExecEnd*` seam; the remaining C arms (incl. the no-cleanup
  `T_ValuesScanState`/`T_NamedTuplestoreScanState`/`T_WorkTableScanState`) are
  state variants not yet in the non-exhaustive `PlanStateNode`, so their tags
  cannot occur; the catch-all = C `default: elog(ERROR)`. Faithful.

### 7. `ExecShutdownNode` — MATCH
- `(void) ExecShutdownNode_walker(node, NULL)` → `exec_shutdown_node_walker(node,
  estate)?; Ok(())`. Faithful thin driver.

### 8. `ExecShutdownNode_walker` — SEAMED (legitimate)
- `if (node == NULL) return false` handled by callers (typed ref non-NULL).
- `check_stack_depth()` is performed.
- The remainder — `if (node->instrument && node->instrument->running)
  InstrStartNode(...)`, the central `planstate_tree_walker(node,
  ExecShutdownNode_walker, context)` recursion, the 6-arm `ExecShutdown*`
  dispatch (`T_GatherState`/`T_ForeignScanState`/`T_CustomScanState`/
  `T_GatherMergeState`/`T_HashState`/`T_HashJoinState`), and the trailing
  `InstrStopNode(.., 0)` — is a single loud `panic!` documenting the seamed
  dependencies. This is a legitimate seam-and-panic, **not** MISSING own logic:
  the structural core is the callback recursion through `planstate_tree_walker`
  (nodeFuncs.c, an unported neighbor) that must call the Rust walker back; only
  the opaque-handle walker in execParallel-support exists, not the typed
  `PlanStateNode` walker. Additionally the `Instrumentation.running` field
  required by the instrument bracket is not modeled on the trimmed struct, and
  no per-node `ExecShutdown*` seam is declared. The body cannot be threaded
  until the typed walker + the per-node shutdown seams land. Panicking on an
  unported callee is permitted; the function's own structure (stack-depth check
  + the documented body) is present, not silently absent.

### 9. `ExecSetTupleBound` — MATCH (ported this round)
- **Finding (fixed): MISSING → MATCH.** Only the `exec_set_tuple_bound` seam was
  declared (by execParallel/execParallel-support); no body existed in the unit.
- Ported the full C cascade: `IsA(child_node, SortState)` → set/clear
  `bounded`/`bound` on the negative-vs-nonneg branch; `IsA(.., AppendState)` →
  `for i in 0..as_nplans { recurse(appendplans[i]) }`; `IsA(.., MergeAppendState)`
  → `for i in 0..ms_nplans { recurse(mergeplans[i]) }`. The remaining C arms
  (`IncrementalSortState`, projecting `ResultState`→outerPlanState,
  `SubqueryScanState` with NULL qual→ss.subplan, `GatherState`/`GatherMergeState`
  record `tuples_needed`+descend) operate on state variants not yet in the
  non-exhaustive `PlanStateNode`, so their tags cannot occur; documented and
  added as their units land. The final fall-through (any other node = stop
  propagation) is the `_ => {}` no-op. Field names/loop bounds verified against
  the C (`as_nplans`/`appendplans`, `ms_nplans`/`mergeplans`, `bounded`/`bound`).
  Negative-means-no-limit semantics preserved.

## 3. Seam audit

**Owned seam crate (by C-source coverage of execProcnode.c):**
`crates/backend-executor-execProcnode-seams`.

`init_seams()` installs every declaration that maps to an execProcnode.c
function, each exactly once, with nothing but `set()` calls:
- `exec_init_node` ← `ExecInitNode`
- `exec_proc_node` ← `ExecProcNode` (executor.h macro)
- `exec_end_node` ← `ExecEndNode`
- `multi_exec_proc_node` ← `MultiExecProcNode` (installed this round)
- `exec_set_tuple_bound` ← `ExecSetTupleBound` (installed this round)

`seams-init::init_all()` calls `backend_executor_execProcnode::init_seams()`
(seams-init/src/lib.rs:27).

**Parked non-execProcnode.c declarations (correctly left uninstalled):** the
same seam crate also carries `mark_param_execplan_pending`,
`clear_param_execplan`, `param_execplan_pending`,
`exec_set_param_plan_for_pending`, and `link_subplan_planstate`. These were
parked here by the nodeSubplan port; they are **not** execProcnode.c functions —
their bodies operate on the `ParamExecData.execPlan` link (not modeled on the
trimmed struct) and `es_subplanstates`, which belong to the executor's
PARAM_EXEC / initplan machinery (execMain), not the node-dispatch layer. They
have no counterpart C function in this unit and no body to install here; they
remain pending their true owner. This is a pre-existing cross-unit seam-parking
arrangement, not a gap introduced by this unit, and installing them would
require modeling executor-param state owned elsewhere. Documented in
`init_seams()`.

**Outward seam calls** are all thin marshal+delegate to real unported-neighbor
dependencies (no branching/computation in a seam path):
- stack-depth (utils/misc/stack_depth.c), tcop-postgres (check_stack_depth /
  check_for_interrupts), execAmi (ExecReScan), instrument (InstrStartNode/
  InstrStopNode), nodes-core (bms_free). Each is a genuine dependency the
  dispatch layer cannot avoid.

No seam path contains node construction or computation; no `set()` outside the
owner; no uninstalled owned-execProcnode seam.

## 3b. Design conformance

- **opacity-inherited-never-introduced:** no invented opaque handles; the port
  uses the real `PlanStateNode`/`Node`/`EStateData` types and the concrete
  `Instrumentation` struct. `multi_exec_proc_node`'s `TIDBitmap` return is the
  real tidbitmap type, not a stand-in.
- **Mcx + PgResult on allocating fns/seams:** `exec_init_node` takes `Mcx` and
  returns `PgResult`; all fallible dispatch entries return `PgResult`.
- **No shared statics for per-backend globals; no ambient-global seams.** State
  is threaded via `&mut EStateData`.
- **No locks held across `?`**, no registry-shaped side tables.
- **Mirror PG and panic** honored: unported node owners are reached through loud
  per-owner panics that mirror the C switch arm-for-arm; no restructuring to
  dodge a dependency, no silent stubbing of own logic. The two functions that
  were absent/divergent own-logic (`ExecSetTupleBound` MISSING,
  `ExecSetExecProcNode` DIVERGES) were fixed this round, not deferred.
- **Seam signatures mirror the C failure surface:** entries that can
  `ereport(ERROR)` (every dispatch + the stack/interrupt checks) return
  `PgResult`; the infallible `ExecSetExecProcNode`/`ExecSetTupleBound`-leaf
  paths match.

## 4. Verdict

**PASS.** All nine execProcnode.c functions are present with logic matching the
C: seven MATCH, one MATCH ported this round (`ExecSetTupleBound`), one
SEAMED-by-legitimate-unported-neighbor (`ExecShutdownNode_walker`, blocked on
the typed `planstate_tree_walker` + per-node `ExecShutdown*` seams). The two
gaps found during assembly — a divergent no-op `ExecSetExecProcNode` and a
MISSING `ExecSetTupleBound` — were fixed and re-derived from scratch above. All
owned seams that map to an execProcnode.c function are installed by
`init_seams()`, which is wired into `seams-init::init_all()`. `cargo check
--workspace` and `cargo test --workspace` are green. No `todo!()`/`unimplemented!()`
in own logic; the unported-owner arms loud-panic per the repo rules.
</content>
</invoke>
