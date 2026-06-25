# Audit: backend-executor-nodeMaterial

- **Unit:** `backend-executor-nodeMaterial`
- **C source:** `src/backend/executor/nodeMaterial.c` (363 lines, PostgreSQL 18.3)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-executor-nodeMaterial/src/nodeMaterial.rs`
- **Port:** `crates/backend-executor-nodeMaterial/src/lib.rs`
- **Supporting crates audited:** `types-nodes` (node/executor vocabulary),
  `backend-utils-sort-storage-seams`, `backend-executor-execProcnode-seams`,
  `backend-executor-execAmi-seams`, `backend-executor-execTuples-seams`,
  `backend-executor-execUtils-seams`, `backend-tcop-postgres-seams`,
  `backend-utils-init-small-seams`
- **Auditor:** independent re-derivation from the C sources and headers
  (`executor.h`, `tuptable.h`, `sdir.h`, `execnodes.h`, `plannodes.h`,
  `miscadmin.h`), 2026-06-12

## Function inventory (every definition in nodeMaterial.c)

nodeMaterial.c defines exactly six functions: one static (`ExecMaterial`) and
five extern. The c2rust rendering additionally contains four post-preprocessor
header inlines (`newNode` from nodes.h/palloc, `ExecClearTuple` / `ExecCopySlot`
from tuptable.h, `ExecProcNode` from executor.h) — those are owned by other
units; their handling is covered in the seam audit below.

| # | C function | C location | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `ExecMaterial` (static) | nodeMaterial.c:39 | `lib.rs::ExecMaterial` + `lib.rs::exec_material_node` | MATCH | Re-derived line by line against the C and the c2rust rendering. `CHECK_FOR_INTERRUPTS()` → `check_for_interrupts` seam (the C macro's pending-flag test plus `ProcessInterrupts()`; the `ereport(ERROR/FATAL)` longjmp surfaces as `Err`, threaded by `?` exactly where C would longjmp past the rest of the function). First-time tuplestore creation under the identical predicate `tuplestorestate == NULL && node->eflags != 0`: `tuplestore_begin_heap(true, false, work_mem)` (work_mem read via the globals seam at the same point), `tuplestore_set_eflags(node->eflags)`, and under `eflags & EXEC_FLAG_MARK` the extra read pointer with `Assert(ptrno == 1)` → `debug_assert_eq!`. `eof_tuplestore = (NULL) || tuplestore_ateof` → `match as_deref() { None => true, Some => ateof }`. Backward-at-EOF branch: `!forward && eof_tuplestore`, inner `!node->eof_underlying` guard, `tuplestore_advance(ts, forward)` false → C `return NULL` ≡ `Ok(false)` (no slot clear in either), then `eof_tuplestore = false` outside the inner guard — placement matches. C derefs `tuplestorestate` unguarded here; the port's `.expect` panics on the same impossible input. Fetch branch: `!eof_tuplestore` → `tuplestore_gettupleslot(ts, forward, false, slot)` true → return slot (`Ok(true)`, slot id = `ps_ResultTupleSlot`); false and `forward` → `eof_tuplestore = true` (no flag change when backward, matching C's fallthrough to the final clear). Subplan branch: `eof_tuplestore && !node->eof_underlying` → `ExecProcNode(outerPlanState(node))` via seam; `TupIsNull(outerslot)` (`NULL || TTS_EMPTY`, verified tuptable.h:310/96) ≡ `None` or `is_empty()` → set `eof_underlying = true`, return NULL ≡ `Ok(false)`; else `if (tuplestorestate) tuplestore_puttupleslot` then `ExecCopySlot(slot, outerslot); return slot` ≡ `slot_pair_mut` + `exec_copy_slot` seam + `Ok(true)`. Final path: `return ExecClearTuple(slot)` ≡ clear via seam then `Ok(false)` — the slot ends empty in both; C returns the cleared (empty) pointer where the owned dispatch convention returns `None`, indistinguishable to any `TupIsNull`-using caller (all of them). `exec_material_node` is the installed `ExecProcNode` callback: `castNode(MaterialState, pstate)` ≡ match arm with loud panic on tag mismatch, exactly C's `castNode` assertion. |
| 2 | `ExecInitMaterial` | nodeMaterial.c:163 | `lib.rs::ExecInitMaterial` | MATCH | `makeNode(MaterialState)` (palloc0 + tag) ≡ `Box<MaterialState::default()>` — every field's default re-checked against zeroing (None/0/false). `ss.ps.plan = (Plan *) node` ≡ owned clone of the plan node (plans are read-only during execution; clone is observationally identical). `ss.ps.state = estate` replaced by explicit `&mut EStateData` threading (crate-wide model). `ExecProcNode = ExecMaterial` ≡ `Some(exec_material_node)`. `eflags & (REWIND|BACKWARD|MARK)` with REWIND=0x0004, BACKWARD=0x0008, MARK=0x0010 verified against executor.h:67-69; BACKWARD ⇒ `|= REWIND`; `eof_underlying = false`; `tuplestorestate = NULL`; child shielding `eflags &= ~(REWIND|BACKWARD|MARK)`; `outerPlanState(matstate) = ExecInitNode(outerPlan(node), estate, eflags)` ≡ `lefttree = exec_init_node(node.plan.lefttree.as_deref(), ...)` (seam returns `None` for NULL child, matching C's NULL-in/NULL-out); `ExecInitResultTupleSlotTL(&ss.ps, &TTSOpsMinimalTuple)` ≡ seam with `TupleSlotKind::MinimalTuple` (the C `&TTSOps*` pointer-identity token); `ps_ProjInfo = NULL`; `ExecCreateScanSlotFromOuterPlan(estate, &ss, &TTSOpsMinimalTuple)` ≡ seam. Statement order identical. |
| 3 | `ExecEndMaterial` | nodeMaterial.c:240 | `lib.rs::ExecEndMaterial` | MATCH | `if (tuplestorestate != NULL) tuplestore_end(...)` then unconditional `= NULL` ≡ `take()` + seam (when None, C's NULL store stays NULL — identical). `ExecEndNode(outerPlanState(node))` ≡ seam on `lefttree` (C derefs the possibly-NULL pointer inside ExecEndNode, which handles NULL; the port's `.expect` panics only if init never ran, the same impossible state). |
| 4 | `ExecMaterialMarkPos` | nodeMaterial.c:262 | `lib.rs::ExecMaterialMarkPos` | MATCH | `Assert(eflags & MARK)` → `debug_assert!`; not-materialized early return; `tuplestore_copy_read_pointer(ts, 0, 1)` then `tuplestore_trim(ts)` — same order, same constants. |
| 5 | `ExecMaterialRestrPos` | nodeMaterial.c:290 | `lib.rs::ExecMaterialRestrPos` | MATCH | `Assert(eflags & MARK)`; early return; `tuplestore_copy_read_pointer(ts, 1, 0)` — src/dest reversed from MarkPos exactly as in C. |
| 6 | `ExecReScanMaterial` | nodeMaterial.c:313 | `lib.rs::ExecReScanMaterial` | MATCH | `ExecClearTuple(ps_ResultTupleSlot)` first, unconditionally. `eflags != 0` branch: not-materialized early return; `outerPlan->chgParam != NULL || (eflags & REWIND) == 0` → `tuplestore_end` + NULL (≡ `take()`), then `if (chgParam == NULL) ExecReScan(outerPlan)` (seam), `eof_underlying = false`; else `tuplestore_rescan`. The port hoists the chgParam read before `tuplestore_end`; `tuplestore_end` cannot touch `chgParam`, so both reads see the same value. `eflags == 0` branch: `if (chgParam == NULL) ExecReScan(outerPlan)`, `eof_underlying = false`. All predicates, ordering, and state writes identical. |

## Constants verified against headers

| Constant | Port value | Header | OK |
|---|---|---|---|
| `EXEC_FLAG_REWIND` | 0x0004 | executor.h:67 | yes |
| `EXEC_FLAG_BACKWARD` | 0x0008 | executor.h:68 | yes |
| `EXEC_FLAG_MARK` | 0x0010 | executor.h:69 | yes |
| `TTS_FLAG_EMPTY` | 1 << 1 | tuptable.h:95 | yes |
| `TupIsNull` | `None \|\| is_empty()` | tuptable.h:310/96 | yes |
| `Backward/NoMovement/ForwardScanDirection` | −1 / 0 / +1 | sdir.h:26-28 | yes |
| `ScanDirectionIsForward` | `== ForwardScanDirection` | sdir.h:64-65 | yes |
| tuplestore read pointers (active=0, mark=1) | 0 / 1 | nodeMaterial.c:74-76, 275, 303 | yes |

`MaterialState` / `Material` / `Plan` / `PlanState` / `ScanState` / `EState`
field sets in `types-nodes` checked against execnodes.h / plannodes.h: trimmed
to consumed fields, names and comments faithful, `Default` ≡ `makeNode`
zeroing for every retained field.

## Seam audit

Every outward call crosses into an unported owner unit, so each seam is
justified by a real missing dependency (a direct dep does not exist to take):

- `backend-utils-sort-storage-seams` (tuplestore.c owner): all 11 declarations
  (`begin_heap`, `set_eflags`, `alloc_read_pointer`, `ateof`, `advance`,
  `gettupleslot`, `puttupleslot`, `copy_read_pointer`, `trim`, `rescan`,
  `end`) are pure signatures over the opaque `Tuplestorestate` carrier
  (`types_nodes::funcapi`, type-erased payload only the owner names). No logic.
- `backend-executor-execProcnode-seams`: `exec_init_node`, `exec_proc_node`,
  `exec_end_node`. The executor.h inline `ExecProcNode` (chgParam-triggered
  `ExecReScan` + callback dispatch, visible in the c2rust rendering) is owned
  by the execProcnode/executor.h unit and lives behind this seam — none of its
  logic was duplicated or dropped in this crate.
- `backend-executor-execAmi-seams`: `exec_re_scan`. Signature only.
- `backend-executor-execTuples-seams`: `exec_init_result_tuple_slot_tl`,
  `exec_clear_tuple`, `exec_copy_slot` (the tuptable.h virtual-dispatch
  inlines whose `TupleTableSlotOps` tables execTuples.c owns). Signatures only.
- `backend-executor-execUtils-seams`: `exec_create_scan_slot_from_outer_plan`.
  Signature only.
- `backend-tcop-postgres-seams`: `check_for_interrupts` (the miscadmin.h
  macro + `ProcessInterrupts`, owned by tcop/postgres.c). Signature only.
- `backend-utils-init-small-seams`: `work_mem` (globals.c GUC read).
  Signature only.

All seam call sites in `lib.rs` are thin: argument conversion (slot-id →
`&mut TupleTableSlot`, `Option` unwrap mirroring a C deref), one `call`, result
conversion. No branching, node construction, or computation occurs inside a
seam path.

Wiring: `grep` over `crates/` finds zero `::set(` calls anywhere (no
out-of-owner installs; the owners are unported, so every declared slot panics
loudly on call — the prescribed behavior). nodeMaterial reaches outward only
through per-owner seam crates and so needs no `<unit>-seams` crate of its own;
its `init_seams()` is empty and is invoked by `seams-init::init_all()`
(`crates/seams-init/src/lib.rs`), which contains nothing but `init_seams()`
calls. `exec_material_node` is installed into `PlanState.ExecProcNode` by
`ExecInitMaterial` itself, exactly as the C assigns the function pointer — not
a seam.

## Build / test

`cargo build --workspace` and `cargo test --workspace` clean (68 tests pass,
0 fail).

## Spot-check of MATCH verdicts

`ExecMaterial` and `ExecReScanMaterial` (the two functions with non-trivial
control flow) were re-derived a second time directly against the c2rust
rendering (`nodeMaterial.rs:1635` and `:1773`), branch by branch, including
the backward-at-EOF extra-advance placement, the `forward`-gated
`eof_tuplestore = true`, the `tuplestore_puttupleslot` NULL-store guard, and
the chgParam/REWIND rescan predicate. No divergence found.

## Verdict

**PASS** — all 6 functions MATCH (with outward calls SEAMED per the rules);
zero seam findings.
