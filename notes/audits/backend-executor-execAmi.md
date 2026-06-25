# Audit: backend-executor-execAmi

- **Unit:** `backend-executor-execAmi`
- **C source:** `src/backend/executor/execAmi.c` (655 lines, PostgreSQL 18.3)
- **c2rust rendering:** `../pgrust/c2rust-runs/backend-executor-execAmi/src/execAmi.rs`
- **Port:** `crates/backend-executor-execAmi/src/lib.rs`
- **Supporting crates audited:** `backend-executor-execAmi-seams` (owner
  install), new `backend-executor-instrument-seams`,
  `backend-executor-nodeSubplan-seams`, `backend-access-index-amapi-seams`,
  extensions to `backend-executor-execUtils-seams` and
  `backend-utils-cache-syscache-seams`, and the `types-nodes` additions
  (pathnodes.rs, planstate/nodes tag helpers, PlanStateData fields,
  SubPlanState, Instrumentation, ExprContext, Plan.parallel_aware/extParam,
  Bitmapset::clone_in)
- **Auditor:** independent re-derivation from the C sources and headers
  (`nodetags.h` generated, `extensible.h`, `pathnodes.h`, `plannodes.h`,
  `execnodes.h`, `elog.h`), cross-checked against the c2rust rendering,
  2026-06-12

## Function inventory (every definition in execAmi.c)

execAmi.c defines exactly seven functions: one static
(`IndexSupportsBackwardScan`, declared at line 66) and six extern. The c2rust
rendering contains the same seven plus post-preprocessor header inlines
(`ObjectIdGetDatum`, `list_length`, `list_nth_cell`, `GETSTRUCT`) owned by
other units; the inlines' uses are folded into the seam projections below.

A note on the dispatch switches: the owned dispatch enums (`PlanStateNode`,
`Node`, `PathNode`) currently carry only the variants whose owning node units
exist (Material, plus the eight path variants this unit consumes). C arms for
node types with no enum variant are unrepresentable inputs in the Rust type
system; the wildcard arm reproduces the C `default:` exactly, so behavior is
provably identical on every constructible input. This is the repo convention
established by the merged `backend-executor-nodeMaterial` port ("node ports
must add arms"), not absent logic: every arm whose input can exist is present.

| # | C function | C location | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `ExecReScan` | execAmi.c:77 | `lib.rs::exec_re_scan` | MATCH | Re-derived line by line. (1) `if (node->instrument) InstrEndLoop(node->instrument)` ≡ `if let Some(instr) = ...instrument.as_deref_mut()` → `instr_end_loop` seam; the seam's `PgResult` threads the C `elog(ERROR, "InstrEndLoop called on running node")` longjmp via `?`. (2) `if (node->chgParam != NULL)` block: `foreach(l, node->initPlan)` ≡ index loop over `head.initPlan` (`None` = `NIL` = zero iterations); per element, `if (splan->plan->extParam != NULL) UpdateChangedParamSet(splan, node->chgParam)` ≡ the `splan_has_ext_param` probe + `update_changed_param_set` seam call, with `newchg` re-read from `head.chgParam` each iteration (C reads `node->chgParam` at call time — same value, including bits added by earlier iterations' `ExecReScanSetParamPlan`); then `if (splan->chgParam != NULL) ExecReScanSetParamPlan(sstate, node)` re-tested *after* the update, as in C (UpdateChangedParamSet can set `splan->chgParam`). The seam splits the C `parent` into the parts nodeSubplan.c actually consumes (`parent->chgParam` slot for `bms_add_member`, `parent->state` ≡ threaded `estate`) — verified against nodeSubplan.c:1257-1294. `foreach(l, node->subPlan)` ≡ second loop, extParam-conditional update only, no SetParamPlan — matches C. Child trees: `outerPlanState`/`innerPlanState` (`lefttree`/`righttree`) updated with the (possibly augmented) `chgParam` — matches. C derefs `sstate->planstate` and `splan->plan` unguarded (never NULL in C); the port's `None` short-circuits/`expect`s differ only on inputs that are UB in C. (3) `if (node->ps_ExprContext) ReScanExprContext(...)` ≡ `re_scan_expr_context` seam. (4) per-node switch: `T_MaterialState` → direct `nodeMaterial::ExecReScanMaterial` (direct dep, not a seam — owner is merged); `default:` → `elog(ERROR, "unrecognized node type: %d")` ≡ `Err(unrecognized_node_type(tag))` (ERROR level, default sqlstate = `ERRCODE_INTERNAL_ERROR` via `default_sqlstate_for_level`, message text identical with the tag printed as the same integer). All other C arms are unrepresentable (no enum variant). (5) trailing `if (chgParam) { bms_free; = NULL }` ≡ unconditional `= None` (no-op when already None; bms_free is allocator bookkeeping). The clear is skipped on the error paths in both (C longjmps, port `return Err`/`?`). |
| 2 | `ExecMarkPos` | execAmi.c:327 | `lib.rs::exec_mark_pos` | MATCH | `T_MaterialState` → `nodeMaterial::ExecMaterialMarkPos` (direct dep). C `default:` is the soft path: `elog(DEBUG2, "unrecognized node type: %d")`, no error — port arm evaluates `elog(DEBUG2, ...)` which returns `Ok(())` for sub-ERROR levels (DEBUG2 = 13, verified against elog.h's DEBUG5..DEBUG1 = 10..14). IndexScan/IndexOnlyScan/CustomScan/Sort/Result arms unrepresentable. |
| 3 | `ExecRestrPos` | execAmi.c:376 | `lib.rs::exec_restr_pos` | MATCH | `T_MaterialState` → `nodeMaterial::ExecMaterialRestrPos`; C `default:` is the hard `elog(ERROR, "unrecognized node type: %d")` ≡ `Err(unrecognized_node_type(tag))`. Same unrepresentable-arm note. |
| 4 | `ExecSupportsMarkRestore` | execAmi.c:418 | `lib.rs::exec_supports_mark_restore` | MATCH | Switches on `pathnode->pathtype` (data field, kept as data in `PathData`), not the node tag — exactly as C. `T_IndexScan`/`T_IndexOnlyScan` → `castNode(IndexPath, ...)->indexinfo->amcanmarkpos` ≡ variant match (panic on mismatch ≡ castNode assertion failure). `T_Material`/`T_Sort` → true. `T_CustomScan` → `flags & CUSTOMPATH_SUPPORT_MARK_RESTORE` (0x0002, verified extensible.h:85). `T_Result` → ProjectionPath recurses on `subpath`; MinMaxAggPath / GroupResultPath / plain-Path fallthrough → false (C's release behavior; the `Assert(IsA(pathnode, Path))` is debug-only, and the port's `_ => false` covers the same inputs). `T_Append`/`T_MergeAppend` → `list_length(subpaths) == 1` ≡ `subpaths.len() == 1` recursing on `linitial` ≡ `[0]`; otherwise false; castNode mismatch panics. `default:` → false. All pathtype tag constants verified (table below). |
| 5 | `ExecSupportsBackwardScan` | execAmi.c:511 | `lib.rs::exec_supports_backward_scan` | MATCH | `node == NULL → false` ≡ `Option::None → Ok(false)`. `node->parallel_aware → false` before the switch ≡ `plan_head().parallel_aware` check. Switch on `nodeTag(node)`: `T_Material` is in the "these don't evaluate tlist → true" group ≡ `Node::Material(_) => Ok(true)`. Every other C arm (Result, Append+nasyncplans, SampleScan, Gather, IndexScan/IndexOnlyScan → `IndexSupportsBackwardScan`, SubqueryScan, CustomScan flag, IncrementalSort, LockRows/Limit recursion, default → false) is unrepresentable until those `Node` variants land; the wildcard reproduces `default: return false`, the only reachable case. Return type is `PgResult<bool>` because the index arms will call the fallible `index_supports_backward_scan`. |
| 6 | `IndexSupportsBackwardScan` (static) | execAmi.c:603 | `lib.rs::index_supports_backward_scan` (private, as in C) | MATCH | `SearchSysCache1(RELOID, indexid)` + `GETSTRUCT->relam` ≡ `search_relation_relam` projection seam; cache miss (`!HeapTupleIsValid`) → the error is raised HERE, in this crate, with the exact C text `cache lookup failed for relation %u` at ERROR/`ERRCODE_INTERNAL_ERROR` (c2rust confirms level 21 + errmsg_internal). `GetIndexAmRoutineByAmId(relam, false)` + `->amcanbackward` ≡ `index_am_canbackward` projection seam (amapi.c owns the handler-lookup logic and its `noerror=false` ereports; the seam contract pins them). `pfree(amroutine)` / `ReleaseSysCache` are allocator/refcount bookkeeping owned by the installers. Currently uncalled (callers' Node variants pending) — kept private with `#[cfg_attr(not(test), allow(dead_code))]`, exercised by tests. |
| 7 | `ExecMaterializesOutput` | execAmi.c:636 | `lib.rs::exec_materializes_output` | MATCH | Pure tag-set membership: `T_Material, T_FunctionScan, T_TableFuncScan, T_CteScan, T_NamedTuplestoreScan, T_WorkTableScan, T_Sort` → true, default false. The seven-tag list matches the C case list exactly; all values verified below. |

## Constants verified against headers

All values checked against the PostgreSQL 18.3 generated
`src/backend/nodes/nodetags.h` and against the c2rust constants
(post-preprocessor ground truth):

| Constant | Port value | Header | OK |
|---|---|---|---|
| `T_Result` | 331 | nodetags.h:348 | yes |
| `T_Append` | 334 | nodetags.h:351 | yes |
| `T_MergeAppend` | 335 | nodetags.h:352 | yes |
| `T_IndexScan` | 341 | nodetags.h:358 | yes |
| `T_IndexOnlyScan` | 342 | nodetags.h:359 | yes |
| `T_FunctionScan` | 348 | nodetags.h:365 | yes |
| `T_TableFuncScan` | 350 | nodetags.h:367 | yes |
| `T_CteScan` | 351 | nodetags.h:368 | yes |
| `T_NamedTuplestoreScan` | 352 | nodetags.h:369 | yes |
| `T_WorkTableScan` | 353 | nodetags.h:370 | yes |
| `T_CustomScan` | 355 | nodetags.h:372 | yes |
| `T_Material` | 360 | nodetags.h:377 | yes |
| `T_Sort` | 362 | nodetags.h:379 | yes |
| `T_MaterialState` | 424 | nodetags.h:441 | yes |
| `CUSTOMPATH_SUPPORT_BACKWARD_SCAN` | 0x0001 | extensible.h:84 | yes |
| `CUSTOMPATH_SUPPORT_MARK_RESTORE` | 0x0002 | extensible.h:85 | yes |
| `DEBUG2` | 13 | elog.h (DEBUG5..DEBUG1 = 10..14) | yes |
| bare-`elog(ERROR)` sqlstate | `ERRCODE_INTERNAL_ERROR` | via `PgError::error` / `default_sqlstate_for_level` | yes |

`types-nodes` additions checked against the C headers: `PathData.pathtype`,
`IndexOptInfo.amcanmarkpos`, `CustomPath.flags` (uint32), `ProjectionPath.subpath`,
`AppendPath.subpaths` / `MergeAppendPath.subpaths` (pathnodes.h);
`Plan.parallel_aware` / `Plan.extParam` (plannodes.h); `PlanStateData`
`instrument`/`righttree`/`initPlan`/`subPlan`/`ps_ExprContext` and
`SubPlanState.planstate` (execnodes.h) — all trimmed to consumed fields with
faithful names; `Plan::clone_in` extended to deep-copy the two new fields.
`Bitmapset::clone_in` is `bms_copy` (nwords + word copy), storage-only as
documented.

## Seam audit

Owner install:

- `backend-executor-execAmi-seams::exec_re_scan` was declared pending by the
  merged nodeMaterial port; this crate's `init_seams()` is exactly one `set()`
  installing `exec_re_scan`, and `seams-init::init_all()` calls it
  (seams-init/src/lib.rs:12). The signature reconciliation (explicit `estate`,
  `PgResult<()>`) matches what nodeMaterial's call sites already used — those
  call sites are unchanged on this branch.
- No `set()` on any of these seams occurs outside the owner except test-local
  mocks inside `#[cfg(test)]` modules (execAmi tests, nodeMaterial tests) —
  the established test pattern, not production wiring.

Outward seams — every owner unit is `todo` in CATALOG.tsv, so a direct dep
cannot exist; every call site is thin marshal + delegate with no branching or
node construction in the seam path:

- `backend-executor-instrument-seams::instr_end_loop` (new): one signature
  over `&mut Instrumentation`; the C `InstrEndLoop` error surfaces as
  `PgResult`. Called at the exact C point.
- `backend-executor-nodeSubplan-seams` (new): `update_changed_param_set`
  (the `Mcx` parameter is the owned-model home for the C
  `CurrentMemoryContext` allocations of `bms_intersect`/`bms_join`, the
  per-query context as in the executor) and `exec_re_scan_set_param_plan`
  (C `parent` split into its consumed parts `parent->chgParam` +
  `parent->state`, verified against nodeSubplan.c). Declarations only.
- `backend-access-index-amapi-seams::index_am_canbackward` (new):
  caller-shaped projection of `GetIndexAmRoutineByAmId(amoid, false)
  ->amcanbackward`; the handler-lookup logic and its ereports belong to
  amapi.c (the owner) and the contract pins `noerror = false`. The `pfree`
  of the returned routine is the installer's allocation bookkeeping.
- `backend-executor-execUtils-seams::re_scan_expr_context` (extended): one
  signature over `&mut ExprContext`; the reset logic stays with execUtils.c.
- `backend-utils-cache-syscache-seams::search_relation_relam` (extended):
  RELOID lookup projected to `Form_pg_class.relam`; cache miss is `Ok(None)`
  and the `cache lookup failed for relation %u` error is raised in THIS crate,
  as in C. `ReleaseSysCache` stays with the installer.

Direct deps (correctly NOT seams): `backend-executor-nodeMaterial` for the
three `T_MaterialState` dispatch arms — the owner is merged, no cycle (it
calls back into execAmi only through the `exec_re_scan` seam, which exists
precisely to break that cycle).

No seam findings.

## Build & tests

`cargo build --workspace` clean; `cargo test -p backend-executor-execAmi`
passes (6 tests exercising the param-propagation walk, mark/restore dispatch,
the full `ExecSupportsMarkRestore` truth table, backward-scan checks including
the parallel_aware gate, the syscache-miss error text, and the
`ExecMaterializesOutput` tag table); `cargo test -p
backend-executor-nodeMaterial` still passes (9 tests — the seam-signature
reconciliation did not disturb the merged caller).

## Verdict

**PASS.** All 7/7 functions `MATCH`; constants verified against the generated
headers; seam wiring complete and clean. Unit marked `audited` in CATALOG.tsv.
