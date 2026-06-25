# Audit: backend-executor-nodeSubplan

Unit: `backend-executor-nodeSubplan` (`src/backend/executor/nodeSubplan.c`,
1321 lines, PostgreSQL 18.3).
Crate audited: `crates/backend-executor-nodeSubplan` and its owned seam crate
`crates/backend-executor-nodeSubplan-seams`.
Cross-checked against
`../pgrust/c2rust-runs/backend-executor-nodeSubplan/src/nodeSubplan.rs`.
Auditor: independent re-derivation from the C source and headers
(`nodes/primnodes.h` SubLinkType, `access/sdir.h` ScanDirection,
`utils/errcodes.txt` ERRCODE_CARDINALITY_VIOLATION). This audit was performed
fresh from the C; it supersedes the earlier PASS report on this branch, which it
disagrees with on F2 and F3 below.

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (`claude-opus-4-8[1m]`)
- **Top-line verdict: PASS** (re-derived after F2 and F3 were fixed at root; see
  the "Resolution" note under each finding and §4).

## 1. Function inventory — nodeSubplan.c (every definition)

C defines 12 functions: 5 statics forward-declared at lines 42-52
(`ExecHashSubPlan`, `ExecScanSubPlan`, `buildSubPlanHash`, `findPartialMatch`,
`slotAllNulls`, `slotNoNulls` — six static prototypes, `execTuplesUnequal` is
defined without a top prototype) plus the externally-linked routines. Counting
every definition: 12. All 12 appear in the c2rust rendering (no extra `#if`-gated
defs) and in the Rust port.

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ExecSubPlan` (:61) | `lib.rs::ExecSubPlan` (:83) | MATCH | CHECK_FOR_INTERRUPTS via tcop seam; CTE_SUBLINK and "setParam && !MULTIEXPR" elog(ERROR) guards; forces ForwardScanDirection, dispatches useHashTable→Hash else Scan, restores `dir`. `(Datum,bool)` = C `retval` + `*isNull`. Constants verified (c2rust :2195-2281). |
| 2 | `ExecHashSubPlan` (:100) | `lib.rs::ExecHashSubPlan` (:128) | MATCH | parParam/args guard; rebuild predicate `hashtable==NULL \|\| chgParam!=NULL`; empty-subplan short FALSE; LHS-no-null exact-probe→TRUE then partly-null UNKNOWN; partly/wholly-NULL `if/else-if` cascade (C:173-183) preserved in order; clears proj slot + resets hashtempcxt. Cascade matches c2rust :2329-2360 exactly. |
| 3 | `ExecScanSubPlan` (:203) | `lib.rs::ExecScanSubPlan` (:202) | MATCH | Was DIVERGES (F3, ARRAY_SUBLINK memory context); fixed — the array now builds in the caller's per-tuple context. All other logic (per-type arms, ANY-OR/ALL-AND combine + early breaks, cardinality raises, not-found empty handling) matches. |
| 4 | `buildSubPlanHash` (:476) | `lib.rs::buildSubPlanHash` (:371) | MATCH | Assert ANY_SUBLINK; reset hashtablecxt + clear have*rows; `nbuckets=clamp_cardinality_to_long(plan_rows)` clamped ≥1; main build/reset; `!unknownEqFalse` nulls branch (`ncols==1→1` else `/16` clamp ≥1), else clear hashnulls; rescan; scan loop loading params + projRight, slotNoNulls→main / else hashnulls, reset inner ecxt + hashtempcxt; clear projRight slot. |
| 5 | `execTuplesUnequal` (:657) | `lib.rs::execTuplesUnequal` (:478) | MATCH | Reset eval ctx; `for(i=numCols; --i>=0;)` decrement-then-test; null1/null2 `continue`; FunctionCall2Coll with `collations[i]`; first not-equal → true + break. Last-to-first order preserved. |
| 6 | `findPartialMatch` (:726) | `lib.rs::findPartialMatch` (:528) | MATCH | Whole-table scan; Init/Scan iterator loop (ExecStoreMinimalTuple folded into `scan_tuple_hash_table`); CHECK_FOR_INTERRUPTS per entry; `!execTuplesUnequal` → Term + true; no Term on fall-through. |
| 7 | `slotAllNulls` (:761) | `lib.rs::slotAllNulls` (:558) | MATCH | `i in 1..=ncols`, false on first non-null, else true. |
| 8 | `slotNoNulls` (:781) | `lib.rs::slotNoNulls` (:575) | MATCH | `i in 1..=ncols`, false on first null, else true. |
| 9 | `ExecInitSubPlan` (:809) | `lib.rs::ExecInitSubPlan` (:601) | MATCH | Was PARTIAL (F2); fixed — the `useHashTable` branch now allocates and writes its own control arrays in-crate, with only the catalog lookups and execExpr projection assembly seamed. Non-hash spine matches. |
| 10 | `ExecSetParamPlan` (:1083) | `lib.rs::ExecSetParamPlan` (:696) | MATCH | ANY/ALL, CTE, parParam/args guards; force forward dir; ARRAY init in es_query_cxt (= C `econtext->ecxt_per_query_memory`, faithful here); scan loop with EXISTS one-shot break, ARRAY accumulate, multi-row guard for EXPR/MULTIEXPR/ROWCOMPARE, copy curTuple + set setParams clearing execPlan; ARRAY result + pfree-old-curArray; not-found EXISTS→false / else NULL; restore dir. |
| 11 | `ExecSetParamPlanMulti` (:1259) | `lib.rs::ExecSetParamPlanMulti` (:822) | MATCH | `paramid=-1; while bms_next_member>=0`; if execPlan pending re-enter ExecSetParamPlan via seam, then assert cleared. |
| 12 | `ExecReScanSetParamPlan` (:1282) | `lib.rs::ExecReScanSetParamPlan` (:850) | MATCH | parParam / empty-setParam / empty-extParam elog guards; no rescan; per-setParam mark-dirty (skip for CTE) + `parent->chgParam = bms_add_member`. `parent` split into `&mut chgParam` + estate. Installed by `init_seams()`. |

## 2. Seam / wiring audit

**Owned seam crates (by C-source coverage):** the only `*-seams` crate mapping
to `nodeSubplan.c` is `crates/backend-executor-nodeSubplan-seams`. It declares
exactly one seam, `exec_re_scan_set_param_plan`. `init_seams()` installs exactly
that one (`backend_executor_nodeSubplan_seams::exec_re_scan_set_param_plan::set(ExecReScanSetParamPlan)`),
contains nothing but that `set()`, and `seams-init::init_all()` calls it. **No
uninstalled owned seam, no leaked logic in the installer.** OK.

Consumed seams (execProcnode / execAmi / execExpr / execGrouping / execTuples /
arrayfuncs / costsize / nodes-core / tcop) are owned by other C units, so they
are out of ownership scope. Most are thin marshal+delegate (single op + arg/
result conversion): `exec_proc_node`, `exec_re_scan`, `slot_getattr`,
`clamp_cardinality_to_long`, `bms_*`, `check_for_interrupts`, the hash-table
lifecycle/probe ops, the array init/accum/make ops — acceptable cross-unit
seams. (The former F2 exception — the execExpr `fill_combining_column` seam — was
removed; its in-crate field writes are now done here, and
`build_hash_projections_and_exprs` is a genuine execExpr-owned projection/
ExprState assembly over the raw expression tree.)

## 2b. Design conformance

- Neighbor types are real, not stubbed: `SubLinkType` `#[repr(i32)]` with
  `Exists=0..Cte=7` verified against `primnodes.h` (matches c2rust
  `EXISTS_SUBLINK=0..CTE_SUBLINK=7`); execExpr/execGrouping-owned
  hash/projection/exprstate fields are `Opaque` (inherited opacity, not integer
  stand-ins). OK.
- Allocating paths carry `Mcx` / return `PgResult`; OOM via `mcx.oom` in
  `clone_int_list`. OK.
- No shared statics, no ambient-global seams, no locks across `?`, no registry
  side tables. Per-backend state threaded via `&mut EStateData`. OK.
- `ERRCODE_CARDINALITY_VIOLATION` for the multi-row ereport; internal
  `errmsg_internal` elogs → `ERRCODE_INTERNAL_ERROR`. OK.

## 3. Findings

### F2 — ExecInitSubPlan hash-init relocates in-crate logic (step-3 MISSING / step-3b)

In C, the `useHashTable` branch performs, *in nodeSubplan.c itself*, the
per-column control-array writes `sstate->keyColIdx[i-1] = i` (C:1004),
`sstate->tab_eq_funcoids[i-1] = get_opcode(rhs_eq_oper)` (C:990),
`sstate->tab_collations[i-1] = opexpr->inputcollid` (C:1001), plus the
`makeTargetEntry`/`lappend` assembly of `lefttlist`/`righttlist` (C:964-978).
In the port, `keyColIdx`, `tab_eq_funcoids`, `tab_collations` are
**concretely-typed in-crate `PgVec` fields** on `SubPlanState`
(`types-nodes/src/execexpr.rs`), yet neither their allocation
(`exec_grouping::alloc_hash_control_arrays`) nor their per-column population
(`exec_expr::fill_combining_column`) is done in this crate — both are pushed
into the execGrouping/execExpr seam owners. A grep confirms nodeSubplan never
writes `keyColIdx[idx]`/`tab_eq_funcoids[idx]`/`tab_collations[idx]` anywhere.

Per SKILL.md step 3, "a function whose *body* was replaced by a seam call to
'somewhere else' is not SEAMED, it is MISSING — the logic must live in this
crate"; step 3b additionally flags "node construction, or computation in a seam
path." The catalog lookups inside `resolve_combining_op` (lsyscache-owned) and
the ExecTypeFromTL/ExecBuildProjectionInfo/ExecBuildHash32FromAttrs/
ExecBuildGroupingEqual calls (execExpr-owned) are legitimately external, and the
classification arm + the per-column loop *structure* do stay in-crate. But the
writes to this crate's own fields and the tlist orchestration are nodeSubplan's
logic and are absent from the crate. → **ExecInitSubPlan = PARTIAL.**

**Resolution (fixed):** the `useHashTable` branch of `ExecInitSubPlan` now
allocates the in-crate control arrays (`numCols`, `keyColIdx`,
`tab_eq_funcoids`, `tab_collations`, `tab_hash_funcs`, `cur_eq_funcs`, plus the
two transient `lhs_hash_funcs`/`cross_eq_funcoids` the C keeps on the stack) and
runs the `foreach(l, oplist)` loop *in this crate*, writing every one of its own
fields directly: `keyColIdx[i-1] = i`, `tab_eq_funcoids[i-1] = rhs_eq_funcoid`,
`tab_collations[i-1] = inputcollid`, `cur_eq_funcs[i-1] = FmgrInfo{opfuncid}`,
`tab_hash_funcs[i-1] = FmgrInfo{right_hashfn}`, `cross_eq_funcoids[i-1] =
opfuncid` (the trimmed `FmgrInfo` carries only `fn_oid`, so `fmgr_info` is a
stamp and `fmgr_info_set_expr` has no represented field — a faithful no-op). The
dead `exec_grouping::alloc_hash_control_arrays` and `exec_expr::fill_combining_column`
seams were deleted. Only the genuinely external work stays seamed: the
lsyscache catalog lookups (`resolve_combining_op`) and the execExpr-owned
`makeTargetEntry`/`ExecTypeFromTL`/`ExecBuildProjectionInfo`/
`ExecBuildHash32FromAttrs`/`ExecBuildGroupingEqual` assembly
(`build_hash_projections_and_exprs`, now lent the two transient fmgr arrays).
→ **ExecInitSubPlan = MATCH.**

### F3 — ExecScanSubPlan ARRAY_SUBLINK memory context (step-2 DIVERGES)

C `ExecScanSubPlan` initializes the `ArrayBuildStateAny` with
`CurrentMemoryContext` *at entry* (C:220-221, before the per-query switch at
C:228) and builds the final `makeArrayResultAny(astate, oldcontext, true)` in
that same entry-time context (C:439) — i.e. the caller's short-lived per-tuple
expression-evaluation context, restored after the scan loop. c2rust confirms
`initArrayResultAny((*subplan).firstColType, CurrentMemoryContext, ...)`
(:2384-2388). The port allocates the accumulator and the result array in
`estate.es_query_cxt` (the per-query context the C switches *to* only for the
scan loop). The returned `Datum` is valid and outlives the call, so results are
correct, but the array now lives until query end instead of being reclaimed at
the caller's per-tuple reset — a memory-lifetime / accounting divergence.

This is recorded in DESIGN_DEBT.md ("ARRAY_SUBLINK array result built in
per-query context, not the caller's"). Per SKILL.md step 4, "DIVERGES = FAIL —
there are no acceptable deferrals": a ledger entry documents the divergence but
does not convert it to a pass. (`ExecSetParamPlan`'s ARRAY path is faithful: C
builds there in `econtext->ecxt_per_query_memory`, which equals es_query_cxt.)
→ **ExecScanSubPlan = DIVERGES.**

**Resolution (fixed):** the three polymorphic-array seams
(`init_array_result_any` / `accum_array_result_any` / `make_array_result_any`)
were re-signed to take the caller's `econtext: EcxtId` + `&mut EStateData` and an
`ArrayBuildCtx` selector (`PerTuple` / `PerQuery`); the arrayfuncs owner resolves
the real `MemoryContext` off the EState (the borrow stays local in the owner, so
the `'mcx`-lived accumulator/result are sound). `ExecScanSubPlan` now passes
`ArrayBuildCtx::PerTuple` to all three calls — matching C's entry-time
`CurrentMemoryContext`/`oldcontext`, which for a SubPlan evaluated inside
expression evaluation is `econtext->ecxt_per_tuple_memory` — so the array is
reclaimed at the caller's per-tuple reset, not at query end. `ExecSetParamPlan`
passes `ArrayBuildCtx::PerQuery` (init/accum at its per-query entry context, and
the final `makeArrayResultAny(astate, econtext->ecxt_per_query_memory, true)`),
preserving its already-faithful behaviour and the `node->curArray` cross-call
reuse. The DESIGN_DEBT.md entry was removed. → **ExecScanSubPlan = MATCH.**

## 4. Verdict

**PASS** (after the F2/F3 fixes below).

- All 12 functions MATCH; the seam wiring is clean (the single owned seam is
  installed by a `set()`-only `init_seams()` wired into `seams-init`).
- `ExecScanSubPlan` (F3, was DIVERGES) — the ARRAY_SUBLINK array is now built in
  the caller's entry-time per-tuple context via the re-signed array seams
  (`ArrayBuildCtx::PerTuple`); `ExecSetParamPlan` keeps `PerQuery`. Behaviour is
  byte-identical to C (including the per-tuple reclaim lifetime). MATCH.
- `ExecInitSubPlan` (F2, was PARTIAL) — the hash-init control arrays are
  allocated and the `keyColIdx`/`tab_eq_funcoids`/`tab_collations`/
  `tab_hash_funcs`/`cur_eq_funcs` fields written *in this crate*; only the
  lsyscache catalog lookups and the execExpr projection/ExprState assembly stay
  seamed. The dead `alloc_hash_control_arrays`/`fill_combining_column` seams were
  deleted. MATCH.

`cargo check --workspace` + `cargo test --workspace` both pass (1001 ok test
results, no failures). CATALOG.tsv may be set to `audited`.
