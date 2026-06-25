# Audit: backend-executor-nodeNestloop

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]
- **Branch:** port/backend-executor-nodeNestloop
- **C source:** `src/backend/executor/nodeNestloop.c` (postgres-18.3, 400 lines)
- **c2rust:** `c2rust-runs/backend-executor-nodeNestloop/src/nodeNestloop.rs`
- **Port:** `crates/backend-executor-nodeNestloop/src/lib.rs`,
  `crates/types-nodes/src/nodenestloop.rs`

This is an independent re-derivation from the C, the c2rust rendering, and the
headers — not a review of the porter's self-audit.

## Top-line verdict: **PASS**

All four C functions are `MATCH`. The unit owns no `<unit>-seams` crate (its
functions are reached through the executor dispatch `execProcnode`, a direct dep
with no cycle), so its `init_seams()` is legitimately empty and is wired into
`seams-init`. All outward calls are real cross-unit dependencies and are thin
marshal+delegate; the join driver logic lives entirely in-crate. Zero seam
findings, zero design-conformance findings.

## 1. Function inventory

All four C function definitions in `nodeNestloop.c`, cross-checked against the
c2rust rendering (which kept all four and added no others):

| # | C function | C lines | c2rust |
|---|------------|---------|--------|
| 1 | `ExecNestLoop` (static) | 59–255 | 1762 |
| 2 | `ExecInitNestLoop` | 261–352 | 1884 |
| 3 | `ExecEndNestLoop` | 360–374 | 1973 |
| 4 | `ExecReScanNestLoop` | 380–400 | 1978 |

Localized inline helpers materialized at their call sites (from headers, not
separate C defs): `TupIsNull`, `ResetExprContext` (`MemoryContextReset`),
`InstrCountFiltered1/2`, `castNode`, `slot_getattr`.

## 2. Per-function comparison

| Function | C loc | Port loc | Verdict |
|----------|-------|----------|---------|
| `ExecNestLoop` | 59 | lib.rs:95 (`ExecNestLoop`), :372 (`exec_nestloop_node` cb) | MATCH |
| `ExecInitNestLoop` | 261 | lib.rs:389 | MATCH |
| `ExecEndNestLoop` | 360 | lib.rs:520 | MATCH |
| `ExecReScanNestLoop` | 380 | lib.rs:541 | MATCH |

### `ExecNestLoop` (C 59–255, c2rust 1762–1881, port lib.rs:95–297) — MATCH

- `CHECK_FOR_INTERRUPTS()` → `tcop_postgres::check_for_interrupts::call()?`
  (c2rust: `if InterruptPending != 0 { ProcessInterrupts() }`).
- node-info reads: `jointype`/`single_match`/`econtext` cached up front;
  `joinqual`/`otherqual` read lazily via `exec_joinqual`/`exec_otherqual` —
  loop-invariant in C too, equivalent.
- `ResetExprContext(econtext)` → `ecxt_per_tuple_memory.reset()` at top (C:91)
  and on the qual-fail tail (C:251, port:295).
- new-outer block: `ExecProcNode(outerPlan)`→`exec_outer`; `TupIsNull` early
  `return Ok(None)` (C `return NULL`); sets `ecxt_outertuple`, clears
  `nl_NeedNewOuter`/`nl_MatchedOuter`.
- nestParams loop: per param, `slot_getattr(outerSlot, varattno)` →
  `es_param_exec_vals[paramno]` `.value`/`.isnull`; then
  `innerPlan->chgParam = bms_add_member(chgParam, paramno)`. `varattno > 0`
  Assert kept as `debug_assert!`; `IsA(Var)`/`varno==OUTER_VAR` asserts subsumed
  by the typed `Var` field. Port collects `(paramno, varattno)` first for borrow
  reasons; writes performed in the same order — behavior identical.
- `ExecReScan(innerPlan)` → `execAmi::exec_re_scan`.
- inner fetch: `ExecProcNode(innerPlan)`→`exec_inner`; sets `ecxt_innertuple`.
- inner-null branch: set `nl_NeedNewOuter=true`; if `!nl_MatchedOuter && (LEFT
  || ANTI)`, swap in `nl_NullInnerTupleSlot`, eval `otherqual` (NULL⇒true), pass
  ⇒ `return ExecProject(ps_ProjInfo)`, else `InstrCountFiltered2`; then
  `continue` — correctly skipping the bottom reset (C:200).
- matched branch: `ExecQual(joinqual)` ⇒ `nl_MatchedOuter=true`; `JOIN_ANTI` ⇒
  set `nl_NeedNewOuter`, `continue`; `single_match` ⇒ set `nl_NeedNewOuter`;
  `otherqual` pass ⇒ `return ExecProject`, else `InstrCountFiltered2`; joinqual
  fail ⇒ `InstrCountFiltered1`. All predicates, ordering, and the
  nfiltered1-vs-nfiltered2 selection match c2rust exactly.
- final `ResetExprContext` reached only on the non-null-inner no-return path.

`tup_is_null`: `None⇒true`, else `slot.is_empty()` (c2rust `is_null() ||
tts_flags & TTS_FLAG_EMPTY`). `instr_count_filtered1/2`: `Option<instrument>`
guard, `+= 1.0` on `nfiltered1`/`nfiltered2` `f64` (c2rust `is_null()` guard +
`+= 1 as c_double`).

### `ExecInitNestLoop` (C 261–352, c2rust 1884–1971, port lib.rs:389–513) — MATCH

- `Assert(!(eflags & (BACKWARD|MARK)))` → `debug_assert!`. Constants verified
  against `executor.h`: BACKWARD=0x8, MARK=0x10, REWIND=0x4.
- `makeNode(NestLoopState)` → `alloc_in(mcx, NestLoopStateData::default())?`;
  NodeTag carried by `PlanStateNode::NestLoop`. `ps.plan` and
  `ExecProcNode=exec_nestloop_node` set; `ps.state=estate` implicit (estate
  threaded as a parameter in the owned model).
- `ExecAssignExprContext` → `execUtils::exec_assign_expr_context`.
- outer init `ExecInitNode(outerPlan, eflags)` → `execProcnode::exec_init_node`;
  then REWIND toggle `nestParams==NIL ? |=REWIND : &=~REWIND` (`is_empty()` for
  NIL); then inner init with the adjusted eflags. eflags mutated between the two
  init calls, exactly as C.
- `ExecInitResultTupleSlotTL(&TTSOpsVirtual)` + `ExecAssignProjectionInfo(NULL)`
  → execTuples / execUtils seams (`TupleSlotKind::Virtual` / `None`).
- `ps.qual=ExecInitQual(plan.qual)`; `jointype=join.jointype`;
  `joinqual=ExecInitQual(join.joinqual)` — order preserved.
- `single_match = inner_unique || jointype==JOIN_SEMI` (c2rust 1923).
- jointype switch: `INNER|SEMI⇒{}`; `LEFT|ANTI⇒ nl_NullInnerTupleSlot =
  ExecInitNullTupleSlot(estate, ExecGetResultType(inner), &TTSOpsVirtual)`;
  default ⇒ `elog(ERROR, "unrecognized join type: %d")`. JoinType values
  verified: INNER=0, LEFT=1, SEMI=4, ANTI=5 — matches c2rust arms `0|4=>{}`,
  `1|5=>null slot`. Error returns `PgError::error("unrecognized join type:
  {n}")`, matching C `elog(ERROR)` (XX000/ERROR).
  - Ownership note: `ExecGetResultType` is a borrow in C; the port `clone_in`s
    the descriptor into `mcx` before `exec_init_null_tuple_slot` (owned model
    can't alias). Identical contents — behaviorally equivalent.
- final `nl_NeedNewOuter=true`, `nl_MatchedOuter=false`; returns
  `PgBox<NestLoopStateData>`.

### `ExecEndNestLoop` (C 360–374) — MATCH

`ExecEndNode(outer)` then `ExecEndNode(inner)` via `execProcnode::exec_end_node`,
in that order. Port guards each child with `if let Some(..)`; both children
always exist post-init, so the `None` path is unreachable — equivalent to C's
unconditional deref.

### `ExecReScanNestLoop` (C 380–400) — MATCH

`if (outerPlan->chgParam == NULL) ExecReScan(outerPlan)` → `outer_chg_null` from
`chgParam.is_none()` (`unwrap_or(true)` for the impossible no-outer case ⇒
rescan). Inner deliberately not rescanned (comment preserved). Sets
`nl_NeedNewOuter=true`, `nl_MatchedOuter=false`.

## 3. Seam and wiring audit

**Owned seam crates: none.** The unit's only C file is `nodeNestloop.c`, and
there is no `crates/backend-executor-nodeNestloop-seams`. By the
ownership-by-C-source rule the unit owns zero seam declarations, so the empty
`init_seams()` (lib.rs:57) is correct — nothing to install. It is nonetheless
called from `seams-init/src/lib.rs:29`, and the crate is declared in
`seams-init/Cargo.toml:33`. PASS.

**Inward dispatch:** `ExecNestLoop` is reached through `execProcnode`'s node
dispatch, which can depend on this crate directly (no cycle) — confirming no
inward `-seams` crate is required. The `exec_nestloop_node` callback installed
into `ps.ExecProcNode` does `castNode`+delegate, no logic.

**Outward seam calls** — each a real cross-unit dependency, each thin
marshal+delegate (arg convert, one call, result convert); no branching, node
construction, or computation in any seam path:

| Seam call | Owner | C call |
|-----------|-------|--------|
| `tcop_postgres::check_for_interrupts` | tcop/postgres | CHECK_FOR_INTERRUPTS |
| `execProcnode::exec_proc_node` | execProcnode | ExecProcNode (outer/inner) |
| `execProcnode::exec_init_node` | execProcnode | ExecInitNode |
| `execProcnode::exec_end_node` | execProcnode | ExecEndNode |
| `execAmi::exec_re_scan` | execAmi | ExecReScan |
| `execExpr::exec_qual` | execExpr | ExecQual |
| `execExpr::exec_init_qual` | execExpr | ExecInitQual |
| `execExpr::exec_project` | execExpr | ExecProject |
| `execUtils::exec_assign_expr_context` | execUtils | ExecAssignExprContext |
| `execUtils::exec_assign_projection_info` | execUtils | ExecAssignProjectionInfo |
| `execTuples::exec_init_result_tuple_slot_tl` | execTuples | ExecInitResultTupleSlotTL |
| `execTuples::exec_init_null_tuple_slot` | execTuples | ExecInitNullTupleSlot |
| `execTuples::exec_get_result_type` | execTuples | ExecGetResultType |
| `execTuples::slot_getattr` | execTuples | slot_getattr |
| `nodes_core::bms_add_member` | nodes/bitmapset | bms_add_member |

No function body was replaced by a "compute elsewhere" seam: the
NeedNewOuter/MatchedOuter driver, nestParams PARAM_EXEC plumbing,
single_match / antijoin / left-join null-extension, instrumentation counters,
and per-tuple econtext reset are all in-crate. All seam calls that can
`ereport(ERROR)` return `PgResult` and propagate via `?`.

## 3b. Design conformance

- **Allocation + `Mcx`/`PgResult`:** `ExecInitNestLoop` allocates via
  `alloc_in(mcx, ..)` and returns `PgResult<PgBox<..>>`; the null-tupledesc clone
  uses `alloc_in(mcx, ..)?`. Allocating seam calls thread `Mcx`/`PgResult`. Conforms.
- **Opacity:** no invented handles or stand-ins. `NestLoop`/`NestLoopParam`/
  `NestLoopState` are real structs mirroring the C; `Var`/`Join`/`JoinStateData`
  are real typed fields (opacity inherited, not introduced).
- **Per-backend globals:** none introduced — no shared statics, ambient-global
  seams, or registry-shaped side tables.
- **Locks across `?`:** none.
- **Mirror-PG / NodeTags:** `T_NestLoop=356`, `T_NestLoopState=421` verified
  against the c2rust NodeTag table (232, 167). Struct field orders match
  `plannodes.h:938-952` and `execnodes.h:2164-2170`.
- **`panic!`:** only in `castNode` arms (the typed `PlanStateNode`/`Node` enum
  makes the wrong variant type-impossible; C's `castNode` is an Assert/abort) —
  the sanctioned `unreachable`-class usage.
- **Divergence markers:** none needed.

No design-conformance findings.

## 4. Verdict

**PASS** — 4/4 functions MATCH, no owned seams to install (empty `init_seams()`
correct and wired), all outward seams thin and justified, zero seam findings,
zero design-conformance findings.
