# Audit: backend-executor-nodeTableFuncscan

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]
- **Branch:** port/backend-executor-nodeTableFuncscan
- **C source:** `src/backend/executor/nodeTableFuncscan.c` (postgres-18.3)
- **c2rust:** `c2rust-runs/backend-executor-nodeTableFuncscan/src/nodeTableFuncscan.rs`
- **Port:** `crates/backend-executor-nodeTableFuncscan/src/lib.rs`

Independent re-derivation from the C; the port's comments and the prior in-tree
audit were not trusted.

## Top-line verdict: **PASS**

Every C function is present with matching logic. The two seam crates introduced
by this port (`execScan-seams`, `tablefuncRoutine-seams`) are declaration-only
and owned by their future owners; this unit owns no `<unit>-seams` crate, so the
empty `init_seams()` is correct. No seam findings; no design-conformance
findings.

## 1. Function inventory (C source)

The C file defines exactly 9 functions (3 file-scope statics with forward
declarations plus 4 exported entry points; `TableFuncNext`/`TableFuncRecheck`
are static, the three `tfunc*` are static, the four `Exec*` are exported — note
`ExecTableFuncScan` itself is static). c2rust kept all 9. All are ported.

| # | C function | C loc | Kind | Port loc (lib.rs) | Verdict |
|---|------------|-------|------|-------------------|---------|
| 1 | `TableFuncNext` | :53 | static (access mtd) | `TableFuncNext` :79 | MATCH |
| 2 | `TableFuncRecheck` | :80 | static (recheck mtd) | `TableFuncRecheck` :104 | MATCH |
| 3 | `ExecTableFuncScan` | :96 | static (ExecProcNode) | `ExecTableFuncScan` :117 + `exec_table_func_scan_node` :126 | MATCH |
| 4 | `ExecInitTableFuncScan` | :110 | exported | `ExecInitTableFuncScan` :147 | MATCH |
| 5 | `ExecEndTableFuncScan` | :219 | exported | `ExecEndTableFuncScan` :305 | MATCH |
| 6 | `ExecReScanTableFuncScan` | :236 | exported | `ExecReScanTableFuncScan` :316 | MATCH |
| 7 | `tfuncFetchRows` | :267 | static | `tfuncFetchRows` :363 + `tfunc_fetch_body` :420 | MATCH |
| 8 | `tfuncInitialize` | :339 | static | `tfuncInitialize` :457 | MATCH |
| 9 | `tfuncLoadRows` | :434 | static | `tfuncLoadRows` :571 | MATCH (provisional seam, see notes) |

## 2. Per-function detail

### 1. TableFuncNext — MATCH
- First-call guard `node->tupstore == NULL` → `node.tupstore.is_none()`; calls
  `tfuncFetchRows(node, ps_ExprContext)` (econtext threaded via estate).
- `tuplestore_gettupleslot(tupstore, true, false, scanslot)` (forward=true,
  copy=false). C discards the `(void)` return and always returns the slot; the
  port returns the bool, which equals `!TupIsNull(slot)` post-fetch — the exact
  signal `ExecScan` consumes via `TupIsNull`. Behavior-identical.

### 2. TableFuncRecheck — MATCH
- Unconditional `return true;` → `Ok(true)`.

### 3. ExecTableFuncScan — MATCH
- `castNode(TableFuncScanState, pstate)` → `PlanStateNode::TableFuncScan` match,
  panic on mismatch (mirrors C `castNode`).
- `ExecScan(&node->ss, TableFuncNext, TableFuncRecheck)` → `exec_scan::call`
  passing the two in-crate `fn`s. The driver lives in execScan.c (seam).

### 4. ExecInitTableFuncScan — MATCH
- `Assert(!(eflags & EXEC_FLAG_MARK))` → `debug_assert!` (EXEC_FLAG_MARK,
  executor.h); child-NULL asserts → `lefttree/righttree.is_none()`.
- `makeNode` + plan/state/ExecProcNode wiring (state back-link is the threaded
  estate; ExecProcNode = `exec_table_func_scan_node`).
- `ExecAssignExprContext` → execUtils seam.
- `BuildDescFromLists(colnames, coltypes, coltypmods, colcollations)` →
  toastdesc seam, all four lists via `list_or_empty`.
- natts + per-column atttypid captured before the descriptor moves into the slot
  — equivalent to C reading them back off the shared pointer.
- `ExecInitScanTupleSlot(..., &TTSOpsMinimalTuple)` → execTuples seam with
  `TupleSlotKind::MinimalTuple`.
- `ExecInitResultTypeTL` + `ExecAssignScanProjectionInfo`.
- `qual = ExecInitQual(node->scan.plan.qual, ...)`.
- routine selector `functype == TFT_XMLTABLE ? Xml : Jsonb` →
  `TableFuncRoutineKind::from_functype` (verified: TFT_XMLTABLE→XmlTable,
  TFT_JSON_TABLE→JsonbTable in types-nodes/nodetablefuncscan.rs).
- `perTableCxt = AllocSetContextCreate(CurrentMemoryContext, "TableFunc per
  value context", ...)` → `mcx.context().new_child("TableFunc per value
  context")`; `opaque = NULL` is the `new_in` default.
- ns_names copy; ns_uris/docexpr/rowexpr/colexprs/coldefexprs/colvalexprs/
  passingvalexprs each through `ExecInitExpr`/`ExecInitExprList` with NULL-cell
  preservation matching C list shapes.
- `notnulls = tf->notnulls` (cloned Bitmapset).
- `in_functions`/`typioparams` allocated `natts`-sized; loop calls
  `getTypeInputInfo(atttypid, &in_funcid, &typioparams[i])` then
  `fmgr_info(in_funcid, &in_functions[i])` → `get_type_input_info` +
  `fmgr_info_check` (eager lookup-failure surface preserved).

### 5. ExecEndTableFuncScan — MATCH
- `if (tupstore) tuplestore_end(tupstore); tupstore = NULL;` → `take()` + end.

### 6. ExecReScanTableFuncScan — MATCH
- `chgparam = node->ss.ps.chgParam` captured as `.is_some()`.
- `if (ps_ResultTupleSlot) ExecClearTuple(...)`.
- `ExecScanReScan(&node->ss)` → exec_scan_rescan seam.
- `if (chgparam) { if (tupstore) { end; NULL; } }` then
  `if (tupstore) tuplestore_rescan(...)` — order preserved exactly (after
  end+take, the rescan branch sees None and is skipped).

### 7. tfuncFetchRows + tfunc_fetch_body — MATCH
- `Assert(opaque == NULL)` → debug_assert.
- tuplestore created in `ecxt_per_query_memory` with `tuplestore_begin_heap(false,
  false, work_mem)` (randomAccess=false, interXact=false, work_mem from globals).
- PG_TRY body: `InitOpaque(tstate, scanslot->tts_tupleDescriptor->natts)` →
  `routine_init_opaque(..., scan_slot_natts)`; `ExecEvalExpr(docexpr)`; if
  `!isnull` → tfuncInitialize, `ordinal = 1`, tfuncLoadRows.
- PG_CATCH: `if (opaque) DestroyOpaque; RE_THROW` → on `Err`, destroy opaque
  (discarding its own error per RE_THROW) then return the original `Err`.
- Success path: `if (opaque) { DestroyOpaque; opaque = NULL; }` then
  `MemoryContextReset(perTableCxt)`. perTableCxt reset only on success (C does
  not reset on the catch path) — preserved.

### 8. tfuncInitialize — MATCH
- ordinalitycol read from `((TableFuncScan*)plan)->tablefunc->ordinalitycol`.
- `SetDocument(tstate, doc)`.
- `forboth(ns_uris, ns_names)`: zips to `min(len)` (forboth stops at the shorter
  list); per pair: eval expr, `if isnull ereport(ERRCODE_NULL_VALUE_NOT_ALLOWED,
  "namespace URI must not be null")`; `TextDatumGetCString` → `text_to_cstring`;
  `ns_name = ns_node ? strVal : NULL`; `SetNamespace`.
- `if (routine->SetRowFilter)` → `routine_has_set_row_filter(kind)` presence
  check; eval rowexpr, null → "row filter expression must not be null", then
  `SetRowFilter(TextDatumGetCString)`.
- colexprs loop `colno = 0..ncols`, skip `colno == ordinalitycol`; non-null
  colexpr → eval, null → error with `errdetail("Filter for column \"%s\" is
  null.")` using attname; null colexpr → `colfilter = NameStr(att->attname)`;
  `SetColumnFilter(tstate, colfilter, colno)`. `colno++` runs every iteration
  (including the ordinality column).

### 9. tfuncLoadRows — MATCH (provisional seam dependency)
- tupdesc/natts/ordinalitycol read.
- `MemoryContextSwitchTo(ecxt_per_tuple_memory)` → no ambient ctx; per-tuple
  context reset at loop bottom.
- `while (routine->FetchRow(tstate))`: `cell = list_head(coldefexprs)` →
  `cell = 0`; `CHECK_FOR_INTERRUPTS`; `ExecClearTuple(scanslot)`.
- inner column loop: ordinality col → `Int32GetDatum(ordinal++)` with `ordinal`
  i64 truncated to i32, post-increment — `Datum::from_i32(ord as i32);
  ordinal += 1`; nulls[col]=false.
- else: `GetValue(tstate, colno, att->atttypid, att->atttypmod, &isnull)`;
  `if (isnull && cell != NULL) { coldefexpr = lfirst(cell); if (coldefexpr)
  values = ExecEvalExpr(...) }` → `if isnull && cell < ncoldefs { if Some(...)
  eval }`; `if (isnull && bms_is_member(colno, notnulls)) ereport(...,"null is
  not allowed in column \"%s\"")`; `nulls[col]=isnull`.
- `if (cell != NULL) cell = lnext(coldefexprs, cell)` runs every column
  iteration → `if cell < ncoldefs { cell += 1 }` outside the ordinality/else
  split.
- `tuplestore_putvalues(tupstore, tupdesc, values, nulls)` then per-tuple reset.

  **Provisional note:** C's `values`/`nulls` alias the scan slot's own
  `tts_values`/`tts_isnull`; the port uses fresh `PgVec` scratch arrays because
  the slot payload model is not yet landed (`exec_scan_slot_descriptor` is a
  ledgered PROVISIONAL seam in execTuples-seams). Behavior is provably identical:
  C never reads the slot's value arrays after this loop — only the tuplestore is
  read downstream, and `tuplestore_putvalues` consumes the arrays by value. The
  slot is `ExecClearTuple`'d (empty) in both. Not a divergence; full logic is
  present.

## 3. Seam audit

**Owned `-seams` crates:** none. The only C file in c_sources is
`nodeTableFuncscan.c`, and there is no `crates/backend-executor-nodeTableFuncscan-seams`.
Therefore `init_seams()` being empty (`{}`) is correct — there are no owned
declarations to install. Wired into `seams-init` per the catalog note (no-op
installer, no cycle since callers reach this crate directly).

**Outward seam calls — all thin marshal + delegate, each a real dependency:**

| Seam | Owner unit | Justification |
|------|-----------|---------------|
| `execScan::exec_scan / exec_scan_rescan / exec_assign_scan_projection_info` | execScan.c | generic scan driver; reverse dep (driver re-enters this node's access/recheck fns) |
| `execUtils::exec_assign_expr_context` | execUtils.c | expr-context setup |
| `execTuples::exec_init_scan_tuple_slot / exec_init_result_type_tl / exec_clear_tuple / exec_scan_slot_descriptor` | execTuples.c | slot/result-type ops |
| `execExpr::exec_init_qual / exec_init_expr / exec_init_expr_list / exec_eval_expr_switch_context` | execExpr.c / execExprInterp.c | expression init + eval |
| `tupdesc(toastdesc)::build_desc_from_lists` | common/tupdesc.c | descriptor build |
| `routine::routine_*` (9 methods + presence) | xml.c / jsonpath_exec.c | TableFuncRoutine vtable, keyed by kind |
| `tuplestore::tuplestore_*` | tuplestore.c | output store |
| `lsyscache::get_type_input_info`, `fmgr::fmgr_info_check` | lsyscache.c / fmgr.c | type-IO lookup |
| `varlena::text_to_cstring` | varlena.c | text→cstring |
| `nodes_core::bms_is_member` | bitmapset.c | NOT NULL membership |
| `globals::work_mem` | globals.c | GUC |
| `tcop_postgres::check_for_interrupts` | postgres.c | interrupt check |

No branching/node-construction/computation lives inside a seam path in this
crate beyond argument/result marshaling. The two introduced declaration-only
crates:

- `backend-executor-execScan-seams`: declarations only, no `set()`/installer.
  Specialized to `TableFuncScanState` (documented: execScan.c will install a
  generic impl and per-node entry points marshal). Owned by execScan.c; not
  installed yet → calls panic until that lands. Acceptable per skill (unported
  callee panics are fine).
- `backend-executor-tablefuncRoutine-seams`: declarations only, owned by
  xml.c/jsonpath_exec.c; `routine_has_set_row_filter` correctly models the C
  `if (routine->SetRowFilter)` NULL-pointer presence test. Not installed yet →
  panics until owners land.

Confirmed by grep: no `set()` for any of these seams exists anywhere in
`crates/`. No `set()` outside an owner; no uninstalled seam this unit should
own. No seam finding.

## 3b. Design conformance

- **Opacity (types.md 6-7):** `opaque` stays the C builder-private space carried
  on the node (set/read only through the routine seams); no invented handle.
- **Mcx + PgResult on allocators:** `ExecInitTableFuncScan` takes
  `estate.es_query_cxt` and returns `PgResult<PgBox<...>>`; all allocating
  helpers thread `mcx` and return `PgResult`.
- **No shared statics for per-backend globals:** node state is owned
  `TableFuncScanState` threaded by `&mut`; `work_mem`/interrupts reached via
  seams, not ambient statics in this crate.
- **No ambient-global seams / registry side tables:** none.
- **Locks across `?`:** none held.
- **Divergence markers:** the slot-payload PROVISIONAL is ledgered in the
  execTuples-seams doc-comments (`exec_scan_slot_descriptor`) and in this crate's
  notes — not unledgered.
- **Neighbor-dependency decisions (AGENTS.md):** unported neighbors
  (execScan/execTuples/execExpr/xml/jsonpath/tuplestore/...) are reached through
  per-owner seam crates that panic until the owner lands — the sanctioned
  seam-and-panic, not restructure-around or silent stub.

No design finding.

## 4. Verdict

**PASS.** All 9 functions MATCH; logic is complete (no MISSING/PARTIAL/DIVERGES).
Seam wiring is correct (no owned `-seams` crate; empty `init_seams()` justified;
all outward calls are thin marshal+delegate to real dependencies). No
design-conformance violations. CATALOG.tsv row already reads `audited`.
