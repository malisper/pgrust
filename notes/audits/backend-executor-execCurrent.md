# Audit: backend-executor-execCurrent

**Top-line verdict: PASS** (see section 4).
**Date:** 2026-06-12 — **Model:** Opus 4.8 (1M context), `claude-opus-4-8[1m]`.
**Branch:** `port/backend-executor-execCurrent`.

C source: `src/backend/executor/execCurrent.c` (PostgreSQL 18.3).
Crate: `crates/backend-executor-execCurrent`.
c2rust: `../pgrust/c2rust-runs/backend-executor-execCurrent/src/execCurrent.rs`.

Audit is independent: re-derived from the C and c2rust, not from the port's
comments or build status.

## 1. Function inventory

From `grep -nE '^[A-Za-z_].*\($'` plus statics/inline helpers in the C file,
cross-checked against the c2rust rendering:

| # | C function | kind | port location |
|---|---|---|---|
| 1 | `execCurrentOf` | extern | `exec_current_of` + `exec_current_of_resolve` + `resolve_rowmark_strategy` + `resolve_scan_strategy` |
| 2 | `fetch_cursor_param_value` | static | `fetch_cursor_param_value` |
| 3 | `search_plan_tree` | static | `search_plan_tree` |
| 4 | `ItemPointerIsValid` | inline macro (itemptr.h) | `types_tuple::heaptuple::item_pointer_is_valid` |
| 5 | `slot_getsysattr` | inline (tuptable.h) | inside the `scan_node_extract_tid` seam (execMain), not this file's logic |

## 2. Per-function comparison

### 1. `execCurrentOf` — MATCH (with SEAMED live-state reads)

Control flow, in order, verified line-by-line against the C:

- cursor name: `cexpr->cursor_name ? : fetch_cursor_param_value(...)` →
  `match &cexpr.cursor_name { Some => from_str_in, None => fetch_cursor_param_value }`. MATCH.
- `get_rel_name(table_oid)`; NULL → `elog(ERROR, "cache lookup failed for relation %u", table_oid)`.
  Port: `lsyscache::get_rel_name`, `None` → `PgError::error(format!("cache lookup failed for relation {table_oid}"))`
  (default SQLSTATE `XX000` = `ERRCODE_INTERNAL_ERROR`, matching a bare `elog(ERROR)`). MATCH.
- `GetPortalByName`; `!PortalIsValid` → `ERRCODE_UNDEFINED_CURSOR`,
  `"cursor \"%s\" does not exist"`. Port: `with_running_cursor` lends the cursor; `None`
  → same code+message. MATCH (SEAMED lookup; decision in-crate).
- `portal->strategy != PORTAL_ONE_SELECT` → `ERRCODE_INVALID_CURSOR_STATE`,
  `"is not a SELECT query"`. Port: `portal.strategy != PORTAL_ONE_SELECT (=0)`, same. MATCH.
  `PORTAL_ONE_SELECT` value verified = 0 (first `PortalStrategy` enumerator, portal.h).
- `queryDesc == NULL || queryDesc->estate == NULL` → `"is held from a previous transaction"`,
  `24000`. Port: `!portal.has_live_query`, same. MATCH (the seam sets `has_live_query`
  exactly to `queryDesc != NULL && estate != NULL`).
- strategy split `if (es_rowmarks)`: port `if !estate.es_rowmarks.is_empty()` —
  empty PgVec is the C NULL `es_rowmarks`. MATCH.

FOR UPDATE/SHARE branch (`resolve_rowmark_strategy`):
- `for (i = 0; i < es_range_table_size; i++) thiserm = es_rowmarks[i]` → loop over
  `0..es_range_table_size`, `es_rowmarks.get(i)`. MATCH (iterates RT size, not vec len).
- skip `thiserm == NULL || !RowMarkRequiresRowShareLock(thiserm->markType)`:
  port `None => continue` then `!markType.requires_row_share_lock() => continue`.
  `RowMarkRequiresRowShareLock(m) == (m <= ROW_MARK_KEYSHARE)`; port
  `(self as u32) <= KeyShare(3)`. ROW_MARK values verified: EXCLUSIVE 0,
  NOKEYEXCLUSIVE 1, SHARE 2, KEYSHARE 3, REFERENCE 4, COPY 5. MATCH.
- `relid == table_oid`; second match → `"has multiple FOR UPDATE/SHARE references to table \"%s\""`,
  `24000`. Port: `erm.is_some()` → same. MATCH.
- `erm == NULL` after loop → `"does not have a FOR UPDATE/SHARE reference to table \"%s\""`. MATCH.
- `portal->atStart || portal->atEnd` → `"is not positioned on a row"`, `24000`. MATCH.
- `ItemPointerIsValid(&erm->curCtid)` → `Found(curCtid)`; else `return false` →
  `NotOnThisTable`. MATCH.

plain-scan branch (`resolve_scan_strategy`):
- `search_plan_tree(planstate, table_oid, &pending_rescan)`; NULL →
  `"is not a simply updatable scan of table \"%s\""`, `24000`. MATCH.
- `portal->atStart || portal->atEnd` → not positioned (tested at top level, after the
  NULL check, before the inactive check — order preserved). MATCH.
- `TupIsNull(ss_ScanTupleSlot) || pending_rescan` → `NotOnThisTable`. Port:
  `ss_ScanTupleSlot None` or slot `is_empty()` (TTS_EMPTY = `TupIsNull`) or missing slot →
  `scan_slot_is_null`, OR `pending_rescan`. MATCH.
- IndexOnlyScan vs default TID extraction: `IsA(scanstate, IndexOnlyScanState)` →
  `scannode.tag() == T_IndexOnlyScanState`; physical read delegated to
  `scan_node_extract_tid` seam returning `Tid`/`NotUpdatable`; null self-ctid →
  `"is not a simply updatable scan"` in-crate. The `USE_ASSERT_CHECKING` tableoid
  cross-check is inside the seam (it owns the live slot/scandesc). `Assert(ItemPointerIsValid)`
  → `debug_assert!`. MATCH (SEAMED physical read; branch + error decision in-crate).

### 2. `fetch_cursor_param_value` — MATCH (with SEAMED hook+decode)

- C gate `paramInfo && paramId > 0 && paramId <= paramInfo->numParams`. Port checks
  `param_id > 0` in-crate; `paramInfo != NULL && param_id <= numParams` is the
  `fetch_cursor_param` seam's contract (returns `None` for any out-of-range/absent
  param → C falls through). Behaviorally identical: the hook is never reached for an
  out-of-range param, and the fall-through to "no value found" is preserved. MATCH.
- `paramFetch` hook dispatch + `OidIsValid(prm->ptype) && !prm->isnull` gate +
  `TextDatumGetCString` are the seam (live `ParamListInfo` + text I/O). `None` =
  not-usable. MATCH (SEAMED).
- `prm->ptype != REFCURSOROID` → `ERRCODE_DATATYPE_MISMATCH`,
  `"type of parameter %d (%s) does not match that when preparing the plan (%s)"`,
  args `(paramId, format_type_be(prm->ptype), format_type_be(REFCURSOROID))`.
  Port: `WrongType(ptype)` → `format_type_be(ptype)` / `format_type_be(REFCURSOROID)`,
  same message+code. `REFCURSOROID = 1790` verified (types-tuple). MATCH.
- refcursor → `TextDatumGetCString(prm->value)` returned. Port: `RefCursor(value)`. MATCH.
- fall-through `ereport(ERROR, ERRCODE_UNDEFINED_OBJECT, "no value found for parameter %d")`.
  Port: same code+message. MATCH.

### 3. `search_plan_tree` — MATCH

- `node == NULL` → return NULL: the owned model passes `&PlanStateNode` (non-null);
  the optional root is unwrapped by the caller's `planstate.and_then(...)`, and recursive
  descents guard with `Option`/`if let`. MATCH.
- `switch (nodeTag(node))`: dispatch on `node.tag()`.
  - relation scans (`T_SeqScanState`, `T_SampleScanState`, `T_IndexScanState`,
    `T_IndexOnlyScanState`, `T_BitmapHeapScanState`, `T_TidScanState`,
    `T_TidRangeScanState`, `T_ForeignScanState`, `T_CustomScanState`): cast to
    `ScanState`, `if (ss_currentRelation && RelationGetRelid == table_oid) result = sstate`.
    Port: `node.as_scan_state()`, `ss_currentRelation.rd_id == table_oid`. All nine
    `*State` NodeTag values verified against c2rust (403,404,405,406,408,409,410,418,419).
    `as_scan_state` returns `None` for the only present variants (Material/MergeJoin), so the
    arm is currently the C `default:` — correct: those scan-node variants are not yet in
    `PlanStateNode`. MATCH.
  - `T_AppendState` (397): loop `appendplans[0..as_nplans]`, recurse, second non-null →
    `return NULL` (multiple), else accumulate. Port: `append_input_states()` loop,
    `result.is_some() => return None`. MATCH (helper returns `None` until AppendState lands).
  - `T_ResultState` (394) / `T_LimitState` (437): recurse `outerPlanState(node)`
    (= `node->lefttree`). Port: `outer_plan_state()` = `ps_head().lefttree`. MATCH.
  - `T_SubqueryScanState` (411): recurse `((SubqueryScanState*)node)->subplan`. Port:
    `subquery_subplan_state()`. MATCH (helper returns `None` until the variant lands).
  - `default:` → no descent. Port: `_ => {}`. MATCH.
- post: `if (result && node->chgParam != NULL) *pending_rescan = true`. Port:
  `result.is_some() && ps_head().chgParam.is_some()`. MATCH.

### 4. `ItemPointerIsValid` — MATCH

`(pointer != NULL) && (pointer->ip_posid != 0)`. Port `item_pointer_is_valid`:
`ip_posid != INVALID_OFFSET_NUMBER (=0)`; the owned `&` removes the NULL check.
`InvalidOffsetNumber = 0` verified (storage/off.h). MATCH.

### 5. `slot_getsysattr` — SEAMED

Inline tuptable helper; `TableOidAttributeNumber`/`SelfItemPointerAttributeNumber`
reads of the live slot are inside `scan_node_extract_tid` (execMain owns the live
slot payload). Not this file's owned algorithm. SEAMED.

## 3. Seam / wiring audit

Owned `-seams` crates (by C-source coverage): execCurrent.c maps to no
`crates/backend-executor-execCurrent-seams` and no other per-file seam crate —
this unit owns **no** inward seam crate (no crate calls `exec_current_of` across a
cycle; the C caller `nodeTidscan.c` depends directly). Therefore no `init_seams()`
and no `seams-init` entry are required. CORRECT.

Outward seam calls (each justified by a real cycle to an unported owner or reuse of
an existing shared seam; all thin marshal + delegate):

| seam | owner (unported) | shape | verdict |
|---|---|---|---|
| `lsyscache::get_rel_name(mcx, relid)` | backend-utils-cache-lsyscache | value lookup | reused as-is, OK |
| `format_type::format_type_be(mcx, oid)` | backend-utils-adt-format-type | value lookup | reused as-is, OK |
| `portalmem::with_running_cursor(name, f)` | backend-utils-mmgr-portalmem | callback-shape lend of live `PortalData`/`QueryDesc`/`EState`/`PlanState`; all decision logic runs in `f` (in-crate) | OK — no logic in the seam; lends a borrow, no `&'static mut` |
| `execMain::fetch_cursor_param(mcx, econtext, id)` | backend-executor-execMain | hook dispatch + text decode of one live param | OK — thin; in-crate gate/errors |
| `execMain::scan_node_extract_tid(estate, slot, is_index_only)` | backend-executor-execMain | one physical read of the live scan tuple/scandesc | OK — the branch flag + error decision are in-crate |

All five owners are unported, so the seams panic loudly until they land
(mirror-PG-and-panic). No `search_plan_tree`/rowmark-loop/param-gate logic was left
in any seam — the src-idiomatic `resolve_scan_tid`/`resolve_rowmark_tid`
algorithm-bearing seams were **reclaimed** into the crate (skill step 2e). No
finding.

## 3b. Design conformance

- **Neighbor types defined for real** (types.md 6-7): `CurrentOfExpr`, `ExecRowMark`,
  `RowMarkType` (real `#[repr(u32)]` enum, values verified, not an int alias),
  `RunningCursorState` (borrowed view, not `&'static mut`), `CurrentOfTid`,
  `FetchedCursorParam`, `ScanTidOutcome` — all real trimmed structs/enums in the
  right `types-*` crates. No `Oid`/`usize`/`&[u8]` stand-ins. OK.
- **Allocating fns take `Mcx` + return `PgResult`**: `exec_current_of`,
  `fetch_cursor_param_value` take `Mcx`, return `PgResult`; the working names use
  `PgString::from_str_in` (fallible, OOM → `mcx.oom`). The allocating seams
  (`get_rel_name`, `format_type_be`, `fetch_cursor_param`) take `Mcx`. OK.
- **No shared statics / ambient-global seams**: none introduced; the portal lookup
  takes the name as a parameter; no zero-arg getter seam. OK.
- **No locks across `?`**: this unit acquires none; the lent state is a borrow held
  only inside the callback. OK.
- **No registry side tables**: none. OK.
- **No unledgered divergence markers**: grepped — no `for now`/`TODO`/`hack`/
  `simplified` in the diff. OK.
- **Panics**: the two `.expect()`s guard invariants the seam contract/algorithm
  guarantee (`estate` Some when `has_live_query`; `as_scan_state` Some for a node
  `search_plan_tree` only ever returns as a relation scan) — type-justified, not
  error-path stand-ins. `debug_assert!` mirrors C `Assert`. OK.

## 4. Verdict

All functions: MATCH (with SEAMED physical/live-state reads per step 3's rules).
Zero seam findings; zero design-conformance findings.

**PASS.**
