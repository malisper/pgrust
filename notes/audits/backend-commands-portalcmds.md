# Audit: backend-commands-portalcmds

- Unit: `backend-commands-portalcmds` (`src/backend/commands/portalcmds.c`, PostgreSQL 18.3)
- Crate: `crates/backend-commands-portalcmds`
- Date: 2026-06-12
- Model: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Sources compared:
  - C ground truth: `../pgrust/postgres-18.3/src/backend/commands/portalcmds.c`
  - c2rust: `../pgrust/c2rust-runs/backend-commands-portalcmds/src/portalcmds.rs`
  - Rust port: `crates/backend-commands-portalcmds/src/lib.rs`

## Top-line verdict: **PASS**

Every C function is `MATCH`. All outward dependencies are justified seam
delegations across the command↔portal/executor cycle. The one owned seam crate
(`backend-commands-portalcmds-seams`) has all five declarations installed by
`init_seams()`, which contains only `set()` calls and is wired from
`seams-init::init_all()`. Zero seam findings; zero design-conformance findings.

## 1. Function inventory

Enumerated from the C file and cross-checked against the c2rust rendering. The C
file defines 5 externally-visible functions plus the inline helper
`SetQueryCompletion` (declared inline in `tcop/cmdtag.h`, emitted into this
translation unit; c2rust renders it as a local `unsafe extern "C" fn`). c2rust
also emitted header inlines `MemoryContextSwitchTo`, `list_length`,
`list_nth_cell`, `IsQueryIdEnabled` — these are not portalcmds.c functions and
are owned elsewhere (mmgr / pg_list / queryjumble); they appear in the port as
ambient-context dissolution or seam calls, not as bodies to reproduce here.

| # | C function | C lines | Port location | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `PerformCursorOpen` | 44-164 | `lib.rs:87` `PerformCursorOpen` | MATCH | see §2.1 |
| 2 | `PerformPortalFetch` | 176-217 | `lib.rs:228` `PerformPortalFetch` | MATCH | see §2.2 |
| 3 | `PerformPortalClose` | 223-260 | `lib.rs:290` `PerformPortalClose` | MATCH | see §2.3 |
| 4 | `PortalCleanup` | 272-315 | `lib.rs:325` `PortalCleanup` | MATCH | see §2.4 |
| 5 | `PersistHoldablePortal` | 325-506 | `lib.rs:362` `PersistHoldablePortal` + `lib.rs:428` `persist_holdable_portal_try` | MATCH | see §2.5 |
| h | `SetQueryCompletion` (inline, cmdtag.h) | — | `lib.rs:268` `set_query_completion` | MATCH | trivial 2-field set; verified against c2rust 2388-2395 |

## 2. Per-function comparison

### 2.1 PerformCursorOpen — MATCH
- Empty/NULL portalname → `ERRCODE_INVALID_CURSOR_NAME` (34000, severity ERROR
  21). Port: `lib.rs:106-109`. The predicate `!cstmt->portalname ||
  portalname[0]=='\0'` maps to `is_none_or(str::is_empty)`. ✓
- `!(options & CURSOR_OPT_HOLD)` → `RequireTransactionBlock` (seamed to xact);
  else `InSecurityRestrictedOperation()` → `ERRCODE_INSUFFICIENT_PRIVILEGE`
  (42501). `CURSOR_OPT_HOLD = 0x20` verified (c2rust 2386, types-nodes
  `portalcmds.rs:19`). Port `lib.rs:115-123`. ✓
- `IsQueryIdEnabled()` → `JumbleQuery` (seamed to queryjumble);
  `post_parse_analyze_hook` → `run_post_parse_analyze_hook` (seamed to analyze).
  Ordering jstate-then-hook preserved. ✓
- `QueryRewrite` (seamed to rewrite); `list_length(rewritten) != 1` → `elog(ERROR,
  "non-SELECT statement in DECLARE CURSOR")`; `query->commandType != CMD_SELECT`
  (CMD_SELECT=1, verified) → same elog. Port `lib.rs:142-155`. ✓
- `pg_plan_query` (seamed to tcop/postgres). ✓
- SCROLL decision: C reads `plan->rowMarks == NIL` and
  `ExecSupportsBackwardScan(plan->planTree)` off the **portal-context copy**
  (after `copyObject` at C:111). Port reads both off the working-context plan
  *before* delegating to portalmem (`lib.rs:166-167`), justified because
  copyObject is a deep copy that preserves rowMarks and planTree. Guard
  `!(cursorOptions & (CURSOR_OPT_SCROLL|CURSOR_OPT_NO_SCROLL))` then sets SCROLL
  or NO_SCROLL. `CURSOR_OPT_SCROLL=0x2`, `CURSOR_OPT_NO_SCROLL=0x4` verified.
  Port `lib.rs:195-208`. ✓
- `copyObject(plan)` + `pstrdup(sourceText)` + `PortalDefineQuery(...,
  CMDTAG_SELECT, list_make1(plan), NULL)` all run inside C's
  `MemoryContextSwitchTo(portal->portalContext)` block, so the copies land in
  the portal context. The port delegates this to
  `portalmem::portal_define_query_select` (portalmem owns `portalContext`'s
  `Mcx`); likewise `copyParamList(params)` → `copy_param_list_into_portal`.
  Allocating into another unit's context is only reachable through its owner
  here (no ambient current context) — this is the documented neighbor pattern,
  not a logic relocation: no branching/computation moved, only the
  copy-into-owner-arena mechanics. ✓
- `PortalStart(portal, params, 0, GetActiveSnapshot())` → `get_active_snapshot`
  (snapmgr) then `portal_start` (pquery). `Assert(strategy ==
  PORTAL_ONE_SELECT)` → `debug_assert_eq!`. ✓

### 2.2 PerformPortalFetch — MATCH
- Empty/NULL name → INVALID_CURSOR_NAME (34000). ✓
- `GetPortalByName` (portalmem); `!PortalIsValid` → UNDEFINED_CURSOR (34000). ✓
- `stmt->ismove` → dest = `None_Receiver` (DestNone). Port builds
  `DestReceiver::new(CommandDest::None)`. ✓
- `PortalRunFetch(portal, direction, howMany, dest)` (pquery). FetchDirection is
  mapped 1:1 from the parser enum to the portal-runtime enum (`map_fetch_direction`,
  same variants from parsenodes.h). ✓
- `if (qc) SetQueryCompletion(qc, ismove ? CMDTAG_MOVE : CMDTAG_FETCH,
  nprocessed)`. CMDTAG enum positions verified against cmdtaglist.h:
  CMDTAG_UNKNOWN=0 (first), CMDTAG_FETCH=154 (155th entry), CMDTAG_MOVE=164
  (165th), CMDTAG_SELECT=179 (180th) — match types-portal constants exactly. ✓

### 2.3 PerformPortalClose — MATCH
- `name == NULL` → `PortalHashTableDeleteAll()` and return. Port maps `None`. ✓
- `name[0]=='\0'` → INVALID_CURSOR_NAME. ✓
- `GetPortalByName`/`!PortalIsValid` → UNDEFINED_CURSOR. ✓
- `PortalDrop(portal, false)` (portalmem). ✓

### 2.4 PortalCleanup — MATCH
- Asserts → `debug_assert`. The `cleanup == PortalCleanup` assert is implicit
  (this is the hook). ✓
- `queryDesc = portal->queryDesc; if (queryDesc) { portal->queryDesc = NULL; ...}`
  — the reset-before-use is preserved by `borrow_mut().queryDesc.take()`
  (`lib.rs:334`). ✓
- `status != PORTAL_FAILED` guard, then save/set/restore `CurrentResourceOwner`
  to `portal->resowner` around `ExecutorFinish`/`ExecutorEnd`/`FreeQueryDesc`.
  Port uses the `resowner::with_current_resource_owner` scoped callback (owned by
  resowner; the save/restore-global RAII pattern) wrapping the three seamed
  executor calls in order. ✓

### 2.5 PersistHoldablePortal (+ persist_holdable_portal_try) — MATCH
- Entry asserts: `createSubid != InvalidSubTransactionId` (=0 → `!= 0`),
  `queryDesc != NULL`, `holdContext != NULL`, `holdStore != NULL`,
  `holdSnapshot == NULL`. ✓
- `CreateTupleDescCopy` into `holdContext` → `copy_tup_desc_into_hold_context`
  (portalmem owns holdContext). ✓
- `MarkPortalActive` → `mark_portal_active` (portalmem). ✓
- Save/set/restore of `ActivePortal`+`PortalContext` (portalmem-owned globals)
  and `CurrentResourceOwner` (resowner-owned) modeled as nested scoped callbacks
  (`with_portal_globals` ⊃ `with_current_resource_owner` ⊃ try-body). ✓
- TRY body (`persist_holdable_portal_try`):
  - `direction = ForwardScanDirection`; `PushActiveSnapshot(queryDesc->snapshot)`. ✓
  - `if (cursorOptions & CURSOR_OPT_SCROLL) ExecutorRewind` else `if (atEnd)
    direction = NoMovementScanDirection`. ✓
  - `queryDesc->dest = CreateDestReceiver(DestTuplestore)`;
    `SetTuplestoreDestReceiverParams(dest, holdStore, holdContext, true, NULL,
    NULL)` — the `detoast=true` flag preserved (tstore seams). ✓
  - `ExecutorRun(queryDesc, direction, 0)`. ✓
  - `dest->rDestroy(dest); dest = NULL`. ✓
  - `portal->queryDesc = NULL` (double-shutdown guard) then
    `ExecutorFinish`/`ExecutorEnd`/`FreeQueryDesc`. ✓
  - Position fixup: `if (atEnd) while(tuplestore_skiptuples(holdStore, 1000000,
    true));` else `tuplestore_rescan(holdStore); if (cursorOptions &
    CURSOR_OPT_SCROLL) if(!tuplestore_skiptuples(holdStore, portalPos, true))
    elog(ERROR, "unexpected end of tuple stream")`. Skip count `1_000_000`,
    `int64 ntuples` param, `portalPos as i64` all match. ✓
- CATCH path: `MarkPortalFailed(portal)` + restore globals + `PG_RE_THROW()`.
  Port: on `Err`, scoped callbacks have already unwound (globals restored), then
  `mark_portal_failed`, then propagate `Err`. The C order is MarkPortalFailed →
  restore → rethrow; the port restores → MarkPortalFailed → propagate. Verified
  behaviorally identical: `MarkPortalFailed` (portalmem.c:442) sets status and
  invokes `portal->cleanup` (= `PortalCleanup`), which reads only
  `portal->resowner` and manages its own resource-owner scope; it does not read
  the ambient ActivePortal/PortalContext/CurrentResourceOwner globals, so their
  restore order relative to MarkPortalFailed is immaterial. MATCH. ✓
- Success tail: `status = PORTAL_READY`; `PopActiveSnapshot`;
  `MemoryContextDeleteChildren(portalContext)` → `memory_context_delete_children`
  (portalmem). ✓

## 3. Seam audit

**Owned seam crates (by C-source coverage):** the unit's only `c_sources` entry
is `portalcmds.c`, so the single owned seam crate is
`crates/backend-commands-portalcmds-seams`. (The `*/portalcmds.c`
helper-check catalog row maps to the same file and is covered by the same seam
crate.)

`backend-commands-portalcmds-seams` declares 5 inward seams:
`perform_cursor_open`, `perform_portal_fetch`, `perform_portal_close`,
`portal_cleanup`, `persist_holdable_portal`. All 5 are installed by
`backend_commands_portalcmds::init_seams()` (`lib.rs:49-55`), which contains
nothing but `set()` calls. `seams-init::init_all()` calls it
(`crates/seams-init/src/lib.rs:23`). No uninstalled declaration; no `set()`
outside the owner. ✓

Inward seam bodies are thin marshal+delegate:
- `perform_cursor_open_seam`/`perform_portal_fetch_seam`/`perform_portal_close_seam`
  (`lib.rs:58-76`) just forward to the real functions. ✓
- `portal_cleanup` and `persist_holdable_portal` install the real functions
  directly. ✓

Outward seam calls — each justified by a real command↔portal/executor/planner
cycle, thin convert+call+convert, no branching/node-construction in the seam
path (the branching lives in this crate's bodies):

| Seam | Owner | Purpose |
|---|---|---|
| `xact::require_transaction_block` | access/transam/xact | RequireTransactionBlock |
| `miscinit::in_security_restricted_operation` | utils/init/miscinit | InSecurityRestrictedOperation |
| `queryjumble::is_query_id_enabled`, `jumble_query` | nodes/queryjumble | IsQueryIdEnabled / JumbleQuery |
| `analyze::run_post_parse_analyze_hook` | parser/analyze | post_parse_analyze_hook |
| `rewrite::query_rewrite` | rewrite/rewriteHandler | QueryRewrite |
| `postgres::pg_plan_query` | tcop/postgres | pg_plan_query |
| `executor::{exec_supports_backward_scan,executor_finish,executor_end,free_query_desc,executor_rewind,executor_run}` | executor/execMain | ExecSupportsBackwardScan + executor lifecycle |
| `portalmem::{create_portal,portal_define_query_select,copy_param_list_into_portal,get_portal_by_name,portal_hash_table_delete_all,portal_drop,copy_tup_desc_into_hold_context,mark_portal_active,mark_portal_failed,with_portal_globals,memory_context_delete_children}` | utils/mmgr/portalmem | portal storage / portalContext-arena copies / ActivePortal+PortalContext RAII |
| `pquery::{portal_start,portal_run_fetch}` | tcop/pquery | PortalStart / PortalRunFetch |
| `snapmgr::{get_active_snapshot,push_active_snapshot,pop_active_snapshot}` | utils/time/snapmgr | active snapshot stack |
| `resowner::with_current_resource_owner` | utils/resowner/resowner | CurrentResourceOwner RAII |
| `tstore::{create_dest_receiver_tuplestore,set_tuplestore_dest_receiver_params,dest_destroy}` | executor/tstoreReceiver | tuplestore DestReceiver |
| `sortstore::{tuplestore_skiptuples,tuplestore_rescan}` | utils/sort/storage | tuplestore positioning |

No function body was replaced by a "delegate elsewhere" call: every C control
branch is present in this crate; only leaf operations owned by neighbors are
seamed.

## 3b. Design conformance

- **Opacity (types.md 6-7):** `Portal` is a real `Rc<RefCell<PortalData>>` open
  handle owned by portalmem (types-rel `Relation` precedent), not an invented
  stand-in; `QueryDesc`/`DestReceiver`/`ResourceOwner` are threaded as real
  handles, not dereferenced opaquely. No invented opacity. ✓
- **Mcx + PgResult for allocation:** `PerformCursorOpen` takes `mcx: Mcx<'mcx>`
  (the working/message context) and all fallible seams return `PgResult`.
  Portal-context allocations are delegated to portalmem (the context owner). ✓
- **No shared statics for per-backend globals / no ambient-global seams:**
  ActivePortal, PortalContext, CurrentResourceOwner, CurrentMemoryContext are
  not modeled as shared statics in this crate; they are dissolved into scoped
  RAII callbacks owned by the respective owners (query-lifecycle-raii). ✓
- **Locks across `?`:** none held in this unit. ✓
- **Registry-shaped side tables:** none. ✓
- **Unledgered divergence markers:** none; no TODO/FIXME/divergence comments. ✓

## 4. Spot-check of MATCH verdicts

Re-derived three in detail: (a) CMDTAG enum positions counted directly in
`cmdtaglist.h` (UNKNOWN=0 … FETCH=154, MOVE=164, SELECT=179) — confirmed against
types-portal constants; (b) all three SQLSTATEs cross-checked against
`errcodes.txt` (INVALID_CURSOR_NAME=34000, UNDEFINED_CURSOR=34000,
INSUFFICIENT_PRIVILEGE=42501, all severity ERROR=21); (c) the PG_CATCH
reordering in `PersistHoldablePortal` re-verified against `MarkPortalFailed`
(portalmem.c:442) — its only side effect (status set + `portal->cleanup` →
`PortalCleanup`) does not read the ambient globals, so the restore-order swap is
sound.

## Verdict: **PASS**

All 6 entries MATCH; all outward dependencies properly SEAMED across real
cycles; the owned seam crate is fully installed and wired; no
design-conformance findings. Set `CATALOG.tsv` row to `audited`.
