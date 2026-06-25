# Audit: backend-utils-mmgr-portalmem

- **Unit:** `backend-utils-mmgr-portalmem`
- **Branch:** `port/backend-utils-mmgr-portalmem`
- **C source:** `src/backend/utils/mmgr/portalmem.c` (PG 18.3, 1293 lines)
- **c2rust:** `c2rust-runs/backend-utils-mmgr-portalmem/src/portalmem.rs`
- **Port:** `crates/backend-utils-mmgr-portalmem/src/lib.rs`
- **Date:** 2026-06-12
- **Model:** Opus 4.8 (claude-opus-4-8[1m])
- **Verdict:** **PASS**

Independent from-scratch re-derivation against the C and c2rust sources (not
trusting the prior audit or the port's self-review). Confirms the
previously-failing finding (`PortalGetPrimaryStmt` MISSING) is resolved at the
root, and adds one fix this round: a DESIGN_DEBT ledger entry for the three
uninstalled deep-copy seams (an unledgered-divergence finding under step 3b).

## Previously-failing finding — confirmed RESOLVED

`PortalGetPrimaryStmt` (#3) was FAIL/MISSING in the prior cycle because its
body had been exported across a `portalcmds_seam::first_can_set_tag_stmt` seam
while `portal->stmts` was modeled as opaque `ExternHandle` (types.md rule-6
invented opacity). Independently re-verified as fixed:

- `types_portal::PortalData.stmts` is the real owned
  `Option<Vec<PlannedStmt<'static>>>` (lib.rs:302), no longer an `ExternHandle`.
- `types_nodes::nodeindexscan::PlannedStmt.canSetTag` is a real `bool` field
  (nodeindexscan.rs:143, propagated by `clone_in`), matching c2rust.
- `PortalGetPrimaryStmt` (lib.rs:278) runs the `foreach`/`canSetTag` walk
  in-crate over the owned `Vec`, returning the index of the first `canSetTag`
  stmt (positional analog of the C `PlannedStmt *`).
- `first_can_set_tag_stmt` seam is gone — zero references repo-wide.

## 1. Function inventory + verdicts

`portalmem.c` defines 27 functions (25 extern + 2 static:
`PortalReleaseCachedPlan`, `HoldPortal`) plus the three hash-table macros
`PortalHashTable{Lookup,Insert,Delete}`. Every one is enumerated below;
cross-checked against the c2rust rendering (`portalmem.rs`).

| # | C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `EnablePortalManager` | :104 | lib.rs:247 | MATCH | TopPortalContext + table created; Assert→ok_or guard. |
| 2 | `GetPortalByName` | :130 | lib.rs:262 | MATCH | NULL/empty-name→None; else hash lookup. |
| 3 | `PortalGetPrimaryStmt` | :151 | lib.rs:278 | **MATCH** (was MISSING) | In-crate `foreach(portal->stmts)`/`canSetTag` walk over real owned `Vec<PlannedStmt>`; seam removed. |
| 4 | `CreatePortal` | :175 | lib.rs:293 | MATCH | dup→ERROR/WARNING(ERRCODE_DUPLICATE_CURSOR)+PortalDrop(false); all non-zero fields init; resowner/cleanup/subid/level via seams; HashTableInsert sets name; SetIdentifier(name|"<unnamed>"). |
| 5 | `CreateNewPortal` | :235 | lib.rs:384 | MATCH | thread_local `unnamed_portal_count`, `<unnamed portal %u>`, loop-until-nonconflict, CreatePortal(false,false). |
| 6 | `PortalDefineQuery` | :282 | lib.rs:400 | MATCH | Asserts→debug_assert; stores all 7 fields; status→PORTAL_DEFINED. (The cursor-case caller copy lives in the seamed `portal_define_query_select`, ledgered.) |
| 7 | `PortalReleaseCachedPlan` (static) | :310 | lib.rs (release_cached_plan) | MATCH (SEAMED callee) | cplan guard; ReleaseCachedPlan via plancache seam; clears cplan + stmts. |
| 8 | `PortalCreateHoldStore` | :444 | lib.rs:444 | MATCH | holdContext = child of TopPortalContext (not portalContext); tuplestore_begin_heap(scroll) via tuplestore-hold seam; Asserts→debug_assert. |
| 9 | `PinPortal` | :371 | lib.rs:565 | MATCH | elog ERROR "portal already pinned". |
| 10 | `UnpinPortal` | :380 | lib.rs:576 | MATCH | elog ERROR "portal not pinned". |
| 11 | `MarkPortalActive` | :395 | lib.rs:587 | MATCH | runtime test→ERROR ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE; sets ACTIVE + activeSubid. |
| 12 | `MarkPortalDone` | :414 | lib.rs:604 | MATCH | →DONE; run+clear cleanup hook. |
| 13 | `MarkPortalFailed` | :442 | lib.rs:617 | MATCH | assert status!=DONE; →FAILED; run+clear cleanup. |
| 14 | `PortalDrop` | :468 | lib.rs:629 | MATCH | pinned/ACTIVE→ERROR ERRCODE_INVALID_CURSOR_STATE; cleanup; HashTableDelete; ReleaseCachedPlan; holdSnapshot unregister; resowner 3-phase release+delete under `(!isTopCommit||FAILED)`, isCommit=(status!=FAILED); holdStore end(drop); holdContext+portalContext drop; struct freed. Order + predicates match. |
| 15 | `PortalHashTableDeleteAll` | :607 | lib.rs:734 | MATCH | NULL-table early return; skip ACTIVE; PortalDrop(false); restart-on-drop. |
| 16 | `HoldPortal` (static) | :636 | lib.rs (HoldPortal) | MATCH | PortalCreateHoldStore; PersistHoldablePortal (seam); ReleaseCachedPlan; resowner=NULL; createSubid/activeSubid=Invalid; createLevel=0. |
| 17 | `PreCommit_Portals` | :677 | lib.rs:783 | MATCH | pinned&&!autoHeld→ERROR; ACTIVE→unregister holdSnapshot+null resowner/portalSnapshot+continue; HOLD&&subid!=Invalid&&READY→isPrepare ERROR(FEATURE_NOT_SUPPORTED) else HoldPortal; subid==Invalid→continue; else PortalDrop(true); restart after state change; returns result. |
| 18 | `AtAbort_Portals` | :781 | lib.rs:859 | MATCH | ACTIVE&&shmem_exit_inprogress→MarkPortalFailed (direct ipc-dsm-core dep); subid==Invalid/autoHeld→continue; READY→MarkPortalFailed; run+clear cleanup; ReleaseCachedPlan; resowner=NULL; non-ACTIVE→DeleteChildren. Re-reads status. |
| 19 | `AtCleanup_Portals` | :858 | lib.rs:915 | MATCH | skip ACTIVE; subid==Invalid\|\|autoHeld→continue; force-unpin; cleanup-still-set→WARNING+clear; PortalDrop(false). |
| 20 | `PortalErrorCleanup` | :917 | lib.rs:960 | MATCH | autoHeld→unpin+PortalDrop(false). |
| 21 | `AtSubCommit_Portals` | :943 | lib.rs:972 | MATCH (owner dissolved) | createSubid==mySubid→reparent+level; activeSubid==mySubid→parentSubid; ResourceOwnerNewParent via seam. parentXactOwner dissolved to NULL per query-lifecycle-raii (ledgered); reparent seam no-op until resowner lands — control flow present. |
| 22 | `AtSubAbort_Portals` | :979 | lib.rs:1004 | MATCH (owner dissolved) | not-mine+activeSubid==mySubid: activeSubid→parent, ACTIVE→Failed, FAILED&&resowner→NewParent(myXactOwner)+null; mine: READY/ACTIVE→Failed, cleanup, ReleaseCachedPlan, resowner=NULL, DeleteChildren. Owner args dissolved (ledgered). |
| 23 | `AtSubCleanup_Portals` | :1092 | lib.rs:1073 | MATCH | createSubid!=mySubid→continue; force-unpin; cleanup-still-set→WARNING+clear; PortalDrop(false). |
| 24 | `pg_cursor` | :1131 | lib.rs:1108 | MATCH (split) | In-crate: one-scan walk collecting visible && sourceText!=NULL rows + the 6 column values. SRF/Datum body (InitMaterializedSRF + putvalues) crosses the portalcmds seam — correct fmgr/Datum value-layer split. |
| 25 | `ThereAreNoReadyPortals` | :1171 | lib.rs:1147 | MATCH | any READY→false. |
| 26 | `HoldPinnedPortals` | :1207 | lib.rs:1158 | MATCH | pinned&&!autoHeld: strategy!=ONE_SELECT→ERROR(OBJECT_NOT_IN_PREREQUISITE_STATE); status!=READY→elog ERROR; HoldPortal; autoHeld=true. |
| 27 | `ForgetPortalSnapshots` | :1256 | lib.rs:1193 | MATCH | clear portalSnapshot (count); pop all ActiveSnapshot (count); mismatch→ERROR with both counts. Seams active_snapshot_set/pop_active_snapshot. |

Hash-table macros → `portal_hash_table_{lookup,insert,delete}` (lib.rs:178/186/211):
MATCH — insert dup→ERROR "duplicate portal name", delete-missing→WARNING, name
truncation to `MAX_PORTALNAME_LEN-1` mirrors dynahash `HASH_STRINGS` keying.

Spot-checked in detail this cycle (re-derived line-by-line against the C):
`PortalGetPrimaryStmt`, `CreatePortal`, `PortalDrop`, `PreCommit_Portals` — all
confirmed MATCH including error SQLSTATEs, branch predicates, and ordering.

## 2. Seam audit

**Owned seam crate (by C-source coverage):**
`backend-utils-mmgr-portalmem-seams` maps to `portalmem.c`. It declares 17
seams. Their disposition:

- 6 inward xact-lifecycle seams (`pre_commit_portals`, `at_abort_portals`,
  `at_cleanup_portals`, `at_subcommit_portals`, `at_subabort_portals`,
  `at_subcleanup_portals`) — all installed by `seams_install::init_seams()`.
- 8 portalcmds-facing portal-operation seams (`create_portal`,
  `get_portal_by_name`, `portal_hash_table_delete_all`, `portal_drop`,
  `mark_portal_active`, `mark_portal_failed`, `memory_context_delete_children`,
  `with_portal_globals`) — all installed. These exist because portalcmds is in
  a real dependency cycle with portalmem; the shared `types_portal::Portal`
  open handle crosses, and the bodies live in this crate.
- 3 deep-copy-into-portal-context seams (`portal_define_query_select`,
  `copy_param_list_into_portal`, `copy_tup_desc_into_hold_context`) —
  **uninstalled** (seam-and-panic). Their bodies are caller-side (portalcmds.c)
  copy operations (`copyObject(PlannedStmt)`/`pstrdup`, `copyParamList`,
  `CreateTupleDescCopy`) that deep-copy foreign objects into portalmem-owned
  `'static` arenas; the copy infrastructure is unported. No portalmem.c
  function depends on them — `PortalDefineQuery` itself is fully in-crate.

`init_seams()` contains nothing but `set()` calls (+ two thin owner-dissolving
wrappers for the sub-xact owner args) and `seams-init::init_all()` calls
`backend_utils_mmgr_portalmem::init_seams()` (seams-init/src/lib.rs:71).

**Outward-seam thinness:** `release_cached_plan`, `resource_owner_*`,
`tuplestore_begin_heap`, `unregister_snapshot_from_owner`,
`active_snapshot_set`/`pop_active_snapshot`, the three xact getters, the
cleanup/persist/pg_cursor-SRF seams — each is thin marshal+delegate, no
branching/node-construction in the seam path, each justified by a real unported
dependency. No seam-thinness finding.

## 3b. Design conformance

- **types.md rules 6-7 (opacity inherited, never introduced):** the prior
  invented-opacity on `portal->stmts` is gone (now real `Vec<PlannedStmt>`).
  Remaining handle fields (`portalParams`, `queryEnv`, `tupDesc`, `queryDesc`,
  `formats`, `cplan`, snapshots, resowner) are pure store/load/pass-through of
  sibling-subsystem objects with no in-crate logic over their contents →
  acceptable inherited opacity. `holdStore` is now a real owned
  `Tuplestorestate<'static>` (prior DEBT resolved). Not findings.
- Allocation: scan scratch in `pg_cursor`/portal-id collection is charged to a
  per-call owned `MemoryContext` via fallible helpers returning `PgResult` —
  conforms to mcx + PgResult.
- resowner reparent dissolved to NULL/no-op per docs/query-lifecycle-raii.md and
  ledgered in DESIGN_DEBT.md — sanctioned (control flow present, only the
  unported callee inert).
- No shared statics for per-backend globals (all `thread_local!`), no
  ambient-global seams (the `with_portal_globals` scoped-capability seam
  replaces ambient `ActivePortal`/`PortalContext` setters), no locks across `?`,
  no registry side tables.
- **Finding (fixed this cycle): unledgered divergence.** The three uninstalled
  deep-copy seams were seam-and-panic with no DESIGN_DEBT entry (step 3b lists
  "unledgered divergence markers" as merge-blocking). **Fix applied:** added a
  DESIGN_DEBT.md entry ("portalmem deep-copy-into-portal-context seams") naming
  the three seams, why they panic (unported `'static`-arena copy infra), that no
  portalmem.c function depends on them, and the landing condition. They are now
  acceptable unported-callee panics, not absent owned logic.

## 4. Verdict

**PASS.** All 27 `portalmem.c` functions are present and MATCH (the
previously-failing `PortalGetPrimaryStmt` MISSING is resolved at the root — real
owned `Vec<PlannedStmt>`, real `canSetTag`, in-crate walk, seam removed). All 14
installed owned seams are wired by a `set()`-only `init_seams()`; the 3
uninstalled seams are unported caller-side copy callees (no portalmem.c logic
absent) and are now ledgered. cargo check + the crate's 3 tests are green.
