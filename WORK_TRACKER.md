# pgrust — Work Tracker / Roadmap

> 🎯 **North Star: 100% of pg_regress tests passing, as fast as possible.**
> The complete picture: every category, every audit backlog, **every keystone**. Ordered by *tests-unblocked per unit of effort*. Maintained by the orchestrator.

_Last updated: 2026-06-18 · origin/main ≈ `32adbf99a`_

---

## Where we are
- **Passing:** `smoke` (SELECT 1; `pg_class` seqscan 415/68 rows) — verified live (io_combine_limit boot wall fixed).
- **Just unlocked:** **`count(*)` executes** (#165) — aggregate frontier open. **GUC registry complete** (402/404). **typcache complete**.
- **Closest test wins:** `boolean` (~5 bounded fixes); int/text suite (VALUES ✓; needs agg follow-ons + GROUP BY).
- **Infra:** persistent harness ✓ · shm-leak fixed ✓ · crash-survival ✓ · live boot restored ✓ · artifacts ~6 GB → **~8 build lanes**.

## 🔥 Critical path (fastest route to test-wins)
1. **Aggregate follow-ons:** `min`/`max` fall-through *(firing `a4ba09d6`)*; `count(*) FROM pg_class` setrefs INDEX_VAR *(firing `a2abe0c1`)*; `sum`/`avg` need the **Datum tail** (keystone #4) + numeric transfns.
2. **GROUP BY / DISTINCT / FOR UPDATE** — planner.c completion *(firing `abe15b79`)*.
3. **boolean.sql** — fix workflow *(`wf9ys7ghh`)* + error-position.
4. **fmgr completeness** — core seams *(`aaceafe7`)* + adt registration *(`a58be86c`)*.

---

## The 10 categories (status)
1. **pg_regress + correctness bugs** [DRIVER] — boolean workflow active; open diffs: boolin ws, error-position, pg_input_is_valid, func-in-FROM.
2. **Wiring seams** — fmgr dispatch 56→ (Phase 1 firing `aaceafe7`); install floor ~163 keystone/FDW-gated.
3. **Finishing partial ports — 176 gaps / 47 crates** — firing: planner(35), lmgr-lock(15), createplan(2); done: typcache(7); pending: arrayfuncs(19), misc2(16), relcache(14), xlog(6); deferred off-path: EXPLAIN(63), walsender(20), reorderbuffer(9).
4. **Keystones** — see register below.
5. **Entirely-unported units** — datetime registration `a58be86c`; JSON node-model (queued); array_expanded.c (queued); 76 adt crates unregistered (driving to 0).
6. **Registry-completeness** — GUC ✅ **402/404** (floor 2) · fmgr seams 🔨 · fmgr builtins 🔨 · syscache 85/85 ✅. ⚠️ installing a GUC activates its assign-hook → boot panic if unported.
7. **Crash-resilience** — ✅ DONE (t_bits, TLS-unwind, persistent harness, shm).
8. **Cluster bootstrap / self-hosting** — NOT STARTED (Rust initdb + genbki + StartupXLOG; leans on C initdb today).
9. **Dark subsystems** — replication/WAL, parallel query, extended-query protocol, pgstat.
10. **Verification / faithfulness meta** — audits (28-crate, partial-176, churn, fmgr, GUC) + guards + DESIGN_DEBT.

---

## 🧱 KEYSTONE REGISTER (all of them)

### ✅ Resolved
- **K1 · #159 plancache F0** ('static plan arena de-handle) — `cd85c8d63`
- **K2 · Crash-reinit / TLS-during-unwind** — `d0c9149ab`
- **K3 · Datum by-ref bridge (core)** (`function_call_invoke_datum`) — `aa47aba39`

### 🎯 Decided & sequenced (near-term; committed)
- **K4 · Datum by-ref TAIL** — `datumCopy`/`datumFree` for by-ref enum arms + agg trans-value copy. Blocks numeric `sum`/`avg`/`stddev`. *Deep-type* → fire into the next deep-slot. **(next keystone up)**
- **K5 · JSON node-model + grammar** — 12 raw nodes → types-nodes, grammar productions, ~25 transforms, executor eval. JSON_TABLE carved out as its own sub-campaign. *Deep-type, committed, Lane 0 queued.*
- **K6 · Expanded-array** — `array_expanded.c` + `ExpandedArrayHeader`. Bounded A+B (~700 LOC) for `array_append`/`prepend`; Lane C (executor in-place mutation) folds into K4. *Queued.*

### ⛔ Blocked / deferred (gate *later* test classes — do NOT block correct results now)
- **K7 · planagg MinMaxAggPath subroot** — `min`/`max` *index* optimization (cross-root subquery_planner path-arena). Correctness unblocked via fall-through (K-firing `a4ba09d6`); only speed deferred.
- **K8 · set-op subquery_planner cross-root path-arena** — UNION/INTERSECT/EXCEPT planning. Same class as K7.
- **K9 · Executor #167/#169 EState/PlanState de-handle** — gates PREPARE / cursors / extended-query protocol (the real portalmem follow-on to K1).
- **K10 · DshashKeyKind `Custom{compare,hash,copy}` variant** — parallel-worker shared record typmod registry (types-storage + dshash + session). Parallel-query only.

### 📡 Known longer-term keystones (from prior analysis — each gates a specific feature/test area; re-validate when reached)
- **FormedTuple carrier** — header-only `HeapTupleData` vs full user-data; being chipped (`exec_force_store_formed_heap_tuple` landed). Gates trigger slots, MERGE oldtuple, inval.
- **TriggerData carrier-widen + WHEN-qual / ResultRelInfo-trim** — gates triggers (`triggers.sql`, RI/FK).
- **GRANT executor / aclchk** (pg_init_privs write seam, DEFACLROLENSPOBJ projection, Acl carrier) — gates GRANT/REVOKE/privileges.
- **pgstat per-kind callback registry** — gates the statistics views (12 per-kind crates + core).
- **partprune PartitionPruneStep carrier** — gates runtime partition pruning.
- **copyfrom CopyParseState handle** — gates COPY FROM (re-model to value carriers).
- **TableAmRoutine mcx-free vtable** — gates custom table AMs / some heapam dispatch.
- **ruleutils deparse_namespace** — gates EXPLAIN VERBOSE, `pg_get_*def`, rule/view deparse.
- **IndexCreateArgs widen** — gates some CREATE INDEX paths.
- **ts_lexize fmgr-dispatch** — gates full-text search.
- **prepunion subroot / cross-root PathId** — set-op child path import (relates to K8).

> Legend: K1–K3 done · K4–K6 committed/sequenced · K7–K10 deferred (gate later test classes) · 📡 longer-term, feature-area-gated.

---

## Active lanes (≈8)
`abe15b79` planner (GROUP BY) · `a58be86c` fmgr-register · `aaceafe7` fmgr Phase-1 seams · `ab7b8757` lmgr-lock · `a0b9a1b9` createplan · `a4ba09d6` min/max fall-through · `a2abe0c1` setrefs INDEX_VAR · `wf9ys7ghh` boolean workflow

## Build/infra notes
- Artifacts ~6 GB/build (was ~10–14) after `713252a14` (line-tables-only + `incremental=false`). Cap **~8**, keep ~20 GB buffer.
- **Rebuild cost = crate depth.** Leaf crates (adt/commands/executor-nodes/setrefs) rebuild cheap → fan out freely. **Deep types** (`types-nodes`/`types-tuple`/`types-datum`) invalidate the world → **serialize** the deep-type keystones (K4/K5/K6), firing into thin windows.
- Auto-reaper: 90-min-idle worktrees + shm/prune.

## Recently landed
count(\*) exec (#165) · GUC registry 402/404 · typcache complete · io_combine_limit boot fix · Datum by-ref bridge (+saophash) · crash-reinit (TLS-unwind) · seclabel→DROP · comment→DROP · multi-row VALUES · parse_expr XML · t_bits crash · setrefs Aggref fixup · smaller build artifacts · plancache F0 (was already done)

## DROP status
CREATE→INSERT→SELECT ✓ · DROP: comment ✓ → seclabel ✓ → **next wall `relation_is_nailed`** (tablecmds seam).
