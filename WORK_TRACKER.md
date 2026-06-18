# pgrust — Work Tracker / Roadmap

> 🎯 **North Star: 100% of pg_regress tests passing, as fast as possible.**
> The complete picture: every category, every audit backlog, **every keystone**. Ordered by *tests-unblocked per unit of effort*. Kept current by the orchestrator as lanes land.

_Last updated: 2026-06-18 · origin/main ≈ `5f57e550a`_

---

## Where we are
- **Passing:** `smoke` (SELECT 1; `pg_class` seqscan 415/68 rows) — verified live.
- **Just unlocked:** **`count(*)` executes** (#165). **GUC registry complete** (402/404). **typcache** + **lmgr-lock** complete.
- **Closest test wins:** `boolean` (~5 bounded fixes); int/text suite (VALUES ✓; needs agg follow-ons + GROUP BY).
- **Infra:** persistent harness ✓ · shm-leak fixed ✓ · crash-survival ✓ · live boot restored ✓ · artifacts ~6 GB → **~8 build lanes** · reaper shm+prune-only (idle-rm disrupted live lanes, removed).

## 🔥 Critical path (fastest route to test-wins)
1. **Aggregate follow-ons:** `min`/`max` fall-through ✅. **SHARED BLOCKER → column-agg `ecxt_scantuple` NULL / SCAN_VAR** — `min`/`max`/`count(col)`/`sum` all reach exec then wall in agg result-projection (the Agg references a scan Var but `ecxt_scantuple` is NULL). Gates *every column aggregate*; fire after `a2abe0c1` (setrefs) lands. `count(*) FROM pg_class` setrefs INDEX_VAR *(firing `a2abe0c1`)*. `sum`/`avg` also need **Datum tail (K4)** + numeric transfns.
2. **GROUP BY / DISTINCT / FOR UPDATE** — planner.c completion *(firing `abe15b79`)*.
3. **boolean.sql** — bool TYPE correct ✅ (I/O, ops, seqscans, error text verbatim). **Forcing-function gaps in its 2nd half (hit the whole suite):** error-position LINE/caret (~10 hunks) · text-cast literal truncation bug · **`set_joinrel_size_estimates` seam (JOINS — highest fan-out)** · `relation_is_nailed` (DROP) · INSERT slice-range · `proc_arg_attrs`. Owned by `a85f618f`; broad ones (joins first) queued.
4. **fmgr completeness** — core seams *(re-fired `a6d5c9d6`)* + adt registration *(`a58be86c`)*.

---

## The 10 categories (status)
1. **pg_regress + correctness bugs** [DRIVER] — boolean workflow active; open diffs: boolin ws, error-position, pg_input_is_valid, func-in-FROM.
2. **Wiring seams** — fmgr dispatch **56/67 installed** (floor: 6 blocked on the fmgr frame-carrier by-ref keystone + 5 fastpath/bgworker); seam-install floor ~163 keystone/FDW-gated.
3. **Finishing partial ports — 176 gaps / 47 crates** — firing: planner(35), arrayfuncs(19), misc2(16), EXPLAIN(63 structural); ✅ done: typcache(7), lmgr-lock(15→6), createplan(3→0); pending: relcache(14, needs policy `a3b93c9f`), xlog(6); deferred off-path: walsender(20), reorderbuffer(9).
4. **Keystones** — see register below.
5. **Entirely-unported units** — datetime registration `a58be86c`; JSON node-model (queued); array_expanded.c (queued); 76 adt crates unregistered (driving to 0).
6. **Registry-completeness** — GUC ✅ **402/404** (floor 2) · fmgr seams 🔨 · fmgr builtins 🔨 · syscache 85/85 ✅. ⚠️ installing a GUC activates its assign-hook → boot panic if unported.
7. **Crash-resilience** — ✅ DONE (t_bits, TLS-unwind, persistent harness, shm).
8. **Cluster bootstrap / self-hosting** — NOT STARTED (Rust initdb + genbki + StartupXLOG; leans on C initdb today).
9. **Dark subsystems** — replication/WAL, parallel query, extended-query protocol, pgstat.
10. **Verification / faithfulness meta** — audits + guards + DESIGN_DEBT.

---

## 🧱 KEYSTONE REGISTER (all of them)

### ✅ Resolved
- **K1 · #159 plancache F0** — `cd85c8d63`
- **K2 · Crash-reinit / TLS-during-unwind** — `d0c9149ab`
- **K3 · Datum by-ref bridge (core)** — `aa47aba39`

### 🎯 Decided & sequenced (near-term; committed)
- **K4 · Datum by-ref TAIL** — `datumCopy`/`datumFree` by-ref arms + agg trans-value copy. Blocks numeric `sum`/`avg`. *Deep-type* → **next keystone up**, firing into the next thin window.
- **K5 · JSON node-model + grammar** — 12 raw nodes → types-nodes + grammar + ~25 transforms + executor eval; JSON_TABLE carved out. *Deep-type, committed, Lane 0 queued.*
- **K6 · Expanded-array** — `array_expanded.c`; bounded A+B (~700 LOC) for `array_append`/`prepend`; Lane C folds into K4. *Queued.*

### ⛔ Blocked / deferred (gate *later* test classes — do NOT block correct results now)
- **K7 · planagg MinMaxAggPath subroot** — `min`/`max` *index* optimization. Correctness unblocked via fall-through (`a4ba09d6`); only speed deferred.
- **K8 · set-op subquery_planner cross-root path-arena** — UNION/INTERSECT/EXCEPT.
- **K9 · Executor #167/#169 EState/PlanState de-handle** — PREPARE / cursors / extended-query protocol.
- **K10 · DshashKeyKind `Custom{compare,hash,copy}`** — parallel-worker shared record typmod registry.

### 📡 Known longer-term keystones (each gates a specific feature/test area; re-validate when reached)
- **FormedTuple carrier** — header-only tuple vs full content; being chipped. Gates trigger slots, MERGE, inval.
- **TriggerData carrier-widen + WHEN-qual / ResultRelInfo-trim** — gates triggers (`triggers.sql`, RI/FK).
- **GRANT executor / aclchk** — gates GRANT/REVOKE/privileges.
- **pgstat per-kind callback registry** — gates statistics views.
- **partprune PartitionPruneStep carrier** — gates runtime partition pruning.
- **copyfrom CopyParseState handle** — gates COPY FROM.
- **TableAmRoutine mcx-free vtable** — gates custom table AMs.
- **ruleutils deparse_namespace** — gates EXPLAIN VERBOSE / `pg_get_*def` / rule deparse.
- **IndexCreateArgs widen** — gates some CREATE INDEX paths.
- **ts_lexize fmgr-dispatch** — gates full-text search.
- **proc/procarray-private accessors** — FastPath F3 lock family (per-slot `fpRelId[]`/`fpLockBits[]` + cross-proc `allProcs` iterator) + blocker-status reporters (`BackendPidGetProc`, `lockGroupMembers`). Gates lmgr-lock's last 6 + pg_locks blocker views.
- **prepunion subroot / cross-root PathId** — set-op child path import (relates to K8).
- **fmgr frame-carrier by-ref widening** (TD-FMGR-GETARG-BYREF) — trimmed `types_nodes` FunctionCallInfoBaseData carries bare-word args, no by-ref channel → blocks `pg_getarg_{name,text_pp,varlena_pp,cstring}` + `typmodin`. Cousin of K4.
- **numeric by-ref Datum const-fold** (`make_const`, parse_node.c:418) — `abs(x::numeric)` + numeric-literal const-folding wall here; part of the K4 Datum-by-ref cluster. Also blocks the ~619 unregistered numeric/by-ref builtins from const-folding.

> Legend: K1–K3 done · K4–K6 committed/sequenced · K7–K10 deferred · 📡 longer-term, feature-area-gated.

---

## Active lanes
`abe15b79` planner (GROUP BY) · `a85f618f` boolean→PASS · `ae2d175a` arrayfuncs · `ac327c72` misc2 · `a3b93c9f` policy/RLS · `a870e34d` EXPLAIN structural · `a59005ae` column-agg ecxt_scantuple · `a9b78cc5` joins (set_joinrel_size_estimates)

## Build/infra notes
- Artifacts ~6 GB/build (was ~10–14): `713252a14` (line-tables-only) + `c794aeac6` (**`incremental=false` now on `[profile.dev]`** — the lanes' default build; was only on the unused fast-check profile). Cap **~8**, keep ~20 GB buffer.
- **Rebuild cost = crate depth.** Leaf crates rebuild cheap → fan out freely. **Deep types** (`types-nodes`/`types-tuple`/`types-datum`) invalidate the world → **serialize** K4/K5/K6 into thin windows.
- Reaper = shm + `git worktree prune` ONLY (idle-based auto-rm disrupted live lanes twice; lanes self-clean + orchestrator reaps on pressure).

## Recently landed
count(\*) exec (#165) · **count(\*) FROM pg_class = 415** (setrefs varno + IndexOnlyScan) · fmgr builtin registry 2003→1574 (0 mismatches) · cargo dev incremental=0 · min/max planner fall-through · createplan (unique/groupingsets/async, 3→0) · GUC registry 402/404 · typcache complete · lmgr-lock (15→6) · fmgr Phase-1 seams (56/67) · bool.c 100% (type correct e2e) + boolean parser/func-RTE fixes · io_combine_limit boot fix · Datum by-ref bridge (+saophash) · crash-reinit (TLS-unwind) · seclabel→DROP · comment→DROP · multi-row VALUES · parse_expr XML · t_bits crash · setrefs Aggref fixup · smaller build artifacts · plancache F0

## DROP status
CREATE→INSERT→SELECT ✓ · DROP: comment ✓ → seclabel ✓ → **next wall `relation_is_nailed`** (tablecmds seam).
