# pgrust — Work Tracker / Roadmap

> 🎯 **North Star: 100% of pg_regress tests passing, as fast as possible.**
> The complete picture: every category, every audit backlog, every keystone. Ordered by *tests-unblocked per unit of effort*. Maintained by the orchestrator; updated as lanes land.

_Last updated: 2026-06-18 · origin/main ≈ `84f11b282`_

---

## Where we are
- **Passing:** `smoke` (SELECT 1; `pg_class` seqscan 415/68 rows) — verified live again (io_combine_limit boot wall fixed).
- **Just unlocked:** **`count(*)` executes** (#165 keystone) — the aggregate frontier is open.
- **Closest test wins:** `boolean` (~5 bounded fixes); int/text type suite (VALUES ✓; needs the agg follow-ons + GROUP BY); GROUP BY (planner lane firing).
- **Infra:** persistent harness ✓ · shm-leak fixed ✓ · crash-survival ✓ (t_bits + TLS-unwind) · live boot restored ✓ · build artifacts ~6 GB → **~8 concurrent build lanes**.

## 🔥 Critical path (fastest route to test-wins)
1. **Aggregate follow-ons** (now that `count(*)` works): `sum`/`avg` need numeric transfns (`int4_sum`/`int8_sum`) ported+registered; `min`/`max` need the **planagg MinMaxAggPath subroot keystone**; `count(f1)` + `count(*) FROM pg_class` are **planner setrefs plan-shape bugs** (SCAN_VAR/INDEX_VAR).
2. **GROUP BY / DISTINCT / FOR UPDATE** — planner.c completion *(firing `abe15b79`)*.
3. **boolean.sql** — fix workflow *(`wf9ys7ghh`)* + error-position. → flips the first real regression file.
4. **fmgr + GUC registry completeness** — *(firing: `aaceafe7` core seams, `a58be86c` adt register)* — stops recurring wiring walls.
5. **Datum by-ref tail** — agg trans-value by-ref (the `datumCopy` keystone) for numeric/by-ref aggregates.

---

## The 10 categories

### 1. pg_regress + correctness bugs — **DRIVER**
- Harness ✓ (persistent, shm-fixed, crash-survival, live-boot restored).
- Open diffs: boolin whitespace, error-position (`LINE N: ^`), `pg_input_is_valid`, func-in-FROM. Fixed: t_bits, relacl, io_combine_limit boot wall.
- **Active:** boolean workflow `wf9ys7ghh`. **Next:** boolean → PASS; then int4/text/int8.

### 2. Wiring seams
- **fmgr dispatch: 56/66 seams uninstalled** → Phase 1 (46 core seams) *firing* `aaceafe7`; Phase 2 (6 fastpath) queued; bgworker waits.
- Seam-install floor ~163 legit-blocked (FDW / AM-parallel / keystone-gated).

### 3. Finishing partial ports — **176 real gaps / 47 crates** (the audit backlog)
| crate | gaps | status |
|---|---|---|
| EXPLAIN | 63 | deferred (off-path; explain.sql + plan-checks) |
| **planner** | 35 | 🔨 firing `abe15b79` (GROUP BY→FOR UPDATE→DISTINCT→windows→setops) |
| walsender | 20 | deferred (replication) |
| arrayfuncs | 19 | pending (array_append/prepend = expanded-array keystone) |
| misc2 | 16 | pending (reg* binary codecs low-pri + domain helpers) |
| **lmgr-lock** | 15 | 🔨 firing `ab7b8757` |
| relcache | 14 | pending (delegates to unported policy/trigger owners) |
| reorderbuffer | 9 | deferred (logical decoding) |
| **typcache** | 7 | 🔨 firing `a852476` |
| xlog | 6 | pending (boot/recovery — see #8) |
| **createplan** | 2 | 🔨 firing `a0b9a1b9` (unique + groupingsets converters) |

### 4. Keystones — see the **Keystone register** below.

### 5. Porting entirely-unported units
- **datetime registration** (1500+ fns) `a58be86c` 🔨 · JSON node-model (Lane 0 queued) · `array_expanded.c` (queued, ~700 LOC) · EXPLAIN detail · `parse_jsontable.c` (carved out) · **76 adt crates lack builtin registration** (`a58be86c` driving to zero).

### 6. Registry-completeness passes (the "drive to 100%" pattern)
- GUC slots ✅ (enable_geqo + costsize/bufmgr/xact). ⚠️ **CAUTION: installing a GUC activates its assign-hook — if unported, it panics at BOOT** (io_combine_limit was one; watch for more).
- fmgr dispatch seams 🔨 (#2). fmgr builtin registration 🔨 (#5). syscache projections **85/85 ✅**. fmgr canonical builtins **3102**. catalog OIDs per-need.

### 7. Crash-resilience / robustness — ✅ **DONE**
- t_bits ✓ · TLS-during-unwind ✓ · persistent harness ✓ · shm-leak ✓. Cluster survives a backend death (verified). Note: re-install-on-fork-reinit correctly judged unnecessary (registries survive fork).

### 8. Cluster bootstrap / self-hosting — **NOT STARTED** (the hidden milestone)
- Rust-native `initdb` + `genbki` bootstrap catalog data + clean `StartupXLOG`/`ShutdownXLOG`. Currently leans on the **C `initdb`** for fixtures (masks this). Walls: StartupXLOG/ShutdownXLOG substrate, genbki catalog data. The line between "runs on C scaffolding" and "self-hosting."

### 9. Large still-dark subsystems
- replication/WAL (xlog 6, walsender 20, reorderbuffer 9) · **parallel query/workers** · **extended-query protocol** (PREPARE/bind/execute — gated by the executor #167/#169 portal keystone) · pgstat/activity.

### 10. Verification / faithfulness meta
- Audits run: 28-crate, full-codebase partial (176), churn-hotspot, fmgr-dispatch, GUC. Guards: `seams-init`, `no-todo`. Debt: `DESIGN_DEBT.md`. Ensures "ported" = "faithful."

---

## Keystone register
| # | Keystone | Decision | Status |
|---|---|---|---|
| 1 | Datum by-ref | Bridge layer | ✅ core done; **tail: agg trans by-ref = `datumCopy` keystone** (numeric aggs) |
| 2 | #159 PREPARE F0 | Commit | ✅ already done (`cd85c8d63`) |
| 3 | JSON node-model + grammar | Commit | 📋 Lane 0 queued (node→grammar→transforms→executor; JSON_TABLE carved out) |
| 4 | Expanded-array | Bounded | 📋 queued (A+B ~700 LOC; Lane C folds into Datum keystone) |
| 5 | Crash-reinit (TLS-unwind) | Fix | ✅ done (`d0c9149ab`) |
| 6 | **planagg MinMaxAggPath subroot** (NEW) | — | ⛔ blocks `min`/`max` index optimization (cross-root subquery_planner path-arena) |
| 7 | **Executor #167/#169 EState/PlanState de-handle** (NEW) | — | ⛔ gates portalmem PREPARE / cursors / extended-query protocol |
| 8 | **set-op subquery_planner cross-root path-arena** | — | ⛔ blocks UNION/INTERSECT/EXCEPT planning (same class as #6) |

## Active lanes (≈8, the build cap)
`abe15b79` planner (GROUP BY) · `a58be86c` fmgr-register (adt builtins) · `aaceafe7` fmgr Phase-1 seams · `ab7b8757` lmgr-lock · `a852476` typcache · `a0b9a1b9` createplan · `wf9ys7ghh` boolean workflow

## Build/infra notes
- Artifacts ~6 GB/build (was ~10–14) after `713252a14` (line-tables-only debug + `incremental=false`). Cap **~8** concurrent build lanes; keep ~20 GB free buffer.
- **Rebuild cost = crate depth.** Leaf crates (adt/commands/executor-nodes) rebuild cheap → fan out freely. **Deep types** (`types-nodes`/`types-tuple`/`types-datum`) invalidate the world → **serialize** the deep-type keystones (Datum, JSON node-model, expanded-array).
- Auto-reaper: 90-min-idle worktrees + shm/prune (de-fanged from the 25-min version that disrupted a live lane).

## Recently landed
count(\*) exec (#165) · GUC completeness · io_combine_limit boot fix · Datum by-ref bridge (+saophash) · crash-reinit (TLS-unwind) · seclabel→DROP · comment→DROP · multi-row VALUES · parse_expr XML · t_bits crash · setrefs Aggref fixup · smaller build artifacts · plancache F0 (was already done)

## DROP status
CREATE→INSERT→SELECT ✓ · DROP: comment ✓ → seclabel ✓ → **next wall `relation_is_nailed`** (tablecmds seam).
