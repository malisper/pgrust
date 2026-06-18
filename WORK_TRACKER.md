# pgrust — Work Tracker

> 🎯 **North Star: 100% of pg_regress tests passing, as fast as possible.**
> Maintained by the orchestrator; updated as lanes land. Everything below is ordered by *tests-unblocked per unit of effort*.

_Last updated: 2026-06-18 · origin/main `d0c9149ab`_

---

## Where we are
- **Passing:** `smoke` (SELECT 1; seqscan `pg_class` 415/68 rows).
- **Closest wins:** `boolean` (executes end-to-end; ~5 bounded fixes from PASS); the int/text type suite (multi-row VALUES ✓; gated on the `count(*)` keystone).
- **Infra solid:** persistent harness ✓, shm-leak fixed ✓, crash-survival ✓ (backend death → recoverable ERROR, cluster stays up).

## 🔥 Critical path (do these first — most tests per unit effort)
1. **`count(*)` / aggregate execution** (#165 `EEOP_AGG_PLAIN_TRANS`) — *in flight* `a7483fe3`. Unblocks every aggregate / GROUP BY across the suite.
2. **Datum by-ref bridge** — *in flight* `aae07bf4`. Unblocks by-ref types (text/numeric) through fmgr; gates much of the type suite.
3. **boolean.sql fixes** — *in flight* workflow `wf9ys7ghh` (enable_geqo ✓, boolin whitespace, pg_input_is_valid, func-in-FROM) + error-position. → flips the first real regression file.
4. **fmgr + GUC registry completeness** — *in flight* (`ac1fd151` GUC, `a58be86c` fmgr-register) + queued (fmgr core seams). Stops the recurring "wire up fmgr/GUC" walls.
5. **`planner.c` completion (35 gaps)** — *queued*. Unblocks GROUP BY / windows / set-ops / DISTINCT / FOR UPDATE.

---

## The 10 categories

### 1. pg_regress + correctness bugs — **DRIVER**
- Harness: persistent instance ✓, shm-leak fixed ✓, crash-survival ✓.
- Open diffs: boolin whitespace, error-position (`LINE N: ^`). Fixed: t_bits null-bitmap, relacl constant.
- **Active:** boolean workflow `wf9ys7ghh`. **Next:** re-run boolean → PASS; then int4/text.

### 2. Wiring seams
- fmgr dispatch: **56/66 seams uninstalled** → Phase 1 (core seams) *queued behind Datum-bridge*; Phase 3 (adt register) `a58be86c`.
- Seam-install floor: ~163 legit-blocked (FDW / AM-parallel / keystone-gated).

### 3. Finishing partial ports
- Full-codebase audit: **176 real gaps / 47 crates.** Top: EXPLAIN 63 · **planner 35** · walsender 20 · arrayfuncs 19 · misc2 16 · lmgr-lock 15 · relcache 14 · reorderbuffer 9 · typcache 7 · xlog 6 · createplan 2.
- **Next:** planner.c (35) after `count(*)` lands.

### 4. Keystones — see scorecard. **4 of 5 resolved or firing.**

### 5. Porting entirely-unported units
- datetime registration (1500+ fns) `a58be86c`; JSON node-model (queued); `array_expanded.c` (queued); EXPLAIN detail; `parse_jsontable.c` (carved out).

### 6. Registry-completeness passes
- GUC slots `ac1fd151` (enable_geqo ✓ + sweep) · fmgr dispatch (above) · syscache projections **85/85 ✓** · fmgr builtins canonical 3102 (registration in progress) · catalog OIDs per-need.

### 7. Crash-resilience / robustness — ✅ **DONE this round**
- t_bits ✓ · TLS-during-unwind ✓ · persistent harness ✓ · shm-leak ✓. Cluster survives a backend death (verified).

### 8. Cluster bootstrap / self-hosting — **NOT STARTED**
- Rust-native `initdb` + `genbki` bootstrap catalog data + clean `StartupXLOG`/`ShutdownXLOG`. Currently leans on the **C `initdb`** for fixtures (masks this). Biggest hidden milestone.

### 9. Large still-dark subsystems
- replication/WAL (xlog 6, walsender 20, reorderbuffer 9) · parallel query/workers · extended-query protocol (PREPARE/bind/execute — gated by the portalmem keystone) · pgstat/activity.

### 10. Verification / faithfulness meta
- Audits done: 28-crate, full-codebase partial, churn-hotspot, fmgr-dispatch, GUC. Guards: `seams-init`, `no-todo`. Debt: `DESIGN_DEBT.md`.

---

## Keystone scorecard
| # | Keystone | Decision | Status |
|---|---|---|---|
| 1 | Datum by-ref | Bridge layer | 🔨 firing `aae07bf4` |
| 2 | #159 PREPARE F0 | Commit | ✅ already done (`cd85c8d63`) |
| 3 | JSON nodes | Commit | 📋 Lane 0 queued |
| 4 | Expanded-array | Scope→bounded | 📋 queued (~700 LOC) |
| 5 | Crash-reinit | Fix | ✅ done (`d0c9149ab`) |

## Active lanes
`a7483fe3` count(*) exec · `aae07bf4` Datum-bridge · `ac1fd151` GUC complete · `a58be86c` fmgr-register · `wf9ys7ghh` boolean workflow

## Recently landed
crash-reinit (TLS-unwind) · seclabel→DROP · comment→DROP · multi-row VALUES (test_setup int tables) · parse_expr XML · t_bits crash fix · enable_geqo GUC · plancache F0 (was already done)

## DROP status
CREATE→INSERT→SELECT ✓. DROP: comment ✓ → seclabel ✓ → next wall `relation_is_nailed` (tablecmds seam).
