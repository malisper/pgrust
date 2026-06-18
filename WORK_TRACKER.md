# pgrust — Work Tracker / Roadmap

> 🎯 **North Star: 100% of pg_regress tests passing, as fast as possible.**
> The complete picture: every category, every audit backlog, **every keystone**. Ordered by *tests-unblocked per unit of effort*. Kept current by the orchestrator as lanes land.

_Last updated: 2026-06-18 · origin/main ≈ `5f57e550a`_

---

## Where we are
- **Passing:** `smoke` (SELECT 1; `pg_class` seqscan 415/68 rows) — verified live.
- **Just unlocked:** **numeric/bit literals** (`1.5`, `B'101'`) · **polymorphic fns** (`int4range(1,5)`) · **aggregates + GROUP BY execute** (count/min/max/sum + grouped rows; `avg` firing) · **DISTINCT** (de-dups) · **joins** (comma + INNER/LEFT/FULL + non-equi/cross). `count(*) FROM pg_class`=415.
- **Closest test wins:** `boolean` (~5 bounded fixes); int/text suite (VALUES ✓; needs agg follow-ons + GROUP BY).
- **Infra:** persistent harness ✓ · shm-leak fixed ✓ · crash-survival ✓ · live boot restored ✓ · artifacts ~6 GB → **~8 build lanes** · reaper shm+prune-only (idle-rm disrupted live lanes, removed).

## 🔥 Critical path (fastest route to test-wins)
1. **Aggregates:** `count(*)`/`min`/`max`/`count(col)` ✅ **execute** (setrefs varno fix resolved the agg var-mapping; no extra exec change needed). `count(*) FROM pg_class`=415 ✅. **Now firing:** `sum`/`avg` (int_sum transfns in numeric.c, `a40748ae`) · **GROUP BY execution** (nodeAgg grouped per-group loop, `a048453e`). `avg(numeric)`/numeric-literal aggs also need the by-ref const-fold (K4 cluster).
2. **GROUP BY / DISTINCT / FOR UPDATE** — planner.c completion *(firing `abe15b79`)*.
3. **boolean.sql — 13→5 hunks.** ✅ Fixed: error-position (LINE/caret), cstring-NUL/whitespace, `set_joinrel_size_estimates`, `proc_arg_attrs`. **Remaining 5 = keystones:** DDL/DML cluster (CREATE-lockmode / INSERT heap-range / DROP `relation_is_nailed`) *(firing `ac81ca4b`)* · FK-join selectivity (ForeignKeyOptInfo arena) · shared-typcache (K10).
4. **fmgr completeness** — core seams *(re-fired `a6d5c9d6`)* + adt registration *(`a58be86c`)*.

---

## The 10 categories (status)
1. **pg_regress + correctness bugs** [DRIVER] — boolean workflow active; open diffs: boolin ws, error-position, pg_input_is_valid, func-in-FROM.
2. **Wiring seams** — fmgr dispatch **56/67 installed** (floor: 6 blocked on the fmgr frame-carrier by-ref keystone + 5 fastpath/bgworker); seam-install floor ~163 keystone/FDW-gated.
3. **Finishing partial ports — 176 gaps / 47 crates** — firing: arrayfuncs(19), EXPLAIN(63 structural), planner priorities 2-5; ✅ done: typcache(7), lmgr-lock(15→6), createplan(3→0), misc2(15/16), planner GROUP BY (priority-1); pending: relcache(14, needs policy `a3b93c9f`), xlog(6); deferred off-path: walsender(20), reorderbuffer(9).
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
- **K8 · set-op UNION/INTERSECT/EXCEPT** — **COMMITTED**; 3/4 blockers resolved (subquery_planner + estimates + all builders/converters/exec nodes real). Prereq = **cross-root path-arena import primitive** *(firing `a9f0a896`)*, then K8's clean 5-lane port.
- **K9 · PREPARE / extended-protocol** — **COMMITTED**; foundation done (portal arena, SELECT 1 via portal) + `prepare.c` fully ported (957 lines) but blocked on the **param-list de-handle** *(firing `a7869f86`, deep slot)* + the #167/#169 cursor/EPQ leg.
- **K10 · DshashKeyKind `Custom{compare,hash,copy}`** — parallel-worker shared record typmod registry. Single-backend **SIDESTEPPED** (`44ad03cf1` uses local registry); only parallel-query needs it.

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
- **fmgr frame-carrier by-ref widening** (TD-FMGR-GETARG-BYREF) — trimmed `types_nodes` FunctionCallInfoBaseData carries bare-word args, no by-ref channel → blocks `pg_getarg_{name,text_pp,varlena_pp,cstring}` + `typmodin`. **🔥 #1 TYPE-TEST BLOCKER** (type-sweep confirmed): gates char/varchar/numeric/name + cascades via `test_setup` `char(4)` to int4/int8/text. **NEXT deep-slot priority after param-list.** Cousin of K4.
- **fn_expr-through-Func-step** (TD-FMGR-FN-OID-AND-EXPR-NODE) — by-OID Func dispatch drops `flinfo->fn_expr`, so `get_fn_expr_rettype/argtype`→0 → **polymorphic functions** (range/array/enum constructors) fail "type OID 0" *(firing `af31b5cf`)*. Same cluster as frame-carrier.
- **executor grouping/equality** — nodeAgg per-group + nodeUnique de-dup (DISTINCT) via `exec_tuples_match`. GROUP BY + DISTINCT both PLAN ✅ but don't yet group/de-dup at exec *(GROUP-BY-exec firing `a048453e`; DISTINCT de-dup follows same machinery)*.
- **LockRows path carrier + EPQ** (EvalPlanQual) — FOR UPDATE planning ✅; needs `LockRowsPath.rowMarks`→`PlanRowMarkId` carrier + `create_lockrows_plan` + the EvalPlanQual executor. Gates FOR UPDATE/SHARE.
- **make_const by-ref const-fold** (parse_node.c:418/455) — numeric (`1.5`), bit (`B'101'`), and ALL by-ref-typed LITERALS can't fold into a `Const` → **huge fraction of suite blocked**. K4 cluster. *(firing `ae958c49`)*.
- **tablecmds `alter_table_slow`** — the ALTER TABLE engine (ADD/ALTER COLUMN, constraints, ENABLE RLS). Gates most `ALTER TABLE` across the suite; ~22k LOC campaign.
- **FK-join selectivity / ForeignKeyOptInfo arena** (`get_foreign_key_join_selectivity`, `root->fkey_list`) — unmodeled arena-handle; gates FK-aware join selectivity.

> Legend: K1–K3 done · K4–K6 committed/sequenced · K7–K10 deferred · 📡 longer-term, feature-area-gated.

---

## Active lanes
**(15-lane goal)** `ac81ca4b` DDL/DML · `aadeb37f` HAVING · `a9119f58` avg · `a091e9e9` window fns · `a9c4410a` SRF · `ae91a044` RowExpr · `a70606ab` ruleutils-deparse · `a638453d` datetime sweep · `a30047d4` expr-node sweep · `ada1d774` evalexpr const-fold · `a4ea9f8b` xml · `a9f0a896` **K8-prereq path-arena** · `ae6e5cd6` json · `a7869f86` **K9-prereq param-list (deep slot)** · `a3e10fdd` build-size

## Build/infra notes
- Artifacts ~6 GB/build (was ~10–14): `713252a14` (line-tables-only) + `c794aeac6` (**`incremental=false` now on `[profile.dev]`** — the lanes' default build; was only on the unused fast-check profile). Cap **~8**, keep ~20 GB buffer.
- **Rebuild cost = crate depth.** Leaf crates rebuild cheap → fan out freely. **Deep types** (`types-nodes`/`types-tuple`/`types-datum`) invalidate the world → **serialize** K4/K5/K6 into thin windows.
- Reaper = shm + `git worktree prune` ONLY (idle-based auto-rm disrupted live lanes twice; lanes self-clean + orchestrator reaps on pressure).

## Recently landed
**numeric/bit literals (make_const) ✅** · **polymorphic fns (fn_expr) ✅** · evaluate_expr const-fold ✅ · ruleutils deparse (pg_get_expr) ✅ · RowExpr parser · **avg(int) ✅** (finalize_aggregate ported) · **HAVING ✅** · window planning · xml exprs (XMLCONCAT/PARSE/ROOT) ✅ · jsonb ops (->/->>/@>/||) ✅ · **GROUP BY exec ✅** · **DISTINCT de-dup ✅** · cross/non-equi joins ✅ · ALTER TABLE spine + ENABLE RLS · CREATE TEMP TABLE · record-typmod (local registry) · geo (74 OIDs) · **INNER/LEFT/FULL JOIN ✅** · **2-table joins** ✅ · **DISTINCT + FOR-UPDATE planning** · **count/min/max/sum aggregates** ✅ · type-adt registration: inet/enum/cash/range/multirange/acl/bit/geo · arrayfuncs (sort/shuffle/sample) · EXPLAIN structural · **boolean error-position (LINE/caret) + varlena cstring-NUL** · policy/RLS (CREATE POLICY + RelationBuildRowSecurity) · proc_arg_attrs · count(\*) exec (#165) · **GROUP BY planning** (+ real parser p_rtable bug fix) · misc2 (15/16) · **count(\*) FROM pg_class = 415** (setrefs varno + IndexOnlyScan) · fmgr builtin registry 2003→1574 (0 mismatches) · cargo dev incremental=0 · min/max planner fall-through · createplan (unique/groupingsets/async, 3→0) · GUC registry 402/404 · typcache complete · lmgr-lock (15→6) · fmgr Phase-1 seams (56/67) · bool.c 100% (type correct e2e) + boolean parser/func-RTE fixes · io_combine_limit boot fix · Datum by-ref bridge (+saophash) · crash-reinit (TLS-unwind) · seclabel→DROP · comment→DROP · multi-row VALUES · parse_expr XML · t_bits crash · setrefs Aggref fixup · smaller build artifacts · plancache F0

## DROP status
CREATE→INSERT→SELECT ✓ · DROP: comment ✓ → seclabel ✓ → **next wall `relation_is_nailed`** (tablecmds seam).
