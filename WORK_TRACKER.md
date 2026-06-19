# pgrust â Work Tracker / Roadmap

> ð¯ **North Star: 100% of pg_regress tests passing, as fast as possible.**
> The complete picture: every category, every audit backlog, **every keystone**. Ordered by *tests-unblocked per unit of effort*. Kept current by the orchestrator as lanes land.

_Last updated: 2026-06-18 · origin/main

---

## Where we are
- **Passing:** `smoke` (SELECT 1; `pg_class` seqscan 415/68 rows) â verified live.
- **Closest test win:** **`boolean` runs end-to-end (600/597 lines)** â by-ref keystone fixed its text-INSERT crash; text storage round-trips. ONLY remaining diff = one non-fatal `debug_assert` (`47 vs 49`, a catalog-row varlena-size mismatch in heap_form_tuple) during CREATE TABLE â first-green blocker, lane in flight. `select_having`/`numeric` also ~1 hunk.
- **â BY-REF DATUM KEYSTONE â decomposed into 3 paths, 1 done + 2 in flight:** the "by-ref Datum VALUE unification" wall is actually 3 distinct paths. (a) **fmgr arg/output boundary** â DONE (header-ful flip `1907af523`+batches â unlocked EXTRACT, bpchar, numeric, jsonb, text arrays, UPDATE/DELETE ctid). (b) **wire-output/arg_bytes** ✅ DONE (`8c806ca2b` â arg_bytes read 1-byte SHORT headers as 4-byte, dropping 3 payload bytes; fixed via VARDATA_ANY; also fixes char/varchar/name). (c) **support-function fcinfo** ✅ DONE â hash (`c3c247b1a`) + sort (already worked) + selectivity (`be1df0607`); text/name GROUP BY/ORDER BY/DISTINCT/UNION-dedup/selectivity all work. KEYSTONE FULLY CLOSED. KEY LESSON: verify in POSTMASTER mode, not `--single` (the latter skips the wire path where real regress bugs live).
- **Now executing broadly:** SELECT/joins/HashJoin Â· aggregates incl. array_agg/string_agg + count(\*)/sum over user tables + count(\*)/sum OVER + **FILTER/DISTINCT aggregates** Â· window funcs (row_number/rank/dense_rank, single+multiple, Â±PARTITION BY) Â· set-ops (UNION/ALL/INTERSECT/EXCEPT; relation-scan dedup cross-root fixed) Â· subqueries (scalar/EXISTS/IN/ANY) Â· **CTEs + WITH RECURSIVE (plan)** Â· **LATERAL** Â· DDL (TABLE/INDEX-build #341/VIEW/SEQUENCE/DOMAIN + PK/UNIQUE) Â· **CREATE TABLE AS** Â· DML (INSERT/INSERTâ¦SELECT/ON CONFLICT; UPDATE/DELETE) Â· **LIMIT/OFFSET** Â· GROUPING SETS (plan stages 1-2) Â· transactions Â· EXPLAIN/SHOW/SET Â· PREPARE/EXECUTE.
- **Stability fixes (shared-cluster enablers):** SIGALRM async-signal-safe SetLatch (960-timer stress, 0 aborts) Â· **5 UAF/drop-order fixes** (tuplesort-over-join, BuildCachedPlan qlist, portal-context-move, TOAST double-close + scan-context, SubLink shallow-clone) Â· per-statement buffer-pin leak. Documented as **TD-STATIC-EROSION** (`'static`-erased arena lifetimes = root of the cross-context UAF / wrong-context-clone class).
- **Measurement keystone:** SHARED-cluster needs per-test crashes eliminated (kill crashes, NOT port WAL-recovery #157). test_setup `tuple concurrently deleted` FIXED (HOT-redirect TID); text-INSERT cluster-killer FIXED (the by-ref keystone). Remaining test_setup gates: **COPY FROM** (needs table_multi_insertâFormedTuple, now unblocked post-flip â re-fire pending) + btbuild #341 â.
- **Node-opaque migration (P3 build-speed payoff):** P1 done; **P2 hard-tail ~complete** â ~16 consumer crates migrated this session (outfuncs 245/readfuncs 77/setrefs/vars/ruleutils/init-subselect/prepjointree/explain/clauses+Expr-tables/coerce/prepagg/func/appendinfo/relnode/**nodes-core**) + infra: **`Expr` tag surface** (`b449f0090`) + **full Expr accessor surface** (`d87652f13`, ~285 methods). Remaining: nodefuncs exprType/Typmod/Collation tablesâexpr_tag + Node-side `as_*_mut` accessors + the mut-walkers (lanes in flight). Then **P2 DONE â P3** (delete enum) â **P4** (split the 446-crate monolith) = the build win. MEASURED: migrated crate compiles ~13% faster (debug); per-crate object ~1.4% larger (debug masks the real release/monomorphization win); enum-vs-trait size needs P3/release to measure. 172/598 seam crates pull the enum (decouple at P4).
- **Infra:** persistent harness â Â· shm-leak fixed â Â· crash-survival â Â· live boot restored â Â· artifacts ~6 GB â **~8 build lanes** Â· reaper shm+prune-only (idle-rm disrupted live lanes, removed).

## ð¥ Critical path (fastest route to test-wins)
1. **boolean.sql first-green** â runs end-to-end; ONLY the `47 vs 49` catalog-row varlena-size `debug_assert` (heap_form_tuple) remains. Fix â our **first fully-green regression file**. (lane in flight)
2. **COPY FROM** (re-fire post-flip) â loads test_setup's onek/tenk1/int*_tbl â unblocks ~half the suite (int/float/join/select sweeps are all test_setup-table-gated).
3. **Hashed agg/distinct/dedup** (`hash_create_memory` #165) â unblocks hashed GROUP BY, DISTINCT, UNION dedup output. (lane in flight)
4. **EXISTS/IN execution** (`is_notclause` + `convert_VALUES_to_ANY`) + **bitmap scan plan** (LIKE-prefix path). (lanes in flight)
5. **By-ref facet sweep** â proactively find remaining non-varlena-by-ref read sites (name/fixed-len types wrongly header-stripped) rather than one regress-file at a time.
6. **Finish P2 migration** (nodes-core/coerce/prepagg/appendinfo) â enables **P3** (delete enum) â **P4** (split types-nodes monolith) = the build-speed payoff.

---

## The 10 categories (status)
1. **pg_regress + correctness bugs** [DRIVER] â boolean workflow active; open diffs: boolin ws, error-position, pg_input_is_valid, func-in-FROM.
2. **Wiring seams** â fmgr dispatch **56/67 installed** (floor: 6 blocked on the fmgr frame-carrier by-ref keystone + 5 fastpath/bgworker); seam-install floor ~163 keystone/FDW-gated.
3. **Finishing partial ports â 176 gaps / 47 crates** â firing: arrayfuncs(19), EXPLAIN(63 structural), planner priorities 2-5; â done: typcache(7), lmgr-lock(15â6), createplan(3â0), misc2(15/16), planner GROUP BY (priority-1); pending: relcache(14, needs policy `a3b93c9f`), xlog(6); deferred off-path: walsender(20), reorderbuffer(9).
4. **Keystones** â see register below.
5. **Entirely-unported units** â datetime registration `a58be86c`; JSON node-model (queued); array_expanded.c (queued); 76 adt crates unregistered (driving to 0).
6. **Registry-completeness** â GUC â **402/404** (floor 2) Â· fmgr seams ð¨ Â· fmgr builtins ð¨ Â· syscache 85/85 â. â ï¸ installing a GUC activates its assign-hook â boot panic if unported.
7. **Crash-resilience** â â DONE (t_bits, TLS-unwind, persistent harness, shm).
8. **Cluster bootstrap / self-hosting** â NOT STARTED (Rust initdb + genbki + StartupXLOG; leans on C initdb today).
9. **Dark subsystems** â replication/WAL, parallel query, extended-query protocol, pgstat.
10. **Verification / faithfulness meta** â audits + guards + DESIGN_DEBT.

---

## ð§± KEYSTONE REGISTER (all of them)

### â Resolved
- **K1 Â· #159 plancache F0** â `cd85c8d63`
- **K2 Â· Crash-reinit / TLS-during-unwind** â `d0c9149ab`
- **K3 Â· Datum by-ref bridge (core)** â `aa47aba39`

### ð¯ Decided & sequenced (near-term; committed)
- **K4 Â· Datum by-ref TAIL** â `datumCopy`/`datumFree` by-ref arms + agg trans-value copy. Blocks numeric `sum`/`avg`. *Deep-type* â **next keystone up**, firing into the next thin window.
- **K5 Â· JSON node-model + grammar** â 12 raw nodes â types-nodes + grammar + ~25 transforms + executor eval; JSON_TABLE carved out. *Deep-type, committed, Lane 0 queued.*
- **K6 Â· Expanded-array** â `array_expanded.c`; bounded A+B (~700 LOC) for `array_append`/`prepend`; Lane C folds into K4. *Queued.*

### â Blocked / deferred (gate *later* test classes â do NOT block correct results now)
- **K7 Â· planagg MinMaxAggPath subroot** â `min`/`max` *index* optimization. Correctness unblocked via fall-through (`a4ba09d6`); only speed deferred.
- **K8 Â· set-op UNION/INTERSECT/EXCEPT** â **COMMITTED**; 3/4 blockers resolved (subquery_planner + estimates + all builders/converters/exec nodes real). Prereq = **cross-root path-arena import primitive** *(firing `a9f0a896`)*, then K8's clean 5-lane port.
- **K9 Â· PREPARE / extended-protocol** â **COMMITTED**; foundation done (portal arena, SELECT 1 via portal) + `prepare.c` fully ported (957 lines) but blocked on the **param-list de-handle** *(firing `a7869f86`, deep slot)* + the #167/#169 cursor/EPQ leg.
- **K10 Â· DshashKeyKind `Custom{compare,hash,copy}`** â parallel-worker shared record typmod registry. Single-backend **SIDESTEPPED** (`44ad03cf1` uses local registry); only parallel-query needs it.

### ð¡ Known longer-term keystones (each gates a specific feature/test area; re-validate when reached)
- **FormedTuple carrier** â header-only tuple vs full content; being chipped. Gates trigger slots, MERGE, inval.
- **TriggerData carrier-widen + WHEN-qual / ResultRelInfo-trim** â gates triggers (`triggers.sql`, RI/FK).
- **GRANT executor / aclchk** â gates GRANT/REVOKE/privileges.
- **pgstat per-kind callback registry** â gates statistics views.
- **partprune PartitionPruneStep carrier** â gates runtime partition pruning.
- **copyfrom CopyParseState handle** â gates COPY FROM.
- **TableAmRoutine mcx-free vtable** â gates custom table AMs.
- **ruleutils deparse_namespace** â gates EXPLAIN VERBOSE / `pg_get_*def` / rule deparse.
- **IndexCreateArgs widen** â gates some CREATE INDEX paths.
- **ts_lexize fmgr-dispatch** â gates full-text search.
- **proc/procarray-private accessors** â FastPath F3 lock family (per-slot `fpRelId[]`/`fpLockBits[]` + cross-proc `allProcs` iterator) + blocker-status reporters (`BackendPidGetProc`, `lockGroupMembers`). Gates lmgr-lock's last 6 + pg_locks blocker views.
- **prepunion subroot / cross-root PathId** â set-op child path import (relates to K8).
- **fmgr frame-carrier by-ref widening** (TD-FMGR-GETARG-BYREF) â trimmed `types_nodes` FunctionCallInfoBaseData carries bare-word args, no by-ref channel â blocks `pg_getarg_{name,text_pp,varlena_pp,cstring}` + `typmodin`. **typmodin RESOLVED** (`b14ea3fab` â installed via cstring[]-build + OidFunctionCall1; unblocked char/numeric/time/interval typmods + the test_setup cascade). Remaining: by-ref scalar-accessor for timetz / XMLELEMENT-content / map_sql_value (by-ref Datum â stable pointer). Cousin of K4.
- **fn_expr-through-Func-step** (TD-FMGR-FN-OID-AND-EXPR-NODE) â by-OID Func dispatch drops `flinfo->fn_expr`, so `get_fn_expr_rettype/argtype`â0 â **polymorphic functions** (range/array/enum constructors) fail "type OID 0" *(firing `af31b5cf`)*. Same cluster as frame-carrier.
- **executor grouping/equality** â nodeAgg per-group + nodeUnique de-dup (DISTINCT) via `exec_tuples_match`. GROUP BY + DISTINCT both PLAN â but don't yet group/de-dup at exec *(GROUP-BY-exec firing `a048453e`; DISTINCT de-dup follows same machinery)*.
- **LockRows path carrier + EPQ** (EvalPlanQual) â FOR UPDATE planning â; needs `LockRowsPath.rowMarks`â`PlanRowMarkId` carrier + `create_lockrows_plan` + the EvalPlanQual executor. Gates FOR UPDATE/SHARE.
- **make_const by-ref const-fold** (parse_node.c:418/455) â numeric (`1.5`), bit (`B'101'`), and ALL by-ref-typed LITERALS can't fold into a `Const` â **huge fraction of suite blocked**. K4 cluster. *(firing `ae958c49`)*.
- **tablecmds `alter_table_slow`** â the ALTER TABLE engine (ADD/ALTER COLUMN, constraints, ENABLE RLS). Gates most `ALTER TABLE` across the suite; ~22k LOC campaign.
- **FK-join selectivity / ForeignKeyOptInfo arena** (`get_foreign_key_join_selectivity`, `root->fkey_list`) â unmodeled arena-handle; gates FK-aware join selectivity.

> Legend: K1âK3 done Â· K4âK6 committed/sequenced Â· K7âK10 deferred Â· ð¡ longer-term, feature-area-gated.

---

## Active lanes
**(15-lane goal)** `ac81ca4b` DDL/DML Â· `aadeb37f` HAVING Â· `a9119f58` avg Â· `a091e9e9` window fns Â· `a9c4410a` SRF Â· `ae91a044` RowExpr Â· `a70606ab` ruleutils-deparse Â· `a638453d` datetime sweep Â· `a30047d4` expr-node sweep Â· `ada1d774` evalexpr const-fold Â· `a4ea9f8b` xml Â· `a9f0a896` **K8-prereq path-arena** Â· `ae6e5cd6` json Â· `a7869f86` **K9-prereq param-list (deep slot)** Â· `a3e10fdd` build-size

## Build/infra notes
- Artifacts ~6 GB/build (was ~10â14): `713252a14` (line-tables-only) + `c794aeac6` (**`incremental=false` now on `[profile.dev]`** â the lanes' default build; was only on the unused fast-check profile). Cap **~8**, keep ~20 GB buffer.
- **Rebuild cost = crate depth.** Leaf crates rebuild cheap â fan out freely. **Deep types** (`types-nodes`/`types-tuple`/`types-datum`) invalidate the world â **serialize** K4/K5/K6 into thin windows.
- Reaper = shm + `git worktree prune` ONLY (idle-based auto-rm disrupted live lanes twice; lanes self-clean + orchestrator reaps on pressure).

## Recently landed
**hashed agg/distinct/UNION-dedup (by-value keys) â** Â· **bitmap scan plan (BitmapHeapScan/Or/And) â** Â· **EXISTS/NOT EXISTS execute â** Â· **text concat/format/format_nv â** Â· **RESET datestyle/timezone â** Â· **set-op cross-root range-table (relation-scan legs) â** Â· **P2 migration: nodes-core/coerce/prepagg/func/appendinfo/relnode + Expr tag+accessor surface â** Â· **â BY-REF DATUM header-ful flip (keystone) â** Â· **UPDATE/DELETE round-trip execute â** Â· **boolean runs end-to-end (1 debug_assert from green) â** Â· **EXTRACT/datetime (post-flip) â** Â· **LIMIT/OFFSET â** Â· **CREATE TABLE AS â** Â· **WITH RECURSIVE (plan) â** Â· **LATERAL â** Â· **aggregate FILTER/DISTINCT â** Â· **GROUPING SETS plan stages 1-2 â** Â· **btbuild #341 (index build) â** Â· **set-op relation-scan cross-root dedup â** Â· **SubLink deep-clone â** Â· **create_group_path (GROUP BY no-agg) â** Â· **placeholder jointree â** Â· **patternsel/LIKE estimators â** Â· **5 UAF/drop-order + buffer-pin-leak fixes â** Â· **P2 migration hard-tail: outfuncs/readfuncs/setrefs/vars/ruleutils/init-subselect/prepjointree/explain/clauses + Expr tag surface â** Â· **test_setup: "tuple concurrently deleted" FIXED (HOT-redirect TID) â** Â· **execMain constraint cluster (ExecConstraints/RelCheck/PartitionCheck/NOT-NULL, installed) â** Â· **serial CREATE INDEX (parallel-worker crash â 0 workers) â** Â· **jsonb by-ref header (JSONBOID verbatim â []/{}/nested render) â** Â· **PKâIndexStmt indexParams (pkey clone ordering) â** Â· **UPDATE/DELETE execute (ctid-junk by-ref Datum) â** Â· **SIGALRM async-signal-safe SetLatch (RefCell-reentrancy abort fix) â** Â· **tuplesort UAF over join (multi-key ORDER BY) â** Â· **BuildCachedPlan qlist UAF + EXECUTE no-param â** Â· **window funcs exec: row_number/rank/dense_rank, single+multiple, Â±PARTITION BY â** Â· **count(\*)/sum OVER window aggregates (initialize_peragg) â** Â· **count(\*)/count(col)/sum over user tables (Aggref::clone_in #280) â** Â· **process_utility_wrapper (recursive sub-DDL) â** Â· **set_attnotnull (PK NOT NULL) â** Â· **GROUPING SETS stage-1 (expand_grouping_sets + Node::IntList) â** Â· **mark_nullable_by_grouping â** Â· **numeric decimal literals (1.5) by-ref fix â** Â· **ARRAY['a','b'] (header-less varlena elements) â** Â· **container-varlena verbatim I/O (fix ARRAY type-3) â** Â· **EXISTS over zero rows (TupIsNull) â** Â· **CreateExecutorState seam â** Â· **node-opaque migration P2 (~55 consumer crates) ð** Â· **INSERTâ¦SELECT â** Â· **ON CONFLICT (analyze+plan+createplan spine + infer_arbiter) â** Â· **CREATE TABLE PK/UNIQUE (transformIndexConstraintCatalog) â** Â· **CREATE DOMAIN + CoerceToDomain â** Â· **IN (hashed) / ANY (materialized) subquery exec â** Â· **non-recursive CTEs â** Â· **UNION ALL â** Â· **INTERSECT/EXCEPT â** Â· **RETURNING (analyze+plan) â** Â· **array_agg/string_agg + numeric internal-state aggregates â** Â· **SHOW/SET/RESET â** Â· **PREPARE/DEALLOCATE + EXECUTE portal de-handle â** Â· **pg_input_is_valid/pg_input_error_info (SRF OID 6211) â** Â· **by-ref Datum arg/output bridge (incl. 1-byte short header) â** Â· **postmaster survives backend crash (no shmem double-init) â** Â· **node-opaque migration P2 (~37 consumer crates migrated) ð** Â· **ARRAY-coerce (int[]::text[]) â** Â· vacuum seams Â· **DDL: CREATE INDEX/VIEW/SEQUENCE â** Â· **DML: UPDATE/DELETE spine** Â· **tableam index-fetch + unique checks â** Â· **xlog heap_update WAL-persist â** Â· **varlena-header Datum bridge (char/varchar store) â** Â· **ScalarArrayOp (IN-list) by-ref â** Â· **transaction blocks (BEGIN/COMMIT/SAVEPOINT/PREPARE TXN) â** Â· **K8 UNION â** Â· **scalar subqueries + EXISTS â** Â· **EXPLAIN/SHOW row-buffering â** Â· **T_List walker (FROM VALUES) â** Â· **HINT/DETAIL emission (tree-wide) â** Â· **node-opaque migration P1 (1385 accessors, additive) â** Â· math fns â Â· oidvector/reg* â Â· **CASE/COALESCE/GREATEST/LEAST â** Â· **to_json/to_jsonb/array_to_json â** Â· **text[] literals (by-ref array I/O) â** Â· oidvector I/O Â· **FROM-clause SRFs (generate_series) â** Â· **HashJoin exec â** Â· **CoerceViaIO/RelabelType casts â** Â· **transaction blocks (BEGIN/COMMIT/SAVEPOINT) â** Â· **K9 param-list de-handle â** Â· internal-agg Internal substrate Â· pull_varnos_contains_zero (ORDER BY 2,1) Â· **numeric/bit literals (make_const) â** Â· **polymorphic fns (fn_expr) â** Â· evaluate_expr const-fold â Â· ruleutils deparse (pg_get_expr) â Â· RowExpr parser Â· **avg(int) â** (finalize_aggregate ported) Â· **HAVING â** Â· window planning Â· **typmodin installed** (char(N)/numeric(p,s)/time(N) â #1 type-test blocker; interval 36â11) â Â· ARRAY[1,2,3] â Â· string fns â Â· xml exprs â Â· jsonb ops â Â· **GROUP BY exec â** Â· **DISTINCT de-dup â** Â· cross/non-equi joins â Â· ALTER TABLE spine + ENABLE RLS Â· CREATE TEMP TABLE Â· record-typmod (local registry) Â· geo (74 OIDs) Â· **INNER/LEFT/FULL JOIN â** Â· **2-table joins** â Â· **DISTINCT + FOR-UPDATE planning** Â· **count/min/max/sum aggregates** â Â· type-adt registration: inet/enum/cash/range/multirange/acl/bit/geo Â· arrayfuncs (sort/shuffle/sample) Â· EXPLAIN structural Â· **boolean error-position (LINE/caret) + varlena cstring-NUL** Â· policy/RLS (CREATE POLICY + RelationBuildRowSecurity) Â· proc_arg_attrs Â· count(\*) exec (#165) Â· **GROUP BY planning** (+ real parser p_rtable bug fix) Â· misc2 (15/16) Â· **count(\*) FROM pg_class = 415** (setrefs varno + IndexOnlyScan) Â· fmgr builtin registry 2003â1574 (0 mismatches) Â· cargo dev incremental=0 Â· min/max planner fall-through Â· createplan (unique/groupingsets/async, 3â0) Â· GUC registry 402/404 Â· typcache complete Â· lmgr-lock (15â6) Â· fmgr Phase-1 seams (56/67) Â· bool.c 100% (type correct e2e) + boolean parser/func-RTE fixes Â· io_combine_limit boot fix Â· Datum by-ref bridge (+saophash) Â· crash-reinit (TLS-unwind) Â· seclabelâDROP Â· commentâDROP Â· multi-row VALUES Â· parse_expr XML Â· t_bits crash Â· setrefs Aggref fixup Â· smaller build artifacts Â· plancache F0

## DROP status
CREATEâINSERTâSELECT â Â· DROP: comment â â seclabel â â **next wall `relation_is_nailed`** (tablecmds seam).
