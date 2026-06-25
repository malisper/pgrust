# Audit: backend-executor-nodeSamplescan

Independent function-by-function audit of `port/backend-executor-nodeSamplescan`
against `src/backend/executor/nodeSamplescan.c` (PostgreSQL 18.3), the c2rust
rendering (`../pgrust/c2rust-runs/backend-executor-nodeSamplescan/`), and the C
headers. Re-derived from sources; the port's comments/self-review were not
trusted.

## Scope

`c_sources` (CATALOG): `src/backend/executor/nodeSamplescan.c` — a single C file,
8 function definitions. The port additionally reproduces the `execScan.c` driver
(`ExecScan` / `ExecScanExtended` / `ExecScanFetch`) that `ExecSampleScan` calls;
in C `ExecScan` is a non-inlined `execScan.c` function and `ExecScanExtended` /
`ExecScanFetch` are `static pg_attribute_always_inline` helpers in
`include/executor/execScan.h`. These are audited too since the node's control
flow depends on them.

## Function inventory and verdicts

| C function (nodeSamplescan.c) | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| `SampleNext` (static) | :41 | `SampleNext` :64 | MATCH | `if (!begun) tablesample_init; return tablesample_getnext` — identical. |
| `SampleRecheck` (static) | :59 | `SampleRecheck` :81 | MATCH | Unconditional `Ok(true)` — identical. |
| `ExecSampleScan` (static, ExecProcNode) | :78 | `ExecSampleScan` :92 | MATCH | Delegates to `ExecScan(node, SampleNext, SampleRecheck)`; method pointers modeled as fn items. |
| `ExecInitSampleScan` | :92 | `ExecInitSampleScan` :312 | MATCH | makeNode→owned struct; both `Assert(outerPlan/innerPlan==NULL)`→`debug_assert`; expr-context, scan-relation open, slot init, result-type/projection, qual/args/repeatable compile, no-REPEATABLE random seed, `GetTsmRoutine`, optional `InitSampleScan`, `begun=false`. Field reads/leaf ops are seamed (see seam audit). |
| `ExecEndSampleScan` | :178 | `ExecEndSampleScan` :399 | MATCH | optional `EndSampleScan`; `if scan_desc table_endscan`. |
| `ExecReScanSampleScan` | :201 | `ExecReScanSampleScan` :414 | MATCH | resets `begun/done/haveblock=false`, `donetuples=0`, then `ExecScanReScan`. Field writes are own-logic in this crate; `ExecScanReScan` is the execScan.c owner (seam). |
| `tablesample_init` (static) | :217 | `tablesample_init` :434 | MATCH | `donetuples=0`; params array sized `list_length(args)`; per-arg eval w/ null→`ERRCODE_INVALID_TABLESAMPLE_ARGUMENT`; REPEATABLE branch eval w/ null→`ERRCODE_INVALID_TABLESAMPLE_REPEAT`, else `hashfloat8`; non-REPEATABLE uses `scanstate.seed`; `use_bulkread=use_pagemode=true`; `BeginSampleScan`; `allow_sync = (NextSampleBlock==NULL)`; beginscan-vs-rescan branch on `ss_currentScanDesc==NULL`; `begun=true`. `pfree(params)` modeled by per-query context reclaim (documented; behaviour-preserving). |
| `tablesample_getnext` (static) | :319 | `tablesample_getnext` :515 | MATCH | `ExecClearTuple`; `if done return NULL`; for(;;) loop: block fetch sets done+returns NULL on exhaustion; tuple fetch failure clears `haveblock` and continues; success breaks; `donetuples++`. |

### execScan.c driver (linked into nodeSamplescan.o / inlined headers)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `ExecScan` | execScan.c:46 | `ExecScan` :255 | MATCH | reads `es_epq_active`, `qual`, `ps_ProjInfo`; calls `ExecScanExtended`. epq_active modeled as bool flag via seam; qual/proj presence read from local typed fields. |
| `ExecScanExtended` | execScan.h:159 | `ExecScanExtended` :187 | MATCH | no-qual/no-proj fast path; reset-econtext; loop fetch; NULL→clear proj resultslot or return; set `ecxt_scantuple`; `qual==NULL || ExecQual`; project-or-return; `InstrCountFiltered1` on fail; reset and retry. |
| `ExecScanFetch` | execScan.h:30 | `ExecScanFetch` :108 | MATCH | `CHECK_FOR_INTERRUPTS`; EPQ-active block with all four sub-branches (scanrelid==0 ext-param recheck; relsubs_done; relsubs_slot replacement; relsubs_rowmark fetch) reproduced with identical predicates, `relsubs_done=true` marks, TupIsNull checks, recheck/clear semantics; else access method. The C `relsubs_slot` branch rebinds `slot` to the replacement slot; the port copies the replacement into the scan slot (`epq_load_relsubs_slot`) then checks/rechecks the scan slot — behaviour-equivalent. |
| `InstrCountFiltered1` (macro) | executor.h | `InstrCountFiltered1` :278 | MATCH | guarded `instrument.nfiltered1 += delta`. |

## Constants verified against headers

- `ERRCODE_INVALID_TABLESAMPLE_ARGUMENT` (SQLSTATE 2202H) and
  `ERRCODE_INVALID_TABLESAMPLE_REPEAT` (2202G) — both error paths fire under the
  same `isnull` predicate as C; tests assert the SQLSTATEs.
- `SampleScanState` struct (execnodes.h) compared field-for-field against
  `types-samplescan::SampleScanState`: `ss, args, repeatable, tsmroutine,
  tsm_state, use_bulkread, use_pagemode, begun, seed, donetuples(int64→i64),
  haveblock, done` all present, correct types/widths. `ss_currentRelation` /
  `ss_currentScanDesc` are split out of `ScanStateData` per repo convention but
  carry the C `ss.ss_currentRelation` / `ss.ss_currentScanDesc` semantics.

## Seam audit

Owned seam crates by C-source coverage: `backend-executor-nodeSamplescan-seams`
(maps to nodeSamplescan.c). All declarations in that crate are **outward** —
leaf operations owned by other, still-unported subsystems:

- table access methods: `table_beginscan_sampling`, `table_rescan_set_params`,
  `table_endscan`, `table_scan_sample_next_block`, `table_scan_sample_next_tuple`;
- expression compile/eval: `exec_init_qual`, `exec_init_expr_list`,
  `exec_init_repeatable_expr`, `exec_eval_arg_in_per_tuple_context`,
  `exec_eval_repeatable_in_per_tuple_context`, `exec_qual`, `exec_project`;
- execUtils/execScan init: `init_plan_state_links`, `exec_assign_expr_context`,
  `exec_open_scan_relation`, `exec_init_scan_tuple_slot`,
  `exec_init_result_type_tl`, `exec_assign_scan_projection_info`,
  `exec_scan_rescan`;
- tablesample registry/callbacks (tsmapi.h): `get_tsm_routine`,
  `tsm_has_init_sample_scan`, `tsm_init_sample_scan`, `tsm_begin_sample_scan`,
  `tsm_has_next_sample_block`, `tsm_has_end_sample_scan`, `tsm_end_sample_scan`;
- PRNG/hash: `pg_prng_uint32_global`, `hashfloat8`;
- execScan.c leaf ops / scan-slot plumbing: `check_for_interrupts`,
  `reset_per_tuple_expr_context`, `set_econtext_scantuple_to_scan_slot`,
  `exec_clear_scan_tuple`, `exec_clear_proj_result_slot`;
- EvalPlanQual field access on the foreign-owned `EPQState`: `scan_scanrelid`,
  `es_epq_active_present`, `epq_param_is_member_of_ext_param`,
  `epq_relsubs_done`, `epq_set_relsubs_done`, `epq_relsubs_slot_present`,
  `epq_load_relsubs_slot`, `epq_relsubs_rowmark_present`,
  `eval_plan_qual_fetch_row_mark`.

Each seam is a thin marshal+delegate to a real cross-crate leaf operation backed
by a genuine dependency on an unported owner (table AM, execExpr/execExprInterp,
execUtils/execScan, tsmapi, pg_prng, EPQState/execMain). No branching, node
construction, or computation lives in a seam path — all control flow (the
init/end/rescan sequences, the getnext loop, the execScan driver loop and EPQ
branch tree) is implemented **in this crate**. No function body was replaced by a
"do it elsewhere" seam.

The node owns **no inward-facing seam**, so `init_seams()` is correctly empty;
the outward declarations are installed by their owning subsystems when they land.
`init_seams()` is wired into `seams-init::init_all()` (lib.rs:78). The
`seams-init` recurrence guard
(`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) passes.

## Design conformance

- No invented opacity: `tsm_state` is `Option<Opaque>` (the C `void *` the
  tablesample method keeps for itself); `TsmRoutine`/`Relation`/`TableScanDesc`
  are real typed handles carried, not stand-ins.
- Allocating paths (`vec_with_capacity_in` for the node and the params array) are
  `Mcx`-charged (`estate.es_query_cxt`) and return `PgResult`; OOM maps to
  `ERRCODE_OUT_OF_MEMORY`.
- Error surface mirrors the C: the only `ereport(ERROR…)` sites are the two
  null-param checks, both reproduced with matching SQLSTATE/severity.
- No shared statics for per-backend globals; no ambient-global seams beyond the
  documented `pg_global_prng_state` PRNG read (faithful to C).
- No `todo!`/`unimplemented!`/`unreachable!`/own-logic stubs and no unledgered
  divergence markers (grep clean).

## Gates

- `cargo check --workspace` — passes (warnings only, unrelated crates).
- `cargo test -p backend-executor-nodeSamplescan` — 13 passed, 0 failed.
- `cargo test -p seams-init` — 2 passed (both recurrence-guard checks), 0 failed.

## Verdict

**PASS.** Every function MATCH; every seam is a justified thin outward delegate;
init_seams wired and guard-clean; no stubs/divergence; constants verified against
headers. CATALOG row set to `audited`.
