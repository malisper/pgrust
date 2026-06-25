# Audit: backend-commands-prepare (independent /audit-crate pass)

- **Unit:** `backend-commands-prepare`
- **C source:** `src/backend/commands/prepare.c` (PostgreSQL 18.3, 760 lines)
- **c2rust reference:** `../pgrust/c2rust-runs/backend-commands-prepare/src/prepare.rs`
- **Port:** `crates/backend-commands-prepare/src/lib.rs`
- **Branch:** `port/backend-commands-prepare`
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

## Reconcile re-audit (2026-06-13): `exec_prepare_expr_list` seam collision — PASS

- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

Merged current `refs/heads/main` (8325f593+) into the branch. The blocking
collision: `exec_prepare_expr_list` in `backend-executor-execExpr-seams` existed
twice — prepare's branch declared it with introduced-opacity stand-ins
(`EStateHandle`, `&[PgBox<Node>]`, `ExprStateHandle`), while the already-merged
execPartition consumer declared the canonical real-typed signature
`fn(&[Expr], &mut EStateData) -> PgVec<PgBox<ExprState>>`. Per
opacity-inherited-never-introduced the real-typed signature is canonical;
prepare's handles were the violation. Resolution, re-derived against
`prepare.c`'s `EvaluateParams`/`ExecuteQuery`/`ExplainExecuteQuery` and the
c2rust run:

- **Dropped** prepare's duplicate opaque `exec_prepare_expr_list` declaration;
  the single canonical real-typed seam remains (verified: exactly one definition).
- **`EvaluateParams`** (lib.rs:359) now builds a working `PgVec<Expr>` from the
  analyzed parameters and threads `params_work.as_slice()` (`&[Expr]`) and
  `&mut EStateData` into the canonical seam, reading back real `&ExprState`. The
  per-param loop, the two ereports (SYNTAX_ERROR 42601 / DATATYPE_MISMATCH 42804
  with hint + `parser_errposition` cursor), `makeParamList`, and the lock-step
  eval loop are unchanged in logic — re-derived line-by-line against C
  (prepare.rs:3176-3356). **MATCH.**
- **Adapted prepare's other handle-threading seams to real types** (none were
  installed by any owner, so contracts were free to fix):
  `create_executor_state(mcx) -> PgBox<EStateData>`, `free_executor_state` now
  consumes the owned `PgBox<EStateData>`, `eval_exec_param_into_list` takes
  `&ExprState` + `&mut EStateData`. Removed the `estate_set_param_list_info`
  seam: setting `es.es_param_list_info = params` is now a plain field write on
  the owned real struct, faithfully mirroring C `estate->es_param_list_info =
  params;` (prepare.c:182, 627). Both `ExecuteQuery` and `ExplainExecuteQuery`
  call sites updated (create → field-set → EvaluateParams → conditional free).
- **`analyze_one_exec_param`** (parse-expr-seams) now returns the analyzed real
  `Expr` per parameter (`Option<PgBox<Expr>>` + the unchanged failure metadata),
  so the driver collects the `lfirst(l) = expr` working list. Still one call to
  one owner; coercion-failed ereport reproduced in-crate. **Acceptable** (S1).
- **`EStateData::es_param_list_info`** added (execnodes.rs) carrying the
  `ParamListInfo` as the *inherited* opaque handle (params unit unported) — it
  lands with prepare, its first consumer, per types.md rule 3.
- **Retired** the now-orphaned `parsestmt::EStateHandle` /
  `parsestmt::ExprStateHandle` introduced-opacity stand-ins (verified: no
  remaining references anywhere).

Remaining opacity is strictly inherited: `ParamListInfoHandle` (params unit
unported, returned by the `make_param_list` owner seam) and `CachedPlan*`/portal
handles — no invented opacity is introduced by this reconcile.

Incidental merge conflicts resolved by union/dedup: dep-list conflicts
(parse-analyze/snapmgr/plancache/types-nodes Cargo.toml), the two-consumer-slice
`backend-parser-analyze-seams` and `backend-utils-cache-plancache-seams` lib.rs
(disambiguated the two `RawStmt` types — `copy_query` vs `parsestmt`), the
`backend-utils-resowner-resowner-seams` and `arrayfuncs-seams` import/seam
unions, and a duplicate `get_current_statement_start_timestamp` seam in
`backend-access-transam-xact-seams` (both sides added it; de-duplicated).
`Cargo.lock` regenerated.

Gate after reconcile: `cargo check --workspace` clean (warnings only, in
unrelated crates); `cargo test --workspace` all green (0 failed). The 14
prepare.c functions remain MATCH (the table below); the seam collision is
resolved and no introduced opacity remains. Verdict: **PASS**.

---

## Sync re-audit (2026-06-13): merge of current `main` — PASS

Merged current `refs/heads/main` into the branch and reconciled the named
shared-vocabulary collision in `backend-parser-parse-type-seams`, plus two
merge-surfaced fallout points. main's shapes are authoritative.

- **Seam collision (`typename_type_id`).** main rewrote the seam to
  `fn typename_type_id(type_name: &types_opclass::TypeName) -> PgResult<Oid>`
  (concrete `TypeName` with the real grammar fields
  `names`/`typeOid`/`setof`/`pct_type`/`typemod`/`location`) and added
  `typename_to_string`; it dropped `source_text`. The branch had carried each
  argtype as an opaque parser node (`parsestmt::TypeName { node: PgBox<Node> }`)
  and passed `p_sourcetext` to the seam. Reconciled to main's seam: removed the
  opaque `parsestmt::TypeName`, made `PrepareStmt::argtypes` carry the concrete
  `types_opclass::TypeName` (the same vocabulary `opclasscmds` established — the
  grammar's `makeTypeName` fixes these fields, so this is *inherited* concrete
  structure, not an invented decomposition), and the call site now passes `tn`
  straight through (`typename_type_id::call(tn)`). C `typenameTypeId(pstate, tn)`
  threads pstate only for `parser_errposition`; main's seam mirrors PostgreSQL's
  own `typenameTypeId(NULL, …)` entry, so dropping `source_text` is faithful and
  consumer-driven. The PrepareQuery loop logic (nargs/palloc/foreach/push) is
  byte-for-byte unchanged.
- **`SnapshotHandle` relocation.** main removed `types-scan::snapshot` and
  re-homed `SnapshotHandle` in `types-execparallel`. Repointed the branch's two
  consumer-slice seam crates (`backend-utils-time-snapmgr-pre-seams`,
  `backend-tcop-pquery-pre-seams`) to `types_execparallel::SnapshotHandle` (dep +
  import). Still inherited opacity; only the home crate changed.
- **Mechanical unions.** `backend-access-transam-xact::init_seams()` — kept both
  sides' `set()` calls (HEAD added `get_current_statement_start_timestamp`; main
  added `is_in_parallel_mode`/`require_transaction_block`/`xact_redo`/
  `xact_log_{commit,abort}_record`; all installer fns verified present).
  `docs/types.md` and `Cargo.lock` (duplicate package entry) resolved to main's
  rows + the de-duplicated parse-type-seams entry.

Gate after reconcile: `cargo check --workspace` clean (warnings only, all in
unrelated crates); `cargo test --workspace` all green. Verdict stands: **PASS**.

---

## Prior audit (2026-06-12)

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`

> Independent from-scratch re-audit (sources re-derived; port comments / prior
> self-review / green build not trusted). This pass (a) confirms the
> previously-failing F1 is fully resolved at root, and (b) found and fixed one
> new transcribed-constant defect (F2). Both are now clean.

## Top-line verdict: **PASS**

- **F1 (was FAIL):** `PrepareQuery` dropping `stmt_location`/`stmt_len` —
  **RESOLVED**, verified byte-for-byte this pass (see below).
- **F2 (found + fixed this pass):** `CURSOR_OPT_PARALLEL_OK` const transcribed as
  `0x0400` (= `CURSOR_OPT_CUSTOM_PLAN`); the header value is `0x0800`. Fixed.

## Function inventory & verdicts

All 14 C function definitions enumerated from prepare.c (3 static:
`InitQueryHashTable`, `EvaluateParams`, `build_regtype_array`; 11 extern) and
cross-checked against the c2rust rendering (all 14 present at prepare.rs:2883,
2998, 3176, 3357, 3382, 3446, 3505, 3515, 3526, 3534, 3552, 3581, 3779, 3866).
Every one has a Rust counterpart. The port adds two private helpers (`hash_key`,
`make_raw_stmt`).

| # | C function (prepare.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `PrepareQuery` (58) | `PrepareQuery` (127) | **MATCH** | Empty-name guard (INVALID_PSTATEMENT_DEFINITION); `make_raw_stmt` now builds a real `RawStmt` carrying `stmt`/`stmt_location`/`stmt_len`; argtypes TypeName→OID loop gated on `nargs`; CreateCachedPlan/analyze+rewrite/CompleteCachedPlan/Store all threaded through the RawStmt. See F1 (resolved). |
| 2 | `ExecuteQuery` (149) | `ExecuteQuery` (223) | MATCH | fixed_result elog, EvaluateParams gated on `num_params>0`, portal create+visible=false+strdup, GetCachedPlan→PortalDefineQuery with no fallible step between, intoClause `!=1`/`!=CMD_SELECT` (WRONG_OBJECT_TYPE), eflags/count selection, PortalStart/Run/Drop, conditional FreeExecutorState. |
| 3 | `EvaluateParams` (280, static) | `EvaluateParams` (352) | MATCH | `nparams!=num_params` (SYNTAX_ERROR + errdetail) before `num_params==0`→NULL; copyObject→clone-into working vec; per-param transform/exprType/coerce/assign_collations bundled in parser seam (S1); coercion-failed ereport (DATATYPE_MISMATCH + hint + errposition) reproduced in-crate; makeParamList + lock-step eval loop. |
| 4 | `InitQueryHashTable` (371, static) | `InitQueryHashTable` (462) | MATCH | Lazy `None`→`Some(HashMap::with_capacity(32))`; idempotent guard preserves the `!prepared_queries` precondition. |
| 5 | `StorePreparedStatement` (391) | `StorePreparedStatement` (476) | MATCH | GetCurrentStatementStartTimestamp first, lazy init, duplicate-key check (DUPLICATE_PSTATEMENT) before insert, fields filled, then SaveCachedPlan. NAMEDATALEN-1 truncation via `hash_key`. |
| 6 | `FetchPreparedStatement` (433) | `FetchPreparedStatement` (528) | MATCH | `prepared_queries==NULL ⇒ NULL`; `!entry && throwError`→UNDEFINED_PSTATEMENT; else `Ok(None)`. |
| 7 | `FetchPreparedStatementResultDesc` (465) | `FetchPreparedStatementResultDesc` (555) | MATCH | `Assert(fixed_result)`→`debug_assert!`; `resultDesc ? CreateTupleDescCopy : NULL`. |
| 8 | `FetchPreparedStatementTargetList` (488) | `FetchPreparedStatementTargetList` (579) | MATCH | CachedPlanGetTargetList + copyObject delegated; empty Vec == NIL. |
| 9 | `DeallocateQuery` (504) | `DeallocateQuery` (593) | MATCH | `name ? DropPreparedStatement(name,true) : DropAllPreparedStatements()`. |
| 10 | `DropPreparedStatement` (518) | `DropPreparedStatement` (608) | MATCH | Fetch with `showError`, then (if found) DropCachedPlan + remove by `stmt_name`. |
| 11 | `DropAllPreparedStatements` (540) | `DropAllPreparedStatements` (631) | MATCH | `!prepared_queries`→empty snapshot→no-op; else drop+remove each. Snapshot-then-drain behaviorally equivalent to the hash_seq scan (removal keyed by `stmt_name`; entries are independent owned copies, no dangling iterator). |
| 12 | `ExplainExecuteQuery` (570) | `ExplainExecuteQuery` (661) | MATCH | begin bookkeeping, fetch, fixed_result elog, optional EvaluateParams (pstate passed straight through — EvaluateParams reads only p_sourcetext, which C copies into the throwaway pstate_params), GetCachedPlan(CurrentResourceOwner, p_queryEnv), planduration, memory/buffer accounting guarded in-crate, per-plan CMD_UTILITY vs ExplainOnePlan branch, ExplainSeparatePlans between, FreeExecutorState, ReleaseCachedPlan. See S2. |
| 13 | `pg_prepared_statement` (684) | `pg_prepared_statement` (802) | MATCH | InitMaterializedSRF, null guard, single-scan snapshot, 8 values / nulls[8]={0}, name/query_string text, prepare_time int64, build_regtype_array(param_types), resultDesc? result_types regtype[] : nulls[4]=true, from_sql bool, num_generic/custom_plans int64, putvalues; returns `(Datum)0`. |
| 14 | `build_regtype_array` (746, static) | `build_regtype_array` (889) | MATCH | ObjectIdGetDatum per element, `construct_array_builtin(...,REGTYPEOID=2206)`; empty input → zero-element array (not NULL). |

Helpers: `hash_key` (NAMEDATALEN=64, char-boundary-safe truncation) — correct.
`make_raw_stmt` — builds the real `RawStmt` carrying both span fields (F1 fix).

## Constants verified against headers (re-derived, not from memory)

- `NAMEDATALEN=64` (`pg_config_manual.h:29`) — match.
- `FETCH_ALL=LONG_MAX` (`parsenodes.h:3425`); LP64 `long` → `i64::MAX` — match.
- `PARAM_FLAG_CONST=0x0001` (`params.h:88`) — match.
- `REGTYPEOID=2206` (`pg_type.dat:389`) — match.
- `CURSOR_OPT_PARALLEL_OK=0x0800` (`parsenodes.h:3390`) — **F2: port had
  `0x0400` (= `CURSOR_OPT_CUSTOM_PLAN`, `parsenodes.h:3389`). Fixed this pass.**
- `num_generic_plans`/`num_custom_plans` are `int64`; port uses
  `Datum::from_i64`. Match.
- SQLSTATEs as used: INVALID_PSTATEMENT_DEFINITION, DUPLICATE_PSTATEMENT,
  UNDEFINED_PSTATEMENT, WRONG_OBJECT_TYPE, SYNTAX_ERROR, DATATYPE_MISMATCH;
  variable-result `elog(ERROR)` paths use default XX000.

## Seam audit

**Ownership.** This unit's sole C source is `commands/prepare.c`. The seam crate
that would map to it (`backend-commands-prepare-seams`) does not exist — no
ported unit calls into prepare.c (it is dispatched by the unported
`tcop/utility.c`). The `-pre-seams` crates prepare consumes
(`backend-tcop-pquery-pre-seams`, `backend-utils-mmgr-portalmem-pre-seams`,
`backend-utils-time-snapmgr-pre-seams`) map to *other* units' C files
(pquery.c / portalmem.c / snapmgr.c) and are installed by those owners'
`init_seams()` when they land (mirroring the plancache `-pc-seams` convention) —
they are **not** owned by prepare. With zero owned seam crates there is no
`init_seams()` obligation. **No finding.**

All outward calls cross a documented owner's `-seams` crate and panic until the
owner lands (createas, explain, execExpr, params, parse-expr, parse-type,
analyze, pquery, utility, plancache, funcapi, mcxt, portalmem, resowner,
snapmgr, xact, tupdesc, arrayfuncs, format-type, varlena). Live owner values are
inherited-opaque handles in `types_nodes::parsestmt` (types.md rule 6, ledgered
as TD-PREPARE-1) — no invented opacity.

Seam thinness:

- **S1 — `parse_expr::analyze_one_exec_param`** (EvaluateParams per-param body):
  bundles four consecutive parser calls (`transformExpr`, `exprType`,
  `coerce_to_target_type`, `assign_expr_collations`) + store-back into one call
  to a single owner (the parser unit; all four are `parse_*.c`). The coercion-
  failed `ereport` decision is reproduced in-crate (lib.rs:411-424) from the
  returned `coercion_failed`/`given_type_id`/`expr_location`. No driver-level
  branching or node construction beyond what the parser owns. **Acceptable.**
- **S2 — explain bookkeeping seams** (`explain_execute_begin`,
  `explain_planduration`, `explain_memory_accounting`,
  `explain_buffer_accounting`): each marshals one explain-owned operation; the
  `if (es->memory)`/`if (es->buffers)` guards stay in the driver
  (lib.rs:717-730), mirroring prepare.c:589-650. **Acceptable.**
- Field accessors (`plansource_*`, `cached_plan_stmt_list`, `portal_*`) are pure
  reads/marshals. **No finding.**

## Findings

### F1 (RESOLVED): `PrepareQuery` carries `stmt_location`/`stmt_len`

Previously FAIL: `make_raw_stmt` discarded both span fields and no seam could
receive them. **Verified fixed this pass:** `types_nodes::parsestmt::RawStmt`
(parsestmt.rs:155-161) now has `stmt`/`stmt_location`/`stmt_len`; `make_raw_stmt`
(lib.rs:914-925) populates all three from the `PrepareQuery` arguments; and both
`create_cached_plan` (plancache-seams:28-34) and `analyze_and_rewrite_varparams`
(analyze-seams:49-51) take `&RawStmt<'mcx>`, so the span flows into the plan
cache and the analyzer exactly as prepare.c:81-120. Re-derived against C — match.

### F2 (FIXED this pass): `CURSOR_OPT_PARALLEL_OK` constant value

The port declared `const CURSOR_OPT_PARALLEL_OK: i32 = 0x0400`. Against
`nodes/parsenodes.h:3389-3390`, `0x0400` is `CURSOR_OPT_CUSTOM_PLAN`;
`CURSOR_OPT_PARALLEL_OK` is `0x0800`. The constant is currently dead
(`let _ = CURSOR_OPT_PARALLEL_OK;` at lib.rs:203 — the actual cursor option is
baked into the `complete_cached_plan` owner seam, not passed by prepare), so
there is no live behavioral divergence today. But a wrong flag-bit value is
exactly the silent-corruption class the audit skill calls out, and if later
wired to the seam it would force a custom plan instead of allowing parallel
mode. Fixed to `0x0800` and rebuilt clean.

## Design conformance (step 3b)

- Allocating functions/seams take `Mcx` and return `PgResult`. No
  `&'static mut`, no ambient-global seam.
- Per-backend `prepared_queries` is a `thread_local!` `RefCell<Option<HashMap>>`
  (AGENTS.md "Backend-global state") — not a shared static. OK.
- Inherited opacity only (ledgered TD-PREPARE-1); no invented handles
  (types.md rules 6-7).
- No locks held across `?`: every `PREPARED_QUERIES.with(...)` borrow scope
  closes (returning an owned value / completing the mutation) before any seam
  `?`. OK.
- No registry-shaped side tables. No unledgered divergence markers after F2.

## Conclusion

**PASS.** All 14 functions MATCH; F1 confirmed resolved at root; F2 (wrong
`CURSOR_OPT_PARALLEL_OK` flag bit) found and fixed this pass; seam wiring clean
(zero owned seam crates, all outward calls thin marshal+delegate);
design-conformance clean. `cargo check -p backend-commands-prepare` green.
`CATALOG.tsv` row may remain `audited`.
