# Session handoff — pgrust pg_regress push (snapshot)

> Written at the end of a long driving session (origin/main ≈ `b09928b8e`). Purpose: let a
> fresh session resume immediately. **First moves in the new session:** `git fetch origin`,
> read this doc + `WORK_TRACKER.md` (repo root roadmap) + the auto-memory index (`MEMORY.md`),
> then re-fire any in-flight lanes below that didn't land.

## Standing goal
Drive **pg_regress toward 100% passing** (North Star) by maintaining a fleet of ~15 parallel
subagent "lanes," each: own git worktree off **latest origin/main**, fix one thing, gate, push.
Secondary committed goal: the **node-format enum→opaque migration** (build-size fix, see below).
Memory note `goal-maintain-10-lanes-complete-roadmap` has the full operating rules.

## How lanes work (operational)
- **Gate (merge requirement):** `cargo build --bin postgres` + `cargo test -p seams-init` +
  `cargo test -p no-todo-guard` all green. Then push origin/main with a robust 5× fetch+rebase+push.
- Each lane: `git worktree add /tmp/<name> origin/main`, `CARGO_INCREMENTAL=0`, self-cleans
  (`git worktree remove --force`) on finish. Lanes do NOT edit WORK_TRACKER.md (orchestrator does).
- **Parallel-keystone directive (user, 2026-06-18):** run INDEPENDENT keystones in parallel; only
  serialize lanes editing the SAME deep crate. No more "one deep slot at a time."
- Disk ~83 GB free; ~1.6 GB per worktree; ~15-19 lanes fits. Builds are `opt-level=0` (slow runtime).
- When a lane completes it notifies; top the fleet back up by firing the next-highest-value item.

## What works NOW (big SQL surface — all on origin/main)
- **SELECT** spine: seqscans, projection, WHERE, ORDER BY, LIMIT, DISTINCT, GROUP BY, HAVING.
- **Joins:** INNER/LEFT/FULL/cross/non-equi, **HashJoin** exec.
- **Aggregates:** count/min/max/sum/avg(int) + finalize_aggregate. (array_agg/string_agg/stddev/
  avg(numeric) = internal-state, BLOCKED, see keystones.)
- **Set-ops:** UNION, INTERSECT, EXCEPT. (UNION ALL + recursive = in-flight/next-leg.)
- **Subqueries:** scalar `(SELECT ..)`, EXISTS. (IN/ANY compile fixed; exec needs hashed-IN +
  materialize_finished_plan.)
- **Expr nodes:** CASE/COALESCE/NULLIF/GREATEST/LEAST/NullTest/BooleanTest/BoolExpr, ARRAY[...],
  RowExpr(parse), CoerceViaIO/RelabelType, ScalarArrayOp (IN-list, by-ref), evaluate_expr const-fold.
- **DDL:** CREATE TABLE/INDEX/VIEW/SEQUENCE (in-session; on-disk persistence via xlog FPI now works).
- **DML:** INSERT; UPDATE/DELETE spine (walls on ctid-junk by-ref Datum keystone).
- **Statements:** transaction blocks (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/PREPARE TXN), SHOW/SET/RESET,
  EXPLAIN (analyze+plan+print; row-buffering via held-cursor tuplestore), FROM-clause SRFs
  (generate_series), xml exprs, jsonb ops + to_json family.
- **Types:** typmodin (char/varchar/numeric/time), HINT/DETAIL error emission (tree-wide), reg* casts,
  oidvector, by-ref array I/O (text[] literals), varlena-header Datum bridge (char/varchar store).
- **Milestones:** `SELECT 1`, `count(*) FROM pg_class = 415`.

## ★ THE dominant keystone: by-ref Datum VALUE unification
The #1 recurring wall (`"Datum: scalar accessor called on a by-reference value"`, char/varchar/name/
float/boolean's pg_input_error_info, UPDATE/DELETE ctid-junk). **Root cause (precisely traced):**
`RefPayload::Varlena` (fmgr lane) carries the **header-LESS** payload; the canonical heap
`Datum::ByRef` must be a **fully-framed varlena** (header-FUL). The convert sites in
`crates/backend-utils-fmgr-core/src/lib.rs` (`ref_out_to_datum`, `datum_to_ref_arg`,
`tuple_value_to_arg`) must stamp/strip the 4-byte header — and to do so correctly they need the
attribute **`typlen`** at the fmgr boundary (to distinguish varlena vs fixed-by-ref like `name`/`tid`
vs cstring). The bpchar lane (`02e8f61b2`) fixed the char/varchar *store* path this way; the broader
value path (composite returns, scalar accessors, ctid junk) is the remaining keystone. The deep-slot
lane was on this. **Highest leverage in the whole tree** — unblocks ~5 type files + UPDATE/DELETE +
boolean + name + float.

## Other live keystones
- **xlog ShutdownXLOG teardown** — trailing error after every `--single` session (XLogCtl shmem
  substrate). Cosmetic for regress (after output) but blocks clean re-boot. Lane in-flight.
- **IN/ANY subquery EXEC** — compile fixed (`157e9f033`); needs `build_hash_projections_and_exprs`
  (hashed IN) + `materialize_finished_plan` (createplan, uncorrelated ANY).
- **Internal-state aggregates (#324)** — array_agg/string_agg/stddev/avg(numeric): need
  `AggCheckCallContext` agg-context threaded into the by-OID transfn fcinfo frame + an `'mcx`-erased
  state lane (can't be `Box<dyn Any>`, state is `'mcx`-bound). RefPayload::Internal substrate landed.
- **#165 Agg-as-PlanState / window-exec** — bottom out on the same #324 fcinfo.context two-frame issue.
- **test_setup can't run** — the regress harness re-initdbs per test, so `int4_tbl`/`onek`/`tenk1`
  never exist → int/text files cascade-fail. Inherent to the harness unless test_setup
  (CREATE TABLESPACE/GRANT/COPY) runs. Not per-file fixable.

## In-flight lanes when session ended (re-fire any whose commit isn't on origin/main)
Check `git log --oneline -40 origin/main` first; re-fire the ones that didn't land:
- **by-ref Datum value** (the keystone above) — deep, highest value.
- **ShutdownXLOG clean shutdown**, **bad buffer ID** (select/float8 "bad buffer ID -2"),
  **UNION ALL**, **CTEs (WITH)**, **ArrayCoerceExpr** (`int[]::text[]`, needs array_map+int4_numeric),
  **RETURNING** (INSERT/UPDATE/DELETE), **PREPARE/EXECUTE** (prepare.c install + boundParams const-fold),
  **GROUPING SETS/ROLLUP/CUBE**, **internal-state aggregates** (#324 above).
- **fast pg_regress tally** (read-only measurement, see below).
- (background bash) **relfast build timing** (see below).

## Release build for faster regress runs (IN PROGRESS)
Debug binary is `opt-level=0` → regress runs are very slow. Full release profile (thin-LTO +
`codegen-units=1`) builds in ~30-60 min — too slow. Solution being measured: a middle profile
```toml
[profile.relfast]
inherits = "release"
opt-level = 2      # ~90% of runtime win
lto = false        # removes the #1 build-time cost
codegen-units = 256 # parallel codegen (vs release's serial cu=1)
debug = false
```
A timed build of this was running at session end (target ~2× debug build time, ~10-15× faster runtime).
If still too slow, drop to `opt-level = 1`. **Action for new session:** finish this measurement; if
reasonable, add `relfast` to root `Cargo.toml` and use `cargo build --profile relfast` for the
measurement/regress binary (NOT for the dev fleet — keep dev at opt0).

## pg_regress measurement methodology
- Harness: `git show origin/pg-regress-setup:tools/run_pg_regress.sh` (sed its harness dir to a
  private /tmp path to avoid clobbering concurrent lanes), `SKIP_BUILD=1
  PGRUST_PGSHAREDIR=/tmp/pgrust_share bash tools/run_pg_regress.sh <files...>`.
- **Full run must be hang-proof:** per-file `timeout 60`, ~8 concurrent, per-file cluster isolation
  (a crashing test can't stall the suite — the serial no-timeout version stalled at 1/215 file).
- **boolean.sql = 5 hunks** (no crash): remaining walls = the by-ref Datum keystone
  (pg_input_error_info) + an `invalid type name ""` empty-string-literal mis-parse. Very close.

## Node-format enum→opaque migration (committed; P1 DONE)
- **Why:** `types-nodes` = 16 MB rlib (giant 241-variant `Node` enum match code + 5.7 MB metadata read
  by 445 crates) — the build blowup. Fix = opaque `Node<'mcx>(PgNodeBox<'mcx>)` over
  `PgBox<dyn NodePayload<'mcx>>`, tag-keyed downcast (C's castNode), no `Any`.
- **Full plan + safety review:** `docs/proposals/node-opaque-migration.md` (committed). Reviewed vs
  rust-lang forum #75906 + `better_any` — SOUND given 2 build-time asserts: (a) every payload is
  single-lifetime `T<'mcx>`, (b) invariance in `'mcx` (free via trait object). Two `unsafe` spots only
  (in PgNodeBox): the unsize coercion (`from_raw_in`, will vanish when allocator_api stabilizes) and
  the tag-keyed downcast (`&*(ptr as *const T)` under a tag check). User decided: PgNodeBox newtype,
  drop Any, not worried about the vtable benchmark.
- **P1 LANDED (`af257b644`):** generator emits 280 `ntag::T_*` consts + 1385 `as_/is_/expect_/into_`
  accessors as enum matches, 100% additive (enum untouched, 445 consumers byte-identical).
- **Next: P2** = migrate the ~60 giant-`match` consumer crates (createplan/node_walker/setrefs/
  copyfuncs/outfuncs/readfuncs/...) to `node_tag()`+accessors. **This is the WORKFLOW fan-out target**
  (user OK'd workflows for the larger refactors). Then P3 (single representation-flip commit, after the
  risk-gate asserts) and P4 (crate-split → dissolves the 445-fan-in, the real compile win).

## Key memory notes (auto-loaded each session)
goal-maintain-10-lanes-complete-roadmap (the goal+rules, now 15 lanes + parallel keystones) ·
roadmap-tracker-keep-current · port-full-functions-no-bounded-partial · monitor-disk-and-clean ·
smoke-fixture-regen-and-isolation. The keystone notes (Datum unify, #165, #324) are in MEMORY.md.
