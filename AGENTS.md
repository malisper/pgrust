# pgrust-fabled — idiomatic Postgres-in-Rust, fourth attempt

Goal: a complete, idiomatic-Rust rewrite of PostgreSQL 18.3. This repo restarts from
the lessons of three prior attempts, all of which live in the sibling repo
`../pgrust`.

## Prior attempts (reference material, all under ../pgrust)

| Path | What it is | Role here |
|---|---|---|
| `../pgrust/postgres-18.3` | The original C source | Ground truth for behavior |
| `../pgrust/c2rust-runs` | c2rust mechanical translation: 1,067 standalone crates, one per C translation unit, each proven by replacing its `.c.o` in a real Postgres build that links and passes regress | Ground truth for the **crate catalog** and a Rust-syntax rendering of the exact C logic |
| `../pgrust/manifests/*.json` | Per-unit metadata mapping a c2rust unit to its C source file(s) | 333 units covered; for the rest, infer from `c2rust-runs/<unit>/src/*.rs` basenames |
| `../pgrust/src` | "pgrust-ABI": hand port constrained to C API compatibility | Abandoned — ABI compat blocked idiomatic Rust. Secondary reference only |
| `../pgrust/src-idiomatic` | Third attempt: idiomatic from scratch, 1,093 crates | **Primary source to copy from.** Mostly what we want, with the gaps below |

src-idiomatic's two known gaps — the reason this repo exists:

1. **The seams were never fully wired.** Wiring was deferred to the end, into one
   central `seams` crate plus a `seam-init` crate that grew to 2,564 files and
   ~519k lines. Nobody could track what was installed; unwired seams panicked at
   runtime long after the crates "passed".
2. **The logic is not all ported, even where it claims to be.** Comments, docs, and
   agents all assert completeness; wiring work kept discovering missing logic.
   Treat every "already ported" claim in src-idiomatic as suspect and verify
   against the C.

A third failure mode to never repeat: **logic leaked into the wiring layer.** Old
`seam-init` wire files contain real algorithms (e.g.
`seam-init/src/wire_keystone_executor.rs` implements the executor's
`ExecProcNode` dispatch). Logic belongs in the owning crate; seams only marshal
and delegate.

## What this repo does differently

1. **Catalog-driven progress.** `CATALOG.tsv` lists every c2rust unit with its C
   sources and a status. Crates here need not map 1:1 onto units (src-idiomatic
   combined some; c2rust duplicated some) — the mapping is recorded per row.
   The `/next-crates` skill maintains it and computes what to work on next.
2. **Per-owner seam crates, wired at port time.** Each crate `X` that must be
   called across a dependency cycle gets a companion `crates/X-seams` crate
   holding only seam declarations. Consumers depend on `X-seams`; `X` itself
   installs every implementation from its own `pub fn init_seams()`; the
   `crates/seams-init` crate just calls each crate's `init_seams()` — one line
   per crate, no other code. Wiring happens **in the same change as the port**,
   never deferred.
3. **100% of the logic at port time.** No deferrals, no "TODO later" stubs of
   logic. A call into a not-yet-ported crate goes through that owner's seam
   crate and panics loudly until the owner lands — that is the only acceptable
   missing piece. Loud panic always beats a silent stub.
4. **Audit is a merge blocker.** Every ported crate passes a function-by-function
   logic comparison against both the original C and the c2rust translation
   (`/audit-crate`) before it may merge.
5. **Fewer comments.** src-idiomatic drowned in porting-history narration. Keep
   only comments that state a constraint the code can't; strip provenance notes,
   per-line narration, and wave/audit breadcrumbs when copying code over.

## Seam rules (the core discipline)

- **Direct cargo dependency by default.** A seam exists only where a direct dep
  would create a cycle. If `cargo` accepts the edge, use the edge.
- A seam is a **thin marshal + delegate slot**: convert arguments, call one
  function, convert the result. No branching beyond delegation, no node
  construction beyond marshaling, no algorithms. If you are writing logic in a
  seam closure or an `init_seams()`, it belongs in the crate — move it.
- Declarations for `X`'s functions live **only** in `X-seams`. `X-seams` depends
  only on the `types-*` crates its signatures need and `seam-core`.
- **Seam-crate ownership is by C-source coverage, not crate-name match.** A
  unit owns every `X-seams` whose `X` maps to a C file in the unit's
  `c_sources` — a combined unit (one crate covering many files) owns many
  per-file seam crates, and its `init_seams()` must install ALL of them.
  Checking only for `crates/<unit>-seams` by name is how the rmgrdesc units
  merged with empty installers while nine seam crates each sat panicking.
- `X::init_seams()` installs **every** seam in `X-seams`. Install is once;
  duplicate install panics; calling an uninstalled seam panics with its path.
  Exactly one installer per seam means no install-ordering problems.
- `seams-init` is an aggregator only: one `X::init_seams()` line per crate.
- **A seam signature mirrors the C function's failure surface.** If the C
  function (or any path it takes) can `ereport`/`elog` at ERROR or higher, the
  seam returns `types_error::PgResult<T>`; infallible C functions return bare
  values. Seams must never return `&'static mut` (aliasable mutable statics are
  unsound) — shared-state access goes through a callback shape instead, e.g.
  `pub fn with_shmem_archiver(f: &mut dyn FnMut(&mut PgStatShared_Archiver))`.
- **Allocating seams take `Mcx<'mcx>`.** If the C function allocates its
  result in the caller's current context, the seam takes the target context
  handle and its allocated outputs carry `'mcx`
  (`seam!(pub fn f<'mcx>(mcx: Mcx<'mcx>, ...) -> PgResult<Foo<'mcx>>)`); the
  `seam!` macro accepts lifetime generics. Ambient-context assumptions must
  not cross a seam boundary.
- **No ambient-global seams.** A seam must not model another subsystem's
  per-backend global as a zero-argument getter (e.g. `transaction_xmin()`) —
  that pre-commits the unported owner to ambient state the lifecycle model
  forbids. Pass the value explicitly as a parameter (narrowest capability);
  the caller reads it off its own facet/state when the owner lands.
- **Seam declarations are not frozen — fixing a wrong signature is preferred.**
  A seam's signature belongs to the not-yet-ported owner, so an existing
  declaration is just an earlier consumer's best guess. If it misrepresents the
  C (wrong failure surface, wrong types, missing parameters), change the
  declaration and update the existing call sites in the same change; do not
  work around it with a second seam, an adapter, or by matching the wrong
  shape. The declaration must be what the owner will actually install.

Example — `parser` needs `vacuum::vacuum_rel` but a direct dep would cycle:

```text
crates/backend-commands-vacuum-seams/   # decls only; deps: types-core, types-error, seam-core
    seam_core::seam!(pub fn vacuum_rel(relid: types_core::Oid) -> types_error::PgResult<()>);
crates/backend-commands-vacuum/         # owner; deps include its own -seams crate
    pub fn init_seams() {
        backend_commands_vacuum_seams::vacuum_rel::set(crate::vacuum_rel);
    }
crates/backend-parser-.../              # consumer; deps: backend-commands-vacuum-seams
    backend_commands_vacuum_seams::vacuum_rel::call(relid)?
crates/seams-init/                      # aggregator
    pub fn init_all() { backend_commands_vacuum::init_seams(); ... }
```

## When your unit needs something an unported neighbor owns

Nearly all design debt in early batches came from this one decision point.
The C you are porting references a function, type, value, or buffer owned by
a unit that is not ported yet. The sanctioned answer per case — the easy
alternatives in parentheses are exactly the debt the review keeps finding:

| You need the neighbor's… | Do this | NOT this |
|---|---|---|
| function | call through the owner's `-seams` crate | inline a stub of its logic |
| **type** (struct/union/enum it defines) | **define the real type now**, trimmed to consumed fields, in the right `types-*` crate, values verified against the C header | an `Oid`/`usize`/`u64` alias, a `&[u8]` byte blob with a transcribed size constant, an empty stand-in struct |
| per-backend global's **value** | take it as an explicit parameter (narrowest capability); the caller will read it off its facet/state when the owner lands | a zero-argument getter seam |
| memory for its **output** | pass `Mcx<'mcx>` through the seam; output carries `'mcx` | returning owned `Vec`/`String` from an allocating seam |

Defining a neighbor's type early is expected port work, not scope creep — a
trimmed real type is cheap now and a signature-break cascade later.

Two conventions, settled (do not improvise per-crate):
- `elog(FATAL)`/`elog(PANIC)` in logic this unit OWNS → `Err(PgError)` at
  that level, like every other severity. `unreachable!()` is only for arms
  the type system makes impossible (all enum variants covered), with the C
  line cited.
- Out-of-memory errors come from `mcx.oom(size)` — never a hand-rolled
  `PgError::error("out of memory")`, which loses the SQLSTATE and the
  context-name detail.

## Memory allocation (mcx)

Context-allocated memory goes through `crates/mcx` (`Mcx<'mcx>` handles,
`PgVec`/`PgBox`/`PgHashMap`/`PgString`); design and the C translation table in
`docs/mctx-design.md`. There is no ambient current context — functions that
allocate take an `Mcx<'mcx>` parameter.

- **Always allocate through the fallible APIs.** In C every `palloc` can
  `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`; the faithful port is
  `try_reserve`/`try_new_in`-style calls whose failure converts to the
  context's OOM error (`mcx.oom(size)`), and the function returns
  `types_error::PgResult<T>`. The infallible APIs (`push` past a limit,
  `vec![…]`-style construction) abort the process instead of erroring — never
  rely on them for the allocating step. Pattern: `try_reserve` the growth,
  then do the infallible insert; helpers `alloc_in`/`vec_with_capacity_in`/
  `slice_in` in `mcx` wrap this.
- A function that allocates is therefore fallible: it returns `PgResult<T>`,
  same as the seam failure-surface rule — OOM is part of every allocating C
  function's failure surface.

## Locks and held resources

A lock or pinned resource may never be held across a `?` (or any early
return) without a `Drop` guard. "Caller must release via X" doc-comment
contracts and bare-index release functions are forbidden — that is the
src-idiomatic TD-17 leak class. If a function must return with a lock held,
it returns the guard (or takes a with-style callback).

## Query lifecycle (resowner, snapshots, error context)

The standing model is `docs/query-lifecycle-raii.md` — read it before porting
anything that touches `CurrentResourceOwner`, `ActiveSnapshot`,
`error_context_stack`, `EState`, or `PG_TRY` cleanup. The short version:

- Resources are RAII guards (release on `Drop`); scoping is Rust ownership.
  **Never a registry with release authority** — src-idiomatic ran that
  experiment and it failed in both directions (TD-17: leaks with the sweep
  off, cross-owner over-release with it on).
- Transaction/portal cleanup is an **owner value**: `commit()` consumes it
  (ordered teardown + leak WARNINGs); `Drop` is the abort path. Promotion to
  a longer scope is moving the guard into the owner.
- Threading currency is the `Ctx<'q>` bundle (mcx + resources + snapshot
  stack — a closed facet set; PG's own `EState` pattern). Take bare
  `Mcx<'_>` when allocation is all you need; `Ctx` only for two or more
  facets. Pass down via the `rb()` reborrow.
- Error context attaches on propagation (`map_err` + `add_context`), not via
  an ambient callback chain.

## Backend-global state: thread_local, never shared statics

A backend maps to one thread (looking ahead to a threaded server). C's
per-process globals are per-*backend* state — every backend has its own copy
(inherited at fork, diverging via SET / session state). Port them as
`thread_local!`, never as `static`/`Atomic*` shared across threads: a shared
static silently changes the semantics (one session's `SET log_min_messages`
would affect every session). This covers GUC-assigned knobs, `MyProcPid`-style
identity, `debug_query_string`, and every other C global that lives in backend
private memory. The only legitimately cross-thread state is what C keeps in
*shared memory* — port that as explicitly shared, synchronized types. Crates
holding `thread_local!` need `std` (it is not in `core`); that is an accepted
reason for a crate not to be `no_std`.

### RefCell access: prefer the fallible form on re-entrancy-prone state

Backend-global state behind `RefCell` (or `RefCell` inside a `thread_local!`)
is **re-entrancy-prone**: C's equivalent is a bare global that any nested code
path can touch, but Rust's `RefCell` enforces borrow exclusivity at runtime and
`borrow()`/`borrow_mut()` **panic** on a conflict. In a backend that panic is a
hard FATAL that drops the connection — and these are not theoretical: this
session alone they showed up as the plan-cache `ResetPlanCache` double-borrow
(invalidation fired mid-iteration), the `ParallelMessagePending` signal-handler
re-entry, and the GUC `prohibitValueChange` re-entry through a store-backed
accessor. The trigger is always the same shape: a path holds a `borrow`, then
calls something (`LockRelationOid` → `AcceptInvalidationMessages`,
`CHECK_FOR_INTERRUPTS`, a signal handler, an fmgr callback, a GUC accessor) that
re-enters and tries to borrow the same cell.

So, for any `RefCell`-guarded global that a nested/interrupt/callback path can
reach (which is most of them — when unsure, assume it can):

- **Don't hold a borrow across a call that can re-enter.** Snapshot what you
  need under a momentary borrow, drop it, *then* make the call (this mirrors C's
  pointer-stable iteration). This is the primary defense — it fixes the bug, not
  just the symptom.
- **Use the fallible form** — `try_borrow()` / `try_borrow_mut()` — and convert
  a conflict into a handleable `Err(PgError)` (an `ereport(ERROR)`-equivalent),
  **not** a panic. A re-entrancy bug should surface as a catchable SQL error the
  backend can recover from, not a FATAL that kills the session. Reserve the
  panicking `borrow()`/`borrow_mut()` for cells that are provably never
  re-entered (a function-local `RefCell`, or state touched on exactly one
  non-re-entrant path).

This belongs to the [`already borrowed` panic family](#recurring-root-cause-bug-families):
the structural fix is "don't borrow across re-entry"; `try_borrow*` is the
seatbelt that turns the next missed case into a graceful error instead of a
dropped connection.

## Repo layout

```
CATALOG.tsv            c2rust unit catalog + port status (source of truth for progress)
audits/                per-crate audit reports (one per /audit-crate run)
crates/seam-core       the seam! macro
crates/seams-init      startup aggregator of every crate's init_seams()
crates/types-*         layered shared-type crates (core/datum/wchar/tuple/...), copied
                       incrementally from src-idiomatic; layering and placement rules
                       in docs/types.md — never one god crate
crates/<unit>          ported crates
crates/<unit>-seams    seam declarations for <unit>, created on first cyclic caller
.claude/skills/        next-crates / port-crate / audit-crate
```

## Workflow per crate

1. `/next-crates` — pick what to port next (bottom-up by dependencies).
2. `/port-crate <unit>` — copy from src-idiomatic, complete ALL logic, re-seam to
   the per-owner scheme, wire `init_seams()` immediately, trim comments.
3. `/audit-crate <crate>` — function-by-function vs C and c2rust. **Hard merge
   blocker**; fix and re-audit until clean.
4. Merge; update `CATALOG.tsv`.

## Build and test

The build needs PostgreSQL's generated `nodetags.h`. It is now **VENDORED in the
repo** at `crates/_support/types/nodes/vendor/nodetags.h`, so the build is
**self-contained — no sibling PostgreSQL source checkout required**:

```sh
cargo test --workspace --locked --no-fail-fast
```

`PGRUST_NODETAGS_H` still **overrides** the vendored copy if you need a different
one (e.g. a different PG minor); the sibling `pgrust/postgres-18.3` checkout
remains a fallback. (Note: the *regression test fixtures* — `sql/`/`expected/` —
still come from the sibling `postgres-18.3` checkout; only the build is
self-contained.)

The Mac build server is available for agent builds through the restricted
`pgrust-ci` SSH command:

```sh
ssh pgrust-ci@66.201.40.58 submit build main
ssh pgrust-ci@66.201.40.58 submit test main
ssh pgrust-ci@66.201.40.58 status JOB_ID
ssh pgrust-ci@66.201.40.58 logs JOB_ID
ssh pgrust-ci@66.201.40.58 download JOB_ID > JOB_ID.tar.zst
```

As of June 17, 2026, `cargo test --workspace --locked --no-fail-fast` on
`main` is known to fail compiling `backend-commands-async` tests because
`crates/backend-commands-async/src/tests.rs` calls
`parallel_seams::is_parallel_worker::set(...)`, but that seam is currently a
function, not a seam module with `set`.

## Recovery / crash TAP suite (`scripts/run-recovery-tap`)

Drives PostgreSQL's `src/test/recovery/t/*.pl` against the pgrust server while
borrowing the C client tools. Run ONE test with a timeout (macOS has no
`timeout`; kill + `pkill -f rtap-<tag>` if a run hangs):

```sh
RECOVERY_TAP_DIR=/tmp/rtap-<tag> PG_TEST_TIMEOUT_DEFAULT=25 \
  PG_SRC=/Users/<you>/workspace/work/pgrust/postgres-18.3 \
  PG_INSTALL=/tmp/pgrust_pginstall scripts/run-recovery-tap t/NNN_name.pl
```

- **Run in an ISOLATED PG_SRC copy when another agent is also running TAP.**
  The harness `cd $PG_SRC/src/test/recovery` and two concurrent `prove` runs
  from the *same* recovery dir collide on the shared cwd / random port range
  → spurious `Bailout ... pg_ctl start failed` with NO node log. Symptom of
  interference (vs a real bug): the node *data dirs exist* (init ran) but 0 `ok`
  lines and an immediate bail; re-running gives different results. Fix: make a
  private PG_SRC where everything under `src/test/` is symlinked EXCEPT
  `recovery`, which is a real copy, and point `PG_SRC` at it:
  ```sh
  ISO=$SCRATCH/pgsrc-iso; mkdir -p "$ISO/src/test"
  for d in $PG_SRC/src/test/*; do n=$(basename "$d");
    [ "$n" = recovery ] && cp -R "$d" "$ISO/src/test/recovery" || ln -s "$d" "$ISO/src/test/$n"; done
  ```
  A unique `RECOVERY_TAP_DIR` per run already isolates the datadir + portlock,
  but NOT the cwd's `tmp_check`; the copy does.
- **To see WHY a node won't start, read the startup-process server log, don't
  grep the port inventory.** Capture it by starting the failing node by hand
  with `PGRUST_PANIC_VERBOSE=1` + `-c io_method=sync -c max_stack_depth=7000kB`
  (from the node's `postmaster.opts`). `startup process ... exit code 101` is a
  Rust panic — almost always an **uninstalled seam** on a recovery path
  (`seam not installed: ...`) or a redo-arm `PANIC`. Find the failing node by
  which `*_data/pgdata` dirs exist; manual-start state can be stale, so re-run
  fresh if the control file looks inconsistent.
- The `io_method=worker` panic is pre-handled (the harness injects
  `io_method=sync` via `$TEMP_CONFIG`); a fresh-initdb primary that won't start
  is a real bug, not the io_method one.

## Hard rules (learned the hard way)

- Verify "already ported" against the code, never against docs, comments, or an
  agent's assertion. Plans and deferral comments in ../pgrust are routinely stale.
- Audits enumerate **every function** from the C file — never reason by category
  or wildcard. That is how missing cores slipped through before.
- Parity includes SQLSTATEs, OIDs, NodeTags, constants, and limits, not just
  control flow. Transcribed constant tables are a recurring silent-corruption bug.
- A green build is not parity. Don't trust the port's own review; the audit is
  independent.
- Seams marshal and delegate; prefer a loud panic over code that pretends.
- Default workflow once your code works: rebase onto latest `main`, run a quick
  round of rust tests (targeted crates + a cheap `cargo check`/build gate; full
  suite only for major/high-risk substrate changes), then merge into `main`.
  Pushing to a remote still requires being asked.
- **Build/verify and commit in the SAME worktree.** Never use another worktree's
  working tree as a build cache while committing to a different branch — that
  splits "where it was verified" from "what got committed," and the committed
  files may not be byte-identical to what you actually tested. If you must reuse a
  sibling worktree's artifact cache, set `CARGO_TARGET_DIR` to share the *target/*
  dir, not the source tree. Before reporting done: confirm the committed files
  match the verified build (`git status` clean, `git diff` empty in the build
  worktree) and leave no stray uncommitted edits behind in any worktree.

---

# Regression-lane playbook (for background lanes driving pg_regress green)

Most background lanes are not porting a fresh crate top-to-bottom — they are
driving a specific regression file (or crash, or keystone) to fewer diff lines.
This section is the operational knowledge those lanes keep re-deriving from
scratch. **Read it before you start measuring; it will save you several build
and boot cycles.**

## Which regression tests to run (DON'T run the full suite)

**Run ONLY the regression files directly relevant to what you changed, plus a
cheap compile gate (`cargo check`/`cargo build`). Do NOT run the full 230-file
suite.** It is slow (many minutes), and a localized change cannot affect files it
doesn't touch. The full suite is the coordinator's call for a genuine tree-wide
substrate change — not something an individual change-lane runs by default.

Pick the relevant files by what your change exercises, e.g.:
- integer/numeric arithmetic → `int4`, `int8`, `numeric`, `float8`
- sort/comparator → `tuplesort`, `select_distinct`, `create_index`
- collation/ICU → `collate`, `collate.utf8`, `collate.icu.utf8`, `strings`
- planner/selectivity → `select`, `join`, `subselect`, `aggregates`, `create_index`
- expression interpreter / fmgr dispatch → `expressions`, `case`, `aggregates`, a
  couple of type files
- ANALYZE/stats → `stats_ext`, `stats`, `vacuum`

Pick the handful that cover your change + a couple of nearby files as a guard.
Quality over quantity — a clean diff on the right 5–10 files beats a slow full run.

## Avoid the cross-file ORDERING trap (the #1 false-FAIL source)

Regression files are NOT standalone: many depend on objects created by EARLIER
files in `parallel_schedule`. Running one file in isolation (just `test_setup` +
your file) FALSE-fails with `relation "X" does not exist` / `function "Y" does not
exist` — that is a missing-prerequisite artifact, **NOT a pgrust bug**. Two ways
to avoid it:

1. **Persistent-session model (preferred):** ONE boot, `createdb regression`, run
   `test_setup.sql` first, then run your target file(s) — and any prerequisite
   files — **in `parallel_schedule` order, in the SAME session/db**. This is what
   the coordinator's `regress_check.sh` does; mirror it. Reusing one DB across the
   schedule prefix satisfies the dependency chain.
2. **Know the common prerequisites** when running a single file: e.g. `aggregates`
   needs `create_aggregate`; `join`/many SELECT files need `test_setup`'s tables +
   `inherit` (`b_star`); `geometry` needs `box_tbl` (test_setup); `horology` needs
   `timestamp_tbl`; `multirangetypes` needs `rangetypes`. When unsure, run the
   schedule PREFIX up to and including your file in one session.

If a "FAIL" is `relation/function/type does not exist` for an object your change
didn't touch, suspect the ordering trap first — re-run with prerequisites (or in
the persistent-session model) before treating it as a real diff.

## The fastest path through a lane

1. **Build dev, not release.** Your gate is `cargo build --bin postgres` (~28 s
   cold, ~5 s incremental for a leaf-crate edit) → `target/debug/postgres`.
   Release is 100–270 s and only the measure harness needs it. Never gate on
   `--release`.
2. **Don't re-measure what you can reason about.** Boot once, run the file once,
   read the diff, group hunks by root cause. One dominant cause usually explains
   a big consecutive block — fix that, don't nibble single lines.
3. **One fix, full port, push.** Port the *entire* function/branch you touch
   (not just the path the query hits), gate, push. Resist exploring adjacent
   files — that is where the long lanes (22 min+) lose time vs the short ones
   (14 min).

## Boot recipe (copy verbatim — do not re-derive)

pgrust's own `initdb`/catalog bootstrap is unported, so you boot the pgrust
binary against a **C-initdb'd** datadir:

```sh
# 1. C-initdb a pristine datadir (one-time per lane; ~0.4 s)
/tmp/pgrust_pginstall/bin/initdb -D <UNIQUE_dd> --no-locale --encoding=UTF8 -U postgres

# 2. boot the pgrust binary (~1.7 s release / ~2.5 s dev to ready)
ulimit -s 65520
RUST_MIN_STACK=33554432 target/debug/postgres -D <UNIQUE_dd> \
    -k <UNIQUE_sock> -p <UNIQUE_port> -c io_method=sync -c max_stack_depth=60000

# 3. probe
/tmp/pgrust_pginstall/bin/psql -h <UNIQUE_sock> -p <UNIQUE_port> -U postgres -c 'SELECT 1'
```

- `max_stack_depth=60000` and `ulimit -s 65520` are **mandatory** — the port's
  per-statement frames are large (see frame-bloat note); the C default refuses
  to boot. `RUST_MIN_STACK=33554432` likewise (deep recursion overflows the
  default thread stack on dev builds).
- **UNIQUE datadir/sock/port per lane.** Other lanes are live. Collisions =
  spurious crashes. Clean up your clusters on exit.
- **NEVER `pkill -9 -f release/postgres`** and never touch another datadir —
  you will kill the measure binary or a sibling lane. Scope kills to your own
  datadir: `pkill -9 -f "<UNIQUE_dd> "`.
- A trailing `ShutdownXLOG xlog-driver` error on shutdown is **pre-existing and
  unrelated** — ignore it.

## Per-file diff measurement (the env-fidelity gotchas that cause FALSE diffs)

The regression source AND the expected results live under
`/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress`:
- **Input SQL:** `sql/<f>.sql`
- **Expected results (what you diff against):** `expected/<f>.out` — and the
  platform/feature alternates `expected/<f>_1.out`, `_2.out`, `_3.out` (e.g.
  `collate.icu.utf8_1.out` is the no-ICU skip output, `numa_1.out` the no-libnuma
  skip). A file PASSES if your output matches ANY one of its expected variants, so
  always diff against the **min (best-of)** of `<f>.out`/`_1`/`_2`/`_3`, not just
  `<f>.out`. Counting only `<f>.out` produces false FAILs on files that ship
  alternates.

So: run `sql/<f>.sql`, capture your output, and `diff` it against the best of the
`expected/<f>*.out` set.

Several mistakes produce large *false* diffs that look like port bugs but are
harness artifacts. Avoid all of these:

- **Export the COPY-data vars** or `test_setup.sql`'s `COPY ... FROM :'filename'`
  loads nothing and you get ~230 phantom "empty table" diff lines:
  `export PG_ABS_SRCDIR=<regress dir> PG_LIBDIR=/tmp/pgrust_pginstall/lib/postgresql PG_DLSUFFIX=.dylib`.
- **Substitute psql vars.** Files reference `:abs_srcdir`/`:libdir`; unsubstituted
  they emit ~8 identical error lines. Pass `-v abs_srcdir=... -v libdir=...` (or
  run through the real pg_regress var set).
- **Include `regress.so`/`regress.dylib`.** The C-helper-function files
  (`create_function_c`, `create_misc`, `create_type`, `alter_table`, `misc`,
  `tsearch`, `triggers`, …) do `\set regresslib :libdir '/regress' :dlsuffix` and
  `CREATE FUNCTION … AS :'regresslib'`, so the regress shared lib MUST exist at
  `<libdir>/regress.{dylib,so}` or those tests diverge with load errors. The built
  lib is NOT in the install libdir — it lives at
  `/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress/regress.dylib`
  (macOS) / `regress.so` (Linux). Before running such files, copy (or symlink) it
  into the libdir you pass, e.g.
  `cp .../src/test/regress/regress.dylib /tmp/pgrust_pginstall/lib/postgresql/`
  (on Linux use `regress.so`). Without it, the `regress`-backed CREATE FUNCTIONs
  fail and cascade — a harness artifact, not a port bug.
- **Feed SQL on STDIN, never `psql -f <file>`.** With `-f`, psql prefixes every
  error/notice with `psql:<file>:<line>: ` while the expected `.out` has bare
  `ERROR:`/`NOTICE:` — so *every* file that emits an expected error/notice FALSE-
  fails (~2 diff lines per message; e.g. boolean shows a phantom 26-line diff).
  Use `psql ... < sql/<f>.sql` (pg_regress-style), like `onefile.sh` does.
- **Export `PG_ABS_BUILDDIR` and make a `results/` dir**, or COPY-to-file / `\o` /
  `lo_export` tests FALSE-fail with `relative path not allowed` and cascade
  (misc → ~1000 phantom lines; also copy, copyencoding, psql, largeobject). Files
  do `\getenv abs_builddir PG_ABS_BUILDDIR` then write under `:abs_builddir/results/`.
  `export PG_ABS_BUILDDIR=<writable dir>; mkdir -p "$PG_ABS_BUILDDIR/results"`.
- **Run against a database named `regression`, not `postgres`.** The expected
  files were captured in pg_regress's `regression` db, so running in `postgres`
  FALSE-fails every file that echoes the db name — `information_schema` views
  (sequence, domain, updatable_views, privileges), `table_to_xmlschema` (xmlmap),
  and error text like `permission denied for database <db>` (subscription,
  publication, create_role, foreign_data, dependency). `createdb regression` after
  boot and run test_setup + every file against it.
- **Run `test_setup.sql` first, in the *same* session**, then the file. Many
  files depend on objects test_setup creates.
- **Honor schedule dependencies.** e.g. `aggregates` depends on
  `create_aggregate` — prepend it or you get ~230 phantom "function does not
  exist" lines. (This is why aggregates.sql's *true* baseline is ~1718, not the
  ~1990 a naive run reports.)
- **DEV builds + `SET max_stack_depth='100kB'` = a FALSE whole-file cascade.**
  `jsonb.sql`, `json.sql`, `infinite_recurse.sql` set `max_stack_depth='100kB'`
  to test recursion limits. The dev binary's per-statement frame is ~400 kB
  (vs C's few kB), so under that GUC even `SELECT 1` trips `check_stack_depth`
  and the session breaks until `RESET` — cascading the rest of the file (jsonb
  reads ~9700 false difflines / "5% passing" this way). The RELEASE binary the
  official measure uses has small frames and is unaffected, so it's a
  DEV-MEASURE-ONLY artifact. When measuring one of those 3 files with a dev
  build, neutralize the line first:
  `sed -E "s/SET max_stack_depth = '?100kB'?/SET max_stack_depth = 7680/" sql/<f>.sql | psql ...`
  (7680 kB ≈ dev-frame headroom; the deep-recursion test still errors correctly,
  just at a deeper limit). Or skip the `SET…/RESET` block. Do NOT report a dev
  build's raw jsonb/json difflines as the real state.
- **Match pg_regress's locale/TZ env** or date/error text diffs on formatting:
  `PGTZ=America/Los_Angeles PGDATESTYLE='Postgres, MDY' LANG=C LC_MESSAGES=C
  PGOPTIONS='-c intervalstyle=postgres_verbose'`, and psql flags
  `-X -a -q -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on`.
- **`autovacuum=off` changes plan shapes.** The harness boots autovacuum off
  (launcher unported), so `pg_class.reltuples` stays `-1` and the planner picks
  Bitmap/SeqScan where real PG (autovacuum on) picks Index-Only-Scan. Real PG
  18.3 reproduces the identical diff under `autovacuum=off` — so an EXPLAIN
  row-estimate/plan-shape diff is often NOT a pgrust bug. Confirm against real PG
  before "fixing" it.

A ready harness with all of this baked in lives at
`/private/tmp/qmeasure/run_sharded.sh` (and a single-file mode); prefer adapting
it over hand-rolling.

## Common time-wasters observed across lanes (don't repeat these)

- **Re-deriving the boot/measure recipe every lane.** It's above. Copy it.
- **Measuring against a naive baseline**, then "fixing" phantom diffs that are
  really the COPY-data / schedule-dependency / autovacuum artifacts above.
- **Editing a deep crate when a leaf edit suffices.** Touching `types-nodes`,
  `types-fmgr`, or `backend-utils-fmgr-core` recompiles hundreds of reverse-deps
  (30 s–several min/build); a leaf `backend-utils-adt-*` edit is ~5 s. Stay at
  the leaf unless the fix genuinely belongs deeper.
- **Re-discovering a known keystone.** Before going deep on a blocker, assume it
  may already be characterized — the recurring ones are listed below. If your
  diff bottoms out on one, document it and move to a contained fix; don't burn
  the lane re-proving a multi-session blocker.
- **Running 5 forks in one shared worktree.** Their concurrent `git commit`s
  cross-contaminate and one fork's `git stash` can sweep another's uncommitted
  work aside. One worktree per fork.
- **Trusting a `panic!("… not ported …")` comment.** Many are stale — the
  substrate landed since. Check the actual seam/install state before concluding
  it's a wall.

## Recurring root-cause bug families (most "deep" panics are one of these)

When a fix looks like a deep keystone, check it against these first — most are
contained:

1. **Uninstalled seam over a ported body.** `seam not installed: X::y` with the
   body fully ported in the owner — the owner's `init_seams()` just never
   installs it. Fix = add the one install line. (Single biggest false-keystone
   class.)
2. **Derived `.clone()` should be `clone_in(mcx)`/`copyObject`.** A
   `SubLink::clone`/`Aggref::clone`/`SubPlanExpr::clone` panic is a contained
   routing gap, not a `'static` blocker — route through `clone_in`. Silent-drop
   variants (a clone that nulls a field) are the worst; the audit found the
   tree-wide set is small and mostly swept.
3. **By-ref Datum losing its arm at a boundary.** `Datum::as_ref_bytes on a
   non-flat/by-value value` — a header-ful vs header-less / typbyval mismatch at
   the fmgr or slot-deform boundary. Respect each attribute's typbyval/typlen.
4. **Arena use-after-free / double-free** in `Mcx::deallocate` — an Expr/SubLink
   interned into a transient context that's freed before the planner arena drop.
   Fix = `clone_in(run.mcx())` into the durable arena (cf. commit c162a67d3).
5. **Port-introduced unwind/cleanup escalation** — a recoverable error turned
   into a backend kill by Rust strictness on the abort path (non-reentrant Mutex
   deadlock, Mutex-poison cascade, Err-in-cleanup abort-in-abort, move-field +
   inline-restore not unwind-safe, **a panic crossing a c2rust `extern "C"`
   nounwind frame**). Fix = thread_local/RefCell not Mutex on per-backend state;
   idempotent no-op cleanup; `Drop` guards for move-restore; `extern "C-unwind"`
   on parse/longjmp frames.
6. **Wrong transcribed `#define`/OID/SQLSTATE constant.** Silent-corruption
   class; verify constants against the C header, not against the existing port.

## Architecture state you must build on (current, not historical)

- **Node is opaque.** `types_nodes::Node<'mcx>` is `struct Node(PgNodeBox)`, not
  an enum (the flip landed; it's a −32 MB release win). Construct nodes with
  `mk_<variant>(mcx, …)?` (fallible — thread `?`), inspect with `as_<variant>()`
  / `into_<variant>()` / `node_tag()` accessors. **Never** write
  `Node::Variant(x)` or `match node { Node::V(..) => }` — those won't compile.
  Mutators that build nodes take a trailing `mcx` and return `PgResult`.
- **fmgr error model is migrating panic→Result.** New builtin bodies are
  `fn(&mut Fcinfo) -> PgResult<Datum>` registered via `register_builtins_native`
  (`func: None`); the dispatch arm calls them directly and propagates `?` with
  no `catch_unwind`. Legacy `-> Datum` bodies that panic on error still work via
  the catch_unwind bridge. **Reference implementation: `backend-utils-adt-int8`.**
  If you add/edit a builtin, follow the native shape; don't reintroduce a
  panic-on-error `-> Datum` body.

## Codebase map (where things live — to cut exploration)

1,431 crates; `crates/<unit>` ported code, `crates/<unit>-seams` its seam decls.
Families by size: `backend-utils-*` (276, incl. all `adt-*` type I/O + fmgr),
`backend-access-*` (207, AMs: heap/nbtree/gist/gin/spgist/brin + transam/WAL),
`backend-executor-*` (110), `backend-commands-*` (98, utility statements),
`backend-storage-*` (85), `backend-optimizer-*` (70, planner), `backend-catalog-*`
(69), `backend-parser-*` (36). Landmark crates on the SELECT spine:

| Concern | Crate |
|---|---|
| main loop / simple-query (`exec_simple_query`, dispatch) | `backend-tcop-postgres` |
| parse analysis (`transform*`, `parse_analyze`) | `backend-parser-analyze` |
| planner entry (`subquery_planner`, `grouping_planner`) | `backend-optimizer-plan-planner` |
| plan-node construction (`create_*_plan`) | `backend-optimizer-plan-createplan` |
| selectivity / cost | `backend-optimizer-path-*`, `…util-plancat`, `…adt-selfuncs` |
| executor top (`ExecutorRun`, `ExecutePlan`) | `backend-executor-execMain` |
| node init/dispatch (`ExecInitNode`, `ExecProcNode`) | `backend-executor-execProcnode` |
| expression eval (`ExecInterpExpr`, EEOP steps) | `types-nodes` (execexpr.rs) |
| aggregate executor | `backend-executor-nodeAgg`, `…nodeWindowAgg` |
| fmgr dispatch / builtin registry | `backend-utils-fmgr-core` |
| scalar type I/O + ops | `backend-utils-adt-*` (int8/float/numeric/varlena/…) |
| node types + opaque Node + EEOP interp | `types-nodes` |
| fmgr types (`FunctionCallInfoBaseData`) | `types-fmgr` |
| Datum + by-ref/by-value lanes | `types-datum` |
| memory contexts (`Mcx`, `PgVec`/`PgBox`) | `mcx` |
| catalog index build/reindex | `backend-catalog-index`, `…executor-execIndexing` |

The C ground truth for any unit is in `../pgrust/postgres-18.3`; the c2rust
mechanical rendering (exact logic, Rust syntax) is in `../pgrust/c2rust-runs`;
the idiomatic source to copy from is `../pgrust/src-idiomatic` (verify against C —
its "already ported" claims are unreliable).

# WASM web demo (pgrust.com): build + deploy

The interactive browser demo at https://pgrust.com is a wasm64 single-user
`postgres` running entirely client-side. Source: `tools/wasm-web/` (index.html,
repl.js, backend.js, worker.js, format.js, pgrust-harness.js, build.sh, serve.mjs).

## Build the engine + assets
`tools/wasm-web/build.sh` assembles the three deployable assets into
`tools/wasm-web/assets/`: `postgres.wasm` (the engine), `vfs.img` (packed
initdb'd datadir + sharedir bytes), `vfs.json` (the VFS manifest).

PRODUCTION engine uses the `wasm-prod` profile (`opt-level="s"` + lto + strip) +
a `wasm-opt -O2` post-pass:
```sh
# needs: rust nightly, the wasm64 target, binaryen (`brew install binaryen`)
cargo +nightly build -Zbuild-std=std,panic_abort --bin postgres \
  --target wasm64-unknown-unknown --profile wasm-prod
PGRUST_DATADIR=<an-initdb'd-datadir> tools/wasm-web/build.sh   # runs wasm-opt + packs the VFS
```
Result: ~34 MB wasm (vs the `wasm-boot` bring-up profile's ~178 MB). opt-level
`"s"` beats `"2"`/`"3"` here — the workload is dispatch/btree-bound, not
arithmetic, so higher opt just bloats the module and slows the browser
cold-compile with NO query-time win. Verify correctness with
`tools/wasm-web/run-node.mjs <sql-file>` (NOT the browser path — run-node uses its
own loader). Local browser test: `node tools/wasm-web/serve.mjs 8090`.

## Deploy to pgrust.com (S3 + CloudFront)
- Bucket `s3://pgrust`; CloudFront distribution `E8737IN03F13`. IAM note:
  `s3:ListAllMyBuckets` and `cloudfront:ListDistributions` are DENIED, but
  per-object `aws s3 cp`/`sync` and `cloudfront create-invalidation` are ALLOWED.
  Find the distribution id from the bucket OAC policy if needed:
  `aws s3api get-bucket-policy --bucket pgrust`.
- HTML/JS: `aws s3 cp <file> s3://pgrust/<file> --cache-control no-cache` (updates
  propagate; the worker is also `?v=` cache-busted).
- After ANY upload: `aws cloudfront create-invalidation --distribution-id E8737IN03F13 --paths "/*"`
  (the edge serves cached objects until invalidated; the user must hard-refresh to
  clear their own browser cache too).

## COMPRESS the big assets (brotli — the dominant first-load win)
`postgres.wasm` (~34 MB) + `vfs.img` (~40 MB) ≈ 74 MB raw download. CloudFront will
NOT auto-compress them (they exceed its 10 MB limit), so PRE-compress with brotli
and upload with `Content-Encoding: br` (browsers decompress transparently;
`WebAssembly.compileStreaming` handles a br response). Brotli crushes them — wasm
→ ~21%, the datadir VFS → ~4% (mostly zero pages) — so ~74 MB becomes ~8.5 MB
(~8.5× faster cold start; "a couple of seconds" instead of 10–30 s):
```sh
A=tools/wasm-web/assets
brotli -q 11 -c $A/postgres.wasm > /tmp/postgres.wasm.br
brotli -q 11 -c $A/vfs.img       > /tmp/vfs.img.br
aws s3 cp /tmp/postgres.wasm.br s3://pgrust/assets/postgres.wasm \
  --content-type application/wasm    --content-encoding br --cache-control "public, max-age=86400"
aws s3 cp /tmp/vfs.img.br       s3://pgrust/assets/vfs.img \
  --content-type binary/octet-stream --content-encoding br --cache-control "public, max-age=86400"
aws cloudfront create-invalidation --distribution-id E8737IN03F13 --paths "/assets/*" "/*"
```
VERIFY the encoding round-trips (macOS `curl` lacks brotli, so fetch the RAW bytes
and decode with the `brotli` CLI — `curl --compressed` returns 0 bytes here, a
false alarm, not a broken deploy):
```sh
curl -s https://pgrust.com/assets/postgres.wasm -o /tmp/w.br      # the served ~7 MB br payload
[ "$(brotli -d -c /tmp/w.br | wc -c)" = "$(wc -c < tools/wasm-web/assets/postgres.wasm)" ] && echo OK
```
The audience is memory64 browsers (Chrome ≥133), all of which support brotli, so a
single `br` encoding (no gzip fallback) is fine.

## Gotchas
- The wasm is memory64 (`is64`) but NOT shared-memory, so it does NOT need
  COOP/COEP cross-origin-isolation (serve.mjs sets them defensively; S3/CloudFront
  don't, and the site works without them — verified by parsing the wasm memory
  section flags: `shared=no`).
- Multi-line result values (`string_agg(…, E'\n')` — the Mandelbrot demo) span
  physical lines in the single-user printtup stream; backend.js folds continuation
  lines (those not starting with a tab) back onto their field before parsing, and
  renders a 1×1 multi-line cell as raw text. Don't regress that.
- The Docker image (`malisper/pgrust:v0.1`, multi-arch) is a SEPARATE artifact —
  built from the repo `Dockerfile`, not this wasm path. It bundles only `psql`
  (no `pg_isready`); probe readiness over TCP (`psql -h 127.0.0.1`), since the
  first-init temp server listens on the unix socket only.
