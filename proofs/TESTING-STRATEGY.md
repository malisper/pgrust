# Testing strategy for the 1,538 proof-excluded functions

Date: 2026-07-30 (rev 2). Base: main `515372715f`. Companion data:
`proofs/EXCLUDED_COVERAGE.tsv` (machine-generated per-function map; method in §2).

`proofs/USER_FACING_FUNCTIONS.tsv` has 3,189 rows; 1,538 (48%) are `excluded(<reason>)`
from the Kani equivalence-proof program. This document answers: how is each excluded
class tested instead, what covers it today (with evidence), and what to build next.
It holds itself to the campaign's gate-blindness law — a mechanism nobody runs is not
coverage — plus this revision's corollary: **a corpus that never varies is not input
coverage.**

Reason breakdown (leading token): state 699, engine 334 (planner 46, sortsupport 16,
window 15, trigger 13, am-handler 9, tablesample 2, bare/pattern-match+tsearch+xml+jsonpath 233),
wall 194, typcache 190, blocked 65, SRF 20, agg-state 16, unimplemented 8, planner-node 4,
port 3, non-surface 3, catalog 1, no-logic 1.

## 0. Governing principles

**Two axes, never collapsed.** Every claim below separates:

- **Oracle** — what decides correctness: real-PG expected output, vendored C compared
  in-process, an invariant, or nothing.
- **Input coverage** — how much input space is explored: fixed hand-written corpus,
  random, coverage-guided fuzz, exhaustive, or formal proof.

pg_regress against real-PG expected outputs is a genuine differential **oracle** with
**fixed-corpus** input coverage: each function sees the handful of literals someone
wrote into a demo query. `float8in` appearing in regress says nothing about
subnormals, exponent edges, or shortest-round-trip images. Rev 1 of this document
collapsed the axes into a single 73% "covered" figure; that was the gate-blindness
pattern and is retracted. The honest headline:

> **1,122 of the 1,538 excluded functions (73%) have a differential oracle today via
> the fleet-run regress corpus — but only 1 of the 1,538 (`interval_div`, via one
> native-diff binary) has any attributable input-space exploration beyond that fixed
> corpus. Coverage-guided fuzzing reaches zero of them. On the input-coverage axis,
> the excluded half of the catalog is effectively at zero.**

**Coverage-guided differential fuzzing (CGF) is the primary technique** for closing
that axis; regress is demoted to smoke/regression rail plus oracle plumbing.

**In-process wherever possible.** In-process differential runs millions of
cases/sec; over-the-wire SQL runs thousands. Three orders of magnitude decide how
much input space actually gets covered, so every class is pushed toward in-process
first, and "needs a live server" is the fallback of last resort, argued per class.
The lever that makes this achievable far beyond the obviously-pure functions is the
**seams architecture**: pgrust routes session/catalog state through
`seam_core::seam!` fn-pointer slots — **188 `*_seams` crates, 1,372 seam slots
repo-wide**; `syscache_seams` alone exposes 135 row-shape accessors over catalog
data, and the production installer (`cache_syscache/src/projections.rs`, 125
`::set` calls) is itself mock-shaped. A test harness installs a mock with
`<slot>::set(closure)` before boot — **~95 test files already do exactly this**
(cleanest template: `crates/backend/utils/adt/regproc/src/tests.rs`, 623 lines, a
hand-written fake catalog behind 12 seams; shared install-if-absent scaffolding in
`crates/_support/test_boot`). Where the mock serves the *real vendored catalog
values* — built-in `pg_type`/`pg_proc`/`pg_authid` rows are static data in the
repo — the function becomes an in-process CGF target with no approximation. (Per the standing "seams not for impl swap"
ruling, seams are circular-dep breakers; harness-side mock installation for tests is
the one additional use adopted here, and it introduces no alternate production
backends.) §3.1 classifies all 699 state rows through this lens; a parallel lane is
doing the same for typcache via a generated static-catalog mock.

---

## 1. What testing already exists (inventory, with run-status)

### 1.1 pg_regress differential parity vs real PostgreSQL 18.3 — EXISTS, RUNS, PASSES; oracle: real-PG, coverage: fixed corpus

- Corpus: 232 `sql/` files / 265 expected files in the vendored PG source
  (`../pgrust-fabled/vendor/postgres-src/src/test/regress`); 230 scheduled tests.
- Runner: `scripts/pg-regress-fast.sh` drives the **real `pg_regress` binary** in
  `--use-existing` mode against a pgrust postmaster with C 18.3 `initdb`/`psql`, and
  points `--expecteddir` at the vendored PG expected outputs. Expected files are
  never written by pgrust; pinned-transcript harnesses are minted from live C
  (`scripts/auth-regen-expected.sh`). The oracle is sound.
- Overlay: `regress/overlay/sql/` (154 files) is byte-identical vendor SQL plus
  `-- pgrust:` annotations only (`cmp` guard enforced); 10,608 `pgrust:rowsort`
  annotations relax row order; everything else is byte-exact.
- Anti-gaming: `scripts/lane-gates.sh` adds engagement floors and a refusal
  allowlist ("regress-parity alone is gameable").
- Isolation: 119 upstream specs + 12 overlay EPQ specs via real
  `pg_isolation_regress`, C outputs verbatim; last recorded 119×3 arms PASS.
- Run evidence: repeated fleet jobs with S3 artifacts ("pg_regress ALL: 230/230
  ok", `notes/batchemit-lane.md:105`,
  `scripts/lane-gates.floors.fleet-pod-pgdg:26-30`); latest dated pass ~2026-07-21.

**What it is not:** input coverage. Fixed, hand-written, feature-demonstrating.
Catches regressions on inputs someone already thought of; explores nothing. Further
caveats: corpus lives outside this repo (fresh clone can't run it); **no CI** (no
`.github/`) — gates fire only when a human submits a fleet job; standing
env-failure allowlist (e.g. `psql_pipeline`); documented comparator flake mode;
contrib isolation lists unwired.

### 1.2 SQL-level differential e2e scripts — EXISTS, RUNS (per-lane); oracle: real-PG, coverage: fixed corpus

622 `*e2e*.sh` in `scripts/`, ~227 booting pgrust and C PG 18 side by side and
byte-comparing (`*-oracle-e2e.sh`, `fk-e2e.sh`, `trigger-*-e2e.sh`,
`regress-diff.sh`). 485 include kill-9 + recovery; several do bidirectional WAL
cross-replay. The per-train leg manifest lives in the fabled fleet repo — from this
tree we cannot enumerate standing gates vs one-shot lane artifacts. All fixed-corpus.

### 1.3 sqlsmith differential campaigns — EXISTS, RAN, found real bugs; oracle: real-PG; coverage: random query-shape, per-function unattributable

`scripts/sqlsmith/run-campaign.sh` + `triage.py`: 20k-query crash-hunt, then
full-corpus replay against both engines with per-statement divergence classes.
Checked-in repro corpora prove campaigns ran and yielded. Two limits keep it from
counting as input coverage for any specific row: generation is grammar-random with
**no coverage feedback**, and there is no per-function attribution — nobody can say
which of the 1,538 a campaign reached; value generation (column refs + simple
constants) is weakest exactly where the excluded functions are risky. Operator-run,
not commit-gated.

### 1.4 simharness (= `crash-simulator/`, byte-identical duplicate) — EXISTS, PARTIALLY RUNS; oracle: real-PG + property oracles; coverage: random over a narrow vocabulary

Seeded generative SQL campaign engine (20k LOC, 21 property oracles incl. TLP/NoREC,
differential-vs-C classifier, shrinker, multi-session, `sim-fault` crash-cut +
`StartupXLOG` recovery leg). 14-leg converge gate recorded all-PASS 2026-07-19 — on
an author box, not a recorded fleet gate. No coverage guidance. Known holes:
generated plans emit **zero** Crash/TornWrite faults (driver `NotWiredYet`,
weight 0); fault × multi-session never composed; the 24h FP-budget campaign and
planted-bug gate open; `--diff-c` has a known DUT-split bug; **generator vocabulary
contains no FOREIGN KEY and no trigger DDL** (`tools/simharness/src/gen/schema.rs`).
Like sqlsmith: real exploration, per-function unattributable, value-weak.

### 1.5 Deterministic simulation substrate (`--cfg pgrust_sim`) — EXISTS, RUNS in-crate

SimVfs (`crates/backend/storage/file/vfs/src/sim.rs`, 4,173 lines): 512-byte
torn-write floor, fsyncgate failure state machine, dirent durability loss;
`xlogrecovery/tests/sim_crash_sweep.rs` (23 tests) + `fd/src/tests/crash_sweep.rs`
(6). Seed-deterministic (SimClock in `crates/port/pg_clock/src/sim.rs`, SimEntropy
in `pg_strong_random/src/sim.rs`). Last recorded 4/4 pass 2026-07-19 (author box).
Covers recovery machinery, not catalog functions — but the sim cfgs double as
ready-made mocks for the clock/entropy state functions (§3.1).

### 1.6 Loom — EXISTS, RUNS ROUTINELY (best-evidenced gate in the repo)

77 loom tests across 5 crates; `loom-fast` blocking per train, `loom-exhaustive`
post-merge with a dated fleet-job ledger (`notes/loom-exhaustive-ledger.md`). Sync
primitives only; listed for completeness.

### 1.7 cargo-fuzz — EXISTS, RAN ONCE (2026-07-08), NOT ROUTINE; touches zero excluded rows

`fuzz/`: two libFuzzer targets, `wal_record` (WAL decode) and `wire_pqformat`
(protocol format). Crash-only oracle, no C comparison; neither reaches any of the
1,538 catalog functions. The one campaign found 3 real P1 overflows (fixed in
`e34f01e93a`); since then zero runs, corpus gitignored, needs nightly, detached
workspace so even its stable smoke tests never run under root `cargo test`. The only
coverage-guided harness in the project, and it is dormant.

### 1.8 Native (non-Kani) C-vs-Rust differential binaries — EXISTS, MANUAL ONLY; oracle: vendored C; coverage: grid + mass-random; 1 excluded row

The proof-of-concept for §3's primary technique: 8 `src/bin/native_diff_*` binaries
in 5 proofs families (`datetime-b`, `datetime-cmp`, `float-agg`, `brin-minmax`,
`json-escape`) link the **same vendored REL_18_STABLE C** as the Kani harnesses via
a 6-line `cc` build.rs and drive grid + xorshift mass-random inputs (one recorded
run: ~10.7M cases; it caught the `-ffp-contract=off` fma-fusion divergence,
1,779/10.7M cases). Their targets are almost all *proved* functions being re-checked
natively; intersecting the drivers with the excluded ledger yields exactly **one**
excluded row (`interval_div`, interval_avg mean arm). 47/57 families have vendored
`c/`; only 5 have native linking; `proofs/run-suite.sh` is Kani-only. No
proptest/quickcheck/arbitrary anywhere in the repo. Precedent harnesses for
state-pinning exist: `proofs/state-seam-probe` (seam-pinned sequence-adjacent
functions) and `proofs/typcache-inst` (instantiated typcache state in a harness).

### 1.9 Crate differential tests vs live PG — EXISTS, FAIL-OPEN; fixed corpus

7 `tests/differential*.rs` files (like, regex_core, oracle_compat, varchar, quote,
varlena, pathnodes) shell out to `psql -h /tmp -p 5432` against live PG 18.3,
compare results + SQLSTATEs — and **skip silently when no PG is reachable**:
fail-open, vacuous on any box without a warm PG (gate-blindness defect; fix §5).
Hand-enumerated inputs (the like test: 27 strings × 44 patterns): a good fixed
corpus, not exploration.

### 1.10 Antithesis — DOES NOT EXIST

Prose only (README, survey; staged as a budget decision). Zero SDK integration,
zero runs. No plan below leans on it. The simharness verdict grammar remains the
right packaging hook if an engagement ever happens.

---

## 2. The count: oracle vs input coverage across the 1,538

Method (mechanical, reproducible): take the 1,538 excluded names; look for each in
(a) the vendored 232-file regress SQL + isolation specs, (b) in-repo SQL corpora
(`regress/overlay/sql`, `fixtures/*.sql`), (c) the 436 in-repo Rust test files.
Because the ledger uses C/`prosrc` names while SQL calls `proname`, and many
functions are invoked via operators/casts/aggregates/type literals, join through
the vendored PG 18 catalog `.dat` files (`pg_proc`, `pg_operator`, `pg_aggregate`,
`pg_type`, `pg_amproc`). Separately, intersect the excluded list with what the
native-diff binaries and cargo-fuzz targets actually drive (grep-verified call
sites; incidental identifier hits discarded).

**Axis 1 — has a differential oracle today** (some fixed-corpus test reaches it and
compares against real PG):

| class | total | named | via proname | via operator | via aggregate | via type-IO/typmod | via amproc (conditional) | parallel-agg-only (conditional) | wire-name-only (weak) | **no oracle at all** |
|---|---|---|---|---|---|---|---|---|---|---|
| state | 699 | 239 | 127 | 0 | 29 | 8 | 2 | 9 | 6 | **279** |
| engine | 334 | 112 | 58 | 25 | 1 | 5 | 38 | 0 | 6 | **89** |
| wall | 194 | 87 | 47 | 35 | 0 | 13 | 2 | 0 | 4 | **6** |
| typcache | 190 | 96 | 5 | 39 | 8 | 6 | 7 | 3 | 10 | **16** |
| blocked | 65 | 12 | 17 | 0 | 3 | 7 | 1 | 3 | 7 | **15** |
| SRF | 20 | 17 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | **2** |
| agg-state | 16 | 0 | 0 | 0 | 11 | 0 | 0 | 0 | 0 | **5** |
| others | 20 | 4 | 1 | 0 | 1 | 4 | 4 | 0 | 2 | **4** |
| **TOTAL** | **1538** | **567** | **256** | **99** | **53** | **43** | **54** | **15** | **35** | **416** |

**Axis 2 — has attributable input-space exploration today:**

| technique | excluded rows reached |
|---|---|
| native differential mass-random (§1.8) | **1** (`interval_div`) |
| cargo-fuzz coverage-guided (§1.7) | **0** |
| sqlsmith / simharness randomized SQL (§1.3–1.4) | unattributable per-function; value generation too weak to credit |
| **any attributable exploration** | **1 of 1,538 (0.07%)** |

So: 1,122 rows sit behind a real-PG oracle with a fixed corpus; 416 have nothing at
all; input-space exploration of the excluded catalog is effectively zero. The proof
program explores exhaustively on the proved half; the excluded half has an oracle
but no explorer. That asymmetry is what §3–§5 exist to fix.

Qualifications on Axis 1 (they only weaken it): amproc rows (54) count only if
regress builds the relevant index type; parallel-agg rows (15) execute only with
parallelism forced; wire rows (35, plus every `_recv`/`_send` reached by name) are
text-mentioned while the binary paths are barely executed anywhere.

The 416 no-oracle rows concentrate in: `pg_stat_*` (102), other `pg_*`
admin/introspection (98), privilege/ACL fns (21), `binary_upgrade_*` (19),
`pg_get_*` ruleutils variants (13) in **state**; tsearch parser/dictionary
internals, jsonpath `_tz` variants, xml, selectivity estimators, and the 12
RI_FKey_\* in **engine** (RI is name-invisible to this method but heavily
exercised via FK DDL in regress + `fk-e2e.sh`; kept conservative-uncovered in the
TSV, handled in §3.4).

Per-function detail: `proofs/EXCLUDED_COVERAGE.tsv` — columns `oracle_today`,
`input_coverage_today`, `state_mockability`, `test_strategy`.

---

## 3. The plan: in-process coverage-guided differential fuzzing first

Two harness shapes, in strict preference order:

**Shape A — in-process CGF (Rust vs vendored C), the default for every class.**
cargo-fuzz/libFuzzer target per family: structured inputs via `arbitrary` (Datum
images, varlena payloads with length/toast-header edges, struct-shaped decoders —
not raw byte soup), call the Rust builtin and the vendored REL_18_STABLE C shim on
the same input, byte-compare result images and error SQLSTATEs. Coverage
instrumentation steers generation into branch structure (compile the C with
`-fsanitize-coverage` where cheap; Rust-side guidance alone already beats blind
random). Throughput: millions of cases/sec. The proofs tree makes this unusually
cheap — 47/57 families vendor exactly the right C, Kani shims define the ABI, the
native build.rs pattern is proven, existing grid generators become seed corpora.
**Where a function reads session/catalog state, the first question is whether that
state arrives through a seam; if yes, a harness-installed mock serving the real
vendored catalog values turns it into a Shape A target too** (precedents:
`proofs/state-seam-probe`, `proofs/typcache-inst`; the typcache static-catalog mock
is being built by a parallel lane).

**Shape B — SQL-level differential fuzzing (both servers, statement-grain), the
fallback of last resort.** Thousands of cases/sec at best; reserved for functions
whose semantics only exist inside a live server (§3.1's irreducible column) and for
whole-behavior scenarios (RI, crash-recovery). Value-mutating generation aimed at
named function surfaces with **per-function attribution logged** (which makes it
creditable on the coverage axis, unlike sqlsmith today); coverage promotion later
via pgrust-side LLVM coverage snapshots per batch. simharness is the chassis — it
has the dual-server classifier, shrinker, and seeded determinism; it lacks
vocabulary and attribution.

Regress and the e2e fleet remain the smoke/regression rail and the oracle plumbing
Shape B reuses. They verify nothing about input space and are no longer described
as verification.

### 3.1 The 694 `excluded(state)` rows, re-assessed for in-process mockability

(699 ledger rows incl. duplicates-by-oid.) Mechanical sub-classing plus seam
verification in the code (seam mechanism: `seam_core::seam!` = `AtomicPtr` slot,
`::set()` once, panic on uninstalled call; the double-install panic means a mock
harness installs before — instead of — `seams_init::init_all()`, or uses
`test_boot`'s install-if-absent `stub!`). Summary — the sized engineering plan the
class reduces to:

| verdict | rows | share |
|---|---|---|
| **mockable-now** (state already behind an existing seam / sim cfg / TLS setter) | **75** | 11% |
| **mockable-in-process** (in-process with mock catalog rows / fabricated context; no server) | **178** | 25% |
| **mockable-with-new-seam** (narrow choke point exists; seam+mock is real work) | **48** | 7% |
| **mixed — needs per-row triage** | **114** | 16% |
| **irreducible — only meaningful in a live server** | **284** | 41% |

Per sub-class (counts; verdict; why):

- **reg\* I/O incl. to_reg\* (56) — mockable-now, precedent in-tree.** Verified in
  `crates/backend/utils/adt/regproc/src/lib.rs`: **zero direct catalog access** —
  every state touch is a seam (`syscache_seams::lookup_pg_proc_name_candidates`,
  `lookup_pg_namespace_oid_by_name`, `namespace_seams::fetch_search_path`,
  `aclchk_seams::object_aclcheck`, `miscinit_seams::get_user_id`, ...), and
  `regproc/src/tests.rs` already installs a hand-written fake catalog behind 12 of
  them. Built-in pg_proc/pg_namespace/pg_authid rows are static vendored data; the
  generated mock serves real values — same shape as the typcache mock, should share
  its generator. `lsyscache` is likewise a pure seam consumer (120 seam call
  sites, zero direct syscache) — mockable-now.
- **session identity / current_database / GUC-reporting (8) — mockable-now, two
  ways.** `GetUserId`-family state is thread-local `Cell`s with a public atomic
  setter (`CaptureSessionIdentityState`/`ReplaceSessionIdentityState`,
  `miscinit/src/userid.rs`), and name resolution is already
  `syscache_seams::lookup_authid_rolname`; crates that can't dep on miscinit go
  through `miscinit_seams` (`get_user_id`, `get_session_user_id`, ...). Either
  route is a few lines.
- **clock (5: now, clock_timestamp, timeofday, ...) and entropy (6: drandom,
  setseed, ...) — mockable-now.** SimClock/SimEntropy already exist
  (`crates/port/pg_clock/src/sim.rs`, `pg_strong_random/src/sim.rs`) — the DST
  substrate doubles as the harness mock.
- **privilege/ACL (95: has_\*_privilege, aclitem I/O, pg_has_role, ...) —
  mockable-in-process, with two named seam cuts.** `adt_acl` is the one mixed
  crate: `get_role_oid` calls the **real** `GetSysCacheOid(AUTHNAME, ...)`
  directly (`acl/src/membership.rs:308`) — but `syscache_seams::
  lookup_authid_by_rolname` already exists with exactly the right signature, so
  the cut is near-mechanical; `has_privs_of_role`'s membership walk is the harder
  choke (it is precisely what `proofs/state-seam-probe` stubbed for its aclmask
  proof, so the pinning shape is known). Beyond that, the work is synthetic
  catalog content (roles, relations, ACL arrays) served to both sides — these
  functions are only interesting over user objects, and the C oracle needs the
  same fixtures.
- **agg accumulators (40: numeric_accum, int8_avg_\*, serialize/deserialize/
  combine) + ordered-set/hypothetical finals (13) — mockable-in-process.** State =
  aggcontext memory + (for finals) tuplesort state, all fabricable in-process; no
  catalog at all. Natural Shape A targets with high Tier-1 value (silent wrong
  aggregates).
- **GUC show/set (6), timezone (2), comments/obj_description (3), misc catalog
  lookups (2), planner support fns (8), typanalyze hooks (4) —
  mockable-in-process.** Session-local tables or syscache-seam reads with small
  fixture needs; support/typanalyze need fabricated planner nodes/VacAttrStats.
- **SRF generate_series family (5) — mockable-in-process, wrapper included.**
  Census correction to the old assumption that SRF plumbing needs an executor:
  ValuePerCall mode needs only an `fcinfo.resultinfo` pointing at a
  `T_ReturnSetInfo` node and an `FmgrInfo` with a `Box<FuncCallContext>` in
  `fn_extra` — plain structs, constructible in a test (`funcapi_srf/src/lib.rs`,
  112 lines); Materialize mode needs `InitMaterializedSRF` + an in-memory
  `Tuplestore`, no executor node. **There is a callable core**; what varies per
  SRF is its data source, which is the usual seam question.
- **ruleutils/deparse pg_get_\* (45) — mockable-with-new-seam, expensive.**
  Deparse walks arbitrary user-object catalog graphs (pg_class, pg_attribute,
  pg_rewrite, ...). The reads are seam-shaped but a useful mock is a whole
  synthetic-catalog corpus, and the C oracle needs the same objects — realistically
  this stays Shape B (create objects via SQL both sides, compare deparse output,
  fuzz the *object definitions*), with the seam-mock reserved for targeted kernels.
- **other pg_\* admin (107) — mixed, needs per-row triage.** Contains a sizeable
  syscache-visible subset that is mockable (`pg_typeof`, `pg_*_is_visible`,
  `pg_basetype`, `pg_input_is_valid` — the latter fans into type-input functions
  and is itself a fuzz multiplier), statistics-type I/O
  (`pg_ndistinct/pg_mcv_list/pg_dependencies in/out/recv/send` — parsing is
  in-process testable), and genuinely server-bound rows (replication slots,
  `pg_promote`, `pg_notify`, relation sizes, `pg_sleep`). Triage is a half-day
  with the TSV open.
- **misc-residual (7) — mixed** (parse_ident likely near-pure; validators and
  amvalidate are catalog-bound).
- **trigger-context (3: suppress_redundant_updates_trigger, ...) —
  mockable-with-new-seam, borderline.** `TriggerData` is a plain `#[repr(C)]`
  struct with public constructors (`types_trigger_call/src/lib.rs`); ~12 test
  files already fabricate `RelationData` literals. Real but expensive
  per-function, and the C oracle needs the same fabrication in C.
- **Irreducible (284 total), with honest reasons — note "shmem" is NOT the
  reason.** pgrust "shared memory" is heap (`storage/ipc/shmem/src/lib.rs`: a
  leaked zeroed allocation shared across backend *threads*), so shmem-backed
  structures can be initialized in-process. What is irreducible is the **oracle**:
  - `pg_stat_*` stats (138): `pgstatfuncs` deps directly on
    `backend_status`/`pgstat` with no seam between them, and even with one, the C
    side would report its own live cluster's nondeterministic counters — there is
    no shared expected value. Blocker = oracle equivalence, not execution. Test =
    invariants + view-shape differential (§3.6 tier 3).
  - WAL/replication admin (37), advisory locks (21), lock views (4),
    xact/snapshot (4): live WAL positions, lock-manager state, live xids — same
    oracle problem. (`pg_lock_status`'s 16-column row formatter is pure given a
    `LockInstanceData`; a `get_lock_status_data` seam would free that half.)
  - large objects (20): no choke point — the function *is* the storage path (real
    heap scans of pg_largeobject under a real snapshot in a real transaction,
    resowner-tracked descriptors). Shape B pinned-state.
  - filesystem admin (19): server directory layout; Shape B smoke + invariants.
  - `binary_upgrade_*` (19): only meaningful inside pg_upgrade; pg_upgrade e2e.
  - xml export via SPI (16: table_to_xml, ts_stat, ...): executes SQL internally.
  - index-maintenance admin (6): operates on on-disk indexes.

Net: **~301 rows (43%) of the state class can move in-process** (75 now, 178 with
fixture work, 48 with new seam work), plus whatever the 114-row triage yields;
~284 stay server-side — and of those, the stats/monitoring majority is Tier 3
(invariants suffice), so the irreducible-AND-important residue is small (large
objects, xact/snapshot, admin actions), which is exactly what Shape B is for.

The census also names the cheapest implementation: a `_support` crate mirroring
`cache_syscache/src/projections.rs` (the 125-call production installer) but
sourcing rows from the vendored static builtin catalog data, installed via
`test_boot`'s install-if-absent macro so per-test overrides win. One crate covers
reg\* I/O, lsyscache, typcache, format_type, and session identity at once; the
two named seam cuts (`get_role_oid`, `get_lock_status_data`) extend it. There is
also a second escape hatch for hit-path realism: `catcache/src/testing.rs`
(`init_cache_bare` + `insert_positive`) builds a warm **real** syscache without
the relcache/scan substrate, so the genuine `SearchSysCache` hit path can run
under the mock data rather than being bypassed.

### 3.2 `wall` (194) → Shape A CGF, first wave

Float I/O strtod-class, Ryu images, numeric transcendentals, to_char/to_date,
geometry, json builders, hashes. Highest value-density: silent numeric drift lives
here, and the 10.7M-case fma finding proves the method draws blood. All
prerequisites in-tree.

### 3.3 `typcache` (190) → Shape A via the generated static-catalog mock

`typcache::lookup_type_cache` reaches the catalog solely via
`syscache_seams::lookup_pg_type_typcache_shape::call(type_id)`; built-in pg_type
rows are static vendored data, so a generated mock serves real values. A parallel
lane is building that mock and will report how many of the 185 typcache rows it
reclassifies to Shape A; array/range/record kernels then fuzz in-process. Binary
wire residue (`record_recv`, int2vector/oidvector) folds into the COPY BINARY
target (work item 3). Whatever the mock cannot serve (anonymous record registries
built at runtime) stays Shape B.

### 3.4 `engine:trigger` / RI_FKey_\* (13) → scenario + crash-replay + simharness FK vocabulary (Shape B by necessity)

Data-integrity behavior spanning executor, snapshots, and EPQ — unit differential
is the wrong shape and no seam mock reproduces it honestly. Already strong on
fixed corpus: regress `foreign_key.sql` (2,509 lines), `fk-e2e.sh` incl. the gold
gate (real PG boots the pgrust datadir and must itself enforce the pgrust-created
FK — catching wrong catalog state, not just wrong messages),
`trigger-crash-replay-e2e.sh`. The hole is randomized + concurrent +
crash-composed RI: simharness vocabulary (work item 2). `ri_triggers` (1,929
lines) has zero unit tests — owed regardless.

### 3.5 `engine` bare: LIKE/regex/jsonpath/tsearch/xml (~233) → Shape A kernels + Shape B for session-dependent variants

Regex compile/exec, LIKE matchers, jsonpath eval, tsvector/tsquery ops are
in-process computable today (most take no catalog state — they're excluded as
"engine-dispatched", which affects how they're *called*, not what they *compute*).
Shape A them directly; the existing live-PG crate differential corpora become seed
templates. jsonpath `_tz` needs session TimeZone — a GUC pin, mockable-now.
tsearch parser/dictionary internals need ts config catalogs (static built-ins →
mock like typcache); xml: verify pgrust ships xml before spending anything.

### 3.6 Remaining engine + planner classes

- **selectivity estimators (46 + planner-node 4) → estimate-diff-monitor.** They
  affect plan choice, not results. Assert only outputs ∈ [0,1] and non-NaN (NaN
  poisons cost comparison — GL-TAIL-2 precedent). Beyond that: EXPLAIN-estimate
  comparison (same schema + ANALYZE both sides, per-node estimated rows within
  tolerance), alert-don't-fail. Not worth CGF depth. Note these are *also* Shape
  A-able against vendored C selfuncs with mocked stats tuples if drift monitoring
  ever finds something worth pinning.
- **sortsupport/am-handler/window/tablesample (~42) → randomized index battery +
  amcheck.** No SQL surface of their own; correct iff structures built through
  them are. Fuzz the *data* (Shape B tables, adversarial key distributions), build
  identical indexes both sides, compare amcheck verdicts + index-scan results;
  sortsupport comparators additionally Shape A (pure comparisons).
- **agg-state (16) + 15 parallel-only rows →** Shape A for transition/serialize
  kernels (aggcontext fabrication, §3.1) plus one Shape B forced-parallel leg
  (`parallel_setup_cost=0` etc.) so combine/serialize actually execute end-to-end.
- **blocked (65) / unimplemented (8) / port (3) → punch list, not technique.**
  Testing can't substitute for implementation; the 12 unimplemented+port rows
  belong on the v0.2 release checklist so nothing ships silently.
- **SRF (20)** → value cores Shape A where extractable; wrappers Shape B
  pinned-state.

---

## 4. Risk tiers (risk × cost; what gets CGF depth first)

**Tier 1 — silent wrong answers or data corruption if wrong → CGF depth mandatory:**
- Type I/O round-trips (float4/8, numeric, datetime/interval — wall core, ~120
  rows): silent value drift on dump/restore/COPY/wire. Cost **low** (Shape A).
- recv/send binary wire (~70 rows): silent corruption on COPY BINARY / binary
  protocol; effectively untested. Cost low-medium (work item 3).
- Aggregate machinery incl. parallel combine/serialize (~70 rows across
  state/agg-state): silent wrong aggregates. Cost low (Shape A after aggcontext
  fabrication).
- RI_FKey_\* (13): orphaned/wrongly-cascaded rows. Cost medium (work item 2).
- Array/range/record mutation ops (~60 rows): wrong stored values (Shape A via
  typcache mock).
- Hash functions feeding joins/aggs: Shape A, cheap.
- Privilege/ACL functions (95): wrong answer = privilege escalation surface
  (NSACL precedent: a deferred authorization check was a live security bug).
  Tier 1 by consequence even though "read-only".

**Tier 2 — visible wrong answers, no corruption:** LIKE/regex/tsearch/jsonpath
results, geometry distances, to_char images, deparse output used by pg_dump
(arguably T1 for dump/restore fidelity — revisit after triage).

**Tier 3 — cosmetic/monitoring/plan-shape → invariants + monitoring only:**
`pg_stat_*` values, error message text, selectivity accuracy, `timeofday`
formatting. Never worth fuzz depth.

Rule the tiers encode: **anything that writes, round-trips, hashes, aggregates, or
gates access gets coverage-guided input depth; anything that only reports gets
shape checks.**

---

## 5. Recommended next three pieces of work

### Work item 1 (start immediately): in-process CGF platform — wall class first, seam-mock second wave

Sized: ~1 week platform + first 10 families; ~30–60 min/family after; seam-mock
wave gated on the typcache-mock lane's generator.

Spec an agent can start on:
1. New cargo-fuzz workspace `proofs/fuzz/` (nightly-only acceptable there; keep
   out of the root workspace like `fuzz/` is). One target per proof family:
   `proofs/fuzz/fuzz_targets/<family>.rs`.
2. Shared `proofs/fuzz/diffcore` lib: `#[derive(Arbitrary)]` structured input
   types (f64-from-bits, bounded numeric digit strings, datetime component
   structs, varlena payloads with length/toast-header edges), dual-call +
   byte-exact Datum/varlena image compare, SQLSTATE compare on error paths,
   replay-artifact writer (libFuzzer-minimized failing inputs into
   `proofs/fuzz/artifacts/<family>/`).
3. Native build prerequisite: the 6-line `cc` build.rs (pattern:
   `proofs/datetime-b/build.rs`) for wall families lacking it (42 of 47 with
   vendored `c/`). `-fwrapv` and `-ffp-contract=off` mandatory (documented
   1,779/10.7M fma false-divergence without the latter). Linking gotcha documented
   in `native_diff_datetime_b.rs`: the target must `use proof_<family> as _;` to
   keep the C archive in the link. Where cheap, compile C with
   `-fsanitize-coverage=trace-pc-guard` so guidance sees both sides.
4. Seed corpora: existing grid generators (strtod hard cases, 17-digit
   round-trips, exponent edges, Ryu shortest-image boundaries, datetime bounds)
   emitted once into `corpus/<family>/`. Grid + random + guidance, in that order.
5. Wire a `fuzz-diff` tier into `proofs/run-suite.sh` (currently Kani-only):
   `run-suite.sh fuzz-diff <family> --minutes N`, plus `--ci-smoke` replaying the
   checked-in corpus + artifacts deterministically on stable (no nightly), so a
   gate exists that actually runs per train — otherwise this becomes §1.7's
   dormancy. Register the smoke in the release-gate tier list.
6. First wave (Tier 1 order): float I/O (float4in/float8in/float8out incl. Ryu
   images), numeric transcendentals, datetime I/O + to_char/to_date + trunc/age,
   json builders, hashes, geometry distance kernels. Second wave (after the
   typcache/state mock generator lands): reg\* I/O, privilege/ACL with fixture
   catalogs, agg accumulators via aggcontext fabrication, regex/LIKE/jsonpath
   kernels.
7. Acceptance per family: ≥2 CPU-hours guided fuzz, zero divergence with a
   coverage report showing branch saturation on both C and Rust targets, or a
   filed divergence with minimized replay artifact; ledger note per family. Fixed
   time budget per family (expensive-rerun-loop guard); findings ratchet into the
   seed corpus.

### Work item 2: FK/trigger vocabulary + per-function attribution for simharness

Sized: ~1 week. Add to `tools/simharness/src/gen/`: a child table with
`REFERENCES parent(id)` (per-run draw over ON DELETE/UPDATE ∈ {NO ACTION,
RESTRICT, CASCADE, SET NULL, SET DEFAULT}), generator weights for
parent-delete/parent-update/child-insert, ledger-model extension predicting
post-action child multisets, and **per-function attribution logging** (which
catalog functions each campaign executed — what makes Shape B creditable on the
coverage axis at all). Then: (a) run under the existing differential oracle,
(b) compose with `sim-fault` crash cuts so RI enforcement survives recovery,
(c) multi-session concurrent parent-delete vs child-insert (the EPQ-adjacent race
no fixed corpus reaches). Also owed: first unit tests on `ri_triggers`. Touches
actively-developed harness code — coordinate with the DST lane owner.

### Work item 3: binary-wire differential fuzz + forced-parallel leg + fail-closed fixes

Sized: ~4 days, bundled:
- **3a** COPY BINARY differential fuzz target: structured-arbitrary row images
  through `COPY ... (FORMAT binary)` out/in on both engines; byte-compare binary
  files and reloaded contents; cross-feed pgrust's binary file into C and vice
  versa. Closes the weakest oracle column (~70 recv/send rows) and builds the COPY
  fuzz surface `docs/correctness-testing-survey.md` flagged as unoccupied.
- **3b** forced-parallel aggregate corpus — flips the 15 `parallel-only`
  conditional rows to actually-executed.
- **3c** fail-closed fixes: the 7 crate differential tests must fail (not skip)
  when PG is unreachable unless `PGRUST_DIFF_ALLOW_SKIP=1`; the `fuzz/`
  workspace's stable corpus-replay smokes get a gate-script caller so the
  2026-07-08 findings stay live. Direct gate-blindness-law remediations.

After these three plus the state/typcache mock wave: wall, wire, aggregates, and
the seam-mockable state surface all gain coverage-guided in-process depth; the
worst Tier-1 hole (RI under concurrency/crash) is closing with attribution; the
two fail-open mechanisms are fail-closed. The residue is the irreducible-but-Tier-3
monitoring surface (invariants) and the small irreducible-and-important set (large
objects, xact/snapshot, admin actions) riding Shape B.

---

## 6. Ledger annotation: deferred, sidecar shipped instead

The request was to add a `test_strategy` annotation to the excluded rows of
`proofs/USER_FACING_FUNCTIONS.tsv` if mechanical and safe. It is not safe right
now: 16 active `.wt-prf-*` worktrees are editing that file (the triage lane's copy
changed hours before this was written); a 1,538-row rewrite would conflict with
every in-flight lane. Instead: `proofs/EXCLUDED_COVERAGE.tsv` — one row per
excluded function, `name, class, oracle_today, input_coverage_today,
state_mockability, test_strategy` — join-able on `name`. Follow-up at the next
proof-lane quiesce: a one-shot script folds `test_strategy` into the ledger notes
column, joining on (name, reason) and refusing on any row whose status changed
since this snapshot; then delete the sidecar.

## 7. README honesty note

`README.md` currently implies the proof program is the correctness story. The
accurate public claim: proofs (exhaustive, machine-checked) for the provable
~half; a real-PG differential oracle over a fixed corpus for 73% of the excluded
half — smoke, not verification; input-space exploration of the excluded half is
effectively zero today and is being built as in-process coverage-guided
differential fuzzing per this document. The Antithesis sentence stays future-tense
until an engagement actually runs.
