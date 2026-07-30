# Testing strategy for the 1,538 proof-excluded functions

Date: 2026-07-30. Base: main `515372715f`. Companion data: `proofs/EXCLUDED_COVERAGE.tsv`
(machine-generated per-function coverage + strategy map; regeneration method in §2).

`proofs/USER_FACING_FUNCTIONS.tsv` has 3,189 rows; 1,538 (48%) are `excluded(<reason>)`
from the Kani equivalence-proof program. This document answers: **how is each excluded
class tested instead, what already covers it today (with evidence), and what to build
next.** It deliberately distinguishes "a mechanism exists that could cover this" from
"this is covered today by something that actually runs" (the campaign's own
gate-blindness law: a mechanism nobody runs is not coverage).

Reason breakdown (leading token): state 699, engine 334 (planner 46, sortsupport 16,
window 15, trigger 13, am-handler 9, tablesample 2, bare/pattern-match+tsearch+xml+jsonpath 233),
wall 194, typcache 190, blocked 65, SRF 20, agg-state 16, unimplemented 8, planner-node 4,
port 3, non-surface 3, catalog 1, no-logic 1.

---

## 1. What testing already exists (inventory, with run-status)

### 1.1 pg_regress differential parity vs real PostgreSQL 18.3 — EXISTS, RUNS, PASSES

This is the single most important fact in this document: **the full upstream PG 18.3
regression suite runs against pgrust with expected outputs taken verbatim from real
PostgreSQL — it is a differential test, not a self-referential regression test.**

- Corpus: 232 `sql/` files / 265 expected files in the vendored PG source
  (`../pgrust-reference/vendor/postgres-src/src/test/regress`); 230 scheduled tests.
- Runner: `scripts/pg-regress-fast.sh` drives the **real `pg_regress` binary** in
  `--use-existing` mode against a pgrust postmaster with C 18.3 `initdb`/`psql`, and
  passes `--expecteddir` pointing at the vendored PG expected outputs. Expected files
  are never written by pgrust. Pinned-transcript harnesses are minted from live C
  (`scripts/auth-regen-expected.sh`: "MINTED from live C — never hand-edited").
- Overlay: `regress/overlay/sql/` (154 files) contains byte-identical copies of vendor
  SQL plus `-- pgrust:` annotations only (a `cmp` guard in the runner enforces this);
  10,608 `pgrust:rowsort` annotations relax those statements to row-multiset
  comparison. Unannotated statements remain byte-exact.
- Anti-gaming: `scripts/lane-gates.sh` adds engagement floors and a refusal allowlist
  because "regress-parity alone is gameable (a lane that refuses everything passes it)".
- Isolation: 119 upstream isolation specs + 12 overlay EPQ specs
  (`regress/isolation-overlay/`), expected outputs = C outputs verbatim, run via real
  `pg_isolation_regress`. Last recorded: 119×3 arms PASS.
- Run evidence: repeated CI cluster jobs with S3 artifacts, e.g. "pg_regress ALL: 230/230 ok"
  (`notes/batchemit-lane.md:105`, `notes/asciilen-lane.md:107`,
  `scripts/lane-gates.floors.CI-pod-pgdg:26-30`); progression from 44/230
  byte-identical on 2026-07-04 to 230/230 by mid-July. Latest dated pass evidence in
  this tree is ~2026-07-21; nothing shows a run at the 2026-07-30 tip.

Caveats: (1) the corpus is **not in this repo** — a fresh clone cannot run regress
(depends on two sibling checkouts, one absent on this laptop); (2) there is **no CI**
(no `.github/`) — the gate fires only when a human submits a CI cluster job; (3) standing
env-failure allowlist entries (e.g. `psql_pipeline`); (4) the order-compare comparator
has a documented flake mode; (5) contrib isolation lists are not wired.

### 1.2 SQL-level differential e2e scripts — EXISTS, RUNS (per-lane), 622 scripts

`scripts/` holds 622 `*e2e*.sh`, ~227 of which boot pgrust and C PG 18 side by side on
an identical corpus and byte-compare (`*-oracle-e2e.sh` family, `fk-e2e.sh`,
`trigger-*-e2e.sh`, `sqlsmith-diff-e2e.sh`, `regress-diff.sh` with its 630-line
frozen-C expected). 485 scripts include a kill-9 + recovery phase; several do
**bidirectional WAL cross-replay** (C replays pgrust's WAL and vice versa). The leg
manifest that decides which of these run per train lives in the reference CI cluster repo, not
here — from this tree we cannot enumerate which are standing gates vs one-shot lane
artifacts. Treat individual e2e scripts as "ran when their lane ran".

### 1.3 sqlsmith differential campaigns — EXISTS, RAN, found real bugs

`scripts/sqlsmith/run-campaign.sh` + `triage.py`: 20k-query crash-hunt against pgrust,
then full-corpus replay against **both** pgrust and C with per-statement divergence
classes (`rust-crash`, `wrong-results`, `rust-err-c-ok`, `err-text-drift`, ...).
Checked-in repro corpora (`repros-campaign1/3.sql`, `repros-wrongresults39.sql`, ...)
prove real campaigns ran and yielded. The "zero-crash ratchet" is cited in
`docs/unsafe-audit-2026-07-19.md`. Operator-run, not commit-gated.

### 1.4 simharness (= `crash-simulator/`, byte-identical duplicate) — EXISTS, PARTIALLY RUNS

`tools/simharness/`: seeded generative SQL campaign engine (20k LOC, 21 property
oracles incl. TLP/NoREC, differential-vs-C classifier, shrinker, multi-session, and a
`sim-fault` leg that composes a whole-node crash cut with product `StartupXLOG`
recovery and re-verifies committed multisets). 14-leg converge gate
(`scripts/sim-harness-converge-e2e.sh`) recorded all-PASS 2026-07-19 — **on an author
box, not a recorded CI cluster gate**. Known holes: generated plans emit **zero**
Crash/TornWrite faults (driver `NotWiredYet`, emission weight 0), fault × multi-session
never composed, the 24h FP-budget CI cluster campaign and planted-bug gate are still open,
`--diff-c` has a known DUT-split bug. **The generator vocabulary contains no FOREIGN
KEY, no trigger DDL** (`tools/simharness/src/gen/schema.rs`).

### 1.5 Deterministic simulation substrate (`--cfg pgrust_sim`) — EXISTS, RUNS in-crate

SimVfs (`crates/backend/storage/file/vfs/src/sim.rs`, 4,173 lines): 512-byte torn-write
floor, fsyncgate-semantics failure state machine, directory-entry durability loss.
`xlogrecovery/tests/sim_crash_sweep.rs` (23 tests) sweeps a crash cut over every
workload op; `fd/src/tests/crash_sweep.rs` (6 tests) exercises the real
fsync/durable_rename protocol. SimClock + SimEntropy make runs seed-deterministic.
Last recorded 4/4 sweep pass 2026-07-19 (author box).

### 1.6 Loom — EXISTS, RUNS ROUTINELY (best-evidenced gate in the repo)

77 loom `#[test]`s across 5 crates (runtime 51, waiter 14+1, latch 6, pg_barrier 3,
fdnb 2). Two-tier policy: `loom-fast` blocking per train (≤5 min), `loom-exhaustive`
post-merge (dated ledger with CI cluster job IDs, `notes/loom-exhaustive-ledger.md`).
Not relevant to the excluded-function catalog (it covers sync primitives), listed for
completeness.

### 1.7 cargo-fuzz — EXISTS, RAN ONCE (2026-07-08), NOT ROUTINE

`fuzz/`: two libFuzzer targets, `wal_record` (CRC-corrected WAL record bodies through
`decode_record`) and `wire_pqformat`. Crash-only oracle (panic = P1), no C comparison.
The one campaign found 3 real P1 overflows (fixed in `e34f01e93a`). Since then: zero
runs, corpus gitignored, needs nightly, detached workspace so even its stable smoke
tests never run under root `cargo test`. The COPY fuzz target proposed in
`docs/correctness-testing-survey.md` §6 was never built.

### 1.8 Native (non-Kani) C-vs-Rust differential binaries — EXISTS, MANUAL ONLY

The real template for high-volume differential testing: 8 `src/bin/native_diff_*`
binaries in proofs families (`datetime-b`, `datetime-cmp`, `float-agg`, `brin-minmax`,
`json-escape`) that link the **same vendored REL_18_STABLE C** as the Kani harnesses
natively via a 6-line `cc` build.rs and drive grid + xorshift mass-random inputs (one
recorded run: ~10.7M cases). 47/57 proof families already have vendored `c/`; only 5
have the native build.rs. `proofs/run-suite.sh` is Kani-only and knows nothing of
these. No proptest/quickcheck/arbitrary anywhere in the repo.

### 1.9 Crate differential tests vs live PG — EXISTS, FAIL-OPEN

7 `tests/differential*.rs` files (like, regex_core, oracle_compat, varchar, quote,
varlena, pathnodes) shell out to `psql -h /tmp -p 5432` against live PG 18.3 and
compare results + SQLSTATEs — but **skip silently when no PG is reachable**. Under the
gate-blindness law this is a defect: on any box without a warm PG these pass
vacuously. (Fix proposed in §5.)

### 1.10 Antithesis — DOES NOT EXIST

Prose only (README, survey, DST literature review; staged as a budget decision).
Zero SDK integration, zero runs. **No plan below may lean on Antithesis as the
near-term answer for anything.** The simharness verdict grammar was deliberately
designed to be Antithesis-packageable later; that remains the right hook.

---

## 2. How many of the 1,538 are already exercised — the count nobody had done

Method (mechanical, reproducible): take the 1,538 excluded names; look for each in
(a) the vendored 232-file regress SQL + isolation specs, (b) the in-repo SQL corpora
(`regress/overlay/sql`, `fixtures/*.sql` incl. the ~60 e2e fixture files), (c) the 436
in-repo Rust test files. Because the ledger uses C/`prosrc` names while SQL calls
`proname`, and many functions are invoked via operators/casts/aggregates/type
literals rather than by name, join through the vendored PG 18 catalog `.dat` files
(`pg_proc`, `pg_operator`, `pg_aggregate`, `pg_type`, `pg_amproc`): a function counts
as reached if its `proname` appears, or it implements an operator whose symbol appears,
or it is transition/final machinery of an aggregate whose name appears, or it is the
in/out function of a type whose name appears (literals/casts), etc.

Result (full per-function map in `proofs/EXCLUDED_COVERAGE.tsv`):

| class | total | direct name | via proname | via operator | via aggregate | via type-IO/typmod | via amproc (index-implicit) | parallel-agg-only | binary-wire only | **no evidence at all** |
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

Headline: **~1,122 of the 1,538 (73%) are already exercised by the CI-run
differential regress corpus or an in-repo test, and nobody had counted.** Because
regress expected outputs come from real PG (§1.1), every one of those reaches is a
*differential* check, not a smoke test.

Three honest qualifications:

1. **Exercised ≠ tested to depth.** Appearing in regress means some inputs are
   byte-compared against C — often a handful. For total functions on small domains
   (comparators, boolean ops) that is substantial; for `float8in`, `to_char`, or
   jsonpath it samples a sliver of the input space. Depth is exactly what §3 assigns
   per class.
2. **The amproc column (54) and parallel-agg column (15) are conditional.** amproc
   support functions are reached only when regress builds/uses the relevant index
   type; agg combine/serialize functions only run under parallel aggregation — a
   regress run with parallelism disabled never executes them. These need the forced
   arms in §3.
3. **binary-wire (35 + every `_recv`/`_send` counted elsewhere by name) is the
   weakest column**: recv/send appear in corpus text but binary COPY / binary
   protocol round-trips are barely exercised anywhere. Treat as uncovered in
   practice.

The 416 with no evidence at all concentrate exactly where you'd expect:
`pg_stat_*` (102), other `pg_*` admin/introspection (98), privilege/ACL fns (21),
`binary_upgrade_*` (19), `pg_get_*` ruleutils variants (13) in **state**;
tsearch parser/dictionary internals, jsonpath `_tz` variants, xml, selectivity
estimators, and the **12 RI_FKey_\*** in **engine**; and small residues elsewhere
(the wall class residue is just 6 names).

Note on RI_FKey_\*: they show "no evidence" under name-matching, but §1.2's
`fk-e2e.sh` + regress `foreign_key.sql` (2,509 lines) exercise them heavily via FK
DDL. The map's `via` columns can't see trigger dispatch; the per-function TSV keeps
them conservative-uncovered and §3 handles them explicitly.

---

## 3. Class → technique map

Ordering principle: every technique below must terminate in a *differential* or
*invariant* check against C 18.3, because that is the project's standing bar and the
only oracle that scales.

### 3.1 `wall` (194) → native differential fuzzing — CONFIRMED, cheapest high-volume win

These are provable-in-principle functions the solver can't reach (float I/O
strtod-class, Ryu result images, numeric transcendentals, `to_char`/`to_date`,
geometry distances, json builders, md5/sha). The hypothesis holds and the evidence
strengthens it: the vendored C and the shims already exist (47/57 families have
`c/`), the native-link pattern is proven in 5 families with a 6-line build.rs, and
one existing binary already did a 10.7M-case run. Only 6/194 wall rows have no
regress reach at all — but regress depth here is trivial relative to the input
space, and this class is precisely where silent numeric drift lives (the
`-ffp-contract=off` finding — 1,779/10.7M cases diverging via fma fusion — was
caught exactly this way). **Build the shared native-diff runner (work item 1, §5).**

### 3.2 `state` (699) → split three ways

The hypothesis ("SQL differential with pinned state, invariants where genuinely
env-dependent") survives, but the inventory shows most of the class is *already*
in the first bucket:

- **(a) Deterministic-under-pinned-state (≈420 rows, incl. the 366 already reached
  via name/proname):** `current_user`, reg* I/O, `has_*_privilege`, ACL functions,
  large objects, `pg_get_viewdef`-style ruleutils. Regress already covers these
  differentially (same roles/objects created on both sides ⇒ same output). The gap
  is the ~100 admin/introspection functions regress never calls. Technique: extend
  the e2e-oracle pattern (§1.2) with a pinned-state corpus that creates identical
  catalogs on both servers and byte-compares. Owner of record: one new
  `state-oracle-e2e.sh` sweeping the uncovered list from the TSV.
- **(b) Genuinely environment-dependent (≈150 rows):** `pg_stat_*` counters, backend
  PIDs/addresses, WAL positions, `pg_lock_status` values. Equality vs C is
  meaningless; test **invariants**: view/SRF shape matches C exactly (column names,
  types — this IS differential), values are type-valid, counters are monotone where
  C's are, known identities hold (e.g. blocks_hit ≤ blocks_fetched). Marked
  `invariant-only` in the TSV (143 rows).
- **(c) `binary_upgrade_*` (19):** only meaningful inside pg_upgrade; test = a
  pg_upgrade e2e (C-initiated upgrade of a pgrust cluster and vice versa), which the
  fk-e2e "gold gate" pattern (real PG boots pgrust's datadir) already prototypes.

### 3.3 `engine:trigger` / RI_FKey_\* (13) → scenario + crash-replay + simulator vocabulary — CONFIRMED, with a named hole

Already strong today: regress `foreign_key.sql`/`triggers.sql`/`constraints.sql` run
differentially; `scripts/fk-e2e.sh` byte-compares FK behavior *and* has the gold gate
(real PG 18 boots the pgrust datadir and must itself enforce the pgrust-created FK —
catching wrong catalog state, not just wrong messages); `trigger-crash-replay-e2e.sh`
covers trigger side-effects across PANIC + recovery. The hole is concurrency:
**the simharness generator emits no FK/trigger DDL, so RI is never exercised under
the multi-session oracle, under crash cuts, or under shrinking; and `ri_triggers`
(1,929 lines) has zero unit tests.** RI under concurrent
delete/update with EPQ is the classic silent-corruption surface. Work item 2 (§5).
Antithesis remains the eventual amplifier but is not load-bearing (§1.10).

### 3.4 `engine:planner` selectivity estimators (46 + planner-node 4) → estimate-diff, monitoring stance — CONFIRMED

`eqsel`/`neqsel`/`scalarltsel`-class functions affect plan choice, not results; a
wrong estimate is a performance bug, with two exceptions worth asserting: outputs
must be in [0,1] and non-NaN (a NaN selectivity poisons cost comparison — the
cost-route NaN abstention in GL-TAIL-2 is precedent). Technique: an
EXPLAIN-estimate differential harness — same schema + same `ANALYZE` statistics on
both servers, compare **estimated row counts** per plan node across a query corpus
within tolerance, alert-don't-fail on drift. Regress `stats_ext.sql` (390 rowsort
annotations) already exercises the extended-stats paths differentially. Tier-3
otherwise; no per-function assertion program is warranted.

### 3.5 `engine` bare: LIKE/regex/jsonpath/tsearch/xml (~233) → SQL differential, one family at a time

- LIKE/regex: heavily reached via operators in regress `strings.sql` plus the
  dedicated live-PG differential test (`adt/like/tests/differential.rs`) — which
  must be made fail-closed (§5, item 3c).
- jsonpath `_tz` variants (uncovered): need a pinned-TimeZone differential corpus —
  trivially expressible as an e2e script.
- tsearch parser/dictionary/rank internals (~50 uncovered) and xml (pgrust may not
  even ship xml — verify before spending): the largest genuinely untested engine
  block. SQL differential through `ts_parse`/`ts_debug`/`ts_rank` corpora; upstream
  regress `tsearch.sql` runs in the 230 — confirm engagement (a refused/stubbed
  tsearch path would pass regress via the allowlist trap; check
  `lane-gates.allowlist` for tsearch refusals before trusting it).

### 3.6 `engine:sortsupport`/`am-handler`/window/tablesample (~42) + amproc-implicit rows → index battery + amcheck

Sortsupport and am support functions have no SQL-visible surface of their own; they
are correct iff sorts/indexes built through them are correct. Technique: index-build
differential (build same index both sides, compare `amcheck` verification + query
results through the index; `contrib/amcheck` is ported). Window support: covered via
window-function corpus (`window.sql` + fixtures `window-tier2/3.sql` already reach
the named window functions); the `*_support` planner-optimization entries fold into
§3.4's monitoring stance.

### 3.7 `typcache` (190) → SQL differential through arrays/ranges/records — CONFIRMED, mostly already covered

174/190 have regress reach (arrays.sql, multirangetypes.sql 456 rowsort annotations,
rowtypes.sql). The residual 16 are range canonical/subdiff and `*_support`
functions that execute implicitly on every range constructor — reached, just
invisible to name-matching. Real remaining work: (a) record_recv/record_send and
the int2vector/oidvector wire functions → binary COPY differential (work item 3);
(b) `array_sort_order*`, `width_bucket_array`, `array_position_start` → add to the
pinned-state e2e corpus. Depth beyond regress comes free once the simharness
vocabulary grows composite/array columns (natural extension of work item 2).

### 3.8 `agg-state` (16) + parallel-only machinery (15 across classes) → forced-parallel aggregate differential

json_agg/jsonb_object_agg transition/final functions are reached via their aggregate
names; the strict/unique variants (5 uncovered) need corpus rows with NULLs and
duplicate keys. Crucially, **combine/serialize/deserialize run only under parallel
aggregation** — one e2e leg with `parallel_setup_cost=0, parallel_tuple_cost=0,
min_parallel_table_scan_size=0` re-running the aggregate corpus makes the parallel
paths differential too. Cheap: one script.

### 3.9 `blocked` (65), `unimplemented` (8), `port` (3) → per-row triage, not a technique

These reasons mean "the Rust side isn't there or can't be built yet". Testing can't
substitute for implementation; the strategy is: keep them in regress reach where
possible (50/65 blocked rows already are), and treat the TSV as the punch list.
`length_in_encoding` (unimplemented, unreached) and friends should not silently
ship — the v0.2 release checklist should carry the 12 unimplemented+port rows.

### 3.10 SRF (20) → pinned-state SQL differential

17/20 already reached. `pg_timezone_abbrevs_*` residue folds into the pinned-TZ
corpus of §3.5.

---

## 4. Risk tiers (risk × cost, what gets built first)

**Tier 1 — silent wrong answers or data corruption if wrong (test to depth):**
- RI_FKey_\* (13): referential integrity; failure = orphaned/wrongly-cascaded rows,
  detectable only after the fact. Cost of coverage: medium (simharness vocabulary).
- Type I/O round-trips (float4/8 in/out, numeric, datetime/interval I/O — the wall
  class core, ~120 rows): failure = silent value drift on dump/restore/COPY/wire.
  Cost: **low** (native-diff runner reuses existing C + shims).
- recv/send binary wire (~70 rows across classes): silent corruption on
  COPY BINARY / binary protocol; effectively untested today. Cost: low-medium.
- Array/range/record mutation ops (array_set/replace/remove, range ops, ~60 rows):
  wrong stored values. Mostly regress-reached; depth via simharness columns.
- Aggregate transition machinery incl. parallel combine/serialize (31): silent
  wrong aggregates, the parallel paths currently conditional-at-best. Cost: low.
- Sequence/nextval-adjacent state fns: already the subject of
  `proofs/state-seam-probe` (seam-proof technique demonstrated); keep on the proof
  track, not the test track.

**Tier 2 — visible wrong answers, no corruption:**
LIKE/regex/tsearch/jsonpath results, geometry distances, `to_char` images, hash
functions (wrong hash = wrong join/agg results — arguably T1, but regress+e2e reach
is already dense), selectivity NaN/range violations.

**Tier 3 — cosmetic / monitoring / plan-shape:**
`pg_stat_*` values, `pg_get_*` deparse text drift, error message text, selectivity
accuracy, `timeofday` formatting. Invariant-only; never worth per-function
differential depth. 143 rows marked `invariant-only` in the TSV.

Rule of thumb the tiers encode: **anything that writes, round-trips, or feeds an
aggregate gets input-space depth; anything that only reports gets shape checks.**

---

## 5. Recommended next three pieces of work

### Work item 1 (start immediately): shared native-diff fuzz runner for the `wall` class

Sized: ~2–3 agent-days for the runner + first 10 families; then ~30 min/family.

Precise spec:
1. New crate `proofs/native-diff/` (own workspace member alongside proof crates)
   containing the shared driver: xorshift64\* input streams (copy from
   `proofs/datetime-b/src/bin/native_diff_datetime_b.rs`), per-family case budget
   (default 10M), byte-exact Datum/varlena image comparison, first-divergence
   reporting with replayable seed, and a `--json` verdict line
   (`NATIVE-DIFF|<family>|<fn>|PASS|<n>` / `...|FAIL|<seed>|<case>`).
2. For each wall-class family lacking one, add the 6-line native `build.rs`
   (pattern: `cc` on `c/pg_*.c`, `-fwrapv`, skip under `CARGO_CFG_KANI`;
   datetime-b's `-ffp-contract=off` flag is mandatory — documented 1,779/10.7M
   fma-fusion divergence without it). 42 families need this; wall-class families
   first: float I/O (float4in/float8in/float8out incl. Ryu images), numeric
   transcendentals (sqrt/exp/ln/log/power), datetime I/O + to_char/to_date +
   timestamp_trunc/age, geometry distance/closest-point, json builders, md5/sha.
   The linking gotcha is documented in `native_diff_datetime_b.rs`: the driver must
   `use proof_<family> as _;` to keep the C archive in the link.
3. Wire a `native-diff` tier into `proofs/run-suite.sh` (it currently runs Kani
   only) so the runner is one command, and add it to the release-gate tier list so
   it actually runs (gate-blindness law — a runner without a caller is §1.8 again).
4. Acceptance: ≥10M cases per wall family with zero divergence, or a filed
   divergence with replay seed; ledger note per family. Input generators must cover
   the strtod hard cases (subnormals, 17-digit round-trips, exponent edges, Ryu
   shortest-image boundaries) via boundary grids in addition to random streams —
   grid+random is the established pattern.

This converts the 194 wall rows from "excluded, shallow regress reach" to
"differentially tested at 10^7 scale against the identical vendored C the proofs
use" — the strongest statement available short of a proof, phrased in the same
C≡Rust frame as the proof program.

### Work item 2: FK/trigger vocabulary for simharness (Tier-1 RI hole)

Sized: ~1 week. Add to `tools/simharness/src/gen/`: a second table with
`REFERENCES parent(id)` (per-run draw over ON DELETE/UPDATE action ∈ {NO ACTION,
RESTRICT, CASCADE, SET NULL, SET DEFAULT}), generator weights for
parent-delete/parent-update/child-insert ops, and a ledger model extension that
predicts post-action child multisets. Then: (a) run under the existing differential
oracle (C decides correctness), (b) compose with `sim-fault` crash cuts so RI
enforcement survives recovery, (c) let the multi-session oracle drive concurrent
parent-delete vs child-insert (the EPQ-adjacent race that regress cannot reach).
Also owed: first unit tests on `ri_triggers` (currently zero). This is the only
work item that touches actively-developed harness code; coordinate with the DST
lane owner.

### Work item 3: binary-wire differential (recv/send) + parallel-agg leg + fail-closed fixes

Sized: ~3 days, three small deliverables bundled because each is a script, not a
system:
- **3a** `copy-binary-oracle-e2e.sh`: for every type with regress presence, round-trip
  a value corpus through `COPY ... TO ... (FORMAT binary)` → `COPY ... FROM` on both
  engines and byte-compare both the binary files and the reloaded table contents;
  plus cross-feed (pgrust's binary file loaded into C and vice versa). Closes the
  weakest column in §2 (~70 recv/send rows) and finally builds the COPY fuzz surface
  the survey flagged as unoccupied.
- **3b** forced-parallel aggregate corpus (§3.8) — flips the 15 `parallel-only` rows
  to exercised.
- **3c** fail-closed fixes: the 7 crate differential tests must fail (not skip) when
  PG is unreachable unless `PGRUST_DIFF_ALLOW_SKIP=1`; and the fuzz workspace's
  stable smoke tests should be invoked from a gate script so the 2026-07-08 corpus
  regressions stay live. Both are direct gate-blindness-law remediations.

After these three: the wall class is depth-tested, the worst Tier-1 hole (RI under
concurrency/crash) is closing, binary wire goes from untested to differential, and
the two fail-open mechanisms are fail-closed. The remaining large block — the ~280
state-class admin/introspection functions — is Tier-3-dominated and follows as the
pinned-state corpus (§3.2a) plus invariant battery (§3.2b) at leisure.

---

## 6. Ledger annotation: deferred, sidecar shipped instead

The request was to add a `test_strategy` annotation to the excluded rows of
`proofs/USER_FACING_FUNCTIONS.tsv` if it could be done mechanically and safely.
It cannot be done safely **right now**: 16 active `.wt-prf-*` worktrees are editing
that file (the triage lane's copy changed 3 hours before this was written), and a
1,538-row mechanical rewrite would conflict with every in-flight lane commit.

Instead this change ships `proofs/EXCLUDED_COVERAGE.tsv` — one row per excluded
function: `name, class, exercised_today (evidence route), test_strategy` — which is
join-able on `name` and carries the full §2/§3 assignment. Follow-up (proposed for
the next proof-lane quiesce): fold the `test_strategy` token into the ledger's notes
column via a one-shot script that joins on (name, reason) and refuses on any row
whose status has changed since this snapshot, then delete the sidecar.

## 7. README honesty note

`README.md` currently leads with the proof program as the correctness story. Given
48% of the user-facing catalog is excluded, the public claim should be phrased as
the three-legged story it actually is (proofs for the provable ~half; byte-exact
pg_regress differential vs real PG 18.3 for the SQL-reachable surface — 73% of the
excluded set already sits under it; targeted differential fuzz/scenario/invariant
testing for the rest), and the Antithesis sentence should stay future-tense until
an engagement actually runs.
