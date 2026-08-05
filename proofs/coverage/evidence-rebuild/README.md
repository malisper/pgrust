# evidence-rebuild — measured reconstruction of unverifiable coverage-of-record (2026-07-31)

CHARTER: the gate audit (`docs/verification/phase1-gate-audit-2026-07-31.md`, branch
`docs/p1-gate-audit`) and the cov-resweep lane (`proofs/cov-resweep` @ a594887353)
converged on done crates whose coverage-of-record could not be verified from any banked
artifact. A sibling lane relocates evidence that EXISTS; this lane REBUILT the cases where
it doesn't, by measurement: local full-corpus `fuzz/cov-export.sh` replay of the committed
corpus, joined with `merge-coverage.py --sloc-rule v2 --exclude-const-tables` (the
cov-resweep recipe). Nothing here was adjusted toward a recorded number; below-recorded
deltas are reported as findings.

Rule of record for the census-close re-runs: all five DO-NOT-PUBLISH captures were
FUZZ-ONLY joins (`sources.kani.raw_files == 0`) that took the `--no-census-required`
escape hatch because the fail-closed census machinery is kani-harness-oriented and the
lanes had no kani leg IN THE CAPTURE. The correct closure is an explicit EMPTY census
(`empty-census.tsv` herein: zero expected kani harnesses — a true, checkable statement for
a fuzz-only join), which closes with expected=0/ran=0/failed=0 and stamps
`census_closed: true`. The empty census asserts nothing about any kani evidence a lane
cites elsewhere; it closes THIS capture only.

Source-identity discipline: for every crate measured, the crate sources were verified
bit-identical (git diff empty) between the tree measured at and the lane's gate tip.
Where a lane's crate differs from origin/main (unlanded fixes), measurement was done AT
THE LANE TIP and is flagged below.

## 1. Rebuilt lanes (no lcov statement existed anywhere)

### p1-lanep @ 3252ebe854 — adt/scalar, adt/xid8funcs
Replayed committed corpora `scalarxid_diff` (2174 entries) + `snapio_diff` (2173) in
`.wt-p1-lanep` (lane tip; adt/scalar == origin/main, adt/xid8funcs DIFFERS from main —
the strtou64 ERANGE fix is unlanded, so numbers hold at the lane tip only).

| crate | recorded | REBUILT | verdict |
|---|---|---|---|
| adt/scalar | 470/743 | **470/743** | EXACT |
| adt/xid8funcs | 205 measured / 343 amended denom | **205/343** | EXACT |

Join: `lanep/summary.json` (census_closed=true, empty census — fuzz-only capture).
Union inputs: `fuzz-scalarxid_diff-local-20260731.lcov.gz` + `fuzz-snapio_diff-local-20260731.lcov.gz`
(both trimmed to adt/scalar + adt/xid8funcs SF blocks). No `--line-table-lcov` (matches
the recorded cut exactly without it).

### p1-laneae @ 16eda6ece7 — adt/tsvector_core, adt/tsrank
Replayed committed corpora `tsvector_core_diff` (11426 entries) + `tsrank_diff` (5907) in
`.wt-p1-laneae` (lane tip; BOTH crates differ from origin/main — the lane's bug fixes are
unlanded; numbers hold at the lane tip only). The lane recorded only a JOINT 1802/2022
figure; per-crate numerators did not exist anywhere. They now do:

| crate | recorded | REBUILT | verdict |
|---|---|---|---|
| joint | 1802/2022 | **1802/2022** | EXACT |
| adt/tsvector_core | (none) | **1282/1492** | derived; 1282 + 210 branch exception lines = 1492 (residual 0) |
| adt/tsrank | (none) | **520/530** | derived; 520 + 10 branch exception lines = 530 (residual 0) |

Join: `laneae/summary.json` (census_closed=true). Union inputs:
`fuzz-tsvector_core_diff-local-20260731.lcov.gz` + `fuzz-tsrank_diff-local-20260731.lcov.gz`.
The claims row's "1392 SLOC in-scope" for tsvector_core is an in-scope carve cut; the
whole-crate v2 accounting above balances exactly with the lane's exception rows.

### p1-laneag @ 1649a104b6 — adt/like (+ the 367-vs-368 off-by-one)
Replayed committed corpus `like_diff` (2705 entries) in `.wt-p1-laneag` (adt/like is
bit-identical to origin/main).

| cut | recorded | REBUILT |
|---|---|---|
| gate cut (no line table) | 278/367 | **278/367** EXACT |
| with `--line-table-lcov` | — | 279/368 |

**Off-by-one RESOLVED**: the single line is `builtins.rs:96`
(`Ok(Datum::from_bool(crate::$core(` — a macro_rules! body line). The plain v2 cut
excludes it; supplying the capture lcov as an instrument line table reinstates it (a DA
record exists), giving 368 — and the line is fuzz-COVERED under that cut. So 368 = the
original ranking cut (commit 85ce2d0a05, pre-re-baseline) and 367 = the gate cut and the
current ranking row (re-baselined at e25331c7e9). The discrepancy is denominator-cut-only:
uncovered-minus-excepted is 0 on both cuts. No claim needs revisiting.

Join: `laneag/summary.json` (census_closed=true). Union input:
`fuzz-like_diff-local-20260731.lcov.gz`.

## 2. census-close/ — the five DO-NOT-PUBLISH gate captures re-run properly

Enumerated from the gate audit (finding 12): **laneh, laney, lanek, lanem, laneo** — each
gate rested on a summary.json self-labelled `census_closed:false` / "DO NOT PUBLISH"
(`--no-census-required`). All five re-run with the empty census (see rule of record
above); every rebuilt summary herein stamps `census_closed: true`.

lcov provenance: lanem/lanep/laneae/laneag lcovs are fresh replays by this lane;
laneh/laney lcovs are the cov-resweep sweep's local full-corpus exports still on
`.wt-covresweep` disk (mtime 2026-07-31 14:24, tree = origin/main eff70bb262, crate
sources verified bit-identical to the lane tips); lanek's are the resweep exports at the
lanek tip on `.wt-p1-lanek` disk; laneo's are the resweep exports at the laneo tip on
`.wt-p1-laneo` disk. All are now trimmed+banked here (they were previously disk-only or
branch-only).

| lane | crate | recorded | REBUILT (census closed) | verdict |
|---|---|---|---|---|
| laneh | common/pg_prng | 67/75 | **67/75** | EXACT |
| laneh | adt/arrayutils | 95/102 | **95/102** | EXACT |
| laneh | common/hashfn | 155/156 (summary basis 164 = documented reinstatement) | **163/164** (line-table basis) | matches the documented reinstated basis; same-basis-exact per cov-resweep |
| laney | adt/adt_timestamp | 2107/3138 | **2108/3140** | +2 denom = test-scope-fix reveal (both covered); **REGRESSION: lib.rs:1034 recorded-covered, corpus-UNREPRODUCIBLE** (confirmed uncovered in this rebuild; fleet corpus input never banked; witness seed owed) |
| lanek | adt/formatting | 3075/3400 | **3160/3445** (lane tip 4277103748) | recount HIGHER (+85 net); **REGRESSION: dch.rs 731,749,759,777,809 recorded-covered, corpus-UNREPRODUCIBLE** (confirmed uncovered in this rebuild; gate-time local lcovs lost) |
| lanem | adt/varchar | 429/492 | **429/492** (gate cut; 430/493 with line table) | EXACT — the LOST `fuzz/coverage/varchar_diff.lcov` of record was rebuilt by fresh replay (varchar_diff corpus, 2644 entries) |
| laneo | adt/encode | 287/315 | **287/315** | EXACT |
| laneo | common/pglz | 208/231 | **208/231** | EXACT |
| laneo | common/sha2 | 213/220 | **213/220** | EXACT |

Joins with `--line-table-lcov` = the capture lcovs themselves (the lane recipe): laneh,
laney, lanek, laneo; lanem banked at the line-table basis with the gate cut stated above
(both reproduce). Per-lane `summary.json` + trimmed lcov.gz under `census-close/<lane>/`.

### Why the census could close for all five
None of the five captures merged any kani artifact — there was never an open kani-harness
disposition to account for. The DO-NOT-PUBLISH stamp was an artifact of the tooling's
(correct) refusal to default-close, not of any unaccounted harness. The durable fix for
future fuzz-only gates: pass an explicit empty census instead of `--no-census-required`.

## Below-recorded deltas (findings, not adjustments)
1. adt/adt_timestamp lib.rs:1034 — recorded-covered, unreproducible from the committed
   corpus (fleet lcov lost). uncov-exc for the crate worsens by 1 until a witness seed is
   banked. (Corroborates cov-resweep; now confirmed by an independent join.)
2. adt/formatting dch.rs x5 (731,749,759,777,809) — same class (gate-time local lcovs
   lost). The crate's `done` carried these on lost evidence; net number still moves UP
   (+85), but these 5 specific lines need witness seeds or exception rows.
3. No other crate in this lane's scope measured below its recorded number.

## Sources-drift flags
- adt/xid8funcs, adt/tsvector_core, adt/tsrank: measured at lane tips because the lane
  fixes are NOT on origin/main (gate-audit CRITICAL findings 3/4 remain open — the
  rebuilt numbers certify the lane-tip trees, not main).
- All other crates measured on trees bit-identical to both the lane tip and origin/main.
