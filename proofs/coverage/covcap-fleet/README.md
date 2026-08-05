# covcap-fleet — fleet coverage-capture root cause, fix, and validation (2026-08-01)

CHARTER: the fleet fuzz-campaign `coverage.lcov` was a PARTIAL capture
(adt/jsonpath 34.54% fleet vs 88.38% local on the same sha; gram.rs 100/846
vs 829/846), which forced the local-only coverage-of-record rule and the
cov-resweep lane. This lane root-caused the partial capture on the pod,
fixed it in the fleet runner, and validated the fixed capture line-for-line
against banked local lcovs before flipping the rule.

## Root cause (pod-side evidence, not inference)

**ASan `detect_stack_use_after_return` (fake stack) is default-ON in modern
LLVM on Linux and breaks PostgreSQL-style stack-depth probing.** The
harnesses arm `stack_depth` exactly like a backend thread
(`set_stack_base()` + 1536kB budget); under the fake stack, `&local`
addresses come from per-size-class fake-stack pools, so
`|stack_base - current|` is garbage. Measured on the instrumented probe
(job `pgrust-fuzz-campaign-1785562281-7d56-43703` @ e7f50992e356, same sha
as the proven-partial job `...-1785518461-61c1-18958`):

- `stack_is_too_deep` was called 17,095 times and `stack_depth_exceeded`
  raised 17,095 times — **100% of guard evaluations raised 54001**
  (lcov DA counts, `prefix-broken-jsonpath-capture-forensics.txt` + the
  probe's coverage.lcov in S3).
- Every input therefore took the harnesses' documented 54001 depth-carve
  (`depth_carved` returns silently), so on the pod the Rust deep plane was
  neither **covered** nor **compared** — the shallow lcov was an honest
  record of a silently-shallow replay. macOS ASan leaves the fake stack
  off, hence rich local captures from the identical corpus.
- Eliminated by the same probe (capture-forensics artifact): corpus
  completeness (12,214/12,214 units replayed; the fleet corpus is a strict
  SUPERSET of the committed corpus — 11,212 committed + 989 grown, 0
  missing), per-batch profraw loss (16/16 healthy 3.25MB profraws, 35,042
  function records each), merge loss (merged profdata = exact sum of the
  16), binary mismatch (single covbin candidate, sha256 recorded),
  llvm-cov export errors (stderr captured, empty).

NOTE the blast radius is bigger than coverage: the FUZZ phase ran under the
same broken guard, so fleet campaigns for stack-guarded targets were
exploring (and comparing) only the shallow plane. With the fix, the very
first 20k-exec probe surfaced a 27s slow-unit in deep jsonpath parsing.
Campaign owners of stack-guarded crates should weigh re-running their
sufficiency floors (coverage numbers of record are unaffected — those were
already local).

## Fix (pgrust-fabled `fleet/fuzz-campaign` @ a9f7920aba23)

1. `run-fuzz-campaign.sh` pins
   `ASAN_OPTIONS=detect_stack_use_after_return=0` for every phase (fuzz
   run, artifact repro, coverage replay).
2. Capture self-checks (gate-blindness law — a short capture must FAIL
   LOUDLY): the capture is INVALID unless (a) units replayed == corpus
   files on disk == cargo-fuzz's declared input count, (b) nonempty
   profraws == workers == clean libFuzzer exits, (c) optional
   `FUZZ_COV_FLOOR_LINES` floor (submit `--cov-floor`) against the
   target's recorded baseline. An invalid capture is renamed
   `coverage.INVALID.lcov`, cov_lines is zeroed, and the job exits RED.
3. Provenance artifacts uploaded per target: `capture-forensics.txt`
   (replay counts, per-batch profraw llvm-profdata summaries, covbin
   sha256), `coverage-raw-profraw.tar.gz`, `llvm-cov-export.err`.
4. The replay-completeness count is occurrence-based, not line-anchored:
   16 workers share one unbuffered stderr and interleaving merged an
   `Executed` line into another worker's banner (validation job
   `...-1785563695`, coverage.log line 6925) — the line-anchored check
   red-flagged a complete replay once; hardened in a9f7920aba23.

## Validation (fixed fleet capture vs banked local lcov, same sha+corpus)

- **Worst case, adt/jsonpath** (the crate that exposed the defect): fixed
  fleet job `...-1785563219-34f8-64436` @ e7f50992e356 vs the banked local
  lcov (`proofs/coverage/evidence-rebuild-2/laneaa/`):
  gram.rs 829/846, scan.rs 823/855, mutability 92/94, builtins 52/56,
  lib.rs 0/1 — **identical hit sets line-for-line**; path.rs 612 vs 613
  (one local-only line, path.rs:377, from a corpus-marginal input in the
  laneaa committed corpus vs the S3 bank; zero fleet-only lines).
  `validate-jsonpath-vs-local.txt`.
- **Worst case #2, adt/float** (cov-resweep's sharpest mover, 821→913):
  fixed fleet jobs `...-1785563695` + `...-1785564148` @ 560d89a3e1a1
  (float crate, drivers, and corpora bit-identical to the resweep basis
  eff70bb262; the resweep sha itself predates the 560d89a3e1 GNU-ld
  duplicate-symbol link fix and cannot build on Linux) vs
  `proofs/cov-resweep/fuzz-floatfam-local-20260731.lcov`: **identical
  hit sets line-for-line on every adt/float file** (aggregates 43,
  builtins 0, funcs 528, io 531, lib 87 distinct hit lines; zero deltas
  either direction). `validate-floatfam-vs-local.txt`.
- **Control, adt/adt_timestamp** (fleet was already complete pre-fix —
  proves the fix does not inflate): fixed fleet job
  `...-1785563379-4862-52724` @ bd970a8a291d vs
  `proofs/cov-resweep/fuzz-timestamp_diff-local-20260731.lcov`:
  builtins.rs 439/983, interval.rs 1203/1407, lib.rs 888/1225 —
  **identical hit sets line-for-line, zero deltas either way**
  (`validate-timestamp-control-vs-local.txt`).

## Rule change

`.claude/skills/fleet/SKILL.md` + `docs/verification/CAMPAIGN-INTELLIGENCE.md`
now permit the FIXED fleet capture as coverage of record **conditional on
the self-checks**: runner at/after pgrust-fabled `fleet/fuzz-campaign`
a9f7920aba23, job rc=0 with a `coverage.lcov` (never `coverage.INVALID.lcov`),
and the per-target `capture-forensics.txt` present. Local
`fuzz/cov-export.sh` remains valid and is the tiebreaker for any dispute.
