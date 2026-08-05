# INFRA DEFECT: the fleet mutants-audit RAIL never runs (Linux link failure)

Found by lane p1-laneaa, 2026-07-31, while triaging its two trailing
mutation audits. **Campaign-wide: every `mutants-audit` job's differential
replay rail is silently vacuous, so no fuzz-only crate's audit has ever
actually been swept by its corpus.** This is the gate-blindness class
(`.claude/skills` + memory `gate-blindness-law`): the job reports a number
that looks like an audit result but measures nothing.

## Evidence

Two jobs for `adt/jsonpath_exec` at sha `5954314b2f`:

| job | jobs | rail_max | result |
|---|---|---|---|
| pgrust-mutants-audit-1785521139-4ab3-38326 | 4 | 150 (default) | total 935, caught_in_crate 268, **missed 245**, `"rail": "unavailable"`, `rail_unswept: 245` |
| pgrust-mutants-audit-1785522900-78fb-47866 | 2 | **600** | identical: 935 / 268 / **245** / `"rail": "unavailable"` / `rail_unswept: 245` |

Raising `MUTANTS_RAIL_MAX` from 150 to 600 changed nothing — the cap was
NOT the cause (that was this lane's first hypothesis; it is refuted).

Root cause is in the job's rail BASELINE build (`rail-baseline.log` in the
fetched artifacts):

```
error: linking with `cc` failed: exit status: 1
/usr/bin/ld: .../libpg_difffuzz_hashenc.a(...pg_hashenc_glue.o): in function `pg_hashenc_b64_encode':
  undefined reference to `hashenc_impl_pg_b64_encode'
  undefined reference to `hashenc_impl_pg_b64_decode'
  undefined reference to `hashenc_impl_pg_b64_enc_len'
  undefined reference to `hashenc_impl_pg_b64_dec_len'
error: could not compile `decoder_fuzz` (lib test) due to 1 previous error
```

So `cargo test -p decoder_fuzz` — the rail's baseline, and the command the
rail uses to replay corpora against a mutant — **does not link on Linux at
all**. With no baseline, the rail marks itself `unavailable` and every
missed mutant is filed `rail_unswept`.

## Why it is invisible on the laptop

The link line orders the two hashenc archives
`-lpg_difffuzz_hashenc_fe … -lpg_difffuzz_hashenc`, i.e. the archive that
DEFINES the renamed `hashenc_impl_pg_b64_*` symbols (the `_fe` lib, built
from verbatim `src/common/base64.c` with the symbol-isolation `-D` renames,
`fuzz/core/build.rs:115`) comes BEFORE the glue archive that REFERENCES them
(`fuzz/core/build.rs:130`). GNU `ld` resolves static archives in a single
left-to-right pass, so a reference appearing after its definer's archive is
unresolvable; Apple's linker resolves regardless of order. The same
workspace therefore builds and tests clean on macOS and fails only on the
fleet — the exact "Linux-only failures invisible on macOS" trap already
recorded in the campaign's `FAILED is not a verdict` lesson.

Note this is PRE-EXISTING and NOT caused by adt/jsonpath[_exec]: the symbol
isolation and its ordering predate this lane (landed with the
hashenc/cryptofam oracles, `fuzz/core/build.rs` "SYMBOL ISOLATION" comment),
and nothing in either jsonpath oracle touches those symbols.

## Suggested fix (not applied here — cross-lane file, needs an owner)

Make the link order irrelevant rather than merely reordering it (the
workspace has several mutually-referencing oracle archives, and a pure
reorder will rot again):

- emit the archives so each is self-contained, or
- link the group with `-Wl,--start-group … -Wl,--end-group` on GNU ld, or
- repeat the definer archive after the referencing one, or
- simplest robust option: compile `pg_hashenc_glue.c` INTO the `_fe`
  archive so definition and reference share one archive.

Whatever the fix, it needs a **Linux** gate — the reason this survived is
that no `cargo test -p decoder_fuzz` runs on Linux in CI. The campaign's
own `cross-compile check locally` lesson applies:
`cargo check -p decoder_fuzz --target aarch64-unknown-linux-gnu` (and ideally
a link) catches it on the laptop.

## Two reporting defects found alongside it

1. `mutants-summary.json` contradicts itself: top level
   `"genuinely_missed_total": 245` vs the crate row
   `"genuinely_missed": 0`. A consumer reading either field alone draws the
   opposite conclusion. The honest rendering of this run is "245 missed,
   0 swept, rail broken".
2. `scripts/submit-mutants-audit.sh` (pgrust-fast side) documents neither
   the rail knobs (`MUTANTS_RAIL_SWEEP`, `MUTANTS_RAIL_DIR`,
   `MUTANTS_RAIL_TIMEOUT`, `MUTANTS_RAIL_MAX`, defaults 1 / `fuzz/core` /
   420 / 150 in the fabled submit script) nor the fact that an unavailable
   rail still exits rc=1 with a "NOT CLEAN" banner that looks like a
   survivor verdict. A rail that cannot run should FAIL LOUDLY as an infra
   error, distinct from "mutants survived".

## Status of this lane's audits

Both crates' campaign done-gates are closed on their own evidence (coverage
accounted 100%, 10M-exec fleet differentials green, exceptions recorded) —
the mutation audit is explicitly trailing and non-blocking per
`.claude/skills/fuzzuproof-crate/SKILL.md`. What is owed, once the rail
links on Linux, is the sweep of:

- `adt/jsonpath_exec`: 245 mutants unswept (of 935; 268 caught in-crate,
  419 unviable, 3 timeout) — jobs 1785521139 / 1785522900 @ 5954314b2f.
  Many are expected ARID-BY-CARVE (json_table.rs, `query_common` and the
  SRF wrappers, the session-TZ datetime family all carry `excluded-state`
  exception rows); the honest audit is the remainder on fuzz-measured lines.
- `adt/jsonpath`: no usable result yet — job 1785522356 (jobs=4) was
  **OOMKilled** (exit 137), job 1785522886 (jobs=2) hit BackoffLimitExceeded
  with no artifacts, job 1785524199 (jobs=1, c8g.8xlarge) was the third
  attempt. Even on success its rail would have been vacuous for the reason
  above.

---

# SECOND INFRA CAVEAT: a fuzz-campaign job's `coverage.lcov` is NOT a
# full-corpus capture — do not cite it as the crate's coverage number

Measured on this lane's own campaign `pgrust-fuzz-campaign-1785518461-61c1-18958`
(jsonpath_diff, 10,007,621 execs). Feeding the job's own
`coverage.lcov` to the standard merge gives **34.54%** for adt/jsonpath,
while the LOCAL capture (`fuzz/cov-export.sh jsonpath_diff`, which replays
the whole committed corpus under instrumentation, the recipe of record) gives
**88.38%** on the same sha and the same corpus. Per-file, on `gram.rs`
(identical 846 DA lines on both sides — so it is NOT a path-mapping or
SLOC-rule mismatch):

| capture | gram.rs DA lines hit |
|---|---|
| fleet `coverage.lcov` | 100 / 846 |
| local `cov-export.sh` | 829 / 846 |

The job's own `campaign-stats.json` separately reports `cov_lines: 2028`,
which is close to the local merged number (2001) — i.e. the job KNOWS the
real figure; it is the exported lcov that reflects only a small slice of
inputs. Whatever the mechanism (partial profraw merge, or the cov step
replaying only a subset), the consequence is what matters:

- **Sufficiency** claims (exec count, planes compared, zero divergences) come
  from the fleet campaign — solid, that is what the floor is about.
- **Coverage** numbers must come from the LOCAL full-corpus capture. A lane
  that merges the fleet lcov instead will under-report by ~2.5x and then
  "explain" the gap with exception rows for lines that are in fact covered —
  the exact failure mode the pre-share rendered-red audit exists to catch.

This lane's gate used the local capture for both crates. Worth a sweep of any
lane that cited a fleet `coverage.lcov`.

---

# STATUS UPDATE 2026-08-02 (fix/mutants-rail lane): RAIL REWORKED — red baseline can no longer yield a number

Two distinct vacuity mechanisms existed:

1. **Linux link failure** (the original finding above): FIXED earlier by
   p1-laneaj (build.rs "LINK-ORDER LAW": the hashenc glue archive now
   compiles/emits before the `_fe` archive).
2. **Whole-lib red baseline**: the rail ran `cargo test --release` over the
   ENTIRE shared decoder_fuzz lib, so any sibling lane's red test made every
   lane's audit vacuous — at main 70ead1ef2ebc, 5 sibling tests were red
   (fleet log: mutants-audit/7045a77259.../pgrust-mutants-audit-1785669167-16fa-85714/
   rail-baseline.log) and the runner's auto-carve + `missed=N` output let
   lanes record numbers over a baseline nobody had triaged.

## Rail rework (fabled fleet/fuzz-campaign-hardening @ 171e248752a6fc55a704a5bbea88eed1694fa746)

- `MUTANTS_RAIL_FILTER` / `--rail-filter`: scope the rail baseline AND sweep
  to the crate under audit's own fuzz/core suite(s) (e.g. `timestamp_diff::`).
  SET THIS in every lane's audit submit — it makes the audit immune to
  sibling-lane breakage. A filter matching ZERO tests is RED, never green.
- RED baseline (scoped or not) = **VOID audit**: distinct exit code **65**,
  a VOID mutants-summary.json with the red reason and NO missed/caught
  count of any kind. Auto-carve is REMOVED; explicit `--rail-skip` names
  are recorded in the summary. Self-test: fabled
  `fleet/jobs/test-mutants-rail.sh` (proves the red paths fire).
- `scripts/fetch-mutants-results.sh` refuses to conclude on
  `"rail": "red-baseline"` and on any unknown rail mode (fail-closed);
  self-test rows in `scripts/test-mutants-verdict.sh`.

## The five red tests at 70ead1ef2ebc (fleet baseline, linux-aarch64/gcc/release)

| test | verdict | disposition |
|---|---|---|
| nodesfam_diff x2 (unported census + rtekind seed gates) | HARNESS (stale census pins after the RTE_RESULT port 484033d90b9; port verified C-exact vs readfuncs.c:428) | FIXED this lane |
| diff::tests::dasind_fp_contraction_witness | HARNESS (absolute bit-pin minted on Apple libm; glibc asin/acos last-ulp differs — fleet log shows ...86 vs pinned ...82 with C never compared) | FIXED this lane: portable Rust==C compare + macOS-only pin |
| jsonpathexec_diff::tests::recursion_guard_probe | HARNESS (frame-size-dependent ladder: needs exec frames > parse frames, true in macOS debug, false in linux release — stack-guard-bounds-in-bytes law) | FIXED this lane: asymmetric budgets (parse 2048kB, exec 100kB floor) |
| timestamp_diff::tests::replay_committed_corpus | ORACLE-SIDE PROCESS-GLOBAL STATE POISONING (harness/oracle defect, NOT a pgrust bug). Failing unit NAMED: corpus/timestamp_diff/0ac33966e31f8312ab4677b996bfe51198523c85 (C dterr=2/22008 vs Rust 22007). The unit replays CLEAN with pristine C state everywhere: fleet sorted 25,171-unit libFuzzer sweep at main (job ...1af7-40058, 0 divergences), macOS whole-lib any order, timestamp-suite-alone. It fails ONLY in the whole-lib cargo test at the pod readdir order (ordinal 1034; reproduced at 3 shas incl. after driver entry-serialization). Attribution probe (job ...7afc-64115): same-thread retry FAILS and FRESH-THREAD retry FAILS = the sticky state is the C oracle TU\'s process-global statics (datecache/deltacache/tzabbrevcache class), not Rust thread-locals — Rust\'s 22007 is the correct verdict for this input under pristine C. Same corruption class as the flaky DecodeInterval SIGSEGV (FINDING-parallel-cargotest-oracle-segv.md); scribbler hunt routed to the ASan side-channel (#84) | banked witness stands; NOT a product finding |
