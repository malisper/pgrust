# regex_diff campaign of record r3 (task #69, lane final/regexcore-closeout)

## IN FLIGHT (fetch-only — NEVER resubmit while live)

    job:  pgrust-fuzz-campaign-1785782326-2a1d-43026
    sha:  78451eae07bd63a48c07039ec9d86fdecb99df53 (final/regexcore-closeout,
          harness port commit; the carve fix 073bdd7f7be came after submit
          and is r4-confirm material)
    args: --targets regex_diff --secs 7200 --fork 15 --deadline 21600
          dict=auto (runner resolved fuzz/regex_diff.dict, 69 lines)
          value_profile=0
    runner: fabled .wt-t90-fixes b485c3ba0 (#68/#89/#90/#109 fixed chain)
    S3:   s3://pgrust-fleet-results-149051628381/fuzz-campaign/78451eae07bd63a48c07039ec9d86fdecb99df53/pgrust-fuzz-campaign-1785782326-2a1d-43026/
    submitted: 2026-08-03 ~11:37 PDT; pod Running, lld build rc=0 (118s),
          corpus_in=5017 (S3 bank carries r2's fuzz delta).

A first submit without the [LONG-JOB] token (-1785782293) was deleted
seconds after creation and resubmitted as the job above — cap-audit
honesty, the r2 lane's own precedent.

## r2 adjudication (job -1785600521 @2aa2a97665e, artifacts banked in
## fleet-r2-pgrust-fuzz-campaign-1785600521/)

r2 stats: 3,922,036 execs, corpus 3652->5017, divergences=70,
sanitizer_artifacts=22, cov_lines=0, rc=1. Artifact census: 5 crash +
65 slow-unit (counted as the 70 "divergences") and 10 oom + 12 timeout
(the 22 sanitizer rows).

| class | count | verdict | evidence |
|---|---|---|---|
| ETOOBIG error-priority (crash-*) | 5 | ENVIRONMENTAL NON-SURFACE (harness carve hole, now closed) | All 5 reproduce at main tip 78451eae07b: Rust "regular expression is too complex" vs C "parentheses () not balanced" (4x) / "invalid escape \\ sequence" (1x). Attribution probe regex_core/tests/etoobig_error_priority.rs: at 30MiB Rust stack budget both engines report the SAME syntax error => the ETOOBIG is the stack guard firing mid-parse at the 2048kB server default (ratified stack-band asymmetry), NOT REG_MAX_COMPILE_SPACE accounting. Compile plane carved both-failed/one-ETOOBIG at 073bdd7f7be (exec/prefix planes already carved it); the 5 inputs banked as corpus seeds seed-r2-etoobig-priority-*. |
| compile-bomb slow-unit | 65 | INHERITED-UPSTREAM PERF CLASS (no comparator divergence) | Known banked class (both sides REG_ETOOBIG; C 22.8s vs Rust 40.2s uninstrumented — lane TRIAGE + timeout_unit_attribution probe). Spot-check at main tip: slow-unit-013a7c7a executes CLEAN in 47.5s, slow-unit-0976b337 in 10.1s (instrumented) — no panic, no divergence. |
| timeout | 12 | same compile-bomb family at the libFuzzer -timeout threshold | runner repro logs: replay runs until wall cap, no divergence (e.g. timeout-121728e8). timeout-deb6fe2e IS the lane's banked timeout artifact. |
| oom | 10 | FORK-MODE CUMULATIVE-RSS HEURISTIC ARTIFACT | runner's own single-unit repro replays run CLEAN (e.g. oom-03dfda3a: "Executed ... in 3158 ms", no OOM) — worker-cumulative RSS attributed to the last unit; per replay-rail notes these units are REG_MAX_COMPILE_SPACE-bounded (~480MB native). |

## r2 cov_lines=0 mechanism (diagnosed, evidence in
## fleet-r2-.../regex_diff/{coverage.log,capture-forensics.txt})

Coverage replay ran 16 corpus batches through the coverage-instrumented
binary; batches 12 and 15 wrote 0-BYTE profraw files ("empty raw profile
file") because the process died mid-batch (exit 71) before profile write,
and cargo-cov then failed the whole capture ("Failed to generate coverage
data") => cov_lines=0. The killer is the ETOOBIG error-priority class
above: the coverage build's larger frames flip near-threshold corpus
units into the driver panic that the fuzz build did not hit (same
environmental-guard mechanism). NOT the tzfam LSan class (#67), NOT
instrument-unmapped. The carve at 073bdd7f7be removes the abort class;
r4-confirm expected to capture coverage cleanly.

## r3 adjudication (task #69, 2026-08-03, lane fix/69-etoobig-precedence)

r3 stats (campaign-stats.json, fetched from S3): 2,486,543 execs, corpus
5017->5278 (+261), divergences=77, sanitizer_artifacts=34, rc=1.
Artifact census: 5 crash + 72 slow-unit (the 77 "divergences") and
oom+timeout rows (the 34 sanitizer rows) — slow-unit/oom/timeout are the
same inherited-upstream compile-bomb / fork-RSS classes adjudicated at r2
(spot checks unchanged). The ONE real divergence class:

| class | count | verdict | evidence |
|---|---|---|---|
| ETOOBIG error-priority (crash-*) | 5 | ENVIRONMENTAL STACK BAND (mechanism now MEASURED); carve REWORKED input-decidable | crash-{067339cc,4aba7b57,8ff0c3ef,956d58cd,acc8219c}: Rust "regular expression is too complex" vs C "parentheses () not balanced" (3x) / "invalid escape \\ sequence" (2x), all cflags=ADVANCED, panic at regex_diff.rs:250 (pre-carve driver). All 5 reproduce locally at the campaign harness (5/5 fire, macOS debug). MECHANISM (measured, not inferred): both guards sit at the identical site — C regc_nfa.c:1386 duptraverse / Rust regex_core regex_nfa.rs duptraverse (trip backtraces confirm duptraverse on every artifact) — byte-based at the same 2048kB budget; the Rust duptraverse frame is 96 bytes vs C's 48 (arm64 release disassembly), so Rust trips at ~half C's chained-duplication depth. Measured peaks on the 5 patterns: C 518-745kB vs Rust 1039-1492kB (2.00x throughout, release); the fleet build's instrumentation inflated Rust past the 2048kB budget (debug: 2447-3506kB needed). At any budget past the band both engines report the SAME syntax error (regex_core/tests/etoobig_error_priority.rs r3_* pins). C discovers EPAREN at regcomp.c:757 (EOS at parse end) and EESCAPE in regc_lex.c lexescape — both AFTER the duplication work, so no error-precedence product fix exists: C reports the syntax error only because its frames are smaller. |

CARVE REWORK (carve-discipline: input-decidable, never "the sides
disagree"): the r2-era compile-plane carve keyed on the verdict pair
(is_etoobig XOR). Replaced by stack_band_carve(pat, cflags) — a
pattern-only estimator of the deepest bounded-quantifier duplication
chain (dup_chain_estimate), gated to ERE/ARE grammar, floor 4000.
Calibration: the ten banked r2+r3 class seeds estimate 7,750-857,430;
99.2% of the corpus estimates <4,000; tripping at est 4,000 within
2048kB needs >524B/frame (11x the release frame, ~2x the worst observed
debug/instrumented inflation). Must-fail controls
(tests::stack_band_must_fail_controls): in-band wrong-message pairs,
out-of-band one-sided ETOOBIG, and BASIC/QUOTE-flag inputs all still
fail the plane. The 5 r3 inputs banked as corpus seeds
seed-r3-etoobig-priority-* (sha1 = artifact names).

SQL-reachability: band-limited conformance variance, ratified
non-surface. In-band patterns can produce different `invalid regular
expression: ...` DETAIL text (2201B either way) between a pgrust and a
PG server at the same max_stack_depth — but real PG exhibits the same
variance between its own gcc/clang/-O builds (the trip point is
environmental by construction; that is why max_stack_depth is a GUC).
On these exact 5 inputs an uninstrumented release pgrust agrees with C
18.3 byte-for-byte at 2048kB.
