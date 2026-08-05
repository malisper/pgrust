# tablesample_diff floor adjudication (p1-wavea, 2026-08-01)

Floor job: `pgrust-fuzz-campaign-1785633616-50de-51261` @ sha
`5377c71b3e56ed548589bbc8a86f51b02a774741` (branch proofs/p1-wavea-tsmpl),
c8g.4xlarge, FUZZ_FORK=14, activeDeadlineSeconds=10800 (verified on the
live spec).

Verdict: **PASS** — 10,223,199 execs (requested 10,000,000), outcome=run,
0 divergences, 0 sanitizer artifacts, 0 stray artifacts swept
(`stray_artifacts_swept: 0`, `unclassified_stray_artifacts: 0` — no
cross-target strays to adjudicate on this row), wall 141 s, cov_lines 473.

fetch-fuzz-results.sh printed `NOT CLEAN ... bad_rows=1`: FALSE POSITIVE of
the checker, not of the job — its BADROWS grep counts every
`"outcome": "..."` key in campaign-stats.json and this schema carries a
TOP-LEVEL `"outcome": "complete"` alongside the per-target rows; the only
target row is `"outcome": "run"`. divergences_total=0,
sanitizer_artifacts_total=0, crashed_early_total=0, rc=0.

Coverage equation (job lcov, crates/backend/access/tablesample/tablesample/
src/lib.rs): 211 DA lines total = 138 fuzz-measured + 73 exception rows
(21 excluded-state Tsm::get/from_symbol/not_a_tsm_routine syscache-seam
census carve; 51 planner-carve sample_scan_get_sample_size/extract_fraction/
clamp_row_est census carve; 1 defensive-c-parity Bernoulli NextSampleBlock
panic guard). All 73 unit-test witnessed (tests::tsm_get_carve_witness,
tests::sample_scan_get_sample_size_carve_witness,
tests::bernoulli_next_sample_block_panics). lib.rs unchanged since the
floor sha (last touched pre-lane at 3e1c1414cd).

Injection sweep at plane creation: 13/13 plants caught (12/12 on the first
pass; P2-sqlstate-drop re-run once after a patch-line-number fix in the
sweep script — the plane itself caught it immediately). No plane required
new directed seeds.

Detection-power note (documented limit): a round-vs-rint mutant of the
percent_cutoff tie (e.g. percent = 25*2^-31, product exactly 0.5) shifts
the cutoff by exactly 1 and is observable only via a hash_bytes preimage
of the cutoff value (~2^-32/exec); the sweep's cutoff plant (P5, /100 ->
/101) witnesses the plane at coarse grain instead.

Corpus: fleet snapshot 337 + local grown 332 merge-minimized to 156
coverage-preserving inputs (-merge=1), banked with the 98 directed
single-field-witness/boundary seeds re-added (254 total inc. .gitkeep
sibling handling; replay-clean via tests::seed_corpus_replays_clean).
