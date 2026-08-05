# SUITE dark-harness measure sweep (proofs/measure-sweep lane)

Sweep of the 724 `expected=unmeasured` SUITE.tsv rows at main sha
`33d7d09d31fad10a3009a7125acdb6f3246cbaa4` via the fleet kani-suite job's new
`--tier measure` mode (fabled branch `fleet/measure-sweep`, commit 25b1b1881 —
parity with `proofs/run-suite.sh measure`).

## Validation

- Local smoke (benchmark-smoke-first): `run-suite.sh measure` over a 5-row mini
  manifest — 3 unmeasured-green (int-cmp, wall 1-10s), 2 unmeasured-failed
  (json-escape: inferred flags lack `-Z stubbing` — harness recipe defect,
  reproduced locally; NOT a divergence).
- Fleet smoke: `--tier measure --rows 1-6` on c8g.2xlarge
  (job pgrust-kani-suite-1785473357-71bb-97642) — COMPLETE, 6/6 rows accounted,
  suite_rc=0, all artifacts (incl. suite-promotion-candidates.tsv) on S3.
  Outcomes: 4 unmeasured-timeout, 1 unmeasured-rss-kill, 1 unmeasured-failed
  (aclcheck eq_aclitemout_named: VERIFICATION FAILED).

## Full-sweep job IDs (submitted 2026-07-30, deadline 10800s each)

| shard rows | job |
|------------|-----|
| 1-121   | pgrust-kani-suite-1785474907-0cb7-22299 |
| 122-242 | pgrust-kani-suite-1785474912-0808-22420 |
| 243-363 | pgrust-kani-suite-1785474916-5cd7-22796 |
| 364-484 | pgrust-kani-suite-1785474920-78dc-23121 |
| 485-604 | pgrust-kani-suite-1785474924-5cfe-23356 |
| 605-724 | pgrust-kani-suite-1785474929-50fd-23546 |

## Shard layout (full sweep)

724 measure-selected rows in SUITE.tsv file order, 6 shards on c8g.16xlarge
(conc ~18, packed in-pod pool + node governor), unmeasured rows get the 600s
per-harness timeout, so one waller costs a slot for <=600s, never the shard:

| shard | rows    | families |
|-------|---------|----------|
| 1     | 1-121   | datetime-b aclcheck brin-minmax cash |
| 2     | 122-242 | cash datetime-b datetime-cmp float-agg |
| 3     | 243-363 | float-agg float-arith geo-cmp |
| 4     | 364-484 | geo-cmp gist-geo int-arith int-cmp json-escape jsonb-gin |
| 5     | 485-604 | jsonb-gin network numeric-arith numeric-probe oracle-compat pg_lsn scalar-misc strings-scalar text-slice |
| 6     | 605-724 | text-slice uuid vector-io xid8snap jsonb-probe |

Per-shard artifacts: s3://pgrust-fleet-results-149051628381/kani-suite/33d7d09d31fad10a3009a7125acdb6f3246cbaa4/<job>/
(suite-results.tsv + suite-promotion-candidates.tsv + logs.tar.gz).

Completeness floor: one result row per shard row; a shard with no output is
RED, not skipped. Promotion into SUITE.tsv tiers is NOT done here — the
promotion file + summary go to adjudication.

## RESULTS (sweep complete 2026-07-30)

Completeness floor PASSED: 724/724 result rows, key-diff clean; all 6 shards
suite_rc=0. Split: 379 unmeasured-green (promotion candidates,
suite-promotion-candidates.tsv) / 149 failed / 123 timeout / 73 rss-kill.

Green wall distribution (UPPER bound — includes shared family build +
17-way co-tenancy): <=10s: 90, 11-30s: 101, 31-60s: 53, 61-120s: 34,
>120s: 101. 244/379 green at <=60s.

Failure triage:
- 107 NO-VERDICT = recipe defects, dominated by missing `-Z stubbing` in
  dark-row inferred flags (cash 96, jsonb-probe 5, json-escape 3,
  scalar-misc 2, gist-geo 1). CONFIRMED LOCALLY: cash
  eq_cash_div_cash_by_zero reproduces the compile error with SUITE.tsv flags
  and verifies SUCCESSFUL in seconds with `-Z stubbing` added. Fix flags,
  re-sweep these rows.
- 42 VERIFICATION FAILED: 25 are control_/ctl_/neg_control_/rust_panics_/
  witness_ negative controls registered dark — FAILED is correct there
  (candidates for expected=must-fail).
- 17 divergence-suspect FAILED (eq_/spot_/probe_/cover_): vector-io oidvector
  cluster (7: eq_ovin_len0..4, eq_ovout_spots, cover_ovin_both_arms);
  datetime-b timetz part(tz) cluster (4: eq_timetz_part_tz{,_hour,_minute},
  spot_timetz_part_tz_time_nonzero) — HIGHEST-PRIORITY triage; float-arith
  eq_dsqrt + dsqrt_bisect_grid; float-agg eq_finals_nullity_t3/_t6 +
  probe_sqrt_self_determinism; aclcheck eq_aclitemout_named.
- 196 timeout/rss-kill residue concentrated in aclcheck (38 t/o), jsonb-gin
  (17+10), text-slice (18 rss), xid8snap (15 rss), oracle-compat, geo-cmp:
  need bigger budgets or decomposition; not promotable as-is.
