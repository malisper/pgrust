# RULING OF RECORD: dark-harness promotion (Michael, 2026-07-31, via coordinator)

The dark-harness promotion package — **330 harnesses <=60s
(`promotion-staging-le60s.tsv`) + 155 harnesses >60s
(`promotion-staging-gt60s.tsv`)**, assembled by `build-promotion-staging.sh`
from the 724-row measure sweep (PLAN.md) plus the recipe-fix re-sweep
(job pgrust-kani-suite-1785478585-4690-71334) — is **APPROVED for promotion**
to `expected=green`, with ONE carve:

- `aclcheck/proofs::spot_aclitemout_numeric` is **HELD OUT** until its owed
  rig reduction lands (kernel-extract or domain split).

## Rationale of record

The 7 owed fleet CONFIRM legs (branch `proofs/sweep-confirm-legs` @
619c4e7bdb3b6eca7240cf9e193235334688ef25; kani-suite jobs
pgrust-kani-suite-1785526030-3461-77428 and
pgrust-kani-suite-1785530890-10d9-92878; artifacts archived at
`~/pgrust-fleet-archive/kani-suite-e4f99485c89d/`) came back:

- **4 legs GREEN (10 harnesses)** — appended to the staging files as fresh
  candidates (8 -> le60s, 2 -> gt60s) and promoted with the package.
- **2 legs walls-exactly-as-triaged** (vector-io ovin + aclcheck
  `eq_aclitemout_named`): zero failed checks, the pre-fix FAILED verdicts
  cleared; equivalence coverage of record is carried by the 269M-check
  exhaustive native diff (vector-io, `tests/exhaustive_ovin.rs`) and the
  spot harnesses (aclitemout) respectively.
- **1 unexpected wall** (`spot_aclitemout_numeric`): symex wall even at
  40GB / 2400s / conc 1 (peak RSS 9.8 GB), solver never reached, zero
  failed checks — a solver-capacity fact, not a verification gap. But its
  green expectation was runqueue triage only, never measured, so it does
  not promote. HELD OUT; rig reduction owed (kernel-extract or domain
  split). It joins the aclitemout output-image wall class (CNF-width law).

## Execution (this lane, proofs/dark-promotion)

Tier mapping is the one staged by `build-promotion-staging.sh` and
adjudicated as such: `<=60s -> per-commit`, `>60s -> release-gate`. The
recorded `time_s` is the fleet measure-sweep wall — an UPPER bound
(17-way co-tenant shard + shared family build), not a solo solve time.

Accounting at execution (staging files as merged from
`proofs/sweep-confirm-legs`, i.e. package + the 10 CONFIRM greens):

| staging file | rows | promoted here | already green |
|---|---|---|---|
| promotion-staging-le60s.tsv (-> per-commit) | 338 | 336 | 2 |
| promotion-staging-gt60s.tsv (-> release-gate) | 157 | 154 | 3 |
| total | 495 | 490 | 5 |

The 5 "already green" are the cash MIN/-1 ratification lane's rows
(`eq_cash_div_int{2,4,8}_band16`, `eq_cash_div_int8_by_{neg1,zero}`),
promoted earlier with locally measured solo times; those rows keep the
ratification lane's record (the newer, richer measurement).

Held-out row annotation lives on the `aclcheck/proofs::spot_aclitemout_numeric`
row in `SUITE.tsv`, citing this ruling and both job IDs.
