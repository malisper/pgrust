# adt/formatting done-gate remediation (p1-lanek-remediation, proofs/gr-fmt)

Remediation of gate-audit CRITICAL findings 5+6 for adt/formatting
(`docs/verification/phase1-gate-audit-2026-07-31.md`): the crate was marked
done with 218 "corpus-gap (REACHABLE, not excepted)" lines neither measured
nor excepted, and the six docker-ground-truthed divergence fixes lived only on
the lane branch.

## Basis

Capture sha **e55e06b53d** on proofs/gr-fmt (origin/main bd0ceab3fa + the six
p1-lanek divergence fixes). The formatting crate sources are BIT-IDENTICAL to
the lane capture sha ee28d1fdb446, so all lane-era line numbers carry over
unchanged. The fixes now on this branch (finding 5 CLOSED at this basis):
dch_entry cc*100 wrap, dch DCH_W mday wrap, dch DCH_WW yday wrap, num_entry
float4/8 V-multiplier, calendar date2j '+day' wrap, calendar j2date
full-unsigned wrap (the calendar pair is in adt_datetime, taken verbatim from
the lane branch).

## Coverage of record (local full-corpus export, per the cov-resweep ruling)

`fuzz/cov-export.sh fmt_dch_diff` + `fuzz/cov-export.sh fmt_num_diff` over the
full committed corpora (fuzz/corpus/fmt_dch_diff 40.8k files incl. 2,028
`gr-*` + 1 `gr2-*` witness seeds; fmt_num_diff 6.5k incl. 2,655 `gr-*` + 1
`gr2-*`), toolchain nightly-2026-07-17, banked here as
`fuzz-fmtfam-e55e06b53d.lcov.gz` (both targets' lcovs concatenated).
Join: `proofs/coverage/merge-coverage.py --sloc-rule v2 --exclude-const-tables
--line-table-lcov <both> --auto-exceptions` (summary.json + files/ +
auto-exceptions.tsv in this dir). The kani leg is intentionally empty — this
is the lane's ratified fuzz-first route; the census mechanism applies to kani
harnesses, hence `--no-census-required` (nothing kani-shaped can silently
fail into "uncovered" here, but the summary carries the stamp regardless).

## Gate equation (v2-SLOC, join denominator incl. 45 line-table-reinstated)

    3445 = 3164 measured (fuzz, local full-corpus)
         +  107 excepted   (58 hand ledger rows, lane tag p1-lanek-remediation,
                            in proofs/coverage/phase1-exceptions.tsv
                            + 49 mechanical rows, auto-exceptions.tsv here)
         +  174 RECORDED residual (residual-lanek-remediated.tsv, per-line
                            reasons; classes: 85 soft-error-shape,
                            35 defensive-dead, 34 env-pin-shadow,
                            16 const-registry, 2 harness-shape, 2 seam-glue)

    uncovered − excepted − recorded = 0.  Reproduce with equation-check.py
    (inputs: the join outdir + hunt-verdicts.tsv + soft-list.txt).

Prior books, same basis family: lane recorded 3075/3400 with 218 corpus-gap
uncovered-and-unexcepted; cov-resweep local replay measured 3160/3445. This
remediation measures 3164/3445: +2 from the deterministic witness battery
(fuzz/gen_seeds_gr_fmt.py: dch.rs 350 b.c.-lowercase arm, dch.rs 777 interval
RM month=-12 arm) and +2 from targeted hunt seeds (gr2-strtol-neg-erange →
fromchar.rs 297; gr2-v-multi-numeric-overflow → num_entry.rs 151). Every other
former "corpus-gap" line was adjudicated per line (hunt-verdicts.tsv): the
218-line class dissolves into soft-error arms (harness and C oracle both run
the hard-error plane), env-pinned arms (GMT/C-locale/empty zoneabbrevtbl),
structurally dead C-parity guards, dead pub API, const-eval lines, and
instrument-unmappable executed lines — none was left silently uncovered.

## The 5 dch.rs regression lines (731/749/759/777/809)

- 731/749/759 (`} % 1000/100/10` ISO-year expression tails): EXECUTED —
  flanking lines run 8k-149k times in this capture; the instrument emits no
  DA record for the shape. Filed as instrument-unmappable ledger rows with
  per-row flanking-count evidence. The "regression" was an instrument-mapping
  difference between the gate-time capture and the local toolchain, not lost
  corpus reach.
- 777 (interval RM `MONTHS_PER_YEAR-1` arm): WITNESSED — seed
  gr-iv-monneg13-f08 (interval month=-13, picture RM); DA=2.
- 809 (`_ => {}` after DCH_FX): genuinely unreachable — every DCH keyword id
  has an explicit match arm; catch-all required by match exhaustiveness.
  unreachable-arm ledger row.

## Bug found and fixed during remediation

Witness seed gr-iv-imaxdy-f19 (interval day=INT_MAX, picture `J`) panicked
debug-only in adt_datetime date2j (`+ day` overflow) — the exact defect the
lane's divergence fix #2 addresses; landing the lane's calendar.rs (this
branch) fixes it. Replays of both full seed batteries are clean, 0 divergence.
