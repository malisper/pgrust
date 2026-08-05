# p1-lanel trailing mutation-testing audit

Mutation findings are OFF the campaign critical path: survivors are FINDINGS,
not failures. This file records what was actually adjudicated, by what
instrument, and what remains a genuine gap.

Crates: `crates/backend/utils/adt/adt_date`, `crates/backend/utils/adt/adt_datetime`.
cargo-mutants 27.1.0, per-mutant test timeout 300s.

## Jobs

| job | sha | crate | rail | note |
|---|---|---|---|---|
| `pgrust-mutants-audit-1785501658-7789-80648` | f377f4a929 | both | enabled | DIED on default 3600s `ACTIVE_DEADLINE`, no artifacts |
| `pgrust-mutants-audit-1785505383-10c4-78283` | f377f4a929 | adt_date | enabled, cap 150 | 79 rail-unswept |
| `pgrust-mutants-audit-1785505389-0a94-78978` | f377f4a929 | adt_datetime | enabled, cap 150 | 766 rail-unswept |
| `pgrust-mutants-audit-1785509201-09f5-20659` | 06592b9769 | adt_date | **unavailable** | rail baseline SIGSEGV — see defect 2 |
| `pgrust-mutants-audit-1785509215-0b59-25530` | 06592b9769 | adt_datetime | **unavailable** | rail baseline SIGSEGV — see defect 2 |
| `pgrust-mutants-audit-1785510743-0786-67890` | c6d123c6b0 | adt_date | enabled, cap 1200 | **full sweep, authoritative** |
| `pgrust-mutants-audit-1785510750-4a82-67974` | c6d123c6b0 | adt_datetime | enabled, cap 1200 | **full sweep, authoritative** |
| `pgrust-mutants-audit-1785516693-6dcb-707` | c6d123c6b0 | adt_datetime | enabled, cap 1200, rail-timeout 90s | hedge against a projected deadline miss; deleted once the sibling landed |

The rail is `cargo test --release` in `fuzz/core` — the differential-plane
seed-replay + non-vacuity test rails, NOT a full 10M corpus replay. A mutant
is `caught-by-rail` when a banked seed makes the Rust side disagree with the
verbatim-C oracle.

## Defect 1 — the 845 unswept mutants: a per-crate sweep cap

`MUTANTS_RAIL_MAX` defaults to **150 per crate**
(`fleet/jobs/run-mutants-audit.sh:56`). adt_date had 229 in-crate survivors
and adt_datetime 916, so 79 and 766 overflowed the cap and were emitted as
`rail-unswept(cap=150)` — recorded, loudly, but never adjudicated. The
arithmetic is exact: 229-150=79, 916-150=766.

Remedy is the one the script itself documents: raise the cap (or shard the
crate list). Re-run with `MUTANTS_RAIL_MAX=1200`.

**Why this mattered:** `genuinely_missed = 81` for adt_datetime sat next to
766 unadjudicated mutants. Reporting the 81 as the finding count would have
been the campaign's recurring gate-blindness shape — a clean-looking number
over an unmeasured population.

## Defect 2 — the rail baseline SIGSEGVs (LIVE, FIXED at `c6d123c6b0`)

Raising the cap exposed something worse. At `06592b9769` both jobs reported:

```
"rail": "unavailable", "rc": 1
"caught_by_rail": 0, "rail_unswept": 0, "genuinely_missed": 229 / 916
```

`rail_unswept: 0` reads like the cap problem was solved. It was not: the rail
never ran, so **every** survivor was recorded `genuinely-missed` by default.
A summary that adjudicates nothing while reporting zero unswept is strictly
worse than the honest `rail-unswept` token it replaced.

Cause, from `rail-baseline.log`:

```
process didn't exit successfully: decoder_fuzz-fc5804c1f13cecec
  (signal: 11, SIGSEGV: invalid memory reference)
```

The vendored `datetime.c` lookup caches in `fuzz/core/csrc/pg_datetime_io_io.c`
— `zoneabbrevtbl`, `datecache[]`, `deltacache[]`, `tzabbrevcache[]` — were
process-global. In PostgreSQL that is faithful (process-per-backend), but the
multi-threaded `cargo test` rails run several drivers in one process, and
`pg_dt_install_pinned_abbrevs` is *already* `_Thread_local` ("install once per
thread"), so every thread wrote the same shared cells.

The precise hazard is publish order, not a use-after-free
(`InstallTimeZoneAbbrevs` reassigns and memsets; it frees nothing):

- `DecodeTimezoneAbbrev`'s cache-hit arm matches on `tzc->abbrev`, then reads
  `tzc->tz` (`pg_datetime_verbatim.inc:2903-2909`).
- The fill publishes `abbrev` **before** `tz`
  (`pg_datetime_verbatim.inc:2962-2966`), and `InstallTimeZoneAbbrevs`
  memsets the whole cache.
- So a second thread can match a freshly-published name and read a stale or
  zeroed `pg_tz *`, which the DYNTZ path then dereferences.

This is the same publish-order shape as the `pg_dt_fcinfo_data` bug fixed
earlier in this lane, and it appeared for the same reason: the
`datetime_convert_diff` abbrev arms are the first rail tests to install an
abbrev table. It is **Linux-aarch64 only** — 6/6 clean locally on macOS, which
is the "Linux-only crashes invisible on macOS" trap again.

Fix (`c6d123c6b0`): `_Thread_local` on all four caches. Storage duration only;
no computation shimmed. Verified: rail baseline green on the fleet and the
sweep proceeds to adjudicate mutants.

Note `diff::tests::dasind_fp_contraction_witness` is red on the fleet host and
is carved by the runner (`rail_carved_tests`) — pre-existing, unrelated, and it
passes locally.

## Post-sweep numbers — what the cap was actually hiding

Both crates swept to completion at `c6d123c6b0`, `rail_unswept: 0`, rc=0.

| | capped (150) | **full sweep** |
|---|---|---|
| adt_date `caught_by_rail` | 8 | **29** |
| adt_date `genuinely_missed` | 142 | **200** |
| adt_date `rail_unswept` | 79 | **0** |
| adt_datetime `caught_by_rail` | 68 | **541** |
| adt_datetime `genuinely_missed` | 81 | **370** |
| adt_datetime `rail_unswept` | 766 | **0** |
| **total caught_by_rail** | 76 | **570** |
| **total genuinely_missed** | 223 | **570** |
| total `rail_timeout` | 0 | 5 |

The differential plane kills **570** of the 1145 in-crate survivors — 7.5× what
the capped run reported. The honest finding count is **570** genuinely-missed,
not 223. The cap understated both the plane's power and the size of the
residual, in opposite directions.

The 5 `rail_timeout`s are non-terminating mutants (e.g. `+=` → `*=` on a loop
counter in `DecodeTimezoneAbbrevPrefix`); they are bounded by
`MUTANTS_RAIL_TIMEOUT` and not gaps. At 420s each they dominated the tail and
projected an ~8.4h run against a 7h deadline, hence the hedge job; 90s is
ample (the rail's normal wall is ~15s) and is the better default for a
re-run.

## Reachability analysis of the 1145 in-crate survivors

Independent of the rail, each survivor's line was joined against the UNION
line coverage of all four floor-clean targets' 10M corpora
(`datetime_io_diff`, `interval_engine_diff`, `datetime_engine_diff` @
db1e7f827e; `datetime_convert_diff` @ d36bdb059b). If no corpus input reaches
a line, the rail *provably* cannot kill a mutant on it.

Under the capped jobs, **703 of the 845 unswept mutants sat on lines the corpus
actually reaches** — the population most likely to be killed was exactly what
the cap discarded, which the full sweep then confirmed (541 new kills in
adt_datetime alone).

Post-sweep, the 570 genuinely-missed break down by reachability and by the
function's `phase1-routes.tsv` status:

| routes status | COVERED | UNCOVERED | no counter | total |
|---|---|---|---|---|
| `fuzzed` | 145 | 28 | 9 | 182 |
| NO-ROW (internal helper) | 112 | 55 | 20 | 187 |
| `proved` | 19 | 86 | 0 | 105 |
| `planned` | 0 | 52 | 4 | 56 |
| `excepted` | 5 | 22 | 0 | 27 |
| `blocked` | 13 | 0 | 0 | 13 |

## Proof-covered survivors — verified, not asserted

Mutation testing runs `cargo test`, which does **not** execute Kani proofs. A
survivor inside a proved function is therefore an artifact of the instrument.
That claim is worthless unless demonstrated, so it was demonstrated: for three
distinct harness families a survivor was applied to the shipped source, the
harness re-run, and a control run performed unmutated.

| function | mutant | harness | mutated | control (unmutated) |
|---|---|---|---|---|
| `date_pli` (lib.rs:380) | `days >= 0 { result < date }` → `<=` | `misc-ops proofs::eq_date_pli` | **FAILED** `cerr != C_OK` | SUCCESSFUL |
| `date_cmp_internal` (lib.rs:348) | `d1 < d2` → `d1 > d2` | `datetime-cmp proofs::eq_date_cmp` | **FAILED** `r.as_i32() == c` | SUCCESSFUL |
| `fc_in_range_time_interval` (builtins.rs:1073) | `offset.time < 0` → `> 0` | `datetime-b rem::eq_in_range_time_interval` | **FAILED** (3 checks) | SUCCESSFUL, 11.3s |

All three are full-symbolic dual-execution against verbatim 18.3 C, and
`date_pli`/`date_mii` carry a must-fail control
(`control_date_pli_vs_c_mii`) proving the fallible rig is non-vacuous. The
`date_cmp_internal` mutant is the same one observed surviving the fuzz rail
locally — the proof kills what the rail cannot.

On that basis the following unreachable-line survivor groups are
**proof-covered** (routes status `proved`, harness verified to constrain the
mutated expression): `date_cmp_internal` (10), `fc_in_range_time_interval` (7),
`fc_in_range_timetz_interval` (7), `date_pli` (5), `date_mii` (5),
`date_cmp_timestamptz_internal` (5), `timetz_recv` (4), `time_mi_time` (3),
the 12 `fc_{date,time,timetz,interval}_{larger,smaller}` / `fc_overlaps_*`
comparison wrappers (3 each = 36), `fc_timetz_hash{,_extended}` (4),
`CheckDateTokenTables` (2), and singletons `datetimetz_timestamptz`,
`time_recv`, `fc_date_finite`, `fc_interval_finite`.

### Registry tension found in passing

`in_range_time_interval/4137` and `in_range_timetz_interval/4138` are `proved`
in `docs/verification/phase1-routes.tsv` and in
`proofs/USER_FACING_FUNCTIONS.tsv` (with walls 1.9s / 31.4s), but their
harnesses `rem::eq_in_range_time{,tz}_interval` are `unmeasured/unmeasured` in
`proofs/SUITE.tsv` with a `DARK-SWEEP 2026-07-30 registered unrun` note. I
measured `rem::eq_in_range_time_interval` directly: **VERIFICATION SUCCESSFUL,
11.3s**, so the ledger's `proved` is correct and the SUITE row is the stale
one. The `timetz` sibling was not run here and remains unmeasured in SUITE.

## Does the FULL corpus kill what the seed rail missed?

The rail is a seed-replay test set, not the 10M-evolved corpus, so a survivor on
a corpus-reached line might still be killed by a full replay. Two representative
`COVERED` survivors were checked with `proofs/coverage/mutkill.sh`, which
rebuilds the target and replays the whole committed corpus (`-runs=0`):

| mutant | full-corpus verdict | why |
|---|---|---|
| `decode.rs:1668` `dterr < 0` → `<= 0` in `DecodeTimeOnly` | SURVIVED | **provably equivalent**: `DecodeNumberField` returns only `DTK_DATE`(2), `DTK_TIME`(3) or a negative DTERR — never 0 — so the changed boundary is unreachable |
| `decode.rs:1555` `HOURS_PER_DAY / 2` → `* 2` in `DecodeDateTime` | SURVIVED | reachable and behaviour-changing (12 PM ⇒ hour 24), but **unobservable through any arm this lane drives**: `date_in` discards `tm_hour`, and the time arms go through `DecodeTimeOnly`. The observing entry point is `timestamp_in`, in the unclaimed `adt_timestamp` |

So for these the seed rail was not understating: the corpus genuinely cannot
discriminate them, for two different and principled reasons. That also means
`caught_by_rail = 570` is a **lower bound** on the plane's power, and the
residual is not uniformly "missing seeds".

## REAL GAPS

**1. `extract_date` / 6199 and `extract_time` / 6200 — 56 survivors, all on
corpus-UNCOVERED lines.**

These are the two rows this lane left honestly `planned`. No proof and no
driven fuzz arm discriminates them, so all 56 mutants are genuinely
unadjudicated by any instrument — they are not instrument artifacts.

What closes them: the oracle's `int64_to_numeric` / `int64_div_fast_to_numeric`
are abort-stubs, so a `datetime_convert_diff` arm needs the numeric.c closure
(~540 lines: `int64_to_numericvar`, `make_result_opt_error`,
`alloc_var`/`free_var`/`strip_var`, `get_str_from_var`, `round_var`,
`numeric_out`) plus a byte-copy of numeric.c's private `NumericVar` /
`NumericDigit`. `extract_time` is fully closable that way; `extract_date` also
needs `numeric_in` for the `±Infinity` literals in its `DATE_NOT_FINITE` arms,
or an honest carve of that arm.

Do these 56 reveal genuine behavioral latitude? Yes — they are the ordinary
comparison/arithmetic mutants of a NUMERIC-returning extract path with no
differential witness of any kind behind it. Nothing currently pins that
path's field selection or its rounding against C.

**2. `determine_time_zone_offset_internal` — 77 survivors (68 + 9 in its
`overflow` closure), all on corpus-COVERED lines, no routes row.**

Carve-limited rather than untested. `tz.rs` is inside the session-timezone
state seam: the oracle pins GMT, so the offset is always 0 and the
DST-transition search is degenerate. The corpus reaches these lines but cannot
discriminate arithmetic inside them. Closing this needs a non-GMT differential
posture (a real tzfile on both sides), which is exactly what the carve
currently excludes — so it is a **known limit of the pinned environment**, and
it should be recorded against the carve rather than silently counted as
"missed". It is the second-largest group in the whole audit and was entirely
invisible in the capped run.

**3. `fmt_sec_g02` — 13 survivors, `blocked` route.** Consistent with the
existing ruling: both sides defer to float formatting (Rust vs PG's
`snprintf`), which has no CBMC remedy and no exact differential plane. Already
recorded as `blocked`; the mutants confirm the route decision rather than
contradicting it.

**Not individually adjudicated:** the remaining `fuzzed`-status and NO-ROW
survivors (369 combined, minus the 77 tz and the groups above). The two
full-corpus probes above show this population contains at least provably
equivalent mutants and at least cross-crate-unobservable ones, but each
verdict there is per-mutant work and only 2 of them were actually settled.
They are reported as unadjudicated-at-mutant-granularity, not as clean.

## Const-context survivors (no runtime counter)

23 survivors carry no `function_name` and sit in const initializers:
`adt_datetime/consts.rs:97-99` (the `DTK_*_M` bitmask compositions; mutants
swap `|` for `^`/`&`), `consts.rs:106-112` (the `DTERR_*` error-code
constants), `consts.rs:132` (`TZDISP_LIMIT`), and `adt_date/lib.rs:115`
(`DATE_WORKBUF = MAXDATELEN + 1`). These are compile-time-evaluated, so they
have no coverage counter and the corpus-reachability instrument does not apply
to them; a mutation is observable only through whatever runtime code consumes
the constant. `lib.rs:115` is load-bearing and already has a differential
provenance note (the 129-vs-153 workbuf finding).
