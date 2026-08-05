# p1-lanel rendered red-line audit

Numeric cross-checks are NECESSARY BUT NOT SUFFICIENT (Michael found 6
bogus-red classes by eye). Every uncovered line in the lane's 13 in-scope files
was rendered with source context and adjudicated. Merge audited:
`proofs/coverage/lanel/out` @ sha db1e7f827e, SLOC v2, 55 auto-exception rows.

Per-file residual after auto-exceptions (`REMAINING` = lines I hand-eyeballed):

| file | sloc | uncovered | auto | remaining |
|---|---|---|---|---|
| adt_date/builtins.rs | 703 | 633 | 34 | 599 |
| adt_date/lib.rs | 778 | 526 | 5 | 521 |
| adt_date/interval_corpus.rs | 9 | 9 | 9 | 0 |
| adt_datetime/decode.rs | 1785 | 177 | 1 | 176 |
| adt_datetime/tz.rs | 156 | 62 | 0 | 62 |
| adt_datetime/consts.rs | 24 | 17 | 0 | 17 |
| adt_datetime/tables.rs | 9 | 9 | 2 | 7 |
| adt_datetime/errors.rs | 54 | 8 | 1 | 7 |
| adt_datetime/calendar.rs | 87 | 3 | 3 | 0 |
| adt_datetime/encode.rs | 420 | 1 | 0 | 1 |

## BOGUS-RED the auto classes already catch (no action)

- `calendar.rs` 1/6/10 — `pub static DAY_TAB/MONTHS/DAYS` declaration heads,
  correctly classified `auto:table-head`. calendar.rs is honestly 84/84 of its
  adjudicable lines; the raw llvm-cov 103/103 and the v2 84/87 do NOT conflict.
- `interval_corpus.rs` 9/9 — fully auto-classified; the file is a data corpus.

## BOGUS-RED found by eye, NOT covered by a licensed class (16 lines)

These are structurally unmeasurable at runtime, so no fuzz target or proof
could ever turn them green. They are NOT honest signal, but I did not invent
classes for them — new auto classes need `rig-auto-classes.py` GREEN licensing
(AUTO-EXCEPTIONS.md). Handing to the campaign-accelerator owner.

1. **`encode.rs:105` — expression/cast continuation (1 line).** The line is
   `}) as u32`, closing the `if`-expression in `display_year`. It has **no DA
   record in any of the three lcovs**, while lines 101-104 and 106 of the same
   expression are covered (counts 24/42 io, 200/3489 engine). LLVM folds the
   cast into the surrounding region and emits no counter. Would need an
   `expr-cont` / `cast-cont` class; the existing `fmt-cont` veto logic does not
   reach it.
2. **`tables.rs` 5-12 — `const fn tk()` body (7 lines).** `tk()` is a `const
   fn` invoked from 133 sites, **every one of them inside a `pub static`
   initializer** (`DATETKTBL`, `DELTATKTBL`). It is evaluated entirely at
   compile time; there is no runtime path. `--exclude-const-tables` removes the
   table bodies but leaves the const constructor's definition in, so tables.rs
   reads a misleading 0/9.
3. **`consts.rs` 97-99, 203-207 (8 lines).** 97-99 are `pub const DTK_*_M =
   DTK_M(..) | ..` const-expression initializers; 203-207 are
   `const _: () = { assert!(size_of/offset_of ..) }` compile-time layout
   assertions. Both are compile-time-only. (Note: this is distinct from the
   coverage ruling that runtime `assert!`s stay IN and measured — these are
   const-context asserts, checked by the compiler, where a violation is a build
   failure rather than an unexecuted line.)

Impact is immaterial to the headline: excluding all 16 moves TOTAL from
2592/4037 (64.21%) to 2592/4021 (64.46%), +0.25pp. Reported for correctness,
not to move the number.

## HONEST RED (the large majority — correctly left to adjudication)

Every remaining red line traces to a route row that this lane classified as
open, excepted, or cross-crate. The coverage and the routes file agree, which
is the cross-check that matters:

- **`decode.rs` (176)** — three honest families and nothing else:
  the tz-abbrev family (~60 lines: `ClearTimeZoneAbbrevCache`,
  `DecodeTimezoneAbbrev` 258-275, `DecodeTimezoneAbbrevPrefix` 284-332, and the
  TZ-token arms of `DecodeDateTime` 1463-1590 / `DecodeTimeOnly` 1888-2035),
  matching the tz.rs state-seam carve and the two honestly-OPEN abbrev rows;
  ~50 unreached `return DTERR_BAD_FORMAT/FIELD_OVERFLOW/dterr` one-liners deep
  in the parse cascades; and 3 defensive `panic!`s (1435 unrecognized RESERV
  token, 1590/2035 session-tz-not-initialized).
  **Proof-covered-unmeasured subset:** `CheckDateTokenTable`/`CheckDateTokenTables`
  (2777-2793) are green under `hlp::check_date_token_table{,s}_concrete`, and
  `datebsearch:122` (`return None`, the miss path) under
  `hlp::eq_datebsearch_*_cells`. Fuzz cannot drive startup validators; the
  proofs do, and the census names them.
- **`tz.rs` (62)** — 100% inside the state-seam carve: `ConvertTimeZoneAbbrevs`
  19, `FetchDynamicTimeZone` 17, `DetermineTimeZoneAbbrevOffset` 7, `overflow`
  7, `InstallTimeZoneAbbrevs` 3, `interpret_timezone_abbrev_at` 3,
  `pg_tz_acceptable`/`pg_get_timezone_name`/`pg_localtime` 2 each. All 22 of
  these route rows are `excepted`.
- **`adt_date/lib.rs` (521)** — dominated by exactly the rows left OPEN:
  `extract_date` 53, `date2timestamptz_opt_overflow` 37 (date_timestamptz/1174),
  `date2timestamp_opt_overflow` 16, `timestamptz_date` 11, `timestamptz_timetz`
  11, `timestamp_date` 10; plus carved `timetz_zone` 17 and TRIAGE-HELD
  `timetz_part_common` 31; plus undriven recv/typmod/cmp paths
  (`timetz_recv` 11, `anytime_typmod_check` 16, `timetz_cmp_internal` 12).
- **`adt_date/builtins.rs` (599)** — `fc_*` wrappers for functions no arm
  drives: the cross-crate interval/timestamp families (`fc_interval_send` 12,
  `fc_timestamp_send` 11, `fc_timestamptz_pl/mi_interval` 10 each), the
  not-registrable families the file header already documents (`fc_time_support`
  29 = planner node, `fc_in_range_*` 19/17/14), and `fc_extract_date` 11.
- **`errors.rs` (7)** — two unexecuted error constructors: interval field
  overflow (35-36) and time-zone-not-recognized (48-52); the latter is reached
  only via the carved abbrev path.
- **`consts.rs` 215-249** — the `is_nobegin`/`is_noend`/`not_finite` chain and
  `token_bytes`. Verified honest: `not_finite`'s only callers are
  adt_date/lib.rs:974/999/1011, inside `time_pl_interval`/`time_mi_interval`/
  `timetz_pl_interval` — precisely the rows left OPEN. `token_bytes`' callers
  are decode.rs:2783 (proof-covered `CheckDateTokenTable`) and tz.rs:286
  (carved). Consistent, not artifact.

## Verdict

No bogus-red escaped into the honest-signal bucket, and no honest signal was
laundered as an artifact. 16 structurally-unmeasurable lines (+0.25pp) need a
licensed auto class; everything else is a real, attributable gap that the
routes file already names.

---

# Refresh: four-target merge (2026-07-31, adds datetime_convert_diff @ d36bdb059b)

The convert target moved adt_date/lib.rs 372/778 (was 252), builtins.rs 70/703
(unchanged — it calls CORE fns, not fc wrappers), decode.rs 1666/1785 (was
1608; the tz-abbrev resolver arms went green), tz.rs 132/156 (was 94),
consts.rs 16/24 (interval helpers). Every residual line was re-rendered with
source context and mapped to its enclosing item (fn-span parser + hand dumps
of all multi-line clusters); the adjudication is now carried per-line:

- 838 rows appended to `proofs/coverage/phase1-exceptions.tsv`
  (proof-covered-unmeasured 588, excluded-state 142, instrument-unmappable 61,
  const-eval-only 28, defensive-c-parity 19).
- 382 lines honestly owed, per-line in `OWED-UNCOVERED.tsv` — NOT exception-
  carried. Big groups: extract_date/extract_time numeric faces (72), thin fc
  wrappers with proved cores but nothing driving the wrapper itself (107),
  adt_timestamp cross-crate wrappers hosted in builtins.rs (68), timetz
  part(tz) TRIAGE-HOLD (31), fc_time_support planner node (25), unhit decode
  error/overflow one-liners (45), lib.rs error ctors / soft-error faces /
  unhit special arms (34), interval-overflow error ctor reachable only via
  interval_in (2).

## New adjudications made by eye in this pass

- decode.rs 432-441 (`APPEND_CHAR`-style macro body inside ParseDateTime):
  executed on EVERY exec, yet NO DA record in any of the 4 lcovs (verified
  against the raw lcov line tables 2026-07-31) — instrument-unmappable,
  carried with rig evidence. Same instrument shape as encode.rs:105.
- decode.rs `if !have_tz { return DTERR_BAD_FORMAT }` family (18 lines):
  defensive-c-parity — C datetime.c has the identical `if (tzp == NULL)`
  arms and every SQL entry point passes a tz out-param.
- decode.rs DTZ/DYNTZ/named-zone arm bodies + DetermineTimeZoneAbbrevOffset
  epilogues (43 lines): excluded-state. zoneabbrevtbl is session config; the
  io rig pins GMT and bounds tz-name admission (the 2GB-OOM fix). The
  resolvers themselves ARE fuzzed at engine grain (datetime_convert_diff
  arms 5-6, pinned table) — only the in-band decode paths stay carved.
- lib.rs 275 (`panic!("invalid argument for EncodeSpecialDate")`):
  defensive-c-parity — C date.c EncodeSpecialDate has the same elog(ERROR).
- lib.rs 241/251-252 (date_in soft-error `escontext` face): honest-uncovered,
  owed — no target drives soft-error input mode.
- builtins.rs b/bn (const fn FmgrBuiltin constructors), lib.rs
  TIME_SCALES/TIME_OFFSETS + TimeTzADT layout const-asserts, consts.rs DTK
  mask consts + Interval layout asserts, tables.rs tk(): const-eval-only.
- errors.rs 48-53 (DTERR_BAD_ZONE_ABBREV ctor): excluded-state — needs a
  configured abbrev-file entry naming an unknown zone. 35-36 (interval
  overflow ctor): owed cross-crate (only interval_in reaches it).

## Bottom line (the 100% question)

measured + exception-carried: adt_datetime 2500/2547 = **98.15%**,
adt_date 1155/1490 = **77.52%**. Neither crate claims 100%; the shortfall is
named per-line in OWED-UNCOVERED.tsv. Nothing in the residual is silent.

---

# Refresh: p1-lanel2 closeout, six-target merge (2026-07-31)

Merge adds timestamp_diff (p1-laney merge: 20,696-input corpus) and the new
datetime_closeout_diff (8 arms: extract numeric faces vs verbatim C with the
ARG-CAPTURE numeric plane, all undriven fc wrappers incl. recv/soft-error/
typmod/in_range, date skip-support). TOTAL fuzz 3448/4165 (82.79%);
**adt_date 1490/1490 and adt_datetime 2675/2675 = 100.00% accounted**
(measured + exception-carried), zero owed, zero exception rows on covered
lines (cross-checked mechanically).

Every residual red line was adjudicated per-line this pass (the 61-line
final residual hand-rendered with source; sample re-render across all seven
classes eyeballed clean):

- 33 decode.rs parse-cascade dead arms -> unreachable-arm rows, each with
  the structural reason (scanner shape guarantees, dispatcher pre-routing,
  table totality) after a ~370-seed directed battery turned none green.
- 7 strtod dblmin-helper dead arms -> unreachable-arm (2-adic /
  limb-geometry impossibility arguments).
- instrument-unmappable: macro-rule @ret lines (expansions driven 12k+),
  no-DA call-argument/continuation lines with rig evidence.
- defensive-c-parity: date_in default dtype arm (C default equally dead),
  AdjustTimeForTypmod negative branch (TimeADT on-disk invariant).
- 204 formerly proof-covered-unmeasured rows PURGED: the closeout target
  now measures those lines directly (rows on covered lines are banned).

Two product defects found and FIXED during the audit (both ground-truthed
on docker postgres:18.3): (1) timetz_cmp_internal checked-add debug panic
on the SQL-reachable in_range wrap band -> wrapping_add C-parity;
(2) strtod_model treated long exact-subnormal tokens (hex >256 digits,
ALL decimal) as inexact ERANGE -> 22007 while real 18.3 accepts them ->
delegates to adt_float strtod_c (uncapped exactness) + tininess refinement.

Bogus-red carried classes from the first audit (encode.rs:105, tables.rs
tk(), consts.rs const-asserts) remain exception-carried unchanged.
