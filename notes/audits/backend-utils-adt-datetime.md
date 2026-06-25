# Audit: backend-utils-adt-datetime

**Verdict: PASS** (after fix)

Independent audit of the newly-added fmgr PGFunction builtin layer, the outward-seam
adapters/installs, and the freshly-ported `DecodeTimezoneAbbrevPrefix`, plus a
spot-check of the previously-ported value cores. Re-derived from the C sources
(`date.c`, `datetime.c`, `timestamp.c` under PG 18.3) and `pg_proc.dat`; port
comments and self-review were not trusted.

c_sources (CATALOG): `*/date.c, */datetime.c, */timestamp.c`. There is **no
standalone `isoweek.c`** in PG 18.3 — the isoweek cores (`date2isoweek`, etc.)
live inline in `timestamp.c`/`date.c`; the crate owns
`backend-utils-adt-isoweek-seams` for them. The task's "isoweek.c" is a misnomer.

## Counts

- **MATCH**: 220 distinct C fmgr functions registered (235 OID rows — several C
  functions back multiple pg_proc OIDs); all spot-checked cores + all seam
  adapters + `DecodeTimezoneAbbrevPrefix` MATCH.
- **DIVERGES**: 0 remaining (1 fixed — see below).
- **MISSING**: 0. 33 C fmgr functions are loudly-unregistered in 5
  documented-blocked categories (verified genuinely blocked, see below).
- **PARTIAL**: 0.

## Fix applied

**DEFECT (DIVERGES, fixed): `extract(field FROM time)` / `extract(field FROM timetz)`
returned float8 where numeric is required.**

`fc_extract_time` (OID 6200) and `fc_extract_timetz` (OID 6201) call cores
`time_part_common`/`timetz_part_common` with `retnumeric=true`. For the
integer-valued fields the cores return the `Int(i64)` result variant, deferring
the int→numeric conversion to the boundary (per the core's own contract; the
fractional EPOCH/SECOND/MILLISEC fields already return the `Numeric` variant).
The C does `PG_RETURN_NUMERIC(int64_to_numeric(intresult))` (date.c:2302,
timetz at date.c:3102). But the boundary formatters
`ret_time_part_result`/`ret_timetz_part_result` mapped `Int(i) => ret_f64(i as f64)`
— emitting a float8 bit-pattern where a numeric varlena is required. Silent
result-type/value corruption for `extract(hour|minute|second(int part)|timezone|
timezone_hour|timezone_minute FROM time/timetz)`. (`date_part`, OID 1385/1273,
`retnumeric=false`, returns float8 legitimately and was unaffected; the `Int`
variant is only produced on the `retnumeric` path.)

Fix (in `src/fmgr_builtins.rs`): both `Int` arms now route through a new
`ret_int64_numeric` helper that builds the numeric varlena via
`int64_to_numericvar` (mirroring the already-correct `ret_extract_date` `Int`
arm). `cargo check` + `cargo test` green after the fix (210 passed, 0 failed,
7 ignored — tzdb/parallel-worker, documented).

## 1. fmgr shim layer

### Registration parity (vs pg_proc.dat)

All 235 registered `builtin(oid, name, nargs, strict, retset, fc_*)` rows were
machine-checked against `src/include/catalog/pg_proc.dat`:
**0 mismatches** on (oid, name, nargs, proisstrict, proretset). No duplicate OIDs.

### Per-shim verification

Each `fc_<name>` shim was traced (subagents + lead): correct arg slots/width/sign
(`arg_i32` DateADT/int4/typmod, `arg_i64` Timestamp/TimeADT/int8, `arg_f64`
float8, `arg_cstring` for `_in`, `arg_text`→`&str`+lowercase, by-ref
Interval/TimeTzADT decode), correct value core, correct result marshaling.
Verified in full:

- **I/O** (`*_in`/`*_out`/`*_recv`/`*_send`): `_in`/`_recv` read typmod at slot 2
  (skipping typeid slot 1); cstring/varlena lanes correct.
- **Cross-type comparison macros** (`date_ts_cmp!`, `ts_date_cmp!`,
  `ts_tstz_cmp!` with `$swap`): the arg-swap + cmp-sign-negation is
  algebraically identical to C's flipped-operator form for every op
  (eq/ne/lt/le/gt/ge); `*_cmp_*` sign verified.
- **OID→core aliasing**: `timestamptz_eq`(1152)/`timestamp_eq`(2052) etc. share
  one core (binary-identical i64 representation, same C function); `date_add`/
  `date_subtract` (6221/6223/6222/6273) alias `timestamptz_pl/mi_interval(_at_zone)`;
  `mul_d_interval(float8,interval)` swaps args into `interval_mul`. All confirmed
  vs prosrc.
- **overlaps** (`overlaps_time`/`_timetz`/`_timestamp`, strict=false): NULLs read
  via `nullable_*`, None→result NULL — matches the non-strict contract.
- **Interval/TimeTzADT byte (de)serialization**: the by-ref boundary image is the
  native LE POD layout `Interval{time:i64,day:i32,month:i32}` (16B) /
  `TimeTzADT{time:i64,zone:i32}` (12B), field order matching the
  `types-datetime` structs; round-trips. The wire format (`*_recv`/`*_send` in
  `binio.rs`) is correctly **big-endian per field** with field order time/day/month
  and time/zone (matches C `pq_sendint64`/`pq_sendint32`). The two encodings are
  the correct distinct lanes.
- **now-family** (now/transaction_timestamp→start-ts, statement_timestamp,
  clock_timestamp, timeofday→text): read no args, correct cores.

### Loudly-blocked (33 C fmgr fns NOT registered — all genuinely blocked)

Machine-confirmed the unregistered set is exactly:
- **typmodin/typmodout** (12): need cstring[] ArrayType→&[i32] boundary marshaling
  (owned by arrayfuncs deconstruct_array; the boundary RefPayload has no array
  lane). Cores (`anytime_typmodin`, `anytimestamp_typmodin`, `intervaltypmodin`)
  exist taking `&[i32]` — genuinely gated on the boundary, correct to defer.
- **planner-support** (8): take `internal` SupportRequest*/SortSupport nodes,
  unmodeled at the fmgr boundary.
- **interval AVG aggregate** (7): operate on an `internal` IntervalAggState
  transition value (nodeAgg lifecycle).
- **SRFs** (5): generate_series_* + pg_timezone_* need the FuncCallContext SRF
  protocol (funcapi), not the one-shot boundary.
- **postmaster globals** (2): pg_postmaster_start_time/pg_conf_load_time read
  PgStartTime/PgReloadTime owned by the postmaster.

None silently stubbed; all loudly absent with accurate rationale.

## 2. Seam adapters (src/seam_impls.rs)

All **28** declared seams across the three owned seam crates
(`backend-utils-adt-timestamp-seams` 15, `-datetime-seams` 7, `-isoweek-seams` 6)
are installed via `::set` in `init_seams()` (declared set == installed set, exact).
`seams-init::init_all()` calls `backend_utils_adt_datetime::init_seams()`.

Adapters are thin marshal+delegate to the correct cores; verified:
`j2date`→tuple→YmdDate; `isoweek2date`/`isoweekdate2date` &mut-out→YmdDate;
`timestamp2tm` want_tz selecting zone-field resolution; `validate_date`
`isjulian=false` (consumer formatting.c has no Julian token path — correct);
`determine_time_zone_offset` using `session_timezone()`; the TzHandle
intern/resolve round-trip between `decode_timezone_abbrev_prefix` and
`determine_time_zone_abbrev_offset`. **The TimestampDifference family +
timestamptz_to_str + parse_recovery_target_time + JsonEncodeDateTime contain
real C logic and live in the crate (not in seam closures) — they are owner
cores exposed via the seam, which is correct.** No logic-in-seam-closure
finding.

## 3. DecodeTimezoneAbbrevPrefix (src/decode.rs:958)

Line-by-line vs datetime.c `DecodeTimezoneAbbrevPrefix`: MATCH. Prefix
downcasing loop (TOKMAXLEN bound, break on `\0`/non-alpha, `pg_tolower`);
truncating-search loop; session_timezone leg (`TimeZoneAbbrevIsKnown`,
isfixed→`offset = -offset` sign flip, dyntz→`tz = session_timezone`);
zoneabbrevtbl leg via the thread-local resolver hook (no resolver == C's
`zoneabbrevtbl == NULL`), DYNTZ with `FetchDynamicTimeZone` failure falling
through to the next-shorter prefix, fixed-offset using `tp->value`; `-1`
no-match return (seam returns `tzlen <= 0` convention). Faithful.

## 4. Cores spot-check (10 sampled, all MATCH; constants digit-verified)

`date_in`, `timestamp_in`, `interval_in`, `date_pli`, `timestamp_mi_interval`,
`extract_date`, `interval_cmp`/`interval_cmp_value`, `date2j`/`j2date`,
`overlaps_timestamp` (the intricate 3×3 NULL truth-table), `timestamp2tm`.
All control flow, branches, error SQLSTATEs
(DATETIME_VALUE_OUT_OF_RANGE / INVALID_DATETIME_FORMAT /
FEATURE_NOT_SUPPORTED / INVALID_PARAMETER_VALUE), integer widths/overflow
guards, and constants (POSTGRES_EPOCH_JDATE=2451545, UNIX_EPOCH_JDATE=2440588,
USECS_PER_DAY=86400000000, DAYS_PER_MONTH=30, the date2j/j2date Julian magic
numbers) verified faithful. The remaining ~150 cores are pre-ported (this layer
added only the fmgr shims + seams); the 10-sample baseline is sound.

## 5. Design conformance

- No `todo!`/`unimplemented!`/`unreachable!()` in the crate.
- Per-backend mutable globals (DateStyle/DateOrder/IntervalStyle, the timezone
  resolver, the TzHandle registry) are all `thread_local!`. The only non-tl
  statics are immutable read-only tables (`datetktbl`/`deltatktbl`/`months`/
  `days`, C `static const`), `&'static str` message helpers, one-time `Once`
  init guard, and two test-only `Mutex<()>` serialization locks.
- The TzHandle registry interns `Rc<pg_tz>`→`TzHandle(u32)`. `TzHandle`/
  `TzAbbrevMatch` are declared in **types-datetime** (inherited opacity — the
  owned surface name for C's `pg_tz *` round-tripped across the seam), not
  invented here. The intern closure holds the `RefCell` borrow only over
  infallible ops (no `?` across the borrow).
- Allocating seam `timestamptz_to_str` takes `Mcx<'mcx>` + returns
  `PgResult<PgString<'mcx>>`. No lock held across `?`.
- Banked divergence (ledgered in CATALOG): catastrophic GMT-init `pg_tzset` Err
  mapped to `DTERR_BAD_TIMEZONE` inside the i32-returning DecodeDateTime/
  DecodeTimeOnly (cannot propagate PgError) — pre-existing, accepted.

## Build

`cargo check -p backend-utils-adt-datetime`: clean (only pre-existing warnings in
unrelated crates). `cargo test -p backend-utils-adt-datetime`: 210 passed, 0
failed, 7 ignored.

## Verdict

**PASS.** One DIVERGES defect found and fixed (extract numeric marshaling for
time/timetz integer fields); re-verified from scratch. Zero MISSING, zero
remaining DIVERGES/PARTIAL, zero seam findings, zero design-conformance findings.
