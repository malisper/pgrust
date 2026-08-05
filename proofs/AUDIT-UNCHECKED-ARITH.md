# AUDIT: unchecked arithmetic vs PostgreSQL checked helpers

Date: 2026-07-30. Base: origin/main 69c3c7eb9043. Reconnaissance only — no fixes here;
each candidate gets its own lane. C reference: vendored PostgreSQL 18.x at
`pgrust-fabled/vendor/postgres-src`.

Charter: four instances of "the port replaced a C checked helper with a bare Rust
operator (or vice versa)" were confirmed in one day (on_ppath float8_pl, money MIN/-1,
transarray `+=`, hashchar sign extension). This audit enumerates the whole class.

## Verdict up front

The class is **mostly clean**. Of **~279 checked-helper call sites** in ported C code,
**278 are MATCHED** and exactly **1 is BARE** — the already-known `on_ppath` (empirically
re-confirmed below). Zero wrongly-WRAPPING sites, zero unported gaps in audited files.
The four instances found today are the exceptions, not the rule.

The live residue is in the **inverse direction**: ~13 sites where pgrust does bare Rust
arithmetic on the wrap plane where C is `-fwrapv` — release-parity holds (verified for
the SQL-reachable one), but **debug/test builds panic**. That is the transarray class
(instance 3), and its full sibling inventory is below.

## 1. Helper vocabulary (what C guards)

`src/include/common/int.h`:
- `pg_add_s16/s32/s64_overflow`, `pg_sub_*`, `pg_mul_*`, `pg_neg_*` and the `u16/u32/u64`
  variants — return `true` on overflow; callers `ereport` 22003 (or soft-error / fallback).

`src/include/utils/float.h`:
- `float4_pl/mi/mul/div`, `float8_pl/mi/mul/div` — result `isinf` (from finite inputs)
  → `float_overflow_error()` 22003; mul/div result `== 0` from nonzero inputs
  → `float_underflow_error()` 22003; div checks zero divisor → 22012 (NaN dividend exempt).

`src/include/common/int128.h`:
- `int128_add_int64`, `int128_add_uint64`, `int128_add_int64_mul_int64`,
  `int128_compare`, `int128_to_int64` — non-native-int128 fallback; on this tree the
  audited files use native `int128` arms, covered under the numeric aggregates below.

pgrust's counterparts: `crates/backend/utils/adt/float/src/lib.rs:231-320` (verified
C-faithful, same SQLSTATEs), `int/src/lib.rs:30-60` + `int8/src/lib.rs:27-51`
(`overflowing_*`-based `pg_*_overflow` twins), and per-site `checked_*` calls elsewhere.

## 2. Call-site census (forward direction: C checked → is Rust checked?)

| Package | C files | Sites | MATCHED | BARE | WRAPPING-defect | N/A |
|---|---|---|---|---|---|---|
| A int arithmetic | int.c, int8.c, numutils.c, numeric.c | 51 | 51 | 0 | 0 | 0 |
| B strings/money | cash.c, varbit.c, varlena.c, oracle_compat.c, formatting.c | 46 | 46 | 0 | 0 | 0 |
| C date/time | timestamp.c, date.c, datetime.c | 67 | 67 | 0 | 0 | 0 |
| D arrays/catalog | arrayfuncs.c, arrayutils.c, array_userfuncs.c, float.c(int), detoast.c, heap.c, pg_constraint.c, tablecmds.c | 34 | 34 | 0 | 0 | 0 |
| E float helpers | float.c(float), geo_ops.c, cash.c(float), timestamp.c(float) | ~134 | ~133 | **1** | 0 | 0 |
| **Total** | | **~279 (audited to site or count-diff level)** | **278** | **1** | **0** | **0** |

Notes on method: packages A–D were audited site-by-site (every C `pg_*_overflow` call
mapped to a rust line). Package E's geo_ops.c (110 sites across 49 functions) was audited
by per-function checked-helper count diff (C awk scan vs rust scan, imports excluded)
plus manual reads of the anomalies; all 49 functions matched count-for-count except
`on_ppath`.

### Package A — int.c / int8.c / numutils.c / numeric.c (51/51 MATCHED)

All `int2/4/8` operator families (`int4pl`…`int28mul`, 36 sites), `in_range_*` (3),
`int4inc/int8inc/int8dec` (agg hot path), `int4lcm/int8lcm`, `generate_series_step_int4/8`
(overflow zeroes step, as C), `pg_strtoint{16,32,64}_safe` fast+slow `pg_neg_u*` (6 sites
→ `numutils/src/lib.rs:223-227,333-337`, u64-accumulator equivalent),
`int64_div_fast_to_numeric` (`numeric/src/ops.rs:969` checked_mul → i128 fallback,
mirrors the HAVE_INT128 arm), `numericvar_to_int64/uint64` (`numeric/src/var.rs:700-733`).
Errors are the correct 22003 family (`integer/smallint/bigint out of range`).

### Package B — cash / varbit / varlena / oracle_compat / formatting (46/46 MATCHED)

cash 9/9 (incl. `cash_in`'s chained `checked_mul(10)/checked_sub` with soft-error
preserved; `cash_mul_float8`/`cash_div_float8` via checked `float8_mul/div`);
varbit 2/2 (`bitsubstring` overflow ⇒ run-to-end, not error — polarity preserved);
varlena 8/8 (`text_substring`/`bytea_substring` overflow ⇒ `-1` sentinels, overlay ⇒
22003; `text_format_parse_digits` → "number is out of range"); oracle_compat 8/8
(shared `worst_case_bytelen` 54000 guard for lpad/rpad/translate; `repeat` checked);
formatting 19/19 (all `DCH_from_char`/`do_to_timestamp` sites → 22008 or
`DTERR_FIELD_OVERFLOW`, soft-error preserved). Full site-by-site table in the audit
transcript; every SQLSTATE matches C.

### Package C — timestamp.c / date.c / datetime.c (67/67 MATCHED)

All 52 timestamp.c sites (incl. `make_timestamp_internal`, `AdjustIntervalForTypmod`
ereturn path, `make_interval` — whose single `float8_mul` correctly raises 22003 while
the int sites raise 22008 —, `timestamp_mi`, justify family, `timestamp[tz]_pl_interval`
with the `julian < 0` vs `< -1` distinction preserved verbatim, `interval_um` via
`0i64.checked_sub`, `finite_interval_pl/mi`, `interval_mul/div` carries,
`timestamp[tz]_bin`, `interval_part` numeric fallback, `generate_series` support),
12 datetime.c sites (`int64_multiply_add`, Adjust* family, `DecodeInterval`), 3 date.c
sites (`make_date` `checked_neg`, `in_range_time[tz]_interval` overflow⇒bool). Fine
points that would have been easy to get wrong (implicit i64→i32 narrowing before the
checked add in `interval_justify_hours`) are bit-identical.

### Package D — arrays / catalog / DDL (34/34 MATCHED)

- arrayfuncs.c 19/19: `overflowing_sub/add` chains mirroring the C
  `pg_sub_s32_overflow || pg_add_s32_overflow` idiom
  (`arrayfuncs/src/io.rs:255-256`, `element.rs:511-512,559-574,935-950`).
- arrayutils.c `ArrayCheckBounds` → local `add_s32_overflow` (`arrayutils/src/lib.rs:16,91`).
- array_userfuncs.c 2/2 → `checked_add/checked_sub` (`array_userfuncs/src/lib.rs:87,90`).
- float.c `width_bucket_float8` count+1 both arms → `checked_add` (`float/src/funcs.rs:983,1003`).
- detoast.c slicelimit → `checked_add` with C's exact fallback (`detoast/src/lib.rs:248-258`).
- heap.c / pg_constraint.c / tablecmds.c inhcount increments (9 C sites): rust uses the
  equivalent `if x == i16::MAX { "too many inheritance parents" }` pre-check before a
  bare `+ 1` at every site (constraints.rs:910-918, pg_constraint/lib.rs:1552-1560,
  attach.rs:540-552/758-771, alter.rs:2210-2220/3915-3926, inheritance.rs:614,726,
  1299-1305,1494-1500). Semantically MATCHED for `+1`.

### Package E — float helper call sites (~134; 1 BARE)

- float.c: 21/21 MATCHED (`fc_dpi` operator dispatch, `float48*/float84*`,
  degrees/radians, `float8_combine`, `float8_regr_combine`).
- cash.c float sites 2/2, timestamp.c 1/1 (counted in B/C tables as well).
- geo_ops.c: 110 sites, 49 functions; 48 functions matched count-for-count
  (`circle_poly` is split across `fc_circle_poly` + `circle_poly_vertex` but totals
  match; `point_mul_point`/`point_div_point` verified by read). One divergence:

**BARE #1 (the only forward defect): `on_ppath`** —
`crates/backend/utils/adt/geo/src/proximity.rs:390`: `FPeq(a + b, point_dt(...))` where C
geo_ops.c uses `FPeq(float8_pl(a, b), ...)`. Known instance; being fixed by a sibling
lane; counted here, not fixed.

## 3. Ranked candidate defects (forward)

1. **on_ppath** (`geo/src/proximity.rs:390`) — reachable from SQL literals; empirically
   confirmed (below). Severity: wrong answer (`f`) where C raises 22003. Already owned by
   a sibling lane. **This is the entire forward-direction defect list.**

## 4. Empirical confirmations (docker: `malisper/pgrust:v0.2` vs `postgres:18`)

| Query | postgres:18 | pgrust v0.2 | Verdict |
|---|---|---|---|
| `SELECT point(0,0) <@ path '[(9.5e307,1),(9.5e307,-1)]';` (finite distances, overflowing sum) | `ERROR: value out of range: overflow` | `f` | **CONFIRMS on_ppath BARE** |
| `SELECT '-92233720368547758.08'::money / (-1)::int8;` | silently returns `-$92,233,720,368,547,758.08` (aarch64; SIGFPE on x86-64) | `ERROR: money out of range` | Confirms the **deliberate documented divergence** (upstream `cash_div_int64` lacks the MIN/-1 guard its multiply path has; pgrust guards, `cash/src/lib.rs:428-441`) |
| `SELECT '92233720368547758.07'::money + '0.01'::money;` | `ERROR: money out of range` | same | parity |
| `SELECT to_date('-2147483648 BC','FMCC BC');` | `ERROR: date/time field value out of range` | same | parity |
| `SELECT int4_avg_accum('{1,9223372036854775807}'::int8[], 1);` | `{2,-9223372036854775808}` | `{2,-9223372036854775808}` | **Release parity on the wrap plane — and proof the transarray overflow is SQL-reachable; a debug build panics on exactly this query** |

## 5. Inverse direction: Rust stricter than C

No `[profile.release]`/`[profile.dist]` in the workspace sets `overflow-checks`, so all
"BARE-on-the-wrap-plane" items below wrap identically to C `-fwrapv` in release; the
failure mode is **debug/test-build panic** (per project law, a debug-only guard on
something C doesn't check is a ported-in constraint to delete / rewrite as `wrapping_*`).

### 5a. Debug-panic inventory (bare Rust ops where C is -fwrapv), ranked by reachability

1. **transarray avg family** — `numeric/src/builtins.rs:906-907`
   (`fc_int2_avg_accum`/`fc_int4_avg_accum` macro: `*td += 1; *td.add(1) += newval` on
   i64), `:932-933` (`_inv` siblings), `:957-958` (`fc_int4_avg_combine`). C
   numeric.c:6797-6926 is bare under `-fwrapv`. **SQL-reachable directly** with a hostile
   transvalue (`int4_avg_accum('{1,9223372036854775807}'::int8[], 1)` — verified above).
   This is confirmed instance 3 + its previously-unfixed siblings. The correct pattern
   already exists next door: `int2_sum/int4_sum` (`builtins.rs:31`) uses `wrapping_add`
   with a pinning test.
2. **int128 accumulators** — `numeric/src/aggregates.rs:762-765` (`do_int128_accum`:
   `sum_x2 += newval*newval; sum_x += newval; n += 1`), `:771-774` (`do_int128_discard`),
   `numeric/src/builtins.rs:589-592` (`poly_combine_common`). C numeric.c:5640-5977 bare.
   Practically unreachable via honest SQL (~1.8e19 max-value rows), combine path needs
   pre-overflowed states.
3. **numeric agg row counters** — `numeric/src/aggregates.rs:401,405,450,560-569` (i64
   `++/--/+=`). Unreachable.
4. **formatting century math** — `formatting/src/dch_entry.rs:351` (`tmfc.cc = -tmfc.cc`),
   `:377` (`tmfc.cc * 100 + ...`), `:382` (`tm.tm_year = -tm.tm_year`). C
   formatting.c:4557/4595 identically bare (upstream checked the sibling arms but not
   these). Agent analysis says drivable via `to_date` with `FMCC`/`FMYYYY` extremes; my
   first-cut repro was rejected earlier by a range check, so treat reachability as
   plausible-unverified. Debug-only either way.
5. **array slice inner sum** — `arrayfuncs/src/element.rs:572,948` (`dims[0] + lbs[0]`
   bare inside the overflowing_sub call). Bare in C too; bounded by ArrayCheckBounds at
   construction. Parity; note only.

### 5b. Stricter-in-release divergences (panic or error where C proceeds)

6. **`ConstraintSetParentConstraint`** — `pg_constraint/src/lib.rs:1406-1408` replaces
   C's `pg_add_s16_overflow` (pg_constraint.c:1150) with a **release-effective**
   `assert!(prior_inhcount == 0)` then hard-sets 1. If the partition-attach invariant
   ever fails to hold, rust panics where C increments/errors. Likely unreachable today;
   flag for the assert-audit lane.
7. **`itmin2interval().expect(...)`** — `adt_timestamp/src/builtins.rs:1206,1243`; C
   `(void)`-discards with a "can't overflow" comment. Unreachable (`|gmtoff| < 2^17`);
   cosmetic.
8. **`cash_div_int64` MIN/-1 guard** — `cash/src/lib.rs:428-441`: deliberate, documented,
   fixes an upstream bug (x86-64 C SIGFPEs). Keep; not a defect. (= confirmed instance 2.)

### 5c. Correct wrapping_* usages (verified intentional, not defects)

`int2_sum/int4_sum` (`numeric/src/builtins.rs:31`), `numericvar_to_int128` sniffer
(`numeric/src/var.rs:754-760`), `interval_to_char` months math
(`formatting/src/dch_entry.rs:190-193`), `int4_to_char` 'V' scaling
(`formatting/src/num_entry.rs:314` — note the int8 path at `:377` correctly stays
*checked* because C there calls `int8mul`), `text_right` `wrapping_neg`
(`oracle_compat/src/lib.rs:566`), and the date/time `wrapping_*` family
(`adt_date/src/lib.rs:358,373,384,467,996-1032`) — all mirror bare C sites, several with
pinning tests. Polarity is right in every case examined.

## 6. Ledger rows that should change (NOT edited here — many lanes writing it)

- on_ppath / geo proximity rows: annotate "BARE float8_pl (audit-confirmed empirically
  2026-07-30); fix owned by sibling lane".
- numeric transarray rows (`int2/int4_avg_accum`, `_inv`, `int4_avg_combine`): add
  "debug-panic on wrap plane; needs wrapping_* like int4_sum; SQL-reachable via hostile
  transvalue".
- cash rows: annotate "MIN/-1 guard = deliberate divergence from upstream bug; keep".
- pg_constraint `ConstraintSetParentConstraint`: annotate "release assert replaces C
  checked add; assert-audit candidate".

## 7. Suggested follow-up lanes (in priority order)

1. Transarray/avg-accum `wrapping_*` sweep (items 5a.1, and opportunistically 5a.2-3)
   — one mechanical lane, pattern already proven by `int2_sum`.
2. formatting `dch_entry.rs` cc-math wrapping_* + reachability test (5a.4).
3. Assert-audit: `pg_constraint/src/lib.rs:1406` (5b.6).
4. (Already owned) on_ppath fix.
