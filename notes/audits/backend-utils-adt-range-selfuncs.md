# Audit: backend-utils-adt-range-selfuncs

- **Date:** 2026-06-12
- **Model:** Opus (Opus 4.8, 1M)
- **Verdict:** PASS
- **C sources:** `src/backend/utils/adt/rangetypes_selfuncs.c`,
  `src/backend/utils/adt/multirangetypes_selfuncs.c` (PostgreSQL 18.3)
- **Port crate:** `crates/backend-utils-adt-range-selfuncs`
  (`src/lib.rs`, `src/range.rs`, `src/multirange.rs`)

This is an independent re-audit. The function inventory was re-derived from the
C sources and cross-checked against the c2rust run
(`c2rust-runs/backend-utils-adt-range-selfuncs/src/*.rs`), which contains all 26
function definitions (the 11 estimation kernels appear twice — once per C file —
plus the two `default_*_selectivity`, two `*sel` entry points, and two
`calc_*sel` per file). The kernels are byte-identical between the two C files;
the port shares them once in `lib.rs`. All logic was compared C ↔ c2rust ↔ port.

## Function inventory

The 11 shared estimation kernels are identical in both C files, so a single port
function covers both C definitions. Per-file unique functions
(`default_*_selectivity`, `*sel`, `calc_*sel`, the per-vocabulary
`calc_hist_selectivity`) have one row each.

| C function (file:line) | Port location | Verdict | Notes |
|---|---|---|---|
| `default_range_selectivity` (range:67) | `range.rs:41` | MATCH | switch arms identical; 0.01 / 0.005 / `DEFAULT_RANGE_INEQ_SEL` / `DEFAULT_INEQ_SEL` / default 0.01 |
| `rangesel` (range:108) | `range.rs:78` | MATCH | get_restriction_variable punt, IsA(Const) punt, constisnull→0.0, commute via get_commutator, `@>elem` single-point range build, same-type DatumGetRangeTypeP, else default; final CLAMP_PROBABILITY |
| `calc_rangesel` (range:231) | `range.rs:194` | MATCH | const deserialize for empty flag; delegates null/empty-frac + merge to shared `calc_sel`; CONTAINED merge branch; strict null multiplier + clamp |
| `calc_hist_selectivity` (range, range:373) | `range.rs:250` | MATCH | needs_length_hist = CONTAINS\|CONTAINED; full operator switch incl. `&&`/`@>elem` sum, CONTAINS, CONTAINED infinite-bound sub-cases; -1.0 on no stats; const deserialize moved to caller (equivalent, see notes below) |
| `default_multirange_selectivity` (mr:78) | `multirange.rs:57` | MATCH | all 27 operator arms identical |
| `multirangesel` (mr:137) | `multirange.rs:107` | MATCH | same prologue; `@>elem`→make_multirange singleton; range-promotion arm (7 ops); var-is-elem/range punt arm (8 ops); same-type DatumGetMultirangeTypeP |
| `calc_multirangesel` (mr:291) | `multirange.rs:258` | MATCH | empty iff rangeCount==0; overall bounds from range 0 / range rangeCount-1; CONTAINED merge for the two contained ops; shared `calc_sel` |
| `calc_hist_selectivity` (mr, mr:456) | `multirange.rs:353` | MATCH | rng_typcache = typcache->rngtype; needs_length_hist = 4 contains/contained ops; full operator switch; filtered-out ops → elog; -1.0 on no stats |
| `calc_hist_selectivity_scalar` (range:596 / mr:707) | `lib.rs:400` | MATCH | rbound_bsearch + Max(index,0)/(n-1) + linear interpolation; shared once |
| `rbound_bsearch` (range:628 / mr:739) | `lib.rs:432` | MATCH | lower=-1, upper=n-1, middle=(lower+upper+1)/2, `cmp<0 \|\| (equal&&cmp==0)` |
| `length_hist_bsearch` (range:657 / mr:768) | `lib.rs:456` | MATCH | same bsearch on f64 length values |
| `get_position` (range:683 / mr:794) | `lib.rs:475` | MATCH | finite/finite subdiff w/ NaN & zero-width punts to 0.5, clamp [0,1]; the three infinite-bound branches identical |
| `get_len_position` (range:762 / mr:873) | `lib.rs:548` | MATCH | `1.0-(hist2-value)/(hist2-hist1)`; infinite branches: 1.0 / 0.0 / 0.5 |
| `get_distance` (range:807 / mr:918) | `lib.rs:581` | MATCH | subdiff w/ NaN/negative→1.0; both-infinite equal-lower→0.0 else +Inf; one-infinite→+Inf |
| `calc_length_hist_frac` (range:855 / mr:966) | `lib.rs:620` | MATCH | trapezoid integral; first/last bin interpolation; degenerate length1==length2→PB; `PA>0\|\|PB>0` NaN guards; inf/inf→0.5 |
| `calc_hist_selectivity_contained` (range:1018 / mr:1131) | `lib.rs:748` | MATCH | upper inclusive flip + lower=true; upper_index Min clamp; backward bin loop, final-bin bin_width subtraction & clamp 0.0 |
| `calc_hist_selectivity_contains` (range:1139 / mr:1252) | `lib.rs:864` | MATCH | lower_index bsearch (equal=true), Min clamp; prev_dist = get_distance(lower,upper); backward loop with `1.0 - calc_length_hist_frac` |

## Cross-checked constants (verified against headers, not memory)

- Range operator OIDs 3884–3896 — verified vs `pg_operator.dat` (all 13 match).
- Multirange operator OIDs 2862–2877, 3585, 4035, 4142, 4395–4400, 4539, 4540 —
  verified vs `pg_operator.dat` (all 27 match, including the non-contiguous
  ranges).
- `STATISTIC_KIND_BOUNDS_HISTOGRAM` = 7, `STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM`
  = 6 — verified vs `pg_statistic.h`; port `types-selfuncs` matches.
- `ATTSTATSSLOT_VALUES` = 0x01, `ATTSTATSSLOT_NUMBERS` = 0x02 — verified vs
  `lsyscache.h`; port matches.
- `DEFAULT_INEQ_SEL` = 0.3333333333333333, `DEFAULT_RANGE_INEQ_SEL` = 0.005,
  `DEFAULT_MULTIRANGE_INEQ_SEL` = 0.005 — verified vs `selfuncs.h`; port matches.
- `RangeIsEmpty` = RANGE_EMPTY flag (port reads it via `range_deserialize`'s
  `empty` out-flag); `MultirangeIsEmpty` = `rangeCount == 0` (port reads
  `rangeCount` directly). Both verified vs `rangetypes.h` / `multirangetypes.h`.

## Error paths

Three `elog(ERROR, ...)` sites in each C file, all mapped to `PgError`/`ERROR`
(internal error, no SQLSTATE) via `elog_error`:
1. `"invalid empty fraction statistic"` when `sslot.nnumbers != 1`
   (`lib.rs:174`, fires under same predicate).
2. `"bounds histogram contains an empty range"` when a deserialized histogram
   range is empty (`lib.rs:349`).
3. `"unexpected operator %u"` / `"unknown range/multirange operator %u"` —
   the empty-const default arm and the operator-switch default arm
   (`range.rs:242/356`, `multirange.rs:343/474`). The filtered-out operator arms
   that C lists explicitly before `default` are reproduced and route to the same
   elog.

## Structural divergences reviewed (all behavior-preserving)

- **Const-bound extraction relocated.** C extracts `const_lower`/`const_upper`
  inside `calc_hist_selectivity` (range:452 / mr:539); the port extracts them in
  `calc_rangesel`/`calc_multirangesel` and threads them into the hist closure.
  For non-empty constants the extraction is unconditional in both, and the
  values are only consumed on the histogram path (called only for non-empty), so
  results are identical. The C `Assert(!empty)` / `Assert(rangeCount>0)` becomes
  a structural guarantee in the port (empty branch never reaches the closure).
- **Shared `calc_sel` / `calc_hist_prologue`.** The null/empty-frac lookup, the
  empty-vs-histogram merge, the strict-null multiplier + final clamp, and the
  security-check + bounds-histogram + length-histogram extraction loop are
  factored into shared helpers parameterized by per-vocabulary closures. Each
  branch maps 1:1 to the C; the `<@`-family merge test and empty-const switch are
  supplied by the vocabulary modules, matching the C per-file switches.
- **AttStatsSlot / VariableStatData cleanup is RAII.** `free_attstatsslot`
  becomes `AttStatsSlot` Drop (slots held in `HistData`); `ReleaseVariableStats`
  becomes `VarStatsGuard` Drop, covering every early-return / `?` path that the C
  handles with explicit `ReleaseVariableStats(vardata)` before each
  `PG_RETURN_FLOAT8`. Conforms to AGENTS.md RAII requirement for C cleanup.
- **CLAMP_PROBABILITY** branch order preserved (`<0.0` then `>1.0`, NaN passes
  through) in `clamp_probability`.

## Seam audit

**Owned seam crates: none.** The unit's `c_sources` are
`rangetypes_selfuncs.c` and `multirangetypes_selfuncs.c`; no
`backend-utils-adt-range-selfuncs-seams` or
`backend-utils-adt-multirangetypes-selfuncs-seams` crate exists, and correctly
so — nothing in the system calls *into* these estimator functions across a seam
(they are reached via fmgr dispatch, not a cross-cycle seam). `init_seams()` is
therefore an empty installer with no owned seam declarations outstanding — not a
finding.

**Outward seam calls** (all thin marshal + delegate, each justified by a real
dependency on an unported neighbor):
- `backend-utils-adt-rangetypes-seams`: `range_cmp_bounds`, `range_subdiff`,
  `range_deserialize`, `range_serialize`, `range_get_typcache`,
  `datum_get_range_type_p`.
- `backend-utils-adt-multirangetypes-seams`: `multirange_get_typcache`,
  `make_multirange`, `multirange_get_bounds`, `datum_get_multirange_type_p`.
- `backend-utils-adt-selfuncs-seams`: `get_restriction_variable`,
  `release_variable_stats`, `statistic_proc_security_check`,
  `stats_tuple_stanullfrac`.
- `backend-utils-cache-lsyscache-seams`: `get_commutator`, `get_attstatsslot`.

Each call site converts arguments, makes one delegated call, and consumes the
result. All branching (operator dispatch, calc-vs-default classification,
const construction, the histogram loops) is the unit's own logic in-crate — no
computation, node construction, or branching lives in a seam path. No function
body was replaced by a delegating "somewhere else" call.

## Design conformance

- Allocating paths (`range_serialize`, `make_multirange`,
  `datum_get_*_type_p`, `get_attstatsslot`, histogram vectors) take `Mcx` and
  return `PgResult`; OOM via `try_reserve` + `mcx.oom`.
- No shared statics for per-backend globals; no ambient-global seams; no locks
  held across `?`; no registry-shaped side tables.
- Neighbor-dependency decisions use per-owner seams with RAII drop guards as
  AGENTS.md prescribes; no invented opacity, no unledgered divergence markers.
- Build: `cargo build -p backend-utils-adt-range-selfuncs` compiles clean.

## Verdict

**PASS.** Every function MATCH; zero seam findings; constants verified against
headers; error paths and edge cases preserved. The catalog row is set to
`audited`.
