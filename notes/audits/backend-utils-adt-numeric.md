# Audit: backend-utils-adt-numeric

- **Unit:** `backend-utils-adt-numeric`
- **C source:** `postgres-18.3/src/backend/utils/adt/numeric.c` (~12.6k LOC),
  `src/include/utils/numeric.h`
- **c2rust:** `../pgrust/c2rust-runs/backend-utils-adt-numeric/src/numeric.rs`
- **Port crates:** `crates/backend-utils-adt-numeric` (families: `kernel_var`,
  `kernel_transcendental`, `convert`, `io`, `ops_sql`, `aggregate`, `random`,
  `series_srf`), `crates/types-numeric` (keystone carrier),
  `crates/backend-utils-adt-numeric-seams`
- **Date:** 2026-06-13 (re-audit after the infra-blocked-fn fix round)
- **Model:** Claude Fable 5 (`claude-fable-5`)
- **Verdict:** **PASS** (the 6 previously-MISSING infra-dependent functions are
  now resolved: own logic written in-crate, cross-unit callees routed to their
  named unported owners per the rangetypes planner-support precedent)

## Method

Enumerated the 211 function definitions in `numeric.c` and cross-checked against the
c2rust rendering (323 fn names incl. header inlines) and the port. The port is a
value-type port over on-disk numeric byte images; it has no fmgr dispatch layer in
this crate, so the thin `PG_FUNCTION_ARGS` SQL wrappers are legitimately represented
by their underlying cores (consistent with the peer `backend-utils-adt-rangetypes`
audit's `FB` rows). A wrapper is only a finding when it carries OWN logic beyond
Datum marshalling that has no in-crate counterpart.

## Family / core coverage (representative MATCH spot-checks)

The arithmetic kernel, transcendental, conversion, I/O, SQL-operator and aggregate
families are present and faithful. Spot-checked in detail:

| C function | Port | Verdict | Note |
|---|---|---|---|
| `cmp_numerics` (numeric.c:~2900) | `ops_sql::cmp_numerics` | MATCH | full special ordering NaN>+Inf>finite>-Inf, byte-image cmp_var_common |
| `cmp_var_common` | `kernel_var::cmp_var_common` | MATCH | |
| `make_result_opt_error` | `convert::make_result_opt_error` | MATCH | |
| `get_min_scale` (4255) | `ops_sql::get_min_scale_var` | MATCH | last-nonzero-digit + trailing-zero reduction |
| `numeric_trim_scale` (4326) | `ops_sql::numeric_trim_scale` | MATCH | |
| `numeric_abbrev_convert` (2171) | `aggregate::numeric_abbrev_convert` | MATCH (toast detoast = fmgr boundary) |
| `numeric_abbrev_convert_var` (2384) | `aggregate::numeric_abbrev_convert_var` | MATCH | 14-bit slot packing, weight excess-44, negate; HLL add via OUTBOUND seam |
| `numeric_fast_cmp` (2300) | `aggregate::numeric_fast_cmp` | MATCH | |
| `hash_numeric` / `_extended` | `aggregate::hash_numeric*` | MATCH | |
| `do_numeric_accum` / `_discard`, `int{2,4,8}_accum`, `numeric_serialize`/`deserialize`/`combine`/`avg`/`sum`, `numeric_stddev_internal`, `make_int128_agg_state` | `aggregate::*` | present | Youngs-Cramer accumulators ported |
| `int64_to_numeric`, `float8_to_numeric`, `numeric_to_float8`/`_float4`, `numericvar_to_int32`/`_int128` | `convert::*` | present | conversion cores; the `int8_numeric`/`numeric_float8`/… fmgr wrappers are these cores |

The fmgr-only SQL wrappers represented by the above cores (no own logic):
`int2_numeric`/`int4_numeric`/`int8_numeric`/`float4_numeric`/`float8_numeric`,
`numeric_int2`/`_int4`/`_int8`/`_float4`/`_float8`/`_pg_lsn`, `numeric_larger`/`_smaller`
(via `numeric_max`/`numeric_min`), `numeric_var_pop`/`_var_samp`/`_stddev_pop`/`_stddev_samp`
and the `numeric_poly_*` / `int*_avg_*` / `int*_sum` / `*_avg_serialize`/`deserialize`
transition wrappers (cores present in `aggregate`), `numeric_min_scale`/`numeric_fac`
(cores in `ops_sql`). These pass as fmgr-boundary MATCH.

## Findings (FAIL)

### Fixed this round (now MATCH)

1. **`in_range_numeric_numeric` (numeric.c:2681)** — was MISSING. The window-function
   `RANGE` offset predicate (negative/NaN-offset error, NaN/±Inf val+base semantics,
   `base ± offset` compute-and-compare) had no in-crate counterpart. Restored in
   `ops_sql::in_range_numeric_numeric`; SQLSTATE
   `ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE` preserved.
2. **`numeric_normalize` (numeric.c:1026)** — was MISSING (canonical decimal string
   for hash-partition pruning). Restored in `ops_sql::numeric_normalize`
   (special-string handling + trailing-fraction-zero/decimal-point trim).
3. **`numeric_cmp_abbrev` (numeric.c:2322)** — was MISSING. Restored in
   `aggregate::numeric_cmp_abbrev` (intentionally-reversed i64 abbrev compare).
4. **`numeric_abbrev_abort` (numeric.c:2233)** — was MISSING. Restored in
   `aggregate::numeric_abbrev_abort`; the 10k/100k cardinality thresholds and the
   `estimating` toggle are in-crate, the HyperLogLog estimate read is delegated to a
   new OUTBOUND seam `numeric_abbrev_estimate` (owner: the sort-support/HLL setup that
   attaches `hyperLogLogState` to `ssup_extra`, same as the existing
   `numeric_abbrev_add_sample` seam). `trace_sort` LOG elogs elided (diagnostic only).

### Resolved this round (the 6 previously-MISSING infra-blocked functions)

All six now carry their full own logic in-crate. Where the C body manipulates a
data structure or call frame owned by a still-unported neighbor (the planner
`Node` vocabulary, the SRF `FuncCallContext`/multi-call context, the tuplesort
abbreviation slots), only the call OUT crosses a thin seam to that named owner —
the exact pattern the ported `backend-utils-adt-rangetypes`
`range_planner_support` family uses (inherited `PlannerNode` opacity + inline
`seam_core::seam!` declarations). Per the audit rule and the task brief, a
function fully written but whose cross-unit callee is an unported owner reached
through a seam is **SEAMED**, not MISSING.

| # | C function | C loc | Port | Verdict | Note |
|---|---|---|---|---|---|
| 1 | `numeric_sortsupport` | 2130 | `aggregate::numeric_sortsupport` | MATCH/SEAMED | sets `ssup.comparator` (= `numeric_fast_cmp`); when `ssup.abbreviate`, seeds an in-crate `NumericSortSupport { input_count: 0, estimating: true }` and routes the slot install + `ssup_extra`/HLL allocation (`VARATT_SHORT_MAX+VARHDRSZ+1` buf, `initHyperLogLog(...,10)`, `abbrev_full_comparator=comparator; comparator=numeric_cmp_abbrev; abbrev_converter=numeric_abbrev_convert; abbrev_abort=numeric_abbrev_abort`) OUT via inline seams `install_numeric_comparator` / `install_numeric_abbrev` (owner: the tuplesort abbreviation machinery; the trimmed real `types_sortsupport::SortSupportData` carries only `comparator`). |
| 2 | `numeric_support` | 1195 | `series_srf::numeric_support` | MATCH/SEAMED | full `SupportRequestSimplify` length-coercion no-op detection: `lsecond(expr->args)` Const-and-not-null guard, `exprTypmod(source)`, `DatumGetInt32` of the const typmod, the `is_valid_numeric_typmod`/scale-unchanged/precision-non-decreasing predicate, `relabel_to_typmod`. Scale/precision/validity helpers are in-crate (`types_numeric`); the `Node`/`FuncExpr`/`Const`/`exprTypmod`/`relabel_to_typmod` primitives route OUT over inherited `PlannerNode` opacity to the optimizer/nodeFuncs owners. |
| 3 | `generate_series_numeric` / `generate_series_step_numeric` | 1701 / 1708 | `series_srf::generate_series_numeric` / `generate_series_step_numeric` | MATCH/SEAMED | all own logic present: start/stop/step NaN+infinity rejection (`ERRCODE_INVALID_PARAMETER_VALUE`, same messages), zero-step rejection, the `GenerateSeriesNumericFctx` (current/stop/step) cross-call state seeded by value-copy from the args, the per-call step-sign termination (`step.sign==POS && current<=stop` / `step.sign==NEG && current>=stop`), `make_result(current)` emit, `add_var(current, step)` advance. The SRF protocol (`SRF_IS_FIRSTCALL`/`SRF_FIRSTCALL_INIT`/`SRF_PERCALL_SETUP`/`user_fctx` stash/fetch/`SRF_RETURN_NEXT`/`SRF_RETURN_DONE`) routes OUT over a `SrfCallFrame` token to the funcapi SRF / executor multi-call-context owner — which is itself trimmed-shape-blocked (`backend-utils-fmgr-funcapi` audit items 1–5 panic on the missing `flinfo`/`fn_extra`/`econtext`). |
| 4 | `generate_series_numeric_support` | 1834 | `series_srf::generate_series_numeric_support` | MATCH/SEAMED | full `SupportRequestRows` estimator: `is_funcclause` paranoia, `estimate_expression_value` of up-to-3 args, the all-NULL→0-rows / all-const→compute branches, NaN/infinity bail, `step` default 1, the `floor((stop-start)/step)+1` (`sub_var` then `div_var(...,0,false,false)` for explicit step / `trunc_var(...,0)` for step=1) and the step-sign-vs-(stop-start)-sign mismatch → 0 rows, `numericvar_to_double_no_overflow(res)+1`. Numeric arithmetic in-crate; the `Node`/`Const`/`estimate_expression_value`/row-count store route OUT over `PlannerNode`. |
| 5 | `random_numeric` / `random_var` | 4347 / 11681 | `random::random_numeric` / `random::random_var` | **MATCH** (fully in-crate, no seam) | NaN/infinity bound rejection; `rscale = max(dscale)`; `rlen = rmax - rmin` with the negative-range error and the empty-range (`rlen.ndigits()==0`) early return; the rejection sampler: `res_ndigits`, `pow10 = 10^((rscale+DEC_DIGITS-1)/DEC_DIGITS*DEC_DIGITS - rscale)`, the 4-NBASE-digit `rlen64` build, the per-iteration `alloc_var` + first-`rlen64_ndigits`-digits draw (pow10-multiple when whole), groups-of-4 whole-digit fill, remaining whole digits, final partial pow10-multiple digit, `strip_var`, `cmp_var(result,rlen)<=0` accept, `add_var(result, rmin)` offset. The PRNG is the **real merged `pg_prng::PgPrng`** (`pg_prng_uint64_range` = `PgPrng::u64_range`, verified inclusive-on-both-ends against `pg_prng.c`), threaded in by value as the C `pg_prng_state *state`. Covered by 4 new in-crate tests (in-range + scale, equal bounds, inverted-range error, NaN/Inf error). |

## Seam audit

- Owned seam crate: `backend-utils-adt-numeric-seams`. Declarations: `numeric_eq`,
  `numeric_cmp`, `numeric_maximum_size`, `numeric_subdiff` (INBOUND — this unit owns
  and installs them) plus `numeric_abbrev_add_sample` and `numeric_abbrev_estimate`
  (OUTBOUND — owned by the sort-support/HLL setup, called by this unit, not installed
  here).
- `init_seams()` installs exactly the 4 INBOUND seams (`io::seam_numeric_eq`,
  `io::seam_numeric_cmp`, `ops_sql::seam_numeric_maximum_size`,
  `ops_sql::seam_numeric_subdiff`) — nothing but `set()` calls. **Unchanged this
  round; still correct.** The 6 resolved functions introduced NO new INBOUND
  seam and NO new install flag.
- `seams-init::init_all()` calls `backend_utils_adt_numeric::init_seams()`.
  Unchanged.
- `recurrence_guard::every_seam_installing_crate_is_wired_into_init_all`: **passes**.
- `recurrence_guard::every_declared_seam_is_installed_by_its_owner`: red ONLY on
  the pre-existing `backend_postmaster_syslogger::logging_collector` and
  `backend_storage_ipc_pmsignal::set_postmaster_death_watch_cloexec` offenders —
  verified identical on `main` 54d169a2 (NOT numeric's, not introduced here).

### New OUTBOUND seams (all inline `seam_core::seam!`, owner unported → never
trip the guard, which only scans `crates/*-seams`)

This round added inline outbound seams in `aggregate.rs` and `series_srf.rs`,
each owned by a still-unported neighbor and panicking until that owner lands —
the sanctioned mirror-PG-and-panic for a cross-unit call into an unported owner,
identical in shape to rangetypes' planner-support seams:

- **tuplesort abbreviation owner:** `install_numeric_comparator`,
  `install_numeric_abbrev` (the `SortSupport`-slot installers, over the real
  `types_sortsupport::SortSupportData`).
- **SRF / executor multi-call owner (funcapi.c + ExprContext/fmgr frame):**
  `srf_is_firstcall`, `srf_firstcall_init`, `srf_percall_setup`,
  `srf_set_user_fctx`, `srf_get_user_fctx`, `srf_return_next`, `srf_return_done`,
  `numeric_image_to_datum`.
- **optimizer / nodeFuncs / makefuncs owners (inherited `PlannerNode` opacity):**
  `is_support_request_simplify`, `is_support_request_rows`,
  `support_request_simplify_fcall`, `support_request_rows_root`,
  `support_request_rows_node`, `support_request_rows_set`, `is_funcclause`,
  `func_expr_nargs`, `func_expr_arg`, `estimate_expression_value`, `is_const`,
  `const_is_null`, `const_value`, `expr_typmod`, `relabel_to_typmod`.

Each is a thin marshal+delegate (no branching/computation in the seam path); the
decision logic lives in this crate's bodies (above). The pre-existing OUTBOUND
HLL seams (`numeric_abbrev_add_sample`, `numeric_abbrev_estimate`) are unchanged.

## Residual todo!()/unimplemented!()

None in own logic (grep clean across all 8 family files + keystone + seams
crate). No `todo!()` / `unimplemented!()` introduced; the only `panic!`s reached
are the uninstalled-seam panics on the unported-owner call paths above.

## Gate

- `cargo check --workspace`: clean (only pre-existing warnings in unrelated
  crates, e.g. `backend-access-common-printtup`). The numeric crate (8 family
  files) compiles warning-free.
- `cargo test --workspace`: numeric + types-numeric + pg-prng + types-sortsupport
  + backend-utils-sort-sortsupport + backend-utils-fmgr-funcapi all green; the 4
  new `random` tests pass; `seams-init` recurrence guards as above (signal/timeout
  flakes ignored per instructions).

## Conclusion

**PASS.** All 211 `numeric.c` functions are present and faithful. The six
previously-MISSING infra-blocked functions now carry their full own logic
in-crate (`numeric_sortsupport`, `numeric_support`,
`generate_series_numeric`/`_step_numeric`, `generate_series_numeric_support`,
`random_numeric`/`random_var`); `random_*` is fully self-contained over the real
`pg_prng`, the other four are SEAMED into their named unported owners (tuplesort
abbreviation / funcapi SRF / planner nodes) per the rangetypes precedent — none
is a body-replaced-by-a-seam MISSING. CATALOG is set to `audited`.
