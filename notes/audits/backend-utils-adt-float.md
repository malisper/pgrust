# Audit: backend-utils-adt-float (`utils/adt/float.c`)

Independent function-by-function audit against
`../pgrust/postgres-18.3/src/backend/utils/adt/float.c` (4167 LOC) and
`utils/float.h`, cross-checked with the c2rust rendering.

## Model reconciliation

The port copies the src-idiomatic base and reconciles it to this repo's model:
`types`/`seams`/`backend-utils-mctx` → `types-core`/`types-datum`/`types-error`/
`mcx`; the libm seam (`erf`/`erfc`/`tgamma`/`lgamma`) re-homed onto the
`backend-utils-adt-float-seams` crate as outward seams; the aggregate transition
array re-homed onto the directly-ported `backend-utils-adt-arrayfuncs`
(`construct_array` + `fetch_att`, `Mcx`/`&[u8]` model) instead of the old
`TransArrayHandle` opaque handle + central `agg` seams. The bare `Datum
fn(FunctionCallInfo)` registry wiring is deferred project-wide (Datum-redesign
lifetime gate), mirroring rangetypes/adt-formatting; every C `Datum fn` is
exposed as a typed core the fmgr boundary will wrap 1:1.

## Function inventory & verdicts

`float.h` inline cores (consumed via the C `Datum` wrappers):

| C fn | port | verdict |
|---|---|---|
| float_overflow_error / underflow / zero_divide_error (85/93/101) | lib.rs | MATCH (msg + SQLSTATE 22003/22003/22012) |
| is_infinite (118) | lib.rs | MATCH |
| FLOAT{4,8}_FITS_IN_INT{16,32,64} (c.h) | lib.rs | MATCH (PG_INTnn_MIN powers-of-two bounds) |
| float{4,8}_{pl,mi,mul,div} (float.h 160-271) | lib.rs | MATCH (over/underflow + zero-divide predicates verbatim) |
| float{4,8}_{eq,ne,lt,le,gt,ge,min,max} (float.h 280-379) | lib.rs | MATCH (NaN-aware) |
| float{4,8}_cmp_internal (816/910) | lib.rs | MATCH |

`float.c` functions:

| C fn (line) | port | verdict |
|---|---|---|
| float4in (164) | io::float4in | MATCH (thin wrapper over float4in_internal; "real") |
| float4in_internal (183) | io::float4in_internal | MATCH (strtof semantics; hex-float; NaN/Inf spellings; ERANGE 22003; endptr; junk 22P02) |
| float4out (319) | io::float4out / float4out_with | MATCH (extra_float_digits as param; ryu vs %.*g) |
| float4recv (339) | io::float4recv | MATCH (pq_getmsgfloat4; short-buf 08P01) |
| float4send (350) | io::float4send | MATCH (big-endian IEEE bits) |
| float8in (364) | io::float8in | MATCH |
| float8in_internal (395) | io::float8in_internal | MATCH (strtod; ERANGE val==0\|\|>=HUGE_VAL ↔ is_infinite\|\|(0&&nonzero); special spellings order; endptr/junk) |
| float8out (522) | io::float8out | MATCH |
| float8out_internal (536) | io::float8out_internal / _with | MATCH |
| float8recv (556) | io::float8recv | MATCH |
| float8send (567) | io::float8send | MATCH |
| float{4,8}{abs,um,up,larger,smaller} (591-712) | lib.rs | MATCH |
| float{4,8}{pl,mi,mul,div} Datum (728-797) | — | deferred fmgr wrapper over the float.h cores (cores: MATCH) |
| float{4,8}{eq,ne,lt,le,gt,ge} Datum (826-965) | — | deferred fmgr wrapper over NaN-aware cores (cores: MATCH) |
| btfloat4cmp (880) | lib::btfloat4cmp | MATCH |
| btfloat4fastcmp (889) | lib::btfloat4fastcmp | MATCH (SortSupport comparator core) |
| btfloat4sortsupport (898) | btfloat4fastcmp exposed | SEAMED-equiv: the entry only assigns `ssup->comparator`; SortSupport struct wiring is the deferred fmgr boundary, comparator logic ported |
| btfloat8cmp (974) | lib::btfloat8cmp | MATCH |
| btfloat8fastcmp (983) | lib::btfloat8fastcmp | MATCH |
| btfloat8sortsupport (992) | btfloat8fastcmp exposed | SEAMED-equiv (as btfloat4sortsupport) |
| btfloat48cmp / btfloat84cmp (1001/1011) | lib.rs | MATCH (widen f4→f8) |
| in_range_float8_float8 (1027) | funcs.rs | MATCH (offset NaN/<0 → 22013; NaN val/base; inf-offset+inf-base short-circuit; sum compare) |
| in_range_float4_float8 (1103) | funcs.rs | MATCH (base widened to f8 after the NaN checks) |
| ftod/dtof/dtoi4/dtoi2/i4tod/i2tod/ftoi4/ftoi2/i4tof/i2tof (1183-1350) | funcs.rs | MATCH (rint=round_ties_even; FITS range checks; "integer/smallint out of range" 22003) |
| dround/dceil/dfloor/dsign/dtrunc (1368-1428) | funcs.rs | MATCH (dsign NaN→0 via else; dtrunc toward-zero) |
| dsqrt (1446) | funcs::dsqrt | MATCH (neg→2201F; over/underflow) |
| dcbrt (1470) | funcs::dcbrt | MATCH |
| dpow (1489) | funcs::dpow | MATCH (NaN^0/1^NaN; 0^-/neg^non-int 2201F; inf y then inf x; finite result-inspection incl glibc-bug branch) |
| dexp (1644) | funcs::dexp | MATCH |
| dlog1/dlog10 (1690/1722) | funcs.rs | MATCH (zero/neg → 2201E; over/underflow w/ arg!=1) |
| dacos/dasin/datan/datan2/dcos/dcot/dsin/dtan (1755-1965) | funcs.rs | MATCH (NaN passthrough; domain/inf checks; overflow) |
| init_degree_constants (2019) | funcs::degree_consts (OnceLock) | MATCH (runtime-computed; tan_45/cot_45 via sind/cosd_q1) |
| asind_q1/acosd_q1 (2048/2081) | funcs.rs | MATCH |
| dacosd/dasind/datand/datan2d (2108-2214) | funcs.rs | MATCH |
| sind_0_to_30/cosd_0_to_60/sind_q1/cosd_q1 (2252-2299) | funcs.rs | MATCH |
| dcosd (2318) | funcs::dcosd | MATCH (one sign flip at >90; cos even) |
| dcotd (2373) | funcs::dcotd | MATCH (3 sign flips; +0 normalize) |
| dsind (2439) | funcs::dsind | MATCH (sign flips at <0 and >180) |
| dtand (2495) | funcs::dtand | MATCH (3 sign flips; +0 normalize) |
| degrees (2561) | funcs::degrees | MATCH (float8_div by RADIANS_PER_DEGREE) |
| dpi (2573) | funcs::dpi | MATCH |
| radians (2583) | funcs::radians | MATCH (float8_mul) |
| dsinh/dcosh/dtanh/dasinh/dacosh/datanh (2598-2714) | funcs.rs | MATCH (dcosh underflow guard; dacosh domain >=1; datanh endpoints→±Inf) |
| derf/derfc (2752/2772) | funcs.rs | MATCH (libm erf/erfc via float-seams; inf→overflow) |
| dgamma/dlgamma (2796/2850) | funcs.rs | MATCH (tgamma/lgamma via float-seams; NaN/inf/zero handling) |
| check_float8_array (2927) | aggregates::check_float8_array | MATCH (NDIM!=1\|\|DIMS[0]!=n\|\|HASNULL\|\|ELEMTYPE!=FLOAT8OID → "%s: expected %d-element float8 array"; direct fetch_att walk) |
| float8_combine (2951) | aggregates::float8_combine | MATCH (Youngs-Cramer combine; overflow) |
| float8_accum (3043) | aggregates::float8_accum | MATCH (kernel identical; always-construct == AggCheckCallContext both legs' final values) |
| float4_accum (3124) | aggregates::float4_accum | MATCH (widen f4→f8) |
| float8_avg/var_pop/var_samp/stddev_pop/stddev_samp (3207-3293) | aggregates.rs | MATCH (N==0/<=1 NULL gates) |
| float8_regr_accum (3336) | aggregates::float8_regr_accum | MATCH (Y=arg1, X=arg2; 6-elem kernel; full overflow predicate) |
| float8_regr_combine (3458) | aggregates.rs | MATCH |
| float8_regr_{sxx,syy,sxy,avgx,avgy} (3590-3672) | aggregates.rs | MATCH (N<1 NULL) |
| float8_covar_pop/covar_samp/corr (3691-3729) | aggregates.rs | MATCH (N gates; corr horiz/vert NULL) |
| float8_regr_r2/slope/intercept (3758-3818) | aggregates.rs | MATCH (vertical-line NULL; r2 horiz→1.0) |
| float48{pl,mi,mul,div} / float84{pl,mi,mul,div} (3862-3931) | funcs.rs | MATCH (widen + float8 core) |
| float48{eq..ge} / float84{eq..ge} (3949-4051) | funcs.rs | MATCH (widen + NaN-aware float8 cmp) |
| width_bucket_float8 (4074) | funcs::width_bucket_float8 | MATCH (count<=0/NaN/inf bounds → 2201G; pg_add_s32_overflow → checked_add → 22003; quotient `as i32` truncation; >=count clamp; +1) |

No function MISSING, PARTIAL, or DIVERGES.

## Seams & wiring

- Owned inward seams (`backend-utils-adt-float-seams`): `float8_mul`, `float8_div`
  — both installed by `init_seams()` (`set()` only), wired into
  `seams-init::init_all()`. Consumed by `backend-utils-adt-cash`.
- Outward libm seams added to the same crate: `erf`/`erfc`/`tgamma`/`lgamma`.
  No PostgreSQL owner crate exists (these are C-stdlib `<math.h>` not in Rust
  `std`); they are mirror-pg-and-panic until a `common-libm` provider lands.
  The owner crate itself calls them (`funcs.rs`), so the install-guard's
  outward-seam exclusion applies — not the float crate's inward contract.
- No CONTRACT_RECONCILE_PENDING allowlist entry existed for float8_mul/div (no
  prior owner); none removed, none added.
- `arrayfuncs` is a direct dependency (no cycle: arrayfuncs does not depend on
  float). No seam, no logic in any wiring path.

## Design conformance

- Allocating functions take `Mcx<'mcx>` and return `PgResult` (the aggregate
  transition builders; out functions return owned `String` per the repo text
  idiom). No ambient context.
- No invented opacity: the transition value is real array bytes (`&[u8]` /
  `PgVec<'mcx,u8>`), not a handle/token. The old `TransArrayHandle` opaque type
  was eliminated.
- No shared statics for per-backend globals; the degree constants are a process
  `OnceLock` of compile-invariant IEEE values (matching C's run-once globals).
- No locks, no registry side tables, no ambient-global seams.

## Verdict: PASS
