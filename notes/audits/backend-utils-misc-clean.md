# Audit: backend-utils-misc-clean

- Date: 2026-06-13
- Model: Opus 4.8 (1M context) — model id `claude-opus-4-8[1m]`
- Branch: `port/backend-utils-misc-clean`
- Auditor: independent (re-derived from C + c2rust + PG headers; did not trust
  the port's own self-review, its comments, or the prior audit revision)

## Top-line verdict: PASS

All 20 functions across the four C files are MATCH or correctly delegated. The
previously-failing Finding 1 (help-config reset columns) is **resolved**: the
fix commit `54914485` makes BOOL/INT/REAL reset columns reproduce the
zero-initialized `reset_val` that C's `printMixedStruct` actually prints from
the `--describe-config` entry point. No seam findings. No design-conformance
findings.

## Scope

Unit C sources (CATALOG.tsv, under `../pgrust/postgres-18.3/`):
`*/help_config.c`, `*/pg_rusage.c`, `*/queryenvironment.c`, `*/sampling.c`
(all in `src/backend/utils/misc/`).

Crates (by C-source coverage):
- `help_config.c` -> `backend-utils-misc-help-config` (added this change)
- `pg_rusage.c` -> `backend-utils-misc-pg-rusage` (pre-existing on main)
- `queryenvironment.c` -> `backend-utils-misc-queryenvironment` (pre-existing)
- `sampling.c` -> `backend-utils-misc-sampling` (pre-existing)

c2rust reference: `../pgrust/c2rust-runs/backend-utils-misc-clean/src/{help_config,pg_rusage,queryenvironment,sampling}.rs`.

Function inventory enumerated directly from the four C files (20 definitions
total: 3 + 2 + 6 + 9). Every definition has a row below.

## Per-function table

| C function (file:line) | Port location | Verdict | Notes |
|---|---|---|---|
| `GucInfoMain` (help_config.c:46) | help-config lib.rs:61 + render_guc_info:67 + guc_info_rows:91 | MATCH | C prints every visible GUC then exit(0); port returns rendered text (no palloc in C, owned String is faithful). Visible filter + per-row render preserved. Sort-by-name mirrors `build_guc_variables` ordering — same set/order. |
| `displayStruct` (help_config.c:73) | help-config lib.rs:100 | MATCH | `!(flags & (GUC_NO_SHOW_ALL\|GUC_NOT_IN_SAMPLE\|GUC_DISALLOW_IN_FILE))`; flag constants from `types_guc`, mask exact. |
| `printMixedStruct` (help_config.c:86) | help-config lib.rs:107 + value_columns:167 | MATCH | Column count/order, types, %g -0 normalization, NULL-desc -> "" all match. Reset columns now correct — see Resolution below. |
| `config_enum_lookup_by_value` (guc.c:3023, called from printMixedStruct) | help-config lib.rs:140 | MATCH | val-match over `options.entries()`, `elog(ERROR, "could not find enum option %d for %s")` -> Err. External enum-option arrays panic until owner installs (mirror-PG-and-panic). In-crate helper, not a seam. |
| `pg_rusage_init` (pg_rusage.c:27) | pg-rusage lib.rs:101 | MATCH | getrusage(RUSAGE_SELF)+gettimeofday; OS-failure status ignored as in C. |
| `pg_rusage_show` (pg_rusage.c:40) | pg-rusage lib.rs:112 + show_between:121 + PgRUsageDelta::between:155 + elapsed_pair:173 | MATCH | Per-field borrow-a-second usec fixup, `(int)`-narrowed differences, `/10000` after the int cast, `%d.%02d` format verbatim. Owned String instead of the non-reentrant `static char[100]` (faithful; no palloc). |
| `create_queryEnv` (queryenvironment.c:38) | queryenvironment lib.rs:41 | MATCH | palloc0 -> `QueryEnvironment::new_in(mcx)`; empty list. |
| `get_visible_ENR_metadata` (queryenvironment.c:44) | queryenvironment lib.rs:50 | MATCH | NULL env -> None; else `get_ENR` then `&enr->md`. Assert(refname!=NULL) is `&str`. |
| `register_ENR` (queryenvironment.c:68) | queryenvironment lib.rs:66 | MATCH | dup-check Assert -> debug_assert; lappend -> try_reserve+push, OOM -> ereport (lappend can ereport, hence PgResult). |
| `unregister_ENR` (queryenvironment.c:81) | queryenvironment lib.rs:93 | MATCH | match-then-list_delete -> position-then-remove. |
| `get_ENR` (queryenvironment.c:95) | queryenvironment lib.rs:104 + enr_index:114 | MATCH | foreach + strcmp(name)==0 -> iter position by name; None on miss. |
| `ENRMetadataGetTupDesc` (queryenvironment.c:124) | queryenvironment lib.rs:148 | MATCH | XOR Assert preserved; tupdesc path borrows; catalog path table_open(NoLock)/rd_att/table_close(NoLock) direct dep (not seamed). rd_att cloned into mcx for ownership; returned descriptor content identical. |
| `BlockSampler_Init` (sampling.c:38) | sampling lib.rs:56 | MATCH | N/n/t/m set; seed; `Min(n,N)` in BlockNumber(uint32) space matching C's implicit promotion. |
| `BlockSampler_HasMore` (sampling.c:57) | sampling lib.rs:77 | MATCH | `(t<N)&&(m<n)`. |
| `BlockSampler_Next` (sampling.c:63) | sampling lib.rs:81 | MATCH | K=N-t (wrapping), k=n-m, `(k as uint32)>=K` shortcut, single-fract Algorithm-S loop `while V<p { t++; K--; p*=1-k/K }`, select+return t++. Wrapping arithmetic matches C unsigned. |
| `reservoir_init_selection_state` (sampling.c:132) | sampling lib.rs:145 | MATCH | seed from global prng; `W = exp(-log(fract)/n)`. |
| `reservoir_get_next_S` (sampling.c:146) | sampling lib.rs:155 | MATCH | Re-derived in full. Algorithm X branch (t<=22n) and Algorithm Z branch transcribed exactly: floor(X), (6.3) lhs/rhs test, f(S)/cg(X) y-product loop (numer/denom decrement order equivalent — body multiplies with pre-decrement values, then `denom-=1; numer-=1`, matching C's body+for-step), W-in-advance, final accept test. `rs.W=W` only in the Z branch (X branch leaves it untouched), matching C. |
| `sampler_random_init_state` (sampling.c:233) | sampling lib.rs:223 | MATCH | `pg_prng_seed(state, (uint64) seed)`. |
| `sampler_random_fract` (sampling.c:240) | sampling lib.rs:228 | MATCH | `do { res = pg_prng_double } while res==0.0`. |
| `anl_random_fract` (sampling.c:265) | sampling lib.rs:263 + with_old_reservoir_state:288 | MATCH | first-use seed guard + fract. oldrs is per-backend `static` -> thread_local (not a shared static). |
| `anl_init_selection_state` (sampling.c:280) | sampling lib.rs:268 | MATCH | first-use seed guard + `exp(-log(fract)/n)`. |
| `anl_get_next_S` (sampling.c:295) | sampling lib.rs:273 | MATCH | `oldrs.W=*stateptr; reservoir_get_next_S; *stateptr=oldrs.W`. Correctly omits the init guard (C `anl_get_next_S` has none). |

Statics: the C `union mixedStruct` (help_config.c:30) -> `GucSetting` enum from
`backend-utils-misc-guc-tables` (no opacity invented). The C file-scope
`static ReservoirStateData oldrs` / `static bool oldrs_initialized`
(sampling.c:262-263) -> a single `thread_local!`
`RefCell<(ReservoirStateData, bool)>` — per-backend semantics preserved.

## Resolution of prior Finding 1 (help-config reset columns)

The prior revision FAILED because `value_columns` read `boot_val` for the
BOOL/INT/REAL reset column. Re-deriving from the C source settles what the
correct value is:

1. `GucInfoMain` is reached only from `main.c:220` (the `--describe-config`
   dispatch). It calls `build_guc_variables()` and nothing else before
   iterating and printing.
2. `build_guc_variables` (guc.c:903) only sets each option's `gen.vartype` and
   populates the GUC hash table. It never assigns `reset_val`. The runtime
   population of `reset_val` happens in `InitializeOneGUCOption`
   (guc.c:1673/1691/1709/...), which this dispatch path never reaches.
3. In `struct config_bool/int/real` (guc_tables.h:216/230/246), `reset_val` is
   a trailing "variable field, initialized at runtime" sitting after the
   constant fields. The static `ConfigureNames*` initializers (e.g.
   `enable_seqscan` at guc_tables.c:801: `{gen}, &var, true, NULL, NULL, NULL`)
   stop at the three hooks and leave `reset_val`/`reset_extra` unspecified — C
   zero-initializes them.

Therefore at `printMixedStruct` time `reset_val == 0` for every BOOL/INT/REAL
option regardless of `boot_val`, so C prints `FALSE` / `0` / `0` for those
reset columns. The c2rust rendering confirms it reads `_bool.reset_val` etc.

The fix (`value_columns`, lib.rs:167-203) now emits the constant
`"FALSE"` (BOOL), `0` (INT), `format_real(0.0)` (REAL) for the reset column,
while continuing to read the genuinely-constant `min`/`max` from the real
fields and `boot_val` for STRING/ENUM (which C does print from `boot_val`). The
`min: i32 / max: i32` and `min: f64 / max: f64` field types in
`backend-utils-misc-guc-tables` match the C widths. Verdict: MATCH.

The original prior-audit phrasing ("C prints reset_val, port must print
reset_val") was directionally incomplete; the fix's reasoning (reset_val is
zero at this point) is the precise account and the emitted output now matches C
byte-for-byte for these columns.

## Seam audit (step 3)

Ownership is by C-source coverage. Enumerated `crates/*-seams` for each owned C
file: **no owned seam crates exist** — there is no `*-help-config-seams`,
`*-pg-rusage-seams`, `*-queryenvironment-seams`, or `*-sampling-seams` crate
(confirmed by directory listing). No owned seam declarations to install, so no
empty-installer violation is possible.

- `backend-utils-misc-help-config`: no seams crate, no inward callers. Outward
  calls are `ereport` (direct dep on `backend-utils-error`) and reads of
  `backend-utils-misc-guc-tables` statics (acyclic).
  `config_enum_lookup_by_value` is an in-crate helper, not a seam. No finding.
- `backend-utils-misc-pg-rusage`: `init_seams()` empty by design (leaf; only OS
  calls). Wired in `seams-init` lib.rs:68. OK.
- `backend-utils-misc-queryenvironment`: `init_seams()` empty (owns no seams);
  `ENRMetadataGetTupDesc` calls `table_open`/`table_close` as a direct
  dependency, not across a seam. Wired in `seams-init` lib.rs:69. OK.
- `backend-utils-misc-sampling`: `init_seams()` empty (leaf). Wired in
  `seams-init` lib.rs:70. OK.

No `set()` outside an owner, no uninstalled seam, no computation/branching in a
seam path. Zero seam findings.

## Design conformance (step 3b)

- Opacity: `mixedStruct` union -> `GucSetting` enum (real, owner-defined; no
  invented handle). `QueryEnvironment` opaque struct -> real `Vec`-backed
  struct. No invented opacity. OK.
- Mcx/PgResult: `create_queryEnv`/`register_ENR`/`ENRMetadataGetTupDesc` take
  `Mcx` and return `PgResult` where they allocate or can ereport; help-config
  returns `PgResult<String>` and pg_rusage/sampling correctly stay infallible
  (no palloc). OK.
- Per-backend globals: sampling `oldrs`/`oldrs_initialized` -> `thread_local!`,
  not a shared static. OK.
- No ambient-global seams, no locks across `?`, no registry-shaped side tables,
  no unledgered divergence markers. The help-config reset-column behavior is now
  documented in the crate as the faithful C behavior (not an approximation).

## Validation

`cargo test -p backend-utils-misc-help-config`: 5 passed, 0 failed
(`renders_common_setting_full_line` asserts the exact tab-separated prefix;
`enum_lookup_uses_visible_option_name` exercises the enum boot_val path).

## Disposition

PASS. The CATALOG.tsv row may be marked `audited`. Every function MATCH; no
seam or design-conformance findings; the prior merge-blocking Finding 1 is
resolved and re-derived clean from scratch.
