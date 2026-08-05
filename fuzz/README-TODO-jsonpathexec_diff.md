# jsonpathexec_diff (crates/backend/utils/adt/jsonpath_exec) — lane p1-laneaa

Differential fuzz target: shipped Rust `adt_jsonpath_exec` (path-eval core
`executeJsonPath` port + fc_ wrappers) vs vendored PostgreSQL 18.3 C.
Oracle pin: Stamp-18.3, upstream sha
`62d6c7d3df6287f1bd83199c1a746e50d31571a0` — never `:latest`, never 18.4.
Method doc: `.claude/skills/fuzzuproof-crate/SKILL.md`. Sibling target of
record: `jsonpath_diff` (same csrc family, same conventions).

Functions covered (arm map in `core/src/jsonpathexec_diff.rs`):

| function | oid | arm | wrapper plane |
|---|---|---|---|
| `jsonb_path_exists` (+`_tz` 1177) | 4005 | 0 | `fc_jsonb_path_exists`(`_tz`) |
| `jsonb_path_match` (+`_tz` 2030) | 4009 | 1 | `fc_jsonb_path_match`(`_tz`) |
| `jsonb_path_query_array` (+`_tz` 1180) | 4007 | 2 | `fc_jsonb_path_query_array`(`_tz`) |
| `jsonb_path_query_first` (+`_tz` 2023) | 4008 | 3 | `fc_jsonb_path_query_first`(`_tz`) |
| `jsonb_path_exists_opr` (@?) | 4010 | 4 | `fc_jsonb_path_exists_opr` |
| `jsonb_path_match_opr` (@@) | 4011 | 5 | `fc_jsonb_path_match_opr` |
| `jsonb_path_query_core` rows (SRF collection core of 4006/1179) | — | 6 | (SRF plumbing carved) |

## Input-strategy decision (recorded per the charter)

The driver parses the JSON document and vars ONCE with the shipped Rust
`adt_jsonb` crate and feeds the identical jsonb IMAGE BYTES to both
engines (C `JsonbContainer` layout is byte-identical by design). Chosen
over vendoring C jsonb_in/json.c/jsonapi.c because it removes an entire
parser call graph from the oracle and keeps this differential focused on
path-eval. **Doc-parse/serialize parity is out of scope here — it is the
cross-crate adt/jsonb plane owned by lane p1-lanev**; a divergence there
would be triaged to that lane (recorded, not fixed). The image invariant
is exercised on every iteration: both engines walk the same bytes with
independent container readers, so a layout misinterpretation surfaces as
a result-plane divergence. The jsonpath image is likewise parsed on both
sides (verdict+image must agree, else skip — the parse plane is
jsonpath_diff's covered plane) and the agreed image is fed to both.

## Oracle provenance (fuzz/core/csrc/jsonpath/, extending jsonpath_diff's)

VERBATIM whole files:

- `jsonpath_exec.c` — `src/backend/utils/adt/jsonpath_exec.c`
- `jsonb_util.c` — `src/backend/utils/adt/jsonb_util.c`
- `regex/regexec.c` (+ its `#include`d `regex/rege_dfa.c`) — `src/backend/regex/`
- `include/utils/jsonb.h` — verbatim header (replaces the old minimal shim)
- `pg_qsort_arg.c` — `src/port/qsort_arg.c` + `include/lib/sort_template.h`
  verbatim (jsonb object-pair sort; the comparator is total, but the exact
  sort is vendored anyway)

EXTRACTED VERBATIM (markers emitted by `extract_verbatim.py`):

- `pg_numeric_min.c` additions — numeric_{add,sub,mul,div,mod}_opt_error,
  numeric_{cmp,eq,abs,ceil,floor,trunc,is_nan,is_inf},
  numeric_int4/int8_opt_error, int64_to_numeric, int2/int4/int8_numeric,
  float4/float8_numeric, numerictypmodin, cmp_numerics/cmp_var(_common),
  sub_var, div_var(+div_var_int, div_var_int64), select_div_scale,
  mod_var, div_mod_var, ceil_var, floor_var, numericvar_to_int32/int64,
  set_var_from_num, numeric_sign_internal, common/int.h s64-overflow
  helpers.
- `pg_jsonb_min.c` (new) — jsonb.c JsonbExtractScalar +
  Jsonb(Container)TypeName; varlena.c cstring_to_text(_with_len),
  text_to_cstring, varstr_cmp, check_collation_set; bool.c
  parse_bool(_with_len); int.c int4in; int8.c int8in; float.c
  float8in_internal; regexp.c RE_compile_and_execute chain incl. the
  compiled-regex cache (`cached_re_str` + `RE_compile_and_cache` +
  `RE_wchar_execute` + `RE_execute` verbatim; the cache statics are
  declared `_Thread_local` here and `pg_jsonpath_regex_cache_reset()`
  zeroes the index at every pg_diff entry because the cache memory lives
  in the per-entry TLS arena — thread-locality + per-entry reset are the
  only deviations, both environment not logic).
- `pg_support_min.c` additions — numutils.c pg_strtoint64(_safe),
  pg_ltoa, pg_ultoa_n, DIGIT_TABLE/decimalLength32, int.h
  pg_neg_u64_overflow.

SHIMS (environment only; every group documented in
`pg_jsonpath_exec_env.c`'s header):

- driver entries `pg_diff_jsonb_path_{exists,match,query_array,
  query_first,query_items}` routing through the VERBATIM fmgr wrappers;
- DATETIME CARVE SENTINELS — parse_datetime, JsonEncodeDateTime,
  session_timezone, timestamp2tm, j2date, DetermineTimeZoneOffset,
  AdjustTime(stamp)ForTypmod, anytime(stamp)_typmod_check, all
  date_/time_/timetz_/timestamp_/timestamptz_ cmp+conversion fmgr fns:
  LOUD abort() stubs that must never fire (the driver filters datetime
  paths on both engines);
- executor/SRF/hash stubs (JSON_TABLE, ExecEvalExpr, init/per_MultiFuncCall,
  jsonb_in's executor JSONOID arm, GIN/hash opclass entries,
  list_delete_first's SRF-only use, pg_strncoll under the C-collation
  pin): LOUD abort() stubs, unreachable-by-construction;
- MemoryContext tokens over the TLS arena (no-op switch/create/delete;
  per-entry arena reset + regex-cache reset);
- `construct_array_builtin`/`ArrayGetIntegerTypmods` 2-element CSTRING
  typmod round trip for `.decimal(p,s)` (conversion via VERBATIM
  pg_strtoint32, exactly arrayutils.c's behavior);
- `pg_server_to_any` same-encoding identity under the UTF-8 pin (only
  called when server encoding != UTF-8, i.e. never here);
- `DirectInputFunctionCallSafe` mini-fmgr (real fmgr.c semantics incl.
  SOFT_ERROR_OCCURRED protocol) in the shim `fmgr.h`;
- shim headers for compile surface only: funcapi.h (SRF macros over
  abort stubs), nodes/execnodes.h + primnodes.h SQL/JSON + JsonTable
  node shapes (verbatim where marked), utils/date.h / datetime.h /
  timestamp.h / float.h (verbatim typedef/constant/inline shapes,
  carve-stub decls), common/hashfn.h, port/pg_bitutils.h,
  utils/array(_model).h, utils/varlena.h, lib/sort_template.h, c.h.
- ERRCODE_* additions generated MECHANICALLY from
  `backend/utils/errcodes.txt` (a hand-written first draft had 4 wrong
  sqlstates — never write sqlstates from memory).

Symbol isolation: all new generic-named exports added to
`JSONPATH_SHARED_SYMS` in `core/build.rs` (jporcl_ prefix), including the
two that already collide across families today (float8in_internal in
pg_float_io.c, hash_any in pg_mac_io.c).

## Carve-outs (documented per the skill's rules)

- **DATETIME METHOD FAMILY** (ruled in the claim): `.datetime()`,
  `.date()`, `.time()`, `.time_tz()`, `.timestamp()`, `.timestamp_tz()`
  and jbvDatetime comparison paths (session-timezone state; lib.rs
  execute_datetime_method / compare_datetime / session_tz_offset /
  encode_datetime). Implemented at the DRIVER level by walking the
  PARSED item tree (`path_has_datetime_item`, mirroring the printer's
  child visits) — not text scanning, so keys named "datetime" stay in
  domain. Both engines skip; the C sentinels abort if one leaks.
- **SRF/MultiFuncCall plumbing** of jsonb_path_query (fc wrapper only;
  the pure row-collection core `jsonb_path_query_core` IS covered, arm 6
  vs a C entry that serializes the same per-item images through VERBATIM
  getIthJsonbValueFromContainer + JsonbValueToJsonb).
- **json_table.rs** entirely (ruled in the claim).
- stack-depth (54001), message text, invalid-UTF-8/interior-NUL inputs:
  same rationale as jsonpath_diff.
- path-parse plane: owned by jsonpath_diff (both-parse-ok gate here).

## Status

- [x] 1. C oracle vendored; whole family compiles + links
      (commit 173aaf5ccd).
- [x] 2. Rust driver: 7 arms, planes = result verdict (true/false/NULL/
      hard) + sqlstate + result-image bytes (byte-exact) + per-row images
      (arm 6) + fc-wrapper plane (11 of 12 catalog wrappers; the SRF
      wrapper pair is the documented carve). Registered in
      fuzz/Cargo.toml, core/Cargo.toml, core/src/lib.rs; `cargo check`
      clean.
- [x] 3. Seeds: `fuzz/gen_seeds_jsonpathexec.py` — 720 (path, doc[, vars])
      pairs harvested mechanically from regress `jsonb_jsonpath.sql`
      (`jsonb_path_*` calls + `@?`/`@@` expressions), rotated across
      arms/flags, + witness pairs (single-dimension deltas: array index /
      one doc leaf / silent flip / lax-vs-strict / vars present-vs-absent)
      = 866 mechanical seeds; dictionary
      `fuzz/jsonpathexec_diff.dict` = superset of jsonpath_diff's + json
      doc tokens.
- [x] 4a. Units: `cargo test --manifest-path core/Cargo.toml jsonpathexec`
      7/7 green (regress matrix over all arms × silent × tz × vars;
      error shapes incl. 22012 div-by-zero, numeric overflow,
      .double() over strings/'inf'/'NaN'/1e400, .decimal() typmod range,
      .keyvalue(), variables present/missing/non-object/null, @?/@@
      NULL semantics, like_regex flags i/s/m/x/q + invalid pattern,
      strict/lax structural errors, deep nesting within caps; datetime
      carve filter positive AND negative cases; witness pairs; full
      corpus replay). Whole fuzz-core suite 93/93 (sibling setups made
      mutually tolerant of one-binary seam installs).
- [x] 4b. Local smokes (laptop, per the lane's hard rules; sancov on the
      C side too, dict, max_len=768): see "Smoke results".
- [ ] 5. FLEET CAMPAIGN (lane coordinator submits; >=10M execs or 24h
      CPU floor for any fuzz-only claim; one job per target).
- [ ] 6. Bookkeeping: routes rows (docs/verification/phase1-routes.tsv)
      flips for the covered lib.rs/builtins.rs functions, ledger rows,
      claim row on main (coordinator/lane owner).
- [ ] 7. Done-gate: coverage merge, rendered-red-line audit, trailing
      cargo-mutants, CI replay rail.

## Smoke results

Run 1 (2026-07-31, laptop, 100s, sancov both sides, dict, max_len=768,
seeded from the 866 mechanical seeds): **1,636,416 execs, ZERO
divergences** (first attempt died at link on list_delete_first — release
+ sancov keeps the SRF wrapper alive; fixed with the documented
unreachable stub, not a behavior change).

Run 2 (same recipe, 120s, over the grown corpus): **488,130 execs, ZERO
divergences**, final `cov: 15634 ft: 61927`, corpus 3780 in-run entries.
`cargo fuzz cmin` minimized the banked corpus to 3295 files carrying
**12,585 coverage edges / 52,719 features**; the 866 mechanical seeds
were re-applied on top (committed corpus: 4161 files). Zero artifacts.

Datetime-carve hit rate over the committed corpus (unit replay counters):
**321 / 4146 in-domain execs ≈ 7.7%** (the corpus deliberately contains
280 datetime-family regress paths to keep exercising the filter).

Docker ground-truth spot checks (postgres:18.3, trust auth): query_array
unwrapping (`[1, "2", {}, [3], null, true]` × `$[*] ? (@ > 0)` →
`[1, 3]`), strict query_first with filter, exists 4-arg false, match
`[true]` → t, `@?` with exists(), `.double()` over 0.1/1e10/-1e-10,
like_regex flag "i", `.keyvalue()` id/key/value rows — all matched the
engines' agreed outputs (the crate's own tests additionally lock these
shapes against regress expected output).

These are laptop smokes only; no fuzz-only sufficiency claim may cite
them — the >=10M-exec / 24h-CPU floor is the FLEET campaign's job.

## Divergences

None yet. (Run 1's crash was a LINK failure, not a divergence; the unit
tier found zero disagreements across the regress matrix, and both smokes
ran divergence-free.)

Triage protocol when one appears: oracle-shim-bug → fix in-lane;
suspected pgrust bug → minimize, ground-truth against docker
`postgres:18.3` (`SELECT jsonb_path_query_array('doc'::jsonb,
'path'::jsonpath)` etc.), bank seed, write up here; suspected adt/jsonb
doc-parse/serialize divergence → record for lane p1-lanev, do not fix.

## Fleet re-confirm (2026-07-31) + slow-unit triage

Job `pgrust-fuzz-campaign-1785519081-6232-42303` @ `0efdbb73c016ad26000a520b0ded24093eff8f7a`
(fork=12, c8g.4xlarge): **10,005,901 execs**, cov_lines 13297, corpus 6446 -> 7364,
**0 value divergences**, 0 sanitizer artifacts, rc=0. This is the campaign of record
for the crate (it covers the SHORT-VARLENA and JSON_EXISTS/List-vars planes added in
the coverage close-out; the earlier 10.14M run `...-1785514414-6d16-88500` predates them).

**One artifact, classified NOT a divergence:**
`slow-unit-6f3ace6571ce07cea7599724f5f77929ff582ea1` — libFuzzer slow-unit report,
37,865 ms on the fleet. Content: an arm-1 (match) input whose path text is a
`like_regex` with heavily nested alternation/quantifiers (`(|b`+ ... ){1,6}`, ~229
pattern bytes). Both engines validate a `like_regex` pattern by COMPILING it during
jsonpath PARSING (C `makeItemLikeRegex` -> `pg_regcomp`; Rust `make_item_like_regex`
-> `regex_core::regex_compile::pg_regcomp`), so the cost is NFA construction in the
shared-algorithm regex engine, not path evaluation.

Triage evidence (probe `slow_unit_probe::slow_unit_timing_probe`, committed,
`#[ignore]`d, driven by `PGRUST_SLOW_UNIT=<file>`): the SAME input replays through the
full differential iteration in **718 ms** on the laptop release build — no hang, no
infinite loop, and the value/verdict/sqlstate planes AGREED (it is in the corpus and
the replay rail is green). The fleet's 50x is the instrumented build: sancov on BOTH
the Rust and the vendored-C objects plus fork-mode accounting.

Classification: **performance artifact of a shared upstream property** (PG's regex
compiler has no polynomial guarantee and PG ships no regex timeout), not a pgrust
defect and not a correctness divergence. The input is banked as corpus seed
`slowunit-regex-compile-6f3ace65` so any future ASYMMETRY (one engine slow, the other
fast) is caught by the rail. NOT closed as "no issue": if a per-side timing plane is
ever wanted, that probe is the hook.

**Reader beware (gate-blindness class):** `scripts/fetch-fuzz-results.sh` prints
`DIVERGENCE target=... artifact=slow-unit-...` and `campaign-stats.json` counts the
artifact in `"divergences": 1` — a slow-unit is NOT a value divergence. Always open the
artifact before believing the count.

## ASan-pass re-fire triage (2026-08-01, divtriage/jsonpath)

The ASan tree-wide side channel (task #84) reported "jsonpathexec 5" as
behavioral divergences. Triage verdict: **ALL FIVE ARE SLOW-UNIT artifacts
of the ALREADY-TRIAGED regex-compile class** documented above ("Fleet
re-confirm (2026-07-31) + slow-unit triage") — NOT value divergences, and
one is not even new:

- `slow-unit-6f3ace6571ce07cea7599724f5f77929ff582ea1` is BYTE-IDENTICAL
  to the banked corpus seed `corpus/jsonpathexec_diff/
  slowunit-regex-compile-6f3ace65` (verified with cmp). The ASan leg
  simply re-replayed our own banked seed slowly enough to re-report it.
- The other four (`005a59e0`, `7d7853cb`, `965a2f8b`, `e5f66f04`) are
  mutants of the same input: arm-1 `lax $[*] ? (@ like_regex
  "^ like_...(|b`+ ...){1,6}...")` — nested alternation/quantifiers with a
  leading `^` anchor, i.e. the same pg_regcomp NFA-construction blowup.

Evidence (job `pgrust-fuzz-campaign-1785622556-44a7-65960` @
`91c635b5fd00880af14f8152612d531a649924cd`, fresh ASan leg, 2,012,053
execs): fleet `run.log` has zero panics, `oom/timeout/crash: 0/0/0`; the
"5 divergences" in campaign-stats is exactly the artifact-file count —
the "Reader beware" gate-blindness class this README already documents.
Local replay of all 5 (`-runs=1`, sancov fuzz build): every input executes
to completion, planes agree, 8.9-51.7 s. Full corpus replay at this tree:
8,961 execs, ZERO divergences.

Mechanism of the re-fire: `PGRUST_FUZZ_CASAN=1` slows the C oracle's regex
compile ~3x, pushing mutants of the banked slow seed over libFuzzer's
slow-unit threshold. No pgrust bug, no new carve; the standing
recommendation stands — the campaign runner should classify `slow-unit-*`
artifacts as SLOW-UNIT, never DIVERGENCE.

[Mechanism note, 2026-08-03 (task #143 addendum): the leg above ran on the
task-#84 tree, where `PGRUST_FUZZ_CASAN=1` armed -fsanitize=address on ALL
vendored oracle cc::Builds, including this family's — hence the 3x C-side
regex-compile slowdown. At current main the same env name is a fleet-alias
of `PGRUST_ORACLE_ASAN=1` in fuzz/core/build.rs (the fleet runner's --casan
path exports the CASAN spelling): under a cargo-fuzz build
(CARGO_CFG_FUZZING) either name arms ASan on the wcharfam + regexfam family
oracle builds only (wcharfam/spellfam TUs are additionally always armed
under fuzzing). The jsonpath family TU is NOT in the currently-armed set,
so a --casan leg at main slows this target only via the process-wide ASan
malloc interposition, not instrumented oracle code.]
