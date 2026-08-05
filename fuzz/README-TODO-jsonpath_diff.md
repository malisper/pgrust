# jsonpath_diff (crates/backend/utils/adt/jsonpath) — lane p1-laneaa

Differential fuzz target: shipped Rust `adt_jsonpath` vs vendored
PostgreSQL 18.3 C. Oracle pin: Stamp-18.3, upstream sha
`62d6c7d3df6287f1bd83199c1a746e50d31571a0` — never `:latest`, never 18.4.
Method doc: `.claude/skills/fuzzuproof-crate/SKILL.md`.

Functions covered:

| function | oid | C source | arm |
|---|---|---|---|
| `jsonpath_in` | 4001 | `jsonpath.c` | 0 (+2 for parse) |
| `jsonpath_recv` | 4002 | `jsonpath.c` | 1 |
| `jsonpath_out` | 4003 | `jsonpath.c` | 0, 1 |
| `jsonpath_send` | 4004 | `jsonpath.c` | 1 |
| `jspIsMutable` | — (planner entry) | `jsonpath.c` | 2 |

## Oracle provenance (fuzz/core/csrc/jsonpath/)

VERBATIM whole files, copied unmodified from the vendored tree:

- `jsonpath.c` — `src/backend/utils/adt/jsonpath.c`
- `jsonpath_internal.h` — same directory
- `pg_stringinfo.c` — `src/common/stringinfo.c`
- `regex/regcomp.c` (which `#include`s `regc_lex.c`, `regc_color.c`,
  `regc_nfa.c`, `regc_cvec.c`, `regc_pg_locale.c`, `regc_locale.c`),
  `regex/regerror.c`, `regex/regfree.c` — `src/backend/regex/`
- `include/regex/{regex,regguts,regcustom,regerrs}.h`,
  `include/utils/jsonpath.h`, `include/nodes/miscnodes.h`,
  `include/nodes/pg_list.h`, `include/lib/stringinfo.h`,
  `include/port/simd.h`, `include/utils/ascii.h` — verbatim headers

GENERATED, checked in (so the fleet build needs no bison/flex):

- `jsonpath_gram.c` / `jsonpath_gram.h` — bison 2.3, from the vendored
  `jsonpath_gram.y`: `bison -d -o jsonpath_gram.c <src>/jsonpath_gram.y`
- `jsonpath_scan.c` — flex 2.6.4, from the vendored `jsonpath_scan.l`:
  `flex -o jsonpath_scan.c <src>/jsonpath_scan.l`

EXTRACTED VERBATIM (per-function/per-range provenance markers emitted
mechanically by `csrc/jsonpath/extract_verbatim.py`; every marker names the
source file and 1-based line range):

- `pg_numeric_min.c` — exactly the `numeric_in` / `numeric_out` /
  `numeric_uminus` call graph from `numeric.c` (the `DirectFunctionCall`
  sites in `jsonpath.c` and `jsonpath_gram.y`): NumericVar machinery,
  digit buffers, set_var_from_str, the non-decimal integer parser,
  get_str_from_var, make_result(_opt_error), apply_typmod(_special),
  round/trunc/strip_var, add/mul/cmp/add_abs/sub_abs. No aggregates, no
  wider arithmetic.
- `pg_formatting_min.c` — exactly the `datetime_format_has_tz` call graph
  from `formatting.c` (used by `jspIsMutableWalker`): KeyWord/KeySuffix/
  FormatNode types, DCH tables + index, the DCH picture cache,
  parse_format, DCH_datetime_type, DCH_cache_*.
- `pg_support_min.c` — `pqformat.c` (pq_sendtext/begintypsend/endtypsend/
  getmsgint/getmsgbytes/copymsgbytes/getmsgtext), `json.c`
  (escape_json_char/escape_json/escape_json_with_len), `list.c`
  (new_list/enlarge_list/list_make1/2_impl/new_tail_cell/lappend),
  `value.c` (makeString), `wchar.c` (pg_utf_mblen,
  pg_utf2wchar_with_len, pg_utf8_islegal, the shift-DFA +
  utf8_advance + pg_utf8_verifychar/verifystr), `mbutils.c`
  (pg_unicode_to_server(_noerror), pg_mb2wchar_with_len, pg_mblen*,
  pg_verify_mbstr), `numutils.c` (pg_strtoint32(_safe) + hexlookup),
  `pgstrcasecmp.c`, `wstrncmp.c` (pg_char_and_wchar_strncmp), plus the
  `common/int.h` inline helpers those need.

Shims are ENVIRONMENT ONLY and each is listed in the file that carries it
(`include/postgres.h` header + `pg_jsonpath_env.c` header):

- TLS growable pointer arena behind palloc/palloc0/repalloc/pfree/
  palloc_extended (models PG's per-query memory-context reset; every
  `pg_diff_*` entry resets it first, so an error longjmp cannot leak — the
  2026-07-31 LSan incident class);
- `ereport`/`elog`/`errsave`/`ereturn` → TLS errcode + message/detail
  capture; ERROR `siglongjmp`s to the entry's `sigsetjmp`; `errsave`
  against a live `ErrorSaveContext` records a soft error and falls through,
  the real protocol (`nodes/miscnodes.h` is vendored verbatim);
- ERRCODE_* are the real `MAKE_SQLSTATE` encodings, so the sqlstate plane
  compares genuine PostgreSQL sqlstates against `SqlState(i32)`;
- ENCODING PIN UTF-8 (the pin the crate itself makes): `GetDatabaseEncoding`
  → `PG_UTF8`, encoding table reduced to the UTF-8 row bound to the verbatim
  UTF-8 functions; client encoding == server encoding, so
  `pg_client_to_server` takes `pg_any_to_server`'s same-encoding arm —
  **including its `pg_verify_mbstr` validation** (see divergence 1);
- COLLATION PIN: `pg_newlocale_from_collation` returns a `ctype_is_c`
  default-locale entry, so `regc_pg_locale.c` lands on
  `PG_REGEX_STRATEGY_C`, mirroring the Rust side's
  `pg_locale::set_default_locale_c_for_tests()`. Documented as a model in
  `include/utils/pg_locale.h`; the BUILTIN-provider ctype/case hooks are
  unreachable under it and are loud abort stubs
  (`include/common/unicode_{category,case}.h`);
- `exprType` reads the type oid off the driver's `PgDiffVarExpr` nodes,
  mirroring the shipped Rust vars model `&[(&[u8], Oid)]`
  (`include/nodes/nodeFuncs.h`);
- `check_stack_depth` / `CHECK_FOR_INTERRUPTS` no-ops (see the driver's
  54001 carve);
- `Assert` compiled out (a production/NDEBUG build, which is what the
  `postgres:18.3` docker ground truth is);
- DCH picture cache allocations go to plain malloc in
  `pg_formatting_min.c`: the cache is long-lived in real PG and must not be
  freed by the per-call arena reset.

Symbol isolation: `build.rs` compiles this family as its own `cc::Build`
with every generic-named exported symbol renamed `jporcl_*`
(`JSONPATH_SHARED_SYMS`), the same rationale as `CRYPTO_SHARED_SYMS` — each
oracle family keeps its OWN vendored copy next to the others, so drift
between families is a divergence instead of a silent cross-bind.
`PGRUST_FUZZ_CSANCOV=1` instruments this family too (NEZHA union coverage).

## Status

- [x] 1. Vendor the C oracle; compiles clean; standalone C smoke green.
- [x] 2. Rust driver (`core/src/jsonpath_diff.rs`): 3 arms, all planes
      (value bytes / verdict / sqlstate) + the fc-wrapper plane for all four
      `fc_*` wrappers (including the escontext-armed `fc_jsonpath_in`
      shape). Build gate uncommented in `core/build.rs`.
- [x] 3. Dictionary (`fuzz/jsonpath_diff.dict`) from `jsonpath_scan.l` /
      `jsonpath_gram.y` tokens + selector/mode prefixes; seeds harvested
      mechanically by `fuzz/gen_seeds_jsonpath.py` (regress
      `jsonpath.sql` / `jsonb_jsonpath.sql` / `jsonpath_encoding.sql`
      `::jsonpath` literals + the crate's own vector tables), committed
      under `fuzz/corpus/jsonpath_diff/`.
- [x] 4a. `cargo check --manifest-path fuzz/Cargo.toml --bin jsonpath_diff`
      clean; `cargo test --manifest-path fuzz/core/Cargo.toml jsonpath`
      6/6 green (full corpus replay, all regress ok+err vectors in hard AND
      soft mode, unicode/numeric/regex/nesting edges, wire-framing edges,
      19 paths × 32 var models).
- [x] 4b. Local smoke on the laptop (short only, per the lane's hard rules):
      `PGRUST_FUZZ_CSANCOV=1 cargo +nightly-2026-07-17 fuzz run
      jsonpath_diff -- -max_total_time=90 -dict=jsonpath_diff.dict
      -max_len=520`. Results in "Smoke results" below.
- [ ] 5. FLEET CAMPAIGN (lane coordinator submits; the floor for a
      fuzz-only claim is >=10M execs or 24h CPU per family, all planes,
      campaign size recorded in the ledger row).
- [ ] 6. Bookkeeping: `proofs/USER_FACING_FUNCTIONS.tsv` rows,
      `proofs/SUITE.tsv`, `docs/verification/phase1-routes.tsv` statuses,
      the claim row on `main`.
- [ ] 7. Done-gate: coverage merge (100% in-scope v2-SLOC under
      proof∪fuzz or a recorded executable exception), rendered-red-line
      eyeball audit, trailing `cargo mutants`, replay rail in CI.

## Smoke results

Run 1 (2026-07-31, laptop, 90s, sancov on both sides, dict + max_len=520,
seeded from the 874 harvested seeds): **1 divergence, triaged below**; the
run reached the crash within the first seconds, so exec volume is not
meaningful for that run.

Run 2 (after the domain fix, same recipe, 90s): **219,559 execs, ZERO
divergences**.

Run 3 (same recipe, 120s): **243,179 execs, ZERO divergences**; final
`cov: 11999 ft: 56309`, corpus grown to 3663 in-run entries. `cargo fuzz
cmin` then minimized the banked corpus to 3307 files preserving all
**12,014 coverage edges / 56,341 features**, and the mechanically harvested
seed set + the divergence-1 regression seed were re-applied on top
(committed corpus: 4182 files, ~300 KB of input bytes). Zero artifacts
remain from runs 2-3 (`fuzz/artifacts/jsonpath_diff/` holds only the
divergence-1 input, which is also banked as a seed).

Run 4 (2026-07-31, after the divergence-2 recursion-guard fix + in-harness
guard arming/carve, same recipe, 90s): **222,784 execs, ZERO divergences,
zero artifacts**; the full committed corpus replays clean in the debug test
tier (it aborted pre-fix on the fleet 250-deep seed). In-run corpus growth
banked as a smoke delta.

These are laptop smokes only, per the lane's hard rules. The >=10M-exec /
24h-CPU floor is the FLEET campaign's job (step 5) and has NOT been run —
no fuzz-only sufficiency claim may cite these numbers.

## Divergences

### Divergence 2 — parser native-stack overflow = process abort (PGRUST BUG, FIXED)

Found by the fleet campaign corpus (a ~250-deep arm-1 paren seed aborted the
debug corpus replay with `fatal runtime error: stack overflow`), then
ground-truthed as SQL-reachable: `gram.rs` is a hand-written
RECURSIVE-DESCENT port of C 18.3's BISON-generated `jsonpath_gram.y`. The C
parser keeps its parse/value stacks on the HEAP, bounded by `YYMAXDEPTH`,
and exhaustion is a clean soft-errorable error; the Rust port recursed on
the NATIVE stack with NO depth guard anywhere in `gram.rs` (the FLATTEN and
PRINT walks in `path.rs:188/835` had the `check_stack_depth` guards
mirroring `jsonpath.c:249/529`; the parser had none — bison's bound simply
had no analogue to mirror). pgrust is thread-per-backend in ONE process, so
a single client string literal `('('*N || '1' || ')'*N)::jsonpath` aborted
the whole server (every session): availability/DoS class.

Ground truth, docker `postgres:18.3` (`docker exec laneaa-pg183 psql -U
postgres -v VERBOSITY=verbose -tAc ...`), banked verbatim:

```
select (repeat('(',8000)||'1'||repeat(')',8000))::jsonpath is not null
 -> t
select (repeat('(',9995)||'1'||repeat(')',9995))::jsonpath is not null
 -> t                                       (last OK, binary-searched)
select (repeat('(',9996)||'1'||repeat(')',9996))::jsonpath is not null
 -> ERROR:  42601: memory exhausted at or near ")" of jsonpath input
    LOCATION:  jsonpath_yyerror, jsonpath_scan.l:382
select message, sql_error_code
  from pg_input_error_info(repeat('(',9996)||'1'||repeat(')',9996), 'jsonpath')
 -> memory exhausted at or near ")" of jsonpath input|42601   (soft-errorable)
select ('$ ? ('||repeat('!(',12000)||'@ == 1'||repeat(')',12000)||')')::jsonpath
 -> ERROR:  42601: memory exhausted at or near "!" of jsonpath input
select ('((((1))))')::jsonpath -> 1
select ('(($.a))')::jsonpath   -> $."a"
```

Pre-fix Rust (release profile, laptop, guard-relevant thread stacks):
coordinator-measured on the shipped release with the 8 MiB backend stack
(`launch_backend::child_thread_stack_size` floor,
`crates/backend/postmaster/launch_backend/src/lib.rs:773`): N=8000 OK,
N=12000 `fatal runtime error: stack overflow, aborting` (SIGABRT). On this
laptop's release test build (lto=thin, cgu=1, aarch64-darwin) even N=8000
aborts an 8 MiB thread (~1.17kB native stack per paren level measured) —
frame sizes vary by build, the abort does not.

FIX (`gram.rs::check_depth`): `stack_depth::check_stack_depth()` at
`parse_unary` + `parse_delimited_predicate` — the minimal total cut of the
parser's recursion cycles (totality proof in the `check_depth` doc comment;
the two cycles `!(!(...))` and unary `-{n}` are vertex-disjoint, so one
check cannot suffice). The depth error rides the crate's normal plumbing:
hard `PgError` without an escontext, recorded softly (and not overwritten by
the generic syntax error, via `aborted`) with one.

PARITY DECISION — option (a), 54001, NOT C's 42601. C's bound is
`YYMAXDEPTH` in units of LALR stack ENTRIES (per-level growth differs by
production: paren flip at N=9996); mirroring it exactly would require
emulating bison stack growth per shape, and an inexact mirror would
misclassify inputs in the region PG still accepts. So the guard is the
crate-standard `check_stack_depth`: ERRCODE_STATEMENT_TOO_COMPLEX (54001)
"stack depth limit exceeded", threshold = `max_stack_depth` GUC. We did NOT
fabricate C's "memory exhausted" message.

Measured post-fix (release, armed thread, binary-searched):

| config | paren flip (last OK / first 54001) | not-chain flip |
|---|---|---|
| max_stack_depth=2048kB (server default after rlimit adjust) | 1793 / 1794 | 1867 / 1868 |
| max_stack_depth=100kB (boot default) | 85 / 86 | 85 / 86 |
| unarmed thread (guard base unset) | unchanged: N=8000 parses, out = `1`, byte-identical to PG | — |

RESIDUAL (documented, accepted): in the deep region BOTH sides now raise a
clean, soft-errorable error, but the errcode (54001 vs 42601) and the
threshold (max_stack_depth bytes, ~N=1794 at the 2048kB server default, vs
YYMAXDEPTH entries, N=9996) differ — i.e. paren nesting in [1794, 9995]
parses on real PG and errors 54001 on an armed pgrust backend. That region
is far outside this differential's MAX_TEXT=512 domain (<=511 levels,
~600kB release stack, under every armed threshold in play), so the corpus
and the fuzz domain are unaffected in release. Regression tests:
`crates/backend/utils/adt/jsonpath/src/tests.rs`
(`parser_depth_guard_bounds_every_recursion_cycle` — five distinct
recursion-cycle shapes on a 1 MiB guarded thread,
`parser_depth_guard_is_soft_errorable`,
`parser_depth_guard_below_threshold_round_trips`).

Harness change riding this fix: `setup()` now arms the guard per-thread at
1536kB (2 MiB libtest thread minus C's STACK_DEPTH_SLOP admission rule) and
`depth_carved` removes Rust-side 54001 verdicts from the comparison domain
(no C counterpart in-harness by construction — the shim's
`check_stack_depth` is a no-op). Pre-fix, the fleet 250-deep seed ABORTED
the debug corpus replay; post-fix the full committed corpus replays clean.

### Divergence 1 — invalid-UTF-8 datetime template (OUT OF DOMAIN, domain narrowed)

Artifact (banked as `corpus/jsonpath_diff/seed-div1-invalid-utf8-datetime-template`,
also in `fuzz/artifacts/jsonpath_diff/`):

```
[2, 4, 36, 46, 100, 97, 116, 101, 116, 105, 109, 101, 40, 34, 100, 212,
 116, 101, 116, 105, 109, 101, 32, 116, 101, 109, 112, 108, 97, 116, 101,
 247, 7, 34, 41]
```

i.e. arm 2 (mutability), var model 4, source text
`$.datetime("d\xD4tetime template\xF7\x07")` — a datetime template
containing bytes that are not valid UTF-8.

Observed: `jspIsMutable` C = hard error `22021`
("invalid byte sequence for encoding UTF8", raised from `parse_format`'s
`pg_mblen_cstr` → `report_invalid_encoding`), Rust = `Ok` (not mutable).

Triage: **neither side is wrong; the input is unreachable in a real
server.** PostgreSQL validates client bytes with `pg_verify_mbstr` inside
`pg_any_to_server` (mbutils.c) at the client/server boundary, *before* any
input function runs, so an invalid sequence can never reach `jsonpath_in`,
can never be stored inside a jsonpath value, and therefore can never reach
`jspIsMutable`'s datetime-template inspection. The two sides simply made
different (both unreachable) choices about defending an impossible input:
the C `parse_format` reports, while pgrust's
`crates/backend/utils/adt/formatting/src/parse.rs::pg_mblen_cstr`
deliberately swallows the range error
(`mbutils::pg_mblen_range(s).unwrap_or(s.len())`) with an in-code comment
saying the Err path is dead. This lane's differential is what *proves* that
comment's reachability claim, so it is recorded here as an audit note, not
a bug: **no crate change made** (the deviation is in `adt_formatting`, not
in the crate under test, and it is only observable over inputs the pipeline
excludes).

Docker ground-truthing was not run for this one and could not settle it:
there is no SQL that delivers invalid UTF-8 into a `jsonpath` value on a
`postgres:18.3` server — that impossibility *is* the finding.

Resolution: the shared domain for arms 0 and 2 now requires valid UTF-8
source text (`in_domain()` in the driver), matching the invariant the input
pipeline guarantees. The encoding plane stays under comparison exactly
where PostgreSQL itself enforces it: arm 1 (`jsonpath_recv`), which runs
`pg_verify_mbstr` in-band — and that comparison already earned its keep
(see divergence 0).

### Divergence 0 — ORACLE SHIM BUG (fixed in-lane, pre-smoke)

Found by the unit-test tier, before the fuzz smoke: `jsonpath_recv` of a
wire body containing invalid UTF-8 gave C = `42601` (syntax error) vs
Rust = `22021`. Cause was in the ORACLE, not the crate: the first
`pg_client_to_server` shim was the identity, but real `pg_any_to_server`'s
same-encoding arm still *validates* with `pg_verify_mbstr`. Fixed by
vendoring that verifier chain verbatim (`wchar.c` shift-DFA +
`utf8_advance` + `pg_utf8_verifychar/verifystr`, `mbutils.c`
`pg_verify_mbstr`, `utils/ascii.h` `is_valid_ascii`); pgrust was right.
Durable lesson for future oracles: an "encoding conversion is the identity
under a same-encoding pin" shim is WRONG — the same-encoding arm is a
validator, not a no-op.

## ASan-pass re-fire triage (2026-08-01, divtriage/jsonpath)

The ASan tree-wide side channel (task #84, ledger
`fuzz/ASAN-TREEWIDE-FINDINGS.md`) reported "jsonpath 5" under "behavioral
divergences surfaced in passing". Triage verdict: **ALL FIVE ARE libFuzzer
SLOW-UNIT artifacts, not value divergences** — the already-ratified
regex-compile performance class of `fuzz/FINDING-jsonpath-parse-complexity.md`
(pg_regcomp `fixconstraintloops`/`clonesuccessorstates` state-cloning blowup
on `^`-anchors under quantified alternation; PG 18.3 pays the same
super-polynomial cost to within ~2x). No new shape; no pgrust bug; no carve
row needed.

Evidence (job `pgrust-fuzz-campaign-1785622556-44a7-65960` @
`91c635b5fd00880af14f8152612d531a649924cd`, fresh ASan leg, 621,981 execs):

- All 5 artifacts are named `slow-unit-*`; every input is an arm-0
  `$ ? (@ like_regex "...")` whose pattern is `^^^^`-anchor runs under
  `|`/`+`/`{m,n}` — three of the five literally contain the
  `pawt@r^^^^|\\\\\?\^^^\\Y||pawt@r` repeated unit minimized in the
  FINDING doc: `slow-unit-{50f8874f,899856ad,93f982b9,d063679c,f6de6b93}`.
- Fleet `run.log`: zero `panicked` lines, `oom/timeout/crash: 0/0/0` —
  no divergence panic ever fired; the 5 counted "divergences" are the
  artifact-file count (the documented "Reader beware" gate-blindness class
  in README-TODO-jsonpathexec_diff.md: campaign-stats counts every
  artifacts/ file, and slow-units land there).
- Local replay of all 5 through the uninstrumented-oracle target
  (`-runs=1`): each EXECUTES TO COMPLETION with all planes agreeing
  (16.7-78.8 s under the sancov fuzz build; no panic, no artifact).
- Full committed corpus replay at this tree: 17,677 execs, ZERO
  divergences; fresh 120 s leg likewise (see commit message).

Why the ASan leg surfaced them when phase-1 legs mostly don't: ASan on the
C oracle (`PGRUST_FUZZ_CASAN=1`) multiplies the C side's regex-compile time
~3x, so mutants of the banked slow seeds cross libFuzzer's slow-unit
reporting threshold far more often. [Mechanism note, 2026-08-03, task #143
addendum: that leg ran on the task-#84 tree where the flag armed ALL oracle
cc::Builds. At current main `PGRUST_FUZZ_CASAN=1` is the fleet-interface
alias of `PGRUST_ORACLE_ASAN=1` (fuzz/core/build.rs; the fleet runner's
--casan export): under CARGO_CFG_FUZZING either name arms the wcharfam +
regexfam family oracle builds only — this jsonpath family TU is not in the
armed set, so the 3x compile inflation described here does not reproduce at
main until the tree-wide task-#84 pass lands.] The ledger line "correctly classified
DIVERGENCE, san=0" is WRONG for this cluster — kind should have been
SLOW-UNIT. Runner fix owed (same as the jsonpathexec_diff note): classify
`slow-unit-*` artifacts separately from divergences in campaign-stats.

Fresh 120 s local legs at this triage tree (both targets): ZERO value
divergences. Each leg ended with a libFuzzer cumulative-RSS OOM artifact
(non-fork local runs accumulate the TLS-arena high-water across execs; one
"oom-" input replays alone in 60 ms / 138 MB RSS, the exec one is the
EMPTY file da39a3ee...) — an accounting artifact of the local recipe, not
an input property; the fleet recipe (-fork=8 -ignore_ooms=1) is immune.
