# README-TODO: regexp_diff (crates/backend/utils/adt/regexp)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `textregexeq` | 1254 | `regexp.c` |
| `textregexne` | 1256 | `regexp.c` |
| `texticregexeq` | 1238 | `regexp.c` |
| `texticregexne` | 1239 | `regexp.c` |
| `nameregexeq` | 79 | `regexp.c` |
| `nameregexne` | 1252 | `regexp.c` |
| `similar_escape` | 1623 | `regexp.c` |
| `similar_to_escape_1` | 1987 | `regexp.c` |
| `similar_to_escape_2` | 1986 | `regexp.c` |
| `textregexsubstr` | 2073 | `regexp.c` |
| `textregexreplace_noopt` | 2284 | `regexp.c` |
| `textregexreplace` | 2285 | `regexp.c` |
| `regexp_count` | 6256 | `regexp.c` |
| `regexp_instr` | 6262 | `regexp.c` |
| `regexp_like` | 6264 | `regexp.c` |
| `regexp_substr` | 6269 | `regexp.c` |
| `regexp_match` | 3397 | `regexp.c` |
| `regexp_split_to_array` | 2768 | `regexp.c` |
| `textregexreplace_extended` | 6251 | `regexp.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_regexp_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verified against
      `../pgrust-fabled/vendor/postgres-src`). All `#error` gates removed
      with their pastes. The Spencer ENGINE is vendored verbatim under
      `core/csrc/regexfam/` (own shim include tree; regexport.c deliberately
      not vendored — no SQL face in scope. regprefix.c WAS vendored verbatim
      in the 2026-07-31 hardening pass for the regexp_fixed_prefix arm).
- [x] Document every shim in the file header (plumbing only, never logic;
      errcode -> `PG_DIFF_REGEXP_ERR_*` classes 1..6).
- [x] palloc/palloc0/repalloc/pfree on the TLS arena (made GROWABLE — the
      match/split result loops allocate O(nmatches) texts); every
      `pg_diff_*` entry resets arena + errcode first (entry prologue macro).
- [x] `pg_diff_*` driver entries written (18 entries incl. parse_re_flags).
- [x] build.rs: pg_regexp_io.c + regexfam compile in their OWN cc::Build
      (`pg_difffuzz_regexfam`) so the regexfam shim headers never shadow
      csrc/shim for other oracles (gate comment updated in place).

## 2. Implement the Rust driver (`core/src/regexp_diff.rs`)

- [x] All 19 arms implemented, all three comparison planes; no `todo!()`
      remains. Pins: UTF-8 encoding, collation 950, regex_engine=spencer
      (verified at runtime by tests::spencer_path_pinned — this dev build
      DOES link libre2, so the pin is load-bearing).
- [x] fc-wrapper plane on every scalar arm, arity-matched to the presence
      combination (all builtins variants driven); SRF fc wrappers carved
      (fmgr machinery; per-row cores driven directly). No regexp builtin
      takes an escontext (no soft-error shapes in this family).
- [x] SKIPPED rows recorded in the module header (SRF shells, bpchar
      aliases) with reasons.
- [x] HARDENING PASS (2026-07-31): arms 19/20 nameicregexeq/ne (oids
      1240/1241, REG_ICASE name cores + fc plane) and arm 21
      regexp_fixed_prefix (vs verbatim regexp.c regexp_fixed_prefix +
      vendored regprefix.c; prefix-bytes/exact/NULL planes).  Selector
      widened to %22.  43 witness seeds added for the argument-validation
      branches (g-flag rejection on count/instr/like/substr/split, start<=0,
      n<=0, n>nmatches, endoption domain, subexpr <0/>npatterns, unmatched
      optional group so<0, digit-first replace flags HINT, multibyte
      invalid option, similar_escape empty/2-char/multibyte escapes) +
      matches_branch_witnesses test mirroring them; re_cache_eviction_sweep
      covers the >32-entry cache eviction + move-to-front (Rust-only — the
      cache is the lane's carve).
- [x] Extern decls live; dead_code allow dropped; tests un-ignored + grown
      (per-arm ok+error smokes with captures, flags-error smoke,
      similar_escape quote-separator cases, degenerate/multibyte witnesses,
      param boundary witnesses, exhaustive parse_re_flags sweep len<=2 over
      all bytes plus 1e5 random len<=4, seed replay).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin regexp_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable
      (93 passed / 3 ignored = other lanes' scaffolds).

## 3. Seeds, dictionary, corpus

- [x] `fuzz/regexp_diff.dict` extended (regex.sql/strings.sql tokens:
      quantifiers, lookaround, backrefs, [[:class:]], ARE directors,
      embedded options, flag alphabet, SIMILAR TO metachars, multibyte).
- [x] `fuzz/corpus/regexp_diff/` seeded (64 seeds, grammar-shaped, incl.
      empty-match/degenerate and multibyte witnesses). COMMIT + S3-bank
      are the lane coordinator's steps (agents never run git here).

## 4. Campaign (nightly toolchain)

DIVERGENCE FOUND (2026-07-31 local smoke, ~60k execs in): the Rust Spencer
port (regex_core — the ENGINE lane's crate; this lane reports, doesn't fix)
raises REG_ETOOBIG "regular expression is too complex" on nested bounded
quantifiers that BOTH the vendored 18.3 C engine and real postgres:18.3
(byte-exact Docker replay via decode(base64)) accept.  Minimized 32-byte
repro + SQL repro + executable-exception carve + witness test:
fuzz/core/src/regexp_diff.rs `known_etoobig_divergence` /
tests::known_etoobig_divergence_repro.  Related engine-lane finding: the
Rust engine's rstacktoodeep() is hardcoded 0 (C consults
stack_is_too_deep()), so deep-recursion patterns can stack-overflow a 2MB
thread outright (seed replay + witness tests run on 64MB threads).

CAMPAIGN NOTE (2026-07-31 local smoke): Spencer bounded-quantifier patterns
over empty-capable alternations (e.g. `(\y.|){95}`) compile in seconds-not-
milliseconds on BOTH sides (identical engines; a reproducer completed in
~11s dev+ASan, no hang, no divergence) and transiently allocate hundreds of
MB — run campaign legs with `-timeout=60 -rss_limit_mb=4096` (default 2048
trips on the high-water, LSan-verified leak-free; only libFuzzer's own
56-byte thread object reports at exit).  PGRUST_FUZZ_CSANCOV=1 instruments
the regexfam build too (same env gate as the main oracle lib).

RUN RECIPE (this target only — FORK MODE IS MANDATORY): the mutator keeps
synthesizing nested-bounded-quantifier compile bombs (`(\y.|){95}` shapes),
so a single-process leg always dies on one before its run budget; the bomb is
NOT a divergence (both engines are the same Spencer code and both take
seconds).  Run legs as:
  regexp_diff -fork=2 -ignore_timeouts=1 -ignore_ooms=1 -ignore_crashes=0 \
      -runs=<N> -timeout=25 -rss_limit_mb=4096 -max_len=96 \
      -artifact_prefix=<dir>/ -dict=regexp_diff.dict corpus/regexp_diff
`-ignore_crashes=0` keeps a REAL divergence (harness panic) fatal while the
pathological-compile timeouts/OOMs are isolated in children and skipped.
Verdict after a leg = zero `crash-*` artifacts in the artifact dir.

CORPUS HYGIENE (this target only): libFuzzer retains compile-bomb patterns
because they light up huge amounts of engine coverage, and they then dominate
every later leg's wall clock (one unit = 90s).  The checked-in corpus is
kept to units that replay in <2s; the pathological ones live in
fuzz/artifacts/regexp_diff/ (gitignored) as slow-unit-*/timeout-* evidence
rather than in the corpus.  Re-prune after any campaign leg:
  for f in corpus/regexp_diff/*; do timeout 2 ./target/.../regexp_diff \
      -runs=1 -rss_limit_mb=4096 "$f" >/dev/null 2>&1 || rm "$f"; done

LOCAL SMOKE OF RECORD (2026-07-31): 207,437 execs, fork=2 release build,
exit 0, ZERO crash artifacts (only slow-unit-* compile bombs, expected and
symmetric); cov 9,750 edges / ft 47,481 over a 2,884-unit corpus.  The one
divergence class found across all legs is the ETOOBIG report above.

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run regexp_diff` — floor for any fuzz-only claim:
      >=10M execs or 24h CPU per family, all planes compared; record the
      campaign size in the ledger row.
- [ ] Ground-truth law: no divergence recorded from the vendored oracle
      alone — replay against `postgres:18.3` Docker; triage Csmith-style
      (pgrust-bug / oracle-platform-variance carve / upstream-bug).

## 5. Bookkeeping (every commit) and done-gate

- [ ] Ledger rows (`proofs/USER_FACING_FUNCTIONS.tsv`) with the standardized
      qualifier grammar; every harness in `proofs/SUITE.tsv`;
      `proofs/lint-suite-rows.py` clean.
- [ ] Flip `docs/verification/phase1-routes.tsv` statuses as functions land
      (evidence = this target's name); update the crate's claim row in
      `phase1-claims.tsv` and its `phase1-ranking.tsv` row. Pull before
      editing shared TSVs and RE-READ after any pull before writing.
- [ ] Done-gate: coverage merge 100% in-scope v2-SLOC under proof-union-fuzz
      or recorded executable exception; rendered-red-line eyeball audit;
      `cargo mutants` pilot on fuzz-only regions; replay rail in CI.
