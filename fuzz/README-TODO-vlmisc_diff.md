# README-TODO: vlmisc_diff (crates/backend/utils/adt/varlena)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `to_hex32` | 2089 | `varlena.c` |
| `to_hex64` | 2090 | `varlena.c` |
| `to_bin32` | 6330 | `varlena.c` |
| `to_bin64` | 6331 | `varlena.c` |
| `to_oct32` | 6332 | `varlena.c` |
| `to_oct64` | 6333 | `varlena.c` |
| `unistr` | 6198 | `varlena.c` |
| `unicode_version` | 4549 | `varlena.c` |
| `unicode_assigned` | 6105 | `varlena.c` |
| `unicode_normalize_func` | 4350 | `varlena.c` |
| `unicode_is_normalized` | 4351 | `varlena.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_vlmisc_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verified against
      `../pgrust-fabled/vendor/postgres-src`). All `#error` gates removed
      with their pastes. Unicode data tables + unicode_norm.c +
      levenshtein.c vendored wholesale under `core/csrc/vlmisc/` with
      provenance banners (include paths flattened, marked SHIM inline).
- [x] Document every shim in the file header (9 numbered shims: fmgr
      unwrap, UTF8 encoding fence, ereport->errcode-class+longjmp, plain-4B
      text framing, StringInfo, List, TLS arena, symbol isolation, bswap).
      Errcode classes 1=22023, 2=42601, 3=22021, 4=0A000, 9=internal.
- [x] palloc/palloc0/repalloc/pfree on the TLS arena (MAX raised 64->512,
      documented); every `pg_diff_*` entry calls `pg_diff_arena_reset()`
      first, then `pg_diff_errcode = 0`, then arms setjmp.
- [x] `pg_diff_*` driver entries written (15: six to_* conversions, unistr,
      unicode_version/assigned/normalize/is_normalized, both levenshtein
      instantiations, both split helpers).
- [ ] Uncomment the `.file("csrc/pg_vlmisc_io.c")` line in `core/build.rs`.
      (PARENT'S FLIP — sibling vltext/vlbytea oracles are mid-fill in this
      worktree. Verified independently: `cc -fsyntax-only` and full `cc -c
      -Wall` clean; linked + executed via a scratch harness, see below.)

## 2. Implement the Rust driver (`core/src/vlmisc_diff.rs`)

- [x] Every `*_diff` arm filled (15 arms; selector % 15), all three
      comparison planes; `todo!()`s and `#![allow(dead_code)]` gone.
      Hand-added non-oid arms: varstr_levenshtein,
      varstr_levenshtein_less_equal, SplitIdentifierString, SplitGUCList
      (split verdict + 0x1F-joined list image).
- [x] fc-wrapper plane on arms 0-10 (fc_to_*, fc_unistr,
      fc_unicode_version/assigned/normalize_func/is_normalized). Arms
      11-14 are core-only: no fc_* wrapper exists (fuzzystrmatch /
      backend-internal helpers). No wrapper here takes an escontext.
- [x] SKIPPED rows + executable fences recorded in the module header:
      icu_unicode_version (state), NUL fence, utf8-walk fence (FINDING:
      Rust unicode_assigned panics where C 22021s), KNOWN-BUG fence
      (split_* panic on dangling separator — P1, reported), U+11A7
      oracle-version carve (unicode_norm implements post-18.3 C 273fe94),
      ASCII separator fence.
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin vlmisc_diff`
      green on stable. `cargo test` link is gated on the build.rs flip
      (tests are written and `#[ignore]`d with that message); the full
      suite was executed against the real oracle via a scratch harness:
      6/6 arm smokes green + 59-seed replay green + 3.6M-exec random/
      mutation soak green (2026-07-31).

## 3. Seeds, dictionary, corpus

- [x] `fuzz/vlmisc_diff.dict` extended (unistr escape forms, surrogate hex
      bands, NFC/NFD/NFKC/NFKD in several casings, multibyte tokens,
      split punctuation, the 0x1F join sentinel).
- [x] `fuzz/corpus/vlmisc_diff/` seeded with 59 seeds covering every arm's
      ok + error shapes (COMMIT is the parent's: this lane does not run
      git). S3-bank at landing.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run vlmisc_diff` — floor for any fuzz-only claim:
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
