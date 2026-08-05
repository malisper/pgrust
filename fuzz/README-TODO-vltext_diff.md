# README-TODO: vltext_diff (crates/backend/utils/adt/varlena)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `textin` | 46 | `varlena.c` |
| `textout` | 47 | `varlena.c` |
| `textlen` | 1257 | `varlena.c` |
| `textoctetlen` | 1374 | `varlena.c` |
| `textcat` | 1258 | `varlena.c` |
| `text_substr` | 877 | `varlena.c` |
| `text_substr_no_len` | 883 | `varlena.c` |
| `textpos` | 849 | `varlena.c` |
| `texteq` | 67 | `varlena.c` |
| `textne` | 157 | `varlena.c` |
| `text_lt` | 740 | `varlena.c` |
| `text_le` | 741 | `varlena.c` |
| `text_gt` | 742 | `varlena.c` |
| `text_ge` | 743 | `varlena.c` |
| `bttextcmp` | 360 | `varlena.c` |
| `text_larger` | 458 | `varlena.c` |
| `text_smaller` | 459 | `varlena.c` |
| `text_pattern_lt` | 2160 | `varlena.c` |
| `text_pattern_le` | 2161 | `varlena.c` |
| `text_pattern_ge` | 2163 | `varlena.c` |
| `text_pattern_gt` | 2164 | `varlena.c` |
| `bttext_pattern_cmp` | 2166 | `varlena.c` |
| `btvarstrequalimage` | 5050 | `varlena.c` |
| `text_starts_with` | 3696 | `varlena.c` |
| `replace_text` | 2087 | `varlena.c` |
| `split_part` | 2088 | `varlena.c` |
| `textoverlay` | 1404 | `varlena.c` |
| `textoverlay_no_len` | 1405 | `varlena.c` |
| `textsend` | 2415 | `varlena.c` |
| `textrecv` | 2414 | `varlena.c` |
| `unknownin` | 109 | `varlena.c` |
| `unknownout` | 110 | `varlena.c` |
| `unknownrecv` | 2416 | `varlena.c` |
| `unknownsend` | 2417 | `varlena.c` |
| `hashtext` | 400 | `varlena.c` |
| `hashtextextended` | 448 | `varlena.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_vltext_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
      (Done: varlena.c text family + hashfunc.c hashtext/-extended +
      hashfn.c hash_bytes/-extended + wchar.c UTF8 verify/mblen + mbutils.c
      mblen/verify walks + pqformat.c getmsgtext/typsend + common/int.h
      overflow helpers. `cc -fsyntax-only`/`cc -c` clean with build.rs flags;
      only pg_diff_vltext_* symbols exported.)
- [x] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
      (Classes 1..6 = 22011/22003/22023/22021/08P01/42P22, 98 internal;
      environment fence documented: db encoding UTF8, C collation 950,
      client encoding SQL_ASCII identity.)
- [x] Keep palloc/palloc0/repalloc/pfree on the emitted TLS arena (models
      PG's memory-context reset; error paths strand allocations otherwise
      — the 2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
      No hand `free()` of arena pointers; every `pg_diff_*` entry calls
      `pg_diff_arena_reset()` first.
      (PG_DIFF_ARENA_MAX raised 64 -> 512 for the replace/split/overlay
      loop + StringInfo growth + entry-side argument images; documented at
      the arena.)
- [x] Write the `pg_diff_*` driver entries (section pattern in the file;
      `pg_diff_arena_reset()` then `pg_diff_errcode = 0` per entry).
      (Entries are `pg_diff_vltext_*` per the build.rs SYMBOL ISOLATION
      note; everything else is static.)
- [ ] Uncomment the `.file("csrc/pg_vltext_io.c")` line in `core/build.rs`.
      (LEFT TO THE PARENT by charter — sibling vlbytea/vlmisc oracles are
      mid-fill in this worktree. Verified out-of-tree: in a scratch copy of
      fuzz/ with the gate uncommented and the sibling modules stubbed, the
      oracle links and all vltext smoke tests + a 300k-exec randomized soak
      pass with zero divergences.)

## 2. Implement the Rust driver (`core/src/vltext_diff.rs`)

- [x] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm. (All 36 functions implemented across 28 arm
      bodies; no skipped rows. One input-domain fence: text_starts_with
      takes valid-UTF-8/NUL-free texts — executable gate + rationale in the
      module header.)
- [x] fc-wrapper plane per arm via `fc_call` / `varlena::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
      (No varlena text-family wrapper takes an escontext; error wrappers are
      exercised via the InvalidOid collation flag and the error decodes.)
- [x] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves). (None skipped; locale/ICU collation arms are the
      chartered campaign carve, documented as the C-collation fence.)
- [x] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay). (Tests carry
      `#[ignore = "gate: ..."]` because they LINK against the oracle: they
      run once the parent uncomments the build.rs gate — all verified green
      in the scratch harness.)
- [ ] `cargo check --manifest-path fuzz/Cargo.toml --bin vltext_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.
      (vltext_diff.rs itself is diagnostics-clean; the in-tree lib check is
      currently blocked by the CONCURRENT sibling scaffolds' own compile
      errors in vlbytea_diff.rs (unresolved guc_tables/detoast). With those
      modules stubbed in a scratch copy, `cargo check --bin vltext_diff` and
      `cargo test -p decoder_fuzz vltext` are green. Re-run in-tree once the
      siblings land.)

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/vltext_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
      (Separators/needles, 2/3/4-byte UTF-8 + truncations/overlongs/
      surrogates, split-length and i32 band headers, collation flag bytes.)
- [ ] Seed `fuzz/corpus/vltext_diff/` (>=30 seeds; `gen_seeds.sh` pattern) and
      COMMIT the corpus (plain `git add`, no `-f`) + S3-bank it.
      (60 sha1-named seeds WRITTEN covering every arm incl. all error
      classes and single-byte-difference witness pairs; replay green in the
      scratch harness. `git add`/commit + S3 bank left to the parent per the
      no-git constraint.)

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run vltext_diff` — floor for any fuzz-only claim:
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
