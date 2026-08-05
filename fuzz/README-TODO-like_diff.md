# README-TODO: like_diff (crates/backend/utils/adt/like)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `textlike` | 850 | `like.c` |
| `textnlike` | 851 | `like.c` |
| `namelike` | 858 | `like.c` |
| `namenlike` | 859 | `like.c` |
| `texticlike` | 1633 | `like.c` |
| `texticnlike` | 1634 | `like.c` |
| `nameiclike` | 1635 | `like.c` |
| `nameicnlike` | 1636 | `like.c` |
| `like_escape` | 1637 | `like.c` |
| `bytealike` | 2005 | `like.c` |
| `byteanlike` | 2006 | `like.c` |
| `like_escape_bytea` | 2009 | `like.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_like_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verify against
      `../pgrust-fabled/vendor/postgres-src`). Remove each `#error` gate
      together with its paste — never before.
      (like.c core spliced byte-for-byte from the vendored checkout;
      like_match.c pasted VERBATIM once per stamping exactly as like.c
      #includes it; asc_tolower/pg_ascii_tolower/pg_utf_mblen/pnstrdup/
      cstring_to_text vendored verbatim from their TUs.)
- [x] Document every shim in the file header (plumbing only, never logic:
      ereturn -> int sentinel, fmgr unwrapping, caller buffers, C-locale
      ctype shims). Map each errcode to a `PG_DIFF_ERR_*` class constant.
      (`PG_DIFF_LIKE_ERR_*`: 1=22025, 2=42P22, 3=0A000, 4=22021; dead
      locale arms shimmed ABORT-LOUD.)
- [x] Keep palloc/palloc0/repalloc/pfree on the emitted TLS arena (models
      PG's memory-context reset; error paths strand allocations otherwise
      — the 2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
      No hand `free()` of arena pointers; every `pg_diff_*` entry calls
      `pg_diff_arena_reset()` first.
- [x] Write the `pg_diff_*` driver entries (section pattern in the file;
      `pg_diff_arena_reset()` then `pg_diff_errcode = 0` per entry).
      (`pg_diff_like_*`, plus `pg_diff_like_set_encoding` for the
      UTF8/LATIN1 plane selector.)
- [x] Uncomment the `.file("csrc/pg_like_io.c")` line in `core/build.rs`.

## 2. Implement the Rust driver (`core/src/like_diff.rs`)

- [x] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). Remove each
      `todo!()` with its arm.
- [x] fc-wrapper plane per arm via `fc_call` / `adt_like::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      soft-error `ErrorSaveNode` shape where the wrapper takes an escontext.
      (No LIKE wrapper takes an escontext — noted in the module header; the
      ic and escape arms drive the fn_extra scratch install + reuse
      branches through one resolved FmgrInfo.)
- [x] Record every SKIPPED row (stateful/PRNG/clock/locale) in the module
      header with its reason; executable exceptions per the skill (never
      comment-only carves). (Alias/bpchar oids = same PGFunction; the five
      prosupport rows; non-UTF8-multibyte encodings; nondeterministic
      collations — dead arms abort-loud in the oracle.)
- [x] HARDENING PASS (2026-07-31): direct kernel arms 12/13/14 diff the pub
      wrappers sb_match_text / utf8_match_text / sb_imatch_text against the
      C SB_MatchText / UTF8_MatchText / SB_IMatchText stampings on the raw
      TRUE/FALSE/ABORT tristate (raw-byte domain, None-vs-Some(C) locale
      plane); every exec additionally runs the five *_support wrappers'
      unhandled-tag NULL leg (fc_support_unhandled_tag_plane — the
      Selectivity/IndexCondition panic legs stay deliberately untriggered).
      Selector widened to %15; 12 kernel witness seeds + dict tokens added.
- [x] Uncomment the extern decls; drop `#![allow(dead_code)]`; un-ignore and
      flesh out the tests (per-arm ok+error smoke, fc-plane smoke,
      seed-corpus replay).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin like_diff` and
      `cargo test --manifest-path fuzz/core/Cargo.toml` green on stable.
      (Plus the #[ignore]d exhaustive kernel sweep: 609,961 (text,pattern)
      pairs over {a,b,%,_,\} len<=4, UTF8 plane, 0.65 s, zero divergences.)

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/like_diff.dict` (CmpLog + dictionary day-one for
      parser-shaped targets; tokens from the vendored regress SQL literals).
- [x] Seed `fuzz/corpus/like_diff/` (46 seeds: wildcard/escape witness
      pairs, both encoding planes, collation-error arm, bytea NULs,
      multibyte-boundary shapes). COMMIT is the lane coordinator's git step
      (plain `git add`, no `-f`) + S3-bank it.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run like_diff` — floor for any fuzz-only claim:
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
