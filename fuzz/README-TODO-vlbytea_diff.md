# README-TODO: vlbytea_diff (crates/backend/utils/adt/varlena)

Scaffolded by `fuzz/scaffold.py`. Ordered checklist to the campaign
done-gate, per `.claude/skills/fuzzuproof-crate/SKILL.md` (read it first;
oracle pin: PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) — never :latest, never 18.4).

Function rows given at scaffold time:

| function | oid | C source |
|---|---|---|
| `byteain` | 1244 | `varlena.c` |
| `byteaout` | 31 | `varlena.c` |
| `bytearecv` | 2412 | `varlena.c` |
| `byteasend` | 2413 | `varlena.c` |
| `byteaoctetlen` | 720 | `varlena.c` |
| `byteacat` | 2011 | `varlena.c` |
| `byteaeq` | 1948 | `varlena.c` |
| `byteane` | 1953 | `varlena.c` |
| `bytealt` | 1949 | `varlena.c` |
| `byteale` | 1950 | `varlena.c` |
| `byteagt` | 1951 | `varlena.c` |
| `byteage` | 1952 | `varlena.c` |
| `byteacmp` | 1954 | `varlena.c` |
| `bytea_larger` | 6393 | `varlena.c` |
| `bytea_smaller` | 6394 | `varlena.c` |
| `byteaGetByte` | 721 | `varlena.c` |
| `byteaSetByte` | 722 | `varlena.c` |
| `byteaGetBit` | 723 | `varlena.c` |
| `byteaSetBit` | 724 | `varlena.c` |
| `bytea_substr` | 2012 | `varlena.c` |
| `bytea_substr_no_len` | 2013 | `varlena.c` |
| `byteaoverlay` | 749 | `varlena.c` |
| `byteaoverlay_no_len` | 752 | `varlena.c` |
| `byteapos` | 2014 | `varlena.c` |
| `bytea_bit_count` | 6163 | `varlena.c` |
| `bytea_int2` | 6370 | `varlena.c` |
| `bytea_int4` | 6371 | `varlena.c` |
| `bytea_int8` | 6372 | `varlena.c` |
| `int2_bytea` | 6367 | `varlena.c` |
| `int4_bytea` | 6368 | `varlena.c` |
| `int8_bytea` | 6369 | `varlena.c` |
| `bytea_reverse` | 6382 | `varlena.c` |
| `hashvarlena` | 456 | `varlena.c` |
| `hashvarlenaextended` | 772 | `varlena.c` |
| `hashbytea` | 6413 | `varlena.c` |
| `hashbyteaextended` | 6414 | `varlena.c` |

## 1. Vendor the C oracle (compile gate)

- [x] Paste VERBATIM upstream C into `core/csrc/pg_vlbytea_io.c` at every
      `TODO(scaffold)` site, from `src/backend/utils/adt/...` @
      `62d6c7d3df6287f1bd83199c1a746e50d31571a0` (re-verified against
      `../pgrust-fabled/vendor/postgres-src`). All `#error` gates removed
      with their pastes. Also vendored: encode.c hex codec, hashfunc.c hash
      wrappers + hashfn.c kernels (renamed `vlbytea_hash_bytes*`, static —
      SYMBOL ISOLATION vs pg_mac_io.c's copy), detoast.c plain-slice arm,
      pg_bitutils.c popcount table, common/int.h pg_add_s32_overflow,
      mbutils.c/wchar.c pg_mblen_range core (22021 arm of the hex-digit
      errmsg).
- [x] Document every shim in the file header (plumbing only, never logic:
      ereport -> errcode class + longjmp, fmgr unwrapping, caller buffers,
      bytea_output GUC as a settable static = environment pinning). Errcode
      classes 1..7 + 99 mapped to `PG_DIFF_ERR_*` constants.
- [x] Keep palloc/palloc0/repalloc/pfree on the emitted TLS arena (models
      PG's memory-context reset; error paths strand allocations otherwise
      — the 2026-07-31 LSan incident class, proofs/p1-lanej @ 7306d300196).
      No hand `free()` of arena pointers; every `pg_diff_*` entry calls
      `pg_diff_arena_reset()` first (via PG_VLBYTEA_ENTRY()).
- [x] Write the `pg_diff_*` driver entries (section pattern in the file;
      `pg_diff_arena_reset()` then `pg_diff_errcode = 0` per entry).
- [ ] Uncomment the `.file("csrc/pg_vlbytea_io.c")` line in `core/build.rs`.
      PARENT-OWNED (agent instructed not to edit build.rs while sibling
      lanes fill their oracles). Oracle compiles clean standalone:
      `cc -fsyntax-only`/-c with -fno-strict-aliasing -fwrapv
      -ffp-contract=off. NOTE for the parent: also add
      `detoast = { path = "../../crates/backend/access/common/detoast" }`
      to fuzz/core/Cargo.toml and install `detoast::init_seams` at campaign
      boot — the driver probes the detoast_attr_slice seam and carves the
      substr/overlay slice-reaching execs when it is uninstalled (see the
      module doc in core/src/vlbytea_diff.rs).

## 2. Implement the Rust driver (`core/src/vlbytea_diff.rs`)

- [x] Fill each `*_diff` arm: C oracle call, shipped-Rust core call, ALL
      THREE comparison planes (value bytes/bits + Ok/Err verdict +
      errcode/sqlstate class; message text out of scope). All 36 arms
      implemented; every `todo!()` removed.
- [x] fc-wrapper plane per arm via `fc_call` / `varlena::builtins::fc_*`
      (wrapper == core: Datum value / bytes / error verdict + sqlstate);
      fc_byteain additionally driven in the soft-error `ErrorSaveNode`
      shape (the only escontext-taking wrapper in the family).
- [x] Record every SKIPPED row in the module header with its reason
      (bytea_sortsupport, bytea_string_agg_transfn/finalfn, toasted arg
      forms); the detoast-seam carve is an executable probe, not a
      comment-only carve.
- [x] Extern decls live; `#![allow(dead_code)]` dropped; tests written
      un-ignored (per-arm ok+error smoke, fc-plane smoke, seed-corpus
      replay, selector soup). They link once the parent uncomments the
      csrc gate; verified locally by linking the compiled oracle object
      via RUSTFLAGS: 11/11 green + slice-plane grid + 2M-exec random soak
      clean (2026-07-31).
- [x] `cargo check --manifest-path fuzz/Cargo.toml --bin vlbytea_diff`
      green on stable (no warnings in the module). Full
      `cargo test --manifest-path fuzz/core/Cargo.toml` is gate-blocked on
      the parent's build.rs uncomment (sibling oracles link in the same
      binary).

## 3. Seeds, dictionary, corpus

- [x] Extend `fuzz/vlbytea_diff.dict` (byteain hex/escape/octal tokens,
      whitespace set, truncated-multibyte 22021 leads).
- [x] Seed `fuzz/corpus/vlbytea_diff/` — 77 seeds written (byteain forms,
      cmp witness pairs both orders, banded index edges, substr/overlay
      shapes, bytea_int2/4/8 lengths 0..9, hash seeds). COMMIT still owed:
      this agent was instructed not to run `git add`; parent runs the plain
      `git add` (no `-f`) + S3-bank.

## 4. Campaign (nightly toolchain)

- [ ] Sancov on the C oracle side too (union coverage, NEZHA finding).
- [ ] `cargo +nightly fuzz run vlbytea_diff` — floor for any fuzz-only claim:
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
