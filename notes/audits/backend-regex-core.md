# Audit: backend-regex-core

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Claude Fable 5 (Opus 4.8, 1M)
- Branch audited: `port/backend-regex-core` (assembled from the six
  `decomp/backend-regex-core-fam-*` family branches off
  `decomp/backend-regex-core-scaffold`)

## Scope

`c_sources` (CATALOG.tsv): `regcomp.c`, `regerror.c`, `regexec.c`,
`regexport.c`, `regfree.c`, `regprefix.c`. `regcomp.c` `#include`s the
compile-time machinery (`regc_color.c`, `regc_cvec.c`, `regc_lex.c`,
`regc_locale.c`, `regc_nfa.c`, `regc_pg_locale.c`); `regexec.c` `#include`s
`rege_dfa.c`. C ground truth under `../pgrust/postgres-18.3/src/backend/regex/`
and `src/include/regex/{regguts.h,regcustom.h,regex.h,regexport.h,regerrs.h}`.

The unit was decomposed into six family modules:
`regex_foundation` (regc_cvec.c + regc_color.c), `regex_nfa` (regc_nfa.c +
the colormap↔arc bridge in `regex_nfa/nfacolor.rs`), `regex_locale`
(regc_locale.c + regc_pg_locale.c), `regex_compile` (regcomp.c + regc_lex.c),
`regex_exec` (regexec.c + rege_dfa.c + regprefix.c), `regex_export_free_error`
(regexport.c + regfree.c + regerror.c + the opaque-handle registry).
`regguts`/`regex_consts`/`regex_error` are crate-root type-contract modules.

## 1. Function inventory

Enumerated every top-level definition across all 13 C files (`regc_*.c`,
`rege_dfa.c`, plus the six named `c_sources`); cross-checked against
`../pgrust/c2rust-runs/backend-regex-core/src/*.rs`. All `dump*`/`stdump`/
`stid`/`dump`/`dumpst` functions are gated under `#ifdef REG_DEBUG`
(regc_nfa.c:1597+, 3644+; regcomp.c) and excluded from the build config — they
are absent from c2rust and correctly not ported.

By-name diff of the ~201 non-debug C functions against the port's function
set found four C names with no like-named Rust fn:

| C fn | Port handling | Verdict |
|------|---------------|---------|
| `freev` (regcomp.c) | `pg_regcomp` error/cleanup path; the per-field frees (`rfree`/`freenfa`/`freesubre`/`cleanst`/`freecvec`/`freelacons`) and the `ERR(err)`-return are subsumed by `RegResult` propagation + Rust ownership Drop. | MATCH (idiomatic) |
| `rfree` (regcomp.c) | The `regex_t` freer; `RegexT`/`Box<Guts>`/arena `Vec`s drop on scope (see `pg_regfree`/`seam_pg_regfree`). `re_magic`/`magic` invalidation has no analogue (ownership prevents reuse). | MATCH (idiomatic) |
| `freelacons` (regcomp.c) | The lacons vector drops with `Vars`/`Guts`. | MATCH (idiomatic) |
| `freedfa` (rege_dfa.c) | Explicit `drop(d)`/`drop(s)` at each C `freedfa` site (regex_exec.rs:1437,1470,1483,1517,1518) + scope-drop of per-tree/per-lacon DFAs (regex_exec.rs:1795). | MATCH (idiomatic) |

Every other non-debug C function has a same-named (or directly mapped, e.g.
`dupnfa_cross` for the cross-NFA `dupnfa`) port function. No `MISSING`,
`PARTIAL`, or `DIVERGES` verdicts.

## 2. Per-function comparison (representative + all integration-touched)

The audit read C / c2rust / port for every family. Highlights and the
functions touched during assembly:

| Function | C location | Port location | Verdict | Notes |
|----------|-----------|---------------|---------|-------|
| `dupnfa` / `duptraverse` | regc_nfa.c:1354/1378 | regex_nfa.rs:1183/1216 | MATCH | `start==stop` EMPTY-arc shortcut; tmp marks dup; cleartraverse runs unconditionally (first error wins). |
| `dupnfa_cross` / `duptraverse_cross` | regc_nfa.c:1354 (cross-NFA call from `nfanode`) | regex_nfa.rs:1346/1388 | MATCH | **Implemented this round** (was the one residual `todo!()`). Arena split: `tmp` scribbled on `src` states (→ `dst` ids), duplicates `newstate`'d in `dst`, arcs copied via `newarc(dst,…,has_parent=true)` reading `src` arc (type,co); `cleartraverse` walks `src`. Mirrors C `dupnfa`/`duptraverse` exactly. |
| `compact` | regc_nfa.c | regex_nfa.rs:3067 | MATCH | Two-pass flat-arena lowering; PLAIN→`co`, LACON→`ncolors+co` sets HASLACONS; COLORLESS terminator per state; CNFA_NOPROGRESS on pre + pre-out targets; `ncolors=maxcolor+1`; ESPACE on usize overflow. |
| `specialcolors` | regc_nfa.c | regex_nfa.rs:1566 | MATCH | `parent` arg threads the C `nfa->parent` BOS/EOS inheritance; top-level allocates pseudocolors, child inherits. |
| `makesearch` | regcomp.c:621 | regex_compile.rs:2995 | MATCH | `has_parent` threaded (called on top NFA `false`, on child `true` from `nfanode`); anchored test, implicit `.*`/`^*`/`\A*`, progress/no-progress split. |
| `nfanode` | regcomp.c:2348 | regex_compile.rs:3086 | MATCH | child NFA via `newnfa(parent)`, `dupnfa_cross`, specialcolors(inherit)/optimize/makesearch/compact, freenfa. |
| `element` | regc_locale.c | regex_locale.rs:652 | MATCH | Returns `ElementResult{code,note_ulocale}`; callers OR `REG_ULOCALE` into `v` (the C `NOTE(REG_ULOCALE)` side effect threaded out of the leaf). `chrnamed` intentionally ignores the note (C saves/restores `v->err`). |
| `eclass` | regc_locale.c:500 | regex_locale.rs:755 | MATCH | `cflags` threaded for the `REG_FAKE` test; `cases` case-expansion via `allcases`. |
| `pg_regerror` + `rerrs[]` | regerror.c + regerrs.h | regex_export_free_error.rs:222 | MATCH | All 20 table rows verified field-by-field against `regerrs.h` (codes, names, explain strings identical, same order); `-1` sentinel special-cased; `default:` table-scan + unknown-code `0x%x` fallback. |
| `pg_regfree` | regfree.c | regex_export_free_error.rs:204 | MATCH | NULL guard at seam; `re_fns->free` dispatch ≡ Drop of owned `RegexT`. |

## 3. Seams and wiring

**Owned seam crate:** `backend-regex-core-seams` (covers
`regcomp.c`/`regexec.c`/`regprefix.c`/`regfree.c`). It declares exactly four
inward seams — `pg_regcomp`, `pg_regexec`, `pg_regprefix`, `pg_regfree`
(seam_core::seam! ×4). `init_seams()` in `lib.rs` installs all four via `set()`
and nothing else; `seams-init::init_all()` calls `backend_regex_core::init_seams()`
(seams-init/src/lib.rs:40). No uninstalled seam, no `set()` outside the owner.
The four adapters (`seam_pg_*` in `regex_export_free_error.rs`) are thin marshal
+ delegate (one engine call, opaque-`RegexHandle`↔`RegexT` registry marshal,
result conversion). PASS.

**Outward seam calls (to unported neighbors):** `regex_locale` routes the
underlying `pg_locale_t`/ICU lookups (`pg_newlocale_from_collation`,
`regex_wc_isclass`/`toupper`/`tolower`) through `backend-utils-adt-pg-locale`'s
seam, and mb helpers through `backend-utils-mb-mbutils`'s seam, and stack-depth
through `backend-utils-misc-stack-depth`'s seam. Each is a real unported-neighbor
dependency; the `pg_wc_is*` probe family and `pg_set_regex_collation`/
`pg_ctype_get_cache` logic are owned and implemented here. The colormap↔arc
bridge in `regex_nfa/nfacolor.rs` is all owned logic. No own-logic was replaced
by a seam call.

## 3b. Design conformance

- Opacity: no invented handles. The public `RegexHandle` (types_regex) is the
  pre-existing opaque token the ADT layer already uses; the engine works on the
  real `RegexT`/`Guts`/`Nfa`/`Cnfa` structs with typed `StateId`/`ArcId` arena
  indices (not opaque pointers). PASS (opacity inherited, not introduced).
- Allocating fns/seams take `Mcx` + return `PgResult`/`RegResult`: `newcvec`/
  `getcvec`/`newstate`/`newnfa`/`newarc`/… all thread `Mcx<'mcx>` and return
  `RegResult`; `seam_pg_regcomp` builds a transient `MemoryContext`. PASS.
- Per-backend globals: the compiled-regex registry is `thread_local!`
  (`REGEX_REGISTRY`), not a shared static. PASS.
- No locks held across `?`, no registry-shaped side tables beyond the
  thread-local handle map (the C cache equivalent), no unledgered divergence
  markers.

## 4. Gate

`cargo check --workspace` and `cargo test --workspace` both pass (regex-core
compiles clean, zero warnings in the crate; pre-existing unrelated warnings in
`backend-access-common-printtup` only). No `todo!()`/`unimplemented!()` remain
in own logic.

## Verdict

**PASS.** Every C function is `MATCH` (or idiomatic Drop-subsumed free
function); zero `MISSING`/`PARTIAL`/`DIVERGES`; seams correctly declared and
installed; design rules satisfied. The single residual `dupnfa_cross` `todo!()`
left by the family decomposition was implemented and verified against the C
`dupnfa`/`duptraverse` this round.
