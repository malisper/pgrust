# Audit: backend-tsearch-ispell-regis

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]
- **Branch:** port/backend-tsearch-ispell-regis
- **C sources:** `src/backend/tsearch/regis.c`, `src/backend/tsearch/dict_ispell.c` (postgres-18.3)
- **c2rust:** `c2rust-runs/backend-tsearch-ispell-regis/src/{regis,dict_ispell}.rs`
- **Port:** `crates/backend-tsearch-ispell-regis/src/{regis,dict_ispell,lib}.rs`

## Top-line verdict: PASS

Every C function is `MATCH` or properly `SEAMED`. Zero seam findings, zero
design-conformance findings. Build of the owning crate is clean.

## Function inventory and verdicts

### regis.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `RS_isRegis` | regis.c:30-71 | regis.rs:131-171 (`rs_is_regis`) | MATCH | Same 4-state machine (WAIT/ONEOF/ONEOF_IN/NONEOF), same branch predicates, same `t_iseq` `[`,`^`,`]` literals, same default-state `elog(ERROR)` → `Err(XX000)` "internal error in RS_isRegis: state N", same final `state == RS_IN_WAIT` result. `t_isalpha`/`pg_mblen` SEAMED. |
| `newRegisNode` (static) | regis.c:73-82 | regis.rs:256-271 (`push_node`) | MATCH | C `palloc0(RNHDRSZ+len+1)` + linked-list append → owned `PgVec` push with fallible `try_reserve`/`oom`. `len`-sized allocation is purely a C capacity detail (growable vec), no behavioral effect. |
| `RS_compile` | regis.c:84-158 | regis.rs:177-250 (`rs_compile`) | MATCH | memset+issuffix init, identical per-state node creation / `ts_copychar` accumulation, all three `shouldn't get here` `elog(ERROR)` → `invalid_regis_pattern` (XX000) plus the post-loop `state != RS_IN_WAIT` check, default-state internal-error, and final nchar=node-count walk. `ptr`-chasing replaced by `cur: Option<usize>` index — behavior identical. |
| `RS_free` | regis.c:160-174 | regis.rs:95-99 (`Regis::free`) | MATCH | C pfree-chain + `node=NULL` → `nodes.clear()` (drop uncharges) + nchar reset. |
| `mb_strchr` (static) | regis.c:176-205 | regis.rs:287-302 | MATCH | Same `while (*ptr && !res)` walk, only same-byte-length classes compared, byte-for-byte equality, advance by `plen`. The C reverse `while(i--)` byte loop is equivalent to the slice `==` over `clen` bytes. |
| `RS_execute` | regis.c:207-252 | regis.rs:308-349 (`rs_execute`) | MATCH | Char-count, `len < nchar` early-out, issuffix skip of `len-nchar` leading chars, per-node ONEOF/NONEOF membership via `mb_strchr`, advance one node + one char. Unrecognized-type `elog(ERROR)` is unreachable in the owned 2-variant enum (C default arm cannot fire), correctly elided. |
| `ts_copychar_with_len` (inline, ts_locale.h) | n/a | folded into `copy_char_into` (regis.rs:277-281) | MATCH | memcpy of `len` bytes → `extend_from_slice`. |
| `ts_copychar_cstr` (inline, ts_locale.h) | n/a | folded into `copy_char_into` | MATCH | `ts_copychar_with_len(dest,src,pg_mblen_cstr(src))`; port copies the already-sliced whole char `ch = &c[..clen]`. |
| `t_iseq` (inline, ts_locale.h) | n/a | regis.rs:124-126 | MATCH | ASCII leading-byte compare; ported in-crate (pure). |

### dict_ispell.c

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `dispell_init` | dict_ispell.c:30-109 | dict_ispell.rs:38-99 | MATCH (with SEAMED callees) | palloc0 DictISpell → owned struct; `NIStartBuild`/`NIImport{Dictionary,Affixes}`/`NISort{Dictionary,Affixes}`/`NIFinishBuild` SEAMED to spell unit threading `SpellHandle`; `get_tsearch_config_filename`/`readstoplist` SEAMED to ts_utils; `defGetString`/`str_tolower` SEAMED. Same dictfile/afffile/stopwords dispatch with same duplicate-flag guards, same `ERRCODE_INVALID_PARAMETER_VALUE` errmsgs ("multiple DictFile/AffFile/StopWords parameters", "unrecognized Ispell parameter", "missing AffFile/DictFile parameter"), same `affloaded && dictloaded` → sort, `else if !affloaded`, `else` ordering. `readstoplist(..., str_tolower)` → `readstoplist(..., lowercase=true)` (wordop selector). |
| `dispell_lexize` | dict_ispell.c:111-149 | dict_ispell.rs:106-144 | MATCH (with SEAMED callees) | `len <= 0` → `None`; `str_tolower(in,len,DEFAULT_COLLATION_OID=100)` SEAMED; `NINormalizeWord` SEAMED; `res == NULL` → `None`; the cptr<=ptr stop-word compaction is faithfully an in-place retain (skip stop words, keep the rest, preserving nvariant/flags/lexeme — the C whole-struct memcpy). C `NULL`-lexeme terminator = end of the seam's `PgVec` (no sentinel entry to skip). DEFAULT_COLLATION_OID=100 verified against c2rust const. |

## Seam audit

**Owned seam crate (by C-source coverage):** `backend-tsearch-ispell-regis-seams`
(maps to `dict_ispell.c` — the fmgr template methods). It declares
`dispell_init` and `dispell_lexize`; both are installed by the crate's
`init_seams()` (lib.rs:25-28), which contains only `set()` calls.
`seams-init::init_all()` calls `init_seams()` (seams-init/src/lib.rs:49). No
uninstalled owned seam; no `set()` outside the owner. `regis.c` has no owned
inward seam crate (its callers depend on this crate directly — correct, leaf).

**Outward seams** — all thin marshal+delegate, each justified by a real
unported-neighbor dependency, no branching/computation in the seam path:

- `backend-tsearch-ts-locale-seams::t_isalpha` — `t_isalpha_cstr` (ts_locale.c unported).
- `backend-utils-mb-mbutils-seams::pg_mblen_range` — `pg_mblen_cstr` (mbutils.c unported).
- `backend-tsearch-spell-seams::{spell_start_build, spell_import_dictionary, spell_import_affixes, spell_sort_dictionary, spell_sort_affixes, spell_finish_build, spell_normalize_word}` — the `NI*` ISpell pipeline (spell.c unported). Threads opaque `SpellHandle` (inherited opacity: C embeds full `IspellDict obj`; resolved to real struct when spell unit lands — no invented stand-in).
- `backend-tsearch-ts-utils-seams::{get_tsearch_config_filename, readstoplist, searchstoplist}` — ts_utils.c unported.
- `backend-utils-adt-formatting-seams::str_tolower` — formatting.c unported.
- `backend-commands-define-seams::def_get_string` — defGetString (define.c unported).

All outward seam signatures return `PgResult` where the C can `ereport(ERROR)`
(import/sort/normalize/config/tolower all can error); the pure predicates
(`t_isalpha`, `pg_mblen_range`, `searchstoplist`) are infallible. Allocating
seams (`get_tsearch_config_filename`, `readstoplist`, `str_tolower`,
`spell_normalize_word`) take `Mcx<'mcx>`. No outward call performs node
construction or branching beyond arg/result conversion.

## Design conformance (step 3b)

- Opacity: `SpellHandle` is inherited (C `IspellDict obj` embedded struct), not invented — conforms to types.md rules 6-7.
- Allocations: every allocating fn/seam carries `Mcx` + `PgResult`; node/data/lexeme pushes use `try_reserve` + `mcx.oom`. No `&'static mut`.
- No shared statics for per-backend globals, no ambient-global seams, no locks held across `?`, no registry-shaped side tables, no unledgered divergence markers.
- Error parity: every `elog(ERROR)`/`ereport(ERROR)` maps to a `PgError` with matching SQLSTATE (XX000 internal for regis; `ERRCODE_INVALID_PARAMETER_VALUE` for ispell options) and ERROR severity, fired under identical predicates.

## Result

**PASS** — all functions MATCH or properly SEAMED; zero seam findings; zero
design findings.
