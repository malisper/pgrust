# Audit: backend-utils-adt-like

Function-by-function audit of `crates/backend-utils-adt-like` against
PostgreSQL 18.3 `src/backend/utils/adt/like.c`, `like_match.c`, and
`like_support.c` and the prior src-idiomatic port.

## Scope

Ported: `like.c` + `like_match.c` (the matcher template `#include`d four times).
NOT ported: `like_support.c` (justified below).

## like.c — every function

| C function | Rust | Verdict |
|---|---|---|
| `wchareq` (static inline) | `wchareq` | OK. First-byte fast path, then `pg_mblen_with_len` on both, compare lengths, then byte loop. C returns `int 0/1`; Rust returns `bool` (only used as a boolean by `CHAREQ`). Infallible (the mblen seam is infallible in the repo). |
| `SB_lower_char` (static) | `SB_lower_char` | OK. Three legs match exactly: `ctype_is_c → pg_ascii_tolower`; `is_default → pg_tolower`; else `tolower_l(c, info.lt)` → `char_tolower(c, collation)` seam. The flag-core handle does not carry `info.lt`, so the seam re-keys by collation Oid (the owner re-resolves). |
| `GenericMatchText` (static inline) | `GenericMatchText` | OK. `collation==0 → ereport(INDETERMINATE_COLLATION, "...for LIKE")`; `pg_newlocale_from_collation`; then `max_length==1 → SB`, `GetDatabaseEncoding()==PG_UTF8 → UTF8`, else `MB`. SQLSTATE 42P22, message + hint verbatim. |
| `Generic_Text_IC_like` (static inline) | `Generic_Text_IC_like` | OK. `collation==0 → "...for ILIKE"`; resolve locale; `!deterministic → ereport(FEATURE_NOT_SUPPORTED 0A000, "nondeterministic collations are not supported for ILIKE")`; then `max_length>1 || provider==ICU → lower() both + UTF8/MB match with NULL locale`, else `SB_IMatchText`. The two `lower()` `DirectFunctionCall1Coll(lower, collation, ...)` map to `formatting::case::str_tolower(mcx, payload, collation)` (= SQL `lower()`, oracle_compat.c). |
| `namelike` | `namelike` | OK. `NameStr` + `strlen` NUL-trim via `name_str`; `GenericMatchText(...) == LIKE_TRUE`. |
| `namenlike` | `namenlike` | OK. `!= LIKE_TRUE`. |
| `textlike` | `textlike` | OK. `== LIKE_TRUE` on payload bytes. |
| `textnlike` | `textnlike` | OK. `!= LIKE_TRUE`. |
| `bytealike` | `bytealike` | OK. `SB_MatchText(..., NULL) == LIKE_TRUE` (no collation, NULL locale). |
| `byteanlike` | `byteanlike` | OK. `!= LIKE_TRUE`. |
| `nameiclike` | `nameiclike` | OK. `name_text(NameGetDatum)` → `varlena::wire_io::name_text`; `Generic_Text_IC_like(...) == LIKE_TRUE`. |
| `nameicnlike` | `nameicnlike` | OK. `!= LIKE_TRUE`. |
| `texticlike` | `texticlike` | OK. |
| `texticnlike` | `texticnlike` | OK. |
| `like_escape` | `like_escape` | OK. `max_length==1 → SB_do_like_escape` else `MB_do_like_escape`; returns payload. |
| `like_escape_bytea` | `like_escape_bytea` | OK. Always `SB_do_like_escape`. |

## like_match.c — every function (template, instantiated 4×)

| C symbol | Rust | Verdict |
|---|---|---|
| `MatchText` (the body) | `match_text` | OK. Verified branch-for-branch against C lines 79–250: fast-`%` path, `check_stack_depth`, the main `while (tlen>0 && plen>0)` loop with the `\\`/`%`/`_`/nondeterministic/literal arms, the `%`-skip-wildcards inner loop, `firstpat` computation (incl. the escaped-firstpat `plen<2` error leg), the recursive `%`-search, the nondeterministic substring-partition branch (subpattern scan, unescaped copy, end-of-pattern `pg_strncoll` shortcut, growing-substring loop with `CHECK_FOR_INTERRUPTS`), and the tail (`tlen>0 → FALSE`, trailing-`%` consume, `LIKE_TRUE`/`LIKE_ABORT`). `NextByte` post-match advance preserved. |
| `MB_MatchText` (`#include` 1) | `MB_MatchText` | OK. `NextCharMode::MultiByte`, `CaseFold::None`. |
| `SB_MatchText` (`#include` 2) | `SB_MatchText` | OK. `SingleByte`, `None`. |
| `SB_IMatchText` (`#include` 3) | `SB_IMatchText` | OK. `SingleByte`, `SbLower` (MATCH_LOWER). Non-NULL locale + collation threaded. |
| `UTF8_MatchText` (`#include` 4) | `UTF8_MatchText` | OK. `Utf8` NextChar (`p++; while ((*p&0xC0)==0x80)`), `None`. |
| `do_like_escape` (the body) | `do_like_escape` | OK. `elen==0` doubles backslashes (with `CopyAdvChar`); else single-char-escape validation (`invalid escape string` 22025), `'\\'`-escape verbatim-copy shortcut, then the `afterescape` state machine converting escape→`\\` and doubling `\\` except right after an escape. `CHAREQ`/`CopyAdvChar`/`NextChar` parameterized by `NextCharMode`. |
| `SB_do_like_escape` / `MB_do_like_escape` | same names | OK. Thin wrappers; return `PgVec<'mcx,u8>` payload (C `palloc`+`SET_VARSIZE`; the varlena header is the layered Datum boundary's job). |
| `NextByte`/`NextChar`/`CHAREQ`/`CopyAdvChar`/`GETCHAR`/`MATCH_LOWER` macros | `next_char_len`/`copy_adv_char_len`/`chareq`/`getchar` | OK. Each reproduces the per-instantiation macro expansion. |

## Constants / SQLSTATEs

`LIKE_TRUE=1`, `LIKE_FALSE=0`, `LIKE_ABORT=-1`; `ERRCODE_INVALID_ESCAPE_SEQUENCE=22025`,
`ERRCODE_INDETERMINATE_COLLATION=42P22`, `ERRCODE_FEATURE_NOT_SUPPORTED=0A000`;
`C_COLLATION_OID=950`, `PG_UTF8=6`, `COLLPROVIDER_ICU='i'`. All verified against the C headers.

## like_support.c — NOT ported (justified)

Every entry point (`textlike_support`, `texticlike_support`, `textregexeq_support`,
`texticregexeq_support`, `text_starts_with_support`, and the `*sel`/`*joinsel`
selectivity functions) is a bare-word `PGFunction` handler (the registry is
deferred) whose body operates on planner `Node`/`Const`/`PlannerInfo` and the
`supportnodes.h` `SupportRequestSelectivity`/`SupportRequestIndexCondition`
types — none of which are modeled in `types-nodes`/`types-pathnodes` yet — and
reaches the unported selfuncs (`patternsel`, `prefix_selectivity`,
`scalarineqsel`) and index-qual machinery. There is no in-repo caller. Per the
project rule (seam-and-panic only for a reachable unported neighbor; bare-word
PGFunction registry deferred), porting the body now would require inventing the
entire planner-support node infrastructure. Deferred to when the planner-support
nodes + selfuncs land. Recorded on the CATALOG `backend-utils-adt-string-byte`
row, which retains `like_support.c`.

## Seams

Owns NO inward seams (the fmgr dispatcher depends on this crate directly), so
`init_seams()` is empty — confirmed correct by the seams-init guard.

Consumes (all installed by their owners' `init_seams`, or panic-until-owner-lands):
- mbutils: `pg_mblen_range` (= C `pg_mblen_with_len`), `pg_database_encoding_max_length`, `get_database_encoding`.
- pg-locale: `pg_newlocale_from_collation`, `pg_strncoll` (keyed by Oid), and NEW `char_tolower` (= `tolower_l`; owner pg_locale_libc unported → loud panic until it lands).
- stack-depth `check_stack_depth`; postgres `check_for_interrupts`.
Direct deps: `formatting::case::str_tolower` (SQL `lower()`), `varlena::wire_io::name_text`, `port_pgstrcasecmp::{pg_tolower, pg_ascii_tolower}`.

No `todo!`/`unimplemented!`; no own-logic stubs. OOM via `mcx`'s fallible
allocators. No locks held across `?`.

## Tests

23 tests pass: the SB matcher truth table, escape-at-end errors, `do_like_escape`
(no-escape / `\\`-escape / custom-escape state machine / multi-char error),
indeterminate-collation errors, `name_str` NUL-trim, `SB_lower_char` C-locale
fold, and the golden cases transcribed 1:1 from strings.out (LIKE/ILIKE/ESCAPE,
name + bytea, the `%`/`_` combo bug regressions) under C collation + single-byte
SQL_ASCII.

## Verdict: PASS
