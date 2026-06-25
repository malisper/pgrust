# Audit: backend-utils-adt-quote

- Date: 2026-06-13
- Model: Claude Fable 5 (Opus 4.8 1M)
- Branch: port/backend-utils-adt-quote
- Verdict: **PASS**

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Sources re-derived; the port's own comments/self-review were not trusted.

## Sources

- C: `../pgrust/postgres-18.3/src/backend/utils/adt/quote.c`
- c2rust completeness oracle: `../pgrust/c2rust-runs/backend-utils-adt-quote/src/quote.rs`
- Port: `crates/backend-utils-adt-quote/src/{lib.rs,seams.rs}`
- Owned seam crate: `crates/backend-utils-adt-quote-seams`

## Function inventory (oracle = c2rust)

c2rust function defs: `quote_ident`, `quote_literal_internal` (static),
`quote_literal`, `quote_literal_cstr`, `quote_nullable`
(`DatumGetPointer`/`PointerGetDatum` are inlined macros, not C functions). This
is the complete set in quote.c; all five present in the port.

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `quote_literal_internal` (static) | quote.c | lib.rs `quote_literal_internal` | MATCH | Backslash scan → emit `'E'` + break; opening `'`; per-byte loop doubling `SQL_STR_DOUBLE(c,true)` chars then copy; closing `'`. Byte-for-byte vs c2rust:641. Worst-case `len*2+3` validated vs `MAX_ALLOC_SIZE` (mirrors `AllocSizeIsValid`), fallible `try_reserve` → `ERRCODE_PROGRAM_LIMIT_EXCEEDED`. Returned vec `len()` == C return. `+VARHDRSZ`/`+1 NUL` are envelope artifacts dropped at this byte-content boundary. |
| `quote_literal_cstr` | quote.c | lib.rs `quote_literal_cstr` | MATCH (+ inward SEAM) | C: `strlen` → palloc `len*2+3+1` → internal → NUL-terminate. Port returns owned `String` (NUL dropped). This is the unit's only inward seam `backend_utils_adt_quote_seams::quote_literal_cstr` (`&str -> String`, infallible); OOM/UTF-8 `expect` mirror the `palloc` ereport at the no-Err seam boundary (UTF-8 unreachable: ASCII delimiters around valid-UTF-8 input). |
| `quote_literal` | quote.c | lib.rs `quote_literal` | MATCH | fmgr entry: detoast text, palloc worst-case `text`, internal into VARDATA, SET_VARSIZE. Port exposes the content transform on decoded bytes (`= quote_literal_internal`); varlena envelope deferred per repo-wide fmgr/Datum policy. |
| `quote_nullable` | quote.c | lib.rs `quote_nullable` | MATCH | `PG_ARGISNULL(0)` → text `"NULL"`; else `DirectFunctionCall1(quote_literal,...)`. Port: `None` → `b"NULL"`, `Some` → `quote_literal`. Matches c2rust:778. |
| `quote_ident` | quote.c | lib.rs `quote_ident` | SEAMED | `text_to_cstring` → `quote_identifier` → `cstring_to_text`. `quote_identifier` is defined in `ruleutils.c:13029` (NOT quote.c) → real cross-unit dep, reached via `backend_utils_adt_ruleutils_seams::quote_identifier` (declared; panics until ruleutils lands — correct mirror-PG-and-panic). cstring↔text is the fmgr boundary; no quote.c-side logic beyond the delegation. |

## Constants verified (against c.h, not memory)

- `ESCAPE_STRING_SYNTAX` = `'E'` — c.h:1141. Port `b'E'` (0x45). MATCH.
- `SQL_STR_DOUBLE(ch, eb)` = `(ch)=='\'' || ((ch)=='\\' && (eb))` — c.h:1138-1139.
  Port `sql_str_double` matches; callsite passes `escape_backslash = true`. MATCH.

## Seam audit

- Owned seam crate by C-source coverage (quote.c): `backend-utils-adt-quote-seams`,
  one inward decl `quote_literal_cstr`.
- `crate::init_seams()` is `set()`-only and installs `quote_literal_cstr`; no other
  owned declaration → no uninstalled seam. Wired into `seams-init::init_all()`
  (seams-init/src/lib.rs:159; dep at Cargo.toml:163).
- Outward seam `ruleutils::quote_identifier`: justified real cycle, thin
  marshal+delegate (one call + Mcx materialize), no branching/computation.
- No own-logic stubs, no `todo!()`/`unimplemented!()`, no invented opacity.
  Allocating fns take `Mcx`/`PgResult`; no shared statics, no locks.

## Gates

- `cargo test -p seams-init` recurrence_guard: both checks PASS
  (`every_seam_installing_crate_is_wired_into_init_all`,
  `every_declared_seam_is_installed_by_its_owner`).
- `cargo check --workspace`: clean (only pre-existing unrelated warnings).
- `cargo test --workspace`: quote crate 8/8 pass. Only failure is the known flaky
  `backend-utils-misc-timeout::periodic_timeout_reschedules` (passes under
  `--no-fail-fast`) — one of the 2 timeout flakes to ignore; unrelated to quote.c.

## Verdict: PASS

All 5 quote.c functions present and MATCH (or correctly SEAMED). Zero seam
findings, zero own-logic stubs, constants verified against headers.
