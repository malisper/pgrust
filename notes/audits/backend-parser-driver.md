# Audit: backend-parser-driver (`src/backend/parser/parser.c`)

Independent function-by-function audit, re-derived from the C source
(`postgres-18.3/src/backend/parser/parser.c`), the c2rust rendering
(`c2rust-runs/backend-parser-driver`), and the headers. PostgreSQL 18.3.

## Function inventory

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `raw_parser` (42) | `lib.rs::raw_parser` | SEAMED | Body is `scanner_init` + `parser_init` + `base_yyparse` + `scanner_finish`, all owned by scan.l/gram.y (unported). The grammar reentrantly drives `base_yylex` (this crate), so the driver reaches the full drive across that cycle via `backend-parser-gram-seams::base_yyparse`. The mode-token seed (`mode_token`/`mode_seed`) is ported in-crate as parser.c's own `mode_token[]` array logic. |
| `base_yylex` (110) | `lib.rs::BaseLexer::base_yylex` + `finish_uident_usconst` | MATCH | Lookahead model: stateless `core_yylex` seam replaces flex's in-place buffer mutation + `lookahead_end`/`lookahead_hold_char` un-truncation; the merged-token output and error locations are identical. All five multiword merges (FORMAT→FORMAT_LA on JSON; NOT→NOT_LA on BETWEEN/IN_P/LIKE/ILIKE/SIMILAR; NULLS_P→NULLS_LA on FIRST_P/LAST_P; WITH→WITH_LA on TIME/ORDINALITY; WITHOUT→WITHOUT_LA on TIME) and the UIDENT/USCONST UESCAPE branch match C branch-for-branch. The C `cur_token_length` hardwiring + `\0` un-truncation are scanner-buffer bookkeeping the stateless seam makes unnecessary; behavior preserved. |
| `hexval` (327) | `udeescape.rs::hexval` | MATCH | Digit/a-f/A-F arms identical; the `elog(ERROR,"invalid hexadecimal digit")` fallthrough is unreachable given the `isxdigit` guards at every call site (same as C's `/* not reached */`). |
| `check_unicode_value` (341) | `udeescape.rs::check_unicode_value` | MATCH | `!is_valid_unicode_codepoint` → `ERRCODE_SYNTAX_ERROR` "invalid Unicode escape value". |
| `check_uescapechar` (351) | `udeescape.rs::check_uescapechar` | MATCH | `isxdigit \| '+' \| '\'' \| '"' \| scanner_isspace` → reject. `scanner_isspace` reimplemented in place (flex `{space}` = `' ' \t \n \r \f`) per the repo precedent (arrayfuncs/varlena/misc2). |
| `str_udeescape` (371) | `udeescape.rs::str_udeescape` | MATCH | Doubled-escape, 4-hex `\XXXX`, 6-hex `\+XXXXXX`, ordinary-char, surrogate-pair joining, and all `invalid_pair`/`invalid Unicode escape`(+hint) error arms match. C reads `in[1..7]` one past a possible NUL — mirrored by `byte()` returning 0 past end. Output is `PgVec<u8>` in `mcx` (C pallocs + repallocs; allocator-api2 Vec growth is the same amortized policy). Error cursors carry the C byte offset `in - str + position + 3`; `base_yylex` runs them through `scanner_errposition` (C's active errposition callback). |
| `scanner_errposition` (scan.l:1139) | `lib.rs::scanner_errposition` | MATCH | `location < 0` → 0; else `pg_mbstrlen_with_len(scanbuf, location) + 1`. Mb seam (owner mbutils.c). |

No C function omitted. No `todo!`/`unimplemented!`/own-logic stub.

## Seam audit

This unit's only `c_source` is `parser.c`. It owns **no** `*-seams` crate
(there is no `backend-parser-driver-seams`): `base_yylex`/`str_udeescape`/
`raw_parser` are called by the unported scanner/grammar owners across direct
deps once those land, not across a cycle into this crate. Its `init_seams()` is
therefore empty (correct, mirrors `backend-commands-functioncmds`) and it is NOT
wired into `init_all()` — confirmed by both seams-init guard tests (pass).

Outward seams (declared in the **owners'** seam crates, installed by those
owners when they land — panic-until-landed = mirror-PG-and-panic, never silent):

- `core_yylex` → new `backend-parser-scan-seams` (owner `backend-parser-scan`,
  unported). Stateless `CoreToken` model.
- `base_yyparse` drive → new `backend-parser-gram-seams` (owner
  `backend-parser-gram`, unported).
- `truncate_identifier` → new `backend-parser-scansup-seams` (owner
  `backend-parser-scansup`, unported).
- `pg_unicode_to_server`, `pg_mbstrlen_with_len` → existing
  `backend-utils-mb-mbutils-seams`.

The inverse guard (`every_declared_seam_is_installed_by_its_owner`) does not
flag the three new seam crates because their owner dirs are absent under
`crates/` (genuinely unported) — condition (a) skips them. PASS.

## Design conformance

- Allocation: `str_udeescape`/`truncate_identifier`/`core_yylex` seams carry
  `Mcx` + `PgResult` + `PgVec` exactly where C pallocs/ereports. No infallible
  `String`/`Vec`/`format!` on a palloc path.
- Opacity: no invented handle/alias for a typed C value. `RawParseMode` is the
  real enum (added to `types-parsenodes`, values verified vs parser.h);
  `CoreToken` is a real value type, not an opaque token.
- No statics/registries; the lexer state lives in the `BaseLexer` value.
- Constants verified against headers, not memory: all 30 grammar token codes vs
  `gram_tokens.txt`; `MAX_UNICODE_EQUIVALENT_STRING=16` vs pg_wchar.h:345;
  `NAMEDATALEN=64`; `RawParseMode` discriminants vs parser.h.
- One `.expect()` in `finish_uident_usconst` guards a structural invariant
  (lookahead is unconditionally set on the line that dispatches into it; C reads
  it unconditionally too), not an error path standing in for an ereport.

## Verdict: PASS

Every C function MATCH or SEAMED per the rules above. Zero seam findings, zero
design findings. `cargo check --workspace` clean; `cargo test -p
backend-parser-driver` 16/16 pass; `cargo test -p seams-init` 2/2 pass.
