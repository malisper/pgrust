# Audit: backend-bootstrap-bootscanner

C source: `src/backend/bootstrap/bootscanner.l` (flex lexer for the BKI
bootstrap parser). Port: `crates/backend-bootstrap-bootscanner/src/lib.rs`.

Independent re-derivation from the `.l` source + the Bison `%token` order in
`bootparse.y`. Audit done against the C, not the port's comments.

## Function inventory

| C construct (bootscanner.l) | Port location | Verdict | Notes |
|---|---|---|---|
| `fprintf_to_ereport` (static, line 36) | — | MATCH (vacuous) | flex-runtime-only: redirects flex's `yy_fatal_error` `fprintf` into `ereport(ERROR)`. The owned-value rewrite has no flex buffer machinery, so this internal-error hook has no reachable counterpart. The real lexer error surface (`. { elog(ERROR ...) }`) is ported as `unexpected_character`. |
| flex rule block / `boot_yylex` (lines 74-127) | `BootScanner::next_token` + `boot_yylex` | MATCH | rule-by-rule below |
| `boot_yyerror` (line 131) | `boot_yyerror` | MATCH | `elog(ERROR, "%s at line %d", message, yylineno)` → `Err(PgError::new(ERROR, format!("{} at line {}", message, line)))` |
| `yyalloc` (line 145) | — | MATCH (vacuous) | flex allocator shim (`palloc`); no flex runtime in the owned rewrite |
| `yyrealloc` (line 151) | — | MATCH (vacuous) | flex allocator shim (`repalloc`/`palloc`) |
| `yyfree` (line 160) | — | MATCH (vacuous) | flex allocator shim (`pfree`) |

### `next_token` rule-by-rule vs the flex rules

| flex rule | port branch | Verdict |
|---|---|---|
| `open`/`close`/`create`/`OID`/`bootstrap`/`shared_relation`/`rowtype_oid`/`insert`/`declare`/`build`/`indices`/`unique`/`index`/`on`/`using`/`toast`/`FORCE`/`NOT`/`NULL` → kw + return TOKEN | `is_id_byte` longest-match run → `keyword_token` exact lookup → `Keyword(kw)` + kind | MATCH — whole-token, case-sensitive keyword match. Longest-match (flex) reproduced by reading the full `{id}` run then matching the exact string; a keyword only wins when the entire token equals it (e.g. `opened` stays `Id`). |
| `_null_` → return NULLVAL (no kw) | `None if yytext == "_null_"` → `NullVal`, value `None` | MATCH — value-less, exactly as C sets no `yylval`. |
| `","`/`"="`/`"("`/`")"` | `single_char_token` per byte | MATCH |
| `[\n]` → `yylineno++` | `b'\n'` → position+1, line+1, at_bol=true | MATCH (line counter parity) |
| `[\r\t ]` → ignore | `b'\r'|b'\t'|b' '` → skip, at_bol=false | MATCH |
| `^\#[^\n]*` → drop comment | `b'#' if self.at_bol` → `skip_comment` | MATCH — `^` BOL anchor modeled by `at_bol`, set true at start and after `\n`, false after any other consumed char. A `#` not at BOL falls through to `.` → error (verified by test `comments_only_start_at_beginning_of_line`). |
| `{id}` `[-A-Za-z0-9_]+` → `yylval->str = pstrdup(yytext)`; return ID | non-keyword, non-`_null_` `{id}` → `Id` + `String(yytext)` | MATCH — id charset `[-A-Za-z0-9_]` = `is_id_byte`. |
| `{sid}` `\'([^']|\'\')*\'` → `DeescapeQuotedString(yytext)`; return ID | `b'\''` → `quoted_end` (doubled-quote aware) → `Id` + `String(DeescapeQuotedString(yytext))` | MATCH — `quoted_end` treats `''` as an embedded quote and only ends on a lone `'`, matching the `([^']|\'\')*` pattern; de-escape delegated to the shared ported routine. Unterminated quote → no end → `unexpected_character("'")` (flex would fail the `{sid}` rule and fall to `.`). |
| `.` → `elog(ERROR, "syntax error at line %d: unexpected character \"%s\"", yylineno, yytext)` | `_` → `unexpected_character(ch)` | MATCH — same message string and line number. |
| `<<EOF>>` (implicit) | position ≥ len → `Eof`, code 0 | MATCH |

## Constants

Bison numbers named tokens from 258 in `%token` declaration order
(`bootparse.y` lines 102-110): ID=258, COMMA=259, EQUALS=260, LPAREN=261,
RPAREN=262, NULLVAL=263, OPEN=264, XCLOSE=265, XCREATE=266, INSERT_TUPLE=267,
XDECLARE=268, INDEX=269, ON=270, USING=271, XBUILD=272, INDICES=273, UNIQUE=274,
XTOAST=275, OBJ_ID=276, XBOOTSTRAP=277, XSHARED_RELATION=278, XROWTYPE_OID=279,
XFORCE=280, XNOT=281, XNULL=282. EOF=0. All match the port's `pub const`s and
`BootTokenKind::token_code`. MATCH.

## Seams and wiring

- **Owned seam crates:** none. `bootscanner.l` is consumed directly by
  `bootparse` (`bootparse::boot_yyparse(&mut scanner)`), with no dependency
  cycle. No crate calls *into* the scanner across a cycle, so there is no
  `backend-bootstrap-bootscanner-seams` crate and no `init_seams()` — correct
  for a leaf consumer. (The `boot_yylex_init` seam in
  `backend-bootstrap-bootparse-seams` is bootparse's, not this unit's.)
- **Outward calls:** one — `DeescapeQuotedString` in
  `backend-utils-misc-guc-file`, a direct cargo dependency (no cycle), called
  directly. Matches the C, which shares the same routine. Verified de-escape
  parity: `\n`→newline, `\141`→`a`, `''`→`'` (test passes).

## Design conformance

- No stand-in type aliases, no `todo!`/`unimplemented!`, no shared statics
  (the `&'static str` are the C constant keyword string literals, not globals),
  no unledgered divergence markers.
- Error sites return `PgResult`/`PgError(ERROR)` exactly where the C
  `elog/ereport(ERROR)`s — no owned-logic panic standing in for an error path.
- Allocations (`format!`, `.to_owned()`, `.clone()`) sit at error-message
  construction and `yytext`/`pstrdup` capture, matching the C's palloc'd
  counterparts.

## Verdict: PASS

Every C function MATCH (or vacuous MATCH for the three flex-runtime-only shims
and the flex fatal-error hook, which have no counterpart in an owned-value
scanner). All token constants verified against `bootparse.y` token order. No
seam findings, no design-conformance findings. 7/7 crate tests pass; `cargo
check --workspace` clean.
