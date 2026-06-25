# Audit: backend-utils-misc-guc-file

- **Verdict: PASS**
- Date: 2026-06-13 (re-audit after tokenizer maximal-munch fix)
- Model: claude-opus-4-8[1m]
- Branch: fix/diverge-backend-utils-misc-guc-file
- Unit C source: `src/backend/utils/misc/guc-file.l`
- Port crate: `crates/backend-utils-misc-guc-file`
- c2rust reference: `pgrust/c2rust-runs/backend-utils-misc-guc-file/src/guc_file.rs`

This audit is independent of the port: every function was re-derived from the
C source and c2rust rendering. Two blockers have now been found and fixed on
this lineage: (1) 8-bit cleanliness (an earlier `DIVERGES`, fixed on the port
branch), and (2) the flex tokenizer's token-boundary derivation (a `DIVERGES`
fixed on this branch — see §2b). The unit is re-audited clean below.

## 1. Function inventory

`guc-file.l` defines the following functions (the flex `%%` scanner rules plus
the C action code). The c2rust run additionally emitted the entire
flex-generated scanner machinery (`GUC_yylex`, `yy_get_next_buffer`,
`GUC_yy_create_buffer`, `yy_get_previous_state`, the `GUC_yy*` buffer/getter/
setter family, `yy_init_globals`, `GUC_yylex_init`/`_destroy`, etc.) — that is
generated flex runtime, not hand-written source logic, and the port replaces it
with a hand-written `Lexer`/`parse_line` reproducing the same token classes.

| # | C function | Kind | Port location | Verdict |
|---|-----------|------|---------------|---------|
| 1 | `ProcessConfigFile` | exported | `lib.rs` `ProcessConfigFile` | MATCH (core delegated via SEAM) |
| 2 | `ParseConfigFile` | exported | `lib.rs` `ParseConfigFile` | MATCH |
| 3 | `record_config_file_error` | exported | `lib.rs` `record_config_file_error` | MATCH |
| 4 | `GUC_flex_fatal` | static | (flex-fatal longjmp machinery) | N/A — see note |
| 5 | `ParseConfigFp` | exported | `lib.rs` `ParseConfigFp` | MATCH |
| 6 | `ParseConfigDirectory` | exported | `lib.rs` `ParseConfigDirectory` | MATCH |
| 7 | `FreeConfigVariables` | exported | `lib.rs` `FreeConfigVariables` | MATCH |
| 8 | `FreeConfigVariable` | static | (folded into `Vec`/`Drop`) | MATCH |
| 9 | `DeescapeQuotedString` | exported | `lib.rs` `DeescapeQuotedString` | MATCH |
| L | flex scanner (`yylex` + token rules) | generated | `lib.rs` `Lexer::next_token`/`parse_line`/`match_*` | MATCH (re-derived to maximal munch, §2b) |

Callees defined in *other* C files (not part of this unit) and reached across
seams: `ProcessConfigFileInternal` (guc.c), `AbsoluteConfigLocation`,
`GetConfFilesInDir` (conffiles.c), `guc_name_compare` (guc.c — trivially
re-implemented locally as a case-insensitive compare, which is exactly its
behavior for the ASCII directive names `include`/`include_if_exists`/
`include_dir`; this is a leaf comparison, not absent logic).

## 2. Per-function findings

### ProcessConfigFile — MATCH (core SEAMED)
- `Assert((PGC_POSTMASTER && !IsUnderPostmaster) || PGC_SIGHUP)` →
  `debug_assert!` with `is_under_postmaster` seam. Match.
- `elevel = IsUnderPostmaster ? DEBUG2 : LOG` → identical. Match.
- The private `config file processing` memory context (create/switch/delete) is
  the leak-isolation wrapper; the owned `Vec` the parse path returns is dropped
  on unwind, so the wrapper reduces to elevel selection + the internal call.
  `ProcessConfigFileInternal` is owned by guc.c and routed via
  `backend_utils_misc_guc_seams::process_config_file_internal` — a real cross-
  unit dependency (guc.c is unported). Legitimate SEAM (thin delegate, no logic).

### ParseConfigFile — MATCH
- All-blank/empty name rejection: C `strspn(name," \t\r\n")==strlen(name)` →
  `bytes().all(|b| b' '|b'\t'|b'\r'|b'\n')`. Empty string: C `strspn==0==strlen`
  true; Rust `all` over empty iterator is `true`. Match. ERRCODE
  `INVALID_PARAMETER_VALUE`. Match.
- Depth check `depth > CONF_FILE_MAX_DEPTH` (10), ERRCODE
  `PROGRAM_LIMIT_EXCEEDED`. Constant verified against `conffiles.h`
  (`CONF_FILE_MAX_DEPTH 10`, `CONF_FILE_START_DEPTH 0`). Match.
- `AbsoluteConfigLocation` via conffiles seam. Direct-recursion check
  `calling_file && strcmp(abs_path, calling_file)==0` →
  `calling_file.is_some_and(|cf| abs_path == cf)`. ERRCODE
  `INVALID_PARAMETER_VALUE`. Match.
- Open failure: strict → `errcode_for_file_access` + `%m` recorded; non-strict →
  `ereport(LOG, "skipping missing configuration file ...")` and return true.
  Match. (`%m` left literal in the message, expanded by the error builder
  against the saved errno — the repo convention.)
- The record-vs-throw split (record below ERROR, propagate `Err` at/above) is
  factored into `record_or_throw`, mirroring the C longjmp boundary. Match.

### ParseConfigFp — MATCH
- Per-logical-line loop. Strings cannot span physical newlines (the `STRING`
  regex `\'([^'\\\n]|\\.|\'\')*\'` excludes raw `\n`), so splitting on `\n`
  (with trailing `\r` stripped, since flex eats `\r` as whitespace) reproduces
  the `ConfigFileLineno`-on-EOL token boundaries exactly.
- Line numbering: C uses `ConfigFileLineno - 1` for the settled line (EOL bumped
  the counter to N+1) and reports near-token errors at `ConfigFileLineno` (= N,
  EOL not yet consumed); near-end-of-line errors at `ConfigFileLineno - 1` (= N).
  Both resolve to physical line N, which the port uses directly. Verified across
  name-only lines, extra-token lines, and bad-first-token lines. Match.
- include / include_if_exists / include (strict) directives recurse with
  `depth+1`; non-zero failures set `OK=false`. `guc_name_compare` case-
  insensitive. Match.
- Ordinary variable appended with name/value/filename/sourceline, `ignore=false`,
  `applied=false`. Match.
- Syntax error reporting: ERRCODE `SYNTAX_ERROR`, near-end-of-line vs near-token
  message forms. Match. (Cosmetic note below.)
- Abandonment: `errorcount >= 100 || elevel <= DEBUG1` → ERRCODE
  `PROGRAM_LIMIT_EXCEEDED` "too many syntax errors", break. The port guards with
  `errorcount > 0` so the check runs only on error iterations, matching the C
  placement inside `parse_error:`. Because the C breaks in the same iteration
  the count reaches 100 (or on the first error at DEBUG1), a later successful
  line can never re-trigger it. Match.

### ParseConfigDirectory — MATCH
- `GetConfFilesInDir` via conffiles seam; `!filenames` (NULL) → record `err_msg`
  and return false. Loop strict-parses each file in sorted order; first failure
  returns false. Match.

### record_config_file_error — MATCH
- Builds an error `ConfigVariable` (name/value NULL, errmsg set, filename
  optional, `ignore=true`, `applied=false`) and appends. The C head/tail linked-
  list append becomes a `Vec::push`. Match.

### FreeConfigVariables / FreeConfigVariable — MATCH
- C frees each node's name/value/errmsg/filename then the node. The owning `Vec`
  drops its `String`/`PathBuf` fields; `FreeConfigVariables` is `Vec::clear`.
  Behaviorally identical (no aliasing, owned values). Match.

### DeescapeQuotedString — MATCH
- Leading/trailing quote asserts (`debug_assert`, len>=2). Escape switch:
  `b f n r t` → `\b \f \n \r \t`; octal `0-7` accumulates up to 3 digits with
  `i += k-1` (k>=1 since the first byte matched), truncated to 8 bits
  (C casts `long` octVal to `char`; Rust `u8` wrapping) — verified `\777`→0xFF
  in both. Default → the escaped byte verbatim. `''` → single `'`. Trailing-
  quote handling differs in mechanism (C copies then overwrites with `\0`; Rust
  bounds the loop at `i+1 < len` to never emit the closing quote) but is
  behaviorally identical, verified on `'\''`. Match.

### Lexer (next_token) / parse_line / match_* — MATCH (re-derived; see §2b)
- Token classes reproduced: ID, QUALIFIED_ID, STRING, UNQUOTED_STRING, INTEGER
  (incl. `0x` hex and trailing UNIT_LETTERs), REAL (incl. EXPONENT), EQUALS,
  GUC_ERROR catch-all, GUC_EOL (empty/comment line → `None`).
- GUC token enum values verified against guc-file.l's `enum { GUC_ID=1,
  GUC_STRING=2, GUC_INTEGER=3, GUC_REAL=4, GUC_EQUALS=5, GUC_UNQUOTED_STRING=6,
  GUC_QUALIFIED_ID=7, GUC_EOL=99, GUC_ERROR=100 }` and the c2rust constants
  (`guc_file.rs:786-791`). The Rust port uses a `TokenKind` discriminant (not
  the numeric value) plus `None`-for-EOL; the *classification* into these
  classes is what matters and is verified arm-for-arm. Match.
- `LETTER = [A-Za-z_\200-\377]` — `is_letter` accepts `_` and bytes `>= 0x80`;
  `LETTER_OR_DIGIT` adds `[0-9]`. UNIT_LETTER `[a-zA-Z]` = `is_ascii_alphabetic`.
  HEXDIGIT = `is_ascii_hexdigit`. Verified against the `%%`-block definitions.
  Match.
- **Tokenizer now implements flex maximal munch directly** (one `match_*` fn per
  `%%` rule returning the longest match length at the current position; the
  scanner picks the greatest length, ties broken by guc-file.l rule order: ID,
  QUALIFIED_ID, STRING, UNQUOTED_STRING, INTEGER, REAL, EQUALS, then the catch-
  all `.` consuming exactly one byte → GUC_ERROR). This reproduces flex's token
  *boundaries* exactly, including: the single-byte GUC_ERROR for any unmatched
  byte; the `''`-empty-string-vs-doubled-quote disambiguation (STRING returns
  the longest prefix that reaches a closing quote); the bare-exponent `1.5e`
  split (REAL `1.5` + ID `e`); `0x` with no hex digits rejected; QUALIFIED_ID
  matching exactly one dot (a three-part dotted run is the longer UNQUOTED_STRING
  by rule length, so flex and the port both pick UNQUOTED_STRING). Each arm was
  re-derived from the regex char classes; new regression tests pin the
  previously-divergent boundaries (§2b).
- parse_line grammar `NAME [=] VALUE EOL`: first token must be ID/QUALIFIED_ID;
  value must be ID/STRING/INTEGER/REAL/UNQUOTED_STRING (QUALIFIED_ID rejected as
  a value — matches C, verified by test `qualified_id_is_allowed_for_name_but_not_value`);
  STRING values run through `DeescapeQuotedString`; a trailing extra token is a
  near-token error. Match.

## 2a. Blocker found and fixed (was DIVERGES, now MATCH)

**8-bit cleanliness — `ParseConfigFile` file read.** The original port read the
file with `std::fs::read_to_string`, which requires valid UTF-8. The flex
scanner is `%option 8bit` and the `LETTER` class explicitly includes
`\200-\377`, so a config file containing non-UTF-8 bytes (e.g. a Latin-1 value)
parses fine in C but was rejected as unreadable by the port (the InvalidData
error has no `raw_os_error`, producing a spurious "could not open configuration
file" with an empty errno). This is a behavioral divergence on valid C input.

Fix (this branch): read the file as raw bytes (`std::fs::read`); `ParseConfigFp`,
`logical_lines`, `Lexer`, and the `classify_token`/`is_*` predicates now operate
on `&[u8]`. Token text is lossily UTF-8-converted only when stored in the
`String`-typed `ConfigVariable` (the same lossy boundary the port already used
for the single-byte GUC_ERROR token and for `DeescapeQuotedString`'s result, and
an inherent property of the `String`-typed name/value carried across the GUC
seam — not introduced here). Regression test `parses_non_utf8_bytes` added.

## 2b. Blocker found and fixed (tokenizer was DIVERGES, now MATCH)

**Token boundaries — `Lexer::next_token` / `classify_token`.** The earlier port
tokenized by taking the longest run of bytes up to a fixed delimiter set
(`[ \t\r\n#=]`) and then classifying that whole run. flex does not work that
way: it performs *maximal munch over the per-rule regex char classes* and breaks
length ties by `%%` rule order. The delimiter-run approach diverges on any input
where a token ends at a byte that is in neither the delimiter set nor the rule's
char class — flex ends the token there and tokenizes the remainder separately,
while the old port lumped it into one (mis-classified) run. Concrete divergences
(all observable in the emitted `near token "..."` diagnostic and the recorded
`ConfigVariable.errmsg`, which are part of the parser's contract):

- `name = a,b` — flex: value UNQUOTED_STRING `a`, then `,` → GUC_ERROR (one
  byte); diagnostic `near token ","`. Old port: whole run `a,b` → GUC_ERROR;
  diagnostic `near token "a,b"`.
- `'''` — flex: STRING `''` (the longest match that reaches a closing quote, an
  empty string) then a stray `'` → GUC_ERROR. Old port: greedily treated `''`
  as an embedded doubled quote, ran off the line, and returned a single
  GUC_ERROR for the whole `'''`.
- `1.5e` — flex: REAL `1.5` then ID `e` (the `{EXPONENT}?` is consumed only when
  `[Ee]{SIGN}?{DIGIT}+` fully matches). Old port: one run `1.5e` → GUC_ERROR.
- `1e5` — flex: INTEGER `1e` (digit + UNIT_LETTER) then INTEGER `5`. Old port:
  one run `1e5` → GUC_ERROR.

In every case the divergence was the token boundary and therefore the GUC_ERROR
granularity and the `near token` text (the single-byte catch-all vs a merged
run); the accept/reject outcome and SQLSTATE/severity happened to coincide on
most inputs, but the emitted message is itself part of the observable behavior
and diverged.

Fix (this branch): `next_token` now computes the longest match for every flex
rule via one `match_*` helper per rule and selects greatest-length / first-rule,
with the catch-all consuming exactly one byte; the unterminated-`'` and bare-
unmatched-byte paths both fall through to that catch-all (matching flex's `.`).
`scan_quoted`/`classify_token`/the `is_*` predicates were replaced by
`match_string`/`match_id`/`match_qualified_id`/`match_unquoted_string`/
`match_integer`/`match_real`/`match_equals`/`match_sign`, each a faithful
transcription of its `%%` regex. Eight regression tests pin the boundaries
above (`tokenizer_*`). `cargo test -p backend-utils-misc-guc-file` green
(17 tests).

## 3. Seam audit

Owned seam crate (by C-source coverage of `guc-file.l`): the only guc-file.l
function exposed as an inward seam is `ProcessConfigFile`.

- `crates/backend-utils-misc-guc-file-seams` `process_config_file` — installed by
  this crate's `init_seams()` (`process_config_file::set(ProcessConfigFile)`).
  `seams-init::init_all` calls `backend_utils_misc_guc_file::init_seams()`
  (verified `seams-init/src/lib.rs:71`). Signature `fn(GucContext) ->
  PgResult<()>` mirrors the C `ereport(ERROR)`/OOM failure surface. OK.
- Outward callee seams used, all justified by real unported dependencies and
  thin (convert + one call + convert):
  - `backend_utils_misc_guc_seams::process_config_file_internal` (guc.c) —
    declared, uninstalled (guc.c unported → panics on call). Correct: the logic
    lives in guc.c, not here; this is a callee, not a relocated body.
  - `backend_utils_misc_conffiles_seams::{absolute_config_location,
    get_conf_files_in_dir}` (conffiles.c) — leaf path helpers.
  - `backend_utils_init_small_seams::is_under_postmaster` — backend-local flag.
- No branching/node-construction/computation occurs inside any seam call path in
  this crate.

**Note (pre-existing, not this unit's debt, not a blocker):** the
`backend-utils-misc-guc-file-seams` crate also declares six *guc.c* inward seams
(`new_guc_nest_level`, `at_eoxact_guc`, `guc_check_errdetail`, `at_start_guc`,
`log_xact_sample_rate`, `set_config_with_handle`). These are guc.c functions
(not guc-file.l), are not installed by this crate (correctly — their logic is
not here), and predate this port (last touched by earlier commits, not commit
355de4ee). They are mis-homed into this seam crate and should migrate to
`backend-utils-misc-guc-seams` when guc.c is ported; they are not owned by this
unit's C coverage, so they are not installable here and are out of scope for this
audit's PASS/FAIL. Flagged for the eventual guc.c port.

## 3b. Design conformance

- Allocating/fallible functions return `PgResult` and take no manual `Mcx`
  (the parser builds owned Rust collections; the C `config file processing`
  MemoryContext is the leak-isolation arena, reproduced by owned `Vec`/`Drop`).
  No `&'static mut`, no invented opacity, no shared statics for per-backend
  globals (the C `static ConfigFileLineno`/`GUC_flex_fatal_jmp` are per-call
  locals here — correct, they were per-recursion save/restore in C). OK.
- The record-vs-throw boundary returns `Err(PgError)` rather than holding any
  lock; no locks across `?`. No registry side tables. No unledgered divergence
  markers. OK.
- Neighbor-dependency decisions (AGENTS.md): unported guc.c/conffiles.c callees
  are seam-and-panic, not restructured-around or stubbed — conforms to
  "Mirror PG and panic". OK.

## 4. Verdict

**PASS.** Every function MATCH or properly SEAMED; both DIVERGES found on this
lineage (8-bit file read, fixed on the port branch; tokenizer maximal-munch
boundaries, fixed on this branch and re-derived in §2b) are resolved and
re-audited. Zero seam findings within the unit's ownership.
`cargo test -p backend-utils-misc-guc-file` green (17 tests, incl. the new
`tokenizer_*` boundary regressions and the non-UTF-8 regression).
