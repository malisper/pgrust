# Audit: backend-replication-syncrep-scanner

**Unit:** `backend-replication-syncrep-scanner`
**C source:** `src/backend/replication/syncrep_scanner.l` (+ generated
`build-clean/src/backend/replication/syncrep_scanner.c`)
**Branch:** `port/backend-replication-syncrep-scanner`
**Verdict: PASS**

## Scope

This unit owns the flex lexer for the `synchronous_standby_names` GUC. The
c2rust run (`c2rust-runs/backend-replication-syncrep-scanner/src/syncrep_scanner.rs`,
2670 lines) is the flex-generated DFA (`yy_accept`, `yy_ec`, `yy_base`,
`yy_nxt`, `yy_chk`, `syncrep_yylex`, `yy_get_next_buffer`, buffer machinery,
`yylex_init`/`destroy`/`scan_string`, etc.) plus the seven hand-written
functions from the `.l` file. The port faithfully reimplements the hand-written
`.l` logic and replaces the generated DFA tables with a hand-written matcher
over the exact `.l` rules — the standard, sanctioned treatment for a flex
scanner (the generated table machinery is not own logic to be transcribed).

## Function inventory (hand-written `.l` functions + the rule set)

| C function / construct | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| Lexer rules (the `%% … %%` DFA) | `.l` 89-138 | `syncrep_yylex` + helpers, lib.rs 173-381 | MATCH | see rule-by-rule below |
| `fprintf_to_ereport` | `.l` 34-38 | n/a (not reimplemented) | MATCH (vacuous) | flex `yy_fatal_error` redirect; only fires on flex internal buffer-overflow fatals which a hand-written scanner cannot reach. No observable behavior. |
| `syncrep_yyerror` | `.l` 155-170 | lib.rs 362-381 | MATCH | first-error-wins guard; `yytext[0]` set → `"%s at or near \"%s\""`, else `"%s at end of input"`; does NOT raise (collects into `parse_error_msg`). psprintf OOM → `PgResult` Err. Bison-mandated unused first arg dropped. |
| `syncrep_scanner_init` | `.l` 172-186 | lib.rs 133-141 | MATCH | constructs scanner over input; the `palloc0` extra / `yylex_init` / `yyset_extra` / `yy_scan_string` collapse into the owned-state ctor. `yylex_init`-failure `elog(ERROR)` cannot occur (no fallible init). |
| `syncrep_scanner_finish` | `.l` 188-193 | lib.rs 148-150 | MATCH | `pfree(extra)`+`yylex_destroy` → drop of owned scanner; mcx strings reclaimed on context reset. |
| `yyalloc` | `.l` 200-204 | n/a | MATCH (vacuous) | flex palloc adapter; safe-Rust scanner allocates via mcx/PgString. |
| `yyrealloc` | `.l` 206-213 | n/a | MATCH (vacuous) | as above. |
| `yyfree` | `.l` 215-220 | n/a | MATCH (vacuous) | as above. |

### Rule-by-rule (`%% … %%`)

| `.l` rule | C loc | Port | Verdict |
|---|---|---|---|
| `{space}+` ignore; `space=[ \t\n\r\f\v]` | 76,90 | `skip_space` + `is_space` (` \t\n\r 0x0c 0x0b`) | MATCH |
| `[Aa][Nn][Yy]` → ANY | 94 | `scan_identifier`, `eq_ignore_ascii_case("any")` | MATCH (longest-match: keyword only when it is the whole ident run; `anything` → NAME) |
| `[Ff][Ii][Rr][Ss][Tt]` → FIRST | 95 | `scan_identifier`, `eq_ignore_ascii_case("first")` | MATCH |
| `{xdstart}` `BEGIN(xd)`; `{xddouble}`→`"`; `{xdinside}` `[^"]+`; `{xdstop}`→NAME from xdbuf | 97-112 | `scan_quoted_identifier` | MATCH (xdbuf accumulation; NAME value from xdbuf not yytext; yytext left = lone closing quote) |
| `<xd><<EOF>>` → yyerror "unterminated quoted identifier"; return JUNK | 113-116 | `scan_quoted_identifier` EOF branch | MATCH (empty match ⇒ empty yytext ⇒ "... at end of input"; returns Junk) |
| `{identifier}` → `pstrdup(yytext)`; NAME | 118-121 | `scan_identifier` NAME arm; `ident_start=[A-Za-z\200-\377_]`, `ident_cont` +`[0-9$]` | MATCH |
| `{digit}+` → `pstrdup(yytext)`; NUM | 123-126 | `scan_number` | MATCH |
| `"*"` → `yylval->str="*"`; NAME | 128-131 | `*` arm (mcx copy of `"*"`) | MATCH |
| `","` `"("` `")"` → ASCII codes | 133-135 | Comma/LeftParen/RightParen, token_code = ASCII byte | MATCH |
| `.` → JUNK | 137 | trailing JUNK arm | MATCH (only ASCII non-class bytes reach here; `utf8_len` high-bit branch is dead on valid-UTF-8 `&str` input since high bytes are `ident`/`xdinside` and consumed earlier; for the bytes that do reach `.`, `utf8_len`==1 == flex one-byte advance) |
| `<<EOF>>` (INITIAL) → YY_NULL (0) | flex | `Eof` arm, token_code 0 | MATCH |

## Seam audit

This unit's `c_sources` is only `syncrep_scanner.l` (+ its generated `.c`).
By the C-source-coverage ownership rule it owns **no** seam crate:
`crates/backend-replication-syncrep-seams` maps to `syncrep.c`, which is the
separate unit `backend-replication-syncrep` (CATALOG row 411) and declares
`sync_rep_wait_for_lsn` / `sync_rep_cleanup_at_proc_exit` — nothing this unit
owns. The scanner makes **no outward seam calls**: it is the leaf driven *by*
the Bison grammar (the cycle-partner `backend-replication-syncrep-gram` calls
`syncrep_yylex`), so nothing crosses a seam. `init_seams()` is correctly empty
and is wired into `seams-init::init_all()` (lib.rs:130, Cargo dep at :134). No
seam findings.

## Design conformance

- Allocating fns carry `Mcx` + `PgResult` (PgString allocation, OOM → Err) —
  matches the C `palloc`/`pstrdup`/`psprintf` `ereport(ERROR)` paths.
- No invented opacity; the owned `SyncrepScanner` is a real value, not a handle.
- No shared statics / ambient globals (scanner state is fully owned).
- No registry-shaped side tables, no locks-across-`?`, no unledgered divergence.
- `syncrep_yyerror` correctly does NOT raise (collects message), per the C.

## Gates

- `cargo test -p backend-replication-syncrep-scanner` — 9 passed.
- `cargo test -p seams-init` — recurrence_guard `every_seam_installing_crate_is_wired_into_init_all`
  and `every_declared_seam_is_installed_by_its_owner` both pass.
- `cargo check --workspace` — clean (only pre-existing unrelated warnings in
  `backend-access-common-printtup`).

## Conclusion

Every hand-written function and every lexer rule has a behavior-identical
counterpart; no `MISSING`/`PARTIAL`/`DIVERGES`, no own-logic stubs, no deferred
escapes, no seam findings. **PASS.**
