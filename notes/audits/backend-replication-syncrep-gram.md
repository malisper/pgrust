# Audit: backend-replication-syncrep-gram

C source: `src/backend/replication/syncrep_gram.y`
(`build-clean/src/backend/replication/syncrep_gram.c`, c2rust:
`c2rust-runs/backend-replication-syncrep-gram/src/syncrep_gram.rs`).

Port: `crates/backend-replication-syncrep-gram/src/lib.rs`.
Owned outward-seam crate: `crates/backend-replication-syncrep-scanner-seams`
(the grammar -> scanner cycle edge).

## Function inventory

The `.y` file has exactly two function definitions: the static helper
`create_syncrep_config` and the Bison-generated `syncrep_yyparse` (the grammar
productions `result`, `standby_config`, `standby_list`, `standby_name`). Bison's
generated state-machine tables (`yytranslate`, `yypact`, `yytable`, `yycheck`,
`yydefact`, `yyr1`, `yyr2`, `yypgoto`, `yydefgoto`, `yystos`) are a build
artifact of the grammar, not hand-written C; the port reproduces the *grammar*
(the source of truth) as a recursive descent, not the generated tables.

| Function | C location | Port location | Verdict | Notes |
|---|---|---|---|---|
| `create_syncrep_config` | gram.y:85-119 | lib.rs `create_syncrep_config` | MATCH | `size = offsetof(member_names) + Σ(strlen+1)`; `palloc(size)`; header `config_size=size`, `num_sync=atoi`, `syncrep_method`, `nmembers=list_length`; pack each name + nul, advance by `strlen+1`. Flat chunk is one `Mcx`-charged `PgVec<u8>`; `palloc` OOM -> `PgError`. repr(C) header offsets asserted (test). |
| `atoi` (C library, used by create_syncrep_config) | — | lib.rs `atoi` | MATCH | Leading decimal run, trailing junk ignored, empty/no-leading-digit -> 0; overflow saturates to i32::MAX (scanner only emits digit runs). |
| `syncrep_yyparse` / `result` / `standby_config` / `standby_list` / `standby_name` | gram.y:53-83 (grammar) | lib.rs `Parser::*`, `syncrep_yyparse` | MATCH | Recursive descent over all 4 `standby_config` alternatives, comma `standby_list`, `NAME\|NUM` `standby_name`. LALR(1) lookahead on a leading NUM (bare list vs `NUM '('...')'`) resolved as Bison resolves it. `num_sync`/`syncrep_method` per rule: bare list & `NUM(...)` & FIRST -> PRIORITY, ANY -> QUORUM; implicit `num_sync="1"`. Member list = `Mcx`-charged `PgVec<PgString>` (the C `List*`). |
| token codes NAME/NUM/JUNK/ANY/FIRST | gram.y `%token` -> syncrep_gram.h | lib.rs consts | MATCH | 258/259/260/261/262 — Bison declaration-order numbering from 258; YYMAXUTOK=262 in c2rust confirms the range. Single-char tokens use ASCII byte; EOF=0. |
| SYNC_REP_PRIORITY/QUORUM | syncrep.h:35-36 | lib.rs consts | MATCH | 0 / 1. |
| `SyncRepConfigData` | syncrep.h:63-72 | lib.rs struct | MATCH | repr(C) header (config_size:i32, num_sync:i32, syncrep_method:u8, nmembers:i32, member_names FAM); offsets 0/4/8/12, size 16 asserted. |

## Seams and wiring

- **Outward (grammar -> scanner).** The grammar drives the scanner
  (`syncrep_yylex`, `syncrep_yyerror`, lifecycle) which references the grammar's
  token codes — a genuine TU cycle. These are declared in the owner's seam crate
  `backend-replication-syncrep-scanner-seams` (`syncrep_scanner_init`,
  `syncrep_yylex`, `syncrep_yyerror`, `syncrep_scanner_error_msg`,
  `syncrep_scanner_finish`) and panic until the scanner unit installs them. Each
  grammar call site is thin marshal + delegate (no logic in a seam path). The
  opaque `SyncrepScannerHandle(u64)` stands in for C's `yyscan_t` (`void *`) —
  inherited opacity, mirroring the existing `ReadStreamHandle` precedent.
  `SyncrepLexeme { token: i32, value: Option<PgString> }` mirrors the C
  `(return code, yylval->str)` pair.
- **Inward.** Nothing crosses a cycle to call *into* the grammar: in C only
  `syncrep.c` calls `syncrep_yyparse`, a same-layer caller resolvable by a direct
  dependency. The grammar therefore owns no `-seams` crate and correctly has no
  `init_seams()` (cf. `functioncmds`/`dest`). No `seams-init` wiring is required.

## Design conformance

- Allocating paths take `Mcx` and return `PgResult` exactly where the C
  `palloc`/`pstrdup` can `ereport` OOM. No infallible `Vec`/`String`/`format!` on
  a palloc path (error-message construction at the `Err` site only).
- No invented opacity beyond the inherited `yyscan_t`. No stand-in integer
  aliases, no `&[u8]` blob for typed data, no per-backend statics (the only
  statics are in `#[cfg(test)]`), no zero-arg getter seams, no locks.
- No `todo!`/`unimplemented!`; no unledgered divergence markers.

## Verdict: PASS

Every function MATCH; outward seams justified by a real cycle and thin; no inward
seams owned; design rules satisfied. Cleared to merge once the gate is green.
Behavioral coverage: 9 in-crate tests (flat-chunk layout/offsets, atoi semantics,
all four config forms, the NUM-lookahead bare-list case, and the syntax/scanner
error messages) driven by an in-test scanner installed into the seams.
