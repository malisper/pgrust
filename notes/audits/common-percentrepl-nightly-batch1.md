# Audit: common-percentrepl-nightly-batch1

- **Unit:** `common-percentrepl-nightly-batch1` (`*/percentrepl.c`)
- **Crate:** `crates/common-percentrepl`
- **C source:** `../pgrust/postgres-18.3/src/common/percentrepl.c`
- **c2rust:** `../pgrust/c2rust-runs/common-percentrepl-nightly-batch1/src/percentrepl.rs`
- **Branch audited:** `port/common-percentrepl-nightly-batch1` (`c4fce248`)
- **Verdict:** PASS

## 1. Function inventory

The C TU defines exactly one function (no statics, no inline helpers).
The c2rust run renders the same single function. Complete coverage.

| # | C function | C loc | Port loc | Verdict |
|---|-----------|-------|----------|---------|
| 1 | `replace_percent_placeholders` | percentrepl.c:58-137 | lib.rs:41-106 | MATCH |

## 2. Function-by-function comparison

### `replace_percent_placeholders` — MATCH

C signature: `char *replace_percent_placeholders(const char *instr,
const char *param_name, const char *letters, ...)`. Result built in a
`StringInfo` via `initStringInfo`/`appendStringInfo*`, returns the palloc'd
`result.data`.

Port signature: `fn(mcx, instr: &str, param_name: &str,
values: &[(char, Option<&str>)]) -> PgResult<PgString<'mcx>>`.

Model mapping (all behavior-preserving):
- Variadic `(letters, ...)` (NUL-terminated letter string + one `char*` per
  letter, any of which may be `NULL`) -> `&[(char, Option<&str>)]`. The C
  lookup scans `letters` left-to-right, stops at the first matching letter,
  and treats a `NULL` value as unsupported. The slice scan reproduces that
  first-letter-wins behavior, and `None` == C `NULL`. Verified by the
  `first_matching_letter_wins` and `none_value_is_unexpected` tests.
- `StringInfo` palloc'd result -> `PgString::new_in(mcx)` charged to the
  caller mcx; every append is the fallible `try_push`/`try_push_str` (`?`),
  so an allocator refusal surfaces as `Err` instead of aborting. This is the
  AGENTS.md allocating-fn rule (Mcx + PgResult). Conforming.

Control flow, branch by branch:
- C outer loop `for (sp = instr; *sp; sp++)` -> `while let Some(ch) =
  chars.next()`. MATCH.
- `*sp != '%'` -> append char. Port: `if ch != '%' { try_push(ch)?; continue }`.
  MATCH.
- `*sp == '%'`, peek `sp[1]`:
  - `sp[1] == '%'`: C does `sp++; appendStringInfoChar(*sp)` (one `%`). Port:
    `chars.next()` yields `'%'`, `try_push('%')`. MATCH.
  - `sp[1] == '\0'` (trailing `%`): error. Port: `chars.next()` returns `None`
    -> trailing-% error. MATCH.
  - else: `sp++`, scan `letters` with `va_arg`; on letter match, if value
    non-NULL append + set `found`, then `break`; if value NULL break with
    `found` still false. Port mirrors with `for &(letter,val) in values`,
    `break` on match, `found` set only when `Some`. MATCH.
  - `if (!found)` -> unknown-placeholder error reporting `*sp` (the matched
    letter). Port reports `next`. MATCH.

Error paths (both `ereport(ERROR, ...)` sites; FRONTEND `pg_log_error` twins
emit identical text):
- SQLSTATE: C `ERRCODE_INVALID_PARAMETER_VALUE`; c2rust-computed errcode digit
  math = "22023". Port uses `ERRCODE_INVALID_PARAMETER_VALUE`, defined in
  types-error as `make_sqlstate(*b"22023")`. Constant verified, MATCH.
- Severity: ERROR -> `PgError::error(...)`. MATCH.
- Message: `"invalid value for parameter \"%s\": \"%s\""` with `param_name`,
  `instr` -> `format!("invalid value for parameter \"{param_name}\":
  \"{instr}\"")`. MATCH.
- Trailing-% detail: C format `"String ends unexpectedly after escape
  character \"%%\"."` renders single `%`; port literal uses single `%`. MATCH.
- Unknown-placeholder detail: C `"String contains unexpected placeholder
  \"%%%c\"."` with `*sp` renders `%`+char; port `"String contains unexpected
  placeholder \"%{next}\"."`. MATCH.

Edge cases:
- Empty input -> empty result (loop body never runs). `empty_input_yields_empty`.
- Repeated placeholders -> re-scanned each occurrence (port re-scans `values`
  per `%`). `repeated_placeholders`.
- Multibyte/Unicode passthrough: C iterates bytes; placeholders/`%` are ASCII
  so multibyte sequences pass through unchanged. Port iterates `char`s of a
  valid UTF-8 `&str`; identical observable result for valid UTF-8 input.
  `unicode_text_preserved`.

No own-logic stubs, no `todo!`/`unimplemented!`, no deferred/SEAMED escape —
the entire function body is implemented in this crate.

## 3. Seam and wiring audit

Ownership is by C-source coverage. The only C file is `percentrepl.c`.
No `crates/*percentrepl*-seams` crate exists; no other unit declares a
percentrepl seam. The crate is a pure acyclic leaf (deps: `mcx`,
`types-error` only) and owns **no** inward seams.

- Crate has no `init_seams()` — correct for a leaf owning no seam crates.
- `seams-init` contains no percentrepl reference — correct.
- No outward seam calls (no dependency cycle; the two direct deps compile).

`recurrence_guard` (both `every_seam_installing_crate_is_wired_into_init_all`
and `every_declared_seam_is_installed_by_its_owner`) passes.

## 4. Design conformance

- Allocating fn carries `Mcx` + returns `PgResult`. Conforming.
- `#![no_std]` + `#![forbid(unsafe_code)]`; no invented opacity, no shared
  statics, no ambient-global seams, no registry side tables, no unledgered
  divergence markers.

## 5. Gates

- `cargo test -p common-percentrepl`: 9 passed.
- `cargo test -p seams-init`: 2 passed (recurrence_guard green).
- `cargo check --workspace`: clean (only pre-existing unrelated warnings in
  backend-access-common-printtup).

## Verdict: PASS

Every function MATCH, zero seam findings, all gates green.
