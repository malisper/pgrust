# Audit: common-percentrepl

C source: `src/common/percentrepl.c` (single TU, one public function, no statics/inline helpers).
Port: `crates/common-percentrepl/src/lib.rs`.
c2rust: `../pgrust/c2rust-runs/*/percentrepl*` (one fn, matches).

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `replace_percent_placeholders` | percentrepl.c:58-137 | lib.rs `replace_percent_placeholders` | MATCH | see below |

Library calls used by the C (`initStringInfo`, `appendStringInfoChar`, `appendStringInfoString`, `ereport`/`errcode`/`errmsg`/`errdetail`) are not functions defined in this TU; they map to `PgString` (mcx) and `PgError` (types-error) respectively.

## Per-branch comparison of `replace_percent_placeholders`

- Result buffer: C `initStringInfo` + return `result.data` (palloc'd). Port `PgString::new_in(mcx)` returned as `Ok`. The variadic `(letters, ...)` pair → `&[(char, Option<&str>)]`; `Mcx` + `PgResult<PgString>` mirror the C palloc surface (fallible appends instead of abort-on-OOM). MATCH.
- Non-`%` char: C else-branch `appendStringInfoChar(&result, *sp)`. Port `result.try_push(ch)`. Byte-iteration vs char-iteration is identical: `%` is the ASCII byte 0x25 which never appears inside a UTF-8 multibyte sequence, so the only special-cased byte is unaffected. MATCH.
- `%%`: C `sp++; appendStringInfoChar(&result, *sp)` → single `%`. Port `result.try_push('%')`. MATCH.
- `%` at end (`sp[1]=='\0'`): C `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, errmsg("invalid value for parameter \"%s\": \"%s\"", param_name, instr), errdetail("String ends unexpectedly after escape character \"%\"."))`. Port returns `Err(PgError::error(<same msg>).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE).with_detail(<same detail>))`. SQLSTATE 22023 verified vs errcodes.txt:185. The FRONTEND `pg_log_error`+`exit(1)` twin emits identical text; collapsed to the same Err. MATCH.
- Placeholder lookup: C scans `letters` in order, `va_arg` per letter, breaks at the first matching letter; appends only if `val != NULL`, else leaves `found=false`. Port `for &(letter,val) in values { if next==letter { if Some(v) {push; found=true} break } }`. First-letter-wins and None==NULL→error preserved. MATCH.
- Unknown / NULL-value placeholder: C `ereport(... errdetail("String contains unexpected placeholder \"%%%c\".", *sp))` → `%` + char. Port `with_detail(format!("String contains unexpected placeholder \"%{next}\"."))`. `%%%c` == literal `%` then the char == port output. MATCH.

## Constants
- `ERRCODE_INVALID_PARAMETER_VALUE` = SQLSTATE `22023`: verified against `errcodes.txt:185` and repo `types-error` const. MATCH.

## Seams and wiring
- Pure leaf: deps `mcx` + `types-error`, acyclic. No C file in this unit maps to any `crates/X-seams` crate (no `percentrepl-seams`). Owns no inward seams → correctly has no `init_seams()` and is not in `seams-init::init_all()`. No outward seam calls (no cycle). No finding.

## Design conformance
- Allocating fn takes `Mcx<'mcx>` and returns `PgResult` — conforms.
- `format!`/`with_detail(String)` only at `Err`-return (errmsg/errdetail) sites — sanctioned.
- No opaque stand-in type aliases, no `&[u8]` blobs, no statics/Atomic/Mutex/OnceCell, no held locks, no `unwrap`/`panic`/`todo`/`unimplemented` outside tests, no unledgered divergence markers.

## Verdict: PASS
Every function MATCH; zero seam findings; design-conformance clean. 9 unit tests pass; `cargo check -p common-percentrepl -p seams-init` clean.
