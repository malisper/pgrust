# Audit: probe-adt-scalar-bool

- **Verdict:** PASS
- **Date:** 2026-06-13
- **Model:** Claude Fable 5 (Opus 4.8 [1m])
- **C source:** `src/backend/utils/adt/bool.c` (PostgreSQL 18.3)
- **Oracle:** `../pgrust/c2rust-runs/probe-adt-scalar-bool/src/bool.rs`
- **Port:** `crates/probe-adt-scalar-bool/src/lib.rs`

## Method

Enumerated every function in the c2rust run (the completeness oracle) and in
`bool.c`, then compared C vs c2rust vs port function-by-function. The c2rust run
additionally renders the inlined macro/static helpers the preprocessor pulled in
(`DatumGetBool`, `BoolGetDatum`, `UInt32GetDatum`, `DatumGetPointer`,
`PointerGetDatum`, `DatumGetCString`, `CStringGetDatum`, `DatumGetInt64`,
`UInt64GetDatum`, `hash_uint32`, `hash_uint32_extended`, `pq_writeint8`,
`pq_sendint8`, `pq_sendbyte`, `isascii`, `__istype`, `isspace`). These are
fmgr/`Datum` marshalling and pqformat/ctype shims, not part of `bool.c`'s own
function surface; their effect is folded into the idiomatic Rust signatures
(`Datum::from_u32/from_u64`, `arg as i32 as u32`, the pqformat seam crate, the
local `is_space`). They are accounted for, not missing.

## Per-function table

| C fn | C loc | Port loc | Verdict | Notes |
|------|-------|----------|---------|-------|
| parse_bool | bool.c:30 | lib.rs:79 | MATCH | delegates to parse_bool_with_len with value.len() == strlen |
| parse_bool_with_len | bool.c:36 | lib.rs:92 | MATCH | branch order, all 8 spellings, 'o'≥2-char special case, single-char '1'/'0', default→None all preserved. pg_strncasecmp prefix semantics modelled via pg_strncasecmp_eq (reads NUL past slice end on both value and literal); to_lower_ascii matches the ASCII arm of pg_strncasecmp (high-bit locale arm never reaches these literals) |
| boolin | bool.c:127 | lib.rs:158 | MATCH | leading/trailing ASCII isspace trim, then parse; bad spelling routes ERRCODE_INVALID_TEXT_REPRESENTATION via escontext (soft → returns false suppress-value + records error) or hard throw; message embeds the original untrimmed input (in_str) exactly as C |
| boolout | bool.c:158 | lib.rs:194 | MATCH | 't'/'f'; the palloc(2)+NUL is the cstring marshalling boundary, &'static str here |
| boolrecv | bool.c:175 | lib.rs:204 | MATCH | pq_getmsgbyte != 0; via pqformat (non-cyclic, direct call) |
| boolsend | bool.c:188 | lib.rs:210 | MATCH | pq_begintypsend / pq_sendbyte(1/0) / pq_endtypsend via pqformat |
| booltext | bool.c:205 | lib.rs:221 | SEAMED | "true"/"false" then cstring_to_text via backend-utils-adt-varlena-seams — real cycle (varlena unported); thin marshal+delegate, no logic in the seam path |
| booleq | bool.c:224 | lib.rs:231 | MATCH | arg1 == arg2 |
| boolne | bool.c:233 | lib.rs:236 | MATCH | arg1 != arg2 |
| boollt | bool.c:242 | lib.rs:241 | MATCH | false<true: !arg1 & arg2 equals C bool (0/1) < |
| boolgt | bool.c:251 | lib.rs:246 | MATCH | arg1 & !arg2 |
| boolle | bool.c:260 | lib.rs:251 | MATCH | arg1 <= arg2 (Rust bool ordering false<true matches C) |
| boolge | bool.c:269 | lib.rs:256 | MATCH | arg1 >= arg2 |
| hashbool | bool.c:278 | lib.rs:267 | MATCH | hash_bytes_uint32((int32)bool as u32); (int32) widening preserved via `as i32 as u32` |
| hashboolextended | bool.c:284 | lib.rs:274 | MATCH | hash_bytes_uint32_extended((int32)bool, seed as u64) |
| booland_statefunc | bool.c:300 | lib.rs:284 | MATCH | arg1 && arg2 |
| boolor_statefunc | bool.c:312 | lib.rs:290 | MATCH | arg1 || arg2 |
| makeBoolAggState | bool.c:324 | lib.rs:315 | MATCH | None agg_context == AggCheckCallContext()==0 → elog(ERROR) internal (XX000); fields zeroed |
| bool_accum | bool.c:341 | lib.rs:332 | MATCH | None state → makeBoolAggState; null value skipped; aggcount++ / aggtrue++ on true |
| bool_accum_inv | bool.c:362 | lib.rs:356 | MATCH | None state → elog(ERROR "bool_accum_inv called with NULL state"); aggcount-- / aggtrue-- on true |
| bool_alltrue | bool.c:383 | lib.rs:376 | MATCH | None or aggcount==0 → NULL; else aggtrue == aggcount |
| bool_anytrue | bool.c:398 | lib.rs:387 | MATCH | None or aggcount==0 → NULL; else aggtrue > 0 |

## Seam and wiring audit

- **Owned seam crates:** none. No `crates/X-seams` maps to `bool.c`; the only
  seam crates matching "bool" by grep are unrelated files merely mentioning the
  word. Correct.
- `init_seams()` is empty (no owned declarations to install) — allowed, since
  the unit owns no seam crate.
- `seams-init::init_all()` calls `probe_adt_scalar_bool::init_seams();`
  (seams-init/src/lib.rs:169; Cargo dep at :173). Wired.
- `recurrence_guard` module present in seams-init and the workspace builds, so
  the guard passes.
- Outward seam: `cstring_to_text` (varlena) only — justified cycle, thin
  marshal+delegate. pqformat (`pq_*`) and common-hashfn (`hash_bytes_*`) are
  non-cyclic and called directly, not seamed. No own-logic stubs.

## Design conformance

- Allocating path (`booltext`, `boolsend`) takes `Mcx` and returns `PgResult`.
- Error paths return `PgResult` (`boolin`, the agg makers) — mirror the C
  ereport/elog failure surface; no `&'static mut`, no invented opacity, no
  shared statics. `BoolAggState` is a real by-value struct, not an opaque handle.

## Gate

- `cargo check --workspace`: clean.
- `cargo test --workspace`: `probe-adt-scalar-bool` 11/11 pass. The only failure
  was `backend-utils-misc-timeout::signal_handler_fires_reached_timeouts`, a
  known time-based timeout flake (passed on isolated re-run) — one of the 2
  ignored timeout flakes, unrelated to this unit.

**PASS** — every function MATCH or SEAMED per rule 3; zero seam findings; zero
design findings.

## Update 2026-06-15 — `parse_bool` seam install (wf-bool)

`bool.c` was already fully ported and audited here, superseding the
`backend-utils-adt-bool` port request (same C file, same repo model: canonical
`Datum`, `Mcx`, plain typed fns). The one gap was wiring: the `parse_bool` seam
declared in `backend-utils-adt-scalar-seams` (nominal owner
`backend-utils-adt-scalar`, still `todo` as one combined unit) had **no
installer**, so its GUC/walsender consumer
(`backend-tcop-backend-startup`, `replication=...`) would have panicked on first
call. `bool.c` is `parse_bool`'s real home, so this crate now installs it from
`init_seams()` (added dep `backend-utils-adt-scalar-seams`; no cycle). The
sibling `datum_copy` seam in that same crate is installed by its own owner
(`backend-utils-adt-scalar-datum-core`) — no double install. Verdict unchanged:
**PASS**.
