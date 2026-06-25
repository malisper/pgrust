# Audit: backend-commands-explain-state

Date: 2026-06-13
Model: Claude Fable 5
Verdict: PASS (independent re-audit)

C source: `src/backend/commands/explain_state.c` (380 lines, PostgreSQL 18.3).
Crate: `crates/backend-commands-explain-state` (+ `-seams`, + fields added to
`types-explain`). Audited independently from the C, the c2rust rendering
(`c2rust-runs/backend-commands-explain-state/src/explain_state.rs`), and the
headers.

## Function inventory

All 7 C function definitions (c2rust confirms the same 7 `extern "C" fn`s; the
two `port/pg_bitutils.h` inlines `pg_nextpower2_32`/`pg_leftmost_one_pos32` are
ported in-crate as private helpers).

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| `NewExplainState` (61) | `NewExplainState` | MATCH | `palloc0`→`ExplainState::new_in(mcx)`; `es->costs=true`; `makeStringInfo()`→the empty `PgString` `new_in` builds. |
| `ParseExplainOptionList` (77) | `ParseExplainOptionList` | MATCH | Full `strcmp` dispatch for all 12 in-core names (analyze/verbose/costs/buffers/wal/settings/generic_plan/timing/summary/memory/serialize/format); SERIALIZE arg=off/none/text/binary + no-arg→TEXT; FORMAT text/xml/json/yaml; all 4 cross-option validations (WAL/TIMING/SERIALIZE require ANALYZE → `ERRCODE_INVALID_PARAMETER_VALUE` 22023; GENERIC_PLAN+ANALYZE conflict → 22023; unrecognized value → 22023; unrecognized option → `ERRCODE_SYNTAX_ERROR` 42601); timing/buffers/summary default to `analyze` when not explicitly set; plugin hook invoked when present. |
| `GetExplainExtensionId` (221) | `GetExplainExtensionId` | MATCH | Linear name search returns existing id; else append. C's `assigned`/`allocated` + `pg_nextpower2_32` `repalloc` of a `TopMemoryContext` array → backend-lifetime `Vec` (`len()` = assigned), behaviour identical (dense, stable per backend). |
| `GetExplainExtensionState` (259) | `GetExplainExtensionState` | MATCH | `Assert(id>=0)`→`debug_assert`; `id>=allocated`→`None`; returns the stored slot (`void*` NULL → `None`). |
| `SetExplainExtensionState` (278) | `SetExplainExtensionState` | MATCH | First alloc `Max(16, pg_nextpower2_32(id+1))`, `palloc0`→`resize(_, None)` zero-fill; full-grow `pg_nextpower2_32(id+1)`, `repalloc0` zero-fill of the new region → `resize`; store slot. Growth fallible (`try_reserve`+`mcx.oom`). |
| `RegisterExtensionExplainOption` (318) | `RegisterExtensionExplainOption` | MATCH | Search by name → update handler in place; else append. Same backend-lifetime `Vec` growth model. |
| `ApplyExtensionExplainOption` (367) | `ApplyExtensionExplainOption` | MATCH | Search options by `opt->defname`; on hit invoke handler (call seam) and return true; else false. Handler looked up without holding the registry borrow (foreign handler may re-enter). |

Constants verified against `commands/explain_state.h`: `ExplainFormat`
TEXT=0/XML=1/JSON=2/YAML=3 and `ExplainSerializeOption` NONE=0/TEXT=1/BINARY=2 —
match the `types-explain` enum variant order/`#[default]`. SQLSTATEs verified
against `types-error` (`ERRCODE_INVALID_PARAMETER_VALUE`=22023,
`ERRCODE_SYNTAX_ERROR`=42601). `pg_nextpower2_32` verified against
`port/pg_bitutils.h` (already-power-of-2 fast path; `1<<(msb+1)` otherwise; the
`num>0 && num<=UINT32_MAX/2+1` precondition is a `debug_assert`).

## Seam / wiring audit

Owned seam crate: `crates/backend-commands-explain-state-seams` (the only
`X-seams` whose `X` = this unit's C file). It declares two seams:
`call_option_handler` and `call_validate_options_hook` — invocations of foreign
extension function pointers (`ExplainOptionHandler` /
`explain_validate_options_hook`) stored in the registry this unit owns.

- These are **not** installed by this crate's `init_seams()` (empty). This is
  correct, not a finding: the callees are foreign extension code (e.g.
  `pg_overexplain`), installed by the extension when it lands — the
  mirror-PG-and-panic pattern, identical in shape to `commands/user.c`'s
  `call_check_password_hook` (declared in `user-seams`, never self-installed).
  The in-core option dispatch can never reach them (no handler/hook can be
  registered without an extension), so the panic-until-installed path is
  unreachable in the ported tree. `init_seams()` being empty mirrors the
  `functioncmds` precedent (owns no inward seam an installed crate provides);
  not wired into `seams-init` because it installs nothing (recurrence guards
  both green).
- Outward calls: `defGetBoolean`/`defGetString` are **direct** calls on
  `backend-commands-define` (leaf crate, no cycle — justified, no seam needed);
  `parser_errposition` crosses `backend-parser-small1-seams` (owner `parse_node.c`
  is `todo`, so it panics on the error-message-position path — acceptable
  unported-callee panic, same as the merged `commands/cluster` does). All seam
  paths are thin marshal+delegate; no logic lives in a seam.

## Design conformance

- Opacity inherited, not introduced: `void **extension_state` → opaque
  `ExtensionStateHandle(u64)` (a genuine `void *`); `ExplainOptionHandler` /
  `explain_validate_options_hook_type` → opaque `Handle(u64)` callback tokens.
  No invented stand-in aliases (`grep` for `type _ = Oid|usize|uN|iN;` clean).
- Per-backend file statics (`ExplainExtensionNameArray`,
  `ExplainExtensionOptionArray`, `explain_validate_options_hook`) modeled as a
  `thread_local!` `RefCell<Registry>` of plain owned `String`/`Vec` — the
  `MemoryContextAlloc(TopMemoryContext,...)` backend-lifetime-global rule
  (docs/mctx-design.md decision 5), matching `prepare.c`'s thread_local hash.
  Not a shared `static`/`Mutex`/`OnceCell`.
- The caller-owned `extension_state` array (a field of the per-query
  `ExplainState` in `mcx`) grows fallibly with `try_reserve` + `mcx.oom` (the
  Mcx+PgResult allocation rule); `palloc0`/`repalloc0` zero-fill is `resize(_,
  None)`.
- `format!` appears only at `errmsg(...)` return-`Err` sites (the
  error-construction allocation exemption). `to_owned()` only feeds the
  backend-lifetime registry.
- No `todo!()`/`unimplemented!()`/own-logic `panic!`/`unreachable!`.

## Verdict: PASS

All 7 functions MATCH; seam wiring correct; design-conformance clean. 16 unit
tests pass; `cargo check --workspace` green; both seams-init recurrence guards
pass.
