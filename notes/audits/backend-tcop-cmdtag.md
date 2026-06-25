# Audit: backend-tcop-cmdtag

- **Verdict: PASS**
- **Date:** 2026-06-13
- **Model:** Claude Opus 4.8 (1M context)
- **Branch:** port/backend-tcop-cmdtag
- **Unit C sources:** `src/backend/tcop/cmdtag.c` (CATALOG `c_sources = */cmdtag.c`)
- **Crate:** `crates/backend-tcop-cmdtag` (`src/lib.rs`)
- **References:** C `postgres-18.3/src/backend/tcop/cmdtag.c`, header
  `src/include/tcop/cmdtag.h`, data `src/include/tcop/cmdtaglist.h`; c2rust
  `c2rust-runs/backend-tcop-cmdtag-dest/src/cmdtag.rs`.

## 1. Function inventory

Enumerated from cmdtag.c (every definition) and cross-checked against the c2rust
rendering. cmdtag.c defines exactly one `static` table object and 8 functions; no
static/inline helpers beyond the macro-generated table. The c2rust `cmdtag.rs`
contains the same 8 `#[no_mangle]` functions plus `run_static_initializers`
(the c2rust lowering of the `static const` table initializer — data, not a C
function). No `#if`-gated functions exist outside the build config (file has no
conditional compilation).

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | `tag_behavior[]` (static const table, `PG_CMDTAG` macro over cmdtaglist.h) | cmdtag.c:33-35 | `TAG_BEHAVIOR` lib.rs:66-260 | MATCH | 193 rows; diffed byte-for-byte against cmdtaglist.h (name, evt, rw, rowcnt) — identical. `namelen` field dropped, replaced by `name.len()` (all names pure ASCII < 256 bytes, so byte-identical to C `strlen`; verified by `names_are_ascii` test). |
| 2 | `InitializeQueryCompletion` | cmdtag.c:39-44 | `initialize_query_completion` lib.rs:279-282 | MATCH | sets `commandTag = CMDTAG_UNKNOWN` (=0), `nprocessed = 0`. `&mut` for the out pointer. |
| 3 | `GetCommandTagName` | cmdtag.c:46-50 | `get_command_tag_name` lib.rs:285-287 | MATCH | `tag_behavior[tag].name`. |
| 4 | `GetCommandTagNameAndLen` | cmdtag.c:52-57 | `get_command_tag_name_and_len` lib.rs:292-295 | MATCH | returns `(name, name.len())` instead of writing `*len`; `name.len()` == C `namelen` (ASCII). |
| 5 | `command_tag_display_rowcount` | cmdtag.c:59-63 | `command_tag_display_rowcount` lib.rs:298-300 | MATCH | |
| 6 | `command_tag_event_trigger_ok` | cmdtag.c:65-69 | `command_tag_event_trigger_ok` lib.rs:303-305 | MATCH | |
| 7 | `command_tag_table_rewrite_ok` | cmdtag.c:71-75 | `command_tag_table_rewrite_ok` lib.rs:308-310 | MATCH | |
| 8 | `GetCommandTagEnum` | cmdtag.c:82-107 | `get_command_tag_enum` lib.rs:320-349 | MATCH | binary search; see detail below. |
| 9 | `BuildQueryCompletionString` | cmdtag.c:120-163 | `build_query_completion_string` lib.rs:360-398 | MATCH | idiomatic Mcx+PgResult buffer; see detail below. |

Indexing helper `row()` (lib.rs:269-275) has no C counterpart — it is the
bounds-checked form of the C raw `tag_behavior[commandTag]` index. C indexes with
the raw enum (UB on an out-of-range value); the port panics on out-of-range
(`CommandTag` is always a valid enumerator in PG, so neither path fires in
practice). Behaviorally equivalent on all valid inputs.

### Detail: GetCommandTagEnum (bsearch)
C uses pointers `base`, `last`, `position`; port uses `isize` indices.
- `base = 0`, `last = len-1` ≡ `tag_behavior` / `tag_behavior + lengthof - 1`.
- loop guard `last >= base` ≡ C `while (last >= base)`.
- `position = base + ((last - base) >> 1)` is the exact transcription of C
  `base + ((last - base) >> 1)` (`last >= base` keeps the shift operand
  non-negative, identical to C's signed `>>`).
- `result == 0` → return `position`; `< 0` → `last = position - 1`; else
  `base = position + 1` — all match.
- NULL/empty guard: C `commandname == NULL || *commandname == '\0'` → port treats
  the slice as NUL-terminated (truncates at first NUL) and returns
  `CMDTAG_UNKNOWN` when empty. `pg_strcasecmp` (port-pgstrcasecmp) reads `0` past
  the end of either slice (`byte_at`), so comparing the (NUL-stripped) input
  against the raw `&str` name is byte-identical to C's NUL-terminated compare.
  Verified: `enum_roundtrips_every_tag` (all 193, plus lowercased),
  `enum_null_and_empty_are_unknown`, `enum_stops_at_embedded_nul`,
  `enum_unrecognized_is_unknown`.

### Detail: BuildQueryCompletionString
- copies `tagname` (memcpy → `PgString::from_str_in`), then if
  `display_rowcount && !nameonly`: for `CMDTAG_INSERT` (=158) appends `" 0"`,
  then `" "`, then the decimal of `nprocessed` via `pg_ulltoa_n`. Control flow,
  the INSERT special case, and ordering match cmdtag.c:146-155 exactly.
- `pg_ulltoa_n` (backend-utils-adt-numutils) writes digits at buffer start and
  returns the count; the port slices `digits[..n]` and appends — same bytes the C
  writes at `bufp` and advances past. Verified: `build_select_includes_rowcount`
  ("SELECT 5"), `build_insert_includes_oid_zero_and_rowcount` ("INSERT 0 7"),
  `build_update_delete_merge_rowcount`, `build_nameonly_omits_rowcount`,
  `build_non_rowcount_tag_is_name_only`, `build_large_rowcount` (u64::MAX).
- Buffer-bound assert `taglen <= COMPLETION_TAG_BUFSIZE - MAXINT8LEN - 4`
  (= 64-20-4 = 40) preserved as `debug_assert!`. `MAXINT8LEN = 20` matches
  builtins.h; `COMPLETION_TAG_BUFSIZE = 64` matches cmdtag.h (verified in
  types-portal).
- Idiomatic divergence (ledgered): the C signature writes into a caller buffer
  and returns `Size` (strlen); the port returns an mcx-charged `PgResult<PgString>`
  (allocating-function rule §3b: `Mcx` + `PgResult`). Content is byte-identical;
  the strlen is recoverable as `.len()`. No logic loss.

## 2. Constants verified (against headers, not memory)
- `CMDTAG_UNKNOWN = 0`, `CMDTAG_INSERT = 158` — match cmdtaglist.h list positions
  and the c2rust enum (`CMDTAG_INSERT: CommandTag = 158`, `CMDTAG_UNKNOWN = 0`).
- `COMPLETION_TAG_BUFSIZE = 64` (cmdtag.h:17). `MAXINT8LEN = 20` (builtins.h).
- Table length 193 matches c2rust `[CommandTagBehavior; 193]` and
  `grep -c '^PG_CMDTAG' cmdtaglist.h`.
- Full 193-row table diffed byte-for-byte (name | event_trigger_ok |
  table_rewrite_ok | display_rowcount) between cmdtaglist.h and `TAG_BEHAVIOR`:
  **identical**, no transcription drift.

## 3. Seam audit
**Owned seam crates:** the unit's only C file is `cmdtag.c`; the only possible
owned seam crate would be `crates/backend-tcop-cmdtag-seams`, which does not
exist. Correct: cmdtag.c is a pure acyclic leaf. Its two callees —
`pg_strcasecmp` (port-pgstrcasecmp) and `pg_ulltoa_n`
(backend-utils-adt-numutils) — are acyclic leaves called directly as normal
crate dependencies, no seam. `seams-init` contains no cmdtag entry, which is
correct (nothing to install). No outward seam calls, no uninstalled
declarations, no `set()` outside an owner. **Zero seam findings.**

## 3b. Design conformance
- No invented opacity: `CommandTag`, `QueryCompletion`, `CMDTAG_*`,
  `COMPLETION_TAG_BUFSIZE` are real types/constants sourced from `types-portal`
  (types.md rules 6-7 satisfied — nothing fabricated).
- Allocating function `build_query_completion_string` correctly takes `Mcx` and
  returns `PgResult` (allocation refusal surfaces as recoverable `PgError`,
  never aborts) — §3b allocating-function rule satisfied.
- No shared statics for per-backend state (`TAG_BEHAVIOR` is immutable `static`
  const data, the direct analog of C `static const`). No ambient-global seams,
  no locks, no registry side tables.
- Idiomatic divergence (buffer→PgString return) is ledgered in the doc comment.
- Header inlines `SetQueryCompletion`/`CopyQueryCompletion` are declared in
  cmdtag.h (not cmdtag.c) and are not part of this unit's `c_sources`; their
  absence from this crate is correct — they belong with the `QueryCompletion`
  owner (types-portal) and its consumers.

## 4. Build & tests
`cargo test -p backend-tcop-cmdtag`: 18 passed, 0 failed (incl. byte-exact
display-flag spot checks, full-table enum round-trip, sortedness/bsearch
invariant, build-string cases).

## Verdict
**PASS.** All 9 inventory items MATCH; the command-tag table is byte-for-byte
identical to cmdtaglist.h; all constants verified against headers; the unit is a
pure leaf with no owned seam crate and zero seam findings; design-conformance
rules satisfied.
