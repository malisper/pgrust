# Audit: backend-backup-sink

- **Verdict: PASS**
- Date: 2026-06-13
- Model: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- Branch: `port/backend-backup-sink`
- C sources: `src/backend/backup/basebackup_sink.c` (+ header
  `src/include/backup/basebackup_sink.h`)
- c2rust: `c2rust-runs/backend-backup-sink/src/basebackup_sink.rs`
- Port: `crates/backend-backup-sink/src/lib.rs`

## 1. Function inventory

`basebackup_sink.c` defines 9 functions, all of which forward a callback to the
successor sink. The header `basebackup_sink.h` defines 9 `static inline`
dispatch helpers that the c2rust run inlined into the same crate (`bbsink_*`),
plus the `bbsink`, `bbsink_state`, `bbsink_ops` structs and the `tablespaceinfo`
support type (from `basebackup.h`). The c2rust rendering contains exactly these
9 `#[no_mangle]` forward functions + 9 inline dispatch helpers and no other
function definitions (no statics, no extra helpers). The port reimplements the
9 inline dispatch helpers as free functions and the 9 forwarders as free
functions, replacing the C vtable with the `BbsinkOps` trait.

### `.c` forwarding functions

| # | C function (basebackup_sink.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `bbsink_forward_begin_backup` :23 | `bbsink_forward_begin_backup` :525 | MATCH | Asserts next!=NULL (port: `.expect`), forwards begin_backup with this sink's `buffer_length`, then shares next's buffer. C copies `bbs_buffer = bbs_next->bbs_buffer`; port sets `shares_next_buffer=true` so buffer queries delegate to next — behaviorally identical. C `Assert(bbs_state != NULL)` is structurally guaranteed (state passed by `&mut`). |
| 2 | `bbsink_forward_begin_archive` :36 | `bbsink_forward_begin_archive` :541 | MATCH | Forwards to next with archive_name. |
| 3 | `bbsink_forward_archive_contents` :53 | `bbsink_forward_archive_contents` :559 | MATCH | C asserts next!=NULL and buffer/length equal next's; port `assert_shared_buffer` checks next present, `shares_next_buffer`, and equal lengths — same predicate in the sharing model. Forwards len. |
| 4 | `bbsink_forward_end_archive` :65 | `bbsink_forward_end_archive` :573 | MATCH | Forwards to next. |
| 5 | `bbsink_forward_begin_manifest` :75 | `bbsink_forward_begin_manifest` :585 | MATCH | Forwards to next. |
| 6 | `bbsink_forward_manifest_contents` :88 | `bbsink_forward_manifest_contents` :599 | MATCH | Same shared-buffer assert + forward as #3. |
| 7 | `bbsink_forward_end_manifest` :100 | `bbsink_forward_end_manifest` :613 | MATCH | Forwards to next. |
| 8 | `bbsink_forward_end_backup` :110 | `bbsink_forward_end_backup` :625 | MATCH | Forwards endptr, endtli to next. |
| 9 | `bbsink_forward_cleanup` :120 | `bbsink_forward_cleanup` :639 | MATCH | Forwards to next. |

### Header inline dispatch helpers (`bbsink_*`)

| # | C inline (basebackup_sink.h) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 10 | `bbsink_begin_backup` :174 | `bbsink_begin_backup` :400 | MATCH | Sets state+buffer_length, calls begin_backup, then asserts `buffer != NULL` and `buffer_length % BLCKSZ == 0`. C asserts `buffer_length > 0` (port: pre-assert), `sink != NULL` (structural). |
| 11 | `bbsink_begin_archive` :190 | `bbsink_begin_archive` :419 | MATCH | Dispatches begin_archive. |
| 12 | `bbsink_archive_contents` :199 | `bbsink_archive_contents` :430 | MATCH | Asserts `len > 0 && len <= buffer_length` (port uses `buffer_length()`, which for a forwarding sink resolves to next's length — equal to the C `bbs_buffer_length` propagated at begin_backup). |
| 13 | `bbsink_end_archive` :215 | `bbsink_end_archive` :448 | MATCH | Dispatches end_archive. |
| 14 | `bbsink_begin_manifest` :224 | `bbsink_begin_manifest` :456 | MATCH | Dispatches begin_manifest. |
| 15 | `bbsink_manifest_contents` :233 | `bbsink_manifest_contents` :466 | MATCH | Same `len` assert as #12. |
| 16 | `bbsink_end_manifest` :245 | `bbsink_end_manifest` :482 | MATCH | Dispatches end_manifest. |
| 17 | `bbsink_end_backup` :254 | `bbsink_end_backup` :495 | MATCH | Asserts `tablespace_num == list_length(tablespaces)` → port `tablespace_num == tablespaces.len()`. Then dispatches end_backup with endptr, endtli. |
| 18 | `bbsink_cleanup` :264 | `bbsink_cleanup` :511 | MATCH | Dispatches cleanup. |

### Structs / constants verified

- `bbsink_state` fields (tablespaces, tablespace_num, bytes_done, bytes_total,
  bytes_total_is_valid, startptr, starttli) — match `BbsinkState`.
- `bbsink_ops` 9 callbacks — match `BbsinkOps` trait methods, same order/arity.
- `tablespaceinfo` (oid: Oid/u32, path, rpath, size: int64/i64 with -1 = None) —
  match `TablespaceInfo`. `size: Option<i64>` faithfully models the C `-1`
  sentinel.
- `BLCKSZ = 8192` (types-core) matches the PG default.
- `XLogRecPtr = u64`, `TimeLineID = u32`, `Oid = u32`, `Size = usize` —
  all match.

## 2. Seam audit

**Owned seam crates:** none. The only C file in this unit's `c_sources` is
`basebackup_sink.c`; no `crates/X-seams` maps to it. `backend-backup-walsummary-seams`
maps to `walsummary.c` (a different unit) and is not owned here.

- `init_seams()` is empty — correct, since there are no owned seam declarations
  to install (the unit is a vtable leaf with no cross-cycle callers). Per SKILL
  §3 an empty installer is a FAIL only when owned seam crates are outstanding;
  none are.
- `seams-init::init_all` calls `backend_backup_sink::init_seams()` (seams-init
  lib.rs:22) — wired.
- No outward seam calls in the port. No function body was replaced by a seam
  call to elsewhere; all logic lives in this crate. Zero seam findings.

## 3b. Design conformance

- Buffer allocation (`set_buffer`) takes `Mcx` and returns `PgResult`, using
  fallible `vec_with_capacity_in` (enforces `MaxAllocSize`, surfaces OOM as a
  recoverable `PgError` rather than aborting). Conforms to the allocating-fn
  rule. `#![forbid(unsafe_code)]`.
- No invented opacity: `bbsink`/`bbsink_state`/`bbsink_ops` become real Rust
  types (`Bbsink`, `BbsinkState`, `BbsinkOps` trait); the C function-pointer
  vtable becomes a trait object. No stand-in handles introduced.
- Shared backup `state` is threaded explicitly through dispatch (`&mut BbsinkState`)
  rather than stored as a per-sink back-pointer or a shared static — avoids a
  shared-static-for-per-backup-global violation while preserving "single shared
  state" semantics.
- No locks, no registry side tables, no ambient-global seams, no unledgered
  divergence markers.
- The `dispatch` helper temporarily moves the ops box out behind a
  Drop-restoring guard so callbacks can borrow the sink's buffer/next without
  aliasing the box; the guard restores ops even on unwind. This is a
  borrow-checker accommodation with no behavioral divergence (the placeholder
  `NoopOps` is never invoked: `unreachable!`).

## 4. Verdict

All 18 functions MATCH. Zero seam findings, zero design-conformance findings.
`cargo test -p backend-backup-sink`: 9 passed.

**PASS.**
