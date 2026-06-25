# Audit: backend-access-transam-xlogbackup

C source: `src/backend/access/transam/xlogbackup.c` (PostgreSQL 18.3)
Port crate: `crates/backend-access-transam-xlogbackup`
Type added: `BackupState` -> `crates/types-wal/src/wal.rs`

## 1. Function inventory

xlogbackup.c defines exactly one function. The c2rust rendering confirms no
additional statics/inline helpers were compiled in; the only `extern` items it
pulls in are the macro-expanded inline `XLogFileName` and the StringInfo /
timezone externals.

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `build_backup_content` | xlogbackup.c:29-94 | lib.rs `build_backup_content` | MATCH | full body ported; see §2 |

Macros/inlines used by the function (xlog_internal.h / xlogdefs.h), ported
in-crate as pure arithmetic (no cross-crate dependency):

| C macro/inline | C loc | Port loc | Verdict |
|---|---|---|---|
| `XLogSegmentsPerXLogId` | xlog_internal.h:100 | `xlog_segments_per_xlog_id` | MATCH |
| `XLByteToSeg` | xlog_internal.h:117 | `xl_byte_to_seg` | MATCH |
| `XLogFileName` (inline) | xlog_internal.h:166 | `xlog_file_name` | MATCH |
| `LSN_FORMAT_ARGS` | xlogdefs.h:44 | `append_lsn_line` | MATCH |

## 2. build_backup_content — line-by-line

- `Assert(state != NULL)` — N/A: `state: &BackupState` is non-null by type.
- START TIME: `pg_strftime(..., "%Y-%m-%d %H:%M:%S %Z", pg_localtime(&starttime, log_timezone))`
  -> `format_backup_time(state.starttime(), log_timezone)`. C never NULL-checks
  `pg_localtime`; Rust `pg_localtime`/`pg_strftime` return `Option`, surfaced as
  `PgError::error` (recoverable) — this is *stricter* than C's NULL-deref crash,
  not a behavioral divergence on valid input. 128-byte buffer mirrored. MATCH.
- `XLByteToSeg(startpoint, startsegno, wal_segment_size)` then
  `XLogFileName(startxlogfile, starttli, startsegno, wal_segment_size)` ->
  identical arithmetic; output format `%08X%08X%08X`. Byte-identical to main's
  audited canonical `backend-access-transam-xlog::XLogFileName`. MATCH.
- `appendStringInfo("START WAL LOCATION: %X/%X (file %s)\n", LSN_FORMAT_ARGS, file)`
  -> `append_lsn_line("START WAL LOCATION", startpoint, Some(file))`.
  `%X/%X` of `(uint32)(lsn>>32), (uint32)lsn` -> `format!("{:X}/{:X}", (lsn>>32) as u32, lsn as u32)`. MATCH.
- `if (ishistoryfile)` STOP WAL LOCATION block — same XLByteToSeg/XLogFileName +
  append; gated on `ishistoryfile`. MATCH.
- CHECKPOINT LOCATION `%X/%X` -> `append_lsn_line(.., None)`. MATCH.
- `BACKUP METHOD: streamed\n` literal. MATCH.
- `BACKUP FROM: %s` with `started_in_recovery ? "standby" : "primary"`. MATCH.
- `START TIME: %s` <- startstrbuf. MATCH.
- `LABEL: %s` <- `state->name` — copied verbatim from the raw `[u8; MAXPGPATH+1]`
  up to the first NUL (`backup_name`), no UTF-8 validation, so server-encoding /
  non-UTF-8 labels survive byte-for-byte (matches C printf). MATCH.
- `START TIMELINE: %u` <- starttli. MATCH.
- `if (ishistoryfile)` STOP TIME / STOP TIMELINE block (same strftime path on
  `stoptime`, `%u` on stoptli). MATCH.
- `Assert(XLogRecPtrIsInvalid(istartpoint) == (istarttli == 0))` ->
  `debug_assert_eq!(istartpoint == 0, istarttli == 0)`. `XLogRecPtrIsInvalid`
  is `== 0` (InvalidXLogRecPtr). MATCH.
- `if (!XLogRecPtrIsInvalid(istartpoint))` -> `if state.istartpoint() != 0`:
  INCREMENTAL FROM LSN `%X/%X` + INCREMENTAL FROM TLI `%u`. MATCH.
- C `data = result->data; pfree(result); return data` -> return owned `Vec<u8>`
  (the palloc'd-string analog). MATCH.

Return type: C returns `char *` (infallible). Port returns `PgResult<Vec<u8>>`
to carry the two `Option` failures from `pg_localtime`/`pg_strftime` that C
silently NULL-derefs/overflows; on all valid inputs this is identical. This
matches the established src-idiomatic counterpart's contract. Acceptable.

`build_backup_content_default` is a convenience wrapper (DEFAULT_XLOG_SEG_SIZE +
`state_pgtz::log_timezone()`), modeling C's read of the `wal_segment_size` /
`log_timezone` globals at the call site. No C divergence.

## 3. Seams and wiring

No owned seam crate exists or is required. C externals:

- `wal_segment_size`, `log_timezone` — C globals, threaded as parameters
  (`log_timezone` via `state-pgtz` in the `_default` wrapper). Per AGENTS.md no
  zero-arg ambient-global getter seam is introduced.
- `pg_localtime` / `pg_strftime` — ported crates, **direct** deps
  (`backend-timezone-localtime` / `backend-timezone-strftime`); no cycle, so no
  seam, per the default "depend directly" rule.
- `XLByteToSeg` / `XLogFileName` / `XLogSegmentsPerXLogId` — xlog_internal.h
  macros, not owned functions; ported in-crate (same as main's xlog crate).

No `crates/*-seams` maps to `xlogbackup.c`, so there is no `init_seams()` to
wire and the seams-init recurrence guard has nothing to require. Confirmed:
`cargo test -p seams-init` (both recurrence guards) passes.

## 3b. Design conformance

- BackupState carries the real struct field-for-field (xlogbackup.h:20-38), no
  opaque handle / invented opacity. PASS.
- No `Mcx`/`PgResult` allocation-rule violation: the C builds the result with
  `appendStringInfo` (palloc) but its only failure mode is OOM; the port's
  `Vec`/`format!` allocations mirror that, and the genuine error paths
  (`pg_localtime`/`pg_strftime` Option) are `PgError` at the return-Err site.
- No shared statics for per-backend globals (log_timezone comes from
  `state-pgtz`'s thread_local). No ambient-global getter seam. No locks. No
  registry side-tables. No `todo!`/`unimplemented!`/`unreachable!`/own-logic
  `panic!`. PASS.

## 4. Verdict: PASS

All functions MATCH. Zero seam findings. Zero design-conformance findings.
Gate: `cargo check --workspace` clean, `cargo test -p backend-access-transam-xlogbackup`
(4 pass), `cargo test -p seams-init` (recurrence guards pass).
